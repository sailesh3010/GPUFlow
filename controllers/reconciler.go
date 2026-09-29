// Package controllers implements Kubernetes-style declarative reconciliation loops
// for InferenceCluster and GPUNode resources.
package controllers

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// ReconcileResult contains the result of a Reconcile invocation.
type ReconcileResult struct {
	Requeue      bool
	RequeueAfter time.Duration
}

// Request contains the information necessary to reconcile a Kubernetes object.
type Request struct {
	Namespace string
	Name      string
}

// Reconciler is the interface implemented by controllers.
type Reconciler interface {
	Reconcile(ctx context.Context, req Request) (ReconcileResult, error)
}

// WorkQueue is a thread-safe, deduplicated work queue with rate limiting.
type WorkQueue struct {
	mu        sync.Mutex
	cond      *sync.Cond
	queue     []Request
	inQueue   map[Request]bool
	shutDown  bool
	failures  map[Request]int
	baseDelay time.Duration
	maxDelay  time.Duration
}

// NewWorkQueue creates a new workqueue.
func NewWorkQueue(baseDelay, maxDelay time.Duration) *WorkQueue {
	q := &WorkQueue{
		queue:     make([]Request, 0),
		inQueue:   make(map[Request]bool),
		failures:  make(map[Request]int),
		baseDelay: baseDelay,
		maxDelay:  maxDelay,
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// Add adds an item to the work queue if not already present.
func (q *WorkQueue) Add(req Request) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.shutDown {
		return
	}
	if q.inQueue[req] {
		return
	}

	q.queue = append(q.queue, req)
	q.inQueue[req] = true
	q.cond.Signal()
}

// AddRateLimited adds an item to the queue with exponential backoff on retry.
func (q *WorkQueue) AddRateLimited(req Request) {
	q.mu.Lock()
	count := q.failures[req]
	q.failures[req] = count + 1
	delay := q.baseDelay * time.Duration(1<<count)
	if delay > q.maxDelay {
		delay = q.maxDelay
	}
	q.mu.Unlock()

	go func() {
		time.Sleep(delay)
		q.Add(req)
	}()
}

// Forget removes failure count history for an item on success.
func (q *WorkQueue) Forget(req Request) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.failures, req)
}

// Get blocks until an item is available from the queue.
func (q *WorkQueue) Get() (Request, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for len(q.queue) == 0 && !q.shutDown {
		q.cond.Wait()
	}

	if q.shutDown && len(q.queue) == 0 {
		return Request{}, true
	}

	item := q.queue[0]
	q.queue = q.queue[1:]
	delete(q.inQueue, item)
	return item, false
}

// ShutDown shuts down the workqueue.
func (q *WorkQueue) ShutDown() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.shutDown = true
	q.cond.Broadcast()
}

// Controller coordinates the reconciliation loop for a Reconciler.
type Controller struct {
	Name       string
	Reconciler Reconciler
	Queue      *WorkQueue
	Logger     *slog.Logger
	stopCh     chan struct{}
	wg         sync.WaitGroup
}

// NewController creates a new Controller.
func NewController(name string, reconciler Reconciler, logger *slog.Logger) *Controller {
	return &Controller{
		Name:       name,
		Reconciler: reconciler,
		Queue:      NewWorkQueue(50*time.Millisecond, 5*time.Second),
		Logger:     logger.With("controller", name),
		stopCh:     make(chan struct{}),
	}
}

// Start runs the controller worker goroutines until ctx is cancelled.
func (c *Controller) Start(ctx context.Context, workers int) {
	for i := 0; i < workers; i++ {
		c.wg.Add(1)
		go func(workerID int) {
			defer c.wg.Done()
			c.runWorker(ctx, workerID)
		}(i)
	}
}

// Enqueue enqueues an item for reconciliation.
func (c *Controller) Enqueue(req Request) {
	c.Queue.Add(req)
}

func (c *Controller) runWorker(ctx context.Context, id int) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		default:
		}

		req, shutdown := c.Queue.Get()
		if shutdown {
			return
		}

		c.processNextWorkItem(ctx, req)
	}
}

func (c *Controller) processNextWorkItem(ctx context.Context, req Request) {
	c.Logger.Debug("reconciling item", "namespace", req.Namespace, "name", req.Name)
	result, err := c.Reconciler.Reconcile(ctx, req)
	if err != nil {
		c.Logger.Error("reconciliation failed", "namespace", req.Namespace, "name", req.Name, "error", err)
		c.Queue.AddRateLimited(req)
		return
	}

	c.Queue.Forget(req)

	if result.RequeueAfter > 0 {
		go func() {
			select {
			case <-time.After(result.RequeueAfter):
				c.Queue.Add(req)
			case <-ctx.Done():
			case <-c.stopCh:
			}
		}()
	} else if result.Requeue {
		c.Queue.Add(req)
	}
}

// Stop stops the controller and waits for workers to finish.
func (c *Controller) Stop() {
	close(c.stopCh)
	c.Queue.ShutDown()
	c.wg.Wait()
}
