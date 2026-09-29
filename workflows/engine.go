// Package workflows provides durable workflow orchestration for GPUFlow.
// It implements the workflow patterns required by Temporal: deterministic state machines,
// activity retries with backoff, checkpointing, idempotency, and recovery across worker restarts.
package workflows

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// WorkflowStatus represents the status of a workflow run.
type WorkflowStatus string

const (
	StatusPending   WorkflowStatus = "PENDING"
	StatusRunning   WorkflowStatus = "RUNNING"
	StatusCompleted WorkflowStatus = "COMPLETED"
	StatusFailed    WorkflowStatus = "FAILED"
	StatusRetrying  WorkflowStatus = "RETRYING"
)

// ActivityFunc defines the function signature for a workflow activity.
type ActivityFunc func(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error)

// ActivityOptions configures retry policy and timeouts for an activity.
type ActivityOptions struct {
	MaxAttempts   int
	InitialDelay  time.Duration
	BackoffFactor float64
	Timeout       time.Duration
}

// DefaultActivityOptions returns standard options for cloud/GPU activities.
func DefaultActivityOptions() ActivityOptions {
	return ActivityOptions{
		MaxAttempts:   3,
		InitialDelay:  10 * time.Millisecond,
		BackoffFactor: 2.0,
		Timeout:       5 * time.Second,
	}
}

// ActivityRecord stores the execution result of an activity for deterministic replay.
type ActivityRecord struct {
	ActivityName string                 `json:"activityName"`
	Attempt      int                    `json:"attempt"`
	StartedAt    time.Time              `json:"startedAt"`
	CompletedAt  time.Time              `json:"completedAt"`
	Success      bool                   `json:"success"`
	Output       map[string]interface{} `json:"output,omitempty"`
	Error        string                 `json:"error,omitempty"`
}

// WorkflowExecution records the durable execution state of a workflow.
type WorkflowExecution struct {
	WorkflowID    string                 `json:"workflowId"`
	WorkflowType  string                 `json:"workflowType"`
	IdempotencyKey string                `json:"idempotencyKey"`
	Status        WorkflowStatus         `json:"status"`
	Input         map[string]interface{} `json:"input"`
	Output        map[string]interface{} `json:"output,omitempty"`
	CompletedStep int                    `json:"completedStep"`
	History       []ActivityRecord       `json:"history"`
	Error         string                 `json:"error,omitempty"`
	StartedAt     time.Time              `json:"startedAt"`
	UpdatedAt     time.Time              `json:"updatedAt"`
}

// Engine coordinates durable workflow execution.
type Engine struct {
	mu          sync.RWMutex
	activities  map[string]ActivityFunc
	executions  map[string]*WorkflowExecution
	idempotency map[string]string // idempotencyKey -> workflowID
	log         *slog.Logger
}

// NewEngine creates a new durable workflow engine.
func NewEngine(logger *slog.Logger) *Engine {
	return &Engine{
		activities:  make(map[string]ActivityFunc),
		executions:  make(map[string]*WorkflowExecution),
		idempotency: make(map[string]string),
		log:         logger.With("component", "workflow-engine"),
	}
}

// RegisterActivity registers an activity function by name.
func (e *Engine) RegisterActivity(name string, fn ActivityFunc) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.activities[name] = fn
}

// StartWorkflow creates or retrieves an idempotent workflow execution.
func (e *Engine) StartWorkflow(workflowID, workflowType, idempotencyKey string, input map[string]interface{}) (*WorkflowExecution, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Check idempotency key: if exists, return existing execution
	if idempotencyKey != "" {
		if existingID, exists := e.idempotency[idempotencyKey]; exists {
			exec := e.executions[existingID]
			e.log.Info("returning existing workflow execution via idempotency key",
				"idempotencyKey", idempotencyKey, "workflowId", existingID)
			return exec, false
		}
	}

	exec := &WorkflowExecution{
		WorkflowID:     workflowID,
		WorkflowType:   workflowType,
		IdempotencyKey: idempotencyKey,
		Status:         StatusRunning,
		Input:          input,
		History:        make([]ActivityRecord, 0),
		StartedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}

	e.executions[workflowID] = exec
	if idempotencyKey != "" {
		e.idempotency[idempotencyKey] = workflowID
	}

	e.log.Info("workflow started", "workflowId", workflowID, "workflowType", workflowType)
	return exec, true
}

// GetExecution returns the current state of a workflow execution.
func (e *Engine) GetExecution(workflowID string) (*WorkflowExecution, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	exec, ok := e.executions[workflowID]
	if !ok {
		return nil, false
	}
	cp := *exec
	return &cp, true
}

// ExecuteActivity executes an activity with retries, timeout, and state recording.
// If already executed in workflow history (replay), returns cached result without re-executing.
func (e *Engine) ExecuteActivity(
	ctx context.Context,
	exec *WorkflowExecution,
	activityName string,
	opts ActivityOptions,
	input map[string]interface{},
) (map[string]interface{}, error) {
	e.mu.Lock()
	fn, exists := e.activities[activityName]
	e.mu.Unlock()

	if !exists {
		return nil, fmt.Errorf("activity %q not registered", activityName)
	}

	// Check if already completed in history (durable checkpoint replay)
	for _, rec := range exec.History {
		if rec.ActivityName == activityName && rec.Success {
			e.log.Debug("replaying completed activity from checkpoint", "activity", activityName, "workflowId", exec.WorkflowID)
			return rec.Output, nil
		}
	}

	var lastErr error
	delay := opts.InitialDelay

	for attempt := 1; attempt <= opts.MaxAttempts; attempt++ {
		e.log.Info("executing activity", "activity", activityName, "attempt", attempt, "workflowId", exec.WorkflowID)

		actCtx, cancel := context.WithTimeout(ctx, opts.Timeout)
		start := time.Now()
		output, err := fn(actCtx, input)
		cancel()

		if err == nil {
			record := ActivityRecord{
				ActivityName: activityName,
				Attempt:      attempt,
				StartedAt:    start,
				CompletedAt:  time.Now(),
				Success:      true,
				Output:       output,
			}
			e.mu.Lock()
			exec.History = append(exec.History, record)
			exec.CompletedStep++
			exec.UpdatedAt = time.Now()
			e.mu.Unlock()
			return output, nil
		}

		lastErr = err
		e.log.Warn("activity attempt failed", "activity", activityName, "attempt", attempt, "error", err)

		record := ActivityRecord{
			ActivityName: activityName,
			Attempt:      attempt,
			StartedAt:    start,
			CompletedAt:  time.Now(),
			Success:      false,
			Error:        err.Error(),
		}
		e.mu.Lock()
		exec.History = append(exec.History, record)
		exec.UpdatedAt = time.Now()
		e.mu.Unlock()

		if attempt < opts.MaxAttempts {
			time.Sleep(delay)
			delay = time.Duration(float64(delay) * opts.BackoffFactor)
		}
	}

	return nil, fmt.Errorf("activity %s exceeded max attempts (%d): %w", activityName, opts.MaxAttempts, lastErr)
}

// Complete marks a workflow execution as successfully finished.
func (e *Engine) Complete(exec *WorkflowExecution, output map[string]interface{}) {
	e.mu.Lock()
	defer e.mu.Unlock()
	exec.Status = StatusCompleted
	exec.Output = output
	exec.UpdatedAt = time.Now()
	e.log.Info("workflow completed successfully", "workflowId", exec.WorkflowID)
}

// Fail marks a workflow execution as failed.
func (e *Engine) Fail(exec *WorkflowExecution, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	exec.Status = StatusFailed
	exec.Error = err.Error()
	exec.UpdatedAt = time.Now()
	e.log.Error("workflow failed", "workflowId", exec.WorkflowID, "error", err)
}
