// Package events provides the event bus abstraction for GPUFlow.
// It supports both an in-memory bus for development and a Kafka backend for production.
package events

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/gpuflow/gpuflow/pkg/types"
)

// Handler processes events from the event bus.
type Handler func(ctx context.Context, event types.Event) error

// Bus is the event bus interface.
type Bus interface {
	// Publish sends an event to the bus.
	Publish(ctx context.Context, topic string, event types.Event) error

	// Subscribe registers a handler for events on the given topic.
	Subscribe(topic string, handler Handler) error

	// Close shuts down the event bus.
	Close() error
}

// Topic constants for GPUFlow events.
const (
	TopicNodeEvents      = "gpuflow.node.events"
	TopicClusterEvents   = "gpuflow.cluster.events"
	TopicHealthEvents    = "gpuflow.health.events"
	TopicSchedulerEvents = "gpuflow.scheduler.events"
	TopicAuditEvents     = "gpuflow.audit.events"
)

// InMemoryBus implements Bus using in-memory channels.
// Used for development and testing when Kafka is not available.
type InMemoryBus struct {
	mu       sync.RWMutex
	handlers map[string][]Handler
	log      *slog.Logger
	history  []types.Event
	histMu   sync.Mutex
}

// NewInMemoryBus creates an in-memory event bus.
func NewInMemoryBus(log *slog.Logger) *InMemoryBus {
	return &InMemoryBus{
		handlers: make(map[string][]Handler),
		log:      log,
		history:  make([]types.Event, 0),
	}
}

func (b *InMemoryBus) Publish(ctx context.Context, topic string, event types.Event) error {
	b.log.Info("event published",
		slog.String("topic", topic),
		slog.String("eventType", string(event.EventType)),
		slog.String("eventId", event.EventID),
	)

	// Record in history
	b.histMu.Lock()
	b.history = append(b.history, event)
	b.histMu.Unlock()

	// Dispatch to handlers
	b.mu.RLock()
	handlers := b.handlers[topic]
	b.mu.RUnlock()

	for _, h := range handlers {
		if err := h(ctx, event); err != nil {
			b.log.Error("event handler error",
				slog.String("topic", topic),
				slog.String("eventType", string(event.EventType)),
				slog.Any("error", err),
			)
		}
	}

	return nil
}

func (b *InMemoryBus) Subscribe(topic string, handler Handler) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.handlers[topic] = append(b.handlers[topic], handler)
	b.log.Info("subscribed to topic", slog.String("topic", topic))
	return nil
}

func (b *InMemoryBus) Close() error {
	b.log.Info("event bus closed")
	return nil
}

// History returns all events published to the bus.
func (b *InMemoryBus) History() []types.Event {
	b.histMu.Lock()
	defer b.histMu.Unlock()

	result := make([]types.Event, len(b.history))
	copy(result, b.history)
	return result
}

// NewBus creates a new default in-memory event bus.
func NewBus(logger *slog.Logger) Bus {
	return NewInMemoryBus(logger)
}

// Helper factory functions for standard events

func NodeDiscovered(nodeID, correlationID string) types.Event {
	return types.Event{
		EventID:       fmt.Sprintf("evt-ndisc-%d", time.Now().UnixNano()),
		EventType:     types.EventNodeDiscovered,
		Timestamp:     time.Now(),
		NodeID:        nodeID,
		CorrelationID: correlationID,
	}
}

func NodeReady(nodeID, correlationID string) types.Event {
	return types.Event{
		EventID:       fmt.Sprintf("evt-nrdy-%d", time.Now().UnixNano()),
		EventType:     types.EventNodeReady,
		Timestamp:     time.Now(),
		NodeID:        nodeID,
		CorrelationID: correlationID,
	}
}

func NodeFailed(nodeID, correlationID string) types.Event {
	return types.Event{
		EventID:       fmt.Sprintf("evt-nfail-%d", time.Now().UnixNano()),
		EventType:     types.EventNodeFailed,
		Timestamp:     time.Now(),
		NodeID:        nodeID,
		CorrelationID: correlationID,
	}
}

func NodeDraining(nodeID, correlationID string) types.Event {
	return types.Event{
		EventID:       fmt.Sprintf("evt-ndrain-%d", time.Now().UnixNano()),
		EventType:     types.EventNodeDraining,
		Timestamp:     time.Now(),
		NodeID:        nodeID,
		CorrelationID: correlationID,
	}
}

func NodeRepaired(nodeID, correlationID string) types.Event {
	return types.Event{
		EventID:       fmt.Sprintf("evt-nrep-%d", time.Now().UnixNano()),
		EventType:     types.EventNodeRepaired,
		Timestamp:     time.Now(),
		NodeID:        nodeID,
		CorrelationID: correlationID,
	}
}

func ClusterReady(clusterID, correlationID string) types.Event {
	return types.Event{
		EventID:       fmt.Sprintf("evt-crdy-%d", time.Now().UnixNano()),
		EventType:     types.EventClusterReady,
		Timestamp:     time.Now(),
		ClusterID:     clusterID,
		CorrelationID: correlationID,
	}
}

func ClusterScaled(clusterID string, replicas int, correlationID string) types.Event {
	return types.Event{
		EventID:       fmt.Sprintf("evt-cscld-%d", time.Now().UnixNano()),
		EventType:     types.EventClusterScaled,
		Timestamp:     time.Now(),
		ClusterID:     clusterID,
		CorrelationID: correlationID,
		Data: map[string]string{
			"replicas": fmt.Sprintf("%d", replicas),
		},
	}
}

func ClusterDeleted(clusterID, correlationID string) types.Event {
	return types.Event{
		EventID:       fmt.Sprintf("evt-cdel-%d", time.Now().UnixNano()),
		EventType:     types.EventClusterDeleted,
		Timestamp:     time.Now(),
		ClusterID:     clusterID,
		CorrelationID: correlationID,
	}
}
