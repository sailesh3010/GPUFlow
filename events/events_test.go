package events

import (
	"context"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gpuflow/gpuflow/pkg/types"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestPublishAndSubscribe(t *testing.T) {
	bus := NewInMemoryBus(testLogger())
	ctx := context.Background()

	var received int32
	bus.Subscribe(TopicNodeEvents, func(ctx context.Context, event types.Event) error {
		atomic.AddInt32(&received, 1)
		if event.EventType != types.EventNodeFailed {
			t.Errorf("expected NODE_FAILED, got %s", event.EventType)
		}
		return nil
	})

	bus.Publish(ctx, TopicNodeEvents, types.Event{
		EventID:   "evt-1",
		EventType: types.EventNodeFailed,
		Timestamp: time.Now(),
		NodeID:    "gpu-node-01",
	})

	if atomic.LoadInt32(&received) != 1 {
		t.Errorf("expected 1 event received, got %d", received)
	}
}

func TestMultipleSubscribers(t *testing.T) {
	bus := NewInMemoryBus(testLogger())
	ctx := context.Background()

	var count int32
	for i := 0; i < 3; i++ {
		bus.Subscribe(TopicClusterEvents, func(ctx context.Context, event types.Event) error {
			atomic.AddInt32(&count, 1)
			return nil
		})
	}

	bus.Publish(ctx, TopicClusterEvents, types.Event{
		EventID:   "evt-2",
		EventType: types.EventClusterCreated,
		Timestamp: time.Now(),
	})

	if atomic.LoadInt32(&count) != 3 {
		t.Errorf("expected 3 handlers called, got %d", count)
	}
}

func TestHistory(t *testing.T) {
	bus := NewInMemoryBus(testLogger())
	ctx := context.Background()

	bus.Publish(ctx, TopicNodeEvents, types.Event{EventID: "e1", EventType: types.EventNodeReady, Timestamp: time.Now()})
	bus.Publish(ctx, TopicNodeEvents, types.Event{EventID: "e2", EventType: types.EventNodeFailed, Timestamp: time.Now()})

	history := bus.History()
	if len(history) != 2 {
		t.Errorf("expected 2 events in history, got %d", len(history))
	}
}

func TestNoSubscribers(t *testing.T) {
	bus := NewInMemoryBus(testLogger())
	ctx := context.Background()

	// Should not panic with no subscribers
	err := bus.Publish(ctx, TopicNodeEvents, types.Event{
		EventID:   "evt-orphan",
		EventType: types.EventNodeDiscovered,
		Timestamp: time.Now(),
	})
	if err != nil {
		t.Errorf("publish with no subscribers should not error: %v", err)
	}
}

func TestKafkaBusPublishAndSubscribe(t *testing.T) {
	kBus := NewKafkaBus(KafkaConfig{
		Brokers:     "localhost:9092",
		TopicPrefix: "gpuflow",
		GroupID:     "gpuflow-group",
	}, testLogger())
	ctx := context.Background()

	var received int32
	_ = kBus.Subscribe(TopicNodeEvents, func(ctx context.Context, event types.Event) error {
		atomic.AddInt32(&received, 1)
		return nil
	})

	err := kBus.Publish(ctx, TopicNodeEvents, NodeFailed("gpu-node-03", "corr-test"))
	if err != nil {
		t.Fatalf("unexpected kafka publish error: %v", err)
	}

	time.Sleep(20 * time.Millisecond)
	if atomic.LoadInt32(&received) != 1 {
		t.Fatalf("expected 1 event received by kafka subscriber, got %d", received)
	}

	records := kBus.PublishedRecords()
	if len(records) != 1 {
		t.Fatalf("expected 1 record in kafka log, got %d", len(records))
	}
	if records[0].NodeID != "gpu-node-03" {
		t.Fatalf("expected node gpu-node-03 in kafka record, got %s", records[0].NodeID)
	}
}
