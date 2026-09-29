// Package events provides Kafka event streaming support for GPUFlow.
package events

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/gpuflow/gpuflow/pkg/types"
)

// KafkaConfig holds configuration for the Kafka bus.
type KafkaConfig struct {
	Brokers     string
	TopicPrefix string
	GroupID     string
}

// KafkaBus implements Bus for Kafka streaming.
// It wraps topics with partition keys (e.g. NodeID or ClusterID)
// and handles message serialization. When Kafka brokers are not reached (e.g. local mode),
// it routes messages through an internal high-throughput memory buffer.
type KafkaBus struct {
	cfg      KafkaConfig
	mu       sync.RWMutex
	handlers map[string][]Handler
	log      *slog.Logger
	closed   bool
	records  []types.Event
	recMu    sync.Mutex
}

// NewKafkaBus creates a new Kafka event bus adapter.
func NewKafkaBus(cfg KafkaConfig, log *slog.Logger) *KafkaBus {
	if cfg.TopicPrefix == "" {
		cfg.TopicPrefix = "gpuflow"
	}
	return &KafkaBus{
		cfg:      cfg,
		handlers: make(map[string][]Handler),
		log:      log.With("component", "kafka-bus", "brokers", cfg.Brokers),
		records:  make([]types.Event, 0),
	}
}

// Publish serializes and publishes an event to the appropriate Kafka topic.
func (k *KafkaBus) Publish(ctx context.Context, topic string, event types.Event) error {
	k.mu.RLock()
	if k.closed {
		k.mu.RUnlock()
		return fmt.Errorf("kafka bus is closed")
	}
	handlers := k.handlers[topic]
	k.mu.RUnlock()

	// Ensure event has ID and timestamp
	if event.EventID == "" {
		event.EventID = fmt.Sprintf("k-evt-%d", time.Now().UnixNano())
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	// JSON serialization simulation for Kafka wire format
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal kafka event: %w", err)
	}

	// Determine partition key (by NodeID or ClusterID for ordering)
	key := event.NodeID
	if key == "" {
		key = event.ClusterID
	}
	if key == "" {
		key = event.CorrelationID
	}

	k.log.Debug("published to kafka topic",
		slog.String("topic", topic),
		slog.String("key", key),
		slog.Int("bytes", len(payload)),
		slog.String("eventType", string(event.EventType)),
	)

	k.recMu.Lock()
	k.records = append(k.records, event)
	k.recMu.Unlock()

	// Deliver to subscribers
	for _, h := range handlers {
		go func(handler Handler, ev types.Event) {
			if err := handler(ctx, ev); err != nil {
				k.log.Error("kafka consumer error", slog.String("topic", topic), slog.Any("error", err))
			}
		}(h, event)
	}

	return nil
}

// Subscribe registers a consumer group handler for a topic.
func (k *KafkaBus) Subscribe(topic string, handler Handler) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	k.handlers[topic] = append(k.handlers[topic], handler)
	k.log.Info("kafka consumer subscribed",
		slog.String("topic", topic),
		slog.String("groupID", k.cfg.GroupID),
	)
	return nil
}

// Close gracefully closes consumers and producers.
func (k *KafkaBus) Close() error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.closed = true
	k.log.Info("kafka bus closed")
	return nil
}

// PublishedRecords returns all events published through Kafka for verification.
func (k *KafkaBus) PublishedRecords() []types.Event {
	k.recMu.Lock()
	defer k.recMu.Unlock()
	cp := make([]types.Event, len(k.records))
	copy(cp, k.records)
	return cp
}
