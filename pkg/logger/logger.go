// Package logger provides structured JSON logging for GPUFlow.
// All log output includes component, correlation ID, and resource identifiers
// to enable tracing across the distributed control plane.
package logger

import (
	"context"
	"log/slog"
	"os"
)

type contextKey string

const (
	correlationIDKey contextKey = "correlationId"
	componentKey     contextKey = "component"
)

// New creates a new structured JSON logger for the given component.
func New(component string) *slog.Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	return slog.New(handler).With(
		slog.String("component", component),
	)
}

// NewDev creates a text-format logger for development.
func NewDev(component string) *slog.Logger {
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})
	return slog.New(handler).With(
		slog.String("component", component),
	)
}

// WithCorrelationID adds a correlation ID to the context.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationIDKey, id)
}

// CorrelationID extracts the correlation ID from the context.
func CorrelationID(ctx context.Context) string {
	if id, ok := ctx.Value(correlationIDKey).(string); ok {
		return id
	}
	return ""
}

// WithNode returns a logger with node ID attached.
func WithNode(log *slog.Logger, nodeID string) *slog.Logger {
	return log.With(slog.String("nodeId", nodeID))
}

// WithCluster returns a logger with cluster ID attached.
func WithCluster(log *slog.Logger, clusterID string) *slog.Logger {
	return log.With(slog.String("clusterId", clusterID))
}

// WithCorrelation returns a logger with correlation ID attached.
func WithCorrelation(log *slog.Logger, correlationID string) *slog.Logger {
	return log.With(slog.String("correlationId", correlationID))
}
