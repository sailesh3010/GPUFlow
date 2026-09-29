// Package simulator — failure injection for chaos testing.
package simulator

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"time"
)

// FailureType represents a category of simulated failure.
type FailureType string

const (
	FailureGPU         FailureType = "GPU_FAILURE"
	FailureDriver      FailureType = "DRIVER_FAILURE"
	FailureNetwork     FailureType = "NETWORK_FAILURE"
	FailureTemperature FailureType = "TEMPERATURE_HIGH"
	FailureUnreachable FailureType = "NODE_UNREACHABLE"
)

// FailureInjector creates controlled failures in the fleet for testing.
type FailureInjector struct {
	fleet *FleetSimulator
	log   *slog.Logger
	rng   *rand.Rand
}

// NewFailureInjector creates a failure injector.
func NewFailureInjector(fleet *FleetSimulator, log *slog.Logger) *FailureInjector {
	return &FailureInjector{
		fleet: fleet,
		log:   log,
		rng:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// InjectNodeFailure simulates a complete node failure.
func (fi *FailureInjector) InjectNodeFailure(ctx context.Context, nodeID string) error {
	fi.log.Info("injecting node failure",
		slog.String("nodeId", nodeID),
		slog.String("failureType", string(FailureUnreachable)),
	)

	return fi.fleet.FailNode(ctx, nodeID, "chaos: node failure injected")
}

// InjectGPUFailure simulates a single GPU failure.
func (fi *FailureInjector) InjectGPUFailure(ctx context.Context, nodeID, gpuID string) error {
	fi.log.Info("injecting GPU failure",
		slog.String("nodeId", nodeID),
		slog.String("gpuId", gpuID),
		slog.String("failureType", string(FailureGPU)),
	)

	return fi.fleet.FailGPU(ctx, nodeID, gpuID)
}

// InjectRandomFailure picks a random healthy node and fails it.
func (fi *FailureInjector) InjectRandomFailure(ctx context.Context) (string, error) {
	nodes := fi.fleet.ListNodes()

	var healthy []string
	for _, n := range nodes {
		if n.Health == "HEALTHY" {
			healthy = append(healthy, n.ID)
		}
	}

	if len(healthy) == 0 {
		return "", fmt.Errorf("no healthy nodes available for failure injection")
	}

	target := healthy[fi.rng.Intn(len(healthy))]
	err := fi.InjectNodeFailure(ctx, target)
	return target, err
}

// InjectDriverFailure simulates a driver crash by degrading all GPUs on a node.
func (fi *FailureInjector) InjectDriverFailure(ctx context.Context, nodeID string) error {
	fi.log.Info("injecting driver failure",
		slog.String("nodeId", nodeID),
		slog.String("failureType", string(FailureDriver)),
	)

	node, err := fi.fleet.GetNode(nodeID)
	if err != nil {
		return err
	}

	// Fail all GPUs on the node (driver crash affects all devices)
	for _, gpu := range node.GPUs {
		if err := fi.fleet.FailGPU(ctx, nodeID, gpu.ID); err != nil {
			fi.log.Warn("failed to fail GPU", slog.String("gpuId", gpu.ID), slog.Any("error", err))
		}
	}

	return nil
}

// InjectNetworkFailure simulates a network partition by failing the node.
func (fi *FailureInjector) InjectNetworkFailure(ctx context.Context, nodeID string) error {
	fi.log.Info("injecting network failure",
		slog.String("nodeId", nodeID),
		slog.String("failureType", string(FailureNetwork)),
	)

	return fi.fleet.FailNode(ctx, nodeID, "chaos: network failure injected")
}
