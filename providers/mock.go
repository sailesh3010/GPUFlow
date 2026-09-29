package providers

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/gpuflow/gpuflow/pkg/types"
	"github.com/gpuflow/gpuflow/simulator"
)

// MockProvider implements Provider using the fleet simulator.
// This is the primary provider for local development and testing.
type MockProvider struct {
	fleet *simulator.FleetSimulator
	log   *slog.Logger
}

var _ Provider = (*MockProvider)(nil) // compile-time interface check

// NewMockProvider creates a mock provider backed by the fleet simulator.
func NewMockProvider(fleet *simulator.FleetSimulator, log *slog.Logger) *MockProvider {
	return &MockProvider{
		fleet: fleet,
		log:   log,
	}
}

func (m *MockProvider) Name() string {
	return "mock"
}

// Provision creates a simulated node and runs it through the provisioning lifecycle.
// Idempotent: returns existing node if ID already exists.
func (m *MockProvider) Provision(ctx context.Context, spec NodeSpec) (*types.Node, error) {
	// Idempotency: check if node already exists
	if existing, err := m.fleet.GetNode(spec.ID); err == nil {
		m.log.Info("node already exists, returning existing",
			slog.String("nodeId", spec.ID),
			slog.String("state", string(existing.State)),
		)
		return existing, nil
	}

	simSpec := simulator.NodeSpec{
		ID:               spec.ID,
		Provider:         "mock",
		Region:           spec.Region,
		Rack:             spec.Rack,
		GPUModel:         spec.GPUModel,
		GPUCount:         spec.GPUCount,
		GPUMemoryGB:      spec.GPUMemoryGB,
		CPUCores:         spec.CPUCores,
		RAMGB:            spec.RAMGB,
		NetworkBandwidth: spec.NetworkBandwidth,
		Topology:         spec.Topology,
	}

	_, err := m.fleet.CreateNode(ctx, simSpec)
	if err != nil {
		return nil, fmt.Errorf("mock provision: %w", err)
	}

	// Run through provisioning lifecycle
	if err := m.fleet.ProvisionNode(ctx, spec.ID, ""); err != nil {
		return nil, fmt.Errorf("mock provision lifecycle: %w", err)
	}

	// Return fresh state after provisioning
	return m.fleet.GetNode(spec.ID)
}

func (m *MockProvider) Deprovision(ctx context.Context, nodeID string) error {
	// First drain the node
	existingNode, err := m.fleet.GetNode(nodeID)
	if err != nil {
		// Already gone — idempotent
		return nil
	}

	// Release all allocated GPUs
	for _, gpu := range existingNode.GPUs {
		if gpu.Allocated {
			if err := m.fleet.ReleaseGPUs(ctx, nodeID, []string{gpu.ID}); err != nil {
				m.log.Warn("failed to release GPU during deprovision",
					slog.String("nodeId", nodeID),
					slog.String("gpuId", gpu.ID),
					slog.Any("error", err),
				)
			}
		}
	}

	return m.fleet.DeleteNode(ctx, nodeID)
}

func (m *MockProvider) Health(_ context.Context, nodeID string) (*HealthStatus, error) {
	node, err := m.fleet.GetNode(nodeID)
	if err != nil {
		return nil, fmt.Errorf("health check: %w", err)
	}

	gpuHealth := make([]GPUHealthInfo, len(node.GPUs))
	for i, gpu := range node.GPUs {
		gpuHealth[i] = GPUHealthInfo{
			ID:          gpu.ID,
			Health:      gpu.Health,
			Temperature: gpu.Temperature,
			Utilization: gpu.Utilization,
		}
	}

	return &HealthStatus{
		NodeID:    nodeID,
		Healthy:   node.Health == types.NodeHealthHealthy,
		Health:    node.Health,
		Message:   fmt.Sprintf("node %s is %s", nodeID, node.Health),
		GPUHealth: gpuHealth,
	}, nil
}

func (m *MockProvider) PowerCycle(ctx context.Context, nodeID string) error {
	m.log.Info("power cycling node", slog.String("nodeId", nodeID))
	return m.fleet.RecoverNode(ctx, nodeID)
}

func (m *MockProvider) List(_ context.Context) ([]*types.Node, error) {
	return m.fleet.ListNodes(), nil
}
