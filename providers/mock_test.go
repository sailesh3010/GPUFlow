package providers

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/gpuflow/gpuflow/pkg/types"
	"github.com/gpuflow/gpuflow/simulator"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func newTestProvider() (*MockProvider, *simulator.FleetSimulator) {
	log := testLogger()
	fleet := simulator.NewFleetSimulator(log)
	provider := NewMockProvider(fleet, log)
	return provider, fleet
}

func TestMockProviderProvision(t *testing.T) {
	provider, _ := newTestProvider()
	ctx := context.Background()

	node, err := provider.Provision(ctx, NodeSpec{
		ID:               "test-node-01",
		GPUModel:         types.GPUModelH100,
		GPUCount:         8,
		GPUMemoryGB:      80,
		CPUCores:         96,
		RAMGB:            512,
		NetworkBandwidth: "400Gbps",
		Topology:         types.TopologyNVLink,
		Region:           "local-1",
		Rack:             "rack-a",
	})

	if err != nil {
		t.Fatalf("Provision failed: %v", err)
	}

	if node.State != types.NodeStateReady {
		t.Errorf("expected READY state after provision, got %s", node.State)
	}
	if len(node.GPUs) != 8 {
		t.Errorf("expected 8 GPUs, got %d", len(node.GPUs))
	}
}

func TestMockProviderProvisionIdempotency(t *testing.T) {
	provider, _ := newTestProvider()
	ctx := context.Background()

	spec := NodeSpec{
		ID:       "idem-node",
		GPUModel: types.GPUModelH100,
		GPUCount: 8,
		GPUMemoryGB: 80,
		Region:   "local-1",
		Rack:     "rack-a",
	}

	node1, err := provider.Provision(ctx, spec)
	if err != nil {
		t.Fatalf("first Provision failed: %v", err)
	}

	node2, err := provider.Provision(ctx, spec)
	if err != nil {
		t.Fatalf("second Provision failed: %v", err)
	}

	if node1.ID != node2.ID {
		t.Error("idempotent provision should return same node")
	}
}

func TestMockProviderDeprovision(t *testing.T) {
	provider, _ := newTestProvider()
	ctx := context.Background()

	_, err := provider.Provision(ctx, NodeSpec{
		ID:       "deprov-node",
		GPUModel: types.GPUModelA100,
		GPUCount: 8,
		GPUMemoryGB: 80,
		Region:   "local-1",
		Rack:     "rack-a",
	})
	if err != nil {
		t.Fatalf("Provision failed: %v", err)
	}

	if err := provider.Deprovision(ctx, "deprov-node"); err != nil {
		t.Fatalf("Deprovision failed: %v", err)
	}

	// Deprovisioning again should be idempotent
	if err := provider.Deprovision(ctx, "deprov-node"); err != nil {
		t.Fatalf("second Deprovision should be idempotent: %v", err)
	}
}

func TestMockProviderHealth(t *testing.T) {
	provider, _ := newTestProvider()
	ctx := context.Background()

	_, err := provider.Provision(ctx, NodeSpec{
		ID:       "health-node",
		GPUModel: types.GPUModelH100,
		GPUCount: 8,
		GPUMemoryGB: 80,
		Region:   "local-1",
		Rack:     "rack-a",
	})
	if err != nil {
		t.Fatalf("Provision failed: %v", err)
	}

	health, err := provider.Health(ctx, "health-node")
	if err != nil {
		t.Fatalf("Health check failed: %v", err)
	}

	if !health.Healthy {
		t.Error("expected healthy node")
	}
	if len(health.GPUHealth) != 8 {
		t.Errorf("expected 8 GPU health entries, got %d", len(health.GPUHealth))
	}
}

func TestMockProviderList(t *testing.T) {
	provider, _ := newTestProvider()
	ctx := context.Background()

	provider.Provision(ctx, NodeSpec{ID: "list-1", GPUModel: types.GPUModelH100, GPUCount: 8, GPUMemoryGB: 80, Region: "local-1", Rack: "rack-a"})
	provider.Provision(ctx, NodeSpec{ID: "list-2", GPUModel: types.GPUModelA100, GPUCount: 8, GPUMemoryGB: 80, Region: "local-1", Rack: "rack-b"})

	nodes, err := provider.List(ctx)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(nodes) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(nodes))
	}
}

func TestMockProviderName(t *testing.T) {
	provider, _ := newTestProvider()
	if provider.Name() != "mock" {
		t.Errorf("expected provider name 'mock', got %s", provider.Name())
	}
}

func TestMockProviderPowerCycle(t *testing.T) {
	provider, fleet := newTestProvider()
	ctx := context.Background()

	_, err := provider.Provision(ctx, NodeSpec{
		ID:       "cycle-node",
		GPUModel: types.GPUModelH100,
		GPUCount: 8,
		GPUMemoryGB: 80,
		Region:   "local-1",
		Rack:     "rack-a",
	})
	if err != nil {
		t.Fatalf("Provision failed: %v", err)
	}

	// Fail the node first
	fleet.FailNode(ctx, "cycle-node", "test failure")

	// Power cycle to recover
	if err := provider.PowerCycle(ctx, "cycle-node"); err != nil {
		t.Fatalf("PowerCycle failed: %v", err)
	}

	node, _ := fleet.GetNode("cycle-node")
	if node.Health != types.NodeHealthHealthy {
		t.Errorf("expected healthy after power cycle, got %s", node.Health)
	}
}
