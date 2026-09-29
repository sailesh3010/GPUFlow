package simulator

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"

	"github.com/gpuflow/gpuflow/pkg/types"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestCreateNode(t *testing.T) {
	fleet := NewFleetSimulator(testLogger())
	ctx := context.Background()

	node, err := fleet.CreateNode(ctx, DefaultH100Spec("test-node-01", "rack-a"))
	if err != nil {
		t.Fatalf("CreateNode failed: %v", err)
	}

	if node.ID != "test-node-01" {
		t.Errorf("expected ID test-node-01, got %s", node.ID)
	}
	if node.State != types.NodeStateDiscovered {
		t.Errorf("expected state DISCOVERED, got %s", node.State)
	}
	if len(node.GPUs) != 8 {
		t.Errorf("expected 8 GPUs, got %d", len(node.GPUs))
	}
	for _, gpu := range node.GPUs {
		if gpu.Model != types.GPUModelH100 {
			t.Errorf("expected GPU model H100, got %s", gpu.Model)
		}
		if gpu.MemoryGB != 80 {
			t.Errorf("expected 80GB memory, got %d", gpu.MemoryGB)
		}
		if gpu.Health != types.GPUHealthHealthy {
			t.Errorf("expected healthy GPU, got %s", gpu.Health)
		}
	}
}

func TestCreateDuplicateNode(t *testing.T) {
	fleet := NewFleetSimulator(testLogger())
	ctx := context.Background()

	_, err := fleet.CreateNode(ctx, DefaultH100Spec("dup-node", "rack-a"))
	if err != nil {
		t.Fatalf("first CreateNode failed: %v", err)
	}

	_, err = fleet.CreateNode(ctx, DefaultH100Spec("dup-node", "rack-a"))
	if err == nil {
		t.Error("expected error creating duplicate node")
	}
}

func TestDeleteNode(t *testing.T) {
	fleet := NewFleetSimulator(testLogger())
	ctx := context.Background()

	_, err := fleet.CreateNode(ctx, DefaultH100Spec("del-node", "rack-a"))
	if err != nil {
		t.Fatalf("CreateNode failed: %v", err)
	}

	if err := fleet.DeleteNode(ctx, "del-node"); err != nil {
		t.Fatalf("DeleteNode failed: %v", err)
	}

	_, err = fleet.GetNode("del-node")
	if err == nil {
		t.Error("expected error getting deleted node")
	}
}

func TestDeleteNodeWithAllocatedGPUs(t *testing.T) {
	fleet := NewFleetSimulator(testLogger())
	ctx := context.Background()

	_, err := fleet.CreateNode(ctx, DefaultH100Spec("busy-node", "rack-a"))
	if err != nil {
		t.Fatalf("CreateNode failed: %v", err)
	}

	// Provision the node so it becomes schedulable
	if err := fleet.ProvisionNode(ctx, "busy-node", ""); err != nil {
		t.Fatalf("ProvisionNode failed: %v", err)
	}

	_, err = fleet.AllocateGPUs(ctx, "busy-node", 2, "workload-1")
	if err != nil {
		t.Fatalf("AllocateGPUs failed: %v", err)
	}

	err = fleet.DeleteNode(ctx, "busy-node")
	if err == nil {
		t.Error("expected error deleting node with allocated GPUs")
	}
}

func TestNodeStateTransitions(t *testing.T) {
	fleet := NewFleetSimulator(testLogger())
	ctx := context.Background()

	_, err := fleet.CreateNode(ctx, DefaultH100Spec("trans-node", "rack-a"))
	if err != nil {
		t.Fatalf("CreateNode failed: %v", err)
	}

	// Valid provisioning path
	transitions := []types.NodeState{
		types.NodeStateProvisioning,
		types.NodeStateOSReady,
		types.NodeStateDriverInstall,
		types.NodeStateCUDAReady,
		types.NodeStateValidating,
		types.NodeStateReady,
	}

	for _, state := range transitions {
		if err := fleet.TransitionNode(ctx, "trans-node", state, "test", "corr-1"); err != nil {
			t.Fatalf("transition to %s failed: %v", state, err)
		}
	}

	node, _ := fleet.GetNode("trans-node")
	if node.State != types.NodeStateReady {
		t.Errorf("expected READY, got %s", node.State)
	}
}

func TestInvalidStateTransition(t *testing.T) {
	fleet := NewFleetSimulator(testLogger())
	ctx := context.Background()

	_, err := fleet.CreateNode(ctx, DefaultH100Spec("invalid-trans", "rack-a"))
	if err != nil {
		t.Fatalf("CreateNode failed: %v", err)
	}

	// Cannot go directly from DISCOVERED to READY
	err = fleet.TransitionNode(ctx, "invalid-trans", types.NodeStateReady, "test", "")
	if err == nil {
		t.Error("expected error for invalid transition DISCOVERED -> READY")
	}
}

func TestAllocateGPUs(t *testing.T) {
	fleet := NewFleetSimulator(testLogger())
	ctx := context.Background()

	_, err := fleet.CreateNode(ctx, DefaultH100Spec("alloc-node", "rack-a"))
	if err != nil {
		t.Fatalf("CreateNode failed: %v", err)
	}

	if err := fleet.ProvisionNode(ctx, "alloc-node", ""); err != nil {
		t.Fatalf("ProvisionNode failed: %v", err)
	}

	gpuIDs, err := fleet.AllocateGPUs(ctx, "alloc-node", 4, "workload-1")
	if err != nil {
		t.Fatalf("AllocateGPUs failed: %v", err)
	}
	if len(gpuIDs) != 4 {
		t.Errorf("expected 4 allocated GPUs, got %d", len(gpuIDs))
	}

	node, _ := fleet.GetNode("alloc-node")
	if node.AllocatedGPUs() != 4 {
		t.Errorf("expected 4 allocated, got %d", node.AllocatedGPUs())
	}
	if node.AvailableGPUs() != 4 {
		t.Errorf("expected 4 available, got %d", node.AvailableGPUs())
	}
	if node.State != types.NodeStateAllocated {
		t.Errorf("expected ALLOCATED state, got %s", node.State)
	}
}

func TestAllocateMoreThanAvailable(t *testing.T) {
	fleet := NewFleetSimulator(testLogger())
	ctx := context.Background()

	_, err := fleet.CreateNode(ctx, DefaultH100Spec("over-alloc", "rack-a"))
	if err != nil {
		t.Fatalf("CreateNode failed: %v", err)
	}
	fleet.ProvisionNode(ctx, "over-alloc", "")

	_, err = fleet.AllocateGPUs(ctx, "over-alloc", 10, "workload-big")
	if err == nil {
		t.Error("expected error allocating more GPUs than available")
	}
}

func TestAllocateOnUnschedulableNode(t *testing.T) {
	fleet := NewFleetSimulator(testLogger())
	ctx := context.Background()

	_, err := fleet.CreateNode(ctx, DefaultH100Spec("unsched-node", "rack-a"))
	if err != nil {
		t.Fatalf("CreateNode failed: %v", err)
	}

	// Node is in DISCOVERED state — not schedulable
	_, err = fleet.AllocateGPUs(ctx, "unsched-node", 1, "workload-1")
	if err == nil {
		t.Error("expected error allocating on unschedulable node")
	}
}

func TestReleaseGPUs(t *testing.T) {
	fleet := NewFleetSimulator(testLogger())
	ctx := context.Background()

	_, err := fleet.CreateNode(ctx, DefaultH100Spec("release-node", "rack-a"))
	if err != nil {
		t.Fatalf("CreateNode failed: %v", err)
	}
	fleet.ProvisionNode(ctx, "release-node", "")

	gpuIDs, _ := fleet.AllocateGPUs(ctx, "release-node", 4, "workload-1")

	if err := fleet.ReleaseGPUs(ctx, "release-node", gpuIDs); err != nil {
		t.Fatalf("ReleaseGPUs failed: %v", err)
	}

	node, _ := fleet.GetNode("release-node")
	if node.AllocatedGPUs() != 0 {
		t.Errorf("expected 0 allocated after release, got %d", node.AllocatedGPUs())
	}
	if node.State != types.NodeStateReady {
		t.Errorf("expected READY after full release, got %s", node.State)
	}
}

func TestFailNode(t *testing.T) {
	fleet := NewFleetSimulator(testLogger())
	ctx := context.Background()

	_, err := fleet.CreateNode(ctx, DefaultH100Spec("fail-node", "rack-a"))
	if err != nil {
		t.Fatalf("CreateNode failed: %v", err)
	}
	fleet.ProvisionNode(ctx, "fail-node", "")

	if err := fleet.FailNode(ctx, "fail-node", "test failure"); err != nil {
		t.Fatalf("FailNode failed: %v", err)
	}

	node, _ := fleet.GetNode("fail-node")
	if node.State != types.NodeStateFailed {
		t.Errorf("expected FAILED, got %s", node.State)
	}
	if node.Health != types.NodeHealthFailed {
		t.Errorf("expected health FAILED, got %s", node.Health)
	}
}

func TestFailGPU(t *testing.T) {
	fleet := NewFleetSimulator(testLogger())
	ctx := context.Background()

	node, _ := fleet.CreateNode(ctx, DefaultH100Spec("gpu-fail-node", "rack-a"))
	fleet.ProvisionNode(ctx, "gpu-fail-node", "")

	gpuID := node.GPUs[0].ID
	if err := fleet.FailGPU(ctx, "gpu-fail-node", gpuID); err != nil {
		t.Fatalf("FailGPU failed: %v", err)
	}

	updated, _ := fleet.GetNode("gpu-fail-node")
	if updated.GPUs[0].Health != types.GPUHealthFailed {
		t.Errorf("expected GPU health FAILED, got %s", updated.GPUs[0].Health)
	}
	if updated.Health != types.NodeHealthDegraded {
		t.Errorf("expected node DEGRADED after GPU failure, got %s", updated.Health)
	}
}

func TestRecoverNode(t *testing.T) {
	fleet := NewFleetSimulator(testLogger())
	ctx := context.Background()

	_, err := fleet.CreateNode(ctx, DefaultH100Spec("recover-node", "rack-a"))
	if err != nil {
		t.Fatalf("CreateNode failed: %v", err)
	}
	fleet.ProvisionNode(ctx, "recover-node", "")
	fleet.FailNode(ctx, "recover-node", "test failure")

	if err := fleet.RecoverNode(ctx, "recover-node"); err != nil {
		t.Fatalf("RecoverNode failed: %v", err)
	}

	node, _ := fleet.GetNode("recover-node")
	if node.State != types.NodeStateRepairing {
		t.Errorf("expected REPAIRING, got %s", node.State)
	}
	if node.Health != types.NodeHealthHealthy {
		t.Errorf("expected health HEALTHY after recovery, got %s", node.Health)
	}
}

func TestProvisionNodeLifecycle(t *testing.T) {
	fleet := NewFleetSimulator(testLogger())
	ctx := context.Background()

	_, err := fleet.CreateNode(ctx, DefaultH100Spec("prov-node", "rack-a"))
	if err != nil {
		t.Fatalf("CreateNode failed: %v", err)
	}

	if err := fleet.ProvisionNode(ctx, "prov-node", "test-correlation"); err != nil {
		t.Fatalf("ProvisionNode failed: %v", err)
	}

	node, _ := fleet.GetNode("prov-node")
	if node.State != types.NodeStateReady {
		t.Errorf("expected READY after provisioning, got %s", node.State)
	}

	// Verify state transitions were recorded
	transitions := fleet.GetNodeTransitions("prov-node")
	// Should be: "" -> DISCOVERED, DISCOVERED -> PROVISIONING, ... -> READY
	if len(transitions) < 7 {
		t.Errorf("expected at least 7 transitions, got %d", len(transitions))
	}
}

func TestFleetStats(t *testing.T) {
	fleet := NewFleetSimulator(testLogger())
	ctx := context.Background()

	fleet.CreateNode(ctx, DefaultH100Spec("stats-1", "rack-a"))
	fleet.CreateNode(ctx, DefaultA100Spec("stats-2", "rack-b"))
	fleet.ProvisionNode(ctx, "stats-1", "")
	fleet.ProvisionNode(ctx, "stats-2", "")

	fleet.AllocateGPUs(ctx, "stats-1", 4, "workload-1")

	stats := fleet.Stats()
	if stats.TotalNodes != 2 {
		t.Errorf("expected 2 nodes, got %d", stats.TotalNodes)
	}
	if stats.TotalGPUs != 16 {
		t.Errorf("expected 16 GPUs, got %d", stats.TotalGPUs)
	}
	if stats.AllocatedGPUs != 4 {
		t.Errorf("expected 4 allocated, got %d", stats.AllocatedGPUs)
	}
	if stats.AvailableGPUs != 12 {
		t.Errorf("expected 12 available, got %d", stats.AvailableGPUs)
	}
}

func TestCreateDefaultFleet(t *testing.T) {
	fleet := NewFleetSimulator(testLogger())
	ctx := context.Background()

	if err := fleet.CreateDefaultFleet(ctx); err != nil {
		t.Fatalf("CreateDefaultFleet failed: %v", err)
	}

	nodes := fleet.ListNodes()
	if len(nodes) != 4 {
		t.Errorf("expected 4 nodes, got %d", len(nodes))
	}

	stats := fleet.Stats()
	if stats.TotalGPUs != 32 {
		t.Errorf("expected 32 GPUs, got %d", stats.TotalGPUs)
	}
}

func TestConcurrentAccess(t *testing.T) {
	fleet := NewFleetSimulator(testLogger())
	ctx := context.Background()

	fleet.CreateNode(ctx, DefaultH100Spec("concurrent-1", "rack-a"))
	fleet.ProvisionNode(ctx, "concurrent-1", "")

	var wg sync.WaitGroup
	errCh := make(chan error, 20)

	// Concurrent reads
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := fleet.GetNode("concurrent-1")
			if err != nil {
				errCh <- err
			}
		}()
	}

	// Concurrent stats
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fleet.Stats()
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent access error: %v", err)
	}
}

func TestGPUAvailability(t *testing.T) {
	gpu := types.GPU{
		ID:       "test-gpu",
		Health:   types.GPUHealthHealthy,
		Allocated: false,
	}
	if !gpu.IsAvailable() {
		t.Error("healthy unallocated GPU should be available")
	}

	gpu.Allocated = true
	if gpu.IsAvailable() {
		t.Error("allocated GPU should not be available")
	}

	gpu.Allocated = false
	gpu.Health = types.GPUHealthFailed
	if gpu.IsAvailable() {
		t.Error("failed GPU should not be available")
	}
}

func TestStateTransitionValidation(t *testing.T) {
	tests := []struct {
		name    string
		from    types.NodeState
		to      types.NodeState
		wantErr bool
	}{
		{"discovered to provisioning", types.NodeStateDiscovered, types.NodeStateProvisioning, false},
		{"discovered to ready (skip)", types.NodeStateDiscovered, types.NodeStateReady, true},
		{"ready to allocated", types.NodeStateReady, types.NodeStateAllocated, false},
		{"ready to draining", types.NodeStateReady, types.NodeStateDraining, false},
		{"ready to failed", types.NodeStateReady, types.NodeStateFailed, false},
		{"failed to repairing", types.NodeStateFailed, types.NodeStateRepairing, false},
		{"failed to ready (skip)", types.NodeStateFailed, types.NodeStateReady, true},
		{"decommissioned to anything", types.NodeStateDecommissioned, types.NodeStateReady, true},
		{"repairing to validating", types.NodeStateRepairing, types.NodeStateValidating, false},
		{"draining to decommissioning", types.NodeStateDraining, types.NodeStateDecommissioning, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTransition(tt.from, tt.to)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateTransition(%s, %s) error = %v, wantErr %v", tt.from, tt.to, err, tt.wantErr)
			}
		})
	}
}
