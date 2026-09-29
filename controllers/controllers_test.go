package controllers

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/gpuflow/gpuflow/events"
	v1 "github.com/gpuflow/gpuflow/pkg/apis/gpuflow/v1"
	"github.com/gpuflow/gpuflow/pkg/types"
	"github.com/gpuflow/gpuflow/scheduler"
	"github.com/gpuflow/gpuflow/simulator"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

func setupTestEnvironment() (*simulator.FleetSimulator, *scheduler.Scheduler, events.Bus) {
	logger := testLogger()
	fleet := simulator.NewFleetSimulator(logger)
	bus := events.NewBus(logger)
	sched := scheduler.New(fleet, logger)
	return fleet, sched, bus
}

func TestGPUNodeReconcilerLifecycle(t *testing.T) {
	fleet, _, bus := setupTestEnvironment()
	logger := testLogger()
	reconciler := NewGPUNodeReconciler(fleet, bus, logger)

	node := &v1.GPUNode{
		Metadata: v1.ObjectMeta{Name: "test-node-01"},
		Spec: v1.GPUNodeSpec{
			Provider: "local",
			Region:   "us-west-1",
			Rack:     "rack-A1",
			GPU: v1.GPUResourceSpec{
				Model:    types.GPUModelH100,
				Count:    8,
				MemoryGB: 80,
			},
			Topology: v1.TopologySpec{Type: types.TopologyNVLink},
		},
	}

	reconciler.Register(node)
	ctx := context.Background()
	req := Request{Name: "test-node-01"}

	// Reconcile loop across state machine until READY
	for i := 0; i < 15; i++ {
		res, err := reconciler.Reconcile(ctx, req)
		if err != nil {
			t.Fatalf("unexpected error during reconcile: %v", err)
		}
		if !res.Requeue && res.RequeueAfter == 0 {
			break
		}
	}

	updated, ok := reconciler.Get("test-node-01")
	if !ok {
		t.Fatal("node not found")
	}

	if updated.Status.Phase != types.NodeStateReady {
		t.Fatalf("expected node to reach READY, got %s", updated.Status.Phase)
	}

	if len(updated.Status.StateTransitions) == 0 {
		t.Fatal("expected state transitions to be recorded")
	}

	if updated.Status.AvailableGPUs != 8 {
		t.Fatalf("expected 8 available GPUs, got %d", updated.Status.AvailableGPUs)
	}
}

func TestInferenceClusterReconcilerProvisionAndDrift(t *testing.T) {
	fleet, sched, bus := setupTestEnvironment()
	logger := testLogger()
	ctx := context.Background()

	// Pre-create 2 healthy H100 nodes (16 GPUs total)
	for _, name := range []string{"gpu-node-01", "gpu-node-02"} {
		_, _ = fleet.CreateNode(ctx, simulator.NodeSpec{
			ID:               name,
			Provider:         "local",
			Region:           "local-1",
			Rack:             "rack-1",
			GPUModel:         types.GPUModelH100,
			GPUCount:         8,
			GPUMemoryGB:      80,
			CPUCores:         32,
			RAMGB:            256,
			NetworkBandwidth: "400Gbps",
			Topology:         types.TopologyNVLink,
		})
		// Transition to READY so they are schedulable
		_ = fleet.TransitionNode(ctx, name, types.NodeStateProvisioning, "", "")
		_ = fleet.TransitionNode(ctx, name, types.NodeStateOSReady, "", "")
		_ = fleet.TransitionNode(ctx, name, types.NodeStateDriverInstall, "", "")
		_ = fleet.TransitionNode(ctx, name, types.NodeStateCUDAReady, "", "")
		_ = fleet.TransitionNode(ctx, name, types.NodeStateValidating, "", "")
		_ = fleet.TransitionNode(ctx, name, types.NodeStateReady, "", "")
	}

	reconciler := NewInferenceClusterReconciler(fleet, sched, bus, logger)

	cluster := &v1.InferenceCluster{
		Metadata: v1.ObjectMeta{Name: "llama-test"},
		Spec: v1.InferenceClusterSpec{
			Replicas: 2,
			Model:    v1.ModelSpec{Name: "llama-70b"},
			Runtime:  v1.RuntimeSpec{Name: "vllm"},
			Resources: v1.ResourceRequirements{
				GPU: v1.GPUResourceSpec{
					Model:    types.GPUModelH100,
					Count:    8,
					MemoryGB: 80,
				},
			},
			Scheduling: v1.SchedulingSpec{
				Topology: types.TopologyNVLink,
				Strategy: types.StrategyBinPack,
			},
		},
	}

	reconciler.Register(cluster)
	req := Request{Name: "llama-test"}

	// First reconcile: allocate both replicas
	_, err := reconciler.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("failed initial reconcile: %v", err)
	}

	updated, _ := reconciler.Get("llama-test")
	if updated.Status.Phase != types.ClusterPhaseReady {
		t.Fatalf("expected cluster phase READY, got %s", updated.Status.Phase)
	}
	if updated.Status.Replicas.Ready != 2 {
		t.Fatalf("expected 2 ready replicas, got %d", updated.Status.Replicas.Ready)
	}
	if updated.Status.AllocatedGPUs != 16 {
		t.Fatalf("expected 16 allocated GPUs, got %d", updated.Status.AllocatedGPUs)
	}

	// Now simulate failure of node-01 -> causes drift
	_ = fleet.FailNode(ctx, "gpu-node-01", "simulated hardware failure")

	// Add a replacement node so reconciler can heal
	_, _ = fleet.CreateNode(ctx, simulator.NodeSpec{
		ID:               "gpu-node-03",
		Provider:         "local",
		Region:           "local-1",
		Rack:             "rack-1",
		GPUModel:         types.GPUModelH100,
		GPUCount:         8,
		GPUMemoryGB:      80,
		CPUCores:         32,
		RAMGB:            256,
		NetworkBandwidth: "400Gbps",
		Topology:         types.TopologyNVLink,
	})
	_ = fleet.TransitionNode(ctx, "gpu-node-03", types.NodeStateProvisioning, "", "")
	_ = fleet.TransitionNode(ctx, "gpu-node-03", types.NodeStateOSReady, "", "")
	_ = fleet.TransitionNode(ctx, "gpu-node-03", types.NodeStateDriverInstall, "", "")
	_ = fleet.TransitionNode(ctx, "gpu-node-03", types.NodeStateCUDAReady, "", "")
	_ = fleet.TransitionNode(ctx, "gpu-node-03", types.NodeStateValidating, "", "")
	_ = fleet.TransitionNode(ctx, "gpu-node-03", types.NodeStateReady, "", "")

	// Reconcile again: drift should be detected and reconciled with replacement
	_, err = reconciler.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("failed reconcile after failure: %v", err)
	}

	updatedAfterDrift, _ := reconciler.Get("llama-test")
	if updatedAfterDrift.Status.Phase != types.ClusterPhaseReady {
		t.Fatalf("expected cluster to recover to READY, got %s", updatedAfterDrift.Status.Phase)
	}
	if updatedAfterDrift.Status.AllocatedGPUs != 16 {
		t.Fatalf("expected 16 allocated GPUs after replacement, got %d", updatedAfterDrift.Status.AllocatedGPUs)
	}
}

func TestInferenceClusterScaleDown(t *testing.T) {
	fleet, sched, bus := setupTestEnvironment()
	logger := testLogger()
	ctx := context.Background()

	for _, name := range []string{"gpu-node-01", "gpu-node-02"} {
		_, _ = fleet.CreateNode(ctx, simulator.NodeSpec{
			ID:               name,
			Provider:         "local",
			Region:           "local-1",
			Rack:             "rack-1",
			GPUModel:         types.GPUModelH100,
			GPUCount:         8,
			GPUMemoryGB:      80,
			CPUCores:         32,
			RAMGB:            256,
			NetworkBandwidth: "400Gbps",
			Topology:         types.TopologyNVLink,
		})
		_ = fleet.TransitionNode(ctx, name, types.NodeStateProvisioning, "", "")
		_ = fleet.TransitionNode(ctx, name, types.NodeStateOSReady, "", "")
		_ = fleet.TransitionNode(ctx, name, types.NodeStateDriverInstall, "", "")
		_ = fleet.TransitionNode(ctx, name, types.NodeStateCUDAReady, "", "")
		_ = fleet.TransitionNode(ctx, name, types.NodeStateValidating, "", "")
		_ = fleet.TransitionNode(ctx, name, types.NodeStateReady, "", "")
	}

	reconciler := NewInferenceClusterReconciler(fleet, sched, bus, logger)
	cluster := &v1.InferenceCluster{
		Metadata: v1.ObjectMeta{Name: "scale-test"},
		Spec: v1.InferenceClusterSpec{
			Replicas: 2,
			Model:    v1.ModelSpec{Name: "llama-70b"},
			Runtime:  v1.RuntimeSpec{Name: "vllm"},
			Resources: v1.ResourceRequirements{
				GPU: v1.GPUResourceSpec{
					Model:    types.GPUModelH100,
					Count:    8,
					MemoryGB: 80,
				},
			},
		},
	}

	reconciler.Register(cluster)
	req := Request{Name: "scale-test"}

	_, _ = reconciler.Reconcile(ctx, req)

	// Now scale down from 2 to 1
	cluster.Spec.Replicas = 1
	_, err := reconciler.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("failed scale down reconcile: %v", err)
	}

	updated, _ := reconciler.Get("scale-test")
	if updated.Status.Replicas.Ready != 1 {
		t.Fatalf("expected 1 ready replica after scale down, got %d", updated.Status.Replicas.Ready)
	}
	if updated.Status.AllocatedGPUs != 8 {
		t.Fatalf("expected 8 allocated GPUs, got %d", updated.Status.AllocatedGPUs)
	}
}
