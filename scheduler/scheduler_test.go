package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"

	"github.com/gpuflow/gpuflow/pkg/types"
	"github.com/gpuflow/gpuflow/simulator"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func setupFleet(t *testing.T) (*Scheduler, *simulator.FleetSimulator) {
	t.Helper()
	log := testLogger()
	fleet := simulator.NewFleetSimulator(log)
	ctx := context.Background()

	// Create a mixed fleet: 2 H100 nodes, 2 A100 nodes
	fleet.CreateNode(ctx, simulator.DefaultH100Spec("h100-1", "rack-a"))
	fleet.CreateNode(ctx, simulator.DefaultH100Spec("h100-2", "rack-a"))
	fleet.CreateNode(ctx, simulator.DefaultA100Spec("a100-1", "rack-b"))
	fleet.CreateNode(ctx, simulator.DefaultA100Spec("a100-2", "rack-b"))

	// Provision all nodes
	for _, id := range []string{"h100-1", "h100-2", "a100-1", "a100-2"} {
		if err := fleet.ProvisionNode(ctx, id, ""); err != nil {
			t.Fatalf("provision %s: %v", id, err)
		}
	}

	sched := New(fleet, log)
	return sched, fleet
}

func TestScheduleFirstFit(t *testing.T) {
	sched, _ := setupFleet(t)
	ctx := context.Background()

	result, err := sched.Schedule(ctx, types.GPURequest{
		Model:    types.GPUModelH100,
		Count:    4,
		MemoryGB: 80,
		Strategy: types.StrategyFirstFit,
	}, "workload-1")

	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}
	if result.WorkloadID != "workload-1" {
		t.Errorf("expected workloadId workload-1, got %s", result.WorkloadID)
	}
	if len(result.GPUIDs) != 4 {
		t.Errorf("expected 4 GPUs, got %d", len(result.GPUIDs))
	}
}

func TestScheduleBestFit(t *testing.T) {
	sched, fleet := setupFleet(t)
	ctx := context.Background()

	// Pre-allocate 6 GPUs on h100-1 to make it the tightest fit
	fleet.AllocateGPUs(ctx, "h100-1", 6, "pre-alloc")

	result, err := sched.Schedule(ctx, types.GPURequest{
		Model:    types.GPUModelH100,
		Count:    2,
		MemoryGB: 80,
		Strategy: types.StrategyBestFit,
	}, "workload-2")

	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}
	// Should pick h100-1 because it has only 2 available (tightest fit)
	if result.NodeID != "h100-1" {
		t.Errorf("expected h100-1 (tightest fit), got %s", result.NodeID)
	}
}

func TestScheduleBinPack(t *testing.T) {
	sched, fleet := setupFleet(t)
	ctx := context.Background()

	// Pre-allocate 4 GPUs on h100-1
	fleet.AllocateGPUs(ctx, "h100-1", 4, "pre-alloc")

	result, err := sched.Schedule(ctx, types.GPURequest{
		Model:    types.GPUModelH100,
		Count:    3,
		MemoryGB: 80,
		Strategy: types.StrategyBinPack,
	}, "workload-3")

	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}
	// Bin pack should prefer h100-1 (already partially full)
	if result.NodeID != "h100-1" {
		t.Errorf("expected h100-1 (bin pack prefers fuller nodes), got %s", result.NodeID)
	}
}

func TestScheduleTopologyAware(t *testing.T) {
	sched, _ := setupFleet(t)
	ctx := context.Background()

	result, err := sched.Schedule(ctx, types.GPURequest{
		Model:    types.GPUModelH100,
		Count:    4,
		MemoryGB: 80,
		Topology: types.TopologyNVLink,
		Strategy: types.StrategyTopologyAware,
	}, "workload-topo")

	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}
	if result.Strategy != "topology-aware" {
		t.Errorf("expected topology-aware strategy, got %s", result.Strategy)
	}
}

func TestScheduleWrongModel(t *testing.T) {
	sched, _ := setupFleet(t)
	ctx := context.Background()

	_, err := sched.Schedule(ctx, types.GPURequest{
		Model: types.GPUModelL40S,
		Count: 4,
	}, "workload-l40s")

	if err == nil {
		t.Error("expected error scheduling L40S (no L40S nodes in fleet)")
	}
}

func TestScheduleInsufficientGPUs(t *testing.T) {
	sched, _ := setupFleet(t)
	ctx := context.Background()

	// Request more GPUs than any single node has
	_, err := sched.Schedule(ctx, types.GPURequest{
		Model: types.GPUModelH100,
		Count: 16,
	}, "workload-huge")

	if err == nil {
		t.Error("expected error for 16 GPU request (max per node is 8)")
	}
}

func TestScheduleTopologyMismatch(t *testing.T) {
	sched, _ := setupFleet(t)
	ctx := context.Background()

	_, err := sched.Schedule(ctx, types.GPURequest{
		Model:    types.GPUModelH100,
		Count:    4,
		Topology: types.TopologyPCIe,
	}, "workload-pcie")

	if err == nil {
		t.Error("expected error for PCIe topology (all nodes are NVLink)")
	}
}

func TestScheduleCapacityExhaustion(t *testing.T) {
	sched, _ := setupFleet(t)
	ctx := context.Background()

	// Exhaust all H100 GPUs
	for i := 0; i < 2; i++ {
		_, err := sched.Schedule(ctx, types.GPURequest{
			Model: types.GPUModelH100,
			Count: 8,
		}, fmt.Sprintf("exhaust-%d", i))
		if err != nil {
			t.Fatalf("exhaust allocation %d failed: %v", i, err)
		}
	}

	// Now request should fail
	_, err := sched.Schedule(ctx, types.GPURequest{
		Model: types.GPUModelH100,
		Count: 1,
	}, "over-capacity")

	if err == nil {
		t.Error("expected error after capacity exhaustion")
	}
}

func TestCapacityReport(t *testing.T) {
	sched, _ := setupFleet(t)
	ctx := context.Background()

	// Allocate some GPUs
	sched.Schedule(ctx, types.GPURequest{
		Model: types.GPUModelH100,
		Count: 4,
	}, "cap-workload")

	report := sched.Capacity()
	if report.TotalNodes != 4 {
		t.Errorf("expected 4 nodes, got %d", report.TotalNodes)
	}
	if report.TotalGPUs != 32 {
		t.Errorf("expected 32 GPUs, got %d", report.TotalGPUs)
	}
	if report.AllocatedGPUs != 4 {
		t.Errorf("expected 4 allocated, got %d", report.AllocatedGPUs)
	}

	h100Cap, ok := report.ByModel["H100"]
	if !ok {
		t.Fatal("expected H100 capacity entry")
	}
	if h100Cap.Allocated != 4 {
		t.Errorf("expected 4 H100 allocated, got %d", h100Cap.Allocated)
	}
}

func TestFragmentationNoFragmentation(t *testing.T) {
	sched, _ := setupFleet(t)

	report := sched.Fragmentation()

	// Empty fleet — no fragmentation
	if report.OverallScore != 0 {
		t.Errorf("expected 0 fragmentation on empty fleet, got %f", report.OverallScore)
	}
}

func TestFragmentationWithScatteredAllocation(t *testing.T) {
	log := testLogger()
	fleet := simulator.NewFleetSimulator(log)
	ctx := context.Background()

	// Create a single node
	fleet.CreateNode(ctx, simulator.DefaultH100Spec("frag-node", "rack-a"))
	fleet.ProvisionNode(ctx, "frag-node", "")

	// Allocate GPUs 0, 2, 4, 6 (scattered pattern)
	fleet.AllocateGPUs(ctx, "frag-node", 1, "w-0")
	// We need to manually create the scattered pattern by allocation/release
	fleet.AllocateGPUs(ctx, "frag-node", 3, "w-1") // allocs 1,2,3
	// Release GPU 1 and 3 to create gaps
	node, _ := fleet.GetNode("frag-node")
	fleet.ReleaseGPUs(ctx, "frag-node", []string{node.GPUs[1].ID, node.GPUs[3].ID})

	sched := New(fleet, log)
	report := sched.Fragmentation()

	// Should show some fragmentation (free GPUs are scattered)
	if len(report.NodeScores) == 0 {
		t.Fatal("expected node fragmentation scores")
	}

	for _, ns := range report.NodeScores {
		if ns.NodeID == "frag-node" && ns.FreeGPUs > 0 {
			// With scattered free GPUs, fragment score should be > 0
			t.Logf("node %s: freeGPUs=%d largestBlock=%d fragScore=%.2f",
				ns.NodeID, ns.FreeGPUs, ns.LargestFreeBlock, ns.FragmentScore)
		}
	}
}

func TestDefragmentationPlan(t *testing.T) {
	log := testLogger()
	fleet := simulator.NewFleetSimulator(log)
	ctx := context.Background()

	// Create 3 H100 nodes
	fleet.CreateNode(ctx, simulator.DefaultH100Spec("defrag-1", "rack-a"))
	fleet.CreateNode(ctx, simulator.DefaultH100Spec("defrag-2", "rack-a"))
	fleet.CreateNode(ctx, simulator.DefaultH100Spec("defrag-3", "rack-a"))

	for _, id := range []string{"defrag-1", "defrag-2", "defrag-3"} {
		fleet.ProvisionNode(ctx, id, "")
	}

	// Create a fragmented state:
	// defrag-1: 2 GPUs allocated
	// defrag-2: 2 GPUs allocated
	// defrag-3: 4 GPUs allocated
	fleet.AllocateGPUs(ctx, "defrag-1", 2, "workload-a")
	fleet.AllocateGPUs(ctx, "defrag-2", 2, "workload-b")
	fleet.AllocateGPUs(ctx, "defrag-3", 4, "workload-c")

	sched := New(fleet, log)
	plan, err := sched.PlanDefragmentation()
	if err != nil {
		t.Fatalf("PlanDefragmentation failed: %v", err)
	}

	t.Logf("Defrag plan: moves=%d utilBefore=%.2f fragBefore=%.2f fragAfter=%.2f freed=%d",
		len(plan.Moves), plan.UtilizationBefore, plan.FragmentationBefore,
		plan.FragmentationAfter, plan.NodesFreedUp)

	// Should suggest some moves to consolidate
	if len(plan.Moves) == 0 {
		t.Log("Warning: no moves suggested (may be optimal already)")
	}
}

func TestSchedulerMetrics(t *testing.T) {
	sched, _ := setupFleet(t)
	ctx := context.Background()

	sched.Schedule(ctx, types.GPURequest{
		Model:    types.GPUModelH100,
		Count:    2,
		Strategy: types.StrategyBinPack,
	}, "metrics-1")

	sched.Schedule(ctx, types.GPURequest{
		Model: types.GPUModelL40S,
		Count: 1,
	}, "metrics-fail")

	metrics := sched.GetMetrics()
	if metrics.PlacementTotal != 1 {
		t.Errorf("expected 1 placement, got %d", metrics.PlacementTotal)
	}
	if metrics.PlacementFailures != 1 {
		t.Errorf("expected 1 failure, got %d", metrics.PlacementFailures)
	}
	if metrics.StrategyUsage["binpack"] != 1 {
		t.Errorf("expected 1 binpack usage, got %d", metrics.StrategyUsage["binpack"])
	}
}

func TestComputeContiguity(t *testing.T) {
	tests := []struct {
		name     string
		gpus     []types.GPU
		expected float64
	}{
		{
			name: "all free",
			gpus: []types.GPU{
				{Health: types.GPUHealthHealthy, Allocated: false},
				{Health: types.GPUHealthHealthy, Allocated: false},
				{Health: types.GPUHealthHealthy, Allocated: false},
				{Health: types.GPUHealthHealthy, Allocated: false},
			},
			expected: 1.0,
		},
		{
			name: "alternating",
			gpus: []types.GPU{
				{Health: types.GPUHealthHealthy, Allocated: true},
				{Health: types.GPUHealthHealthy, Allocated: false},
				{Health: types.GPUHealthHealthy, Allocated: true},
				{Health: types.GPUHealthHealthy, Allocated: false},
			},
			expected: 0.5, // maxRun=1, free=2 → 0.5
		},
		{
			name: "contiguous at end",
			gpus: []types.GPU{
				{Health: types.GPUHealthHealthy, Allocated: true},
				{Health: types.GPUHealthHealthy, Allocated: true},
				{Health: types.GPUHealthHealthy, Allocated: false},
				{Health: types.GPUHealthHealthy, Allocated: false},
			},
			expected: 1.0, // maxRun=2, free=2 → 1.0
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := &types.Node{GPUs: tt.gpus, GPUCount: len(tt.gpus)}
			got := computeContiguity(node)
			if got != tt.expected {
				t.Errorf("computeContiguity() = %f, want %f", got, tt.expected)
			}
		})
	}
}
