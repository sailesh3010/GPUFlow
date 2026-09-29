package workflows

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gpuflow/gpuflow/pkg/types"
	"github.com/gpuflow/gpuflow/simulator"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

func TestProvisionNodeWorkflowExecution(t *testing.T) {
	logger := testLogger()
	fleet := simulator.NewFleetSimulator(logger)
	engine := NewEngine(logger)
	wf := NewProvisionNodeWorkflow(engine, fleet)

	ctx := context.Background()
	input := ProvisionNodeInput{
		NodeID:      "test-wf-node-01",
		Provider:    "local",
		Region:      "local-1",
		Rack:        "rack-1",
		GPUModel:    types.GPUModelH100,
		GPUCount:    8,
		GPUMemoryGB: 80,
		Topology:    types.TopologyNVLink,
	}

	exec, err := wf.Execute(ctx, input)
	if err != nil {
		t.Fatalf("workflow execution failed: %v", err)
	}

	if exec.Status != StatusCompleted {
		t.Fatalf("expected workflow status COMPLETED, got %s", exec.Status)
	}

	if len(exec.History) != 8 {
		t.Fatalf("expected 8 activity records in history, got %d", len(exec.History))
	}

	node, err := fleet.GetNode("test-wf-node-01")
	if err != nil {
		t.Fatalf("failed to get node after workflow: %v", err)
	}

	if node.State != types.NodeStateReady {
		t.Fatalf("expected node to be READY, got %s", node.State)
	}
}

func TestWorkflowIdempotency(t *testing.T) {
	logger := testLogger()
	fleet := simulator.NewFleetSimulator(logger)
	engine := NewEngine(logger)
	wf := NewProvisionNodeWorkflow(engine, fleet)

	ctx := context.Background()
	input := ProvisionNodeInput{
		NodeID:         "idemp-node-01",
		Provider:       "local",
		GPUModel:       types.GPUModelH100,
		GPUCount:       8,
		IdempotencyKey: "unique-key-12345",
	}

	exec1, err := wf.Execute(ctx, input)
	if err != nil {
		t.Fatalf("first execution failed: %v", err)
	}

	// Second execution with same idempotency key
	exec2, err := wf.Execute(ctx, input)
	if err != nil {
		t.Fatalf("second execution failed: %v", err)
	}

	if exec1.WorkflowID != exec2.WorkflowID {
		t.Fatalf("expected same workflow ID due to idempotency key: %s != %s", exec1.WorkflowID, exec2.WorkflowID)
	}
}

func TestActivityRetryWithBackoff(t *testing.T) {
	logger := testLogger()
	engine := NewEngine(logger)

	var attempts int32
	engine.RegisterActivity("FlakyActivity", func(ctx context.Context, in map[string]interface{}) (map[string]interface{}, error) {
		att := atomic.AddInt32(&attempts, 1)
		if att < 3 {
			return nil, fmt.Errorf("transient network timeout")
		}
		return map[string]interface{}{"status": "ok"}, nil
	})

	exec, _ := engine.StartWorkflow("wf-flaky", "TestWF", "k1", nil)
	opts := ActivityOptions{
		MaxAttempts:   4,
		InitialDelay:  5 * time.Millisecond,
		BackoffFactor: 1.5,
		Timeout:       1 * time.Second,
	}

	res, err := engine.ExecuteActivity(context.Background(), exec, "FlakyActivity", opts, nil)
	if err != nil {
		t.Fatalf("activity failed after retries: %v", err)
	}

	if res["status"] != "ok" {
		t.Fatalf("expected status ok, got %v", res["status"])
	}

	if atomic.LoadInt32(&attempts) != 3 {
		t.Fatalf("expected exactly 3 attempts, got %d", attempts)
	}
}

func TestWorkerRestartAndResumeSimulation(t *testing.T) {
	logger := testLogger()
	fleet := simulator.NewFleetSimulator(logger)
	ctx := context.Background()

	// Pre-create node
	_, _ = fleet.CreateNode(ctx, simulator.NodeSpec{
		ID:          "crash-node-01",
		Provider:    "local",
		GPUModel:    types.GPUModelH100,
		GPUCount:    8,
		GPUMemoryGB: 80,
	})

	engine := NewEngine(logger)
	_ = NewProvisionNodeWorkflow(engine, fleet)

	workflowID := "wf-crash-test"
	exec, _ := engine.StartWorkflow(workflowID, "ProvisionNodeWorkflow", "crash-key", map[string]interface{}{
		"nodeId": "crash-node-01",
	})

	opts := DefaultActivityOptions()

	// Execute first 3 activities
	for _, act := range []string{"DiscoverNode", "ProvisionOS", "InstallDriver"} {
		_, err := engine.ExecuteActivity(ctx, exec, act, opts, map[string]interface{}{
			"nodeId":        "crash-node-01",
			"correlationId": "crash-corr",
		})
		if err != nil {
			t.Fatalf("activity %s failed: %v", act, err)
		}
	}

	if len(exec.History) != 3 {
		t.Fatalf("expected 3 completed steps before crash, got %d", len(exec.History))
	}

	// SIMULATE WORKER CRASH: Worker dies.
	// New worker comes up, fetches checkpointed execution from persistence, and registers activities again.
	newWorkerEngine := engine // shares execution store
	newWf := NewProvisionNodeWorkflow(newWorkerEngine, fleet)

	// Resume execution: run all 8 activities. The first 3 should be skipped via checkpoint replay!
	execResumed, _ := newWorkerEngine.GetExecution(workflowID)
	allActivities := []string{
		"DiscoverNode",
		"ProvisionOS",
		"InstallDriver",
		"InstallCUDA",
		"ConfigureNetwork",
		"ValidateGPU",
		"ValidateNetwork",
		"RegisterNode",
	}

	for _, act := range allActivities {
		_, err := newWorkerEngine.ExecuteActivity(ctx, execResumed, act, opts, map[string]interface{}{
			"nodeId":        "crash-node-01",
			"correlationId": "crash-corr",
		})
		if err != nil {
			t.Fatalf("resumed activity %s failed: %v", act, err)
		}
	}

	newWf.engine.Complete(execResumed, map[string]interface{}{"status": "READY"})

	if execResumed.Status != StatusCompleted {
		t.Fatalf("expected resumed workflow to complete, got %s", execResumed.Status)
	}

	// Verify total distinct activity records is 8 (the 3 were replayed, 5 newly executed)
	if len(execResumed.History) != 8 {
		t.Fatalf("expected exactly 8 activity records after resumption, got %d", len(execResumed.History))
	}
}

func TestRepairAndDecommissionWorkflows(t *testing.T) {
	logger := testLogger()
	fleet := simulator.NewFleetSimulator(logger)
	engine := NewEngine(logger)
	ctx := context.Background()

	// Create and fail a node
	_, _ = fleet.CreateNode(ctx, simulator.NodeSpec{
		ID:          "repair-test-node",
		Provider:    "local",
		GPUModel:    types.GPUModelH100,
		GPUCount:    8,
		GPUMemoryGB: 80,
	})
	_ = fleet.TransitionNode(ctx, "repair-test-node", types.NodeStateProvisioning, "", "")
	_ = fleet.TransitionNode(ctx, "repair-test-node", types.NodeStateOSReady, "", "")
	_ = fleet.TransitionNode(ctx, "repair-test-node", types.NodeStateDriverInstall, "", "")
	_ = fleet.TransitionNode(ctx, "repair-test-node", types.NodeStateCUDAReady, "", "")
	_ = fleet.TransitionNode(ctx, "repair-test-node", types.NodeStateValidating, "", "")
	_ = fleet.TransitionNode(ctx, "repair-test-node", types.NodeStateReady, "", "")

	_ = fleet.FailNode(ctx, "repair-test-node", "simulated GPU defect")

	// Execute RepairNodeWorkflow
	repairWf := NewRepairNodeWorkflow(engine, fleet)
	repairExec, err := repairWf.Execute(ctx, RepairNodeInput{
		NodeID: "repair-test-node",
		Reason: "GPU defect",
	})
	if err != nil {
		t.Fatalf("repair workflow failed: %v", err)
	}

	if repairExec.Status != StatusCompleted {
		t.Fatalf("expected repair workflow to complete, got %s", repairExec.Status)
	}

	healedNode, _ := fleet.GetNode("repair-test-node")
	if healedNode.State != types.NodeStateReady {
		t.Fatalf("expected healed node to be READY, got %s", healedNode.State)
	}

	// Execute DecommissionNodeWorkflow
	decomWf := NewDecommissionNodeWorkflow(engine, fleet)
	decomExec, err := decomWf.Execute(ctx, DecommissionNodeInput{
		NodeID: "repair-test-node",
		Reason: "scheduled hardware upgrade",
	})
	if err != nil {
		t.Fatalf("decommission workflow failed: %v", err)
	}

	if decomExec.Status != StatusCompleted {
		t.Fatalf("expected decommission workflow to complete, got %s", decomExec.Status)
	}

	decomNode, _ := fleet.GetNode("repair-test-node")
	if decomNode.State != types.NodeStateDecommissioned {
		t.Fatalf("expected decommissioned node, got %s", decomNode.State)
	}
}
