package health

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/gpuflow/gpuflow/pkg/types"
	"github.com/gpuflow/gpuflow/simulator"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestDetectNodeFailure(t *testing.T) {
	log := testLogger()
	fleet := simulator.NewFleetSimulator(log)
	ctx := context.Background()

	fleet.CreateNode(ctx, simulator.DefaultH100Spec("health-1", "rack-a"))
	fleet.ProvisionNode(ctx, "health-1", "")

	var remediatedNodes []string
	handler := func(ctx context.Context, nodeID string, issue HealthIssue) error {
		remediatedNodes = append(remediatedNodes, nodeID)
		return nil
	}

	detector := NewDetector(fleet, handler, log, 1*time.Hour) // long interval, we'll check manually

	// Fail the node
	fleet.FailNode(ctx, "health-1", "test failure")

	// Run check
	issues := detector.CheckNow(ctx)
	if len(issues) == 0 {
		t.Error("expected health issues after node failure")
	}

	foundFailed := false
	for _, issue := range issues {
		if issue.Type == IssueNodeFailed {
			foundFailed = true
		}
	}
	if !foundFailed {
		t.Error("expected NODE_FAILED issue")
	}

	if len(remediatedNodes) == 0 {
		t.Error("expected remediation handler to be called")
	}
}

func TestDetectGPUFailure(t *testing.T) {
	log := testLogger()
	fleet := simulator.NewFleetSimulator(log)
	ctx := context.Background()

	node, _ := fleet.CreateNode(ctx, simulator.DefaultH100Spec("gpu-health", "rack-a"))
	fleet.ProvisionNode(ctx, "gpu-health", "")

	// Fail a single GPU
	fleet.FailGPU(ctx, "gpu-health", node.GPUs[0].ID)

	detector := NewDetector(fleet, nil, log, 1*time.Hour)
	issues := detector.CheckNow(ctx)

	hasGPUFailure := false
	for _, issue := range issues {
		if issue.Type == IssueGPUFailure {
			hasGPUFailure = true
		}
	}
	if !hasGPUFailure {
		t.Error("expected GPU_FAILURE issue")
	}
}

func TestActiveIssuesCleared(t *testing.T) {
	log := testLogger()
	fleet := simulator.NewFleetSimulator(log)
	ctx := context.Background()

	fleet.CreateNode(ctx, simulator.DefaultH100Spec("clear-node", "rack-a"))
	fleet.ProvisionNode(ctx, "clear-node", "")

	detector := NewDetector(fleet, nil, log, 1*time.Hour)

	// No issues on healthy node
	issues := detector.CheckNow(ctx)
	if len(issues) != 0 {
		t.Errorf("expected no issues on healthy node, got %d", len(issues))
	}

	// Fail and detect
	fleet.FailNode(ctx, "clear-node", "test")
	issues = detector.CheckNow(ctx)
	if len(issues) == 0 {
		t.Error("expected issues after failure")
	}

	// Recover and check — active issues should clear
	fleet.RecoverNode(ctx, "clear-node")
	fleet.TransitionNode(ctx, "clear-node", types.NodeStateValidating, "recovery", "")
	fleet.TransitionNode(ctx, "clear-node", types.NodeStateReady, "recovered", "")

	detector.CheckNow(ctx) // this triggers cleanup
	active := detector.ActiveIssues()
	if len(active) != 0 {
		t.Errorf("expected cleared issues after recovery, still have %d", len(active))
	}
}
