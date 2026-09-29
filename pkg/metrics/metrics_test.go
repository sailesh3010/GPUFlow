package metrics

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gpuflow/gpuflow/scheduler"
	"github.com/gpuflow/gpuflow/simulator"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

func TestPrometheusMetricsExposition(t *testing.T) {
	log := testLogger()
	fleet := simulator.NewFleetSimulator(log)
	ctx := context.Background()

	_ = fleet.CreateDefaultFleet(ctx)
	for _, n := range fleet.ListNodes() {
		_ = fleet.ProvisionNode(ctx, n.ID, "test")
	}

	sched := scheduler.New(fleet, log)
	reg := NewRegistry(fleet, sched)

	// Record sample metrics
	reg.RecordReconciliation(true)
	reg.RecordReconciliation(false)
	reg.RecordRepair()
	reg.RecordWorkflowDuration("ProvisionNodeWorkflow", 250*time.Millisecond)

	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()

	reg.Handler().ServeHTTP(rec, req)

	resp := rec.Result()
	if resp.StatusCode != 200 {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	content := string(body)

	requiredMetrics := []string{
		"gpuflow_gpu_utilization",
		"gpuflow_gpu_allocated",
		"gpuflow_gpu_free",
		"gpuflow_node_health",
		"gpuflow_node_state",
		"gpuflow_scheduler_placement_total",
		"gpuflow_scheduler_placement_failures",
		"gpuflow_fragmentation_score",
		"gpuflow_reconciliation_total",
		"gpuflow_reconciliation_errors",
		"gpuflow_workflow_duration",
		"gpuflow_repair_total",
	}

	for _, m := range requiredMetrics {
		if !strings.Contains(content, m) {
			t.Errorf("missing required metric: %s", m)
		}
	}
}
