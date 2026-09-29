package apiserver

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/gpuflow/gpuflow/controllers"
	"github.com/gpuflow/gpuflow/events"
	"github.com/gpuflow/gpuflow/health"
	v1 "github.com/gpuflow/gpuflow/pkg/apis/gpuflow/v1"
	"github.com/gpuflow/gpuflow/pkg/metrics"
	"github.com/gpuflow/gpuflow/pkg/types"
	"github.com/gpuflow/gpuflow/scheduler"
	"github.com/gpuflow/gpuflow/simulator"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

func setupTestServer() (*Server, *simulator.FleetSimulator) {
	log := testLogger()
	fleet := simulator.NewFleetSimulator(log)
	ctx := context.Background()

	_ = fleet.CreateDefaultFleet(ctx)
	for _, n := range fleet.ListNodes() {
		_ = fleet.ProvisionNode(ctx, n.ID, "setup")
	}

	bus := events.NewBus(log)
	sched := scheduler.New(fleet, log)
	detector := health.NewDetector(fleet, nil, log, 1*time.Minute)
	clusterCtrl := controllers.NewInferenceClusterReconciler(fleet, sched, bus, log)
	reg := metrics.NewRegistry(fleet, sched)

	srv := NewServer(fleet, sched, detector, bus, clusterCtrl, reg, log)
	return srv, fleet
}

func TestAPIServerNodes(t *testing.T) {
	srv, _ := setupTestServer()

	// List nodes
	req := httptest.NewRequest("GET", "/api/v1/nodes", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var nodes []types.Node
	_ = json.NewDecoder(rec.Body).Decode(&nodes)
	if len(nodes) != 4 {
		t.Fatalf("expected 4 nodes, got %d", len(nodes))
	}

	// Get single node
	req = httptest.NewRequest("GET", "/api/v1/nodes/gpu-node-01", nil)
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	// Fail node
	req = httptest.NewRequest("POST", "/api/v1/nodes/gpu-node-01/fail", nil)
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on fail node, got %d", rec.Code)
	}

	// Drain node
	req = httptest.NewRequest("POST", "/api/v1/nodes/gpu-node-02/drain", nil)
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on drain node, got %d", rec.Code)
	}
}

func TestAPIServerClusterLifecycle(t *testing.T) {
	srv, _ := setupTestServer()

	// Create cluster
	cluster := v1.InferenceCluster{
		Metadata: v1.ObjectMeta{Name: "api-test-cluster"},
		Spec: v1.InferenceClusterSpec{
			Replicas: 1,
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

	body, _ := json.Marshal(cluster)
	req := httptest.NewRequest("POST", "/api/v1/clusters", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d: %s", rec.Code, rec.Body.String())
	}

	// Get cluster
	req = httptest.NewRequest("GET", "/api/v1/clusters/api-test-cluster", nil)
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	// Scale cluster to 2
	scaleBody, _ := json.Marshal(ScaleRequest{Replicas: 2})
	req = httptest.NewRequest("POST", "/api/v1/clusters/api-test-cluster/scale", bytes.NewReader(scaleBody))
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on scale, got %d: %s", rec.Code, rec.Body.String())
	}

	// Delete cluster
	req = httptest.NewRequest("DELETE", "/api/v1/clusters/api-test-cluster", nil)
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on delete, got %d", rec.Code)
	}
}

func TestAPIServerSchedulerAndOptimization(t *testing.T) {
	srv, _ := setupTestServer()

	// Capacity
	req := httptest.NewRequest("GET", "/api/v1/scheduler/capacity", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on capacity, got %d", rec.Code)
	}

	// Fragmentation
	req = httptest.NewRequest("GET", "/api/v1/scheduler/fragmentation", nil)
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on fragmentation, got %d", rec.Code)
	}

	// Optimization plan
	req = httptest.NewRequest("POST", "/api/v1/optimization/plan", nil)
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on optimization plan, got %d", rec.Code)
	}

	// Metrics endpoint
	req = httptest.NewRequest("GET", "/metrics", nil)
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on metrics, got %d", rec.Code)
	}
}
