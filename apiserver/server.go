// Package apiserver provides the REST API for operational access to GPUFlow.
package apiserver

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
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

// Server is the GPUFlow REST API server.
type Server struct {
	fleet       *simulator.FleetSimulator
	scheduler   *scheduler.Scheduler
	detector    *health.Detector
	eventBus    events.Bus
	clusterCtrl *controllers.InferenceClusterReconciler
	metricsReg  *metrics.Registry
	log         *slog.Logger
	mux         *http.ServeMux
	server      *http.Server
}

// NewServer creates a new API server.
func NewServer(
	fleet *simulator.FleetSimulator,
	sched *scheduler.Scheduler,
	detector *health.Detector,
	eventBus events.Bus,
	clusterCtrl *controllers.InferenceClusterReconciler,
	metricsReg *metrics.Registry,
	log *slog.Logger,
) *Server {
	s := &Server{
		fleet:       fleet,
		scheduler:   sched,
		detector:    detector,
		eventBus:    eventBus,
		clusterCtrl: clusterCtrl,
		metricsReg:  metricsReg,
		log:         log,
		mux:         http.NewServeMux(),
	}
	s.registerRoutes()
	return s
}

func (s *Server) registerRoutes() {
	// Prometheus metrics
	if s.metricsReg != nil {
		s.mux.Handle("GET /metrics", s.metricsReg.Handler())
	}

	// Cluster endpoints
	s.mux.HandleFunc("GET /api/v1/clusters", s.handleListClusters)
	s.mux.HandleFunc("GET /api/v1/clusters/{name}", s.handleGetCluster)
	s.mux.HandleFunc("POST /api/v1/clusters", s.handleCreateCluster)
	s.mux.HandleFunc("POST /api/v1/clusters/{name}/scale", s.handleScaleCluster)
	s.mux.HandleFunc("DELETE /api/v1/clusters/{name}", s.handleDeleteCluster)

	// Node endpoints
	s.mux.HandleFunc("GET /api/v1/nodes", s.handleListNodes)
	s.mux.HandleFunc("GET /api/v1/nodes/{id}", s.handleGetNode)
	s.mux.HandleFunc("POST /api/v1/nodes/{id}/fail", s.handleFailNode)
	s.mux.HandleFunc("POST /api/v1/nodes/{id}/drain", s.handleDrainNode)

	// Scheduler endpoints
	s.mux.HandleFunc("GET /api/v1/scheduler/capacity", s.handleCapacity)
	s.mux.HandleFunc("GET /api/v1/scheduler/fragmentation", s.handleFragmentation)

	// Optimization endpoints
	s.mux.HandleFunc("POST /api/v1/optimization/plan", s.handleOptimizationPlan)
	s.mux.HandleFunc("POST /api/v1/optimization/apply", s.handleOptimizationApply)

	// Health endpoints
	s.mux.HandleFunc("GET /api/v1/health", s.handleHealthCheck)
	s.mux.HandleFunc("GET /api/v1/health/issues", s.handleHealthIssues)

	// Fleet stats
	s.mux.HandleFunc("GET /api/v1/fleet/stats", s.handleFleetStats)
}

// Start begins serving HTTP requests.
func (s *Server) Start(addr string) error {
	s.server = &http.Server{
		Addr:         addr,
		Handler:      s.loggingMiddleware(s.mux),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	s.log.Info("API server starting", slog.String("addr", addr))
	return s.server.ListenAndServe()
}

// Shutdown gracefully stops the server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

// Handler returns the HTTP handler for testing.
func (s *Server) Handler() http.Handler {
	return s.mux
}

// ─── Middleware ───

func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		s.log.Info("request",
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Duration("duration", time.Since(start)),
		)
	})
}

// ─── Response Helpers ───

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// ─── Node Handlers ───

func (s *Server) handleListNodes(w http.ResponseWriter, _ *http.Request) {
	nodes := s.fleet.ListNodes()
	writeJSON(w, http.StatusOK, nodes)
}

func (s *Server) handleGetNode(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	node, err := s.fleet.GetNode(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, node)
}

func (s *Server) handleFailNode(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	if err := s.fleet.FailNode(r.Context(), id, "API: manual failure injection"); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Publish event
	if s.eventBus != nil {
		_ = s.eventBus.Publish(r.Context(), events.TopicNodeEvents, types.Event{
			EventID:   fmt.Sprintf("evt-%d", time.Now().UnixNano()),
			EventType: types.EventNodeFailed,
			Timestamp: time.Now(),
			NodeID:    id,
		})
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"status": "node marked as FAILED",
		"nodeId": id,
	})
}

func (s *Server) handleDrainNode(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	if err := s.fleet.TransitionNode(r.Context(), id, types.NodeStateDraining, "API: manual drain", ""); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"status": "node draining initiated",
		"nodeId": id,
	})
}

// ─── Cluster Handlers ───

type ScaleRequest struct {
	Replicas int `json:"replicas"`
}

func (s *Server) handleCreateCluster(w http.ResponseWriter, r *http.Request) {
	var cluster v1.InferenceCluster
	if err := json.NewDecoder(r.Body).Decode(&cluster); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if cluster.Metadata.Name == "" {
		writeError(w, http.StatusBadRequest, "metadata.name is required")
		return
	}
	if cluster.Spec.Replicas < 1 {
		cluster.Spec.Replicas = 1
	}

	if s.clusterCtrl != nil {
		s.clusterCtrl.Register(&cluster)
		_, err := s.clusterCtrl.Reconcile(r.Context(), controllers.Request{Name: cluster.Metadata.Name})
		if err != nil {
			writeError(w, http.StatusInternalServerError, "reconciliation failed: "+err.Error())
			return
		}
		updated, _ := s.clusterCtrl.Get(cluster.Metadata.Name)
		writeJSON(w, http.StatusCreated, updated)
		return
	}

	writeJSON(w, http.StatusCreated, cluster)
}

func (s *Server) handleListClusters(w http.ResponseWriter, _ *http.Request) {
	if s.clusterCtrl != nil {
		clusters := s.clusterCtrl.List()
		writeJSON(w, http.StatusOK, clusters)
		return
	}
	writeJSON(w, http.StatusOK, []*v1.InferenceCluster{})
}

func (s *Server) handleGetCluster(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if s.clusterCtrl != nil {
		c, ok := s.clusterCtrl.Get(name)
		if !ok {
			writeError(w, http.StatusNotFound, fmt.Sprintf("cluster %q not found", name))
			return
		}
		writeJSON(w, http.StatusOK, c)
		return
	}
	writeError(w, http.StatusNotFound, "cluster controller not configured")
}

func (s *Server) handleScaleCluster(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var req ScaleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request: "+err.Error())
		return
	}

	if s.clusterCtrl != nil {
		c, ok := s.clusterCtrl.Get(name)
		if !ok {
			writeError(w, http.StatusNotFound, fmt.Sprintf("cluster %q not found", name))
			return
		}
		c.Spec.Replicas = req.Replicas
		s.clusterCtrl.Register(c)
		_, err := s.clusterCtrl.Reconcile(r.Context(), controllers.Request{Name: name})
		if err != nil {
			writeError(w, http.StatusInternalServerError, "scaling failed: "+err.Error())
			return
		}
		updated, _ := s.clusterCtrl.Get(name)
		writeJSON(w, http.StatusOK, updated)
		return
	}
	writeError(w, http.StatusBadRequest, "cluster controller not configured")
}

func (s *Server) handleDeleteCluster(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if s.clusterCtrl != nil {
		err := s.clusterCtrl.Delete(r.Context(), name)
		if err != nil {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "deleted", "cluster": name})
		return
	}
	writeError(w, http.StatusBadRequest, "cluster controller not configured")
}

// ─── Scheduler Handlers ───

func (s *Server) handleCapacity(w http.ResponseWriter, _ *http.Request) {
	report := s.scheduler.Capacity()
	writeJSON(w, http.StatusOK, report)
}

func (s *Server) handleFragmentation(w http.ResponseWriter, _ *http.Request) {
	report := s.scheduler.Fragmentation()
	writeJSON(w, http.StatusOK, report)
}

func (s *Server) handleOptimizationPlan(w http.ResponseWriter, _ *http.Request) {
	plan, err := s.scheduler.PlanDefragmentation()
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, plan)
}

func (s *Server) handleOptimizationApply(w http.ResponseWriter, r *http.Request) {
	plan, err := s.scheduler.PlanDefragmentation()
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Apply migrations
	for _, move := range plan.Moves {
		_ = s.fleet.ReleaseGPUs(r.Context(), move.FromNodeID, move.FromGPUIDs)
		_, _ = s.fleet.AllocateGPUs(r.Context(), move.ToNodeID, len(move.FromGPUIDs), move.WorkloadID)
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":       "APPLIED",
		"movesApplied": len(plan.Moves),
		"nodesFreed":   plan.NodesFreedUp,
		"plan":         plan,
	})
}

// ─── Health Handlers ───

func (s *Server) handleHealthCheck(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "healthy"})
}

func (s *Server) handleHealthIssues(w http.ResponseWriter, _ *http.Request) {
	if s.detector == nil {
		writeJSON(w, http.StatusOK, []health.HealthIssue{})
		return
	}
	issues := s.detector.ActiveIssues()
	writeJSON(w, http.StatusOK, issues)
}

// ─── Fleet Stats ───

func (s *Server) handleFleetStats(w http.ResponseWriter, _ *http.Request) {
	stats := s.fleet.Stats()
	writeJSON(w, http.StatusOK, stats)
}
