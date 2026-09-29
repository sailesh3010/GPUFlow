// Package metrics implements Prometheus-compatible metrics exposition for GPUFlow.
package metrics

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gpuflow/gpuflow/pkg/types"
	"github.com/gpuflow/gpuflow/scheduler"
	"github.com/gpuflow/gpuflow/simulator"
)

// Registry manages and exposes GPUFlow Prometheus metrics.
type Registry struct {
	mu                      sync.RWMutex
	fleet                   *simulator.FleetSimulator
	scheduler               *scheduler.Scheduler
	reconciliationTotal     int64
	reconciliationErrors    int64
	repairsTotal            int64
	workflowDurationSeconds map[string]float64
}

// NewRegistry creates a metrics registry linked to the fleet and scheduler.
func NewRegistry(fleet *simulator.FleetSimulator, sched *scheduler.Scheduler) *Registry {
	return &Registry{
		fleet:                   fleet,
		scheduler:               sched,
		workflowDurationSeconds: make(map[string]float64),
	}
}

// RecordReconciliation increments reconciliation metrics.
func (r *Registry) RecordReconciliation(success bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reconciliationTotal++
	if !success {
		r.reconciliationErrors++
	}
}

// RecordRepair increments the repairs total metric.
func (r *Registry) RecordRepair() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.repairsTotal++
}

// RecordWorkflowDuration records the execution time of a workflow.
func (r *Registry) RecordWorkflowDuration(wfType string, duration time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.workflowDurationSeconds[wfType] = duration.Seconds()
}

// Handler returns an http.Handler serving Prometheus metrics in standard text format.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_ = r.WriteMetrics(w)
	})
}

// WriteMetrics writes the Prometheus formatted metrics to any string writer.
func (r *Registry) WriteMetrics(w http.ResponseWriter) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var sb strings.Builder

	sb.WriteString("# HELP gpuflow_gpu_utilization Current GPU utilization percentage by node and GPU\n")
	sb.WriteString("# TYPE gpuflow_gpu_utilization gauge\n")

	sb.WriteString("# HELP gpuflow_gpu_allocated Number of currently allocated GPUs by node\n")
	sb.WriteString("# TYPE gpuflow_gpu_allocated gauge\n")

	sb.WriteString("# HELP gpuflow_gpu_free Number of available GPUs by node\n")
	sb.WriteString("# TYPE gpuflow_gpu_free gauge\n")

	sb.WriteString("# HELP gpuflow_node_health Node health status (1 for current health status)\n")
	sb.WriteString("# TYPE gpuflow_node_health gauge\n")

	sb.WriteString("# HELP gpuflow_node_state Node lifecycle state (1 for active state)\n")
	sb.WriteString("# TYPE gpuflow_node_state gauge\n")

	sb.WriteString("# HELP gpuflow_scheduler_placement_total Total placement attempts\n")
	sb.WriteString("# TYPE gpuflow_scheduler_placement_total counter\n")

	sb.WriteString("# HELP gpuflow_scheduler_placement_failures Total failed placements\n")
	sb.WriteString("# TYPE gpuflow_scheduler_placement_failures counter\n")

	sb.WriteString("# HELP gpuflow_fragmentation_score Fleet capacity fragmentation score (0.0 to 1.0)\n")
	sb.WriteString("# TYPE gpuflow_fragmentation_score gauge\n")

	sb.WriteString("# HELP gpuflow_reconciliation_total Total controller reconciliations\n")
	sb.WriteString("# TYPE gpuflow_reconciliation_total counter\n")

	sb.WriteString("# HELP gpuflow_reconciliation_errors Total controller reconciliation errors\n")
	sb.WriteString("# TYPE gpuflow_reconciliation_errors counter\n")

	sb.WriteString("# HELP gpuflow_workflow_duration Workflow execution duration in seconds\n")
	sb.WriteString("# TYPE gpuflow_workflow_duration gauge\n")

	sb.WriteString("# HELP gpuflow_repair_total Total node repair workflows executed\n")
	sb.WriteString("# TYPE gpuflow_repair_total counter\n")

	if r.fleet != nil {
		nodes := r.fleet.ListNodes()
		for _, node := range nodes {
			sb.WriteString(fmt.Sprintf("gpuflow_gpu_allocated{node=\"%s\",model=\"%s\"} %d\n",
				node.ID, node.GPUModel, node.AllocatedGPUs()))
			sb.WriteString(fmt.Sprintf("gpuflow_gpu_free{node=\"%s\",model=\"%s\"} %d\n",
				node.ID, node.GPUModel, node.AvailableGPUs()))

			for _, h := range []types.NodeHealth{types.NodeHealthHealthy, types.NodeHealthDegraded, types.NodeHealthFailed} {
				val := 0
				if node.Health == h {
					val = 1
				}
				sb.WriteString(fmt.Sprintf("gpuflow_node_health{node=\"%s\",health=\"%s\"} %d\n", node.ID, h, val))
			}

			sb.WriteString(fmt.Sprintf("gpuflow_node_state{node=\"%s\",state=\"%s\"} 1\n", node.ID, node.State))

			for _, gpu := range node.GPUs {
				sb.WriteString(fmt.Sprintf("gpuflow_gpu_utilization{node=\"%s\",gpu=\"%s\",model=\"%s\"} %d\n",
					node.ID, gpu.ID, gpu.Model, gpu.Utilization))
			}
		}
	}

	if r.scheduler != nil {
		schedMetrics := r.scheduler.GetMetrics()
		sb.WriteString(fmt.Sprintf("gpuflow_scheduler_placement_total %d\n", schedMetrics.PlacementTotal))
		sb.WriteString(fmt.Sprintf("gpuflow_scheduler_placement_failures %d\n", schedMetrics.PlacementFailures))

		fragReport := r.scheduler.Fragmentation()
		sb.WriteString(fmt.Sprintf("gpuflow_fragmentation_score %0.4f\n", fragReport.OverallScore))
	}

	sb.WriteString(fmt.Sprintf("gpuflow_reconciliation_total %d\n", r.reconciliationTotal))
	sb.WriteString(fmt.Sprintf("gpuflow_reconciliation_errors %d\n", r.reconciliationErrors))
	sb.WriteString(fmt.Sprintf("gpuflow_repair_total %d\n", r.repairsTotal))

	for wfType, dur := range r.workflowDurationSeconds {
		sb.WriteString(fmt.Sprintf("gpuflow_workflow_duration{workflow=\"%s\"} %0.4f\n", wfType, dur))
	}

	_, err := w.Write([]byte(sb.String()))
	return err
}
