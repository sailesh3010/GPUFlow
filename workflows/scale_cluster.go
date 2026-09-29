// Package workflows implements ScaleClusterWorkflow.
package workflows

import (
	"context"
	"fmt"
	"time"

	"github.com/gpuflow/gpuflow/scheduler"
)

// ScaleClusterInput contains parameters for scaling an inference cluster.
type ScaleClusterInput struct {
	ClusterName     string `json:"clusterName"`
	CurrentReplicas int    `json:"currentReplicas"`
	DesiredReplicas int    `json:"desiredReplicas"`
	CorrelationID   string `json:"correlationId"`
}

// ScaleClusterWorkflow manages scaling workflows.
type ScaleClusterWorkflow struct {
	engine    *Engine
	scheduler *scheduler.Scheduler
}

// NewScaleClusterWorkflow creates a new ScaleClusterWorkflow.
func NewScaleClusterWorkflow(engine *Engine, sched *scheduler.Scheduler) *ScaleClusterWorkflow {
	wf := &ScaleClusterWorkflow{
		engine:    engine,
		scheduler: sched,
	}
	wf.registerActivities()
	return wf
}

func (w *ScaleClusterWorkflow) registerActivities() {
	w.engine.RegisterActivity("CheckClusterCapacity", func(ctx context.Context, in map[string]interface{}) (map[string]interface{}, error) {
		report := w.scheduler.Capacity()
		return map[string]interface{}{
			"availableGPUs": report.AvailableGPUs,
			"totalGPUs":     report.TotalGPUs,
		}, nil
	})

	w.engine.RegisterActivity("ScaleClusterReplicas", func(ctx context.Context, in map[string]interface{}) (map[string]interface{}, error) {
		desired := in["desiredReplicas"].(int)
		return map[string]interface{}{"scaledTo": desired, "status": "COMPLETED"}, nil
	})
}

// Execute orchestrates the scale workflow.
func (w *ScaleClusterWorkflow) Execute(ctx context.Context, in ScaleClusterInput) (*WorkflowExecution, error) {
	if in.CorrelationID == "" {
		in.CorrelationID = fmt.Sprintf("scale-corr-%s-%d", in.ClusterName, time.Now().UnixNano())
	}

	workflowID := fmt.Sprintf("wf-scale-%s-%d", in.ClusterName, time.Now().UnixNano())
	idempotencyKey := fmt.Sprintf("scale-%s-%d", in.ClusterName, in.DesiredReplicas)

	inputMap := map[string]interface{}{
		"clusterName":     in.ClusterName,
		"currentReplicas": in.CurrentReplicas,
		"desiredReplicas": in.DesiredReplicas,
		"correlationId":   in.CorrelationID,
	}

	exec, _ := w.engine.StartWorkflow(workflowID, "ScaleClusterWorkflow", idempotencyKey, inputMap)
	opts := DefaultActivityOptions()

	for _, act := range []string{"CheckClusterCapacity", "ScaleClusterReplicas"} {
		_, err := w.engine.ExecuteActivity(ctx, exec, act, opts, inputMap)
		if err != nil {
			w.engine.Fail(exec, err)
			return exec, err
		}
	}

	w.engine.Complete(exec, map[string]interface{}{
		"clusterName": in.ClusterName,
		"replicas":    in.DesiredReplicas,
	})

	return exec, nil
}
