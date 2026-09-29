// Package workflows implements DecommissionNodeWorkflow for orderly node teardown.
package workflows

import (
	"context"
	"fmt"
	"time"

	"github.com/gpuflow/gpuflow/pkg/types"
	"github.com/gpuflow/gpuflow/simulator"
)

// DecommissionNodeInput contains parameters for node decommissioning.
type DecommissionNodeInput struct {
	NodeID        string `json:"nodeId"`
	Reason        string `json:"reason"`
	CorrelationID string `json:"correlationId"`
}

// DecommissionNodeWorkflow manages safe node draining and hardware release.
type DecommissionNodeWorkflow struct {
	engine *Engine
	fleet  *simulator.FleetSimulator
}

// NewDecommissionNodeWorkflow creates a new DecommissionNodeWorkflow.
func NewDecommissionNodeWorkflow(engine *Engine, fleet *simulator.FleetSimulator) *DecommissionNodeWorkflow {
	wf := &DecommissionNodeWorkflow{
		engine: engine,
		fleet:  fleet,
	}
	wf.registerActivities()
	return wf
}

func (w *DecommissionNodeWorkflow) registerActivities() {
	w.engine.RegisterActivity("CordonNode", func(ctx context.Context, in map[string]interface{}) (map[string]interface{}, error) {
		nodeID := in["nodeId"].(string)
		corrID := in["correlationId"].(string)
		_ = w.fleet.TransitionNode(ctx, nodeID, types.NodeStateDraining, "Cordoning node for decommission", corrID)
		return map[string]interface{}{"cordoned": true}, nil
	})

	w.engine.RegisterActivity("DrainWorkloads", func(ctx context.Context, in map[string]interface{}) (map[string]interface{}, error) {
		nodeID := in["nodeId"].(string)
		node, err := w.fleet.GetNode(nodeID)
		if err == nil {
			var toFree []string
			for _, g := range node.GPUs {
				if g.Allocated {
					toFree = append(toFree, g.ID)
				}
			}
			if len(toFree) > 0 {
				_ = w.fleet.ReleaseGPUs(ctx, nodeID, toFree)
			}
		}
		return map[string]interface{}{"drained": true}, nil
	})

	w.engine.RegisterActivity("DeprovisionHardware", func(ctx context.Context, in map[string]interface{}) (map[string]interface{}, error) {
		nodeID := in["nodeId"].(string)
		corrID := in["correlationId"].(string)
		_ = w.fleet.TransitionNode(ctx, nodeID, types.NodeStateDecommissioning, "Releasing hardware", corrID)
		_ = w.fleet.TransitionNode(ctx, nodeID, types.NodeStateDecommissioned, "Hardware decommissioned", corrID)
		return map[string]interface{}{"decommissioned": true}, nil
	})
}

// Execute runs the decommission workflow.
func (w *DecommissionNodeWorkflow) Execute(ctx context.Context, in DecommissionNodeInput) (*WorkflowExecution, error) {
	if in.CorrelationID == "" {
		in.CorrelationID = fmt.Sprintf("decom-corr-%s-%d", in.NodeID, time.Now().UnixNano())
	}

	workflowID := fmt.Sprintf("wf-decom-%s", in.NodeID)
	idempotencyKey := fmt.Sprintf("decom-%s", in.NodeID)

	inputMap := map[string]interface{}{
		"nodeId":        in.NodeID,
		"reason":        in.Reason,
		"correlationId": in.CorrelationID,
	}

	exec, _ := w.engine.StartWorkflow(workflowID, "DecommissionNodeWorkflow", idempotencyKey, inputMap)
	opts := DefaultActivityOptions()

	for _, act := range []string{"CordonNode", "DrainWorkloads", "DeprovisionHardware"} {
		_, err := w.engine.ExecuteActivity(ctx, exec, act, opts, inputMap)
		if err != nil {
			w.engine.Fail(exec, err)
			return exec, err
		}
	}

	w.engine.Complete(exec, map[string]interface{}{
		"nodeId": in.NodeID,
		"status": "DECOMMISSIONED",
	})

	return exec, nil
}
