// Package workflows implements RepairNodeWorkflow for automated node self-healing.
package workflows

import (
	"context"
	"fmt"
	"time"

	"github.com/gpuflow/gpuflow/pkg/types"
	"github.com/gpuflow/gpuflow/simulator"
)

// RepairNodeInput contains parameters for repairing a failed GPU node.
type RepairNodeInput struct {
	NodeID        string `json:"nodeId"`
	Reason        string `json:"reason"`
	CorrelationID string `json:"correlationId"`
}

// RepairNodeWorkflow manages the automated remediation pipeline.
type RepairNodeWorkflow struct {
	engine *Engine
	fleet  *simulator.FleetSimulator
}

// NewRepairNodeWorkflow creates a new RepairNodeWorkflow.
func NewRepairNodeWorkflow(engine *Engine, fleet *simulator.FleetSimulator) *RepairNodeWorkflow {
	wf := &RepairNodeWorkflow{
		engine: engine,
		fleet:  fleet,
	}
	wf.registerActivities()
	return wf
}

func (w *RepairNodeWorkflow) registerActivities() {
	w.engine.RegisterActivity("DrainNodeWorkloads", func(ctx context.Context, in map[string]interface{}) (map[string]interface{}, error) {
		nodeID := in["nodeId"].(string)
		corrID := in["correlationId"].(string)
		// Mark draining if currently degraded/failed
		_ = w.fleet.TransitionNode(ctx, nodeID, types.NodeStateDraining, "Evicting workloads for repair", corrID)
		return map[string]interface{}{"drained": true, "nodeId": nodeID}, nil
	})

	w.engine.RegisterActivity("PowerCycleHardware", func(ctx context.Context, in map[string]interface{}) (map[string]interface{}, error) {
		nodeID := in["nodeId"].(string)
		corrID := in["correlationId"].(string)
		_ = w.fleet.TransitionNode(ctx, nodeID, types.NodeStateRepairing, "BMC IPMI power cycle & PCIe reset", corrID)
		return map[string]interface{}{"powerCycle": "SUCCESS"}, nil
	})

	w.engine.RegisterActivity("ValidateRepairedGPU", func(ctx context.Context, in map[string]interface{}) (map[string]interface{}, error) {
		nodeID := in["nodeId"].(string)
		corrID := in["correlationId"].(string)
		// Recover GPUs in simulator
		_ = w.fleet.RecoverNode(ctx, nodeID)
		_ = w.fleet.TransitionNode(ctx, nodeID, types.NodeStateValidating, "Post-repair validation checks", corrID)
		return map[string]interface{}{"dcgm": "PASS", "eccErrors": 0}, nil
	})

	w.engine.RegisterActivity("ReRegisterRepairedNode", func(ctx context.Context, in map[string]interface{}) (map[string]interface{}, error) {
		nodeID := in["nodeId"].(string)
		corrID := in["correlationId"].(string)
		_ = w.fleet.TransitionNode(ctx, nodeID, types.NodeStateReady, "Node healed and returned to service", corrID)
		return map[string]interface{}{"status": "READY", "healthy": true}, nil
	})
}

// Execute runs the repair workflow with durability and retries.
func (w *RepairNodeWorkflow) Execute(ctx context.Context, in RepairNodeInput) (*WorkflowExecution, error) {
	if in.CorrelationID == "" {
		in.CorrelationID = fmt.Sprintf("repair-corr-%s-%d", in.NodeID, time.Now().UnixNano())
	}

	workflowID := fmt.Sprintf("wf-repair-%s-%d", in.NodeID, time.Now().UnixNano())
	idempotencyKey := fmt.Sprintf("repair-%s-%d", in.NodeID, time.Now().Unix())

	inputMap := map[string]interface{}{
		"nodeId":        in.NodeID,
		"reason":        in.Reason,
		"correlationId": in.CorrelationID,
	}

	exec, _ := w.engine.StartWorkflow(workflowID, "RepairNodeWorkflow", idempotencyKey, inputMap)
	opts := DefaultActivityOptions()

	activities := []string{
		"DrainNodeWorkloads",
		"PowerCycleHardware",
		"ValidateRepairedGPU",
		"ReRegisterRepairedNode",
	}

	for _, act := range activities {
		_, err := w.engine.ExecuteActivity(ctx, exec, act, opts, inputMap)
		if err != nil {
			w.engine.Fail(exec, err)
			return exec, err
		}
	}

	w.engine.Complete(exec, map[string]interface{}{
		"nodeId": in.NodeID,
		"status": "REPAIRED_AND_READY",
	})

	return exec, nil
}
