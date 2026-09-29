// Package workflows implements ProvisionNodeWorkflow and its activities.
package workflows

import (
	"context"
	"fmt"
	"time"

	"github.com/gpuflow/gpuflow/pkg/types"
	"github.com/gpuflow/gpuflow/simulator"
)

// ProvisionNodeInput contains the input parameters for ProvisionNodeWorkflow.
type ProvisionNodeInput struct {
	NodeID         string             `json:"nodeId"`
	Provider       string             `json:"provider"`
	Region         string             `json:"region"`
	Rack           string             `json:"rack"`
	GPUModel       types.GPUModel     `json:"gpuModel"`
	GPUCount       int                `json:"gpuCount"`
	GPUMemoryGB    int                `json:"gpuMemoryGB"`
	Topology       types.TopologyType `json:"topology"`
	CorrelationID  string             `json:"correlationId"`
	IdempotencyKey string             `json:"idempotencyKey"`
}

// ProvisionNodeWorkflow runs the 8-step bare-metal provisioning lifecycle.
type ProvisionNodeWorkflow struct {
	engine *Engine
	fleet  *simulator.FleetSimulator
}

// NewProvisionNodeWorkflow creates a new workflow runner and registers its activities.
func NewProvisionNodeWorkflow(engine *Engine, fleet *simulator.FleetSimulator) *ProvisionNodeWorkflow {
	wf := &ProvisionNodeWorkflow{
		engine: engine,
		fleet:  fleet,
	}
	wf.registerActivities()
	return wf
}

func (w *ProvisionNodeWorkflow) registerActivities() {
	w.engine.RegisterActivity("DiscoverNode", func(ctx context.Context, in map[string]interface{}) (map[string]interface{}, error) {
		nodeID := in["nodeId"].(string)
		return map[string]interface{}{"status": "discovered", "nodeId": nodeID}, nil
	})

	w.engine.RegisterActivity("ProvisionOS", func(ctx context.Context, in map[string]interface{}) (map[string]interface{}, error) {
		nodeID := in["nodeId"].(string)
		corrID := in["correlationId"].(string)
		_ = w.fleet.TransitionNode(ctx, nodeID, types.NodeStateProvisioning, "PXE OS boot", corrID)
		_ = w.fleet.TransitionNode(ctx, nodeID, types.NodeStateOSReady, "Ubuntu 22.04 LTS installed", corrID)
		return map[string]interface{}{"os": "Ubuntu 22.04 LTS", "kernel": "5.15.0-generic"}, nil
	})

	w.engine.RegisterActivity("InstallDriver", func(ctx context.Context, in map[string]interface{}) (map[string]interface{}, error) {
		nodeID := in["nodeId"].(string)
		corrID := in["correlationId"].(string)
		_ = w.fleet.TransitionNode(ctx, nodeID, types.NodeStateDriverInstall, "NVIDIA driver 535.129.03", corrID)
		return map[string]interface{}{"driver": "535.129.03", "status": "installed"}, nil
	})

	w.engine.RegisterActivity("InstallCUDA", func(ctx context.Context, in map[string]interface{}) (map[string]interface{}, error) {
		nodeID := in["nodeId"].(string)
		corrID := in["correlationId"].(string)
		_ = w.fleet.TransitionNode(ctx, nodeID, types.NodeStateCUDAReady, "CUDA 12.2 installed", corrID)
		return map[string]interface{}{"cuda": "12.2", "nvrtc": "available"}, nil
	})

	w.engine.RegisterActivity("ConfigureNetwork", func(ctx context.Context, in map[string]interface{}) (map[string]interface{}, error) {
		return map[string]interface{}{"roce": "enabled", "mtu": 9000, "bandwidth": "400Gbps"}, nil
	})

	w.engine.RegisterActivity("ValidateGPU", func(ctx context.Context, in map[string]interface{}) (map[string]interface{}, error) {
		nodeID := in["nodeId"].(string)
		corrID := in["correlationId"].(string)
		_ = w.fleet.TransitionNode(ctx, nodeID, types.NodeStateValidating, "Running dcgm-diag and nvbandwidth tests", corrID)
		return map[string]interface{}{"dcgmStatus": "PASS", "nvlink": "PASS", "pcie": "PASS"}, nil
	})

	w.engine.RegisterActivity("ValidateNetwork", func(ctx context.Context, in map[string]interface{}) (map[string]interface{}, error) {
		return map[string]interface{}{"latencyMicros": 1.2, "packetLoss": 0.0}, nil
	})

	w.engine.RegisterActivity("RegisterNode", func(ctx context.Context, in map[string]interface{}) (map[string]interface{}, error) {
		nodeID := in["nodeId"].(string)
		corrID := in["correlationId"].(string)
		_ = w.fleet.TransitionNode(ctx, nodeID, types.NodeStateReady, "Node registered to control plane", corrID)
		return map[string]interface{}{"status": "READY", "schedulable": true}, nil
	})
}

// Execute orchestrates the 8 provisioning activities with durability and checkpointing.
func (w *ProvisionNodeWorkflow) Execute(ctx context.Context, in ProvisionNodeInput) (*WorkflowExecution, error) {
	if in.CorrelationID == "" {
		in.CorrelationID = fmt.Sprintf("corr-%s-%d", in.NodeID, time.Now().UnixNano())
	}
	if in.IdempotencyKey == "" {
		in.IdempotencyKey = fmt.Sprintf("prov-%s", in.NodeID)
	}

	inputMap := map[string]interface{}{
		"nodeId":        in.NodeID,
		"provider":      in.Provider,
		"region":        in.Region,
		"rack":          in.Rack,
		"gpuModel":      string(in.GPUModel),
		"gpuCount":      in.GPUCount,
		"correlationId": in.CorrelationID,
	}

	workflowID := fmt.Sprintf("wf-prov-%s", in.NodeID)
	exec, isNew := w.engine.StartWorkflow(workflowID, "ProvisionNodeWorkflow", in.IdempotencyKey, inputMap)
	if !isNew && exec.Status == StatusCompleted {
		return exec, nil
	}

	// Ensure node exists in simulator
	_, err := w.fleet.GetNode(in.NodeID)
	if err != nil {
		mem := in.GPUMemoryGB
		if mem == 0 {
			mem = 80
		}
		_, err = w.fleet.CreateNode(ctx, simulator.NodeSpec{
			ID:               in.NodeID,
			Provider:         in.Provider,
			Region:           in.Region,
			Rack:             in.Rack,
			GPUModel:         in.GPUModel,
			GPUCount:         in.GPUCount,
			GPUMemoryGB:      mem,
			CPUCores:         32,
			RAMGB:            256,
			NetworkBandwidth: "400Gbps",
			Topology:         in.Topology,
		})
		if err != nil {
			w.engine.Fail(exec, err)
			return exec, err
		}
	}

	opts := DefaultActivityOptions()
	activities := []string{
		"DiscoverNode",
		"ProvisionOS",
		"InstallDriver",
		"InstallCUDA",
		"ConfigureNetwork",
		"ValidateGPU",
		"ValidateNetwork",
		"RegisterNode",
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
		"status": "READY",
	})

	return exec, nil
}
