// Package controllers implements the GPUNodeReconciler.
package controllers

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/gpuflow/gpuflow/events"
	v1 "github.com/gpuflow/gpuflow/pkg/apis/gpuflow/v1"
	"github.com/gpuflow/gpuflow/pkg/types"
	"github.com/gpuflow/gpuflow/simulator"
)

// GPUNodeReconciler reconciles GPUNode objects against the simulated fleet.
type GPUNodeReconciler struct {
	mu       sync.RWMutex
	nodes    map[string]*v1.GPUNode
	fleet    *simulator.FleetSimulator
	eventBus events.Bus
	logger   *slog.Logger
}

// NewGPUNodeReconciler creates a new GPUNode reconciler.
func NewGPUNodeReconciler(fleet *simulator.FleetSimulator, bus events.Bus, logger *slog.Logger) *GPUNodeReconciler {
	return &GPUNodeReconciler{
		nodes:    make(map[string]*v1.GPUNode),
		fleet:    fleet,
		eventBus: bus,
		logger:   logger.With("reconciler", "GPUNode"),
	}
}

// Register registers or updates a GPUNode in the controller.
func (r *GPUNodeReconciler) Register(node *v1.GPUNode) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nodes[node.Metadata.Name] = node
}

// Get retrieves a GPUNode by name.
func (r *GPUNodeReconciler) Get(name string) (*v1.GPUNode, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n, ok := r.nodes[name]
	if !ok {
		return nil, false
	}
	copy := *n
	return &copy, true
}

// List returns all registered GPUNodes.
func (r *GPUNodeReconciler) List() []*v1.GPUNode {
	r.mu.RLock()
	defer r.mu.RUnlock()
	res := make([]*v1.GPUNode, 0, len(r.nodes))
	for _, n := range r.nodes {
		copy := *n
		res = append(res, &copy)
	}
	return res
}

// Reconcile drives actual state toward desired state for a GPUNode.
func (r *GPUNodeReconciler) Reconcile(ctx context.Context, req Request) (ReconcileResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	node, exists := r.nodes[req.Name]
	if !exists {
		return ReconcileResult{}, nil
	}

	correlationID := fmt.Sprintf("node-reconcile-%s-%d", req.Name, time.Now().UnixNano())
	r.logger.Info("reconciling GPUNode", "name", req.Name, "phase", node.Status.Phase)

	// Fetch actual node from the fleet simulator
	actualNode, err := r.fleet.GetNode(req.Name)
	if err != nil {
		// Node does not exist in simulator yet, need to create it
		r.logger.Info("creating node in simulator fleet", "name", req.Name)
		mem := node.Spec.GPU.MemoryGB
		if mem == 0 {
			mem = 80
		}
		newSpec := simulator.NodeSpec{
			ID:               req.Name,
			Provider:         node.Spec.Provider,
			Region:           node.Spec.Region,
			Rack:             node.Spec.Rack,
			GPUModel:         node.Spec.GPU.Model,
			GPUCount:         node.Spec.GPU.Count,
			GPUMemoryGB:      mem,
			CPUCores:         32,
			RAMGB:            256,
			NetworkBandwidth: "400Gbps",
			Topology:         node.Spec.Topology.Type,
		}
		if _, err := r.fleet.CreateNode(ctx, newSpec); err != nil {
			return ReconcileResult{}, fmt.Errorf("failed to create node in simulator: %w", err)
		}
		r.recordTransition(node, types.NodeState(""), types.NodeStateDiscovered, "Node detected", correlationID)
		_ = r.eventBus.Publish(ctx, events.TopicNodeEvents, events.NodeDiscovered(req.Name, correlationID))
		return ReconcileResult{Requeue: true}, nil
	}

	// Synchronize status from simulator
	node.Status.Health = actualNode.Health
	node.Status.AllocatedGPUs = actualNode.AllocatedGPUs()
	node.Status.AvailableGPUs = actualNode.AvailableGPUs()
	node.Status.GPUs = actualNode.GPUs

	// Step-by-step state machine progression
	switch actualNode.State {
	case types.NodeStateDiscovered:
		if err := r.fleet.TransitionNode(ctx, req.Name, types.NodeStateProvisioning, "Starting OS provisioning", correlationID); err == nil {
			r.recordTransition(node, types.NodeStateDiscovered, types.NodeStateProvisioning, "Starting OS provisioning", correlationID)
			return ReconcileResult{RequeueAfter: 10 * time.Millisecond}, nil
		}
	case types.NodeStateProvisioning:
		if err := r.fleet.TransitionNode(ctx, req.Name, types.NodeStateOSReady, "OS installation complete", correlationID); err == nil {
			r.recordTransition(node, types.NodeStateProvisioning, types.NodeStateOSReady, "OS installation complete", correlationID)
			return ReconcileResult{RequeueAfter: 10 * time.Millisecond}, nil
		}
	case types.NodeStateOSReady:
		if err := r.fleet.TransitionNode(ctx, req.Name, types.NodeStateDriverInstall, "Installing NVIDIA driver", correlationID); err == nil {
			r.recordTransition(node, types.NodeStateOSReady, types.NodeStateDriverInstall, "Installing NVIDIA driver", correlationID)
			return ReconcileResult{RequeueAfter: 10 * time.Millisecond}, nil
		}
	case types.NodeStateDriverInstall:
		if err := r.fleet.TransitionNode(ctx, req.Name, types.NodeStateCUDAReady, "CUDA toolkit verified", correlationID); err == nil {
			r.recordTransition(node, types.NodeStateDriverInstall, types.NodeStateCUDAReady, "CUDA toolkit verified", correlationID)
			return ReconcileResult{RequeueAfter: 10 * time.Millisecond}, nil
		}
	case types.NodeStateCUDAReady:
		if err := r.fleet.TransitionNode(ctx, req.Name, types.NodeStateValidating, "Validating GPUs and interconnects", correlationID); err == nil {
			r.recordTransition(node, types.NodeStateCUDAReady, types.NodeStateValidating, "Validating GPUs and interconnects", correlationID)
			return ReconcileResult{RequeueAfter: 10 * time.Millisecond}, nil
		}
	case types.NodeStateValidating:
		if err := r.fleet.TransitionNode(ctx, req.Name, types.NodeStateReady, "Node fully validated and schedulable", correlationID); err == nil {
			r.recordTransition(node, types.NodeStateValidating, types.NodeStateReady, "Node fully validated and schedulable", correlationID)
			_ = r.eventBus.Publish(ctx, events.TopicNodeEvents, events.NodeReady(req.Name, correlationID))
			node.SetCondition(v1.ConditionProvisioned, v1.ConditionTrue, "Provisioned", "Node provisioning complete")
			node.SetCondition(v1.ConditionReady, v1.ConditionTrue, "Ready", "Node is ready to accept workloads")
			node.Status.Phase = types.NodeStateReady
			return ReconcileResult{}, nil
		}
	case types.NodeStateReady:
		node.Status.Phase = types.NodeStateReady
		node.SetCondition(v1.ConditionReady, v1.ConditionTrue, "Ready", "Node healthy and schedulable")
	case types.NodeStateAllocated:
		node.Status.Phase = types.NodeStateAllocated
		node.SetCondition(v1.ConditionReady, v1.ConditionTrue, "Allocated", "Node running workloads")
	case types.NodeStateFailed:
		node.Status.Phase = types.NodeStateFailed
		node.SetCondition(v1.ConditionReady, v1.ConditionFalse, "Failed", "Node has failed")
		node.SetCondition(v1.ConditionDegraded, v1.ConditionTrue, "NodeFailed", "Node marked degraded/failed")
	case types.NodeStateRepairing:
		node.Status.Phase = types.NodeStateRepairing
		node.SetCondition(v1.ConditionReady, v1.ConditionFalse, "Repairing", "Automated remediation in progress")
	case types.NodeStateDraining:
		node.Status.Phase = types.NodeStateDraining
		node.SetCondition(v1.ConditionReady, v1.ConditionFalse, "Draining", "Workloads being drained")
	case types.NodeStateDecommissioned:
		node.Status.Phase = types.NodeStateDecommissioned
		node.SetCondition(v1.ConditionReady, v1.ConditionFalse, "Decommissioned", "Node decommissioned")
	}

	node.Status.Phase = actualNode.State
	return ReconcileResult{}, nil
}

func (r *GPUNodeReconciler) recordTransition(node *v1.GPUNode, prev, next types.NodeState, reason, correlationID string) {
	node.Status.Phase = next
	transition := types.StateTransition{
		Timestamp:     time.Now(),
		NodeID:        node.Metadata.Name,
		PreviousState: prev,
		NewState:      next,
		Reason:        reason,
		CorrelationID: correlationID,
	}
	node.Status.StateTransitions = append(node.Status.StateTransitions, transition)
}
