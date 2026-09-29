// Package simulator provides a realistic GPU fleet simulation.
// It manages a fleet of virtual GPU nodes with realistic resource models,
// health states, and failure injection — without requiring any real GPU hardware.
package simulator

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/gpuflow/gpuflow/pkg/types"
)

// FleetSimulator manages a fleet of simulated GPU nodes.
// All operations are thread-safe.
type FleetSimulator struct {
	mu          sync.RWMutex
	nodes       map[string]*types.Node
	transitions []types.StateTransition
	log         *slog.Logger
	rng         *rand.Rand
}

// NewFleetSimulator creates a new fleet simulator.
func NewFleetSimulator(log *slog.Logger) *FleetSimulator {
	return &FleetSimulator{
		nodes:       make(map[string]*types.Node),
		transitions: make([]types.StateTransition, 0),
		log:         log,
		rng:         rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// NodeSpec describes the desired configuration for a new node.
type NodeSpec struct {
	ID               string
	Provider         string
	Region           string
	Rack             string
	GPUModel         types.GPUModel
	GPUCount         int
	GPUMemoryGB      int
	CPUCores         int
	RAMGB            int
	NetworkBandwidth string
	Topology         types.TopologyType
}

// DefaultH100Spec returns a spec for a standard H100 node.
func DefaultH100Spec(id, rack string) NodeSpec {
	return NodeSpec{
		ID:               id,
		Provider:         "local",
		Region:           "local-1",
		Rack:             rack,
		GPUModel:         types.GPUModelH100,
		GPUCount:         8,
		GPUMemoryGB:      80,
		CPUCores:         96,
		RAMGB:            512,
		NetworkBandwidth: "400Gbps",
		Topology:         types.TopologyNVLink,
	}
}

// DefaultA100Spec returns a spec for a standard A100 node.
func DefaultA100Spec(id, rack string) NodeSpec {
	return NodeSpec{
		ID:               id,
		Provider:         "local",
		Region:           "local-1",
		Rack:             rack,
		GPUModel:         types.GPUModelA100,
		GPUCount:         8,
		GPUMemoryGB:      80,
		CPUCores:         64,
		RAMGB:            512,
		NetworkBandwidth: "200Gbps",
		Topology:         types.TopologyNVLink,
	}
}

// CreateNode adds a new simulated node in DISCOVERED state.
func (f *FleetSimulator) CreateNode(_ context.Context, spec NodeSpec) (*types.Node, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, exists := f.nodes[spec.ID]; exists {
		return nil, fmt.Errorf("node %s already exists", spec.ID)
	}

	gpus := make([]types.GPU, spec.GPUCount)
	for i := range gpus {
		gpus[i] = types.GPU{
			ID:          fmt.Sprintf("%s-gpu-%02d", spec.ID, i),
			Model:       spec.GPUModel,
			MemoryGB:    spec.GPUMemoryGB,
			Utilization: 0,
			Temperature: 35 + f.rng.Intn(10), // idle temp 35-44°C
			Health:      types.GPUHealthHealthy,
			Allocated:   false,
		}
	}

	now := time.Now()
	node := &types.Node{
		ID:               spec.ID,
		Provider:         spec.Provider,
		Region:           spec.Region,
		Rack:             spec.Rack,
		GPUModel:         spec.GPUModel,
		GPUCount:         spec.GPUCount,
		GPUs:             gpus,
		CPUCores:         spec.CPUCores,
		RAMGB:            spec.RAMGB,
		NetworkBandwidth: spec.NetworkBandwidth,
		Topology:         spec.Topology,
		State:            types.NodeStateDiscovered,
		Health:           types.NodeHealthHealthy,
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	f.nodes[spec.ID] = node
	f.recordTransition(spec.ID, "", types.NodeStateDiscovered, "node created", "")

	f.log.Info("node created",
		slog.String("nodeId", spec.ID),
		slog.String("gpuModel", string(spec.GPUModel)),
		slog.Int("gpuCount", spec.GPUCount),
	)

	return node, nil
}

// DeleteNode removes a node from the fleet.
func (f *FleetSimulator) DeleteNode(_ context.Context, nodeID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	node, exists := f.nodes[nodeID]
	if !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	// Check for allocated GPUs — prevent deletion of nodes with active workloads
	if node.AllocatedGPUs() > 0 {
		return fmt.Errorf("node %s has %d allocated GPUs; drain before deleting", nodeID, node.AllocatedGPUs())
	}

	delete(f.nodes, nodeID)
	f.log.Info("node deleted", slog.String("nodeId", nodeID))
	return nil
}

// GetNode returns a copy of the node.
func (f *FleetSimulator) GetNode(nodeID string) (*types.Node, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	node, exists := f.nodes[nodeID]
	if !exists {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}

	// Return a copy to prevent races
	copy := *node
	copy.GPUs = make([]types.GPU, len(node.GPUs))
	for i := range node.GPUs {
		copy.GPUs[i] = node.GPUs[i]
	}
	return &copy, nil
}

// ListNodes returns copies of all nodes.
func (f *FleetSimulator) ListNodes() []*types.Node {
	f.mu.RLock()
	defer f.mu.RUnlock()

	result := make([]*types.Node, 0, len(f.nodes))
	for _, node := range f.nodes {
		copy := *node
		copy.GPUs = make([]types.GPU, len(node.GPUs))
		for i := range node.GPUs {
			copy.GPUs[i] = node.GPUs[i]
		}
		result = append(result, &copy)
	}
	return result
}

// TransitionNode moves a node to a new state with validation.
func (f *FleetSimulator) TransitionNode(_ context.Context, nodeID string, newState types.NodeState, reason, correlationID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	node, exists := f.nodes[nodeID]
	if !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	oldState := node.State

	if err := validateTransition(oldState, newState); err != nil {
		return fmt.Errorf("invalid transition for node %s: %w", nodeID, err)
	}

	node.State = newState
	node.UpdatedAt = time.Now()

	// Update health based on state
	switch newState {
	case types.NodeStateFailed:
		node.Health = types.NodeHealthFailed
	case types.NodeStateDegraded:
		node.Health = types.NodeHealthDegraded
	case types.NodeStateReady, types.NodeStateAllocated:
		node.Health = types.NodeHealthHealthy
	}

	f.recordTransition(nodeID, oldState, newState, reason, correlationID)

	f.log.Info("node state transition",
		slog.String("nodeId", nodeID),
		slog.String("from", string(oldState)),
		slog.String("to", string(newState)),
		slog.String("reason", reason),
	)

	return nil
}

// validateTransition checks if a state transition is legal.
func validateTransition(from, to types.NodeState) error {
	allowed := map[types.NodeState][]types.NodeState{
		"": {types.NodeStateDiscovered}, // initial creation
		types.NodeStateDiscovered:      {types.NodeStateProvisioning, types.NodeStateFailed},
		types.NodeStateProvisioning:    {types.NodeStateOSReady, types.NodeStateFailed},
		types.NodeStateOSReady:         {types.NodeStateDriverInstall, types.NodeStateFailed},
		types.NodeStateDriverInstall:   {types.NodeStateCUDAReady, types.NodeStateFailed},
		types.NodeStateCUDAReady:       {types.NodeStateValidating, types.NodeStateFailed},
		types.NodeStateValidating:      {types.NodeStateReady, types.NodeStateFailed},
		types.NodeStateReady:           {types.NodeStateAllocated, types.NodeStateDraining, types.NodeStateDegraded, types.NodeStateFailed},
		types.NodeStateAllocated:       {types.NodeStateReady, types.NodeStateDraining, types.NodeStateDegraded, types.NodeStateFailed},
		types.NodeStateDraining:        {types.NodeStateReady, types.NodeStateDecommissioning, types.NodeStateFailed},
		types.NodeStateDegraded:        {types.NodeStateRepairing, types.NodeStateFailed, types.NodeStateDraining},
		types.NodeStateFailed:          {types.NodeStateRepairing, types.NodeStateDecommissioning},
		types.NodeStateRepairing:       {types.NodeStateValidating, types.NodeStateFailed},
		types.NodeStateDecommissioning: {types.NodeStateDecommissioned, types.NodeStateFailed},
		types.NodeStateDecommissioned:  {}, // terminal state
	}

	valid, exists := allowed[from]
	if !exists {
		return fmt.Errorf("unknown state %s", from)
	}

	for _, s := range valid {
		if s == to {
			return nil
		}
	}

	return fmt.Errorf("cannot transition from %s to %s", from, to)
}

// AllocateGPUs marks GPUs on a node as allocated to a workload.
func (f *FleetSimulator) AllocateGPUs(_ context.Context, nodeID string, count int, workloadID string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	node, exists := f.nodes[nodeID]
	if !exists {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}

	if !node.IsSchedulable() {
		return nil, fmt.Errorf("node %s is not schedulable (state=%s, health=%s)", nodeID, node.State, node.Health)
	}

	available := make([]int, 0)
	for i := range node.GPUs {
		if node.GPUs[i].IsAvailable() {
			available = append(available, i)
		}
	}

	if len(available) < count {
		return nil, fmt.Errorf("node %s has %d available GPUs, requested %d", nodeID, len(available), count)
	}

	allocated := make([]string, count)
	for i := 0; i < count; i++ {
		idx := available[i]
		node.GPUs[idx].Allocated = true
		node.GPUs[idx].WorkloadID = workloadID
		node.GPUs[idx].Utilization = 30 + f.rng.Intn(50) // simulate 30-79% utilization
		node.GPUs[idx].Temperature = 55 + f.rng.Intn(20) // simulate 55-74°C under load
		allocated[i] = node.GPUs[idx].ID
	}

	// If any GPUs are allocated, node should be ALLOCATED
	if node.State == types.NodeStateReady {
		node.State = types.NodeStateAllocated
		node.UpdatedAt = time.Now()
	}

	f.log.Info("GPUs allocated",
		slog.String("nodeId", nodeID),
		slog.String("workloadId", workloadID),
		slog.Int("count", count),
	)

	return allocated, nil
}

// ReleaseGPUs marks GPUs as free.
func (f *FleetSimulator) ReleaseGPUs(_ context.Context, nodeID string, gpuIDs []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	node, exists := f.nodes[nodeID]
	if !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	releaseSet := make(map[string]bool)
	for _, id := range gpuIDs {
		releaseSet[id] = true
	}

	for i := range node.GPUs {
		if releaseSet[node.GPUs[i].ID] {
			node.GPUs[i].Allocated = false
			node.GPUs[i].WorkloadID = ""
			node.GPUs[i].Utilization = 0
			node.GPUs[i].Temperature = 35 + f.rng.Intn(10)
		}
	}

	// If no GPUs allocated, transition back to READY
	if node.AllocatedGPUs() == 0 && node.State == types.NodeStateAllocated {
		node.State = types.NodeStateReady
		node.UpdatedAt = time.Now()
	}

	f.log.Info("GPUs released",
		slog.String("nodeId", nodeID),
		slog.Int("count", len(gpuIDs)),
	)

	return nil
}

// FailNode simulates a node failure.
func (f *FleetSimulator) FailNode(ctx context.Context, nodeID, reason string) error {
	return f.TransitionNode(ctx, nodeID, types.NodeStateFailed, reason, "")
}

// FailGPU simulates a single GPU failure on a node.
func (f *FleetSimulator) FailGPU(_ context.Context, nodeID, gpuID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	node, exists := f.nodes[nodeID]
	if !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	for i := range node.GPUs {
		if node.GPUs[i].ID == gpuID {
			node.GPUs[i].Health = types.GPUHealthFailed
			node.GPUs[i].Utilization = 0
			node.GPUs[i].Temperature = 0

			f.log.Info("GPU failed",
				slog.String("nodeId", nodeID),
				slog.String("gpuId", gpuID),
			)

			// If enough GPUs fail, degrade the node
			failedCount := 0
			for j := range node.GPUs {
				if node.GPUs[j].Health == types.GPUHealthFailed {
					failedCount++
				}
			}
			if failedCount > 0 && node.Health == types.NodeHealthHealthy {
				node.Health = types.NodeHealthDegraded
				if node.State == types.NodeStateReady || node.State == types.NodeStateAllocated {
					node.State = types.NodeStateDegraded
					node.UpdatedAt = time.Now()
				}
			}

			return nil
		}
	}

	return fmt.Errorf("GPU %s not found on node %s", gpuID, nodeID)
}

// RecoverNode simulates node recovery.
func (f *FleetSimulator) RecoverNode(ctx context.Context, nodeID string) error {
	f.mu.Lock()

	node, exists := f.nodes[nodeID]
	if !exists {
		f.mu.Unlock()
		return fmt.Errorf("node %s not found", nodeID)
	}

	// Restore all GPU health
	for i := range node.GPUs {
		node.GPUs[i].Health = types.GPUHealthHealthy
		if !node.GPUs[i].Allocated {
			node.GPUs[i].Utilization = 0
			node.GPUs[i].Temperature = 35 + f.rng.Intn(10)
		}
	}
	node.Health = types.NodeHealthHealthy
	f.mu.Unlock()

	// Use the public method which acquires its own lock
	return f.TransitionNode(ctx, nodeID, types.NodeStateRepairing, "recovery initiated", "")
}

// GetTransitions returns all recorded state transitions.
func (f *FleetSimulator) GetTransitions() []types.StateTransition {
	f.mu.RLock()
	defer f.mu.RUnlock()

	result := make([]types.StateTransition, len(f.transitions))
	copy(result, f.transitions)
	return result
}

// GetNodeTransitions returns transitions for a specific node.
func (f *FleetSimulator) GetNodeTransitions(nodeID string) []types.StateTransition {
	f.mu.RLock()
	defer f.mu.RUnlock()

	result := make([]types.StateTransition, 0)
	for _, t := range f.transitions {
		if t.NodeID == nodeID {
			result = append(result, t)
		}
	}
	return result
}

// FleetStats returns aggregate fleet statistics.
type FleetStats struct {
	TotalNodes     int     `json:"totalNodes"`
	HealthyNodes   int     `json:"healthyNodes"`
	DegradedNodes  int     `json:"degradedNodes"`
	FailedNodes    int     `json:"failedNodes"`
	TotalGPUs      int     `json:"totalGPUs"`
	AllocatedGPUs  int     `json:"allocatedGPUs"`
	AvailableGPUs  int     `json:"availableGPUs"`
	FailedGPUs     int     `json:"failedGPUs"`
	Utilization    float64 `json:"utilization"` // average GPU utilization
}

// Stats returns aggregate fleet statistics.
func (f *FleetSimulator) Stats() FleetStats {
	f.mu.RLock()
	defer f.mu.RUnlock()

	stats := FleetStats{}
	totalUtil := 0
	allocatedCount := 0

	for _, node := range f.nodes {
		stats.TotalNodes++

		switch node.Health {
		case types.NodeHealthHealthy:
			stats.HealthyNodes++
		case types.NodeHealthDegraded:
			stats.DegradedNodes++
		case types.NodeHealthFailed:
			stats.FailedNodes++
		}

		for i := range node.GPUs {
			stats.TotalGPUs++
			gpu := &node.GPUs[i]

			if gpu.Health == types.GPUHealthFailed {
				stats.FailedGPUs++
				continue
			}

			if gpu.Allocated {
				stats.AllocatedGPUs++
				totalUtil += gpu.Utilization
				allocatedCount++
			} else if gpu.IsAvailable() {
				stats.AvailableGPUs++
			}
		}
	}

	if allocatedCount > 0 {
		stats.Utilization = float64(totalUtil) / float64(allocatedCount)
	}

	return stats
}

// recordTransition records a state transition (must be called with lock held).
func (f *FleetSimulator) recordTransition(nodeID string, from, to types.NodeState, reason, correlationID string) {
	f.transitions = append(f.transitions, types.StateTransition{
		Timestamp:     time.Now(),
		NodeID:        nodeID,
		PreviousState: from,
		NewState:      to,
		Reason:        reason,
		CorrelationID: correlationID,
	})
}

// CreateDefaultFleet creates a standard fleet for development and demos.
func (f *FleetSimulator) CreateDefaultFleet(ctx context.Context) error {
	specs := []NodeSpec{
		DefaultH100Spec("gpu-node-01", "rack-a"),
		DefaultH100Spec("gpu-node-02", "rack-a"),
		DefaultA100Spec("gpu-node-03", "rack-b"),
		DefaultA100Spec("gpu-node-04", "rack-b"),
	}

	for _, spec := range specs {
		if _, err := f.CreateNode(ctx, spec); err != nil {
			return fmt.Errorf("creating node %s: %w", spec.ID, err)
		}
	}

	return nil
}

// ProvisionNode simulates the provisioning lifecycle for a node.
// Transitions through: DISCOVERED → PROVISIONING → OS_READY → DRIVER_INSTALLING → CUDA_READY → VALIDATING → READY
func (f *FleetSimulator) ProvisionNode(ctx context.Context, nodeID, correlationID string) error {
	steps := []struct {
		state  types.NodeState
		reason string
	}{
		{types.NodeStateProvisioning, "provisioning started"},
		{types.NodeStateOSReady, "OS installation complete"},
		{types.NodeStateDriverInstall, "driver installation started"},
		{types.NodeStateCUDAReady, "CUDA toolkit installed"},
		{types.NodeStateValidating, "validation started"},
		{types.NodeStateReady, "all validations passed"},
	}

	for _, step := range steps {
		if err := f.TransitionNode(ctx, nodeID, step.state, step.reason, correlationID); err != nil {
			return fmt.Errorf("provisioning step %s failed: %w", step.state, err)
		}
	}

	return nil
}
