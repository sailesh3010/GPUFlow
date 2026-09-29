// Package types defines the core domain types for GPUFlow.
// These types are shared across all components and represent
// the canonical resource model for GPUs, nodes, and workloads.
package types

import (
	"fmt"
	"time"
)

// GPUModel represents a GPU hardware model.
type GPUModel string

const (
	GPUModelH100 GPUModel = "H100"
	GPUModelA100 GPUModel = "A100"
	GPUModelL40S GPUModel = "L40S"
)

// GPUHealth represents the health status of a GPU.
type GPUHealth string

const (
	GPUHealthHealthy  GPUHealth = "HEALTHY"
	GPUHealthDegraded GPUHealth = "DEGRADED"
	GPUHealthFailed   GPUHealth = "FAILED"
	GPUHealthUnknown  GPUHealth = "UNKNOWN"
)

// GPU represents a single GPU device within a node.
type GPU struct {
	ID          string    `json:"id"`
	Model       GPUModel  `json:"model"`
	MemoryGB    int       `json:"memoryGB"`
	Utilization int       `json:"utilization"` // 0-100 percent
	Temperature int       `json:"temperature"` // Celsius
	Health      GPUHealth `json:"health"`
	Allocated   bool      `json:"allocated"`
	WorkloadID  string    `json:"workloadId,omitempty"`
}

// IsAvailable returns true if the GPU is healthy and not allocated.
func (g *GPU) IsAvailable() bool {
	return g.Health == GPUHealthHealthy && !g.Allocated
}

// NodeState represents the lifecycle state of a GPU node.
type NodeState string

const (
	NodeStateDiscovered      NodeState = "DISCOVERED"
	NodeStateProvisioning    NodeState = "PROVISIONING"
	NodeStateOSReady         NodeState = "OS_READY"
	NodeStateDriverInstall   NodeState = "DRIVER_INSTALLING"
	NodeStateCUDAReady       NodeState = "CUDA_READY"
	NodeStateValidating      NodeState = "VALIDATING"
	NodeStateReady           NodeState = "READY"
	NodeStateAllocated       NodeState = "ALLOCATED"
	NodeStateDraining        NodeState = "DRAINING"
	NodeStateDegraded        NodeState = "DEGRADED"
	NodeStateFailed          NodeState = "FAILED"
	NodeStateRepairing       NodeState = "REPAIRING"
	NodeStateDecommissioning NodeState = "DECOMMISSIONING"
	NodeStateDecommissioned  NodeState = "DECOMMISSIONED"
)

// NodeHealth represents the overall health of a node.
type NodeHealth string

const (
	NodeHealthHealthy  NodeHealth = "HEALTHY"
	NodeHealthDegraded NodeHealth = "DEGRADED"
	NodeHealthFailed   NodeHealth = "FAILED"
	NodeHealthUnknown  NodeHealth = "UNKNOWN"
)

// TopologyType represents GPU interconnect topology.
type TopologyType string

const (
	TopologyNVLink TopologyType = "NVLINK"
	TopologyPCIe   TopologyType = "PCIE"
	TopologyNone   TopologyType = "NONE"
)

// Node represents a GPU node in the fleet.
type Node struct {
	ID               string       `json:"id"`
	Provider         string       `json:"provider"`
	Region           string       `json:"region"`
	Rack             string       `json:"rack"`
	GPUModel         GPUModel     `json:"gpuModel"`
	GPUCount         int          `json:"gpuCount"`
	GPUs             []GPU        `json:"gpus"`
	CPUCores         int          `json:"cpuCores"`
	RAMGB            int          `json:"ramGB"`
	NetworkBandwidth string       `json:"networkBandwidth"`
	Topology         TopologyType `json:"topology"`
	State            NodeState    `json:"state"`
	Health           NodeHealth   `json:"health"`
	CreatedAt        time.Time    `json:"createdAt"`
	UpdatedAt        time.Time    `json:"updatedAt"`
}

// AvailableGPUs returns the count of GPUs that are healthy and unallocated.
func (n *Node) AvailableGPUs() int {
	count := 0
	for i := range n.GPUs {
		if n.GPUs[i].IsAvailable() {
			count++
		}
	}
	return count
}

// AllocatedGPUs returns the count of allocated GPUs.
func (n *Node) AllocatedGPUs() int {
	count := 0
	for i := range n.GPUs {
		if n.GPUs[i].Allocated {
			count++
		}
	}
	return count
}

// IsSchedulable returns true if the node can accept new workloads.
func (n *Node) IsSchedulable() bool {
	return (n.State == NodeStateReady || n.State == NodeStateAllocated) &&
		n.Health == NodeHealthHealthy
}

// StateTransition records a node state change.
type StateTransition struct {
	Timestamp     time.Time `json:"timestamp"`
	NodeID        string    `json:"nodeId"`
	PreviousState NodeState `json:"previousState"`
	NewState      NodeState `json:"newState"`
	Reason        string    `json:"reason"`
	CorrelationID string    `json:"correlationId"`
}

// SchedulingStrategy represents the algorithm used for GPU placement.
type SchedulingStrategy string

const (
	StrategyFirstFit    SchedulingStrategy = "first-fit"
	StrategyBestFit     SchedulingStrategy = "best-fit"
	StrategyBinPack     SchedulingStrategy = "binpack"
	StrategyTopologyAware SchedulingStrategy = "topology-aware"
)

// GPURequest represents a request for GPU resources.
type GPURequest struct {
	Model    GPUModel           `json:"model"`
	Count    int                `json:"count"`
	MemoryGB int                `json:"memoryGB"`
	Topology TopologyType       `json:"topology"`
	Strategy SchedulingStrategy `json:"strategy"`
	Priority int                `json:"priority"` // higher = more important
}

// Allocation represents a GPU placement decision.
type Allocation struct {
	ID         string    `json:"id"`
	WorkloadID string    `json:"workloadId"`
	NodeID     string    `json:"nodeId"`
	GPUIDs     []string  `json:"gpuIds"`
	CreatedAt  time.Time `json:"createdAt"`
}

// ClusterPhase represents the phase of an inference cluster.
type ClusterPhase string

const (
	ClusterPhasePending      ClusterPhase = "PENDING"
	ClusterPhaseProvisioning ClusterPhase = "PROVISIONING"
	ClusterPhaseReady        ClusterPhase = "READY"
	ClusterPhaseDegraded     ClusterPhase = "DEGRADED"
	ClusterPhaseFailed       ClusterPhase = "FAILED"
	ClusterPhaseDeleting     ClusterPhase = "DELETING"
)

// EventType represents the type of a system event.
type EventType string

const (
	EventNodeDiscovered   EventType = "NODE_DISCOVERED"
	EventNodeProvisioned  EventType = "NODE_PROVISIONED"
	EventNodeReady        EventType = "NODE_READY"
	EventNodeFailed       EventType = "NODE_FAILED"
	EventNodeDraining     EventType = "NODE_DRAINING"
	EventNodeRepaired     EventType = "NODE_REPAIRED"
	EventNodeDecommission EventType = "NODE_DECOMMISSIONED"
	EventGPUFailed        EventType = "GPU_FAILED"
	EventGPURecovered     EventType = "GPU_RECOVERED"
	EventClusterCreated   EventType = "CLUSTER_CREATED"
	EventClusterReady     EventType = "CLUSTER_READY"
	EventClusterScaled    EventType = "CLUSTER_SCALED"
	EventClusterFailed    EventType = "CLUSTER_FAILED"
	EventClusterDeleted   EventType = "CLUSTER_DELETED"
	EventWorkloadPlaced   EventType = "WORKLOAD_PLACED"
	EventWorkloadEvicted  EventType = "WORKLOAD_EVICTED"
	EventDriftDetected    EventType = "DRIFT_DETECTED"
	EventDriftResolved    EventType = "DRIFT_RESOLVED"
)

// Event represents a system event for the event bus.
type Event struct {
	EventID       string            `json:"eventId"`
	EventType     EventType         `json:"eventType"`
	Timestamp     time.Time         `json:"timestamp"`
	NodeID        string            `json:"nodeId,omitempty"`
	ClusterID     string            `json:"clusterId,omitempty"`
	CorrelationID string            `json:"correlationId"`
	Data          map[string]string `json:"data,omitempty"`
}

// String implements fmt.Stringer for Event.
func (e Event) String() string {
	return fmt.Sprintf("[%s] %s node=%s cluster=%s correlation=%s",
		e.Timestamp.Format(time.RFC3339), e.EventType, e.NodeID, e.ClusterID, e.CorrelationID)
}
