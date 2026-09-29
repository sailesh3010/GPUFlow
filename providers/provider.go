// Package providers defines the infrastructure provider abstraction.
// The control plane interacts with node infrastructure exclusively through
// this interface, enabling clean separation from provider-specific logic.
package providers

import (
	"context"

	"github.com/gpuflow/gpuflow/pkg/types"
)

// NodeSpec describes the desired state of a node to provision.
type NodeSpec struct {
	ID               string           `json:"id"`
	GPUModel         types.GPUModel   `json:"gpuModel"`
	GPUCount         int              `json:"gpuCount"`
	GPUMemoryGB      int              `json:"gpuMemoryGB"`
	CPUCores         int              `json:"cpuCores"`
	RAMGB            int              `json:"ramGB"`
	NetworkBandwidth string           `json:"networkBandwidth"`
	Topology         types.TopologyType `json:"topology"`
	Region           string           `json:"region"`
	Rack             string           `json:"rack"`
}

// HealthStatus represents a provider's view of node health.
type HealthStatus struct {
	NodeID    string           `json:"nodeId"`
	Healthy   bool             `json:"healthy"`
	Health    types.NodeHealth `json:"health"`
	Message   string           `json:"message"`
	GPUHealth []GPUHealthInfo  `json:"gpuHealth"`
}

// GPUHealthInfo represents health information for a single GPU.
type GPUHealthInfo struct {
	ID          string          `json:"id"`
	Health      types.GPUHealth `json:"health"`
	Temperature int             `json:"temperature"`
	Utilization int             `json:"utilization"`
	MemoryUsed  int             `json:"memoryUsedGB"`
}

// Provider is the infrastructure abstraction for GPU node management.
// All node lifecycle operations go through this interface.
type Provider interface {
	// Name returns the provider identifier.
	Name() string

	// Provision creates a new node according to the spec.
	// Must be idempotent — calling with the same spec.ID returns the existing node.
	Provision(ctx context.Context, spec NodeSpec) (*types.Node, error)

	// Deprovision removes a node. Must be idempotent.
	Deprovision(ctx context.Context, nodeID string) error

	// Health returns the current health status of a node.
	Health(ctx context.Context, nodeID string) (*HealthStatus, error)

	// PowerCycle simulates a hard restart of a node.
	PowerCycle(ctx context.Context, nodeID string) error

	// List returns all nodes managed by this provider.
	List(ctx context.Context) ([]*types.Node, error)
}
