// Package v1 defines the Kubernetes-native API types for GPUFlow Custom Resource Definitions.
package v1

import (
	"time"

	"github.com/gpuflow/gpuflow/pkg/types"
)

// ConditionStatus represents status of a condition.
type ConditionStatus string

const (
	ConditionTrue    ConditionStatus = "True"
	ConditionFalse   ConditionStatus = "False"
	ConditionUnknown ConditionStatus = "Unknown"
)

// ConditionType represents the condition type.
type ConditionType string

const (
	ConditionReady         ConditionType = "Ready"
	ConditionScheduled     ConditionType = "Scheduled"
	ConditionProvisioned   ConditionType = "Provisioned"
	ConditionDriftDetected ConditionType = "DriftDetected"
	ConditionDegraded      ConditionType = "Degraded"
)

// Condition contains details for one aspect of the current state of this API Resource.
type Condition struct {
	Type               ConditionType   `json:"type" yaml:"type"`
	Status             ConditionStatus `json:"status" yaml:"status"`
	LastTransitionTime time.Time       `json:"lastTransitionTime" yaml:"lastTransitionTime"`
	Reason             string          `json:"reason" yaml:"reason"`
	Message            string          `json:"message" yaml:"message"`
}

// ModelSpec defines the model to run on the cluster.
type ModelSpec struct {
	Name string `json:"name" yaml:"name"`
}

// RuntimeSpec defines the inference runtime engine.
type RuntimeSpec struct {
	Name string `json:"name" yaml:"name"`
}

// GPUResourceSpec specifies the GPU hardware requirements.
type GPUResourceSpec struct {
	Model    types.GPUModel `json:"model" yaml:"model"`
	Count    int            `json:"count" yaml:"count"`
	MemoryGB int            `json:"memoryGB" yaml:"memoryGB"`
}

// ResourceRequirements defines the compute resources.
type ResourceRequirements struct {
	GPU GPUResourceSpec `json:"gpu" yaml:"gpu"`
}

// SchedulingSpec defines placement strategies.
type SchedulingSpec struct {
	Topology types.TopologyType       `json:"topology,omitempty" yaml:"topology,omitempty"`
	Strategy types.SchedulingStrategy `json:"strategy,omitempty" yaml:"strategy,omitempty"`
}

// AvailabilitySpec defines high availability configuration.
type AvailabilitySpec struct {
	MinHealthy int `json:"minHealthy,omitempty" yaml:"minHealthy,omitempty"`
}

// InferenceClusterSpec defines the desired state of InferenceCluster.
type InferenceClusterSpec struct {
	Replicas     int                  `json:"replicas" yaml:"replicas"`
	Model        ModelSpec            `json:"model" yaml:"model"`
	Runtime      RuntimeSpec          `json:"runtime" yaml:"runtime"`
	Resources    ResourceRequirements `json:"resources" yaml:"resources"`
	Scheduling   SchedulingSpec       `json:"scheduling,omitempty" yaml:"scheduling,omitempty"`
	Availability AvailabilitySpec     `json:"availability,omitempty" yaml:"availability,omitempty"`
}

// ReplicaStatus tracks desired, ready, and failed replica counts.
type ReplicaStatus struct {
	Desired int `json:"desired" yaml:"desired"`
	Ready   int `json:"ready" yaml:"ready"`
	Failed  int `json:"failed" yaml:"failed"`
}

// InferenceClusterStatus defines the observed state of InferenceCluster.
type InferenceClusterStatus struct {
	Phase         types.ClusterPhase `json:"phase" yaml:"phase"`
	Replicas      ReplicaStatus      `json:"replicas" yaml:"replicas"`
	AllocatedGPUs int                `json:"allocatedGPUs" yaml:"allocatedGPUs"`
	AssignedNodes []string           `json:"assignedNodes,omitempty" yaml:"assignedNodes,omitempty"`
	Conditions    []Condition        `json:"conditions,omitempty" yaml:"conditions,omitempty"`
	ObservedGeneration int64         `json:"observedGeneration,omitempty" yaml:"observedGeneration,omitempty"`
}

// ObjectMeta contains standard Kubernetes object metadata.
type ObjectMeta struct {
	Name        string            `json:"name" yaml:"name"`
	Namespace   string            `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	Labels      map[string]string `json:"labels,omitempty" yaml:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty" yaml:"annotations,omitempty"`
	Generation  int64             `json:"generation,omitempty" yaml:"generation,omitempty"`
}

// TypeMeta contains API version and Kind.
type TypeMeta struct {
	APIVersion string `json:"apiVersion" yaml:"apiVersion"`
	Kind       string `json:"kind" yaml:"kind"`
}

// InferenceCluster is the Schema for the inferenceclusters API.
type InferenceCluster struct {
	TypeMeta   `json:",inline" yaml:",inline"`
	Metadata   ObjectMeta             `json:"metadata" yaml:"metadata"`
	Spec       InferenceClusterSpec   `json:"spec" yaml:"spec"`
	Status     InferenceClusterStatus `json:"status,omitempty" yaml:"status,omitempty"`
}

// SetCondition sets or updates a condition on the InferenceCluster status.
func (ic *InferenceCluster) SetCondition(condType ConditionType, status ConditionStatus, reason, message string) {
	now := time.Now()
	for i, c := range ic.Status.Conditions {
		if c.Type == condType {
			if c.Status != status || c.Reason != reason || c.Message != message {
				ic.Status.Conditions[i].Status = status
				ic.Status.Conditions[i].LastTransitionTime = now
				ic.Status.Conditions[i].Reason = reason
				ic.Status.Conditions[i].Message = message
			}
			return
		}
	}
	ic.Status.Conditions = append(ic.Status.Conditions, Condition{
		Type:               condType,
		Status:             status,
		LastTransitionTime: now,
		Reason:             reason,
		Message:            message,
	})
}

// TopologySpec defines the interconnect topology.
type TopologySpec struct {
	Type types.TopologyType `json:"type" yaml:"type"`
}

// GPUNodeSpec defines the desired specification of a GPUNode.
type GPUNodeSpec struct {
	Provider string          `json:"provider" yaml:"provider"`
	Region   string          `json:"region,omitempty" yaml:"region,omitempty"`
	Rack     string          `json:"rack,omitempty" yaml:"rack,omitempty"`
	GPU      GPUResourceSpec `json:"gpu" yaml:"gpu"`
	Topology TopologySpec    `json:"topology,omitempty" yaml:"topology,omitempty"`
}

// GPUNodeStatus defines the observed state of GPUNode.
type GPUNodeStatus struct {
	Phase            types.NodeState        `json:"phase" yaml:"phase"`
	Health           types.NodeHealth       `json:"health" yaml:"health"`
	GPUs             []types.GPU            `json:"gpus,omitempty" yaml:"gpus,omitempty"`
	AllocatedGPUs    int                    `json:"allocatedGPUs" yaml:"allocatedGPUs"`
	AvailableGPUs    int                    `json:"availableGPUs" yaml:"availableGPUs"`
	Conditions       []Condition            `json:"conditions,omitempty" yaml:"conditions,omitempty"`
	StateTransitions []types.StateTransition `json:"stateTransitions,omitempty" yaml:"stateTransitions,omitempty"`
}

// GPUNode is the Schema for the gpunodes API.
type GPUNode struct {
	TypeMeta `json:",inline" yaml:",inline"`
	Metadata ObjectMeta    `json:"metadata" yaml:"metadata"`
	Spec     GPUNodeSpec   `json:"spec" yaml:"spec"`
	Status   GPUNodeStatus `json:"status,omitempty" yaml:"status,omitempty"`
}

// SetCondition sets or updates a condition on the GPUNode status.
func (gn *GPUNode) SetCondition(condType ConditionType, status ConditionStatus, reason, message string) {
	now := time.Now()
	for i, c := range gn.Status.Conditions {
		if c.Type == condType {
			if c.Status != status || c.Reason != reason || c.Message != message {
				gn.Status.Conditions[i].Status = status
				gn.Status.Conditions[i].LastTransitionTime = now
				gn.Status.Conditions[i].Reason = reason
				gn.Status.Conditions[i].Message = message
			}
			return
		}
	}
	gn.Status.Conditions = append(gn.Status.Conditions, Condition{
		Type:               condType,
		Status:             status,
		LastTransitionTime: now,
		Reason:             reason,
		Message:            message,
	})
}
