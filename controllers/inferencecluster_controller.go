// Package controllers implements the InferenceClusterReconciler.
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
	"github.com/gpuflow/gpuflow/scheduler"
	"github.com/gpuflow/gpuflow/simulator"
)

// InferenceClusterReconciler reconciles InferenceCluster objects.
type InferenceClusterReconciler struct {
	mu        sync.RWMutex
	clusters  map[string]*v1.InferenceCluster
	fleet     *simulator.FleetSimulator
	scheduler *scheduler.Scheduler
	eventBus  events.Bus
	logger    *slog.Logger
}

// NewInferenceClusterReconciler creates a new reconciler.
func NewInferenceClusterReconciler(
	fleet *simulator.FleetSimulator,
	sched *scheduler.Scheduler,
	bus events.Bus,
	logger *slog.Logger,
) *InferenceClusterReconciler {
	return &InferenceClusterReconciler{
		clusters:  make(map[string]*v1.InferenceCluster),
		fleet:     fleet,
		scheduler: sched,
		eventBus:  bus,
		logger:    logger.With("reconciler", "InferenceCluster"),
	}
}

// Register registers or updates a cluster in the reconciler's cache.
func (r *InferenceClusterReconciler) Register(cluster *v1.InferenceCluster) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clusters[cluster.Metadata.Name] = cluster
}

// Get retrieves a cluster by name.
func (r *InferenceClusterReconciler) Get(name string) (*v1.InferenceCluster, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c, ok := r.clusters[name]
	if !ok {
		return nil, false
	}
	copy := *c
	return &copy, true
}

// List returns all registered clusters.
func (r *InferenceClusterReconciler) List() []*v1.InferenceCluster {
	r.mu.RLock()
	defer r.mu.RUnlock()
	res := make([]*v1.InferenceCluster, 0, len(r.clusters))
	for _, c := range r.clusters {
		copy := *c
		res = append(res, &copy)
	}
	return res
}

// Delete removes a cluster and releases allocated GPUs.
func (r *InferenceClusterReconciler) Delete(ctx context.Context, name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	c, ok := r.clusters[name]
	if !ok {
		return fmt.Errorf("cluster %q not found", name)
	}

	workloadPrefix := fmt.Sprintf("%s-replica", name)
	for _, nodeID := range c.Status.AssignedNodes {
		node, err := r.fleet.GetNode(nodeID)
		if err == nil {
			var toFree []string
			for _, g := range node.GPUs {
				if g.WorkloadID == workloadPrefix {
					toFree = append(toFree, g.ID)
				}
			}
			if len(toFree) > 0 {
				_ = r.fleet.ReleaseGPUs(ctx, nodeID, toFree)
			}
		}
	}

	delete(r.clusters, name)
	_ = r.eventBus.Publish(ctx, events.TopicClusterEvents, events.ClusterDeleted(name, fmt.Sprintf("corr-%d", time.Now().UnixNano())))
	return nil
}

// Reconcile drives actual state toward desired state for an InferenceCluster.
func (r *InferenceClusterReconciler) Reconcile(ctx context.Context, req Request) (ReconcileResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	cluster, exists := r.clusters[req.Name]
	if !exists {
		// Object deleted, nothing to reconcile
		return ReconcileResult{}, nil
	}

	correlationID := fmt.Sprintf("reconcile-%s-%d", req.Name, time.Now().UnixNano())
	r.logger.Info("reconciling InferenceCluster", "name", req.Name, "desiredReplicas", cluster.Spec.Replicas)

	// Step 1: Read current state & compute differences
	desiredReplicas := cluster.Spec.Replicas
	gpusPerReplica := cluster.Spec.Resources.GPU.Count
	targetTotalGPUs := desiredReplicas * gpusPerReplica

	// Verify health of currently assigned GPUs on the simulator
	actualHealthyAllocatedGPUs := 0
	activeNodes := make([]string, 0)
	workloadPrefix := fmt.Sprintf("%s-replica", req.Name)

	for _, nodeID := range cluster.Status.AssignedNodes {
		node, err := r.fleet.GetNode(nodeID)
		if err != nil || node.Health == types.NodeHealthFailed || node.State == types.NodeStateFailed {
			r.logger.Warn("assigned node is unhealthy or missing", "nodeId", nodeID, "cluster", req.Name)
			continue
		}
		// Count healthy allocated GPUs for this cluster's replica
		for _, gpu := range node.GPUs {
			if gpu.Allocated && gpu.WorkloadID == workloadPrefix && gpu.Health == types.GPUHealthHealthy {
				actualHealthyAllocatedGPUs++
			}
		}
		activeNodes = append(activeNodes, nodeID)
	}

	// Step 2: Drift Detection
	driftDetected := false
	if cluster.Status.Phase == types.ClusterPhaseReady && actualHealthyAllocatedGPUs < targetTotalGPUs {
		driftDetected = true
		r.logger.Warn("DRIFT DETECTED: allocated GPUs below target",
			"cluster", req.Name,
			"targetGPUs", targetTotalGPUs,
			"actualHealthyGPUs", actualHealthyAllocatedGPUs)

		cluster.SetCondition(v1.ConditionDriftDetected, v1.ConditionTrue, "GPUCapacityLost",
			fmt.Sprintf("Actual healthy GPUs (%d) < Target GPUs (%d)", actualHealthyAllocatedGPUs, targetTotalGPUs))

		_ = r.eventBus.Publish(ctx, events.TopicClusterEvents, types.Event{
			EventID:       fmt.Sprintf("evt-drift-%d", time.Now().UnixNano()),
			EventType:     types.EventDriftDetected,
			Timestamp:     time.Now(),
			ClusterID:     req.Name,
			CorrelationID: correlationID,
			Data: map[string]string{
				"targetGPUs": fmt.Sprintf("%d", targetTotalGPUs),
				"actualGPUs": fmt.Sprintf("%d", actualHealthyAllocatedGPUs),
			},
		})
	}

	// Step 3: Reconcile replicas
	currentReadyReplicas := actualHealthyAllocatedGPUs / gpusPerReplica
	if currentReadyReplicas < desiredReplicas {
		cluster.Status.Phase = types.ClusterPhaseProvisioning
		cluster.SetCondition(v1.ConditionReady, v1.ConditionFalse, "Provisioning", "Allocating GPU capacity")

		replicasToSchedule := desiredReplicas - currentReadyReplicas
		r.logger.Info("allocating capacity for replicas", "needed", replicasToSchedule)

		strategy := cluster.Spec.Scheduling.Strategy
		if strategy == "" {
			strategy = types.StrategyBinPack
		}
		topology := cluster.Spec.Scheduling.Topology
		if topology == "" {
			topology = types.TopologyNVLink
		}

		gpuReq := types.GPURequest{
			Model:    cluster.Spec.Resources.GPU.Model,
			Count:    gpusPerReplica,
			MemoryGB: cluster.Spec.Resources.GPU.MemoryGB,
			Topology: topology,
			Strategy: strategy,
			Priority: 10,
		}

		for i := 0; i < replicasToSchedule; i++ {
			placement, err := r.scheduler.Schedule(ctx, gpuReq, workloadPrefix)
			if err != nil {
				r.logger.Error("failed to schedule replica", "error", err)
				cluster.SetCondition(v1.ConditionScheduled, v1.ConditionFalse, "InsufficientCapacity", err.Error())
				cluster.Status.Phase = types.ClusterPhaseDegraded
				return ReconcileResult{RequeueAfter: 2 * time.Second}, nil
			}

			activeNodes = append(activeNodes, placement.NodeID)
			actualHealthyAllocatedGPUs += gpusPerReplica
			currentReadyReplicas++
		}
	} else if currentReadyReplicas > desiredReplicas {
		// Scale down
		r.logger.Info("scaling down cluster", "current", currentReadyReplicas, "desired", desiredReplicas)
		replicasToFree := currentReadyReplicas - desiredReplicas
		for i := 0; i < replicasToFree && len(activeNodes) > 0; i++ {
			lastNodeID := activeNodes[len(activeNodes)-1]
			activeNodes = activeNodes[:len(activeNodes)-1]

			node, err := r.fleet.GetNode(lastNodeID)
			if err == nil {
				var toFree []string
				for _, g := range node.GPUs {
					if g.WorkloadID == workloadPrefix {
						toFree = append(toFree, g.ID)
					}
				}
				if len(toFree) > 0 {
					_ = r.fleet.ReleaseGPUs(ctx, lastNodeID, toFree)
				}
			}
			actualHealthyAllocatedGPUs -= gpusPerReplica
			currentReadyReplicas--
		}
		_ = r.eventBus.Publish(ctx, events.TopicClusterEvents, events.ClusterScaled(req.Name, desiredReplicas, correlationID))
	}

	// Step 4: Update status and conditions
	cluster.Status.AssignedNodes = activeNodes
	cluster.Status.AllocatedGPUs = actualHealthyAllocatedGPUs
	cluster.Status.Replicas = v1.ReplicaStatus{
		Desired: desiredReplicas,
		Ready:   currentReadyReplicas,
		Failed:  desiredReplicas - currentReadyReplicas,
	}

	if currentReadyReplicas >= desiredReplicas {
		cluster.Status.Phase = types.ClusterPhaseReady
		cluster.SetCondition(v1.ConditionScheduled, v1.ConditionTrue, "Scheduled", "All replicas scheduled")
		cluster.SetCondition(v1.ConditionReady, v1.ConditionTrue, "Ready", "All replicas healthy and running")

		if driftDetected {
			cluster.SetCondition(v1.ConditionDriftDetected, v1.ConditionFalse, "DriftResolved", "Capacity reconciled back to desired state")
			_ = r.eventBus.Publish(ctx, events.TopicClusterEvents, types.Event{
				EventID:       fmt.Sprintf("evt-drift-res-%d", time.Now().UnixNano()),
				EventType:     types.EventDriftResolved,
				Timestamp:     time.Now(),
				ClusterID:     req.Name,
				CorrelationID: correlationID,
			})
		}

		_ = r.eventBus.Publish(ctx, events.TopicClusterEvents, events.ClusterReady(req.Name, correlationID))
	} else {
		cluster.Status.Phase = types.ClusterPhaseDegraded
		cluster.SetCondition(v1.ConditionReady, v1.ConditionFalse, "Degraded",
			fmt.Sprintf("%d/%d replicas ready", currentReadyReplicas, desiredReplicas))
	}

	cluster.Status.ObservedGeneration = cluster.Metadata.Generation
	return ReconcileResult{}, nil
}
