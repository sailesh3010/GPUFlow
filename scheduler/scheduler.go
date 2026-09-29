// Package scheduler implements GPU-aware workload placement.
// It supports multiple scheduling strategies from simple first-fit to
// topology-aware bin packing, and computes fragmentation metrics.
package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/gpuflow/gpuflow/pkg/types"
)

// NodeSource provides the scheduler with current fleet state.
// This decouples the scheduler from the simulator/provider.
type NodeSource interface {
	ListNodes() []*types.Node
	GetNode(id string) (*types.Node, error)
	AllocateGPUs(ctx context.Context, nodeID string, count int, workloadID string) ([]string, error)
	ReleaseGPUs(ctx context.Context, nodeID string, gpuIDs []string) error
}

// PlacementResult represents the outcome of a scheduling decision.
type PlacementResult struct {
	WorkloadID  string    `json:"workloadId"`
	NodeID      string    `json:"nodeId"`
	GPUIDs      []string  `json:"gpuIds"`
	Strategy    string    `json:"strategy"`
	Score       float64   `json:"score"`
	PlacedAt    time.Time `json:"placedAt"`
}

// Metrics tracks scheduler activity for observability.
type Metrics struct {
	mu                 sync.Mutex
	PlacementTotal     int64
	PlacementFailures  int64
	StrategyUsage      map[string]int64
}

// Scheduler allocates GPU resources to workloads.
type Scheduler struct {
	source  NodeSource
	log     *slog.Logger
	metrics *Metrics
}

// New creates a new scheduler.
func New(source NodeSource, log *slog.Logger) *Scheduler {
	return &Scheduler{
		source: source,
		log:    log,
		metrics: &Metrics{
			StrategyUsage: make(map[string]int64),
		},
	}
}

// Schedule places a GPU request on the best available node.
func (s *Scheduler) Schedule(ctx context.Context, req types.GPURequest, workloadID string) (*PlacementResult, error) {
	strategy := req.Strategy
	if strategy == "" {
		strategy = types.StrategyBestFit
	}

	s.log.Info("scheduling workload",
		slog.String("workloadId", workloadID),
		slog.String("gpuModel", string(req.Model)),
		slog.Int("gpuCount", req.Count),
		slog.String("strategy", string(strategy)),
	)

	candidates := s.findCandidates(req)
	if len(candidates) == 0 {
		s.metrics.mu.Lock()
		s.metrics.PlacementFailures++
		s.metrics.mu.Unlock()

		return nil, fmt.Errorf("no nodes satisfy request: model=%s count=%d memory=%dGB topology=%s",
			req.Model, req.Count, req.MemoryGB, req.Topology)
	}

	var selected *scoredNode
	switch strategy {
	case types.StrategyFirstFit:
		selected = s.firstFit(candidates)
	case types.StrategyBestFit:
		selected = s.bestFit(candidates)
	case types.StrategyBinPack:
		selected = s.binPack(candidates, req)
	case types.StrategyTopologyAware:
		selected = s.topologyAware(candidates, req)
	default:
		selected = s.bestFit(candidates)
	}

	if selected == nil {
		s.metrics.mu.Lock()
		s.metrics.PlacementFailures++
		s.metrics.mu.Unlock()

		return nil, fmt.Errorf("no suitable node found after scoring")
	}

	gpuIDs, err := s.source.AllocateGPUs(ctx, selected.node.ID, req.Count, workloadID)
	if err != nil {
		s.metrics.mu.Lock()
		s.metrics.PlacementFailures++
		s.metrics.mu.Unlock()

		return nil, fmt.Errorf("allocation failed on node %s: %w", selected.node.ID, err)
	}

	s.metrics.mu.Lock()
	s.metrics.PlacementTotal++
	s.metrics.StrategyUsage[string(strategy)]++
	s.metrics.mu.Unlock()

	result := &PlacementResult{
		WorkloadID: workloadID,
		NodeID:     selected.node.ID,
		GPUIDs:     gpuIDs,
		Strategy:   string(strategy),
		Score:      selected.score,
		PlacedAt:   time.Now(),
	}

	s.log.Info("workload placed",
		slog.String("workloadId", workloadID),
		slog.String("nodeId", selected.node.ID),
		slog.Int("gpuCount", len(gpuIDs)),
		slog.Float64("score", selected.score),
		slog.String("strategy", string(strategy)),
	)

	return result, nil
}

// Release frees GPUs allocated to a workload.
func (s *Scheduler) Release(ctx context.Context, nodeID string, gpuIDs []string) error {
	return s.source.ReleaseGPUs(ctx, nodeID, gpuIDs)
}

// scoredNode pairs a node with its scheduling score.
type scoredNode struct {
	node  *types.Node
	score float64
}

// findCandidates returns nodes that meet the GPU request requirements.
func (s *Scheduler) findCandidates(req types.GPURequest) []scoredNode {
	nodes := s.source.ListNodes()
	var candidates []scoredNode

	for _, node := range nodes {
		if !node.IsSchedulable() {
			continue
		}
		if node.GPUModel != req.Model {
			continue
		}
		if req.MemoryGB > 0 {
			// Check that the GPUs have sufficient memory
			if len(node.GPUs) > 0 && node.GPUs[0].MemoryGB < req.MemoryGB {
				continue
			}
		}
		if req.Topology != "" && req.Topology != types.TopologyNone {
			if node.Topology != req.Topology {
				continue
			}
		}

		available := node.AvailableGPUs()
		if available < req.Count {
			continue
		}

		candidates = append(candidates, scoredNode{node: node})
	}

	return candidates
}

// firstFit returns the first node that fits.
func (s *Scheduler) firstFit(candidates []scoredNode) *scoredNode {
	if len(candidates) == 0 {
		return nil
	}
	candidates[0].score = 1.0
	return &candidates[0]
}

// bestFit returns the node with the fewest available GPUs (tightest fit).
func (s *Scheduler) bestFit(candidates []scoredNode) *scoredNode {
	if len(candidates) == 0 {
		return nil
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].node.AvailableGPUs() < candidates[j].node.AvailableGPUs()
	})

	best := &candidates[0]
	// Score: inverse of remaining GPUs (tighter = higher score)
	best.score = 1.0 - float64(best.node.AvailableGPUs())/float64(best.node.GPUCount)
	return best
}

// binPack scores nodes by how efficiently they pack resources.
// It minimizes wasted capacity by preferring nodes that will be most full after allocation.
func (s *Scheduler) binPack(candidates []scoredNode, req types.GPURequest) *scoredNode {
	if len(candidates) == 0 {
		return nil
	}

	for i := range candidates {
		node := candidates[i].node

		// GPU utilization after placement
		allocatedAfter := float64(node.AllocatedGPUs()+req.Count) / float64(node.GPUCount)

		// Contiguity bonus: prefer nodes where free GPUs are clustered together
		contiguity := computeContiguity(node)

		// Combined score: heavily weight utilization, small bonus for contiguity
		candidates[i].score = allocatedAfter*0.8 + contiguity*0.2
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].score > candidates[j].score
	})

	return &candidates[0]
}

// topologyAware extends bin packing with topology preference.
// It strongly prefers nodes with the requested topology and applies
// a contiguity bonus for NVLink placements.
func (s *Scheduler) topologyAware(candidates []scoredNode, req types.GPURequest) *scoredNode {
	if len(candidates) == 0 {
		return nil
	}

	for i := range candidates {
		node := candidates[i].node

		baseScore := float64(node.AllocatedGPUs()+req.Count) / float64(node.GPUCount)

		// Topology match bonus
		topoBonus := 0.0
		if req.Topology != "" && node.Topology == req.Topology {
			topoBonus = 0.3
		}

		// NVLink contiguity is more important
		contiguity := computeContiguity(node)
		contiguityWeight := 0.1
		if node.Topology == types.TopologyNVLink {
			contiguityWeight = 0.2
		}

		candidates[i].score = baseScore*0.5 + topoBonus + contiguity*contiguityWeight
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].score > candidates[j].score
	})

	return &candidates[0]
}

// computeContiguity measures how clustered free GPUs are on a node.
// Returns 1.0 for perfectly contiguous, 0.0 for maximally fragmented.
func computeContiguity(node *types.Node) float64 {
	if node.GPUCount == 0 {
		return 0
	}

	freeCount := 0
	maxRun := 0
	currentRun := 0

	for _, gpu := range node.GPUs {
		if gpu.IsAvailable() {
			freeCount++
			currentRun++
			if currentRun > maxRun {
				maxRun = currentRun
			}
		} else {
			currentRun = 0
		}
	}

	if freeCount == 0 {
		return 1.0 // no free GPUs — not fragmented
	}

	return float64(maxRun) / float64(freeCount)
}

// CapacityReport describes the fleet's current capacity.
type CapacityReport struct {
	TotalNodes    int                      `json:"totalNodes"`
	TotalGPUs     int                      `json:"totalGPUs"`
	AllocatedGPUs int                      `json:"allocatedGPUs"`
	AvailableGPUs int                      `json:"availableGPUs"`
	Utilization   float64                  `json:"utilization"`
	ByModel       map[string]ModelCapacity `json:"byModel"`
}

// ModelCapacity describes capacity for a specific GPU model.
type ModelCapacity struct {
	Total     int     `json:"total"`
	Allocated int     `json:"allocated"`
	Available int     `json:"available"`
	Utilization float64 `json:"utilization"`
}

// Capacity returns a report of current fleet capacity.
func (s *Scheduler) Capacity() CapacityReport {
	nodes := s.source.ListNodes()

	report := CapacityReport{
		ByModel: make(map[string]ModelCapacity),
	}

	for _, node := range nodes {
		report.TotalNodes++

		model := string(node.GPUModel)
		mc := report.ByModel[model]

		for _, gpu := range node.GPUs {
			report.TotalGPUs++
			mc.Total++

			if gpu.Allocated {
				report.AllocatedGPUs++
				mc.Allocated++
			} else if gpu.IsAvailable() {
				report.AvailableGPUs++
				mc.Available++
			}
		}

		report.ByModel[model] = mc
	}

	if report.TotalGPUs > 0 {
		report.Utilization = float64(report.AllocatedGPUs) / float64(report.TotalGPUs)
	}
	for model, mc := range report.ByModel {
		if mc.Total > 0 {
			mc.Utilization = float64(mc.Allocated) / float64(mc.Total)
		}
		report.ByModel[model] = mc
	}

	return report
}

// FragmentationReport describes resource fragmentation across the fleet.
type FragmentationReport struct {
	OverallScore float64                       `json:"overallScore"` // 0=perfect, 1=fully fragmented
	NodeScores   []NodeFragmentation           `json:"nodeScores"`
}

// NodeFragmentation describes fragmentation on a single node.
type NodeFragmentation struct {
	NodeID           string  `json:"nodeId"`
	TotalGPUs        int     `json:"totalGPUs"`
	FreeGPUs         int     `json:"freeGPUs"`
	LargestFreeBlock int     `json:"largestFreeBlock"`
	FragmentScore    float64 `json:"fragmentScore"` // 0=contiguous, 1=scattered
}

// Fragmentation computes fragmentation metrics for the fleet.
func (s *Scheduler) Fragmentation() FragmentationReport {
	nodes := s.source.ListNodes()

	report := FragmentationReport{}
	totalFree := 0
	totalScattered := 0.0

	for _, node := range nodes {
		if !node.IsSchedulable() {
			continue
		}

		freeCount := 0
		maxBlock := 0
		currentBlock := 0

		for _, gpu := range node.GPUs {
			if gpu.IsAvailable() {
				freeCount++
				currentBlock++
				if currentBlock > maxBlock {
					maxBlock = currentBlock
				}
			} else {
				currentBlock = 0
			}
		}

		var fragScore float64
		if freeCount == 0 {
			fragScore = 0 // fully packed — no fragmentation
		} else {
			// fragScore = 1 - (largest contiguous block / total free)
			fragScore = 1.0 - float64(maxBlock)/float64(freeCount)
		}

		report.NodeScores = append(report.NodeScores, NodeFragmentation{
			NodeID:           node.ID,
			TotalGPUs:        node.GPUCount,
			FreeGPUs:         freeCount,
			LargestFreeBlock: maxBlock,
			FragmentScore:    fragScore,
		})

		totalFree += freeCount
		totalScattered += fragScore * float64(freeCount)
	}

	if totalFree > 0 {
		report.OverallScore = totalScattered / float64(totalFree)
	}

	return report
}

// GetMetrics returns current scheduler metrics.
func (s *Scheduler) GetMetrics() Metrics {
	s.metrics.mu.Lock()
	defer s.metrics.mu.Unlock()

	usage := make(map[string]int64)
	for k, v := range s.metrics.StrategyUsage {
		usage[k] = v
	}
	return Metrics{
		PlacementTotal:    s.metrics.PlacementTotal,
		PlacementFailures: s.metrics.PlacementFailures,
		StrategyUsage:     usage,
	}
}

// ─── Defragmentation Engine ───

// MigrationMove describes a single workload migration.
type MigrationMove struct {
	WorkloadID string `json:"workloadId"`
	FromNodeID string `json:"fromNodeId"`
	FromGPUIDs []string `json:"fromGpuIds"`
	ToNodeID   string `json:"toNodeId"`
	Priority   int    `json:"priority"`
}

// DefragPlan describes a complete defragmentation plan.
type DefragPlan struct {
	Moves                    []MigrationMove `json:"moves"`
	UtilizationBefore        float64         `json:"utilizationBefore"`
	UtilizationAfter         float64         `json:"utilizationAfter"`
	FragmentationBefore      float64         `json:"fragmentationBefore"`
	FragmentationAfter       float64         `json:"fragmentationAfter"`
	NodesFreedUp             int             `json:"nodesFreedUp"`
	EstimatedDisruptionScore float64         `json:"estimatedDisruptionScore"` // 0=none, 1=max
}

// PlanDefragmentation generates a migration plan to reduce fragmentation.
// It does NOT execute the plan — the caller must review and apply.
func (s *Scheduler) PlanDefragmentation() (*DefragPlan, error) {
	nodes := s.source.ListNodes()

	// Separate into source and target nodes
	type nodeInfo struct {
		node       *types.Node
		allocated  int
		available  int
		workloads  map[string][]string // workloadID -> gpuIDs
	}

	var infos []nodeInfo
	for _, node := range nodes {
		if !node.IsSchedulable() {
			continue
		}

		ni := nodeInfo{
			node:      node,
			allocated: node.AllocatedGPUs(),
			available: node.AvailableGPUs(),
			workloads: make(map[string][]string),
		}

		for _, gpu := range node.GPUs {
			if gpu.Allocated && gpu.WorkloadID != "" {
				ni.workloads[gpu.WorkloadID] = append(ni.workloads[gpu.WorkloadID], gpu.ID)
			}
		}

		infos = append(infos, ni)
	}

	if len(infos) < 2 {
		return nil, fmt.Errorf("need at least 2 schedulable nodes for defragmentation")
	}

	// Compute before metrics
	fragBefore := s.Fragmentation()
	capBefore := s.Capacity()

	// Strategy: move workloads from lightly-loaded nodes to heavily-loaded ones
	// Sort by utilization ascending (most empty first = best source)
	sort.Slice(infos, func(i, j int) bool {
		utilI := float64(infos[i].allocated) / float64(infos[i].node.GPUCount)
		utilJ := float64(infos[j].allocated) / float64(infos[j].node.GPUCount)
		return utilI < utilJ
	})

	var moves []MigrationMove
	nodesFreed := 0

	for si := 0; si < len(infos); si++ {
		source := &infos[si]
		if source.allocated == 0 {
			continue // already empty
		}

		// Try to move all workloads from this source to other nodes
		for wlID, gpuIDs := range source.workloads {
			gpuNeeded := len(gpuIDs)

			// Find a target with enough free capacity
			for ti := len(infos) - 1; ti > si; ti-- {
				target := &infos[ti]

				if target.available >= gpuNeeded && target.node.GPUModel == source.node.GPUModel {
					moves = append(moves, MigrationMove{
						WorkloadID: wlID,
						FromNodeID: source.node.ID,
						FromGPUIDs: gpuIDs,
						ToNodeID:   target.node.ID,
					})

					target.available -= gpuNeeded
					target.allocated += gpuNeeded
					source.available += gpuNeeded
					source.allocated -= gpuNeeded
					break
				}
			}
		}

		if source.allocated == 0 {
			nodesFreed++
		}
	}

	// Estimate post-defrag fragmentation
	estimatedFragAfter := math.Max(0, fragBefore.OverallScore-0.1*float64(len(moves)))

	plan := &DefragPlan{
		Moves:               moves,
		UtilizationBefore:   capBefore.Utilization,
		UtilizationAfter:    capBefore.Utilization, // same total allocation, different distribution
		FragmentationBefore: fragBefore.OverallScore,
		FragmentationAfter:  estimatedFragAfter,
		NodesFreedUp:        nodesFreed,
	}

	if len(moves) > 0 {
		plan.EstimatedDisruptionScore = float64(len(moves)) / float64(len(infos))
	}

	return plan, nil
}
