# GPU Scheduling

## Overview

GPUFlow's scheduler places GPU workloads on nodes using configurable strategies. The scheduler considers GPU model, memory, topology, current allocations, and fragmentation when making placement decisions.

## Scheduling Strategies

### 1. First Fit

The simplest strategy. Returns the first node that satisfies all constraints.

- **Pros**: Fast, predictable
- **Cons**: Can lead to unbalanced utilization
- **Use case**: Development, testing

### 2. Best Fit

Selects the node with the fewest available GPUs (tightest fit after placement).

- **Pros**: Reduces waste on individual nodes
- **Cons**: Can leave many partially-used nodes
- **Use case**: When minimizing per-node waste is important

### 3. Bin Pack

Scores nodes by how full they will be after placement, preferring to pack workloads tightly.

**Scoring formula:**
```
score = utilization_after * 0.8 + contiguity * 0.2
```

Where:
- `utilization_after = (allocated + requested) / total`
- `contiguity = largest_contiguous_free_block / total_free`

- **Pros**: Consolidates workloads, frees up entire nodes
- **Cons**: May increase blast radius of node failure
- **Use case**: Cost optimization, maximizing utilization

### 4. Topology-Aware

Extends bin packing with topology preferences (e.g., NVLink interconnects).

**Scoring formula:**
```
score = base * 0.5 + topology_bonus + contiguity * weight
```

Where:
- `topology_bonus = 0.3` if topology matches
- `weight = 0.2` for NVLink, `0.1` otherwise

- **Pros**: Optimizes for GPU-to-GPU communication latency
- **Cons**: More restrictive, may reject otherwise viable nodes
- **Use case**: Multi-GPU training, tensor parallelism

## Candidate Filtering

Before scoring, candidates are filtered by:
1. ✅ Node is schedulable (READY or ALLOCATED + HEALTHY)
2. ✅ GPU model matches request
3. ✅ GPU memory meets minimum
4. ✅ Topology matches (if specified)
5. ✅ Sufficient available GPUs

## Fragmentation

### Definition

Fragmentation occurs when free GPUs exist but cannot satisfy placement requirements because they are scattered across nodes.

### Fragmentation Score

```
nodeFragScore = 1 - (largestContiguousFreeBlock / totalFreeGPUs)
```

| Example | Free GPUs | Largest Block | Score |
|---------|-----------|---------------|-------|
| `--------` (all free) | 8 | 8 | 0.00 |
| `AAAA----` | 4 | 4 | 0.00 |
| `A-A-A---` | 5 | 3 | 0.40 |
| `A-A-A-A-` | 4 | 1 | 0.75 |

### Fleet-Wide Score

```
overallScore = Σ(nodeFragScore × nodeFreeGPUs) / totalFreeGPUs
```

## Defragmentation

The defrag planner generates migration plans to consolidate workloads:

1. Sort nodes by utilization (ascending)
2. For each lightly-loaded source node:
   - Find a target with matching GPU model and sufficient capacity
   - Plan workload migration from source to target
3. Return plan with before/after metrics

The planner never executes automatically — the plan must be reviewed and applied.
