# GPU Scheduling & Capacity Defragmentation

## 1. Overview

GPUFlow features a specialized GPU scheduler designed for multi-GPU training and high-throughput inference (vLLM, TensorRT-LLM). It solves key challenges unique to GPU clusters:
- **Interconnect Topology**: NVLink vs PCIe bus bandwidth penalties.
- **Resource Granularity**: Multi-GPU co-location on same NUMA nodes.
- **Capacity Fragmentation**: Free GPUs scattered across nodes preventing multi-GPU allocations.

---

## 2. Scheduling Decision Pipeline

```mermaid
flowchart TD
    REQ["1. Incoming GPURequest<br/>(Model, Count, Memory, Topology, Strategy)"] --> FILTER["2. Candidate Filtering Phase"]

    subgraph FilterPhase ["Candidate Filtering Rules"]
        F1["Rule 1: Node State == READY or ALLOCATED"]
        F2["Rule 2: Node Health == HEALTHY"]
        F3["Rule 3: GPU Model == Request.Model (e.g. H100)"]
        F4["Rule 4: GPU Memory >= Request.MemoryGB"]
        F5["Rule 5: Available GPUs >= Request.Count"]
        F6["Rule 6: Interconnect Topology Match (NVLink)"]
    end

    FILTER --> F1 --> F2 --> F3 --> F4 --> F5 --> F6
    F6 --> CANDIDATES{"Any Candidates<br/>Survive?"}

    CANDIDATES --> |No| ERR_REQUEUE["Reject with InsufficientCapacity<br/>Requeue in WorkQueue with Backoff"]
    CANDIDATES --> |Yes| STRATEGY_SELECT{"3. Apply Selected Strategy"}

    subgraph Strategies ["Scoring Algorithms"]
        S_FIRST["First-Fit<br/>Select first matching node"]
        S_BEST["Best-Fit<br/>Select node with fewest available GPUs"]
        S_BINPACK["BinPack<br/>Score = 0.8 * UtilAfter + 0.2 * Contiguity"]
        S_TOPO["Topology-Aware<br/>Score = Base + 0.3 (NVLink) + Contiguity Weight"]
    end

    STRATEGY_SELECT --> |first-fit| S_FIRST
    STRATEGY_SELECT --> |best-fit| S_BEST
    STRATEGY_SELECT --> |binpack| S_BINPACK
    STRATEGY_SELECT --> |topology-aware| S_TOPO

    S_FIRST --> SELECT_NODE["4. Select Highest Scored Node"]
    S_BEST --> SELECT_NODE
    S_BINPACK --> SELECT_NODE
    S_TOPO --> SELECT_NODE

    SELECT_NODE --> ALLOCATE["5. Allocate Contiguous GPUs on Node"]
    ALLOCATE --> UPDATE_TELEMETRY["6. Update Telemetry & Metrics<br/>(Utilization 30-79%, Temp 55-74°C)"]
    UPDATE_TELEMETRY --> PLACED(["Workload Successfully Placed"])
```

---

## 3. Fragmentation Analysis & Scoring

Fragmentation occurs when total free GPUs across the fleet are sufficient, but no single node has enough contiguous GPUs to satisfy a request (e.g., an 8-GPU NVLink llama-70b replica).

### Scoring Model:

$$\text{nodeFragScore} = 1.0 - \frac{\text{largestContiguousFreeBlock}}{\text{totalFreeGPUs}}$$

$$\text{fleetFragmentationScore} = \frac{\sum (\text{nodeFragScore}_i \times \text{freeGPUs}_i)}{\sum \text{freeGPUs}_i}$$

```mermaid
graph TD
    subgraph Optimal ["Ideal Node: Score = 0.00"]
        O_GPUS["[ GPU 0: Alloc ] [ GPU 1: Alloc ] [ GPU 2: Free ] [ GPU 3: Free ] [ GPU 4: Free ] [ GPU 5: Free ] [ GPU 6: Free ] [ GPU 7: Free ]<br/>Total Free: 6 | Largest Contiguous Block: 6<br/><b>Score = 1 - (6/6) = 0.00</b>"]
    end

    subgraph Fragmented ["Fragmented Node: Score = 0.50"]
        F_GPUS["[ GPU 0: Alloc ] [ GPU 1: Free ] [ GPU 2: Free ] [ GPU 3: Alloc ] [ GPU 4: Free ] [ GPU 5: Free ] [ GPU 6: Free ] [ GPU 7: Alloc ]<br/>Total Free: 5 | Largest Contiguous Block: 3 (GPUs 4-6)<br/><b>Score = 1 - (3/5) = 0.40</b>"]
    end

    subgraph MaximallyFragmented ["Scattered Node: Score = 0.75"]
        M_GPUS["[ GPU 0: Alloc ] [ GPU 1: Free ] [ GPU 2: Alloc ] [ GPU 3: Free ] [ GPU 4: Alloc ] [ GPU 5: Free ] [ GPU 6: Alloc ] [ GPU 7: Free ]<br/>Total Free: 4 | Largest Contiguous Block: 1<br/><b>Score = 1 - (1/4) = 0.75</b>"]
    end
```

---

## 4. Defragmentation Migration Planner

The defragmentation engine analyzes active fleet allocations and calculates an optimal migration plan to consolidate fragmented nodes:

```mermaid
sequenceDiagram
    autonumber
    participant Admin as Operator / Optimizer
    participant Defrag as Defrag Planner
    participant Sched as Scheduler
    participant Fleet as Fleet Simulator

    Admin->>Defrag: POST /api/v1/optimization/plan
    Defrag->>Sched: Read Fleet Fragmentation Score
    Defrag->>Fleet: Inspect Node Workload Allocations

    rect rgb(240, 248, 255)
    Note over Defrag: 1. Sort nodes ascending by allocation (least loaded first)<br/>2. Source: gpu-node-04 (2 GPUs in use)<br/>3. Target: gpu-node-03 (6 GPUs in use, 2 GPUs free)
    end

    Defrag->>Defrag: Compute Migration Move (workload-3 from node-04 → node-03)
    Defrag->>Defrag: Calculate Post-Defrag Metrics:<br/>Nodes Freed: 1 (gpu-node-04)<br/>Frag Before: 0.35 → Frag After: 0.05
    Defrag-->>Admin: Return DefragPlan JSON

    Admin->>Defrag: POST /api/v1/optimization/apply
    Defrag->>Fleet: ReleaseGPUs(gpu-node-04, workload-3)
    Defrag->>Fleet: AllocateGPUs(gpu-node-03, workload-3)
    Fleet-->>Admin: Defragmentation Plan Applied (1 Node Freed Up)
```
