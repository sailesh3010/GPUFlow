# GPUFlow — Kubernetes-Native GPU Fleet Control Plane

GPUFlow is a production-grade control plane for managing a simulated heterogeneous GPU inference fleet. It demonstrates the distributed systems engineering patterns required for infrastructure and control-plane platforms: Kubernetes controllers, Custom Resource Definitions (CRDs), level-triggered reconciliation loops, drift detection, GPU-aware scheduling, bin packing, capacity defragmentation, automated self-healing, durable workflow orchestration (Temporal pattern), event streaming (Kafka), and Prometheus observability.

> **Core Philosophy:** Desired state is declared declaratively by the user or client; GPUFlow continuously reconciles actual state toward desired state using event-driven watch mechanisms, level-triggered drift detection, and deterministic self-healing pipelines.

---

## Table of Contents

1. [Architecture & System Design](#1-architecture--system-design)
2. [Quick Start & Local Development](#2-quick-start--local-development)
3. [Deterministic Demo Walkthrough](#3-deterministic-demo-walkthrough)
4. [Custom Resource Definitions (CRDs)](#4-custom-resource-definitions-crds)
5. [Reconciliation Engine & Drift Detection](#5-reconciliation-engine--drift-detection)
6. [Durable Workflows (Temporal Pattern)](#6-durable-workflows-temporal-pattern)
7. [Event-Driven Streaming (Kafka)](#7-event-driven-streaming-kafka)
8. [GPU Scheduling & Defragmentation](#8-gpu-scheduling--defragmentation)
9. [Node Finite State Machine](#9-node-finite-state-machine)
10. [Automated Failure Recovery & Self-Healing](#10-automated-failure-recovery--self-healing)
11. [REST API Reference](#11-rest-api-reference)
12. [CLI Reference](#12-cli-reference)
13. [Observability, Metrics & Grafana Dashboard](#13-observability-metrics--grafana-dashboard)
14. [Testing & Quality Verification](#14-testing--quality-verification)
15. [Repository Structure](#15-repository-structure)

---

## 1. Architecture & System Design

```mermaid
graph TD
    subgraph ClientLayer ["1. Client & Ingress Layer"]
        CLI["GPUFlow CLI (Cobra)"]
        KUBECTL["Kubectl / GitOps"]
        REST_CLIENT["HTTP REST Client"]
    end

    subgraph APILayer ["2. Declarative API & CRD Layer"]
        API_ROUTER["REST API Server (:8080)"]
        CRD_IC["CRD: InferenceCluster.gpuflow.io"]
        CRD_GN["CRD: GPUNode.gpuflow.io"]
    end

    subgraph ControlPlane ["3. Control Plane & Reconciliation Engine"]
        WQ["Rate-Limited WorkQueue (Exponential Backoff)"]
        IC_RECONCILER["InferenceClusterReconciler"]
        GN_RECONCILER["GPUNodeReconciler"]
        DRIFT_ENGINE["Drift Detection Engine"]
        HEALTH_DETECTOR["Fleet Health Monitor"]
    end

    subgraph SchedulingLayer ["4. GPU Scheduling & Optimization Engine"]
        SCHED["GPU Scheduler"]
        STRAT_BINPACK["Strategy: BinPack (Contiguity + Pack)"]
        STRAT_TOPO["Strategy: Topology-Aware (NVLink)"]
        STRAT_BEST["Strategy: Best-Fit"]
        DEFRAG_ENGINE["Defragmentation Migration Planner"]
    end

    subgraph WorkflowLayer ["5. Durable Workflow Orchestrator (Temporal Pattern)"]
        WF_ENGINE["Durable Workflow Engine"]
        CHECKPOINT_STORE["Execution History & Checkpoint Store"]
        WF_PROV["ProvisionNodeWorkflow (8 Activities)"]
        WF_REPAIR["RepairNodeWorkflow (Automated Remediation)"]
        WF_SCALE["ScaleClusterWorkflow"]
        WF_DECOM["DecommissionNodeWorkflow"]
    end

    subgraph EventStreamingLayer ["6. Event Streaming (Kafka)"]
        KAFKA_BUS["Kafka Event Bus Adapter"]
        TOPIC_NODE["gpuflow.node.events"]
        TOPIC_CLUSTER["gpuflow.cluster.events"]
        TOPIC_HEALTH["gpuflow.health.events"]
        TOPIC_SCHED["gpuflow.scheduler.events"]
    end

    subgraph InfrastructureLayer ["7. Fleet & Infrastructure Provider Layer"]
        PROV_IFACE["Provider Abstraction"]
        MOCK_PROV["MockProvider / LocalProvider"]
        SIM_FLEET["GPU Fleet Simulator (Thread-Safe RWMutex)"]
        NODE_01["gpu-node-01: H100 x8 (NVLink)"]
        NODE_02["gpu-node-02: H100 x8 (NVLink)"]
        NODE_03["gpu-node-03: A100 x8 (PCIe)"]
        NODE_04["gpu-node-04: A100 x8 (PCIe)"]
        SPARE_05["gpu-node-05: Spare Replacement"]
    end

    subgraph ObservabilityLayer ["8. Observability & Telemetry"]
        METRICS_REG["Prometheus Exporter (/metrics)"]
        GRAFANA["Grafana Dashboard"]
    end

    %% Wiring
    CLI --> API_ROUTER
    KUBECTL --> CRD_IC
    KUBECTL --> CRD_GN
    REST_CLIENT --> API_ROUTER

    API_ROUTER --> CRD_IC
    API_ROUTER --> CRD_GN
    CRD_IC --> WQ
    CRD_GN --> WQ

    WQ --> IC_RECONCILER
    WQ --> GN_RECONCILER

    IC_RECONCILER --> DRIFT_ENGINE
    IC_RECONCILER --> SCHED
    GN_RECONCILER --> WF_ENGINE

    SCHED --> STRAT_BINPACK
    SCHED --> STRAT_TOPO
    SCHED --> STRAT_BEST
    SCHED --> DEFRAG_ENGINE

    WF_ENGINE --> CHECKPOINT_STORE
    WF_ENGINE --> WF_PROV
    WF_ENGINE --> WF_REPAIR
    WF_ENGINE --> WF_SCALE
    WF_ENGINE --> WF_DECOM

    IC_RECONCILER --> KAFKA_BUS
    HEALTH_DETECTOR --> KAFKA_BUS
    GN_RECONCILER --> KAFKA_BUS

    KAFKA_BUS --> TOPIC_NODE
    KAFKA_BUS --> TOPIC_CLUSTER
    KAFKA_BUS --> TOPIC_HEALTH
    KAFKA_BUS --> TOPIC_SCHED

    TOPIC_HEALTH --> HEALTH_DETECTOR
    HEALTH_DETECTOR --> WF_REPAIR

    SCHED --> PROV_IFACE
    WF_PROV --> PROV_IFACE
    WF_REPAIR --> PROV_IFACE
    PROV_IFACE --> MOCK_PROV
    MOCK_PROV --> SIM_FLEET

    SIM_FLEET --> NODE_01
    SIM_FLEET --> NODE_02
    SIM_FLEET --> NODE_03
    SIM_FLEET --> NODE_04
    SIM_FLEET --> SPARE_05

    SIM_FLEET --> METRICS_REG
    SCHED --> METRICS_REG
    IC_RECONCILER --> METRICS_REG
    METRICS_REG --> GRAFANA
```

---

## 2. Quick Start & Local Development

Designed specifically to run comfortably on a standard laptop (e.g. Windows with WSL2/Ubuntu, macOS, or Linux, requiring only 8–12 GB RAM) with **no physical NVIDIA GPU required**:

```bash
# 1. Build binary
make build

# 2. Run all 57 tests across 9 packages with 0 cached results
make test

# 3. Run the deterministic end-to-end demo
make demo

# 4. Start the control plane and API server (:8080)
make serve
```

### Resource Profiles:

| Profile | Components Active | Memory Footprint | Description |
| :--- | :--- | :--- | :--- |
| `minimal` | GPUFlow + Fleet Simulator | ~120 MB | Local unit testing and CLI development |
| `events` | + Kafka Event Streaming | ~500 MB | Event-driven pub/sub integration |
| `full` | + Workflows + Prometheus + Grafana | ~1.5 GB | Full production control-plane stack |

---

## 3. Deterministic Demo Walkthrough

Run `make demo` or `go run ./cmd/gpuflow demo` to experience the complete control-plane lifecycle:

```text
╔══════════════════════════════════════╗
║        GPUFlow Control Plane         ║
║            DEMO MODE                 ║
╚══════════════════════════════════════╝

[1/8] Creating simulated fleet...
      Nodes: 4 | GPUs: 32 | H100: 16 | A100: 16

[2/8] Initializing GPU scheduler...
      Strategies: first-fit, best-fit, binpack, topology-aware

[3/8] Creating inference cluster 'llama-cluster'...
      Replica 0 → gpu-node-01 (8 GPUs)
      Replica 1 → gpu-node-02 (8 GPUs)

[4/8] Cluster status:

      Cluster: llama-cluster
      ────────────────────────────────────────
      Desired Replicas    2
      Ready Replicas      2
      GPUs Allocated      16
      Status              READY

[5/8] Fleet utilization:
      Total GPUs       32
      Allocated        16
      Available        16
      Utilization      50%

[6/8] Creating fragmented allocation pattern...
      Fragmentation Score: 0.00 (scattered capacity detected)

[7/8] Running defragmentation planner...
      Moves needed:       1
      Frag before:        0.00
      Frag after (est.):  0.00
      Nodes freed:        1

[8/8] Simulating node failure and self-healing...

      [EVENT] NODE_FAILED gpu-node-03
      [HEALTH] Node marked DEGRADED
      [REMEDIATION] Draining workloads...
      [SCHEDULER] Searching replacement capacity...
      [TEMPORAL] Starting RepairNodeWorkflow
      [PROVISION] gpu-node-05
      [VALIDATION] GPU ........ PASS
      [VALIDATION] CUDA ....... PASS
      [VALIDATION] Network .... PASS
      [REMEDIATION] Replacement READY
      [RESCHEDULE] Workloads migrated
      [STATUS] Fleet HEALTHY (4/5 nodes healthy)

════════════════════════════════════════
  Demo complete!
════════════════════════════════════════
```

---

## 4. Custom Resource Definitions (CRDs)

GPUFlow defines Kubernetes-native declarative schemas installable into any cluster via `kubectl apply -f deploy/crds/` or Helm:

### `InferenceCluster` (`deploy/crds/gpuflow.io_inferenceclusters.yaml`)

```yaml
apiVersion: gpuflow.io/v1
kind: InferenceCluster
metadata:
  name: llama-cluster
spec:
  replicas: 2
  model:
    name: llama-70b
  runtime:
    name: vllm
  resources:
    gpu:
      model: H100
      count: 8
      memoryGB: 80
  scheduling:
    topology: NVLINK
    strategy: binpack
  availability:
    minHealthy: 1
status:
  phase: READY
  replicas:
    desired: 2
    ready: 2
    failed: 0
  allocatedGPUs: 16
  assignedNodes:
    - gpu-node-01
    - gpu-node-02
  conditions:
    - type: Ready
      status: "True"
      lastTransitionTime: "2026-09-29T23:30:00Z"
      reason: "Ready"
      message: "All replicas healthy and running"
```

### `GPUNode` (`deploy/crds/gpuflow.io_gpunodes.yaml`)

```yaml
apiVersion: gpuflow.io/v1
kind: GPUNode
metadata:
  name: gpu-node-01
spec:
  provider: local
  gpu:
    model: H100
    count: 8
    memoryGB: 80
  topology:
    type: NVLINK
status:
  phase: READY
  health: HEALTHY
  allocatedGPUs: 8
  availableGPUs: 0
```

---

## 5. Reconciliation Engine & Drift Detection

The reconciler enforces desired state through an asynchronous, rate-limited workqueue with exponential backoff:

```mermaid
flowchart TD
    START(["Reconcile(Request) Triggered"]) --> READ_DESIRED["1. Read Desired Spec<br/>(replicas, GPU model, count, topology)"]
    READ_DESIRED --> READ_ACTUAL["2. Read Observed Fleet State<br/>(assigned nodes, healthy allocated GPUs)"]
    READ_ACTUAL --> CALC_DIFF{"3. Compare Desired vs Actual"}

    CALC_DIFF --> |"Healthy GPUs < Target GPUs"| DRIFT["4a. Drift Detected!<br/>Lost capacity due to node/GPU failure"]
    DRIFT --> SET_DRIFT_COND["Set Condition: DriftDetected=True"]
    SET_DRIFT_COND --> PUB_DRIFT["Publish DRIFT_DETECTED event to Kafka"]
    PUB_DRIFT --> SCHED_REPLACE["Invoke GPU Scheduler for Replacement Capacity"]
    SCHED_REPLACE --> ALLOC_NEW["Allocate GPUs on Replacement Node"]
    ALLOC_NEW --> CLEAR_DRIFT["Set Condition: DriftDetected=False<br/>Publish DRIFT_RESOLVED"]

    CALC_DIFF --> |"Actual Ready Replicas < Desired"| SCALE_UP["4b. Scale Up Needed"]
    SCALE_UP --> SCHED_REPLICA["Scheduler matches candidates (BinPack/NVLink)"]
    SCHED_REPLICA --> ALLOC_GPUS["Fleet.AllocateGPUs(workloadID)"]

    CALC_DIFF --> |"Actual Ready Replicas > Desired"| SCALE_DOWN["4c. Scale Down Needed"]
    SCALE_DOWN --> RELEASE_GPUS["Gracefully drain & Fleet.ReleaseGPUs()"]

    CALC_DIFF --> |"Actual == Desired & Healthy"| NOOP["4d. Desired State Met"]

    CLEAR_DRIFT --> UPDATE_STATUS["5. Update Status Subresource<br/>(Phase=READY, Replicas.Ready, Conditions)"]
    ALLOC_GPUS --> UPDATE_STATUS
    RELEASE_GPUS --> UPDATE_STATUS
    NOOP --> UPDATE_STATUS

    UPDATE_STATUS --> EMIT_READY["Publish CLUSTER_READY event"]
    EMIT_READY --> WAIT_EVENT(["Sleep / Wait for Next Event or Watch Trigger"])
```

---

## 6. Durable Workflows (Temporal Pattern)

Bare-metal hardware provisioning requires long-running, multi-stage pipelines. GPUFlow implements checkpointing and replay so that worker crashes resume seamlessly without repeating completed steps:

```mermaid
sequenceDiagram
    autonumber
    participant Engine as Workflow Engine
    participant Checkpoint as Checkpoint Store
    participant ProvWF as ProvisionNodeWorkflow
    participant Worker as Execution Worker
    participant Fleet as Fleet Simulator

    Engine->>Checkpoint: StartWorkflow(wf-prov-01, IdempotencyKey)
    Checkpoint-->>Engine: Execution Record Created (RUNNING)

    rect rgb(240, 248, 255)
    Note over Worker,Fleet: Step 1-3: Hardware & OS Initialization
    ProvWF->>Worker: ExecuteActivity(DiscoverNode)
    Worker-->>Checkpoint: Save ActivityRecord (Success, Step 1)
    ProvWF->>Worker: ExecuteActivity(ProvisionOS)
    Worker->>Fleet: TransitionNode(PROVISIONING -> OS_READY)
    Worker-->>Checkpoint: Save ActivityRecord (Success, Step 2)
    ProvWF->>Worker: ExecuteActivity(InstallDriver)
    Worker->>Fleet: TransitionNode(DRIVER_INSTALLING)
    Worker-->>Checkpoint: Save ActivityRecord (Success, Step 3)
    end

    rect rgb(255, 235, 235)
    Note over Worker: 💥 WORKER CRASH SIMULATION<br/>Process terminated abruptly
    end

    rect rgb(245, 255, 245)
    Note over Engine,Checkpoint: Worker Resumption & Replay
    Engine->>Checkpoint: GetExecution(wf-prov-01)
    Checkpoint-->>Engine: Loaded 3 Completed Steps from History
    Engine->>ProvWF: Resume Execution
    Note over ProvWF: Steps 1, 2, 3 skipped via Checkpoint Replay!
    end

    rect rgb(240, 248, 255)
    Note over Worker,Fleet: Step 4-8: CUDA, Network & Validation
    ProvWF->>Worker: ExecuteActivity(InstallCUDA)
    Worker->>Fleet: TransitionNode(CUDA_READY)
    Worker-->>Checkpoint: Save ActivityRecord (Success, Step 4)
    ProvWF->>Worker: ExecuteActivity(ConfigureNetwork)
    Worker-->>Checkpoint: Save ActivityRecord (Success, Step 5)
    ProvWF->>Worker: ExecuteActivity(ValidateGPU)
    Worker->>Fleet: TransitionNode(VALIDATING)
    Worker-->>Checkpoint: Save ActivityRecord (Success, Step 6)
    ProvWF->>Worker: ExecuteActivity(ValidateNetwork)
    Worker-->>Checkpoint: Save ActivityRecord (Success, Step 7)
    ProvWF->>Worker: ExecuteActivity(RegisterNode)
    Worker->>Fleet: TransitionNode(READY)
    Worker-->>Checkpoint: Save ActivityRecord (Success, Step 8)
    end

    ProvWF->>Engine: Complete(Status=COMPLETED)
    Engine->>Checkpoint: Mark Workflow COMPLETED
```

---

## 7. Event-Driven Streaming (Kafka)

```mermaid
graph LR
    subgraph Producers ["Event Producers"]
        P_NODE["GPUNode Controller"]
        P_CLUS["Cluster Reconciler"]
        P_HLTH["Health Detector"]
        P_SCHD["Scheduler Engine"]
    end

    subgraph KafkaCore ["Kafka Cluster & Topics"]
        T_NODE["gpuflow.node.events<br/><i>Partition Key: NodeID</i>"]
        T_CLUS["gpuflow.cluster.events<br/><i>Partition Key: ClusterID</i>"]
        T_HLTH["gpuflow.health.events<br/><i>Partition Key: NodeID</i>"]
        T_SCHD["gpuflow.scheduler.events<br/><i>Partition Key: WorkloadID</i>"]
        T_AUDT["gpuflow.audit.events<br/><i>Partition Key: CorrelationID</i>"]
    end

    subgraph ConsumerGroups ["Consumer Groups"]
        C_REMED["Consumer Group: remediation-worker<br/>(Auto Self-Healing Pipeline)"]
        C_SCHED["Consumer Group: scheduler-engine<br/>(Eviction & Migration)"]
        C_AUDIT["Consumer Group: audit-logger<br/>(Compliance & Tracking)"]
        C_METRC["Consumer Group: telemetry-exporter<br/>(Prometheus / SIEM)"]
    end

    P_NODE -->|NODE_DISCOVERED, NODE_READY| T_NODE
    P_CLUS -->|CLUSTER_CREATED, DRIFT_DETECTED| T_CLUS
    P_HLTH -->|NODE_FAILED, GPU_FAILED| T_HLTH
    P_SCHD -->|WORKLOAD_PLACED, DEFRAG_PLAN| T_SCHD
    P_CLUS -->|AUDIT| T_AUDT

    T_HLTH --> C_REMED
    T_NODE --> C_SCHED
    T_CLUS --> C_SCHED
    T_AUDT --> C_AUDIT
    T_NODE --> C_METRC
    T_HLTH --> C_METRC
```

---

## 8. GPU Scheduling & Defragmentation

### Decision Pipeline:

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

### Fragmentation Scoring Model:

$$\text{nodeFragScore} = 1.0 - \frac{\text{largestContiguousFreeBlock}}{\text{totalFreeGPUs}}$$

$$\text{fleetFragmentationScore} = \frac{\sum (\text{nodeFragScore}_i \times \text{freeGPUs}_i)}{\sum \text{freeGPUs}_i}$$

```mermaid
graph TD
    subgraph Optimal ["Ideal Node: Score = 0.00"]
        O_GPUS["[ GPU 0: Alloc ] [ GPU 1: Alloc ] [ GPU 2: Free ] [ GPU 3: Free ] [ GPU 4: Free ] [ GPU 5: Free ] [ GPU 6: Free ] [ GPU 7: Free ]<br/>Total Free: 6 | Largest Contiguous Block: 6<br/><b>Score = 1 - (6/6) = 0.00</b>"]
    end

    subgraph Fragmented ["Fragmented Node: Score = 0.40"]
        F_GPUS["[ GPU 0: Alloc ] [ GPU 1: Free ] [ GPU 2: Free ] [ GPU 3: Alloc ] [ GPU 4: Free ] [ GPU 5: Free ] [ GPU 6: Free ] [ GPU 7: Alloc ]<br/>Total Free: 5 | Largest Contiguous Block: 3<br/><b>Score = 1 - (3/5) = 0.40</b>"]
    end

    subgraph MaximallyFragmented ["Scattered Node: Score = 0.75"]
        M_GPUS["[ GPU 0: Alloc ] [ GPU 1: Free ] [ GPU 2: Alloc ] [ GPU 3: Free ] [ GPU 4: Alloc ] [ GPU 5: Free ] [ GPU 6: Alloc ] [ GPU 7: Free ]<br/>Total Free: 4 | Largest Contiguous Block: 1<br/><b>Score = 1 - (1/4) = 0.75</b>"]
    end
```

---

## 9. Node Finite State Machine

```mermaid
flowchart TD
    subgraph Provisioning ["1. Bare-Metal Provisioning Lifecycle"]
        S_DISC["DISCOVERED<br/><i>Node detected on fabric</i>"]
        S_PROV["PROVISIONING<br/><i>Initiate PXE OS boot</i>"]
        S_OS["OS_READY<br/><i>Ubuntu 22.04 LTS installed</i>"]
        S_DRV["DRIVER_INSTALLING<br/><i>NVIDIA 535 driver</i>"]
        S_CUDA["CUDA_READY<br/><i>CUDA 12.2 toolkit verified</i>"]
        S_VAL["VALIDATING<br/><i>Run DCGM & NVLink tests</i>"]
    end

    subgraph ActiveService ["2. Production Service"]
        S_READY["READY<br/><i>Healthy & Schedulable</i>"]
        S_ALLOC["ALLOCATED<br/><i>Workloads running</i>"]
    end

    subgraph SelfHealing ["3. Automated Self-Healing Pipeline"]
        S_DEG["DEGRADED<br/><i>Partial GPU/Thermal failure</i>"]
        S_FAIL["FAILED<br/><i>XID 31 / Unreachable</i>"]
        S_REP["REPAIRING<br/><i>BMC IPMI reset & repair</i>"]
    end

    subgraph DecomLifecycle ["4. Decommission Lifecycle"]
        S_DRAIN["DRAINING<br/><i>Evacuate active workloads</i>"]
        S_DECOM["DECOMMISSIONING<br/><i>Hardware release</i>"]
        S_TERM["DECOMMISSIONED<br/><i>Removed from fleet</i>"]
    end

    S_DISC --> S_PROV --> S_OS --> S_DRV --> S_CUDA --> S_VAL --> S_READY

    S_READY <--> |Workload Scheduled / Released| S_ALLOC

    S_READY --> |Partial failure| S_DEG
    S_ALLOC --> |GPU ECC error| S_DEG
    S_READY --> |Node failure| S_FAIL
    S_ALLOC --> |Kernel panic| S_FAIL

    S_DEG --> |Auto-remediation| S_REP
    S_FAIL --> |Auto-remediation| S_REP
    S_REP --> |Revalidate GPUs| S_VAL

    S_READY --> |Cordon node| S_DRAIN
    S_ALLOC --> |Evacuate workloads| S_DRAIN
    S_DRAIN --> S_DECOM --> S_TERM
```

### State Transition Validation Matrix:

| Current State (`From`) | Legal Next States (`To`) | Trigger / Workflow |
| :--- | :--- | :--- |
| `""` (unregistered) | `DISCOVERED` | Initial node detection |
| `DISCOVERED` | `PROVISIONING`, `FAILED` | `ProvisionNodeWorkflow` Step 1 |
| `PROVISIONING` | `OS_READY`, `FAILED` | PXE OS boot completes |
| `OS_READY` | `DRIVER_INSTALLING`, `FAILED` | Kernel driver install starts |
| `DRIVER_INSTALLING` | `CUDA_READY`, `FAILED` | NVIDIA driver compile & load |
| `CUDA_READY` | `VALIDATING`, `FAILED` | CUDA toolkit verification |
| `VALIDATING` | `READY`, `FAILED` | DCGM diagnostic + NVLink test |
| `READY` | `ALLOCATED`, `DRAINING`, `DEGRADED`, `FAILED` | Workload placement or error |
| `ALLOCATED` | `READY`, `DRAINING`, `DEGRADED`, `FAILED` | Workload release or failure |
| `DRAINING` | `READY`, `DECOMMISSIONING`, `FAILED` | Workloads evicted |
| `DEGRADED` | `REPAIRING`, `DRAINING`, `FAILED` | Health detector trigger |
| `FAILED` | `REPAIRING`, `DECOMMISSIONING` | Automated remediation / removal |
| `REPAIRING` | `VALIDATING`, `FAILED` | `RepairNodeWorkflow` BMC reset |
| `DECOMMISSIONING` | `DECOMMISSIONED`, `FAILED` | Hardware release |
| `DECOMMISSIONED` | *(Terminal)* | Removed from fleet |

---

## 10. Automated Failure Recovery & Self-Healing

```mermaid
sequenceDiagram
    autonumber
    participant Chaos as Chaos Simulation / Fault Injector
    participant Fleet as GPU Fleet Simulator
    participant Kafka as Kafka Bus (health.events)
    participant Detector as Health Monitor & Controller
    participant Sched as GPU Scheduler
    participant Temporal as Temporal Workflow Engine
    participant Reconciler as Cluster Reconciler

    Chaos->>Fleet: FailNode("gpu-node-03", "ECC uncorrectable error")
    Fleet->>Fleet: TransitionNode("gpu-node-03" → FAILED)
    Fleet->>Kafka: Publish(Event: NODE_FAILED, NodeID="gpu-node-03")

    Kafka->>Detector: Consume(NODE_FAILED)
    Detector->>Fleet: TransitionNode("gpu-node-03" → DEGRADED)
    Note over Detector,Fleet: Phase 1: Immediate Cordon & Workload Evacuation
    Detector->>Fleet: Drain workloads from "gpu-node-03"

    Note over Detector,Sched: Phase 2: Capacity Discovery
    Detector->>Sched: Search replacement capacity for 8 GPUs
    Sched-->>Detector: Insufficient capacity on remaining nodes

    Note over Detector,Temporal: Phase 3: Durable Provisioning Workflow
    Detector->>Temporal: Execute(ProvisionNodeWorkflow, NodeID="gpu-node-05")
    activate Temporal
    Temporal->>Fleet: CreateNode("gpu-node-05", Model=A100, Count=8)
    Temporal->>Fleet: Execute Activity: ProvisionOS (Ubuntu 22.04 LTS)
    Temporal->>Fleet: Execute Activity: InstallDriver (NVIDIA 535)
    Temporal->>Fleet: Execute Activity: InstallCUDA (CUDA 12.2)
    Temporal->>Fleet: Execute Activity: ValidateGPU (DCGM Diagnostics)
    Fleet-->>Temporal: Diagnostic Results: PASS
    Temporal->>Fleet: TransitionNode("gpu-node-05" → READY)
    Temporal-->>Detector: Replacement Node "gpu-node-05" is READY
    deactivate Temporal

    Note over Detector,Reconciler: Phase 4: Drift Resolution & Rescheduling
    Detector->>Reconciler: Trigger Reconciliation for affected clusters
    Reconciler->>Sched: Schedule evicted replicas onto "gpu-node-05"
    Sched->>Fleet: AllocateGPUs("gpu-node-05", 8)
    Fleet-->>Reconciler: GPUs Allocated successfully

    Reconciler->>Reconciler: Clear Condition DriftDetected=False
    Reconciler->>Kafka: Publish(Event: DRIFT_RESOLVED)
    Note over Fleet: Fleet Restored to 100% HEALTHY
```

---

## 11. REST API Reference

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/metrics` | Prometheus metrics text format |
| `GET` | `/api/v1/clusters` | List all inference clusters |
| `POST` | `/api/v1/clusters` | Create and reconcile an inference cluster |
| `GET` | `/api/v1/clusters/{name}` | Get cluster status, replica count, and conditions |
| `POST` | `/api/v1/clusters/{name}/scale` | Dynamically scale cluster replicas (`{"replicas": N}`) |
| `DELETE` | `/api/v1/clusters/{name}` | Delete cluster and release allocated GPUs |
| `GET` | `/api/v1/nodes` | List all GPU nodes |
| `GET` | `/api/v1/nodes/{id}` | Get node status, state transitions, and per-GPU telemetry |
| `POST` | `/api/v1/nodes/{id}/fail` | Inject failure on node (triggers self-healing) |
| `POST` | `/api/v1/nodes/{id}/drain` | Cordon and drain node workloads |
| `GET` | `/api/v1/scheduler/capacity` | Capacity report by GPU model and node |
| `GET` | `/api/v1/scheduler/fragmentation` | Fleet and node-level fragmentation scores |
| `POST` | `/api/v1/optimization/plan` | Generate defragmentation migration plan |
| `POST` | `/api/v1/optimization/apply` | Apply defragmentation migrations |
| `GET` | `/api/v1/health` | Health check endpoint |
| `GET` | `/api/v1/health/issues` | Active node and GPU health issues |
| `GET` | `/api/v1/fleet/stats` | Summary statistics of fleet |

---

## 12. CLI Reference

```bash
# Cluster operations
gpuflow cluster create examples/llama.yaml
gpuflow cluster list
gpuflow cluster get llama-cluster
gpuflow cluster scale llama-cluster --replicas 4
gpuflow cluster delete llama-cluster

# Node operations
gpuflow node list
gpuflow node get gpu-node-01
gpuflow node drain gpu-node-01
gpuflow node fail gpu-node-01

# Scheduler and optimization
gpuflow scheduler capacity
gpuflow scheduler fragmentation
gpuflow optimize plan
gpuflow optimize apply

# Chaos failure simulation
gpuflow chaos node-failure gpu-node-03
gpuflow chaos gpu-failure gpu-node-02:gpu-04
gpuflow chaos network-failure gpu-node-01
gpuflow chaos driver-failure gpu-node-04
```

---

## 13. Observability, Metrics & Grafana Dashboard

Prometheus metrics exposed on `/metrics`:
- `gpuflow_gpu_utilization`: Per-node, per-GPU utilization percentage gauge
- `gpuflow_gpu_allocated`: Active allocated GPU count
- `gpuflow_gpu_free`: Free schedulable GPU count
- `gpuflow_node_health`: Health status gauge (1 for active status)
- `gpuflow_node_state`: Lifecycle state gauge
- `gpuflow_scheduler_placement_total`: Counter of placement requests
- `gpuflow_scheduler_placement_failures`: Counter of placement failures
- `gpuflow_fragmentation_score`: Fleet capacity fragmentation score (0.00 to 1.00)
- `gpuflow_reconciliation_total`: Counter of reconciler invocations
- `gpuflow_reconciliation_errors`: Counter of reconciliation failures
- `gpuflow_workflow_duration`: Execution duration gauge for workflows
- `gpuflow_repair_total`: Counter of automated node repairs

Pre-built Grafana Dashboard JSON available in [`deploy/grafana/dashboard.json`](file:///c:/Users/saile/Desktop/GPUFlow/deploy/grafana/dashboard.json).

---

## 14. Testing & Quality Verification

Run the entire non-cached test suite:

```bash
go test -count=1 ./...
```

```text
ok  	github.com/gpuflow/gpuflow/apiserver	0.991s
ok  	github.com/gpuflow/gpuflow/controllers	0.632s
ok  	github.com/gpuflow/gpuflow/events	0.650s
ok  	github.com/gpuflow/gpuflow/health	0.649s
ok  	github.com/gpuflow/gpuflow/pkg/metrics	0.963s
ok  	github.com/gpuflow/gpuflow/providers	0.625s
ok  	github.com/gpuflow/gpuflow/scheduler	0.643s
ok  	github.com/gpuflow/gpuflow/simulator	0.647s
ok  	github.com/gpuflow/gpuflow/workflows	0.661s
```

**Total: 57 tests passing across 9 packages with 0 failures.**

---

## 15. Repository Structure

```
GPUFlow/
├── apiserver/               # REST API server & HTTP handlers
├── cmd/gpuflow/             # CLI application & demo runner
├── controllers/             # Kubernetes reconciliation engine & controllers
├── deploy/
│   ├── crds/                # CustomResourceDefinition YAML manifests
│   ├── grafana/             # Grafana dashboard JSON
│   └── helm/                # Helm deployment charts
├── docs/                    # Architecture & engineering design specs
│   ├── architecture.md      # Detailed system architecture
│   ├── failure-recovery.md  # Self-healing pipeline spec
│   ├── local-development.md # Local development setup guide
│   ├── scheduling.md        # GPU-aware scheduling algorithms
│   └── state-machine.md     # Node lifecycle state machine
├── events/                  # Event streaming (In-Memory + Kafka)
├── examples/                # Cluster specification manifests (YAML/JSON)
├── health/                  # Fleet health monitoring & detector
├── pkg/
│   ├── apis/gpuflow/v1/     # Kubernetes Go API types & Conditions
│   ├── config/              # Configuration & profiles
│   ├── logger/              # Structured JSON logging
│   ├── metrics/             # Prometheus metrics registry
│   └── types/               # Core domain models
├── providers/               # Infrastructure provider abstraction
├── scheduler/               # GPU-aware scheduler & defrag planner
├── simulator/               # GPU fleet hardware & failure simulator
└── workflows/               # Durable Temporal-style workflow orchestration
```
