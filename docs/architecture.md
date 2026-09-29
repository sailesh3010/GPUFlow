# GPUFlow Architecture & System Design

## 1. System Overview

GPUFlow is a Kubernetes-native control plane for managing heterogeneous GPU inference fleets. It incorporates declarative Custom Resource Definitions (CRDs), level-triggered reconciliation loops, drift detection, GPU-aware scheduling, capacity defragmentation, durable workflow orchestration, event streaming, and Prometheus observability.

---

## 2. End-to-End System Layer Architecture

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

## 3. Declarative Reconciliation & Drift Detection Flow

The Kubernetes controller follows an event-driven reconciliation pattern with rate-limited exponential backoffs. It continuously checks for hardware degradation or state drift.

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

## 4. Durable Temporal-Style Workflow Architecture

GPU provisioning requires long-running bare-metal operations (OS boot, firmware flashing, driver compile, CUDA validation). GPUFlow implements checkpointing and replay so that worker crashes resume seamlessly:

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

## 5. Event-Driven Kafka Streaming Architecture

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

## 6. End-to-End Automated Self-Healing Pipeline

```mermaid
sequenceDiagram
    autonumber
    participant Chaos as Chaos / Failure Injector
    participant Fleet as Fleet Simulator
    participant Kafka as Kafka Bus (Topic: health.events)
    participant Health as Health Controller
    participant Sched as GPU Scheduler
    participant Temporal as Temporal Repair Workflow
    participant Clust as Cluster Reconciler

    Chaos->>Fleet: FailNode("gpu-node-03")
    Fleet->>Fleet: State → FAILED, Health → FAILED
    Fleet->>Kafka: Publish(NODE_FAILED, NodeID="gpu-node-03")

    Kafka->>Health: Consume(NODE_FAILED)
    Health->>Fleet: TransitionNode(DEGRADED)
    Note over Health: Drain workloads off gpu-node-03

    Health->>Sched: Search replacement capacity for evicted workloads
    Sched-->>Health: Capacity unavailable on existing nodes

    Health->>Temporal: Execute(RepairNodeWorkflow / ProvisionReplacement)
    activate Temporal
    Temporal->>Fleet: CreateNode("gpu-node-05")
    Temporal->>Fleet: Run OS, Driver, CUDA provisioning
    Temporal->>Fleet: Run DCGM & NVLink validation
    Fleet-->>Temporal: Validation PASS
    Temporal->>Fleet: TransitionNode("gpu-node-05" → READY)
    Temporal-->>Health: Replacement READY
    deactivate Temporal

    Health->>Clust: Trigger Reconcile(InferenceCluster)
    Clust->>Sched: Schedule evicted replicas onto "gpu-node-05"
    Sched->>Fleet: AllocateGPUs("gpu-node-05", 8)
    Fleet-->>Clust: GPUs Allocated

    Clust->>Clust: Set Condition Ready=True, DriftDetected=False
    Clust->>Kafka: Publish(DRIFT_RESOLVED)
    Note over Fleet: Fleet restored to 100% HEALTHY
```

---

## 7. Package Dependencies & Decoupling

```mermaid
graph TD
    CMD["cmd/gpuflow"] --> APISERVER["apiserver"]
    CMD --> CONTROLLERS["controllers"]
    CMD --> WORKFLOWS["workflows"]
    CMD --> SCHEDULER["scheduler"]
    CMD --> EVENTS["events"]
    CMD --> HEALTH["health"]
    CMD --> SIMULATOR["simulator"]
    CMD --> METRICS["pkg/metrics"]
    CMD --> CONFIG["pkg/config"]
    CMD --> LOGGER["pkg/logger"]

    APISERVER --> CONTROLLERS
    APISERVER --> SCHEDULER
    APISERVER --> HEALTH
    APISERVER --> EVENTS
    APISERVER --> METRICS
    APISERVER --> SIMULATOR

    CONTROLLERS --> SCHEDULER
    CONTROLLERS --> EVENTS
    CONTROLLERS --> SIMULATOR
    CONTROLLERS --> APIS["pkg/apis/gpuflow/v1"]

    WORKFLOWS --> SIMULATOR
    WORKFLOWS --> SCHEDULER
    WORKFLOWS --> TYPES["pkg/types"]

    SCHEDULER --> TYPES
    HEALTH --> SIMULATOR
    HEALTH --> EVENTS
    EVENTS --> TYPES
    PROVIDERS["providers"] --> SIMULATOR
    PROVIDERS --> TYPES
    SIMULATOR --> TYPES
    METRICS --> SIMULATOR
    METRICS --> SCHEDULER
```
