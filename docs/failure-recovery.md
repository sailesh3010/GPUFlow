# Failure Recovery & Automated Remediation

## 1. Overview

In large GPU clusters, hardware degradation is a daily occurrence: NVLink errors, thermal throttling, uncorrectable double-bit ECC errors, and PCIe bus resets. GPUFlow handles failures autonomously through level-triggered health detection, event streaming, and durable self-healing workflows without human intervention.

---

## 2. Failure Severity & Auto-Recovery Matrix

| Failure Type | Source / Metric | Severity | Automated Remediation Flow |
| :--- | :--- | :--- | :--- |
| `GPU_FAILURE` | DCGM fatal ECC error | High | Degrade node → Drain affected workload → BMC reset GPU → Revalidate |
| `DRIVER_FAILURE` | NVIDIA XID 31 / 79 | Critical | Mark FAILED → Drain node → Reload kernel module / Power cycle |
| `TEMPERATURE_HIGH` | GPU Temp > 88°C | Warning | Throttle utilization → Monitor → Degrade if sustained |
| `NETWORK_FAILURE` | RoCE / InfiniBand drop | Critical | Cordon node → Evacuate replicas → Reschedule to spare |
| `NODE_UNREACHABLE` | Heartbeat timeout (>30s) | Critical | Evict workloads → Provision replacement node via Temporal |

---

## 3. End-to-End Self-Healing Flow Diagram

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

## 4. Idempotency & Retry Guarantees

Every step in the remediation pipeline is protected against duplicate execution:

```mermaid
flowchart LR
    REQ["Remediation Request<br/>(NodeID, CorrelationID)"] --> IDEMP_CHECK{"Idempotency Key<br/>Exists in History?"}

    IDEMP_CHECK --> |Yes & COMPLETED| RETURN_CACHED["Return Cached Result<br/>(Skip Execution)"]
    IDEMP_CHECK --> |Yes & RUNNING| ATTACH["Attach to Existing<br/>Workflow Execution"]
    IDEMP_CHECK --> |No| RUN_ATTEMPT["Execute Activity with Timeout & Retries"]

    RUN_ATTEMPT --> CHECK_ERR{"Attempt<br/>Succeeded?"}
    CHECK_ERR --> |Yes| SAVE_CHECKPOINT["Persist Activity Checkpoint"]
    CHECK_ERR --> |No & Retries < Max| BACKOFF["Wait Exponential Backoff<br/>(Initial: 10ms, Factor: 2.0)"]
    BACKOFF --> RUN_ATTEMPT
    CHECK_ERR --> |No & Retries Exceeded| ROLLBACK["Mark FAILED & Rollback<br/>Release Allocated Resources"]
```
