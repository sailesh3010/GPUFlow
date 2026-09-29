# Node State Machine & Lifecycle Management

## 1. Overview

Every GPU compute node in GPUFlow is governed by a strict, validated finite state machine. Transitions are deterministic, persisted with correlation IDs, and verified to ensure that partially-provisioned or unvalidated nodes are never made schedulable.

---

## 2. Comprehensive State Transition Diagram

```mermaid
stateDiagram-v2
    direction TB

    %% Happy Path: Provisioning
    state "Provisioning Lifecycle (Bare Metal to Production)" as ProvBlock {
        [*] --> DISCOVERED : Node detected on fabric
        DISCOVERED --> PROVISIONING : Initiate PXE boot
        PROVISIONING --> OS_READY : Ubuntu 22.04 LTS installed
        OS_READY --> DRIVER_INSTALLING : NVIDIA 535.129.03 driver
        DRIVER_INSTALLING --> CUDA_READY : CUDA 12.2 toolkit verified
        CUDA_READY --> VALIDATING : Run DCGM & NVLink bandwidth tests
        VALIDATING --> READY : Validation PASS (8/8 GPUs healthy)
    }

    %% Operational States
    state "Active Service" as ActiveBlock {
        READY --> ALLOCATED : Workload scheduled
        ALLOCATED --> READY : All workloads completed/released
    }

    %% Failure & Automated Remediation
    state "Automated Self-Healing Pipeline" as FailureBlock {
        READY --> DEGRADED : Partial GPU failure (1-7 GPUs)
        ALLOCATED --> DEGRADED : GPU dropped off bus / ECC error
        READY --> FAILED : Complete node failure / XID 31
        ALLOCATED --> FAILED : Kernel panic / Network lost
        DEGRADED --> FAILED : Cascade failure
        DEGRADED --> REPAIRING : Trigger RepairNodeWorkflow
        FAILED --> REPAIRING : Automated remediation
        REPAIRING --> VALIDATING : IPMI powercycle & GPU reset
    }

    %% Decommission Lifecycle
    state "Decommissioning" as DecomBlock {
        READY --> DRAINING : Operator cordons node
        ALLOCATED --> DRAINING : Evacuate active workloads
        DRAINING --> DECOMMISSIONING : Hardware release
        DECOMMISSIONING --> DECOMMISSIONED : Terminal state
        DECOMMISSIONED --> [*]
    }
```

---

## 3. Transition Validation Rules & Allowed Transitions

Transitions outside this matrix are rejected with a runtime error (`invalid transition for node ...`):

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

## 4. State Transition Persistence

Every transition records a permanent immutable audit trail:

```json
{
  "timestamp": "2026-09-29T23:30:00.124Z",
  "nodeId": "gpu-node-03",
  "previousState": "ALLOCATED",
  "newState": "FAILED",
  "reason": "XID 31: GPU memory page retirement failure",
  "correlationId": "reconcile-llama-cluster-1727633400"
}
```
