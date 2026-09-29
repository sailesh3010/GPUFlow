# Node State Machine & Lifecycle Management

## 1. Overview

Every GPU compute node in GPUFlow is governed by a strict, validated finite state machine. Transitions are deterministic, persisted with correlation IDs, and verified to ensure that partially-provisioned or unvalidated nodes are never made schedulable.

---

## 2. Comprehensive State Transition Diagram

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
