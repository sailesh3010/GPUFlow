# GPUFlow Architecture

## Overview

GPUFlow is a control plane that manages GPU inference fleet resources using a declarative, reconciliation-based model. Users declare desired state; GPUFlow continuously drives actual state toward desired state.

## Core Design Principles

1. **Declarative Over Imperative**: Users specify *what* they want, not *how* to get there
2. **Reconciliation**: Controllers continuously compare desired vs actual state
3. **Idempotency**: All operations are safe to retry
4. **Event-Driven**: Components react to events, not polling
5. **Provider Abstraction**: Control plane is decoupled from infrastructure specifics

## Component Architecture

```mermaid
graph TD
    CLI["CLI (Cobra)"] --> API["REST API"]
    API --> SCHED["GPU Scheduler"]
    API --> FLEET["Fleet Simulator"]
    API --> HEALTH["Health Detector"]

    SCHED --> FLEET
    HEALTH --> FLEET
    HEALTH --> BUS["Event Bus"]

    BUS --> |NODE_FAILED| HEALTH
    BUS --> |CLUSTER_CREATED| AUDIT["Audit Log"]

    FLEET --> NODE1["gpu-node-01\nH100 x8"]
    FLEET --> NODE2["gpu-node-02\nH100 x8"]
    FLEET --> NODE3["gpu-node-03\nA100 x8"]
    FLEET --> NODE4["gpu-node-04\nA100 x8"]
```

## Data Flow: Workload Placement

```mermaid
sequenceDiagram
    participant User
    participant API
    participant Scheduler
    participant Fleet

    User->>API: POST /clusters {H100 x8, binpack}
    API->>Scheduler: Schedule(GPURequest)
    Scheduler->>Fleet: ListNodes()
    Fleet-->>Scheduler: [node-01, node-02, ...]
    Scheduler->>Scheduler: Score candidates (binpack)
    Scheduler->>Fleet: AllocateGPUs(node-01, 8)
    Fleet-->>Scheduler: [gpu-01..08]
    Scheduler-->>API: PlacementResult
    API-->>User: 201 Created
```

## Data Flow: Self-Healing

```mermaid
sequenceDiagram
    participant Failure as Failure Injector
    participant Fleet
    participant Health as Health Detector
    participant Bus as Event Bus
    participant Scheduler
    participant Provider

    Failure->>Fleet: FailNode(gpu-node-03)
    Fleet->>Fleet: State → FAILED
    Health->>Fleet: CheckNow()
    Fleet-->>Health: node-03 FAILED
    Health->>Bus: Publish(NODE_FAILED)
    Health->>Health: Remediation handler
    Note over Health: Drain workloads
    Health->>Provider: Provision(replacement)
    Provider->>Fleet: CreateNode → ProvisionNode
    Fleet-->>Provider: gpu-node-05 READY
    Health->>Scheduler: Reschedule workloads
```

## Package Dependencies

```mermaid
graph LR
    CMD["cmd/gpuflow"] --> API["apiserver"]
    CMD --> SCHED["scheduler"]
    CMD --> SIM["simulator"]
    CMD --> HEALTH["health"]
    CMD --> EVT["events"]
    CMD --> CFG["pkg/config"]
    CMD --> LOG["pkg/logger"]

    API --> SCHED
    API --> SIM
    API --> HEALTH
    API --> EVT
    API --> TYPES["pkg/types"]

    SCHED --> TYPES
    SIM --> TYPES
    HEALTH --> TYPES
    EVT --> TYPES

    PROV["providers"] --> SIM
    PROV --> TYPES
```

## Thread Safety

All simulator operations use `sync.RWMutex`:
- **Writes** (create, transition, allocate, fail): exclusive lock
- **Reads** (get, list, stats): shared lock
- **GetNode/ListNodes**: return deep copies to prevent races

## Configuration Profiles

| Profile | Components | Use Case |
|---------|------------|----------|
| `minimal` | GPUFlow + Simulator + PostgreSQL | Day-to-day development |
| `events` | + Kafka | Testing event-driven flows |
| `full` | + Temporal + Prometheus + Grafana | Full demo/integration tests |
