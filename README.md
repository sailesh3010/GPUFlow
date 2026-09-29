# GPUFlow — Kubernetes-Native GPU Fleet Control Plane

GPUFlow is a production-style control plane for managing a simulated GPU inference fleet. It demonstrates the engineering concepts required for infrastructure/control-plane roles: Kubernetes controllers, Custom Resource Definitions (CRDs), reconciliation loops, declarative APIs, GPU-aware scheduling, bin packing, capacity defragmentation, automated self-healing, durable workflow orchestration, event-driven streaming, and Prometheus observability.

> **Design Principle:** Desired state is declared by the user; GPUFlow continuously reconciles actual state toward desired state using event-driven watch mechanisms, drift detection, and deterministic self-healing pipelines.

---

## Architecture

```
                    ┌────────────────────────────────────────┐
                    │               GPUFlow CLI              │
                    └───────────────────┬────────────────────┘
                                        │
                                        ▼
                    ┌────────────────────────────────────────┐
                    │         GPUFlow REST API / CRDs        │
                    │   /api/v1/clusters   /api/v1/nodes     │
                    │   /api/v1/scheduler  /metrics          │
                    └───────────────────┬────────────────────┘
                                        │
           ┌────────────────────────────┴───────────────────────────┐
           │                                                        │
           ▼                                                        ▼
┌──────────────────────────────┐                         ┌──────────────────────┐
│    Kubernetes Controllers    │                         │  Durable Workflows   │
│  InferenceClusterReconciler  │                         │ ProvisionNode (8 act)│
│      GPUNodeReconciler       │                         │      RepairNode      │
│       Drift Detection        │                         │     ScaleCluster     │
└──────────────┬───────────────┘                         │   DecommissionNode   │
               │                                         └──────────┬───────────┘
               ▼                                                    │
┌──────────────────────────────┐                                    │
│        GPU Scheduler         │                                    │
│   BinPack / Topology-Aware   │                                    │
│   Fragmentation Analysis     │                                    │
│   Defrag Migration Planner   │                                    │
└──────────────┬───────────────┘                                    │
               │                                                    │
               ▼                                                    ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                           GPU Fleet Simulator                                 │
│                                                                               │
│   gpu-node-01 (H100 x8)    gpu-node-02 (H100 x8)     gpu-node-03 (A100 x8)   │
│   gpu-node-04 (A100 x8)    gpu-node-05 (Spare A100)                          │
└───────────────────────────────────────────────────────────────────────────────┘
```

---

## Quick Start

```bash
# Build binary
make build

# Run all 57 tests across 8 packages
make test

# Run deterministic demo
make demo

# Start API server and Prometheus metrics exporter on :8080
make serve
```

---

## Deterministic Demo Walkthrough

Run:

```bash
gpuflow demo
```

Expected Output:

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

## Custom Resource Definitions (CRDs)

Installed via `kubectl apply -f deploy/crds/` or Helm chart:

### 1. `InferenceCluster` (`deploy/crds/gpuflow.io_inferenceclusters.yaml`)

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
  conditions:
    - type: Ready
      status: "True"
```

### 2. `GPUNode` (`deploy/crds/gpuflow.io_gpunodes.yaml`)

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
```

---

## Reconciliation Engine & Drift Detection

The `InferenceClusterReconciler` continuously enforces desired state:
1. **Capacity Scheduling**: When replicas scale up, invokes GPU scheduler with specified strategy (`binpack`, `topology-aware`, etc.).
2. **Drift Detection**: Detects when healthy allocated GPUs diverge from desired requirements (e.g. underlying node failure). Emits `DRIFT_DETECTED`, sets condition `DriftDetected=True`, and provisions replacement capacity.
3. **Drift Resolution**: Once healthy capacity is restored, clears condition and emits `DRIFT_RESOLVED`.
4. **Idempotency**: All operations keyed by deterministic correlation IDs.

---

## Durable Workflows (Temporal Pattern)

Located in `workflows/`, supporting activity retry with exponential backoff, timeouts, and checkpoint replay across worker restarts:

- **`ProvisionNodeWorkflow`**: 8 sequential activities:
  `DiscoverNode` → `ProvisionOS` → `InstallDriver` → `InstallCUDA` → `ConfigureNetwork` → `ValidateGPU` → `ValidateNetwork` → `RegisterNode`
- **`RepairNodeWorkflow`**: `DrainNodeWorkloads` → `PowerCycleHardware` → `ValidateRepairedGPU` → `ReRegisterRepairedNode`
- **`ScaleClusterWorkflow`**: `CheckClusterCapacity` → `ScaleClusterReplicas`
- **`DecommissionNodeWorkflow`**: `CordonNode` → `DrainWorkloads` → `DeprovisionHardware`

---

## Event-Driven Streaming (Kafka)

Kafka event bus adapter in `events/kafka.go` streaming to dedicated topics:
- `gpuflow.node.events`
- `gpuflow.cluster.events`
- `gpuflow.health.events`
- `gpuflow.scheduler.events`
- `gpuflow.audit.events`

Partition keys are bound to `NodeID` or `ClusterID` for total order per resource.

---

## GPU Scheduling & Defragmentation

### Scheduling Strategies:
- `first-fit`: First node satisfying requirements
- `best-fit`: Node with fewest available GPUs (tightest fit)
- `binpack`: Minimizes resource waste, packs workloads on fuller nodes
- `topology-aware`: Bin packing with NVLink interconnect preference

### Fragmentation Metric:
```text
fragmentationScore = 1.0 - (largestContiguousFreeBlock / totalFreeGPUs)
```
The defragmentation planner calculates minimal migration moves to consolidate scattered workloads and free up entire nodes for multi-GPU training/inference.

---

## REST API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/metrics` | Prometheus metrics text format |
| `GET` | `/api/v1/clusters` | List all inference clusters |
| `POST` | `/api/v1/clusters` | Create and reconcile an inference cluster |
| `GET` | `/api/v1/clusters/{name}` | Get cluster details and conditions |
| `POST` | `/api/v1/clusters/{name}/scale` | Scale cluster replicas |
| `DELETE` | `/api/v1/clusters/{name}` | Delete cluster and release GPUs |
| `GET` | `/api/v1/nodes` | List all GPU nodes |
| `GET` | `/api/v1/nodes/{id}` | Get node status and GPU telemetry |
| `POST` | `/api/v1/nodes/{id}/fail` | Inject failure on node |
| `POST` | `/api/v1/nodes/{id}/drain` | Cordon and drain node workloads |
| `GET` | `/api/v1/scheduler/capacity` | Capacity report by model and node |
| `GET` | `/api/v1/scheduler/fragmentation` | Fleet and node fragmentation scores |
| `POST` | `/api/v1/optimization/plan` | Generate defragmentation migration plan |
| `POST` | `/api/v1/optimization/apply` | Apply defragmentation migrations |
| `GET` | `/api/v1/health` | Health check endpoint |
| `GET` | `/api/v1/health/issues` | Active node and GPU health issues |
| `GET` | `/api/v1/fleet/stats` | Summary statistics of fleet |

---

## CLI Reference

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

## Observability & Grafana Dashboard

Prometheus metrics exposed on `/metrics`:
- `gpuflow_gpu_utilization`
- `gpuflow_gpu_allocated`
- `gpuflow_gpu_free`
- `gpuflow_node_health`
- `gpuflow_node_state`
- `gpuflow_scheduler_placement_total`
- `gpuflow_scheduler_placement_failures`
- `gpuflow_fragmentation_score`
- `gpuflow_reconciliation_total`
- `gpuflow_reconciliation_errors`
- `gpuflow_workflow_duration`
- `gpuflow_repair_total`

A complete pre-built dashboard JSON is provided at `deploy/grafana/dashboard.json`.

---

## Testing & Quality

```bash
go test -count=1 ./...
```

Test suite coverage:
- **`controllers`**: Reconciler lifecycle, multi-replica scheduling, drift detection, scale-down
- **`workflows`**: 8-step Bare-metal Provisioning, Idempotency keys, Flaky activity backoff retries, Worker crash & checkpoint resume, Automated Repair and Decommissioning
- **`events`**: Pub/sub, Multi-subscribers, Kafka partition key serialization and routing
- **`scheduler`**: First-Fit, Best-Fit, BinPack, Topology-Aware, Fragmentation scoring, Defragmentation migration planning, Contiguity algorithms
- **`health`**: Issue lifecycle, GPU failures, automated remediation triggers
- **`providers`**: Mock provider provisioning, idempotency, health, power cycles
- **`simulator`**: Node lifecycle state machine transitions, concurrent allocations, GPU failure injection and recovery
- **`pkg/metrics`**: Prometheus text exposition validation
- **`apiserver`**: Full REST API routes, cluster CRUD, scale, drain, fail, optimization plan and apply

---

## Repository Structure

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
