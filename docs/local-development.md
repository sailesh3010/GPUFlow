# Local Development

## Prerequisites

- Go 1.24+ (tested with 1.27)
- Docker Desktop (for Phases 3+ with Kubernetes)
- kubectl (for CRD phases)
- Helm (for deployment phases)

## Quick Start

```bash
# Clone
git clone https://github.com/gpuflow/gpuflow.git
cd gpuflow

# Build
go build -o bin/gpuflow ./cmd/gpuflow

# Test
go test ./... -count=1

# Run demo
go run ./cmd/gpuflow demo

# Start server
go run ./cmd/gpuflow serve
```

## Running Profiles

### Minimal (default, ~100MB RAM)

```bash
# Just GPUFlow and the simulator
make up-minimal
# or
GPUFLOW_PROFILE=minimal go run ./cmd/gpuflow serve
```

### Events (~500MB RAM)

```bash
# Requires Kafka
docker compose up -d kafka
make up-events
```

### Full (~2GB RAM)

```bash
# Requires Kafka + Temporal + Prometheus + Grafana
docker compose up -d
make up-full
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `GPUFLOW_PROFILE` | `minimal` | Run profile |
| `GPUFLOW_SERVER_PORT` | `8080` | API server port |
| `GPUFLOW_KAFKA_ENABLED` | `false` | Enable Kafka |
| `GPUFLOW_KAFKA_BROKERS` | `localhost:9092` | Kafka brokers |
| `GPUFLOW_TEMPORAL_ENABLED` | `false` | Enable Temporal |
| `GPUFLOW_TEMPORAL_HOST` | `localhost:7233` | Temporal host |
| `GPUFLOW_DB_HOST` | `localhost` | PostgreSQL host |
| `GPUFLOW_DB_PORT` | `5432` | PostgreSQL port |
| `GPUFLOW_SIM_NODES` | `4` | Default fleet size |
| `GPUFLOW_SIM_GPUS` | `8` | GPUs per node |
| `GPUFLOW_METRICS_PORT` | `9090` | Prometheus port |

## Testing

```bash
# All tests
go test ./...

# Verbose
go test ./... -v

# Race detector
go test ./... -race

# Coverage
go test ./... -coverprofile=coverage.out
go tool cover -html=coverage.out

# Specific package
go test ./scheduler/... -v
go test ./simulator/... -v
```

## API Testing

```bash
# Start server in one terminal
go run ./cmd/gpuflow serve

# In another terminal:
curl http://localhost:8080/api/v1/fleet/stats | jq
curl http://localhost:8080/api/v1/nodes | jq
curl http://localhost:8080/api/v1/scheduler/capacity | jq
curl http://localhost:8080/api/v1/scheduler/fragmentation | jq

# Create cluster
curl -X POST http://localhost:8080/api/v1/clusters \
  -H "Content-Type: application/json" \
  -d '{"name":"test","replicas":1,"gpu":{"model":"H100","count":4}}' | jq

# Fail a node
curl -X POST http://localhost:8080/api/v1/nodes/gpu-node-03/fail | jq
```

## Resource Constraints

This project is designed for a 16GB RAM Windows laptop:

- Single-node K8s cluster (Docker Desktop)
- Single Kafka broker
- Single Temporal server
- Lightweight PostgreSQL
- All components can be disabled individually
- GPU simulation is pure software — no hardware needed
