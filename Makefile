.PHONY: build test demo serve clean lint vet fmt help

# Default target
help: ## Show this help
	@echo "GPUFlow - Kubernetes-Native GPU Fleet Control Plane"
	@echo ""
	@echo "Usage:"
	@echo "  make <target>"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

# Binary path
BIN := bin/gpuflow
ifeq ($(OS),Windows_NT)
  BIN := bin/gpuflow.exe
endif

# Build
build: ## Build the gpuflow binary
	go build -o $(BIN) ./cmd/gpuflow

# Test
test: ## Run all tests
	go test ./... -count=1

test-v: ## Run all tests with verbose output
	go test ./... -v -count=1

test-race: ## Run tests with race detector
	go test ./... -race -count=1

test-cover: ## Run tests with coverage
	go test ./... -coverprofile=coverage.out -count=1
	go tool cover -html=coverage.out -o coverage.html

# Code quality
lint: ## Run linter
	go vet ./...

vet: ## Run go vet
	go vet ./...

fmt: ## Format code
	gofmt -w .

# Run
serve: build ## Start the control plane server
	$(BIN) serve

demo: build ## Run the full demo
	$(BIN) demo

# Profiles
up-minimal: build ## Start with minimal profile (no Kafka/Temporal)
	GPUFLOW_PROFILE=minimal $(BIN) serve

up-events: build ## Start with event-driven profile (with Kafka)
	GPUFLOW_PROFILE=events $(BIN) serve

up-full: build ## Start with full profile (Kafka + Temporal + Metrics)
	GPUFLOW_PROFILE=full $(BIN) serve

# Docker
docker-build: ## Build Docker image
	docker build -t gpuflow:latest .

docker-run: docker-build ## Run in Docker
	docker run -p 8080:8080 gpuflow:latest

# Clean
clean: ## Remove build artifacts
	rm -rf bin/ coverage.out coverage.html
