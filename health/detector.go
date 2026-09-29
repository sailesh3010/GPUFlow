// Package health implements GPU fleet health monitoring and automated remediation.
// It periodically checks node health and triggers self-healing workflows
// when failures are detected.
package health

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/gpuflow/gpuflow/pkg/types"
)

// NodeHealthProvider retrieves node health information.
type NodeHealthProvider interface {
	ListNodes() []*types.Node
	GetNode(id string) (*types.Node, error)
	TransitionNode(ctx context.Context, nodeID string, newState types.NodeState, reason, correlationID string) error
}

// RemediationHandler is called when a health issue is detected and remediation should begin.
type RemediationHandler func(ctx context.Context, nodeID string, issue HealthIssue) error

// HealthIssue represents a detected health problem.
type HealthIssue struct {
	NodeID    string         `json:"nodeId"`
	Type      HealthIssueType `json:"type"`
	Severity  Severity       `json:"severity"`
	Message   string         `json:"message"`
	DetectedAt time.Time     `json:"detectedAt"`
}

// HealthIssueType categorizes health problems.
type HealthIssueType string

const (
	IssueGPUFailure     HealthIssueType = "GPU_FAILURE"
	IssueDriverFailure  HealthIssueType = "DRIVER_FAILURE"
	IssueNetworkFailure HealthIssueType = "NETWORK_FAILURE"
	IssueTempHigh       HealthIssueType = "TEMPERATURE_HIGH"
	IssueNodeFailed     HealthIssueType = "NODE_FAILED"
	IssueNodeDegraded   HealthIssueType = "NODE_DEGRADED"
)

// Severity of a health issue.
type Severity string

const (
	SeverityWarning  Severity = "WARNING"
	SeverityCritical Severity = "CRITICAL"
)

// Detector monitors fleet health and generates issues.
type Detector struct {
	provider     NodeHealthProvider
	handler      RemediationHandler
	log          *slog.Logger
	interval     time.Duration
	mu           sync.Mutex
	activeIssues map[string]HealthIssue
	cancel       context.CancelFunc
}

// NewDetector creates a health detector.
func NewDetector(provider NodeHealthProvider, handler RemediationHandler, log *slog.Logger, interval time.Duration) *Detector {
	return &Detector{
		provider:     provider,
		handler:      handler,
		log:          log,
		interval:     interval,
		activeIssues: make(map[string]HealthIssue),
	}
}

// Start begins periodic health monitoring. Call Stop() to terminate.
func (d *Detector) Start(ctx context.Context) {
	ctx, d.cancel = context.WithCancel(ctx)

	go func() {
		ticker := time.NewTicker(d.interval)
		defer ticker.Stop()

		// Run once immediately
		d.check(ctx)

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				d.check(ctx)
			}
		}
	}()

	d.log.Info("health detector started", slog.Duration("interval", d.interval))
}

// Stop terminates health monitoring.
func (d *Detector) Stop() {
	if d.cancel != nil {
		d.cancel()
	}
}

// CheckNow runs a single health check synchronously.
func (d *Detector) CheckNow(ctx context.Context) []HealthIssue {
	return d.check(ctx)
}

// ActiveIssues returns all currently active health issues.
func (d *Detector) ActiveIssues() []HealthIssue {
	d.mu.Lock()
	defer d.mu.Unlock()

	issues := make([]HealthIssue, 0, len(d.activeIssues))
	for _, issue := range d.activeIssues {
		issues = append(issues, issue)
	}
	return issues
}

// check runs a single health check pass across all nodes.
func (d *Detector) check(ctx context.Context) []HealthIssue {
	nodes := d.provider.ListNodes()
	var newIssues []HealthIssue

	for _, node := range nodes {
		issues := d.checkNode(node)
		for _, issue := range issues {
			d.mu.Lock()
			if _, exists := d.activeIssues[issue.NodeID+string(issue.Type)]; !exists {
				d.activeIssues[issue.NodeID+string(issue.Type)] = issue
				newIssues = append(newIssues, issue)

				d.log.Warn("health issue detected",
					slog.String("nodeId", issue.NodeID),
					slog.String("type", string(issue.Type)),
					slog.String("severity", string(issue.Severity)),
					slog.String("message", issue.Message),
				)
			}
			d.mu.Unlock()

			if d.handler != nil {
				if err := d.handler(ctx, issue.NodeID, issue); err != nil {
					d.log.Error("remediation failed",
						slog.String("nodeId", issue.NodeID),
						slog.Any("error", err),
					)
				}
			}
		}
	}

	// Clear resolved issues
	d.mu.Lock()
	for key, issue := range d.activeIssues {
		node, err := d.provider.GetNode(issue.NodeID)
		if err != nil {
			delete(d.activeIssues, key) // node gone
			continue
		}
		if node.Health == types.NodeHealthHealthy &&
			(node.State == types.NodeStateReady || node.State == types.NodeStateAllocated) {
			delete(d.activeIssues, key)
		}
	}
	d.mu.Unlock()

	return newIssues
}

// checkNode examines a single node for health issues.
func (d *Detector) checkNode(node *types.Node) []HealthIssue {
	var issues []HealthIssue

	// Check node-level failure
	if node.State == types.NodeStateFailed {
		issues = append(issues, HealthIssue{
			NodeID:     node.ID,
			Type:       IssueNodeFailed,
			Severity:   SeverityCritical,
			Message:    "node is in FAILED state",
			DetectedAt: time.Now(),
		})
		return issues
	}

	if node.State == types.NodeStateDegraded {
		issues = append(issues, HealthIssue{
			NodeID:     node.ID,
			Type:       IssueNodeDegraded,
			Severity:   SeverityWarning,
			Message:    "node is in DEGRADED state",
			DetectedAt: time.Now(),
		})
	}

	// Check individual GPU health
	failedGPUs := 0
	for _, gpu := range node.GPUs {
		if gpu.Health == types.GPUHealthFailed {
			failedGPUs++
		}
		if gpu.Temperature > 90 {
			issues = append(issues, HealthIssue{
				NodeID:     node.ID,
				Type:       IssueTempHigh,
				Severity:   SeverityWarning,
				Message:    "GPU temperature exceeds 90°C: " + gpu.ID,
				DetectedAt: time.Now(),
			})
		}
	}

	if failedGPUs > 0 {
		severity := SeverityWarning
		if failedGPUs > len(node.GPUs)/2 {
			severity = SeverityCritical
		}
		issues = append(issues, HealthIssue{
			NodeID:     node.ID,
			Type:       IssueGPUFailure,
			Severity:   severity,
			Message:    fmt.Sprintf("%d GPU(s) failed", failedGPUs),
			DetectedAt: time.Now(),
		})
	}

	return issues
}

// Needed for the fmt.Sprintf in checkNode
func init() {}
