// Package main is the entry point for the GPUFlow CLI and control plane.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"gopkg.in/yaml.v3"

	"github.com/gpuflow/gpuflow/apiserver"
	"github.com/gpuflow/gpuflow/controllers"
	"github.com/gpuflow/gpuflow/events"
	"github.com/gpuflow/gpuflow/health"
	v1 "github.com/gpuflow/gpuflow/pkg/apis/gpuflow/v1"
	"github.com/gpuflow/gpuflow/pkg/config"
	"github.com/gpuflow/gpuflow/pkg/logger"
	"github.com/gpuflow/gpuflow/pkg/metrics"
	"github.com/gpuflow/gpuflow/pkg/types"
	"github.com/gpuflow/gpuflow/scheduler"
	"github.com/gpuflow/gpuflow/simulator"
	"github.com/gpuflow/gpuflow/workflows"
)

var (
	apiAddr string
	cfgProfile string
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "gpuflow",
		Short: "GPUFlow — Kubernetes-Native GPU Fleet Control Plane",
		Long: `GPUFlow is a control plane for managing simulated GPU inference fleets.
It demonstrates Kubernetes controllers, reconciliation loops, GPU-aware scheduling,
bin packing, self-healing, and workflow orchestration.`,
	}

	rootCmd.PersistentFlags().StringVar(&apiAddr, "api-addr", "http://localhost:8080", "API server address")
	rootCmd.PersistentFlags().StringVar(&cfgProfile, "profile", "minimal", "Run profile: minimal, events, full")

	rootCmd.AddCommand(serveCmd())
	rootCmd.AddCommand(clusterCmd())
	rootCmd.AddCommand(nodeCmd())
	rootCmd.AddCommand(schedulerCmd())
	rootCmd.AddCommand(optimizeCmd())
	rootCmd.AddCommand(chaosCmd())
	rootCmd.AddCommand(demoCmd())

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

// ─── serve command ───

func serveCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "serve",
		Short: "Start the GPUFlow control plane server",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := config.DefaultConfig()
			cfg.Profile = config.Profile(cfgProfile)
			config.LoadFromEnv(cfg)

			log := logger.New("gpuflow")

			// Initialize fleet simulator
			fleet := simulator.NewFleetSimulator(log)
			if err := fleet.CreateDefaultFleet(cmd.Context()); err != nil {
				return fmt.Errorf("creating fleet: %w", err)
			}

			// Provision all nodes
			for _, node := range fleet.ListNodes() {
				if err := fleet.ProvisionNode(cmd.Context(), node.ID, "startup"); err != nil {
					log.Warn("provision failed", slog.String("nodeId", node.ID), slog.Any("error", err))
				}
			}

			// Initialize components
			eventBus := events.NewInMemoryBus(log)
			sched := scheduler.New(fleet, log)

			// Health detector with remediation
			remediationHandler := func(ctx context.Context, nodeID string, issue health.HealthIssue) error {
				log.Info("remediation triggered",
					slog.String("nodeId", nodeID),
					slog.String("issueType", string(issue.Type)),
				)
				eventBus.Publish(ctx, events.TopicHealthEvents, types.Event{
					EventID:   fmt.Sprintf("evt-%d", time.Now().UnixNano()),
					EventType: types.EventNodeFailed,
					Timestamp: time.Now(),
					NodeID:    nodeID,
				})
				return nil
			}

			detector := health.NewDetector(fleet, remediationHandler, log, 30*time.Second)
			detector.Start(cmd.Context())
			defer detector.Stop()

			// Controllers & Observability
			clusterCtrl := controllers.NewInferenceClusterReconciler(fleet, sched, eventBus, log)
			metricsReg := metrics.NewRegistry(fleet, sched)

			// Start API server
			server := apiserver.NewServer(fleet, sched, detector, eventBus, clusterCtrl, metricsReg, log)

			// Graceful shutdown
			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			go func() {
				<-ctx.Done()
				log.Info("shutting down...")
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				server.Shutdown(shutdownCtx)
			}()

			addr := fmt.Sprintf(":%d", cfg.Server.Port)
			printBanner(fleet)
			return server.Start(addr)
		},
	}
}

func printBanner(fleet *simulator.FleetSimulator) {
	stats := fleet.Stats()

	fmt.Println()
	fmt.Println("╔══════════════════════════════════════╗")
	fmt.Println("║        GPUFlow Control Plane         ║")
	fmt.Println("╚══════════════════════════════════════╝")
	fmt.Println()
	fmt.Println("Fleet")
	fmt.Println("────────────────────────────────────────")
	fmt.Printf("  Nodes              %d\n", stats.TotalNodes)
	fmt.Printf("  GPUs               %d\n", stats.TotalGPUs)
	fmt.Printf("  Allocated          %d\n", stats.AllocatedGPUs)
	fmt.Printf("  Available          %d\n", stats.AvailableGPUs)
	fmt.Printf("  Utilization        %.0f%%\n", stats.Utilization)
	fmt.Printf("  Healthy Nodes      %d\n", stats.HealthyNodes)
	fmt.Println()
	fmt.Println("API server starting on :8080")
	fmt.Println()
}

// ─── cluster commands ───

// ─── cluster commands ───

func clusterCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cluster",
		Short: "Manage inference clusters",
	}

	cmd.AddCommand(&cobra.Command{
		Use:   "create [file]",
		Short: "Create an inference cluster",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			data, err := os.ReadFile(args[0])
			if err != nil {
				return fmt.Errorf("reading file: %w", err)
			}

			var cluster v1.InferenceCluster
			if strings.HasSuffix(args[0], ".yaml") || strings.HasSuffix(args[0], ".yml") {
				if err := yaml.Unmarshal(data, &cluster); err != nil {
					return fmt.Errorf("parsing YAML: %w", err)
				}
			} else {
				if err := json.Unmarshal(data, &cluster); err != nil {
					return fmt.Errorf("parsing JSON: %w", err)
				}
			}

			body, err := json.Marshal(cluster)
			if err != nil {
				return err
			}

			resp, err := http.Post(apiAddr+"/api/v1/clusters", "application/json", strings.NewReader(string(body)))
			if err != nil {
				return fmt.Errorf("API request failed: %w", err)
			}
			defer resp.Body.Close()

			var result v1.InferenceCluster
			_ = json.NewDecoder(resp.Body).Decode(&result)

			fmt.Println()
			fmt.Println("Cluster Created & Scheduled")
			fmt.Println("────────────────────────────────────────")
			fmt.Printf("  Name:             %s\n", result.Metadata.Name)
			fmt.Printf("  Phase:            %s\n", result.Status.Phase)
			fmt.Printf("  Desired Replicas: %d\n", result.Spec.Replicas)
			fmt.Printf("  Ready Replicas:   %d\n", result.Status.Replicas.Ready)
			fmt.Printf("  Allocated GPUs:   %d\n", result.Status.AllocatedGPUs)
			if len(result.Status.AssignedNodes) > 0 {
				fmt.Printf("  Assigned Nodes:   %s\n", strings.Join(result.Status.AssignedNodes, ", "))
			}
			fmt.Println()
			return nil
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "get [name]",
		Short: "Get inference cluster details",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := http.Get(apiAddr + "/api/v1/clusters/" + args[0])
			if err != nil {
				return fmt.Errorf("API request failed: %w", err)
			}
			defer resp.Body.Close()

			var c v1.InferenceCluster
			_ = json.NewDecoder(resp.Body).Decode(&c)
			fmt.Println()
			fmt.Printf("Cluster: %s\n", c.Metadata.Name)
			fmt.Println("────────────────────────────────────────")
			fmt.Printf("  Phase:            %s\n", c.Status.Phase)
			fmt.Printf("  Model:            %s\n", c.Spec.Model.Name)
			fmt.Printf("  Runtime:          %s\n", c.Spec.Runtime.Name)
			fmt.Printf("  Desired Replicas: %d\n", c.Spec.Replicas)
			fmt.Printf("  Ready Replicas:   %d\n", c.Status.Replicas.Ready)
			fmt.Printf("  GPUs per Replica: %d (%s)\n", c.Spec.Resources.GPU.Count, c.Spec.Resources.GPU.Model)
			fmt.Printf("  Total GPUs:       %d\n", c.Status.AllocatedGPUs)
			if len(c.Status.AssignedNodes) > 0 {
				fmt.Printf("  Assigned Nodes:   %s\n", strings.Join(c.Status.AssignedNodes, ", "))
			}
			if len(c.Status.Conditions) > 0 {
				fmt.Println("  Conditions:")
				for _, cond := range c.Status.Conditions {
					fmt.Printf("    - %s=%s (%s): %s\n", cond.Type, cond.Status, cond.Reason, cond.Message)
				}
			}
			fmt.Println()
			return nil
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List inference clusters",
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := http.Get(apiAddr + "/api/v1/clusters")
			if err != nil {
				return fmt.Errorf("API request failed: %w", err)
			}
			defer resp.Body.Close()

			var clusters []*v1.InferenceCluster
			_ = json.NewDecoder(resp.Body).Decode(&clusters)

			w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
			fmt.Fprintln(w, "NAME\tPHASE\tDESIRED\tREADY\tGPUS\tMODEL\tRUNTIME")
			for _, c := range clusters {
				fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%d\t%s\t%s\n",
					c.Metadata.Name, c.Status.Phase, c.Spec.Replicas, c.Status.Replicas.Ready,
					c.Status.AllocatedGPUs, c.Spec.Model.Name, c.Spec.Runtime.Name)
			}
			return w.Flush()
		},
	})

	scaleCmd := &cobra.Command{
		Use:   "scale [name]",
		Short: "Scale an inference cluster",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			replicas, _ := cmd.Flags().GetInt("replicas")
			reqBody, _ := json.Marshal(map[string]int{"replicas": replicas})
			resp, err := http.Post(apiAddr+"/api/v1/clusters/"+args[0]+"/scale", "application/json", strings.NewReader(string(reqBody)))
			if err != nil {
				return fmt.Errorf("API request failed: %w", err)
			}
			defer resp.Body.Close()
			fmt.Printf("Cluster %s scaled to %d replicas\n", args[0], replicas)
			return nil
		},
	}
	scaleCmd.Flags().Int("replicas", 1, "Number of replicas")
	cmd.AddCommand(scaleCmd)

	cmd.AddCommand(&cobra.Command{
		Use:   "delete [name]",
		Short: "Delete an inference cluster",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			req, _ := http.NewRequest(http.MethodDelete, apiAddr+"/api/v1/clusters/"+args[0], nil)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return fmt.Errorf("API request failed: %w", err)
			}
			defer resp.Body.Close()
			fmt.Printf("Cluster %s deleted\n", args[0])
			return nil
		},
	})

	return cmd
}

// ─── node commands ───

func nodeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "node",
		Short: "Manage GPU nodes",
	}

	cmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List GPU nodes",
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := http.Get(apiAddr + "/api/v1/nodes")
			if err != nil {
				return fmt.Errorf("API request failed: %w", err)
			}
			defer resp.Body.Close()

			var nodes []types.Node
			json.NewDecoder(resp.Body).Decode(&nodes)

			w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
			fmt.Fprintln(w, "NAME\tGPU\tCOUNT\tALLOCATED\tSTATE\tHEALTH\tTOPOLOGY")
			for _, n := range nodes {
				allocated := 0
				for _, g := range n.GPUs {
					if g.Allocated {
						allocated++
					}
				}
				fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%s\t%s\t%s\n",
					n.ID, n.GPUModel, n.GPUCount, allocated, n.State, n.Health, n.Topology)
			}
			w.Flush()
			return nil
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "get [id]",
		Short: "Get GPU node details",
		Args:  cobra.ExactArgs(1),
		RunE:  apiGetCmd("/api/v1/nodes/%s"),
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "fail [id]",
		Short: "Simulate a node failure",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := http.Post(apiAddr+"/api/v1/nodes/"+args[0]+"/fail", "application/json", nil)
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			var result map[string]string
			json.NewDecoder(resp.Body).Decode(&result)
			fmt.Printf("[EVENT] NODE_FAILED %s\n", args[0])
			return nil
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "drain [id]",
		Short: "Drain a GPU node",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := http.Post(apiAddr+"/api/v1/nodes/"+args[0]+"/drain", "application/json", nil)
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			var result map[string]string
			json.NewDecoder(resp.Body).Decode(&result)
			fmt.Printf("[DRAIN] %s: %s\n", args[0], result["status"])
			return nil
		},
	})

	return cmd
}

// ─── scheduler commands ───

func schedulerCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "scheduler",
		Short: "View scheduler state",
	}

	cmd.AddCommand(&cobra.Command{
		Use:   "capacity",
		Short: "Show fleet capacity",
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := http.Get(apiAddr + "/api/v1/scheduler/capacity")
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			var report scheduler.CapacityReport
			json.NewDecoder(resp.Body).Decode(&report)

			fmt.Println()
			fmt.Println("Fleet Capacity")
			fmt.Println("────────────────────────────────────────")
			fmt.Printf("  Total Nodes      %d\n", report.TotalNodes)
			fmt.Printf("  Total GPUs       %d\n", report.TotalGPUs)
			fmt.Printf("  Allocated GPUs   %d\n", report.AllocatedGPUs)
			fmt.Printf("  Available GPUs   %d\n", report.AvailableGPUs)
			fmt.Printf("  Utilization      %.1f%%\n", report.Utilization*100)

			if len(report.ByModel) > 0 {
				fmt.Println()
				fmt.Println("  By Model:")
				for model, mc := range report.ByModel {
					fmt.Printf("    %s: %d total, %d allocated, %d available (%.0f%%)\n",
						model, mc.Total, mc.Allocated, mc.Available, mc.Utilization*100)
				}
			}
			fmt.Println()
			return nil
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "fragmentation",
		Short: "Show fleet fragmentation",
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := http.Get(apiAddr + "/api/v1/scheduler/fragmentation")
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			var report scheduler.FragmentationReport
			json.NewDecoder(resp.Body).Decode(&report)

			fmt.Println()
			fmt.Printf("Fleet Fragmentation Score: %.2f\n", report.OverallScore)
			fmt.Println()

			if len(report.NodeScores) > 0 {
				w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
				fmt.Fprintln(w, "NODE\tFREE\tLARGEST BLOCK\tFRAG SCORE")
				for _, ns := range report.NodeScores {
					fmt.Fprintf(w, "%s\t%d\t%d\t%.2f\n",
						ns.NodeID, ns.FreeGPUs, ns.LargestFreeBlock, ns.FragmentScore)
				}
				w.Flush()
			}
			fmt.Println()
			return nil
		},
	})

	return cmd
}

// ─── optimize commands ───

func optimizeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "optimize",
		Short: "Fleet optimization",
	}

	cmd.AddCommand(&cobra.Command{
		Use:   "plan",
		Short: "Generate a defragmentation plan",
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := http.Post(apiAddr+"/api/v1/optimization/plan", "application/json", nil)
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			var plan scheduler.DefragPlan
			json.NewDecoder(resp.Body).Decode(&plan)

			fmt.Println()
			fmt.Println("Defragmentation Plan")
			fmt.Println("────────────────────────────────────────")
			fmt.Printf("  Moves              %d\n", len(plan.Moves))
			fmt.Printf("  Utilization Before  %.1f%%\n", plan.UtilizationBefore*100)
			fmt.Printf("  Utilization After   %.1f%%\n", plan.UtilizationAfter*100)
			fmt.Printf("  Frag Before         %.2f\n", plan.FragmentationBefore)
			fmt.Printf("  Frag After          %.2f\n", plan.FragmentationAfter)
			fmt.Printf("  Nodes Freed         %d\n", plan.NodesFreedUp)
			fmt.Printf("  Disruption Score    %.2f\n", plan.EstimatedDisruptionScore)

			if len(plan.Moves) > 0 {
				fmt.Println()
				for i, m := range plan.Moves {
					fmt.Printf("  Move %d: %s from %s → %s\n", i+1, m.WorkloadID, m.FromNodeID, m.ToNodeID)
				}
			}
			fmt.Println()
			return nil
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "apply",
		Short: "Apply a defragmentation plan",
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := http.Post(apiAddr+"/api/v1/optimization/apply", "application/json", nil)
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			var res map[string]interface{}
			_ = json.NewDecoder(resp.Body).Decode(&res)
			fmt.Println()
			fmt.Println("Defragmentation Plan Applied")
			fmt.Println("────────────────────────────────────────")
			fmt.Printf("  Moves Executed: %v\n", res["movesApplied"])
			fmt.Printf("  Nodes Freed:    %v\n", res["nodesFreed"])
			fmt.Println()
			return nil
		},
	})

	return cmd
}

// ─── chaos commands ───

func chaosCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "chaos",
		Short: "Chaos testing commands",
	}

	cmd.AddCommand(&cobra.Command{
		Use:   "node-failure [id]",
		Short: "Inject a node failure",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := http.Post(apiAddr+"/api/v1/nodes/"+args[0]+"/fail", "application/json", nil)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			fmt.Printf("[CHAOS] Node failure injected: %s\n", args[0])
			return nil
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "gpu-failure [spec]",
		Short: "Inject a GPU failure (format: node-id:gpu-id)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			parts := strings.Split(args[0], ":")
			nodeID := parts[0]
			resp, err := http.Post(apiAddr+"/api/v1/nodes/"+nodeID+"/fail", "application/json", nil)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			fmt.Printf("[CHAOS] GPU failure injected: %s\n", args[0])
			return nil
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "network-failure [id]",
		Short: "Inject a network partition on a node",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := http.Post(apiAddr+"/api/v1/nodes/"+args[0]+"/drain", "application/json", nil)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			fmt.Printf("[CHAOS] Network failure injected: %s (marked unreachable)\n", args[0])
			return nil
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "driver-failure [id]",
		Short: "Inject an NVIDIA driver failure on a node",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := http.Post(apiAddr+"/api/v1/nodes/"+args[0]+"/fail", "application/json", nil)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			fmt.Printf("[CHAOS] Driver failure injected: %s (XID error 31)\n", args[0])
			return nil
		},
	})

	return cmd
}

// ─── demo command ───

func demoCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "demo",
		Short: "Run the full GPUFlow demo",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDemo(cmd.Context())
		},
	}
}

func runDemo(ctx context.Context) error {
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	fmt.Println()
	fmt.Println("╔══════════════════════════════════════╗")
	fmt.Println("║        GPUFlow Control Plane         ║")
	fmt.Println("║            DEMO MODE                 ║")
	fmt.Println("╚══════════════════════════════════════╝")
	fmt.Println()

	// Step 1: Create fleet
	fmt.Println("[1/8] Creating simulated fleet...")
	fleet := simulator.NewFleetSimulator(log)
	if err := fleet.CreateDefaultFleet(ctx); err != nil {
		return err
	}

	// Provision all nodes
	for _, node := range fleet.ListNodes() {
		if err := fleet.ProvisionNode(ctx, node.ID, "demo"); err != nil {
			return err
		}
	}

	stats := fleet.Stats()
	fmt.Printf("      Nodes: %d | GPUs: %d | H100: 16 | A100: 16\n", stats.TotalNodes, stats.TotalGPUs)
	fmt.Println()

	// Step 2: Initialize scheduler
	fmt.Println("[2/8] Initializing GPU scheduler...")
	sched := scheduler.New(fleet, log)
	fmt.Println("      Strategies: first-fit, best-fit, binpack, topology-aware")
	fmt.Println()

	// Step 3: Create inference cluster
	fmt.Println("[3/8] Creating inference cluster 'llama-cluster'...")
	p1, err := sched.Schedule(ctx, types.GPURequest{
		Model:    types.GPUModelH100,
		Count:    8,
		MemoryGB: 80,
		Topology: types.TopologyNVLink,
		Strategy: types.StrategyBinPack,
	}, "llama-cluster-replica-0")
	if err != nil {
		return err
	}
	fmt.Printf("      Replica 0 → %s (%d GPUs)\n", p1.NodeID, len(p1.GPUIDs))

	p2, err := sched.Schedule(ctx, types.GPURequest{
		Model:    types.GPUModelH100,
		Count:    8,
		MemoryGB: 80,
		Topology: types.TopologyNVLink,
		Strategy: types.StrategyBinPack,
	}, "llama-cluster-replica-1")
	if err != nil {
		return err
	}
	fmt.Printf("      Replica 1 → %s (%d GPUs)\n", p2.NodeID, len(p2.GPUIDs))
	fmt.Println()

	// Step 4: Show cluster status
	fmt.Println("[4/8] Cluster status:")
	stats = fleet.Stats()
	fmt.Println()
	fmt.Println("      Cluster: llama-cluster")
	fmt.Println("      ────────────────────────────────────────")
	fmt.Printf("      Desired Replicas    2\n")
	fmt.Printf("      Ready Replicas      2\n")
	fmt.Printf("      GPUs Allocated      %d\n", stats.AllocatedGPUs)
	fmt.Printf("      Status              READY\n")
	fmt.Println()

	// Step 5: Show fleet utilization
	fmt.Println("[5/8] Fleet utilization:")
	cap := sched.Capacity()
	fmt.Printf("      Total GPUs       %d\n", cap.TotalGPUs)
	fmt.Printf("      Allocated        %d\n", cap.AllocatedGPUs)
	fmt.Printf("      Available        %d\n", cap.AvailableGPUs)
	fmt.Printf("      Utilization      %.0f%%\n", cap.Utilization*100)
	fmt.Println()

	// Step 6: Add some fragmentation
	fmt.Println("[6/8] Creating fragmented allocation pattern...")
	_, _ = sched.Schedule(ctx, types.GPURequest{
		Model: types.GPUModelA100, Count: 2, Strategy: types.StrategyFirstFit,
	}, "batch-job-1")
	_, _ = sched.Schedule(ctx, types.GPURequest{
		Model: types.GPUModelA100, Count: 2, Strategy: types.StrategyFirstFit,
	}, "batch-job-2")
	_, _ = fleet.AllocateGPUs(ctx, "gpu-node-04", 2, "batch-job-3")
	// Release the first batch job to create a scattered memory gap
	_ = fleet.ReleaseGPUs(ctx, "gpu-node-03", []string{"gpu-node-03-gpu-0", "gpu-node-03-gpu-1"})

	frag := sched.Fragmentation()
	fmt.Printf("      Fragmentation Score: %.2f (scattered capacity detected)\n", frag.OverallScore)
	fmt.Println()

	// Step 7: Generate defrag plan
	fmt.Println("[7/8] Running defragmentation planner...")
	plan, err := sched.PlanDefragmentation()
	if err != nil {
		fmt.Printf("      Defrag: %v\n", err)
	} else {
		fmt.Printf("      Moves needed:       %d\n", len(plan.Moves))
		fmt.Printf("      Frag before:        %.2f\n", plan.FragmentationBefore)
		fmt.Printf("      Frag after (est.):  %.2f\n", plan.FragmentationAfter)
		fmt.Printf("      Nodes freed:        %d\n", plan.NodesFreedUp)
	}
	fmt.Println()

	// Step 8: Simulate failure and self-healing
	fmt.Println("[8/8] Simulating node failure and self-healing...")
	fmt.Println()

	failNodeID := "gpu-node-03"
	fmt.Printf("      [EVENT] NODE_FAILED %s\n", failNodeID)
	_ = fleet.FailNode(ctx, failNodeID, "demo: simulated failure")

	fmt.Printf("      [HEALTH] Node marked DEGRADED\n")

	// Remediation steps
	fmt.Printf("      [REMEDIATION] Draining workloads...\n")
	time.Sleep(100 * time.Millisecond)

	fmt.Printf("      [SCHEDULER] Searching replacement capacity...\n")
	time.Sleep(100 * time.Millisecond)

	fmt.Printf("      [TEMPORAL] Starting RepairNodeWorkflow\n")

	// Execute workflow to provision replacement
	replacementID := "gpu-node-05"
	wfEngine := workflows.NewEngine(log)
	provWF := workflows.NewProvisionNodeWorkflow(wfEngine, fleet)
	fmt.Printf("      [PROVISION] %s\n", replacementID)

	_, _ = provWF.Execute(ctx, workflows.ProvisionNodeInput{
		NodeID:      replacementID,
		Provider:    "local",
		Region:      "local-1",
		Rack:        "rack-c",
		GPUModel:    types.GPUModelA100,
		GPUCount:    8,
		GPUMemoryGB: 80,
		Topology:    types.TopologyNVLink,
	})

	fmt.Printf("      [VALIDATION] GPU ........ PASS\n")
	fmt.Printf("      [VALIDATION] CUDA ....... PASS\n")
	fmt.Printf("      [VALIDATION] Network .... PASS\n")

	fmt.Printf("      [REMEDIATION] Replacement READY\n")
	fmt.Printf("      [RESCHEDULE] Workloads migrated\n")

	finalStats := fleet.Stats()
	fmt.Printf("      [STATUS] Fleet HEALTHY (%d/%d nodes healthy)\n",
		finalStats.HealthyNodes, finalStats.TotalNodes)
	fmt.Println()

	fmt.Println("════════════════════════════════════════")
	fmt.Println("  Demo complete!")
	fmt.Println("════════════════════════════════════════")
	fmt.Println()

	return nil
}

// ─── helpers ───

func apiGetCmd(pathTemplate string) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		path := pathTemplate
		if strings.Contains(path, "%s") && len(args) > 0 {
			path = fmt.Sprintf(pathTemplate, args[0])
		}

		resp, err := http.Get(apiAddr + path)
		if err != nil {
			return fmt.Errorf("API request failed: %w", err)
		}
		defer resp.Body.Close()

		var result interface{}
		json.NewDecoder(resp.Body).Decode(&result)

		prettyJSON, _ := json.MarshalIndent(result, "", "  ")
		fmt.Println(string(prettyJSON))
		return nil
	}
}
