// Package config provides application configuration with environment-based overrides.
package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds the full application configuration.
type Config struct {
	// Server configuration
	Server ServerConfig

	// Simulator configuration
	Simulator SimulatorConfig

	// Kafka configuration (optional, event-driven profile)
	Kafka KafkaConfig

	// Temporal configuration (optional, full profile)
	Temporal TemporalConfig

	// Database configuration
	Database DatabaseConfig

	// Metrics configuration
	Metrics MetricsConfig

	// Profile controls which components are enabled
	Profile Profile
}

// Profile determines which infrastructure components are active.
type Profile string

const (
	ProfileMinimal Profile = "minimal"
	ProfileEvents  Profile = "events"
	ProfileFull    Profile = "full"
)

// ServerConfig holds API server configuration.
type ServerConfig struct {
	Port         int
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// SimulatorConfig holds GPU simulator configuration.
type SimulatorConfig struct {
	DefaultNodeCount   int
	DefaultGPUPerNode  int
	FailureRate        float64
	HealthCheckInterval time.Duration
}

// KafkaConfig holds Kafka connection configuration.
type KafkaConfig struct {
	Enabled  bool
	Brokers  []string
	GroupID  string
}

// TemporalConfig holds Temporal connection configuration.
type TemporalConfig struct {
	Enabled   bool
	HostPort  string
	Namespace string
	TaskQueue string
}

// DatabaseConfig holds PostgreSQL configuration.
type DatabaseConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
	SSLMode  string
}

// MetricsConfig holds Prometheus configuration.
type MetricsConfig struct {
	Enabled bool
	Port    int
}

// DefaultConfig returns a configuration suitable for local development.
func DefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Port:         8080,
			ReadTimeout:  15 * time.Second,
			WriteTimeout: 15 * time.Second,
		},
		Simulator: SimulatorConfig{
			DefaultNodeCount:   4,
			DefaultGPUPerNode:  8,
			FailureRate:        0.05,
			HealthCheckInterval: 30 * time.Second,
		},
		Kafka: KafkaConfig{
			Enabled: false,
			Brokers: []string{"localhost:9092"},
			GroupID: "gpuflow",
		},
		Temporal: TemporalConfig{
			Enabled:   false,
			HostPort:  "localhost:7233",
			Namespace: "gpuflow",
			TaskQueue: "gpuflow-tasks",
		},
		Database: DatabaseConfig{
			Host:     "localhost",
			Port:     5432,
			User:     "gpuflow",
			Password: "gpuflow",
			DBName:   "gpuflow",
			SSLMode:  "disable",
		},
		Metrics: MetricsConfig{
			Enabled: true,
			Port:    9090,
		},
		Profile: ProfileMinimal,
	}
}

// LoadFromEnv loads configuration values from environment variables,
// overriding the provided defaults. This follows 12-factor app conventions.
func LoadFromEnv(cfg *Config) {
	if v := os.Getenv("GPUFLOW_PROFILE"); v != "" {
		cfg.Profile = Profile(v)
	}

	// Enable components based on profile
	switch cfg.Profile {
	case ProfileFull:
		cfg.Kafka.Enabled = true
		cfg.Temporal.Enabled = true
		cfg.Metrics.Enabled = true
	case ProfileEvents:
		cfg.Kafka.Enabled = true
		cfg.Metrics.Enabled = true
	case ProfileMinimal:
		// defaults are fine
	}

	// Server overrides
	if v := os.Getenv("GPUFLOW_SERVER_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			cfg.Server.Port = port
		}
	}

	// Kafka overrides
	if v := os.Getenv("GPUFLOW_KAFKA_ENABLED"); v != "" {
		cfg.Kafka.Enabled = v == "true"
	}
	if v := os.Getenv("GPUFLOW_KAFKA_BROKERS"); v != "" {
		cfg.Kafka.Brokers = []string{v}
	}

	// Temporal overrides
	if v := os.Getenv("GPUFLOW_TEMPORAL_ENABLED"); v != "" {
		cfg.Temporal.Enabled = v == "true"
	}
	if v := os.Getenv("GPUFLOW_TEMPORAL_HOST"); v != "" {
		cfg.Temporal.HostPort = v
	}

	// Database overrides
	if v := os.Getenv("GPUFLOW_DB_HOST"); v != "" {
		cfg.Database.Host = v
	}
	if v := os.Getenv("GPUFLOW_DB_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			cfg.Database.Port = port
		}
	}
	if v := os.Getenv("GPUFLOW_DB_USER"); v != "" {
		cfg.Database.User = v
	}
	if v := os.Getenv("GPUFLOW_DB_PASSWORD"); v != "" {
		cfg.Database.Password = v
	}
	if v := os.Getenv("GPUFLOW_DB_NAME"); v != "" {
		cfg.Database.DBName = v
	}

	// Simulator overrides
	if v := os.Getenv("GPUFLOW_SIM_NODES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Simulator.DefaultNodeCount = n
		}
	}
	if v := os.Getenv("GPUFLOW_SIM_GPUS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Simulator.DefaultGPUPerNode = n
		}
	}

	// Metrics overrides
	if v := os.Getenv("GPUFLOW_METRICS_ENABLED"); v != "" {
		cfg.Metrics.Enabled = v == "true"
	}
	if v := os.Getenv("GPUFLOW_METRICS_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			cfg.Metrics.Port = port
		}
	}
}

// KafkaEnabled returns whether Kafka should be used.
func (c *Config) KafkaEnabled() bool {
	return c.Kafka.Enabled
}

// TemporalEnabled returns whether Temporal should be used.
func (c *Config) TemporalEnabled() bool {
	return c.Temporal.Enabled
}
