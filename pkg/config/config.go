package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/viper"
)

// Config contains the configuration for Verifiable SQLite
type Config struct {
	// Feature flags for enabling/disabling functionality
	EnableVerification bool `mapstructure:"enable_verification"`
	EnableWarnings     bool `mapstructure:"enable_warnings"`
	EnableLogging      bool `mapstructure:"enable_logging"`
	
	// Verification job queue size
	JobQueueSize int `mapstructure:"job_queue_size"`
	
	// Number of worker goroutines for verification
	WorkerCount int `mapstructure:"worker_count"`
	
	// Timeout for verification jobs in milliseconds
	VerificationTimeoutMs int `mapstructure:"verification_timeout_ms"`
	
	// Metrics configuration
	MetricsServerEnabled bool   `mapstructure:"metrics_server_enabled"`
	MetricsServerAddr    string `mapstructure:"metrics_server_addr"`
}

// DefaultConfig returns the default configuration
func DefaultConfig() Config {
	return Config{
		EnableVerification:   true,
		EnableWarnings:       true,
		EnableLogging:        true,
		JobQueueSize:         100,
		WorkerCount:          4,
		VerificationTimeoutMs: 5000, // 5 seconds
		MetricsServerEnabled:  false,
		MetricsServerAddr:     ":9090",
	}
}

// Validate ensures the configuration values are within valid ranges
func (c *Config) Validate() error {
	if c.JobQueueSize <= 0 {
		return fmt.Errorf("job_queue_size must be positive, got %d", c.JobQueueSize)
	}
	
	if c.WorkerCount <= 0 {
		return fmt.Errorf("worker_count must be positive, got %d", c.WorkerCount)
	}
	
	if c.VerificationTimeoutMs <= 0 {
		return fmt.Errorf("verification_timeout_ms must be positive, got %d", c.VerificationTimeoutMs)
	}
	
	if c.MetricsServerEnabled && c.MetricsServerAddr == "" {
		return fmt.Errorf("metrics_server_addr cannot be empty when metrics_server_enabled is true")
	}
	
	return nil
}

// Load loads configuration from a configuration file, environment variables, and flags
// Configuration precedence (highest to lowest):
// 1. Command line flags (not implemented in this function)
// 2. Environment variables: VSQLITE_ENABLE_VERIFICATION, etc.
// 3. Configuration file (YAML, JSON, or TOML)
// 4. Default values
func Load() (Config, error) {
	// Start with the default configuration
	config := DefaultConfig()

	// Set up Viper
	v := viper.New()
	v.SetConfigName("config")         // Name of config file (without extension)
	v.SetConfigType("yaml")           // YAML config files
	v.AddConfigPath(".")              // Look for config in the working directory
	v.AddConfigPath("$HOME/.vsqlite") // Look in .vsqlite in home directory
	v.AddConfigPath("/etc/vsqlite/")  // Look in /etc/vsqlite/

	// Set default values (same as DefaultConfig)
	v.SetDefault("enable_verification", config.EnableVerification)
	v.SetDefault("enable_warnings", config.EnableWarnings)
	v.SetDefault("enable_logging", config.EnableLogging)
	v.SetDefault("job_queue_size", config.JobQueueSize)
	v.SetDefault("worker_count", config.WorkerCount)
	v.SetDefault("verification_timeout_ms", config.VerificationTimeoutMs)
	v.SetDefault("metrics_server_enabled", config.MetricsServerEnabled)
	v.SetDefault("metrics_server_addr", config.MetricsServerAddr)

	// Environment variables
	v.SetEnvPrefix("VSQLITE")                  // Prefix for environment variables
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_")) // Replace dots with underscores in env vars
	v.AutomaticEnv()                           // Read in environment variables

	// Read the configuration file
	if err := v.ReadInConfig(); err != nil {
		// It's okay if config file doesn't exist, but other errors are reported
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return config, fmt.Errorf("error reading config file: %v", err)
		}
	}

	// Unmarshal the config into our Config struct
	if err := v.Unmarshal(&config); err != nil {
		return config, fmt.Errorf("unable to decode config: %v", err)
	}
	
	// Validate the configuration
	if err := config.Validate(); err != nil {
		return config, fmt.Errorf("invalid configuration: %v", err)
	}

	return config, nil
}

// CreateExampleConfig writes an example configuration file
func CreateExampleConfig(path string) error {
	// Create example config content
	configContent := `# Verifiable SQLite Configuration

# Feature flags
enable_verification: true  # Whether to enable verification
enable_warnings: true      # Whether to warn about non-deterministic functions
enable_logging: true       # Whether to enable logging

# Job queue settings
job_queue_size: 100        # Maximum number of verification jobs to queue
worker_count: 4            # Number of worker goroutines for verification
verification_timeout_ms: 5000  # Timeout for verification jobs in milliseconds

# Metrics settings
metrics_server_enabled: false  # Whether to enable the metrics server
metrics_server_addr: ":9090"   # Address for the metrics server (empty for no server)

# V1 Limitations:
# - SELECT-based state capture (not WAL)
# - No query rewriting
# - Internal re-execution only
# - Verification skipped for tables without PKs
`

	// Write the example config to the specified path
	if err := os.WriteFile(path, []byte(configContent), 0644); err != nil {
		return fmt.Errorf("failed to write example config file: %v", err)
	}

	return nil
}