package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/viper"
)

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

// Load loads configuration from a configuration file, environment variables, and flags
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

	return config, nil
}

// CreateExampleConfig writes an example configuration file
func CreateExampleConfig(path string) error {
	// Create example config content
	configContent := `# Verifiable SQLite Configuration

# Feature flags
enable_verification: true
enable_warnings: true
enable_logging: true

# Job queue settings
job_queue_size: 100
worker_count: 4
verification_timeout_ms: 5000

# Metrics settings
metrics_server_enabled: false
metrics_server_addr: ":9090"
`

	// Write to file
	err := os.WriteFile(path, []byte(configContent), 0644)
	if err != nil {
		return fmt.Errorf("error writing example config: %v", err)
	}

	return nil
}