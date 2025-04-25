package vsqlite

import (
	"github.com/gaj/verifiable-sqlite/pkg/config"
	"github.com/gaj/verifiable-sqlite/pkg/log"
	"github.com/gaj/verifiable-sqlite/pkg/metrics"
	"github.com/gaj/verifiable-sqlite/pkg/replay"
	"time"
)

// Global replay engine for verification jobs
var (
	verificationEngine *replay.Engine
	metricsCollector   *metrics.Metrics
	metricsSingleton   *metrics.Metrics
)

// Initialize the verification engine and other components
func init() {
	// Initialize the metrics collector
	metricsCollector = metrics.NewMetrics()
	metricsSingleton = metricsCollector
	
	// Load default configuration
	defaultCfg, err := config.Load()
	if err != nil {
		log.Error("failed to load configuration", "error", err)
		// Use default config if loading fails
		defaultCfg = config.DefaultConfig()
	}
	
	// Start metrics server if enabled
	if defaultCfg.MetricsServerEnabled {
		if err := metricsCollector.StartMetricsServer(defaultCfg.MetricsServerAddr); err != nil {
			log.Error("failed to start metrics server", "error", err)
		}
	}
	
	// By default, don't start the verification engine here.
	// Tests and applications should call InitVerification explicitly
}

// InitVerification sets up the verification engine with the provided configuration
// This is used by tests and applications to initialize the verification engine
func InitVerification(cfg config.Config) {
	log.SetupDefault()
	
	// Reuse the metrics collector initialized in the init() function
	// or create a new one if it hasn't been initialized yet
	if metricsCollector == nil {
		metricsCollector = metrics.NewMetrics()
		metricsSingleton = metricsCollector
	}
	
	// Start metrics server if enabled
	if cfg.MetricsServerEnabled {
		// Try to start the metrics server - StartMetricsServer will handle
		// the case where a server is already running
		if err := metricsCollector.StartMetricsServer(cfg.MetricsServerAddr); err != nil {
			log.Error("failed to start metrics server", "error", err)
		}
	}
	
	// Create and start the replay engine
	if cfg.EnableVerification {
		engineOpts := []replay.EngineOption{
			replay.WithMetrics(metricsCollector),
			replay.WithTimeout(time.Duration(cfg.VerificationTimeoutMs) * time.Millisecond),
		}
		verificationEngine = replay.NewEngine(cfg.JobQueueSize, cfg.WorkerCount, engineOpts...)
		verificationEngine.Start()
	}
}

// Shutdown safely stops all verification processes
func Shutdown() {
	if verificationEngine != nil {
		verificationEngine.Stop()
	}
	
	// Stop metrics server with a 5-second timeout
	if metricsCollector != nil {
		metricsCollector.StopMetricsServer(5 * time.Second)
	}
}

// GetVerificationEngine returns the global verification engine
func GetVerificationEngine() *replay.Engine {
	return verificationEngine
}

// GetMetrics returns the metrics collector for testing and inspection
func GetMetrics() *metrics.Metrics {
	return metricsSingleton
}

// SetMetrics sets the metrics collector - useful for testing
func SetMetrics(m *metrics.Metrics) {
	metricsSingleton = m
}