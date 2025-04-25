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
)

// InitVerification sets up the verification engine
func InitVerification(cfg config.Config) {
	log.SetupDefault()
	
	// Initialize metrics collector
	metricsCollector = metrics.NewMetrics()
	
	// Start metrics server if enabled
	if cfg.MetricsServerEnabled {
		if err := metricsCollector.StartMetricsServer(cfg.MetricsServerAddr); err != nil {
			log.Error("failed to start metrics server", "error", err)
		}
	}
	
	// Create and start the replay engine
	if cfg.EnableVerification {
		verificationEngine = replay.NewEngine(cfg)
		// Pass metrics to the engine
		verificationEngine.SetMetrics(metricsCollector)
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

// GetMetrics returns the global metrics collector
func GetMetrics() *metrics.Metrics {
	return metricsCollector
}