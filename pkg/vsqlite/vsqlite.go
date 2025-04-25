package vsqlite

import (
	"github.com/gaj/verifiable-sqlite/pkg/config"
	"github.com/gaj/verifiable-sqlite/pkg/log"
	"github.com/gaj/verifiable-sqlite/pkg/replay"
)

// Global replay engine for verification jobs
var (
	verificationEngine *replay.Engine
)

// InitVerification sets up the verification engine
func InitVerification(cfg config.Config) {
	log.SetupDefault()
	
	// Create and start the replay engine
	if cfg.EnableVerification {
		verificationEngine = replay.NewEngine(cfg)
		verificationEngine.Start()
	}
}

// Shutdown safely stops all verification processes
func Shutdown() {
	if verificationEngine != nil {
		verificationEngine.Stop()
	}
}

// GetVerificationEngine returns the global verification engine
func GetVerificationEngine() *replay.Engine {
	return verificationEngine
}