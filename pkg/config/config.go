package config

type Config struct {
	// Feature flags for enabling/disabling functionality
	EnableVerification bool
	EnableWarnings     bool
	EnableLogging      bool
	
	// Verification job queue size
	JobQueueSize int
	
	// Number of worker goroutines for verification
	WorkerCount int
	
	// Timeout for verification jobs in milliseconds
	VerificationTimeoutMs int
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
	}
}