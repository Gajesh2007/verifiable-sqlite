package log

import (
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/gaj/verifiable-sqlite/pkg/types"
)

var (
	logger     *slog.Logger
	loggerOnce sync.Once
)

// SetupDefault sets up the default logger to stdout
func SetupDefault() {
	loggerOnce.Do(func() {
		logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))
	})
}

// SetupWithWriter sets up the logger with a custom writer
func SetupWithWriter(w io.Writer) {
	logger = slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
}

// Debug logs a debug message
func Debug(msg string, args ...any) {
	initLogger()
	logger.Debug(msg, args...)
}

// Info logs an info message
func Info(msg string, args ...any) {
	initLogger()
	logger.Info(msg, args...)
}

// Warn logs a warning message
func Warn(msg string, args ...any) {
	initLogger()
	logger.Warn(msg, args...)
}

// Error logs an error message
func Error(msg string, args ...any) {
	initLogger()
	logger.Error(msg, args...)
}

// LogStateCommitment logs a state commitment record
func LogStateCommitment(record types.StateCommitmentRecord) {
	initLogger()
	logger.Info("state_commitment",
		"tx_id", record.TxID,
		"query", record.Query,
		"sql", record.SQL,
		"timestamp", record.Timestamp.Format(time.RFC3339Nano),
		"pre_root_captured", record.PreRootCaptured,
		"post_root_claimed", record.PostRootClaimed,
		"performance_ms", record.PerformanceMs,
	)
}

// LogVerificationResult logs a verification result
func LogVerificationResult(result types.VerificationResult) {
	initLogger()
	logger.Info("verification_result",
		"tx_id", result.TxID,
		"query", result.Query,
		"success", result.Success,
		"timestamp", result.Timestamp.Format(time.RFC3339Nano),
		"pre_root_captured", result.PreRootCaptured,
		"post_root_claimed", result.PostRootClaimed,
		"post_root_calculated", result.PostRootCalculated,
		"error", result.Error,
		"mismatches", result.Mismatches,
	)
}

// LogNonDeterministicWarning logs a warning about non-deterministic functions
func LogNonDeterministicWarning(sql string, functions []string) {
	initLogger()
	logger.Warn("non_deterministic_function_detected",
		"sql", sql,
		"functions", functions,
	)
}

// initLogger initializes the logger if it hasn't been initialized yet
func initLogger() {
	loggerOnce.Do(func() {
		logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))
	})
}