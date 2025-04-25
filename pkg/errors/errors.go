// Package errors defines specific error types for Verifiable SQLite
package errors

import (
	"fmt"
	"errors"
)

// Standard errors
var (
	// ErrVerificationFailed indicates that a verification job failed
	ErrVerificationFailed = errors.New("verification failed")
	
	// ErrStateCapture indicates a failure during state capture
	ErrStateCapture = errors.New("state capture failed")
	
	// ErrAnalysisFailed indicates the SQL analysis failed
	ErrAnalysisFailed = errors.New("SQL analysis failed")
	
	// ErrNoPrimaryKey indicates a table has no primary key
	ErrNoPrimaryKey = errors.New("table has no primary key")
	
	// ErrJobQueueFull indicates the verification job queue is full
	ErrJobQueueFull = errors.New("verification job queue is full")
	
	// ErrEngineNotRunning indicates the verification engine is not running
	ErrEngineNotRunning = errors.New("verification engine is not running")
	
	// ErrReplayFailed indicates the query replay failed during verification
	ErrReplayFailed = errors.New("query replay failed during verification")
	
	// ErrStateMismatch indicates a mismatch between claimed and actual state
	ErrStateMismatch = errors.New("state mismatch between claimed and actual")
	
	// StateMismatch is an alias for ErrStateMismatch
	StateMismatch = ErrStateMismatch
	
	// ErrNonDeterministicQuery indicates a query has non-deterministic elements
	ErrNonDeterministicQuery = errors.New("query contains non-deterministic elements")
)

// New creates a new error with the given text
func New(text string) error {
	return errors.New(text)
}

// Newf creates a new formatted error
func Newf(format string, args ...interface{}) error {
	return fmt.Errorf(format, args...)
}

// Wrap wraps an error with a message
func Wrap(err error, message string) error {
	return fmt.Errorf("%s: %w", message, err)
}

// Wrapf wraps an error with a formatted message
func Wrapf(err error, format string, args ...interface{}) error {
	return fmt.Errorf("%s: %w", fmt.Sprintf(format, args...), err)
}

// WrapError wraps an error with a specific context message
func WrapError(err error, message string) error {
	return fmt.Errorf("%s: %w", message, err)
}

// WrapVerificationError wraps an error as a verification failure
func WrapVerificationError(err error, txID string) error {
	return fmt.Errorf("verification failed for transaction %s: %w", txID, err)
}

// WrapStateCaptureError wraps an error as a state capture failure
func WrapStateCaptureError(err error, table string) error {
	return fmt.Errorf("state capture failed for table %s: %w", table, err)
}

// Is returns true if the target error is contained within err
// This is a convenience wrapper around errors.Is
func Is(err, target error) bool {
	return errors.Is(err, target)
}

// As finds the first error in err's chain that matches target
// This is a convenience wrapper around errors.As
func As(err error, target interface{}) bool {
	return errors.As(err, target)
}
