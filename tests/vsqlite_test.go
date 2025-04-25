package tests

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/gaj/verifiable-sqlite/pkg/config"
	"github.com/gaj/verifiable-sqlite/pkg/log"
	"github.com/gaj/verifiable-sqlite/pkg/vsqlite"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// captureLogOutput captures log output during test execution
func captureLogOutput(t *testing.T) *bytes.Buffer {
	var logBuffer bytes.Buffer
	log.SetupWithWriter(&logBuffer) // We'll ignore the error as it should not fail with a buffer
	return &logBuffer
}

// assertVerificationSuccess checks if verification succeeded in logs
func assertVerificationSuccess(t *testing.T, logs string) {
	assert.Contains(t, logs, "verification succeeded", 
		"Logs should contain verification success message")
}

// assertVerificationFailure checks if verification failed in logs
func assertVerificationFailure(t *testing.T, logs string) {
	assert.Contains(t, logs, "verification failed", 
		"Logs should contain verification failure message")
}

func TestBasicDMLOperations(t *testing.T) {
	// Capture log output
	logBuffer := captureLogOutput(t)
	
	// Initialize the verification engine
	cfg := config.DefaultConfig()
	vsqlite.InitVerification(cfg)
	defer vsqlite.Shutdown()

	// Create a new in-memory database
	db, err := vsqlite.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Create a test table
	_, err = db.Exec(`CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		email TEXT UNIQUE,
		created_at DATETIME
	)`)
	require.NoError(t, err)

	// Test INSERT
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	now := time.Now().Format(time.RFC3339)
	result, err := tx.Exec("INSERT INTO users (name, email, created_at) VALUES (?, ?, ?)",
		"Test User", "test@example.com", now)
	require.NoError(t, err)

	lastID, err := result.LastInsertId()
	require.NoError(t, err)
	assert.Equal(t, int64(1), lastID)

	err = tx.Commit()
	require.NoError(t, err)

	// Test SELECT
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Test UPDATE within a transaction
	tx, err = db.BeginTx(ctx, nil)
	require.NoError(t, err)

	result, err = tx.Exec("UPDATE users SET name = ? WHERE id = ?", "Updated User", lastID)
	require.NoError(t, err)

	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(1), rowsAffected)

	err = tx.Commit()
	require.NoError(t, err)

	// Verify the update
	var name string
	err = db.QueryRow("SELECT name FROM users WHERE id = ?", lastID).Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "Updated User", name)

	// Test DELETE within a transaction
	tx, err = db.BeginTx(ctx, nil)
	require.NoError(t, err)

	result, err = tx.Exec("DELETE FROM users WHERE id = ?", lastID)
	require.NoError(t, err)

	rowsAffected, err = result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(1), rowsAffected)

	err = tx.Commit()
	require.NoError(t, err)

	// Verify the delete
	err = db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Allow time for async verification to complete
	time.Sleep(300 * time.Millisecond)
	
	// Verify that all verification operations succeeded
	logs := logBuffer.String()
	assertVerificationSuccess(t, logs)
	assert.NotContains(t, logs, "verification failed", 
		"No verification failures should be present in the logs")
}

func TestNonDeterministicWarning(t *testing.T) {
	// Capture log output
	logBuffer := captureLogOutput(t)
	
	// Initialize the verification engine
	cfg := config.DefaultConfig()
	vsqlite.InitVerification(cfg)
	defer vsqlite.Shutdown()

	// Create a new in-memory database
	db, err := vsqlite.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Create a test table
	_, err = db.Exec(`CREATE TABLE records (
		id INTEGER PRIMARY KEY,
		value TEXT,
		timestamp DATETIME
	)`)
	require.NoError(t, err)

	// Execute a query with non-deterministic function (CURRENT_TIMESTAMP)
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	_, err = tx.Exec("INSERT INTO records (value, timestamp) VALUES (?, CURRENT_TIMESTAMP)",
		"Non-deterministic test")
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	// Allow time for async verification to process
	time.Sleep(300 * time.Millisecond)

	// Check logs for non-deterministic warning
	logs := logBuffer.String()
	assert.Contains(t, logs, "non-deterministic",
		"Logs should contain warning about non-deterministic query")
}

// TestNoVerificationForTableWithoutPK tests the skip verification behavior for tables without PKs
func TestNoVerificationForTableWithoutPK(t *testing.T) {
	// Capture log output
	logBuffer := captureLogOutput(t)
	
	// Initialize the verification engine
	cfg := config.DefaultConfig()
	vsqlite.InitVerification(cfg)
	defer vsqlite.Shutdown()

	// Create a new in-memory database
	db, err := vsqlite.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Create a test table WITHOUT primary key
	_, err = db.Exec(`CREATE TABLE logs (
		message TEXT,
		level TEXT,
		created_at DATETIME
	)`)
	require.NoError(t, err)

	// Insert into table without PK
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	_, err = tx.Exec("INSERT INTO logs (message, level, created_at) VALUES (?, ?, ?)",
		"Test message", "INFO", time.Now().Format(time.RFC3339))
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	// Allow time for async verification to process
	time.Sleep(300 * time.Millisecond)

	// Check logs for skipping verification
	logs := logBuffer.String()
	assert.Contains(t, logs, "skipping verification",
		"Logs should indicate skipping verification for table without primary key")
}