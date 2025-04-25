package tests

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/gaj/verifiable-sqlite/pkg/config"
	"github.com/gaj/verifiable-sqlite/pkg/vsqlite"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBasicDMLOperations(t *testing.T) {
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
	time.Sleep(100 * time.Millisecond)
}

func TestNonDeterministicWarning(t *testing.T) {
	// Initialize the verification engine
	cfg := config.DefaultConfig()
	vsqlite.InitVerification(cfg)
	defer vsqlite.Shutdown()

	// Create a new in-memory database
	db, err := vsqlite.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Create a test table
	_, err = db.Exec(`CREATE TABLE events (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		timestamp DATETIME
	)`)
	require.NoError(t, err)

	// Execute a query with a non-deterministic function
	// This should log a warning but still execute
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	// Use current_timestamp which is non-deterministic
	_, err = tx.Exec("INSERT INTO events (name, timestamp) VALUES (?, current_timestamp)",
		"Test Event")
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	// Verify the insert
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM events").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Allow time for async verification to complete
	time.Sleep(100 * time.Millisecond)
}