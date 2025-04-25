package main

import (
	"context"
	"os"
	"testing"

	"github.com/gaj/verifiable-sqlite/pkg/config"
	"github.com/gaj/verifiable-sqlite/pkg/vsqlite"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSimpleOperations(t *testing.T) {
	// Remove test database if it exists
	os.Remove("./test.db")

	// Initialize VSQLite
	cfg := config.DefaultConfig()
	vsqlite.InitVerification(cfg)
	defer vsqlite.Shutdown()

	// Open database
	db, err := vsqlite.Open("sqlite3", "./test.db")
	require.NoError(t, err)
	defer db.Close()

	// Create a test table
	_, err = db.Exec(`CREATE TABLE test (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL
	)`)
	require.NoError(t, err)

	// Start a transaction
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	// Insert data
	_, err = tx.Exec("INSERT INTO test (name) VALUES (?)", "Test Item")
	require.NoError(t, err)

	// Commit the transaction
	err = tx.Commit()
	require.NoError(t, err)

	// Query the data
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Clean up
	os.Remove("./test.db")
}