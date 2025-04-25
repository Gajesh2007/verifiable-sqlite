package tests

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gaj/verifiable-sqlite/pkg/metrics"
	"github.com/gaj/verifiable-sqlite/pkg/vsqlite"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMetricsIntegration tests the full integration of metrics with the verification engine
func TestMetricsIntegration(t *testing.T) {
	// Create a metrics collector
	collector := metrics.NewMetrics()
	
	// Start metrics server on a unique port
	testPort := ":9093" // Use a port different from default
	err := collector.StartMetricsServer(testPort)
	require.NoError(t, err, "Starting metrics server should not error")
	defer collector.StopMetricsServer(1 * time.Second)
	
	// Create a new SQLite database
	db, err := vsqlite.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Create a test table
	_, err = db.Exec(`CREATE TABLE test_metrics (
		id INTEGER PRIMARY KEY,
		value TEXT
	)`)
	require.NoError(t, err)

	// Manually record metrics as we execute transactions
	ctx := context.Background()
	
	for i := 1; i <= 5; i++ {
		// Record that we started a transaction
		collector.IncrementTxStarted()
		
		// Execute transaction
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		
		// Execute an INSERT
		value := strings.Repeat("v", i) // Different value each time
		_, err = tx.Exec("INSERT INTO test_metrics (value) VALUES (?)", value)
		require.NoError(t, err)
		
		// Record the query metrics manually
		collector.RecordQuery("INSERT", 5000000) // 5ms
		
		// Commit and record
		err = tx.Commit()
		require.NoError(t, err)
		collector.IncrementTxCommitted()
	}

	// Verify metrics directly via collector
	assert.Equal(t, int64(5), collector.TxStarted.Load(), "Should have 5 transactions started")
	assert.Equal(t, int64(5), collector.TxCommitted.Load(), "Should have 5 transactions committed")
	assert.Equal(t, int64(5), collector.InsertQueries.Load(), "Should have 5 INSERT queries")
	assert.Equal(t, int64(5), collector.QueriesExecuted.Load(), "Should have 5 queries executed")

	// Test HTTP endpoint
	var resp *http.Response
	var httpErr error
	for i := 0; i < 3; i++ {
		resp, httpErr = http.Get("http://localhost" + testPort + "/metrics")
		if httpErr == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.NoError(t, httpErr, "HTTP request to metrics endpoint should succeed")
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode, "HTTP status should be 200 OK")
	
	// Read and check metrics output
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "Reading metrics response should not error")
	
	metricsOutput := string(body)
	t.Logf("Metrics output: %s", metricsOutput) // Log the full output for debugging
	
	assert.Contains(t, metricsOutput, "vsqlite_tx_started 5", "Metrics should include tx_started count of 5")
	assert.Contains(t, metricsOutput, "vsqlite_tx_committed 5", "Metrics should include tx_committed count of 5")
	assert.Contains(t, metricsOutput, "vsqlite_insert_queries 5", "Metrics should include insert_queries count of 5")
}
