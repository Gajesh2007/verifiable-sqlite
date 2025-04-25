package metrics

import (
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/gaj/verifiable-sqlite/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetricsCollection(t *testing.T) {
	// Create a new metrics collector
	metrics := NewMetrics()

	// Test transaction metrics
	metrics.IncrementTxStarted()
	metrics.IncrementTxCommitted()
	metrics.IncrementTxRolledBack()

	assert.Equal(t, int64(1), metrics.TxStarted.Load(), "Transaction started should be 1")
	assert.Equal(t, int64(1), metrics.TxCommitted.Load(), "Transaction committed should be 1")
	assert.Equal(t, int64(1), metrics.TxRolledBack.Load(), "Transaction rolled back should be 1")

	// Test query metrics
	metrics.RecordQuery("SELECT", 500000) // 0.5ms
	metrics.RecordQuery("INSERT", 5000000) // 5ms
	metrics.RecordQuery("UPDATE", 50000000) // 50ms
	metrics.RecordQuery("DELETE", 500000000) // 500ms
	metrics.RecordQuery("OTHER", 1500000000) // 1500ms

	assert.Equal(t, int64(5), metrics.QueriesExecuted.Load(), "Queries executed should be 5")
	assert.Equal(t, int64(1), metrics.SelectQueries.Load(), "SELECT queries should be 1")
	assert.Equal(t, int64(1), metrics.InsertQueries.Load(), "INSERT queries should be 1")
	assert.Equal(t, int64(1), metrics.UpdateQueries.Load(), "UPDATE queries should be 1")
	assert.Equal(t, int64(1), metrics.DeleteQueries.Load(), "DELETE queries should be 1")
	assert.Equal(t, int64(1), metrics.OtherQueries.Load(), "OTHER queries should be 1")

	// Test histogram buckets
	assert.Equal(t, int64(1), metrics.QueryTimeUnder1ms.Load(), "Queries under 1ms should be 1")
	assert.Equal(t, int64(1), metrics.QueryTime1to10ms.Load(), "Queries 1-10ms should be 1")
	assert.Equal(t, int64(1), metrics.QueryTime10to100ms.Load(), "Queries 10-100ms should be 1")
	assert.Equal(t, int64(1), metrics.QueryTime100to1000ms.Load(), "Queries 100-1000ms should be 1")
	assert.Equal(t, int64(1), metrics.QueryTimeOver1000ms.Load(), "Queries over 1000ms should be 1")

	// Test verification metrics
	metrics.RecordVerification(true, 10000000)  // 10ms success
	metrics.RecordVerification(false, 20000000) // 20ms failure

	assert.Equal(t, int64(1), metrics.QueriesVerified.Load(), "Queries verified should be 1")
	assert.Equal(t, int64(1), metrics.QueriesFailedVerification.Load(), "Failed verifications should be 1")

	// Test error metrics
	metrics.IncrementErrors()
	assert.Equal(t, int64(1), metrics.Errors.Load(), "Errors should be 1")

	// Test Update method with verification result
	result := types.VerificationResult{
		Success:   true,
		Duration:  100 * time.Millisecond,
		QueryInfo: types.QueryInfo{Type: types.QueryTypeUpdate},
	}
	metrics.Update(result)

	assert.Equal(t, int64(2), metrics.QueriesVerified.Load(), "Queries verified should be 2 after update")
	assert.Equal(t, int64(2), metrics.UpdateQueries.Load(), "UPDATE queries should be 2 after update")
}

func TestMetricsServer(t *testing.T) {
	// Create a new metrics collector
	metrics := NewMetrics()

	// Populate some metrics
	metrics.IncrementTxStarted()
	metrics.IncrementTxStarted()
	metrics.IncrementTxCommitted()
	metrics.RecordQuery("INSERT", 5000000) // 5ms

	// Start the metrics server on a random port to avoid conflicts
	testPort := 9091
	addr := fmt.Sprintf(":%d", testPort)
	err := metrics.StartMetricsServer(addr)
	require.NoError(t, err, "Starting metrics server should not error")

	// Allow the server time to start
	time.Sleep(100 * time.Millisecond)

	// Make a request to the metrics endpoint
	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/metrics", testPort))
	require.NoError(t, err, "HTTP request to metrics server should not error")
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode, "HTTP status code should be 200 OK")

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "Reading response body should not error")

	// Check that metrics output contains expected values
	output := string(body)
	assert.Contains(t, output, "vsqlite_tx_started 2", "Metrics should include tx_started count")
	assert.Contains(t, output, "vsqlite_tx_committed 1", "Metrics should include tx_committed count")
	assert.Contains(t, output, "vsqlite_insert_queries 1", "Metrics should include insert_queries count")

	// Stop the metrics server with a short timeout
	metrics.StopMetricsServer(1 * time.Second)
}
