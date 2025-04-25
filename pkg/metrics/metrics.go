package metrics

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gaj/verifiable-sqlite/pkg/log"
	"github.com/gaj/verifiable-sqlite/pkg/types"
)

// Metrics collects and exposes metrics for Verifiable SQLite
type Metrics struct {
	// Transaction metrics
	TxStarted     atomic.Int64
	TxCommitted   atomic.Int64
	TxRolledBack  atomic.Int64
	
	// Connection metrics
	ConnectionsOpened  atomic.Int64
	ConnectionsClosed  atomic.Int64
	
	// Query metrics
	QueriesExecuted         atomic.Int64
	QueriesVerified         atomic.Int64
	QueriesFailedVerification atomic.Int64
	
	// Per operation type (SELECT, INSERT, UPDATE, DELETE, OTHER)
	SelectQueries atomic.Int64
	InsertQueries atomic.Int64
	UpdateQueries atomic.Int64
	DeleteQueries atomic.Int64
	OtherQueries  atomic.Int64

	// Performance metrics
	CumulativeQueryTimeNs   atomic.Int64
	CumulativeVerificationTimeNs atomic.Int64

	// Histogram buckets for query execution time (ms)
	QueryTimeUnder1ms   atomic.Int64
	QueryTime1to10ms    atomic.Int64
	QueryTime10to100ms  atomic.Int64
	QueryTime100to1000ms atomic.Int64
	QueryTimeOver1000ms atomic.Int64
	
	// Errors
	Errors atomic.Int64
	
	// Metrics server
	server *http.Server
}

// NewMetrics creates a new metrics collector
func NewMetrics() *Metrics {
	return &Metrics{}
}

// IncrementTxStarted increments the number of transactions started
func (m *Metrics) IncrementTxStarted() {
	m.TxStarted.Add(1)
}

// IncrementTxCommitted increments the number of transactions committed
func (m *Metrics) IncrementTxCommitted() {
	m.TxCommitted.Add(1)
}

// IncrementTxRolledBack increments the number of transactions rolled back
func (m *Metrics) IncrementTxRolledBack() {
	m.TxRolledBack.Add(1)
}

// RecordQuery records metrics for a query
func (m *Metrics) RecordQuery(queryType string, durationNs int64) {
	m.QueriesExecuted.Add(1)
	m.CumulativeQueryTimeNs.Add(durationNs)

	// Record query type
	switch queryType {
	case "SELECT":
		m.SelectQueries.Add(1)
	case "INSERT":
		m.InsertQueries.Add(1)
	case "UPDATE":
		m.UpdateQueries.Add(1)
	case "DELETE":
		m.DeleteQueries.Add(1)
	default:
		m.OtherQueries.Add(1)
	}

	// Record in histogram buckets
	durationMs := durationNs / 1_000_000
	switch {
	case durationMs < 1:
		m.QueryTimeUnder1ms.Add(1)
	case durationMs < 10:
		m.QueryTime1to10ms.Add(1)
	case durationMs < 100:
		m.QueryTime10to100ms.Add(1)
	case durationMs < 1000:
		m.QueryTime100to1000ms.Add(1)
	default:
		m.QueryTimeOver1000ms.Add(1)
	}
}

// RecordVerification records metrics for a verification
func (m *Metrics) RecordVerification(success bool, durationNs int64) {
	if success {
		m.QueriesVerified.Add(1)
	} else {
		m.QueriesFailedVerification.Add(1)
	}
	m.CumulativeVerificationTimeNs.Add(durationNs)
}

// IncrementErrors increments the error count
func (m *Metrics) IncrementErrors() {
	m.Errors.Add(1)
}

// Update updates metrics based on a verification result
func (m *Metrics) Update(result types.VerificationResult) {
	// Record verification result
	success := result.Success
	
	// Record verification metrics
	durationNs := result.Duration.Nanoseconds()
	m.RecordVerification(success, durationNs)
	
	// Record query type if available
	if result.QueryInfo.Type != "" {
		queryType := string(result.QueryInfo.Type)
		m.RecordQuery(queryType, durationNs)
	}
	
	// Count errors if verification failed
	if !success {
		m.IncrementErrors()
	}
}

// IncrementConnectionOpened increments the count of opened database connections
func (m *Metrics) IncrementConnectionOpened() {
	m.ConnectionsOpened.Add(1)
}

// IncrementConnectionClosed increments the count of closed database connections
func (m *Metrics) IncrementConnectionClosed() {
	m.ConnectionsClosed.Add(1)
}

// MetricsHandler returns an HTTP handler for metrics
func (m *Metrics) MetricsHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "# Verifiable SQLite Metrics\n\n")
		
		// Transaction metrics
		fmt.Fprintf(w, "# Transaction Metrics\n")
		fmt.Fprintf(w, "vsqlite_tx_started %d\n", m.TxStarted.Load())
		fmt.Fprintf(w, "vsqlite_tx_committed %d\n", m.TxCommitted.Load())
		fmt.Fprintf(w, "vsqlite_tx_rolled_back %d\n", m.TxRolledBack.Load())
		
		// Connection metrics
		fmt.Fprintf(w, "\n# Connection Metrics\n")
		fmt.Fprintf(w, "vsqlite_connections_opened %d\n", m.ConnectionsOpened.Load())
		fmt.Fprintf(w, "vsqlite_connections_closed %d\n", m.ConnectionsClosed.Load())
		
		// Query metrics
		fmt.Fprintf(w, "\n# Query Metrics\n")
		fmt.Fprintf(w, "vsqlite_queries_executed %d\n", m.QueriesExecuted.Load())
		fmt.Fprintf(w, "vsqlite_queries_verified %d\n", m.QueriesVerified.Load())
		fmt.Fprintf(w, "vsqlite_queries_failed_verification %d\n", m.QueriesFailedVerification.Load())
		
		// Query types
		fmt.Fprintf(w, "\n# Query Types\n")
		fmt.Fprintf(w, "vsqlite_select_queries %d\n", m.SelectQueries.Load())
		fmt.Fprintf(w, "vsqlite_insert_queries %d\n", m.InsertQueries.Load())
		fmt.Fprintf(w, "vsqlite_update_queries %d\n", m.UpdateQueries.Load())
		fmt.Fprintf(w, "vsqlite_delete_queries %d\n", m.DeleteQueries.Load())
		fmt.Fprintf(w, "vsqlite_other_queries %d\n", m.OtherQueries.Load())
		
		// Performance metrics
		fmt.Fprintf(w, "\n# Performance Metrics\n")
		fmt.Fprintf(w, "vsqlite_cumulative_query_time_ns %d\n", m.CumulativeQueryTimeNs.Load())
		fmt.Fprintf(w, "vsqlite_cumulative_verification_time_ns %d\n", m.CumulativeVerificationTimeNs.Load())
		
		// Average time if we have data
		if queries := m.QueriesExecuted.Load(); queries > 0 {
			avgQueryTimeNs := m.CumulativeQueryTimeNs.Load() / queries
			fmt.Fprintf(w, "vsqlite_avg_query_time_ns %d\n", avgQueryTimeNs)
		}
		
		if verifications := m.QueriesVerified.Load() + m.QueriesFailedVerification.Load(); verifications > 0 {
			avgVerificationTimeNs := m.CumulativeVerificationTimeNs.Load() / verifications
			fmt.Fprintf(w, "vsqlite_avg_verification_time_ns %d\n", avgVerificationTimeNs)
		}
		
		// Latency histograms
		fmt.Fprintf(w, "\n# Latency Histograms\n")
		fmt.Fprintf(w, "vsqlite_query_time_under_1ms %d\n", m.QueryTimeUnder1ms.Load())
		fmt.Fprintf(w, "vsqlite_query_time_1_10ms %d\n", m.QueryTime1to10ms.Load())
		fmt.Fprintf(w, "vsqlite_query_time_10_100ms %d\n", m.QueryTime10to100ms.Load())
		fmt.Fprintf(w, "vsqlite_query_time_100_1000ms %d\n", m.QueryTime100to1000ms.Load())
		fmt.Fprintf(w, "vsqlite_query_time_over_1000ms %d\n", m.QueryTimeOver1000ms.Load())
		
		// Errors
		fmt.Fprintf(w, "\n# Errors\n")
		fmt.Fprintf(w, "vsqlite_errors %d\n", m.Errors.Load())
	}
}

// StartMetricsServer starts an HTTP server to expose metrics
func (m *Metrics) StartMetricsServer(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", m.MetricsHandler())
	
	m.server = &http.Server{
		Addr:    addr,
		Handler: mux,
	}
	
	go func() {
		if err := m.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("metrics server error", "error", err)
		}
	}()
	
	log.Info("metrics server started", "addr", addr)
	return nil
}

// StopMetricsServer stops the metrics server
func (m *Metrics) StopMetricsServer(timeout time.Duration) {
	if m.server == nil {
		return
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	
	if err := m.server.Shutdown(ctx); err != nil {
		log.Error("error shutting down metrics server", "error", err)
	}
	
	log.Info("metrics server stopped")
}
