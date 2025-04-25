package replay

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/gaj/verifiable-sqlite/pkg/capture"
	"github.com/gaj/verifiable-sqlite/pkg/config"
	"github.com/gaj/verifiable-sqlite/pkg/interceptor"
	"github.com/gaj/verifiable-sqlite/pkg/log"
	"github.com/gaj/verifiable-sqlite/pkg/metrics"
	"github.com/gaj/verifiable-sqlite/pkg/types"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Ensure we use sql package
var _ sql.Result = (*MockSQLResult)(nil)

// MockSQLResult implements sql.Result for use in verification
type MockSQLResult struct {
	lastID       int64
	rowsAffected int64
}

// LastInsertId returns the last inserted ID - implements sql.Result
func (m *MockSQLResult) LastInsertId() (int64, error) {
	return m.lastID, nil
}

// RowsAffected returns the number of rows affected - implements sql.Result
func (m *MockSQLResult) RowsAffected() (int64, error) {
	return m.rowsAffected, nil
}

// Engine manages the asynchronous verification of database operations
type Engine struct {
	jobQueue    chan types.VerificationJob
	workerCount int
	stopCh      chan struct{}
	wg          sync.WaitGroup
	running     bool
	mu          sync.Mutex
	config      config.Config
	dbPool      *pgxpool.Pool
	metrics     *metrics.Metrics // Metrics collector for observability
}

// NewEngine creates a new replay engine
func NewEngine(cfg config.Config) *Engine {
	return &Engine{
		jobQueue:    make(chan types.VerificationJob, cfg.JobQueueSize),
		workerCount: cfg.WorkerCount,
		stopCh:      make(chan struct{}),
		config:      cfg,
		// Initialize metrics inline for standalone use, but can be overridden with SetMetrics
		metrics:     nil,
	}
}

// SetMetrics sets the metrics collector for the engine
// This allows the vsqlite package to inject a shared metrics collector
func (e *Engine) SetMetrics(m *metrics.Metrics) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.metrics = m
}

// Start begins the verification worker pool
func (e *Engine) Start() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.running {
		return nil
	}

	// Create a connection pool for SQLite verification
	var err error
	e.dbPool, err = pgxpool.New(context.Background(), "sqlite://:memory:")
	if err != nil {
		return fmt.Errorf("failed to create connection pool: %v", err)
	}

	e.running = true
	e.stopCh = make(chan struct{})

	for i := 0; i < e.workerCount; i++ {
		e.wg.Add(1)
		go e.worker(i)
	}

	log.Info("replay engine started", "workers", e.workerCount)
	return nil
}

// Stop halts the verification worker pool
func (e *Engine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.running {
		return
	}

	close(e.stopCh)
	e.wg.Wait()
	
	if e.dbPool != nil {
		e.dbPool.Close()
	}
	
	e.running = false

	log.Info("replay engine stopped")
}

// SubmitJob adds a verification job to the queue
func (e *Engine) SubmitJob(job types.VerificationJob) error {
	e.mu.Lock()
	running := e.running
	e.mu.Unlock()

	if !running {
		return fmt.Errorf("replay engine is not running")
	}

	select {
	case e.jobQueue <- job:
		log.Debug("job submitted to replay engine", "tx_id", job.TxID)
		return nil
	default:
		log.Warn("job queue is full, dropping job", "tx_id", job.TxID)
		return fmt.Errorf("job queue is full")
	}
}

// worker processes verification jobs from the queue
func (e *Engine) worker(id int) {
	defer e.wg.Done()

	log.Debug("verification worker started", "worker_id", id)

	for {
		select {
		case <-e.stopCh:
			log.Debug("verification worker stopping", "worker_id", id)
			return
		case job := <-e.jobQueue:
			// Create a context with timeout
			ctx, cancel := context.WithTimeout(
				context.Background(),
				time.Duration(e.config.VerificationTimeoutMs)*time.Millisecond,
			)
			
			// Process the job
			result := e.processJob(ctx, job)
			
			// Log the result
			log.LogVerificationResult(result)
			
			// Update metrics if available
			if e.metrics != nil {
				e.metrics.Update(result)
			}
			
			cancel()
		}
	}
}

// processJob performs the verification process for a single job
func (e *Engine) processJob(ctx context.Context, job types.VerificationJob) types.VerificationResult {
	startTime := time.Now()
	
	result := types.VerificationResult{
		TxID:              job.TxID,
		Query:             job.Query,
		Timestamp:         startTime,
		PreRootCaptured:   job.PreRootCaptured,
		PostRootClaimed:   job.PostRootClaimed,
		PostRootCalculated: "",
		Success:           false,
	}

	// Acquire a connection from the pool
	conn, err := e.dbPool.Acquire(ctx)
	if err != nil {
		result.Error = fmt.Sprintf("failed to acquire connection: %v", err)
		result.Duration = time.Since(startTime)
		return result
	}
	defer conn.Release()

	// Begin a transaction for verification
	tx, err := conn.Begin(ctx)
	if err != nil {
		result.Error = fmt.Sprintf("failed to begin transaction: %v", err)
		result.Duration = time.Since(startTime)
		return result
	}
	// Use defer for transaction rollback as a safety net
	// This ensures the transaction is rolled back if not explicitly committed
	defer tx.Rollback(ctx)

	// Set up deterministic session parameters
	err = SetDeterministicSessionParamsWithPgx(ctx, tx)
	if err != nil {
		result.Error = fmt.Sprintf("failed to set deterministic session params: %v", err)
		result.Duration = time.Since(startTime)
		return result
	}

	// Set up temporary verification database
	err = SetupVerificationDBWithPgx(ctx, tx, job.TableSchemas, job.PreStateData)
	if err != nil {
		result.Error = fmt.Sprintf("failed to set up verification DB: %v", err)
		result.Duration = time.Since(startTime)
		return result
	}

	// Parse the SQL query to get required info for state capture
	queryInfo, err := interceptor.AnalyzeQuery(job.SQL)
	if err != nil {
		result.Error = fmt.Sprintf("failed to analyze query: %v", err)
		result.Duration = time.Since(startTime)
		return result
	}
	
	// Store query info for metrics collection
	result.QueryInfo = queryInfo

	// Execute query deterministically with the stored arguments
	err = ExecuteQueriesDeterministicallyWithPgx(ctx, tx, job.SQL, job.Args...)
	if err != nil {
		result.Error = fmt.Sprintf("failed to execute query: %v", err)
		result.Duration = time.Since(startTime)
		return result
	}

	// Create a mock sql.Result implementation for capture
	mockResult := &MockSQLResult{lastID: 1, rowsAffected: 1}
	
	// CRITICAL FIX: Capture post-state using pgx within the SAME transaction
	// BEFORE any commit or rollback
	postState, postRoot, err := capture.CapturePostStateWithPgx(ctx, queryInfo, tx, mockResult, job.PreStateData, job.TableSchemas)
	if err != nil {
		result.Error = fmt.Sprintf("failed to capture post-state: %v", err)
		result.Duration = time.Since(startTime)
		// No need to explicitly rollback here as it's handled by defer
		return result
	}

	// Fill in the calculated post-state root commitment
	result.PostRootCalculated = postRoot
	
	// Compare roots
	if postRoot != job.PostRootClaimed {
		log.Warn("post-state root mismatch", 
			"calculated", postRoot, 
			"claimed", job.PostRootClaimed)
		
		// Find specific mismatches
		mismatches := findMismatches(job.PostStateData, postState)
		result.Mismatches = mismatches
		
		log.Warn("verification failed", "tx_id", job.TxID, "mismatches_count", len(mismatches))
		result.Duration = time.Since(startTime)
		return result
	}
	
	// Mark success and return
	result.Success = true
	log.Info("verification succeeded", "tx_id", job.TxID)
	result.Duration = time.Since(startTime)
	return result
}

// parseQueryForCapture converts a SQL query string to QueryInfo for the capture process
// This reuses the actual interceptor to ensure consistent analysis
func parseQueryForCapture(query string) (types.QueryInfo, error) {
	return interceptor.AnalyzeQuery(query)
}

// findMismatches identifies differences between claimed and calculated post-state
func findMismatches(claimed, calculated map[string][]types.Row) []string {
	var mismatches []string
	
	// Check if tables match
	claimedTables := make(map[string]bool)
	calculatedTables := make(map[string]bool)
	
	for table := range claimed {
		claimedTables[table] = true
	}
	
	for table := range calculated {
		calculatedTables[table] = true
	}
	
	// Check for missing tables
	for table := range claimedTables {
		if !calculatedTables[table] {
			mismatches = append(mismatches, fmt.Sprintf("table '%s' present in claimed state but missing in calculated state", table))
		}
	}
	
	for table := range calculatedTables {
		if !claimedTables[table] {
			mismatches = append(mismatches, fmt.Sprintf("table '%s' present in calculated state but missing in claimed state", table))
		}
	}
	
	// Check row counts
	for table, claimedRows := range claimed {
		calculatedRows, exists := calculated[table]
		if !exists {
			continue // Already reported as missing
		}
		
		if len(claimedRows) != len(calculatedRows) {
			mismatches = append(mismatches, fmt.Sprintf("row count mismatch for table '%s': claimed=%d, calculated=%d", 
				table, len(claimedRows), len(calculatedRows)))
		}
	}
	
	// Build maps of rows by row ID for easier comparison
	for table, claimedRows := range claimed {
		calculatedRows, exists := calculated[table]
		if !exists {
			continue // Already reported as missing
		}
		
		// Skip if row counts don't match (already reported)
		if len(claimedRows) != len(calculatedRows) {
			continue
		}
		
		// Build maps for comparison
		claimedRowMap := make(map[string]types.Row)
		calculatedRowMap := make(map[string]types.Row)
		
		for _, row := range claimedRows {
			claimedRowMap[row.RowID] = row
		}
		
		for _, row := range calculatedRows {
			calculatedRowMap[row.RowID] = row
		}
		
		// Check for missing rows by ID
		for rowID, claimedRow := range claimedRowMap {
			if _, exists := calculatedRowMap[rowID]; !exists {
				mismatches = append(mismatches, fmt.Sprintf("row ID '%s' in table '%s' present in claimed state but missing in calculated state", 
					rowID, table))
			} else {
				// Row exists in both, check if values match
				calculatedRow := calculatedRowMap[rowID]
				
				// Check if column counts match
				if len(claimedRow.Values) != len(calculatedRow.Values) {
					mismatches = append(mismatches, fmt.Sprintf("column count mismatch for row ID '%s' in table '%s': claimed=%d, calculated=%d", 
						rowID, table, len(claimedRow.Values), len(calculatedRow.Values)))
					continue
				}
				
				// Check if column values match
				for col, claimedVal := range claimedRow.Values {
					calculatedVal, exists := calculatedRow.Values[col]
					if !exists {
						mismatches = append(mismatches, fmt.Sprintf("column '%s' for row ID '%s' in table '%s' present in claimed state but missing in calculated state", 
							col, rowID, table))
						continue
					}
					
					// Compare values - for V1 we do simple string comparison
					// In V2 we would need more sophisticated type-aware comparison
					if fmt.Sprintf("%v", claimedVal) != fmt.Sprintf("%v", calculatedVal) {
						mismatches = append(mismatches, fmt.Sprintf("value mismatch for column '%s' in row ID '%s' of table '%s': claimed='%v', calculated='%v'", 
							col, rowID, table, claimedVal, calculatedVal))
					}
				}
			}
		}
		
		// Check for extra rows in calculated state
		for rowID := range calculatedRowMap {
			if _, exists := claimedRowMap[rowID]; !exists {
				mismatches = append(mismatches, fmt.Sprintf("row ID '%s' in table '%s' present in calculated state but missing in claimed state", 
					rowID, table))
			}
		}
	}
	
	return mismatches
}