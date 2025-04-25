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
)

// Ensure we use sql package
var _ sql.Result = (*MockSQLResult)(nil)

// MockSQLResult is a minimal implementation of sql.Result tailored for the
// verification flow.  For now we only need the two methods required by the
// database/sql.Result interface – returning deterministic constant values is
// sufficient for our state-capture logic during tests.
type MockSQLResult struct {
	lastID       int64
	rowsAffected int64
}

// LastInsertId returns a deterministic last-insert id.
func (m *MockSQLResult) LastInsertId() (int64, error) {
	return m.lastID, nil
}

// RowsAffected returns the configured number of affected rows.
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

    // We do not rely on a shared connection pool any more.  Each verification
    // job builds its own in-memory SQLite database via `SetupVerificationDB`,
    // ensuring a completely isolated, deterministic environment.  This makes
    // the engine self-contained and eliminates the previous dependency on a
    // PostgreSQL-specific pgxpool which was not suitable for SQLite.

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
        Success:           false,
    }

    // ---------------------------------------------------------------------
    // Build an isolated in-memory SQLite database that represents the
    // transaction's pre-state.  This guarantees deterministic, side-effect-
    // free verification without any external dependencies.
    // ---------------------------------------------------------------------

    verificationDB, err := SetupVerificationDB(ctx, job.TableSchemas, job.PreStateData)
    if err != nil {
        result.Error = fmt.Sprintf("failed to set up verification DB: %v", err)
        result.Duration = time.Since(startTime)
        return result
    }
    // Always close – the DB only lives for the duration of the verification.
    defer verificationDB.Close()

    var tx *sql.Tx
    var postState map[string][]types.Row
    var postRoot string

    // Check if we're dealing with a sequence of SQL statements (transaction)
    // or a single statement
    if len(job.SQLSequence) > 0 {
        // ------------------------------------------------------------------
        // Handle a sequence of SQL statements (transaction-level verification)
        // ------------------------------------------------------------------
        log.Debug("executing sequence of SQL statements", "count", len(job.SQLSequence))
        
        // Construct a single queryInfo that includes all affected tables
        // This requires us to aggregate tables from all statements in the sequence
        tablesMap := make(map[string]bool)
        for _, sql := range job.SQLSequence {
            // Parse each SQL to identify affected tables
            queryInfo, err := interceptor.AnalyzeQuery(sql)
            if err != nil {
                log.Warn("failed to analyze query in sequence", "sql", sql, "error", err)
                continue
            }
            
            // Add each table to our set
            for _, table := range queryInfo.Tables {
                tablesMap[table] = true
            }
        }
        
        // Convert map to slice for the combined queryInfo
        affectedTables := make([]string, 0, len(tablesMap))
        for table := range tablesMap {
            affectedTables = append(affectedTables, table)
        }
        
        // Create a combined queryInfo for post-state capture
        combinedQueryInfo := types.QueryInfo{
            SQL:    "TRANSACTION", 
            Type:   types.QueryTypeOther,
            Tables: affectedTables,
        }
        
        // Execute the sequence of SQL statements
        tx, err = ExecuteQueriesSequenceDeterministically(ctx, verificationDB, job.SQLSequence, job.ArgsSequence)
        if err != nil {
            result.Error = fmt.Sprintf("failed to execute SQL sequence: %v", err)
            result.Duration = time.Since(startTime)
            return result
        }
        
        // Rollback the tx in case we exit early
        defer tx.Rollback()
        
        // ------------------------------------------------------------------
        // Capture the resulting post-state while still inside the same tx.
        // ------------------------------------------------------------------
        mockResult := &MockSQLResult{lastID: 1, rowsAffected: 1}
        
        postState, postRoot, err = capture.CapturePostStateInTx(ctx, tx, combinedQueryInfo, mockResult, job.PreStateData, job.TableSchemas, nil)
        if err != nil {
            result.Error = fmt.Sprintf("failed to capture post-state: %v", err)
            result.Duration = time.Since(startTime)
            return result
        }
        
        result.PostRootCalculated = postRoot
        
    } else {
        // ------------------------------------------------------------------
        // Handle a single SQL statement (legacy behavior)
        // ------------------------------------------------------------------
        // Determine which SQL string we should execute (historical jobs stored the
        // statement in either `SQL` or the older `Query` field).
        sqlToExecute := job.SQL
        if sqlToExecute == "" {
            sqlToExecute = job.Query
        }
    
        // ------------------------------------------------------------------
        // Parse the SQL so we know which tables / rows to capture afterwards.
        // ------------------------------------------------------------------
        queryInfo, err := interceptor.AnalyzeQuery(sqlToExecute)
        if err != nil {
            result.Error = fmt.Sprintf("failed to analyze query: %v", err)
            result.Duration = time.Since(startTime)
            return result
        }
        result.QueryInfo = queryInfo
    
        // ------------------------------------------------------------------
        // Execute the SQL deterministically inside a single transaction.
        // ------------------------------------------------------------------
        tx, err = ExecuteQueriesDeterministically(ctx, verificationDB, sqlToExecute, job.Args...)
        if err != nil {
            result.Error = fmt.Sprintf("failed to execute query: %v", err)
            result.Duration = time.Since(startTime)
            return result
        }
        // Rollback the tx in case we exit early
        defer tx.Rollback()
    
        // ------------------------------------------------------------------
        // Capture the resulting post-state while still inside the same tx.
        // ------------------------------------------------------------------
        mockResult := &MockSQLResult{lastID: 1, rowsAffected: 1}
    
        postState, postRoot, err = capture.CapturePostStateInTx(ctx, tx, queryInfo, mockResult, job.PreStateData, job.TableSchemas, job.Args)
        if err != nil {
            result.Error = fmt.Sprintf("failed to capture post-state: %v", err)
            result.Duration = time.Since(startTime)
            return result
        }
        
        result.PostRootCalculated = postRoot
    }

    // ------------------------------------------------------------------
    // We intentionally ROLLBACK (via the deferred call) after capturing the
    // post-state.  Committing here would make the temp DB persist beyond the
    // verification scope and is **not** required for deterministic replay.
    // ------------------------------------------------------------------

    if result.PostRootCalculated != job.PostRootClaimed {
        log.Warn("post-state root mismatch", "calculated", result.PostRootCalculated, "claimed", job.PostRootClaimed)

        // For transaction-level verification, we need to get the postState from
        // the outer scope, but this might be nil if there was an error
        var mismatches []string
        if postState != nil {
            mismatches = findMismatches(job.PostStateData, postState)
        } else {
            mismatches = []string{"Failed to calculate post-state for comparison"}
        }
        result.Mismatches = mismatches

        result.Duration = time.Since(startTime)
        return result // success=false by default
    }

    result.Success = true
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