package replay

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gaj/verifiable-sqlite/pkg/capture"
	"github.com/gaj/verifiable-sqlite/pkg/config"
	"github.com/gaj/verifiable-sqlite/pkg/interceptor"
	"github.com/gaj/verifiable-sqlite/pkg/log"
	"github.com/gaj/verifiable-sqlite/pkg/types"
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
}

// NewEngine creates a new replay engine
func NewEngine(cfg config.Config) *Engine {
	return &Engine{
		jobQueue:    make(chan types.VerificationJob, cfg.JobQueueSize),
		workerCount: cfg.WorkerCount,
		stopCh:      make(chan struct{}),
		config:      cfg,
	}
}

// Start begins the verification worker pool
func (e *Engine) Start() {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.running {
		return
	}

	e.running = true
	e.stopCh = make(chan struct{})

	for i := 0; i < e.workerCount; i++ {
		e.wg.Add(1)
		go e.worker(i)
	}

	log.Info("replay engine started", "workers", e.workerCount)
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

	log.Debug("worker started", "worker_id", id)

	for {
		select {
		case <-e.stopCh:
			log.Debug("worker stopping", "worker_id", id)
			return
		case job := <-e.jobQueue:
			log.Debug("worker processing job", "worker_id", id, "tx_id", job.TxID)
			
			// Create timeout context
			ctx, cancel := context.WithTimeout(context.Background(), 
				time.Duration(e.config.VerificationTimeoutMs)*time.Millisecond)
			
			// Process the job
			result := e.processJob(ctx, job)
			
			// Log the result
			log.LogVerificationResult(result)
			
			cancel()
		}
	}
}

// processJob performs the verification process for a single job
func (e *Engine) processJob(ctx context.Context, job types.VerificationJob) types.VerificationResult {
	result := types.VerificationResult{
		TxID:              job.TxID,
		Query:             job.Query,
		Timestamp:         time.Now(),
		PreRootCaptured:   job.PreRootCaptured,
		PostRootClaimed:   job.PostRootClaimed,
		PostRootCalculated: "",
		Success:           false,
	}

	// Set up temporary verification database
	db, err := SetupVerificationDB(ctx, job.TableSchemas, job.PreStateData)
	if err != nil {
		result.Error = fmt.Sprintf("failed to set up verification DB: %v", err)
		return result
	}
	defer db.Close()

	// Execute query deterministically with the stored arguments
	tx, execErr := ExecuteQueriesDeterministically(ctx, db, job.SQL, job.Args...)
	if execErr != nil {
		result.Error = fmt.Sprintf("failed to execute query: %v", execErr)
		return result
	}

	// Capture post-state using the same capture logic
	queryInfo, err := parseQueryForCapture(job.SQL)
	if err != nil {
		result.Error = fmt.Sprintf("failed to parse query for capture: %v", err)
		tx.Rollback()
		return result
	}

	// Create a mock sql.Result implementation
	mockResult := &MockSQLResult{lastID: 1, rowsAffected: 1}
	
	// Capture post-state
	postState, postRoot, err := capture.CapturePostStateInTx(ctx, tx, queryInfo, mockResult, job.PreStateData, job.TableSchemas)
	if err != nil {
		result.Error = fmt.Sprintf("failed to capture post-state: %v", err)
		tx.Rollback()
		return result
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		result.Error = fmt.Sprintf("failed to commit transaction: %v", err)
		return result
	}

	// Compare the calculated root with the claimed root
	result.PostRootCalculated = postRoot
	result.Success = (postRoot == job.PostRootClaimed)

	if !result.Success {
		// Find mismatches
		result.Mismatches = findMismatches(job.PostStateData, postState)
	}

	return result
}

// parseQueryForCapture converts a SQL query string to QueryInfo for the capture process
// This reuses the actual interceptor to ensure consistent analysis
func parseQueryForCapture(query string) (types.QueryInfo, error) {
	// Use the actual query interceptor to ensure consistent behavior
	return interceptor.AnalyzeQuery(query)
}

// determineQueryType is a simple function to determine query type from SQL string
func determineQueryType(query string) types.QueryType {
	queryLower := strings.ToLower(query)
	if strings.HasPrefix(queryLower, "select") {
		return types.QueryTypeSelect
	} else if strings.HasPrefix(queryLower, "insert") {
		return types.QueryTypeInsert
	} else if strings.HasPrefix(queryLower, "update") {
		return types.QueryTypeUpdate
	} else if strings.HasPrefix(queryLower, "delete") {
		return types.QueryTypeDelete
	}
	return types.QueryTypeUnknown
}

// extractTablesFromQuery extracts table names from a SQL query string
// This is a simplified version for the replay engine
func extractTablesFromQuery(query string) []string {
	// For V1, this is a stub - in a real implementation, we would parse the query
	// The job already has the tables, so this isn't critical for replay
	return []string{}
}

// findMismatches identifies differences between claimed and calculated post-state
func findMismatches(claimed, calculated map[string][]types.Row) []string {
	var mismatches []string

	// Compare table by table
	for tableName, claimedRows := range claimed {
		calculatedRows, exists := calculated[tableName]
		if !exists {
			mismatches = append(mismatches, fmt.Sprintf("table %s missing in calculated state", tableName))
			continue
		}

		// Create maps for easier comparison
		claimedMap := make(map[string]types.Row)
		for _, row := range claimedRows {
			claimedMap[row.RowID] = row
		}

		calculatedMap := make(map[string]types.Row)
		for _, row := range calculatedRows {
			calculatedMap[row.RowID] = row
		}

		// Check for missing or extra rows
		for rowID := range claimedMap {
			if _, exists := calculatedMap[rowID]; !exists {
				mismatches = append(mismatches, fmt.Sprintf("row %s missing in calculated state", rowID))
			}
		}

		for rowID := range calculatedMap {
			if _, exists := claimedMap[rowID]; !exists {
				mismatches = append(mismatches, fmt.Sprintf("extra row %s in calculated state", rowID))
			}
		}

		// Compare common rows
		for rowID, claimedRow := range claimedMap {
			calculatedRow, exists := calculatedMap[rowID]
			if !exists {
				continue // Already reported as missing
			}

			// Compare values
			for colName, claimedVal := range claimedRow.Values {
				calculatedVal, exists := calculatedRow.Values[colName]
				if !exists {
					mismatches = append(mismatches, fmt.Sprintf("column %s missing in row %s", colName, rowID))
					continue
				}

				// Compare values - this is simplified and might need more robust comparison
				if fmt.Sprintf("%v", claimedVal) != fmt.Sprintf("%v", calculatedVal) {
					mismatches = append(mismatches, 
						fmt.Sprintf("value mismatch in row %s, column %s: claimed=%v, calculated=%v", 
							rowID, colName, claimedVal, calculatedVal))
				}
			}
		}
	}

	return mismatches
}