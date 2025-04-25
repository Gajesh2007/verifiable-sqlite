package vsqlite

import (
    "context"
    "database/sql"
    "strings"
    "time"

	"github.com/gaj/verifiable-sqlite/pkg/capture"
	"github.com/gaj/verifiable-sqlite/pkg/interceptor"
	"github.com/gaj/verifiable-sqlite/pkg/log"
	"github.com/gaj/verifiable-sqlite/pkg/types"
)

// Tx is a wrapper around sql.Tx that adds verification
type Tx struct {
	*sql.Tx
	db      *DB
	ctx     context.Context
	txID    string
	queries []string

	// Aggregated verification data (per full transaction)
	preCaptured    bool
	preState       map[string][]types.Row
	schemas        map[string]types.TableSchema
	preRoot        string
	aggregatedArgs [][]interface{} // Store args for each query
	affectedTables map[string]bool // Track all affected tables
}

// ExecContext executes a query within the transaction
func (tx *Tx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	log.Debug("executing query in transaction", "tx_id", tx.txID, "query", query)

	// Add to transaction history BEFORE execution
	tx.queries = append(tx.queries, query)
	// Store args associated with this query
	currentArgs := make([]interface{}, len(args))
    copy(currentArgs, args)
	tx.aggregatedArgs = append(tx.aggregatedArgs, currentArgs)

	// Analyze the query
	queryInfo, err := interceptor.AnalyzeQuery(query)
	if err != nil {
		log.Warn("failed to analyze query", "tx_id", tx.txID, "error", err)
		// Fallback: Assume it might be DML if analysis fails, but capture might be inaccurate
		// A safer approach might be to disallow verification if analysis fails. For V1, we proceed.
		if queryInfo.Type == types.QueryTypeUnknown {
			// Simplistic guess based on keywords for fallback
			lowerQuery := strings.ToLower(strings.TrimSpace(query))
			if strings.HasPrefix(lowerQuery, "insert") || strings.HasPrefix(lowerQuery, "update") || strings.HasPrefix(lowerQuery, "delete") {
				queryInfo.Type = types.QueryTypeOther // Mark as other, but attempt capture
				// Need table names - this is where robust parsing is crucial
			} else {
                 // If not DML or analysis failed, execute normally without verification steps
                return tx.Tx.ExecContext(ctx, query, args...)
            }
		}
	}

	// Handle DML operations specifically for state capture
	if queryInfo.Type == types.QueryTypeInsert ||
		queryInfo.Type == types.QueryTypeUpdate ||
		queryInfo.Type == types.QueryTypeDelete {

		startTime := time.Now() // Start timer specifically for DML + capture overhead

		// Capture pre-state ONCE before the *first* DML in this Tx
		if !tx.preCaptured {
			// IMPORTANT: Need to determine affected tables for pre-state capture
            // If queryInfo has tables, use them. If analysis failed, this is tricky.
            // For V1, maybe capture pre-state only if analysis succeeded?
            if len(queryInfo.Tables) > 0 {
				// Track all tables affected throughout the transaction
				if tx.affectedTables == nil {
					tx.affectedTables = make(map[string]bool)
				}
				for _, table := range queryInfo.Tables {
					tx.affectedTables[table] = true
				}

                pre, sch, root, captureErr := capture.CapturePreStateInTx(ctx, tx.Tx, queryInfo, args)
                if captureErr != nil {
                    log.Warn("failed to capture pre-state", "tx_id", tx.txID, "error", captureErr)
					// Potentially mark transaction as non-verifiable? Or proceed with missing pre-state.
					if metricsCollector != nil {
						metricsCollector.IncrementErrors()
					}
                } else {
                    tx.preCaptured = true
                    tx.preState = pre
                    tx.schemas = sch // Store schemas captured with pre-state
                    tx.preRoot = root
                }
            } else {
				log.Warn("skipping pre-state capture, no tables identified", "tx_id", tx.txID, "query", query)
			}
		} else {
			// If pre-state was already captured, ensure we track tables affected by subsequent DMLs
			if tx.affectedTables == nil {
				tx.affectedTables = make(map[string]bool)
			}
			for _, table := range queryInfo.Tables {
				tx.affectedTables[table] = true
			}
		}

		// Execute the user's query
		result, execErr := tx.Tx.ExecContext(ctx, query, args...)
		if execErr != nil {
            if metricsCollector != nil {
                metricsCollector.IncrementErrors()
            }
			return nil, execErr // Propagate DB errors
		}

		// DO NOT capture post-state here. It will be done once before Commit.

		// Record query metrics
		if metricsCollector != nil {
			metricsCollector.RecordQuery(queryInfo.Type.String(), time.Since(startTime).Nanoseconds())
		}

		return result, nil // Return result of user's DML
	}

	// For non-DML operations, execute normally
	return tx.Tx.ExecContext(ctx, query, args...)
}

// Exec executes a query within the transaction
func (tx *Tx) Exec(query string, args ...interface{}) (sql.Result, error) {
	return tx.ExecContext(tx.ctx, query, args...)
}

// QueryContext executes a query that returns rows
func (tx *Tx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	log.Debug("querying in transaction", "tx_id", tx.txID, "query", query)

	// Add to transaction history
	tx.queries = append(tx.queries, query)

	// Analyze the query for warnings only
	_, err := interceptor.AnalyzeQuery(query)
	if err != nil {
		log.Warn("failed to analyze query", "tx_id", tx.txID, "error", err)
		// Continue with execution even if analysis fails
	}

	return tx.Tx.QueryContext(ctx, query, args...)
}

// Query executes a query that returns rows
func (tx *Tx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return tx.QueryContext(tx.ctx, query, args...)
}

// QueryRowContext executes a query that returns a single row
func (tx *Tx) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	log.Debug("querying for single row in transaction", "tx_id", tx.txID, "query", query)

	// Add to transaction history
	tx.queries = append(tx.queries, query)

	// Analyze the query for warnings only
	_, err := interceptor.AnalyzeQuery(query)
	if err != nil {
		log.Warn("failed to analyze query", "tx_id", tx.txID, "error", err)
		// Continue with execution even if analysis fails
	}

	return tx.Tx.QueryRowContext(ctx, query, args...)
}

// QueryRow executes a query that returns a single row
func (tx *Tx) QueryRow(query string, args ...interface{}) *sql.Row {
	return tx.QueryRowContext(tx.ctx, query, args...)
}

// Commit commits the transaction
func (tx *Tx) Commit() error {
	log.Debug("committing transaction", "tx_id", tx.txID, "query_count", len(tx.queries))

	// If verification is enabled AND at least one DML was executed...
	if verificationEngine != nil && tx.preCaptured {
		// 1. Capture the FINAL post-state just before committing
		var finalPostState map[string][]types.Row
		var finalPostRoot string
		var captureErr error

		// Need to construct a QueryInfo representing all affected tables
		affectedTablesList := make([]string, 0, len(tx.affectedTables))
		for table := range tx.affectedTables {
			affectedTablesList = append(affectedTablesList, table)
		}
		finalQueryInfo := types.QueryInfo{
			SQL:    "End of Transaction State Capture", // Placeholder SQL
			Type:   types.QueryTypeOther, // Not tied to a single DML type
			Tables: affectedTablesList,
			// PKValues/Columns are not directly relevant for capturing *all* rows of affected tables
		}

		// Create a simple implementation of sql.Result directly for the final state capture
        mockResult := sqlResultMock{0, 0}

		// Pass nil for args, as we are capturing the final state, not state related to a specific DML with args
		finalPostState, finalPostRoot, captureErr = capture.CapturePostStateInTx(tx.ctx, tx.Tx, finalQueryInfo, mockResult, tx.preState, tx.schemas, nil)
		if captureErr != nil {
			log.Error("failed to capture final post-state", "tx_id", tx.txID, "error", captureErr)
			// Decide handling: Rollback? Log error and commit anyway?
			// For V1, log and proceed to commit, but verification job will likely fail/be inaccurate.
			if metricsCollector != nil {
                metricsCollector.IncrementErrors()
            }
		}

		// 2. Log the single StateCommitmentRecord for the whole transaction
		commitmentRecord := types.StateCommitmentRecord{
			TxID:            tx.txID,
			Query:           "TRANSACTION", // Represents the whole transaction
			SQL:             strings.Join(tx.queries, ";\n"),
			Timestamp:       time.Now(),
			PreRootCaptured: tx.preRoot,     // Root before first DML
			PostRootClaimed: finalPostRoot, // Root after last DML (before commit)
			// PerformanceMs: Needs calculation if desired for the whole TX
		}
		log.LogStateCommitment(commitmentRecord)

		// 3. Submit the single verification job for the whole transaction
		job := types.VerificationJob{
			TxID:            tx.txID,
			Query:           "TRANSACTION",
			SQLSequence:     tx.queries,         // Send the sequence
			ArgsSequence:    tx.aggregatedArgs,  // Send the aggregated args
			PreStateData:    tx.preState,        // State before first DML
			PostStateData:   finalPostState,     // Final captured state before commit
			TableSchemas:    tx.schemas,
			PreRootCaptured: tx.preRoot,
			PostRootClaimed: finalPostRoot,
		}

		if errSubmit := verificationEngine.SubmitJob(job); errSubmit != nil {
			log.Warn("failed to submit verification job", "tx_id", tx.txID, "error", errSubmit)
			if metricsCollector != nil {
				metricsCollector.IncrementErrors()
			}
		}
	}

	// 4. Perform the actual commit
	err := tx.Tx.Commit()

	// 5. Update metrics based on commit success/failure
	if metricsCollector != nil {
		if err == nil {
			metricsCollector.IncrementTxCommitted()
		} else {
			// If commit fails, it's effectively a rollback
			metricsCollector.IncrementTxRolledBack()
		}
	}

	// Log transaction result
	if err == nil && len(tx.queries) > 0 {
		log.Info("transaction committed",
			"tx_id", tx.txID,
			"query_count", len(tx.queries))
	} else if err != nil {
		log.Error("transaction commit failed", "tx_id", tx.txID, "error", err)
	}

	return err
}

// Rollback aborts the transaction
func (tx *Tx) Rollback() error {
	log.Debug("rolling back transaction", "tx_id", tx.txID, "query_count", len(tx.queries))
	
	// Perform the actual rollback
	err := tx.Tx.Rollback()
	
	// Log transaction summary
	if err == nil && len(tx.queries) > 0 {
		log.Info("transaction rolled back",
			"tx_id", tx.txID,
			"query_count", len(tx.queries))
	}

	if metricsCollector != nil {
		metricsCollector.IncrementTxRolledBack()
	}
	
	return err
}

type sqlResultMock struct {
	lastID       int64
	rowsAffected int64
}

func (r sqlResultMock) LastInsertId() (int64, error) {
	return r.lastID, nil
}

func (r sqlResultMock) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}