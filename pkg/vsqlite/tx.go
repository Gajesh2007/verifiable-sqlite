package vsqlite

import (
	"context"
	"database/sql"
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
}

// ExecContext executes a query within the transaction
func (tx *Tx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	log.Debug("executing query in transaction", "tx_id", tx.txID, "query", query)

	// Add to transaction history
	tx.queries = append(tx.queries, query)

	// Analyze the query
	queryInfo, err := interceptor.AnalyzeQuery(query)
	if err != nil {
		log.Warn("failed to analyze query", "tx_id", tx.txID, "error", err)
		// Continue with execution even if analysis fails
	}

	// For DML operations, capture state and verify
	if queryInfo.Type == types.QueryTypeInsert ||
		queryInfo.Type == types.QueryTypeUpdate ||
		queryInfo.Type == types.QueryTypeDelete {

		return tx.execWithVerification(ctx, queryInfo, query, args...)
	}

	// For non-DML operations, execute normally
	return tx.Tx.ExecContext(ctx, query, args...)
}

// execWithVerification executes a DML query with state capture and verification
func (tx *Tx) execWithVerification(ctx context.Context, queryInfo types.QueryInfo, query string, args ...interface{}) (sql.Result, error) {
	// Track performance
	startTime := time.Now()

	// 1. Capture pre-state
	preState, schemas, preRoot, err := capture.CapturePreStateInTx(ctx, tx.Tx, queryInfo)
	if err != nil {
		log.Warn("failed to capture pre-state", "tx_id", tx.txID, "error", err)
		// Continue with execution even if capture fails
	}

	// 2. Execute the user's query
	result, err := tx.Tx.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	// 3. Capture post-state
	postState, postRoot, err := capture.CapturePostStateInTx(ctx, tx.Tx, queryInfo, result, preState, schemas)
	if err != nil {
		log.Warn("failed to capture post-state", "tx_id", tx.txID, "error", err)
		// Continue even if capture fails, just skip verification
	}

	// Calculate execution time
	executionTime := time.Since(startTime).Milliseconds()

	// 4. Log state commitment record
	commitmentRecord := types.StateCommitmentRecord{
		TxID:            tx.txID,
		Query:           queryInfo.Type.String() + " query",
		SQL:             query,
		Timestamp:       time.Now(),
		PreRootCaptured: preRoot,
		PostRootClaimed: postRoot,
		PerformanceMs:   executionTime,
	}
	log.LogStateCommitment(commitmentRecord)

	// 5. Submit verification job asynchronously
	if verificationEngine != nil {
		// Convert args to interface{} slice that can be stored
		queryArgs := make([]interface{}, len(args))
		for i, arg := range args {
			queryArgs[i] = arg
		}
		
		job := types.VerificationJob{
			TxID:            tx.txID,
			Query:           queryInfo.Type.String(),
			SQL:             query,
			Args:            queryArgs,
			PreStateData:    preState,
			PostStateData:   postState,
			TableSchemas:    schemas,
			PreRootCaptured: preRoot,
			PostRootClaimed: postRoot,
		}

		if err := verificationEngine.SubmitJob(job); err != nil {
			log.Warn("failed to submit verification job", "tx_id", tx.txID, "error", err)
		}
	}

	return result, nil
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
	
	// Perform the actual commit
	err := tx.Tx.Commit()
	
	// Log transaction summary
	if err == nil && len(tx.queries) > 0 {
		log.Info("transaction committed",
			"tx_id", tx.txID,
			"query_count", len(tx.queries))
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
	
	return err
}