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
	preCaptured bool
	preState    map[string][]types.Row
	schemas     map[string]types.TableSchema
	preRoot     string

	postState   map[string][]types.Row
	postRoot    string
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
    startTime := time.Now()

    // Capture pre-state once (before the first DML in this Tx)
    if !tx.preCaptured {
        pre, sch, root, err := capture.CapturePreStateInTx(ctx, tx.Tx, queryInfo, args)
        if err != nil {
            log.Warn("failed to capture pre-state", "tx_id", tx.txID, "error", err)
            if metricsCollector != nil {
                metricsCollector.IncrementErrors()
            }
        } else {
            tx.preCaptured = true
            tx.preState = pre
            tx.schemas = sch
            tx.preRoot = root
        }
    }

    // Execute the user query
    result, err := tx.Tx.ExecContext(ctx, query, args...)
    if err != nil {
        if metricsCollector != nil {
            metricsCollector.IncrementErrors()
        }
        return nil, err
    }

    // Update post-state snapshot after this statement
    post, root, err := capture.CapturePostStateInTx(ctx, tx.Tx, queryInfo, result, tx.preState, tx.schemas, args)
    if err == nil {
        tx.postState = post
        tx.postRoot = root
    } else {
        if metricsCollector != nil {
            metricsCollector.IncrementErrors()
        }
    }

    // Metrics per statement
    if metricsCollector != nil {
        metricsCollector.RecordQuery(queryInfo.Type.String(), time.Since(startTime).Nanoseconds())
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

	// If verification data was captured, submit a single job now
	if err == nil && tx.preCaptured && verificationEngine != nil {
		job := types.VerificationJob{
			TxID:            tx.txID,
			Query:           "TRANSACTION",
			SQL:             strings.Join(tx.queries, ";\n"),
			Args:            nil, // Not replaying with placeholders across multi stmt yet
			PreStateData:    tx.preState,
			PostStateData:   tx.postState,
			TableSchemas:    tx.schemas,
			PreRootCaptured: tx.preRoot,
			PostRootClaimed: tx.postRoot,
		}
        if errSubmit := verificationEngine.SubmitJob(job); errSubmit != nil {
            log.Warn("failed to submit verification job", "tx_id", tx.txID, "error", errSubmit)
            if metricsCollector != nil {
                metricsCollector.IncrementErrors()
            }
		}
	}

	if metricsCollector != nil {
		if err == nil {
			metricsCollector.IncrementTxCommitted()
		} else {
			metricsCollector.IncrementTxRolledBack()
		}
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