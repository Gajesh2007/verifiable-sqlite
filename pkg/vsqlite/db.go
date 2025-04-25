package vsqlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/gaj/verifiable-sqlite/pkg/interceptor"
	"github.com/gaj/verifiable-sqlite/pkg/log"
	"github.com/gaj/verifiable-sqlite/pkg/types"
)

// DB is a wrapper around sql.DB that adds verification
type DB struct {
	*sql.DB
	dsn string
}

// Open opens a connection to a SQLite database and returns a wrapped DB
func Open(driverName, dataSourceName string) (*DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	// Ping to verify the connection works
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}

	log.Debug("opened connection to SQLite database", "driver", driverName, "dsn", dataSourceName)
	return &DB{DB: db, dsn: dataSourceName}, nil
}

// BeginTx starts a transaction and returns a wrapped Tx
func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	log.Debug("beginning transaction")
	
	// Start the transaction using the underlying sql.DB
	tx, err := db.DB.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}

	// Metrics: transaction started
	if metricsCollector != nil {
		metricsCollector.IncrementTxStarted()
	}

	// Create a transaction ID for tracking
	txID := fmt.Sprintf("tx-%d", time.Now().UnixNano())
	
	// Create and return the wrapped transaction
	return &Tx{
		Tx:      tx,
		db:      db,
		ctx:     ctx,
		txID:    txID,
		queries: []string{},
	}, nil
}

// Begin starts a transaction and returns a wrapped Tx
func (db *DB) Begin() (*Tx, error) {
	return db.BeginTx(context.Background(), nil)
}

// Exec executes a query without returning any rows
// It does not provide verification as it's not transaction-scoped
func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.ExecContext(context.Background(), query, args...)
}

// ExecContext executes a query without returning any rows
// If the query is a DML operation, it will be intercepted for verification
func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	log.Debug("executing query on DB", "query", query)
	
	// Analyze the query
	queryInfo, err := interceptor.AnalyzeQuery(query)
	if err != nil {
		log.Warn("failed to analyze query", "error", err)
		// Continue with execution even if analysis fails
	}

	// For non-transactional DML, we create a transaction automatically
	if queryInfo.Type == types.QueryTypeInsert || 
	   queryInfo.Type == types.QueryTypeUpdate || 
	   queryInfo.Type == types.QueryTypeDelete {
		
		// Begin a transaction
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to begin auto-transaction for DML: %v", err)
		}
		
		// Execute the query in the transaction
		result, err := tx.ExecContext(ctx, query, args...)
		if err != nil {
			tx.Rollback()
			return nil, err
		}
		
		// Commit the transaction
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("failed to commit auto-transaction: %v", err)
		}
		
		return result, nil
	}
	
	// For non-DML queries, execute directly
	return db.DB.ExecContext(ctx, query, args...)
}

// Query executes a query that returns rows
func (db *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return db.QueryContext(context.Background(), query, args...)
}

// QueryContext executes a query that returns rows
func (db *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	log.Debug("querying DB", "query", query)
	
	// Analyze the query for warnings only
	_, err := interceptor.AnalyzeQuery(query)
	if err != nil {
		log.Warn("failed to analyze query", "error", err)
		// Continue with execution even if analysis fails
	}
	
	// Execute the query directly
	return db.DB.QueryContext(ctx, query, args...)
}

// QueryRow executes a query that returns a single row
func (db *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	return db.QueryRowContext(context.Background(), query, args...)
}

// QueryRowContext executes a query that returns a single row
func (db *DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	log.Debug("querying DB for single row", "query", query)
	
	// Analyze the query for warnings only
	_, err := interceptor.AnalyzeQuery(query)
	if err != nil {
		log.Warn("failed to analyze query", "error", err)
		// Continue with execution even if analysis fails
	}
	
	// Execute the query directly
	return db.DB.QueryRowContext(ctx, query, args...)
}

// Close closes the database connection
func (db *DB) Close() error {
	log.Debug("closing database connection")
	return db.DB.Close()
}