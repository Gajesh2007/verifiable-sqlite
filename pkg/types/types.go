package types

import (
	"context"
	"database/sql"
	"time"
)

// Value represents a database value with type information
// For V1, we use interface{} but this will need stricter typing later
type Value interface{}

// ColumnInfo describes a column in a database table
type ColumnInfo struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	PrimaryKey bool `json:"primary_key"`
	NotNull   bool `json:"not_null"`
}

// TableSchema represents the schema of a database table
type TableSchema struct {
	Name       string       `json:"name"`
	Columns    []ColumnInfo `json:"columns"`
	PrimaryKey []string     `json:"primary_key"` // Names of primary key columns
}

// Row represents a database row with its unique identifier
type Row struct {
	RowID  string            `json:"row_id"`   // Canonically generated row identifier
	Values map[string]Value  `json:"values"`   // Column name -> value mapping
}

// RowID is a unique identifier for a row across tables
type RowID string

// QueryType represents the type of SQL query
type QueryType string

const (
	QueryTypeUnknown QueryType = "UNKNOWN"
	QueryTypeSelect  QueryType = "SELECT"
	QueryTypeInsert  QueryType = "INSERT"
	QueryTypeUpdate  QueryType = "UPDATE"
	QueryTypeDelete  QueryType = "DELETE"
	QueryTypeOther   QueryType = "OTHER"    // For DDL, PRAGMA, etc.
)

// String returns the string representation of QueryType
func (qt QueryType) String() string {
	return string(qt)
}

// QueryInfo contains metadata about a parsed SQL query
type QueryInfo struct {
	SQL            string            `json:"sql"`
	Type           QueryType         `json:"type"`
	Tables         []string          `json:"tables"`
	PKValues       map[string][]Value `json:"pk_values"` // Table -> PK values
	PKColumns      map[string][]string `json:"pk_columns"` // Table -> PK column names (in same order as values)
	NonDeterministic bool            `json:"non_deterministic"`
}

// ArgPlaceholder represents a positional placeholder ("?") in a SQL statement
// captured during query analysis.  The Index corresponds to the element in the
// args slice that will be supplied at execution time.
type ArgPlaceholder struct {
    Index int
}

// StateCommitmentRecord represents a record of a state transition with cryptographic commitments
type StateCommitmentRecord struct {
	TxID            string    `json:"tx_id"`
	Query           string    `json:"query"`
	SQL             string    `json:"sql"`  // Actual SQL executed
	Timestamp       time.Time `json:"timestamp"`
	PreRootCaptured string    `json:"pre_root_captured"`
	PostRootClaimed string    `json:"post_root_claimed"`
	PerformanceMs   int64     `json:"performance_ms"`  // Execution time in milliseconds
}

// VerificationJob contains all data needed to asynchronously verify a state transition
type VerificationJob struct {
	TxID            string                   `json:"tx_id"`
	Query           string                   `json:"query"`     // Query type/description
	SQLSequence     []string                 `json:"sql_sequence"` // Sequence of SQL statements to execute
	ArgsSequence    [][]interface{}          `json:"args_sequence"` // Sequence of arguments for each SQL statement
	SQL             string                   `json:"sql"`       // Actual SQL to execute (legacy field)
	Args            []interface{}            `json:"args"`      // Query arguments (legacy field)
	PreStateData    map[string][]Row         `json:"pre_state_data"`    // Table -> Rows
	PostStateData   map[string][]Row         `json:"post_state_data"`   // Table -> Rows
	TableSchemas    map[string]TableSchema   `json:"table_schemas"`     // Table -> Schema
	PreRootCaptured string                   `json:"pre_root_captured"`
	PostRootClaimed string                   `json:"post_root_claimed"`
}

// VerificationResult contains the outcome of a verification job
type VerificationResult struct {
	TxID              string    `json:"tx_id"`
	Query             string    `json:"query"`
	Success           bool      `json:"success"`
	Timestamp         time.Time `json:"timestamp"`
	PreRootCaptured   string    `json:"pre_root_captured"`
	PostRootClaimed   string    `json:"post_root_claimed"`
	PostRootCalculated string   `json:"post_root_calculated"`
	Error             string    `json:"error,omitempty"`
	Mismatches        []string  `json:"mismatches,omitempty"`
	Duration          time.Duration `json:"duration"`
	QueryInfo         QueryInfo `json:"query_info,omitempty"`
}

// DBExecutor is an interface that can execute database queries
// This allows us to use either *sql.DB or *sql.Tx
type DBExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}