package replay

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	
	"github.com/gaj/verifiable-sqlite/pkg/log"
	"github.com/gaj/verifiable-sqlite/pkg/types"
	"github.com/jackc/pgx/v5"
	_ "github.com/mattn/go-sqlite3"
)

// SetupVerificationDB creates a temporary SQLite database for verification
func SetupVerificationDB(ctx context.Context, schemas map[string]types.TableSchema, preState map[string][]types.Row) (*sql.DB, error) {
	// Create an in-memory SQLite database
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, fmt.Errorf("failed to create in-memory SQLite DB: %v", err)
	}

	// Set isolation level and other pragmas for deterministic execution
	if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to set foreign_keys pragma: %v", err)
	}

	// Create tables in the schema
	for _, schema := range schemas {
		if err := createTable(ctx, db, schema); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to create table %s: %v", schema.Name, err)
		}
	}

	// Populate with pre-state data
	for tableName, rows := range preState {
		schema, ok := schemas[tableName]
		if !ok {
			db.Close()
			return nil, fmt.Errorf("schema not found for table %s", tableName)
		}

		if err := populateTable(ctx, db, schema, rows); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to populate table %s: %v", tableName, err)
		}
	}

	return db, nil
}

// SetupVerificationDBWithPgx creates a temporary SQLite database for verification using pgx
func SetupVerificationDBWithPgx(ctx context.Context, tx pgx.Tx, schemas map[string]types.TableSchema, preState map[string][]types.Row) error {
	// Create tables in the schema
	for _, schema := range schemas {
		if err := createTableWithPgx(ctx, tx, schema); err != nil {
			return fmt.Errorf("failed to create table %s: %v", schema.Name, err)
		}
	}

	// Populate with pre-state data
	for tableName, rows := range preState {
		schema, ok := schemas[tableName]
		if !ok {
			return fmt.Errorf("schema not found for table %s", tableName)
		}

		if err := populateTableWithPgx(ctx, tx, schema, rows); err != nil {
			return fmt.Errorf("failed to populate table %s: %v", tableName, err)
		}
	}

	return nil
}

// createTable creates a table in the verification database
func createTable(ctx context.Context, db *sql.DB, schema types.TableSchema) error {
	// Build the CREATE TABLE statement
	var columns []string
	
	for _, col := range schema.Columns {
		colDef := fmt.Sprintf("\"%s\" %s", col.Name, col.Type)
		
		if col.NotNull {
			colDef += " NOT NULL"
		}
		
		if col.PrimaryKey {
			colDef += " PRIMARY KEY"
		}
		
		columns = append(columns, colDef)
	}
	
	// If there's a multi-column primary key, add it as a constraint
	if len(schema.PrimaryKey) > 1 {
		// Quote each pk col
		quoted := make([]string, len(schema.PrimaryKey))
		for i, c := range schema.PrimaryKey {
			quoted[i] = fmt.Sprintf("\"%s\"", c)
		}
		columns = append(columns, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(quoted, ", ")))
	}
	
	createSQL := fmt.Sprintf("CREATE TABLE \"%s\" (%s)", schema.Name, strings.Join(columns, ", "))
	
	// Execute the CREATE TABLE
	_, err := db.ExecContext(ctx, createSQL)
	return err
}

// createTableWithPgx creates a table in the verification database using pgx
func createTableWithPgx(ctx context.Context, tx pgx.Tx, schema types.TableSchema) error {
	// Build the CREATE TABLE statement
	var columns []string
	
	for _, col := range schema.Columns {
		colDef := fmt.Sprintf("\"%s\" %s", col.Name, col.Type)
		
		if col.NotNull {
			colDef += " NOT NULL"
		}
		
		if col.PrimaryKey {
			colDef += " PRIMARY KEY"
		}
		
		columns = append(columns, colDef)
	}
	
	// If there's a multi-column primary key, add it as a constraint
	if len(schema.PrimaryKey) > 1 {
		pkCols := make([]string, len(schema.PrimaryKey))
		for i, col := range schema.PrimaryKey {
			pkCols[i] = fmt.Sprintf("\"%s\"", col)
		}
		columns = append(columns, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(pkCols, ", ")))
	}
	
	createSQL := fmt.Sprintf("CREATE TABLE \"%s\" (%s)", schema.Name, strings.Join(columns, ", "))
	
	// Execute the CREATE TABLE
	_, err := tx.Exec(ctx, createSQL)
	return err
}

// populateTable populates a table with pre-state data
func populateTable(ctx context.Context, db *sql.DB, schema types.TableSchema, rows []types.Row) error {
	if len(rows) == 0 {
		return nil // Nothing to populate
	}
	
	// Begin a transaction for bulk insert
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()
	
	// Prepare column names for INSERT
	var columnNames []string
	for _, col := range schema.Columns {
		columnNames = append(columnNames, fmt.Sprintf("\"%s\"", col.Name))
	}
	
	// Create placeholders for prepared statement
	placeholders := make([]string, len(columnNames))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	
	// Prepare the INSERT statement
	insertSQL := fmt.Sprintf("INSERT INTO \"%s\" (%s) VALUES (%s)",
		schema.Name,
		strings.Join(columnNames, ", "),
		strings.Join(placeholders, ", "))
	
	stmt, err := tx.PrepareContext(ctx, insertSQL)
	if err != nil {
		return err
	}
	defer stmt.Close()
	
	// Insert each row
	for _, row := range rows {
		var values []interface{}
		for _, col := range schema.Columns {
			val, ok := row.Values[col.Name]
			if !ok {
				values = append(values, nil) // NULL for missing values
			} else {
				values = append(values, val)
			}
		}
		
		if _, err := stmt.ExecContext(ctx, values...); err != nil {
			return err
		}
	}
	
	return tx.Commit()
}

// populateTableWithPgx populates a table with pre-state data using pgx
func populateTableWithPgx(ctx context.Context, tx pgx.Tx, schema types.TableSchema, rows []types.Row) error {
	if len(rows) == 0 {
		return nil // Nothing to populate
	}
	
	// Prepare column names for INSERT
	var columnNames []string
	for _, col := range schema.Columns {
		columnNames = append(columnNames, fmt.Sprintf("\"%s\"", col.Name))
	}
	
	// Create placeholders for prepared statement
	placeholders := make([]string, len(columnNames))
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	
	// Prepare the INSERT statement
	insertSQL := fmt.Sprintf("INSERT INTO \"%s\" (%s) VALUES (%s)",
		schema.Name, 
		strings.Join(columnNames, ", "),
		strings.Join(placeholders, ", "))
	
	// Insert each row
	for _, row := range rows {
		var values []interface{}
		for _, col := range schema.Columns {
			val, ok := row.Values[col.Name]
			if !ok {
				values = append(values, nil) // NULL for missing values
			} else {
				values = append(values, val)
			}
		}
		
		if _, err := tx.Exec(ctx, insertSQL, values...); err != nil {
			return err
		}
	}
	
	return nil
}

// ExecuteQueriesDeterministically executes SQL queries in a deterministic manner
func ExecuteQueriesDeterministically(ctx context.Context, db *sql.DB, query string, args ...interface{}) (*sql.Tx, error) {
	// Begin a transaction for the query execution
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %v", err)
	}

	// Set transaction level pragmas for deterministic execution
	if _, err := tx.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		tx.Rollback()
		return nil, fmt.Errorf("failed to set foreign_keys pragma: %v", err)
	}

	// Execute the query with the provided arguments
	log.Debug("executing query in verification", "query", query)
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		tx.Rollback()
		return nil, fmt.Errorf("failed to execute query: %v", err)
	}

	// Return the transaction for state capture and commit
	return tx, nil
}

// ExecuteQueriesDeterministicallyWithPgx executes SQL queries in a deterministic manner using pgx
func ExecuteQueriesDeterministicallyWithPgx(ctx context.Context, tx pgx.Tx, query string, args ...interface{}) error {
	log.Debug("executing query in verification with pgx", "query", query)
	_, err := tx.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to execute query with pgx: %v", err)
	}
	return nil
}

// SetDeterministicSessionParams sets SQLite session parameters for deterministic execution
func SetDeterministicSessionParams(ctx context.Context, tx *sql.Tx) error {
	// Set pragmas for deterministic execution
	pragmas := []string{
		"PRAGMA foreign_keys = ON",
		"PRAGMA temp_store = MEMORY",
		"PRAGMA synchronous = OFF",
	}
	
	for _, pragma := range pragmas {
		if _, err := tx.ExecContext(ctx, pragma); err != nil {
			return fmt.Errorf("failed to set pragma %s: %v", pragma, err)
		}
	}
	
	return nil
}

// SetDeterministicSessionParamsWithPgx sets SQLite session parameters for deterministic execution using pgx
func SetDeterministicSessionParamsWithPgx(ctx context.Context, tx pgx.Tx) error {
	// Set pragmas for deterministic execution
	pragmas := []string{
		"PRAGMA foreign_keys = ON",
		"PRAGMA temp_store = MEMORY",
		"PRAGMA synchronous = OFF",
	}
	
	for _, pragma := range pragmas {
		if _, err := tx.Exec(ctx, pragma); err != nil {
			return fmt.Errorf("failed to set pragma %s: %v", pragma, err)
		}
	}
	
	return nil
}