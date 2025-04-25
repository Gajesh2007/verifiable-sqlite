package replay

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	
	"github.com/gaj/verifiable-sqlite/pkg/log"
	"github.com/gaj/verifiable-sqlite/pkg/types"
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

// createTable creates a table in the verification database
func createTable(ctx context.Context, db *sql.DB, schema types.TableSchema) error {
	// Build the CREATE TABLE statement
	var columns []string
	
	for _, col := range schema.Columns {
		colDef := fmt.Sprintf("%s %s", col.Name, col.Type)
		
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
		columns = append(columns, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(schema.PrimaryKey, ", ")))
	}
	
	createSQL := fmt.Sprintf("CREATE TABLE %s (%s)", schema.Name, strings.Join(columns, ", "))
	
	// Execute the CREATE TABLE
	_, err := db.ExecContext(ctx, createSQL)
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
		columnNames = append(columnNames, col.Name)
	}
	
	// Create placeholders for prepared statement
	placeholders := make([]string, len(columnNames))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	
	// Prepare the INSERT statement
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
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
		for _, colName := range columnNames {
			val, ok := row.Values[colName]
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