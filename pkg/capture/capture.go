package capture

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/gaj/verifiable-sqlite/pkg/commitment"
	"github.com/gaj/verifiable-sqlite/pkg/errors"
	"github.com/gaj/verifiable-sqlite/pkg/log"
	"github.com/gaj/verifiable-sqlite/pkg/types"
	"github.com/jackc/pgx/v5"
)

// schemaCache caches table schemas to avoid redundant queries
var (
	schemaCache      = make(map[string]types.TableSchema)
	schemaCacheMutex sync.RWMutex
)

// getTableSchemas retrieves schemas for the specified tables
func getTableSchemas(ctx context.Context, executor types.DBExecutor, tableNames []string) (map[string]types.TableSchema, error) {
	schemas := make(map[string]types.TableSchema)

	for _, tableName := range tableNames {
		// Check cache first
		schemaCacheMutex.RLock()
		schema, found := schemaCache[tableName]
		schemaCacheMutex.RUnlock()

		if found {
			schemas[tableName] = schema
			continue
		}

		// Get table info from SQLite schema
		schema, err := getTableSchema(ctx, executor, tableName)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get schema for table %s", tableName)
		}

		// Cache the schema
		schemaCacheMutex.Lock()
		schemaCache[tableName] = schema
		schemaCacheMutex.Unlock()

		schemas[tableName] = schema
	}

	return schemas, nil
}

// getTableSchema retrieves schema for a single table
func getTableSchema(ctx context.Context, executor types.DBExecutor, tableName string) (types.TableSchema, error) {
	schema := types.TableSchema{
		Name:    tableName,
		Columns: []types.ColumnInfo{},
	}

	// Get column info using pragma_table_info
	rows, err := executor.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", tableName))
	if err != nil {
		return schema, errors.Wrap(err, "failed to get column info")
	}
	defer rows.Close()

	for rows.Next() {
		var cid int
		var name string
		var typeName string
		var notNull int
		var defaultValue sql.NullString
		var pk int

		if err := rows.Scan(&cid, &name, &typeName, &notNull, &defaultValue, &pk); err != nil {
			return schema, errors.Wrap(err, "failed to scan row")
		}

		columnInfo := types.ColumnInfo{
			Name:       name,
			Type:       typeName,
			NotNull:    notNull == 1,
			PrimaryKey: pk > 0,
		}

		schema.Columns = append(schema.Columns, columnInfo)

		// Add to primary key list if this is a PK column
		if pk > 0 {
			schema.PrimaryKey = append(schema.PrimaryKey, name)
		}
	}

	if err = rows.Err(); err != nil {
		return schema, errors.Wrap(err, "failed to read rows")
	}

	// If no explicit primary key, look for rowid
	if len(schema.PrimaryKey) == 0 {
		// First, check if this is a WITHOUT ROWID table
		withoutRowidQuery, err := executor.QueryContext(ctx, fmt.Sprintf("SELECT sql FROM sqlite_master WHERE type='table' AND name='%s'", tableName))
		if err == nil {
			defer withoutRowidQuery.Close()
			if withoutRowidQuery.Next() {
				var tableSql string
				if err := withoutRowidQuery.Scan(&tableSql); err == nil {
					// If the table was created WITH WITHOUT ROWID, we can't use rowid as a PK
					if strings.Contains(strings.ToUpper(tableSql), "WITHOUT ROWID") {
						log.Debug("table created WITHOUT ROWID, skipping rowid as PK", "table", tableName)
						return schema, nil
					}
				}
			}
		}
		
		// Check if this is a regular rowid table (most SQLite tables are)
		// pragma_table_xinfo shows hidden columns like rowid
		rowsQuery, err := executor.QueryContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM pragma_table_xinfo('%s') WHERE name = 'rowid'", tableName))
		if err == nil {
			defer rowsQuery.Close()
			if rowsQuery.Next() {
				var hasRowid int
				if err := rowsQuery.Scan(&hasRowid); err == nil && hasRowid > 0 {
					log.Debug("using implicit rowid as primary key for table without explicit PK", "table", tableName)
					schema.PrimaryKey = []string{"rowid"}
					
					// Add rowid as a column to the schema since it doesn't appear in normal PRAGMA table_info
					schema.Columns = append(schema.Columns, types.ColumnInfo{
						Name:       "rowid",
						Type:       "INTEGER",
						NotNull:    true,
						PrimaryKey: true,
					})
				} else {
					log.Warn("table has no primary key and is not a standard rowid table", "table", tableName)
				}
			}
		} else {
			log.Warn("failed to check rowid status", "table", tableName, "error", err)
		}
	}

	return schema, nil
}

// CapturePreStateInTx captures the state of tables affected by a query before
// executing the query. It returns the captured state, table schemas, and a
// Merkle root of the pre-state.
func CapturePreStateInTx(ctx context.Context, tx *sql.Tx, queryInfo types.QueryInfo, args []interface{}) (map[string][]types.Row, map[string]types.TableSchema, string, error) {
	preState := make(map[string][]types.Row)
	schemas := make(map[string]types.TableSchema)

	// Extract PK values from the query's WHERE clause to optimize state capture
	if len(queryInfo.Tables) == 0 {
		log.Debug("no tables affected by query", "sql", queryInfo.SQL)
		return preState, schemas, "", nil
	}

	// Synchronization for concurrent schema lookups and state captures
	var wg sync.WaitGroup
	schemasMutex := &sync.Mutex{}
	preStateMutex := &sync.Mutex{}
	errChan := make(chan error, len(queryInfo.Tables))

	for _, table := range queryInfo.Tables {
		wg.Add(1)
		go func(tableName string) {
			defer wg.Done()

			// Get table schema (including PK information)
			schema, err := getTableSchema(ctx, tx, tableName)
			if err != nil {
				errChan <- errors.WrapStateCaptureError(err, tableName)
				return
			}

			// Skip tables without PKs - verification is not supported for these
			if len(schema.PrimaryKey) == 0 {
				log.Warn("skipping state capture for table without primary key",
					"table", tableName)
				return
			}

			schemasMutex.Lock()
			schemas[tableName] = schema
			schemasMutex.Unlock()

			// Check if we have PK values to capture rows
			if pkValues, ok := queryInfo.PKValues[tableName]; ok && len(pkValues) > 0 {
				// Capture specific rows that will be affected
				rows, err := captureRowsByPK(ctx, tx, tableName, schema, pkValues, args)
				if err != nil {
					errChan <- errors.Wrapf(err, "failed to capture rows by PK for table %s", tableName)
					return
				}

				preStateMutex.Lock()
				preState[tableName] = rows
				preStateMutex.Unlock()

				log.Debug("captured pre-state by PK",
					"table", tableName,
					"row_count", len(rows),
					"pk_values", pkValues)
			} else {
				// Fallback to capturing all rows in the table
				log.Warn("falling back to full table capture for pre-state",
					"table", tableName,
					"query_type", queryInfo.Type,
					"sql", queryInfo.SQL)

				rows, err := captureAllRows(ctx, tx, tableName, schema)
				if err != nil {
					errChan <- errors.Wrapf(err, "failed to capture all rows for table %s", tableName)
					return
				}

				preStateMutex.Lock()
				preState[tableName] = rows
				preStateMutex.Unlock()

				log.Debug("captured all rows for pre-state",
					"table", tableName,
					"row_count", len(rows))
			}
		}(table)
	}

	// Wait for all goroutines to finish
	wg.Wait()
	close(errChan)

	// Check for errors
	var errs []string
	for err := range errChan {
		errs = append(errs, err.Error())
	}

	if len(errs) > 0 {
		// Join all errors into a single error message
		return nil, nil, "", errors.Wrap(errors.ErrStateCapture, strings.Join(errs, "; "))
	}

	// Check if we have an empty pre-state (which is valid if no tables with PKs)
	if len(preState) == 0 {
		log.Warn("no pre-state captured", "tables", queryInfo.Tables)
		return preState, schemas, "", nil
	}

	// Generate a Merkle root commitment of the pre-state
	tableRoots := make(map[string]string)
	for tableName, rows := range preState {
		tableRoot, err := commitment.GenerateTableRoot(tableName, rows)
		if err != nil {
			return nil, nil, "", errors.Wrapf(err, "failed to generate table root for %s", tableName)
		}
		tableRoots[tableName] = tableRoot
	}

	// Generate overall database root
	dbRoot := commitment.GenerateDatabaseRoot(tableRoots)

	return preState, schemas, dbRoot, nil
}

// CapturePostStateInTx captures the state of affected rows after executing a query
func CapturePostStateInTx(ctx context.Context, tx *sql.Tx, queryInfo types.QueryInfo, result sql.Result, preState map[string][]types.Row, schemas map[string]types.TableSchema, args []interface{}) (map[string][]types.Row, string, error) {
	if len(queryInfo.Tables) == 0 {
		return nil, "", errors.Wrap(errors.ErrStateCapture, "no tables identified in query")
	}

	// Capture state for each affected table
	postState := make(map[string][]types.Row)
	tableRoots := make(map[string]string)

	// Synchronization for concurrent state captures
	var wg sync.WaitGroup
	postStateMutex := &sync.Mutex{}
	errChan := make(chan error, len(queryInfo.Tables))

	for _, tableName := range queryInfo.Tables {
		wg.Add(1)
		go func(tableName string) {
			defer wg.Done()

			schema, found := schemas[tableName]
			if !found {
				errChan <- errors.Newf("schema not found for table %s", tableName)
				return
			}

			// Skip tables without a primary key for V1
			if len(schema.PrimaryKey) == 0 {
				log.Warn("skipping verification for table without primary key",
					"table", tableName,
					"query_type", queryInfo.Type)
				return
			}

			// Determine which rows to capture based on query type and PK values
			var rows []types.Row
			var captureErr error

			switch queryInfo.Type {
			case types.QueryTypeInsert:
				// For INSERT, capture the newly inserted row using last_insert_rowid()
				lastID, err := result.LastInsertId()
				if err != nil {
					log.Warn("failed to get last insert ID, falling back to full table capture",
						"table", tableName,
						"error", err)
					
					rows, captureErr = captureAllRows(ctx, tx, tableName, schema)
				} else {
					log.Debug("capturing inserted row by ID",
						"table", tableName,
						"last_insert_id", lastID)
					
					rows, captureErr = captureRowsByPK(ctx, tx, tableName, schema, []types.Value{lastID}, args)
				}

			case types.QueryTypeUpdate:
				// For UPDATE, recapture the same rows as in pre-state
				pkValues, hasPKs := queryInfo.PKValues[tableName]

				if hasPKs && len(pkValues) > 0 {
					log.Debug("capturing updated rows by PK",
						"table", tableName,
						"pk_count", len(pkValues))
					
					rows, captureErr = captureRowsByPK(ctx, tx, tableName, schema, pkValues, args)
				} else {
					log.Warn("falling back to full table capture for post-state",
						"table", tableName,
						"query_type", queryInfo.Type,
						"sql", queryInfo.SQL)
					
					rows, captureErr = captureAllRows(ctx, tx, tableName, schema)
				}

			case types.QueryTypeDelete:
				// For DELETE, we should find rows that no longer exist
				pkValues, hasPKs := queryInfo.PKValues[tableName]

				if hasPKs && len(pkValues) > 0 {
					log.Debug("verifying deleted rows by PK",
						"table", tableName,
						"pk_count", len(pkValues))
					
					rows, captureErr = captureRowsByPK(ctx, tx, tableName, schema, pkValues, args)
				} else {
					log.Warn("falling back to full table capture for deleted rows",
						"table", tableName,
						"query_type", queryInfo.Type,
						"sql", queryInfo.SQL)
					
					rows, captureErr = captureAllRows(ctx, tx, tableName, schema)
				}

			case types.QueryTypeSelect:
				// For SELECT, we don't need to capture anything
				log.Debug("skipping post-state capture for SELECT", "table", tableName)
				return

			default:
				log.Warn("unsupported query type for state capture",
					"query_type", queryInfo.Type,
					"table", tableName)
				return
			}

			if captureErr != nil {
				errChan <- errors.Wrapf(captureErr, "failed to capture post-state for table %s", tableName)
				return
			}

			// Store captured rows in post-state
			postStateMutex.Lock()
			postState[tableName] = rows
			postStateMutex.Unlock()

			// Generate Merkle root for this table
			tableRoot, err := commitment.GenerateTableRoot(tableName, rows)
			if err != nil {
				errChan <- errors.Wrapf(err, "failed to generate table root for %s", tableName)
				return
			}

			// Store table root
			postStateMutex.Lock()
			tableRoots[tableName] = tableRoot
			postStateMutex.Unlock()

			log.Debug("captured post-state",
				"table", tableName,
				"row_count", len(rows))
		}(tableName)
	}

	// Wait for all goroutines to finish
	wg.Wait()
	close(errChan)

	// Check for errors
	var errs []string
	for err := range errChan {
		errs = append(errs, err.Error())
	}

	if len(errs) > 0 {
		// Join all errors into a single error message
		return nil, "", errors.Wrap(errors.ErrStateCapture, strings.Join(errs, "; "))
	}

	// Generate overall database root
	dbRoot := commitment.GenerateDatabaseRoot(tableRoots)

	return postState, dbRoot, nil
}

// CapturePostStateWithPgx captures the state of affected rows after executing a query using pgx
func CapturePostStateWithPgx(ctx context.Context, queryInfo types.QueryInfo, tx pgx.Tx, result sql.Result, preState map[string][]types.Row, schemas map[string]types.TableSchema, args []interface{}) (map[string][]types.Row, string, error) {
	if len(queryInfo.Tables) == 0 {
		return nil, "", errors.New("no tables identified in query")
	}

	// Capture state for each affected table
	state := make(map[string][]types.Row)
	tableRoots := make(map[string]string)

	for _, tableName := range queryInfo.Tables {
		schema, found := schemas[tableName]
		if !found {
			return nil, "", errors.Newf("schema not found for table %s", tableName)
		}

		// Skip tables without a primary key for simplicity in V1
		if len(schema.PrimaryKey) == 0 {
			log.Warn("skipping verification for table without primary key - consider adding a primary key for V1 verification support", 
				"table", tableName,
				"query_type", queryInfo.Type)
			continue
		}

		// Determine which rows to capture based on query type and PK values
		var rows []types.Row
		switch queryInfo.Type {
		case types.QueryTypeInsert:
			// For INSERT, capture the newly inserted row using last_insert_rowid()
			lastID, err := result.LastInsertId()
			if err != nil {
				log.Warn("failed to get last insert ID", "error", err)
				// Fall back to capturing all rows
				capturedRows, err := captureAllRowsWithPgx(ctx, tx, tableName, schema)
				if err != nil {
					return nil, "", errors.Wrapf(err, "failed to capture all rows for table %s", tableName)
				}
				rows = capturedRows
			} else {
				capturedRows, err := captureRowsByPKWithPgx(ctx, tx, tableName, schema, []types.Value{lastID}, args)
				if err != nil {
					return nil, "", errors.Wrapf(err, "failed to capture row by ID for table %s", tableName)
				}
				rows = capturedRows
			}

		case types.QueryTypeUpdate:
			// For UPDATE, recapture the same rows as in pre-state
			pkValues, hasPKs := queryInfo.PKValues[tableName]

			if hasPKs && len(pkValues) > 0 {
				// We have PK values from the WHERE clause
				capturedRows, err := captureRowsByPKWithPgx(ctx, tx, tableName, schema, pkValues, args)
				if err != nil {
					return nil, "", errors.Wrapf(err, "failed to capture rows by PK for table %s", tableName)
				}
				rows = capturedRows
			} else {
				// Fall back to capturing all rows
				log.Warn("falling back to full table capture - WHERE clause could not be parsed for PK values", 
					"table", tableName, 
					"query_type", queryInfo.Type, 
					"sql", queryInfo.SQL)
				capturedRows, err := captureAllRowsWithPgx(ctx, tx, tableName, schema)
				if err != nil {
					return nil, "", errors.Wrapf(err, "failed to capture all rows for table %s", tableName)
				}
				rows = capturedRows
			}

		case types.QueryTypeDelete:
			// For DELETE, we should find rows that no longer exist
			pkValues, hasPKs := queryInfo.PKValues[tableName]

			if hasPKs && len(pkValues) > 0 {
				capturedRows, err := captureRowsByPKWithPgx(ctx, tx, tableName, schema, pkValues, args)
				if err != nil {
					return nil, "", errors.Wrapf(err, "failed to capture rows by PK for table %s", tableName)
				}
				rows = capturedRows
			} else {
				capturedRows, err := captureAllRowsWithPgx(ctx, tx, tableName, schema)
				if err != nil {
					return nil, "", errors.Wrapf(err, "failed to capture all rows for table %s", tableName)
				}
				rows = capturedRows
			}

		case types.QueryTypeSelect:
			// For SELECT, we don't need to capture anything
			continue

		default:
			log.Warn("unsupported query type for state capture", "query_type", queryInfo.Type)
			continue
		}

		state[tableName] = rows

		// Generate Merkle root for this table
		tableRoot, err := commitment.GenerateTableRoot(tableName, rows)
		if err != nil {
			return nil, "", errors.Wrapf(err, "failed to generate table root for %s", tableName)
		}
		tableRoots[tableName] = tableRoot
	}

	// Generate overall database root
	dbRoot := commitment.GenerateDatabaseRoot(tableRoots)

	return state, dbRoot, nil
}

// captureRowsByPK captures specific rows based on primary key values
func captureRowsByPK(ctx context.Context, tx *sql.Tx, tableName string, schema types.TableSchema, pkValues []types.Value, queryArgs []interface{}) ([]types.Row, error) {
	// Build the query to select rows by primary key
	var rows []types.Row

	// Get primary key column or columns
	var pkColumns []string
	if len(schema.PrimaryKey) > 0 {
		pkColumns = schema.PrimaryKey
	} else {
		// If no PK defined, use rowid as fallback
		pkColumns = []string{"rowid"}
		log.Warn("table has no defined primary key, using rowid", "table", tableName)
	}

	// For multi-column PKs, the query would need to be much more complex
	// For V1, we'll use a simpler approach with a single PK column
	pkColumn := pkColumns[0]

	// Create the WHERE clause with placeholders
	placeholders := make([]string, len(pkValues))
	for i := range pkValues {
		placeholders[i] = "?"
	}

	query := fmt.Sprintf("SELECT * FROM %s WHERE %s IN (%s)",
		tableName, pkColumn, strings.Join(placeholders, ","))

	// Convert pkValues (resolving placeholders) into []interface{} for QueryContext
	resolvedArgs := make([]interface{}, len(pkValues))
	for i, v := range pkValues {
		if ph, ok := v.(types.ArgPlaceholder); ok {
			if ph.Index < len(queryArgs) {
				resolvedArgs[i] = queryArgs[ph.Index]
			} else {
				resolvedArgs[i] = nil // placeholder index out of range – treat as nil
			}
		} else {
			resolvedArgs[i] = v
		}
	}

	// Execute the query
	resultRows, err := tx.QueryContext(ctx, query, resolvedArgs...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute query")
	}
	defer resultRows.Close()

	// Process the results
	columns, err := resultRows.Columns()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get columns")
	}

	for resultRows.Next() {
		// Scan into a slice of interfaces
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := resultRows.Scan(valuePtrs...); err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}

		// Convert to a map of column name to value
		rowValues := make(map[string]types.Value)
		for i, col := range columns {
			rowValues[col] = values[i]
		}

		// Create a Row with a unique ID
		rowID := commitment.GenerateRowID(tableName, rowValues, schema.PrimaryKey)
		row := types.Row{
			RowID:  rowID,
			Values: rowValues,
		}

		rows = append(rows, row)
	}

	if err = resultRows.Err(); err != nil {
		return nil, errors.Wrap(err, "failed to read rows")
	}

	return rows, nil
}

// captureAllRows captures all rows from a table
func captureAllRows(ctx context.Context, tx *sql.Tx, tableName string, schema types.TableSchema) ([]types.Row, error) {
	query := fmt.Sprintf("SELECT * FROM %s", tableName)

	// Execute the query
	resultRows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute query")
	}
	defer resultRows.Close()

	// Process the results
	columns, err := resultRows.Columns()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get columns")
	}

	var rows []types.Row
	for resultRows.Next() {
		// Scan into a slice of interfaces
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := resultRows.Scan(valuePtrs...); err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}

		// Convert to a map of column name to value
		rowValues := make(map[string]types.Value)
		for i, col := range columns {
			rowValues[col] = values[i]
		}

		// Create a Row with a unique ID
		rowID := commitment.GenerateRowID(tableName, rowValues, schema.PrimaryKey)
		row := types.Row{
			RowID:  rowID,
			Values: rowValues,
		}

		rows = append(rows, row)
	}

	if err = resultRows.Err(); err != nil {
		return nil, errors.Wrap(err, "failed to read rows")
	}

	return rows, nil
}

// captureRowsByPKWithPgx captures specific rows based on primary key values using pgx
func captureRowsByPKWithPgx(ctx context.Context, tx pgx.Tx, tableName string, schema types.TableSchema, pkValues []types.Value, queryArgs []interface{}) ([]types.Row, error) {
	var rows []types.Row

	// Build column list for SELECT
	var columnNames []string
	for _, col := range schema.Columns {
		columnNames = append(columnNames, fmt.Sprintf("\"%s\"", col.Name))
	}

	// Map of column name -> column index for quick lookup
	colIndices := make(map[string]int)
	for i, col := range schema.Columns {
		colIndices[col.Name] = i
	}

	// Determine which PK column to use
	primaryKey := schema.PrimaryKey[0] // For V1, we're using the first PK column for simplicity

    // Build the WHERE clause for the primary key values
    whereConditions := make([]string, len(pkValues))
    resolvedArgs := make([]interface{}, len(pkValues))

    for i, val := range pkValues {
        whereConditions[i] = fmt.Sprintf("\"%s\" = $%d", primaryKey, i+1)
        if ph, ok := val.(types.ArgPlaceholder); ok {
            if ph.Index < len(queryArgs) {
                resolvedArgs[i] = queryArgs[ph.Index]
            } else {
                resolvedArgs[i] = nil
            }
        } else {
            resolvedArgs[i] = val
        }
    }

	// Build the complete query
	query := fmt.Sprintf("SELECT %s FROM \"%s\" WHERE %s",
		strings.Join(columnNames, ", "),
		tableName,
		strings.Join(whereConditions, " OR "))

	// Execute the query
    pgxRows, err := tx.Query(ctx, query, resolvedArgs...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute query")
	}
	defer pgxRows.Close()

	// Process the rows
	for pgxRows.Next() {
		// Prepare destination slice for the scan
		scanDest := make([]interface{}, len(schema.Columns))
		for i := range scanDest {
			scanDest[i] = new(interface{})
		}

		// Scan the row data
		if err := pgxRows.Scan(scanDest...); err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}

		// Create a Row object with column values
		row := types.Row{
			Values: make(map[string]types.Value),
		}

		// Extract values from scan destinations
		for i, col := range schema.Columns {
			val := scanDest[i].(*interface{})
			if *val != nil {
				row.Values[col.Name] = *val
			}
		}

		// Generate a row ID from primary key values
		// Create a map that contains only the PK values for this row
		pkMap := make(map[string]types.Value)
		for _, pkCol := range schema.PrimaryKey {
			idx, exists := colIndices[pkCol]
			if !exists {
				return nil, errors.Newf("primary key column %s not found in result set", pkCol)
			}
			valPtr := scanDest[idx].(*interface{})
			if *valPtr != nil {
				pkMap[pkCol] = *valPtr
			}
		}

		// Generate the row ID using the correct function signature
		rowID := commitment.GenerateRowID(tableName, pkMap, schema.PrimaryKey)
		row.RowID = rowID

		rows = append(rows, row)
	}

	if err := pgxRows.Err(); err != nil {
		return nil, errors.Wrap(err, "failed to read rows")
	}

	return rows, nil
}

// captureAllRowsWithPgx captures all rows from a table using pgx
func captureAllRowsWithPgx(ctx context.Context, tx pgx.Tx, tableName string, schema types.TableSchema) ([]types.Row, error) {
	var rows []types.Row

	// Build column list for SELECT
	var columnNames []string
	for _, col := range schema.Columns {
		columnNames = append(columnNames, fmt.Sprintf("\"%s\"", col.Name))
	}

	// Map of column name -> column index for quick lookup
	colIndices := make(map[string]int)
	for i, col := range schema.Columns {
		colIndices[col.Name] = i
	}

	// Build the complete query
	query := fmt.Sprintf("SELECT %s FROM \"%s\"",
		strings.Join(columnNames, ", "),
		tableName)

	// Execute the query
	pgxRows, err := tx.Query(ctx, query)
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute query")
	}
	defer pgxRows.Close()

	// Process the rows
	for pgxRows.Next() {
		// Prepare destination slice for the scan
		scanDest := make([]interface{}, len(schema.Columns))
		for i := range scanDest {
			scanDest[i] = new(interface{})
		}

		// Scan the row data
		if err := pgxRows.Scan(scanDest...); err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}

		// Create a Row object
		row := types.Row{
			Values: make(map[string]types.Value),
		}

		// Extract values from scan destinations
		for i, col := range schema.Columns {
			val := scanDest[i].(*interface{})
			if *val != nil {
				row.Values[col.Name] = *val
			}
		}

		// Generate a row ID from primary key values
		if len(schema.PrimaryKey) == 0 {
			return nil, errors.Newf("table %s has no primary key", tableName)
		}

		// Create a map that contains only the PK values for this row
		pkMap := make(map[string]types.Value)
		for _, pkCol := range schema.PrimaryKey {
			idx, exists := colIndices[pkCol]
			if !exists {
				return nil, errors.Newf("primary key column %s not found in result set", pkCol)
			}
			valPtr := scanDest[idx].(*interface{})
			if *valPtr != nil {
				pkMap[pkCol] = *valPtr
			}
		}

		// Generate the row ID using the correct function signature
		rowID := commitment.GenerateRowID(tableName, pkMap, schema.PrimaryKey)
		row.RowID = rowID

		rows = append(rows, row)
	}

	if err := pgxRows.Err(); err != nil {
		return nil, errors.Wrap(err, "failed to read rows")
	}

	return rows, nil
}
