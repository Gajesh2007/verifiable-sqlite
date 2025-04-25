package interceptor

import (
	"strings"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/gaj/verifiable-sqlite/pkg/log"
	"github.com/gaj/verifiable-sqlite/pkg/types"
)

// NonDeterministicFunctions is a list of SQLite functions that are non-deterministic
var NonDeterministicFunctions = []string{
	"random", 
	"datetime('now')", 
	"date('now')", 
	"time('now')", 
	"current_timestamp",
	"current_time",
	"current_date",
}

// AnalyzeQuery parses and analyzes a SQL query to extract metadata using AST parsing
func AnalyzeQuery(sql string) (types.QueryInfo, error) {
	info := types.QueryInfo{
		SQL:              sql,
		Type:             types.QueryTypeUnknown,
		Tables:           []string{},
		PKValues:         make(map[string][]types.Value),
		PKColumns:        make(map[string][]string),
		NonDeterministic: false,
	}

	// Check for non-deterministic functions
	nonDetFuncs := detectNonDeterministicFunctions(sql)
	if len(nonDetFuncs) > 0 {
		info.NonDeterministic = true
		log.LogNonDeterministicWarning(sql, nonDetFuncs)
	}

	// Parse the SQL query using vitess-sqlparser
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		// Some SQLite-specific queries might not parse with vitess
		// Fall back to basic type detection
		log.Debug("SQL parsing failed, fallback to basic detection", "error", err, "sql", sql)
		return fallbackBasicAnalysis(sql)
	}

	// Extract information from the parsed AST
	extractInfoFromAST(&info, stmt)

	return info, nil
}

// extractInfoFromAST extracts information from the parsed SQL AST
func extractInfoFromAST(info *types.QueryInfo, stmt sqlparser.Statement) {
    // placeholder index counter increments every time we visit a ValArg '?'.
    argCounter := 0
	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		info.Type = types.QueryTypeSelect
		extractTablesFromSelect(info, stmt)
	
	case *sqlparser.Insert:
		info.Type = types.QueryTypeInsert
		tableName := extractTableNameFromTableExpr(stmt.Table)
		info.Tables = append(info.Tables, tableName)
	
	case *sqlparser.Update:
		info.Type = types.QueryTypeUpdate
		// Update uses TableExprs (multiple table expressions)
		if len(stmt.TableExprs) > 0 {
			tableName := extractTableNameFromTableExpr(stmt.TableExprs[0])
			info.Tables = append(info.Tables, tableName)
			
			// Extract primary key values from WHERE clause
				if stmt.Where != nil {
					extractPKValuesFromWhere(info, tableName, stmt.Where.Expr, &argCounter)
				}
		}
	
	case *sqlparser.Delete:
		info.Type = types.QueryTypeDelete
		// Delete can have multiple tables in TableExprs
		if len(stmt.TableExprs) > 0 {
			tableName := extractTableNameFromTableExpr(stmt.TableExprs[0])
			info.Tables = append(info.Tables, tableName)
			
			// Extract primary key values from WHERE clause
				if stmt.Where != nil {
					extractPKValuesFromWhere(info, tableName, stmt.Where.Expr, &argCounter)
				}
		}
	
	default:
		// Other statement types like CREATE, DROP, etc.
		info.Type = types.QueryTypeOther
	}
}

// extractTablesFromSelect extracts table names from a SELECT statement
func extractTablesFromSelect(info *types.QueryInfo, stmt *sqlparser.Select) {
	// Extract tables from FROM clause
	extractTablesFromTableExpr(info, stmt.From)
}

// extractTablesFromTableExpr traverses table expressions to extract all table names
func extractTablesFromTableExpr(info *types.QueryInfo, tableExprs sqlparser.TableExprs) {
	for _, tableExpr := range tableExprs {
		switch expr := tableExpr.(type) {
		case *sqlparser.AliasedTableExpr:
			// Simple table reference, possibly with alias
			tableName := extractTableNameFromTableExpr(expr)
			if tableName != "" {
				info.Tables = append(info.Tables, tableName)
			}
		
		case *sqlparser.JoinTableExpr:
			// JOIN expression, recursively extract from left and right
			if expr.LeftExpr != nil {
				extractTablesFromTableExpr(info, sqlparser.TableExprs{expr.LeftExpr})
			}
			if expr.RightExpr != nil {
				extractTablesFromTableExpr(info, sqlparser.TableExprs{expr.RightExpr})
			}
		
		case *sqlparser.ParenTableExpr:
			// Parenthesized table expression, recursively extract
			extractTablesFromTableExpr(info, expr.Exprs)
		}
	}
}

// extractTableNameFromTableExpr extracts the actual table name from a table expression
func extractTableNameFromTableExpr(tableExpr sqlparser.SQLNode) string {
	switch expr := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		switch tableNode := expr.Expr.(type) {
		case sqlparser.TableName:
			return tableNode.Name.String()
		case *sqlparser.Subquery:
			// Subqueries don't have direct table names
			return ""
		}
	case sqlparser.TableName:
		return expr.Name.String()
	case *sqlparser.ParenTableExpr:
		// Parenthesized table expression, not a direct table name
		return ""
	case *sqlparser.JoinTableExpr:
		// JOIN doesn't have a direct table name
		return ""
	}
	return ""
}

// extractPKValuesFromWhere extracts potential primary key values from a WHERE clause
func extractPKValuesFromWhere(info *types.QueryInfo, tableName string, expr sqlparser.Expr, argCounter *int) {
	switch expr := expr.(type) {
	case *sqlparser.ComparisonExpr:
		// Check for column = value or column = ?
		if expr.Operator == "=" {
			colName := extractColumnName(expr.Left)
			if colName != "" {
					value := extractExprValue(expr.Right, argCounter)
				// Initialize PKColumns and PKValues maps if needed
				if info.PKColumns[tableName] == nil {
					info.PKColumns[tableName] = []string{}
				}
				if info.PKValues[tableName] == nil {
					info.PKValues[tableName] = []types.Value{}
				}
				
				info.PKColumns[tableName] = append(info.PKColumns[tableName], colName)
				info.PKValues[tableName] = append(info.PKValues[tableName], value)
			}
		}
	
	case *sqlparser.AndExpr:
		// Recursively extract from left and right of AND
			extractPKValuesFromWhere(info, tableName, expr.Left, argCounter)
			extractPKValuesFromWhere(info, tableName, expr.Right, argCounter)
	
	case *sqlparser.ParenExpr:
		// Recursively extract from parenthesized expression
			extractPKValuesFromWhere(info, tableName, expr.Expr, argCounter)
	
	// Other expression types like OR, IN, etc. not handled for PK extraction in V1
	// Can be extended in future versions
	}
}

// extractColumnName extracts a column name from an expression
func extractColumnName(expr sqlparser.Expr) string {
	switch expr := expr.(type) {
	case *sqlparser.ColName:
		return expr.Name.String()
	}
	return ""
}

// extractExprValue extracts a value from an expression
func extractExprValue(expr sqlparser.Expr, argCounter *int) types.Value {
    switch expr := expr.(type) {
    case *sqlparser.SQLVal:
        switch expr.Type {
        case sqlparser.StrVal:
            return string(expr.Val)
        case sqlparser.IntVal:
            return string(expr.Val)
        case sqlparser.FloatVal:
            return string(expr.Val)
        case sqlparser.ValArg:
            // '?' placeholder – record index and increment counter
            idx := *argCounter
            *argCounter = *argCounter + 1
            return types.ArgPlaceholder{Index: idx}
        }
    }
    return nil
}

// fallbackBasicAnalysis provides a basic SQL analysis when AST parsing fails
// This is mainly for SQLite-specific statements that the parser might not handle
func fallbackBasicAnalysis(sql string) (types.QueryInfo, error) {
	info := types.QueryInfo{
		SQL:              sql,
		Type:             types.QueryTypeUnknown,
		Tables:           []string{},
		PKValues:         make(map[string][]types.Value),
		PKColumns:        make(map[string][]string),
		NonDeterministic: false,
	}

	sql = strings.TrimSpace(strings.ToLower(sql))
	
	// Basic query type detection
	if strings.HasPrefix(sql, "select") {
		info.Type = types.QueryTypeSelect
	} else if strings.HasPrefix(sql, "insert") {
		info.Type = types.QueryTypeInsert
	} else if strings.HasPrefix(sql, "update") {
		info.Type = types.QueryTypeUpdate
	} else if strings.HasPrefix(sql, "delete") {
		info.Type = types.QueryTypeDelete
	} else if strings.HasPrefix(sql, "create") || 
		strings.HasPrefix(sql, "drop") || 
		strings.HasPrefix(sql, "alter") {
		info.Type = types.QueryTypeOther
	} else if strings.HasPrefix(sql, "pragma") || 
		strings.HasPrefix(sql, "begin") || 
		strings.HasPrefix(sql, "commit") || 
		strings.HasPrefix(sql, "rollback") {
		info.Type = types.QueryTypeOther
	}

	// Log that we're using fallback mode
	log.Warn("Using fallback SQL analysis mode", "sql", sql, "detected_type", info.Type)
	
	return info, nil
}

// detectNonDeterministicFunctions checks if the SQL contains non-deterministic functions
func detectNonDeterministicFunctions(sql string) []string {
	sql = strings.ToLower(sql)
	var detected []string
	
	for _, fn := range NonDeterministicFunctions {
		if strings.Contains(sql, strings.ToLower(fn)) {
			detected = append(detected, fn)
		}
	}
	
	return detected
}