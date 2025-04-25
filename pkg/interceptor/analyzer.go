package interceptor

import (
	"regexp"
	"strings"

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

// AnalyzeQuery parses and analyzes a SQL query to extract metadata
// This is a simplified version without using the Vitess parser
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

	// Simple query type detection
	sql = strings.TrimSpace(strings.ToLower(sql))
	
	if strings.HasPrefix(sql, "select") {
		info.Type = types.QueryTypeSelect
		// Extract table with simple regex
		tables := extractTablesWithRegex(sql)
		info.Tables = append(info.Tables, tables...)
	} else if strings.HasPrefix(sql, "insert") {
		info.Type = types.QueryTypeInsert
		// For INSERT, extract the table name after "INSERT INTO"
		re := regexp.MustCompile(`insert\s+into\s+([a-zA-Z0-9_]+)`)
		matches := re.FindStringSubmatch(sql)
		if len(matches) > 1 {
			info.Tables = append(info.Tables, matches[1])
		}
	} else if strings.HasPrefix(sql, "update") {
		info.Type = types.QueryTypeUpdate
		// For UPDATE, extract the table name after "UPDATE"
		re := regexp.MustCompile(`update\s+([a-zA-Z0-9_]+)`)
		matches := re.FindStringSubmatch(sql)
		if len(matches) > 1 {
			tableName := matches[1]
			info.Tables = append(info.Tables, tableName)
			
			// Extract primary key values from WHERE clause with regex
			// This is simplified and won't handle all cases
			whereRe := regexp.MustCompile(`where\s+([a-zA-Z0-9_]+)\s*=\s*['"]?([^'"\s)]+)['"]?`)
			whereMatches := whereRe.FindAllStringSubmatch(sql, -1)
			
			for _, match := range whereMatches {
				if len(match) > 2 {
					colName := match[1]
					value := match[2]
					
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
		}
	} else if strings.HasPrefix(sql, "delete") {
		info.Type = types.QueryTypeDelete
		// For DELETE, extract the table name after "FROM"
		re := regexp.MustCompile(`delete\s+from\s+([a-zA-Z0-9_]+)`)
		matches := re.FindStringSubmatch(sql)
		if len(matches) > 1 {
			tableName := matches[1]
			info.Tables = append(info.Tables, tableName)
			
			// Extract primary key values from WHERE clause with regex
			// This is simplified and won't handle all cases
			whereRe := regexp.MustCompile(`where\s+([a-zA-Z0-9_]+)\s*=\s*['"]?([^'"\s)]+)['"]?`)
			whereMatches := whereRe.FindAllStringSubmatch(sql, -1)
			
			for _, match := range whereMatches {
				if len(match) > 2 {
					colName := match[1]
					value := match[2]
					
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
		}
	}

	return info, nil
}

// extractTablesWithRegex extracts table names from a SQL query using regex
func extractTablesWithRegex(sql string) []string {
	var tables []string
	
	// This is a simplified approach that won't work for all cases
	// But it's good enough for basic queries in our example
	fromRe := regexp.MustCompile(`from\s+([a-zA-Z0-9_]+)`)
	joinRe := regexp.MustCompile(`join\s+([a-zA-Z0-9_]+)`)
	
	fromMatches := fromRe.FindAllStringSubmatch(sql, -1)
	for _, match := range fromMatches {
		if len(match) > 1 {
			tables = append(tables, match[1])
		}
	}
	
	joinMatches := joinRe.FindAllStringSubmatch(sql, -1)
	for _, match := range joinMatches {
		if len(match) > 1 {
			tables = append(tables, match[1])
		}
	}
	
	return tables
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
	
	// Additional pattern matching for common non-deterministic patterns
	if regexp.MustCompile(`datetime\s*\(\s*['"]now['"]`).MatchString(sql) {
		detected = append(detected, "datetime('now')")
	}
	
	return detected
}