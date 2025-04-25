package commitment

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/gaj/verifiable-sqlite/pkg/types"
)

// GenerateRowID creates a unique identifier for a database row
func GenerateRowID(tableName string, row map[string]types.Value, pkColumns []string) string {
	// If no primary key columns are provided, use all columns
	if len(pkColumns) == 0 {
		var allColumns []string
		for col := range row {
			allColumns = append(allColumns, col)
		}
		sort.Strings(allColumns)
		pkColumns = allColumns
	}

	// Sort the PK columns to ensure consistent ordering
	sort.Strings(pkColumns)

	// Create a map of just the PK values
	pkValues := make(map[string]types.Value)
	for _, col := range pkColumns {
		if val, ok := row[col]; ok {
			pkValues[col] = val
		}
	}

	// Build deterministic byte slice of "colName:serializedValue|" in the
	// *sorted* pkColumns order – avoids reliance on Go’s json map encoding.
	var buf []byte
	for _, col := range pkColumns {
		valBytes, err := serializeValue(pkValues[col])
		if err != nil {
			// If serialization fails we still fall back to fmt.Sprintf to avoid panic.
			valBytes = []byte(fmt.Sprintf("%v", pkValues[col]))
		}
		buf = append(buf, []byte(col)...)
		buf = append(buf, ':')
		buf = append(buf, valBytes...)
		buf = append(buf, '|')
	}

	// Hash with domain separation
	h := sha256.New()
	h.Write([]byte(DomainRowID))
	h.Write([]byte(tableName))
	h.Write([]byte(":"))
	h.Write(buf)

	return hex.EncodeToString(h.Sum(nil))
}

// serializeValue converts a value to a deterministic byte representation
func serializeValue(val types.Value) ([]byte, error) {
	if val == nil {
		return []byte("null"), nil
	}

	switch v := val.(type) {
	case int:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(v))
		return buf, nil
	case int64:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(v))
		return buf, nil
	case float64:
		buf := make([]byte, 8)
		bits := binary.BigEndian.Uint64(buf)
		binary.BigEndian.PutUint64(buf, bits)
		return buf, nil
	case bool:
		if v {
			return []byte{1}, nil
		}
		return []byte{0}, nil
	case string:
		return []byte(v), nil
	case []byte:
		return v, nil
	case time.Time:
		return []byte(v.UTC().Format(time.RFC3339Nano)), nil
	default:
		// For other types, use JSON serialization
		jsonData, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize value: %v", err)
		}
		return jsonData, nil
	}
}

// serializeRow converts a row to a deterministic byte representation
func serializeRow(row types.Row) ([]byte, error) {
	// Sort the columns to ensure consistent ordering
	var columns []string
	for col := range row.Values {
		columns = append(columns, col)
	}
	sort.Strings(columns)

	// Serialize each column value and join them
	var serialized []byte
	serialized = append(serialized, []byte(row.RowID)...)

	for _, col := range columns {
		val := row.Values[col]
		colBytes, err := serializeValue(val)
		if err != nil {
			return nil, err
		}
		
		// Add column name and value with separators
		serialized = append(serialized, []byte(col)...)
		serialized = append(serialized, ':')
		serialized = append(serialized, colBytes...)
		serialized = append(serialized, '|')
	}

	return serialized, nil
}

// GenerateTableRoot calculates a Merkle root for a table's rows
func GenerateTableRoot(tableName string, rows []types.Row) (string, error) {
	// Sort rows by RowID for deterministic ordering
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].RowID < rows[j].RowID
	})

	// Serialize each row and prepare leaves for Merkle tree
	var leaves [][]byte
	for _, row := range rows {
		rowBytes, err := serializeRow(row)
		if err != nil {
			return "", fmt.Errorf("failed to serialize row: %v", err)
		}
		
		// Create a leaf with table name and row data
		tableRowBytes := append([]byte(tableName+":"), rowBytes...)
		leaves = append(leaves, tableRowBytes)
	}

	// Build the Merkle tree and return the root
	rootNode := BuildMerkleTree(leaves)
	return MerkleRootHex(rootNode), nil
}

// GenerateDatabaseRoot calculates a Merkle root for the entire database
func GenerateDatabaseRoot(tableRoots map[string]string) string {
	// Sort table names for deterministic ordering
	var tableNames []string
	for tableName := range tableRoots {
		tableNames = append(tableNames, tableName)
	}
	sort.Strings(tableNames)

	// Create leaves for the database Merkle tree
	var leaves [][]byte
	for _, tableName := range tableNames {
		rootHex := tableRoots[tableName]
		tableRoot, _ := hex.DecodeString(rootHex) // Should never fail if roots are valid hex
		
		// Combine table name and its Merkle root
		tableLeaf := append([]byte(tableName+":"), tableRoot...)
		leaves = append(leaves, tableLeaf)
	}

	// Build the database-level Merkle tree
	dbRootNode := BuildMerkleTree(leaves)
	
	// Add the domain prefix for database roots and hash again
	dbRoot := MerkleRootHex(dbRootNode)
	dbBytes, _ := hex.DecodeString(dbRoot)
	finalHash := hashWithDomain(DomainDatabase, dbBytes)
	
	return hex.EncodeToString(finalHash)
}