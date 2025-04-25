package commitment

import (
	"testing"
	"time"

	"github.com/gaj/verifiable-sqlite/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestGenerateRowID(t *testing.T) {
	// Test generating row IDs with primary keys
	tableName := "users"
	rowData := map[string]types.Value{
		"id":    int64(1),
		"name":  "John",
		"email": "john@example.com",
	}
	pkColumns := []string{"id"}

	rowID := GenerateRowID(tableName, rowData, pkColumns)
	assert.NotEmpty(t, rowID)

	// Same inputs should produce same ID
	rowID2 := GenerateRowID(tableName, rowData, pkColumns)
	assert.Equal(t, rowID, rowID2)

	// Different PK value should produce different ID
	rowData2 := map[string]types.Value{
		"id":    int64(2),
		"name":  "John",  // Same non-PK value
		"email": "john@example.com", // Same non-PK value
	}
	rowID3 := GenerateRowID(tableName, rowData2, pkColumns)
	assert.NotEqual(t, rowID, rowID3)

	// Multi-column PK
	multiPkColumns := []string{"id", "email"}
	multiPkRowID := GenerateRowID(tableName, rowData, multiPkColumns)
	assert.NotEmpty(t, multiPkRowID)
	assert.NotEqual(t, rowID, multiPkRowID) // Different PK selection should change ID

	// Test with empty PK columns (should use all columns)
	noPkRowID := GenerateRowID(tableName, rowData, []string{})
	assert.NotEmpty(t, noPkRowID)
}

func TestSerializeValue(t *testing.T) {
	tests := []struct {
		name     string
		value    types.Value
		wantErr  bool
		wantNull bool
	}{
		{"nil", nil, false, true},
		{"int", 123, false, false},
		{"int64", int64(123), false, false},
		{"float64", float64(123.45), false, false},
		{"bool_true", true, false, false},
		{"bool_false", false, false, false},
		{"string", "test", false, false},
		{"bytes", []byte("test"), false, false},
		{"time", time.Now(), false, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := serializeValue(tc.value)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if tc.wantNull {
					assert.Equal(t, "null", string(result))
				} else {
					assert.NotEmpty(t, result)
				}
			}
		})
	}
}

func TestGenerateTableRoot(t *testing.T) {
	tableName := "users"
	rows := []types.Row{
		{
			RowID: "row1",
			Values: map[string]types.Value{
				"id":    int64(1),
				"name":  "John",
				"email": "john@example.com",
			},
		},
		{
			RowID: "row2",
			Values: map[string]types.Value{
				"id":    int64(2),
				"name":  "Jane",
				"email": "jane@example.com",
			},
		},
	}

	// Generate table root
	root, err := GenerateTableRoot(tableName, rows)
	assert.NoError(t, err)
	assert.NotEmpty(t, root)

	// Same inputs should produce same root
	root2, err := GenerateTableRoot(tableName, rows)
	assert.NoError(t, err)
	assert.Equal(t, root, root2)

	// Different table name should produce different root
	diffTableRoot, err := GenerateTableRoot("accounts", rows)
	assert.NoError(t, err)
	assert.NotEqual(t, root, diffTableRoot)

	// Different data should produce different root
	diffRows := []types.Row{
		{
			RowID: "row1",
			Values: map[string]types.Value{
				"id":    int64(1),
				"name":  "John",
				"email": "different@example.com", // Changed value
			},
		},
		{
			RowID: "row2",
			Values: map[string]types.Value{
				"id":    int64(2),
				"name":  "Jane",
				"email": "jane@example.com",
			},
		},
	}
	diffDataRoot, err := GenerateTableRoot(tableName, diffRows)
	assert.NoError(t, err)
	assert.NotEqual(t, root, diffDataRoot)
}

func TestGenerateDatabaseRoot(t *testing.T) {
	// Test with multiple tables
	tableRoots := map[string]string{
		"users":    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"products": "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210",
	}

	// Generate DB root
	dbRoot := GenerateDatabaseRoot(tableRoots)
	assert.NotEmpty(t, dbRoot)

	// Same inputs should produce same root
	dbRoot2 := GenerateDatabaseRoot(tableRoots)
	assert.Equal(t, dbRoot, dbRoot2)

	// Different table roots should produce different DB root
	diffTableRoots := map[string]string{
		"users":    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"products": "0000000000000000000000000000000000000000000000000000000000000000", // Changed
	}
	diffDbRoot := GenerateDatabaseRoot(diffTableRoots)
	assert.NotEqual(t, dbRoot, diffDbRoot)

	// Test with empty table roots
	emptyDbRoot := GenerateDatabaseRoot(map[string]string{})
	assert.NotEmpty(t, emptyDbRoot)
}