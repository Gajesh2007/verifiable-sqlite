package replay

import (
	"testing"

	"github.com/gaj/verifiable-sqlite/pkg/config"
	"github.com/gaj/verifiable-sqlite/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestVerificationJobSubmission(t *testing.T) {
	// Create a test engine with small job queue and worker count
	cfg := config.Config{
		EnableVerification:   true,
		JobQueueSize:         5,
		WorkerCount:          1,
		VerificationTimeoutMs: 1000,
	}
	engine := NewEngine(cfg)

	// Test engine initial state
	assert.NotNil(t, engine)
	assert.Equal(t, 5, cap(engine.jobQueue))
	assert.Equal(t, 1, engine.workerCount)
	assert.False(t, engine.running)

	// Test starting the engine
	engine.Start()
	assert.True(t, engine.running)

	// Create a test job
	job := types.VerificationJob{
		TxID:            "test-tx-1",
		Query:           "INSERT INTO test (id, value) VALUES (1, 'test')",
		PreRootCaptured: "pre-root-hash",
		PostRootClaimed: "post-root-hash",
		TableSchemas: map[string]types.TableSchema{
			"test": {
				Name: "test",
				Columns: []types.ColumnInfo{
					{Name: "id", Type: "INTEGER", PrimaryKey: true},
					{Name: "value", Type: "TEXT"},
				},
				PrimaryKey: []string{"id"},
			},
		},
		PreStateData: map[string][]types.Row{},
		PostStateData: map[string][]types.Row{
			"test": {
				{
					RowID: "test-row-1",
					Values: map[string]types.Value{
						"id":    int64(1),
						"value": "test",
					},
				},
			},
		},
	}

	// Test job submission
	err := engine.SubmitJob(job)
	assert.NoError(t, err)

	// Stop the engine to clean up
	engine.Stop()
	assert.False(t, engine.running)
}

func TestSubmitJobToStoppedEngine(t *testing.T) {
	// Create a test engine that's not started
	cfg := config.DefaultConfig()
	engine := NewEngine(cfg)

	// Try to submit a job to a stopped engine
	job := types.VerificationJob{
		TxID:  "test-tx-1",
		Query: "INSERT INTO test (id) VALUES (1)",
	}

	err := engine.SubmitJob(job)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not running")
}

func TestJobQueueFull(t *testing.T) {
	// Create a test engine with very small job queue
	cfg := config.Config{
		EnableVerification: true,
		JobQueueSize:       1, // Only 1 job allowed
		WorkerCount:        1,
	}
	engine := NewEngine(cfg)
	engine.Start()
	defer engine.Stop()

	// Create jobs to fill and overflow the queue
	job1 := types.VerificationJob{TxID: "tx1", Query: "SELECT 1"}
	job2 := types.VerificationJob{TxID: "tx2", Query: "SELECT 2"}

	// Submit the first job (should succeed)
	err1 := engine.SubmitJob(job1)
	assert.NoError(t, err1)

	// Replace the engine's job channel with a blocked one to simulate a full queue
	engine.mu.Lock()
	blockedChan := make(chan types.VerificationJob, 1)
	blockedChan <- job1 // Fill the channel
	engine.jobQueue = blockedChan
	engine.mu.Unlock()

	// Try to submit another job (should fail)
	err2 := engine.SubmitJob(job2)
	assert.Error(t, err2)
	assert.Contains(t, err2.Error(), "job queue is full")
}

func TestFindMismatches(t *testing.T) {
	// Create test data
	claimed := map[string][]types.Row{
		"test": {
			{
				RowID: "row1",
				Values: map[string]types.Value{
					"id":    int64(1),
					"value": "test",
				},
			},
			{
				RowID: "row2",
				Values: map[string]types.Value{
					"id":    int64(2),
					"value": "test2",
				},
			},
		},
	}

	// Same data should have no mismatches
	calculated1 := claimed
	mismatches1 := findMismatches(claimed, calculated1)
	assert.Empty(t, mismatches1)

	// Missing table
	calculated2 := map[string][]types.Row{}
	mismatches2 := findMismatches(claimed, calculated2)
	assert.NotEmpty(t, mismatches2)
	assert.Contains(t, mismatches2[0], "table test missing")

	// Missing row
	calculated3 := map[string][]types.Row{
		"test": {
			{
				RowID: "row1",
				Values: map[string]types.Value{
					"id":    int64(1),
					"value": "test",
				},
			},
			// row2 is missing
		},
	}
	mismatches3 := findMismatches(claimed, calculated3)
	assert.NotEmpty(t, mismatches3)
	assert.Contains(t, mismatches3[0], "row row2 missing")

	// Value mismatch
	calculated4 := map[string][]types.Row{
		"test": {
			{
				RowID: "row1",
				Values: map[string]types.Value{
					"id":    int64(1),
					"value": "test",
				},
			},
			{
				RowID: "row2",
				Values: map[string]types.Value{
					"id":    int64(2),
					"value": "different", // Changed value
				},
			},
		},
	}
	mismatches4 := findMismatches(claimed, calculated4)
	assert.NotEmpty(t, mismatches4)
	assert.Contains(t, mismatches4[0], "value mismatch")
}

func TestParseQueryForCapture(t *testing.T) {
	// Test different query types
	tests := []struct {
		name        string
		query       string
		expectedType types.QueryType
	}{
		{
			name:        "select query",
			query:       "SELECT * FROM users",
			expectedType: types.QueryTypeSelect,
		},
		{
			name:        "insert query",
			query:       "INSERT INTO users (name) VALUES ('test')",
			expectedType: types.QueryTypeInsert,
		},
		{
			name:        "update query",
			query:       "UPDATE users SET name = 'test' WHERE id = 1",
			expectedType: types.QueryTypeUpdate,
		},
		{
			name:        "delete query",
			query:       "DELETE FROM users WHERE id = 1",
			expectedType: types.QueryTypeDelete,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			info, err := parseQueryForCapture(tc.query)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedType, info.Type)
			assert.Equal(t, tc.query, info.SQL)
		})
	}
}