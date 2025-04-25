package replay

import (
	"testing"
	"time"

	"github.com/gaj/verifiable-sqlite/pkg/config"
	"github.com/gaj/verifiable-sqlite/pkg/metrics"
	"github.com/gaj/verifiable-sqlite/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)


func TestVerificationJobSubmission(t *testing.T) {
	// Create a test engine with small job queue and worker count
	cfg := config.Config{
		EnableVerification:   true,
		JobQueueSize:         5,
		WorkerCount:          1,
		VerificationTimeoutMs: 1000,
	}
	engine := NewEngine(
		cfg.JobQueueSize, 
		cfg.WorkerCount,
		WithTimeout(time.Duration(cfg.VerificationTimeoutMs) * time.Millisecond),
	)
	engine.SetMetrics(metrics.NewMetrics())
	
	// Test engine initial state
	assert.NotNil(t, engine)
	assert.Equal(t, 5, cap(engine.jobQueue))
	assert.Equal(t, 1, engine.workerCount)
	assert.False(t, engine.running)

	// No external connection pool is required for the current implementation.

	// Test starting the engine
	err := engine.Start()
	require.NoError(t, err)
	assert.True(t, engine.running)

	// Create a test job
	job := types.VerificationJob{
		TxID:            "test-tx-1",
		Query:           "INSERT INTO test (id, value) VALUES (1, 'test')",
		SQL:             "INSERT INTO test (id, value) VALUES (1, 'test')",
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
						"id":    1,
						"value": "test",
					},
				},
			},
		},
	}

	// Submit the job
	err = engine.SubmitJob(job)
	require.NoError(t, err)

	// Allow time for job processing
	time.Sleep(100 * time.Millisecond)

	// Stop the engine
	engine.Stop()
	assert.False(t, engine.running)
}

func TestSubmitJobToStoppedEngine(t *testing.T) {
	// Create a test engine but don't start it
	cfg := config.DefaultConfig()
	engine := NewEngine(
		cfg.JobQueueSize,
		cfg.WorkerCount,
		WithTimeout(time.Duration(cfg.VerificationTimeoutMs) * time.Millisecond),
	)

	// Create a test job
	job := types.VerificationJob{
		TxID: "test-tx-2",
	}

	// Try to submit a job to the stopped engine
	err := engine.SubmitJob(job)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not running")
}

func TestJobQueueFull(t *testing.T) {
	// Create a test engine with very small queue
	cfg := config.Config{
		EnableVerification:   true,
		JobQueueSize:         1, // Only room for 1 job
		WorkerCount:          1,
		VerificationTimeoutMs: 5000,
	}
	engine := NewEngine(
		cfg.JobQueueSize,
		cfg.WorkerCount,
		WithTimeout(time.Duration(cfg.VerificationTimeoutMs) * time.Millisecond),
	)
	// Use a channel that's deliberately slow to process
	engine.jobQueue = make(chan types.VerificationJob, 1)
	engine.SetMetrics(metrics.NewMetrics())

	// Start the engine
	err := engine.Start()
	require.NoError(t, err)

	// Create a test job that will fill the queue
	job1 := types.VerificationJob{TxID: "test-tx-3"}
	job2 := types.VerificationJob{TxID: "test-tx-4"}

	// Submit one job to fill the queue
	err = engine.SubmitJob(job1)
	assert.NoError(t, err)

	// Submit another job which should fail because queue is full
	err = engine.SubmitJob(job2)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "job queue is full")

	// Stop the engine
	engine.Stop()
}

func TestFindMismatches(t *testing.T) {
	// Create test states
	claimed := map[string][]types.Row{
		"test": {
			{
				RowID: "row1",
				Values: map[string]types.Value{
					"id":    1,
					"value": "test1",
				},
			},
			{
				RowID: "row2",
				Values: map[string]types.Value{
					"id":    2,
					"value": "test2",
				},
			},
		},
	}

	// Test case 1: Missing table in calculated state
	calculated1 := map[string][]types.Row{}
	mismatches1 := findMismatches(claimed, calculated1)
	assert.Len(t, mismatches1, 1)
	assert.Contains(t, mismatches1[0], "table 'test' present in claimed state but missing in calculated state")

	// Test case 2: Row count mismatch
	calculated2 := map[string][]types.Row{
		"test": {
			{
				RowID: "row1",
				Values: map[string]types.Value{
					"id":    1,
					"value": "test1",
				},
			},
		},
	}
	mismatches2 := findMismatches(claimed, calculated2)
	assert.Len(t, mismatches2, 1)
	assert.Contains(t, mismatches2[0], "row count mismatch for table 'test'")

	// Test case 3: Value mismatch
	calculated3 := map[string][]types.Row{
		"test": {
			{
				RowID: "row1",
				Values: map[string]types.Value{
					"id":    1,
					"value": "different",
				},
			},
			{
				RowID: "row2",
				Values: map[string]types.Value{
					"id":    2,
					"value": "test2",
				},
			},
		},
	}
	mismatches3 := findMismatches(claimed, calculated3)
	assert.Len(t, mismatches3, 1)
	assert.Contains(t, mismatches3[0], "value mismatch")

	// Test case 4: No mismatch (identical states)
	calculated4 := map[string][]types.Row{
		"test": {
			{
				RowID: "row1",
				Values: map[string]types.Value{
					"id":    1,
					"value": "test1",
				},
			},
			{
				RowID: "row2",
				Values: map[string]types.Value{
					"id":    2,
					"value": "test2",
				},
			},
		},
	}
	mismatches4 := findMismatches(claimed, calculated4)
	assert.Len(t, mismatches4, 0)
}

func TestParseQueryForCapture(t *testing.T) {
	// Test valid queries
	testCases := []struct {
		sql     string
		wantErr bool
		wantType string
	}{
		{
			sql:     "SELECT * FROM users WHERE id = 1",
			wantErr: false,
			wantType: "SELECT",
		},
		{
			sql:     "INSERT INTO users (name, email) VALUES ('Test', 'test@example.com')",
			wantErr: false,
			wantType: "INSERT",
		},
		{
			sql:     "UPDATE users SET name = 'New Name' WHERE id = 1",
			wantErr: false,
			wantType: "UPDATE",
		},
		{
			sql:     "DELETE FROM users WHERE id = 1",
			wantErr: false,
			wantType: "DELETE",
		},
		{
			sql:     "INVALID SQL QUERY",
			wantErr: false,
			wantType: "UNKNOWN",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.sql, func(t *testing.T) {
			info, err := parseQueryForCapture(tc.sql)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.wantType, string(info.Type))
			}
		})
	}
}