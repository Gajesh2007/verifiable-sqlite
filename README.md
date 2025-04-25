# Verifiable SQLite

A Go library that provides a verifiable wrapper around SQLite's `database/sql` interface. The library intercepts database operations, captures state changes, and asynchronously verifies state transitions through deterministic replay.

## Overview

Verifiable SQLite adds a layer of integrity verification to your SQLite database operations by:

1. **Intercepting database operations**: Monitoring and analyzing DML queries (INSERT, UPDATE, DELETE)
2. **Capturing state changes**: Recording the state of affected rows before and after each operation
3. **Generating cryptographic commitments**: Creating Merkle roots representing database state
4. **Verifying state transitions**: Asynchronously replaying operations to confirm state changes

This V1 MVP focuses on internal verification-by-re-execution to ensure database operations are deterministic.

## V1 Architecture

The Verifiable SQLite system consists of the following key components:

1. **SQL Interceptor**: Wraps standard database/sql operations to intercept and analyze SQL queries.
2. **Query Analyzer**: Parses SQL AST to extract affected tables, identify query types, and detect primary key references.
3. **State Capture**: Records the state of affected rows before and after each operation.
4. **Merkle Commitment**: Generates cryptographic commitments (Merkle roots) for database state.
5. **Replay Engine**: Asynchronously replays operations in an isolated environment to verify state transitions.
6. **Metrics Collector**: Tracks performance metrics, verification results, and operational statistics.

### Transaction Flow

1. When a transaction begins, it is assigned a unique ID
2. During the transaction, DML operations are intercepted and aggregated
3. The state of affected rows is captured once before the first DML operation
4. When the transaction is committed:
   - The final state of affected rows is captured
   - A single verification job is created for the entire transaction
   - The transaction is committed to the database
   - The verification job is processed asynchronously

## V1 Limitations

The current MVP has several important limitations:

- **SELECT-based state capture**: Uses SELECT queries to capture state (less efficient than WAL-based approaches)
- **No external verification**: Only supports internal re-execution (no on-chain or external verification)
- **Tables without primary keys**: Verification is skipped for tables without primary keys
- **Non-deterministic functions**: Only warnings are issued for non-deterministic SQLite functions
- **Simple Query Protocol**: No support for advanced/extended query protocols
- **Race conditions**: Potential race conditions in high-concurrency environments
- **Performance overhead**: Additional overhead for state capture and verification

## Installation

```bash
go get github.com/gaj/verifiable-sqlite
```

## Usage

```go
package main

import (
    "context"
    "fmt"

    "github.com/gaj/verifiable-sqlite/pkg/config"
    "github.com/gaj/verifiable-sqlite/pkg/vsqlite"
    _ "github.com/mattn/go-sqlite3"
)

func main() {
    // Initialize the verification engine
    cfg := config.DefaultConfig()
    vsqlite.InitVerification(cfg)
    defer vsqlite.Shutdown()

    // Open a database connection
    db, err := vsqlite.Open("sqlite3", "file:test.db?cache=shared")
    if err != nil {
        panic(err)
    }
    defer db.Close()

    // Create a table
    _, err = db.Exec(`CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT UNIQUE,
        created_at DATETIME
    )`)
    if err != nil {
        panic(err)
    }

    // Begin a transaction
    ctx := context.Background()
    tx, err := db.BeginTx(ctx, nil)
    if err != nil {
        panic(err)
    }

    // Execute some DML operations
    _, err = tx.Exec("INSERT INTO users (name, email) VALUES (?, ?)",
        "John Doe", "john@example.com")
    if err != nil {
        tx.Rollback()
        panic(err)
    }

    // Commit the transaction (triggers verification)
    if err := tx.Commit(); err != nil {
        panic(err)
    }

    fmt.Println("Transaction committed and verification job submitted")
}
```

## Configuration

Verifiable SQLite can be configured through a YAML config file, environment variables, or directly in code:

```yaml
# config.yaml example
enable_verification: true
enable_warnings: true
enable_logging: true
job_queue_size: 100
worker_count: 4
verification_timeout_ms: 5000
metrics_server_enabled: true
metrics_server_addr: ":9090"
```

Environment variables (override config file):
```bash
export VSQLITE_ENABLE_VERIFICATION=true
export VSQLITE_WORKER_COUNT=8
```

## Monitoring

### Logs

Verifiable SQLite logs critical events that can be used to monitor verification:

1. **State Commitment Records**: Logged when a transaction is committed
   ```
   {"level":"info","timestamp":"2025-04-25T11:30:45.123Z","msg":"state commitment record","tx_id":"tx-123","pre_root":"hash1","post_root":"hash2","query":"TRANSACTION"}
   ```

2. **Verification Results**: Logged when verification completes
   ```
   {"level":"info","timestamp":"2025-04-25T11:30:45.456Z","msg":"verification result","tx_id":"tx-123","success":true,"duration_ms":15}
   ```

3. **Warnings**: Important warnings about skipped verification or non-deterministic functions
   ```
   {"level":"warn","timestamp":"2025-04-25T11:30:45.789Z","msg":"skipping verification for table without primary key","table":"logs"}
   ```

### Metrics

Metrics are exposed via HTTP endpoint when `metrics_server_enabled` is true:

```bash
curl http://localhost:9090/metrics
```

Key metrics include:
- Transaction counts (started, committed, rolled back)
- Query counts by type (SELECT, INSERT, UPDATE, DELETE)
- Verification results (success, failure)
- Performance timings (query execution, verification)
- Error counts

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for details.

## License

[MIT License](LICENSE)