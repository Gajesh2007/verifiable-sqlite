# Verifiable SQLite

A Go library that provides a verifiable wrapper around SQLite's `database/sql` interface. The library intercepts database operations, captures state changes, and asynchronously verifies state transitions through deterministic replay.

## Overview

Verifiable SQLite adds a layer of integrity verification to your SQLite database operations by:

1. **Intercepting database operations**: Monitoring and analyzing DML queries (INSERT, UPDATE, DELETE)
2. **Capturing state changes**: Recording the state of affected rows before and after each operation
3. **Generating cryptographic commitments**: Creating Merkle roots representing database state
4. **Verifying state transitions**: Asynchronously replaying operations to confirm state changes

This provides a foundation for database integrity that can be independently verified.

## Features

- Wraps the standard Go `database/sql` interface for SQLite
- Intercepts and analyzes SQL queries to detect affected tables and primary keys
- Captures row-level state changes within transactions
- Generates Merkle tree commitments for database state
- Asynchronously verifies state transitions through deterministic replay
- Warns about non-deterministic SQLite functions
- Supports all standard `database/sql` operations

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

    // Open a SQLite database
    db, err := vsqlite.Open("sqlite3", "example.db")
    if err != nil {
        panic(err)
    }
    defer db.Close()

    // Create a table
    _, err = db.Exec(`CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT UNIQUE
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

    // Insert data with verification
    result, err := tx.Exec("INSERT INTO users (name, email) VALUES (?, ?)",
        "John Doe", "john@example.com")
    if err != nil {
        tx.Rollback()
        panic(err)
    }

    // Commit the transaction
    if err := tx.Commit(); err != nil {
        panic(err)
    }

    fmt.Println("Data inserted and verified")
}
```

## How It Works

1. When a DML operation is executed within a transaction, the library:
   - Analyzes the query to identify affected tables and primary keys
   - Captures the pre-state of affected rows using SELECTs within the transaction
   - Executes the original query
   - Captures the post-state of affected rows
   - Generates Merkle roots for the pre and post states
   - Logs the state commitment record
   - Submits a verification job to the replay engine

2. The replay engine asynchronously:
   - Creates an isolated in-memory SQLite database
   - Restores the pre-state
   - Replays the DML operation
   - Captures the post-state
   - Verifies that the resulting state matches the claimed state
   - Logs the verification result

## Configuration

The library can be configured through the `config.Config` struct:

```go
cfg := config.Config{
    EnableVerification:    true,  // Enable/disable verification
    EnableWarnings:        true,  // Enable/disable warnings for non-deterministic functions
    EnableLogging:         true,  // Enable/disable logging
    JobQueueSize:          100,   // Size of verification job queue
    WorkerCount:           4,     // Number of worker goroutines
    VerificationTimeoutMs: 5000,  // Timeout for verification jobs (5 seconds)
}
vsqlite.InitVerification(cfg)
```

## Limitations (V1)

- Only supports simple DML queries (complex SQL, subqueries, CTEs not fully supported)
- State capture is SELECT-based and can be inefficient for large tables
- Tables without primary keys have limited support
- Non-deterministic functions are detected but not rewritten
- Only provides internal verification (no public proof generation)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.