**Tech Spec: Verifiable SQLite Wrapper (Go) - V1 MVP (Verification-by-Re-execution)**

**1. Introduction & Goals**

1.1.  **Project:** Verifiable SQLite - Go Wrapper V1
1.2.  **Goal:** To create a Go library that wraps the standard `database/sql` interface for SQLite. This wrapper will intercept database operations (primarily DML), capture relevant pre/post state information *within the transaction*, calculate cryptographic state commitments (Merkle roots), and trigger an asynchronous deterministic replay process to internally verify the state transition.
1.3.  **Core Principle (V1):** Database integrity is verified internally by the library's asynchronous re-execution mechanism. The library intercepts user operations, claims a resulting state hash, and then separately re-executes the operation in a controlled environment to confirm its own claim. Trust is derived from observing successful internal verification logs.
1.4.  **V1 Scope (MVP):**
    *   Implement a Go library (`pkg/vsqlite`) providing a wrapper around `database/sql` specifically for use with an SQLite driver (e.g., `mattn/go-sqlite3`).
    *   The wrapper intercepts `DB.ExecContext`, `DB.QueryContext`, `DB.BeginTx`, `Tx.ExecContext`, `Tx.QueryContext`, `Tx.Commit`, `Tx.Rollback`.
    *   Parse simple DML queries (INSERT, UPDATE, DELETE) using a robust Go SQL parser (`vitess-sqlparser`) to identify the target table and attempt to extract primary key values from WHERE clauses (for UPDATE/DELETE).
    *   Detect and **log warnings** for known non-deterministic SQL function usage (`random()`, `datetime('now')`, etc.). No rewriting or blocking in V1.
    *   Implement **in-transaction state capture**: Before and after executing user DML within a transaction, run `SELECT` statements *within the same transaction* against the user's SQLite DB file to capture the state of affected rows (identified via PKs where possible, otherwise potentially falling back to full table capture for the specific table as a simpler V1 approach).
    *   Generate table Merkle roots and an overall database Merkle root (`PreRootCaptured`, `PostRootClaimed`) based on captured state.
    *   **Wrapper Workflow (for DML in Tx):**
        1.  Intercept `tx.ExecContext(sql)`.
        2.  Analyze `sql` to get `QueryInfo` (type, table, PKs from WHERE if possible).
        3.  Call internal `CapturePreStateInTx(tx, queryInfo)` -> Runs SELECTs, generates `PreRootCaptured`, returns `PreStateData`.
        4.  Execute the user's original DML `sql` using `tx.ExecContext`. Handle errors.
        5.  Call internal `CapturePostStateInTx(tx, queryInfo)` -> Runs SELECTs, generates `PostRootClaimed`, returns `PostStateData`.
        6.  Log `StateCommitmentRecord { TxID, Query, PreRootCaptured, PostRootClaimed }`.
        7.  Dispatch `VerificationJob { TxID, Query, PreStateData, PostStateData, PreRootCaptured, PostRootClaimed, Schema }` asynchronously via channel.
    *   **Replay Engine (Asynchronous Goroutine Pool):**
        1.  Receive `VerificationJob`.
        2.  Create a **temporary, isolated SQLite database** environment (e.g., copy the DB file corresponding to the *pre-state*, or create an in-memory DB and populate from `job.PreStateData`).
        3.  Connect to the temporary DB.
        4.  Start a transaction in the temporary DB.
        5.  Set deterministic parameters (e.g., PRAGMAs if applicable).
        6.  Re-execute `job.Query`.
        7.  Capture the actual post-state using the *same* state capture logic (`CapturePostStateInTx`) against the temporary DB's transaction.
        8.  Calculate `PostRootCalculated`.
        9.  Compare `PostRootCalculated` vs `job.PostRootClaimed`.
        10. Log `VerificationResult { TxID, Success: bool, ..., Mismatches: Option<...> }`.
        11. **Clean up** the temporary SQLite environment (close connection, delete temporary file or let in-memory DB go out of scope).
    *   **Out of Scope for V1:** Query rewriting, complex SQL analysis (subqueries, CTEs), trigger handling, explicit support for tables without PKs (will warn/skip), WAL-based capture, on-chain components, public verification APIs, high concurrency optimizations.

1.5.  **Success Criteria (V1):**
    *   Applications using the wrapper library can perform basic CRUD operations against an SQLite database.
    *   The wrapper logs `StateCommitmentRecord` for DML operations executed via `ExecContext`.
    *   The wrapper logs warnings for detected non-deterministic functions.
    *   The Replay Engine asynchronously processes jobs using temporary SQLite databases.
    *   The Replay Engine logs `VerificationResult` correctly indicating success (`true`) when claimed and calculated post-roots match, and failure (`false`) when they don't.
    *   The system functions correctly for single-threaded application use.

**2. High-Level Architecture (Go - SQLite Wrapper)**

```
+-------------------+      +-----------------------------------+      +-----------------+
|    Application    |----->| VerifiableSQLite Wrapper Library  |----->| SQLite Driver   |
| (Uses Wrapper DB) |      | (Wraps database/sql)              |      | (mattn/go-sqlite3)|
+-------------------+      +-----------------------------------+      +-----------------+
                              | Intercepts Calls     |         |      | Reads/Writes
                              |                      v         |      v
                       +------------------+<------>+-------+---------+
                       | Query Interceptor|        | State |         | SQLite DB File  |
                       | (SQL Parser)     |------->| Capture         | (User's Data)   |
                       +------------------+        | (In-Tx SELECTs) |                 |
                              |                    +-------+---------+                 |
                              | Calculates Roots           | Logs Commitment         |
                              v                            v                         v
+--------------------------+-----------------+   +-----------------+     +-------------------+
| Commitment Generator     |                 |   | Logging Service |     | Job Queue         |
| (Merkle Tree Logic)      |-------------------->| (log/slog)      |---->| (e.g., Channel)   |
+--------------------------+-----------------+   +-----------------+     +-------------------+
                                                                               | Receive Job Async
                                                                               v
                                                   +---------------------------------------------------+
                                                   | Replay Engine (Async Workers)                     |
                                                   | ┌───────────────────────────────────────────────┐ |
                                                   | │ Worker:                                       │ |
                                                   | │ 1. Create Temp/Isolated SQLite DB             │ |
                                                   | │ 2. Restore Pre-State                          │ |
                                                   | │ 3. Replay Query (Deterministic)               │ |
                                                   | │ 4. Capture Post-State (using State Capture)   │ |
                                                   | │ 5. Calculate PostRoot (using Commit Gen)      │ |
                                                   | │ 6. Compare Roots                              │ |
                                                   | │ 7. Log VerificationResult                     │ |
                                                   | │ 8. Cleanup Temp DB                            │ |
                                                   | └───────────────────────────────────────────────┘ |
                                                   +---------------------------------------------------+
```

**3. Component Breakdown & Sequencing (Go)**

**(Sequence 1: Foundation - ~15% Effort)**

3.1.  **Project Setup & Core Types (`pkg/vsqlite/vsqlite.go`, `pkg/types/types.go`, `pkg/config/config.go`, `pkg/log/log.go`)**
    *   **Action:** Setup Go module. Define core `types` structs (as previously listed, ensuring `Value` handles SQLite types appropriately - consider mapping to Go standard types `string`, `int64`, `float64`, `[]byte`, `time.Time`, `bool`). Define `Config` (simpler: maybe just feature flags for V1, as DB path is passed on `Open`). Setup `slog`.
    *   **Instructions:** Define `types.RowID` canonical generation early.

3.2.  **Commitment Generation Logic (`pkg/commitment/merkle.go`, `pkg/commitment/generator.go`)**
    *   **Action:** Implement `hashWithDomain`, `BuildMerkleTree`, `GenerateTableRoot`, `GenerateDatabaseRoot`, `GenerateRowID` (hash-based PKs).
    *   **Instructions:** Same as Postgres spec, ensure deterministic serialization handles SQLite types correctly. Add unit tests.

**(Sequence 2: Wrapper & Interception - ~30% Effort)**

3.3.  **Verifiable SQLite Wrapper (`pkg/vsqlite/db.go`, `pkg/vsqlite/tx.go`)**
    *   **Action:** Create `vsqlite.DB` and `vsqlite.Tx` structs that embed `*sql.DB` and `*sql.Tx` respectively. Implement methods like `Open(driverName, dataSourceName)`, `BeginTx`, `Commit`, `Rollback`, `ExecContext`, `QueryContext` for both DB and Tx.
    *   **Instructions:** The wrapped methods should:
        *   Log the call (debug level).
        *   For `ExecContext` (on DB or Tx): Call the interceptor. If it's DML, trigger the pre/post capture sequence. Delegate the actual execution to the embedded `sql.DB` or `sql.Tx`.
        *   For `QueryContext`: Call the interceptor (mainly for logging/warnings in V1). Delegate to embedded object.
        *   For `BeginTx`: Call `db.BeginTx`, wrap the result in `vsqlite.Tx`, and update internal transaction state.
        *   For `Commit`/`Rollback`: Update internal state, delegate to embedded `sql.Tx`.

3.4.  **Query Interceptor (`pkg/interceptor/analyzer.go`)**
    *   **Action:** Implement `AnalyzeQuery(sql string)` using `vitess-sqlparser`. Extract `QueryType` (simple DML/SELECT), `AffectedTables`, detect non-deterministic SQLite functions (e.g., `random()`, `datetime('now')`), and populate `types.QueryInfo`. Implement logic to extract primary key values from simple WHERE clauses (e.g., `WHERE id = ?` or `WHERE key1 = ? AND key2 = ?`).
    *   **Instructions:** Focus on reliable table name extraction and basic WHERE clause parsing for PK identification needed by state capture. Log warnings on detection.

**(Sequence 3: In-Transaction State Capture - ~25% Effort)**

3.5.  **State Capture Service (`pkg/capture/capture.go`)**
    *   **Action:** Implement `CapturePreStateInTx(ctx, tx *sql.Tx, queryInfo)` and `CapturePostStateInTx(ctx, tx *sql.Tx, queryInfo)`. Also implement `getTableSchemas(ctx, executor dbExecutor, tableNames [])`.
    *   **Instructions:**
        *   `getTableSchemas` uses `tx.QueryContext` to query `sqlite_master` or `pragma_table_info` and `pragma_index_list`/`pragma_index_info` to get column names, types, and primary keys for the affected tables. Cache schema info per connection/transaction if possible.
        *   `CapturePre/PostStateInTx` must:
            *   Use the *passed-in transaction* `tx`.
            *   Determine which rows to SELECT. If PKs were extracted from `queryInfo.SQL`'s WHERE clause, use `SELECT * FROM table WHERE pk1 = ? AND pk2 = ?`. **If WHERE parsing is too complex or absent for V1, fall back to `SELECT * FROM table` for the affected table (document this inefficiency).**
            *   Execute the SELECT using `tx.QueryContext`.
            *   Scan results into `[]types.Row`, using the canonical `GenerateRowID`.
            *   Return `map[string][]types.Row` and `map[string]types.TableSchema`.

**(Sequence 4: Replay Engine & Verification - ~30% Effort)**

3.6.  **Replay Engine Setup & Worker (`pkg/replay/engine.go`)**
    *   **Action:** Implement `Engine` struct, `Start`/`Stop` methods, worker pool goroutines listening on the `jobQueue` channel.
    *   **Instructions:** Each worker processes one job at a time.

3.7.  **Deterministic Execution & State Restoration (`pkg/replay/deterministic.go`)**
    *   **Action:** Implement `SetupVerificationDB`, `ExecuteQueriesDeterministically`.
    *   **Instructions:**
        *   `SetupVerificationDB`:
            *   Option 1 (Simpler): Create an **in-memory** SQLite DB (`:memory:`).
            *   Option 2 (More Realistic): Create a **temporary file copy** of the user's DB file *as it was before the transaction* (requires mechanism to get pre-tx DB state/file, maybe pass file path in job). This is complex.
            *   **Recommendation for V1:** Use in-memory DB. Create schema using `job.TableSchemas`. Populate using `job.PreStateData`.
        *   `ExecuteQueriesDeterministically`: Connect to the temporary/in-memory DB, start a transaction, execute `job.QuerySequence`. SQLite has fewer deterministic parameters to set, but consider relevant PRAGMAs if needed.

3.8.  **Replay Engine - Verification Flow (`pkg/replay/engine.go`)**
    *   **Action:** Implement the `processJob` logic: Receive job -> Setup Temp DB -> Replay Query -> Capture Actual Post-State (using `capture.CapturePostStateInTx` against the *temp* DB's tx) -> Calculate `PostRootCalculated` (using `commitment` logic) -> Compare Roots -> Log `VerificationResult` -> Cleanup Temp DB.
    *   **Instructions:** Ensure errors at any stage result in a `Success: false` log and proper cleanup. Reuse the exact same capture and commitment logic as the main wrapper.

**(Sequence 5: Testing & Refinement - Ongoing)**

3.9. **Unit & Integration Testing (`pkg/.../*_test.go`, `tests/`)**
    *   **Action:** Write unit tests for parsing, hashing, Merkle trees, RowID generation. Write integration tests that:
        *   Use the `vsqlite` wrapper library.
        *   Perform CRUD operations.
        *   Verify that `StateCommitmentRecord` and `VerificationResult` logs are produced as expected.
        *   Test successful verification cases.
        *   Test verification failures caused by non-deterministic functions used in the application SQL.
    *   **Instructions:** Use an in-memory SQLite DB for most integration tests for speed and isolation. Capture logs using `log.SetupWithWriter`.

**4. Data Structures (`pkg/types/types.go`)**

*   Keep structs lean for V1. `Value` can be `interface{}` initially, but add comments about needing stricter types later.
*   Ensure `StateCommitmentRecord` and `VerificationJob`/`Result` contain all necessary info for logging and replay.

**5. Core Algorithms (`pkg/commitment`)**

*   Ensure hashing and Merkle logic is identical to the refined Postgres version for consistency if both projects proceed. Use domain separation rigorously.

**6. API Definitions (V1)**

*   The primary API is the Go library interface provided by `pkg/vsqlite`. It should mirror `database/sql` (`Open`, `DB.ExecContext`, `DB.QueryContext`, `DB.BeginTx`, `Tx.ExecContext`, `Tx.Commit`, etc.).

**7. Security Considerations (V1 - SQLite)**

*   **SQL Injection:** Still relevant for the *internal* SELECTs used by state capture and any dynamic SQL in replay setup. Use parameterized queries where possible, strictly validate/quote identifiers.
*   **File Access:** The library needs access to the user's SQLite DB file. Ensure file paths provided to `vsqlite.Open` are handled safely.
*   **Resource Exhaustion:** A complex query during replay could consume excessive resources. Implement timeouts (`context.WithTimeout`) around DB operations within the Replay Engine.

**8. Performance Goals (V1 - SQLite)**

*   Wrapper overhead for SELECTs: < 1ms (p95).
*   Wrapper overhead for DML (capture+commit log+job dispatch): < 50ms (p95) - *highly dependent on state capture SELECT performance*.
*   Replay Engine Verification Latency (per simple DML): < 200ms (p95) - *should be faster than Postgres due to simpler setup*.

**9. Limitations & V2+ Roadmap**

*   **State Capture:** SELECT-based is inefficient and cannot handle triggers or non-PK tables well. V2 needs a better mechanism (VFS hooks? Application-level explicit logging?).
*   **SQL Support:** V1 only parses simple DML/SELECT. V2 needs broader SQL support.
*   **Concurrency:** SQLite's concurrency is limited. This wrapper doesn't add multi-writer capability.
*   **Verification:** Internal only. V2 needs external proof generation/verification and on-chain components.
*   **Rewriting:** No deterministic rewriting in V1.