# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Test Commands

```bash
# Build the project
cargo build

# Run all tests
cargo test

# Run a specific test module
cargo test --test mod basic       # Basic end-to-end tests
cargo test --test mod comparison  # Comparison operator tests
cargo test --test mod join        # JOIN tests
cargo test --test mod aggregate   # Aggregate function tests

# Run a single test by name
cargo test test_name_here

# Run with verbose output
cargo test -- --nocapture

# Run the CLI directly
cargo run -- -s "SELECT * FROM data" data.csv

# Run in interactive REPL mode
cargo run -- --interactive data.csv
```

## Architecture Overview

Sqawk is an SQL-based CLI tool for processing delimiter-separated files (CSV, TSV, etc.). It loads files into in-memory tables, executes SQL queries, and optionally writes results back.

### Core Components

- **`main.rs`**: Entry point orchestrating the pipeline: CLI parsing → file loading → SQL execution → output → optional writeback
- **`database.rs`**: Central store for in-memory tables, manages table definitions and schemas
- **`table.rs`**: In-memory table representation with columns, rows, and projection capabilities
- **`sql_executor.rs`**: SQL parsing (via `sqlparser` crate) and execution against in-memory tables

### SQL Execution (VM-based)

SQL execution uses a bytecode VM inspired by SQLite's architecture:
1. SQL is parsed via the `sqlparser` crate
2. The AST is compiled to bytecode (`src/vm/compiler.rs`)
3. The VM engine executes the bytecode (`src/vm/engine.rs`)

### VM Module (`src/vm/`)

- `bytecode.rs`: Bytecode instruction set and register/program types
- `compiler.rs`: SQL AST to bytecode. Holds `code_expr`, the single expression
  compiler (the analogue of SQLite's `sqlite3ExprCode`) used from the SELECT
  list, WHERE, HAVING, ORDER BY and SET clauses alike, plus `NameCtx`, which
  carries the FROM sources a name can resolve against
- `compiler_aggregate.rs`: aggregates, GROUP BY, HAVING
- `compiler_join.rs`: joins
- `compiler_dml.rs`: INSERT / UPDATE / DELETE
- `compiler_ddl.rs`: CREATE / DROP / ALTER / TRUNCATE
- `compiler_window.rs`: window functions
- `ast_compat.rs`: thin accessors normalizing sqlparser AST shapes
- `engine.rs`: Executes bytecode instructions

Each statement is compiled and executed separately, so a statement observes the
effects of the ones before it.

### File Handling

- **`file_handler.rs`**: Manages loading files into database tables
- **`csv_handler.rs`**: Standard CSV file parsing (comma-separated)
- **`delim_handler.rs`**: Custom delimiter support via `-F` flag (TSV, colon-separated, etc.)

### SQL Features

Expression compilation, including all scalar and string functions, lives in
`src/vm/compiler.rs` behind `code_expr`. Aggregates are compiled in
`src/vm/compiler_aggregate.rs` and executed by the `AggStep`/`AggFinal`
opcodes; `src/aggregate.rs` retains only the function-name lookup.

### Safe Writeback Model

By default, all modifications remain in memory only. The `--write` flag must be explicitly provided to save changes back to source files. Only tables modified by INSERT/UPDATE/DELETE are written.

## Test Organization

Tests are in `tests/` organized by functionality:
- `basic/`, `comparison/`, `join/`, `join_on/`, `order_by/`, `update/`
- `aggregate/`, `alias/`, `distinct/`, `group_by/`, `limit_offset/`
- `delimiter/`, `csv_handler/`, `string_functions/`, `repl/`
- `helpers/`: Test utilities
- `common.rs`: Shared test helpers including REPL script runner

Two suites carry the correctness contract:

- `tests/golden/`: exact-output characterization tests. They assert the
  COMPLETE stdout -- every row, in order -- via `assert_query`. The rest of the
  suite uses `predicate::str::contains`, which is substring matching: a test
  expecting `"Engineering,3"` passes even when extra wrong rows are emitted and
  regardless of row order. Prefer `assert_query` for new tests.
- `tests/defects/`: `#[ignore]`d tests encoding CORRECT behaviour for known
  defects. Run with `cargo test --test mod defects -- --ignored`. Remove the
  `#[ignore]` when a defect is fixed; the list only shrinks.

`tests/sqlite_diff/harness.py` is a differential tester against SQLite (via
python3's `sqlite3` module), not part of `cargo test`. Run
`cargo build --release && python3 -I tests/sqlite_diff/harness.py
tests/sqlite_diff/queries.sql tests/sqlite_diff/queries2.sql`; the SUMMARY
line counts pass / DIFF (wrong answer) / sqawk_err.

Tests use `assert_cmd` for CLI testing and `tempfile` for temporary test data.
