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

### Dual Execution Engines

The codebase has two SQL execution paths:
1. **Direct execution** (default): `SqlExecutor::execute()` - interprets SQL AST directly
2. **VM execution** (`--vm` flag): `SqlExecutor::execute_vm()` - compiles SQL to bytecode, then executes in a VM (inspired by SQLite's architecture)

### VM Module (`src/vm/`)

- `bytecode.rs`: Defines bytecode instruction set
- `compiler.rs`: Compiles SQL AST to bytecode
- `engine.rs`: Executes bytecode instructions

### File Handling

- **`file_handler.rs`**: Manages loading files into database tables
- **`csv_handler.rs`**: Standard CSV file parsing (comma-separated)
- **`delim_handler.rs`**: Custom delimiter support via `-F` flag (TSV, colon-separated, etc.)

### SQL Features

- `aggregate.rs`: COUNT, SUM, AVG, MIN, MAX functions
- `join.rs`: Cross join and INNER JOIN implementations
- `string_functions.rs`: UPPER, LOWER, TRIM, SUBSTR, REPLACE

### Safe Writeback Model

By default, all modifications remain in memory only. The `--write` flag must be explicitly provided to save changes back to source files. Only tables modified by INSERT/UPDATE/DELETE are written.

## Test Organization

Tests are in `tests/` organized by functionality:
- `basic/`, `comparison/`, `join/`, `join_on/`, `order_by/`, `update/`
- `aggregate/`, `alias/`, `distinct/`, `group_by/`, `limit_offset/`
- `delimiter/`, `csv_handler/`, `string_functions/`, `repl/`
- `helpers/`: Test utilities
- `common.rs`: Shared test helpers including REPL script runner

Tests use `assert_cmd` for CLI testing and `tempfile` for temporary test data.
