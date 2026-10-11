# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Test Commands

```bash
# Build the project
cargo build

# Run all tests
cargo test --workspace   # sqawk + the tsq dev crate

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

# Generate test data with tsq (unpublished workspace crate in tsq/)
cargo run -p tsq -- --seed 42 --rows 1000 --output-dir /tmp/sqawk-test
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
- `compiler_select.rs`: the SELECT driver -- `compile_query`, compound
  operators (UNION/INTERSECT/EXCEPT via keyed ephemeral tables) and the single
  output stage (ORDER BY with hidden sort keys, DISTINCT, LIMIT/OFFSET) every
  SELECT core ends in
- `compiler_aggregate.rs`: aggregates, GROUP BY, HAVING
- `compiler_from.rs`: the FROM planner used by every SELECT -- tables, comma
  lists and join chains become nested loop levels (ON per level, WHERE after
  outer-join NULL-extension, hash join on equi-conditions); the projection /
  aggregate stage is a body callback emitted at the innermost level; a
  FROM-less SELECT is a plan with zero sources (one row)
- `compiler_dml.rs`: INSERT / UPDATE / DELETE
- `compiler_ddl.rs`: CREATE / DROP / ALTER / TRUNCATE
- `compiler_window.rs`: the window stage -- finished rows (every column, or
  a grouped query's keys and aggregates) plus window keys/arguments go into
  an ephemeral table; one `WindowCompute` per distinct PARTITION BY/ORDER BY
  sorts it and fills each function's result column (`src/vm/window.rs` is
  the run-time half); the output stage reads rows back with each window call
  bound like an aggregate, so window calls work inside any expression and in
  ORDER BY
- `compiler_subquery.rs`: scalar / EXISTS / IN subqueries, compiled by the
  same query compiler as fragments of the enclosing program (`Once` when
  uncorrelated; correlation found by `NameCtx` outward resolution)
- `compiler_check.rs`: rejects every parsed clause sqawk does not implement
  (RETURNING, ON CONFLICT, UPDATE FROM, WITH, QUALIFY, ...) before
  compilation; INSERT/UPDATE/DELETE/Query/Select are destructured
  exhaustively so a new sqlparser field fails to compile until handled
- `parse.rs`: `parse_sql`, the only SQL-to-AST entry point: parses with
  `dialect.rs`'s `SqawkDialect` (Hive syntax, SQLite operator precedence),
  rewrites compound SELECTs left-deep (equal precedence, left to right), and
  enforces SQLite's limits (expression depth 1000, 500 compound terms) with an
  iterative walk before anything recurses over the tree
- `ast_compat.rs`: thin accessors normalizing sqlparser AST shapes
- `engine.rs`: Executes bytecode instructions
- `arith.rs`: SQLite numeric semantics -- text-to-number coercion, i64
  arithmetic with overflow to real, CAST, truth values
- `func.rs`: the scalar function table (`FuncDef`: name, arity, body) run by
  the `Function` opcode (SQLite's OP_Function); arity is checked at compile
  time. Lazy functions (COALESCE, IFNULL, IIF, NULLIF) are compiled to jumps
- `datetime.rs`: DATE/TIME/DATETIME/STRFTIME/... (SQLite time strings and
  modifiers, UTC)
- `like.rs`: the LIKE matcher (no regex; compiled pattern cached per
  instruction)
- `agg.rs`: aggregate accumulators shared by AggStep/AggFinal and windows

Each statement is compiled and executed separately, so a statement observes the
effects of the ones before it.

### File Handling

- **`file_handler.rs`**: Manages loading files into database tables
- **`csv_handler.rs`**: The single loader (`read_table`, built on the `csv`
  crate) for every delimiter and for stdin, plus writeback. `.csv` files always
  have a header; other files keep the header guess and `#`-comment skipping.
  Fields load as Integer/Float only when the text round-trips exactly, so
  untouched cells write back byte-for-byte

### SQL Features

Expression compilation lives in `src/vm/compiler.rs` behind `code_expr`;
scalar functions are looked up in `src/vm/func.rs` and called through the
`Function` opcode. Aggregates (including FILTER and GROUP_CONCAT's separator)
are compiled in `src/vm/compiler_aggregate.rs` and executed by the
`AggStep`/`AggFinal` opcodes over `src/vm/agg.rs` accumulators;
`src/aggregate.rs` retains only the function-name lookup and arity. Semantics
follow SQLite 3.45; deliberate differences are listed in
`doc/sql_reference.md` ("Differences from SQLite").

### Safe Writeback Model

By default, all modifications remain in memory only. The `--write` flag must be explicitly provided to save changes back to source files. Only tables modified by INSERT/UPDATE/DELETE are written.

## Test Organization

Tests are in `tests/` organized by functionality:
- `basic/`, `comparison/`, `join/`, `join_on/`, `order_by/`, `update/`
- `unsupported/`: parsed-but-unimplemented syntax must error and modify nothing
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
