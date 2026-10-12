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

# tsq (unpublished workspace crate in tsq/): data, differential testing
cargo run -p tsq -- gen --seed 42 --rows 1000 --output-dir /tmp/sqawk-test
cargo build --release --workspace
target/release/tsq diff --seed 1 --queries 20000 --keep-going --shrink
target/release/tsq corpus tsq/corpus/*.sql
```

## Architecture Overview

Sqawk is an SQL-based CLI tool for processing delimiter-separated files (CSV, TSV, etc.). It loads files into in-memory tables, executes SQL queries, and optionally writes results back.

### Core Components

- **`main.rs`**: Entry point orchestrating the pipeline: CLI parsing → file loading → SQL execution → output → optional writeback. Any error is printed once as `sqawk: <message>` (one line, exit 1); clap usage errors exit 2
- **`cli.rs`**: clap arguments; `-s`/`-f` are merged into one ordered script by command-line position
- **`config.rs`**: `AppConfig`, the parsed options (-v, -F, --tabledef, --write, header/comment/output flags) passed to every component instead of global state
- **`repl.rs`**: the interactive shell (`-i`). On a terminal: rustyline editor with prompt, multi-line statements, tab completion and history (`$XDG_STATE_HOME/sqawk/history` when that is absolute, else `~/.sqawk_history`); piped, it reads lines and prints only command output. Dot commands (`.tables`, `.schema`, `.save`, `.cd`, `.load`, ...) live here
- **`error.rs`**: `SqawkError`; each `Display` is one complete line and no variant exposes a `source()` (that would print the cause twice). File I/O errors name the user's file (`SqawkError::open` / `write`), never a temp file
- **`diag.rs`**: `diag!` / `outln!` -- every stderr diagnostic (errors, `-v`) and REPL stdout line goes through them; write errors are ignored, so a closed stream never panics. Do not use `eprintln!`/`println!`
- **`database.rs`**: Central store for in-memory tables; owns modification tracking (`mark_modified` / `is_modified` / `modified_tables`, which decide what `--write` saves) and transactions (`begin` / `commit` / `rollback`; a table's original is kept the first time a transaction changes it)
- **`table.rs`**: In-memory table representation with columns (name, type affinity, declared type name), rows, and projection capabilities
- **`sql_executor.rs`**: `SqlExecutor`, the facade `main.rs` and the REPL use: `execute` hands SQL text to `vm::execute_vm` and applies the output delimiter to the result tables; it also saves modified tables (`save_modified_tables`, `save_table`), rolls back a transaction left open at exit (`finish`), and answers the REPL's table/column/row-count queries. It does no parsing itself

### SQL Execution (VM-based)

SQL execution uses a bytecode VM inspired by SQLite's architecture:
1. SQL is parsed by `src/vm/parse.rs` (`parse_sql`, sqlparser with `SqawkDialect`)
2. The AST is compiled to bytecode (`src/vm/compiler.rs`)
3. The VM engine executes the bytecode (`src/vm/engine.rs`)

### VM Module (`src/vm/`)

- `mod.rs`: `execute_vm`, the statement loop: parses the script, then per
  statement runs BEGIN/COMMIT/ROLLBACK directly on the `Database`,
  materializes each derived table (`FROM (SELECT ...) t`) as a temporary
  table under its alias, compiles and executes the statement, and applies the
  engine's `TableModification`s (insert/replace/delete rows, create/drop/alter/
  truncate tables) to the `Database`, marking each changed table modified
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
  (RETURNING, ON CONFLICT, UPDATE FROM, QUALIFY, ...) before
  compilation; INSERT/UPDATE/DELETE/Query/Select are destructured
  exhaustively so a new sqlparser field fails to compile until handled
- `parse.rs`: `parse_sql`, the only SQL-to-AST entry point: parses with
  `dialect.rs`'s `SqawkDialect` (Hive syntax, SQLite operator precedence),
  rewrites compound SELECTs left-deep (equal precedence, left to right), and
  enforces SQLite's limits (expression depth 1000, 500 compound terms) with an
  iterative walk before anything recurses over the tree
- `mod.rs`: `execute_vm` / `execute_statements`, the per-statement loop:
  pre-pass, compile, execute, apply modifications, drop the statement's
  registered tables
- `prepass.rs`: runs before each statement compiles. Derived tables and CTEs
  become registered tables (`sqawk_derived_N`, `sqawk_cte_N`) and FROM items
  are rewritten to them; CTE scoping (siblings, self, shadowing, lazy single
  materialization), `WITH ... DML` unwrapping, and recursive CTEs (SQLite's
  queue algorithm, one run per generation when the queue is FIFO). Other
  one-part references sharing a CTE's name become `main.t`, and the compiler
  refuses any one-part CTE name left over (`SqlCompiler::cte_names`), so a
  missed rewrite cannot read the loaded table
- `parse.rs`: `parse_sql` (SQLite precedence, hex literals, MATERIALIZED
  hints dropped, compounds left-deep, depth / compound limits) and
  `for_each_child`, the one enumeration of a statement's queries, FROM items
  and expressions shared by the limit walk and the pre-pass
- `ast_compat.rs`: thin accessors normalizing sqlparser AST shapes
- `engine.rs`: Executes bytecode instructions
- `arith.rs`: SQLite numeric semantics -- text-to-number coercion, i64
  arithmetic with overflow to real, bitwise operators, CAST, truth values
- `func.rs`: the scalar function table (`FuncDef`: name, arity, body) run by
  the `Function` opcode (SQLite's OP_Function); arity is checked at compile
  time. Lazy functions (COALESCE, IFNULL, IIF, NULLIF) are compiled to jumps
- `printf.rs`: SQLite's printf (a port of `sqlite3_str_vappendf`), used by
  PRINTF/FORMAT, QUOTE and the date functions
- `datetime.rs`: DATE/TIME/DATETIME/STRFTIME/TIMEDIFF/... (a port of SQLite
  3.45's date.c: time strings, modifiers, `localtime` via the C library)
- `like.rs`: the LIKE matcher (no regex; compiled pattern cached per
  instruction)
- `agg.rs`: aggregate accumulators shared by AggStep/AggFinal and windows

Each statement is compiled and executed separately, so a statement observes the
effects of the ones before it.

### File Handling

- **`file_handler.rs`**: Manages loading files into database tables and saving
  them. `parse_table_spec` splits a file operand `[NAME=]PATH` (NAME only when
  identifier-like; otherwise the table is named after the file stem; `-` is
  stdin, table `stdin`); `has_tab_extension` is the `.tsv`/`.tab` rule used
  for loading and for `CREATE TABLE ... LOCATION`; `load_location` loads (or
  starts) a LOCATION table under its declared columns
- **`csv_handler.rs`**: The single loader (`read_table`, built on the `csv`
  crate) for every delimiter and for stdin, plus writeback. `.tsv`/`.tab` are
  tab-separated, everything else comma (unless `-F`); the first line is always
  the header unless `--no-header`/`--tabledef`; `#` lines are comments only
  under `--comments`. Each table keeps its file's bytes (`RawRows` in
  `table.rs`: one buffer + per-row offsets), so untouched rows write back
  byte-for-byte; every row mutation in `table.rs` keeps the offsets in sync

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

By default, all modifications remain in memory only. The `--write` flag must be explicitly provided to save changes back to source files. Only tables changed by INSERT/UPDATE/DELETE/ALTER/TRUNCATE are written, and rows a statement did not change are written back byte for byte. `CREATE TABLE t (cols) LOCATION 'file'` follows Hive external-table semantics (`file_handler::load_location`): an existing file is loaded under the declared columns (its header line skipped, checked against the column count and written back verbatim; `WITH (HEADER=false)` makes the first line data; `WITH (COMMENTS=true)` is `--comments`) and loading it is not a modification; a file that does not exist yet is written even with no DML after it (created with just the header). The delimiter is WITH (DELIMITER=...) if given, else tab for `.tsv`/`.tab`, else comma. A LOCATION file already loaded as another table is an error. Scratch tables (CREATE TABLE without LOCATION, CREATE TABLE AS) are never written. Every target is checked for writability before any file is written; a read-only file aborts the whole writeback. An open transaction is rolled back before writeback.

## Test Organization

Tests are in `tests/` organized by functionality:
- `basic/`, `comparison/`, `join/`, `join_on/`, `order_by/`, `update/`
- `unsupported/`: parsed-but-unimplemented syntax must error and modify nothing
- `aggregate/`, `alias/`, `distinct/`, `group_by/`, `limit_offset/`
- `delimiter/`, `file_io/`, `headerless/`, `stdin/`, `string_functions/`, `repl/`, `transaction/`
- loader and writer unit tests live in `src/csv_handler.rs` (`#[cfg(test)]`)
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

`tsq/` (the unpublished `tsq` crate) is the differential tester. Build with
`cargo build --release --workspace`; `--sqawk PATH` or `SQAWK=path`
overrides the binary (default: the `sqawk` next to `tsq`). Data goes to temp
dirs. Module map: `rng.rs` (ChaCha8, the only randomness), `data.rs` (tables,
lossless CSV/TSV writer), `gen.rs` (`tsq gen`), `run.rs` (spawning sqawk with
the SQL on stdin and a time limit), `corpus.rs`, and `diff/`: `oracle.rs`
(rusqlite's bundled SQLite 3.45 -- `.cargo/config.toml` enables its math
functions -- loaded with sqawk's per-cell inference into untyped columns,
results rendered as sqawk prints them: reals via `CAST(x AS TEXT)`, NULL
empty, RFC 4180 quoting, headers included, compared byte for byte),
`schema.rs` (per-column properties deciding which comparisons both engines
agree on), `sqlgen.rs` (the generator, built on `ast.rs` shrinkable trees),
`allow.rs` (one rule per documented difference), `shrink.rs`, `tables.rs`
(built-in `harness` / `edge` table sets).

- `tsq diff --seed N --queries 20000 --keep-going [--scale large]
  [--tables edge] [--edge-rate 0.05] [--shrink --out DIR]`: random SQL
  vs SQLite. `DIFF SUMMARY {pass, diff, sqawk_err, sqlite_err, both_err,
  crash}`, then the documented-difference counts, SQLite time-outs (too
  heavy for the reference; not findings) and `findings N` (exit 1 when
  non-zero). `--only N` replays one test; `--print` shows the SQL.
- `tsq corpus tsq/corpus/*.sql`: the hand corpora (`queries*.sql` on the
  `harness` tables, `hand*.sql` on `edge`, chosen by a `-- tsq-tables:`
  line); same summary as `CORPUS SUMMARY`.
- `cargo test --workspace` runs fast smoke budgets of both (a few seconds).

`tests/sqlite_diff/` still holds Python robustness testers, not part of
`cargo test`; run `cargo build --release` first (`SQAWK=path` overrides the
binary). `harness.py` and `diffh.py` are superseded by `tsq corpus` / `tsq
diff` (same corpora, same counts).

- `python3 -I tests/sqlite_diff/wb.py [SEED [N]]`: `--write` round-trip
  fuzzer; WRITEBACK SUMMARY must show bad 0 and sqawk_err 0 (untouched
  records byte-identical; BOM, comments and CRLF kept).
- `python3 -I tests/sqlite_diff/csvfuzz.py`: malformed and huge inputs;
  CSVFUZZ SUMMARY must show crash 0 and writeback_bad 0 (clean `sqawk:`
  errors are expected).
- `python3 -I tests/sqlite_diff/fuzz.py`: garbage SQL, limit values, deep
  nesting; FUZZ SUMMARY must show findings 0.

Every remaining DIFF / sqawk_err must be a difference or an unsupported
feature listed in `doc/sql_reference.md` ("Differences from SQLite",
"Unsupported syntax"); panic and hang must be 0. NULL and `''` both print
empty, so the testers cannot tell them apart.

Tests use `assert_cmd` for CLI testing and `tempfile` for temporary test data.
