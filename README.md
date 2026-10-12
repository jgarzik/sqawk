# Sqawk

[![Crates.io](https://img.shields.io/crates/v/sqawk.svg)](https://crates.io/crates/sqawk)
[![MIT licensed](https://img.shields.io/crates/l/sqawk.svg)](./LICENSE)

Sqawk is an SQL-based command-line tool for processing delimiter-separated files (CSV, TSV, etc.), inspired by the classic `awk` command. It loads data into in-memory tables, executes SQL queries, and optionally writes results back to files.

## Features

- **SQL Query Engine** - SELECT, INSERT, UPDATE, DELETE with WHERE, ORDER BY, GROUP BY, HAVING, LIMIT/OFFSET
- **Joins** - INNER, LEFT, RIGHT, FULL OUTER, and CROSS joins with ON, USING or NATURAL; equality joins run as hash joins
- **Aggregates** - COUNT, SUM, TOTAL, AVG, MIN, MAX, GROUP_CONCAT/STRING_AGG with GROUP BY, DISTINCT and `FILTER (WHERE ...)`
- **Functions** - String (UPPER, LOWER, LENGTH, SUBSTR, INSTR, REPLACE, TRIM, LPAD/RPAD, PRINTF, QUOTE, ...), math (ABS, ROUND, CEIL, FLOOR, SIGN, multi-argument MIN/MAX, bitwise operators), conditional (COALESCE, IFNULL, NULLIF, IIF, TYPEOF), date/time (DATE, TIME, DATETIME, STRFTIME, JULIANDAY, UNIXEPOCH, TIMEDIFF with SQLite's modifiers)
- **SQLite semantics** - 64-bit integer arithmetic, SQLite CAST rules, ASCII case-insensitive LIKE with ESCAPE, character-based strings; unimplemented syntax is an error, never silently ignored
- **Subqueries** - Scalar, `IN (SELECT ...)`, and `EXISTS`, including correlated
- **Common Table Expressions** - `WITH` and `WITH RECURSIVE` (tree walks, counters), on SELECT, INSERT, UPDATE and DELETE
- **Set Operations** - UNION, UNION ALL, INTERSECT, EXCEPT
- **Window Functions** - ROW_NUMBER, RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE, and aggregates with `OVER (PARTITION BY ... ORDER BY ...)`, ROWS frames and named windows
- **DDL** - CREATE TABLE, CREATE TABLE AS SELECT, DROP, ALTER TABLE ADD COLUMN, TRUNCATE
- **Transactions** - BEGIN, COMMIT, ROLLBACK over the in-memory tables; nothing uncommitted is written
- **Expressions** - CASE, CAST, BETWEEN, IN, LIKE/ILIKE ... ESCAPE, IS [NOT] DISTINCT FROM, `||`, arithmetic; ORDER BY ... NULLS FIRST/LAST
- **File Formats** - CSV, TSV (`.tsv`/`.tab`), and custom delimiters (`-F`); headerless files via `--no-header` or `--tabledef`; `#` comment lines via `--comments`; untouched rows survive `--write` byte for byte
- **Pipelines** - Reads standard input with a `-` file operand
- **Safe by Default** - Files unchanged unless `--write` flag is specified
- **Interactive REPL** - Explore data interactively with `-i` flag

Every input file is loaded whole into memory: plan on six to seven times the
file's size, plus the result set (see
[Working with Large Files](doc/user_guide.md#working-with-large-files)).

## Installation

```sh
cargo install --locked sqawk
```

This installs the `sqawk` binary. Requires Rust 1.88 or newer. `--locked`
builds with the dependency versions sqawk was tested with.

## Quick Examples

```sh
# Query a CSV file
sqawk -s "SELECT name, salary FROM employees WHERE department = 'Engineering'" employees.csv

# Join two files
sqawk -s "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id" users.csv orders.csv

# Aggregate data
sqawk -s "SELECT department, AVG(salary) FROM employees GROUP BY department" employees.csv

# Modify and save
sqawk -s "UPDATE data SET status = 'archived' WHERE year < 2020" data.csv --write

# Read standard input as the table "stdin"
cat employees.csv | sqawk -s "SELECT name FROM stdin WHERE salary > 70000" -

# Run the statements in a file; print tab-separated results without a header
sqawk -f report.sql --output-delimiter '\t' --no-output-header employees.csv
```

Errors are one line on stderr (`sqawk: no such column: nmae`) with exit
status 1; usage errors exit with status 2.

## `tsq` - test data generator, differential tester and fuzzer (development only)

The source tree also contains `tsq`, the tool that tests sqawk: it generates
deterministic test data, checks sqawk's answers and its `--write` output
against SQLite, fuzzes the command line and the REPL, and measures
performance. It is a separate workspace crate (`tsq/`) that is not
published, so `cargo install sqawk` does not install it. Run it from a
checkout:

```sh
cargo build --release --workspace
target/release/tsq gen --seed 42 --rows 1000 --output-dir /tmp/sqawk-test
sqawk -s "SELECT * FROM customers LIMIT 10" /tmp/sqawk-test/data/customers.csv
target/release/tsq diff --seed 1 --queries 20000 --keep-going --shrink
target/release/tsq corpus tsq/corpus/*.sql
target/release/tsq wb --seed 1 --iterations 2000
target/release/tsq cli --seed 1 --iterations 2000
target/release/tsq stress --rows 1000000 --json stress.json
```

The global options `--seed` (64-bit; a clock seed is printed when omitted),
`--sqawk PATH` (default: `$SQAWK`, else the `sqawk` next to `tsq`) and
`--verbose` apply to every subcommand. The same seed gives the same data and
queries on every OS.

- **`tsq gen`** writes `data/` (customers, products, orders, order_items,
  reviews, with foreign keys and realistic distributions) and a
  `metadata.json` recording the seed, options and row counts. `--rows N`
  (customers; the other tables scale from it), `--edge-rate F` (replace
  non-key cells with edge values: i64 limits, `007`, `1.50`, ` 5`, Unicode,
  embedded quotes and line breaks, long strings, ...), `--format csv|tsv`,
  `--crlf`, `--bom`, `--no-header`, `--ragged-rate F` (rows that leave off
  trailing empty fields).
- **`tsq diff`** generates random SQL (expressions, joins of every kind,
  subqueries, aggregates, windows, compounds, CTEs including recursive ones,
  DML sequences, transactions, `CREATE TABLE AS`, `ALTER TABLE`) and runs
  each test through sqawk and through SQLite 3.45 loaded with the same
  values, comparing every result set byte for byte. `--queries N`,
  `--scale small|large|ROWS`, `--tables gen|edge|harness`, `--edge-rate F`,
  `--keep-going`, `--shrink` (minimize each finding; reproductions go to
  `--out DIR`), `--only N` (one test), `--print`, `--jobs N`, `--timeout S`.
  The last line is `DIFF SUMMARY {pass, diff, sqawk_err, sqlite_err,
  both_err, crash}`; non-passing results covered by a documented difference
  are counted separately, and any other is a finding (exit status 1).
- **`tsq corpus FILE...`** runs hand-written query files (one test per line,
  `O:` for an ordered comparison) the same way; `tsq/corpus/` holds them.
- **`tsq wb`** fuzzes `--write`: random files (CSV / TSV / `-F` separators,
  LF / CRLF / mixed line endings, byte order marks, ragged rows, blank and
  `#` comment lines, quoted and multi-line fields, edge values) and random
  UPDATE / DELETE / INSERT / ALTER TABLE / TRUNCATE sequences with
  transactions and `CREATE TABLE ... LOCATION`. Every file must equal what
  the writeback rules give for SQLite's final table (untouched rows byte for
  byte, changed rows from their values); rolled-back work and read-only
  targets must leave files untouched. On Unix it also kills sqawk during a
  large write: the file must be the old or the new content, never partial.
  `--iterations N`, `--only N`, `--kill-rate F`. Last line: `WB SUMMARY
  {ok, bad, crash}`.
- **`tsq cli`** fuzzes the command line and the REPL: random flag and
  operand combinations, piped REPL scripts, malformed files, garbage and
  very large SQL, standard output closed early. Every run must exit 0, 1 or
  2 (or a `.exit N` status) within the time limit, print errors as one
  `sqawk: ` line, never panic, and leave no stray files. `--iterations N`,
  `--only N`. Last line: `CLI SUMMARY {ok, bad, crash}`.
- **`tsq stress`** times workloads (scan, filter, group, joins, sort,
  distinct, windows, CTEs, subqueries, compounds, `--write`, a 1000-column
  table) over generated data, `--rows` 1,000,000 to 10,000,000 in the
  largest table, recording wall time and, on Linux, the peak RSS of sqawk.
  `--json OUT` saves the results; `--baseline FILE --tolerance PCT` exits 1
  when a workload regressed; `--repeat K` keeps the best of K runs;
  `--workloads a,b` / `--list`.

`cargo test --workspace` runs small budgets of `diff`, `corpus`, `wb` and
`cli`; `stress` is run by hand.

## Documentation

- [User Guide](doc/user_guide.md) - Installation, CLI options, and examples
- [SQL Reference](doc/sql_reference.md) - Complete SQL syntax and functions
- [Database Architecture](doc/database.md) - Technical internals (for contributors)

## License

MIT License - see [LICENSE](LICENSE)

## Contributing

Contributions welcome. Any contribution submitted for inclusion shall be licensed as MIT.
