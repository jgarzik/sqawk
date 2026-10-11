# Sqawk

[![Crates.io](https://img.shields.io/crates/v/sqawk.svg)](https://crates.io/crates/sqawk)
[![MIT licensed](https://img.shields.io/crates/l/sqawk.svg)](./LICENSE)

Sqawk is an SQL-based command-line tool for processing delimiter-separated files (CSV, TSV, etc.), inspired by the classic `awk` command. It loads data into in-memory tables, executes SQL queries, and optionally writes results back to files.

## Features

- **SQL Query Engine** - SELECT, INSERT, UPDATE, DELETE with WHERE, ORDER BY, GROUP BY, HAVING, LIMIT/OFFSET
- **Joins** - INNER, LEFT, RIGHT, FULL OUTER, and CROSS joins with ON conditions
- **Aggregates** - COUNT, SUM, TOTAL, AVG, MIN, MAX, GROUP_CONCAT/STRING_AGG with GROUP BY, DISTINCT and `FILTER (WHERE ...)`
- **Functions** - String (UPPER, LOWER, LENGTH, SUBSTR, INSTR, REPLACE, TRIM, LPAD/RPAD, PRINTF, ...), math (ABS, ROUND, CEIL, FLOOR, SIGN, multi-argument MIN/MAX), conditional (COALESCE, IFNULL, NULLIF, IIF, TYPEOF), date/time (DATE, TIME, DATETIME, STRFTIME, JULIANDAY, UNIXEPOCH with modifiers)
- **SQLite semantics** - 64-bit integer arithmetic, SQLite CAST rules, ASCII case-insensitive LIKE with ESCAPE, character-based strings; unimplemented syntax is an error, never silently ignored
- **Subqueries** - Scalar, `IN (SELECT ...)`, and `EXISTS`, including correlated
- **Set Operations** - UNION, UNION ALL, INTERSECT, EXCEPT
- **Window Functions** - ROW_NUMBER, RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE, and aggregates with `OVER (PARTITION BY ... ORDER BY ...)`, ROWS frames and named windows
- **DDL** - CREATE TABLE, CREATE TABLE AS SELECT, DROP, ALTER TABLE ADD COLUMN, TRUNCATE
- **Expressions** - CASE, CAST, BETWEEN, IN, LIKE/ILIKE ... ESCAPE, IS [NOT] DISTINCT FROM, `||`, arithmetic; ORDER BY ... NULLS FIRST/LAST
- **File Formats** - CSV, TSV (`.tsv`/`.tab`), and custom delimiters (`-F`); headerless files via `--no-header` or `--tabledef`; `#` comment lines via `--comments`; untouched rows survive `--write` byte for byte
- **Pipelines** - Reads standard input with a `-` file operand
- **Safe by Default** - Files unchanged unless `--write` flag is specified
- **Interactive REPL** - Explore data interactively with `-i` flag

## Installation

```sh
cargo install sqawk
```

This installs the `sqawk` binary. Requires Rust 1.88 or newer.

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

## `tsq` - test data generator (development only)

The source tree also contains `tsq`, which generates deterministic multi-table
CSV data plus a corpus of SQL queries for exercising sqawk. It is a separate
workspace crate (`tsq/`) that is not published, so `cargo install sqawk` does
not install it. Run it from a checkout:

```sh
cargo run -p tsq -- --seed 42 --rows 1000 --output-dir /tmp/sqawk-test
sqawk -s "SELECT * FROM customers LIMIT 10" /tmp/sqawk-test/data/customers.csv
```

It writes `data/` (customers, products, orders, order_items, reviews, with
realistic foreign-key relationships), `queries/` (numbered `.sql` files
covering selects, joins, aggregates, subqueries and window functions),
`verify/run_verification.sh`, and a `metadata.json` recording the seed and row
counts. The same seed always produces the same data.

| Option | Meaning |
| --- | --- |
| `-s`, `--seed` | Seed for reproducible generation; a random one is printed if omitted |
| `-r`, `--rows` | Base customer row count; other tables scale proportionally (default 100000) |
| `-o`, `--output-dir` | Where to write the generated tree (required) |
| `-v`, `--verbose` | Show generation progress |

## Documentation

- [User Guide](doc/user_guide.md) - Installation, CLI options, and examples
- [SQL Reference](doc/sql_reference.md) - Complete SQL syntax and functions
- [Database Architecture](doc/database.md) - Technical internals (for contributors)

## License

MIT License - see [LICENSE](LICENSE)

## Contributing

Contributions welcome. Any contribution submitted for inclusion shall be licensed as MIT.
