# Sqawk

[![Crates.io](https://img.shields.io/crates/v/sqawk.svg)](https://crates.io/crates/sqawk)
[![Docs.rs](https://docs.rs/sqawk/badge.svg)](https://docs.rs/sqawk)
[![MIT licensed](https://img.shields.io/crates/l/sqawk.svg)](./LICENSE)

Sqawk is an SQL-based command-line tool for processing delimiter-separated files (CSV, TSV, etc.), inspired by the classic `awk` command. It loads data into in-memory tables, executes SQL queries, and optionally writes results back to files.

## Features

- **SQL Query Engine** - SELECT, INSERT, UPDATE, DELETE with WHERE, ORDER BY, GROUP BY, HAVING, LIMIT/OFFSET
- **Joins** - INNER, LEFT, RIGHT, FULL OUTER, and CROSS joins with ON conditions
- **Aggregates** - COUNT, SUM, AVG, MIN, MAX with GROUP BY support
- **Functions** - String (UPPER, LOWER, SUBSTR, REPLACE, etc.), math (ABS, ROUND, etc.), date/time
- **File Formats** - CSV, TSV, and custom delimiters; headerless files via `--tabledef`
- **Safe by Default** - Files unchanged unless `--write` flag is specified
- **Interactive REPL** - Explore data interactively with `-i` flag

## Installation

```sh
cargo install sqawk
```

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
```

## Documentation

- [User Guide](doc/user_guide.md) - Installation, CLI options, and examples
- [SQL Reference](doc/sql_reference.md) - Complete SQL syntax and functions
- [Database Architecture](doc/database.md) - Technical internals (for contributors)

## License

MIT License - see [LICENSE](LICENSE)

## Contributing

Contributions welcome. Any contribution submitted for inclusion shall be licensed as MIT.
