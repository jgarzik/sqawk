# Sqawk

[![Crates.io](https://img.shields.io/crates/v/sqawk.svg)](https://crates.io/crates/sqawk)
[![Docs.rs](https://docs.rs/sqawk/badge.svg)](https://docs.rs/sqawk)
[![MIT licensed](https://img.shields.io/crates/l/sqawk.svg)](./LICENSE)

Sqawk is an SQL-based command-line tool for processing CSV files, inspired by the classic `awk` command. It loads CSV data into in-memory tables, executes SQL queries against these tables, and writes the results back to the console or files.

## Features

- Fast in-memory SQL query execution for CSV files
- Support for SELECT, INSERT, and DELETE operations
- Filter data with SQL WHERE clauses
- Process multiple files in a single command
- Safe by default: doesn't modify files without explicit request

## Installation

```sh
cargo install sqawk
```

## Usage

### Basic SELECT query

```sh
sqawk -s "SELECT * FROM data" data.csv
```

This loads `data.csv` into an in-memory table called "data" and performs a SELECT query.

### Filtering data with WHERE clause

```sh
sqawk -s "SELECT * FROM employees WHERE salary > 50000" employees.csv
```

### Deleting rows

```sh
sqawk -s "DELETE FROM data WHERE id = 5" data.csv --write
```

This removes rows with id = 5 and saves the changes back to data.csv.

### Multiple operations

```sh
sqawk -s "DELETE FROM data WHERE inactive = 1" -s "SELECT * FROM data" data.csv --write
```

This executes multiple SQL statements in sequence, deleting rows and then showing the results.

### Multiple files

```sh
sqawk -s "SELECT * FROM users" -s "SELECT * FROM orders" users.csv orders.csv
```

### Verbose mode

```sh
sqawk -s "SELECT * FROM data WHERE value > 100" data.csv -v
```

### Write mode

```sh
sqawk -s "DELETE FROM data WHERE status = 'expired'" data.csv --write
```

By default, sqawk doesn't modify input files. Use the `--write` flag to save changes back to the original files.

## Documentation

For more information, see the [documentation](https://github.com/jgarzik/sqawk).

## License

Licensed under the MIT License ([LICENSE](LICENSE) or http://opensource.org/licenses/MIT).

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you shall be licensed as MIT, without any additional 
terms or conditions.