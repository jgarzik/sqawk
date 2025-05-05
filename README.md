# Sqawk

[![Crates.io](https://img.shields.io/crates/v/sqawk.svg)](https://crates.io/crates/sqawk)
[![Docs.rs](https://docs.rs/sqawk/badge.svg)](https://docs.rs/sqawk)
[![MIT licensed](https://img.shields.io/crates/l/sqawk.svg)](./LICENSE)

Sqawk is an SQL-based command-line tool for processing CSV files, inspired by the classic `awk` command. It loads CSV data into in-memory tables, executes SQL queries against these tables, and writes the results back to the console or files.

## Features

- **Powerful SQL Query Engine**
  - Support for SELECT, INSERT, UPDATE, and DELETE operations
  - WHERE clause filtering with comparison operators
  - ORDER BY for sorting results (ASC/DESC)
  - Column aliases with the AS keyword
  - Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
  
- **Multi-Table Operations**
  - Cross joins between tables
  - Inner joins using WHERE conditions
  - Support for joining multiple tables
  - Table-qualified column names

- **Smart Data Handling**
  - Automatic type inference (Integer, Float, Boolean, String)
  - Type coercion for comparisons
  - Null value support
  
- **CSV Processing**
  - Fast in-memory execution
  - Process multiple files in a single command
  - Table name customization
  - Chain multiple SQL statements
  
- **Safe Operation**
  - Doesn't modify files without explicit request (--write flag)
  - Only writes back tables that were modified
  - Verbose mode for operation transparency

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

### Updating rows

```sh
sqawk -s "UPDATE data SET status = 'active' WHERE id = 5" data.csv --write
```

This updates the status field to 'active' for rows with id = 5 and saves the changes back to data.csv.

### Deleting rows

```sh
sqawk -s "DELETE FROM data WHERE id = 5" data.csv --write
```

This removes rows with id = 5 and saves the changes back to data.csv.

### Multiple operations

```sh
sqawk -s "UPDATE data SET status = 'inactive' WHERE last_login < '2023-01-01'" -s "DELETE FROM data WHERE status = 'inactive' AND last_login < '2022-01-01'" -s "SELECT * FROM data" data.csv --write
```

This executes multiple SQL statements in sequence: first marking recent inactive accounts, then removing very old inactive accounts, and finally showing the results.

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

For more detailed information, see:

- [SQL Language Reference](doc/sql_reference.md) - Comprehensive guide to Sqawk's SQL dialect
- [In-Memory Database Architecture](doc/database.md) - Technical details about the database implementation

## License

Licensed under the MIT License ([LICENSE](LICENSE) or http://opensource.org/licenses/MIT).

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you shall be licensed as MIT, without any additional 
terms or conditions.