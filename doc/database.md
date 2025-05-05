# Sqawk In-Memory Database Documentation

## Overview

Sqawk implements a lightweight in-memory database engine that supports SQL operations on CSV data. The database is designed for performance and simplicity, focusing on providing essential SQL functionality for data analysis tasks directly on CSV files without requiring an external database engine.

## Table of Contents

1. [Data Model](#data-model)
2. [Data Types](#data-types)
3. [SQL Support](#sql-support)
4. [Comparison Operators](#comparison-operators)
5. [Architecture](#architecture)
6. [Performance Considerations](#performance-considerations)
7. [Limitations](#limitations)

## Data Model

### Tables

Tables in Sqawk are in-memory representations of CSV files. Each table has:

- A unique name (derived from the CSV filename or explicitly provided)
- A set of columns with names (derived from the CSV header row)
- Zero or more rows of data
- Optional metadata including the source file path

Tables maintain an internal mapping of column names to their indices for efficient access.

### Rows and Columns

- **Rows**: Represented as vectors of values with one element per column
- **Columns**: Identified by name, with automatic type inference based on content
- **Schema**: Dynamically determined from the CSV header row
- **Column Types**: Column types are not explicitly declared but inferred at runtime

### Table Lifecycle

Tables are loaded from CSV files at startup and can be modified through SQL operations. Any modifications are kept in memory until explicitly written back to disk with the `--write` flag.

## Data Types

Sqawk supports the following data types internally:

| Type | Description | Example | Storage |
|------|-------------|---------|---------|
| `Null` | Missing or null value | NULL | Special variant |
| `Integer` | 64-bit signed integer | 42 | i64 |
| `Float` | 64-bit floating point | 3.14 | f64 |
| `String` | UTF-8 text | "hello" | String |
| `Boolean` | True/false value | true | bool |

### Type Conversion

When loading data from CSV files, Sqawk attempts to infer the most appropriate type for each value:

1. First tries to parse as an `Integer`
2. If that fails, tries to parse as a `Float`
3. If that fails, tries to parse as a `Boolean` (true/false, yes/no, 1/0)
4. If all else fails, stores the value as a `String`
5. Empty values are stored as `Null`

### Type Coercion

For comparison operations, Sqawk implements automatic type coercion:

- `Integer` and `Float` values can be compared with each other (integer is converted to float)
- All other type combinations require exact type matches for comparison
- Comparisons involving `Null` follow NULL semantics (generally returning false)

## SQL Support

### Supported SQL Statements

Sqawk currently supports the following SQL operations:

| Statement | Description | Example |
|-----------|-------------|---------|
| `SELECT` | Query data from tables | `SELECT * FROM users WHERE age > 30` |
| `INSERT` | Add new rows to tables | `INSERT INTO users VALUES (4, 'Dave', 28)` |
| `DELETE` | Remove rows from tables | `DELETE FROM users WHERE age < 18` |

### SELECT Statement

The `SELECT` statement supports:

- Column selection (`SELECT col1, col2, ...`)
- Wildcard selection (`SELECT *`)
- `WHERE` clause for filtering rows
- Single table queries only (no JOINs)

### INSERT Statement

The `INSERT` statement supports:

- Direct value insertion with the `VALUES` clause
- All columns must be provided in the correct order
- No column mapping or partial column insertion

### DELETE Statement

The `DELETE` statement supports:

- Deleting all rows from a table (`DELETE FROM table`)
- Conditional deletion with `WHERE` clause (`DELETE FROM table WHERE condition`)

## Comparison Operators

Sqawk supports the standard SQL comparison operators:

| Operator | Description | Example |
|----------|-------------|---------|
| `=` | Equal to | `WHERE age = 30` |
| `!=` | Not equal to | `WHERE name != 'Alice'` |
| `>` | Greater than | `WHERE age > 18` |
| `<` | Less than | `WHERE price < 100` |
| `>=` | Greater than or equal to | `WHERE age >= 21` |
| `<=` | Less than or equal to | `WHERE score <= 5` |

### Operator Behavior with Different Types

- **Numeric Comparisons**: Both `Integer` and `Float` values can be compared using any comparison operator
- **String Comparisons**: String values are compared lexicographically
- **Boolean Comparisons**: Boolean values can be compared for equality/inequality only
- **NULL Handling**: Comparisons with NULL follow SQL NULL semantics (generally returning false)

## Architecture

The in-memory database system consists of these primary components:

### Table Module

The `Table` struct represents an in-memory table with:
- Column metadata
- Row data
- Methods for accessing and manipulating rows
- Projection capabilities (selecting subsets of columns)

### SQL Executor

The `SqlExecutor` implements SQL parsing and execution:
- Uses `sqlparser` crate to parse SQL statements
- Converts parsed AST to operations on in-memory tables
- Handles WHERE clause evaluation
- Tracks which tables have been modified

### CSV Handler

The `CsvHandler` manages I/O between CSV files and in-memory tables:
- Loads CSV files into tables
- Extracts column names from header rows
- Writes modified tables back to CSV files
- Maintains a registry of loaded tables

## Performance Considerations

Sqawk's in-memory database is optimized for:

- **Fast Loading**: CSV files are parsed directly into memory
- **Efficient Filtering**: WHERE clauses are applied in a single pass
- **Low Memory Overhead**: Simple data structures minimize memory usage
- **Zero Configuration**: No setup required, works directly on CSV files

For larger datasets, consider:
- The entire dataset must fit in memory
- Complex queries may require multiple passes over the data
- Write operations create new copies of the data in memory

## Limitations

Current limitations of the database system:

- **No Indices**: All operations scan the entire table
- **No Joins**: Only single-table queries are supported
- **Limited Expression Support**: Complex expressions in WHERE clauses are not supported
- **No Aggregations**: GROUP BY, HAVING, and aggregate functions are not implemented
- **No Transactions**: All operations are applied immediately
- **No Schema Enforcement**: Column types are inferred, not declared
- **No Constraints**: Uniqueness, foreign keys, etc. are not supported

---

*This documentation describes the current state of the Sqawk in-memory database as of the project's current version. Future versions may add additional capabilities.*