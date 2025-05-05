# Sqawk User Guide

## Table of Contents

1. [Introduction](#introduction)
2. [Installation](#installation)
   - [From Cargo (Recommended)](#from-cargo-recommended)
   - [Building from Source](#building-from-source) 
   - [Installation Notes](#installation-notes)
3. [Getting Started](#getting-started)
   - [Basic Command Structure](#basic-command-structure)
   - [Your First Sqawk Command](#your-first-sqawk-command)
   - [How Sqawk Processes Files](#how-sqawk-processes-files)
4. [Command Line Options](#command-line-options)
   - [SQL Statement Option (-s)](#sql-statement-option--s)
   - [Write Flag (--write)](#write-flag---write)
   - [Field Separator Option (-F)](#field-separator-option--f)
   - [Verbose Mode (-v)](#verbose-mode--v)
   - [Help (--help)](#help---help)
5. [Working with Files](#working-with-files)
   - [File Format Support](#file-format-support)
   - [Table Naming](#table-naming)
   - [Handling Multiple Files](#handling-multiple-files)
   - [File Writeback Behavior](#file-writeback-behavior)
6. [Common Usage Patterns](#common-usage-patterns)
   - [Data Exploration](#data-exploration)
   - [Data Cleanup](#data-cleanup)
   - [Data Transformation](#data-transformation)
   - [Joining Data from Multiple Files](#joining-data-from-multiple-files)
   - [Generating Reports](#generating-reports)
7. [Working with Large Files](#working-with-large-files)
8. [Troubleshooting](#troubleshooting)
9. [Appendices](#appendices)
   - [Comparing with Other Tools](#comparing-with-other-tools)
   - [Best Practices](#best-practices)
   - [Additional Resources](#additional-resources)

## Introduction

Sqawk is an SQL-based command-line tool for processing delimiter-separated files (CSV, TSV, etc.), inspired by the classic `awk` command. It combines the powerful query capabilities of SQL with the simplicity of command-line tools, allowing you to analyze and transform data without setting up a database server.

**Key Features:**

- Process CSV, TSV, and custom-delimited files with SQL queries
- No database setup or schema definition required
- Automatic type inference and cross-file operations
- Powerful SQL dialect including joins, sorting, filtering, and aggregations
- Safe operation with explicit write-back control

Sqawk is designed for data analysts, developers, system administrators, and anyone who works with tabular data files and wants the power of SQL without the overhead of a full database system.

## Installation

### From Cargo (Recommended)

The simplest way to install Sqawk is through Cargo, Rust's package manager:

```sh
cargo install sqawk
```

This will download, compile, and install the latest version of Sqawk from crates.io.

### Building from Source

To build from source:

1. Clone the repository:
   ```sh
   git clone https://github.com/username/sqawk.git
   cd sqawk
   ```

2. Build and install using Cargo:
   ```sh
   cargo build --release
   cargo install --path .
   ```

### Installation Notes

- Sqawk is designed to work on Linux, macOS, and Windows systems where Rust is supported
- The binary is self-contained with no runtime dependencies
- After installation, the `sqawk` command should be available in your PATH

## Getting Started

### Basic Command Structure

The basic structure of a Sqawk command is:

```
sqawk [OPTIONS] [FILES]
```

Where:
- `OPTIONS` include SQL statements and other flags
- `FILES` are the delimiter-separated files to process

### Your First Sqawk Command

Let's start with a simple example. If you have a CSV file named `employees.csv` with this content:

```
id,name,department,salary
1,Alice,Engineering,75000
2,Bob,Marketing,65000
3,Charlie,Engineering,80000
```

You can query it with:

```sh
sqawk -s "SELECT * FROM employees WHERE department = 'Engineering'" employees.csv
```

The output will be:

```
id,name,department,salary
1,Alice,Engineering,75000
3,Charlie,Engineering,80000
```

### How Sqawk Processes Files

When you run a Sqawk command:

1. Sqawk loads each specified file into memory as a table
2. Table names are derived from file names (without extensions) or can be explicitly assigned
3. The first row is treated as column headers
4. Data types are automatically inferred (numbers, strings, etc.)
5. SQL queries are executed against the in-memory tables
6. Results are displayed on the console
7. If `--write` is specified, modified tables are saved back to the source files

## Command Line Options

### SQL Statement Option (-s)

The `-s` option specifies an SQL statement to execute:

```sh
sqawk -s "SELECT * FROM data WHERE value > 100" data.csv
```

You can provide multiple SQL statements by using multiple `-s` options:

```sh
sqawk -s "SELECT COUNT(*) FROM data" -s "SELECT AVG(value) FROM data" data.csv
```

Statements are executed in sequence, with each operating on the current state of the tables.

### Write Flag (--write)

By default, Sqawk doesn't modify your files, only reading from them and displaying results. To save changes back to the original files, use the `--write` flag (or its shorthand `-w`):

```sh
sqawk -s "DELETE FROM data WHERE status = 'expired'" data.csv --write
```

Important notes about the write behavior:
- Only tables that were actually modified by an operation (INSERT, UPDATE, DELETE) are saved
- The original file format and delimiter are preserved
- Column order and headers are maintained
- Without `--write`, your files remain untouched regardless of the SQL operations

### Field Separator Option (-F)

The `-F` option allows you to specify a custom field separator for your files:

```sh
# Process a tab-delimited file
sqawk -F '\t' -s "SELECT * FROM data" data.tsv

# Process a pipe-delimited file
sqawk -F '|' -s "SELECT * FROM logs" logs.txt
```

Notes on field separators:
- The default separator is comma (,) for CSV files
- For files with .tsv extension, tab is used as the default separator
- Common separators include tab (`\t`), comma (`,`), colon (`:`), and pipe (`|`)
- The specified separator is also used when writing back to files

### Verbose Mode (-v)

The verbose mode provides additional information about the operations being performed:

```sh
sqawk -s "SELECT * FROM data WHERE id > 1000" data.csv -v
```

Verbose output includes:
- SQL statements being executed
- Number of rows affected or returned
- Table loading information
- Write status (whether changes were saved)

This mode is particularly useful for debugging or understanding exactly what Sqawk is doing with your data.

### Help (--help)

For a quick reference of all available options:

```sh
sqawk --help
```

## Working with Files

### File Format Support

Sqawk supports various delimiter-separated file formats:

- **CSV files**: Standard comma-separated values
- **TSV files**: Tab-separated values
- **Custom-delimited files**: Files with any single-character delimiter

File format detection follows these rules:
1. If a specific delimiter is provided with `-F`, it's used regardless of file extension
2. Files with `.csv` extension use comma as the default delimiter
3. Files with `.tsv` extension use tab as the default delimiter
4. Other file extensions default to comma unless specified otherwise

### Table Naming

By default, the table name is derived from the filename (without extension):

```sh
sqawk -s "SELECT * FROM employees" employees.csv  # Table name is "employees"
```

You can explicitly specify a table name:

```sh
sqawk -s "SELECT * FROM staff" staff=employees.csv  # Table name is "staff"
```

This is particularly useful when:
- Working with files that have non-SQL-friendly names
- Wanting more descriptive table names than the filename
- Loading multiple files that would otherwise have name conflicts

### Handling Multiple Files

Sqawk can process multiple files in a single command:

```sh
sqawk -s "SELECT users.name, orders.date FROM users, orders WHERE users.id = orders.user_id" users.csv orders.csv
```

When working with multiple files:
- Each file is loaded as a separate table
- Tables can be joined or queried independently
- Column names should be qualified with table names to avoid ambiguity
- Multiple SQL statements can operate on different tables

### File Writeback Behavior

Sqawk follows a safe-by-default approach to file modification:

- Files are never modified unless the `--write` flag is provided
- Only tables that were actually changed are written back
- When writing back:
  - Original delimiters and formatting are preserved
  - Column order remains the same
  - Header row is preserved
  - Empty values are written as empty fields, not NULLs

Example of safe write behavior:

```sh
# This only writes back changes to data.csv, not to lookup.csv which was only read
sqawk -s "UPDATE data SET category = lookup.category FROM lookup WHERE data.code = lookup.code" -s "SELECT * FROM data" data.csv lookup.csv --write
```

## Common Usage Patterns

### Data Exploration

Quickly analyze and explore data files:

```sh
# Count the number of records
sqawk -s "SELECT COUNT(*) FROM data" data.csv

# Get basic statistics
sqawk -s "SELECT MIN(value) AS min, MAX(value) AS max, AVG(value) AS avg FROM data" data.csv

# See distribution by category
sqawk -s "SELECT category, COUNT(*) FROM data GROUP BY category ORDER BY COUNT(*) DESC" data.csv

# Find unique values in a column
sqawk -s "SELECT DISTINCT category FROM data ORDER BY category" data.csv

# Count unique values
sqawk -s "SELECT COUNT(DISTINCT category) AS unique_categories FROM data" data.csv

# Find unique combinations of columns
sqawk -s "SELECT DISTINCT department, role FROM employees" employees.csv
```

### Data Cleanup

Clean and transform data files:

```sh
# Remove duplicate records using Sqawk's DISTINCT keyword
sqawk -s "SELECT DISTINCT * FROM data" data.csv > deduped_data.csv

# Extract only unique combinations of name and email
sqawk -s "SELECT DISTINCT name, email FROM contacts" contacts.csv > unique_contacts.csv

# Delete rows with missing values
sqawk -s "DELETE FROM data WHERE email IS NULL OR email = ''" data.csv --write

# Fix casing issues
sqawk -s "UPDATE data SET name = UPPER(name)" data.csv --write
```

### Data Transformation

Transform data for analysis or export:

```sh
# Extract subset of columns
sqawk -s "SELECT id, name, email FROM contacts" contacts.csv > minimal_contacts.csv

# Reshape data by filtering and sorting
sqawk -s "SELECT * FROM data WHERE region = 'North' ORDER BY date DESC" data.csv > north_region_latest.csv

# Create derived columns
sqawk -s "SELECT id, name, salary, salary * 0.3 AS bonus FROM employees" employees.csv
```

### Joining Data from Multiple Files

Combine data from different files:

```sh
# Simple join between two files
sqawk -s "SELECT users.name, orders.product_id, orders.date FROM users INNER JOIN orders ON users.id = orders.user_id" users.csv orders.csv

# Three-way join with filtering
sqawk -s "SELECT users.name AS customer, products.name AS product, orders.date 
          FROM users 
          INNER JOIN orders ON users.id = orders.user_id 
          INNER JOIN products ON orders.product_id = products.product_id 
          WHERE orders.date > '2023-01-01'" 
      users.csv orders.csv products.csv
      
# Using DISTINCT with JOINs to find unique customer-product pairs
sqawk -s "SELECT DISTINCT users.name, products.name 
          FROM users 
          INNER JOIN orders ON users.id = orders.user_id 
          INNER JOIN products ON orders.product_id = products.product_id" 
      users.csv orders.csv products.csv
```

### Generating Reports

Create summary reports from data:

```sh
# Sales summary by region
sqawk -s "SELECT region, COUNT(*) AS order_count, SUM(amount) AS total_sales 
          FROM orders 
          GROUP BY region 
          ORDER BY total_sales DESC" 
      orders.csv

# Monthly trends
sqawk -s "SELECT SUBSTR(date, 1, 7) AS month, COUNT(*) AS transaction_count 
          FROM transactions 
          GROUP BY month 
          ORDER BY month" 
      transactions.csv
```

## Working with Large Files

Sqawk loads all data into memory, which provides excellent performance but requires consideration when working with large files:

**Tips for handling large files:**

1. **Filter early**: When possible, use WHERE clauses to reduce the working dataset
   ```sh
   sqawk -s "SELECT * FROM large_data WHERE date > '2023-01-01'" large_data.csv
   ```

2. **Select only needed columns**: Minimize memory usage by selecting only required columns
   ```sh
   sqawk -s "SELECT id, name FROM large_data" large_data.csv
   ```

3. **Process in batches**: Split large files and process them in segments
   ```sh
   # Process first using head command (Unix/Linux)
   head -n 1000000 large_data.csv > batch1.csv
   sqawk -s "SELECT * FROM batch1 WHERE value > 100" batch1.csv
   ```

4. **Monitor memory usage**: Particularly when joining large tables, be aware of memory constraints
   ```sh
   # Using a more targeted join condition reduces memory requirements
   sqawk -s "SELECT a.id, b.name FROM large_a INNER JOIN large_b ON a.id = b.id WHERE a.region = 'West'" large_a.csv large_b.csv
   ```

## Troubleshooting

**Common Issues and Solutions:**

1. **"Table not found" error**:
   - Check that the filename matches the table name in your SQL
   - If using custom table names, verify the syntax: `tablename=filename.csv`

2. **Delimiter issues**:
   - Use the `-F` option to specify the correct delimiter
   - For tab-delimited files, use `-F '\t'`
   - Ensure consistent delimiters throughout your files

3. **Type conversion errors**:
   - Sqawk automatically infers types but sometimes needs hints
   - Use explicit casts in SQL when needed: `CAST(value AS INT)`

4. **Memory limitations**:
   - If processing very large files, filter data early in your queries
   - Consider processing in batches or using more targeted queries

5. **Changes not saved**:
   - Remember to use the `--write` flag to save changes
   - Only modified tables are written back

6. **Special characters in files**:
   - For files with quotes or special characters, Sqawk follows CSV escaping rules
   - If encountering parsing issues, check for malformed CSV data

For more help, use the verbose mode (`-v`) to see detailed information about processing.

## Appendices

### Comparing with Other Tools

**Sqawk vs. SQL Databases:**
- Sqawk: No setup, works directly with files, perfect for ad-hoc analysis
- SQL Databases: Better for persistent storage, indexing, and concurrent access

**Sqawk vs. Awk:**
- Sqawk: SQL-based, better for complex joins and aggregations
- Awk: Pattern-matching focus, better for line-by-line text processing

**Sqawk vs. CSV Processing Libraries:**
- Sqawk: Immediate SQL interface without programming
- Libraries: More flexible but require writing code

### Best Practices

1. **Start with read-only operations** before using `--write` to modify files
2. **Use version control** or backups before modifying important data files
3. **Qualify column names** with table names in multi-table queries
4. **Use verbose mode** (`-v`) when learning or debugging
5. **Chain SQL statements** for complex operations rather than using complex subqueries
6. **Test on sample data** before processing large files

### Additional Resources

- [SQL Language Reference](sql_reference.md) - Complete guide to Sqawk's SQL dialect
- [GitHub Repository](https://github.com/username/sqawk) - Source code and issue tracking
- [Release Notes](https://github.com/username/sqawk/releases) - Latest features and bug fixes

---

*This user guide describes Sqawk as of its current version. Features and behavior may change in future releases.*