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
   - [Interactive Mode (-i)](#interactive-mode--i)
   - [Write Flag (--write)](#write-flag---write)
   - [Field Separator Option (-F)](#field-separator-option--f)
   - [Table Definition Option (--tabledef)](#table-definition-option---tabledef)
   - [Verbose Mode (-v)](#verbose-mode--v)
   - [Help (--help)](#help---help)
5. [Working with Files](#working-with-files)
   - [File Format Support](#file-format-support)
   - [Reading Standard Input](#reading-standard-input)
   - [Table Naming](#table-naming)
   - [Handling Multiple Files](#handling-multiple-files)
   - [File Writeback Behavior](#file-writeback-behavior)
   - [Defining Schemas with CREATE TABLE](#defining-schemas-with-create-table)
6. [Examples](#examples)
   - [Basic Queries](#basic-queries)
   - [Aggregation and Grouping](#aggregation-and-grouping)
   - [Distinct Values](#distinct-values)
   - [Joins](#joins)
   - [Data Modification](#data-modification)
   - [String Functions](#string-functions)
   - [System Files](#system-files---tabledef)
   - [Output Redirection](#output-redirection)
   - [Interactive REPL Session](#interactive-repl-session)
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
- Subqueries (scalar, `IN`, `EXISTS`, correlated) and derived tables
- Set operations (UNION, INTERSECT, EXCEPT) and window functions
- Interactive REPL mode for SQL exploration and execution
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
   git clone https://github.com/jgarzik/sqawk.git
   cd sqawk
   ```

2. Build and install using Cargo:
   ```sh
   cargo build --release
   cargo install --path .
   ```

### Installation Notes

- Sqawk is designed to work on Linux, macOS, and Windows systems where Rust is supported
- Building requires Rust 1.88 or newer
- The binaries are self-contained with no runtime dependencies
- Installing the crate provides **two** commands: `sqawk`, and `tsq`, a
  generator of deterministic multi-table CSV data and SQL queries for
  exercising sqawk. Run `tsq --help`, or see the
  [project README](../README.md#tsq---test-data-generator)

## Getting Started

### Basic Command Structure

The basic structure of a Sqawk command is:

```
sqawk [OPTIONS] <FILES>...
```

Where:
- `OPTIONS` include SQL statements and other flags
- `FILES` are the delimiter-separated files to process. At least one is
  required. Each may be written as `[table_name=]file_path`, and a path of `-`
  reads [standard input](#reading-standard-input)

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
3. The first row is treated as column headers (always for `.csv` files and
   standard input; other files are checked for a missing header -- see
   [Header Rows](#header-rows))
4. Data types are inferred per cell: an empty field is NULL, a number written
   in its canonical form (`42`, `1.5`) is a number, and everything else is
   text, kept exactly as written (`007`, `1.50`, `1e3`, `true`)
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

### Interactive Mode (-i)

The `-i` (or `--interactive`) option launches Sqawk in REPL (Read-Eval-Print Loop) mode, providing an interactive SQL shell similar to the sqlite3 command-line utility:

```sh
sqawk -i data.csv employees.csv
```

This opens an interactive SQL prompt where you can:
- Enter SQL statements and execute them immediately
- Explore tables and schema interactively
- Execute multiple statements in sequence
- Toggle settings like write mode

**REPL Commands:**

| Command | Description |
|---------|-------------|
| `.cd DIRECTORY` | Change the working directory |
| `.changes [on\|off]` | Show the number of rows changed by each statement |
| `.exit [CODE]` | Exit the REPL, optionally with an exit code |
| `.help` | List the available commands |
| `.load [TABLE=]FILE` | Load `FILE`, optionally under the name `TABLE` |
| `.print STRING...` | Print a literal string |
| `.quit` | Exit the REPL |
| `.save [TABLE]` | Save changes to all modified tables, or just `TABLE` |
| `.schema [TABLE]` | Show the schema for `TABLE`, or for every table |
| `.show [WHAT]` | Show current settings and status |
| `.stats [on\|off]` | Toggle statistics display |
| `.tables [PATTERN]` | List tables, optionally matching a LIKE pattern |
| `.version` | Show source, library and compiler versions |
| `.write [on\|off]` | Toggle writing changes to files (default off) |

`.help` inside the REPL always lists the authoritative set.

**Example REPL Session:**

```
$ sqawk -i employees.csv sales.csv
Welcome to Sqawk interactive mode!
Enter SQL statements or commands, terminate with ';'.
Type .help for available commands.
sqawk> SELECT name, department FROM employees WHERE salary > 70000;
name,department
Alice,Engineering
Charlie,Engineering

sqawk> .tables
Tables:
  employees
  sales

sqawk> .schema employees
CREATE TABLE employees (
  id TEXT,
  name TEXT,
  department TEXT,
  salary TEXT
);

sqawk> .changes on
Changes display enabled

sqawk> UPDATE employees SET salary = salary * 1.1 WHERE department = 'Engineering';
2 rows affected
Changes not saved: use .write to save changes to files

sqawk> .save employees
Changes saved to table 'employees'

sqawk> .exit
```

Two things in that transcript are worth calling out:

- `.schema` reports every column as `TEXT`. Loaded files carry no declared
  schema -- types are inferred per value at comparison time, not per column, so
  there is no column type for `.schema` to report. See
  [Type coercion](sql_reference.md#type-coercion).
- Row counts are only printed after `.changes on`; the default is off.

The interactive mode is particularly useful for:
- Exploring datasets without writing multiple commands
- Testing and refining complex queries
- Performing multiple operations in sequence
- Learning and experimenting with SQL

### Write Flag (--write)

By default, Sqawk doesn't modify your files, only reading from them and displaying results. To save changes back to the original files, use the `--write` flag (or its shorthand `-w`):

```sh
sqawk -s "DELETE FROM data WHERE status = 'expired'" data.csv --write
```

Important notes about the write behavior:
- Only tables that were actually modified by an operation (INSERT, UPDATE, DELETE) are saved
- Cells the statement did not change are written back exactly as they were
  read: `007` stays `007`, `1.50` stays `1.50`, quoted text keeps its quotes
  where they are needed. The one normalisation is that quotes a field did
  not need (`"plain"`) are dropped
- The delimiter, the header line (or its absence), CRLF line endings, the
  file's permissions and row order are preserved. Writing through a symlink
  updates the file it points to and leaves the link in place
- NULL is written as an empty field
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
- Files ending in `.csv` default to comma; **any other extension defaults to
  tab**, `.tsv` and `.txt` alike. A comma-separated `.txt` file therefore needs
  an explicit `-F,`. A file with no extension at all defaults to comma
- Common separators include tab (`\t`), comma (`,`), colon (`:`), and pipe (`|`)
- The separator must be a single ASCII character other than a quote or line
  break; the two-character escape `\t` means tab. Anything else (including an
  empty `-F ''`) is rejected with an error
- `-F` applies to every input file in the invocation, not per file
- The input separator is preserved when writing a table back with `--write`
- Query results on stdout use `-F` when it is given, and comma otherwise --
  reading a tab-separated file without `-F` prints comma-separated results

### Table Definition Option (--tabledef)

The `--tabledef` option allows you to define column names for files that don't have header rows, such as system files like `/etc/passwd`:

```sh
# Process /etc/passwd with meaningful column names
sqawk -F: --tabledef=passwd:username,password,uid,gid,gecos,home,shell \
    -s "SELECT username, home FROM passwd WHERE uid >= 1000" \
    passwd=/etc/passwd
```

Format: `--tabledef=table_name:col1,col2,col3,...`

This is useful for:
- System files like `/etc/passwd`, `/etc/group`, `/etc/hosts`
- Log files with fixed column formats
- Any file without a header row

Multiple table definitions can be provided:

```sh
sqawk -F: \
    --tabledef=passwd:username,password,uid,gid,gecos,home,shell \
    --tabledef=group:groupname,password,gid,members \
    -s "SELECT username, groupname FROM passwd, group WHERE passwd.gid = group.gid" \
    passwd=/etc/passwd group=/etc/group
```

### Header Rows

- A `.csv` file's first line is always its header, as is the first line of
  standard input. Lines starting with `#` are data
- Any other file (`.tsv`, `.txt`, no extension) is checked: if its first line
  looks like data rather than column names -- a number, a path starting with
  `/`, `*`, `root` or `nobody` -- it is treated as data and the columns are
  named `a`, `b`, `c`, ... Lines starting with `#` are comments and skipped
- `--no-header` says no input file has a header row: the first line is data
  and the columns are named `a`, `b`, `c`, ... (or by `--tabledef`)
- `--tabledef` likewise means the file has no header row

A table loaded without a header row is written back without one.

```sh
sqawk --no-header -s "SELECT a, b FROM pairs WHERE b > 10" pairs.csv
```

### Verbose Mode (-v)

The verbose mode provides additional information about the operations being performed:

```sh
sqawk -s "SELECT * FROM data WHERE id > 1000" data.csv -v
```

Verbose output goes to standard error, so it never mixes with query results.
It includes:
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
2. Files with a `.csv` extension use comma as the default delimiter
3. Files with **any other** extension -- `.tsv`, `.txt`, `.log` -- default to tab
4. Files with **no** extension default to comma, as does standard input

#### Comment Support

Delimited files other than `.csv` may contain comment lines. Lines that begin
with `#` are ignored when such a file is read. In a `.csv` file, a `#` line is
an ordinary record:

```
# This line is a comment and will be ignored
id,name,department,salary
1,Alice,Engineering,75000
# Another comment line
2,Bob,Marketing,65000
```

Comment support is useful for:
- Adding metadata or documentation within data files
- Temporarily excluding rows from processing
- Adding version information or data provenance details

#### Rows With The Wrong Field Count

- A row with **too few** fields is padded with NULL
- A row with **too many** fields is an error naming the line, since there is
  no column to hold the extra values and dropping them would lose data

```csv
a,b,c
1,2        # loads as 1, 2, NULL
1,2,3,4    # error: record has 4 fields, but the table has 3 columns
```

Padding is silent. If a query returns unexpected NULLs in the last columns,
check the input for short rows.

Fields are read per RFC 4180: a quoted field may contain the delimiter, line
breaks, and doubled quotes (`"say ""hi"""`). Fields are never trimmed, and a
UTF-8 byte order mark at the start of the file is ignored.

Completely empty input is a different matter. The first line is the header, so
a file or pipe with no content has no column names and is rejected with *"has
no header row"* rather than loading an unusable table.

### Defining Schemas with CREATE TABLE

While Sqawk automatically infers types from input files, you can explicitly define table schemas using the CREATE TABLE statement. This is useful for:

- Creating empty tables without loading from a file
- Ensuring specific column types for data validation
- Defining output file formats for new tables

#### Basic CREATE TABLE Syntax

```sql
CREATE TABLE table_name (
    column1 data_type,
    column2 data_type,
    ...
) [LOCATION 'file_path'] 
  [STORED AS file_format]
  [WITH (option_name='option_value', ...)]
```

Example:

```sql
-- Create a new employee table with specific column types
CREATE TABLE employees (
    id INT,
    name TEXT,
    department TEXT,
    salary FLOAT
) LOCATION './data/employees.csv'
  STORED AS TEXTFILE
  WITH (DELIMITER=',');
```

#### Supported Data Types

Sqawk supports these data types in CREATE TABLE statements:

- `INT` or `INTEGER`: For whole numbers
- `FLOAT` or `REAL`: For decimal numbers
- `TEXT` or `STRING`: For text values
- `BOOLEAN`: For true/false values

#### Setting File Location and Format

Use the LOCATION clause to specify where the table data should be stored:

```sql
CREATE TABLE logs (
    timestamp TEXT,
    level TEXT,
    message TEXT
) LOCATION './logs/app.log'
```

Currently, only TEXTFILE format is supported:

```sql
CREATE TABLE users (
    id INT,
    name TEXT,
    email TEXT
) LOCATION './users.csv' STORED AS TEXTFILE
```

#### Specifying Custom Delimiters

For non-CSV formats, specify the delimiter with the WITH clause:

```sql
CREATE TABLE server_logs (
    timestamp TEXT,
    server_id TEXT,
    status INT,
    response_time FLOAT
) LOCATION './logs/server.log'
  STORED AS TEXTFILE
  WITH (DELIMITER='\t')
```

#### Working with Created Tables

After creating a table, you can insert data and query it:

```sql
-- Create table
CREATE TABLE products (id INT, name TEXT, price FLOAT);

-- Insert data
INSERT INTO products VALUES (1, 'Keyboard', 49.99), (2, 'Mouse', 29.99);

-- Query data
SELECT * FROM products WHERE price < 40;
```

Use the `--write` flag to save changes to the specified location:

```sh
sqawk -s "CREATE TABLE data (id INT, value FLOAT) LOCATION './output.csv';
          INSERT INTO data VALUES (1, 10.5), (2, 20.7);
          SELECT * FROM data;" seed.csv --write
```

Note the trailing `seed.csv`. At least one input file is always required, even
when every table in the query is created by the statement itself; `sqawk` with
no file operand exits with a usage error.

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
  - Unchanged cells are written back byte for byte
  - Original delimiter, header line (or its absence) and line endings are preserved
  - Column and row order remain the same
  - NULL is written as an empty field
  - The file keeps its permissions; a symlink keeps pointing at the rewritten file

Example of safe write behavior:

```sh
# Only data.csv is rewritten; lookup.csv was read by the SELECT but never modified
sqawk -s "DELETE FROM data WHERE code NOT IN (SELECT code FROM lookup)" \
      -s "SELECT * FROM data" \
      data.csv lookup.csv --write
```

`UPDATE ... FROM other_table` is **not** supported, and is silently accepted
without doing anything -- no error, no rows changed. A cross-table update has
to go through a correlated subquery in the SET expression:

```sh
sqawk -s "UPDATE data SET category =
            (SELECT category FROM lookup WHERE lookup.code = data.code)" \
      data.csv lookup.csv --write
```

A row with no matching lookup row gets NULL (written back as an empty field);
add `WHERE code IN (SELECT code FROM lookup)` to leave such rows untouched.

## Examples

### Basic Queries

```sh
# Count records
sqawk -s "SELECT COUNT(*) FROM data" data.csv

# Filter rows
sqawk -s "SELECT * FROM data WHERE status = 'active'" data.csv

# Sort results
sqawk -s "SELECT * FROM data ORDER BY date DESC" data.csv

# Limit output
sqawk -s "SELECT * FROM data LIMIT 10" data.csv
```

### Aggregation and Grouping

```sh
# Basic statistics
sqawk -s "SELECT MIN(value), MAX(value), AVG(value) FROM data" data.csv

# Group by with count
sqawk -s "SELECT category, COUNT(*) FROM data GROUP BY category" data.csv

# Multiple aggregates
sqawk -s "SELECT region, COUNT(*) AS orders, SUM(amount) AS total
          FROM orders GROUP BY region ORDER BY total DESC" orders.csv
```

### Distinct Values

```sh
# Unique values in a column
sqawk -s "SELECT DISTINCT category FROM data" data.csv

# Count unique values
sqawk -s "SELECT COUNT(DISTINCT category) FROM data" data.csv

# Unique combinations
sqawk -s "SELECT DISTINCT department, role FROM employees" employees.csv
```

### Joins

```sh
# Inner join
sqawk -s "SELECT u.name, o.date FROM users u
          INNER JOIN orders o ON u.id = o.user_id" users.csv orders.csv

# Left join (include all users, even without orders)
sqawk -s "SELECT u.name, o.date FROM users u
          LEFT JOIN orders o ON u.id = o.user_id" users.csv orders.csv

# Three-table join
sqawk -s "SELECT u.name, p.name AS product, o.date
          FROM users u
          INNER JOIN orders o ON u.id = o.user_id
          INNER JOIN products p ON o.product_id = p.id" \
      users.csv orders.csv products.csv
```

### Data Modification

```sh
# Update values
sqawk -s "UPDATE data SET status = 'archived' WHERE date < '2023-01-01'" data.csv --write

# Delete rows
sqawk -s "DELETE FROM data WHERE status = 'expired'" data.csv --write

# Insert new rows
sqawk -s "INSERT INTO data VALUES (100, 'New Item', 'active')" data.csv --write
```

### String Functions

```sh
# Case conversion
sqawk -s "SELECT UPPER(name), LOWER(email) FROM contacts" contacts.csv

# Substring extraction
sqawk -s "SELECT SUBSTR(date, 1, 7) AS month FROM transactions" transactions.csv

# String replacement
sqawk -s "UPDATE data SET phone = REPLACE(phone, '-', '')" data.csv --write

# List each department's members on one line
sqawk -s "SELECT dept, GROUP_CONCAT(name, '; ') FROM staff GROUP BY dept" staff.csv
```

### SQL Semantics

Sqawk answers as SQLite does, with a few documented exceptions (see "Differences
from SQLite" in the [SQL reference](sql_reference.md)):

- Integer arithmetic stays integer: `5 / 2` is `2`, `7 % 3` is `1`; overflow
  becomes a real, and division by zero is NULL.
- `LIKE` is case-insensitive for ASCII letters; `ILIKE` folds all of Unicode.
- String functions count characters, not bytes; `'a' || NULL` is NULL.
- Dates are UTC: `DATE('now')`, `DATE(d, '+1 month')`, `STRFTIME('%Y', d)`.
- Syntax that parses but is not implemented (`RETURNING`, `WITH`,
  `UPDATE ... FROM`, ...) is an error, never silently ignored.

### System Files (--tabledef)

```sh
# Query /etc/passwd
sqawk -F: --tabledef=passwd:user,pass,uid,gid,gecos,home,shell \
    -s "SELECT user, home FROM passwd WHERE uid >= 1000" \
    passwd=/etc/passwd

# Query /etc/hosts
sqawk --tabledef=hosts:ip,hostname \
    -s "SELECT * FROM hosts WHERE ip LIKE '192.168.%'" \
    hosts=/etc/hosts
```

### Output Redirection

```sh
# Export filtered data to new file
sqawk -s "SELECT * FROM data WHERE region = 'North'" data.csv > north_data.csv

# Deduplicate to new file
sqawk -s "SELECT DISTINCT * FROM data" data.csv > deduped.csv

# Convert TSV to CSV: a .tsv is parsed as tab-separated, and stdout falls back
# to comma because -F was not given
sqawk -s "SELECT * FROM data" data.tsv > data.csv
```

The output delimiter is whatever `-F` says, and comma when `-F` is absent. It
does not follow the input file's delimiter, which is what makes the conversion
above work.

Output is a well-formed delimited file: a field containing the delimiter, a
quote or a line break, or with leading or trailing spaces, is quoted
(RFC 4180, with embedded quotes doubled), and NULL is an empty field. A row
consisting of a single NULL prints as `""` so that it is not a blank line.
When the reader goes away (`sqawk ... | head`), sqawk stops printing quietly
and exits successfully.

### Reading Standard Input

A file operand of `-` reads standard input, so sqawk can sit in the middle of a
pipeline:

```sh
# Bare "-" loads stdin as a table called "stdin"
cat employees.csv | sqawk -s "SELECT name FROM stdin WHERE salary > 70000" -

# Name it explicitly with table_name=-
curl -s https://example.com/data.csv |
    sqawk -s "SELECT COUNT(*) FROM remote" remote=-

# stdin mixes freely with files on disk
cat orders.csv | sqawk -s "SELECT u.name, o.total
                           FROM stdin o JOIN users u ON u.id = o.user_id" \
                       - users.csv
```

Points to note:

- Piped input is treated as **comma-separated**, matching what sqawk writes to
  stdout, so `sqawk ... | sqawk ... -` round-trips. `-F` overrides it
- Standard input can only be read once; a second `-` is an error
- `--tabledef` works on stdin exactly as on a file, keyed by the table name
  (`--tabledef=stdin:col1,col2` for a bare `-`)
- A stdin table has no file to write back to. `--write` on one is an error
  rather than a silent no-op; redirect the query's output instead
- `-` cannot be combined with `-i`, and `.load -` inside the REPL is refused
  for the same reason: the REPL reads its own commands from standard input,
  and reading a table from it would swallow the rest of the session

### Interactive REPL Session

```sh
sqawk -i sales.csv customers.csv
```

```
sqawk> .tables
Tables:
  customers
  sales

sqawk> .schema sales
CREATE TABLE sales (
  id TEXT,
  customer_id TEXT,
  amount TEXT
);

sqawk> SELECT COUNT(*) FROM sales;
COUNT
1250

sqawk> SELECT c.name, SUM(s.amount) AS total
       FROM customers c JOIN sales s ON c.id = s.customer_id
       GROUP BY c.name ORDER BY total DESC LIMIT 3;
name,total
Enterprise Corp,58750.25
Acme Inc,45620.75
Globex,31004.10

sqawk> .exit
```

An unaliased aggregate takes the function's name as its column header --
`COUNT`, `SUM`, `AVG` -- so use `AS` when the header matters downstream.

## Working with Large Files

Sqawk holds a whole table in memory at once, so the dataset must fit, which is
worth planning for with large files:

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
   sqawk -s "SELECT a.id, b.name FROM large_a a INNER JOIN large_b b ON a.id = b.id WHERE a.region = 'West'" large_a.csv large_b.csv
   ```

## Troubleshooting

**Common Issues and Solutions:**

1. **"Table not found" error**:
   - Check that the filename matches the table name in your SQL
   - If using custom table names, verify the syntax: `tablename=filename.csv`
   - Ensure file paths are correct and files are accessible

2. **Delimiter issues**:
   - Use the `-F` option to specify the correct delimiter
   - For tab-delimited files, use `-F '\t'`
   - Ensure consistent delimiters throughout your files

3. **Unexpected results from comparisons**:
   - Types are inferred per value, so one column can hold numbers and text
   - Comparing a number against a numeric string coerces and compares
     numerically, so `WHERE salary > '60000'` behaves as you would expect
   - Use explicit casts when you want to force one interpretation:
     `CAST(value AS INT)`
   - See the SQL reference for the full coercion and NULL rules

4. **Rows unexpectedly missing from results**:
   - An empty field reads as NULL, and any comparison involving NULL is
     UNKNOWN, so such rows satisfy neither `x > 5` nor `x <= 5`
   - This follows SQL, and is not an error -- use `IS NULL` / `IS NOT NULL`
     to test for NULL explicitly
   - `COUNT(*)` counts rows while `COUNT(col)` counts non-NULL values

5. **CSV parsing errors with malformed rows**:
   - Error messages about "field count mismatch" indicate rows with inconsistent numbers of fields
   - Error messages include line numbers to help locate problematic rows
   - Common causes include:
     - Missing fields or extra delimiters
     - Improperly escaped quotes inside fields
     - Newlines within quoted fields
   - Use the error recovery options described in the File Format Support section to handle malformed rows

6. **Issues with comment lines**:
   - Comments must start at the beginning of a line with the comment character
   - Comment characters appearing within data (not at the start of a line) are treated as regular data
   - If you're seeing unexpected parsing errors, check if comment lines are properly formatted

7. **Memory limitations**:
   - If processing very large files, filter data early in your queries
   - Consider processing in batches or using more targeted queries
   - Select only the columns you need rather than using SELECT *

8. **Changes not saved**:
   - Remember to use the `--write` flag to save changes
   - Only modified tables are written back
   - Check verbose output (`-v`) to confirm which tables were modified

9. **SQL syntax errors**:
   - Try running your query in interactive mode to get immediate feedback
   - Use the `-v` verbose flag to see the exact SQL being executed
   - Verify SQL statement syntax, particularly quotes, parentheses, and required clauses

10. **Special characters in files**:
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
5. **Chain SQL statements** with `-s` or `;` when it reads more clearly; each
   statement produces its own result and sees the effects of the ones before it
6. **Test on sample data** before processing large files

### Additional Resources

- [SQL Language Reference](sql_reference.md) - Complete guide to Sqawk's SQL dialect
- [GitHub Repository](https://github.com/jgarzik/sqawk) - Source code and issue tracking
- [Release Notes](https://github.com/jgarzik/sqawk/releases) - Latest features and bug fixes

---

*This user guide describes Sqawk as of its current version. Features and behavior may change in future releases.*