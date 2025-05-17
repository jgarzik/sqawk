# Sqawk SQL Language Reference

## Introduction

Sqawk provides a powerful SQL-like query language for processing delimiter-separated files. While CSV (comma-separated values) is the default format, Sqawk also supports other delimiter-separated formats like TSV (tab-separated values) and custom delimiters. This document serves as a reference for the SQL dialect supported by Sqawk and explains its syntax, features, and limitations.

## Table of Contents

1. [Introduction](#introduction)
2. [Table Names and File Specification](#table-names-and-file-specification)
3. [File Formats and Field Separators](#file-formats-and-field-separators)
4. [Chaining SQL Statements](#chaining-sql-statements)
5. [Data Types](#data-types)
6. [SQL Statement Types](#sql-statement-types)
7. [CREATE TABLE Statement](#create-table-statement)
   - [Basic Syntax](#create-table-basic-syntax)
   - [Data Types](#create-table-data-types)
   - [Table Location and Format](#table-location-and-format)
   - [Custom Delimiters](#custom-delimiters)
8. [SELECT Statement](#select-statement)
   - [Basic Syntax](#basic-syntax)
   - [Column Selection](#column-selection)
   - [Column Aliases](#column-aliases)
   - [WHERE Clause](#where-clause)
   - [String Functions](#string-functions)
   - [ORDER BY Clause](#order-by-clause)
   - [LIMIT and OFFSET Clauses](#limit-and-offset-clauses)
   - [Aggregate Functions](#aggregate-functions)
   - [GROUP BY Clause](#group-by-clause)
   - [HAVING Clause](#having-clause)
9. [Multi-Table Operations (Joins)](#multi-table-operations-joins)
10. [INSERT Statement](#insert-statement)
11. [UPDATE Statement](#update-statement)
12. [DELETE Statement](#delete-statement)
13. [Limitations](#limitations)
14. [Writeback Behavior](#writeback-behavior)

## Table Names and File Specification

When using Sqawk, delimiter-separated files are loaded as in-memory tables. By default, the table name is derived from the filename (without the extension). You can also explicitly specify a table name:

```bash
# Default table name: "sample"
sqawk -s "SELECT * FROM sample" sample.csv

# Explicitly named table: "users"
sqawk -s "SELECT * FROM users" users=sample.csv
```

This naming flexibility allows you to:
- Use meaningful table names that differ from filenames
- Work with multiple files that would otherwise have the same derived table name
- Create more readable SQL queries with domain-specific table names

## File Formats and Field Separators

Sqawk can work with various delimiter-separated file formats, not just standard CSV files. The default behavior is to treat files as comma-separated values (CSV), but you can specify a custom field separator using the `-F` option:

```bash
# Process a tab-delimited file (TSV)
sqawk -F '\t' -s "SELECT * FROM employees WHERE salary > 70000" employees.tsv

# Process a colon-delimited file
sqawk -F ':' -s "SELECT id, name, email FROM contacts" contacts.txt

# Process a pipe-delimited file
sqawk -F '|' -s "SELECT * FROM data" data.txt
```

The `-F` option is similar to awk's field separator option, allowing Sqawk to handle a wide variety of text file formats. This capability is particularly useful when working with:

- Tab-delimited files (TSV)
- Exports from various systems that use custom delimiters
- Fixed-width files converted to a delimiter format
- Log files with specific field separators

When using the `-F` option, Sqawk will:
1. Parse the file using the specified delimiter instead of commas
2. Automatically detect and preserve the header row for column names
3. Perform the same type inference and SQL operations as with CSV files
4. Write back to the original format when using the `--write` flag

### File Format Detection

Sqawk uses the following logic to determine which file format handler to use:
- If the `-F` option is specified, the file is treated as a custom delimiter-separated file
- Files with a `.csv` extension are treated as standard CSV files
- Other file extensions without a specified delimiter are treated as tab-delimited by default

### Comment Support in CSV Files

Sqawk supports comment lines in CSV files. Lines that begin with a comment character (typically '#') are ignored during processing:

```csv
# This is a comment and will be ignored
id,name,age
1,Alice,32
# Another comment
2,Bob,25
```

Comment support is particularly useful for:
- Adding metadata or documentation within the file
- Temporarily excluding rows from processing
- Adding version information or data provenance details

### Error Recovery Options

When processing CSV or other delimiter-separated files, Sqawk can handle malformed rows in several ways:

- **Strict Mode**: By default, malformed rows (those with too few or too many fields) cause an error
- **Recovery Mode**: With appropriate options, Sqawk can:
  - Skip malformed rows entirely
  - Pad malformed rows with NULL values if they have too few fields
  - Truncate malformed rows if they have too many fields

This error recovery capability is especially useful when working with imperfect data sources where strict conformance to the expected format isn't guaranteed.

## Chaining SQL Statements

Sqawk allows you to execute multiple SQL statements in sequence, with each statement operating on the result of the previous ones:

```bash
# Execute two SQL statements in sequence
sqawk -s "DELETE FROM users WHERE inactive = true" -s "SELECT * FROM users" users.csv
```

This allows for complex operations such as:
- Modifying data and then viewing the results
- Performing multi-step data transformations
- Running sequential operations like cleanup and then analysis

Each statement executes against the in-memory state after the previous statement's execution.

## Data Types

Sqawk supports the following data types:

| Type | Description | Example | Storage |
|------|-------------|---------|---------|
| `Null` | Missing or null value | NULL | Special variant |
| `Integer` | 64-bit signed integer | 42 | i64 |
| `Float` | 64-bit floating point | 3.14 | f64 |
| `String` | UTF-8 text | "hello" | String |
| `Boolean` | True/false value | true | bool |

### Type Inference

When loading data from delimiter-separated files, Sqawk automatically infers the most appropriate type for each value:

1. First tries to parse as an `Integer`
2. If that fails, tries to parse as a `Float` 
3. If that fails, tries to parse as a `Boolean` (values like true/false, yes/no, 1/0)
4. If all else fails, stores the value as a `String`
5. Empty values are stored as `Null`

This dynamic type inference provides flexibility when working with delimiter-separated data, which typically doesn't include explicit type information. The same type inference logic applies to all file formats, whether they are CSV files, TSV files, or files with custom delimiters.

### Type Coercion in Comparisons

Sqawk implements SQL-like type coercion rules when comparing values:

- **NULL Values**: 
  - NULL equals NULL
  - NULL is less than any other value
  - Comparisons between NULL and non-NULL values generally evaluate to false

- **Numeric Comparisons**:
  - `Integer` and `Float` values can be compared directly
  - When comparing different numeric types, integers are converted to floats

- **Same-Type Comparisons**:
  - Strings are compared lexicographically (dictionary order)
  - Boolean values follow false < true

- **Different-Type Comparisons**:
  - Types follow a strict precedence order: NULL < Boolean < Number < String
  - This means:
    - Boolean values are less than any numeric or string value
    - Numbers (both Integer and Float) are less than any String value
    - Strings are greater than all other types

This type precedence system is particularly important for operations like `MIN()` and `MAX()` and when sorting values with `ORDER BY`.

## SQL Statement Types

Sqawk currently supports the following SQL statement types:

| Statement | Description | Example |
|-----------|-------------|---------|
| `SELECT` | Query data from tables | `SELECT * FROM users WHERE age > 30` |
| `CREATE TABLE` | Create a new table with a defined schema | `CREATE TABLE users (id INT, name TEXT, age INT)` |
| `INSERT` | Add new rows to tables | `INSERT INTO users VALUES (4, 'Dave', 28)` |
| `UPDATE` | Modify existing rows in tables | `UPDATE users SET age = 29 WHERE name = 'Dave'` |
| `DELETE` | Remove rows from tables | `DELETE FROM users WHERE age < 18` |

## CREATE TABLE Statement

### Create Table Basic Syntax

```sql
CREATE TABLE table_name (
    column1 data_type,
    column2 data_type,
    ...
) [LOCATION 'file_path'] 
  [STORED AS file_format]
  [WITH (option_name='option_value', ...)]
```

The CREATE TABLE statement allows you to define a new table with a specified schema. The statement requires:

- A table name
- One or more column definitions with their data types
- Optional LOCATION clause to specify where the table should be stored
- Optional STORED AS clause to specify the file format
- Optional WITH clause to specify additional table properties

Example of a basic CREATE TABLE statement:

```sql
CREATE TABLE users (
    id INT,
    name TEXT,
    email TEXT,
    age INT,
    salary FLOAT
)
```

### Create Table Data Types

Sqawk supports the following data types in CREATE TABLE statements:

| Data Type | Description | Example |
|-----------|-------------|---------|
| `INT` or `INTEGER` | 64-bit signed integer | `id INT` |
| `FLOAT` or `REAL` | 64-bit floating point | `salary FLOAT` |
| `TEXT` or `STRING` | UTF-8 text | `name TEXT` |
| `BOOLEAN` | True/false value | `active BOOLEAN` |

When defining columns, you must specify a data type for each column. This type information is used when inserting data into the table and for data validation.

```sql
CREATE TABLE products (
    product_id INT,
    name TEXT,
    description TEXT,
    price FLOAT,
    in_stock BOOLEAN
)
```

### Table Location and Format

You can specify a location for the table's data file using the LOCATION keyword, followed by a path string:

```sql
CREATE TABLE sales (
    id INT,
    date TEXT,
    amount FLOAT
) LOCATION './data/sales.csv'
```

Currently, Sqawk only supports the TEXTFILE format via the STORED AS clause:

```sql
CREATE TABLE events (
    event_id INT,
    timestamp TEXT,
    type TEXT
) LOCATION './data/events.csv' STORED AS TEXTFILE
```

### Custom Delimiters

You can specify a custom delimiter for the table using the WITH clause:

```sql
CREATE TABLE logs (
    log_id INT,
    timestamp TEXT,
    level TEXT,
    message TEXT
) LOCATION './data/logs.tsv'
  STORED AS TEXTFILE
  WITH (DELIMITER='\t')
```

This is particularly useful when working with tab-delimited files, semicolon-delimited files, or other custom formats.

Complete example with all options:

```sql
CREATE TABLE financial_data (
    account_id INT,
    transaction_date TEXT,
    amount FLOAT,
    category TEXT
) LOCATION './data/financial.csv'
  STORED AS TEXTFILE
  WITH (DELIMITER=',')
```

The CREATE TABLE statement only defines the table's structure - it doesn't load or modify any data. After creating a table, you can insert data into it using the INSERT statement.

## SELECT Statement

### Basic Syntax

```sql
SELECT [DISTINCT] [column_list | *]
FROM table_name [, table_name2, ...]
[WHERE condition]
[GROUP BY column_list]
[HAVING condition]
[ORDER BY column [ASC|DESC], ...]
[LIMIT count [OFFSET skip_count]]
```

### DISTINCT Keyword

The `DISTINCT` keyword eliminates duplicate rows from the result set:

```sql
-- Return unique combinations of name and age 
SELECT DISTINCT name, age FROM users

-- Return unique department values
SELECT DISTINCT department FROM employees

-- Can be used with aggregate functions
SELECT COUNT(DISTINCT department) AS unique_departments FROM employees
```

When using DISTINCT:
- Rows are considered identical only if all selected column values match exactly
- The comparison uses the same type system as the rest of SQL operations
- DISTINCT is applied after WHERE filtering but before ORDER BY
- DISTINCT can be used with JOINs to find unique combinations across tables
- DISTINCT is particularly useful for finding unique values or removing redundant results

DISTINCT has two different applications in SQL:

1. **Query-level DISTINCT** - Applied to the entire result set:
   ```sql
   SELECT DISTINCT department, location FROM employees
   ```

2. **Aggregate function DISTINCT** - Applied to the values within an aggregate function:
   ```sql
   SELECT COUNT(DISTINCT department) FROM employees
   ```

In the second case, the COUNT operation is performed only on unique department values, rather than counting duplicates.

### Column Selection

You can select specific columns or use a wildcard:

```sql
-- Select specific columns
SELECT name, age FROM users

-- Select all columns
SELECT * FROM users

-- Select with column qualification (table names)
SELECT users.name, orders.date FROM users, orders
```

### Column Aliases

You can provide alternative names for columns using the `AS` keyword:

```sql
-- Rename columns in the result
SELECT name AS employee_name, age AS employee_age FROM employees

-- Alias can be used without the AS keyword
SELECT name employee_name, age employee_age FROM employees

-- Aggregate functions can also have aliases
SELECT COUNT(*) AS total_count, AVG(salary) AS average_salary FROM employees
```

Column aliases are particularly useful when:
- Making result column names more descriptive
- Renaming complex expressions
- Disambiguating columns with the same name from different tables
- Using in combination with ORDER BY to sort by aliased columns

Aliases defined in the SELECT clause can be referenced in the ORDER BY clause:

```sql
-- Sort by the aliased column 'years'
SELECT name, age AS years FROM employees ORDER BY years DESC
```

### WHERE Clause

The `WHERE` clause filters rows based on conditions:

```sql
-- Equality
SELECT * FROM users WHERE name = 'Alice'

-- Inequality
SELECT * FROM users WHERE age != 30

-- Greater than
SELECT * FROM users WHERE age > 25

-- Less than
SELECT * FROM users WHERE age < 40

-- Greater than or equal to
SELECT * FROM users WHERE age >= 18

-- Less than or equal to
SELECT * FROM users WHERE age <= 65
```

### String Functions

Sqawk supports the following string functions for manipulating and comparing text data in WHERE clauses:

| Function | Description | Example |
|----------|-------------|---------|
| `UPPER(str)` | Converts a string to uppercase | `SELECT * FROM users WHERE UPPER(name) = 'ALICE'` |
| `LOWER(str)` | Converts a string to lowercase | `SELECT * FROM users WHERE LOWER(email) = 'alice@example.com'` |
| `TRIM(str)` | Removes leading and trailing whitespace | `SELECT * FROM users WHERE TRIM(username) = 'alice'` |
| `SUBSTR(str, start[, length])` | Extracts a substring | `SELECT * FROM users WHERE SUBSTR(email, 1, 5) = 'alice'` |
| `REPLACE(str, find, replace)` | Replaces all occurrences of a substring | `SELECT * FROM users WHERE REPLACE(email, '@example.com', '') = 'alice'` |

These string functions can be used in WHERE clauses to filter rows based on string manipulations:

```sql
-- Case-insensitive equality using UPPER or LOWER
SELECT * FROM users WHERE UPPER(name) = 'ALICE'

-- Working with substrings
SELECT * FROM emails WHERE SUBSTR(email, -4) = '.com'

-- Find users with trimmed whitespace
SELECT * FROM users WHERE TRIM(username) = 'alice'

-- Replace parts of strings for comparison
SELECT * FROM contacts WHERE REPLACE(phone, '-', '') = '1234567890'

-- Combining string functions
SELECT * FROM users WHERE UPPER(SUBSTR(name, 1, 1)) = 'A'
```

String functions can be nested and combined for more complex string operations. They are particularly useful for:

- Case-insensitive searching
- Pattern matching when working with text data
- Data cleaning and normalization
- Extracting portions of strings for comparison

> **Note:** Currently, string functions are only supported in WHERE clauses and cannot be used directly in the SELECT clause for projection. This limitation is documented in the [Limitations](#limitations) section.

### ORDER BY Clause

The `ORDER BY` clause sorts results by one or more columns:

```sql
-- Ascending order (default)
SELECT * FROM users ORDER BY age

-- Descending order
SELECT * FROM users ORDER BY age DESC

-- Multiple columns with different directions
SELECT * FROM users ORDER BY age ASC, name DESC

-- Order by aliased columns
SELECT name AS employee_name, age AS years FROM users ORDER BY years DESC
```

### LIMIT and OFFSET Clauses

The `LIMIT` and `OFFSET` clauses control the number of rows returned by a query:

```sql
-- Return only the first 10 rows
SELECT * FROM users LIMIT 10

-- Skip the first 5 rows and return the next 10
SELECT * FROM users LIMIT 10 OFFSET 5

-- Combine with ORDER BY for pagination
SELECT * FROM users ORDER BY age DESC LIMIT 10 OFFSET 20
```

LIMIT and OFFSET are particularly useful for:
- Pagination of large result sets
- Retrieving "top N" results when combined with ORDER BY
- Sampling data from large tables
- Creating efficient user interfaces that load data incrementally

Important characteristics:
- LIMIT accepts a positive integer specifying the maximum number of rows to return
- OFFSET (optional) specifies the number of rows to skip before starting to return rows
- Both clauses are applied after all other query operations (WHERE, GROUP BY, ORDER BY, etc.)
- If OFFSET is greater than or equal to the number of rows after filtering, an empty result set is returned
- LIMIT with a value of 0 will return an empty result set

### Aggregate Functions

Sqawk supports the following aggregate functions:

| Function | Description | Example |
|----------|-------------|---------|
| `COUNT(*)` | Count rows | `SELECT COUNT(*) FROM users` |
| `SUM(column)` | Sum values in column | `SELECT SUM(salary) FROM employees` |
| `AVG(column)` | Average of values in column | `SELECT AVG(age) FROM users` |
| `MIN(column)` | Minimum value in column | `SELECT MIN(salary) FROM employees` |
| `MAX(column)` | Maximum value in column | `SELECT MAX(age) FROM users` |

Aggregate functions can be used with column aliases:

```sql
SELECT COUNT(*) AS count, SUM(salary) AS total_salary, AVG(age) AS avg_age FROM employees
```

Aggregate functions can also be used with `WHERE` clauses to filter input rows:

```sql
SELECT COUNT(*) AS count, AVG(salary) AS avg_salary FROM employees WHERE department = 'Engineering'
```

### GROUP BY Clause

The `GROUP BY` clause allows you to group rows that have the same values in specified columns and apply aggregate functions to each group:

```sql
SELECT column1, column2, aggregate_function(column3)
FROM table_name
GROUP BY column1, column2
```

Examples of using GROUP BY:

```sql
-- Group by a single column
SELECT department, COUNT(*) AS employee_count, AVG(salary) AS avg_salary
FROM employees
GROUP BY department

-- Group by multiple columns
SELECT department, location, COUNT(*) AS employee_count
FROM employees
GROUP BY department, location

-- Group by with ORDER BY
SELECT department, COUNT(*) AS employee_count, SUM(salary) AS total_salary
FROM employees
GROUP BY department
ORDER BY total_salary DESC
```

GROUP BY can be used with all aggregate functions (COUNT, SUM, AVG, MIN, MAX) and can be combined with column aliases:

```sql
SELECT department, 
       COUNT(*) AS count, 
       SUM(salary) AS total_salary, 
       AVG(salary) AS avg_salary,
       MIN(salary) AS min_salary,
       MAX(salary) AS max_salary
FROM employees
GROUP BY department
```

Rules and behavior:
- All columns in the SELECT clause that are not in aggregate functions must be included in the GROUP BY clause
- Column aliases defined in the SELECT clause cannot be used in the GROUP BY clause (but they can be used in ORDER BY)
- GROUP BY columns are always included in the result set
- NULL values in GROUP BY columns are treated as a single group

### HAVING Clause

The `HAVING` clause filters grouped results based on conditions, similar to how the WHERE clause filters individual rows:

```sql
SELECT column1, column2, aggregate_function(column3)
FROM table_name
GROUP BY column1, column2
HAVING condition
```

The HAVING clause is applied after groups are formed and aggregate functions are calculated, whereas the WHERE clause is applied before grouping.

Examples of using HAVING:

```sql
-- Filter groups based on aggregate results
SELECT department, COUNT(*) AS employee_count, AVG(salary) AS avg_salary
FROM employees
GROUP BY department
HAVING COUNT(*) > 5

-- Filter groups using multiple conditions
SELECT department, COUNT(*) AS employee_count, AVG(salary) AS avg_salary
FROM employees
GROUP BY department
HAVING COUNT(*) > 5 AND AVG(salary) > 60000

-- Combine WHERE and HAVING clauses
SELECT department, COUNT(*) AS employee_count, AVG(salary) AS avg_salary
FROM employees
WHERE location = 'New York'
GROUP BY department
HAVING AVG(salary) > 70000
```

Key characteristics of the HAVING clause:
- Applied after GROUP BY and aggregate calculations
- Can reference aggregate functions
- Can contain arithmetic operations (e.g., `HAVING AVG(salary) * 1.1 > 75000`)
- Can be combined with other clauses like ORDER BY and LIMIT
- HAVING without GROUP BY treats the entire table as a single group

## Multi-Table Operations (Joins)

### Cross Joins

A cross join creates a Cartesian product of all rows from both tables:

```sql
SELECT * FROM users, orders
```

This returns all possible combinations of rows from the `users` and `orders` tables.

### Inner Joins

Sqawk supports two inner join syntax styles:

#### 1. Using WHERE Conditions:

```sql
SELECT * FROM users, orders WHERE users.id = orders.user_id
```

This returns only the rows where a user ID in the `users` table matches a user_id in the `orders` table.

#### 2. Using INNER JOIN ... ON Syntax:

```sql
SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id
```

The explicit JOIN...ON syntax provides a clearer structure for complex joins and better distinguishes the join criteria from other filtering conditions.

### Combining Joins with WHERE Clauses

When using explicit JOIN...ON syntax, you can still add WHERE clauses to filter the joined results:

```sql
SELECT users.name, orders.product_id 
FROM users INNER JOIN orders ON users.id = orders.user_id 
WHERE orders.product_id > 100
```

This returns only rows that satisfy both the join condition AND the additional WHERE filter.

### Multi-Table Joins

You can join multiple tables using either syntax:

```sql
-- Using traditional WHERE joins
SELECT users.name, products.name, orders.date 
FROM users, orders, products 
WHERE users.id = orders.user_id AND products.product_id = orders.product_id

-- Or using explicit JOIN...ON syntax
SELECT users.name, products.name, orders.date 
FROM users 
INNER JOIN orders ON users.id = orders.user_id
INNER JOIN products ON products.product_id = orders.product_id
```

### Column Naming in Joins

In join results, columns are qualified with their table names to avoid ambiguity:

```sql
SELECT users.name AS user_name, orders.date AS order_date 
FROM users INNER JOIN orders ON users.id = orders.user_id
```

## INSERT Statement

### Basic Syntax

```sql
INSERT INTO table_name VALUES (value1, value2, ...)
```

The `INSERT` statement adds a new row to the table:

```sql
INSERT INTO users VALUES (5, 'Eve', 42)
```

All columns must be provided in the correct order.

## UPDATE Statement

### Basic Syntax

```sql
UPDATE table_name SET column1 = value1 [, column2 = value2, ...] [WHERE condition]
```

The `UPDATE` statement modifies existing rows:

```sql
-- Update specific rows
UPDATE users SET age = 21 WHERE name = 'Alice'

-- Update all rows
UPDATE users SET active = true
```

## DELETE Statement

### Basic Syntax

```sql
DELETE FROM table_name [WHERE condition]
```

The `DELETE` statement removes rows from a table:

```sql
-- Delete specific rows
DELETE FROM users WHERE age < 18

-- Delete all rows
DELETE FROM users
```

## Limitations

Current limitations of Sqawk's SQL implementation:

- **Table Operations**:
  - No indices are used; all operations scan the entire table
  - No schema enforcement or constraints
  - No views

- **Join Operations**:
  - INNER JOIN with ON conditions is supported
  - No outer joins (LEFT, RIGHT, or FULL OUTER) are supported
  
- **Query Features**:
  - WHERE clauses support a variety of expressions including comparisons, logical operators, and string functions
  - String functions are only supported in WHERE clauses, not in SELECT clauses for projection
  
- **Error Handling**:
  - Errors are reported with detailed messages and context
  - CSV parsing errors include line numbers to help locate issues
  - No subqueries
  - No window functions
  - No common table expressions (CTEs)

- **Data Manipulation**:
  - No transactions (BEGIN, COMMIT, ROLLBACK)
  - INSERT must provide values for all columns
  - No column specifications in INSERT (must follow table column order)

**Supported Features**:
- Column aliases (AS keyword)
- DISTINCT keyword for removing duplicate rows
- ORDER BY with ascending/descending sorting
- LIMIT and OFFSET for pagination and result set control
- Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- GROUP BY clause for data aggregation
- HAVING clause for filtering grouped results
- Arithmetic operations in expressions (addition, subtraction, multiplication, division)
- Multi-column sorting
- Table-qualified column names
- Cross joins and inner joins through both WHERE conditions and INNER JOIN...ON syntax
- Support for custom field separators with -F option
- Compatible with CSV, TSV, and custom-delimited files

## Writeback Behavior

Modified tables are only written back to their source files if:
- The `--write` (or `-w`) flag is explicitly provided
- The table was actually modified by an SQL operation (INSERT, UPDATE, DELETE)

When writing data back:
- The original file format (CSV, TSV, or custom delimiter) is preserved
- Header rows are maintained
- Column order is preserved
- Data types are formatted appropriately based on the original values

Without the `--write` flag, source files remain untouched regardless of operations performed. This allows for exploratory data analysis without the risk of modifying source files.

---

*This document describes the SQL dialect supported by Sqawk as of the current version. Future versions may add additional capabilities.*