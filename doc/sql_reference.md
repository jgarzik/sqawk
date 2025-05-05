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
7. [SELECT Statement](#select-statement)
   - [Basic Syntax](#basic-syntax)
   - [Column Selection](#column-selection)
   - [Column Aliases](#column-aliases)
   - [WHERE Clause](#where-clause)
   - [ORDER BY Clause](#order-by-clause)
   - [Aggregate Functions](#aggregate-functions)
   - [GROUP BY Clause](#group-by-clause)
7. [Multi-Table Operations (Joins)](#multi-table-operations-joins)
8. [INSERT Statement](#insert-statement)
9. [UPDATE Statement](#update-statement)
10. [DELETE Statement](#delete-statement)
11. [Limitations](#limitations)
12. [Writeback Behavior](#writeback-behavior)

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
| `INSERT` | Add new rows to tables | `INSERT INTO users VALUES (4, 'Dave', 28)` |
| `UPDATE` | Modify existing rows in tables | `UPDATE users SET age = 29 WHERE name = 'Dave'` |
| `DELETE` | Remove rows from tables | `DELETE FROM users WHERE age < 18` |

## SELECT Statement

### Basic Syntax

```sql
SELECT [column_list | *]
FROM table_name [, table_name2, ...]
[WHERE condition]
[ORDER BY column [ASC|DESC], ...]
```

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
  - No complex expressions in WHERE clauses (only simple comparisons)
  - No HAVING clause
  - No subqueries
  - No window functions
  - No common table expressions (CTEs)

- **Data Manipulation**:
  - No transactions (BEGIN, COMMIT, ROLLBACK)
  - INSERT must provide values for all columns
  - No column specifications in INSERT (must follow table column order)

**Supported Features**:
- Column aliases (AS keyword)
- ORDER BY with ascending/descending sorting
- Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- GROUP BY clause for data aggregation
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