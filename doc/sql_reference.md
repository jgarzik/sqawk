# Sqawk SQL Language Reference

## Introduction

Sqawk provides a powerful SQL-like query language for processing CSV files. This document serves as a reference for the SQL dialect supported by Sqawk and explains its syntax, features, and limitations.

## Table of Contents

1. [Introduction](#introduction)
2. [Table Names and File Specification](#table-names-and-file-specification)
3. [Chaining SQL Statements](#chaining-sql-statements)
4. [SQL Statement Types](#sql-statement-types)
5. [SELECT Statement](#select-statement)
   - [Basic Syntax](#basic-syntax)
   - [Column Selection](#column-selection)
   - [Column Aliases](#column-aliases)
   - [WHERE Clause](#where-clause)
   - [ORDER BY Clause](#order-by-clause)
   - [Aggregate Functions](#aggregate-functions)
6. [Multi-Table Operations (Joins)](#multi-table-operations-joins)
7. [INSERT Statement](#insert-statement)
8. [UPDATE Statement](#update-statement)
9. [DELETE Statement](#delete-statement)
10. [Data Types](#data-types)
11. [Limitations](#limitations)
12. [Writeback Behavior](#writeback-behavior)

## Table Names and File Specification

When using Sqawk, CSV files are loaded as in-memory tables. By default, the table name is derived from the filename (without the extension). You can also explicitly specify a table name:

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

## Multi-Table Operations (Joins)

### Cross Joins

A cross join creates a Cartesian product of all rows from both tables:

```sql
SELECT * FROM users, orders
```

This returns all possible combinations of rows from the `users` and `orders` tables.

### Inner Joins

Inner joins match rows based on a condition:

```sql
SELECT * FROM users, orders WHERE users.id = orders.user_id
```

This returns only the rows where a user ID in the `users` table matches a user_id in the `orders` table.

### Multi-Table Joins

You can join multiple tables:

```sql
SELECT users.name, products.name, orders.date 
FROM users, orders, products 
WHERE users.id = orders.user_id AND products.product_id = orders.product_id
```

### Column Naming in Joins

In join results, columns are qualified with their table names to avoid ambiguity:

```sql
SELECT users.name AS user_name, orders.date AS order_date FROM users, orders WHERE users.id = orders.user_id
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

When loading data from CSV files, Sqawk automatically infers the most appropriate type for each value:

1. First tries to parse as an `Integer`
2. If that fails, tries to parse as a `Float` 
3. If that fails, tries to parse as a `Boolean` (values like true/false, yes/no, 1/0)
4. If all else fails, stores the value as a `String`
5. Empty values are stored as `Null`

This dynamic type inference provides flexibility when working with CSV data, which typically doesn't include explicit type information.

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

## Limitations

Current limitations of Sqawk's SQL implementation:

- **Table Operations**:
  - No indices are used; all operations scan the entire table
  - No schema enforcement or constraints
  - No views

- **Join Operations**:
  - No explicit `JOIN ... ON` syntax yet; must use `WHERE` conditions
  - No outer joins (LEFT, RIGHT, or FULL OUTER) are supported

- **Query Features**:
  - No complex expressions in WHERE clauses (only simple comparisons)
  - No GROUP BY or HAVING clauses (though aggregate functions are supported)
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
- Multi-column sorting
- Table-qualified column names
- Cross joins and inner joins through WHERE conditions

## Writeback Behavior

Modified tables are only written back to CSV files if:
- The `--write` (or `-w`) flag is explicitly provided
- The table was actually modified by an SQL operation (INSERT, UPDATE, DELETE)

Without the `--write` flag, source files remain untouched regardless of operations performed.

---

*This document describes the SQL dialect supported by Sqawk as of the current version. Future versions may add additional capabilities.*