# Sqawk SQL Reference

## Data Types

| Type | Aliases |
|------|---------|
| `INTEGER` | `INT` |
| `REAL` | `FLOAT`, `DOUBLE` |
| `TEXT` | `STRING` |
| `BOOLEAN` | `BOOL` |
| `NULL` | |

Type inference on load: Integer → Float → Boolean → String. Empty values become NULL.

## SELECT

```sql
SELECT [DISTINCT] column_list | *
FROM table [alias] [, table2 ...]
[JOIN ...]
[WHERE condition]
[GROUP BY columns]
[HAVING condition]
[ORDER BY column [ASC|DESC], ...]
[LIMIT n [OFFSET m]]
```

### Column Selection

```sql
SELECT *                        -- all columns
SELECT col1, col2               -- specific columns
SELECT table.col                -- qualified
SELECT col AS alias             -- aliased
SELECT col alias                -- alias without AS
```

### WHERE Operators

| Operator | Example |
|----------|---------|
| `=`, `!=`, `<>` | `col = 5` |
| `<`, `>`, `<=`, `>=` | `col > 10` |
| `AND`, `OR`, `NOT` | `a > 1 AND b < 5` |
| `IS NULL`, `IS NOT NULL` | `col IS NULL` |
| `LIKE`, `ILIKE` | `name LIKE 'A%'` |
| `BETWEEN` | `col BETWEEN 1 AND 10` |
| `IN` | `col IN (1, 2, 3)` |
| `IN (SELECT ...)` | `id IN (SELECT id FROM t)` |

### CASE Expression

```sql
CASE WHEN cond THEN result [WHEN ...] [ELSE default] END
CASE expr WHEN val THEN result [WHEN ...] [ELSE default] END
```

### Aggregate Functions

| Function | Description |
|----------|-------------|
| `COUNT(*)`, `COUNT(col)`, `COUNT(DISTINCT col)` | Row/value count |
| `SUM(col)` | Sum of values |
| `AVG(col)` | Average |
| `MIN(col)` | Minimum |
| `MAX(col)` | Maximum |

### String Functions

| Function | Description |
|----------|-------------|
| `UPPER(s)` | Uppercase |
| `LOWER(s)` | Lowercase |
| `TRIM(s)` | Remove leading/trailing whitespace |
| `SUBSTR(s, start [, len])` | Substring (1-indexed) |
| `SUBSTRING(s FROM start [FOR len])` | Substring (alternate syntax) |
| `REPLACE(s, from, to)` | Replace occurrences |
| `CONCAT(s1, s2, ...)` | Concatenate strings |
| `LENGTH(s)` | String length |
| `LEFT(s, n)` | First n characters |
| `RIGHT(s, n)` | Last n characters |
| `LPAD(s, len, pad)` | Left-pad to length |
| `RPAD(s, len, pad)` | Right-pad to length |

### Math Functions

| Function | Description |
|----------|-------------|
| `ABS(n)` | Absolute value |
| `ROUND(n [, d])` | Round to `d` decimal places, or to the nearest integer |
| `CEIL(n)`, `CEILING(n)` | Round up |
| `FLOOR(n)` | Round down |

`ROUND(3.14159, 2)` is `3.14`. A negative `d` rounds to the left of the decimal
point, so `ROUND(1234.5, -2)` is `1200`. `ABS`, `CEIL`, `CEILING` and `FLOOR`
take exactly one argument and reject a second.

### Arithmetic

`+`, `-`, `*`, `/`, `%` (modulo)

### Conditional and Conversion

| Expression | Description |
|------------|-------------|
| `CAST(expr AS TYPE)` | Convert to `INTEGER`, `REAL`, `TEXT`, `BOOLEAN` |
| `COALESCE(a, b, ...)` | First non-NULL argument |
| `NULLIF(a, b)` | NULL when `a = b`, otherwise `a` |
| `a \|\| b` | String concatenation |

### Result Column Names

An expression without `AS` is named after its function -- `COUNT`, `SUM`,
`ROUND(expr)`, `COALESCE(expr)` -- and a bare `CAST` or arithmetic expression
is named `expr`. Use `AS` whenever the header is consumed downstream.

### Date/Time Functions

| Function | Description |
|----------|-------------|
| `NOW()`, `CURRENT_TIMESTAMP` | Current timestamp |
| `CURRENT_DATE` | Current date |
| `CURRENT_TIME` | Current time |
| `DATE(expr)` | Extract date |
| `TIME(expr)` | Extract time |

## JOIN

```sql
-- Cross join (cartesian product)
SELECT * FROM t1, t2

-- Inner join
SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id

-- Outer joins
SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id
SELECT * FROM t1 RIGHT JOIN t2 ON t1.id = t2.id
SELECT * FROM t1 FULL JOIN t2 ON t1.id = t2.id

-- Multiple joins
SELECT * FROM t1
  JOIN t2 ON t1.id = t2.t1_id
  JOIN t3 ON t2.id = t3.t2_id
```

## Subqueries

```sql
-- Scalar subquery
SELECT name FROM employees WHERE salary = (SELECT MAX(salary) FROM employees)

-- IN / NOT IN
SELECT name FROM users WHERE id IN (SELECT user_id FROM orders)
SELECT name FROM users WHERE id NOT IN (SELECT user_id FROM orders)

-- EXISTS / NOT EXISTS
SELECT name FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)

-- Correlated: the inner query references the outer row
SELECT name FROM employees e
  WHERE salary > (SELECT AVG(salary) FROM employees WHERE department = e.department)
```

```sql
-- Derived table: a subquery in FROM, which must be given an alias
SELECT name FROM (SELECT name, salary FROM employees WHERE salary > 70000) t
SELECT COUNT(*) FROM (SELECT department FROM employees GROUP BY department) t
```

A derived table is materialized before the outer query runs and exists only for
the statement that declares it. Its alias may not shadow a real table.

## Window Functions

```sql
SELECT name, ROW_NUMBER() OVER (ORDER BY salary DESC) FROM employees
SELECT name, RANK()       OVER (ORDER BY department) FROM employees
SELECT name, DENSE_RANK() OVER (ORDER BY department) FROM employees
SELECT name, LAG(salary)  OVER (ORDER BY salary) FROM employees
SELECT name, LEAD(salary) OVER (ORDER BY salary) FROM employees

-- Aggregates over a window
SELECT name, SUM(salary) OVER (PARTITION BY department) FROM employees
```

Supported functions: `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`, and the
aggregates `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`.

The frame follows the standard: **without** `ORDER BY` in the `OVER` clause the
frame is the whole partition, so every row sees the partition total; **with**
`ORDER BY` the frame grows row by row, giving a running value.

```sql
-- 210000 on every Engineering row
SELECT name, SUM(salary) OVER (PARTITION BY department) FROM employees

-- 65000, 135000, 210000 across the Engineering rows
SELECT name, SUM(salary) OVER (PARTITION BY department ORDER BY salary) FROM employees
```

Explicit frame clauses (`ROWS BETWEEN ...`) are not supported.

## Set Operations

```sql
SELECT ... UNION SELECT ...           -- combined, deduplicated
SELECT ... UNION ALL SELECT ...       -- combined, with duplicates
SELECT ... INTERSECT SELECT ...       -- rows in both
SELECT ... EXCEPT SELECT ...          -- rows in first but not second
```

## INSERT

```sql
INSERT INTO table VALUES (v1, v2, ...)
INSERT INTO table (col1, col2) VALUES (v1, v2)
INSERT INTO table SELECT ... FROM other_table
```

## UPDATE

```sql
UPDATE table SET col1 = val1 [, col2 = val2, ...]
[WHERE condition]
```

`UPDATE ... FROM other_table` is not supported and is accepted without effect.
To pull a value from another table, use a correlated subquery in the SET
expression -- it must select an aggregate:

```sql
UPDATE data SET category =
    (SELECT MAX(category) FROM lookup WHERE lookup.code = data.code)
```

A correlated scalar subquery selecting a bare column is rejected with
*"Correlated scalar subquery must select an aggregate function"*.

## DELETE

```sql
DELETE FROM table [WHERE condition]
```

## CREATE TABLE

```sql
CREATE TABLE name (
    col1 TYPE,
    col2 TYPE,
    ...
)
[LOCATION 'path']
[STORED AS TEXTFILE]
[WITH (DELIMITER='...')]
```

```sql
CREATE TABLE name AS SELECT ... FROM ...
```

## DROP TABLE

```sql
DROP TABLE name
DROP TABLE IF EXISTS name
```

## ALTER TABLE

```sql
ALTER TABLE name ADD COLUMN col_name TYPE
```

## TRUNCATE

```sql
TRUNCATE TABLE name
```

## NULL handling

An empty field reads as NULL.

Comparison follows SQL three-valued logic: any comparison involving NULL is
UNKNOWN, and a row whose `WHERE` evaluates to UNKNOWN is not returned. This
means a NULL row satisfies neither `x > 5` nor `x <= 5`, and `NULL = NULL` is
UNKNOWN rather than true. Use `IS NULL` / `IS NOT NULL` to test for NULL.

Aggregates skip NULLs: `COUNT(*)` counts rows while `COUNT(col)` counts
non-NULL values. `ORDER BY` sorts NULLs first.

## Type coercion

Values are typed per cell as they are read, so one column may hold integers,
floats and text.

When a comparison mixes a number and a string, the string is converted to a
number if it parses as one, and the two are compared textually otherwise. This
matches arithmetic, so `x + '1'` and `x > '1'` agree — awk-like rather than
strict SQL, which suits untyped delimited input.

## Writeback

Modifications (INSERT, UPDATE, DELETE) remain in-memory unless `--write` flag is specified.
