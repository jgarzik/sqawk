# Sqawk SQL Reference

## Data Types

| Type | Aliases |
|------|---------|
| `INTEGER` | `INT` |
| `REAL` | `FLOAT`, `DOUBLE` |
| `TEXT` | `STRING` |
| `BOOLEAN` | `BOOL` |
| `NULL` | |

Type inference on load, per cell: an empty field is NULL; a canonical integer
(`42`, `-7` -- printing the number gives back the same text) is INTEGER; a
canonical float (`1.5`, `-0.25`) is REAL; everything else is TEXT, verbatim and
untrimmed -- including `007`, `1.50`, `1e3`, ` 42`, `true` and `NaN`. Booleans
are never inferred. The rule guarantees that every loaded cell prints, and is
written back, exactly as it was read.

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
point, so `ROUND(1234.5, -2)` is `1200`. A `d` that is not a number is an error
rather than a silent `0`. `ABS`, `CEIL`, `CEILING` and `FLOOR` take exactly one
argument and reject a second.

A result that lands on a whole number prints without a decimal part, as
everywhere else in sqawk: `ROUND(2.0, 2)` is `2`, matching `AVG` and plain
arithmetic rather than SQL's `2.0`.

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
is named `expr`. `ROUND` includes its precision, so `ROUND(salary, 2)` is named
`ROUND(salary, 2)` and two roundings of one column to different precisions stay
addressable. Use `AS` whenever the header is consumed downstream.

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

-- Multiple joins, any mix of INNER / LEFT / CROSS
SELECT * FROM t1
  JOIN t2 ON t1.id = t2.t1_id
  LEFT JOIN t3 ON t2.id = t3.t2_id

-- USING and NATURAL: the shared columns appear once in SELECT *
SELECT * FROM t1 JOIN t2 USING (id)
SELECT * FROM t1 NATURAL LEFT JOIN t2
```

WHERE filters the joined rows, including the NULL-extended rows of an outer
join (`... LEFT JOIN t2 ON ... WHERE t2.id IS NULL` is an anti-join). An
unqualified column name must be unambiguous across the joined tables.

A RIGHT JOIN whose left side already contains an outer join, and a FULL JOIN
followed by further joins, are rejected as unsupported.

Joins on an equality (`ON a.k = b.k`, or the same condition in WHERE for a
comma or inner join) run as hash joins: the inner table is indexed once
instead of being rescanned for every outer row.

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

A subquery is a full query: it may use joins, GROUP BY, HAVING, DISTINCT,
ORDER BY, LIMIT/OFFSET and UNION/INTERSECT/EXCEPT, and it may appear anywhere
an expression can -- the SELECT list, WHERE, ON, HAVING, ORDER BY, LIMIT, CASE,
function and aggregate arguments, UPDATE SET, DELETE WHERE and INSERT VALUES.

- A name the subquery does not define is looked up in the enclosing queries,
  innermost first, so a subquery may reference a column of any enclosing
  query, at any depth. Such a subquery is *correlated* and runs again for each
  outer row; an uncorrelated one runs once.
- A scalar subquery yields its first row's value, or NULL when it returns no
  rows. It must return exactly one column (*"sub-select returns 2 columns -
  expected 1"*); so must the subquery of IN.
- `x IN (SELECT ...)` follows SQL's three-valued logic: over an empty result
  it is false (NOT IN true) even for a NULL `x`; otherwise a NULL `x`, or a
  miss when the result contains a NULL, is NULL -- so `NOT IN` over a result
  containing a NULL is never true.
- In UPDATE and DELETE, a subquery sees the table as it was before the
  statement started. (SQLite instead lets a correlated subquery see rows the
  same UPDATE has already changed.)
- Not supported: row values (`(a, b) IN (SELECT x, y ...)`), `ANY`/`ALL`,
  and a derived table inside a subquery used as an expression.

## Window Functions

```sql
SELECT name, ROW_NUMBER() OVER (ORDER BY salary DESC) FROM employees
SELECT name, RANK()       OVER (PARTITION BY department ORDER BY salary) FROM employees
SELECT name, LAG(salary, 1, 0) OVER (ORDER BY salary) FROM employees
SELECT name, salary - AVG(salary) OVER (PARTITION BY department) FROM employees
SELECT department, COUNT(*), RANK() OVER (ORDER BY COUNT(*) DESC)
  FROM employees GROUP BY department
SELECT name, SUM(salary) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
  FROM employees
SELECT name, SUM(salary) OVER w, RANK() OVER (w ORDER BY salary)
  FROM employees WINDOW w AS (PARTITION BY department)
```

Supported functions:

| Function | Value |
|---|---|
| `ROW_NUMBER()` | 1, 2, ... within the partition |
| `RANK()` | 1 + number of rows before the current row's peers (gaps after ties) |
| `DENSE_RANK()` | number of the current peer group (no gaps) |
| `PERCENT_RANK()` | (rank - 1) / (partition rows - 1); 0 for a one-row partition |
| `CUME_DIST()` | rows up to and including the last peer / partition rows |
| `NTILE(n)` | bucket 1..n; the first `rows % n` buckets get one row more |
| `LAG(x [, offset [, default]])` | `x` from `offset` (default 1) rows back, else `default` (NULL) |
| `LEAD(x [, offset [, default]])` | likewise, forward |
| `FIRST_VALUE(x)`, `LAST_VALUE(x)`, `NTH_VALUE(x, n)` | `x` at the first / last / n-th row of the frame |
| `COUNT(*)`, `COUNT(x)`, `SUM`, `AVG`, `MIN`, `MAX` | the aggregate over the frame |

Partition and order keys may be any expressions, any number of them; NULLs
sort first. Rows with equal ORDER BY keys are *peers*. A query may use any
number of window functions with different `OVER` clauses, and window calls
may appear inside expressions (`ROUND(AVG(x) OVER (...), 1)`) and in ORDER BY.
Windows work over joins, over grouped rows (they run after GROUP BY and
HAVING), and in subqueries. A window function in WHERE, GROUP BY, HAVING or
inside an aggregate is an error (*misuse of window function*).

The frame (the rows an aggregate or FIRST_VALUE / LAST_VALUE / NTH_VALUE
sees) defaults, as in SQLite, to `RANGE BETWEEN UNBOUNDED PRECEDING AND
CURRENT ROW`: **without** `ORDER BY` that is the whole partition, so every row
sees the partition total; **with** `ORDER BY` it is a running value in which
peers share the same value.

```sql
-- 210000 on every Engineering row
SELECT name, SUM(salary) OVER (PARTITION BY department) FROM employees

-- 65000, 135000, 210000 across the Engineering rows
SELECT name, SUM(salary) OVER (PARTITION BY department ORDER BY salary) FROM employees
```

Explicit frames: `ROWS` with any of `UNBOUNDED PRECEDING`, `n PRECEDING`,
`CURRENT ROW`, `n FOLLOWING`, `UNBOUNDED FOLLOWING` (`n` a non-negative
integer literal), and `RANGE` with `UNBOUNDED` / `CURRENT ROW` bounds. Named
windows (`WINDOW w AS (...)`, `OVER w`, `OVER (w ORDER BY ...)`) are
supported. Not supported, and rejected with an error: `RANGE` frames with an
offset, `GROUPS` frames, `EXCLUDE`, `FILTER` on a window function, and
`DISTINCT` inside one. SUM of integers is an integer; AVG, PERCENT_RANK and
CUME_DIST are floats.

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
expression:

```sql
UPDATE data SET category =
    (SELECT category FROM lookup WHERE lookup.code = data.code)
```

When several lookup rows match, the first is used; a row with no match gets
NULL.

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

An empty field reads as NULL, and NULL is printed (and written back) as an
empty field.

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
strict SQL, which suits untyped delimited input. Two strings compare as text.

Text that spells a number (`007`, `1.50`, `1e3`, which load as text to keep
their spelling) is used as that number by arithmetic and by `SUM` and `AVG`.
`ORDER BY`, `GROUP BY`, `MIN` and `MAX` order such text among the numbers by
value; values equal as numbers but spelled differently (`7` and `007`) remain
distinct groups.

## Writeback

Modifications (INSERT, UPDATE, DELETE) remain in-memory unless `--write` flag is specified.
Cells a statement did not change are written back byte for byte; NULL is
written as an empty field; the source file's delimiter, header line (or its
absence), line endings and permissions are kept.
