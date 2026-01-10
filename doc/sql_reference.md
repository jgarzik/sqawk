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
| `ROUND(n)` | Round to nearest integer |
| `CEIL(n)`, `CEILING(n)` | Round up |
| `FLOOR(n)` | Round down |

### Arithmetic

`+`, `-`, `*`, `/`, `%` (modulo)

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

## Writeback

Modifications (INSERT, UPDATE, DELETE) remain in-memory unless `--write` flag is specified.
