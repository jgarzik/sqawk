# Sqawk SQL Reference

## Data Types

Every value is INTEGER, REAL, TEXT or NULL (what `TYPEOF` reports). `TRUE`
and `FALSE` are the integers 1 and 0.

A column type declared in CREATE TABLE or ALTER TABLE ADD COLUMN is recorded
but not enforced: any type name is accepted, any value can be stored in the
column, and every value keeps its own type (see Type coercion). The name is
kept as written and `.schema` shows it, with keywords in upper case and
spacing normalized by the parser (`decimal(10, 2)` shows as `DECIMAL(10,2)`;
a name that is not a keyword, such as `money`, keeps its case). Its affinity
follows SQLite's rules, in order: a name containing `INT` is INTEGER
(`BIGINT`, `SMALLINT`); `CHAR`, `CLOB` or `TEXT` is TEXT (`VARCHAR(20)`);
`BLOB` is BLOB; `REAL`, `FLOA` or `DOUB` is REAL (`DOUBLE PRECISION`);
anything else is NUMERIC (`DECIMAL(10,2)`, `BOOLEAN`, `DATE`, `STRING`). The
affinity converts no value; it is the type `CREATE TABLE ... AS` gives a
column copied from the table. Every column needs a type: `CREATE TABLE t (a)`
is a parse error. A loaded file's columns have no declared type; `.schema`
shows them as `TEXT`.

Type inference on load, per cell: an empty field is NULL; a canonical integer
(`42`, `-7` -- printing the number gives back the same text) is INTEGER; a
canonical float (`1.5`, `-0.25`, `2.0`, `1.0e+20` -- printing the real, see
Real output, gives back the same text) is REAL; everything else is TEXT,
verbatim and untrimmed -- including `007`, `1.50`, `1e3`, `-0.0`,
`0.30000000000000004` (more digits than a real prints), ` 42`, `true` and
`NaN`. Booleans are never inferred. The rule guarantees that every loaded cell
prints, and is written back, exactly as it was read.

### Real output

A REAL prints exactly as SQLite's `CAST(x AS TEXT)` prints it (`%!.15g`): 15
significant digits, rounded half up; positional notation when the exponent is
from -4 to 14, else exponent notation with a signed, at least two-digit
exponent; trailing zeros dropped, but a whole number keeps `.0`. So `0.1 + 0.2`
is `0.3`, `2.0 * 1` is `2.0`, `1e20` is `1.0e+20`, `1.5e-300` is `1.5e-300`,
`9223372036854775807 + 1` is `9.22337203685478e+18`, negative zero is `0.0`,
and the infinities are `Inf` and `-Inf`. The same text is what `||`, string
functions and `CAST(x AS TEXT)` see, and what `--write` writes for a changed
cell.

## SELECT

```sql
SELECT [DISTINCT] column_list | *
FROM table [alias] [, table2 ...]
[JOIN ...]
[WHERE condition]
[GROUP BY columns]
[HAVING condition]
[ORDER BY expr [ASC|DESC] [NULLS FIRST|NULLS LAST], ...]
[LIMIT n [OFFSET m]]
```

### Column Selection

```sql
SELECT *                        -- all columns
SELECT col1, col2               -- specific columns
SELECT table.col                -- qualified (by the alias, if the table has one)
SELECT main.table.col           -- `main` is the one schema; any other is an error
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
| `LIKE`, `ILIKE` | `name LIKE 'A%'`, `code LIKE 'x\_%' ESCAPE '\'` |
| `IS [NOT]`, `IS [NOT] DISTINCT FROM` | `a IS b`, `a IS NOT DISTINCT FROM b` (NULL-safe `=`) |
| `BETWEEN` | `col BETWEEN 1 AND 10` |
| `IN` | `col IN (1, 2, 3)` |
| `IN (SELECT ...)` | `id IN (SELECT id FROM t)` |

Operators bind as in SQLite, loosest first; operators on one line share a
level and group left to right (`2 = 1 < 2` is `2 = (1 < 2)`, `2 * 3 || 4` is
`2 * (3 || 4)`, `'abc' LIKE 'A%' = 1` is `('abc' LIKE 'A%') = 1`):

| Operators |
|-----------|
| `OR` |
| `AND` |
| `NOT` |
| `=` `==` `!=` `<>` `IS [NOT]` `IS [NOT] DISTINCT FROM` `IN` `LIKE` `BETWEEN` |
| `<` `<=` `>` `>=` |
| `&` `\|` `<<` `>>` |
| `+` `-` |
| `*` `/` `%` |
| `\|\|` |
| unary `-` `+` `~` |

An expression may nest at most 1000 levels deep (SQLite's limit; a chain
`a + b + ...` of n terms is n levels, parentheses add none). A deeper one is
the error `expression tree is too large (maximum depth 1000)`.

The SQL parser has its own, lower limit on *nesting*: parentheses and
function calls may nest 47 deep (`((((1))))`, `abs(abs(1))`), and scalar
subqueries inside one another about half as deep. Deeper nesting is the
error `recursion limit exceeded`.

`LIKE` follows SQLite: `%` matches any run of characters and `_` exactly one
character (not byte), both including newlines; letters compare
case-insensitively for ASCII only (`'a%'` matches `Alice`, but `'é'` does not
match `'É'`). `ESCAPE 'c'` makes the character after `c` literal. The pattern
may be any expression, such as another column. `ILIKE` (not in SQLite) folds
Unicode case. A NULL value, pattern or escape gives NULL.

### CASE Expression

```sql
CASE WHEN cond THEN result [WHEN ...] [ELSE default] END
CASE expr WHEN val THEN result [WHEN ...] [ELSE default] END
```

### Aggregate Functions

| Function | Description |
|----------|-------------|
| `COUNT(*)`, `COUNT(col)`, `COUNT(DISTINCT col)` | Row/value count |
| `SUM(x)` | Sum; an integer while every value is an integer |
| `TOTAL(x)` | Sum as a real; `0.0` (not NULL) over no rows |
| `AVG(x)` | Average (a real) |
| `MIN(x)`, `MAX(x)` | Least / greatest value |
| `GROUP_CONCAT(x [, sep])` | Non-NULL values joined by `sep` (default `,`) |
| `STRING_AGG(x, sep)` | Same as `GROUP_CONCAT(x, sep)` |

Any aggregate takes `DISTINCT` (`SUM(DISTINCT x)`; `GROUP_CONCAT(DISTINCT x)`
only with one argument) and a filter: `COUNT(*) FILTER (WHERE age > 30)`
aggregates only the rows for which the condition is true. Aggregates skip
NULLs; over no rows COUNT is 0, TOTAL 0.0 and the rest NULL. SUM of integers
that overflows 64 bits is an `integer overflow` error, as in SQLite (TOTAL and
AVG carry on in floating point). Real sums use Kahan-Babuska-Neumaier
compensated summation, as SQLite does, so `SUM` of `0.1`, `0.2`, `0.3` is
exactly `0.6`. Text that spells a number is summed as that number; other text
is skipped.

### String Functions

String functions count characters, not bytes. Unless noted, a NULL argument
gives NULL, and a number argument is used in its text form.

| Function | Description |
|----------|-------------|
| `UPPER(s)`, `LOWER(s)` | Case conversion (all of Unicode) |
| `LENGTH(s)` | Number of characters |
| `TRIM(s)` | Strip whitespace from both ends |
| `LTRIM(s [, chars])`, `RTRIM(s [, chars])` | Strip whitespace, or any of `chars`, from the start / the end |
| `TRIM([BOTH\|LEADING\|TRAILING] chars FROM s)` | Strip any of `chars` from both ends / the start / the end |
| `SUBSTR(s, start [, len])`, `SUBSTRING` | Substring (see below) |
| `SUBSTRING(s FROM start [FOR len])` | Substring (alternate syntax) |
| `REPLACE(s, from, to)` | Replace every occurrence; an empty `from` leaves `s` alone |
| `INSTR(s, t)` | 1-based position of the first `t` in `s`, 0 if absent |
| `CONCAT(a, b, ...)` | Concatenation; NULL arguments count as `''` |
| `CONCAT_WS(sep, a, ...)` | Concatenation with a separator; NULL arguments skipped |
| `LEFT(s, n)`, `RIGHT(s, n)` | First / last `n` characters |
| `LPAD(s, len [, fill])`, `RPAD` | Pad to `len` characters (see below) |
| `HEX(x)` | Upper-case hex of the UTF-8 bytes; `''` for NULL |
| `CHAR(n, ...)` | The characters with these code points |
| `UNICODE(s)` | Code point of the first character |
| `PRINTF(fmt, ...)`, `FORMAT` | SQLite's printf (see below) |
| `QUOTE(x)` | The SQL literal for `x`: `'it''s'`, `12`, `1.5`, `NULL` |
| `a \|\| b` | Concatenation; NULL if either side is NULL |

`PRINTF` is SQLite's: conversions `%d %i %u %f %e %E %g %G %x %X %o %c %s
%z %q %Q %w %p %r %n %%`, flags `-`, `+`, space, `0`, `#` (alternate form:
`0x` / `0` prefixes, keep trailing zeros), `,` (thousands separators) and
`!` (widths and precisions in characters, and up to 26 significant digits
for reals), a width and a precision (`*` takes either from the arguments),
and the length modifiers `l` / `ll`, which are ignored. Details that follow
SQLite: `%.3d` of 7 is `007`; `%u` reads the value as unsigned 64-bit
(`-1` is `18446744073709551615`); `%q` doubles single quotes, `%Q` also
encloses the text in them (NULL is `NULL`), `%w` doubles double quotes;
`%r` is an ordinal (`1st`, `22nd`); `%.3c` repeats the character three
times; a real is printed from at most 16 significant digits, rounded half
up, so `%.20f` of 0.1 is `0.10000000000000000000`, while a precision of
zero for `%f` or `%g` rounds nothing (`%.0f` of 2.5 is `2`); `%s` widths and precisions
count bytes, not characters, unless `!` is given. A missing argument is
NULL, which prints as 0 or `''`. An unknown conversion ends the output
(`printf('a%yb')` is `a`), and a result with no output at all is NULL
(`printf('')`).

`SUBSTR` follows SQLite: positions are 1-based; a negative `start` counts
from the end (`SUBSTR('hello', -3)` is `llo`); `start` 0 is a position before
the first character, so `SUBSTR('hello', 0, 2)` is `h`; a negative `len` takes
the characters before `start` (`SUBSTR('hello', 3, -2)` is `he`).

`LPAD`/`RPAD` are not in SQLite and follow PostgreSQL: the fill (default a
space) is repeated as needed, `LPAD('ab', 5, 'xy')` is `xyxab`; a string
longer than `len` is cut to its first `len` characters (`LPAD('hello', 2)` is
`he`); an empty fill leaves a short string as it is; `len <= 0` gives `''`.

No function builds a string longer than SQLite's default length limit,
1,000,000,000 bytes: `LPAD('a', 10000000000)`, a `PRINTF` width or precision
past the limit (SQLite returns NULL or wraps the number), or a `REPLACE`,
`HEX` or concatenation whose result would exceed it is the error "string or
blob too big". The length is checked before anything is allocated.

### Math Functions

| Function | Description |
|----------|-------------|
| `ABS(n)` | Absolute value |
| `ROUND(n [, d])` | Round to `d` decimal places (default 0); always a real |
| `CEIL(n)`, `CEILING(n)` | Round up |
| `FLOOR(n)` | Round down |
| `SIGN(n)` | -1, 0 or 1; NULL for text that is not a number |
| `MIN(a, b, ...)`, `MAX(a, b, ...)` | Least / greatest argument; NULL if any is NULL |

`ROUND(3.14159, 2)` is `3.14`. Halves round away from zero, judged on the
value actually stored: `ROUND(2.675, 2)` is `2.67`, because 2.675 is stored as
2.67499999.... A negative `d` is treated as 0, as in SQLite:
`ROUND(1234.5, -2)` is `1235.0`. A `d` that is not a number is an error rather
than a silent `0`.

`ABS` of an integer is an integer, and `ABS(-9223372036854775808)` is an
`integer overflow` error. `CEIL` and `FLOOR` return an integer argument
unchanged. With one argument, `MIN` and `MAX` are the aggregates.

`ROUND` returns a real, so a whole result prints with `.0` (see Real output):
`ROUND(2.0, 2)` is `2.0`.

### Arithmetic

`+`, `-`, `*`, `/`, `%` follow SQLite:

- Integers stay 64-bit integers: `5 / 2` is `2`, `-7 / 2` is `-3` (truncation
  toward zero), `7 % 3` is `1`. Integers beyond 2^53 stay exact.
- A result that does not fit in 64 bits is computed as a real instead:
  `9223372036854775807 + 1` is the real `9.22337203685478e+18`.
- A real operand makes the result real (`7.0 / 2` is `3.5`). `%` with a real
  operand takes the remainder of the operands truncated to integers, as a
  real (`5.5 % 2` is `1.0`).
- Division or remainder by zero is NULL. Any NULL operand gives NULL.
- Text is read by its longest numeric prefix: `'12abc' + 0` is `12`,
  `' 5 ' + 1` is `6`, `'1e3' + 0` is the real `1000.0`, `'abc' + 1` is `1`.

The bitwise operators `&`, `|`, `<<`, `>>` and prefix `~` also follow
SQLite. They bind tighter than `<` and looser than `+` (`1 + 2 << 1` is 6).
Both operands are read as 64-bit integers: reals are truncated, text is read
by its integer prefix as `CAST(text AS INTEGER)` reads it (`'12abc' & 7` is
4). A negative shift count shifts the other way, a shift of 64 or more gives
0 (or -1 for `>>` of a negative number), `>>` keeps the sign, and a NULL
operand gives NULL.

Integer literals may be written in hex: `0x1F`, `0XFF`. Up to 16 hex digits
are read as a 64-bit two's-complement value (`0xFFFFFFFFFFFFFFFF` is -1);
more is the error "hex literal too big". Text such as `'0x10'` is not read as
hex.

### Conditional and Conversion

| Expression | Description |
|------------|-------------|
| `CAST(expr AS type)` | Convert (see below) |
| `COALESCE(a, b, ...)` | First non-NULL argument |
| `IFNULL(a, b)` | `COALESCE(a, b)` |
| `NULLIF(a, b)` | NULL when `a = b`, otherwise `a` |
| `IIF(cond, a, b)` | `CASE WHEN cond THEN a ELSE b END` |
| `TYPEOF(x)` | `'integer'`, `'real'`, `'text'` or `'null'` |

COALESCE, IFNULL and IIF evaluate only the argument they return.

`CAST` maps any type name onto INTEGER, REAL, TEXT or NUMERIC by SQLite's
affinity rules: a name containing `INT` is INTEGER (`BIGINT`, `SMALLINT`);
`CHAR`, `CLOB` or `TEXT` is TEXT (`VARCHAR(10)`); `REAL`, `FLOA` or `DOUB` is
REAL; anything else (`NUMERIC`, `DECIMAL`, `BOOLEAN`, `DATE`) is NUMERIC.
One exception: `STRING` is a TEXT alias (`CAST('x' AS STRING)` is the text
`x`), where SQLite's rules make it NUMERIC. The exception is CAST's alone; a
column declared `STRING` has NUMERIC affinity (see Data Types).

- `CAST(text AS INTEGER)` reads an optional sign and the digits after it:
  `'3.9'` is 3, `'12abc'` is 12, `' 5 '` is 5, `'1e3'` is 1, `'abc'` is 0.
  Out-of-range values saturate at the 64-bit limits.
- `CAST(real AS INTEGER)` truncates toward zero, saturating.
- `CAST(text AS REAL)` reads the longest numeric prefix (`'1e3x'` is 1000).
- `CAST(text AS NUMERIC)` gives an integer when the number is whole
  (`'3.0'` is 3), else a real. `CAST(1 AS BOOLEAN)` is 1.

### Result Column Names

Result columns are named as SQLite names them:

- an `AS` alias names its column;
- a column reference, qualified or not (`u.name`, `NAME`, `(name)`), is named
  by the column as the table declares it (`name`), and `*` / `t.*` expand to
  the tables' column names -- in a join duplicates stay (`id,name,id,total`);
- any other expression is named by its text exactly as written, spacing and
  case included: `COUNT(*)`, `n  +  1`, `upper(name)||1`,
  `a IS NOT DISTINCT FROM 5`;
- a compound SELECT takes its names from its leftmost SELECT.

The columns of a derived table (`FROM (SELECT ...)`) and of a table made by
`CREATE TABLE ... AS` are named the same way and then made unique as SQLite
does: a repeated name gets `:1`, `:2`, ... (`id, id:1`). In a derived table a
column reference is named as written (`id` for a column declared `Id`).

Use `AS` whenever the header is consumed downstream.

### Column Positions in ORDER BY and GROUP BY

As in SQLite, an ORDER BY or GROUP BY term that is an integer literal --
under any parentheses and unary `+` / `-` -- whose value fits in 32 bits is
a result-column position: `ORDER BY 2`, `ORDER BY +1`, `GROUP BY (1)` and
`ORDER BY - -1` name a column. It must lie between 1 and the number of
result columns, or the statement fails with
`1st ORDER BY term out of range - should be between 1 and 2` (or
`GROUP BY`; `-1` and `0` are out of range).
Any other term is an ordinary expression, so a constant one does not reorder
rows and gives a single group: `ORDER BY 1.0`, `ORDER BY 4294967297`,
`ORDER BY 1+0`, `ORDER BY '1'`. In a compound SELECT an ORDER BY term that
is not a position must name a result column, or the statement fails with
`1st ORDER BY term does not match any column in the result set`. Inside a
window (`OVER (ORDER BY 1)`) an integer is always a constant.

### Date/Time Functions

| Function | Description |
|----------|-------------|
| `DATE(t, modifier...)` | `YYYY-MM-DD` |
| `TIME(t, modifier...)` | `HH:MM:SS` |
| `DATETIME(t, modifier...)` | `YYYY-MM-DD HH:MM:SS` |
| `JULIANDAY(t, modifier...)` | Julian day number (a real) |
| `UNIXEPOCH(t, modifier...)` | Seconds since 1970-01-01 (an integer) |
| `STRFTIME(fmt, t, modifier...)` | Formatted (see below) |
| `TIMEDIFF(a, b)` | The time from `b` to `a`: `+YYYY-MM-DD HH:MM:SS.SSS` |
| `CURRENT_DATE`, `CURRENT_TIME`, `CURRENT_TIMESTAMP`, `NOW()` | Now |

These follow SQLite 3.45 exactly, in UTC. The time string `t` is `now`,
`[-]YYYY-MM-DD`, `YYYY-MM-DD HH:MM[:SS[.fff]]` (with any whitespace or `T`
between date and time, and an optional `Z` / `±HH:MM` zone; trailing
whitespace is allowed), `HH:MM[:SS[.fff]]` (on 2000-01-01), or a number: a
Julian day number, or seconds since 1970 with the `unixepoch` modifier.
With no arguments the functions return the current moment. Years run from
-4713 to 9999; a negative year prints with its sign (`-0001-12-31`).

Modifiers:

- `±N days|hours|minutes|seconds|months|years` (fractions allowed),
  `±HH:MM[:SS[.fff]]`, `±YYYY-MM-DD` and `±YYYY-MM-DD HH:MM[:SS[.fff]]`;
- `start of day|month|year`, `weekday N` (0 = Sunday);
- `unixepoch`, `julianday` and `auto` (seconds since 1970 or a Julian day
  number, by magnitude), each only as the first modifier of a number;
- `subsec` / `subsecond`: milliseconds in `DATETIME`, `TIME`, `UNIXEPOCH`
  (then a real) and `STRFTIME('%s')`;
- `localtime` and `utc`: convert between UTC and the local time zone of the
  operating system (the `TZ` environment variable where it is set).

`STRFTIME` conversions: `%d` day, `%e` day space-padded, `%f` seconds with
milliseconds, `%F` `%Y-%m-%d`, `%H` hour, `%k` hour space-padded, `%I` / `%l`
12-hour hour, `%j` day of year, `%J` Julian day number, `%m` month, `%M`
minute, `%p` / `%P` `AM` / `am`, `%R` `%H:%M`, `%s` seconds since 1970, `%S`
seconds, `%T` `%H:%M:%S`, `%u` weekday 1-7 (Monday 1), `%w` weekday 0-6
(Sunday 0), `%W` week of year, `%Y` year, `%%`. Any other conversion makes
the result NULL, as in SQLite 3.45.

A malformed time string or an unknown modifier gives NULL. A day past the end
of its month, up to 31, rolls into the next month: `DATE('2024-02-30')` is
`2024-03-01`, and `DATE('2024-01-31', '+1 month')` is `2024-03-02`.
As in SQLite, a fractional month or year count moves the calendar by its
whole part and then adds the fraction as 30-day months or 365-day years
(`DATETIME('2024-01-01', '+1.5 months')` is `2024-02-16 00:00:00`); a count
beyond a unit's range (about 14713 years) gives NULL; and the result must lie
in years -4713 to 9999, which is checked after the last modifier.

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
-- Derived table: a subquery in FROM
SELECT name FROM (SELECT name, salary FROM employees WHERE salary > 70000) t
SELECT COUNT(*) FROM (SELECT department FROM employees GROUP BY department) t
```

A derived table is materialized before the statement runs and exists only for
the statement that declares it. It may appear in any FROM -- including inside
a subquery, a compound arm or an INSERT ... SELECT -- and its alias is
optional and may shadow a real table's name. A derived table that refers to a
column of an enclosing query cannot be materialized up front and is an error.

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
- Not supported: row values (`(a, b) IN (SELECT x, y ...)`), `ANY`/`ALL`.

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
| `COUNT(*)`, `COUNT(x)`, `SUM`, `TOTAL`, `AVG`, `MIN`, `MAX`, `GROUP_CONCAT(x [, sep])`, `STRING_AGG(x, sep)` | the aggregate over the frame |

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
`DISTINCT` inside one. SUM of integers is an integer; TOTAL, AVG,
PERCENT_RANK and CUME_DIST are floats.

## Set Operations

```sql
SELECT ... UNION SELECT ...           -- combined, deduplicated
SELECT ... UNION ALL SELECT ...       -- combined, with duplicates
SELECT ... INTERSECT SELECT ...       -- rows in both
SELECT ... EXCEPT SELECT ...          -- rows in first but not second
```

As in SQLite, the four operators have equal precedence and apply left to
right: `A UNION ALL B INTERSECT C` is `(A UNION ALL B) INTERSECT C`.
Parenthesize an operand to group differently. A compound may have at most
500 terms (`too many terms in compound SELECT`).

## INSERT

```sql
INSERT INTO table VALUES (v1, v2, ...)
INSERT INTO table (col1, col2) VALUES (v1, v2)
INSERT INTO table SELECT ... FROM other_table
```

The SELECT may be any query (joins, GROUP BY, ORDER BY/LIMIT, UNION, ...).
It must yield one column per target column.

## UPDATE

```sql
UPDATE table SET col1 = val1 [, col2 = val2, ...]
[WHERE condition]
```

`UPDATE ... FROM other_table` is not supported and is rejected with an error.
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

Column types are recorded, not enforced (see Data Types). With `LOCATION`,
the table is written to that file under `--write`, even if nothing is
inserted (the file then holds only the header). The file's delimiter is the
`WITH (DELIMITER='...')` value when given, else the one the file would be
loaded with: tab for a `.tsv` or `.tab` path, comma for any other.

`CREATE TABLE ... AS` gives the new table the query's result columns.
Column and table constraints (`PRIMARY KEY`, `NOT NULL`, `DEFAULT`, ...) are
rejected: sqawk does not enforce them. `STORED AS` takes only `TEXTFILE` and `WITH` only
`DELIMITER`; anything else there is an error. A table without `LOCATION`
(every `CREATE TABLE ... AS` table among them) has no file and lasts for the
run; `CREATE TEMP TABLE` is accepted for such a table.

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
non-NULL values. `ORDER BY` sorts NULLs first ascending and last descending,
unless the term says `NULLS FIRST` or `NULLS LAST` (also in a window's ORDER
BY).

NULL and the empty string both print as an empty field.

## Type coercion

Values are typed per cell as they are read, so one column may hold integers,
floats and text.

Every comparison -- `=`, `<>`, `<`, `<=`, `>`, `>=`, `IS`, `BETWEEN`, `IN`
with a list, `IN (SELECT ...)`, `CASE x WHEN`, `NULLIF`, JOIN conditions
(including hash joins), `ORDER BY`, `MIN` / `MAX` and window ordering -- uses
one rule:

- A value's *numeric reading* is the value itself for an integer or real, and
  for text the number it spells in full: an optional sign, digits with an
  optional point, an optional exponent, optionally surrounded by ASCII
  whitespace (`'007'`, `' 1.5 '`, `'1e3'`, `'+3'`). `'12abc'`, `'0x10'`,
  `'inf'`, `'NaN'` and `''` have none.
- Two values that both have a numeric reading compare as numbers, whatever
  their types -- text against text included: `'10' > '9'`, `'1' = '1.0'`,
  `'007' = 7`, and integers compare exactly (no rounding through floating
  point).
- A number (or numeric-reading text) is less than text without one:
  `10 < 'abc'`, `'-x' > 10`.
- Two texts without a numeric reading compare byte by byte (BINARY):
  `'B' < 'a'`.
- Any NULL operand makes the comparison UNKNOWN (sorting puts NULLs first).

`<` and `ORDER BY` therefore agree: a column sorts `9, 10, -x, abc`, and
`WHERE s < '9'` selects nothing from it. `x IN (SELECT ...)` matches exactly the
rows `x = ...` (and the equivalent JOIN) matches: `'9' IN (SELECT 9)` is 1.

Values equal under this rule but not the same value -- text `'1'` and integer
`1`, or text `'007'` and `'7'` -- stay apart in `DISTINCT`, `GROUP BY`,
`UNION` / `INTERSECT` / `EXCEPT` and `COUNT(DISTINCT ...)`, as in SQLite; a
number sorts before text of equal value, and equal texts by spelling. An
integer and a real of equal value (`1` and `1.0`) are one value.

Text that spells a number is used as that number by arithmetic and by `SUM`
and `AVG`. Arithmetic reads any text by its numeric prefix (see Arithmetic). In
a condition (WHERE, `NOT`, `AND`, CASE), text is likewise read as a number, so
`'x'` is false and `'1x'` true, as in SQLite.

## Writeback

Modifications remain in memory unless the `--write` flag is specified. Then
every table that INSERT, UPDATE, DELETE, ALTER TABLE or TRUNCATE changed, or
that `CREATE TABLE ... LOCATION` created, is written to its file. A table
with no file (CREATE TABLE without LOCATION, CREATE TABLE ... AS) is
skipped, and a table dropped by DROP TABLE is not written.
Rows no statement changed are written back byte for byte, exactly as they
were read; a changed or inserted row is written from its values, NULL as an
empty field. The source file's delimiter, header line (or its absence), byte
order mark, line endings, blank lines, `#` comment lines (under
`--comments`) and permissions are kept.

Every target file is checked before any is written: a read-only file fails
the run (`cannot write 'x.csv': file is read-only`) and no file is written.
Each file is replaced atomically by renaming a temp file over it, so a file
with several hard links is split from them (the other links keep the old
contents); a symlink is followed and stays in place.

## Transactions

```sql
BEGIN [DEFERRED | IMMEDIATE | EXCLUSIVE] [TRANSACTION]
COMMIT [TRANSACTION]
END [TRANSACTION]
ROLLBACK [TRANSACTION]
```

`ROLLBACK` restores every table (rows, schema, created and dropped tables) to
its state at `BEGIN`; `COMMIT` / `END` keeps the changes. The three `BEGIN`
modifiers are accepted and mean plain `BEGIN`. As in SQLite, `BEGIN` inside a
transaction is an error (`cannot start a transaction within a transaction`),
as are `COMMIT` and `ROLLBACK` outside one (`cannot commit - no transaction
is active`). A transaction still open after the last statement, or when the
REPL exits, is rolled back, before any `--write` writeback. `SAVEPOINT`,
`RELEASE` and `ROLLBACK TO` are not supported.

## Unsupported syntax

The SQL parser accepts many dialects. Anything it accepts that sqawk does not
implement is an error ("... is not supported"), never silently ignored, and
nothing is modified. That includes:

- `INSERT OR REPLACE` / `OR IGNORE` / ..., `ON CONFLICT`, `INSERT OVERWRITE`,
  and `RETURNING` on INSERT, UPDATE and DELETE (`REPLACE INTO` does not parse)
- `UPDATE ... FROM`, `DELETE ... USING`, `DELETE t FROM t JOIN ...`, and
  UPDATE / DELETE with `ORDER BY` or `LIMIT`
- `WITH` (common table expressions), `FETCH FIRST`, `FOR UPDATE`, `TOP`,
  `QUALIFY`, `SORT BY` / `CLUSTER BY` / `DISTRIBUTE BY`, `LATERAL VIEW`,
  `SELECT ... INTO`, `GROUP BY ... WITH ROLLUP`
- `TABLESAMPLE`, table hints, `LATERAL` subqueries, column aliases on a FROM
  item
- `WITHIN GROUP`, `IGNORE NULLS`, `ORDER BY` inside an aggregate's argument
  list, `FILTER` or `DISTINCT` on a scalar function, `FILTER` on a window
  function
- `CAST ... FORMAT`, `LIKE ANY`, blob literals (`X'00'`) and parameters
- `COLLATE` (any collation, in expressions and `ORDER BY`)
- a RIGHT JOIN whose left side already contains an outer join, a FULL JOIN
  followed by further joins, parenthesized joins, and `INTERSECT ALL` /
  `EXCEPT ALL`
- table and column constraints, `CREATE TABLE IF NOT EXISTS`,
  `PARTITIONED BY`, `ROW FORMAT`, `STORED AS` any format but `TEXTFILE`,
  `WITH` options other than `DELIMITER`, table comments, and `LOCATION` /
  `STORED AS` / `WITH` or a column list on `CREATE TABLE ... AS`
- savepoints, transaction modes (`ISOLATION LEVEL`, `READ ONLY`) and
  `START TRANSACTION`
- statements other than SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, DROP
  TABLE, ALTER TABLE ADD COLUMN, TRUNCATE and BEGIN / COMMIT / ROLLBACK (no
  views, indexes or triggers)

## Differences from SQLite

sqawk aims to give SQLite's answers. Where it deliberately does not:

| Area | SQLite | sqawk |
|------|--------|-------|
| Column types | Declared affinity converts `'10'` stored in an INTEGER column to 10; text compares with text as text (`'10' < '9'`) | Each cell is typed when read; numeric-looking text compares numerically everywhere, text against text included (`a = '10'` matches 10, `'10' > '9'`), see Type coercion |
| NULL vs `''` | Distinct in output | Both print as an empty field |
| `CAST(x AS STRING)` | NUMERIC affinity: `CAST('x' AS STRING)` is the integer `0` | TEXT: `CAST('x' AS STRING)` is the text `x` |
| `UPPER` / `LOWER` | ASCII letters only | All of Unicode |
| `TRIM(s)` | Strips spaces only | Strips all whitespace |
| `SUM` / `AVG` over text | Non-numeric text counts as 0: `SUM` becomes real, `AVG` divides by every non-NULL value, and over only non-numeric text both give `0.0` | Non-numeric text is skipped: `SUM` stays an integer over integers and integer text, `AVG` divides by the count of numeric values, and over only non-numeric text both give NULL |
| Bare column in an aggregate query (`SELECT k, MAX(y), s FROM t GROUP BY k`, `SELECT COUNT(*), x FROM t`) | Allowed: the value comes from one row of the group | Rejected: `'s' must appear in GROUP BY or be used in an aggregate` |
| `INSERT OR REPLACE`, `ON CONFLICT` | Plain INSERT when the table has no unique constraint | Rejected: sqawk tables have no keys, so the statement cannot do what it asks |
| Subqueries in UPDATE / DELETE | A correlated subquery sees rows already changed | Sees the table as it was before the statement |
| Aggregate of outer-query columns in a subquery (`(SELECT MIN(t.x) FROM u)`) | Evaluated in the outer query, which becomes an aggregate query | Rejected; an aggregate that also reads a column of the subquery's own tables aggregates in the subquery, as in SQLite |
| Derived table (`FROM (SELECT ...)`) reading an outer-query column | Correlated | Rejected (`no such column`): a derived table is computed once, before the statement |
| `ILIKE`, `LPAD`, `RPAD`, `LEFT`, `RIGHT`, `TRIM(... FROM ...)` | Not available | Available |
| `PRINTF` width or precision past the length limit | NULL, or the number wrapped to 31 bits | Error "string or blob too big" |
| Parser | SQLite grammar | sqlparser's Hive dialect with SQLite's operator precedence: `GLOB`, `LIMIT n, m`, `TRIM(s, chars)`, `REPLACE INTO`, `x IN ()` do not parse, `REGEXP` is rejected, and parentheses nest at most 47 deep |

Not implemented: common table expressions, views, indexes, triggers,
`RETURNING`, row values, `GLOB`, and blob values.
