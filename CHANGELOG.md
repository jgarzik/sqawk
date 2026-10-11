# Changelog

All notable changes to sqawk are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/).

## [0.9.0] - 2026-10-11

sqawk now gives SQLite 3.45's answers, apart from a short list of documented
differences ("Differences from SQLite" in `doc/sql_reference.md`), and
`--write` reproduces every row a statement did not change byte for byte. Many
results, headers and error messages change as a consequence; they are listed
under **Breaking changes**.

### Breaking changes

#### Files and output

- **File types and headers.** `.tsv` and `.tab` files are tab-separated;
  every other file, and standard input, is comma-separated unless `-F` says
  otherwise. The first line of every file is the header row unless
  `--no-header` or `--tabledef` is given; sqawk no longer guesses whether a
  file has a header.
- **`#` lines are data** unless the new `--comments` flag is given (for any
  file type). Previously non-`.csv` files skipped them silently.
- **NULL prints as an empty field** in results and is written back as one. A
  row consisting of a single NULL prints as `""`.
- **Real numbers print as SQLite prints them** (`%!.15g`): 15 significant
  digits, and a whole number keeps `.0` (`AVG` of integers gives `77500.0`,
  `ROUND(2.0, 2)` gives `2.0`, `1e20` gives `1.0e+20`, infinities `Inf` /
  `-Inf`). A loaded field is a REAL only if it is printed back exactly the
  same way; `1.50`, `1e3` and `007` stay text.
- **Result column names follow SQLite**: a column reference is named by the
  bare column (`u.name` gives `name`), `*` over a join keeps duplicate names,
  and an unaliased expression is named by its SQL text as written
  (`COUNT(*)`, `upper(name)||1`) instead of `COUNT` / `expr`.
- **Query output delimiter** is `--output-delimiter`, else `-F`, else comma.
- Fields are quoted per RFC 4180 on output, including fields with leading or
  trailing spaces.

#### SQL semantics

- **One comparison rule everywhere** (`=`, `<`, `IN`, `IN (SELECT ...)`,
  joins, `ORDER BY`, `MIN`/`MAX`, windows): two values that both read as
  numbers compare as numbers, text against text included (`'10' > '9'`,
  `'007' = 7`); numbers sort before other text; other text compares byte by
  byte. `DISTINCT`, `GROUP BY` and the compound operators keep text `'1'` and
  integer `1` apart, as SQLite does.
- **Integer arithmetic stays integer**: `5 / 2` is `2`, `-7 / 2` is `-3`,
  `%` is the integer remainder; an overflowing result becomes a real; division
  or remainder by zero is NULL; text operands are read by their numeric
  prefix.
- **`LIKE` is case-insensitive** for ASCII letters, `_` and `%` match any
  character including a newline, and `ESCAPE` is honoured. `ILIKE` folds all
  of Unicode.
- **Operator precedence is SQLite's**: `=`, `IS`, `IN`, `LIKE` and `BETWEEN`
  share one level below `<`, `<=`, `>`, `>=`; `||` binds tightest of the
  binary operators. Compound SELECTs (`UNION`, `INTERSECT`, `EXCEPT`) have
  equal precedence and apply left to right.
- **`ROUND(x, -N)`** treats a negative `N` as 0, as SQLite does
  (`ROUND(1234.5, -2)` is `1235.0`).
- `SUM` of integers that overflows 64 bits is the error `integer overflow`;
  `SUM`, `AVG` and `TOTAL` use compensated (Neumaier) summation. `SUM` /
  `AVG` / `MIN` / `MAX` over no rows are NULL.
- `CAST` follows SQLite's affinity rules for every type name.
- String functions count characters, not bytes; `||` with a NULL operand is
  NULL, while `CONCAT` treats NULL as `''`; `SUBSTR` follows SQLite's rules
  for zero and negative positions.
- `UNION`, `INTERSECT` and `EXCEPT` results without `ORDER BY` come out
  sorted, as in SQLite.
- Table names match case-insensitively (`FROM EMP` finds `emp.csv`).

#### Names and errors

- **Unknown qualifiers are errors**: `zz.k`, or a table's own name after it
  has been aliased, no longer falls back to the only table in FROM
  (`no such column: zz.k`). The same applies in UPDATE and DELETE.
- **Schema qualifiers**: `main.t` is accepted; any other schema is an error
  (`no such table: foo.t`).
- An aggregate whose arguments read only outer-query columns, and a derived
  table that reads an outer-query column, are errors instead of wrong
  answers.
- **Unsupported syntax is an error**, never silently ignored: `RETURNING`,
  `ON CONFLICT`, `INSERT OR ...`, `UPDATE ... FROM`, `DELETE ... USING`,
  `WITH`, `QUALIFY`, table constraints, `STORED AS` other than `TEXTFILE`,
  `WITH` options other than `DELIMITER`, savepoints and more (see
  "Unsupported syntax" in `doc/sql_reference.md`). Malformed `--tabledef`
  values and definitions for tables no file loads are errors too.
- **Error format**: every error is one line on standard error,
  `sqawk: <message>`, with exit status 1; command-line usage errors exit
  with status 2. Messages follow SQLite where it has one (`no such column: x`,
  `no such table: t`, `no such function: F`, `misuse of aggregate function
  F()`, `ambiguous column name: c`). Load errors read `FILE: line N:
  MESSAGE`. The SQL text and cause chains are no longer echoed.
- `-v` output goes to standard error; its format is documented as unstable.

#### Command line and REPL

- `-i` together with `-s` or `-f` is a usage error (exit status 2).
- **REPL output**: when standard input is not a terminal, the REPL prints no
  banner, prompts or status messages, and "Query returned N rows" is gone in
  both modes; results print exactly as with `-s`.
- **REPL exit status**: `.exit N` sets the exit status; a piped script with
  any failed statement or dot-command exits 1. Unknown dot-commands and
  failed ones are errors.
- **REPL history** is kept only for a terminal, in
  `$XDG_STATE_HOME/sqawk/history`, else `~/.sqawk_history` -- never in the
  current directory.
- Loading the same file twice, or under an empty table name, is an error; a
  `NAME=` prefix names the table only when `NAME` is identifier-like.

#### Packaging

- `cargo install sqawk` installs only the `sqawk` binary. The `tsq` test-data
  generator is an unpublished workspace crate (`cargo run -p tsq`).
- The crate is binary-only: the `sqawk` library target, which exposed
  internal modules, is gone.

### Added

- `-f FILE` / `--file FILE` runs the statements in a file (`-` for standard
  input); repeatable and ordered with `-s`.
- `--field-separator` (long form of `-F`), `--output-delimiter`,
  `--no-output-header`, `--no-header` and `--comments`.
- Transactions: `BEGIN [DEFERRED | IMMEDIATE | EXCLUSIVE]`, `COMMIT` / `END`,
  `ROLLBACK`. A transaction still open at the end is rolled back before any
  writeback.
- Joins: `USING` and `NATURAL`, any chain of INNER / LEFT / CROSS joins,
  expressions and aggregates over every join type, and hash joins for
  equality conditions.
- Subqueries are full queries (joins, GROUP BY, ORDER BY, LIMIT, compounds)
  in any expression position, including UPDATE SET, DELETE WHERE and INSERT
  VALUES; correlated subqueries at any depth. Derived tables may appear in any
  FROM, and their alias is optional.
- `INSERT ... SELECT` and `CREATE TABLE ... AS` accept any query.
- Window functions: `PERCENT_RANK`, `CUME_DIST`, `NTILE`, `LAG` / `LEAD`
  with offset and default, `FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE`, and the
  aggregates over frames; `ROWS` frames, `RANGE` frames with unbounded /
  current-row bounds, peers, named windows, several different `OVER` clauses
  in one query, window calls inside expressions and in ORDER BY, and windows
  over grouped rows.
- Aggregates: `TOTAL`, `GROUP_CONCAT(x [, sep])`, `STRING_AGG(x, sep)`,
  `FILTER (WHERE ...)`, and `DISTINCT` on any aggregate.
- `ORDER BY ... NULLS FIRST | NULLS LAST`; `IS [NOT] DISTINCT FROM` and
  NULL-safe `IS` / `IS NOT`.
- Scalar functions: `INSTR`, `TYPEOF`, `IFNULL`, `IIF`, `LPAD`, `RPAD`,
  `CONCAT_WS`, `HEX`, `CHAR`, `UNICODE`, `SIGN`, `PRINTF` / `FORMAT` (a port
  of SQLite's printf), `QUOTE`, multi-argument `MIN` / `MAX`, two-argument
  `LTRIM` / `RTRIM`, and `TRIM([BOTH | LEADING | TRAILING] chars FROM s)`.
- Date and time: `DATETIME`, `JULIANDAY`, `UNIXEPOCH`, `STRFTIME`,
  `TIMEDIFF` and SQLite's time strings and modifiers (a port of SQLite's
  `date.c`, including `localtime` / `utc`, `subsec`, `auto` and negative
  years).
- Hex integer literals (`0x1F`) and the bitwise operators `&`, `|`, `<<`,
  `>>`, `~`.
- Verbatim writeback: rows no statement changed are written back exactly as
  read (quoting, spacing, ragged rows, line endings, invalid UTF-8), and the
  byte order mark, blank lines and `--comments` lines are kept in place.
- `--write` checks every target first and refuses a read-only file before
  writing anything (`cannot write 'x.csv': file is read-only`).
- REPL: tab completion of dot-commands, SQL keywords, function names, table
  and column names; `.tables PATTERN` with LIKE patterns; a final statement
  without `;` runs at end of input; Ctrl-C abandons the current entry.
- Limits as in SQLite: expression depth 1000 and 500 compound terms, each a
  clean error.

### Fixed

- Wrong answers: ORDER BY, LIMIT and DISTINCT were ignored on joins and
  compound SELECTs; outer joins applied WHERE before NULL-extension; `NOT`,
  `NOT IN`, `NOT BETWEEN` and `NOT LIKE` mishandled NULL; GROUP BY compared
  only its first key; `INSERT ... SELECT` and `CREATE TABLE ... AS` dropped
  ORDER BY, LIMIT, GROUP BY and joins; a correlated subquery could bind to the
  wrong table.
- Data safety: a header-less guess could turn the first row into column
  names; `#` lines were deleted on `--write`; values were rewritten in a
  different form; an unclosed quote silently swallowed the rest of the file;
  duplicate or empty header names were accepted; a read-only file was
  replaced.
- Crashes: arithmetic, `ABS`, `SUBSTR`, `PRINTF` widths and date modifiers no
  longer overflow or panic; deeply nested expressions are an error instead of
  a stack overflow; a closed standard output or standard error never panics.
- Line numbers in load errors are right for CRLF and CR-only files.
- `CURRENT_DATE` and the date functions no longer use a 365-day year and
  30-day months.
- Performance: correlated `EXISTS` / `IN` and equality joins use hash
  lookups (a 200k-row correlated `EXISTS` went from 13.4 s to 0.05 s); the
  release binary is built with `codegen-units = 1` and stripped (9.2 MB to
  5.4 MB).

[0.9.0]: https://github.com/jgarzik/sqawk/compare/v0.8.2...v0.9.0
