# Sqawk In-Memory Database Documentation

## Overview

Sqawk implements a lightweight in-memory database engine that supports SQL operations on delimiter-separated data (CSV, TSV, etc.). The database is designed for performance and simplicity, focusing on providing essential SQL functionality for data analysis tasks directly on delimiter-separated files without requiring an external database engine.

## Table of Contents

1. [Data Model](#data-model)
2. [Architecture](#architecture)
3. [File I/O Patterns](#file-io-patterns)
4. [Storage Backend Architecture](#storage-backend-architecture)
5. [Performance Considerations](#performance-considerations)

## Data Model

### Tables

Tables in Sqawk are in-memory representations of delimiter-separated files. Each table has:

- A unique name (derived from the file's filename or explicitly provided)
- A set of columns with names (derived from the file's header row)
- Zero or more rows of data
- Optional metadata including the source file path and delimiter information

Tables maintain an internal mapping of column names to their indices for efficient access.

### Rows and Columns

- **Rows**: Represented as vectors of values with one element per column
- **Columns**: Identified by name, with automatic type inference based on content
- **Schema**: Dynamically determined from the file's header row
- **Column Types**: Not declared; inferred at runtime **per value**, so a single
  column may hold integers, floats and text
- **NULL**: An empty field reads as NULL. Comparisons follow SQL three-valued
  logic, so a comparison involving NULL is UNKNOWN and its row is filtered out
  rather than raising an error. Sorting and grouping treat NULL as equal to
  NULL, which SQL comparison must not — the two use separate paths deliberately
- **Coercion**: A comparison mixing a number and a string converts the string
  when it parses as a number, and compares textually otherwise. See
  [SQL Reference](sql_reference.md) for the exact rules

### Table Lifecycle

Tables are loaded from delimiter-separated files at startup and can be modified through SQL operations. Sqawk uses a safe, sed-like writeback model:

1. **Default Behavior**: All modifications remain in memory only
2. **Tracking Changes**: The system tracks which tables have been modified by any operation
3. **Write on Exit**: Modified tables are only written back to their source files if:
   - The `--write` (or `-w`) flag is explicitly provided
   - The table was actually modified by an SQL operation (INSERT, UPDATE, DELETE)
4. **Safe Execution**: Without the `--write` flag, source files remain untouched regardless of operations performed
5. **Write Only Modified**: Only tables that were changed are written; unmodified tables are not rewritten
6. **Format Preservation**: Original file formats and delimiters are preserved during writeback

This design ensures that users can experiment with data manipulations while maintaining the integrity of source files. The verbose mode (`-v`) provides additional confirmation about whether changes were saved or not.

## Architecture

The in-memory database system consists of these primary components:

### Table Module

The `Table` struct represents an in-memory table with:
- Column metadata
- Row data
- Methods for accessing and manipulating rows
- Projection capabilities (selecting subsets of columns)

### SQL Execution: a bytecode VM

SQL is not interpreted against the tables directly. It is compiled to bytecode
and run on a register virtual machine, following SQLite's architecture:

1. **Parse** — the `sqlparser` crate produces an AST
2. **Compile** — `vm::compiler` lowers the AST to a bytecode program
3. **Execute** — `vm::engine` runs that program against table cursors

`SqlExecutor` is a thin wrapper over this pipeline: it forwards to the VM,
tracks which tables were modified, and provides `save_modified_tables()`, which
writes back only tables that actually changed.

The compiler has one recursive expression compiler, `code_expr` (the analogue
of SQLite's `sqlite3ExprCode`), used from every position an expression can
appear: the SELECT list, WHERE, HAVING, ORDER BY, SET clauses, join conditions,
function arguments and subqueries. Name resolution is carried separately in a
`NameCtx`, which models the visible FROM sources, whether a column lives
behind a cursor or in an already-loaded register, and -- for a subquery -- a
link to the enclosing query's scope. A node supported in one position is
therefore supported in all of them.

### SELECT Pipeline

Every SELECT core runs one pipeline (`src/vm/compiler_select.rs`):

    FROM planner -> WHERE -> [aggregate stage -> HAVING] -> [window stage] -> output stage

- A SELECT without FROM is a FROM plan with zero sources, producing one row,
  so WHERE, ORDER BY, DISTINCT and LIMIT apply to it as to any other query.
- The aggregate stage collects aggregate calls from the SELECT list, HAVING
  and ORDER BY. Without GROUP BY there is always exactly one output row (COUNT
  of no rows is 0, SUM/AVG/MIN/MAX of no rows NULL); with GROUP BY, no input
  rows means no output rows.
- The window stage runs after aggregation, so a window may range over
  grouped rows (`SUM(COUNT(*)) OVER ()`) and over any join. Each finished row
  goes into an ephemeral table with everything the output needs (all FROM
  columns, or the GROUP BY keys and aggregates) plus each window's partition
  keys, order keys and arguments. For each distinct window definition
  (PARTITION BY + ORDER BY) one `WindowCompute` stably sorts the table by its
  keys, walks the partitions and peer groups, and writes each function's
  value -- each with its own state and frame -- into a result column. The
  definitions are computed last-first, so without ORDER BY rows come out in
  the first window's order, as in SQLite. The output stage then reads the
  rows back, with every window call bound to its value like an aggregate, so
  a window call may appear in any expression and in ORDER BY; anywhere else
  (WHERE, GROUP BY, HAVING, an aggregate argument) it is an error.
- The output stage computes the output columns, then ORDER BY (an ordinal, a
  SELECT alias, a projected expression, or else a hidden sort key such as an
  unprojected column or aggregate), DISTINCT, and LIMIT/OFFSET. HAVING and
  sort-key expressions may name SELECT aliases. LIMIT and OFFSET accept any
  expression yielding an integer; a negative LIMIT means no limit.
- Compound SELECTs (UNION [ALL], INTERSECT, EXCEPT, in any chain) take their
  header from the leftmost SELECT; ORDER BY (ordinals or leftmost column
  names) and LIMIT/OFFSET apply to the whole compound, and a DISTINCT on one
  arm applies to that arm only. As in SQLite, set operators use keyed
  ephemeral tables (`OpenEphemeral` with p3 = 1, `IdxInsert`, `IdxDelete`,
  `Found`/`NotFound`), and without ORDER BY a UNION, INTERSECT or EXCEPT
  result comes out sorted. INTERSECT ALL / EXCEPT ALL are not supported.

### Subqueries

A subquery (scalar, `EXISTS`, `IN`) is compiled by the same query compiler as
a statement, as a fragment of the enclosing statement's program placed where
the expression is evaluated (`src/vm/compiler_subquery.rs`), as in SQLite. So
every SELECT feature works inside one. Only the destination differs: a scalar
subquery copies its first row's value into a register and stops, EXISTS sets
a flag on its first row and stops, and IN inserts its rows into a keyed
ephemeral table that the left operand probes with `Found`, with SQL's
three-valued result (a NULL member makes a miss NULL; an empty set makes IN
false even for a NULL operand).

- **Name resolution**: the subquery's FROM scope links to the scope of the
  expression containing it. A name is resolved innermost scope first, then
  outward, so the inner table shadows an outer one with the same column and an
  outer table is found by its alias or, unaliased, by its own name even when
  the subquery reads the same table under another alias.
- **Correlation**: a subquery that resolves any name outward -- itself or
  through a subquery nested in it -- is correlated. This is decided during
  name resolution, not by matching qualifier strings. An uncorrelated
  subquery's fragment is guarded by `Once` and runs for the first evaluation
  only; a correlated one reruns on every evaluation, i.e. per outer row.
- **Correlated execution**: a subquery's cursors and hash indexes do not
  depend on the outer row, so they are set up once (`Once` around the
  `OpenRead`s and hash builds). An equality between an inner column and an
  outer column -- `WHERE d.k = f.k` -- becomes a hash probe, so a correlated
  `EXISTS`/`IN`/scalar lookup costs one probe per outer row instead of a
  rescan. Aggregate accumulators, sorters and DISTINCT tables are reset at
  the start of every run.
- A subquery's cursors are numbered apart from the statement's, so it never
  disturbs an enclosing scan.

### Statement Execution Model

A script is parsed once, then each statement is compiled and executed on its
own, with its modifications applied before the next statement compiles. Two
consequences follow:

- Each statement produces its own result set, with its own column schema. They
  are not merged.
- A statement observes the database as the previous one left it, which is what
  `CREATE TABLE t; INSERT INTO t ...; SELECT * FROM t` requires.

A subquery in `FROM` (a derived table) is materialized before its enclosing
statement compiles: the subquery runs, its result is registered under the
alias, and the statement is rewritten to reference that name. The registration
lasts only for that statement.

### File Handlers

- `FileHandler` (`file_handler.rs`) resolves `[table_name=]path` operands,
  chooses the delimiter (`-F`, else comma for `.csv`, no extension and stdin,
  tab otherwise) and the header policy (`.csv` and stdin: header always;
  other files: headerless guess and `#` comments; `--no-header`/`--tabledef`:
  no header), and routes writeback.
- `csv_handler.rs` does all parsing and writing. Every format is read by the
  `csv` crate with one configuration (RFC 4180 quoting, multi-line quoted
  fields, no trimming, BOM stripped, ragged rows padded), and written back by
  a small RFC 4180 record writer that also quotes fields with leading or
  trailing whitespace.

## File I/O Patterns

### Atomic File Writes

When saving modified tables back to disk (via the `--write` flag), Sqawk uses an atomic write pattern to ensure data integrity:

1. **Write to Temporary File**: Data is first written to a temporary file in the same directory as the target file
2. **Keep Permissions**: The original file's permissions are copied onto the temporary file
3. **Sync to Disk**: The temporary file is explicitly synced (`fsync`) to ensure data is durably stored on the physical device
4. **Atomic Rename**: The temporary file is renamed to the target filename using an atomic rename operation

This pattern provides several safety guarantees:

- **Crash Safety**: If the process crashes during writing, the original file remains intact
- **Disk Full Protection**: If the disk runs out of space, the original file is preserved
- **All-or-Nothing Semantics**: Either the complete new file exists, or the original file is unchanged

The temporary file is created in the same directory as the target file to ensure the rename operation is atomic (same filesystem requirement on POSIX systems). A symlinked path is resolved first, so the link's target is rewritten and the link itself is left in place.

### Delimiter Consistency

Sqawk maintains delimiter consistency between input and output:

- When a file is loaded with a specific delimiter (via `-F` flag or file extension detection), that delimiter is preserved in the table metadata
- When files are written back, the original delimiter format is preserved

This ensures that TSV files remain TSV, colon-separated files remain colon-separated, and so on. The principle of least surprise: output format matches input format unless explicitly overridden.

### Output Modes

Sqawk supports two primary output modes:

1. **Stdout Output**: Query results (SELECT) are written to standard output using
   the `-F` delimiter, or comma when `-F` was not given. This is independent of
   the delimiter a source table was parsed with
2. **File Writeback**: Modified tables (via INSERT, UPDATE, DELETE) are written back to their source files when `--write` is specified, preserving that file's delimiter

For stdout output, the data is streamed directly without buffering concerns. For file writeback, the atomic write pattern described above is used.

## Storage

Every table holds its rows in memory as owned values (`Vec<Row>`). Files are
read whole and parsed by the `csv` crate -- RFC 4180 quoting, quoted fields
spanning lines, no trimming, UTF-8 byte order mark stripped -- and each cell's
type is inferred with a rule that round-trips (see "Type inference" below), so
an unmodified cell is written back byte for byte.

Standard input (the `-` operand) is read the same way, directly from the pipe.
The resulting table has no file path, so `--write` reports it as unwritable
rather than writing anywhere.

### Type inference

- empty field: NULL
- canonical integer (`42`, `-7`, where formatting the number back gives the
  same text): INTEGER
- canonical float (`1.5`, `-0.25`): REAL
- anything else, verbatim: TEXT -- including `007`, `1.50`, `1e3`, ` 42`,
  `true`, `NaN`

Text that spells a number still acts as one where a number is needed:
arithmetic, `SUM`/`AVG`, comparison against a number, and sort order.

### Source layout

Each table records how its file was laid out -- whether it had a header line,
whether records ended in CRLF, and whether `#` lines were comments -- and
writeback reproduces it. Writeback also keeps the file's permissions and, for
a symlink, rewrites the link's target and leaves the link in place.

In a file read with comment skipping (any delimited file that is not `.csv`),
each `#` comment line is kept verbatim, with its own line ending, and
anchored to the data that follows it: comments before the header (or, in a
headerless file, before the first row) stay at the top, comments before a
row stay with that row, and comments after the last row stay at the end. So
a no-op UPDATE writes such a file back byte for byte. When rows change:

- UPDATE replaces rows in place, so every comment stays where it was.
- DELETE leaves a deleted row's comments where they were: in front of the
  next surviving row, or of the trailing comments. Deleting every row (or
  TRUNCATE) keeps all comments, in order.
- INSERT appends rows after the last row and before the trailing comments.

Blank lines are not kept (the reader skips them), and `#` lines in a `.csv`
file are data, not comments.

### Row Identity Across Updates

UPDATE replaces a row in place rather than deleting and re-appending it, so a
table keeps its row order and `--write` does not reorder the user's file.

## Performance Considerations

Sqawk's in-memory database is optimized for:

- **Fast Loading**: Delimiter-separated files are parsed directly into memory
- **Format Flexibility**: Support for CSV, TSV, and custom-delimited files
- **Efficient Filtering**: WHERE clauses are applied in a single pass
- **Low Memory Overhead**: Simple data structures minimize memory usage
- **Zero Configuration**: No setup required, works directly with files in various formats

For larger datasets, consider:
- The entire dataset must fit in memory
- Complex queries may require multiple passes over the data

## Join Implementation

The database engine supports combining data from multiple tables through SQL-standard join operations.

### Supported Join Types

- **INNER JOIN**: Returns rows when there is a match in both tables
- **LEFT OUTER JOIN**: Returns all rows from the left table, with matched rows from the right (or NULLs)
- **RIGHT OUTER JOIN**: Returns all rows from the right table, with matched rows from the left (or NULLs)
- **FULL OUTER JOIN**: Returns all rows from both tables, with NULLs where no match exists
- **CROSS JOIN**: Returns the Cartesian product of both tables

### Join Syntax

- **ON constraints**: Supported, and the condition is a full expression, not
  just a single comparison: `ON a.id = b.id AND UPPER(a.name) = 'X'`
- **Table aliases**: Supported (`FROM users u JOIN orders o ON u.id = o.user_id`)
- **USING / NATURAL**: Supported; the shared columns are merged into one
  output column (the first non-NULL of the joined columns)
- **Multi-table joins**: Any chain of INNER / LEFT / CROSS joins and comma
  lists. RIGHT JOIN is supported when everything to its left is inner-joined;
  FULL JOIN must be the last join
- **Expressions**: The SELECT list, ON, WHERE, GROUP BY, HAVING and ORDER BY
  of a join accept the same expressions as a single-table query
- **Aggregates over joins**: Supported for every join type, including the
  NULL-extended rows of outer joins

### Technical Implementation

- One FROM planner (`src/vm/compiler_from.rs`) serves every SELECT: each
  source table is a cursor and a loop level, outermost first
- ON conditions are evaluated at their join's level; WHERE conditions after
  outer-join NULL-extension, at the innermost level that has all their tables
- Outer joins keep a per-level match flag; an unmatched row re-runs the inner
  levels once with the inner tables on a NULL row (`NullRow`). RIGHT JOIN is a
  LEFT JOIN with the loop order swapped; FULL JOIN adds a pass over the right
  table for rows that never matched
- An equality between the inner table and the outer tables becomes a hash
  join (`HashInsert` / `HashProbe` / `HashNext`). Keys are normalized so that
  values `=` treats as equal (1, 1.0, '1') share a bucket; NULL keys never
  match; the full join condition is re-checked on every candidate

## Current System Limitations

The database engine has several architectural limitations:

- **No Index Structure**: Tables are scanned in full; the only index is the
  temporary hash index an equi-join builds for its inner table
- **No Transaction Support**: A statement's modifications are applied once it
  completes; there is no BEGIN/COMMIT and no rollback across statements
- **Schema Flexibility**: Types are inferred per value rather than enforced
- **No Constraints System**: Referential integrity not enforced
- **Whole-dataset residency**: A table must fit in memory

Unsupported SQL of note: CTEs (`WITH`), parenthesized joins, and RANGE
frames with an offset, GROUPS frames and FILTER on window functions.

---

*This documentation describes the current state of the Sqawk in-memory database as of the project's current version. Future versions may add additional capabilities.*