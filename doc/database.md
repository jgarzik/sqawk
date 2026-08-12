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
function arguments and correlated subqueries. Name resolution is carried
separately in a `NameCtx`, which models the visible FROM sources, whether a
column lives behind a cursor or in an already-loaded register, and the
inner/outer scopes a correlated subquery needs. A node supported in one
position is therefore supported in all of them.

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

The file handling system consists of multiple components:

#### `FileHandler` Trait
- Defines a common interface for different file format handlers
- Provides abstraction for loading and saving tables from various file formats
- Allows for consistent treatment of different delimiter-separated formats

#### `CsvHandler` Implementation
- Specialized handler for standard CSV files
- Uses the csv crate for parsing and writing
- Handles comma-separated values with standard CSV escaping rules

#### `DelimHandler` Implementation
- Handles files with custom delimiters (e.g., TSV, colon-separated, etc.)
- Configured via the `-F` command-line option
- Supports tab, colon, pipe, and other custom separators
- Preserves the original delimiter and format during writeback

## File I/O Patterns

### Atomic File Writes

When saving modified tables back to disk (via the `--write` flag), Sqawk uses an atomic write pattern to ensure data integrity:

1. **Write to Temporary File**: Data is first written to a temporary file in the same directory as the target file
2. **Sync to Disk**: The temporary file is explicitly synced (`fsync`) to ensure data is durably stored on the physical device
3. **Atomic Rename**: The temporary file is renamed to the target filename using an atomic rename operation

This pattern provides several safety guarantees:

- **Crash Safety**: If the process crashes during writing, the original file remains intact
- **Disk Full Protection**: If the disk runs out of space, the original file is preserved
- **All-or-Nothing Semantics**: Either the complete new file exists, or the original file is unchanged

The temporary file is created in the same directory as the target file to ensure the rename operation is atomic (same filesystem requirement on POSIX systems).

### Delimiter Consistency

Sqawk maintains delimiter consistency between input and output:

- When a file is loaded with a specific delimiter (via `-F` flag or file extension detection), that delimiter is preserved in the table metadata
- Query results are output using the same delimiter as the source table
- When files are written back, the original delimiter format is preserved

This ensures that TSV files remain TSV, colon-separated files remain colon-separated, and so on. The principle of least surprise: output format matches input format unless explicitly overridden.

### Output Modes

Sqawk supports two primary output modes:

1. **Stdout Output**: Query results (SELECT) are written to standard output using
   the `-F` delimiter, or comma when `-F` was not given. This is independent of
   the delimiter a source table was parsed with
2. **File Writeback**: Modified tables (via INSERT, UPDATE, DELETE) are written back to their source files when `--write` is specified, preserving that file's delimiter

For stdout output, the data is streamed directly without buffering concerns. For file writeback, the atomic write pattern described above is used.

## Storage Backend Architecture

Sqawk supports multiple storage backends for table row data, enabling efficient handling of different data sources:

### Memory Storage

The `MemoryStorage` backend stores all row data in heap-allocated memory:
- Used for tables with no backing file (`CREATE TABLE`, `CREATE TABLE AS
  SELECT`), and for any mmap table after its first modification
- All string values are owned (heap-allocated)
- Supports all operations including INSERT, UPDATE, DELETE
- Data is mutable throughout the session

### Mmap Storage (Default for on-disk files)

The `MmapStorage` backend uses memory-mapped files for zero-copy access:
- Automatically used when loading files from disk
- String values borrow directly from the mmap'd region (zero allocation)
- Read-only by default to maintain zero-copy efficiency
- Converts to MemoryStorage on first write operation

### Automatic Backend Selection

The storage backend is selected automatically based on the data source:
- **On-disk files**: Uses mmap for zero-copy loading
- **Tables created by DDL**: Uses in-memory storage; there is no file to map
- **Write operations**: Mmap tables convert to memory on first modification

Standard input (the `-` operand) is not seekable and cannot be mapped directly,
so it is spooled to a temporary file and then loaded by the ordinary mmap path.
The temp file handle is held for the run's duration, because the mmap borrows
from it. The resulting table is then detached from that path, so `--write` has
no target and reports the table as unwritable rather than rewriting a temp file
that is about to be deleted.

### Row Identity Across Updates

UPDATE replaces a row in place rather than deleting and re-appending it, so a
table keeps its row order and `--write` does not reorder the user's file.

### Write Operation Behavior

When a table with mmap storage is modified (INSERT, UPDATE, DELETE):
1. All rows are copied to heap memory (converting Cow::Borrowed to Cow::Owned)
2. The storage backend switches from Mmap to Memory
3. Subsequent operations use the in-memory copy
4. The original file remains unchanged until `--write` is specified

This design provides:
- Zero-allocation loading for read-only queries
- Transparent handling of write operations
- Memory efficiency for large read-only datasets

## Performance Considerations

Sqawk's in-memory database is optimized for:

- **Fast Loading**: Delimiter-separated files are parsed directly into memory
- **Zero-Copy Access**: Memory-mapped files enable direct access without allocation overhead
- **Format Flexibility**: Support for CSV, TSV, and custom-delimited files
- **Efficient Filtering**: WHERE clauses are applied in a single pass
- **Low Memory Overhead**: Simple data structures minimize memory usage
- **Zero Configuration**: No setup required, works directly with files in various formats

For larger datasets, consider:
- The entire dataset must fit in memory (or be addressable via mmap)
- Complex queries may require multiple passes over the data
- Write operations on mmap tables trigger a copy to memory
- Custom delimiters may have slightly different performance characteristics than standard CSV
- Read-only operations on large files benefit most from mmap storage

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
- **USING / NATURAL**: Not supported
- **Multi-table joins**: Supports chaining 3+ tables in a single query
- **Aggregates over joins**: Supported for inner joins, which are rewritten to
  the equivalent comma join. Deliberately NOT applied to outer joins, since
  moving the ON condition into WHERE would discard the NULL-extended rows an
  outer join exists to produce

### Technical Implementation

- Joins are compiled to VM bytecode for efficient execution
- Nested loop algorithm with appropriate NULL handling for outer joins
- Column references use table qualifiers for disambiguation in result sets
- Type coercion rules are applied consistently in join conditions

## Current System Limitations

The database engine has several architectural limitations:

- **No Index Structure**: All operations perform full table scans
- **No Transaction Support**: A statement's modifications are applied once it
  completes; there is no BEGIN/COMMIT and no rollback across statements
- **Schema Flexibility**: Types are inferred per value rather than enforced
- **No Constraints System**: Referential integrity not enforced
- **Whole-dataset residency**: A table must fit in memory, or be addressable
  via mmap

Unsupported SQL of note: CTEs (`WITH`), explicit window frames
(`ROWS BETWEEN ...`), `USING`/`NATURAL` joins, and aggregates over outer
joins.

---

*This documentation describes the current state of the Sqawk in-memory database as of the project's current version. Future versions may add additional capabilities.*