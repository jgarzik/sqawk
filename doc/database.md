# Sqawk In-Memory Database Documentation

## Overview

Sqawk implements a lightweight in-memory database engine that supports SQL operations on delimiter-separated data (CSV, TSV, etc.). The database is designed for performance and simplicity, focusing on providing essential SQL functionality for data analysis tasks directly on delimiter-separated files without requiring an external database engine.

## Table of Contents

1. [Data Model](#data-model)
2. [Architecture](#architecture)
3. [Storage Backend Architecture](#storage-backend-architecture)
4. [Performance Considerations](#performance-considerations)

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
- **Column Types**: Column types are not explicitly declared but inferred at runtime

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

### SQL Executor

The `SqlExecutor` implements SQL parsing and execution:
- Uses `sqlparser` crate to parse SQL statements
- Converts parsed AST to operations on in-memory tables
- Handles WHERE clause evaluation
- Maintains a set of modified table names (`modified_tables`) to track changes
- Provides `save_modified_tables()` method that only writes back tables that were actually modified

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

## Storage Backend Architecture

Sqawk supports multiple storage backends for table row data, enabling efficient handling of different data sources:

### Memory Storage (Default for stdin)

The `MemoryStorage` backend stores all row data in heap-allocated memory:
- Used when reading from stdin or pipes
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
- **Stdin/pipes**: Uses traditional in-memory storage
- **Write operations**: Mmap tables convert to memory on first modification

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

The database engine includes support for combining data from multiple tables through joins:

### Join Engine Design

- **Cross Join Implementation**: Creates a Cartesian product of all rows
- **Filter-Based Joins**: Uses WHERE conditions for relationship-based filtering
- **Multi-table Support**: Handles joining multiple tables in sequence

### Column Naming Strategy

To maintain clarity when working with multiple tables:

- **Qualified Naming**: Columns are prefixed with their table names
- **Consistent Referencing**: A consistent naming convention is applied in multi-table operations
- **Qualified References**: Column references include table qualifiers in conditions

### Technical Implementation

- The join operation first creates a cross product, then applies filters
- Tables are processed in the order specified
- The column naming system ensures disambiguation in result sets
- Type coercion rules are applied consistently in join conditions

## Current System Limitations

The database engine has several architectural limitations:

- **No Index Structure**: All operations perform full table scans
- **Limited Join Capabilities**: Advanced join syntax not implemented
- **No Transaction Support**: Operations are applied immediately with no rollback capability
- **Schema Flexibility**: Types are inferred rather than enforced
- **No Constraints System**: Referential integrity not enforced

---

*This documentation describes the current state of the Sqawk in-memory database as of the project's current version. Future versions may add additional capabilities.*