# Sqawk In-Memory Database Documentation

## Overview

Sqawk implements a lightweight in-memory database engine that supports SQL operations on CSV data. The database is designed for performance and simplicity, focusing on providing essential SQL functionality for data analysis tasks directly on CSV files without requiring an external database engine.

## Table of Contents

1. [Data Model](#data-model)
2. [Architecture](#architecture)
3. [Performance Considerations](#performance-considerations)

## Data Model

### Tables

Tables in Sqawk are in-memory representations of CSV files. Each table has:

- A unique name (derived from the CSV filename or explicitly provided)
- A set of columns with names (derived from the CSV header row)
- Zero or more rows of data
- Optional metadata including the source file path

Tables maintain an internal mapping of column names to their indices for efficient access.

### Rows and Columns

- **Rows**: Represented as vectors of values with one element per column
- **Columns**: Identified by name, with automatic type inference based on content
- **Schema**: Dynamically determined from the CSV header row
- **Column Types**: Column types are not explicitly declared but inferred at runtime

### Table Lifecycle

Tables are loaded from CSV files at startup and can be modified through SQL operations. Sqawk uses a safe, sed-like writeback model:

1. **Default Behavior**: All modifications remain in memory only
2. **Tracking Changes**: The system tracks which tables have been modified by any operation
3. **Write on Exit**: Modified tables are only written back to their source CSV files if:
   - The `--write` (or `-w`) flag is explicitly provided
   - The table was actually modified by an SQL operation (INSERT, UPDATE, DELETE)
4. **Safe Execution**: Without the `--write` flag, source files remain untouched regardless of operations performed
5. **Write Only Modified**: Only tables that were changed are written; unmodified tables are not rewritten

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

### CSV Handler

The `CsvHandler` manages I/O between CSV files and in-memory tables:
- Loads CSV files into tables
- Extracts column names from header rows
- Maintains a registry of loaded tables with their source file paths
- Provides `save_table()` method to write a specific table back to its source file
- Handles table renaming through custom file specifications (`tablename=file.csv`)

## Performance Considerations

Sqawk's in-memory database is optimized for:

- **Fast Loading**: CSV files are parsed directly into memory
- **Efficient Filtering**: WHERE clauses are applied in a single pass
- **Low Memory Overhead**: Simple data structures minimize memory usage
- **Zero Configuration**: No setup required, works directly on CSV files

For larger datasets, consider:
- The entire dataset must fit in memory
- Complex queries may require multiple passes over the data
- Write operations create new copies of the data in memory

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