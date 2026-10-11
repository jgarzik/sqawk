//! SQL execution module for sqawk
//!
//! This module provides a thin wrapper around the VM execution engine for SQL processing.
//! It handles:
//!
//! - SQL statement execution via the bytecode VM engine
//! - Tracking of modified tables for selective write-back
//! - Table management operations for the REPL interface
//!
//! The module implements a non-destructive approach, modifying only in-memory tables
//! until explicitly requested to save changes back to the original files.

use std::collections::HashSet;

use anyhow::Result;

use crate::capacity::DEFAULT_TABLE_CAPACITY;
use crate::config::AppConfig;
use crate::database::Database;
use crate::error::{SqawkError, SqawkResult};
use crate::file_handler::FileHandler;
use crate::table::DataType;

/// SQL statement executor
///
/// This executor wraps the VM execution engine and provides additional functionality
/// for tracking modified tables and integrating with the REPL interface.
pub struct SqlExecutor<'a> {
    /// Database for storing and accessing tables
    database: &'a mut Database,

    /// File handler for loading and saving tables
    file_handler: &'a mut FileHandler,

    /// Names of tables that have been modified
    modified_tables: HashSet<String>,

    /// Application configuration for global settings
    config: AppConfig,

    /// Number of rows affected by the last DML statement
    affected_rows: usize,
    /// Whether the last executed script contained a row-counting DML statement
    dml_executed: bool,
}

impl<'a> SqlExecutor<'a> {
    /// Create a new SQL executor with the given database, file handler, and application configuration
    pub fn new(
        database: &'a mut Database,
        file_handler: &'a mut FileHandler,
        config: &AppConfig,
    ) -> Self {
        SqlExecutor {
            database,
            file_handler,
            modified_tables: HashSet::with_capacity(DEFAULT_TABLE_CAPACITY),
            config: config.clone(),
            affected_rows: 0,
            dml_executed: false,
        }
    }

    /// Execute an SQL statement using the VM engine
    ///
    /// This method delegates execution to the VM-based bytecode engine, which:
    /// 1. Parses the SQL using sqlparser
    /// 2. Compiles the parsed SQL into bytecode
    /// 3. Executes the bytecode in a VM
    ///
    /// # Arguments
    /// * `sql` - SQL statement to execute
    ///
    /// # Returns
    /// * One result table per statement that produced rows, in order. A
    ///   multi-statement script yields one result set per statement; they are
    ///   deliberately NOT merged, since each has its own column schema.
    pub fn execute(&mut self, sql: &str) -> SqawkResult<Vec<crate::table::Table>> {
        if self.config.verbose() {
            eprintln!("Executing SQL: {}", sql);
        }

        // Execute via VM engine
        let result = crate::vm::execute_vm(sql, self.database, self.config.verbose())?;

        // Track modified tables
        for table_name in result.modified_tables {
            self.modified_tables.insert(table_name);
        }

        // Track affected rows from the last statement
        self.affected_rows = result.affected_rows;
        self.dml_executed = result.dml_executed;

        // Set delimiter on each result table to match config
        let mut tables = result.tables;
        if let Some(delim) = self.config.field_separator() {
            for t in &mut tables {
                t.set_delimiter(delim.clone());
            }
        }

        Ok(tables)
    }

    /// Save all modified tables back to their source files
    ///
    /// This function writes any tables that have been modified during execution
    /// (e.g., through INSERT, UPDATE, or DELETE statements) back to their source files.
    /// Only tables that have been modified will be saved, preserving the original
    /// files if no changes were made.
    ///
    /// Every table is checked for a writeback target before any of them is
    /// written. An unwritable table (one read from standard input, say) used
    /// to surface on its own turn in the loop, after an arbitrary number of
    /// real source files had already been rewritten -- and since
    /// `modified_tables` is a `HashSet`, which ones varied between runs of the
    /// same command. Sorting makes the writes themselves ordered too.
    ///
    /// # Returns
    /// * `Ok(usize)` - Number of tables saved
    /// * `Err` if any error occurs during saving
    pub fn save_modified_tables(&self) -> Result<usize> {
        let mut names: Vec<&String> = self.modified_tables.iter().collect();
        names.sort();

        for table_name in &names {
            self.file_handler.check_table_writable(table_name)?;
        }

        for table_name in &names {
            // Use the file handler to write the table back to its source file
            self.file_handler.save_table(table_name)?;
        }

        Ok(names.len())
    }

    /// Check if a specific table has been modified
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to check
    ///
    /// # Returns
    /// * `bool` - True if the table has been modified
    pub fn is_table_modified(&self, table_name: &str) -> bool {
        self.modified_tables.contains(table_name)
    }

    /// Get a list of all available table names
    ///
    /// # Returns
    /// * `Vec<String>` - List of table names
    pub fn table_names(&self) -> Vec<String> {
        self.database.table_names()
    }

    /// Get column names for a specific table
    ///
    /// # Arguments
    /// * `table_name` - Name of the table
    ///
    /// # Returns
    /// * `SqawkResult<Vec<String>>` - List of column names
    pub fn get_table_columns(&self, table_name: &str) -> SqawkResult<Vec<String>> {
        let table = self.database.get_table(table_name)?;
        Ok(table.columns().to_vec())
    }

    /// Get the column definitions with type information for a table
    ///
    /// # Arguments
    /// * `table_name` - The name of the table
    ///
    /// # Returns
    /// * `SqawkResult<Vec<(String, DataType)>>` - List of column names with their data types
    pub fn get_table_column_types(&self, table_name: &str) -> SqawkResult<Vec<(String, DataType)>> {
        let table = self.database.get_table(table_name)?;
        let column_types = table
            .column_metadata()
            .iter()
            .map(|col| (col.name.clone(), col.data_type))
            .collect();
        Ok(column_types)
    }

    /// Check if any tables have been modified
    ///
    /// # Returns
    /// * `bool` - True if any tables have been modified
    pub fn has_modified_tables(&self) -> bool {
        !self.modified_tables.is_empty()
    }

    /// Load a file as a table
    ///
    /// # Arguments
    /// * `file_spec` - File specification in format [table_name=]file_path
    ///
    /// # Returns
    /// * `SqawkResult<Option<(String, String)>>` - Tuple of (table_name, file_path) if successful
    pub fn load_file(&mut self, file_spec: &str) -> SqawkResult<Option<(String, String)>> {
        self.file_handler.load_file(file_spec)
    }

    /// Execute SQL statement and return a ResultSet for REPL mode
    ///
    /// # Arguments
    /// * `sql` - SQL statement to execute
    ///
    /// # Returns
    /// * One ResultSet per statement that produced rows, in order.
    ///
    /// Returning only the last one hid earlier result sets AND suppressed the
    /// change count of a trailing DML statement, because the caller treated
    /// "there is a result set" as "this line produced no changes".
    pub fn execute_sql(&mut self, sql: &str) -> Result<Vec<ResultSet>> {
        let result = self.execute(sql)?;

        Ok(result
            .iter()
            .map(|table| ResultSet {
                columns: table.columns().to_vec(),
                rows: table.rows_as_strings(),
            })
            .collect())
    }

    /// Whether the last executed script ran a row-counting DML statement.
    ///
    /// Distinguishes "a DML ran and changed nothing" from "no DML ran", which
    /// a count of zero cannot express on its own.
    pub fn last_statement_changed_rows(&self) -> bool {
        self.dml_executed
    }

    /// Check if a table exists
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to check
    ///
    /// # Returns
    /// * `bool` - True if the table exists
    pub fn table_exists(&self, table_name: &str) -> bool {
        self.file_handler.has_table(table_name)
    }

    /// Check if a table is modified (alias for is_table_modified)
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to check
    ///
    /// # Returns
    /// * `bool` - True if the table has been modified
    pub fn table_is_modified(&self, table_name: &str) -> bool {
        self.modified_tables.contains(table_name)
    }

    /// Save a specific table
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to save
    ///
    /// # Returns
    /// * `SqawkResult<()>` - Success or error
    pub fn save_table(&self, table_name: &str) -> SqawkResult<()> {
        if !self.file_handler.has_table(table_name) {
            return Err(SqawkError::TableNotFound(table_name.to_string()));
        }

        if !self.modified_tables.contains(table_name) {
            // Table exists but isn't modified, just return success
            return Ok(());
        }

        self.file_handler.save_table(table_name)
    }

    /// Get the number of rows affected by the last executed statement
    ///
    /// Returns the count of rows affected by the most recent INSERT, UPDATE, or DELETE.
    pub fn get_affected_row_count(&self) -> SqawkResult<usize> {
        Ok(self.affected_rows)
    }
}

/// Result set structure for REPL output
#[derive(Debug)]
pub struct ResultSet {
    /// Column names
    pub columns: Vec<String>,
    /// Rows as strings
    pub rows: Vec<Vec<String>>,
}
