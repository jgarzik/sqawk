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
            println!("Executing SQL: {}", sql);
        }

        // Execute via VM engine
        let result = crate::vm::execute_vm(sql, self.database, self.config.verbose())?;

        // Track modified tables
        for table_name in result.modified_tables {
            self.modified_tables.insert(table_name);
        }

        // Track affected rows from the last statement
        self.affected_rows = result.affected_rows;

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
    /// # Returns
    /// * `Ok(usize)` - Number of tables saved
    /// * `Err` if any error occurs during saving
    pub fn save_modified_tables(&self) -> Result<usize> {
        let mut count = 0;
        for table_name in &self.modified_tables {
            // Use the file handler to write the table back to its source file
            self.file_handler.save_table(table_name)?;
            count += 1;
        }

        Ok(count)
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
    /// * `Result<Option<ResultSet>>` - Optional ResultSet containing query results
    pub fn execute_sql(&mut self, sql: &str) -> Result<Option<ResultSet>> {
        let result = self.execute(sql)?;

        // The REPL shows one result set at a time; a multi-statement line
        // reports the last statement's.
        Ok(result.last().map(|table| ResultSet {
            columns: table.columns().to_vec(),
            rows: table.rows_as_strings(),
        }))
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
