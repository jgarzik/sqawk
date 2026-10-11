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

use crate::config::AppConfig;
use crate::database::Database;
use crate::error::{SqawkError, SqawkResult};
use crate::file_handler::FileHandler;

/// SQL statement executor
///
/// This executor wraps the VM execution engine and provides additional functionality
/// for tracking modified tables and integrating with the REPL interface.
pub struct SqlExecutor<'a> {
    /// Database for storing and accessing tables
    database: &'a mut Database,

    /// File handler for loading and saving tables
    file_handler: &'a mut FileHandler,

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
            diag!("Executing SQL: {}", sql);
        }

        // Execute via VM engine (which also records the modified tables in
        // the database)
        let result = crate::vm::execute_vm(sql, self.database, self.config.verbose())?;

        // Track affected rows from the last statement
        self.affected_rows = result.affected_rows;
        self.dml_executed = result.dml_executed;

        // Result tables print with the output delimiter
        let mut tables = result.tables;
        let delim = self.config.output_delimiter();
        for t in &mut tables {
            t.set_delimiter(delim.clone());
        }

        Ok(tables)
    }

    /// Save all modified tables back to their source files
    ///
    /// Writes every table changed by INSERT, UPDATE, DELETE, ALTER TABLE,
    /// TRUNCATE or CREATE TABLE ... LOCATION back to its file. A scratch
    /// table (no file: CREATE TABLE without LOCATION, CREATE TABLE ... AS)
    /// is skipped.
    ///
    /// Every table is checked before any of them is written: a table read
    /// from standard input, a read-only file, or a directory that cannot
    /// take the temp file fails the whole writeback with nothing written.
    /// Writes happen in table-name order.
    ///
    /// # Returns
    /// * `Ok(usize)` - Number of tables saved
    /// * `Err` if any error occurs during saving
    pub fn save_modified_tables(&self) -> SqawkResult<usize> {
        let mut targets = Vec::new();
        for table_name in self.database.modified_tables() {
            if self.file_handler.check_table_writable(&table_name)? {
                targets.push(table_name);
            } else if self.config.verbose() {
                diag!(
                    "Table '{}' has no file (CREATE TABLE without LOCATION); not written",
                    table_name
                );
            }
        }

        for table_name in &targets {
            // Use the file handler to write the table back to its source file
            self.file_handler.save_table(table_name)?;
        }

        Ok(targets.len())
    }

    /// Whether a transaction (BEGIN without COMMIT / ROLLBACK yet) is open.
    pub fn in_transaction(&self) -> bool {
        self.database.in_transaction()
    }

    /// End of input: roll back a transaction left open, as SQLite discards
    /// uncommitted work when the connection closes. Called before the final
    /// writeback, so --write never saves uncommitted changes.
    pub fn finish(&mut self) {
        if self.database.in_transaction() {
            if self.config.verbose() {
                diag!("Transaction still open at exit: rolled back");
            }
            // Cannot fail: a transaction is open.
            let _ = self.database.rollback();
        }
    }

    /// Check if a specific table has been modified
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to check
    ///
    /// # Returns
    /// * `bool` - True if the table has been modified
    pub fn is_table_modified(&self, table_name: &str) -> bool {
        self.database.is_modified(table_name)
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
    /// * `SqawkResult<Vec<(String, String)>>` - each column's name and type
    ///   name (the declared name for a declared column)
    pub fn get_table_column_types(&self, table_name: &str) -> SqawkResult<Vec<(String, String)>> {
        let table = self.database.get_table(table_name)?;
        let column_types = table
            .column_metadata()
            .iter()
            .map(|col| (col.name.clone(), col.type_name()))
            .collect();
        Ok(column_types)
    }

    /// Check if any tables have been modified
    ///
    /// # Returns
    /// * `bool` - True if any tables have been modified
    pub fn has_modified_tables(&self) -> bool {
        !self.database.modified_tables().is_empty()
    }

    /// Load a file as a table
    ///
    /// # Arguments
    /// * `file_spec` - File specification in format [table_name=]file_path
    ///
    /// # Returns
    /// * `SqawkResult<Option<(String, String)>>` - Tuple of (table_name, file_path) if successful
    ///
    /// Refused inside a transaction: ROLLBACK restores the tables as they
    /// were at BEGIN, which would silently unload the file.
    pub fn load_file(&mut self, file_spec: &str) -> SqawkResult<Option<(String, String)>> {
        if self.database.in_transaction() {
            return Err(SqawkError::InvalidSqlQuery(
                "cannot load a file within a transaction".to_string(),
            ));
        }
        self.file_handler.load_file(file_spec)
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
        self.database.is_modified(table_name)
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

        if !self.database.is_modified(table_name) {
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
