//! Common SQL Executor trait for sqawk
//!
//! This module defines a common interface for SQL executors in the sqawk application.
//! By implementing this trait, different execution engines (direct execution and VM-based)
//! can be used interchangeably by the rest of the application, particularly the REPL.

use crate::error::SqawkResult;
use crate::table::Table;

/// Common interface for SQL executors in sqawk
///
/// This trait defines the required methods that all SQL executors must implement,
/// allowing the application to switch between execution engines seamlessly.
pub trait SqlExecutorTrait {
    /// Execute a SQL statement and return the result
    ///
    /// # Arguments
    /// * `sql` - The SQL statement to execute
    ///
    /// # Returns
    /// * `SqawkResult<Option<Table>>` - Result of the operation, possibly containing a table
    fn execute(&mut self, sql: &str) -> SqawkResult<Option<Table>>;
    
    /// Get the number of rows affected by the last operation
    ///
    /// # Returns
    /// * `usize` - Number of affected rows
    fn get_affected_row_count(&self) -> usize;
    
    /// Get the names of all tables currently loaded
    ///
    /// # Returns
    /// * `Vec<String>` - List of table names
    fn get_table_names(&self) -> Vec<String>;
    
    /// Get column names for a specific table
    ///
    /// # Arguments
    /// * `table_name` - Name of the table
    ///
    /// # Returns
    /// * `SqawkResult<Vec<String>>` - List of column names
    fn get_table_columns(&self, table_name: &str) -> SqawkResult<Vec<String>>;
    
    /// Get column names and types for a specific table
    ///
    /// # Arguments
    /// * `table_name` - Name of the table
    ///
    /// # Returns
    /// * `SqawkResult<Vec<(String, String)>>` - List of column names and their types
    fn get_table_columns_with_types(&self, table_name: &str) -> SqawkResult<Vec<(String, String)>>;
    
    /// Set whether to write changes back to files
    ///
    /// # Arguments
    /// * `write_mode` - Whether to write changes
    fn set_write_mode(&mut self, write_mode: bool);
    
    /// Get whether write mode is enabled
    ///
    /// # Returns
    /// * `bool` - Whether write mode is enabled
    fn get_write_mode(&self) -> bool;
    
    /// Save all modified tables to their associated files
    ///
    /// # Returns
    /// * `SqawkResult<usize>` - Number of tables saved
    fn save_modified_tables(&mut self) -> SqawkResult<usize>;
}