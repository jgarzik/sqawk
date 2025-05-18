//! SQL executor trait for sqawk
//!
//! This module defines a common interface for SQL executors in the sqawk application.
//! It allows different execution engines (direct and VM-based) to present the same
//! interface to other components, especially the REPL.

use crate::error::SqawkResult;
use crate::table::Table;

/// Trait defining the common interface for SQL executors
///
/// This trait allows different SQL execution engines to be used interchangeably
/// by other components of the application, particularly the REPL.
pub trait SqlExecutorTrait {
    /// Execute a SQL statement
    ///
    /// # Arguments
    /// * `sql` - The SQL statement to execute
    ///
    /// # Returns
    /// * `Ok(Some(Table))` for SELECT queries with results
    /// * `Ok(None)` for other statement types (INSERT, UPDATE, DELETE)
    /// * `Err` if the statement execution fails
    fn execute(&mut self, sql: &str) -> SqawkResult<Option<Table>>;
    
    /// Get the number of rows affected by the last operation
    ///
    /// # Returns
    /// * The number of rows affected
    fn get_affected_row_count(&self) -> usize;
    
    /// Get a list of all table names
    ///
    /// # Returns
    /// * A vector of table names
    fn get_table_names(&self) -> Vec<String>;
    
    /// Get column names for a table
    ///
    /// # Arguments
    /// * `table_name` - The name of the table
    ///
    /// # Returns
    /// * `Ok(Vec<String>)` with column names if the table exists
    /// * `Err` if the table doesn't exist
    fn get_table_columns(&self, table_name: &str) -> SqawkResult<Vec<String>>;
    
    /// Get column names and types for a table
    ///
    /// # Arguments
    /// * `table_name` - The name of the table
    ///
    /// # Returns
    /// * `Ok(Vec<(String, String)>)` with column names and types if the table exists
    /// * `Err` if the table doesn't exist
    fn get_table_columns_with_types(&self, table_name: &str) -> SqawkResult<Vec<(String, String)>>;
    
    /// Set whether to write changes back to files
    ///
    /// # Arguments
    /// * `write_mode` - Whether to write changes
    fn set_write_mode(&mut self, write_mode: bool);
    
    /// Get whether write mode is enabled
    ///
    /// # Returns
    /// * Whether write mode is enabled
    fn get_write_mode(&self) -> bool;
    
    /// Save modified tables to their files
    ///
    /// # Returns
    /// * `Ok(usize)` with the number of tables saved
    /// * `Err` if saving fails
    fn save_modified_tables(&mut self) -> SqawkResult<usize>;
}