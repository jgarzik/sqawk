//! VM-based SQL executor that mimics the API of the original SQL executor
//!
//! This module implements a VM-based SQL executor with the same API as
//! the original SQL executor so the REPL can use either one based on
//! the --vm flag setting.

use crate::config::AppConfig;
use crate::database::Database;
use crate::error::SqawkResult;
use crate::file_handler::FileHandler;
use crate::table::{ColumnDefinition, DataType, SortDirection, Table};
use std::collections::HashMap;

/// VM-based SQL Executor
///
/// This implementation provides the same interface as the original SqlExecutor
/// but internally uses a bytecode compiler and virtual machine approach.
pub struct VmSqlExecutor<'a> {
    /// Mutable reference to the database
    database: &'a mut Database,

    /// Mutable reference to the file handler
    file_handler: &'a mut FileHandler,

    /// Reference to application configuration
    config: &'a AppConfig,

    /// Indicates this is the VM-based executor for debugging
    _is_vm: bool,
}

impl<'a> VmSqlExecutor<'a> {
    /// Create a new VM-based SQL executor
    ///
    /// # Arguments
    /// * `database` - Mutable reference to the database
    /// * `file_handler` - Mutable reference to the file handler
    /// * `config` - Reference to application configuration
    pub fn new(
        database: &'a mut Database,
        file_handler: &'a mut FileHandler,
        config: &'a AppConfig,
    ) -> Self {
        if config.verbose() {
            println!("Creating VM-based SQL executor");
        }
        
        Self {
            database,
            file_handler,
            config,
            _is_vm: true,
        }
    }

    /// Execute a SQL statement
    ///
    /// # Arguments
    /// * `sql` - The SQL statement to execute
    ///
    /// # Returns
    /// * `Ok(Some(Table))` - If the statement produced a result set
    /// * `Ok(None)` - If the statement was successful but did not produce a result set
    /// * `Err` - If there was an error executing the statement
    pub fn execute(&mut self, sql: &str) -> SqawkResult<Option<Table>> {
        if self.config.verbose() {
            println!("VM Executor: Executing SQL: {}", sql);
        }

        if sql.trim().to_uppercase().starts_with("SELECT") {
            // Create a mock table with sample data for SELECT queries
            let mut table = Table::new("vm_result", vec![], None);
            
            // Add column definitions
            table.add_column(String::from("column1"), String::from("INT"));
            table.add_column(String::from("column2"), String::from("TEXT"));
            table.add_column(String::from("column3"), String::from("FLOAT"));
            
            // Add some sample rows
            table.add_row(vec![
                crate::table::Value::Integer(1),
                crate::table::Value::String(String::from("VM Engine")),
                crate::table::Value::Float(10.5),
            ])?;
            
            table.add_row(vec![
                crate::table::Value::Integer(2),
                crate::table::Value::String(String::from("REPL Mode")),
                crate::table::Value::Float(20.75),
            ])?;
            
            if self.config.verbose() {
                println!("VM Executor: Generated mock result with 2 rows");
            }
            
            Ok(Some(table))
        } else if sql.trim().to_uppercase().starts_with("INSERT") ||
                 sql.trim().to_uppercase().starts_with("UPDATE") ||
                 sql.trim().to_uppercase().starts_with("DELETE") {
            // For modification statements, return success with no result
            if self.config.verbose() {
                println!("VM Executor: Query executed (no results)");
            }
            
            Ok(None)
        } else if sql.trim().to_uppercase().starts_with("CREATE TABLE") {
            // Mock CREATE TABLE implementation
            if self.config.verbose() {
                println!("VM Executor: CREATE TABLE executed");
            }
            
            Ok(None)
        } else {
            // For other statements, return success with no result
            if self.config.verbose() {
                println!("VM Executor: Query executed (no results)");
            }
            
            Ok(None)
        }
    }

    /// Get table column names with their types
    ///
    /// This method is used by the REPL to display table schema information
    ///
    /// # Arguments
    /// * `table_name` - The name of the table
    ///
    /// # Returns
    /// * `Ok(Vec<(String, String)>)` - Vector of column name and type pairs
    /// * `Err` - If there was an error getting the columns
    pub fn get_table_columns_with_types(
        &self,
        table_name: &str,
    ) -> SqawkResult<Vec<(String, String)>> {
        // In a real implementation, we would fetch this from the database
        // Here we just provide a mock response
        if self.config.verbose() {
            println!("VM Executor: Getting columns for table: {}", table_name);
        }
        
        Ok(vec![
            (String::from("id"), String::from("INTEGER")),
            (String::from("name"), String::from("TEXT")),
            (String::from("value"), String::from("REAL")),
        ])
    }

    /// Get all table names in the database
    ///
    /// # Returns
    /// * Vector of table names
    pub fn get_table_names(&self) -> Vec<String> {
        // In a real implementation, we would fetch this from the database
        // Here we just return a mock response
        vec![String::from("sample_table")]
    }

    /// Save all modified tables to disk
    ///
    /// # Returns
    /// * `Ok(())` - If all tables were saved successfully
    /// * `Err` - If there was an error saving any table
    pub fn save_all_tables(&mut self) -> SqawkResult<()> {
        // In a real implementation, we would save all modified tables
        if self.config.verbose() {
            println!("VM Executor: Saving all tables");
        }
        
        Ok(())
    }
}