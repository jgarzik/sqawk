//! SQL VM Executor module for sqawk
//!
//! This module implements a bytecode-based SQL execution engine inspired by SQLite's approach.
//! Unlike the current executor, this implementation compiles SQL statements to bytecode
//! instructions and then executes them in a virtual machine.
//!
//! Currently, this is a mock implementation that provides the foundation for the real
//! engine to be built incrementally. It implements the SqlExecutorTrait interface
//! to be compatible with the REPL and other components of the application.

use std::collections::HashMap;
use crate::database::Database;
use crate::error::{SqawkError, SqawkResult};
use crate::executor_trait::SqlExecutorTrait;
use crate::file_handler::FileHandler;
use crate::table::{Table, Value};

/// SQL VM executor that compiles SQL to bytecode and then executes it
pub struct SqlVmExecutor<'a> {
    /// Database containing tables
    database: &'a mut Database,
    /// File handler for loading and saving tables
    file_handler: &'a mut FileHandler<'a>,
    /// Whether to write changes back to files
    write_mode: bool,
    /// Whether the executor is in verbose mode
    verbose: bool,
    /// Number of rows affected by the last operation
    affected_row_count: usize,
    /// Cache of mock tables for testing
    mock_tables: HashMap<String, Table>,
}

impl<'a> SqlVmExecutor<'a> {
    /// Create a new SQL VM executor
    pub fn new(
        database: &'a mut Database,
        file_handler: &'a mut FileHandler<'a>,
        write_mode: bool,
        verbose: bool,
    ) -> Self {
        Self {
            database,
            file_handler,
            write_mode,
            verbose,
            affected_row_count: 0,
            mock_tables: HashMap::new(),
        }
    }
    
    /// Initialize the mock tables for testing
    fn init_mock_tables(&mut self) -> SqawkResult<()> {
        // Only initialize once
        if !self.mock_tables.is_empty() {
            return Ok(());
        }
        
        // Create a simple mock table with sample data
        let mut table = Table::new("mock_result", vec![], None);
        
        // Add column definitions
        table.add_column("column1".to_string(), "INT".to_string());
        table.add_column("column2".to_string(), "TEXT".to_string());
        table.add_column("column3".to_string(), "FLOAT".to_string());
        
        // Add some sample data
        table.add_row(vec![
            Value::Integer(1), 
            Value::String("VM Test".to_string()), 
            Value::Float(10.5)
        ])?;
        
        table.add_row(vec![
            Value::Integer(2), 
            Value::String("Bytecode Engine".to_string()), 
            Value::Float(20.75)
        ])?;
        
        // Store in our mock tables cache
        self.mock_tables.insert("mock_result".to_string(), table);
        
        Ok(())
    }
}

impl<'a> SqlExecutorTrait for SqlVmExecutor<'a> {
    fn execute(&mut self, sql: &str) -> SqawkResult<Option<Table>> {
        if self.verbose {
            println!("VM Executor: Mock implementation");
            println!("SQL statement: {}", sql);
        }
        
        // Initialize mock tables if needed
        self.init_mock_tables()?;
        
        // For SELECT statements, return a mock table with sample data
        if sql.trim().to_uppercase().starts_with("SELECT") {
            if let Some(table) = self.mock_tables.get("mock_result") {
                if self.verbose {
                    println!("VM Executor: Created mock result table with {} rows", table.row_count());
                }
                
                // Set affected row count for SELECT queries (number of rows returned)
                self.affected_row_count = table.row_count();
                
                // Clone the table to return
                Ok(Some(table.clone()))
            } else {
                Err(SqawkError::TableNotFound("mock_result".to_string()))
            }
        } else {
            // For non-SELECT statements, return success with no result
            if self.verbose {
                println!("VM Executor: Query executed (no results)");
            }
            
            // For non-SELECT statements, set affected row count to 1 (mock)
            self.affected_row_count = 1;
            
            Ok(None)
        }
    }
    
    fn get_affected_row_count(&self) -> usize {
        self.affected_row_count
    }
    
    fn get_table_names(&self) -> Vec<String> {
        let mut names = self.database.get_table_names();
        
        // Add mock tables in VM mode
        for name in self.mock_tables.keys() {
            if !names.contains(name) {
                names.push(name.clone());
            }
        }
        
        names
    }
    
    fn get_table_columns(&self, table_name: &str) -> SqawkResult<Vec<String>> {
        // Check mock tables first
        if let Some(table) = self.mock_tables.get(table_name) {
            return Ok(table.columns());
        }
        
        // Otherwise check database
        if let Some(table) = self.database.get_table(table_name) {
            Ok(table.columns())
        } else {
            Err(SqawkError::TableNotFound(table_name.to_string()))
        }
    }
    
    fn get_table_columns_with_types(&self, table_name: &str) -> SqawkResult<Vec<(String, String)>> {
        // Check mock tables first
        if let Some(table) = self.mock_tables.get(table_name) {
            let columns = table.columns();
            let mut result = Vec::with_capacity(columns.len());
            
            for (i, col_name) in columns.iter().enumerate() {
                // Get column type as string
                let type_str = if i == 0 {
                    "INTEGER".to_string()
                } else if i == 1 {
                    "TEXT".to_string()
                } else {
                    "REAL".to_string()
                };
                
                result.push((col_name.clone(), type_str));
            }
            
            return Ok(result);
        }
        
        // Otherwise check database
        if let Some(table) = self.database.get_table(table_name) {
            let meta = table.column_metadata();
            let result: Vec<(String, String)> = meta
                .iter()
                .map(|col| (col.name.clone(), col.data_type.to_string()))
                .collect();
            
            Ok(result)
        } else {
            Err(SqawkError::TableNotFound(table_name.to_string()))
        }
    }
    
    fn set_write_mode(&mut self, write_mode: bool) {
        self.write_mode = write_mode;
    }
    
    fn get_write_mode(&self) -> bool {
        self.write_mode
    }
    
    fn save_modified_tables(&mut self) -> SqawkResult<usize> {
        if self.verbose {
            println!("VM Executor: Mock save operation");
        }
        
        // In VM mode, delegate to the file handler for real tables
        if self.write_mode {
            self.file_handler.save_modified_tables()
        } else {
            Ok(0) // No tables saved when write mode is disabled
        }
    }
}