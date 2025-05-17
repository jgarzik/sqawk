//! Database module for sqawk
//!
//! This module provides a central store for all tables in the application.
//! It maintains a collection of tables indexed by name and provides
//! methods for table access, manipulation, and querying.

use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;

use crate::error::{SqawkError, SqawkResult};
use crate::table::Table;

/// Central database that owns all tables
pub struct Database {
    /// In-memory tables indexed by their names
    tables: HashMap<String, Table>,
    
    /// Set of tables that have been modified since loading
    modified_tables: HashSet<String>,
}

impl Database {
    /// Create a new, empty database
    pub fn new() -> Self {
        Database {
            tables: HashMap::new(),
            modified_tables: HashSet::new(),
        }
    }
    
    /// Add a table to the database
    ///
    /// # Arguments
    /// * `name` - Name of the table
    /// * `table` - The table to add
    ///
    /// # Returns
    /// * `Ok(())` if the table was added successfully
    /// * `Err` if a table with that name already exists
    pub fn add_table(&mut self, name: String, mut table: Table) -> SqawkResult<()> {
        if self.tables.contains_key(&name) {
            return Err(SqawkError::TableAlreadyExists(name));
        }
        
        // For tables with a file path (like those created with CREATE TABLE + LOCATION),
        // make sure the file path is in a normalized form
        if let Some(file_path) = table.file_path() {
            // Create a normalized path that always uses system paths correctly
            let normalized_path = if file_path.is_relative() {
                match std::env::current_dir() {
                    Ok(current_dir) => current_dir.join(file_path),
                    Err(_) => file_path.to_path_buf(),
                }
            } else {
                file_path.to_path_buf()
            };
            
            // Set the normalized path back on the table
            table.set_file_path(Some(normalized_path));
        }
        
        self.tables.insert(name, table);
        Ok(())
    }
    
    /// Get a reference to a table by name
    ///
    /// # Arguments
    /// * `name` - Name of the table to get
    ///
    /// # Returns
    /// * `Ok(&Table)` if the table exists
    /// * `Err` if the table doesn't exist
    pub fn get_table(&self, name: &str) -> SqawkResult<&Table> {
        self.tables
            .get(name)
            .ok_or_else(|| SqawkError::TableNotFound(name.to_string()))
    }
    
    /// Get a mutable reference to a table by name
    ///
    /// # Arguments
    /// * `name` - Name of the table to get
    ///
    /// # Returns
    /// * `Ok(&mut Table)` if the table exists
    /// * `Err` if the table doesn't exist
    pub fn get_table_mut(&mut self, name: &str) -> SqawkResult<&mut Table> {
        self.tables
            .get_mut(name)
            .ok_or_else(|| SqawkError::TableNotFound(name.to_string()))
    }
    
    /// Check if a table exists
    ///
    /// # Arguments
    /// * `name` - Name of the table to check
    ///
    /// # Returns
    /// * `true` if the table exists
    /// * `false` if the table doesn't exist
    pub fn table_exists(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
    
    /// Get the names of all tables
    ///
    /// # Returns
    /// * `Vec<String>` - Names of all tables
    pub fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }
    
    /// Get column names for a specific table
    ///
    /// # Arguments
    /// * `table_name` - Name of the table
    ///
    /// # Returns
    /// * `SqawkResult<Vec<String>>` - List of column names
    pub fn get_table_columns(&self, table_name: &str) -> SqawkResult<Vec<String>> {
        let table = self.get_table(table_name)?;
        Ok(table.columns().clone())
    }
    
    /// Mark a table as modified
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to mark as modified
    pub fn mark_table_modified(&mut self, table_name: &str) {
        self.modified_tables.insert(table_name.to_string());
    }
    
    /// Check if a table has been modified
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to check
    ///
    /// # Returns
    /// * `bool` - True if the table has been modified
    pub fn is_table_modified(&self, table_name: &str) -> bool {
        self.modified_tables.contains(table_name)
    }
    
    /// Get the set of modified table names
    ///
    /// # Returns
    /// * `&HashSet<String>` - The set of modified table names
    pub fn modified_tables(&self) -> &HashSet<String> {
        &self.modified_tables
    }
    
    /// Check if any tables have been modified
    ///
    /// # Returns
    /// * `bool` - True if any tables have been modified
    pub fn has_modified_tables(&self) -> bool {
        !self.modified_tables.is_empty()
    }
    
    /// Get the number of tables
    ///
    /// # Returns
    /// * `usize` - Number of tables
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }
}