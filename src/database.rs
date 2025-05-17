//! Database module for sqawk
//!
//! This module provides a central database implementation that owns and manages all tables.
//! It serves as the primary interface for table operations in the system.

use std::collections::HashMap;

use crate::error::{SqawkError, SqawkResult};
use crate::table::Table;

/// Central Database class that owns all tables in the system
pub struct Database {
    /// Collection of tables by name
    tables: HashMap<String, Table>,
}

impl Database {
    /// Create a new empty database
    pub fn new() -> Self {
        Database {
            tables: HashMap::new(),
        }
    }

    /// Add a table to the database
    ///
    /// # Arguments
    /// * `name` - Name of the table
    /// * `table` - Table to add
    ///
    /// # Returns
    /// * `Ok(())` if the table was successfully added
    /// * `Err` if a table with that name already exists
    pub fn add_table(&mut self, name: String, table: Table) -> SqawkResult<()> {
        if self.tables.contains_key(&name) {
            return Err(SqawkError::TableAlreadyExists(name));
        }
        self.tables.insert(name, table);
        Ok(())
    }

    /// Get a reference to a table
    ///
    /// # Arguments
    /// * `name` - Name of the table to get
    ///
    /// # Returns
    /// * `Ok(&Table)` if the table exists
    /// * `Err` if the table doesn't exist
    pub fn get_table(&self, name: &str) -> SqawkResult<&Table> {
        self.tables.get(name).ok_or_else(|| SqawkError::TableNotFound(name.to_string()))
    }

    /// Get a mutable reference to a table
    ///
    /// # Arguments
    /// * `name` - Name of the table to get
    ///
    /// # Returns
    /// * `Ok(&mut Table)` if the table exists
    /// * `Err` if the table doesn't exist
    pub fn get_table_mut(&mut self, name: &str) -> SqawkResult<&mut Table> {
        self.tables.get_mut(name).ok_or_else(|| SqawkError::TableNotFound(name.to_string()))
    }

    /// Check if a table exists
    ///
    /// # Arguments
    /// * `name` - Name of the table to check
    ///
    /// # Returns
    /// * `true` if the table exists, `false` otherwise
    pub fn has_table(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    /// Remove a table from the database
    ///
    /// # Arguments
    /// * `name` - Name of the table to remove
    ///
    /// # Returns
    /// * `Ok(())` if the table was successfully removed
    /// * `Err` if the table doesn't exist
    pub fn remove_table(&mut self, name: &str) -> SqawkResult<()> {
        if !self.tables.contains_key(name) {
            return Err(SqawkError::TableNotFound(name.to_string()));
        }
        self.tables.remove(name);
        Ok(())
    }

    /// Get names of all tables in the database
    ///
    /// # Returns
    /// * Vector of table names
    pub fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    /// Get number of tables in the database
    ///
    /// # Returns
    /// * Number of tables
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}