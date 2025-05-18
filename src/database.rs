//! Database module for sqawk
//!
//! This module provides the central database structure for managing tables in sqawk.
//! It serves as the single source of truth for all table operations and maintains
//! the state of tables throughout the application.
//!
//! The Database struct is responsible for:
//! - Storing all tables with their names
//! - Providing a unified interface for table operations

use std::collections::HashMap;
use crate::error::{SqawkError, SqawkResult};
use crate::table::Table;

/// Represents the central database that holds all tables
pub struct Database {
    /// In-memory tables indexed by their names
    tables: HashMap<String, Table>,
}

impl Database {
    /// Create a new, empty database
    pub fn new() -> Self {
        Database {
            tables: HashMap::new(),
        }
    }
    
    /// Add a table to the database
    ///
    /// # Arguments
    /// * `name` - The name of the table
    /// * `table` - The table to add
    ///
    /// # Returns
    /// * `Ok(())` if the table was successfully added
    /// * `Err` if a table with the same name already exists
    pub fn add_table(&mut self, name: String, table: Table) -> SqawkResult<()> {
        if self.tables.contains_key(&name) {
            return Err(SqawkError::TableAlreadyExists(name));
        }
        
        self.tables.insert(name, table);
        Ok(())
    }
    
    /// Get a reference to a table by name
    ///
    /// # Arguments
    /// * `name` - The name of the table to get
    ///
    /// # Returns
    /// * `Ok(&Table)` reference to the requested table
    /// * `Err` if the table doesn't exist
    pub fn get_table(&self, name: &str) -> SqawkResult<&Table> {
        self.tables.get(name).ok_or_else(|| SqawkError::TableNotFound(name.to_string()))
    }
    
    /// Get a mutable reference to a table by name
    ///
    /// # Arguments
    /// * `name` - The name of the table to get
    ///
    /// # Returns
    /// * `Ok(&mut Table)` mutable reference to the requested table
    /// * `Err` if the table doesn't exist
    pub fn get_table_mut(&mut self, name: &str) -> SqawkResult<&mut Table> {
        self.tables.get_mut(name).ok_or_else(|| SqawkError::TableNotFound(name.to_string()))
    }
    
    /// Get the names of all tables in the database
    ///
    /// # Returns
    /// * Vector of table names
    pub fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }
    
    /// Get the number of tables in the database
    ///
    /// # Returns
    /// * The number of tables
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }
    
    /// Check if a table with the given name exists
    ///
    /// # Arguments
    /// * `name` - The name of the table to check
    ///
    /// # Returns
    /// * `true` if the table exists
    /// * `false` if the table doesn't exist
    pub fn has_table(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_add_and_get_table() {
        let mut db = Database::new();
        
        // Create a table directly
        let table = Table::new("test", vec!["id".to_string(), "name".to_string()], None);
        db.add_table("test".to_string(), table).unwrap();
        
        // Get the table
        let table = db.get_table("test").unwrap();
        assert_eq!(table.name(), "test");
        assert_eq!(table.columns(), &["id", "name"]);
    }
    
    #[test]
    fn test_table_operations() {
        let mut db = Database::new();
        
        // Create some tables directly
        let table1 = Table::new("table1", vec!["col1".to_string()], None);
        let table2 = Table::new("table2", vec!["col2".to_string()], None);
        db.add_table("table1".to_string(), table1).unwrap();
        db.add_table("table2".to_string(), table2).unwrap();
        
        // Check table count
        assert_eq!(db.table_count(), 2);
        
        // Check table names
        let names = db.table_names();
        assert!(names.contains(&"table1".to_string()));
        assert!(names.contains(&"table2".to_string()));
        
        // Check if table exists
        assert!(db.has_table("table1"));
        assert!(db.has_table("table2"));
        
        // Remove a table manually
        db.tables.remove("table1");
        assert_eq!(db.table_count(), 1);
        assert!(!db.has_table("table1"));
        assert!(db.has_table("table2"));
    }
}