//! Database module for sqawk
//!
//! This module provides the central database structure for managing tables in sqawk.
//! It serves as the single source of truth for all table operations and maintains
//! the state of tables throughout the application.
//!
//! The Database struct is responsible for:
//! - Storing all tables with their names
//! - Tracking which tables have been modified
//! - Providing a unified interface for table operations

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use crate::error::{SqawkError, SqawkResult};
use crate::table::{ColumnDefinition, Table};

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
    
    /// Mark a table as modified
    ///
    /// # Arguments
    /// * `name` - The name of the table that was modified
    ///
    /// # Returns
    /// * `Ok(())` if the table was successfully marked as modified
    /// * `Err` if the table doesn't exist
    pub fn mark_table_modified(&mut self, name: &str) -> SqawkResult<()> {
        if !self.tables.contains_key(name) {
            return Err(SqawkError::TableNotFound(name.to_string()));
        }
        
        // Add to modified set and also update the table's internal modified flag
        self.modified_tables.insert(name.to_string());
        
        // Update the table's internal modified flag
        let table = self.tables.get_mut(name).unwrap(); // Safe due to check above
        table.set_modified(true);
        
        Ok(())
    }
    
    /// Check if a table has been modified
    ///
    /// # Arguments
    /// * `name` - The name of the table to check
    ///
    /// # Returns
    /// * `true` if the table has been modified
    /// * `false` if the table hasn't been modified or doesn't exist
    pub fn is_table_modified(&self, name: &str) -> bool {
        self.modified_tables.contains(name)
    }
    
    /// Get the names of all tables in the database
    ///
    /// # Returns
    /// * Vector of table names
    pub fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }
    
    /// Get the names of all modified tables
    ///
    /// # Returns
    /// * Vector of modified table names
    pub fn modified_table_names(&self) -> Vec<String> {
        self.modified_tables.iter().cloned().collect()
    }
    
    /// Check if the database has any modified tables
    ///
    /// # Returns
    /// * `true` if any tables have been modified
    /// * `false` if no tables have been modified
    pub fn has_modified_tables(&self) -> bool {
        !self.modified_tables.is_empty()
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
    
    /// Create a new table with the given name and columns
    ///
    /// # Arguments
    /// * `name` - The name of the new table
    /// * `columns` - The columns of the new table
    /// * `file_path` - Optional file path for the table
    /// * `delimiter` - Optional delimiter for the table (defaults to comma)
    ///
    /// # Returns
    /// * `Ok(())` if the table was successfully created
    /// * `Err` if a table with the same name already exists
    pub fn create_table(
        &mut self,
        name: &str,
        columns: Vec<String>,
        file_path: Option<PathBuf>,
        delimiter: Option<String>,
    ) -> SqawkResult<()> {
        if self.tables.contains_key(name) {
            return Err(SqawkError::TableAlreadyExists(name.to_string()));
        }
        
        let table = if let Some(delim) = delimiter {
            Table::new_with_delimiter(name, columns, file_path, delim)
        } else {
            Table::new(name, columns, file_path)
        };
        
        self.tables.insert(name.to_string(), table);
        self.modified_tables.insert(name.to_string());
        
        Ok(())
    }
    
    /// Create a new table with a schema
    ///
    /// # Arguments
    /// * `name` - The name of the new table
    /// * `schema` - The schema of the new table
    /// * `file_path` - Optional file path for the table
    /// * `delimiter` - Optional delimiter for the table (defaults to comma)
    /// * `file_format` - Optional file format for the table (defaults to TEXTFILE)
    ///
    /// # Returns
    /// * `Ok(())` if the table was successfully created
    /// * `Err` if a table with the same name already exists
    pub fn create_table_with_schema(
        &mut self,
        name: &str,
        schema: Vec<ColumnDefinition>,
        file_path: Option<PathBuf>,
        delimiter: Option<String>,
        file_format: Option<String>,
    ) -> SqawkResult<()> {
        if self.tables.contains_key(name) {
            return Err(SqawkError::TableAlreadyExists(name.to_string()));
        }
        
        let table = Table::new_with_schema(
            name,
            schema,
            file_path,
            delimiter,
            file_format,
            false, // Default to non-verbose mode
        );
        
        self.tables.insert(name.to_string(), table);
        self.modified_tables.insert(name.to_string());
        
        Ok(())
    }
    
    /// Drop a table from the database
    ///
    /// # Arguments
    /// * `name` - The name of the table to drop
    ///
    /// # Returns
    /// * `Ok(())` if the table was successfully dropped
    /// * `Err` if the table doesn't exist
    pub fn drop_table(&mut self, name: &str) -> SqawkResult<()> {
        if !self.tables.contains_key(name) {
            return Err(SqawkError::TableNotFound(name.to_string()));
        }
        
        self.tables.remove(name);
        self.modified_tables.remove(name);
        
        Ok(())
    }
    
    /// Clear all modification flags
    ///
    /// This method clears the set of modified tables. It's typically called
    /// after saving all modified tables to disk.
    pub fn clear_modifications(&mut self) {
        self.modified_tables.clear();
        
        // Also clear the modified flag in each table
        for table in self.tables.values_mut() {
            table.set_modified(false);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_create_and_get_table() {
        let mut db = Database::new();
        
        // Create a table
        let cols = vec!["id".to_string(), "name".to_string()];
        db.create_table("test", cols, None, None).unwrap();
        
        // Get the table
        let table = db.get_table("test").unwrap();
        assert_eq!(table.name(), "test");
        assert_eq!(table.columns(), &["id", "name"]);
    }
    
    #[test]
    fn test_mark_table_modified() {
        let mut db = Database::new();
        
        // Create a table
        let cols = vec!["id".to_string(), "name".to_string()];
        db.create_table("test", cols, None, None).unwrap();
        
        // Table should be marked as modified when created
        assert!(db.is_table_modified("test"));
        
        // Clear modifications
        db.clear_modifications();
        assert!(!db.is_table_modified("test"));
        
        // Mark table as modified
        db.mark_table_modified("test").unwrap();
        assert!(db.is_table_modified("test"));
    }
    
    #[test]
    fn test_table_operations() {
        let mut db = Database::new();
        
        // Create some tables
        db.create_table("table1", vec!["col1".to_string()], None, None).unwrap();
        db.create_table("table2", vec!["col2".to_string()], None, None).unwrap();
        
        // Check table count
        assert_eq!(db.table_count(), 2);
        
        // Check table names
        let names = db.table_names();
        assert!(names.contains(&"table1".to_string()));
        assert!(names.contains(&"table2".to_string()));
        
        // Drop a table
        db.drop_table("table1").unwrap();
        assert_eq!(db.table_count(), 1);
        assert!(!db.has_table("table1"));
        assert!(db.has_table("table2"));
    }
    
    #[test]
    fn test_create_table_with_schema() {
        let mut db = Database::new();
        
        // Create a schema
        let schema = vec![
            ColumnDefinition {
                name: "id".to_string(),
                data_type: crate::table::DataType::Integer,
            },
            ColumnDefinition {
                name: "name".to_string(),
                data_type: crate::table::DataType::Text,
            },
        ];
        
        // Create a table with the schema
        db.create_table_with_schema(
            "test_schema",
            schema,
            None,
            Some("|".to_string()),
            Some("TEXTFILE".to_string()),
        ).unwrap();
        
        // Get the table
        let table = db.get_table("test_schema").unwrap();
        assert_eq!(table.name(), "test_schema");
        assert_eq!(table.columns(), &["id", "name"]);
        assert_eq!(table.delimiter(), "|");
    }
}