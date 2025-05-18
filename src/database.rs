//! Database module for sqawk
//!
//! This module provides the central database structure for managing tables in sqawk.
//! It serves as the single source of truth for all table operations and maintains
//! the state of tables throughout the application.
//!
//! The Database struct is responsible for:
//! - Storing all tables with their names
//! - Providing a unified interface for table operations

use crate::config::AppConfig;
use crate::error::{SqawkError, SqawkResult};
use crate::table::Table;
use std::collections::HashMap;

/// Represents the central database that holds all tables
pub struct Database {
    /// In-memory tables indexed by their names
    tables: HashMap<String, Table>,
}

/// Methods for direct table manipulation - used only in special cases
impl Database {
    /// Remove a table from the database by name
    ///
    /// This is a specialized method intended for use when replacing tables
    /// that were loaded with different schemas.
    ///
    /// # Arguments
    /// * `name` - The name of the table to remove
    ///
    /// # Returns
    /// * `true` if the table was found and removed, `false` otherwise
    pub fn remove_table(&mut self, name: &str) -> bool {
        self.tables.remove(name).is_some()
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
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
        self.tables
            .get(name)
            .ok_or_else(|| SqawkError::TableNotFound(name.to_string()))
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
        self.tables
            .get_mut(name)
            .ok_or_else(|| SqawkError::TableNotFound(name.to_string()))
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

    /// Compile table definitions from the application configuration
    ///
    /// This method processes the table definitions provided through command-line
    /// arguments and creates corresponding table schemas in the database.
    /// It serves as the central mechanism for converting raw tabledef strings
    /// into properly structured table objects.
    ///
    /// # Arguments
    /// * `config` - The application configuration containing table definitions
    ///
    /// # Returns
    /// * `Ok(())` if all table definitions were compiled successfully
    /// * `Err` if there was an issue with any table definition
    pub fn compile_table_definitions(&mut self, config: &AppConfig) -> SqawkResult<()> {
        // Process each table definition string (format: "table_name:col1,col2,...")
        for tabledef in config.table_definitions() {
            if let Some((table_name, columns_str)) = tabledef.split_once(':') {
                let columns = columns_str
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .collect::<Vec<String>>();

                if !columns.is_empty() {
                    // Create a new table with the specified columns
                    // Note: No file_path is assigned here as this is a table definition only
                    let columns_len = columns.len(); // Store the length before we move columns
                    let table = Table::new(table_name, columns, None);

                    // Add the table to the database, overwriting any existing definition with the same name
                    // This ensures CLI definitions take precedence
                    if self.has_table(table_name) {
                        // Remove the existing table first to avoid conflict errors
                        // Use direct replacement via insert/remove to avoid errors with private fields
                        self.tables.remove(table_name);
                    }

                    self.add_table(table_name.to_string(), table)?;

                    if config.verbose() {
                        println!(
                            "Compiled table definition for '{}' with {} columns",
                            table_name, columns_len
                        );
                    }
                }
            } else if config.verbose() {
                eprintln!("Invalid table definition format: {}", tabledef);
                eprintln!("Expected format: table_name:col1,col2,...");
            }
        }

        Ok(())
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
