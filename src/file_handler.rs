//! File handling module for sqawk
//!
//! This module provides a unified interface for loading and saving different file formats:
//! - CSV files (comma-separated values)
//! - Delimiter-separated files (tab, colon, etc.)
//!
//! It abstracts away the specific file format details and provides a consistent API
//! for the rest of the application to work with in-memory tables.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::csv_handler::CsvHandler;
use crate::database::Database;
use crate::delim_handler::DelimHandler;
use crate::error::{SqawkError, SqawkResult};
use crate::table::Table;

/// Enum representing different file formats supported by sqawk
#[derive(Debug, Clone, Copy)]
pub enum FileFormat {
    /// CSV (comma-separated values)
    Csv,
    /// Delimiter-separated values
    Delimited,
}

/// Unified file handler that delegates to specific format handlers
pub struct FileHandler {
    /// Handler for CSV files
    csv_handler: CsvHandler,

    /// Handler for delimiter-separated files
    delim_handler: DelimHandler,

    /// Default format to use if not specified (underscore prefix indicates it's intentionally unused for now)
    _default_format: FileFormat,

    /// Custom field separator if specified
    field_separator: Option<String>,

    /// Custom column names for tables
    /// Map from table name to a vector of column names
    table_column_defs: HashMap<String, Vec<String>>,
    
    /// Optional reference to a database object
    /// When present, this database will be used as the source of truth
    /// During the transition, this will be None, and FileHandler will use its own tables
    database: Option<*mut Database>,
    
    /// Local tables storage for backward compatibility
    /// This will be deprecated once the transition to Database is complete
    tables: HashMap<String, Table>,
}

// Add safety implementation for the raw pointer to Database
unsafe impl Send for FileHandler {}
unsafe impl Sync for FileHandler {}

impl FileHandler {
    /// Create a new FileHandler with specified field separator and column definitions
    ///
    /// # Arguments
    /// * `field_separator` - Optional field separator character/string
    /// * `tabledef` - Optional vector of table column definitions in format "table_name:col1,col2,..."
    ///
    /// # Returns
    /// A new FileHandler instance ready to load and manage tables
    pub fn new(field_separator: Option<String>, tabledef: Option<Vec<String>>) -> Self {
        let default_format = if field_separator.is_some() {
            FileFormat::Delimited
        } else {
            FileFormat::Csv
        };

        // Process any table column definitions
        let mut table_column_defs = HashMap::new();
        if let Some(defs) = tabledef {
            for def in defs {
                if let Some((table_name, columns_str)) = def.split_once(':') {
                    let columns = columns_str
                        .split(',')
                        .map(|s| s.trim().to_string())
                        .collect::<Vec<String>>();

                    if !columns.is_empty() {
                        table_column_defs.insert(table_name.to_string(), columns);
                    }
                }
            }
        }

        FileHandler {
            tables: HashMap::new(),
            csv_handler: CsvHandler::new(),
            delim_handler: DelimHandler::new(field_separator.clone()),
            _default_format: default_format,
            field_separator,
            table_column_defs,
            database: None,
        }
    }
    
    /// Create a new FileHandler that uses the provided Database as the source of truth
    ///
    /// # Arguments
    /// * `field_separator` - Optional field separator character/string
    /// * `tabledef` - Optional vector of table column definitions in format "table_name:col1,col2,..."
    /// * `database` - Mutable reference to the database to use
    ///
    /// # Returns
    /// A new FileHandler instance that delegates table operations to the database
    pub fn new_with_database(
        field_separator: Option<String>, 
        tabledef: Option<Vec<String>>,
        database: &mut Database
    ) -> Self {
        let mut handler = Self::new(field_separator, tabledef);
        
        // Store a raw pointer to the database
        // SAFETY: The caller must ensure that the database outlives this FileHandler
        handler.database = Some(database as *mut Database);
        
        handler
    }
    
    /// Get a reference to the database if available
    ///
    /// # Returns
    /// * `Some(&mut Database)` if a database was provided
    /// * `None` if no database was provided
    fn database_mut(&mut self) -> Option<&mut Database> {
        if let Some(db_ptr) = self.database {
            // SAFETY: The caller of `new_with_database` ensures the database outlives this FileHandler
            unsafe { Some(&mut *db_ptr) }
        } else {
            None
        }
    }

    /// Load a file into an in-memory table with explicit return of table name and path
    ///
    /// # Arguments
    /// * `file_spec` - File specification in format [table_name=]file_path
    ///
    /// # Returns
    /// * `SqawkResult<Option<(String, String)>>` - Tuple of (table_name, file_path) if successful
    pub fn load_file(&mut self, file_spec: &str) -> SqawkResult<Option<(String, String)>> {
        // Parse file spec to get table name and file path
        let (table_name, file_path) = self.parse_file_spec(file_spec)?;
        let file_path_str = file_path.to_string_lossy().to_string();

        // Determine the file format based on extension
        let format = self.detect_format(&file_path);

        // Check if custom column names are defined for this table
        let custom_columns = self.table_column_defs.get(&table_name).cloned();

        match format {
            FileFormat::Csv => {
                let table = self.csv_handler.load_csv(file_spec, custom_columns, None)?;
                
                // If we have a database, add the table to it
                if let Some(db) = self.database_mut() {
                    db.add_table(table_name.clone(), table)?;
                } else {
                    // Otherwise, store it locally (backward compatibility)
                    self.tables.insert(table_name.clone(), table);
                }
            }
            FileFormat::Delimited => {
                let delimiter = self.field_separator.as_deref().unwrap_or("\t");
                let table =
                    self.delim_handler.load_delimited(file_spec, delimiter, custom_columns)?;
                
                // If we have a database, add the table to it
                if let Some(db) = self.database_mut() {
                    db.add_table(table_name.clone(), table)?;
                } else {
                    // Otherwise, store it locally (backward compatibility)
                    self.tables.insert(table_name.clone(), table);
                }
            }
        }

        Ok(Some((table_name, file_path_str)))
    }

    /// Parse a file specification into a table name and path
    ///
    /// # Arguments
    /// * `file_spec` - File specification in format [table_name=]file_path
    ///
    /// # Returns
    /// * `SqawkResult<(String, PathBuf)>` - Tuple of (table_name, file_path)
    pub fn parse_file_spec(&self, file_spec: &str) -> SqawkResult<(String, PathBuf)> {
        // Check for explicit table name in format "table_name=file_path"
        if let Some(pos) = file_spec.find('=') {
            let (table_name, file_path) = file_spec.split_at(pos);
            
            // Strip the '=' from the file path
            let file_path = &file_path[1..];
            
            // Validate that the file exists
            let path = PathBuf::from(file_path);
            if !path.exists() {
                return Err(SqawkError::FileNotFound(file_path.to_string()));
            }
            
            Ok((table_name.to_string(), path))
        } else {
            // No explicit table name, use the file name without extension
            let path = PathBuf::from(file_spec);
            
            // Validate that the file exists
            if !path.exists() {
                return Err(SqawkError::FileNotFound(file_spec.to_string()));
            }
            
            // Get file name without extension as table name
            let file_name = path.file_name()
                .ok_or_else(|| SqawkError::InvalidFileSpec(file_spec.to_string()))?
                .to_string_lossy();
            
            // Extract name without extension
            let table_name = if let Some(pos) = file_name.rfind('.') {
                file_name[..pos].to_string()
            } else {
                file_name.to_string()
            };
            
            Ok((table_name, path))
        }
    }

    /// Get a reference to a table by name
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to retrieve
    ///
    /// # Returns
    /// * `SqawkResult<&Table>` - Reference to the requested table
    pub fn get_table(&self, table_name: &str) -> SqawkResult<&Table> {
        // If we have a database, try to get the table from it
        if let Some(db_ptr) = self.database {
            // SAFETY: The caller of `new_with_database` ensures the database outlives this FileHandler
            let db = unsafe { &*db_ptr };
            db.get_table(table_name)
        } else {
            // Otherwise, use local tables (backward compatibility)
            match self.tables.get(table_name) {
                Some(table) => Ok(table),
                None => Err(SqawkError::TableNotFound(table_name.to_string())),
            }
        }
    }

    /// Get a mutable reference to a table by name
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to retrieve
    ///
    /// # Returns
    /// * `SqawkResult<&mut Table>` - Mutable reference to the requested table
    pub fn get_table_mut(&mut self, table_name: &str) -> SqawkResult<&mut Table> {
        // Check if we have a database reference
        if self.database.is_some() {
            // SAFETY: We've already checked that database is Some, and we know
            // the database reference outlives this FileHandler
            let db = unsafe { &mut *(self.database.unwrap()) };
            db.get_table_mut(table_name)
        } else {
            // Otherwise, use local tables (backward compatibility)
            match self.tables.get_mut(table_name) {
                Some(table) => Ok(table),
                None => Err(SqawkError::TableNotFound(table_name.to_string())),
            }
        }
    }

    /// Add a table to the collection
    ///
    /// # Arguments
    /// * `name` - Name of the table
    /// * `table` - Table to add
    ///
    /// # Returns
    /// * `SqawkResult<()>` - Result of the operation
    pub fn add_table(&mut self, name: String, table: Table) -> SqawkResult<()> {
        // If we have a database, add the table to it
        if let Some(db) = self.database_mut() {
            db.add_table(name, table)
        } else {
            // Otherwise, store it locally (backward compatibility)
            if self.tables.contains_key(&name) {
                return Err(SqawkError::TableAlreadyExists(name));
            }
            
            self.tables.insert(name, table);
            Ok(())
        }
    }

    /// Get all table names
    ///
    /// # Returns
    /// * `Vec<String>` - Vector of table names
    pub fn table_names(&self) -> Vec<String> {
        // If we have a database, get table names from it
        if let Some(db_ptr) = self.database {
            // SAFETY: The caller of `new_with_database` ensures the database outlives this FileHandler
            let db = unsafe { &*db_ptr };
            db.table_names()
        } else {
            // Otherwise, use local tables (backward compatibility)
            self.tables.keys().cloned().collect()
        }
    }

    /// Get the number of tables
    ///
    /// # Returns
    /// * `usize` - Number of tables
    pub fn table_count(&self) -> usize {
        // If we have a database, get table count from it
        if let Some(db_ptr) = self.database {
            // SAFETY: The caller of `new_with_database` ensures the database outlives this FileHandler
            let db = unsafe { &*db_ptr };
            db.table_count()
        } else {
            // Otherwise, use local tables (backward compatibility)
            self.tables.len()
        }
    }

    /// Check if a table exists
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to check
    ///
    /// # Returns
    /// * `bool` - True if the table exists
    pub fn has_table(&self, table_name: &str) -> bool {
        // If we have a database, check if the table exists in it
        if let Some(db_ptr) = self.database {
            // SAFETY: The caller of `new_with_database` ensures the database outlives this FileHandler
            let db = unsafe { &*db_ptr };
            db.has_table(table_name)
        } else {
            // Otherwise, check local tables (backward compatibility)
            self.tables.contains_key(table_name)
        }
    }

    /// Save a modified table back to its original file
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to save
    ///
    /// # Returns
    /// * `SqawkResult<()>` - Result of the operation
    pub fn save_table(&self, table_name: &str) -> SqawkResult<()> {
        // Get a reference to the table
        let table = self.get_table(table_name)?;

        // Check if the table has an associated file path
        let file_path = match table.file_path() {
            Some(path) => path,
            None => return Err(SqawkError::NoFilePath(table_name.to_string())),
        };

        // Determine the format based on the file extension
        let format = self.detect_format(file_path);

        // Get the delimiter from the table
        let delimiter = table.delimiter();

        // Save the table based on the format
        match format {
            FileFormat::Csv => {
                // Delegation to CSV handler (comma is the standard CSV delimiter)
                if delimiter == "," {
                    self.csv_handler.save_csv(table, file_path)?;
                } else {
                    // If delimiter is not a comma, use the delimited handler
                    self.delim_handler.save_delimited(table, file_path, delimiter)?;
                }
            }
            FileFormat::Delimited => {
                // Delegation to delimited handler
                self.delim_handler.save_delimited(table, file_path, delimiter)?;
            }
        }

        Ok(())
    }

    /// Detect file format based on extension
    ///
    /// # Arguments
    /// * `path` - File path
    ///
    /// # Returns
    /// * `FileFormat` - Detected format based on file extension
    fn detect_format(&self, path: &Path) -> FileFormat {
        if let Some(ext) = path.extension() {
            match ext.to_string_lossy().to_lowercase().as_str() {
                "csv" => FileFormat::Csv,
                _ => FileFormat::Delimited,
            }
        } else {
            // Default to CSV if no extension
            FileFormat::Csv
        }
    }
}