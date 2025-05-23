//! File handling module for sqawk
//!
//! This module provides a unified interface for loading and saving different file formats:
//! - CSV files (comma-separated values)
//! - Delimiter-separated files (tab, colon, etc.)
//!
//! It abstracts away the specific file format details and provides a consistent API
//! for the rest of the application to work with in-memory tables.

use std::path::{Path, PathBuf};

use crate::config::AppConfig;
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
///
/// This struct provides a consistent interface for working with different file formats
/// by delegating to specialized handlers (CSV or delimiter-separated). It acts as a facade
/// that simplifies file operations for the rest of the application.
///
/// Key responsibilities:
/// - Determining the appropriate handler based on file format
/// - Managing the relationship between tables and their source files
/// - Providing access to the database for table operations
/// - Handling table loading and saving with proper format detection
pub struct FileHandler {
    /// Handler for CSV files (comma-separated values)
    csv_handler: CsvHandler,

    /// Handler for delimiter-separated files (tab, pipe, etc.)
    delim_handler: DelimHandler,

    /// Reference to a database object which is the source of truth for tables
    /// Stored as a raw pointer to avoid borrowing limitations
    database: *mut Database,

    /// Application configuration for global settings like verbosity and field separators
    config: AppConfig,
}

// Add safety implementation for the raw pointer to Database
unsafe impl Send for FileHandler {}
unsafe impl Sync for FileHandler {}

impl FileHandler {
    /// Create a new FileHandler with application config and database
    ///
    /// # Arguments
    /// * `config` - Application configuration with global settings
    /// * `database` - Mutable reference to the database to use as source of truth
    ///
    /// # Returns
    /// A new FileHandler instance ready to load and manage tables
    pub fn new(config: &AppConfig, database: &mut Database) -> Self {
        FileHandler {
            csv_handler: CsvHandler::new(),
            delim_handler: DelimHandler::new(),
            // SAFETY: The caller must ensure that the database outlives this FileHandler
            database: database as *mut Database,
            config: config.clone(),
        }
    }

    /// Get a mutable reference to the database
    ///
    /// Provides safe access to the database reference stored as a raw pointer.
    /// This design allows the FileHandler to maintain a reference to the Database
    /// while avoiding Rust's borrowing conflicts in complex operations.
    ///
    /// # Safety
    /// Safety is guaranteed because:
    /// - The database pointer is initialized in the constructor and never changes
    /// - The FileHandler's lifetime is tied to the Database through the constructor's contract
    /// - All access to the database happens through this controlled interface
    ///
    /// # Returns
    /// * `&mut Database` - Mutable reference to the database
    fn database_mut(&mut self) -> &mut Database {
        // SAFETY: The caller of `new` ensures the database outlives this FileHandler,
        // and we have exclusive access through `&mut self`
        unsafe { &mut *self.database }
    }

    /// Load a file into an in-memory table and add it to the database
    ///
    /// This method handles the complete file loading process:
    /// 1. Parses the file specification to extract table name and file path
    /// 2. Checks if the table already exists in the database
    /// 3. Automatically detects file format (CSV or delimiter-separated)
    /// 4. Delegates to the appropriate handler based on format
    /// 5. Adds the loaded table to the database
    ///
    /// If a table with the same name already exists in the database, it will be
    /// replaced with the newly loaded table.
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

        // First, check if the table already exists in the database
        // This could happen if it was defined through CLI table definitions
        let existing_schema;
        {
            // Create a temporary scope for the database borrow
            let db = self.database_mut();
            existing_schema = db.has_table(&table_name);
        }

        // Show verbose output if needed
        let verbose = self.config.verbose();
        if existing_schema && verbose {
            println!(
                "Table '{}' already exists in database, loading data into existing schema",
                table_name
            );
        }

        // Determine the file format based on extension
        let format = self.detect_format(&file_path);

        // Column definitions now come exclusively from Database
        // No need for custom columns logic here anymore

        // Create the table based on the format
        let table = match format {
            FileFormat::Csv => {
                // Load the table from the CSV file, no custom columns since we use Database schemas
                self.csv_handler.load_csv(file_spec, None, None)?
            }
            FileFormat::Delimited => {
                let delimiter = self
                    .config
                    .field_separator()
                    .unwrap_or_else(|| "\t".to_string());
                // No custom columns since we use Database schemas
                self.delim_handler
                    .load_delimited(file_spec, &delimiter, None)?
            }
        };

        // Now that we have the table, we can update the database without borrowing conflicts
        {
            // Create a new scope for database operations
            let db = self.database_mut();

            // Handle existing schema if needed
            if existing_schema {
                // For now, we'll replace it - in the future we might want to handle this
                // more gracefully with schema validation and merging
                if verbose {
                    println!("Replacing existing table '{}' with loaded data", table_name);
                }
                db.remove_table(&table_name);
            }

            // Add the table to the database
            db.add_table(table_name.clone(), table)?;
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
            let file_name = path
                .file_name()
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
        // SAFETY: The caller of `new` ensures the database outlives this FileHandler
        let db = unsafe { &*self.database };
        db.get_table(table_name)
    }

    /// Get a mutable reference to a table by name
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to retrieve
    ///
    /// # Returns
    /// * `SqawkResult<&mut Table>` - Mutable reference to the requested table
    pub fn get_table_mut(&mut self, table_name: &str) -> SqawkResult<&mut Table> {
        // SAFETY: The caller of `new` ensures the database outlives this FileHandler
        let db = unsafe { &mut *self.database };
        db.get_table_mut(table_name)
    }

    /// Add a table to the collection
    ///
    /// # Arguments
    /// * `name` - Name of the table
    /// * `table` - Table to add
    ///
    /// # Returns
    /// * `SqawkResult<()>` - Result of the operation
    pub fn add_table(&mut self, name: String, mut table: Table) -> SqawkResult<()> {
        // Check if the table has a file path before adding and log information
        if let Some(path) = table.file_path() {
            if self.config.verbose() {
                println!("Adding table '{}' with file path: {:?}", name, path);
            }

            // Make absolute path if necessary (needed for CREATE TABLE with relative paths)
            if !path.is_absolute() {
                // Get current directory
                if let Ok(mut cur_dir) = std::env::current_dir() {
                    // Join with the relative path
                    cur_dir.push(path.clone());
                    if self.config.verbose() {
                        println!("Converting to absolute path: {:?}", cur_dir);
                    }
                    // Update the file path in the table
                    table.set_file_path(cur_dir);
                }
            }
        } else if self.config.verbose() {
            println!("Adding table '{}' with NO file path", name);
        }

        // Add the table to the database
        self.database_mut().add_table(name, table)
    }

    /// Get all table names
    ///
    /// # Returns
    /// * `Vec<String>` - Vector of table names
    pub fn table_names(&self) -> Vec<String> {
        // SAFETY: The caller of `new` ensures the database outlives this FileHandler
        let db = unsafe { &*self.database };
        db.table_names()
    }

    /// Get the number of tables
    ///
    /// # Returns
    /// * `usize` - Number of tables
    pub fn table_count(&self) -> usize {
        // SAFETY: The caller of `new` ensures the database outlives this FileHandler
        let db = unsafe { &*self.database };
        db.table_count()
    }

    /// Check if a table exists
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to check
    ///
    /// # Returns
    /// * `bool` - True if the table exists
    pub fn has_table(&self, table_name: &str) -> bool {
        // SAFETY: The caller of `new` ensures the database outlives this FileHandler
        let db = unsafe { &*self.database };
        db.has_table(table_name)
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

        if self.config.verbose() {
            eprintln!("In FileHandler::save_table for table '{}'", table_name);

            // Access database directly to check if the table exists there
            let db = unsafe { &*self.database };
            if let Ok(db_table) = db.get_table(table_name) {
                if let Some(path) = db_table.file_path() {
                    eprintln!(
                        "Database has table '{}' with file_path '{:?}'",
                        table_name, path
                    );
                } else {
                    eprintln!("Database has table '{}' but NO file_path", table_name);
                }
            } else {
                eprintln!("Table '{}' not found in database", table_name);
            }
        }

        // Check if the table has an associated file path
        let file_path = match table.file_path() {
            Some(path) => {
                if self.config.verbose() {
                    eprintln!("Table '{}' has file_path '{:?}'", table_name, path);
                }
                path
            }
            None => {
                // Log debugging information
                if self.config.verbose() {
                    eprintln!("Table '{}' has NO file_path", table_name);
                    eprintln!(
                        "  Table details - Name: {}, Columns: {}, Delimiter: '{}'",
                        table.name(),
                        table.columns().join(","),
                        table.delimiter()
                    );
                }

                // For tables created with CREATE TABLE, the file path should be set
                return Err(SqawkError::NoFilePath(table_name.to_string()));
            }
        };

        // For tables created with CREATE TABLE, the file may not exist yet
        // Make sure parent directories exist
        if let Some(parent) = file_path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent).map_err(SqawkError::IoError)?;
            }
        }

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
                    self.delim_handler
                        .save_delimited(table, file_path, delimiter)?;
                }
            }
            FileFormat::Delimited => {
                // Delegation to delimited handler
                self.delim_handler
                    .save_delimited(table, file_path, delimiter)?;
            }
        }

        Ok(())
    }

    /// Detect file format based on file extension
    ///
    /// This method examines the file extension to determine the appropriate handler:
    /// - `.csv` files are treated as CSV (comma-separated values)
    /// - All other extensions are treated as delimiter-separated files
    /// - Files without extensions default to CSV format
    ///
    /// # Arguments
    /// * `path` - File path to analyze
    ///
    /// # Returns
    /// * `FileFormat` - Detected format (Csv or Delimited) based on file extension
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
