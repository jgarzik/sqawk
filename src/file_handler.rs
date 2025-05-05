//! File handling module for sqawk
//!
//! This module provides a unified interface for loading and saving different file formats:
//! - CSV files (comma-separated values)
//! - Delimiter-separated files (tab, colon, etc.)
//!
//! It abstracts away the specific file format details and provides a consistent API
//! for the rest of the application to work with in-memory tables.

use std::collections::HashMap;
use std::path::PathBuf;

use crate::csv_handler::CsvHandler;
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
    /// In-memory tables indexed by their names
    tables: HashMap<String, Table>,
    
    /// Handler for CSV files
    csv_handler: CsvHandler,
    
    /// Handler for delimiter-separated files
    delim_handler: DelimHandler,
    
    /// Default format to use if not specified (underscore prefix indicates it's intentionally unused for now)
    _default_format: FileFormat,
    
    /// Custom field separator if specified
    field_separator: Option<String>,
}

impl FileHandler {
    /// Create a new FileHandler with specified field separator
    ///
    /// # Arguments
    /// * `field_separator` - Optional field separator character/string
    ///
    /// # Returns
    /// A new FileHandler instance ready to load and manage tables
    pub fn new(field_separator: Option<String>) -> Self {
        let default_format = if field_separator.is_some() {
            FileFormat::Delimited
        } else {
            FileFormat::Csv
        };
        
        FileHandler {
            tables: HashMap::new(),
            csv_handler: CsvHandler::new(),
            delim_handler: DelimHandler::new(field_separator.clone()),
            _default_format: default_format,
            field_separator,
        }
    }
    
    /// Load a file into an in-memory table
    ///
    /// This method determines the file format based on extension or explicit format argument
    /// and delegates to the appropriate handler.
    ///
    /// # Arguments
    /// * `file_spec` - File specification in the format `[table_name=]file_path`
    /// * `format` - Optional format override
    ///
    /// # Returns
    /// * `Ok(())` if the file was successfully loaded
    /// * `Err` if there was an error parsing the file spec, opening the file, or parsing the file data
    pub fn load_file(&mut self, file_spec: &str) -> SqawkResult<()> {
        // Parse file spec to get table name and file path
        let (table_name, file_path) = self.parse_file_spec(file_spec)?;
        
        // Determine the file format based on extension
        let format = self.detect_format(&file_path);
        
        match format {
            FileFormat::Csv => {
                let table = self.csv_handler.load_csv(file_spec)?;
                self.tables.insert(table_name, table);
            },
            FileFormat::Delimited => {
                let delimiter = self.field_separator.as_deref().unwrap_or("\t");
                let table = self.delim_handler.load_delimited(file_spec, delimiter)?;
                self.tables.insert(table_name, table);
            },
        }
        
        Ok(())
    }
    
    /// Save a table back to its source file
    ///
    /// Writes the current state of a table back to its source file,
    /// preserving column order and formatting values appropriately.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to write to its source file
    ///
    /// # Returns
    /// * `Ok(())` if the table was successfully written
    /// * `Err` if the table doesn't exist, lacks a source file, or if there was an error writing the file
    pub fn save_table(&self, table_name: &str) -> SqawkResult<()> {
        let table = self.get_table(table_name)?;
        
        // Check if the table has a source file
        let file_path = table.source_file().ok_or_else(|| {
            SqawkError::InvalidSqlQuery(format!(
                "Table '{}' doesn't have a source file",
                table_name
            ))
        })?;
        
        // Determine the file format based on extension
        let format = self.detect_format(file_path);
        
        match format {
            FileFormat::Csv => {
                self.csv_handler.save_table(table_name, table)?;
            },
            FileFormat::Delimited => {
                let delimiter = self.field_separator.as_deref().unwrap_or("\t");
                self.delim_handler.save_table(table_name, table, delimiter)?;
            },
        }
        
        Ok(())
    }
    
    /// Get a reference to a table by name
    pub fn get_table(&self, name: &str) -> SqawkResult<&Table> {
        self.tables
            .get(name)
            .ok_or_else(|| SqawkError::TableNotFound(name.to_string()))
    }
    
    /// Get a mutable reference to a table by name
    pub fn get_table_mut(&mut self, name: &str) -> SqawkResult<&mut Table> {
        self.tables
            .get_mut(name)
            .ok_or_else(|| SqawkError::TableNotFound(name.to_string()))
    }
    
    /// Get the names of all tables in the collection
    pub fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }
    
    /// Get the number of tables in the collection
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }
    
    /// Parse a file specification into table name and file path
    ///
    /// Handles two formats:
    /// 1. `table_name=file_path` - Explicit table name and file path
    /// 2. `file_path` - Table name derived from file name
    ///
    /// # Arguments
    /// * `file_spec` - File specification in one of the supported formats
    ///
    /// # Returns
    /// * `Ok((String, PathBuf))` - Tuple of (table_name, file_path)
    /// * `Err` - If the file specification is invalid
    fn parse_file_spec(&self, file_spec: &str) -> SqawkResult<(String, PathBuf)> {
        if let Some((table_name, file_path)) = file_spec.split_once('=') {
            // Table name specified explicitly
            Ok((table_name.to_string(), PathBuf::from(file_path)))
        } else {
            // Table name derived from file name
            let path = PathBuf::from(file_spec);
            // Check that path has a filename
            path.file_name().ok_or_else(|| {
                SqawkError::InvalidFileSpec(format!("Invalid file specification: {}", file_spec))
            })?;

            let stem = path.file_stem().ok_or_else(|| {
                SqawkError::InvalidFileSpec(format!("Invalid file specification: {}", file_spec))
            })?;

            Ok((stem.to_string_lossy().to_string(), path))
        }
    }
    
    /// Detect file format based on extension and default settings
    ///
    /// # Arguments
    /// * `path` - File path to inspect
    ///
    /// # Returns
    /// The detected file format
    fn detect_format(&self, path: &PathBuf) -> FileFormat {
        // If a field separator was explicitly provided, use delimited format
        if self.field_separator.is_some() {
            return FileFormat::Delimited;
        }
        
        // Otherwise determine by extension
        match path.extension().and_then(|ext| ext.to_str()) {
            Some("csv") => FileFormat::Csv,
            _ => FileFormat::Delimited, // Default to delimited for non-csv files
        }
    }
}