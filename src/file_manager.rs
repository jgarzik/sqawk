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

    /// Default format to use if not specified
    _default_format: FileFormat,

    /// Custom field separator if specified
    field_separator: Option<String>,

    /// Custom column names for tables
    /// Map from table name to a vector of column names
    table_column_defs: HashMap<String, Vec<String>>,
}

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
            csv_handler: CsvHandler::new(),
            delim_handler: DelimHandler::new(field_separator.clone()),
            _default_format: default_format,
            field_separator,
            table_column_defs,
        }
    }

    /// Load a file into an in-memory table in the database
    ///
    /// # Arguments
    /// * `file_spec` - File specification in format [table_name=]file_path
    /// * `database` - Database where the table will be stored
    ///
    /// # Returns
    /// * `SqawkResult<Option<(String, String)>>` - Tuple of (table_name, file_path) if successful
    pub fn load_file(&self, file_spec: &str, database: &mut Database) -> SqawkResult<Option<(String, String)>> {
        // Parse file specification to get table name and file path
        let (table_name, file_path_str) = self.parse_file_spec(file_spec)?;
        let file_path = Path::new(&file_path_str);

        // Determine the file format based on extension
        let format = self.detect_format(file_path);

        // Check if custom column names are defined for this table
        let custom_columns = self.table_column_defs.get(&table_name).cloned();

        match format {
            FileFormat::Csv => {
                let table = self.csv_handler.load_csv(file_spec, custom_columns, None)?;
                database.add_table(table_name.clone(), table)?;
            }
            FileFormat::Delimited => {
                let delimiter = self.field_separator.as_deref().unwrap_or("\t");
                let table =
                    self.delim_handler
                        .load_delimited(file_spec, delimiter, custom_columns)?;
                database.add_table(table_name.clone(), table)?;
            }
        }

        Ok(Some((table_name, file_path_str)))
    }

    /// Save a table from the database back to its source file
    ///
    /// Writes the current state of a table back to its source file,
    /// preserving column order and formatting values appropriately.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to write to its source file
    /// * `database` - Database containing the table
    ///
    /// # Returns
    /// * `Ok(())` if the table was successfully written
    /// * `Err` if the table doesn't exist, lacks a source file, or if there was an error writing the file
    pub fn save_table(&self, table_name: &str, database: &Database) -> SqawkResult<()> {
        let table = database.get_table(table_name)?;

        // Check if the table has a source file
        let file_path = table.file_path().ok_or_else(|| {
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
            }
            FileFormat::Delimited => {
                let delimiter = self.field_separator.as_deref().unwrap_or("\t");
                self.delim_handler
                    .save_table(table_name, table, delimiter)?;
            }
        }

        Ok(())
    }

    /// Parse a file specification into table name and file path
    ///
    /// # Arguments
    /// * `file_spec` - File specification as "table_name=file_path" or just "file_path"
    ///
    /// # Returns
    /// * `Ok((String, String))` containing the extracted table name and file path
    /// * `Err` if parsing failed
    fn parse_file_spec(&self, file_spec: &str) -> SqawkResult<(String, String)> {
        if let Some((table_name, file_path)) = file_spec.split_once('=') {
            // Table name and file path are explicitly specified
            Ok((table_name.trim().to_string(), file_path.trim().to_string()))
        } else {
            // Only file path is specified, derive table name from file name
            let path = Path::new(file_spec.trim());
            
            let file_stem = path
                .file_stem()
                .ok_or_else(|| {
                    SqawkError::InvalidFileSpec(format!("Invalid file path: {}", file_spec))
                })?
                .to_string_lossy()
                .to_string();
                
            Ok((file_stem, file_spec.trim().to_string()))
        }
    }

    /// Detect the file format based on a file path
    ///
    /// # Arguments
    /// * `path` - The file path to analyze
    ///
    /// # Returns
    /// * `FileFormat` - The detected format (CSV or Delimited)
    fn detect_format(&self, path: &Path) -> FileFormat {
        // Check if we have a custom field separator
        if self.field_separator.is_some() {
            return FileFormat::Delimited;
        }

        // Check file extension
        match path.extension().and_then(|e| e.to_str()) {
            Some("csv") => FileFormat::Csv,
            _ => FileFormat::Delimited, // Default to delimited for any other extension
        }
    }

    /// Detect file format based on a file specification
    ///
    /// # Arguments
    /// * `file_spec` - File specification (can include table_name=)
    ///
    /// # Returns
    /// * `FileFormat` - The detected format
    fn detect_format_from_spec(&self, file_spec: &str) -> FileFormat {
        // Extract the file path part from the specification
        let file_path = if let Some((_table_name, path)) = file_spec.split_once('=') {
            path.trim()
        } else {
            file_spec.trim()
        };

        self.detect_format(Path::new(file_path))
    }

    /// Get custom column definitions for a specific file
    ///
    /// # Arguments
    /// * `file_spec` - File specification in format [table_name=]file_path
    ///
    /// # Returns
    /// * `Option<Vec<String>>` - Custom column names if defined
    fn get_column_defs(&self, file_spec: &str) -> Option<Vec<String>> {
        // Extract table name from the file spec
        let table_name = if let Some((name, _)) = file_spec.split_once('=') {
            name.trim()
        } else {
            // For specs without explicit table name, derive from file path
            let path = Path::new(file_spec.trim());
            path.file_stem()?.to_str()?
        };

        // Lookup custom column definitions for this table
        self.table_column_defs.get(table_name).cloned()
    }

    /// Get the field separator
    ///
    /// # Returns
    /// * `Option<&str>` - The field separator, if any
    pub fn field_separator(&self) -> Option<&str> {
        self.field_separator.as_deref()
    }
}