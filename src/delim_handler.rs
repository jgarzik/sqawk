//! Delimiter-separated values file handling module for sqawk
//!
//! This module handles loading and saving delimiter-separated files into in-memory tables.
//! It works with files that use custom delimiters like tabs, colons, or any other character
//! to separate fields, similar to awk's -F option.
//!
//! The module provides functionality for:
//!
//! - Loading files with custom field separators specified by the user
//! - Parsing file specifications in the format [table_name=]file_path
//! - Converting between delimited records and the internal Value type
//! - Writing modified tables back to delimiter-separated files
//!
//! This implementation reuses the CSV crate's functionality but configures it
//! to use the specified delimiter instead of commas.

use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::PathBuf;

use crate::error::{SqawkError, SqawkResult};
use crate::table::{Table, Value};

/// Handles loading and saving delimiter-separated value files
pub struct DelimHandler {
    /// Default field separator to use if none is specified
    _field_separator: Option<String>, // Underscore prefix indicates that it's intentionally unused
}

impl DelimHandler {
    /// Create a new DelimHandler with an optional default field separator
    ///
    /// # Arguments
    /// * `field_separator` - Optional default field separator
    ///
    /// # Returns
    /// A new DelimHandler instance ready to load and manage delimiter-separated files
    pub fn new(field_separator: Option<String>) -> Self {
        DelimHandler {
            _field_separator: field_separator,
        }
    }

    /// Load a delimiter-separated file into an in-memory table
    ///
    /// This method parses files with the specified delimiter and header rows,
    /// creating tables with appropriate column names and automatically inferring
    /// data types for each cell.
    ///
    /// # Arguments
    /// * `file_spec` - File specification in the format `[table_name=]file_path`
    /// * `delimiter` - Delimiter character to use for parsing
    ///
    /// # Returns
    /// * `Ok(Table)` - The successfully loaded table
    /// * `Err` if there was an error parsing the file spec, opening the file, or parsing the file data
    pub fn load_delimited(&self, file_spec: &str, delimiter: &str) -> SqawkResult<Table> {
        // Parse file spec to get table name and file path
        let (table_name, file_path) = self.parse_file_spec(file_spec)?;

        // Open the file
        let file = File::open(&file_path)?;
        let reader = BufReader::new(file);

        // Get the delimiter as a byte
        let delimiter_byte = if delimiter.len() == 1 {
            delimiter.as_bytes()[0]
        } else if delimiter == "\\t" {
            b'\t' // Handle special case for tab
        } else {
            return Err(SqawkError::InvalidFileSpec(format!(
                "Invalid delimiter: {}. Must be a single character.",
                delimiter
            )));
        };

        // Create a CSV reader with custom delimiter
        let mut csv_reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .delimiter(delimiter_byte)
            .from_reader(reader);

        // Get headers
        let headers = csv_reader
            .headers()
            .map_err(SqawkError::CsvError)?
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();

        // Create a new table
        let mut table = Table::new(&table_name, headers, Some(file_path.clone()));

        // Read rows
        for result in csv_reader.records() {
            let record = result.map_err(SqawkError::CsvError)?;

            // Convert record to a row of values
            let row = record.iter().map(Value::from).collect();

            table.add_row(row)?;
        }

        Ok(table)
    }

    /// Save a table back to a delimiter-separated file
    ///
    /// Writes the current state of a table back to its source file,
    /// preserving column order and formatting values appropriately.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to write
    /// * `table` - The table to save
    /// * `delimiter` - Delimiter character to use for writing
    ///
    /// # Returns
    /// * `Ok(())` if the table was successfully written
    /// * `Err` if the table lacks a source file, or if there was an error writing the file
    pub fn save_table(&self, _table_name: &str, table: &Table, delimiter: &str) -> SqawkResult<()> {
        // Check if the table has a source file
        let file_path = table.source_file().ok_or_else(|| {
            SqawkError::InvalidSqlQuery(format!(
                "Table '{}' doesn't have a source file",
                table.name()
            ))
        })?;

        // Get the delimiter as a byte
        let delimiter_byte = if delimiter.len() == 1 {
            delimiter.as_bytes()[0]
        } else if delimiter == "\\t" {
            b'\t' // Handle special case for tab
        } else {
            return Err(SqawkError::InvalidFileSpec(format!(
                "Invalid delimiter: {}. Must be a single character.",
                delimiter
            )));
        };

        // Open the file for writing
        let file = File::create(file_path)?;
        let writer = BufWriter::new(file);

        // Create a CSV writer with custom delimiter
        let mut csv_writer = csv::WriterBuilder::new()
            .delimiter(delimiter_byte)
            .from_writer(writer);

        // Write headers
        csv_writer
            .write_record(table.columns())
            .map_err(SqawkError::CsvError)?;

        // Write rows
        for row in table.rows() {
            let record: Vec<String> = row.iter().map(|value| value.to_string()).collect();

            csv_writer
                .write_record(&record)
                .map_err(SqawkError::CsvError)?;
        }

        // Flush and finish
        csv_writer.flush()?;

        Ok(())
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
}
