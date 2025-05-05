//! CSV file handling module for sqawk
//!
//! This module handles loading CSV files into in-memory tables and saving tables back to CSV files.
//! It provides functionality for:
//!
//! - Loading CSV files with automatic header detection
//! - Parsing file specifications in the format [table_name=]file_path.csv
//! - Managing a collection of in-memory tables
//! - Converting between CSV records and the internal Value type
//! - Writing modified tables back to CSV files
//!
//! The module uses buffered I/O operations for efficiency and maintains
//! a mapping between table names and their source files for writeback operations.

use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::PathBuf;

use crate::error::{SqawkError, SqawkResult};
use crate::table::{Table, Value};

/// Handles loading and saving CSV files
///
/// This struct provides methods for loading tables from CSV files
/// and writing them back when modified. It's specialized for handling
/// data in CSV format with commas as separators.
pub struct CsvHandler {}

impl Default for CsvHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl CsvHandler {
    /// Create a new CsvHandler
    ///
    /// # Returns
    /// A new CsvHandler instance ready to load and manage CSV files
    pub fn new() -> Self {
        CsvHandler {}
    }

    /// Load a CSV file into an in-memory table
    ///
    /// This method parses CSV files with header rows, creating tables with
    /// appropriate column names and automatically inferring data types for each cell.
    ///
    /// # Arguments
    /// * `file_spec` - File specification in the format `[table_name=]file_path.csv`
    ///                 If table_name is not specified, the file name without extension is used.
    ///
    /// # Returns
    /// * `Ok(Table)` - The successfully loaded table
    /// * `Err` if there was an error parsing the file spec, opening the file, or parsing the CSV data
    pub fn load_csv(&self, file_spec: &str) -> SqawkResult<Table> {
        // Parse file spec to get table name and file path
        let (table_name, file_path) = self.parse_file_spec(file_spec)?;

        // Open the CSV file
        let file = File::open(&file_path)?;
        let reader = BufReader::new(file);

        // Create a CSV reader
        let mut csv_reader = csv::ReaderBuilder::new()
            .has_headers(true)
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

    /// Save a table back to its source CSV file
    ///
    /// Writes the current state of a table back to its source CSV file,
    /// preserving column order and formatting values appropriately.
    /// This is used for implementing the --write flag functionality.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to write
    /// * `table` - The table to save
    ///
    /// # Returns
    /// * `Ok(())` if the table was successfully written
    /// * `Err` if the table lacks a source file, or if there was an error writing the file
    pub fn save_table(&self, _table_name: &str, table: &Table) -> SqawkResult<()> {
        // Check if the table has a source file
        let file_path = table.source_file().ok_or_else(|| {
            SqawkError::InvalidSqlQuery(format!(
                "Table '{}' doesn't have a source file",
                table.name()
            ))
        })?;

        // Open the CSV file for writing
        let file = File::create(file_path)?;
        let writer = BufWriter::new(file);

        // Create a CSV writer
        let mut csv_writer = csv::WriterBuilder::new().from_writer(writer);

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
    /// 1. `table_name=file_path.csv` - Explicit table name and file path
    /// 2. `file_path.csv` - Table name derived from file name
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
