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
use std::path::{Path, PathBuf};

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
    
    /// Save a table to a CSV file
    ///
    /// # Arguments
    /// * `table` - The table to save
    /// * `file_path` - The path to the file
    ///
    /// # Returns
    /// * `SqawkResult<()>` - Result of the operation
    pub fn save_csv(&self, table: &Table, file_path: &Path) -> SqawkResult<()> {
        // Create a CSV writer
        let file = File::create(file_path).map_err(SqawkError::IoError)?;
        let mut writer = csv::Writer::from_writer(file);
            
        // Write header row
        writer.write_record(table.columns())
            .map_err(SqawkError::CsvError)?;
            
        // Write data rows
        for row in table.rows() {
            let string_values: Vec<String> = row.iter()
                .map(|value| value.to_string())
                .collect();
                
            writer.write_record(&string_values)
                .map_err(SqawkError::CsvError)?;
        }
        
        writer.flush().map_err(SqawkError::CsvError)?;
        
        Ok(())
    }

    /// Load a CSV file into an in-memory table
    ///
    /// This method parses CSV files with header rows, creating tables with
    /// appropriate column names and automatically inferring data types for each cell.
    ///
    /// # Arguments
    /// * `file_spec` - File specification in the format `[table_name=]file_path.csv`
    ///   If table_name is not specified, the file name without extension is used.
    /// * `custom_columns` - Optional custom column names to use instead of detected/generated ones
    /// * `recover_errors` - When true, malformed rows will be skipped instead of causing the operation to fail
    ///
    /// # Features
    /// * Supports comments in CSV files (lines starting with #)
    /// * Can recover from malformed rows by skipping them
    /// * Provides detailed error information including line numbers
    ///
    /// # Returns
    /// * `Ok(Table)` - The successfully loaded table
    /// * `Err` if there was an error parsing the file spec, opening the file, or parsing the CSV data
    ///
    /// # Enhanced Error Handling
    /// When the optional `recover_errors` parameter is set to true, the function will:
    /// * Skip malformed rows instead of failing
    /// * Log detailed error information including line numbers
    /// * Continue processing the file to extract all valid rows
    pub fn load_csv(
        &self,
        file_spec: &str,
        custom_columns: Option<Vec<String>>,
        recover_errors: Option<bool>,
    ) -> SqawkResult<Table> {
        // Parse file spec to get table name and file path
        let (table_name, file_path) = self.parse_file_spec(file_spec)?;

        // Open the CSV file
        let file = File::open(&file_path)?;
        let reader = BufReader::new(file);

        // Create a CSV reader with enhanced options
        let mut csv_reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .comment(Some(b'#')) // Support comment lines starting with #
            // Enable flexible mode only if error recovery is requested
            .flexible(true) // Always use flexible mode to allow for skipping errors
            .from_reader(reader);

        // Get headers or use custom column names if provided
        let headers = if let Some(columns) = custom_columns {
            // Use the provided custom column names
            columns
        } else {
            // Use column names from the CSV header row
            csv_reader
                .headers()
                .map_err(SqawkError::CsvError)?
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        };

        // Create a new table with comma delimiter (since this is the CSV handler)
        let mut table = Table::new_with_delimiter(&table_name, headers, Some(file_path.clone()), ",".to_string());

        // Read rows with enhanced error handling
        let should_recover = recover_errors.unwrap_or(false);
        let mut skipped_rows = 0;
        let mut row_number = 0;

        for result in csv_reader.records() {
            row_number += 1;

            match result {
                Ok(record) => {
                    if should_recover && record.len() != table.column_count() {
                        // In recovery mode, handle rows with different column counts
                        let mut row = Vec::new();

                        // For each column in our table
                        for i in 0..table.column_count() {
                            if i < record.len() {
                                // Use the value if it exists
                                row.push(Value::from(record.get(i).unwrap_or("")));
                            } else {
                                // Pad with null values if we need more
                                row.push(Value::Null);
                            }
                        }

                        // Now we have a properly sized row, add it without validation
                        table.add_row_recovery(row)?;
                    } else {
                        // Normal path - convert record to a row of values and validate
                        let row = record.iter().map(Value::from).collect();
                        // This call can fail if the columns don't match and we're not in recovery mode
                        if let Err(e) = table.add_row(row) {
                            if should_recover {
                                // If we're in recovery mode, log and continue
                                skipped_rows += 1;
                                eprintln!(
                                    "Warning: Skipping row at line {} with inconsistent field count: {}",
                                    row_number + 1, // +1 for header row
                                    e
                                );
                            } else {
                                // If we're not in recovery mode, propagate the error
                                return Err(e);
                            }
                        }
                    }
                }
                Err(csv_err) if should_recover => {
                    // Skip this row and continue processing if recovery is enabled
                    skipped_rows += 1;
                    eprintln!(
                        "Warning: Skipping malformed row at line {}: {}",
                        row_number + 1, // +1 for header row
                        csv_err
                    );
                }
                Err(csv_err) => {
                    // Provide detailed error context when failing
                    return Err(SqawkError::CsvParseError {
                        file: file_path.to_string_lossy().to_string(),
                        line: row_number + 1, // +1 for header row
                        error: format!("{}", csv_err),
                    });
                }
            }
        }

        // Report the number of skipped rows if there were any and we're in recovery mode
        if should_recover && skipped_rows > 0 {
            eprintln!(
                "Note: Skipped {} malformed rows while loading {}",
                skipped_rows,
                file_path.to_string_lossy()
            );
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
        let file_path = table.file_path().ok_or_else(|| {
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
