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
use std::io::BufReader;
use std::path::{Path, PathBuf};

use crate::error::{SqawkError, SqawkResult};
use crate::table::{Table, Value};

/// Handles loading and saving delimiter-separated value files
///
/// This struct provides methods for working with files that use custom delimiters
/// (like tabs, pipes, or spaces) instead of commas. It's similar to the CsvHandler
/// but allows for flexibility in the field separator character.
///
/// Key capabilities:
/// - Loading files with custom field separators
/// - Inferring column names when headers are missing
/// - Automatic data type detection for values
/// - Writing tables back to delimited files
pub struct DelimHandler {}

impl Default for DelimHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl DelimHandler {
    /// Create a new DelimHandler
    ///
    /// # Returns
    /// A new DelimHandler instance ready to load and manage delimiter-separated files
    pub fn new() -> Self {
        DelimHandler {}
    }

    /// Save a table to a delimiter-separated file
    ///
    /// # Arguments
    /// * `table` - The table to save
    /// * `file_path` - The path to the file
    /// * `delimiter` - The delimiter to use (e.g., ",", "\t")
    ///
    /// # Returns
    /// * `SqawkResult<()>` - Result of the operation
    pub fn save_delimited(
        &self,
        table: &Table,
        file_path: &Path,
        delimiter: &str,
    ) -> SqawkResult<()> {
        use std::fs::File;
        use std::io::{BufWriter, Write};

        // Open the file for writing
        let file = File::create(file_path).map_err(SqawkError::IoError)?;
        let mut writer = BufWriter::new(file);

        // Write the header row
        let header = table.columns().join(delimiter);
        writeln!(writer, "{}", header).map_err(SqawkError::IoError)?;

        // Write data rows
        for row in table.rows() {
            let row_values: Vec<String> = row.iter().map(|value| value.to_string()).collect();

            let row_str = row_values.join(delimiter);
            writeln!(writer, "{}", row_str).map_err(SqawkError::IoError)?;
        }

        // Flush and close the writer
        writer.flush().map_err(SqawkError::IoError)?;

        Ok(())
    }

    /// Load a delimiter-separated file into an in-memory table
    ///
    /// This method parses files with the specified delimiter and header rows,
    /// creating tables with appropriate column names and automatically inferring
    /// data types for each cell.
    ///
    /// # Headers and Column Detection
    /// The method implements several strategies for determining column names:
    /// - Uses custom column names if provided
    /// - Detects header row in the file when present
    /// - Intelligently determines if first row is data rather than headers (for files like /etc/passwd)
    /// - Generates alphabetical column names (a, b, c...) when headers are missing
    ///
    /// # Special Delimiter Handling
    /// - Supports single-character delimiters (e.g., comma, pipe, colon)
    /// - Special handling for tab delimiter with `\t` notation
    /// - Supports comment lines starting with '#' for system files
    ///
    /// # Arguments
    /// * `file_spec` - File specification in the format `[table_name=]file_path`
    /// * `delimiter` - Delimiter character to use for parsing
    /// * `custom_columns` - Optional custom column names to use instead of detected/generated ones
    ///
    /// # Returns
    /// * `Ok(Table)` - The successfully loaded table
    /// * `Err` if there was an error parsing the file spec, opening the file, or parsing the file data
    pub fn load_delimited(
        &self,
        file_spec: &str,
        delimiter: &str,
        custom_columns: Option<Vec<String>>,
    ) -> SqawkResult<Table> {
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
        // Also add support for comment lines (starting with #) for system files like /etc/passwd
        let mut csv_reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .delimiter(delimiter_byte)
            .comment(Some(b'#')) // Support for comment lines starting with #
            .flexible(true) // Allow for variable number of fields
            .from_reader(reader);

        // If custom column names are provided, use them
        // Otherwise detect/generate headers based on file content
        let headers = if let Some(columns) = custom_columns {
            // Use the provided custom column names
            columns
        } else {
            // No custom column names, generate or detect from file
            match csv_reader.headers().map_err(SqawkError::CsvError) {
                Ok(header_row) => {
                    // Check if the first row looks like data rather than headers
                    // This helps with system files like /etc/passwd that don't have headers
                    let is_likely_data = header_row.iter().any(|field| {
                        // Common indicators that a field is data, not a header
                        field.starts_with('/') || // Path
                        field == "*" ||           // Password placeholder
                        field == "root" ||        // Common username
                        field == "nobody" ||      // Common username
                        field.parse::<i32>().is_ok() // Numeric ID
                    });

                    if is_likely_data {
                        // Generate alphabetical column names (a, b, c, etc.)
                        (0..header_row.len())
                            .map(|i| {
                                // Convert number to alphabetical column name (a, b, ..., z, aa, ab, ...)
                                let mut name = String::new();
                                let mut n = i;
                                loop {
                                    name.insert(0, (b'a' + (n % 26) as u8) as char);
                                    n /= 26;
                                    if n == 0 {
                                        break;
                                    }
                                    n -= 1; // Adjust for the shift from 0-based to 1-based
                                }
                                name
                            })
                            .collect::<Vec<_>>()
                    } else {
                        // Use the headers as they are
                        header_row.iter().map(|s| s.to_string()).collect::<Vec<_>>()
                    }
                }
                Err(_) => {
                    // If we couldn't read headers, try to determine column count from first record
                    let record_iter = csv_reader.records();
                    let first_record = record_iter.into_iter().next();

                    if let Some(Ok(record)) = first_record {
                        // Generate alphabetical column names (a, b, c, etc.)
                        (0..record.len())
                            .map(|i| {
                                // Convert number to alphabetical column name (a, b, ..., z, aa, ab, ...)
                                let mut name = String::new();
                                let mut n = i;
                                loop {
                                    name.insert(0, (b'a' + (n % 26) as u8) as char);
                                    n /= 26;
                                    if n == 0 {
                                        break;
                                    }
                                    n -= 1; // Adjust for the shift from 0-based to 1-based
                                }
                                name
                            })
                            .collect::<Vec<_>>()
                    } else {
                        // Fallback to a minimal set if we can't determine field count
                        vec!["a".to_string()]
                    }
                }
            }
        };

        // Create a new table with the determined headers and custom delimiter
        let mut table = Table::new_with_delimiter(
            &table_name,
            headers,
            Some(file_path.clone()),
            delimiter.to_string(),
        );

        // Read rows
        for result in csv_reader.records() {
            let record = result.map_err(SqawkError::CsvError)?;

            // Convert record to a row of values
            let row = record.iter().map(Value::from).collect();

            table.add_row(row)?;
        }

        Ok(table)
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
