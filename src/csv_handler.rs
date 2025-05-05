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

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::PathBuf;

// Import removed as it's no longer needed
// Using csv crate without specific imports
use csv;

use crate::error::{SqawkError, SqawkResult};
use crate::table::{Table, Value};

/// Handles loading and saving CSV files
///
/// This struct serves as the central repository for in-memory tables and provides
/// methods for loading tables from CSV files and writing them back when modified.
/// It maintains a mapping of table names to their data structures and handles
/// the table name resolution for file specifications.
pub struct CsvHandler {
    /// In-memory tables indexed by their names
    tables: HashMap<String, Table>,
}

impl CsvHandler {
    /// Create a new CsvHandler with an empty table collection
    /// 
    /// # Returns
    /// A new CsvHandler instance ready to load and manage tables
    /// 
    /// # Example
    /// ```
    /// # use sqawk::csv_handler::CsvHandler;
    /// let handler = CsvHandler::new();
    /// ```
    pub fn new() -> Self {
        CsvHandler {
            tables: HashMap::new(),
        }
    }

    /// Load a CSV file into an in-memory table
    ///
    /// This method parses CSV files with header rows, creating tables with
    /// appropriate column names and automatically inferring data types for each cell.
    /// The table is added to the handler's collection and can be accessed by name.
    ///
    /// # Arguments
    /// * `file_spec` - File specification in the format `[table_name=]file_path.csv`
    ///                 If table_name is not specified, the file name without extension is used.
    ///
    /// # Returns
    /// * `Ok(())` if the file was successfully loaded
    /// * `Err` if there was an error parsing the file spec, opening the file, or parsing the CSV data
    ///
    /// # Example
    /// ```no_run
    /// # use sqawk::csv_handler::CsvHandler;
    /// # use sqawk::error::SqawkResult;
    /// # fn example() -> SqawkResult<()> {
    /// # let mut handler = CsvHandler::new();
    /// // Load with default table name (derived from filename)
    /// handler.load_csv("data/users.csv")?;
    /// 
    /// // Load with explicit table name
    /// handler.load_csv("people=data/users.csv")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn load_csv(&mut self, file_spec: &str) -> SqawkResult<()> {
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
            .map_err(|e| SqawkError::CsvError(e))?
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();

        // Create a new table
        let mut table = Table::new(&table_name, headers, Some(file_path.clone()));

        // Read rows
        for result in csv_reader.records() {
            let record = result.map_err(|e| SqawkError::CsvError(e))?;

            // Convert record to a row of values
            let row = record.iter().map(|field| Value::from(field)).collect();

            table.add_row(row)?;
        }

        // Add table to the collection
        self.tables.insert(table_name, table);

        Ok(())
    }

    /// Save a table back to its source CSV file
    ///
    /// Writes the current state of a table back to its source CSV file,
    /// preserving column order and formatting values appropriately.
    /// This is used for implementing the --write flag functionality.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to write to its source file
    ///
    /// # Returns
    /// * `Ok(())` if the table was successfully written
    /// * `Err` if the table doesn't exist, lacks a source file, or if there was an error writing the file
    ///
    /// # Example
    /// ```no_run
    /// # use sqawk::csv_handler::CsvHandler;
    /// # use sqawk::error::SqawkResult;
    /// # fn example() -> SqawkResult<()> {
    /// # let mut handler = CsvHandler::new();
    /// # handler.load_csv("users.csv")?;
    /// // After modifying the table data, write it back to disk
    /// handler.save_table("users")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn save_table(&self, table_name: &str) -> SqawkResult<()> {
        let table = self.get_table(table_name)?;

        // Check if the table has a source file
        let file_path = table.source_file().ok_or_else(|| {
            SqawkError::InvalidSqlQuery(format!(
                "Table '{}' doesn't have a source file",
                table_name
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
            .map_err(|e| SqawkError::CsvError(e))?;

        // Write rows
        for row in table.rows() {
            let record: Vec<String> = row.iter().map(|value| value.to_string()).collect();

            csv_writer
                .write_record(&record)
                .map_err(|e| SqawkError::CsvError(e))?;
        }

        // Flush and finish
        csv_writer.flush()?;

        Ok(())
    }

    /// Get a reference to a table by name
    ///
    /// Retrieves a read-only reference to a table for query operations.
    ///
    /// # Arguments
    /// * `name` - Name of the table to retrieve
    ///
    /// # Returns
    /// * `Ok(&Table)` - Reference to the requested table
    /// * `Err` - If the table doesn't exist in the collection
    ///
    /// # Example
    /// ```no_run
    /// # use sqawk::csv_handler::CsvHandler;
    /// # use sqawk::error::SqawkResult;
    /// # fn example() -> SqawkResult<()> {
    /// # let mut handler = CsvHandler::new();
    /// # handler.load_csv("users.csv")?;
    /// let users_table = handler.get_table("users")?;
    /// println!("Table has {} rows", users_table.row_count());
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_table(&self, name: &str) -> SqawkResult<&Table> {
        self.tables
            .get(name)
            .ok_or_else(|| SqawkError::TableNotFound(name.to_string()))
    }

    /// Get a mutable reference to a table by name
    ///
    /// Retrieves a mutable reference to a table for modification operations.
    ///
    /// # Arguments
    /// * `name` - Name of the table to retrieve
    ///
    /// # Returns
    /// * `Ok(&mut Table)` - Mutable reference to the requested table
    /// * `Err` - If the table doesn't exist in the collection
    ///
    /// # Example
    /// ```no_run
    /// # use sqawk::csv_handler::CsvHandler;
    /// # use sqawk::error::SqawkResult;
    /// # use sqawk::table::Value;
    /// # fn example() -> SqawkResult<()> {
    /// # let mut handler = CsvHandler::new();
    /// # handler.load_csv("users.csv")?;
    /// // Get a mutable reference for modifications
    /// let users_table = handler.get_table_mut("users")?;
    /// // Perform modifications...
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_table_mut(&mut self, name: &str) -> SqawkResult<&mut Table> {
        self.tables
            .get_mut(name)
            .ok_or_else(|| SqawkError::TableNotFound(name.to_string()))
    }

    /// Get the names of all tables in the collection
    ///
    /// Returns a vector containing all table names in no particular order.
    /// This is useful for diagnostic information and for iterating over all tables.
    ///
    /// # Returns
    /// * A vector of table names as strings
    ///
    /// # Example
    /// ```no_run
    /// # use sqawk::csv_handler::CsvHandler;
    /// # let mut handler = CsvHandler::new();
    /// // Get names of all loaded tables
    /// let table_names = handler.table_names();
    /// println!("Loaded tables: {:?}", table_names);
    /// ```
    pub fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    /// Get the number of tables in the collection
    ///
    /// Returns the count of tables currently loaded in the handler.
    ///
    /// # Returns
    /// * The number of tables
    ///
    /// # Example
    /// ```no_run
    /// # use sqawk::csv_handler::CsvHandler;
    /// # let handler = CsvHandler::new();
    /// println!("Loaded {} tables", handler.table_count());
    /// ```
    pub fn table_count(&self) -> usize {
        self.tables.len()
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
