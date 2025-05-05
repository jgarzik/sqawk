//! CSV file handling module for sqawk
//!
//! This module handles loading CSV files into in-memory tables and saving tables back to CSV files.

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
pub struct CsvHandler {
    /// In-memory tables
    tables: HashMap<String, Table>,
}

impl CsvHandler {
    /// Create a new CsvHandler
    pub fn new() -> Self {
        CsvHandler {
            tables: HashMap::new(),
        }
    }

    /// Load a CSV file into an in-memory table
    ///
    /// The file_spec format is [table_name=]file_path.csv
    /// If table_name is not specified, the file name without extension is used.
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

    /// Add a table to the collection
    ///
    /// This method is not currently used but kept for future extensibility
    #[allow(dead_code)]
    pub fn add_table(&mut self, table: Table) -> SqawkResult<()> {
        let name = table.name().to_string();
        self.tables.insert(name, table);
        Ok(())
    }

    /// Get the names of all tables
    pub fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    /// Get the number of tables
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    /// Parse a file specification into table name and file path
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
