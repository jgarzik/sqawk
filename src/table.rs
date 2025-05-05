//! Table module for sqawk
//!
//! This module provides the in-memory table representation for the sqawk utility.

use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::error::{SqawkError, SqawkResult};

/// Represents a value in a table cell
/// 
/// This enum provides the possible data types for a cell value in a table.
/// It supports the common SQL data types and allows for type conversions
/// between numeric types (Integer <-> Float) for comparison operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    /// Represents a NULL or missing value
    Null,
    /// 64-bit signed integer
    Integer(i64),
    /// 64-bit floating point number
    Float(f64),
    /// UTF-8 string
    String(String),
    /// Boolean value (true/false)
    Boolean(bool),
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => a == b,
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Boolean(a), Value::Boolean(b)) => a == b,
            // Handle comparisons between Integer and Float
            (Value::Integer(a), Value::Float(b)) => *a as f64 == *b,
            (Value::Float(a), Value::Integer(b)) => *a == *b as f64,
            // All other combinations are not equal
            _ => false,
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Integer(i) => write!(f, "{}", i),
            Value::Float(float) => write!(f, "{}", float),
            Value::String(s) => write!(f, "{}", s),
            Value::Boolean(b) => write!(f, "{}", b),
        }
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        // Try to parse as integer first
        if let Ok(i) = s.parse::<i64>() {
            return Value::Integer(i);
        }
        
        // Try to parse as float
        if let Ok(fl) = s.parse::<f64>() {
            return Value::Float(fl);
        }
        
        // Try to parse as boolean
        match s.to_lowercase().as_str() {
            "true" | "yes" | "1" => return Value::Boolean(true),
            "false" | "no" | "0" => return Value::Boolean(false),
            "" => return Value::Null,
            _ => {}
        }
        
        // Default to string
        Value::String(s.to_string())
    }
}

/// Represents a row in a table
pub type Row = Vec<Value>;

/// Represents an in-memory table
#[derive(Debug, Clone)]
pub struct Table {
    /// Name of the table
    name: String,
    
    /// Column names
    columns: Vec<String>,
    
    /// Map of column names to their indices
    column_map: HashMap<String, usize>,
    
    /// Rows of data
    rows: Vec<Row>,
    
    /// Source file path, if loaded from a file
    source_file: Option<PathBuf>,
    
    /// Whether the table was modified since loading
    modified: bool,
}

impl Table {
    /// Create a new table with the given name and columns
    pub fn new(name: &str, columns: Vec<String>, source_file: Option<PathBuf>) -> Self {
        let column_map = columns
            .iter()
            .enumerate()
            .map(|(i, name)| (name.clone(), i))
            .collect();
        
        Table {
            name: name.to_string(),
            columns,
            column_map,
            rows: Vec::new(),
            source_file,
            modified: false,
        }
    }
    
    /// Get the table name
    #[allow(dead_code)]
    pub fn name(&self) -> &str {
        &self.name
    }
    
    /// Get the columns of the table
    pub fn columns(&self) -> &[String] {
        &self.columns
    }
    
    /// Get the column count
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }
    
    /// Get the rows of the table
    pub fn rows(&self) -> &[Row] {
        &self.rows
    }
    
    /// Get the row count
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }
    
    /// Get a reference to a row by index
    #[allow(dead_code)]
    pub fn row(&self, index: usize) -> Option<&Row> {
        self.rows.get(index)
    }
    
    /// Add a row to the table
    pub fn add_row(&mut self, row: Row) -> SqawkResult<()> {
        if row.len() != self.columns.len() {
            return Err(SqawkError::InvalidSqlQuery(format!(
                "Row has {} columns, but table '{}' has {} columns",
                row.len(),
                self.name,
                self.columns.len()
            )));
        }
        
        self.rows.push(row);
        self.modified = true;
        Ok(())
    }
    
    /// Get the source file path
    pub fn source_file(&self) -> Option<&PathBuf> {
        self.source_file.as_ref()
    }
    
    /// Check if the table was modified
    #[allow(dead_code)]
    pub fn is_modified(&self) -> bool {
        self.modified
    }
    
    /// Get the index of a column by name
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.column_map.get(name).copied()
    }
    
    /// Print the table to stdout
    pub fn print_to_stdout(&self) -> Result<()> {
        // Print header
        for (i, col) in self.columns.iter().enumerate() {
            if i > 0 {
                print!(",");
            }
            print!("{}", col);
        }
        println!();
        
        // Print rows
        for row in &self.rows {
            for (i, val) in row.iter().enumerate() {
                if i > 0 {
                    print!(",");
                }
                print!("{}", val);
            }
            println!();
        }
        
        Ok(())
    }
    
    /// Create a new table with a subset of rows matching a predicate
    pub fn select<F>(&self, predicate: F) -> Self 
    where
        F: Fn(&Row) -> bool
    {
        let mut result = Table::new(&self.name, self.columns.clone(), None);
        
        for row in &self.rows {
            if predicate(row) {
                result.rows.push(row.clone());
            }
        }
        
        result
    }
    
    /// Replace all rows with a new set
    /// 
    /// This method is useful for operations like DELETE that need to replace
    /// the content of the table with a filtered subset of rows.
    /// 
    /// # Arguments
    /// * `new_rows` - The new set of rows to replace the existing ones
    pub fn replace_rows(&mut self, new_rows: Vec<Row>) {
        self.rows = new_rows;
        self.modified = true;
    }
    
    /// Delete rows that match a predicate
    ///
    /// This method removes rows that match the given predicate function.
    /// It's the inverse of select() - it keeps rows where the predicate is false.
    ///
    /// # Arguments
    /// * `predicate` - A function that returns true for rows that should be deleted
    ///
    /// # Returns
    /// The number of rows that were deleted
    pub fn delete_where<F>(&mut self, predicate: F) -> usize
    where
        F: Fn(&Row) -> bool
    {
        let original_count = self.rows.len();
        
        // Keep rows where the predicate is false (inverse of select)
        let remaining_rows: Vec<Row> = self.rows
            .iter()
            .filter(|row| !predicate(row))
            .cloned()
            .collect();
        
        let new_count = remaining_rows.len();
        self.replace_rows(remaining_rows);
        
        // Return number of deleted rows
        original_count - new_count
    }
    
    /// Create a new table with only specified columns
    pub fn project(&self, column_indices: &[usize]) -> SqawkResult<Self> {
        // Validate column indices
        for &idx in column_indices {
            if idx >= self.columns.len() {
                return Err(SqawkError::ColumnNotFound(format!("Column index {} out of bounds", idx)));
            }
        }
        
        // Create new column list
        let columns: Vec<String> = column_indices
            .iter()
            .map(|&idx| self.columns[idx].clone())
            .collect();
        
        let mut result = Table::new(&self.name, columns, None);
        
        // Project rows
        for row in &self.rows {
            let projected_row: Vec<Value> = column_indices
                .iter()
                .map(|&idx| row[idx].clone())
                .collect();
            
            result.add_row(projected_row)?;
        }
        
        Ok(result)
    }
}
