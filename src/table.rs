//! Table module for sqawk
//!
//! This module provides the in-memory table representation for the sqawk utility.

use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;

use anyhow::Result;
use serde::{Deserialize, Serialize};

/// Represents a reference to a column, which can be qualified with a table name
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnRef {
    /// Optional table name qualifier
    pub table_name: Option<String>,
    /// Column name
    pub column_name: String,
}

impl ColumnRef {
    /// Create a new column reference
    pub fn new(column_name: &str) -> Self {
        Self {
            table_name: None,
            column_name: column_name.to_string(),
        }
    }

    /// Create a new qualified column reference
    pub fn qualified(table_name: &str, column_name: &str) -> Self {
        Self {
            table_name: Some(table_name.to_string()),
            column_name: column_name.to_string(),
        }
    }

    /// Get the fully qualified name (table.column)
    pub fn qualified_name(&self) -> String {
        match &self.table_name {
            Some(table) => format!("{}.{}", table, self.column_name),
            None => self.column_name.clone(),
        }
    }
}

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



    /// Get the name of the table
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
    
    /// Get value at specific row and column
    pub fn get_value(&self, row_idx: usize, col_idx: usize) -> Option<&Value> {
        self.rows.get(row_idx).and_then(|row| row.get(col_idx))
    }
    
    /// Check if the table has been modified
    pub fn is_modified(&self) -> bool {
        self.modified
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
        F: Fn(&Row) -> bool,
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
    
    /// Update a single value in a specific row and column
    ///
    /// # Arguments
    /// * `row_idx` - The index of the row to update
    /// * `col_idx` - The index of the column to update
    /// * `value` - The new value to set
    ///
    /// # Returns
    /// * `Ok(())` if the update was successful
    /// * `Err` if the row or column index is out of bounds
    pub fn update_value(&mut self, row_idx: usize, col_idx: usize, value: Value) -> SqawkResult<()> {
        if row_idx >= self.rows.len() {
            return Err(SqawkError::InvalidSqlQuery(format!(
                "Row index {} is out of bounds (table has {} rows)",
                row_idx,
                self.rows.len()
            )));
        }
        
        if col_idx >= self.columns.len() {
            return Err(SqawkError::ColumnNotFound(format!(
                "Column index {} is out of bounds (table has {} columns)",
                col_idx,
                self.columns.len()
            )));
        }
        
        self.rows[row_idx][col_idx] = value;
        self.modified = true;
        Ok(())
    }

    /// Create a new table with only specified columns
    pub fn project(&self, column_indices: &[usize]) -> SqawkResult<Self> {
        // Validate column indices
        for &idx in column_indices {
            if idx >= self.columns.len() {
                return Err(SqawkError::ColumnNotFound(format!(
                    "Column index {} out of bounds",
                    idx
                )));
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
            let projected_row: Vec<Value> =
                column_indices.iter().map(|&idx| row[idx].clone()).collect();

            result.add_row(projected_row)?;
        }

        Ok(result)
    }
    
    /// Create a new table with fully qualified column names (table.column)
    ///
    /// This is useful for join operations where columns from different tables
    /// need to be distinguishable.
    pub fn with_qualified_columns(&self) -> Self {
        let qualified_columns = self
            .columns
            .iter()
            .map(|col| format!("{}.{}", self.name, col))
            .collect();
            
        let mut result = Table::new(&self.name, qualified_columns, None);
        
        // Copy rows
        for row in &self.rows {
            // Safe to unwrap since we're just copying rows
            result.add_row(row.clone()).unwrap();
        }
        
        result
    }
    
    /// Get a column's index, handling qualified names ("table.column")
    ///
    /// This allows lookup of columns using both simple names and qualified names.
    /// For example, both "name" and "users.name" would match the "name" column
    /// in a table named "users".
    ///
    /// # Arguments
    /// * `column_ref` - The column reference, which may be qualified
    ///
    /// # Returns
    /// * The column index if found, or None if not found
    pub fn get_column_index(&self, column_ref: &ColumnRef) -> Option<usize> {
        match &column_ref.table_name {
            // If qualified, check table name matches
            Some(table_name) if table_name != &self.name => None,
            // Either matching table name or no table qualification
            _ => self.column_index(&column_ref.column_name),
        }
    }
    
    /// Execute a CROSS JOIN with another table
    ///
    /// This creates a cartesian product of the two tables.
    ///
    /// # Arguments
    /// * `right` - The right table to join with
    ///
    /// # Returns
    /// * A new table containing the joined data
    pub fn cross_join(&self, right: &Self) -> SqawkResult<Self> {
        // Create result table with prefixed column names
        let mut columns = Vec::new();
        
        // Add columns from left table (self)
        for col in self.columns() {
            // If the column already has a table prefix, keep it as is
            // Otherwise, add the table name prefix
            if col.contains('.') {
                columns.push(col.clone());
            } else {
                columns.push(format!("{}.{}", self.name, col));
            }
        }
        
        // Add columns from right table
        for col in right.columns() {
            // If the column already has a table prefix, keep it as is
            // Otherwise, add the table name prefix
            if col.contains('.') {
                columns.push(col.clone());
            } else {
                columns.push(format!("{}.{}", right.name, col));
            }
        }
        
        // Create a new table to hold the join result
        // Use original table names in the result to keep column references simple
        let mut result = Table::new(
            &format!("join_result"), 
            columns, 
            None
        );
        
        // For CROSS JOIN, we include every combination of rows
        for left_row in self.rows() {
            for right_row in right.rows() {
                // Create combined row
                let mut new_row = Vec::with_capacity(self.column_count() + right.column_count());
                
                // Add values from left row
                for i in 0..self.column_count() {
                    new_row.push(left_row.get(i).unwrap_or(&Value::Null).clone());
                }
                
                // Add values from right row
                for i in 0..right.column_count() {
                    new_row.push(right_row.get(i).unwrap_or(&Value::Null).clone());
                }
                
                // Add the combined row to the result table
                result.add_row(new_row)?;
            }
        }
        
        Ok(result)
    }
}
