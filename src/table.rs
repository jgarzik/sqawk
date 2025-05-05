//! Table module for sqawk
//!
//! This module provides the in-memory table representation for the sqawk utility.
//! It handles all data storage, manipulation, and table operations including:
//! 
//! - Dynamic type inference for data from delimiter-separated files
//! - In-memory data storage with column mapping
//! - Table operations (select, project, update, delete)
//! - Table joins (cross joins and inner joins via WHERE conditions)
//! - Column resolution with qualified names (table.column)

use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;

use anyhow::Result;
use serde::{Deserialize, Serialize};

/// Represents a reference to a column, which can be qualified with a table name
/// 
/// This structure is used for handling column references in SQL queries,
/// particularly for supporting table-qualified column names (e.g., "table.column")
/// which are essential for resolving column names in JOINs and multi-table queries.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnRef {
    /// Optional table name qualifier
    /// When present, it specifies the table to which the column belongs
    pub table_name: Option<String>,
    
    /// Column name
    /// The actual name of the column being referenced
    pub column_name: String,
}

impl ColumnRef {
    // Removed unused methods
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

/// Implementation of equality comparison for Value
/// 
/// This implementation allows comparison between different types with appropriate
/// type coercion, such as comparing integers with floating point numbers.
/// Other type combinations are considered not equal, following SQL comparison rules.
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

// Implement the Eq trait for Value
// This is necessary for HashMap keys
impl Eq for Value {}

// Implement the Hash trait for Value
// This is necessary for HashMap keys
impl std::hash::Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Value::Null => {
                // Hash a special value for Null
                0_i32.hash(state);
            },
            Value::Integer(i) => {
                // Hash the integer value
                1_i32.hash(state);
                i.hash(state);
            },
            Value::Float(f) => {
                // Convert float to bits for hashing to avoid NaN issues
                2_i32.hash(state);
                f.to_bits().hash(state);
            },
            Value::String(s) => {
                // Hash the string value
                3_i32.hash(state);
                s.hash(state);
            },
            Value::Boolean(b) => {
                // Hash the boolean value
                4_i32.hash(state);
                b.hash(state);
            },
        }
    }
}

/// Implementation of ordering comparison for Value
///
/// This implementation allows ordering comparison between different types with appropriate
/// type coercion, following SQL comparison rules:
/// - NULL values are considered less than any non-NULL value
/// - Numbers (Integer and Float) can be compared with each other
/// - Strings are compared lexicographically
/// - Booleans compare false < true
/// - Different types follow a precedence order: NULL < Boolean < Number < String
impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        use std::cmp::Ordering;
        
        match (self, other) {
            // NULL handling: NULL is less than anything but equal to NULL
            (Value::Null, Value::Null) => Some(Ordering::Equal),
            (Value::Null, _) => Some(Ordering::Less),
            (_, Value::Null) => Some(Ordering::Greater),
            
            // Same types comparison
            (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::String(a), Value::String(b)) => a.partial_cmp(b),
            (Value::Boolean(a), Value::Boolean(b)) => a.partial_cmp(b),
            
            // Mixed number types
            (Value::Integer(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
            (Value::Float(a), Value::Integer(b)) => a.partial_cmp(&(*b as f64)),
            
            // Different types follow precedence order:
            // Boolean < Number < String
            (Value::Boolean(_), Value::Integer(_) | Value::Float(_) | Value::String(_)) => 
                Some(Ordering::Less),
            (Value::Integer(_) | Value::Float(_), Value::String(_)) => 
                Some(Ordering::Less),
            (Value::String(_), Value::Boolean(_) | Value::Integer(_) | Value::Float(_)) => 
                Some(Ordering::Greater),
            (Value::Integer(_) | Value::Float(_), Value::Boolean(_)) => 
                Some(Ordering::Greater),
        }
    }
}

/// Implementation of string formatting for Value
/// 
/// This implementation provides human-readable string representations of all value types.
/// It ensures values are properly displayed when printing tables or generating output in delimited format.
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

/// Implementation of string conversion to Value with automatic type inference
///
/// This implementation enables automatic type detection when loading data from delimiter-separated files.
/// It attempts to parse the string value in the following order:
/// 1. As an integer (i64)
/// 2. As a floating point number (f64)
/// 3. As a boolean (recognizing various common boolean representations)
/// 4. Empty strings are converted to NULL values
/// 5. Any other content is stored as a string
///
/// This type inference approach allows for efficient data storage and comparisons
/// without requiring explicit type declarations in the input files.
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

/// Sort direction for a column in ORDER BY clause
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SortDirection {
    /// Sort in ascending order (default)
    Ascending,
    /// Sort in descending order
    Descending,
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



    /// Get the columns of the table
    /// 
    /// Returns a slice containing all column names in the table. 
    /// The column names maintain their original order as specified when
    /// the table was created or loaded from a file.
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Get the column count
    /// 
    /// Returns the number of columns in the table. This is useful for
    /// validation when adding rows or performing operations that need to
    /// check column bounds.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Get the rows of the table
    /// 
    /// Returns a slice containing all rows in the table. Each row is a vector
    /// of Value enums representing the cell values. This provides read-only
    /// access to the table data for processing or querying.
    pub fn rows(&self) -> &[Row] {
        &self.rows
    }
    
    /// Get the name of the table
    ///
    /// Returns the name of the table as a string slice.
    /// This is useful for operations that need to access the table's name
    /// such as logging, error messages, or generating SQL output.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the row count
    /// 
    /// Returns the number of rows in the table. This is useful for
    /// determining the size of the result set or for validation.
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }



    /// Add a row to the table
    ///
    /// Adds a new row (vector of values) to the table, verifying that the
    /// number of columns in the row matches the table definition. This operation
    /// marks the table as modified, indicating it should be written back to disk
    /// if changes are being saved.
    ///
    /// # Arguments
    /// * `row` - Vector of values to add as a new row
    ///
    /// # Returns
    /// * `Ok(())` if the row was successfully added
    /// * `Err` if the row doesn't match the table schema
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
    /// 
    /// Returns the path to the original file from which this table was loaded,
    /// if applicable. This is used when writing changes back to disk.
    ///
    /// # Returns
    /// * `Some(PathBuf)` containing the source file path
    /// * `None` if the table wasn't loaded from a file
    pub fn source_file(&self) -> Option<&PathBuf> {
        self.source_file.as_ref()
    }

    /// Get the index of a column by name
    /// 
    /// Looks up a column by name and returns its index in the table.
    /// This is essential for implementing SQL operations that reference
    /// columns by name rather than position.
    ///
    /// # Arguments
    /// * `name` - The name of the column to look up
    ///
    /// # Returns
    /// * `Some(usize)` with the column index if found
    /// * `None` if no column with that name exists
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.column_map.get(name).copied()
    }

    /// Print the table to stdout
    /// 
    /// Formats and prints the table contents to standard output in comma-delimited format.
    /// This is used for displaying query results to the user.
    /// 
    /// # Returns
    /// * `Ok(())` if the table was successfully printed
    /// * `Err` if there was an error writing to stdout
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
    /// 
    /// Filters the table rows based on a provided predicate function.
    /// This is the core implementation of SQL WHERE clause functionality.
    /// 
    /// # Arguments
    /// * `predicate` - A function that takes a row reference and returns a boolean
    ///                 indicating whether the row should be included in the result
    /// 
    /// # Returns
    /// * A new table containing only the rows that match the predicate
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
    /// 
    /// Projects the table to include only the columns specified by their indices.
    /// This is the core implementation of the SQL SELECT column list functionality,
    /// allowing queries to specify which columns should be included in the result.
    /// 
    /// # Arguments
    /// * `column_indices` - Array of column indices to include in the result table
    /// 
    /// # Returns
    /// * `Ok(Table)` containing only the specified columns from the original table
    /// * `Err` if any column index is out of bounds
    /// Create a new table with only specified columns and optional aliases
    /// 
    /// Projects the table to include only the columns specified by their indices,
    /// applying any aliases provided.
    /// 
    /// # Arguments
    /// * `column_specs` - Array of column indices and optional aliases to include in the result table
    /// 
    /// # Returns
    /// * `Ok(Table)` containing only the specified columns from the original table with aliases applied
    /// * `Err` if any column index is out of bounds
    pub fn project_with_aliases(&self, column_specs: &[(usize, Option<String>)]) -> SqawkResult<Self> {
        // Validate column indices
        for &(idx, _) in column_specs {
            if idx >= self.columns.len() {
                return Err(SqawkError::ColumnNotFound(format!(
                    "Column index {} out of bounds",
                    idx
                )));
            }
        }

        // Create new column list with aliases where specified
        let columns: Vec<String> = column_specs
            .iter()
            .map(|&(idx, ref alias)| {
                if let Some(alias_name) = alias {
                    alias_name.clone()
                } else {
                    self.columns[idx].clone()
                }
            })
            .collect();

        let mut result = Table::new(&self.name, columns, self.source_file.clone());

        // Project rows
        for row in &self.rows {
            let projected_row: Vec<Value> =
                column_specs.iter().map(|&(idx, _)| row[idx].clone()).collect();

            result.add_row(projected_row)?;
        }

        Ok(result)
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
    /// Perform a cross join between two tables
    ///
    /// This method implements the Cartesian product of two tables, combining every row from
    /// the left table with every row from the right table.
    ///
    /// # Arguments
    /// * `right` - The right-hand table to join with
    ///
    /// # Returns
    /// * A new table containing the cross join result
    pub fn cross_join(&self, right: &Self) -> SqawkResult<Self> {
        // Create result columns with proper prefixes
        let columns = self.create_joined_columns(right);
        
        // Create a new table to hold the join result
        let mut result = Table::new(
            "join_result", 
            columns, 
            None
        );
        
        // Fill with cross-joined rows
        self.fill_cross_joined_rows(right, &mut result)?;
        
        Ok(result)
    }
    
    /// Perform an inner join with another table based on a join condition
    ///
    /// This function creates a new table containing rows from both tables where
    /// the join condition is satisfied. The join condition is evaluated for each
    /// pair of rows, and only matching pairs are included in the result.
    ///
    /// # Arguments
    /// * `right` - The right-hand table for the join
    /// * `join_condition` - A function that takes a row from the combined table
    ///                     and returns whether it satisfies the join condition
    ///
    /// # Returns
    /// * A new table containing the inner join result
    /// Perform an INNER JOIN between two tables with a custom condition
    ///
    /// This method implements the SQL INNER JOIN operation, which combines rows from
    /// two tables that satisfy a join condition. The implementation follows a
    /// two-step approach: first creating a cross join (Cartesian product), then
    /// filtering the combined rows based on the provided condition.
    ///
    /// # Arguments
    /// * `right` - The right-hand table to join with
    /// * `join_condition` - A closure that evaluates whether a combined row should be included
    ///   The closure receives:
    ///   - A combined row from both tables
    ///   - A reference to the combined table (for column lookups)
    ///   The closure returns a boolean indicating whether the row satisfies the join condition
    ///
    /// # Returns
    /// * A new table containing only the rows that satisfy the join condition
    /// * `Err` if there was an error evaluating the condition or adding rows
    ///
    /// # Usage Example
    /// 
    /// This method is typically used to implement SQL's INNER JOIN operation
    /// with an ON condition. For example, implementing:
    /// 
    /// SELECT * FROM employees INNER JOIN departments 
    /// ON employees.dept_id = departments.id
    /// 
    /// The implementation first finds the column indexes for the join keys,
    /// then compares the values in those columns for each row combination.
    pub fn inner_join<F>(&self, right: &Self, join_condition: F) -> SqawkResult<Self> 
    where 
        F: Fn(&[Value], &Self) -> SqawkResult<bool>
    {
        // Step 1: Create the output columns structure - this must be done before creating
        // the result table to ensure columns from both tables are properly qualified
        let columns = self.create_joined_columns(right);
        
        // Step 2: Create a new table to hold the join result
        let name = format!("{}_inner_join", self.name());
        let mut result = Table::new(
            &name, 
            columns, 
            None
        );
        
        // Step 3: First create the cross join (Cartesian product) to evaluate conditions against
        // This creates every possible combination of rows from both tables
        let combined_tables = self.cross_join(right)?;
        
        // Step 4: Filter the cross join result based on the join condition
        // This is effectively the ON clause in SQL's "INNER JOIN ... ON" syntax
        for row in combined_tables.rows().iter() {
            // Evaluate the condition for this row, which comes from the closure
            // provided by the SQL executor based on the ON condition
            if join_condition(row, &combined_tables)? {
                // Add matching rows to the result table
                result.add_row(row.clone())?;
            }
        }
        
        Ok(result)
    }
    
    /// Create column names for a joined table
    ///
    /// This function creates a list of qualified column names by prefixing
    /// each column name with its table name if it doesn't already have a prefix.
    ///
    /// # Arguments
    /// * `right` - The right-hand table for the join
    ///
    /// # Returns
    /// * A vector of qualified column names
    fn create_joined_columns(&self, right: &Self) -> Vec<String> {
        let mut columns = Vec::new();
        
        // Add columns from left table (self) with prefixes
        self.add_prefixed_columns(&mut columns);
        
        // Add columns from right table with prefixes
        right.add_prefixed_columns(&mut columns);
        
        columns
    }
    
    /// Add columns with table name prefixes to a column list
    ///
    /// This function adds column names to a list, prefixing them with
    /// the table name if they don't already have a prefix.
    ///
    /// # Arguments
    /// * `columns` - The column list to add to
    fn add_prefixed_columns(&self, columns: &mut Vec<String>) {
        for col in self.columns() {
            // If the column already has a table prefix, keep it as is
            // Otherwise, add the table name prefix
            if col.contains('.') {
                columns.push(col.clone());
            } else {
                columns.push(format!("{}.{}", self.name, col));
            }
        }
    }
    
    /// Fill a table with cross-joined rows
    ///
    /// This function creates rows for a cross join by combining each row
    /// from the left table with each row from the right table.
    ///
    /// # Arguments
    /// * `right` - The right-hand table for the join
    /// * `result` - The result table to fill with rows
    ///
    /// # Returns
    /// * Ok(()) if all rows were successfully added
    /// * Err if there was an error adding a row
    fn fill_cross_joined_rows(&self, right: &Self, result: &mut Self) -> SqawkResult<()> {
        // For CROSS JOIN, we include every combination of rows
        for left_row in self.rows() {
            for right_row in right.rows() {
                // Create combined row
                let new_row = self.combine_rows(left_row, right_row, right.column_count());
                
                // Add the combined row to the result table
                result.add_row(new_row)?;
            }
        }
        
        Ok(())
    }
    
    /// Combine two rows into a single row
    ///
    /// This function combines a row from the left table with a row
    /// from the right table to create a joined row.
    ///
    /// # Arguments
    /// * `left_row` - A row from the left table
    /// * `right_row` - A row from the right table
    /// * `right_column_count` - The number of columns in the right table
    ///
    /// # Returns
    /// * A new combined row
    fn combine_rows(&self, left_row: &[Value], right_row: &[Value], right_column_count: usize) -> Vec<Value> {
        let mut new_row = Vec::with_capacity(self.column_count() + right_column_count);
        
        // Add values from left row
        for i in 0..self.column_count() {
            new_row.push(left_row.get(i).unwrap_or(&Value::Null).clone());
        }
        
        // Add values from right row
        for i in 0..right_column_count {
            new_row.push(right_row.get(i).unwrap_or(&Value::Null).clone());
        }
        
        new_row
    }
    
    /// Sort the table by one or more columns
    ///
    /// This method implements the ORDER BY functionality for SQL queries.
    /// It takes a list of column indices and their respective sort directions,
    /// then sorts the table rows accordingly. Sort direction can be either
    /// ascending (the default) or descending.
    ///
    /// # Arguments
    /// * `sort_columns` - A vector of tuples containing (column_index, sort_direction)
    ///
    /// # Returns
    /// * A new sorted table if successful
    /// * Error if any column index is invalid
    pub fn sort(&self, sort_columns: Vec<(usize, SortDirection)>) -> SqawkResult<Self> {
        // Validate column indices
        for (col_idx, _) in &sort_columns {
            if *col_idx >= self.column_count() {
                return Err(SqawkError::ColumnNotFound(format!(
                    "Column index {} out of bounds for ORDER BY (table has {} columns)",
                    col_idx,
                    self.column_count()
                )));
            }
        }
        
        // Create a new table with the same structure
        let mut result = Table::new(&self.name, self.columns.clone(), None);
        
        // Clone the rows for sorting
        let mut sorted_rows = self.rows.clone();
        
        // Sort the rows based on the specified columns and directions
        sorted_rows.sort_by(|row_a, row_b| {
            // Compare each sort column in order until a difference is found
            for &(col_idx, direction) in &sort_columns {
                // Compare values using our PartialOrd implementation
                match row_a[col_idx].partial_cmp(&row_b[col_idx]) {
                    Some(ordering) => {
                        // If not equal, return the ordering (possibly reversed for DESC)
                        if ordering != std::cmp::Ordering::Equal {
                            return match direction {
                                SortDirection::Ascending => ordering,
                                SortDirection::Descending => ordering.reverse(),
                            };
                        }
                    }
                    // If values can't be compared (which shouldn't happen with our implementation),
                    // continue to the next column
                    None => continue,
                }
            }
            
            // If all specified columns are equal, maintain stable sort
            std::cmp::Ordering::Equal
        });
        
        // Add the sorted rows to the result table
        for row in sorted_rows {
            result.add_row(row)?;
        }
        
        Ok(result)
    }
}
