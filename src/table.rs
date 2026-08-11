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

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;

use anyhow::Result;

use crate::error::{SqawkError, SqawkResult};
use crate::storage::Storage;

/// Represents a value in a table cell
///
/// This enum provides the possible data types for a cell value in a table.
/// It supports the common SQL data types and allows for type conversions
/// between numeric types (Integer <-> Float) for comparison operations.
///
/// String values use `Cow<'static, str>` to support both:
/// - Owned strings (heap-allocated, from in-memory tables)
/// - Borrowed strings (zero-copy, from memory-mapped files)
#[derive(Debug, Clone)]
pub enum Value {
    /// Represents a NULL or missing value
    Null,
    /// 64-bit signed integer
    Integer(i64),
    /// 64-bit floating point number
    Float(f64),
    /// UTF-8 string (owned or borrowed via Cow)
    String(Cow<'static, str>),
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
            }
            Value::Integer(i) => {
                // Hash the integer value
                1_i32.hash(state);
                i.hash(state);
            }
            Value::Float(f) => {
                // Convert float to bits for hashing to avoid NaN issues
                2_i32.hash(state);
                f.to_bits().hash(state);
            }
            Value::String(s) => {
                // Hash the string value
                3_i32.hash(state);
                s.hash(state);
            }
            Value::Boolean(b) => {
                // Hash the boolean value
                4_i32.hash(state);
                b.hash(state);
            }
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
            (Value::Boolean(_), Value::Integer(_) | Value::Float(_) | Value::String(_)) => {
                Some(Ordering::Less)
            }
            (Value::Integer(_) | Value::Float(_), Value::String(_)) => Some(Ordering::Less),
            (Value::String(_), Value::Boolean(_) | Value::Integer(_) | Value::Float(_)) => {
                Some(Ordering::Greater)
            }
            (Value::Integer(_) | Value::Float(_), Value::Boolean(_)) => Some(Ordering::Greater),
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

        // Default to owned string
        Value::String(Cow::Owned(s.to_string()))
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
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

        // Default to owned string (avoid extra allocation)
        Value::String(Cow::Owned(s))
    }
}

/// Represents a row in a table
pub type Row = Vec<Value>;

// =============================================================================
// Index-Based Result Types (Phase 4A)
// =============================================================================
// These types enable efficient query processing by storing row indices instead
// of cloning entire rows. Materialization only happens at output time.
//
// Note: These types are defined in Phase 4A and will be integrated in Phases 4B-4F.
// The #[allow(dead_code)] attributes will be removed as each type is adopted.

/// Reference to a row within a specific table
///
/// This lightweight struct (16 bytes) replaces cloning entire rows (~100+ bytes).
/// Used for tracking which rows match query conditions.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RowRef {
    /// Index of the table in the source table list (for JOINs)
    pub table_idx: usize,
    /// Index of the row within that table
    pub row_idx: usize,
}

#[allow(dead_code)]
impl RowRef {
    /// Create a new row reference
    pub fn new(table_idx: usize, row_idx: usize) -> Self {
        Self { table_idx, row_idx }
    }

    /// Create a row reference for single-table operations (table_idx = 0)
    pub fn single(row_idx: usize) -> Self {
        Self {
            table_idx: 0,
            row_idx,
        }
    }
}

/// Set of row references for single-table operations
///
/// Instead of `Vec<Vec<Value>>` (clones all row data), this stores only
/// indices into the original table. Memory usage: O(n * 8 bytes) vs O(n * row_size).
#[allow(dead_code)]
#[derive(Debug)]
pub struct RowSet<'a> {
    /// Reference to the source table (borrowed, not owned)
    source: &'a Table,
    /// Indices of selected rows (into source.rows)
    indices: Vec<usize>,
}

#[allow(dead_code)]
impl<'a> RowSet<'a> {
    /// Create a new empty RowSet referencing the given table
    pub fn new(source: &'a Table) -> Self {
        Self {
            source,
            indices: Vec::new(),
        }
    }

    /// Create a RowSet with pre-allocated capacity
    pub fn with_capacity(source: &'a Table, capacity: usize) -> Self {
        Self {
            source,
            indices: Vec::with_capacity(capacity),
        }
    }

    /// Add a row index to the set
    pub fn push(&mut self, row_idx: usize) {
        self.indices.push(row_idx);
    }

    /// Get the number of rows in this set
    pub fn len(&self) -> usize {
        self.indices.len()
    }

    /// Check if the set is empty
    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    /// Get the source table
    pub fn source(&self) -> &'a Table {
        self.source
    }

    /// Get the row indices
    pub fn indices(&self) -> &[usize] {
        &self.indices
    }

    /// Get a value from a specific row and column (no clone)
    pub fn get_value(&self, row_idx: usize, col_idx: usize) -> Option<&Value> {
        let actual_row = *self.indices.get(row_idx)?;
        self.source.rows().get(actual_row)?.get(col_idx)
    }

    /// Materialize a single row (clones values - use sparingly)
    pub fn materialize_row(&self, row_idx: usize) -> Option<Vec<Value>> {
        let actual_row = *self.indices.get(row_idx)?;
        self.source.rows().get(actual_row).cloned()
    }

    /// Iterate over row indices
    pub fn iter(&self) -> impl Iterator<Item = usize> + '_ {
        self.indices.iter().copied()
    }
}

/// Row references for multi-table operations (JOINs)
///
/// For JOIN operations, we need to track which rows from each table
/// form a result row. Option<usize> handles NULL rows in outer joins.
#[allow(dead_code)]
#[derive(Debug)]
pub struct JoinedRowSet<'a> {
    /// References to source tables (borrowed)
    sources: Vec<&'a Table>,
    /// Each entry is row indices from each source table
    /// Option<usize> allows NULL rows for outer joins
    row_pairs: Vec<Vec<Option<usize>>>,
}

#[allow(dead_code)]
impl<'a> JoinedRowSet<'a> {
    /// Create a new JoinedRowSet for two tables
    pub fn new(left: &'a Table, right: &'a Table) -> Self {
        Self {
            sources: vec![left, right],
            row_pairs: Vec::new(),
        }
    }

    /// Create with pre-allocated capacity
    pub fn with_capacity(left: &'a Table, right: &'a Table, capacity: usize) -> Self {
        Self {
            sources: vec![left, right],
            row_pairs: Vec::with_capacity(capacity),
        }
    }

    /// Add a matched row pair (INNER JOIN)
    pub fn add_match(&mut self, left_idx: usize, right_idx: usize) {
        self.row_pairs.push(vec![Some(left_idx), Some(right_idx)]);
    }

    /// Add unmatched left row with NULL right (LEFT JOIN)
    pub fn add_left_only(&mut self, left_idx: usize) {
        self.row_pairs.push(vec![Some(left_idx), None]);
    }

    /// Add unmatched right row with NULL left (RIGHT JOIN)
    pub fn add_right_only(&mut self, right_idx: usize) {
        self.row_pairs.push(vec![None, Some(right_idx)]);
    }

    /// Get the number of result rows
    pub fn len(&self) -> usize {
        self.row_pairs.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.row_pairs.is_empty()
    }

    /// Get value from a joined row (returns &Value or NULL for outer join misses)
    pub fn get_value(&self, row_idx: usize, table_idx: usize, col_idx: usize) -> &Value {
        // Static NULL for outer join misses
        static NULL_VALUE: Value = Value::Null;

        if let Some(row_pair) = self.row_pairs.get(row_idx) {
            if let Some(Some(actual_row)) = row_pair.get(table_idx) {
                if let Some(table) = self.sources.get(table_idx) {
                    if let Some(row) = table.rows().get(*actual_row) {
                        if let Some(value) = row.get(col_idx) {
                            return value;
                        }
                    }
                }
            }
        }
        &NULL_VALUE
    }

    /// Get the source tables
    pub fn sources(&self) -> &[&'a Table] {
        &self.sources
    }

    /// Materialize a single result row (clones values)
    pub fn materialize_row(
        &self,
        row_idx: usize,
        left_cols: usize,
        right_cols: usize,
    ) -> Option<Vec<Value>> {
        // Verify row exists
        let _ = self.row_pairs.get(row_idx)?;
        let mut result = Vec::with_capacity(left_cols + right_cols);

        // Left table columns
        for col_idx in 0..left_cols {
            result.push(self.get_value(row_idx, 0, col_idx).clone());
        }

        // Right table columns
        for col_idx in 0..right_cols {
            result.push(self.get_value(row_idx, 1, col_idx).clone());
        }

        Some(result)
    }
}

/// Unified VM result type - either single-table indices, joined indices, or materialized
///
/// This enum allows the VM to work with index-based results throughout query
/// processing, only materializing to actual values when necessary.
#[allow(dead_code)]
#[derive(Debug)]
pub enum IndexedResult<'a> {
    /// Result from single table scan/filter
    Single(RowSet<'a>),
    /// Result from JOIN operations
    Joined(JoinedRowSet<'a>),
    /// Materialized result (for complex expressions, aggregates, or final output)
    Materialized(Vec<Vec<Value>>),
}

#[allow(dead_code)]
impl<'a> IndexedResult<'a> {
    /// Get the number of result rows
    pub fn len(&self) -> usize {
        match self {
            IndexedResult::Single(rs) => rs.len(),
            IndexedResult::Joined(js) => js.len(),
            IndexedResult::Materialized(rows) => rows.len(),
        }
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Check if this result is already materialized
    pub fn is_materialized(&self) -> bool {
        matches!(self, IndexedResult::Materialized(_))
    }
}

/// Lazy row view - provides access to row values without immediate cloning
///
/// This struct allows iteration over result rows while deferring value
/// cloning until actually needed (e.g., for output).
#[allow(dead_code)]
#[derive(Debug)]
pub struct LazyRow<'a, 'b> {
    /// Reference to the indexed result
    result: &'b IndexedResult<'a>,
    /// Index within the result
    row_idx: usize,
}

#[allow(dead_code)]
impl<'a, 'b> LazyRow<'a, 'b> {
    /// Create a new lazy row view
    pub fn new(result: &'b IndexedResult<'a>, row_idx: usize) -> Self {
        Self { result, row_idx }
    }

    /// Get the row index
    pub fn index(&self) -> usize {
        self.row_idx
    }

    /// Materialize this row to owned values (clones - use when needed for output)
    pub fn materialize(&self) -> Option<Vec<Value>> {
        match self.result {
            IndexedResult::Single(rs) => rs.materialize_row(self.row_idx),
            IndexedResult::Joined(js) => {
                // For joined rows, we need column counts
                let left_cols = js.sources().first().map(|t| t.column_count()).unwrap_or(0);
                let right_cols = js.sources().get(1).map(|t| t.column_count()).unwrap_or(0);
                js.materialize_row(self.row_idx, left_cols, right_cols)
            }
            IndexedResult::Materialized(rows) => rows.get(self.row_idx).cloned(),
        }
    }
}

/// Builder for constructing IndexedResult during VM execution
///
/// This builder accumulates row indices during query processing and
/// produces an IndexedResult at the end.
#[allow(dead_code)]
#[derive(Debug)]
pub struct IndexedResultBuilder<'a> {
    /// Source tables being queried
    sources: Vec<&'a Table>,
    /// Accumulated row indices (single table: one index per row)
    single_indices: Vec<usize>,
    /// Accumulated row pairs (joins: multiple indices per row)
    join_indices: Vec<Vec<Option<usize>>>,
    /// Whether this is a join operation
    is_join: bool,
}

#[allow(dead_code)]
impl<'a> IndexedResultBuilder<'a> {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            sources: Vec::new(),
            single_indices: Vec::new(),
            join_indices: Vec::new(),
            is_join: false,
        }
    }

    /// Add a source table
    pub fn add_source(&mut self, table: &'a Table) {
        self.sources.push(table);
        if self.sources.len() > 1 {
            self.is_join = true;
        }
    }

    /// Add a single row index (for single-table operations)
    pub fn add_row_single(&mut self, row_idx: usize) {
        self.single_indices.push(row_idx);
    }

    /// Add a joined row (for multi-table operations)
    pub fn add_row_joined(&mut self, indices: Vec<Option<usize>>) {
        self.join_indices.push(indices);
    }

    /// Check if any rows have been added
    pub fn is_empty(&self) -> bool {
        self.single_indices.is_empty() && self.join_indices.is_empty()
    }

    /// Get the number of result rows
    pub fn len(&self) -> usize {
        if self.is_join {
            self.join_indices.len()
        } else {
            self.single_indices.len()
        }
    }

    /// Clear accumulated results (for reuse)
    pub fn clear(&mut self) {
        self.single_indices.clear();
        self.join_indices.clear();
    }

    /// Build the final IndexedResult
    pub fn build(self) -> Option<IndexedResult<'a>> {
        if self.sources.is_empty() {
            return None;
        }

        if self.is_join && self.sources.len() >= 2 {
            let joined = JoinedRowSet {
                sources: self.sources,
                row_pairs: self.join_indices,
            };
            Some(IndexedResult::Joined(joined))
        } else if let Some(source) = self.sources.into_iter().next() {
            let row_set = RowSet {
                source,
                indices: self.single_indices,
            };
            Some(IndexedResult::Single(row_set))
        } else {
            None
        }
    }
}

impl<'a> Default for IndexedResultBuilder<'a> {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// End Index-Based Result Types
// =============================================================================

/// Represents a table with a storage backend
#[derive(Debug)]
pub struct Table {
    /// Name of the table
    name: String,

    /// Complete column metadata including name and type
    cols: Vec<Column>,

    /// Map of column names to their indices
    column_map: HashMap<String, usize>,

    /// Row data storage backend (memory or mmap)
    storage: Storage,

    /// File path associated with this table (for loading or saving)
    file_path: Option<PathBuf>,

    /// Whether the table was modified since loading
    modified: bool,

    /// Custom delimiter for this table's file (default is comma)
    delimiter: String,
}

/// Data type for a column in a table schema
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    /// Integer (i64)
    Integer,
    /// Floating point (f64)
    Float,
    /// Text/String
    Text,
    /// Boolean
    Boolean,
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Integer => write!(f, "INTEGER"),
            DataType::Float => write!(f, "REAL"),
            DataType::Text => write!(f, "TEXT"),
            DataType::Boolean => write!(f, "BOOLEAN"),
        }
    }
}

/// Represents a column in a table with name and type information
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    /// Name of the column
    pub name: String,
    /// Data type of the column
    pub data_type: DataType,
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.name, self.data_type)
    }
}

/// Column definition for a table schema (used during table creation)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDefinition {
    /// Name of the column
    pub name: String,
    /// Data type of the column
    pub data_type: DataType,
}

impl Table {
    /// Create a new table with the given name and column names
    ///
    /// This constructor defaults all columns to Text type
    pub fn new(name: &str, column_names: Vec<String>, file_path: Option<PathBuf>) -> Self {
        // Create column_map from column names
        let column_map = column_names
            .iter()
            .enumerate()
            .map(|(i, name)| (name.clone(), i))
            .collect();

        // Create full column metadata objects
        let cols = column_names
            .iter()
            .map(|name| Column {
                name: name.clone(),
                data_type: DataType::Text,
            })
            .collect();

        Table {
            name: name.to_string(),
            cols,
            column_map,
            storage: Storage::new_memory(),
            file_path,
            modified: false,
            delimiter: ",".to_string(), // Default to comma delimiter
        }
    }

    /// Create a new table with the given name, column names, and a specific delimiter
    pub fn new_with_delimiter(
        name: &str,
        column_names: Vec<String>,
        file_path: Option<PathBuf>,
        delimiter: String,
    ) -> Self {
        let mut table = Self::new(name, column_names, file_path);
        table.delimiter = delimiter;
        table
    }

    /// Create a new table with a specific storage backend
    ///
    /// This constructor is used for memory-mapped file storage where the
    /// storage already contains the parsed data.
    pub fn with_storage(
        name: &str,
        column_names: Vec<String>,
        file_path: Option<PathBuf>,
        delimiter: String,
        storage: Storage,
    ) -> Self {
        // Create column_map from column names
        let column_map = column_names
            .iter()
            .enumerate()
            .map(|(i, name)| (name.clone(), i))
            .collect();

        // Create full column metadata objects (default to Text type)
        let cols = column_names
            .iter()
            .map(|name| Column {
                name: name.clone(),
                data_type: DataType::Text,
            })
            .collect();

        Table {
            name: name.to_string(),
            cols,
            column_map,
            storage,
            file_path,
            modified: false,
            delimiter,
        }
    }

    /// Create a new table with a schema
    pub fn new_with_schema(
        name: &str,
        schema: Vec<ColumnDefinition>,
        file_path: Option<PathBuf>,
        delimiter: Option<String>,
    ) -> Self {
        // Extract column names from the schema
        let columns: Vec<String> = schema.iter().map(|col_def| col_def.name.clone()).collect();

        // Create full column metadata objects
        let cols = schema
            .iter()
            .map(|col_def| Column {
                name: col_def.name.clone(),
                data_type: col_def.data_type,
            })
            .collect();

        // Create column_map from column names
        let column_map = columns
            .iter()
            .enumerate()
            .map(|(i, name)| (name.clone(), i))
            .collect();

        Table {
            name: name.to_string(),
            cols,
            column_map,
            storage: Storage::new_memory(),
            file_path,
            modified: true, // Tables created with schema are considered modified
            delimiter: delimiter.unwrap_or_else(|| ",".to_string()),
        }
    }

    /// Get the columns of the table
    ///
    /// Returns a vector containing all column names in the table.
    /// The column names maintain their original order as specified when
    /// the table was created or loaded from a file.
    pub fn columns(&self) -> Vec<String> {
        // Get column names from Column objects
        // This prepares for future removal of the columns field
        self.cols.iter().map(|col| col.name.clone()).collect()
    }

    /// Get all columns with their metadata
    ///
    /// Returns a slice containing all column metadata including names and types.
    pub fn column_metadata(&self) -> &[Column] {
        &self.cols
    }

    /// Get the column count
    ///
    /// Returns the number of columns in the table. This is useful for
    /// validation when adding rows or performing operations that need to
    /// check column bounds.
    pub fn column_count(&self) -> usize {
        self.columns().len()
    }

    /// Get the rows of the table
    ///
    /// Returns a slice containing all rows in the table. Each row is a vector
    /// of Value enums representing the cell values. This provides read-only
    /// access to the table data for processing or querying.
    pub fn rows(&self) -> &[Row] {
        self.storage.rows()
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
        self.storage.row_count()
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
        if row.len() != self.column_count() {
            return Err(SqawkError::InvalidSqlQuery(format!(
                "Row has {} columns, but table '{}' has {} columns",
                row.len(),
                self.name,
                self.column_count()
            )));
        }

        // Ensure storage is mutable (converts mmap to memory if needed)
        self.storage.ensure_mutable();
        self.storage.push_row(row);
        self.modified = true;
        Ok(())
    }

    /// Add a row by cloning values from a slice
    ///
    /// This method allows reusing a row buffer by cloning values from a slice.
    /// It validates that the slice length matches the table column count.
    ///
    /// # Arguments
    /// * `row` - Slice of values to clone and add as a new row
    ///
    /// # Returns
    /// * `Ok(())` if the row was successfully added
    /// * `Err` if the slice length doesn't match the table schema
    pub fn add_row_from_slice(&mut self, row: &[Value]) -> SqawkResult<()> {
        if row.len() != self.column_count() {
            return Err(SqawkError::InvalidSqlQuery(format!(
                "Row has {} columns, but table '{}' has {} columns",
                row.len(),
                self.name,
                self.column_count()
            )));
        }

        // Ensure storage is mutable (converts mmap to memory if needed)
        self.storage.ensure_mutable();
        self.storage.push_row(row.to_vec());
        self.modified = true;
        Ok(())
    }

    /// Get the file path associated with this table
    ///
    /// Returns the path to the file associated with this table,
    /// either as the source file it was loaded from or the destination
    /// file specified in CREATE TABLE. This is used when writing
    /// changes back to disk.
    ///
    /// # Returns
    /// * `Some(PathBuf)` containing the file path
    /// * `None` if the table has no associated file
    pub fn file_path(&self) -> Option<&PathBuf> {
        self.file_path.as_ref()
    }

    /// Get the delimiter for this table
    ///
    /// Returns the delimiter used for this table.
    /// This is used when writing the table to a file.
    ///
    /// # Returns
    /// * A string reference to the delimiter (always defined, defaults to comma)
    pub fn delimiter(&self) -> &String {
        &self.delimiter
    }

    /// Set the delimiter for this table
    ///
    /// # Arguments
    /// * `delimiter` - The new delimiter string to use
    pub fn set_delimiter(&mut self, delimiter: String) {
        self.delimiter = delimiter;
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
        let delim = &self.delimiter;

        // Print header
        let column_names = self.columns();
        for (i, col) in column_names.iter().enumerate() {
            if i > 0 {
                print!("{}", delim);
            }
            print!("{}", col);
        }
        println!();

        // Print rows
        for row in self.rows() {
            for (i, val) in row.iter().enumerate() {
                if i > 0 {
                    print!("{}", delim);
                }
                print!("{}", val);
            }
            println!();
        }

        Ok(())
    }

    /// Replace all rows with a new set
    ///
    /// This method is useful for operations like DELETE that need to replace
    /// the content of the table with a filtered subset of rows.
    ///
    /// # Arguments
    /// * `new_rows` - The new set of rows to replace the existing ones
    pub fn replace_rows(&mut self, new_rows: Vec<Row>) {
        // Ensure storage is mutable (converts mmap to memory if needed)
        self.storage.ensure_mutable();
        self.storage.replace_rows(new_rows);
        self.modified = true;
    }

    /// Replace a single row in place, preserving its position.
    ///
    /// Used by UPDATE. Expressing an update as delete-plus-insert appends the
    /// new row, which reorders the table -- and with --write, the user's file.
    pub fn replace_row(&mut self, row_index: usize, row: Row) -> SqawkResult<()> {
        self.storage.ensure_mutable();
        let rows = self.storage.rows_mut().ok_or_else(|| {
            SqawkError::InvalidSqlQuery("Table storage is not mutable".to_string())
        })?;
        if row_index >= rows.len() {
            return Err(SqawkError::InvalidSqlQuery(format!(
                "Row index {} out of range for table '{}'",
                row_index, self.name
            )));
        }
        rows[row_index] = row;
        self.modified = true;
        Ok(())
    }

    /// Add a column to the table with a specified data type
    #[cfg(test)]
    pub fn add_column(&mut self, name: String, data_type_str: String) {
        let data_type = match data_type_str.to_uppercase().as_str() {
            "INT" | "INTEGER" => DataType::Integer,
            "FLOAT" | "REAL" | "DOUBLE" => DataType::Float,
            "BOOL" | "BOOLEAN" => DataType::Boolean,
            _ => DataType::Text,
        };
        let column = Column {
            name: name.clone(),
            data_type,
        };
        self.cols.push(column);
        let new_index = self.cols.len() - 1;
        self.column_map.insert(name, new_index);
        // Ensure storage is mutable (converts mmap to memory if needed)
        self.storage.ensure_mutable();
        self.modified = true;
    }

    /// Add a new column with a default value for all existing rows
    pub fn add_column_with_default(
        &mut self,
        name: String,
        data_type: DataType,
        default_value: Value,
    ) -> crate::error::SqawkResult<()> {
        let column = Column {
            name: name.clone(),
            data_type,
        };
        self.cols.push(column);
        let new_index = self.cols.len() - 1;
        self.column_map.insert(name, new_index);

        // Ensure storage is mutable (converts mmap to memory if needed)
        self.storage.ensure_mutable();

        // Add default value to all existing rows
        if let Some(rows) = self.storage.rows_mut() {
            for row in rows {
                row.push(default_value.clone());
            }
        }

        self.modified = true;
        Ok(())
    }

    /// Clear all rows from the table (TRUNCATE TABLE)
    pub fn clear_rows(&mut self) -> crate::error::SqawkResult<()> {
        // Ensure storage is mutable (converts mmap to memory if needed)
        self.storage.ensure_mutable();
        self.storage.replace_rows(Vec::new());
        self.modified = true;
        Ok(())
    }

    /// Convert all rows to string representation
    ///
    /// This method converts all table rows to a vector of string vectors,
    /// useful for displaying in the REPL interface.
    ///
    /// # Returns
    /// * Vec<Vec<String>> - All rows converted to strings
    pub fn rows_as_strings(&self) -> Vec<Vec<String>> {
        self.rows()
            .iter()
            .map(|row| row.iter().map(|value| value.to_string()).collect())
            .collect()
    }
}
