//! In-memory storage backend
//!
//! This module provides traditional in-memory storage for table rows.
//! All data is heap-allocated and owned by the storage.

use crate::table::Row;

/// In-memory storage backend
///
/// Stores table rows in a heap-allocated vector. This is the traditional
/// storage mode where all data is copied into memory during loading.
#[derive(Debug, Clone)]
pub struct MemoryStorage {
    /// Row data
    rows: Vec<Row>,
}

impl MemoryStorage {
    /// Create a new empty memory storage
    pub fn new() -> Self {
        Self { rows: Vec::new() }
    }

    /// Create memory storage with existing rows
    #[allow(dead_code)]
    pub fn with_rows(rows: Vec<Row>) -> Self {
        Self { rows }
    }

    /// Get the number of rows
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Get all rows as a slice
    pub fn rows(&self) -> &[Row] {
        &self.rows
    }

    /// Get mutable reference to rows
    pub fn rows_mut(&mut self) -> &mut Vec<Row> {
        &mut self.rows
    }

    /// Add a row to the storage
    pub fn push_row(&mut self, row: Row) {
        self.rows.push(row);
    }

    /// Replace all rows
    pub fn replace_rows(&mut self, rows: Vec<Row>) {
        self.rows = rows;
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}
