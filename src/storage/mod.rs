//! Storage backends for table row data
//!
//! This module provides different storage backends for table rows:
//! - `MemoryStorage`: Traditional in-memory storage with owned data
//! - `MmapStorage`: Memory-mapped file storage for zero-copy access

pub mod memory;
pub mod mmap;

use crate::table::Row;

/// Storage backend for table row data
///
/// This enum wraps different storage implementations, allowing tables
/// to use either in-memory storage or memory-mapped file storage.
/// Column metadata is managed by the Table struct, not the storage.
#[derive(Debug)]
pub enum Storage {
    /// In-memory storage with owned data
    Memory(memory::MemoryStorage),
    /// Memory-mapped file storage (read-only, zero-copy)
    #[allow(dead_code)]
    Mmap(mmap::MmapStorage),
}

impl Storage {
    /// Create a new empty in-memory storage
    pub fn new_memory() -> Self {
        Storage::Memory(memory::MemoryStorage::new())
    }

    /// Create in-memory storage with existing rows
    #[allow(dead_code)]
    pub fn with_rows(rows: Vec<Row>) -> Self {
        Storage::Memory(memory::MemoryStorage::with_rows(rows))
    }

    /// Get the number of rows
    pub fn row_count(&self) -> usize {
        match self {
            Storage::Memory(m) => m.row_count(),
            Storage::Mmap(m) => m.row_count(),
        }
    }

    /// Get all rows as a slice
    pub fn rows(&self) -> &[Row] {
        match self {
            Storage::Memory(m) => m.rows(),
            Storage::Mmap(m) => m.rows(),
        }
    }

    /// Get a mutable reference to rows (only for mutable backends)
    ///
    /// Returns None for read-only storage backends like Mmap.
    pub fn rows_mut(&mut self) -> Option<&mut Vec<Row>> {
        match self {
            Storage::Memory(m) => Some(m.rows_mut()),
            Storage::Mmap(_) => None, // Mmap is read-only
        }
    }

    /// Add a row to the storage
    ///
    /// Note: For mmap storage, this will panic. Use is_mutable() to check first.
    pub fn push_row(&mut self, row: Row) {
        match self {
            Storage::Memory(m) => m.push_row(row),
            Storage::Mmap(_) => panic!("Cannot add rows to read-only mmap storage"),
        }
    }

    /// Check if this storage is mutable
    #[allow(dead_code)]
    pub fn is_mutable(&self) -> bool {
        match self {
            Storage::Memory(_) => true,
            Storage::Mmap(_) => false,
        }
    }

    /// Replace all rows
    ///
    /// Note: For mmap storage, this will panic. Use is_mutable() to check first,
    /// or call ensure_mutable() before calling this method.
    pub fn replace_rows(&mut self, rows: Vec<Row>) {
        match self {
            Storage::Memory(m) => m.replace_rows(rows),
            Storage::Mmap(_) => panic!("Cannot replace rows in read-only mmap storage"),
        }
    }

    /// Convert this storage to a mutable in-memory storage if needed
    ///
    /// If the storage is already mutable (Memory), this is a no-op.
    /// If the storage is read-only (Mmap), it copies all rows to memory,
    /// converting borrowed strings to owned strings.
    pub fn ensure_mutable(&mut self) {
        use crate::table::Value;
        use std::borrow::Cow;

        match self {
            Storage::Memory(_) => {
                // Already mutable, nothing to do
            }
            Storage::Mmap(mmap) => {
                // Copy all rows to memory, converting borrowed strings to owned
                let rows: Vec<Row> = mmap
                    .rows()
                    .iter()
                    .map(|row| {
                        row.iter()
                            .map(|value| match value {
                                Value::String(cow) => {
                                    // Convert borrowed to owned
                                    Value::String(Cow::Owned(cow.to_string()))
                                }
                                // Other value types are Copy or already owned
                                v => v.clone(),
                            })
                            .collect()
                    })
                    .collect();
                *self = Storage::Memory(memory::MemoryStorage::with_rows(rows));
            }
        }
    }

    /// Get column names from the storage (for mmap only)
    ///
    /// Returns None for memory storage (columns are stored in Table).
    #[allow(dead_code)]
    pub fn columns(&self) -> Option<&[String]> {
        match self {
            Storage::Memory(_) => None,
            Storage::Mmap(m) => Some(m.columns()),
        }
    }
}
