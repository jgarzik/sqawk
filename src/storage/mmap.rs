//! Memory-mapped storage backend
//!
//! This module provides zero-copy access to CSV files by memory-mapping them.
//! String values reference data directly in the mmap'd region instead of
//! being copied to the heap.

use std::borrow::Cow;
use std::fs::File;
use std::path::Path;

use memmap2::Mmap;

use crate::capacity::{estimate_row_count, DEFAULT_ROW_CAPACITY};
use crate::error::{SqawkError, SqawkResult};
use crate::table::{Row, Value};

/// Memory-mapped storage backend
///
/// This storage backend maps a CSV file directly into memory and parses
/// rows lazily. String values borrow directly from the mmap'd region,
/// avoiding heap allocation.
///
/// # Safety
///
/// This struct uses unsafe code to create `'static` references from the
/// mmap'd region. This is safe because:
/// 1. The Mmap is owned by this struct
/// 2. The rows reference data within the Mmap
/// 3. The struct cannot be mutated after creation
/// 4. When the struct is dropped, both the rows and Mmap are dropped together
#[derive(Debug)]
pub struct MmapStorage {
    /// The memory-mapped file data
    #[allow(dead_code)]
    mmap: Mmap,
    /// Column names from the header row
    columns: Vec<String>,
    /// Parsed row data (borrows from mmap)
    rows: Vec<Row>,
}

#[allow(dead_code)]
impl MmapStorage {
    /// Open a CSV file with memory-mapping
    ///
    /// # Arguments
    /// * `path` - Path to the CSV file
    /// * `delimiter` - Field delimiter (e.g., ',' for CSV, '\t' for TSV)
    ///
    /// # Returns
    /// * `Ok(MmapStorage)` if the file was successfully opened and parsed
    /// * `Err` if the file couldn't be opened or parsed
    pub fn open<P: AsRef<Path>>(path: P, delimiter: u8) -> SqawkResult<Self> {
        let file = File::open(path.as_ref()).map_err(SqawkError::IoError)?;

        // Safety: We're only reading the file, and the Mmap will be kept alive
        // for as long as the MmapStorage exists
        let mmap = unsafe { Mmap::map(&file).map_err(SqawkError::IoError)? };

        // Hint to the kernel for better read-ahead
        #[cfg(unix)]
        {
            let ptr = mmap.as_ptr() as *mut libc::c_void;
            let len = mmap.len();
            unsafe {
                // MADV_SEQUENTIAL: Expect sequential page references
                libc::madvise(ptr, len, libc::MADV_SEQUENTIAL);
                // MADV_WILLNEED: Will need these pages, initiate read-ahead
                libc::madvise(ptr, len, libc::MADV_WILLNEED);
            }
        }

        // Parse the CSV structure
        let (columns, rows) = Self::parse_csv(&mmap, delimiter)?;

        Ok(Self {
            mmap,
            columns,
            rows,
        })
    }

    /// Parse CSV data from memory-mapped region
    ///
    /// # Safety
    /// This function creates Value::String with Cow::Borrowed references that
    /// point into the mmap'd memory. The caller must ensure the returned rows
    /// do not outlive the mmap.
    fn parse_csv(data: &Mmap, delimiter: u8) -> SqawkResult<(Vec<String>, Vec<Row>)> {
        if data.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }

        // Find line boundaries - estimate capacity from file size
        let estimated_lines = estimate_row_count(data.len());
        let mut lines: Vec<&[u8]> = Vec::with_capacity(estimated_lines);
        let mut start = 0;

        for (i, &byte) in data.iter().enumerate() {
            if byte == b'\n' {
                // Handle \r\n line endings
                let end = if i > 0 && data[i - 1] == b'\r' {
                    i - 1
                } else {
                    i
                };
                if end > start {
                    lines.push(&data[start..end]);
                }
                start = i + 1;
            }
        }
        // Handle last line without newline
        if start < data.len() {
            let end = if data[data.len() - 1] == b'\r' {
                data.len() - 1
            } else {
                data.len()
            };
            if end > start {
                lines.push(&data[start..end]);
            }
        }

        if lines.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }

        // Parse header row (first line) - these are owned strings for column names
        let header_line = lines[0];
        let columns: Vec<String> = Self::split_fields(header_line, delimiter)
            .iter()
            .map(|field| {
                // Column names are always owned (we need them as keys)
                String::from_utf8_lossy(field).into_owned()
            })
            .collect();

        // Parse data rows
        let mut rows = Vec::with_capacity(lines.len() - 1);

        for line in lines.iter().skip(1) {
            let fields = Self::split_fields(line, delimiter);
            let mut row = Vec::with_capacity(columns.len());

            for field in fields {
                // Convert field bytes to a Value
                // Safety: We use unsafe to extend the lifetime to 'static
                // This is safe because the mmap outlives the rows (both are in the same struct)
                let value = unsafe { Self::parse_field_borrowed(field) };
                row.push(value);
            }

            // Pad row with nulls if it has fewer fields than headers
            while row.len() < columns.len() {
                row.push(Value::Null);
            }

            rows.push(row);
        }

        Ok((columns, rows))
    }

    /// Split a line into fields by delimiter
    fn split_fields(line: &[u8], delimiter: u8) -> Vec<&[u8]> {
        let mut fields = Vec::with_capacity(DEFAULT_ROW_CAPACITY);
        let mut start = 0;
        let mut in_quotes = false;

        for (i, &byte) in line.iter().enumerate() {
            if byte == b'"' {
                in_quotes = !in_quotes;
            } else if byte == delimiter && !in_quotes {
                fields.push(&line[start..i]);
                start = i + 1;
            }
        }

        // Add the last field
        fields.push(&line[start..]);

        fields
    }

    /// Parse a field as a Value with borrowed string data
    ///
    /// # Safety
    /// The returned Value contains a Cow::Borrowed that references the input bytes.
    /// The caller must ensure the bytes outlive the Value.
    unsafe fn parse_field_borrowed(field: &[u8]) -> Value {
        // Convert to str first
        let s = match std::str::from_utf8(field) {
            Ok(s) => s,
            Err(_) => {
                // If not valid UTF-8, convert lossily and return owned
                return Value::String(Cow::Owned(String::from_utf8_lossy(field).into_owned()));
            }
        };

        // Trim whitespace and quotes
        let trimmed = s.trim();
        let trimmed = if trimmed.starts_with('"') && trimmed.ends_with('"') && trimmed.len() >= 2 {
            &trimmed[1..trimmed.len() - 1]
        } else {
            trimmed
        };

        // Try to parse as integer
        if let Ok(i) = trimmed.parse::<i64>() {
            return Value::Integer(i);
        }

        // Try to parse as float
        if let Ok(f) = trimmed.parse::<f64>() {
            return Value::Float(f);
        }

        // Try to parse as boolean (case-insensitive without allocation)
        if trimmed.is_empty() {
            return Value::Null;
        }
        if trimmed.eq_ignore_ascii_case("true") || trimmed.eq_ignore_ascii_case("yes") {
            return Value::Boolean(true);
        }
        if trimmed.eq_ignore_ascii_case("false") || trimmed.eq_ignore_ascii_case("no") {
            return Value::Boolean(false);
        }

        // Return as borrowed string
        // Safety: We extend the lifetime to 'static. The caller must ensure the
        // data lives long enough.
        let static_str: &'static str = std::mem::transmute(trimmed);
        Value::String(Cow::Borrowed(static_str))
    }

    /// Get the column names
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Get the number of rows
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Get all rows as a slice
    pub fn rows(&self) -> &[Row] {
        &self.rows
    }
}

// MmapStorage is not Clone because the mmap'd references would become invalid
// if the Mmap were dropped independently

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_mmap_storage_basic() {
        // Create a temporary CSV file
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "name,age,city").unwrap();
        writeln!(file, "Alice,30,NYC").unwrap();
        writeln!(file, "Bob,25,LA").unwrap();

        // Open with mmap
        let storage = MmapStorage::open(file.path(), b',').unwrap();

        // Check columns
        assert_eq!(storage.columns(), &["name", "age", "city"]);

        // Check row count
        assert_eq!(storage.row_count(), 2);

        // Check first row values
        let rows = storage.rows();
        assert_eq!(rows[0].len(), 3);

        // String values should be borrowed
        match &rows[0][0] {
            Value::String(cow) => {
                assert!(matches!(cow, Cow::Borrowed(_)));
                assert_eq!(cow.as_ref(), "Alice");
            }
            _ => panic!("Expected string value"),
        }

        // Integer values should be parsed
        assert_eq!(rows[0][1], Value::Integer(30));

        // Another string value
        match &rows[0][2] {
            Value::String(cow) => {
                assert!(matches!(cow, Cow::Borrowed(_)));
                assert_eq!(cow.as_ref(), "NYC");
            }
            _ => panic!("Expected string value"),
        }
    }

    #[test]
    fn test_mmap_storage_empty_file() {
        let file = NamedTempFile::new().unwrap();
        let storage = MmapStorage::open(file.path(), b',').unwrap();

        assert!(storage.columns().is_empty());
        assert_eq!(storage.row_count(), 0);
    }

    #[test]
    fn test_mmap_storage_tsv() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "name\tvalue").unwrap();
        writeln!(file, "test\t42").unwrap();

        let storage = MmapStorage::open(file.path(), b'\t').unwrap();

        assert_eq!(storage.columns(), &["name", "value"]);
        assert_eq!(storage.row_count(), 1);

        let rows = storage.rows();
        assert_eq!(rows[0][1], Value::Integer(42));
    }
}
