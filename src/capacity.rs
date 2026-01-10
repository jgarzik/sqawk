//! Capacity hints for pre-allocating collections
//!
//! These constants define initial capacities for various collections
//! to reduce reallocation overhead. Values are based on typical usage patterns.

// =============================================================================
// Row and Column Capacities
// =============================================================================

/// Default capacity for row vectors (number of fields per row).
/// Most CSV/TSV files have fewer than 64 columns.
pub const DEFAULT_ROW_CAPACITY: usize = 16;

/// Default capacity for column definition vectors.
pub const DEFAULT_COLUMN_CAPACITY: usize = 16;

// =============================================================================
// VM Engine Capacities
// =============================================================================

/// Default capacity for VM register vectors.
/// Most programs use fewer than 64 registers.
pub const DEFAULT_REGISTER_CAPACITY: usize = 32;

/// Default capacity for cursor HashMap.
/// Most queries use 1-4 cursors.
pub const DEFAULT_CURSOR_CAPACITY: usize = 4;

/// Default capacity for sorter HashMap.
/// Most queries have 0-2 sorters.
pub const DEFAULT_SORTER_CAPACITY: usize = 2;

/// Default capacity for accumulator HashMap.
/// Most aggregate queries have 1-8 accumulators.
pub const DEFAULT_ACCUMULATOR_CAPACITY: usize = 4;

/// Default capacity for result rows vector.
/// Pre-allocate for small result sets; grows as needed.
pub const DEFAULT_RESULT_CAPACITY: usize = 64;

/// Default capacity for pending modifications vector.
pub const DEFAULT_MODIFICATIONS_CAPACITY: usize = 4;

// =============================================================================
// Compiler Capacities
// =============================================================================

/// Default capacity for instruction vectors in programs.
pub const DEFAULT_INSTRUCTION_CAPACITY: usize = 64;

// =============================================================================
// Table/Database Capacities
// =============================================================================

/// Default capacity for table HashMap in database.
/// Most sessions work with 1-8 tables.
pub const DEFAULT_TABLE_CAPACITY: usize = 8;

// =============================================================================
// Sorter Capacities
// =============================================================================

/// Default capacity for sorter row vectors.
/// Grows dynamically, but start with reasonable size.
pub const DEFAULT_SORTER_ROWS_CAPACITY: usize = 256;

/// Default capacity for sort key vectors.
pub const DEFAULT_SORT_KEYS_CAPACITY: usize = 4;

// =============================================================================
// File-based Estimation
// =============================================================================

/// Average bytes per row estimate for CSV files.
/// Used to estimate row count from file size.
pub const ESTIMATED_BYTES_PER_ROW: usize = 128;

/// Minimum row capacity for file-based estimation.
pub const MIN_ESTIMATED_ROW_CAPACITY: usize = 64;

/// Maximum row capacity for file-based estimation.
/// Prevents excessive pre-allocation for very large files.
pub const MAX_ESTIMATED_ROW_CAPACITY: usize = 10_000_000;

/// Estimate row count from file size (for mmap pre-allocation).
/// Returns a capacity hint, not an exact count.
#[inline]
pub fn estimate_row_count(file_size: usize) -> usize {
    let estimate = file_size / ESTIMATED_BYTES_PER_ROW;
    estimate.clamp(MIN_ESTIMATED_ROW_CAPACITY, MAX_ESTIMATED_ROW_CAPACITY)
}

// =============================================================================
// Headerless File Detection
// =============================================================================

/// Check if a line is a comment (starts with #).
#[inline]
pub fn is_comment_line(line: &[u8]) -> bool {
    line.starts_with(b"#")
}

/// Check if a field looks like data rather than a header name.
/// Used to auto-detect headerless files like /etc/passwd.
#[inline]
pub fn is_data_field(field: &str) -> bool {
    field.starts_with('/') ||      // Path
    field == "*" ||                // Password placeholder
    field == "root" ||             // Common username
    field == "nobody" ||           // Common username
    field.parse::<i32>().is_ok() // Numeric ID
}

/// Check if a row of fields looks like data rather than headers.
pub fn is_likely_data_row<S: AsRef<str>>(fields: &[S]) -> bool {
    fields.iter().any(|f| is_data_field(f.as_ref()))
}

/// Generate alphabetical column names (a, b, c, ..., z, aa, ab, ...).
pub fn generate_alpha_columns(count: usize) -> Vec<String> {
    (0..count)
        .map(|i| {
            let mut name = String::new();
            let mut n = i;
            loop {
                name.insert(0, (b'a' + (n % 26) as u8) as char);
                n /= 26;
                if n == 0 {
                    break;
                }
                n -= 1;
            }
            name
        })
        .collect()
}
