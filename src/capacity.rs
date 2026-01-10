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
