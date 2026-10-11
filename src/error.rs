//! Error handling for sqawk
//!
//! This module defines custom error types for the sqawk application.
//! It provides a comprehensive error handling system that categorizes
//! different failure modes, supports error propagation, and supplies
//! helpful error messages to users.

use std::error::Error;
use std::fmt;

/// SqawkError represents all possible errors that can occur in the sqawk application
///
/// This enum provides a comprehensive set of error types that can occur during:
/// - File I/O operations
/// - File parsing and handling
/// - SQL query parsing
/// - SQL query execution
/// - Table and column operations
///
/// Each variant includes descriptive error messages to help users understand
/// and troubleshoot problems.
#[derive(Debug)]
pub enum SqawkError {
    /// Error during file system operations (reading/writing files)
    IoError(std::io::Error),

    /// Error while parsing or processing delimited file data
    CsvError(csv::Error),

    /// Enhanced CSV parsing error with file location information
    CsvParseError {
        file: String,
        line: usize,
        error: String,
    },

    /// Error during SQL query parsing with sqlparser
    SqlParseError(sqlparser::parser::ParserError),

    /// Error when a referenced table doesn't exist
    TableNotFound(String),

    /// Error when trying to create a table that already exists
    TableAlreadyExists(String),

    /// Error when a file doesn't exist
    FileNotFound(String),

    /// Error when a table doesn't have an associated file path
    NoFilePath(String),

    /// Error when a referenced column doesn't exist in a table
    ColumnNotFound(String),

    /// Error for invalid file=table specifications
    InvalidFileSpec(String),

    /// Error for SQL features that aren't implemented yet
    UnsupportedSqlFeature(String),

    /// Error for semantically invalid SQL queries
    InvalidSqlQuery(String),

    /// Error in VM execution
    VmError(String),
}

impl fmt::Display for SqawkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SqawkError::IoError(e) => write!(f, "I/O error: {}", e),
            SqawkError::CsvError(e) => write!(f, "File parsing error: {}", e),
            SqawkError::CsvParseError { file, line, error } => {
                write!(f, "CSV parse error in {} at line {}: {}", file, line, error)
            }
            SqawkError::SqlParseError(e) => write!(f, "SQL parsing error: {}", e),
            SqawkError::TableNotFound(name) => write!(f, "no such table: {}", name),
            SqawkError::TableAlreadyExists(name) => write!(f, "Table '{}' already exists", name),
            SqawkError::FileNotFound(path) => write!(f, "File not found: {}", path),
            SqawkError::NoFilePath(name) => {
                write!(f, "Table '{}' has no associated file path", name)
            }
            SqawkError::ColumnNotFound(name) => write!(f, "no such column: {}", name),
            SqawkError::InvalidFileSpec(spec) => write!(f, "Invalid file specification: {}", spec),
            SqawkError::UnsupportedSqlFeature(feature) => {
                write!(f, "Unsupported SQL feature: {}", feature)
            }
            SqawkError::InvalidSqlQuery(msg) => write!(f, "Invalid SQL query: {}", msg),
            SqawkError::VmError(msg) => write!(f, "VM execution error: {}", msg),
        }
    }
}

impl Error for SqawkError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            SqawkError::IoError(e) => Some(e),
            SqawkError::CsvError(e) => Some(e),
            SqawkError::SqlParseError(e) => Some(e),
            _ => None,
        }
    }
}

// From implementations for automatic error conversion (replaces #[from])

impl From<std::io::Error> for SqawkError {
    fn from(err: std::io::Error) -> Self {
        SqawkError::IoError(err)
    }
}

impl From<csv::Error> for SqawkError {
    fn from(err: csv::Error) -> Self {
        SqawkError::CsvError(err)
    }
}

impl From<sqlparser::parser::ParserError> for SqawkError {
    fn from(err: sqlparser::parser::ParserError) -> Self {
        SqawkError::SqlParseError(err)
    }
}

// Custom implementation of PartialEq for SqawkError
// This implementation only compares the variant names, not their content
// This is useful for testing, where we want to check if an error is of the right type
// but don't care about the exact error message
impl PartialEq for SqawkError {
    fn eq(&self, other: &Self) -> bool {
        // Match on self and other to check if they are the same variant
        match (self, other) {
            (SqawkError::IoError(_), SqawkError::IoError(_)) => true,
            (SqawkError::CsvError(_), SqawkError::CsvError(_)) => true,
            (SqawkError::CsvParseError { .. }, SqawkError::CsvParseError { .. }) => true,
            (SqawkError::SqlParseError(_), SqawkError::SqlParseError(_)) => true,
            (SqawkError::TableNotFound(_), SqawkError::TableNotFound(_)) => true,
            (SqawkError::TableAlreadyExists(_), SqawkError::TableAlreadyExists(_)) => true,
            (SqawkError::FileNotFound(_), SqawkError::FileNotFound(_)) => true,
            (SqawkError::NoFilePath(_), SqawkError::NoFilePath(_)) => true,
            (SqawkError::ColumnNotFound(_), SqawkError::ColumnNotFound(_)) => true,
            (SqawkError::InvalidFileSpec(_), SqawkError::InvalidFileSpec(_)) => true,
            (SqawkError::UnsupportedSqlFeature(_), SqawkError::UnsupportedSqlFeature(_)) => true,
            (SqawkError::InvalidSqlQuery(_), SqawkError::InvalidSqlQuery(_)) => true,
            (SqawkError::VmError(_), SqawkError::VmError(_)) => true,
            // If variants are different, they are not equal
            _ => false,
        }
    }
}

/// Result type alias for operations that can produce a SqawkError
///
/// This type alias simplifies function signatures and error handling throughout the codebase.
/// It represents either a successful result of type `T` or a `SqawkError`.
pub type SqawkResult<T> = std::result::Result<T, SqawkError>;
