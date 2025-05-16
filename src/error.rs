//! Error handling for sqawk
//!
//! This module defines custom error types for the sqawk application.
//! It provides a comprehensive error handling system that categorizes
//! different failure modes, supports error propagation, and supplies
//! helpful error messages to users.
//!
//! The module uses thiserror to minimize boilerplate code and create
//! a consistent error handling approach throughout the codebase.

use thiserror::Error;

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
#[derive(Error, Debug)]
pub enum SqawkError {
    /// Error during file system operations (reading/writing files)
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    /// Error while parsing or processing delimited file data
    #[error("File parsing error: {0}")]
    CsvError(#[from] csv::Error),
    
    /// Enhanced CSV parsing error with file location information
    #[error("CSV parse error in {file} at line {line}: {error}")]
    CsvParseError {
        file: String,
        line: usize,
        error: String,
    },

    /// Error during SQL query parsing with sqlparser
    #[error("SQL parsing error: {0}")]
    SqlParseError(#[from] sqlparser::parser::ParserError),

    /// Error when a referenced table doesn't exist
    #[error("Table '{0}' not found")]
    TableNotFound(String),

    /// Error when a referenced column doesn't exist in a table
    #[error("Column '{0}' not found")]
    ColumnNotFound(String),

    /// Error for invalid file=table specifications
    #[error("Invalid file specification: {0}")]
    InvalidFileSpec(String),

    /// Error for SQL features that aren't implemented yet
    #[error("Unsupported SQL feature: {0}")]
    UnsupportedSqlFeature(String),

    /// Error for type mismatches or conversion failures
    #[error("Type error: {0}")]
    TypeError(String),

    /// Error for semantically invalid SQL queries
    #[error("Invalid SQL query: {0}")]
    InvalidSqlQuery(String),

    /// Error for division by zero in arithmetic operations
    #[error("Division by zero")]
    DivideByZero,

    /// Error for invalid function arguments
    #[error("Invalid function arguments: {0}")]
    InvalidFunctionArguments(String),
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
            (SqawkError::ColumnNotFound(_), SqawkError::ColumnNotFound(_)) => true,
            (SqawkError::InvalidFileSpec(_), SqawkError::InvalidFileSpec(_)) => true,
            (SqawkError::UnsupportedSqlFeature(_), SqawkError::UnsupportedSqlFeature(_)) => true,
            (SqawkError::TypeError(_), SqawkError::TypeError(_)) => true,
            (SqawkError::InvalidSqlQuery(_), SqawkError::InvalidSqlQuery(_)) => true,
            (SqawkError::DivideByZero, SqawkError::DivideByZero) => true,
            (SqawkError::InvalidFunctionArguments(_), SqawkError::InvalidFunctionArguments(_)) => {
                true
            }
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
