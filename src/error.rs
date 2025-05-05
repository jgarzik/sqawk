//! Error handling for sqawk
//!
//! This module defines custom error types for the sqawk application.

use thiserror::Error;

/// SqawkError represents all possible errors that can occur in the sqawk application
#[derive(Error, Debug)]
pub enum SqawkError {
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("CSV error: {0}")]
    CsvError(#[from] csv::Error),

    #[error("SQL parsing error: {0}")]
    SqlParseError(#[from] sqlparser::parser::ParserError),

    #[error("Table '{0}' not found")]
    TableNotFound(String),

    #[error("Column '{0}' not found")]
    ColumnNotFound(String),

    #[error("Invalid file specification: {0}")]
    InvalidFileSpec(String),

    #[error("Unsupported SQL feature: {0}")]
    UnsupportedSqlFeature(String),

    #[error("Type error: {0}")]
    TypeError(String),

    #[error("Invalid SQL query: {0}")]
    InvalidSqlQuery(String),
}

/// Result type alias for SqawkError
pub type SqawkResult<T> = std::result::Result<T, SqawkError>;
