//! Error handling for sqawk
//!
//! This module defines custom error types for the sqawk application.
//! It provides a comprehensive error handling system that categorizes
//! different failure modes, supports error propagation, and supplies
//! helpful error messages to users.
//!
//! Every error's `Display` is one complete, self-contained line: the CLI
//! prints it as `sqawk: <message>`. No variant exposes an inner error through
//! `source()`, because its message is already part of the line; a caller
//! that walked the chain would print the cause twice.

use std::error::Error;
use std::fmt;
use std::path::Path;

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

    /// An I/O error on a named file: `<action> '<path>': <reason>`, e.g.
    /// `cannot open 'x.csv': Permission denied`
    FileError {
        action: &'static str,
        path: String,
        error: std::io::Error,
    },

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

    /// Error when a table doesn't have an associated file path
    NoFilePath(String),

    /// Error when a referenced column doesn't exist in a table
    ColumnNotFound(String),

    /// Error for invalid file=table specifications
    InvalidFileSpec(String),

    /// Error for SQL that sqawk parses but does not implement
    UnsupportedSqlFeature(String),

    /// Error for semantically invalid SQL queries
    InvalidSqlQuery(String),

    /// Error in VM execution
    VmError(String),
}

impl SqawkError {
    /// An I/O error opening (reading) the file at `path`.
    pub fn open(path: impl AsRef<Path>, error: std::io::Error) -> Self {
        SqawkError::FileError {
            action: "cannot open",
            path: path.as_ref().display().to_string(),
            error,
        }
    }

    /// An I/O error writing the file at `path`.
    pub fn write(path: impl AsRef<Path>, error: std::io::Error) -> Self {
        SqawkError::FileError {
            action: "cannot write",
            path: path.as_ref().display().to_string(),
            error,
        }
    }
}

/// The reason part of an I/O error, without the `(os error N)` suffix the
/// standard library appends, and without the `at path "..."` suffix the
/// tempfile crate appends (which names an internal temporary file).
pub fn io_reason(e: &std::io::Error) -> String {
    let mut s = e.to_string();
    if let Some(i) = s.find(" at path \"") {
        s.truncate(i);
    }
    if let Some(i) = s.find(" (os error ") {
        s.truncate(i);
    }
    s
}

impl fmt::Display for SqawkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SqawkError::IoError(e) => write!(f, "{}", io_reason(e)),
            SqawkError::FileError {
                action,
                path,
                error,
            } => write!(f, "{} '{}': {}", action, path, io_reason(error)),
            SqawkError::CsvError(e) => write!(f, "{}", e),
            SqawkError::CsvParseError { file, line, error } => {
                write!(f, "{}: line {}: {}", file, line, error)
            }
            SqawkError::SqlParseError(e) => {
                use sqlparser::parser::ParserError;
                match e {
                    ParserError::TokenizerError(s) | ParserError::ParserError(s) => {
                        write!(f, "{}", s)
                    }
                    ParserError::RecursionLimitExceeded => write!(f, "recursion limit exceeded"),
                }
            }
            SqawkError::TableNotFound(name) => write!(f, "no such table: {}", name),
            SqawkError::TableAlreadyExists(name) => write!(f, "table {} already exists", name),
            SqawkError::NoFilePath(name) => {
                write!(
                    f,
                    "table '{}' has no associated file path \
                     (CREATE TABLE ... LOCATION 'file.csv' gives it one)",
                    name
                )
            }
            SqawkError::ColumnNotFound(name) => write!(f, "no such column: {}", name),
            SqawkError::InvalidFileSpec(msg)
            | SqawkError::UnsupportedSqlFeature(msg)
            | SqawkError::InvalidSqlQuery(msg)
            | SqawkError::VmError(msg) => write!(f, "{}", msg),
        }
    }
}

impl Error for SqawkError {}

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
        std::mem::discriminant(self) == std::mem::discriminant(other)
    }
}

/// Result type alias for operations that can produce a SqawkError
///
/// This type alias simplifies function signatures and error handling throughout the codebase.
/// It represents either a successful result of type `T` or a `SqawkError`.
pub type SqawkResult<T> = std::result::Result<T, SqawkError>;

#[cfg(test)]
mod tests {
    use super::*;

    /// The platform's "permission denied" OS error. Raw error numbers differ
    /// by platform: 13 is EACCES on Unix but ERROR_INVALID_DATA on Windows,
    /// where access denied is 5.
    #[cfg(unix)]
    const DENIED: i32 = 13;
    #[cfg(windows)]
    const DENIED: i32 = 5;

    /// The OS's own text for `DENIED` without the "(os error N)" suffix. The
    /// wording comes from the OS (and differs between Windows and Wine), so
    /// it is read rather than spelled out.
    fn denied_text() -> String {
        let shown = std::io::Error::from_raw_os_error(DENIED).to_string();
        let suffix = format!(" (os error {})", DENIED);
        let text = shown
            .strip_suffix(&suffix)
            .expect("io::Error display ends in (os error N)");
        text.to_string()
    }

    #[test]
    fn io_reason_strips_os_error_and_temp_path() {
        let e = std::io::Error::from_raw_os_error(DENIED);
        assert_eq!(io_reason(&e), denied_text());
        // glibc's wording; Miri supplies its own lower-case text.
        #[cfg(all(unix, not(miri)))]
        assert_eq!(io_reason(&e), "Permission denied");
        let e = std::io::Error::other("Permission denied (os error 13) at path \"/d/.tmpAb12\"");
        assert_eq!(io_reason(&e), "Permission denied");
    }

    #[test]
    fn file_error_names_file_once() {
        let e = SqawkError::open("np.csv", std::io::Error::from_raw_os_error(DENIED));
        assert_eq!(
            e.to_string(),
            format!("cannot open 'np.csv': {}", denied_text())
        );
    }
}
