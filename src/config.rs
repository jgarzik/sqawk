//! Configuration module for sqawk
//!
//! This module provides a centralized configuration structure for the application.
//! It handles global settings that are passed down through the application rather
//! than using global state or passing individual settings.

/// Application configuration
///
/// This struct encapsulates all global configuration settings for the application.
/// It is created at startup and passed to components that need access to configuration.
/// This approach avoids global mutable state and makes dependencies explicit.
#[derive(Debug, Clone)]
pub struct AppConfig {
    /// Whether to show verbose output
    verbose: bool,

    /// Custom field separator for tables
    field_separator: Option<String>,

    /// Custom column definitions for tables
    /// Format: "table_name:col1,col2,..."
    table_definitions: Vec<String>,

    /// Whether to write changes back to files
    write_changes: bool,

    /// Treat the first line of every input file as data (`--no-header`)
    no_header: bool,

    /// Skip lines starting with `#` in input files (`--comments`)
    comments: bool,

    /// Delimiter between fields of query results (`--output-delimiter`)
    output_delimiter: Option<String>,

    /// Omit the header line of every result set (`--no-output-header`)
    no_output_header: bool,
}

impl AppConfig {
    /// Create a new application configuration
    ///
    /// # Arguments
    /// * `verbose` - Whether to show verbose output
    /// * `field_separator` - Optional field separator character/string from command line
    /// * `table_definitions` - Optional vector of table column definitions
    /// * `write_changes` - Whether to write changes back to files
    pub fn new(
        verbose: bool,
        field_separator: Option<String>,
        table_definitions: Vec<String>,
        write_changes: bool,
    ) -> Self {
        Self {
            verbose,
            field_separator,
            table_definitions,
            write_changes,
            no_header: false,
            comments: false,
            output_delimiter: None,
            no_output_header: false,
        }
    }

    /// Set the delimiter for query results (`None`: follow -F, else comma)
    pub fn with_output_delimiter(mut self, delimiter: Option<String>) -> Self {
        self.output_delimiter = delimiter;
        self
    }

    /// The delimiter for query results: `--output-delimiter`, else the -F
    /// separator, else a comma.
    pub fn output_delimiter(&self) -> String {
        self.output_delimiter
            .clone()
            .or_else(|| self.field_separator.clone())
            .unwrap_or_else(|| ",".to_string())
    }

    /// Set whether result sets are printed without their header line
    pub fn with_no_output_header(mut self, no_output_header: bool) -> Self {
        self.no_output_header = no_output_header;
        self
    }

    /// Whether result sets are printed without their header line
    pub fn no_output_header(&self) -> bool {
        self.no_output_header
    }

    /// Set whether input files have no header row
    pub fn with_no_header(mut self, no_header: bool) -> Self {
        self.no_header = no_header;
        self
    }

    /// Whether input files have no header row
    pub fn no_header(&self) -> bool {
        self.no_header
    }

    /// Set whether lines starting with `#` are comments
    pub fn with_comments(mut self, comments: bool) -> Self {
        self.comments = comments;
        self
    }

    /// Whether lines starting with `#` are comments
    pub fn comments(&self) -> bool {
        self.comments
    }

    /// Get the verbose flag
    pub fn verbose(&self) -> bool {
        self.verbose
    }

    /// Get the field separator
    pub fn field_separator(&self) -> Option<String> {
        self.field_separator.clone()
    }

    /// Get the table definitions
    pub fn table_definitions(&self) -> &[String] {
        &self.table_definitions
    }

    /// Get whether to write changes
    pub fn write_changes(&self) -> bool {
        self.write_changes
    }

    /// Set whether to write changes
    pub fn set_write_changes(&mut self, write: bool) {
        self.write_changes = write;
    }
}
