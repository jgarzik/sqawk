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
    
    /// Whether to use the VM execution engine
    use_vm: bool,
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
        use_vm: bool,
    ) -> Self {
        Self {
            verbose,
            field_separator,
            table_definitions,
            write_changes,
            use_vm,
        }
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
    
    /// Get whether to use VM execution engine
    pub fn use_vm(&self) -> bool {
        self.use_vm
    }
    
    /// Set whether to use VM execution engine
    #[allow(dead_code)]  // Mark as intentionally unused
    pub fn set_use_vm(&mut self, use_vm: bool) {
        self.use_vm = use_vm;
    }
}
