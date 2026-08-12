//! File handling module for sqawk
//!
//! This module provides a unified interface for loading and saving different file formats:
//! - CSV files (comma-separated values)
//! - Delimiter-separated files (tab, colon, etc.)
//!
//! It abstracts away the specific file format details and provides a consistent API
//! for the rest of the application to work with in-memory tables.

use std::path::{Path, PathBuf};

use crate::config::AppConfig;
use crate::csv_handler::CsvHandler;
use crate::database::Database;
use crate::delim_handler::DelimHandler;
use crate::error::{SqawkError, SqawkResult};
use crate::storage::mmap::MmapStorage;
use crate::storage::Storage;
use crate::table::Table;

/// File operand that names standard input rather than a path on disk.
pub const STDIN_SPEC: &str = "-";

/// Table name given to a bare `-` operand.
pub const STDIN_TABLE: &str = "stdin";

/// Enum representing different file formats supported by sqawk
#[derive(Debug, Clone, Copy)]
pub enum FileFormat {
    /// CSV (comma-separated values)
    Csv,
    /// Delimiter-separated values
    Delimited,
}

/// Unified file handler that delegates to specific format handlers
///
/// This struct provides a consistent interface for working with different file formats
/// by delegating to specialized handlers (CSV or delimiter-separated). It acts as a facade
/// that simplifies file operations for the rest of the application.
///
/// Key responsibilities:
/// - Determining the appropriate handler based on file format
/// - Managing the relationship between tables and their source files
/// - Providing access to the database for table operations
/// - Handling table loading and saving with proper format detection
pub struct FileHandler {
    /// Handler for CSV files (comma-separated values)
    csv_handler: CsvHandler,

    /// Handler for delimiter-separated files (tab, pipe, etc.)
    delim_handler: DelimHandler,

    /// Reference to a database object which is the source of truth for tables
    /// Stored as a raw pointer to avoid borrowing limitations
    database: *mut Database,

    /// Application configuration for global settings like verbosity and field separators
    config: AppConfig,

    /// Standard input, spooled to a temporary file.
    ///
    /// Stdin is not seekable and cannot be mmap'd, so a `-` operand is copied
    /// to a temp file and then loaded by the ordinary path. The handle is kept
    /// here because dropping it deletes the file, which an mmap'd table is
    /// still reading from. At most one `-` operand is accepted per run.
    stdin_spool: Option<tempfile::NamedTempFile>,

    /// Name of the table loaded from `-`, if any. Only used to explain why
    /// `--write` cannot save it.
    stdin_table: Option<String>,

    /// Whether `-` may still be read. The REPL clears this: it reads its own
    /// commands from stdin, so a `.load -` would swallow the command stream.
    stdin_available: bool,
}

// Add safety implementation for the raw pointer to Database
unsafe impl Send for FileHandler {}
unsafe impl Sync for FileHandler {}

impl FileHandler {
    /// Create a new FileHandler with application config and database
    ///
    /// # Arguments
    /// * `config` - Application configuration with global settings
    /// * `database` - Mutable reference to the database to use as source of truth
    ///
    /// # Returns
    /// A new FileHandler instance ready to load and manage tables
    pub fn new(config: &AppConfig, database: &mut Database) -> Self {
        FileHandler {
            csv_handler: CsvHandler::new(),
            delim_handler: DelimHandler::new(),
            // SAFETY: The caller must ensure that the database outlives this FileHandler
            database: database as *mut Database,
            config: config.clone(),
            stdin_spool: None,
            stdin_table: None,
            stdin_available: true,
        }
    }

    /// Refuse any further `-` operand.
    ///
    /// Called when entering interactive mode: the REPL reads its commands from
    /// standard input, so `.load -` would consume the rest of the script (or
    /// block until Ctrl-D on a terminal) and the queued commands would never
    /// run. main.rs rejects `-` among the startup operands with a clearer
    /// message; this covers every path that reaches the loader afterwards.
    pub fn disable_stdin(&mut self) {
        self.stdin_available = false;
    }

    /// Copy standard input to a temporary file and return its path.
    ///
    /// The `.csv` suffix is deliberate: it makes format detection treat piped
    /// input as comma-separated, which matches what sqawk itself writes to
    /// stdout, so `sqawk ... | sqawk ... -` round-trips. `-F` still overrides.
    fn spool_stdin(&mut self) -> SqawkResult<PathBuf> {
        if !self.stdin_available {
            return Err(SqawkError::InvalidFileSpec(
                "cannot read standard input here: the REPL reads its own commands from stdin"
                    .to_string(),
            ));
        }
        if self.stdin_spool.is_some() {
            return Err(SqawkError::InvalidFileSpec(
                "standard input can only be read once; '-' was given more than once".to_string(),
            ));
        }

        let mut spool = tempfile::Builder::new()
            .prefix("sqawk-stdin-")
            .suffix(".csv")
            .tempfile()?;
        // No sync_all: this file is deleted at exit, io::copy writes straight
        // to the File with no user-space buffer, and the mmap below reads
        // through the page cache. Forcing durability would just add a full
        // flush of the whole pipe before the first query runs.
        std::io::copy(&mut std::io::stdin().lock(), spool.as_file_mut())?;

        let path = spool.path().to_path_buf();
        self.stdin_spool = Some(spool);
        Ok(path)
    }

    /// Get a mutable reference to the database
    ///
    /// Provides safe access to the database reference stored as a raw pointer.
    /// This design allows the FileHandler to maintain a reference to the Database
    /// while avoiding Rust's borrowing conflicts in complex operations.
    ///
    /// # Safety
    /// Safety is guaranteed because:
    /// - The database pointer is initialized in the constructor and never changes
    /// - The FileHandler's lifetime is tied to the Database through the constructor's contract
    /// - All access to the database happens through this controlled interface
    ///
    /// # Returns
    /// * `&mut Database` - Mutable reference to the database
    fn database_mut(&mut self) -> &mut Database {
        // SAFETY: The caller of `new` ensures the database outlives this FileHandler,
        // and we have exclusive access through `&mut self`
        unsafe { &mut *self.database }
    }

    /// Load a file into an in-memory table and add it to the database
    ///
    /// This method handles the complete file loading process:
    /// 1. Parses the file specification to extract table name and file path
    /// 2. Checks if the table already exists in the database
    /// 3. Automatically detects file format (CSV or delimiter-separated)
    /// 4. Delegates to the appropriate handler based on format
    /// 5. Adds the loaded table to the database
    ///
    /// If a table with the same name already exists in the database, it will be
    /// replaced with the newly loaded table.
    ///
    /// # Arguments
    /// * `file_spec` - File specification in format [table_name=]file_path
    ///
    /// # Returns
    /// * `SqawkResult<Option<(String, String)>>` - Tuple of (table_name, file_path) if successful
    pub fn load_file(&mut self, file_spec: &str) -> SqawkResult<Option<(String, String)>> {
        // Parse file spec to get table name and file path
        let (table_name, file_path) = self.parse_file_spec(file_spec)?;

        // A "-" operand is replaced by the temp file stdin was spooled into,
        // so everything below loads it as an ordinary file. `from_stdin` then
        // detaches that path again, because the temp file is not somewhere
        // --write may ever write back to.
        let from_stdin = file_path == Path::new(STDIN_SPEC);
        let (file_spec, file_path) = if from_stdin {
            let spooled = self.spool_stdin()?;
            (
                format!("{}={}", table_name, spooled.to_string_lossy()),
                spooled,
            )
        } else {
            (file_spec.to_string(), file_path)
        };
        let file_spec = file_spec.as_str();

        let file_path_str = if from_stdin {
            STDIN_SPEC.to_string()
        } else {
            file_path.to_string_lossy().to_string()
        };

        // First, check if the table already exists in the database
        // This could happen if it was defined through CLI table definitions (--tabledef)
        let predefined_columns: Option<Vec<String>>;
        {
            // Create a temporary scope for the database borrow
            let db = self.database_mut();
            predefined_columns = if db.has_table(&table_name) {
                // Get the predefined column names to use instead of auto-detected ones
                db.get_table(&table_name).ok().map(|t| t.columns().to_vec())
            } else {
                None
            };
        }

        // Show verbose output if needed
        let verbose = self.config.verbose();
        if predefined_columns.is_some() && verbose {
            println!(
                "Table '{}' has predefined schema, using those column names",
                table_name
            );
        }

        // Determine the file format based on extension
        let format = self.detect_format(&file_path);

        // Column definitions now come exclusively from Database
        // No need for custom columns logic here anymore

        // Determine delimiter for this file
        // If -F flag is provided, always use it (overrides format detection)
        let delimiter_str = if let Some(sep) = self.config.field_separator() {
            sep
        } else {
            match format {
                FileFormat::Csv => ",".to_string(),
                FileFormat::Delimited => "\t".to_string(),
            }
        };
        let delimiter_byte = delimiter_str.as_bytes()[0];

        // Try to use mmap for on-disk files (zero-copy loading)
        // Only use mmap if the file exists and is a regular file
        let table = if file_path.is_file() {
            // Use memory-mapped storage for zero-copy access
            // Pass predefined columns so mmap treats first row as data when appropriate
            match MmapStorage::open_with_columns(
                &file_path,
                delimiter_byte,
                predefined_columns.clone(),
            ) {
                Ok(mmap_storage) => {
                    if verbose {
                        println!("Using mmap storage for table '{}'", table_name);
                    }
                    let columns = mmap_storage.columns().to_vec();
                    Table::with_storage(
                        &table_name,
                        columns,
                        Some(file_path.clone()),
                        delimiter_str,
                        Storage::Mmap(mmap_storage),
                    )
                }
                Err(e) => {
                    // Fall back to regular loading if mmap fails
                    if verbose {
                        println!(
                            "Mmap failed for '{}', falling back to regular loading: {}",
                            table_name, e
                        );
                    }
                    self.load_file_regular(file_spec, &format, &predefined_columns)?
                }
            }
        } else {
            // Stdin or other non-file source - use regular loading
            self.load_file_regular(file_spec, &format, &predefined_columns)?
        };

        // A source with no header row yields a table with no columns, which
        // the compiler cannot project from -- it indexed result_regs[0] and
        // panicked. Empty input is routine for a pipe, so say what is wrong.
        if table.columns().is_empty() {
            return Err(SqawkError::InvalidFileSpec(format!(
                "'{}' has no header row: expected a first line of column names",
                file_path_str
            )));
        }

        // Drop the spool path so the table has no writeback target: --write
        // must never rewrite the temp file standing in for stdin.
        let mut table = table;
        if from_stdin {
            table.detach_file_path();
            self.stdin_table = Some(table_name.clone());
        }

        // Now that we have the table, we can update the database without borrowing conflicts
        {
            // Create a new scope for database operations
            let db = self.database_mut();

            // Handle existing schema if needed (from --tabledef)
            if predefined_columns.is_some() {
                // Remove the placeholder table created by --tabledef
                // We're replacing it with the actual table containing data
                if verbose {
                    println!(
                        "Replacing predefined schema for '{}' with loaded data",
                        table_name
                    );
                }
                db.remove_table(&table_name);
            }

            // Add the table to the database
            db.add_table(table_name.clone(), table)?;
        }

        Ok(Some((table_name, file_path_str)))
    }

    /// Load a file using the regular (non-mmap) method
    ///
    /// This is the fallback method used when mmap is not available or fails.
    fn load_file_regular(
        &self,
        file_spec: &str,
        format: &FileFormat,
        predefined_columns: &Option<Vec<String>>,
    ) -> SqawkResult<Table> {
        match format {
            FileFormat::Csv => {
                // Load the table from the CSV file
                self.csv_handler
                    .load_csv(file_spec, predefined_columns.clone(), None)
            }
            FileFormat::Delimited => {
                let delimiter = self
                    .config
                    .field_separator()
                    .unwrap_or_else(|| "\t".to_string());
                self.delim_handler
                    .load_delimited(file_spec, &delimiter, predefined_columns.clone())
            }
        }
    }

    /// Parse a file specification into a table name and path
    ///
    /// # Arguments
    /// * `file_spec` - File specification in format [table_name=]file_path
    ///
    /// # Returns
    /// * `SqawkResult<(String, PathBuf)>` - Tuple of (table_name, file_path)
    pub fn parse_file_spec(&self, file_spec: &str) -> SqawkResult<(String, PathBuf)> {
        // Check for explicit table name in format "table_name=file_path"
        if let Some(pos) = file_spec.find('=') {
            let (table_name, file_path) = file_spec.split_at(pos);

            // Strip the '=' from the file path
            let file_path = &file_path[1..];

            // "-" names standard input, which has nothing on disk to check for.
            let path = PathBuf::from(file_path);
            if file_path != STDIN_SPEC && !path.exists() {
                return Err(SqawkError::FileNotFound(file_path.to_string()));
            }

            Ok((table_name.to_string(), path))
        } else if file_spec == STDIN_SPEC {
            // A bare "-" reads standard input into a table called "stdin".
            Ok((STDIN_TABLE.to_string(), PathBuf::from(STDIN_SPEC)))
        } else {
            // No explicit table name, use the file name without extension
            let path = PathBuf::from(file_spec);

            // Validate that the file exists
            if !path.exists() {
                return Err(SqawkError::FileNotFound(file_spec.to_string()));
            }

            // Get file name without extension as table name
            let file_name = path
                .file_name()
                .ok_or_else(|| SqawkError::InvalidFileSpec(file_spec.to_string()))?
                .to_string_lossy();

            // Extract name without extension
            let table_name = if let Some(pos) = file_name.rfind('.') {
                file_name[..pos].to_string()
            } else {
                file_name.to_string()
            };

            Ok((table_name, path))
        }
    }

    /// Get a reference to a table by name
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to retrieve
    ///
    /// # Returns
    /// * `SqawkResult<&Table>` - Reference to the requested table
    pub fn get_table(&self, table_name: &str) -> SqawkResult<&Table> {
        // SAFETY: The caller of `new` ensures the database outlives this FileHandler
        let db = unsafe { &*self.database };
        db.get_table(table_name)
    }

    /// Get all table names
    ///
    /// # Returns
    /// * `Vec<String>` - Vector of table names
    pub fn table_names(&self) -> Vec<String> {
        // SAFETY: The caller of `new` ensures the database outlives this FileHandler
        let db = unsafe { &*self.database };
        db.table_names()
    }

    /// Get the number of tables
    ///
    /// # Returns
    /// * `usize` - Number of tables
    pub fn table_count(&self) -> usize {
        // SAFETY: The caller of `new` ensures the database outlives this FileHandler
        let db = unsafe { &*self.database };
        db.table_count()
    }

    /// Explain why `table_name` has no writeback target.
    fn unwritable_table_error(&self, table_name: &str) -> SqawkError {
        if self.stdin_table.as_deref() == Some(table_name) {
            return SqawkError::InvalidFileSpec(format!(
                "table '{}' was read from standard input and cannot be written back; \
                 redirect the results instead",
                table_name
            ));
        }

        // For tables created with CREATE TABLE, the file path should be set
        SqawkError::NoFilePath(table_name.to_string())
    }

    /// Report whether `table_name` could be written back, without writing it.
    ///
    /// Lets a caller reject the whole writeback up front instead of failing
    /// part-way through and leaving some source files rewritten and others not.
    pub fn check_table_writable(&self, table_name: &str) -> SqawkResult<()> {
        let table = self.get_table(table_name)?;
        if table.file_path().is_none() {
            return Err(self.unwritable_table_error(table_name));
        }
        Ok(())
    }

    /// Check if a table exists
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to check
    ///
    /// # Returns
    /// * `bool` - True if the table exists
    pub fn has_table(&self, table_name: &str) -> bool {
        // SAFETY: The caller of `new` ensures the database outlives this FileHandler
        let db = unsafe { &*self.database };
        db.has_table(table_name)
    }

    /// Save a modified table back to its original file
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to save
    ///
    /// # Returns
    /// * `SqawkResult<()>` - Result of the operation
    pub fn save_table(&self, table_name: &str) -> SqawkResult<()> {
        // Get a reference to the table
        let table = self.get_table(table_name)?;

        if self.config.verbose() {
            eprintln!("In FileHandler::save_table for table '{}'", table_name);

            // Access database directly to check if the table exists there
            let db = unsafe { &*self.database };
            if let Ok(db_table) = db.get_table(table_name) {
                if let Some(path) = db_table.file_path() {
                    eprintln!(
                        "Database has table '{}' with file_path '{:?}'",
                        table_name, path
                    );
                } else {
                    eprintln!("Database has table '{}' but NO file_path", table_name);
                }
            } else {
                eprintln!("Table '{}' not found in database", table_name);
            }
        }

        // Check if the table has an associated file path
        let file_path = match table.file_path() {
            Some(path) => {
                if self.config.verbose() {
                    eprintln!("Table '{}' has file_path '{:?}'", table_name, path);
                }
                path
            }
            None => {
                // Log debugging information
                if self.config.verbose() {
                    eprintln!("Table '{}' has NO file_path", table_name);
                    eprintln!(
                        "  Table details - Name: {}, Columns: {}, Delimiter: '{}'",
                        table.name(),
                        table.columns().join(","),
                        table.delimiter()
                    );
                }

                return Err(self.unwritable_table_error(table_name));
            }
        };

        // For tables created with CREATE TABLE, the file may not exist yet
        // Make sure parent directories exist
        if let Some(parent) = file_path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent).map_err(SqawkError::IoError)?;
            }
        }

        // Determine the format based on the file extension
        let format = self.detect_format(file_path);

        // Get the delimiter from the table
        let delimiter = table.delimiter();

        // Save the table based on the format
        match format {
            FileFormat::Csv => {
                // Delegation to CSV handler (comma is the standard CSV delimiter)
                if delimiter == "," {
                    self.csv_handler.save_csv(table, file_path)?;
                } else {
                    // If delimiter is not a comma, use the delimited handler
                    self.delim_handler
                        .save_delimited(table, file_path, delimiter)?;
                }
            }
            FileFormat::Delimited => {
                // Delegation to delimited handler
                self.delim_handler
                    .save_delimited(table, file_path, delimiter)?;
            }
        }

        Ok(())
    }

    /// Detect file format based on file extension
    ///
    /// This method examines the file extension to determine the appropriate handler:
    /// - `.csv` files are treated as CSV (comma-separated values)
    /// - All other extensions are treated as delimiter-separated files
    /// - Files without extensions default to CSV format
    ///
    /// # Arguments
    /// * `path` - File path to analyze
    ///
    /// # Returns
    /// * `FileFormat` - Detected format (Csv or Delimited) based on file extension
    fn detect_format(&self, path: &Path) -> FileFormat {
        if let Some(ext) = path.extension() {
            match ext.to_string_lossy().to_lowercase().as_str() {
                "csv" => FileFormat::Csv,
                _ => FileFormat::Delimited,
            }
        } else {
            // Default to CSV if no extension
            FileFormat::Csv
        }
    }
}
