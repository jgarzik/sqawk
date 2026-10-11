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
use crate::csv_handler::{self, HeaderMode, ReadOptions};
use crate::database::Database;
use crate::error::{SqawkError, SqawkResult};
use crate::table::{delimiter_byte, Table};

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
    /// Reference to a database object which is the source of truth for tables
    /// Stored as a raw pointer to avoid borrowing limitations
    database: *mut Database,

    /// Application configuration for global settings like verbosity and field separators
    config: AppConfig,

    /// Whether standard input has already been read as a table. At most one
    /// `-` operand is accepted per run.
    stdin_consumed: bool,

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
            // SAFETY: The caller must ensure that the database outlives this FileHandler
            database: database as *mut Database,
            config: config.clone(),
            stdin_consumed: false,
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

    /// Read all of standard input.
    fn read_stdin(&mut self) -> SqawkResult<Vec<u8>> {
        if !self.stdin_available {
            return Err(SqawkError::InvalidFileSpec(
                "cannot read standard input here: the REPL reads its own commands from stdin"
                    .to_string(),
            ));
        }
        if self.stdin_consumed {
            return Err(SqawkError::InvalidFileSpec(
                "standard input can only be read once; '-' was given more than once".to_string(),
            ));
        }
        self.stdin_consumed = true;
        let mut data = Vec::new();
        std::io::Read::read_to_end(&mut std::io::stdin().lock(), &mut data)?;
        Ok(data)
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
        let (table_name, file_path) = self.parse_file_spec(file_spec)?;

        // A "-" operand reads standard input. It is parsed as CSV (comma,
        // header row) unless -F says otherwise, which matches what sqawk
        // itself writes to stdout, so `sqawk ... | sqawk ... -` round-trips.
        // It has no writeback target: --write must never touch it.
        let from_stdin = file_path == Path::new(STDIN_SPEC);
        let file_path_str = if from_stdin {
            STDIN_SPEC.to_string()
        } else {
            file_path.to_string_lossy().to_string()
        };

        // A table that already exists is either a --tabledef placeholder,
        // whose column names this file supplies the data for, or a genuine
        // table, in which case two inputs resolve to the same name.
        let predefined_columns: Option<Vec<String>> = {
            let db = self.database_mut();
            match db.get_table(&table_name) {
                Ok(t) if t.is_placeholder() => Some(t.columns().to_vec()),
                Ok(_) => return Err(SqawkError::TableAlreadyExists(table_name)),
                Err(_) => None,
            }
        };

        let verbose = self.config.verbose();
        if predefined_columns.is_some() && verbose {
            eprintln!(
                "Table '{}' has predefined schema, using those column names",
                table_name
            );
        }

        // Delimiter: -F if given, else comma for .csv (and stdin and files
        // without an extension), tab for anything else.
        let format = if from_stdin {
            FileFormat::Csv
        } else {
            self.detect_format(&file_path)
        };
        let delimiter_str = self
            .config
            .field_separator()
            .unwrap_or_else(|| match format {
                FileFormat::Csv => ",".to_string(),
                FileFormat::Delimited => "\t".to_string(),
            });

        // Header policy: a .csv file (or stdin) always has a header row, and
        // its '#' lines are data. Other delimited files get the headerless
        // guess (for files like /etc/passwd) and '#' comment lines.
        let csv_rules = from_stdin || Self::has_csv_extension(&file_path);
        let header = if self.config.no_header() {
            HeaderMode::Absent
        } else if csv_rules {
            HeaderMode::Present
        } else {
            HeaderMode::Guess
        };
        let opts = ReadOptions {
            delimiter: delimiter_byte(&delimiter_str),
            header,
            comments: !csv_rules,
            columns: predefined_columns.clone(),
            recover_errors: false,
        };

        let data = if from_stdin {
            self.read_stdin()?
        } else {
            std::fs::read(&file_path)?
        };
        let target = if from_stdin {
            None
        } else {
            Some(file_path.clone())
        };
        let table = csv_handler::read_table(
            &table_name,
            &data,
            target,
            delimiter_str,
            &opts,
            &file_path_str,
        )?;

        // A source with no header row yields a table with no columns, which
        // the compiler cannot project from. Empty input is routine for a
        // pipe, so say what is wrong.
        if table.columns().is_empty() {
            return Err(SqawkError::InvalidFileSpec(format!(
                "'{}' has no header row: expected a first line of column names",
                file_path_str
            )));
        }

        if from_stdin {
            self.stdin_table = Some(table_name.clone());
        }

        let db = self.database_mut();
        if predefined_columns.is_some() {
            // Replace the --tabledef placeholder with the loaded table.
            if verbose {
                eprintln!(
                    "Replacing predefined schema for '{}' with loaded data",
                    table_name
                );
            }
            db.remove_table(&table_name);
        }
        db.add_table(table_name.clone(), table)?;

        Ok(Some((table_name, file_path_str)))
    }

    /// Whether `path` has a `.csv` extension (any case).
    fn has_csv_extension(path: &Path) -> bool {
        path.extension()
            .is_some_and(|ext| ext.to_string_lossy().eq_ignore_ascii_case("csv"))
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
            if !parent.as_os_str().is_empty() && !parent.exists() {
                std::fs::create_dir_all(parent).map_err(SqawkError::IoError)?;
            }
        }

        csv_handler::save_table(table, file_path)
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
