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
use crate::csv_handler::{self, ReadOptions};
use crate::database::Database;
use crate::error::{SqawkError, SqawkResult};
use crate::table::{delimiter_byte, ColumnDefinition, FileLayout, Table};

/// File operand that names standard input rather than a path on disk.
pub const STDIN_SPEC: &str = "-";

/// Table name given to a bare `-` operand.
pub const STDIN_TABLE: &str = "stdin";

/// Unified file handler that delegates to specific format handlers
///
/// This struct provides a consistent interface for working with different file formats
/// by delegating to specialized handlers (CSV or delimiter-separated). It acts as a facade
/// that simplifies file operations for the rest of the application.
///
/// Key responsibilities:
/// - Determining the appropriate handler based on file format
/// - Managing the relationship between tables and their source files
/// - Handling table loading and saving with proper format detection
///
/// It holds no tables: the [`Database`] it loads into or saves from is
/// passed to each call.
pub struct FileHandler {
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

impl FileHandler {
    /// Create a new FileHandler with application config
    ///
    /// # Arguments
    /// * `config` - Application configuration with global settings
    ///
    /// # Returns
    /// A new FileHandler instance ready to load and manage tables
    pub fn new(config: &AppConfig) -> Self {
        FileHandler {
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
        std::io::Read::read_to_end(&mut std::io::stdin().lock(), &mut data)
            .map_err(|e| SqawkError::open("standard input", e))?;
        Ok(data)
    }

    /// Load a file into an in-memory table and add it to `db`
    ///
    /// * `file_spec` - `[table_name=]file_path` (see [`parse_table_spec`]);
    ///   `-` is standard input
    ///
    /// The delimiter is -F if given, else tab for `.tsv`/`.tab` files and
    /// comma for everything else. The first line is the header unless
    /// `--no-header` or `--tabledef` supplies the column names. `#` lines
    /// are comments only under `--comments`.
    ///
    /// The table records the file's canonical absolute path, so a later
    /// `.cd` in the REPL does not change which file `.save` writes, and the
    /// same file cannot be loaded twice under different names (writeback
    /// would make one copy overwrite the other).
    ///
    /// # Returns
    /// * `(table_name, file_path)` as given
    pub fn load_file(
        &mut self,
        db: &mut Database,
        file_spec: &str,
    ) -> SqawkResult<Option<(String, String)>> {
        let (table_name, file_path) = parse_table_spec(file_spec)?;

        // A "-" operand reads standard input. It is parsed as CSV (comma,
        // header row) unless -F says otherwise, which matches what sqawk
        // itself writes to stdout, so `sqawk ... | sqawk ... -` round-trips.
        // It has no writeback target: --write must never touch it.
        let from_stdin = file_path == Path::new(STDIN_SPEC);
        let file_path_str = file_path.to_string_lossy().to_string();

        let canonical = if from_stdin {
            None
        } else {
            let real =
                std::fs::canonicalize(&file_path).map_err(|e| SqawkError::open(&file_path, e))?;
            if real.is_dir() {
                return Err(SqawkError::open(
                    &file_path,
                    std::io::Error::new(std::io::ErrorKind::IsADirectory, "Is a directory"),
                ));
            }
            Some(real)
        };

        // A table that already exists is either a --tabledef placeholder,
        // whose column names this file supplies the data for, or a genuine
        // table, in which case two inputs resolve to the same name.
        let predefined_columns: Option<Vec<String>> = match db.get_table(&table_name) {
            Ok(t) if t.is_placeholder() => Some(t.columns().to_vec()),
            Ok(_) => return Err(SqawkError::TableAlreadyExists(table_name)),
            Err(_) => None,
        };
        if let Some(real) = &canonical {
            check_not_loaded(db, real, &file_path_str)?;
        }

        let verbose = self.config.verbose();
        if predefined_columns.is_some() && verbose {
            diag!(
                "Table '{}' has predefined schema, using those column names",
                table_name
            );
        }

        let delimiter_str = self.config.field_separator().unwrap_or_else(|| {
            if !from_stdin && has_tab_extension(&file_path) {
                "\t".to_string()
            } else {
                ",".to_string()
            }
        });
        let opts = ReadOptions {
            delimiter: delimiter_byte(&delimiter_str),
            // --tabledef names the columns; the first line is data.
            header: !self.config.no_header() && predefined_columns.is_none(),
            comments: self.config.comments(),
            columns: predefined_columns.clone(),
        };

        let data = match &canonical {
            None => self.read_stdin()?,
            Some(real) => std::fs::read(real).map_err(|e| SqawkError::open(&file_path, e))?,
        };
        let table = csv_handler::read_table(
            &table_name,
            data,
            canonical,
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

        if predefined_columns.is_some() {
            // Replace the --tabledef placeholder with the loaded table.
            if verbose {
                diag!(
                    "Replacing predefined schema for '{}' with loaded data",
                    table_name
                );
            }
            db.remove_table(&table_name);
        }
        db.add_table(table_name.clone(), table)?;

        Ok(Some((table_name, file_path_str)))
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

    /// Report whether `table_name` can be written back, without writing it.
    ///
    /// Lets a caller reject the whole writeback up front instead of failing
    /// part-way through and leaving some source files rewritten and others not.
    ///
    /// # Returns
    /// * `Ok(true)` - the table has a file and that file can be written
    /// * `Ok(false)` - a scratch table (CREATE TABLE without LOCATION, or
    ///   CREATE TABLE ... AS): it has no file, and writeback skips it
    /// * `Err` - a table read from standard input, or a file that is
    ///   read-only or in a directory that cannot be written
    pub fn check_table_writable(&self, db: &Database, table_name: &str) -> SqawkResult<bool> {
        let table = db.get_table(table_name)?;
        match table.file_path() {
            Some(path) => {
                csv_handler::check_writable(path)?;
                Ok(true)
            }
            None if self.stdin_table.as_deref() == Some(table_name) => {
                Err(self.unwritable_table_error(table_name))
            }
            None => Ok(false),
        }
    }

    /// Save a modified table of `db` back to its original file
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to save
    ///
    /// # Returns
    /// * `SqawkResult<()>` - Result of the operation
    pub fn save_table(&self, db: &Database, table_name: &str) -> SqawkResult<()> {
        let table = db.get_table(table_name)?;

        if self.config.verbose() {
            diag!("In FileHandler::save_table for table '{}'", table_name);
        }

        // Check if the table has an associated file path
        let file_path = match table.file_path() {
            Some(path) => {
                if self.config.verbose() {
                    diag!("Table '{}' has file_path '{:?}'", table_name, path);
                }
                path
            }
            None => {
                // Log debugging information
                if self.config.verbose() {
                    diag!("Table '{}' has NO file_path", table_name);
                    diag!(
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
                std::fs::create_dir_all(parent).map_err(|e| SqawkError::write(file_path, e))?;
            }
        }

        // The rename in save_table would replace even a read-only file.
        csv_handler::check_writable(file_path)?;
        csv_handler::save_table(table, file_path)
    }
}

/// Fail if `real` (a canonical path) is already some table's file: writeback
/// would make one table's copy overwrite the other's. `shown` names the file
/// in the error.
fn check_not_loaded(db: &Database, real: &Path, shown: &str) -> SqawkResult<()> {
    for name in db.table_names() {
        let table = db.get_table(&name)?;
        if table.file_path().map(PathBuf::as_path) == Some(real) && !table.is_placeholder() {
            return Err(SqawkError::InvalidFileSpec(format!(
                "file {} is already loaded as table {}",
                shown, name
            )));
        }
    }
    Ok(())
}

/// How `CREATE TABLE ... LOCATION` reads its file (the `WITH` options).
#[derive(Debug, Clone)]
pub struct LocationOptions {
    /// `WITH (DELIMITER=...)`; else tab for `.tsv` / `.tab`, comma otherwise
    pub delimiter: Option<String>,
    /// The file's first line is a header (`WITH (HEADER=false)` clears it)
    pub header: bool,
    /// `#` lines are comments (`WITH (COMMENTS=true)`)
    pub comments: bool,
}

/// The table `CREATE TABLE name (schema) LOCATION 'path'` creates.
///
/// The declared columns are the schema of the file, as for a Hive external
/// table. When the file exists it is loaded through the normal loader under
/// the declared names and types: its first line is a header unless
/// `options.header` is false, and a header must have one field per declared
/// column; it is skipped as data and written back as read. When it does not
/// exist the table starts empty, and `--write` creates the file (with a
/// header line of the declared names unless `options.header` is false).
///
/// A relative path resolves against the current directory; the table
/// records the absolute path (canonical when the file exists), so a later
/// `.cd` in the REPL does not change which file is written. A file already
/// loaded as another table is an error, as is a directory.
///
/// # Returns
/// * The table, and whether its file already existed (loading it is not a
///   modification; a new file is created on `--write`)
pub fn load_location(
    db: &Database,
    table_name: &str,
    path: &str,
    schema: Vec<ColumnDefinition>,
    options: &LocationOptions,
) -> SqawkResult<(Table, bool)> {
    let given = Path::new(path);
    let delimiter = options.delimiter.clone().unwrap_or_else(|| {
        if has_tab_extension(given) {
            "\t".to_string()
        } else {
            ",".to_string()
        }
    });
    let existing = match std::fs::metadata(given) {
        Ok(meta) if meta.is_dir() => {
            return Err(SqawkError::open(
                given,
                std::io::Error::new(std::io::ErrorKind::IsADirectory, "Is a directory"),
            ))
        }
        Ok(_) => true,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
        Err(e) => return Err(SqawkError::open(given, e)),
    };
    let real = if existing {
        std::fs::canonicalize(given).map_err(|e| SqawkError::open(given, e))?
    } else {
        absolute_path(given).map_err(|e| SqawkError::open(given, e))?
    };
    check_not_loaded(db, &real, path)?;

    if !existing {
        let mut table = Table::new_with_schema(table_name, schema, Some(real), Some(delimiter));
        table.set_file_layout(FileLayout {
            has_header: options.header,
            ..FileLayout::default()
        });
        return Ok((table, false));
    }

    let opts = ReadOptions {
        delimiter: delimiter_byte(&delimiter),
        header: options.header,
        comments: options.comments,
        columns: Some(schema.iter().map(|c| c.name.clone()).collect()),
    };
    let data = std::fs::read(&real).map_err(|e| SqawkError::open(given, e))?;
    let mut table = csv_handler::read_table(table_name, data, Some(real), delimiter, &opts, path)?;
    table.set_declared_columns(schema);
    Ok((table, true))
}

/// `path` made absolute against the current directory, without resolving
/// symlinks in the file name (the file need not exist). An existing parent
/// directory is canonicalized, so two spellings of one new file compare
/// equal.
fn absolute_path(path: &Path) -> std::io::Result<PathBuf> {
    let abs = std::path::absolute(path)?;
    if let (Some(parent), Some(name)) = (abs.parent(), abs.file_name()) {
        if let Ok(real_parent) = std::fs::canonicalize(parent) {
            return Ok(real_parent.join(name));
        }
    }
    Ok(abs)
}

/// Whether `path` names a tab-separated file: extension `.tsv` or `.tab`
/// (any case).
pub(crate) fn has_tab_extension(path: &Path) -> bool {
    path.extension().is_some_and(|ext| {
        let ext = ext.to_string_lossy();
        ext.eq_ignore_ascii_case("tsv") || ext.eq_ignore_ascii_case("tab")
    })
}

/// Whether `name` can be the table-name prefix of `NAME=PATH`: non-empty,
/// letters, digits and `_` only (so never a path).
fn is_table_name(name: &str) -> bool {
    !name.is_empty() && name.chars().all(|c| c.is_alphanumeric() || c == '_')
}

/// Split a file operand `[table_name=]file_path` into a table name and path.
///
/// `NAME=PATH` names the table only when NAME is identifier-like (letters,
/// digits, `_`); otherwise the whole operand is the path, so a file in a
/// directory such as `k=v/` loads as expected. When NAME is identifier-like
/// but PATH does not exist and the whole operand does, the operand is a
/// path too. Without a name, the table is named after the file without its
/// extension; `-` is standard input, as the table `stdin`.
///
/// Errors: an empty name (`=file.csv`), or a file name that leaves an empty
/// table name (`.csv`).
pub fn parse_table_spec(file_spec: &str) -> SqawkResult<(String, PathBuf)> {
    if let Some((name, path)) = file_spec.split_once('=') {
        if name.is_empty() {
            return Err(SqawkError::InvalidFileSpec(format!(
                "'{}': empty table name before '='",
                file_spec
            )));
        }
        if is_table_name(name)
            && (path == STDIN_SPEC || Path::new(path).exists() || !Path::new(file_spec).exists())
        {
            if path.is_empty() {
                return Err(SqawkError::InvalidFileSpec(format!(
                    "'{}': empty file path after '='",
                    file_spec
                )));
            }
            return Ok((name.to_string(), PathBuf::from(path)));
        }
    }
    if file_spec == STDIN_SPEC {
        return Ok((STDIN_TABLE.to_string(), PathBuf::from(STDIN_SPEC)));
    }
    let path = PathBuf::from(file_spec);
    let file_name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let table_name = match file_name.rfind('.') {
        Some(pos) => file_name[..pos].to_string(),
        None => file_name,
    };
    if table_name.is_empty() {
        return Err(SqawkError::InvalidFileSpec(format!(
            "'{}': cannot derive a table name from the file name; use NAME={}",
            file_spec, file_spec
        )));
    }
    Ok((table_name, path))
}
