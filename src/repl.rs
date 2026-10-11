use rustyline::completion::{Completer, Pair};
use rustyline::config::CompletionType;
use rustyline::error::ReadlineError;
use rustyline::highlight::{CmdKind, Highlighter};
use rustyline::hint::Hinter;
use rustyline::history::DefaultHistory;
use rustyline::validate::{self, Validator};
use rustyline::{Config, Context, Editor, Helper};
use std::borrow::Cow;
use std::fmt;

use crate::config::AppConfig;
use crate::error::SqawkError;
use crate::sql_executor::SqlExecutor;

/// An error from one REPL command. Its `Display` is the bare message; the
/// REPL prints it as `sqawk: <message>`, the CLI's error form.
#[derive(Debug)]
pub enum ReplError {
    Readline(ReadlineError),
    Io(std::io::Error),
    Sqawk(SqawkError),
}

impl fmt::Display for ReplError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReplError::Readline(err) => write!(f, "{}", err),
            ReplError::Io(err) => write!(f, "{}", crate::error::io_reason(err)),
            ReplError::Sqawk(err) => write!(f, "{}", err),
        }
    }
}

impl std::error::Error for ReplError {}

impl From<ReadlineError> for ReplError {
    fn from(err: ReadlineError) -> Self {
        ReplError::Readline(err)
    }
}

impl From<std::io::Error> for ReplError {
    fn from(err: std::io::Error) -> Self {
        ReplError::Io(err)
    }
}

impl From<SqawkError> for ReplError {
    fn from(err: SqawkError) -> Self {
        ReplError::Sqawk(err)
    }
}

pub type Result<T> = std::result::Result<T, ReplError>;

const HISTORY_FILE: &str = ".sqawk_history";

/// Command completer for REPL commands
#[derive(Default)]
struct CommandCompleter {
    /// List of available dot commands for auto-completion
    commands: Vec<String>,
}

impl CommandCompleter {
    /// Create a new command completer with the list of available commands
    fn new() -> Self {
        let commands = vec![
            ".cd", ".changes", ".exit", ".help", ".load", ".print", ".quit", ".save", ".schema",
            ".show", ".stats", ".tables", ".version", ".write",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect();

        Self { commands }
    }
}

// Implement the required traits for Helper
impl Completer for CommandCompleter {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Self::Candidate>)> {
        // Only provide completion for dot commands
        if line.starts_with('.') {
            // Split the input into command and argument parts
            let parts: Vec<&str> = line.splitn(2, ' ').collect();
            let partial_cmd = parts[0];

            // We're completing a command (not an argument)
            let start_pos = 0; // Start of the command
            let candidates: Vec<Pair> = self
                .commands
                .iter()
                .filter(|cmd| cmd.starts_with(partial_cmd))
                .map(|cmd| Pair {
                    display: cmd.clone(),
                    replacement: cmd.clone(),
                })
                .collect();

            Ok((start_pos, candidates))
        } else {
            // No completion for SQL statements for now
            Ok((pos, vec![]))
        }
    }
}

// Implement minimal no-op versions of the other traits required by Helper
impl Hinter for CommandCompleter {
    type Hint = String;

    fn hint(&self, _line: &str, _pos: usize, _ctx: &Context<'_>) -> Option<Self::Hint> {
        None
    }
}

impl Highlighter for CommandCompleter {
    fn highlight<'l>(&self, line: &'l str, _pos: usize) -> Cow<'l, str> {
        Cow::Borrowed(line)
    }

    fn highlight_char(&self, _line: &str, _pos: usize, _kind: CmdKind) -> bool {
        false
    }
}

impl Validator for CommandCompleter {
    fn validate(
        &self,
        ctx: &mut validate::ValidationContext,
    ) -> rustyline::Result<validate::ValidationResult> {
        let input = ctx.input().trim();

        // Empty input is valid (just press enter)
        if input.is_empty() {
            return Ok(validate::ValidationResult::Valid(None));
        }

        // Dot commands are complete on a single line
        if input.starts_with('.') {
            return Ok(validate::ValidationResult::Valid(None));
        }

        // SQL statements must end with semicolon
        if input.ends_with(';') {
            return Ok(validate::ValidationResult::Valid(None));
        }

        // Otherwise, we need more input (multiline SQL)
        Ok(validate::ValidationResult::Incomplete)
    }
}

impl Helper for CommandCompleter {}

/// Commands that can be executed in the REPL
#[derive(Debug)]
enum ReplCommand {
    /// Execute SQL statement
    Sql(String),
    /// Load a file into a table
    Load(String),
    /// Show the list of tables matching an optional pattern
    Tables(Option<String>),
    /// Show the columns of a table or schema
    Schema(Option<String>),
    /// Toggle writing changes to files
    WriteMode(Option<String>),
    /// Show help message
    Help,
    /// Exit the REPL with optional exit code
    Exit(Option<String>),
    /// Change directory
    ChangeDirectory(String),
    /// Toggle showing number of changes
    Changes(Option<String>),
    /// Print a string literal
    Print(String),
    /// Show version information
    Version,
    /// Save changes to modified tables
    Save(Option<String>),
    /// Show current settings and metadata
    Show(Option<String>),
    /// Show statistics or toggle statistics mode
    Stats(Option<String>),
    /// Unknown command
    Unknown(String),
}

/// REPL interface for interactive SQL entry
pub struct Repl<'a> {
    /// SQL executor for running queries
    executor: SqlExecutor<'a>,
    /// Rustyline editor for command line editing
    editor: Editor<CommandCompleter, DefaultHistory>,
    /// Application configuration for global settings
    config: AppConfig,
    /// Whether the REPL is running
    running: bool,
    /// Whether to show number of rows changed by SQL statements
    show_changes: bool,
    /// Whether to show query statistics
    show_stats: bool,
}

impl<'a> Repl<'a> {
    /// Create a new REPL
    pub fn new(executor: SqlExecutor<'a>, app_config: &AppConfig) -> Self {
        // Create rustyline configuration with list-style completion
        let rustyline_config = Config::builder()
            .completion_type(CompletionType::List)
            .build();

        // Create editor with our custom command completer
        let helper = CommandCompleter::new();
        let mut editor = Editor::with_config(rustyline_config).expect("Failed to create editor");

        // Set the helper manually
        editor.set_helper(Some(helper));

        // Load history if available
        let _ = editor.load_history(HISTORY_FILE);

        Self {
            executor,
            editor,
            config: app_config.clone(),
            running: true,
            show_changes: false, // Default to not showing changes
            show_stats: false,   // Default to not showing stats
        }
    }

    /// Run the REPL
    pub fn run(&mut self) -> Result<()> {
        outln!("Welcome to Sqawk interactive mode!");
        outln!("Enter SQL statements or commands, terminate with ';'.");
        outln!("Type .help for available commands.");

        while self.running {
            match self.read_command() {
                Ok(command) => {
                    if let Err(e) = self.execute_command(command) {
                        diag!("sqawk: {}", crate::one_line(&e.to_string()));
                    }
                }
                Err(ReadlineError::Interrupted) => {
                    outln!("CTRL-C");
                    break;
                }
                Err(ReadlineError::Eof) => {
                    outln!("CTRL-D");
                    break;
                }
                Err(err) => {
                    diag!("sqawk: {}", crate::one_line(&err.to_string()));
                    break;
                }
            }
        }

        // Uncommitted work is discarded, as when SQLite closes a connection.
        self.executor.finish();

        self.editor.save_history(HISTORY_FILE).unwrap_or_else(|e| {
            diag!("sqawk: cannot save history: {}", e);
        });

        Ok(())
    }

    /// Read a command from the user
    fn read_command(&mut self) -> rustyline::Result<ReplCommand> {
        let prompt = "sqawk> ";
        let input = self.editor.readline(prompt)?;

        if !input.trim().is_empty() {
            let _ = self.editor.add_history_entry(&input);
        }

        Ok(self.parse_command(&input))
    }

    /// Parse a command from user input
    fn parse_command(&self, input: &str) -> ReplCommand {
        let input = input.trim();

        if let Some(stripped) = input.strip_prefix('.') {
            let parts: Vec<&str> = stripped.splitn(2, ' ').collect();
            let command = parts[0].to_lowercase();

            match command.as_str() {
                "exit" => {
                    if parts.len() > 1 {
                        ReplCommand::Exit(Some(parts[1].trim().to_string()))
                    } else {
                        ReplCommand::Exit(None)
                    }
                }
                "quit" => ReplCommand::Exit(None),
                "tables" => {
                    if parts.len() > 1 {
                        ReplCommand::Tables(Some(parts[1].trim().to_string()))
                    } else {
                        ReplCommand::Tables(None)
                    }
                }
                "schema" => {
                    if parts.len() > 1 {
                        ReplCommand::Schema(Some(parts[1].trim().to_string()))
                    } else {
                        // With no argument, show schema for all tables
                        ReplCommand::Schema(None)
                    }
                }
                "load" => {
                    if parts.len() > 1 {
                        ReplCommand::Load(parts[1].trim().to_string())
                    } else {
                        ReplCommand::Unknown("File path required for .load command".to_string())
                    }
                }
                "write" => {
                    if parts.len() > 1 {
                        ReplCommand::WriteMode(Some(parts[1].trim().to_string()))
                    } else {
                        ReplCommand::WriteMode(None)
                    }
                }
                "cd" => {
                    if parts.len() > 1 {
                        ReplCommand::ChangeDirectory(parts[1].trim().to_string())
                    } else {
                        ReplCommand::Unknown("Directory path required for .cd command".to_string())
                    }
                }
                "changes" => {
                    if parts.len() > 1 {
                        ReplCommand::Changes(Some(parts[1].trim().to_string()))
                    } else {
                        ReplCommand::Changes(None)
                    }
                }
                "print" => {
                    if parts.len() > 1 {
                        ReplCommand::Print(parts[1].to_string())
                    } else {
                        ReplCommand::Print("".to_string()) // Print an empty line
                    }
                }
                "version" => ReplCommand::Version,
                "help" => ReplCommand::Help,
                "save" => {
                    if parts.len() > 1 {
                        ReplCommand::Save(Some(parts[1].trim().to_string()))
                    } else {
                        ReplCommand::Save(None)
                    }
                }
                "show" => {
                    if parts.len() > 1 {
                        ReplCommand::Show(Some(parts[1].trim().to_string()))
                    } else {
                        ReplCommand::Show(None)
                    }
                }
                "stats" => {
                    if parts.len() > 1 {
                        ReplCommand::Stats(Some(parts[1].trim().to_string()))
                    } else {
                        ReplCommand::Stats(None)
                    }
                }
                _ => ReplCommand::Unknown(format!("Unknown command: .{}", command)),
            }
        } else if !input.is_empty() {
            ReplCommand::Sql(input.to_string())
        } else {
            ReplCommand::Unknown("Empty command".to_string())
        }
    }

    /// Execute a command
    fn execute_command(&mut self, command: ReplCommand) -> Result<()> {
        match command {
            ReplCommand::Sql(sql) => self.execute_sql(&sql),
            ReplCommand::Load(file_spec) => self.load_file(&file_spec),
            ReplCommand::Tables(pattern) => self.show_tables(pattern.as_deref()),
            ReplCommand::Schema(table_name) => self.show_schema(table_name.as_deref()),
            ReplCommand::WriteMode(arg) => self.toggle_write(arg.as_deref()),
            ReplCommand::Help => self.show_help(),
            ReplCommand::Exit(code) => self.exit_repl(code.as_deref()),
            ReplCommand::ChangeDirectory(dir) => self.change_directory(&dir),
            ReplCommand::Changes(arg) => self.toggle_changes(arg.as_deref()),
            ReplCommand::Save(table_name) => self.save_tables(table_name.as_deref()),
            ReplCommand::Show(option) => self.show_settings(option.as_deref()),
            ReplCommand::Stats(option) => self.show_stats(option.as_deref()),
            ReplCommand::Print(text) => {
                outln!("{}", text);
                Ok(())
            }
            ReplCommand::Version => self.show_version(),
            ReplCommand::Unknown(msg) => {
                diag!("sqawk: {}", msg);
                Ok(())
            }
        }
    }

    /// Execute SQL statement
    fn execute_sql(&mut self, sql: &str) -> Result<()> {
        // Record start time if statistics are enabled
        let start_time = if self.show_stats {
            Some(std::time::Instant::now())
        } else {
            None
        };

        let result = self.executor.execute_sql(sql)?;

        // Print every statement's result set, each with its own header.
        for result_set in &result {
            if result_set.rows.is_empty() {
                outln!("Query returned no rows");
            } else {
                outln!("Query returned {} rows", result_set.rows.len());
                // Header and rows with the output delimiter, quoted where a
                // field needs it (NULL is already an empty field in a
                // ResultSet).
                let writer = crate::table::RecordWriter {
                    delimiter: crate::table::delimiter_byte(&self.config.output_delimiter()),
                    terminator: b"\n",
                    comments: false,
                };
                let stdout = std::io::stdout();
                let mut out = stdout.lock();
                if !self.config.no_output_header() {
                    let _ = writer.write(&mut out, result_set.columns.iter().map(String::as_str));
                }
                for row in &result_set.rows {
                    let _ = writer.write(&mut out, row.iter().map(String::as_str));
                }
            }
        }

        // The change count is reported independently of whether the line also
        // produced rows. Gating it on "no result set" hid the count for a
        // trailing DML on a line like `SELECT ...; DELETE ...`.
        if self.show_changes && self.executor.last_statement_changed_rows() {
            // For non-SELECT statements that don't return rows (INSERT, UPDATE, DELETE)
            // Try to display the number of affected rows if show_changes is enabled
            // Reported even when zero. With `.changes on` a statement that
            // matched nothing otherwise produced no output at all, leaving it
            // ambiguous whether it had run.
            if let Ok(affected_rows) = self.executor.get_affected_row_count() {
                outln!("{} rows affected", affected_rows);
            }
        }

        // Display query statistics if enabled
        if let Some(start_time) = start_time {
            let execution_time = start_time.elapsed();
            outln!("Run Time: {:.3} ms", execution_time.as_secs_f64() * 1000.0);
        }

        // Save changes if write mode is enabled. Inside a transaction nothing
        // is saved until COMMIT: uncommitted changes never reach a file.
        if !self.executor.in_transaction() {
            if self.config.write_changes() {
                let saved_count = self.executor.save_modified_tables()?;

                if saved_count > 0 {
                    outln!("Changes saved to {} tables", saved_count);
                }
            } else if self.executor.has_modified_tables() {
                outln!("Changes not saved: use .write to save changes to files");
            }
        }

        Ok(())
    }

    /// Load a file into a table
    fn load_file(&mut self, file_spec: &str) -> Result<()> {
        // The file load now needs to pass the field separator
        // Since our SqlExecutor's load_file method has been updated to handle it
        let result = self.executor.load_file(file_spec)?;

        match result {
            Some((table_name, file_path)) => {
                outln!("Loaded table '{}' from '{}'", table_name, file_path);
                Ok(())
            }
            None => {
                outln!("No table created");
                Ok(())
            }
        }
    }

    /// Show the list of tables, optionally filtered by a pattern
    fn show_tables(&self, pattern: Option<&str>) -> Result<()> {
        let tables = self.executor.table_names();
        if tables.is_empty() {
            outln!("No tables loaded");
            return Ok(());
        }

        outln!("Tables:");
        match pattern {
            Some(pat) => {
                // Filter tables matching the pattern (SQL LIKE pattern)
                // Convert SQL LIKE pattern to regex
                let regex_pattern = pat.replace("%", ".*").replace("_", ".");
                let regex = regex::Regex::new(&format!("^{}$", regex_pattern))
                    .unwrap_or_else(|_| regex::Regex::new(".*").unwrap()); // Fallback to match all if regex is invalid

                let matching_tables: Vec<&String> =
                    tables.iter().filter(|name| regex.is_match(name)).collect();

                if matching_tables.is_empty() {
                    outln!("  No tables match pattern: {}", pat);
                } else {
                    for table in matching_tables {
                        let modified = if self.executor.is_table_modified(table) {
                            " (modified)"
                        } else {
                            ""
                        };
                        outln!("  {}{}", table, modified);
                    }
                }
            }
            None => {
                // Show all tables
                for table in tables {
                    let modified = if self.executor.is_table_modified(&table) {
                        " (modified)"
                    } else {
                        ""
                    };
                    outln!("  {}{}", table, modified);
                }
            }
        }

        Ok(())
    }

    // The show_columns functionality is now handled by show_schema with a specific table name

    /// Show help message
    fn show_help(&self) -> Result<()> {
        outln!("Available commands:");
        outln!("  .cd DIRECTORY         Change the working directory to DIRECTORY");
        outln!(
            "  .changes [on|off]     Show number of rows changed by SQL (currently: {})",
            if self.show_changes { "ON" } else { "OFF" }
        );
        outln!("  .exit ?CODE?          Exit the REPL with optional code");
        outln!("  .help                 Show this help message");
        outln!("  .load [TABLE=]FILE    Load FILE into TABLE");
        outln!("  .print STRING...      Print literal STRING");
        outln!("  .quit                 Exit the REPL");
        outln!("  .save ?TABLE?         Save changes to all tables or a specific TABLE");
        outln!("  .schema ?TABLE?       Show schema for a specific table or all tables");
        outln!("  .show ?WHAT?          Show current settings and status information");
        outln!(
            "  .stats [on|off]       Toggle statistics display (currently: {})",
            if self.show_stats { "ON" } else { "OFF" }
        );
        outln!("  .tables ?TABLE?       List names of tables matching LIKE pattern TABLE");
        outln!("  .version              Show source, library and compiler versions");
        outln!(
            "  .write [on|off]       Toggle writing changes to files (currently: {})",
            if self.config.write_changes() {
                "ON"
            } else {
                "OFF"
            }
        );
        outln!("  SQL_STATEMENT         Execute SQL statement");
        Ok(())
    }

    /// Exit the REPL with an optional exit code
    fn exit_repl(&mut self, code: Option<&str>) -> Result<()> {
        self.running = false;

        // If an exit code is provided, we'll just acknowledge it
        // In a real program, this would set the process exit code
        if let Some(code_str) = code {
            match code_str.parse::<i32>() {
                Ok(code) => {
                    outln!("Exit code set to: {}", code);
                }
                Err(_) => {
                    diag!("sqawk: invalid exit code: {}", code_str);
                }
            }
        }

        Ok(())
    }

    /// Display schema information for a table or all tables
    fn show_schema(&self, table_name: Option<&str>) -> Result<()> {
        match table_name {
            Some(name) => {
                // Show schema for specific table
                match self.executor.get_table_column_types(name) {
                    Ok(column_types) => {
                        outln!("CREATE TABLE {} (", name);
                        for (i, (column_name, data_type)) in column_types.iter().enumerate() {
                            if i < column_types.len() - 1 {
                                outln!("  {} {},", column_name, data_type);
                            } else {
                                outln!("  {} {}", column_name, data_type);
                            }
                        }
                        outln!(");");
                    }
                    Err(_) => {
                        diag!("sqawk: no such table: {}", name);
                    }
                }
            }
            None => {
                // Show schema for all tables
                for name in self.executor.table_names() {
                    if let Ok(column_types) = self.executor.get_table_column_types(&name) {
                        outln!("CREATE TABLE {} (", name);
                        for (i, (column_name, data_type)) in column_types.iter().enumerate() {
                            if i < column_types.len() - 1 {
                                outln!("  {} {},", column_name, data_type);
                            } else {
                                outln!("  {} {}", column_name, data_type);
                            }
                        }
                        outln!(");");
                    }
                }
            }
        }
        Ok(())
    }

    /// Change the current working directory
    fn change_directory(&self, dir: &str) -> Result<()> {
        match std::env::set_current_dir(dir) {
            Ok(_) => {
                outln!("Changed directory to {}", dir);
                Ok(())
            }
            Err(e) => {
                diag!(
                    "sqawk: cannot change directory to '{}': {}",
                    dir,
                    crate::error::io_reason(&e)
                );
                Ok(())
            }
        }
    }

    /// Toggle showing number of rows changed by SQL statements
    fn toggle_changes(&mut self, arg: Option<&str>) -> Result<()> {
        match arg {
            Some("on") => {
                self.show_changes = true;
                outln!("Changes display enabled");
            }
            Some("off") => {
                self.show_changes = false;
                outln!("Changes display disabled");
            }
            _ => {
                // Toggle current state
                self.show_changes = !self.show_changes;
                outln!(
                    "Changes display {}",
                    if self.show_changes {
                        "enabled"
                    } else {
                        "disabled"
                    }
                );
            }
        }
        Ok(())
    }

    /// Show version information
    ///
    /// Read from the manifest rather than written out, so it cannot drift.
    /// This reported 0.1.1 for a long time while the crate was at 0.8.0.
    fn show_version(&self) -> Result<()> {
        outln!("Sqawk version {}", env!("CARGO_PKG_VERSION"));
        outln!("sqlparser {}", crate::vm::SQLPARSER_VERSION);
        Ok(())
    }

    /// Save changes to tables
    ///
    /// Explicitly writes changes to disk for all modified tables or a specific table
    /// if one is specified. This is useful when write mode is off but you want to
    /// save specific changes.
    fn save_tables(&mut self, table_name: Option<&str>) -> Result<()> {
        if self.executor.in_transaction() {
            return Err(ReplError::Sqawk(crate::error::SqawkError::InvalidSqlQuery(
                "cannot save within a transaction: COMMIT or ROLLBACK first".to_string(),
            )));
        }
        match table_name {
            Some(name) => {
                // Save a specific table if it exists and is modified
                if !self.executor.table_exists(name) {
                    return Err(ReplError::Sqawk(crate::error::SqawkError::TableNotFound(
                        name.to_string(),
                    )));
                }

                if !self.executor.table_is_modified(name) {
                    outln!("Table '{}' has no changes to save", name);
                    return Ok(());
                }

                self.executor.save_table(name)?;
                outln!("Changes saved to table '{}'", name);
                Ok(())
            }
            None => {
                // Save all modified tables
                if !self.executor.has_modified_tables() {
                    outln!("No modified tables to save");
                    return Ok(());
                }

                let count = self.executor.save_modified_tables()?;
                outln!("Changes saved to {} tables", count);
                Ok(())
            }
        }
    }
}

impl Repl<'_> {
    /// Toggle writing changes to files
    fn toggle_write(&mut self, arg: Option<&str>) -> Result<()> {
        match arg {
            Some("on") => {
                self.config.set_write_changes(true);
                outln!("Write mode enabled - changes will be saved to files");
            }
            Some("off") => {
                self.config.set_write_changes(false);
                outln!("Write mode disabled - changes will not be saved to files");
            }
            _ => {
                // Toggle current state
                let current = self.config.write_changes();
                self.config.set_write_changes(!current);
                outln!(
                    "Write mode {}",
                    if self.config.write_changes() {
                        "enabled"
                    } else {
                        "disabled"
                    }
                );
            }
        }
        Ok(())
    }

    /// Show current settings and configuration
    fn show_settings(&self, option: Option<&str>) -> Result<()> {
        match option {
            Some("tables") => {
                // Show detailed table information
                self.show_tables_metadata()
            }
            _ => {
                // Show general settings
                outln!("Sqawk Settings:");
                outln!(
                    "  Write Mode:   {}",
                    if self.config.write_changes() {
                        "ON"
                    } else {
                        "OFF"
                    }
                );
                outln!(
                    "  Changes Display: {}",
                    if self.show_changes { "ON" } else { "OFF" }
                );
                outln!(
                    "  Statistics:  {}",
                    if self.show_stats { "ON" } else { "OFF" }
                );
                outln!(
                    "  Verbose:     {}",
                    if self.config.verbose() { "ON" } else { "OFF" }
                );

                // Show field separator if defined
                if let Some(sep) = self.config.field_separator() {
                    outln!("  Field Separator: '{}'", sep);
                } else {
                    outln!("  Field Separator: Default (tab for .tsv/.tab, else comma)");
                }

                // Show counts
                let tables = self.executor.table_names();
                let modified_count = tables
                    .iter()
                    .filter(|t| self.executor.is_table_modified(t))
                    .count();

                outln!("  Tables Loaded: {}", tables.len());
                outln!("  Modified Tables: {}", modified_count);

                Ok(())
            }
        }
    }

    /// Show detailed metadata for all tables
    fn show_tables_metadata(&self) -> Result<()> {
        let tables = self.executor.table_names();
        if tables.is_empty() {
            outln!("No tables loaded");
            return Ok(());
        }

        outln!("Table Information:");
        for table_name in tables {
            // Get table metadata
            let is_modified = self.executor.is_table_modified(&table_name);
            let column_count = match self.executor.get_table_columns(&table_name) {
                Ok(cols) => cols.len(),
                Err(_) => 0,
            };

            outln!("  Table: {}", table_name);
            outln!(
                "    Status: {}",
                if is_modified { "MODIFIED" } else { "Unchanged" }
            );
            outln!("    Columns: {}", column_count);
            // We don't have direct access to row count or filename in current implementation
            // These would be good additions to the API in the future

            outln!();
        }

        Ok(())
    }

    /// Show statistics or toggle statistics mode
    fn show_stats(&mut self, option: Option<&str>) -> Result<()> {
        match option {
            Some("on") => {
                self.show_stats = true;
                outln!("Statistics display enabled");
                Ok(())
            }
            Some("off") => {
                self.show_stats = false;
                outln!("Statistics display disabled");
                Ok(())
            }
            None => {
                // Toggle the current state
                self.show_stats = !self.show_stats;
                outln!(
                    "Statistics display {}",
                    if self.show_stats {
                        "enabled"
                    } else {
                        "disabled"
                    }
                );
                Ok(())
            }
            Some(_) => {
                outln!("Unknown option for .stats");
                outln!("Usage: .stats [on|off]");
                Ok(())
            }
        }
    }
}
