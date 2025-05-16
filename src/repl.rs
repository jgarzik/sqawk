use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::fmt;
use std::process::Command;

use crate::error::SqawkError;
use crate::sql_executor::SqlExecutor;

// Define a custom error type for the REPL
#[derive(Debug)]
pub enum ReplError {
    SqlExecutor(anyhow::Error),
    Readline(ReadlineError),
    Io(std::io::Error),
    Sqawk(SqawkError),
}

impl fmt::Display for ReplError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReplError::SqlExecutor(err) => write!(f, "SQL execution error: {}", err),
            ReplError::Readline(err) => write!(f, "Input error: {}", err),
            ReplError::Io(err) => write!(f, "I/O error: {}", err),
            ReplError::Sqawk(err) => write!(f, "Sqawk error: {}", err),
        }
    }
}

impl std::error::Error for ReplError {}

impl From<anyhow::Error> for ReplError {
    fn from(err: anyhow::Error) -> Self {
        ReplError::SqlExecutor(err)
    }
}

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
    /// Unknown command
    Unknown(String),
}

/// REPL interface for interactive SQL entry
pub struct Repl {
    /// SQL executor for running queries
    executor: SqlExecutor,
    /// Rustyline editor for command line editing
    editor: DefaultEditor,
    /// Whether to print verbose output
    _verbose: bool,
    /// Whether to write changes to files
    write: bool,
    /// Whether the REPL is running
    running: bool,
    /// Field separator for delimited files
    _field_separator: Option<String>,
    /// Whether to show number of rows changed by SQL statements
    show_changes: bool,
}

impl Repl {
    /// Create a new REPL
    pub fn new(
        executor: SqlExecutor,
        verbose: bool,
        write: bool,
        field_separator: Option<String>,
    ) -> Self {
        let mut editor = DefaultEditor::new().unwrap_or_else(|err| {
            eprintln!("Warning: Failed to initialize editor: {}", err);
            DefaultEditor::new().expect("Critical error initializing editor")
        });
        let _ = editor.load_history(HISTORY_FILE);

        Self {
            executor,
            editor,
            _verbose: verbose,
            write,
            running: true,
            _field_separator: field_separator,
            show_changes: false, // Default to not showing changes
        }
    }

    /// Run the REPL
    pub fn run(&mut self) -> Result<()> {
        println!("Welcome to Sqawk interactive mode!");
        println!("Enter SQL statements or commands, terminate with ';'.");
        println!("Type .help for available commands.");

        while self.running {
            match self.read_command() {
                Ok(command) => {
                    if let Err(e) = self.execute_command(command) {
                        eprintln!("Error: {}", e);
                    }
                }
                Err(ReadlineError::Interrupted) => {
                    println!("CTRL-C");
                    break;
                }
                Err(ReadlineError::Eof) => {
                    println!("CTRL-D");
                    break;
                }
                Err(err) => {
                    eprintln!("Error: {:?}", err);
                    break;
                }
            }
        }

        self.editor.save_history(HISTORY_FILE).unwrap_or_else(|e| {
            eprintln!("Failed to save history: {}", e);
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
                "columns" => {
                    if parts.len() > 1 {
                        ReplCommand::Schema(Some(parts[1].trim().to_string()))
                    } else {
                        ReplCommand::Unknown("Table name required for .columns command".to_string())
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
            ReplCommand::Print(text) => {
                println!("{}", text);
                Ok(())
            }
            ReplCommand::Version => self.show_version(),
            ReplCommand::Unknown(msg) => {
                eprintln!("{}", msg);
                Ok(())
            }
        }
    }

    /// Execute SQL statement
    fn execute_sql(&mut self, sql: &str) -> Result<()> {
        let result = match self.executor.execute_sql(sql) {
            Ok(result) => result,
            Err(err) => return Err(ReplError::SqlExecutor(err)),
        };

        // Print results
        if let Some(result_set) = result {
            if result_set.rows.is_empty() {
                println!("Query returned no rows");
            } else {
                println!("Query returned {} rows", result_set.rows.len());
                // Print column headers
                println!("{}", result_set.columns.join(","));

                // Print rows
                for row in result_set.rows {
                    println!("{}", row.join(","));
                }
            }
        } else if self.show_changes {
            // For non-SELECT statements that don't return rows (INSERT, UPDATE, DELETE)
            // Try to display the number of affected rows if show_changes is enabled
            if let Ok(affected_rows) = self.executor.get_affected_row_count() {
                if affected_rows > 0 {
                    println!("{} rows affected", affected_rows);
                }
            }
        }

        // Save changes if write mode is enabled
        if self.write {
            let saved_count = match self.executor.save_modified_tables() {
                Ok(count) => count,
                Err(err) => return Err(ReplError::SqlExecutor(err)),
            };

            if saved_count > 0 {
                println!("Changes saved to {} tables", saved_count);
            }
        } else if self.executor.has_modified_tables() {
            println!("Changes not saved: use .write to save changes to files");
        }

        Ok(())
    }

    /// Load a file into a table
    fn load_file(&mut self, file_spec: &str) -> Result<()> {
        let result = match self.executor.load_file(file_spec) {
            Ok(result) => result,
            Err(err) => return Err(ReplError::Sqawk(err)),
        };

        match result {
            Some((table_name, file_path)) => {
                println!("Loaded table '{}' from '{}'", table_name, file_path);
                Ok(())
            }
            None => {
                println!("No table created");
                Ok(())
            }
        }
    }

    /// Show the list of tables, optionally filtered by a pattern
    fn show_tables(&self, pattern: Option<&str>) -> Result<()> {
        let tables = self.executor.table_names();
        if tables.is_empty() {
            println!("No tables loaded");
            return Ok(());
        }

        println!("Tables:");
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
                    println!("  No tables match pattern: {}", pat);
                } else {
                    for table in matching_tables {
                        let modified = if self.executor.is_table_modified(table) {
                            " (modified)"
                        } else {
                            ""
                        };
                        println!("  {}{}", table, modified);
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
                    println!("  {}{}", table, modified);
                }
            }
        }

        Ok(())
    }

    // The show_columns functionality is now handled by show_schema with a specific table name

    /// Show help message
    fn show_help(&self) -> Result<()> {
        println!("Available commands:");
        println!("  .cd DIRECTORY         Change the working directory to DIRECTORY");
        println!(
            "  .changes [on|off]     Show number of rows changed by SQL (currently: {})",
            if self.show_changes { "ON" } else { "OFF" }
        );
        println!("  .columns TABLE        Show columns for TABLE (alias for .schema TABLE)");
        println!("  .exit ?CODE?          Exit the REPL with optional code");
        println!("  .help                 Show this help message");
        println!("  .load [TABLE=]FILE    Load FILE into TABLE");
        println!("  .print STRING...      Print literal STRING");
        println!("  .quit                 Exit the REPL");
        println!("  .schema ?TABLE?       Show schema for a specific table or all tables");
        println!("  .tables ?TABLE?       List names of tables matching LIKE pattern TABLE");
        println!("  .version              Show source, library and compiler versions");
        println!(
            "  .write [on|off]       Toggle writing changes to files (currently: {})",
            if self.write { "ON" } else { "OFF" }
        );
        println!("  SQL_STATEMENT         Execute SQL statement");
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
                    println!("Exit code set to: {}", code);
                }
                Err(_) => {
                    eprintln!("Invalid exit code: {}", code_str);
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
                match self.executor.get_table_columns(name) {
                    Ok(columns) => {
                        println!("CREATE TABLE {} (", name);
                        for (i, column) in columns.iter().enumerate() {
                            // For now, we'll just use TEXT as the type
                            // since we don't have direct type information
                            let data_type = "TEXT";
                            if i < columns.len() - 1 {
                                println!("  {} {},", column, data_type);
                            } else {
                                println!("  {} {}", column, data_type);
                            }
                        }
                        println!(");");
                    }
                    Err(_) => {
                        eprintln!("No such table: {}", name);
                    }
                }
            }
            None => {
                // Show schema for all tables
                for name in self.executor.table_names() {
                    if let Ok(columns) = self.executor.get_table_columns(&name) {
                        println!("CREATE TABLE {} (", name);
                        for (i, column) in columns.iter().enumerate() {
                            // For now, we'll just use TEXT as the type
                            let data_type = "TEXT";
                            if i < columns.len() - 1 {
                                println!("  {} {},", column, data_type);
                            } else {
                                println!("  {} {}", column, data_type);
                            }
                        }
                        println!(");");
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
                println!("Changed directory to {}", dir);
                Ok(())
            }
            Err(e) => {
                eprintln!("Failed to change directory: {}", e);
                Ok(())
            }
        }
    }

    /// Toggle showing number of rows changed by SQL statements
    fn toggle_changes(&mut self, arg: Option<&str>) -> Result<()> {
        match arg {
            Some("on") => {
                self.show_changes = true;
                println!("Changes display enabled");
            }
            Some("off") => {
                self.show_changes = false;
                println!("Changes display disabled");
            }
            _ => {
                // Toggle current state
                self.show_changes = !self.show_changes;
                println!(
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
    fn show_version(&self) -> Result<()> {
        println!("Sqawk version 0.1.1");
        println!("Running on Rust {}", get_rustc_version());
        Ok(())
    }
}

/// Get the Rust compiler version
fn get_rustc_version() -> String {
    match Command::new("rustc").arg("--version").output() {
        Ok(output) => {
            if output.status.success() {
                match String::from_utf8(output.stdout) {
                    Ok(version) => version.trim().to_string(),
                    Err(_) => "unknown (utf8 error)".to_string(),
                }
            } else {
                "unknown (command failed)".to_string()
            }
        }
        Err(_) => "unknown (command not found)".to_string(),
    }
}

impl Repl {
    /// Toggle writing changes to files
    fn toggle_write(&mut self, arg: Option<&str>) -> Result<()> {
        match arg {
            Some("on") => {
                self.write = true;
                println!("Write mode enabled - changes will be saved to files");
            }
            Some("off") => {
                self.write = false;
                println!("Write mode disabled - changes will not be saved to files");
            }
            _ => {
                // Toggle current state
                self.write = !self.write;
                println!(
                    "Write mode {}",
                    if self.write { "enabled" } else { "disabled" }
                );
            }
        }
        Ok(())
    }
}
