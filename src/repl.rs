use rustyline::error::ReadlineError;
use rustyline::{DefaultEditor, Editor, History};
use rustyline::history::DefaultHistory;
use std::io::Write;

use crate::error::{SqawkError, SqawkResult};
use crate::sql_executor::SqlExecutor;

const HISTORY_FILE: &str = ".sqawk_history";

/// Commands that can be executed in the REPL
#[derive(Debug)]
enum Command {
    /// Execute SQL statement
    Sql(String),
    /// Load a file into a table
    Load(String),
    /// Show the list of tables
    Tables,
    /// Show the columns of a table
    Columns(String),
    /// Toggle writing changes to files
    WriteMode(Option<String>),
    /// Show help message
    Help,
    /// Exit the REPL
    Exit,
    /// Unknown command
    Unknown(String),
}

/// REPL interface for interactive SQL entry
pub struct Repl {
    /// SQL executor for running queries
    executor: SqlExecutor,
    /// Rustyline editor for command line editing
    editor: Editor<()>,
    /// Whether to print verbose output
    verbose: bool,
    /// Whether to write changes to files
    write: bool,
    /// Whether the REPL is running
    running: bool,
    /// Field separator for delimited files
    field_separator: Option<String>,
}

impl Repl {
    /// Create a new REPL
    pub fn new(
        executor: SqlExecutor,
        verbose: bool,
        write: bool,
        field_separator: Option<String>,
    ) -> Self {
        let mut editor = Editor::<()>::new();
        let _ = editor.load_history(HISTORY_FILE);
        
        Self {
            executor,
            editor,
            verbose,
            write,
            running: true,
            field_separator,
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
    fn read_command(&mut self) -> rustyline::Result<Command> {
        let prompt = "sqawk> ";
        let input = self.editor.readline(prompt)?;
        
        if !input.trim().is_empty() {
            self.editor.add_history_entry(&input);
        }
        
        Ok(self.parse_command(&input))
    }
    
    /// Parse a command from user input
    fn parse_command(&self, input: &str) -> Command {
        let input = input.trim();
        
        if input.starts_with('.') {
            let parts: Vec<&str> = input[1..].splitn(2, ' ').collect();
            let command = parts[0].to_lowercase();
            
            match command.as_str() {
                "exit" | "quit" => Command::Exit,
                "tables" => Command::Tables,
                "columns" | "schema" => {
                    if parts.len() > 1 {
                        Command::Columns(parts[1].trim().to_string())
                    } else {
                        Command::Unknown("Table name required for .columns command".to_string())
                    }
                },
                "load" => {
                    if parts.len() > 1 {
                        Command::Load(parts[1].trim().to_string())
                    } else {
                        Command::Unknown("File path required for .load command".to_string())
                    }
                },
                "write" => {
                    if parts.len() > 1 {
                        Command::WriteMode(Some(parts[1].trim().to_string()))
                    } else {
                        Command::WriteMode(None)
                    }
                },
                "help" => Command::Help,
                _ => Command::Unknown(format!("Unknown command: .{}", command)),
            }
        } else if !input.is_empty() {
            Command::Sql(input.to_string())
        } else {
            Command::Unknown("Empty command".to_string())
        }
    }
    
    /// Execute a command
    fn execute_command(&mut self, command: Command) -> Result<()> {
        match command {
            Command::Sql(sql) => self.execute_sql(&sql),
            Command::Load(file_spec) => self.load_file(&file_spec),
            Command::Tables => self.show_tables(),
            Command::Columns(table_name) => self.show_columns(&table_name),
            Command::WriteMode(arg) => self.toggle_write(arg.as_deref()),
            Command::Help => self.show_help(),
            Command::Exit => {
                self.running = false;
                Ok(())
            },
            Command::Unknown(msg) => {
                eprintln!("{}", msg);
                Ok(())
            },
        }
    }
    
    /// Execute SQL statement
    fn execute_sql(&mut self, sql: &str) -> Result<()> {
        let result = self.executor.execute_sql(sql)?;
        
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
        }
        
        // Save changes if write mode is enabled
        if self.write {
            let saved_count = self.executor.save_modified_tables()?;
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
        match self.executor.load_file(file_spec)? {
            Some((table_name, file_path)) => {
                println!("Loaded table '{}' from '{}'", table_name, file_path);
                Ok(())
            },
            None => {
                println!("No table created");
                Ok(())
            }
        }
    }
    
    /// Show the list of tables
    fn show_tables(&self) -> Result<()> {
        let tables = self.executor.table_names();
        if tables.is_empty() {
            println!("No tables loaded");
        } else {
            println!("Tables:");
            for table in tables {
                let modified = if self.executor.is_table_modified(&table) {
                    " (modified)"
                } else {
                    ""
                };
                println!("  {}{}", table, modified);
            }
        }
        Ok(())
    }
    
    /// Show the columns of a table
    fn show_columns(&self, table_name: &str) -> Result<()> {
        match self.executor.get_table_columns(table_name) {
            Ok(columns) => {
                println!("Columns for table '{}':", table_name);
                for column in columns {
                    println!("  {}", column);
                }
                Ok(())
            },
            Err(e) => Err(e.into()),
        }
    }
    
    /// Show help message
    fn show_help(&self) -> Result<()> {
        println!("Available commands:");
        println!("  .help                 Show this help message");
        println!("  .exit, .quit          Exit the REPL");
        println!("  .tables               List all tables");
        println!("  .columns TABLE        Show columns for TABLE");
        println!("  .load [TABLE=]FILE    Load FILE into TABLE");
        println!("  .write [on|off]       Toggle writing changes to files (currently: {})", 
                 if self.write { "ON" } else { "OFF" });
        println!("  SQL_STATEMENT         Execute SQL statement");
        Ok(())
    }
    
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
                println!("Write mode {}", if self.write { "enabled" } else { "disabled" });
            }
        }
        Ok(())
    }
}