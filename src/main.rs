//! sqawk - an SQL-based command-line utility for processing delimited files
//!
//! This tool loads CSV and delimiter-separated files into memory as tables,
//! executes SQL queries against them, and can save modified tables back to their
//! original format.
//!
//! # Overview
//!
//! sqawk is designed to bring the power of SQL to command-line data processing.
//! It allows users to query and manipulate delimiter-separated files (CSV, TSV, etc.)
//! using standard SQL syntax, without requiring a database server or schema setup.
//!
//! # Core Functionality
//!
//! - **File Handling**: Load and save CSV, TSV, and custom-delimited files
//! - **SQL Engine**: Execute SQL queries against in-memory tables
//! - **Multi-Table Operations**: Join data across multiple files
//! - **Data Manipulation**: SELECT, UPDATE, INSERT, DELETE operations
//! - **Safe Processing**: Optional write-back model (changes are only saved if requested)
//!
//! # Program Flow
//!
//! 1. Parse command-line arguments
//! 2. Load specified files into in-memory tables
//! 3. Execute SQL statements in sequence
//! 4. Print query results to stdout
//! 5. Save modified tables back to disk if requested

mod aggregate;
mod cli;
mod config;
mod csv_handler;
mod database;
mod delim_handler;
mod error;
mod file_handler;
mod repl;
mod sql_executor;
mod string_functions;
mod table;

use anyhow::{Context, Result};
// Not explicitly importing SqawkArgs as it's not directly used
use config::AppConfig;
use database::Database;
use file_handler::FileHandler;
use repl::Repl;
use sql_executor::SqlExecutor;

/// Main entry point for the sqawk utility
///
/// This function orchestrates the entire application flow:
/// 1. Parses command line arguments
/// 2. Loads input files according to specifications
/// 3. Executes SQL statements against the in-memory tables
/// 4. Outputs results to stdout
/// 5. Handles errors with context for better diagnostics
/// 6. Optionally saves modified tables back to disk
///
/// The design follows a functional pipeline approach:
/// - Files → In-memory Tables → SQL Processing → Results → (Optional) File Writeback
///
/// # Returns
/// * `Ok(())` if all operations completed successfully
/// * `Err` with context if any step fails
fn main() -> Result<()> {
    // Step 1: Parse command-line arguments
    // This handles -s/--sql, file specs, -F (field separator), --write, and -v flags
    let args = cli::parse_args()?;

    // Step 1b: Create a centralized application configuration
    // This will be passed to all components that need configuration settings
    let config = AppConfig::new(
        args.verbose,                                  // Verbose output flag
        args.field_separator.clone(),                  // Field separator for tables
        args.tabledef.clone(),                         // Table column definitions
        args.write,                                    // Whether to write changes to files
    );

    // Configure diagnostics output if verbose mode is enabled (-v flag)
    // This is important for debugging and understanding the execution flow
    if config.verbose() {
        println!("Running in verbose mode");
        println!("Arguments: {args:?}");
    }

    // Step 2a: Create a new Database instance to serve as the central store for tables
    let mut database = Database::new();
    
    // Step 2b: Initialize the file handler with the application configuration
    // The file handler uses config for field separator, table defs, and verbosity
    let mut file_handler = FileHandler::new(
        &config,
        &mut database,
    );

    // Step 2b: Load all specified files into in-memory tables
    // Each file can specify its table name with table_name=file_path syntax
    for file_spec in &args.files {
        file_handler
            .load_file(file_spec)
            .with_context(|| format!("Failed to load file: {file_spec}"))?;
    }

    // Log table loading results in verbose mode
    if config.verbose() {
        let table_count = file_handler.table_count();
        println!("Loaded {table_count} tables");
        for table_name in file_handler.table_names() {
            println!("Table '{table_name}' loaded");
        }
    }

    // Step 3: Create SQL executor
    // The executor maintains state across statements, allowing multi-statement operations
    let mut sql_executor = SqlExecutor::new(&mut database, &mut file_handler, &config);

    // Check if interactive mode is requested
    if args.interactive {
        // Start REPL (Read-Eval-Print Loop) for interactive SQL entry
        let mut repl = Repl::new(sql_executor, &config);
        match repl.run() {
            Ok(_) => return Ok(()),
            Err(e) => return Err(anyhow::anyhow!("Failed to run interactive mode: {}", e)),
        }
    }

    // Process each SQL statement in the order specified on the command line
    // This allows operations like: UPDATE -> DELETE -> SELECT to see the effects
    for sql in &args.sql {
        // Log the SQL being executed in verbose mode
        if args.verbose {
            println!("Executing SQL: {sql}");
        }

        // Execute the SQL statement against the in-memory tables
        // The result may be a table (for SELECT) or None (for UPDATE, DELETE, INSERT)
        let result = sql_executor
            .execute(sql)
            .with_context(|| format!("Failed to execute SQL: {sql}"))?;

        // Step 4: Output results to stdout (for SELECT queries)
        match result {
            // For SELECT queries that return data
            Some(table) => {
                if args.verbose {
                    let row_count = table.row_count();
                    println!("Query returned {row_count} rows");
                }
                // Print the result table in delimiter-separated format
                table.print_to_stdout()?;
            }
            // For statements that don't return data (UPDATE, DELETE, INSERT)
            None => {
                if args.verbose {
                    println!("Query executed successfully (no results to display)");
                }
            }
        }
    }

    // Step 5: Handle file writeback based on the --write flag
    // By default, Sqawk operates in read-only mode unless explicitly told to write
    if args.write {
        // Only tables that were actually modified (by UPDATE, INSERT, DELETE)
        // will be written back to their source files
        sql_executor
            .save_modified_tables()
            .context("Failed to save modified tables")?;
    } else if args.verbose {
        // In verbose mode, remind the user that changes weren't saved
        println!("Changes not saved: use --write to save changes to files");
    }

    Ok(())
}
