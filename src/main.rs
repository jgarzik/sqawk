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
mod capacity;
mod cli;
mod config;
mod csv_handler;
mod database;
mod error;
mod file_handler;
mod repl;
mod sql_executor;
mod table;
mod vm;

use anyhow::{Context, Result};
// Not explicitly importing SqawkArgs as it's not directly used
use config::AppConfig;
use database::Database;
use file_handler::FileHandler;
use repl::Repl;
use sql_executor::SqlExecutor;

/// Stack size sqawk runs on.
///
/// Parsing, compiling and evaluating an expression recurse once per level of
/// nesting, and SQLite's depth limit (1000, enforced right after parsing)
/// needs more than the 8 MiB main-thread stack in a debug build. The memory
/// is reserved, not committed: pages are touched only as deep as recursion
/// actually goes.
const MAIN_STACK_BYTES: usize = 256 * 1024 * 1024;

/// Run [`run`] on a freshly mapped [`MAIN_STACK_BYTES`] stack, on this
/// thread. (A second thread with a large stack would work too, but once a
/// process has two threads glibc's malloc takes a lock on every call, which
/// slows a large load or GROUP BY by several percent.)
fn main() -> Result<()> {
    stacker::grow(MAIN_STACK_BYTES, run)
}

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
fn run() -> Result<()> {
    // Step 1: Parse command-line arguments
    // This handles -s/--sql, file specs, -F (field separator), --write, and -v flags
    let args = cli::parse_args()?;

    // Step 1b: Create a centralized application configuration
    // This will be passed to all components that need configuration settings
    let field_separator = args
        .field_separator
        .as_deref()
        .map(cli::validate_separator)
        .transpose()?;
    let config = AppConfig::new(
        args.verbose,          // Verbose output flag
        field_separator,       // Field separator for tables
        args.tabledef.clone(), // Table column definitions
        args.write,            // Whether to write changes to files
    )
    .with_no_header(args.no_header)
    .with_comments(args.comments);

    // Configure diagnostics output if verbose mode is enabled (-v flag)
    // This is important for debugging and understanding the execution flow
    if config.verbose() {
        eprintln!("Running in verbose mode");
        eprintln!("Arguments: {args:?}");
    }

    // Step 2a: Create a new Database instance to serve as the central store for tables
    let mut database = Database::new();

    // Step 2b: Compile table definitions from the CLI arguments
    // This makes Database the source of truth for table schemas defined via CLI
    database
        .compile_table_definitions(&config)
        .context("Failed to compile table definitions")?;

    // Step 2c: Initialize the file handler with the application configuration
    // The file handler uses config for field separator and verbosity
    // Note: table definitions are now handled by Database directly
    let mut file_handler = FileHandler::new(&config, &mut database);

    // The REPL reads its commands from stdin, so it cannot also consume stdin
    // as table data: whichever got there first would starve the other.
    if args.interactive
        && args.files.iter().any(|spec| {
            file_handler::parse_table_spec(spec)
                .is_ok_and(|(_, path)| path.as_os_str() == file_handler::STDIN_SPEC)
        })
    {
        anyhow::bail!(
            "cannot read a table from standard input in interactive mode: \
             the REPL reads its own commands from stdin"
        );
    }

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
        eprintln!("Loaded {table_count} tables");
        for table_name in file_handler.table_names() {
            eprintln!("Table '{table_name}' loaded");
        }
    }

    // The REPL is about to take ownership of stdin for its command stream, so
    // no later `.load -` may consume it.
    if args.interactive {
        file_handler.disable_stdin();
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

    let mut stdout_open = true;

    // Process each SQL statement in the order specified on the command line
    // This allows operations like: UPDATE -> DELETE -> SELECT to see the effects
    for sql in &args.sql {
        // Execute the SQL statement against the in-memory tables
        // The result may be a table (for SELECT) or None (for UPDATE, DELETE, INSERT)
        let result = sql_executor
            .execute(sql)
            .with_context(|| format!("Failed to execute SQL: {sql}"))?;

        // Step 4: Output results to stdout (for SELECT queries).
        //
        // One result set per statement, each with its own header. They used to
        // share a single buffer and schema, so `SELECT a; SELECT b` printed
        // every row under the last statement's header.
        for table in &result {
            if config.verbose() {
                let row_count = table.row_count();
                eprintln!("Query returned {row_count} rows");
            }
            // Print the result table in delimiter-separated format. Once the
            // reader of a pipe has gone away (`sqawk ... | head`), stop
            // printing but keep going: later statements and --write still run.
            if stdout_open {
                if let Err(e) = table.print_to_stdout() {
                    if e.kind() != std::io::ErrorKind::BrokenPipe {
                        return Err(e).context("Failed to write results");
                    }
                    stdout_open = false;
                }
            }
        }
        if result.is_empty() && config.verbose() {
            eprintln!("Query executed successfully (no results to display)");
        }
    }

    // Step 5: Handle file writeback based on the configuration
    // By default, Sqawk operates in read-only mode unless explicitly told to write
    if config.write_changes() {
        // Only tables that were actually modified (by UPDATE, INSERT, DELETE)
        // will be written back to their source files
        sql_executor
            .save_modified_tables()
            .context("Failed to save modified tables")?;
    } else if config.verbose() {
        // In verbose mode, remind the user that changes weren't saved
        eprintln!("Changes not saved: use --write to save changes to files");
    }

    // The process is about to exit, so freeing every loaded cell one by one
    // is wasted work (measurable on large inputs); let the OS reclaim it.
    drop(sql_executor);
    drop(file_handler);
    std::mem::forget(database);

    Ok(())
}
