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
//!
//! Any error ends the run with one line on stderr, `sqawk: <message>`, and
//! exit status 1 (clap's own usage errors exit with status 2).

#[macro_use]
mod diag;

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

use std::process::ExitCode;

use cli::{SqawkArgs, SqlSource};
use config::AppConfig;
use database::Database;
use error::{SqawkError, SqawkResult};
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

/// Parse the command line, then run [`run`] on a freshly mapped
/// [`MAIN_STACK_BYTES`] stack, on this thread. (A second thread with a large
/// stack would work too, but once a process has two threads glibc's malloc
/// takes a lock on every call, which slows a large load or GROUP BY by
/// several percent.)
fn main() -> ExitCode {
    let args = cli::parse_args();
    match stacker::grow(MAIN_STACK_BYTES, || run(args)) {
        Ok(status) => ExitCode::from(status),
        Err(e) => {
            diag!("sqawk: {}", one_line(&e.to_string()));
            ExitCode::FAILURE
        }
    }
}

/// `msg` on a single line: line breaks (which an error quoting multi-line
/// SQL could contain) become spaces, so every error is exactly one line.
pub fn one_line(msg: &str) -> String {
    if !msg.contains(['\n', '\r']) {
        return msg.to_string();
    }
    msg.replace("\r\n", " ").replace(['\n', '\r'], " ")
}

/// Whether a file operand reads standard input.
fn reads_stdin(spec: &str) -> bool {
    file_handler::parse_table_spec(spec)
        .is_ok_and(|(_, path)| path.as_os_str() == file_handler::STDIN_SPEC)
}

/// The text of every `-s` / `-f` argument, in command-line order. Files are
/// read up front, so a mistyped `-f` path fails before anything runs.
fn read_script(args: &SqawkArgs) -> SqawkResult<Vec<String>> {
    let sql_from_stdin = args
        .script
        .iter()
        .any(|s| matches!(s, SqlSource::File(f) if f == file_handler::STDIN_SPEC));
    if sql_from_stdin && args.files.iter().any(|spec| reads_stdin(spec)) {
        return Err(SqawkError::InvalidFileSpec(
            "standard input cannot supply both SQL (-f -) and a table (-)".to_string(),
        ));
    }
    let mut stdin_read = false;
    let texts: Vec<(bool, String)> = args
        .script
        .iter()
        .map(|source| {
            let is_file = matches!(source, SqlSource::File(_));
            read_source(source, &mut stdin_read).map(|text| (is_file, text))
        })
        .collect::<SqawkResult<_>>()?;
    // A file with no statements (empty, or only whitespace) runs nothing;
    // an empty `-s` is still an error.
    Ok(texts
        .into_iter()
        .filter(|(is_file, text)| !(*is_file && text.trim().is_empty()))
        .map(|(_, text)| text)
        .collect())
}

/// The SQL text of one `-s` / `-f` argument.
fn read_source(source: &SqlSource, stdin_read: &mut bool) -> SqawkResult<String> {
    match source {
        SqlSource::Text(sql) => Ok(sql.clone()),
        SqlSource::File(path) if path == file_handler::STDIN_SPEC => {
            if *stdin_read {
                return Err(SqawkError::InvalidFileSpec(
                    "standard input can only be read once; '-f -' was given more than once"
                        .to_string(),
                ));
            }
            *stdin_read = true;
            let mut sql = String::new();
            std::io::Read::read_to_string(&mut std::io::stdin().lock(), &mut sql)
                .map_err(|e| SqawkError::open("standard input", e))?;
            Ok(sql)
        }
        SqlSource::File(path) => {
            std::fs::read_to_string(path).map_err(|e| SqawkError::open(path, e))
        }
    }
}

/// Main entry point for the sqawk utility
///
/// This function orchestrates the entire application flow:
/// 1. Loads input files according to specifications
/// 2. Executes SQL statements against the in-memory tables
/// 3. Outputs results to stdout
/// 4. Optionally saves modified tables back to disk
///
/// The design follows a functional pipeline approach:
/// - Files → In-memory Tables → SQL Processing → Results → (Optional) File Writeback
///
/// # Returns
/// * `Ok(status)` with the exit status: 0 for a completed run, or what the
///   REPL session ended with (see [`Repl::run`])
/// * `Err` with the first error; `main` prints it as one line
fn run(args: SqawkArgs) -> SqawkResult<u8> {
    // Create a centralized application configuration
    // This will be passed to all components that need configuration settings
    let config = AppConfig::new(
        args.verbose,                 // Verbose output flag
        args.field_separator.clone(), // Field separator for tables
        args.tabledef.clone(),        // Table column definitions
        args.write,                   // Whether to write changes to files
    )
    .with_no_header(args.no_header)
    .with_comments(args.comments)
    .with_output_delimiter(args.output_delimiter.clone())
    .with_no_output_header(args.no_output_header);

    // Debugging output (-v); its format is not stable.
    if config.verbose() {
        diag!("Running in verbose mode");
        diag!("Arguments: {args:?}");
    }

    let script = read_script(&args)?;

    // Create a new Database instance to serve as the central store for tables
    let mut database = Database::new();

    // Compile table definitions from the CLI arguments
    // This makes Database the source of truth for table schemas defined via CLI
    database.compile_table_definitions(&config)?;

    // Initialize the file handler with the application configuration
    // The file handler uses config for field separator and verbosity
    let mut file_handler = FileHandler::new(&config);

    // The REPL reads its commands from stdin, so it cannot also consume stdin
    // as table data: whichever got there first would starve the other.
    if args.interactive && args.files.iter().any(|spec| reads_stdin(spec)) {
        return Err(SqawkError::InvalidFileSpec(
            "cannot read a table from standard input in interactive mode: \
             the REPL reads its own commands from stdin"
                .to_string(),
        ));
    }

    // Load all specified files into in-memory tables
    // Each file can specify its table name with table_name=file_path syntax
    for file_spec in &args.files {
        file_handler.load_file(&mut database, file_spec)?;
    }
    database.check_table_definitions_used()?;

    // Log table loading results in verbose mode
    if config.verbose() {
        let table_count = database.table_count();
        diag!("Loaded {table_count} tables");
        for table_name in database.table_names() {
            diag!("Table '{table_name}' loaded");
        }
    }

    // The REPL is about to take ownership of stdin for its command stream, so
    // no later `.load -` may consume it.
    if args.interactive {
        file_handler.disable_stdin();
    }

    // Create SQL executor, which takes ownership of the database and file handler
    // The executor maintains state across statements, allowing multi-statement operations
    let mut sql_executor = SqlExecutor::new(database, file_handler, &config);

    // Check if interactive mode is requested
    if args.interactive {
        // Start REPL (Read-Eval-Print Loop) for interactive SQL entry
        let mut repl = Repl::new(sql_executor, &config);
        return Ok(repl.run());
    }

    let mut stdout_open = true;
    let header = !config.no_output_header();

    // Process each SQL statement in the order specified on the command line
    // This allows operations like: UPDATE -> DELETE -> SELECT to see the effects
    for sql in &script {
        // Execute the SQL statement against the in-memory tables
        // The result may be a table (for SELECT) or None (for UPDATE, DELETE, INSERT)
        let result = sql_executor.execute(sql)?;

        // Output results to stdout (for SELECT queries).
        //
        // One result set per statement, each with its own header. They used to
        // share a single buffer and schema, so `SELECT a; SELECT b` printed
        // every row under the last statement's header.
        for table in &result {
            if config.verbose() {
                let row_count = table.row_count();
                diag!("Query returned {row_count} rows");
            }
            // Print the result table in delimiter-separated format. Once the
            // reader of a pipe has gone away (`sqawk ... | head`), stop
            // printing but keep going: later statements and --write still run.
            if stdout_open {
                if let Err(e) = table.print_to_stdout(header) {
                    if e.kind() != std::io::ErrorKind::BrokenPipe {
                        return Err(SqawkError::write("standard output", e));
                    }
                    stdout_open = false;
                }
            }
        }
        if result.is_empty() && config.verbose() {
            diag!("Query executed successfully (no results to display)");
        }
    }

    // A transaction left open by the last statement is rolled back, so its
    // changes are neither written back nor kept.
    sql_executor.finish();

    // Handle file writeback based on the configuration
    // By default, Sqawk operates in read-only mode unless explicitly told to write
    if config.write_changes() {
        // Only modified tables with a file are written back
        sql_executor.save_modified_tables()?;
    } else if config.verbose() {
        // In verbose mode, remind the user that changes weren't saved
        diag!("Changes not saved: use --write to save changes to files");
    }

    // The process is about to exit, so freeing every loaded cell one by one
    // is wasted work (measurable on large inputs); let the OS reclaim it.
    std::mem::forget(sql_executor);

    Ok(0)
}
