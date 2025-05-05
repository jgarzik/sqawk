//! sqawk - an SQL-based command-line utility for processing delimited files
//!
//! This tool loads CSV and delimiter-separated files into memory as tables, 
//! executes SQL queries against them, and can save modified tables back to their
//! original format.

mod aggregate;
mod cli;
mod csv_handler;
mod delim_handler;
mod error;
mod file_handler;
mod sql_executor;
mod table;

use anyhow::{Context, Result};
// Not explicitly importing SqawkArgs as it's not directly used
use file_handler::FileHandler;
use sql_executor::SqlExecutor;

/// Main entry point for the sqawk utility
fn main() -> Result<()> {
    // Parse command-line arguments
    let args = cli::parse_args()?;

    // Set up logging if verbose mode is enabled
    if args.verbose {
        println!("Running in verbose mode");
        println!("Arguments: {:?}", args);
    }

    // Create a new file handler for loading and saving files
    let mut file_handler = FileHandler::new(args.field_separator.clone());

    // Load all specified files into memory
    for file_spec in &args.files {
        file_handler
            .load_file(file_spec)
            .with_context(|| format!("Failed to load file: {}", file_spec))?;
    }

    if args.verbose {
        println!("Loaded {} tables", file_handler.table_count());
        for table_name in file_handler.table_names() {
            println!("Table '{}' loaded", table_name);
        }
    }

    // Create SQL executor and execute all SQL statements
    let mut sql_executor = SqlExecutor::new_with_verbose(file_handler, args.verbose);
    for sql in &args.sql {
        if args.verbose {
            println!("Executing SQL: {}", sql);
        }

        let result = sql_executor
            .execute(sql)
            .with_context(|| format!("Failed to execute SQL: {}", sql))?;

        // Print results to stdout
        match result {
            Some(table) => {
                if args.verbose {
                    println!("Query returned {} rows", table.row_count());
                }
                table.print_to_stdout()?;
            }
            None => {
                if args.verbose {
                    println!("Query executed successfully (no results to display)");
                }
            }
        }
    }

    // Save any modified tables back to their original files (only if write flag is enabled)
    if args.write {
        sql_executor
            .save_modified_tables()
            .context("Failed to save modified tables")?;
    } else if args.verbose {
        println!("Changes not saved: use --write to save changes to files");
    }

    Ok(())
}
