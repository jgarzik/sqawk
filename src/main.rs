//! sqawk - an SQL-based command-line utility for processing CSV files
//! 
//! This tool loads CSV files into memory as tables, executes SQL queries against
//! them, and can save modified tables back to CSV files.

mod cli;
mod csv_handler;
mod error;
mod sql_executor;
mod table;

use anyhow::{Context, Result};
// Not explicitly importing SqawkArgs as it's not directly used
use csv_handler::CsvHandler;
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

    // Create a new CSV handler for loading and saving files
    let mut csv_handler = CsvHandler::new();
    
    // Load all specified CSV files into memory
    for file_spec in &args.files {
        csv_handler.load_csv(file_spec)
            .with_context(|| format!("Failed to load CSV file: {}", file_spec))?;
    }

    if args.verbose {
        println!("Loaded {} tables", csv_handler.table_count());
        for table_name in csv_handler.table_names() {
            println!("Table '{}' loaded", table_name);
        }
    }

    // Create SQL executor and execute all SQL statements
    let mut sql_executor = SqlExecutor::new(csv_handler);
    for sql in &args.sql {
        if args.verbose {
            println!("Executing SQL: {}", sql);
        }
        
        let result = sql_executor.execute(sql)
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

    // Save any modified tables back to CSV files
    sql_executor.save_modified_tables()
        .context("Failed to save modified tables")?;

    Ok(())
}
