//! CLI argument parsing module for sqawk
//!
//! This module handles parsing command-line arguments using the clap crate.

use anyhow::Result;
use clap::Parser;

/// Command-line arguments for sqawk
#[derive(Parser, Debug)]
#[clap(author, version, about = "SQL-based CSV processor")]
pub struct SqawkArgs {
    /// SQL statements to execute
    #[clap(short, long, required = true, help = "SQL statement to execute")]
    pub sql: Vec<String>,

    /// CSV files to process - format: [table_name=]file_path.csv
    #[clap(required = true, help = "CSV files to process as [table_name=]file_path.csv")]
    pub files: Vec<String>,

    /// Verbose output
    #[clap(short, long, help = "Enable verbose output")]
    pub verbose: bool,
    
    /// Dry run mode: don't modify any files
    #[clap(long, help = "Don't write changes back to files (dry run)")]
    pub dry_run: bool,
}

/// Parse command-line arguments
pub fn parse_args() -> Result<SqawkArgs> {
    Ok(SqawkArgs::parse())
}
