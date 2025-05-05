//! CLI argument parsing module for sqawk
//!
//! This module handles parsing command-line arguments using the clap crate.
//! It defines the command-line interface structure and behavior for the application,
//! enabling users to specify SQL statements, input files, and processing options.
//!
//! Key features of the CLI:
//! - Support for multiple SQL statements in a single invocation
//! - Flexible file specification with optional table name overrides
//! - Opt-in file modification with the --write flag
//! - Diagnostic output control via the --verbose flag

use anyhow::Result;
use clap::Parser;

/// Command-line arguments for sqawk
///
/// This struct represents all configurable aspects of the application through
/// command-line parameters. It is automatically populated by clap based on
/// the provided arguments.
///
/// The CLI follows the philosophy of being non-destructive by default, 
/// requiring an explicit flag to modify input files.
#[derive(Parser, Debug)]
#[clap(author, version, about = "SQL-based CSV processor")]
pub struct SqawkArgs {
    /// SQL statements to execute
    /// 
    /// Multiple SQL statements can be provided and they will be executed in sequence.
    /// Each statement operates on the state resulting from the previous statement.
    /// Example: -s "SELECT * FROM data" -s "DELETE FROM data WHERE id = 1"
    #[clap(short, long, required = true, help = "SQL statement to execute")]
    pub sql: Vec<String>,

    /// CSV files to process - format: [table_name=]file_path.csv
    /// 
    /// Users can optionally specify a table name by prefixing the file path.
    /// If no table name is specified, the base filename (without extension)
    /// is used as the table name in SQL queries.
    /// Example: users=data/people.csv or just data/products.csv
    #[clap(
        required = true,
        help = "CSV files to process as [table_name=]file_path.csv"
    )]
    pub files: Vec<String>,

    /// Enable verbose diagnostic output
    ///
    /// When enabled, shows detailed information about SQL execution,
    /// table loading, and modifications made to the data.
    #[clap(short, long, help = "Enable verbose output")]
    pub verbose: bool,

    /// Write changes back to input files (default is to not modify files)
    ///
    /// By default, sqawk will not modify any input files. This flag must be
    /// specified to write modified tables back to their respective CSV files.
    /// Only tables that were modified by SQL statements will be written.
    #[clap(short = 'w', long, help = "Write changes back to input files")]
    pub write: bool,
}

/// Parse command-line arguments into the SqawkArgs structure
///
/// This function uses clap to handle argument parsing, validation, and help text generation.
/// It automatically generates usage information, handles errors for missing required arguments,
/// and properly validates supported options.
///
/// # Returns
/// * `Ok(SqawkArgs)` - Command-line arguments successfully parsed
/// * `Err` - Error during argument parsing (handled by clap, usually results in help text display)
pub fn parse_args() -> Result<SqawkArgs> {
    Ok(SqawkArgs::parse())
}
