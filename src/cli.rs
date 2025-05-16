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
#[clap(
    author,
    version,
    about = "SQL-based processor for delimiter-separated files"
)]
pub struct SqawkArgs {
    /// SQL statements to execute
    ///
    /// Multiple SQL statements can be provided and they will be executed in sequence.
    /// Each statement operates on the state resulting from the previous statement.
    /// Example: -s "SELECT * FROM data" -s "DELETE FROM data WHERE id = 1"
    #[clap(
        short,
        long,
        required_unless_present = "interactive",
        help = "SQL statement to execute"
    )]
    pub sql: Vec<String>,

    /// Start in interactive mode (REPL)
    ///
    /// When enabled, launches an interactive shell for entering SQL commands.
    /// This mode allows exploration of data with immediate feedback.
    /// Type .help in the interactive shell to see available commands.
    #[clap(short, long, help = "Start in interactive mode")]
    pub interactive: bool,

    /// Input files to process - format: [table_name=]file_path
    ///
    /// Users can optionally specify a table name by prefixing the file path.
    /// If no table name is specified, the base filename (without extension)
    /// is used as the table name in SQL queries.
    /// Example: users=data/people.csv or just data/products.csv
    #[clap(
        required = true,
        help = "Input files to process as [table_name=]file_path"
    )]
    pub files: Vec<String>,

    /// Define column names for tables - format: table_name:col1,col2,col3,...
    ///
    /// For files without header rows (like system files), this option allows
    /// specifying explicit column names to use instead of the default a,b,c,... naming.
    /// Multiple table definitions can be provided for different tables.
    /// Example: --tabledef=passwd:username,password,uid,gid,gecos,home,shell
    #[clap(
        long,
        help = "Define column names for tables as table_name:col1,col2,col3,..."
    )]
    pub tabledef: Vec<String>,

    /// Specify field separator character
    ///
    /// Similar to awk's -F option, this sets the field separator for all input files.
    /// Default behavior is to use commas for .csv files and tabs for other file types.
    /// Examples: -F: for colon-separated files, -F\\t for tab-separated files.
    #[clap(short = 'F', help = "Field separator character")]
    pub field_separator: Option<String>,

    /// Enable verbose diagnostic output
    ///
    /// When enabled, shows detailed information about SQL execution,
    /// table loading, and modifications made to the data.
    #[clap(short, long, help = "Enable verbose output")]
    pub verbose: bool,

    /// Write changes back to input files (default is to not modify files)
    ///
    /// By default, sqawk will not modify any input files. This flag must be
    /// specified to write modified tables back to their respective files.
    /// Only tables that were modified by SQL statements will be written,
    /// and their original file format and delimiters will be preserved.
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
