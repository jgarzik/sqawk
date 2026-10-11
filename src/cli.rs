//! CLI argument parsing module for sqawk
//!
//! This module handles parsing command-line arguments using the clap crate.
//! It defines the command-line interface structure and behavior for the application,
//! enabling users to specify SQL statements, input files, and processing options.
//!
//! Key features of the CLI:
//! - SQL given inline (`-s`) or read from files (`-f`), run in command-line order
//! - Flexible file specification with optional table name overrides
//! - Opt-in file modification with the --write flag
//! - Output delimiter and header control
//! - Debugging output via the --verbose flag

use clap::{CommandFactory, FromArgMatches, Parser};

/// Command-line arguments for sqawk
///
/// This struct represents all configurable aspects of the application through
/// command-line parameters. It is automatically populated by clap based on
/// the provided arguments.
///
/// The CLI follows the philosophy of being non-destructive by default,
/// requiring an explicit flag to modify input files.
// `long_about` is set explicitly. Without it clap derives the long help from
// this struct's doc comment, which is written for maintainers and read as
// nonsense in `sqawk --help`.
#[derive(Parser, Debug)]
#[clap(
    author,
    version,
    about = "SQL-based processor for delimiter-separated files",
    long_about = "sqawk runs SQL against delimiter-separated files (CSV, TSV, etc.).\n\n\
                  Each input file is loaded as an in-memory table named after the file, \
                  or after the name given as [table_name=]file_path. Statements run in \
                  the order given, and each one sees the effects of those before it.\n\n\
                  Input files are never modified unless --write is given, and then only \
                  the tables that INSERT, UPDATE, DELETE, ALTER TABLE or TRUNCATE changed \
                  (and tables created with CREATE TABLE ... LOCATION). BEGIN / COMMIT / \
                  ROLLBACK group statements; a transaction still open at the end is \
                  rolled back before anything is written.\n\n\
                  Errors are reported on standard error as one line, `sqawk: <message>`, \
                  with exit status 1; command-line usage errors exit with status 2."
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
        value_name = "SQL",
        required_unless_present_any = ["interactive", "file"],
        help = "SQL statement(s) to execute (repeatable)"
    )]
    pub sql: Vec<String>,

    /// Files of SQL statements to execute
    ///
    /// Each file may hold several `;`-separated statements. `-f -` reads the
    /// statements from standard input. `-s` and `-f` may be mixed; they run
    /// in the order given on the command line.
    #[clap(
        short = 'f',
        long = "file",
        value_name = "FILE",
        help = "Read SQL statements from FILE ('-' for stdin; repeatable, runs in order with -s)"
    )]
    pub file: Vec<String>,

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
    ///
    /// A path of `-` reads standard input, as the table `stdin` or under the
    /// name given as `table_name=-`. Piped input is treated as comma-separated
    /// unless -F says otherwise, and cannot be written back with --write.
    #[clap(
        required = true,
        help = "Input files to process as [table_name=]file_path ('-' for stdin)"
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
    /// Similar to awk's -F option, this sets the field separator for all input files,
    /// and the output delimiter unless --output-delimiter is given.
    /// Without it, `.tsv` and `.tab` files are tab-separated and every other file
    /// (and standard input) is comma-separated.
    /// Examples: -F: for colon-separated files, -F'\t' for tab-separated files.
    #[clap(
        short = 'F',
        long = "field-separator",
        value_name = "SEP",
        value_parser = parse_separator,
        help = "Input field separator, one character or \\t (also the output delimiter by default)"
    )]
    pub field_separator: Option<String>,

    /// Delimiter between fields of query results
    ///
    /// Defaults to the -F separator when one is given, else a comma. Same
    /// rules as -F: one ASCII character other than a quote or line break, or
    /// `\t` for tab.
    #[clap(
        long = "output-delimiter",
        value_name = "SEP",
        value_parser = parse_separator,
        help = "Delimiter for query results (default: the -F separator, else comma)"
    )]
    pub output_delimiter: Option<String>,

    /// Omit the header line of every result set
    #[clap(
        long = "no-output-header",
        help = "Omit the header line of query results"
    )]
    pub no_output_header: bool,

    /// Input files have no header row
    ///
    /// The first line of every input file is data. Columns are named a, b,
    /// c, ... unless --tabledef names them. Writeback emits no header line.
    #[clap(long = "no-header", help = "Treat the first line of each file as data")]
    pub no_header: bool,

    /// Lines starting with `#` are comments
    ///
    /// Without this flag a line starting with `#` is an ordinary record. With
    /// it, such lines are skipped when files are read, and kept in place when
    /// a table is written back with --write.
    #[clap(long, help = "Skip lines starting with '#' (kept on --write)")]
    pub comments: bool,

    /// Print debugging information on standard error
    ///
    /// Shows table loading, the compiled bytecode and each executed
    /// instruction. The output is for debugging; its format is not stable.
    #[clap(
        short,
        long,
        help = "Print debugging output on stderr (format is not stable)"
    )]
    pub verbose: bool,

    /// Write changes back to input files (default is to not modify files)
    ///
    /// By default, sqawk will not modify any input files. This flag must be
    /// specified to write modified tables back to their respective files.
    /// Only tables that were modified by SQL statements will be written,
    /// and their original file format and delimiters will be preserved.
    #[clap(short = 'w', long, help = "Write changes back to input files")]
    pub write: bool,

    /// Every `-s` and `-f`, in command-line order (filled in by [`parse_args`]).
    #[clap(skip)]
    pub script: Vec<SqlSource>,
}

/// One `-s` or `-f` argument.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlSource {
    /// `-s SQL`: the statement text itself
    Text(String),
    /// `-f FILE`: a file of statements (`-` is standard input)
    File(String),
}

/// Parse command-line arguments into the SqawkArgs structure
///
/// clap handles usage errors itself (message on stderr, exit status 2).
/// The `-s` and `-f` values are merged into [`SqawkArgs::script`] by their
/// position on the command line.
pub fn parse_args() -> SqawkArgs {
    let matches = SqawkArgs::command().get_matches();
    let mut args = match SqawkArgs::from_arg_matches(&matches) {
        Ok(args) => args,
        Err(e) => e.exit(),
    };
    let mut script: Vec<(usize, SqlSource)> = Vec::new();
    if let Some(idx) = matches.indices_of("sql") {
        script.extend(idx.zip(args.sql.iter().cloned().map(SqlSource::Text)));
    }
    if let Some(idx) = matches.indices_of("file") {
        script.extend(idx.zip(args.file.iter().cloned().map(SqlSource::File)));
    }
    script.sort_by_key(|(i, _)| *i);
    args.script = script.into_iter().map(|(_, s)| s).collect();
    args
}

/// clap value parser for `-F` / `--output-delimiter`.
fn parse_separator(sep: &str) -> Result<String, String> {
    validate_separator(sep)
}

/// Validate a field separator and return it in canonical form.
///
/// The separator must be exactly one ASCII byte, other than a quote or a
/// line break; the two-character escape `\t` is accepted as a tab.
pub fn validate_separator(sep: &str) -> Result<String, String> {
    let sep = if sep == "\\t" { "\t" } else { sep };
    match sep.as_bytes() {
        [] => Err("separator must not be empty".to_string()),
        [b'"' | b'\n' | b'\r'] => Err("separator cannot be a quote or a line break".to_string()),
        [b] if b.is_ascii() => Ok(sep.to_string()),
        _ => Err(format!(
            "separator must be a single ASCII character (or \\t for tab), got {:?}",
            sep
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn separators() {
        assert_eq!(validate_separator("\\t").unwrap(), "\t");
        assert_eq!(validate_separator(":").unwrap(), ":");
        assert!(validate_separator("").is_err());
        assert!(validate_separator("\"").is_err());
        assert!(validate_separator("ab").is_err());
    }

    #[test]
    fn sql_and_files_keep_command_line_order() {
        let m = SqawkArgs::command()
            .try_get_matches_from(["sqawk", "-s", "A", "-f", "x.sql", "-s", "B", "t.csv"])
            .unwrap();
        let idx: Vec<usize> = m.indices_of("sql").unwrap().collect();
        let fidx: Vec<usize> = m.indices_of("file").unwrap().collect();
        assert!(idx[0] < fidx[0] && fidx[0] < idx[1]);
    }
}
