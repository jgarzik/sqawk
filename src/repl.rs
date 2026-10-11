//! The interactive shell (`sqawk -i`).
//!
//! When standard input is a terminal the REPL is a line editor: banner,
//! `sqawk> ` prompt, history, tab completion, and status messages after
//! commands. When standard input is not a terminal (a piped or redirected
//! script) it reads the input line by line and standard output carries only
//! what the commands produce: query results and the output of `.tables`,
//! `.schema`, `.print` and the other reporting commands. Status messages are
//! then dropped (written to standard error under `-v`).
//!
//! Errors are printed as `sqawk: <message>` on standard error and the
//! session continues. A script that hit any error exits with status 1;
//! `.exit N` exits with status N in both modes.

use rustyline::completion::{Completer, Pair};
use rustyline::config::CompletionType;
use rustyline::error::ReadlineError;
use rustyline::highlight::{CmdKind, Highlighter};
use rustyline::hint::Hinter;
use rustyline::history::DefaultHistory;
use rustyline::validate::{self, Validator};
use rustyline::{Config, Context, Editor, Helper};
use std::borrow::Cow;
use std::ffi::OsString;
use std::fmt;
use std::io::{BufRead, IsTerminal};
use std::path::PathBuf;

use crate::config::AppConfig;
use crate::error::SqawkError;
use crate::sql_executor::SqlExecutor;
use crate::vm::like::{Case, Pattern};

/// An error from one REPL command. Its `Display` is the bare message; the
/// REPL prints it as `sqawk: <message>`, the CLI's error form.
#[derive(Debug)]
pub enum ReplError {
    Io(std::io::Error),
    Sqawk(SqawkError),
    /// A malformed or failed dot-command.
    Command(String),
}

impl fmt::Display for ReplError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReplError::Io(err) => write!(f, "{}", crate::error::io_reason(err)),
            ReplError::Sqawk(err) => write!(f, "{}", err),
            ReplError::Command(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for ReplError {}

impl From<std::io::Error> for ReplError {
    fn from(err: std::io::Error) -> Self {
        ReplError::Io(err)
    }
}

impl From<SqawkError> for ReplError {
    fn from(err: SqawkError) -> Self {
        ReplError::Sqawk(err)
    }
}

pub type Result<T> = std::result::Result<T, ReplError>;

/// Every dot-command, for completion.
const DOT_COMMANDS: &[&str] = &[
    ".cd", ".changes", ".exit", ".help", ".load", ".print", ".quit", ".save", ".schema", ".show",
    ".stats", ".tables", ".version", ".write",
];

/// Dot-commands whose argument is a table name.
const TABLE_ARG_COMMANDS: &[&str] = &[".save", ".schema", ".tables"];

/// SQL keywords offered by tab completion: the words of the statements and
/// clauses sqawk implements.
const SQL_KEYWORDS: &[&str] = &[
    "ADD",
    "ALL",
    "ALTER",
    "AND",
    "AS",
    "ASC",
    "BEGIN",
    "BETWEEN",
    "BY",
    "CASE",
    "CAST",
    "COLUMN",
    "COMMIT",
    "CREATE",
    "CROSS",
    "CURRENT",
    "DELETE",
    "DESC",
    "DISTINCT",
    "DROP",
    "ELSE",
    "END",
    "ESCAPE",
    "EXCEPT",
    "EXISTS",
    "FALSE",
    "FILTER",
    "FIRST",
    "FOLLOWING",
    "FROM",
    "FULL",
    "GROUP",
    "HAVING",
    "IF",
    "ILIKE",
    "IN",
    "INNER",
    "INSERT",
    "INTEGER",
    "INTERSECT",
    "INTO",
    "IS",
    "JOIN",
    "LAST",
    "LEFT",
    "LIKE",
    "LIMIT",
    "LOCATION",
    "NATURAL",
    "NOT",
    "NULL",
    "NULLS",
    "OFFSET",
    "ON",
    "OR",
    "ORDER",
    "OUTER",
    "OVER",
    "PARTITION",
    "PRECEDING",
    "RANGE",
    "REAL",
    "RENAME",
    "RIGHT",
    "ROLLBACK",
    "ROW",
    "ROWS",
    "SELECT",
    "SET",
    "TABLE",
    "TEXT",
    "THEN",
    "TO",
    "TRANSACTION",
    "TRUE",
    "TRUNCATE",
    "UNBOUNDED",
    "UNION",
    "UPDATE",
    "USING",
    "VALUES",
    "WHEN",
    "WHERE",
    // Aggregate and window functions (scalar functions come from the
    // function table).
    "AVG",
    "COUNT",
    "CUME_DIST",
    "DENSE_RANK",
    "FIRST_VALUE",
    "GROUP_CONCAT",
    "LAG",
    "LAST_VALUE",
    "LEAD",
    "MAX",
    "MIN",
    "NTH_VALUE",
    "NTILE",
    "PERCENT_RANK",
    "RANK",
    "ROW_NUMBER",
    "STRING_AGG",
    "SUM",
    "TOTAL",
];

/// The loaded tables and their columns, as completion sees them.
type Schema = Vec<(String, Vec<String>)>;

/// Whether `name` starts with `prefix`, ignoring ASCII case.
fn starts_with_ci(name: &str, prefix: &str) -> bool {
    name.len() >= prefix.len()
        && name.is_char_boundary(prefix.len())
        && name[..prefix.len()].eq_ignore_ascii_case(prefix)
}

/// The completions for the cursor at byte offset `pos` of `line`: the byte
/// offset the replacement starts at, and the candidates.
///
/// * A dot-command name completes from [`DOT_COMMANDS`]; the argument of
///   `.schema`, `.save` and `.tables` completes table names.
/// * `table.prefix` completes the columns of `table`.
/// * Any other word completes table names, column names, SQL keywords and
///   function names. Keywords follow the case of the typed word (lower case
///   if it is all lower case, else upper case).
/// * An empty word completes table names.
///
/// Matching is a case-insensitive prefix match.
fn complete_line(line: &str, pos: usize, schema: &Schema) -> (usize, Vec<String>) {
    let pos = pos.min(line.len());
    let before = &line[..pos];
    let trimmed = before.trim_start();

    if trimmed.starts_with('.') {
        let lead = before.len() - trimmed.len();
        return match trimmed.find(char::is_whitespace) {
            None => {
                let cands = DOT_COMMANDS
                    .iter()
                    .filter(|c| starts_with_ci(c, trimmed))
                    .map(|c| c.to_string())
                    .collect();
                (lead, cands)
            }
            Some(end) => {
                let cmd = trimmed[..end].to_ascii_lowercase();
                if !TABLE_ARG_COMMANDS.contains(&cmd.as_str()) {
                    return (pos, Vec::new());
                }
                let word_start = before.rfind(char::is_whitespace).map_or(0, |i| {
                    i + before[i..].chars().next().map_or(1, char::len_utf8)
                });
                let prefix = &before[word_start..];
                let cands = schema
                    .iter()
                    .filter(|(t, _)| starts_with_ci(t, prefix))
                    .map(|(t, _)| t.clone())
                    .collect();
                (word_start, cands)
            }
        };
    }

    // The word under the cursor: identifier characters and dots.
    let start = before
        .char_indices()
        .rev()
        .take_while(|(_, c)| c.is_alphanumeric() || *c == '_' || *c == '.')
        .last()
        .map_or(pos, |(i, _)| i);
    let word = &before[start..];

    if let Some(dot) = word.rfind('.') {
        let qualifier = &word[..dot];
        let prefix = &word[dot + 1..];
        let cands = schema
            .iter()
            .find(|(t, _)| t.eq_ignore_ascii_case(qualifier))
            .map(|(_, cols)| {
                cols.iter()
                    .filter(|c| starts_with_ci(c, prefix))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        return (start + dot + 1, cands);
    }

    let mut cands: Vec<String> = Vec::new();
    let mut push = |s: String| {
        if !cands.iter().any(|c| c.eq_ignore_ascii_case(&s)) {
            cands.push(s);
        }
    };
    for (table, _) in schema {
        if starts_with_ci(table, word) {
            push(table.clone());
        }
    }
    if word.is_empty() {
        return (start, cands);
    }
    for (_, cols) in schema {
        for col in cols {
            if starts_with_ci(col, word) {
                push(col.clone());
            }
        }
    }
    let lower = word.chars().any(|c| c.is_ascii_lowercase())
        && !word.chars().any(|c| c.is_ascii_uppercase());
    let mut words: Vec<&str> = SQL_KEYWORDS
        .iter()
        .copied()
        .chain(crate::vm::scalar_function_names())
        .filter(|k| starts_with_ci(k, word))
        .collect();
    words.sort_unstable();
    for k in words {
        push(if lower {
            k.to_ascii_lowercase()
        } else {
            k.to_string()
        });
    }
    (start, cands)
}

/// What a piece of SQL text holds, ignoring whitespace and comments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SqlScan {
    /// It holds something other than whitespace, comments and `;`.
    has_tokens: bool,
    /// Its last token is a `;` outside any quote or comment.
    ends_with_semicolon: bool,
}

/// Scan `sql` for [`SqlScan`]: `'...'`, `"..."`, `` `...` `` and `[...]`
/// quote, `--` and `/* */` are comments.
fn scan_sql(sql: &str) -> SqlScan {
    let mut has_tokens = false;
    let mut semi = false;
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            c if c.is_whitespace() => {}
            '-' if chars.peek() == Some(&'-') => {
                for c in chars.by_ref() {
                    if c == '\n' {
                        break;
                    }
                }
            }
            '/' if chars.peek() == Some(&'*') => {
                chars.next();
                let mut prev = '\0';
                for c in chars.by_ref() {
                    if prev == '*' && c == '/' {
                        break;
                    }
                    prev = c;
                }
            }
            ';' => semi = true,
            '\'' | '"' | '`' | '[' => {
                has_tokens = true;
                semi = false;
                let close = if c == '[' { ']' } else { c };
                for c in chars.by_ref() {
                    if c == close {
                        break;
                    }
                }
            }
            _ => {
                has_tokens = true;
                semi = false;
            }
        }
    }
    SqlScan {
        has_tokens,
        ends_with_semicolon: semi,
    }
}

/// The history file for `XDG_STATE_HOME` / `HOME`: `$XDG_STATE_HOME/sqawk/history`
/// when `XDG_STATE_HOME` is an absolute path, else `$HOME/.sqawk_history`
/// when `HOME` is set and non-empty, else none.
fn history_path(xdg_state_home: Option<OsString>, home: Option<OsString>) -> Option<PathBuf> {
    if let Some(state) = xdg_state_home
        .map(PathBuf::from)
        .filter(|p| p.is_absolute())
    {
        return Some(state.join("sqawk").join("history"));
    }
    home.filter(|h| !h.is_empty())
        .map(|h| PathBuf::from(h).join(".sqawk_history"))
}

/// The rustyline helper: completion and multi-line statement entry.
#[derive(Default)]
struct ReplHelper {
    /// The loaded tables and columns, refreshed before every prompt.
    schema: Schema,
}

impl Completer for ReplHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Self::Candidate>)> {
        let (start, cands) = complete_line(line, pos, &self.schema);
        Ok((
            start,
            cands
                .into_iter()
                .map(|c| Pair {
                    display: c.clone(),
                    replacement: c,
                })
                .collect(),
        ))
    }
}

impl Hinter for ReplHelper {
    type Hint = String;

    fn hint(&self, _line: &str, _pos: usize, _ctx: &Context<'_>) -> Option<Self::Hint> {
        None
    }
}

impl Highlighter for ReplHelper {
    fn highlight<'l>(&self, line: &'l str, _pos: usize) -> Cow<'l, str> {
        Cow::Borrowed(line)
    }

    fn highlight_char(&self, _line: &str, _pos: usize, _kind: CmdKind) -> bool {
        false
    }
}

impl Validator for ReplHelper {
    /// Input is complete when it is blank, a dot-command, or SQL ending in
    /// `;`; otherwise Enter continues the statement on a new line.
    fn validate(
        &self,
        ctx: &mut validate::ValidationContext,
    ) -> rustyline::Result<validate::ValidationResult> {
        let input = ctx.input();
        let scan = scan_sql(input);
        if input.trim_start().starts_with('.') || !scan.has_tokens || scan.ends_with_semicolon {
            Ok(validate::ValidationResult::Valid(None))
        } else {
            Ok(validate::ValidationResult::Incomplete)
        }
    }
}

impl Helper for ReplHelper {}

/// Commands that can be executed in the REPL
#[derive(Debug)]
enum ReplCommand {
    /// Execute SQL statement
    Sql(String),
    /// Blank input or only comments: nothing to do
    Nothing,
    /// Load a file into a table
    Load(String),
    /// Show the list of tables matching an optional pattern
    Tables(Option<String>),
    /// Show the columns of a table or schema
    Schema(Option<String>),
    /// Toggle writing changes to files
    WriteMode(Option<String>),
    /// Show help message
    Help,
    /// Exit the REPL with optional exit code
    Exit(Option<String>),
    /// Change directory
    ChangeDirectory(String),
    /// Toggle showing number of changes
    Changes(Option<String>),
    /// Print a string literal
    Print(String),
    /// Show version information
    Version,
    /// Save changes to modified tables
    Save(Option<String>),
    /// Show current settings and metadata
    Show(Option<String>),
    /// Show statistics or toggle statistics mode
    Stats(Option<String>),
    /// A malformed or unknown dot-command (the error message)
    Invalid(String),
}

/// REPL interface for interactive SQL entry
pub struct Repl<'a> {
    /// SQL executor for running queries
    executor: SqlExecutor<'a>,
    /// Application configuration for global settings
    config: AppConfig,
    /// Standard input is a terminal: line editing, prompt, banner, history
    /// and status messages
    interactive: bool,
    /// The line editor (interactive only)
    editor: Option<Editor<ReplHelper, DefaultHistory>>,
    /// Where history is kept (interactive only, when a location exists)
    history: Option<PathBuf>,
    /// Whether the REPL is running
    running: bool,
    /// The status `.exit N` asked for
    exit_code: Option<u8>,
    /// A command failed (makes a script's exit status 1)
    failed: bool,
    /// Whether to show number of rows changed by SQL statements
    show_changes: bool,
    /// Whether to show query statistics
    show_stats: bool,
}

impl<'a> Repl<'a> {
    /// Create a new REPL
    pub fn new(executor: SqlExecutor<'a>, app_config: &AppConfig) -> Self {
        let interactive = std::io::stdin().is_terminal();
        let mut editor = None;
        let mut history = None;
        if interactive {
            let rustyline_config = Config::builder()
                .completion_type(CompletionType::List)
                .auto_add_history(false)
                .build();
            match Editor::with_config(rustyline_config) {
                Ok(mut ed) => {
                    ed.set_helper(Some(ReplHelper::default()));
                    history =
                        history_path(std::env::var_os("XDG_STATE_HOME"), std::env::var_os("HOME"));
                    if let Some(path) = &history {
                        let _ = ed.load_history(path);
                    }
                    editor = Some(ed);
                }
                Err(e) => diag!("sqawk: cannot start the line editor: {}", e),
            }
        }

        Self {
            executor,
            config: app_config.clone(),
            interactive,
            editor,
            history,
            running: true,
            exit_code: None,
            failed: false,
            show_changes: false,
            show_stats: false,
        }
    }

    /// Run the REPL until `.exit`, `.quit` or end of input, and return the
    /// process exit status: the `.exit N` code if one was given, else 1 for
    /// a script in which any command failed, else 0.
    pub fn run(&mut self) -> u8 {
        if self.editor.is_some() {
            outln!("Welcome to Sqawk interactive mode!");
            outln!("Enter SQL statements or commands, terminate with ';'.");
            outln!("Type .help for available commands.");
            self.run_editor();
        } else {
            self.run_script();
        }

        // Uncommitted work is discarded, as when SQLite closes a connection.
        self.executor.finish();

        if let (Some(editor), Some(path)) = (self.editor.as_mut(), &self.history) {
            let saved = match path.parent() {
                Some(dir) => std::fs::create_dir_all(dir).map_err(ReadlineError::from),
                None => Ok(()),
            }
            .and_then(|()| editor.save_history(path));
            if let Err(e) = saved {
                diag!(
                    "sqawk: cannot save history to '{}': {}",
                    path.display(),
                    crate::one_line(&e.to_string())
                );
            }
        }

        match self.exit_code {
            Some(code) => code,
            None if self.failed && !self.interactive => 1,
            None => 0,
        }
    }

    /// The terminal loop: one line-edited entry (possibly several lines of a
    /// statement) per command.
    fn run_editor(&mut self) {
        while self.running {
            let schema = self.schema();
            let Some(editor) = self.editor.as_mut() else {
                return;
            };
            if let Some(helper) = editor.helper_mut() {
                helper.schema = schema;
            }
            match editor.readline("sqawk> ") {
                Ok(input) => {
                    if !input.trim().is_empty() {
                        let _ = editor.add_history_entry(input.as_str());
                    }
                    self.dispatch(&input);
                }
                // Ctrl-C abandons the entry being typed.
                Err(ReadlineError::Interrupted) => {}
                Err(ReadlineError::Eof) => break,
                Err(err) => {
                    diag!("sqawk: {}", crate::one_line(&err.to_string()));
                    break;
                }
            }
        }
    }

    /// The script loop: read standard input line by line. A dot-command is a
    /// line of its own; SQL runs once the text gathered ends with `;`, and a
    /// final statement without `;` runs at end of input.
    fn run_script(&mut self) {
        let stdin = std::io::stdin();
        let mut reader = stdin.lock();
        let mut pending = String::new();
        let mut raw = Vec::new();
        while self.running {
            raw.clear();
            match reader.read_until(b'\n', &mut raw) {
                Ok(0) => break,
                Ok(_) => {}
                Err(e) => {
                    diag!(
                        "sqawk: cannot read standard input: {}",
                        crate::error::io_reason(&e)
                    );
                    self.failed = true;
                    break;
                }
            }
            let text = String::from_utf8_lossy(&raw);
            let line = text.trim_end_matches(['\n', '\r']);
            if !scan_sql(&pending).has_tokens && line.trim_start().starts_with('.') {
                pending.clear();
                self.dispatch(line);
                continue;
            }
            pending.push_str(line);
            pending.push('\n');
            if scan_sql(&pending).ends_with_semicolon {
                let sql = std::mem::take(&mut pending);
                self.dispatch(&sql);
            }
        }
        if self.running {
            self.dispatch(&pending);
        }
    }

    /// The loaded tables and their columns.
    fn schema(&self) -> Schema {
        self.executor
            .table_names()
            .into_iter()
            .map(|t| {
                let cols = self.executor.get_table_columns(&t).unwrap_or_default();
                (t, cols)
            })
            .collect()
    }

    /// Parse and run one command, reporting its error.
    fn dispatch(&mut self, input: &str) {
        let command = self.parse_command(input);
        if let Err(e) = self.execute_command(command) {
            diag!("sqawk: {}", crate::one_line(&e.to_string()));
            self.failed = true;
        }
    }

    /// A status message (a confirmation or reminder rather than requested
    /// output): on standard output in a terminal session; in a script, on
    /// standard error under `-v` and otherwise dropped.
    fn note(&self, msg: fmt::Arguments<'_>) {
        if self.interactive {
            crate::diag::out_line(msg);
        } else if self.config.verbose() {
            crate::diag::write_line(msg);
        }
    }

    /// Parse a command from user input
    fn parse_command(&self, input: &str) -> ReplCommand {
        let trimmed = input.trim();

        if let Some(stripped) = trimmed.strip_prefix('.') {
            let parts: Vec<&str> = stripped.splitn(2, ' ').collect();
            let command = parts[0].to_lowercase();
            let arg = parts.get(1).map(|a| a.trim().to_string());

            match command.as_str() {
                "exit" => ReplCommand::Exit(arg),
                "quit" => ReplCommand::Exit(None),
                "tables" => ReplCommand::Tables(arg),
                "schema" => ReplCommand::Schema(arg),
                "load" => match arg {
                    Some(a) if !a.is_empty() => ReplCommand::Load(a),
                    _ => ReplCommand::Invalid("usage: .load [TABLE=]FILE".to_string()),
                },
                "write" => ReplCommand::WriteMode(arg),
                "cd" => match arg {
                    Some(a) if !a.is_empty() => ReplCommand::ChangeDirectory(a),
                    _ => ReplCommand::Invalid("usage: .cd DIRECTORY".to_string()),
                },
                "changes" => ReplCommand::Changes(arg),
                "print" => ReplCommand::Print(parts.get(1).unwrap_or(&"").to_string()),
                "version" => ReplCommand::Version,
                "help" => ReplCommand::Help,
                "save" => ReplCommand::Save(arg),
                "show" => ReplCommand::Show(arg),
                "stats" => ReplCommand::Stats(arg),
                _ => ReplCommand::Invalid(format!("unknown command: .{}", command)),
            }
        } else if scan_sql(input).has_tokens {
            ReplCommand::Sql(input.to_string())
        } else {
            ReplCommand::Nothing
        }
    }

    /// Execute a command
    fn execute_command(&mut self, command: ReplCommand) -> Result<()> {
        match command {
            ReplCommand::Sql(sql) => self.execute_sql(&sql),
            ReplCommand::Nothing => Ok(()),
            ReplCommand::Load(file_spec) => self.load_file(&file_spec),
            ReplCommand::Tables(pattern) => self.show_tables(pattern.as_deref()),
            ReplCommand::Schema(table_name) => self.show_schema(table_name.as_deref()),
            ReplCommand::WriteMode(arg) => self.toggle_write(arg.as_deref()),
            ReplCommand::Help => self.show_help(),
            ReplCommand::Exit(code) => self.exit_repl(code.as_deref()),
            ReplCommand::ChangeDirectory(dir) => self.change_directory(&dir),
            ReplCommand::Changes(arg) => self.toggle_changes(arg.as_deref()),
            ReplCommand::Save(table_name) => self.save_tables(table_name.as_deref()),
            ReplCommand::Show(option) => self.show_settings(option.as_deref()),
            ReplCommand::Stats(option) => self.show_stats(option.as_deref()),
            ReplCommand::Print(text) => {
                outln!("{}", text);
                Ok(())
            }
            ReplCommand::Version => self.show_version(),
            ReplCommand::Invalid(msg) => Err(ReplError::Command(msg)),
        }
    }

    /// Execute SQL statement
    fn execute_sql(&mut self, sql: &str) -> Result<()> {
        // Record start time if statistics are enabled
        let start_time = if self.show_stats {
            Some(std::time::Instant::now())
        } else {
            None
        };

        let result = self.executor.execute(sql)?;

        // Every statement's result set, each with its own header, printed
        // exactly as the command line prints it.
        let header = !self.config.no_output_header();
        for table in &result {
            let _ = table.print_to_stdout(header);
        }

        // The change count is reported independently of whether the line also
        // produced rows: a trailing DML on `SELECT ...; DELETE ...` reports
        // too. It is reported even when zero, so a statement that matched
        // nothing still shows that it ran.
        if self.show_changes && self.executor.last_statement_changed_rows() {
            if let Ok(affected_rows) = self.executor.get_affected_row_count() {
                outln!("{} rows affected", affected_rows);
            }
        }

        // Display query statistics if enabled
        if let Some(start_time) = start_time {
            let execution_time = start_time.elapsed();
            outln!("Run Time: {:.3} ms", execution_time.as_secs_f64() * 1000.0);
        }

        // Save changes if write mode is enabled. Inside a transaction nothing
        // is saved until COMMIT: uncommitted changes never reach a file.
        if !self.executor.in_transaction() {
            if self.config.write_changes() {
                let saved_count = self.executor.save_modified_tables()?;

                if saved_count > 0 {
                    self.note(format_args!("Changes saved to {} tables", saved_count));
                }
            } else if self.executor.has_modified_tables() {
                self.note(format_args!(
                    "Changes not saved: use .write to save changes to files"
                ));
            }
        }

        Ok(())
    }

    /// Load a file into a table
    fn load_file(&mut self, file_spec: &str) -> Result<()> {
        match self.executor.load_file(file_spec)? {
            Some((table_name, file_path)) => {
                self.note(format_args!(
                    "Loaded table '{}' from '{}'",
                    table_name, file_path
                ));
            }
            None => self.note(format_args!("No table created")),
        }
        Ok(())
    }

    /// Show the list of tables, optionally only those whose name matches the
    /// LIKE pattern `pattern` (`%` and `_` wildcards, ASCII case-insensitive).
    fn show_tables(&self, pattern: Option<&str>) -> Result<()> {
        let tables = self.executor.table_names();
        if tables.is_empty() {
            outln!("No tables loaded");
            return Ok(());
        }

        outln!("Tables:");
        let like = pattern.map(|p| Pattern::new(p, None, Case::Ascii));
        let matching: Vec<&String> = tables
            .iter()
            .filter(|name| like.as_ref().is_none_or(|l| l.matches(name)))
            .collect();
        if matching.is_empty() {
            outln!("  No tables match pattern: {}", pattern.unwrap_or(""));
        }
        for table in matching {
            let modified = if self.executor.is_table_modified(table) {
                " (modified)"
            } else {
                ""
            };
            outln!("  {}{}", table, modified);
        }

        Ok(())
    }

    /// Show help message
    fn show_help(&self) -> Result<()> {
        outln!("Available commands:");
        outln!("  .cd DIRECTORY         Change the working directory to DIRECTORY");
        outln!(
            "  .changes [on|off]     Show number of rows changed by SQL (currently: {})",
            if self.show_changes { "ON" } else { "OFF" }
        );
        outln!("  .exit ?CODE?          Exit the REPL with exit status CODE");
        outln!("  .help                 Show this help message");
        outln!("  .load [TABLE=]FILE    Load FILE into TABLE");
        outln!("  .print STRING...      Print literal STRING");
        outln!("  .quit                 Exit the REPL");
        outln!("  .save ?TABLE?         Save changes to all tables or a specific TABLE");
        outln!("  .schema ?TABLE?       Show schema for a specific table or all tables");
        outln!("  .show ?WHAT?          Show current settings and status information");
        outln!(
            "  .stats [on|off]       Toggle statistics display (currently: {})",
            if self.show_stats { "ON" } else { "OFF" }
        );
        outln!("  .tables ?PATTERN?     List names of tables matching LIKE pattern PATTERN");
        outln!("  .version              Show source, library and compiler versions");
        outln!(
            "  .write [on|off]       Toggle writing changes to files (currently: {})",
            if self.config.write_changes() {
                "ON"
            } else {
                "OFF"
            }
        );
        outln!("  SQL_STATEMENT         Execute SQL statement");
        Ok(())
    }

    /// Exit the REPL, with exit status `code` when given. The status is
    /// taken modulo 256, as the operating system reports it.
    fn exit_repl(&mut self, code: Option<&str>) -> Result<()> {
        if let Some(code_str) = code.filter(|c| !c.is_empty()) {
            let code = code_str
                .parse::<i64>()
                .map_err(|_| ReplError::Command(format!("invalid exit code: {}", code_str)))?;
            self.exit_code = Some(code.rem_euclid(256) as u8);
        }
        self.running = false;
        Ok(())
    }

    /// Print the CREATE TABLE form of `name`'s columns.
    fn print_schema(&self, name: &str) -> Result<()> {
        let column_types = self.executor.get_table_column_types(name)?;
        outln!("CREATE TABLE {} (", name);
        for (i, (column_name, data_type)) in column_types.iter().enumerate() {
            let sep = if i + 1 < column_types.len() { "," } else { "" };
            outln!("  {} {}{}", column_name, data_type, sep);
        }
        outln!(");");
        Ok(())
    }

    /// Display schema information for a table or all tables
    fn show_schema(&self, table_name: Option<&str>) -> Result<()> {
        match table_name {
            Some(name) => self.print_schema(name),
            None => {
                for name in self.executor.table_names() {
                    self.print_schema(&name)?;
                }
                Ok(())
            }
        }
    }

    /// Change the current working directory
    fn change_directory(&self, dir: &str) -> Result<()> {
        std::env::set_current_dir(dir).map_err(|e| {
            ReplError::Command(format!(
                "cannot change directory to '{}': {}",
                dir,
                crate::error::io_reason(&e)
            ))
        })?;
        self.note(format_args!("Changed directory to {}", dir));
        Ok(())
    }

    /// Toggle showing number of rows changed by SQL statements
    fn toggle_changes(&mut self, arg: Option<&str>) -> Result<()> {
        self.show_changes = match arg {
            Some("on") => true,
            Some("off") => false,
            _ => !self.show_changes,
        };
        self.note(format_args!(
            "Changes display {}",
            if self.show_changes {
                "enabled"
            } else {
                "disabled"
            }
        ));
        Ok(())
    }

    /// Show version information
    ///
    /// Read from the manifest rather than written out, so it cannot drift.
    fn show_version(&self) -> Result<()> {
        outln!("Sqawk version {}", env!("CARGO_PKG_VERSION"));
        outln!("sqlparser {}", crate::vm::SQLPARSER_VERSION);
        Ok(())
    }

    /// Save changes to tables
    ///
    /// Explicitly writes changes to disk for all modified tables or a specific table
    /// if one is specified. This is useful when write mode is off but you want to
    /// save specific changes.
    fn save_tables(&mut self, table_name: Option<&str>) -> Result<()> {
        if self.executor.in_transaction() {
            return Err(ReplError::Sqawk(crate::error::SqawkError::InvalidSqlQuery(
                "cannot save within a transaction: COMMIT or ROLLBACK first".to_string(),
            )));
        }
        match table_name {
            Some(name) => {
                // Save a specific table if it exists and is modified
                if !self.executor.table_exists(name) {
                    return Err(ReplError::Sqawk(crate::error::SqawkError::TableNotFound(
                        name.to_string(),
                    )));
                }

                if !self.executor.table_is_modified(name) {
                    self.note(format_args!("Table '{}' has no changes to save", name));
                    return Ok(());
                }

                self.executor.save_table(name)?;
                self.note(format_args!("Changes saved to table '{}'", name));
                Ok(())
            }
            None => {
                // Save all modified tables
                if !self.executor.has_modified_tables() {
                    self.note(format_args!("No modified tables to save"));
                    return Ok(());
                }

                let count = self.executor.save_modified_tables()?;
                self.note(format_args!("Changes saved to {} tables", count));
                Ok(())
            }
        }
    }

    /// Toggle writing changes to files
    fn toggle_write(&mut self, arg: Option<&str>) -> Result<()> {
        let on = match arg {
            Some("on") => true,
            Some("off") => false,
            _ => !self.config.write_changes(),
        };
        self.config.set_write_changes(on);
        self.note(format_args!(
            "Write mode {}",
            if on { "enabled" } else { "disabled" }
        ));
        Ok(())
    }

    /// Show current settings and configuration
    fn show_settings(&self, option: Option<&str>) -> Result<()> {
        match option {
            Some("tables") => {
                // Show detailed table information
                self.show_tables_metadata()
            }
            _ => {
                // Show general settings
                outln!("Sqawk Settings:");
                outln!(
                    "  Write Mode:   {}",
                    if self.config.write_changes() {
                        "ON"
                    } else {
                        "OFF"
                    }
                );
                outln!(
                    "  Changes Display: {}",
                    if self.show_changes { "ON" } else { "OFF" }
                );
                outln!(
                    "  Statistics:  {}",
                    if self.show_stats { "ON" } else { "OFF" }
                );
                outln!(
                    "  Verbose:     {}",
                    if self.config.verbose() { "ON" } else { "OFF" }
                );

                // Show field separator if defined
                if let Some(sep) = self.config.field_separator() {
                    outln!("  Field Separator: '{}'", sep);
                } else {
                    outln!("  Field Separator: Default (tab for .tsv/.tab, else comma)");
                }

                // Show counts
                let tables = self.executor.table_names();
                let modified_count = tables
                    .iter()
                    .filter(|t| self.executor.is_table_modified(t))
                    .count();

                outln!("  Tables Loaded: {}", tables.len());
                outln!("  Modified Tables: {}", modified_count);

                Ok(())
            }
        }
    }

    /// Show detailed metadata for all tables
    fn show_tables_metadata(&self) -> Result<()> {
        let tables = self.executor.table_names();
        if tables.is_empty() {
            outln!("No tables loaded");
            return Ok(());
        }

        outln!("Table Information:");
        for table_name in tables {
            let is_modified = self.executor.is_table_modified(&table_name);
            let column_count = match self.executor.get_table_columns(&table_name) {
                Ok(cols) => cols.len(),
                Err(_) => 0,
            };

            outln!("  Table: {}", table_name);
            outln!(
                "    Status: {}",
                if is_modified { "MODIFIED" } else { "Unchanged" }
            );
            outln!("    Columns: {}", column_count);
            outln!();
        }

        Ok(())
    }

    /// Show statistics or toggle statistics mode
    fn show_stats(&mut self, option: Option<&str>) -> Result<()> {
        self.show_stats = match option {
            Some("on") => true,
            Some("off") => false,
            None => !self.show_stats,
            Some(other) => {
                return Err(ReplError::Command(format!(
                    "unknown option for .stats: {} (usage: .stats [on|off])",
                    other
                )))
            }
        };
        self.note(format_args!(
            "Statistics display {}",
            if self.show_stats {
                "enabled"
            } else {
                "disabled"
            }
        ));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> Schema {
        vec![
            (
                "employees".to_string(),
                vec!["id".to_string(), "name".to_string(), "dept".to_string()],
            ),
            (
                "Depts".to_string(),
                vec!["dept".to_string(), "Descr".to_string()],
            ),
        ]
    }

    fn complete(line: &str) -> (usize, Vec<String>) {
        complete_line(line, line.len(), &schema())
    }

    #[test]
    fn completes_dot_commands() {
        assert_eq!(complete(".ta"), (0, vec![".tables".to_string()]));
        assert_eq!(
            complete(".s"),
            (
                0,
                vec![
                    ".save".to_string(),
                    ".schema".to_string(),
                    ".show".to_string(),
                    ".stats".to_string()
                ]
            )
        );
        assert_eq!(complete("  .VER"), (2, vec![".version".to_string()]));
    }

    #[test]
    fn completes_table_argument_of_dot_commands() {
        assert_eq!(complete(".schema em"), (8, vec!["employees".to_string()]));
        assert_eq!(complete(".save d"), (6, vec!["Depts".to_string()]));
        assert_eq!(complete(".cd em"), (6, vec![]));
    }

    #[test]
    fn completes_keywords_in_typed_case() {
        assert_eq!(complete("SEL"), (0, vec!["SELECT".to_string()]));
        assert_eq!(complete("sel"), (0, vec!["select".to_string()]));
        let (start, cands) = complete("SELECT * FROM employees WH");
        assert_eq!(start, 24);
        assert_eq!(cands, vec!["WHEN".to_string(), "WHERE".to_string()]);
    }

    #[test]
    fn completes_function_names() {
        let (_, cands) = complete("SELECT upp");
        assert_eq!(cands, vec!["upper".to_string()]);
        let (_, cands) = complete("SELECT ROW_");
        assert_eq!(cands, vec!["ROW_NUMBER".to_string()]);
    }

    #[test]
    fn completes_table_and_column_names_case_insensitively() {
        let (start, cands) = complete("SELECT * FROM EMP");
        assert_eq!(start, 14);
        assert_eq!(cands, vec!["employees".to_string()]);
        // Tables and columns first, then keywords; duplicates folded.
        let (_, cands) = complete("SELECT de");
        assert_eq!(
            cands,
            vec![
                "Depts".to_string(),
                "dept".to_string(),
                "Descr".to_string(),
                "delete".to_string(),
                "dense_rank".to_string(),
                "desc".to_string()
            ]
        );
    }

    #[test]
    fn completes_columns_after_table_qualifier() {
        assert_eq!(
            complete("SELECT employees.n"),
            (17, vec!["name".to_string()])
        );
        assert_eq!(
            complete("SELECT DEPTS."),
            (13, vec!["dept".to_string(), "Descr".to_string()])
        );
        assert_eq!(complete("SELECT nosuch.x"), (14, vec![]));
    }

    #[test]
    fn empty_word_completes_table_names() {
        assert_eq!(
            complete("SELECT * FROM "),
            (14, vec!["employees".to_string(), "Depts".to_string()])
        );
    }

    #[test]
    fn completion_respects_cursor_position() {
        let line = "SELECT na FROM employees";
        assert_eq!(
            complete_line(line, 9, &schema()),
            (7, vec!["name".to_string(), "natural".to_string()])
        );
    }

    #[test]
    fn completer_trait_returns_pairs() {
        let helper = ReplHelper { schema: schema() };
        let history = DefaultHistory::new();
        let ctx = Context::new(&history);
        let (start, pairs) = helper.complete("SELECT * FROM emp", 17, &ctx).unwrap();
        assert_eq!(start, 14);
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].replacement, "employees");
    }

    #[test]
    fn scan_finds_statement_end() {
        let s = |t: &str| {
            let r = scan_sql(t);
            (r.has_tokens, r.ends_with_semicolon)
        };
        assert_eq!(s(""), (false, false));
        assert_eq!(s("  -- only a comment\n"), (false, false));
        assert_eq!(s("/* c */ ;"), (false, true));
        assert_eq!(s("SELECT 1"), (true, false));
        assert_eq!(s("SELECT 1;"), (true, true));
        assert_eq!(s("SELECT 1; -- trailing"), (true, true));
        assert_eq!(s("SELECT 1; /* x */"), (true, true));
        assert_eq!(s("SELECT ';"), (true, false));
        assert_eq!(s("SELECT ';'"), (true, false));
        assert_eq!(s("SELECT 'it''s';"), (true, true));
        assert_eq!(s("SELECT \"a;\" FROM t"), (true, false));
        assert_eq!(s("SELECT 1; SELECT 2"), (true, false));
    }

    #[test]
    fn history_location() {
        let os = |s: &str| Some(OsString::from(s));
        assert_eq!(
            history_path(os("/state"), os("/home/u")),
            Some(PathBuf::from("/state/sqawk/history"))
        );
        assert_eq!(
            history_path(None, os("/home/u")),
            Some(PathBuf::from("/home/u/.sqawk_history"))
        );
        // A relative XDG_STATE_HOME is invalid and ignored.
        assert_eq!(
            history_path(os("rel"), os("/home/u")),
            Some(PathBuf::from("/home/u/.sqawk_history"))
        );
        assert_eq!(history_path(os(""), os("")), None);
        assert_eq!(history_path(None, None), None);
    }
}
