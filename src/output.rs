//! Where result rows go.
//!
//! The VM hands each statement's rows to a [`ResultSink`] one at a time, as
//! they are produced (SQLite's step model), so a `SELECT *` over a large
//! table is never held in memory as a whole result before printing.
//! [`StdoutSink`] prints them; [`CollectSink`] gathers each result set into
//! a [`Table`] (derived tables, CTEs and tests).

use std::io::{BufWriter, ErrorKind, Stdout, Write};

use crate::error::{SqawkError, SqawkResult};
use crate::table::{ColumnDefinition, RecordWriter, Table, Value, delimiter_byte};
use crate::vm::bytecode::Register;

/// The size of [`StdoutSink`]'s buffer. It is also flushed at the end of
/// every statement.
const STDOUT_BUFFER: usize = 64 * 1024;

/// Receives one statement's result set at a time: `begin` with its columns,
/// then each row, then `end`. A statement with result columns but no rows
/// still gets `begin` and `end` (its header prints); one with no result
/// columns (DML, DDL) gets nothing. When the statement fails part-way, the
/// sink gets `abort` in place of `end`, after whatever rows it already had.
pub trait ResultSink {
    /// A result set with these columns starts.
    fn begin(&mut self, columns: &[ColumnDefinition]) -> SqawkResult<()>;
    /// One row, as wide as the columns given to `begin`.
    fn row(&mut self, values: &[Register]) -> SqawkResult<()>;
    /// The result set is complete.
    fn end(&mut self) -> SqawkResult<()>;
    /// The statement failed after `begin`; the error follows.
    fn abort(&mut self);
}

/// Gathers each result set into a table named `result` and hands it to a
/// callback at `end`. An aborted result set is dropped.
pub struct CollectSink<F> {
    table: Option<Table>,
    on_table: F,
}

impl<F: FnMut(Table) -> SqawkResult<()>> CollectSink<F> {
    pub fn new(on_table: F) -> Self {
        CollectSink {
            table: None,
            on_table,
        }
    }
}

impl<F: FnMut(Table) -> SqawkResult<()>> ResultSink for CollectSink<F> {
    fn begin(&mut self, columns: &[ColumnDefinition]) -> SqawkResult<()> {
        self.table = Some(Table::new_with_schema(
            "result",
            columns.to_vec(),
            None,
            None,
        ));
        Ok(())
    }

    fn row(&mut self, values: &[Register]) -> SqawkResult<()> {
        let table = self
            .table
            .as_mut()
            .ok_or_else(|| SqawkError::VmError("result row before its columns".to_string()))?;
        table.add_row(values.iter().cloned().map(Value::from).collect())
    }

    fn end(&mut self) -> SqawkResult<()> {
        match self.table.take() {
            Some(table) => (self.on_table)(table),
            None => Ok(()),
        }
    }

    fn abort(&mut self) {
        self.table = None;
    }
}

/// How result sets are printed (`--format`, the REPL's `.mode`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Format {
    /// Delimited records with RFC 4180 quoting (the output delimiter)
    Csv,
    /// Tab-separated records, quoted as CSV is
    Tsv,
    /// One JSON array of objects per result set
    Json,
    /// One JSON object per row, one per line
    Jsonl,
    /// A Markdown table (pipes, header separator row)
    Markdown,
    /// An aligned table with box-drawing borders
    Table,
}

impl Format {
    pub const NAMES: &'static [&'static str] =
        &["csv", "tsv", "json", "jsonl", "markdown", "table"];

    pub fn parse(s: &str) -> Option<Self> {
        Some(match s.to_ascii_lowercase().as_str() {
            "csv" => Format::Csv,
            "tsv" => Format::Tsv,
            "json" => Format::Json,
            "jsonl" => Format::Jsonl,
            "markdown" | "md" => Format::Markdown,
            "table" | "box" => Format::Table,
            _ => return None,
        })
    }

    pub fn name(self) -> &'static str {
        match self {
            Format::Csv => "csv",
            Format::Tsv => "tsv",
            Format::Json => "json",
            Format::Jsonl => "jsonl",
            Format::Markdown => "markdown",
            Format::Table => "table",
        }
    }
}

/// Everything that decides how a result set prints.
#[derive(Debug, Clone)]
pub struct OutputSettings {
    pub format: Format,
    /// The CSV field delimiter (`--output-delimiter`, else `-F`, else comma)
    pub delimiter: String,
    /// Print each result set's column names (not for JSON, whose objects
    /// carry them; Markdown always has its header row)
    pub header: bool,
    /// How NULL prints in csv, tsv, markdown and table output
    pub null: String,
}

/// One cell of a buffered (markdown / table) result: its text, and whether
/// it is a number (right-aligned).
struct Cell {
    text: String,
    number: bool,
    null: bool,
}

/// Prints result sets to standard output in the configured [`Format`].
///
/// CSV, TSV and JSON Lines stream row by row; JSON streams one array per
/// result set; Markdown and table output hold one result set to size their
/// columns. Everything goes through a 64 KiB buffer flushed at the end of
/// each statement.
///
/// Once the reader of a pipe has gone away (`sqawk ... | head`), printing
/// stops quietly but statements keep running, so later statements and
/// `--write` still happen. Any other write error is returned, unless the
/// sink was made with `ignore_errors` (the REPL, whose output is best
/// effort like every other line it prints).
pub struct StdoutSink {
    settings: OutputSettings,
    writer: RecordWriter,
    verbose: bool,
    ignore_errors: bool,
    open: bool,
    out: Option<BufWriter<Stdout>>,
    rows: usize,
    result_sets: usize,
    /// The current result set's column names.
    names: Vec<String>,
    /// Markdown / table rows held for sizing.
    held: Vec<Vec<Cell>>,
}

impl StdoutSink {
    /// `verbose`: report each result set's row count on stderr after its
    /// rows.
    pub fn new(settings: OutputSettings, verbose: bool, ignore_errors: bool) -> Self {
        let delimiter = match settings.format {
            Format::Tsv => b'\t',
            _ => delimiter_byte(&settings.delimiter),
        };
        StdoutSink {
            settings,
            writer: RecordWriter {
                delimiter,
                terminator: b"\n",
                comments: false,
            },
            verbose,
            ignore_errors,
            open: true,
            out: None,
            rows: 0,
            result_sets: 0,
            names: Vec::new(),
            held: Vec::new(),
        }
    }

    /// Result sets begun since the last call (statements that had result
    /// columns).
    pub fn take_result_sets(&mut self) -> usize {
        std::mem::take(&mut self.result_sets)
    }

    /// Note the outcome of a write: a closed pipe (or any error, when
    /// errors are ignored) silences the rest of the output.
    fn check(&mut self, result: std::io::Result<()>) -> SqawkResult<()> {
        match result {
            Ok(()) => Ok(()),
            Err(e) => {
                self.open = false;
                self.out = None;
                if e.kind() == ErrorKind::BrokenPipe || self.ignore_errors {
                    Ok(())
                } else {
                    Err(SqawkError::write("standard output", e))
                }
            }
        }
    }

    /// Write `bytes` to the buffer, if output is still open.
    fn put(&mut self, bytes: &[u8]) -> SqawkResult<()> {
        let result = match self.out.as_mut() {
            Some(out) => out.write_all(bytes),
            None => return Ok(()),
        };
        self.check(result)
    }

    fn flush(&mut self) -> SqawkResult<()> {
        let result = match self.out.as_mut() {
            Some(out) => out.flush(),
            None => Ok(()),
        };
        self.out = None;
        self.check(result)
    }

    /// The text a NULL or other value shows in csv / tsv / markdown /
    /// table output.
    fn text<'v>(&self, v: &'v Register) -> std::borrow::Cow<'v, str> {
        match v {
            Register::Null => std::borrow::Cow::Owned(self.settings.null.clone()),
            other => other.field_str(),
        }
    }

    /// Close the current result set: finish a JSON array, draw a held
    /// table.
    fn finish_set(&mut self) -> SqawkResult<()> {
        match self.settings.format {
            Format::Json => {
                let close: &[u8] = if self.rows == 0 { b"]\n" } else { b"\n]\n" };
                self.put(close)
            }
            Format::Markdown => {
                let text = render_markdown(&self.names, &self.held);
                self.held.clear();
                self.put(text.as_bytes())
            }
            Format::Table => {
                let text = render_table(&self.names, &self.held, self.settings.header);
                self.held.clear();
                self.put(text.as_bytes())
            }
            Format::Csv | Format::Tsv | Format::Jsonl => Ok(()),
        }
    }
}

impl ResultSink for StdoutSink {
    fn begin(&mut self, columns: &[ColumnDefinition]) -> SqawkResult<()> {
        self.rows = 0;
        self.result_sets += 1;
        self.names = columns.iter().map(|c| c.name.clone()).collect();
        self.held.clear();
        // Best-effort output tries again for each result set.
        if self.ignore_errors {
            self.open = true;
        }
        if !self.open {
            return Ok(());
        }
        self.out = Some(BufWriter::with_capacity(STDOUT_BUFFER, std::io::stdout()));
        match self.settings.format {
            Format::Csv | Format::Tsv if self.settings.header => {
                let mut rec = Vec::new();
                let result = self
                    .writer
                    .write(&mut rec, columns.iter().map(|c| c.name.as_str()));
                self.check(result)?;
                self.put(&rec)
            }
            Format::Json => self.put(b"["),
            _ => Ok(()),
        }
    }

    fn row(&mut self, values: &[Register]) -> SqawkResult<()> {
        self.rows += 1;
        if self.out.is_none() {
            return Ok(());
        }
        match self.settings.format {
            Format::Csv | Format::Tsv => {
                let fields: Vec<_> = values.iter().map(|v| self.text(v)).collect();
                let mut rec = Vec::new();
                let result = self
                    .writer
                    .write(&mut rec, fields.iter().map(|f| f.as_ref()));
                self.check(result)?;
                self.put(&rec)
            }
            Format::Json | Format::Jsonl => {
                let mut obj = String::new();
                if self.settings.format == Format::Json {
                    obj.push_str(if self.rows == 1 { "\n  " } else { ",\n  " });
                }
                json_object(&mut obj, &self.names, values);
                if self.settings.format == Format::Jsonl {
                    obj.push('\n');
                }
                self.put(obj.as_bytes())
            }
            Format::Markdown | Format::Table => {
                let cells = values
                    .iter()
                    .map(|v| Cell {
                        text: escape_cell(&self.text(v)),
                        number: matches!(v, Register::Integer(_) | Register::Float(_)),
                        null: matches!(v, Register::Null),
                    })
                    .collect();
                self.held.push(cells);
                Ok(())
            }
        }
    }

    fn end(&mut self) -> SqawkResult<()> {
        let result = self.finish_set().and_then(|()| self.flush());
        if self.verbose {
            diag!("Query returned {} rows", self.rows);
        }
        result
    }

    fn abort(&mut self) {
        // The rows already produced are printed before the error is.
        let _ = self.finish_set();
        let _ = self.flush();
    }
}

/// `v` as a JSON value: numbers stay numbers (a non-finite real is the
/// string sqawk prints for it), NULL is null.
fn json_value(out: &mut String, v: &Register) {
    match v {
        Register::Null => out.push_str("null"),
        Register::Integer(i) => out.push_str(&i.to_string()),
        Register::Float(f) if f.is_finite() => out.push_str(&crate::vm::arith::real_text(*f)),
        Register::Boolean(b) => out.push_str(if *b { "true" } else { "false" }),
        other => json_string(out, &other.field_str()),
    }
}

fn json_string(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

fn json_object(out: &mut String, names: &[String], values: &[Register]) {
    out.push('{');
    for (i, (name, v)) in names.iter().zip(values).enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        json_string(out, name);
        out.push_str(": ");
        json_value(out, v);
    }
    out.push('}');
}

/// A cell's text on one line: line breaks and tabs shown as `\n`, `\r`,
/// `\t`.
fn escape_cell(s: &str) -> String {
    if !s.contains(['\n', '\r', '\t']) {
        return s.to_string();
    }
    s.replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}

fn width(s: &str) -> usize {
    s.chars().count()
}

/// Column widths over the header and every held row.
fn widths(names: &[String], rows: &[Vec<Cell>], header: bool) -> Vec<usize> {
    (0..names.len())
        .map(|i| {
            let head = if header { width(&names[i]) } else { 0 };
            rows.iter()
                .filter_map(|r| r.get(i))
                .map(|c| width(&c.text))
                .fold(head, usize::max)
        })
        .collect()
}

fn pad(out: &mut String, text: &str, w: usize, right: bool) {
    let fill = w.saturating_sub(width(text));
    if right {
        out.extend(std::iter::repeat_n(' ', fill));
        out.push_str(text);
    } else {
        out.push_str(text);
        out.extend(std::iter::repeat_n(' ', fill));
    }
}

/// A box-drawn table; numbers right-aligned.
fn render_table(names: &[String], rows: &[Vec<Cell>], header: bool) -> String {
    let ws = widths(names, rows, header);
    let rule = |l: char, m: char, r: char| {
        let mut s = String::new();
        s.push(l);
        for (i, w) in ws.iter().enumerate() {
            if i > 0 {
                s.push(m);
            }
            s.extend(std::iter::repeat_n('─', w + 2));
        }
        s.push(r);
        s.push('\n');
        s
    };
    let mut out = rule('┌', '┬', '┐');
    if header {
        out.push('│');
        for (name, w) in names.iter().zip(&ws) {
            out.push(' ');
            pad(&mut out, &escape_cell(name), *w, false);
            out.push_str(" │");
        }
        out.push('\n');
        out.push_str(&rule('├', '┼', '┤'));
    }
    for row in rows {
        out.push('│');
        for (c, w) in row.iter().zip(&ws) {
            out.push(' ');
            pad(&mut out, &c.text, *w, c.number);
            out.push_str(" │");
        }
        out.push('\n');
    }
    out.push_str(&rule('└', '┴', '┘'));
    out
}

/// A Markdown table: a column whose non-NULL values are all numbers (and
/// has some) is right-aligned (`---:`); `|` in a value is escaped.
fn render_markdown(names: &[String], rows: &[Vec<Cell>]) -> String {
    let esc = |s: &str| s.replace('|', "\\|");
    let names: Vec<String> = names.iter().map(|n| esc(&escape_cell(n))).collect();
    let rows: Vec<Vec<Cell>> = rows
        .iter()
        .map(|r| {
            r.iter()
                .map(|c| Cell {
                    text: esc(&c.text),
                    number: c.number,
                    null: c.null,
                })
                .collect()
        })
        .collect();
    let ws: Vec<usize> = widths(&names, &rows, true)
        .iter()
        .map(|w| (*w).max(3))
        .collect();
    let numeric: Vec<bool> = (0..names.len())
        .map(|i| {
            let cells = || rows.iter().filter_map(|r| r.get(i)).filter(|c| !c.null);
            cells().next().is_some() && cells().all(|c| c.number)
        })
        .collect();
    let mut out = String::from("|");
    for (n, w) in names.iter().zip(&ws) {
        out.push(' ');
        pad(&mut out, n, *w, false);
        out.push_str(" |");
    }
    out.push_str("\n|");
    for (w, num) in ws.iter().zip(&numeric) {
        out.push(' ');
        if *num {
            out.extend(std::iter::repeat_n('-', w - 1));
            out.push(':');
        } else {
            out.extend(std::iter::repeat_n('-', *w));
        }
        out.push_str(" |");
    }
    out.push('\n');
    for row in &rows {
        out.push('|');
        for (c, (w, num)) in row.iter().zip(ws.iter().zip(&numeric)) {
            out.push(' ');
            pad(&mut out, &c.text, *w, *num);
            out.push_str(" |");
        }
        out.push('\n');
    }
    out
}
