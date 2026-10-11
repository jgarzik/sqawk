//! Delimited file reading and writing for sqawk
//!
//! Every input -- `.csv`, TSV, `-F`-delimited files and standard input -- is
//! parsed here by the `csv` crate with one configuration: RFC 4180 quoting
//! (`""` inside a quoted field is a literal quote), quoted fields may span
//! lines, fields are never trimmed, a UTF-8 byte order mark is stripped, and
//! rows may be ragged (short rows are padded with NULL).
//!
//! Writeback goes through the same module, so that a file read and written
//! back without changes is byte-identical: cells are inferred with a rule
//! that round-trips (see `Value::from(&str)`), NULL is the empty field, and
//! the header/line-ending/comment layout of the source is recorded on the
//! table and reproduced.

use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use tempfile::NamedTempFile;

use crate::capacity::{generate_alpha_columns, is_likely_data_row};
use crate::error::{SqawkError, SqawkResult};
use crate::table::{FileLayout, Table, Value};

/// How the first record of a file is treated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeaderMode {
    /// The first record is the header (the rule for `.csv` and stdin).
    Present,
    /// Every record is data; columns are named a, b, c, ... unless given.
    Absent,
    /// Guess from the first record (`is_likely_data_row`); used for
    /// non-.csv delimited files such as /etc/passwd.
    Guess,
}

/// Options for reading one delimited file.
#[derive(Debug, Clone)]
pub struct ReadOptions {
    /// Field separator byte
    pub delimiter: u8,
    /// Header policy
    pub header: HeaderMode,
    /// Skip lines that start with `#`
    pub comments: bool,
    /// Column names to use; implies the file has no header row
    pub columns: Option<Vec<String>>,
    /// Skip or repair malformed records instead of failing
    pub recover_errors: bool,
}

impl Default for ReadOptions {
    /// Plain CSV: comma, header row, no comment lines.
    fn default() -> Self {
        ReadOptions {
            delimiter: b',',
            header: HeaderMode::Present,
            comments: false,
            columns: None,
            recover_errors: false,
        }
    }
}

/// UTF-8 byte order mark
const BOM: &[u8] = b"\xef\xbb\xbf";

/// Field bytes to text. Invalid UTF-8 is replaced rather than rejected.
fn field_text(bytes: &[u8]) -> std::borrow::Cow<'_, str> {
    String::from_utf8_lossy(bytes)
}

/// Parse delimited `data` into a table.
///
/// # Arguments
/// * `table_name` - name of the resulting table
/// * `data` - the whole file contents
/// * `file_path` - writeback target recorded on the table (None for stdin)
/// * `delimiter` - the delimiter string recorded on the table
/// * `opts` - parsing options
/// * `source` - file name used in error messages
pub fn read_table(
    table_name: &str,
    data: &[u8],
    file_path: Option<PathBuf>,
    delimiter: String,
    opts: &ReadOptions,
    source: &str,
) -> SqawkResult<Table> {
    let data = data.strip_prefix(BOM).unwrap_or(data);

    // Line ending of the first record decides the writeback terminator.
    let crlf = data
        .iter()
        .position(|&b| b == b'\n')
        .is_some_and(|i| i > 0 && data[i - 1] == b'\r');

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .delimiter(opts.delimiter)
        .comment(if opts.comments { Some(b'#') } else { None })
        .from_reader(data);

    let mut record = csv::ByteRecord::new();
    let mut line = 0usize;

    // First record: the header, or the first data row.
    let mut pending_first: Option<Vec<Value>> = None;
    let has_first = read_record(&mut reader, &mut record, source, &mut line)?;
    let header_mode = if opts.columns.is_some() {
        HeaderMode::Absent
    } else {
        opts.header
    };
    let (columns, has_header) = if !has_first {
        (opts.columns.clone().unwrap_or_default(), false)
    } else {
        let fields: Vec<String> = record.iter().map(|f| field_text(f).into_owned()).collect();
        let is_header = match header_mode {
            HeaderMode::Present => true,
            HeaderMode::Absent => false,
            HeaderMode::Guess => !is_likely_data_row(&fields),
        };
        if is_header {
            (fields, true)
        } else {
            let cols = opts
                .columns
                .clone()
                .unwrap_or_else(|| generate_alpha_columns(fields.len()));
            pending_first = Some(fields.into_iter().map(Value::from).collect());
            (cols, false)
        }
    };

    let mut table = Table::new_with_delimiter(table_name, columns, file_path, delimiter);
    table.set_file_layout(FileLayout {
        has_header,
        crlf,
        comments: opts.comments,
    });
    let width = table.column_count();
    let mut sink = RowSink {
        rows: Vec::new(),
        width,
        skipped: 0,
        recover: opts.recover_errors,
        source,
    };
    if let Some(first) = pending_first {
        sink.push(first, line)?;
    }

    loop {
        match read_record(&mut reader, &mut record, source, &mut line) {
            Ok(true) => {
                let row: Vec<Value> = record
                    .iter()
                    .map(|f| match std::str::from_utf8(f) {
                        Ok(s) => Value::from(s),
                        Err(_) => Value::from(field_text(f).into_owned()),
                    })
                    .collect();
                sink.push(row, line)?;
            }
            Ok(false) => break,
            Err(e) if opts.recover_errors => {
                sink.skipped += 1;
                eprintln!("Warning: Skipping malformed row: {}", e);
            }
            Err(e) => return Err(e),
        }
    }

    if sink.skipped > 0 {
        eprintln!(
            "Note: Skipped {} malformed rows while loading {}",
            sink.skipped, source
        );
    }

    // Loading is not a modification.
    table.replace_rows(sink.rows);
    table.mark_unmodified();
    Ok(table)
}

/// Collects loaded rows, enforcing the table width.
struct RowSink<'a> {
    rows: Vec<Vec<Value>>,
    width: usize,
    skipped: usize,
    recover: bool,
    source: &'a str,
}

impl RowSink<'_> {
    fn push(&mut self, mut row: Vec<Value>, line: usize) -> SqawkResult<()> {
        if row.len() > self.width {
            if self.recover {
                self.skipped += 1;
                eprintln!(
                    "Warning: Skipping row at line {} of {}: {} fields, expected {}",
                    line,
                    self.source,
                    row.len(),
                    self.width
                );
                return Ok(());
            }
            return Err(SqawkError::CsvParseError {
                file: self.source.to_string(),
                line,
                error: format!(
                    "record has {} fields, but the table has {} columns",
                    row.len(),
                    self.width
                ),
            });
        }
        // A short record is padded: the missing trailing fields are NULL.
        row.resize(self.width, Value::Null);
        self.rows.push(row);
        Ok(())
    }
}

/// Read the next record; false at end of input. `line` tracks the 1-based
/// line where the record starts, for error messages.
fn read_record(
    reader: &mut csv::Reader<&[u8]>,
    record: &mut csv::ByteRecord,
    source: &str,
    line: &mut usize,
) -> SqawkResult<bool> {
    match reader.read_byte_record(record) {
        Ok(more) => {
            if let Some(pos) = record.position() {
                *line = pos.line() as usize;
            }
            Ok(more)
        }
        Err(e) => Err(SqawkError::CsvParseError {
            file: source.to_string(),
            line: e.position().map_or(*line + 1, |p| p.line() as usize),
            error: e.to_string(),
        }),
    }
}

/// Write `table` back to `file_path`, reproducing the source layout.
///
/// The write is atomic: a temp file in the target's directory is written,
/// synced, given the original file's permissions, and renamed over the
/// target. A symlink is resolved first, so the link survives and its target
/// is what gets rewritten.
pub fn save_table(table: &Table, file_path: &Path) -> SqawkResult<()> {
    let target = match std::fs::canonicalize(file_path) {
        Ok(real) => real,
        // A file that does not exist yet (CREATE TABLE ... LOCATION).
        Err(_) => file_path.to_path_buf(),
    };
    let parent = match target.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    let temp_file = NamedTempFile::new_in(parent)?;

    {
        let mut out = BufWriter::new(temp_file.as_file());
        let layout = table.file_layout();
        table.write_delimited(&mut out, layout.has_header, layout.crlf, layout.comments)?;
        out.flush()?;
    }

    if let Ok(meta) = std::fs::metadata(&target) {
        temp_file.as_file().set_permissions(meta.permissions())?;
    }
    temp_file.as_file().sync_all()?;
    temp_file
        .persist(&target)
        .map_err(|e| SqawkError::IoError(e.error))?;
    Ok(())
}

/// Loads and saves CSV files (library convenience API).
#[allow(dead_code)] // not used by the binary; part of the library API
pub struct CsvHandler {}

impl Default for CsvHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(dead_code)]
impl CsvHandler {
    /// Create a new CsvHandler
    pub fn new() -> Self {
        CsvHandler {}
    }

    /// Save a table to a CSV file (see `save_table`)
    pub fn save_csv(&self, table: &Table, file_path: &Path) -> SqawkResult<()> {
        save_table(table, file_path)
    }

    /// Load a CSV file into a table.
    ///
    /// * `file_spec` - `[table_name=]file_path`
    /// * `custom_columns` - column names to use; the file then has no header
    ///   row and its first line is data
    /// * `recover_errors` - skip malformed records instead of failing
    ///
    /// The first line is the header unless `custom_columns` is given. `#`
    /// lines are data; use `load_with_options` for comment skipping.
    pub fn load_csv(
        &self,
        file_spec: &str,
        custom_columns: Option<Vec<String>>,
        recover_errors: Option<bool>,
    ) -> SqawkResult<Table> {
        let opts = ReadOptions {
            columns: custom_columns,
            recover_errors: recover_errors.unwrap_or(false),
            ..ReadOptions::default()
        };
        self.load_with_options(file_spec, &opts)
    }

    /// Load a delimited file with explicit options.
    pub fn load_with_options(&self, file_spec: &str, opts: &ReadOptions) -> SqawkResult<Table> {
        let (table_name, file_path) = parse_file_spec(file_spec)?;
        let data = std::fs::read(&file_path)?;
        let delimiter = (opts.delimiter as char).to_string();
        read_table(
            &table_name,
            &data,
            Some(file_path.clone()),
            delimiter,
            opts,
            &file_path.to_string_lossy(),
        )
    }
}

/// Split `[table_name=]file_path`; the table name defaults to the file stem.
#[allow(dead_code)]
fn parse_file_spec(file_spec: &str) -> SqawkResult<(String, PathBuf)> {
    if let Some((table_name, file_path)) = file_spec.split_once('=') {
        return Ok((table_name.to_string(), PathBuf::from(file_path)));
    }
    let path = PathBuf::from(file_spec);
    let stem = path
        .file_stem()
        .ok_or_else(|| {
            SqawkError::InvalidFileSpec(format!("Invalid file specification: {}", file_spec))
        })?
        .to_string_lossy()
        .to_string();
    Ok((stem, path))
}
