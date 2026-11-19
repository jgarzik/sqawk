//! Delimited file reading and writing for sqawk
//!
//! Every input -- `.csv`, `.tsv`, `-F`-delimited files and standard input --
//! is parsed here by the `csv` crate with one configuration: RFC 4180 quoting
//! (`""` inside a quoted field is a literal quote), quoted fields may span
//! lines, fields are never trimmed, and rows may be ragged (short rows are
//! padded with NULL). A quote that is never closed is an error.
//!
//! Writeback goes through the same module. Each table keeps the bytes it was
//! read from, so every row no statement changed is written back exactly as
//! it was read (quoting, padding, line ending, invalid UTF-8 and all), as are
//! the byte order mark, the header, blank lines and `#` comment lines (see
//! `RawRows` and `CommentLines`). Rows that were inserted or changed are
//! written with RFC 4180 quoting where a field needs it.

use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use tempfile::NamedTempFile;

use crate::capacity::generate_alpha_columns;
use crate::error::{SqawkError, SqawkResult};
use crate::table::{CommentLines, FileLayout, RawRows, RawSpan, Table, Value};

/// Options for reading one delimited file.
#[derive(Debug, Clone)]
pub struct ReadOptions {
    /// Field separator byte
    pub delimiter: u8,
    /// The first record is the header. When false, every record is data and
    /// the columns are named a, b, c, ... unless `columns` names them.
    /// When true and `columns` is given, the header is skipped as data (and
    /// kept for writeback) but `columns` names the columns; the header must
    /// have exactly one field per column.
    pub header: bool,
    /// Skip lines that start with `#` (`--comments`)
    pub comments: bool,
    /// Column names to use instead of the header's (see `header`)
    pub columns: Option<Vec<String>>,
    /// Keep the file's bytes for byte-for-byte writeback (see `RawRows`).
    /// False when the run can never write the table back (no `--write`, not
    /// the REPL, or standard input): the bytes would only cost memory.
    pub keep_source: bool,
}

impl Default for ReadOptions {
    /// Plain CSV: comma, header row, no comment lines.
    fn default() -> Self {
        ReadOptions {
            delimiter: b',',
            header: true,
            comments: false,
            columns: None,
            keep_source: true,
        }
    }
}

/// UTF-8 byte order mark
const BOM: &[u8] = b"\xef\xbb\xbf";

/// Field bytes to text. Invalid UTF-8 is replaced rather than rejected (the
/// row's source bytes are kept, so an untouched row is written back as read).
fn field_text(bytes: &[u8]) -> std::borrow::Cow<'_, str> {
    String::from_utf8_lossy(bytes)
}

/// Parse delimited `data` (a whole file) into a table.
///
/// Under `opts.keep_source` the table keeps `data` itself: every record's
/// source bytes (see `RawRows`), so writeback reproduces the rows no
/// statement changed byte-for-byte.
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
    data: Vec<u8>,
    file_path: Option<PathBuf>,
    delimiter: String,
    opts: &ReadOptions,
    source: &str,
) -> SqawkResult<Table> {
    parse(
        Feed::whole(data),
        table_name,
        file_path,
        delimiter,
        opts,
        source,
    )
}

/// [`read_table`] reading from `input`. Without `opts.keep_source` the
/// input is parsed as it is read, holding only the bytes of the record at
/// hand rather than the whole file; rows, line numbers and errors are the
/// same either way. An I/O error is reported as opening `io_name`.
pub fn read_table_from<R: Read>(
    table_name: &str,
    mut input: R,
    io_name: &Path,
    file_path: Option<PathBuf>,
    delimiter: String,
    opts: &ReadOptions,
    source: &str,
) -> SqawkResult<Table> {
    if opts.keep_source {
        let mut data = Vec::new();
        input
            .read_to_end(&mut data)
            .map_err(|e| SqawkError::open(io_name, e))?;
        return read_table(table_name, data, file_path, delimiter, opts, source);
    }
    let feed = Feed::stream(input, io_name).map_err(|e| SqawkError::open(io_name, e))?;
    parse(feed, table_name, file_path, delimiter, opts, source)
}

/// The one parse loop behind [`read_table`] and [`read_table_from`].
fn parse<R: Read>(
    feed: Feed<R>,
    table_name: &str,
    file_path: Option<PathBuf>,
    delimiter: String,
    opts: &ReadOptions,
    source: &str,
) -> SqawkResult<Table> {
    let bom = feed.bom;
    // Offset of the body within the buffer the spans index (the BOM).
    let lead = feed.lead;
    let keep = opts.keep_source;

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .delimiter(opts.delimiter)
        .comment(if opts.comments { Some(b'#') } else { None })
        .from_reader(feed);

    let header = opts.header;
    let mut record = csv::ByteRecord::new();
    let mut lines = LineCounter::default();
    // The bytes between records: blank lines, and `#` lines under
    // --comments. Kept verbatim, anchored to the record that follows.
    let mut gaps = CommentLines::default();
    let mut header_span = RawSpan::NONE;
    let mut spans: Vec<RawSpan> = Vec::new();
    let mut rows: Vec<Vec<Value>> = Vec::new();
    let mut columns: Option<Vec<String>> = None;
    let mut width = 0;
    // Where the previous record's bytes end.
    let mut prev_end = 0usize;
    // The last record's span within the body.
    let mut last: Option<(usize, usize)> = None;

    loop {
        let more = read_record(&mut reader, &mut record, source, &mut lines)?;
        let from = record.position().map_or(prev_end, |p| p.byte() as usize);
        let read_to = reader.position().byte() as usize;
        let feed = reader.get_mut();
        let from = from.clamp(prev_end, feed.end());
        let start = feed.record_start(from, opts.comments);
        // A comment line that ends the input without a line ending comes
        // back from the csv crate as an empty record; it is not a row.
        if !more || feed.past_end(start) {
            if keep {
                gaps.tail = feed.slice(prev_end, feed.end()).to_vec();
            }
            break;
        }
        let mut end = read_to.clamp(start, feed.end());
        // csv-core ends a CRLF record at the CR; the LF belongs to it too.
        if end > start && feed.byte(end - 1) == Some(b'\r') && feed.byte(end) == Some(b'\n') {
            end += 1;
        }
        let line = feed.line_at(&mut lines, start);
        let gap = if keep {
            feed.slice(prev_end, start).to_vec()
        } else {
            Vec::new()
        };
        // Nothing before this record is needed again.
        feed.trim(start);
        prev_end = end;
        last = Some((start, end));
        let span = RawSpan {
            start: lead + start,
            end: lead + end,
        };

        let Some(cols) = &columns else {
            // The first record: the header, or the first row.
            gaps.head = gap;
            let cols = if header {
                header_span = span;
                match &opts.columns {
                    None => header_names(record.iter().map(|f| field_text(f).into_owned())),
                    Some(cols) if cols.len() == record.len() => cols.clone(),
                    Some(cols) => {
                        return Err(SqawkError::CsvParseError {
                            file: source.to_string(),
                            line,
                            error: format!(
                                "header has {} fields, but the table declares {} columns",
                                record.len(),
                                cols.len()
                            ),
                        });
                    }
                }
            } else {
                opts.columns
                    .clone()
                    .unwrap_or_else(|| generate_alpha_columns(record.len()))
            };
            check_column_names(&cols, source, line)?;
            width = cols.len();
            columns = Some(cols);
            if header {
                continue;
            }
            push_row(&mut rows, &record, width, source, line)?;
            if keep {
                spans.push(span);
            }
            continue;
        };
        debug_assert_eq!(cols.len(), width);
        if !gap.is_empty() {
            gaps.before.entry(rows.len()).or_default().extend(gap);
        }
        push_row(&mut rows, &record, width, source, line)?;
        if keep {
            spans.push(span);
        }
    }

    let mut feed = reader.into_inner();
    if let Some(e) = feed.io_error.take() {
        return Err(SqawkError::open(&feed.io_name, e));
    }

    // csv-core takes a quote that is never closed as running to the end of
    // the input, so the rest of the file would load as one field.
    if let Some((start, end)) = last
        && let Some(quote) = unclosed_quote(feed.slice(start, end), opts.delimiter)
    {
        return Err(SqawkError::CsvParseError {
            file: source.to_string(),
            line: feed.line_at(&mut lines, start + quote),
            error: "unterminated quoted field".to_string(),
        });
    }

    let columns = match columns {
        Some(cols) => cols,
        // No records at all (empty input, or only blank/comment lines).
        None => {
            let cols = opts.columns.clone().unwrap_or_default();
            check_column_names(&cols, source, 1)?;
            gaps.head = std::mem::take(&mut gaps.tail);
            cols
        }
    };

    let crlf = feed.crlf.unwrap_or(false);
    let mut table = Table::new_with_delimiter(table_name, columns, file_path, delimiter);
    table.set_file_layout(FileLayout {
        // A file with no header line yet gets one of the given column names.
        has_header: header && (header_span != RawSpan::NONE || opts.columns.is_some()),
        bom,
        crlf,
        comments: opts.comments,
    });
    // Loading is not a modification.
    table.replace_rows(rows);
    if keep {
        table.set_raw(RawRows {
            buf: feed.buf,
            header: header_span,
            rows: spans,
        });
        table.set_comment_lines(gaps);
    } else {
        table.mark_source_dropped();
    }
    table.mark_unmodified();
    Ok(table)
}

/// Column names from a file's header line, named as sqlite3 names such
/// columns: an empty name becomes `columnN` (its 1-based position) and a
/// repeated one (case-insensitively) gets `:1`, `:2`, ... as the columns of
/// a derived table do. Only the table's names change; the header line is
/// written back as it was.
fn header_names(fields: impl Iterator<Item = String>) -> Vec<String> {
    let mut names: Vec<String> = fields
        .enumerate()
        .map(|(i, f)| {
            if f.is_empty() {
                format!("column{}", i + 1)
            } else {
                f
            }
        })
        .collect();
    crate::table::unique_column_names(&mut names);
    names
}

/// Reject a declared column list (`--tabledef`, LOCATION) with an empty or
/// repeated name. Names are case-insensitive, so `a` and `A` are the same
/// column.
fn check_column_names(cols: &[String], source: &str, line: usize) -> SqawkResult<()> {
    let err = |error: String| SqawkError::CsvParseError {
        file: source.to_string(),
        line,
        error,
    };
    if let Some(i) = cols.iter().position(String::is_empty) {
        return Err(err(format!("column {} has an empty name", i + 1)));
    }
    let mut seen = std::collections::HashSet::with_capacity(cols.len());
    let mut reported = std::collections::HashSet::new();
    let mut dups: Vec<&str> = Vec::new();
    for name in cols {
        let key = name.to_ascii_lowercase();
        if seen.contains(&key) {
            if reported.insert(key) {
                dups.push(name);
            }
        } else {
            seen.insert(key);
        }
    }
    if !dups.is_empty() {
        return Err(err(format!("duplicate column name: {}", dups.join(", "))));
    }
    Ok(())
}

/// Append `record` as a row of `width` columns. A short record is padded
/// with NULL; a long one is an error.
fn push_row(
    rows: &mut Vec<Vec<Value>>,
    record: &csv::ByteRecord,
    width: usize,
    source: &str,
    line: usize,
) -> SqawkResult<()> {
    if record.len() > width {
        return Err(SqawkError::CsvParseError {
            file: source.to_string(),
            line,
            error: format!(
                "record has {} fields, but the table has {} columns",
                record.len(),
                width
            ),
        });
    }
    let mut row: Vec<Value> = Vec::with_capacity(width);
    row.extend(record.iter().map(|f| match std::str::from_utf8(f) {
        Ok(s) => Value::from(s),
        Err(_) => Value::from(field_text(f).into_owned()),
    }));
    // A short record is padded: the missing trailing fields are NULL.
    row.resize(width, Value::Null);
    rows.push(row);
    Ok(())
}

/// If `rec` (one record's bytes) ends inside a quoted field, the offset of
/// that field's opening quote. Mirrors csv-core's quoting: a quote opens a
/// quoted field only at the start of a field, `""` inside one is a literal
/// quote, and after the closing quote the field continues unquoted up to the
/// delimiter.
fn unclosed_quote(rec: &[u8], delimiter: u8) -> Option<usize> {
    #[derive(PartialEq)]
    enum S {
        FieldStart,
        Unquoted,
        Quoted,
        QuoteInQuoted,
    }
    let mut state = S::FieldStart;
    let mut opened = 0;
    for (i, &b) in rec.iter().enumerate() {
        state = match state {
            S::FieldStart if b == b'"' => {
                opened = i;
                S::Quoted
            }
            S::FieldStart | S::Unquoted | S::QuoteInQuoted if b == delimiter => S::FieldStart,
            S::FieldStart | S::Unquoted | S::QuoteInQuoted if b == b'\r' || b == b'\n' => {
                S::FieldStart
            }
            S::FieldStart | S::Unquoted => S::Unquoted,
            S::Quoted if b == b'"' => S::QuoteInQuoted,
            S::Quoted => S::Quoted,
            S::QuoteInQuoted if b == b'"' => S::Quoted,
            S::QuoteInQuoted => S::Unquoted,
        };
    }
    (state == S::Quoted).then_some(opened)
}

/// Where [`Feed::line_at`] has counted lines up to.
///
/// The line is counted from the bytes themselves, not taken from csv-core's
/// record positions: csv-core ends a CRLF record at the CR and counts its LF
/// only when the next record is read, so its line numbers lag by one in CRLF
/// files. A line ends at LF, at CRLF, or at a CR not followed by LF (the
/// three terminators the csv crate accepts), so every line ending convention
/// and any mix of them number lines alike. Offsets are asked for in
/// increasing order, so the count resumes where the last one stopped.
#[derive(Default)]
struct LineCounter {
    /// Body offset counted up to
    pos: usize,
    /// Line breaks before `pos`
    breaks: usize,
}

/// How many bytes [`Feed`] reads from its input at a time (one under test,
/// so that every byte is a chunk boundary).
const FEED_CHUNK: u64 = if cfg!(test) { 1 } else { 64 * 1024 };

/// How many bytes before the current record a streaming [`Feed`] lets
/// accumulate before dropping them (dropping moves the rest down).
const FEED_TRIM_AT: usize = if cfg!(test) { 1 } else { 64 * 1024 };

/// The input as the csv reader sees it (the file body, after any byte order
/// mark), plus the bytes it has passed that the loader still needs.
///
/// Built from a whole file ([`Feed::whole`]) it holds every byte. Built
/// from a stream ([`Feed::stream`]) it holds a window: bytes are read in
/// chunks as the csv reader or a lookahead asks for them, and everything
/// before the current record is dropped. Offsets are body offsets either
/// way, and every byte the loader looks at is read first, so both give the
/// same rows, line numbers and errors.
struct Feed<R> {
    input: R,
    /// The window: body offset `base` is at `buf[lead]`.
    buf: Vec<u8>,
    /// Bytes at the front of `buf` that are not body (a whole file's BOM).
    lead: usize,
    base: usize,
    /// Body offset handed to the csv reader so far.
    served: usize,
    /// `input` has ended (or failed).
    eof: bool,
    /// The window is never trimmed (a whole file).
    whole: bool,
    /// The body began with a UTF-8 byte order mark.
    bom: bool,
    /// Whether the first line break of the body is CRLF, once one is seen.
    crlf: Option<bool>,
    /// A read error, reported (as opening `io_name`) in place of a parse
    /// error.
    io_error: Option<std::io::Error>,
    io_name: PathBuf,
}

impl Feed<std::io::Empty> {
    fn whole(data: Vec<u8>) -> Self {
        let bom = data.starts_with(BOM);
        let lead = if bom { BOM.len() } else { 0 };
        let mut feed = Feed {
            input: std::io::empty(),
            buf: data,
            lead,
            base: 0,
            served: 0,
            eof: true,
            whole: true,
            bom,
            crlf: None,
            io_error: None,
            io_name: PathBuf::new(),
        };
        feed.note_line_break(lead);
        feed
    }
}

impl<R: Read> Feed<R> {
    /// A feed over `input`, its byte order mark (if any) already read.
    fn stream(input: R, io_name: &Path) -> std::io::Result<Self> {
        let mut feed = Feed {
            input,
            buf: Vec::new(),
            lead: 0,
            base: 0,
            served: 0,
            eof: false,
            whole: false,
            bom: false,
            crlf: None,
            io_error: None,
            io_name: io_name.to_path_buf(),
        };
        while feed.buf.len() < BOM.len() && !feed.eof {
            feed.read_chunk()?;
        }
        if feed.buf.starts_with(BOM) {
            feed.buf.drain(..BOM.len());
            feed.bom = true;
        }
        feed.note_line_break(0);
        Ok(feed)
    }

    /// Body offset just past the window.
    fn end(&self) -> usize {
        self.base + self.buf.len() - self.lead
    }

    fn idx(&self, offset: usize) -> usize {
        offset - self.base + self.lead
    }

    /// Append the next chunk of input to the window: a whole chunk, or
    /// everything up to the end of input.
    fn read_chunk(&mut self) -> std::io::Result<()> {
        let n = (&mut self.input)
            .take(FEED_CHUNK)
            .read_to_end(&mut self.buf)?;
        self.eof = (n as u64) < FEED_CHUNK;
        Ok(())
    }

    /// Read until the window reaches body offset `offset` or the input
    /// ends. A read error ends the input; it is reported later.
    fn ensure(&mut self, offset: usize) {
        while self.end() < offset && !self.eof {
            let old = self.buf.len();
            if let Err(e) = self.read_chunk() {
                self.io_error = Some(e);
                self.eof = true;
            }
            self.note_line_break(old);
        }
    }

    /// Record whether the body's first line break is CRLF, looking from
    /// buffer index `from`.
    fn note_line_break(&mut self, from: usize) {
        if self.crlf.is_some() {
            return;
        }
        if let Some(i) = self.buf[from..].iter().position(|&b| b == b'\n') {
            let i = from + i;
            self.crlf = Some(i > self.lead && self.buf[i - 1] == b'\r');
        }
    }

    /// The byte at body offset `offset`, None past the end of input.
    fn byte(&mut self, offset: usize) -> Option<u8> {
        self.ensure(offset + 1);
        if offset < self.end() {
            Some(self.buf[self.idx(offset)])
        } else {
            None
        }
    }

    /// Whether `offset` is at or past the end of input.
    fn past_end(&mut self, offset: usize) -> bool {
        self.byte(offset).is_none()
    }

    /// The bytes at body offsets `from..to` (both within the window).
    fn slice(&self, from: usize, to: usize) -> &[u8] {
        &self.buf[self.idx(from)..self.idx(to)]
    }

    /// Drop the bytes before body offset `offset` (a streaming feed only,
    /// and only once the first line break is known).
    fn trim(&mut self, offset: usize) {
        if self.whole || self.crlf.is_none() || offset - self.base < FEED_TRIM_AT {
            return;
        }
        let k = self.idx(offset);
        self.buf.drain(..k);
        self.base = offset;
    }

    /// Where the record the csv crate reads from `from` begins: past the
    /// blank lines (CR and LF bytes) and, with `comments`, the `#` lines
    /// (each running to its LF) that csv-core skips first.
    fn record_start(&mut self, from: usize, comments: bool) -> usize {
        let mut i = from;
        while let Some(b) = self.byte(i) {
            match b {
                b'\r' | b'\n' => i += 1,
                b'#' if comments => {
                    i += 1;
                    loop {
                        match self.byte(i) {
                            Some(b'\n') => {
                                i += 1;
                                break;
                            }
                            Some(_) => i += 1,
                            None => break,
                        }
                    }
                }
                _ => break,
            }
        }
        i
    }

    /// The 1-based line `offset` is on, counting on from `lines`.
    fn line_at(&mut self, lines: &mut LineCounter, offset: usize) -> usize {
        // One byte past `offset` decides whether a CR before it is a CRLF.
        self.ensure(offset + 1);
        let offset = offset.min(self.end());
        if offset < lines.pos {
            if self.base > 0 {
                // The bytes before the window are gone (offsets come in
                // increasing order, so this is not reached).
                return 1 + lines.breaks;
            }
            *lines = LineCounter::default();
        }
        let buf = &self.buf;
        let (from, to) = (self.idx(lines.pos), self.idx(offset));
        lines.breaks += (from..to)
            .filter(|&i| match buf[i] {
                b'\n' => true,
                b'\r' => buf.get(i + 1) != Some(&b'\n'),
                _ => false,
            })
            .count();
        lines.pos = offset;
        1 + lines.breaks
    }
}

impl<R: Read> Read for Feed<R> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        self.ensure(self.served + 1);
        if self.io_error.is_some() {
            return Err(std::io::Error::other("read error"));
        }
        let avail = &self.buf[self.idx(self.served)..];
        let n = avail.len().min(out.len());
        out[..n].copy_from_slice(&avail[..n]);
        self.served += n;
        Ok(n)
    }
}

/// Read the next record; false at end of input.
fn read_record<R: Read>(
    reader: &mut csv::Reader<Feed<R>>,
    record: &mut csv::ByteRecord,
    source: &str,
    lines: &mut LineCounter,
) -> SqawkResult<bool> {
    match reader.read_byte_record(record) {
        Ok(more) => Ok(more),
        Err(e) => {
            let feed = reader.get_mut();
            if let Some(io) = feed.io_error.take() {
                return Err(SqawkError::open(&feed.io_name, io));
            }
            let at = e.position().map_or(lines.pos, |p| p.byte() as usize);
            Err(SqawkError::CsvParseError {
                file: source.to_string(),
                line: feed.line_at(lines, at),
                error: e.to_string(),
            })
        }
    }
}

/// Where a writeback of `file_path` lands.
struct WriteTarget {
    /// The file rewritten: `file_path` with symlinks resolved.
    target: PathBuf,
    /// The directory the temp file is created in.
    parent: PathBuf,
    /// How errors name the file: relative to the working directory when it
    /// is inside it. Never the temp file.
    shown: PathBuf,
}

impl WriteTarget {
    fn of(file_path: &Path) -> Self {
        let target = match std::fs::canonicalize(file_path) {
            Ok(real) => real,
            // A file that does not exist yet (CREATE TABLE ... LOCATION).
            Err(_) => file_path.to_path_buf(),
        };
        let parent = match target.parent() {
            Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
            _ => PathBuf::from("."),
        };
        let shown = std::env::current_dir()
            .and_then(std::fs::canonicalize)
            .ok()
            .and_then(|cwd| target.strip_prefix(cwd).ok().map(Path::to_path_buf))
            .unwrap_or_else(|| target.clone());
        WriteTarget {
            target,
            parent,
            shown,
        }
    }
}

/// Fail unless [`save_table`] can replace `file_path`, writing nothing: the
/// file must not be read-only (by its mode bits, or because opening it for
/// writing is refused), and its directory must take a new file (checked by
/// creating and removing an empty temp file there). A file that does not
/// exist yet passes when its directory does not exist either; `save_table`
/// creates it.
///
/// The atomic rename would replace even a read-only file, so this check is
/// what keeps --write from overwriting one.
pub fn check_writable(file_path: &Path) -> SqawkResult<()> {
    let WriteTarget {
        target,
        parent,
        shown,
    } = WriteTarget::of(file_path);
    let read_only = || {
        SqawkError::write(
            &shown,
            std::io::Error::new(std::io::ErrorKind::PermissionDenied, "file is read-only"),
        )
    };
    if let Ok(meta) = std::fs::metadata(&target) {
        if meta.permissions().readonly() {
            return Err(read_only());
        }
        if let Err(e) = std::fs::OpenOptions::new().append(true).open(&target) {
            return Err(if e.kind() == std::io::ErrorKind::PermissionDenied {
                read_only()
            } else {
                SqawkError::write(&shown, e)
            });
        }
    }
    if parent.is_dir() {
        NamedTempFile::new_in(&parent).map_err(|e| SqawkError::write(&shown, e))?;
    }
    Ok(())
}

/// Write `table` back to `file_path`, reproducing the source layout.
///
/// The write is atomic: a temp file in the target's directory is written,
/// synced, given the original file's permissions, and renamed over the
/// target. A symlink is resolved first, so the link survives and its target
/// is what gets rewritten.
///
/// The rename replaces the directory entry, so a file with several hard
/// links is split from them: the other links keep the old contents.
pub fn save_table(table: &Table, file_path: &Path) -> SqawkResult<()> {
    let WriteTarget {
        target,
        parent,
        shown,
    } = WriteTarget::of(file_path);
    let fail = |e: std::io::Error| SqawkError::write(&shown, e);
    let temp_file = NamedTempFile::new_in(&parent).map_err(fail)?;

    {
        let mut out = BufWriter::new(temp_file.as_file());
        table.write_source_layout(&mut out).map_err(fail)?;
        out.flush().map_err(fail)?;
    }

    if let Ok(meta) = std::fs::metadata(&target) {
        temp_file
            .as_file()
            .set_permissions(meta.permissions())
            .map_err(fail)?;
    }
    temp_file.as_file().sync_all().map_err(fail)?;
    temp_file.persist(&target).map_err(|e| fail(e.error))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{ReadOptions, read_table, read_table_from, unclosed_quote};
    use crate::error::SqawkError;
    use crate::table::Table;

    fn load(data: &[u8], opts: &ReadOptions) -> Result<Table, SqawkError> {
        read_table("t", data.to_vec(), None, ",".into(), opts, "t.csv")
    }

    fn written(t: &Table) -> Vec<u8> {
        let mut out = Vec::new();
        t.write_source_layout(&mut out).unwrap();
        out
    }

    #[test]
    fn comments_are_skipped_and_kept() {
        let data = std::fs::read("tests/data/commented.csv").unwrap();
        let opts = ReadOptions {
            comments: true,
            ..ReadOptions::default()
        };
        let table = load(&data, &opts).unwrap();
        assert_eq!(table.rows().len(), 3);
        assert_eq!(table.rows()[0][1].to_string(), "Alice");
        assert_eq!(table.rows()[2][1].to_string(), "Charlie");
        assert_eq!(written(&table), data);
    }

    /// Without `keep_source` the rows load the same, but the table refuses
    /// to be written back rather than writing regenerated bytes.
    #[test]
    fn dropped_source_refuses_writeback() {
        let opts = ReadOptions {
            keep_source: false,
            ..ReadOptions::default()
        };
        let table = load(b"a,b\n1, x\n", &opts).unwrap();
        assert_eq!(table.rows().len(), 1);
        assert_eq!(table.rows()[0][1].to_string(), " x");
        let err = table.write_source_layout(&mut Vec::new()).unwrap_err();
        assert!(err.to_string().contains("internal error"), "{err}");
    }

    #[test]
    fn hash_lines_are_data_by_default() {
        let table = load(b"#h\n#x\n1\n", &ReadOptions::default()).unwrap();
        assert_eq!(table.columns(), vec!["#h"]);
        assert_eq!(table.rows().len(), 2);
        assert_eq!(table.rows()[0][0].to_string(), "#x");
    }

    #[test]
    fn custom_columns_make_the_first_line_data() {
        let opts = ReadOptions {
            header: false,
            columns: Some(vec!["x".into(), "y".into()]),
            ..ReadOptions::default()
        };
        let table = load(b"id,name\n1,a\n", &opts).unwrap();
        assert_eq!(table.columns(), vec!["x", "y"]);
        assert_eq!(table.rows().len(), 2);
        assert_eq!(table.rows()[0][1].to_string(), "name");
        assert_eq!(written(&table), b"id,name\n1,a\n");
    }

    /// Columns given over a header: the header is skipped, must match the
    /// column count, and is written back as read.
    #[test]
    fn custom_columns_over_a_header() {
        let opts = ReadOptions {
            columns: Some(vec!["x".into(), "y".into()]),
            ..ReadOptions::default()
        };
        let table = load(b"\"id\",name\r\n1,a\r\n", &opts).unwrap();
        assert_eq!(table.columns(), vec!["x", "y"]);
        assert_eq!(table.rows().len(), 1);
        assert_eq!(written(&table), b"\"id\",name\r\n1,a\r\n");
        match load(b"\n\na,b,c\n1,2,3\n", &opts) {
            Err(SqawkError::CsvParseError { line, error, .. }) => {
                assert_eq!(line, 3);
                assert_eq!(
                    error,
                    "header has 3 fields, but the table declares 2 columns"
                );
            }
            other => panic!("unexpected: {other:?}"),
        }
        // No header line yet: writeback gives the file one.
        let table = load(b"", &opts).unwrap();
        assert_eq!(table.columns(), vec!["x", "y"]);
        assert_eq!(written(&table), b"x,y\n");
    }

    #[test]
    fn too_many_fields_is_an_error() {
        let data = std::fs::read("tests/data/malformed.csv").unwrap();
        match load(&data, &ReadOptions::default()) {
            Err(SqawkError::CsvParseError { file, line, .. }) => {
                assert_eq!(file, "t.csv");
                assert_eq!(line, 6);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    /// The line a load error names.
    fn error_line(data: &[u8], opts: &ReadOptions) -> usize {
        match load(data, opts) {
            Err(SqawkError::CsvParseError { line, .. }) => line,
            other => panic!("{:?}: unexpected {other:?}", String::from_utf8_lossy(data)),
        }
    }

    /// Load errors must name the same line whatever the line endings. csv-core
    /// ends a CRLF record at the CR and counts its LF only when the next
    /// record is read, so its line numbers are one too low in CRLF files (a
    /// Windows checkout of tests/data/malformed.csv reported line 5, not 6).
    #[test]
    fn load_error_line_is_the_same_for_crlf() {
        let lf = b"id,name,age\n1,Alice,30\n3,Charlie,35\n5,Too,Many,Fields\n";
        let crlf = b"id,name,age\r\n1,Alice,30\r\n3,Charlie,35\r\n5,Too,Many,Fields\r\n";
        let quote = b"id,name\r\n1,\"a\r\nb\"\r\n2,x\r\n3,y,z\r\n";
        for (data, want) in [(&lf[..], 4), (&crlf[..], 4), (&quote[..], 5)] {
            assert_eq!(
                error_line(data, &ReadOptions::default()),
                want,
                "{:?}",
                String::from_utf8_lossy(data)
            );
        }
    }

    /// Every line ending convention, any mix of them, quoted line breaks,
    /// blank lines and a BOM: the error names the line the bad record is on.
    #[test]
    fn load_error_line_any_line_endings() {
        let opts = ReadOptions::default();
        for (data, want) in [
            // Mixed endings.
            (&b"a,b\r\n1,2\n3,4\r\n5,6,7\n"[..], 4),
            (b"a,b\n1,2\r\n3,4\n5,6,7\r\n", 4),
            // CR alone ends a record, so it ends a line.
            (b"a,b\r1,2\r3,4,5\r", 3),
            // Quoted fields holding LF, CRLF and both.
            (b"a,b\n1,\"x\ny\"\n2,\"p\r\nq\"\r\n3,4,5\n", 6),
            (b"a,b\r\n1,\"x\ny\r\nz\"\r\n3,4,5\r\n", 5),
            // Blank lines (LF and CRLF) before the bad record.
            (b"a,b\n\n1,2\n\n\n3,4,5\n", 6),
            (b"a,b\r\n\r\n1,2\r\n\r\n\r\n3,4,5\r\n", 6),
            // The BOM is not a line.
            (b"\xef\xbb\xbfa,b\r\n1,2\r\n3,4,5\r\n", 3),
            (b"\xef\xbb\xbfa,b\n1,2\n3,4,5\n", 3),
            // The last record, without a line ending.
            (b"a,b\r\n1,2\r\n3,4,5", 3),
        ] {
            assert_eq!(
                error_line(data, &opts),
                want,
                "{:?}",
                String::from_utf8_lossy(data)
            );
        }
    }

    /// Skipped `#` lines count as lines.
    #[test]
    fn load_error_line_after_comments() {
        let opts = ReadOptions {
            comments: true,
            ..ReadOptions::default()
        };
        for (data, want) in [
            (&b"# head\na,b\n# one\n1,2\n# two\n# three\n3,4,5\n"[..], 7),
            (
                b"# head\r\na,b\r\n# one\r\n1,2\r\n# two\r\n# three\r\n3,4,5\r\n",
                7,
            ),
            (b"# head\r\na,b\n\r\n# one\r\n\n1,2\r\n3,4,5\n", 7),
            (b"\xef\xbb\xbf# head\r\na,b\r\n3,4,5\r\n", 3),
        ] {
            assert_eq!(
                error_line(data, &opts),
                want,
                "{:?}",
                String::from_utf8_lossy(data)
            );
        }
    }

    /// An unclosed quote is reported at the line of its opening quote.
    #[test]
    fn unclosed_quote_line_any_line_endings() {
        let opts = ReadOptions::default();
        for (data, want) in [
            (&b"a,b\n1,2\n3,\"x\n4,5\n"[..], 3),
            (b"a,b\r\n1,2\r\n3,\"x\r\n4,5\r\n", 3),
            (b"a,b\r\n1,\"p\r\nq\"\r\n\r\n3,\"x\r\n4,5\r\n", 5),
            (b"\xef\xbb\xbfa,b\r\n1,2\n3,\"x\r\n", 3),
            (b"a,b\r1,2\r3,\"x\r4,5\r", 3),
        ] {
            assert_eq!(
                error_line(data, &opts),
                want,
                "{:?}",
                String::from_utf8_lossy(data)
            );
        }
        let comments = ReadOptions {
            comments: true,
            ..ReadOptions::default()
        };
        assert_eq!(error_line(b"#c\r\na,b\r\n#d\r\n1,\"x\r\n", &comments), 4);
    }

    /// A bad header is reported at its own line, past BOM, blank and
    /// comment lines.
    #[test]
    fn bad_header_line() {
        // A header that does not fit the declared columns (LOCATION) is an
        // error on the header's own line, whatever comes before it.
        let declared = ReadOptions {
            columns: Some(vec!["x".to_string()]),
            ..ReadOptions::default()
        };
        assert_eq!(error_line(b"a,b\r\n1,2\r\n", &declared), 1);
        assert_eq!(error_line(b"\r\n\r\na,b\r\n", &declared), 3);
        let comments = ReadOptions {
            comments: true,
            ..declared
        };
        assert_eq!(error_line(b"\xef\xbb\xbf#c\r\n\na,,b\r\n", &comments), 3);
    }

    #[test]
    fn untouched_bytes_round_trip() {
        for data in [
            &b"k,s\n1,\"abc\"\n2,trail \n"[..],
            b"\xef\xbb\xbfk,s\r\n1,a\r\n\r\n2,\"x\r\ny\"\n3,\xff",
            b"k,s,t\n1\n\n\n2,b,c",
            b"a\n",
            b"a",
            b"",
            b"\n\n",
            b"a,b\r1,2\r3,4\r",
        ] {
            let table = load(data, &ReadOptions::default());
            let table = match table {
                Ok(t) => t,
                Err(_) => continue,
            };
            assert_eq!(written(&table), data, "{:?}", String::from_utf8_lossy(data));
        }
        let opts = ReadOptions {
            comments: true,
            ..ReadOptions::default()
        };
        for data in [&b"#c\na\n#d\n1\n#e"[..], b"#only", b"a\n1\n# tail\n\n"] {
            let table = load(data, &opts).unwrap();
            assert_eq!(written(&table), data, "{:?}", String::from_utf8_lossy(data));
        }
    }

    #[test]
    fn unclosed_quote_detection() {
        assert_eq!(unclosed_quote(b"1,\"2\n3,4\n", b','), Some(2));
        assert_eq!(unclosed_quote(b"1,\"2\"\n", b','), None);
        assert_eq!(unclosed_quote(b"1,\"a\"\"b\"\n", b','), None);
        assert_eq!(unclosed_quote(b"1,\"a\"\"\n", b','), Some(2));
        assert_eq!(unclosed_quote(b"1,a\"b\n", b','), None);
        assert_eq!(unclosed_quote(b"\"a\"x\"y", b','), None);
        assert!(matches!(
            load(b"a,b\n1,\"2\n3,4\n", &ReadOptions::default()),
            Err(SqawkError::CsvParseError { line: 2, .. })
        ));
    }

    #[test]
    fn header_names_are_made_unique() {
        for (data, want) in [
            (&b"a,a\n"[..], &["a", "a:1"][..]),
            (b"a,A\n", &["a", "A:1"]),
            (b"a,,c\n", &["a", "column2", "c"]),
            (b",\n", &["column1", "column2"]),
        ] {
            let table = load(data, &ReadOptions::default()).unwrap();
            let names: Vec<&str> = table
                .column_metadata()
                .iter()
                .map(|c| c.name.as_str())
                .collect();
            assert_eq!(names, want, "{:?}", String::from_utf8_lossy(data));
        }
        // A declared column list is SQL the user wrote: still rejected.
        for cols in [&["a", "A"][..], &["a", ""]] {
            let opts = ReadOptions {
                columns: Some(cols.iter().map(|c| c.to_string()).collect()),
                header: false,
                ..ReadOptions::default()
            };
            assert!(load(b"1,2\n", &opts).is_err(), "{cols:?}");
        }
    }

    /// A reader that hands out at most `k` bytes per read, so the streaming
    /// loader meets every chunk boundary.
    struct Trickle<'a> {
        data: &'a [u8],
        k: usize,
    }

    impl std::io::Read for Trickle<'_> {
        fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
            let n = self.k.min(out.len()).min(self.data.len());
            out[..n].copy_from_slice(&self.data[..n]);
            self.data = &self.data[n..];
            Ok(n)
        }
    }

    /// A loaded table, or its error, as comparable text.
    fn outcome(r: crate::error::SqawkResult<Table>) -> String {
        match r {
            Ok(t) => format!("{:?} {:?} {:?}", t.columns(), t.rows(), t.file_layout()),
            Err(e) => format!("error: {e}"),
        }
    }

    /// Parsing a stream gives exactly what parsing the whole file gives:
    /// rows, layout, error text and line numbers, wherever the input's
    /// chunks happen to end.
    #[test]
    fn streaming_matches_whole_file() {
        const PIECES: &[&[u8]] = &[
            b"a",
            b"bc",
            b"",
            b"12",
            b"-3.5",
            b"\"q,\"\"x\"",
            b"\"multi\nline\"",
            b"\"cr\r\nlf\"",
            b" sp ",
            b"#c",
            b"\xff\xfe",
            b"\"open",
            b"x\"y",
        ];
        const ENDS: &[&[u8]] = &[b"\n", b"\r\n", b"\r", b"\n\n", b"\n#note\n", b"\r\n\r\n"];
        let mut seed: u64 = 0x5eed;
        let mut next = |n: usize| {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((seed >> 33) as usize) % n
        };
        for case in 0..3000 {
            let mut data = Vec::new();
            if next(5) == 0 {
                data.extend_from_slice(b"\xef\xbb\xbf");
            }
            let width = 1 + next(3);
            for _ in 0..next(8) {
                let fields = if next(6) == 0 { 1 + next(4) } else { width };
                for f in 0..fields {
                    if f > 0 {
                        data.push(b',');
                    }
                    data.extend_from_slice(PIECES[next(PIECES.len())]);
                }
                if next(8) != 0 {
                    data.extend_from_slice(ENDS[next(ENDS.len())]);
                }
            }
            let opts = ReadOptions {
                header: next(4) != 0,
                comments: next(2) == 0,
                columns: (next(5) == 0).then(|| (0..width).map(|i| format!("c{i}")).collect()),
                keep_source: false,
                ..ReadOptions::default()
            };
            let whole = outcome(read_table(
                "t",
                data.clone(),
                None,
                ",".into(),
                &opts,
                "t.csv",
            ));
            let kept = outcome(read_table(
                "t",
                data.clone(),
                None,
                ",".into(),
                &ReadOptions {
                    keep_source: true,
                    ..opts.clone()
                },
                "t.csv",
            ));
            assert_eq!(
                whole,
                kept,
                "case {case}: {:?}",
                String::from_utf8_lossy(&data)
            );
            let input = Trickle {
                data: &data,
                k: 1 + case % 7,
            };
            let streamed = outcome(read_table_from(
                "t",
                input,
                std::path::Path::new("t.csv"),
                None,
                ",".into(),
                &opts,
                "t.csv",
            ));
            assert_eq!(
                whole,
                streamed,
                "case {case}: {:?}",
                String::from_utf8_lossy(&data)
            );
        }
    }
}
