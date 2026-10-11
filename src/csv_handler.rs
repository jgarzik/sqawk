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

use std::io::{BufWriter, Write};
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
    pub header: bool,
    /// Skip lines that start with `#` (`--comments`)
    pub comments: bool,
    /// Column names to use; implies the file has no header row
    pub columns: Option<Vec<String>>,
}

impl Default for ReadOptions {
    /// Plain CSV: comma, header row, no comment lines.
    fn default() -> Self {
        ReadOptions {
            delimiter: b',',
            header: true,
            comments: false,
            columns: None,
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

/// Parse delimited `data` into a table.
///
/// The table keeps `data` itself: every record's source bytes (see
/// `RawRows`), so writeback reproduces the rows no statement changed
/// byte-for-byte.
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
    let bom = data.starts_with(BOM);
    let base = if bom { BOM.len() } else { 0 };
    let body = &data[base..];

    // Line ending of the first record decides the terminator of the records
    // writeback generates.
    let crlf = body
        .iter()
        .position(|&b| b == b'\n')
        .is_some_and(|i| i > 0 && body[i - 1] == b'\r');

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .delimiter(opts.delimiter)
        .comment(if opts.comments { Some(b'#') } else { None })
        .from_reader(body);

    let header = opts.header && opts.columns.is_none();
    let mut record = csv::ByteRecord::new();
    let mut line = 0usize;
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
    // The last record's span within `body`.
    let mut last: Option<(usize, usize)> = None;

    loop {
        let more = read_record(&mut reader, &mut record, source, &mut line)?;
        let from = record
            .position()
            .map_or(prev_end, |p| p.byte() as usize)
            .clamp(prev_end, body.len());
        let start = record_start(body, from, opts.comments);
        // A comment line that ends the input without a line ending comes
        // back from the csv crate as an empty record; it is not a row.
        if !more || start >= body.len() {
            gaps.tail = body[prev_end..].to_vec();
            break;
        }
        let mut end = (reader.position().byte() as usize).clamp(start, body.len());
        // csv-core ends a CRLF record at the CR; the LF belongs to it too.
        if end > start && end < body.len() && body[end - 1] == b'\r' && body[end] == b'\n' {
            end += 1;
        }
        let gap = &body[prev_end..start];
        prev_end = end;
        last = Some((start, end));
        let span = RawSpan {
            start: base + start,
            end: base + end,
        };

        let Some(cols) = &columns else {
            // The first record: the header, or the first row.
            gaps.head = gap.to_vec();
            let cols = if header {
                header_span = span;
                record.iter().map(|f| field_text(f).into_owned()).collect()
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
            spans.push(span);
            continue;
        };
        debug_assert_eq!(cols.len(), width);
        if !gap.is_empty() {
            gaps.before
                .entry(rows.len())
                .or_default()
                .extend_from_slice(gap);
        }
        push_row(&mut rows, &record, width, source, line)?;
        spans.push(span);
    }

    // csv-core takes a quote that is never closed as running to the end of
    // the input, so the rest of the file would load as one field.
    if let Some((start, end)) = last {
        if let Some(quote) = unclosed_quote(&body[start..end], opts.delimiter) {
            let at = start + quote;
            let quote_line = 1 + body[..at].iter().filter(|&&b| b == b'\n').count();
            return Err(SqawkError::CsvParseError {
                file: source.to_string(),
                line: quote_line,
                error: "unterminated quoted field".to_string(),
            });
        }
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

    drop(reader);
    let mut table = Table::new_with_delimiter(table_name, columns, file_path, delimiter);
    table.set_file_layout(FileLayout {
        has_header: header && header_span != RawSpan::NONE,
        bom,
        crlf,
        comments: opts.comments,
    });
    // Loading is not a modification.
    table.replace_rows(rows);
    table.set_raw(RawRows {
        buf: data,
        header: header_span,
        rows: spans,
    });
    table.set_comment_lines(gaps);
    table.mark_unmodified();
    Ok(table)
}

/// Reject a header (or column list) with an empty or repeated name. Names
/// are case-insensitive, so `a` and `A` are the same column.
fn check_column_names(cols: &[String], source: &str, line: usize) -> SqawkResult<()> {
    let err = |error: String| SqawkError::CsvParseError {
        file: source.to_string(),
        line,
        error,
    };
    if let Some(i) = cols.iter().position(String::is_empty) {
        return Err(err(format!("column {} has an empty name", i + 1)));
    }
    let mut dups: Vec<&str> = Vec::new();
    for (i, name) in cols.iter().enumerate() {
        let repeated = cols[..i].iter().any(|n| n.eq_ignore_ascii_case(name));
        if repeated && !dups.iter().any(|d| d.eq_ignore_ascii_case(name)) {
            dups.push(name);
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

/// Where the record the csv crate reads from `from` begins: past the blank
/// lines (CR and LF bytes) and, with `comments`, the `#` lines (each running
/// to its LF) that csv-core skips first.
fn record_start(data: &[u8], from: usize, comments: bool) -> usize {
    let mut i = from;
    while i < data.len() {
        match data[i] {
            b'\r' | b'\n' => i += 1,
            b'#' if comments => {
                i = data[i..]
                    .iter()
                    .position(|&b| b == b'\n')
                    .map_or(data.len(), |n| i + n + 1);
            }
            _ => break,
        }
    }
    i
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
    // Every failure names the target file, never the temp file; relative to
    // the working directory when it is inside it.
    let shown = std::env::current_dir()
        .and_then(std::fs::canonicalize)
        .ok()
        .and_then(|cwd| target.strip_prefix(cwd).ok().map(Path::to_path_buf))
        .unwrap_or_else(|| target.clone());
    let fail = |e: std::io::Error| SqawkError::write(&shown, e);
    let temp_file = NamedTempFile::new_in(parent).map_err(fail)?;

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
    use super::{read_table, unclosed_quote, ReadOptions};
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
            columns: Some(vec!["x".into(), "y".into()]),
            ..ReadOptions::default()
        };
        let table = load(b"id,name\n1,a\n", &opts).unwrap();
        assert_eq!(table.columns(), vec!["x", "y"]);
        assert_eq!(table.rows().len(), 2);
        assert_eq!(table.rows()[0][1].to_string(), "name");
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
    fn bad_header_names() {
        for data in [&b"a,a\n"[..], b"a,A\n", b"a,,c\n", b",\n"] {
            assert!(
                load(data, &ReadOptions::default()).is_err(),
                "{:?}",
                String::from_utf8_lossy(data)
            );
        }
    }
}
