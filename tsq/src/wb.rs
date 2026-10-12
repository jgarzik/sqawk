//! `tsq wb`: the `--write` writeback fuzzer.
//!
//! Each iteration writes one to three random delimited files (`.csv`,
//! `.tsv`, `.txt`, `.tab`, `.dat`; comma, tab or a `-F` separator; LF, CRLF
//! or mixed line endings; optional byte order mark and final line ending;
//! ragged rows; blank lines and, under `--comments`, `#` lines; quoted and
//! multi-line fields, invalid UTF-8, and `tsq gen`'s edge values), plus at
//! times files opened by `CREATE TABLE ... LOCATION` (existing or new, with
//! their own `WITH` options). A random statement sequence (UPDATE, DELETE,
//! INSERT, ALTER TABLE ADD COLUMN, TRUNCATE, SELECT, BEGIN / COMMIT / END /
//! ROLLBACK, a transaction left open) runs through sqawk with `--write` and,
//! statement by statement, through the SQLite oracle holding the same
//! values.
//!
//! The expected file is computed from the oracle, independently of sqawk,
//! by the documented writeback rules (doc/sql_reference.md "Writeback"):
//! a row no statement changed (same rowid, every value the same value of
//! the same type) is its source bytes; a changed or inserted row is
//! written from the oracle's values (integers in decimal, reals as
//! `CAST(x AS TEXT)`, NULL empty) with RFC 4180 quoting where a field
//! holds the delimiter, a quote, CR or LF, or starts or ends with a space
//! or tab, or (under comments) a first field starts with `#`; the byte
//! order mark, header line, line ending of the first line, blank and
//! comment lines (which stay in front of the next surviving row, or move to
//! the end) are kept; ALTER TABLE rewrites every line. Checks:
//!
//! * every file's bytes equal the expected bytes (so untouched records are
//!   byte-identical and in order, BOM / comments / line endings are kept,
//!   and updated and inserted values are the oracle's);
//! * a sequence whose changes were all rolled back leaves files
//!   byte-identical;
//! * a read-only target that a committed statement changed fails the run
//!   (exit 1, one `sqawk: ... read-only` line) and no file is written;
//! * SELECT output equals the oracle's, and re-reading each written file
//!   gives the oracle's final rows;
//! * no file other than the expected ones (no temp file) is left behind.
//!
//! On Unix, some iterations also kill sqawk (SIGKILL through `kill`, no
//! libc) at random moments of a large `--write` run: the file must then be
//! either the original or the complete new content, never partial; temp
//! files a killed run leaves (`.tmp*`, the `tempfile` crate's naming,
//! created next to the target) are counted and reported.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use rusqlite::types::Value;
use rusqlite::Connection;

use crate::data::Table;
use crate::diff::oracle::{quote_ident, real_text, Oracle};
use crate::diff::render::raw_records;
use crate::gen;
use crate::rng::Rng;
use crate::run::{crash_text, execute, Job};

pub struct WbOptions {
    pub iterations: usize,
    pub timeout: Duration,
    /// Probability that an iteration also runs a kill-during-write trial
    /// (Unix only).
    pub kill_rate: f64,
}

/// One iteration's verdict.
enum Verdict {
    Ok,
    Bad(String),
    Crash(String),
}

/// Cell values beyond `tsq gen`'s edge set (wb.py's list): quoting,
/// embedded separators and numeric-looking text.
const CELLS: &[&str] = &[
    "abc",
    "a,b",
    "line1\nline2",
    "q\"q",
    "\"",
    "007",
    "1.50",
    " 5",
    "trail ",
    "héllo",
    "日本",
    "-0",
    "1e3",
    "NaN",
    "inf",
    "0x10",
    "+3",
    "12.0",
    "1.5",
    "42",
    "-7",
    "9223372036854775808",
    "a\"b",
    "\t",
    "#notcomment",
    "multi\r\nline",
    "NULL",
    "true",
    "a;b",
    "a|b",
];

/// SET / VALUES expressions both engines evaluate alike. `{c}` is a column
/// of the table, `{d}` another one, `{id}` the key column.
const EXPRS: &[&str] = &[
    "'X'",
    "NULL",
    "1.0",
    "42",
    "-7",
    "0.5",
    "1e20",
    "'a,\"b'",
    "'007'",
    "'  pad '",
    "'line1\nline2'",
    "'tab\there'",
    "'#hash'",
    "''",
    "'héllo'",
    "'9223372036854775807'",
    "9223372036854775807",
    "'NULL'",
    "TRUE",
    "FALSE",
    "'semi;colon'",
    "'pipe|bar'",
    "'cr\rx'",
    "length({c})",
    "{c}",
    "{d}",
    "{c} || 'x'",
    "{id} * 2",
    "{id} + 0.5",
    "{id} * 1.5",
    "-{id}",
    "abs({id})",
    "{id} % 3",
    "coalesce({c}, 'dflt')",
    "substr({c}, 2, 3)",
    "replace({c}, 'a', 'Z')",
    "printf('%05d', {id})",
    "CASE WHEN {id} % 2 = 0 THEN 'even' ELSE {c} END",
    "typeof({c})",
    "({id} > 3)",
    "({c} IS NULL)",
    "round({id} / 3.0, 2)",
    "{id} / 2",
    "CAST({id} AS TEXT)",
    "max({id}, 3)",
    "hex({c})",
    "' lead'",
];

/// A `CREATE TABLE ... LOCATION` file's `WITH` options.
#[derive(Clone, Debug)]
struct Location {
    /// The file existed before the run.
    existing: bool,
}

/// One table as the model holds it: what writeback must produce.
#[derive(Clone, Debug)]
struct Model {
    name: String,
    path: PathBuf,
    /// The file's bytes before the run (None: created by the run).
    original: Option<Vec<u8>>,
    delim: u8,
    header: bool,
    comments: bool,
    crlf: bool,
    bom: bool,
    columns: Vec<String>,
    header_raw: Option<Vec<u8>>,
    /// False once ALTER TABLE rewrote every line.
    raw_valid: bool,
    rows: Vec<Row>,
    head: Vec<u8>,
    before: BTreeMap<usize, Vec<u8>>,
    tail: Vec<u8>,
    location: Option<Location>,
}

#[derive(Clone, Debug)]
struct Row {
    rowid: i64,
    raw: Option<Vec<u8>>,
    vals: Vec<Value>,
}

/// Whether two values are the same value of the same type (sqawk keeps a
/// row's source bytes only then).
fn identical(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Real(x), Value::Real(y)) => x.to_bits() == y.to_bits(),
        _ => a == b,
    }
}

fn field_text(conn: &Connection, v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::Integer(i) => i.to_string(),
        Value::Real(f) => real_text(conn, *f),
        Value::Text(s) => s.clone(),
        Value::Blob(b) => String::from_utf8_lossy(b).into_owned(),
    }
}

/// One record as sqawk writes it (writeback and query output alike).
fn write_record(out: &mut Vec<u8>, fields: &[String], delim: u8, comments: bool, eol: &[u8]) {
    for (i, f) in fields.iter().enumerate() {
        if i > 0 {
            out.push(delim);
        }
        let b = f.as_bytes();
        let quote = !b.is_empty()
            && ((i == 0 && comments && b[0] == b'#')
                || b.iter()
                    .any(|&c| c == delim || c == b'"' || c == b'\n' || c == b'\r')
                || matches!(b[0], b' ' | b'\t')
                || matches!(b[b.len() - 1], b' ' | b'\t'));
        if quote {
            out.push(b'"');
            out.extend_from_slice(f.replace('"', "\"\"").as_bytes());
            out.push(b'"');
        } else {
            out.extend_from_slice(b);
        }
    }
    if fields.len() == 1 && fields[0].is_empty() {
        out.extend_from_slice(b"\"\"");
    }
    out.extend_from_slice(eol);
}

impl Model {
    fn eol(&self) -> &'static [u8] {
        if self.crlf {
            b"\r\n"
        } else {
            b"\n"
        }
    }

    /// The bytes writeback produces for this table.
    fn render(&self, conn: &Connection) -> Vec<u8> {
        let eol = self.eol();
        let mut out = Vec::new();
        if self.bom {
            out.extend_from_slice(b"\xef\xbb\xbf");
        }
        let mut open = false;
        let put = |out: &mut Vec<u8>, bytes: &[u8], open: &mut bool| {
            if bytes.is_empty() {
                return;
            }
            if *open {
                out.extend_from_slice(eol);
            }
            out.extend_from_slice(bytes);
            *open = !matches!(bytes.last(), Some(b'\n' | b'\r'));
        };
        put(&mut out, &self.head, &mut open);
        if self.header {
            match self.header_raw.as_ref().filter(|_| self.raw_valid) {
                Some(raw) => put(&mut out, raw, &mut open),
                None => {
                    let mut rec = Vec::new();
                    write_record(&mut rec, &self.columns, self.delim, self.comments, eol);
                    put(&mut out, &rec, &mut open);
                }
            }
        }
        for (i, row) in self.rows.iter().enumerate() {
            if let Some(b) = self.before.get(&i) {
                put(&mut out, b, &mut open);
            }
            match row.raw.as_ref().filter(|_| self.raw_valid) {
                Some(raw) => put(&mut out, raw, &mut open),
                None => {
                    if open {
                        out.extend_from_slice(eol);
                        open = false;
                    }
                    let fields: Vec<String> =
                        row.vals.iter().map(|v| field_text(conn, v)).collect();
                    write_record(&mut out, &fields, self.delim, self.comments, eol);
                }
            }
        }
        for (_, b) in self.before.range(self.rows.len()..) {
            put(&mut out, b, &mut open);
        }
        put(&mut out, &self.tail, &mut open);
        out
    }

    /// Bring the model in line with the oracle's table after a statement:
    /// rows gone are deleted (their lines move on), rows whose values
    /// changed lose their source bytes, new rowids are appended.
    fn sync(&mut self, conn: &Connection) -> Result<()> {
        let now = table_rows(conn, &self.name)?;
        let ids: BTreeSet<i64> = now.keys().copied().collect();
        // Deletions, with the comment lines of removed rows carried along.
        let old = std::mem::take(&mut self.rows);
        let mut before = std::mem::take(&mut self.before);
        let mut pending: Vec<u8> = Vec::new();
        for (i, row) in old.into_iter().enumerate() {
            if let Some(mut lines) = before.remove(&i) {
                pending.append(&mut lines);
            }
            if ids.contains(&row.rowid) {
                if !pending.is_empty() {
                    self.before
                        .insert(self.rows.len(), std::mem::take(&mut pending));
                }
                self.rows.push(row);
            }
        }
        for (_, mut lines) in before {
            pending.append(&mut lines);
        }
        pending.append(&mut self.tail);
        self.tail = pending;
        // Changes and insertions.
        let known: BTreeSet<i64> = self.rows.iter().map(|r| r.rowid).collect();
        for row in &mut self.rows {
            let vals = &now[&row.rowid];
            let same = vals.len() == row.vals.len()
                && vals.iter().zip(&row.vals).all(|(a, b)| identical(a, b));
            if !same {
                row.raw = None;
                row.vals = vals.clone();
            }
        }
        for (id, vals) in &now {
            if !known.contains(id) {
                self.rows.push(Row {
                    rowid: *id,
                    raw: None,
                    vals: vals.clone(),
                });
            }
        }
        Ok(())
    }
}

/// `rowid -> values` of a table, in rowid order.
fn table_rows(conn: &Connection, name: &str) -> Result<BTreeMap<i64, Vec<Value>>> {
    let mut st = conn.prepare(&format!("SELECT rowid, * FROM {}", quote_ident(name)))?;
    let n = st.column_count();
    let mut out = BTreeMap::new();
    let mut rows = st.query([])?;
    while let Some(r) = rows.next()? {
        let id: i64 = r.get(0)?;
        let vals = (1..n)
            .map(|i| r.get::<_, Value>(i))
            .collect::<rusqlite::Result<Vec<_>>>()?;
        out.insert(id, vals);
    }
    Ok(out)
}

/// A generated file: its bytes and the model of what sqawk loads.
struct Built {
    bytes: Vec<u8>,
    model: Model,
    /// Field text per row (padded), as the oracle loads it.
    cells: Vec<Vec<String>>,
}

/// A random cell value: field text, and the raw bytes when they are not
/// the text's UTF-8 (invalid UTF-8 in the source).
fn random_value(r: &mut Rng) -> (String, Option<Vec<u8>>) {
    let roll = r.unit();
    if roll < 0.01 {
        // Invalid UTF-8: kept byte for byte, read as U+FFFD.
        let raw = b"caf\xe9".to_vec();
        return (String::from_utf8_lossy(&raw).into_owned(), Some(raw));
    }
    let v = if roll < 0.35 {
        r.pick(CELLS).to_string()
    } else if roll < 0.55 {
        r.pick(gen::EDGE_VALUES).to_string()
    } else if roll < 0.7 {
        r.range(-1000, 100_000).to_string()
    } else if roll < 0.8 {
        format!("{}.{}", r.range(0, 999), r.range(1, 9))
    } else if roll < 0.9 {
        String::new()
    } else {
        let words = ["alpha", "beta", "gamma", "delta", "x y", "O'Brien"];
        (*r.pick(&words)).to_string()
    };
    (v, None)
}

/// The raw field for `v` (or `raw` bytes) in a file with delimiter `delim`.
fn encode_field(
    r: &mut Rng,
    v: &str,
    raw: Option<&[u8]>,
    delim: u8,
    quoted_empty: bool,
) -> Vec<u8> {
    if let Some(b) = raw {
        return b.to_vec();
    }
    if v.is_empty() {
        return if quoted_empty {
            b"\"\"".to_vec()
        } else {
            Vec::new()
        };
    }
    let b = v.as_bytes();
    let must = b.iter().any(|&c| c == delim || c == b'\r' || c == b'\n') || b[0] == b'"';
    let has_quote = b.contains(&b'"');
    // A quote inside an unquoted field is a literal quote.
    let quote = must || (has_quote && r.chance(0.7)) || r.chance(0.1);
    if quote {
        let mut out = vec![b'"'];
        out.extend_from_slice(v.replace('"', "\"\"").as_bytes());
        out.push(b'"');
        out
    } else {
        b.to_vec()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Eol {
    Lf,
    Crlf,
    Mixed,
}

struct FileOpts {
    delim: u8,
    header: bool,
    comments: bool,
    eol: Eol,
    bom: bool,
    final_eol: bool,
    ragged: bool,
}

/// Build one file's bytes and its model.
fn build_file(
    r: &mut Rng,
    seed: u64,
    name: &str,
    o: &FileOpts,
    ncols: usize,
    nrows: usize,
) -> Built {
    let columns: Vec<String> = if o.header {
        std::iter::once("id".to_string())
            .chain((1..ncols).map(|i| format!("c{i}")))
            .collect()
    } else {
        (0..ncols)
            .map(|i| ((b'a' + i as u8) as char).to_string())
            .collect()
    };
    // Values, salted with tsq gen's edge injection (the key column stays).
    let mut t = Table::new(name, &[]);
    t.columns = columns.clone();
    let mut raws: Vec<Vec<Option<Vec<u8>>>> = Vec::new();
    for i in 0..nrows {
        let mut row = vec![(i + 1).to_string()];
        let mut raw_row = vec![None];
        for _ in 1..ncols {
            let (v, raw) = random_value(r);
            row.push(v);
            raw_row.push(raw);
        }
        t.rows.push(row);
        raws.push(raw_row);
    }
    let edge_rate = if r.chance(0.5) { 0.15 } else { 0.0 };
    gen::inject_edges(
        std::slice::from_mut(&mut t),
        seed ^ r.range(0, i64::MAX) as u64,
        edge_rate,
    );
    for (i, row) in t.rows.iter_mut().enumerate() {
        if row[0] != (i + 1).to_string() {
            row[0] = (i + 1).to_string();
        }
        for (c, cell) in row.iter().enumerate() {
            // An edge value replaced an invalid-UTF-8 cell.
            if raws[i][c].is_some()
                && *cell != String::from_utf8_lossy(raws[i][c].as_ref().unwrap())
            {
                raws[i][c] = None;
            }
        }
    }

    let eol_of = |r: &mut Rng| -> &'static [u8] {
        match o.eol {
            Eol::Lf => b"\n",
            Eol::Crlf => b"\r\n",
            Eol::Mixed => {
                if r.chance(0.5) {
                    b"\r\n"
                } else {
                    b"\n"
                }
            }
        }
    };
    let gap = |r: &mut Rng, k: usize| -> Vec<u8> {
        let mut g = Vec::new();
        if r.chance(0.12) {
            g.extend_from_slice(eol_of(r));
        }
        if o.comments && r.chance(0.2) {
            let text = match r.below(3) {
                0 => format!("# a comment, {k}"),
                1 => "#".to_string(),
                _ => format!("#x,\"y\t{k}"),
            };
            g.extend_from_slice(text.as_bytes());
            g.extend_from_slice(eol_of(r));
        }
        g
    };

    let mut bytes = Vec::new();
    if o.bom {
        bytes.extend_from_slice(b"\xef\xbb\xbf");
    }
    let nrec = nrows + usize::from(o.header);
    let mut head = Vec::new();
    let mut header_raw = None;
    let mut before = BTreeMap::new();
    let mut rows = Vec::new();
    let mut cells = Vec::new();
    for k in 0..nrec {
        let g = gap(r, k);
        bytes.extend_from_slice(&g);
        let last = k + 1 == nrec;
        let term: &[u8] = if last && !o.final_eol { b"" } else { eol_of(r) };
        let mut rec = Vec::new();
        if o.header && k == 0 {
            head = g;
            for (i, c) in columns.iter().enumerate() {
                if i > 0 {
                    rec.push(o.delim);
                }
                rec.extend_from_slice(c.as_bytes());
            }
            rec.extend_from_slice(term);
            header_raw = Some(rec.clone());
        } else {
            let i = k - usize::from(o.header);
            if k == 0 {
                head = g;
            } else if !g.is_empty() {
                before.insert(i, g);
            }
            let mut n = ncols;
            if o.ragged && r.chance(0.3) {
                n = r.range(1, ncols as i64 - 1) as usize;
            }
            let mut padded = Vec::new();
            for (c, (cell, raw)) in t.rows[i].iter().zip(&raws[i]).enumerate() {
                if c > 0 && c < n {
                    rec.push(o.delim);
                }
                if c < n {
                    let qe = t.quoted_empty.contains(&(i, c)) || (n == 1 && cell.is_empty());
                    rec.extend_from_slice(&encode_field(r, cell, raw.as_deref(), o.delim, qe));
                    padded.push(cell.clone());
                } else {
                    padded.push(String::new());
                }
            }
            rec.extend_from_slice(term);
            rows.push(Row {
                rowid: i as i64 + 1,
                raw: Some(rec.clone()),
                vals: Vec::new(),
            });
            cells.push(padded);
        }
        bytes.extend_from_slice(&rec);
    }
    let mut tail = Vec::new();
    if o.final_eol {
        tail = gap(r, nrec);
        bytes.extend_from_slice(&tail);
    }
    if nrec == 0 {
        // No record at all: every line is the head.
        head = std::mem::take(&mut tail);
    }
    // The line ending of the first line decides generated lines' ending.
    let body = if o.bom { &bytes[3..] } else { &bytes[..] };
    let crlf = body
        .iter()
        .position(|&b| b == b'\n')
        .is_some_and(|i| i > 0 && body[i - 1] == b'\r');
    let model = Model {
        name: name.to_string(),
        path: PathBuf::new(),
        original: Some(bytes.clone()),
        delim: o.delim,
        header: o.header,
        comments: o.comments,
        crlf,
        bom: o.bom,
        columns,
        header_raw,
        raw_valid: true,
        rows,
        head,
        before,
        tail,
        location: None,
    };
    Built {
        bytes,
        model,
        cells,
    }
}

/// One statement: the text sqawk runs and the text the oracle runs.
#[derive(Clone, Debug)]
enum Stmt {
    Data {
        sqawk: String,
        oracle: String,
        select: bool,
        /// ALTER TABLE on this table (every line rewritten).
        alter: Option<usize>,
    },
    Begin(&'static str),
    End {
        sqawk: &'static str,
        commit: bool,
    },
}

impl Stmt {
    fn sqawk(&self) -> String {
        match self {
            Stmt::Data { sqawk, .. } => sqawk.clone(),
            Stmt::Begin(s) => s.to_string(),
            Stmt::End { sqawk, .. } => sqawk.to_string(),
        }
    }
}

fn sql_str(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn delim_sql(d: u8) -> String {
    sql_str(&(d as char).to_string())
}

/// The model's columns other than the key, or the key when none.
fn data_cols(m: &Model) -> Vec<String> {
    if m.columns.len() > 1 {
        m.columns[1..].to_vec()
    } else {
        m.columns.clone()
    }
}

fn expr(r: &mut Rng, m: &Model) -> String {
    let cols = data_cols(m);
    let c = r.pick(&cols).clone();
    let d = r.pick(&m.columns).clone();
    r.pick(EXPRS)
        .replace("{c}", &quote_ident(&c))
        .replace("{d}", &quote_ident(&d))
        .replace("{id}", &quote_ident(&m.columns[0]))
}

fn cond(r: &mut Rng, m: &Model) -> String {
    let id = quote_ident(&m.columns[0]);
    let n = m.rows.len() as i64 + 3;
    let cols = data_cols(m);
    let c = quote_ident(r.pick(&cols).as_str());
    match r.below(12) {
        0 | 1 => {
            let k = r.range(1, 4);
            let list: Vec<String> = (0..k).map(|_| r.range(0, n).to_string()).collect();
            format!("{id} IN ({})", list.join(", "))
        }
        2 => format!("{id} = {}", r.range(0, n)),
        3 => format!("{id} > {}", r.range(0, n)),
        4 => format!("{id} <= {}", r.range(0, n)),
        5 => format!("{id} % 2 = 0"),
        6 => format!("{c} IS NULL"),
        7 => format!("{c} IS NOT NULL"),
        8 => "1".to_string(),
        9 => "0".to_string(),
        10 => format!("{id} BETWEEN {} AND {}", r.range(0, n), r.range(0, n)),
        _ => format!("length({c}) > {}", r.range(0, 6)),
    }
}

/// A random data statement on table `ti`.
fn data_stmt(r: &mut Rng, models: &[Model], ti: usize, alter_seq: &mut usize) -> Stmt {
    let m = &models[ti];
    let t = quote_ident(&m.name);
    let id = quote_ident(&m.columns[0]);
    let cols = data_cols(m);
    let plain = |s: String| Stmt::Data {
        sqawk: s.clone(),
        oracle: s,
        select: false,
        alter: None,
    };
    let roll = r.below(100);
    if roll < 32 {
        let mut sets = vec![format!(
            "{} = {}",
            quote_ident(r.pick(&cols).as_str()),
            expr(r, m)
        )];
        if r.chance(0.25) {
            let c2 = quote_ident(r.pick(&cols).as_str());
            if !sets[0].starts_with(&format!("{c2} ")) {
                sets.push(format!("{c2} = {}", expr(r, m)));
            }
        }
        if r.chance(0.05) {
            sets.push(format!("{id} = {id} + 100"));
        }
        plain(format!(
            "UPDATE {t} SET {} WHERE {}",
            sets.join(", "),
            cond(r, m)
        ))
    } else if roll < 47 {
        let w = if r.chance(0.05) {
            String::new()
        } else if r.chance(0.1) && models.len() > 1 {
            let other = &models[(ti + 1) % models.len()];
            format!(
                " WHERE {id} IN (SELECT {} FROM {})",
                quote_ident(&other.columns[0]),
                quote_ident(&other.name)
            )
        } else {
            format!(" WHERE {}", cond(r, m))
        };
        plain(format!("DELETE FROM {t}{w}"))
    } else if roll < 70 {
        match r.below(3) {
            0 => {
                let c = quote_ident(r.pick(&cols).as_str());
                let k = r.range(1, 3);
                let vals: Vec<String> = (0..k)
                    .map(|_| format!("({}, {})", r.range(900, 999), lit(r)))
                    .collect();
                plain(format!(
                    "INSERT INTO {t} ({id}, {c}) VALUES {}",
                    vals.join(", ")
                ))
            }
            1 => {
                let vals: Vec<String> = (0..m.columns.len())
                    .map(|i| {
                        if i == 0 {
                            r.range(900, 999).to_string()
                        } else {
                            lit(r)
                        }
                    })
                    .collect();
                plain(format!("INSERT INTO {t} VALUES ({})", vals.join(", ")))
            }
            _ => {
                let c = quote_ident(r.pick(&cols).as_str());
                let src = &models[r.below(models.len())];
                let sc = quote_ident(r.pick(&src.columns).as_str());
                plain(format!(
                    "INSERT INTO {t} ({id}, {c}) SELECT {} + 1000, {sc} FROM {} WHERE {}",
                    quote_ident(&src.columns[0]),
                    quote_ident(&src.name),
                    cond(r, src)
                ))
            }
        }
    } else if roll < 76 {
        *alter_seq += 1;
        let col = format!("x{}", *alter_seq);
        let ty = *r.pick(&["TEXT", "INTEGER", "REAL", "DECIMAL(10,2)", "VARCHAR(20)"]);
        Stmt::Data {
            sqawk: format!("ALTER TABLE {t} ADD COLUMN {col} {ty}"),
            // No declared type: SQLite would apply the affinity, which
            // sqawk records but never applies.
            oracle: format!("ALTER TABLE {t} ADD COLUMN {col}"),
            select: false,
            alter: Some(ti),
        }
    } else if roll < 80 {
        Stmt::Data {
            sqawk: format!("TRUNCATE TABLE {t}"),
            oracle: format!("DELETE FROM {t}"),
            select: false,
            alter: None,
        }
    } else {
        let s = if r.chance(0.5) {
            format!("SELECT COUNT(*) FROM {t}")
        } else {
            format!("SELECT * FROM {t}")
        };
        Stmt::Data {
            sqawk: s.clone(),
            oracle: s,
            select: true,
            alter: None,
        }
    }
}

fn lit(r: &mut Rng) -> String {
    loop {
        let e = *r.pick(EXPRS);
        if !e.contains('{') {
            return e.to_string();
        }
    }
}

/// A query result as sqawk prints it.
fn render_result(conn: &Connection, sql: &str, delim: u8, header: bool) -> Result<Vec<u8>> {
    let mut st = conn.prepare(sql)?;
    let n = st.column_count();
    let names: Vec<String> = st.column_names().iter().map(|s| s.to_string()).collect();
    let mut out = Vec::new();
    if header {
        write_record(&mut out, &names, delim, false, b"\n");
    }
    let mut rows = st.query([])?;
    while let Some(row) = rows.next()? {
        let fields = (0..n)
            .map(|i| row.get::<_, Value>(i).map(|v| field_text(conn, &v)))
            .collect::<rusqlite::Result<Vec<_>>>()?;
        write_record(&mut out, &fields, delim, false, b"\n");
    }
    Ok(out)
}

fn short(b: &[u8]) -> String {
    let s = String::from_utf8_lossy(b);
    let t: String = s.chars().take(600).collect();
    format!("{t:?}{}", if s.chars().count() > 600 { "..." } else { "" })
}

/// Where two byte strings first differ, with context.
fn first_difference(want: &[u8], got: &[u8]) -> String {
    let i = want
        .iter()
        .zip(got)
        .position(|(a, b)| a != b)
        .unwrap_or(want.len().min(got.len()));
    let from = i.saturating_sub(60);
    format!(
        "first difference at byte {i} (want {} bytes, got {}):\n      want ...{}\n      got  ...{}",
        want.len(),
        got.len(),
        short(&want[from..(i + 80).min(want.len())]),
        short(&got[from..(i + 80).min(got.len())])
    )
}

fn list_dir(dir: &Path) -> BTreeSet<String> {
    fs::read_dir(dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default()
}

fn set_readonly(path: &Path, ro: bool) {
    if let Ok(meta) = fs::metadata(path) {
        let mut p = meta.permissions();
        #[allow(clippy::permissions_set_readonly_false)]
        p.set_readonly(ro);
        let _ = fs::set_permissions(path, p);
    }
}

/// A finished run as the checks see it.
struct Ran {
    code: Option<i32>,
    stdout: Vec<u8>,
    stderr: String,
    timed_out: bool,
}

fn crash_of(r: &Ran) -> Option<String> {
    if r.timed_out {
        return Some("time limit".to_string());
    }
    if let Some(t) = crash_text(&r.stderr) {
        return Some(format!("stderr contains {t:?}: {}", r.stderr.trim()));
    }
    match r.code {
        None => Some("killed by a signal".to_string()),
        Some(0..=2) => None,
        Some(c) => Some(format!("exit status {c}")),
    }
}

fn one(
    seed: u64,
    it: usize,
    sqawk: &Path,
    root: &Path,
    timeout: Duration,
) -> Result<(Verdict, String)> {
    let mut r = Rng::stream(seed, it as u64 + 1000);
    let dir = root.join(format!("it{it}"));
    fs::create_dir_all(&dir)?;

    // Command-line options shared by every input file.
    let fsep: Option<u8> = match r.below(10) {
        0 | 1 => Some(b'\t'),
        2 => Some(b';'),
        3 => Some(b'|'),
        _ => None,
    };
    let comments = r.chance(0.35);
    let no_header = r.chance(0.15);
    let nfiles = r.range(1, 3) as usize;

    let mut models: Vec<Model> = Vec::new();
    let mut cells: Vec<Vec<Vec<String>>> = Vec::new();
    let mut specs: Vec<String> = Vec::new();
    let eol_pick = |r: &mut Rng| match r.below(10) {
        0..=3 => Eol::Lf,
        4..=7 => Eol::Crlf,
        _ => Eol::Mixed,
    };
    for k in 0..nfiles {
        let ext = *r.pick(&["csv", "csv", "tsv", "txt", "tab", "dat"]);
        let delim = fsep.unwrap_or(if ext == "tsv" || ext == "tab" {
            b'\t'
        } else {
            b','
        });
        let o = FileOpts {
            delim,
            header: !no_header,
            comments,
            eol: eol_pick(&mut r),
            bom: r.chance(0.15),
            final_eol: r.chance(0.8),
            // Without a header the first row sets the width, so every row
            // must be complete for the file to read back.
            ragged: !no_header && r.chance(0.25),
        };
        let ncols = r.range(2, 6) as usize;
        let nrows = if r.chance(0.08) {
            0
        } else if r.chance(0.1) {
            r.range(100, 300) as usize
        } else {
            r.range(1, 25) as usize
        };
        let file_name = format!("t{k}.{ext}");
        let explicit = r.chance(0.3);
        let name = if explicit {
            format!("n{k}")
        } else {
            format!("t{k}")
        };
        let mut b = build_file(
            &mut r,
            seed,
            &name,
            &o,
            ncols,
            if no_header { nrows.max(1) } else { nrows },
        );
        let path = dir.join(&file_name);
        fs::write(&path, &b.bytes)?;
        let shown = if r.chance(0.5) {
            file_name.clone()
        } else {
            path.to_string_lossy().into_owned()
        };
        b.model.path = path;
        specs.push(if explicit {
            format!("{name}={shown}")
        } else {
            shown
        });
        models.push(b.model);
        cells.push(b.cells);
    }
    let ncli = models.len();
    // Files opened by CREATE TABLE ... LOCATION.
    let mut creates: Vec<String> = Vec::new();
    let nloc = if r.chance(0.25) { 1 } else { 0 } + if r.chance(0.1) { 1 } else { 0 };
    for k in 0..nloc {
        let existing = k == 0 && r.chance(0.8);
        let ext = *r.pick(&["csv", "tsv", "txt"]);
        let delim = *r.pick(b",\t;|");
        let header = r.chance(0.8);
        let lcomments = r.chance(0.3);
        let ncols = r.range(2, 5) as usize;
        let name = format!("l{k}");
        let file_name = format!("loc{k}.{ext}");
        let path = dir.join(&file_name);
        let o = FileOpts {
            delim,
            header,
            comments: lcomments,
            eol: eol_pick(&mut r),
            bom: r.chance(0.1),
            final_eol: r.chance(0.8),
            ragged: r.chance(0.25),
        };
        let types: Vec<&'static str> = (0..ncols)
            .map(|i| {
                if i == 0 {
                    "INT"
                } else {
                    *r.pick(&["TEXT", "INTEGER", "REAL", "DECIMAL(10,2)", "STRING"])
                }
            })
            .collect();
        let mut b = if existing {
            let nrows = r.range(if header { 0 } else { 1 }, 20) as usize;
            let b = build_file(&mut r, seed, &name, &o, ncols, nrows);
            fs::write(&path, &b.bytes)?;
            b
        } else {
            Built {
                bytes: Vec::new(),
                model: Model {
                    name: name.clone(),
                    path: PathBuf::new(),
                    original: None,
                    delim,
                    header,
                    comments: lcomments,
                    crlf: false,
                    bom: false,
                    columns: Vec::new(),
                    header_raw: None,
                    raw_valid: true,
                    rows: Vec::new(),
                    head: Vec::new(),
                    before: BTreeMap::new(),
                    tail: Vec::new(),
                    location: None,
                },
                cells: Vec::new(),
            }
        };
        // The declared names are the schema; the header line is kept as read.
        b.model.columns = std::iter::once("id".to_string())
            .chain((1..ncols).map(|i| format!("v{i}")))
            .collect();
        let shown = if r.chance(0.5) {
            file_name
        } else {
            path.to_string_lossy().into_owned()
        };
        b.model.path = path;
        b.model.location = Some(Location { existing });
        let decl: Vec<String> = b
            .model
            .columns
            .iter()
            .zip(&types)
            .map(|(c, t)| format!("{c} {t}"))
            .collect();
        creates.push(format!(
            "CREATE TABLE {name} ({}) LOCATION {} WITH (DELIMITER={}, HEADER={}, COMMENTS={})",
            decl.join(", "),
            sql_str(&shown),
            delim_sql(delim),
            header,
            lcomments
        ));
        models.push(b.model);
        cells.push(b.cells);
    }

    // The oracle: every table, as sqawk loads it, in untyped columns.
    let tables: Vec<Table> = models
        .iter()
        .zip(&cells)
        .map(|(m, c)| {
            let mut t = Table::new(&m.name, &[]);
            t.columns = m.columns.clone();
            t.rows = c.clone();
            t
        })
        .collect();
    let oracle = Oracle::new(&tables)?;
    let conn = oracle.conn();
    for m in models.iter_mut() {
        let vals = table_rows(conn, &m.name)?;
        for row in m.rows.iter_mut() {
            row.vals = vals[&row.rowid].clone();
        }
    }
    let originals: Vec<Option<Vec<u8>>> = models.iter().map(|m| m.original.clone()).collect();

    // The statement sequence, validated statement by statement on the
    // oracle (a statement SQLite rejects is dropped).
    let mut script: Vec<Stmt> = creates
        .iter()
        .map(|c| Stmt::Data {
            sqawk: c.clone(),
            oracle: String::new(),
            select: false,
            alter: None,
        })
        .collect();
    let out_delim = fsep.unwrap_or(b',');
    let mut expected_stdout = Vec::new();
    let mut in_txn: Option<Vec<Model>> = None;
    let mut committed_change: BTreeSet<usize> = BTreeSet::new();
    let mut txn_change: BTreeSet<usize> = BTreeSet::new();
    let mut alter_seq = 0usize;
    let nstmts = r.range(1, 8) as usize;
    let readonly_target = if r.chance(0.08) {
        Some(r.below(ncli))
    } else {
        None
    };
    let force_write = readonly_target.is_some() && r.chance(0.7);
    for _ in 0..nstmts {
        let roll = r.below(100);
        let stmt = if roll < 12 {
            if in_txn.is_none() {
                Stmt::Begin(r.pick_copy(&["BEGIN", "BEGIN TRANSACTION", "BEGIN IMMEDIATE"]))
            } else if r.chance(0.5) {
                Stmt::End {
                    sqawk: r.pick_copy(&["COMMIT", "END", "COMMIT TRANSACTION"]),
                    commit: true,
                }
            } else {
                Stmt::End {
                    sqawk: r.pick_copy(&["ROLLBACK", "ROLLBACK TRANSACTION"]),
                    commit: false,
                }
            }
        } else {
            let ti = r.below(models.len());
            data_stmt(&mut r, &models, ti, &mut alter_seq)
        };
        apply(
            conn,
            &stmt,
            &mut models,
            &mut in_txn,
            &mut committed_change,
            &mut txn_change,
            &mut expected_stdout,
            out_delim,
            &mut script,
        )?;
    }
    if let Some(ti) = readonly_target.filter(|_| force_write) {
        if in_txn.is_some() {
            let s = Stmt::End {
                sqawk: "COMMIT",
                commit: true,
            };
            apply(
                conn,
                &s,
                &mut models,
                &mut in_txn,
                &mut committed_change,
                &mut txn_change,
                &mut expected_stdout,
                out_delim,
                &mut script,
            )?;
        }
        let m = &models[ti];
        let s = format!(
            "INSERT INTO {} ({}) VALUES (777)",
            quote_ident(&m.name),
            quote_ident(&m.columns[0])
        );
        let s = Stmt::Data {
            sqawk: s.clone(),
            oracle: s,
            select: false,
            alter: None,
        };
        apply(
            conn,
            &s,
            &mut models,
            &mut in_txn,
            &mut committed_change,
            &mut txn_change,
            &mut expected_stdout,
            out_delim,
            &mut script,
        )?;
    }
    // A transaction left open is rolled back before writeback.
    if let Some(saved) = in_txn.take() {
        conn.execute_batch("ROLLBACK")?;
        models = saved;
        txn_change.clear();
    }
    if script.is_empty() {
        let m = &models[0];
        let s = format!(
            "UPDATE {} SET {} = {} WHERE 0",
            quote_ident(&m.name),
            quote_ident(&m.columns[0]),
            quote_ident(&m.columns[0])
        );
        script.push(Stmt::Data {
            sqawk: s.clone(),
            oracle: s,
            select: false,
            alter: None,
        });
    }

    // The command line.
    let mut args: Vec<OsString> = Vec::new();
    let mut flags_desc = Vec::new();
    if let Some(d) = fsep {
        args.push("-F".into());
        let s = if d == b'\t' && r.chance(0.5) {
            "\\t".to_string()
        } else {
            (d as char).to_string()
        };
        flags_desc.push(format!("-F {s:?}"));
        args.push(s.into());
    }
    if comments {
        args.push("--comments".into());
        flags_desc.push("--comments".to_string());
    }
    if no_header {
        args.push("--no-header".into());
        flags_desc.push("--no-header".to_string());
    }
    args.push("-w".into());
    let texts: Vec<String> = script.iter().map(Stmt::sqawk).collect();
    let mut stdin = None;
    let mut extra_files: BTreeSet<String> = BTreeSet::new();
    match r.below(4) {
        0 => {
            args.push("-s".into());
            args.push(texts.join("; ").into());
        }
        1 => {
            for t in &texts {
                args.push("-s".into());
                args.push(t.into());
            }
        }
        2 => {
            fs::write(dir.join("script.sql"), texts.join(";\n") + ";\n")?;
            extra_files.insert("script.sql".to_string());
            args.push("-f".into());
            args.push("script.sql".into());
        }
        _ => {
            stdin = Some((texts.join(";\n") + "\n").into_bytes());
            args.push("-f".into());
            args.push("-".into());
        }
    }
    for s in &specs {
        args.push(s.into());
    }
    let readonly = readonly_target.map(|i| models[i].path.clone());
    if let Some(p) = &readonly {
        set_readonly(p, true);
    }
    let mut desc = format!(
        "flags [{}] files [{}]{}\n  script:\n",
        flags_desc.join(" "),
        specs.join(" "),
        readonly
            .as_ref()
            .map(|p| format!(
                " read-only {}",
                p.file_name().unwrap_or_default().to_string_lossy()
            ))
            .unwrap_or_default()
    );
    for t in &texts {
        desc.push_str(&format!("    {t};\n"));
    }
    let before_listing = list_dir(&dir);
    let mut job = Job::new(sqawk, args.clone());
    job.stdin = stdin;
    job.cwd = Some(dir.clone());
    job.limit = timeout;
    let f = execute(&job);
    if let Some(p) = &readonly {
        set_readonly(p, false);
    }
    let ran = Ran {
        code: f.code,
        stderr: f.stderr_text(),
        stdout: f.stdout,
        timed_out: f.timed_out,
    };
    if let Some(e) = f.spawn_error {
        anyhow::bail!(e);
    }
    if let Some(c) = crash_of(&ran) {
        return Ok((Verdict::Crash(c), desc));
    }

    // A read-only target whose data a kept statement changed must refuse
    // the run. One that only a statement changing nothing touched (UPDATE
    // to the same values, TRUNCATE of an empty table) may be refused too.
    let must_refuse = readonly_target.is_some_and(|i| committed_change.contains(&i));
    let refused = must_refuse || (readonly_target.is_some() && ran.code != Some(0));
    let mut problems = Vec::new();
    if refused {
        let lines: Vec<&str> = ran.stderr.lines().collect();
        if ran.code != Some(1)
            || lines.len() != 1
            || !lines[0].starts_with("sqawk: ")
            || !lines[0].contains("read-only")
        {
            problems.push(format!(
                "read-only target changed: want exit 1 and one `sqawk: ... read-only` line, got {:?} {:?}",
                ran.code,
                ran.stderr.trim()
            ));
        }
        for (m, orig) in models.iter().zip(&originals) {
            let now = fs::read(&m.path).ok();
            if now != *orig {
                problems.push(format!(
                    "{} written although the run was refused",
                    m.path.file_name().unwrap_or_default().to_string_lossy()
                ));
            }
        }
    } else if ran.code != Some(0) || !ran.stderr.is_empty() {
        problems.push(format!(
            "sqawk failed ({:?}): {}",
            ran.code,
            ran.stderr.trim()
        ));
    } else {
        for (i, m) in models.iter().enumerate() {
            let fname = m
                .path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .into_owned();
            let now = fs::read(&m.path).ok();
            let created_new = m.location.as_ref().is_some_and(|l| !l.existing);
            // A new LOCATION file is created by --write even when nothing
            // was inserted.
            let want = Some(m.render(conn));
            if !committed_change.contains(&i) && !created_new {
                // Self-check of the model: nothing kept changed it, so it
                // must reproduce the file exactly.
                if want != originals[i] {
                    problems.push(format!(
                        "tsq model of unchanged {fname} differs from the file: {}",
                        first_difference(
                            originals[i].as_deref().unwrap_or(b""),
                            want.as_deref().unwrap_or(b"")
                        )
                    ));
                }
            }
            if now != want {
                problems.push(format!(
                    "{fname}: {}\n      original {}",
                    first_difference(
                        want.as_deref().unwrap_or(b""),
                        now.as_deref().unwrap_or(b"")
                    ),
                    short(originals[i].as_deref().unwrap_or(b""))
                ));
            }
        }
        if ran.stdout != expected_stdout {
            problems.push(format!(
                "SELECT output: {}",
                first_difference(&expected_stdout, &ran.stdout)
            ));
        }
    }
    // No stray files: inputs, LOCATION files, the script.
    let after = list_dir(&dir);
    let mut allowed = before_listing.clone();
    for m in &models {
        if !refused {
            allowed.insert(
                m.path
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .into_owned(),
            );
        }
    }
    allowed.extend(extra_files);
    let stray: Vec<&String> = after.difference(&allowed).collect();
    if !stray.is_empty() {
        problems.push(format!("stray files left: {stray:?}"));
    }
    // Re-read every written file: its rows must be the oracle's.
    if problems.is_empty() && !refused {
        for (i, m) in models.iter().enumerate() {
            if !committed_change.contains(&i) && m.location.as_ref().is_none_or(|l| l.existing) {
                continue;
            }
            // A headerless file with no rows names no columns: it cannot
            // be read as an input file.
            if m.rows.is_empty() && !m.header {
                continue;
            }
            let want = render_result(
                conn,
                &format!("SELECT * FROM {} ORDER BY rowid", quote_ident(&m.name)),
                b',',
                false,
            )?;
            let mut a: Vec<OsString> = vec![
                "--output-delimiter".into(),
                ",".into(),
                "--no-output-header".into(),
            ];
            if i < ncli {
                if let Some(d) = fsep {
                    a.push("-F".into());
                    a.push((d as char).to_string().into());
                }
                if comments {
                    a.push("--comments".into());
                }
                if no_header {
                    a.push("--no-header".into());
                }
                a.push("-s".into());
                a.push(format!("SELECT * FROM {}", quote_ident(&m.name)).into());
                a.push(format!("{}={}", m.name, m.path.display()).into());
            } else {
                let decl: Vec<String> = m
                    .columns
                    .iter()
                    .map(|c| format!("{} TEXT", quote_ident(c)))
                    .collect();
                a.push("-s".into());
                a.push(
                    format!(
                        "CREATE TABLE {} ({}) LOCATION {} WITH (DELIMITER={}, HEADER={}, COMMENTS={}); SELECT * FROM {}",
                        quote_ident(&m.name),
                        decl.join(", "),
                        sql_str(&m.path.to_string_lossy()),
                        delim_sql(m.delim),
                        m.header,
                        m.comments,
                        quote_ident(&m.name)
                    )
                    .into(),
                );
                // sqawk needs one input file; LOCATION ignores the
                // command-line layout flags, so a plain one does.
                let dummy = dir.join("zz_dummy.csv");
                fs::write(&dummy, "x\n1\n")?;
                a.push(dummy.into_os_string());
            }
            let mut job = Job::new(sqawk, a);
            job.limit = timeout;
            let g = execute(&job);
            let rr = Ran {
                code: g.code,
                stderr: g.stderr_text(),
                stdout: g.stdout,
                timed_out: g.timed_out,
            };
            if let Some(c) = crash_of(&rr) {
                return Ok((Verdict::Crash(format!("re-read of {}: {c}", m.name)), desc));
            }
            if rr.code != Some(0) {
                problems.push(format!(
                    "re-read of {} failed: {}",
                    m.name,
                    rr.stderr.trim()
                ));
                continue;
            }
            let want_recs = raw_records(&String::from_utf8_lossy(&want));
            let got_recs = raw_records(&String::from_utf8_lossy(&rr.stdout));
            if want_recs != got_recs {
                problems.push(format!(
                    "re-read of {} gives other rows: {}",
                    m.name,
                    first_difference(&want, &rr.stdout)
                ));
            }
        }
    }
    let _ = fs::remove_dir_all(&dir);
    if problems.is_empty() {
        Ok((Verdict::Ok, desc))
    } else {
        Ok((Verdict::Bad(problems.join("\n  ")), desc))
    }
}

/// Run `stmt` on the oracle and the model; append it to `script` unless
/// SQLite rejects it.
#[allow(clippy::too_many_arguments)]
fn apply(
    conn: &Connection,
    stmt: &Stmt,
    models: &mut Vec<Model>,
    in_txn: &mut Option<Vec<Model>>,
    committed: &mut BTreeSet<usize>,
    txn_change: &mut BTreeSet<usize>,
    stdout: &mut Vec<u8>,
    out_delim: u8,
    script: &mut Vec<Stmt>,
) -> Result<()> {
    match stmt {
        Stmt::Begin(_) => {
            conn.execute_batch("BEGIN")?;
            *in_txn = Some(models.clone());
            txn_change.clear();
        }
        Stmt::End { commit: true, .. } => {
            conn.execute_batch("COMMIT")?;
            *in_txn = None;
            committed.append(txn_change);
        }
        Stmt::End { commit: false, .. } => {
            conn.execute_batch("ROLLBACK")?;
            if let Some(saved) = in_txn.take() {
                *models = saved;
            }
            txn_change.clear();
        }
        Stmt::Data {
            oracle,
            select,
            alter,
            ..
        } => {
            if *select {
                match render_result(conn, oracle, out_delim, true) {
                    Ok(out) => stdout.extend_from_slice(&out),
                    Err(_) => return Ok(()),
                }
            } else {
                let before: Vec<BTreeMap<i64, Vec<Value>>> = models
                    .iter()
                    .map(|m| table_rows(conn, &m.name))
                    .collect::<Result<_>>()?;
                if conn.execute_batch(oracle).is_err() {
                    return Ok(());
                }
                if let Some(ti) = alter {
                    let m = &mut models[*ti];
                    m.raw_valid = false;
                    if let Some(col) = oracle.split_whitespace().last() {
                        m.columns.push(col.to_string());
                    }
                }
                for (i, m) in models.iter_mut().enumerate() {
                    m.sync(conn)?;
                    let after = table_rows(conn, &m.name)?;
                    if after != before[i] || *alter == Some(i) {
                        if in_txn.is_some() {
                            txn_change.insert(i);
                        } else {
                            committed.insert(i);
                        }
                    }
                }
            }
        }
    }
    script.push(stmt.clone());
    Ok(())
}

/// The kill-during-write trials (Unix: `kill -KILL`, no libc).
#[cfg(unix)]
mod kill {
    use super::*;
    use std::process::{Command, Stdio};
    use std::time::Instant;

    pub struct Big {
        pub dir: PathBuf,
        pub original: Vec<u8>,
        pub new: Vec<u8>,
        /// Duration of an undisturbed run.
        pub time: Duration,
    }

    pub const SQL: &str = "UPDATE big SET c1 = c1 || 'x' WHERE id % 3 = 0";

    pub fn prepare(sqawk: &Path, root: &Path, seed: u64) -> Result<Big> {
        let dir = root.join("kill");
        fs::create_dir_all(&dir)?;
        let mut r = Rng::stream(seed, 7);
        let mut data = b"id,c1,c2,c3\n".to_vec();
        for i in 1..=60_000 {
            data.extend_from_slice(
                format!(
                    "{i},{},{}.5,word{}\n",
                    r.range(0, 1_000_000),
                    r.range(0, 9999),
                    r.range(0, 99)
                )
                .as_bytes(),
            );
        }
        let path = dir.join("big.csv");
        fs::write(&path, &data)?;
        let start = Instant::now();
        let st = Command::new(sqawk)
            .args(["-w", "-s", SQL, "big.csv"])
            .current_dir(&dir)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .context("run sqawk")?;
        let time = start.elapsed();
        anyhow::ensure!(st.success(), "reference --write run failed");
        let new = fs::read(&path)?;
        anyhow::ensure!(new != data, "reference run changed nothing");
        Ok(Big {
            dir,
            original: data,
            new,
            time,
        })
    }

    pub struct Trial {
        pub outcome: &'static str,
        pub leftovers: Vec<String>,
        pub problem: Option<String>,
    }

    pub fn trial(sqawk: &Path, big: &Big, r: &mut Rng) -> Result<Trial> {
        let path = big.dir.join("big.csv");
        fs::write(&path, &big.original)?;
        let mut child = Command::new(sqawk)
            .args(["-w", "-s", SQL, "big.csv"])
            .current_dir(&big.dir)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .context("spawn sqawk")?;
        let start = Instant::now();
        let on_temp = r.chance(0.5);
        let delay = Duration::from_secs_f64(big.time.as_secs_f64() * 1.2 * r.unit());
        let after_temp = Duration::from_micros(r.range(0, 3000) as u64);
        let mut seen_temp: Option<Instant> = None;
        let mut finished = false;
        loop {
            if let Some(_s) = child.try_wait()? {
                finished = true;
                break;
            }
            let now = Instant::now();
            if on_temp {
                if seen_temp.is_none() && list_dir(&big.dir).iter().any(|n| n.starts_with(".tmp")) {
                    seen_temp = Some(now);
                }
                if seen_temp.is_some_and(|t| now - t >= after_temp) || now - start > big.time * 4 {
                    break;
                }
                std::thread::sleep(Duration::from_micros(100));
            } else {
                if now - start >= delay {
                    break;
                }
                std::thread::sleep(Duration::from_micros(200));
            }
        }
        if !finished {
            // The child is not reaped yet, so its pid is still its own.
            let _ = Command::new("kill")
                .args(["-KILL", &child.id().to_string()])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
        let status = child.wait()?;
        let now = fs::read(&path)?;
        let outcome = if now == big.original {
            "old"
        } else if now == big.new {
            "new"
        } else {
            "partial"
        };
        let mut leftovers = Vec::new();
        let mut problem = None;
        for name in list_dir(&big.dir) {
            if name == "big.csv" {
                continue;
            }
            if !name.starts_with(".tmp") {
                problem = Some(format!("unexpected file left: {name}"));
            }
            let _ = fs::remove_file(big.dir.join(&name));
            leftovers.push(name);
        }
        if outcome == "partial" {
            problem = Some(format!(
                "file is neither old nor new after kill ({} bytes; old {}, new {})",
                now.len(),
                big.original.len(),
                big.new.len()
            ));
        }
        if finished && (status.code() != Some(0) || outcome != "new") {
            problem = Some(format!(
                "undisturbed run ended {status:?} with {outcome} content"
            ));
        }
        Ok(Trial {
            outcome: if finished { "finished" } else { outcome },
            leftovers,
            problem,
        })
    }
}

/// What the kill-during-write trials saw.
#[derive(Default)]
struct KillStats {
    /// `old` / `new` (file content after the kill) or `finished` (the run
    /// ended before the kill).
    outcomes: BTreeMap<&'static str, usize>,
    leftover_count: usize,
    leftover_names: BTreeSet<String>,
}

pub fn run(seed: u64, sqawk: &Path, opts: &WbOptions, verbose: bool) -> Result<i32> {
    let root = tempfile::Builder::new().prefix("tsq-wb-").tempdir()?;
    let (mut ok, mut bad, mut crash) = (0usize, 0usize, 0usize);
    #[cfg(unix)]
    let mut big: Option<kill::Big> = None;
    #[cfg_attr(not(unix), allow(unused_mut))]
    let mut ks = KillStats::default();
    let mut kr = Rng::stream(seed, 9);
    for it in 0..opts.iterations {
        let (v, desc) = one(seed, it, sqawk, root.path(), opts.timeout)
            .with_context(|| format!("iteration {it}"))?;
        match v {
            Verdict::Ok => {
                ok += 1;
                if verbose {
                    println!("ok it={it} {}", desc.lines().next().unwrap_or(""));
                }
            }
            Verdict::Bad(p) => {
                bad += 1;
                println!("BAD it={it} (tsq wb --seed {seed} --only {it})\n  {p}\n  {desc}");
            }
            Verdict::Crash(p) => {
                crash += 1;
                println!("CRASH it={it} (tsq wb --seed {seed} --only {it})\n  {p}\n  {desc}");
            }
        }
        let do_kill = kr.chance(opts.kill_rate);
        #[cfg(unix)]
        if do_kill {
            if big.is_none() {
                big = Some(kill::prepare(sqawk, root.path(), seed)?);
            }
            let t = kill::trial(sqawk, big.as_ref().expect("prepared"), &mut kr)?;
            *ks.outcomes.entry(t.outcome).or_default() += 1;
            ks.leftover_count += t.leftovers.len();
            ks.leftover_names.extend(t.leftovers.iter().map(|n| {
                if n.starts_with(".tmp") {
                    ".tmp*".to_string()
                } else {
                    n.clone()
                }
            }));
            if let Some(p) = t.problem {
                bad += 1;
                println!("BAD kill trial after it={it}: {p}");
            }
        }
        #[cfg(not(unix))]
        let _ = do_kill;
    }
    println!(
        "WB SUMMARY {{ok: {ok}, bad: {bad}, crash: {crash}}} kill_trials {} {:?} leftover_temp_files {} {:?}",
        ks.outcomes.values().sum::<usize>(),
        ks.outcomes,
        ks.leftover_count,
        ks.leftover_names
    );
    Ok(if bad + crash > 0 { 1 } else { 0 })
}

/// Replay one iteration (`--only N`), printing its script.
pub fn run_one(seed: u64, it: usize, sqawk: &Path, opts: &WbOptions) -> Result<i32> {
    let root = tempfile::Builder::new().prefix("tsq-wb-").tempdir()?;
    let (v, desc) = one(seed, it, sqawk, root.path(), opts.timeout)?;
    println!("{desc}");
    match v {
        Verdict::Ok => {
            println!("WB SUMMARY {{ok: 1, bad: 0, crash: 0}}");
            Ok(0)
        }
        Verdict::Bad(p) => {
            println!("BAD\n  {p}\nWB SUMMARY {{ok: 0, bad: 1, crash: 0}}");
            Ok(1)
        }
        Verdict::Crash(p) => {
            println!("CRASH\n  {p}\nWB SUMMARY {{ok: 0, bad: 0, crash: 1}}");
            Ok(1)
        }
    }
}
