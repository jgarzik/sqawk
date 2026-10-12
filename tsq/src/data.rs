//! Tables as tsq holds them, and the delimited-file writer.
//!
//! A cell is the exact field text sqawk will read; the empty string is an
//! empty field (NULL to sqawk). The writer is lossless: whatever the format
//! options, sqawk reads every cell back as the same text, so the oracle can
//! load the in-memory cells instead of re-parsing the files.

use std::collections::BTreeSet;
use std::fs;
use std::io::{BufWriter, Write};
use std::path::Path;

use anyhow::{Context, Result};

use crate::rng::Rng;

#[derive(Clone, Debug)]
pub struct Table {
    pub name: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
    /// Empty cells written as a quoted `""` instead of nothing (both read
    /// as NULL; the edge injector uses this to exercise the reader).
    pub quoted_empty: BTreeSet<(usize, usize)>,
}

impl Table {
    pub fn new(name: &str, columns: &[&str]) -> Self {
        Table {
            name: name.to_string(),
            columns: columns.iter().map(|c| c.to_string()).collect(),
            rows: Vec::new(),
            quoted_empty: BTreeSet::new(),
        }
    }
}

/// How a table is laid out on disk.
#[derive(Clone, Debug)]
pub struct Format {
    pub delimiter: u8,
    pub crlf: bool,
    pub bom: bool,
    pub header: bool,
    /// Probability that a row is written short: its trailing empty fields
    /// are left off (sqawk pads a short row with NULL, so the data is the
    /// same).
    pub ragged_rate: f64,
}

impl Default for Format {
    fn default() -> Self {
        Format {
            delimiter: b',',
            crlf: false,
            bom: false,
            header: true,
            ragged_rate: 0.0,
        }
    }
}

impl Format {
    /// The file extension sqawk picks the delimiter from.
    pub fn extension(&self) -> &'static str {
        if self.delimiter == b'\t' {
            "tsv"
        } else {
            "csv"
        }
    }
}

fn needs_quotes(s: &str, delimiter: u8) -> bool {
    s.bytes()
        .any(|b| b == delimiter || b == b'"' || b == b'\r' || b == b'\n')
}

fn push_field(out: &mut Vec<u8>, s: &str, delimiter: u8, quote_empty: bool) {
    if (s.is_empty() && quote_empty) || needs_quotes(s, delimiter) {
        out.push(b'"');
        out.extend_from_slice(s.replace('"', "\"\"").as_bytes());
        out.push(b'"');
    } else {
        out.extend_from_slice(s.as_bytes());
    }
}

/// The bytes of `table` in format `fmt`. `rng` decides which rows are
/// written short when `fmt.ragged_rate` is positive.
pub fn render_table(table: &Table, fmt: &Format, rng: &mut Rng) -> Vec<u8> {
    let eol: &[u8] = if fmt.crlf { b"\r\n" } else { b"\n" };
    let mut out = Vec::new();
    if fmt.bom {
        out.extend_from_slice(b"\xef\xbb\xbf");
    }
    if fmt.header {
        for (i, c) in table.columns.iter().enumerate() {
            if i > 0 {
                out.push(fmt.delimiter);
            }
            push_field(&mut out, c, fmt.delimiter, false);
        }
        out.extend_from_slice(eol);
    }
    for (r, row) in table.rows.iter().enumerate() {
        let mut n = row.len();
        if fmt.ragged_rate > 0.0 && rng.chance(fmt.ragged_rate) {
            while n > 1 && row[n - 1].is_empty() && !table.quoted_empty.contains(&(r, n - 1)) {
                n -= 1;
            }
        }
        // A line holding one empty field would be a blank line, which the
        // reader skips: quote it so the row survives.
        let lone_empty = n == 1 && row[0].is_empty();
        for (c, cell) in row[..n].iter().enumerate() {
            if c > 0 {
                out.push(fmt.delimiter);
            }
            let quote_empty = lone_empty || table.quoted_empty.contains(&(r, c));
            push_field(&mut out, cell, fmt.delimiter, quote_empty);
        }
        out.extend_from_slice(eol);
    }
    out
}

/// Write `table` to `dir/<name>.<ext>`; returns the path.
pub fn write_table(
    dir: &Path,
    table: &Table,
    fmt: &Format,
    rng: &mut Rng,
) -> Result<std::path::PathBuf> {
    let path = dir.join(format!("{}.{}", table.name, fmt.extension()));
    let file = fs::File::create(&path).with_context(|| format!("create {}", path.display()))?;
    let mut w = BufWriter::new(file);
    w.write_all(&render_table(table, fmt, rng))?;
    w.flush()?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quoting_and_short_rows() {
        let mut t = Table::new("t", &["a", "b", "c"]);
        t.rows
            .push(vec!["x,y".into(), "say \"hi\"".into(), "".into()]);
        t.rows.push(vec!["1".into(), "".into(), "".into()]);
        t.quoted_empty.insert((1, 1));
        let mut rng = Rng::new(1);
        let fmt = Format {
            ragged_rate: 1.0,
            ..Format::default()
        };
        let s = String::from_utf8(render_table(&t, &fmt, &mut rng)).unwrap();
        assert_eq!(s, "a,b,c\n\"x,y\",\"say \"\"hi\"\"\"\n1,\"\"\n");
    }
}
