//! The reference engine: SQLite (rusqlite's bundled build) holding exactly
//! the values sqawk loads, rendering its answers exactly as sqawk prints.
//!
//! Values: sqawk types each loaded cell by itself (src/table.rs
//! `infer_value`): an empty field is NULL; canonical integer text (what i64
//! formatting prints back, so not `007`, `-0`, `+3`) is INTEGER; text equal
//! to SQLite's `CAST(x AS TEXT)` of its double (`1.5`, `2.0`, `1.0e+20`, not
//! `1.50`, `1e3`, `.5`) is REAL; anything else is TEXT, verbatim. `infer`
//! mirrors that rule from its documentation (doc/sql_reference.md, "Data
//! Types"), independently of sqawk's code.
//!
//! Columns are declared with no type, so they have no affinity and SQLite
//! stores each value as inserted: one column can hold integers, reals and
//! text, as in sqawk.
//!
//! Output: NULL is an empty field, integers print in decimal, reals as
//! SQLite's own `CAST(x AS TEXT)` (`1.0`, `0.3`, `1.0e+20`, `Inf`), text
//! verbatim; records are built by `render::record`. NULL and the empty
//! string both print as an empty field ("Differences from SQLite", NULL vs
//! '' row), so neither side can tell them apart.

use std::time::{Duration, Instant};

use anyhow::Result;
use rusqlite::backup::Backup;
use rusqlite::types::{Value, ValueRef};
use rusqlite::Connection;

use super::render::{record, ResultSet};
use crate::data::Table;

/// SQLite's `CAST(f AS TEXT)`: what sqawk prints for a real.
pub fn real_text(conn: &Connection, f: f64) -> String {
    conn.prepare_cached("SELECT CAST(?1 AS TEXT)")
        .and_then(|mut s| s.query_row([f], |r| r.get::<_, Option<String>>(0)))
        .ok()
        .flatten()
        .unwrap_or_default()
}

fn canonical_int(s: &str) -> Option<i64> {
    let digits = s.strip_prefix('-').unwrap_or(s);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) || s == "-0" {
        return None;
    }
    if digits.len() > 1 && digits.starts_with('0') {
        return None;
    }
    s.parse::<i64>().ok()
}

/// The value sqawk loads from field text `s`.
pub fn infer(conn: &Connection, s: &str) -> Value {
    if s.is_empty() {
        return Value::Null;
    }
    if let Some(i) = canonical_int(s) {
        return Value::Integer(i);
    }
    if s.contains('.')
        && s.bytes()
            .all(|b| b.is_ascii_digit() || matches!(b, b'-' | b'+' | b'.' | b'e'))
    {
        if let Ok(f) = s.parse::<f64>() {
            if f.is_finite() && real_text(conn, f) == s {
                return Value::Real(f);
            }
        }
    }
    Value::Text(s.to_string())
}

fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// `sql` split into statements at `;` outside quotes (the corpus format's
/// rule, as the Python harness split them).
pub fn split_statements(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut quote: Option<char> = None;
    for ch in sql.chars() {
        match quote {
            Some(q) if ch == q => quote = None,
            Some(_) => {}
            None if ch == '\'' || ch == '"' => quote = Some(ch),
            None => {}
        }
        if ch == ';' && quote.is_none() {
            if !cur.trim().is_empty() {
                out.push(std::mem::take(&mut cur));
            }
            cur.clear();
        } else {
            cur.push(ch);
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur);
    }
    out
}

#[derive(Clone, Debug)]
pub enum SqlResult {
    Ok(Vec<ResultSet>),
    Err(String),
}

pub struct Oracle {
    conn: Connection,
}

impl Oracle {
    /// A database holding `tables` as sqawk would load them.
    pub fn new(tables: &[Table]) -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch("BEGIN")?;
        for t in tables {
            let cols: Vec<String> = t.columns.iter().map(|c| quote_ident(c)).collect();
            conn.execute_batch(&format!(
                "CREATE TABLE {} ({})",
                quote_ident(&t.name),
                cols.join(", ")
            ))?;
            let marks: Vec<String> = (1..=cols.len()).map(|i| format!("?{i}")).collect();
            let mut ins = conn.prepare(&format!(
                "INSERT INTO {} VALUES ({})",
                quote_ident(&t.name),
                marks.join(", ")
            ))?;
            for row in &t.rows {
                let vals: Vec<Value> = (0..cols.len())
                    .map(|i| infer(&conn, row.get(i).map(String::as_str).unwrap_or("")))
                    .collect();
                ins.execute(rusqlite::params_from_iter(vals))?;
            }
        }
        conn.execute_batch("COMMIT")?;
        Ok(Oracle { conn })
    }

    /// Every value of `table`, as loaded, column by column.
    pub fn columns(&self, table: &str, ncols: usize) -> Result<Vec<Vec<Value>>> {
        let mut st = self
            .conn
            .prepare(&format!("SELECT * FROM {}", quote_ident(table)))?;
        let mut out = vec![Vec::new(); ncols];
        let mut rows = st.query([])?;
        while let Some(r) = rows.next()? {
            for (i, col) in out.iter_mut().enumerate() {
                col.push(r.get::<_, Value>(i)?);
            }
        }
        Ok(out)
    }

    /// Run `stmts` in order and collect every result set. A sequence that
    /// is not all SELECTs runs on a fresh copy of the database, so each
    /// sequence starts from the loaded data and transactions work as in a
    /// new session.
    pub fn run(&self, stmts: &[String], limit: Duration) -> SqlResult {
        let readonly = stmts.iter().all(|s| {
            let t = s.trim_start();
            t.len() >= 6 && t[..6].eq_ignore_ascii_case("select")
        });
        if readonly {
            return run_on(&self.conn, stmts, limit);
        }
        let mut copy = match Connection::open_in_memory() {
            Ok(c) => c,
            Err(e) => return SqlResult::Err(e.to_string()),
        };
        let copied = Backup::new(&self.conn, &mut copy)
            .and_then(|b| b.run_to_completion(1 << 30, Duration::ZERO, None));
        if let Err(e) = copied {
            return SqlResult::Err(format!("backup failed: {e}"));
        }
        run_on(&copy, stmts, limit)
    }
}

fn error_text(e: &rusqlite::Error) -> String {
    match e {
        rusqlite::Error::SqliteFailure(_, Some(m)) => m.clone(),
        other => other.to_string(),
    }
}

fn run_on(conn: &Connection, stmts: &[String], limit: Duration) -> SqlResult {
    let deadline = Instant::now() + limit;
    conn.progress_handler(10_000, Some(move || Instant::now() > deadline));
    let result = (|| -> rusqlite::Result<Vec<ResultSet>> {
        let mut sets = Vec::new();
        for (source, sql) in stmts.iter().enumerate() {
            let mut st = conn.prepare(sql)?;
            let ncol = st.column_count();
            let header: Vec<String> = st.column_names().iter().map(|s| s.to_string()).collect();
            let mut rows_out = Vec::new();
            let mut rows = st.query([])?;
            while let Some(row) = rows.next()? {
                if ncol == 0 {
                    continue;
                }
                let mut fields = Vec::with_capacity(ncol);
                for i in 0..ncol {
                    fields.push(match row.get_ref(i)? {
                        ValueRef::Null => String::new(),
                        ValueRef::Integer(v) => v.to_string(),
                        ValueRef::Real(f) => real_text(conn, f),
                        ValueRef::Text(b) => String::from_utf8_lossy(b).into_owned(),
                        ValueRef::Blob(b) => {
                            let hex: String = b.iter().map(|x| format!("{x:02X}")).collect();
                            format!("<blob {hex}>")
                        }
                    });
                }
                rows_out.push(record(&fields));
            }
            if ncol > 0 {
                sets.push(ResultSet {
                    header: record(&header),
                    rows: rows_out,
                    source,
                });
            }
        }
        Ok(sets)
    })();
    // Leave no transaction open on the shared connection.
    if !conn.is_autocommit() {
        let _ = conn.execute_batch("ROLLBACK");
    }
    conn.progress_handler(0, None::<fn() -> bool>);
    match result {
        Ok(s) => SqlResult::Ok(s),
        Err(e) if Instant::now() > deadline => SqlResult::Err(format!(
            "timeout after {:.1}s ({})",
            limit.as_secs_f64(),
            error_text(&e)
        )),
        Err(e) => SqlResult::Err(error_text(&e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inference_matches_the_documented_rule() {
        let c = Connection::open_in_memory().unwrap();
        assert_eq!(infer(&c, ""), Value::Null);
        assert_eq!(infer(&c, "42"), Value::Integer(42));
        assert_eq!(infer(&c, "-7"), Value::Integer(-7));
        assert_eq!(infer(&c, "1.5"), Value::Real(1.5));
        assert_eq!(infer(&c, "2.0"), Value::Real(2.0));
        assert_eq!(infer(&c, "1.0e+20"), Value::Real(1e20));
        for t in [
            "007",
            "-0",
            "+3",
            "1.50",
            "1e3",
            ".5",
            "-0.0",
            " 42",
            "true",
            "NaN",
            "9223372036854775808",
            "0.30000000000000004",
        ] {
            assert_eq!(infer(&c, t), Value::Text(t.to_string()), "{t}");
        }
    }

    #[test]
    fn statements_split_outside_quotes() {
        assert_eq!(
            split_statements("SELECT ';'; SELECT \"a;b\" ;  "),
            vec!["SELECT ';'", " SELECT \"a;b\" "]
        );
    }

    #[test]
    fn renders_like_sqawk() {
        let mut t = Table::new("t", &["a", "b"]);
        t.rows.push(vec!["1".into(), "x,y".into()]);
        t.rows.push(vec!["".into(), "1.50".into()]);
        let o = Oracle::new(&[t]).unwrap();
        match o.run(
            &["SELECT a, b, a * 1.0 AS r FROM t".to_string()],
            Duration::from_secs(5),
        ) {
            SqlResult::Ok(sets) => {
                assert_eq!(sets[0].header, "a,b,r");
                assert_eq!(sets[0].rows, vec!["1,\"x,y\",1.0", ",1.50,"]);
            }
            SqlResult::Err(e) => panic!("{e}"),
        }
    }
}
