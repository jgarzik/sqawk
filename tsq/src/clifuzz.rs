//! `tsq cli`: the command-line and REPL surface fuzzer. It absorbs the
//! retired Python tools csvfuzz.py (malformed and huge inputs) and fuzz.py
//! (garbage SQL, limit values, deep nesting) as randomized generators.
//!
//! Each iteration builds one sqawk run from a random scenario:
//!
//! * `flags`: random flag and operand combinations -- `-s` / `-f FILE` /
//!   `-f -` in any order, `-F` and `--output-delimiter` (valid and
//!   invalid), `--no-header`, `--no-output-header`, `--comments`,
//!   `--tabledef` (good and bad), `name=path` operands (including a `k=v/`
//!   directory and `=x`), `-` for standard input, missing files,
//!   directories, a read-only file under `-w`, `-i` with `-s`, unknown
//!   flags, `--help` / `--version`;
//! * `repl`: `-i` with a script on standard input -- dot-commands with good
//!   and bad arguments (`.load`, `.save`, `.cd`, `.exit N`, ...), statements
//!   split over lines, transactions, garbage, an unterminated last
//!   statement;
//! * `malformed`: csvfuzz.py's malformed files (unclosed quotes, NUL bytes,
//!   invalid UTF-8, mixed line endings, duplicate headers, ...) and random
//!   byte-level mutations of a valid file, read and written back;
//! * `garbage`: fuzz.py's random bytes and token soup as SQL;
//! * `limits`: fuzz.py's scalar calls at the limits (printf widths of 2^31,
//!   i64 extremes in substr / lag / ntile / LIMIT / round, dates past 9999,
//!   ...), their numbers re-drawn from the extremes;
//! * `huge`: very large statements through `-f` (deep nesting, long
//!   operator chains, big IN lists and UNIONs, wide select lists, long
//!   literals, many statements, many-table joins);
//! * `closed_stdout`: a large result with standard output closed after a
//!   few bytes (a broken pipe on every OS), then a `--write` statement that
//!   must still run;
//! * `wide`: thousands of columns, a multi-megabyte field, a one-cell
//!   `--write` that must change exactly one line.
//!
//! The oracle: the run ends within the time limit; the exit status is 0, 1
//! or 2 (or a `.exit N` status); standard error never shows a panic, a
//! stack overflow or Rust's overflow text; a failed command line prints
//! exactly one line, `sqawk: <message>` (status 1), or clap's usage error
//! (status 2, `error: ...` and `Usage:`); a successful one prints nothing on
//! standard error; a piped REPL prints no banner or prompt, and each of its
//! error lines starts with `sqawk: `; no file appears in the working
//! directory, HOME or XDG_STATE_HOME (all fresh temp dirs), no temp file is
//! left next to the data, read-only files never change, and without
//! `--write` (or `.write` / `.save`) no input file changes.

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};

use crate::diff::tables;
use crate::gen;
use crate::rng::Rng;
use crate::run::{crash_text, execute, Finished, Job, StdoutMode};

pub struct CliOptions {
    pub iterations: usize,
    pub only: Option<usize>,
    pub timeout: Duration,
}

/// csvfuzz.py's malformed inputs.
const MALFORMED: &[(&str, &[u8])] = &[
    ("empty.csv", b""),
    ("newline_only.csv", b"\n"),
    ("header_only.csv", b"a,b,c\n"),
    ("header_only_nonl.csv", b"a,b,c"),
    ("ragged_short.csv", b"a,b,c\n1,2\n3,4,5\n"),
    ("ragged_long.csv", b"a,b\n1,2,3,4\n5,6\n"),
    ("unclosed_quote.csv", b"a,b\n1,\"abc\n2,3\n"),
    ("quote_mid.csv", b"a,b\n1,ab\"c\n2,\"x\"y\n"),
    ("nul_bytes.csv", b"a,b\n1,x\x00y\n2,\x00\n"),
    ("invalid_utf8.csv", b"a,b\n1,\xff\xfe\n2,caf\xe9\n"),
    ("bom.csv", b"\xef\xbb\xbfid,name\n1,x\n"),
    ("crlf_mixed.csv", b"a,b\r\n1,x\r\n2,y\n3,z\r\n"),
    ("cr_only.csv", b"a,b\r1,x\r2,y\r"),
    ("dup_header.csv", b"a,a,b\n1,2,3\n"),
    ("empty_header.csv", b",b,\n1,2,3\n"),
    ("space_header.csv", b"first name, age ,\"we,ird\"\n x , 5 ,q\n"),
    ("blank_lines.csv", b"a,b\n1,2\n\n3,4\n\n"),
    ("trailing_comma.csv", b"a,b,\n1,2,\n"),
    ("embedded_nl.csv", b"a,b\n1,\"line1\nline2\"\n2,\"x\"\"y\"\n"),
    ("only_quotes.csv", b"\"\"\n\"\"\n"),
    ("numeric_header.csv", b"1,2,3\n4,5,6\n"),
    ("comment_hash.csv", b"a,b\n#c,d\n1,2\n"),
    (
        "huge_int.csv",
        b"a\n99999999999999999999\n-9223372036854775809\n9223372036854775807\n1e999\nNaN\ninf\n-0\n0.1000\n",
    ),
    ("tabs.tsv", b"a\tb\n1\t\"q\tq\"\n# comment\n2\tx\n"),
    ("noheader.txt", b"1 2 3\n4 5 6\n"),
    ("case_dup.csv", b"id,ID\n1,2\n"),
    ("quote_header.csv", b"\"a\"\"b\",c\n1,2\n"),
    ("lone_cr_field.csv", b"a,b\n1,\"x\ry\"\n"),
];

/// fuzz.py's garbage-SQL tokens.
const TOKENS: &[&str] = &[
    "SELECT",
    "FROM",
    "WHERE",
    "(",
    ")",
    ",",
    "a",
    "b",
    "d",
    "e",
    "*",
    "+",
    "-",
    "/",
    "%",
    "||",
    "=",
    "<",
    ">",
    "AND",
    "OR",
    "NOT",
    "NULL",
    "1",
    "0",
    "-9223372036854775808",
    "9223372036854775807",
    "'x'",
    "''",
    "1e999",
    "GROUP BY",
    "ORDER BY",
    "HAVING",
    "LIMIT",
    "OFFSET",
    "UNION",
    "JOIN",
    "ON",
    "LEFT",
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "END",
    "IN",
    "EXISTS",
    "OVER",
    "PARTITION BY",
    "ROWS",
    "count(*)",
    "sum(",
    "substr(",
    "printf(",
    "'%999999999d'",
    "'%.999999999f'",
    "INSERT INTO d VALUES",
    "UPDATE d SET k =",
    "DELETE FROM d",
    ";",
    "k",
    "x",
    "aid",
    "DISTINCT",
    "AS",
    "CAST(",
    "AS INTEGER)",
    "lag(",
    "ntile(",
    "row_number()",
    "date(",
    "'+1e18 days'",
    "zeroblob(",
    "char(",
    "1114112",
    "-1",
    "IS",
    "BETWEEN",
    "LIKE",
    "ESCAPE",
    "'%'",
    "WITH",
    "VALUES",
    "(SELECT",
    "RECURSIVE",
    "BEGIN",
    "COMMIT",
    "ROLLBACK",
    "TRUNCATE TABLE d",
    "ALTER TABLE d ADD COLUMN z TEXT",
    "CREATE TABLE",
    "DROP TABLE d",
    "FILTER",
    "WINDOW",
];

/// fuzz.py's scalar calls at the limits. `{N}` is re-drawn from EXTREMES.
const LIMITS: &[&str] = &[
    "SELECT printf('%{N}d', 1)",
    "SELECT printf('%.{N}f', 1)",
    "SELECT printf('%{N}s', '')",
    "SELECT substr('abc', {N}, {N})",
    "SELECT char({N})",
    "SELECT lpad('a', {N})",
    "SELECT rpad('a', {N}, 'xy')",
    "SELECT date('2024-01-01', '+{N} days')",
    "SELECT date('2024-01-01', '+{N} seconds')",
    "SELECT datetime({N}, 'unixepoch')",
    "SELECT julianday({N})",
    "SELECT strftime('%J', {N})",
    "SELECT ntile({N}) OVER () FROM d",
    "SELECT lag(k, {N}) OVER () FROM d",
    "SELECT nth_value(k, {N}) OVER () FROM d",
    "SELECT sum(k) OVER (ROWS BETWEEN {N} PRECEDING AND {N} FOLLOWING) FROM d",
    "SELECT k FROM d LIMIT {N} OFFSET {N}",
    "SELECT k FROM d LIMIT {N}",
    "SELECT k FROM d ORDER BY {N}",
    "SELECT k FROM d GROUP BY {N}",
    "SELECT round(1.5, {N})",
    "SELECT {N} % {N}, {N} / {N}, {N} * {N}, {N} + {N}, {N} - {N}",
    "SELECT abs({N})",
    "SELECT -({N})",
    "SELECT CAST({N} AS INTEGER), CAST({N} AS REAL), CAST({N} AS TEXT)",
    "SELECT hex(printf('%{N}d', 1))",
    "SELECT length(printf('%{N}s', ''))",
    "SELECT 'a' LIKE printf('%{N}s', '') || 'b'",
    "SELECT instr(printf('%{N}s', 'a'), 'a')",
    "SELECT group_concat(k, printf('%{N}s', '')) FROM d",
    "SELECT unixepoch('9999-12-31 23:59:59', '+{N} second')",
    "SELECT strftime('%s', '0000-01-01', '-{N} day')",
    "SELECT date({N})",
    "SELECT time({N})",
    "SELECT zeroblob({N})",
    "SELECT sum({N}) FROM d",
    "SELECT avg({N}) FROM d",
    "SELECT total({N}) FROM d",
    "SELECT max({N}, {N}), min({N}, {N})",
    "SELECT printf('%.*f', {N}, 1.5)",
    "SELECT printf('%*d', {N}, 1)",
    "SELECT substr(printf('%5000s', ''), {N})",
    "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < 1000) SELECT count(*) FROM c LIMIT {N}",
    "SELECT * FROM d LIMIT 1 OFFSET {N}",
    "SELECT round({N}, {N})",
    "SELECT {N} BETWEEN {N} AND {N}",
    "SELECT 1 FROM d a1, d a2, d a3, d a4, d a5, d a6 LIMIT {N}",
];

const EXTREMES: &[&str] = &[
    "9223372036854775807",
    "-9223372036854775808",
    "9223372036854775808",
    "2147483648",
    "2147483647",
    "-2147483649",
    "999999999",
    "1000000001",
    "1114112",
    "55296",
    "-1",
    "0",
    "1",
    "1e308",
    "-1e308",
    "1e18",
    "1e300",
    "0.5",
    "-0.0",
    "NULL",
    "'x'",
    "'9223372036854775807'",
];

/// Valid statements over the data set's tables.
const GOOD_SQL: &[&str] = &[
    "SELECT * FROM a",
    "SELECT COUNT(*) FROM b",
    "SELECT 1",
    "SELECT k, COUNT(*) FROM a GROUP BY k ORDER BY k",
    "SELECT * FROM d ORDER BY k",
    "SELECT a.aid, b.bid FROM a JOIN b ON a.aid = b.aid",
    "UPDATE d SET wt = 99 WHERE k = 1",
    "INSERT INTO d VALUES (77, 'new', 1)",
    "DELETE FROM e WHERE eid = 1",
    "BEGIN",
    "COMMIT",
    "ROLLBACK",
    "SELECT * FROM people",
    "SELECT name FROM people WHERE id = 2",
    "ALTER TABLE dup ADD COLUMN z TEXT",
    "TRUNCATE TABLE dup",
    "SELECT * FROM nosuch",
    "SELECT nosuchcol FROM a",
    "CREATE TABLE tmp AS SELECT * FROM a",
    "DROP TABLE dup",
    "SELECT * FROM stdin",
    "SELECT * FROM t",
    "SELECT * FROM x",
    "SELECT * FROM ro",
    "UPDATE ro SET v = 'changed'",
    "SELECT * FROM cmt",
    "SELECT * FROM nohdr",
];

/// Separators: valid ones and the invalid ones clap must reject.
const SEPS: &[&str] = &[
    ",", "\t", "\\t", ";", "|", ":", " ", "x", "", "ab", "\"", "\n", "\r", "é", "\\n", "\\",
];

/// What one run is allowed to do and what it must look like.
struct Expect {
    repl: bool,
    verbose: bool,
    /// Exit statuses beyond 0, 1 and 2 (`.exit N`).
    extra_codes: BTreeSet<i32>,
    /// Exactly this status.
    code: Option<i32>,
    /// Input files may change (`-w`, `.write`, `.save`).
    writes: bool,
    /// New entries allowed in the data directory.
    new_files: BTreeSet<String>,
    /// A help / version run: stdout must not be empty.
    info: bool,
    /// Extra check after the run.
    post: Option<Post>,
}

enum Post {
    /// The file must differ from these bytes (a write that had to happen).
    Changed(PathBuf, Vec<u8>),
    /// The file must differ from these bytes in exactly one line.
    OneLine(PathBuf, Vec<u8>),
}

impl Expect {
    fn new() -> Self {
        Expect {
            repl: false,
            verbose: false,
            extra_codes: BTreeSet::new(),
            code: None,
            writes: false,
            new_files: BTreeSet::new(),
            info: false,
            post: None,
        }
    }
}

/// One iteration's run.
struct Case {
    scenario: &'static str,
    args: Vec<OsString>,
    stdin: Option<Vec<u8>>,
    stdout: StdoutMode,
    expect: Expect,
}

/// The fixed data set, copied fresh into each iteration's directory.
fn write_data(dir: &Path) -> Result<()> {
    fs::create_dir_all(dir)?;
    gen::write_csv_tables(dir, &tables::edge())?;
    fs::write(dir.join("people.tsv"), "id\tname\n1\tann\n2\tbob\n")?;
    fs::write(dir.join("nohdr.txt"), "1,x\n2,y\n")?;
    fs::write(dir.join("cmt.csv"), "#c\nid,v\n1,a\n#x\n2,b\n")?;
    fs::write(dir.join("ro.csv"), "id,v\n1,a\n")?;
    fs::create_dir_all(dir.join("k=v"))?;
    fs::write(dir.join("k=v").join("t.csv"), "id,v\n1,a\n")?;
    fs::create_dir_all(dir.join("adir"))?;
    fs::create_dir_all(dir.join("bad"))?;
    for (name, bytes) in MALFORMED {
        fs::write(dir.join("bad").join(name), bytes)?;
    }
    Ok(())
}

fn copy_tree(from: &Path, to: &Path) -> Result<()> {
    fs::create_dir_all(to)?;
    for e in fs::read_dir(from)? {
        let e = e?;
        let p = e.path();
        let q = to.join(e.file_name());
        if p.is_dir() {
            copy_tree(&p, &q)?;
        } else {
            fs::copy(&p, &q)?;
        }
    }
    Ok(())
}

/// Every file under `dir`: relative path -> bytes.
fn snapshot(dir: &Path) -> Vec<(String, Option<Vec<u8>>)> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(rd) = fs::read_dir(&d) else { continue };
        for e in rd.flatten() {
            let p = e.path();
            let rel = p
                .strip_prefix(dir)
                .map(|r| r.to_string_lossy().replace('\\', "/"))
                .unwrap_or_default();
            if p.is_dir() {
                out.push((rel + "/", None));
                stack.push(p);
            } else {
                out.push((rel, fs::read(&p).ok()));
            }
        }
    }
    out.sort();
    out
}

fn set_readonly(path: &Path, ro: bool) {
    if let Ok(meta) = fs::metadata(path) {
        let mut p = meta.permissions();
        #[allow(clippy::permissions_set_readonly_false)]
        p.set_readonly(ro);
        let _ = fs::set_permissions(path, p);
    }
}

fn token_soup(r: &mut Rng, quotes: bool) -> String {
    let n = r.range(2, 30) as usize;
    let mut v = Vec::new();
    for _ in 0..n {
        let t = *r.pick(TOKENS);
        if !quotes && t.contains('\'') {
            continue;
        }
        v.push(t);
    }
    v.join(" ")
}

fn random_bytes(r: &mut Rng, max: usize) -> Vec<u8> {
    let n = r.range(1, max as i64) as usize;
    (0..n).map(|_| r.below(256) as u8).collect()
}

fn limit_sql(r: &mut Rng) -> String {
    let t = *r.pick(LIMITS);
    let mut out = String::new();
    let mut rest = t;
    while let Some(i) = rest.find("{N}") {
        out.push_str(&rest[..i]);
        out.push_str(r.pick_copy(EXTREMES));
        rest = &rest[i + 3..];
    }
    out.push_str(rest);
    out
}

/// A very large statement (fuzz.py's nesting and size cases, sizes drawn).
fn huge_sql(r: &mut Rng) -> String {
    let pick = |r: &mut Rng, xs: &[usize]| *r.pick(xs);
    match r.below(20) {
        0 => {
            let n = pick(r, &[40, 47, 48, 100, 1000, 20000]);
            format!("SELECT {}1{}", "(".repeat(n), ")".repeat(n))
        }
        1 => format!(
            "SELECT {}",
            vec!["1"; pick(r, &[1000, 5000, 20000])].join("+")
        ),
        2 => format!(
            "SELECT count(*) FROM d WHERE {}",
            vec!["k IS NOT NULL OR 1"; pick(r, &[100, 1000, 5000])].join(" AND ")
        ),
        3 => format!("SELECT {}", vec!["'a'"; pick(r, &[1000, 10000])].join("||")),
        4 => format!("SELECT {}1", "NOT ".repeat(pick(r, &[100, 1000, 10000]))),
        5 => format!("SELECT {}1", "- ".repeat(pick(r, &[100, 1000, 10000]))),
        6 => {
            let n = pick(r, &[100, 1000, 3000]);
            format!("SELECT {}1{}", "coalesce(".repeat(n), ", 2)".repeat(n))
        }
        7 => {
            let n = pick(r, &[100, 2000]);
            format!(
                "SELECT {}1{}",
                "CASE WHEN 1 THEN ".repeat(n),
                " END".repeat(n)
            )
        }
        8 => {
            let n = pick(r, &[30, 300, 2000]);
            format!("SELECT {}1{}", "(SELECT ".repeat(n), ")".repeat(n))
        }
        9 => {
            let n = pick(r, &[20, 100, 500]);
            format!(
                "SELECT * FROM {}d{}",
                "(SELECT * FROM ".repeat(n),
                ")".repeat(n)
            )
        }
        10 => {
            let n = pick(r, &[1000, 20000, 100000]);
            let list: Vec<String> = (0..n).map(|i| i.to_string()).collect();
            format!("SELECT count(*) FROM d WHERE k IN ({})", list.join(","))
        }
        11 => {
            let n = pick(r, &[1000, 20000]);
            let list: Vec<String> = (0..n).map(|i| format!("'s{i}'")).collect();
            format!("SELECT count(*) FROM d WHERE label IN ({})", list.join(","))
        }
        12 => {
            let n = pick(r, &[100, 1000, 3000]);
            let kw = if r.chance(0.5) {
                " UNION ALL "
            } else {
                " UNION "
            };
            (0..n)
                .map(|i| format!("SELECT {i}"))
                .collect::<Vec<_>>()
                .join(kw)
        }
        13 => {
            let n = pick(r, &[1000, 5000, 20000]);
            let cols: Vec<String> = (0..n).map(|i| format!("k+{i}")).collect();
            format!("SELECT {} FROM d", cols.join(","))
        }
        14 => format!(
            "SELECT length('{}')",
            "x".repeat(pick(r, &[100_000, 1_000_000, 5_000_000]))
        ),
        15 => vec!["SELECT 1"; pick(r, &[100, 1000, 5000])].join(";"),
        16 => {
            let n = pick(r, &[100, 1000, 5000]);
            let terms: Vec<String> = (0..n).map(|i| format!("k = {i}")).collect();
            format!("SELECT count(*) FROM d WHERE {}", terms.join(" OR "))
        }
        17 => {
            let n = pick(r, &[50, 200]);
            format!("SELECT {}1{}", "abs(".repeat(n), ")".repeat(n))
        }
        18 => {
            let n = pick(r, &[100, 2000]);
            let ins: Vec<String> = (0..n)
                .map(|i| format!("INSERT INTO d VALUES ({i},'x',1)"))
                .collect();
            format!("{}; SELECT count(*) FROM d", ins.join(";"))
        }
        _ => {
            let n = pick(r, &[8, 16, 64]);
            let from: Vec<String> = (0..n).map(|i| format!("d t{i}")).collect();
            let wh: Vec<String> = (0..n).map(|i| format!("t{i}.k = 1")).collect();
            format!(
                "SELECT count(*) FROM {} WHERE {}",
                from.join(", "),
                wh.join(" AND ")
            )
        }
    }
}

/// A random byte-level mutation of a valid CSV file.
fn mutate(r: &mut Rng, base: &[u8]) -> Vec<u8> {
    let mut v = base.to_vec();
    let edits = r.range(1, 6);
    for _ in 0..edits {
        let at = if v.is_empty() {
            0
        } else {
            r.below(v.len() + 1)
        };
        match r.below(6) {
            0 => {
                let b = *r.pick(&[b'"', b',', b'\r', b'\n', 0u8, 0xff, b'#', b' ', b'\t', 0xc3]);
                v.insert(at, b);
            }
            1 if !v.is_empty() => {
                v.remove(at.min(v.len() - 1));
            }
            2 => v.truncate(at),
            3 => {
                let line = b"1,2,3,4,5,6,7,8,9\n";
                for (i, b) in line.iter().enumerate() {
                    v.insert(at + i, *b);
                }
            }
            4 => {
                let dup = v.clone();
                v.extend_from_slice(&dup);
            }
            _ => {
                v.splice(at..at, b"\"\"\"".iter().copied());
            }
        }
    }
    v
}

fn os(s: impl Into<OsString>) -> OsString {
    s.into()
}

fn build(r: &mut Rng, data: &Path, scratch: &Path) -> Result<Case> {
    let roll = r.below(100);
    let d = |name: &str| data.join(name).into_os_string();
    let mut expect = Expect::new();
    let mut args: Vec<OsString> = Vec::new();
    let mut stdin = None;
    let mut stdout = StdoutMode::Capture;
    let edge_files = ["a.csv", "b.csv", "d.csv", "e.csv", "dup.csv"];
    let scenario: &'static str;
    if roll < 30 {
        scenario = "flags";
        let mut pieces: Vec<Vec<OsString>> = Vec::new();
        let mut sql_from_stdin = false;
        let mut table_from_stdin = false;
        // No SQL or no operand is a usage error: keep those rare.
        let nsql = if r.chance(0.08) { 0 } else { r.range(1, 3) };
        for _ in 0..nsql {
            pieces.push(match r.below(10) {
                0..=4 => vec![os("-s"), os(*r.pick(GOOD_SQL))],
                5 => vec![os("-s"), os(token_soup(r, true))],
                6 => vec![os("-s"), os("")],
                7 => {
                    let f = scratch.join(format!("q{}.sql", r.below(1000)));
                    let mut text = String::new();
                    for _ in 0..r.range(1, 4) {
                        text.push_str(r.pick_copy(GOOD_SQL));
                        text.push_str(if r.chance(0.8) { ";\n" } else { "\n" });
                    }
                    fs::write(&f, text)?;
                    vec![os("-f"), f.into_os_string()]
                }
                8 => {
                    sql_from_stdin = true;
                    vec![os("-f"), os("-")]
                }
                _ => vec![
                    os("-f"),
                    if r.chance(0.5) {
                        d("missing.sql")
                    } else {
                        d("adir")
                    },
                ],
            });
        }
        let nfiles = if r.chance(0.08) { 0 } else { r.range(1, 4) };
        for _ in 0..nfiles {
            pieces.push(match r.below(24) {
                0..=3 | 16..=19 => vec![d(r.pick_copy(&edge_files))],
                4 | 20 => vec![d("people.tsv")],
                5 => vec![os(format!("x={}", data.join("a.csv").display()))],
                6 => vec![d("k=v/t.csv")],
                7 => vec![os("=x")],
                8 => {
                    table_from_stdin = true;
                    vec![os(if r.chance(0.5) { "-" } else { "t=-" })]
                }
                9 => vec![d("missing.csv")],
                10 => vec![d("adir")],
                11 => vec![d("ro.csv")],
                12 => {
                    let (name, _) = *r.pick(MALFORMED);
                    vec![data.join("bad").join(name).into_os_string()]
                }
                13 | 21 => vec![d("cmt.csv")],
                14 | 22 => vec![d("nohdr.txt")],
                _ => vec![os(format!("{}=", data.join("a.csv").display()))],
            });
        }
        for _ in 0..r.range(0, 4) {
            // Usage errors (exit 2) end a run before anything else is
            // exercised, so the pieces that cause them are rare.
            pieces.push(match r.below(30) {
                0..=3 => vec![
                    os(r.pick_copy(&["-F", "--field-separator"])),
                    os(*r.pick(&SEPS[..7])),
                ],
                4 => vec![
                    os(r.pick_copy(&["-F", "--field-separator"])),
                    os(*r.pick(SEPS)),
                ],
                5 | 6 => vec![os("--output-delimiter"), os(*r.pick(&SEPS[..7]))],
                7 => vec![os("--output-delimiter"), os(*r.pick(SEPS))],
                8 | 9 => vec![os("--no-output-header")],
                10 | 11 => vec![os("--no-header")],
                12 | 13 => vec![os("--comments")],
                14..=16 => vec![os(format!(
                    "--tabledef={}",
                    r.pick(&[
                        "nohdr:n,v",
                        "nohdr:a,b,c",
                        "nosuch:x",
                        "bad",
                        ":x",
                        "nohdr:",
                        "a:p,q",
                        "nohdr:x,x"
                    ])
                ))],
                17 | 18 => {
                    expect.verbose = true;
                    vec![os("-v")]
                }
                19..=22 => {
                    expect.writes = true;
                    vec![os(if r.chance(0.5) { "-w" } else { "--write" })]
                }
                23 => vec![os("-i")],
                24 => vec![os("--bogus")],
                25 => vec![os("-s")],
                26 => vec![os("-F")],
                27 => {
                    expect.info = true;
                    vec![os(*r.pick(&["--help", "-h", "-V", "--version"]))]
                }
                _ => vec![os("--comments")],
            });
            // A flag given twice is a usage error; once is enough.
            let last = pieces.len() - 1;
            let key = |p: &[OsString]| match p[0].to_str() {
                Some("--write") => "-w".to_string(),
                Some("--field-separator") => "-F".to_string(),
                Some(s) => s.to_string(),
                None => String::new(),
            };
            let k = key(&pieces[last]);
            if pieces[..last].iter().any(|p| key(p) == k) && !r.chance(0.05) {
                pieces.pop();
            }
        }
        // Shuffle the pieces (Fisher-Yates), keeping each flag's value
        // with it.
        for i in (1..pieces.len()).rev() {
            let j = r.below(i + 1);
            pieces.swap(i, j);
        }
        for p in pieces {
            args.extend(p);
        }
        if sql_from_stdin || table_from_stdin || r.chance(0.2) {
            stdin = Some(if sql_from_stdin && (!table_from_stdin || r.chance(0.5)) {
                format!("{};\n", r.pick(GOOD_SQL)).into_bytes()
            } else {
                b"a,b\n1,2\n3,4\n".to_vec()
            });
        }
        // `-i` reads its commands from stdin.
        if args.iter().any(|a| a == "-i") {
            expect.repl = true;
            if stdin.is_none() {
                stdin = Some(b"SELECT 1;\n".to_vec());
            }
        }
    } else if roll < 50 {
        scenario = "repl";
        expect.repl = true;
        args.push(os("-i"));
        for f in edge_files.iter().take(r.range(1, 5) as usize) {
            args.push(d(f));
        }
        if r.chance(0.3) {
            args.push(d("people.tsv"));
        }
        if r.chance(0.15) {
            expect.verbose = true;
            args.push(os("-v"));
        }
        if r.chance(0.15) {
            args.push(os("-w"));
            expect.writes = true;
        }
        if r.chance(0.1) {
            args.push(os("--no-output-header"));
        }
        let mut script = String::new();
        let n = r.range(1, 15);
        for _ in 0..n {
            match r.below(20) {
                0..=6 => {
                    let sql = *r.pick(GOOD_SQL);
                    // Split the statement over lines at random spaces.
                    let mut line = String::new();
                    for w in sql.split(' ') {
                        line.push_str(w);
                        line.push(if r.chance(0.2) { '\n' } else { ' ' });
                    }
                    script.push_str(line.trim_end());
                    script.push_str(";\n");
                }
                7 => {
                    script.push_str(&token_soup(r, false));
                    script.push_str(";\n");
                }
                8 => {
                    let cmd = match r.below(6) {
                        0 => format!(".load {}", data.join("people.tsv").display()),
                        1 => format!(".load p2={}", data.join("cmt.csv").display()),
                        2 => ".load".to_string(),
                        3 => ".load /nonexistent/x.csv".to_string(),
                        4 => format!(".load {}", data.join("adir").display()),
                        _ => format!(
                            ".load {}",
                            data.join("bad").join(r.pick(MALFORMED).0).display()
                        ),
                    };
                    script.push_str(&cmd);
                    script.push('\n');
                }
                9 => {
                    let cmd = match r.below(4) {
                        0 => format!(".cd {}", data.display()),
                        1 => ".cd".to_string(),
                        2 => ".cd /nonexistent/dir".to_string(),
                        _ => format!(".cd {}", data.join("a.csv").display()),
                    };
                    script.push_str(&cmd);
                    script.push('\n');
                }
                10 => {
                    expect.writes = true;
                    script.push_str(r.pick_copy(&[
                        ".save",
                        ".save d",
                        ".save nosuch",
                        ".write on",
                        ".write off",
                        ".write",
                    ]));
                    script.push('\n');
                }
                11 => {
                    script.push_str(r.pick_copy(&[
                        ".tables",
                        ".tables a%",
                        ".tables zz",
                        ".schema",
                        ".schema a",
                        ".schema nosuch",
                        ".help",
                        ".version",
                        ".show",
                        ".show tables",
                        ".stats on",
                        ".stats off",
                        ".stats bogus",
                        ".changes on",
                        ".changes off",
                        ".changes x",
                        ".print hello world",
                        ".bogus",
                        ".",
                        ".TABLES",
                    ]));
                    script.push('\n');
                }
                12 => {
                    script.push_str(r.pick_copy(&[
                        "BEGIN;\n",
                        "COMMIT;\n",
                        "ROLLBACK;\n",
                        "END;\n",
                    ]));
                }
                13 => {
                    script.push_str(&format!(
                        "CREATE TABLE nt (id INT, v TEXT) LOCATION '{}';\nINSERT INTO nt VALUES (1, 'x');\n",
                        data.join("newloc.csv").display()
                    ));
                    expect.new_files.insert("newloc.csv".to_string());
                }
                14 => script.push_str("\n   \n"),
                15 => script.push_str("SELECT 1; SELECT 2;\n"),
                16 => script.push_str("SELECT 'a;b', \"k\" FROM d WHERE k = 1;\n"),
                _ => script.push_str(&format!("{};\n", limit_sql(r))),
            }
        }
        match r.below(6) {
            0 => {
                let code = *r.pick(&[
                    "0",
                    "1",
                    "3",
                    "-1",
                    "255",
                    "256",
                    "300",
                    "abc",
                    "99999999999999999999",
                    "",
                ]);
                if let Ok(c) = code.parse::<i64>() {
                    expect.extra_codes.insert(c.rem_euclid(256) as i32);
                }
                script.push_str(&format!(".exit {code}\n"));
                // Lines after .exit never run.
                script.push_str("SELECT 'after exit';\n");
            }
            1 => script.push_str(".quit\n"),
            2 => script.push_str("SELECT * FROM d WHERE k = 1"),
            3 => script.push_str("SELECT 'unterminated"),
            _ => {}
        }
        if r.chance(0.1) {
            script = script.replace('\n', "\r\n");
        }
        stdin = Some(script.into_bytes());
    } else if roll < 63 {
        scenario = "malformed";
        let (name, bytes): (String, Vec<u8>) = if r.chance(0.5) {
            let (n, b) = *r.pick(MALFORMED);
            (n.to_string(), b.to_vec())
        } else {
            let base = fs::read(data.join("a.csv"))?;
            let ext = *r.pick(&["csv", "tsv", "txt"]);
            (format!("m.{ext}"), mutate(r, &base))
        };
        let path = scratch.join(&name);
        fs::write(&path, &bytes)?;
        let table = name.split('.').next().unwrap_or("m").to_string();
        if r.chance(0.3) {
            args.push(os("--comments"));
        }
        if r.chance(0.2) {
            args.push(os("--no-header"));
        }
        if r.chance(0.15) {
            args.push(os("-F"));
            args.push(os(*r.pick(&[",", "\\t", ";", " "])));
        }
        let sql = match r.below(4) {
            0 => format!("SELECT * FROM \"{table}\""),
            1 => format!("SELECT count(*) FROM \"{table}\""),
            2 => {
                expect.writes = true;
                args.push(os("-w"));
                format!("DELETE FROM \"{table}\" WHERE rowid = 1; INSERT INTO \"{table}\" SELECT * FROM \"{table}\" LIMIT 1")
            }
            _ => {
                expect.writes = true;
                args.push(os("-w"));
                format!("UPDATE \"{table}\" SET \"{}\" = 'X'", 'a')
            }
        };
        args.push(os("-s"));
        args.push(os(sql));
        args.push(path.into_os_string());
    } else if roll < 75 {
        scenario = "garbage";
        let sql: Vec<u8> = if r.chance(0.33) {
            random_bytes(r, 80)
        } else {
            token_soup(r, true).into_bytes()
        };
        let text_ok = std::str::from_utf8(&sql).is_ok() && !sql.contains(&0);
        if text_ok && r.chance(0.5) {
            args.push(os("-s"));
            args.push(os(String::from_utf8(sql).expect("checked")));
        } else if r.chance(0.5) {
            let f = scratch.join("garbage.sql");
            fs::write(&f, &sql)?;
            args.push(os("-f"));
            args.push(f.into_os_string());
        } else {
            args.push(os("-f"));
            args.push(os("-"));
            stdin = Some(sql);
        }
        for f in edge_files {
            args.push(d(f));
        }
    } else if roll < 83 {
        scenario = "limits";
        args.push(os("-s"));
        args.push(os(limit_sql(r)));
        args.push(d("d.csv"));
    } else if roll < 89 {
        scenario = "huge";
        let sql = huge_sql(r);
        if r.chance(0.7) {
            let f = scratch.join("huge.sql");
            fs::write(&f, &sql)?;
            args.push(os("-f"));
            args.push(f.into_os_string());
            args.push(d("d.csv"));
        } else {
            expect.repl = true;
            args.push(os("-i"));
            args.push(d("d.csv"));
            stdin = Some(format!("{sql};\n.exit\n").into_bytes());
        }
    } else if roll < 95 {
        scenario = "closed_stdout";
        let n = *r.pick(&[1000, 100_000, 300_000]);
        let close_after = *r.pick(&[0usize, 1, 100, 10_000]);
        stdout = StdoutMode::CloseAfter(close_after);
        let big = format!(
            "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < {n}) SELECT n, 'xxxxxxxxxxxxxxxxxxxx' AS pad FROM c"
        );
        if r.chance(0.7) {
            args.push(os("-s"));
            args.push(os(big));
            args.push(os("-s"));
            args.push(os("UPDATE d SET wt = 4242 WHERE k = 1"));
            args.push(os("-w"));
            // -v traces every instruction: only on the small result.
            if n == 1000 && r.chance(0.5) {
                expect.verbose = true;
                args.push(os("-v"));
            }
            args.push(d("d.csv"));
            expect.writes = true;
            expect.code = Some(0);
            let before = fs::read(data.join("d.csv"))?;
            expect.post = Some(Post::Changed(data.join("d.csv"), before));
        } else {
            expect.repl = true;
            args.push(os("-i"));
            args.push(d("d.csv"));
            stdin = Some(format!("{big};\n.tables\n.help\nSELECT 1;\n.exit 3\n").into_bytes());
            expect.code = Some(3);
            expect.extra_codes.insert(3);
        }
    } else {
        scenario = "wide";
        let roll = r.below(3);
        if roll == 0 {
            let len = *r.pick(&[1_000_000usize, 4_000_000]);
            let mut b = b"a,b\n1,".to_vec();
            b.extend(std::iter::repeat_n(b'x', len));
            b.push(b'\n');
            let p = scratch.join("hugefield.csv");
            fs::write(&p, b)?;
            args.push(os("-s"));
            args.push(os("SELECT length(b) FROM hugefield"));
            args.push(p.into_os_string());
        } else {
            let ncol = *r.pick(&[500usize, 1000, 2000, 10000]);
            let hdr: Vec<String> = (0..ncol).map(|i| format!("c{i}")).collect();
            let mut text = hdr.join(",");
            text.push('\n');
            for row in 0..20usize {
                let cells: Vec<String> = (0..ncol).map(|i| (row * i).to_string()).collect();
                text.push_str(&cells.join(","));
                text.push('\n');
            }
            let p = scratch.join("wide.csv");
            fs::write(&p, &text)?;
            if roll == 1 {
                let sql = *r.pick(&[
                    "SELECT count(*), sum(c999) FROM wide",
                    "SELECT * FROM wide",
                    "SELECT c1 % 3, count(*) FROM wide GROUP BY 1",
                    "SELECT count(*) FROM wide x JOIN wide y ON x.c5 = y.c5",
                ]);
                args.push(os("-s"));
                args.push(os(sql));
            } else {
                expect.writes = true;
                args.push(os("-w"));
                args.push(os("-s"));
                args.push(os(format!(
                    "UPDATE wide SET c{} = 0 WHERE c1 = 3",
                    ncol / 2
                )));
                expect.code = Some(0);
                expect.post = Some(Post::OneLine(p.clone(), text.into_bytes()));
            }
            args.push(p.into_os_string());
        }
    }
    Ok(Case {
        scenario,
        args,
        stdin,
        stdout,
        expect,
    })
}

fn show_args(args: &[OsString]) -> String {
    args.iter()
        .map(|a| {
            let s = a.to_string_lossy();
            let s: String = if s.chars().count() > 120 {
                format!(
                    "{}...({} chars)",
                    s.chars().take(100).collect::<String>(),
                    s.chars().count()
                )
            } else {
                s.into_owned()
            };
            if s.is_empty()
                || s.contains([
                    ' ', '\t', '\n', '\'', '"', ';', '*', '(', ')', '|', '<', '>',
                ])
            {
                format!("{s:?}")
            } else {
                s
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Problems with a finished run; the first element says crash or bad.
fn judge(f: &Finished, e: &Expect, timeout: Duration) -> (bool, Vec<String>) {
    let mut crash = false;
    let mut bad = Vec::new();
    let stderr = f.stderr_text();
    let stdout = String::from_utf8_lossy(&f.stdout);
    if f.timed_out {
        return (
            true,
            vec![format!("hang: no exit within {}s", timeout.as_secs())],
        );
    }
    let Some(code) = f.code else {
        return (
            true,
            vec![format!("killed by a signal; stderr {:?}", stderr.trim())],
        );
    };
    if let Some(t) = crash_text(&stderr) {
        crash = true;
        bad.push(format!("stderr contains {t:?}: {}", tail(&stderr)));
    }
    if let Some(want) = e.code {
        if code != want {
            bad.push(format!(
                "exit status {code}, want {want}; stderr {}",
                tail(&stderr)
            ));
        }
    } else if !(0..=2).contains(&code) && !e.extra_codes.contains(&code) {
        bad.push(format!("exit status {code}; stderr {}", tail(&stderr)));
    }
    let lines: Vec<&str> = stderr.lines().collect();
    if e.repl && !e.verbose {
        for l in &lines {
            if code == 2 && (l.starts_with("error:") || !l.starts_with("sqawk: ")) {
                // A usage error before the REPL starts is clap's.
                continue;
            }
            if !l.starts_with("sqawk: ") {
                bad.push(format!("REPL stderr line not `sqawk: `: {l:?}"));
                break;
            }
        }
        if stdout.contains("Welcome to Sqawk") || stdout.contains("sqawk> ") {
            bad.push("piped REPL printed its banner or prompt".to_string());
        }
    } else if !e.repl {
        match code {
            0 => {
                if !e.verbose && !stderr.is_empty() {
                    bad.push(format!("success with stderr output: {}", tail(&stderr)));
                }
                if e.info && f.stdout.is_empty() {
                    bad.push("help / version printed nothing".to_string());
                }
            }
            1 => {
                let last = lines.last().copied().unwrap_or("");
                if !last.starts_with("sqawk: ") {
                    bad.push(format!("error line not `sqawk: `: {}", tail(&stderr)));
                } else if !e.verbose && lines.len() != 1 {
                    bad.push(format!("error is {} lines: {}", lines.len(), tail(&stderr)));
                }
            }
            // clap: `error: ...`, then the usage or a pointer to --help.
            2 if !stderr.starts_with("error: ")
                || !(stderr.contains("Usage:") || stderr.contains("try '--help'")) =>
            {
                bad.push(format!(
                    "status 2 without clap's usage error: {}",
                    tail(&stderr)
                ));
            }
            _ => {}
        }
    }
    (crash, bad)
}

fn tail(s: &str) -> String {
    let t = s.trim();
    let n = t.chars().count();
    if n > 400 {
        format!("{:?}", t.chars().skip(n - 400).collect::<String>())
    } else {
        format!("{t:?}")
    }
}

/// Lines of `a` and `b` that differ (and whether the line counts match).
fn line_diff(a: &[u8], b: &[u8]) -> (usize, bool) {
    let la: Vec<&[u8]> = a.split(|&c| c == b'\n').collect();
    let lb: Vec<&[u8]> = b.split(|&c| c == b'\n').collect();
    let n = la.iter().zip(&lb).filter(|(x, y)| x != y).count();
    (n, la.len() == lb.len())
}

/// One iteration: (crashed, problems, description with run time).
fn one(
    seed: u64,
    it: usize,
    sqawk: &Path,
    base: &Path,
    timeout: Duration,
) -> Result<(bool, Vec<String>, String)> {
    let mut r = Rng::stream(seed, it as u64 + 5000);
    let root = base.join(format!("it{it}"));
    let data = root.join("data");
    let scratch = root.join("scratch");
    let cwd = root.join("cwd");
    let home = root.join("home");
    let state = root.join("state");
    copy_tree(&base.join("data"), &data)?;
    for d in [&scratch, &cwd, &home, &state] {
        fs::create_dir_all(d)?;
    }
    let ro = data.join("ro.csv");
    set_readonly(&ro, true);
    let ro_bytes = fs::read(&ro)?;
    let case = build(&mut r, &data, &scratch)?;
    let desc = format!(
        "[{}] sqawk {}{}",
        case.scenario,
        show_args(&case.args),
        case.stdin
            .as_ref()
            .map(|s| {
                let t = String::from_utf8_lossy(s);
                let t: String = t.chars().take(600).collect();
                format!("\n  stdin: {t:?}")
            })
            .unwrap_or_default()
    );
    let before = snapshot(&data);
    let mut job = Job::new(sqawk, case.args.clone());
    job.stdin = case.stdin.clone();
    job.cwd = Some(cwd.clone());
    job.limit = timeout;
    job.stdout = case.stdout;
    job.env = vec![
        ("HOME".to_string(), home.clone().into_os_string()),
        ("USERPROFILE".to_string(), home.clone().into_os_string()),
        ("XDG_STATE_HOME".to_string(), state.clone().into_os_string()),
        ("RUST_BACKTRACE".to_string(), OsString::from("0")),
    ];
    let f = execute(&job);
    if let Some(e) = &f.spawn_error {
        anyhow::bail!("{e}");
    }
    let (crash, mut bad) = judge(&f, &case.expect, timeout);
    // Files.
    for (label, d) in [
        ("working directory", &cwd),
        ("HOME", &home),
        ("XDG_STATE_HOME", &state),
    ] {
        let left = snapshot(d);
        if !left.is_empty() {
            bad.push(format!(
                "files left in {label}: {:?}",
                left.iter().map(|x| &x.0).collect::<Vec<_>>()
            ));
        }
    }
    if fs::read(&ro).ok().as_deref() != Some(&ro_bytes[..]) {
        bad.push("read-only ro.csv changed".to_string());
    }
    let after = snapshot(&data);
    let before_names: BTreeSet<&String> = before.iter().map(|x| &x.0).collect();
    for (name, bytes) in &after {
        if !before_names.contains(name) {
            let base_name = name.rsplit('/').next().unwrap_or(name);
            if !case.expect.new_files.contains(base_name) {
                bad.push(format!("new file in the data directory: {name}"));
            }
            continue;
        }
        if !case.expect.writes {
            let old = before
                .iter()
                .find(|x| &x.0 == name)
                .and_then(|x| x.1.as_ref());
            if old != bytes.as_ref() {
                bad.push(format!("{name} changed without --write"));
            }
        }
    }
    for (name, _) in &before {
        if !after.iter().any(|x| &x.0 == name) && !case.expect.writes {
            bad.push(format!("{name} disappeared"));
        }
    }
    let scratch_left: Vec<String> = snapshot(&scratch)
        .into_iter()
        .map(|x| x.0)
        .filter(|n| n.rsplit('/').next().unwrap_or(n).starts_with(".tmp"))
        .collect();
    if !scratch_left.is_empty() {
        bad.push(format!("temp files left: {scratch_left:?}"));
    }
    match &case.expect.post {
        Some(Post::Changed(p, old)) if f.code == Some(0) => {
            if fs::read(p).ok().as_deref() == Some(&old[..]) {
                bad.push(format!(
                    "{} not written after the reader closed stdout",
                    p.file_name().unwrap_or_default().to_string_lossy()
                ));
            }
        }
        Some(Post::OneLine(p, old)) if f.code == Some(0) => {
            let now = fs::read(p).unwrap_or_default();
            let (n, same_count) = line_diff(old, &now);
            if n != 1 || !same_count {
                bad.push(format!(
                    "one-cell --write changed {n} lines (line count kept: {same_count})"
                ));
            }
        }
        _ => {}
    }
    set_readonly(&ro, false);
    let _ = fs::remove_dir_all(&root);
    let first_err: String = f
        .stderr_text()
        .lines()
        .next()
        .unwrap_or("")
        .chars()
        .take(160)
        .collect();
    let desc = format!(
        "{:.2}s exit {} {desc}{}",
        f.elapsed.as_secs_f64(),
        f.code.map_or("-".to_string(), |c| c.to_string()),
        if first_err.is_empty() {
            String::new()
        } else {
            format!("\n  stderr: {first_err}")
        }
    );
    Ok((crash, bad, desc))
}

pub fn run(seed: u64, sqawk: &Path, opts: &CliOptions, verbose: bool) -> Result<i32> {
    let base = tempfile::Builder::new().prefix("tsq-cli-").tempdir()?;
    write_data(&base.path().join("data")).context("write the data set")?;
    let (mut ok, mut bad, mut crash) = (0usize, 0usize, 0usize);
    let range: Vec<usize> = match opts.only {
        Some(n) => vec![n],
        None => (0..opts.iterations).collect(),
    };
    for it in range {
        let (is_crash, problems, desc) = one(seed, it, sqawk, base.path(), opts.timeout)
            .with_context(|| format!("iteration {it}"))?;
        if problems.is_empty() {
            ok += 1;
            if verbose || opts.only.is_some() {
                println!("ok it={it} {desc}");
            }
        } else {
            if is_crash {
                crash += 1;
            } else {
                bad += 1;
            }
            println!(
                "{} it={it} (tsq cli --seed {seed} --only {it})\n  {desc}\n  {}",
                if is_crash { "CRASH" } else { "BAD" },
                problems.join("\n  ")
            );
        }
    }
    // A read-only file left behind would stop the temp dir's removal on
    // Windows.
    set_readonly(&base.path().join("data").join("ro.csv"), false);
    println!("CLI SUMMARY {{ok: {ok}, bad: {bad}, crash: {crash}}}");
    Ok(if bad + crash > 0 { 1 } else { 0 })
}
