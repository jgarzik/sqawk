//! Red list: the code defects found by the 2026-10 documentation review.
//!
//! The review checked every claim in README.md, CLAUDE.md and doc/*.md
//! against the binary. Most findings were wrong documentation; the two below
//! were wrong behaviour, and every test in this module encodes the
//! **correct** behaviour for one of them. Each was `#[ignore]`d when written
//! and un-ignored as its defect was fixed; all of them now pass. A few tests
//! (marked in their doc comments) passed when written and pin behaviour the
//! fixes had to keep.
//! Run the red list with:
//!     cargo test --test mod defects -- --ignored
//!
//! - A: `CREATE TABLE ... LOCATION 'x.tsv'` without `WITH (DELIMITER=...)`
//!   wrote comma-separated text, while a `.tsv` / `.tab` file loads as
//!   tab-separated, so the file did not load back as written.
//! - B: a declared column type other than INT/INTEGER, REAL/FLOAT/DOUBLE or
//!   BOOL/BOOLEAN was recorded as TEXT. The declared name is now recorded as
//!   written (shown by `.schema`) and its affinity follows SQLite's rules
//!   (section 3.1 of SQLite's datatype documentation).

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

use crate::helpers::create_temp_dir;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn sqawk_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_sqawk"))
}

/// Run the sqawk binary with `args` in `cwd`, feeding `stdin`, with HOME and
/// XDG_STATE_HOME pointed at `cwd` so nothing touches the real home
/// directory.
fn run_in(cwd: &Path, args: &[&str], stdin: &[u8]) -> std::io::Result<Output> {
    let mut child = Command::new(sqawk_bin())
        .args(args)
        .current_dir(cwd)
        .env("HOME", cwd)
        .env("XDG_STATE_HOME", cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    {
        let mut si = child.stdin.take().unwrap();
        si.write_all(stdin)?;
    }
    child.wait_with_output()
}

/// Run `args` in `cwd`, require success, and return stdout.
fn ok_in(cwd: &Path, args: &[&str], stdin: &[u8]) -> Result<String, Box<dyn std::error::Error>> {
    let out = run_in(cwd, args, stdin)?;
    assert!(
        out.status.success(),
        "sqawk {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    Ok(String::from_utf8_lossy(&out.stdout).replace("\r\n", "\n"))
}

/// In a fresh directory holding `one.csv`, run `CREATE TABLE t (a INT,
/// b TEXT) LOCATION './<file><with>'` plus two INSERTs under `--write`, then
/// return the written file's text and what a second run reading it back
/// (default delimiter rule) prints for `SELECT b, a`.
fn create_and_reload(
    file: &str,
    with: &str,
) -> Result<(String, String), Box<dyn std::error::Error>> {
    let dir = create_temp_dir()?;
    fs::write(dir.path().join("one.csv"), "k\n1\n")?;
    let sql = format!(
        "CREATE TABLE t (a INT, b TEXT) LOCATION './{file}'{with}; \
         INSERT INTO t VALUES (1, 'p'), (2, 'q')"
    );
    ok_in(dir.path(), &["--write", "-s", &sql, "one.csv"], b"")?;
    let written = fs::read_to_string(dir.path().join(file))?.replace("\r\n", "\n");
    let stem = Path::new(file).file_stem().unwrap().to_str().unwrap();
    let reread = ok_in(
        dir.path(),
        &["-s", &format!("SELECT b, a FROM {stem}"), file],
        b"",
    )?;
    Ok((written, reread))
}

/// Pipe `script` into `sqawk --interactive one.csv` in a fresh directory and
/// return stdout.
fn repl(script: &str) -> Result<String, Box<dyn std::error::Error>> {
    let dir = create_temp_dir()?;
    fs::write(dir.path().join("one.csv"), "k\n1\n")?;
    ok_in(dir.path(), &["--interactive", "one.csv"], script.as_bytes())
}

// ---------------------------------------------------------------------------
// A: LOCATION's default delimiter follows the file extension
// ---------------------------------------------------------------------------

/// A `.tsv` LOCATION is written tab-separated, so it loads back as written.
#[test]
fn location_tsv_is_written_tab_separated() -> TestResult {
    let (written, reread) = create_and_reload("x.tsv", "")?;
    assert_eq!(written, "a\tb\n1\tp\n2\tq\n");
    assert_eq!(reread, "b,a\np,1\nq,2\n");
    Ok(())
}

/// `.tab` follows the same rule as `.tsv`.
#[test]
fn location_tab_is_written_tab_separated() -> TestResult {
    let (written, reread) = create_and_reload("y.tab", "")?;
    assert_eq!(written, "a\tb\n1\tp\n2\tq\n");
    assert_eq!(reread, "b,a\np,1\nq,2\n");
    Ok(())
}

/// Control (passed when written): any other extension is comma-separated.
#[test]
fn location_csv_is_written_comma_separated() -> TestResult {
    let (written, reread) = create_and_reload("c.csv", "")?;
    assert_eq!(written, "a,b\n1,p\n2,q\n");
    assert_eq!(reread, "b,a\np,1\nq,2\n");
    Ok(())
}

/// Control (passed when written): an explicit `WITH (DELIMITER=...)` wins
/// over the extension.
#[test]
fn location_explicit_delimiter_wins_over_extension() -> TestResult {
    let dir = create_temp_dir()?;
    fs::write(dir.path().join("one.csv"), "k\n1\n")?;
    ok_in(
        dir.path(),
        &[
            "--write",
            "-s",
            "CREATE TABLE t (a INT, b TEXT) LOCATION './z.tsv' WITH (DELIMITER=';'); \
             INSERT INTO t VALUES (1, 'p')",
            "one.csv",
        ],
        b"",
    )?;
    let written = fs::read_to_string(dir.path().join("z.tsv"))?.replace("\r\n", "\n");
    assert_eq!(written, "a;b\n1;p\n");
    Ok(())
}

// ---------------------------------------------------------------------------
// B: declared column types are recorded as written
// ---------------------------------------------------------------------------

/// `.schema` shows each declared type name as written.
#[test]
fn schema_shows_declared_type_names() -> TestResult {
    let out = repl(
        "CREATE TABLE q (a BIGINT, b DECIMAL(10,2), c VARCHAR(20), d INT, \
         e BOOLEAN, f BLOB, g DOUBLE PRECISION, h MONEY);\n.schema q\n",
    )?;
    assert_eq!(
        out,
        "CREATE TABLE q (\n  a BIGINT,\n  b DECIMAL(10,2),\n  c VARCHAR(20),\n  d INT,\n  \
         e BOOLEAN,\n  f BLOB,\n  g DOUBLE PRECISION,\n  h MONEY\n);\n"
    );
    Ok(())
}

/// ALTER TABLE ... ADD COLUMN records its declared type as written too.
#[test]
fn alter_add_column_shows_declared_type_name() -> TestResult {
    let out = repl("CREATE TABLE q (a INTEGER);\nALTER TABLE q ADD COLUMN b BIGINT;\n.schema q\n")?;
    assert_eq!(out, "CREATE TABLE q (\n  a INTEGER,\n  b BIGINT\n);\n");
    Ok(())
}

/// CREATE TABLE ... AS names each column's type by its affinity: a column
/// of a declared table carries its declared type's SQLite affinity (INT
/// substring -> INTEGER; CHAR/CLOB/TEXT -> TEXT; BLOB -> BLOB;
/// REAL/FLOA/DOUB -> REAL; anything else -> NUMERIC), and a loaded column
/// or an expression keeps the type CTAS always gave it.
#[test]
fn ctas_column_types_follow_declared_affinity() -> TestResult {
    let out = repl(
        "CREATE TABLE q (a BIGINT, b DECIMAL(10,2), c VARCHAR(20), e BOOLEAN, \
         f BLOB, g DOUBLE PRECISION, h CHARINT);\n\
         CREATE TABLE r AS SELECT a, b, c, e, f, g, h, k, LENGTH(k) AS n FROM q, one;\n\
         .schema r\n",
    )?;
    assert_eq!(
        out,
        "CREATE TABLE r (\n  a INTEGER,\n  b NUMERIC,\n  c TEXT,\n  e NUMERIC,\n  f BLOB,\n  \
         g REAL,\n  h INTEGER,\n  k TEXT,\n  n INTEGER\n);\n"
    );
    Ok(())
}

/// Control (passed when written): a declared type is not enforced -- every
/// value keeps its own type.
#[test]
fn declared_types_are_not_enforced() -> TestResult {
    let out = repl(
        "CREATE TABLE q (a BIGINT, b DECIMAL(10,2), c VARCHAR(2));\n\
         INSERT INTO q VALUES ('abc', 'x', 12345);\n\
         SELECT a, typeof(a), b, typeof(b), c, typeof(c) FROM q;\n",
    )?;
    assert_eq!(
        out,
        "a,typeof(a),b,typeof(b),c,typeof(c)\nabc,text,x,text,12345,integer\n"
    );
    Ok(())
}
