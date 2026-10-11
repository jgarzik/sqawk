//! File IO: file-type rules, table specs, and byte-exact writeback.
//!
//! Every assertion is exact: complete stdout, or the complete file bytes
//! after `--write`.

use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};

use crate::helpers::{assert_query_on, create_temp_dir, run_sql};

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// Write `content` to `dir/name`, returning the path as a string.
fn put(dir: &Path, name: &str, content: &[u8]) -> Result<String, Box<dyn std::error::Error>> {
    let p = dir.join(name);
    fs::write(&p, content)?;
    Ok(p.to_str().unwrap().to_string())
}

/// Run `sql` with `--write` (plus `extra`) over a temp file `name` holding
/// `content`; require success and return the file's bytes afterwards.
fn write_back(
    name: &str,
    content: &[u8],
    extra: &[&str],
    sql: &str,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let dir = create_temp_dir()?;
    let p = put(dir.path(), name, content)?;
    let mut args = extra.to_vec();
    args.push("--write");
    let (_out, stderr, ok) = run_sql(sql, &[&p], &args)?;
    assert!(ok, "write failed\n  sql: {sql}\n  stderr: {stderr}");
    Ok(fs::read(&p)?)
}

fn assert_write_back(
    name: &str,
    content: &[u8],
    extra: &[&str],
    sql: &str,
    expected: &[u8],
) -> TestResult {
    let actual = write_back(name, content, extra, sql)?;
    assert_eq!(
        String::from_utf8_lossy(&actual),
        String::from_utf8_lossy(expected),
        "\n=== sql ===\n{sql}\n"
    );
    assert_eq!(actual, expected);
    Ok(())
}

// ---------------------------------------------------------------------------
// File-type rules
// ---------------------------------------------------------------------------

#[test]
fn tab_extension_is_tab_separated() -> TestResult {
    assert_query_on(&[("t.tab", "a\tb\n1\t2\n")], "SELECT b FROM t", "b\n2")?;
    assert_query_on(&[("u.TSV", "a\tb\n1\t2\n")], "SELECT b FROM u", "b\n2")
}

#[test]
fn field_separator_overrides_extension() -> TestResult {
    let dir = create_temp_dir()?;
    let p = put(dir.path(), "t.tsv", b"a;b\n1;2\n")?;
    let (out, stderr, ok) = run_sql("SELECT b FROM t", &[&p], &["-F;"])?;
    assert!(ok, "{stderr}");
    assert_eq!(out, "b\n2\n");
    Ok(())
}

#[test]
fn stdin_is_comma_separated_with_header() -> TestResult {
    let mut child = Command::new(env!("CARGO_BIN_EXE_sqawk"))
        .args(["-s", "SELECT b FROM stdin", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;
    std::io::Write::write_all(&mut child.stdin.take().unwrap(), b"a,b\n1,2\n")?;
    let out = child.wait_with_output()?;
    assert!(out.status.success());
    assert_eq!(String::from_utf8(out.stdout)?, "b\n2\n");
    Ok(())
}

#[test]
fn comments_flag_applies_to_csv_too() -> TestResult {
    assert_write_back(
        "c.csv",
        b"# head\na,b\n# mid\n1,2\n3,4\n# tail\n",
        &["--comments"],
        "DELETE FROM c WHERE a = 1",
        b"# head\na,b\n# mid\n3,4\n# tail\n",
    )
}

// ---------------------------------------------------------------------------
// Verbatim writeback of untouched rows
// ---------------------------------------------------------------------------

/// Blank lines are kept where they were.
#[test]
fn blank_lines_are_kept() -> TestResult {
    assert_write_back(
        "b.csv",
        b"a,b\n\n1,2\n\n\n3,4\n\n",
        &[],
        "UPDATE b SET b = 9 WHERE a = 3",
        b"a,b\n\n1,2\n\n\n3,9\n\n",
    )
}

/// A CRLF file whose last line has no line ending gets CRLF before an
/// appended row; the untouched rows keep their bytes.
#[test]
fn crlf_insert_after_unterminated_last_line() -> TestResult {
    assert_write_back(
        "c.csv",
        b"k,s\r\n1,\"a\"\r\n2,b",
        &[],
        "INSERT INTO c VALUES (3, 'c')",
        b"k,s\r\n1,\"a\"\r\n2,b\r\n3,c\r\n",
    )
}

/// Mixed line endings in untouched rows survive.
#[test]
fn mixed_line_endings_are_kept() -> TestResult {
    let content = b"k,s\r\n1,a\n2,b\r\n3,c\n";
    assert_write_back("m.csv", content, &[], "UPDATE m SET k = k", content)
}

/// An UPDATE that changes a row writes that row from its values; the other
/// rows keep their bytes.
#[test]
fn changed_row_is_rewritten() -> TestResult {
    assert_write_back(
        "q.csv",
        b"k,s\n1,\"abc\"\n2,\"abc\"\n",
        &[],
        "UPDATE q SET s = 'x' WHERE k = 2",
        b"k,s\n1,\"abc\"\n2,x\n",
    )
}

/// A value replaced by an equal value of another type (5 -> 5.0) is a change.
#[test]
fn update_to_different_type_is_a_change() -> TestResult {
    assert_write_back(
        "q.csv",
        b"k,s\n1,5\n2,5\n",
        &[],
        "UPDATE q SET s = 5.0 WHERE k = 2",
        b"k,s\n1,5\n2,5.0\n",
    )
}

/// ALTER TABLE ADD COLUMN changes every line, header included.
#[test]
fn alter_add_column_rewrites_every_line() -> TestResult {
    assert_write_back(
        "q.csv",
        b"\"k\",s\n1,\"abc\"\n",
        &[],
        "ALTER TABLE q ADD COLUMN t TEXT",
        b"k,s,t\n1,abc,\n",
    )
}

/// DELETE keeps the surviving rows' bytes; the BOM stays.
#[test]
fn delete_keeps_other_rows_and_bom() -> TestResult {
    assert_write_back(
        "d.csv",
        b"\xef\xbb\xbfk,s\n1, a \n2,b\n3,\"c\"",
        &[],
        "DELETE FROM d WHERE k = 2",
        b"\xef\xbb\xbfk,s\n1, a \n3,\"c\"",
    )
}

// ---------------------------------------------------------------------------
// Malformed input
// ---------------------------------------------------------------------------

/// An unclosed quote is a load error naming the line where the field
/// starts; with --write nothing is written.
#[test]
fn unclosed_quote_is_load_error() -> TestResult {
    let dir = create_temp_dir()?;
    let content = b"a,b\n1,2\n3,\"x\n4,5\n";
    let p = put(dir.path(), "u.csv", content)?;
    let (_out, stderr, ok) = run_sql("DELETE FROM u", &[&p], &["--write"])?;
    assert!(!ok);
    assert!(
        stderr.contains("u.csv: line 3: unterminated quoted field"),
        "{stderr}"
    );
    assert_eq!(fs::read(&p)?, content);
    Ok(())
}

#[test]
fn duplicate_header_names_are_named() -> TestResult {
    let dir = create_temp_dir()?;
    let p = put(dir.path(), "h.csv", b"id,Name,x,NAME,y\n1,2,3,4,5\n")?;
    let (_out, stderr, ok) = run_sql("SELECT 1", &[&p], &[])?;
    assert!(!ok);
    assert!(stderr.contains("duplicate column name: NAME"), "{stderr}");
    Ok(())
}

// ---------------------------------------------------------------------------
// Column names are case-insensitive in every statement
// ---------------------------------------------------------------------------

#[test]
fn insert_and_update_columns_are_case_insensitive() -> TestResult {
    assert_write_back(
        "c.csv",
        b"Id,Name\n1,a\n",
        &[],
        "INSERT INTO c (ID, NAME) VALUES (2, 'b'); UPDATE c SET name = 'z' WHERE id = 1",
        b"Id,Name\n1,z\n2,b\n",
    )
}

// ---------------------------------------------------------------------------
// Table specs
// ---------------------------------------------------------------------------

#[test]
fn name_prefix_names_the_table() -> TestResult {
    let dir = create_temp_dir()?;
    put(dir.path(), "data.csv", b"a\n1\n")?;
    let spec = format!("t_1={}", dir.path().join("data.csv").display());
    let (out, stderr, ok) = run_sql("SELECT a FROM t_1", &[&spec], &[])?;
    assert!(ok, "{stderr}");
    assert_eq!(out, "a\n1\n");
    Ok(())
}

#[test]
fn file_name_without_stem_is_error() -> TestResult {
    let dir = create_temp_dir()?;
    let p = put(dir.path(), ".csv", b"a\n1\n")?;
    let (_out, stderr, ok) = run_sql("SELECT 1", &[&p], &[])?;
    assert!(!ok);
    assert!(stderr.contains("cannot derive a table name"), "{stderr}");
    Ok(())
}

#[cfg(unix)]
#[test]
fn same_file_through_symlink_is_error() -> TestResult {
    let dir = create_temp_dir()?;
    let p = put(dir.path(), "a.csv", b"a\n1\n")?;
    let link = dir.path().join("b.csv");
    std::os::unix::fs::symlink(&p, &link)?;
    let (_out, stderr, ok) = run_sql("SELECT 1", &[&p, link.to_str().unwrap()], &[])?;
    assert!(!ok);
    assert!(stderr.contains("is already loaded as table a"), "{stderr}");
    Ok(())
}
