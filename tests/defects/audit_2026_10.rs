//! Red list from the 2026-10 audit.
//!
//! Every test in this module encodes **correct** behaviour for a defect found
//! in the 2026-10 audit, and each is `#[ignore]`d until its defect is fixed.
//! When a fix lands, remove the `#[ignore]` on the tests it turns green; the
//! list only shrinks.
//!
//! Run the red list with:
//!     cargo test --test mod defects -- --ignored
//!
//! Expected outputs were computed by loading the same data into SQLite
//! (python3 `sqlite3`) unless the doc comment says otherwise. Output NULL is
//! rendered as an EMPTY field; most tests avoid NULL in their expected output
//! so that they fail only for the defect they name.
//!
//! Root-cause locations are hints as of the audit (file:line), not contracts.

use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use crate::helpers::{
    assert_query, assert_query_fails, create_temp_dir, departments_csv, employees_csv,
    nullable_csv, orders_csv, run_sql, users_csv,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

// ---------------------------------------------------------------------------
// Local helpers
// ---------------------------------------------------------------------------

/// Small outer-join fixtures. `lt` and `rt` share keys 1 and 3; key 2 is only
/// in `lt` and key 4 only in `rt`.
const LT: &str = "k,a\n1,10\n2,20\n3,30\n";
const RT: &str = "k,b\n1,100\n3,300\n4,400\n";
const XT: &str = "k,c\n1,one\n2,two\n";

/// Write each `(file_name, content)` into `dir`, returning the paths.
fn write_files(
    dir: &Path,
    files: &[(&str, &[u8])],
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut out = Vec::new();
    for (name, content) in files {
        let p = dir.join(name);
        fs::write(&p, content)?;
        out.push(p.to_str().unwrap().to_string());
    }
    Ok(out)
}

/// `assert_query` over ad-hoc tables written to a fresh temp dir.
fn assert_query_on(files: &[(&str, &str)], sql: &str, expected: &str) -> TestResult {
    let dir = create_temp_dir()?;
    let bytes: Vec<(&str, &[u8])> = files.iter().map(|(n, c)| (*n, c.as_bytes())).collect();
    let paths = write_files(dir.path(), &bytes)?;
    let refs: Vec<&str> = paths.iter().map(String::as_str).collect();
    assert_query(&refs, sql, expected)
}

/// Copy `content` into a temp file `file_name`, run `sql` with `--write`, and
/// return the file's raw bytes afterwards (no normalization).
fn write_back(
    file_name: &str,
    content: &[u8],
    sql: &str,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let dir = create_temp_dir()?;
    let paths = write_files(dir.path(), &[(file_name, content)])?;
    let (_out, stderr, ok) = run_sql(sql, &[&paths[0]], &["--write"])?;
    assert!(ok, "write query failed\n  sql: {sql}\n  stderr: {stderr}");
    Ok(fs::read(&paths[0])?)
}

/// Assert that a `--write` run leaves the file byte-for-byte equal to `expected`.
fn assert_write_back(file_name: &str, content: &str, sql: &str, expected: &str) -> TestResult {
    let actual = write_back(file_name, content.as_bytes(), sql)?;
    assert_eq!(
        String::from_utf8_lossy(&actual),
        expected,
        "\n=== sql ===\n{sql}\n=== input ===\n{content:?}\n=== expected (exact bytes) ===\n{expected:?}\n=== actual ===\n{:?}\n",
        String::from_utf8_lossy(&actual)
    );
    Ok(())
}

/// Assert `sql` succeeds and emits no data rows (header only, or nothing).
fn assert_no_rows(files: &[&str], sql: &str) -> TestResult {
    let (stdout, stderr, ok) = run_sql(sql, files, &[])?;
    assert!(ok, "query failed\n  sql: {sql}\n  stderr: {stderr}");
    let lines = stdout.lines().filter(|l| !l.trim().is_empty()).count();
    assert!(
        lines <= 1,
        "expected no data rows\n  sql: {sql}\n  stdout:\n{stdout}"
    );
    Ok(())
}

/// Assert `sql` succeeds and emits exactly `n` data rows after the header.
fn assert_row_count(files: &[&str], sql: &str, n: usize) -> TestResult {
    let (stdout, stderr, ok) = run_sql(sql, files, &[])?;
    assert!(ok, "query failed\n  sql: {sql}\n  stderr: {stderr}");
    let lines = stdout.lines().filter(|l| !l.trim().is_empty()).count();
    assert_eq!(
        lines,
        n + 1,
        "expected {n} data rows\n  sql: {sql}\n  stdout:\n{stdout}"
    );
    Ok(())
}

/// Today's date (UTC) as YYYY-MM-DD, computed from SystemTime without chrono
/// (Howard Hinnant's civil_from_days).
fn today_utc() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let z = secs.div_euclid(86_400) + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!("{y:04}-{m:02}-{d:02}")
}

fn sqawk_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_sqawk"))
}

// ---------------------------------------------------------------------------
// Group 1: Data integrity / IO
// ---------------------------------------------------------------------------

/// Writeback re-renders every cell from its inferred type, so `007` becomes
/// `7` and `01234` becomes `1234` (type inference in table.rs / csv_handler.rs:
/// non-canonical integers must stay text). Untouched cells must be
/// byte-identical.
#[test]
fn writeback_preserves_leading_zeros() -> TestResult {
    assert_write_back(
        "t.csv",
        "id,zip,note\n1,007,a\n2,01234,b\n",
        "UPDATE t SET note = 'c' WHERE id = 2",
        "id,zip,note\n1,007,a\n2,01234,c\n",
    )
}

/// Same root cause: `1.50` -> `1.5`, `1e3` -> `1000`, `2.0` -> `2`.
#[test]
fn writeback_preserves_decimal_spelling() -> TestResult {
    assert_write_back(
        "t.csv",
        "id,amt,note\n1,1.50,a\n2,1e3,b\n3,2.0,c\n",
        "UPDATE t SET note = 'z' WHERE id = 3",
        "id,amt,note\n1,1.50,a\n2,1e3,b\n3,2.0,z\n",
    )
}

/// Writeback drops the quotes from `"  sp  "` (csv writer in file_handler.rs
/// does not preserve/require quoting for significant whitespace).
#[test]
fn writeback_preserves_quoted_spaces() -> TestResult {
    assert_write_back(
        "t.csv",
        "id,note,v\n1,\"  sp  \",a\n2,x,b\n",
        "UPDATE t SET v = 'c' WHERE id = 2",
        "id,note,v\n1,\"  sp  \",a\n2,x,c\n",
    )
}

/// Writeback double-escapes embedded quotes: `"say ""hi"""` becomes
/// `"say """"hi"""""` (the value is escaped once by sqawk and again by the
/// csv writer).
#[test]
fn writeback_preserves_embedded_quotes() -> TestResult {
    assert_write_back(
        "t.csv",
        "id,note,v\n1,\"say \"\"hi\"\"\",a\n2,x,b\n",
        "UPDATE t SET v = 'c' WHERE id = 2",
        "id,note,v\n1,\"say \"\"hi\"\"\",a\n2,x,c\n",
    )
}

/// An empty field loads as NULL and is written back as the literal `NULL`.
#[test]
fn writeback_preserves_empty_field() -> TestResult {
    assert_write_back(
        "t.csv",
        "id,note,v\n1,,a\n2,x,b\n",
        "UPDATE t SET v = 'c' WHERE id = 2",
        "id,note,v\n1,,a\n2,x,c\n",
    )
}

/// An explicitly inserted NULL must be written back as an empty field, not
/// the text `NULL`.
#[test]
fn writeback_writes_null_as_empty() -> TestResult {
    assert_write_back(
        "t.csv",
        "id,note,v\n1,x,a\n",
        "INSERT INTO t VALUES (2, NULL, 'b')",
        "id,note,v\n1,x,a\n2,,b\n",
    )
}

/// A `.csv` data row whose first field starts with `#` is treated as a
/// comment and silently dropped on load, so writeback deletes it
/// (comment skipping in csv_handler.rs must not apply to .csv files).
#[test]
fn writeback_preserves_hash_row() -> TestResult {
    assert_write_back(
        "h.csv",
        "name,v\n#x,1\ny,2\n",
        "UPDATE h SET v = 3 WHERE name = 'y'",
        "name,v\n#x,1\ny,3\n",
    )
}

/// Same root cause: in a .csv, a `#` line is data.
#[test]
fn csv_hash_line_is_data() -> TestResult {
    assert_query_on(
        &[("h.csv", "name,v\n#x,1\ny,2\n")],
        "SELECT name, v FROM h",
        "name,v\n#x,1\ny,2",
    )
}

/// Writeback always emits LF, converting a CRLF file to LF line endings.
#[test]
fn writeback_preserves_crlf() -> TestResult {
    assert_write_back(
        "c.csv",
        "a,b\r\n1,x\r\n2,y\r\n",
        "UPDATE c SET b = 'z' WHERE a = 2",
        "a,b\r\n1,x\r\n2,z\r\n",
    )
}

/// The atomic temp-file-and-rename writeback (file_handler.rs) creates the new
/// file with mode 0600, so a 0644 file loses group/other read permission.
#[cfg(unix)]
#[test]
fn writeback_preserves_file_mode() -> TestResult {
    use std::os::unix::fs::PermissionsExt;
    let dir = create_temp_dir()?;
    let p = dir.path().join("m.csv");
    fs::write(&p, "a,b\n1,x\n")?;
    fs::set_permissions(&p, fs::Permissions::from_mode(0o644))?;
    let (_o, stderr, ok) = run_sql("UPDATE m SET b = 'z'", &[p.to_str().unwrap()], &["--write"])?;
    assert!(ok, "write failed: {stderr}");
    assert_eq!(fs::read_to_string(&p)?, "a,b\n1,z\n");
    let mode = fs::metadata(&p)?.permissions().mode() & 0o777;
    assert_eq!(mode, 0o644, "file mode changed to {mode:o}");
    Ok(())
}

/// Writing through a symlink renames the temp file over the link itself: the
/// symlink is replaced by a regular file and the target is left stale.
#[cfg(unix)]
#[test]
fn writeback_through_symlink_updates_target() -> TestResult {
    let dir = create_temp_dir()?;
    let target = dir.path().join("target.csv");
    let link = dir.path().join("link.csv");
    fs::write(&target, "a,b\n1,x\n")?;
    std::os::unix::fs::symlink(&target, &link)?;
    let (_o, stderr, ok) = run_sql(
        "UPDATE link SET b = 'z'",
        &[link.to_str().unwrap()],
        &["--write"],
    )?;
    assert!(ok, "write failed: {stderr}");
    assert!(
        fs::symlink_metadata(&link)?.file_type().is_symlink(),
        "symlink was replaced by a regular file"
    );
    assert_eq!(fs::read_to_string(&target)?, "a,b\n1,z\n");
    Ok(())
}

/// A quoted field containing a newline is split into two records on load
/// (line-oriented reading in csv_handler.rs).
#[test]
fn quoted_newline_is_one_row() -> TestResult {
    assert_query_on(
        &[("n.csv", "a,b\n1,\"x\ny\"\n2,z\n")],
        "SELECT COUNT(*) AS c FROM n",
        "c\n2",
    )
}

/// Same root cause: the embedded-newline record is mangled on writeback.
#[test]
fn writeback_preserves_quoted_newline() -> TestResult {
    assert_write_back(
        "n.csv",
        "a,b\n1,\"x\ny\"\n2,z\n",
        "UPDATE n SET b = 'w' WHERE a = 2",
        "a,b\n1,\"x\ny\"\n2,w\n",
    )
}

/// Output NULL is rendered as the text `NULL`; it must be an empty field
/// (value formatting for stdout in main.rs / table.rs).
#[test]
fn stdout_null_is_empty_field() -> TestResult {
    assert_query(
        &[nullable_csv()],
        "SELECT id, score FROM nullable WHERE id = 2",
        "id,score\n2,",
    )
}

/// Stdout is not CSV: fields containing the delimiter or quotes are printed
/// raw (`1,x, y`), so the output cannot be parsed back. RFC 4180 quoting
/// required.
#[test]
fn stdout_is_rfc4180_quoted() -> TestResult {
    assert_query_on(
        &[("q.csv", "a,b\n1,\"x, y\"\n2,\"q\"\"r\"\n")],
        "SELECT a, b FROM q",
        "a,b\n1,\"x, y\"\n2,\"q\"\"r\"",
    )
}

/// Numeric-looking text that does not round-trip canonically must stay text:
/// `007` displays as `7`, `1.50` as `1.5`, `1e3` as `1000`.
#[test]
fn non_canonical_numbers_display_verbatim() -> TestResult {
    assert_query_on(
        &[("t.csv", "zip,amt,e\n007,1.50,1e3\n")],
        "SELECT zip, amt, e FROM t",
        "zip,amt,e\n007,1.50,1e3",
    )
}

/// The headerless auto-detection (file_handler.rs) guesses that a numeric
/// first line is data and names columns a,b. For .csv files the first line is
/// always the header.
#[test]
fn csv_first_line_is_always_header() -> TestResult {
    assert_query_on(
        &[("num.csv", "1,2\n3,4\n")],
        "SELECT * FROM num",
        "1,2\n3,4",
    )
}

/// A UTF-8 BOM is kept as part of the first column name, so `id` is not found.
#[test]
fn utf8_bom_is_stripped() -> TestResult {
    let dir = create_temp_dir()?;
    let p = write_files(dir.path(), &[("bom.csv", b"\xef\xbb\xbfid,name\n1,x\n")])?;
    assert_query(&[&p[0]], "SELECT id, name FROM bom", "id,name\n1,x")
}

/// Two input files resolving to the same table name are silently accepted
/// (the second replaces the first in database.rs); must be an error.
#[test]
fn duplicate_table_name_is_error() -> TestResult {
    let dir = create_temp_dir()?;
    fs::create_dir(dir.path().join("d1"))?;
    fs::create_dir(dir.path().join("d2"))?;
    let a = dir.path().join("d1/t.csv");
    let b = dir.path().join("d2/t.csv");
    fs::write(&a, "a\n1\n")?;
    fs::write(&b, "a\n2\n")?;
    let (stdout, _stderr, ok) = run_sql(
        "SELECT a FROM t",
        &[a.to_str().unwrap(), b.to_str().unwrap()],
        &[],
    )?;
    assert!(!ok, "duplicate table name accepted; stdout:\n{stdout}");
    Ok(())
}

/// `-F ''` panics with index-out-of-bounds (file_handler.rs:250); must be a
/// clean error.
#[test]
fn empty_field_separator_is_clean_error() -> TestResult {
    let (_stdout, stderr, ok) = run_sql("SELECT * FROM users", &[users_csv()], &["-F", ""])?;
    assert!(!ok, "empty separator accepted");
    assert!(!stderr.contains("panicked"), "panicked:\n{stderr}");
    Ok(())
}

/// Writing to a closed pipe (`sqawk ... | head -1`) panics with "failed
/// printing to stdout: Broken pipe" (println! in main.rs output path).
#[test]
fn closed_stdout_pipe_does_not_panic() -> TestResult {
    let dir = create_temp_dir()?;
    let p = dir.path().join("big.csv");
    let mut s = String::from("n\n");
    for i in 0..200_000 {
        s.push_str(&i.to_string());
        s.push('\n');
    }
    fs::write(&p, s)?;
    let mut child = Command::new(sqawk_bin())
        .arg("-s")
        .arg("SELECT n FROM big")
        .arg(&p)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    {
        let mut out = BufReader::new(child.stdout.take().unwrap());
        let mut line = String::new();
        out.read_line(&mut line)?;
        assert_eq!(line, "n\n");
        // `out` dropped here: the read end of the pipe is closed.
    }
    let output = child.wait_with_output()?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stderr.contains("panicked"), "panicked on EPIPE:\n{stderr}");
    Ok(())
}

/// `-v` diagnostics (arguments, bytecode, execution trace) are printed to
/// stdout, corrupting the result stream. They belong on stderr.
#[test]
fn verbose_diagnostics_not_on_stdout() -> TestResult {
    let sql = "SELECT id, name FROM users";
    let (plain, _e, ok1) = run_sql(sql, &[users_csv()], &[])?;
    let (verbose, _e, ok2) = run_sql(sql, &[users_csv()], &["-v"])?;
    assert!(ok1 && ok2);
    assert_eq!(verbose, plain, "-v changed stdout");
    Ok(())
}

// ---------------------------------------------------------------------------
// Group 2: Wrong answers (silent)
// ---------------------------------------------------------------------------

/// Uncorrelated IN-subqueries are filtered by a compile-time interpreter
/// (`evaluate_where_at_compile_time`, compiler.rs:3198) that treats any
/// operator it does not know -- here LIKE -- as true, so every row matches.
#[test]
fn in_subquery_with_like() -> TestResult {
    assert_query(
        &[employees_csv()],
        "SELECT name FROM employees WHERE id IN (SELECT id FROM employees WHERE department LIKE 'E%')",
        "name\nAlice\nCharlie\nFrank",
    )
}

/// Same root cause: a function call in the subquery WHERE matches nothing.
#[test]
fn in_subquery_with_function() -> TestResult {
    assert_query(
        &[employees_csv()],
        "SELECT id FROM employees WHERE id IN (SELECT id FROM employees WHERE UPPER(department) = 'HR')",
        "id\n8",
    )
}

/// Same root cause: arithmetic in the subquery WHERE matches nothing.
#[test]
fn in_subquery_with_arithmetic() -> TestResult {
    assert_query(
        &[employees_csv()],
        "SELECT name FROM employees WHERE id IN (SELECT id FROM employees WHERE age + 0 = 30)",
        "name\nAlice",
    )
}

/// Same root cause: an IN-list in an EXISTS subquery WHERE is treated as
/// true, so EXISTS over an empty result is true.
#[test]
fn exists_subquery_with_in_list() -> TestResult {
    assert_no_rows(
        &[employees_csv(), departments_csv()],
        "SELECT name FROM employees WHERE EXISTS (SELECT 1 FROM departments WHERE department IN ('Nope', 'Zilch'))",
    )
}

/// Uncorrelated scalar subquery whose select item is an expression over an
/// aggregate (`MAX(x) + 0`) evaluates wrongly, so the comparison never holds.
#[test]
fn scalar_subquery_with_aggregate_arithmetic() -> TestResult {
    assert_query(
        &[users_csv(), employees_csv()],
        "SELECT id FROM users WHERE (SELECT MAX(salary) + 0 FROM employees WHERE department = 'HR') = 45000",
        "id\n1\n2",
    )
}

/// NOT IN treats a NULL left operand as "not in"; NULL NOT IN (...) is
/// unknown and must not match (three-valued logic in the WHERE compiler).
#[test]
fn not_in_list_excludes_null() -> TestResult {
    assert_query(
        &[nullable_csv()],
        "SELECT id FROM nullable WHERE score NOT IN (90, 70)",
        "id\n4",
    )
}

/// NOT BETWEEN matches NULL.
#[test]
fn not_between_excludes_null() -> TestResult {
    assert_query(
        &[nullable_csv()],
        "SELECT id FROM nullable WHERE score NOT BETWEEN 60 AND 80",
        "id\n1\n4\n5",
    )
}

/// NOT (x LIKE ...) matches NULL.
#[test]
fn not_of_like_excludes_null() -> TestResult {
    assert_query(
        &[nullable_csv()],
        "SELECT id FROM nullable WHERE NOT (name LIKE 'A%')",
        "id\n2\n3\n5",
    )
}

/// NOT (a OR b) where `a` is NULL and `b` false is NOT(NULL) = NULL; sqawk
/// treats it as true.
#[test]
fn not_of_or_excludes_null() -> TestResult {
    assert_query(
        &[nullable_csv()],
        "SELECT id FROM nullable WHERE NOT (score > 60 OR id = 1)",
        "id\n4",
    )
}

/// `x NOT IN (SELECT col)` where col contains a NULL is never true; sqawk
/// returns every row.
#[test]
fn not_in_subquery_with_null_is_empty() -> TestResult {
    assert_no_rows(
        &[employees_csv(), nullable_csv()],
        "SELECT id FROM employees WHERE age NOT IN (SELECT score FROM nullable)",
    )
}

/// `1 NOT IN (2, NULL)` is unknown, so no rows.
#[test]
fn literal_not_in_list_with_null_is_unknown() -> TestResult {
    assert_no_rows(
        &[employees_csv()],
        "SELECT id FROM employees WHERE 1 NOT IN (2, NULL)",
    )
}

/// `NULL AND 1` is NULL, not false.
#[test]
fn null_and_true_is_null() -> TestResult {
    assert_query(
        &[users_csv()],
        "SELECT id, (NULL AND 1) IS NULL AS x FROM users",
        "id,x\n1,1\n2,1",
    )
}

/// `DELETE ... WHERE grade NOT IN ('A')` also deletes the row whose grade is
/// NULL; NOT IN over NULL is unknown and must keep it.
#[test]
fn delete_not_in_keeps_null_row() -> TestResult {
    assert_query(
        &[nullable_csv()],
        "DELETE FROM nullable WHERE grade NOT IN ('A'); SELECT id FROM nullable",
        "id\n1\n3\n5",
    )
}

/// WHERE is ignored on LEFT JOIN (compiler_join.rs:384), so the anti-join
/// idiom returns every left row.
#[test]
fn left_join_where_is_null_anti_join() -> TestResult {
    assert_query_on(
        &[("lt.csv", LT), ("rt.csv", RT)],
        "SELECT lt.a AS a FROM lt LEFT JOIN rt ON lt.k = rt.k WHERE rt.k IS NULL",
        "a\n20",
    )
}

/// Same root cause: a value filter on the right table of a LEFT JOIN.
#[test]
fn left_join_where_on_right_column() -> TestResult {
    assert_query(
        &[users_csv(), orders_csv()],
        "SELECT users.name AS n FROM users LEFT JOIN orders ON users.id = orders.user_id WHERE orders.id = 102",
        "n\nJane",
    )
}

/// Same root cause on RIGHT JOIN (also hit by the right-table column shift
/// below).
#[test]
fn right_join_where_is_null() -> TestResult {
    assert_query_on(
        &[("lt.csv", LT), ("rt.csv", RT)],
        "SELECT rt.k AS k, rt.b AS b FROM lt RIGHT JOIN rt ON lt.k = rt.k WHERE lt.k IS NULL",
        "k,b\n4,400",
    )
}

/// Same root cause on FULL JOIN.
#[test]
fn full_join_where_is_null() -> TestResult {
    assert_query_on(
        &[("lt.csv", LT), ("rt.csv", RT)],
        "SELECT lt.a AS a FROM lt FULL JOIN rt ON lt.k = rt.k WHERE rt.k IS NULL",
        "a\n20",
    )
}

/// Explicit CROSS JOIN drops the WHERE clause, returning the full product.
#[test]
fn cross_join_applies_where() -> TestResult {
    assert_query(
        &[users_csv(), orders_csv()],
        "SELECT users.name AS n FROM users CROSS JOIN orders WHERE orders.id = 101",
        "n\nJohn\nJane",
    )
}

/// Explicit CROSS JOIN does not register table aliases ("Table 'u' not found").
#[test]
fn cross_join_with_aliases() -> TestResult {
    assert_query(
        &[users_csv(), orders_csv()],
        "SELECT u.name AS n FROM users u CROSS JOIN orders o WHERE o.id = 101",
        "n\nJohn\nJane",
    )
}

/// In a two-table join, selecting right-table columns without any left-table
/// column resolves them one slot to the left: `orders.id` yields NULL and
/// `orders.date` yields the id (JOIN projection column offsets,
/// compiler_join.rs).
#[test]
fn join_right_table_columns_resolve_correctly() -> TestResult {
    assert_query(
        &[users_csv(), orders_csv()],
        "SELECT orders.id AS oid, orders.date AS d FROM users JOIN orders ON users.id = orders.user_id WHERE orders.id = 102",
        "oid,d\n102,2023-01-20",
    )
}

/// INSERT with a column list writes values positionally into the wrong
/// columns (compiler_dml.rs:60): email gets the name.
#[test]
fn insert_column_list_maps_by_name() -> TestResult {
    assert_query(
        &[users_csv()],
        "INSERT INTO users (email, id, name) VALUES ('z@x', 3, 'Zed'); SELECT * FROM users",
        "id,name,email\n1,John,john@example.com\n2,Jane,jane@example.com\n3,Zed,z@x",
    )
}

/// Omitted columns of a multi-row INSERT are not set to NULL (Null opcode
/// p1/p2 misuse in compiler_dml.rs): the second row inherits stale register
/// values.
#[test]
fn insert_multi_row_omitted_columns_are_null() -> TestResult {
    assert_query(
        &[users_csv()],
        "INSERT INTO users (id, name) VALUES (3, 'A'), (4, 'B'); SELECT id FROM users WHERE email IS NULL",
        "id\n3\n4",
    )
}

/// Same root cause, with the column list leaving a middle column out.
#[test]
fn insert_multi_row_omitted_middle_column() -> TestResult {
    assert_query(
        &[users_csv()],
        "INSERT INTO users (id, email) VALUES (3, 'a@x'), (4, 'b@x'); SELECT id, email FROM users WHERE name IS NULL",
        "id,email\n3,a@x\n4,b@x",
    )
}

/// Set operations ignore ORDER BY (compiler.rs:863).
#[test]
fn union_applies_order_by() -> TestResult {
    assert_query(
        &[employees_csv(), departments_csv()],
        "SELECT name FROM employees UNION SELECT name FROM departments ORDER BY name DESC",
        "name\nHenry\nGrace\nFrank\nEve\nDavid\nCharlie\nBob\nAlice",
    )
}

/// Set operations ignore LIMIT (compiler.rs:863).
#[test]
fn union_applies_limit() -> TestResult {
    assert_query(
        &[employees_csv(), departments_csv()],
        "SELECT name FROM employees UNION SELECT name FROM departments ORDER BY name DESC LIMIT 3",
        "name\nHenry\nGrace\nFrank",
    )
}

/// Same root cause on UNION ALL with ORDER BY on two columns.
#[test]
fn union_all_applies_order_by() -> TestResult {
    assert_query(
        &[employees_csv(), departments_csv()],
        "SELECT id, name FROM employees WHERE id = 1 UNION ALL SELECT id, name FROM departments WHERE id = 2 ORDER BY id DESC",
        "id,name\n2,Bob\n1,Alice",
    )
}

/// Same root cause on INTERSECT.
#[test]
fn intersect_applies_order_by() -> TestResult {
    assert_query(
        &[employees_csv(), departments_csv()],
        "SELECT department FROM employees INTERSECT SELECT department FROM departments ORDER BY department DESC",
        "department\nSales\nMarketing\nEngineering",
    )
}

/// Same root cause on EXCEPT.
#[test]
fn except_applies_order_by() -> TestResult {
    assert_query(
        &[employees_csv(), departments_csv()],
        "SELECT department FROM employees EXCEPT SELECT department FROM departments WHERE department = 'Sales' ORDER BY department DESC",
        "department\nMarketing\nHR\nEngineering",
    )
}

/// A set operation's header comes from the right-hand SELECT; SQL takes
/// column names from the left.
#[test]
fn union_header_from_left_side() -> TestResult {
    assert_query(
        &[users_csv(), orders_csv()],
        "SELECT id AS a FROM users UNION SELECT user_id AS b FROM orders",
        "a\n1\n2",
    )
}

/// DISTINCT on the right side of UNION ALL deduplicates the left side's rows
/// too (compiler.rs:1160).
#[test]
fn union_all_distinct_applies_to_one_side_only() -> TestResult {
    assert_query(
        &[employees_csv(), departments_csv()],
        "SELECT department FROM employees WHERE id < 3 UNION ALL SELECT DISTINCT department FROM departments",
        "department\nEngineering\nMarketing\nEngineering\nMarketing\nSales",
    )
}

/// LIMIT is ignored on a set operation of literal SELECTs.
#[test]
fn literal_union_applies_limit() -> TestResult {
    assert_row_count(&[users_csv()], "SELECT 1 UNION ALL SELECT 2 LIMIT 1", 1)
}

/// HAVING without GROUP BY is ignored.
#[test]
fn having_without_group_by() -> TestResult {
    assert_no_rows(
        &[employees_csv()],
        "SELECT COUNT(*) AS c FROM employees HAVING COUNT(*) > 100",
    )
}

/// HAVING is ignored on a JOIN ... GROUP BY.
#[test]
fn having_on_join() -> TestResult {
    assert_query(
        &[users_csv(), orders_csv()],
        "SELECT u.name AS n FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name HAVING COUNT(*) > 2",
        "n\nJohn",
    )
}

/// HAVING is ignored on a comma-join ... GROUP BY.
#[test]
fn having_on_comma_join() -> TestResult {
    assert_query(
        &[users_csv(), orders_csv()],
        "SELECT u.name AS n FROM users u, orders o WHERE u.id = o.user_id GROUP BY u.name HAVING COUNT(*) > 2",
        "n\nJohn",
    )
}

/// Comma-join GROUP BY with two keys groups by the first key only.
#[test]
fn comma_join_group_by_uses_all_keys() -> TestResult {
    assert_query(
        &[users_csv(), orders_csv()],
        "SELECT u.name AS n, o.id AS o FROM users u, orders o WHERE u.id = o.user_id GROUP BY u.name, o.id",
        "n,o\nJane,102\nJane,104\nJohn,101\nJohn,103\nJohn,105",
    )
}

/// Comma-join OFFSET skips only the Nth row instead of the first N rows.
/// Expected rows follow nested-loop order (users outer), which is what both
/// sqawk's scan and SQLite produce here.
#[test]
fn comma_join_offset_skips_first_n() -> TestResult {
    assert_query(
        &[users_csv(), orders_csv()],
        "SELECT users.name AS n, orders.id AS o FROM users, orders WHERE users.id = orders.user_id LIMIT 2 OFFSET 2",
        "n,o\nJohn,105\nJane,102",
    )
}

/// DISTINCT is dropped when LIMIT is present.
#[test]
fn distinct_with_limit() -> TestResult {
    assert_query(
        &[employees_csv()],
        "SELECT DISTINCT department FROM employees LIMIT 4",
        "department\nEngineering\nMarketing\nSales\nHR",
    )
}

/// LIMIT/OFFSET with a computed column returns no rows.
#[test]
fn limit_offset_with_computed_column() -> TestResult {
    assert_query(
        &[employees_csv()],
        "SELECT id, age + 1 AS a FROM employees LIMIT 2 OFFSET 2",
        "id,a\n3,36\n4,41",
    )
}

/// LIMIT 0 with a computed column returns every row.
#[test]
fn limit_zero_with_computed_column() -> TestResult {
    assert_no_rows(
        &[employees_csv()],
        "SELECT id, age + 1 AS a FROM employees LIMIT 0",
    )
}

/// `SELECT *, expr` puts the expression value in the second column and
/// shifts the star columns right, under the original headers.
#[test]
fn star_with_expression_column_order() -> TestResult {
    assert_query(
        &[employees_csv()],
        "SELECT *, age + 1 AS a FROM employees WHERE id = 1",
        "id,name,age,salary,department,role,a\n1,Alice,30,70000,Engineering,Developer,31",
    )
}

/// `SELECT 1 WHERE 0` emits a row: WHERE is ignored on FROM-less SELECT.
#[test]
fn fromless_select_applies_where() -> TestResult {
    assert_no_rows(&[users_csv()], "SELECT 1 WHERE 0")
}

/// FROM-less SELECT ignores LIMIT.
#[test]
fn fromless_select_applies_limit() -> TestResult {
    assert_no_rows(&[users_csv()], "SELECT 1 LIMIT 0")
}

/// `SUM(x) OVER ()` computes a running total instead of the partition total
/// (compiler_window.rs: empty OVER must use the whole partition as frame).
#[test]
#[ignore]
fn window_sum_over_empty_is_total() -> TestResult {
    assert_query(
        &[employees_csv()],
        "SELECT id, SUM(salary) OVER () AS s FROM employees WHERE id < 4",
        "id,s\n1,190000\n2,190000\n3,190000",
    )
}

/// Same root cause for `COUNT(*) OVER ()`.
#[test]
#[ignore]
fn window_count_over_empty_is_total() -> TestResult {
    assert_query(
        &[employees_csv()],
        "SELECT id, COUNT(*) OVER () AS c FROM employees WHERE id < 4",
        "id,c\n1,3\n2,3\n3,3",
    )
}

/// LAG's third (default) argument is ignored.
#[test]
#[ignore]
fn window_lag_default() -> TestResult {
    assert_query(
        &[employees_csv()],
        "SELECT id, LAG(name, 1, 'none') OVER (ORDER BY id) AS p FROM employees WHERE id < 4",
        "id,p\n1,none\n2,Alice\n3,Bob",
    )
}

/// `SUM() OVER (ORDER BY k)` uses a ROWS frame; the default frame is RANGE,
/// so ORDER BY peers share the same running total.
#[test]
#[ignore]
fn window_sum_order_by_includes_peers() -> TestResult {
    assert_query(
        &[employees_csv()],
        "SELECT id, SUM(salary) OVER (ORDER BY department) AS s FROM employees",
        "id,s\n1,210000\n3,210000\n6,210000\n8,255000\n2,370000\n5,370000\n4,540000\n7,540000",
    )
}

/// Two window functions in one SELECT: the second evaluates to 0.
#[test]
#[ignore]
fn window_two_functions_in_one_select() -> TestResult {
    assert_query(
        &[employees_csv()],
        "SELECT id, RANK() OVER (ORDER BY department) AS rk, DENSE_RANK() OVER (ORDER BY department) AS dr FROM employees",
        "id,rk,dr\n1,1,1\n3,1,1\n6,1,1\n8,4,2\n2,5,3\n5,5,3\n4,7,4\n7,7,4",
    )
}

/// A correlated scalar subquery in the SELECT list is rejected ("Only column
/// references supported in SELECT") once ORDER BY / LIMIT is added.
#[test]
fn correlated_scalar_with_order_by_limit() -> TestResult {
    assert_query(
        &[employees_csv()],
        "SELECT e.name AS n, (SELECT COUNT(*) FROM employees e2 WHERE e2.department = e.department) AS c FROM employees e ORDER BY e.name LIMIT 3",
        "n,c\nAlice,3\nBob,2\nCharlie,3",
    )
}

/// In a correlated scalar subquery, `MAX(e2.salary)` / `AVG(e2.salary)`
/// aggregates column 0 (id) instead of salary: the qualified argument is not
/// resolved against the inner alias.
#[test]
fn correlated_scalar_aggregate_of_qualified_column() -> TestResult {
    assert_query(
        &[employees_csv()],
        "SELECT e.name AS n, (SELECT MAX(e2.salary) FROM employees e2 WHERE e2.department = e.department) AS m FROM employees e WHERE e.id < 4",
        "n,m\nAlice,75000\nBob,60000\nCharlie,75000",
    )
}

/// A correlated reference through the unaliased outer table name
/// (`employees.department`) never matches, giving 0.
#[test]
fn correlated_scalar_unaliased_outer_table() -> TestResult {
    assert_query(
        &[employees_csv()],
        "SELECT name, (SELECT COUNT(*) FROM employees e2 WHERE e2.department = employees.department) AS c FROM employees WHERE id < 4",
        "name,c\nAlice,3\nBob,2\nCharlie,3",
    )
}

/// `DELETE t FROM t JOIN u ...` (multi-table DELETE) is accepted and deletes
/// every row of `users`; must be rejected.
#[test]
#[ignore]
fn delete_with_join_is_error() -> TestResult {
    assert_query_fails(
        &[users_csv(), orders_csv()],
        "DELETE users FROM users JOIN orders ON users.id = orders.user_id",
        "Unsupported",
    )
}

/// GROUP BY over zero rows fails with "No current row in sorter 0"; must
/// return the header only.
#[test]
fn group_by_over_zero_rows() -> TestResult {
    assert_query(
        &[employees_csv()],
        "SELECT department, COUNT(*) AS c FROM employees WHERE salary > 1000000 GROUP BY department",
        "department,c",
    )
}

/// Same root cause for ORDER BY over zero rows.
#[test]
fn order_by_over_zero_rows() -> TestResult {
    assert_query(
        &[employees_csv()],
        "SELECT name FROM employees WHERE salary > 1000000 ORDER BY name",
        "name",
    )
}

// ---------------------------------------------------------------------------
// Group 3: Accepted-but-ignored clauses (must error)
// ---------------------------------------------------------------------------

/// `FILTER (WHERE ...)` on an aggregate is parsed and silently ignored
/// (returns 8, not 4).
#[test]
#[ignore]
fn aggregate_filter_clause_is_error() -> TestResult {
    assert_query_fails(
        &[employees_csv()],
        "SELECT COUNT(*) FILTER (WHERE age > 30) AS c FROM employees",
        "Unsupported",
    )
}

/// `NULLS FIRST/LAST` in ORDER BY is silently ignored.
#[test]
#[ignore]
fn order_by_nulls_last_is_error() -> TestResult {
    assert_query_fails(
        &[nullable_csv()],
        "SELECT id FROM nullable ORDER BY score NULLS LAST",
        "Unsupported",
    )
}

/// `INSERT ... RETURNING` performs the insert and emits nothing.
#[test]
#[ignore]
fn insert_returning_is_error() -> TestResult {
    assert_query_fails(
        &[users_csv()],
        "INSERT INTO users VALUES (3, 'Zed', 'z@x') RETURNING id",
        "Unsupported",
    )
}

/// `INSERT ... ON CONFLICT DO NOTHING` is silently ignored.
#[test]
#[ignore]
fn insert_on_conflict_is_error() -> TestResult {
    assert_query_fails(
        &[users_csv()],
        "INSERT INTO users VALUES (1, 'Zed', 'z@x') ON CONFLICT DO NOTHING",
        "Unsupported",
    )
}

/// `INSERT OR REPLACE` is treated as a plain INSERT.
#[test]
#[ignore]
fn insert_or_replace_is_error() -> TestResult {
    assert_query_fails(
        &[users_csv()],
        "INSERT OR REPLACE INTO users VALUES (1, 'Zed', 'z@x')",
        "Unsupported",
    )
}

/// `UPDATE ... FROM` silently ignores the FROM clause.
#[test]
#[ignore]
fn update_from_is_error() -> TestResult {
    assert_query_fails(
        &[users_csv(), orders_csv()],
        "UPDATE users SET name = 'X' FROM orders WHERE users.id = 1",
        "Unsupported",
    )
}

/// `DELETE ... USING` silently ignores the USING clause.
#[test]
#[ignore]
fn delete_using_is_error() -> TestResult {
    assert_query_fails(
        &[users_csv(), orders_csv()],
        "DELETE FROM users USING orders WHERE users.id = 1",
        "Unsupported",
    )
}

/// NTILE is accepted and computes ROW_NUMBER.
#[test]
#[ignore]
fn ntile_is_error() -> TestResult {
    assert_query_fails(
        &[employees_csv()],
        "SELECT id, NTILE(2) OVER (ORDER BY id) AS t FROM employees",
        "Unsupported",
    )
}

/// FIRST_VALUE is accepted and computes ROW_NUMBER.
#[test]
#[ignore]
fn first_value_is_error() -> TestResult {
    assert_query_fails(
        &[employees_csv()],
        "SELECT id, FIRST_VALUE(name) OVER (ORDER BY id) AS f FROM employees",
        "Unsupported",
    )
}

/// LAST_VALUE is accepted and computes ROW_NUMBER.
#[test]
#[ignore]
fn last_value_is_error() -> TestResult {
    assert_query_fails(
        &[employees_csv()],
        "SELECT id, LAST_VALUE(name) OVER (ORDER BY id) AS f FROM employees",
        "Unsupported",
    )
}

/// PERCENT_RANK is accepted and computes ROW_NUMBER.
#[test]
#[ignore]
fn percent_rank_is_error() -> TestResult {
    assert_query_fails(
        &[employees_csv()],
        "SELECT id, PERCENT_RANK() OVER (ORDER BY id) AS f FROM employees",
        "Unsupported",
    )
}

// ---------------------------------------------------------------------------
// Group 4: Semantics (SQLite-compatible)
// ---------------------------------------------------------------------------

/// Integer division and modulo: `5 / 2` is 2 and `-7 / 2` is -3 (truncating),
/// not 2.5 / -3.5 (Divide always produces a float in engine.rs).
#[test]
#[ignore]
fn integer_division_truncates() -> TestResult {
    assert_query(
        &[users_csv()],
        "SELECT 5 / 2 AS a, 7 % 3 AS b, -7 / 2 AS c FROM users WHERE id = 1",
        "a,b,c\n2,1,-3",
    )
}

/// Same root cause, observed through WHERE.
#[test]
#[ignore]
fn integer_division_in_where() -> TestResult {
    assert_query(
        &[employees_csv()],
        "SELECT name FROM employees WHERE id = 1 AND 7 % 3 = 1 AND 5 / 2 = 2",
        "name\nAlice",
    )
}

/// i64 overflow saturates at i64::MAX; SQLite promotes to REAL, so the sum
/// compares greater than i64::MAX.
#[test]
#[ignore]
fn integer_overflow_promotes_to_float() -> TestResult {
    assert_query(
        &[users_csv()],
        "SELECT id FROM users WHERE 9223372036854775807 + 1 > 9223372036854775807",
        "id\n1\n2",
    )
}

/// Integer arithmetic goes through f64, losing precision above 2^53.
#[test]
#[ignore]
fn large_integer_arithmetic_is_exact() -> TestResult {
    assert_query(
        &[users_csv()],
        "SELECT 9007199254740993 + 0 AS a FROM users WHERE id = 1",
        "a\n9007199254740993",
    )
}

/// CAST(text AS INTEGER) yields 0 unless the whole string is an integer;
/// SQLite takes the longest numeric prefix ('3.9' -> 3, '12abc' -> 12).
#[test]
#[ignore]
fn cast_text_to_integer_uses_numeric_prefix() -> TestResult {
    assert_query(
        &[users_csv()],
        "SELECT CAST('3.9' AS INTEGER) AS a, CAST('12abc' AS INTEGER) AS b FROM users WHERE id = 1",
        "a,b\n3,12",
    )
}

/// A float literal inserted into a column is not summed as a number
/// (SUM gives 7, not 9.5).
#[test]
fn inserted_float_literal_is_numeric() -> TestResult {
    assert_query(
        &[orders_csv()],
        "INSERT INTO orders VALUES (106, 2.5, 1, 'x'); SELECT SUM(user_id) AS s FROM orders",
        "s\n9.5",
    )
}

/// LIKE is case-sensitive; SQLite's LIKE is ASCII case-insensitive.
#[test]
#[ignore]
fn like_is_ascii_case_insensitive() -> TestResult {
    assert_query(
        &[employees_csv()],
        "SELECT name FROM employees WHERE name LIKE 'a%'",
        "name\nAlice",
    )
}

/// LIKE ... ESCAPE is ignored, so an escaped `%` never matches literally.
#[test]
#[ignore]
fn like_escape_percent() -> TestResult {
    assert_query_on(
        &[("s.csv", "k,s\n1,x\n4,x%y\n5,A_b\n")],
        "SELECT k FROM s WHERE s LIKE 'x\\%y' ESCAPE '\\'",
        "k\n4",
    )
}

/// Same root cause for an escaped `_`.
#[test]
#[ignore]
fn like_escape_underscore() -> TestResult {
    assert_query_on(
        &[("s.csv", "k,s\n1,x\n4,x%y\n5,A_b\n")],
        "SELECT k FROM s WHERE s LIKE '%\\_%' ESCAPE '\\'",
        "k\n5",
    )
}

/// `x || NULL` returns `x`; it must be NULL.
#[test]
#[ignore]
fn concat_with_null_is_null() -> TestResult {
    assert_query(
        &[users_csv()],
        "SELECT LENGTH(name || NULL) IS NULL AS a FROM users",
        "a\n1\n1",
    )
}

/// LENGTH counts bytes, not characters.
#[test]
#[ignore]
fn length_counts_characters() -> TestResult {
    assert_query(
        &[users_csv()],
        "SELECT LENGTH('日本語') AS a FROM users WHERE id = 1",
        "a\n3",
    )
}

/// SUBSTR with a negative start returns '' (must count from the end) and a
/// zero start does not shrink the length as SQLite does.
#[test]
#[ignore]
fn substr_negative_and_zero_start() -> TestResult {
    assert_query(
        &[users_csv()],
        "SELECT SUBSTR('hello', -3) AS a, SUBSTR('hello', 0, 2) AS b FROM users WHERE id = 1",
        "a,b\nllo,h",
    )
}

/// CURRENT_DATE returns the wrong date (a hand-rolled epoch-to-civil
/// conversion in the date functions); must equal today's UTC date.
#[test]
#[ignore]
fn current_date_is_today() -> TestResult {
    assert_query(
        &[users_csv()],
        "SELECT CURRENT_DATE AS d FROM users WHERE id = 1",
        &format!("d\n{}", today_utc()),
    )
}

/// SUM/AVG/MIN/MAX over zero rows return 0; they must return NULL (rendered
/// as an empty field). COUNT stays 0.
#[test]
fn empty_set_aggregates_are_null() -> TestResult {
    assert_query(
        &[employees_csv()],
        "SELECT COUNT(*) AS c, SUM(salary) AS s, AVG(salary) AS a, MIN(salary) AS mn, MAX(salary) AS mx FROM employees WHERE id > 100",
        "c,s,a,mn,mx\n0,,,,",
    )
}

// ---------------------------------------------------------------------------
// Group 5: Features
// ---------------------------------------------------------------------------

/// JOIN ... USING is rejected ("Only ON join constraints supported").
#[test]
fn join_using() -> TestResult {
    assert_query_on(
        &[("lt.csv", LT), ("rt.csv", RT)],
        "SELECT k, a, b FROM lt JOIN rt USING (k)",
        "k,a,b\n1,10,100\n3,30,300",
    )
}

/// NATURAL JOIN is rejected.
#[test]
fn natural_join() -> TestResult {
    assert_query_on(
        &[("lt.csv", LT), ("rt.csv", RT)],
        "SELECT * FROM lt NATURAL JOIN rt",
        "k,a,b\n1,10,100\n3,30,300",
    )
}

/// Two LEFT JOINs in one query are rejected ("Only INNER JOIN with ON clause
/// supported in multi-table joins").
#[test]
fn two_left_joins() -> TestResult {
    assert_query_on(
        &[("lt.csv", LT), ("rt.csv", RT), ("xt.csv", XT)],
        "SELECT lt.k AS k, rt.b AS b, xt.c AS c FROM lt LEFT JOIN rt ON lt.k = rt.k LEFT JOIN xt ON lt.k = xt.k WHERE lt.k = 1",
        "k,b,c\n1,100,one",
    )
}

/// Arithmetic in a JOIN SELECT list is rejected ("Unsupported expression in
/// JOIN projection").
#[test]
fn join_select_arithmetic() -> TestResult {
    assert_query(
        &[users_csv(), orders_csv()],
        "SELECT u.id AS uid, o.product_id * 2 AS p FROM users u JOIN orders o ON u.id = o.user_id WHERE o.id = 104",
        "uid,p\n2,210",
    )
}

/// A scalar function in a JOIN SELECT list is rejected.
#[test]
fn join_select_function() -> TestResult {
    assert_query(
        &[users_csv(), orders_csv()],
        "SELECT u.id AS uid, UPPER(u.name) AS n FROM users u JOIN orders o ON u.id = o.user_id WHERE o.id = 104",
        "uid,n\n2,JANE",
    )
}

/// An aggregate in the SELECT list of an outer join with GROUP BY is rejected.
#[test]
fn outer_join_aggregate_group_by() -> TestResult {
    assert_query(
        &[users_csv(), orders_csv()],
        "SELECT u.name AS n, COUNT(o.id) AS c FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.name",
        "n,c\nJane,2\nJohn,3",
    )
}

/// A scalar function over an aggregate (`ROUND(AVG(x), 1)`) is rejected with
/// GROUP BY ("must appear in GROUP BY"). Values follow sqawk's float
/// formatting (SQLite prints 70000.0).
#[test]
fn round_of_avg_with_group_by() -> TestResult {
    assert_query(
        &[employees_csv()],
        "SELECT department, ROUND(AVG(salary), 1) AS a FROM employees GROUP BY department",
        "department,a\nEngineering,70000\nHR,45000\nMarketing,57500\nSales,85000",
    )
}

/// Arithmetic over an aggregate (`COUNT(*) * 10`) is rejected with GROUP BY.
#[test]
fn aggregate_arithmetic_with_group_by() -> TestResult {
    assert_query(
        &[employees_csv()],
        "SELECT department, COUNT(*) * 10 AS c FROM employees GROUP BY department",
        "department,c\nEngineering,30\nHR,10\nMarketing,20\nSales,20",
    )
}

/// An aggregate over CASE is rejected with GROUP BY.
#[test]
fn sum_of_case_with_group_by() -> TestResult {
    assert_query(
        &[employees_csv()],
        "SELECT department, SUM(CASE WHEN age > 30 THEN 1 ELSE 0 END) AS c FROM employees GROUP BY department",
        "department,c\nEngineering,2\nHR,0\nMarketing,0\nSales,2",
    )
}

/// GROUP BY an expression is rejected ("Only column references supported in
/// GROUP BY").
#[test]
fn group_by_function_expression() -> TestResult {
    assert_query(
        &[employees_csv()],
        "SELECT UPPER(department) AS d, COUNT(*) AS c FROM employees GROUP BY UPPER(department)",
        "d,c\nENGINEERING,3\nHR,1\nMARKETING,2\nSALES,2",
    )
}

/// Same, grouping by a numeric expression.
#[test]
fn group_by_length_expression() -> TestResult {
    assert_query(
        &[employees_csv()],
        "SELECT LENGTH(name) AS l, COUNT(*) AS c FROM employees GROUP BY LENGTH(name)",
        "l,c\n3,2\n5,5\n7,1",
    )
}

/// GROUP BY ordinal (`GROUP BY 1`) is rejected.
#[test]
fn group_by_ordinal() -> TestResult {
    assert_query(
        &[employees_csv()],
        "SELECT department, COUNT(*) AS c FROM employees GROUP BY 1",
        "department,c\nEngineering,3\nHR,1\nMarketing,2\nSales,2",
    )
}

/// ORDER BY the alias of a computed column is rejected ("Only column
/// references supported in SELECT").
#[test]
fn order_by_expression_alias() -> TestResult {
    assert_query(
        &[employees_csv()],
        "SELECT name, salary * 2 AS dbl FROM employees ORDER BY dbl DESC LIMIT 3",
        "name,dbl\nGrace,180000\nDavid,160000\nFrank,150000",
    )
}

/// HAVING with AND of two aggregates is rejected.
#[test]
fn having_with_and() -> TestResult {
    assert_query(
        &[employees_csv()],
        "SELECT department, COUNT(*) AS c FROM employees GROUP BY department HAVING COUNT(*) >= 2 AND MIN(salary) > 56000",
        "department,c\nEngineering,3\nSales,2",
    )
}

/// HAVING referencing a SELECT alias is rejected ("Column 'c' not found").
#[test]
fn having_with_alias() -> TestResult {
    assert_query(
        &[employees_csv()],
        "SELECT department, COUNT(*) AS c FROM employees GROUP BY department HAVING c > 1",
        "department,c\nEngineering,3\nMarketing,2\nSales,2",
    )
}

/// HAVING aggregate IS NULL is rejected.
#[test]
fn having_aggregate_is_null() -> TestResult {
    assert_query(
        &[nullable_csv()],
        "SELECT grade FROM nullable GROUP BY grade HAVING SUM(score) IS NULL",
        "grade\nB",
    )
}

/// HAVING an aggregate that is not in the SELECT list is rejected.
#[test]
fn having_aggregate_not_in_select() -> TestResult {
    assert_query(
        &[employees_csv()],
        "SELECT department FROM employees GROUP BY department HAVING MAX(age) > 40",
        "department\nSales",
    )
}

/// GROUP_CONCAT is unsupported. Output is RFC 4180 quoted because the value
/// contains the delimiter.
#[test]
#[ignore]
fn group_concat() -> TestResult {
    assert_query(
        &[employees_csv()],
        "SELECT GROUP_CONCAT(name) AS g FROM employees WHERE id < 4",
        "g\n\"Alice,Bob,Charlie\"",
    )
}

/// IFNULL is unsupported.
#[test]
#[ignore]
fn ifnull_function() -> TestResult {
    assert_query(
        &[nullable_csv()],
        "SELECT id, IFNULL(score, 0) AS s FROM nullable",
        "id,s\n1,90\n2,0\n3,70\n4,50\n5,90",
    )
}

/// IIF is unsupported.
#[test]
#[ignore]
fn iif_function() -> TestResult {
    assert_query(
        &[nullable_csv()],
        "SELECT id, IIF(score > 60, 'y', 'n') AS s FROM nullable WHERE id < 4",
        "id,s\n1,y\n2,n\n3,y",
    )
}

/// INSTR is unsupported.
#[test]
#[ignore]
fn instr_function() -> TestResult {
    assert_query(
        &[employees_csv()],
        "SELECT id, INSTR(name, 'li') AS s FROM employees WHERE id < 4",
        "id,s\n1,2\n2,0\n3,5",
    )
}

/// TYPEOF is unsupported.
#[test]
#[ignore]
fn typeof_function() -> TestResult {
    assert_query(
        &[employees_csv()],
        "SELECT TYPEOF(id) AS a, TYPEOF(name) AS b, TYPEOF(1.5) AS c FROM employees WHERE id = 1",
        "a,b,c\ninteger,text,real",
    )
}

/// LPAD/RPAD are documented in doc/sql_reference.md ("Left-pad to length",
/// "Right-pad to length") but unsupported. SQLite lacks them; expected values
/// follow the documented semantics.
#[test]
#[ignore]
fn lpad_rpad_functions() -> TestResult {
    assert_query(
        &[employees_csv()],
        "SELECT LPAD(name, 6, '*') AS a, RPAD(name, 6, '*') AS b FROM employees WHERE id < 3",
        "a,b\n*Alice,Alice*\n***Bob,Bob***",
    )
}

/// Arithmetic combining two window functions is rejected ("Unsupported
/// function: SUM").
#[test]
#[ignore]
fn window_function_arithmetic() -> TestResult {
    assert_query(
        &[employees_csv()],
        "SELECT id, SUM(age) OVER (ORDER BY id) + SUM(salary) OVER (ORDER BY id) AS s FROM employees WHERE id < 4",
        "id,s\n1,70030\n2,125055\n3,190090",
    )
}
