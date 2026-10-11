//! Red list: the findings of the 2026-10 v1-readiness review.
//!
//! Every test in this module encodes **correct** behaviour for a finding of
//! that review (a 4,923-query adversarial diff against SQLite plus a
//! release-readiness audit of IO, CLI and REPL behaviour). Each failing test
//! is `#[ignore]`d until its defect is fixed; remove the attribute then.
//! Run the red list with:
//!     cargo test --test mod defects -- --ignored
//!
//! A few tests are NOT ignored: they pass today and pin behaviour that the
//! fixes must keep (each says so in its doc comment).
//!
//! Expected outputs were computed with SQLite 3.45 (python3 `sqlite3`) unless
//! the doc comment says otherwise. Output NULL is rendered as an EMPTY field.
//! Where sqawk deliberately differs from SQLite the doc comment names the
//! rule being encoded:
//!
//! - numeric-looking text compares numerically everywhere (`=`, `<`, IN list,
//!   IN subquery, JOIN, ORDER BY, MIN/MAX), and numbers sort before
//!   non-numeric text; DISTINCT / GROUP BY keep `'1'` and `1` apart;
//! - a REAL is rendered as SQLite's `CAST(x AS TEXT)` renders it (`%.15g`,
//!   with `.0` on integral values, `Inf` / `-Inf`);
//! - `.tsv` / `.tab` files are tab-separated and every other file is
//!   comma-separated unless -F is given; the first line is always the header
//!   unless `--no-header` is given; `#` lines are data unless `--comments` is
//!   given;
//! - every error is one line `sqawk: <message>` on stderr with exit status 1.
//!
//! Root-cause locations are hints as of the review (file:line), not contracts.

use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

use crate::helpers::{assert_query_on, assert_query_on_fails, create_temp_dir, run_sql, ONE};

type TestResult = Result<(), Box<dyn std::error::Error>>;

// ---------------------------------------------------------------------------
// Local helpers
// ---------------------------------------------------------------------------

/// `d(k, n, s)`: three rows, all keys distinct.
const D: (&str, &str) = ("d.csv", "k,n,s\n1,10,x\n2,20,y\n3,30,z\n");
/// `a(aid)` and `b(aid, v)` for correlated subqueries.
const A: (&str, &str) = ("a.csv", "aid\n1\n2\n");
const B: (&str, &str) = ("b.csv", "aid,v\n1,5\n1,7\n2,9\n");
/// `tx(k, n)` for transaction tests.
const TX: &str = "k,n\n1,10\n2,20\n";

/// `assert_query_on` over the one-row table `one`.
fn one(sql: &str, expected: &str) -> TestResult {
    assert_query_on(&[ONE], sql, expected)
}

fn sqawk_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_sqawk"))
}

/// Run the sqawk binary with `args` in `cwd`, feeding `stdin` (empty when
/// `None`), with HOME and XDG_STATE_HOME pointed at `cwd` so nothing touches
/// the real home directory.
fn run_in(cwd: &Path, args: &[&str], stdin: Option<&[u8]>) -> std::io::Result<Output> {
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
        if let Some(input) = stdin {
            si.write_all(input)?;
        }
    }
    child.wait_with_output()
}

/// Write `(name, bytes)` files into `dir`, returning their absolute paths.
fn write_files(dir: &Path, files: &[(&str, &[u8])]) -> std::io::Result<Vec<String>> {
    let mut out = Vec::new();
    for (name, content) in files {
        let p = dir.join(name);
        if let Some(parent) = p.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&p, content)?;
        out.push(p.to_str().unwrap().to_string());
    }
    Ok(out)
}

/// Write `content` to temp file `file_name`, run the `-s` statements with
/// `extra` args plus `--write`, require success, and return the file bytes.
fn write_back(
    file_name: &str,
    content: &[u8],
    extra: &[&str],
    sqls: &[&str],
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let dir = create_temp_dir()?;
    let p = write_files(dir.path(), &[(file_name, content)])?;
    let mut args: Vec<&str> = extra.to_vec();
    args.push("--write");
    for s in sqls {
        args.push("-s");
        args.push(s);
    }
    args.push(&p[0]);
    let out = run_in(dir.path(), &args, None)?;
    assert!(
        out.status.success(),
        "write run failed\n  sql: {sqls:?}\n  stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    Ok(fs::read(&p[0])?)
}

/// Assert that a `--write` run leaves the file byte-for-byte equal to `expected`.
fn assert_write_back(
    file_name: &str,
    content: &[u8],
    extra: &[&str],
    sqls: &[&str],
    expected: &[u8],
) -> TestResult {
    let actual = write_back(file_name, content, extra, sqls)?;
    assert_eq!(
        String::from_utf8_lossy(&actual),
        String::from_utf8_lossy(expected),
        "\n=== sql ===\n{sqls:?}\n=== input ===\n{:?}\n",
        String::from_utf8_lossy(content)
    );
    assert_eq!(actual, expected, "bytes differ (invalid UTF-8 region)");
    Ok(())
}

/// Run `sqls` with `--write` over `file_name` and require that the run FAILS
/// and leaves the file byte-identical.
fn assert_write_refused(file_name: &str, content: &[u8], sqls: &[&str]) -> TestResult {
    let dir = create_temp_dir()?;
    let p = write_files(dir.path(), &[(file_name, content)])?;
    let mut args = vec!["--write"];
    for s in sqls {
        args.push("-s");
        args.push(s);
    }
    args.push(&p[0]);
    let out = run_in(dir.path(), &args, None)?;
    assert!(
        !out.status.success(),
        "run unexpectedly succeeded\n  sql: {sqls:?}"
    );
    assert_eq!(fs::read(&p[0])?, content, "file changed\n  sql: {sqls:?}");
    Ok(())
}

/// The CLI error contract: exit status 1, stderr is exactly one line starting
/// `sqawk: `, no "Caused by" chain, no panic.
fn assert_clean_error(out: &Output, must_contain: &[&str]) {
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(1), "exit status; stderr:\n{stderr}");
    assert!(!stderr.contains("panicked"), "panicked:\n{stderr}");
    assert!(
        !stderr.contains("Caused by"),
        "error chain printed:\n{stderr}"
    );
    assert_eq!(
        stderr.lines().count(),
        1,
        "stderr is not one line:\n{stderr}"
    );
    assert!(
        stderr.starts_with("sqawk: "),
        "stderr lacks the `sqawk: ` prefix:\n{stderr}"
    );
    for s in must_contain {
        assert!(stderr.contains(s), "stderr lacks {s:?}:\n{stderr}");
    }
}

/// Run sqawk with `-s` statements over `(name, content)` files in a temp dir.
fn run_sqls(
    files: &[(&str, &str)],
    extra: &[&str],
    sqls: &[&str],
) -> Result<Output, Box<dyn std::error::Error>> {
    let dir = create_temp_dir()?;
    let bytes: Vec<(&str, &[u8])> = files.iter().map(|(n, c)| (*n, c.as_bytes())).collect();
    let paths = write_files(dir.path(), &bytes)?;
    let mut args: Vec<&str> = extra.to_vec();
    for s in sqls {
        args.push("-s");
        args.push(s);
    }
    for p in &paths {
        args.push(p);
    }
    Ok(run_in(dir.path(), &args, None)?)
}

/// Run the `-s` statements over `files` and require exactly `expected` on
/// stdout (byte-exact apart from a trailing newline).
fn assert_sqls(
    files: &[(&str, &str)],
    extra: &[&str],
    sqls: &[&str],
    expected: &str,
) -> TestResult {
    let out = run_sqls(files, extra, sqls)?;
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "run failed\n  sql: {sqls:?}\n  stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        stdout.trim_end_matches('\n'),
        expected,
        "\n=== sql ===\n{sqls:?}\n"
    );
    Ok(())
}

/// Require that the run fails without crashing: nonzero exit status that is
/// not a signal, no panic, no stack overflow, a non-empty message.
fn assert_fails_cleanly(out: &Output) {
    let stderr = String::from_utf8_lossy(&out.stderr);
    let code = out.status.code();
    assert!(
        matches!(code, Some(c) if c != 0 && c != 101),
        "exit status {:?}: expected a clean failure (nonzero, not a signal or panic)\nstderr: {}",
        out.status,
        &stderr[..stderr.len().min(500)]
    );
    assert!(!stderr.contains("panicked"), "panicked");
    assert!(!stderr.contains("overflow"), "stack overflow");
    assert!(!stderr.trim().is_empty(), "no error message");
}

/// True when file permissions are enforced for this user (false for root).
fn perms_enforced(dir: &Path) -> bool {
    let p = dir.join(".perm_probe");
    fs::write(&p, "x").unwrap();
    let mut perm = fs::metadata(&p).unwrap().permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut perm, 0o000);
    fs::set_permissions(&p, perm).unwrap();
    let enforced = fs::File::open(&p).is_err();
    fs::remove_file(&p).unwrap();
    enforced
}

fn chmod(p: &Path, mode: u32) {
    let mut perm = fs::metadata(p).unwrap().permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut perm, mode);
    fs::set_permissions(p, perm).unwrap();
}

/// Generate `SELECT t1 <sep> t2 ... AS v FROM one` with `n` terms.
fn chain(n: usize, sep: &str, term: &str) -> String {
    format!("SELECT {} AS v FROM one", vec![term; n].join(sep))
}

/// Run `sql` over the one-row table and return the process output.
fn run_one(sql: &str) -> Result<Output, Box<dyn std::error::Error>> {
    run_sqls(&[ONE], &[], &[sql])
}

/// Run a piped (non-tty) REPL over `files` in a temp dir with `script` on
/// stdin. Returns the output and the temp dir (for inspecting files).
fn run_repl(
    files: &[(&str, &str)],
    script: &str,
) -> Result<(Output, tempfile::TempDir), Box<dyn std::error::Error>> {
    let dir = create_temp_dir()?;
    let bytes: Vec<(&str, &[u8])> = files.iter().map(|(n, c)| (*n, c.as_bytes())).collect();
    write_files(dir.path(), &bytes)?;
    let mut args = vec!["-i"];
    for (n, _) in files {
        args.push(n);
    }
    let out = run_in(dir.path(), &args, Some(script.as_bytes()))?;
    Ok((out, dir))
}

/// Table names listed by `.tables <pattern>` in a piped REPL.
fn repl_tables(pattern: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let files = [
        ("emp.csv", "a\n1\n"),
        ("exa.csv", "a\n1\n"),
        ("Ebig.csv", "a\n1\n"),
        ("rw.csv", "a\n1\n"),
    ];
    let (out, _dir) = run_repl(&files, &format!(".tables {pattern}\n"))?;
    let stdout = String::from_utf8_lossy(&out.stdout);
    let names = ["emp", "exa", "Ebig", "rw"];
    let mut listed: Vec<String> = stdout
        .lines()
        .map(str::trim)
        .filter(|l| names.contains(l))
        .map(String::from)
        .collect();
    listed.sort();
    Ok(listed)
}

// ===========================================================================
// A. Semantics
// ===========================================================================

// ---------------------------------------------------------------------------
// A1. Name resolution: unknown qualifiers silently resolve
// (`NameCtx::lookup` / `resolve_qualified`, compiler.rs ~321/~342: a
// single-source query accepts any qualifier)
// ---------------------------------------------------------------------------

/// `zz` names no table in FROM, yet `zz.k` resolves to `d.k`.
#[test]
#[ignore]
fn unknown_qualifier_is_error() -> TestResult {
    assert_query_on_fails(&[D], "SELECT zz.k AS v FROM d", "no such column: zz.k")
}

/// An UPDATE whose WHERE names an unknown qualifier updates rows and writes
/// them back. It must fail and leave the file unchanged.
#[test]
#[ignore]
fn unknown_qualifier_in_update_is_error() -> TestResult {
    assert_write_refused(
        "d.csv",
        D.1.as_bytes(),
        &["UPDATE d SET n = 0 WHERE zz.k = 1"],
    )
}

/// `zz.*` with no table `zz` expands to `d.*` (star expansion, compiler.rs ~897).
#[test]
#[ignore]
fn unknown_qualifier_star_is_error() -> TestResult {
    assert_query_on_fails(&[D], "SELECT zz.* FROM d", "zz")
}

/// A derived table that references an outer column is materialized once,
/// without the correlation (`materialize_in_factor`, vm/mod.rs ~409), so every
/// row gets max(v) over all of `b` (9, 9). It must be rejected.
#[test]
#[ignore]
fn correlated_derived_table_is_error() -> TestResult {
    let out = run_sqls(
        &[A, B],
        &[],
        &["SELECT aid, (SELECT max(v) FROM (SELECT b.v FROM b WHERE b.aid = a.aid) q) AS m FROM a"],
    )?;
    assert_fails_cleanly(&out);
    Ok(())
}

/// Guard (passes today): an alias on the UPDATE target resolves. The
/// qualifier fix must keep it working (pass the alias into `Src.refname`,
/// compiler_dml.rs ~210).
#[test]
fn aliased_update_target_resolves() -> TestResult {
    assert_write_back(
        "d.csv",
        D.1.as_bytes(),
        &[],
        &["UPDATE d AS x SET n = 1 WHERE x.k = 1"],
        b"k,n,s\n1,1,x\n2,20,y\n3,30,z\n",
    )
}

/// A schema qualifier other than `main` is silently ignored: `foo.d` reads `d`.
#[test]
#[ignore]
fn unknown_schema_is_error() -> TestResult {
    assert_query_on_fails(&[D], "SELECT * FROM foo.d", "foo")
}

/// `UPDATE foo.d` updates `d` and writes it back. It must fail, file unchanged.
#[test]
#[ignore]
fn unknown_schema_in_update_is_error() -> TestResult {
    assert_write_refused("d.csv", D.1.as_bytes(), &["UPDATE foo.d SET n = 5"])
}

/// Guard (passes today): SQLite's `main` schema qualifier is accepted.
#[test]
fn main_schema_qualifier_works() -> TestResult {
    assert_query_on(&[D], "SELECT k FROM main.d AS x WHERE x.k = 1", "k\n1")
}

// ---------------------------------------------------------------------------
// A2. Operator precedence differs from SQLite (Hive dialect precedence in
// sqlparser; plan: SqawkDialect with SQLite levels, compounds left-deep)
// ---------------------------------------------------------------------------

/// `IS NOT DISTINCT FROM` parses its RHS as `0 AND 0`; SQLite binds it at the
/// `=` level: `(0 IS NOT DISTINCT FROM 0) AND 0` = 0.
#[test]
fn is_not_distinct_from_binds_tighter_than_and() -> TestResult {
    one(
        "SELECT 0 IS NOT DISTINCT FROM 0 AND 0 AS v FROM one",
        "v\n0",
    )
}

/// Same defect in a WHERE: parsed as `k IS NOT DISTINCT FROM (2 AND n > 0)`,
/// which selects k = 1 instead of k = 2.
#[test]
fn is_not_distinct_from_in_where() -> TestResult {
    assert_query_on(
        &[D],
        "SELECT k FROM d WHERE k IS NOT DISTINCT FROM 2 AND n > 0",
        "k\n2",
    )
}

/// LIKE and `=` share one precedence level in SQLite (left-to-right):
/// `('abc' LIKE 'A%') = 1` = 1.
#[test]
fn like_and_equals_same_level() -> TestResult {
    one("SELECT 'abc' LIKE 'A%' = 1 AS v FROM one", "v\n1")
}

/// `<` binds tighter than `=` in SQLite: `2 = (1 < 2)` = 0.
#[test]
fn less_than_binds_tighter_than_equals() -> TestResult {
    one("SELECT 2 = 1 < 2 AS v FROM one", "v\n0")
}

/// `||` binds tighter than `*` in SQLite: `2 * (3 || 4)` = 68.
#[test]
fn concat_binds_tighter_than_multiply() -> TestResult {
    one("SELECT 2 * 3 || 4 AS v FROM one", "v\n68")
}

/// Compound operators have equal precedence, left to right, in SQLite:
/// `(1 UNION ALL 2) INTERSECT 2` = {2}. sqawk binds INTERSECT tighter.
#[test]
fn compound_operators_left_to_right() -> TestResult {
    one(
        "SELECT 1 AS v UNION ALL SELECT 2 INTERSECT SELECT 2",
        "v\n2",
    )
}

/// Guard (passes today): `(1 INTERSECT 1) UNION 2` = {1, 2}.
#[test]
fn compound_intersect_then_union() -> TestResult {
    one("SELECT 1 AS v INTERSECT SELECT 1 UNION SELECT 2", "v\n1\n2")
}

// ---------------------------------------------------------------------------
// A3. Comparison rule unification. Rule (deliberate difference from SQLite):
// numeric-looking text compares numerically everywhere; numbers sort before
// non-numeric text. (IN-subquery set: compiler_subquery.rs ~136, engine
// `Found`/`row_key`; `sql_compare` engine ~2621; `compare_values` ~500)
// ---------------------------------------------------------------------------

/// `'9' IN (9)` and `'9' = (SELECT 9)` are 1 but `'9' IN (SELECT 9)` is 0:
/// the IN-subquery set compares by exact key.
#[test]
fn in_subquery_numeric_text_matches_number() -> TestResult {
    one(
        "SELECT '9' IN (SELECT 9) AS a, '9' IN (9) AS b, '9' = (SELECT 9) AS c FROM one",
        "a,b,c\n1,1,1",
    )
}

/// An IN subquery must match the same rows as the equivalent JOIN. `tk.k`
/// holds numeric-looking text (`007`, `9.0`, `1e1`); the JOIN matches all
/// four rows, the IN subquery only 8.
#[test]
fn in_subquery_agrees_with_join() -> TestResult {
    let files = [
        ("na.csv", "n\n7\n8\n9\n10\n"),
        ("tk.csv", "k\n007\n8\n9.0\n1e1\n"),
    ];
    assert_query_on(
        &files,
        "SELECT na.n AS n FROM na JOIN tk ON na.n = tk.k ORDER BY 1",
        "n\n7\n8\n9\n10",
    )?;
    assert_query_on(
        &files,
        "SELECT n FROM na WHERE n IN (SELECT k FROM tk) ORDER BY n",
        "n\n7\n8\n9\n10",
    )
}

/// Two numeric-looking texts compare numerically, as ORDER BY and MIN/MAX
/// already order them (`'9'` before `'10'`). `<` compares them lexically.
#[test]
fn numeric_text_less_than_is_numeric() -> TestResult {
    one(
        "SELECT '10' < '9' AS a, '9' < '10' AS b, '010' < '9' AS c FROM one",
        "a,b,c\n0,1,0",
    )
}

/// Numbers sort before non-numeric text in ORDER BY; `<` must agree:
/// `'-x' < 10` is 0 and `10 < 'abc'` is 1.
#[test]
fn non_numeric_text_vs_number_order() -> TestResult {
    one(
        "SELECT '-x' < 10 AS a, 10 < 'abc' AS b FROM one",
        "a,b\n0,1",
    )
}

/// ORDER BY puts `9, 10` before `-x, abc`, but `s < '9'` selects `-x`.
/// Under one total order no row is below 9.
#[test]
fn less_than_agrees_with_order_by() -> TestResult {
    let st = [("st.csv", "s\n10\n9\nabc\n-x\n")];
    assert_query_on(&st, "SELECT s FROM st ORDER BY s", "s\n9\n10\n-x\nabc")?;
    assert_query_on(&st, "SELECT s FROM st WHERE s < '9' ORDER BY s", "s")
}

/// Guard (passes today, matches SQLite): DISTINCT and GROUP BY keep the text
/// `'1'` and the integer `1` apart.
#[test]
fn distinct_and_group_by_keep_text_and_integer_apart() -> TestResult {
    one(
        "SELECT DISTINCT v FROM (SELECT '1' AS v UNION ALL SELECT 1 UNION ALL SELECT 1) AS q",
        "v\n1\n1",
    )?;
    one(
        "SELECT typeof(v) AS t, count(*) AS c FROM \
         (SELECT '1' AS v UNION ALL SELECT 1 UNION ALL SELECT '1') AS q GROUP BY v ORDER BY t",
        "t,c\ninteger,1\ntext,2",
    )
}

// ---------------------------------------------------------------------------
// A4. Aggregate of outer-only columns in a subquery
// (`compile_select_grouped`, compiler_aggregate.rs ~228)
// ---------------------------------------------------------------------------

/// `MIN(d.n)` inside a subquery over `b` aggregates only outer columns; sqawk
/// returns NULL for every row. It must be a clean error.
#[test]
#[ignore]
fn outer_only_aggregate_in_subquery_is_error() -> TestResult {
    let out = run_sqls(
        &[D, B],
        &[],
        &["SELECT k, (SELECT MIN(d.n) FROM b WHERE b.v >= d.n) AS m FROM d"],
    )?;
    assert_fails_cleanly(&out);
    Ok(())
}

// ---------------------------------------------------------------------------
// A5. Deep expressions overflow the stack (recursive parse/compile/eval).
// Limits as SQLite: expression depth 1000, 500 compound terms.
// ---------------------------------------------------------------------------

/// 10,000-term `1+1+...`: SIGABRT stack overflow. Must be a clean error.
#[test]
fn deep_plus_chain_is_clean_error() -> TestResult {
    assert_fails_cleanly(&run_one(&chain(10_000, "+", "1"))?);
    Ok(())
}

/// 7,000-term OR chain: stack overflow.
#[test]
fn deep_or_chain_is_clean_error() -> TestResult {
    assert_fails_cleanly(&run_one(&chain(7_000, " OR ", "id = 2"))?);
    Ok(())
}

/// 10,000-term AND chain: stack overflow.
#[test]
fn deep_and_chain_is_clean_error() -> TestResult {
    assert_fails_cleanly(&run_one(&chain(10_000, " AND ", "id = 1"))?);
    Ok(())
}

/// 5,000-term `||` chain: stack overflow.
#[test]
fn deep_concat_chain_is_clean_error() -> TestResult {
    assert_fails_cleanly(&run_one(&chain(5_000, " || ", "'a'"))?);
    Ok(())
}

/// 3,000 UNION ALL arms: stack overflow.
#[test]
fn many_union_arms_is_clean_error() -> TestResult {
    let sql = (0..3000)
        .map(|i| format!("SELECT {i} AS v"))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    assert_fails_cleanly(&run_one(&sql)?);
    Ok(())
}

/// A 900-term `1+...` chain is below SQLite's depth limit and must work; the
/// debug build overflows its stack today.
#[test]
fn plus_chain_of_900_works() -> TestResult {
    one(&chain(900, "+", "1"), "v\n900")
}

/// Guard (passes today): 400 UNION ALL arms (below the 500-term limit) work.
#[test]
fn union_of_400_arms_works() -> TestResult {
    let sql = (0..400)
        .map(|i| format!("SELECT {i} AS v"))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let expected = std::iter::once("v".to_string())
        .chain((0..400).map(|i| i.to_string()))
        .collect::<Vec<_>>()
        .join("\n");
    one(&sql, &expected)
}

// ---------------------------------------------------------------------------
// A6. Result column names (SQLite rules: a column reference is named by its
// bare column name, `*` expands to bare names, any other unaliased
// expression is named by its SQL text as written). `output_name`
// compiler.rs ~936, `get_column_name_from_expr` ~2004, `star_name` ~850.
// ---------------------------------------------------------------------------

const U: (&str, &str) = ("u.csv", "id,name\n1,ann\n");
const O: (&str, &str) = ("o.csv", "id,uid\n7,1\n");
const T: (&str, &str) = ("t.csv", "n,name\n5,ab\n");

/// In a join, `u.name, o.id` are headed `u.name,o.id`; SQLite: `name,id`.
#[test]
#[ignore]
fn qualified_column_header_is_bare_name() -> TestResult {
    assert_query_on(
        &[U, O],
        "SELECT u.name, o.id FROM u JOIN o ON u.id = o.uid",
        "name,id\nann,7",
    )
}

/// `*` over a join is headed `u.id,u.name,o.id,o.uid`; SQLite uses bare
/// names, duplicates included.
#[test]
#[ignore]
fn star_over_join_header_is_bare_names() -> TestResult {
    assert_query_on(&[U, O], "SELECT * FROM u, o", "id,name,id,uid\n1,ann,7,1")
}

/// Unaliased expressions are headed `expr`; SQLite uses the text as written.
#[test]
#[ignore]
fn expression_header_is_sql_text() -> TestResult {
    assert_query_on(&[T], "SELECT n*2 FROM t", "n*2\n10")?;
    assert_query_on(&[T], "SELECT upper(name)||1 FROM t", "upper(name)||1\nAB1")?;
    assert_query_on(&[T], "SELECT n  +  1 FROM t", "n  +  1\n6")
}

/// `COUNT(*)` is headed `COUNT`; SQLite: `COUNT(*)`.
#[test]
#[ignore]
fn count_star_header_is_sql_text() -> TestResult {
    assert_query_on(&[T], "SELECT COUNT(*) FROM t", "COUNT(*)\n1")
}

// ---------------------------------------------------------------------------
// A7. REAL rendering and summation. Rendering = SQLite `CAST(x AS TEXT)`:
// `%.15g`, `.0` on integral values, `Inf`/`-Inf` (Value Display, table.rs
// ~158; `arith::text_of` ~290; share `c_general`, func.rs ~620). SUM/TOTAL/
// AVG use Neumaier summation as SQLite does (agg.rs `Sum::Real`).
// ---------------------------------------------------------------------------

/// Printed as 301 digits instead of `1.5e+300`.
#[test]
fn large_real_uses_exponent() -> TestResult {
    one("SELECT 1e300 * 1.5 AS v FROM one", "v\n1.5e+300")
}

/// Printed as `0.000...015` instead of `1.5e-300`.
#[test]
fn tiny_real_uses_exponent() -> TestResult {
    one("SELECT 1.5e-300 AS v FROM one", "v\n1.5e-300")
}

/// Printed as `0.30000000000000004` instead of `0.3`.
#[test]
fn real_prints_15_significant_digits() -> TestResult {
    one("SELECT 0.1 + 0.2 AS v FROM one", "v\n0.3")
}

/// Integer overflow to real prints `9223372036854776000`; SQLite
/// `9.22337203685478e+18`.
#[test]
fn overflowed_integer_prints_as_real() -> TestResult {
    one(
        "SELECT 9223372036854775807 + 1 AS v FROM one",
        "v\n9.22337203685478e+18",
    )
}

/// An integral real prints `2`; SQLite renders `2.0`.
#[test]
fn integral_real_prints_point_zero() -> TestResult {
    one("SELECT 2.0 * 1 AS v FROM one", "v\n2.0")
}

/// Infinity prints `inf` / `-inf`; SQLite `Inf` / `-Inf`.
#[test]
fn infinity_prints_like_sqlite() -> TestResult {
    one(
        "SELECT 1e308 * 10 AS v, -1e308 * 10 AS w FROM one",
        "v,w\nInf,-Inf",
    )
}

/// `ROUND(x, N)` with negative N rounds to tens/hundreds (1200); SQLite
/// treats negative N as 0: `ROUND(1234.5, -2)` = 1235.0 (func.rs ~666).
#[test]
fn round_negative_digits_is_zero() -> TestResult {
    one("SELECT ROUND(1234.5, -2) AS v FROM one", "v\n1235.0")
}

/// Naive summation: SUM over 17.07, 10.51, 23.41, 29.99, 22.82 is
/// 103.79999999999998, so `SUM(x) = 103.8` is 0. SQLite (Neumaier) gives
/// exactly 103.8.
#[test]
fn sum_is_compensated() -> TestResult {
    let m = [("m.csv", "x\n17.07\n10.51\n23.41\n29.99\n22.82\n")];
    assert_query_on(
        &m,
        "SELECT SUM(x) = 103.8 AS e, TOTAL(x) = 103.8 AS t, SUM(x) AS s, AVG(x) AS a FROM m",
        "e,t,s,a\n1,1,103.8,20.76",
    )
}

/// SUM of 0.1, 0.2, 0.3 prints `0.6000000000000001`; SQLite `0.6`, and
/// `SUM(x) = 0.6` is 1.
#[test]
fn sum_of_tenths() -> TestResult {
    let p = [("p.csv", "x\n0.1\n0.2\n0.3\n")];
    assert_query_on(
        &p,
        "SELECT SUM(x) AS s, SUM(x) = 0.6 AS e FROM p",
        "s,e\n0.6,1",
    )
}

// ===========================================================================
// B. IO / data safety
// ===========================================================================

// ---------------------------------------------------------------------------
// B8. Verbatim writeback: untouched rows must be written byte-for-byte as
// read (record each row's raw bytes at load; csv_handler.rs / table.rs)
// ---------------------------------------------------------------------------

/// Unnecessary quotes on an untouched row are dropped (`"abc"` -> `abc`).
#[test]
#[ignore]
fn writeback_keeps_unnecessary_quotes() -> TestResult {
    assert_write_back(
        "q.csv",
        b"k,s\n1,\"abc\"\n2,x\n",
        &[],
        &["UPDATE q SET s = 'y' WHERE k = 2"],
        b"k,s\n1,\"abc\"\n2,y\n",
    )
}

/// An unquoted trailing space on an untouched row gets quoted (`"trail "`).
#[test]
#[ignore]
fn writeback_keeps_trailing_space_unquoted() -> TestResult {
    assert_write_back(
        "q.csv",
        b"k,s\n1,trail \n2,x\n",
        &[],
        &["UPDATE q SET s = 'y' WHERE k = 2"],
        b"k,s\n1,trail \n2,y\n",
    )
}

/// A TSV field `a"b` on an untouched row is rewritten as `"a""b"`.
#[test]
#[ignore]
fn writeback_keeps_tsv_quote_char() -> TestResult {
    assert_write_back(
        "t.tsv",
        b"k\ts\n1\ta\"b\n2\tz\n",
        &[],
        &["UPDATE t SET s = 'y' WHERE k = 2"],
        b"k\ts\n1\ta\"b\n2\ty\n",
    )
}

/// The UTF-8 BOM is dropped on writeback.
#[test]
#[ignore]
fn writeback_keeps_bom() -> TestResult {
    assert_write_back(
        "bom.csv",
        b"\xef\xbb\xbfk,s\n1,a\n2,b\n",
        &[],
        &["UPDATE bom SET s = 'y' WHERE k = 2"],
        b"\xef\xbb\xbfk,s\n1,a\n2,y\n",
    )
}

/// A short (ragged) untouched row is padded with a trailing comma.
#[test]
#[ignore]
fn writeback_keeps_short_row() -> TestResult {
    assert_write_back(
        "rag.csv",
        b"k,s,t\n1,a\n2,b,c\n",
        &[],
        &["UPDATE rag SET s = 'y' WHERE k = 2"],
        b"k,s,t\n1,a\n2,y,c\n",
    )
}

/// Invalid UTF-8 in an untouched row is replaced by U+FFFD.
#[test]
#[ignore]
fn writeback_keeps_invalid_utf8() -> TestResult {
    assert_write_back(
        "bad.csv",
        b"k,s\n1,\xff\xfe\n2,b\n",
        &[],
        &["UPDATE bad SET s = 'y' WHERE k = 2"],
        b"k,s\n1,\xff\xfe\n2,y\n",
    )
}

/// The original last line (no final newline, quoted) is rewritten when a row
/// is appended. It must stay `2,"b"`, with the new row on its own line.
#[test]
#[ignore]
fn writeback_insert_after_unterminated_last_line() -> TestResult {
    assert_write_back(
        "nq.csv",
        b"k,s\n1,a\n2,\"b\"",
        &[],
        &["INSERT INTO nq VALUES (3, 'c')"],
        b"k,s\n1,a\n2,\"b\"\n3,c\n",
    )
}

/// A no-op UPDATE over every row leaves the file byte-identical.
#[test]
#[ignore]
fn writeback_noop_update_is_identity() -> TestResult {
    let content = b"k,s\n1,\"abc\"\n2,trail \n3,x\n";
    assert_write_back("q.csv", content, &[], &["UPDATE q SET k = k"], content)
}

// ---------------------------------------------------------------------------
// B9. Unclosed quote is silently accepted (the csv crate yields the rest of
// the file as one field)
// ---------------------------------------------------------------------------

/// `1,"2\n3,4\n` loads as one row with b = `2\n3,4\n`. Must be an error
/// naming the line where the quoted field starts.
#[test]
#[ignore]
fn unclosed_quote_is_error() -> TestResult {
    let out = run_sqls(
        &[("uq.csv", "a,b\n1,\"2\n3,4\n")],
        &[],
        &["SELECT * FROM uq"],
    )?;
    assert_clean_error(&out, &["unterminated", "line 2"]);
    Ok(())
}

// ---------------------------------------------------------------------------
// B10. Duplicate / empty column names (csv_handler.rs ~126; compiler_ddl.rs)
// ---------------------------------------------------------------------------

fn assert_load_fails(name: &str, content: &str) -> TestResult {
    let out = run_sqls(&[(name, content)], &[], &["SELECT 1 AS v"])?;
    assert_fails_cleanly(&out);
    Ok(())
}

/// Header `a,a` loads; both columns read as the first.
#[test]
#[ignore]
fn duplicate_header_is_error() -> TestResult {
    assert_load_fails("dh.csv", "a,a\n1,2\n")
}

/// Header `a,A` loads; column names are case-insensitive, so it is a duplicate.
#[test]
#[ignore]
fn case_insensitive_duplicate_header_is_error() -> TestResult {
    assert_load_fails("dhc.csv", "a,A\n1,2\n")
}

/// Header `a,,c` loads with an empty column name.
#[test]
#[ignore]
fn empty_header_name_is_error() -> TestResult {
    assert_load_fails("eh.csv", "a,,c\n1,2,3\n")
}

/// `ALTER TABLE al ADD COLUMN a INT` adds a second column `a`.
#[test]
#[ignore]
fn alter_add_existing_column_is_error() -> TestResult {
    let out = run_sqls(
        &[("al.csv", "a,b\n1,2\n")],
        &[],
        &["ALTER TABLE al ADD COLUMN a INT"],
    )?;
    assert_fails_cleanly(&out);
    Ok(())
}

/// `CREATE TABLE ct (x INT, x INT)` creates two columns named `x`.
#[test]
#[ignore]
fn create_table_duplicate_column_is_error() -> TestResult {
    assert_fails_cleanly(&run_one("CREATE TABLE ct (x INT, x INT)")?);
    Ok(())
}

/// `CREATE TABLE ct (x INT, X INT)`: a case-insensitive duplicate.
#[test]
#[ignore]
fn create_table_case_insensitive_duplicate_is_error() -> TestResult {
    assert_fails_cleanly(&run_one("CREATE TABLE ct (x INT, X INT)")?);
    Ok(())
}

// ---------------------------------------------------------------------------
// B11. Table specs (`file_handler.rs` ~286, `main.rs` ~110): a `name=` prefix
// is a table name only when it is identifier-like; paths are canonical
// ---------------------------------------------------------------------------

/// `<dir>/k=v/e.csv` is split at `=` into table `<dir>/k` and file `v/e.csv`
/// ("File not found"). It is a path; the table is `e`.
#[test]
#[ignore]
fn equals_in_directory_is_path() -> TestResult {
    let dir = create_temp_dir()?;
    let p = write_files(dir.path(), &[("k=v/e.csv", b"a\n1\n")])?;
    let (stdout, stderr, ok) = run_sql("SELECT a FROM e", &[&p[0]], &[])?;
    assert!(ok, "load failed: {stderr}");
    assert_eq!(stdout.trim_end(), "a\n1");
    Ok(())
}

/// `=al.csv` (empty table name) is accepted.
#[test]
#[ignore]
fn empty_table_name_is_error() -> TestResult {
    let dir = create_temp_dir()?;
    write_files(dir.path(), &[("al.csv", b"a\n1\n")])?;
    let out = run_in(dir.path(), &["-s", "SELECT 1 AS v", "=al.csv"], None)?;
    assert_fails_cleanly(&out);
    Ok(())
}

/// The same file loaded under two names is accepted; with --write the two
/// copies would overwrite each other. Must be an error.
#[test]
#[ignore]
fn same_file_twice_is_error() -> TestResult {
    let dir = create_temp_dir()?;
    write_files(dir.path(), &[("al.csv", b"a\n1\n")])?;
    let out = run_in(
        dir.path(),
        &["-s", "SELECT a FROM x1", "x1=al.csv", "x2=./al.csv"],
        None,
    )?;
    assert_fails_cleanly(&out);
    Ok(())
}

/// REPL `.cd ..` then `.save` writes `s.csv` into the NEW directory (the path
/// is stored relative). It must write the file that was loaded.
#[test]
#[ignore]
fn repl_save_after_cd_writes_original_file() -> TestResult {
    let dir = create_temp_dir()?;
    let sub = dir.path().join("sub");
    write_files(dir.path(), &[("sub/s.csv", b"a\n1\n")])?;
    let out = run_in(
        &sub,
        &["-i", "s.csv"],
        Some(b"INSERT INTO s VALUES (9);\n.cd ..\n.save\n"),
    )?;
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(fs::read_to_string(sub.join("s.csv"))?, "a\n1\n9\n");
    assert!(
        !dir.path().join("s.csv").exists(),
        ".save wrote a new file in the cd target"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// B12. Silently ignored inputs (compiler_check.rs ~357; database.rs ~147-183)
// ---------------------------------------------------------------------------

/// `STORED AS PARQUET` is ignored: a CSV is written to `p.pq`.
#[test]
#[ignore]
fn stored_as_parquet_is_error() -> TestResult {
    let dir = create_temp_dir()?;
    write_files(dir.path(), &[("al.csv", b"a\n1\n")])?;
    let out = run_in(
        dir.path(),
        &[
            "-w",
            "-s",
            "CREATE TABLE p (a INT) LOCATION 'p.pq' STORED AS PARQUET",
            "-s",
            "INSERT INTO p VALUES (1)",
            "al.csv",
        ],
        None,
    )?;
    assert_fails_cleanly(&out);
    assert!(!dir.path().join("p.pq").exists(), "p.pq was written");
    Ok(())
}

fn tabledef_fails(def: &str, extra: &[&str]) -> TestResult {
    let mut args = vec![def];
    args.extend_from_slice(extra);
    let out = run_sqls(&[("al.csv", "a,b\n1,2\n")], &args, &["SELECT * FROM al"])?;
    assert_fails_cleanly(&out);
    Ok(())
}

/// `--tabledef=bad` (no `:`) is ignored.
#[test]
#[ignore]
fn tabledef_without_colon_is_error() -> TestResult {
    tabledef_fails("--tabledef=bad", &[])
}

/// `--tabledef=nosuch:a,b` names no loaded table and is ignored.
#[test]
#[ignore]
fn tabledef_unknown_table_is_error() -> TestResult {
    tabledef_fails("--tabledef=nosuch:a,b", &[])
}

/// `--tabledef=al:x,` creates a column with an empty name.
#[test]
#[ignore]
fn tabledef_empty_column_is_error() -> TestResult {
    tabledef_fails("--tabledef=al:x,", &["--no-header"])
}

// ---------------------------------------------------------------------------
// B13. Writeback edge cases (sql_executor.rs ~123, csv_handler.rs save ~320)
// ---------------------------------------------------------------------------

/// A read-only (0444) file is replaced by the atomic rename. --write must
/// refuse up front and write nothing, including other modified files.
#[test]
#[ignore]
fn write_to_read_only_file_is_refused() -> TestResult {
    let dir = create_temp_dir()?;
    if !perms_enforced(dir.path()) {
        return Ok(());
    }
    let p = write_files(
        dir.path(),
        &[("rw.csv", b"a,b\n1,2\n"), ("ro.csv", b"a,b\n1,2\n")],
    )?;
    chmod(Path::new(&p[1]), 0o444);
    let out = run_in(
        dir.path(),
        &[
            "-w",
            "-s",
            "INSERT INTO rw VALUES (3, 4)",
            "-s",
            "INSERT INTO ro VALUES (5, 6)",
            "rw.csv",
            "ro.csv",
        ],
        None,
    )?;
    chmod(Path::new(&p[1]), 0o644);
    assert_clean_error(&out, &["ro.csv"]);
    assert_eq!(fs::read_to_string(&p[0])?, "a,b\n1,2\n", "rw.csv written");
    assert_eq!(fs::read_to_string(&p[1])?, "a,b\n1,2\n", "ro.csv written");
    Ok(())
}

/// A scratch table (CREATE TABLE without LOCATION) makes --write fail with
/// "has no associated file path", and the real modified file is not written.
/// The scratch table must be skipped.
#[test]
#[ignore]
fn write_skips_scratch_table() -> TestResult {
    assert_write_back(
        "rw.csv",
        b"a,b\n1,2\n",
        &[],
        &[
            "CREATE TABLE sc (x INT)",
            "INSERT INTO sc VALUES (1)",
            "INSERT INTO rw VALUES (3, 4)",
        ],
        b"a,b\n1,2\n3,4\n",
    )
}

/// DROP of a modified table makes --write fail with "Table 'rw' not found".
/// The dropped table is not written; the run succeeds.
#[test]
#[ignore]
fn write_after_drop_of_modified_table() -> TestResult {
    assert_write_back(
        "rw.csv",
        b"a,b\n1,2\n",
        &[],
        &["INSERT INTO rw VALUES (3, 4)", "DROP TABLE rw"],
        b"a,b\n1,2\n",
    )
}

// ---------------------------------------------------------------------------
// B14. File-type rules (file_handler.rs ~192-215): `.tsv`/`.tab` are tab,
// everything else comma; first line always the header; `#` is data unless
// `--comments`
// ---------------------------------------------------------------------------

/// A comma-separated `.txt` file is split on tabs (one column `a,b`).
#[test]
#[ignore]
fn txt_file_is_comma_separated() -> TestResult {
    assert_query_on(&[("c.txt", "a,b\n1,2\n")], "SELECT b FROM c", "b\n2")
}

/// A numeric first line in a `.tsv` is guessed to be data (columns a, b).
/// It is the header.
#[test]
#[ignore]
fn tsv_first_line_is_header() -> TestResult {
    assert_query_on(
        &[("num.tsv", "1\t2\n3\t4\n")],
        "SELECT * FROM num",
        "1,2\n3,4",
    )
}

/// Same for a file without an extension.
#[test]
#[ignore]
fn extensionless_first_line_is_header() -> TestResult {
    assert_query_on(&[("numx", "1,2\n3,4\n")], "SELECT * FROM numx", "1,2\n3,4")
}

/// `#` lines in a `.tsv` are skipped as comments by default; they are data.
#[test]
#[ignore]
fn hash_line_is_data_by_default() -> TestResult {
    assert_query_on(
        &[("h.tsv", "a\tb\n#x\t1\n2\t3\n")],
        "SELECT * FROM h",
        "a,b\n#x,1\n2,3",
    )
}

/// `--comments` is rejected as an unknown flag. It skips `#` lines on load ...
#[test]
#[ignore]
fn comments_flag_skips_hash_lines() -> TestResult {
    assert_sqls(
        &[("hx", "a,b\n#x,1\n2,3\n")],
        &["--comments"],
        &["SELECT * FROM hx"],
        "a,b\n2,3",
    )
}

/// ... and writeback keeps them in place.
#[test]
#[ignore]
fn comments_flag_writeback_keeps_hash_lines() -> TestResult {
    assert_write_back(
        "hx",
        b"a,b\n#x,1\n2,3\n",
        &["--comments"],
        &["UPDATE hx SET b = 9 WHERE a = 2"],
        b"a,b\n#x,1\n2,9\n",
    )
}

// ===========================================================================
// C. CLI / REPL / errors
// ===========================================================================

// ---------------------------------------------------------------------------
// C15. CLI flags (cli.rs, config.rs, main.rs)
// ---------------------------------------------------------------------------

/// `-f FILE` runs SQL from a file.
#[test]
#[ignore]
fn sql_from_file() -> TestResult {
    let dir = create_temp_dir()?;
    write_files(
        dir.path(),
        &[("one.csv", b"id\n1\n"), ("q.sql", b"SELECT 1 AS v;\n")],
    )?;
    let out = run_in(dir.path(), &["-f", "q.sql", "one.csv"], None)?;
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&out.stdout), "v\n1\n");
    Ok(())
}

/// `-f -` reads SQL from stdin.
#[test]
#[ignore]
fn sql_from_stdin() -> TestResult {
    let dir = create_temp_dir()?;
    write_files(dir.path(), &[("one.csv", b"id\n1\n")])?;
    let out = run_in(dir.path(), &["-f", "-", "one.csv"], Some(b"SELECT 2 AS v"))?;
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&out.stdout), "v\n2\n");
    Ok(())
}

/// `-s` and `-f` run in command-line order.
#[test]
#[ignore]
fn sql_and_files_run_in_order() -> TestResult {
    let dir = create_temp_dir()?;
    write_files(
        dir.path(),
        &[("one.csv", b"id\n1\n"), ("q2.sql", b"SELECT 2 AS v;\n")],
    )?;
    let out = run_in(
        dir.path(),
        &[
            "-s",
            "SELECT 1 AS v",
            "-f",
            "q2.sql",
            "-s",
            "SELECT 3 AS v",
            "one.csv",
        ],
        None,
    )?;
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&out.stdout), "v\n1\nv\n2\nv\n3\n");
    Ok(())
}

/// `--output-delimiter` sets the output separator (and overrides -F's).
#[test]
#[ignore]
fn output_delimiter_flag() -> TestResult {
    assert_sqls(
        &[("p.txt", "a|b\n1|2\n")],
        &["-F", "|", "--output-delimiter", ";"],
        &["SELECT * FROM p"],
        "a;b\n1;2",
    )
}

/// `--no-output-header` omits the header line.
#[test]
#[ignore]
fn no_output_header_flag() -> TestResult {
    assert_sqls(
        &[("rw.csv", "a,b\n1,2\n")],
        &["--no-output-header"],
        &["SELECT * FROM rw"],
        "1,2",
    )
}

/// `--field-separator` is the long form of -F.
#[test]
#[ignore]
fn field_separator_long_flag() -> TestResult {
    assert_sqls(
        &[("c.txt", "a:b\n1:2\n")],
        &["--field-separator", ":"],
        &["SELECT b FROM c"],
        "b\n2",
    )
}

/// `-i` with `-s` silently ignores `-s` and starts the REPL. It is a usage
/// error (exit 2).
#[test]
#[ignore]
fn interactive_with_sql_is_usage_error() -> TestResult {
    let dir = create_temp_dir()?;
    write_files(dir.path(), &[("one.csv", b"id\n1\n")])?;
    let out = run_in(dir.path(), &["-i", "-s", "SELECT 1", "one.csv"], Some(b""))?;
    assert_eq!(out.status.code(), Some(2));
    Ok(())
}

// ---------------------------------------------------------------------------
// C16. Error format: one line `sqawk: <msg>`, exit 1 (error.rs ~71-103
// prints Display and then the source chain, repeating the cause)
// ---------------------------------------------------------------------------

/// A directory as input prints `Error: ... Caused by: 0: I/O error: Is a
/// directory ... 1: Is a directory ...`.
#[test]
#[ignore]
fn error_directory_input() -> TestResult {
    let dir = create_temp_dir()?;
    fs::create_dir(dir.path().join("adir.csv"))?;
    let out = run_in(dir.path(), &["-s", "SELECT 1", "adir.csv"], None)?;
    assert_clean_error(&out, &["adir.csv", "Is a directory"]);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(stderr.matches("Is a directory").count(), 1, "{stderr}");
    Ok(())
}

/// A syntax error prints a multi-line chain repeating the parser message.
#[test]
#[ignore]
fn error_syntax() -> TestResult {
    let out = run_one("SELEC 1")?;
    assert_clean_error(&out, &["SELEC"]);
    Ok(())
}

/// An unknown column prints `Error: Failed to execute SQL: <sql>` plus a
/// "Caused by" line.
#[test]
#[ignore]
fn error_unknown_column() -> TestResult {
    let out = run_one("SELECT nope FROM one")?;
    assert_clean_error(&out, &["nope"]);
    Ok(())
}

/// An unknown function reports "Unsupported function: FOO"; SQLite's message
/// is `no such function: FOO`.
#[test]
#[ignore]
fn error_unknown_function() -> TestResult {
    let out = run_one("SELECT foo(id) FROM one")?;
    assert_clean_error(&out, &["no such function: FOO"]);
    Ok(())
}

/// An unreadable input file reports "Permission denied" twice and does not
/// name the file in the cause.
#[test]
#[ignore]
fn error_permission_denied_names_file() -> TestResult {
    let dir = create_temp_dir()?;
    if !perms_enforced(dir.path()) {
        return Ok(());
    }
    let p = write_files(dir.path(), &[("np.csv", b"a\n1\n")])?;
    chmod(Path::new(&p[0]), 0o000);
    let out = run_in(dir.path(), &["-s", "SELECT 1", "np.csv"], None)?;
    chmod(Path::new(&p[0]), 0o644);
    assert_clean_error(&out, &["np.csv", "Permission denied"]);
    Ok(())
}

/// A write failure (read-only directory) names the internal temp file
/// (`.tmpXXXX`).
#[test]
#[ignore]
fn error_write_failure_hides_temp_file() -> TestResult {
    let dir = create_temp_dir()?;
    if !perms_enforced(dir.path()) {
        return Ok(());
    }
    write_files(dir.path(), &[("rod/e.csv", b"a\n1\n")])?;
    let rod = dir.path().join("rod");
    chmod(&rod, 0o555);
    let out = run_in(
        dir.path(),
        &["-w", "-s", "DELETE FROM e", "rod/e.csv"],
        None,
    );
    chmod(&rod, 0o755);
    let out = out?;
    assert_clean_error(&out, &["e.csv"]);
    assert!(
        !String::from_utf8_lossy(&out.stderr).contains(".tmp"),
        "temp file name leaked: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// C17. `-v` panics when stderr is closed (`eprintln!`)
// ---------------------------------------------------------------------------

/// With stderr's read end closed, `-v` exits 101 ("failed printing to stderr").
#[test]
#[ignore]
fn verbose_with_closed_stderr_does_not_panic() -> TestResult {
    let dir = create_temp_dir()?;
    // 20,000 rows make -v write megabytes to stderr, far beyond a pipe
    // buffer, so a write after the read end is closed is certain.
    let mut big = String::from("n\n");
    for i in 0..20_000 {
        big.push_str(&format!("{i}\n"));
    }
    write_files(dir.path(), &[("big.csv", big.as_bytes())])?;
    let mut child = Command::new(sqawk_bin())
        .args(["-v", "-s", "SELECT n FROM big", "big.csv"])
        .current_dir(dir.path())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()?;
    let mut first = [0u8; 16];
    child.stderr.as_mut().unwrap().read_exact(&mut first)?;
    drop(child.stderr.take());
    let status = child.wait()?;
    assert_ne!(status.code(), Some(101), "panicked on closed stderr");
    assert!(status.code().is_some(), "killed by signal: {status:?}");
    Ok(())
}

// ---------------------------------------------------------------------------
// C18. Piped (non-tty) REPL (repl.rs)
// ---------------------------------------------------------------------------

/// Piped REPL stdout carries the banner, "Query returned 1 rows" and
/// "CTRL-D" around the result. Only query results belong on stdout.
#[test]
#[ignore]
fn repl_piped_stdout_is_results_only() -> TestResult {
    let (out, _d) = run_repl(&[("rw.csv", "a,b\n1,2\n")], "SELECT a FROM rw;\n")?;
    assert!(out.status.success());
    assert_eq!(String::from_utf8_lossy(&out.stdout), "a\n1\n");
    Ok(())
}

/// `.exit 3` exits with status 0.
#[test]
#[ignore]
fn repl_exit_code() -> TestResult {
    let (out, _d) = run_repl(&[("rw.csv", "a,b\n1,2\n")], ".exit 3\n")?;
    assert_eq!(out.status.code(), Some(3));
    Ok(())
}

/// A SQL error in a piped script still exits 0.
#[test]
#[ignore]
fn repl_script_error_exits_nonzero() -> TestResult {
    let (out, _d) = run_repl(&[("rw.csv", "a,b\n1,2\n")], "SELECT nope FROM rw;\n")?;
    assert!(!out.status.success(), "exit 0 after SQL error");
    Ok(())
}

/// A final statement without `;` at EOF is silently dropped.
#[test]
#[ignore]
fn repl_unterminated_last_statement_runs() -> TestResult {
    let (out, _d) = run_repl(&[("rw.csv", "a,b\n1,2\n")], "SELECT 2 AS v")?;
    assert!(
        String::from_utf8_lossy(&out.stdout).contains("v\n2\n"),
        "stdout:\n{}",
        String::from_utf8_lossy(&out.stdout)
    );
    Ok(())
}

/// A piped REPL writes `.sqawk_history` into the current directory. History
/// is kept only for a tty (in `$XDG_STATE_HOME/sqawk/history` or
/// `~/.sqawk_history`).
#[test]
#[ignore]
fn repl_piped_writes_no_history() -> TestResult {
    let (_out, dir) = run_repl(&[("rw.csv", "a,b\n1,2\n")], "SELECT a FROM rw;\n")?;
    fn walk(p: &Path, found: &mut Vec<PathBuf>) {
        for e in fs::read_dir(p).unwrap().flatten() {
            let path = e.path();
            if path.is_dir() {
                walk(&path, found);
            } else if path.to_string_lossy().contains("hist") {
                found.push(path);
            }
        }
    }
    let mut found = Vec::new();
    walk(dir.path(), &mut found);
    assert!(found.is_empty(), "history written: {found:?}");
    Ok(())
}

// ---------------------------------------------------------------------------
// C19. REPL `.tables PATTERN` uses a regex, not LIKE (repl.rs)
// ---------------------------------------------------------------------------

/// `.tables e%` misses `Ebig`: LIKE is ASCII case-insensitive.
#[test]
#[ignore]
fn repl_tables_pattern_case_insensitive() -> TestResult {
    assert_eq!(repl_tables("e%")?, vec!["Ebig", "emp", "exa"]);
    Ok(())
}

/// `.tables e.a` lists `exa`: `.` is a literal in LIKE.
#[test]
#[ignore]
fn repl_tables_pattern_dot_is_literal() -> TestResult {
    assert_eq!(repl_tables("e.a")?, Vec::<String>::new());
    Ok(())
}

// ---------------------------------------------------------------------------
// C20. Scalar functions and operators (func.rs, datetime.rs, arith.rs)
// ---------------------------------------------------------------------------

/// `printf('%.3d', 7)` ignores the precision (prints `7`).
#[test]
#[ignore]
fn printf_integer_precision_pads() -> TestResult {
    one("SELECT printf('%.3d', 7) AS v FROM one", "v\n007")
}

/// `printf('%u', -1)` prints `-1`; SQLite reinterprets as unsigned 64-bit.
#[test]
#[ignore]
fn printf_unsigned() -> TestResult {
    one(
        "SELECT printf('%u', -1) AS v FROM one",
        "v\n18446744073709551615",
    )
}

/// A negative year is rejected (NULL).
#[test]
#[ignore]
fn date_negative_year() -> TestResult {
    one("SELECT date('-0001-12-31') AS v FROM one", "v\n-0001-12-31")
}

/// A trailing space after the date is rejected (NULL).
#[test]
#[ignore]
fn date_trailing_space() -> TestResult {
    one("SELECT date('2024-01-01 ') AS v FROM one", "v\n2024-01-01")
}

/// `strftime('%u', ...)` (ISO weekday, Sunday = 7) yields NULL.
#[test]
#[ignore]
fn strftime_iso_weekday() -> TestResult {
    one("SELECT strftime('%u', '2024-01-07') AS v FROM one", "v\n7")
}

/// The `subsec` modifier yields NULL.
#[test]
#[ignore]
fn date_modifier_subsec() -> TestResult {
    one(
        "SELECT datetime('2024-01-01 10:00:00.123', 'subsec') AS v FROM one",
        "v\n2024-01-01 10:00:00.123",
    )
}

/// The `auto` modifier yields NULL.
#[test]
#[ignore]
fn date_modifier_auto() -> TestResult {
    one(
        "SELECT datetime(1700000000, 'auto') AS v FROM one",
        "v\n2023-11-14 22:13:20",
    )
}

/// The `julianday` modifier yields NULL.
#[test]
#[ignore]
fn date_modifier_julianday() -> TestResult {
    one(
        "SELECT date(2460000.5, 'julianday') AS v FROM one",
        "v\n2023-02-25",
    )
}

/// The `+HH:MM` modifier yields NULL.
#[test]
#[ignore]
fn date_modifier_time_offset() -> TestResult {
    one(
        "SELECT datetime('2024-01-01 10:00:00', '+01:30') AS v FROM one",
        "v\n2024-01-01 11:30:00",
    )
}

/// The `+YYYY-MM-DD` modifier yields NULL.
#[test]
#[ignore]
fn date_modifier_date_offset() -> TestResult {
    one(
        "SELECT date('2024-01-01', '+0001-02-03') AS v FROM one",
        "v\n2025-03-04",
    )
}

/// `0x10` fails with "Literal X'10' is not supported" (the parser reads it as
/// a blob literal).
#[test]
#[ignore]
fn hex_integer_literal() -> TestResult {
    one("SELECT 0x10 AS v FROM one", "v\n16")
}

/// Bitwise operators fail to parse (`<<`) or are unsupported.
#[test]
#[ignore]
fn bitwise_operators() -> TestResult {
    one(
        "SELECT 5 & 3 AS a, 5 | 3 AS b, ~5 AS c, 1 << 4 AS d, 256 >> 4 AS e FROM one",
        "a,b,c,d,e\n1,7,-6,16,16",
    )
}

/// Bitwise operators bind below `+` and above `=` in SQLite:
/// `(1 + 2) << 1` = 6 and `(6 & 3) = 2` = 1.
#[test]
#[ignore]
fn bitwise_operator_precedence() -> TestResult {
    one(
        "SELECT 1 + 2 << 1 AS a, 6 & 3 = 2 AS b FROM one",
        "a,b\n6,1",
    )
}

// ===========================================================================
// D. Transactions: BEGIN / COMMIT / ROLLBACK over the in-memory tables
// (rejected today as "BEGIN statements are not supported")
// ===========================================================================

/// BEGIN; UPDATE; ROLLBACK restores the original values.
#[test]
#[ignore]
fn rollback_restores_values() -> TestResult {
    assert_sqls(
        &[("tx.csv", TX)],
        &[],
        &[
            "BEGIN",
            "UPDATE tx SET n = 0",
            "ROLLBACK",
            "SELECT n FROM tx",
        ],
        "n\n10\n20",
    )
}

/// BEGIN; INSERT; COMMIT keeps the insert (statements in one `-s`).
#[test]
#[ignore]
fn commit_keeps_changes() -> TestResult {
    assert_sqls(
        &[("tx.csv", TX)],
        &[],
        &["BEGIN; INSERT INTO tx VALUES (3, 30); COMMIT; SELECT k FROM tx"],
        "k\n1\n2\n3",
    )
}

/// A committed change is written back.
#[test]
#[ignore]
fn commit_is_written_back() -> TestResult {
    assert_write_back(
        "tx.csv",
        TX.as_bytes(),
        &[],
        &["BEGIN", "INSERT INTO tx VALUES (3, 30)", "COMMIT"],
        b"k,n\n1,10\n2,20\n3,30\n",
    )
}

/// After ROLLBACK, --write leaves the file byte-identical.
#[test]
#[ignore]
fn rollback_is_not_written_back() -> TestResult {
    let content = b"k,n\n1,\"10\"\n2,20\n";
    assert_write_back(
        "tx.csv",
        content,
        &[],
        &["BEGIN", "UPDATE tx SET n = 0", "ROLLBACK"],
        content,
    )
}

/// A transaction still open at exit is rolled back before --write.
#[test]
#[ignore]
fn open_transaction_at_exit_is_rolled_back() -> TestResult {
    assert_write_back(
        "tx.csv",
        TX.as_bytes(),
        &[],
        &["BEGIN", "UPDATE tx SET n = 0"],
        TX.as_bytes(),
    )
}

/// BEGIN inside BEGIN is an error (SQLite: "cannot start a transaction
/// within a transaction").
#[test]
#[ignore]
fn nested_begin_is_error() -> TestResult {
    let out = run_sqls(&[("tx.csv", TX)], &[], &["BEGIN", "BEGIN"])?;
    assert_clean_error(&out, &["within a transaction"]);
    Ok(())
}

/// COMMIT without BEGIN is an error (SQLite: "cannot commit - no transaction
/// is active").
#[test]
#[ignore]
fn commit_without_begin_is_error() -> TestResult {
    let out = run_sqls(&[("tx.csv", TX)], &[], &["COMMIT"])?;
    assert_clean_error(&out, &["no transaction is active"]);
    Ok(())
}

/// ROLLBACK without BEGIN is an error (SQLite: "cannot rollback - no
/// transaction is active").
#[test]
#[ignore]
fn rollback_without_begin_is_error() -> TestResult {
    let out = run_sqls(&[("tx.csv", TX)], &[], &["ROLLBACK"])?;
    assert_clean_error(&out, &["no transaction is active"]);
    Ok(())
}

/// SAVEPOINT, RELEASE and ROLLBACK TO are rejected with a clean
/// "not supported" error.
#[test]
#[ignore]
fn savepoints_are_not_supported() -> TestResult {
    for stmt in ["SAVEPOINT s", "RELEASE s", "ROLLBACK TO s"] {
        let out = run_sqls(&[("tx.csv", TX)], &[], &["BEGIN", stmt])?;
        assert_clean_error(&out, &["not supported"]);
    }
    Ok(())
}

/// A piped REPL script: BEGIN; UPDATE; ROLLBACK; SELECT shows the original
/// values.
#[test]
#[ignore]
fn repl_rollback_restores_values() -> TestResult {
    let (out, _d) = run_repl(
        &[("tx.csv", TX)],
        "BEGIN;\nUPDATE tx SET n = 0;\nROLLBACK;\nSELECT n FROM tx;\n",
    )?;
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("n\n10\n20\n"), "stdout:\n{stdout}");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    Ok(())
}
