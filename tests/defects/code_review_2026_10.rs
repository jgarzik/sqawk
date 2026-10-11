//! Regression cover for the defects found by the 2026-10 code review of the
//! `updates` branch.
//!
//! Every test in this module encodes **correct** behaviour for a defect found
//! in that review. Each was `#[ignore]`d when written and un-ignored as its
//! defect was fixed; all of them now pass, so the red list is empty again.
//! A few tests (marked in their doc comments) were added with a fix rather
//! than before it. Run the red list with:
//!     cargo test --test mod defects -- --ignored
//!
//! Expected outputs were computed with SQLite 3.45 (python3 `sqlite3`) unless
//! the doc comment says otherwise. Output NULL is rendered as an EMPTY field;
//! a lone empty field prints as `""`.
//!
//! "No panic" tests run against the debug build, where integer overflow
//! panics: they require a clean error or the correct result.
//!
//! Root-cause locations are hints as of the review (file:line), not contracts.

use std::fs;

use crate::helpers::{assert_query_on, assert_query_on_fails, create_temp_dir, run_sql, ONE};

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// `assert_query_on` over the one-row table `one`.
fn one(sql: &str, expected: &str) -> TestResult {
    assert_query_on(&[ONE], sql, expected)
}

/// `assert_query_on_fails` over the one-row table `one`.
fn one_fails(sql: &str, expected_err: &str) -> TestResult {
    assert_query_on_fails(&[ONE], sql, expected_err)
}

/// Write `content` to a temp file `file_name`, run `sql` over it with
/// `extra` arguments plus `--write`, and return the file's bytes afterwards.
fn write_back(
    file_name: &str,
    content: &str,
    extra: &[&str],
    sql: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let dir = create_temp_dir()?;
    let p = dir.path().join(file_name);
    fs::write(&p, content)?;
    let mut args = extra.to_vec();
    args.push("--write");
    let (_out, stderr, ok) = run_sql(sql, &[p.to_str().unwrap()], &args)?;
    assert!(ok, "write query failed\n  sql: {sql}\n  stderr: {stderr}");
    Ok(String::from_utf8(fs::read(&p)?)?)
}

/// Assert that a `--write` run leaves the file byte-for-byte equal to `expected`.
fn assert_write_back(
    file_name: &str,
    content: &str,
    extra: &[&str],
    sql: &str,
    expected: &str,
) -> TestResult {
    let actual = write_back(file_name, content, extra, sql)?;
    assert_eq!(
        actual, expected,
        "\n=== sql ===\n{sql}\n=== input ===\n{content:?}\n=== expected (exact bytes) ===\n{expected:?}\n=== actual ===\n{actual:?}\n"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Finding 1: day/hour/minute/second modifiers overflow (datetime.rs:318)
// ---------------------------------------------------------------------------

/// `jd + (n * per).round() as i64` is unchecked: a huge or infinite count
/// overflows the add (a panic in debug). SQLite rejects a count beyond each
/// unit's limit, so the result is NULL.
#[test]
fn date_modifier_huge_day_count_is_null() -> TestResult {
    one(
        "SELECT date('2024-01-01', '+1e300 days') AS a, \
         date('2024-01-01', '+inf days') AS b, \
         date('2024-01-01', '-inf days') AS c, \
         date('2024-01-01', '+1e18 seconds') AS d, \
         date('2024-01-01', '+1e18 hours') AS e FROM one",
        "a,b,c,d,e\n,,,,",
    )
}

/// SQLite checks the moment's range once, after all modifiers, so a
/// modifier may step outside 0000-9999 if a later one steps back in.
/// sqawk checked after every modifier and returned NULL.
#[test]
fn date_range_is_checked_after_all_modifiers() -> TestResult {
    one(
        "SELECT date('9999-12-31', '+1 day', '-1 day') AS a, \
         date('0000-01-01', '-1 day', '+1 day') AS b FROM one",
        "a,b\n9999-12-31,0000-01-01",
    )
}

// ---------------------------------------------------------------------------
// Finding 2: month/year modifiers overflow (datetime.rs:328)
// ---------------------------------------------------------------------------

/// `p.m + k` / `p.y + k` are unchecked. SQLite: NULL.
#[test]
fn date_modifier_huge_month_count_is_null() -> TestResult {
    one(
        "SELECT date('2024-01-01', '+9223372036854775807 months') AS a, \
         date('2024-01-01', '-9223372036854775808 months') AS b, \
         date('2024-01-01', '+9223372036854775807 years') AS c, \
         date('2024-01-01', '+1e300 years') AS d FROM one",
        "a,b,c,d\n,,,",
    )
}

// ---------------------------------------------------------------------------
// Finding 3: printf width / precision overflow (func.rs:428, :472)
// ---------------------------------------------------------------------------

/// Width parsing `width * 10 + d` overflows usize. SQLite returns NULL here
/// (its result is too big); sqawk gives a clean error.
#[test]
fn printf_huge_width_is_clean_error() -> TestResult {
    one_fails(
        "SELECT printf('%99999999999999999999d', 1) AS p FROM one",
        "string or blob too big",
    )
}

/// A precision past u16::MAX panics inside Rust's formatting machinery
/// ("Formatting argument out of range"), in release builds too. SQLite
/// wraps the precision to 31 bits and prints `1`; sqawk gives a clean error.
#[test]
fn printf_huge_precision_is_clean_error() -> TestResult {
    one_fails(
        "SELECT printf('%.2147483648f', 1.0) AS p FROM one",
        "string or blob too big",
    )
}

/// A precision or width above u16::MAX but well within the length limit
/// works, as in SQLite.
#[test]
fn printf_large_precision_works() -> TestResult {
    one(
        "SELECT length(printf('%.70000f', 1.0)) AS f, \
         length(printf('%.70000e', 1.0)) AS e, \
         substr(printf('%.70000e', 1.0), -5) AS t, \
         printf('%.70000g', 0.5) AS g, \
         length(printf('%70000d', 1)) AS w FROM one",
        "f,e,t,g,w\n70002,70006,0e+00,0.5,70000",
    )
}

/// Found while fixing finding 3: an infinite real printed as `inf` with
/// `%f` and as `infe+00` with `%e` / `%g`. SQLite prints `Inf`.
#[test]
fn printf_infinity_prints_inf() -> TestResult {
    one(
        "SELECT printf('%f|%e|%g|%5.1f|%G', 1e308 * 10, -1e308 * 10, 1e308 * 10, \
         1e308 * 10, 1e308 * 10) AS p FROM one",
        "p\nInf|-Inf|Inf|  Inf|Inf",
    )
}

// ---------------------------------------------------------------------------
// Finding 4: `#` comment lines are dropped by --write (csv_handler.rs:101)
// ---------------------------------------------------------------------------

/// A TSV with leading, interleaved and trailing comments.
const TSV: &str = "# leading comment\n\
                   id\tname\tnote\n\
                   # before the first row\n\
                   1\tAlice\tx\n\
                   2\tBob\ty\n\
                   # between 2 and 3\n\
                   #another\n\
                   3\tCarol\tz\n\
                   # trailing 1\n\
                   # trailing 2\n";

/// A no-op UPDATE must leave the file byte-identical, comments included.
#[test]
fn tsv_comments_survive_noop_update() -> TestResult {
    assert_write_back("t.tsv", TSV, &[], "UPDATE t SET id = id WHERE id = 1", TSV)
}

/// Comments keep their own line endings in a CRLF file.
#[test]
fn crlf_tsv_comments_survive_noop_update() -> TestResult {
    let crlf = "#c1\r\nid\tv\r\n#c2\r\n1\ta\r\n2\tb\r\n#end\r\n";
    assert_write_back(
        "t.tsv",
        crlf,
        &[],
        "UPDATE t SET id = id WHERE id = 1",
        crlf,
    )
}

/// A headerless colon-separated file (the /etc/passwd shape) with comments.
#[test]
fn headerless_colon_file_comments_survive_noop_update() -> TestResult {
    let passwd = "# /etc/passwd excerpt\n\
                  root:x:0:0:root:/root:/bin/bash\n\
                  # system accounts\n\
                  daemon:x:1:1:daemon:/usr/sbin:/usr/sbin/nologin\n\
                  bin:x:2:2:bin:/bin:/usr/sbin/nologin\n\
                  # end\n";
    assert_write_back(
        "passwd.txt",
        passwd,
        &["-F", ":"],
        "UPDATE passwd SET c = c WHERE a = 'root'",
        passwd,
    )
}

/// UPDATE replaces rows in place, so the comments stay where they were.
#[test]
fn comments_stay_put_on_update() -> TestResult {
    assert_write_back(
        "t.tsv",
        TSV,
        &[],
        "UPDATE t SET name = 'Zed' WHERE id = 2",
        &TSV.replace("Bob", "Zed"),
    )
}

/// DELETE keeps a deleted row's comments at their position: before the next
/// surviving row, or at the end.
#[test]
fn comments_stay_put_on_delete() -> TestResult {
    assert_write_back(
        "t.tsv",
        TSV,
        &[],
        "DELETE FROM t WHERE id <> 2",
        "# leading comment\n\
         id\tname\tnote\n\
         # before the first row\n\
         2\tBob\ty\n\
         # between 2 and 3\n\
         #another\n\
         # trailing 1\n\
         # trailing 2\n",
    )
}

/// INSERT appends rows after the last row and before the trailing comments.
#[test]
fn insert_goes_before_trailing_comments() -> TestResult {
    assert_write_back(
        "t.tsv",
        TSV,
        &[],
        "INSERT INTO t VALUES (4, 'Dan', 'w')",
        "# leading comment\n\
         id\tname\tnote\n\
         # before the first row\n\
         1\tAlice\tx\n\
         2\tBob\ty\n\
         # between 2 and 3\n\
         #another\n\
         3\tCarol\tz\n\
         4\tDan\tw\n\
         # trailing 1\n\
         # trailing 2\n",
    )
}

/// Edge cases, written with the fix: a `#` line inside a quoted multi-line
/// field is data, not a comment; blank lines are dropped (they always were);
/// a last comment line without a newline is reproduced as it is (the csv
/// crate reports it as an empty record, which used to load as a phantom
/// all-NULL row and be written back as a `\t` line).
#[test]
fn comment_edge_cases_survive_noop_update() -> TestResult {
    let input = "id\tnote\n1\t\"line one\n#not a comment\"\n\n# real comment\n2\tb\n#eof";
    assert_write_back(
        "t.tsv",
        input,
        &[],
        "UPDATE t SET id = id WHERE id = 1",
        &input.replace("\n\n", "\n"),
    )
}

/// TRUNCATE (and deleting every row) keeps all comment lines, in order.
#[test]
fn comments_survive_deleting_every_row() -> TestResult {
    let comments_only = "# leading comment\n\
                         id\tname\tnote\n\
                         # before the first row\n\
                         # between 2 and 3\n\
                         #another\n\
                         # trailing 1\n\
                         # trailing 2\n";
    assert_write_back("t.tsv", TSV, &[], "DELETE FROM t", comments_only)?;
    assert_write_back("t.tsv", TSV, &[], "TRUNCATE TABLE t", comments_only)
}

// ---------------------------------------------------------------------------
// Finding 5: SUBSTR negates an i64::MIN length (func.rs:226)
// ---------------------------------------------------------------------------

/// `p2 = -p2` overflows for i64::MIN. SQLite returns ''.
#[test]
fn substr_min_length_does_not_overflow() -> TestResult {
    one(
        "SELECT substr('hello', 1, -9223372036854775808) || '|' || \
         substr('hello', 0, -9223372036854775808) || '|' || \
         substr('hello', 9223372036854775807, 9223372036854775807) AS s FROM one",
        "s\n||",
    )
}

// ---------------------------------------------------------------------------
// Findings 6 and 9: DISTINCT aggregates dedupe on Debug text (agg.rs:79)
// ---------------------------------------------------------------------------

/// `format!("{:?}")` makes Integer(1) and Float(1.0) distinct.
#[test]
fn count_distinct_integer_equals_real() -> TestResult {
    one(
        "SELECT COUNT(DISTINCT x) AS c FROM (SELECT 1 AS x UNION ALL SELECT 1.0) t",
        "c\n1",
    )
}

/// The same for SUM, AVG and GROUP_CONCAT.
#[test]
fn sum_avg_group_concat_distinct_integer_equals_real() -> TestResult {
    one(
        "SELECT SUM(DISTINCT x) AS s, AVG(DISTINCT x) AS a, GROUP_CONCAT(DISTINCT x) AS g \
         FROM (SELECT 1 AS x UNION ALL SELECT 1.0 UNION ALL SELECT 2) t",
        "s,a,g\n3,1.5,\"1,2\"",
    )
}

/// Text '1' and integer 1 are different values to DISTINCT (SQLite: 2).
/// Not ignored: already right; guards the fix above.
#[test]
fn count_distinct_text_differs_from_integer() -> TestResult {
    one(
        "SELECT COUNT(DISTINCT x) AS c FROM (SELECT 1 AS x UNION ALL SELECT '1') t",
        "c\n2",
    )
}

/// GROUP BY must agree with DISTINCT: 1 and 1.0 are one group, text '1'
/// another (numbers sort before text). sqawk merged all three.
#[test]
fn group_by_text_differs_from_integer() -> TestResult {
    one(
        "SELECT x, COUNT(*) AS c FROM \
         (SELECT 1 AS x UNION ALL SELECT 1.0 UNION ALL SELECT '1') t GROUP BY x",
        "x,c\n1,2\n1,1",
    )
}

// ---------------------------------------------------------------------------
// Finding 7: printf %g picks the exponent before rounding (func.rs:561)
// ---------------------------------------------------------------------------

/// C (and SQLite) choose between fixed and exponent form by the exponent
/// AFTER rounding to P significant digits.
#[test]
fn printf_g_exponent_after_rounding() -> TestResult {
    one(
        "SELECT printf('%g', 999999.5) AS a, printf('%g', 9.9999996) AS b, \
         printf('%g', 0.00009999995) AS c, printf('%g', 123456789) AS d, \
         printf('%g', 1e-5) AS e, printf('%g', 0) AS f, printf('%.3g', 999.5) AS g, \
         printf('%g', -999999.5) AS h, printf('%g', 1000000) AS i, \
         printf('%g', 100000) AS j FROM one",
        "a,b,c,d,e,f,g,h,i,j\n1e+06,10,0.0001,1.23457e+08,1e-05,0,1e+03,-1e+06,1e+06,100000",
    )
}

// ---------------------------------------------------------------------------
// Finding 8: fractional month/year modifiers give NULL (datetime.rs:324)
// ---------------------------------------------------------------------------

/// SQLite adds the whole months/years on the calendar, then the fraction as
/// 30-day months / 365-day years.
#[test]
fn fractional_month_and_year_modifiers() -> TestResult {
    one(
        "SELECT datetime('2024-01-01', '+1.5 months') AS a, \
         datetime('2024-01-01', '-0.5 years') AS b, \
         datetime('2024-01-01', '+2.25 years') AS c, \
         datetime('2024-01-31', '+1.5 months') AS d, \
         datetime('2024-01-01', '-1.5 months') AS e, \
         datetime('2024-03-31', '+0.1 months') AS f, \
         datetime('2024-01-01', '+0.3333 years') AS g, \
         datetime('2024-02-29 12:00:00', '+1.75 years') AS h FROM one",
        "a,b,c,d,e,f,g,h\n\
         2024-02-16 00:00:00,2023-07-02 12:00:00,2026-04-02 06:00:00,2024-03-17 00:00:00,\
         2023-11-16 00:00:00,2024-04-03 00:00:00,2024-05-01 15:42:28,2025-11-30 06:00:00",
    )
}

// ---------------------------------------------------------------------------
// Finding 10: unbounded result lengths (func.rs:328)
// ---------------------------------------------------------------------------

/// LPAD / RPAD allocate whatever length they are asked for (10 GB here, an
/// allocation failure abort). Results are capped at SQLite's default
/// SQLITE_MAX_LENGTH, 1,000,000,000 bytes: "string or blob too big".
#[test]
fn lpad_rpad_huge_length_is_clean_error() -> TestResult {
    one_fails(
        "SELECT lpad('a', 10000000000) AS p FROM one",
        "string or blob too big",
    )?;
    one_fails(
        "SELECT rpad('a', 10000000000) AS p FROM one",
        "string or blob too big",
    )
}

/// Just over the cap is refused before anything is allocated.
#[test]
fn lpad_just_over_the_cap_is_clean_error() -> TestResult {
    one_fails(
        "SELECT length(lpad('a', 1000000001)) AS p FROM one",
        "string or blob too big",
    )
}

/// printf width is capped the same way.
#[test]
fn printf_width_over_the_cap_is_clean_error() -> TestResult {
    one_fails(
        "SELECT length(printf('%10000000000d', 1)) AS p FROM one",
        "string or blob too big",
    )
}

/// REPLACE growth: 50,000 matches each replaced by 20,001 bytes is just over
/// 1e9 bytes from inputs of a few tens of kilobytes.
#[test]
fn replace_growth_over_the_cap_is_clean_error() -> TestResult {
    one_fails(
        "SELECT length(replace(rpad('', 50000, 'a'), 'a', rpad('', 20001, 'b'))) AS p FROM one",
        "string or blob too big",
    )
}
