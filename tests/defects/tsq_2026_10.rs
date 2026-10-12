//! Red list: defects found by `tsq diff` (2026-10), the differential tester
//! that runs random SQL through sqawk and through SQLite 3.45.
//!
//! Every test encodes **correct** behaviour: the answer SQLite 3.45 gives
//! (python3 `sqlite3` 3.45.1 and rusqlite's bundled 3.45.0 agree on each),
//! which is the behaviour sqawk documents (doc/sql_reference.md: "Semantics
//! follow SQLite 3.45"; none of these is a listed difference). Each is
//! `#[ignore]`d until fixed; remove the attribute with the fix. Run with:
//!     cargo test --test mod defects -- --ignored
//!
//! Each test names the tsq seed and test number that found it; `tsq diff
//! --seed S --only N --shrink` replays and minimizes it.

use std::time::Duration;

use crate::helpers::{assert_query_on, create_temp_dir};

type TestResult = Result<(), Box<dyn std::error::Error>>;

const ONE: &[(&str, &str)] = &[("t.csv", "x\n1\n")];

/// A real prints as SQLite's `CAST(x AS TEXT)` (doc "Real output"), which
/// rounds a decimal expansion of about 18 digits half up to 15 digits. The
/// double nearest 443.85 / 5867 is 0.07565195159365944999..., whose 18-digit
/// expansion ends ...4945|00 and rounds up to ...595; sqawk rounded the exact
/// binary value and printed ...594. Found by tsq diff --seed 1 --only 19555
/// (also PERCENT_RANK values at --scale large, seed 1 test 26).
///
/// The expansion is SQLite's double-double one (3.50, and 3.45 built
/// without long double as tsq's oracle is): 100.0 / 476.42 is ...536 there
/// (...535 with x87 long double).
#[test]
fn real_text_rounds_like_sqlite() -> TestResult {
    assert_query_on(
        ONE,
        "SELECT 443.85 / 5867 AS r, CAST(443.85 / 5867 AS TEXT) AS t, \
         443.85 / 5867 || '' AS c, 100.0 / 476.42 AS d FROM t",
        "r,t,c,d\n0.0756519515936595,0.0756519515936595,0.0756519515936595,0.209898828764536",
    )
}

/// QUOTE of a real whose 15-digit form does not read back as the same value
/// falls back to SQLite's `%!.20e`, which prints the digits SQLite's decoder
/// produces (`6.44491850400551701e-04`). sqawk dropped the trailing digits
/// (`6.444918504005517e-04`). Found by tsq diff --seed 1 --only 17090. The
/// digits are the double-double ones (SQLite 3.50; 3.45 without long double):
/// `...392e+01` and `...3493e+09` where x87 long double gives `...39e+01` and
/// `...349e+09`. "Reads back" is SQLite's own text-to-real reading, which
/// for 1e-300 gives the double that `1.0e-300` gives, so it quotes short.
#[test]
fn quote_real_fallback_digits() -> TestResult {
    assert_query_on(
        ONE,
        "SELECT QUOTE(1 / 1551.61) AS a, QUOTE(17 / 1.5) AS b, QUOTE(1e10 / 3) AS c, \
         QUOTE(1e-300) AS d FROM t",
        "a,b,c,d\n6.44491850400551701e-04,1.133333333333333392e+01,3.333333333333333493e+09,1.0e-300",
    )
}

/// `%` with a real operand takes the remainder of the operands truncated to
/// integers (doc "Arithmetic"), and integers beyond 2^53 stay exact: the
/// integer operand must not pass through a double. sqawk rounded
/// 9007199254740993 to ...992 first (`% 2.0` gave 0.0, not 1.0); an
/// out-of-range real clamps to the i64 limit as in SQLite. Found by tsq diff
/// --seed 2 --only 175.
#[test]
fn remainder_with_real_keeps_integers_exact() -> TestResult {
    assert_query_on(
        ONE,
        "SELECT 9007199254740993 % 2.0 AS a, 9007199254740993 % 10.0 AS b, \
         -9223372036854775793 % -2.67e20 AS c FROM t",
        "a,b,c\n1.0,3.0,-9.22337203685478e+18",
    )
}

/// ROUND judges halves "on the value actually stored" (doc "Math
/// Functions": `ROUND(2.675, 2)` is 2.67). 1058.05 is stored as
/// 1058.04999999999995..., so ROUND(1058.05, 1) is 1058.0 in SQLite; sqawk
/// gave 1058.1. Found by tsq diff --seed 4 --only 2989.
#[test]
fn round_judges_the_stored_value() -> TestResult {
    assert_query_on(
        ONE,
        "SELECT ROUND(1058.05, 1) AS a, ROUND(-1058.05, 1) AS b, ROUND(0.285, 2) AS c FROM t",
        "a,b,c\n1058.0,-1058.0,0.28",
    )
}

// ---------------------------------------------------------------------------
// Performance cliffs. The answers are right; the time is the defect: each
// query finishes in milliseconds in SQLite and takes quadratic time in
// sqawk, so `tsq diff --scale large` reports it as a crash (its 10 s limit).
// The data is sized so the quadratic code needs over a minute (debug build)
// while linear code needs well under a second; the 20 s limit leaves room
// for slow runners without letting the quadratic code pass.
// ---------------------------------------------------------------------------

/// Run `sql` over `files` (written to a temp dir), killed after 20 s, and
/// require the exact stdout.
fn assert_fast(files: &[(&str, String)], sql: &str, expected: &str) -> TestResult {
    let dir = create_temp_dir()?;
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s").arg(sql);
    for (name, content) in files {
        let path = dir.path().join(name);
        std::fs::write(&path, content)?;
        cmd.arg(path);
    }
    let out = cmd.timeout(Duration::from_secs(20)).output()?;
    assert!(
        out.status.success(),
        "sqawk did not finish within 20 s ({})\n  sql: {sql}",
        out.status
    );
    assert_eq!(String::from_utf8_lossy(&out.stdout), expected);
    Ok(())
}

fn csv(header: &str, rows: impl Iterator<Item = String>) -> String {
    let mut s = format!("{header}\n");
    for r in rows {
        s.push_str(&r);
        s.push('\n');
    }
    s
}

/// An EXISTS subquery whose only tie to the outer row is a predicate on the
/// outer columns (`t1.b < t1.c`) must not rescan the inner table for every
/// outer row: SQLite evaluates such a term once per outer row and stops the
/// subquery at its first row; sqawk was O(n^2) (about 95 s for these 12000
/// rows in a debug build, 14 s in a release build; 0.04 s once fixed). Found by tsq diff --scale large --seed 1
/// --only 35.
#[test]
fn exists_with_outer_only_predicate_is_not_quadratic() -> TestResult {
    let r = csv(
        "id,a,b,c",
        (1..=12000).map(|i| format!("{i},{},w{},w{}", i % 7, i % 13, i % 11)),
    );
    assert_fast(
        &[("r.csv", r)],
        "SELECT COUNT(*) FROM r t1 WHERE EXISTS \
         (SELECT 1 FROM r t3 WHERE t3.a < 19 AND t1.b < t1.c)",
        "COUNT(*)\n5962\n",
    )
}

/// A window frame that ends at UNBOUNDED FOLLOWING while its start moves
/// (`ROWS BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING`, `RANGE BETWEEN
/// CURRENT ROW AND UNBOUNDED FOLLOWING`) was recomputed from scratch for
/// every row in sqawk: O(n^2), about 77 s for these 60000 rows in a debug
/// build (0.2 s once fixed) where SQLite takes 0.01 s (it removes rows
/// leaving the frame instead). Found by tsq diff --scale large --seed 7
/// (tests 277 and 392).
#[test]
fn window_frame_to_unbounded_following_is_not_quadratic() -> TestResult {
    let w = csv("id,x", (1..=60000).map(|i| format!("{i},{}", i % 97)));
    assert_fast(
        &[("w.csv", w)],
        "SELECT SUM(c) AS s, MAX(m) AS m FROM (SELECT \
         COUNT(x) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING) AS c, \
         MAX(x) OVER (ORDER BY id RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS m \
         FROM w)",
        "s,m\n1800149997,96\n",
    )
}

/// A WHERE term on the first table of a join that is an `IN (SELECT ...)`
/// was applied only after the whole join was built, while a plain
/// comparison on the same column is applied at that table's loop. Here the join is
/// 40 million rows unfiltered (93 s in a debug build) and 40000 filtered
/// (0.4 s); SQLite takes 0.2 s.
/// Found by tsq diff --scale large --seed 7 --only 233 (and --seed 3
/// --only 12902 at small scale).
#[test]
fn in_subquery_filter_applies_before_the_joins() -> TestResult {
    let p = csv("pid,q", (1..=1000).map(|i| format!("{i},{i}")));
    let it = csv(
        "iid,pid",
        (1..=200000).map(|i| format!("{i},{}", i % 1000 + 1)),
    );
    assert_fast(
        &[("p.csv", p), ("it.csv", it)],
        "SELECT COUNT(*) FROM p t1 JOIN it t2 ON t1.pid = t2.pid \
         JOIN it t3 ON t1.pid = t3.pid WHERE t1.pid IN (SELECT pid FROM p WHERE q = 7)",
        "COUNT(*)\n40000\n",
    )
}

/// A CTE is computed when a query first reads it, and a WHERE term that
/// reads no table is tested before the FROM tables are opened: here `d` is
/// read only by an arm whose WHERE is constant false ('D' sorts before
/// 'abc'), so SQLite never computes `d` -- nor `c`, read only by `d`. sqawk
/// computed every referenced CTE before the statement ran: `c` is 26667 rows
/// and its self-join in `d` 118 million (18 s in a release build, minutes
/// in a debug build); SQLite takes 0.001 s. Found by tsq diff --seed 4 --only
/// 3486 (91 s there).
#[test]
#[ignore]
fn cte_read_only_under_a_false_where_is_never_computed() -> TestResult {
    let t = csv("id,k", (1..=400).map(|i| format!("{i},{}", i % 6)));
    assert_fast(
        &[("t.csv", t)],
        "WITH c AS (SELECT t1.k AS k FROM t t1 JOIN t t2 USING (k)), \
         d(k) AS (SELECT id FROM t INTERSECT SELECT x.k FROM c x JOIN c y USING (k)) \
         SELECT k FROM d WHERE 'D' > 'abc' UNION SELECT COUNT(*) FROM t",
        "k\n400\n",
    )
}
