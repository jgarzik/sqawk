//! Exact-output tests for the FROM planner: join chains, outer joins with
//! WHERE, USING / NATURAL, and hash-join key semantics.
//!
//! Expected outputs were computed by loading the same data into SQLite
//! (python3 `sqlite3`). NULL prints as an empty field.

use crate::helpers::{assert_query, assert_query_fails, create_temp_dir};

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// `lt` and `rt` share keys 1 and 3 (3 twice in `rt`); 2 is only in `lt`, 4
/// only in `rt`, and each has a row with a NULL key.
const LT: &str = "k,a\n1,10\n2,20\n3,30\n,40\n";
const RT: &str = "k,b\n1,100\n3,300\n3,301\n4,400\n,999\n";
const XT: &str = "k,c\n1,one\n2,two\n5,five\n";
/// Keys spelled as decimals: `1.0` must equal `lt`'s integer 1.
const FT: &str = "k,f\n1.0,x\n3.5,y\n";
/// Shares no key with `lt`.
const NM: &str = "k,z\n7,q\n8,r\n";

fn assert_on(sql: &str, expected: &str) -> TestResult {
    let dir = create_temp_dir()?;
    let mut paths = Vec::new();
    for (name, content) in [
        ("lt.csv", LT),
        ("rt.csv", RT),
        ("xt.csv", XT),
        ("ft.csv", FT),
        ("nm.csv", NM),
    ] {
        let p = dir.path().join(name);
        std::fs::write(&p, content)?;
        paths.push(p.to_str().unwrap().to_string());
    }
    let refs: Vec<&str> = paths.iter().map(String::as_str).collect();
    assert_query(&refs, sql, expected)
}

fn assert_on_fails(sql: &str, expected_err: &str) -> TestResult {
    let dir = create_temp_dir()?;
    let mut paths = Vec::new();
    for (name, content) in [("lt.csv", LT), ("rt.csv", RT), ("xt.csv", XT)] {
        let p = dir.path().join(name);
        std::fs::write(&p, content)?;
        paths.push(p.to_str().unwrap().to_string());
    }
    let refs: Vec<&str> = paths.iter().map(String::as_str).collect();
    assert_query_fails(&refs, sql, expected_err)
}

#[test]
fn three_table_left_join_chain() -> TestResult {
    assert_on(
        "SELECT lt.k AS k, rt.b AS b, xt.c AS c FROM lt LEFT JOIN rt ON lt.k = rt.k \
         LEFT JOIN xt ON lt.k = xt.k ORDER BY lt.a, rt.b",
        "k,b,c\n1,100,one\n2,,two\n3,300,\n3,301,\n,,",
    )
}

#[test]
fn left_join_chain_on_null_extended_side() -> TestResult {
    // The second LEFT JOIN keys on the first one's right side, which is NULL
    // for unmatched rows: those rows still appear, NULL-extended again.
    assert_on(
        "SELECT lt.a AS a, rt.b AS b, xt.c AS c FROM lt LEFT JOIN rt ON lt.k = rt.k \
         LEFT JOIN xt ON xt.k = rt.k ORDER BY lt.a, rt.b",
        "a,b,c\n10,100,one\n20,,\n30,300,\n30,301,\n40,,",
    )
}

#[test]
fn left_join_then_inner_join() -> TestResult {
    assert_on(
        "SELECT lt.a AS a, rt.b AS b, xt.c AS c FROM lt LEFT JOIN rt ON lt.k = rt.k \
         JOIN xt ON xt.k = lt.k ORDER BY lt.a, rt.b",
        "a,b,c\n10,100,one\n20,,two",
    )
}

#[test]
fn full_join_with_where_on_both_sides() -> TestResult {
    // WHERE applies after the FULL JOIN, to matched and NULL-extended rows on
    // both sides alike.
    assert_on(
        "SELECT lt.a AS a, rt.b AS b FROM lt FULL JOIN rt ON lt.k = rt.k \
         WHERE lt.a > 20 OR rt.b > 300 OR lt.a IS NULL ORDER BY rt.b, lt.a",
        "a,b\n40,\n30,300\n30,301\n,400\n,999",
    )
}

#[test]
fn right_join_with_inner_joined_left_side() -> TestResult {
    // `(lt JOIN xt) RIGHT JOIN rt`: a right row matches only a combined
    // lt+xt row, otherwise both are NULL.
    assert_on(
        "SELECT lt.a AS a, rt.b AS b, xt.c AS c FROM lt JOIN xt ON lt.k = xt.k \
         RIGHT JOIN rt ON rt.k = lt.k ORDER BY rt.b",
        "a,b,c\n10,100,one\n,300,\n,301,\n,400,\n,999,",
    )
}

#[test]
fn using_select_star_merges_the_column() -> TestResult {
    assert_on(
        "SELECT * FROM lt JOIN rt USING (k) ORDER BY b",
        "k,a,b\n1,10,100\n3,30,300\n3,30,301",
    )
}

#[test]
fn full_join_using_coalesces_the_column() -> TestResult {
    // The merged `k` takes the right side's value on rows only `rt` has.
    assert_on(
        "SELECT * FROM lt FULL JOIN rt USING (k) ORDER BY a, b",
        "k,a,b\n4,,400\n,,999\n1,10,100\n2,20,\n3,30,300\n3,30,301\n,40,",
    )
}

#[test]
fn natural_left_join() -> TestResult {
    assert_on(
        "SELECT * FROM lt NATURAL LEFT JOIN xt ORDER BY a",
        "k,a,c\n1,10,one\n2,20,two\n3,30,\n,40,",
    )
}

#[test]
fn unqualified_column_in_both_tables_is_ambiguous() -> TestResult {
    assert_on_fails("SELECT k FROM lt JOIN rt ON lt.k = rt.k", "ambiguous")
}

#[test]
fn on_clause_cannot_reference_a_later_table() -> TestResult {
    assert_on_fails(
        "SELECT lt.a FROM lt JOIN rt ON rt.k = xt.k JOIN xt ON xt.k = lt.k",
        "joined later",
    )
}

#[test]
fn right_join_after_outer_join_is_unsupported() -> TestResult {
    assert_on_fails(
        "SELECT lt.a FROM lt LEFT JOIN rt ON lt.k = rt.k RIGHT JOIN xt ON xt.k = lt.k",
        "not supported",
    )
}

#[test]
fn hash_join_with_duplicate_keys_on_both_sides() -> TestResult {
    // Key 3 appears twice in `rt`, and the self-join doubles it again: every
    // pairing must be produced.
    assert_on(
        "SELECT lt.a AS a, rt.b AS b FROM lt JOIN rt ON lt.k = rt.k \
         JOIN rt AS r2 ON r2.k = rt.k ORDER BY lt.a, rt.b",
        "a,b\n10,100\n30,300\n30,300\n30,301\n30,301",
    )
}

#[test]
fn hash_join_null_keys_never_match() -> TestResult {
    // Both tables have a NULL-key row; NULL = NULL is not true, so `lt`'s
    // row survives the LEFT JOIN only NULL-extended.
    assert_on(
        "SELECT lt.a AS a, rt.b AS b FROM lt LEFT JOIN rt ON lt.k = rt.k ORDER BY lt.a, rt.b",
        "a,b\n10,100\n20,\n30,300\n30,301\n40,",
    )
}

#[test]
fn hash_join_integer_key_matches_decimal_text_key() -> TestResult {
    assert_on(
        "SELECT lt.a AS a, ft.f AS f FROM lt JOIN ft ON lt.k = ft.k ORDER BY lt.a",
        "a,f\n10,x",
    )
}

#[test]
fn hash_join_integer_key_matches_float_key() -> TestResult {
    // The probe key `lt.k + 0.0` is a Float; 1.0 must find the Integer 1.
    assert_on(
        "SELECT lt.a AS a, rt.b AS b FROM lt JOIN rt ON rt.k = lt.k + 0.0 ORDER BY lt.a, rt.b",
        "a,b\n10,100\n30,300\n30,301",
    )
}

#[test]
fn left_hash_join_with_no_matches() -> TestResult {
    assert_on(
        "SELECT lt.a AS a, nm.z AS z FROM lt LEFT JOIN nm ON lt.k = nm.k ORDER BY lt.a",
        "a,z\n10,\n20,\n30,\n40,",
    )
}

#[test]
fn group_by_over_left_join() -> TestResult {
    assert_on(
        "SELECT lt.k AS k, COUNT(rt.b) AS n FROM lt LEFT JOIN rt ON lt.k = rt.k \
         GROUP BY lt.k ORDER BY lt.k",
        "k,n\n,0\n1,1\n2,0\n3,2",
    )
}
