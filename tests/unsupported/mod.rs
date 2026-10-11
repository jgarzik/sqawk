//! Syntax that parses but that sqawk does not implement must be an error.
//!
//! sqlparser accepts many dialects, so a statement can carry clauses the
//! compiler never reads. Ignoring one is worse than failing: `DELETE ...
//! USING` used to delete every row, `INSERT OR REPLACE` to duplicate keys,
//! `WITH` to report its CTE as a missing table. Each test below names one
//! category of such clause (see `src/vm/compiler_check.rs`); every one must
//! fail with "not supported" and leave the data untouched.

use crate::helpers::{assert_query_fails, create_temp_dir, run_sql};
use std::fs;

type R = Result<(), Box<dyn std::error::Error>>;

const EMP: &str = "id,name,dept,salary\n1,Alice,Eng,100\n2,Bob,Eng,80\n3,Carol,Sales,70\n";
const ONE: &str = "id\n1\n";

/// Run each statement with `--write` against fresh copies of the fixtures;
/// each must fail with "not supported" and leave both files unchanged.
fn assert_rejected(sqls: &[&str]) -> R {
    for sql in sqls {
        let dir = create_temp_dir()?;
        let emp = dir.path().join("emp.csv");
        let one = dir.path().join("one.csv");
        fs::write(&emp, EMP)?;
        fs::write(&one, ONE)?;
        let (stdout, stderr, ok) = run_sql(
            sql,
            &[emp.to_str().unwrap(), one.to_str().unwrap()],
            &["--write"],
        )?;
        assert!(!ok, "accepted: {sql}\n  stdout: {stdout}");
        assert!(
            stderr.contains("not supported"),
            "wrong error for: {sql}\n  stderr: {stderr}"
        );
        assert_eq!(fs::read_to_string(&emp)?, EMP, "emp modified by: {sql}");
        assert_eq!(fs::read_to_string(&one)?, ONE, "one modified by: {sql}");
    }
    Ok(())
}

#[test]
fn insert_conflict_and_returning_clauses() -> R {
    assert_rejected(&[
        "INSERT OR REPLACE INTO emp VALUES (1, 'x', 'y', 1)",
        "INSERT INTO emp VALUES (9, 'x', 'y', 1) RETURNING id",
        "INSERT INTO emp VALUES (1, 'x', 'y', 1) ON CONFLICT DO NOTHING",
        "INSERT OVERWRITE TABLE emp SELECT * FROM emp",
        "INSERT INTO emp VALUES (9, 'x', 'y', 1) LIMIT 1",
    ])
}

#[test]
fn update_clauses() -> R {
    assert_rejected(&[
        "UPDATE emp SET salary = 1 RETURNING id",
        "UPDATE emp SET salary = 1 FROM one WHERE emp.id = one.id",
        "UPDATE emp SET salary = 1 LIMIT 1",
    ])
}

#[test]
fn delete_clauses() -> R {
    assert_rejected(&[
        "DELETE FROM emp RETURNING id",
        "DELETE FROM emp USING one WHERE emp.id = one.id",
        "DELETE emp FROM emp JOIN one ON emp.id = one.id",
        "DELETE FROM emp ORDER BY id LIMIT 1",
        "DELETE FROM emp LIMIT 1",
    ])
}

/// Query-level clauses besides ORDER BY / LIMIT / OFFSET.
#[test]
fn query_clauses() -> R {
    assert_rejected(&[
        "WITH h AS (SELECT * FROM emp) SELECT id FROM h",
        "SELECT id FROM emp FETCH FIRST 2 ROWS ONLY",
        "SELECT id FROM emp FOR UPDATE",
        "SELECT id FROM emp WHERE id IN (WITH h AS (SELECT 1) SELECT * FROM h)",
    ])
}

/// SELECT-core clauses from other dialects (several are Hive's own).
#[test]
fn select_clauses() -> R {
    assert_rejected(&[
        "SELECT TOP 2 id FROM emp",
        "SELECT id FROM emp QUALIFY ROW_NUMBER() OVER (ORDER BY id) = 1",
        "SELECT id FROM emp SORT BY id",
        "SELECT id FROM emp CLUSTER BY id",
        "SELECT id FROM emp DISTRIBUTE BY id",
        "SELECT id, x FROM emp LATERAL VIEW explode(id) t AS x",
        "SELECT id INTO newt FROM emp",
        "SELECT dept, COUNT(*) FROM emp GROUP BY dept WITH ROLLUP",
    ])
}

#[test]
fn from_item_clauses() -> R {
    assert_rejected(&[
        "SELECT id FROM emp TABLESAMPLE (10 PERCENT)",
        "SELECT id FROM emp WITH (NOLOCK)",
        "SELECT id FROM emp e, LATERAL (SELECT 1) x",
    ])
}

/// Call syntax a scalar function or an aggregate cannot honour.
#[test]
fn function_call_clauses() -> R {
    assert_rejected(&[
        "SELECT UPPER(name) FILTER (WHERE id > 1) FROM emp",
        "SELECT UPPER(DISTINCT name) FROM emp",
        "SELECT MAX(name) IGNORE NULLS FROM emp",
        "SELECT GROUP_CONCAT(name ORDER BY id) FROM emp",
        "SELECT MAX(salary) WITHIN GROUP (ORDER BY salary) FROM emp",
        "SELECT SUM(salary) FILTER (WHERE id > 1) OVER () FROM emp",
    ])
}

/// Expression forms: CAST ... FORMAT, LIKE ANY, blob and other literals.
#[test]
fn expression_forms() -> R {
    assert_rejected(&[
        "SELECT CAST(name AS TEXT FORMAT 'x') FROM emp",
        "SELECT id FROM emp WHERE name LIKE ANY ('a%', 'b%')",
        "SELECT X'00' FROM one",
    ])
}

/// sqawk enforces no constraints, so declaring one is an error.
#[test]
fn create_table_clauses() -> R {
    assert_rejected(&[
        "CREATE TABLE z (id INTEGER PRIMARY KEY)",
        "CREATE TABLE z (id INTEGER NOT NULL)",
        "CREATE TABLE z (id INTEGER, PRIMARY KEY (id))",
        "CREATE TABLE IF NOT EXISTS z (id INTEGER)",
        "CREATE TABLE z (id INTEGER) PARTITIONED BY (p INT)",
        "CREATE TABLE z (id INTEGER) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','",
    ])
}

/// The red-list case: `UPDATE ... FROM` used to update every row from
/// nothing; the documentation said it was "accepted without effect".
#[test]
fn update_from_message() -> R {
    assert_query_fails(
        &["tests/data/users.csv", "tests/data/orders.csv"],
        "UPDATE users SET name = 'X' FROM orders WHERE users.id = 1",
        "UPDATE ... FROM is not supported",
    )
}

/// An unsupported statement is named by its keywords (the error used to
/// dump the statement's whole syntax tree).
#[test]
fn unsupported_statement_message() -> R {
    assert_query_fails(
        &["tests/data/users.csv"],
        "CREATE VIEW v AS SELECT * FROM users",
        "CREATE VIEW statements are not supported",
    )
}
