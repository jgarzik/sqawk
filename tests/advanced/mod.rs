//! Tests for advanced SQL features and edge cases in sqawk
//!
//! This module contains tests for complex features and scenarios that are not
//! covered by the focused test modules.

use crate::helpers::{assert_query_on, create_temp_dir};
use predicates::prelude::*;
use std::fs;

/// Test sequential SQL statements and their dependencies
#[test]
fn test_sequential_sql_statements() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let file_path = temp_dir.path().join("people.csv");
    let content = "id,name,age\n1,Alice,30\n2,Bob,25\n3,Charlie,35\n";
    fs::write(&file_path, content)?;

    // Execute multiple statements with sequential dependencies
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("INSERT INTO people (id, name, age) VALUES (4, 'David', 40)")
        .arg("-s")
        .arg("UPDATE people SET age = 41 WHERE name = 'David'")
        .arg("-s")
        .arg("SELECT * FROM people WHERE name = 'David'")
        .arg(file_path.to_str().unwrap())
        .arg("-v");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("4,David,41"));

    Ok(())
}

/// Test NULL value handling with IS NULL and IS NOT NULL operators
#[test]
fn test_is_null_operator() -> Result<(), Box<dyn std::error::Error>> {
    // Create a file with NULL values
    let temp_dir = create_temp_dir()?;
    let file_path = temp_dir.path().join("null_test.csv");

    let content = "id,name,department\n1,Alice,Engineering\n2,,Marketing\n3,Charlie,\n";
    fs::write(&file_path, content)?;

    // Test IS NULL operator
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM null_test WHERE department IS NULL")
        .arg(file_path.to_str().unwrap())
        .arg("-v");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("3,Charlie,"));

    // Test IS NOT NULL operator
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM null_test WHERE name IS NOT NULL")
        .arg(file_path.to_str().unwrap())
        .arg("-v");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("1,Alice,Engineering"))
        .stdout(predicate::str::contains("3,Charlie,"));

    Ok(())
}

/// Employees with NULL salaries and managers, for the three-valued WHERE tests.
const EMP: (&str, &str) = (
    "emp.csv",
    "id,name,dept,salary,mgr\n\
     1,Alice,Eng,100,\n\
     2,Bob,Eng,80,1\n\
     3,Carol,Sales,70,1\n\
     4,Dan,Sales,,3\n\
     5,Eve,Ops,90,\n",
);

/// AND/OR/NOT and parentheses in WHERE: AND binds tighter than OR, NOT
/// tighter than both, and a NULL operand follows the SQL truth tables
/// (expected output computed with SQLite 3.45).
#[test]
fn test_complex_where_expressions() -> Result<(), Box<dyn std::error::Error>> {
    // AND before OR.
    assert_query_on(
        &[EMP],
        "SELECT id, name FROM emp WHERE dept = 'Eng' AND salary > 85 OR dept = 'Ops'",
        "id,name\n1,Alice\n5,Eve",
    )?;
    // Parentheses override it.
    assert_query_on(
        &[EMP],
        "SELECT id, name FROM emp WHERE dept = 'Eng' AND (salary > 85 OR dept = 'Ops')",
        "id,name\n1,Alice",
    )?;
    // NOT (FALSE OR NULL) is NULL, so Dan (NULL salary) is excluded.
    assert_query_on(
        &[EMP],
        "SELECT id, name FROM emp WHERE NOT (dept = 'Eng' OR salary < 75)",
        "id,name\n5,Eve",
    )?;
    // NOT applies to the comparison only, then AND.
    assert_query_on(
        &[EMP],
        "SELECT id, name FROM emp WHERE NOT dept = 'Sales' AND mgr IS NOT NULL",
        "id,name\n2,Bob",
    )?;
    assert_query_on(
        &[EMP],
        "SELECT id, name FROM emp \
         WHERE (salary >= 80 AND NOT mgr IS NULL) OR (dept = 'Sales' AND salary IS NULL)",
        "id,name\n2,Bob\n4,Dan",
    )?;
    // LIKE is case-insensitive: NOT (name LIKE 'c%') excludes Carol.
    assert_query_on(
        &[EMP],
        "SELECT id, name FROM emp WHERE salary > 75 OR mgr = 1 AND NOT (name LIKE 'c%')",
        "id,name\n1,Alice\n2,Bob\n5,Eve",
    )?;
    // The same operators as values: TRUE is 1, FALSE is 0.
    assert_query_on(
        &[EMP],
        "SELECT id, salary > 75 AND mgr IS NULL, NOT (salary < 75) OR mgr = 3 \
         FROM emp ORDER BY id",
        "id,salary > 75 AND mgr IS NULL,NOT (salary < 75) OR mgr = 3\n\
         1,1,1\n\
         2,0,1\n\
         3,0,0\n\
         4,0,1\n\
         5,1,1",
    )
}
