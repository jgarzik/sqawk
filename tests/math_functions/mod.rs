//! Math function tests for sqawk VM
//!
//! Tests for ABS, ROUND, CEIL, FLOOR functions.

use crate::helpers::{create_custom_csv, create_temp_dir};

/// Test ABS function with negative value
#[test]
fn test_abs_negative() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,value\n1,-5\n2,10\n3,-15\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT id, ABS(value) AS abs_val FROM data")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,5"))
        .stdout(predicates::str::contains("2,10"))
        .stdout(predicates::str::contains("3,15"));

    Ok(())
}

/// Test ROUND function
#[test]
fn test_round() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,value\n1,3.7\n2,3.2\n3,3.5\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT id, ROUND(value) AS rounded FROM data")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,4"))
        .stdout(predicates::str::contains("2,3"))
        .stdout(predicates::str::contains("3,4"));

    Ok(())
}

/// Test CEIL function
#[test]
fn test_ceil() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,value\n1,3.1\n2,3.9\n3,4.0\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT id, CEIL(value) AS ceiled FROM data")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,4"))
        .stdout(predicates::str::contains("2,4"))
        .stdout(predicates::str::contains("3,4"));

    Ok(())
}

/// Test FLOOR function
#[test]
fn test_floor() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,value\n1,3.1\n2,3.9\n3,4.0\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT id, FLOOR(value) AS floored FROM data")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,3"))
        .stdout(predicates::str::contains("2,3"))
        .stdout(predicates::str::contains("3,4"));

    Ok(())
}

/// ROUND with an explicit precision argument.
///
/// The second argument used to be parsed and then silently dropped, so
/// `ROUND(3.14159, 2)` returned `3` rather than `3.14`.
#[test]
fn test_round_precision() -> Result<(), Box<dyn std::error::Error>> {
    crate::helpers::assert_query(
        &["tests/data/employees.csv"],
        "SELECT ROUND(3.14159, 2) AS a, ROUND(2.71828, 3) AS b, ROUND(1.005, 0) AS c \
         FROM employees LIMIT 1",
        concat!("a,b,c", "\n3.14,2.718,1"),
    )
}

/// A zero or absent precision must keep the original whole-number behaviour.
#[test]
fn test_round_precision_zero_matches_one_arg() -> Result<(), Box<dyn std::error::Error>> {
    crate::helpers::assert_query(
        &["tests/data/employees.csv"],
        "SELECT ROUND(3.7) AS a, ROUND(3.7, 0) AS b FROM employees LIMIT 1",
        concat!("a,b", "\n4,4"),
    )
}

/// Negative precision rounds to the left of the decimal point.
#[test]
fn test_round_negative_precision() -> Result<(), Box<dyn std::error::Error>> {
    crate::helpers::assert_query(
        &["tests/data/employees.csv"],
        "SELECT ROUND(1234.5, -2) AS a FROM employees LIMIT 1",
        concat!("a", "\n1200"),
    )
}

/// Precision applies to a column value, not just a literal.
#[test]
fn test_round_precision_on_column() -> Result<(), Box<dyn std::error::Error>> {
    crate::helpers::assert_query(
        &["tests/data/employees.csv"],
        "SELECT name, ROUND(salary / 7.0, 2) AS share FROM employees LIMIT 3",
        concat!(
            "name,share",
            "\nAlice,10000",
            "\nBob,7857.14",
            "\nCharlie,9285.71",
        ),
    )
}

/// A precision argument is only meaningful for ROUND; the other math
/// functions take exactly one argument and must say so rather than ignore it.
#[test]
fn test_math_func_rejects_extra_argument() -> Result<(), Box<dyn std::error::Error>> {
    crate::helpers::assert_query_fails(
        &["tests/data/employees.csv"],
        "SELECT ABS(-5, 2) FROM employees",
        "ABS takes exactly one argument",
    )
}
