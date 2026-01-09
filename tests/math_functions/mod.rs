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

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
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

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
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

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
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

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
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
