//! Arithmetic operator tests for sqawk VM
//!
//! Tests for +, -, *, /, % operators.

use crate::helpers::{create_custom_csv, create_temp_dir};

/// Test addition operator
#[test]
fn test_addition() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,a,b\n1,10,5\n2,20,3\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT a + b AS sum FROM data")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("15"))
        .stdout(predicates::str::contains("23"));

    Ok(())
}

/// Test subtraction operator
#[test]
fn test_subtraction() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,a,b\n1,10,5\n2,20,3\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT a - b AS diff FROM data")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("5"))
        .stdout(predicates::str::contains("17"));

    Ok(())
}

/// Test multiplication operator
#[test]
fn test_multiplication() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,a,b\n1,4,5\n2,3,7\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT a * b AS product FROM data")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("20"))
        .stdout(predicates::str::contains("21"));

    Ok(())
}

/// Test division operator
#[test]
fn test_division() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,a,b\n1,20,4\n2,15,3\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT a / b AS quotient FROM data")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("5"))
        .stdout(predicates::str::contains("5"));

    Ok(())
}

/// Test modulo operator
#[test]
fn test_modulo() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,a,b\n1,10,3\n2,17,5\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT a % b AS remainder FROM data")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1"))
        .stdout(predicates::str::contains("2"));

    Ok(())
}

/// Test unary minus
#[test]
fn test_unary_minus() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,value\n1,5\n2,-3\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT -value AS negated FROM data")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("-5"))
        .stdout(predicates::str::contains("3"));

    Ok(())
}
