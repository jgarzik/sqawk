//! CASE WHEN expression tests for sqawk VM
//!
//! This file contains tests for SQL CASE WHEN expressions.

use crate::helpers::{create_custom_csv, create_temp_dir};

/// Test simple CASE with integer conditions
#[test]
fn test_case_simple_integers() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,status\n1,1\n2,2\n3,3\n4,1\n";
    let file_path = create_custom_csv(temp_dir.path(), "records.csv", content)?;

    // Test simple CASE expression (CASE status WHEN 1 THEN 'active' ...)
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM records WHERE CASE status WHEN 1 THEN 1 WHEN 2 THEN 0 ELSE 0 END = 1")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,1"))
        .stdout(predicates::str::contains("4,1"));

    Ok(())
}

/// Test searched CASE with comparison conditions
#[test]
fn test_case_searched_greater_than() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,value\n1,10\n2,50\n3,100\n4,25\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    // Test searched CASE expression (CASE WHEN value > 50 THEN ...)
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM data WHERE CASE WHEN value > 50 THEN 1 ELSE 0 END = 1")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("3,100"));

    Ok(())
}

/// Test CASE with multiple WHEN branches
#[test]
fn test_case_multiple_when_branches() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,grade\n1,95\n2,85\n3,75\n4,65\n5,55\n";
    let file_path = create_custom_csv(temp_dir.path(), "grades.csv", content)?;

    // Test CASE with multiple WHEN branches - filter for A grades (>= 90)
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM grades WHERE CASE WHEN grade > 89 THEN 1 WHEN grade > 79 THEN 0 ELSE 0 END = 1")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,95"));

    Ok(())
}

/// Test CASE with ELSE clause
#[test]
fn test_case_with_else() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,type\n1,premium\n2,standard\n3,basic\n4,premium\n";
    let file_path = create_custom_csv(temp_dir.path(), "accounts.csv", content)?;

    // Test CASE where ELSE applies - non-premium accounts
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM accounts WHERE CASE type WHEN 'premium' THEN 0 ELSE 1 END = 1")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("2,standard"))
        .stdout(predicates::str::contains("3,basic"));

    Ok(())
}

/// Test CASE with no matches going to ELSE
#[test]
fn test_case_no_when_match_uses_else() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,code\n1,X\n2,Y\n3,Z\n";
    let file_path = create_custom_csv(temp_dir.path(), "codes.csv", content)?;

    // No code matches A or B, so all go to ELSE (1)
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM codes WHERE CASE code WHEN 'A' THEN 0 WHEN 'B' THEN 0 ELSE 1 END = 1")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,X"))
        .stdout(predicates::str::contains("2,Y"))
        .stdout(predicates::str::contains("3,Z"));

    Ok(())
}

/// Test CASE with equality condition
#[test]
fn test_case_searched_equality() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,status\n1,active\n2,pending\n3,inactive\n4,active\n";
    let file_path = create_custom_csv(temp_dir.path(), "items.csv", content)?;

    // Test searched CASE with equality
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM items WHERE CASE WHEN status = 'active' THEN 1 ELSE 0 END = 1")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,active"))
        .stdout(predicates::str::contains("4,active"));

    Ok(())
}
