//! LEFT JOIN tests for sqawk VM
//!
//! This file contains tests for SQL LEFT OUTER JOIN operations.

use crate::helpers::{create_custom_csv, create_temp_dir};

/// Test LEFT JOIN returns all rows from left table with matches
#[test]
fn test_left_join_with_matches() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content_a = "id,name\n1,Alice\n2,Bob\n";
    let content_b = "id,dept\n1,Engineering\n2,Sales\n";
    let file_a = create_custom_csv(temp_dir.path(), "a.csv", content_a)?;
    let file_b = create_custom_csv(temp_dir.path(), "b.csv", content_b)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM a LEFT JOIN b ON a.id = b.id")
        .arg(file_a.to_str().unwrap())
        .arg(file_b.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,Alice,1,Engineering"))
        .stdout(predicates::str::contains("2,Bob,2,Sales"));

    Ok(())
}

/// Test LEFT JOIN returns NULL for non-matching rows
#[test]
fn test_left_join_with_nulls() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content_a = "id,name\n1,Alice\n2,Bob\n3,Charlie\n";
    let content_b = "id,dept\n1,Engineering\n2,Sales\n";
    let file_a = create_custom_csv(temp_dir.path(), "a.csv", content_a)?;
    let file_b = create_custom_csv(temp_dir.path(), "b.csv", content_b)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM a LEFT JOIN b ON a.id = b.id")
        .arg(file_a.to_str().unwrap())
        .arg(file_b.to_str().unwrap());

    // Charlie (id=3) has no match, so right columns should be NULL
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,Alice,1,Engineering"))
        .stdout(predicates::str::contains("2,Bob,2,Sales"))
        .stdout(predicates::str::contains("3,Charlie,,"));

    Ok(())
}

/// Test LEFT JOIN with all non-matching rows
#[test]
fn test_left_join_all_nulls() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content_a = "id,name\n1,Alice\n2,Bob\n";
    let content_b = "id,dept\n3,Engineering\n4,Sales\n";
    let file_a = create_custom_csv(temp_dir.path(), "a.csv", content_a)?;
    let file_b = create_custom_csv(temp_dir.path(), "b.csv", content_b)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM a LEFT JOIN b ON a.id = b.id")
        .arg(file_a.to_str().unwrap())
        .arg(file_b.to_str().unwrap());

    // No matches, all rows should have NULL for right columns
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,Alice,,"))
        .stdout(predicates::str::contains("2,Bob,,"));

    Ok(())
}

/// Test LEFT OUTER JOIN syntax (equivalent to LEFT JOIN)
#[test]
fn test_left_outer_join_syntax() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content_a = "id,name\n1,Alice\n";
    let content_b = "id,dept\n1,Engineering\n";
    let file_a = create_custom_csv(temp_dir.path(), "a.csv", content_a)?;
    let file_b = create_custom_csv(temp_dir.path(), "b.csv", content_b)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM a LEFT OUTER JOIN b ON a.id = b.id")
        .arg(file_a.to_str().unwrap())
        .arg(file_b.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,Alice,1,Engineering"));

    Ok(())
}

/// Test LEFT JOIN with empty right table
#[test]
fn test_left_join_empty_right() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content_a = "id,name\n1,Alice\n2,Bob\n";
    let content_b = "id,dept\n";
    let file_a = create_custom_csv(temp_dir.path(), "a.csv", content_a)?;
    let file_b = create_custom_csv(temp_dir.path(), "b.csv", content_b)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM a LEFT JOIN b ON a.id = b.id")
        .arg(file_a.to_str().unwrap())
        .arg(file_b.to_str().unwrap());

    // All left rows should be returned with NULL for right columns
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,Alice,,"))
        .stdout(predicates::str::contains("2,Bob,,"));

    Ok(())
}
