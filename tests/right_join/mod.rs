//! RIGHT JOIN tests for sqawk VM
//!
//! This file contains tests for SQL RIGHT OUTER JOIN operations.

use crate::helpers::{create_custom_csv, create_temp_dir};

/// Test RIGHT JOIN returns all rows from right table with matches
#[test]
fn test_right_join_with_matches() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content_a = "id,name\n1,Alice\n2,Bob\n";
    let content_b = "id,dept\n1,Engineering\n2,Sales\n";
    let file_a = create_custom_csv(temp_dir.path(), "a.csv", content_a)?;
    let file_b = create_custom_csv(temp_dir.path(), "b.csv", content_b)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM a RIGHT JOIN b ON a.id = b.id")
        .arg(file_a.to_str().unwrap())
        .arg(file_b.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,Alice,1,Engineering"))
        .stdout(predicates::str::contains("2,Bob,2,Sales"));

    Ok(())
}

/// Test RIGHT JOIN returns NULL for non-matching rows
#[test]
fn test_right_join_with_nulls() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content_a = "id,name\n1,Alice\n2,Bob\n";
    let content_b = "id,dept\n1,Engineering\n2,Sales\n3,Marketing\n";
    let file_a = create_custom_csv(temp_dir.path(), "a.csv", content_a)?;
    let file_b = create_custom_csv(temp_dir.path(), "b.csv", content_b)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM a RIGHT JOIN b ON a.id = b.id")
        .arg(file_a.to_str().unwrap())
        .arg(file_b.to_str().unwrap());

    // Marketing (id=3) has no match in left table, so left columns should be NULL
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,Alice,1,Engineering"))
        .stdout(predicates::str::contains("2,Bob,2,Sales"))
        .stdout(predicates::str::contains("NULL,NULL,3,Marketing"));

    Ok(())
}

/// Test RIGHT JOIN with all non-matching rows
#[test]
fn test_right_join_all_nulls() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content_a = "id,name\n1,Alice\n2,Bob\n";
    let content_b = "id,dept\n3,Engineering\n4,Sales\n";
    let file_a = create_custom_csv(temp_dir.path(), "a.csv", content_a)?;
    let file_b = create_custom_csv(temp_dir.path(), "b.csv", content_b)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM a RIGHT JOIN b ON a.id = b.id")
        .arg(file_a.to_str().unwrap())
        .arg(file_b.to_str().unwrap());

    // No matches, all rows should have NULL for left columns
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("NULL,NULL,3,Engineering"))
        .stdout(predicates::str::contains("NULL,NULL,4,Sales"));

    Ok(())
}

/// Test RIGHT OUTER JOIN syntax (equivalent to RIGHT JOIN)
#[test]
fn test_right_outer_join_syntax() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content_a = "id,name\n1,Alice\n";
    let content_b = "id,dept\n1,Engineering\n";
    let file_a = create_custom_csv(temp_dir.path(), "a.csv", content_a)?;
    let file_b = create_custom_csv(temp_dir.path(), "b.csv", content_b)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM a RIGHT OUTER JOIN b ON a.id = b.id")
        .arg(file_a.to_str().unwrap())
        .arg(file_b.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,Alice,1,Engineering"));

    Ok(())
}

/// Test RIGHT JOIN with empty left table
#[test]
fn test_right_join_empty_left() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content_a = "id,name\n";
    let content_b = "id,dept\n1,Engineering\n2,Sales\n";
    let file_a = create_custom_csv(temp_dir.path(), "a.csv", content_a)?;
    let file_b = create_custom_csv(temp_dir.path(), "b.csv", content_b)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM a RIGHT JOIN b ON a.id = b.id")
        .arg(file_a.to_str().unwrap())
        .arg(file_b.to_str().unwrap());

    // All right rows should be returned with NULL for left columns
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("NULL,NULL,1,Engineering"))
        .stdout(predicates::str::contains("NULL,NULL,2,Sales"));

    Ok(())
}
