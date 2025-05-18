//! Tests for UPDATE functionality
//!
//! This file contains tests for the SQL UPDATE statement.

use crate::helpers::{create_temp_dir, prepare_test_file};

use std::fs;

#[test]
fn test_update_with_where() -> Result<(), Box<dyn std::error::Error>> {
    // This test verifies the UPDATE functionality with a WHERE clause
    let temp_dir = create_temp_dir()?;
    let file_path = prepare_test_file(temp_dir.path())?;

    // First verify we have initial data
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM people")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,Alice,32"))
        .stdout(predicates::str::contains("2,Bob,25"))
        .stdout(predicates::str::contains("3,Charlie,35"));

    // Now execute UPDATE with WHERE
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("UPDATE people SET age = 31 WHERE name = 'Alice'")
        .arg("-s")
        .arg("SELECT * FROM people") // Check result after update
        .arg(file_path.to_str().unwrap())
        .arg("-v"); // Verbose mode to see more details

    // Verify update worked correctly
    cmd.assert()
        .success()
        .stderr(predicates::str::contains("Updated 1 rows")) // Should update only Alice
        .stdout(predicates::str::contains("id,name,age"))
        .stdout(predicates::str::contains("1,Alice,31")) // Alice's age should be updated
        .stdout(predicates::str::contains("2,Bob,25"))
        .stdout(predicates::str::contains("3,Charlie,35"));

    // File should NOT be modified by default (without --write flag)
    let content = fs::read_to_string(&file_path)?;
    assert!(content.contains("1,Alice,32")); // Original data should still be there
    assert!(content.contains("2,Bob,25"));
    assert!(content.contains("3,Charlie,35"));

    Ok(())
}

#[test]
fn test_update_all_rows() -> Result<(), Box<dyn std::error::Error>> {
    // This test verifies the UPDATE functionality without a WHERE clause (updates all rows)
    let temp_dir = create_temp_dir()?;
    let file_path = prepare_test_file(temp_dir.path())?;

    // First verify we have initial data
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM people")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("id,name,age"))
        .stdout(predicates::str::contains("1,Alice,32"));

    // Now execute UPDATE without WHERE
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("UPDATE people SET age = 40") // Update all ages to 40
        .arg("-s")
        .arg("SELECT * FROM people") // Check result after update
        .arg(file_path.to_str().unwrap())
        .arg("-v"); // Verbose mode to see more details

    // Verify update worked correctly - all ages should be 40
    cmd.assert()
        .success()
        .stderr(predicates::str::contains("Updated 3 rows")) // Should update all 3 rows
        .stdout(predicates::str::contains("id,name,age"))
        .stdout(predicates::str::contains("1,Alice,40"))
        .stdout(predicates::str::contains("2,Bob,40"))
        .stdout(predicates::str::contains("3,Charlie,40"));

    // File should NOT be modified by default (without --write flag)
    let content = fs::read_to_string(&file_path)?;
    assert!(content.contains("1,Alice,32")); // Original data should still be there
    assert!(content.contains("2,Bob,25"));
    assert!(content.contains("3,Charlie,35"));

    Ok(())
}

#[test]
fn test_update_and_write() -> Result<(), Box<dyn std::error::Error>> {
    // This test verifies that UPDATE with --write flag persists changes to the file
    let temp_dir = create_temp_dir()?;
    let file_path = prepare_test_file(temp_dir.path())?;

    // Execute UPDATE with --write flag
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("UPDATE people SET name = 'Alicia' WHERE name = 'Alice'")
        .arg("-s")
        .arg("SELECT * FROM people") // Check result after update
        .arg("--write") // Add write flag to save changes to file
        .arg(file_path.to_str().unwrap());

    // Verify output shows the update
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("id,name,age"))
        .stdout(predicates::str::contains("1,Alicia,32")) // Name should be updated
        .stdout(predicates::str::contains("2,Bob,25"))
        .stdout(predicates::str::contains("3,Charlie,35"));

    // File SHOULD be modified with --write flag
    let content = fs::read_to_string(&file_path)?;
    assert!(content.contains("1,Alicia,32")); // Alice should be renamed to Alicia
    assert!(!content.contains("1,Alice,32")); // Original name should be gone
    assert!(content.contains("2,Bob,25"));
    assert!(content.contains("3,Charlie,35"));

    Ok(())
}
