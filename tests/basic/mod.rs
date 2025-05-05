//! Basic end-to-end tests for sqawk
//!
//! This file contains fundamental end-to-end tests for the sqawk application.

use crate::helpers::{
    create_custom_csv, create_temp_dir, get_static_sample_file, prepare_test_file, run_test_case,
    run_test_case_with_static_file, SqawkTestCase,
};

use assert_cmd;
use predicates;
use predicates::prelude::PredicateBooleanExt;
use std::fs; // Import trait for .not()

#[test]
fn test_basic_select() -> Result<(), Box<dyn std::error::Error>> {
    let test_case = SqawkTestCase {
        sql: "SELECT * FROM people".to_string(),
        expected_stdout: vec![
            "id,name,age".to_string(),
            "1,Alice,30".to_string(),
            "2,Bob,25".to_string(),
            "3,Charlie,35".to_string(),
        ],
        ..Default::default()
    };

    run_test_case(test_case)
}

#[test]
fn test_filtered_select() -> Result<(), Box<dyn std::error::Error>> {
    let test_case = SqawkTestCase {
        sql: "SELECT name FROM people WHERE age = 30".to_string(),
        expected_stdout: vec!["name".to_string(), "Alice".to_string()],
        verbose: true,
        ..Default::default()
    };

    run_test_case(test_case)
}

#[test]
fn test_insert() -> Result<(), Box<dyn std::error::Error>> {
    // We don't use this test_case directly, so we prefix with _
    let _test_case = SqawkTestCase {
        sql: "INSERT INTO people (id, name, age) VALUES (4, 'Dave', 40)".to_string(),
        args: vec!["-s".to_string(), "SELECT * FROM people".to_string()],
        expected_stdout: vec![
            "id,name,age".to_string(),
            "1,Alice,30".to_string(),
            "2,Bob,25".to_string(),
            "3,Charlie,35".to_string(),
            "4,Dave,40".to_string(),
        ],
        ..Default::default()
    };

    // We need to verify the file was modified, so we'll use a custom test function
    let temp_dir = create_temp_dir()?;
    let file_path = prepare_test_file(temp_dir.path())?;

    // Build the command
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("INSERT INTO people (id, name, age) VALUES (4, 'Dave', 40)")
        .arg("-s")
        .arg("SELECT * FROM people")
        .arg("--write") // Add write flag to save changes to file
        .arg(file_path.to_str().unwrap());

    // Check output
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("id,name,age"))
        .stdout(predicates::str::contains("1,Alice,30"))
        .stdout(predicates::str::contains("2,Bob,25"))
        .stdout(predicates::str::contains("3,Charlie,35"))
        .stdout(predicates::str::contains("4,Dave,40"));

    // Verify the file was modified to include Dave
    let content = fs::read_to_string(&file_path)?;
    assert!(content.contains("4,Dave,40"));

    Ok(())
}

#[test]
fn test_custom_table_name() -> Result<(), Box<dyn std::error::Error>> {
    let test_case = SqawkTestCase {
        sql: "SELECT * FROM custom_table".to_string(),
        table_name: Some("custom_table".to_string()),
        expected_stdout: vec![
            "id,name,age".to_string(),
            "1,Alice,30".to_string(),
            "2,Bob,25".to_string(),
            "3,Charlie,35".to_string(),
        ],
        ..Default::default()
    };

    run_test_case(test_case)
}

#[test]
fn test_invalid_sql() -> Result<(), Box<dyn std::error::Error>> {
    let test_case = SqawkTestCase {
        sql: "INVALID SQL".to_string(),
        expected_stderr: vec!["Failed to execute SQL".to_string()],
        should_succeed: false,
        ..Default::default()
    };

    run_test_case(test_case)
}

#[test]
fn test_multiple_files() -> Result<(), Box<dyn std::error::Error>> {
    // This test demonstrates using multiple files with custom data
    let temp_dir = create_temp_dir()?;

    // Create people.csv file
    let people_file = prepare_test_file(temp_dir.path())?;

    // Create scores.csv file
    let scores_content = "id,score\n1,95\n2,75\n3,85\n";
    let scores_file = create_custom_csv(temp_dir.path(), "scores.csv", scores_content)?;

    // Use direct command execution since our SQL is simpler
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM scores WHERE score > 80")
        .arg(format!("scores={}", scores_file.to_str().unwrap()))
        .arg(format!("people={}", people_file.to_str().unwrap()));

    // Check output
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("id,score"))
        .stdout(predicates::str::contains("1,95"))
        .stdout(predicates::str::contains("3,85"));

    Ok(())
}

#[test]
fn test_where_greater_than() -> Result<(), Box<dyn std::error::Error>> {
    let test_case = SqawkTestCase {
        sql: "SELECT * FROM people WHERE age > 25".to_string(),
        expected_stdout: vec![
            "id,name,age".to_string(),
            "1,Alice,30".to_string(),
            "3,Charlie,35".to_string(),
        ],
        verbose: true,
        ..Default::default()
    };

    run_test_case(test_case)
}

#[test]
fn test_where_less_than() -> Result<(), Box<dyn std::error::Error>> {
    let test_case = SqawkTestCase {
        sql: "SELECT * FROM people WHERE age <= 30".to_string(),
        expected_stdout: vec![
            "id,name,age".to_string(),
            "1,Alice,30".to_string(),
            "2,Bob,25".to_string(),
        ],
        verbose: true,
        ..Default::default()
    };

    run_test_case(test_case)
}

#[test]
fn test_static_file_query() -> Result<(), Box<dyn std::error::Error>> {
    // This test demonstrates using the existing static test file
    // This avoids creating temporary files for simple queries,
    // improving test performance.
    let test_case = SqawkTestCase {
        sql: "SELECT * FROM sample WHERE age > 25".to_string(),
        expected_stdout: vec![
            "id,name,age".to_string(),
            "1,Alice,30".to_string(),
            "3,Charlie,35".to_string(),
        ],
        verbose: true,
        ..Default::default()
    };

    // Use the static sample.csv file
    run_test_case_with_static_file(test_case, get_static_sample_file())
}

#[test]
fn test_delete_with_where() -> Result<(), Box<dyn std::error::Error>> {
    // This test verifies the DELETE functionality with a WHERE clause
    // We need to verify the file was modified, so we'll use a custom test function
    let temp_dir = create_temp_dir()?;
    let file_path = prepare_test_file(temp_dir.path())?;

    // First verify we have 3 rows initially
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM people")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,Alice,30"))
        .stdout(predicates::str::contains("2,Bob,25"))
        .stdout(predicates::str::contains("3,Charlie,35"));

    // Now execute DELETE with WHERE
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("DELETE FROM people WHERE age > 30")
        .arg("-s")
        .arg("SELECT * FROM people") // Check result after deletion
        .arg(file_path.to_str().unwrap())
        .arg("-v") // Verbose mode to see more details
        ; // Prevent file modifications

    // Verify deletion worked correctly
    cmd.assert()
        .success()
        .stderr(predicates::str::contains("Deleted 1 rows")) // Should delete only Charlie
        .stdout(predicates::str::contains("id,name,age"))
        .stdout(predicates::str::contains("1,Alice,30"))
        .stdout(predicates::str::contains("2,Bob,25"))
        .stdout(predicates::str::contains("3,Charlie,35").not());

    // File should NOT be modified by default (without --write flag)
    let content = fs::read_to_string(&file_path)?;
    assert!(content.contains("1,Alice,30"));
    assert!(content.contains("2,Bob,25"));
    assert!(content.contains("3,Charlie,35")); // Charlie should still be there

    Ok(())
}

#[test]
fn test_delete_all() -> Result<(), Box<dyn std::error::Error>> {
    // This test verifies the DELETE functionality without a WHERE clause (deletes all rows)
    let temp_dir = create_temp_dir()?;
    let file_path = prepare_test_file(temp_dir.path())?;

    // First verify we have rows initially
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM people")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("id,name,age"))
        .stdout(predicates::str::contains("1,Alice,30"));

    // Now execute DELETE without WHERE
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("DELETE FROM people")
        .arg("-s")
        .arg("SELECT * FROM people") // Check result after deletion
        .arg(file_path.to_str().unwrap())
        .arg("-v") // Verbose mode to see more details
        ; // Prevent file modifications

    // Verify deletion worked correctly - should only show header, no rows
    cmd.assert()
        .success()
        .stderr(predicates::str::contains("Deleted 3 rows")) // Should delete all 3 rows
        .stdout(predicates::str::contains("id,name,age"))
        .stdout(predicates::str::contains("1,Alice,30").not())
        .stdout(predicates::str::contains("2,Bob,25").not())
        .stdout(predicates::str::contains("3,Charlie,35").not());

    // File should NOT be modified by default (without --write flag)
    let content = fs::read_to_string(&file_path)?;
    assert!(content.contains("id,name,age"));
    assert!(content.contains("1,Alice,30")); // All rows should still be there
    assert!(content.contains("2,Bob,25"));
    assert!(content.contains("3,Charlie,35"));

    Ok(())
}
