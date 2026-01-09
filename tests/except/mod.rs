//! EXCEPT tests for sqawk VM
//!
//! This file contains tests for SQL EXCEPT set operation.

use crate::helpers::{create_custom_csv, create_temp_dir};

/// Test EXCEPT returns rows in left but not in right
#[test]
fn test_except_basic() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content_a = "id,name\n1,Alice\n2,Bob\n3,Charlie\n";
    let content_b = "id,name\n2,Bob\n3,Charlie\n4,David\n";
    let file_a = create_custom_csv(temp_dir.path(), "a.csv", content_a)?;
    let file_b = create_custom_csv(temp_dir.path(), "b.csv", content_b)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM a EXCEPT SELECT * FROM b")
        .arg("--vm")
        .arg(file_a.to_str().unwrap())
        .arg(file_b.to_str().unwrap());

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8_lossy(&output);

    // EXCEPT should only return Alice (in a but not in b)
    assert!(output_str.contains("1,Alice"), "Alice should be in EXCEPT result");
    assert!(!output_str.contains("2,Bob"), "Bob should not be in EXCEPT result");
    assert!(
        !output_str.contains("3,Charlie"),
        "Charlie should not be in EXCEPT result"
    );
    assert!(
        !output_str.contains("4,David"),
        "David should not be in EXCEPT result"
    );

    Ok(())
}

/// Test EXCEPT with disjoint tables (no common rows)
#[test]
fn test_except_disjoint_tables() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content_a = "id,name\n1,Alice\n2,Bob\n";
    let content_b = "id,name\n3,Charlie\n4,David\n";
    let file_a = create_custom_csv(temp_dir.path(), "a.csv", content_a)?;
    let file_b = create_custom_csv(temp_dir.path(), "b.csv", content_b)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM a EXCEPT SELECT * FROM b")
        .arg("--vm")
        .arg(file_a.to_str().unwrap())
        .arg(file_b.to_str().unwrap());

    // EXCEPT of disjoint tables should return all rows from a
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,Alice"))
        .stdout(predicates::str::contains("2,Bob"));

    Ok(())
}

/// Test EXCEPT with identical tables
#[test]
fn test_except_identical_tables() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name\n1,Alice\n2,Bob\n";
    let file_a = create_custom_csv(temp_dir.path(), "a.csv", content)?;
    let file_b = create_custom_csv(temp_dir.path(), "b.csv", content)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM a EXCEPT SELECT * FROM b")
        .arg("--vm")
        .arg(file_a.to_str().unwrap())
        .arg(file_b.to_str().unwrap());

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8_lossy(&output);

    // EXCEPT of identical tables should return empty result
    assert!(!output_str.contains("Alice"), "Alice should not be in result");
    assert!(!output_str.contains("Bob"), "Bob should not be in result");

    Ok(())
}

/// Test EXCEPT with first table being subset
#[test]
fn test_except_subset_as_first() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content_a = "id,name\n2,Bob\n";
    let content_b = "id,name\n1,Alice\n2,Bob\n3,Charlie\n";
    let file_a = create_custom_csv(temp_dir.path(), "a.csv", content_a)?;
    let file_b = create_custom_csv(temp_dir.path(), "b.csv", content_b)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM a EXCEPT SELECT * FROM b")
        .arg("--vm")
        .arg(file_a.to_str().unwrap())
        .arg(file_b.to_str().unwrap());

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8_lossy(&output);

    // Since Bob is in both, result should be empty
    assert!(!output_str.contains("Bob"), "Bob should not be in result");

    Ok(())
}

/// Test EXCEPT with second table being subset
#[test]
fn test_except_subset_as_second() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content_a = "id,name\n1,Alice\n2,Bob\n3,Charlie\n";
    let content_b = "id,name\n2,Bob\n";
    let file_a = create_custom_csv(temp_dir.path(), "a.csv", content_a)?;
    let file_b = create_custom_csv(temp_dir.path(), "b.csv", content_b)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM a EXCEPT SELECT * FROM b")
        .arg("--vm")
        .arg(file_a.to_str().unwrap())
        .arg(file_b.to_str().unwrap());

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8_lossy(&output);

    // Alice and Charlie should be in result, Bob should not
    assert!(output_str.contains("1,Alice"), "Alice should be in result");
    assert!(output_str.contains("3,Charlie"), "Charlie should be in result");
    assert!(!output_str.contains("2,Bob"), "Bob should not be in result");

    Ok(())
}

/// Test EXCEPT removes duplicates from left side
#[test]
fn test_except_removes_duplicates() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    // Table a has Alice twice
    let content_a = "id,name\n1,Alice\n1,Alice\n2,Bob\n";
    // Table b has Bob
    let content_b = "id,name\n2,Bob\n";
    let file_a = create_custom_csv(temp_dir.path(), "a.csv", content_a)?;
    let file_b = create_custom_csv(temp_dir.path(), "b.csv", content_b)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM a EXCEPT SELECT * FROM b")
        .arg("--vm")
        .arg(file_a.to_str().unwrap())
        .arg(file_b.to_str().unwrap());

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8_lossy(&output);

    // Alice should appear only once (EXCEPT deduplicates)
    let alice_count = output_str.matches("1,Alice").count();
    assert_eq!(alice_count, 1, "Alice should appear only once in EXCEPT result");
    assert!(!output_str.contains("2,Bob"), "Bob should not be in result");

    Ok(())
}
