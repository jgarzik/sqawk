//! Tests for output functionality
//!
//! This module tests both stdout output and file writeback behavior,
//! including the atomic write pattern (temp file + rename).

use crate::helpers::{create_custom_csv, create_temp_dir};
use std::fs;

// ============================================================================
// STDOUT OUTPUT TESTS
// ============================================================================

#[test]
fn test_stdout_select_csv() -> Result<(), Box<dyn std::error::Error>> {
    // Test that SELECT output goes to stdout in CSV format
    let temp_dir = create_temp_dir()?;
    let file_path = create_custom_csv(
        temp_dir.path(),
        "data.csv",
        "id,name,value\n1,Alice,100\n2,Bob,200\n",
    )?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM data")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("id,name,value"))
        .stdout(predicates::str::contains("1,Alice,100"))
        .stdout(predicates::str::contains("2,Bob,200"));

    Ok(())
}

#[test]
fn test_stdout_select_with_delimiter() -> Result<(), Box<dyn std::error::Error>> {
    // Test that SELECT output uses the correct delimiter for TSV files
    let temp_dir = create_temp_dir()?;
    let file_path = create_custom_csv(
        temp_dir.path(),
        "data.tsv",
        "id\tname\tvalue\n1\tAlice\t100\n2\tBob\t200\n",
    )?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-F")
        .arg("\t")
        .arg("-s")
        .arg("SELECT * FROM data")
        .arg(file_path.to_str().unwrap());

    // Output should be tab-delimited
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("id\tname\tvalue"))
        .stdout(predicates::str::contains("1\tAlice\t100"))
        .stdout(predicates::str::contains("2\tBob\t200"));

    Ok(())
}

#[test]
fn test_stdout_empty_result() -> Result<(), Box<dyn std::error::Error>> {
    // Test that queries with no matching rows still output headers
    let temp_dir = create_temp_dir()?;
    let file_path = create_custom_csv(
        temp_dir.path(),
        "data.csv",
        "id,name,value\n1,Alice,100\n2,Bob,200\n",
    )?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM data WHERE id = 999")
        .arg(file_path.to_str().unwrap());

    // Should succeed with just headers (no data rows match)
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("id,name,value"));

    Ok(())
}

// ============================================================================
// FILE OUTPUT TESTS (ATOMIC WRITEBACK)
// ============================================================================

#[test]
fn test_file_write_csv() -> Result<(), Box<dyn std::error::Error>> {
    // Test that --write flag correctly saves CSV modifications to file
    let temp_dir = create_temp_dir()?;
    let file_path = create_custom_csv(
        temp_dir.path(),
        "data.csv",
        "id,name,value\n1,Alice,100\n2,Bob,200\n",
    )?;

    // Update a value and write back
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("UPDATE data SET value = 150 WHERE name = 'Alice'")
        .arg("--write")
        .arg(file_path.to_str().unwrap());

    cmd.assert().success();

    // Verify file was updated
    let content = fs::read_to_string(&file_path)?;
    assert!(
        content.contains("1,Alice,150"),
        "File should contain updated value"
    );
    assert!(
        !content.contains("1,Alice,100"),
        "File should not contain old value"
    );
    assert!(
        content.contains("2,Bob,200"),
        "Other rows should be unchanged"
    );

    Ok(())
}

#[test]
fn test_file_write_tsv() -> Result<(), Box<dyn std::error::Error>> {
    // Test that --write flag correctly saves TSV modifications to file
    let temp_dir = create_temp_dir()?;
    let file_path = create_custom_csv(
        temp_dir.path(),
        "data.tsv",
        "id\tname\tvalue\n1\tAlice\t100\n2\tBob\t200\n",
    )?;

    // Update a value and write back
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-F")
        .arg("\t")
        .arg("-s")
        .arg("UPDATE data SET value = 150 WHERE name = 'Alice'")
        .arg("--write")
        .arg(file_path.to_str().unwrap());

    cmd.assert().success();

    // Verify file was updated with tab delimiter preserved
    let content = fs::read_to_string(&file_path)?;
    assert!(
        content.contains("1\tAlice\t150"),
        "File should contain updated value with tabs"
    );
    assert!(
        !content.contains("1\tAlice\t100"),
        "File should not contain old value"
    );
    assert!(
        content.contains("2\tBob\t200"),
        "Other rows should be unchanged"
    );

    Ok(())
}

#[test]
fn test_file_write_insert() -> Result<(), Box<dyn std::error::Error>> {
    // Test INSERT with --write correctly persists new rows
    let temp_dir = create_temp_dir()?;
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", "id,name,value\n1,Alice,100\n")?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("INSERT INTO data (id, name, value) VALUES (2, 'Bob', 200)")
        .arg("--write")
        .arg(file_path.to_str().unwrap());

    cmd.assert().success();

    // Verify file contains the new row
    let content = fs::read_to_string(&file_path)?;
    assert!(content.contains("1,Alice,100"), "Original row should exist");
    assert!(content.contains("2,Bob,200"), "New row should be added");

    Ok(())
}

#[test]
fn test_file_write_delete() -> Result<(), Box<dyn std::error::Error>> {
    // Test DELETE with --write correctly removes rows from file
    let temp_dir = create_temp_dir()?;
    let file_path = create_custom_csv(
        temp_dir.path(),
        "data.csv",
        "id,name,value\n1,Alice,100\n2,Bob,200\n3,Charlie,300\n",
    )?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("DELETE FROM data WHERE name = 'Bob'")
        .arg("--write")
        .arg(file_path.to_str().unwrap());

    cmd.assert().success();

    // Verify Bob was deleted
    let content = fs::read_to_string(&file_path)?;
    assert!(content.contains("1,Alice,100"), "Alice should remain");
    assert!(!content.contains("2,Bob,200"), "Bob should be deleted");
    assert!(content.contains("3,Charlie,300"), "Charlie should remain");

    Ok(())
}

#[test]
fn test_file_no_write_by_default() -> Result<(), Box<dyn std::error::Error>> {
    // Test that modifications are NOT written without --write flag
    let temp_dir = create_temp_dir()?;
    let original_content = "id,name,value\n1,Alice,100\n2,Bob,200\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", original_content)?;

    // Update without --write flag
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("UPDATE data SET value = 999 WHERE name = 'Alice'")
        .arg(file_path.to_str().unwrap());

    cmd.assert().success();

    // Verify file was NOT modified
    let content = fs::read_to_string(&file_path)?;
    assert_eq!(
        content, original_content,
        "File should be unchanged without --write"
    );

    Ok(())
}

#[test]
fn test_file_write_preserves_original_on_read_only_query() -> Result<(), Box<dyn std::error::Error>>
{
    // Test that SELECT with --write doesn't modify file (no changes to persist)
    let temp_dir = create_temp_dir()?;
    let original_content = "id,name,value\n1,Alice,100\n2,Bob,200\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", original_content)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM data")
        .arg("--write")
        .arg(file_path.to_str().unwrap());

    cmd.assert().success();

    // Verify file is unchanged (SELECT doesn't modify data)
    let content = fs::read_to_string(&file_path)?;
    assert_eq!(
        content, original_content,
        "File should be unchanged after SELECT"
    );

    Ok(())
}

#[test]
fn test_file_write_multiple_operations() -> Result<(), Box<dyn std::error::Error>> {
    // Test multiple SQL operations with single --write at end
    let temp_dir = create_temp_dir()?;
    let file_path = create_custom_csv(
        temp_dir.path(),
        "data.csv",
        "id,name,value\n1,Alice,100\n2,Bob,200\n3,Charlie,300\n",
    )?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("UPDATE data SET value = 150 WHERE name = 'Alice'")
        .arg("-s")
        .arg("DELETE FROM data WHERE name = 'Bob'")
        .arg("-s")
        .arg("INSERT INTO data (id, name, value) VALUES (4, 'Diana', 400)")
        .arg("--write")
        .arg(file_path.to_str().unwrap());

    cmd.assert().success();

    // Verify all operations were applied
    let content = fs::read_to_string(&file_path)?;
    assert!(
        content.contains("1,Alice,150"),
        "Alice value should be updated"
    );
    assert!(!content.contains("2,Bob,200"), "Bob should be deleted");
    assert!(
        content.contains("3,Charlie,300"),
        "Charlie should be unchanged"
    );
    assert!(content.contains("4,Diana,400"), "Diana should be added");

    Ok(())
}
