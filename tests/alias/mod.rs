//! Tests for column aliasing in sqawk
//!
//! Tests for SQL column aliasing functionality with AS keyword.

use crate::helpers::create_temp_dir;
use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;
use std::io::Write;
use std::path::PathBuf;

// Helper function to create an aliases test file
fn create_aliases_file() -> Result<(tempfile::TempDir, PathBuf), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let file_path = temp_dir.path().join("aliases.csv");

    // Create a CSV file for alias testing
    let content = "id,name,age,department,role\n1,Alice,30,Engineering,Developer\n2,Bob,25,Marketing,Specialist\n3,Charlie,35,Finance,Manager\n";

    let mut file = fs::File::create(&file_path)?;
    file.write_all(content.as_bytes())?;

    // Return both the TempDir (to keep it alive) and the file path
    Ok((temp_dir, file_path))
}

#[test]
fn test_basic_column_aliases() -> Result<(), Box<dyn std::error::Error>> {
    // Create test data file - keep temp_dir alive for the test duration
    let (_temp_dir, file_path) = create_aliases_file()?;

    // Run sqawk with basic column aliases
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT name AS employee_name, age AS employee_age, department AS dept FROM aliases")
        .arg(file_path.to_str().unwrap())
        .arg("-v");

    // Check output
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("employee_name,employee_age,dept"))
        .stdout(predicate::str::contains("Alice,30,Engineering"))
        .stdout(predicate::str::contains("Bob,25,Marketing"))
        .stdout(predicate::str::contains("Charlie,35,Finance"));

    // Verify SQL execution message appears somewhere in the output (stdout or stderr)
    let output = cmd.output()?;
    let stdout = String::from_utf8(output.stdout)?;
    let stderr = String::from_utf8(output.stderr)?;

    let contains_sql = stdout.contains("Executing SQL: SELECT name AS employee_name, age AS employee_age, department AS dept FROM aliases") ||
                      stderr.contains("Executing SQL: SELECT name AS employee_name, age AS employee_age, department AS dept FROM aliases");

    assert!(contains_sql, "SQL execution message not found in output");

    Ok(())
}

#[test]
fn test_mixed_aliases_and_regular_columns() -> Result<(), Box<dyn std::error::Error>> {
    // Create test data file - keep temp_dir alive for the test duration
    let (_temp_dir, file_path) = create_aliases_file()?;

    // Run sqawk with a mix of aliased and regular columns
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT id, name AS employee_name, department FROM aliases")
        .arg(file_path.to_str().unwrap())
        .arg("-v");

    // Check output
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("id,employee_name,department"))
        .stdout(predicate::str::contains("1,Alice,Engineering"))
        .stdout(predicate::str::contains("2,Bob,Marketing"))
        .stdout(predicate::str::contains("3,Charlie,Finance"));

    Ok(())
}

#[test]
fn test_aliases_with_where_clause() -> Result<(), Box<dyn std::error::Error>> {
    // Create test data file - keep temp_dir alive for the test duration
    let (_temp_dir, file_path) = create_aliases_file()?;

    // Run sqawk with aliases and a WHERE clause
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT name AS employee_name, age AS employee_age FROM aliases WHERE age > 25")
        .arg(file_path.to_str().unwrap())
        .arg("-v");

    // Check output - should only include rows with age > 25
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("employee_name,employee_age"))
        .stdout(predicate::str::contains("Alice,30"))
        .stdout(predicate::str::contains("Charlie,35"));

    // Verify we don't see Bob who is age 25
    let output = cmd.output()?;
    let stdout = String::from_utf8(output.stdout)?;
    assert!(
        !stdout.contains("Bob,25"),
        "Should not contain Bob (age <= 25)"
    );

    Ok(())
}
