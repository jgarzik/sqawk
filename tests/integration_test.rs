//! Integration tests for sqawk
//! 
//! This file contains end-to-end tests for the sqawk application.

use std::fs;
use std::path::PathBuf;
use std::process::Command;

use assert_cmd::prelude::*;
use predicates::prelude::*;
use tempfile::TempDir;

#[test]
fn test_basic_select() -> Result<(), Box<dyn std::error::Error>> {
    // Create a temporary directory for test files
    let temp_dir = TempDir::new()?;
    let test_file = prepare_test_file(temp_dir.path())?;
    
    // Run sqawk with a SELECT query
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM people")
        .arg(test_file.to_str().unwrap());
    
    // Check output
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("id,name,age"))
        .stdout(predicate::str::contains("1,Alice,30"))
        .stdout(predicate::str::contains("2,Bob,25"))
        .stdout(predicate::str::contains("3,Charlie,35"));
    
    Ok(())
}

#[test]
fn test_filtered_select() -> Result<(), Box<dyn std::error::Error>> {
    // Create a temporary directory for test files
    let temp_dir = TempDir::new()?;
    let test_file = prepare_test_file(temp_dir.path())?;
    
    // Run sqawk with a SELECT query with WHERE clause
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT name FROM people WHERE age = 30")
        .arg(test_file.to_str().unwrap());
    
    // Check output
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("name"))
        .stdout(predicate::str::contains("Alice"))
        .stdout(predicate::str::contains("Bob").not())
        .stdout(predicate::str::contains("Charlie").not());
    
    Ok(())
}

#[test]
fn test_insert() -> Result<(), Box<dyn std::error::Error>> {
    // Create a temporary directory for test files
    let temp_dir = TempDir::new()?;
    let test_file = prepare_test_file(temp_dir.path())?;
    
    // Run sqawk with an INSERT query
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("INSERT INTO people (id, name, age) VALUES (4, 'Dave', 40)")
        .arg("-s")
        .arg("SELECT * FROM people")
        .arg(test_file.to_str().unwrap());
    
    // Check output
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("id,name,age"))
        .stdout(predicate::str::contains("1,Alice,30"))
        .stdout(predicate::str::contains("2,Bob,25"))
        .stdout(predicate::str::contains("3,Charlie,35"))
        .stdout(predicate::str::contains("4,Dave,40"));
    
    // Check that the file was modified
    let content = fs::read_to_string(&test_file)?;
    assert!(content.contains("4,Dave,40"));
    
    Ok(())
}

#[test]
fn test_custom_table_name() -> Result<(), Box<dyn std::error::Error>> {
    // Create a temporary directory for test files
    let temp_dir = TempDir::new()?;
    let test_file = prepare_test_file(temp_dir.path())?;
    
    // Run sqawk with a custom table name
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM custom_table")
        .arg(format!("custom_table={}", test_file.to_str().unwrap()));
    
    // Check output
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("id,name,age"))
        .stdout(predicate::str::contains("1,Alice,30"))
        .stdout(predicate::str::contains("2,Bob,25"))
        .stdout(predicate::str::contains("3,Charlie,35"));
    
    Ok(())
}

#[test]
fn test_invalid_sql() -> Result<(), Box<dyn std::error::Error>> {
    // Create a temporary directory for test files
    let temp_dir = TempDir::new()?;
    let test_file = prepare_test_file(temp_dir.path())?;
    
    // Run sqawk with invalid SQL
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("INVALID SQL")
        .arg(test_file.to_str().unwrap());
    
    // Check error output
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("Failed to execute SQL"));
    
    Ok(())
}

// Helper function to create a test CSV file
fn prepare_test_file(dir: &std::path::Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let file_path = dir.join("people.csv");
    let content = "id,name,age\n1,Alice,30\n2,Bob,25\n3,Charlie,35\n";
    fs::write(&file_path, content)?;
    Ok(file_path)
}
