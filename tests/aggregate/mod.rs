//! Tests for SQL aggregate functions in sqawk
//!
//! Tests for COUNT, SUM, AVG, MIN, MAX functions and their combinations.

use std::fs;
use std::path::PathBuf;
use assert_cmd::Command;
use predicates::prelude::*;
use std::io::Write;
use crate::helpers::create_temp_dir;

// Helper function to create an aggregates test file
fn create_aggregates_file() -> Result<(tempfile::TempDir, PathBuf), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let file_path = temp_dir.path().join("aggregates.csv");
    
    // Create a CSV file for aggregate function testing
    let content = "id,name,age,salary,department\n1,Alice,30,70000,Engineering\n2,Bob,25,55000,Marketing\n3,Charlie,35,65000,Engineering\n4,David,40,80000,Sales\n5,Eve,28,60000,Marketing\n";
    
    let mut file = fs::File::create(&file_path)?;
    file.write_all(content.as_bytes())?;
    
    // Return both the TempDir (to keep it alive) and the file path
    Ok((temp_dir, file_path))
}

#[test]
fn test_basic_aggregate_functions() -> Result<(), Box<dyn std::error::Error>> {
    // Create test data file - keep temp_dir alive for the test duration
    let (_temp_dir, file_path) = create_aggregates_file()?;
    
    // Run sqawk with basic aggregate functions
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT COUNT(*), SUM(age), AVG(salary), MIN(age), MAX(salary) FROM aggregates")
        .arg(file_path.to_str().unwrap())
        .arg("-v");
    
    // Check output - actual output uses simpler column names (COUNT,SUM,AVG,MIN,MAX)
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("COUNT,SUM,AVG,MIN,MAX"))
        .stdout(predicate::str::contains("5,158,66000,25,80000"));
    
    Ok(())
}

#[test]
fn test_aggregate_functions_with_aliases() -> Result<(), Box<dyn std::error::Error>> {
    // Create test data file - keep temp_dir alive for the test duration
    let (_temp_dir, file_path) = create_aggregates_file()?;
    
    // Run sqawk with aggregate functions and aliases
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT COUNT(*) AS count, SUM(salary) AS total_salary, AVG(age) AS avg_age, MIN(salary) AS min_salary, MAX(age) AS max_age FROM aggregates")
        .arg(file_path.to_str().unwrap())
        .arg("-v");
    
    // Check output
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("count,total_salary,avg_age,min_salary,max_age"))
        .stdout(predicate::str::contains("5,330000,31.6,55000,40"));
    
    Ok(())
}

#[test]
fn test_aggregate_functions_with_filter() -> Result<(), Box<dyn std::error::Error>> {
    // Create test data file - keep temp_dir alive for the test duration
    let (_temp_dir, file_path) = create_aggregates_file()?;
    
    // Run sqawk with filtered aggregate functions
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT COUNT(*), SUM(salary), AVG(age), MIN(salary), MAX(age) FROM aggregates WHERE age > 25")
        .arg(file_path.to_str().unwrap())
        .arg("-v");
    
    // Check output - should only include rows with age > 25
    // Actual output uses simpler column names (COUNT,SUM,AVG,MIN,MAX)
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("COUNT,SUM,AVG,MIN,MAX"))
        .stdout(predicate::str::contains("4,275000,33.25,60000,40"));
    
    Ok(())
}

#[test]
fn test_aggregate_on_basic_table() -> Result<(), Box<dyn std::error::Error>> {
    // We'll use the sample.csv file which is a standard test file
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT COUNT(*) AS total_count, AVG(age) AS average_age FROM sample")
        .arg("tests/data/sample.csv")
        .arg("-v");
    
    // Check output
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("total_count,average_age"))
        .stdout(predicate::str::contains("3,30"));
    
    Ok(())
}