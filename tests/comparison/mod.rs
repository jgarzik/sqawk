//! Tests for SQL comparison operators
//! 
//! This module contains tests for all SQL comparison operators (=, !=, >, <, >=, <=)
//! to ensure they work correctly with integer types.

use assert_cmd;
use predicates;
use predicates::prelude::PredicateBooleanExt;

/// Get path to the static sample test CSV file
fn get_test_data_path() -> std::path::PathBuf {
    std::path::PathBuf::from("tests/data/sample.csv")
}

/// Get path to the static boundaries test CSV file with extreme values
fn get_boundaries_data_path() -> std::path::PathBuf {
    std::path::PathBuf::from("tests/data/boundaries.csv")
}

// Test cases for each comparison operator

#[test]
fn test_equals_operator() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = get_test_data_path();
    
    // Using the sample.csv file which has columns: id,name,age
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s").arg("SELECT * FROM sample WHERE age = 30")
       .arg(file_path);
       
    cmd.assert()
       .success()
       .stdout(predicates::str::contains("id,name,age"))
       .stdout(predicates::str::contains("1,Alice,30"));
    
    Ok(())
}

#[test]
fn test_not_equals_operator() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = get_test_data_path();
    
    // Using the sample.csv file which has columns: id,name,age
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s").arg("SELECT * FROM sample WHERE age != 30")
       .arg(file_path);
       
    cmd.assert()
       .success()
       .stdout(predicates::str::contains("id,name,age"))
       .stdout(predicates::str::contains("2,Bob,25"))
       .stdout(predicates::str::contains("3,Charlie,35"))
       .stdout(predicates::str::contains("1,Alice,30").not());
    
    Ok(())
}

#[test]
fn test_greater_than_operator() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = get_test_data_path();
    
    // Using the sample.csv file which has columns: id,name,age
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s").arg("SELECT * FROM sample WHERE age > 30")
       .arg(file_path);
       
    cmd.assert()
       .success()
       .stdout(predicates::str::contains("id,name,age"))
       .stdout(predicates::str::contains("3,Charlie,35"))
       .stdout(predicates::str::contains("1,Alice,30").not())
       .stdout(predicates::str::contains("2,Bob,25").not());
    
    Ok(())
}

#[test]
fn test_less_than_operator() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = get_test_data_path();
    
    // Using the sample.csv file which has columns: id,name,age
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s").arg("SELECT * FROM sample WHERE age < 30")
       .arg(file_path);
       
    cmd.assert()
       .success()
       .stdout(predicates::str::contains("id,name,age"))
       .stdout(predicates::str::contains("2,Bob,25"))
       .stdout(predicates::str::contains("1,Alice,30").not())
       .stdout(predicates::str::contains("3,Charlie,35").not());
    
    Ok(())
}

#[test]
fn test_greater_than_or_equal_operator() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = get_test_data_path();
    
    // Using the sample.csv file which has columns: id,name,age
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s").arg("SELECT * FROM sample WHERE age >= 30")
       .arg(file_path);
       
    cmd.assert()
       .success()
       .stdout(predicates::str::contains("id,name,age"))
       .stdout(predicates::str::contains("1,Alice,30"))
       .stdout(predicates::str::contains("3,Charlie,35"))
       .stdout(predicates::str::contains("2,Bob,25").not());
    
    Ok(())
}

#[test]
fn test_less_than_or_equal_operator() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = get_test_data_path();
    
    // Using the sample.csv file which has columns: id,name,age
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s").arg("SELECT * FROM sample WHERE age <= 30")
       .arg(file_path);
       
    cmd.assert()
       .success()
       .stdout(predicates::str::contains("id,name,age"))
       .stdout(predicates::str::contains("1,Alice,30"))
       .stdout(predicates::str::contains("2,Bob,25"))
       .stdout(predicates::str::contains("3,Charlie,35").not());
    
    Ok(())
}

#[test]
fn test_equals_with_no_matches() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = get_test_data_path();
    
    // Using the sample.csv file which has columns: id,name,age
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s").arg("SELECT * FROM sample WHERE age = 40")
       .arg(file_path);
       
    // Should return just the header, no data rows
    cmd.assert()
       .success()
       .stdout(predicates::str::contains("id,name,age"))
       .stdout(predicates::str::contains("id,name,age").and(
           predicates::str::contains("1,").not()
           .and(predicates::str::contains("2,").not())
           .and(predicates::str::contains("3,").not())
       ));
    
    Ok(())
}

// Boundary tests for comparison operators

#[test]
fn test_comparison_boundary_values() -> Result<(), Box<dyn std::error::Error>> {
    // Use the static boundaries.csv file with extreme integer values
    let file_path = get_boundaries_data_path();
    
    // Test with max integer value
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s").arg("SELECT * FROM boundaries WHERE value = 9223372036854775807")
       .arg(file_path.clone());
       
    cmd.assert()
       .success()
       .stdout(predicates::str::contains("id,value"))
       .stdout(predicates::str::contains("4,9223372036854775807"));
    
    // Test with min integer value
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s").arg("SELECT * FROM boundaries WHERE value = -9223372036854775808")
       .arg(file_path.clone());
       
    cmd.assert()
       .success()
       .stdout(predicates::str::contains("id,value"))
       .stdout(predicates::str::contains("5,-9223372036854775808"));
    
    // Test greater than zero
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s").arg("SELECT * FROM boundaries WHERE value > 0")
       .arg(file_path.clone());
       
    cmd.assert()
       .success()
       .stdout(predicates::str::contains("id,value"))
       .stdout(predicates::str::contains("2,1"))
       .stdout(predicates::str::contains("4,9223372036854775807"));
    
    // Test less than zero
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s").arg("SELECT * FROM boundaries WHERE value < 0")
       .arg(file_path.clone());
       
    cmd.assert()
       .success()
       .stdout(predicates::str::contains("id,value"))
       .stdout(predicates::str::contains("3,-1"))
       .stdout(predicates::str::contains("5,-9223372036854775808"));
    
    Ok(())
}