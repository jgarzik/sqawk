//! CAST type conversion tests for sqawk VM
//!
//! This file contains tests for SQL CAST type conversion expressions.

use crate::helpers::{create_custom_csv, create_temp_dir};

/// Test CAST integer to text
#[test]
fn test_cast_integer_to_text() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,value\n1,100\n2,200\n3,300\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    // Test CAST integer to TEXT for comparison
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM data WHERE CAST(value AS TEXT) = '100'")
        .arg("--vm")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,100"));

    Ok(())
}

/// Test CAST text to integer
#[test]
fn test_cast_text_to_integer() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,value\n1,50\n2,150\n3,250\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    // Test CAST literal to INTEGER for comparison
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM data WHERE CAST('100' AS INTEGER) < value")
        .arg("--vm")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("2,150"))
        .stdout(predicates::str::contains("3,250"));

    Ok(())
}

/// Test CAST with VARCHAR type
#[test]
fn test_cast_to_varchar() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,num\n1,42\n2,99\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    // Test CAST to VARCHAR (should work like TEXT)
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM data WHERE CAST(num AS VARCHAR) = '42'")
        .arg("--vm")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,42"));

    Ok(())
}

/// Test CAST column to integer for comparison
#[test]
fn test_cast_column_to_integer() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,strnum\n1,100\n2,200\n3,50\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    // Cast string column to integer for numeric comparison
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM data WHERE CAST(strnum AS INTEGER) > 75")
        .arg("--vm")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,100"))
        .stdout(predicates::str::contains("2,200"));

    Ok(())
}

/// Test CAST preserves integer values
#[test]
fn test_cast_integer_to_integer() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,value\n1,10\n2,20\n3,30\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    // CAST integer to integer should preserve value
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM data WHERE CAST(value AS INT) = 20")
        .arg("--vm")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("2,20"));

    Ok(())
}

/// Test multiple CAST expressions
#[test]
fn test_cast_both_sides() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,code\n1,A\n2,B\n3,C\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    // Cast id to text for string comparison
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM data WHERE CAST(id AS TEXT) = '2'")
        .arg("--vm")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("2,B"));

    Ok(())
}
