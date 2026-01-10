//! Extended string function tests for sqawk VM
//!
//! Tests for CONCAT, LEFT, RIGHT functions.

use crate::helpers::{create_custom_csv, create_temp_dir};

/// Test CONCAT function with multiple arguments
#[test]
fn test_concat() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,first,last\n1,John,Doe\n2,Jane,Smith\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT CONCAT(first, ' ', last) AS full_name FROM data")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("John Doe"))
        .stdout(predicates::str::contains("Jane Smith"));

    Ok(())
}

/// Test LEFT function
#[test]
fn test_left() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name\n1,Alexander\n2,Bob\n3,Charlotte\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT LEFT(name, 3) AS short FROM data")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("Ale"))
        .stdout(predicates::str::contains("Bob"))
        .stdout(predicates::str::contains("Cha"));

    Ok(())
}

/// Test RIGHT function
#[test]
fn test_right() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name\n1,Alexander\n2,Bob\n3,Charlotte\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT RIGHT(name, 3) AS ending FROM data")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("der"))
        .stdout(predicates::str::contains("Bob"))
        .stdout(predicates::str::contains("tte"));

    Ok(())
}
