//! BETWEEN operator tests for sqawk VM
//!
//! This file contains tests for SQL BETWEEN operator.

use crate::helpers::{create_custom_csv, create_temp_dir};

/// Test basic BETWEEN with integer values
#[test]
fn test_between_integers() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name,age\n1,Alice,25\n2,Bob,30\n3,Charlie,35\n4,David,40\n5,Eve,20\n";
    let file_path = create_custom_csv(temp_dir.path(), "people.csv", content)?;

    // Test BETWEEN with integer range using VM
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM people WHERE age BETWEEN 25 AND 35")
        .arg("--vm")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,Alice,25"))
        .stdout(predicates::str::contains("2,Bob,30"))
        .stdout(predicates::str::contains("3,Charlie,35"));

    Ok(())
}

/// Test BETWEEN excluding boundary values
#[test]
fn test_between_boundaries() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,value\n1,10\n2,20\n3,30\n4,40\n5,50\n";
    let file_path = create_custom_csv(temp_dir.path(), "numbers.csv", content)?;

    // Test BETWEEN includes boundaries (20 and 40 should be included)
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM numbers WHERE value BETWEEN 20 AND 40")
        .arg("--vm")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("2,20"))
        .stdout(predicates::str::contains("3,30"))
        .stdout(predicates::str::contains("4,40"));

    Ok(())
}

/// Test NOT BETWEEN
#[test]
fn test_not_between() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,score\n1,50\n2,75\n3,100\n4,25\n5,90\n";
    let file_path = create_custom_csv(temp_dir.path(), "scores.csv", content)?;

    // Test NOT BETWEEN - should return values outside the range
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM scores WHERE score NOT BETWEEN 60 AND 80")
        .arg("--vm")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,50"))
        .stdout(predicates::str::contains("3,100"))
        .stdout(predicates::str::contains("4,25"))
        .stdout(predicates::str::contains("5,90"));

    Ok(())
}

/// Test BETWEEN with no matches
#[test]
fn test_between_no_matches() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,value\n1,5\n2,10\n3,15\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    // Test BETWEEN with range that has no matches - VM mode returns empty output
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM data WHERE value BETWEEN 100 AND 200")
        .arg("--vm")
        .arg(file_path.to_str().unwrap());

    // VM mode with no results returns empty output (no header)
    cmd.assert().success();

    Ok(())
}

/// Test BETWEEN with single value in range
#[test]
fn test_between_single_match() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,price\n1,99\n2,150\n3,201\n";
    let file_path = create_custom_csv(temp_dir.path(), "products.csv", content)?;

    // Test BETWEEN with exactly one value in range
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM products WHERE price BETWEEN 100 AND 200")
        .arg("--vm")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("2,150"));

    Ok(())
}

/// Test BETWEEN with equal low and high bounds
#[test]
fn test_between_equal_bounds() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,num\n1,5\n2,10\n3,15\n";
    let file_path = create_custom_csv(temp_dir.path(), "nums.csv", content)?;

    // Test BETWEEN where low == high (should match exact value)
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM nums WHERE num BETWEEN 10 AND 10")
        .arg("--vm")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("2,10"));

    Ok(())
}
