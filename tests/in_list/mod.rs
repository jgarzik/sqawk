//! IN list operator tests for sqawk VM
//!
//! This file contains tests for SQL IN (list) operator.

use crate::helpers::{create_custom_csv, create_temp_dir};

/// Test basic IN with integer values
#[test]
fn test_in_list_integers() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name,age\n1,Alice,25\n2,Bob,30\n3,Charlie,35\n4,David,40\n5,Eve,20\n";
    let file_path = create_custom_csv(temp_dir.path(), "people.csv", content)?;

    // Test IN with integer list using VM
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM people WHERE age IN (25, 35, 40)")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,Alice,25"))
        .stdout(predicates::str::contains("3,Charlie,35"))
        .stdout(predicates::str::contains("4,David,40"));

    Ok(())
}

/// Test IN with string values
#[test]
fn test_in_list_strings() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name,city\n1,Alice,NYC\n2,Bob,LA\n3,Charlie,Chicago\n4,David,NYC\n";
    let file_path = create_custom_csv(temp_dir.path(), "people.csv", content)?;

    // Test IN with string list using VM
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM people WHERE city IN ('NYC', 'LA')")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,Alice,NYC"))
        .stdout(predicates::str::contains("2,Bob,LA"))
        .stdout(predicates::str::contains("4,David,NYC"));

    Ok(())
}

/// Test NOT IN
#[test]
fn test_not_in_list() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,status\n1,active\n2,pending\n3,inactive\n4,active\n5,archived\n";
    let file_path = create_custom_csv(temp_dir.path(), "records.csv", content)?;

    // Test NOT IN using VM
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM records WHERE status NOT IN ('active', 'archived')")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("2,pending"))
        .stdout(predicates::str::contains("3,inactive"));

    Ok(())
}

/// Test IN with single value (equivalent to =)
#[test]
fn test_in_list_single_value() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,value\n1,10\n2,20\n3,30\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    // Test IN with single value using VM
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM data WHERE value IN (20)")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("2,20"));

    Ok(())
}

/// Test IN with no matches
#[test]
fn test_in_list_no_matches() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,num\n1,5\n2,10\n3,15\n";
    let file_path = create_custom_csv(temp_dir.path(), "nums.csv", content)?;

    // Test IN with values that don't match
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM nums WHERE num IN (100, 200, 300)")
        .arg(file_path.to_str().unwrap());

    // VM mode with no results returns empty output
    cmd.assert().success();

    Ok(())
}

/// Test IN with all values matching
#[test]
fn test_in_list_all_match() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,code\n1,A\n2,B\n3,C\n";
    let file_path = create_custom_csv(temp_dir.path(), "codes.csv", content)?;

    // Test IN that matches all rows
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM codes WHERE code IN ('A', 'B', 'C')")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,A"))
        .stdout(predicates::str::contains("2,B"))
        .stdout(predicates::str::contains("3,C"));

    Ok(())
}
