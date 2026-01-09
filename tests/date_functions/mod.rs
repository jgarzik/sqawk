//! Date/time function tests for sqawk VM
//!
//! Tests for DATE, TIME, NOW functions.

use crate::helpers::{create_custom_csv, create_temp_dir};

/// Test NOW() function returns a timestamp
#[test]
fn test_now() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id\n1\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT NOW() AS current_time FROM data")
        .arg(file_path.to_str().unwrap());

    // NOW() should return something with date-like format (YYYY-MM-DD)
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("202")); // Year starts with 202x

    Ok(())
}

/// Test DATE function extracts date from datetime string
#[test]
fn test_date_extract() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,datetime\n1,2024-01-15 10:30:00\n2,2024-06-20 14:45:00\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT DATE(datetime) AS dt FROM data")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("2024-01-15"))
        .stdout(predicates::str::contains("2024-06-20"));

    Ok(())
}

/// Test TIME function extracts time from datetime string
#[test]
fn test_time_extract() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,datetime\n1,2024-01-15 10:30:00\n2,2024-06-20 14:45:00\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT TIME(datetime) AS tm FROM data")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("10:30:00"))
        .stdout(predicates::str::contains("14:45:00"));

    Ok(())
}

/// Test CURRENT_DATE returns today's date
#[test]
fn test_current_date() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id\n1\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT CURRENT_DATE AS today FROM data")
        .arg(file_path.to_str().unwrap());

    // Should return date in YYYY-MM-DD format
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("202")); // Year starts with 202x

    Ok(())
}
