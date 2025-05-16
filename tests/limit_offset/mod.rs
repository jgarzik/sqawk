//! Integration tests for LIMIT and OFFSET functionality
//!
//! Tests for SQL queries with LIMIT and OFFSET clauses in various contexts
//! including with ORDER BY, aggregate functions, and GROUP BY.

use assert_cmd::Command;
use std::path::PathBuf;

// Helper function to get the path to static test files
fn get_sample_file() -> PathBuf {
    PathBuf::from("tests/data/sample.csv")
}

fn get_departments_file() -> PathBuf {
    PathBuf::from("tests/data/departments.csv")
}

fn get_duplicates_file() -> PathBuf {
    PathBuf::from("tests/data/duplicates.csv")
}

#[test]
fn test_limit_basic() -> Result<(), Box<dyn std::error::Error>> {
    // Test basic LIMIT functionality
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM sample LIMIT 2")
        .arg(get_sample_file().to_str().unwrap())
        .arg("-v");

    // Execute the command and capture its output
    let output = cmd.output()?;
    let success = output.status.success();
    let stderr = String::from_utf8(output.stderr)?;
    let stdout = String::from_utf8(output.stdout)?;

    // Verify the command succeeded
    assert!(success, "Command failed");

    // Verify verbose output contains LIMIT indication
    assert!(
        stderr.contains("Applying LIMIT/OFFSET"),
        "Missing 'Applying LIMIT/OFFSET' in stderr"
    );

    // Check that the output contains the expected data
    assert!(stdout.contains("id,name,age"), "Header should be present");
    assert!(stdout.contains("1,Alice,32"), "First row should be present");
    assert!(stdout.contains("2,Bob,25"), "Second row should be present");

    // Make sure limited rows are not present
    assert!(
        !stdout.contains("3,Charlie,35"),
        "Third row should not be present"
    );
    assert!(
        !stdout.contains("4,Dave,28"),
        "Fourth row should not be present"
    );

    Ok(())
}

#[test]
fn test_limit_with_offset() -> Result<(), Box<dyn std::error::Error>> {
    // Test LIMIT with OFFSET
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM sample LIMIT 2 OFFSET 1")
        .arg(get_sample_file().to_str().unwrap())
        .arg("-v");

    // Execute the command and capture its output
    let output = cmd.output()?;
    let success = output.status.success();
    let stderr = String::from_utf8(output.stderr)?;
    let stdout = String::from_utf8(output.stdout)?;

    // Verify the command succeeded
    assert!(success, "Command failed");

    // Verify verbose output contains LIMIT indication
    assert!(
        stderr.contains("Applying LIMIT/OFFSET"),
        "Missing 'Applying LIMIT/OFFSET' in stderr"
    );

    // Check that the output contains the expected data
    assert!(stdout.contains("id,name,age"), "Header should be present");
    assert!(stdout.contains("2,Bob,25"), "First row should be present");
    assert!(
        stdout.contains("3,Charlie,35"),
        "Second row should be present"
    );

    // Make sure limited and offset rows are not present
    assert!(
        !stdout.contains("1,Alice,30"),
        "First original row should not be present due to offset"
    );
    assert!(
        !stdout.contains("4,Dave,28"),
        "Fourth row should not be present due to limit"
    );

    Ok(())
}

#[test]
fn test_limit_with_order_by() -> Result<(), Box<dyn std::error::Error>> {
    // Test LIMIT with ORDER BY
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM sample ORDER BY age DESC LIMIT 2")
        .arg(get_sample_file().to_str().unwrap())
        .arg("-v");

    // Execute the command and capture its output
    let output = cmd.output()?;
    let success = output.status.success();
    let stderr = String::from_utf8(output.stderr)?;
    let stdout = String::from_utf8(output.stdout)?;

    // Verify the command succeeded
    assert!(success, "Command failed");

    // Verify stderr contains expected messages in the right order
    assert!(
        stderr.contains("Applying ORDER BY"),
        "Missing 'Applying ORDER BY' in stderr"
    );
    assert!(
        stderr.contains("Applying LIMIT/OFFSET"),
        "Missing 'Applying LIMIT/OFFSET' in stderr"
    );

    // Check output contains the correct sorted rows with limit
    assert!(stdout.contains("id,name,age"), "Header should be present");
    assert!(
        stdout.contains("3,Charlie,35"),
        "First row (highest age) should be present"
    );
    assert!(
        stdout.contains("1,Alice,30"),
        "Second row (second highest age) should be present"
    );

    // Make sure other rows are not present
    assert!(
        !stdout.contains("2,Bob,25"),
        "Row with age 25 should not be present"
    );
    assert!(
        !stdout.contains("4,Dave,28"),
        "Row with age 28 should not be present"
    );

    Ok(())
}

#[test]
fn test_limit_with_aggregates() -> Result<(), Box<dyn std::error::Error>> {
    // Test LIMIT with aggregate functions and GROUP BY
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT department, COUNT(*) AS count, AVG(salary) AS avg_salary FROM departments GROUP BY department ORDER BY avg_salary DESC LIMIT 2")
        .arg(get_departments_file().to_str().unwrap())
        .arg("-v");

    // Execute the command and capture its output
    let output = cmd.output()?;
    let success = output.status.success();
    let stderr = String::from_utf8(output.stderr)?;
    let stdout = String::from_utf8(output.stdout)?;

    // Verify the command succeeded
    assert!(success, "Command failed");

    // Verify stderr contains expected messages
    assert!(
        stderr.contains("Applying aggregate functions"),
        "Missing 'Applying aggregate functions' in stderr"
    );
    assert!(
        stderr.contains("Applying GROUP BY"),
        "Missing 'Applying GROUP BY' in stderr"
    );
    assert!(
        stderr.contains("Applying ORDER BY"),
        "Missing 'Applying ORDER BY' in stderr"
    );
    assert!(
        stderr.contains("Applying LIMIT/OFFSET"),
        "Missing 'Applying LIMIT/OFFSET' in stderr"
    );

    // Check output contains header and expected departments
    assert!(
        stdout.contains("department,count,avg_salary"),
        "Header should be present"
    );

    // Check Engineering department has highest average salary and appears first
    assert!(
        stdout.contains("Engineering,3,"),
        "Row for Engineering department should be present"
    );

    // Check Marketing department has second highest average salary
    assert!(
        stdout.contains("Marketing,2,"),
        "Row for Marketing department should be present"
    );

    // Finance department should be excluded by the LIMIT
    assert!(
        !stdout.contains("Finance"),
        "Finance department should not be present due to LIMIT"
    );

    Ok(())
}

#[test]
fn test_distinct_with_limit() -> Result<(), Box<dyn std::error::Error>> {
    // Test DISTINCT with LIMIT and OFFSET
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT DISTINCT department, role FROM duplicates ORDER BY department ASC LIMIT 3 OFFSET 1")
        .arg(get_duplicates_file().to_str().unwrap())
        .arg("-v");

    // Execute the command and capture its output
    let output = cmd.output()?;
    let success = output.status.success();
    let stderr = String::from_utf8(output.stderr)?;
    let stdout = String::from_utf8(output.stdout)?;

    // Verify the command succeeded
    assert!(success, "Command failed");

    // Verify stderr contains expected messages
    assert!(
        stderr.contains("Applying DISTINCT"),
        "Missing 'Applying DISTINCT' in stderr"
    );
    assert!(
        stderr.contains("Applying ORDER BY"),
        "Missing 'Applying ORDER BY' in stderr"
    );
    assert!(
        stderr.contains("Applying LIMIT/OFFSET"),
        "Missing 'Applying LIMIT/OFFSET' in stderr"
    );

    // Check output contains the expected header and rows
    assert!(
        stdout.contains("department,role"),
        "Header should be present"
    );

    // With department ASC ordering and offset 1, we should skip "Engineering,Developer"
    // and get "Engineering,Manager", "Finance,Analyst", and "HR,Manager"
    assert!(
        stdout.contains("Engineering,Manager"),
        "Row with Engineering,Manager should be present"
    );
    assert!(
        stdout.contains("Finance,Analyst"),
        "Row with Finance,Analyst should be present"
    );
    assert!(
        stdout.contains("HR,Manager"),
        "Row with HR,Manager should be present"
    );

    // Verify that excluded rows are not present
    assert!(
        !stdout.contains("Engineering,Developer"),
        "Engineering,Developer should be excluded due to OFFSET"
    );
    assert!(
        !stdout.contains("Marketing"),
        "Marketing should not be present due to LIMIT"
    );

    Ok(())
}

#[test]
fn test_zero_limit() -> Result<(), Box<dyn std::error::Error>> {
    // Test LIMIT 0 (should return only the header)
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM sample LIMIT 0")
        .arg(get_sample_file().to_str().unwrap())
        .arg("-v");

    // Execute the command and capture its output
    let output = cmd.output()?;
    let success = output.status.success();
    let stderr = String::from_utf8(output.stderr)?;
    let stdout = String::from_utf8(output.stdout)?;

    // Verify the command succeeded
    assert!(success, "Command failed");

    // Verify stderr contains LIMIT indication
    assert!(
        stderr.contains("Applying LIMIT/OFFSET"),
        "Missing 'Applying LIMIT/OFFSET' in stderr"
    );

    // Check output contains the header
    assert!(stdout.contains("id,name,age"), "Header should be present");

    // Check that no data rows are present
    assert!(
        !stdout.contains("Alice"),
        "Row with Alice should not be present"
    );
    assert!(
        !stdout.contains("Bob"),
        "Row with Bob should not be present"
    );
    assert!(
        !stdout.contains("Charlie"),
        "Row with Charlie should not be present"
    );
    assert!(
        !stdout.contains("Dave"),
        "Row with Dave should not be present"
    );

    Ok(())
}

#[test]
fn test_offset_beyond_table_size() -> Result<(), Box<dyn std::error::Error>> {
    // Test OFFSET beyond table size (should return only the header)
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM sample LIMIT 10 OFFSET 10")
        .arg(get_sample_file().to_str().unwrap())
        .arg("-v");

    // Execute the command and capture its output
    let output = cmd.output()?;
    let success = output.status.success();
    let stderr = String::from_utf8(output.stderr)?;
    let stdout = String::from_utf8(output.stdout)?;

    // Verify the command succeeded
    assert!(success, "Command failed");

    // Verify stderr contains LIMIT indication
    assert!(
        stderr.contains("Applying LIMIT/OFFSET"),
        "Missing 'Applying LIMIT/OFFSET' in stderr"
    );

    // Check output contains the header
    assert!(stdout.contains("id,name,age"), "Header should be present");

    // Check that no data rows are present (offset is beyond table size)
    assert!(
        !stdout.contains("Alice"),
        "Row with Alice should not be present"
    );
    assert!(
        !stdout.contains("Bob"),
        "Row with Bob should not be present"
    );
    assert!(
        !stdout.contains("Charlie"),
        "Row with Charlie should not be present"
    );
    assert!(
        !stdout.contains("Dave"),
        "Row with Dave should not be present"
    );

    Ok(())
}
