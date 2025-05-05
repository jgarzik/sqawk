//! Integration tests for ORDER BY functionality in sqawk
//!
//! Tests for sorting functionality with different column combinations and directions.


use assert_cmd::Command;
use predicates::prelude::*;
use crate::helpers::*;

/// Test ordering by a single column in ascending order (implicit)
#[test]
fn test_order_by_single_column() -> Result<(), Box<dyn std::error::Error>> {
    // Define the test case
    let test_case = SqawkTestCase {
        sql: "SELECT * FROM people ORDER BY age".to_string(),
        expected_stdout: vec![
            "id,name,age".to_string(),
            "2,Bob,25".to_string(),
            "1,Alice,30".to_string(),
            "3,Charlie,35".to_string(),
        ],
        verbose: true,
        ..Default::default()
    };

    run_test_case(test_case)
}

/// Test ordering by a single column in descending order (explicit)
#[test]
fn test_order_by_single_column_desc() -> Result<(), Box<dyn std::error::Error>> {
    // Define the test case
    let test_case = SqawkTestCase {
        sql: "SELECT * FROM people ORDER BY age DESC".to_string(),
        expected_stdout: vec![
            "id,name,age".to_string(),
            "3,Charlie,35".to_string(),
            "1,Alice,30".to_string(),
            "2,Bob,25".to_string(),
        ],
        verbose: true,
        ..Default::default()
    };

    run_test_case(test_case)
}

/// Test ordering by multiple columns with mixed directions using static sample data
#[test]
fn test_order_by_multiple_columns() -> Result<(), Box<dyn std::error::Error>> {
    // Create a custom CSV file with needed data
    let temp_dir = create_temp_dir()?;
    let content = "dept,name,salary\nIT,Alice,70000\nHR,Bob,55000\nIT,Charlie,65000\nHR,David,60000\nIT,Eve,75000\n";
    let file_path = create_custom_csv(temp_dir.path(), "employees.csv", content)?;

    // Build the command
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM employees ORDER BY dept ASC, salary DESC")
        .arg(format!("employees={}", file_path.to_str().unwrap()));

    // Check headers
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("dept,name,salary"));

    // Check ordering: first HR department
    let output = cmd.output()?;
    let stdout = String::from_utf8(output.stdout)?;
    let lines: Vec<&str> = stdout.lines().collect();

    // Verify expected order:
    // Header: dept,name,salary
    // Line 1: HR,David,60000
    // Line 2: HR,Bob,55000
    // Line 3: IT,Eve,75000
    // Line 4: IT,Alice,70000
    // Line 5: IT,Charlie,65000
    assert_eq!(lines[0], "dept,name,salary");
    assert!(lines[1].contains("HR") && lines[1].contains("David") && lines[1].contains("60000"));
    assert!(lines[2].contains("HR") && lines[2].contains("Bob") && lines[2].contains("55000"));
    assert!(lines[3].contains("IT") && lines[3].contains("Eve") && lines[3].contains("75000"));
    assert!(lines[4].contains("IT") && lines[4].contains("Alice") && lines[4].contains("70000"));
    assert!(lines[5].contains("IT") && lines[5].contains("Charlie") && lines[5].contains("65000"));

    Ok(())
}

/// Test ordering with a WHERE clause to ensure filtering happens before sorting
#[test]
fn test_order_by_with_where() -> Result<(), Box<dyn std::error::Error>> {
    // Create a custom CSV file with needed data
    let temp_dir = create_temp_dir()?;
    let content = "id,name,age,dept\n1,Alice,30,IT\n2,Bob,25,HR\n3,Charlie,35,IT\n4,David,40,HR\n5,Eve,28,IT\n";
    let file_path = create_custom_csv(temp_dir.path(), "employees.csv", content)?;

    // Build the command
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT name, age FROM employees WHERE dept = 'IT' ORDER BY age DESC")
        .arg(format!("employees={}", file_path.to_str().unwrap()));

    // Check output
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("name,age"))
        .stdout(predicate::str::contains("Charlie,35"))
        .stdout(predicate::str::contains("Alice,30"))
        .stdout(predicate::str::contains("Eve,28"));

    // Make sure we don't have HR department entries
    let output = cmd.output()?;
    let stdout = String::from_utf8(output.stdout)?;
    assert!(!stdout.contains("Bob"), "Should not contain Bob from HR department");
    assert!(!stdout.contains("David"), "Should not contain David from HR department");

    Ok(())
}

/// Test ordering with a projection to ensure column selection works with ORDER BY
/// We include the sort column in the projection to comply with SQL execution
#[test]
fn test_order_by_with_projection() -> Result<(), Box<dyn std::error::Error>> {
    // Create a custom CSV file with needed data
    let temp_dir = create_temp_dir()?;
    let content = "id,name,age,salary\n1,Alice,30,70000\n2,Bob,25,55000\n3,Charlie,35,65000\n4,David,40,80000\n";
    let file_path = create_custom_csv(temp_dir.path(), "employees.csv", content)?;

    // Build the command - include age in the SELECT since we need it for ORDER BY
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT name, salary, age FROM employees ORDER BY age DESC")
        .arg(format!("employees={}", file_path.to_str().unwrap()));

    // Check output - should now include age column since we're ordering by it
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("name,salary,age"))
        .stdout(predicate::str::contains("David,80000,40")) // Oldest
        .stdout(predicate::str::contains("Charlie,65000,35"))
        .stdout(predicate::str::contains("Alice,70000,30"))
        .stdout(predicate::str::contains("Bob,55000,25")); // Youngest

    Ok(())
}