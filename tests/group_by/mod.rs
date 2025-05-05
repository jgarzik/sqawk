//! Tests for GROUP BY functionality in sqawk
//!
//! Tests for SQL GROUP BY clause with various aggregate functions.

use crate::helpers::create_temp_dir;
use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;
use std::io::Write;
use std::path::PathBuf;

// Helper function to create a departments test file
fn create_departments_file() -> Result<(tempfile::TempDir, PathBuf), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let file_path = temp_dir.path().join("departments.csv");

    // Create a CSV file for GROUP BY testing
    let content = "id,name,department,salary\n1,Alice,Engineering,75000\n2,Bob,Marketing,65000\n3,Charlie,Engineering,85000\n4,David,Sales,60000\n5,Eve,Marketing,70000\n6,Frank,Engineering,90000\n7,Grace,Sales,65000\n";

    let mut file = fs::File::create(&file_path)?;
    file.write_all(content.as_bytes())?;

    // Return both the TempDir (to keep it alive) and the file path
    Ok((temp_dir, file_path))
}

#[test]
fn test_basic_group_by() -> Result<(), Box<dyn std::error::Error>> {
    // Create test data file - keep temp_dir alive for the test duration
    let (_temp_dir, file_path) = create_departments_file()?;

    // Run sqawk with basic GROUP BY
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT department, COUNT(*) AS count, SUM(salary) AS total_salary, AVG(salary) AS avg_salary FROM departments GROUP BY department")
        .arg(file_path.to_str().unwrap())
        .arg("-v");

    // Check output
    cmd.assert().success().stdout(predicate::str::contains(
        "department,count,total_salary,avg_salary",
    ));

    // Verify the output has correct counts and aggregations
    let output = cmd.output()?;
    let stdout = String::from_utf8(output.stdout)?;

    // Engineering should have 3 employees, total salary 250000, avg 83333.33
    assert!(
        stdout.contains("Engineering,3,250000,83333.33"),
        "Engineering row incorrect"
    );

    // Marketing should have 2 employees, total salary 135000, avg 67500
    assert!(
        stdout.contains("Marketing,2,135000,67500"),
        "Marketing row incorrect"
    );

    // Sales should have 2 employees, total salary 125000, avg 62500
    assert!(
        stdout.contains("Sales,2,125000,62500"),
        "Sales row incorrect"
    );

    Ok(())
}

#[test]
fn test_group_by_with_order_by() -> Result<(), Box<dyn std::error::Error>> {
    // Create test data file - keep temp_dir alive for the test duration
    let (_temp_dir, file_path) = create_departments_file()?;

    // Run sqawk with GROUP BY and ORDER BY
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT department, COUNT(*) AS employee_count, AVG(salary) AS avg_salary FROM departments GROUP BY department ORDER BY avg_salary DESC")
        .arg(file_path.to_str().unwrap())
        .arg("-v");

    // Check output
    cmd.assert().success().stdout(predicate::str::contains(
        "department,employee_count,avg_salary",
    ));

    // Check for presence of all departments
    cmd.assert()
        .stdout(predicate::str::contains("Engineering"))
        .stdout(predicate::str::contains("Marketing"))
        .stdout(predicate::str::contains("Sales"));

    // Verify the order is correct (by avg_salary DESC)
    let output = cmd.output()?;
    let stdout = String::from_utf8(output.stdout)?;

    // Extract just the result data rows
    let data_rows: Vec<&str> = stdout
        .lines()
        .filter(|line| {
            line.contains(",")
                && (line.contains("Engineering")
                    || line.contains("Marketing")
                    || line.contains("Sales"))
        })
        .collect();

    // Should be in order: Engineering (83333.333), Marketing (67500), Sales (62500)
    assert!(
        data_rows[0].contains("Engineering"),
        "First row should be Engineering with highest avg_salary"
    );
    assert!(
        data_rows[1].contains("Marketing"),
        "Second row should be Marketing with middle avg_salary"
    );
    assert!(
        data_rows[2].contains("Sales"),
        "Third row should be Sales with lowest avg_salary"
    );

    Ok(())
}

#[test]
fn test_group_by_with_complex_expressions() -> Result<(), Box<dyn std::error::Error>> {
    // Create test data file - keep temp_dir alive for the test duration
    let (_temp_dir, file_path) = create_departments_file()?;

    // Run sqawk with complex GROUP BY expressions
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT department, COUNT(*) AS employee_count, SUM(salary) AS total_salary, AVG(salary) AS avg_salary, MIN(salary) AS min_salary, MAX(salary) AS max_salary FROM departments GROUP BY department ORDER BY avg_salary DESC")
        .arg(file_path.to_str().unwrap())
        .arg("-v");

    // Check output - just verify the command runs successfully
    cmd.assert().success();

    // Get the output to check the specific values
    let output = cmd.output()?;
    let stdout = String::from_utf8(output.stdout)?;

    // Check that header is correct
    assert!(
        stdout.contains("department,employee_count,total_salary,avg_salary,min_salary,max_salary"),
        "Header row not found or incorrect"
    );

    // Check each department is present and has correct employee count
    assert!(
        stdout.contains("Engineering,3,"),
        "Engineering row has incorrect employee count"
    );
    assert!(
        stdout.contains("Marketing,2,"),
        "Marketing row has incorrect employee count"
    );
    assert!(
        stdout.contains("Sales,2,"),
        "Sales row has incorrect employee count"
    );

    // Check total salaries are correct
    assert!(
        stdout.contains(",250000,"),
        "Engineering total salary should be 250000"
    );
    assert!(
        stdout.contains(",135000,"),
        "Marketing total salary should be 135000"
    );
    assert!(
        stdout.contains(",125000,"),
        "Sales total salary should be 125000"
    );

    // Check min/max values
    assert!(
        stdout.contains(",75000,90000"),
        "Engineering min/max salaries should be 75000/90000"
    );
    assert!(
        stdout.contains(",65000,70000"),
        "Marketing min/max salaries should be 65000/70000"
    );
    assert!(
        stdout.contains(",60000,65000"),
        "Sales min/max salaries should be 60000/65000"
    );

    Ok(())
}

#[test]
fn test_group_by_with_having() -> Result<(), Box<dyn std::error::Error>> {
    // Create test data file - keep temp_dir alive for the test duration
    let (_temp_dir, file_path) = create_departments_file()?;

    // Run sqawk with GROUP BY and HAVING
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT department, COUNT(*) AS employee_count, AVG(salary) AS avg_salary FROM departments GROUP BY department HAVING COUNT(*) > 2")
        .arg(file_path.to_str().unwrap())
        .arg("-v");

    // Check output
    cmd.assert().success();

    // Get the output to check the specific values
    let output = cmd.output()?;
    let stdout = String::from_utf8(output.stdout)?;

    // Check that header is correct
    assert!(
        stdout.contains("department,employee_count,avg_salary"),
        "Header row not found or incorrect"
    );

    // Engineering has 3 employees, so it should be included
    assert!(
        stdout.contains("Engineering,3,"),
        "Engineering should appear in results with 3 employees"
    );

    // Marketing and Sales only have 2 employees, so they should be filtered out by HAVING
    assert!(
        !stdout.contains("Marketing,2,"),
        "Marketing should be filtered out by HAVING clause"
    );
    assert!(
        !stdout.contains("Sales,2,"),
        "Sales should be filtered out by HAVING clause"
    );

    Ok(())
}

#[test]
fn test_group_by_with_having_avg() -> Result<(), Box<dyn std::error::Error>> {
    // Create test data file - keep temp_dir alive for the test duration
    let (_temp_dir, file_path) = create_departments_file()?;

    // Run sqawk with GROUP BY and HAVING with AVG function
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT department, COUNT(*) AS employee_count, AVG(salary) AS avg_salary FROM departments GROUP BY department HAVING AVG(salary) > 70000")
        .arg(file_path.to_str().unwrap())
        .arg("-v");

    // Check output
    cmd.assert().success();

    // Get the output to check the specific values
    let output = cmd.output()?;
    let stdout = String::from_utf8(output.stdout)?;

    // Engineering has avg salary > 70000, so it should be included
    assert!(
        stdout.contains("Engineering,3,"),
        "Engineering should appear in results with avg salary > 70000"
    );

    // Marketing has avg salary < 70000, so it should be filtered out
    assert!(
        !stdout.contains("Marketing,2,"),
        "Marketing should be filtered out by HAVING clause"
    );

    // Sales has avg salary < 70000, so it should be filtered out
    assert!(
        !stdout.contains("Sales,2,"),
        "Sales should be filtered out by HAVING clause"
    );

    Ok(())
}
