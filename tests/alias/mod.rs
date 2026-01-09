//! Tests for column aliasing in sqawk
//!
//! Tests for SQL column aliasing functionality with AS keyword.

use crate::helpers::get_employees_file;
use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn test_basic_column_aliases() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = get_employees_file();

    // Run sqawk with basic column aliases
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT name AS employee_name, age AS employee_age, department AS dept FROM employees")
        .arg(file_path.to_str().unwrap())
        .arg("-v");

    // Check output - verify aliased column names and sample data
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("employee_name,employee_age,dept"))
        .stdout(predicate::str::contains("Alice,30,Engineering"))
        .stdout(predicate::str::contains("Bob,25,Marketing"))
        .stdout(predicate::str::contains("Charlie,35,Engineering"));

    // Verify SQL execution message appears somewhere in the output (stdout or stderr)
    let output = cmd.output()?;
    let stdout = String::from_utf8(output.stdout)?;
    let stderr = String::from_utf8(output.stderr)?;

    let contains_sql = stdout.contains("Executing SQL: SELECT name AS employee_name, age AS employee_age, department AS dept FROM employees") ||
                      stderr.contains("Executing SQL: SELECT name AS employee_name, age AS employee_age, department AS dept FROM employees");

    assert!(contains_sql, "SQL execution message not found in output");

    Ok(())
}

#[test]
fn test_mixed_aliases_and_regular_columns() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = get_employees_file();

    // Run sqawk with a mix of aliased and regular columns
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT id, name AS employee_name, department FROM employees")
        .arg(file_path.to_str().unwrap())
        .arg("-v");

    // Check output - verify mixed column names
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("id,employee_name,department"))
        .stdout(predicate::str::contains("1,Alice,Engineering"))
        .stdout(predicate::str::contains("2,Bob,Marketing"))
        .stdout(predicate::str::contains("3,Charlie,Engineering"));

    Ok(())
}

#[test]
fn test_aliases_with_where_clause() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = get_employees_file();

    // Run sqawk with aliases and a WHERE clause
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT name AS employee_name, age AS employee_age FROM employees WHERE age > 25")
        .arg(file_path.to_str().unwrap())
        .arg("-v");

    // Check output - should only include rows with age > 25
    // From employees.csv: Alice(30), Charlie(35), David(40), Eve(28), Frank(32), Grace(45)
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("employee_name,employee_age"))
        .stdout(predicate::str::contains("Alice,30"))
        .stdout(predicate::str::contains("Charlie,35"))
        .stdout(predicate::str::contains("David,40"));

    // Verify we don't see Bob (age 25) or Henry (age 22)
    let output = cmd.output()?;
    let stdout = String::from_utf8(output.stdout)?;
    assert!(
        !stdout.contains("Bob,25"),
        "Should not contain Bob (age <= 25)"
    );
    assert!(
        !stdout.contains("Henry,22"),
        "Should not contain Henry (age <= 25)"
    );

    Ok(())
}
