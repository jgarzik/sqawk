//! Tests for SQL aggregate functions in sqawk
//!
//! Tests for COUNT, SUM, AVG, MIN, MAX functions and their combinations.

use crate::helpers::get_employees_file;
use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn test_basic_aggregate_functions() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = get_employees_file();

    // Run sqawk with basic aggregate functions
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT COUNT(*), SUM(age), AVG(salary), MIN(age), MAX(salary) FROM employees")
        .arg(file_path.to_str().unwrap())
        .arg("-v");

    // employees.csv has 8 rows:
    // COUNT(*) = 8, SUM(age) = 257, AVG(salary) = 67500, MIN(age) = 22, MAX(salary) = 90000
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("COUNT,SUM,AVG,MIN,MAX"))
        .stdout(predicate::str::contains("8,257,67500,22,90000"));

    Ok(())
}

#[test]
fn test_aggregate_functions_with_aliases() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = get_employees_file();

    // Run sqawk with aggregate functions and aliases
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT COUNT(*) AS count, SUM(salary) AS total_salary, AVG(age) AS avg_age, MIN(salary) AS min_salary, MAX(age) AS max_age FROM employees")
        .arg(file_path.to_str().unwrap())
        .arg("-v");

    // count = 8, total_salary = 540000, avg_age = 32.125, min_salary = 45000, max_age = 45
    cmd.assert()
        .success()
        .stdout(predicate::str::contains(
            "count,total_salary,avg_age,min_salary,max_age",
        ))
        .stdout(predicate::str::contains("8,540000,32.125,45000,45"));

    Ok(())
}

#[test]
fn test_aggregate_functions_with_filter() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = get_employees_file();

    // Run sqawk with filtered aggregate functions
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT COUNT(*), SUM(salary), AVG(age), MIN(salary), MAX(age) FROM employees WHERE age > 25")
        .arg(file_path.to_str().unwrap())
        .arg("-v");

    // Rows with age > 25: Alice(30), Charlie(35), David(40), Eve(28), Frank(32), Grace(45) = 6 rows
    // COUNT = 6, SUM(salary) = 440000, AVG(age) = 35, MIN(salary) = 60000, MAX(age) = 45
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("COUNT,SUM,AVG,MIN,MAX"))
        .stdout(predicate::str::contains("6,440000,35,60000,45"));

    Ok(())
}

#[test]
fn test_aggregate_on_employees() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = get_employees_file();

    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT COUNT(*) AS total_count, AVG(age) AS average_age FROM employees")
        .arg(file_path.to_str().unwrap())
        .arg("-v");

    // COUNT(*) = 8, AVG(age) = 32.125
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("total_count,average_age"))
        .stdout(predicate::str::contains("8,32.125"));

    Ok(())
}
