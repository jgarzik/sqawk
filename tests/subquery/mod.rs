//! Scalar subquery tests for sqawk VM
//!
//! This file contains tests for scalar subqueries with aggregate functions.
//! Phase 4F: Subquery support

use crate::helpers::{create_custom_csv, create_temp_dir};

/// Test scalar subquery with MAX aggregate
#[test]
fn test_scalar_subquery_max() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name,age\n1,Alice,25\n2,Bob,30\n3,Charlie,35\n4,David,40\n5,Eve,20\n";
    let file_path = create_custom_csv(temp_dir.path(), "people.csv", content)?;

    // Test: SELECT * FROM people WHERE age = (SELECT MAX(age) FROM people)
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM people WHERE age = (SELECT MAX(age) FROM people)")
        .arg("--vm")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("4,David,40"));

    Ok(())
}

/// Test scalar subquery with MIN aggregate
#[test]
fn test_scalar_subquery_min() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name,age\n1,Alice,25\n2,Bob,30\n3,Charlie,35\n4,David,40\n5,Eve,20\n";
    let file_path = create_custom_csv(temp_dir.path(), "people.csv", content)?;

    // Test: SELECT * FROM people WHERE age = (SELECT MIN(age) FROM people)
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM people WHERE age = (SELECT MIN(age) FROM people)")
        .arg("--vm")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("5,Eve,20"));

    Ok(())
}

/// Test scalar subquery with COUNT aggregate
#[test]
fn test_scalar_subquery_count() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,value\n1,100\n2,200\n3,300\n4,400\n5,500\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    // Test: SELECT * FROM data WHERE id = (SELECT COUNT(*) FROM data)
    // COUNT(*) = 5, so should match id=5
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM data WHERE id = (SELECT COUNT(*) FROM data)")
        .arg("--vm")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("5,500"));

    Ok(())
}

/// Test scalar subquery with SUM aggregate
#[test]
fn test_scalar_subquery_sum() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,value\n1,10\n2,20\n3,30\n4,40\n5,100\n";
    let _file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    // Test: SELECT * FROM data WHERE value = (SELECT SUM(value) FROM data WHERE id <= 3)
    // SUM of 10+20+30 = 60, but 60 is not in the table, so no results
    // Let's test with a matching value
    let content2 = "id,value\n1,10\n2,20\n3,30\n4,60\n";
    let file_path2 = create_custom_csv(temp_dir.path(), "data2.csv", content2)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM data2 WHERE value = (SELECT SUM(value) FROM data2 WHERE id <= 3)")
        .arg("--vm")
        .arg(file_path2.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("4,60"));

    Ok(())
}

/// Test scalar subquery with AVG aggregate
#[test]
fn test_scalar_subquery_avg() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,score\n1,80\n2,90\n3,100\n4,90\n";
    let file_path = create_custom_csv(temp_dir.path(), "scores.csv", content)?;

    // AVG(score) = (80+90+100+90)/4 = 90
    // Should match id=2 and id=4 where score=90
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM scores WHERE score = (SELECT AVG(score) FROM scores)")
        .arg("--vm")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("2,90"))
        .stdout(predicates::str::contains("4,90"));

    Ok(())
}

/// Test scalar subquery returning no rows (NULL case)
#[test]
fn test_scalar_subquery_empty_result() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,value\n1,10\n2,20\n3,30\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    // Subquery with impossible WHERE returns no rows
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM data WHERE value = (SELECT MAX(value) FROM data WHERE value > 1000)")
        .arg("--vm")
        .arg(file_path.to_str().unwrap());

    // MAX on empty set is NULL, so no matches expected
    cmd.assert().success();

    Ok(())
}

/// Test scalar subquery with greater than comparison
#[test]
fn test_scalar_subquery_greater_than() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name,age\n1,Alice,25\n2,Bob,30\n3,Charlie,35\n4,David,40\n5,Eve,20\n";
    let file_path = create_custom_csv(temp_dir.path(), "people.csv", content)?;

    // Test: SELECT * FROM people WHERE age > (SELECT AVG(age) FROM people)
    // AVG(age) = (25+30+35+40+20)/5 = 30
    // Should return people with age > 30: Charlie(35), David(40)
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM people WHERE age > (SELECT AVG(age) FROM people)")
        .arg("--vm")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("3,Charlie,35"))
        .stdout(predicates::str::contains("4,David,40"));

    Ok(())
}
