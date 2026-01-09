//! COALESCE and NULLIF function tests for sqawk VM
//!
//! This file contains tests for SQL COALESCE and NULLIF functions.

use crate::helpers::{create_custom_csv, create_temp_dir};

/// Test COALESCE with first non-null value
#[test]
fn test_coalesce_first_non_null() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name,nickname\n1,Alice,Ally\n2,Bob,\n3,Charlie,Chuck\n";
    let file_path = create_custom_csv(temp_dir.path(), "people.csv", content)?;

    // Test COALESCE returns first non-NULL value
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM people WHERE COALESCE(nickname, name) = 'Ally'")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,Alice,Ally"));

    Ok(())
}

/// Test COALESCE falls through to second value when first is NULL
#[test]
fn test_coalesce_fallback_to_second() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,primary_val,secondary_val\n1,,fallback1\n2,main2,fallback2\n";
    let file_path = create_custom_csv(temp_dir.path(), "values.csv", content)?;

    // When primary is NULL/empty, COALESCE should use secondary
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM values WHERE COALESCE(primary_val, secondary_val) = 'fallback1'")
        .arg(file_path.to_str().unwrap());

    // VM outputs NULL for null values
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,NULL,fallback1"));

    Ok(())
}

/// Test COALESCE with literal default
#[test]
fn test_coalesce_with_literal_default() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,val\n1,\n2,present\n3,\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    // Test COALESCE with literal default value
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM data WHERE COALESCE(val, 'default') = 'default'")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,"))
        .stdout(predicates::str::contains("3,"));

    Ok(())
}

/// Test NULLIF returns NULL when values equal
#[test]
fn test_nullif_returns_null_when_equal() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name\n1,Alice\n2,Bob\n3,Charlie\n";
    let file_path = create_custom_csv(temp_dir.path(), "people.csv", content)?;

    // NULLIF(name, 'Alice') should return NULL for Alice
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM people WHERE NULLIF(name, 'Alice') IS NULL")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,Alice"));

    Ok(())
}

/// Test NULLIF returns first value when not equal
#[test]
fn test_nullif_returns_value_when_not_equal() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,code\n1,A\n2,B\n3,C\n";
    let file_path = create_custom_csv(temp_dir.path(), "codes.csv", content)?;

    // NULLIF(code, 'A') returns NULL for 'A', returns original value for others
    // So NULLIF(code, 'A') IS NOT NULL should match 'B' and 'C'
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM codes WHERE NULLIF(code, 'A') IS NOT NULL")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("2,B"))
        .stdout(predicates::str::contains("3,C"));

    Ok(())
}

/// Test COALESCE with multiple arguments
#[test]
fn test_coalesce_multiple_args() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,a,b,c\n1,,,third\n2,,second,third\n3,first,second,third\n";
    let file_path = create_custom_csv(temp_dir.path(), "multi.csv", content)?;

    // COALESCE(a, b, c) should return first non-null
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM multi WHERE COALESCE(a, b, c) = 'third'")
        .arg(file_path.to_str().unwrap());

    // VM outputs NULL for null values
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,NULL,NULL,third"));

    Ok(())
}
