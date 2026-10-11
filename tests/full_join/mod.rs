//! FULL JOIN tests for sqawk VM
//!
//! This file contains tests for SQL FULL OUTER JOIN operations.

use crate::helpers::{create_custom_csv, create_temp_dir};

/// Test FULL JOIN returns all rows from both tables with matches
#[test]
fn test_full_join_with_matches() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content_a = "id,name\n1,Alice\n2,Bob\n";
    let content_b = "id,dept\n1,Engineering\n2,Sales\n";
    let file_a = create_custom_csv(temp_dir.path(), "a.csv", content_a)?;
    let file_b = create_custom_csv(temp_dir.path(), "b.csv", content_b)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM a FULL JOIN b ON a.id = b.id")
        .arg(file_a.to_str().unwrap())
        .arg(file_b.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,Alice,1,Engineering"))
        .stdout(predicates::str::contains("2,Bob,2,Sales"));

    Ok(())
}

/// Test FULL JOIN returns NULLs for non-matching rows on both sides
#[test]
fn test_full_join_with_nulls_both_sides() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content_a = "id,name\n1,Alice\n2,Bob\n3,Charlie\n";
    let content_b = "id,dept\n1,Engineering\n2,Sales\n4,Marketing\n";
    let file_a = create_custom_csv(temp_dir.path(), "a.csv", content_a)?;
    let file_b = create_custom_csv(temp_dir.path(), "b.csv", content_b)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM a FULL JOIN b ON a.id = b.id")
        .arg(file_a.to_str().unwrap())
        .arg(file_b.to_str().unwrap());

    // Charlie (id=3) has no match in right table -> right columns NULL
    // Marketing (id=4) has no match in left table -> left columns NULL
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,Alice,1,Engineering"))
        .stdout(predicates::str::contains("2,Bob,2,Sales"))
        .stdout(predicates::str::contains("3,Charlie,,"))
        .stdout(predicates::str::contains(",,4,Marketing"));

    Ok(())
}

/// Test FULL JOIN with completely disjoint tables
#[test]
fn test_full_join_disjoint() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content_a = "id,name\n1,Alice\n2,Bob\n";
    let content_b = "id,dept\n3,Engineering\n4,Sales\n";
    let file_a = create_custom_csv(temp_dir.path(), "a.csv", content_a)?;
    let file_b = create_custom_csv(temp_dir.path(), "b.csv", content_b)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM a FULL JOIN b ON a.id = b.id")
        .arg(file_a.to_str().unwrap())
        .arg(file_b.to_str().unwrap());

    // No matches, all rows should have NULLs on the opposite side
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,Alice,,"))
        .stdout(predicates::str::contains("2,Bob,,"))
        .stdout(predicates::str::contains(",,3,Engineering"))
        .stdout(predicates::str::contains(",,4,Sales"));

    Ok(())
}

/// Test FULL OUTER JOIN syntax (equivalent to FULL JOIN)
#[test]
fn test_full_outer_join_syntax() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content_a = "id,name\n1,Alice\n";
    let content_b = "id,dept\n1,Engineering\n";
    let file_a = create_custom_csv(temp_dir.path(), "a.csv", content_a)?;
    let file_b = create_custom_csv(temp_dir.path(), "b.csv", content_b)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM a FULL OUTER JOIN b ON a.id = b.id")
        .arg(file_a.to_str().unwrap())
        .arg(file_b.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,Alice,1,Engineering"));

    Ok(())
}

/// Test FULL JOIN with empty left table
#[test]
fn test_full_join_empty_left() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content_a = "id,name\n";
    let content_b = "id,dept\n1,Engineering\n2,Sales\n";
    let file_a = create_custom_csv(temp_dir.path(), "a.csv", content_a)?;
    let file_b = create_custom_csv(temp_dir.path(), "b.csv", content_b)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM a FULL JOIN b ON a.id = b.id")
        .arg(file_a.to_str().unwrap())
        .arg(file_b.to_str().unwrap());

    // All right rows should be returned with NULL for left columns
    cmd.assert()
        .success()
        .stdout(predicates::str::contains(",,1,Engineering"))
        .stdout(predicates::str::contains(",,2,Sales"));

    Ok(())
}

/// Test FULL JOIN with empty right table
#[test]
fn test_full_join_empty_right() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content_a = "id,name\n1,Alice\n2,Bob\n";
    let content_b = "id,dept\n";
    let file_a = create_custom_csv(temp_dir.path(), "a.csv", content_a)?;
    let file_b = create_custom_csv(temp_dir.path(), "b.csv", content_b)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM a FULL JOIN b ON a.id = b.id")
        .arg(file_a.to_str().unwrap())
        .arg(file_b.to_str().unwrap());

    // All left rows should be returned with NULL for right columns
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,Alice,,"))
        .stdout(predicates::str::contains("2,Bob,,"));

    Ok(())
}
