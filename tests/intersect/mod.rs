//! INTERSECT tests for sqawk VM
//!
//! This file contains tests for SQL INTERSECT set operation.

use crate::helpers::{create_custom_csv, create_temp_dir};

/// Test INTERSECT returns only common rows
#[test]
fn test_intersect_common_rows() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content_a = "id,name\n1,Alice\n2,Bob\n3,Charlie\n";
    let content_b = "id,name\n2,Bob\n3,Charlie\n4,David\n";
    let file_a = create_custom_csv(temp_dir.path(), "a.csv", content_a)?;
    let file_b = create_custom_csv(temp_dir.path(), "b.csv", content_b)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM a INTERSECT SELECT * FROM b")
        .arg(file_a.to_str().unwrap())
        .arg(file_b.to_str().unwrap());

    // INTERSECT should only return Bob and Charlie (common to both)
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("2,Bob"))
        .stdout(predicates::str::contains("3,Charlie"));

    // Verify Alice and David are NOT in the result
    let output = cmd.output()?.stdout;
    let output_str = String::from_utf8_lossy(&output);
    assert!(
        !output_str.contains("1,Alice"),
        "Alice should not be in INTERSECT result"
    );
    assert!(
        !output_str.contains("4,David"),
        "David should not be in INTERSECT result"
    );

    Ok(())
}

/// Test INTERSECT with disjoint tables (no common rows)
#[test]
fn test_intersect_disjoint_tables() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content_a = "id,name\n1,Alice\n2,Bob\n";
    let content_b = "id,name\n3,Charlie\n4,David\n";
    let file_a = create_custom_csv(temp_dir.path(), "a.csv", content_a)?;
    let file_b = create_custom_csv(temp_dir.path(), "b.csv", content_b)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM a INTERSECT SELECT * FROM b")
        .arg(file_a.to_str().unwrap())
        .arg(file_b.to_str().unwrap());

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8_lossy(&output);

    // INTERSECT of disjoint tables should return only header
    assert!(
        !output_str.contains("Alice"),
        "Alice should not be in INTERSECT of disjoint tables"
    );
    assert!(
        !output_str.contains("Bob"),
        "Bob should not be in INTERSECT of disjoint tables"
    );
    assert!(
        !output_str.contains("Charlie"),
        "Charlie should not be in INTERSECT of disjoint tables"
    );
    assert!(
        !output_str.contains("David"),
        "David should not be in INTERSECT of disjoint tables"
    );

    Ok(())
}

/// Test INTERSECT with identical tables
#[test]
fn test_intersect_identical_tables() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name\n1,Alice\n2,Bob\n";
    let file_a = create_custom_csv(temp_dir.path(), "a.csv", content)?;
    let file_b = create_custom_csv(temp_dir.path(), "b.csv", content)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM a INTERSECT SELECT * FROM b")
        .arg(file_a.to_str().unwrap())
        .arg(file_b.to_str().unwrap());

    // INTERSECT of identical tables should return all rows (deduplicated)
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,Alice"))
        .stdout(predicates::str::contains("2,Bob"));

    Ok(())
}

/// Test INTERSECT with one table being subset of another
#[test]
fn test_intersect_subset_tables() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content_a = "id,name\n1,Alice\n2,Bob\n3,Charlie\n";
    let content_b = "id,name\n2,Bob\n";
    let file_a = create_custom_csv(temp_dir.path(), "a.csv", content_a)?;
    let file_b = create_custom_csv(temp_dir.path(), "b.csv", content_b)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM a INTERSECT SELECT * FROM b")
        .arg(file_a.to_str().unwrap())
        .arg(file_b.to_str().unwrap());

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8_lossy(&output);

    // Only Bob should be in the result
    assert!(output_str.contains("2,Bob"), "Bob should be in result");
    assert!(
        !output_str.contains("1,Alice"),
        "Alice should not be in result"
    );
    assert!(
        !output_str.contains("3,Charlie"),
        "Charlie should not be in result"
    );

    Ok(())
}

/// Test INTERSECT removes duplicates
#[test]
fn test_intersect_removes_duplicates() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    // Table a has Bob twice
    let content_a = "id,name\n2,Bob\n2,Bob\n";
    // Table b has Bob once
    let content_b = "id,name\n2,Bob\n";
    let file_a = create_custom_csv(temp_dir.path(), "a.csv", content_a)?;
    let file_b = create_custom_csv(temp_dir.path(), "b.csv", content_b)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM a INTERSECT SELECT * FROM b")
        .arg(file_a.to_str().unwrap())
        .arg(file_b.to_str().unwrap());

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8_lossy(&output);

    // INTERSECT should deduplicate, so Bob should appear only once
    let bob_count = output_str.matches("2,Bob").count();
    assert_eq!(
        bob_count, 1,
        "Bob should appear only once in INTERSECT result"
    );

    Ok(())
}
