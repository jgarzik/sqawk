//! UNION and UNION ALL tests for sqawk VM
//!
//! This file contains tests for SQL UNION and UNION ALL set operations.

use crate::helpers::{create_custom_csv, create_temp_dir};

/// Test UNION ALL combines all rows from both tables
#[test]
fn test_union_all_combines_all_rows() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content_a = "id,name\n1,Alice\n2,Bob\n";
    let content_b = "id,name\n2,Bob\n3,Charlie\n";
    let file_a = create_custom_csv(temp_dir.path(), "a.csv", content_a)?;
    let file_b = create_custom_csv(temp_dir.path(), "b.csv", content_b)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM a UNION ALL SELECT * FROM b")
        .arg(file_a.to_str().unwrap())
        .arg(file_b.to_str().unwrap());

    // UNION ALL should include Bob twice (once from each table)
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,Alice"))
        .stdout(predicates::str::contains("2,Bob"))
        .stdout(predicates::str::contains("3,Charlie"));

    // Count occurrences of Bob to verify duplicates are kept
    let output = cmd.output()?.stdout;
    let output_str = String::from_utf8_lossy(&output);
    let bob_count = output_str.matches("2,Bob").count();
    assert_eq!(bob_count, 2, "UNION ALL should keep duplicate rows");

    Ok(())
}

/// Test UNION removes duplicate rows
#[test]
fn test_union_removes_duplicates() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content_a = "id,name\n1,Alice\n2,Bob\n";
    let content_b = "id,name\n2,Bob\n3,Charlie\n";
    let file_a = create_custom_csv(temp_dir.path(), "a.csv", content_a)?;
    let file_b = create_custom_csv(temp_dir.path(), "b.csv", content_b)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM a UNION SELECT * FROM b")
        .arg(file_a.to_str().unwrap())
        .arg(file_b.to_str().unwrap());

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8_lossy(&output);

    // UNION should include Bob only once (deduplication)
    let bob_count = output_str.matches("2,Bob").count();
    assert_eq!(bob_count, 1, "UNION should deduplicate rows");

    Ok(())
}

/// Test UNION with disjoint tables (no common rows)
#[test]
fn test_union_disjoint_tables() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content_a = "id,name\n1,Alice\n2,Bob\n";
    let content_b = "id,name\n3,Charlie\n4,David\n";
    let file_a = create_custom_csv(temp_dir.path(), "a.csv", content_a)?;
    let file_b = create_custom_csv(temp_dir.path(), "b.csv", content_b)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM a UNION SELECT * FROM b")
        .arg(file_a.to_str().unwrap())
        .arg(file_b.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,Alice"))
        .stdout(predicates::str::contains("2,Bob"))
        .stdout(predicates::str::contains("3,Charlie"))
        .stdout(predicates::str::contains("4,David"));

    Ok(())
}

/// Test UNION with identical tables
#[test]
fn test_union_identical_tables() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name\n1,Alice\n2,Bob\n";
    let file_a = create_custom_csv(temp_dir.path(), "a.csv", content)?;
    let file_b = create_custom_csv(temp_dir.path(), "b.csv", content)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM a UNION SELECT * FROM b")
        .arg(file_a.to_str().unwrap())
        .arg(file_b.to_str().unwrap());

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8_lossy(&output);

    // With identical tables, UNION should only return 2 unique rows
    let alice_count = output_str.matches("1,Alice").count();
    let bob_count = output_str.matches("2,Bob").count();
    assert_eq!(alice_count, 1, "Alice should appear once after UNION");
    assert_eq!(bob_count, 1, "Bob should appear once after UNION");

    Ok(())
}

/// Test UNION ALL with identical tables
#[test]
fn test_union_all_identical_tables() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name\n1,Alice\n2,Bob\n";
    let file_a = create_custom_csv(temp_dir.path(), "a.csv", content)?;
    let file_b = create_custom_csv(temp_dir.path(), "b.csv", content)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM a UNION ALL SELECT * FROM b")
        .arg(file_a.to_str().unwrap())
        .arg(file_b.to_str().unwrap());

    let output = cmd.assert().success().get_output().stdout.clone();
    let output_str = String::from_utf8_lossy(&output);

    // UNION ALL should keep all 4 rows (2 from each table)
    let alice_count = output_str.matches("1,Alice").count();
    let bob_count = output_str.matches("2,Bob").count();
    assert_eq!(alice_count, 2, "Alice should appear twice with UNION ALL");
    assert_eq!(bob_count, 2, "Bob should appear twice with UNION ALL");

    Ok(())
}

/// Test UNION with empty second table
#[test]
fn test_union_with_empty_table() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content_a = "id,name\n1,Alice\n2,Bob\n";
    let content_b = "id,name\n";
    let file_a = create_custom_csv(temp_dir.path(), "a.csv", content_a)?;
    let file_b = create_custom_csv(temp_dir.path(), "b.csv", content_b)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM a UNION SELECT * FROM b")
        .arg(file_a.to_str().unwrap())
        .arg(file_b.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,Alice"))
        .stdout(predicates::str::contains("2,Bob"));

    Ok(())
}
