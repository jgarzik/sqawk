//! Tests for DDL (Data Definition Language) operations
//!
//! This file contains tests for DROP TABLE, ALTER TABLE, TRUNCATE, and CREATE TABLE AS SELECT.

use crate::helpers::{create_custom_csv, create_temp_dir};

#[test]
fn test_drop_table_basic() -> Result<(), Box<dyn std::error::Error>> {
    // Test that DROP TABLE queues the table for removal
    let temp_dir = create_temp_dir()?;
    let file_path = create_custom_csv(
        temp_dir.path(),
        "people.csv",
        "id,name,age\n1,Alice,32\n2,Bob,25\n",
    )?;

    // DROP TABLE should succeed (table queued for removal)
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("DROP TABLE people")
        .arg("-v")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("DropTable: queued drop"));

    Ok(())
}

#[test]
fn test_drop_table_nonexistent() -> Result<(), Box<dyn std::error::Error>> {
    // Test that DROP TABLE fails for non-existent table
    let temp_dir = create_temp_dir()?;
    let file_path = create_custom_csv(temp_dir.path(), "people.csv", "id,name,age\n1,Alice,32\n")?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("DROP TABLE nonexistent")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .failure()
        .stderr(predicates::str::contains("not found"));

    Ok(())
}

#[test]
fn test_drop_table_if_exists() -> Result<(), Box<dyn std::error::Error>> {
    // Test that DROP TABLE IF EXISTS doesn't error for non-existent tables
    let temp_dir = create_temp_dir()?;
    let file_path = create_custom_csv(
        temp_dir.path(),
        "people.csv",
        "id,name,age\n1,Alice,32\n2,Bob,25\n",
    )?;

    // Drop non-existent table with IF EXISTS (should not error)
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("DROP TABLE IF EXISTS nonexistent")
        .arg("-v")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("does not exist (IF EXISTS)"));

    Ok(())
}

#[test]
fn test_drop_table_if_exists_existing() -> Result<(), Box<dyn std::error::Error>> {
    // Test that DROP TABLE IF EXISTS works for existing tables
    let temp_dir = create_temp_dir()?;
    let file_path = create_custom_csv(
        temp_dir.path(),
        "people.csv",
        "id,name,age\n1,Alice,32\n2,Bob,25\n",
    )?;

    // Drop existing table with IF EXISTS
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("DROP TABLE IF EXISTS people")
        .arg("-v")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("DropTable: queued drop"));

    Ok(())
}

#[test]
fn test_truncate_table() -> Result<(), Box<dyn std::error::Error>> {
    // Test that TRUNCATE removes all rows from the table
    let temp_dir = create_temp_dir()?;
    let file_path = create_custom_csv(
        temp_dir.path(),
        "people.csv",
        "id,name,age\n1,Alice,32\n2,Bob,25\n3,Charlie,35\n",
    )?;

    // Truncate the table and verify it's empty
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("TRUNCATE TABLE people")
        .arg("-s")
        .arg("SELECT COUNT(*) as cnt FROM people")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("0"));

    Ok(())
}

#[test]
fn test_truncate_verbose() -> Result<(), Box<dyn std::error::Error>> {
    // Test TRUNCATE in verbose mode
    let temp_dir = create_temp_dir()?;
    let file_path = create_custom_csv(
        temp_dir.path(),
        "people.csv",
        "id,name,age\n1,Alice,32\n2,Bob,25\n",
    )?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("TRUNCATE TABLE people")
        .arg("-v")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("Truncate: queued truncate"));

    Ok(())
}

#[test]
fn test_alter_table_add_column() -> Result<(), Box<dyn std::error::Error>> {
    // Test that ALTER TABLE ADD COLUMN adds a new column with NULL values
    let temp_dir = create_temp_dir()?;
    let file_path = create_custom_csv(temp_dir.path(), "people.csv", "id,name\n1,Alice\n2,Bob\n")?;

    // Add a new column and verify
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("ALTER TABLE people ADD COLUMN age INTEGER")
        .arg("-s")
        .arg("SELECT * FROM people")
        .arg(file_path.to_str().unwrap());

    // New column should appear with NULL values (shown as empty)
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("id,name,age"));

    Ok(())
}

#[test]
fn test_alter_table_add_text_column() -> Result<(), Box<dyn std::error::Error>> {
    // Test ALTER TABLE ADD COLUMN with TEXT type
    let temp_dir = create_temp_dir()?;
    let file_path = create_custom_csv(temp_dir.path(), "people.csv", "id,name\n1,Alice\n2,Bob\n")?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("ALTER TABLE people ADD COLUMN email TEXT")
        .arg("-s")
        .arg("SELECT * FROM people")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("id,name,email"));

    Ok(())
}

#[test]
fn test_create_table_as_select() -> Result<(), Box<dyn std::error::Error>> {
    // Test CREATE TABLE AS SELECT creates a new table from query results
    let temp_dir = create_temp_dir()?;
    let file_path = create_custom_csv(
        temp_dir.path(),
        "people.csv",
        "id,name,age,department\n1,Alice,32,Engineering\n2,Bob,25,Sales\n3,Charlie,35,Engineering\n",
    )?;

    // Create new table from SELECT with WHERE clause
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("CREATE TABLE engineers AS SELECT id, name FROM people WHERE department = 'Engineering'")
        .arg("-s")
        .arg("SELECT * FROM engineers")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("id,name"))
        .stdout(predicates::str::contains("1,Alice"))
        .stdout(predicates::str::contains("3,Charlie"));

    Ok(())
}

#[test]
fn test_create_table_as_select_all_columns() -> Result<(), Box<dyn std::error::Error>> {
    // Test CREATE TABLE AS SELECT with all columns
    let temp_dir = create_temp_dir()?;
    let file_path = create_custom_csv(
        temp_dir.path(),
        "people.csv",
        "id,name,age\n1,Alice,32\n2,Bob,25\n",
    )?;

    // Create new table with SELECT *
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("CREATE TABLE backup AS SELECT * FROM people")
        .arg("-s")
        .arg("SELECT * FROM backup")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("id,name,age"))
        .stdout(predicates::str::contains("1,Alice,32"))
        .stdout(predicates::str::contains("2,Bob,25"));

    Ok(())
}

#[test]
fn test_create_table_as_select_with_filter() -> Result<(), Box<dyn std::error::Error>> {
    // Test CREATE TABLE AS SELECT with age filter
    let temp_dir = create_temp_dir()?;
    let file_path = create_custom_csv(
        temp_dir.path(),
        "people.csv",
        "id,name,age\n1,Alice,32\n2,Bob,25\n3,Charlie,35\n",
    )?;

    // Create new table with filtered data
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("CREATE TABLE seniors AS SELECT id, name FROM people WHERE age > 30")
        .arg("-s")
        .arg("SELECT COUNT(*) as cnt FROM seniors")
        .arg(file_path.to_str().unwrap());

    // Should have Alice (32) and Charlie (35) = 2 rows
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("2"));

    Ok(())
}

#[test]
fn test_create_table_verbose() -> Result<(), Box<dyn std::error::Error>> {
    // Test CREATE TABLE AS SELECT in verbose mode
    let temp_dir = create_temp_dir()?;
    let file_path = create_custom_csv(temp_dir.path(), "people.csv", "id,name\n1,Alice\n2,Bob\n")?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("CREATE TABLE copy AS SELECT * FROM people")
        .arg("-v")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("CreateTable"));

    Ok(())
}
