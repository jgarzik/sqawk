//! Tests for DDL (Data Definition Language) operations
//!
//! This file contains tests for DROP TABLE, ALTER TABLE, TRUNCATE, CREATE TABLE AS SELECT,
//! and CREATE TABLE with LOCATION clause.

use crate::helpers::{create_custom_csv, create_temp_dir};
use std::fs;

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

// =============================================================================
// CREATE TABLE with LOCATION tests
// =============================================================================

#[test]
fn test_create_table_with_location_creates_empty_table() -> Result<(), Box<dyn std::error::Error>> {
    // Test CREATE TABLE with LOCATION creates an empty table with specified schema
    let temp_dir = create_temp_dir()?;
    let output_file = temp_dir.path().join("output.csv");

    let dummy_file = create_custom_csv(temp_dir.path(), "dummy.csv", "x\n1\n")?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg(format!(
            "CREATE TABLE mydata (id INT, name TEXT, value INT) LOCATION '{}'",
            output_file.to_str().unwrap()
        ))
        .arg("-s")
        .arg("SELECT * FROM mydata")
        .arg(dummy_file.to_str().unwrap());

    // Table exists but is empty (only header row in output)
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("id,name,value"));

    Ok(())
}

#[test]
fn test_create_table_with_location_insert_and_write() -> Result<(), Box<dyn std::error::Error>> {
    // Test INSERT into table with LOCATION, then write back
    let temp_dir = create_temp_dir()?;
    let output_file = temp_dir.path().join("output.csv");

    let dummy_file = create_custom_csv(temp_dir.path(), "dummy.csv", "x\n1\n")?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg(format!(
            "CREATE TABLE mydata (id INT, name TEXT) LOCATION '{}'",
            output_file.to_str().unwrap()
        ))
        .arg("-s")
        .arg("INSERT INTO mydata VALUES (1, 'Alice'), (2, 'Bob')")
        .arg("-s")
        .arg("SELECT * FROM mydata")
        .arg("--write")
        .arg(dummy_file.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("Alice"))
        .stdout(predicates::str::contains("Bob"));

    // Verify the file was created with the data
    let contents = fs::read_to_string(&output_file)?;
    assert!(contents.contains("Alice"), "File should contain Alice");
    assert!(contents.contains("Bob"), "File should contain Bob");

    Ok(())
}

#[test]
fn test_create_table_with_location_and_delimiter() -> Result<(), Box<dyn std::error::Error>> {
    // Test CREATE TABLE with LOCATION and custom tab delimiter
    let temp_dir = create_temp_dir()?;
    let output_file = temp_dir.path().join("output.tsv");

    let dummy_file = create_custom_csv(temp_dir.path(), "dummy.csv", "x\n1\n")?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg(format!(
            "CREATE TABLE mydata (id INT, name TEXT) LOCATION '{}' STORED AS TEXTFILE WITH (DELIMITER='\t')",
            output_file.to_str().unwrap()
        ))
        .arg("-s")
        .arg("INSERT INTO mydata VALUES (1, 'Alice')")
        .arg("--write")
        .arg(dummy_file.to_str().unwrap());

    cmd.assert().success();

    // Verify the file was created with tab delimiter
    let contents = fs::read_to_string(&output_file)?;
    assert!(contents.contains('\t'), "File should use tab delimiter");
    assert!(contents.contains("Alice"), "File should contain Alice");

    Ok(())
}

#[test]
fn test_create_table_with_location_multiple_operations() -> Result<(), Box<dyn std::error::Error>> {
    // Test CREATE TABLE with LOCATION followed by INSERT, UPDATE, SELECT
    let temp_dir = create_temp_dir()?;
    let output_file = temp_dir.path().join("output.csv");

    let dummy_file = create_custom_csv(temp_dir.path(), "dummy.csv", "x\n1\n")?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg(format!(
            "CREATE TABLE mydata (id INT, name TEXT, value INT) LOCATION '{}'",
            output_file.to_str().unwrap()
        ))
        .arg("-s")
        .arg("INSERT INTO mydata VALUES (1, 'Alice', 100), (2, 'Bob', 200)")
        .arg("-s")
        .arg("UPDATE mydata SET value = 999 WHERE name = 'Alice'")
        .arg("-s")
        .arg("SELECT name, value FROM mydata WHERE value > 500")
        .arg("--write")
        .arg(dummy_file.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("Alice"))
        .stdout(predicates::str::contains("999"));

    // Verify the file contains the updated data
    let contents = fs::read_to_string(&output_file)?;
    assert!(
        contents.contains("999"),
        "File should contain updated value"
    );

    Ok(())
}
