//! Tests for output functionality
//!
//! This module tests both stdout output and file writeback behavior,
//! including the atomic write pattern (temp file + rename).

use crate::helpers::{create_custom_csv, create_temp_dir};
use std::fs;

// ============================================================================
// STDOUT OUTPUT TESTS
// ============================================================================

#[test]
fn test_stdout_select_csv() -> Result<(), Box<dyn std::error::Error>> {
    // Test that SELECT output goes to stdout in CSV format
    let temp_dir = create_temp_dir()?;
    let file_path = create_custom_csv(
        temp_dir.path(),
        "data.csv",
        "id,name,value\n1,Alice,100\n2,Bob,200\n",
    )?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM data")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("id,name,value"))
        .stdout(predicates::str::contains("1,Alice,100"))
        .stdout(predicates::str::contains("2,Bob,200"));

    Ok(())
}

#[test]
fn test_stdout_select_with_delimiter() -> Result<(), Box<dyn std::error::Error>> {
    // Test that SELECT output uses the correct delimiter for TSV files
    let temp_dir = create_temp_dir()?;
    let file_path = create_custom_csv(
        temp_dir.path(),
        "data.tsv",
        "id\tname\tvalue\n1\tAlice\t100\n2\tBob\t200\n",
    )?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-F")
        .arg("\t")
        .arg("-s")
        .arg("SELECT * FROM data")
        .arg(file_path.to_str().unwrap());

    // Output should be tab-delimited
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("id\tname\tvalue"))
        .stdout(predicates::str::contains("1\tAlice\t100"))
        .stdout(predicates::str::contains("2\tBob\t200"));

    Ok(())
}

#[test]
fn test_stdout_empty_result() -> Result<(), Box<dyn std::error::Error>> {
    // Test that queries with no matching rows still output headers
    let temp_dir = create_temp_dir()?;
    let file_path = create_custom_csv(
        temp_dir.path(),
        "data.csv",
        "id,name,value\n1,Alice,100\n2,Bob,200\n",
    )?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM data WHERE id = 999")
        .arg(file_path.to_str().unwrap());

    // Should succeed with just headers (no data rows match)
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("id,name,value"));

    Ok(())
}

// ============================================================================
// FILE OUTPUT TESTS (ATOMIC WRITEBACK)
// ============================================================================

#[test]
fn test_file_write_csv() -> Result<(), Box<dyn std::error::Error>> {
    // Test that --write flag correctly saves CSV modifications to file
    let temp_dir = create_temp_dir()?;
    let file_path = create_custom_csv(
        temp_dir.path(),
        "data.csv",
        "id,name,value\n1,Alice,100\n2,Bob,200\n",
    )?;

    // Update a value and write back
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("UPDATE data SET value = 150 WHERE name = 'Alice'")
        .arg("--write")
        .arg(file_path.to_str().unwrap());

    cmd.assert().success();

    // Verify file was updated
    let content = fs::read_to_string(&file_path)?;
    assert!(
        content.contains("1,Alice,150"),
        "File should contain updated value"
    );
    assert!(
        !content.contains("1,Alice,100"),
        "File should not contain old value"
    );
    assert!(
        content.contains("2,Bob,200"),
        "Other rows should be unchanged"
    );

    Ok(())
}

#[test]
fn test_file_write_tsv() -> Result<(), Box<dyn std::error::Error>> {
    // Test that --write flag correctly saves TSV modifications to file
    let temp_dir = create_temp_dir()?;
    let file_path = create_custom_csv(
        temp_dir.path(),
        "data.tsv",
        "id\tname\tvalue\n1\tAlice\t100\n2\tBob\t200\n",
    )?;

    // Update a value and write back
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-F")
        .arg("\t")
        .arg("-s")
        .arg("UPDATE data SET value = 150 WHERE name = 'Alice'")
        .arg("--write")
        .arg(file_path.to_str().unwrap());

    cmd.assert().success();

    // Verify file was updated with tab delimiter preserved
    let content = fs::read_to_string(&file_path)?;
    assert!(
        content.contains("1\tAlice\t150"),
        "File should contain updated value with tabs"
    );
    assert!(
        !content.contains("1\tAlice\t100"),
        "File should not contain old value"
    );
    assert!(
        content.contains("2\tBob\t200"),
        "Other rows should be unchanged"
    );

    Ok(())
}

#[test]
fn test_file_write_insert() -> Result<(), Box<dyn std::error::Error>> {
    // Test INSERT with --write correctly persists new rows
    let temp_dir = create_temp_dir()?;
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", "id,name,value\n1,Alice,100\n")?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("INSERT INTO data (id, name, value) VALUES (2, 'Bob', 200)")
        .arg("--write")
        .arg(file_path.to_str().unwrap());

    cmd.assert().success();

    // Verify file contains the new row
    let content = fs::read_to_string(&file_path)?;
    assert!(content.contains("1,Alice,100"), "Original row should exist");
    assert!(content.contains("2,Bob,200"), "New row should be added");

    Ok(())
}

#[test]
fn test_file_write_delete() -> Result<(), Box<dyn std::error::Error>> {
    // Test DELETE with --write correctly removes rows from file
    let temp_dir = create_temp_dir()?;
    let file_path = create_custom_csv(
        temp_dir.path(),
        "data.csv",
        "id,name,value\n1,Alice,100\n2,Bob,200\n3,Charlie,300\n",
    )?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("DELETE FROM data WHERE name = 'Bob'")
        .arg("--write")
        .arg(file_path.to_str().unwrap());

    cmd.assert().success();

    // Verify Bob was deleted
    let content = fs::read_to_string(&file_path)?;
    assert!(content.contains("1,Alice,100"), "Alice should remain");
    assert!(!content.contains("2,Bob,200"), "Bob should be deleted");
    assert!(content.contains("3,Charlie,300"), "Charlie should remain");

    Ok(())
}

#[test]
fn test_file_no_write_by_default() -> Result<(), Box<dyn std::error::Error>> {
    // Test that modifications are NOT written without --write flag
    let temp_dir = create_temp_dir()?;
    let original_content = "id,name,value\n1,Alice,100\n2,Bob,200\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", original_content)?;

    // Update without --write flag
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("UPDATE data SET value = 999 WHERE name = 'Alice'")
        .arg(file_path.to_str().unwrap());

    cmd.assert().success();

    // Verify file was NOT modified
    let content = fs::read_to_string(&file_path)?;
    assert_eq!(
        content, original_content,
        "File should be unchanged without --write"
    );

    Ok(())
}

#[test]
fn test_file_write_preserves_original_on_read_only_query() -> Result<(), Box<dyn std::error::Error>>
{
    // Test that SELECT with --write doesn't modify file (no changes to persist)
    let temp_dir = create_temp_dir()?;
    let original_content = "id,name,value\n1,Alice,100\n2,Bob,200\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", original_content)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM data")
        .arg("--write")
        .arg(file_path.to_str().unwrap());

    cmd.assert().success();

    // Verify file is unchanged (SELECT doesn't modify data)
    let content = fs::read_to_string(&file_path)?;
    assert_eq!(
        content, original_content,
        "File should be unchanged after SELECT"
    );

    Ok(())
}

#[test]
fn test_file_write_multiple_operations() -> Result<(), Box<dyn std::error::Error>> {
    // Test multiple SQL operations with single --write at end
    let temp_dir = create_temp_dir()?;
    let file_path = create_custom_csv(
        temp_dir.path(),
        "data.csv",
        "id,name,value\n1,Alice,100\n2,Bob,200\n3,Charlie,300\n",
    )?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("UPDATE data SET value = 150 WHERE name = 'Alice'")
        .arg("-s")
        .arg("DELETE FROM data WHERE name = 'Bob'")
        .arg("-s")
        .arg("INSERT INTO data (id, name, value) VALUES (4, 'Diana', 400)")
        .arg("--write")
        .arg(file_path.to_str().unwrap());

    cmd.assert().success();

    // Verify all operations were applied
    let content = fs::read_to_string(&file_path)?;
    assert!(
        content.contains("1,Alice,150"),
        "Alice value should be updated"
    );
    assert!(!content.contains("2,Bob,200"), "Bob should be deleted");
    assert!(
        content.contains("3,Charlie,300"),
        "Charlie should be unchanged"
    );
    assert!(content.contains("4,Diana,400"), "Diana should be added");

    Ok(())
}

// ============================================================================
// RESULTS OF EARLIER STATEMENTS SURVIVE A LATER ERROR
// ============================================================================

/// Each statement's result set is printed when that statement finishes, so
/// a later failure does not swallow the output of the ones before it (the
/// sqlite3 shell behaves the same way).
#[test]
fn earlier_results_print_before_a_later_error() -> Result<(), Box<dyn std::error::Error>> {
    let dir = create_temp_dir()?;
    let t = create_custom_csv(dir.path(), "t.csv", "k\n1\n2\n")?;
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT 1 AS a; SELECT k FROM t; SELECT nosuch FROM t; SELECT 9 AS never")
        .arg(t.to_str().unwrap());
    cmd.assert()
        .code(1)
        .stdout("a\n1\nk\n1\n2\n")
        .stderr("sqawk: no such column: nosuch\n");
    Ok(())
}

/// The same through `-f`: a script file is one statement list.
#[test]
fn earlier_results_print_before_a_later_error_in_a_script_file()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = create_temp_dir()?;
    let t = create_custom_csv(dir.path(), "t.csv", "k\n1\n")?;
    let script = create_custom_csv(
        dir.path(),
        "q.sql",
        "SELECT k FROM t;\nSELECT nope FROM t;\n",
    )?;
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-f")
        .arg(script.to_str().unwrap())
        .arg(t.to_str().unwrap());
    cmd.assert().code(1).stdout("k\n1\n");
    Ok(())
}

/// With --write, a failing script still writes nothing, though the rows the
/// earlier statements produced are printed.
#[test]
fn failed_script_prints_earlier_rows_and_writes_nothing() -> Result<(), Box<dyn std::error::Error>>
{
    let dir = create_temp_dir()?;
    let t = create_custom_csv(dir.path(), "t.csv", "k\n1\n2\n")?;
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("--write")
        .arg("-s")
        .arg("UPDATE t SET k = k + 10; SELECT k FROM t; SELECT bad FROM t")
        .arg(t.to_str().unwrap());
    cmd.assert().code(1).stdout("k\n11\n12\n");
    assert_eq!(fs::read_to_string(&t)?, "k\n1\n2\n");
    Ok(())
}

/// A derived table's rows are an intermediate result: only the statement's
/// own result set reaches stdout.
#[test]
fn derived_table_rows_are_not_printed() -> Result<(), Box<dyn std::error::Error>> {
    let dir = create_temp_dir()?;
    let one = create_custom_csv(dir.path(), "one.csv", "id\n1\n")?;
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT x + 1 AS y FROM (SELECT 1 AS x) d; WITH c AS (SELECT 5 AS z) SELECT z FROM c")
        .arg(one.to_str().unwrap());
    cmd.assert().success().stdout("y\n2\nz\n5\n");
    Ok(())
}

/// The REPL prints the earlier statements of a failing line too.
#[test]
fn repl_line_prints_earlier_results_before_an_error() -> Result<(), Box<dyn std::error::Error>> {
    let dir = create_temp_dir()?;
    let t = create_custom_csv(dir.path(), "t.csv", "k\n7\n")?;
    let out = crate::common::run_repl_script(
        "SELECT k FROM t; SELECT nosuch FROM t;\nSELECT 2 AS after;\n",
        &[t.to_str().unwrap()],
        None,
        false,
    );
    assert!(out.contains("k\n7\n"), "earlier rows missing: {out:?}");
    assert!(out.contains("after\n2\n"), "next line missing: {out:?}");
    Ok(())
}

// ============================================================================
// ROWS STREAM TO STDOUT AS THEY ARE PRODUCED
// ============================================================================

/// A runtime error part-way through a statement keeps the rows produced
/// before it (sqlite3 prints them too), then reports the error.
#[test]
fn rows_before_a_runtime_error_are_printed() -> Result<(), Box<dyn std::error::Error>> {
    let dir = create_temp_dir()?;
    let t = create_custom_csv(dir.path(), "t.csv", "k\n1\n-2\n-9223372036854775808\n4\n")?;
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT abs(k) AS a FROM t; SELECT 9 AS never")
        .arg(t.to_str().unwrap());
    cmd.assert()
        .code(1)
        .stdout("a\n1\n2\n")
        .stderr("sqawk: integer overflow\n");
    Ok(())
}

/// The output delimiter and --no-output-header apply to streamed rows, and
/// fields are quoted as in a written file.
#[test]
fn streamed_rows_use_output_delimiter_and_header_flag() -> Result<(), Box<dyn std::error::Error>> {
    let dir = create_temp_dir()?;
    let t = create_custom_csv(dir.path(), "t.csv", "k;v\n1;\"a;b\"\n2;\" x\"\n")?;
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-F")
        .arg(";")
        .arg("--no-output-header")
        .arg("-s")
        .arg("SELECT k, v, NULL AS n, 1.5 AS r FROM t")
        .arg(t.to_str().unwrap());
    cmd.assert()
        .success()
        .stdout("1;\"a;b\";;1.5\n2;\" x\";;1.5\n");
    Ok(())
}

/// An empty result still prints its header, once per statement; a
/// statement with no result columns (DML) prints nothing.
#[test]
fn empty_result_prints_header_only() -> Result<(), Box<dyn std::error::Error>> {
    let dir = create_temp_dir()?;
    let t = create_custom_csv(dir.path(), "t.csv", "k\n1\n")?;
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT k AS a, k AS b FROM t WHERE k > 5; DELETE FROM t; SELECT k FROM t")
        .arg(t.to_str().unwrap());
    cmd.assert().success().stdout("a,b\nk\n");
    Ok(())
}

/// With -v the row count is reported after the statement's rows.
#[test]
fn verbose_row_count_follows_the_rows() -> Result<(), Box<dyn std::error::Error>> {
    let dir = create_temp_dir()?;
    let t = create_custom_csv(dir.path(), "t.csv", "k\n1\n2\n")?;
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-v")
        .arg("-s")
        .arg("SELECT k FROM t")
        .arg(t.to_str().unwrap());
    let out = cmd.assert().success().get_output().clone();
    assert_eq!(String::from_utf8(out.stdout)?, "k\n1\n2\n");
    let err = String::from_utf8(out.stderr)?;
    assert!(err.contains("Query returned 2 rows"), "{err}");
    Ok(())
}
