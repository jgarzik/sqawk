//! Test helpers for sqawk integration tests
//!
//! This module provides helper functions and structs to simplify
//! writing integration tests for the sqawk application.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use predicates::prelude::*;
use tempfile::TempDir;

/// Create a Command for running the sqawk binary
///
/// Uses the cargo_bin_cmd! macro which is compatible with custom cargo build directories.
#[macro_export]
macro_rules! sqawk_cmd {
    () => {
        assert_cmd::cargo::cargo_bin_cmd!("sqawk")
    };
}

/// Represents a test case for sqawk
pub struct SqawkTestCase {
    /// The SQL query to execute
    pub sql: String,
    /// Additional command line arguments
    pub args: Vec<String>,
    /// Table name to use (optional)
    pub table_name: Option<String>,
    /// Expected strings in stdout
    pub expected_stdout: Vec<String>,
    /// Expected strings in stderr
    pub expected_stderr: Vec<String>,
    /// Whether the command is expected to succeed
    pub should_succeed: bool,
    /// Whether to use verbose mode
    pub verbose: bool,
}

impl Default for SqawkTestCase {
    fn default() -> Self {
        SqawkTestCase {
            sql: String::new(),
            args: Vec::new(),
            table_name: None,
            expected_stdout: Vec::new(),
            expected_stderr: Vec::new(),
            should_succeed: true,
            verbose: false,
        }
    }
}

/// Run a test using the standard people.csv static fixture
pub fn run_test_case(test_case: SqawkTestCase) -> Result<(), Box<dyn std::error::Error>> {
    run_test_case_with_static_file(test_case, get_people_file())
}

/// Run a test using a static test file that already exists in the project
/// For write operations (--write flag), makes a copy of the file to ensure the original is never modified
pub fn run_test_case_with_static_file(
    test_case: SqawkTestCase,
    static_file: PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    // Check if any arguments contain --write or -w flag
    let needs_copy = test_case
        .args
        .iter()
        .any(|arg| arg == "--write" || arg == "-w");

    if needs_copy {
        // Create a temporary directory
        let temp_dir = create_temp_dir()?;

        // Copy the static file to the temp directory to protect the original
        let file_name = static_file.file_name().unwrap().to_str().unwrap();
        let temp_file_path = temp_dir.path().join(file_name);
        fs::copy(&static_file, &temp_file_path)?;

        // Use the copied file for the test
        run_test_case_with_file(test_case, temp_file_path)
    } else {
        // For read-only operations, use the original file directly
        run_test_case_with_file(test_case, static_file)
    }
}

/// Common test helper function used by both temporary and static file test runners
fn run_test_case_with_file(
    test_case: SqawkTestCase,
    test_file: PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    // Build the command using cargo_bin_cmd! macro (compatible with custom build dirs)
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");

    // Add SQL statement
    cmd.arg("-s").arg(&test_case.sql);

    // Add verbose flag if requested
    if test_case.verbose {
        cmd.arg("-v");
    }

    // Not writing to files is now the default behavior, so no need for additional flags

    // Add any additional arguments
    for arg in &test_case.args {
        cmd.arg(arg);
    }

    // Add table name mapping if specified, otherwise use the default file
    if let Some(table_name) = test_case.table_name {
        cmd.arg(format!("{}={}", table_name, test_file.to_str().unwrap()));
    } else {
        cmd.arg(test_file.to_str().unwrap());
    }

    // Set up assertions
    let mut assert = cmd.assert();

    // Check success/failure
    if test_case.should_succeed {
        assert = assert.success();
    } else {
        assert = assert.failure();
    }

    // Check stdout expectations
    for expected in test_case.expected_stdout {
        assert = assert.stdout(predicate::str::contains(expected));
    }

    // Check stderr expectations
    for expected in test_case.expected_stderr {
        assert = assert.stderr(predicate::str::contains(expected));
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Exact-output helpers
//
// The `contains`-based assertions above are substring matches: a test expecting
// "Engineering,3" still passes when three extra wrong rows are emitted, and
// passes regardless of row order. Every defect in the step-0 red list survived
// the suite for exactly that reason.
//
// The helpers below assert the *complete* stdout -- every row, in order. Use
// these for new tests.
// ---------------------------------------------------------------------------

/// Run `sql` against `files` and return (stdout, stderr, success) verbatim.
///
/// Each entry in `files` is passed to sqawk as-is, so both `path.csv` and
/// `name=path.csv` forms work.
pub fn run_sql(
    sql: &str,
    files: &[&str],
    extra_args: &[&str],
) -> Result<(String, String, bool), Box<dyn std::error::Error>> {
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s").arg(sql);
    for arg in extra_args {
        cmd.arg(arg);
    }
    for file in files {
        cmd.arg(file);
    }

    let output = cmd.output()?;
    Ok((
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
        output.status.success(),
    ))
}

/// Assert that `sql` over `files` produces exactly `expected` on stdout.
///
/// Both sides are trimmed of leading/trailing blank lines and of trailing
/// whitespace on each line, so tests can use indented raw strings. Everything
/// else -- row content, column order, row order, row count -- must match.
pub fn assert_query(
    files: &[&str],
    sql: &str,
    expected: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (stdout, stderr, success) = run_sql(sql, files, &[])?;
    assert!(
        success,
        "query failed unexpectedly\n  sql: {sql}\n  stderr: {stderr}"
    );

    let actual = normalize_output(&stdout);
    let want = normalize_output(expected);
    assert_eq!(
        actual, want,
        "\n=== query ===\n{sql}\n=== expected ===\n{want}\n=== actual ===\n{actual}\n"
    );
    Ok(())
}

/// Assert that `sql` over `files` fails, with `expected_err` appearing in stderr.
pub fn assert_query_fails(
    files: &[&str],
    sql: &str,
    expected_err: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (stdout, stderr, success) = run_sql(sql, files, &[])?;
    assert!(
        !success,
        "query unexpectedly succeeded\n  sql: {sql}\n  stdout: {stdout}"
    );
    assert!(
        stderr.contains(expected_err),
        "\n=== query ===\n{sql}\n=== expected stderr to contain ===\n{expected_err}\n=== actual stderr ===\n{stderr}\n"
    );
    Ok(())
}

/// Trim trailing whitespace per line and drop leading/trailing blank lines.
fn normalize_output(s: &str) -> String {
    s.lines()
        .map(|l| l.trim_end())
        .collect::<Vec<_>>()
        .join("\n")
        .trim_matches('\n')
        .to_string()
}

/// Run `sql` with `--write` against a private copy of `fixture`, and return the
/// resulting file contents.
///
/// Needed because modifications are in-memory only without `--write`, and
/// `UPDATE ...; SELECT ...` in a single invocation does not currently emit the
/// SELECT's rows -- so writeback is the way to observe what a DML statement
/// actually did.
pub fn run_with_write(
    fixture: &str,
    table: &str,
    sql: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let dir = create_temp_dir()?;
    let src = PathBuf::from(fixture);
    let dest = dir.path().join(src.file_name().unwrap());
    fs::copy(&src, &dest)?;

    let spec = format!("{}={}", table, dest.to_str().unwrap());
    let (_stdout, stderr, success) = run_sql(sql, &[&spec], &["--write"])?;
    assert!(
        success,
        "write query failed\n  sql: {sql}\n  stderr: {stderr}"
    );

    Ok(normalize_output(&fs::read_to_string(&dest)?))
}

/// Assert that `sql` run with `--write` leaves `fixture` with exactly
/// `expected` contents.
pub fn assert_after_write(
    fixture: &str,
    table: &str,
    sql: &str,
    expected: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let actual = run_with_write(fixture, table, sql)?;
    let want = normalize_output(expected);
    assert_eq!(
        actual, want,
        "\n=== sql ===\n{sql}\n=== expected file ===\n{want}\n=== actual file ===\n{actual}\n"
    );
    Ok(())
}

/// `assert_query` over ad-hoc tables: each `(file_name, content)` is
/// written to a fresh temp dir and loaded under its file stem.
pub fn assert_query_on(
    files: &[(&str, &str)],
    sql: &str,
    expected: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let dir = create_temp_dir()?;
    let mut paths = Vec::new();
    for (name, content) in files {
        let p = dir.path().join(name);
        fs::write(&p, content)?;
        paths.push(p.to_str().unwrap().to_string());
    }
    let refs: Vec<&str> = paths.iter().map(String::as_str).collect();
    assert_query(&refs, sql, expected)
}

/// `assert_query_fails` over ad-hoc tables (see [`assert_query_on`]).
pub fn assert_query_on_fails(
    files: &[(&str, &str)],
    sql: &str,
    expected_err: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let dir = create_temp_dir()?;
    let mut paths = Vec::new();
    for (name, content) in files {
        let p = dir.path().join(name);
        fs::write(&p, content)?;
        paths.push(p.to_str().unwrap().to_string());
    }
    let refs: Vec<&str> = paths.iter().map(String::as_str).collect();
    assert_query_fails(&refs, sql, expected_err)
}

/// A one-row table `one(id)`, for queries over constant expressions.
pub const ONE: (&str, &str) = ("one.csv", "id\n1\n");

/// Path to the nullable.csv fixture (contains empty fields -> NULL values).
///
/// No other fixture in tests/data/ has an empty field, so nothing in the suite
/// currently exercises a NULL flowing into a comparison.
pub fn nullable_csv() -> &'static str {
    "tests/data/nullable.csv"
}

/// Path to employees.csv as a plain &str, for the exact-output helpers.
pub fn employees_csv() -> &'static str {
    "tests/data/employees.csv"
}

/// Path to departments.csv as a plain &str, for the exact-output helpers.
pub fn departments_csv() -> &'static str {
    "tests/data/departments.csv"
}

/// Path to users.csv as a plain &str, for the exact-output helpers.
pub fn users_csv() -> &'static str {
    "tests/data/users.csv"
}

/// Path to orders.csv as a plain &str, for the exact-output helpers.
pub fn orders_csv() -> &'static str {
    "tests/data/orders.csv"
}

/// Helper function to create a temp directory for tests, respecting CARGO_TARGET_TMPDIR if set
pub fn create_temp_dir() -> Result<TempDir, Box<dyn std::error::Error>> {
    if let Ok(cargo_target_tmpdir) = env::var("CARGO_TARGET_TMPDIR") {
        // If CARGO_TARGET_TMPDIR is set, ensure the directory exists
        fs::create_dir_all(&cargo_target_tmpdir)?;

        // Then create the temporary directory there
        let temp_dir = TempDir::new_in(cargo_target_tmpdir)?;
        Ok(temp_dir)
    } else {
        // Otherwise, use the default tempfile behavior
        let temp_dir = TempDir::new()?;
        Ok(temp_dir)
    }
}

/// Path to the employees.csv fixture (comprehensive test data with salary, department, role)
pub fn get_employees_file() -> PathBuf {
    PathBuf::from("tests/data/employees.csv")
}

/// Path to the people.csv fixture (simple test data: id, name, age)
pub fn get_people_file() -> PathBuf {
    PathBuf::from("tests/data/people.csv")
}

/// Path to the departments.csv fixture
pub fn get_departments_file() -> PathBuf {
    PathBuf::from("tests/data/departments.csv")
}

/// Path to the duplicates.csv fixture
pub fn get_duplicates_file() -> PathBuf {
    PathBuf::from("tests/data/duplicates.csv")
}

/// Path to the strings.csv fixture (for string function tests)
pub fn get_strings_file() -> PathBuf {
    PathBuf::from("tests/data/strings.csv")
}

/// Path to boundaries.csv fixture (for comparison tests)
pub fn get_boundaries_file() -> PathBuf {
    PathBuf::from("tests/data/boundaries.csv")
}

/// Path to users.csv fixture (for join tests)
pub fn get_users_file() -> PathBuf {
    PathBuf::from("tests/data/users.csv")
}

/// Path to orders.csv fixture (for join tests)
pub fn get_orders_file() -> PathBuf {
    PathBuf::from("tests/data/orders.csv")
}

/// Path to products.csv fixture (for join tests)
pub fn get_products_file() -> PathBuf {
    PathBuf::from("tests/data/products.csv")
}

/// Path to employees.tsv fixture (for delimiter tests)
pub fn get_employees_tsv_file() -> PathBuf {
    PathBuf::from("tests/data/employees.tsv")
}

/// Path to contacts.txt fixture (for colon-delimiter tests)
pub fn get_contacts_file() -> PathBuf {
    PathBuf::from("tests/data/contacts.txt")
}

/// Path to sample.csv fixture (simple test data: id, name, age)
pub fn get_sample_file() -> PathBuf {
    PathBuf::from("tests/data/sample.csv")
}

/// Helper function to create a test CSV file with custom data
pub fn create_custom_csv(
    dir: &Path,
    filename: &str,
    content: &str,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let file_path = dir.join(filename);
    fs::write(&file_path, content)?;
    Ok(file_path)
}
