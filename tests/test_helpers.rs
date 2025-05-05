//! Test helpers for sqawk integration tests
//! 
//! This module provides helper functions and structs to simplify
//! writing integration tests for the sqawk application.

use std::fs;
use std::path::{Path, PathBuf};
use std::env;
use std::process::Command;

use assert_cmd::prelude::*;
use predicates::prelude::*;
use tempfile::TempDir;

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

/// Run a test using a temporary csv file created specifically for this test
pub fn run_test_case(test_case: SqawkTestCase) -> Result<(), Box<dyn std::error::Error>> {
    // Create a temporary directory for test files
    let temp_dir = create_temp_dir()?;
    let test_file = prepare_test_file(temp_dir.path())?;
    
    // Call the common test function
    run_test_case_with_file(test_case, test_file)
}

/// Run a test using a static test file that already exists in the project
pub fn run_test_case_with_static_file(
    test_case: SqawkTestCase, 
    test_file: PathBuf
) -> Result<(), Box<dyn std::error::Error>> {
    run_test_case_with_file(test_case, test_file)
}

/// Common test helper function used by both temporary and static file test runners
fn run_test_case_with_file(
    test_case: SqawkTestCase, 
    test_file: PathBuf
) -> Result<(), Box<dyn std::error::Error>> {
    // Build the command
    let mut cmd = Command::cargo_bin("sqawk")?;
    
    // Add SQL statement
    cmd.arg("-s").arg(&test_case.sql);
    
    // Add verbose flag if requested
    if test_case.verbose {
        cmd.arg("-v");
    }
    
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

/// Helper function to create a standard test CSV file with people data
/// This uses a template identical to the static sample.csv test file
pub fn prepare_test_file(dir: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let file_path = dir.join("people.csv");
    let content = "id,name,age\n1,Alice,30\n2,Bob,25\n3,Charlie,35\n";
    fs::write(&file_path, content)?;
    Ok(file_path)
}

/// Helper function to get the path to the static sample.csv test file
pub fn get_static_sample_file() -> PathBuf {
    PathBuf::from("tests/data/sample.csv")
}

/// Helper function to create a test CSV file with custom data
pub fn create_custom_csv(dir: &Path, filename: &str, content: &str) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let file_path = dir.join(filename);
    fs::write(&file_path, content)?;
    Ok(file_path)
}