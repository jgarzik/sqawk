//! Tests for delimiter options in sqawk
//!
//! Tests for different file delimiter options (-F flag) with tab and colon separators.

use crate::helpers::create_temp_dir;
use assert_cmd::Command;
use std::fs;
use std::io::Write;
use std::path::PathBuf;

// Helper function to create a tab-delimited test file
fn create_tab_delimited_file() -> Result<(tempfile::TempDir, PathBuf), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let file_path = temp_dir.path().join("employees.tsv");

    // Create a tab-delimited file for testing
    let content = "id\tname\tsalary\tdepartment\n1\tAlice\t75000\tEngineering\n2\tBob\t65000\tMarketing\n3\tCharlie\t85000\tEngineering\n4\tDavid\t60000\tSales\n";

    let mut file = fs::File::create(&file_path)?;
    file.write_all(content.as_bytes())?;

    // Return both the TempDir (to keep it alive) and the file path
    Ok((temp_dir, file_path))
}

// Helper function to create a colon-delimited test file
fn create_colon_delimited_file() -> Result<(tempfile::TempDir, PathBuf), Box<dyn std::error::Error>>
{
    let temp_dir = create_temp_dir()?;
    let file_path = temp_dir.path().join("contacts.txt");

    // Create a colon-delimited file for testing
    let content = "id:name:email:phone\n1:Alice:alice@example.com:555-1234\n2:Bob:bob@example.com:555-5678\n3:Charlie:charlie@example.com:555-9012\n5:David:david@example.com:555-3456\n10:Eve:eve@example.com:555-7890\n";

    let mut file = fs::File::create(&file_path)?;
    file.write_all(content.as_bytes())?;

    // Return both the TempDir (to keep it alive) and the file path
    Ok((temp_dir, file_path))
}

#[test]
fn test_tab_delimiter() -> Result<(), Box<dyn std::error::Error>> {
    // Create a tab-delimited file - keep temp_dir alive for the test duration
    let (_temp_dir, file_path) = create_tab_delimited_file()?;

    // Run sqawk with tab delimiter
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM employees WHERE salary > 70000")
        .arg("-F")
        .arg("\t")
        .arg(file_path.to_str().unwrap())
        .arg("-v");

    // Just check if the command executes successfully
    cmd.assert().success();

    // Verify that the output contains the expected data
    let output = cmd.output()?;
    let stdout = String::from_utf8(output.stdout)?;

    // Check that header and desired rows are present
    assert!(
        stdout.contains("id,name,salary,department"),
        "Header row should be present"
    );
    assert!(
        stdout.contains("1,Alice,75000,Engineering"),
        "Row for Alice should be present (salary > 70000)"
    );
    assert!(
        stdout.contains("3,Charlie,85000,Engineering"),
        "Row for Charlie should be present (salary > 70000)"
    );

    // Check that we don't see rows with salary <= 70000
    assert!(
        !stdout.contains("Bob") || !stdout.contains("65000"),
        "Should not contain Bob with salary 65000 (salary <= 70000)"
    );
    assert!(
        !stdout.contains("David") || !stdout.contains("60000"),
        "Should not contain David with salary 60000 (salary <= 70000)"
    );

    Ok(())
}

#[test]
fn test_colon_delimiter() -> Result<(), Box<dyn std::error::Error>> {
    // Create a colon-delimited file - keep temp_dir alive for the test duration
    let (_temp_dir, file_path) = create_colon_delimited_file()?;

    // Run sqawk with colon delimiter
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT id, name, email FROM contacts WHERE id > 5")
        .arg("-F")
        .arg(":")
        .arg(file_path.to_str().unwrap())
        .arg("-v");

    // Just check if the command executes successfully
    cmd.assert().success();

    // Verify that the output contains the expected data
    let output = cmd.output()?;
    let stdout = String::from_utf8(output.stdout)?;

    // Check that header and desired rows are present
    assert!(
        stdout.contains("id,name,email"),
        "Header row should be present"
    );
    assert!(
        stdout.contains("10,Eve,eve@example.com"),
        "Row for Eve should be present (id > 5)"
    );

    // Check that we don't see rows with id <= 5
    assert!(
        !stdout.contains("Alice") || !stdout.contains("alice@example.com"),
        "Should not contain Alice (id <= 5)"
    );
    assert!(
        !stdout.contains("Bob") || !stdout.contains("bob@example.com"),
        "Should not contain Bob (id <= 5)"
    );
    assert!(
        !stdout.contains("Charlie") || !stdout.contains("charlie@example.com"),
        "Should not contain Charlie (id <= 5)"
    );
    assert!(
        !stdout.contains("David") || !stdout.contains("david@example.com"),
        "Should not contain David (id <= 5)"
    );

    Ok(())
}
