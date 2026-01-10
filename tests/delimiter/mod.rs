//! Tests for delimiter options in sqawk
//!
//! Tests for different file delimiter options (-F flag) with tab and colon separators.

use crate::helpers::{get_contacts_file, get_employees_tsv_file};

#[test]
fn test_tab_delimiter() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = get_employees_tsv_file();

    // Run sqawk with tab delimiter
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT id, name, salary, department FROM employees WHERE salary > 70000")
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
    // employees.tsv has: id, name, department, salary, age
    // Rows with salary > 70000: John Smith (75000), Bob Johnson (80000), Alice Williams (85000),
    //                          David Miller (90000), Frank Wilson (82000), Henry Martin (78000)
    // Output should be tab-delimited (matching input)
    assert!(
        stdout.contains("id\tname\tsalary\tdepartment"),
        "Header row should be present with tab delimiter"
    );
    assert!(
        stdout.contains("John Smith") && stdout.contains("75000"),
        "Row for John Smith should be present (salary > 70000)"
    );
    assert!(
        stdout.contains("Bob Johnson") && stdout.contains("80000"),
        "Row for Bob Johnson should be present (salary > 70000)"
    );
    assert!(
        stdout.contains("Alice Williams") && stdout.contains("85000"),
        "Row for Alice Williams should be present (salary > 70000)"
    );

    // Check that we don't see rows with salary <= 70000
    assert!(
        !stdout.contains("Jane Doe") || !stdout.contains("65000"),
        "Should not contain Jane Doe with salary 65000 (salary <= 70000)"
    );
    assert!(
        !stdout.contains("Charlie Brown") || !stdout.contains("60000"),
        "Should not contain Charlie Brown with salary 60000 (salary <= 70000)"
    );

    Ok(())
}

#[test]
fn test_colon_delimiter() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = get_contacts_file();

    // Run sqawk with colon delimiter
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
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
    // contacts.txt has: id:name:email:phone:city
    // Rows with id > 5: David Miller (6), Emily Davis (7), Frank Wilson (8), Grace Taylor (9), Henry Martin (10)
    // Output should be colon-delimited (matching input)
    assert!(
        stdout.contains("id:name:email"),
        "Header row should be present with colon delimiter"
    );
    assert!(
        stdout.contains("David Miller") && stdout.contains("david.miller@example.com"),
        "Row for David Miller should be present (id > 5)"
    );
    assert!(
        stdout.contains("Henry Martin") && stdout.contains("henry.martin@example.com"),
        "Row for Henry Martin should be present (id > 5)"
    );

    // Check that we don't see rows with id <= 5
    assert!(
        !stdout.contains("John Smith") || !stdout.contains("john.smith@example.com"),
        "Should not contain John Smith (id <= 5)"
    );
    assert!(
        !stdout.contains("Jane Doe") || !stdout.contains("jane.doe@example.com"),
        "Should not contain Jane Doe (id <= 5)"
    );
    assert!(
        !stdout.contains("Bob Johnson") || !stdout.contains("bob.johnson@example.com"),
        "Should not contain Bob Johnson (id <= 5)"
    );

    Ok(())
}
