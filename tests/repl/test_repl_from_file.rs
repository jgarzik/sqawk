use assert_cmd::Command;

#[test]
fn test_repl_from_file() {
    // Removed unused temporary directory

    // Run sqawk with input from repl_commands.txt in the tmp directory
    let mut cmd = Command::cargo_bin("sqawk").unwrap();
    let assert = cmd
        .arg("--interactive")
        .arg("tests/data/sample.csv")
        .pipe_stdin("tests/tmp/test_repl_commands.txt")
        .unwrap()
        .assert()
        .success();

    // Get the output
    let output = assert.get_output();
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Verify expected output elements
    assert!(stdout.contains("Tables:"));
    assert!(stdout.contains("sample"));
    // The .tables s% command outputs a header of "Tables:" not "Tables matching 's%':"
    assert!(stdout.contains("id,name,age"));
    assert!(stdout.contains("1,Alice,32"));
    assert!(stdout.contains("1 rows affected")); // The "Changes:" output format
    assert!(stdout.contains("Sqawk version"));
}
