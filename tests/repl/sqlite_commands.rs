use std::io::Write;
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

/// Test for sqlite3-compatible commands implemented in the REPL
#[test]
fn test_repl_sqlite_commands() {
    // Commands to test SQLite-compatible commands
    let test_commands = ".help\n.version\n.tables\n.cd .\n.changes on\n.changes off\n.exit\n";

    // Start the sqawk process with sample data loaded
    let mut process = Command::new("cargo")
        .args(&[
            "run",
            "--bin",
            "sqawk",
            "tests/data/sample.csv",
            "--interactive",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to start sqawk process");

    // Get handle to stdin
    let mut stdin = process.stdin.take().expect("Failed to open stdin");
    let _stdout = process.stdout.take().expect("Failed to open stdout");
    let _stderr = process.stderr.take().expect("Failed to open stderr");

    // Give the process a moment to start up
    thread::sleep(Duration::from_millis(100));

    // Write test commands to REPL
    stdin
        .write_all(test_commands.as_bytes())
        .expect("Failed to write to stdin");

    // Wait for process to complete
    let status = process.wait().expect("Failed to wait for sqawk process");
    assert!(status.success(), "Process did not exit successfully");
}