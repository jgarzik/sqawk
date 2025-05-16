use std::io::Write;
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

/// Test for basic REPL commands
#[test]
fn test_repl_basic_dot_commands() {
    // Commands to test REPL commands
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

/// Test for the .show command to display settings and metadata
#[test]
fn test_repl_show_command() {
    // Commands to test the .show command
    let test_commands = ".show\n.show tables\n.exit\n";

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

/// Test for the .stats command to toggle statistics display
#[test]
fn test_repl_stats_command() {
    // Commands to test the .stats command
    let test_commands = ".stats on\nSELECT * FROM sample;\n.stats off\nSELECT * FROM sample;\n.exit\n";

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
