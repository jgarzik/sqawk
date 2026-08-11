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
        .args([
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
        .args([
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
    let test_commands =
        ".stats on\nSELECT * FROM sample;\n.stats off\nSELECT * FROM sample;\n.exit\n";

    // Start the sqawk process with sample data loaded
    let mut process = Command::new("cargo")
        .args([
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

/// `.version` must report the crate's actual version.
///
/// It was hardcoded to "0.1.1" while the crate was at 0.8.0, and nothing
/// noticed because nothing asserted on it. Reading it from the manifest makes
/// drift impossible; this test guards the reporting path itself.
#[test]
fn test_repl_version_matches_crate() {
    use std::io::Write;
    use std::process::{Command, Stdio};

    let mut child = Command::new(env!("CARGO_BIN_EXE_sqawk"))
        .arg("-i")
        .arg("tests/data/employees.csv")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to start sqawk");

    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(b".version\n.exit\n")
        .expect("write");

    let out = child.wait_with_output().expect("wait");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let expected = format!("Sqawk version {}", env!("CARGO_PKG_VERSION"));
    assert!(
        stdout.contains(&expected),
        "expected `{expected}` in .version output, got:\n{stdout}"
    );
}
