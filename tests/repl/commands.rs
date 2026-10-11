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
    let mut process = Command::new(env!("CARGO_BIN_EXE_sqawk"))
        .args(["tests/data/sample.csv", "--interactive"])
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
    let mut process = Command::new(env!("CARGO_BIN_EXE_sqawk"))
        .args(["tests/data/sample.csv", "--interactive"])
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
    let mut process = Command::new(env!("CARGO_BIN_EXE_sqawk"))
        .args(["tests/data/sample.csv", "--interactive"])
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

/// A DML statement that matches nothing must report 0, not inherit the
/// previous statement's count.
///
/// The affected-row count was only overwritten when the new count was non-zero,
/// which conflated "this statement was not DML" with "this DML matched no
/// rows". So `UPDATE <matches 3>; UPDATE <matches nothing>` reported 3.
#[test]
fn test_repl_zero_row_update_reports_zero() {
    use std::io::Write;
    use std::process::{Command, Stdio};

    let dir = crate::helpers::create_temp_dir().expect("temp dir");
    let src = std::path::PathBuf::from("tests/data/employees.csv");
    let dest = dir.path().join("employees.csv");
    std::fs::copy(&src, &dest).expect("copy fixture");

    let mut child = Command::new(env!("CARGO_BIN_EXE_sqawk"))
        .arg("-i")
        .arg(format!("employees={}", dest.to_str().unwrap()))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to start sqawk");

    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(
            b".changes on\n\
              UPDATE employees SET salary = 1 WHERE department = 'Engineering';\n\
              UPDATE employees SET salary = 2 WHERE department = 'NoSuchDept';\n\
              .exit\n",
        )
        .expect("write");

    let out = child.wait_with_output().expect("wait");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let counts: Vec<&str> = stdout
        .lines()
        .filter(|l| l.ends_with("rows affected"))
        .collect();

    assert_eq!(
        counts,
        ["3 rows affected", "0 rows affected"],
        "second UPDATE matched nothing and must report 0, got:\n{stdout}"
    );
}

/// A trailing DML statement on a multi-statement REPL line must still report
/// its change count, and a plain SELECT must not report one at all.
///
/// execute_sql returned only the last row-producing statement, so a trailing
/// DELETE was invisible and repl.rs never reached the change-count branch.
/// Reporting the count unconditionally then printed "0 rows affected" after
/// every SELECT, so the executor now tracks whether a DML actually ran.
#[test]
fn test_repl_reports_changes_for_trailing_dml() {
    use std::io::Write;
    use std::process::{Command, Stdio};

    let dir = crate::helpers::create_temp_dir().expect("temp dir");
    let dest = dir.path().join("people.csv");
    std::fs::copy("tests/data/people.csv", &dest).expect("copy fixture");

    let mut child = Command::new(env!("CARGO_BIN_EXE_sqawk"))
        .arg("-i")
        .arg(format!("people={}", dest.to_str().unwrap()))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to start sqawk");

    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(
            b".changes on\n\
              SELECT * FROM people; DELETE FROM people WHERE id = 1;\n\
              SELECT id FROM people;\n\
              .exit\n",
        )
        .expect("write");

    let out = child.wait_with_output().expect("wait");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let counts: Vec<&str> = stdout
        .lines()
        .filter(|l| l.ends_with("rows affected"))
        .collect();

    assert_eq!(
        counts,
        ["1 rows affected"],
        "the DELETE must report once and the SELECTs not at all, got:\n{stdout}"
    );
    // Both SELECT result sets print; a piped session's stdout carries
    // nothing else.
    assert_eq!(
        stdout,
        "id,name,age\n1,Alice,32\n2,Bob,25\n3,Charlie,35\n1 rows affected\nid\n2\n3\n"
    );
}

/// A statement that fails must not leave its derived-table alias registered.
#[test]
fn test_repl_failed_derived_table_does_not_leak() {
    use std::io::Write;
    use std::process::{Command, Stdio};

    let mut child = Command::new(env!("CARGO_BIN_EXE_sqawk"))
        .arg("-i")
        .arg("tests/data/people.csv")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to start sqawk");

    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(
            b"SELECT nosuch FROM (SELECT * FROM people) x;\n\
              SELECT id FROM (SELECT * FROM people) x;\n\
              .tables\n\
              .exit\n",
        )
        .expect("write");

    let out = child.wait_with_output().expect("wait");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    let combined = format!("{stdout}{stderr}");

    assert!(
        !combined.contains("shadows an existing table"),
        "the failed statement leaked its alias, got:\n{combined}"
    );
    assert!(
        !stdout.lines().any(|l| l.trim() == "x"),
        ".tables lists a phantom derived-table alias, got:\n{stdout}"
    );
}
