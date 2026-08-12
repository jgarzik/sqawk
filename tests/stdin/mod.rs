//! Tests for reading a table from standard input via the `-` file operand.
//!
//! sqawk had no stdin path at all: `-` was rejected as a missing file, so it
//! could not be the receiving end of a pipe.

const EMPLOYEES: &str = "id,name,department,salary\n\
                         1,Alice,Engineering,75000\n\
                         2,Bob,Marketing,65000\n\
                         3,Charlie,Engineering,80000\n";

/// Run sqawk with `input` on stdin and return (stdout, stderr, success).
fn run_with_stdin(args: &[&str], input: &str) -> (String, String, bool) {
    let out = assert_cmd::cargo::cargo_bin_cmd!("sqawk")
        .args(args)
        .write_stdin(input)
        .output()
        .expect("failed to run sqawk");

    (
        String::from_utf8_lossy(&out.stdout).trim_end().to_string(),
        String::from_utf8_lossy(&out.stderr).to_string(),
        out.status.success(),
    )
}

/// A bare `-` loads standard input as the table `stdin`.
#[test]
fn bare_dash_loads_table_named_stdin() {
    let (stdout, stderr, ok) = run_with_stdin(
        &[
            "-s",
            "SELECT name, salary FROM stdin WHERE salary > 70000",
            "-",
        ],
        EMPLOYEES,
    );
    assert!(ok, "sqawk failed: {stderr}");
    assert_eq!(stdout, "name,salary\nAlice,75000\nCharlie,80000");
}

/// `table_name=-` names the stdin table explicitly.
#[test]
fn named_stdin_table() {
    let (stdout, stderr, ok) =
        run_with_stdin(&["-s", "SELECT COUNT(*) FROM emp", "emp=-"], EMPLOYEES);
    assert!(ok, "sqawk failed: {stderr}");
    assert_eq!(stdout, "COUNT\n3");
}

/// Piped input is comma-separated by default, so sqawk's own output can be
/// piped straight back into sqawk.
#[test]
fn stdin_defaults_to_comma() {
    let (stdout, stderr, ok) = run_with_stdin(
        &["-s", "SELECT department FROM stdin LIMIT 1", "-"],
        EMPLOYEES,
    );
    assert!(ok, "sqawk failed: {stderr}");
    assert_eq!(stdout, "department\nEngineering");
}

/// `-F` still overrides the delimiter for piped input.
#[test]
fn stdin_honours_field_separator() {
    let (stdout, stderr, ok) =
        run_with_stdin(&["-F:", "-s", "SELECT b FROM stdin", "-"], "a:b\n1:2\n");
    assert!(ok, "sqawk failed: {stderr}");
    assert_eq!(stdout, "b\n2");
}

/// A stdin table joins against tables loaded from real files.
#[test]
fn stdin_joins_with_a_file() {
    let (stdout, stderr, ok) = run_with_stdin(
        &[
            "-s",
            "SELECT s.name FROM stdin s JOIN employees e ON s.name = e.name ORDER BY s.name LIMIT 2",
            "-",
            "tests/data/employees.csv",
        ],
        EMPLOYEES,
    );
    assert!(ok, "sqawk failed: {stderr}");
    assert_eq!(stdout, "s.name\nAlice\nBob");
}

/// --tabledef supplies column names for headerless piped input.
#[test]
fn stdin_respects_tabledef() {
    let (stdout, stderr, ok) = run_with_stdin(
        &[
            "--tabledef=stdin:first,second",
            "-s",
            "SELECT second FROM stdin",
            "-",
        ],
        "1,2\n3,4\n",
    );
    assert!(ok, "sqawk failed: {stderr}");
    assert_eq!(stdout, "second\n2\n4");
}

/// Standard input can only be consumed once.
#[test]
fn stdin_given_twice_is_rejected() {
    let (_, stderr, ok) = run_with_stdin(&["-s", "SELECT 1 FROM stdin", "-", "a=-"], EMPLOYEES);
    assert!(!ok, "expected failure when '-' is given twice");
    assert!(
        stderr.contains("only be read once"),
        "unexpected stderr: {stderr}"
    );
}

/// A stdin table has no file to write back to, and --write must say so
/// rather than inventing a target.
#[test]
fn stdin_table_cannot_be_written_back() {
    let (_, stderr, ok) = run_with_stdin(
        &["-s", "DELETE FROM stdin WHERE id = 1", "-", "--write"],
        EMPLOYEES,
    );
    assert!(!ok, "expected --write on a stdin table to fail");
    assert!(
        stderr.contains("read from standard input"),
        "unexpected stderr: {stderr}"
    );
}

/// The REPL reads its commands from stdin, so it cannot also read a table
/// from stdin.
#[test]
fn stdin_is_rejected_in_interactive_mode() {
    let (_, stderr, ok) = run_with_stdin(&["-i", "-"], EMPLOYEES);
    assert!(!ok, "expected -i with '-' to fail");
    assert!(
        stderr.contains("interactive mode"),
        "unexpected stderr: {stderr}"
    );
}

/// A genuinely missing file is still an error: `-` is the only special case.
#[test]
fn missing_file_still_errors() {
    let (_, stderr, ok) = run_with_stdin(&["-s", "SELECT 1 FROM nope", "nope.csv"], "");
    assert!(!ok, "expected a missing file to fail");
    assert!(stderr.contains("nope.csv"), "unexpected stderr: {stderr}");
}

/// Empty piped input used to reach the compiler with a zero-column table and
/// panic on `result_regs[0]`. An empty pipe is routine for `-`.
#[test]
fn empty_stdin_errors_rather_than_panicking() {
    let (_, stderr, ok) = run_with_stdin(&["-s", "SELECT * FROM stdin", "-"], "");
    assert!(!ok, "expected empty stdin to be an error");
    assert!(
        !stderr.contains("panicked"),
        "sqawk panicked on empty stdin: {stderr}"
    );
    assert!(
        stderr.contains("no header row"),
        "unexpected stderr: {stderr}"
    );
}

/// Rejecting the stdin table must happen before anything is written, or an
/// unrelated source file is rewritten or not depending on HashSet order.
#[test]
fn stdin_write_rejection_leaves_other_files_untouched() -> Result<(), Box<dyn std::error::Error>> {
    let temp = crate::helpers::create_temp_dir()?;
    let other =
        crate::helpers::create_custom_csv(temp.path(), "other.csv", "id,name\n1,Alice\n2,Bob\n")?;
    let before = std::fs::read_to_string(&other)?;

    // Run repeatedly: the old failure was nondeterministic, driven by the
    // iteration order of the modified-table set.
    for _ in 0..8 {
        let (_, stderr, ok) = run_with_stdin(
            &[
                "-s",
                "DELETE FROM stdin WHERE id = 1",
                "-s",
                "DELETE FROM other WHERE id = 1",
                "-",
                other.to_str().unwrap(),
                "--write",
            ],
            "id,name\n1,Zed\n",
        );
        assert!(!ok, "expected --write on a stdin table to fail");
        assert!(
            stderr.contains("read from standard input"),
            "unexpected stderr: {stderr}"
        );
        assert_eq!(
            std::fs::read_to_string(&other)?,
            before,
            "other.csv was rewritten despite the run failing"
        );
    }
    Ok(())
}

/// The REPL reads its commands from stdin, so `.load -` would swallow the
/// rest of the command stream. The startup guard did not cover it.
#[test]
fn repl_load_dash_is_rejected() {
    let (stdout, _, _) = run_with_stdin(
        &["-i", "tests/data/employees.csv"],
        ".load -\n.tables\n.exit\n",
    );
    assert!(
        !stdout.contains("Loaded table 'stdin'"),
        "`.load -` consumed the REPL command stream: {stdout}"
    );
    // The commands after `.load -` must still run.
    assert!(
        stdout.contains("employees"),
        "`.tables` after `.load -` did not run: {stdout}"
    );
}
