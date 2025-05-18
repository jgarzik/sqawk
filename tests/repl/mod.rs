use std::io::Write;
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

// Include command-related tests
mod commands;
mod test_repl_from_file;

#[test]
fn test_repl_basic_commands() {
    // Create test commands for REPL
    let test_commands = ".help\n.tables\nSELECT * FROM sample;\n.exit\n";

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
        .spawn()
        .expect("Failed to start sqawk process");

    // Get handle to stdin and take stdout to prevent blocking
    let mut stdin = process.stdin.take().expect("Failed to open stdin");
    let _stdout = process.stdout.take().expect("Failed to open stdout");

    // Give the process a moment to start up
    thread::sleep(Duration::from_millis(100));

    // Write test commands to REPL
    stdin
        .write_all(test_commands.as_bytes())
        .expect("Failed to write to stdin");

    // Wait for process to complete (should exit from .exit command)
    let status = process.wait().expect("Failed to wait for sqawk process");
    assert!(status.success(), "Process did not exit successfully");

    // Don't need to read output for this basic test, just verify process terminated successfully
}

#[test]
fn test_repl_table_operations() {
    // Commands to test table operations
    let test_commands = ".tables\nSELECT COUNT(*) FROM sample;\nUPDATE sample SET age = 31 WHERE id = 1;\nSELECT * FROM sample;\n.exit\n";

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
        .spawn()
        .expect("Failed to start sqawk process");

    // Get handle to stdin and take stdout to prevent blocking
    let mut stdin = process.stdin.take().expect("Failed to open stdin");
    let _stdout = process.stdout.take().expect("Failed to open stdout");

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

#[test]
fn test_repl_write_toggle() {
    // Commands to test the .write toggle
    let test_commands =
        ".write\nUPDATE sample SET age = 32 WHERE id = 1;\n.write\nSELECT * FROM sample;\n.exit\n";

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
        .spawn()
        .expect("Failed to start sqawk process");

    // Get handle to stdin and take stdout to prevent blocking
    let mut stdin = process.stdin.take().expect("Failed to open stdin");
    let _stdout = process.stdout.take().expect("Failed to open stdout");

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

#[test]
fn test_repl_multiline_statements() {
    // Commands testing multi-line SQL statements in REPL
    let test_commands =
        "SELECT\n  id,\n  name,\n  age\nFROM\n  sample\nWHERE\n  age > 25;\n.exit\n";

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
        .spawn()
        .expect("Failed to start sqawk process");

    // Get handle to stdin and take stdout to prevent blocking
    let mut stdin = process.stdin.take().expect("Failed to open stdin");
    let _stdout = process.stdout.take().expect("Failed to open stdout");

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

#[test]
fn test_repl_save_command() -> Result<(), Box<dyn std::error::Error>> {
    use std::fs;
    use std::io::Write;
    use tempfile::tempdir;

    // Create a temporary directory
    let temp_dir = tempdir()?;
    let temp_file_path = temp_dir.path().join("sample.csv");

    // Copy sample.csv to the temporary file
    let original_content = fs::read_to_string("tests/data/sample.csv")?;
    let mut temp_file = fs::File::create(&temp_file_path)?;
    temp_file.write_all(original_content.as_bytes())?;
    drop(temp_file); // Close the file

    // Commands to test the .save command
    let test_commands =
        "UPDATE sample SET age = 33 WHERE id = 1;\n.save\nUPDATE sample SET age = 34 WHERE id = 2;\n.save sample\n.exit\n";

    // Start the sqawk process with the temporary file
    let mut process = Command::new("cargo")
        .args([
            "run",
            "--bin",
            "sqawk",
            temp_file_path.to_str().unwrap(),
            "--interactive",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("Failed to start sqawk process");

    // Get handle to stdin and take stdout to prevent blocking
    let mut stdin = process.stdin.take().expect("Failed to open stdin");
    let _stdout = process.stdout.take().expect("Failed to open stdout");

    // Give the process a moment to start up
    thread::sleep(Duration::from_millis(100));

    // Write test commands to REPL
    stdin
        .write_all(test_commands.as_bytes())
        .expect("Failed to write to stdin");

    // Wait for process to complete
    let status = process.wait().expect("Failed to wait for sqawk process");
    assert!(status.success(), "Process did not exit successfully");

    // Verify the temporary file was modified
    let modified_content = fs::read_to_string(&temp_file_path)?;
    assert!(
        modified_content.contains("2,Bob,34"),
        "File was not modified as expected"
    );

    // The temporary directory and its contents will be automatically deleted
    // when it goes out of scope at the end of this function

    Ok(())
}
