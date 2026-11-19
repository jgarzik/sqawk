use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

/// Run a sqawk REPL script with the provided commands and return the output
pub fn run_repl_script(
    commands: &str,
    input_files: &[&str],
    field_separator: Option<&str>,
    verbose: bool,
) -> String {
    // Build command args
    let mut args: Vec<&str> = Vec::new();

    // Add input files
    args.extend(input_files);

    // Add field separator if provided
    if let Some(sep) = field_separator {
        args.push("--field-separator");
        args.push(sep);
    }

    // Add verbose flag if needed
    if verbose {
        args.push("--verbose");
    }

    // Add interactive flag
    args.push("--interactive");

    // Start the sqawk process
    let mut process = Command::new(env!("CARGO_BIN_EXE_sqawk"))
        .args(&args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to start sqawk process");

    // Write the commands, then close stdin: end of input ends the REPL even
    // when the script has no `.exit`.
    let mut stdin = process.stdin.take().expect("Failed to open stdin");
    stdin
        .write_all(commands.as_bytes())
        .expect("Failed to write to stdin");
    drop(stdin);

    // Wait for process to complete
    let output = process
        .wait_with_output()
        .expect("Failed to wait for sqawk process");

    // Combine stdout and stderr
    let mut result = String::from_utf8_lossy(&output.stdout).to_string();
    if !output.stderr.is_empty() {
        result.push('\n');
        result.push_str(&String::from_utf8_lossy(&output.stderr));
    }

    result
}

/// Read a file to a string
pub fn read_file(path: &Path) -> String {
    fs::read_to_string(path).expect("Failed to read file")
}

/// Trim whitespace from each line of text
pub fn trim_lines(text: &str) -> String {
    text.lines()
        .map(|line| line.trim())
        .collect::<Vec<_>>()
        .join("\n")
}
