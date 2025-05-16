use std::io::Write;
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;
use std::fs;
use std::path::Path;

/// Run a sqawk REPL script with the provided commands and return the output
pub fn run_repl_script(
    commands: &str,
    input_files: &[&str],
    field_separator: Option<&str>,
    verbose: bool,
) -> String {
    // Build command args
    let mut args = vec!["run", "--bin", "sqawk"];
    
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
    let mut process = Command::new("cargo")
        .args(&args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to start sqawk process");
    
    // Get handles to stdin and stdout
    let mut stdin = process.stdin.take().expect("Failed to open stdin");
    let _stdout = process.stdout.take().expect("Failed to open stdout");
    
    // Give the process a moment to start up
    thread::sleep(Duration::from_millis(100));
    
    // Write commands to REPL
    stdin
        .write_all(commands.as_bytes())
        .expect("Failed to write to stdin");
    
    // Wait for process to complete
    let output = process.wait_with_output().expect("Failed to wait for sqawk process");
    
    // Combine stdout and stderr
    let mut result = String::from_utf8_lossy(&output.stdout).to_string();
    if !output.stderr.is_empty() {
        result.push_str("\n");
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