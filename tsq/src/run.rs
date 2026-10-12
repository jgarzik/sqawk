//! Running the sqawk binary under a time limit.
//!
//! The SQL always goes in on standard input (`-f -`), never on the command
//! line, so statement length is unbounded on every OS (Windows caps a
//! command line near 32 KB).

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

/// How a sqawk run ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Exit {
    Ok,
    /// A clean error: exit status 1 or 2 with a message.
    Err,
    /// A panic, a signal, an abnormal status, or the time limit.
    Crash,
}

#[derive(Clone, Debug)]
pub struct Outcome {
    pub exit: Exit,
    pub stdout: String,
    pub stderr: String,
    pub elapsed: Duration,
}

impl Outcome {
    /// The last line of standard error (sqawk reports one line).
    pub fn message(&self) -> String {
        self.stderr
            .trim()
            .lines()
            .last()
            .unwrap_or("")
            .chars()
            .take(300)
            .collect()
    }
}

/// The sqawk binary: `--sqawk`, else `$SQAWK`, else `sqawk` next to the
/// running tsq executable (the same cargo target directory).
pub fn locate_sqawk(explicit: Option<&Path>) -> PathBuf {
    if let Some(p) = explicit {
        return p.to_path_buf();
    }
    if let Some(p) = std::env::var_os("SQAWK") {
        return PathBuf::from(p);
    }
    let name = format!("sqawk{}", std::env::consts::EXE_SUFFIX);
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let cand = dir.join(&name);
            if cand.exists() {
                return cand;
            }
        }
    }
    PathBuf::from(name)
}

/// Run `sqawk [args] -f - files...` with `sql` on standard input.
pub fn run_sqawk(
    bin: &Path,
    args: &[&str],
    sql: &str,
    files: &[PathBuf],
    limit: Duration,
) -> Outcome {
    let start = Instant::now();
    let mut cmd = Command::new(bin);
    cmd.args(args)
        .arg("-f")
        .arg("-")
        .args(files)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            return Outcome {
                exit: Exit::Crash,
                stdout: String::new(),
                stderr: format!("cannot run {}: {e}", bin.display()),
                elapsed: start.elapsed(),
            }
        }
    };
    let mut stdin = child.stdin.take().expect("piped stdin");
    let input = sql.as_bytes().to_vec();
    let writer = thread::spawn(move || {
        let _ = stdin.write_all(&input);
    });
    let mut out_pipe = child.stdout.take().expect("piped stdout");
    let mut err_pipe = child.stderr.take().expect("piped stderr");
    let out_reader = thread::spawn(move || {
        let mut v = Vec::new();
        let _ = out_pipe.read_to_end(&mut v);
        v
    });
    let err_reader = thread::spawn(move || {
        let mut v = Vec::new();
        let _ = err_pipe.read_to_end(&mut v);
        v
    });
    let mut timed_out = false;
    let status = loop {
        match child.try_wait() {
            Ok(Some(s)) => break Some(s),
            Ok(None) => {}
            Err(_) => break None,
        }
        if start.elapsed() > limit {
            let _ = child.kill();
            timed_out = true;
            break child.wait().ok();
        }
        thread::sleep(Duration::from_millis(2));
    };
    let _ = writer.join();
    let stdout = String::from_utf8_lossy(&out_reader.join().unwrap_or_default()).into_owned();
    let mut stderr = String::from_utf8_lossy(&err_reader.join().unwrap_or_default()).into_owned();
    let elapsed = start.elapsed();
    let exit = if timed_out {
        stderr.push_str(&format!("\ntimeout after {:.1}s", limit.as_secs_f64()));
        Exit::Crash
    } else {
        match status.and_then(|s| s.code()) {
            Some(0) if !stderr.contains("panicked") => Exit::Ok,
            Some(1) | Some(2) if !stderr.contains("panicked") => Exit::Err,
            Some(c) => {
                stderr.push_str(&format!("\nexit status {c}"));
                Exit::Crash
            }
            None => {
                stderr.push_str("\nkilled by a signal");
                Exit::Crash
            }
        }
    };
    Outcome {
        exit,
        stdout,
        stderr,
        elapsed,
    }
}
