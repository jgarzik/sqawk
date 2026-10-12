//! Running the sqawk binary under a time limit.
//!
//! [`execute`] is the one way every mode spawns sqawk: arguments, optional
//! standard input, working directory and environment, a time limit, and
//! what to do with standard output (keep it, drain it, or close the pipe
//! after a few bytes to make sqawk hit a broken pipe). On Linux it can also
//! follow the child's peak resident set size (`VmHWM` in
//! `/proc/<pid>/status`, polled until the child exits; plain std, no FFI).
//!
//! [`run_sqawk`] is the differential modes' wrapper: the SQL always goes in
//! on standard input (`-f -`), never on the command line, so statement
//! length is unbounded on every OS (Windows caps a command line near
//! 32 KB).

use std::ffi::OsString;
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

/// What happens to the child's standard output.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StdoutMode {
    /// Keep every byte.
    Capture,
    /// Read and drop (only the byte count is kept).
    Discard,
    /// Read at most this many bytes, then close the pipe: the child's next
    /// write fails with a broken pipe (EPIPE; ERROR_NO_DATA on Windows).
    CloseAfter(usize),
}

/// One sqawk run to make.
pub struct Job {
    pub bin: PathBuf,
    pub args: Vec<OsString>,
    /// Bytes fed to standard input, then closed; None gives a null stdin.
    pub stdin: Option<Vec<u8>>,
    pub cwd: Option<PathBuf>,
    /// Extra environment variables.
    pub env: Vec<(String, OsString)>,
    pub limit: Duration,
    pub stdout: StdoutMode,
    /// Follow peak RSS (Linux only; None elsewhere).
    pub watch_rss: bool,
}

impl Job {
    pub fn new<I, S>(bin: &Path, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<OsString>,
    {
        Job {
            bin: bin.to_path_buf(),
            args: args.into_iter().map(Into::into).collect(),
            stdin: None,
            cwd: None,
            env: Vec::new(),
            limit: Duration::from_secs(30),
            stdout: StdoutMode::Capture,
            watch_rss: false,
        }
    }
}

/// How a [`Job`] ended.
#[derive(Clone, Debug)]
pub struct Finished {
    /// The exit status; None when killed by a signal or not started.
    pub code: Option<i32>,
    pub timed_out: bool,
    /// Set when the binary could not be started.
    pub spawn_error: Option<String>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub elapsed: Duration,
    /// Peak resident set size in KiB (Linux, `watch_rss`).
    pub peak_rss_kb: Option<u64>,
}

impl Finished {
    pub fn stderr_text(&self) -> String {
        String::from_utf8_lossy(&self.stderr).into_owned()
    }
}

/// `VmHWM` of a live process, in KiB.
#[cfg(target_os = "linux")]
fn peak_rss(pid: u32) -> Option<u64> {
    let s = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let line = s.lines().find(|l| l.starts_with("VmHWM:"))?;
    line.split_whitespace().nth(1)?.parse().ok()
}

#[cfg(not(target_os = "linux"))]
fn peak_rss(_pid: u32) -> Option<u64> {
    None
}

/// Run `job` to completion (or to its time limit, then kill it).
pub fn execute(job: &Job) -> Finished {
    let start = Instant::now();
    let mut cmd = Command::new(&job.bin);
    cmd.args(&job.args)
        .stdin(if job.stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(d) = &job.cwd {
        cmd.current_dir(d);
    }
    for (k, v) in &job.env {
        cmd.env(k, v);
    }
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            return Finished {
                code: None,
                timed_out: false,
                spawn_error: Some(format!("cannot run {}: {e}", job.bin.display())),
                stdout: Vec::new(),
                stderr: Vec::new(),
                elapsed: start.elapsed(),
                peak_rss_kb: None,
            }
        }
    };
    let writer = child.stdin.take().map(|mut pipe| {
        let input = job.stdin.clone().unwrap_or_default();
        thread::spawn(move || {
            let _ = pipe.write_all(&input);
        })
    });
    let mut out_pipe = child.stdout.take().expect("piped stdout");
    let mode = job.stdout;
    let out_reader = thread::spawn(move || {
        let mut kept = Vec::new();
        // Bytes read so far (also when discarded).
        let mut total = 0usize;
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let want = match mode {
                StdoutMode::CloseAfter(n) if total >= n => break,
                StdoutMode::CloseAfter(n) => (n - total).min(buf.len()),
                _ => buf.len(),
            };
            match out_pipe.read(&mut buf[..want]) {
                Ok(0) | Err(_) => break,
                Ok(k) => {
                    total += k;
                    if mode != StdoutMode::Discard {
                        kept.extend_from_slice(&buf[..k]);
                    }
                }
            }
        }
        // Dropping the pipe here is what closes it early for CloseAfter.
        drop(out_pipe);
        kept
    });
    let mut err_pipe = child.stderr.take().expect("piped stderr");
    let err_reader = thread::spawn(move || {
        let mut v = Vec::new();
        let _ = err_pipe.read_to_end(&mut v);
        v
    });
    let pid = child.id();
    let mut peak: Option<u64> = None;
    let mut timed_out = false;
    let status = loop {
        if job.watch_rss {
            if let Some(kb) = peak_rss(pid) {
                peak = Some(peak.map_or(kb, |p| p.max(kb)));
            }
        }
        match child.try_wait() {
            Ok(Some(s)) => break Some(s),
            Ok(None) => {}
            Err(_) => break None,
        }
        if start.elapsed() > job.limit {
            let _ = child.kill();
            timed_out = true;
            break child.wait().ok();
        }
        thread::sleep(Duration::from_millis(if job.watch_rss { 5 } else { 2 }));
    };
    let elapsed = start.elapsed();
    if let Some(w) = writer {
        let _ = w.join();
    }
    let stdout = out_reader.join().unwrap_or_default();
    let stderr = err_reader.join().unwrap_or_default();
    Finished {
        code: status.and_then(|s| s.code()),
        timed_out,
        spawn_error: None,
        stdout,
        stderr,
        elapsed,
        peak_rss_kb: peak,
    }
}

/// Run `sqawk [args] -f - files...` with `sql` on standard input.
pub fn run_sqawk(
    bin: &Path,
    args: &[&str],
    sql: &str,
    files: &[PathBuf],
    limit: Duration,
) -> Outcome {
    let mut argv: Vec<OsString> = args.iter().map(OsString::from).collect();
    argv.push("-f".into());
    argv.push("-".into());
    argv.extend(files.iter().map(|f| f.as_os_str().to_os_string()));
    let mut job = Job::new(bin, argv);
    job.stdin = Some(sql.as_bytes().to_vec());
    job.limit = limit;
    let f = execute(&job);
    let stdout = String::from_utf8_lossy(&f.stdout).into_owned();
    let mut stderr = f.stderr_text();
    if let Some(e) = f.spawn_error {
        return Outcome {
            exit: Exit::Crash,
            stdout,
            stderr: e,
            elapsed: f.elapsed,
        };
    }
    let exit = if f.timed_out {
        stderr.push_str(&format!("\ntimeout after {:.1}s", limit.as_secs_f64()));
        Exit::Crash
    } else {
        match f.code {
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
        elapsed: f.elapsed,
    }
}

/// Text that only a crash puts on standard error: a panic, a stack
/// overflow, or Rust's arithmetic-overflow panic text. sqawk's own
/// `integer overflow` error (SUM / ABS, as in SQLite) is a clean error.
pub fn crash_text(stderr: &str) -> Option<&'static str> {
    if stderr.contains("panicked") {
        return Some("panicked");
    }
    if stderr.contains("RUST_BACKTRACE") {
        return Some("RUST_BACKTRACE");
    }
    if stderr.contains("stack overflow") {
        return Some("stack overflow");
    }
    let stray = stderr
        .lines()
        .any(|l| l.contains("overflow") && !l.contains("integer overflow"));
    if stray {
        return Some("overflow");
    }
    None
}
