//! Diagnostic output on standard error.
//!
//! Every message sqawk writes to stderr -- error lines and `-v` debugging
//! output alike -- goes through [`write_line`]. A failed write is ignored:
//! a closed or full stderr (`sqawk -v ... 2>&1 | head`) must never abort a
//! run, which `eprintln!` does by panicking.

use std::io::Write;

/// Write one line to standard error, ignoring any write error.
pub fn write_line(args: std::fmt::Arguments<'_>) {
    let stderr = std::io::stderr();
    let mut err = stderr.lock();
    let _ = err.write_fmt(args).and_then(|()| err.write_all(b"\n"));
}

/// Write one line to standard output, ignoring any write error (a reader
/// that has gone away is not an error worth aborting for).
pub fn out_line(args: std::fmt::Arguments<'_>) {
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    let _ = out.write_fmt(args).and_then(|()| out.write_all(b"\n"));
}

/// `eprintln!` that never panics: formats a line onto stderr and ignores
/// write errors.
macro_rules! diag {
    ($($arg:tt)*) => {
        $crate::diag::write_line(format_args!($($arg)*))
    };
}

/// `println!` that never panics: formats a line onto stdout and ignores
/// write errors.
macro_rules! outln {
    () => {
        $crate::diag::out_line(format_args!(""))
    };
    ($($arg:tt)*) => {
        $crate::diag::out_line(format_args!($($arg)*))
    };
}
