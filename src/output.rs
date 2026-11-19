//! Where result rows go.
//!
//! The VM hands each statement's rows to a [`ResultSink`] one at a time, as
//! they are produced (SQLite's step model), so a `SELECT *` over a large
//! table is never held in memory as a whole result before printing.
//! [`StdoutSink`] prints them; [`CollectSink`] gathers each result set into
//! a [`Table`] (derived tables, CTEs and tests).

use std::io::{BufWriter, ErrorKind, Stdout, Write};

use crate::error::{SqawkError, SqawkResult};
use crate::table::{ColumnDefinition, RecordWriter, Table, Value, delimiter_byte};
use crate::vm::bytecode::Register;

/// The size of [`StdoutSink`]'s buffer. It is also flushed at the end of
/// every statement.
const STDOUT_BUFFER: usize = 64 * 1024;

/// Receives one statement's result set at a time: `begin` with its columns,
/// then each row, then `end`. A statement with result columns but no rows
/// still gets `begin` and `end` (its header prints); one with no result
/// columns (DML, DDL) gets nothing. When the statement fails part-way, the
/// sink gets `abort` in place of `end`, after whatever rows it already had.
pub trait ResultSink {
    /// A result set with these columns starts.
    fn begin(&mut self, columns: &[ColumnDefinition]) -> SqawkResult<()>;
    /// One row, as wide as the columns given to `begin`.
    fn row(&mut self, values: &[Register]) -> SqawkResult<()>;
    /// The result set is complete.
    fn end(&mut self) -> SqawkResult<()>;
    /// The statement failed after `begin`; the error follows.
    fn abort(&mut self);
}

/// Gathers each result set into a table named `result` and hands it to a
/// callback at `end`. An aborted result set is dropped.
pub struct CollectSink<F> {
    table: Option<Table>,
    on_table: F,
}

impl<F: FnMut(Table) -> SqawkResult<()>> CollectSink<F> {
    pub fn new(on_table: F) -> Self {
        CollectSink {
            table: None,
            on_table,
        }
    }
}

impl<F: FnMut(Table) -> SqawkResult<()>> ResultSink for CollectSink<F> {
    fn begin(&mut self, columns: &[ColumnDefinition]) -> SqawkResult<()> {
        self.table = Some(Table::new_with_schema(
            "result",
            columns.to_vec(),
            None,
            None,
        ));
        Ok(())
    }

    fn row(&mut self, values: &[Register]) -> SqawkResult<()> {
        let table = self
            .table
            .as_mut()
            .ok_or_else(|| SqawkError::VmError("result row before its columns".to_string()))?;
        table.add_row(values.iter().cloned().map(Value::from).collect())
    }

    fn end(&mut self) -> SqawkResult<()> {
        match self.table.take() {
            Some(table) => (self.on_table)(table),
            None => Ok(()),
        }
    }

    fn abort(&mut self) {
        self.table = None;
    }
}

/// Prints result sets to standard output as delimited records (the
/// writeback format: RFC 4180 quoting, NULL as an empty field), each with
/// its own header line unless headers are off.
///
/// Once the reader of a pipe has gone away (`sqawk ... | head`), printing
/// stops quietly but statements keep running, so later statements and
/// `--write` still happen. Any other write error is returned, unless the
/// sink was made with `ignore_errors` (the REPL, whose output is best
/// effort like every other line it prints).
pub struct StdoutSink {
    writer: RecordWriter,
    header: bool,
    verbose: bool,
    ignore_errors: bool,
    open: bool,
    out: Option<BufWriter<Stdout>>,
    rows: usize,
    result_sets: usize,
}

impl StdoutSink {
    /// `delimiter`: the output delimiter (`-F`); `header`: print each result
    /// set's column names first; `verbose`: report each result set's row
    /// count on stderr after its rows.
    pub fn new(delimiter: &str, header: bool, verbose: bool, ignore_errors: bool) -> Self {
        StdoutSink {
            writer: RecordWriter {
                delimiter: delimiter_byte(delimiter),
                terminator: b"\n",
                comments: false,
            },
            header,
            verbose,
            ignore_errors,
            open: true,
            out: None,
            rows: 0,
            result_sets: 0,
        }
    }

    /// Result sets begun since the last call (statements that had result
    /// columns).
    pub fn take_result_sets(&mut self) -> usize {
        std::mem::take(&mut self.result_sets)
    }

    /// Note the outcome of a write: a closed pipe (or any error, when
    /// errors are ignored) silences the rest of the output.
    fn check(&mut self, result: std::io::Result<()>) -> SqawkResult<()> {
        match result {
            Ok(()) => Ok(()),
            Err(e) => {
                self.open = false;
                self.out = None;
                if e.kind() == ErrorKind::BrokenPipe || self.ignore_errors {
                    Ok(())
                } else {
                    Err(SqawkError::write("standard output", e))
                }
            }
        }
    }

    fn flush(&mut self) -> SqawkResult<()> {
        let result = match self.out.as_mut() {
            Some(out) => out.flush(),
            None => Ok(()),
        };
        self.out = None;
        self.check(result)
    }
}

impl ResultSink for StdoutSink {
    fn begin(&mut self, columns: &[ColumnDefinition]) -> SqawkResult<()> {
        self.rows = 0;
        self.result_sets += 1;
        // Best-effort output tries again for each result set.
        if self.ignore_errors {
            self.open = true;
        }
        if !self.open {
            return Ok(());
        }
        let mut out = BufWriter::with_capacity(STDOUT_BUFFER, std::io::stdout());
        let result = if self.header {
            self.writer
                .write(&mut out, columns.iter().map(|c| c.name.as_str()))
        } else {
            Ok(())
        };
        self.out = Some(out);
        self.check(result)
    }

    fn row(&mut self, values: &[Register]) -> SqawkResult<()> {
        self.rows += 1;
        let Some(out) = self.out.as_mut() else {
            return Ok(());
        };
        let fields: Vec<_> = values.iter().map(Register::field_str).collect();
        let result = self.writer.write(out, fields.iter().map(|f| f.as_ref()));
        self.check(result)
    }

    fn end(&mut self) -> SqawkResult<()> {
        let result = self.flush();
        if self.verbose {
            diag!("Query returned {} rows", self.rows);
        }
        result
    }

    fn abort(&mut self) {
        // The rows already produced are printed before the error is.
        let _ = self.flush();
    }
}
