//! Subqueries: scalar `(SELECT ...)`, `EXISTS (SELECT ...)` and
//! `x IN (SELECT ...)`.
//!
//! As in SQLite, a subquery is not evaluated by the compiler. It is compiled
//! by the same query compiler as any statement ([`SqlCompiler::compile_query_to`],
//! so every SELECT feature works inside one), as a fragment of the enclosing
//! statement's program, placed where the expression is evaluated. Only its
//! destination differs: a register for a scalar subquery or EXISTS, a keyed
//! ephemeral table for IN.
//!
//! The subquery's FROM scope has the enclosing expression's scope as its
//! outer link, so a name the subquery does not define resolves outward,
//! innermost scope first. A subquery that resolves any name outward -- itself
//! or through a subquery nested in it -- is correlated:
//!
//! * An uncorrelated subquery runs once per statement: its fragment is
//!   guarded by `Once`, and later evaluations reuse the result.
//! * A correlated subquery reruns each time the expression is evaluated, i.e.
//!   per outer row. Its cursors and any hash index are set up once (see the
//!   FROM planner), and an equality against an outer column becomes a hash
//!   probe rather than a rescan.

use sqlparser::ast::{Expr, Query};

use super::bytecode::{OpCode, ResultSchema};
use super::compiler::{NameCtx, SqlCompiler};
use super::compiler_select::Sink;
use crate::error::{SqawkError, SqawkResult};

/// The single-value subquery forms.
#[derive(Clone, Copy)]
pub(crate) enum Sub {
    /// `(SELECT ...)`: the first row's value, or NULL for no rows.
    Scalar,
    /// `EXISTS (SELECT ...)`: 1 if there is a row, else 0.
    Exists,
}

/// The error SQLite gives for a subquery used as a value that does not
/// produce exactly one column.
fn check_one_column(schema: &ResultSchema) -> SqawkResult<()> {
    match schema.columns.len() {
        1 => Ok(()),
        n => Err(SqawkError::InvalidSqlQuery(format!(
            "sub-select returns {} columns - expected 1",
            n
        ))),
    }
}

impl<'a> SqlCompiler<'a> {
    /// Record a column reference resolved `depth` scopes outward: the
    /// innermost `depth` subqueries being compiled are correlated.
    pub(crate) fn note_outer_reference(&mut self, depth: usize) {
        let n = self.correlated.len();
        for flag in &mut self.correlated[n.saturating_sub(depth)..] {
            *flag = true;
        }
    }

    /// Emit a subquery fragment: `init` (destination setup), then the query
    /// itself sending its rows to `sink`, the whole guarded by `Once` unless
    /// the query turns out to be correlated. Returns its result schema.
    fn emit_subquery_fragment(
        &mut self,
        query: &Query,
        ctx: &NameCtx<'a>,
        sink: Sink,
        init: &mut dyn FnMut(&mut Self),
    ) -> SqawkResult<ResultSchema> {
        let done = self.label();
        let once = self.program.len();
        self.emit_jump_to(
            OpCode::Once,
            0,
            done,
            0,
            None,
            "uncorrelated subquery: run once",
        );
        init(self);

        let saved_scope = self.outer_scope.replace(ctx.clone());
        let saved_schema = self.program.result_schema.clone();
        self.correlated.push(false);
        let schema = self.compile_query_to(query, sink);
        let correlated = self.correlated.pop().unwrap_or(false);
        self.outer_scope = saved_scope;
        self.program.result_schema = saved_schema;
        let schema = schema?;

        if correlated {
            // Rerun per evaluation: the Once guard becomes a no-op.
            let inst = &mut self.program.instructions[once];
            inst.opcode = OpCode::Noop;
            inst.comment = Some("correlated subquery: run per row".into());
        }
        self.resolve(done);
        Ok(schema)
    }

    /// Compile a scalar or EXISTS subquery into `dest`.
    ///
    /// The value is built in a register of its own and copied out, so a
    /// result computed once survives whatever else the enclosing query does
    /// with `dest` between evaluations.
    pub(crate) fn compile_subquery(
        &mut self,
        query: &Query,
        kind: Sub,
        ctx: &NameCtx<'a>,
        dest: i64,
    ) -> SqawkResult<()> {
        let res = self.allocate_register();
        let (sink, init_op, init_val) = match kind {
            Sub::Scalar => (Sink::Mem(res), OpCode::Null, 0),
            Sub::Exists => (Sink::Exists(res), OpCode::Integer, 0),
        };
        let schema = self.emit_subquery_fragment(query, ctx, sink, &mut |c| {
            c.emit(init_op, init_val, res, 0, None, "subquery result: none yet");
        })?;
        if let Sub::Scalar = kind {
            check_one_column(&schema)?;
        }
        self.emit(OpCode::Copy, res, dest, 0, None, "subquery result");
        Ok(())
    }

    /// Compile `lhs [NOT] IN (SELECT ...)` into `dest`.
    ///
    /// The subquery's rows are materialized into a keyed ephemeral table and
    /// `lhs` is probed with `Found`, with SQL's three-valued result: over an
    /// empty set IN is 0 (NOT IN 1) even for a NULL `lhs`; otherwise a NULL
    /// `lhs` gives NULL, a hit 1, and a miss NULL if the set holds a NULL,
    /// else 0.
    pub(crate) fn compile_in_subquery(
        &mut self,
        lhs: &Expr,
        query: &Query,
        negated: bool,
        ctx: &NameCtx<'a>,
        dest: i64,
    ) -> SqawkResult<()> {
        if matches!(lhs, Expr::Tuple(_)) {
            return Err(SqawkError::UnsupportedSqlFeature(
                "Row-value IN (SELECT ...) is not supported".into(),
            ));
        }
        let set = self.alloc_cursor();
        let nonempty = self.allocate_register();
        let has_null = self.allocate_register();
        let probe = self.allocate_register();
        let schema = self.emit_subquery_fragment(
            query,
            ctx,
            Sink::Set {
                cursor: set,
                nonempty,
            },
            &mut |c| {
                c.emit(
                    OpCode::OpenEphemeral,
                    set,
                    1,
                    1,
                    None,
                    "IN (SELECT ...) set",
                );
                c.emit(
                    OpCode::Integer,
                    0,
                    nonempty,
                    0,
                    None,
                    "IN set: empty so far",
                );
            },
        )?;
        check_one_column(&schema)?;

        let finish = self.label();
        // Whether the set holds a NULL (Found treats NULL keys as equal).
        self.emit(OpCode::Integer, 0, has_null, 0, None, "IN set: no NULL");
        self.emit(OpCode::Null, 0, probe, 0, None, "");
        let no_null = self.label();
        self.emit_jump_to(
            OpCode::NotFound,
            set,
            no_null,
            probe,
            Some("1".into()),
            "IN set: NULL member?",
        );
        self.emit(OpCode::Integer, 1, has_null, 0, None, "IN set: has NULL");
        self.resolve(no_null);

        // Empty set: IN is false whatever the left operand.
        self.emit(OpCode::Integer, 0, dest, 0, None, "IN: false");
        self.emit_jump_to(OpCode::IfZ, nonempty, finish, 0, None, "empty set");

        let x = self.code_expr(lhs, ctx, None)?;
        self.emit(OpCode::IsNull, x, probe, 0, None, "");
        let not_null = self.label();
        self.emit_jump_to(OpCode::IfZ, probe, not_null, 0, None, "");
        self.emit(OpCode::Null, 0, dest, 0, None, "NULL IN (non-empty set)");
        self.emit_jump_to(OpCode::Goto, 0, finish, 0, None, "");
        self.resolve(not_null);

        self.emit(OpCode::Integer, 1, dest, 0, None, "IN: true");
        self.emit_jump_to(
            OpCode::Found,
            set,
            finish,
            x,
            Some("1".into()),
            "IN: probe set",
        );
        self.emit(OpCode::Integer, 0, dest, 0, None, "IN: false");
        self.emit_jump_to(OpCode::IfZ, has_null, finish, 0, None, "no NULL in set");
        self.emit(OpCode::Null, 0, dest, 0, None, "IN: unknown (NULL in set)");
        self.resolve(finish);
        if negated {
            self.emit(OpCode::Not, dest, dest, 0, None, "NOT IN");
        }
        Ok(())
    }
}
