//! SELECT: the query driver, compound operators and the output stage.
//!
//! Every SELECT core runs the same pipeline:
//!
//! ```text
//! FROM planner -> WHERE -> [aggregate stage -> HAVING] -> [window stage] -> output stage
//! ```
//!
//! The output stage computes the output registers, then applies ORDER BY
//! (output-scope names: aliases, ordinals, and hidden sort keys for
//! expressions that are not projected), DISTINCT, and LIMIT/OFFSET, and
//! finally hands each row to a [`Dest`] -- SQLite's `SelectDest`. A plain
//! query's destination is the client (`ResultRow`); a compound operator's arm
//! instead writes into a sorter or a keyed ephemeral table, which is how
//! UNION, INTERSECT and EXCEPT are built (as in SQLite's `multiSelect`).
//!
//! A SELECT without FROM is a FROM plan with zero sources: it produces one
//! row, so WHERE, ORDER BY, DISTINCT and LIMIT apply to it like any other.

use sqlparser::ast::{
    Distinct, Expr, Ident, OrderByExpr, Query, Select, SelectItem, SetExpr, SetOperator,
    SetQuantifier, Value, ValueWithSpan,
};

use super::ast_compat::{
    group_by_exprs, is_group_by_all, is_order_by_all, order_by_is_asc, query_limit, query_offset,
    query_order_by,
};
use super::bytecode::{KeyOrder, OpCode, ResultSchema};
use super::compiler::{Label, NameCtx, OutCol, SqlCompiler};
use super::compiler_check::{check_order_by_expr, check_query, check_select};
use super::compiler_from::FromPlan;
use super::compiler_window::contains_window;
use crate::error::{SqawkError, SqawkResult};

/// LIMIT / OFFSET counter registers, initialized once before the scan.
#[derive(Clone, Copy, Default)]
pub(crate) struct Limits {
    lim: Option<i64>,
    off: Option<i64>,
}

/// Where a query's final rows go, after ORDER BY and LIMIT/OFFSET.
#[derive(Clone, Copy)]
pub(crate) enum Sink {
    /// The client (`ResultRow`).
    Client,
    /// A scalar subquery: the first row's first column into this register,
    /// then stop.
    Mem(i64),
    /// An EXISTS subquery: 1 into this register on the first row, then stop.
    Exists(i64),
    /// An IN subquery: each row's first column into this keyed ephemeral
    /// table, and 1 into `nonempty`.
    Set { cursor: i64, nonempty: i64 },
    /// A table being written by INSERT ... SELECT or CREATE TABLE ... AS:
    /// each row is inserted as `SqlCompiler::insert_target` describes.
    Table,
}

/// Where a SELECT core's finished rows go.
#[derive(Clone, Copy)]
pub(crate) enum Dest {
    /// The query's final destination. The first OFFSET rows are skipped;
    /// the LIMIT-th row jumps to `exit`, ending the whole query.
    Output {
        limits: Limits,
        exit: Label,
        to: Sink,
    },
    /// A compound query's ORDER BY sorter; the payload is the row itself.
    Sorter(i64),
    /// Insert into a keyed ephemeral table (UNION; both sides of
    /// INTERSECT; the left side of EXCEPT).
    Insert(i64),
    /// Remove from a keyed ephemeral table (the right side of EXCEPT).
    Delete(i64),
}

/// One ORDER BY key of a SELECT core.
pub(crate) enum SortKey {
    /// An output column: `ORDER BY 2`, `ORDER BY alias`, or a term that is
    /// textually a projected expression.
    Out(usize),
    /// A hidden sort key: an expression evaluated in the row's own scope (for
    /// a grouped query, the post-aggregation scope), not part of the output.
    Expr(Box<Expr>),
}

/// A planned output stage: register layout plus the ORDER BY / DISTINCT /
/// destination decisions. Built by [`SqlCompiler::output_stage`], fed one row
/// at a time by the producer, closed by [`SqlCompiler::finish_stage`].
pub(crate) struct OutputStage {
    /// Output column count.
    n: usize,
    /// SELECT aliases by output position, for HAVING and sort keys.
    aliases: Vec<(String, usize)>,
    keys: Vec<SortKey>,
    sorter: Option<i64>,
    /// Keyed ephemeral table remembering the rows already emitted.
    distinct: Option<i64>,
    dest: Dest,
    /// Row registers: `[sort keys.., output columns..]`.
    row: i64,
}

impl OutputStage {
    /// First output-column register.
    pub(crate) fn out_base(&self) -> i64 {
        self.row + self.keys.len() as i64
    }

    /// The hidden sort-key expressions, which an aggregate stage must scan
    /// for aggregate calls.
    pub(crate) fn key_exprs(&self) -> impl Iterator<Item = &Expr> {
        self.keys.iter().filter_map(|k| match k {
            SortKey::Expr(e) => Some(e.as_ref()),
            SortKey::Out(_) => None,
        })
    }
}

/// Whether a SELECT is DISTINCT. `DISTINCT ON` is a Postgres extension.
fn is_distinct(select: &Select) -> SqawkResult<bool> {
    match &select.distinct {
        None | Some(Distinct::All) => Ok(false),
        Some(Distinct::Distinct) => Ok(true),
        Some(Distinct::On(_)) => Err(SqawkError::UnsupportedSqlFeature(
            "DISTINCT ON is not supported".into(),
        )),
    }
}

/// A positional ORDER BY key (`ORDER BY 2`) as a zero-based output index.
///
/// Compiled as an ordinary expression, such a key would be a constant and
/// the sort a silent no-op.
fn order_by_ordinal(expr: &Expr, out_columns: usize) -> SqawkResult<Option<usize>> {
    let n = match expr {
        Expr::Value(ValueWithSpan {
            value: Value::Number(n, _),
            ..
        }) => n,
        _ => return Ok(None),
    };
    let idx: usize = n
        .parse()
        .map_err(|_| SqawkError::InvalidSqlQuery(format!("Invalid ORDER BY position: {}", n)))?;
    if idx == 0 || idx > out_columns {
        return Err(SqawkError::InvalidSqlQuery(format!(
            "ORDER BY position {} is out of range (1..{})",
            idx, out_columns
        )));
    }
    Ok(Some(idx - 1))
}

/// The output column whose header `expr` names, if any.
///
/// Matches a qualified name exactly first, then by the bare column name, so
/// `ORDER BY name` finds an output column called `c.name`.
fn schema_position(schema: &ResultSchema, expr: &Expr) -> Option<usize> {
    let want = match expr {
        Expr::Identifier(ident) => ident.value.to_ascii_lowercase(),
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => format!(
            "{}.{}",
            parts[parts.len() - 2].value.to_ascii_lowercase(),
            parts[parts.len() - 1].value.to_ascii_lowercase()
        ),
        _ => return None,
    };
    let cols = &schema.columns;
    if let Some(i) = cols
        .iter()
        .position(|c| c.name.to_ascii_lowercase() == want)
    {
        return Some(i);
    }
    let bare = want.rsplit('.').next().unwrap_or(&want);
    cols.iter().position(|c| {
        let n = c.name.to_ascii_lowercase();
        n.rsplit('.').next() == Some(bare)
    })
}

/// The output column an `ORDER BY alias` names, if `expr` is one.
fn alias_position(outs: &[OutCol], expr: &Expr) -> Option<usize> {
    let Expr::Identifier(ident) = expr else {
        return None;
    };
    outs.iter().position(|o| {
        o.alias
            .as_deref()
            .is_some_and(|a| a.eq_ignore_ascii_case(&ident.value))
    })
}

/// The output column a bare `ORDER BY name` means when `name` does not
/// resolve on its own (typically: ambiguous across joined tables) but exactly
/// one projected column reference has that name.
fn bare_output_position(outs: &[OutCol], expr: &Expr, ctx: &NameCtx<'_>) -> Option<usize> {
    let Expr::Identifier(ident) = expr else {
        return None;
    };
    if ctx.resolve_unqualified(&ident.value).is_ok() {
        return None;
    }
    let mut hits = outs.iter().enumerate().filter(|(_, o)| match &o.expr {
        Expr::Identifier(i) => i.value.eq_ignore_ascii_case(&ident.value),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .is_some_and(|p| p.value.eq_ignore_ascii_case(&ident.value)),
        _ => false,
    });
    match (hits.next(), hits.next()) {
        (Some((i, _)), None) => Some(i),
        _ => None,
    }
}

impl<'a> SqlCompiler<'a> {
    /// Compile a statement-level query, whose rows go to the client.
    pub(crate) fn compile_query(&mut self, query: &Query) -> SqawkResult<()> {
        let schema = self.compile_query_to(query, Sink::Client)?;
        self.program.set_result_schema(schema);
        Ok(())
    }

    /// Compile a query: a SELECT or a compound of SELECTs, with the query's
    /// ORDER BY and LIMIT/OFFSET, sending its rows to `to`. Returns the
    /// result schema.
    ///
    /// A subquery is compiled by this same function, as a fragment of the
    /// enclosing statement's program (see `compiler_subquery.rs`).
    pub(crate) fn compile_query_to(
        &mut self,
        query: &Query,
        to: Sink,
    ) -> SqawkResult<ResultSchema> {
        check_query(query)?;
        for ob in query_order_by(query) {
            check_order_by_expr(ob)?;
        }
        // `ORDER BY ALL` carries no key list, so it would otherwise look
        // identical to "no ORDER BY" and be silently ignored. Reject it.
        if is_order_by_all(query) {
            return Err(SqawkError::UnsupportedSqlFeature(
                "ORDER BY ALL is not supported".into(),
            ));
        }

        let exit = self.label();
        let limits = self.emit_limits(query, exit)?;
        let out = Dest::Output { limits, exit, to };
        let order_by = query_order_by(query);

        let schema = match &*query.body {
            SetExpr::Select(select) => self.compile_select_core(select, order_by, out)?,
            body @ (SetExpr::SetOperation { .. } | SetExpr::Query(_)) if order_by.is_empty() => {
                self.compile_compound(body, out)?
            }
            body @ (SetExpr::SetOperation { .. } | SetExpr::Query(_)) => {
                // Every arm feeds one sorter; its payload is the output row,
                // and the keys are output columns of the leftmost SELECT.
                let sorter = self.alloc_sorter();
                let open = self.program.len();
                self.emit(OpCode::SorterOpen, sorter, 0, 0, None, "compound ORDER BY");
                let schema = self.compile_compound(body, Dest::Sorter(sorter))?;
                let n = schema.columns.len();
                let mut spec = Vec::with_capacity(order_by.len());
                for ob in order_by {
                    let pos = match order_by_ordinal(&ob.expr, n)? {
                        Some(i) => i,
                        None => schema_position(&schema, &ob.expr).ok_or_else(|| {
                            SqawkError::InvalidSqlQuery(format!(
                                "ORDER BY term does not match any column in the result set: {}",
                                ob.expr
                            ))
                        })?,
                    };
                    spec.push(
                        KeyOrder::new(pos, order_by_is_asc(ob), ob.options.nulls_first).spec(),
                    );
                }
                let inst = &mut self.program.instructions[open];
                inst.p2 = n as i64;
                inst.p4 = Some(spec.join(",").into());

                let row = self.allocate_registers(n.max(1));
                self.emit_jump_to(OpCode::SorterSort, sorter, exit, 0, None, "empty: done");
                let top = self.label();
                self.resolve(top);
                self.emit(OpCode::SorterData, sorter, row, n as i64, None, "");
                self.emit_to_dest(row, n, out);
                self.emit_jump_to(OpCode::SorterNext, sorter, top, 0, None, "");
                schema
            }
            _ => {
                return Err(SqawkError::UnsupportedSqlFeature(
                    "Only SELECT queries and compound SELECTs are supported".to_string(),
                ))
            }
        };
        self.resolve(exit);
        Ok(schema)
    }

    /// Evaluate LIMIT and OFFSET into counter registers.
    ///
    /// As in SQLite, either may be any expression yielding an integer; a
    /// negative LIMIT means no limit and a negative OFFSET is zero.
    /// `LIMIT 0` jumps straight to `exit`.
    fn emit_limits(&mut self, query: &Query, exit: Label) -> SqawkResult<Limits> {
        let ctx = NameCtx::empty();
        let operand = |c: &mut Self, e: &Expr, what: &str| -> SqawkResult<i64> {
            let r = c.allocate_register();
            c.code_expr(e, &ctx, Some(r))?;
            c.emit(OpCode::MustBeInt, r, 0, 0, Some(what.to_string()), what);
            Ok(r)
        };
        let lim = match query_limit(query) {
            Some(e) => {
                let r = operand(self, e, "LIMIT")?;
                self.emit_jump_to(OpCode::IfZ, r, exit, 0, None, "LIMIT 0: no rows");
                Some(r)
            }
            None => None,
        };
        let off = match query_offset(query) {
            Some(e) => Some(operand(self, e, "OFFSET")?),
            None => None,
        };
        Ok(Limits { lim, off })
    }

    /// Compile one operand of a compound query, or the compound itself,
    /// sending every row to `dest`. Returns the result schema, which is the
    /// leftmost SELECT's.
    fn compile_compound(&mut self, expr: &SetExpr, dest: Dest) -> SqawkResult<ResultSchema> {
        let (op, quantifier, left, right) = match expr {
            SetExpr::Select(select) => return self.compile_select_core(select, &[], dest),
            SetExpr::Query(q) => {
                check_query(q)?;
                if q.with.is_some()
                    || !query_order_by(q).is_empty()
                    || query_limit(q).is_some()
                    || query_offset(q).is_some()
                {
                    return Err(SqawkError::UnsupportedSqlFeature(
                        "ORDER BY, LIMIT or WITH inside a compound SELECT operand is not supported"
                            .into(),
                    ));
                }
                return self.compile_compound(&q.body, dest);
            }
            SetExpr::SetOperation {
                op,
                set_quantifier,
                left,
                right,
            } => (op, set_quantifier, left, right),
            _ => {
                return Err(SqawkError::UnsupportedSqlFeature(
                    "Unsupported compound SELECT operand".to_string(),
                ))
            }
        };
        let all = match quantifier {
            SetQuantifier::All => true,
            SetQuantifier::None | SetQuantifier::Distinct => false,
            q => {
                return Err(SqawkError::UnsupportedSqlFeature(format!(
                    "{} {} is not supported",
                    op, q
                )))
            }
        };
        self.add_comment(&format!("{}{}", op, if all { " ALL" } else { "" }));

        let check = |l: &ResultSchema, r: &ResultSchema| {
            if l.columns.len() == r.columns.len() {
                Ok(())
            } else {
                Err(SqawkError::InvalidSqlQuery(format!(
                    "SELECTs to the left and right of {} do not have the same number of result columns",
                    op
                )))
            }
        };

        match (op, all) {
            // Both arms straight to the destination: every destination acts
            // per row, so this is right for all of them.
            (SetOperator::Union, true) => {
                let l = self.compile_compound(left, dest)?;
                let r = self.compile_compound(right, dest)?;
                check(&l, &r)?;
                Ok(l)
            }
            // Already deduplicating into a keyed table: nothing to add.
            (SetOperator::Union, false) if matches!(dest, Dest::Insert(_)) => {
                let l = self.compile_compound(left, dest)?;
                let r = self.compile_compound(right, dest)?;
                check(&l, &r)?;
                Ok(l)
            }
            (SetOperator::Union, false) => {
                let (u, open) = self.open_keyed("UNION");
                let l = self.compile_compound(left, Dest::Insert(u))?;
                self.program.instructions[open].p2 = l.columns.len() as i64;
                let r = self.compile_compound(right, Dest::Insert(u))?;
                check(&l, &r)?;
                self.emit_keyed_scan(u, l.columns.len(), dest, None);
                Ok(l)
            }
            (SetOperator::Except | SetOperator::Minus, false) => {
                let (u, open) = self.open_keyed("EXCEPT");
                let l = self.compile_compound(left, Dest::Insert(u))?;
                self.program.instructions[open].p2 = l.columns.len() as i64;
                let r = self.compile_compound(right, Dest::Delete(u))?;
                check(&l, &r)?;
                self.emit_keyed_scan(u, l.columns.len(), dest, None);
                Ok(l)
            }
            (SetOperator::Intersect, false) => {
                // As SQLite: both sides into keyed tables, then the left
                // table's rows that the right one holds -- so the values
                // come from the left (`SELECT 1 INTERSECT SELECT 1.0` is 1).
                let (lt, open_l) = self.open_keyed("INTERSECT left");
                let l = self.compile_compound(left, Dest::Insert(lt))?;
                let n = l.columns.len() as i64;
                self.program.instructions[open_l].p2 = n;
                let (rt, open_r) = self.open_keyed("INTERSECT right");
                self.program.instructions[open_r].p2 = n;
                let r = self.compile_compound(right, Dest::Insert(rt))?;
                check(&l, &r)?;
                self.emit_keyed_scan(lt, l.columns.len(), dest, Some(rt));
                Ok(l)
            }
            (op, _) => Err(SqawkError::UnsupportedSqlFeature(format!(
                "{} ALL is not supported",
                op
            ))),
        }
    }

    /// Open a keyed ephemeral table; returns its cursor and the address of
    /// the OpenEphemeral, whose column count (p2) the caller patches once the
    /// leftmost operand has been compiled.
    fn open_keyed(&mut self, what: &str) -> (i64, usize) {
        let cursor = self.alloc_cursor();
        let addr = self.program.len();
        self.emit(OpCode::OpenEphemeral, cursor, 0, 1, None, what);
        (cursor, addr)
    }

    /// Walk a keyed ephemeral table (in sorted order), sending each row on
    /// -- only the rows also in keyed table `only_in`, if given.
    fn emit_keyed_scan(&mut self, cursor: i64, n: usize, dest: Dest, only_in: Option<i64>) {
        let row = self.allocate_registers(n.max(1));
        let top = self.label();
        let done = self.label();
        let next = self.label();
        self.emit_jump_to(OpCode::Rewind, cursor, done, 0, None, "compound result");
        self.resolve(top);
        for i in 0..n {
            self.emit(OpCode::Column, cursor, i as i64, row + i as i64, None, "");
        }
        if let Some(other) = only_in {
            self.emit_jump_to(
                OpCode::NotFound,
                other,
                next,
                row,
                Some(n.to_string()),
                "INTERSECT: not on the right",
            );
        }
        self.emit_to_dest(row, n, dest);
        self.resolve(next);
        self.emit_jump_to(OpCode::Next, cursor, top, 0, None, "");
        self.resolve(done);
    }

    /// Compile one SELECT core, sending its rows to `dest`, and return its
    /// result schema.
    ///
    /// Every FROM shape -- none, one table, a comma list, any chain of joins
    /// -- goes through the one FROM planner; the stage after it (plain
    /// projection, aggregation, windows) consumes rows from the planner's
    /// loop nest and hands finished rows to the output stage.
    fn compile_select_core(
        &mut self,
        select: &Select,
        order_by: &[OrderByExpr],
        dest: Dest,
    ) -> SqawkResult<ResultSchema> {
        check_select(select)?;
        // `GROUP BY ALL` carries no key list, so it would otherwise look
        // identical to "no GROUP BY" and silently collapse the query.
        if is_group_by_all(&select.group_by) {
            return Err(SqawkError::UnsupportedSqlFeature(
                "GROUP BY ALL is not supported".into(),
            ));
        }
        let distinct = is_distinct(select)?;

        let plan = self.plan_from(&select.from, select.selection.as_ref())?;

        let has_group_by = !group_by_exprs(&select.group_by).is_empty();
        let has_aggregates = self.has_aggregates(&select.projection)
            || select.having.is_some()
            || order_by.iter().any(|ob| self.is_aggregate_expr(&ob.expr));
        let windowed = select.projection.iter().any(|item| match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                contains_window(e)
            }
            _ => false,
        }) || order_by.iter().any(|ob| contains_window(&ob.expr));

        if has_group_by || has_aggregates {
            self.compile_select_grouped(select, &plan, order_by, distinct, dest, windowed)?;
        } else if windowed {
            self.compile_select_windowed(select, &plan, order_by, distinct, dest)?;
        } else {
            let outs = self.select_outputs(select, &plan)?;
            let ctx = plan.ctx();
            let st = self.output_stage(&outs, order_by, distinct, dest, Some(&ctx))?;
            self.emit_scan(&plan, &mut |c, ctx| c.emit_stage_row(&st, &outs, ctx, None))?;
            self.finish_stage(&st);
        }
        Ok(self.program.result_schema.clone())
    }

    /// Expand the SELECT list and set the result schema from it.
    pub(crate) fn select_outputs(
        &mut self,
        select: &Select,
        plan: &FromPlan<'a>,
    ) -> SqawkResult<Vec<OutCol>> {
        let outs = self.expand_projection(select, plan)?;
        if outs.is_empty() {
            return Err(SqawkError::InvalidSqlQuery(
                "SELECT * with no tables specified".into(),
            ));
        }
        let ctx = plan.ctx();
        let mut schema = ResultSchema::new();
        for out in &outs {
            schema.add_column(out.name.clone(), self.infer_expr_type(&out.expr, &ctx));
        }
        self.program.set_result_schema(schema);
        Ok(outs)
    }

    /// A window query without aggregation: every row of the FROM/WHERE scan,
    /// with all its columns, goes to the window stage, which then feeds the
    /// output stage.
    fn compile_select_windowed(
        &mut self,
        select: &Select,
        plan: &FromPlan<'a>,
        order_by: &[OrderByExpr],
        distinct: bool,
        dest: Dest,
    ) -> SqawkResult<()> {
        let outs = self.select_outputs(select, plan)?;
        let ctx = plan.ctx();
        let st = self.output_stage(&outs, order_by, distinct, dest, Some(&ctx))?;
        let mut ws = self.window_stage(select, &outs, &st)?;
        self.emit_scan(plan, &mut |c, ctx| {
            let row = c.materialize_row(ctx);
            c.emit_window_row(&mut ws, &row, None)
        })?;
        self.finish_window(ws, &st, &outs)?;
        self.finish_stage(&st);
        Ok(())
    }

    /// Plan the output stage of a SELECT core and emit its setup (sorter,
    /// DISTINCT table).
    ///
    /// ORDER BY terms are resolved in output scope first -- an ordinal, a
    /// SELECT alias, a projected expression -- and otherwise become hidden
    /// sort keys evaluated in `ctx`. With no `ctx`, hidden keys are not
    /// available.
    pub(crate) fn output_stage(
        &mut self,
        outs: &[OutCol],
        order_by: &[OrderByExpr],
        distinct: bool,
        dest: Dest,
        ctx: Option<&NameCtx<'a>>,
    ) -> SqawkResult<OutputStage> {
        let n = outs.len();
        let schema = self.program.result_schema.clone();
        let mut keys = Vec::with_capacity(order_by.len());
        let mut spec = Vec::with_capacity(order_by.len());
        for (i, ob) in order_by.iter().enumerate() {
            let text = NameCtx::agg_key(&ob.expr);
            let key = if let Some(j) = order_by_ordinal(&ob.expr, n)? {
                SortKey::Out(j)
            } else if let Some(j) = alias_position(outs, &ob.expr) {
                SortKey::Out(j)
            } else if let Some(j) = outs.iter().position(|o| NameCtx::agg_key(&o.expr) == text) {
                SortKey::Out(j)
            } else if let Some(ctx) = ctx {
                match bare_output_position(outs, &ob.expr, ctx) {
                    Some(j) => SortKey::Out(j),
                    None => SortKey::Expr(Box::new(ob.expr.clone())),
                }
            } else {
                SortKey::Out(schema_position(&schema, &ob.expr).ok_or_else(|| {
                    SqawkError::InvalidSqlQuery(format!(
                        "ORDER BY expression must appear in the SELECT list: {}",
                        ob.expr
                    ))
                })?)
            };
            keys.push(key);
            spec.push(KeyOrder::new(i, order_by_is_asc(ob), ob.options.nulls_first).spec());
        }
        let nk = keys.len();

        let sorter = (nk > 0).then(|| {
            let id = self.alloc_sorter();
            self.emit(
                OpCode::SorterOpen,
                id,
                (nk + n) as i64,
                0,
                Some(spec.join(",")),
                "ORDER BY",
            );
            id
        });
        let distinct = distinct.then(|| {
            let id = self.alloc_cursor();
            self.emit(OpCode::OpenEphemeral, id, n as i64, 1, None, "DISTINCT");
            id
        });
        let aliases = outs
            .iter()
            .enumerate()
            .filter_map(|(i, o)| o.alias.clone().map(|a| (a, i)))
            .collect();
        let row = self.allocate_registers((nk + n).max(1));
        Ok(OutputStage {
            n,
            aliases,
            keys,
            sorter,
            distinct,
            dest,
            row,
        })
    }

    /// Emit one row through the output stage: compute the output columns in
    /// `ctx`, then [`Self::emit_stage_computed`].
    pub(crate) fn emit_stage_row(
        &mut self,
        st: &OutputStage,
        outs: &[OutCol],
        ctx: &NameCtx<'a>,
        having: Option<&Expr>,
    ) -> SqawkResult<()> {
        let base = st.out_base();
        for (i, out) in outs.iter().enumerate() {
            self.code_expr(&out.expr, ctx, Some(base + i as i64))?;
        }
        self.emit_stage_computed(st, ctx, having)
    }

    /// The output columns are in their registers: test HAVING, compute the
    /// sort keys, then sort or emit.
    ///
    /// HAVING and hidden sort keys see the SELECT aliases (as SQLite allows)
    /// for names that are not columns: each such alias is bound to its output
    /// register.
    pub(crate) fn emit_stage_computed(
        &mut self,
        st: &OutputStage,
        ctx: &NameCtx<'a>,
        having: Option<&Expr>,
    ) -> SqawkResult<()> {
        let base = st.out_base();
        let needs_scope = having.is_some() || st.key_exprs().next().is_some();
        let mut scope = ctx.clone();
        if needs_scope {
            for (alias, i) in &st.aliases {
                if ctx.resolve_unqualified(alias).is_err() {
                    scope.bind_agg(
                        &Expr::Identifier(Ident::new(alias.clone())),
                        base + *i as i64,
                    );
                }
            }
        }

        let skip = self.label();
        if let Some(having) = having {
            let r = self.code_expr(having, &scope, None)?;
            self.emit_jump_to(OpCode::IfZ, r, skip, 0, None, "HAVING failed");
        }
        for (i, key) in st.keys.iter().enumerate() {
            let dest = st.row + i as i64;
            match key {
                SortKey::Out(j) => self.emit(
                    OpCode::Copy,
                    base + *j as i64,
                    dest,
                    0,
                    None,
                    "sort key = output column",
                ),
                SortKey::Expr(e) => {
                    self.code_expr(e, &scope, Some(dest))?;
                }
            }
        }
        match st.sorter {
            Some(id) => self.emit(
                OpCode::SorterInsert,
                id,
                st.row,
                (st.keys.len() + st.n) as i64,
                None,
                "",
            ),
            None => self.emit_output_row(st, base),
        }
        self.resolve(skip);
        Ok(())
    }

    /// Close the output stage: drain the sorter, if there is one.
    pub(crate) fn finish_stage(&mut self, st: &OutputStage) {
        let Some(id) = st.sorter else { return };
        let done = self.label();
        self.emit_jump_to(OpCode::SorterSort, id, done, 0, None, "empty: done");
        let top = self.label();
        self.resolve(top);
        self.emit(
            OpCode::SorterData,
            id,
            st.row,
            (st.keys.len() + st.n) as i64,
            None,
            "",
        );
        self.emit_output_row(st, st.out_base());
        self.emit_jump_to(OpCode::SorterNext, id, top, 0, None, "");
        self.resolve(done);
    }

    /// A finished output row: drop it if DISTINCT has seen it, else send it
    /// to the destination.
    fn emit_output_row(&mut self, st: &OutputStage, start: i64) {
        let Some(seen) = st.distinct else {
            self.emit_to_dest(start, st.n, st.dest);
            return;
        };
        let skip = self.label();
        self.emit_jump_to(
            OpCode::Found,
            seen,
            skip,
            start,
            Some(st.n.to_string()),
            "DISTINCT: seen",
        );
        self.emit(OpCode::IdxInsert, seen, start, st.n as i64, None, "");
        self.emit_to_dest(start, st.n, st.dest);
        self.resolve(skip);
    }

    /// Insert the `n`-column row at `start` into the INSERT / CREATE TABLE
    /// AS target, scattering it into the target's column positions.
    fn emit_table_insert(&mut self, start: i64, n: usize) {
        let target = self
            .insert_target
            .clone()
            .expect("Sink::Table is used only with an insert target");
        let (row, width) = match &target.columns {
            None => (start, n),
            Some((width, cols)) => {
                let row = self.allocate_registers((*width).max(1));
                for i in 0..*width {
                    self.emit(OpCode::Null, 0, row + i as i64, 0, None, "");
                }
                for (i, &c) in cols.iter().enumerate().take(n) {
                    self.emit(OpCode::Copy, start + i as i64, row + c as i64, 0, None, "");
                }
                (row, *width)
            }
        };
        self.emit(
            OpCode::InsertRow,
            target.cursor,
            row,
            width as i64,
            None,
            "Insert row",
        );
    }

    /// Send the `n`-column row at `start` to `dest`.
    fn emit_to_dest(&mut self, start: i64, n: usize, dest: Dest) {
        let n64 = n as i64;
        match dest {
            Dest::Output { limits, exit, to } => {
                let skip = self.label();
                if let Some(off) = limits.off {
                    let emit = self.label();
                    let dec = self.label();
                    self.emit_jump_to(OpCode::IfPos, off, dec, 0, None, "OFFSET remaining?");
                    self.emit_jump_to(OpCode::Goto, 0, emit, 0, None, "");
                    self.resolve(dec);
                    self.emit_jump_to(OpCode::DecrJumpZero, off, skip, 0, None, "skip row");
                    self.emit_jump_to(OpCode::Goto, 0, skip, 0, None, "");
                    self.resolve(emit);
                }
                match to {
                    Sink::Client => self.emit(OpCode::ResultRow, start, n64, 0, None, "Output row"),
                    // One row is all a scalar subquery or EXISTS needs.
                    Sink::Mem(reg) => {
                        self.emit(OpCode::Copy, start, reg, 0, None, "scalar subquery value");
                        self.emit_jump_to(OpCode::Goto, 0, exit, 0, None, "first row only");
                    }
                    Sink::Exists(reg) => {
                        self.emit(OpCode::Integer, 1, reg, 0, None, "EXISTS: a row");
                        self.emit_jump_to(OpCode::Goto, 0, exit, 0, None, "one row suffices");
                    }
                    Sink::Set { cursor, nonempty } => {
                        self.emit(OpCode::IdxInsert, cursor, start, 1, None, "IN set member");
                        self.emit(OpCode::Integer, 1, nonempty, 0, None, "IN set not empty");
                    }
                    Sink::Table => self.emit_table_insert(start, n),
                }
                if let (Some(lim), Sink::Client | Sink::Set { .. } | Sink::Table) = (limits.lim, to)
                {
                    self.emit_jump_to(OpCode::DecrJumpZero, lim, exit, 0, None, "LIMIT reached");
                }
                self.resolve(skip);
            }
            Dest::Sorter(id) => self.emit(OpCode::SorterInsert, id, start, n64, None, ""),
            Dest::Insert(c) => self.emit(OpCode::IdxInsert, c, start, n64, None, ""),
            Dest::Delete(c) => self.emit(OpCode::IdxDelete, c, start, n64, None, ""),
        }
    }
}
