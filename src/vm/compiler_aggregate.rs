//! Aggregate and GROUP BY compilation
//!
//! Consumes rows from the FROM planner's loop nest, so aggregation works the
//! same over a single table, a comma join or any chain of explicit joins.
//!
//! Without GROUP BY every row steps the accumulators directly. With GROUP BY
//! each row's keys and aggregate arguments go into a sorter; the sorted rows
//! are then walked once, flushing a group whenever the key vector changes.
//! Either way the output row is produced by the shared expression compiler in
//! a post-aggregation scope where each aggregate and each GROUP BY key is
//! bound to the register holding its value, which is what makes `SUM(x) + 1`,
//! `ROUND(AVG(x), 1)`, `UPPER(key)` and arbitrary HAVING expressions work.

use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, OrderByExpr, Select, Value,
    ValueWithSpan,
};

use super::ast_compat::{func_args, group_by_exprs};
use super::bytecode::{OpCode, AGG_DISTINCT, AGG_SEP, AGG_TYPE_MASK};
use super::compiler::{NameCtx, OutCol, SqlCompiler};
use super::compiler_from::{expr_children, FromPlan};
use super::compiler_select::{Dest, OutputStage};
use super::compiler_window::{window_call_children, WindowStage};
use crate::aggregate::AggregateFunction;
use crate::error::{SqawkError, SqawkResult};

/// What a grouped query's output step needs, shared by its flush points.
struct Group<'g, 'a> {
    plan: &'g FromPlan<'a>,
    having: Option<&'g Expr>,
    outs: &'g [OutCol],
    keys: &'g [Expr],
    aggs: &'g [Agg],
    stage: &'g OutputStage,
}

/// One aggregate call of a grouped query.
struct Agg {
    /// The call itself, which the post-aggregation scope binds by its text.
    call: Expr,
    /// AggStep/AggFinal function type, including the DISTINCT and
    /// separator flags.
    func_type: i64,
    /// The arguments; none for `COUNT(*)`. GROUP_CONCAT's separator is the
    /// second.
    args: Vec<Expr>,
    /// `FILTER (WHERE ...)`: only rows for which it is true are aggregated.
    filter: Option<Expr>,
}

impl Agg {
    /// Registers the aggregate's inputs occupy per row: the arguments (one
    /// placeholder for `COUNT(*)`), then the FILTER condition.
    fn slots(&self) -> usize {
        self.args.len().max(1) + self.filter.is_some() as usize
    }

    /// Offset of the FILTER condition within the aggregate's slots.
    fn filter_slot(&self) -> usize {
        self.args.len().max(1)
    }
}

impl<'a> SqlCompiler<'a> {
    /// Collect every aggregate call within `expr`, in order of appearance and
    /// without duplicates.
    ///
    /// Recursive through function arguments, CASE, arithmetic and the rest,
    /// so `ROUND(AVG(x), 1)` and `SUM(CASE ... END)` are found, not just a
    /// top-level call.
    fn collect_aggregates(expr: &Expr, out: &mut Vec<Expr>) {
        if Self::is_aggregate_call(expr) {
            let key = NameCtx::agg_key(expr);
            if !out.iter().any(|e| NameCtx::agg_key(e) == key) {
                out.push(expr.clone());
            }
            return;
        }
        // A window over grouped rows: `SUM(COUNT(*)) OVER ()`.
        if let Expr::Function(f) = expr {
            if f.over.is_some() {
                for child in window_call_children(f) {
                    Self::collect_aggregates(child, out);
                }
                return;
            }
        }
        if let Some(children) = expr_children(expr) {
            for child in children {
                Self::collect_aggregates(child, out);
            }
        }
    }

    /// Describe an aggregate call for AggStep, rejecting every part of the
    /// call syntax the aggregate stage does not implement.
    fn describe_aggregate(call: Expr) -> SqawkResult<Agg> {
        let Expr::Function(func) = &call else {
            unreachable!("collect_aggregates only collects function calls")
        };
        let name = func.name.to_string().to_ascii_uppercase();
        let unsupported = |what: &str| {
            Err(SqawkError::UnsupportedSqlFeature(format!(
                "{} on aggregate {}() is not supported",
                what, name
            )))
        };
        if func.null_treatment.is_some() {
            return unsupported("IGNORE/RESPECT NULLS");
        }
        if !func.within_group.is_empty() {
            return unsupported("WITHIN GROUP");
        }
        if !matches!(func.parameters, FunctionArguments::None) {
            return unsupported("A parameter list");
        }
        match &func.args {
            FunctionArguments::Subquery(_) => return unsupported("A subquery argument"),
            FunctionArguments::List(list) if !list.clauses.is_empty() => {
                return unsupported("ORDER BY / LIMIT inside the argument list")
            }
            _ => {}
        }
        let is_count = name == "COUNT";
        let mut args = Vec::new();
        for a in func_args(func) {
            match a {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => args.push(e.clone()),
                // COUNT(*) counts rows: no argument.
                FunctionArg::Unnamed(FunctionArgExpr::Wildcard) if is_count => {}
                _ => {
                    return Err(SqawkError::InvalidSqlQuery(format!(
                        "Unsupported aggregate argument: {}",
                        call
                    )))
                }
            }
        }
        let agg = AggregateFunction::from_name(&name)
            .expect("collect_aggregates only collects aggregate calls");
        let (min, max) = agg.arity(&name);
        // COUNT() and COUNT(*) both count rows.
        let min = if is_count { 0 } else { min };
        if args.len() < min || args.len() > max {
            return Err(SqawkError::InvalidSqlQuery(format!(
                "wrong number of arguments to function {}()",
                name
            )));
        }
        let mut func_type = Self::agg_func_type_for(func);
        if func_type & AGG_DISTINCT != 0 && args.len() != 1 {
            return Err(SqawkError::InvalidSqlQuery(
                "DISTINCT aggregates must have exactly one argument".into(),
            ));
        }
        if args.len() == 2 {
            func_type |= AGG_SEP;
        }
        Ok(Agg {
            filter: func.filter.as_deref().cloned(),
            call,
            func_type,
            args,
        })
    }

    /// Compile a SELECT with aggregates and/or GROUP BY.
    ///
    /// Aggregates are collected from the SELECT list, HAVING and the ORDER BY
    /// keys that are not output columns, so `ORDER BY COUNT(*)` works without
    /// projecting the count.
    pub(crate) fn compile_select_grouped(
        &mut self,
        select: &Select,
        plan: &FromPlan<'a>,
        order_by: &[OrderByExpr],
        distinct: bool,
        dest: Dest,
        windowed: bool,
    ) -> SqawkResult<()> {
        let ctx = plan.ctx();
        let outs = self.select_outputs(select, plan)?;

        // GROUP BY keys. `GROUP BY 2` names the second output column, and a
        // name that is no column but is a SELECT alias names that column.
        let mut keys: Vec<Expr> = Vec::new();
        for g in group_by_exprs(&select.group_by) {
            let key = match g {
                Expr::Value(ValueWithSpan {
                    value: Value::Number(n, _),
                    ..
                }) => {
                    let pos: usize = n.parse().unwrap_or(0);
                    if pos == 0 || pos > outs.len() {
                        return Err(SqawkError::InvalidSqlQuery(format!(
                            "GROUP BY position {} is out of range (1..{})",
                            n,
                            outs.len()
                        )));
                    }
                    outs[pos - 1].expr.clone()
                }
                Expr::Identifier(ident) if ctx.resolve_unqualified(&ident.value).is_err() => {
                    match outs.iter().find(|o| {
                        o.alias
                            .as_deref()
                            .is_some_and(|a| a.eq_ignore_ascii_case(&ident.value))
                    }) {
                        Some(o) => o.expr.clone(),
                        None => g.clone(),
                    }
                }
                _ => g.clone(),
            };
            keys.push(key);
        }

        let st = self.output_stage(&outs, order_by, distinct, dest, Some(&ctx))?;
        // Windows over the grouped rows: each group's row goes to the window
        // stage, which feeds the output stage once all groups exist.
        let mut ws = if windowed {
            Some(self.window_stage(select, &outs, &st)?)
        } else {
            None
        };

        let mut calls = Vec::new();
        for out in &outs {
            Self::collect_aggregates(&out.expr, &mut calls);
        }
        if let Some(h) = &select.having {
            Self::collect_aggregates(h, &mut calls);
        }
        for e in st.key_exprs() {
            Self::collect_aggregates(e, &mut calls);
        }
        let aggs = calls
            .into_iter()
            .map(Self::describe_aggregate)
            .collect::<SqawkResult<Vec<_>>>()?;
        let group = Group {
            plan,
            having: select.having.as_ref(),
            outs: &outs,
            keys: &keys,
            aggs: &aggs,
            stage: &st,
        };

        let nk = keys.len();
        let na = aggs.len();

        if nk == 0 {
            // The whole input is one group, which exists even when empty.
            self.add_comment("Aggregate query (no GROUP BY)");
            let acc = self.allocate_registers(na.max(1));
            self.emit_acc_reset(acc, na);
            self.emit_scan(plan, &mut |c, ctx| {
                for (j, agg) in aggs.iter().enumerate() {
                    let skip = c.label();
                    if let Some(f) = &agg.filter {
                        let cond = c.code_expr(f, ctx, None)?;
                        c.emit_jump_to(OpCode::IfZ, cond, skip, 0, None, "FILTER: not true");
                    }
                    // -1: COUNT(*), which counts rows rather than values.
                    let val = if agg.args.is_empty() {
                        -1
                    } else {
                        let base = c.allocate_registers(agg.args.len());
                        for (i, e) in agg.args.iter().enumerate() {
                            c.code_expr(e, ctx, Some(base + i as i64))?;
                        }
                        base
                    };
                    c.emit(
                        OpCode::AggStep,
                        agg.func_type,
                        val,
                        acc + j as i64,
                        None,
                        "",
                    );
                    c.resolve(skip);
                }
                Ok(())
            })?;
            self.emit_group_output(&group, -1, acc, ws.as_mut())?;
            return self.finish_grouped(ws, &st, &outs);
        }

        self.add_comment("GROUP BY query");
        let sorter_id = self.alloc_sorter();
        // Sorter row: [keys.., then each aggregate's slots..].
        let mut offsets = Vec::with_capacity(na);
        let mut total = nk;
        for agg in &aggs {
            offsets.push(total);
            total += agg.slots();
        }
        let spec: String = (0..nk)
            .map(|i| format!("{}:asc", i))
            .collect::<Vec<_>>()
            .join(",");
        self.emit(
            OpCode::SorterOpen,
            sorter_id,
            total as i64,
            0,
            Some(spec),
            "GROUP BY sorter",
        );

        let row = self.allocate_registers(total);
        self.emit_scan(plan, &mut |c, ctx| {
            for (i, k) in keys.iter().enumerate() {
                c.code_expr(k, ctx, Some(row + i as i64))?;
            }
            for (agg, &off) in aggs.iter().zip(&offsets) {
                let base = row + off as i64;
                if agg.args.is_empty() {
                    // COUNT(*): a non-NULL placeholder, counted once per row.
                    c.emit(OpCode::Integer, 1, base, 0, None, "COUNT(*)");
                }
                for (i, e) in agg.args.iter().enumerate() {
                    c.code_expr(e, ctx, Some(base + i as i64))?;
                }
                if let Some(f) = &agg.filter {
                    c.code_expr(f, ctx, Some(base + agg.filter_slot() as i64))?;
                }
            }
            c.emit(OpCode::SorterInsert, sorter_id, row, total as i64, None, "");
            Ok(())
        })?;

        // No input rows: no groups, so nothing to drain or flush.
        let done = self.label();
        self.emit_jump_to(
            OpCode::SorterSort,
            sorter_id,
            done,
            0,
            None,
            "empty: no groups",
        );

        let key_reg = self.allocate_registers(nk);
        let acc = self.allocate_registers(na.max(1));
        self.emit_acc_reset(acc, na);
        let first = self.allocate_register();
        self.emit(OpCode::Integer, 1, first, 0, None, "first row");

        let top = self.label();
        let flush = self.label();
        let update_key = self.label();
        let step = self.label();
        self.resolve(top);
        self.emit(OpCode::SorterData, sorter_id, row, total as i64, None, "");
        self.emit_jump_to(
            OpCode::IfPos,
            first,
            update_key,
            0,
            None,
            "first row: no flush",
        );
        // Compare ALL key columns; NULL keys compare equal, so they group
        // together.
        self.emit(
            OpCode::Compare,
            row,
            key_reg,
            nk as i64,
            None,
            "Compare group key vector",
        );
        self.emit_jump_to_three(flush, step, flush, "Group changed?");
        self.resolve(flush);
        self.emit_group_output(&group, key_reg, acc, ws.as_mut())?;
        for j in 0..na {
            self.emit(OpCode::AggReset, acc + j as i64, 0, 0, None, "");
        }
        self.resolve(update_key);
        for i in 0..nk {
            self.emit(
                OpCode::Copy,
                row + i as i64,
                key_reg + i as i64,
                0,
                None,
                "",
            );
        }
        self.emit(OpCode::Integer, 0, first, 0, None, "");
        self.resolve(step);
        for (j, (agg, &off)) in aggs.iter().zip(&offsets).enumerate() {
            let base = row + off as i64;
            let skip = self.label();
            if agg.filter.is_some() {
                self.emit_jump_to(
                    OpCode::IfZ,
                    base + agg.filter_slot() as i64,
                    skip,
                    0,
                    None,
                    "FILTER: not true",
                );
            }
            self.emit(
                OpCode::AggStep,
                agg.func_type,
                base,
                acc + j as i64,
                None,
                "",
            );
            self.resolve(skip);
        }
        self.emit_jump_to(OpCode::SorterNext, sorter_id, top, 0, None, "");
        // The last group, which no key change ever flushed.
        self.emit_group_output(&group, key_reg, acc, ws.as_mut())?;
        self.resolve(done);
        self.finish_grouped(ws, &st, &outs)
    }

    /// Close a grouped query: run the window stage, if any, then drain the
    /// output stage.
    fn finish_grouped(
        &mut self,
        ws: Option<WindowStage<'a>>,
        st: &OutputStage,
        outs: &[OutCol],
    ) -> SqawkResult<()> {
        if let Some(ws) = ws {
            self.finish_window(ws, st, outs)?;
        }
        self.finish_stage(st);
        Ok(())
    }

    /// Start `n` accumulators fresh. A NULL accumulator register is how
    /// AggStep knows to begin a new aggregate; it must be set explicitly
    /// because a subquery's aggregation can run again, once per outer row.
    fn emit_acc_reset(&mut self, acc: i64, n: usize) {
        for j in 0..n as i64 {
            self.emit(OpCode::Null, 0, acc + j, 0, None, "accumulator: fresh");
        }
    }

    /// Emit one output row of a grouped query: finalize the aggregates and
    /// hand the row to the output stage, which tests HAVING.
    ///
    /// Called from both flush points of the GROUP BY drain loop, and once for
    /// an aggregate query without GROUP BY.
    fn emit_group_output(
        &mut self,
        g: &Group<'_, 'a>,
        key_reg: i64,
        acc: i64,
        ws: Option<&mut WindowStage<'a>>,
    ) -> SqawkResult<()> {
        let fin = self.allocate_registers(g.aggs.len().max(1));
        for (j, agg) in g.aggs.iter().enumerate() {
            // p3: the function, for an accumulator no row ever stepped --
            // COUNT of nothing is 0, every other aggregate of nothing NULL.
            self.emit(
                OpCode::AggFinal,
                acc + j as i64,
                fin + j as i64,
                agg.func_type & AGG_TYPE_MASK,
                None,
                "",
            );
        }

        // Post-aggregation scope: aggregates and keys are values in
        // registers; any other column reference is an error.
        let mut ctx = g.plan.ctx();
        for (j, agg) in g.aggs.iter().enumerate() {
            ctx.agg_bindings
                .push((NameCtx::agg_key(&agg.call), fin + j as i64));
        }
        let mut cols = Vec::new();
        for (i, k) in g.keys.iter().enumerate() {
            let reg = key_reg + i as i64;
            ctx.agg_bindings.push((NameCtx::agg_key(k), reg));
            let col = match k {
                Expr::Identifier(ident) => ctx.resolve_unqualified(&ident.value).ok(),
                Expr::CompoundIdentifier(parts) if parts.len() >= 2 => ctx
                    .resolve_qualified(&parts[parts.len() - 2].value, &parts[parts.len() - 1].value)
                    .ok(),
                _ => None,
            };
            if let Some(col) = col {
                cols.push((col, reg));
            }
        }
        ctx.col_bindings = Some(cols);
        match ws {
            Some(ws) => self.emit_window_row(ws, &ctx, g.having),
            None => self.emit_stage_row(g.stage, g.outs, &ctx, g.having),
        }
    }
}
