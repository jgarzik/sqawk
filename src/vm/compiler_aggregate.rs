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
    Expr, FunctionArg, FunctionArgExpr, Select, SelectItem, Value, ValueWithSpan,
};

use super::ast_compat::{func_args, group_by_exprs};
use super::bytecode::{OpCode, ResultSchema};
use super::compiler::{NameCtx, SqlCompiler};
use super::compiler_from::{expr_children, FromPlan};
use crate::error::{SqawkError, SqawkResult};

/// One aggregate call of a grouped query.
struct Agg {
    /// The call itself, which the post-aggregation scope binds by its text.
    call: Expr,
    /// AggStep/AggFinal function type, including the DISTINCT flag.
    func_type: i64,
    /// The argument, or `None` for `COUNT(*)`.
    arg: Option<Expr>,
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
        if let Some(children) = expr_children(expr) {
            for child in children {
                Self::collect_aggregates(child, out);
            }
        }
    }

    /// Describe an aggregate call for AggStep.
    fn describe_aggregate(call: Expr) -> SqawkResult<Agg> {
        let Expr::Function(func) = &call else {
            unreachable!("collect_aggregates only collects function calls")
        };
        let func_type = Self::agg_func_type_for(func);
        let arg = match func_args(func).first() {
            Some(FunctionArg::Unnamed(FunctionArgExpr::Wildcard)) => None,
            Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(e))) => Some(e.clone()),
            _ => {
                return Err(SqawkError::InvalidSqlQuery(format!(
                    "Unsupported aggregate argument: {}",
                    call
                )))
            }
        };
        Ok(Agg {
            call,
            func_type,
            arg,
        })
    }

    /// Compile a SELECT with aggregates and/or GROUP BY.
    pub(crate) fn compile_select_grouped(
        &mut self,
        select: &Select,
        plan: &FromPlan<'a>,
    ) -> SqawkResult<()> {
        let ctx = plan.ctx();
        let multi = plan.srcs.len() > 1;

        let mut items: Vec<(&Expr, String)> = Vec::with_capacity(select.projection.len());
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(e) => items.push((e, self.output_name(e, multi))),
                SelectItem::ExprWithAlias { expr, alias } => {
                    items.push((expr, alias.value.clone()))
                }
                _ => {
                    return Err(SqawkError::UnsupportedSqlFeature(
                        "Only expressions are supported in a GROUP BY or aggregate projection"
                            .into(),
                    ))
                }
            }
        }

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
                    if pos == 0 || pos > items.len() {
                        return Err(SqawkError::InvalidSqlQuery(format!(
                            "GROUP BY position {} is out of range (1..{})",
                            n,
                            items.len()
                        )));
                    }
                    items[pos - 1].0.clone()
                }
                Expr::Identifier(ident) if ctx.resolve_unqualified(&ident.value).is_err() => {
                    match select.projection.iter().find_map(|item| match item {
                        SelectItem::ExprWithAlias { expr, alias }
                            if alias.value.eq_ignore_ascii_case(&ident.value) =>
                        {
                            Some(expr)
                        }
                        _ => None,
                    }) {
                        Some(expr) => expr.clone(),
                        None => g.clone(),
                    }
                }
                _ => g.clone(),
            };
            keys.push(key);
        }

        let mut calls = Vec::new();
        for (e, _) in &items {
            Self::collect_aggregates(e, &mut calls);
        }
        if let Some(h) = &select.having {
            Self::collect_aggregates(h, &mut calls);
        }
        let aggs = calls
            .into_iter()
            .map(Self::describe_aggregate)
            .collect::<SqawkResult<Vec<_>>>()?;

        let mut schema = ResultSchema::new();
        for (e, name) in &items {
            schema.add_column(name.clone(), self.infer_expr_type(e, &ctx));
        }
        self.program.set_result_schema(schema);

        let nk = keys.len();
        let na = aggs.len();

        if nk == 0 {
            // The whole input is one group, which exists even when empty.
            self.add_comment("Aggregate query (no GROUP BY)");
            let acc = self.allocate_registers(na.max(1));
            self.emit_scan(plan, &mut |c, ctx| {
                for (j, agg) in aggs.iter().enumerate() {
                    // -1: COUNT(*), which counts rows rather than values.
                    let val = match &agg.arg {
                        Some(e) => c.code_expr(e, ctx, None)?,
                        None => -1,
                    };
                    c.emit(
                        OpCode::AggStep,
                        agg.func_type,
                        val,
                        acc + j as i64,
                        None,
                        "",
                    );
                }
                Ok(())
            })?;
            return self.emit_group_output(plan, select, &items, &keys, -1, &aggs, acc);
        }

        self.add_comment("GROUP BY query");
        let sorter_id = 0i64;
        let total = nk + na;
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

        // Sorter row: [keys.., aggregate arguments..].
        let row = self.allocate_registers(total);
        self.emit_scan(plan, &mut |c, ctx| {
            for (i, k) in keys.iter().enumerate() {
                c.code_expr(k, ctx, Some(row + i as i64))?;
            }
            for (j, agg) in aggs.iter().enumerate() {
                let dest = row + (nk + j) as i64;
                match &agg.arg {
                    Some(e) => {
                        c.code_expr(e, ctx, Some(dest))?;
                    }
                    // COUNT(*): a non-NULL placeholder, counted once per row.
                    None => c.emit(OpCode::Integer, 1, dest, 0, None, "COUNT(*)"),
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
        self.emit_group_output(plan, select, &items, &keys, key_reg, &aggs, acc)?;
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
        for (j, agg) in aggs.iter().enumerate() {
            self.emit(
                OpCode::AggStep,
                agg.func_type,
                row + (nk + j) as i64,
                acc + j as i64,
                None,
                "",
            );
        }
        self.emit_jump_to(OpCode::SorterNext, sorter_id, top, 0, None, "");
        // The last group, which no key change ever flushed.
        self.emit_group_output(plan, select, &items, &keys, key_reg, &aggs, acc)?;
        self.resolve(done);
        Ok(())
    }

    /// Emit one output row of a grouped query: finalize the aggregates, test
    /// HAVING, compute the SELECT list, ResultRow.
    ///
    /// Called from both flush points of the GROUP BY drain loop, and once for
    /// an aggregate query without GROUP BY.
    #[allow(clippy::too_many_arguments)]
    fn emit_group_output(
        &mut self,
        plan: &FromPlan<'a>,
        select: &Select,
        items: &[(&Expr, String)],
        keys: &[Expr],
        key_reg: i64,
        aggs: &[Agg],
        acc: i64,
    ) -> SqawkResult<()> {
        let fin = self.allocate_registers(aggs.len().max(1));
        for j in 0..aggs.len() {
            self.emit(
                OpCode::AggFinal,
                acc + j as i64,
                fin + j as i64,
                0,
                None,
                "",
            );
        }

        // Post-aggregation scope: aggregates and keys are values in
        // registers; any other column reference is an error.
        let mut ctx = plan.ctx();
        for (j, agg) in aggs.iter().enumerate() {
            ctx.agg_bindings
                .push((NameCtx::agg_key(&agg.call), fin + j as i64));
        }
        let mut cols = Vec::new();
        for (i, k) in keys.iter().enumerate() {
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

        let skip = self.label();
        if let Some(having) = &select.having {
            let r = self.code_expr(having, &ctx, None)?;
            self.emit_jump_to(OpCode::IfZ, r, skip, 0, None, "HAVING failed");
        }
        let out = self.allocate_registers(items.len().max(1));
        for (i, (e, _)) in items.iter().enumerate() {
            self.code_expr(e, &ctx, Some(out + i as i64))?;
        }
        self.emit(OpCode::ResultRow, out, items.len() as i64, 0, None, "");
        self.resolve(skip);
        Ok(())
    }
}
