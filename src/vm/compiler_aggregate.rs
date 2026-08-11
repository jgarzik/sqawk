//! Aggregate function and GROUP BY compilation
//!
//! This module extends SqlCompiler with aggregate and GROUP BY compilation methods.

use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, Select, SelectItem};

use super::bytecode::{OpCode, ResultSchema, AGG_DISTINCT};
use super::compiler::{NameCtx, SqlCompiler};
use crate::error::{SqawkError, SqawkResult};
use crate::table::Table;
use super::ast_compat::{func_args, func_is_distinct, group_by_exprs};

/// One column of a grouped result, in SELECT-list order.
#[derive(Clone, Copy, Debug)]
enum GroupOut {
    /// The n-th GROUP BY key.
    Key(usize),
    /// The n-th aggregate.
    Agg(usize),
}

impl<'a> SqlCompiler<'a> {
    /// Collect every aggregate call within `expr`, in order of appearance.
    ///
    /// Detection used to look only at the top level, so `SUM(salary) + 1` was
    /// not recognised as an aggregate query at all and fell through to the
    /// plain table-scan path, where SUM is not a known scalar function.
    fn collect_aggregates<'e>(expr: &'e Expr, out: &mut Vec<&'e Expr>) {
        if Self::is_aggregate_call(expr) {
            out.push(expr);
            return;
        }
        match expr {
            Expr::BinaryOp { left, right, .. } => {
                Self::collect_aggregates(left, out);
                Self::collect_aggregates(right, out);
            }
            Expr::UnaryOp { expr: inner, .. }
            | Expr::Nested(inner)
            | Expr::Cast { expr: inner, .. } => Self::collect_aggregates(inner, out),
            _ => {}
        }
    }

    pub(crate) fn compile_select_with_aggregate(
        &mut self,
        select: &Select,
        table: &Table,
        table_name: &str,
    ) -> SqawkResult<()> {
        let cursor_idx = 0i64;

        // Build result schema with column names and types
        let schema = self.build_aggregate_result_schema(&select.projection, table);
        self.program.set_result_schema(schema);

        self.add_comment("Aggregate query (no GROUP BY)");

        // Open table
        self.emit(
            OpCode::OpenRead,
            cursor_idx,
            1,
            0,
            Some(table_name.to_string()),
            "",
        );

        // Rewind
        let rewind_addr = self.program.len();
        self.emit(OpCode::Rewind, cursor_idx, 0, 0, None, "");

        let loop_start = self.program.len();

        // Track where we'll need to jump to skip this row (for WHERE filtering)
        let mut where_skip_addr: Option<usize> = None;

        // WHERE clause filtering - must be evaluated BEFORE aggregates
        // If WHERE condition is false, skip to Next
        if let Some(where_expr) = &select.selection {
            let cond_reg = self.compile_where_condition(where_expr, table, cursor_idx as usize)?;
            // If condition is false (0), jump past the AggStep calls
            where_skip_addr = Some(self.program.len());
            self.emit(
                OpCode::IfZ,
                cond_reg,
                0,
                0,
                None,
                "Skip row if WHERE condition is false",
            );
        }

        // Gather every aggregate appearing anywhere in the projection, then
        // step each one per row.
        let mut agg_calls: Vec<&Expr> = Vec::new();
        for item in &select.projection {
            if let SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } = item {
                Self::collect_aggregates(expr, &mut agg_calls);
            }
        }

        let mut acc_regs = Vec::new();
        for call in &agg_calls {
            if let Some((func_type, col_reg)) =
                self.compile_aggregate_step(call, table, cursor_idx)?
            {
                let acc_reg = self.allocate_register();
                acc_regs.push(acc_reg);
                self.emit(OpCode::AggStep, func_type, col_reg, acc_reg, None, "");
            }
        }

        // Next row
        let next_addr = self.program.len();
        self.emit(OpCode::Next, cursor_idx, loop_start as i64, 0, None, "");

        // Patch the WHERE skip jump to point to Next
        if let Some(skip_addr) = where_skip_addr {
            if let Some(inst) = self.program.instructions.get_mut(skip_addr) {
                inst.p2 = next_addr as i64;
            }
        }

        let after_loop = self.program.len();

        // Patch rewind jump
        if let Some(inst) = self.program.instructions.get_mut(rewind_addr) {
            inst.p2 = after_loop as i64;
        }

        // Finalize each aggregate into its own register.
        let agg_start_reg = self.allocate_registers(acc_regs.len().max(1));
        for (i, acc_reg) in acc_regs.iter().enumerate() {
            self.emit(
                OpCode::AggFinal,
                *acc_reg,
                agg_start_reg + i as i64,
                0,
                None,
                "",
            );
        }

        // Bind them by call text, then compile each projection item as an
        // ordinary expression. That is what makes `SUM(salary) + 1` work: the
        // arithmetic is compiled by the shared expression compiler, which
        // resolves the inner SUM to its finalized register.
        let mut ctx = NameCtx::single(table, cursor_idx);
        for (i, call) in agg_calls.iter().enumerate() {
            ctx.agg_bindings
                .push((NameCtx::agg_key(call), agg_start_reg + i as i64));
        }

        let out_items: Vec<&Expr> = select
            .projection
            .iter()
            .filter_map(|item| match item {
                SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => Some(e),
                _ => None,
            })
            .collect();

        let result_start_reg = self.allocate_registers(out_items.len().max(1));
        for (i, expr) in out_items.iter().enumerate() {
            self.code_expr(expr, &ctx, Some(result_start_reg + i as i64))?;
        }

        // Output result row
        self.emit(
            OpCode::ResultRow,
            result_start_reg,
            out_items.len() as i64,
            0,
            None,
            "",
        );

        // Close cursor
        self.emit(OpCode::Close, cursor_idx, 0, 0, None, "");

        Ok(())
    }

    /// Compile an aggregate function step and return (func_type, value_reg)
    pub(crate) fn compile_aggregate_step(
        &mut self,
        expr: &Expr,
        table: &Table,
        cursor_idx: i64,
    ) -> SqawkResult<Option<(i64, i64)>> {
        match expr {
            Expr::Function(func) => {
                let name = func.name.to_string().to_uppercase();
                // DISTINCT rides along as a flag bit on the function type.
                // It used to be dropped entirely -- func.distinct was never
                // read -- so COUNT(DISTINCT department) counted rows.
                let func_type = Self::agg_func_type(&name)
                    | if func_is_distinct(func) { AGG_DISTINCT } else { 0 };

                // Check for COUNT(*)
                if let Some(FunctionArg::Unnamed(FunctionArgExpr::Wildcard)) = func_args(func).first() {
                    // COUNT(*) - use -1 to indicate no column
                    return Ok(Some((func_type, -1)));
                }

                // Compile the argument as a full expression.
                //
                // This used to call resolve_column_expr, which yields a column
                // INDEX and so only accepts a bare column reference --
                // `SUM(salary + age)` was rejected with "Only column
                // references supported in SELECT". An aggregate argument is
                // an ordinary expression evaluated once per row.
                if let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(arg_expr))) =
                    func_args(func).first()
                {
                    let ctx = NameCtx::single(table, cursor_idx);
                    let col_reg = self.code_expr(arg_expr, &ctx, None)?;
                    return Ok(Some((func_type, col_reg)));
                }

                Ok(None)
            }
            _ => Ok(None),
        }
    }

    /// Build result schema for aggregate queries
    pub(crate) fn build_aggregate_result_schema(
        &self,
        projection: &[SelectItem],
        table: &Table,
    ) -> ResultSchema {
        let mut schema = ResultSchema::new();
        for item in projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let name = self.get_column_name_from_expr(expr);
                    let data_type = self.infer_expr_type(expr, table);
                    schema.add_column(name, data_type);
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let data_type = self.infer_expr_type(expr, table);
                    schema.add_column(alias.value.clone(), data_type);
                }
                _ => {}
            }
        }
        schema
    }


    /// Emit one grouped output row: keys, finalized aggregates, HAVING, ResultRow.
    ///
    /// Written once and called from both flush points -- the group-change flush
    /// inside the drain loop and the final flush after it. Those were
    /// copy-pasted duplicates, which is how HAVING came to be applied slightly
    /// differently in each.
    ///
    /// Values are emitted in PROJECTION order. The result block used to be laid
    /// out positionally as [group keys.., aggregates..] while the schema was
    /// built from the SELECT list, so `SELECT COUNT(*), department ... GROUP BY
    /// department` printed the header `COUNT,department` above the values
    /// `Engineering,3`. Emitting through out_plan makes header and value order
    /// the same by construction, and means a group key that is not projected
    /// contributes no column instead of a phantom `col1`.
    #[allow(clippy::too_many_arguments)]
    fn emit_group_flush(
        &mut self,
        select: &Select,
        table: &Table,
        out_plan: &[GroupOut],
        group_col_indices: &[usize],
        agg_info: &[(i64, Option<usize>)],
        group_key_reg: i64,
        acc_base_reg: i64,
        result_start_reg: i64,
    ) -> SqawkResult<()> {
        // Finalize every accumulator into its slot in the internal
        // [keys.., aggs..] block, which HAVING resolves against.
        for i in 0..agg_info.len() {
            self.emit(
                OpCode::AggFinal,
                acc_base_reg + i as i64,
                result_start_reg + group_col_indices.len() as i64 + i as i64,
                0,
                None,
                "",
            );
        }
        for i in 0..group_col_indices.len() {
            self.emit(
                OpCode::Copy,
                group_key_reg + i as i64,
                result_start_reg + i as i64,
                0,
                None,
                "",
            );
        }

        // HAVING gates the row, and is evaluated against the internal block.
        let after_row = self.label();
        if let Some(having_expr) = &select.having {
            let having_reg = self.compile_having_condition(
                having_expr,
                table,
                group_col_indices,
                agg_info,
                result_start_reg,
                group_key_reg,
            )?;
            self.emit_jump_to(OpCode::IfZ, having_reg, after_row, 0, None, "HAVING failed");
        }

        // Gather into a contiguous block in projection order and emit.
        let out_start = self.allocate_registers(out_plan.len().max(1));
        for (i, slot) in out_plan.iter().enumerate() {
            let src = match slot {
                GroupOut::Key(k) => result_start_reg + *k as i64,
                GroupOut::Agg(a) => result_start_reg + group_col_indices.len() as i64 + *a as i64,
            };
            self.emit(OpCode::Copy, src, out_start + i as i64, 0, None, "");
        }
        self.emit(
            OpCode::ResultRow,
            out_start,
            out_plan.len() as i64,
            0,
            None,
            "",
        );

        self.resolve(after_row);
        Ok(())
    }

    /// Compile a SELECT with GROUP BY
    pub(crate) fn compile_select_with_group_by(
        &mut self,
        select: &Select,
        table: &Table,
        table_name: &str,
    ) -> SqawkResult<()> {
        let cursor_idx = 0i64;
        let sorter_id = 0i64;

        // Determine GROUP BY column indices
        let mut group_col_indices: Vec<usize> = Vec::new();
        for expr in group_by_exprs(&select.group_by).iter() {
            match expr {
                Expr::Identifier(ident) => {
                    let col_name = ident.value.to_lowercase();
                    let col_idx = table
                        .column_index(&col_name)
                        .ok_or(SqawkError::ColumnNotFound(col_name))?;
                    group_col_indices.push(col_idx);
                }
                Expr::CompoundIdentifier(parts) => {
                    let col_name = parts
                        .last()
                        .map(|p| p.value.to_lowercase())
                        .unwrap_or_default();
                    let col_idx = table
                        .column_index(&col_name)
                        .ok_or(SqawkError::ColumnNotFound(col_name))?;
                    group_col_indices.push(col_idx);
                }
                _ => {
                    return Err(SqawkError::UnsupportedSqlFeature(
                        "Only column references supported in GROUP BY".into(),
                    ));
                }
            }
        }

        // Determine aggregates in projection
        let mut agg_info: Vec<(i64, Option<usize>)> = Vec::new(); // (func_type, col_idx or None for COUNT(*))
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    if let Expr::Function(func) = expr {
                        let name = func.name.to_string().to_uppercase();
                        if matches!(name.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX") {
                            let func_type = Self::agg_func_type(&name);
                            if let Some(FunctionArg::Unnamed(FunctionArgExpr::Wildcard)) =
                                func_args(func).first()
                            {
                                agg_info.push((func_type, None));
                            } else if let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                arg_expr,
                            ))) = func_args(func).first()
                            {
                                let col_idx = self.resolve_column_expr(arg_expr, table)?;
                                agg_info.push((func_type, Some(col_idx)));
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        // Map each projected item to the group key or aggregate it names, in
        // SELECT-list order. Emitting through this is what keeps the values
        // aligned with the header the schema builder produces.
        let mut out_plan: Vec<GroupOut> = Vec::with_capacity(select.projection.len());
        let mut agg_seen = 0usize;
        for item in &select.projection {
            let expr = match item {
                SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => e,
                _ => {
                    return Err(SqawkError::UnsupportedSqlFeature(
                        "Only expressions are supported in a GROUP BY projection".into(),
                    ))
                }
            };
            if Self::is_aggregate_call(expr) {
                out_plan.push(GroupOut::Agg(agg_seen));
                agg_seen += 1;
                continue;
            }
            // Otherwise it must name a GROUP BY key: in a grouped query no
            // other column has a single well-defined value per row.
            let col_idx = self.resolve_column_expr(expr, table).map_err(|_| {
                SqawkError::InvalidSqlQuery(format!(
                    "'{}' must appear in GROUP BY or be used in an aggregate",
                    expr
                ))
            })?;
            let key_pos = group_col_indices
                .iter()
                .position(|c| *c == col_idx)
                .ok_or_else(|| {
                    SqawkError::InvalidSqlQuery(format!(
                        "'{}' must appear in GROUP BY or be used in an aggregate",
                        expr
                    ))
                })?;
            out_plan.push(GroupOut::Key(key_pos));
        }

        // Build result schema with column names and types
        let schema = self.build_aggregate_result_schema(&select.projection, table);
        self.program.set_result_schema(schema);

        self.add_comment("GROUP BY query");

        // Build sort spec for sorting by GROUP BY columns
        // Use position within sorter (0, 1, 2, ...) not original table column indices
        let sort_spec: String = (0..group_col_indices.len())
            .map(|i| format!("{}:asc", i))
            .collect::<Vec<_>>()
            .join(",");

        // Total columns to store: group columns + aggregate value columns
        let total_cols = group_col_indices.len() + agg_info.len();

        // Open sorter
        self.emit(
            OpCode::SorterOpen,
            sorter_id,
            total_cols as i64,
            0,
            Some(sort_spec),
            "",
        );

        // Open table
        self.emit(
            OpCode::OpenRead,
            cursor_idx,
            1,
            0,
            Some(table_name.to_string()),
            "",
        );

        // Rewind
        let rewind_addr = self.program.len();
        self.emit(OpCode::Rewind, cursor_idx, 0, 0, None, "");

        let loop_start = self.program.len();

        // WHERE filtering.
        //
        // This function never read select.selection, so WHERE was silently
        // ignored on EVERY grouped query: `... WHERE salary > 60000 GROUP BY
        // department` returned all departments with unfiltered counts. Filter
        // before feeding the sorter so excluded rows never reach an
        // accumulator.
        let next_label = self.label();
        if let Some(where_expr) = &select.selection {
            let cond_reg = self.compile_where_condition(where_expr, table, cursor_idx as usize)?;
            self.emit_jump_to(
                OpCode::IfZ,
                cond_reg,
                next_label,
                0,
                None,
                "Skip row if WHERE is false",
            );
        }

        // Allocate registers for row data
        let row_start_reg = self.allocate_registers(total_cols);

        // Load GROUP BY columns
        for (i, &col_idx) in group_col_indices.iter().enumerate() {
            self.emit(
                OpCode::Column,
                cursor_idx,
                col_idx as i64,
                row_start_reg + i as i64,
                None,
                "",
            );
        }

        // Load aggregate value columns
        for (i, (_, col_idx_opt)) in agg_info.iter().enumerate() {
            let dest_reg = row_start_reg + group_col_indices.len() as i64 + i as i64;
            if let Some(col_idx) = col_idx_opt {
                self.emit(
                    OpCode::Column,
                    cursor_idx,
                    *col_idx as i64,
                    dest_reg,
                    None,
                    "",
                );
            } else {
                // COUNT(*) - just use a constant 1
                self.emit(OpCode::Integer, 1, dest_reg, 0, None, "");
            }
        }

        // Insert into sorter
        self.emit(
            OpCode::SorterInsert,
            sorter_id,
            row_start_reg,
            total_cols as i64,
            None,
            "",
        );

        // Next row
        self.resolve(next_label);
        self.emit(OpCode::Next, cursor_idx, loop_start as i64, 0, None, "");

        let after_scan = self.program.len();

        // Patch rewind
        if let Some(inst) = self.program.instructions.get_mut(rewind_addr) {
            inst.p2 = after_scan as i64;
        }

        // Sort the sorter
        self.emit(OpCode::SorterSort, sorter_id, 0, 0, None, "");

        // Now iterate through sorted rows, grouping by GROUP BY columns
        // For each group, accumulate aggregates and output when group changes

        // Allocate registers for current group key
        let group_key_reg = self.allocate_registers(group_col_indices.len());

        // Allocate accumulator registers
        let acc_base_reg = self.allocate_registers(agg_info.len());

        // Initialize group key with NULL (first group detection)
        for i in 0..group_col_indices.len() {
            self.emit(OpCode::Null, 0, group_key_reg + i as i64, 0, None, "");
        }

        // Flag to indicate we've seen at least one row
        let first_row_reg = self.allocate_register();
        self.emit(OpCode::Integer, 1, first_row_reg, 0, None, "");

        // Allocate result registers upfront
        let result_start_reg = self.allocate_registers(group_col_indices.len() + agg_info.len());

        // Start group iteration
        let sorter_loop_start = self.program.len();

        // Get row from sorter
        self.emit(
            OpCode::SorterData,
            sorter_id,
            row_start_reg,
            total_cols as i64,
            None,
            "",
        );

        // Check if first row - if so, skip group change check
        let first_row_jump_addr = self.program.len();
        self.emit(OpCode::IfPos, first_row_reg, 0, 0, None, "");

        // Compare ALL group key columns against the saved key.
        //
        // This was a single `Ne` against column 0, so `GROUP BY department,
        // role` grouped on department alone and reported whichever role
        // happened to arrive first. Compare/Jump handles the whole key vector
        // in two instructions regardless of width, and treats NULL as equal to
        // NULL so NULL keys group together rather than starting a new group on
        // every row.
        self.emit(
            OpCode::Compare,
            row_start_reg,
            group_key_reg,
            group_col_indices.len() as i64,
            None,
            "Compare group key vector",
        );
        // Equal -> same group, skip the flush. Less/Greater -> group changed.
        let flush_label = self.label();
        let skip_output_label = self.label();
        self.emit_jump_to_three(
            flush_label,
            skip_output_label,
            flush_label,
            "Group changed?",
        );
        self.resolve(flush_label);

        // Group changed - output the group just completed.
        self.emit_group_flush(
            select,
            table,
            &out_plan,
            &group_col_indices,
            &agg_info,
            group_key_reg,
            acc_base_reg,
            result_start_reg,
        )?;

        // Reset accumulators for new group
        for (i, _) in agg_info.iter().enumerate() {
            self.emit(OpCode::AggReset, acc_base_reg + i as i64, 0, 0, None, "");
        }

        // === Update group key section (first row jumps here) ===
        let update_group_key_addr = self.program.len();

        // Patch first row jump
        if let Some(inst) = self.program.instructions.get_mut(first_row_jump_addr) {
            inst.p2 = update_group_key_addr as i64;
        }

        // Initialize/update group key from current row
        for i in 0..group_col_indices.len() {
            self.emit(
                OpCode::Copy,
                row_start_reg + i as i64,
                group_key_reg + i as i64,
                0,
                None,
                "",
            );
        }

        // Clear first row flag
        self.emit(OpCode::Integer, 0, first_row_reg, 0, None, "");

        // === Step aggregates section (same-group rows jump here) ===
        self.resolve(skip_output_label);

        // Step aggregates for current row
        for (i, (func_type, _)) in agg_info.iter().enumerate() {
            let value_reg = row_start_reg + group_col_indices.len() as i64 + i as i64;
            self.emit(
                OpCode::AggStep,
                *func_type,
                value_reg,
                acc_base_reg + i as i64,
                None,
                "",
            );
        }

        // Next sorted row
        self.emit(
            OpCode::SorterNext,
            sorter_id,
            sorter_loop_start as i64,
            0,
            None,
            "",
        );

        // Output the final group, which no group-change ever flushed.
        self.emit_group_flush(
            select,
            table,
            &out_plan,
            &group_col_indices,
            &agg_info,
            group_key_reg,
            acc_base_reg,
            result_start_reg,
        )?;

        // Close cursor
        self.emit(OpCode::Close, cursor_idx, 0, 0, None, "");

        Ok(())
    }

    /// Compile a HAVING condition and return the register containing the result (1 or 0)
    /// agg_result_regs maps aggregate function signatures to their result registers
    #[allow(clippy::too_many_arguments)]
    fn compile_having_condition(
        &mut self,
        having_expr: &Expr,
        table: &Table,
        group_col_indices: &[usize],
        agg_info: &[(i64, Option<usize>)],
        result_start_reg: i64,
        group_key_reg: i64,
    ) -> SqawkResult<i64> {
        match having_expr {
            Expr::BinaryOp { left, op, right } => {
                let left_reg = self.compile_having_operand(
                    left,
                    table,
                    group_col_indices,
                    agg_info,
                    result_start_reg,
                    group_key_reg,
                )?;
                let right_reg = self.compile_having_operand(
                    right,
                    table,
                    group_col_indices,
                    agg_info,
                    result_start_reg,
                    group_key_reg,
                )?;
                self.emit_comparison(op, left_reg, right_reg)
            }
            _ => Err(SqawkError::UnsupportedSqlFeature(format!(
                "Unsupported HAVING expression: {:?}",
                having_expr
            ))),
        }
    }

    /// Compile an operand in a HAVING condition
    #[allow(clippy::too_many_arguments)]
    fn compile_having_operand(
        &mut self,
        expr: &Expr,
        table: &Table,
        group_col_indices: &[usize],
        agg_info: &[(i64, Option<usize>)],
        result_start_reg: i64,
        group_key_reg: i64,
    ) -> SqawkResult<i64> {
        match expr {
            Expr::Function(func) => {
                // Match the aggregate on BOTH its function type and its
                // argument column.
                //
                // This used to compare func_type alone, so with two aggregates
                // of the same kind -- `SELECT department, SUM(salary),
                // SUM(age) ... HAVING SUM(age) > 60` -- HAVING bound to
                // SUM(salary), whichever appeared first, and filtered on
                // entirely the wrong column. MIN vs MAX bound correctly, which
                // is why it stayed hidden.
                let name = func.name.to_string().to_uppercase();
                let func_type = Self::agg_func_type(&name)
                    | if func_is_distinct(func) { AGG_DISTINCT } else { 0 };

                let wanted_col: Option<usize> =
                    match func_args(func).first() {
                        Some(FunctionArg::Unnamed(FunctionArgExpr::Wildcard)) => None,
                        Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(arg_expr))) => {
                            Some(self.resolve_column_expr(arg_expr, table)?)
                        }
                        _ => None,
                    };

                for (i, (agg_func_type, agg_col)) in agg_info.iter().enumerate() {
                    if *agg_func_type == func_type && *agg_col == wanted_col {
                        let agg_result_reg =
                            result_start_reg + group_col_indices.len() as i64 + i as i64;
                        let reg = self.allocate_register();
                        self.emit(OpCode::Copy, agg_result_reg, reg, 0, None, "");
                        return Ok(reg);
                    }
                }

                Err(SqawkError::UnsupportedSqlFeature(format!(
                    "Aggregate in HAVING must also appear in SELECT: {}",
                    expr
                )))
            }
            Expr::Value(_) => {
                // Compile the literal through the shared expression compiler.
                //
                // This arm used to handle Value::Number only, and silently
                // emitted NOTHING for anything else -- so `HAVING department =
                // 'Sales'` compared the group key against an uninitialized
                // register and matched no group.
                self.code_expr(expr, &NameCtx::single(table, 0), None)
            }
            Expr::Identifier(ident) => {
                // A GROUP BY key referenced in HAVING. This used to copy
                // group_key_reg unconditionally -- the FIRST key column --
                // whatever name was written, so any reference to a later key
                // silently filtered on the first one.
                let col_name = ident.value.to_ascii_lowercase();
                let col_idx = table
                    .column_index(&col_name)
                    .ok_or_else(|| SqawkError::ColumnNotFound(col_name.clone()))?;
                let key_pos = group_col_indices
                    .iter()
                    .position(|c| *c == col_idx)
                    .ok_or_else(|| {
                        SqawkError::InvalidSqlQuery(format!(
                            "Column '{}' in HAVING must appear in GROUP BY",
                            ident.value
                        ))
                    })?;
                let reg = self.allocate_register();
                self.emit(
                    OpCode::Copy,
                    group_key_reg + key_pos as i64,
                    reg,
                    0,
                    None,
                    "",
                );
                Ok(reg)
            }
            _ => Err(SqawkError::UnsupportedSqlFeature(format!(
                "Unsupported HAVING operand: {:?}",
                expr
            ))),
        }
    }
}
