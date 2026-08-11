//! Aggregate function and GROUP BY compilation
//!
//! This module extends SqlCompiler with aggregate and GROUP BY compilation methods.

use sqlparser::ast::ValueWithSpan;
use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, Select, SelectItem};

use super::bytecode::{OpCode, ResultSchema};
use super::compiler::SqlCompiler;
use crate::error::{SqawkError, SqawkResult};
use crate::table::Table;
use super::ast_compat::{func_args, group_by_exprs};

impl<'a> SqlCompiler<'a> {
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

        // For each aggregate in the projection, emit AggStep
        let mut acc_regs = Vec::new();
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    if let Some((func_type, col_reg)) =
                        self.compile_aggregate_step(expr, table, cursor_idx)?
                    {
                        let acc_reg = self.allocate_register();
                        acc_regs.push((acc_reg, func_type));
                        self.emit(OpCode::AggStep, func_type, col_reg, acc_reg, None, "");
                    }
                }
                _ => {}
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

        // Finalize aggregates and output result
        let result_start_reg = self.allocate_registers(acc_regs.len());

        for (i, (acc_reg, _)) in acc_regs.iter().enumerate() {
            self.emit(
                OpCode::AggFinal,
                *acc_reg,
                result_start_reg + i as i64,
                0,
                None,
                "",
            );
        }

        // Output result row
        self.emit(
            OpCode::ResultRow,
            result_start_reg,
            acc_regs.len() as i64,
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
                let func_type = Self::agg_func_type(&name);

                // Check for COUNT(*)
                if let Some(FunctionArg::Unnamed(FunctionArgExpr::Wildcard)) = func_args(func).first() {
                    // COUNT(*) - use -1 to indicate no column
                    return Ok(Some((func_type, -1)));
                }

                // Get the column argument
                if let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(arg_expr))) =
                    func_args(func).first()
                {
                    let col_idx = self.resolve_column_expr(arg_expr, table)?;
                    let col_reg = self.allocate_register();
                    self.emit(
                        OpCode::Column,
                        cursor_idx,
                        col_idx as i64,
                        col_reg,
                        None,
                        "",
                    );
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

        // Compare first group column with saved group key
        let cmp_reg = self.allocate_register();
        self.emit(OpCode::Ne, row_start_reg, group_key_reg, cmp_reg, None, "");

        // If group NOT changed (cmp_reg is 0), skip output and go to step aggregates
        let skip_output_addr = self.program.len();
        self.emit(OpCode::IfZ, cmp_reg, 0, 0, None, "");

        // Group changed - output previous group
        // Copy group key to result
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

        // Finalize aggregates
        for (i, _) in agg_info.iter().enumerate() {
            let dest_reg = result_start_reg + group_col_indices.len() as i64 + i as i64;
            self.emit(
                OpCode::AggFinal,
                acc_base_reg + i as i64,
                dest_reg,
                0,
                None,
                "",
            );
        }

        // Apply HAVING filter if present
        let mut having_skip_addr: Option<usize> = None;
        if let Some(having_expr) = &select.having {
            let having_reg = self.compile_having_condition(
                having_expr,
                &group_col_indices,
                &agg_info,
                result_start_reg,
                group_key_reg,
            )?;
            // If HAVING condition is false (0), skip ResultRow
            having_skip_addr = Some(self.program.len());
            self.emit(OpCode::IfZ, having_reg, 0, 0, None, "");
        }

        // Output result row
        self.emit(
            OpCode::ResultRow,
            result_start_reg,
            (group_col_indices.len() + agg_info.len()) as i64,
            0,
            None,
            "",
        );

        // Patch HAVING skip jump if present
        if let Some(addr) = having_skip_addr {
            let after_result = self.program.len();
            if let Some(inst) = self.program.instructions.get_mut(addr) {
                inst.p2 = after_result as i64;
            }
        }

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

        // === Step aggregates section (skip output jumps here) ===
        let step_agg_addr = self.program.len();

        // Patch skip output jump
        if let Some(inst) = self.program.instructions.get_mut(skip_output_addr) {
            inst.p2 = step_agg_addr as i64;
        }

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

        // Output final group
        // Copy group key to result
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

        // Finalize aggregates
        for (i, _) in agg_info.iter().enumerate() {
            let dest_reg = result_start_reg + group_col_indices.len() as i64 + i as i64;
            self.emit(
                OpCode::AggFinal,
                acc_base_reg + i as i64,
                dest_reg,
                0,
                None,
                "",
            );
        }

        // Apply HAVING filter for final group if present
        let mut final_having_skip_addr: Option<usize> = None;
        if let Some(having_expr) = &select.having {
            let having_reg = self.compile_having_condition(
                having_expr,
                &group_col_indices,
                &agg_info,
                result_start_reg,
                group_key_reg,
            )?;
            // If HAVING condition is false (0), skip ResultRow
            final_having_skip_addr = Some(self.program.len());
            self.emit(OpCode::IfZ, having_reg, 0, 0, None, "");
        }

        // Output final result row
        self.emit(
            OpCode::ResultRow,
            result_start_reg,
            (group_col_indices.len() + agg_info.len()) as i64,
            0,
            None,
            "",
        );

        // Patch final HAVING skip jump if present
        if let Some(addr) = final_having_skip_addr {
            let after_result = self.program.len();
            if let Some(inst) = self.program.instructions.get_mut(addr) {
                inst.p2 = after_result as i64;
            }
        }

        // Close cursor
        self.emit(OpCode::Close, cursor_idx, 0, 0, None, "");

        Ok(())
    }

    /// Compile a HAVING condition and return the register containing the result (1 or 0)
    /// agg_result_regs maps aggregate function signatures to their result registers
    fn compile_having_condition(
        &mut self,
        having_expr: &Expr,
        group_col_indices: &[usize],
        agg_info: &[(i64, Option<usize>)],
        result_start_reg: i64,
        group_key_reg: i64,
    ) -> SqawkResult<i64> {
        match having_expr {
            Expr::BinaryOp { left, op, right } => {
                let left_reg = self.compile_having_operand(
                    left,
                    group_col_indices,
                    agg_info,
                    result_start_reg,
                    group_key_reg,
                )?;
                let right_reg = self.compile_having_operand(
                    right,
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
    fn compile_having_operand(
        &mut self,
        expr: &Expr,
        group_col_indices: &[usize],
        agg_info: &[(i64, Option<usize>)],
        result_start_reg: i64,
        group_key_reg: i64,
    ) -> SqawkResult<i64> {
        match expr {
            Expr::Function(func) => {
                // Find the aggregate function in agg_info and return its result register
                let name = func.name.to_string().to_uppercase();
                let func_type = Self::agg_func_type(&name);

                // Find matching aggregate in agg_info by function type
                for (i, (agg_func_type, _)) in agg_info.iter().enumerate() {
                    if *agg_func_type == func_type {
                        // Found matching aggregate - copy its result to a new register
                        let agg_result_reg =
                            result_start_reg + group_col_indices.len() as i64 + i as i64;
                        let reg = self.allocate_register();
                        self.emit(OpCode::Copy, agg_result_reg, reg, 0, None, "");
                        return Ok(reg);
                    }
                }

                Err(SqawkError::UnsupportedSqlFeature(format!(
                    "Aggregate function in HAVING not found in SELECT: {}",
                    name
                )))
            }
            Expr::Value(ValueWithSpan { value, .. }) => {
                let reg = self.allocate_register();
                if let sqlparser::ast::Value::Number(n, _) = value {
                    if let Ok(i) = n.parse::<i64>() {
                        self.emit(OpCode::Integer, i, reg, 0, None, "");
                    }
                }
                Ok(reg)
            }
            Expr::Identifier(_) => {
                // GROUP BY column reference - copy from first group key register
                // (simplified - for multi-column GROUP BY would need column name matching)
                let reg = self.allocate_register();
                self.emit(OpCode::Copy, group_key_reg, reg, 0, None, "");
                Ok(reg)
            }
            _ => Err(SqawkError::UnsupportedSqlFeature(format!(
                "Unsupported HAVING operand: {:?}",
                expr
            ))),
        }
    }
}
