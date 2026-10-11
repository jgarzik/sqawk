//! Window function compilation
//!
//! This module extends SqlCompiler with window function compilation methods.

use sqlparser::ast::{Expr, Select, SelectItem, Value, ValueWithSpan, WindowType};

use super::ast_compat::{func_args, order_by_is_asc};
use super::bytecode::{OpCode, ResultSchema};
use super::compiler::{NameCtx, SqlCompiler};
use crate::aggregate::AggregateFunction;
use crate::error::{SqawkError, SqawkResult};
use crate::table::{DataType, Table};

impl<'a> SqlCompiler<'a> {
    /// The window stage: compute every output row of a window query into the
    /// ephemeral table `staging`, from which the output stage reads them.
    pub(crate) fn compile_select_with_window(
        &mut self,
        select: &Select,
        table: &'a Table,
        table_name: &str,
        staging: i64,
    ) -> SqawkResult<()> {
        let cursor_idx = 0i64;
        let ephemeral_cursor = 1i64;

        self.add_comment("Window function query");

        // A wildcard in a window query is not expanded by this path: the
        // projection analysis below counts one output column per SELECT item,
        // so `SELECT *, SUM(x) OVER (...)` emitted two columns for a
        // four-column table and lost the rest. Reject it rather than return a
        // silently truncated row.
        if select.projection.iter().any(|i| {
            matches!(
                i,
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..)
            )
        }) {
            return Err(SqawkError::UnsupportedSqlFeature(
                "SELECT * cannot be combined with a window function; list the columns".into(),
            ));
        }

        // Analyze the projection to find window functions and their specs
        let (window_funcs, mut base_columns) =
            self.analyze_window_projection(&select.projection, table)?;

        if window_funcs.is_empty() {
            return Err(SqawkError::InvalidSqlQuery(
                "No window functions found in projection".to_string(),
            ));
        }

        // Get partition and order columns from the first window function (source table indices)
        // (All window functions should have the same OVER clause for now)
        let (partition_cols, order_cols, order_asc) =
            self.extract_window_spec(&window_funcs[0].1, table)?;

        // Add partition/order columns to base_columns if not already present
        for &col_idx in &partition_cols {
            if !base_columns.contains(&col_idx) {
                base_columns.push(col_idx);
            }
        }
        for &col_idx in &order_cols {
            if !base_columns.contains(&col_idx) {
                base_columns.push(col_idx);
            }
        }

        // Map source table indices to ephemeral cursor positions
        let partition_positions: Vec<usize> = partition_cols
            .iter()
            .filter_map(|&src_idx| base_columns.iter().position(|&bc| bc == src_idx))
            .collect();
        let order_positions: Vec<usize> = order_cols
            .iter()
            .filter_map(|&src_idx| base_columns.iter().position(|&bc| bc == src_idx))
            .collect();

        let total_col_count = base_columns.len();

        // Build result schema
        let schema = self.build_window_result_schema(&select.projection, table);
        self.program.set_result_schema(schema);

        // Build sort spec: partition columns first, then order columns (using ephemeral positions)
        let mut sort_spec_parts = Vec::new();
        for &pos in &partition_positions {
            sort_spec_parts.push(format!("{}:asc", pos));
        }
        for (i, &pos) in order_positions.iter().enumerate() {
            let dir = if order_asc.get(i).copied().unwrap_or(true) {
                "asc"
            } else {
                "desc"
            };
            sort_spec_parts.push(format!("{}:{}", pos, dir));
        }
        let sort_spec = sort_spec_parts.join(",");

        // Open ephemeral cursor for collecting rows
        self.emit(
            OpCode::OpenEphemeral,
            ephemeral_cursor,
            total_col_count as i64,
            0,
            Some(sort_spec),
            "Open ephemeral for window function",
        );

        // Open source table
        self.emit(
            OpCode::OpenRead,
            cursor_idx,
            1,
            0,
            Some(table_name.to_string()),
            "",
        );

        // Rewind source table
        let rewind_addr = self.program.len();
        self.emit(OpCode::Rewind, cursor_idx, 0, 0, None, "");

        let collect_loop_start = self.program.len();

        // Allocate registers for base columns
        let start_reg = self.allocate_registers(total_col_count);

        // Load all base columns
        for (i, &col_idx) in base_columns.iter().enumerate() {
            self.emit(
                OpCode::Column,
                cursor_idx,
                col_idx as i64,
                start_reg + i as i64,
                None,
                "",
            );
        }

        // Apply WHERE filter if present
        if let Some(where_expr) = &select.selection {
            let cond_reg = self.code_expr(where_expr, &NameCtx::single(table, cursor_idx), None)?;
            self.emit(
                OpCode::IfZ,
                cond_reg,
                (self.program.len() + 2) as i64,
                0,
                None,
                "",
            );
        }

        // Insert row into ephemeral
        self.emit(
            OpCode::IdxInsert,
            ephemeral_cursor,
            start_reg,
            total_col_count as i64,
            None,
            "",
        );

        // Next row in source
        self.emit(
            OpCode::Next,
            cursor_idx,
            collect_loop_start as i64,
            0,
            None,
            "",
        );

        let after_collect = self.program.len();

        // Patch rewind
        if let Some(inst) = self.program.instructions.get_mut(rewind_addr) {
            inst.p2 = after_collect as i64;
        }

        // Sort the ephemeral (by partition + order keys)
        self.emit(OpCode::Sort, ephemeral_cursor, 0, 0, None, "");

        let sort_jump_target = self.program.len();

        // Rewind ephemeral for output pass
        let output_rewind_addr = self.program.len();
        self.emit(OpCode::Rewind, ephemeral_cursor, 0, 0, None, "");

        // Allocate registers for output row
        let output_col_count = select.projection.len();
        let output_start_reg = self.allocate_registers(output_col_count);

        // Allocate registers for window state
        let row_num_reg = self.allocate_register(); // Current row number in partition
        let prev_partition_reg = self.allocate_register(); // Previous partition key value
        let rank_reg = self.allocate_register(); // Current rank value
        let prev_order_reg = self.allocate_register(); // Previous order key value for rank
        let dense_rank_reg = self.allocate_register(); // Dense rank value
        let rows_at_rank_reg = self.allocate_register(); // Number of rows at current rank

        // Initialize window state BEFORE the loop
        self.emit(
            OpCode::Integer,
            0,
            row_num_reg,
            0,
            None,
            "Initialize row number",
        );
        self.emit(
            OpCode::Null,
            0,
            prev_partition_reg,
            0,
            None,
            "Initialize prev partition key",
        );
        self.emit(OpCode::Integer, 0, rank_reg, 0, None, "Initialize rank");
        self.emit(
            OpCode::Null,
            0,
            prev_order_reg,
            0,
            None,
            "Initialize prev order key",
        );
        self.emit(
            OpCode::Integer,
            0,
            dense_rank_reg,
            0,
            None,
            "Initialize dense rank",
        );
        self.emit(
            OpCode::Integer,
            0,
            rows_at_rank_reg,
            0,
            None,
            "Initialize rows at rank",
        );

        // Allocate registers for aggregate window functions (SUM, AVG, COUNT, MIN, MAX)
        let agg_sum_reg = self.allocate_register(); // Running sum
        let agg_count_reg = self.allocate_register(); // Running count
        let agg_min_reg = self.allocate_register(); // Running min
        let agg_max_reg = self.allocate_register(); // Running max

        // Initialize aggregate state
        self.emit(
            OpCode::Integer,
            0,
            agg_sum_reg,
            0,
            None,
            "Initialize aggregate sum",
        );
        self.emit(
            OpCode::Integer,
            0,
            agg_count_reg,
            0,
            None,
            "Initialize aggregate count",
        );
        self.emit(
            OpCode::Null,
            0,
            agg_min_reg,
            0,
            None,
            "Initialize aggregate min",
        );
        self.emit(
            OpCode::Null,
            0,
            agg_max_reg,
            0,
            None,
            "Initialize aggregate max",
        );

        // Find if we have aggregate window functions and extract their column positions
        let mut agg_col_pos: Option<usize> = None;
        for (func_name, _) in &window_funcs {
            let ft = self.get_window_func_type(func_name);
            if (5..=9).contains(&ft) {
                // It's an aggregate function - find the column position
                // Look through projection to find the argument
                for item in &select.projection {
                    if let SelectItem::UnnamedExpr(Expr::Function(func))
                    | SelectItem::ExprWithAlias {
                        expr: Expr::Function(func),
                        ..
                    } = item
                    {
                        if func.name.to_string().to_uppercase() == *func_name && func.over.is_some()
                        {
                            if !func_args(func).is_empty() {
                                if let Ok(Expr::Identifier(ident)) =
                                    self.extract_function_arg_expr(&func_args(func)[0])
                                {
                                    if let Some(src_idx) = table.column_index(&ident.value) {
                                        agg_col_pos =
                                            base_columns.iter().position(|&c| c == src_idx);
                                    }
                                }
                            }
                            break;
                        }
                    }
                }
                break;
            }
        }

        // WindowAggStep computes window values - we encode state registers in P4
        let window_state_spec = format!(
            "row_num:{},prev_part:{},rank:{},prev_ord:{},dense_rank:{},rows_at_rank:{},agg_sum:{},agg_count:{},agg_min:{},agg_max:{},agg_col:{}",
            row_num_reg, prev_partition_reg, rank_reg, prev_order_reg, dense_rank_reg, rows_at_rank_reg,
            agg_sum_reg, agg_count_reg, agg_min_reg, agg_max_reg, agg_col_pos.unwrap_or(0)
        );

        // Build window spec for WindowAggStep (using ephemeral cursor positions)
        let part_cols_str = partition_positions
            .iter()
            .map(|c| c.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let ord_cols_str = order_positions
            .iter()
            .map(|c| c.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let window_spec_base = format!(
            "WINDOW:part={}:ord={}:state={}",
            part_cols_str, ord_cols_str, window_state_spec
        );

        // NOW start the output loop (after state initialization)
        let output_loop_start = self.program.len();

        // Emit ONE WindowAggStep at start of loop to update state for this row
        // Get the first window function type for the step
        let first_func_type = if !window_funcs.is_empty() {
            self.get_window_func_type(&window_funcs[0].0)
        } else {
            0
        };
        self.emit(
            OpCode::WindowAggStep,
            first_func_type,
            staging,
            ephemeral_cursor,
            Some(window_spec_base.clone()),
            "Update window state for current row",
        );

        // Compile the output projection
        let mut output_idx = 0;
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    match expr {
                        Expr::Function(func) if func.over.is_some() => {
                            // Window function - just emit WindowValue (state already updated above)
                            let func_name = func.name.to_string().to_uppercase();
                            let func_type = self.get_window_func_type(&func_name);

                            // For LAG/LEAD, extract column position, offset, and partition info
                            let p4 = if func_name == "LAG" || func_name == "LEAD" {
                                // LAG/LEAD(column, offset, default)
                                // Get column position in ephemeral cursor
                                let col_pos = if !func_args(func).is_empty() {
                                    if let Ok(Expr::Identifier(ident)) =
                                        self.extract_function_arg_expr(&func_args(func)[0])
                                    {
                                        if let Some(src_idx) = table.column_index(&ident.value) {
                                            base_columns
                                                .iter()
                                                .position(|&c| c == src_idx)
                                                .unwrap_or(0)
                                        } else {
                                            0
                                        }
                                    } else {
                                        0
                                    }
                                } else {
                                    0
                                };

                                // Get offset (default 1)
                                let offset = if func_args(func).len() >= 2 {
                                    if let Ok(Expr::Value(ValueWithSpan {
                                        value: Value::Number(n, _),
                                        ..
                                    })) = self.extract_function_arg_expr(&func_args(func)[1])
                                    {
                                        n.parse::<i64>().unwrap_or(1)
                                    } else {
                                        1
                                    }
                                } else {
                                    1
                                };

                                // Partition columns for boundary detection (using ephemeral positions)
                                let part_str = partition_positions
                                    .iter()
                                    .map(|p| p.to_string())
                                    .collect::<Vec<_>>()
                                    .join(",");

                                Some(format!(
                                    "{}:cursor={}:col={}:offset={}:parts={}",
                                    func_name, ephemeral_cursor, col_pos, offset, part_str
                                ))
                            } else {
                                None
                            };

                            self.emit(
                                OpCode::WindowValue,
                                row_num_reg,
                                output_start_reg + output_idx as i64,
                                func_type,
                                p4,
                                &format!("Get {} value", func_name),
                            );
                        }
                        Expr::Identifier(ident) => {
                            // Regular column reference
                            if let Some(col_idx) = table.column_index(&ident.value) {
                                if let Some(pos) = base_columns.iter().position(|&c| c == col_idx) {
                                    self.emit(
                                        OpCode::Column,
                                        ephemeral_cursor,
                                        pos as i64,
                                        output_start_reg + output_idx as i64,
                                        None,
                                        "",
                                    );
                                }
                            }
                        }
                        _ => {
                            // Other expressions - compile as usual
                            self.code_expr(
                                expr,
                                &NameCtx::single(table, ephemeral_cursor),
                                Some(output_start_reg + output_idx as i64),
                            )?;
                        }
                    }
                }
                SelectItem::Wildcard(_) => {
                    // Load all columns
                    for (i, &_col_idx) in base_columns.iter().enumerate() {
                        self.emit(
                            OpCode::Column,
                            ephemeral_cursor,
                            i as i64,
                            output_start_reg + output_idx as i64,
                            None,
                            "",
                        );
                        output_idx += 1;
                    }
                    continue;
                }
                _ => {}
            }
            output_idx += 1;
        }

        // Stage the row
        self.emit(
            OpCode::IdxInsert,
            staging,
            output_start_reg,
            output_col_count as i64,
            None,
            "stage window row",
        );

        // Next row in ephemeral
        self.emit(
            OpCode::Next,
            ephemeral_cursor,
            output_loop_start as i64,
            0,
            None,
            "",
        );

        let after_output = self.program.len();

        // Patch output rewind
        if let Some(inst) = self.program.instructions.get_mut(output_rewind_addr) {
            inst.p2 = after_output as i64;
        }

        // Patch sort jump (empty case)
        if let Some(inst) = self.program.instructions.get_mut(sort_jump_target - 1) {
            inst.p2 = after_output as i64;
        }

        // An aggregate window with PARTITION BY and no ORDER BY has the whole
        // partition as its frame, so every row must see the partition total.
        // The streaming pass produces a RUNNING total, whose value at the last
        // row of each partition IS the total -- back-fill from there.
        //
        // With an ORDER BY the running total is the correct answer, so this is
        // emitted only when the window is unordered.
        if order_cols.is_empty() && !partition_cols.is_empty() {
            // With wildcards rejected above, each projection item yields
            // exactly one output column, so the SELECT-list index IS the
            // result-column index that WindowFinalize needs.
            let agg_output_positions: Vec<usize> = select
                .projection
                .iter()
                .enumerate()
                .filter(|(_, item)| match item {
                    SelectItem::UnnamedExpr(Expr::Function(f))
                    | SelectItem::ExprWithAlias {
                        expr: Expr::Function(f),
                        ..
                    } => {
                        f.over.is_some()
                            && AggregateFunction::is_aggregate_name(&f.name.to_string())
                    }
                    _ => false,
                })
                .map(|(i, _)| i)
                .collect();

            // One per aggregate window column: finalizing only the first left
            // any others showing a running value.
            for pos in agg_output_positions {
                self.emit(
                    OpCode::WindowFinalize,
                    pos as i64,
                    staging,
                    0,
                    None,
                    "Give every row its partition's final window value",
                );
            }
        }

        Ok(())
    }

    /// Analyze projection to extract window functions and base columns needed
    #[allow(clippy::type_complexity)]
    fn analyze_window_projection(
        &self,
        projection: &[SelectItem],
        table: &Table,
    ) -> SqawkResult<(Vec<(String, Option<WindowType>)>, Vec<usize>)> {
        let mut window_funcs = Vec::new();
        let mut base_columns = Vec::new();

        for item in projection {
            match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    match expr {
                        Expr::Function(func) if func.over.is_some() => {
                            let name = func.name.to_string().to_uppercase();
                            window_funcs.push((name, func.over.clone()));

                            // If it's an aggregate window function, add argument column
                            if !func_args(func).is_empty() {
                                if let Ok(Expr::Identifier(ident)) =
                                    self.extract_function_arg_expr(&func_args(func)[0])
                                {
                                    if let Some(col_idx) = table.column_index(&ident.value) {
                                        if !base_columns.contains(&col_idx) {
                                            base_columns.push(col_idx);
                                        }
                                    }
                                }
                            }
                        }
                        Expr::Identifier(ident) => {
                            if let Some(col_idx) = table.column_index(&ident.value) {
                                if !base_columns.contains(&col_idx) {
                                    base_columns.push(col_idx);
                                }
                            }
                        }
                        _ => {}
                    }
                }
                SelectItem::Wildcard(_) => {
                    // Include all columns
                    for i in 0..table.column_count() {
                        if !base_columns.contains(&i) {
                            base_columns.push(i);
                        }
                    }
                }
                _ => {}
            }
        }

        Ok((window_funcs, base_columns))
    }

    /// Extract partition and order columns from window specification
    fn extract_window_spec(
        &self,
        window_type: &Option<WindowType>,
        table: &Table,
    ) -> SqawkResult<(Vec<usize>, Vec<usize>, Vec<bool>)> {
        let mut partition_cols = Vec::new();
        let mut order_cols = Vec::new();
        let mut order_asc = Vec::new();

        if let Some(WindowType::WindowSpec(spec)) = window_type {
            // An explicit frame is parsed but not implemented. Accepting it
            // silently returns the default frame's answer -- `ROWS BETWEEN 1
            // PRECEDING AND CURRENT ROW` produced a running total over the
            // whole partition rather than a two-row sliding window -- so it is
            // rejected instead. A clear error beats a plausible wrong number.
            if spec.window_frame.is_some() {
                return Err(SqawkError::UnsupportedSqlFeature(
                    "Explicit window frames (ROWS/RANGE BETWEEN) are not supported".into(),
                ));
            }

            // Extract PARTITION BY columns
            for expr in &spec.partition_by {
                if let Expr::Identifier(ident) = expr {
                    if let Some(col_idx) = table.column_index(&ident.value) {
                        partition_cols.push(col_idx);
                    }
                }
            }

            // Extract ORDER BY columns
            for order_expr in &spec.order_by {
                if let Expr::Identifier(ident) = &order_expr.expr {
                    if let Some(col_idx) = table.column_index(&ident.value) {
                        order_cols.push(col_idx);
                        order_asc.push(order_by_is_asc(order_expr));
                    }
                }
            }
        }

        Ok((partition_cols, order_cols, order_asc))
    }

    /// Build result schema for window function query
    fn build_window_result_schema(&self, projection: &[SelectItem], table: &Table) -> ResultSchema {
        let mut schema = ResultSchema::new();

        for item in projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let (name, data_type) = self.infer_expr_schema(expr, table);
                    schema.add_column(name, data_type);
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let (_, data_type) = self.infer_expr_schema(expr, table);
                    schema.add_column(alias.value.clone(), data_type);
                }
                SelectItem::Wildcard(_) => {
                    for col in table.column_metadata() {
                        schema.add_column(col.name.clone(), col.data_type);
                    }
                }
                _ => {}
            }
        }

        schema
    }

    /// Infer schema (name, type) for an expression
    pub(crate) fn infer_expr_schema(&self, expr: &Expr, table: &Table) -> (String, DataType) {
        match expr {
            Expr::Identifier(ident) => {
                if let Some(col_idx) = table.column_index(&ident.value) {
                    let col = &table.column_metadata()[col_idx];
                    (ident.value.clone(), col.data_type)
                } else {
                    (ident.value.clone(), DataType::Text)
                }
            }
            Expr::Function(func) => {
                let name = func.name.to_string();
                // Window functions return Integer (for ROW_NUMBER, RANK) or the input type (for aggregates)
                let data_type = match name.to_uppercase().as_str() {
                    "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "COUNT" => DataType::Integer,
                    _ => DataType::Float,
                };
                (name, data_type)
            }
            _ => ("expr".to_string(), DataType::Text),
        }
    }
}
