//! JOIN compilation
//!
//! This module extends SqlCompiler with JOIN compilation methods.

use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, JoinConstraint, JoinOperator, Query, Select, SelectItem,
    TableWithJoins,
};

/// Represents an item in a multi-table aggregate projection
#[derive(Clone, Debug)]
enum MultiTableProjectionItem {
    /// A column reference: (table_idx, col_idx)
    Column(usize, usize),
    /// An aggregate: (func_type, optional column ref for the aggregate argument)
    Aggregate(i64, Option<(usize, usize)>),
}

use super::ast_compat::{func_args, group_by_exprs};
use super::bytecode::{OpCode, ResultSchema};
use super::compiler::{MultiTableProjection, NameCtx, SqlCompiler, Src};
use crate::error::{SqawkError, SqawkResult};
use crate::table::Table;

/// Classify a `JoinOperator` into sqawk's join kind plus its constraint.
///
/// This is the single place where `JoinOperator` is destructured, and the match
/// is deliberately EXHAUSTIVE -- no `_` arm. sqlparser treats any AST change as
/// breaking, and `JoinOperator` gains variants across versions: newer releases
/// parse a plain `LEFT JOIN` as `Left` rather than `LeftOuter`, reserving
/// `LeftOuter` for the explicit `LEFT OUTER JOIN` spelling. Behind a catch-all
/// that rename turns every `LEFT JOIN` in the suite into a runtime error (or,
/// worse, silently into an INNER JOIN); with an exhaustive match it is a
/// compile error that names the site.
///
/// `CrossJoin` returns `None` for its constraint -- callers handle it before
/// asking for an ON expression.
fn classify_join(op: &JoinOperator) -> SqawkResult<(&'static str, Option<&JoinConstraint>)> {
    Ok(match op {
        // A bare `JOIN ... ON` is an INNER JOIN. sqlparser distinguishes the
        // spelling (`Join` vs `Inner`); SQL does not.
        JoinOperator::Join(c) | JoinOperator::Inner(c) => ("INNER", Some(c)),
        // Likewise `LEFT JOIN` and `LEFT OUTER JOIN` are the same operation,
        // as are the RIGHT pair. sqlparser reserves the `*Outer` variants for
        // the explicit OUTER spelling.
        JoinOperator::Left(c) | JoinOperator::LeftOuter(c) => ("LEFT", Some(c)),
        JoinOperator::Right(c) | JoinOperator::RightOuter(c) => ("RIGHT", Some(c)),
        JoinOperator::FullOuter(c) => ("FULL", Some(c)),
        JoinOperator::CrossJoin(_) => ("CROSS", None),
        JoinOperator::Semi(_) | JoinOperator::LeftSemi(_) | JoinOperator::RightSemi(_) => {
            return Err(SqawkError::UnsupportedSqlFeature(
                "SEMI JOIN not supported".into(),
            ))
        }
        JoinOperator::Anti(_) | JoinOperator::LeftAnti(_) | JoinOperator::RightAnti(_) => {
            return Err(SqawkError::UnsupportedSqlFeature(
                "ANTI JOIN not supported".into(),
            ))
        }
        JoinOperator::CrossApply => {
            return Err(SqawkError::UnsupportedSqlFeature(
                "CROSS APPLY not supported".into(),
            ))
        }
        JoinOperator::OuterApply => {
            return Err(SqawkError::UnsupportedSqlFeature(
                "OUTER APPLY not supported".into(),
            ))
        }
        JoinOperator::AsOf { .. } => {
            return Err(SqawkError::UnsupportedSqlFeature(
                "ASOF JOIN not supported".into(),
            ))
        }
        // MySQL optimizer hint: semantically a plain INNER JOIN, but it also
        // forces a join order. Rejected rather than silently ignoring the hint.
        JoinOperator::StraightJoin(_) => {
            return Err(SqawkError::UnsupportedSqlFeature(
                "STRAIGHT_JOIN not supported".into(),
            ))
        }
        JoinOperator::ArrayJoin | JoinOperator::LeftArrayJoin | JoinOperator::InnerArrayJoin => {
            return Err(SqawkError::UnsupportedSqlFeature(
                "ARRAY JOIN not supported".into(),
            ))
        }
    })
}

/// The name a query uses to refer to a FROM item: its alias if it has one,
/// otherwise the table name.
///
/// SQL lets a join alias its inputs, and `FROM users u JOIN orders o ON
/// u.id = o.user_id` must resolve `u` and `o`. Aliases already worked for a
/// single table and for comma joins; explicit joins resolved against the table
/// name alone, so any aliased explicit join failed with "Table 'u' not found".
fn table_refname(factor: &sqlparser::ast::TableFactor, fallback: &str) -> String {
    match factor {
        sqlparser::ast::TableFactor::Table {
            alias: Some(alias), ..
        } => alias.name.value.to_ascii_lowercase(),
        _ => fallback.to_ascii_lowercase(),
    }
}

/// Resolved join projection: the left table's column indices, the right
/// table's column indices, the output schema, and where each projected column
/// lives as (side, index within that side) in SELECT-list order.
type JoinProjection = (Vec<usize>, Vec<usize>, ResultSchema, Vec<(usize, usize)>);

impl<'a> SqlCompiler<'a> {
    /// Emit a join result row in SELECT-list order.
    ///
    /// Join registers are laid out left-table columns then right-table
    /// columns, but the schema is built in projection order, so emitting the
    /// raw block put values under the wrong headers whenever the SELECT list
    /// interleaved the tables: `SELECT orders.id, users.name` printed the
    /// header `orders.id,users.name` above the values `John,101`.
    fn emit_join_result_row(
        &mut self,
        out_plan: &[(usize, usize)],
        left_start_reg: i64,
        left_len: usize,
        comment: &str,
    ) {
        let out = self.allocate_registers(out_plan.len().max(1));
        for (i, (side, idx)) in out_plan.iter().enumerate() {
            let src = if *side == 0 {
                left_start_reg + *idx as i64
            } else {
                left_start_reg + left_len as i64 + *idx as i64
            };
            self.emit(OpCode::Copy, src, out + i as i64, 0, None, "");
        }
        self.emit(
            OpCode::ResultRow,
            out,
            out_plan.len() as i64,
            0,
            None,
            comment,
        );
    }

    pub(crate) fn compile_join(
        &mut self,
        table_with_joins: &TableWithJoins,
        projection: &[SelectItem],
        where_clause: &Option<Expr>,
    ) -> SqawkResult<()> {
        // Get the left table name
        let left_table_name = match &table_with_joins.relation {
            sqlparser::ast::TableFactor::Table { name, .. } => self.get_table_name(name)?,
            _ => {
                return Err(SqawkError::UnsupportedSqlFeature(
                    "Only simple table scans are supported in joins".into(),
                ))
            }
        };

        if !self.database.has_table(&left_table_name) {
            return Err(SqawkError::TableNotFound(left_table_name));
        }

        // Handle multiple joins (3+ tables)
        if table_with_joins.joins.len() > 1 {
            return self.compile_multi_join(table_with_joins, projection, where_clause);
        }

        let join = &table_with_joins.joins[0];

        // Get the right table name
        let right_table_name = match &join.relation {
            sqlparser::ast::TableFactor::Table { name, .. } => self.get_table_name(name)?,
            _ => {
                return Err(SqawkError::UnsupportedSqlFeature(
                    "Only simple table references supported in joins".into(),
                ))
            }
        };

        if !self.database.has_table(&right_table_name) {
            return Err(SqawkError::TableNotFound(right_table_name));
        }

        // How the query refers to each side: alias if given, else table name.
        let left_ref = table_refname(&table_with_joins.relation, &left_table_name);
        let right_ref = table_refname(&join.relation, &right_table_name);

        // Check for CROSS JOIN - handle separately
        if matches!(&join.join_operator, JoinOperator::CrossJoin(_)) {
            return self.compile_cross_join(&left_table_name, &right_table_name, projection);
        }

        // Determine join type and constraint. See `classify_join`.
        let (join_type, join_condition) = match classify_join(&join.join_operator)? {
            ("CROSS", _) => unreachable!("Handled above"),
            (kind, Some(constraint)) => (kind, constraint),
            (kind, None) => {
                return Err(SqawkError::UnsupportedSqlFeature(format!(
                    "{kind} JOIN without a constraint is not supported"
                )))
            }
        };

        // Extract ON condition
        let on_expr = match join_condition {
            JoinConstraint::On(expr) => expr,
            JoinConstraint::None => {
                return Err(SqawkError::UnsupportedSqlFeature(
                    "Join without ON condition not supported".into(),
                ))
            }
            _ => {
                return Err(SqawkError::UnsupportedSqlFeature(
                    "Only ON join constraints supported".into(),
                ))
            }
        };

        if self.verbose {
            eprintln!(
                "Join type: {}",
                if join_type == "INNER" {
                    "Inner"
                } else {
                    join_type
                }
            );
            eprintln!("Processing {} JOIN with ON condition", join_type);
        }
        self.add_comment(&format!("{} JOIN {} ON ...", join_type, right_table_name));

        let left_table = self.database.get_table(&left_table_name)?;
        let right_table = self.database.get_table(&right_table_name)?;

        // Determine which columns to output based on projection
        let (left_cols, right_cols, schema, out_plan) = self.resolve_join_projection(
            projection,
            left_table,
            right_table,
            &left_ref,
            &right_ref,
        )?;

        // Set result schema
        self.program.set_result_schema(schema);

        // For LEFT and INNER joins: outer loop on left, inner on right
        // For RIGHT join: outer loop on right, inner on left
        // We'll use "outer" and "inner" variables to abstract this
        let (outer_cursor, inner_cursor) = if join_type == "RIGHT" {
            (1usize, 0usize) // right is outer, left is inner
        } else {
            (0usize, 1usize) // left is outer, right is inner
        };

        let (outer_table_name, inner_table_name) = if join_type == "RIGHT" {
            (right_table_name.clone(), left_table_name.clone())
        } else {
            (left_table_name.clone(), right_table_name.clone())
        };

        // Note: outer_table and inner_table are defined here for potential future use
        // (e.g., for compile-time column lookup), but currently unused
        let (_outer_table, _inner_table) = if join_type == "RIGHT" {
            (right_table, left_table)
        } else {
            (left_table, right_table)
        };

        let (outer_cols, inner_cols) = if join_type == "RIGHT" {
            (right_cols.clone(), left_cols.clone())
        } else {
            (left_cols.clone(), right_cols.clone())
        };

        let left_cursor = 0usize;
        let right_cursor = 1usize;

        // Open both tables
        self.emit(
            OpCode::OpenRead,
            left_cursor as i64,
            1,
            0,
            Some(left_table_name.clone()),
            &format!("Open left table {} for reading", left_table_name),
        );

        self.emit(
            OpCode::OpenRead,
            right_cursor as i64,
            2,
            0,
            Some(right_table_name.clone()),
            &format!("Open right table {} for reading", right_table_name),
        );

        // Allocate registers for columns - always in left, right order
        let left_start_reg = self.allocate_registers(left_cols.len());
        let right_start_reg = self.allocate_registers(right_cols.len());

        // For RIGHT JOIN, outer/inner start regs are swapped
        let (outer_start_reg, inner_start_reg) = if join_type == "RIGHT" {
            (right_start_reg, left_start_reg)
        } else {
            (left_start_reg, right_start_reg)
        };

        // Allocate match flag register
        let match_reg = self.allocate_register();

        // Initialize match flag to 0
        self.emit(
            OpCode::Integer,
            0,
            match_reg,
            0,
            None,
            "Initialize match flag to 0",
        );

        // Rewind outer cursor
        let outer_loop_end_placeholder = self.program.len();
        self.emit(
            OpCode::Rewind,
            outer_cursor as i64,
            0, // Placeholder - will be patched
            0,
            None,
            &format!("Rewind outer cursor ({})", outer_table_name),
        );

        // OUTER LOOP START
        let outer_loop_start = self.program.len();

        // Load outer table columns into their registers
        self.emit_column_loads(outer_cursor as i64, &outer_cols, outer_start_reg, "outer");

        // Reset match flag for this outer iteration
        self.emit(OpCode::Integer, 0, match_reg, 0, None, "Reset match flag");

        // Rewind inner cursor
        let inner_loop_end_placeholder = self.program.len();
        self.emit(
            OpCode::RewindInner,
            inner_cursor as i64,
            0, // Placeholder - will be patched
            0,
            None,
            &format!("Rewind inner cursor ({})", inner_table_name),
        );

        // INNER LOOP START
        let inner_loop_start = self.program.len();

        // Load inner table columns into their registers
        self.emit_column_loads(inner_cursor as i64, &inner_cols, inner_start_reg, "inner");

        // Compile join condition
        let condition_reg = self.compile_join_condition_with_refs(
            on_expr,
            left_table,
            right_table,
            left_cursor,
            right_cursor,
            &left_ref,
            &right_ref,
        )?;

        // If ON condition fails, skip to next inner iteration (placeholder - will be patched)
        let skip_on_addr = self.program.len();
        self.emit(
            OpCode::IfZ,
            condition_reg,
            0,
            0,
            None,
            "Skip if join ON condition false",
        );

        // Compile WHERE clause filtering if present
        let skip_where_addr = if let Some(where_expr) = where_clause {
            // Use compile_join_condition to handle table.column references in WHERE
            let where_cond_reg = self.compile_join_condition_with_refs(
                where_expr,
                left_table,
                right_table,
                left_cursor,
                right_cursor,
                &left_ref,
                &right_ref,
            )?;

            // If WHERE condition fails, skip to next inner iteration
            let skip_addr = self.program.len();
            self.emit(
                OpCode::IfZ,
                where_cond_reg,
                0,
                0,
                None,
                "Skip if WHERE condition false",
            );
            Some(skip_addr)
        } else {
            None
        };

        // Mark match found
        self.emit(
            OpCode::MarkMatch,
            match_reg,
            0,
            0,
            None,
            "Mark that a match was found",
        );

        // Output combined row (matched)
        self.emit_join_result_row(
            &out_plan,
            left_start_reg,
            left_cols.len(),
            "Output matched row",
        );

        // Next inner
        let next_inner_addr = self.program.len();
        self.emit(
            OpCode::Next,
            inner_cursor as i64,
            inner_loop_start as i64,
            0,
            None,
            "Next inner row",
        );

        // Patch skip addresses to jump to Next inner
        if let Some(inst) = self.program.instructions.get_mut(skip_on_addr) {
            inst.p2 = next_inner_addr as i64;
        }
        if let Some(skip_addr) = skip_where_addr {
            if let Some(inst) = self.program.instructions.get_mut(skip_addr) {
                inst.p2 = next_inner_addr as i64;
            }
        }

        // INNER LOOP END - patch the RewindInner jump address
        let inner_loop_end = self.program.len();

        // For LEFT/RIGHT/FULL JOIN: Check if no match was found, output outer row with NULLs for inner
        // LEFT JOIN: outer=left, inner=right, fill right with NULL
        // RIGHT JOIN: outer=right, inner=left, fill left with NULL
        if join_type == "LEFT" || join_type == "RIGHT" || join_type == "FULL" {
            // If a match was found, skip the NULL-filled row entirely.
            //
            // This was a hand-counted `program.len() + 3` justified by a
            // comment enumerating the three instructions that followed. The
            // count silently stopped being 3 the moment the result row needed
            // more than one instruction to emit, so the jump landed inside the
            // block it was supposed to skip and produced spurious NULL rows.
            let skip_null_row = self.label();
            self.emit_jump_to(
                OpCode::IfPos,
                match_reg,
                skip_null_row,
                0,
                None,
                "Skip NULL row if match found",
            );

            // No match: output NULLs for inner columns
            self.emit(
                OpCode::NullRow,
                inner_start_reg,
                inner_cols.len() as i64,
                0,
                None,
                "Fill inner columns with NULL",
            );

            // Output row with NULLs
            self.emit_join_result_row(
                &out_plan,
                left_start_reg,
                left_cols.len(),
                "Output outer row with NULLs for inner",
            );
            self.resolve(skip_null_row);
        }

        // Next outer
        self.emit(
            OpCode::Next,
            outer_cursor as i64,
            outer_loop_start as i64,
            0,
            None,
            "Next outer row",
        );

        // OUTER LOOP END
        let mut outer_loop_end = self.program.len();
        // For FULL JOIN, we need to track where second pass starts
        let mut second_pass_start: Option<usize> = None;

        // For FULL JOIN: Second pass to output unmatched right rows
        // The first pass (LEFT JOIN style) already output matched rows and unmatched left rows
        // Now we need to output right rows that have no match in left table
        if join_type == "FULL" {
            self.add_comment("FULL JOIN: Second pass for unmatched right rows");

            // Save where second pass starts (this is where we jump if left table is empty)
            second_pass_start = Some(self.program.len());

            // Rewind right cursor
            let right_loop_end_placeholder = self.program.len();
            self.emit(
                OpCode::Rewind,
                right_cursor as i64,
                0,
                0,
                None,
                "Rewind right cursor for second pass",
            );

            let right_loop_start = self.program.len();

            // Load right columns
            self.emit_column_loads(right_cursor as i64, &right_cols, right_start_reg, "right");

            // Reset match flag
            self.emit(
                OpCode::Integer,
                0,
                match_reg,
                0,
                None,
                "Reset match flag for right row",
            );

            // Rewind left cursor to check for matches
            let left_check_end_placeholder = self.program.len();
            self.emit(
                OpCode::RewindInner,
                left_cursor as i64,
                0,
                0,
                None,
                "Rewind left cursor to check for matches",
            );

            let left_check_start = self.program.len();

            // Load left columns for condition check
            self.emit_column_loads(left_cursor as i64, &left_cols, left_start_reg, "left");

            // Check join condition
            let condition_reg2 = self.compile_join_condition_with_refs(
                on_expr,
                left_table,
                right_table,
                left_cursor,
                right_cursor,
                &left_ref,
                &right_ref,
            )?;

            // If condition matches, mark it and skip to next right row
            // IfZ skips over MarkMatch if condition is false
            let skip_mark = self.label();
            self.emit_jump_to(
                OpCode::IfZ,
                condition_reg2,
                skip_mark,
                0,
                None,
                "Skip if condition false",
            );

            // Mark that a match was found
            self.emit(
                OpCode::MarkMatch,
                match_reg,
                0,
                0,
                None,
                "Mark that right row has a match",
            );
            self.resolve(skip_mark);

            // Next left (continue checking)
            self.emit(
                OpCode::Next,
                left_cursor as i64,
                left_check_start as i64,
                0,
                None,
                "Next left row",
            );

            let left_check_end = self.program.len();

            // After checking all left rows: if no match, output right row with NULLs for left
            // If match found (IfPos), skip to next right row
            let skip_null_left = self.label();
            self.emit_jump_to(
                OpCode::IfPos,
                match_reg,
                skip_null_left,
                0,
                None,
                "Skip if right row had a match",
            );

            // No match: fill left columns with NULL
            self.emit(
                OpCode::NullRow,
                left_start_reg,
                left_cols.len() as i64,
                0,
                None,
                "Fill left columns with NULL",
            );

            // Output row with NULLs for left
            self.emit_join_result_row(
                &out_plan,
                left_start_reg,
                left_cols.len(),
                "Output unmatched right row with NULLs",
            );
            self.resolve(skip_null_left);

            // Next right row
            self.emit(
                OpCode::Next,
                right_cursor as i64,
                right_loop_start as i64,
                0,
                None,
                "Next right row",
            );

            let right_loop_end = self.program.len();

            // Patch jump addresses for second pass
            if let Some(inst) = self
                .program
                .instructions
                .get_mut(right_loop_end_placeholder)
            {
                inst.p2 = right_loop_end as i64;
            }
            if let Some(inst) = self
                .program
                .instructions
                .get_mut(left_check_end_placeholder)
            {
                inst.p2 = left_check_end as i64;
            }

            outer_loop_end = right_loop_end;
        }

        // Close cursors
        self.emit(
            OpCode::Close,
            left_cursor as i64,
            0,
            0,
            None,
            "Close left cursor",
        );
        self.emit(
            OpCode::Close,
            right_cursor as i64,
            0,
            0,
            None,
            "Close right cursor",
        );

        // Patch jump addresses for main loop
        if let Some(inst) = self
            .program
            .instructions
            .get_mut(outer_loop_end_placeholder)
        {
            // For FULL JOIN, when left table is empty (Rewind returns false),
            // we still need to run second pass to output unmatched right rows
            if let Some(second_start) = second_pass_start {
                inst.p2 = second_start as i64;
            } else {
                inst.p2 = outer_loop_end as i64;
            }
        }
        if let Some(inst) = self
            .program
            .instructions
            .get_mut(inner_loop_end_placeholder)
        {
            inst.p2 = inner_loop_end as i64;
        }

        Ok(())
    }

    /// Compile a CROSS JOIN (Cartesian product)
    pub(crate) fn compile_cross_join(
        &mut self,
        left_table_name: &str,
        right_table_name: &str,
        projection: &[SelectItem],
    ) -> SqawkResult<()> {
        if self.verbose {
            eprintln!("Join type: Cross");
            eprintln!("Processing CROSS JOIN");
        }
        self.add_comment(&format!(
            "CROSS JOIN {} x {}",
            left_table_name, right_table_name
        ));

        let left_table = self.database.get_table(left_table_name)?;
        let right_table = self.database.get_table(right_table_name)?;

        // Determine which columns to output based on projection
        let (left_cols, right_cols, schema, out_plan) = self.resolve_join_projection(
            projection,
            left_table,
            right_table,
            left_table_name,
            right_table_name,
        )?;

        // Set result schema
        self.program.set_result_schema(schema);

        let left_cursor = 0usize;
        let right_cursor = 1usize;

        // Open both tables
        self.emit(
            OpCode::OpenRead,
            left_cursor as i64,
            1,
            0,
            Some(left_table_name.to_string()),
            &format!("Open left table {} for reading", left_table_name),
        );

        self.emit(
            OpCode::OpenRead,
            right_cursor as i64,
            2,
            0,
            Some(right_table_name.to_string()),
            &format!("Open right table {} for reading", right_table_name),
        );

        // Allocate registers for columns
        let left_start_reg = self.allocate_registers(left_cols.len());
        let right_start_reg = self.allocate_registers(right_cols.len());

        // Rewind left cursor (outer loop)
        let left_loop_end_placeholder = self.program.len();
        self.emit(
            OpCode::Rewind,
            left_cursor as i64,
            0, // Placeholder - will be patched
            0,
            None,
            "Rewind left cursor (outer loop)",
        );

        // OUTER LOOP START
        let left_loop_start = self.program.len();

        // Load left table columns
        self.emit_column_loads(left_cursor as i64, &left_cols, left_start_reg, "left");

        // Rewind right cursor (inner loop)
        let right_loop_end_placeholder = self.program.len();
        self.emit(
            OpCode::RewindInner,
            right_cursor as i64,
            0, // Placeholder - will be patched
            0,
            None,
            "Rewind right cursor (inner loop)",
        );

        // INNER LOOP START
        let right_loop_start = self.program.len();

        // Load right table columns
        self.emit_column_loads(right_cursor as i64, &right_cols, right_start_reg, "right");

        // Output combined row
        self.emit_join_result_row(
            &out_plan,
            left_start_reg,
            left_cols.len(),
            "Output cross join row",
        );

        // Next right (inner)
        self.emit(
            OpCode::Next,
            right_cursor as i64,
            right_loop_start as i64,
            0,
            None,
            "Next right row",
        );

        // INNER LOOP END
        let right_loop_end = self.program.len();

        // Next left (outer)
        self.emit(
            OpCode::Next,
            left_cursor as i64,
            left_loop_start as i64,
            0,
            None,
            "Next left row",
        );

        // OUTER LOOP END
        let left_loop_end = self.program.len();

        // Close cursors
        self.emit(
            OpCode::Close,
            left_cursor as i64,
            0,
            0,
            None,
            "Close left cursor",
        );

        self.emit(
            OpCode::Close,
            right_cursor as i64,
            0,
            0,
            None,
            "Close right cursor",
        );

        // Patch jump addresses
        if let Some(inst) = self.program.instructions.get_mut(left_loop_end_placeholder) {
            inst.p2 = left_loop_end as i64;
        }
        if let Some(inst) = self
            .program
            .instructions
            .get_mut(right_loop_end_placeholder)
        {
            inst.p2 = right_loop_end as i64;
        }

        Ok(())
    }

    /// Compile an implicit join with aggregates only (no GROUP BY)
    /// This is a simpler pattern that doesn't require sorting
    fn compile_implicit_join_aggregate_only(
        &mut self,
        select: &Select,
        table_names: &[String],
        table_refs: &[String],
        tables: &[&Table],
        projection_items: &[MultiTableProjectionItem],
    ) -> SqawkResult<()> {
        let num_tables = tables.len();
        self.add_comment("Multi-table aggregate (no GROUP BY)");

        // Open all tables
        for (i, table_name) in table_names.iter().enumerate() {
            self.emit(
                OpCode::OpenRead,
                i as i64,
                (i + 1) as i64,
                0,
                Some(table_name.to_string()),
                &format!("Open table {}", table_name),
            );
        }

        // Allocate accumulator registers for each aggregate
        let num_aggs = projection_items
            .iter()
            .filter(|i| matches!(i, MultiTableProjectionItem::Aggregate(_, _)))
            .count();
        let acc_base_reg = self.allocate_registers(num_aggs.max(1));

        // Create nested loops for the join
        let mut loop_end_placeholders: Vec<usize> = Vec::with_capacity(num_tables);
        let mut loop_starts: Vec<usize> = Vec::with_capacity(num_tables);

        for i in 0..num_tables {
            let rewind_opcode = if i == 0 {
                OpCode::Rewind
            } else {
                OpCode::RewindInner
            };

            let loop_end_placeholder = self.program.len();
            loop_end_placeholders.push(loop_end_placeholder);
            self.emit(
                rewind_opcode,
                i as i64,
                0,
                0,
                None,
                &format!("Rewind cursor {}", i),
            );

            let loop_start = self.program.len();
            loop_starts.push(loop_start);
        }

        // Compile WHERE condition
        let skip_output_addr = if let Some(where_expr) = &select.selection {
            let condition_reg =
                self.compile_multi_table_condition(where_expr, tables, table_refs)?;
            let skip_addr = self.program.len();
            self.emit(
                OpCode::IfZ,
                condition_reg,
                0,
                0,
                None,
                "Skip if WHERE false",
            );
            Some(skip_addr)
        } else {
            None
        };

        // Step aggregates for this matching row
        let mut agg_idx = 0;
        for item in projection_items {
            if let MultiTableProjectionItem::Aggregate(func_type, col_ref) = item {
                let val_reg = if let Some((tbl_idx, col_idx)) = col_ref {
                    let r = self.allocate_register();
                    self.emit(
                        OpCode::Column,
                        *tbl_idx as i64,
                        *col_idx as i64,
                        r,
                        None,
                        "Load aggregate source",
                    );
                    r
                } else {
                    // COUNT(*) - use 1
                    let r = self.allocate_register();
                    self.emit(OpCode::Integer, 1, r, 0, None, "COUNT(*) = 1");
                    r
                };

                self.emit(
                    OpCode::AggStep,
                    *func_type,
                    val_reg,
                    acc_base_reg + agg_idx as i64,
                    None,
                    "",
                );
                agg_idx += 1;
            }
        }

        // Patch skip address for WHERE
        let innermost_next_addr = self.program.len();
        if let Some(skip_addr) = skip_output_addr {
            if let Some(inst) = self.program.instructions.get_mut(skip_addr) {
                inst.p2 = innermost_next_addr as i64;
            }
        }

        // Close loops (innermost first)
        let mut loop_ends: Vec<usize> = Vec::with_capacity(num_tables);
        for i in (0..num_tables).rev() {
            self.emit(
                OpCode::Next,
                i as i64,
                loop_starts[i] as i64,
                0,
                None,
                &format!("Next {}", i),
            );
            loop_ends.push(self.program.len());
        }
        loop_ends.reverse();

        // Close tables
        for i in 0..num_tables {
            self.emit(
                OpCode::Close,
                i as i64,
                0,
                0,
                None,
                &format!("Close cursor {}", i),
            );
        }

        // Patch loop end placeholders
        for (i, placeholder) in loop_end_placeholders.iter().enumerate() {
            if let Some(inst) = self.program.instructions.get_mut(*placeholder) {
                inst.p2 = loop_ends[i] as i64;
            }
        }

        // Finalize aggregates and output single result row
        let result_reg = self.allocate_registers(projection_items.len());
        agg_idx = 0;
        for (i, item) in projection_items.iter().enumerate() {
            match item {
                MultiTableProjectionItem::Column(_, _) => {
                    // This shouldn't happen in aggregate-only queries
                    // but handle it anyway with NULL
                    self.emit(OpCode::Null, 0, result_reg + i as i64, 0, None, "");
                }
                MultiTableProjectionItem::Aggregate(_, _) => {
                    self.emit(
                        OpCode::AggFinal,
                        acc_base_reg + agg_idx as i64,
                        result_reg + i as i64,
                        0,
                        None,
                        "",
                    );
                    agg_idx += 1;
                }
            }
        }

        // Output the single result row
        self.emit(
            OpCode::ResultRow,
            result_reg,
            projection_items.len() as i64,
            0,
            None,
            "Output aggregate result",
        );

        Ok(())
    }

    /// Compile an implicit join with GROUP BY or aggregates
    pub(crate) fn compile_implicit_join_with_group_by(
        &mut self,
        select: &Select,
        query: &Query,
    ) -> SqawkResult<()> {
        let num_tables = select.from.len();

        // Collect table names and aliases
        let mut table_names: Vec<String> = Vec::with_capacity(num_tables);
        let mut table_refs: Vec<String> = Vec::with_capacity(num_tables);
        for from_item in &select.from {
            let (table_name, table_alias) = match &from_item.relation {
                sqlparser::ast::TableFactor::Table { name, alias, .. } => {
                    let tname = self.get_table_name(name)?;
                    let talias = alias.as_ref().map(|a| a.name.value.clone());
                    (tname, talias)
                }
                _ => {
                    return Err(SqawkError::UnsupportedSqlFeature(
                        "Only simple table references supported".into(),
                    ))
                }
            };
            if !self.database.has_table(&table_name) {
                return Err(SqawkError::TableNotFound(table_name));
            }
            table_refs.push(table_alias.unwrap_or_else(|| table_name.clone()));
            table_names.push(table_name);
        }

        let tables: Vec<&Table> = table_names
            .iter()
            .map(|name| self.database.get_table(name))
            .collect::<Result<Vec<_>, _>>()?;

        // Determine GROUP BY column references
        let mut group_col_refs: Vec<(usize, usize)> = Vec::new(); // (table_idx, col_idx)
        for expr in group_by_exprs(&select.group_by).iter() {
            let (tbl_idx, col_idx) = self.resolve_multi_table_column(expr, &tables, &table_refs)?;
            group_col_refs.push((tbl_idx, col_idx));
        }

        // Analyze projection for aggregates and column references
        let mut projection_items: Vec<MultiTableProjectionItem> = Vec::new();
        for item in &select.projection {
            let expr = match item {
                SelectItem::UnnamedExpr(e) => e,
                SelectItem::ExprWithAlias { expr: e, .. } => e,
                SelectItem::Wildcard(_) => {
                    return Err(SqawkError::UnsupportedSqlFeature(
                        "SELECT * not supported with GROUP BY in multi-table queries".into(),
                    ))
                }
                _ => continue,
            };

            match expr {
                Expr::Function(func) => {
                    let name = func.name.to_string().to_uppercase();
                    if matches!(name.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX") {
                        let func_type = Self::agg_func_type(&name);
                        let col_ref = if let Some(FunctionArg::Unnamed(FunctionArgExpr::Wildcard)) =
                            func_args(func).first()
                        {
                            None // COUNT(*)
                        } else if let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(arg_expr))) =
                            func_args(func).first()
                        {
                            let (tbl_idx, col_idx) =
                                self.resolve_multi_table_column(arg_expr, &tables, &table_refs)?;
                            Some((tbl_idx, col_idx))
                        } else {
                            None
                        };
                        projection_items
                            .push(MultiTableProjectionItem::Aggregate(func_type, col_ref));
                    } else {
                        return Err(SqawkError::UnsupportedSqlFeature(format!(
                            "Function {} not supported in multi-table GROUP BY",
                            name
                        )));
                    }
                }
                Expr::CompoundIdentifier(_) | Expr::Identifier(_) => {
                    let (tbl_idx, col_idx) =
                        self.resolve_multi_table_column(expr, &tables, &table_refs)?;
                    projection_items.push(MultiTableProjectionItem::Column(tbl_idx, col_idx));
                }
                _ => {
                    return Err(SqawkError::UnsupportedSqlFeature(format!(
                        "Unsupported expression in GROUP BY projection: {:?}",
                        expr
                    )));
                }
            }
        }

        // Build result schema
        let schema = self.build_multi_table_agg_schema(&select.projection, &tables, &table_refs);
        self.program.set_result_schema(schema);

        // Handle the no-GROUP-BY case (aggregate only) - simpler pattern without sorter
        if group_col_refs.is_empty() {
            return self.compile_implicit_join_aggregate_only(
                select,
                &table_names,
                &table_refs,
                &tables,
                &projection_items,
            );
        }

        // Calculate columns needed in sorter: group columns + aggregate source columns
        let agg_source_cols: Vec<(usize, usize)> = projection_items
            .iter()
            .filter_map(|item| {
                if let MultiTableProjectionItem::Aggregate(_, Some(col_ref)) = item {
                    Some(*col_ref)
                } else {
                    None
                }
            })
            .collect();

        let total_sorter_cols = group_col_refs.len() + agg_source_cols.len() + 1; // +1 for row count

        // Open sorter
        let sorter_id = 0i64;
        let sort_spec: String = (0..group_col_refs.len())
            .map(|i| format!("{}:asc", i))
            .collect::<Vec<_>>()
            .join(",");

        self.emit(
            OpCode::SorterOpen,
            sorter_id,
            total_sorter_cols as i64,
            0,
            Some(sort_spec),
            "Open sorter for GROUP BY",
        );

        // Open all tables
        for (i, table_name) in table_names.iter().enumerate() {
            self.emit(
                OpCode::OpenRead,
                i as i64,
                (i + 1) as i64,
                0,
                Some(table_name.to_string()),
                &format!("Open table {}", table_name),
            );
        }

        // Allocate registers for sorter row
        let sorter_row_reg = self.allocate_registers(total_sorter_cols);

        // Create nested loops
        let mut loop_end_placeholders: Vec<usize> = Vec::with_capacity(num_tables);
        let mut loop_starts: Vec<usize> = Vec::with_capacity(num_tables);

        for i in 0..num_tables {
            let rewind_opcode = if i == 0 {
                OpCode::Rewind
            } else {
                OpCode::RewindInner
            };

            let loop_end_placeholder = self.program.len();
            loop_end_placeholders.push(loop_end_placeholder);
            self.emit(
                rewind_opcode,
                i as i64,
                0,
                0,
                None,
                &format!("Rewind cursor {}", i),
            );

            let loop_start = self.program.len();
            loop_starts.push(loop_start);
        }

        // Compile WHERE condition
        let skip_output_addr = if let Some(where_expr) = &select.selection {
            let condition_reg =
                self.compile_multi_table_condition(where_expr, &tables, &table_refs)?;
            let skip_addr = self.program.len();
            self.emit(
                OpCode::IfZ,
                condition_reg,
                0,
                0,
                None,
                "Skip if WHERE false",
            );
            Some(skip_addr)
        } else {
            None
        };

        // Load group columns into sorter row registers
        for (i, (tbl_idx, col_idx)) in group_col_refs.iter().enumerate() {
            self.emit(
                OpCode::Column,
                *tbl_idx as i64,
                *col_idx as i64,
                sorter_row_reg + i as i64,
                None,
                &format!("Load GROUP BY col {} from table {}", col_idx, tbl_idx),
            );
        }

        // Load aggregate source columns
        for (i, (tbl_idx, col_idx)) in agg_source_cols.iter().enumerate() {
            self.emit(
                OpCode::Column,
                *tbl_idx as i64,
                *col_idx as i64,
                sorter_row_reg + group_col_refs.len() as i64 + i as i64,
                None,
                &format!("Load agg source col {} from table {}", col_idx, tbl_idx),
            );
        }

        // Set row count to 1
        self.emit(
            OpCode::Integer,
            1,
            sorter_row_reg + (total_sorter_cols - 1) as i64,
            0,
            None,
            "Row count = 1",
        );

        // Insert into sorter
        self.emit(
            OpCode::SorterInsert,
            sorter_id,
            sorter_row_reg,
            total_sorter_cols as i64,
            None,
            "Insert into sorter",
        );

        // Patch skip address
        let innermost_next_addr = self.program.len();
        if let Some(skip_addr) = skip_output_addr {
            if let Some(inst) = self.program.instructions.get_mut(skip_addr) {
                inst.p2 = innermost_next_addr as i64;
            }
        }

        // Close loops (innermost first)
        let mut loop_ends: Vec<usize> = Vec::with_capacity(num_tables);
        for i in (0..num_tables).rev() {
            self.emit(
                OpCode::Next,
                i as i64,
                loop_starts[i] as i64,
                0,
                None,
                &format!("Next {}", i),
            );
            loop_ends.push(self.program.len());
        }
        loop_ends.reverse();

        // Close tables
        for i in 0..num_tables {
            self.emit(
                OpCode::Close,
                i as i64,
                0,
                0,
                None,
                &format!("Close cursor {}", i),
            );
        }

        // Patch loop end placeholders
        for (i, placeholder) in loop_end_placeholders.iter().enumerate() {
            if let Some(inst) = self.program.instructions.get_mut(*placeholder) {
                inst.p2 = loop_ends[i] as i64;
            }
        }

        // Sort the sorter
        self.emit(OpCode::SorterSort, sorter_id, 0, 0, None, "Sort");

        // Allocate registers for reading sorter data
        let row_data_reg = self.allocate_registers(total_sorter_cols);

        // Allocate accumulators for aggregates
        let num_aggs = projection_items
            .iter()
            .filter(|i| matches!(i, MultiTableProjectionItem::Aggregate(_, _)))
            .count();
        let acc_base_reg = self.allocate_registers(num_aggs.max(1));

        // Allocate registers for current and previous group key
        let group_key_reg = self.allocate_registers(group_col_refs.len().max(1));

        // Initialize group key with NULL (first group detection)
        for i in 0..group_col_refs.len() {
            self.emit(
                OpCode::Null,
                0,
                group_key_reg + i as i64,
                0,
                None,
                "Init group key NULL",
            );
        }

        // Flag to indicate we've seen at least one row
        let first_row_reg = self.allocate_register();
        self.emit(OpCode::Integer, 1, first_row_reg, 0, None, "first_row = 1");

        // Allocate result registers
        let result_reg = self.allocate_registers(projection_items.len());

        // Start sorter iteration
        let sorter_loop_start = self.program.len();

        // Get row from sorter using SorterData
        self.emit(
            OpCode::SorterData,
            sorter_id,
            row_data_reg,
            total_sorter_cols as i64,
            None,
            "Load sorter row",
        );

        // Check if first row - if so, skip group change check
        let first_row_jump_addr = self.program.len();
        self.emit(
            OpCode::IfPos,
            first_row_reg,
            0,
            0,
            None,
            "Skip compare if first row",
        );

        // Compare first group column with saved group key
        let cmp_reg = self.allocate_register();
        self.emit(
            OpCode::Ne,
            row_data_reg,
            group_key_reg,
            cmp_reg,
            None,
            "Compare group key",
        );

        // If group NOT changed (cmp_reg is 0), skip output and go to step aggregates
        let skip_output_addr = self.program.len();
        self.emit(
            OpCode::IfZ,
            cmp_reg,
            0,
            0,
            None,
            "Skip output if same group",
        );

        // Group changed - output previous group
        // Build result from group key and aggregates
        let mut agg_idx = 0;
        for (i, item) in projection_items.iter().enumerate() {
            match item {
                MultiTableProjectionItem::Column(_, _) => {
                    // Find the position in group_col_refs
                    let group_idx = group_col_refs
                        .iter()
                        .position(|g| {
                            if let MultiTableProjectionItem::Column(t, c) = item {
                                g.0 == *t && g.1 == *c
                            } else {
                                false
                            }
                        })
                        .unwrap_or(0);
                    self.emit(
                        OpCode::Copy,
                        group_key_reg + group_idx as i64,
                        result_reg + i as i64,
                        0,
                        None,
                        "",
                    );
                }
                MultiTableProjectionItem::Aggregate(_, _) => {
                    self.emit(
                        OpCode::AggFinal,
                        acc_base_reg + agg_idx as i64,
                        result_reg + i as i64,
                        0,
                        None,
                        "",
                    );
                    agg_idx += 1;
                }
            }
        }

        // Output result row
        self.emit(
            OpCode::ResultRow,
            result_reg,
            projection_items.len() as i64,
            0,
            None,
            "Output group",
        );

        // Reset accumulators for new group
        for i in 0..num_aggs {
            self.emit(
                OpCode::AggReset,
                acc_base_reg + i as i64,
                0,
                0,
                None,
                "Reset accumulator",
            );
        }

        // === Update group key section (first row jumps here) ===
        let update_group_key_addr = self.program.len();

        // Patch first row jump
        if let Some(inst) = self.program.instructions.get_mut(first_row_jump_addr) {
            inst.p2 = update_group_key_addr as i64;
        }

        // Initialize/update group key from current row
        for i in 0..group_col_refs.len() {
            self.emit(
                OpCode::Copy,
                row_data_reg + i as i64,
                group_key_reg + i as i64,
                0,
                None,
                "",
            );
        }

        // Clear first row flag
        self.emit(OpCode::Integer, 0, first_row_reg, 0, None, "first_row = 0");

        // === Step aggregates section (skip output jumps here) ===
        let step_agg_addr = self.program.len();

        // Patch skip output jump
        if let Some(inst) = self.program.instructions.get_mut(skip_output_addr) {
            inst.p2 = step_agg_addr as i64;
        }

        // Step aggregates for current row
        agg_idx = 0;
        for item in &projection_items {
            if let MultiTableProjectionItem::Aggregate(func_type, col_ref) = item {
                let val_reg = if let Some((agg_src_idx, _)) = agg_source_cols
                    .iter()
                    .enumerate()
                    .find(|(_, c)| Some(*c) == col_ref.as_ref())
                {
                    // Value is in row_data_reg at offset group_col_refs.len() + agg_src_idx
                    row_data_reg + group_col_refs.len() as i64 + agg_src_idx as i64
                } else {
                    // COUNT(*) - use 1
                    let r = self.allocate_register();
                    self.emit(OpCode::Integer, 1, r, 0, None, "COUNT(*) = 1");
                    r
                };

                self.emit(
                    OpCode::AggStep,
                    *func_type,
                    val_reg,
                    acc_base_reg + agg_idx as i64,
                    None,
                    "",
                );
                agg_idx += 1;
            }
        }

        // Next sorted row
        self.emit(
            OpCode::SorterNext,
            sorter_id,
            sorter_loop_start as i64,
            0,
            None,
            "Next sorter row",
        );

        // Output final group (if first_row_reg is 0, we have data)
        // Skip if first_row_reg is still 1 (no data)
        let skip_final_addr = self.program.len();
        self.emit(
            OpCode::IfPos,
            first_row_reg,
            0,
            0,
            None,
            "Skip final if no data",
        );

        // Build final result
        agg_idx = 0;
        for (i, item) in projection_items.iter().enumerate() {
            match item {
                MultiTableProjectionItem::Column(_, _) => {
                    let group_idx = group_col_refs
                        .iter()
                        .position(|g| {
                            if let MultiTableProjectionItem::Column(t, c) = item {
                                g.0 == *t && g.1 == *c
                            } else {
                                false
                            }
                        })
                        .unwrap_or(0);
                    self.emit(
                        OpCode::Copy,
                        group_key_reg + group_idx as i64,
                        result_reg + i as i64,
                        0,
                        None,
                        "",
                    );
                }
                MultiTableProjectionItem::Aggregate(_, _) => {
                    self.emit(
                        OpCode::AggFinal,
                        acc_base_reg + agg_idx as i64,
                        result_reg + i as i64,
                        0,
                        None,
                        "",
                    );
                    agg_idx += 1;
                }
            }
        }

        // Output final result row
        self.emit(
            OpCode::ResultRow,
            result_reg,
            projection_items.len() as i64,
            0,
            None,
            "Output final group",
        );

        // Patch skip final
        let end_addr = self.program.len();
        if let Some(inst) = self.program.instructions.get_mut(skip_final_addr) {
            inst.p2 = end_addr as i64;
        }

        // Handle LIMIT/OFFSET from query (not implemented yet for GROUP BY)
        let _ = query;

        Ok(())
    }

    /// Compile an implicit join (comma-separated tables in FROM with WHERE filter)
    /// e.g., SELECT * FROM users, orders WHERE users.id = orders.user_id
    /// Supports 2 or more tables with nested loop joins
    /// Compile an implicit join with optional LIMIT/OFFSET support
    pub(crate) fn compile_implicit_join_with_query(
        &mut self,
        select: &Select,
        query: &Query,
    ) -> SqawkResult<()> {
        // Extract LIMIT/OFFSET
        let (limit_val, offset_val) = self.extract_limit_offset(query)?;

        // Handle LIMIT 0 optimization
        if limit_val == Some(0) {
            // LIMIT 0: output empty result
            if let Ok(schema) = self.build_implicit_join_schema(select) {
                self.program.set_result_schema(schema);
            }
            return Ok(());
        }

        self.compile_implicit_join_core(select, limit_val, offset_val)
    }

    pub(crate) fn compile_implicit_join(&mut self, select: &Select) -> SqawkResult<()> {
        self.compile_implicit_join_core(select, None, 0)
    }

    fn compile_implicit_join_core(
        &mut self,
        select: &Select,
        limit_val: Option<i64>,
        offset_val: i64,
    ) -> SqawkResult<()> {
        let num_tables = select.from.len();
        if num_tables < 2 {
            return Err(SqawkError::UnsupportedSqlFeature(
                "Implicit join requires at least 2 tables".into(),
            ));
        }

        // Collect all table names and aliases
        // table_names: actual table names for database lookups
        // table_refs: aliases (if provided) or table names - used for column resolution
        let mut table_names: Vec<String> = Vec::with_capacity(num_tables);
        let mut table_refs: Vec<String> = Vec::with_capacity(num_tables);
        for from_item in &select.from {
            let (table_name, table_alias) = match &from_item.relation {
                sqlparser::ast::TableFactor::Table { name, alias, .. } => {
                    let tname = self.get_table_name(name)?;
                    let talias = alias.as_ref().map(|a| a.name.value.clone());
                    (tname, talias)
                }
                _ => {
                    return Err(SqawkError::UnsupportedSqlFeature(
                        "Only simple table references supported in implicit join".into(),
                    ))
                }
            };
            if !self.database.has_table(&table_name) {
                return Err(SqawkError::TableNotFound(table_name));
            }
            // Use alias for column resolution if provided, otherwise use table name
            table_refs.push(table_alias.unwrap_or_else(|| table_name.clone()));
            table_names.push(table_name);
        }

        // Get references to all tables
        let tables: Vec<&Table> = table_names
            .iter()
            .map(|name| self.database.get_table(name))
            .collect::<Result<Vec<_>, _>>()?;

        // Resolve projection for all tables - determine which columns to output
        // Use table_refs (aliases) for column resolution
        let (table_col_indices, output_col_refs, schema, is_wildcard) =
            self.resolve_multi_table_projection(&select.projection, &tables, &table_refs)?;

        // Set result schema
        self.program.set_result_schema(schema);

        // Open all tables
        for (i, table_name) in table_names.iter().enumerate() {
            self.emit(
                OpCode::OpenRead,
                i as i64,
                (i + 1) as i64,
                0,
                Some(table_name.to_string()),
                &format!("Open table {} for reading", table_name),
            );
        }

        // Set up LIMIT counter register if needed
        let limit_reg = if let Some(limit) = limit_val {
            let reg = self.allocate_register();
            self.emit(
                OpCode::Integer,
                limit,
                reg,
                0,
                None,
                &format!("r[{}] = {} (LIMIT counter)", reg, limit),
            );
            Some(reg)
        } else {
            None
        };

        // Set up OFFSET counter register if needed
        let offset_reg = if offset_val > 0 {
            let reg = self.allocate_register();
            self.emit(
                OpCode::Integer,
                offset_val,
                reg,
                0,
                None,
                &format!("r[{}] = {} (OFFSET counter)", reg, offset_val),
            );
            Some(reg)
        } else {
            None
        };

        // Track loop start/end placeholders for patching
        let mut loop_end_placeholders: Vec<usize> = Vec::with_capacity(num_tables);
        let mut loop_starts: Vec<usize> = Vec::with_capacity(num_tables);

        // For SELECT *, allocate registers for each table's columns
        // For explicit projection, we'll allocate output registers later
        let mut table_start_regs: Vec<i64> = Vec::with_capacity(num_tables);
        if is_wildcard {
            for cols in &table_col_indices {
                let start_reg = self.allocate_registers(cols.len().max(1));
                table_start_regs.push(start_reg);
            }
        }

        // Create nested loops for each table
        for i in 0..num_tables {
            let rewind_opcode = if i == 0 {
                OpCode::Rewind
            } else {
                OpCode::RewindInner
            };

            // Rewind cursor
            let loop_end_placeholder = self.program.len();
            loop_end_placeholders.push(loop_end_placeholder);
            self.emit(
                rewind_opcode,
                i as i64,
                0, // Placeholder - will be patched
                0,
                None,
                &format!("Rewind cursor {} ({})", i, table_names[i]),
            );

            // Loop start
            let loop_start = self.program.len();
            loop_starts.push(loop_start);

            // For SELECT *, load columns for this table inside the loop
            if is_wildcard {
                for (col_i, col_idx) in table_col_indices[i].iter().enumerate() {
                    self.emit(
                        OpCode::Column,
                        i as i64,
                        *col_idx as i64,
                        table_start_regs[i] + col_i as i64,
                        None,
                        &format!(
                            "r[{}] = {}.col[{}]",
                            table_start_regs[i] + col_i as i64,
                            table_names[i],
                            col_idx
                        ),
                    );
                }
            }
        }

        // Compile WHERE condition to filter rows at the innermost loop
        let skip_output_addr = if let Some(where_expr) = &select.selection {
            if self.verbose {
                eprintln!("WHERE comparison: {:?}", where_expr);
            }
            // Use compile_multi_table_condition to handle table.column references
            // Use table_refs (aliases) for column resolution
            let condition_reg =
                self.compile_multi_table_condition(where_expr, &tables, &table_refs)?;

            // Jump to next iteration if condition is false (0)
            let skip_addr = self.program.len();
            self.emit(
                OpCode::IfZ,
                condition_reg,
                0, // Placeholder - will be patched
                0,
                None,
                "Skip row if WHERE condition is false",
            );
            Some(skip_addr)
        } else {
            None
        };

        // Handle OFFSET - skip rows until offset is exhausted
        let offset_skip_addr = if let Some(off_reg) = offset_reg {
            let addr = self.program.len();
            self.emit(
                OpCode::DecrJumpZero,
                off_reg,
                0, // Placeholder - will jump to Next
                0,
                None,
                "Decrement OFFSET counter, skip if not yet at offset",
            );
            Some(addr)
        } else {
            None
        };

        // Output combined row (only if WHERE passed and OFFSET exhausted)
        if is_wildcard {
            // For SELECT *, columns are already in consecutive registers
            let total_cols: usize = table_col_indices.iter().map(|c| c.len()).sum();
            self.emit(
                OpCode::ResultRow,
                table_start_regs[0],
                total_cols as i64,
                0,
                None,
                "Output implicit join row",
            );
        } else {
            // For explicit projection, load columns in SELECT order into output registers
            let output_start_reg = self.allocate_registers(output_col_refs.len());

            // Load each column in SELECT order
            for (out_i, (tbl_idx, col_idx)) in output_col_refs.iter().enumerate() {
                self.emit(
                    OpCode::Column,
                    *tbl_idx as i64,
                    *col_idx as i64,
                    output_start_reg + out_i as i64,
                    None,
                    &format!(
                        "r[{}] = {}.col[{}]",
                        output_start_reg + out_i as i64,
                        table_names[*tbl_idx],
                        col_idx
                    ),
                );
            }

            self.emit(
                OpCode::ResultRow,
                output_start_reg,
                output_col_refs.len() as i64,
                0,
                None,
                "Output implicit join row",
            );
        }

        // Handle LIMIT - decrement counter and jump to end if exhausted
        let limit_done_addr = if let Some(lim_reg) = limit_reg {
            let addr = self.program.len();
            self.emit(
                OpCode::DecrJumpZero,
                lim_reg,
                0, // Placeholder - will jump to Close cursors
                0,
                None,
                "Decrement LIMIT counter, jump to end if done",
            );
            Some(addr)
        } else {
            None
        };

        // Patch skip address to jump to innermost Next instruction
        let innermost_next_addr = self.program.len();

        // Patch OFFSET skip address to jump to innermost Next
        if let Some(off_addr) = offset_skip_addr {
            if let Some(inst) = self.program.instructions.get_mut(off_addr) {
                inst.p2 = innermost_next_addr as i64;
            }
        }
        if let Some(skip_addr) = skip_output_addr {
            if let Some(inst) = self.program.instructions.get_mut(skip_addr) {
                inst.p2 = innermost_next_addr as i64;
            }
        }

        // Close loops in reverse order (innermost first)
        let mut loop_ends: Vec<usize> = Vec::with_capacity(num_tables);
        for i in (0..num_tables).rev() {
            // Next for this cursor
            self.emit(
                OpCode::Next,
                i as i64,
                loop_starts[i] as i64,
                0,
                None,
                &format!("Next row from {}", table_names[i]),
            );
            loop_ends.push(self.program.len());
        }
        loop_ends.reverse(); // Restore to table order for patching

        // Close all cursors - this is where LIMIT jumps to when done
        let close_cursors_addr = self.program.len();
        for i in 0..num_tables {
            self.emit(
                OpCode::Close,
                i as i64,
                0,
                0,
                None,
                &format!("Close cursor {}", i),
            );
        }

        // Patch jump addresses for loop ends
        for (i, loop_end_placeholder) in loop_end_placeholders.iter().enumerate() {
            if let Some(inst) = self.program.instructions.get_mut(*loop_end_placeholder) {
                inst.p2 = loop_ends[i] as i64;
            }
        }

        // Patch LIMIT done address to jump to Close cursors
        if let Some(lim_addr) = limit_done_addr {
            if let Some(inst) = self.program.instructions.get_mut(lim_addr) {
                inst.p2 = close_cursors_addr as i64;
            }
        }

        Ok(())
    }

    /// Build schema for implicit join without executing (for LIMIT 0 optimization)
    fn build_implicit_join_schema(&self, select: &Select) -> SqawkResult<ResultSchema> {
        let mut table_names: Vec<String> = Vec::new();
        let mut table_refs: Vec<String> = Vec::new();

        for from_item in &select.from {
            let (table_name, table_alias) = match &from_item.relation {
                sqlparser::ast::TableFactor::Table { name, alias, .. } => {
                    let tname = self.get_table_name(name)?;
                    let talias = alias.as_ref().map(|a| a.name.value.clone());
                    (tname, talias)
                }
                _ => continue,
            };
            table_refs.push(table_alias.unwrap_or_else(|| table_name.clone()));
            table_names.push(table_name);
        }

        let tables: Vec<&Table> = table_names
            .iter()
            .filter_map(|name| self.database.get_table(name).ok())
            .collect();

        if tables.is_empty() {
            return Ok(ResultSchema::new());
        }

        let (_, _, schema, _) =
            self.resolve_multi_table_projection(&select.projection, &tables, &table_refs)?;
        Ok(schema)
    }

    /// Resolve projection for multiple tables
    /// Returns (table_cols_for_loading, output_column_refs, schema, is_wildcard)
    /// - table_cols_for_loading: columns to load per table (for SELECT *)
    /// - output_column_refs: (table_idx, col_idx) pairs in SELECT order (for explicit projection)
    fn resolve_multi_table_projection(
        &self,
        projection: &[SelectItem],
        tables: &[&Table],
        table_names: &[String],
    ) -> SqawkResult<MultiTableProjection> {
        let num_tables = tables.len();
        let name_refs: Vec<&str> = table_names.iter().map(|s| s.as_str()).collect();

        // Check for SELECT *
        for item in projection {
            if matches!(item, SelectItem::Wildcard(_)) {
                let (table_cols, schema) = Self::build_wildcard_schema(tables, &name_refs);
                return Ok((table_cols, Vec::new(), schema, true));
            }
        }

        // Handle explicit column selection - track in SELECT order
        let mut output_cols: Vec<(usize, usize)> = Vec::new();
        let mut schema = ResultSchema::new();

        for item in projection {
            let (tbl_name, col_name, output_name) = match item {
                SelectItem::UnnamedExpr(expr) => {
                    let (t, c) = self.extract_join_column_ref(expr)?;
                    let out = format!("{}.{}", t, c);
                    (t, c, out)
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let (t, c) = self.extract_join_column_ref(expr)?;
                    (t, c, alias.value.clone())
                }
                SelectItem::Wildcard(_) => unreachable!("Handled above"),
                _ => {
                    return Err(SqawkError::UnsupportedSqlFeature(
                        "Unsupported projection item in multi-table JOIN".into(),
                    ));
                }
            };

            let (tbl_idx, col_idx) =
                Self::find_column_in_tables(&tbl_name, &col_name, tables, &name_refs)?;
            output_cols.push((tbl_idx, col_idx));
            schema.add_column(
                output_name,
                tables[tbl_idx].column_metadata()[col_idx].data_type,
            );
        }

        Ok((vec![Vec::new(); num_tables], output_cols, schema, false))
    }

    /// Compile a WHERE condition for multi-table joins
    /// Compile a WHERE condition spanning several tables.
    ///
    /// Delegates to the shared expression compiler with one name-resolution
    /// source per table. Cursor `i` reads `tables[i]`, which is how the
    /// implicit-join loop opens them.
    ///
    /// This replaces a private condition/operand pair that understood only
    /// AND, OR and the six comparisons over compound identifiers and literals.
    /// Anything else -- a function call, arithmetic, IS NULL -- was rejected in
    /// a multi-table WHERE while working in a single-table one.
    fn compile_multi_table_condition(
        &mut self,
        expr: &Expr,
        tables: &[&Table],
        table_names: &[String],
    ) -> SqawkResult<i64> {
        let srcs = tables
            .iter()
            .zip(table_names.iter())
            .enumerate()
            .map(|(i, (table, name))| Src {
                cursor: i as i64,
                reg_base: None,
                table,
                refname: name.to_ascii_lowercase(),
            })
            .collect();
        let ctx = NameCtx::multi(srcs);
        self.code_expr(expr, &ctx, None)
    }

    pub(crate) fn compile_multi_join(
        &mut self,
        table_with_joins: &TableWithJoins,
        projection: &[SelectItem],
        where_clause: &Option<Expr>,
    ) -> SqawkResult<()> {
        // Collect all table names and aliases
        let (first_table_name, first_table_ref) = match &table_with_joins.relation {
            sqlparser::ast::TableFactor::Table { name, alias, .. } => {
                let tname = self.get_table_name(name)?;
                let tref = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| tname.clone());
                (tname, tref)
            }
            _ => {
                return Err(SqawkError::UnsupportedSqlFeature(
                    "Only simple table scans supported in multi-join".into(),
                ))
            }
        };

        let mut table_names = vec![first_table_name];
        let mut table_refs = vec![first_table_ref];
        let mut join_conditions: Vec<&Expr> = Vec::new();

        for join in &table_with_joins.joins {
            let (table_name, table_ref) = match &join.relation {
                sqlparser::ast::TableFactor::Table { name, alias, .. } => {
                    let tname = self.get_table_name(name)?;
                    let tref = alias
                        .as_ref()
                        .map(|a| a.name.value.clone())
                        .unwrap_or_else(|| tname.clone());
                    (tname, tref)
                }
                _ => {
                    return Err(SqawkError::UnsupportedSqlFeature(
                        "Only simple table scans supported in multi-join".into(),
                    ))
                }
            };

            // Only support INNER JOIN for multi-table joins. Routed through
            // `classify_join` so a new/renamed JoinOperator variant is a
            // compile error here too, rather than silently falling into the
            // "Only INNER JOIN" error arm.
            match classify_join(&join.join_operator)? {
                ("INNER", Some(JoinConstraint::On(expr))) => {
                    join_conditions.push(expr);
                }
                _ => {
                    return Err(SqawkError::UnsupportedSqlFeature(
                        "Only INNER JOIN with ON clause supported in multi-table joins".into(),
                    ))
                }
            }

            table_names.push(table_name);
            table_refs.push(table_ref);
        }

        let num_tables = table_names.len();
        self.add_comment(&format!(
            "Multi-table INNER JOIN: {}",
            table_names.join(" x ")
        ));

        // Verify all tables exist and collect table references
        for name in &table_names {
            if !self.database.has_table(name) {
                return Err(SqawkError::TableNotFound(name.clone()));
            }
        }

        // Get table references for projection resolution
        let tables: Vec<&Table> = table_names
            .iter()
            .map(|name| self.database.get_table(name))
            .collect::<Result<Vec<_>, _>>()?;

        // Resolve projection to determine output columns
        // Use table_refs (aliases) for column resolution
        let (_table_col_indices, output_col_refs, schema, is_wildcard) =
            self.resolve_multi_table_projection(projection, &tables, &table_refs)?;

        self.program.set_result_schema(schema);

        // Collect column counts for each table (always load all columns for JOIN condition evaluation)
        let col_counts: Vec<usize> = tables.iter().map(|t| t.column_count()).collect();

        // Open all tables
        for (i, name) in table_names.iter().enumerate() {
            self.emit(
                OpCode::OpenRead,
                i as i64,
                (i + 1) as i64,
                0,
                Some(name.clone()),
                &format!("Open table {} (cursor {})", name, i),
            );
        }

        // Allocate registers for all table columns (needed for JOIN condition evaluation)
        let mut table_start_regs = Vec::new();
        for count in &col_counts {
            let start_reg = self.allocate_registers(*count);
            table_start_regs.push(start_reg);
        }

        // Track loop starts and placeholders for jump patching
        let mut loop_starts = Vec::new();
        let mut loop_end_placeholders = Vec::new();

        // Create nested loops for all tables
        for i in 0..num_tables {
            // Rewind cursor
            let placeholder = self.program.len();
            let opcode = if i == 0 {
                OpCode::Rewind
            } else {
                OpCode::RewindInner
            };
            self.emit(
                opcode,
                i as i64,
                0, // Placeholder
                0,
                None,
                &format!("Rewind cursor {} ({})", i, table_names[i]),
            );
            loop_end_placeholders.push(placeholder);

            // Loop start
            loop_starts.push(self.program.len());

            // Load ALL columns for this table (needed for JOIN condition evaluation)
            for col_idx in 0..col_counts[i] {
                self.emit(
                    OpCode::Column,
                    i as i64,
                    col_idx as i64,
                    table_start_regs[i] + col_idx as i64,
                    None,
                    &format!(
                        "r[{}] = {}.col[{}]",
                        table_start_regs[i] + col_idx as i64,
                        table_names[i],
                        col_idx
                    ),
                );
            }
        }

        // Compile and evaluate all join conditions
        // We'll use placeholders and patch jump addresses after we know the final layout
        let mut skip_to_next_placeholders: Vec<usize> = Vec::new();

        for (i, cond) in join_conditions.iter().enumerate() {
            // Use table_refs (aliases) for column resolution
            let cond_reg =
                self.compile_multi_join_condition(cond, &tables, &table_refs, &table_start_regs)?;

            // If condition fails, skip to next innermost iteration (placeholder)
            let skip_addr = self.program.len();
            skip_to_next_placeholders.push(skip_addr);
            self.emit(
                OpCode::IfZ,
                cond_reg,
                0, // Placeholder - will be patched
                0,
                None,
                &format!("Skip if JOIN condition {} fails", i + 1),
            );
        }

        // Compile WHERE clause if present (additional filtering on top of JOIN conditions)
        if let Some(where_expr) = where_clause {
            if self.verbose {
                eprintln!("Compiling WHERE clause for multi-join: {:?}", where_expr);
            }
            // Use table_refs (aliases) for column resolution
            let where_reg = self.compile_multi_table_condition(where_expr, &tables, &table_refs)?;

            // If WHERE fails, skip to next innermost iteration (placeholder)
            let skip_addr = self.program.len();
            skip_to_next_placeholders.push(skip_addr);
            self.emit(
                OpCode::IfZ,
                where_reg,
                0, // Placeholder - will be patched
                0,
                None,
                "Skip if WHERE condition fails",
            );
        }

        // Output row based on projection type
        if is_wildcard {
            // SELECT * - output all columns
            let total_cols: usize = col_counts.iter().sum();
            self.emit(
                OpCode::ResultRow,
                table_start_regs[0],
                total_cols as i64,
                0,
                None,
                "Output multi-join result row",
            );
        } else {
            // Explicit projection - load columns in SELECT order into output registers
            let output_start_reg = self.allocate_registers(output_col_refs.len());

            // Load each column in SELECT order
            for (out_i, (tbl_idx, col_idx)) in output_col_refs.iter().enumerate() {
                // Use the already-loaded register values
                let src_reg = table_start_regs[*tbl_idx] + *col_idx as i64;
                self.emit(
                    OpCode::Copy,
                    src_reg,
                    output_start_reg + out_i as i64,
                    0,
                    None,
                    &format!(
                        "r[{}] = r[{}] ({}.col[{}])",
                        output_start_reg + out_i as i64,
                        src_reg,
                        table_names[*tbl_idx],
                        col_idx
                    ),
                );
            }

            self.emit(
                OpCode::ResultRow,
                output_start_reg,
                output_col_refs.len() as i64,
                0,
                None,
                "Output multi-join result row",
            );
        }

        // Patch all skip placeholders to jump to the innermost Next (which is next)
        let skip_target = self.program.len();
        for placeholder in &skip_to_next_placeholders {
            if let Some(inst) = self.program.instructions.get_mut(*placeholder) {
                inst.p2 = skip_target as i64;
            }
        }

        // Close nested loops (innermost first)
        let mut loop_ends = Vec::new();
        for i in (0..num_tables).rev() {
            self.emit(
                OpCode::Next,
                i as i64,
                loop_starts[i] as i64,
                0,
                None,
                &format!("Next row from {}", table_names[i]),
            );
            loop_ends.push(self.program.len());
        }
        loop_ends.reverse();

        // Close all cursors
        for i in 0..num_tables {
            self.emit(
                OpCode::Close,
                i as i64,
                0,
                0,
                None,
                &format!("Close cursor {}", i),
            );
        }

        let final_end = self.program.len();

        // Patch jump addresses
        for (i, placeholder) in loop_end_placeholders.iter().enumerate() {
            if let Some(inst) = self.program.instructions.get_mut(*placeholder) {
                // Each rewind jumps to just after its corresponding Next (i.e., the next outer loop's end)
                // The outermost loop jumps to final_end
                if i == 0 {
                    inst.p2 = final_end as i64;
                } else {
                    inst.p2 = loop_ends[i - 1] as i64;
                }
            }
        }

        Ok(())
    }

    /// Compile a join condition for multi-table joins.
    ///
    /// Here every table's columns have already been loaded into registers, so
    /// a column reference is a register offset rather than a cursor read. That
    /// is what `Src::reg_base` models, which lets this share the expression
    /// compiler with the cursor-based paths instead of carrying its own
    /// condition/operand pair -- one that understood a single top-level
    /// comparison and nothing else.
    fn compile_multi_join_condition(
        &mut self,
        expr: &Expr,
        tables: &[&Table],
        table_refs: &[String],
        table_start_regs: &[i64],
    ) -> SqawkResult<i64> {
        let srcs = tables
            .iter()
            .zip(table_refs.iter())
            .zip(table_start_regs.iter())
            .enumerate()
            .map(|(i, ((table, name), base))| Src {
                cursor: i as i64,
                reg_base: Some(*base),
                table,
                refname: name.to_ascii_lowercase(),
            })
            .collect();
        let ctx = NameCtx::multi(srcs);
        self.code_expr(expr, &ctx, None)
    }

    fn resolve_join_projection(
        &self,
        projection: &[SelectItem],
        left_table: &Table,
        right_table: &Table,
        left_table_name: &str,
        right_table_name: &str,
    ) -> SqawkResult<JoinProjection> {
        let tables: Vec<&Table> = vec![left_table, right_table];
        let table_names: Vec<&str> = vec![left_table_name, right_table_name];

        // For SELECT *, return all columns from both tables
        for item in projection {
            if matches!(item, SelectItem::Wildcard(_)) {
                let (table_cols, schema) = Self::build_wildcard_schema(&tables, &table_names);
                // Wildcard output IS left-then-right, so the plan is the
                // identity over that layout.
                let plan = table_cols[0]
                    .iter()
                    .enumerate()
                    .map(|(i, _)| (0usize, i))
                    .chain(table_cols[1].iter().enumerate().map(|(i, _)| (1usize, i)))
                    .collect();
                return Ok((table_cols[0].clone(), table_cols[1].clone(), schema, plan));
            }
        }

        // Handle explicit column selection
        let mut left_cols = Vec::new();
        let mut right_cols = Vec::new();
        let mut schema = ResultSchema::new();
        // Where each projected column lives: (side, index within that side).
        // Registers are laid out left-then-right, but the SELECT list may name
        // them in any order, so the emission order has to be recorded rather
        // than assumed.
        let mut out_plan: Vec<(usize, usize)> = Vec::with_capacity(projection.len());

        for item in projection {
            let (tbl_name, col_name, output_name) = match item {
                SelectItem::UnnamedExpr(expr) => {
                    let (t, c) = self.extract_join_column_ref(expr)?;
                    let out = format!("{}.{}", t, c);
                    (t, c, out)
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let (t, c) = self.extract_join_column_ref(expr)?;
                    (t, c, alias.value.clone())
                }
                SelectItem::Wildcard(_) => unreachable!("Handled above"),
                _ => {
                    return Err(SqawkError::UnsupportedSqlFeature(
                        "Unsupported projection item in JOIN".into(),
                    ));
                }
            };

            let (tbl_idx, col_idx) =
                Self::find_column_in_tables(&tbl_name, &col_name, &tables, &table_names)?;
            if tbl_idx == 0 {
                out_plan.push((0, left_cols.len()));
                left_cols.push(col_idx);
            } else {
                out_plan.push((1, right_cols.len()));
                right_cols.push(col_idx);
            }
            schema.add_column(
                output_name,
                tables[tbl_idx].column_metadata()[col_idx].data_type,
            );
        }

        Ok((left_cols, right_cols, schema, out_plan))
    }

    /// Extract table and column name from a compound identifier (e.g., users.name)
    fn extract_join_column_ref(&self, expr: &Expr) -> SqawkResult<(String, String)> {
        match expr {
            Expr::CompoundIdentifier(parts) => {
                if parts.len() == 2 {
                    Ok((parts[0].value.clone(), parts[1].value.clone()))
                } else {
                    Err(SqawkError::UnsupportedSqlFeature(format!(
                        "Expected table.column reference, got {} parts",
                        parts.len()
                    )))
                }
            }
            Expr::Identifier(ident) => {
                // Unqualified column - would need to search both tables
                // For now, return an error asking for qualified names
                Err(SqawkError::UnsupportedSqlFeature(format!(
                    "Column '{}' must be qualified with table name in JOIN (e.g., table.{})",
                    ident.value, ident.value
                )))
            }
            _ => Err(SqawkError::UnsupportedSqlFeature(format!(
                "Unsupported expression in JOIN projection: {:?}",
                expr
            ))),
        }
    }

    /// Compile a two-table join condition through the shared expression
    /// compiler.
    ///
    /// Qualifiers resolve against `left_ref`/`right_ref` -- the alias each side
    /// was given, or its table name if it had none.
    ///
    /// This replaces a private family of three functions: a condition compiler
    /// that understood only a single top-level BinaryOp, and an operand
    /// compiler beneath it. So `ON a.x = b.y AND a.z = 1` was rejected, as was
    /// any function call in an ON clause, even though both work in a WHERE.
    #[allow(clippy::too_many_arguments)]
    fn compile_join_condition_with_refs(
        &mut self,
        expr: &Expr,
        left_table: &Table,
        right_table: &Table,
        left_cursor: usize,
        right_cursor: usize,
        left_ref: &str,
        right_ref: &str,
    ) -> SqawkResult<i64> {
        let ctx = NameCtx::multi(vec![
            Src {
                cursor: left_cursor as i64,
                reg_base: None,
                table: left_table,
                refname: left_ref.to_ascii_lowercase(),
            },
            Src {
                cursor: right_cursor as i64,
                reg_base: None,
                table: right_table,
                refname: right_ref.to_ascii_lowercase(),
            },
        ]);
        self.code_expr(expr, &ctx, None)
    }

    /// Resolve a column expression to (table_idx, col_idx) for multi-table queries
    fn resolve_multi_table_column(
        &self,
        expr: &Expr,
        tables: &[&Table],
        table_refs: &[String],
    ) -> SqawkResult<(usize, usize)> {
        match expr {
            Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                let table_ref = &parts[0].value;
                let col_name = &parts[1].value;

                // Find the table by reference (alias or name)
                let tbl_idx = table_refs
                    .iter()
                    .position(|r| r.eq_ignore_ascii_case(table_ref))
                    .ok_or_else(|| SqawkError::TableNotFound(table_ref.clone()))?;

                // Find the column in that table
                let col_idx = tables[tbl_idx]
                    .column_index(col_name)
                    .ok_or_else(|| SqawkError::ColumnNotFound(col_name.clone()))?;

                Ok((tbl_idx, col_idx))
            }
            Expr::Identifier(ident) => {
                // Unqualified column - search all tables
                let col_name = &ident.value;
                for (tbl_idx, table) in tables.iter().enumerate() {
                    if let Some(col_idx) = table.column_index(col_name) {
                        return Ok((tbl_idx, col_idx));
                    }
                }
                Err(SqawkError::ColumnNotFound(col_name.clone()))
            }
            _ => Err(SqawkError::UnsupportedSqlFeature(format!(
                "Unsupported column expression in multi-table query: {:?}",
                expr
            ))),
        }
    }

    /// Build result schema for a multi-table aggregate query
    fn build_multi_table_agg_schema(
        &self,
        projection: &[SelectItem],
        tables: &[&Table],
        table_refs: &[String],
    ) -> ResultSchema {
        let mut schema = ResultSchema::new();

        for item in projection {
            let (name, data_type) = match item {
                SelectItem::UnnamedExpr(expr) => {
                    self.get_multi_table_expr_schema(expr, tables, table_refs)
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let (_, dt) = self.get_multi_table_expr_schema(expr, tables, table_refs);
                    (alias.value.clone(), dt)
                }
                _ => ("?".to_string(), crate::table::DataType::Text),
            };
            schema.add_column(name, data_type);
        }

        schema
    }

    /// Get schema info for a single expression in a multi-table context
    fn get_multi_table_expr_schema(
        &self,
        expr: &Expr,
        tables: &[&Table],
        table_refs: &[String],
    ) -> (String, crate::table::DataType) {
        match expr {
            Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                let table_ref = &parts[0].value;
                let col_name = &parts[1].value;

                if let Some(tbl_idx) = table_refs
                    .iter()
                    .position(|r| r.eq_ignore_ascii_case(table_ref))
                {
                    if let Some(col_idx) = tables[tbl_idx].column_index(col_name) {
                        let meta = &tables[tbl_idx].column_metadata()[col_idx];
                        return (format!("{}.{}", table_ref, col_name), meta.data_type);
                    }
                }
                (
                    format!("{}.{}", table_ref, col_name),
                    crate::table::DataType::Text,
                )
            }
            Expr::Identifier(ident) => {
                let col_name = &ident.value;
                for table in tables.iter() {
                    if let Some(col_idx) = table.column_index(col_name) {
                        let meta = &table.column_metadata()[col_idx];
                        return (col_name.clone(), meta.data_type);
                    }
                }
                (col_name.clone(), crate::table::DataType::Text)
            }
            Expr::Function(func) => {
                let func_name = func.name.to_string().to_uppercase();
                // Aggregates typically return numeric types
                let data_type = match func_name.as_str() {
                    "COUNT" => crate::table::DataType::Integer,
                    "SUM" | "AVG" => crate::table::DataType::Float,
                    _ => crate::table::DataType::Text,
                };
                (func_name, data_type)
            }
            _ => ("expr".to_string(), crate::table::DataType::Text),
        }
    }
}
