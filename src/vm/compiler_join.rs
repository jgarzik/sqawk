//! JOIN compilation
//!
//! This module extends SqlCompiler with JOIN compilation methods.

use sqlparser::ast::{
    BinaryOperator, Expr, JoinConstraint, JoinOperator, Query, Select, SelectItem, TableWithJoins,
    Value,
};

use super::bytecode::{Instruction, OpCode, ResultSchema};
use super::compiler::{MultiTableProjection, SqlCompiler};
use crate::error::{SqawkError, SqawkResult};
use crate::table::Table;

impl<'a> SqlCompiler<'a> {
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

        // Check for CROSS JOIN - handle separately
        if matches!(&join.join_operator, JoinOperator::CrossJoin) {
            return self.compile_cross_join(&left_table_name, &right_table_name, projection);
        }

        // Determine join type and constraint
        let (join_type, join_condition) = match &join.join_operator {
            JoinOperator::LeftOuter(constraint) => ("LEFT", constraint),
            JoinOperator::RightOuter(constraint) => ("RIGHT", constraint),
            JoinOperator::FullOuter(constraint) => ("FULL", constraint),
            JoinOperator::Inner(constraint) => ("INNER", constraint),
            JoinOperator::CrossJoin => unreachable!("Handled above"),
            _ => {
                return Err(SqawkError::UnsupportedSqlFeature(
                    "Unsupported join type".into(),
                ))
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
        let (left_cols, right_cols, schema) = self.resolve_join_projection(
            projection,
            left_table,
            right_table,
            &left_table_name,
            &right_table_name,
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
        self.program.add_instruction(Instruction::new(
            OpCode::OpenRead,
            left_cursor as i64,
            1,
            0,
            Some(left_table_name.clone()),
            0,
            Some(format!("Open left table {} for reading", left_table_name)),
        ));

        self.program.add_instruction(Instruction::new(
            OpCode::OpenRead,
            right_cursor as i64,
            2,
            0,
            Some(right_table_name.clone()),
            0,
            Some(format!("Open right table {} for reading", right_table_name)),
        ));

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
        self.program.add_instruction(Instruction::new(
            OpCode::Integer,
            0,
            match_reg,
            0,
            None,
            0,
            Some("Initialize match flag to 0".to_string()),
        ));

        // Rewind outer cursor
        let outer_loop_end_placeholder = self.program.len();
        self.program.add_instruction(Instruction::new(
            OpCode::Rewind,
            outer_cursor as i64,
            0, // Placeholder - will be patched
            0,
            None,
            0,
            Some(format!("Rewind outer cursor ({})", outer_table_name)),
        ));

        // OUTER LOOP START
        let outer_loop_start = self.program.len();

        // Load outer table columns into their registers
        for (i, col_idx) in outer_cols.iter().enumerate() {
            self.program.add_instruction(Instruction::new(
                OpCode::Column,
                outer_cursor as i64,
                *col_idx as i64,
                outer_start_reg + i as i64,
                None,
                0,
                Some(format!(
                    "r[{}] = outer.col[{}]",
                    outer_start_reg + i as i64,
                    col_idx
                )),
            ));
        }

        // Reset match flag for this outer iteration
        self.program.add_instruction(Instruction::new(
            OpCode::Integer,
            0,
            match_reg,
            0,
            None,
            0,
            Some("Reset match flag".to_string()),
        ));

        // Rewind inner cursor
        let inner_loop_end_placeholder = self.program.len();
        self.program.add_instruction(Instruction::new(
            OpCode::RewindInner,
            inner_cursor as i64,
            0, // Placeholder - will be patched
            0,
            None,
            0,
            Some(format!("Rewind inner cursor ({})", inner_table_name)),
        ));

        // INNER LOOP START
        let inner_loop_start = self.program.len();

        // Load inner table columns into their registers
        for (i, col_idx) in inner_cols.iter().enumerate() {
            self.program.add_instruction(Instruction::new(
                OpCode::Column,
                inner_cursor as i64,
                *col_idx as i64,
                inner_start_reg + i as i64,
                None,
                0,
                Some(format!(
                    "r[{}] = inner.col[{}]",
                    inner_start_reg + i as i64,
                    col_idx
                )),
            ));
        }

        // Compile join condition
        let condition_reg = self.compile_join_condition(
            on_expr,
            left_table,
            right_table,
            left_cursor,
            right_cursor,
        )?;

        // If ON condition fails, skip to next inner iteration (placeholder - will be patched)
        let skip_on_addr = self.program.len();
        self.program.add_instruction(Instruction::new(
            OpCode::IfZ,
            condition_reg,
            0, // Placeholder - will be patched to Next inner
            0,
            None,
            0,
            Some("Skip if join ON condition false".to_string()),
        ));

        // Compile WHERE clause filtering if present
        let skip_where_addr = if let Some(where_expr) = where_clause {
            // Use compile_join_condition to handle table.column references in WHERE
            let where_cond_reg = self.compile_join_condition(
                where_expr,
                left_table,
                right_table,
                left_cursor,
                right_cursor,
            )?;

            // If WHERE condition fails, skip to next inner iteration
            let skip_addr = self.program.len();
            self.program.add_instruction(Instruction::new(
                OpCode::IfZ,
                where_cond_reg,
                0, // Placeholder - will be patched to Next inner
                0,
                None,
                0,
                Some("Skip if WHERE condition false".to_string()),
            ));
            Some(skip_addr)
        } else {
            None
        };

        // Mark match found
        self.program.add_instruction(Instruction::new(
            OpCode::MarkMatch,
            match_reg,
            0,
            0,
            None,
            0,
            Some("Mark that a match was found".to_string()),
        ));

        // Output combined row (matched)
        self.program.add_instruction(Instruction::new(
            OpCode::ResultRow,
            left_start_reg,
            (left_cols.len() + right_cols.len()) as i64,
            0,
            None,
            0,
            Some("Output matched row".to_string()),
        ));

        // Next inner
        let next_inner_addr = self.program.len();
        self.program.add_instruction(Instruction::new(
            OpCode::Next,
            inner_cursor as i64,
            inner_loop_start as i64,
            0,
            None,
            0,
            Some("Next inner row".to_string()),
        ));

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
            // Check match flag - if match found (non-zero), skip NULL row output
            // If no match (zero), continue to NullRow
            // Jump calculation: IfPos (current), NullRow (+1), ResultRow (+2), Next outer (+3)
            // So if match found, skip to +3 (Next outer)
            self.program.add_instruction(Instruction::new(
                OpCode::IfPos,
                match_reg,
                (self.program.len() + 3) as i64, // Jump to Next outer if match found
                0,
                None,
                0,
                Some("Skip NULL row if match found".to_string()),
            ));

            // No match: output NULLs for inner columns
            self.program.add_instruction(Instruction::new(
                OpCode::NullRow,
                inner_start_reg,
                inner_cols.len() as i64,
                0,
                None,
                0,
                Some("Fill inner columns with NULL".to_string()),
            ));

            // Output row with NULLs
            self.program.add_instruction(Instruction::new(
                OpCode::ResultRow,
                left_start_reg,
                (left_cols.len() + right_cols.len()) as i64,
                0,
                None,
                0,
                Some("Output outer row with NULLs for inner".to_string()),
            ));
        }

        // Next outer
        self.program.add_instruction(Instruction::new(
            OpCode::Next,
            outer_cursor as i64,
            outer_loop_start as i64,
            0,
            None,
            0,
            Some("Next outer row".to_string()),
        ));

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
            self.program.add_instruction(Instruction::new(
                OpCode::Rewind,
                right_cursor as i64,
                0, // Placeholder
                0,
                None,
                0,
                Some("Rewind right cursor for second pass".to_string()),
            ));

            let right_loop_start = self.program.len();

            // Load right columns
            for (i, col_idx) in right_cols.iter().enumerate() {
                self.program.add_instruction(Instruction::new(
                    OpCode::Column,
                    right_cursor as i64,
                    *col_idx as i64,
                    right_start_reg + i as i64,
                    None,
                    0,
                    Some(format!(
                        "r[{}] = right.col[{}]",
                        right_start_reg + i as i64,
                        col_idx
                    )),
                ));
            }

            // Reset match flag
            self.program.add_instruction(Instruction::new(
                OpCode::Integer,
                0,
                match_reg,
                0,
                None,
                0,
                Some("Reset match flag for right row".to_string()),
            ));

            // Rewind left cursor to check for matches
            let left_check_end_placeholder = self.program.len();
            self.program.add_instruction(Instruction::new(
                OpCode::RewindInner,
                left_cursor as i64,
                0, // Placeholder - will be patched
                0,
                None,
                0,
                Some("Rewind left cursor to check for matches".to_string()),
            ));

            let left_check_start = self.program.len();

            // Load left columns for condition check
            for (i, col_idx) in left_cols.iter().enumerate() {
                self.program.add_instruction(Instruction::new(
                    OpCode::Column,
                    left_cursor as i64,
                    *col_idx as i64,
                    left_start_reg + i as i64,
                    None,
                    0,
                    Some(format!(
                        "r[{}] = left.col[{}]",
                        left_start_reg + i as i64,
                        col_idx
                    )),
                ));
            }

            // Check join condition
            let condition_reg2 = self.compile_join_condition(
                on_expr,
                left_table,
                right_table,
                left_cursor,
                right_cursor,
            )?;

            // If condition matches, mark it and skip to next right row
            // IfZ skips over MarkMatch if condition is false
            self.program.add_instruction(Instruction::new(
                OpCode::IfZ,
                condition_reg2,
                (self.program.len() + 2) as i64, // Skip MarkMatch
                0,
                None,
                0,
                Some("Skip if condition false".to_string()),
            ));

            // Mark that a match was found
            self.program.add_instruction(Instruction::new(
                OpCode::MarkMatch,
                match_reg,
                0,
                0,
                None,
                0,
                Some("Mark that right row has a match".to_string()),
            ));

            // Next left (continue checking)
            self.program.add_instruction(Instruction::new(
                OpCode::Next,
                left_cursor as i64,
                left_check_start as i64,
                0,
                None,
                0,
                Some("Next left row".to_string()),
            ));

            let left_check_end = self.program.len();

            // After checking all left rows: if no match, output right row with NULLs for left
            // If match found (IfPos), skip to next right row
            self.program.add_instruction(Instruction::new(
                OpCode::IfPos,
                match_reg,
                (self.program.len() + 3) as i64, // Skip NullRow and ResultRow
                0,
                None,
                0,
                Some("Skip if right row had a match".to_string()),
            ));

            // No match: fill left columns with NULL
            self.program.add_instruction(Instruction::new(
                OpCode::NullRow,
                left_start_reg,
                left_cols.len() as i64,
                0,
                None,
                0,
                Some("Fill left columns with NULL".to_string()),
            ));

            // Output row with NULLs for left
            self.program.add_instruction(Instruction::new(
                OpCode::ResultRow,
                left_start_reg,
                (left_cols.len() + right_cols.len()) as i64,
                0,
                None,
                0,
                Some("Output unmatched right row with NULLs".to_string()),
            ));

            // Next right row
            self.program.add_instruction(Instruction::new(
                OpCode::Next,
                right_cursor as i64,
                right_loop_start as i64,
                0,
                None,
                0,
                Some("Next right row".to_string()),
            ));

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
        self.program.add_instruction(Instruction::new(
            OpCode::Close,
            left_cursor as i64,
            0,
            0,
            None,
            0,
            Some("Close left cursor".to_string()),
        ));

        self.program.add_instruction(Instruction::new(
            OpCode::Close,
            right_cursor as i64,
            0,
            0,
            None,
            0,
            Some("Close right cursor".to_string()),
        ));

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
        let (left_cols, right_cols, schema) = self.resolve_join_projection(
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
        for (i, col_idx) in left_cols.iter().enumerate() {
            self.emit(
                OpCode::Column,
                left_cursor as i64,
                *col_idx as i64,
                left_start_reg + i as i64,
                None,
                &format!("r[{}] = left.col[{}]", left_start_reg + i as i64, col_idx),
            );
        }

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
        for (i, col_idx) in right_cols.iter().enumerate() {
            self.emit(
                OpCode::Column,
                right_cursor as i64,
                *col_idx as i64,
                right_start_reg + i as i64,
                None,
                &format!("r[{}] = right.col[{}]", right_start_reg + i as i64, col_idx),
            );
        }

        // Output combined row
        self.emit(
            OpCode::ResultRow,
            left_start_reg,
            (left_cols.len() + right_cols.len()) as i64,
            0,
            None,
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

    /// Compile an implicit join (comma-separated tables in FROM with WHERE filter)
    /// e.g., SELECT * FROM users, orders WHERE users.id = orders.user_id
    /// Supports 2 or more tables with nested loop joins
    pub(crate) fn compile_implicit_join(&mut self, select: &Select) -> SqawkResult<()> {
        let num_tables = select.from.len();
        if num_tables < 2 {
            return Err(SqawkError::UnsupportedSqlFeature(
                "Implicit join requires at least 2 tables".into(),
            ));
        }

        // Collect all table names
        let mut table_names: Vec<String> = Vec::with_capacity(num_tables);
        for from_item in &select.from {
            let table_name = match &from_item.relation {
                sqlparser::ast::TableFactor::Table { name, .. } => self.get_table_name(name)?,
                _ => {
                    return Err(SqawkError::UnsupportedSqlFeature(
                        "Only simple table references supported in implicit join".into(),
                    ))
                }
            };
            if !self.database.has_table(&table_name) {
                return Err(SqawkError::TableNotFound(table_name));
            }
            table_names.push(table_name);
        }

        // Get references to all tables
        let tables: Vec<&Table> = table_names
            .iter()
            .map(|name| self.database.get_table(name))
            .collect::<Result<Vec<_>, _>>()?;

        // Resolve projection for all tables - determine which columns to output
        let (table_col_indices, output_col_refs, schema, is_wildcard) =
            self.resolve_multi_table_projection(&select.projection, &tables, &table_names)?;

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
            let condition_reg =
                self.compile_multi_table_condition(where_expr, &tables, &table_names)?;

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

        // Output combined row (only if WHERE passed)
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

        // Patch skip address to jump to innermost Next instruction
        let innermost_next_addr = self.program.len();
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

        // Patch jump addresses for loop ends
        for (i, loop_end_placeholder) in loop_end_placeholders.iter().enumerate() {
            if let Some(inst) = self.program.instructions.get_mut(*loop_end_placeholder) {
                inst.p2 = loop_ends[i] as i64;
            }
        }

        Ok(())
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

        // Check for SELECT *
        for item in projection {
            if matches!(item, SelectItem::Wildcard(_)) {
                // Return all columns from all tables
                let mut table_cols = Vec::with_capacity(num_tables);
                let mut schema = ResultSchema::new();

                for (i, table) in tables.iter().enumerate() {
                    let cols: Vec<usize> = (0..table.column_count()).collect();
                    for meta in table.column_metadata() {
                        schema.add_column(
                            format!("{}.{}", table_names[i], meta.name),
                            meta.data_type,
                        );
                    }
                    table_cols.push(cols);
                }
                return Ok((table_cols, Vec::new(), schema, true));
            }
        }

        // Handle explicit column selection - track in SELECT order
        let mut output_cols: Vec<(usize, usize)> = Vec::new(); // (table_idx, col_idx)
        let mut schema = ResultSchema::new();

        for item in projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let (tbl_name, col_name) = self.extract_join_column_ref(expr)?;
                    let output_name = format!("{}.{}", tbl_name, col_name);

                    // Find which table this column belongs to
                    let mut found = false;
                    for (i, table_name) in table_names.iter().enumerate() {
                        if tbl_name.eq_ignore_ascii_case(table_name) {
                            let columns = tables[i].columns();
                            if let Some(idx) = columns
                                .iter()
                                .position(|c| c.eq_ignore_ascii_case(&col_name))
                            {
                                output_cols.push((i, idx));
                                schema.add_column(
                                    output_name.clone(),
                                    tables[i].column_metadata()[idx].data_type,
                                );
                                found = true;
                                break;
                            } else {
                                return Err(SqawkError::ColumnNotFound(col_name.clone()));
                            }
                        }
                    }
                    if !found {
                        return Err(SqawkError::TableNotFound(tbl_name));
                    }
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let (tbl_name, col_name) = self.extract_join_column_ref(expr)?;
                    let output_name = alias.value.clone();

                    // Find which table this column belongs to
                    let mut found = false;
                    for (i, table_name) in table_names.iter().enumerate() {
                        if tbl_name.eq_ignore_ascii_case(table_name) {
                            let columns = tables[i].columns();
                            if let Some(idx) = columns
                                .iter()
                                .position(|c| c.eq_ignore_ascii_case(&col_name))
                            {
                                output_cols.push((i, idx));
                                schema.add_column(
                                    output_name.clone(),
                                    tables[i].column_metadata()[idx].data_type,
                                );
                                found = true;
                                break;
                            } else {
                                return Err(SqawkError::ColumnNotFound(col_name.clone()));
                            }
                        }
                    }
                    if !found {
                        return Err(SqawkError::TableNotFound(tbl_name));
                    }
                }
                SelectItem::Wildcard(_) => unreachable!("Handled above"),
                _ => {
                    return Err(SqawkError::UnsupportedSqlFeature(
                        "Unsupported projection item in multi-table JOIN".into(),
                    ));
                }
            }
        }

        Ok((vec![Vec::new(); num_tables], output_cols, schema, false))
    }

    /// Compile a WHERE condition for multi-table joins
    fn compile_multi_table_condition(
        &mut self,
        expr: &Expr,
        tables: &[&Table],
        table_names: &[String],
    ) -> SqawkResult<i64> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                match op {
                    BinaryOperator::And => {
                        // Compile left and right conditions
                        let left_reg =
                            self.compile_multi_table_condition(left, tables, table_names)?;
                        let right_reg =
                            self.compile_multi_table_condition(right, tables, table_names)?;

                        // AND: both must be true (non-zero)
                        // Use IfZ to implement: result = left && right
                        let result_reg = self.allocate_register();

                        // Start with 0
                        self.emit(
                            OpCode::Integer,
                            0,
                            result_reg,
                            0,
                            None,
                            &format!("r[{}] = 0 (AND init)", result_reg),
                        );

                        // If left is 0, skip to end (result stays 0)
                        let skip_addr = self.program.len();
                        self.emit(
                            OpCode::IfZ,
                            left_reg,
                            0, // Will patch
                            0,
                            None,
                            "Skip if left is false",
                        );

                        // If right is 0, skip to end (result stays 0)
                        let skip_addr2 = self.program.len();
                        self.emit(
                            OpCode::IfZ,
                            right_reg,
                            0, // Will patch
                            0,
                            None,
                            "Skip if right is false",
                        );

                        // Both true, set result to 1
                        self.emit(
                            OpCode::Integer,
                            1,
                            result_reg,
                            0,
                            None,
                            &format!("r[{}] = 1 (AND true)", result_reg),
                        );

                        // Patch skip addresses
                        let end_addr = self.program.len();
                        if let Some(inst) = self.program.instructions.get_mut(skip_addr) {
                            inst.p2 = end_addr as i64;
                        }
                        if let Some(inst) = self.program.instructions.get_mut(skip_addr2) {
                            inst.p2 = end_addr as i64;
                        }

                        Ok(result_reg)
                    }
                    BinaryOperator::Or => {
                        // Compile left and right conditions
                        let left_reg =
                            self.compile_multi_table_condition(left, tables, table_names)?;
                        let right_reg =
                            self.compile_multi_table_condition(right, tables, table_names)?;

                        // OR: either must be true
                        let result_reg = self.allocate_register();

                        // Start with 1 (optimistic)
                        self.emit(
                            OpCode::Integer,
                            1,
                            result_reg,
                            0,
                            None,
                            &format!("r[{}] = 1 (OR init)", result_reg),
                        );

                        // If left is non-zero, skip to end (result stays 1)
                        let one_reg = self.allocate_register();
                        self.emit(OpCode::Integer, 1, one_reg, 0, None, "temp = 1");
                        let check_reg = self.allocate_register();
                        self.emit(
                            OpCode::Ge,
                            left_reg,
                            one_reg,
                            check_reg,
                            None,
                            &format!("r[{}] = (left >= 1)", check_reg),
                        );
                        let skip_addr = self.program.len();
                        self.emit(
                            OpCode::IfPos,
                            left_reg,
                            0, // Will patch to end
                            0,
                            None,
                            "Skip if left is true",
                        );

                        // Left was false, check right
                        let skip_addr2 = self.program.len();
                        self.emit(
                            OpCode::IfPos,
                            right_reg,
                            0, // Will patch to end
                            0,
                            None,
                            "Skip if right is true",
                        );

                        // Both false, set result to 0
                        self.emit(
                            OpCode::Integer,
                            0,
                            result_reg,
                            0,
                            None,
                            &format!("r[{}] = 0 (OR false)", result_reg),
                        );

                        // Patch skip addresses
                        let end_addr = self.program.len();
                        if let Some(inst) = self.program.instructions.get_mut(skip_addr) {
                            inst.p2 = end_addr as i64;
                        }
                        if let Some(inst) = self.program.instructions.get_mut(skip_addr2) {
                            inst.p2 = end_addr as i64;
                        }

                        Ok(result_reg)
                    }
                    // Comparison operators
                    BinaryOperator::Eq
                    | BinaryOperator::NotEq
                    | BinaryOperator::Lt
                    | BinaryOperator::LtEq
                    | BinaryOperator::Gt
                    | BinaryOperator::GtEq => {
                        let left_reg = self.allocate_register();
                        let right_reg = self.allocate_register();

                        self.compile_multi_table_operand(left, tables, table_names, left_reg)?;
                        self.compile_multi_table_operand(right, tables, table_names, right_reg)?;

                        let result_reg = self.allocate_register();
                        let opcode = Self::binary_op_to_comparison_opcode(op)?;

                        self.emit(
                            opcode,
                            left_reg,
                            right_reg,
                            result_reg,
                            None,
                            &format!(
                                "r[{}] = (r[{}] {:?} r[{}])",
                                result_reg, left_reg, op, right_reg
                            ),
                        );

                        Ok(result_reg)
                    }
                    _ => Err(SqawkError::UnsupportedSqlFeature(format!(
                        "Unsupported binary operator in multi-table WHERE: {:?}",
                        op
                    ))),
                }
            }
            _ => Err(SqawkError::UnsupportedSqlFeature(format!(
                "Unsupported expression type in multi-table WHERE: {:?}",
                expr
            ))),
        }
    }

    /// Compile an operand for a multi-table condition (column ref or literal)
    fn compile_multi_table_operand(
        &mut self,
        expr: &Expr,
        tables: &[&Table],
        table_names: &[String],
        target_reg: i64,
    ) -> SqawkResult<()> {
        match expr {
            Expr::CompoundIdentifier(parts) => {
                if parts.len() == 2 {
                    let tbl_name = &parts[0].value;
                    let col_name = &parts[1].value;

                    // Find which table this column belongs to
                    for (i, table_name) in table_names.iter().enumerate() {
                        if tbl_name.eq_ignore_ascii_case(table_name) {
                            let columns = tables[i].columns();
                            if let Some(col_idx) = columns
                                .iter()
                                .position(|c| c.eq_ignore_ascii_case(col_name))
                            {
                                self.emit(
                                    OpCode::Column,
                                    i as i64,
                                    col_idx as i64,
                                    target_reg,
                                    None,
                                    &format!("r[{}] = {}.{}", target_reg, table_name, col_name),
                                );
                                return Ok(());
                            } else {
                                return Err(SqawkError::ColumnNotFound(col_name.clone()));
                            }
                        }
                    }
                    Err(SqawkError::TableNotFound(tbl_name.clone()))
                } else {
                    Err(SqawkError::UnsupportedSqlFeature(format!(
                        "Compound identifier with {} parts not supported",
                        parts.len()
                    )))
                }
            }
            Expr::Value(value) => {
                match value {
                    Value::Number(num, _) => {
                        if let Ok(int_val) = num.parse::<i64>() {
                            self.emit(
                                OpCode::Integer,
                                int_val,
                                target_reg,
                                0,
                                None,
                                &format!("r[{}] = {}", target_reg, int_val),
                            );
                        } else {
                            return Err(SqawkError::UnsupportedSqlFeature(format!(
                                "Non-integer literal: {}",
                                num
                            )));
                        }
                    }
                    Value::SingleQuotedString(s) => {
                        self.emit(
                            OpCode::String,
                            0,
                            target_reg,
                            0,
                            Some(s.clone()),
                            &format!("r[{}] = '{}'", target_reg, s),
                        );
                    }
                    _ => {
                        return Err(SqawkError::UnsupportedSqlFeature(format!(
                            "Unsupported literal type: {:?}",
                            value
                        )));
                    }
                }
                Ok(())
            }
            _ => Err(SqawkError::UnsupportedSqlFeature(format!(
                "Unsupported operand type in multi-table condition: {:?}",
                expr
            ))),
        }
    }

    /// Compile an implicit join with LIMIT/OFFSET support
    pub(crate) fn compile_implicit_join_with_limit(
        &mut self,
        select: &Select,
        query: &Query,
    ) -> SqawkResult<()> {
        // For now, only support 2 tables
        if select.from.len() != 2 {
            return Err(SqawkError::UnsupportedSqlFeature(format!(
                "Implicit joins with {} tables and LIMIT not yet supported (max 2)",
                select.from.len()
            )));
        }

        // Get table names
        let left_table_name = match &select.from[0].relation {
            sqlparser::ast::TableFactor::Table { name, .. } => self.get_table_name(name)?,
            _ => {
                return Err(SqawkError::UnsupportedSqlFeature(
                    "Only simple table references supported in implicit join".into(),
                ))
            }
        };

        let right_table_name = match &select.from[1].relation {
            sqlparser::ast::TableFactor::Table { name, .. } => self.get_table_name(name)?,
            _ => {
                return Err(SqawkError::UnsupportedSqlFeature(
                    "Only simple table references supported in implicit join".into(),
                ))
            }
        };

        if !self.database.has_table(&left_table_name) {
            return Err(SqawkError::TableNotFound(left_table_name));
        }
        if !self.database.has_table(&right_table_name) {
            return Err(SqawkError::TableNotFound(right_table_name));
        }

        let left_table = self.database.get_table(&left_table_name)?;
        let right_table = self.database.get_table(&right_table_name)?;

        // Determine which columns to output based on projection
        let (left_cols, right_cols, schema) = self.resolve_join_projection(
            &select.projection,
            left_table,
            right_table,
            &left_table_name,
            &right_table_name,
        )?;

        // Set result schema
        self.program.set_result_schema(schema);

        // Extract LIMIT/OFFSET values
        let (limit_val, offset_val) = self.extract_limit_offset(query)?;

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

        // Set up LIMIT counter register if needed
        let limit_reg = if let Some(limit) = limit_val {
            if limit == 0 {
                // LIMIT 0 optimization: skip entire loop
                let halt_addr = self.program.len();
                self.emit(OpCode::Goto, 0, 0, 0, None, "LIMIT 0: skip to end");
                // We'll patch this to jump to Halt
                Some((0i64, Some(halt_addr))) // Special marker for LIMIT 0
            } else {
                let reg = self.allocate_register();
                self.emit(
                    OpCode::Integer,
                    limit,
                    reg,
                    0,
                    None,
                    &format!("r[{}] = {} (LIMIT counter)", reg, limit),
                );
                Some((reg, None))
            }
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
        for (i, col_idx) in left_cols.iter().enumerate() {
            self.emit(
                OpCode::Column,
                left_cursor as i64,
                *col_idx as i64,
                left_start_reg + i as i64,
                None,
                &format!("r[{}] = left.col[{}]", left_start_reg + i as i64, col_idx),
            );
        }

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
        for (i, col_idx) in right_cols.iter().enumerate() {
            self.emit(
                OpCode::Column,
                right_cursor as i64,
                *col_idx as i64,
                right_start_reg + i as i64,
                None,
                &format!("r[{}] = right.col[{}]", right_start_reg + i as i64, col_idx),
            );
        }

        // Compile WHERE condition to filter rows
        let skip_output_addr = if let Some(where_expr) = &select.selection {
            if self.verbose {
                eprintln!("WHERE comparison: {:?}", where_expr);
            }
            // Use compile_join_condition to handle table.column references
            let condition_reg = self.compile_join_condition(
                where_expr,
                left_table,
                right_table,
                left_cursor,
                right_cursor,
            )?;

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
            // If offset counter > 0, skip to Next right
            Some(addr)
        } else {
            None
        };

        // Output combined row (only if WHERE passed and OFFSET exhausted)
        self.emit(
            OpCode::ResultRow,
            left_start_reg,
            (left_cols.len() + right_cols.len()) as i64,
            0,
            None,
            "Output implicit join row",
        );

        // Handle LIMIT - decrement counter and exit loop if exhausted
        let limit_exit_addr = if let Some((lim_reg, limit_0_addr)) = limit_reg {
            if limit_0_addr.is_some() {
                // LIMIT 0 case - no decrement needed
                None
            } else {
                let addr = self.program.len();
                self.emit(
                    OpCode::DecrJumpZero,
                    lim_reg,
                    0, // Placeholder - will jump to loop end
                    0,
                    None,
                    "Decrement LIMIT counter, exit if zero",
                );
                Some(addr)
            }
        } else {
            None
        };

        // Patch skip address to jump to Next instruction
        let next_right_addr = self.program.len();
        if let Some(skip_addr) = skip_output_addr {
            if let Some(inst) = self.program.instructions.get_mut(skip_addr) {
                inst.p2 = next_right_addr as i64;
            }
        }
        // Patch offset skip to also jump to Next
        if let Some(off_addr) = offset_skip_addr {
            if let Some(inst) = self.program.instructions.get_mut(off_addr) {
                inst.p2 = next_right_addr as i64;
            }
        }

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

        // Patch LIMIT exit to jump past loops
        if let Some(lim_addr) = limit_exit_addr {
            if let Some(inst) = self.program.instructions.get_mut(lim_addr) {
                inst.p2 = left_loop_end as i64;
            }
        }

        // Patch LIMIT 0 Goto to jump to after Close
        if let Some((_, Some(limit_0_addr))) = limit_reg {
            let halt_pos = self.program.len();
            if let Some(inst) = self.program.instructions.get_mut(limit_0_addr) {
                inst.p2 = halt_pos as i64;
            }
        }

        Ok(())
    }

    /// Compile a multi-table JOIN (3+ tables)
    pub(crate) fn compile_multi_join(
        &mut self,
        table_with_joins: &TableWithJoins,
        projection: &[SelectItem],
        where_clause: &Option<Expr>,
    ) -> SqawkResult<()> {
        // Collect all table names
        let first_table_name = match &table_with_joins.relation {
            sqlparser::ast::TableFactor::Table { name, .. } => self.get_table_name(name)?,
            _ => {
                return Err(SqawkError::UnsupportedSqlFeature(
                    "Only simple table scans supported in multi-join".into(),
                ))
            }
        };

        let mut table_names = vec![first_table_name];
        let mut join_conditions: Vec<&Expr> = Vec::new();

        for join in &table_with_joins.joins {
            let table_name = match &join.relation {
                sqlparser::ast::TableFactor::Table { name, .. } => self.get_table_name(name)?,
                _ => {
                    return Err(SqawkError::UnsupportedSqlFeature(
                        "Only simple table scans supported in multi-join".into(),
                    ))
                }
            };

            // Only support INNER JOIN for multi-table joins
            match &join.join_operator {
                JoinOperator::Inner(JoinConstraint::On(expr)) => {
                    join_conditions.push(expr);
                }
                _ => {
                    return Err(SqawkError::UnsupportedSqlFeature(
                        "Only INNER JOIN with ON clause supported in multi-table joins".into(),
                    ))
                }
            }

            table_names.push(table_name);
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
        let (_table_col_indices, output_col_refs, schema, is_wildcard) =
            self.resolve_multi_table_projection(projection, &tables, &table_names)?;

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
            let cond_reg =
                self.compile_multi_join_condition(cond, &table_names, &table_start_regs)?;

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
            let where_reg =
                self.compile_multi_table_condition(where_expr, &tables, &table_names)?;

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

    /// Compile a join condition for multi-table joins
    fn compile_multi_join_condition(
        &mut self,
        expr: &Expr,
        table_names: &[String],
        table_start_regs: &[i64],
    ) -> SqawkResult<i64> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let left_reg =
                    self.compile_multi_join_operand(left, table_names, table_start_regs)?;
                let right_reg =
                    self.compile_multi_join_operand(right, table_names, table_start_regs)?;

                let result_reg = self.allocate_register();

                let opcode = Self::binary_op_to_comparison_opcode(op)?;

                self.emit(
                    opcode,
                    left_reg,
                    right_reg,
                    result_reg,
                    None,
                    "Evaluate join condition",
                );

                Ok(result_reg)
            }
            _ => Err(SqawkError::UnsupportedSqlFeature(format!(
                "Unsupported multi-join condition: {:?}",
                expr
            ))),
        }
    }

    /// Compile an operand for multi-table join conditions
    fn compile_multi_join_operand(
        &mut self,
        expr: &Expr,
        table_names: &[String],
        table_start_regs: &[i64],
    ) -> SqawkResult<i64> {
        match expr {
            Expr::CompoundIdentifier(parts) => {
                if parts.len() == 2 {
                    let table_alias = parts[0].value.to_lowercase();
                    let col_name = parts[1].value.to_lowercase();

                    // Find which table this refers to
                    for (i, name) in table_names.iter().enumerate() {
                        if name.to_lowercase() == table_alias {
                            let table = self.database.get_table(name)?;
                            if let Some(col_idx) = table.column_index(&col_name) {
                                // The column value should already be loaded in register
                                let reg = table_start_regs[i] + col_idx as i64;
                                return Ok(reg);
                            }
                        }
                    }
                }
                Err(SqawkError::UnsupportedSqlFeature(format!(
                    "Could not resolve column reference: {:?}",
                    expr
                )))
            }
            Expr::Identifier(ident) => {
                // Unqualified column name - search all tables
                let col_name = ident.value.to_lowercase();
                for (i, name) in table_names.iter().enumerate() {
                    let table = self.database.get_table(name)?;
                    if let Some(col_idx) = table.column_index(&col_name) {
                        let reg = table_start_regs[i] + col_idx as i64;
                        return Ok(reg);
                    }
                }
                Err(SqawkError::UnsupportedSqlFeature(format!(
                    "Could not find column: {}",
                    col_name
                )))
            }
            Expr::Value(val) => {
                let reg = self.allocate_register();
                match val {
                    sqlparser::ast::Value::Number(n, _) => {
                        if let Ok(i) = n.parse::<i64>() {
                            self.emit(OpCode::Integer, i, reg, 0, None, "Load constant");
                        } else {
                            return Err(SqawkError::UnsupportedSqlFeature(
                                "Only integer constants supported".into(),
                            ));
                        }
                    }
                    sqlparser::ast::Value::SingleQuotedString(s) => {
                        self.emit(
                            OpCode::String,
                            0,
                            reg,
                            0,
                            Some(s.clone()),
                            "Load string constant",
                        );
                    }
                    _ => {
                        return Err(SqawkError::UnsupportedSqlFeature(format!(
                            "Unsupported value type: {:?}",
                            val
                        )))
                    }
                }
                Ok(reg)
            }
            _ => Err(SqawkError::UnsupportedSqlFeature(format!(
                "Unsupported operand in multi-join: {:?}",
                expr
            ))),
        }
    }

    /// Resolve projection for join - returns (left_col_indices, right_col_indices, schema)
    fn resolve_join_projection(
        &self,
        projection: &[SelectItem],
        left_table: &Table,
        right_table: &Table,
        left_table_name: &str,
        right_table_name: &str,
    ) -> SqawkResult<(Vec<usize>, Vec<usize>, ResultSchema)> {
        let left_columns = left_table.columns();
        let right_columns = right_table.columns();
        let left_meta = left_table.column_metadata();
        let right_meta = right_table.column_metadata();

        // For SELECT *, return all columns from both tables
        for item in projection {
            if matches!(item, SelectItem::Wildcard(_)) {
                let left_cols: Vec<usize> = (0..left_table.column_count()).collect();
                let right_cols: Vec<usize> = (0..right_table.column_count()).collect();
                // Build schema with table prefixes and types
                let mut schema = ResultSchema::new();
                for meta in left_meta {
                    schema.add_column(format!("{}.{}", left_table_name, meta.name), meta.data_type);
                }
                for meta in right_meta {
                    schema.add_column(
                        format!("{}.{}", right_table_name, meta.name),
                        meta.data_type,
                    );
                }
                return Ok((left_cols, right_cols, schema));
            }
        }

        // Handle explicit column selection
        let mut left_cols = Vec::new();
        let mut right_cols = Vec::new();
        let mut schema = ResultSchema::new();

        for item in projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let (table_name, col_name) = self.extract_join_column_ref(expr)?;
                    let output_name = format!("{}.{}", table_name, col_name);

                    if table_name.eq_ignore_ascii_case(left_table_name) {
                        if let Some(idx) = left_columns
                            .iter()
                            .position(|c| c.eq_ignore_ascii_case(&col_name))
                        {
                            left_cols.push(idx);
                            schema.add_column(output_name, left_meta[idx].data_type);
                        } else {
                            return Err(SqawkError::ColumnNotFound(col_name.clone()));
                        }
                    } else if table_name.eq_ignore_ascii_case(right_table_name) {
                        if let Some(idx) = right_columns
                            .iter()
                            .position(|c| c.eq_ignore_ascii_case(&col_name))
                        {
                            right_cols.push(idx);
                            schema.add_column(output_name, right_meta[idx].data_type);
                        } else {
                            return Err(SqawkError::ColumnNotFound(col_name.clone()));
                        }
                    } else {
                        return Err(SqawkError::TableNotFound(table_name));
                    }
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let (table_name, col_name) = self.extract_join_column_ref(expr)?;
                    let output_name = alias.value.clone();

                    if table_name.eq_ignore_ascii_case(left_table_name) {
                        if let Some(idx) = left_columns
                            .iter()
                            .position(|c| c.eq_ignore_ascii_case(&col_name))
                        {
                            left_cols.push(idx);
                            schema.add_column(output_name, left_meta[idx].data_type);
                        } else {
                            return Err(SqawkError::ColumnNotFound(col_name.clone()));
                        }
                    } else if table_name.eq_ignore_ascii_case(right_table_name) {
                        if let Some(idx) = right_columns
                            .iter()
                            .position(|c| c.eq_ignore_ascii_case(&col_name))
                        {
                            right_cols.push(idx);
                            schema.add_column(output_name, right_meta[idx].data_type);
                        } else {
                            return Err(SqawkError::ColumnNotFound(col_name.clone()));
                        }
                    } else {
                        return Err(SqawkError::TableNotFound(table_name));
                    }
                }
                SelectItem::Wildcard(_) => unreachable!("Handled above"),
                _ => {
                    return Err(SqawkError::UnsupportedSqlFeature(
                        "Unsupported projection item in JOIN".into(),
                    ));
                }
            }
        }

        Ok((left_cols, right_cols, schema))
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

    /// Compile a join condition expression
    fn compile_join_condition(
        &mut self,
        expr: &Expr,
        left_table: &Table,
        right_table: &Table,
        left_cursor: usize,
        right_cursor: usize,
    ) -> SqawkResult<i64> {
        // Handle a.col = b.col style conditions
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let left_reg = self.compile_join_operand(
                    left,
                    left_table,
                    right_table,
                    left_cursor,
                    right_cursor,
                )?;
                let right_reg = self.compile_join_operand(
                    right,
                    left_table,
                    right_table,
                    left_cursor,
                    right_cursor,
                )?;

                let result_reg = self.allocate_register();

                let opcode = Self::binary_op_to_comparison_opcode(op)?;

                self.program.add_instruction(Instruction::new(
                    opcode,
                    left_reg,
                    right_reg,
                    result_reg,
                    None,
                    0,
                    Some("Evaluate join condition".to_string()),
                ));

                Ok(result_reg)
            }
            _ => Err(SqawkError::UnsupportedSqlFeature(format!(
                "Unsupported join condition expression: {:?}",
                expr
            ))),
        }
    }

    /// Compile a join operand (typically a column reference)
    fn compile_join_operand(
        &mut self,
        expr: &Expr,
        left_table: &Table,
        right_table: &Table,
        left_cursor: usize,
        right_cursor: usize,
    ) -> SqawkResult<i64> {
        match expr {
            Expr::CompoundIdentifier(parts) => {
                // Handle table.column syntax
                if parts.len() == 2 {
                    let table_alias = parts[0].value.to_lowercase();
                    let col_name = parts[1].value.to_lowercase();

                    // Try to find column in left or right table
                    // First check if table alias matches
                    let left_table_name = left_table.name().to_lowercase();
                    let right_table_name = right_table.name().to_lowercase();

                    if table_alias == left_table_name {
                        if let Some(col_idx) = left_table.column_index(&col_name) {
                            let reg = self.allocate_register();
                            self.program.add_instruction(Instruction::new(
                                OpCode::Column,
                                left_cursor as i64,
                                col_idx as i64,
                                reg,
                                None,
                                0,
                                Some(format!("Load {}.{} for join", table_alias, col_name)),
                            ));
                            return Ok(reg);
                        }
                    } else if table_alias == right_table_name {
                        if let Some(col_idx) = right_table.column_index(&col_name) {
                            let reg = self.allocate_register();
                            self.program.add_instruction(Instruction::new(
                                OpCode::Column,
                                right_cursor as i64,
                                col_idx as i64,
                                reg,
                                None,
                                0,
                                Some(format!("Load {}.{} for join", table_alias, col_name)),
                            ));
                            return Ok(reg);
                        }
                    }

                    Err(SqawkError::UnsupportedSqlFeature(format!(
                        "Column {}.{} not found in join tables",
                        table_alias, col_name
                    )))
                } else {
                    Err(SqawkError::UnsupportedSqlFeature(
                        "Expected table.column in join condition".into(),
                    ))
                }
            }
            Expr::Identifier(ident) => {
                // Unqualified column name - try to find in either table
                let col_name = ident.value.to_lowercase();

                // Try left table first
                if let Some(col_idx) = left_table.column_index(&col_name) {
                    let reg = self.allocate_register();
                    self.program.add_instruction(Instruction::new(
                        OpCode::Column,
                        left_cursor as i64,
                        col_idx as i64,
                        reg,
                        None,
                        0,
                        Some(format!("Load {} from left for join", col_name)),
                    ));
                    return Ok(reg);
                }

                // Try right table
                if let Some(col_idx) = right_table.column_index(&col_name) {
                    let reg = self.allocate_register();
                    self.program.add_instruction(Instruction::new(
                        OpCode::Column,
                        right_cursor as i64,
                        col_idx as i64,
                        reg,
                        None,
                        0,
                        Some(format!("Load {} from right for join", col_name)),
                    ));
                    return Ok(reg);
                }

                Err(SqawkError::UnsupportedSqlFeature(format!(
                    "Column {} not found in either join table",
                    col_name
                )))
            }
            Expr::Value(val) => {
                // Literal value in join condition
                let reg = self.allocate_register();
                match val {
                    Value::Number(n, _) => {
                        if let Ok(i) = n.parse::<i64>() {
                            self.program.add_instruction(Instruction::new(
                                OpCode::Integer,
                                i,
                                reg,
                                0,
                                None,
                                0,
                                Some("Load literal for join".to_string()),
                            ));
                        }
                    }
                    Value::SingleQuotedString(s) => {
                        self.program.add_instruction(Instruction::new(
                            OpCode::String,
                            0,
                            reg,
                            0,
                            Some(s.clone()),
                            0,
                            Some("Load string literal for join".to_string()),
                        ));
                    }
                    _ => {}
                }
                Ok(reg)
            }
            _ => Err(SqawkError::UnsupportedSqlFeature(format!(
                "Unsupported join operand: {:?}",
                expr
            ))),
        }
    }
}
