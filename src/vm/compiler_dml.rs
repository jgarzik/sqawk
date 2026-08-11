//! DML statement compilation (INSERT, UPDATE, DELETE)
//!
//! This module extends SqlCompiler with DML statement compilation methods.

use sqlparser::ast::{Expr, Select, SelectItem, SetExpr, TableFactor};

use super::ast_compat::assignment_column;
use super::bytecode::OpCode;
use super::compiler::{NameCtx, SqlCompiler};
use crate::error::{SqawkError, SqawkResult};

impl<'a> SqlCompiler<'a> {
    /// Compile an INSERT statement
    pub(crate) fn compile_insert(
        &mut self,
        table_name: &sqlparser::ast::ObjectName,
        columns: &[sqlparser::ast::Ident],
        source: &sqlparser::ast::Query,
    ) -> SqawkResult<()> {
        // Get table name as string
        let table_name_str = self.get_table_name(table_name)?;

        // Get table info
        let table = self.database.get_table(&table_name_str)?;
        let column_count = table.column_count();

        // Determine column indices for insertion
        let column_indices: Vec<usize> = if columns.is_empty() {
            (0..column_count).collect()
        } else {
            columns
                .iter()
                .map(|ident| {
                    table
                        .column_index(&ident.value)
                        .ok_or_else(|| SqawkError::ColumnNotFound(ident.value.clone()))
                })
                .collect::<Result<Vec<_>, _>>()?
        };

        // Handle different source types: VALUES or SELECT
        match &*source.body {
            SetExpr::Values(values) => {
                // INSERT ... VALUES
                // Generate Init and jump
                let init_addr = self.program.len();
                self.emit(OpCode::Init, 0, 0, 0, None, "Start INSERT");

                // Open table for writing (cursor 0)
                self.emit(
                    OpCode::OpenWrite,
                    0,
                    0,
                    0,
                    Some(table_name_str.clone()),
                    &format!("Open {} for writing", table_name_str),
                );

                // Process each row of values
                let base_reg = self.register_counter as usize;
                for value_row in &values.rows {
                    if value_row.len() != column_indices.len() {
                        return Err(SqawkError::InvalidSqlQuery(format!(
                            "INSERT statement has {} values but {} columns were specified",
                            value_row.len(),
                            column_indices.len()
                        )));
                    }

                    // Initialize all columns to NULL
                    for i in 0..column_count {
                        self.emit(
                            OpCode::Null,
                            (base_reg + i) as i64,
                            0,
                            0,
                            None,
                            &format!("Init col {} to NULL", i),
                        );
                    }

                    // Compile each value expression into the appropriate register
                    for (i, expr) in value_row.iter().enumerate() {
                        let col_idx = column_indices[i];
                        let value_reg = base_reg + col_idx;
                        self.compile_expr_into_register(expr, value_reg)?;
                    }

                    // Insert the row
                    self.emit(
                        OpCode::InsertRow,
                        0, // cursor
                        base_reg as i64,
                        column_count as i64,
                        Some(table_name_str.clone()),
                        "Insert row",
                    );
                }

                // Close cursor and halt
                self.emit(OpCode::Close, 0, 0, 0, None, "Close cursor");
                self.emit(OpCode::Halt, 0, 0, 0, None, "End INSERT");

                // Patch init to jump past itself
                self.patch_jump(init_addr, init_addr + 1);

                // Update register counter
                self.register_counter = (base_reg + column_count) as i64;
            }

            SetExpr::Select(select) => {
                // INSERT ... SELECT
                self.compile_insert_select(&table_name_str, column_count, &column_indices, select)?;
            }

            _ => {
                return Err(SqawkError::UnsupportedSqlFeature(
                    "Only INSERT ... VALUES and INSERT ... SELECT are supported".to_string(),
                ));
            }
        }

        Ok(())
    }

    /// Compile INSERT ... SELECT statement
    pub(crate) fn compile_insert_select(
        &mut self,
        target_table_name: &str,
        target_column_count: usize,
        column_indices: &[usize],
        select: &Select,
    ) -> SqawkResult<()> {
        // Get the source table from the SELECT
        if select.from.is_empty() {
            return Err(SqawkError::InvalidSqlQuery(
                "INSERT ... SELECT requires a FROM clause".to_string(),
            ));
        }

        let source_table_name = match &select.from[0].relation {
            TableFactor::Table { name, .. } => self.get_table_name(name)?,
            _ => {
                return Err(SqawkError::UnsupportedSqlFeature(
                    "Only simple table references are supported in INSERT...SELECT".to_string(),
                ))
            }
        };

        let source_table = self.database.get_table(&source_table_name)?;

        // Generate Init
        let init_addr = self.program.len();
        self.emit(OpCode::Init, 0, 0, 0, None, "Start INSERT...SELECT");

        // Open source table for reading (cursor 0)
        self.emit(
            OpCode::OpenRead,
            0,
            0,
            0,
            Some(source_table_name.clone()),
            &format!("Open {} for reading", source_table_name),
        );

        // Open target table for writing (cursor 1)
        self.emit(
            OpCode::OpenWrite,
            1,
            0,
            0,
            Some(target_table_name.to_string()),
            &format!("Open {} for writing", target_table_name),
        );

        // Rewind source cursor
        let rewind_addr = self.program.len();
        self.emit(OpCode::Rewind, 0, 0, 0, None, "Rewind source cursor");

        // Loop body start
        let loop_start = self.program.len();

        // Allocate registers for the row data
        let base_reg = self.register_counter as usize;
        self.register_counter += target_column_count as i64;

        // Initialize all columns to NULL
        for i in 0..target_column_count {
            self.emit(
                OpCode::Null,
                (base_reg + i) as i64,
                0,
                0,
                None,
                &format!("Init col {} to NULL", i),
            );
        }

        // Load columns from SELECT projection
        for (i, proj_item) in select.projection.iter().enumerate() {
            if i >= column_indices.len() {
                break;
            }
            let target_col_idx = column_indices[i];
            let dest_reg = base_reg + target_col_idx;

            match proj_item {
                SelectItem::Wildcard(_) => {
                    // SELECT * - load all columns from source
                    for col_idx in 0..source_table.column_count() {
                        if col_idx < target_column_count {
                            self.emit(
                                OpCode::Column,
                                0,
                                col_idx as i64,
                                (base_reg + col_idx) as i64,
                                None,
                                &format!("Load source col {} to target col {}", col_idx, col_idx),
                            );
                        }
                    }
                    break; // Wildcard handles all columns
                }
                SelectItem::UnnamedExpr(expr) => {
                    // Compile the expression
                    match expr {
                        Expr::Identifier(ident) => {
                            let col_name = ident.value.to_lowercase();
                            if let Some(col_idx) = source_table.column_index(&col_name) {
                                self.emit(
                                    OpCode::Column,
                                    0,
                                    col_idx as i64,
                                    dest_reg as i64,
                                    None,
                                    &format!("Load {} to reg {}", col_name, dest_reg),
                                );
                            }
                        }
                        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                            let col_name = parts[1].value.to_lowercase();
                            if let Some(col_idx) = source_table.column_index(&col_name) {
                                self.emit(
                                    OpCode::Column,
                                    0,
                                    col_idx as i64,
                                    dest_reg as i64,
                                    None,
                                    &format!("Load {} to reg {}", col_name, dest_reg),
                                );
                            }
                        }
                        _ => {
                            // For other expressions, try to compile them
                            self.compile_expr_into_register(expr, dest_reg)?;
                        }
                    }
                }
                SelectItem::ExprWithAlias { expr, .. } => {
                    self.compile_expr_into_register(expr, dest_reg)?;
                }
                _ => {}
            }
        }

        // Check WHERE clause if present
        let skip_addr = if let Some(where_expr) = &select.selection {
            let cond_reg = self.compile_where_condition(where_expr, source_table, 0)?;
            let addr = self.program.len();
            self.emit(
                OpCode::IfZ,
                cond_reg,
                0, // will patch
                0,
                None,
                "Skip if WHERE is false",
            );
            Some(addr)
        } else {
            None
        };

        // Insert the row into target table
        self.emit(
            OpCode::InsertRow,
            1, // target cursor
            base_reg as i64,
            target_column_count as i64,
            Some(target_table_name.to_string()),
            "Insert row into target",
        );

        // Patch skip if present
        if let Some(addr) = skip_addr {
            self.patch_jump(addr, self.program.len());
        }

        // Next row
        self.emit(
            OpCode::Next,
            0, // source cursor
            loop_start as i64,
            0,
            None,
            "Next source row",
        );

        // Patch rewind
        let end_addr = self.program.len();
        self.patch_jump(rewind_addr, end_addr);

        // Close cursors and halt
        self.emit(OpCode::Close, 0, 0, 0, None, "Close source cursor");
        self.emit(OpCode::Close, 1, 0, 0, None, "Close target cursor");
        self.emit(OpCode::Halt, 0, 0, 0, None, "End INSERT...SELECT");

        // Patch init
        self.patch_jump(init_addr, init_addr + 1);

        Ok(())
    }

    /// Compile a DELETE statement
    pub(crate) fn compile_delete(
        &mut self,
        from: &[sqlparser::ast::TableWithJoins],
        selection: Option<&Expr>,
    ) -> SqawkResult<()> {
        if from.len() != 1 {
            return Err(SqawkError::UnsupportedSqlFeature(
                "DELETE with multiple tables is not supported".to_string(),
            ));
        }

        // Get table name
        let table_with_joins = &from[0];
        let table_name = match &table_with_joins.relation {
            TableFactor::Table { name, .. } => self.get_table_name(name)?,
            _ => {
                return Err(SqawkError::UnsupportedSqlFeature(
                    "Only simple table references are supported in DELETE".to_string(),
                ))
            }
        };

        // Get table reference for WHERE condition compilation
        let table = self.database.get_table(&table_name)?;

        // Generate Init
        let init_addr = self.program.len();
        self.emit(OpCode::Init, 0, 0, 0, None, "Start DELETE");

        // Open table for reading (cursor 0)
        let cursor_idx = 0;
        self.emit(
            OpCode::OpenRead,
            cursor_idx,
            0,
            0,
            Some(table_name.clone()),
            &format!("Open {} for reading", table_name),
        );

        // Rewind to first row
        let rewind_addr = self.program.len();
        self.emit(
            OpCode::Rewind,
            cursor_idx,
            0,
            0,
            None,
            "Rewind to first row",
        );

        // Main loop: iterate over rows
        let loop_start = self.program.len();

        // If there's a WHERE clause, check if row matches
        if let Some(where_expr) = selection {
            // Compile WHERE condition using table context
            let cond_reg = self.compile_where_condition(where_expr, table, cursor_idx as usize)?;

            // If condition is false (0), skip to next row
            let skip_addr = self.program.len();
            self.emit(
                OpCode::IfZ,
                cond_reg,
                0, // will patch
                0,
                None,
                "Skip if WHERE is false",
            );

            // Delete the current row
            self.emit(
                OpCode::DeleteRow,
                cursor_idx,
                0,
                0,
                Some(table_name.clone()),
                "Delete matching row",
            );

            // Patch skip to jump to Next
            let next_addr = self.program.len();
            self.patch_jump(skip_addr, next_addr);
        } else {
            // No WHERE clause - delete all rows
            self.emit(
                OpCode::DeleteRow,
                0,
                0,
                0,
                Some(table_name.clone()),
                "Delete row",
            );
        }

        // Next row
        self.emit(
            OpCode::Next,
            0,
            loop_start as i64,
            0,
            None,
            "Move to next row",
        );

        // Patch Rewind to jump here if table is empty
        let end_addr = self.program.len();
        self.patch_jump(rewind_addr, end_addr);

        // Close and halt
        self.emit(OpCode::Close, 0, 0, 0, None, "Close cursor");
        self.emit(OpCode::Halt, 0, 0, 0, None, "End DELETE");

        // Patch init
        self.patch_jump(init_addr, init_addr + 1);

        Ok(())
    }

    /// Compile an UPDATE statement
    pub(crate) fn compile_update(
        &mut self,
        table: &sqlparser::ast::TableWithJoins,
        assignments: &[sqlparser::ast::Assignment],
        selection: Option<&Expr>,
    ) -> SqawkResult<()> {
        // Get table name
        let table_name = match &table.relation {
            TableFactor::Table { name, .. } => self.get_table_name(name)?,
            _ => {
                return Err(SqawkError::UnsupportedSqlFeature(
                    "Only simple table references are supported in UPDATE".to_string(),
                ))
            }
        };

        // Get table info for column lookups
        let table_ref = self.database.get_table(&table_name)?;
        let column_count = table_ref.column_count();

        // Parse assignments to get column indices and expressions
        let mut assignment_map: Vec<(usize, &Expr)> = Vec::new();
        for assignment in assignments {
            // Handle different column identifier formats
            let col_name = assignment_column(assignment)?;

            let col_idx = table_ref
                .column_index(&col_name)
                .ok_or_else(|| SqawkError::ColumnNotFound(col_name.clone()))?;

            assignment_map.push((col_idx, &assignment.value));
        }

        // Generate Init
        let init_addr = self.program.len();
        self.emit(OpCode::Init, 0, 0, 0, None, "Start UPDATE");

        // Open table for reading (cursor 0)
        self.emit(
            OpCode::OpenRead,
            0,
            0,
            0,
            Some(table_name.clone()),
            &format!("Open {} for reading", table_name),
        );

        // Rewind to first row
        let rewind_addr = self.program.len();
        self.emit(OpCode::Rewind, 0, 0, 0, None, "Rewind to first row");

        // Main loop
        let loop_start = self.program.len();

        // Allocate registers for the row data
        let base_reg = self.register_counter as usize;
        self.register_counter += column_count as i64;

        // Load all columns into registers
        for col_idx in 0..column_count {
            self.emit(
                OpCode::Column,
                0, // cursor
                col_idx as i64,
                (base_reg + col_idx) as i64,
                None,
                &format!("Load column {}", col_idx),
            );
        }

        // If there's a WHERE clause, check if row matches
        let skip_addr = if let Some(where_expr) = selection {
            // Compile WHERE condition using table context
            let cond_reg = self.compile_where_condition(where_expr, table_ref, 0)?;

            // If condition is false (0), skip to next row
            let addr = self.program.len();
            self.emit(
                OpCode::IfZ,
                cond_reg,
                0, // will patch
                0,
                None,
                "Skip if WHERE is false",
            );
            Some(addr)
        } else {
            None
        };

        // Apply assignments.
        //
        // The right-hand side is compiled in the row's scope, so a column may
        // appear there: `SET salary = salary + 1`. compile_expr, which this
        // used to call, has no Identifier arm at all and rejected it.
        //
        // Reading through a Column op against the cursor yields the ORIGINAL
        // row value, which is the required semantics -- every RHS is evaluated
        // against the row as it was before any assignment in this statement.
        let update_ctx = NameCtx::single(table_ref, 0);
        for (col_idx, expr) in &assignment_map {
            self.code_expr(expr, &update_ctx, Some((base_reg + *col_idx) as i64))?;
        }

        // Delete old row and insert new row
        self.emit(
            OpCode::DeleteRow,
            0,
            0,
            0,
            Some(table_name.clone()),
            "Delete old row",
        );
        self.emit(
            OpCode::InsertRow,
            0,
            base_reg as i64,
            column_count as i64,
            Some(table_name.clone()),
            "Insert updated row",
        );

        // Patch skip if present
        if let Some(addr) = skip_addr {
            self.patch_jump(addr, self.program.len());
        }

        // Next row
        self.emit(
            OpCode::Next,
            0,
            loop_start as i64,
            0,
            None,
            "Move to next row",
        );

        // Patch Rewind to jump here if table is empty
        let end_addr = self.program.len();
        self.patch_jump(rewind_addr, end_addr);

        // Close and halt
        self.emit(OpCode::Close, 0, 0, 0, None, "Close cursor");
        self.emit(OpCode::Halt, 0, 0, 0, None, "End UPDATE");

        // Patch init
        self.patch_jump(init_addr, init_addr + 1);

        Ok(())
    }
}
