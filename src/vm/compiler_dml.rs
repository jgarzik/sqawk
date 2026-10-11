//! DML statement compilation (INSERT, UPDATE, DELETE)
//!
//! This module extends SqlCompiler with DML statement compilation methods.

use sqlparser::ast::{Expr, SetExpr, TableFactor};

use super::ast_compat::assignment_column;
use super::bytecode::{OpCode, ResultSchema};
use super::compiler::{InsertTarget, NameCtx, SqlCompiler};
use super::compiler_select::Sink;
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
                // Reserve the row block, so registers allocated while
                // compiling a value expression cannot alias a column.
                let base_reg = self.allocate_registers(column_count) as usize;
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
                            0,
                            (base_reg + i) as i64,
                            0,
                            None,
                            &format!("Init col {} to NULL", i),
                        );
                    }

                    // Compile each value expression into the appropriate register
                    for (i, expr) in value_row.iter().enumerate() {
                        let col_idx = column_indices[i];
                        let value_reg = base_reg + col_idx;
                        self.code_expr(expr, &NameCtx::empty(), Some(value_reg as i64))?;
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
            }

            // INSERT ... SELECT: any query, compiled by the SELECT pipeline,
            // its rows sent to the table through `Sink::Table`.
            _ => {
                let cursor = self.alloc_cursor();
                self.emit(
                    OpCode::OpenWrite,
                    cursor,
                    0,
                    0,
                    Some(table_name_str.clone()),
                    &format!("Open {} for writing", table_name_str),
                );
                self.insert_target = Some(InsertTarget {
                    cursor,
                    columns: Some((column_count, column_indices.clone())),
                });
                let schema = self.compile_query_to(source, Sink::Table);
                self.insert_target = None;
                let schema = schema?;
                self.program.set_result_schema(ResultSchema::new());
                if schema.columns.len() != column_indices.len() {
                    return Err(SqawkError::InvalidSqlQuery(format!(
                        "INSERT statement has {} values but {} columns were specified",
                        schema.columns.len(),
                        column_indices.len()
                    )));
                }
            }
        }

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
            let cond_reg = self.code_expr(where_expr, &NameCtx::single(table, cursor_idx), None)?;

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
            let cond_reg = self.code_expr(where_expr, &NameCtx::single(table_ref, 0), None)?;

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

        // Replace the row in place.
        //
        // This was DeleteRow followed by InsertRow, and the insert appends, so
        // every updated row jumped to the end of the table -- persisted to the
        // user's file under --write. UpdateRow keeps the row where it is, and
        // it also makes the affected-row count exact instead of inferred from
        // matching insert and delete tallies.
        self.emit(
            OpCode::UpdateRow,
            0,
            base_reg as i64,
            column_count as i64,
            Some(table_name.clone()),
            "Replace row in place",
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
