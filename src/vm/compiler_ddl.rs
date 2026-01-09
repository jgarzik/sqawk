//! DDL statement compilation (CREATE, DROP, ALTER, TRUNCATE)
//!
//! This module extends SqlCompiler with DDL statement compilation methods.

use sqlparser::ast::{
    AlterTableOperation, ObjectName, ObjectType, SelectItem, SetExpr, TableFactor, Value,
};

use super::bytecode::OpCode;
use super::compiler::SqlCompiler;
use crate::error::{SqawkError, SqawkResult};
use crate::table::DataType;

impl<'a> SqlCompiler<'a> {
    /// Compile a DROP statement (DROP TABLE)
    pub(crate) fn compile_drop(
        &mut self,
        object_type: &ObjectType,
        names: &[ObjectName],
        if_exists: bool,
    ) -> SqawkResult<()> {
        // Only support DROP TABLE for now
        if *object_type != ObjectType::Table {
            return Err(SqawkError::UnsupportedSqlFeature(format!(
                "DROP {:?} is not supported, only DROP TABLE",
                object_type
            )));
        }

        if names.is_empty() {
            return Err(SqawkError::InvalidSqlQuery(
                "DROP TABLE requires a table name".to_string(),
            ));
        }

        let table_name = names[0]
            .0
            .iter()
            .map(|i| i.value.clone())
            .collect::<Vec<_>>()
            .join(".");

        // Generate Init
        let init_addr = self.program.len();
        self.emit(OpCode::Init, 0, 0, 0, None, "Start DROP TABLE");

        // Emit DropTable instruction (p1=1 if IF EXISTS)
        self.emit(
            OpCode::DropTable,
            if if_exists { 1 } else { 0 },
            0,
            0,
            Some(table_name.clone()),
            &format!(
                "Drop table {}{}",
                table_name,
                if if_exists { " IF EXISTS" } else { "" }
            ),
        );

        // Halt
        self.emit(OpCode::Halt, 0, 0, 0, None, "End DROP TABLE");

        // Patch init
        self.program.instructions[init_addr].p2 = (init_addr + 1) as i64;

        Ok(())
    }

    /// Compile an ALTER TABLE statement
    pub(crate) fn compile_alter_table(
        &mut self,
        name: &ObjectName,
        operation: &AlterTableOperation,
    ) -> SqawkResult<()> {
        let table_name = name
            .0
            .iter()
            .map(|i| i.value.clone())
            .collect::<Vec<_>>()
            .join(".");

        // Generate Init
        let init_addr = self.program.len();
        self.emit(OpCode::Init, 0, 0, 0, None, "Start ALTER TABLE");

        // Process the operation
        match operation {
            AlterTableOperation::AddColumn { column_def, .. } => {
                let col_name = column_def.name.value.clone();
                let col_type = match column_def.data_type.to_string().to_uppercase().as_str() {
                    "INTEGER" | "INT" => "INTEGER",
                    "REAL" | "FLOAT" | "DOUBLE" => "REAL",
                    "BOOLEAN" | "BOOL" => "BOOLEAN",
                    _ => "TEXT",
                };

                let spec = format!("{}:{}:{}", table_name, col_name, col_type);

                self.emit(
                    OpCode::AlterTableAdd,
                    0,
                    0,
                    0,
                    Some(spec),
                    &format!("Add column {} to {}", col_name, table_name),
                );
            }
            _ => {
                return Err(SqawkError::UnsupportedSqlFeature(format!(
                    "ALTER TABLE operation {:?} is not supported",
                    operation
                )));
            }
        }

        // Halt
        self.emit(OpCode::Halt, 0, 0, 0, None, "End ALTER TABLE");

        // Patch init
        self.program.instructions[init_addr].p2 = (init_addr + 1) as i64;

        Ok(())
    }

    /// Compile a TRUNCATE TABLE statement
    pub(crate) fn compile_truncate(&mut self, name: &ObjectName) -> SqawkResult<()> {
        let table_name = name
            .0
            .iter()
            .map(|i| i.value.clone())
            .collect::<Vec<_>>()
            .join(".");

        // Generate Init
        let init_addr = self.program.len();
        self.emit(OpCode::Init, 0, 0, 0, None, "Start TRUNCATE TABLE");

        self.emit(
            OpCode::Truncate,
            0,
            0,
            0,
            Some(table_name.clone()),
            &format!("Truncate table {}", table_name),
        );

        // Halt
        self.emit(OpCode::Halt, 0, 0, 0, None, "End TRUNCATE TABLE");

        // Patch init
        self.program.instructions[init_addr].p2 = (init_addr + 1) as i64;

        Ok(())
    }

    /// Compile CREATE TABLE ... AS SELECT
    pub(crate) fn compile_create_table_as_select(
        &mut self,
        name: &ObjectName,
        query: &sqlparser::ast::Query,
    ) -> SqawkResult<()> {
        let table_name = name
            .0
            .iter()
            .map(|i| i.value.clone())
            .collect::<Vec<_>>()
            .join(".");

        // First, compile the SELECT query to get the result schema
        // We need to figure out the column types from the SELECT
        // For now, we'll use a simplified approach: compile the query,
        // then create the table with columns inferred from the result schema

        // Generate Init
        let init_addr = self.program.len();
        self.emit(OpCode::Init, 0, 0, 0, None, "Start CREATE TABLE AS SELECT");

        // Get the SELECT to analyze
        let select = match &*query.body {
            SetExpr::Select(s) => s,
            _ => {
                return Err(SqawkError::UnsupportedSqlFeature(
                    "CREATE TABLE AS only supports simple SELECT".to_string(),
                ));
            }
        };

        // Extract source table info to get column types
        let source_table_name = if let Some(from) = select.from.first() {
            match &from.relation {
                TableFactor::Table { name, .. } => self.get_table_name(name)?,
                _ => {
                    return Err(SqawkError::UnsupportedSqlFeature(
                        "CREATE TABLE AS requires a simple table source".to_string(),
                    ));
                }
            }
        } else {
            return Err(SqawkError::InvalidSqlQuery(
                "CREATE TABLE AS SELECT requires a FROM clause".to_string(),
            ));
        };

        let source_table = self.database.get_table(&source_table_name)?;

        // Build column definitions from the SELECT projection
        let mut col_specs = Vec::new();
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let (col_name, col_type) = self.infer_expr_schema(expr, source_table);
                    let type_str = match col_type {
                        DataType::Integer => "INTEGER",
                        DataType::Float => "REAL",
                        DataType::Boolean => "BOOLEAN",
                        _ => "TEXT",
                    };
                    col_specs.push(format!("{}:{}", col_name, type_str));
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let (_, col_type) = self.infer_expr_schema(expr, source_table);
                    let type_str = match col_type {
                        DataType::Integer => "INTEGER",
                        DataType::Float => "REAL",
                        DataType::Boolean => "BOOLEAN",
                        _ => "TEXT",
                    };
                    col_specs.push(format!("{}:{}", alias.value, type_str));
                }
                SelectItem::Wildcard(_) => {
                    // Add all columns from source table
                    for col in source_table.column_metadata() {
                        let type_str = match col.data_type {
                            DataType::Integer => "INTEGER",
                            DataType::Float => "REAL",
                            DataType::Boolean => "BOOLEAN",
                            _ => "TEXT",
                        };
                        col_specs.push(format!("{}:{}", col.name, type_str));
                    }
                }
                _ => {}
            }
        }

        // Build table spec: "table_name:col1:type1:col2:type2:..."
        let mut spec = table_name.clone();
        for col_spec in &col_specs {
            spec.push(':');
            spec.push_str(col_spec);
        }
        // Add empty file path and delimiter
        spec.push_str("||");

        // Emit CreateTable instruction
        self.emit(
            OpCode::CreateTable,
            0,
            0,
            0,
            Some(spec),
            &format!("Create table {}", table_name),
        );

        // Now compile the SELECT and INSERT into the new table
        // Open the new table for writing
        let cursor_idx = 0i64;
        self.emit(
            OpCode::OpenWrite,
            cursor_idx,
            0,
            0,
            Some(table_name.clone()),
            "Open new table for writing",
        );

        // Open source table for reading
        let source_cursor = 1i64;
        self.emit(
            OpCode::OpenRead,
            source_cursor,
            1,
            0,
            Some(source_table_name.clone()),
            "Open source table",
        );

        // Rewind source
        let rewind_addr = self.program.len();
        self.emit(
            OpCode::Rewind,
            source_cursor,
            0,
            0,
            None,
            "Start reading source",
        );

        let loop_start = self.program.len();

        // Handle WHERE clause if present
        let mut skip_insert_addr: Option<usize> = None;
        if let Some(selection) = &select.selection {
            // Compile the WHERE clause condition (returns the result register)
            let cond_reg =
                self.compile_where_condition(selection, source_table, source_cursor as usize)?;

            // If condition is false (zero), skip the insert
            skip_insert_addr = Some(self.program.len());
            self.emit(
                OpCode::IfZ,
                cond_reg,
                0, // Will be patched to point to Next
                0,
                None,
                "Skip row if WHERE is false",
            );
        }

        // Allocate registers for columns
        let start_reg = self.allocate_register();
        let col_count = col_specs.len();
        for _ in 1..col_count {
            self.allocate_register();
        }

        // Load columns from source based on projection
        let mut reg_idx = 0;
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(sqlparser::ast::Expr::Identifier(ident))
                | SelectItem::ExprWithAlias {
                    expr: sqlparser::ast::Expr::Identifier(ident),
                    ..
                } => {
                    if let Some(col_idx) = source_table.column_index(&ident.value) {
                        self.emit(
                            OpCode::Column,
                            source_cursor,
                            col_idx as i64,
                            start_reg + reg_idx as i64,
                            None,
                            &format!("Load column {}", ident.value),
                        );
                        reg_idx += 1;
                    }
                }
                SelectItem::Wildcard(_) => {
                    for col_idx in 0..source_table.column_count() {
                        self.emit(
                            OpCode::Column,
                            source_cursor,
                            col_idx as i64,
                            start_reg + reg_idx as i64,
                            None,
                            "Load column",
                        );
                        reg_idx += 1;
                    }
                }
                _ => {
                    // For other expressions, try to compile them
                    self.compile_where_operand(
                        match item {
                            SelectItem::UnnamedExpr(e) => e,
                            SelectItem::ExprWithAlias { expr, .. } => expr,
                            _ => continue,
                        },
                        source_table,
                        source_cursor as usize,
                        start_reg + reg_idx as i64,
                    )?;
                    reg_idx += 1;
                }
            }
        }

        // Insert row into new table
        self.emit(
            OpCode::InsertRow,
            cursor_idx,
            start_reg,
            col_count as i64,
            None,
            "Insert row into new table",
        );

        // Next row in source
        let next_addr = self.program.len();
        self.emit(
            OpCode::Next,
            source_cursor,
            loop_start as i64,
            0,
            None,
            "Next source row",
        );

        // Patch the skip-insert jump to point to Next
        if let Some(addr) = skip_insert_addr {
            self.program.instructions[addr].p2 = next_addr as i64;
        }

        let after_loop = self.program.len();

        // Patch rewind
        self.program.instructions[rewind_addr].p2 = after_loop as i64;

        // Close cursors
        self.emit(OpCode::Close, cursor_idx, 0, 0, None, "Close new table");
        self.emit(
            OpCode::Close,
            source_cursor,
            0,
            0,
            None,
            "Close source table",
        );

        // Halt
        self.emit(OpCode::Halt, 0, 0, 0, None, "End CREATE TABLE AS SELECT");

        // Patch init
        self.program.instructions[init_addr].p2 = (init_addr + 1) as i64;

        Ok(())
    }

    /// Compile a CREATE TABLE statement
    pub(crate) fn compile_create_table(
        &mut self,
        name: &ObjectName,
        columns: &[sqlparser::ast::ColumnDef],
        hive_formats: &Option<sqlparser::ast::HiveFormat>,
        location: &Option<String>,
        with_options: &[sqlparser::ast::SqlOption],
    ) -> SqawkResult<()> {
        // Extract table name
        let table_name = name
            .0
            .iter()
            .map(|i| i.value.clone())
            .collect::<Vec<_>>()
            .join(".");

        // Build column specification: "table_name:col1:type1:col2:type2:..."
        let mut spec = table_name.clone();
        for col in columns {
            spec.push(':');
            spec.push_str(&col.name.value);
            spec.push(':');
            // Convert SQL type to simple string
            let type_str = match col.data_type.to_string().to_uppercase().as_str() {
                "INTEGER" | "INT" => "INTEGER",
                "REAL" | "FLOAT" | "DOUBLE" => "REAL",
                "BOOLEAN" | "BOOL" => "BOOLEAN",
                _ => "TEXT",
            };
            spec.push_str(type_str);
        }

        // Get location from hive_formats or direct location
        let file_path = hive_formats
            .as_ref()
            .and_then(|hf| hf.location.clone())
            .or_else(|| location.clone());

        // Extract delimiter from WITH options
        let delimiter = with_options.iter().find_map(|opt| {
            if opt.name.value.to_lowercase() == "delimiter" {
                if let Value::SingleQuotedString(s) = &opt.value {
                    return Some(s.clone());
                }
            }
            None
        });

        // Add file path and delimiter to spec: "...|filepath|delimiter"
        spec.push('|');
        if let Some(ref fp) = file_path {
            spec.push_str(fp);
        }
        spec.push('|');
        if let Some(ref d) = delimiter {
            spec.push_str(d);
        }

        // Generate Init
        let init_addr = self.program.len();
        self.emit(OpCode::Init, 0, 0, 0, None, "Start CREATE TABLE");

        // Emit CreateTable instruction
        self.emit(
            OpCode::CreateTable,
            0,
            0,
            0,
            Some(spec),
            &format!("Create table {}", table_name),
        );

        // Halt
        self.emit(OpCode::Halt, 0, 0, 0, None, "End CREATE TABLE");

        // Patch init
        self.program.instructions[init_addr].p2 = (init_addr + 1) as i64;

        Ok(())
    }
}
