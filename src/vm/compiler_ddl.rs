//! DDL statement compilation (CREATE, DROP, ALTER, TRUNCATE)
//!
//! This module extends SqlCompiler with DDL statement compilation methods.

use sqlparser::ast::{AlterTableOperation, ObjectName, ObjectType};

use super::ast_compat::sql_option_key_value;
use super::bytecode::{spec_escape, OpCode, ResultSchema};
use super::compiler::{InsertTarget, SqlCompiler};
use super::compiler_select::Sink;
use crate::error::{SqawkError, SqawkResult};
use crate::table::{unique_column_names, DataType};

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

        let table_name = self.get_table_name(&names[0])?;

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
        self.patch_jump(init_addr, init_addr + 1);

        Ok(())
    }

    /// Compile an ALTER TABLE statement
    pub(crate) fn compile_alter_table(
        &mut self,
        name: &ObjectName,
        operation: &AlterTableOperation,
    ) -> SqawkResult<()> {
        let table_name = self.get_table_name(name)?;

        // Generate Init
        let init_addr = self.program.len();
        self.emit(OpCode::Init, 0, 0, 0, None, "Start ALTER TABLE");

        // Process the operation
        match operation {
            AlterTableOperation::AddColumn { column_def, .. } => {
                let col_name = column_def.name.value.clone();
                if self
                    .database
                    .get_table(&table_name)?
                    .column_index(&col_name)
                    .is_some()
                {
                    return Err(SqawkError::InvalidSqlQuery(format!(
                        "duplicate column name: {}",
                        col_name
                    )));
                }
                let col_type = Self::sql_type_to_internal(&column_def.data_type.to_string());

                let spec = format!(
                    "{}:{}:{}",
                    spec_escape(&table_name),
                    spec_escape(&col_name),
                    col_type
                );

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
        self.patch_jump(init_addr, init_addr + 1);

        Ok(())
    }

    /// Compile a TRUNCATE TABLE statement
    pub(crate) fn compile_truncate(&mut self, name: &ObjectName) -> SqawkResult<()> {
        let table_name = self.get_table_name(name)?;

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
        self.patch_jump(init_addr, init_addr + 1);

        Ok(())
    }

    /// Compile `CREATE TABLE name AS query`.
    ///
    /// The query is compiled by the ordinary SELECT pipeline, so any query
    /// that can be run can be materialized; its rows go to the new table
    /// through the `Sink::Table` destination. The new table's columns are
    /// the query's result columns, known only once the query is compiled, so
    /// the CreateTable instruction is emitted first and its spec patched in.
    pub(crate) fn compile_create_table_as_select(
        &mut self,
        name: &ObjectName,
        query: &sqlparser::ast::Query,
    ) -> SqawkResult<()> {
        let table_name = self.get_table_name(name)?;
        let create = self.program.len();
        self.emit(
            OpCode::CreateTable,
            0,
            0,
            0,
            None,
            &format!("Create table {}", table_name),
        );
        let cursor = self.alloc_cursor();
        self.emit(
            OpCode::OpenWrite,
            cursor,
            0,
            0,
            Some(table_name.clone()),
            "Open new table for writing",
        );
        self.insert_target = Some(InsertTarget {
            cursor,
            columns: None,
        });
        let schema = self.compile_query_to(query, Sink::Table);
        self.insert_target = None;
        let schema = schema?;
        self.program.set_result_schema(ResultSchema::new());

        // The new table's columns are named as SQLite names them: the
        // query's result names, made unique (`id, id:1`).
        let mut names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
        unique_column_names(&mut names);
        let mut spec = spec_escape(&table_name);
        for (col, name) in schema.columns.iter().zip(&names) {
            let type_str = match col.data_type {
                DataType::Integer => "INTEGER",
                DataType::Float => "REAL",
                DataType::Boolean => "BOOLEAN",
                _ => "TEXT",
            };
            spec.push_str(&format!(":{}:{}", spec_escape(name), type_str));
        }
        spec.push_str("||");
        self.program.instructions[create].p4 = Some(spec.into());
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
        let table_name = self.get_table_name(name)?;

        // Column names are case-insensitive: `x` and `X` are one column.
        for (i, col) in columns.iter().enumerate() {
            let name = &col.name.value;
            if columns[..i]
                .iter()
                .any(|c| c.name.value.eq_ignore_ascii_case(name))
            {
                return Err(SqawkError::InvalidSqlQuery(format!(
                    "duplicate column name: {}",
                    name
                )));
            }
        }

        // Build column specification: "table_name:col1:type1:col2:type2:..."
        let mut spec = spec_escape(&table_name);
        for col in columns {
            spec.push(':');
            spec.push_str(&spec_escape(&col.name.value));
            spec.push(':');
            let type_str = Self::sql_type_to_internal(&col.data_type.to_string());
            spec.push_str(type_str);
        }

        // Get location from hive_formats or direct location
        let file_path = hive_formats
            .as_ref()
            .and_then(|hf| hf.location.clone())
            .or_else(|| location.clone());

        // Extract delimiter from WITH options
        let delimiter = with_options.iter().find_map(|opt| {
            let (key, value) = sql_option_key_value(opt)?;
            if key.to_lowercase() == "delimiter" {
                // sql_option_key_value unwraps the literal, so no quote
                // stripping is needed -- and stripping only single quotes left
                // double-quoted values carrying their quotes.
                return Some(value);
            }
            None
        });

        // Add file path and delimiter to spec: "...|filepath|delimiter"
        spec.push('|');
        if let Some(ref fp) = file_path {
            spec.push_str(&spec_escape(fp));
        }
        spec.push('|');
        if let Some(ref d) = delimiter {
            spec.push_str(&spec_escape(d));
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
        self.patch_jump(init_addr, init_addr + 1);

        Ok(())
    }
}
