//! SQL execution module for sqawk
//!
//! This module handles parsing and executing SQL statements.

use std::collections::HashSet;

use anyhow::Result;
use sqlparser::ast::{SetExpr, Statement, TableWithJoins, Query, Expr, SelectItem, Value as SqlValue};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::csv_handler::CsvHandler;
use crate::error::{SqawkError, SqawkResult};
use crate::table::{Table, Value};

/// SQL statement executor
pub struct SqlExecutor {
    /// CSV handler for managing tables
    csv_handler: CsvHandler,
    
    /// Names of tables that have been modified
    modified_tables: HashSet<String>,
}

impl SqlExecutor {
    /// Create a new SQL executor with the given CSV handler
    pub fn new(csv_handler: CsvHandler) -> Self {
        SqlExecutor {
            csv_handler,
            modified_tables: HashSet::new(),
        }
    }
    
    /// Execute an SQL statement
    ///
    /// Returns Some(Table) for SELECT queries, None for other statements.
    pub fn execute(&mut self, sql: &str) -> SqawkResult<Option<Table>> {
        // Parse the SQL statement
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql)
            .map_err(|e| SqawkError::SqlParseError(e))?;
        
        if statements.is_empty() {
            return Err(SqawkError::InvalidSqlQuery(
                "No SQL statements found".to_string()
            ));
        }
        
        // Execute each statement
        let mut result = None;
        for statement in statements {
            result = self.execute_statement(statement)?;
        }
        
        Ok(result)
    }
    
    /// Execute a single SQL statement
    fn execute_statement(&mut self, statement: Statement) -> SqawkResult<Option<Table>> {
        match statement {
            Statement::Query(query) => {
                self.execute_query(*query)
            },
            Statement::Insert { table_name, columns, source, .. } => {
                self.execute_insert(table_name, columns, source)?;
                Ok(None)
            },
            _ => {
                Err(SqawkError::UnsupportedSqlFeature(
                    format!("Unsupported SQL statement: {:?}", statement)
                ))
            }
        }
    }
    
    /// Execute a SELECT query
    fn execute_query(&self, query: Query) -> SqawkResult<Option<Table>> {
        match *query.body {
            SetExpr::Select(select) => {
                // Get the source table
                if select.from.len() != 1 {
                    return Err(SqawkError::UnsupportedSqlFeature(
                        "Only queries with a single table are supported".to_string()
                    ));
                }
                
                let table_with_joins = &select.from[0];
                let table_name = self.get_table_name(table_with_joins)?;
                let source_table = self.csv_handler.get_table(&table_name)?;
                
                // Determine which columns to include in the result
                let column_indices = self.resolve_select_items(&select.projection, source_table)?;
                
                // Apply projection to get only the requested columns
                let mut result_table = source_table.project(&column_indices)?;
                
                // Apply WHERE clause if present
                if let Some(where_clause) = &select.selection {
                    result_table = self.apply_where_clause(result_table, where_clause)?;
                }
                
                Ok(Some(result_table))
            },
            _ => {
                Err(SqawkError::UnsupportedSqlFeature(
                    "Only simple SELECT statements are supported".to_string()
                ))
            }
        }
    }
    
    /// Execute an INSERT statement
    fn execute_insert(
        &mut self,
        table_name: sqlparser::ast::ObjectName,
        columns: Vec<sqlparser::ast::Ident>,
        source: Box<Query>
    ) -> SqawkResult<()> {
        // Get the target table name
        let table_name = table_name.0.into_iter()
            .map(|i| i.value)
            .collect::<Vec<_>>()
            .join(".");
        
        // Check if the table exists
        let column_count = {
            let table = self.csv_handler.get_table(&table_name)?;
            table.column_count()
        };
        
        // Extract column indices if specified
        let column_indices = if !columns.is_empty() {
            let table = self.csv_handler.get_table(&table_name)?;
            columns.iter()
                .map(|ident| {
                    table.column_index(&ident.value)
                        .ok_or_else(|| SqawkError::ColumnNotFound(ident.value.clone()))
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            (0..column_count).collect()
        };
        
        // Get values to insert
        match *source.body {
            SetExpr::Values(values) => {
                // Process each row of values
                for value_row in &values.rows {
                    if value_row.len() != column_indices.len() {
                        return Err(SqawkError::InvalidSqlQuery(format!(
                            "INSERT statement has {} values but {} columns were specified",
                            value_row.len(),
                            column_indices.len()
                        )));
                    }
                    
                    // Create a full row with NULL values
                    let mut row = vec![Value::Null; column_count];
                    
                    // Fill in the specified columns
                    for (i, expr) in value_row.iter().enumerate() {
                        let col_idx = column_indices[i];
                        row[col_idx] = self.evaluate_expr(expr)?;
                    }
                    
                    // Add the row to the table
                    let table = self.csv_handler.get_table_mut(&table_name)?;
                    table.add_row(row)?;
                }
                
                // Mark the table as modified
                self.modified_tables.insert(table_name);
                
                Ok(())
            },
            // TODO: Support INSERT ... SELECT
            _ => {
                Err(SqawkError::UnsupportedSqlFeature(
                    "Only INSERT ... VALUES is supported".to_string()
                ))
            }
        }
    }
    
    /// Extract the table name from a TableWithJoins
    fn get_table_name(&self, table_with_joins: &TableWithJoins) -> SqawkResult<String> {
        match &table_with_joins.relation {
            sqlparser::ast::TableFactor::Table { name, .. } => {
                Ok(name.0.iter()
                    .map(|i| i.value.clone())
                    .collect::<Vec<_>>()
                    .join("."))
            },
            _ => {
                Err(SqawkError::UnsupportedSqlFeature(
                    "Only simple table references are supported".to_string()
                ))
            }
        }
    }
    
    /// Resolve SELECT items to column indices
    fn resolve_select_items(
        &self,
        items: &[SelectItem],
        table: &Table
    ) -> SqawkResult<Vec<usize>> {
        let mut column_indices = Vec::new();
        
        for item in items {
            match item {
                SelectItem::Wildcard(_) => {
                    // Select all columns
                    for i in 0..table.column_count() {
                        column_indices.push(i);
                    }
                },
                SelectItem::UnnamedExpr(expr) => {
                    // For now, only support direct column references
                    if let Expr::Identifier(ident) = expr {
                        let idx = table.column_index(&ident.value)
                            .ok_or_else(|| SqawkError::ColumnNotFound(ident.value.clone()))?;
                        column_indices.push(idx);
                    } else {
                        return Err(SqawkError::UnsupportedSqlFeature(
                            "Only direct column references are supported in SELECT".to_string()
                        ));
                    }
                },
                SelectItem::ExprWithAlias { .. } => {
                    return Err(SqawkError::UnsupportedSqlFeature(
                        "Column aliases are not supported".to_string()
                    ));
                },
                _ => {
                    return Err(SqawkError::UnsupportedSqlFeature(
                        "Unsupported SELECT item".to_string()
                    ));
                }
            }
        }
        
        Ok(column_indices)
    }
    
    /// Apply a WHERE clause to filter table rows
    fn apply_where_clause(&self, table: Table, where_expr: &Expr) -> SqawkResult<Table> {
        // Create a new table that only includes rows matching the WHERE condition
        let result = table.select(|row| {
            self.evaluate_condition(where_expr, row, &table)
                .unwrap_or(false)
        });
        
        Ok(result)
    }
    
    /// Evaluate a condition expression against a row
    fn evaluate_condition(&self, expr: &Expr, row: &[Value], table: &Table) -> SqawkResult<bool> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_expr_with_row(left, row, table)?;
                let right_val = self.evaluate_expr_with_row(right, row, table)?;
                
                match op {
                    sqlparser::ast::BinaryOperator::Eq => Ok(left_val == right_val),
                    sqlparser::ast::BinaryOperator::NotEq => Ok(left_val != right_val),
                    // Add more operators as needed
                    _ => Err(SqawkError::UnsupportedSqlFeature(
                        format!("Unsupported binary operator: {:?}", op)
                    )),
                }
            },
            Expr::IsNull(expr) => {
                let val = self.evaluate_expr_with_row(expr, row, table)?;
                Ok(val == Value::Null)
            },
            Expr::IsNotNull(expr) => {
                let val = self.evaluate_expr_with_row(expr, row, table)?;
                Ok(val != Value::Null)
            },
            // Add more expression types as needed
            _ => Err(SqawkError::UnsupportedSqlFeature(
                format!("Unsupported WHERE condition: {:?}", expr)
            )),
        }
    }
    
    /// Evaluate an expression to a Value
    fn evaluate_expr(&self, expr: &Expr) -> SqawkResult<Value> {
        match expr {
            Expr::Value(value) => {
                match value {
                    SqlValue::Number(n, _) => {
                        // Try to parse as integer first, then as float
                        if let Ok(i) = n.parse::<i64>() {
                            Ok(Value::Integer(i))
                        } else if let Ok(f) = n.parse::<f64>() {
                            Ok(Value::Float(f))
                        } else {
                            Err(SqawkError::TypeError(
                                format!("Invalid number: {}", n)
                            ))
                        }
                    },
                    SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
                        Ok(Value::String(s.clone()))
                    },
                    SqlValue::Boolean(b) => {
                        Ok(Value::Boolean(*b))
                    },
                    SqlValue::Null => {
                        Ok(Value::Null)
                    },
                    _ => {
                        Err(SqawkError::UnsupportedSqlFeature(
                            format!("Unsupported SQL value: {:?}", value)
                        ))
                    }
                }
            },
            _ => {
                Err(SqawkError::UnsupportedSqlFeature(
                    format!("Unsupported expression: {:?}", expr)
                ))
            }
        }
    }
    
    /// Evaluate an expression with a row context
    fn evaluate_expr_with_row(
        &self,
        expr: &Expr,
        row: &[Value],
        table: &Table
    ) -> SqawkResult<Value> {
        match expr {
            Expr::Identifier(ident) => {
                let idx = table.column_index(&ident.value)
                    .ok_or_else(|| SqawkError::ColumnNotFound(ident.value.clone()))?;
                Ok(row[idx].clone())
            },
            _ => self.evaluate_expr(expr),
        }
    }
    
    /// Save all modified tables back to their source files
    pub fn save_modified_tables(&self) -> Result<()> {
        for table_name in &self.modified_tables {
            self.csv_handler.save_table(table_name)?;
        }
        
        Ok(())
    }
}
