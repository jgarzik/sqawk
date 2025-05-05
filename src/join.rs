//! Join operations module for sqawk
//!
//! This module implements table join operations for combining data from multiple tables.

use std::collections::HashMap;

use sqlparser::ast::{Expr, Join as SqlJoin, JoinConstraint, JoinOperator};

use crate::error::{SqawkError, SqawkResult};
use crate::table::{ColumnRef, Table, Value};

/// Represents possible join types supported by the system
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    /// INNER JOIN - only matching rows from both tables
    Inner,
    /// LEFT JOIN - all rows from left table, matching rows from right (or NULL)
    Left,
    /// RIGHT JOIN - matching rows from left table, all rows from right
    Right,
    /// FULL JOIN - all rows from both tables with NULL for non-matching
    Full,
    /// CROSS JOIN - cartesian product of all rows
    Cross,
}

impl From<&JoinOperator> for JoinType {
    fn from(op: &JoinOperator) -> Self {
        match op {
            JoinOperator::Inner(_) => JoinType::Inner,
            JoinOperator::LeftOuter(_) => JoinType::Left,
            JoinOperator::RightOuter(_) => JoinType::Right,
            JoinOperator::FullOuter(_) => JoinType::Full,
            JoinOperator::CrossJoin => JoinType::Cross,
            // Default to INNER JOIN for unsupported join types
            _ => JoinType::Inner,
        }
    }
}

/// Join executor that handles various join operations between tables
pub struct JoinExecutor {
    /// Evaluation context for join conditions
    context: HashMap<String, Value>,
}

impl JoinExecutor {
    /// Create a new join executor
    pub fn new() -> Self {
        Self {
            context: HashMap::new(),
        }
    }

    /// Execute a join operation between two tables
    ///
    /// # Arguments
    /// * `left` - The left table in the join
    /// * `right` - The right table in the join
    /// * `join_type` - The type of join to perform
    /// * `condition` - The join condition (for ON clause)
    ///
    /// # Returns
    /// * A new table containing the joined data
    pub fn execute_join(
        &mut self,
        left: &Table,
        right: &Table,
        join_type: JoinType,
        condition: Option<&Expr>,
    ) -> SqawkResult<Table> {
        match join_type {
            JoinType::Inner => self.execute_inner_join(left, right, condition),
            JoinType::Cross => self.execute_cross_join(left, right),
            // Start with INNER and CROSS joins for MVP
            _ => Err(SqawkError::UnsupportedSqlFeature(format!(
                "Join type {:?} is not yet supported",
                join_type
            ))),
        }
    }

    /// Execute an INNER JOIN between two tables
    fn execute_inner_join(
        &mut self,
        left: &Table,
        right: &Table,
        condition: Option<&Expr>,
    ) -> SqawkResult<Table> {
        // For INNER JOIN, we need a condition
        let condition = condition.ok_or_else(|| {
            SqawkError::InvalidSqlQuery("INNER JOIN requires ON condition".to_string())
        })?;

        // Create result table with prefixed column names
        let mut columns = Vec::new();
        
        // Add columns from left table with table name prefix
        for col in left.columns() {
            columns.push(format!("{}.{}", left.name(), col));
        }
        
        // Add columns from right table with table name prefix
        for col in right.columns() {
            columns.push(format!("{}.{}", right.name(), col));
        }
        
        // Create a new table to hold the join result
        let mut result = Table::new(&format!("{}_{}_join", left.name(), right.name()), columns, None);
        
        // Perform the join (nested loop join for MVP)
        for (left_idx, left_row) in left.rows().iter().enumerate() {
            for (right_idx, right_row) in right.rows().iter().enumerate() {
                // Clear the context for this row pair
                self.context.clear();
                
                // Add left row values to context
                for (i, col) in left.columns().iter().enumerate() {
                    let col_ref = format!("{}.{}", left.name(), col);
                    let value = left_row.get(i).unwrap_or(&Value::Null).clone();
                    self.context.insert(col_ref, value);
                }
                
                // Add right row values to context
                for (i, col) in right.columns().iter().enumerate() {
                    let col_ref = format!("{}.{}", right.name(), col);
                    let value = right_row.get(i).unwrap_or(&Value::Null).clone();
                    self.context.insert(col_ref, value);
                }
                
                // Evaluate the join condition in this context
                if self.evaluate_join_condition(condition)? {
                    // Condition matched, create combined row
                    let mut new_row = Vec::with_capacity(left.column_count() + right.column_count());
                    
                    // Add values from left row
                    for i in 0..left.column_count() {
                        new_row.push(left_row.get(i).unwrap_or(&Value::Null).clone());
                    }
                    
                    // Add values from right row
                    for i in 0..right.column_count() {
                        new_row.push(right_row.get(i).unwrap_or(&Value::Null).clone());
                    }
                    
                    // Add the combined row to the result table
                    result.add_row(new_row);
                }
            }
        }
        
        Ok(result)
    }

    /// Execute a CROSS JOIN between two tables (cartesian product)
    fn execute_cross_join(&mut self, left: &Table, right: &Table) -> SqawkResult<Table> {
        // Create result table with prefixed column names
        let mut columns = Vec::new();
        
        // Add columns from left table with table name prefix
        for col in left.columns() {
            columns.push(format!("{}.{}", left.name(), col));
        }
        
        // Add columns from right table with table name prefix
        for col in right.columns() {
            columns.push(format!("{}.{}", right.name(), col));
        }
        
        // Create a new table to hold the join result
        let mut result = Table::new(&format!("{}_{}_cross", left.name(), right.name()), columns, None);
        
        // For CROSS JOIN, we include every combination of rows
        for left_row in left.rows() {
            for right_row in right.rows() {
                // Create combined row
                let mut new_row = Vec::with_capacity(left.column_count() + right.column_count());
                
                // Add values from left row
                for i in 0..left.column_count() {
                    new_row.push(left_row.get(i).unwrap_or(&Value::Null).clone());
                }
                
                // Add values from right row
                for i in 0..right.column_count() {
                    new_row.push(right_row.get(i).unwrap_or(&Value::Null).clone());
                }
                
                // Add the combined row to the result table
                result.add_row(new_row);
            }
        }
        
        Ok(result)
    }

    /// Evaluate a join condition expression
    ///
    /// This adapts the SQL executor's condition evaluation for join contexts
    fn evaluate_join_condition(&self, expr: &Expr) -> SqawkResult<bool> {
        // Implementation will be similar to SqlExecutor::evaluate_condition
        // but adapted for the join context
        // This is a simplified placeholder
        match expr {
            Expr::BinaryOp { left, op, right } => {
                // Evaluate left and right expressions
                let left_val = self.evaluate_join_expr(left)?;
                let right_val = self.evaluate_join_expr(right)?;
                
                // Compare based on the operator
                match op {
                    sqlparser::ast::BinaryOperator::Eq => Ok(left_val == right_val),
                    sqlparser::ast::BinaryOperator::NotEq => Ok(left_val != right_val),
                    sqlparser::ast::BinaryOperator::Gt => todo!("Support > operator"),
                    sqlparser::ast::BinaryOperator::Lt => todo!("Support < operator"),
                    sqlparser::ast::BinaryOperator::GtEq => todo!("Support >= operator"),
                    sqlparser::ast::BinaryOperator::LtEq => todo!("Support <= operator"),
                    // Add other operators as needed
                    _ => Err(SqawkError::UnsupportedSqlFeature(format!(
                        "Unsupported operator in join condition: {:?}",
                        op
                    ))),
                }
            }
            // Handle other expression types
            _ => Err(SqawkError::UnsupportedSqlFeature(format!(
                "Unsupported expression in join condition: {:?}",
                expr
            ))),
        }
    }

    /// Evaluate an expression in the join context
    fn evaluate_join_expr(&self, expr: &Expr) -> SqawkResult<Value> {
        match expr {
            Expr::Identifier(ident) => {
                // If it's a simple identifier, first try to find it in context
                if let Some(value) = self.context.get(&ident.value) {
                    Ok(value.clone())
                } else {
                    // Next, try to look for fully qualified version (table.column)
                    for (key, value) in &self.context {
                        if key.ends_with(&format!(".{}", ident.value)) {
                            return Ok(value.clone());
                        }
                    }
                    
                    Err(SqawkError::ColumnNotFound(ident.value.clone()))
                }
            }
            Expr::CompoundIdentifier(parts) => {
                if parts.len() == 2 {
                    // Handle qualified column references (table.column)
                    let table_name = &parts[0].value;
                    let column_name = &parts[1].value;
                    let key = format!("{}.{}", table_name, column_name);
                    
                    if let Some(value) = self.context.get(&key) {
                        Ok(value.clone())
                    } else {
                        Err(SqawkError::ColumnNotFound(key))
                    }
                } else {
                    Err(SqawkError::UnsupportedSqlFeature(
                        "Only table.column identifiers are supported".to_string(),
                    ))
                }
            }
            Expr::Value(val) => match val {
                sqlparser::ast::Value::Number(n, _) => {
                    if let Ok(i) = n.parse::<i64>() {
                        Ok(Value::Integer(i))
                    } else if let Ok(f) = n.parse::<f64>() {
                        Ok(Value::Float(f))
                    } else {
                        Ok(Value::String(n.clone()))
                    }
                }
                sqlparser::ast::Value::SingleQuotedString(s) => Ok(Value::String(s.clone())),
                sqlparser::ast::Value::DoubleQuotedString(s) => Ok(Value::String(s.clone())),
                sqlparser::ast::Value::Boolean(b) => Ok(Value::Boolean(*b)),
                sqlparser::ast::Value::Null => Ok(Value::Null),
                _ => Err(SqawkError::UnsupportedSqlFeature(format!(
                    "Unsupported value type in join condition: {:?}",
                    val
                ))),
            },
            // Add other expression types as needed
            _ => Err(SqawkError::UnsupportedSqlFeature(format!(
                "Unsupported expression type in join condition: {:?}",
                expr
            ))),
        }
    }
}