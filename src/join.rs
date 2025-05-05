//! Join module for the sqawk query engine
//!
//! This module implements join operations between tables.

use sqlparser::ast::{Expr, JoinOperator};

use crate::error::{SqawkError, SqawkResult};
use crate::table::{Table, Value};

/// Join types supported by sqawk
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum JoinType {
    /// Inner join - returns rows when there is a match in both tables
    Inner,
    /// Left join - returns all rows from the left table and matching rows from the right
    Left,
    /// Right join - returns all rows from the right table and matching rows from the left
    Right,
    /// Full join - returns rows when there is a match in one of the tables
    Full,
    /// Cross join - returns the Cartesian product of rows from both tables
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
            // Default to inner join for other types
            _ => JoinType::Inner,
        }
    }
}

/// Executor for join operations
///
/// This struct handles the execution of different types of joins between tables.
pub struct JoinExecutor {
    // Placeholder for future join state
}

impl JoinExecutor {
    /// Create a new join executor
    pub fn new() -> Self {
        JoinExecutor {}
    }

    /// Execute a join operation between two tables
    ///
    /// # Arguments
    /// * `left` - The left table
    /// * `right` - The right table
    /// * `join_type` - The type of join to perform
    /// * `condition` - Optional join condition (for non-cross joins)
    ///
    /// # Returns
    /// * The resulting joined table
    pub fn execute_join(
        &mut self,
        left: &Table,
        right: &Table,
        join_type: JoinType,
        condition: Option<&Expr>,
    ) -> SqawkResult<Table> {
        match join_type {
            JoinType::Cross => self.execute_cross_join(left, right),
            JoinType::Inner => {
                if let Some(on_expr) = condition {
                    self.execute_inner_join(left, right, on_expr)
                } else {
                    // Inner join without condition is equivalent to cross join
                    self.execute_cross_join(left, right)
                }
            }
            _ => Err(SqawkError::UnsupportedSqlFeature(format!(
                "Join type {:?} is not implemented yet",
                join_type
            ))),
        }
    }

    /// Execute a cross join (Cartesian product)
    ///
    /// # Arguments
    /// * `left` - The left table
    /// * `right` - The right table
    ///
    /// # Returns
    /// * The resulting cross-joined table
    fn execute_cross_join(&self, left: &Table, right: &Table) -> SqawkResult<Table> {
        // Use the Table's cross_join method
        left.cross_join(right)
    }

    /// Execute an inner join with an ON condition
    ///
    /// # Arguments
    /// * `left` - The left table
    /// * `right` - The right table
    /// * `on_expr` - The ON condition expression
    ///
    /// # Returns
    /// * The resulting inner-joined table
    fn execute_inner_join(&self, left: &Table, right: &Table, _on_expr: &Expr) -> SqawkResult<Table> {
        // For now, we'll just do a cross join
        // In a real implementation, we would evaluate the ON condition for each row combination
        
        // Create a new table with combined columns from both tables
        let mut result_columns = Vec::with_capacity(left.column_count() + right.column_count());
        
        // Add qualified columns from left table
        for col in left.columns() {
            result_columns.push(format!("left.{}", col));
        }
        
        // Add qualified columns from right table
        for col in right.columns() {
            result_columns.push(format!("right.{}", col));
        }
        
        let mut result = Table::new("joined", result_columns, None);
        
        // The naive nested loop join approach
        for (_left_idx, left_row) in left.rows().iter().enumerate() {
            for (_right_idx, right_row) in right.rows().iter().enumerate() {
                // Join the rows
                let mut new_row = Vec::with_capacity(left_row.len() + right_row.len());
                
                // Add values from left row
                for value in left_row {
                    new_row.push(value.clone());
                }
                
                // Add values from right row
                for value in right_row {
                    new_row.push(value.clone());
                }
                
                // Add the combined row to the result table
                // TODO: Evaluate ON condition here
                let _ = result.add_row(new_row);
            }
        }
        
        Ok(result)
    }
    
    /// Execute a left outer join
    ///
    /// # Arguments
    /// * `left` - The left table
    /// * `right` - The right table
    /// * `on_expr` - The ON condition expression
    ///
    /// # Returns
    /// * The resulting left-joined table
    fn execute_left_join(&self, left: &Table, right: &Table, _on_expr: &Expr) -> SqawkResult<Table> {
        // Create a new table with combined columns
        let mut result_columns = Vec::with_capacity(left.column_count() + right.column_count());
        
        // Add qualified columns from left table
        for col in left.columns() {
            result_columns.push(format!("left.{}", col));
        }
        
        // Add qualified columns from right table
        for col in right.columns() {
            result_columns.push(format!("right.{}", col));
        }
        
        let mut result = Table::new("joined", result_columns, None);
        
        // Left outer join logic:
        // 1. Match rows from left and right based on the join condition
        // 2. Include all rows from the left table, with NULL values for the right table if no match
        
        for left_row in left.rows().iter() {
            let mut matched = false;
            
            for right_row in right.rows().iter() {
                // TODO: Check if rows match based on the join condition
                // For now, we'll just add all combinations
                let mut new_row = Vec::with_capacity(left_row.len() + right_row.len());
                
                // Add values from left row
                for value in left_row {
                    new_row.push(value.clone());
                }
                
                // Add values from right row
                for value in right_row {
                    new_row.push(value.clone());
                }
                
                let _ = result.add_row(new_row);
                matched = true;
            }
            
            // If no match was found, add the left row with NULL values for the right side
            if !matched {
                let mut new_row = Vec::with_capacity(left_row.len() + right.column_count());
                
                // Add values from left row
                for value in left_row {
                    new_row.push(value.clone());
                }
                
                // Add NULL values for the right side
                for _ in 0..right.column_count() {
                    new_row.push(Value::Null);
                }
                
                let _ = result.add_row(new_row);
            }
        }
        
        Ok(result)
    }
}