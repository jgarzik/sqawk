//! SQL execution module for sqawk
//!
//! This module handles parsing and executing SQL statements.

use std::collections::HashSet;

use anyhow::Result;
use sqlparser::ast::{
    Assignment, Expr, Join as SqlJoin, Query, SelectItem, SetExpr, 
    Statement, TableFactor, TableWithJoins, Value as SqlValue,
};
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
        let statements =
            Parser::parse_sql(&dialect, sql).map_err(|e| SqawkError::SqlParseError(e))?;

        if statements.is_empty() {
            return Err(SqawkError::InvalidSqlQuery(
                "No SQL statements found".to_string(),
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
            Statement::Query(query) => self.execute_query(*query),
            Statement::Insert {
                table_name,
                columns,
                source,
                ..
            } => {
                self.execute_insert(table_name, columns, source)?;
                Ok(None)
            }
            Statement::Update {
                table,
                assignments,
                selection,
                ..
            } => {
                let updated_count = self.execute_update(table, assignments, selection)?;
                eprintln!("Updated {} rows", updated_count);
                Ok(None)
            }
            Statement::Delete {
                from, selection, ..
            } => {
                if from.len() != 1 {
                    return Err(SqawkError::UnsupportedSqlFeature(
                        "DELETE with multiple tables is not supported".to_string(),
                    ));
                }
                let table_with_joins = &from[0];
                let deleted_count = self.execute_delete(table_with_joins, selection)?;
                eprintln!("Deleted {} rows", deleted_count);
                Ok(None)
            }
            _ => Err(SqawkError::UnsupportedSqlFeature(format!(
                "Unsupported SQL statement: {:?}",
                statement
            ))),
        }
    }

    /// Execute a SELECT query
    fn execute_query(&self, query: Query) -> SqawkResult<Option<Table>> {
        match *query.body {
            SetExpr::Select(select) => {
                if select.from.is_empty() {
                    return Err(SqawkError::InvalidSqlQuery(
                        "SELECT query must have at least one table".to_string(),
                    ));
                }

                // Process the FROM clause to get a table or join result
                let source_table = self.process_from_clause(&select.from)?;

                // Determine which columns to include in the result (for projection)
                let column_indices = self.resolve_select_items(&select.projection, &source_table)?;

                // IMPORTANT: First filter rows if WHERE clause is present
                // We must apply the WHERE clause before projection to ensure all columns
                // needed for filtering are available during the WHERE evaluation
                let filtered_table = if let Some(where_clause) = &select.selection {
                    self.apply_where_clause(source_table, where_clause)?
                } else {
                    // If no WHERE clause, just use the source table as is
                    source_table
                };

                // Then apply projection to get only the requested columns
                // This happens AFTER filtering to ensure WHERE clauses can access all columns
                let result_table = filtered_table.project(&column_indices)?;

                Ok(Some(result_table))
            }
            _ => Err(SqawkError::UnsupportedSqlFeature(
                "Only simple SELECT statements are supported".to_string(),
            )),
        }
    }
    
    /// Process the FROM clause of a SELECT statement
    ///
    /// This function handles both single table references and joins.
    ///
    /// # Arguments
    /// * `from` - The FROM clause items from the SELECT statement
    ///
    /// # Returns
    /// * The resulting table after processing the FROM clause
    fn process_from_clause(&self, from: &[TableWithJoins]) -> SqawkResult<Table> {
        // Start with the first table in the FROM clause
        let first_table_with_joins = &from[0];
        let first_table_name = self.get_table_name(first_table_with_joins)?;
        let mut result_table = self.csv_handler.get_table(&first_table_name)?.clone();
        
        // Handle any joins in the first TableWithJoins
        if !first_table_with_joins.joins.is_empty() {
            eprintln!("Processing {} joins for table {}", first_table_with_joins.joins.len(), first_table_name);
            result_table = self.process_table_joins(&result_table, &first_table_with_joins.joins)?;
        }
        
        // If there are multiple tables in the FROM clause, join them
        // This is the CROSS JOIN case for "FROM table1, table2, ..."
        if from.len() > 1 {
            eprintln!("Processing multiple tables in FROM clause as CROSS JOINs");
            for table_with_joins in &from[1..] {
                let right_table_name = self.get_table_name(table_with_joins)?;
                let right_table = self.csv_handler.get_table(&right_table_name)?;
                
                // Cross join with the current result table
                result_table = result_table.cross_join(right_table)?;
                
                // Process any joins on this table
                if !table_with_joins.joins.is_empty() {
                    eprintln!("Processing {} joins for table {}", table_with_joins.joins.len(), right_table_name);
                    result_table = self.process_table_joins(&result_table, &table_with_joins.joins)?;
                }
            }
        }
        
        Ok(result_table)
    }
    
    /// Process joins for a table
    ///
    /// This function handles all joins specified in the query for one table.
    ///
    /// # Arguments
    /// * `left_table` - The left table for the join
    /// * `joins` - The join specifications
    ///
    /// # Returns
    /// * The resulting table after processing all joins
    fn process_table_joins(&self, left_table: &Table, joins: &[SqlJoin]) -> SqawkResult<Table> {
        let mut result_table = left_table.clone();
        
        for join in joins {
            // Get the right table
            let right_table_name = match &join.relation {
                TableFactor::Table { name, .. } => name
                    .0
                    .iter()
                    .map(|i| i.value.clone())
                    .collect::<Vec<_>>()
                    .join("."),
                _ => {
                    return Err(SqawkError::UnsupportedSqlFeature(
                        "Only simple table references are supported in joins".to_string(),
                    ))
                }
            };
            
            let right_table = self.csv_handler.get_table(&right_table_name)?;
            
            // For now, we only support CROSS JOIN
            // Later we'll implement proper join types and conditions
            result_table = result_table.cross_join(right_table)?;
        }
        
        Ok(result_table)
    }

    /// Execute an INSERT statement
    fn execute_insert(
        &mut self,
        table_name: sqlparser::ast::ObjectName,
        columns: Vec<sqlparser::ast::Ident>,
        source: Box<Query>,
    ) -> SqawkResult<()> {
        // Get the target table name
        let table_name = table_name
            .0
            .into_iter()
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
            columns
                .iter()
                .map(|ident| {
                    table
                        .column_index(&ident.value)
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
            }
            // TODO: Support INSERT ... SELECT
            _ => Err(SqawkError::UnsupportedSqlFeature(
                "Only INSERT ... VALUES is supported".to_string(),
            )),
        }
    }

    /// Execute a DELETE statement
    ///
    /// This function deletes rows from a table based on an optional WHERE condition.
    /// If no WHERE condition is provided, all rows are deleted.
    ///
    /// # Arguments
    /// * `table_with_joins` - The table reference to delete rows from
    /// * `selection` - Optional WHERE clause to filter which rows to delete
    ///
    /// # Returns
    /// * The number of rows that were deleted
    fn execute_delete(
        &mut self,
        table_with_joins: &TableWithJoins,
        selection: Option<Expr>,
    ) -> SqawkResult<usize> {
        // Get the target table name
        let table_name = self.get_table_name(table_with_joins)?;

        // If there's a WHERE clause, we need to precompute which rows match before modifying the table
        if let Some(ref where_expr) = selection {
            eprintln!("Executing DELETE with WHERE clause: {:?}", where_expr);

            // Create a list of row indices to delete
            let table_ref = self.csv_handler.get_table(&table_name)?;

            // Evaluate WHERE condition for each row before modifying the table
            // to avoid borrow checker issues
            let mut rows_to_delete: Vec<usize> = Vec::new();

            for (idx, row) in table_ref.rows().iter().enumerate() {
                if self
                    .evaluate_condition(where_expr, row, table_ref)
                    .unwrap_or(false)
                {
                    rows_to_delete.push(idx);
                }
            }

            // Now get mutable reference and delete the rows
            let table = self.csv_handler.get_table_mut(&table_name)?;

            // If we have rows to delete, create a new set of rows excluding the ones to delete
            if !rows_to_delete.is_empty() {
                let deleted_count = rows_to_delete.len();

                // Create a new row set excluding the rows to delete
                let mut new_rows: Vec<Vec<Value>> =
                    Vec::with_capacity(table.row_count() - deleted_count);

                for (idx, row) in table.rows().iter().enumerate() {
                    if !rows_to_delete.contains(&idx) {
                        new_rows.push(row.clone());
                    }
                }

                table.replace_rows(new_rows);

                // Mark the table as modified
                self.modified_tables.insert(table_name);

                Ok(deleted_count)
            } else {
                // No rows matched the WHERE condition
                Ok(0)
            }
        } else {
            // No WHERE clause means delete all rows
            eprintln!("Executing DELETE (all rows)");

            let table = self.csv_handler.get_table_mut(&table_name)?;
            let deleted_count = table.row_count();

            // Replace with empty row set
            table.replace_rows(Vec::new());

            // Mark the table as modified
            self.modified_tables.insert(table_name);

            Ok(deleted_count)
        }
    }

    /// Extract the table name from a TableWithJoins
    fn get_table_name(&self, table_with_joins: &TableWithJoins) -> SqawkResult<String> {
        match &table_with_joins.relation {
            sqlparser::ast::TableFactor::Table { name, .. } => Ok(name
                .0
                .iter()
                .map(|i| i.value.clone())
                .collect::<Vec<_>>()
                .join(".")),
            _ => Err(SqawkError::UnsupportedSqlFeature(
                "Only simple table references are supported".to_string(),
            )),
        }
    }

    /// Resolve SELECT items to column indices
    fn resolve_select_items(&self, items: &[SelectItem], table: &Table) -> SqawkResult<Vec<usize>> {
        let mut column_indices = Vec::new();

        for item in items {
            match item {
                SelectItem::Wildcard(_) => {
                    // For any wildcard (*), select all columns
                    for i in 0..table.column_count() {
                        column_indices.push(i);
                    }
                }
                SelectItem::UnnamedExpr(expr) => {
                    match expr {
                        // Simple column reference
                        Expr::Identifier(ident) => {
                            // First try as an exact column name match
                            if let Some(idx) = table.column_index(&ident.value) {
                                column_indices.push(idx);
                            } else {
                                // Try as qualified name by checking column patterns
                                let mut found = false;
                                for (i, col) in table.columns().iter().enumerate() {
                                    if col.ends_with(&format!(".{}", ident.value)) {
                                        column_indices.push(i);
                                        found = true;
                                        break;
                                    }
                                }
                                
                                if !found {
                                    return Err(SqawkError::ColumnNotFound(ident.value.clone()));
                                }
                            }
                        },
                        // Qualified column reference (table.column or join_result.table.column)
                        Expr::CompoundIdentifier(parts) => {
                            // Build the fully qualified column name from parts
                            let qualified_name = parts.iter()
                                .map(|ident| ident.value.clone())
                                .collect::<Vec<_>>()
                                .join(".");
                            
                            eprintln!("Looking for qualified column: {}", qualified_name);
                            
                            // Try to find an exact match for the qualified column
                            let mut found = false;
                            for (i, col) in table.columns().iter().enumerate() {
                                eprintln!("Comparing with column: {}", col);
                                if col == &qualified_name {
                                    column_indices.push(i);
                                    found = true;
                                    break;
                                }
                            }
                            
                            // If we didn't find an exact match, try a suffix match
                            // This helps with cases like "users.id" matching "users_orders_cross.users.id"
                            if !found && parts.len() == 2 {
                                let suffix = format!("{}.{}", parts[0].value, parts[1].value);
                                eprintln!("Trying suffix match for: {}", suffix);
                                
                                for (i, col) in table.columns().iter().enumerate() {
                                    if col.ends_with(&suffix) {
                                        eprintln!("Found suffix match: {} contains {}", col, suffix);
                                        column_indices.push(i);
                                        found = true;
                                        break;
                                    }
                                }
                            }
                            
                            if !found {
                                return Err(SqawkError::ColumnNotFound(qualified_name));
                            }
                        },
                        _ => {
                            return Err(SqawkError::UnsupportedSqlFeature(
                                "Only direct column references are supported in SELECT".to_string(),
                            ));
                        }
                    }
                }
                SelectItem::ExprWithAlias { .. } => {
                    return Err(SqawkError::UnsupportedSqlFeature(
                        "Column aliases are not supported".to_string(),
                    ));
                }
                _ => {
                    return Err(SqawkError::UnsupportedSqlFeature(
                        "Unsupported SELECT item".to_string(),
                    ));
                }
            }
        }

        Ok(column_indices)
    }

    /// Apply a WHERE clause to filter table rows
    ///
    /// This function creates a new table containing only rows that match the condition
    /// specified in the WHERE clause. It evaluates the condition for each row and includes
    /// only those rows for which the condition evaluates to true.
    ///
    /// # Arguments
    /// * `table` - The source table to filter
    /// * `where_expr` - The WHERE clause expression to evaluate
    ///
    /// # Returns
    /// * A new table containing only rows that match the condition
    ///
    /// # Important
    /// This function is called before column projection to ensure all columns
    /// needed for the WHERE condition evaluation are available.
    fn apply_where_clause(&self, table: Table, where_expr: &Expr) -> SqawkResult<Table> {
        eprintln!("Applying WHERE clause: {:?}", where_expr);
        eprintln!("Table before filtering: {} rows", table.row_count());
        
        // Print column names for debugging
        eprintln!("Columns in table:");
        for (i, col) in table.columns().iter().enumerate() {
            eprintln!("[{}] {}", i, col);
        }

        // For multi-condition WHERE clauses, analyze them separately for debugging
        if let Expr::BinaryOp { left, op, right } = where_expr {
            if matches!(op, sqlparser::ast::BinaryOperator::And) {
                eprintln!("WHERE clause has AND operator. Left: {:?}, Right: {:?}", left, right);
            }
        }

        // Create a new table that only includes rows matching the WHERE condition
        // by calling the table.select method with a closure that evaluates the condition
        let result = table.select(|row| {
            // For each row, evaluate the WHERE condition expression
            // If evaluation fails (returns an error), default to false (exclude the row)
            let matches = self
                .evaluate_condition(where_expr, row, &table)
                .unwrap_or(false);

            // Debug output for each row evaluation
            eprintln!("Row: {:?}, matches condition: {}", row, matches);
            matches
        });

        // Log the filter results
        eprintln!("Table after filtering: {} rows", result.row_count());

        Ok(result)
    }

    /// Evaluate a condition expression against a row
    ///
    /// This function evaluates SQL WHERE clause conditions against a specific row.
    /// It handles various expression types including binary operations (comparisons),
    /// IS NULL and IS NOT NULL checks.
    ///
    /// # Arguments
    /// * `expr` - The SQL expression to evaluate
    /// * `row` - The row data to evaluate the expression against
    /// * `table` - The table containing column metadata for the row
    ///
    /// # Returns
    /// * `Ok(true)` if the condition matches the row
    /// * `Ok(false)` if the condition doesn't match
    /// * `Err` if there's an error during evaluation (type mismatch, etc.)
    fn evaluate_condition(&self, expr: &Expr, row: &[Value], table: &Table) -> SqawkResult<bool> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                // Handle logical operators (AND, OR) differently from comparison operators
                match op {
                    // AND operator - evaluate both sides and combine results
                    sqlparser::ast::BinaryOperator::And => {
                        eprintln!("Evaluating AND expression");
                        
                        // Evaluate left condition
                        let left_result = self.evaluate_condition(left, row, table)?;
                        eprintln!("Left condition evaluated to: {}", left_result);
                        
                        // Short-circuit - if left is false, don't evaluate right
                        if !left_result {
                            return Ok(false);
                        }
                        
                        // Evaluate right condition only if left was true
                        let right_result = self.evaluate_condition(right, row, table)?;
                        eprintln!("Right condition evaluated to: {}", right_result);
                        
                        Ok(left_result && right_result)
                    },
                    
                    // OR operator - evaluate both sides and combine results
                    sqlparser::ast::BinaryOperator::Or => {
                        eprintln!("Evaluating OR expression");
                        
                        // Evaluate left condition
                        let left_result = self.evaluate_condition(left, row, table)?;
                        eprintln!("Left condition evaluated to: {}", left_result);
                        
                        // Short-circuit - if left is true, don't evaluate right
                        if left_result {
                            return Ok(true);
                        }
                        
                        // Evaluate right condition only if left was false
                        let right_result = self.evaluate_condition(right, row, table)?;
                        eprintln!("Right condition evaluated to: {}", right_result);
                        
                        Ok(left_result || right_result)
                    },
                    
                    // For comparison operators, evaluate the operands to values first
                    _ => {
                        let left_val = self.evaluate_expr_with_row(left, row, table)?;
                        let right_val = self.evaluate_expr_with_row(right, row, table)?;

                        // Debug print the values being compared
                        eprintln!("WHERE comparison: {:?} {:?} {:?}", left_val, op, right_val);

                        // Handle different comparison operators
                        match op {
                            // Equal (=) operator
                            sqlparser::ast::BinaryOperator::Eq => {
                                // Use the Value's implementation of PartialEq which handles type conversions
                                let result = left_val == right_val;
                                eprintln!("Equality result: {}", result);
                                Ok(result)
                            }

                            // Not equal (!=) operator
                            sqlparser::ast::BinaryOperator::NotEq => Ok(left_val != right_val),

                            // Greater than (>) operator
                            sqlparser::ast::BinaryOperator::Gt => {
                                // Handle each type combination separately for correct numeric comparisons
                                match (&left_val, &right_val) {
                                    // Integer-Integer comparison
                                    (Value::Integer(a), Value::Integer(b)) => Ok(a > b),

                                    // Float-Float comparison
                                    (Value::Float(a), Value::Float(b)) => Ok(a > b),

                                    // Integer-Float comparison (convert Integer to Float)
                                    (Value::Integer(a), Value::Float(b)) => Ok((*a as f64) > *b),

                                    // Float-Integer comparison (convert Integer to Float)
                                    (Value::Float(a), Value::Integer(b)) => Ok(*a > (*b as f64)),

                                    // String-String comparison (lexicographic)
                                    (Value::String(a), Value::String(b)) => Ok(a > b),

                                    // Error for incompatible types
                                    _ => Err(SqawkError::TypeError(format!(
                                        "Cannot compare {:?} and {:?} with >",
                                        left_val, right_val
                                    ))),
                                }
                            }

                            // Less than (<) operator
                            sqlparser::ast::BinaryOperator::Lt => match (&left_val, &right_val) {
                                (Value::Integer(a), Value::Integer(b)) => Ok(a < b),
                                (Value::Float(a), Value::Float(b)) => Ok(a < b),
                                (Value::Integer(a), Value::Float(b)) => Ok((*a as f64) < *b),
                                (Value::Float(a), Value::Integer(b)) => Ok(*a < (*b as f64)),
                                (Value::String(a), Value::String(b)) => Ok(a < b),
                                _ => Err(SqawkError::TypeError(format!(
                                    "Cannot compare {:?} and {:?} with <",
                                    left_val, right_val
                                ))),
                            },

                            // Greater than or equal (>=) operator
                            sqlparser::ast::BinaryOperator::GtEq => match (&left_val, &right_val) {
                                (Value::Integer(a), Value::Integer(b)) => Ok(a >= b),
                                (Value::Float(a), Value::Float(b)) => Ok(a >= b),
                                (Value::Integer(a), Value::Float(b)) => Ok((*a as f64) >= *b),
                                (Value::Float(a), Value::Integer(b)) => Ok(*a >= (*b as f64)),
                                (Value::String(a), Value::String(b)) => Ok(a >= b),
                                _ => Err(SqawkError::TypeError(format!(
                                    "Cannot compare {:?} and {:?} with >=",
                                    left_val, right_val
                                ))),
                            },

                            // Less than or equal (<=) operator
                            sqlparser::ast::BinaryOperator::LtEq => match (&left_val, &right_val) {
                                (Value::Integer(a), Value::Integer(b)) => Ok(a <= b),
                                (Value::Float(a), Value::Float(b)) => Ok(a <= b),
                                (Value::Integer(a), Value::Float(b)) => Ok((*a as f64) <= *b),
                                (Value::Float(a), Value::Integer(b)) => Ok(*a <= (*b as f64)),
                                (Value::String(a), Value::String(b)) => Ok(a <= b),
                                _ => Err(SqawkError::TypeError(format!(
                                    "Cannot compare {:?} and {:?} with <=",
                                    left_val, right_val
                                ))),
                            },
                            // Add more operators as needed
                            _ => Err(SqawkError::UnsupportedSqlFeature(format!(
                                "Unsupported binary operator: {:?}",
                                op
                            ))),
                        }
                    }
                }
            }
            Expr::IsNull(expr) => {
                let val = self.evaluate_expr_with_row(expr, row, table)?;
                Ok(val == Value::Null)
            }
            Expr::IsNotNull(expr) => {
                let val = self.evaluate_expr_with_row(expr, row, table)?;
                Ok(val != Value::Null)
            }
            // Add more expression types as needed
            _ => Err(SqawkError::UnsupportedSqlFeature(format!(
                "Unsupported WHERE condition: {:?}",
                expr
            ))),
        }
    }

    /// Evaluate an expression to a Value
    ///
    /// This function evaluates SQL expressions like literals, constants, etc.
    /// and converts them to our internal Value type.
    ///
    /// # Arguments
    /// * `expr` - The SQL expression to evaluate
    ///
    /// # Returns
    /// * `Ok(Value)` - The resulting value after evaluation
    /// * `Err` - If the expression can't be evaluated or contains unsupported features
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
                            Err(SqawkError::TypeError(format!("Invalid number: {}", n)))
                        }
                    }
                    SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
                        Ok(Value::String(s.clone()))
                    }
                    SqlValue::Boolean(b) => Ok(Value::Boolean(*b)),
                    SqlValue::Null => Ok(Value::Null),
                    _ => Err(SqawkError::UnsupportedSqlFeature(format!(
                        "Unsupported SQL value: {:?}",
                        value
                    ))),
                }
            }
            // Handle unary operations like - (negation)
            Expr::UnaryOp { op, expr } => {
                eprintln!("Evaluating UnaryOp: {:?} on expr: {:?}", op, expr);
                let val = self.evaluate_expr(expr)?;
                eprintln!("Value before applying unary operator: {:?}", val);

                match op {
                    sqlparser::ast::UnaryOperator::Minus => {
                        // Apply negation to numeric values
                        match val {
                            Value::Integer(i) => {
                                let result = Value::Integer(-i);
                                eprintln!("Applying negation to integer: {} -> {}", i, -i);
                                Ok(result)
                            }
                            Value::Float(f) => {
                                let result = Value::Float(-f);
                                eprintln!("Applying negation to float: {} -> {}", f, -f);
                                Ok(result)
                            }
                            _ => Err(SqawkError::TypeError(format!(
                                "Cannot apply negation to non-numeric value: {:?}",
                                val
                            ))),
                        }
                    }
                    sqlparser::ast::UnaryOperator::Plus => {
                        // Plus operator doesn't change the value
                        Ok(val)
                    }
                    sqlparser::ast::UnaryOperator::Not => {
                        // Boolean negation
                        match val {
                            Value::Boolean(b) => Ok(Value::Boolean(!b)),
                            _ => Err(SqawkError::TypeError(format!(
                                "Cannot apply NOT to non-boolean value: {:?}",
                                val
                            ))),
                        }
                    }
                    _ => Err(SqawkError::UnsupportedSqlFeature(format!(
                        "Unsupported unary operator: {:?}",
                        op
                    ))),
                }
            }
            _ => Err(SqawkError::UnsupportedSqlFeature(format!(
                "Unsupported expression: {:?}",
                expr
            ))),
        }
    }

    /// Evaluate an expression with a row context
    ///
    /// This function extends `evaluate_expr` to handle expressions that reference
    /// columns in a specific row (e.g., for WHERE clause evaluation). It first
    /// tries to resolve column references in the current row, and if that doesn't apply,
    /// falls back to standard expression evaluation.
    ///
    /// # Arguments
    /// * `expr` - The SQL expression to evaluate
    /// * `row` - The row containing values for column references
    /// * `table` - The table metadata for column name resolution
    ///
    /// # Returns
    /// * `Ok(Value)` - The resolved value from the row or expression
    /// * `Err` - If column resolution fails or expression evaluation fails
    fn evaluate_expr_with_row(
        &self,
        expr: &Expr,
        row: &[Value],
        table: &Table,
    ) -> SqawkResult<Value> {
        match expr {
            // Simple column reference (unqualified)
            Expr::Identifier(ident) => {
                // First try as an exact column name match
                if let Some(idx) = table.column_index(&ident.value) {
                    if idx < row.len() {
                        return Ok(row[idx].clone());
                    } else {
                        return Err(SqawkError::InvalidSqlQuery(format!(
                            "Column index {} out of bounds for row with {} columns",
                            idx,
                            row.len()
                        )));
                    }
                }
                
                // Try to find a matching qualified column (e.g., for "name", match "table.name")
                for (i, col) in table.columns().iter().enumerate() {
                    if col.ends_with(&format!(".{}", ident.value)) {
                        if i < row.len() {
                            return Ok(row[i].clone());
                        }
                    }
                }
                
                // If we got here, the column wasn't found
                Err(SqawkError::ColumnNotFound(ident.value.clone()))
            },
            // Qualified column reference (table.column or join_result.table.column)
            Expr::CompoundIdentifier(parts) => {
                // Build the fully qualified column name from parts
                let qualified_name = parts.iter()
                    .map(|ident| ident.value.clone())
                    .collect::<Vec<_>>()
                    .join(".");
                
                eprintln!("Looking for qualified column in row evaluation: {}", qualified_name);
                
                // Try to find an exact match for the qualified column
                let mut found = false;
                let mut matched_value = Value::Null;
                
                for (i, col) in table.columns().iter().enumerate() {
                    eprintln!("Comparing with column: {}", col);
                    if col == &qualified_name {
                        if i < row.len() {
                            eprintln!("Found exact column match: {}", qualified_name);
                            return Ok(row[i].clone());
                        } else {
                            return Err(SqawkError::InvalidSqlQuery(format!(
                                "Column index {} out of bounds for row with {} columns",
                                i,
                                row.len()
                            )));
                        }
                    }
                }
                
                // If we didn't find an exact match, try a suffix match
                // This helps with cases like "users.id" matching "users_orders_cross.users.id"
                if parts.len() == 2 {
                    let suffix = format!("{}.{}", parts[0].value, parts[1].value);
                    eprintln!("Trying suffix match for: {}", suffix);
                    
                    for (i, col) in table.columns().iter().enumerate() {
                        if col.ends_with(&suffix) {
                            eprintln!("Found suffix match: {} contains {}", col, suffix);
                            if i < row.len() {
                                return Ok(row[i].clone());
                            } else {
                                return Err(SqawkError::InvalidSqlQuery(format!(
                                    "Column index {} out of bounds for row with {} columns",
                                    i,
                                    row.len()
                                )));
                            }
                        }
                    }
                }
                
                // If we got here, the qualified column wasn't found
                Err(SqawkError::ColumnNotFound(qualified_name))
            },
            // Handle other expression types by delegating to the main evaluate_expr function
            _ => self.evaluate_expr(expr),
        }
    }

    /// Execute an UPDATE statement
    ///
    /// This function updates rows in a table based on the assignments and an optional WHERE condition.
    /// If no WHERE condition is provided, all rows are updated.
    ///
    /// # Arguments
    /// * `table` - The table reference to update
    /// * `assignments` - Column assignments to apply
    /// * `selection` - Optional WHERE clause to filter which rows to update
    ///
    /// # Returns
    /// * The number of rows that were updated
    fn execute_update(
        &mut self,
        table: TableWithJoins,
        assignments: Vec<Assignment>,
        selection: Option<Expr>,
    ) -> SqawkResult<usize> {
        // Get the target table name as a string
        let table_name = self.get_table_name(&table)?;
            
        eprintln!("Executing UPDATE on table: {}", table_name);
        
        // Verify the table exists and get necessary info
        let table_ref = self.csv_handler.get_table(&table_name)?;
        
        // Process assignments to get column indices and their new values
        let column_assignments: Vec<(usize, Expr)> = assignments
            .into_iter()
            .map(|assignment| {
                // The id is a Vec<Ident> but we only support simple column references
                if assignment.id.len() != 1 {
                    return Err(SqawkError::UnsupportedSqlFeature(
                        "Compound column identifiers not supported".to_string(),
                    ));
                }
                
                let column_name = assignment.id[0].value.clone();
                
                let column_idx = table_ref
                    .column_index(&column_name)
                    .ok_or_else(|| SqawkError::ColumnNotFound(column_name))?;
                
                // Clone the Expr value since we can't take ownership of it
                Ok((column_idx, assignment.value.clone()))
            })
            .collect::<SqawkResult<Vec<_>>>()?;
            
        // Find rows to update based on WHERE clause
        let mut rows_to_update = Vec::new();
        
        // Determine which rows match the WHERE clause
        if let Some(ref where_expr) = selection {
            eprintln!("Filtering with WHERE clause: {:?}", where_expr);
            
            for (idx, row) in table_ref.rows().iter().enumerate() {
                if self.evaluate_condition(where_expr, row, table_ref).unwrap_or(false) {
                    rows_to_update.push(idx);
                }
            }
        } else {
            // If no WHERE clause, update all rows
            rows_to_update = (0..table_ref.row_count()).collect();
        }
        
        // Compute all values for each assignment before getting a mutable reference
        // This avoids the borrow checker conflict between evaluate_expr and table_mut
        let mut updates = Vec::new();
        
        // Pre-compute all values to be updated
        for row_idx in &rows_to_update {
            for &(col_idx, ref expr) in &column_assignments {
                let value = self.evaluate_expr(expr)?;
                updates.push((*row_idx, col_idx, value));
            }
        }
        
        // Apply updates with a mutable reference, now that all expressions have been evaluated
        let row_count = rows_to_update.len();
        if row_count > 0 {
            let table = self.csv_handler.get_table_mut(&table_name)?;
            
            // Apply all the pre-computed updates
            for (row_idx, col_idx, value) in updates {
                table.update_value(row_idx, col_idx, value)?;
            }
            
            // Mark the table as modified
            self.modified_tables.insert(table_name);
        }
        
        Ok(row_count)
    }

    /// Save all modified tables back to their source files
    ///
    /// This function writes any tables that have been modified during execution
    /// (e.g., through INSERT, UPDATE, or DELETE statements) back to their source CSV files.
    /// Only tables that have been modified will be saved, preserving the original
    /// CSV files if no changes were made.
    ///
    /// # Returns
    /// * `Ok(())` if all modified tables were saved successfully
    /// * `Err` if any error occurs during saving
    pub fn save_modified_tables(&self) -> Result<()> {
        for table_name in &self.modified_tables {
            // Use the CSV handler to write the table back to its source file
            self.csv_handler.save_table(table_name)?;
        }

        Ok(())
    }
}
