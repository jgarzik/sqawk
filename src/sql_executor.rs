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
use crate::table::{SortDirection, Table, Value};
use crate::aggregate::AggregateFunction;

/// SQL statement executor
pub struct SqlExecutor {
    /// CSV handler for managing tables
    csv_handler: CsvHandler,

    /// Names of tables that have been modified
    modified_tables: HashSet<String>,
    
    /// Verbose mode flag
    verbose: bool,
}

impl SqlExecutor {
    /// Create a new SQL executor with the given CSV handler and verbose flag
    pub fn new_with_verbose(csv_handler: CsvHandler, verbose: bool) -> Self {
        SqlExecutor {
            csv_handler,
            modified_tables: HashSet::new(),
            verbose,
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
                if self.verbose {
                    eprintln!("Updated {} rows", updated_count);
                }
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
                if self.verbose {
                    eprintln!("Deleted {} rows", deleted_count);
                }
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

                // Check if the query contains any aggregate functions
                let has_aggregates = self.contains_aggregate_functions(&select.projection);

                // Determine which columns to include in the result (for projection)
                if has_aggregates {
                    if self.verbose {
                        eprintln!("Applying aggregate functions");
                    }
                    
                    // IMPORTANT: First filter rows if WHERE clause is present
                    // We must apply the WHERE clause before aggregation to ensure all columns
                    // needed for filtering are available during the WHERE evaluation
                    let filtered_table = if let Some(where_clause) = &select.selection {
                        if self.verbose {
                            eprintln!("WHERE comparison");
                        }
                        self.apply_where_clause(source_table, where_clause)?
                    } else {
                        // If no WHERE clause, just use the source table as is
                        source_table
                    };
                    
                    // Apply aggregation functions to the filtered table
                    let result_table = self.apply_aggregate_functions(&select.projection, &filtered_table)?;
                    
                    // GROUP BY not implemented yet, so all aggregate functions apply to the whole column
                    Ok(Some(result_table))
                } else {
                    // For non-aggregate queries, use the normal column resolution and projection
                    let column_specs = self.resolve_select_items(&select.projection, &source_table)?;

                    // IMPORTANT: First filter rows if WHERE clause is present
                    // We must apply the WHERE clause before projection to ensure all columns
                    // needed for filtering are available during the WHERE evaluation
                    let filtered_table = if let Some(where_clause) = &select.selection {
                        if self.verbose {
                            eprintln!("WHERE comparison");
                        }
                        self.apply_where_clause(source_table, where_clause)?
                    } else {
                        // If no WHERE clause, just use the source table as is
                        source_table
                    };

                    // Then apply projection to get only the requested columns with aliases
                    // This happens AFTER filtering to ensure WHERE clauses can access all columns
                    let mut result_table = filtered_table.project_with_aliases(&column_specs)?;
                    
                    // Apply ORDER BY if present
                    // This needs to happen after projection because we need to sort
                    // using the column indices in the result table, not the source table
                    // In sqlparser 0.36, order_by is Vec<OrderByExpr> not Option<Vec<OrderByExpr>>
                    if !query.order_by.is_empty() {
                        if self.verbose {
                            eprintln!("Applying ORDER BY");
                        }
                        result_table = self.apply_order_by(result_table, &query.order_by)?;
                    }

                    Ok(Some(result_table))
                }
            }
            _ => Err(SqawkError::UnsupportedSqlFeature(
                "Only simple SELECT statements are supported".to_string(),
            )),
        }
    }
    
    /// Apply an ORDER BY clause to sort the result table
    ///
    /// # Arguments
    /// * `table` - The table to sort
    /// * `order_by` - The ORDER BY expressions from the SQL query
    ///
    /// # Returns
    /// * A new sorted table
    fn apply_order_by(&self, table: Table, order_by: &[sqlparser::ast::OrderByExpr]) -> SqawkResult<Table> {
        // Convert ORDER BY expressions to column indices and sort directions
        let mut sort_columns = Vec::new();
        
        for order_expr in order_by {
            // Extract the column index for this ORDER BY expression
            let col_idx = match &order_expr.expr {
                Expr::Identifier(ident) => {
                    // Try to find the column by exact name first
                    match table.column_index(&ident.value) {
                        Some(idx) => idx,
                        None => {
                            // Try with qualified names (table.column) lookup
                            let mut found = false;
                            let mut idx = 0;
                            for (i, col) in table.columns().iter().enumerate() {
                                if col.ends_with(&format!(".{}", ident.value)) {
                                    found = true;
                                    idx = i;
                                    break;
                                }
                            }
                            
                            if found {
                                idx
                            } else {
                                return Err(SqawkError::ColumnNotFound(ident.value.clone()));
                            }
                        }
                    }
                },
                // Handle qualified column reference: table.column
                Expr::CompoundIdentifier(idents) => {
                    if idents.len() != 2 {
                        return Err(SqawkError::UnsupportedSqlFeature(
                            "Only simple qualified column references (table.column) are supported in ORDER BY".to_string(),
                        ));
                    }
                    
                    let table_name = &idents[0].value;
                    let column_name = &idents[1].value;
                    let qualified_name = format!("{}.{}", table_name, column_name);
                    
                    match table.column_index(&qualified_name) {
                        Some(idx) => idx,
                        None => {
                            return Err(SqawkError::ColumnNotFound(qualified_name));
                        }
                    }
                },
                _ => {
                    return Err(SqawkError::UnsupportedSqlFeature(
                        "Only simple column references are supported in ORDER BY".to_string(),
                    ));
                }
            };
            
            // Determine sort direction (ASC/DESC)
            let direction = match order_expr.asc {
                // If asc is None or Some(true), use Ascending
                None | Some(true) => SortDirection::Ascending,
                // If asc is Some(false), use Descending
                Some(false) => SortDirection::Descending,
            };
            
            sort_columns.push((col_idx, direction));
        }
        
        // Sort the table using the calculated columns and directions
        table.sort(sort_columns)
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
            result_table = self.process_table_joins(&result_table, &first_table_with_joins.joins)?;
        }
        
        // If there are multiple tables in the FROM clause, join them
        // This is the CROSS JOIN case for "FROM table1, table2, ..."
        if from.len() > 1 {
            if self.verbose {
                eprintln!("Processing multiple tables in FROM clause as CROSS JOINs");
            }
            for table_with_joins in &from[1..] {
                let right_table_name = self.get_table_name(table_with_joins)?;
                let right_table = self.csv_handler.get_table(&right_table_name)?;
                
                // Cross join with the current result table
                result_table = result_table.cross_join(right_table)?;
                
                // Process any joins on this table
                if !table_with_joins.joins.is_empty() {
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

    /// Resolve SELECT items to column indices and aliases
    /// Resolve the column indices for a SELECT statement
    ///
    /// This function takes the SELECT items from a query and resolves them to
    /// column indices in the source table, along with any aliases.
    ///
    /// # Arguments
    /// * `items` - The SELECT items from the query (columns to select)
    /// * `table` - The source table containing the columns
    ///
    /// # Returns
    /// * A vector of column indices and optional aliases corresponding to the SELECT items
    fn resolve_select_items(&self, items: &[SelectItem], table: &Table) -> SqawkResult<Vec<(usize, Option<String>)>> {
        let mut column_specs = Vec::new();

        for item in items {
            match item {
                SelectItem::Wildcard(_) => {
                    // For wildcard, add all columns without aliases
                    for i in 0..table.column_count() {
                        column_specs.push((i, None));
                    }
                }
                SelectItem::UnnamedExpr(expr) => {
                    match expr {
                        // Simple column reference
                        Expr::Identifier(ident) => {
                            let idx = self.get_column_index_for_select(&ident.value, table)?;
                            column_specs.push((idx, None));
                        },
                        // Qualified column reference (table.column or join_result.table.column)
                        Expr::CompoundIdentifier(parts) => {
                            let idx = self.get_qualified_column_index(parts, table)?;
                            column_specs.push((idx, None));
                        },
                        _ => {
                            return Err(SqawkError::UnsupportedSqlFeature(
                                "Only direct column references are supported in SELECT".to_string(),
                            ));
                        }
                    }
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    // If query has aggregates and we're seeing a function with an alias,
                    // we should let apply_aggregate_functions handle it instead
                    if self.contains_aggregate_functions(&[SelectItem::ExprWithAlias { 
                        expr: expr.clone(), 
                        alias: alias.clone() 
                    }]) {
                        // Skip this item, as it will be handled by apply_aggregate_functions
                        // We add a placeholder that won't be used
                        column_specs.push((0, Some(alias.value.clone())));
                    } else {
                        match &*expr {
                            Expr::Identifier(ident) => {
                                // Simple column reference with alias
                                let idx = self.get_column_index_for_select(&ident.value, table)?;
                                column_specs.push((idx, Some(alias.value.clone())));
                            },
                            Expr::CompoundIdentifier(parts) => {
                                // Qualified column reference (table.column or join_result.table.column) with alias
                                let idx = self.get_qualified_column_index(&parts, table)?;
                                column_specs.push((idx, Some(alias.value.clone())));
                            },
                            _ => {
                                return Err(SqawkError::UnsupportedSqlFeature(
                                    "Only direct column references are supported with aliases in SELECT".to_string(),
                                ));
                            }
                        }
                    }
                }
                _ => {
                    return Err(SqawkError::UnsupportedSqlFeature(
                        "Unsupported SELECT item".to_string(),
                    ));
                }
            }
        }

        Ok(column_specs)
    }
    
    /// Get the column index for a simple column name
    ///
    /// Helper function that centralizes column index resolution for simple column names
    fn get_column_index_for_select(&self, column_name: &str, table: &Table) -> SqawkResult<usize> {
        // First try as an exact column name match
        if let Some(idx) = table.column_index(column_name) {
            return Ok(idx);
        }
        
        // Try as qualified name by checking column patterns
        let suffix = format!(".{}", column_name);
        for (i, col) in table.columns().iter().enumerate() {
            if col.ends_with(&suffix) {
                return Ok(i);
            }
        }
        
        // If we got here, the column wasn't found
        Err(SqawkError::ColumnNotFound(column_name.to_string()))
    }
    
    /// Get the column index for a qualified column reference
    ///
    /// Helper function that centralizes column index resolution for qualified column names
    fn get_qualified_column_index(&self, parts: &[sqlparser::ast::Ident], table: &Table) -> SqawkResult<usize> {
        // Build the fully qualified column name from parts
        let qualified_name = parts.iter()
            .map(|ident| ident.value.clone())
            .collect::<Vec<_>>()
            .join(".");
        
        // Try to find an exact match for the qualified column
        if let Some(idx) = table.column_index(&qualified_name) {
            return Ok(idx);
        }
        
        // If we didn't find an exact match, try a suffix match
        // This helps with cases like "users.id" matching "users_orders_cross.users.id"
        if parts.len() == 2 {
            let suffix = format!("{}.{}", parts[0].value, parts[1].value);
            
            for (i, col) in table.columns().iter().enumerate() {
                if col.ends_with(&suffix) {
                    return Ok(i);
                }
            }
        }
        
        // If we got here, the qualified column wasn't found
        Err(SqawkError::ColumnNotFound(qualified_name))
    }
    
    /// Handle a wildcard (*) in a SELECT statement
    ///
    /// This function adds all column indices from the table to the result
    fn handle_wildcard_select(&self, table: &Table, column_indices: &mut Vec<usize>) {
        // For any wildcard (*), select all columns
        for i in 0..table.column_count() {
            column_indices.push(i);
        }
    }
    
    /// Resolve a simple column reference for a SELECT statement
    ///
    /// This function handles unqualified column references like 'name'
    fn resolve_simple_column_for_select(&self, column_name: &str, table: &Table, column_indices: &mut Vec<usize>) -> SqawkResult<()> {
        // First try as an exact column name match
        if let Some(idx) = table.column_index(column_name) {
            column_indices.push(idx);
            return Ok(());
        }
        
        // Try as qualified name by checking column patterns
        let suffix = format!(".{}", column_name);
        for (i, col) in table.columns().iter().enumerate() {
            if col.ends_with(&suffix) {
                column_indices.push(i);
                return Ok(());
            }
        }
        
        // If we got here, the column wasn't found
        Err(SqawkError::ColumnNotFound(column_name.to_string()))
    }
    
    /// Resolve a qualified column reference for a SELECT statement
    ///
    /// This function handles qualified column references like 'table.column'
    fn resolve_qualified_column_for_select(&self, parts: &[sqlparser::ast::Ident], table: &Table, column_indices: &mut Vec<usize>) -> SqawkResult<()> {
        // Build the fully qualified column name from parts
        let qualified_name = parts.iter()
            .map(|ident| ident.value.clone())
            .collect::<Vec<_>>()
            .join(".");
        
        // Try to find an exact match for the qualified column
        if let Some(idx) = table.column_index(&qualified_name) {
            column_indices.push(idx);
            return Ok(());
        }
        
        // If we didn't find an exact match, try a suffix match
        // This helps with cases like "users.id" matching "users_orders_cross.users.id"
        if parts.len() == 2 {
            let suffix = format!("{}.{}", parts[0].value, parts[1].value);
            
            for (i, col) in table.columns().iter().enumerate() {
                if col.ends_with(&suffix) {
                    column_indices.push(i);
                    return Ok(());
                }
            }
        }
        
        // If we got here, the qualified column wasn't found
        Err(SqawkError::ColumnNotFound(qualified_name))
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
        // Create a new table that only includes rows matching the WHERE condition
        // by calling the table.select method with a closure that evaluates the condition
        let result = table.select(|row| {
            // For each row, evaluate the WHERE condition expression
            // If evaluation fails (returns an error), default to false (exclude the row)
            let matches = self
                .evaluate_condition(where_expr, row, &table)
                .unwrap_or(false);

            matches
        });

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
    /// Evaluate a condition expression to a boolean value
    ///
    /// This is the main entry point for condition evaluation used in WHERE clauses
    fn evaluate_condition(&self, expr: &Expr, row: &[Value], table: &Table) -> SqawkResult<bool> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                // Handle logical operators (AND, OR) differently from comparison operators
                match op {
                    sqlparser::ast::BinaryOperator::And => {
                        self.evaluate_logical_and(left, right, row, table)
                    },
                    sqlparser::ast::BinaryOperator::Or => {
                        self.evaluate_logical_or(left, right, row, table)
                    },
                    // For comparison operators, delegate to a separate function
                    _ => {
                        self.evaluate_comparison(left, op, right, row, table)
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
    
    /// Evaluate a logical AND expression with short-circuit evaluation
    fn evaluate_logical_and(&self, left: &Expr, right: &Expr, row: &[Value], table: &Table) -> SqawkResult<bool> {
        // Evaluate left condition
        let left_result = self.evaluate_condition(left, row, table)?;
        
        // Short-circuit - if left is false, don't evaluate right
        if !left_result {
            return Ok(false);
        }
        
        // Evaluate right condition only if left was true
        let right_result = self.evaluate_condition(right, row, table)?;
        
        Ok(left_result && right_result)
    }
    
    /// Evaluate a logical OR expression with short-circuit evaluation
    fn evaluate_logical_or(&self, left: &Expr, right: &Expr, row: &[Value], table: &Table) -> SqawkResult<bool> {
        // Evaluate left condition
        let left_result = self.evaluate_condition(left, row, table)?;
        
        // Short-circuit - if left is true, don't evaluate right
        if left_result {
            return Ok(true);
        }
        
        // Evaluate right condition only if left was false
        let right_result = self.evaluate_condition(right, row, table)?;
        
        Ok(left_result || right_result)
    }
    
    /// Evaluate a comparison expression between two values
    fn evaluate_comparison(&self, left: &Expr, op: &sqlparser::ast::BinaryOperator, right: &Expr, row: &[Value], table: &Table) -> SqawkResult<bool> {
        let left_val = self.evaluate_expr_with_row(left, row, table)?;
        let right_val = self.evaluate_expr_with_row(right, row, table)?;

        match op {
            // Equal (=) operator
            sqlparser::ast::BinaryOperator::Eq => {
                // Use the Value's implementation of PartialEq which handles type conversions
                Ok(left_val == right_val)
            }

            // Not equal (!=) operator
            sqlparser::ast::BinaryOperator::NotEq => Ok(left_val != right_val),

            // Greater than (>) operator
            sqlparser::ast::BinaryOperator::Gt => {
                self.compare_values_with_operator(&left_val, &right_val, ">")
            }

            // Less than (<) operator
            sqlparser::ast::BinaryOperator::Lt => {
                self.compare_values_with_operator(&left_val, &right_val, "<")
            }

            // Greater than or equal (>=) operator
            sqlparser::ast::BinaryOperator::GtEq => {
                self.compare_values_with_operator(&left_val, &right_val, ">=")
            }

            // Less than or equal (<=) operator
            sqlparser::ast::BinaryOperator::LtEq => {
                self.compare_values_with_operator(&left_val, &right_val, "<=")
            }
            
            // Add more operators as needed
            _ => Err(SqawkError::UnsupportedSqlFeature(format!(
                "Unsupported binary operator: {:?}",
                op
            ))),
        }
    }
    
    /// Compare two values with a specific operator
    ///
    /// Helper function to handle type-specific comparisons
    fn compare_values_with_operator(&self, left_val: &Value, right_val: &Value, op_symbol: &str) -> SqawkResult<bool> {
        match (left_val, right_val) {
            // Integer-Integer comparison
            (Value::Integer(a), Value::Integer(b)) => {
                Ok(match op_symbol {
                    ">" => a > b,
                    "<" => a < b,
                    ">=" => a >= b,
                    "<=" => a <= b,
                    _ => return Err(SqawkError::InvalidSqlQuery(
                        format!("Unexpected operator symbol: {}", op_symbol)
                    )),
                })
            }

            // Float-Float comparison
            (Value::Float(a), Value::Float(b)) => {
                Ok(match op_symbol {
                    ">" => a > b,
                    "<" => a < b,
                    ">=" => a >= b,
                    "<=" => a <= b,
                    _ => return Err(SqawkError::InvalidSqlQuery(
                        format!("Unexpected operator symbol: {}", op_symbol)
                    )),
                })
            }

            // Integer-Float comparison (convert Integer to Float)
            (Value::Integer(a), Value::Float(b)) => {
                let a_float = *a as f64;
                Ok(match op_symbol {
                    ">" => a_float > *b,
                    "<" => a_float < *b,
                    ">=" => a_float >= *b,
                    "<=" => a_float <= *b,
                    _ => return Err(SqawkError::InvalidSqlQuery(
                        format!("Unexpected operator symbol: {}", op_symbol)
                    )),
                })
            }

            // Float-Integer comparison (convert Integer to Float)
            (Value::Float(a), Value::Integer(b)) => {
                let b_float = *b as f64;
                Ok(match op_symbol {
                    ">" => *a > b_float,
                    "<" => *a < b_float,
                    ">=" => *a >= b_float,
                    "<=" => *a <= b_float,
                    _ => return Err(SqawkError::InvalidSqlQuery(
                        format!("Unexpected operator symbol: {}", op_symbol)
                    )),
                })
            }

            // String-String comparison (lexicographic)
            (Value::String(a), Value::String(b)) => {
                Ok(match op_symbol {
                    ">" => a > b,
                    "<" => a < b,
                    ">=" => a >= b,
                    "<=" => a <= b,
                    _ => return Err(SqawkError::InvalidSqlQuery(
                        format!("Unexpected operator symbol: {}", op_symbol)
                    )),
                })
            }

            // Error for incompatible types
            _ => Err(SqawkError::TypeError(format!(
                "Cannot compare {:?} and {:?} with {}",
                left_val, right_val, op_symbol
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
                let val = self.evaluate_expr(expr)?;

                match op {
                    sqlparser::ast::UnaryOperator::Minus => {
                        // Apply negation to numeric values
                        match val {
                            Value::Integer(i) => {
                                let result = Value::Integer(-i);
                                Ok(result)
                            }
                            Value::Float(f) => {
                                let result = Value::Float(-f);
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
                self.resolve_simple_column_reference(&ident.value, row, table)
            },
            // Qualified column reference (table.column or join_result.table.column)
            Expr::CompoundIdentifier(parts) => {
                self.resolve_qualified_column_reference(parts, row, table)
            },
            // Handle other expression types by delegating to the main evaluate_expr function
            _ => self.evaluate_expr(expr),
        }
    }
    
    /// Check if the SELECT items contain any aggregate functions
    ///
    /// # Arguments
    /// * `items` - The SELECT items from the query
    ///
    /// # Returns
    /// * `true` if any of the items contains an aggregate function
    fn contains_aggregate_functions(&self, items: &[SelectItem]) -> bool {
        for item in items {
            match item {
                // Check for aggregate functions in non-aliased expressions
                SelectItem::UnnamedExpr(expr) => {
                    if let Expr::Function(func) = expr {
                        // Check if the function name is one of our supported aggregates
                        let name = func.name.0.first().map(|i| i.value.as_str()).unwrap_or("");
                        if AggregateFunction::from_name(name).is_some() {
                            return true;
                        }
                    }
                },
                // Check for aggregate functions in aliased expressions
                SelectItem::ExprWithAlias { expr, .. } => {
                    if let Expr::Function(func) = &*expr {
                        // Check if the function name is one of our supported aggregates
                        let name = func.name.0.first().map(|i| i.value.as_str()).unwrap_or("");
                        if AggregateFunction::from_name(name).is_some() {
                            return true;
                        }
                    }
                },
                _ => {}
            }
        }
        false
    }
    
    /// Apply aggregate functions to a table
    ///
    /// # Arguments
    /// * `items` - The SELECT items from the query
    /// * `table` - The source table
    ///
    /// # Returns
    /// * A new table with the results of the aggregate functions
    fn apply_aggregate_functions(&self, items: &[SelectItem], table: &Table) -> SqawkResult<Table> {
        let mut result_columns = Vec::new();
        let mut result_values = Vec::new();
        
        // Process each item in the SELECT list
        for item in items {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    // Handle function call
                    if let Expr::Function(func) = expr {
                        let func_name = func.name.0.first().map(|i| i.value.clone()).unwrap_or_default();
                        
                        // Check if this is a supported aggregate function
                        if let Some(agg_func) = AggregateFunction::from_name(&func_name) {
                            // Process the function arguments
                            if func.args.len() != 1 {
                                return Err(SqawkError::InvalidSqlQuery(
                                    format!("{} function requires exactly one argument", func_name)
                                ));
                            }
                            
                            // Get the column values for the function argument
                            let column_values = self.get_values_for_function_arg(&func.args[0], table)?;
                            
                            // Execute the aggregate function
                            let result_value = agg_func.execute(&column_values)?;
                            
                            // Add the result to our output
                            result_columns.push(func_name.clone());
                            result_values.push(result_value);
                        } else {
                            return Err(SqawkError::UnsupportedSqlFeature(
                                format!("Unsupported function: {}", func_name)
                            ));
                        }
                    } else {
                        return Err(SqawkError::UnsupportedSqlFeature(
                            "Only aggregate functions are supported in aggregate queries".to_string()
                        ));
                    }
                },
                SelectItem::ExprWithAlias { expr, alias } => {
                    // Handle function call with alias
                    match &*expr {
                        Expr::Function(func) => {
                            let func_name = func.name.0.first().map(|i| i.value.clone()).unwrap_or_default();
                            
                            // Check if this is a supported aggregate function
                            if let Some(agg_func) = AggregateFunction::from_name(&func_name) {
                                // Process the function arguments
                                if func.args.len() != 1 {
                                    return Err(SqawkError::InvalidSqlQuery(
                                        format!("{} function requires exactly one argument", func_name)
                                    ));
                                }
                                
                                // Get the column values for the function argument
                                let column_values = self.get_values_for_function_arg(&func.args[0], table)?;
                                
                                // Execute the aggregate function
                                let result_value = agg_func.execute(&column_values)?;
                                
                                // Add the result to our output with the alias
                                result_columns.push(alias.value.clone());
                                result_values.push(result_value);
                            } else {
                                return Err(SqawkError::UnsupportedSqlFeature(
                                    format!("Unsupported function: {}", func_name)
                                ));
                            }
                        },
                        _ => {
                            return Err(SqawkError::UnsupportedSqlFeature(
                                "Only aggregate functions are supported in aggregate queries".to_string()
                            ));
                        }
                    }
                },
                SelectItem::Wildcard(_) => {
                    return Err(SqawkError::UnsupportedSqlFeature(
                        "Wildcard (*) is not supported in queries with aggregate functions".to_string()
                    ));
                },
                _ => {
                    return Err(SqawkError::UnsupportedSqlFeature(
                        "Unsupported SELECT item in aggregate query".to_string()
                    ));
                }
            }
        }
        
        // Create a new table with a single row containing the aggregate results
        let mut result_table = Table::new("aggregate_result", result_columns, None);
        result_table.add_row(result_values)?;
        Ok(result_table)
    }
    
    /// Get values for a function argument
    ///
    /// # Arguments
    /// * `arg` - The function argument
    /// * `table` - The source table
    ///
    /// # Returns
    /// * A vector of values from the specified column or all values for COUNT(*)
    fn get_values_for_function_arg(&self, arg: &sqlparser::ast::FunctionArg, table: &Table) -> SqawkResult<Vec<Value>> {
        match arg {
            sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Wildcard) => {
                // For COUNT(*), return a list of non-null placeholders, one for each row
                Ok(table.rows().iter().map(|_| Value::Integer(1)).collect())
            },
            sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(expr)) => {
                match expr {
                    Expr::Identifier(ident) => {
                        // Get column index
                        let col_idx = match table.column_index(&ident.value) {
                            Some(idx) => idx,
                            None => {
                                // Try suffix match for qualified columns
                                let mut found = false;
                                let mut idx = 0;
                                for (i, col) in table.columns().iter().enumerate() {
                                    if col.ends_with(&format!(".{}", ident.value)) {
                                        found = true;
                                        idx = i;
                                        break;
                                    }
                                }
                                
                                if found {
                                    idx
                                } else {
                                    return Err(SqawkError::ColumnNotFound(ident.value.clone()));
                                }
                            }
                        };
                        
                        // Extract all values for this column
                        Ok(table.rows().iter().map(|row| row[col_idx].clone()).collect())
                    },
                    Expr::CompoundIdentifier(parts) => {
                        // Handle qualified column references like table.column
                        let qualified_name = parts.iter()
                            .map(|ident| ident.value.clone())
                            .collect::<Vec<_>>()
                            .join(".");
                        
                        // Get column index
                        let col_idx = match table.column_index(&qualified_name) {
                            Some(idx) => idx,
                            None => {
                                // Try suffix match
                                if parts.len() == 2 {
                                    let suffix = format!("{}.{}", parts[0].value, parts[1].value);
                                    
                                    let mut found = false;
                                    let mut idx = 0;
                                    for (i, col) in table.columns().iter().enumerate() {
                                        if col.ends_with(&suffix) {
                                            found = true;
                                            idx = i;
                                            break;
                                        }
                                    }
                                    
                                    if found {
                                        idx
                                    } else {
                                        return Err(SqawkError::ColumnNotFound(qualified_name));
                                    }
                                } else {
                                    return Err(SqawkError::ColumnNotFound(qualified_name));
                                }
                            }
                        };
                        
                        // Extract all values for this column
                        Ok(table.rows().iter().map(|row| row[col_idx].clone()).collect())
                    },
                    _ => Err(SqawkError::UnsupportedSqlFeature(
                        "Only column references are supported in aggregate functions".to_string()
                    )),
                }
            },
            _ => Err(SqawkError::UnsupportedSqlFeature(
                "Unsupported function argument type".to_string()
            )),
        }
    }
    
    /// Resolve a simple (unqualified) column reference like 'name'
    ///
    /// First tries exact match, then tries to match as suffix of qualified column
    fn resolve_simple_column_reference(&self, column_name: &str, row: &[Value], table: &Table) -> SqawkResult<Value> {
        // First try as an exact column name match
        if let Some(idx) = table.column_index(column_name) {
            return self.get_row_value_at_index(idx, row);
        }
        
        // Try to find a matching qualified column (e.g., for "name", match "table.name")
        let suffix = format!(".{}", column_name);
        for (i, col) in table.columns().iter().enumerate() {
            if col.ends_with(&suffix) {
                return self.get_row_value_at_index(i, row);
            }
        }
        
        // If we got here, the column wasn't found
        Err(SqawkError::ColumnNotFound(column_name.to_string()))
    }
    
    /// Resolve a qualified column reference like 'table.column'
    ///
    /// First tries exact match, then tries suffix match for joins
    fn resolve_qualified_column_reference(&self, parts: &[sqlparser::ast::Ident], row: &[Value], table: &Table) -> SqawkResult<Value> {
        // Build the fully qualified column name from parts
        let qualified_name = parts.iter()
            .map(|ident| ident.value.clone())
            .collect::<Vec<_>>()
            .join(".");
        
        // Try to find an exact match for the qualified column
        if let Some(idx) = table.column_index(&qualified_name) {
            return self.get_row_value_at_index(idx, row);
        }
        
        // If we didn't find an exact match, try a suffix match
        // This helps with cases like "users.id" matching "users_orders_cross.users.id"
        if parts.len() == 2 {
            return self.try_suffix_match(parts, row, table);
        }
        
        // If we got here, the qualified column wasn't found
        Err(SqawkError::ColumnNotFound(qualified_name))
    }
    
    /// Try to match a column reference as a suffix
    ///
    /// This helps with joins where the full reference might be something like
    /// 'users_orders_cross.users.id' but the user references 'users.id'
    fn try_suffix_match(&self, parts: &[sqlparser::ast::Ident], row: &[Value], table: &Table) -> SqawkResult<Value> {
        let suffix = format!("{}.{}", parts[0].value, parts[1].value);
        
        for (i, col) in table.columns().iter().enumerate() {
            if col.ends_with(&suffix) {
                return self.get_row_value_at_index(i, row);
            }
        }
        
        // If no suffix match was found, report the column as not found
        Err(SqawkError::ColumnNotFound(
            parts.iter()
                .map(|ident| ident.value.clone())
                .collect::<Vec<_>>()
                .join(".")
        ))
    }
    
    /// Get a value from a row at the specified index with bounds checking
    fn get_row_value_at_index(&self, idx: usize, row: &[Value]) -> SqawkResult<Value> {
        if idx < row.len() {
            Ok(row[idx].clone())
        } else {
            Err(SqawkError::InvalidSqlQuery(format!(
                "Column index {} out of bounds for row with {} columns",
                idx,
                row.len()
            )))
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
        
        // Verify the table exists and get necessary info
        let table_ref = self.csv_handler.get_table(&table_name)?;
        
        // Process assignments to get column indices and their new values
        let column_assignments = self.process_update_assignments(&assignments, table_ref)?;
            
        // Find rows to update based on WHERE clause
        let rows_to_update = self.find_rows_to_update(selection.as_ref(), table_ref)?;
        
        // Compute all values for each assignment before getting a mutable reference
        let updates = self.compute_update_values(&rows_to_update, &column_assignments)?;
        
        // Apply updates with a mutable reference, now that all expressions have been evaluated
        self.apply_updates(&table_name, updates)
    }
    
    /// Process assignment expressions for an UPDATE statement
    ///
    /// Converts SQL assignments to column indices and expressions
    fn process_update_assignments(&self, assignments: &[Assignment], table: &Table) -> SqawkResult<Vec<(usize, Expr)>> {
        assignments
            .iter()
            .map(|assignment| {
                // The id is a Vec<Ident> but we only support simple column references
                if assignment.id.len() != 1 {
                    return Err(SqawkError::UnsupportedSqlFeature(
                        "Compound column identifiers not supported".to_string(),
                    ));
                }
                
                let column_name = assignment.id[0].value.clone();
                
                let column_idx = table
                    .column_index(&column_name)
                    .ok_or_else(|| SqawkError::ColumnNotFound(column_name))?;
                
                // Clone the Expr value since we can't take ownership of it
                Ok((column_idx, assignment.value.clone()))
            })
            .collect::<SqawkResult<Vec<_>>>()
    }
    
    /// Find rows to update based on an optional WHERE clause
    ///
    /// If no WHERE clause is provided, all rows will be updated
    fn find_rows_to_update(&self, where_expr: Option<&Expr>, table: &Table) -> SqawkResult<Vec<usize>> {
        let mut rows_to_update = Vec::new();
        
        if let Some(expr) = where_expr {
            // Filter rows that match the WHERE condition
            for (idx, row) in table.rows().iter().enumerate() {
                if self.evaluate_condition(expr, row, table).unwrap_or(false) {
                    rows_to_update.push(idx);
                }
            }
        } else {
            // If no WHERE clause, update all rows
            rows_to_update = (0..table.row_count()).collect();
        }
        
        Ok(rows_to_update)
    }
    
    /// Compute all values for an UPDATE operation
    ///
    /// This avoids the borrow checker conflict between evaluate_expr and table_mut
    /// by pre-computing all values before applying them
    fn compute_update_values(&self, rows: &[usize], column_assignments: &[(usize, Expr)]) -> SqawkResult<Vec<(usize, usize, Value)>> {
        let mut updates = Vec::new();
        
        // Pre-compute all values to be updated
        for &row_idx in rows {
            for &(col_idx, ref expr) in column_assignments {
                let value = self.evaluate_expr(expr)?;
                updates.push((row_idx, col_idx, value));
            }
        }
        
        Ok(updates)
    }
    
    /// Apply a set of pre-computed updates to a table
    ///
    /// Returns the number of rows that were affected
    fn apply_updates(&mut self, table_name: &str, updates: Vec<(usize, usize, Value)>) -> SqawkResult<usize> {
        // Calculate how many rows were affected (distinct row indices)
        let row_indices: std::collections::HashSet<usize> = updates.iter()
            .map(|(row_idx, _, _)| *row_idx)
            .collect();
        
        let row_count = row_indices.len();
        
        if row_count > 0 {
            let table = self.csv_handler.get_table_mut(table_name)?;
            
            // Apply all the pre-computed updates
            for (row_idx, col_idx, value) in updates {
                table.update_value(row_idx, col_idx, value)?;
            }
            
            // Mark the table as modified
            self.modified_tables.insert(table_name.to_string());
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
