//! SQL Virtual Machine (VM) bytecode execution engine
//!
//! This module implements a bytecode-based SQL execution engine inspired by SQLite's architecture.
//! The engine operates in two phases:
//! 1. Compile SQL statements into bytecode instructions
//! 2. Execute bytecode instructions in a virtual machine (VM)
//!
//! For more information on this approach, see: https://www.sqlite.org/opcode.html

/// The sqlparser version sqawk is built against.
///
/// Reported by the REPL's `.version`, since which SQL dialect version is in
/// use is the single most useful thing to know when a statement is rejected.
pub const SQLPARSER_VERSION: &str = "0.62";

mod agg;
pub(crate) mod arith;
pub mod ast_compat;
pub mod bytecode;
pub mod compiler;
mod compiler_aggregate;
mod compiler_check;
mod compiler_ddl;
mod compiler_dml;
mod compiler_from;
mod compiler_select;
mod compiler_subquery;
mod compiler_window;
mod datetime;
pub mod dialect;
pub mod engine;
mod func;
mod like;
pub mod parse;
mod window;

#[cfg(test)]
mod compiler_tests;
#[cfg(test)]
mod tests;

use std::collections::HashSet;
use std::sync::Arc;

use crate::database::Database;
use crate::error::{SqawkError, SqawkResult};
use crate::table::Table;

/// Result of VM execution including both the result table and modified table names
pub struct VmExecutionResult {
    /// One result table per statement that produced rows, in order.
    ///
    /// A multi-statement script produces one result set per statement; folding
    /// them into a single table would print every statement's rows under one
    /// header.
    pub tables: Vec<Table>,
    /// Number of rows affected by the last DML statement (INSERT, UPDATE, DELETE)
    pub affected_rows: usize,
    /// Whether any statement in this script was a row-counting DML.
    ///
    /// Distinguishes "a DML ran and changed nothing" from "no DML ran at all",
    /// which a count of zero cannot express. Without it the REPL either hides
    /// a genuine zero or prints a meaningless one after every SELECT.
    pub dml_executed: bool,
}

/// Execute SQL using the VM execution engine
///
/// This is the main entry point for VM-based SQL execution in Sqawk.
/// It implements a two-phase approach:
/// 1. SQL parsing and bytecode generation
/// 2. VM execution of bytecode instructions
///
/// Returns both the result table and a set of modified table names.
pub fn execute_vm(
    sql: &str,
    database: &mut Database,
    verbose: bool,
) -> SqawkResult<VmExecutionResult> {
    if verbose {
        diag!("VM Engine: Executing SQL via bytecode: {}", sql);

        let sql_upper = sql.to_uppercase();
        if sql_upper.contains("DISTINCT") {
            diag!("Applying DISTINCT");
        }
        if sql_upper.contains("ORDER BY") {
            diag!("Applying ORDER BY");
        }
        if sql_upper.contains("LIMIT") || sql_upper.contains("OFFSET") {
            diag!("Applying LIMIT/OFFSET");
        }
        if sql_upper.contains("GROUP BY") {
            diag!("Applying GROUP BY");
        }
        if sql_upper.contains("HAVING") {
            diag!("Applying HAVING");
        }
    }

    // Parse once, then compile and execute ONE STATEMENT AT A TIME.
    //
    // Every statement used to be folded into a single bytecode program sharing
    // one result buffer and one schema, which had two consequences:
    //
    //   - `SELECT a; SELECT b` printed the rows of both statements under the
    //     LAST statement's header.
    //   - A statement could not see the effects of the one before it, because
    //     modifications are applied only after the whole program finishes. So
    //     `UPDATE ...; SELECT ...` ran the SELECT against the pre-update table
    //     and, sharing the buffer, emitted nothing useful.
    //
    // Executing per statement gives each its own result set and makes each
    // statement observe the database as the previous one left it, which is
    // also what `CREATE TABLE t; INSERT INTO t ...` requires.
    let statements = parse::parse_sql(sql)?;

    if statements.is_empty() {
        return Err(SqawkError::InvalidSqlQuery(
            "No SQL statements found".to_string(),
        ));
    }

    let source = Arc::new(parse::SqlSource::new(sql));
    execute_statements(statements, database, verbose, &source, false)
}

/// Compile and execute parsed statements one at a time (see `execute_vm`).
///
/// `source` is the text the statements were parsed from; it names unaliased
/// result columns. `in_derived`: the statement is a derived table's query,
/// whose column references are named as written (see `SqlCompiler`).
fn execute_statements(
    statements: Vec<sqlparser::ast::Statement>,
    database: &mut Database,
    verbose: bool,
    source: &Arc<parse::SqlSource>,
    in_derived: bool,
) -> SqawkResult<VmExecutionResult> {
    let mut tables: Vec<Table> = Vec::new();
    let mut affected_rows: usize = 0;
    let mut dml_executed = false;

    for mut statement in statements {
        // BEGIN / COMMIT / ROLLBACK act on the database directly; they have
        // no bytecode.
        if let Some(control) = compiler_check::transaction_control(&statement)? {
            match control {
                compiler_check::TransactionControl::Begin => database.begin()?,
                compiler_check::TransactionControl::Commit => database.commit()?,
                compiler_check::TransactionControl::Rollback => database.rollback()?,
            }
            continue;
        }

        // PHASE 0: MATERIALIZE DERIVED TABLES.
        //
        // `FROM (SELECT ...) t` has no representation in the compiler, which
        // only knows how to open a cursor on a named table. Rather than teach
        // every FROM site about subqueries, each derived table is executed
        // first and registered under its alias, and the statement is rewritten
        // to reference that name. The compiler then sees an ordinary table.
        let derived = materialize_derived_tables(&mut statement, database, verbose, source)?;

        // Phases 1-3 run inside a closure so a failure anywhere still drops
        // the derived tables registered above. Leaving them behind made the
        // alias unusable for the rest of the session: a failed
        // `SELECT bad FROM (SELECT ...) x` left `x` registered, so the next
        // statement using the same alias failed with "shadows an existing
        // table", and `.tables` listed a phantom table.
        let outcome = (|| -> SqawkResult<(Option<Table>, AppliedModifications)> {
            // PHASE 1: SQL -> BYTECODE, against the CURRENT database state.
            let program = {
                let mut compiler = compiler::SqlCompiler::new(database, verbose)
                    .with_source(Some(Arc::clone(source)), in_derived);
                compiler.compile_statement_program(&statement)?
            };

            if verbose {
                diag!("Generated bytecode:");
                diag!("{}", program);
            }

            // PHASE 2: EXECUTE
            let mut vm = engine::VmEngine::new_mut(database, verbose);
            vm.init(program);
            vm.execute()?;

            let result_table = vm.create_result_table()?;
            let modifications = vm.take_modifications();
            drop(vm);

            // PHASE 3: APPLY MODIFICATIONS before the next statement compiles.
            let applied = apply_modifications(database, modifications, verbose)?;
            Ok((result_table, applied))
        })();

        // Derived tables live only for the statement that declared them, and
        // must be dropped whether or not it succeeded.
        for name in &derived {
            database.remove_table(name);
        }

        let (result_table, applied) = outcome?;

        // A DML statement reports its own count, INCLUDING zero.
        //
        // Guarding on `applied.affected_rows > 0` conflated two different
        // things: a statement that was not DML at all (a SELECT contributes no
        // modifications), and a DML statement that matched no rows. The latter
        // then inherited the previous statement's count, so
        // `UPDATE ... WHERE <matches 3>; UPDATE ... WHERE <matches nothing>`
        // reported 3 rows affected instead of 0.
        //
        // Which statements count is the same set SQL's `changes()` uses.
        if is_row_counting_dml(&statement) {
            affected_rows = applied.affected_rows;
            dml_executed = true;
        }

        if let Some(t) = result_table {
            tables.push(t);
        }
    }

    Ok(VmExecutionResult {
        tables,
        affected_rows,
        dml_executed,
    })
}

/// Execute every derived table in `statement` -- in the FROM of the query,
/// of a compound's arms, of subqueries in any expression, of an INSERT ...
/// SELECT or CREATE TABLE ... AS -- register each under an internal name,
/// and rewrite the FROM item to reference that name, keeping the derived
/// table's alias as the item's alias.
///
/// Returns the names registered, so the caller can drop them once the
/// statement finishes -- a derived table is scoped to the statement that
/// declares it.
///
/// A derived table that refers to a column of an enclosing query (inside a
/// correlated subquery) cannot be computed up front; it fails to resolve.
fn materialize_derived_tables(
    statement: &mut sqlparser::ast::Statement,
    database: &mut Database,
    verbose: bool,
    source: &Arc<parse::SqlSource>,
) -> SqawkResult<Vec<String>> {
    let mut registered = Registered {
        names: Vec::new(),
        source: Arc::clone(source),
    };
    let result = materialize_statement(statement, database, verbose, &mut registered);
    if result.is_err() {
        for name in &registered.names {
            database.remove_table(name);
        }
    }
    result.map(|_| registered.names)
}

fn materialize_statement(
    statement: &mut sqlparser::ast::Statement,
    database: &mut Database,
    verbose: bool,
    registered: &mut Registered,
) -> SqawkResult<()> {
    use sqlparser::ast::Statement;
    match statement {
        Statement::Query(query) => {
            materialize_in_query(query, database, verbose, registered)?;
        }
        Statement::Insert(insert) => {
            if let Some(source) = insert.source.as_mut() {
                materialize_in_query(source, database, verbose, registered)?;
            }
        }
        Statement::CreateTable(create) => {
            if let Some(query) = create.query.as_mut() {
                materialize_in_query(query, database, verbose, registered)?;
            }
        }
        Statement::Update(update) => {
            for a in &mut update.assignments {
                materialize_in_expr(&mut a.value, database, verbose, registered)?;
            }
            if let Some(e) = update.selection.as_mut() {
                materialize_in_expr(e, database, verbose, registered)?;
            }
        }
        Statement::Delete(delete) => {
            if let Some(e) = delete.selection.as_mut() {
                materialize_in_expr(e, database, verbose, registered)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// The derived tables registered so far for one statement, and the text the
/// statement was parsed from.
struct Registered {
    names: Vec<String>,
    source: Arc<parse::SqlSource>,
}

fn materialize_in_query(
    query: &mut sqlparser::ast::Query,
    database: &mut Database,
    verbose: bool,
    registered: &mut Registered,
) -> SqawkResult<()> {
    materialize_in_set_expr(&mut query.body, database, verbose, registered)?;
    if let Some(sqlparser::ast::OrderBy {
        kind: sqlparser::ast::OrderByKind::Expressions(obs),
        ..
    }) = query.order_by.as_mut()
    {
        for ob in obs {
            materialize_in_expr(&mut ob.expr, database, verbose, registered)?;
        }
    }
    Ok(())
}

fn materialize_in_set_expr(
    body: &mut sqlparser::ast::SetExpr,
    database: &mut Database,
    verbose: bool,
    registered: &mut Registered,
) -> SqawkResult<()> {
    use sqlparser::ast::{SelectItem, SetExpr};
    match body {
        SetExpr::Select(select) => {
            for twj in &mut select.from {
                materialize_in_factor(&mut twj.relation, database, verbose, registered)?;
                for join in &mut twj.joins {
                    materialize_in_factor(&mut join.relation, database, verbose, registered)?;
                }
            }
            for item in &mut select.projection {
                if let SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } = item
                {
                    materialize_in_expr(e, database, verbose, registered)?;
                }
            }
            for e in select.selection.iter_mut().chain(select.having.iter_mut()) {
                materialize_in_expr(e, database, verbose, registered)?;
            }
            Ok(())
        }
        SetExpr::SetOperation { left, right, .. } => {
            materialize_in_set_expr(left, database, verbose, registered)?;
            materialize_in_set_expr(right, database, verbose, registered)
        }
        SetExpr::Query(q) => materialize_in_query(q, database, verbose, registered),
        _ => Ok(()),
    }
}

/// Find the subqueries within an expression and materialize the derived
/// tables in their FROM clauses.
fn materialize_in_expr(
    expr: &mut sqlparser::ast::Expr,
    database: &mut Database,
    verbose: bool,
    registered: &mut Registered,
) -> SqawkResult<()> {
    use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, FunctionArguments};
    let mut go = |e: &mut Expr| materialize_in_expr(e, database, verbose, registered);
    match expr {
        Expr::Subquery(q) | Expr::Exists { subquery: q, .. } => {
            materialize_in_query(q, database, verbose, registered)
        }
        Expr::InSubquery { expr, subquery, .. } => {
            go(expr)?;
            materialize_in_query(subquery, database, verbose, registered)
        }
        Expr::BinaryOp { left, right, .. }
        | Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right) => {
            go(left)?;
            go(right)
        }
        Expr::Nested(e)
        | Expr::UnaryOp { expr: e, .. }
        | Expr::Cast { expr: e, .. }
        | Expr::IsNull(e)
        | Expr::IsNotNull(e) => go(e),
        Expr::Between {
            expr, low, high, ..
        } => {
            go(expr)?;
            go(low)?;
            go(high)
        }
        Expr::InList { expr, list, .. } => {
            go(expr)?;
            list.iter_mut().try_for_each(go)
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            go(expr)?;
            go(pattern)
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(o) = operand {
                go(o)?;
            }
            for w in conditions {
                go(&mut w.condition)?;
                go(&mut w.result)?;
            }
            if let Some(e) = else_result {
                go(e)?;
            }
            Ok(())
        }
        Expr::Function(f) => {
            if let FunctionArguments::List(list) = &mut f.args {
                for arg in &mut list.args {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                        go(e)?;
                    }
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn materialize_in_factor(
    factor: &mut sqlparser::ast::TableFactor,
    database: &mut Database,
    verbose: bool,
    registered: &mut Registered,
) -> SqawkResult<()> {
    let (subquery, alias) = match factor {
        sqlparser::ast::TableFactor::Derived { lateral: true, .. } => {
            return Err(SqawkError::UnsupportedSqlFeature(
                "LATERAL subqueries are not supported".into(),
            ))
        }
        sqlparser::ast::TableFactor::Derived {
            subquery, alias, ..
        } => (subquery.clone(), alias.clone()),
        _ => return Ok(()),
    };
    if alias.as_ref().is_some_and(|a| !a.columns.is_empty()) {
        return Err(SqawkError::UnsupportedSqlFeature(
            "Column aliases on a FROM item are not supported".into(),
        ));
    }

    // Nested derived tables are materialized innermost-first.
    let mut inner = *subquery;
    materialize_in_query(&mut inner, database, verbose, registered)?;

    // Registered under an internal name, so that an alias may repeat
    // across subqueries and may shadow a real table, as in SQL.
    let name = (1..)
        .map(|i| format!("sqawk_derived_{}", i))
        .find(|n| !database.has_table(n))
        .expect("an unused name");

    let source = Arc::clone(&registered.source);
    let result = execute_statements(
        vec![sqlparser::ast::Statement::Query(Box::new(inner))],
        database,
        verbose,
        &source,
        true,
    )?;
    let mut table = result.tables.into_iter().next_back().ok_or_else(|| {
        SqawkError::InvalidSqlQuery("Subquery in FROM produced no result".to_string())
    })?;
    table.set_name(name.clone());
    table.dedup_column_names();
    database.add_table(name.clone(), table)?;
    registered.names.push(name.clone());

    *factor = sqlparser::ast::TableFactor::Table {
        name: sqlparser::ast::ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
            sqlparser::ast::Ident::new(name),
        )]),
        alias,
        args: None,
        with_hints: Vec::new(),
        version: None,
        partitions: Vec::new(),
        with_ordinality: false,
        json_path: None,
        sample: None,
        index_hints: Vec::new(),
    };
    Ok(())
}

/// Whether a statement's affected-row count is meaningful to report.
///
/// The INSERT/UPDATE/DELETE family, matching what SQL's `changes()` covers. A
/// SELECT or DDL statement leaves the previous count alone rather than
/// resetting it to zero.
fn is_row_counting_dml(statement: &sqlparser::ast::Statement) -> bool {
    use sqlparser::ast::Statement;
    matches!(
        statement,
        Statement::Insert(_)
            | Statement::Update(_)
            | Statement::Delete(_)
            // TRUNCATE removes rows and apply_modifications counts them, so
            // omitting it discarded a real count and reported the PREVIOUS
            // statement's -- or, with the REPL now printing unconditionally,
            // a confident "0 rows affected" for a table it had just emptied.
            | Statement::Truncate(_)
            // CREATE TABLE AS SELECT inserts rows, and those inserts are
            // counted the same way.
            | Statement::CreateTable(_)
    )
}

/// Rows affected by one statement's modifications.
struct AppliedModifications {
    affected_rows: usize,
}

/// Apply a statement's pending modifications to the database, marking each
/// table they change as modified (DROP TABLE takes the mark away with the
/// table).
fn apply_modifications(
    database: &mut Database,
    modifications: Vec<engine::TableModification>,
    verbose: bool,
) -> SqawkResult<AppliedModifications> {
    // PHASE 3: APPLY MODIFICATIONS TO DATABASE

    // Track affected rows count for DML operations
    let mut affected_rows: usize = 0;

    // Collect all deletions by table first (to handle index shifting)
    let mut deletions_by_table: std::collections::HashMap<String, HashSet<usize>> =
        std::collections::HashMap::new();

    // Count inserts by table (for UPDATE detection - UPDATE = DELETE + INSERT pairs)
    let mut insert_counts_by_table: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();

    // Rows replaced in place by UPDATE, per table.
    let mut replace_counts_by_table: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();

    // First pass: collect deletions and apply non-delete modifications
    for modification in modifications {
        match modification {
            engine::TableModification::Insert { table_name, row } => {
                let table = database.get_table_mut(&table_name)?;
                table.add_row(row)?;
                *insert_counts_by_table
                    .entry(table_name.clone())
                    .or_insert(0) += 1;
                database.mark_modified(&table_name);
            }
            engine::TableModification::Replace {
                table_name,
                row_index,
                row,
            } => {
                let table = database.get_table_mut(&table_name)?;
                table.replace_row(row_index, row)?;
                // Tallied per table and reported once, not once per row.
                *replace_counts_by_table
                    .entry(table_name.clone())
                    .or_insert(0) += 1;
                database.mark_modified(&table_name);
            }
            engine::TableModification::Delete {
                table_name,
                row_index,
            } => {
                // Collect deletion indices by table
                deletions_by_table
                    .entry(table_name)
                    .or_default()
                    .insert(row_index);
            }
            engine::TableModification::CreateTable {
                table_name,
                columns,
                file_path,
                delimiter,
            } => {
                // Convert engine ColumnDef to table ColumnDefinition
                let schema: Vec<crate::table::ColumnDefinition> = columns
                    .into_iter()
                    .map(|col| crate::table::ColumnDefinition {
                        name: col.name,
                        data_type: match col.data_type.as_str() {
                            "INTEGER" => crate::table::DataType::Integer,
                            "REAL" => crate::table::DataType::Float,
                            "BOOLEAN" => crate::table::DataType::Boolean,
                            _ => crate::table::DataType::Text,
                        },
                    })
                    .collect();

                // Create the table with schema
                let file_path_buf = file_path.map(std::path::PathBuf::from);
                let table = crate::table::Table::new_with_schema(
                    &table_name,
                    schema,
                    file_path_buf,
                    delimiter,
                );

                // Add to database
                database.add_table(table_name.clone(), table)?;
                database.mark_modified(&table_name);
            }
            engine::TableModification::DropTable { table_name } => {
                // Remove table from database
                if !database.remove_table(&table_name) {
                    return Err(crate::error::SqawkError::TableNotFound(table_name));
                }
                // remove_table also drops the table from the modified set:
                // a dropped table is not written back.
            }
            engine::TableModification::AlterTableAddColumn {
                table_name,
                column_name,
                column_type,
            } => {
                let table = database.get_table_mut(&table_name)?;
                let data_type = match column_type.as_str() {
                    "INTEGER" => crate::table::DataType::Integer,
                    "REAL" => crate::table::DataType::Float,
                    "BOOLEAN" => crate::table::DataType::Boolean,
                    _ => crate::table::DataType::Text,
                };
                table.add_column_with_default(column_name, data_type, crate::table::Value::Null)?;
                database.mark_modified(&table_name);
            }
            engine::TableModification::Truncate { table_name } => {
                let table = database.get_table_mut(&table_name)?;
                let row_count = table.row_count();
                table.clear_rows()?;
                affected_rows += row_count;
                database.mark_modified(&table_name);
            }
        }
    }

    // Count pure inserts (not part of UPDATE) as affected rows first
    // (before deletions_by_table is consumed)
    for (table_name, insert_count) in &insert_counts_by_table {
        let delete_count = deletions_by_table
            .get(table_name)
            .map(|s| s.len())
            .unwrap_or(0);
        if delete_count == 0 {
            // Pure INSERT (not UPDATE)
            affected_rows += insert_count;
        }
    }

    // Second pass: apply all deletions (filtering out deleted indices in one pass)
    for (table_name, indices_to_delete) in deletions_by_table {
        let delete_count = indices_to_delete.len();
        let table = database.get_table_mut(&table_name)?;

        // Keep the rows not being deleted (and their comment lines in place)
        table.retain_rows(|idx| !indices_to_delete.contains(&idx));
        database.mark_modified(&table_name);

        // If we have equal deletes and inserts for this table, it's an UPDATE
        // Otherwise it's a DELETE
        let insert_count = insert_counts_by_table
            .get(&table_name)
            .copied()
            .unwrap_or(0);
        if insert_count == delete_count && insert_count > 0 {
            // UPDATE: count the number of rows updated
            affected_rows += delete_count;
            if verbose {
                diag!("Updated {} rows", delete_count);
            }
        } else {
            // DELETE: count the number of rows deleted
            affected_rows += delete_count;
            if verbose {
                diag!("Deleted {} rows", delete_count);
            }
        }
    }

    for (_table, count) in replace_counts_by_table {
        affected_rows += count;
        if verbose {
            diag!("Updated {} rows", count);
        }
    }

    Ok(AppliedModifications { affected_rows })
}
