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
pub const SQLPARSER_VERSION: &str = "0.64";

mod agg;
pub(crate) mod arith;
pub mod ast_compat;
pub mod bytecode;
mod collate;
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
mod glob;
pub(crate) mod like;
pub mod parse;
mod prepass;
mod printf;
mod regexp;
mod window;

#[cfg(test)]
mod compiler_tests;
#[cfg(test)]
mod tests;

use std::collections::HashSet;
use std::sync::Arc;

use crate::database::Database;
use crate::error::{SqawkError, SqawkResult};
use crate::output::ResultSink;

/// The names of the built-in scalar functions (upper case).
pub(crate) fn scalar_function_names() -> impl Iterator<Item = &'static str> {
    func::names()
}

/// Counts reported by a script's execution (its result sets went to the
/// sink).
pub struct VmExecutionResult {
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
/// Each statement's result rows go to `sink` as the statement produces
/// them, so a later statement's failure cannot swallow the output of
/// earlier ones. A multi-statement script produces one result set per
/// statement (each with its own header); an error from the sink stops the
/// script.
pub fn execute_vm(
    sql: &str,
    database: &mut Database,
    verbose: bool,
    sink: &mut dyn ResultSink,
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
    execute_statements(
        statements,
        database,
        verbose,
        &source,
        false,
        &Arc::default(),
        sink,
    )
}

/// Compile and execute parsed statements one at a time (see `execute_vm`).
///
/// `source` is the text the statements were parsed from; it names unaliased
/// result columns. `in_derived`: the statement is a derived table's or CTE's
/// query, whose column references are named as written (see `SqlCompiler`).
/// `cte_names`: for such a query, the CTE names of the enclosing statement
/// (see `prepass::Prepared::cte_names`); empty at top level. `sink` receives
/// each statement's result rows as the statement produces them.
fn execute_statements(
    statements: Vec<sqlparser::ast::Statement>,
    database: &mut Database,
    verbose: bool,
    source: &Arc<parse::SqlSource>,
    in_derived: bool,
    cte_names: &Arc<HashSet<String>>,
    sink: &mut dyn ResultSink,
) -> SqawkResult<VmExecutionResult> {
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

        // PHASE 0: MATERIALIZE DERIVED TABLES AND CTEs.
        //
        // `FROM (SELECT ...) t` and a CTE reference have no representation in
        // the compiler, which only knows how to open a cursor on a named
        // table. Each is computed first and registered under an internal
        // name, and the statement is rewritten to reference that name (see
        // `prepass.rs`). The compiler then sees ordinary tables.
        let prepared = prepass::run(&mut statement, database, verbose, source, cte_names)?;

        // Phases 1-3 run inside a closure so a failure anywhere still drops
        // the tables registered above. Leaving them behind made the alias
        // unusable for the rest of the session: a failed
        // `SELECT bad FROM (SELECT ...) x` left `x` registered, so the next
        // statement using the same alias failed with "shadows an existing
        // table", and `.tables` listed a phantom table.
        let outcome = (|| -> SqawkResult<AppliedModifications> {
            // PHASE 1: SQL -> BYTECODE, against the CURRENT database state.
            let program = {
                let mut compiler = compiler::SqlCompiler::new(database, verbose)
                    .with_source(Some(Arc::clone(source)), in_derived)
                    .with_cte_names(Arc::clone(&prepared.cte_names));
                compiler.compile_statement_program(&statement)?
            };

            if verbose {
                diag!("Generated bytecode:");
                diag!("{}", program);
            }

            // PHASE 2: EXECUTE
            let mut vm = engine::VmEngine::new_mut(database, verbose);
            vm.init(program);
            stream_rows(&mut vm, sink)?;
            let modifications = vm.take_modifications();
            drop(vm);

            // PHASE 3: APPLY MODIFICATIONS before the next statement compiles.
            apply_modifications(database, modifications, verbose)
        })();

        // Derived tables and CTEs live only for the statement that declared
        // them, and must be dropped whether or not it succeeded.
        for name in &prepared.registered {
            database.remove_table(name);
        }

        let applied = outcome?;

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
    }

    Ok(VmExecutionResult {
        affected_rows,
        dml_executed,
    })
}

/// Run a statement's program, handing its result rows to `sink` as they
/// are produced. A result with columns but no rows still begins and ends
/// (its header prints); a program without result columns sends nothing.
/// On an error after the first row, the sink is aborted (keeping what it
/// already printed) and the error returned.
fn stream_rows(vm: &mut engine::VmEngine<'_>, sink: &mut dyn ResultSink) -> SqawkResult<()> {
    let mut begun = false;
    let outcome = (|| -> SqawkResult<()> {
        while vm.step()? == engine::Step::Row {
            if !begun {
                let width = vm.row().len();
                sink.begin(&engine::result_column_defs(vm.result_schema(), width))?;
                begun = true;
            }
            sink.row(vm.row())?;
        }
        Ok(())
    })();
    if let Err(e) = outcome {
        if begun {
            sink.abort();
        }
        return Err(e);
    }
    if !begun {
        let schema = vm.result_schema();
        if schema.is_empty() {
            return Ok(());
        }
        sink.begin(&engine::result_column_defs(schema, schema.columns.len()))?;
    }
    sink.end()
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

    // (table, row) pairs already replaced by this statement.
    let mut replaced: HashSet<(String, usize)> = HashSet::new();

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
                // A row is replaced once per statement: UPDATE ... FROM
                // replaces a target row for every FROM row it joins, and
                // only the first counts and applies.
                if !replaced.insert((table_name.clone(), row_index)) {
                    continue;
                }
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
                header,
                comments,
            } => {
                // Each column keeps its declared type name; its affinity
                // follows SQLite's rules.
                let schema: Vec<crate::table::ColumnDefinition> = columns
                    .into_iter()
                    .map(|col| crate::table::ColumnDefinition {
                        name: col.name,
                        data_type: crate::table::DataType::from_declared(&col.data_type),
                        decl_type: Some(col.data_type),
                    })
                    .collect();

                let Some(path) = file_path else {
                    // No LOCATION: a scratch table, never written.
                    let table =
                        crate::table::Table::new_with_schema(&table_name, schema, None, delimiter);
                    database.add_table(table_name.clone(), table)?;
                    database.mark_modified(&table_name);
                    continue;
                };
                // The declared columns are the schema of the LOCATION file:
                // an existing file is loaded under them (and is not modified
                // by loading); a new one is created on --write.
                if database.has_table(&table_name) {
                    return Err(crate::error::SqawkError::TableAlreadyExists(table_name));
                }
                let options = crate::file_handler::LocationOptions {
                    delimiter,
                    header,
                    comments,
                };
                let (table, existed) = crate::file_handler::load_location(
                    database,
                    &table_name,
                    &path,
                    schema,
                    &options,
                )?;
                database.add_table(table_name.clone(), table)?;
                if !existed {
                    database.mark_modified(&table_name);
                }
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
                table.add_column_with_default(
                    column_name,
                    column_type,
                    crate::table::Value::Null,
                )?;
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

#[cfg(test)]
mod version_tests {
    /// `.version` reports the sqlparser actually built in: the major and
    /// minor version in Cargo.lock.
    #[test]
    fn sqlparser_version_matches_cargo_lock() {
        let lock = include_str!("../../Cargo.lock");
        let at = lock
            .find("name = \"sqlparser\"")
            .expect("sqlparser in Cargo.lock");
        let key = "version = \"";
        let rest = &lock[at..];
        let rest = &rest[rest.find(key).unwrap() + key.len()..];
        let version = &rest[..rest.find('"').unwrap()];
        let minor: Vec<&str> = version.split('.').take(2).collect();
        assert_eq!(super::SQLPARSER_VERSION, minor.join("."));
    }
}
