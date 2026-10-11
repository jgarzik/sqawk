//! Red-list: the correctness defects found in the 2026-08 audit.
//!
//! Every test in this module encodes **correct** SQL behaviour for a defect
//! found in that audit. Each was `#[ignore]`d when written, because sqawk got
//! it wrong, and un-ignored as it was fixed.
//!
//! Every 2026-08 test here now passes, and so does every test of the
//! `audit_2026_10` submodule (defects from the 2026-10 audit), of the
//! `code_review_2026_10` submodule (defects from the 2026-10 code review of
//! the `updates` branch), of the `v1_review_2026_10` submodule (findings of
//! the 2026-10 v1-readiness review) and of the `doc_review_2026_10`
//! submodule (code defects found by the 2026-10 documentation review). All
//! serve as regression cover. Add a new `#[ignore]`d test when a defect is
//! found, and remove the attribute when it is fixed.
//!
//! Run the red list with:
//!     cargo test --test mod defects -- --ignored
//!
//! These use the exact-output helpers (`assert_query`), not the substring
//! `contains` assertions used elsewhere in the suite. Substring matching is
//! precisely why every defect below survived a 302-test green suite: a test
//! asserting `"Engineering,3"` passes even when three extra wrong rows are
//! emitted, and passes regardless of row order.

mod audit_2026_10;
mod code_review_2026_10;
mod doc_review_2026_10;
mod v1_review_2026_10;

use crate::helpers::{
    assert_after_write, assert_query, departments_csv, employees_csv, nullable_csv, orders_csv,
    users_csv,
};

// ---------------------------------------------------------------------------
// Group 1: WHERE / GROUP BY / HAVING -- silent wrong answers
// ---------------------------------------------------------------------------

/// Defect 1: `compile_select_with_group_by` never reads `select.selection`,
/// so WHERE is silently ignored on *every* GROUP BY query.
/// Currently returns all four departments with unfiltered counts.
#[test]
fn where_is_applied_to_group_by() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &[employees_csv()],
        "SELECT department, COUNT(*) FROM employees WHERE salary > 60000 GROUP BY department",
        "department,COUNT(*)\n\
         Engineering,3\n\
         Sales,2",
    )
}

/// Defect 2: group-change detection compares only the first key column
/// (single `Ne` at compiler_aggregate.rs:396), so multi-column GROUP BY
/// collapses to the first column. Currently returns 4 rows instead of 6,
/// with an arbitrary `role` value.
#[test]
fn group_by_uses_all_key_columns() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &[employees_csv()],
        "SELECT department, role, COUNT(*) FROM employees GROUP BY department, role",
        "department,role,COUNT(*)\n\
         Engineering,Developer,2\n\
         Engineering,Manager,1\n\
         HR,Intern,1\n\
         Marketing,Analyst,1\n\
         Marketing,Specialist,1\n\
         Sales,Director,2",
    )
}

/// Defect 3a: `query` is never passed into the GROUP BY compiler
/// (compiler.rs:760-766), so ORDER BY is silently dropped.
#[test]
fn order_by_applies_to_group_by() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &[employees_csv()],
        "SELECT department, COUNT(*) FROM employees GROUP BY department ORDER BY department DESC",
        "department,COUNT(*)\n\
         Sales,2\n\
         Marketing,2\n\
         HR,1\n\
         Engineering,3",
    )
}

/// Defect 3b: same root cause -- LIMIT is silently dropped on GROUP BY.
#[test]
fn limit_applies_to_group_by() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &[employees_csv()],
        "SELECT department, COUNT(*) FROM employees GROUP BY department LIMIT 1",
        "department,COUNT(*)\n\
         Engineering,3",
    )
}

/// Defect 7: `compile_having_operand` matches aggregates by function *type*,
/// ignoring the argument column, so `HAVING SUM(age)` binds to `SUM(salary)`.
/// Needs two same-type aggregates to surface -- MIN vs MAX bind correctly.
#[test]
fn having_binds_to_the_named_aggregate() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &[employees_csv()],
        "SELECT department, SUM(salary), SUM(age) FROM employees \
         GROUP BY department HAVING SUM(age) > 60",
        "department,SUM(salary),SUM(age)\n\
         Engineering,210000,97\n\
         Sales,170000,85",
    )
}

/// Defect 6: `func.distinct` is never read, so COUNT(DISTINCT x) counts rows.
#[test]
fn count_distinct_deduplicates() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &[employees_csv()],
        "SELECT COUNT(DISTINCT department) FROM employees",
        "COUNT(DISTINCT department)\n4",
    )
}

/// Defect A: the GROUP BY result block is laid out positionally as
/// [group keys.., aggregates..] while the schema is built in projection order,
/// so headers and values disagree. Currently prints `COUNT,department` over
/// `Engineering,3`.
#[test]
fn group_by_respects_projection_order() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &[employees_csv()],
        "SELECT COUNT(*), department FROM employees GROUP BY department",
        "COUNT(*),department\n\
         3,Engineering\n\
         1,HR\n\
         2,Marketing\n\
         2,Sales",
    )
}

/// Defect A (second symptom): a GROUP BY key that is not in the projection
/// still emits a phantom output column, currently named `col1`.
#[test]
fn group_by_emits_only_projected_columns() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &[employees_csv()],
        "SELECT COUNT(*) FROM employees GROUP BY department",
        "COUNT(*)\n3\n1\n2\n2",
    )
}

// ---------------------------------------------------------------------------
// Group 2: JOIN projection and ordering
// ---------------------------------------------------------------------------

/// Defect B: JOIN always emits left-table columns then right-table columns,
/// while the schema is built in SELECT order. Currently prints header
/// `orders.id,users.name` over values `John,101` -- swapped.
#[test]
fn join_respects_projection_order() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &[users_csv(), orders_csv()],
        "SELECT orders.id, users.name FROM users JOIN orders ON users.id = orders.user_id",
        "id,name\n\
         101,John\n\
         103,John\n\
         105,John\n\
         102,Jane\n\
         104,Jane",
    )
}

/// Defect 4: ORDER BY was skipped on explicit JOINs.
#[test]
fn order_by_applies_to_join() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &[users_csv(), orders_csv()],
        "SELECT users.name, orders.id FROM users JOIN orders ON users.id = orders.user_id \
         ORDER BY orders.id DESC",
        "name,id\n\
         John,105\n\
         Jane,104\n\
         John,103\n\
         Jane,102\n\
         John,101",
    )
}

// ---------------------------------------------------------------------------
// Group 3: projection expressions
// ---------------------------------------------------------------------------

/// Defect 5: the `_` arm of `compile_projection_expr` (compiler.rs:2393-2405)
/// emits **no instruction** on failure, leaving the register at its default
/// NULL. The same query without LIMIT returns correct values, because it takes
/// a different compilation path.
#[test]
fn arithmetic_projection_survives_limit() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &[employees_csv()],
        "SELECT name, salary*2 FROM employees LIMIT 3",
        "name,salary*2\n\
         Alice,140000\n\
         Bob,110000\n\
         Charlie,130000",
    )
}

/// `CASE` is routed in WHERE but hits `resolve_column_expr` in a projection,
/// which errors with "Only column references supported in SELECT".
#[test]
fn case_works_in_projection() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &[employees_csv()],
        "SELECT name, CASE WHEN age > 30 THEN 'old' ELSE 'young' END FROM employees LIMIT 3",
        "name,CASE WHEN age > 30 THEN 'old' ELSE 'young' END\n\
         Alice,young\n\
         Bob,young\n\
         Charlie,old",
    )
}

/// `||` parses fine in sqlparser 0.36 -- the compiler simply has no arm for
/// `BinaryOperator::StringConcat` (compiler.rs:1922).
#[test]
fn string_concat_operator_works() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &[employees_csv()],
        "SELECT name || '!' FROM employees LIMIT 2",
        "name || '!'\n\
         Alice!\n\
         Bob!",
    )
}

/// A comparison used as a projected *value* rather than a filter.
#[test]
fn comparison_works_in_projection() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &[employees_csv()],
        "SELECT name, salary > 60000 FROM employees LIMIT 3",
        "name,salary > 60000\n\
         Alice,1\n\
         Bob,0\n\
         Charlie,1",
    )
}

/// Aggregate over an expression -- `resolve_column_expr` rejects anything that
/// is not a bare column reference.
#[test]
fn aggregate_accepts_expression_argument() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &[employees_csv()],
        "SELECT SUM(salary + age) FROM employees",
        "SUM(salary + age)\n540257",
    )
}

/// An aggregate nested inside an expression is not even *detected* as an
/// aggregate query (`is_aggregate_expr` only matches a top-level Function),
/// so it falls into the plain table-scan path.
#[test]
fn aggregate_nested_in_expression_works() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &[employees_csv()],
        "SELECT SUM(salary) + 1 FROM employees",
        "SUM(salary) + 1\n540001",
    )
}

// ---------------------------------------------------------------------------
// Group 4: WHERE clause expression gaps
// ---------------------------------------------------------------------------

/// `NOT` has no arm in the WHERE compiler at all.
#[test]
fn not_works_in_where() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &[employees_csv()],
        "SELECT name FROM employees WHERE NOT (age > 40)",
        "name\n\
         Alice\nBob\nCharlie\nDavid\nEve\nFrank\nHenry",
    )
}

/// Table-alias-qualified columns are rejected in a single-table WHERE:
/// `compile_where_operand` has no `CompoundIdentifier` arm.
#[test]
fn alias_qualified_column_works_in_where() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &[employees_csv()],
        "SELECT name FROM employees e WHERE e.age > 30",
        "name\n\
         Charlie\nDavid\nFrank\nGrace",
    )
}

/// Column resolution is case-insensitive in a projection but case-*sensitive*
/// in WHERE: `SELECT NAME` works while `WHERE NAME = 'Alice'` errors with
/// "Column 'NAME' not found" (find_column_index, compiler.rs:6010).
#[test]
fn column_resolution_is_case_insensitive() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &[employees_csv()],
        "SELECT name FROM employees WHERE NAME = 'Alice'",
        "name\nAlice",
    )
}

// ---------------------------------------------------------------------------
// Group 5: NULL semantics and type coercion
// ---------------------------------------------------------------------------

/// The worst defect for a CSV tool: comparing against a NULL raises a runtime
/// error instead of evaluating to UNKNOWN and filtering the row out.
/// `nullable.csv` row 2 has an empty `score`.
#[test]
fn null_comparison_yields_unknown() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &[nullable_csv()],
        "SELECT name FROM nullable WHERE score > 60",
        "name\n\
         Alice\nCharlie\nEve",
    )
}

/// A NULL must not satisfy an equality test either -- but it must not error.
///
/// NOT ignored: this already passes, because equality is routed through
/// `compare_registers_eq`, which returns `false` for mismatched types instead
/// of erroring like the ordering comparisons do. It is kept here as a
/// regression guard: the three-valued-logic change must not break it.
#[test]
fn null_never_equals_a_value() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &[nullable_csv()],
        "SELECT name FROM nullable WHERE score = 90",
        "name\n\
         Alice\nEve",
    )
}

/// awk-like coercion: a string that parses as a number compares numerically.
/// Matches what `arithmetic_op` already does, so `x + '1'` and `x > '1'` agree.
#[test]
fn string_number_comparison_coerces() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &[employees_csv()],
        "SELECT name FROM employees WHERE salary > '60000'",
        "name\n\
         Alice\nCharlie\nDavid\nFrank\nGrace",
    )
}

// ---------------------------------------------------------------------------
// Group 6: ORDER BY expressiveness
// ---------------------------------------------------------------------------

/// `build_sort_spec` maps the ORDER BY key into the *projected* column list
/// and errors if it is absent, so you cannot sort by a column you don't select.
#[test]
fn order_by_column_not_in_select_list() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &[employees_csv()],
        "SELECT name FROM employees ORDER BY salary DESC",
        "name\n\
         Grace\nDavid\nFrank\nAlice\nCharlie\nEve\nBob\nHenry",
    )
}

/// ORDER BY an arbitrary expression.
#[test]
fn order_by_expression() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &[employees_csv()],
        "SELECT name FROM employees ORDER BY salary * -1 LIMIT 3",
        "name\n\
         Grace\nDavid\nFrank",
    )
}

// ---------------------------------------------------------------------------
// Group 7: DML and FROM expressiveness
// ---------------------------------------------------------------------------

/// `compile_expr` had no `Identifier` arm, so a column could not appear on the
/// right-hand side of a SET. Verified through writeback rather than a trailing
/// SELECT, because `UPDATE ...; SELECT ...` in one invocation does not emit the
/// SELECT's rows -- see `update_then_select_emits_the_select`.
#[test]
fn update_set_from_column() -> Result<(), Box<dyn std::error::Error>> {
    assert_after_write(
        employees_csv(),
        "employees",
        "UPDATE employees SET salary = salary + 1",
        "id,name,age,salary,department,role\n\
         1,Alice,30,70001,Engineering,Developer\n\
         2,Bob,25,55001,Marketing,Specialist\n\
         3,Charlie,35,65001,Engineering,Manager\n\
         4,David,40,80001,Sales,Director\n\
         5,Eve,28,60001,Marketing,Analyst\n\
         6,Frank,32,75001,Engineering,Developer\n\
         7,Grace,45,90001,Sales,Director\n\
         8,Henry,22,45001,HR,Intern",
    )
}

/// An expression RHS combined with WHERE.
#[test]
fn update_set_expression_with_where() -> Result<(), Box<dyn std::error::Error>> {
    assert_after_write(
        employees_csv(),
        "employees",
        "UPDATE employees SET salary = salary * 2 WHERE department = 'HR'",
        "id,name,age,salary,department,role\n\
         1,Alice,30,70000,Engineering,Developer\n\
         2,Bob,25,55000,Marketing,Specialist\n\
         3,Charlie,35,65000,Engineering,Manager\n\
         4,David,40,80000,Sales,Director\n\
         5,Eve,28,60000,Marketing,Analyst\n\
         6,Frank,32,75000,Engineering,Developer\n\
         7,Grace,45,90000,Sales,Director\n\
         8,Henry,22,90000,HR,Intern",
    )
}

/// A DML statement followed by a SELECT must run the SELECT against the
/// updated table and emit its rows. Statements used to share one bytecode
/// program and one result buffer, and modifications were applied only after
/// the whole program finished, so the SELECT saw the pre-update table.
#[test]
fn update_then_select_emits_the_select() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &[employees_csv()],
        "UPDATE employees SET salary = 1 WHERE id = 1; \
         SELECT name, salary FROM employees WHERE id = 1",
        "name,salary\nAlice,1",
    )
}

/// UPDATE must leave rows where it found them.
///
/// It is implemented as delete-plus-insert, and the insert appends, so any row
/// it touches jumps to the end of the table. This is not merely cosmetic: with
/// --write the reordering is persisted to the user's file.
#[test]
fn update_preserves_row_order() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &[employees_csv()],
        "UPDATE employees SET salary = 1 WHERE id = 1; SELECT id, salary FROM employees LIMIT 3",
        "id,salary\n1,1\n2,55000\n3,65000",
    )
}

/// Derived tables parse fine in sqlparser 0.36; `TableFactor::Derived` simply
/// has no handler anywhere in the compiler.
#[test]
fn derived_table_in_from() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &[employees_csv()],
        "SELECT name FROM (SELECT name FROM employees) t LIMIT 2",
        "name\n\
         Alice\nBob",
    )
}

// ---------------------------------------------------------------------------
// Group 8: window functions
// ---------------------------------------------------------------------------

/// Defect 8: an aggregate OVER (PARTITION BY x) with no ORDER BY must return
/// the *partition total* on every row, not a running total.
#[test]
fn partition_aggregate_returns_partition_total() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &[employees_csv()],
        "SELECT name, SUM(salary) OVER (PARTITION BY department) FROM employees",
        "name,SUM(salary) OVER (PARTITION BY department)\n\
         Alice,210000\n\
         Charlie,210000\n\
         Frank,210000\n\
         Henry,45000\n\
         Bob,115000\n\
         Eve,115000\n\
         David,170000\n\
         Grace,170000",
    )
}

// ---------------------------------------------------------------------------
// Group 9: aggregate + GROUP BY interaction on the departments fixture
// ---------------------------------------------------------------------------

/// The suite's existing `test_group_by_with_order_by` asserts this ordering and
/// **passes by coincidence**: alphabetical department order happens to equal
/// avg_salary-descending order in this fixture. Pinned here as an exact-output
/// test so the coincidence cannot hide a regression again.
#[test]
fn group_by_order_by_avg_is_not_coincidental() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &[departments_csv()],
        "SELECT department, COUNT(*) FROM departments GROUP BY department \
         ORDER BY COUNT(*) ASC, department DESC",
        "department,COUNT(*)\n\
         Sales,2\n\
         Marketing,2\n\
         Engineering,3",
    )
}

// ---------------------------------------------------------------------------
// Group 10: multi-statement output
// ---------------------------------------------------------------------------

/// Each statement in a `stmt; stmt` script must print its own result set with
/// its own header. Currently the rows of every statement are concatenated and
/// printed under the LAST statement's schema, so
/// `SELECT name ...; SELECT COUNT(*) ...` emits the header `COUNT` above
/// `Alice`, `Charlie`, `3`.
#[test]
fn multi_statement_output_has_per_statement_headers() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/people.csv"],
        "SELECT name FROM people WHERE age > 30; SELECT COUNT(*) FROM people",
        "name\n\
         Alice\n\
         Charlie\n\
         COUNT(*)\n\
         3",
    )
}

// ---------------------------------------------------------------------------
// Group 11: aggregates over joins
// ---------------------------------------------------------------------------

/// An aggregate over a join is rejected: the join projection resolver accepts
/// only column references, so `COUNT(*)` never reaches the aggregate path.
#[test]
fn aggregate_over_join() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &[users_csv(), orders_csv()],
        "SELECT COUNT(*) FROM users JOIN orders ON users.id = orders.user_id",
        "COUNT(*)\n5",
    )
}
