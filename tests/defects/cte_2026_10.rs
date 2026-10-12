//! Red list: common table expressions (`WITH [RECURSIVE] ...`).
//!
//! Every test in this module encodes **correct** behaviour for `WITH`, which
//! sqawk rejected outright. Expected outputs were computed by running the
//! same SQL over the same data in SQLite 3.45 (python3 `sqlite3`), unless
//! the doc comment says otherwise; errors are SQLite's messages.
//!
//! The rules covered: a CTE is materialized once, on first reference, and an
//! unused one is never evaluated; every CTE of a WITH list sees all its
//! siblings and itself; an inner WITH shadows an outer one; a column list
//! renames; only a one-part name can reference a CTE (`main.t` is the loaded
//! table); a CTE reference is qualified by its alias, else by the CTE name;
//! WITH works on SELECT, INSERT / UPDATE / DELETE, `INSERT ... WITH`,
//! `CREATE TABLE ... AS WITH` and inside any subquery; a CTE named like a
//! loaded table is read from EVERY clause position (never the loaded table);
//! and `WITH RECURSIVE` follows SQLite's queue semantics, checks and errors.
//!
//! Each test was `#[ignore]`d when written and un-ignored when WITH was
//! implemented (`src/vm/prepass.rs`); all of them now pass and guard
//! against regressions. Run the red list with:
//!     cargo test --test mod defects -- --ignored

use std::io::Write;
use std::process::{Command, Stdio};

use crate::helpers::{assert_query_on, assert_query_on_fails};

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// `emp`: a small staff table (Ann's `mgr` is NULL). `tree`: a parent-child
/// table rooted at 1: 1 -> {2, 3}, 2 -> {4, 5}, 3 -> {6}, 4 -> {7}.
const FILES: &[(&str, &str)] = &[
    (
        "emp.csv",
        "id,name,dept,mgr,salary\n1,Ann,eng,,100\n2,Bob,eng,1,80\n3,Cid,ops,1,70\n\
         4,Dee,ops,3,60\n5,Eve,eng,2,90\n",
    ),
    ("tree.csv", "id,parent\n1,\n2,1\n3,1\n4,2\n5,2\n6,3\n7,4\n"),
];

/// Run the REPL over `people.csv` with `script` on stdin; return stdout.
fn repl(script: &str) -> Result<String, Box<dyn std::error::Error>> {
    let mut child = Command::new(env!("CARGO_BIN_EXE_sqawk"))
        .arg("-i")
        .arg("tests/data/people.csv")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    child
        .stdin
        .as_mut()
        .ok_or("no stdin")?
        .write_all(script.as_bytes())?;
    let out = child.wait_with_output()?;
    Ok(String::from_utf8_lossy(&out.stdout).replace("\r\n", "\n"))
}

/// A CTE body is held to the parser's limits like any query: a 1001-deep
/// expression in it is too deep. (sqawk's own limit, SQLite's
/// `SQLITE_MAX_EXPR_DEPTH`.)
#[test]
fn cte_body_depth_limit() -> TestResult {
    let chain = vec!["1"; 1001].join("+");
    assert_query_on_fails(
        FILES,
        &format!("WITH c AS (SELECT {chain} AS v) SELECT v FROM c"),
        "expression tree is too large (maximum depth 1000)",
    )
}

/// `WITH ... DELETE` reports its row count like a plain DELETE, and the CTE
/// leaves nothing registered: `.tables` lists only the loaded table, and the
/// CTE name is free for the next statement. (sqawk REPL behaviour.)
#[test]
fn with_dml_counts_rows_and_leaks_nothing() -> TestResult {
    let stdout = repl(
        ".changes on\n\
         WITH r AS (SELECT id FROM people WHERE age > 30) \
         DELETE FROM people WHERE id IN (SELECT id FROM r);\n\
         WITH r AS (SELECT 1 AS id) SELECT id FROM r;\n\
         .tables\n\
         .exit\n",
    )?;
    assert!(
        stdout.contains("2 rows affected"),
        "row count missing, got:\n{stdout}"
    );
    assert!(
        !stdout.contains("sqawk_"),
        "an internal table leaked, got:\n{stdout}"
    );
    assert!(
        !stdout.lines().any(|l| l.trim() == "r"),
        ".tables lists the CTE, got:\n{stdout}"
    );
    Ok(())
}

/// A statement whose CTE fails leaves nothing registered either.
/// (sqawk REPL behaviour.)
#[test]
fn failed_cte_statement_leaks_nothing() -> TestResult {
    let stdout = repl(
        "WITH a AS (SELECT 1 AS v), b AS (SELECT nosuch FROM a) SELECT * FROM a, b;\n\
         WITH a AS (SELECT 2 AS v) SELECT v FROM a;\n\
         .tables\n\
         .exit\n",
    )?;
    assert!(
        stdout.contains("v\n2\n"),
        "the second statement failed, got:\n{stdout}"
    );
    assert!(
        !stdout.contains("sqawk_"),
        "an internal table leaked, got:\n{stdout}"
    );
    Ok(())
}

/// A recursive counter to 1000 terminates through its WHERE.
/// (Expected computed: 1000 rows summing to 500500.)
#[test]
fn recursive_counter_to_1000() -> TestResult {
    assert_query_on(
        FILES,
        "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < 1000) \
         SELECT COUNT(*), SUM(n), MAX(n) FROM c",
        "COUNT(*),SUM(n),MAX(n)\n1000,500500,1000",
    )
}

/// A CTE named like a loaded table, read from a plain FROM.
#[test]
fn shadow_in_from() -> TestResult {
    assert_query_on(
        FILES,
        "WITH tree AS (SELECT 42 AS id) SELECT id FROM tree",
        "id\n42",
    )
}

/// A CTE reference inside a recursive CTE's setup arm reads the sibling.
#[test]
fn recursive_setup_reads_sibling() -> TestResult {
    assert_query_on(
        FILES,
        "WITH s AS (SELECT 5 AS start), \
         c(n) AS (SELECT start FROM s UNION ALL SELECT n + 1 FROM c WHERE n < 7) \
         SELECT n FROM c",
        "n\n5\n6\n7",
    )
}

// ---------------------------------------------------------------------------
// Plain CTEs
// ---------------------------------------------------------------------------

/// A CTE is a named query in scope for the statement.
#[test]
fn plain_cte() -> TestResult {
    assert_query_on(
        FILES,
        "WITH h AS (SELECT name, salary FROM emp WHERE salary > 75) SELECT name FROM h ORDER BY name",
        "name\n\
         Ann\n\
         Bob\n\
         Eve",
    )
}

/// A column list renames the CTE's columns.
#[test]
fn column_list_renames() -> TestResult {
    assert_query_on(
        FILES,
        "WITH h(n, s) AS (SELECT name, salary FROM emp) SELECT n, s FROM h WHERE s < 75 ORDER BY n",
        "n,s\n\
         Cid,70\n\
         Dee,60",
    )
}

/// `SELECT *` from a CTE with a column list uses those names.
#[test]
fn star_uses_cte_column_names() -> TestResult {
    assert_query_on(
        FILES,
        "WITH h(n, s) AS (SELECT name, salary FROM emp WHERE id = 1) SELECT * FROM h",
        "n,s\n\
         Ann,100",
    )
}

/// A later CTE reads an earlier one.
#[test]
fn chained_ctes() -> TestResult {
    assert_query_on(
        FILES,
        "WITH a AS (SELECT 1 AS n), b AS (SELECT n + 1 AS n FROM a) SELECT n FROM b",
        "n\n\
         2",
    )
}

/// Every CTE of a WITH list sees all its siblings, including later ones.
#[test]
fn forward_reference_to_sibling() -> TestResult {
    assert_query_on(
        FILES,
        "WITH a AS (SELECT * FROM b), b AS (SELECT 1 AS z) SELECT * FROM a",
        "z\n\
         1",
    )
}

/// An unused CTE is never evaluated, even when its body would fail.
#[test]
fn unused_cte_is_never_evaluated() -> TestResult {
    assert_query_on(
        FILES,
        "WITH x AS (SELECT * FROM nosuch) SELECT 1",
        "1\n\
         1",
    )
}

/// An inner WITH shadows an outer CTE of the same name.
#[test]
fn inner_with_shadows_outer() -> TestResult {
    assert_query_on(
        FILES,
        "WITH c AS (SELECT 1 AS v) SELECT (WITH c AS (SELECT 2 AS v) SELECT v FROM c) AS inner_v, v FROM c",
        "inner_v,v\n\
         2,1",
    )
}

/// A CTE body resolves names where it is defined, not where it is referenced.
#[test]
fn cte_body_resolves_in_its_own_scope() -> TestResult {
    assert_query_on(
        FILES,
        "WITH a AS (SELECT v FROM b), b AS (SELECT 1 AS v) SELECT * FROM (WITH b AS (SELECT 2 AS v) SELECT v FROM a) d",
        "v\n\
         1",
    )
}

/// One CTE may be referenced twice in a statement.
#[test]
fn cte_referenced_twice() -> TestResult {
    assert_query_on(
        FILES,
        "WITH h AS (SELECT id FROM emp WHERE id < 4) SELECT x.id, y.id FROM h x JOIN h y ON x.id < y.id ORDER BY x.id, y.id",
        "id,id\n\
         1,2\n\
         1,3\n\
         2,3",
    )
}

/// An unaliased CTE reference is qualified by the CTE's name.
#[test]
fn cte_name_qualifies_columns() -> TestResult {
    assert_query_on(
        FILES,
        "WITH c AS (SELECT 1 AS v) SELECT c.v FROM c",
        "v\n\
         1",
    )
}

/// An aliased CTE reference is qualified by its alias.
#[test]
fn cte_alias_qualifies_columns() -> TestResult {
    assert_query_on(
        FILES,
        "WITH c AS (SELECT 1 AS v) SELECT x.v FROM c AS x",
        "v\n\
         1",
    )
}

/// Once aliased, the CTE's own name no longer qualifies its columns.
#[test]
fn cte_alias_hides_cte_name() -> TestResult {
    assert_query_on_fails(
        FILES,
        "WITH c AS (SELECT 1 AS v) SELECT c.v FROM c AS x",
        "no such column: c.v",
    )
}

/// Errors name the CTE as written, never an internal table.
#[test]
fn unknown_column_names_the_cte() -> TestResult {
    assert_query_on_fails(
        FILES,
        "WITH c AS (SELECT 1 AS v) SELECT c.nosuch FROM c",
        "no such column: c.nosuch",
    )
}

/// A CTE shadows a loaded table of the same name.
#[test]
fn cte_shadows_loaded_table() -> TestResult {
    assert_query_on(
        FILES,
        "WITH emp AS (SELECT 9 AS id) SELECT id FROM emp",
        "id\n\
         9",
    )
}

/// `main.emp` names the loaded table, never a CTE.
#[test]
fn main_schema_reads_the_real_table() -> TestResult {
    assert_query_on(
        FILES,
        "WITH emp AS (SELECT 9 AS id) SELECT id FROM main.emp ORDER BY id",
        "id\n\
         1\n\
         2\n\
         3\n\
         4\n\
         5",
    )
}

/// CTE names match case-insensitively.
#[test]
fn cte_names_match_case_insensitively() -> TestResult {
    assert_query_on(
        FILES,
        "WITH Cte AS (SELECT 1 AS v) SELECT v FROM CTE",
        "v\n\
         1",
    )
}

/// Duplicate column names in a CTE are made unique as in a derived table.
/// (sqawk's derived-table naming; SQLite's own `a:1` is internal.)
#[test]
fn duplicate_result_columns_are_deduplicated() -> TestResult {
    assert_query_on(
        FILES,
        "WITH c AS (SELECT 1 AS a, 2 AS a) SELECT * FROM c",
        "a,a:1\n\
         1,2",
    )
}

/// A compound inside a CTE body evaluates left to right like any compound.
#[test]
fn cte_body_compound_is_left_associative() -> TestResult {
    assert_query_on(
        FILES,
        "WITH u AS (SELECT 1 AS v UNION SELECT 2 INTERSECT SELECT 2) SELECT * FROM u",
        "v\n\
         2",
    )
}

/// `main.t.c` inside a CTE body is `t.c`.
#[test]
fn cte_body_strips_main_schema() -> TestResult {
    assert_query_on(
        FILES,
        "WITH c AS (SELECT main.emp.id AS i FROM emp WHERE id = 1) SELECT i FROM c",
        "i\n\
         1",
    )
}

/// `AS MATERIALIZED` and `AS NOT MATERIALIZED` are accepted hints.
#[test]
fn materialized_hint_is_accepted() -> TestResult {
    assert_query_on(
        FILES,
        "WITH c AS MATERIALIZED (SELECT 1 AS v), d AS NOT MATERIALIZED (SELECT 2 AS w) SELECT v, w FROM c, d",
        "v,w\n\
         1,2",
    )
}

/// WITH inside a scalar subquery.
#[test]
fn cte_in_scalar_subquery() -> TestResult {
    assert_query_on(
        FILES,
        "SELECT name FROM emp WHERE salary = (WITH m AS (SELECT MAX(salary) AS s FROM emp) SELECT s FROM m)",
        "name\n\
         Ann",
    )
}

/// WITH inside a derived table.
#[test]
fn cte_in_derived_table() -> TestResult {
    assert_query_on(
        FILES,
        "SELECT v FROM (WITH c AS (SELECT 5 AS v) SELECT v FROM c) t",
        "v\n\
         5",
    )
}

/// WITH inside an IN subquery.
#[test]
fn cte_in_in_subquery() -> TestResult {
    assert_query_on(
        FILES,
        "SELECT id FROM emp WHERE id IN (WITH h AS (SELECT 1 AS x UNION ALL SELECT 3) SELECT x FROM h) ORDER BY id",
        "id\n\
         1\n\
         3",
    )
}

/// A CTE referenced from a correlated subquery of the main query.
#[test]
fn cte_in_correlated_subquery() -> TestResult {
    assert_query_on(
        FILES,
        "WITH m AS (SELECT dept, MAX(salary) AS mx FROM emp GROUP BY dept) SELECT name FROM emp WHERE salary = (SELECT mx FROM m WHERE m.dept = emp.dept) ORDER BY name",
        "name\n\
         Ann\n\
         Cid",
    )
}

/// A CTE body that refers to an outer query's column cannot be computed up
/// front; like a correlated derived table it is a clean error. (SQLite
/// evaluates it per outer row; a documented difference.)
#[test]
fn correlated_cte_body_is_an_error() -> TestResult {
    assert_query_on_fails(
        FILES,
        "SELECT (WITH c AS (SELECT emp.id AS v) SELECT v FROM c) FROM emp",
        "no such column: emp.id",
    )
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// A non-recursive CTE that reads its own name is a circular reference.
#[test]
fn self_reference_is_circular() -> TestResult {
    assert_query_on_fails(
        FILES,
        "WITH emp AS (SELECT * FROM emp) SELECT * FROM emp",
        "circular reference: emp",
    )
}

/// Two CTEs that read each other are a circular reference.
#[test]
fn mutual_reference_is_circular() -> TestResult {
    assert_query_on_fails(
        FILES,
        "WITH a AS (SELECT * FROM b), b AS (SELECT * FROM a) SELECT * FROM a",
        "circular reference: a",
    )
}

/// Two CTEs of one WITH may not share a name.
#[test]
fn duplicate_cte_name() -> TestResult {
    assert_query_on_fails(
        FILES,
        "WITH c AS (SELECT 1), C AS (SELECT 2) SELECT 1",
        "duplicate WITH table name: C",
    )
}

/// A column list must match the body's column count.
#[test]
fn column_list_count_mismatch() -> TestResult {
    assert_query_on_fails(
        FILES,
        "WITH c(a, b) AS (SELECT 1) SELECT * FROM c",
        "table c has 1 values for 2 columns",
    )
}

/// WITH on a parenthesized compound operand stays rejected (a syntax error
/// in SQLite).
#[test]
fn compound_operand_with_is_rejected() -> TestResult {
    assert_query_on_fails(
        FILES,
        "SELECT 1 UNION (WITH x AS (SELECT 2) SELECT * FROM x)",
        "ORDER BY, LIMIT or WITH inside a compound SELECT operand is not supported",
    )
}

// ---------------------------------------------------------------------------
// Statement forms
// ---------------------------------------------------------------------------

/// `WITH ... INSERT`.
#[test]
fn with_insert() -> TestResult {
    assert_query_on(
        FILES,
        "WITH n AS (SELECT 6 AS id, 'Fay' AS name) INSERT INTO emp (id, name) SELECT id, name FROM n; SELECT id, name FROM emp WHERE id > 4 ORDER BY id",
        "id,name\n\
         5,Eve\n\
         6,Fay",
    )
}

/// `INSERT INTO t WITH ... SELECT`.
#[test]
fn insert_with_select() -> TestResult {
    assert_query_on(
        FILES,
        "INSERT INTO emp (id, name) WITH n AS (SELECT 7 AS id, 'Gus' AS name) SELECT * FROM n; SELECT id, name FROM emp WHERE id > 4 ORDER BY id",
        "id,name\n\
         5,Eve\n\
         7,Gus",
    )
}

/// `WITH ... UPDATE`.
#[test]
fn with_update() -> TestResult {
    assert_query_on(
        FILES,
        "WITH r AS (SELECT id FROM emp WHERE dept = 'ops') UPDATE emp SET salary = 0 WHERE id IN (SELECT id FROM r); SELECT id, salary FROM emp ORDER BY id",
        "id,salary\n\
         1,100\n\
         2,80\n\
         3,0\n\
         4,0\n\
         5,90",
    )
}

/// `WITH ... DELETE`.
#[test]
fn with_delete() -> TestResult {
    assert_query_on(
        FILES,
        "WITH r AS (SELECT id FROM emp WHERE salary < 75) DELETE FROM emp WHERE id IN (SELECT id FROM r); SELECT id FROM emp ORDER BY id",
        "id\n\
         1\n\
         2\n\
         5",
    )
}

/// `CREATE TABLE ... AS WITH ...`.
#[test]
fn create_table_as_with() -> TestResult {
    assert_query_on(
        FILES,
        "CREATE TABLE hi AS WITH r AS (SELECT name FROM emp WHERE salary >= 90) SELECT name FROM r; SELECT * FROM hi ORDER BY name",
        "name\n\
         Ann\n\
         Eve",
    )
}

/// A DML target names a loaded table even when a CTE shares its name.
#[test]
fn dml_target_is_always_a_real_table() -> TestResult {
    assert_query_on(
        FILES,
        "WITH emp AS (SELECT 1 AS id) DELETE FROM emp WHERE id = 1; SELECT id FROM emp ORDER BY id",
        "id\n\
         2\n\
         3\n\
         4\n\
         5",
    )
}

/// A DML target that only a CTE defines is no such table.
#[test]
fn dml_target_never_a_cte() -> TestResult {
    assert_query_on_fails(
        FILES,
        "WITH c AS (SELECT 1 AS x) UPDATE c SET x = 2",
        "no such table: c",
    )
}

// ---------------------------------------------------------------------------
// Every clause position reads the CTE, never the same-named loaded table
// ---------------------------------------------------------------------------

/// A CTE named like a loaded table, read from a JOIN.
#[test]
fn shadow_in_join() -> TestResult {
    assert_query_on(
        FILES,
        "WITH emp AS (SELECT 2 AS id) SELECT t.id FROM tree t JOIN emp ON emp.id = t.id",
        "id\n\
         2",
    )
}

/// ... from a subquery in JOIN ON.
#[test]
fn shadow_in_join_on_subquery() -> TestResult {
    assert_query_on(
        FILES,
        "WITH emp AS (SELECT 2 AS id) SELECT t.id FROM tree t JOIN tree u ON u.id = t.id AND t.id IN (SELECT id FROM emp)",
        "id\n\
         2",
    )
}

/// ... from a subquery in WHERE.
#[test]
fn shadow_in_where_subquery() -> TestResult {
    assert_query_on(
        FILES,
        "WITH emp AS (SELECT 2 AS id) SELECT id FROM tree WHERE id IN (SELECT id FROM emp)",
        "id\n\
         2",
    )
}

/// ... from a subquery in GROUP BY.
#[test]
fn shadow_in_group_by_subquery() -> TestResult {
    assert_query_on(
        FILES,
        "WITH emp AS (SELECT 2 AS id) SELECT COUNT(*) FROM tree GROUP BY id > (SELECT id FROM emp) ORDER BY 1",
        "COUNT(*)\n\
         2\n\
         5",
    )
}

/// ... from a subquery in HAVING.
#[test]
fn shadow_in_having_subquery() -> TestResult {
    assert_query_on(
        FILES,
        "WITH emp AS (SELECT 100 AS id) SELECT COUNT(*) FROM tree HAVING COUNT(*) > (SELECT id FROM emp)",
        "COUNT(*)",
    )
}

/// ... from a subquery in ORDER BY.
#[test]
fn shadow_in_order_by_subquery() -> TestResult {
    assert_query_on(
        FILES,
        "WITH emp AS (SELECT -1 AS id) SELECT id FROM tree ORDER BY id * (SELECT id FROM emp) LIMIT 3",
        "id\n\
         7\n\
         6\n\
         5",
    )
}

/// ... from a subquery in a window's PARTITION BY.
#[test]
fn shadow_in_window_partition() -> TestResult {
    assert_query_on(
        FILES,
        "WITH emp AS (SELECT 5 AS id) SELECT id, COUNT(*) OVER (PARTITION BY id > (SELECT id FROM emp)) AS k FROM tree ORDER BY id",
        "id,k\n\
         1,5\n\
         2,5\n\
         3,5\n\
         4,5\n\
         5,5\n\
         6,2\n\
         7,2",
    )
}

/// ... from a subquery in a window's ORDER BY.
#[test]
fn shadow_in_window_order() -> TestResult {
    assert_query_on(
        FILES,
        "WITH emp AS (SELECT -1 AS id) SELECT id, ROW_NUMBER() OVER (ORDER BY id * (SELECT id FROM emp)) AS r FROM tree ORDER BY id",
        "id,r\n\
         1,7\n\
         2,6\n\
         3,5\n\
         4,4\n\
         5,3\n\
         6,2\n\
         7,1",
    )
}

/// ... from a subquery as a function argument.
#[test]
fn shadow_in_function_argument() -> TestResult {
    assert_query_on(
        FILES,
        "WITH emp AS (SELECT 100 AS id) SELECT COALESCE((SELECT id FROM emp), 0) AS v, ABS((SELECT -id FROM emp)) AS w",
        "v,w\n\
         100,100",
    )
}

/// ... from a subquery in an aggregate's FILTER.
#[test]
fn shadow_in_aggregate_filter() -> TestResult {
    assert_query_on(
        FILES,
        "WITH emp AS (SELECT 2 AS id) SELECT COUNT(*) FILTER (WHERE id IN (SELECT id FROM emp)) AS n FROM tree",
        "n\n\
         1",
    )
}

/// ... from a subquery in CASE.
#[test]
fn shadow_in_case() -> TestResult {
    assert_query_on(
        FILES,
        "WITH emp AS (SELECT 100 AS id) SELECT CASE WHEN (SELECT id FROM emp) = 100 THEN 'cte' ELSE 'real' END AS which",
        "which\n\
         cte",
    )
}

/// ... from a subquery in LIMIT.
#[test]
fn shadow_in_limit() -> TestResult {
    assert_query_on(
        FILES,
        "WITH emp AS (SELECT 2 AS id) SELECT id FROM tree ORDER BY id LIMIT (SELECT id FROM emp)",
        "id\n\
         1\n\
         2",
    )
}

/// ... from subqueries in BETWEEN and an IN list.
#[test]
fn shadow_in_between_and_in_list() -> TestResult {
    assert_query_on(
        FILES,
        "WITH emp AS (SELECT 6 AS id) SELECT id FROM tree WHERE id BETWEEN (SELECT id FROM emp) AND 9 OR id IN (1, (SELECT id - 1 FROM emp)) ORDER BY id",
        "id\n\
         1\n\
         5\n\
         6\n\
         7",
    )
}

/// ... from EXISTS.
#[test]
fn shadow_in_exists() -> TestResult {
    assert_query_on(
        FILES,
        "WITH emp AS (SELECT 100 AS id) SELECT COUNT(*) FROM tree WHERE EXISTS (SELECT 1 FROM emp WHERE emp.id = tree.id)",
        "COUNT(*)\n\
         0",
    )
}

/// ... from a compound's second arm.
#[test]
fn shadow_in_compound_arm() -> TestResult {
    assert_query_on(
        FILES,
        "WITH emp AS (SELECT 100 AS id) SELECT id FROM tree WHERE id = 1 UNION ALL SELECT id FROM emp",
        "id\n\
         1\n\
         100",
    )
}

/// ... from a derived table.
#[test]
fn shadow_in_derived_table() -> TestResult {
    assert_query_on(
        FILES,
        "WITH emp AS (SELECT 2 AS id) SELECT * FROM (SELECT id FROM emp) d",
        "id\n\
         2",
    )
}

/// ... from a sibling CTE's body.
#[test]
fn shadow_in_sibling_cte() -> TestResult {
    assert_query_on(
        FILES,
        "WITH emp AS (SELECT 2 AS id), d AS (SELECT id FROM emp) SELECT * FROM d",
        "id\n\
         2",
    )
}

/// ... from a subquery in UPDATE SET.
#[test]
fn shadow_in_update_set() -> TestResult {
    assert_query_on(
        FILES,
        "WITH emp AS (SELECT 100 AS id) UPDATE tree SET parent = (SELECT id FROM emp) WHERE id = 1; SELECT parent FROM tree WHERE id = 1",
        "parent\n\
         100",
    )
}

/// ... from an INSERT's source.
#[test]
fn shadow_in_insert_source() -> TestResult {
    assert_query_on(
        FILES,
        "WITH emp AS (SELECT 100 AS id) INSERT INTO tree (id) SELECT id FROM emp; SELECT COUNT(*) FROM tree WHERE id = 100",
        "COUNT(*)\n\
         1",
    )
}

/// ... from CREATE TABLE AS.
#[test]
fn shadow_in_ctas() -> TestResult {
    assert_query_on(
        FILES,
        "CREATE TABLE k AS WITH emp AS (SELECT 100 AS id) SELECT id FROM emp; SELECT * FROM k",
        "id\n\
         100",
    )
}

/// Outside the CTE's scope the same name is the loaded table.
#[test]
fn loaded_table_outside_cte_scope() -> TestResult {
    assert_query_on(
        FILES,
        "SELECT (SELECT COUNT(*) FROM emp) AS a, (WITH emp AS (SELECT 1 AS id) SELECT COUNT(*) FROM emp) AS b",
        "a,b\n\
         5,1",
    )
}

/// A reference outside an inner WITH reads the loaded table.
#[test]
fn loaded_table_before_inner_with() -> TestResult {
    assert_query_on(
        FILES,
        "SELECT COUNT(*) FROM emp WHERE id IN (WITH emp AS (SELECT 2 AS id) SELECT id FROM emp)",
        "COUNT(*)\n\
         1",
    )
}

// ---------------------------------------------------------------------------
// WITH RECURSIVE
// ---------------------------------------------------------------------------

/// `WITH RECURSIVE` counter.
#[test]
fn recursive_counter() -> TestResult {
    assert_query_on(
        FILES,
        "WITH RECURSIVE cnt(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM cnt WHERE n < 5) SELECT n FROM cnt",
        "n\n\
         1\n\
         2\n\
         3\n\
         4\n\
         5",
    )
}

/// A self-referencing CTE is recursive without the keyword.
#[test]
fn self_reference_without_keyword_is_recursive() -> TestResult {
    assert_query_on(
        FILES,
        "WITH cnt(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM cnt WHERE n < 5) SELECT n FROM cnt",
        "n\n\
         1\n\
         2\n\
         3\n\
         4\n\
         5",
    )
}

/// Without a column list, the setup arm names the columns.
#[test]
fn recursive_names_from_setup() -> TestResult {
    assert_query_on(
        FILES,
        "WITH RECURSIVE c AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM c WHERE n < 3) SELECT n FROM c",
        "n\n\
         1\n\
         2\n\
         3",
    )
}

/// Breadth-first tree walk (FIFO queue).
#[test]
fn recursive_tree_walk() -> TestResult {
    assert_query_on(
        FILES,
        "WITH RECURSIVE t(id, d) AS (SELECT id, 0 FROM tree WHERE parent IS NULL UNION ALL SELECT tree.id, t.d + 1 FROM tree JOIN t ON tree.parent = t.id) SELECT id, d FROM t",
        "id,d\n\
         1,0\n\
         2,1\n\
         3,1\n\
         4,2\n\
         5,2\n\
         6,2\n\
         7,3",
    )
}

/// `ORDER BY 2 DESC` in the body makes the queue depth-first.
#[test]
fn recursive_depth_first() -> TestResult {
    assert_query_on(
        FILES,
        "WITH RECURSIVE t(id, d) AS (SELECT id, 0 FROM tree WHERE parent IS NULL UNION ALL SELECT tree.id, t.d + 1 FROM tree JOIN t ON tree.parent = t.id ORDER BY 2 DESC) SELECT id, d FROM t",
        "id,d\n\
         1,0\n\
         2,1\n\
         4,2\n\
         7,3\n\
         5,2\n\
         3,1\n\
         6,2",
    )
}

/// A body ORDER BY may name a setup column.
#[test]
fn recursive_order_by_name() -> TestResult {
    assert_query_on(
        FILES,
        "WITH RECURSIVE t(id, d) AS (SELECT id, 0 AS depth FROM tree WHERE parent IS NULL UNION ALL SELECT tree.id, t.d + 1 FROM tree JOIN t ON tree.parent = t.id ORDER BY depth DESC, id DESC) SELECT id, d FROM t",
        "id,d\n\
         1,0\n\
         3,1\n\
         6,2\n\
         2,1\n\
         5,2\n\
         4,2\n\
         7,3",
    )
}

/// Paths built by concatenation.
#[test]
fn recursive_paths() -> TestResult {
    assert_query_on(
        FILES,
        "WITH RECURSIVE p(id, path) AS (SELECT id, CAST(id AS TEXT) FROM tree WHERE parent IS NULL UNION ALL SELECT tree.id, p.path || '/' || tree.id FROM tree JOIN p ON tree.parent = p.id) SELECT id, path FROM p ORDER BY path",
        "id,path\n\
         1,1\n\
         2,1/2\n\
         4,1/2/4\n\
         7,1/2/4/7\n\
         5,1/2/5\n\
         3,1/3\n\
         6,1/3/6",
    )
}

/// UNION discards rows already produced, so a cycle terminates.
#[test]
fn recursive_union_dedupes() -> TestResult {
    assert_query_on(
        FILES,
        "WITH RECURSIVE c(n) AS (SELECT 1 UNION SELECT n % 3 + 1 FROM c) SELECT n FROM c",
        "n\n\
         1\n\
         2\n\
         3",
    )
}

/// Under UNION, duplicate setup rows enter the queue once.
#[test]
fn recursive_union_setup_dedupes() -> TestResult {
    assert_query_on(
        FILES,
        "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT 1 UNION SELECT n + 1 FROM c WHERE n < 3) SELECT n FROM c",
        "n\n\
         1\n\
         2\n\
         3",
    )
}

/// UNION ALL keeps every row.
#[test]
fn recursive_union_all_keeps_duplicates() -> TestResult {
    assert_query_on(
        FILES,
        "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < 3) SELECT n FROM c",
        "n\n\
         1\n\
         1\n\
         2\n\
         2\n\
         3\n\
         3",
    )
}

/// LIMIT ends an otherwise infinite recursion.
#[test]
fn recursive_limit_stops_recursion() -> TestResult {
    assert_query_on(
        FILES,
        "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c LIMIT 4) SELECT n FROM c",
        "n\n\
         1\n\
         2\n\
         3\n\
         4",
    )
}

/// OFFSET skips output rows but they still recurse.
#[test]
fn recursive_limit_offset() -> TestResult {
    assert_query_on(
        FILES,
        "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < 6 LIMIT 3 OFFSET 2) SELECT n FROM c",
        "n\n\
         3\n\
         4\n\
         5",
    )
}

/// Several UNION ALL recursive arms run for each row, in order.
#[test]
fn recursive_multiple_arms() -> TestResult {
    assert_query_on(
        FILES,
        "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < 3 UNION ALL SELECT n + 10 FROM c WHERE n < 3) SELECT n FROM c",
        "n\n\
         1\n\
         2\n\
         11\n\
         3\n\
         12",
    )
}

/// Two-column recursion.
#[test]
fn recursive_fibonacci() -> TestResult {
    assert_query_on(
        FILES,
        "WITH RECURSIVE f(a, b) AS (SELECT 0, 1 UNION ALL SELECT b, a + b FROM f WHERE b < 50) SELECT a FROM f",
        "a\n\
         0\n\
         1\n\
         1\n\
         2\n\
         3\n\
         5\n\
         8\n\
         13\n\
         21\n\
         34",
    )
}

/// A recursive CTE joined to itself.
#[test]
fn recursive_referenced_twice() -> TestResult {
    assert_query_on(
        FILES,
        "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < 3) SELECT c1.n, c2.n FROM c c1 JOIN c c2 ON c1.n = c2.n",
        "n,n\n\
         1,1\n\
         2,2\n\
         3,3",
    )
}

/// A recursive CTE drives a DELETE.
#[test]
fn recursive_in_delete() -> TestResult {
    assert_query_on(
        FILES,
        "WITH RECURSIVE s(id) AS (SELECT 2 UNION ALL SELECT tree.id FROM tree JOIN s ON tree.parent = s.id) DELETE FROM tree WHERE id IN (SELECT id FROM s); SELECT id FROM tree ORDER BY id",
        "id\n\
         1\n\
         3\n\
         6",
    )
}

/// Aggregates over a recursive CTE are fine.
#[test]
fn recursive_with_aggregate_outside() -> TestResult {
    assert_query_on(
        FILES,
        "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < 100) SELECT COUNT(*), SUM(n) FROM c",
        "COUNT(*),SUM(n)\n\
         100,5050",
    )
}

/// A recursive CTE inside a subquery.
#[test]
fn recursive_nested_in_subquery() -> TestResult {
    assert_query_on(
        FILES,
        "SELECT id FROM tree WHERE id IN (WITH RECURSIVE s(x) AS (SELECT 3 UNION ALL SELECT tree.id FROM tree, s WHERE tree.parent = s.x) SELECT x FROM s) ORDER BY id",
        "id\n\
         3\n\
         6",
    )
}

// ---------------------------------------------------------------------------
// WITH RECURSIVE errors
// ---------------------------------------------------------------------------

/// A recursive arm may reference the CTE once.
#[test]
fn recursive_two_direct_references() -> TestResult {
    assert_query_on_fails(
        FILES,
        "WITH c(n) AS (SELECT 1 UNION ALL SELECT c.n + 1 FROM c, c AS d WHERE c.n < 3) SELECT * FROM c",
        "multiple references to recursive table: c",
    )
}

/// A recursive arm may not also reference the CTE in a subquery.
#[test]
fn recursive_reference_in_subquery() -> TestResult {
    assert_query_on_fails(
        FILES,
        "WITH c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < 3 AND n IN (SELECT n FROM c)) SELECT * FROM c",
        "multiple recursive references: c",
    )
}

/// A self-reference only in a subquery is circular.
#[test]
fn self_reference_only_in_subquery() -> TestResult {
    assert_query_on_fails(
        FILES,
        "WITH c AS (SELECT 1 UNION SELECT (SELECT COUNT(*) FROM c)) SELECT * FROM c",
        "circular reference: c",
    )
}

/// A self-reference in a derived table is circular.
#[test]
fn self_reference_in_derived_table() -> TestResult {
    assert_query_on_fails(
        FILES,
        "WITH c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM (SELECT * FROM c) WHERE n < 3) SELECT * FROM c",
        "circular reference: c",
    )
}

/// Recursion requires UNION or UNION ALL.
#[test]
fn self_reference_under_intersect() -> TestResult {
    assert_query_on_fails(
        FILES,
        "WITH c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c INTERSECT SELECT 2) SELECT * FROM c",
        "circular reference: c",
    )
}

/// The setup (leftmost) arm may not reference the CTE.
#[test]
fn self_reference_in_setup() -> TestResult {
    assert_query_on_fails(
        FILES,
        "WITH c(n) AS (SELECT n FROM c UNION ALL SELECT 1) SELECT * FROM c",
        "circular reference: c",
    )
}

/// A recursive arm may not aggregate.
#[test]
fn recursive_aggregate() -> TestResult {
    assert_query_on_fails(
        FILES,
        "WITH c(n) AS (SELECT 1 UNION ALL SELECT COUNT(*) FROM c) SELECT * FROM c",
        "recursive aggregate queries not supported",
    )
}

/// A recursive arm may not GROUP BY.
#[test]
fn recursive_group_by() -> TestResult {
    assert_query_on_fails(
        FILES,
        "WITH c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c GROUP BY n) SELECT * FROM c",
        "recursive aggregate queries not supported",
    )
}

/// A recursive arm may not use window functions.
#[test]
fn recursive_window() -> TestResult {
    assert_query_on_fails(
        FILES,
        "WITH c(n) AS (SELECT 1 UNION ALL SELECT ROW_NUMBER() OVER () FROM c) SELECT * FROM c",
        "cannot use window functions in recursive queries",
    )
}

/// A recursive body's ORDER BY must name a result column.
#[test]
fn recursive_order_by_unknown() -> TestResult {
    assert_query_on_fails(
        FILES,
        "WITH c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < 5 ORDER BY nosuch) SELECT * FROM c",
        "1st ORDER BY term does not match any column in the result set",
    )
}

/// A recursive body's ORDER BY position must be in range.
#[test]
fn recursive_order_by_out_of_range() -> TestResult {
    assert_query_on_fails(
        FILES,
        "WITH c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < 5 ORDER BY 3) SELECT * FROM c",
        "1st ORDER BY term out of range - should be between 1 and 1",
    )
}

/// Every arm has the setup's column count.
#[test]
fn recursive_arm_column_count() -> TestResult {
    assert_query_on_fails(
        FILES,
        "WITH c(n) AS (SELECT 1 UNION ALL SELECT n + 1, 2 FROM c WHERE n < 5) SELECT * FROM c",
        "SELECTs to the left and right of UNION ALL do not have the same number of result columns",
    )
}

/// A recursive body's LIMIT must be an integer.
#[test]
fn recursive_limit_must_be_integer() -> TestResult {
    assert_query_on_fails(
        FILES,
        "WITH c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < 3 LIMIT 'x') SELECT * FROM c",
        "datatype mismatch",
    )
}
