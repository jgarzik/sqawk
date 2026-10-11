//! Exact-output subquery tests: every row, in order.
//!
//! Subqueries are compiled by the same SELECT pipeline as any query, so these
//! exercise clauses inside a subquery (GROUP BY, ORDER BY, LIMIT, DISTINCT,
//! joins, compounds), correlation at any depth, and subqueries in every
//! expression position. Expected outputs were computed by SQLite (python3
//! `sqlite3`) over the same data.

use crate::helpers::{assert_query, assert_query_fails, create_custom_csv, create_temp_dir};
use tempfile::TempDir;

type TestResult = Result<(), Box<dyn std::error::Error>>;

const EMP: &str = "id,name,dept,salary,mgr\n\
1,Alice,Eng,100,\n\
2,Bob,Eng,80,1\n\
3,Carol,Sales,70,1\n\
4,Dave,Sales,70,3\n\
5,Eve,,50,3\n\
6,Frank,HR,,1\n\
7,Gina,Eng,90,2\n";

const DEPT: &str = "dept,dname,floor\n\
Eng,Engineering,3\n\
Sales,Sales Team,1\n\
HR,Human Res,2\n\
Ops,Operations,\n";

/// `k` holds a duplicate and a NULL.
const KS: &str = "k,tag\n1,a\n2,b\n2,c\n,d\n3,e\n";

/// Run `sql` over emp, dept and ks and compare the complete output.
fn check(sql: &str, expected: &str) -> TestResult {
    let dir: TempDir = create_temp_dir()?;
    let emp = create_custom_csv(dir.path(), "emp.csv", EMP)?;
    let dept = create_custom_csv(dir.path(), "dept.csv", DEPT)?;
    let ks = create_custom_csv(dir.path(), "ks.csv", KS)?;
    assert_query(
        &[
            emp.to_str().unwrap(),
            dept.to_str().unwrap(),
            ks.to_str().unwrap(),
        ],
        sql,
        expected,
    )
}

fn check_fails(sql: &str, expected_err: &str) -> TestResult {
    let dir: TempDir = create_temp_dir()?;
    let emp = create_custom_csv(dir.path(), "emp.csv", EMP)?;
    assert_query_fails(&[emp.to_str().unwrap()], sql, expected_err)
}

// ---- Clauses inside a subquery

#[test]
fn scalar_subquery_with_group_by_order_by_limit() -> TestResult {
    check(
        "SELECT name FROM emp WHERE dept = (SELECT dept FROM emp GROUP BY dept ORDER BY COUNT(*) DESC, dept LIMIT 1) ORDER BY id",
        "name\nAlice\nBob\nGina",
    )
}

#[test]
fn scalar_subquery_with_limit_offset() -> TestResult {
    check(
        "SELECT (SELECT dept FROM emp WHERE dept IS NOT NULL GROUP BY dept ORDER BY SUM(salary) DESC LIMIT 1 OFFSET 1) AS d",
        "d\nSales",
    )
}

#[test]
fn scalar_subquery_no_rows_is_null_and_many_rows_takes_first() -> TestResult {
    check(
        "SELECT (SELECT name FROM emp WHERE id > 100) AS none, (SELECT name FROM emp ORDER BY id DESC) AS first",
        "none,first\n,Gina",
    )
}

#[test]
fn scalar_subquery_compound_with_order_by_limit() -> TestResult {
    check(
        "SELECT (SELECT k FROM ks WHERE k IS NOT NULL UNION SELECT 9 ORDER BY 1 DESC LIMIT 1) AS m",
        "m\n9",
    )
}

#[test]
fn in_subquery_compound() -> TestResult {
    check(
        "SELECT id FROM emp WHERE id IN (SELECT 1 UNION SELECT 3) ORDER BY id",
        "id\n1\n3",
    )
}

// ---- IN / NOT IN three-valued logic

#[test]
fn in_subquery_distinct_with_null() -> TestResult {
    check(
        "SELECT id FROM emp WHERE id IN (SELECT DISTINCT k FROM ks) ORDER BY id",
        "id\n1\n2\n3",
    )
}

/// The set holds a NULL, so NOT IN is never true.
#[test]
fn not_in_subquery_with_null_matches_nothing() -> TestResult {
    check(
        "SELECT id FROM emp WHERE id NOT IN (SELECT DISTINCT k FROM ks) ORDER BY id",
        "id",
    )
}

#[test]
fn in_subquery_values_are_three_valued() -> TestResult {
    check(
        "SELECT id, id IN (SELECT k FROM ks) AS i, id NOT IN (SELECT k FROM ks) AS ni, id NOT IN (SELECT k FROM ks WHERE k IS NOT NULL) AS nn FROM emp ORDER BY id",
        "id,i,ni,nn\n1,1,0,0\n2,1,0,0\n3,1,0,0\n4,,,1\n5,,,1\n6,,,1\n7,,,1",
    )
}

/// Over an empty set IN is false even for a NULL operand; over a non-empty
/// set a NULL operand is unknown.
#[test]
fn in_subquery_null_operand() -> TestResult {
    check(
        "SELECT id, mgr IN (SELECT k FROM ks WHERE k > 100) AS e, mgr NOT IN (SELECT k FROM ks WHERE k > 100) AS ne, mgr IN (SELECT k FROM ks) AS f FROM emp ORDER BY id",
        "id,e,ne,f\n1,0,1,\n2,0,1,1\n3,0,1,1\n4,0,1,1\n5,0,1,1\n6,0,1,1\n7,0,1,1",
    )
}

#[test]
fn correlated_in_and_not_in() -> TestResult {
    check(
        "SELECT e.name FROM emp e WHERE e.mgr IN (SELECT m.id FROM emp m WHERE m.dept = e.dept) ORDER BY e.name",
        "name\nBob\nDave\nGina",
    )?;
    check(
        "SELECT e.name FROM emp e WHERE e.mgr NOT IN (SELECT m.id FROM emp m WHERE m.dept <> e.dept) ORDER BY e.name",
        "name\nBob\nDave\nEve\nGina",
    )
}

// ---- EXISTS

#[test]
fn correlated_exists_with_join_inside() -> TestResult {
    check(
        "SELECT d.dname FROM dept d WHERE EXISTS (SELECT 1 FROM emp e JOIN emp m ON e.mgr = m.id WHERE e.dept = d.dept AND m.salary > 80) ORDER BY d.dname",
        "dname\nEngineering\nHuman Res\nSales Team",
    )
}

#[test]
fn uncorrelated_exists_with_left_join_inside() -> TestResult {
    check(
        "SELECT COUNT(*) AS n FROM dept WHERE EXISTS (SELECT 1 FROM emp e LEFT JOIN dept x ON x.dept = e.dept WHERE x.dept IS NULL)",
        "n\n4",
    )
}

#[test]
fn not_exists_correlated() -> TestResult {
    check(
        "SELECT d.dept FROM dept d WHERE NOT EXISTS (SELECT 1 FROM emp e WHERE e.dept = d.dept)",
        "dept\nOps",
    )?;
    // A NULL outer key matches no row.
    check(
        "SELECT e.name FROM emp e WHERE NOT EXISTS (SELECT 1 FROM dept d WHERE d.dept = e.dept)",
        "name\nEve",
    )
}

#[test]
fn exists_as_value() -> TestResult {
    check(
        "SELECT EXISTS (SELECT 1 FROM dept WHERE floor = 1) AS e, NOT EXISTS (SELECT 1 FROM dept WHERE floor = 9) AS ne",
        "e,ne\n1,1",
    )
}

// ---- Correlation

#[test]
fn correlated_scalar_non_aggregate_with_order_by_limit() -> TestResult {
    check(
        "SELECT d.dept, (SELECT e.name FROM emp e WHERE e.dept = d.dept ORDER BY e.salary DESC, e.name LIMIT 1) AS top FROM dept d ORDER BY d.dept",
        "dept,top\nEng,Alice\nHR,Frank\nOps,\nSales,Carol",
    )
}

/// The innermost subquery references the outermost query, two levels up.
#[test]
fn correlated_two_levels_up() -> TestResult {
    check(
        "SELECT d.dept FROM dept d WHERE EXISTS (SELECT 1 FROM emp e WHERE e.dept = d.dept AND e.salary > (SELECT AVG(e2.salary) FROM emp e2 WHERE e2.dept = d.dept)) ORDER BY 1",
        "dept\nEng",
    )
}

/// The innermost subquery references both enclosing queries.
#[test]
fn correlated_to_both_enclosing_queries() -> TestResult {
    check(
        "SELECT d.dept, (SELECT COUNT(*) FROM emp e WHERE e.dept = d.dept AND e.salary >= (SELECT MIN(e2.salary) FROM emp e2 WHERE e2.dept = d.dept AND e2.mgr = e.mgr)) AS n FROM dept d ORDER BY d.dept",
        "dept,n\nEng,2\nHR,0\nOps,0\nSales,2",
    )
}

#[test]
fn correlated_with_group_by_and_distinct_inside() -> TestResult {
    check(
        "SELECT d.dept, (SELECT e.mgr FROM emp e WHERE e.dept = d.dept GROUP BY e.mgr ORDER BY COUNT(*) DESC, e.mgr LIMIT 1) AS m, (SELECT COUNT(DISTINCT e.salary) FROM emp e WHERE e.dept = d.dept) AS c FROM dept d ORDER BY d.dept",
        "dept,m,c\nEng,,3\nHR,1,0\nOps,,0\nSales,1,1",
    )
}

#[test]
fn correlated_with_left_join_inside() -> TestResult {
    check(
        "SELECT d.dept, (SELECT COUNT(*) FROM emp e LEFT JOIN emp m ON m.id = e.mgr AND m.dept = d.dept WHERE e.dept = d.dept AND m.id IS NULL) AS orphans FROM dept d ORDER BY d.dept",
        "dept,orphans\nEng,1\nHR,1\nOps,0\nSales,1",
    )
}

#[test]
fn correlated_in_having() -> TestResult {
    check(
        "SELECT dept, COUNT(*) AS n FROM emp GROUP BY dept HAVING COUNT(*) >= (SELECT floor FROM dept WHERE dept.dept = emp.dept) ORDER BY dept",
        "dept,n\nEng,3\nSales,2",
    )
}

#[test]
fn correlated_in_grouped_select_list() -> TestResult {
    check(
        "SELECT dept, (SELECT dname FROM dept WHERE dept.dept = emp.dept) AS dn, COUNT(*) AS n FROM emp GROUP BY dept ORDER BY dept",
        "dept,dn,n\n,,1\nEng,Engineering,3\nHR,Human Res,1\nSales,Sales Team,2",
    )
}

/// Inner names shadow outer ones; the outer table is found by its own name,
/// and an unqualified outer column resolves outward.
#[test]
fn correlated_name_resolution() -> TestResult {
    check(
        "SELECT name FROM emp e WHERE salary = (SELECT MAX(salary) FROM emp WHERE emp.dept = e.dept) ORDER BY name",
        "name\nAlice\nCarol\nDave",
    )?;
    check(
        "SELECT COUNT(*) AS n FROM emp WHERE EXISTS (SELECT 1 FROM dept WHERE dept = 'Ops')",
        "n\n7",
    )?;
    check(
        "SELECT dname FROM dept WHERE EXISTS (SELECT 1 FROM emp WHERE salary > 75 AND emp.dept = SUBSTR(dname, 1, 3))",
        "dname\nEngineering",
    )
}

#[test]
fn correlated_subquery_without_from() -> TestResult {
    check(
        "SELECT name, (SELECT emp.salary * 2) AS d, (SELECT emp.id + floor FROM dept WHERE dept = 'HR') AS f FROM emp WHERE id < 4 ORDER BY id",
        "name,d,f\nAlice,200,3\nBob,160,4\nCarol,140,5",
    )
}

// ---- Expression positions

#[test]
fn subquery_in_order_by() -> TestResult {
    check(
        "SELECT name FROM emp ORDER BY (SELECT floor FROM dept WHERE dept.dept = emp.dept), id",
        "name\nEve\nCarol\nDave\nFrank\nAlice\nBob\nGina",
    )?;
    check(
        "SELECT name, (SELECT COUNT(*) FROM emp r WHERE r.mgr = emp.id) AS reports FROM emp ORDER BY reports DESC, name LIMIT 3",
        "name,reports\nAlice,3\nCarol,2\nBob,1",
    )
}

#[test]
fn subquery_in_case_and_function_args() -> TestResult {
    check(
        "SELECT name, CASE WHEN salary > (SELECT AVG(salary) FROM emp) THEN 'hi' ELSE 'lo' END AS lvl FROM emp ORDER BY id",
        "name,lvl\nAlice,hi\nBob,hi\nCarol,lo\nDave,lo\nEve,lo\nFrank,lo\nGina,hi",
    )?;
    check(
        "SELECT UPPER((SELECT dname FROM dept WHERE dept = 'HR')) AS u, LENGTH((SELECT dname FROM dept WHERE floor = 1)) AS l",
        "u,l\nHUMAN RES,10",
    )
}

#[test]
fn subquery_in_aggregate_argument() -> TestResult {
    check(
        "SELECT SUM((SELECT floor FROM dept WHERE dept.dept = emp.dept)) AS s FROM emp",
        "s\n13",
    )?;
    check(
        "SELECT MAX((SELECT COUNT(*) FROM emp r WHERE r.mgr = emp.id)) AS m FROM emp",
        "m\n3",
    )
}

#[test]
fn subquery_in_join_on() -> TestResult {
    check(
        "SELECT e.name AS name, d.dname AS dname FROM emp e JOIN dept d ON d.dept = e.dept AND d.floor = (SELECT MAX(floor) FROM dept) ORDER BY e.name",
        "name,dname\nAlice,Engineering\nBob,Engineering\nGina,Engineering",
    )
}

#[test]
fn subquery_in_limit() -> TestResult {
    check(
        "SELECT id FROM emp LIMIT (SELECT COUNT(*) FROM dept)",
        "id\n1\n2\n3\n4",
    )
}

// ---- DML

#[test]
fn subquery_in_update_set() -> TestResult {
    check(
        "UPDATE emp SET mgr = (SELECT MAX(e2.salary) FROM emp e2 WHERE e2.dept = emp.dept) + 1 WHERE dept = 'Sales'; SELECT id, mgr FROM emp ORDER BY id",
        "id,mgr\n1,\n2,1\n3,71\n4,71\n5,3\n6,1\n7,2",
    )?;
    check(
        "UPDATE emp SET salary = (SELECT MIN(salary) FROM emp) WHERE salary IS NULL; SELECT id, salary FROM emp ORDER BY id",
        "id,salary\n1,100\n2,80\n3,70\n4,70\n5,50\n6,50\n7,90",
    )
}

#[test]
fn subquery_in_delete_where() -> TestResult {
    check(
        "DELETE FROM emp WHERE salary < (SELECT AVG(e2.salary) FROM emp e2 WHERE e2.dept = emp.dept); SELECT id FROM emp ORDER BY id",
        "id\n1\n3\n4\n5\n6\n7",
    )?;
    check(
        "DELETE FROM emp WHERE dept NOT IN (SELECT dept FROM dept WHERE floor >= 2); SELECT id FROM emp ORDER BY id",
        "id\n1\n2\n5\n6\n7",
    )
}

#[test]
fn subquery_in_insert_values() -> TestResult {
    check(
        "INSERT INTO emp VALUES ((SELECT MAX(id) + 1 FROM emp), 'Zed', 'Ops', (SELECT MIN(salary) FROM emp), NULL); SELECT id, name, salary FROM emp WHERE dept = 'Ops'",
        "id,name,salary\n8,Zed,50",
    )
}

// ---- Errors

#[test]
fn scalar_subquery_with_two_columns_is_error() -> TestResult {
    check_fails(
        "SELECT id, (SELECT id, name FROM emp) FROM emp",
        "sub-select returns 2 columns - expected 1",
    )
}

#[test]
fn in_subquery_with_two_columns_is_error() -> TestResult {
    check_fails(
        "SELECT id FROM emp WHERE id IN (SELECT id, name FROM emp)",
        "sub-select returns 2 columns - expected 1",
    )
}

#[test]
fn row_value_in_subquery_is_unsupported() -> TestResult {
    check_fails(
        "SELECT id FROM emp WHERE (id, name) IN (SELECT id, name FROM emp)",
        "Unsupported",
    )
}
