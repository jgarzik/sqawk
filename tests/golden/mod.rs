//! Characterization ("golden") tests: exact-output pins on behaviour that
//! sqawk currently gets RIGHT.
//!
//! Generated from real output, then reviewed. These exist to be a semantic
//! oracle for refactors -- most immediately the direct sqlparser 0.36 -> 0.62
//! jump, which has no bisection handle and ~130 mechanical edit sites.
//!
//! Unlike the rest of the suite, these assert the COMPLETE stdout: every row,
//! in order. If a refactor drops a row, reorders output, renames a header, or
//! changes a value, exactly one of these fails and names the query.
//!
//! Known-wrong behaviour is NOT pinned here -- it lives in `tests/defects/`.

use crate::helpers::assert_query;

#[test]
fn select_star() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT * FROM employees",
        concat!(
            "id,name,age,salary,department,role",
            "\n1,Alice,30,70000,Engineering,Developer",
            "\n2,Bob,25,55000,Marketing,Specialist",
            "\n3,Charlie,35,65000,Engineering,Manager",
            "\n4,David,40,80000,Sales,Director",
            "\n5,Eve,28,60000,Marketing,Analyst",
            "\n6,Frank,32,75000,Engineering,Developer",
            "\n7,Grace,45,90000,Sales,Director",
            "\n8,Henry,22,45000,HR,Intern",
        ),
    )
}

#[test]
fn select_columns() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name, salary FROM employees",
        concat!(
            "name,salary",
            "\nAlice,70000",
            "\nBob,55000",
            "\nCharlie,65000",
            "\nDavid,80000",
            "\nEve,60000",
            "\nFrank,75000",
            "\nGrace,90000",
            "\nHenry,45000",
        ),
    )
}

#[test]
fn select_literal() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT 1",
        concat!("expr", "\n1",),
    )
}

/// A FROM-less SELECT goes through the same expression compiler as any
/// other, so string functions, CAST, CASE and `||` all work without FROM.
#[test]
fn select_literal_expressions() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT UPPER('ab') || 'c', CAST('12' AS INTEGER) + 1, CASE WHEN 1 > 2 THEN 'x' ELSE 'y' END, SUBSTR('hello', 2, 3), -ABS(-4)",
        concat!("expr,expr,expr,SUBSTR(expr),expr", "\nABc,13,y,ell,-4"),
    )
}

/// Predicates are values under three-valued logic, in any position.
#[test]
fn select_three_valued_predicates() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT NULL AND 0, NULL OR 1, NULL OR 0, 2 IN (1, NULL), 1 IN (1, NULL), NULL BETWEEN 1 AND 2, NULL LIKE 'a%'",
        concat!("expr,expr,expr,expr,expr,expr,expr", "\n0,1,,,1,,"),
    )
}

#[test]
fn where_eq() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees WHERE department = 'Engineering'",
        concat!("name", "\nAlice", "\nCharlie", "\nFrank",),
    )
}

#[test]
fn where_gt() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees WHERE salary > 70000",
        concat!("name", "\nDavid", "\nFrank", "\nGrace",),
    )
}

#[test]
fn where_and() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees WHERE salary > 60000 AND department = 'Engineering'",
        concat!("name", "\nAlice", "\nCharlie", "\nFrank",),
    )
}

#[test]
fn where_or() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees WHERE department = 'HR' OR department = 'Sales'",
        concat!("name", "\nDavid", "\nGrace", "\nHenry",),
    )
}

#[test]
fn where_ne() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees WHERE department <> 'Engineering'",
        concat!("name", "\nBob", "\nDavid", "\nEve", "\nGrace", "\nHenry",),
    )
}

#[test]
fn where_like() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees WHERE name LIKE 'A%'",
        concat!("name", "\nAlice",),
    )
}

#[test]
fn where_not_like() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees WHERE name NOT LIKE 'A%'",
        concat!(
            "name",
            "\nBob",
            "\nCharlie",
            "\nDavid",
            "\nEve",
            "\nFrank",
            "\nGrace",
            "\nHenry",
        ),
    )
}

#[test]
fn where_ilike() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees WHERE name ILIKE 'a%'",
        concat!("name", "\nAlice",),
    )
}

#[test]
fn where_between() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees WHERE age BETWEEN 30 AND 40",
        concat!("name", "\nAlice", "\nCharlie", "\nDavid", "\nFrank",),
    )
}

#[test]
fn where_not_between() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees WHERE age NOT BETWEEN 30 AND 40",
        concat!("name", "\nBob", "\nEve", "\nGrace", "\nHenry",),
    )
}

#[test]
fn where_in_list() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees WHERE department IN ('HR', 'Sales')",
        concat!("name", "\nDavid", "\nGrace", "\nHenry",),
    )
}

#[test]
fn where_not_in_list() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees WHERE department NOT IN ('HR', 'Sales')",
        concat!("name", "\nAlice", "\nBob", "\nCharlie", "\nEve", "\nFrank",),
    )
}

#[test]
fn where_is_null() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/nullable.csv"],
        "SELECT id FROM nullable WHERE score IS NULL",
        concat!("id", "\n2",),
    )
}

#[test]
fn where_is_not_null() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/nullable.csv"],
        "SELECT id FROM nullable WHERE score IS NOT NULL",
        concat!("id", "\n1", "\n3", "\n4", "\n5",),
    )
}

#[test]
fn order_by_asc() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name, salary FROM employees ORDER BY salary ASC",
        concat!(
            "name,salary",
            "\nHenry,45000",
            "\nBob,55000",
            "\nEve,60000",
            "\nCharlie,65000",
            "\nAlice,70000",
            "\nFrank,75000",
            "\nDavid,80000",
            "\nGrace,90000",
        ),
    )
}

#[test]
fn order_by_desc() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name, salary FROM employees ORDER BY salary DESC",
        concat!(
            "name,salary",
            "\nGrace,90000",
            "\nDavid,80000",
            "\nFrank,75000",
            "\nAlice,70000",
            "\nCharlie,65000",
            "\nEve,60000",
            "\nBob,55000",
            "\nHenry,45000",
        ),
    )
}

#[test]
fn order_by_multi() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT department, name FROM employees ORDER BY department ASC, name DESC",
        concat!(
            "department,name",
            "\nEngineering,Frank",
            "\nEngineering,Charlie",
            "\nEngineering,Alice",
            "\nHR,Henry",
            "\nMarketing,Eve",
            "\nMarketing,Bob",
            "\nSales,Grace",
            "\nSales,David",
        ),
    )
}

#[test]
fn limit() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees LIMIT 3",
        concat!("name", "\nAlice", "\nBob", "\nCharlie",),
    )
}

#[test]
fn limit_offset() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees LIMIT 3 OFFSET 2",
        concat!("name", "\nCharlie", "\nDavid", "\nEve",),
    )
}

#[test]
fn order_by_limit() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 3",
        concat!(
            "name,salary",
            "\nGrace,90000",
            "\nDavid,80000",
            "\nFrank,75000",
        ),
    )
}

#[test]
fn distinct() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT DISTINCT department FROM employees",
        concat!(
            "department",
            "\nEngineering",
            "\nMarketing",
            "\nSales",
            "\nHR",
        ),
    )
}

#[test]
fn distinct_star() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/duplicates.csv"],
        "SELECT DISTINCT name, department FROM duplicates",
        concat!(
            "name,department",
            "\nAlice,Engineering",
            "\nBob,Marketing",
            "\nCharlie,Engineering",
            "\nDave,Finance",
            "\nEve,HR",
            "\nFrank,Sales",
        ),
    )
}

#[test]
fn agg_count() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT COUNT(*) FROM employees",
        concat!("COUNT", "\n8",),
    )
}

#[test]
fn agg_sum() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT SUM(salary) FROM employees",
        concat!("SUM", "\n540000",),
    )
}

#[test]
fn agg_avg() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT AVG(salary) FROM employees",
        concat!("AVG", "\n67500",),
    )
}

#[test]
fn agg_min_max() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT MIN(salary), MAX(salary) FROM employees",
        concat!("MIN,MAX", "\n45000,90000",),
    )
}

#[test]
fn agg_with_where() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT COUNT(*) FROM employees WHERE department = 'Engineering'",
        concat!("COUNT", "\n3",),
    )
}

#[test]
fn agg_alias() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT COUNT(*) AS n, SUM(salary) AS total FROM employees",
        concat!("n,total", "\n8,540000",),
    )
}

#[test]
fn group_by_basic() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT department, COUNT(*) FROM employees GROUP BY department",
        concat!(
            "department,COUNT",
            "\nEngineering,3",
            "\nHR,1",
            "\nMarketing,2",
            "\nSales,2",
        ),
    )
}

#[test]
fn group_by_sum() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT department, SUM(salary) FROM employees GROUP BY department",
        concat!(
            "department,SUM",
            "\nEngineering,210000",
            "\nHR,45000",
            "\nMarketing,115000",
            "\nSales,170000",
        ),
    )
}

#[test]
fn group_by_having() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > 1",
        concat!(
            "department,COUNT",
            "\nEngineering,3",
            "\nMarketing,2",
            "\nSales,2",
        ),
    )
}

#[test]
fn join_on() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/users.csv", "tests/data/orders.csv"],
        "SELECT users.name, orders.id FROM users JOIN orders ON users.id = orders.user_id",
        concat!(
            "users.name,orders.id",
            "\nJohn,101",
            "\nJohn,103",
            "\nJohn,105",
            "\nJane,102",
            "\nJane,104",
        ),
    )
}

#[test]
fn join_inner() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/users.csv", "tests/data/orders.csv"],
        "SELECT users.name, orders.date FROM users INNER JOIN orders ON users.id = orders.user_id",
        concat!(
            "users.name,orders.date",
            "\nJohn,2023-01-15",
            "\nJohn,2023-02-10",
            "\nJohn,2023-03-05",
            "\nJane,2023-01-20",
            "\nJane,2023-02-25",
        ),
    )
}

#[test]
fn join_left() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/users.csv", "tests/data/orders.csv"],
        "SELECT users.name, orders.id FROM users LEFT JOIN orders ON users.id = orders.user_id",
        concat!(
            "users.name,orders.id",
            "\nJohn,101",
            "\nJohn,103",
            "\nJohn,105",
            "\nJane,102",
            "\nJane,104",
        ),
    )
}

#[test]
fn join_right() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/users.csv", "tests/data/orders.csv"],
        "SELECT users.name, orders.id FROM users RIGHT JOIN orders ON users.id = orders.user_id",
        concat!(
            "users.name,orders.id",
            "\nJohn,101",
            "\nJane,102",
            "\nJohn,103",
            "\nJane,104",
            "\nJohn,105",
        ),
    )
}

#[test]
fn join_full() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/users.csv", "tests/data/orders.csv"],
        "SELECT users.name, orders.id FROM users FULL JOIN orders ON users.id = orders.user_id",
        concat!(
            "users.name,orders.id",
            "\nJohn,101",
            "\nJohn,103",
            "\nJohn,105",
            "\nJane,102",
            "\nJane,104",
        ),
    )
}

#[test]
fn join_cross() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/users.csv", "tests/data/orders.csv"],
        "SELECT users.name, orders.id FROM users CROSS JOIN orders",
        concat!(
            "users.name,orders.id",
            "\nJohn,101",
            "\nJohn,102",
            "\nJohn,103",
            "\nJohn,104",
            "\nJohn,105",
            "\nJane,101",
            "\nJane,102",
            "\nJane,103",
            "\nJane,104",
            "\nJane,105",
        ),
    )
}

#[test]
fn join_implicit() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/users.csv", "tests/data/orders.csv"],
        "SELECT users.name, orders.id FROM users, orders WHERE users.id = orders.user_id",
        concat!(
            "users.name,orders.id",
            "\nJohn,101",
            "\nJohn,103",
            "\nJohn,105",
            "\nJane,102",
            "\nJane,104",
        ),
    )
}

#[test]
fn join_three() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/users.csv", "tests/data/orders.csv", "tests/data/products.csv"],
        "SELECT users.name, products.name FROM users, orders, products WHERE users.id = orders.user_id AND orders.product_id = products.product_id",
        concat!(
            "users.name,products.name",
            "\nJohn,Laptop",
            "\nJohn,Headphones",
            "\nJohn,Keyboard",
            "\nJane,Phone",
            "\nJane,Monitor",
        ),
    )
}

#[test]
fn union() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT department FROM employees WHERE age < 26 UNION SELECT department FROM employees WHERE age > 44",
        concat!(
            "department",
            "\nHR",
            "\nMarketing",
            "\nSales",
        ),
    )
}

#[test]
fn union_all() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT department FROM employees WHERE age < 26 UNION ALL SELECT department FROM employees WHERE age > 44",
        concat!(
            "department",
            "\nMarketing",
            "\nHR",
            "\nSales",
        ),
    )
}

#[test]
fn intersect() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT department FROM employees WHERE age < 31 INTERSECT SELECT department FROM employees WHERE age > 31",
        concat!(
            "department",
            "\nEngineering",
        ),
    )
}

#[test]
fn except() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT department FROM employees WHERE age < 31 EXCEPT SELECT department FROM employees WHERE age > 31",
        concat!(
            "department",
            "\nHR",
            "\nMarketing",
        ),
    )
}

#[test]
fn subquery_scalar() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees WHERE salary = (SELECT MAX(salary) FROM employees)",
        concat!("name", "\nGrace",),
    )
}

#[test]
fn subquery_in() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/users.csv", "tests/data/orders.csv"],
        "SELECT name FROM users WHERE id IN (SELECT user_id FROM orders)",
        concat!("name", "\nJohn", "\nJane",),
    )
}

#[test]
fn subquery_exists() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/users.csv", "tests/data/orders.csv"],
        "SELECT name FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)",
        concat!(
            "name",
            "\nJohn",
            "\nJane",
        ),
    )
}

#[test]
fn str_upper() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT UPPER(name) FROM employees",
        concat!(
            "UPPER(name)",
            "\nALICE",
            "\nBOB",
            "\nCHARLIE",
            "\nDAVID",
            "\nEVE",
            "\nFRANK",
            "\nGRACE",
            "\nHENRY",
        ),
    )
}

#[test]
fn str_lower() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT LOWER(name) FROM employees",
        concat!(
            "LOWER(name)",
            "\nalice",
            "\nbob",
            "\ncharlie",
            "\ndavid",
            "\neve",
            "\nfrank",
            "\ngrace",
            "\nhenry",
        ),
    )
}

#[test]
fn str_length() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT LENGTH(name) FROM employees",
        concat!(
            "LENGTH(name)",
            "\n5",
            "\n3",
            "\n7",
            "\n5",
            "\n3",
            "\n5",
            "\n5",
            "\n5",
        ),
    )
}

#[test]
fn str_substr() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT SUBSTR(name, 1, 3) FROM employees",
        concat!(
            "SUBSTR(name)",
            "\nAli",
            "\nBob",
            "\nCha",
            "\nDav",
            "\nEve",
            "\nFra",
            "\nGra",
            "\nHen",
        ),
    )
}

#[test]
fn str_replace() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT REPLACE(department, 'Engineering', 'Eng') FROM employees",
        concat!(
            "REPLACE(department)",
            "\nEng",
            "\nMarketing",
            "\nEng",
            "\nSales",
            "\nMarketing",
            "\nEng",
            "\nSales",
            "\nHR",
        ),
    )
}

#[test]
fn str_trim() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/strings.csv"],
        "SELECT TRIM(padded_text) FROM strings",
        concat!(
            "expr",
            "\ntrimme",
            "\nneeds space",
            "\nwhitespace",
            "\nextra",
            "\npadding",
        ),
    )
}

#[test]
fn str_concat() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT CONCAT(name, department) FROM employees",
        concat!(
            "CONCAT(name)",
            "\nAliceEngineering",
            "\nBobMarketing",
            "\nCharlieEngineering",
            "\nDavidSales",
            "\nEveMarketing",
            "\nFrankEngineering",
            "\nGraceSales",
            "\nHenryHR",
        ),
    )
}

#[test]
fn str_left_right() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT LEFT(name, 2), RIGHT(name, 2) FROM employees",
        concat!(
            "LEFT(name),RIGHT(name)",
            "\nAl,ce",
            "\nBo,ob",
            "\nCh,ie",
            "\nDa,id",
            "\nEv,ve",
            "\nFr,nk",
            "\nGr,ce",
            "\nHe,ry",
        ),
    )
}

#[test]
fn math_abs() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/boundaries.csv"],
        "SELECT ABS(value) FROM boundaries WHERE id < 5",
        concat!("ABS(value)", "\n0", "\n1", "\n1", "\n9223372036854775807",),
    )
}

/// ABS(i64::MIN) has no representable result; as in SQLite it is an
/// "integer overflow" error (it used to saturate to i64::MAX).
#[test]
fn math_abs_of_i64_min_is_overflow() -> Result<(), Box<dyn std::error::Error>> {
    crate::helpers::assert_query_fails(
        &["tests/data/boundaries.csv"],
        "SELECT ABS(value) FROM boundaries",
        "integer overflow",
    )
}

#[test]
fn math_round() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT ROUND(salary) FROM employees",
        concat!(
            "ROUND(salary)",
            "\n70000",
            "\n55000",
            "\n65000",
            "\n80000",
            "\n60000",
            "\n75000",
            "\n90000",
            "\n45000",
        ),
    )
}

#[test]
fn arith_add() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name, salary + 1000 FROM employees",
        concat!(
            "name,expr",
            "\nAlice,71000",
            "\nBob,56000",
            "\nCharlie,66000",
            "\nDavid,81000",
            "\nEve,61000",
            "\nFrank,76000",
            "\nGrace,91000",
            "\nHenry,46000",
        ),
    )
}

#[test]
fn arith_mul() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name, salary * 2 FROM employees",
        concat!(
            "name,expr",
            "\nAlice,140000",
            "\nBob,110000",
            "\nCharlie,130000",
            "\nDavid,160000",
            "\nEve,120000",
            "\nFrank,150000",
            "\nGrace,180000",
            "\nHenry,90000",
        ),
    )
}

#[test]
fn arith_div() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name, salary / 1000 FROM employees",
        concat!(
            "name,expr",
            "\nAlice,70",
            "\nBob,55",
            "\nCharlie,65",
            "\nDavid,80",
            "\nEve,60",
            "\nFrank,75",
            "\nGrace,90",
            "\nHenry,45",
        ),
    )
}

#[test]
fn arith_mod() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name, age % 7 FROM employees",
        concat!(
            "name,expr",
            "\nAlice,2",
            "\nBob,4",
            "\nCharlie,0",
            "\nDavid,5",
            "\nEve,0",
            "\nFrank,4",
            "\nGrace,3",
            "\nHenry,1",
        ),
    )
}

#[test]
fn arith_unary_minus() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name, -salary FROM employees",
        concat!(
            "name,expr",
            "\nAlice,-70000",
            "\nBob,-55000",
            "\nCharlie,-65000",
            "\nDavid,-80000",
            "\nEve,-60000",
            "\nFrank,-75000",
            "\nGrace,-90000",
            "\nHenry,-45000",
        ),
    )
}

#[test]
fn coalesce() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/nullable.csv"],
        "SELECT id, COALESCE(score, 0) FROM nullable",
        concat!(
            "id,COALESCE(score)",
            "\n1,90",
            "\n2,0",
            "\n3,70",
            "\n4,50",
            "\n5,90",
        ),
    )
}

#[test]
fn nullif() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name, NULLIF(department, 'HR') FROM employees",
        concat!(
            "name,NULLIF(department)",
            "\nAlice,Engineering",
            "\nBob,Marketing",
            "\nCharlie,Engineering",
            "\nDavid,Sales",
            "\nEve,Marketing",
            "\nFrank,Engineering",
            "\nGrace,Sales",
            "\nHenry,",
        ),
    )
}

#[test]
fn win_row_number() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name, ROW_NUMBER() OVER (ORDER BY salary DESC) FROM employees",
        concat!(
            "name,ROW_NUMBER",
            "\nGrace,1",
            "\nDavid,2",
            "\nFrank,3",
            "\nAlice,4",
            "\nCharlie,5",
            "\nEve,6",
            "\nBob,7",
            "\nHenry,8",
        ),
    )
}

#[test]
fn win_rank() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name, RANK() OVER (ORDER BY department) FROM employees",
        concat!(
            "name,RANK",
            "\nAlice,1",
            "\nCharlie,1",
            "\nFrank,1",
            "\nHenry,4",
            "\nBob,5",
            "\nEve,5",
            "\nDavid,7",
            "\nGrace,7",
        ),
    )
}

#[test]
fn win_dense_rank() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name, DENSE_RANK() OVER (ORDER BY department) FROM employees",
        concat!(
            "name,DENSE_RANK",
            "\nAlice,1",
            "\nCharlie,1",
            "\nFrank,1",
            "\nHenry,2",
            "\nBob,3",
            "\nEve,3",
            "\nDavid,4",
            "\nGrace,4",
        ),
    )
}

#[test]
fn win_lag() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name, LAG(salary) OVER (ORDER BY salary) FROM employees",
        concat!(
            "name,LAG",
            "\nHenry,",
            "\nBob,45000",
            "\nEve,55000",
            "\nCharlie,60000",
            "\nAlice,65000",
            "\nFrank,70000",
            "\nDavid,75000",
            "\nGrace,80000",
        ),
    )
}

#[test]
fn win_lead() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name, LEAD(salary) OVER (ORDER BY salary) FROM employees",
        concat!(
            "name,LEAD",
            "\nHenry,55000",
            "\nBob,60000",
            "\nEve,65000",
            "\nCharlie,70000",
            "\nAlice,75000",
            "\nFrank,80000",
            "\nDavid,90000",
            "\nGrace,",
        ),
    )
}

#[test]
fn case_in_where() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees WHERE CASE WHEN age > 30 THEN 1 ELSE 0 END = 1",
        concat!("name", "\nCharlie", "\nDavid", "\nFrank", "\nGrace",),
    )
}

#[test]
fn cast_in_where() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees WHERE CAST(salary AS INTEGER) > 70000",
        concat!("name", "\nDavid", "\nFrank", "\nGrace",),
    )
}

#[test]
fn alias_column() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name AS who, salary AS pay FROM employees",
        concat!(
            "who,pay",
            "\nAlice,70000",
            "\nBob,55000",
            "\nCharlie,65000",
            "\nDavid,80000",
            "\nEve,60000",
            "\nFrank,75000",
            "\nGrace,90000",
            "\nHenry,45000",
        ),
    )
}

// ---------------------------------------------------------------------------
// Outer joins WITH unmatched rows on both sides.
//
// The users/orders fixtures cannot exercise NULL-fill: every user has an order
// and every order has a user, so LEFT/RIGHT/FULL all degenerate to INNER there.
// customers/purchases has an unmatched row on each side (Cara has no purchase;
// purchase 13 references customer 99), so these pin the actual outer-join
// semantics.
// ---------------------------------------------------------------------------

#[test]
fn outer_join_inner_baseline() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/customers.csv", "tests/data/purchases.csv"],
        "SELECT customers.name, purchases.item FROM customers INNER JOIN purchases ON customers.id = purchases.customer_id",
        concat!(
            "customers.name,purchases.item",
            "\nAnn,Book",
            "\nAnn,Pen",
            "\nBen,Desk",
        ),
    )
}

#[test]
fn outer_join_left_nullfill() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/customers.csv", "tests/data/purchases.csv"],
        "SELECT customers.name, purchases.item FROM customers LEFT JOIN purchases ON customers.id = purchases.customer_id",
        concat!(
            "customers.name,purchases.item",
            "\nAnn,Book",
            "\nAnn,Pen",
            "\nBen,Desk",
            "\nCara,",
        ),
    )
}

#[test]
fn outer_join_right_nullfill() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/customers.csv", "tests/data/purchases.csv"],
        "SELECT customers.name, purchases.item FROM customers RIGHT JOIN purchases ON customers.id = purchases.customer_id",
        concat!(
            "customers.name,purchases.item",
            "\nAnn,Book",
            "\nAnn,Pen",
            "\nBen,Desk",
            "\n,Orphan",
        ),
    )
}

#[test]
fn outer_join_full_nullfill() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/customers.csv", "tests/data/purchases.csv"],
        "SELECT customers.name, purchases.item FROM customers FULL JOIN purchases ON customers.id = purchases.customer_id",
        concat!(
            "customers.name,purchases.item",
            "\nAnn,Book",
            "\nAnn,Pen",
            "\nBen,Desk",
            "\nCara,",
            "\n,Orphan",
        ),
    )
}

// ---------------------------------------------------------------------------
// Error paths.
//
// Only two tests in the pre-existing suite assert a failure at all, so nothing
// pinned which inputs must be REJECTED. These do -- and they matter especially
// after an AST upgrade, where an unhandled node can quietly start being
// accepted (or a supported one start being refused).
// ---------------------------------------------------------------------------

use crate::helpers::assert_after_write;
use crate::helpers::assert_query_fails;

#[test]
fn err_unknown_column() -> Result<(), Box<dyn std::error::Error>> {
    assert_query_fails(
        &["tests/data/employees.csv"],
        "SELECT nosuchcolumn FROM employees",
        "not found",
    )
}

#[test]
fn err_unknown_table() -> Result<(), Box<dyn std::error::Error>> {
    assert_query_fails(
        &["tests/data/employees.csv"],
        "SELECT name FROM nosuchtable",
        "not found",
    )
}

#[test]
fn err_syntax() -> Result<(), Box<dyn std::error::Error>> {
    assert_query_fails(
        &["tests/data/employees.csv"],
        "SELECT FROM WHERE",
        "Failed to execute SQL",
    )
}

#[test]
fn err_group_by_all_rejected() -> Result<(), Box<dyn std::error::Error>> {
    // GROUP BY ALL parses but carries no key list. It must be rejected rather
    // than silently treated as "no GROUP BY".
    assert_query_fails(
        &["tests/data/employees.csv"],
        "SELECT department, COUNT(*) FROM employees GROUP BY ALL",
        "GROUP BY ALL is not supported",
    )
}

#[test]
fn err_order_by_all_rejected() -> Result<(), Box<dyn std::error::Error>> {
    // Likewise ORDER BY ALL must not be silently ignored. Under the dialect
    // sqawk parses with, `ALL` here comes through as an ordinary identifier
    // rather than as OrderByKind::All, so this is rejected as an unknown
    // column instead of by the explicit guard in compile_query. Either route
    // is acceptable; silently returning unsorted rows is not, which is what
    // this test pins.
    assert_query_fails(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees ORDER BY ALL",
        "not found",
    )
}

#[test]
fn err_semi_join_rejected() -> Result<(), Box<dyn std::error::Error>> {
    // Join kinds sqawk cannot execute must produce a clear error, not a
    // silently wrong join. This is the arm that a sqlparser variant rename
    // would otherwise slip past.
    assert_query_fails(
        &["tests/data/users.csv", "tests/data/orders.csv"],
        "SELECT users.name FROM users LEFT SEMI JOIN orders ON users.id = orders.user_id",
        "not supported",
    )
}

// ---------------------------------------------------------------------------
// WHERE combined with LIMIT/OFFSET.
//
// The suite tested WHERE and tested LIMIT, but never the two together -- and
// the interaction was broken: the WHERE-false jump was computed as a fixed
// offset from the test, which landed on the LIMIT counter's DecrJumpZero. Every
// row that FAILED the filter still decremented the limit, so LIMIT counted rows
// scanned rather than rows returned, silently truncating results.
// ---------------------------------------------------------------------------

#[test]
fn where_with_limit_not_reached() -> Result<(), Box<dyn std::error::Error>> {
    // Limit is larger than the match count: every match must be returned.
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees WHERE department = 'Engineering' LIMIT 5",
        "name\nAlice\nCharlie\nFrank",
    )
}

#[test]
fn where_with_limit_late_matches() -> Result<(), Box<dyn std::error::Error>> {
    // Both matches come after three non-matching rows, so a limit that counts
    // scanned rows would drop Grace.
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees WHERE department = 'Sales' LIMIT 5",
        "name\nDavid\nGrace",
    )
}

#[test]
fn where_with_limit_reached() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees WHERE department = 'Engineering' LIMIT 2",
        "name\nAlice\nCharlie",
    )
}

#[test]
fn where_with_limit_offset() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees WHERE department = 'Engineering' LIMIT 5 OFFSET 1",
        "name\nCharlie\nFrank",
    )
}

#[test]
fn where_with_offset_only() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees WHERE age > 25 OFFSET 2",
        "name\nDavid\nEve\nFrank\nGrace",
    )
}

/// DISTINCT rides on the aggregate function type as a flag bit, so it composes
/// with every aggregate rather than only COUNT. repeats.csv holds 10,10,20,30,30.
#[test]
fn aggregate_distinct_variants() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/repeats.csv"],
        "SELECT SUM(v), SUM(DISTINCT v), COUNT(v), COUNT(DISTINCT v), \
         MIN(DISTINCT v), MAX(DISTINCT v), AVG(DISTINCT v) FROM repeats",
        "SUM,SUM,COUNT,COUNT,MIN,MAX,AVG\n100,60,5,3,10,30,20",
    )
}

// ---------------------------------------------------------------------------
// Expression position symmetry.
//
// SQL draws no distinction between an expression used as a filter and the same
// expression used as a projected value, but sqawk had three parallel
// expression compilers and each supported a different subset. These pin both
// positions for the same node kinds so the two cannot drift apart again.
// ---------------------------------------------------------------------------

#[test]
fn case_as_projected_value() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name, CASE WHEN age > 30 THEN 'old' ELSE 'young' END FROM employees LIMIT 3",
        "name,expr\nAlice,young\nBob,young\nCharlie,old",
    )
}

#[test]
fn cast_as_projected_value() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT CAST(salary AS TEXT) FROM employees LIMIT 2",
        "expr\n70000\n55000",
    )
}

#[test]
fn is_null_as_projected_value() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/nullable.csv"],
        "SELECT id, score IS NULL FROM nullable",
        "id,expr\n1,0\n2,1\n3,0\n4,0\n5,0",
    )
}

#[test]
fn like_as_projected_value() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name, name LIKE 'A%' FROM employees LIMIT 2",
        "name,expr\nAlice,1\nBob,0",
    )
}

#[test]
fn between_as_projected_value() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name, age BETWEEN 30 AND 40 FROM employees LIMIT 3",
        "name,expr\nAlice,1\nBob,0\nCharlie,1",
    )
}

#[test]
fn trim_as_filter() -> Result<(), Box<dyn std::error::Error>> {
    // TRIM/CEIL/FLOOR were handled only in the projection path, so they worked
    // in a SELECT list and failed in WHERE -- the mirror image of SUBSTR.
    assert_query(
        &["tests/data/strings.csv"],
        "SELECT id FROM strings WHERE TRIM(padded_text) = 'trimme'",
        "id\n1",
    )
}

#[test]
fn ceil_as_filter() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees WHERE CEIL(salary) > 85000",
        "name\nGrace",
    )
}

#[test]
fn concat_operator_as_filter() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees WHERE name || '!' = 'Alice!'",
        "name\nAlice",
    )
}

// ---------------------------------------------------------------------------
// GROUP BY interactions that were previously unrepresentable.
// ---------------------------------------------------------------------------

#[test]
fn group_by_with_where_and_having() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT department, COUNT(*) FROM employees WHERE age > 25 GROUP BY department \
         HAVING COUNT(*) > 1",
        // age > 25 drops Bob(25) and Henry(22), leaving Marketing with one
        // row, which HAVING then removes.
        "department,COUNT\nEngineering,3\nSales,2",
    )
}

#[test]
fn group_by_three_columns() -> Result<(), Box<dyn std::error::Error>> {
    // Every key column must participate; a difference in the last one still
    // starts a new group.
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT department, role, COUNT(*) FROM employees GROUP BY department, role",
        "department,role,COUNT\n\
         Engineering,Developer,2\n\
         Engineering,Manager,1\n\
         HR,Intern,1\n\
         Marketing,Analyst,1\n\
         Marketing,Specialist,1\n\
         Sales,Director,2",
    )
}

#[test]
fn group_by_with_where_excluding_a_whole_group() -> Result<(), Box<dyn std::error::Error>> {
    // HR's only row is filtered out, so the group must disappear entirely
    // rather than appear with a zero or unfiltered count.
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT department, COUNT(*) FROM employees WHERE salary > 60000 GROUP BY department",
        "department,COUNT\nEngineering,3\nSales,2",
    )
}

#[test]
fn having_on_a_group_key() -> Result<(), Box<dyn std::error::Error>> {
    // The HAVING operand compiler handled only numeric literals and silently
    // emitted nothing for a string, so this matched no group at all.
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT department, COUNT(*) FROM employees GROUP BY department \
         HAVING department = 'Sales'",
        "department,COUNT\nSales,2",
    )
}

#[test]
fn having_selects_between_same_type_aggregates() -> Result<(), Box<dyn std::error::Error>> {
    // Two SUMs: HAVING must bind to the one it names, not the first in the
    // projection. SUM(age) values are 97/22/53/85 by department.
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT department, SUM(salary), SUM(age) FROM employees GROUP BY department \
         HAVING SUM(age) > 90",
        "department,SUM,SUM\nEngineering,210000,97",
    )
}

// ---------------------------------------------------------------------------
// ORDER BY / LIMIT over paths that emit rows directly rather than through the
// cursor-based sorter: GROUP BY, plain aggregates, and joins.
// ---------------------------------------------------------------------------

#[test]
fn group_by_order_by_aggregate() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT department, COUNT(*) FROM employees GROUP BY department \
         ORDER BY COUNT(*) DESC, department ASC",
        "department,COUNT\nEngineering,3\nMarketing,2\nSales,2\nHR,1",
    )
}

#[test]
fn group_by_order_by_limit_together() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT department, SUM(salary) FROM employees GROUP BY department \
         ORDER BY SUM(salary) DESC LIMIT 2",
        "department,SUM\nEngineering,210000\nSales,170000",
    )
}

#[test]
fn group_by_order_by_offset() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT department, COUNT(*) FROM employees GROUP BY department \
         ORDER BY department ASC LIMIT 2 OFFSET 1",
        "department,COUNT\nHR,1\nMarketing,2",
    )
}

#[test]
fn join_order_by_descending() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/users.csv", "tests/data/orders.csv"],
        "SELECT users.name, orders.id FROM users JOIN orders ON users.id = orders.user_id \
         ORDER BY orders.id DESC",
        "users.name,orders.id\nJohn,105\nJane,104\nJohn,103\nJane,102\nJohn,101",
    )
}

#[test]
fn join_order_by_with_limit() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/users.csv", "tests/data/orders.csv"],
        "SELECT users.name, orders.id FROM users JOIN orders ON users.id = orders.user_id \
         ORDER BY orders.id ASC LIMIT 2",
        "users.name,orders.id\nJohn,101\nJane,102",
    )
}

#[test]
fn order_by_key_absent_from_select_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
    // After aggregation a column that is neither a GROUP BY key nor inside
    // an aggregate has no single value per row, so ordering by it must be an
    // error, not a silently ignored clause.
    assert_query_fails(
        &["tests/data/employees.csv"],
        "SELECT department, COUNT(*) FROM employees GROUP BY department ORDER BY salary",
        "must appear in GROUP BY",
    )
}

// ---------------------------------------------------------------------------
// NULL semantics.
//
// The most consequential area for a CSV tool: empty fields are ubiquitous, and
// a comparison against one used to raise "Cannot compare incompatible types:
// Null and Integer(60)" and abort the query.
// ---------------------------------------------------------------------------

#[test]
fn null_comparison_filters_rather_than_erroring() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/nullable.csv"],
        "SELECT name FROM nullable WHERE score > 60",
        "name\nAlice\nCharlie\nEve",
    )
}

#[test]
fn null_is_not_equal_to_null() -> Result<(), Box<dyn std::error::Error>> {
    // SQL equality over NULL is UNKNOWN, so no row qualifies -- even the rows
    // where both sides are NULL.
    assert_query(
        &["tests/data/nullable.csv"],
        "SELECT id FROM nullable WHERE score = grade",
        "id",
    )
}

#[test]
fn null_excluded_by_negated_comparison_too() -> Result<(), Box<dyn std::error::Error>> {
    // A NULL row satisfies neither `> 60` nor `<= 60`: UNKNOWN is not false.
    assert_query(
        &["tests/data/nullable.csv"],
        "SELECT name FROM nullable WHERE score <= 60",
        "name\n\"\"",
    )
}

#[test]
fn aggregates_skip_nulls() -> Result<(), Box<dyn std::error::Error>> {
    // COUNT(*) counts rows; COUNT(col) counts non-NULL values.
    assert_query(
        &["tests/data/nullable.csv"],
        "SELECT COUNT(*), COUNT(score), SUM(score), AVG(score) FROM nullable",
        "COUNT,COUNT,SUM,AVG\n5,4,300,75",
    )
}

#[test]
fn nulls_sort_first() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/nullable.csv"],
        "SELECT id, score FROM nullable ORDER BY score",
        "id,score\n2,\n4,50\n3,70\n1,90\n5,90",
    )
}

#[test]
fn string_number_coercion_in_comparison() -> Result<(), Box<dyn std::error::Error>> {
    // awk-shaped: a string that parses as a number compares numerically, so
    // this agrees with arithmetic rather than silently matching nothing.
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees WHERE salary > '60000'",
        "name\nAlice\nCharlie\nDavid\nFrank\nGrace",
    )
}

#[test]
fn group_by_aggregate_before_key() -> Result<(), Box<dyn std::error::Error>> {
    // Values follow the SELECT list, not the internal [keys.., aggs..] layout.
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT COUNT(*), SUM(salary), department FROM employees GROUP BY department",
        "COUNT,SUM,department\n\
         3,210000,Engineering\n\
         1,45000,HR\n\
         2,115000,Marketing\n\
         2,170000,Sales",
    )
}

#[test]
fn group_by_key_repeated_in_projection() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT department, COUNT(*), department FROM employees GROUP BY department LIMIT 2",
        "department,COUNT,department\nEngineering,3,Engineering\nHR,1,HR",
    )
}

#[test]
fn group_by_non_key_column_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
    // A column that is neither a group key nor inside an aggregate has no
    // single value per group; it must be an error rather than an arbitrary pick.
    assert_query_fails(
        &["tests/data/employees.csv"],
        "SELECT department, name FROM employees GROUP BY department",
        "must appear in GROUP BY",
    )
}

// ---------------------------------------------------------------------------
// Join projection order.
//
// Join registers are laid out left-table columns then right-table columns. The
// SELECT list may name them in any order, so these pin the reversed and
// interleaved cases -- including on outer joins, where the NULL-filled row
// must follow the same order.
// ---------------------------------------------------------------------------

#[test]
fn join_projection_reversed() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/users.csv", "tests/data/orders.csv"],
        "SELECT orders.id, users.name FROM users JOIN orders ON users.id = orders.user_id",
        "orders.id,users.name\n101,John\n103,John\n105,John\n102,Jane\n104,Jane",
    )
}

#[test]
fn left_join_reversed_projection_nullfill() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/customers.csv", "tests/data/purchases.csv"],
        "SELECT purchases.item, customers.name FROM customers LEFT JOIN purchases \
         ON customers.id = purchases.customer_id",
        "purchases.item,customers.name\nBook,Ann\nPen,Ann\nDesk,Ben\n,Cara",
    )
}

#[test]
fn right_join_reversed_projection_nullfill() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/customers.csv", "tests/data/purchases.csv"],
        "SELECT purchases.item, customers.name FROM customers RIGHT JOIN purchases \
         ON customers.id = purchases.customer_id",
        "purchases.item,customers.name\nBook,Ann\nPen,Ann\nDesk,Ben\nOrphan,",
    )
}

#[test]
fn full_join_reversed_projection_nullfill() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/customers.csv", "tests/data/purchases.csv"],
        "SELECT purchases.item, customers.name FROM customers FULL JOIN purchases \
         ON customers.id = purchases.customer_id",
        "purchases.item,customers.name\nBook,Ann\nPen,Ann\nDesk,Ben\n,Cara\nOrphan,",
    )
}

// ---------------------------------------------------------------------------
// ORDER BY keys independent of the projection.
//
// The sorter payload is [sort keys.., output columns..], so a key is just a
// value computed per row whether or not it is projected. It previously held
// only the output columns, which forced every key to be a selected column.
// ---------------------------------------------------------------------------

#[test]
fn order_by_unselected_column() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees ORDER BY salary DESC",
        "name\nGrace\nDavid\nFrank\nAlice\nCharlie\nEve\nBob\nHenry",
    )
}

#[test]
fn order_by_arithmetic_expression() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees ORDER BY salary * -1 LIMIT 3",
        "name\nGrace\nDavid\nFrank",
    )
}

#[test]
fn order_by_function_then_column() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees ORDER BY LENGTH(name) DESC, name ASC LIMIT 3",
        "name\nCharlie\nAlice\nDavid",
    )
}

#[test]
fn order_by_unselected_column_with_where() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees WHERE department = 'Engineering' ORDER BY age DESC",
        "name\nCharlie\nFrank\nAlice",
    )
}

// ---------------------------------------------------------------------------
// Aggregates composed with expressions.
//
// An aggregate argument is an ordinary expression, and after aggregation an
// aggregate result is an ordinary value. Both directions previously failed:
// the argument went through a resolver that yields a column INDEX, and
// detection looked only at the top level so an aggregate inside arithmetic was
// not recognised as an aggregate query at all.
// ---------------------------------------------------------------------------

#[test]
fn aggregate_over_arithmetic() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT SUM(salary + age) FROM employees",
        "SUM\n540257",
    )
}

#[test]
fn aggregate_over_function() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT MAX(LENGTH(name)), MIN(LENGTH(name)) FROM employees",
        "MAX,MIN\n7,3",
    )
}

#[test]
fn arithmetic_over_aggregate() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT SUM(salary) + 1 FROM employees",
        "expr\n540001",
    )
}

#[test]
fn arithmetic_between_two_aggregates() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT COUNT(*) * 2, MAX(salary) - MIN(salary) FROM employees",
        "expr,expr\n16,45000",
    )
}

#[test]
fn aggregate_mixed_with_plain_projection() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT COUNT(*), SUM(salary) / COUNT(*) FROM employees",
        "COUNT,expr\n8,67500",
    )
}

// ---------------------------------------------------------------------------
// Multi-statement scripts.
//
// Each statement is compiled and executed on its own, so it gets its own
// result set AND observes the database as the previous statement left it.
// ---------------------------------------------------------------------------

#[test]
fn two_selects_keep_separate_headers() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/people.csv"],
        "SELECT name FROM people WHERE age > 30; SELECT COUNT(*) FROM people",
        "name\nAlice\nCharlie\nCOUNT\n3",
    )
}

#[test]
fn select_sees_preceding_delete() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/people.csv"],
        "DELETE FROM people WHERE age < 30; SELECT name FROM people",
        "name\nAlice\nCharlie",
    )
}

#[test]
fn insert_then_select_sees_the_row() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/people.csv"],
        "INSERT INTO people VALUES (4, 'Dana', 41); SELECT name FROM people WHERE age > 40",
        "name\nDana",
    )
}

#[test]
fn create_table_then_insert_then_select() -> Result<(), Box<dyn std::error::Error>> {
    // The third statement can only compile if the first has already taken
    // effect, which is what per-statement execution provides.
    assert_query(
        &["tests/data/people.csv"],
        "CREATE TABLE t (a INTEGER, b TEXT); INSERT INTO t VALUES (1, 'x'); SELECT * FROM t",
        "a,b\n1,x",
    )
}

#[test]
fn update_writeback_preserves_row_order() -> Result<(), Box<dyn std::error::Error>> {
    // UPDATE replaces rows in place. Expressed as delete-plus-insert it
    // appended, so a partial update rewrote the user's file in a different
    // order. Updating the FIRST row is the case that exposes it.
    assert_after_write(
        "tests/data/employees.csv",
        "employees",
        "UPDATE employees SET salary = 1 WHERE id = 1",
        "id,name,age,salary,department,role\n\
         1,Alice,30,1,Engineering,Developer\n\
         2,Bob,25,55000,Marketing,Specialist\n\
         3,Charlie,35,65000,Engineering,Manager\n\
         4,David,40,80000,Sales,Director\n\
         5,Eve,28,60000,Marketing,Analyst\n\
         6,Frank,32,75000,Engineering,Developer\n\
         7,Grace,45,90000,Sales,Director\n\
         8,Henry,22,45000,HR,Intern",
    )
}

// ---------------------------------------------------------------------------
// Window frames.
//
// An aggregate window with PARTITION BY and no ORDER BY has the whole
// partition as its frame, so every row sees the partition total. With an
// ORDER BY the frame grows row by row and a running total is correct. The
// streaming implementation produced a running total in both cases.
// ---------------------------------------------------------------------------

#[test]
fn unordered_window_gives_partition_totals() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name, SUM(salary) OVER (PARTITION BY department) FROM employees",
        "name,SUM\n\
         Alice,210000\nCharlie,210000\nFrank,210000\n\
         Henry,45000\n\
         Bob,115000\nEve,115000\n\
         David,170000\nGrace,170000",
    )
}

#[test]
fn ordered_window_keeps_running_totals() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name, SUM(salary) OVER (PARTITION BY department ORDER BY salary) FROM employees",
        "name,SUM\n\
         Charlie,65000\nAlice,135000\nFrank,210000\n\
         Henry,45000\n\
         Bob,55000\nEve,115000\n\
         David,80000\nGrace,170000",
    )
}

#[test]
fn multiple_unordered_window_aggregates() -> Result<(), Box<dyn std::error::Error>> {
    // Every aggregate window column is finalized, not just the first.
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name, COUNT(salary) OVER (PARTITION BY department), \
         MIN(salary) OVER (PARTITION BY department) FROM employees",
        "name,COUNT,MIN\n\
         Alice,3,65000\nCharlie,3,65000\nFrank,3,65000\n\
         Henry,1,45000\n\
         Bob,2,55000\nEve,2,55000\n\
         David,2,80000\nGrace,2,80000",
    )
}

// ---------------------------------------------------------------------------
// Aggregates over joins.
//
// `A INNER JOIN B ON c` and `FROM A, B WHERE c` mean the same thing, and the
// comma form already supported aggregates, so the explicit form is rewritten
// to it. The rewrite is deliberately NOT applied to outer joins: moving the ON
// condition into WHERE would discard exactly the NULL-extended rows an outer
// join exists to produce.
// ---------------------------------------------------------------------------

#[test]
fn count_over_inner_join() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/users.csv", "tests/data/orders.csv"],
        "SELECT COUNT(*) FROM users JOIN orders ON users.id = orders.user_id",
        "COUNT\n5",
    )
}

#[test]
fn group_by_over_inner_join() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/users.csv", "tests/data/orders.csv"],
        "SELECT users.name, COUNT(*) FROM users JOIN orders ON users.id = orders.user_id \
         GROUP BY users.name",
        "users.name,COUNT\nJane,2\nJohn,3",
    )
}

#[test]
fn inner_join_and_comma_join_agree() -> Result<(), Box<dyn std::error::Error>> {
    // The rewrite's premise, pinned directly.
    assert_query(
        &["tests/data/users.csv", "tests/data/orders.csv"],
        "SELECT COUNT(*) FROM users, orders WHERE users.id = orders.user_id",
        "COUNT\n5",
    )
}

#[test]
fn outer_join_aggregate_is_not_silently_rewritten() -> Result<(), Box<dyn std::error::Error>> {
    // Must not answer as though it were an inner join: Cara has no purchase,
    // so a LEFT JOIN has 4 rows where the inner form has 3. (This used to be
    // rejected outright; aggregates now consume the outer join's rows,
    // NULL-extended ones included. Expected value from SQLite.)
    assert_query(
        &["tests/data/customers.csv", "tests/data/purchases.csv"],
        "SELECT COUNT(*) FROM customers LEFT JOIN purchases ON customers.id = purchases.customer_id",
        "COUNT\n4",
    )
}

// ---------------------------------------------------------------------------
// Derived tables.
//
// The compiler only knows how to open a cursor on a named table, so a subquery
// in FROM is executed first and registered under its alias; the outer query
// then sees an ordinary table. The registration lasts only for the statement.
// ---------------------------------------------------------------------------

#[test]
fn derived_table_filters() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM (SELECT name, salary FROM employees WHERE salary > 70000) t",
        "name\nDavid\nFrank\nGrace",
    )
}

#[test]
fn aggregate_over_derived_table() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT COUNT(*) FROM (SELECT name FROM employees WHERE department = 'Engineering') t",
        "COUNT\n3",
    )
}

#[test]
fn derived_table_over_group_by() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT * FROM (SELECT department, COUNT(*) FROM employees GROUP BY department) t",
        "department,COUNT\nEngineering,3\nHR,1\nMarketing,2\nSales,2",
    )
}

#[test]
fn derived_table_exposes_inner_aliases() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT n FROM (SELECT name AS n FROM employees LIMIT 2) t",
        "n\nAlice\nBob",
    )
}

#[test]
fn nested_derived_tables() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT x FROM (SELECT name AS x FROM (SELECT name FROM employees LIMIT 3) inner1) t",
        "x\nAlice\nBob\nCharlie",
    )
}

#[test]
fn derived_table_alias_may_not_shadow_a_real_table() -> Result<(), Box<dyn std::error::Error>> {
    assert_query_fails(
        &["tests/data/employees.csv"],
        "SELECT name FROM (SELECT name FROM employees) employees",
        "shadows an existing table",
    )
}

#[test]
fn derived_table_does_not_outlive_its_statement() -> Result<(), Box<dyn std::error::Error>> {
    // The alias is registered only for the statement that declares it, so a
    // later statement must not see it.
    assert_query_fails(
        &["tests/data/employees.csv"],
        "SELECT name FROM (SELECT name FROM employees) t; SELECT name FROM t",
        "not found",
    )
}

// ---------------------------------------------------------------------------
// Compound JOIN conditions.
//
// The ON clause used to have its own small expression compiler that understood
// a single top-level comparison, so `ON a = b AND c` was rejected even though
// the same expression works in a WHERE. Join conditions now go through the
// shared expression compiler with a multi-source name context.
// ---------------------------------------------------------------------------

#[test]
fn join_on_with_and() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/users.csv", "tests/data/orders.csv"],
        "SELECT users.name, orders.id FROM users JOIN orders \
         ON users.id = orders.user_id AND orders.id > 102",
        "users.name,orders.id\nJohn,103\nJohn,105\nJane,104",
    )
}

#[test]
fn join_on_with_or() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/users.csv", "tests/data/orders.csv"],
        "SELECT users.name, orders.id FROM users JOIN orders \
         ON users.id = orders.user_id OR orders.id = 999",
        "users.name,orders.id\nJohn,101\nJohn,103\nJohn,105\nJane,102\nJane,104",
    )
}

#[test]
fn left_join_with_compound_on_keeps_nullfill() -> Result<(), Box<dyn std::error::Error>> {
    // The extra ON predicate narrows the match without turning the outer join
    // into an inner one: Cara still appears with NULLs.
    assert_query(
        &["tests/data/customers.csv", "tests/data/purchases.csv"],
        "SELECT customers.name, purchases.item FROM customers LEFT JOIN purchases \
         ON customers.id = purchases.customer_id AND purchases.id > 10",
        "customers.name,purchases.item\nAnn,Pen\nBen,Desk\nCara,",
    )
}

// ---------------------------------------------------------------------------
// Multi-table WHERE expressiveness.
//
// The comma-join WHERE had its own condition compiler that understood only
// AND, OR and the six comparisons over qualified columns and literals. It now
// shares the expression compiler, with one name-resolution source per table.
// ---------------------------------------------------------------------------

#[test]
fn function_in_multi_table_where() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/users.csv", "tests/data/orders.csv"],
        "SELECT users.name, orders.id FROM users, orders \
         WHERE users.id = orders.user_id AND UPPER(users.name) = 'JOHN'",
        "users.name,orders.id\nJohn,101\nJohn,103\nJohn,105",
    )
}

#[test]
fn arithmetic_in_multi_table_where() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/users.csv", "tests/data/orders.csv"],
        "SELECT users.name, orders.id FROM users, orders \
         WHERE users.id = orders.user_id AND orders.id - 100 > 3",
        "users.name,orders.id\nJohn,105\nJane,104",
    )
}

#[test]
fn not_in_multi_table_where() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/users.csv", "tests/data/orders.csv"],
        "SELECT users.name, orders.id FROM users, orders \
         WHERE users.id = orders.user_id AND NOT (orders.id = 101)",
        "users.name,orders.id\nJohn,103\nJohn,105\nJane,102\nJane,104",
    )
}

#[test]
fn unqualified_column_in_multi_table_where() -> Result<(), Box<dyn std::error::Error>> {
    // `date` exists only in orders, so it resolves without a qualifier.
    assert_query(
        &["tests/data/users.csv", "tests/data/orders.csv"],
        "SELECT users.name, orders.id FROM users, orders \
         WHERE users.id = orders.user_id AND date = '2023-01-15'",
        "users.name,orders.id\nJohn,101",
    )
}

#[test]
fn function_in_join_on_clause() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/users.csv", "tests/data/orders.csv"],
        "SELECT users.name, orders.id FROM users JOIN orders \
         ON users.id = orders.user_id AND UPPER(users.name) = 'JANE'",
        "users.name,orders.id\nJane,102\nJane,104",
    )
}

#[test]
fn three_table_join_with_compound_on() -> Result<(), Box<dyn std::error::Error>> {
    // Multi-table joins load every column into registers before evaluating
    // join conditions, so a column reference there is a register offset rather
    // than a cursor read. Both addressing modes now go through one expression
    // compiler, which is what lets this ON clause carry more than a single
    // comparison.
    assert_query(
        &[
            "tests/data/users.csv",
            "tests/data/orders.csv",
            "tests/data/products.csv",
        ],
        "SELECT users.name, products.name FROM users \
         JOIN orders ON users.id = orders.user_id \
         JOIN products ON orders.product_id = products.product_id AND products.price > 200",
        "users.name,products.name\nJohn,Laptop\nJane,Phone\nJane,Monitor",
    )
}

#[test]
fn three_table_join_unfiltered() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &[
            "tests/data/users.csv",
            "tests/data/orders.csv",
            "tests/data/products.csv",
        ],
        "SELECT users.name, products.name, products.price FROM users \
         JOIN orders ON users.id = orders.user_id \
         JOIN products ON orders.product_id = products.product_id",
        "users.name,products.name,products.price\n\
         John,Laptop,1200\nJohn,Headphones,150\nJohn,Keyboard,80\n\
         Jane,Phone,800\nJane,Monitor,350",
    )
}

// ---------------------------------------------------------------------------
// Correlated subquery scoping.
//
// A name binds in the subquery's scope first and only then in the enclosing
// query's. Function arguments must follow the same rule: resolving them in a
// rebuilt inner-only scope makes an outer reference silently bind to the inner
// table, which turns a comparison between the two into a comparison of a value
// with itself -- true for every row.
// ---------------------------------------------------------------------------

#[test]
fn correlated_subquery_scalar() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees e \
         WHERE salary > (SELECT AVG(salary) FROM employees WHERE department = e.department)",
        "name\nEve\nFrank\nGrace",
    )
}

#[test]
fn correlated_subquery_exists() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees e WHERE EXISTS \
         (SELECT 1 FROM employees x WHERE x.department = e.department AND x.age > 40)",
        "name\nDavid\nGrace",
    )
}

#[test]
fn correlated_subquery_with_function_on_outer_column() -> Result<(), Box<dyn std::error::Error>> {
    // Must agree with the unwrapped form above. Wrapping both sides in a
    // function may not change which rows qualify.
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees e WHERE EXISTS \
         (SELECT 1 FROM employees x \
          WHERE UPPER(x.department) = UPPER(e.department) AND x.age > 40)",
        "name\nDavid\nGrace",
    )
}

// ---------------------------------------------------------------------------
// Table aliases in joins.
//
// Aliases worked for a single table and for comma joins, but explicit joins
// resolved column qualifiers against the table name alone, so every aliased
// explicit join failed with "Table 'u' not found" -- including all three join
// examples in the user guide. No test covered an aliased join at all, which is
// also how a later refactor came to pass aliases where table names were
// expected without anything failing.
// ---------------------------------------------------------------------------

#[test]
fn alias_in_inner_join() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/users.csv", "tests/data/orders.csv"],
        "SELECT u.name, o.id FROM users u JOIN orders o ON u.id = o.user_id",
        "u.name,o.id\nJohn,101\nJohn,103\nJohn,105\nJane,102\nJane,104",
    )
}

#[test]
fn alias_in_left_join() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/customers.csv", "tests/data/purchases.csv"],
        "SELECT c.name, p.item FROM customers c LEFT JOIN purchases p ON c.id = p.customer_id",
        "c.name,p.item\nAnn,Book\nAnn,Pen\nBen,Desk\nCara,",
    )
}

#[test]
fn alias_in_three_table_join() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &[
            "tests/data/users.csv",
            "tests/data/orders.csv",
            "tests/data/products.csv",
        ],
        "SELECT u.name, p.name FROM users u \
         JOIN orders o ON u.id = o.user_id \
         JOIN products p ON o.product_id = p.product_id",
        "u.name,p.name\nJohn,Laptop\nJohn,Headphones\nJohn,Keyboard\nJane,Phone\nJane,Monitor",
    )
}

#[test]
fn alias_in_implicit_join() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/users.csv", "tests/data/orders.csv"],
        "SELECT u.name, o.id FROM users u, orders o WHERE u.id = o.user_id",
        "u.name,o.id\nJohn,101\nJohn,103\nJohn,105\nJane,102\nJane,104",
    )
}

#[test]
fn explicit_rows_window_frame() -> Result<(), Box<dyn std::error::Error>> {
    // The frame clause used to be parsed and then ignored (a running total
    // over the whole partition), and then rejected. It is a two-row sliding
    // window.
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name, SUM(salary) OVER (ORDER BY salary ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) \
         AS s FROM employees",
        "name,s\nHenry,45000\nBob,100000\nEve,115000\nCharlie,125000\nAlice,135000\n\
         Frank,145000\nDavid,155000\nGrace,170000",
    )
}

// ---------------------------------------------------------------------------
// ORDER BY / LIMIT over multi-table queries.
//
// Both were silently dropped on grouped multi-table queries, and ORDER BY on
// ungrouped comma joins. The scan there applies LIMIT as it goes, which is
// only correct without ORDER BY -- otherwise it keeps the first N rows and
// sorts those, rather than the top N.
// ---------------------------------------------------------------------------

#[test]
fn grouped_join_order_by_and_limit() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/users.csv", "tests/data/orders.csv"],
        "SELECT u.name, COUNT(*) AS n FROM users u JOIN orders o ON u.id = o.user_id \
         GROUP BY u.name ORDER BY n DESC LIMIT 1",
        "u.name,n\nJohn,3",
    )
}

#[test]
fn implicit_join_order_by_takes_top_rows() -> Result<(), Box<dyn std::error::Error>> {
    // The two highest order ids are 105 and 104. A LIMIT applied before the
    // sort would return 101 and 102 instead.
    assert_query(
        &["tests/data/users.csv", "tests/data/orders.csv"],
        "SELECT u.name, o.id FROM users u, orders o WHERE u.id = o.user_id \
         ORDER BY o.id DESC LIMIT 2",
        "u.name,o.id\nJohn,105\nJane,104",
    )
}

#[test]
fn implicit_join_order_by_without_limit() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/users.csv", "tests/data/orders.csv"],
        "SELECT u.name, o.id FROM users u, orders o WHERE u.id = o.user_id ORDER BY o.id DESC",
        "u.name,o.id\nJohn,105\nJane,104\nJohn,103\nJane,102\nJohn,101",
    )
}

// ---------------------------------------------------------------------------
// Defects found by the high-effort review of this branch.
//
// Each is a place where a path that had not yet been unified drifted from the
// one that had: DISTINCT set in one aggregate site and not another, column
// resolution case-insensitive in NameCtx and exact elsewhere, SUBSTR arity
// checked on the Function spelling but not the Expr::Substring one.
// ---------------------------------------------------------------------------

#[test]
fn distinct_aggregate_under_group_by() -> Result<(), Box<dyn std::error::Error>> {
    // Two sites derived the aggregate function type independently and only one
    // OR-ed in the DISTINCT flag, so DISTINCT was dropped whenever GROUP BY
    // was present -- while the same aggregate without GROUP BY was correct.
    assert_query(
        &["tests/data/repeats_grouped.csv"],
        "SELECT grp, COUNT(DISTINCT val), SUM(DISTINCT val) FROM repeats_grouped GROUP BY grp",
        "grp,COUNT,SUM\na,2,3\nb,2,4",
    )
}

#[test]
fn distinct_aggregate_in_having() -> Result<(), Box<dyn std::error::Error>> {
    // HAVING did set the flag, so it could not match the aggregate the
    // projection had registered without it.
    assert_query(
        &["tests/data/repeats_grouped.csv"],
        "SELECT grp, COUNT(DISTINCT val) FROM repeats_grouped GROUP BY grp \
         HAVING COUNT(DISTINCT val) > 1",
        "grp,COUNT\na,2\nb,2",
    )
}

#[test]
fn order_by_over_select_star_join() -> Result<(), Box<dyn std::error::Error>> {
    // The ORDER BY key was matched against SELECT-list text, which cannot see
    // through a wildcard, so this was rejected outright.
    assert_query(
        &["tests/data/customers.csv", "tests/data/purchases.csv"],
        "SELECT * FROM customers, purchases WHERE customers.id = purchases.customer_id \
         ORDER BY purchases.id DESC",
        "customers.id,customers.name,purchases.id,purchases.customer_id,purchases.item\n\
         2,Ben,12,2,Desk\n\
         1,Ann,11,1,Pen\n\
         1,Ann,10,1,Book",
    )
}

#[test]
fn order_by_bare_key_against_qualified_projection() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/users.csv", "tests/data/orders.csv"],
        "SELECT users.name, orders.id FROM users JOIN orders ON users.id = orders.user_id \
         ORDER BY id DESC LIMIT 2",
        "users.name,orders.id\nJohn,105\nJane,104",
    )
}

#[test]
fn positional_order_by_sorts() -> Result<(), Box<dyn std::error::Error>> {
    // `ORDER BY 2` compiled to the constant 2, giving every row the same sort
    // key and silently turning the sort into a no-op.
    assert_query(
        &["tests/data/customers.csv"],
        "SELECT name, id FROM customers ORDER BY 2 DESC",
        "name,id\nCara,3\nBen,2\nAnn,1",
    )
}

#[test]
fn positional_order_by_out_of_range_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
    assert_query_fails(
        &["tests/data/customers.csv"],
        "SELECT name FROM customers ORDER BY 5",
        "out of range",
    )
}

#[test]
fn window_aggregate_over_explicit_columns() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/windowed.csv"],
        "SELECT a, d, SUM(x) OVER (PARTITION BY d) FROM windowed",
        "a,d,SUM\n1,E,30\n2,E,30\n3,S,30",
    )
}

#[test]
fn window_with_wildcard() -> Result<(), Box<dyn std::error::Error>> {
    // The old window path counted one output column per SELECT item, so a
    // wildcard emitted a truncated row; the window stage saves whole rows.
    assert_query(
        &["tests/data/windowed.csv"],
        "SELECT *, SUM(x) OVER (PARTITION BY d) AS s FROM windowed",
        "a,b,d,x,s\n1,p,E,10,30\n2,q,E,20,30\n3,r,S,30,30",
    )
}

#[test]
fn substr_without_start_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
    // sqlparser parses SUBSTR(x) into Expr::Substring with no start, and
    // defaulting it to 1 returned the whole string unchanged.
    assert_query_fails(
        &["tests/data/employees.csv"],
        "SELECT SUBSTR(name) FROM employees",
        "SUBSTR requires at least two arguments",
    )
}

#[test]
fn unqualified_ambiguous_column_in_multi_table_is_rejected(
) -> Result<(), Box<dyn std::error::Error>> {
    // `id` exists in both tables. The multi-table resolver returned the first
    // match where NameCtx reports ambiguity, so the same name resolved
    // differently depending on which resolver the path reached.
    assert_query_fails(
        &["tests/data/users.csv", "tests/data/orders.csv"],
        "SELECT id, COUNT(*) FROM users, orders WHERE users.id = orders.user_id GROUP BY id",
        "ambiguous",
    )
}

#[test]
fn unqualified_column_in_multi_table_is_case_insensitive() -> Result<(), Box<dyn std::error::Error>>
{
    // The same resolver matched exactly while NameCtx matched
    // case-insensitively, so `NAME` resolved in one path and not the other.
    assert_query(
        &["tests/data/users.csv", "tests/data/orders.csv"],
        "SELECT NAME, COUNT(*) FROM users, orders WHERE users.id = orders.user_id GROUP BY NAME",
        "NAME,COUNT\nJane,2\nJohn,3",
    )
}

// ---------------------------------------------------------------------------
// Output stage and compound SELECTs. Expected outputs computed with SQLite.
// ---------------------------------------------------------------------------

const EMP_DEPT: &[&str] = &["tests/data/employees.csv", "tests/data/departments.csv"];

#[test]
fn compound_order_by_desc_limit_offset() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        EMP_DEPT,
        "SELECT name FROM employees WHERE id <= 4 UNION ALL SELECT name FROM departments ORDER BY 1 DESC LIMIT 3 OFFSET 2",
        "name\nEve\nDavid\nDavid",
    )
}

#[test]
fn compound_three_arm_union_except_chain() -> Result<(), Box<dyn std::error::Error>> {
    // Left-associative: (employees UNION departments) EXCEPT Sales. Without
    // ORDER BY the set result comes out sorted, as in SQLite.
    assert_query(
        EMP_DEPT,
        "SELECT department FROM employees UNION SELECT name FROM departments EXCEPT SELECT department FROM employees WHERE department = 'Sales'",
        "department\nAlice\nBob\nCharlie\nDavid\nEngineering\nEve\nFrank\nGrace\nHR\nMarketing",
    )
}

#[test]
fn compound_three_arm_intersect_chain() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        EMP_DEPT,
        "SELECT department FROM employees INTERSECT SELECT department FROM departments INTERSECT SELECT department FROM employees WHERE age > 30",
        "department\nEngineering\nSales",
    )
}

#[test]
fn compound_order_by_alias_and_limit_expression() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        EMP_DEPT,
        "SELECT 1 AS x UNION SELECT 3 UNION SELECT 2 ORDER BY x DESC LIMIT 1 + 1",
        "x\n3\n2",
    )
}

#[test]
fn compound_column_count_mismatch_is_error() -> Result<(), Box<dyn std::error::Error>> {
    crate::helpers::assert_query_fails(
        EMP_DEPT,
        "SELECT id, name FROM employees UNION SELECT id FROM departments",
        "do not have the same number of result columns",
    )
}

#[test]
fn distinct_order_by_alias() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT DISTINCT department AS d FROM employees ORDER BY d DESC",
        "d\nSales\nMarketing\nHR\nEngineering",
    )
}

#[test]
fn order_by_hidden_key_then_alias() -> Result<(), Box<dyn std::error::Error>> {
    // `age` is not projected: a hidden sort key.
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name AS n FROM employees ORDER BY age DESC, n LIMIT 4",
        "n\nGrace\nDavid\nCharlie\nFrank",
    )
}

#[test]
fn distinct_then_limit_offset() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT DISTINCT department FROM employees ORDER BY department LIMIT 2 OFFSET 1",
        "department\nHR\nMarketing",
    )
}

#[test]
fn order_by_aggregate_not_in_select() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT department FROM employees GROUP BY department ORDER BY COUNT(*) DESC, department",
        "department\nEngineering\nMarketing\nSales\nHR",
    )
}

#[test]
fn having_and_order_by_expression_over_alias() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT department, SUM(salary) AS s FROM employees GROUP BY department HAVING s > 100000 ORDER BY s / 2 DESC",
        "department,s\nEngineering,210000\nSales,170000\nMarketing,115000",
    )
}

#[test]
fn fromless_where_false_is_no_rows() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT 1 AS x WHERE 1 = 0",
        "x",
    )
}

#[test]
fn fromless_where_order_by_limit() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT 2 AS a, 'z' AS b WHERE 1 < 2 ORDER BY 1 LIMIT 5",
        "a,b\n2,z",
    )
}

#[test]
fn offset_beyond_result_is_empty() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees ORDER BY id LIMIT 5 OFFSET 100",
        "name",
    )
}

#[test]
fn negative_limit_means_no_limit() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees LIMIT -1 OFFSET 7",
        "name\nHenry",
    )
}

#[test]
fn non_integer_limit_is_error() -> Result<(), Box<dyn std::error::Error>> {
    crate::helpers::assert_query_fails(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees LIMIT 'x'",
        "datatype mismatch",
    )
}

#[test]
fn empty_input_grouped_aggregate_has_no_rows() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT department, COUNT(*) AS c, SUM(salary) AS s FROM employees WHERE id > 100 GROUP BY department",
        "department,c,s",
    )
}

#[test]
fn empty_input_ungrouped_aggregate_has_one_row() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT COUNT(*) AS c, SUM(salary) AS s FROM employees WHERE id > 100",
        "c,s\n0,",
    )
}
