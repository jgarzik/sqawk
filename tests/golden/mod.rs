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
        concat!(
            "col0",
            "\n1",
        ),
    )
}

#[test]
fn where_eq() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees WHERE department = 'Engineering'",
        concat!(
            "name",
            "\nAlice",
            "\nCharlie",
            "\nFrank",
        ),
    )
}

#[test]
fn where_gt() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees WHERE salary > 70000",
        concat!(
            "name",
            "\nDavid",
            "\nFrank",
            "\nGrace",
        ),
    )
}

#[test]
fn where_and() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees WHERE salary > 60000 AND department = 'Engineering'",
        concat!(
            "name",
            "\nAlice",
            "\nCharlie",
            "\nFrank",
        ),
    )
}

#[test]
fn where_or() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees WHERE department = 'HR' OR department = 'Sales'",
        concat!(
            "name",
            "\nDavid",
            "\nGrace",
            "\nHenry",
        ),
    )
}

#[test]
fn where_ne() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees WHERE department <> 'Engineering'",
        concat!(
            "name",
            "\nBob",
            "\nDavid",
            "\nEve",
            "\nGrace",
            "\nHenry",
        ),
    )
}

#[test]
fn where_like() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees WHERE name LIKE 'A%'",
        concat!(
            "name",
            "\nAlice",
        ),
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
        concat!(
            "name",
            "\nAlice",
        ),
    )
}

#[test]
fn where_between() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees WHERE age BETWEEN 30 AND 40",
        concat!(
            "name",
            "\nAlice",
            "\nCharlie",
            "\nDavid",
            "\nFrank",
        ),
    )
}

#[test]
fn where_not_between() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees WHERE age NOT BETWEEN 30 AND 40",
        concat!(
            "name",
            "\nBob",
            "\nEve",
            "\nGrace",
            "\nHenry",
        ),
    )
}

#[test]
fn where_in_list() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees WHERE department IN ('HR', 'Sales')",
        concat!(
            "name",
            "\nDavid",
            "\nGrace",
            "\nHenry",
        ),
    )
}

#[test]
fn where_not_in_list() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees WHERE department NOT IN ('HR', 'Sales')",
        concat!(
            "name",
            "\nAlice",
            "\nBob",
            "\nCharlie",
            "\nEve",
            "\nFrank",
        ),
    )
}

#[test]
fn where_is_null() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/nullable.csv"],
        "SELECT id FROM nullable WHERE score IS NULL",
        concat!(
            "id",
            "\n2",
        ),
    )
}

#[test]
fn where_is_not_null() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/nullable.csv"],
        "SELECT id FROM nullable WHERE score IS NOT NULL",
        concat!(
            "id",
            "\n1",
            "\n3",
            "\n4",
            "\n5",
        ),
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
        concat!(
            "name",
            "\nAlice",
            "\nBob",
            "\nCharlie",
        ),
    )
}

#[test]
fn limit_offset() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees LIMIT 3 OFFSET 2",
        concat!(
            "name",
            "\nCharlie",
            "\nDavid",
            "\nEve",
        ),
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
        concat!(
            "COUNT",
            "\n8",
        ),
    )
}

#[test]
fn agg_sum() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT SUM(salary) FROM employees",
        concat!(
            "SUM",
            "\n540000",
        ),
    )
}

#[test]
fn agg_avg() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT AVG(salary) FROM employees",
        concat!(
            "AVG",
            "\n67500",
        ),
    )
}

#[test]
fn agg_min_max() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT MIN(salary), MAX(salary) FROM employees",
        concat!(
            "MIN,MAX",
            "\n45000,90000",
        ),
    )
}

#[test]
fn agg_with_where() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT COUNT(*) FROM employees WHERE department = 'Engineering'",
        concat!(
            "COUNT",
            "\n3",
        ),
    )
}

#[test]
fn agg_alias() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT COUNT(*) AS n, SUM(salary) AS total FROM employees",
        concat!(
            "n,total",
            "\n8,540000",
        ),
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
            "\nMarketing",
            "\nHR",
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
            "\nMarketing",
            "\nHR",
        ),
    )
}

#[test]
fn subquery_scalar() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees WHERE salary = (SELECT MAX(salary) FROM employees)",
        concat!(
            "name",
            "\nGrace",
        ),
    )
}

#[test]
fn subquery_in() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/users.csv", "tests/data/orders.csv"],
        "SELECT name FROM users WHERE id IN (SELECT user_id FROM orders)",
        concat!(
            "name",
            "\nJohn",
            "\nJane",
        ),
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
        // NOTE: ABS(i64::MIN) has no representable result. sqawk saturates to
        // i64::MAX rather than erroring or wrapping. Pinned so any change to
        // that choice is deliberate, not so that it is endorsed.
        "SELECT ABS(value) FROM boundaries",
        concat!(
            "ABS(value)",
            "\n0",
            "\n1",
            "\n1",
            "\n9223372036854775807",
            "\n9223372036854775807",
        ),
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
            "\nHenry,NULL",
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
            "\nHenry,NULL",
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
            "\nGrace,NULL",
        ),
    )
}

#[test]
fn case_in_where() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees WHERE CASE WHEN age > 30 THEN 1 ELSE 0 END = 1",
        concat!(
            "name",
            "\nCharlie",
            "\nDavid",
            "\nFrank",
            "\nGrace",
        ),
    )
}

#[test]
fn cast_in_where() -> Result<(), Box<dyn std::error::Error>> {
    assert_query(
        &["tests/data/employees.csv"],
        "SELECT name FROM employees WHERE CAST(salary AS INTEGER) > 70000",
        concat!(
            "name",
            "\nDavid",
            "\nFrank",
            "\nGrace",
        ),
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
            "\nCara,NULL",
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
            "\nNULL,Orphan",
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
            "\nCara,NULL",
            "\nNULL,Orphan",
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
    // After aggregation only the projected columns still exist, so ordering by
    // something else must be an error, not a silently ignored clause.
    assert_query_fails(
        &["tests/data/employees.csv"],
        "SELECT department, COUNT(*) FROM employees GROUP BY department ORDER BY salary",
        "must appear in the SELECT list",
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
        "name\nNULL",
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
        "id,score\n2,NULL\n4,50\n3,70\n1,90\n5,90",
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
