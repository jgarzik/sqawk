//! Tests for SQL aggregate functions in sqawk
//!
//! Tests for COUNT, SUM, AVG, MIN, MAX functions and their combinations.

use crate::helpers::get_employees_file;
use predicates::prelude::*;

#[test]
fn test_basic_aggregate_functions() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = get_employees_file();

    // Run sqawk with basic aggregate functions
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT COUNT(*), SUM(age), AVG(salary), MIN(age), MAX(salary) FROM employees")
        .arg(file_path.to_str().unwrap())
        .arg("-v");

    // employees.csv has 8 rows:
    // COUNT(*) = 8, SUM(age) = 257, AVG(salary) = 67500.0, MIN(age) = 22, MAX(salary) = 90000
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("COUNT,SUM,AVG,MIN,MAX"))
        .stdout(predicate::str::contains("8,257,67500.0,22,90000"));

    Ok(())
}

#[test]
fn test_aggregate_functions_with_aliases() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = get_employees_file();

    // Run sqawk with aggregate functions and aliases
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT COUNT(*) AS count, SUM(salary) AS total_salary, AVG(age) AS avg_age, MIN(salary) AS min_salary, MAX(age) AS max_age FROM employees")
        .arg(file_path.to_str().unwrap())
        .arg("-v");

    // count = 8, total_salary = 540000, avg_age = 32.125, min_salary = 45000, max_age = 45
    cmd.assert()
        .success()
        .stdout(predicate::str::contains(
            "count,total_salary,avg_age,min_salary,max_age",
        ))
        .stdout(predicate::str::contains("8,540000,32.125,45000,45"));

    Ok(())
}

#[test]
fn test_aggregate_functions_with_filter() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = get_employees_file();

    // Run sqawk with filtered aggregate functions
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT COUNT(*), SUM(salary), AVG(age), MIN(salary), MAX(age) FROM employees WHERE age > 25")
        .arg(file_path.to_str().unwrap())
        .arg("-v");

    // Rows with age > 25: Alice(30), Charlie(35), David(40), Eve(28), Frank(32), Grace(45) = 6 rows
    // COUNT = 6, SUM(salary) = 440000, AVG(age) = 35.0, MIN(salary) = 60000, MAX(age) = 45
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("COUNT,SUM,AVG,MIN,MAX"))
        .stdout(predicate::str::contains("6,440000,35.0,60000,45"));

    Ok(())
}

#[test]
fn test_aggregate_on_employees() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = get_employees_file();

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT COUNT(*) AS total_count, AVG(age) AS average_age FROM employees")
        .arg(file_path.to_str().unwrap())
        .arg("-v");

    // COUNT(*) = 8, AVG(age) = 32.125
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("total_count,average_age"))
        .stdout(predicate::str::contains("8,32.125"));

    Ok(())
}

// ---------------------------------------------------------------------------
// GROUP_CONCAT, TOTAL, FILTER, overflow. Expected values from SQLite 3.45.
// ---------------------------------------------------------------------------

mod sqlite_semantics {
    use crate::helpers::{assert_query_on, assert_query_on_fails, ONE};

    type R = Result<(), Box<dyn std::error::Error>>;

    const EMP: (&str, &str) = (
        "emp.csv",
        "id,name,dept,salary\n1,Alice,Eng,100\n2,Bob,Eng,80\n3,Carol,Sales,70\n4,Dave,Sales,\n5,Eve,,50\n",
    );

    /// NULLs are skipped; the default separator is ','; no rows gives NULL.
    #[test]
    fn group_concat_and_string_agg() -> R {
        assert_query_on(
            &[EMP],
            "SELECT GROUP_CONCAT(name) AS g FROM emp",
            "g\n\"Alice,Bob,Carol,Dave,Eve\"",
        )?;
        assert_query_on(
            &[EMP],
            "SELECT STRING_AGG(name, '-') AS g, GROUP_CONCAT(salary) AS s FROM emp",
            "g,s\nAlice-Bob-Carol-Dave-Eve,\"100,80,70,50\"",
        )?;
        assert_query_on(
            &[EMP],
            "SELECT dept, GROUP_CONCAT(name, ';') AS g, GROUP_CONCAT(DISTINCT dept) AS d \
             FROM emp GROUP BY dept",
            "dept,g,d\n,Eve,\nEng,Alice;Bob,Eng\nSales,Carol;Dave,Sales",
        )?;
        assert_query_on(
            &[EMP],
            "SELECT GROUP_CONCAT(name) AS g FROM emp WHERE id > 100",
            "g\n\"\"",
        )
    }

    #[test]
    fn group_concat_as_window_function() -> R {
        assert_query_on(
            &[EMP],
            "SELECT id, GROUP_CONCAT(name, '|') OVER (PARTITION BY dept ORDER BY id) AS g FROM emp",
            "id,g\n5,Eve\n1,Alice\n2,Alice|Bob\n3,Carol\n4,Carol|Dave",
        )
    }

    #[test]
    fn group_concat_distinct_takes_one_argument() -> R {
        assert_query_on_fails(
            &[EMP],
            "SELECT GROUP_CONCAT(DISTINCT name, ',') FROM emp",
            "DISTINCT aggregates must have exactly one argument",
        )
    }

    /// TOTAL is SUM as a real, and 0.0 (not NULL) over no rows.
    #[test]
    fn total() -> R {
        assert_query_on(
            &[EMP],
            "SELECT TOTAL(salary) AS t, TOTAL(NULL) AS n, typeof(TOTAL(salary)) AS ty FROM emp",
            "t,n,ty\n300.0,0.0,real",
        )?;
        assert_query_on(
            &[EMP],
            "SELECT TOTAL(salary) AS t FROM emp WHERE id > 100",
            "t\n0.0",
        )
    }

    #[test]
    fn filter_clause() -> R {
        assert_query_on(
            &[EMP],
            "SELECT COUNT(*) FILTER (WHERE salary > 60) AS a, \
             SUM(salary) FILTER (WHERE dept = 'Eng') AS b, COUNT(*) AS c FROM emp",
            "a,b,c\n3,180,5",
        )?;
        assert_query_on(
            &[EMP],
            "SELECT dept, COUNT(*) FILTER (WHERE salary >= 70) AS a, \
             MAX(name) FILTER (WHERE id > 1) AS m FROM emp GROUP BY dept",
            "dept,a,m\n,0,Eve\nEng,2,Bob\nSales,1,Dave",
        )?;
        assert_query_on(
            &[EMP],
            "SELECT dept, COUNT(*) AS c FROM emp GROUP BY dept \
             HAVING COUNT(*) FILTER (WHERE salary > 75) > 0",
            "dept,c\nEng,2",
        )
    }

    /// SUM of integers that overflows is an error (SQLite); AVG and TOTAL
    /// carry on in floating point.
    #[test]
    fn sum_overflow() -> R {
        const BIG: &str = "(SELECT 9223372036854775807 AS x FROM one UNION ALL SELECT 1 FROM one)";
        assert_query_on_fails(
            &[ONE],
            &format!("SELECT SUM(x) FROM {} z", BIG),
            "integer overflow",
        )?;
        assert_query_on(
            &[ONE],
            &format!("SELECT AVG(x) AS a, TOTAL(x) AS t FROM {} z", BIG),
            "a,t\n4.61168601842739e+18,9.22337203685478e+18",
        )
    }
}
