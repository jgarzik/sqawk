//! Tests for ORDER BY functionality in sqawk
//!
//! Tests for sorting functionality with different column combinations and directions.

use crate::helpers::*;
use predicates::prelude::*;

/// Test ordering by a single column in ascending order (implicit)
#[test]
fn test_order_by_single_column() -> Result<(), Box<dyn std::error::Error>> {
    // Define the test case
    let test_case = SqawkTestCase {
        sql: "SELECT * FROM people ORDER BY age".to_string(),
        expected_stdout: vec![
            "id,name,age".to_string(),
            "2,Bob,25".to_string(),
            "1,Alice,32".to_string(),
            "3,Charlie,35".to_string(),
        ],
        verbose: true,
        ..Default::default()
    };

    run_test_case(test_case)
}

/// Test ordering by a single column in descending order (explicit)
#[test]
fn test_order_by_single_column_desc() -> Result<(), Box<dyn std::error::Error>> {
    // Define the test case
    let test_case = SqawkTestCase {
        sql: "SELECT * FROM people ORDER BY age DESC".to_string(),
        expected_stdout: vec![
            "id,name,age".to_string(),
            "3,Charlie,35".to_string(),
            "1,Alice,32".to_string(),
            "2,Bob,25".to_string(),
        ],
        verbose: true,
        ..Default::default()
    };

    run_test_case(test_case)
}

/// Test ordering by multiple columns with mixed directions using static sample data
#[test]
fn test_order_by_multiple_columns() -> Result<(), Box<dyn std::error::Error>> {
    // Create a custom CSV file with needed data
    let temp_dir = create_temp_dir()?;
    let content = "dept,name,salary\nIT,Alice,70000\nHR,Bob,55000\nIT,Charlie,65000\nHR,David,60000\nIT,Eve,75000\n";
    let file_path = create_custom_csv(temp_dir.path(), "employees.csv", content)?;

    // Build the command
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM employees ORDER BY dept ASC, salary DESC")
        .arg(format!("employees={}", file_path.to_str().unwrap()));

    // Check headers
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("dept,name,salary"));

    // Check ordering: first HR department
    let output = cmd.output()?;
    let stdout = String::from_utf8(output.stdout)?;
    let lines: Vec<&str> = stdout.lines().collect();

    // Verify expected order:
    // Header: dept,name,salary
    // Line 1: HR,David,60000
    // Line 2: HR,Bob,55000
    // Line 3: IT,Eve,75000
    // Line 4: IT,Alice,70000
    // Line 5: IT,Charlie,65000
    assert_eq!(lines[0], "dept,name,salary");
    assert!(lines[1].contains("HR") && lines[1].contains("David") && lines[1].contains("60000"));
    assert!(lines[2].contains("HR") && lines[2].contains("Bob") && lines[2].contains("55000"));
    assert!(lines[3].contains("IT") && lines[3].contains("Eve") && lines[3].contains("75000"));
    assert!(lines[4].contains("IT") && lines[4].contains("Alice") && lines[4].contains("70000"));
    assert!(lines[5].contains("IT") && lines[5].contains("Charlie") && lines[5].contains("65000"));

    Ok(())
}

/// Test ordering with a WHERE clause to ensure filtering happens before sorting
#[test]
fn test_order_by_with_where() -> Result<(), Box<dyn std::error::Error>> {
    // Create a custom CSV file with needed data
    let temp_dir = create_temp_dir()?;
    let content = "id,name,age,dept\n1,Alice,30,IT\n2,Bob,25,HR\n3,Charlie,35,IT\n4,David,40,HR\n5,Eve,28,IT\n";
    let file_path = create_custom_csv(temp_dir.path(), "employees.csv", content)?;

    // Build the command
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT name, age FROM employees WHERE dept = 'IT' ORDER BY age DESC")
        .arg(format!("employees={}", file_path.to_str().unwrap()));

    // Check output
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("name,age"))
        .stdout(predicate::str::contains("Charlie,35"))
        .stdout(predicate::str::contains("Alice,30"))
        .stdout(predicate::str::contains("Eve,28"));

    // Make sure we don't have HR department entries
    let output = cmd.output()?;
    let stdout = String::from_utf8(output.stdout)?;
    assert!(
        !stdout.contains("Bob"),
        "Should not contain Bob from HR department"
    );
    assert!(
        !stdout.contains("David"),
        "Should not contain David from HR department"
    );

    Ok(())
}

/// Test ordering with a projection to ensure column selection works with ORDER BY
/// We include the sort column in the projection to comply with SQL execution
#[test]
fn test_order_by_with_projection() -> Result<(), Box<dyn std::error::Error>> {
    // Create a custom CSV file with needed data
    let temp_dir = create_temp_dir()?;
    let content = "id,name,age,salary\n1,Alice,30,70000\n2,Bob,25,55000\n3,Charlie,35,65000\n4,David,40,80000\n";
    let file_path = create_custom_csv(temp_dir.path(), "employees.csv", content)?;

    // Build the command - include age in the SELECT since we need it for ORDER BY
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT name, salary, age FROM employees ORDER BY age DESC")
        .arg(format!("employees={}", file_path.to_str().unwrap()));

    // Check output - should now include age column since we're ordering by it
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("name,salary,age"))
        .stdout(predicate::str::contains("David,80000,40")) // Oldest
        .stdout(predicate::str::contains("Charlie,65000,35"))
        .stdout(predicate::str::contains("Alice,70000,30"))
        .stdout(predicate::str::contains("Bob,55000,25")); // Youngest

    Ok(())
}

// ---------------------------------------------------------------------------
// NULLS FIRST / NULLS LAST. Expected values from SQLite 3.45.
// ---------------------------------------------------------------------------

mod nulls_placement {
    use crate::helpers::{assert_query_on, ONE};

    type R = Result<(), Box<dyn std::error::Error>>;

    const EMP: (&str, &str) = (
        "emp.csv",
        "id,name,dept,salary\n1,Alice,Eng,100\n2,Bob,Eng,80\n3,Carol,Sales,70\n4,Dave,Sales,\n5,Eve,,50\n",
    );

    #[test]
    fn order_by_nulls_first_last() -> R {
        assert_query_on(
            &[EMP],
            "SELECT id, salary FROM emp ORDER BY salary NULLS LAST, id",
            "id,salary\n5,50\n3,70\n2,80\n1,100\n4,",
        )?;
        assert_query_on(
            &[EMP],
            "SELECT id, salary FROM emp ORDER BY salary DESC NULLS FIRST, id",
            "id,salary\n4,\n1,100\n2,80\n3,70\n5,50",
        )?;
        assert_query_on(
            &[EMP],
            "SELECT id, dept FROM emp ORDER BY dept NULLS FIRST, id DESC",
            "id,dept\n5,\n2,Eng\n1,Eng\n4,Sales\n3,Sales",
        )
    }

    #[test]
    fn window_order_by_nulls() -> R {
        assert_query_on(
            &[EMP],
            "SELECT id, RANK() OVER (ORDER BY salary NULLS LAST) AS r FROM emp",
            "id,r\n5,1\n3,2\n2,3\n1,4\n4,5",
        )?;
        assert_query_on(
            &[EMP],
            "SELECT id, ROW_NUMBER() OVER (ORDER BY salary DESC NULLS FIRST, id) AS r FROM emp",
            "id,r\n4,1\n1,2\n2,3\n3,4\n5,5",
        )
    }

    #[test]
    fn compound_order_by_nulls() -> R {
        assert_query_on(
            &[EMP, ONE],
            "SELECT salary FROM emp UNION SELECT NULL FROM one ORDER BY 1 NULLS LAST",
            "salary\n50\n70\n80\n100\n\"\"",
        )
    }
}

// ---------------------------------------------------------------------------
// Integer ORDER BY / GROUP BY terms. An integer literal that fits in 32 bits,
// under any parentheses and unary `+` / `-`, is a result-column position;
// anything else is an ordinary expression. Expected values from SQLite 3.45.
// ---------------------------------------------------------------------------

mod integer_terms {
    use crate::helpers::{assert_query_on, assert_query_on_fails};

    type R = Result<(), Box<dyn std::error::Error>>;

    const T: (&str, &str) = ("t.csv", "a,b\n3,x\n1,y\n2,z\n");
    const U: (&str, &str) = ("u.csv", "c\n5\n1\n");

    #[test]
    fn parenthesized_and_signed_positions() -> R {
        for sql in [
            "SELECT a FROM t ORDER BY (1)",
            "SELECT a FROM t ORDER BY +(+1)",
            "SELECT a FROM t ORDER BY - -1",
            "SELECT a FROM t ORDER BY 01",
            "SELECT a FROM t ORDER BY 0x1",
        ] {
            assert_query_on(&[T], sql, "a\n1\n2\n3")?;
        }
        assert_query_on(
            &[T],
            "SELECT b, a FROM t ORDER BY 0x2",
            "b,a\ny,1\nz,2\nx,3",
        )
    }

    #[test]
    fn non_positions_are_constants() -> R {
        for sql in [
            "SELECT a FROM t ORDER BY 1e0",
            "SELECT a FROM t ORDER BY 1+0",
            "SELECT a FROM t ORDER BY '1'",
            "SELECT a FROM t ORDER BY -2147483648",
            "SELECT a FROM t ORDER BY 0x80000000",
            "SELECT a FROM t ORDER BY 0xFFFFFFFFFFFFFFFF",
        ] {
            assert_query_on(&[T], sql, "a\n3\n1\n2")?;
        }
        assert_query_on(
            &[T],
            "SELECT COUNT(*) AS n FROM t GROUP BY -2147483648",
            "n\n3",
        )?;
        assert_query_on(&[T], "SELECT COUNT(*) AS n FROM t GROUP BY 1.0", "n\n3")
    }

    #[test]
    fn out_of_range_errors_name_the_term() -> R {
        for (sql, err) in [
            (
                "SELECT a FROM t ORDER BY -(1)",
                "1st ORDER BY term out of range - should be between 1 and 1",
            ),
            (
                "SELECT a FROM t ORDER BY +-1",
                "1st ORDER BY term out of range - should be between 1 and 1",
            ),
            (
                "SELECT a FROM t ORDER BY 0x7fffffff",
                "1st ORDER BY term out of range - should be between 1 and 1",
            ),
            (
                "SELECT a FROM t ORDER BY 1,1,1,1,1,1,1,1,1,1,1,2",
                "12th ORDER BY term out of range - should be between 1 and 1",
            ),
            (
                "SELECT a FROM t ORDER BY 1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,2",
                "22nd ORDER BY term out of range - should be between 1 and 1",
            ),
            (
                "SELECT a FROM t GROUP BY a ORDER BY 2",
                "1st ORDER BY term out of range - should be between 1 and 1",
            ),
            (
                "SELECT a, b FROM t GROUP BY 1, 3",
                "2nd GROUP BY term out of range - should be between 1 and 2",
            ),
        ] {
            assert_query_on_fails(&[T], sql, err)?;
        }
        Ok(())
    }

    #[test]
    fn grouped_positions() -> R {
        assert_query_on(
            &[T],
            "SELECT a % 2 AS p, COUNT(*) AS n FROM t GROUP BY (+1) ORDER BY 1",
            "p,n\n0,1\n1,2",
        )?;
        assert_query_on(
            &[T],
            "SELECT a AS x FROM t GROUP BY x HAVING 1 ORDER BY +1 DESC",
            "x\n3\n2\n1",
        )
    }

    #[test]
    fn compound_positions() -> R {
        assert_query_on(
            &[T, U],
            "SELECT a FROM t UNION SELECT c FROM u ORDER BY +1 DESC",
            "a\n5\n3\n2\n1",
        )?;
        assert_query_on(
            &[T, U],
            "SELECT a FROM t UNION ALL SELECT c FROM u ORDER BY (1)",
            "a\n1\n1\n2\n3\n5",
        )?;
        assert_query_on(
            &[T, U],
            "SELECT a, b FROM t UNION SELECT c, 'w' FROM u ORDER BY - -2, 1",
            "a,b\n1,w\n5,w\n3,x\n1,y\n2,z",
        )
    }

    /// In a compound, a term that is not a position must name a result
    /// column; a constant does not.
    #[test]
    fn compound_non_positions_match_no_column() -> R {
        for sql in [
            "SELECT a FROM t UNION SELECT c FROM u ORDER BY 1.0",
            "SELECT a FROM t UNION SELECT c FROM u ORDER BY 4294967297",
            "SELECT a FROM t UNION SELECT c FROM u ORDER BY 2147483648",
            "SELECT a FROM t UNION SELECT c FROM u ORDER BY 1+0",
        ] {
            assert_query_on_fails(
                &[T, U],
                sql,
                "1st ORDER BY term does not match any column in the result set",
            )?;
        }
        for (sql, err) in [
            (
                "SELECT a FROM t UNION SELECT c FROM u ORDER BY -1",
                "1st ORDER BY term out of range - should be between 1 and 1",
            ),
            (
                "SELECT a FROM t UNION SELECT c FROM u ORDER BY 0",
                "1st ORDER BY term out of range - should be between 1 and 1",
            ),
            (
                "SELECT a, b FROM t UNION SELECT c, 'w' FROM u ORDER BY 1, 3",
                "2nd ORDER BY term out of range - should be between 1 and 2",
            ),
        ] {
            assert_query_on_fails(&[T, U], sql, err)?;
        }
        Ok(())
    }

    #[test]
    fn distinct_positions() -> R {
        assert_query_on(
            &[T],
            "SELECT DISTINCT a % 2 AS p FROM t ORDER BY -(-1) DESC",
            "p\n1\n0",
        )?;
        assert_query_on(
            &[T],
            "SELECT DISTINCT a % 2 AS p FROM t ORDER BY 1.0",
            "p\n1\n0",
        )?;
        assert_query_on_fails(
            &[T],
            "SELECT DISTINCT a FROM t ORDER BY 0",
            "1st ORDER BY term out of range - should be between 1 and 1",
        )
    }

    #[test]
    fn subquery_positions() -> R {
        assert_query_on(
            &[T],
            "SELECT * FROM (SELECT a FROM t ORDER BY +1 DESC LIMIT 2)",
            "a\n3\n2",
        )?;
        assert_query_on(
            &[T],
            "SELECT (SELECT b FROM t ORDER BY (1) DESC LIMIT 1) AS v",
            "v\nz",
        )?;
        assert_query_on(
            &[T],
            "SELECT (SELECT b FROM t ORDER BY 4294967297 LIMIT 1) AS v",
            "v\nx",
        )?;
        for sql in [
            "SELECT a FROM t WHERE a IN (SELECT a FROM t ORDER BY 2)",
            "SELECT a FROM t WHERE EXISTS (SELECT 1 FROM t ORDER BY -1)",
        ] {
            assert_query_on_fails(
                &[T],
                sql,
                "1st ORDER BY term out of range - should be between 1 and 1",
            )?;
        }
        Ok(())
    }

    /// A window's ORDER BY / PARTITION BY integer is a constant, never a
    /// position: every row is a peer.
    #[test]
    fn window_integers_are_constants() -> R {
        for sql in [
            "SELECT a, RANK() OVER (ORDER BY 1) AS r FROM t ORDER BY a",
            "SELECT a, RANK() OVER (ORDER BY -1) AS r FROM t ORDER BY a",
            "SELECT a, RANK() OVER (PARTITION BY 2 ORDER BY 5) AS r FROM t ORDER BY 1",
        ] {
            assert_query_on(&[T], sql, "a,r\n1,1\n2,1\n3,1")?;
        }
        assert_query_on(
            &[T],
            "SELECT a, SUM(a) OVER (ORDER BY 1) AS s FROM t ORDER BY a",
            "a,s\n1,6\n2,6\n3,6",
        )
    }
}
