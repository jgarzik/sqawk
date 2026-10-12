//! Tests for SQL comparison operators
//!
//! This module contains tests for all SQL comparison operators (=, !=, >, <, >=, <=)
//! to ensure they work correctly with integer types.

use predicates::prelude::PredicateBooleanExt;

use crate::helpers::{get_boundaries_file, get_sample_file};

// Test cases for each comparison operator

#[test]
fn test_equals_operator() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = get_sample_file();

    // Using the sample.csv file which has columns: id,name,age
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM sample WHERE age = 32")
        .arg(file_path);

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("id,name,age"))
        .stdout(predicates::str::contains("1,Alice,32"));

    Ok(())
}

#[test]
fn test_not_equals_operator() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = get_sample_file();

    // Using the sample.csv file which has columns: id,name,age
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM sample WHERE age != 32")
        .arg(file_path);

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("id,name,age"))
        .stdout(predicates::str::contains("2,Bob,25"))
        .stdout(predicates::str::contains("3,Charlie,35"))
        .stdout(predicates::str::contains("1,Alice,32").not());

    Ok(())
}

#[test]
fn test_greater_than_operator() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = get_sample_file();

    // Using the sample.csv file which has columns: id,name,age
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM sample WHERE age > 32")
        .arg(file_path);

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("id,name,age"))
        .stdout(predicates::str::contains("3,Charlie,35"))
        .stdout(predicates::str::contains("1,Alice,32").not())
        .stdout(predicates::str::contains("2,Bob,25").not());

    Ok(())
}

#[test]
fn test_less_than_operator() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = get_sample_file();

    // Using the sample.csv file which has columns: id,name,age
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM sample WHERE age < 32")
        .arg(file_path);

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("id,name,age"))
        .stdout(predicates::str::contains("2,Bob,25"))
        .stdout(predicates::str::contains("1,Alice,32").not())
        .stdout(predicates::str::contains("3,Charlie,35").not());

    Ok(())
}

#[test]
fn test_greater_than_or_equal_operator() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = get_sample_file();

    // Using the sample.csv file which has columns: id,name,age
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM sample WHERE age >= 32")
        .arg(file_path);

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("id,name,age"))
        .stdout(predicates::str::contains("1,Alice,32"))
        .stdout(predicates::str::contains("3,Charlie,35"))
        .stdout(predicates::str::contains("2,Bob,25").not());

    Ok(())
}

#[test]
fn test_less_than_or_equal_operator() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = get_sample_file();

    // Using the sample.csv file which has columns: id,name,age
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM sample WHERE age <= 32")
        .arg(file_path);

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("id,name,age"))
        .stdout(predicates::str::contains("1,Alice,32"))
        .stdout(predicates::str::contains("2,Bob,25"))
        .stdout(predicates::str::contains("3,Charlie,35").not());

    Ok(())
}

#[test]
fn test_equals_with_no_matches() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = get_sample_file();

    // Using the sample.csv file which has columns: id,name,age
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM sample WHERE age = 40")
        .arg(file_path);

    // Should return just the header, no data rows
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("id,name,age"))
        .stdout(
            predicates::str::contains("id,name,age").and(
                predicates::str::contains("1,")
                    .not()
                    .and(predicates::str::contains("2,").not())
                    .and(predicates::str::contains("3,").not()),
            ),
        );

    Ok(())
}

// Boundary tests for comparison operators

#[test]
fn test_comparison_boundary_values() -> Result<(), Box<dyn std::error::Error>> {
    // Use the static boundaries.csv file with extreme integer values
    let file_path = get_boundaries_file();

    // Test with max integer value
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM boundaries WHERE value = 9223372036854775807")
        .arg(file_path.clone());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("id,value"))
        .stdout(predicates::str::contains("4,9223372036854775807"));

    // Test with min integer value
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM boundaries WHERE value = -9223372036854775808")
        .arg(file_path.clone());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("id,value"))
        .stdout(predicates::str::contains("5,-9223372036854775808"));

    // Test greater than zero
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM boundaries WHERE value > 0")
        .arg(file_path.clone());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("id,value"))
        .stdout(predicates::str::contains("2,1"))
        .stdout(predicates::str::contains("4,9223372036854775807"));

    // Test less than zero
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM boundaries WHERE value < 0")
        .arg(file_path.clone());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("id,value"))
        .stdout(predicates::str::contains("3,-1"))
        .stdout(predicates::str::contains("5,-9223372036854775808"));

    Ok(())
}

// ---------------------------------------------------------------------------
// Expected values from SQLite 3.45.
// ---------------------------------------------------------------------------

mod sqlite_semantics {
    use crate::helpers::{assert_query, assert_query_on, employees_csv};

    type R = Result<(), Box<dyn std::error::Error>>;

    /// NULL-safe equality.
    #[test]
    fn is_distinct_from() -> R {
        assert_query_on(
            &[("t.csv", "k,x\n1,2\n2,\n")],
            "SELECT k, x IS NOT DISTINCT FROM NULL AS a, x IS DISTINCT FROM 2 AS b, \
             NULL IS DISTINCT FROM NULL AS c, 2 IS NOT DISTINCT FROM 2.0 AS d FROM t",
            "k,a,b,c,d\n1,0,0,0,1\n2,1,1,0,1",
        )
    }

    /// Table names match case-insensitively, like every SQL identifier.
    #[test]
    fn table_names_are_case_insensitive() -> R {
        assert_query(
            &[employees_csv()],
            "SELECT EMPLOYEES.NAME FROM EMPLOYEES WHERE ID = 2",
            "name\nBob",
        )
    }
}
