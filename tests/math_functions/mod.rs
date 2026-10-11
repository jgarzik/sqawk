//! Math function tests for sqawk VM
//!
//! Tests for ABS, ROUND, CEIL, FLOOR functions.

use crate::helpers::{create_custom_csv, create_temp_dir};

/// Test ABS function with negative value
#[test]
fn test_abs_negative() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,value\n1,-5\n2,10\n3,-15\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT id, ABS(value) AS abs_val FROM data")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,5"))
        .stdout(predicates::str::contains("2,10"))
        .stdout(predicates::str::contains("3,15"));

    Ok(())
}

/// Test ROUND function
#[test]
fn test_round() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,value\n1,3.7\n2,3.2\n3,3.5\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT id, ROUND(value) AS rounded FROM data")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,4"))
        .stdout(predicates::str::contains("2,3"))
        .stdout(predicates::str::contains("3,4"));

    Ok(())
}

/// Test CEIL function
#[test]
fn test_ceil() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,value\n1,3.1\n2,3.9\n3,4.0\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT id, CEIL(value) AS ceiled FROM data")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,4"))
        .stdout(predicates::str::contains("2,4"))
        .stdout(predicates::str::contains("3,4"));

    Ok(())
}

/// Test FLOOR function
#[test]
fn test_floor() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,value\n1,3.1\n2,3.9\n3,4.0\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT id, FLOOR(value) AS floored FROM data")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,3"))
        .stdout(predicates::str::contains("2,3"))
        .stdout(predicates::str::contains("3,4"));

    Ok(())
}

/// ROUND with an explicit precision argument.
///
/// The second argument used to be parsed and then silently dropped, so
/// `ROUND(3.14159, 2)` returned `3` rather than `3.14`.
#[test]
fn test_round_precision() -> Result<(), Box<dyn std::error::Error>> {
    crate::helpers::assert_query(
        &["tests/data/employees.csv"],
        "SELECT ROUND(3.14159, 2) AS a, ROUND(2.71828, 3) AS b, ROUND(1.005, 0) AS c \
         FROM employees LIMIT 1",
        concat!("a,b,c", "\n3.14,2.718,1.0"),
    )
}

/// A zero or absent precision must keep the original whole-number behaviour.
#[test]
fn test_round_precision_zero_matches_one_arg() -> Result<(), Box<dyn std::error::Error>> {
    crate::helpers::assert_query(
        &["tests/data/employees.csv"],
        "SELECT ROUND(3.7) AS a, ROUND(3.7, 0) AS b FROM employees LIMIT 1",
        concat!("a,b", "\n4.0,4.0"),
    )
}

/// Negative precision is 0, as in SQLite.
#[test]
fn test_round_negative_precision() -> Result<(), Box<dyn std::error::Error>> {
    crate::helpers::assert_query(
        &["tests/data/employees.csv"],
        "SELECT ROUND(1234.5, -2) AS a FROM employees LIMIT 1",
        concat!("a", "\n1235.0"),
    )
}

/// Precision applies to a column value, not just a literal.
#[test]
fn test_round_precision_on_column() -> Result<(), Box<dyn std::error::Error>> {
    crate::helpers::assert_query(
        &["tests/data/employees.csv"],
        "SELECT name, ROUND(salary / 7.0, 2) AS share FROM employees LIMIT 3",
        concat!(
            "name,share",
            "\nAlice,10000.0",
            "\nBob,7857.14",
            "\nCharlie,9285.71",
        ),
    )
}

/// A precision argument is only meaningful for ROUND; the other math
/// functions take exactly one argument and must say so rather than ignore it.
#[test]
fn test_math_func_rejects_extra_argument() -> Result<(), Box<dyn std::error::Error>> {
    crate::helpers::assert_query_fails(
        &["tests/data/employees.csv"],
        "SELECT ABS(-5, 2) FROM employees",
        "wrong number of arguments to function ABS()",
    )
}

/// The arity check must hold on the no-FROM compile path too.
///
/// `SELECT ABS(-5, 2)` used to return 5 while `SELECT ABS(-5, 2) FROM t`
/// errored: expression compilation has two entry points, and only one of
/// them validated argument counts.
#[test]
fn test_math_func_arity_enforced_without_from() -> Result<(), Box<dyn std::error::Error>> {
    crate::helpers::assert_query_fails(
        &["tests/data/employees.csv"],
        "SELECT ABS(-5, 2)",
        "wrong number of arguments to function ABS()",
    )?;
    crate::helpers::assert_query_fails(
        &["tests/data/employees.csv"],
        "SELECT ROUND(3.14159, 2, 9)",
        "wrong number of arguments to function ROUND()",
    )
}

/// ROUND's precision still works on the no-FROM path.
///
/// Headers are col0/col1 rather than the aliases: `AS` is dropped when there
/// is no FROM clause. That is a separate, pre-existing quirk -- what this
/// test pins is the values.
#[test]
fn test_round_precision_without_from() -> Result<(), Box<dyn std::error::Error>> {
    crate::helpers::assert_query(
        &["tests/data/employees.csv"],
        "SELECT ROUND(3.14159, 2) AS a, ROUND(1234.5, -2) AS b",
        concat!("a,b", "\n3.14,1235.0"),
    )
}

/// CEIL/FLOOR are lowered by sqlparser to Expr::Ceil/Expr::Floor rather than
/// Expr::Function, so they bypassed the arity check entirely: CEIL(1.2, 3)
/// returned 2 while the CEILING spelling of the same call errored.
#[test]
fn test_ceil_floor_reject_scale_argument() -> Result<(), Box<dyn std::error::Error>> {
    for sql in [
        "SELECT CEIL(1.2, 3) FROM employees",
        "SELECT FLOOR(1.7, 3) FROM employees",
        "SELECT CEIL(1.2, 3)",
        "SELECT FLOOR(1.7, 3)",
    ] {
        crate::helpers::assert_query_fails(
            &["tests/data/employees.csv"],
            sql,
            "takes exactly one argument",
        )?;
    }
    Ok(())
}

/// A precision that is not a number must be reported, not treated as 0.
#[test]
fn test_round_rejects_non_numeric_precision() -> Result<(), Box<dyn std::error::Error>> {
    crate::helpers::assert_query_fails(
        &["tests/data/employees.csv"],
        "SELECT ROUND(3.14159, 'two') FROM employees",
        "precision",
    )
}

/// Two ROUND calls differing only in precision must not collapse to one
/// column name, which would make the second column unaddressable.
#[test]
fn test_round_precision_distinguishes_column_names() -> Result<(), Box<dyn std::error::Error>> {
    crate::helpers::assert_query(
        &["tests/data/employees.csv"],
        "SELECT ROUND(3.14159, 1), ROUND(3.14159, 3) FROM employees LIMIT 1",
        // The names contain the delimiter, so the CSV header quotes them.
        concat!("\"ROUND(expr, 1)\",\"ROUND(expr, 3)\"", "\n3.1,3.142"),
    )
}

// ---------------------------------------------------------------------------
// SQLite math semantics. Expected values computed with SQLite 3.45.
// ---------------------------------------------------------------------------

mod sqlite_semantics {
    use crate::helpers::{assert_query_on, ONE};

    type R = Result<(), Box<dyn std::error::Error>>;

    /// ROUND rounds half away from zero on the decimal value actually
    /// stored (2.675 is 2.67499..., so it rounds down) and returns a real.
    #[test]
    fn round_like_sqlite() -> R {
        assert_query_on(
            &[ONE],
            "SELECT round(2.675, 2) AS a, round(0.125, 2) AS b, round(1.005, 2) AS c, \
             round(2.5) AS d, round(-2.5) AS e, round(-2.25, 1) AS f, typeof(round(5)) AS g \
             FROM one",
            "a,b,c,d,e,f,g\n2.67,0.13,1.0,3.0,-3.0,-2.3,real",
        )
    }

    /// ABS keeps an integer an integer; text gives a real. CEIL/FLOOR return
    /// an integer argument unchanged.
    #[test]
    fn abs_ceil_floor_types() -> R {
        assert_query_on(
            &[ONE],
            "SELECT abs(-5) AS a, typeof(abs(-5)) AS b, abs(-2.5) AS c, abs('-3') AS d, \
             typeof(abs('-3')) AS e, ceil(1.2) AS f, typeof(ceil(1.2)) AS g, \
             typeof(ceil(3)) AS h, floor(-1.5) AS i FROM one",
            "a,b,c,d,e,f,g,h,i\n5,integer,2.5,3.0,real,2.0,real,integer,-2.0",
        )
    }
}
