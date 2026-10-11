//! Arithmetic operator tests for sqawk VM
//!
//! Tests for +, -, *, /, % operators.

use crate::helpers::{create_custom_csv, create_temp_dir};

/// Test addition operator
#[test]
fn test_addition() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,a,b\n1,10,5\n2,20,3\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT a + b AS sum FROM data")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("15"))
        .stdout(predicates::str::contains("23"));

    Ok(())
}

/// Test subtraction operator
#[test]
fn test_subtraction() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,a,b\n1,10,5\n2,20,3\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT a - b AS diff FROM data")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("5"))
        .stdout(predicates::str::contains("17"));

    Ok(())
}

/// Test multiplication operator
#[test]
fn test_multiplication() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,a,b\n1,4,5\n2,3,7\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT a * b AS product FROM data")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("20"))
        .stdout(predicates::str::contains("21"));

    Ok(())
}

/// Test division operator
#[test]
fn test_division() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,a,b\n1,20,4\n2,15,3\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT a / b AS quotient FROM data")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("5"))
        .stdout(predicates::str::contains("5"));

    Ok(())
}

/// Test modulo operator
#[test]
fn test_modulo() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,a,b\n1,10,3\n2,17,5\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT a % b AS remainder FROM data")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1"))
        .stdout(predicates::str::contains("2"));

    Ok(())
}

/// Test unary minus
#[test]
fn test_unary_minus() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,value\n1,5\n2,-3\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT -value AS negated FROM data")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("-5"))
        .stdout(predicates::str::contains("3"));

    Ok(())
}

// ---------------------------------------------------------------------------
// SQLite numeric semantics. Expected values computed with SQLite 3.45.
// ---------------------------------------------------------------------------

mod sqlite_semantics {
    use crate::helpers::{assert_query_on, ONE};

    type R = Result<(), Box<dyn std::error::Error>>;

    const T: (&str, &str) = ("t.csv", "k,a,b\n1,7,2.5\n2,-7,\n3,10,0\n");

    /// Integer `/` truncates toward zero and `%` is the integer remainder;
    /// a real operand makes the operation real, and a real `%` takes the
    /// remainder of the truncated operands.
    #[test]
    fn integer_division_and_remainder() -> R {
        assert_query_on(
            &[ONE],
            "SELECT 7/2 AS a, -7/2 AS b, 7%3 AS c, -7%3 AS d, 7%-3 AS e, 7.0/2 AS f, \
             5.5%2 AS h, -7.5%2 AS i FROM one",
            "a,b,c,d,e,f,h,i\n3,-3,1,-1,1,3.5,1,-1",
        )
    }

    #[test]
    fn division_by_zero_is_null() -> R {
        assert_query_on(
            &[ONE],
            "SELECT 5/0 AS a, 5%0 AS b, 5.0/0 AS c, 5%0.5 AS d, 5/0.0 AS e FROM one",
            "a,b,c,d,e\n,,,,",
        )
    }

    /// An integer result that does not fit in i64 is computed as a real.
    #[test]
    fn integer_overflow_becomes_real() -> R {
        assert_query_on(
            &[ONE],
            "SELECT 9223372036854775807+1 > 9223372036854775807 AS a, \
             typeof(9223372036854775807 + 1) AS b, -9223372036854775808 AS c, \
             typeof(-9223372036854775808) AS d, typeof(-(-9223372036854775807-1)) AS e, \
             4611686018427387904 * -2 AS f, typeof((-9223372036854775807-1)/-1) AS g, \
             (-9223372036854775807-1) % -1 AS h FROM one",
            "a,b,c,d,e,f,g,h\n1,real,-9223372036854775808,integer,real,-9223372036854775808,real,0",
        )
    }

    /// Integers above 2^53 stay exact (they used to go through f64).
    #[test]
    fn large_integers_are_exact() -> R {
        assert_query_on(
            &[ONE],
            "SELECT 9007199254740993 + 0 AS a, 9007199254740993 * 1 AS b, \
             9007199254740993 - 1 AS c, 3037000500 * 3037000500 > 9223372036854775807 AS d \
             FROM one",
            "a,b,c,d\n9007199254740993,9007199254740993,9007199254740992,1",
        )
    }

    /// Text operands are read by their longest numeric prefix.
    #[test]
    fn text_operands_use_numeric_prefix() -> R {
        assert_query_on(
            &[ONE],
            "SELECT '12abc' + 0 AS a, ' 5 ' + 1 AS b, 'abc' + 1 AS c, '1e3' + 0 AS d, \
             typeof('1e3' + 0) AS e, '007' + 0 AS f, '3.9x' * 2 AS g FROM one",
            "a,b,c,d,e,f,g\n12,6,1,1000,real,7,7.8",
        )
    }

    #[test]
    fn column_arithmetic_keeps_types() -> R {
        assert_query_on(
            &[T],
            "SELECT k, a/2 AS h, a%3 AS m, a/b AS q, a%b AS r, b*2 AS d, typeof(a/2) AS ta, \
             typeof(b*2) AS tb FROM t",
            "k,h,m,q,r,d,ta,tb\n\
             1,3,1,2.8,1,5,integer,real\n\
             2,-3,-1,,,,integer,null\n\
             3,5,1,,,0,integer,integer",
        )
    }

    /// In a condition, text is read as a number: 'x' is false, '1x' true.
    #[test]
    fn text_truth_value_is_numeric() -> R {
        const S: (&str, &str) = ("s.csv", "k,v\n1,x\n2,1x\n3,0\n4,\n5,0.5\n6,abc\n");
        assert_query_on(&[S], "SELECT k FROM s WHERE v", "k\n2\n5")?;
        assert_query_on(
            &[S],
            "SELECT k, NOT v AS n FROM s",
            "k,n\n1,1\n2,0\n3,1\n4,\n5,0\n6,1",
        )
    }

    /// Integer division through UPDATE, persisted by --write.
    #[test]
    fn update_with_integer_division() -> R {
        crate::helpers::assert_query_on(
            &[("t.csv", "k,a\n1,10\n2,-5\n3,\n")],
            "UPDATE t SET a = a / 4; SELECT * FROM t",
            "k,a\n1,2\n2,-1\n3,",
        )
    }
}
