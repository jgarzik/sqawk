//! CAST type conversion tests for sqawk VM
//!
//! This file contains tests for SQL CAST type conversion expressions.

use crate::helpers::{create_custom_csv, create_temp_dir};

/// Test CAST integer to text
#[test]
fn test_cast_integer_to_text() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,value\n1,100\n2,200\n3,300\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    // Test CAST integer to TEXT for comparison
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM data WHERE CAST(value AS TEXT) = '100'")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,100"));

    Ok(())
}

/// Test CAST text to integer
#[test]
fn test_cast_text_to_integer() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,value\n1,50\n2,150\n3,250\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    // Test CAST literal to INTEGER for comparison
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM data WHERE CAST('100' AS INTEGER) < value")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("2,150"))
        .stdout(predicates::str::contains("3,250"));

    Ok(())
}

/// Test CAST with VARCHAR type
#[test]
fn test_cast_to_varchar() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,num\n1,42\n2,99\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    // Test CAST to VARCHAR (should work like TEXT)
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM data WHERE CAST(num AS VARCHAR) = '42'")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,42"));

    Ok(())
}

/// Test CAST column to integer for comparison
#[test]
fn test_cast_column_to_integer() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,strnum\n1,100\n2,200\n3,50\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    // Cast string column to integer for numeric comparison
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM data WHERE CAST(strnum AS INTEGER) > 75")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,100"))
        .stdout(predicates::str::contains("2,200"));

    Ok(())
}

/// Test CAST preserves integer values
#[test]
fn test_cast_integer_to_integer() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,value\n1,10\n2,20\n3,30\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    // CAST integer to integer should preserve value
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM data WHERE CAST(value AS INT) = 20")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("2,20"));

    Ok(())
}

/// Test multiple CAST expressions
#[test]
fn test_cast_both_sides() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,code\n1,A\n2,B\n3,C\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    // Cast id to text for string comparison
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM data WHERE CAST(id AS TEXT) = '2'")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("2,B"));

    Ok(())
}

// ---------------------------------------------------------------------------
// SQLite CAST semantics. Expected values computed with SQLite 3.45.
// ---------------------------------------------------------------------------

mod sqlite_semantics {
    use crate::helpers::{assert_query_on, ONE};

    type R = Result<(), Box<dyn std::error::Error>>;

    /// CAST(text AS INTEGER) reads an optional sign and the digits after it;
    /// out-of-range values saturate.
    #[test]
    fn text_to_integer_uses_integer_prefix() -> R {
        assert_query_on(
            &[ONE],
            "SELECT CAST('3.9' AS INTEGER) AS a, CAST('12abc' AS INTEGER) AS b, \
             CAST(' 5 ' AS INTEGER) AS c, CAST('abc' AS INTEGER) AS d, \
             CAST('1e3' AS INTEGER) AS e, CAST('-3.9' AS INTEGER) AS f, \
             CAST('9999999999999999999' AS INTEGER) AS g, \
             CAST('-9999999999999999999' AS INTEGER) AS h FROM one",
            "a,b,c,d,e,f,g,h\n3,12,5,0,1,-3,9223372036854775807,-9223372036854775808",
        )
    }

    #[test]
    fn real_to_integer_truncates_and_saturates() -> R {
        assert_query_on(
            &[ONE],
            "SELECT CAST(3.9 AS INTEGER) AS a, CAST(-3.9 AS INTEGER) AS b, \
             CAST(1e30 AS INTEGER) AS c, CAST(-1e30 AS INTEGER) AS d FROM one",
            "a,b,c,d\n3,-3,9223372036854775807,-9223372036854775808",
        )
    }

    #[test]
    fn text_to_real_uses_numeric_prefix() -> R {
        assert_query_on(
            &[ONE],
            "SELECT CAST('12abc' AS REAL) AS a, CAST('abc' AS REAL) AS b, \
             CAST('1e3x' AS REAL) AS c, typeof(CAST(5 AS REAL)) AS d FROM one",
            "a,b,c,d\n12,0,1000,real",
        )
    }

    /// Any type name is mapped by SQLite's affinity rules (BIGINT is
    /// INTEGER, VARCHAR(10) is TEXT, DECIMAL/NUMERIC/BOOLEAN are NUMERIC).
    #[test]
    fn type_names_follow_affinity_rules() -> R {
        assert_query_on(
            &[ONE],
            "SELECT CAST('12' AS NUMERIC) AS a, typeof(CAST('12' AS NUMERIC)) AS b, \
             CAST('1.5' AS NUMERIC) AS c, CAST('12abc' AS NUMERIC) AS d, \
             typeof(CAST('3.0' AS NUMERIC)) AS e, CAST('7x' AS BIGINT) AS f, \
             typeof(CAST(5 AS VARCHAR(10))) AS g, CAST(1 AS BOOLEAN) AS h FROM one",
            "a,b,c,d,e,f,g,h\n12,integer,1.5,12,integer,7,text,1",
        )
    }
}
