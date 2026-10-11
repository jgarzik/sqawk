//! LIKE and ILIKE pattern matching tests for sqawk VM
//!
//! This file contains tests for SQL LIKE and ILIKE pattern matching operations.

use crate::helpers::{create_custom_csv, create_temp_dir};

/// Test basic LIKE pattern with % wildcard (matches any sequence)
#[test]
fn test_like_percent_prefix() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name,city\n1,Alice,Atlanta\n2,Bob,Boston\n3,Charlie,Chicago\n4,David,Denver\n5,Alex,Austin\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    // Test LIKE with prefix pattern using VM
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM data WHERE name LIKE 'A%'")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,Alice,Atlanta"))
        .stdout(predicates::str::contains("5,Alex,Austin"));

    Ok(())
}

/// Test LIKE pattern with % suffix
#[test]
fn test_like_percent_suffix() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name,email\n1,Alice,alice@example.com\n2,Bob,bob@test.org\n3,Charlie,charlie@example.com\n";
    let file_path = create_custom_csv(temp_dir.path(), "users.csv", content)?;

    // Test LIKE with suffix pattern using VM
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM users WHERE email LIKE '%@example.com'")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,Alice,alice@example.com"))
        .stdout(predicates::str::contains("3,Charlie,charlie@example.com"));

    Ok(())
}

/// Test LIKE pattern with % on both sides (contains)
#[test]
fn test_like_percent_contains() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,description\n1,The quick brown fox\n2,A lazy dog\n3,Quick and easy\n4,Slow and steady\n";
    let file_path = create_custom_csv(temp_dir.path(), "items.csv", content)?;

    // Test LIKE with contains pattern using VM
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM items WHERE description LIKE '%and%'")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("3,Quick and easy"))
        .stdout(predicates::str::contains("4,Slow and steady"));

    Ok(())
}

/// Test LIKE pattern with _ wildcard (matches single character)
#[test]
fn test_like_underscore() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,code\n1,A1B\n2,A2B\n3,A12B\n4,A3B\n5,AAB\n";
    let file_path = create_custom_csv(temp_dir.path(), "codes.csv", content)?;

    // Test LIKE with underscore pattern using VM
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM codes WHERE code LIKE 'A_B'")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,A1B"))
        .stdout(predicates::str::contains("2,A2B"))
        .stdout(predicates::str::contains("4,A3B"));

    Ok(())
}

/// Test NOT LIKE pattern
#[test]
fn test_not_like() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name\n1,Alice\n2,Bob\n3,Alex\n4,Charlie\n";
    let file_path = create_custom_csv(temp_dir.path(), "people.csv", content)?;

    // Test NOT LIKE using VM
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM people WHERE name NOT LIKE 'A%'")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("2,Bob"))
        .stdout(predicates::str::contains("4,Charlie"));

    Ok(())
}

/// Test ILIKE case-insensitive pattern matching
#[test]
fn test_ilike_case_insensitive() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name\n1,ALICE\n2,alice\n3,Alice\n4,Bob\n5,ALEX\n";
    let file_path = create_custom_csv(temp_dir.path(), "names.csv", content)?;

    // Test ILIKE case-insensitive pattern using VM
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM names WHERE name ILIKE 'al%'")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,ALICE"))
        .stdout(predicates::str::contains("2,alice"))
        .stdout(predicates::str::contains("3,Alice"))
        .stdout(predicates::str::contains("5,ALEX"));

    Ok(())
}

/// Test exact match pattern (no wildcards)
#[test]
fn test_like_exact_match() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,status\n1,active\n2,inactive\n3,active\n4,pending\n";
    let file_path = create_custom_csv(temp_dir.path(), "records.csv", content)?;

    // Test LIKE with exact pattern using VM
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM records WHERE status LIKE 'active'")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,active"))
        .stdout(predicates::str::contains("3,active"));

    Ok(())
}

/// Test LIKE with special regex characters that need escaping
#[test]
fn test_like_special_chars() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,value\n1,test.csv\n2,test_csv\n3,test+csv\n4,testXcsv\n";
    let file_path = create_custom_csv(temp_dir.path(), "files.csv", content)?;

    // Test LIKE with literal dot using VM
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM files WHERE value LIKE 'test.csv'")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,test.csv"));

    Ok(())
}

// ---------------------------------------------------------------------------
// SQLite LIKE semantics. Expected values computed with SQLite 3.45.
// ---------------------------------------------------------------------------

mod sqlite_semantics {
    use crate::helpers::{assert_query_on, assert_query_on_fails, ONE};

    type R = Result<(), Box<dyn std::error::Error>>;

    const LK: (&str, &str) = (
        "lk.csv",
        "k,s,p\n1,Alice,a%\n2,alice,_LICE\n3,Bob,%o%\n4,x%y,x\\%y\n5,,a%\n",
    );
    const U: (&str, &str) = ("u.csv", "id,w\n1,héllo\n2,日本語\n3,ABC\n4,a_b\n5,x%y\n");

    /// LIKE folds ASCII case only.
    #[test]
    fn like_is_ascii_case_insensitive() -> R {
        assert_query_on(&[LK], "SELECT k FROM lk WHERE s LIKE 'a%'", "k\n1\n2")?;
        assert_query_on(&[U], "SELECT id FROM u WHERE w LIKE 'H%'", "id\n1")
    }

    /// The pattern may be a column; a NULL operand gives NULL.
    #[test]
    fn like_with_column_pattern() -> R {
        assert_query_on(&[LK], "SELECT k FROM lk WHERE s LIKE p", "k\n1\n2\n3")?;
        assert_query_on(
            &[LK],
            "SELECT k, s LIKE p AS m FROM lk",
            "k,m\n1,1\n2,1\n3,1\n4,0\n5,",
        )?;
        assert_query_on(
            &[LK],
            "SELECT k FROM lk WHERE s LIKE p ESCAPE '\\'",
            "k\n1\n2\n3\n4",
        )
    }

    #[test]
    fn not_like_excludes_null() -> R {
        assert_query_on(
            &[LK],
            "SELECT k FROM lk WHERE s NOT LIKE '%o%'",
            "k\n1\n2\n4",
        )
    }

    /// `_` is one character (not one byte), and matches a newline too.
    #[test]
    fn underscore_is_one_character() -> R {
        assert_query_on(&[U], "SELECT id FROM u WHERE w LIKE '__'", "id")?;
        assert_query_on(
            &[U],
            "SELECT id FROM u WHERE w LIKE '___'",
            "id\n2\n3\n4\n5",
        )?;
        assert_query_on(
            &[U],
            "SELECT id FROM u WHERE w LIKE '%\\_%' ESCAPE '\\'",
            "id\n4",
        )
    }

    #[test]
    fn like_edge_cases() -> R {
        assert_query_on(
            &[ONE],
            "SELECT 'a' || char(10) || 'b' LIKE 'a_b' AS a, 'é' LIKE 'É' AS b, \
             123 LIKE '1%' AS c, 'abc' LIKE 'a%' ESCAPE NULL AS d FROM one",
            "a,b,c,d\n1,0,1,",
        )
    }

    /// ILIKE (not in SQLite) folds Unicode case.
    #[test]
    fn ilike_folds_unicode() -> R {
        assert_query_on(&[ONE], "SELECT 'é' ILIKE 'É' AS a FROM one", "a\n1")
    }

    #[test]
    fn escape_must_be_one_character() -> R {
        assert_query_on_fails(
            &[ONE],
            "SELECT 'a' LIKE 'a' ESCAPE 'xy' FROM one",
            "ESCAPE expression must be a single character",
        )
    }
}
