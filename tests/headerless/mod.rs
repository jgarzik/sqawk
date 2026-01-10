//! Tests for headerless file auto-detection
//!
//! This file contains tests for automatic detection of files without headers
//! and the automatic generation of column names (a, b, c, ...).

use crate::helpers::{create_custom_csv, create_temp_dir};

#[test]
fn test_headerless_auto_detect_numeric_first_row() -> Result<(), Box<dyn std::error::Error>> {
    // Test auto-detection when first row contains numeric values
    let temp_dir = create_temp_dir()?;

    // First row has numbers - should be detected as data, not headers
    let data_file = create_custom_csv(
        temp_dir.path(),
        "numbers.csv",
        "1,100,200\n2,150,250\n3,200,300\n",
    )?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT a, b, c FROM numbers")
        .arg(data_file.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("a,b,c"))
        .stdout(predicates::str::contains("1,100,200"));

    Ok(())
}

#[test]
fn test_headerless_passwd_style() -> Result<(), Box<dyn std::error::Error>> {
    // Test auto-detection for /etc/passwd style files
    let temp_dir = create_temp_dir()?;

    // passwd-style: starts with "root" which triggers headerless detection
    let data_file = create_custom_csv(
        temp_dir.path(),
        "passwd",
        "root:x:0:0:root:/root:/bin/bash\nnobody:x:65534:65534:nobody:/nonexistent:/usr/sbin/nologin\n",
    )?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-F:")
        .arg("-s")
        .arg("SELECT a, c, f FROM passwd")
        .arg(data_file.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("root"))
        .stdout(predicates::str::contains("/root"));

    Ok(())
}

#[test]
fn test_headerless_path_in_first_row() -> Result<(), Box<dyn std::error::Error>> {
    // Test auto-detection when first row contains a path
    let temp_dir = create_temp_dir()?;

    // First field starts with "/" - should be detected as data
    let data_file = create_custom_csv(
        temp_dir.path(),
        "paths.csv",
        "/usr/bin,executable,1000\n/etc/config,config,500\n",
    )?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT a, b FROM paths")
        .arg(data_file.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("/usr/bin"))
        .stdout(predicates::str::contains("executable"));

    Ok(())
}

#[test]
fn test_headerless_alphabetic_columns() -> Result<(), Box<dyn std::error::Error>> {
    // Test that column names follow a, b, c, ... pattern
    let temp_dir = create_temp_dir()?;

    let data_file = create_custom_csv(temp_dir.path(), "data.csv", "1,2,3,4,5\n6,7,8,9,10\n")?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT a, b, c, d, e FROM data")
        .arg(data_file.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("a,b,c,d,e"))
        .stdout(predicates::str::contains("1,2,3,4,5"));

    Ok(())
}

#[test]
fn test_headerless_filter_query() -> Result<(), Box<dyn std::error::Error>> {
    // Test filtering on auto-generated column names
    let temp_dir = create_temp_dir()?;

    let data_file = create_custom_csv(
        temp_dir.path(),
        "data.csv",
        "1,Alice,100\n2,Bob,200\n3,Charlie,50\n",
    )?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT b FROM data WHERE c > 75")
        .arg(data_file.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("Alice"))
        .stdout(predicates::str::contains("Bob"));

    Ok(())
}

#[test]
fn test_headerless_aggregate() -> Result<(), Box<dyn std::error::Error>> {
    // Test aggregation with auto-generated column names
    let temp_dir = create_temp_dir()?;

    let data_file = create_custom_csv(
        temp_dir.path(),
        "data.csv",
        "1,North,100\n2,South,200\n3,North,150\n4,South,250\n",
    )?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT b, SUM(c) FROM data GROUP BY b")
        .arg(data_file.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("North"))
        .stdout(predicates::str::contains("South"));

    Ok(())
}

#[test]
fn test_headerless_with_comments() -> Result<(), Box<dyn std::error::Error>> {
    // Test headerless file with comment lines
    let temp_dir = create_temp_dir()?;

    let data_file = create_custom_csv(
        temp_dir.path(),
        "data.csv",
        "# This is a comment\n1,Alice,100\n# Another comment\n2,Bob,200\n",
    )?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM data")
        .arg(data_file.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("Alice"))
        .stdout(predicates::str::contains("Bob"));

    Ok(())
}

#[test]
fn test_headerless_asterisk_field() -> Result<(), Box<dyn std::error::Error>> {
    // Test auto-detection when field contains asterisk (like passwd x field)
    let temp_dir = create_temp_dir()?;

    let data_file = create_custom_csv(temp_dir.path(), "data.csv", "user1,*,1001\nuser2,*,1002\n")?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT a, c FROM data")
        .arg(data_file.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("user1"))
        .stdout(predicates::str::contains("1001"));

    Ok(())
}
