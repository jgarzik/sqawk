//! Tests for headerless files (`--no-header`)
//!
//! The first line of every file is its header unless `--no-header` says the
//! file has none (or `--tabledef` names the columns); there is no guessing.
//! Headerless columns are named a, b, c, ...

use crate::helpers::{create_custom_csv, create_temp_dir};

#[test]
fn test_headerless_auto_detect_numeric_first_row() -> Result<(), Box<dyn std::error::Error>> {
    // A numeric first row is data under --no-header
    let temp_dir = create_temp_dir()?;

    let data_file = create_custom_csv(
        temp_dir.path(),
        "numbers.txt",
        "1,100,200\n2,150,250\n3,200,300\n",
    )?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-F,")
        .arg("--no-header")
        .arg("-s")
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
    // /etc/passwd style files: -F: --no-header
    let temp_dir = create_temp_dir()?;

    let data_file = create_custom_csv(
        temp_dir.path(),
        "passwd",
        "root:x:0:0:root:/root:/bin/bash\nnobody:x:65534:65534:nobody:/nonexistent:/usr/sbin/nologin\n",
    )?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-F:")
        .arg("--no-header")
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
    // A first row holding a path is data under --no-header
    let temp_dir = create_temp_dir()?;

    let data_file = create_custom_csv(
        temp_dir.path(),
        "paths.txt",
        "/usr/bin,executable,1000\n/etc/config,config,500\n",
    )?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-F,")
        .arg("--no-header")
        .arg("-s")
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

    let data_file = create_custom_csv(temp_dir.path(), "data.txt", "1,2,3,4,5\n6,7,8,9,10\n")?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-F,")
        .arg("--no-header")
        .arg("-s")
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
        "data.txt",
        "1,Alice,100\n2,Bob,200\n3,Charlie,50\n",
    )?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-F,")
        .arg("--no-header")
        .arg("-s")
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
        "data.txt",
        "1,North,100\n2,South,200\n3,North,150\n4,South,250\n",
    )?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-F,")
        .arg("--no-header")
        .arg("-s")
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
    // Headerless file with comment lines (--comments)
    let temp_dir = create_temp_dir()?;

    let data_file = create_custom_csv(
        temp_dir.path(),
        "data.txt",
        "# This is a comment\n1,Alice,100\n# Another comment\n2,Bob,200\n",
    )?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-F,")
        .arg("--no-header")
        .arg("--comments")
        .arg("-s")
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
    // A field holding an asterisk (like the passwd x field)
    let temp_dir = create_temp_dir()?;

    let data_file = create_custom_csv(temp_dir.path(), "data.txt", "user1,*,1001\nuser2,*,1002\n")?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-F,")
        .arg("--no-header")
        .arg("-s")
        .arg("SELECT a, c FROM data")
        .arg(data_file.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("user1"))
        .stdout(predicates::str::contains("1001"));

    Ok(())
}

#[test]
fn test_no_header_flag_on_csv() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let data_file = create_custom_csv(temp_dir.path(), "data.csv", "x,1\ny,2\n")?;
    let (stdout, stderr, ok) = crate::helpers::run_sql(
        "SELECT a, b FROM data",
        &[data_file.to_str().unwrap()],
        &["--no-header"],
    )?;
    assert!(ok, "{stderr}");
    assert_eq!(stdout, "a,b\nx,1\ny,2\n");
    Ok(())
}

#[test]
fn test_no_header_writeback_adds_no_header() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let data_file = create_custom_csv(temp_dir.path(), "data.csv", "x,1\ny,2\n")?;
    let (_stdout, stderr, ok) = crate::helpers::run_sql(
        "UPDATE data SET b = 3 WHERE a = 'y'",
        &[data_file.to_str().unwrap()],
        &["--no-header", "--write"],
    )?;
    assert!(ok, "{stderr}");
    assert_eq!(std::fs::read_to_string(&data_file)?, "x,1\ny,3\n");
    Ok(())
}

#[test]
fn test_colon_no_header_writeback_adds_no_header() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let data_file = create_custom_csv(
        temp_dir.path(),
        "passwd",
        "root:x:0:0:root:/root:/bin/bash\nuser:x:1000:1000:User:/home/user:/bin/sh\n",
    )?;
    let (_stdout, stderr, ok) = crate::helpers::run_sql(
        "UPDATE passwd SET g = '/bin/bash' WHERE a = 'user'",
        &[data_file.to_str().unwrap()],
        &["-F:", "--no-header", "--write"],
    )?;
    assert!(ok, "{stderr}");
    assert_eq!(
        std::fs::read_to_string(&data_file)?,
        "root:x:0:0:root:/root:/bin/bash\nuser:x:1000:1000:User:/home/user:/bin/bash\n"
    );
    Ok(())
}
