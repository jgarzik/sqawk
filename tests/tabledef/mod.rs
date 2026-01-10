//! Tests for --tabledef CLI option
//!
//! This file contains tests for defining column names for headerless files
//! using the --tabledef command-line option.

use crate::helpers::{create_custom_csv, create_temp_dir};

#[test]
fn test_tabledef_basic() -> Result<(), Box<dyn std::error::Error>> {
    // Test basic --tabledef usage with a headerless file
    let temp_dir = create_temp_dir()?;

    // Create a headerless file (no header row)
    let data_file = create_custom_csv(
        temp_dir.path(),
        "data.csv",
        "1,Alice,100\n2,Bob,200\n3,Charlie,300\n",
    )?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("--tabledef=data:id,name,value")
        .arg("-s")
        .arg("SELECT name, value FROM data WHERE value > 150")
        .arg(data_file.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("name,value"))
        .stdout(predicates::str::contains("Bob"))
        .stdout(predicates::str::contains("Charlie"));

    Ok(())
}

#[test]
fn test_tabledef_with_custom_delimiter() -> Result<(), Box<dyn std::error::Error>> {
    // Test --tabledef with colon-delimited file (like /etc/passwd)
    let temp_dir = create_temp_dir()?;

    // Create a passwd-like file
    let data_file = create_custom_csv(
        temp_dir.path(),
        "passwd",
        "root:x:0:0:root:/root:/bin/bash\nnobody:x:65534:65534:nobody:/nonexistent:/usr/sbin/nologin\nuser:x:1000:1000:User:/home/user:/bin/bash\n",
    )?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-F:")
        .arg("--tabledef=passwd:username,password,uid,gid,gecos,home,shell")
        .arg("-s")
        .arg("SELECT username, home FROM passwd WHERE uid >= 1000")
        .arg(data_file.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("user"))
        .stdout(predicates::str::contains("/home/user"));

    Ok(())
}

#[test]
fn test_tabledef_multiple_tables() -> Result<(), Box<dyn std::error::Error>> {
    // Test multiple --tabledef options for different tables
    let temp_dir = create_temp_dir()?;

    let users_file = create_custom_csv(temp_dir.path(), "users.csv", "1,Alice\n2,Bob\n")?;

    let orders_file = create_custom_csv(
        temp_dir.path(),
        "orders.csv",
        "101,1,500\n102,2,300\n103,1,200\n",
    )?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("--tabledef=users:id,name")
        .arg("--tabledef=orders:order_id,user_id,amount")
        .arg("-s")
        .arg("SELECT users.name, orders.amount FROM users, orders WHERE users.id = orders.user_id")
        .arg(users_file.to_str().unwrap())
        .arg(orders_file.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("Alice"))
        .stdout(predicates::str::contains("500"));

    Ok(())
}

#[test]
fn test_tabledef_with_table_alias() -> Result<(), Box<dyn std::error::Error>> {
    // Test --tabledef with explicit table name assignment
    let temp_dir = create_temp_dir()?;

    let data_file = create_custom_csv(
        temp_dir.path(),
        "raw_data.csv",
        "1,Apple,1.50\n2,Banana,0.75\n",
    )?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("--tabledef=products:id,name,price")
        .arg("-s")
        .arg("SELECT name, price FROM products")
        .arg(format!("products={}", data_file.to_str().unwrap()));

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("Apple"))
        .stdout(predicates::str::contains("1.5"));

    Ok(())
}

#[test]
fn test_tabledef_aggregate_query() -> Result<(), Box<dyn std::error::Error>> {
    // Test --tabledef with aggregate functions
    let temp_dir = create_temp_dir()?;

    let data_file = create_custom_csv(
        temp_dir.path(),
        "sales.csv",
        "2024-01,North,1000\n2024-01,South,1500\n2024-02,North,1200\n2024-02,South,1800\n",
    )?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("--tabledef=sales:month,region,amount")
        .arg("-s")
        .arg("SELECT region, SUM(amount) as total FROM sales GROUP BY region")
        .arg(data_file.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("North"))
        .stdout(predicates::str::contains("South"));

    Ok(())
}

#[test]
fn test_tabledef_with_write() -> Result<(), Box<dyn std::error::Error>> {
    // Test --tabledef with --write flag
    let temp_dir = create_temp_dir()?;

    let data_file = create_custom_csv(temp_dir.path(), "data.csv", "1,Alice,100\n2,Bob,200\n")?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("--tabledef=data:id,name,value")
        .arg("-s")
        .arg("UPDATE data SET value = 999 WHERE name = 'Alice'")
        .arg("--write")
        .arg(data_file.to_str().unwrap());

    cmd.assert().success();

    // Verify the file was updated
    let contents = std::fs::read_to_string(&data_file)?;
    assert!(
        contents.contains("999"),
        "File should contain updated value"
    );

    Ok(())
}
