//! EXISTS/NOT EXISTS subquery tests for sqawk VM
//!
//! This file contains tests for EXISTS subqueries.
//! Phase 4F: Subquery support

use crate::helpers::{create_custom_csv, create_temp_dir};

/// Test basic EXISTS subquery - table has rows
#[test]
fn test_exists_subquery_has_rows() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;

    let users_content = "id,name\n1,Alice\n2,Bob\n3,Charlie\n";
    let users_path = create_custom_csv(temp_dir.path(), "users.csv", users_content)?;

    let orders_content = "order_id,user_id,amount\n1,1,100\n2,2,200\n";
    let orders_path = create_custom_csv(temp_dir.path(), "orders.csv", orders_content)?;

    // Test: SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders)
    // Since orders table has rows, all users should be returned
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders)")
        .arg(users_path.to_str().unwrap())
        .arg(orders_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,Alice"))
        .stdout(predicates::str::contains("2,Bob"))
        .stdout(predicates::str::contains("3,Charlie"));

    Ok(())
}

/// Test EXISTS subquery with WHERE filter - matching rows
#[test]
fn test_exists_subquery_with_where_match() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;

    let users_content = "id,name\n1,Alice\n2,Bob\n3,Charlie\n";
    let users_path = create_custom_csv(temp_dir.path(), "users.csv", users_content)?;

    let orders_content = "order_id,user_id,amount\n1,1,100\n2,2,200\n";
    let orders_path = create_custom_csv(temp_dir.path(), "orders.csv", orders_content)?;

    // Test: SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE amount > 150)
    // order 2 has amount=200 > 150, so EXISTS is true
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE amount > 150)")
        .arg(users_path.to_str().unwrap())
        .arg(orders_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,Alice"))
        .stdout(predicates::str::contains("2,Bob"))
        .stdout(predicates::str::contains("3,Charlie"));

    Ok(())
}

/// Test EXISTS subquery with WHERE filter - no matching rows
#[test]
fn test_exists_subquery_with_where_no_match() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;

    let users_content = "id,name\n1,Alice\n2,Bob\n3,Charlie\n";
    let users_path = create_custom_csv(temp_dir.path(), "users.csv", users_content)?;

    let orders_content = "order_id,user_id,amount\n1,1,100\n2,2,200\n";
    let orders_path = create_custom_csv(temp_dir.path(), "orders.csv", orders_content)?;

    // Test: SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE amount > 500)
    // No orders with amount > 500, so EXISTS is false - no users returned
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE amount > 500)")
        .arg(users_path.to_str().unwrap())
        .arg(orders_path.to_str().unwrap());

    // No results expected
    cmd.assert().success();

    Ok(())
}

/// Test NOT EXISTS subquery - table has rows
#[test]
fn test_not_exists_subquery_has_rows() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;

    let users_content = "id,name\n1,Alice\n2,Bob\n3,Charlie\n";
    let users_path = create_custom_csv(temp_dir.path(), "users.csv", users_content)?;

    let orders_content = "order_id,user_id,amount\n1,1,100\n2,2,200\n";
    let orders_path = create_custom_csv(temp_dir.path(), "orders.csv", orders_content)?;

    // Test: SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM orders)
    // Since orders table has rows, NOT EXISTS is false - no users returned
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM orders)")
        .arg(users_path.to_str().unwrap())
        .arg(orders_path.to_str().unwrap());

    // No results expected
    cmd.assert().success();

    Ok(())
}

/// Test NOT EXISTS subquery - no matching rows
#[test]
fn test_not_exists_subquery_no_match() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;

    let users_content = "id,name\n1,Alice\n2,Bob\n3,Charlie\n";
    let users_path = create_custom_csv(temp_dir.path(), "users.csv", users_content)?;

    let orders_content = "order_id,user_id,amount\n1,1,100\n2,2,200\n";
    let orders_path = create_custom_csv(temp_dir.path(), "orders.csv", orders_content)?;

    // Test: SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE amount > 1000)
    // No orders with amount > 1000, so NOT EXISTS is true - all users returned
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE amount > 1000)")
        .arg(users_path.to_str().unwrap())
        .arg(orders_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,Alice"))
        .stdout(predicates::str::contains("2,Bob"))
        .stdout(predicates::str::contains("3,Charlie"));

    Ok(())
}

/// Test EXISTS with single-row table
#[test]
fn test_exists_single_row() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;

    let data_content = "id,value\n1,100\n2,200\n3,300\n";
    let data_path = create_custom_csv(temp_dir.path(), "data.csv", data_content)?;

    let single_content = "flag\n1\n";
    let single_path = create_custom_csv(temp_dir.path(), "single.csv", single_content)?;

    // Test: SELECT * FROM data WHERE EXISTS (SELECT 1 FROM single)
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM data WHERE EXISTS (SELECT 1 FROM single)")
        .arg(data_path.to_str().unwrap())
        .arg(single_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,100"))
        .stdout(predicates::str::contains("2,200"))
        .stdout(predicates::str::contains("3,300"));

    Ok(())
}

/// Test EXISTS combined with other WHERE conditions
#[test]
fn test_exists_with_other_conditions() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;

    let users_content = "id,name,age\n1,Alice,25\n2,Bob,30\n3,Charlie,35\n";
    let users_path = create_custom_csv(temp_dir.path(), "users.csv", users_content)?;

    let orders_content = "order_id,user_id,amount\n1,1,100\n";
    let orders_path = create_custom_csv(temp_dir.path(), "orders.csv", orders_content)?;

    // Test: SELECT * FROM users WHERE age > 25 AND EXISTS (SELECT 1 FROM orders)
    // EXISTS is true, but we also filter by age > 25
    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM users WHERE age > 25 AND EXISTS (SELECT 1 FROM orders)")
        .arg(users_path.to_str().unwrap())
        .arg(orders_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("2,Bob,30"))
        .stdout(predicates::str::contains("3,Charlie,35"));

    Ok(())
}
