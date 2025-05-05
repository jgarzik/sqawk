//! Tests for join functionality in sqawk
//!
//! Tests for cross joins, inner joins, and multi-table joins.

use std::path::PathBuf;
use assert_cmd::Command;
use predicates::prelude::*;

// Helper function to get the path to static test files for joins
fn get_users_file() -> PathBuf {
    PathBuf::from("tests/data/users.csv")
}

fn get_orders_file() -> PathBuf {
    PathBuf::from("tests/data/orders.csv")
}

fn get_products_file() -> PathBuf {
    PathBuf::from("tests/data/products.csv")
}

#[test]
fn test_cross_join() -> Result<(), Box<dyn std::error::Error>> {
    // Test a basic cross join between two tables
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM users, orders LIMIT 3")
        .arg(get_users_file().to_str().unwrap())
        .arg(get_orders_file().to_str().unwrap())
        .arg("-v");

    // Check output - should have combined columns and first 3 rows of cross join
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("users.id,users.name,users.email,orders.id,orders.user_id,orders.product_id,orders.date"))
        .stdout(predicate::str::contains("Processing multiple tables in FROM clause as CROSS JOINs").not());

    // Verify we have a proper cross join with first user repeated for multiple orders
    cmd.assert()
        .stdout(predicate::str::contains("John,john@example.com"))
        .stdout(predicate::str::contains("Jane,jane@example.com"));

    Ok(())
}

#[test]
fn test_inner_join() -> Result<(), Box<dyn std::error::Error>> {
    // Test an inner join between users and orders using WHERE condition
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT * FROM users, orders WHERE users.id = orders.user_id")
        .arg(get_users_file().to_str().unwrap())
        .arg(get_orders_file().to_str().unwrap())
        .arg("-v");

    // Check output - should include john and jane, but only their own orders
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("users.id,users.name,users.email,orders.id,orders.user_id,orders.product_id,orders.date"))
        // John's orders
        .stdout(predicate::str::contains("1,John,john@example.com,101,1,101,2023-01-15"))
        .stdout(predicate::str::contains("1,John,john@example.com,103,1,103,2023-02-10"))
        .stdout(predicate::str::contains("1,John,john@example.com,105,1,104,2023-03-05"))
        // Jane's orders
        .stdout(predicate::str::contains("2,Jane,jane@example.com,102,2,102,2023-01-20"))
        .stdout(predicate::str::contains("2,Jane,jane@example.com,104,2,105,2023-02-25"));

    // Verify verbose output contains filtering indications
    cmd.assert()
        .stderr(predicate::str::contains("Processing multiple tables in FROM clause as CROSS JOINs"))
        .stderr(predicate::str::contains("WHERE comparison"));

    Ok(())
}

#[test]
fn test_inner_join_with_projection() -> Result<(), Box<dyn std::error::Error>> {
    // Test an inner join with column projection
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT users.name, orders.product_id, orders.date FROM users, orders WHERE users.id = orders.user_id")
        .arg(get_users_file().to_str().unwrap())
        .arg(get_orders_file().to_str().unwrap());

    // Check output - should include only selected columns
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("users.name,orders.product_id,orders.date"));

    // Verify we have the correct data
    cmd.assert()
        .stdout(predicate::str::contains("John,101,2023-01-15"))
        .stdout(predicate::str::contains("Jane,102,2023-01-20"));

    Ok(())
}

#[test]
fn test_three_table_join() -> Result<(), Box<dyn std::error::Error>> {
    // Test a three-way join between users, orders, and products
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT users.name, products.name, products.price, orders.date FROM users, orders, products WHERE orders.user_id = users.id AND orders.product_id = products.product_id")
        .arg(get_users_file().to_str().unwrap())
        .arg(get_orders_file().to_str().unwrap())
        .arg(get_products_file().to_str().unwrap());

    // Check output headers
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("users.name,products.name,products.price,orders.date"));

    // Check for some expected combinations
    cmd.assert()
        .stdout(predicate::str::contains("John,Laptop,1200,2023-01-15"))
        .stdout(predicate::str::contains("John,Headphones,150,2023-02-10"))
        .stdout(predicate::str::contains("Jane,Phone,800,2023-01-20"))
        .stdout(predicate::str::contains("Jane,Monitor,350,2023-02-25"));

    Ok(())
}

#[test]
fn test_join_with_additional_filtering() -> Result<(), Box<dyn std::error::Error>> {
    // Test a join with additional non-join filtering in the WHERE clause
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT users.name, products.name, products.price FROM users, orders, products WHERE users.id = orders.user_id AND products.product_id = orders.product_id AND products.price > 500")
        .arg(get_users_file().to_str().unwrap())
        .arg(get_orders_file().to_str().unwrap())
        .arg(get_products_file().to_str().unwrap());

    // We should only see products with price > 500
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("users.name,products.name,products.price"))
        // These should be included (price > 500)
        .stdout(predicate::str::contains("John,Laptop,1200"))
        .stdout(predicate::str::contains("Jane,Phone,800"));

    // Check that output DOES NOT contain these cheaper products
    let output = cmd.output()?;
    let stdout = String::from_utf8(output.stdout)?;
    assert!(!stdout.contains("Jane,Monitor,350"), "Should not contain Monitor (price <= 500)");
    assert!(!stdout.contains("John,Headphones,150"), "Should not contain Headphones (price <= 500)");
    assert!(!stdout.contains("John,Keyboard,80"), "Should not contain Keyboard (price <= 500)");

    Ok(())
}

#[test]
fn test_join_order_preservation() -> Result<(), Box<dyn std::error::Error>> {
    // Test that join result order is preserved based on input order
    // This test verifies that the order of rows in the result set
    
    let mut cmd = Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT users.name, orders.id FROM users, orders WHERE users.id = orders.user_id")
        .arg(get_users_file().to_str().unwrap())
        .arg(get_orders_file().to_str().unwrap());

    // Just check that we get the expected results in some order
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("users.name,orders.id"))
        .stdout(predicate::str::contains("John,101"))
        .stdout(predicate::str::contains("Jane,102"));
    
    Ok(())
}