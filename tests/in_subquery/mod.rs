//! IN (SELECT ...) subquery tests for sqawk VM
//!
//! This file contains tests for IN subqueries.

use crate::helpers::{create_custom_csv, create_temp_dir};

/// Test basic IN subquery
#[test]
fn test_in_subquery_basic() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;

    // Create orders table
    let orders_content = "order_id,user_id,amount\n1,1,100\n2,2,200\n3,1,150\n4,3,300\n5,4,250\n";
    let orders_path = create_custom_csv(temp_dir.path(), "orders.csv", orders_content)?;

    // Create users table
    let users_content = "id,name,active\n1,Alice,1\n2,Bob,0\n3,Charlie,1\n4,David,1\n";
    let users_path = create_custom_csv(temp_dir.path(), "users.csv", users_content)?;

    // Test: SELECT * FROM orders WHERE user_id IN (SELECT id FROM users)
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM orders WHERE user_id IN (SELECT id FROM users)")
        .arg(orders_path.to_str().unwrap())
        .arg(users_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,1,100"))
        .stdout(predicates::str::contains("2,2,200"))
        .stdout(predicates::str::contains("3,1,150"))
        .stdout(predicates::str::contains("4,3,300"))
        .stdout(predicates::str::contains("5,4,250"));

    Ok(())
}

/// Test IN subquery with WHERE clause in subquery
#[test]
fn test_in_subquery_with_where() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;

    // Create orders table
    let orders_content = "order_id,user_id,amount\n1,1,100\n2,2,200\n3,1,150\n4,3,300\n5,4,250\n";
    let orders_path = create_custom_csv(temp_dir.path(), "orders.csv", orders_content)?;

    // Create users table - only active users have active=1
    let users_content = "id,name,active\n1,Alice,1\n2,Bob,0\n3,Charlie,1\n4,David,0\n";
    let users_path = create_custom_csv(temp_dir.path(), "users.csv", users_content)?;

    // Test: SELECT * FROM orders WHERE user_id IN (SELECT id FROM users WHERE active = 1)
    // Only users 1 (Alice) and 3 (Charlie) are active
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM orders WHERE user_id IN (SELECT id FROM users WHERE active = 1)")
        .arg(orders_path.to_str().unwrap())
        .arg(users_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,1,100"))
        .stdout(predicates::str::contains("3,1,150"))
        .stdout(predicates::str::contains("4,3,300"));

    Ok(())
}

/// Test NOT IN subquery
#[test]
fn test_not_in_subquery() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;

    // Create orders table
    let orders_content = "order_id,user_id,amount\n1,1,100\n2,2,200\n3,5,150\n4,3,300\n5,6,250\n";
    let orders_path = create_custom_csv(temp_dir.path(), "orders.csv", orders_content)?;

    // Create users table
    let users_content = "id,name\n1,Alice\n2,Bob\n3,Charlie\n4,David\n";
    let users_path = create_custom_csv(temp_dir.path(), "users.csv", users_content)?;

    // Test: SELECT * FROM orders WHERE user_id NOT IN (SELECT id FROM users)
    // Users 5 and 6 are not in the users table
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM orders WHERE user_id NOT IN (SELECT id FROM users)")
        .arg(orders_path.to_str().unwrap())
        .arg(users_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("3,5,150"))
        .stdout(predicates::str::contains("5,6,250"));

    Ok(())
}

/// Test IN subquery with empty subquery result
#[test]
fn test_in_subquery_empty_result() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;

    let data_content = "id,value\n1,10\n2,20\n3,30\n";
    let data_path = create_custom_csv(temp_dir.path(), "data.csv", data_content)?;

    let empty_content = "id,value\n1,100\n2,200\n";
    let empty_path = create_custom_csv(temp_dir.path(), "empty.csv", empty_content)?;

    // Test: SELECT * FROM data WHERE value IN (SELECT value FROM empty WHERE value > 1000)
    // Subquery returns no rows
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM data WHERE value IN (SELECT value FROM empty WHERE value > 1000)")
        .arg(data_path.to_str().unwrap())
        .arg(empty_path.to_str().unwrap());

    // No matches expected when subquery is empty
    cmd.assert().success();

    Ok(())
}

/// Test IN subquery on same table (self-referential)
#[test]
fn test_in_subquery_same_table() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;

    let content = "id,name,manager_id\n1,CEO,0\n2,Alice,1\n3,Bob,1\n4,Charlie,2\n5,David,3\n";
    let file_path = create_custom_csv(temp_dir.path(), "employees.csv", content)?;

    // Test: Find all employees who are managers
    // SELECT * FROM employees WHERE id IN (SELECT manager_id FROM employees)
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM employees WHERE id IN (SELECT manager_id FROM employees)")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,CEO,0"))
        .stdout(predicates::str::contains("2,Alice,1"))
        .stdout(predicates::str::contains("3,Bob,1"));

    Ok(())
}

/// Test IN subquery with string values
#[test]
fn test_in_subquery_strings() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;

    let products_content = "id,name,category\n1,Laptop,Electronics\n2,Mouse,Electronics\n3,Desk,Furniture\n4,Chair,Furniture\n5,Book,Other\n";
    let products_path = create_custom_csv(temp_dir.path(), "products.csv", products_content)?;

    let categories_content = "name,active\nElectronics,1\nFurniture,0\n";
    let categories_path = create_custom_csv(temp_dir.path(), "categories.csv", categories_content)?;

    // Test: SELECT * FROM products WHERE category IN (SELECT name FROM categories WHERE active = 1)
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM products WHERE category IN (SELECT name FROM categories WHERE active = 1)")
        .arg(products_path.to_str().unwrap())
        .arg(categories_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,Laptop,Electronics"))
        .stdout(predicates::str::contains("2,Mouse,Electronics"));

    Ok(())
}
