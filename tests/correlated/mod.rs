//! Correlated subquery tests for sqawk VM
//!
//! This file contains tests for correlated subqueries.

use crate::helpers::{create_custom_csv, create_temp_dir};

/// Test correlated EXISTS subquery - users with orders
#[test]
fn test_correlated_exists_users_with_orders() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;

    let users_content = "id,name\n1,Alice\n2,Bob\n3,Charlie\n";
    let users_path = create_custom_csv(temp_dir.path(), "users.csv", users_content)?;

    let orders_content = "order_id,user_id,amount\n1,1,100\n2,1,150\n3,2,200\n";
    let orders_path = create_custom_csv(temp_dir.path(), "orders.csv", orders_content)?;

    // Test: SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)
    // Alice (id=1) and Bob (id=2) have orders, Charlie (id=3) does not
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)")
        .arg(users_path.to_str().unwrap())
        .arg(orders_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,Alice"))
        .stdout(predicates::str::contains("2,Bob"));

    // Verify Charlie is not in output
    let output = cmd.output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        !stdout.contains("Charlie"),
        "Charlie should not be in output: {}",
        stdout
    );

    Ok(())
}

/// Test correlated NOT EXISTS subquery - users without orders
#[test]
fn test_correlated_not_exists_users_without_orders() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;

    let users_content = "id,name\n1,Alice\n2,Bob\n3,Charlie\n";
    let users_path = create_custom_csv(temp_dir.path(), "users.csv", users_content)?;

    let orders_content = "order_id,user_id,amount\n1,1,100\n2,1,150\n";
    let orders_path = create_custom_csv(temp_dir.path(), "orders.csv", orders_content)?;

    // Test: SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)
    // Only Alice (id=1) has orders, so Bob and Charlie should be returned
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)")
        .arg(users_path.to_str().unwrap())
        .arg(orders_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("2,Bob"))
        .stdout(predicates::str::contains("3,Charlie"));

    // Verify Alice is not in output
    let output = cmd.output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        !stdout.contains("Alice"),
        "Alice should not be in output: {}",
        stdout
    );

    Ok(())
}

/// Test correlated EXISTS with additional filter condition
#[test]
fn test_correlated_exists_with_filter() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;

    let users_content = "id,name\n1,Alice\n2,Bob\n3,Charlie\n";
    let users_path = create_custom_csv(temp_dir.path(), "users.csv", users_content)?;

    let orders_content = "order_id,user_id,amount\n1,1,50\n2,1,150\n3,2,75\n";
    let orders_path = create_custom_csv(temp_dir.path(), "orders.csv", orders_content)?;

    // Test: SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id AND amount > 100)
    // Only Alice (id=1) has an order with amount > 100 (150)
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id AND amount > 100)")
        .arg(users_path.to_str().unwrap())
        .arg(orders_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,Alice"));

    // Verify Bob and Charlie are not in output
    let output = cmd.output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        !stdout.contains("Bob"),
        "Bob should not be in output: {}",
        stdout
    );
    assert!(
        !stdout.contains("Charlie"),
        "Charlie should not be in output: {}",
        stdout
    );

    Ok(())
}

/// Test correlated scalar subquery with MAX
#[test]
fn test_correlated_scalar_subquery_max() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;

    // Each employee has a department, find employees with max salary in their department
    let employees_content = "id,name,dept,salary\n1,Alice,eng,100\n2,Bob,eng,120\n3,Charlie,sales,90\n4,Diana,sales,110\n";
    let employees_path = create_custom_csv(temp_dir.path(), "employees.csv", employees_content)?;

    // Test: SELECT * FROM employees e WHERE salary = (SELECT MAX(salary) FROM employees WHERE dept = e.dept)
    // Bob (120) is max in eng, Diana (110) is max in sales
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM employees e WHERE salary = (SELECT MAX(salary) FROM employees WHERE dept = e.dept)")
        .arg(employees_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("Bob"))
        .stdout(predicates::str::contains("Diana"));

    // Verify Alice and Charlie are not in output
    let output = cmd.output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        !stdout.contains("Alice"),
        "Alice should not be in output: {}",
        stdout
    );
    assert!(
        !stdout.contains("Charlie"),
        "Charlie should not be in output: {}",
        stdout
    );

    Ok(())
}

/// Test correlated scalar subquery with COUNT
#[test]
fn test_correlated_scalar_subquery_count() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;

    let users_content = "id,name\n1,Alice\n2,Bob\n3,Charlie\n";
    let users_path = create_custom_csv(temp_dir.path(), "users.csv", users_content)?;

    // Alice has 3 orders, Bob has 1, Charlie has 0
    let orders_content = "order_id,user_id\n1,1\n2,1\n3,1\n4,2\n";
    let orders_path = create_custom_csv(temp_dir.path(), "orders.csv", orders_content)?;

    // Test: SELECT * FROM users WHERE (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id) > 1
    // Only Alice has more than 1 order
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM users WHERE (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id) > 1")
        .arg(users_path.to_str().unwrap())
        .arg(orders_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("1,Alice"));

    // Verify Bob and Charlie are not in output
    let output = cmd.output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        !stdout.contains("Bob"),
        "Bob should not be in output: {}",
        stdout
    );
    assert!(
        !stdout.contains("Charlie"),
        "Charlie should not be in output: {}",
        stdout
    );

    Ok(())
}

/// Test correlated EXISTS with same table (self-referential)
#[test]
fn test_correlated_exists_same_table() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;

    // Employees with manager_id referencing other employees
    let employees_content = "id,name,manager_id\n1,Alice,0\n2,Bob,1\n3,Charlie,1\n4,Diana,2\n";
    let employees_path = create_custom_csv(temp_dir.path(), "employees.csv", employees_content)?;

    // Test: Find managers (employees who manage someone)
    // SELECT * FROM employees e WHERE EXISTS (SELECT 1 FROM employees WHERE manager_id = e.id)
    // Alice (id=1) manages Bob and Charlie, Bob (id=2) manages Diana
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT * FROM employees e WHERE EXISTS (SELECT 1 FROM employees WHERE manager_id = e.id)")
        .arg(employees_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("Alice"))
        .stdout(predicates::str::contains("Bob"));

    // Verify Charlie and Diana are not in output (they don't manage anyone)
    let output = cmd.output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        !stdout.contains("Charlie"),
        "Charlie should not be in output: {}",
        stdout
    );
    assert!(
        !stdout.contains("Diana"),
        "Diana should not be in output: {}",
        stdout
    );

    Ok(())
}
