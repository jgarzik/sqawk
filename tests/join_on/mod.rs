//! Tests for JOIN ON syntax in sqawk
//!
//! Tests for SQL JOIN with the ON clause, as opposed to using WHERE for join conditions.

use predicates::prelude::*;

use crate::helpers::{get_orders_file, get_products_file, get_users_file};

#[test]
fn test_inner_join_on_basic() -> Result<(), Box<dyn std::error::Error>> {
    // Test a basic INNER JOIN with ON clause
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT users.name, orders.product_id, orders.date FROM users INNER JOIN orders ON users.id = orders.user_id")
        .arg(get_users_file().to_str().unwrap())
        .arg(get_orders_file().to_str().unwrap())
        .arg("-v");

    // Check output
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("name,product_id,date"));

    // Verify join results
    cmd.assert()
        .stdout(predicate::str::contains("John,101,2023-01-15"))
        .stdout(predicate::str::contains("Jane,102,2023-01-20"))
        .stdout(predicate::str::contains("John,103,2023-02-10"))
        .stdout(predicate::str::contains("Jane,105,2023-02-25"))
        .stdout(predicate::str::contains("John,104,2023-03-05"));

    // Verify verbose output shows JOIN processing
    cmd.assert()
        .stderr(predicate::str::contains("Join type: Inner"))
        .stderr(predicate::str::contains(
            "Processing INNER JOIN with ON condition",
        ));

    // Michael has no orders, should not appear
    let output = cmd.output()?;
    let stdout = String::from_utf8(output.stdout)?;
    assert!(
        !stdout.contains("Michael"),
        "Michael should not appear in the results (no matching orders)"
    );

    Ok(())
}

#[test]
fn test_inner_join_on_with_where() -> Result<(), Box<dyn std::error::Error>> {
    // Test INNER JOIN with ON and additional WHERE filtering
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT users.name, orders.product_id, orders.date FROM users INNER JOIN orders ON users.id = orders.user_id WHERE orders.product_id > 102")
        .arg(get_users_file().to_str().unwrap())
        .arg(get_orders_file().to_str().unwrap())
        .arg("-v");

    // Check output - should only include orders with product_id > 102
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("name,product_id,date"));

    // Verify results match the filter
    cmd.assert()
        .stdout(predicate::str::contains("John,103,2023-02-10"))
        .stdout(predicate::str::contains("Jane,105,2023-02-25"))
        .stdout(predicate::str::contains("John,104,2023-03-05"));

    // Verify items with product_id <= 102 are not included
    let output = cmd.output()?;
    let stdout = String::from_utf8(output.stdout)?;
    assert!(
        !stdout.contains(",101,"),
        "Should not contain product_id 101 (≤ 102)"
    );
    assert!(
        !stdout.contains(",102,"),
        "Should not contain product_id 102 (≤ 102)"
    );

    Ok(())
}

#[test]
fn test_three_way_inner_join_on() -> Result<(), Box<dyn std::error::Error>> {
    // Test a three-way INNER JOIN with ON clauses
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT users.name AS user, products.name AS product, products.price, orders.date FROM users INNER JOIN orders ON users.id = orders.user_id INNER JOIN products ON orders.product_id = products.product_id")
        .arg(get_users_file().to_str().unwrap())
        .arg(get_orders_file().to_str().unwrap())
        .arg(get_products_file().to_str().unwrap())
        .arg("-v");

    // Check column aliases and content
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("user,product,price,date"));

    // Check some expected join results
    cmd.assert()
        .stdout(predicate::str::contains("John,Laptop,1200,2023-01-15"))
        .stdout(predicate::str::contains("Jane,Phone,800,2023-01-20"))
        .stdout(predicate::str::contains("John,Headphones,150,2023-02-10"))
        .stdout(predicate::str::contains("Jane,Monitor,350,2023-02-25"))
        .stdout(predicate::str::contains("John,Keyboard,80,2023-03-05"));

    Ok(())
}

#[test]
fn test_complex_operations_with_join_on() -> Result<(), Box<dyn std::error::Error>> {
    // Test a complex query with JOIN ON, WHERE, and ORDER BY
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT users.name AS customer, orders.date AS purchase_date, products.name AS item, products.price AS price FROM users INNER JOIN orders ON users.id = orders.user_id INNER JOIN products ON orders.product_id = products.product_id WHERE products.price > 100 ORDER BY price DESC")
        .arg(get_users_file().to_str().unwrap())
        .arg(get_orders_file().to_str().unwrap())
        .arg(get_products_file().to_str().unwrap())
        .arg("-v");

    // Verify the command executes successfully
    cmd.assert().success();

    // Check basic query output content (using direct contains for robustness)
    let output = cmd.output()?;
    let stdout = String::from_utf8(output.stdout)?;

    // Check header row
    assert!(
        stdout.contains("customer,purchase_date,item,price"),
        "Header row not found or incorrect"
    );

    // Check expected data rows in order
    assert!(
        stdout.contains("John,2023-01-15,Laptop,1200"),
        "Row with John's Laptop not found"
    );
    assert!(
        stdout.contains("Jane,2023-01-20,Phone,800"),
        "Row with Jane's Phone not found"
    );
    assert!(
        stdout.contains("Jane,2023-02-25,Monitor,350"),
        "Row with Jane's Monitor not found"
    );
    assert!(
        stdout.contains("John,2023-02-10,Headphones,150"),
        "Row with John's Headphones not found"
    );

    // Keyboard at $80 shouldn't be included due to price > 100 filter
    assert!(
        !stdout.contains("Keyboard"),
        "Keyboard should not be in results (price ≤ 100)"
    );

    Ok(())
}

/// An inner join's ON conjunct is tested at the deepest table it reads, as
/// a WHERE conjunct is: `b.aid = 0` rejects every b row before the derived
/// table is ever computed, so its overflowing SUM is never reached (SQLite
/// returns no rows too).
#[test]
fn inner_on_conjunct_is_tested_at_its_own_table() -> Result<(), Box<dyn std::error::Error>> {
    const A: (&str, &str) = ("a.csv", "k\n1\n1\n");
    const B: (&str, &str) = ("b.csv", "aid,k\n5,1\n");
    crate::helpers::assert_query_on(
        &[A, B],
        "SELECT 1 AS one FROM b JOIN \
         (SELECT k, SUM(9223372036854775807) AS s FROM a GROUP BY k) d \
         ON b.k = d.k AND b.aid = 0",
        "one",
    )?;
    // After a LEFT JOIN, a conjunct on the NULL-extendable table filters the
    // extended rows, as WHERE would: a row of b with no c match fails it.
    const C: (&str, &str) = ("c.csv", "k,v\n9,x\n");
    crate::helpers::assert_query_on(
        &[A, B, C],
        "SELECT b.aid, c.v FROM b LEFT JOIN c ON c.k = b.k \
         JOIN a ON a.k = b.k AND c.v IS NULL",
        "aid,v\n5,\n5,",
    )
}

/// A conjunct holding a subquery is tested after the level's plain
/// conjuncts and is never hoisted ahead of the loop as a constant (SQLite's
/// wherecode.c defers TERM_VARSELECT terms the same way): no b row passes
/// `aid = 0`, so the overflowing SUM is never computed.
#[test]
fn subquery_conjunct_is_tested_after_plain_ones() -> Result<(), Box<dyn std::error::Error>> {
    const A: (&str, &str) = ("a.csv", "k\n1\n1\n");
    const B: (&str, &str) = ("b.csv", "aid,k\n5,1\n");
    for sql in [
        "SELECT 1 AS one FROM b WHERE b.aid = 0 AND (SELECT SUM(9223372036854775807) FROM a) > 0",
        "SELECT 1 AS one FROM b WHERE (SELECT SUM(9223372036854775807) FROM a) > 0 AND b.aid = 0",
    ] {
        crate::helpers::assert_query_on(&[A, B], sql, "one")?;
    }
    Ok(())
}
