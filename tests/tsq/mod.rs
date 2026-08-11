//! Tests for the `tsq` test-data generator.
//!
//! `tsq` is a second binary installed by `cargo install sqawk`, so it is part
//! of the shipped surface. It had no tests at all; these cover the properties
//! that make it useful -- determinism, referential integrity of the generated
//! tables, and that sqawk can actually read what it writes.

use crate::helpers::create_temp_dir;
use std::fs;
use std::path::Path;

/// Run tsq into a fresh directory and return that directory.
fn generate(seed: u64, rows: usize) -> Result<tempfile::TempDir, Box<dyn std::error::Error>> {
    let dir = create_temp_dir()?;
    let out = dir.path().join("out");
    let output = assert_cmd::cargo::cargo_bin_cmd!("tsq")
        .arg("--seed")
        .arg(seed.to_string())
        .arg("--rows")
        .arg(rows.to_string())
        .arg("--output-dir")
        .arg(out.to_str().unwrap())
        .output()?;
    assert!(
        output.status.success(),
        "tsq failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(dir)
}

fn read(dir: &Path, rel: &str) -> String {
    fs::read_to_string(dir.join("out").join(rel))
        .unwrap_or_else(|e| panic!("missing {rel}: {e}"))
}

#[test]
fn generates_the_expected_tree() -> Result<(), Box<dyn std::error::Error>> {
    let dir = generate(1, 20)?;
    let base = dir.path().join("out");
    for rel in [
        "metadata.json",
        "data/customers.csv",
        "data/products.csv",
        "data/orders.csv",
        "data/order_items.csv",
        "data/reviews.csv",
        "verify/run_verification.sh",
    ] {
        assert!(base.join(rel).exists(), "expected {rel} to be generated");
    }
    assert!(
        base.join("queries").read_dir()?.count() > 0,
        "expected generated queries"
    );
    Ok(())
}

#[test]
fn same_seed_produces_identical_data() -> Result<(), Box<dyn std::error::Error>> {
    // The whole point of the seed: a run is reproducible.
    let a = generate(1234, 25)?;
    let b = generate(1234, 25)?;
    for rel in ["data/customers.csv", "data/orders.csv", "data/products.csv"] {
        assert_eq!(
            read(a.path(), rel),
            read(b.path(), rel),
            "{rel} differed between two runs with the same seed"
        );
    }
    Ok(())
}

#[test]
fn different_seeds_produce_different_data() -> Result<(), Box<dyn std::error::Error>> {
    let a = generate(1, 25)?;
    let b = generate(2, 25)?;
    assert_ne!(
        read(a.path(), "data/customers.csv"),
        read(b.path(), "data/customers.csv"),
        "different seeds should not produce identical data"
    );
    Ok(())
}

#[test]
fn metadata_records_the_seed_and_row_counts() -> Result<(), Box<dyn std::error::Error>> {
    let dir = generate(99, 30)?;
    let meta = read(dir.path(), "metadata.json");
    assert!(meta.contains("\"seed\": 99"), "seed not recorded: {meta}");
    assert!(
        meta.contains("\"customers\": 30"),
        "row count not recorded: {meta}"
    );
    Ok(())
}

#[test]
fn sqawk_can_query_the_generated_data() -> Result<(), Box<dyn std::error::Error>> {
    // The data exists to be fed to sqawk, so the two must agree on format.
    let dir = generate(7, 40)?;
    let customers = dir.path().join("out/data/customers.csv");

    let output = assert_cmd::cargo::cargo_bin_cmd!("sqawk")
        .arg("-s")
        .arg("SELECT COUNT(*) FROM customers")
        .arg(customers.to_str().unwrap())
        .output()?;
    assert!(
        output.status.success(),
        "sqawk could not read tsq output: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("40"),
        "expected 40 customers, got: {stdout}"
    );
    Ok(())
}

#[test]
fn generated_orders_reference_real_customers() -> Result<(), Box<dyn std::error::Error>> {
    // Referential integrity is what makes the join queries meaningful.
    let dir = generate(11, 50)?;
    let base = dir.path().join("out/data");

    let output = assert_cmd::cargo::cargo_bin_cmd!("sqawk")
        .arg("-s")
        // Rows are counted here rather than with COUNT(*), which is not yet
        // supported over a join -- see defects::aggregate_over_join.
        .arg(
            "SELECT orders.order_id FROM orders JOIN customers \
             ON orders.customer_id = customers.customer_id",
        )
        .arg(base.join("orders.csv").to_str().unwrap())
        .arg(base.join("customers.csv").to_str().unwrap())
        .output()?;
    assert!(
        output.status.success(),
        "join over generated data failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Every order must match a customer, so the join count equals the row count.
    let joined = String::from_utf8_lossy(&output.stdout).lines().count() - 1;
    let orders = read(dir.path(), "data/orders.csv").lines().count() - 1;
    assert_eq!(
        joined, orders,
        "every generated order should reference an existing customer"
    );
    Ok(())
}
