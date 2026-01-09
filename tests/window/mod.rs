//! Window function tests for sqawk VM
//!
//! Tests for ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, SUM/AVG/COUNT OVER with OVER clause.

use crate::helpers::{create_custom_csv, create_temp_dir};

/// Test ROW_NUMBER() without partition
#[test]
fn test_row_number_simple() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name\n1,Alice\n2,Bob\n3,Charlie\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT name, ROW_NUMBER() OVER () AS rn FROM data")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("Alice,1"))
        .stdout(predicates::str::contains("Bob,2"))
        .stdout(predicates::str::contains("Charlie,3"));

    Ok(())
}

/// Test ROW_NUMBER() with PARTITION BY
#[test]
fn test_row_number_partition() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name,dept\n1,Alice,Sales\n2,Bob,Sales\n3,Charlie,IT\n4,Diana,IT\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT name, dept, ROW_NUMBER() OVER (PARTITION BY dept) AS rn FROM data")
        .arg(file_path.to_str().unwrap());

    // IT should have 1,2 and Sales should have 1,2
    cmd.assert()
        .success()
        .stdout(predicates::str::contains(",IT,1"))
        .stdout(predicates::str::contains(",IT,2"))
        .stdout(predicates::str::contains(",Sales,1"))
        .stdout(predicates::str::contains(",Sales,2"));

    Ok(())
}

/// Test ROW_NUMBER() with ORDER BY
#[test]
fn test_row_number_order() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name,score\n1,Alice,80\n2,Bob,90\n3,Charlie,70\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT name, score, ROW_NUMBER() OVER (ORDER BY score DESC) AS rn FROM data")
        .arg(file_path.to_str().unwrap());

    // Bob (90) should be first, Alice (80) second, Charlie (70) third
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("Bob,90,1"))
        .stdout(predicates::str::contains("Alice,80,2"))
        .stdout(predicates::str::contains("Charlie,70,3"));

    Ok(())
}

/// Test RANK() with ties
#[test]
fn test_rank_ties() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name,score\n1,Alice,90\n2,Bob,85\n3,Charlie,90\n4,Diana,80\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT name, score, RANK() OVER (ORDER BY score DESC) AS rnk FROM data")
        .arg(file_path.to_str().unwrap());

    // Alice and Charlie both 90 get rank 1, Bob gets 3 (not 2), Diana gets 4
    cmd.assert()
        .success()
        .stdout(predicates::str::contains(",90,1"))
        .stdout(predicates::str::contains("Bob,85,3"))
        .stdout(predicates::str::contains("Diana,80,4"));

    Ok(())
}

/// Test DENSE_RANK()
#[test]
fn test_dense_rank() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name,score\n1,Alice,90\n2,Bob,85\n3,Charlie,90\n4,Diana,80\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT name, score, DENSE_RANK() OVER (ORDER BY score DESC) AS drnk FROM data")
        .arg(file_path.to_str().unwrap());

    // Alice and Charlie get 1, Bob gets 2 (not 3), Diana gets 3
    cmd.assert()
        .success()
        .stdout(predicates::str::contains(",90,1"))
        .stdout(predicates::str::contains("Bob,85,2"))
        .stdout(predicates::str::contains("Diana,80,3"));

    Ok(())
}

/// Test LAG() without partition
#[test]
fn test_lag_simple() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name,salary\n1,Alice,50000\n2,Bob,60000\n3,Charlie,70000\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT name, salary, LAG(salary) OVER (ORDER BY salary) AS prev_salary FROM data")
        .arg(file_path.to_str().unwrap());

    // First row has NULL, others have previous salary
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("Alice,50000,NULL"))
        .stdout(predicates::str::contains("Bob,60000,50000"))
        .stdout(predicates::str::contains("Charlie,70000,60000"));

    Ok(())
}

/// Test LEAD() without partition
#[test]
fn test_lead_simple() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name,salary\n1,Alice,50000\n2,Bob,60000\n3,Charlie,70000\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT name, salary, LEAD(salary) OVER (ORDER BY salary) AS next_salary FROM data")
        .arg(file_path.to_str().unwrap());

    // Last row has NULL, others have next salary
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("Alice,50000,60000"))
        .stdout(predicates::str::contains("Bob,60000,70000"))
        .stdout(predicates::str::contains("Charlie,70000,NULL"));

    Ok(())
}

/// Test LAG() with PARTITION BY
#[test]
fn test_lag_partition() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name,dept,salary\n1,Alice,Sales,50000\n2,Bob,Sales,60000\n3,Charlie,IT,70000\n4,Diana,IT,80000\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT name, dept, salary, LAG(salary) OVER (PARTITION BY dept ORDER BY salary) AS prev_salary FROM data")
        .arg(file_path.to_str().unwrap());

    // First in each partition gets NULL
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("Charlie,IT,70000,NULL"))
        .stdout(predicates::str::contains("Diana,IT,80000,70000"))
        .stdout(predicates::str::contains("Alice,Sales,50000,NULL"))
        .stdout(predicates::str::contains("Bob,Sales,60000,50000"));

    Ok(())
}

/// Test LEAD() with PARTITION BY
#[test]
fn test_lead_partition() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name,dept,salary\n1,Alice,Sales,50000\n2,Bob,Sales,60000\n3,Charlie,IT,70000\n4,Diana,IT,80000\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT name, dept, salary, LEAD(salary) OVER (PARTITION BY dept ORDER BY salary) AS next_salary FROM data")
        .arg(file_path.to_str().unwrap());

    // Last in each partition gets NULL
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("Charlie,IT,70000,80000"))
        .stdout(predicates::str::contains("Diana,IT,80000,NULL"))
        .stdout(predicates::str::contains("Alice,Sales,50000,60000"))
        .stdout(predicates::str::contains("Bob,Sales,60000,NULL"));

    Ok(())
}

/// Test LAG() with offset > 1
#[test]
fn test_lag_offset() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name,salary\n1,Alice,50000\n2,Bob,55000\n3,Charlie,60000\n4,Diana,65000\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg(
            "SELECT name, salary, LAG(salary, 2) OVER (ORDER BY salary) AS prev_2_salary FROM data",
        )
        .arg(file_path.to_str().unwrap());

    // First two rows have NULL, others have salary from 2 rows back
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("Alice,50000,NULL"))
        .stdout(predicates::str::contains("Bob,55000,NULL"))
        .stdout(predicates::str::contains("Charlie,60000,50000"))
        .stdout(predicates::str::contains("Diana,65000,55000"));

    Ok(())
}

/// Test LEAD() with offset > 1
#[test]
fn test_lead_offset() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name,salary\n1,Alice,50000\n2,Bob,55000\n3,Charlie,60000\n4,Diana,65000\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT name, salary, LEAD(salary, 2) OVER (ORDER BY salary) AS next_2_salary FROM data")
        .arg(file_path.to_str().unwrap());

    // Last two rows have NULL, others have salary from 2 rows ahead
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("Alice,50000,60000"))
        .stdout(predicates::str::contains("Bob,55000,65000"))
        .stdout(predicates::str::contains("Charlie,60000,NULL"))
        .stdout(predicates::str::contains("Diana,65000,NULL"));

    Ok(())
}

/// Test SUM() OVER with ORDER BY (running sum)
#[test]
fn test_sum_over_order() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name,salary\n1,Alice,50000\n2,Bob,55000\n3,Charlie,60000\n4,Diana,65000\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT name, salary, SUM(salary) OVER (ORDER BY salary) AS running_sum FROM data")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("Alice,50000,50000"))
        .stdout(predicates::str::contains("Bob,55000,105000"))
        .stdout(predicates::str::contains("Charlie,60000,165000"))
        .stdout(predicates::str::contains("Diana,65000,230000"));

    Ok(())
}

/// Test AVG() OVER with ORDER BY (running average)
#[test]
fn test_avg_over_order() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name,score\n1,Alice,100\n2,Bob,80\n3,Charlie,60\n4,Diana,40\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT name, score, AVG(score) OVER (ORDER BY score DESC) AS running_avg FROM data")
        .arg(file_path.to_str().unwrap());

    // Descending order: Alice(100), Bob(80), Charlie(60), Diana(40)
    // Running avg: 100, 90, 80, 70
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("Alice,100,100"))
        .stdout(predicates::str::contains("Bob,80,90"))
        .stdout(predicates::str::contains("Charlie,60,80"))
        .stdout(predicates::str::contains("Diana,40,70"));

    Ok(())
}

/// Test COUNT() OVER with ORDER BY (running count)
#[test]
fn test_count_over_order() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name,salary\n1,Alice,50000\n2,Bob,55000\n3,Charlie,60000\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT name, salary, COUNT(salary) OVER (ORDER BY salary) AS running_count FROM data")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("Alice,50000,1"))
        .stdout(predicates::str::contains("Bob,55000,2"))
        .stdout(predicates::str::contains("Charlie,60000,3"));

    Ok(())
}

/// Test MIN() and MAX() OVER with ORDER BY
#[test]
fn test_min_max_over_order() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name,salary\n1,Alice,50000\n2,Bob,60000\n3,Charlie,55000\n4,Diana,70000\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT name, salary, MIN(salary) OVER (ORDER BY salary) AS running_min, MAX(salary) OVER (ORDER BY salary) AS running_max FROM data")
        .arg(file_path.to_str().unwrap());

    // Sorted: Alice(50000), Charlie(55000), Bob(60000), Diana(70000)
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("Alice,50000,50000,50000"))
        .stdout(predicates::str::contains("Charlie,55000,50000,55000"))
        .stdout(predicates::str::contains("Bob,60000,50000,60000"))
        .stdout(predicates::str::contains("Diana,70000,50000,70000"));

    Ok(())
}

/// Test SUM() OVER with PARTITION BY
#[test]
fn test_sum_over_partition() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name,dept,salary\n1,Alice,Sales,50000\n2,Bob,Sales,60000\n3,Charlie,IT,70000\n4,Diana,IT,80000\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::Command::cargo_bin("sqawk")?;
    cmd.arg("-s")
        .arg("SELECT name, dept, salary, SUM(salary) OVER (PARTITION BY dept ORDER BY salary) AS dept_sum FROM data")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("Charlie,IT,70000,70000"))
        .stdout(predicates::str::contains("Diana,IT,80000,150000"))
        .stdout(predicates::str::contains("Alice,Sales,50000,50000"))
        .stdout(predicates::str::contains("Bob,Sales,60000,110000"));

    Ok(())
}
