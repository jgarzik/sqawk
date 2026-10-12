//! Window function tests for sqawk VM
//!
//! Tests for ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, SUM/AVG/COUNT OVER with OVER clause.

use crate::helpers::{assert_query, assert_query_fails, create_custom_csv, create_temp_dir};

/// Test ROW_NUMBER() without partition
#[test]
fn test_row_number_simple() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name\n1,Alice\n2,Bob\n3,Charlie\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
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

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
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

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
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

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
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

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
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

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT name, salary, LAG(salary) OVER (ORDER BY salary) AS prev_salary FROM data")
        .arg(file_path.to_str().unwrap());

    // First row has NULL, others have previous salary
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("Alice,50000,"))
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

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT name, salary, LEAD(salary) OVER (ORDER BY salary) AS next_salary FROM data")
        .arg(file_path.to_str().unwrap());

    // Last row has NULL, others have next salary
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("Alice,50000,60000"))
        .stdout(predicates::str::contains("Bob,60000,70000"))
        .stdout(predicates::str::contains("Charlie,70000,"));

    Ok(())
}

/// Test LAG() with PARTITION BY
#[test]
fn test_lag_partition() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name,dept,salary\n1,Alice,Sales,50000\n2,Bob,Sales,60000\n3,Charlie,IT,70000\n4,Diana,IT,80000\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT name, dept, salary, LAG(salary) OVER (PARTITION BY dept ORDER BY salary) AS prev_salary FROM data")
        .arg(file_path.to_str().unwrap());

    // First in each partition gets NULL
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("Charlie,IT,70000,"))
        .stdout(predicates::str::contains("Diana,IT,80000,70000"))
        .stdout(predicates::str::contains("Alice,Sales,50000,"))
        .stdout(predicates::str::contains("Bob,Sales,60000,50000"));

    Ok(())
}

/// Test LEAD() with PARTITION BY
#[test]
fn test_lead_partition() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name,dept,salary\n1,Alice,Sales,50000\n2,Bob,Sales,60000\n3,Charlie,IT,70000\n4,Diana,IT,80000\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT name, dept, salary, LEAD(salary) OVER (PARTITION BY dept ORDER BY salary) AS next_salary FROM data")
        .arg(file_path.to_str().unwrap());

    // Last in each partition gets NULL
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("Charlie,IT,70000,80000"))
        .stdout(predicates::str::contains("Diana,IT,80000,"))
        .stdout(predicates::str::contains("Alice,Sales,50000,60000"))
        .stdout(predicates::str::contains("Bob,Sales,60000,"));

    Ok(())
}

/// Test LAG() with offset > 1
#[test]
fn test_lag_offset() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name,salary\n1,Alice,50000\n2,Bob,55000\n3,Charlie,60000\n4,Diana,65000\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg(
            "SELECT name, salary, LAG(salary, 2) OVER (ORDER BY salary) AS prev_2_salary FROM data",
        )
        .arg(file_path.to_str().unwrap());

    // First two rows have NULL, others have salary from 2 rows back
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("Alice,50000,"))
        .stdout(predicates::str::contains("Bob,55000,"))
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

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT name, salary, LEAD(salary, 2) OVER (ORDER BY salary) AS next_2_salary FROM data")
        .arg(file_path.to_str().unwrap());

    // Last two rows have NULL, others have salary from 2 rows ahead
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("Alice,50000,60000"))
        .stdout(predicates::str::contains("Bob,55000,65000"))
        .stdout(predicates::str::contains("Charlie,60000,"))
        .stdout(predicates::str::contains("Diana,65000,"));

    Ok(())
}

/// Test SUM() OVER with ORDER BY (running sum)
#[test]
fn test_sum_over_order() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name,salary\n1,Alice,50000\n2,Bob,55000\n3,Charlie,60000\n4,Diana,65000\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
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

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
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

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
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

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
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

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
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

// ---------------------------------------------------------------------------
// Exact-output tests for the window stage. Expected rows are SQLite's
// (python3 sqlite3, 3.45) over the same data, with NULL printed empty.
// ---------------------------------------------------------------------------

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// Window fixture: ties in `v`, a NULL partition key (id 7), a NULL order
/// key (id 5) and a NULL text value (id 4).
const W: &str = "id,g,h,v,s\n1,a,x,10,p\n2,a,x,20,q\n3,a,y,20,r\n4,b,x,5,\n\
                 5,b,y,,t\n6,b,y,15,u\n7,,x,30,v\n8,a,y,10,w\n";
const CAT: &str = "g,label\na,Alpha\nb,Beta\nc,Gamma\n";

/// `assert_query` over the fixture tables `w` and `cat`.
fn check(sql: &str, expected: &str) -> TestResult {
    let dir = create_temp_dir()?;
    let w = dir.path().join("w.csv");
    let cat = dir.path().join("cat.csv");
    std::fs::write(&w, W)?;
    std::fs::write(&cat, CAT)?;
    assert_query(&[w.to_str().unwrap(), cat.to_str().unwrap()], sql, expected)
}

/// `assert_query_fails` over the fixture tables.
fn check_fails(sql: &str, err: &str) -> TestResult {
    let dir = create_temp_dir()?;
    let w = dir.path().join("w.csv");
    std::fs::write(&w, W)?;
    assert_query_fails(&[w.to_str().unwrap()], sql, err)
}

#[test]
fn nulls_in_partition_and_order_keys() -> TestResult {
    // NULL keys form their own partition and sort first; NULL v adds nothing
    // to SUM, and ORDER BY peers (equal v) share SUM and RANK.
    check(
        "SELECT id, g, v, SUM(v) OVER (PARTITION BY g ORDER BY v) AS s, \
         RANK() OVER (PARTITION BY g ORDER BY v) AS r FROM w",
        "id,g,v,s,r\n7,,30,30,1\n1,a,10,20,1\n8,a,10,20,1\n2,a,20,60,3\n3,a,20,60,3\n\
         5,b,,,1\n4,b,5,5,2\n6,b,15,20,3",
    )
}

#[test]
fn multi_column_partition_and_order() -> TestResult {
    check(
        "SELECT id, g, h, v, ROW_NUMBER() OVER (PARTITION BY g, h ORDER BY v DESC, id) AS rn FROM w",
        "id,g,h,v,rn\n7,,x,30,1\n2,a,x,20,1\n1,a,x,10,2\n3,a,y,20,1\n8,a,y,10,2\n\
         4,b,x,5,1\n6,b,y,15,1\n5,b,y,,2",
    )
}

#[test]
fn two_window_specs_in_one_select() -> TestResult {
    // Without ORDER BY the rows come out in the first window's order.
    check(
        "SELECT id, ROW_NUMBER() OVER (ORDER BY id DESC) AS a, SUM(v) OVER (PARTITION BY h) AS b FROM w",
        "id,a,b\n8,1,45\n7,2,65\n6,3,45\n5,4,45\n4,5,65\n3,6,45\n2,7,65\n1,8,65",
    )
}

#[test]
fn ties_share_rank_and_running_values() -> TestResult {
    check(
        "SELECT id, v, RANK() OVER (ORDER BY v) AS r, DENSE_RANK() OVER (ORDER BY v) AS d, \
         SUM(v) OVER (ORDER BY v) AS s, COUNT(*) OVER (ORDER BY v) AS c FROM w",
        "id,v,r,d,s,c\n5,,1,1,,1\n4,5,2,2,5,2\n1,10,3,3,25,4\n8,10,3,3,25,4\n\
         6,15,5,4,40,5\n2,20,6,5,80,7\n3,20,6,5,80,7\n7,30,8,6,110,8",
    )
}

#[test]
fn ntile_uneven_buckets() -> TestResult {
    check(
        "SELECT id, NTILE(3) OVER (ORDER BY id) AS t FROM w",
        "id,t\n1,1\n2,1\n3,1\n4,2\n5,2\n6,2\n7,3\n8,3",
    )?;
    // More buckets than rows: one row per bucket.
    check(
        "SELECT id, NTILE(5) OVER (PARTITION BY g ORDER BY id) AS t FROM w",
        "id,t\n7,1\n1,1\n2,2\n3,3\n8,4\n4,1\n5,2\n6,3",
    )
}

#[test]
fn lag_lead_offset_and_default() -> TestResult {
    check(
        "SELECT id, LAG(v, 2, -1) OVER (ORDER BY id) AS l, LEAD(s, 3, 'zz') OVER (ORDER BY id) AS d FROM w",
        "id,l,d\n1,-1,\n2,-1,t\n3,10,u\n4,20,v\n5,20,w\n6,5,zz\n7,,zz\n8,15,zz",
    )
}

#[test]
fn first_last_value_default_frame_vs_full_frame() -> TestResult {
    check(
        "SELECT id, FIRST_VALUE(s) OVER (PARTITION BY g ORDER BY id) AS f, \
         LAST_VALUE(s) OVER (PARTITION BY g ORDER BY id) AS l, \
         LAST_VALUE(s) OVER (PARTITION BY g ORDER BY id \
         ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lf FROM w",
        "id,f,l,lf\n7,v,v,v\n1,p,p,w\n2,p,q,w\n3,p,r,w\n8,p,w,w\n4,,,u\n5,,t,u\n6,,u,u",
    )
}

#[test]
fn nth_value_percent_rank_cume_dist() -> TestResult {
    check(
        "SELECT id, NTH_VALUE(id, 2) OVER (ORDER BY id) AS n, PERCENT_RANK() OVER (ORDER BY v) AS p, \
         CUME_DIST() OVER (ORDER BY v) AS c FROM w",
        "id,n,p,c\n1,,0.285714285714286,0.5\n2,2,0.714285714285714,0.875\n\
         3,2,0.714285714285714,0.875\n4,2,0.142857142857143,0.25\n5,2,0.0,0.125\n\
         6,2,0.571428571428571,0.625\n7,2,1.0,1.0\n8,2,0.285714285714286,0.5",
    )
}

#[test]
fn rows_frames() -> TestResult {
    check(
        "SELECT id, SUM(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s, \
         SUM(v) OVER (ORDER BY id ROWS 2 PRECEDING) AS t, \
         COUNT(*) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS c FROM w",
        "id,s,t,c\n1,30,10,8\n2,50,30,7\n3,45,50,6\n4,25,45,5\n5,20,25,4\n\
         6,45,20,3\n7,55,45,2\n8,40,55,1",
    )?;
    // Frames that exclude the current row, and empty frames at the edges.
    check(
        "SELECT id, SUM(v) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS a, \
         MIN(v) OVER (ORDER BY id ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) AS b FROM w",
        "id,a,b\n1,,20\n2,10,5\n3,30,5\n4,50,15\n5,45,15\n6,25,10\n7,20,10\n8,45,",
    )
}

#[test]
fn range_frame_current_row_to_end() -> TestResult {
    check(
        "SELECT id, SUM(v) OVER (ORDER BY v RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS s FROM w",
        "id,s\n5,110\n4,110\n1,105\n8,105\n6,85\n2,70\n3,70\n7,30",
    )
}

#[test]
fn named_window() -> TestResult {
    check(
        "SELECT id, SUM(v) OVER w1 AS s, ROW_NUMBER() OVER (w1 ORDER BY id) AS r \
         FROM w WINDOW w1 AS (PARTITION BY g)",
        "id,s,r\n7,30,1\n1,60,1\n2,60,2\n3,60,3\n8,60,4\n4,20,1\n5,20,2\n6,20,3",
    )
}

#[test]
fn window_over_join() -> TestResult {
    check(
        "SELECT w.id, cat.label, SUM(w.v) OVER (PARTITION BY cat.label) AS s \
         FROM w JOIN cat ON w.g = cat.g ORDER BY w.id",
        "id,label,s\n1,Alpha,60\n2,Alpha,60\n3,Alpha,60\n4,Beta,20\n5,Beta,20\n\
         6,Beta,20\n8,Alpha,60",
    )?;
    // A NULL-extended row is a row like any other.
    check(
        "SELECT cat.label, w.id, ROW_NUMBER() OVER (PARTITION BY cat.g ORDER BY w.id) AS rn \
         FROM cat LEFT JOIN w ON w.g = cat.g",
        "label,id,rn\nAlpha,1,1\nAlpha,2,2\nAlpha,3,3\nAlpha,8,4\nBeta,4,1\n\
         Beta,5,2\nBeta,6,3\nGamma,,1",
    )
}

#[test]
fn window_over_group_by() -> TestResult {
    check(
        "SELECT g, COUNT(*) AS n, SUM(COUNT(*)) OVER () AS total, \
         RANK() OVER (ORDER BY COUNT(*) DESC) AS r FROM w GROUP BY g",
        "g,n,total,r\na,4,8,1\nb,3,8,2\n,1,8,3",
    )?;
    // HAVING filters groups before the window sees them.
    check(
        "SELECT g, SUM(v) AS s, SUM(SUM(v)) OVER (ORDER BY g) AS run FROM w \
         GROUP BY g HAVING COUNT(*) > 1",
        "g,s,run\na,60,60\nb,20,80",
    )?;
    check(
        "SELECT g, h, COUNT(*) AS n, ROW_NUMBER() OVER (PARTITION BY g ORDER BY h DESC) AS rn \
         FROM w GROUP BY g, h",
        "g,h,n,rn\n,x,1,1\na,y,2,1\na,x,2,2\nb,y,2,1\nb,x,1,2",
    )
}

#[test]
fn window_in_subquery() -> TestResult {
    check(
        "SELECT x.id, x.rn FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY v DESC) AS rn FROM w) x \
         WHERE x.rn <= 3",
        "id,rn\n7,1\n2,2\n3,3",
    )?;
    // A correlated scalar subquery runs its window stage once per outer row.
    check(
        "SELECT id, (SELECT COUNT(*) OVER () FROM w w2 WHERE w2.g = w.g LIMIT 1) AS c FROM w",
        "id,c\n1,4\n2,4\n3,4\n4,3\n5,3\n6,3\n7,\n8,4",
    )
}

#[test]
fn window_in_expression() -> TestResult {
    check(
        "SELECT id, v * 2 + SUM(v) OVER (PARTITION BY g) AS e, \
         ROUND(AVG(v) OVER (PARTITION BY g ORDER BY id), 1) AS r FROM w",
        "id,e,r\n7,90,30.0\n1,80,10.0\n2,100,15.0\n3,100,16.7\n8,80,15.0\n4,30,5.0\n5,,5.0\n6,50,10.0",
    )
}

#[test]
fn window_in_order_by() -> TestResult {
    // A window call only in ORDER BY is a hidden sort key.
    check(
        "SELECT id, v FROM w ORDER BY ROW_NUMBER() OVER (ORDER BY v DESC, id), id",
        "id,v\n7,30\n2,20\n3,20\n6,15\n1,10\n8,10\n4,5\n5,",
    )?;
    check(
        "SELECT id, v, ROW_NUMBER() OVER (ORDER BY v) AS rn FROM w ORDER BY rn DESC LIMIT 3",
        "id,v,rn\n7,30,8\n3,20,7\n2,20,6",
    )
}

#[test]
fn window_with_wildcard_and_distinct() -> TestResult {
    check(
        "SELECT *, ROW_NUMBER() OVER (ORDER BY id DESC) AS rn FROM w WHERE id > 5",
        "id,g,h,v,s,rn\n8,a,y,10,w,1\n7,,x,30,v,2\n6,b,y,15,u,3",
    )?;
    check(
        "SELECT DISTINCT g, COUNT(*) OVER (PARTITION BY g) AS c FROM w",
        "g,c\n,1\na,4\nb,3",
    )
}

#[test]
fn window_over_no_rows_and_no_from() -> TestResult {
    check(
        "SELECT id, ROW_NUMBER() OVER () AS rn FROM w WHERE id > 100",
        "id,rn",
    )?;
    check(
        "SELECT ROW_NUMBER() OVER () AS rn, COUNT(*) OVER () AS c",
        "rn,c\n1,1",
    )
}

#[test]
fn sum_over_integers_stays_integer() -> TestResult {
    // 2^53 + 1 is not representable as a double: a float SUM would print
    // 9007199254740992. AVG is a float.
    let dir = create_temp_dir()?;
    let f = dir.path().join("big.csv");
    std::fs::write(&f, "n\n9007199254740992\n1\n")?;
    assert_query(
        &[f.to_str().unwrap()],
        "SELECT n, SUM(n) OVER () AS s, COUNT(n) OVER () AS c FROM big",
        "n,s,c\n9007199254740992,9007199254740993,2\n1,9007199254740993,2",
    )?;
    check(
        "SELECT AVG(v) OVER (PARTITION BY g) AS a FROM w WHERE g = 'b'",
        "a\n10.0\n10.0\n10.0",
    )
}

/// Moving frames follow SQLite's step / inverse evaluation (values from
/// SQLite 3.45): SUM stays a real once a real has been in the frame, an
/// emptied GROUP_CONCAT is NULL, MIN / MAX and COUNT over frames that
/// start ahead of the current row.
#[test]
fn moving_frames_match_sqlite() -> TestResult {
    let dir = create_temp_dir()?;
    let f = dir.path().join("m.csv");
    std::fs::write(&f, "i,x,s\n1,1.5,a\n2,2,\n3,3,\n4,4,b\n5,,cc\n6,6,d\n")?;
    assert_query(
        &[f.to_str().unwrap()],
        "SELECT i, SUM(x) OVER (ORDER BY i ROWS 1 PRECEDING) AS s, \
         GROUP_CONCAT(s, '-') OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS g, \
         MIN(x) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS mn, \
         COUNT(x) OVER (ORDER BY i ROWS BETWEEN 2 FOLLOWING AND UNBOUNDED FOLLOWING) AS c, \
         MAX(s) OVER (ORDER BY i ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) AS mx, \
         TOTAL(x) OVER (ORDER BY i ROWS BETWEEN 3 PRECEDING AND 2 PRECEDING) AS t FROM m",
        "i,s,g,mn,c,mx,t\n1,1.5,a,1.5,3,,0.0\n2,3.5,a,2,2,b,0.0\n3,5.0,,3,1,cc,1.5\n\
         4,7.0,b,4,1,d,3.5\n5,4.0,b-cc,6,0,d,5.0\n6,6.0,cc-d,6,0,,7.0",
    )
}

#[test]
fn window_function_where_group_by_having_is_error() -> TestResult {
    check_fails(
        "SELECT id FROM w WHERE ROW_NUMBER() OVER () > 1",
        "misuse of window function ROW_NUMBER()",
    )?;
    check_fails(
        "SELECT g FROM w GROUP BY ROW_NUMBER() OVER ()",
        "misuse of window function",
    )?;
    check_fails(
        "SELECT g FROM w GROUP BY g HAVING ROW_NUMBER() OVER () > 1",
        "misuse of window function",
    )?;
    check_fails(
        "SELECT SUM(ROW_NUMBER() OVER ()) FROM w",
        "misuse of window function",
    )
}

#[test]
fn unsupported_window_forms_are_errors() -> TestResult {
    check_fails(
        "SELECT SUM(v) FILTER (WHERE v > 10) OVER () FROM w",
        "FILTER",
    )?;
    check_fails(
        "SELECT SUM(DISTINCT v) OVER () FROM w",
        "DISTINCT is not supported for window functions",
    )?;
    check_fails(
        "SELECT UPPER(s) OVER () FROM w",
        "may not be used as a window function",
    )?;
    check_fails(
        "SELECT SUM(v) OVER (ORDER BY v RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM w",
        "RANGE window frames with an offset are not supported",
    )?;
    check_fails(
        "SELECT SUM(v) OVER (ORDER BY id GROUPS 1 PRECEDING) FROM w",
        "GROUPS window frames are not supported",
    )?;
    check_fails(
        "SELECT SUM(v) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 PRECEDING) FROM w",
        "unsupported frame specification",
    )?;
    check_fails("SELECT SUM(v) OVER nope FROM w", "no such window: nope")?;
    check_fails(
        "SELECT NTILE(0) OVER () FROM w",
        "argument of ntile must be a positive integer",
    )?;
    check_fails(
        "SELECT ROW_NUMBER(v) OVER () FROM w",
        "wrong number of arguments",
    )
}
