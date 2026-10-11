//! Date/time function tests for sqawk VM
//!
//! Tests for DATE, TIME, NOW functions.

use crate::helpers::{create_custom_csv, create_temp_dir};

/// Test NOW() function returns a timestamp
#[test]
fn test_now() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id\n1\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT NOW() AS current_time FROM data")
        .arg(file_path.to_str().unwrap());

    // NOW() should return something with date-like format (YYYY-MM-DD)
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("202")); // Year starts with 202x

    Ok(())
}

/// Test DATE function extracts date from datetime string
#[test]
fn test_date_extract() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,datetime\n1,2024-01-15 10:30:00\n2,2024-06-20 14:45:00\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT DATE(datetime) AS dt FROM data")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("2024-01-15"))
        .stdout(predicates::str::contains("2024-06-20"));

    Ok(())
}

/// Test TIME function extracts time from datetime string
#[test]
fn test_time_extract() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,datetime\n1,2024-01-15 10:30:00\n2,2024-06-20 14:45:00\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT TIME(datetime) AS tm FROM data")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("10:30:00"))
        .stdout(predicates::str::contains("14:45:00"));

    Ok(())
}

/// Test CURRENT_DATE returns today's date
#[test]
fn test_current_date() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id\n1\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT CURRENT_DATE AS today FROM data")
        .arg(file_path.to_str().unwrap());

    // Should return date in YYYY-MM-DD format
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("202")); // Year starts with 202x

    Ok(())
}

// ---------------------------------------------------------------------------
// SQLite date semantics (UTC). Expected values computed with SQLite 3.45.
// ---------------------------------------------------------------------------

mod sqlite_semantics {
    use crate::helpers::{assert_query_on, assert_query_on_fails, ONE};

    type R = Result<(), Box<dyn std::error::Error>>;

    /// Malformed or out-of-range dates are NULL; a day past the end of the
    /// month (up to 31) rolls into the next month, as in SQLite.
    #[test]
    fn date_validation_and_rollover() -> R {
        assert_query_on(
            &[ONE],
            "SELECT date('2024-02-30') AS a, date('2024-02-32') AS b, date('2023-02-29') AS c, \
             date('2024-1-5') AS d, date('2024-01-15 10:30:00') AS e, \
             date('2024-01-15T10:30:00') AS f, date('garbage') AS g, date('2100-02-29') AS h \
             FROM one",
            "a,b,c,d,e,f,g,h\n2024-03-01,,2023-03-01,,2024-01-15,2024-01-15,,2100-03-01",
        )
    }

    #[test]
    fn time_and_datetime() -> R {
        assert_query_on(
            &[ONE],
            "SELECT time('2024-01-15 10:30:00') AS a, time('10:30') AS b, \
             time('2024-01-15') AS c, time('10:60:00') AS d, datetime('2024-01-15') AS e, \
             datetime('10:30') AS f, datetime('2024-01-15 10:30:00.5') AS g FROM one",
            "a,b,c,d,e,f,g\n10:30:00,10:30:00,00:00:00,,2024-01-15 00:00:00,\
             2000-01-01 10:30:00,2024-01-15 10:30:00",
        )
    }

    #[test]
    fn modifiers() -> R {
        assert_query_on(
            &[ONE],
            "SELECT date('2024-01-31', '+1 month') AS a, date('2024-03-15', 'start of month') AS b, \
             datetime('2024-03-15 10:00', '+90 minutes') AS c, \
             date('2024-01-15', 'weekday 0') AS d, date('2024-01-15', 'bogus') AS e, \
             datetime('2024-01-01', '+1.5 days') AS f, date('2024-02-29', '+1 year') AS g \
             FROM one",
            "a,b,c,d,e,f,g\n2024-03-02,2024-03-01,2024-03-15 11:30:00,2024-01-21,,\
             2024-01-02 12:00:00,2025-03-01",
        )
    }

    #[test]
    fn strftime_julianday_unixepoch() -> R {
        assert_query_on(
            &[ONE],
            "SELECT strftime('%Y-%m-%d %H:%M:%S', '2024-01-15 10:30:45') AS a, \
             strftime('%j %w %s %%', '2024-01-15') AS b, \
             strftime('%f', '2024-01-15 10:30:45.123') AS c, strftime('%W', '2024-12-30') AS d, \
             strftime('%q', '2024-01-15') AS e FROM one",
            "a,b,c,d,e\n2024-01-15 10:30:45,015 1 1705276800 %,45.123,53,",
        )?;
        assert_query_on(
            &[ONE],
            "SELECT julianday('2024-01-15') AS a, unixepoch('2024-01-15 10:30:00') AS b, \
             date(1705312200, 'unixepoch') AS c, date(2460324.5) AS d, datetime(0) AS e FROM one",
            "a,b,c,d,e\n2460324.5,1705314600,2024-01-15,2024-01-15,-4713-11-24 12:00:00",
        )
    }

    /// The CURRENT_* values agree with the 'now' time string.
    #[test]
    fn current_values_agree_with_now() -> R {
        assert_query_on(
            &[ONE],
            "SELECT CURRENT_DATE = date('now') AS a, CURRENT_TIMESTAMP = datetime('now') AS b, \
             length(CURRENT_TIME) AS c, CURRENT_DATE LIKE '____-__-__' AS d FROM one",
            "a,b,c,d\n1,1,8,1",
        )
    }

    /// sqawk works in UTC; 'localtime' is refused rather than ignored.
    #[test]
    fn localtime_modifier_is_unsupported() -> R {
        assert_query_on_fails(
            &[ONE],
            "SELECT date('now', 'localtime') FROM one",
            "localtime",
        )
    }
}
