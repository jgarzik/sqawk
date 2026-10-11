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
    use crate::helpers::{assert_query_on, ONE};

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

    /// 'localtime' then 'utc' gives the moment back in any time zone; 'utc'
    /// after an explicit zone does nothing more.
    #[test]
    fn localtime_utc_round_trip() -> R {
        assert_query_on(
            &[ONE],
            "SELECT datetime('2024-01-15 10:30', 'localtime', 'utc') AS a, \
             datetime('2024-01-15 10:30+02:00', 'utc') AS b, \
             date('2024-07-04', 'localtime', 'utc') AS c FROM one",
            "a,b,c\n2024-01-15 10:30:00,2024-01-15 08:30:00,2024-07-04",
        )
    }

    /// 'localtime' and 'utc' follow the time zone of the TZ environment
    /// variable, daylight saving time included (expected values from SQLite
    /// 3.45 with the same TZ).
    #[cfg(unix)]
    #[test]
    fn localtime_follows_tz() -> R {
        let dir = crate::helpers::create_temp_dir()?;
        let one = dir.path().join("one.csv");
        std::fs::write(&one, "id\n1\n")?;
        let out = std::process::Command::new(env!("CARGO_BIN_EXE_sqawk"))
            .env("TZ", "EST5EDT,M3.2.0,M11.1.0")
            .arg("-s")
            .arg(
                "SELECT datetime('2024-07-01 10:00', 'localtime') AS a, \
                 datetime('2024-01-01 10:00', 'localtime') AS b, \
                 datetime('2024-03-10 02:30', 'utc') AS c, \
                 datetime('1950-07-01 10:00', 'localtime') AS d FROM one",
            )
            .arg(&one)
            .output()?;
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(
            String::from_utf8_lossy(&out.stdout),
            "a,b,c,d\n2024-07-01 06:00:00,2024-01-01 05:00:00,2024-03-10 07:30:00,\
             1950-07-01 06:00:00\n"
        );
        Ok(())
    }

    /// Negative years, whitespace around the time, and the time string forms
    /// SQLite accepts or rejects.
    #[test]
    fn time_string_forms() -> R {
        assert_query_on(
            &[ONE],
            "SELECT date('-0001-12-31') AS a, date('2024-01-01 ') AS b, \
             datetime('2024-01-01  10:00') AS c, date(' 2024-01-01') AS d, \
             date('0000-01-01', '-1 day') AS e, time('24:00') AS f, \
             datetime('2024-01-01T10:00:00Z') AS g, date('20240101') AS h FROM one",
            "a,b,c,d,e,f,g,h\n-0001-12-31,2024-01-01,2024-01-01 10:00:00,,-0001-12-31,\
             24:00:00,2024-01-01 10:00:00,",
        )
    }

    /// The modifiers SQLite 3.45 adds to the classic ones; 'ceiling' and
    /// 'floor' arrived in 3.46 and are NULL, as in 3.45.
    #[test]
    fn newer_modifiers() -> R {
        assert_query_on(
            &[ONE],
            "SELECT datetime('2024-01-01 10:00:00.123', 'subsec') AS a, \
             unixepoch('2024-01-01 10:00:00.5', 'subsec') AS b, \
             datetime(1700000000, 'auto') AS c, date(2460000.5, 'auto') AS d, \
             date(2460000.5, 'julianday') AS e, date(1700000000, 'julianday') AS f, \
             datetime('2024-01-01 10:00', '-01:30:15') AS g, \
             datetime('2024-01-31 10:00', '+0001-01-01 02:00') AS h, \
             date('2024-01-31', 'ceiling') AS i FROM one",
            "a,b,c,d,e,f,g,h,i\n2024-01-01 10:00:00.123,1704103200.5,2023-11-14 22:13:20,\
             2023-02-25,2023-02-25,,2024-01-01 08:29:45,2025-03-04 12:00:00,",
        )
    }

    /// Every strftime conversion of SQLite 3.45; later ones (%G %V ...) are
    /// NULL, as is a trailing '%'.
    #[test]
    fn strftime_conversions() -> R {
        assert_query_on(
            &[ONE],
            "SELECT strftime('%d|%e|%f|%F|%H|%I|%j|%J|%k|%l|%m|%M|%p|%P|%R|%s|%S|%T|%u|%w|%W|%Y|%%', \
             '2024-07-07 15:06:07.089') AS a, strftime('%Y %F', '-0012-01-02') AS b, \
             strftime('%G', '2024-01-01') AS c, strftime('%Y%', '2024-01-01') AS d FROM one",
            "a,b,c,d\n07| 7|07.089|2024-07-07|15|03|189|2460499.129248715|15| 3|07|06|PM|pm|\
             15:06|1720364767|07|15:06:07|7|0|27|2024|%,-012 -012-01-02,,",
        )
    }

    #[test]
    fn timediff_values() -> R {
        assert_query_on(
            &[ONE],
            "SELECT timediff('2024-03-01', '2023-01-15 12:00') AS a, \
             timediff('2023-01-15', '2024-03-01 12:00') AS b, \
             timediff('2024-01-01', NULL) AS c FROM one",
            "a,b,c\n+0001-01-14 12:00:00.000,-0001-01-17 12:00:00.000,",
        )
    }
}
