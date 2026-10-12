//! Red list: `CREATE TABLE ... LOCATION` over a file that already exists.
//!
//! `CREATE TABLE t (id INT, name TEXT) LOCATION './existing.csv'` ignored the
//! file's contents (the table started empty) and `--write` then replaced the
//! file with only the rows the session inserted -- or with a bare header when
//! no statement modified the table. Every test in this module encodes the
//! **correct** behaviour, which follows Hive external-table semantics: the
//! declared columns are the schema for the existing file.
//!
//! - An existing LOCATION file is loaded through the normal loader under the
//!   declared column names. Its first line is a header unless
//!   `WITH (HEADER=false)`: it is skipped as data, must have one field per
//!   declared column, and is written back verbatim.
//! - The delimiter is `WITH (DELIMITER=...)`, else tab for `.tsv` / `.tab`
//!   and comma for everything else.
//! - Every untouched row is written back byte-for-byte.
//! - A LOCATION that does not exist is a new, empty table, created on
//!   `--write`.
//! - A file already loaded (as an operand or another table) is an error.
//! - ROLLBACK removes the table and never touches the file.
//!
//! Each test was `#[ignore]`d when written and un-ignored when the defect
//! was fixed; all of them now pass. A few tests (marked in their doc
//! comments) passed when written and pin behaviour the fix had to keep, and
//! the last two were added with the fix. Run the red list with:
//!     cargo test --test mod defects -- --ignored

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use crate::helpers::create_temp_dir;

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// Run the sqawk binary with `args` in `cwd` (HOME pointed there too).
fn run_in(cwd: &Path, args: &[&str]) -> std::io::Result<Output> {
    Command::new(PathBuf::from(env!("CARGO_BIN_EXE_sqawk")))
        .args(args)
        .current_dir(cwd)
        .env("HOME", cwd)
        .env("XDG_STATE_HOME", cwd)
        .output()
}

/// Run `sql` in `cwd` with the file operand `one.csv` (created here),
/// plus `--write` when `write`; require success and return stdout.
fn ok(cwd: &Path, sql: &str, write: bool) -> Result<String, Box<dyn std::error::Error>> {
    fs::write(cwd.join("one.csv"), "k\n1\n")?;
    let mut args = vec!["-s", sql, "one.csv"];
    if write {
        args.insert(0, "--write");
    }
    let out = run_in(cwd, &args)?;
    assert!(
        out.status.success(),
        "sqawk {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    Ok(String::from_utf8_lossy(&out.stdout).replace("\r\n", "\n"))
}

/// Run `args` in `cwd`, require failure, and return stderr.
fn fails(cwd: &Path, args: &[&str]) -> Result<String, Box<dyn std::error::Error>> {
    let out = run_in(cwd, args)?;
    assert!(
        !out.status.success(),
        "sqawk {args:?} succeeded: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    Ok(String::from_utf8_lossy(&out.stderr).into_owned())
}

/// The existing file's rows are the table's rows, under the declared names.
#[test]
fn existing_file_rows_are_visible() -> TestResult {
    let dir = create_temp_dir()?;
    fs::write(dir.path().join("e.csv"), "num,label\n1,alice\n2,bob\n")?;
    let out = ok(
        dir.path(),
        "CREATE TABLE t (id INT, name TEXT) LOCATION './e.csv'; SELECT id, name FROM t",
        false,
    )?;
    assert_eq!(out, "id,name\n1,alice\n2,bob\n");
    Ok(())
}

/// The declared types are the columns' types: `id INT` compares numerically.
#[test]
fn existing_file_uses_declared_types() -> TestResult {
    let dir = create_temp_dir()?;
    fs::write(dir.path().join("e.csv"), "num,label\n10,a\n9,b\n")?;
    let out = ok(
        dir.path(),
        "CREATE TABLE t (id INT, name TEXT) LOCATION './e.csv'; \
         SELECT name FROM t ORDER BY id",
        false,
    )?;
    assert_eq!(out, "name\nb\na\n");
    Ok(())
}

/// With no DML, `--write` leaves the file byte-identical (BOM, CRLF, quoting
/// and the header's own text included).
#[test]
fn no_dml_write_leaves_file_identical() -> TestResult {
    let dir = create_temp_dir()?;
    let data = b"\xef\xbb\xbfnum,\"label\"\r\n1,\"a b\"\r\n2,  bob\r\n";
    fs::write(dir.path().join("e.csv"), data)?;
    ok(
        dir.path(),
        "CREATE TABLE t (id INT, name TEXT) LOCATION './e.csv'",
        true,
    )?;
    assert_eq!(fs::read(dir.path().join("e.csv"))?, data.to_vec());
    Ok(())
}

/// INSERT appends; the original header and rows stay byte-identical.
#[test]
fn insert_write_appends_and_keeps_rows() -> TestResult {
    let dir = create_temp_dir()?;
    fs::write(
        dir.path().join("e.csv"),
        "num,label\n1,\"alice\"\n2,  bob\n",
    )?;
    ok(
        dir.path(),
        "CREATE TABLE t (id INT, name TEXT) LOCATION './e.csv'; \
         INSERT INTO t VALUES (3, 'carol')",
        true,
    )?;
    assert_eq!(
        fs::read_to_string(dir.path().join("e.csv"))?,
        "num,label\n1,\"alice\"\n2,  bob\n3,carol\n"
    );
    Ok(())
}

/// UPDATE of one row rewrites that row only.
#[test]
fn update_one_row_keeps_others() -> TestResult {
    let dir = create_temp_dir()?;
    fs::write(
        dir.path().join("e.csv"),
        "num,label\n1,\"alice\"\n2,  bob\n3,x\n",
    )?;
    ok(
        dir.path(),
        "CREATE TABLE t (id INT, name TEXT) LOCATION './e.csv'; \
         UPDATE t SET name = 'y' WHERE id = 3",
        true,
    )?;
    assert_eq!(
        fs::read_to_string(dir.path().join("e.csv"))?,
        "num,label\n1,\"alice\"\n2,  bob\n3,y\n"
    );
    Ok(())
}

/// A header whose field count differs from the declared column count is an
/// error naming the file and both counts; the file is left alone.
#[test]
fn header_field_count_mismatch_is_an_error() -> TestResult {
    let dir = create_temp_dir()?;
    let data = "a,b,c\n1,2,3\n";
    fs::write(dir.path().join("e.csv"), data)?;
    fs::write(dir.path().join("one.csv"), "k\n1\n")?;
    let err = fails(
        dir.path(),
        &[
            "--write",
            "-s",
            "CREATE TABLE t (id INT, name TEXT) LOCATION './e.csv'; \
             INSERT INTO t VALUES (9, 'z')",
            "one.csv",
        ],
    )?;
    assert!(err.contains("e.csv"), "{err}");
    assert!(
        err.contains("3 fields") && err.contains("2 columns"),
        "{err}"
    );
    assert_eq!(fs::read_to_string(dir.path().join("e.csv"))?, data);
    Ok(())
}

/// `WITH (HEADER=false)`: the first line is data, the columns are the
/// declared ones, and writeback emits no header.
#[test]
fn header_false_first_line_is_data() -> TestResult {
    let dir = create_temp_dir()?;
    fs::write(dir.path().join("d.txt"), "1,alice\n2,bob\n")?;
    let out = ok(
        dir.path(),
        "CREATE TABLE t (id INT, name TEXT) LOCATION './d.txt' WITH (HEADER=false); \
         SELECT * FROM t",
        false,
    )?;
    assert_eq!(out, "id,name\n1,alice\n2,bob\n");
    ok(
        dir.path(),
        "CREATE TABLE t (id INT, name TEXT) LOCATION './d.txt' WITH (HEADER=FALSE); \
         INSERT INTO t VALUES (3, 'carol')",
        true,
    )?;
    assert_eq!(
        fs::read_to_string(dir.path().join("d.txt"))?,
        "1,alice\n2,bob\n3,carol\n"
    );
    Ok(())
}

/// `WITH (HEADER=true)` is the default spelled out.
#[test]
fn header_true_is_the_default() -> TestResult {
    let dir = create_temp_dir()?;
    fs::write(dir.path().join("e.csv"), "num,label\n1,alice\n")?;
    let out = ok(
        dir.path(),
        "CREATE TABLE t (id INT, name TEXT) LOCATION './e.csv' WITH (header='True'); \
         SELECT * FROM t",
        false,
    )?;
    assert_eq!(out, "id,name\n1,alice\n");
    Ok(())
}

/// A `.tsv` LOCATION is read tab-separated.
#[test]
fn tsv_extension_reads_tab_separated() -> TestResult {
    let dir = create_temp_dir()?;
    fs::write(dir.path().join("x.tsv"), "num\tlabel\n1\talice, a\n")?;
    let out = ok(
        dir.path(),
        "CREATE TABLE t (id INT, name TEXT) LOCATION './x.tsv'; \
         INSERT INTO t VALUES (2, 'bob'); SELECT name FROM t",
        true,
    )?;
    assert_eq!(out, "name\n\"alice, a\"\nbob\n");
    assert_eq!(
        fs::read_to_string(dir.path().join("x.tsv"))?,
        "num\tlabel\n1\talice, a\n2\tbob\n"
    );
    Ok(())
}

/// `WITH (DELIMITER=...)` overrides the extension rule.
#[test]
fn with_delimiter_overrides_extension() -> TestResult {
    let dir = create_temp_dir()?;
    fs::write(dir.path().join("x.csv"), "num;label\n1;alice\n")?;
    let out = ok(
        dir.path(),
        "CREATE TABLE t (id INT, name TEXT) LOCATION './x.csv' WITH (DELIMITER=';'); \
         SELECT name, id FROM t",
        false,
    )?;
    assert_eq!(out, "name,id\nalice,1\n");
    Ok(())
}

/// `WITH (COMMENTS=true)`: `#` lines are skipped and written back in place.
#[test]
fn with_comments_skips_and_keeps_hash_lines() -> TestResult {
    let dir = create_temp_dir()?;
    fs::write(
        dir.path().join("c.csv"),
        "# top\nnum,label\n1,alice\n# mid\n2,bob\n",
    )?;
    let out = ok(
        dir.path(),
        "CREATE TABLE t (id INT, name TEXT) LOCATION './c.csv' WITH (COMMENTS=true); \
         DELETE FROM t WHERE id = 1; SELECT * FROM t",
        true,
    )?;
    assert_eq!(out, "id,name\n2,bob\n");
    assert_eq!(
        fs::read_to_string(dir.path().join("c.csv"))?,
        "# top\nnum,label\n# mid\n2,bob\n"
    );
    Ok(())
}

/// A record with more fields than declared columns is the usual load error,
/// with file and line.
#[test]
fn too_many_fields_is_a_load_error() -> TestResult {
    let dir = create_temp_dir()?;
    fs::write(dir.path().join("e.csv"), "num,label\n1,a\n2,b,c\n")?;
    fs::write(dir.path().join("one.csv"), "k\n1\n")?;
    let err = fails(
        dir.path(),
        &[
            "-s",
            "CREATE TABLE t (id INT, name TEXT) LOCATION './e.csv'",
            "one.csv",
        ],
    )?;
    assert!(err.contains("e.csv") && err.contains('3'), "{err}");
    Ok(())
}

/// Passed when written: a LOCATION that does not exist is a new table,
/// created on `--write` with a header of the declared names.
#[test]
fn missing_file_is_created() -> TestResult {
    let dir = create_temp_dir()?;
    ok(
        dir.path(),
        "CREATE TABLE t (id INT, name TEXT) LOCATION './new.csv'; \
         INSERT INTO t VALUES (1, 'a')",
        true,
    )?;
    assert_eq!(
        fs::read_to_string(dir.path().join("new.csv"))?,
        "id,name\n1,a\n"
    );
    Ok(())
}

/// A new file under `WITH (HEADER=false)` is written without a header.
#[test]
fn missing_file_header_false_has_no_header() -> TestResult {
    let dir = create_temp_dir()?;
    ok(
        dir.path(),
        "CREATE TABLE t (id INT, name TEXT) LOCATION './new.csv' WITH (HEADER=false); \
         INSERT INTO t VALUES (1, 'a')",
        true,
    )?;
    assert_eq!(fs::read_to_string(dir.path().join("new.csv"))?, "1,a\n");
    Ok(())
}

/// The LOCATION file is already loaded as an operand: an error, and the
/// file is left alone.
#[test]
fn location_already_loaded_as_operand() -> TestResult {
    let dir = create_temp_dir()?;
    let data = "id,name\n1,alice\n";
    fs::write(dir.path().join("e.csv"), data)?;
    let err = fails(
        dir.path(),
        &[
            "--write",
            "-s",
            "CREATE TABLE t (id INT, name TEXT) LOCATION './e.csv'; \
             INSERT INTO t VALUES (2, 'bob')",
            "e.csv",
        ],
    )?;
    assert!(err.contains("already loaded as table e"), "{err}");
    assert_eq!(fs::read_to_string(dir.path().join("e.csv"))?, data);
    Ok(())
}

/// Two LOCATION tables over one file: the second is an error.
#[test]
fn location_already_loaded_as_another_table() -> TestResult {
    let dir = create_temp_dir()?;
    fs::write(dir.path().join("e.csv"), "id,name\n1,alice\n")?;
    fs::write(dir.path().join("one.csv"), "k\n1\n")?;
    let err = fails(
        dir.path(),
        &[
            "-s",
            "CREATE TABLE t (id INT, name TEXT) LOCATION './e.csv'; \
             CREATE TABLE u (id INT, name TEXT) LOCATION 'e.csv'",
            "one.csv",
        ],
    )?;
    assert!(err.contains("already loaded as table t"), "{err}");
    Ok(())
}

/// Passed when written: ROLLBACK removes the table and does not touch the
/// file.
#[test]
fn rollback_leaves_file_untouched() -> TestResult {
    let dir = create_temp_dir()?;
    let data = "num,label\n1,alice\n";
    fs::write(dir.path().join("e.csv"), data)?;
    ok(
        dir.path(),
        "BEGIN; CREATE TABLE t (id INT, name TEXT) LOCATION './e.csv'; \
         INSERT INTO t VALUES (2, 'bob'); ROLLBACK",
        true,
    )?;
    assert_eq!(fs::read_to_string(dir.path().join("e.csv"))?, data);
    Ok(())
}

/// A LOCATION that is a directory is a clean error.
#[test]
fn location_directory_is_an_error() -> TestResult {
    let dir = create_temp_dir()?;
    fs::create_dir(dir.path().join("sub"))?;
    fs::write(dir.path().join("one.csv"), "k\n1\n")?;
    let err = fails(
        dir.path(),
        &[
            "-s",
            "CREATE TABLE t (id INT) LOCATION './sub'; SELECT * FROM t",
            "one.csv",
        ],
    )?;
    assert!(err.starts_with("sqawk: ") && err.contains("sub"), "{err}");
    Ok(())
}

/// `HEADER` and `COMMENTS` take true or false (any case, quoted or not);
/// anything else is an error that names the option.
#[test]
fn boolean_option_value_is_checked() -> TestResult {
    let dir = create_temp_dir()?;
    fs::write(dir.path().join("one.csv"), "k\n1\n")?;
    for with in ["HEADER=maybe", "COMMENTS='yes'", "header=1"] {
        let sql = format!("CREATE TABLE t (id INT) LOCATION './n.csv' WITH ({with})");
        let err = fails(dir.path(), &["-s", &sql, "one.csv"])?;
        assert!(err.contains("must be true or false"), "{with}: {err}");
    }
    Ok(())
}

/// TRUNCATE leaves the file's own header line, not the declared names.
#[test]
fn truncate_keeps_the_header_line() -> TestResult {
    let dir = create_temp_dir()?;
    fs::write(dir.path().join("e.csv"), "NUM,\"Label\"\n1,a\n")?;
    ok(
        dir.path(),
        "CREATE TABLE t (id INT, name TEXT) LOCATION './e.csv'; TRUNCATE TABLE t",
        true,
    )?;
    assert_eq!(
        fs::read_to_string(dir.path().join("e.csv"))?,
        "NUM,\"Label\"\n"
    );
    Ok(())
}
