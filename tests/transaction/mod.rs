//! BEGIN / COMMIT / ROLLBACK over the in-memory tables, and how they meet
//! `--write` and the REPL; plus the writeback rules that decide which tables
//! are written (scratch tables, DROP, read-only files, hard links).
//!
//! Expected results follow SQLite 3.45.

use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Output, Stdio};

use crate::helpers::create_temp_dir;

type R = Result<(), Box<dyn std::error::Error>>;

const TX: &str = "k,n\n1,10\n2,20\n";

/// Run sqawk in `dir` with `args`, `stdin` piped in.
fn run(dir: &Path, args: &[&str], stdin: &str) -> std::io::Result<Output> {
    let mut child = Command::new(assert_cmd::cargo::cargo_bin!("sqawk"))
        .args(args)
        .current_dir(dir)
        .env("HOME", dir)
        .env("XDG_STATE_HOME", dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    // A process that rejects its arguments can exit before reading stdin;
    // the write then fails with BrokenPipe, which is not a test failure.
    match child.stdin.take().unwrap().write_all(stdin.as_bytes()) {
        Err(e) if e.kind() != std::io::ErrorKind::BrokenPipe => return Err(e),
        _ => {}
    }
    child.wait_with_output()
}

/// Run `-s` statements (with `extra` args) over `tx.csv` holding `TX`;
/// return (output, tx.csv contents afterwards).
fn run_tx(sqls: &[&str], extra: &[&str]) -> Result<(Output, String), Box<dyn std::error::Error>> {
    let dir = create_temp_dir()?;
    fs::write(dir.path().join("tx.csv"), TX)?;
    let mut args: Vec<&str> = extra.to_vec();
    for s in sqls {
        args.push("-s");
        args.push(s);
    }
    args.push("tx.csv");
    let out = run(dir.path(), &args, "")?;
    let file = fs::read_to_string(dir.path().join("tx.csv"))?;
    Ok((out, file))
}

fn stdout(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

fn assert_ok(out: &Output) {
    assert!(out.status.success(), "failed: {}", stderr(out));
}

/// The CLI error contract: exit 1, one stderr line containing `needle`.
fn assert_error(out: &Output, needle: &str) {
    let err = stderr(out);
    assert_eq!(out.status.code(), Some(1), "stderr: {err}");
    assert_eq!(err.lines().count(), 1, "stderr: {err}");
    assert!(err.contains(needle), "stderr: {err}");
}

#[test]
fn rollback_undoes_ddl() -> R {
    let (out, _) = run_tx(
        &[
            "BEGIN",
            "CREATE TABLE q (a INT)",
            "ALTER TABLE tx ADD COLUMN z TEXT",
            "INSERT INTO tx VALUES (3, 30, 'x')",
            "DROP TABLE tx",
            "ROLLBACK",
            "SELECT * FROM tx",
        ],
        &[],
    )?;
    assert_ok(&out);
    assert_eq!(stdout(&out), "k,n\n1,10\n2,20\n");

    let (out, _) = run_tx(
        &["BEGIN; CREATE TABLE q (a INT); ROLLBACK; SELECT * FROM q"],
        &[],
    )?;
    assert_error(&out, "no such table: q");
    Ok(())
}

#[test]
fn rollback_undoes_truncate_and_delete() -> R {
    let (out, file) = run_tx(
        &[
            "BEGIN; DELETE FROM tx WHERE k = 1; TRUNCATE TABLE tx; ROLLBACK",
            "SELECT * FROM tx",
        ],
        &["--write"],
    )?;
    assert_ok(&out);
    assert_eq!(stdout(&out), "k,n\n1,10\n2,20\n");
    assert_eq!(file, TX);
    Ok(())
}

/// A change made before BEGIN stays, and is written, after ROLLBACK.
#[test]
fn rollback_keeps_earlier_changes() -> R {
    let (out, file) = run_tx(
        &[
            "UPDATE tx SET n = 5 WHERE k = 1",
            "BEGIN",
            "UPDATE tx SET n = 6",
            "ROLLBACK",
        ],
        &["--write"],
    )?;
    assert_ok(&out);
    assert_eq!(file, "k,n\n1,5\n2,20\n");
    Ok(())
}

/// DROP inside a rolled-back transaction: the table, and its pending
/// change from before BEGIN, come back and are written.
#[test]
fn rollback_of_drop_restores_pending_write() -> R {
    let (out, file) = run_tx(
        &[
            "INSERT INTO tx VALUES (3, 30)",
            "BEGIN; DROP TABLE tx; ROLLBACK",
        ],
        &["--write"],
    )?;
    assert_ok(&out);
    assert_eq!(file, "k,n\n1,10\n2,20\n3,30\n");
    Ok(())
}

#[test]
fn transactions_in_sequence() -> R {
    let (out, file) = run_tx(
        &[
            "BEGIN; INSERT INTO tx VALUES (3, 30); COMMIT",
            "BEGIN; UPDATE tx SET n = 0; ROLLBACK",
            "BEGIN IMMEDIATE TRANSACTION; DELETE FROM tx WHERE k = 1; END TRANSACTION",
            "SELECT * FROM tx",
        ],
        &["--write"],
    )?;
    assert_ok(&out);
    assert_eq!(stdout(&out), "k,n\n2,20\n3,30\n");
    assert_eq!(file, "k,n\n2,20\n3,30\n");
    Ok(())
}

#[test]
fn begin_modifiers_are_plain_begin() -> R {
    for begin in [
        "BEGIN DEFERRED",
        "BEGIN IMMEDIATE",
        "BEGIN EXCLUSIVE TRANSACTION",
    ] {
        let (out, _) = run_tx(
            &[begin, "UPDATE tx SET n = 0", "ROLLBACK", "SELECT n FROM tx"],
            &[],
        )?;
        assert_ok(&out);
        assert_eq!(stdout(&out), "n\n10\n20\n", "{begin}");
    }
    Ok(())
}

/// A transaction may span `-f` files and `-s` arguments.
#[test]
fn transaction_spans_script_files() -> R {
    let dir = create_temp_dir()?;
    fs::write(dir.path().join("tx.csv"), TX)?;
    fs::write(dir.path().join("a.sql"), "BEGIN;\nUPDATE tx SET n = 0;\n")?;
    let out = run(
        dir.path(),
        &[
            "-f",
            "a.sql",
            "-s",
            "ROLLBACK; SELECT n FROM tx",
            "-w",
            "tx.csv",
        ],
        "",
    )?;
    assert_ok(&out);
    assert_eq!(stdout(&out), "n\n10\n20\n");
    assert_eq!(fs::read_to_string(dir.path().join("tx.csv"))?, TX);
    Ok(())
}

/// Under -v the rollback of a transaction left open is reported.
#[test]
fn open_transaction_rollback_is_reported_verbose() -> R {
    let (out, file) = run_tx(&["BEGIN", "UPDATE tx SET n = 0"], &["-v", "-w"])?;
    assert_ok(&out);
    assert!(
        stderr(&out).contains("Transaction still open at exit: rolled back"),
        "stderr: {}",
        stderr(&out)
    );
    assert_eq!(file, TX);
    Ok(())
}

/// REPL with `.write on`: nothing is saved inside a transaction; `.save`
/// and `.load` are refused there; COMMIT saves.
#[test]
fn repl_write_mode_waits_for_commit() -> R {
    let dir = create_temp_dir()?;
    fs::write(dir.path().join("tx.csv"), TX)?;
    fs::write(dir.path().join("other.csv"), "a\n1\n")?;
    let script = ".write on\nBEGIN;\nUPDATE tx SET n = 7;\n.save\n.load other.csv\n\
                  ROLLBACK;\nBEGIN;\nUPDATE tx SET n = 8 WHERE k = 2;\nCOMMIT;\n";
    let out = run(dir.path(), &["-i", "tx.csv"], script)?;
    let err = stderr(&out);
    assert!(err.contains("cannot save within a transaction"), "{err}");
    assert!(
        err.contains("cannot load a file within a transaction"),
        "{err}"
    );
    assert_eq!(
        fs::read_to_string(dir.path().join("tx.csv"))?,
        "k,n\n1,10\n2,8\n"
    );
    Ok(())
}

/// A REPL session that ends inside a transaction discards it, even with
/// `.write on`.
#[test]
fn repl_exit_discards_open_transaction() -> R {
    let dir = create_temp_dir()?;
    fs::write(dir.path().join("tx.csv"), TX)?;
    let out = run(
        dir.path(),
        &["-i", "tx.csv"],
        ".write on\nBEGIN;\nUPDATE tx SET n = 9;\n.quit\n",
    )?;
    assert!(out.status.success(), "{}", stderr(&out));
    assert_eq!(fs::read_to_string(dir.path().join("tx.csv"))?, TX);
    Ok(())
}

/// A failed statement inside a REPL transaction leaves the transaction
/// open, as in SQLite; ROLLBACK then undoes the earlier change.
#[test]
fn repl_error_keeps_transaction_open() -> R {
    let dir = create_temp_dir()?;
    fs::write(dir.path().join("tx.csv"), TX)?;
    let out = run(
        dir.path(),
        &["-i", "tx.csv"],
        "BEGIN;\nUPDATE tx SET n = 0;\nSELECT nosuch FROM tx;\nROLLBACK;\nSELECT n FROM tx;\n",
    )?;
    assert!(stdout(&out).contains("n\n10\n20\n"), "{}", stdout(&out));
    assert!(stderr(&out).contains("no such column: nosuch"));
    Ok(())
}

// ---------------------------------------------------------------------------
// Writeback rules
// ---------------------------------------------------------------------------

/// CREATE TABLE ... AS makes a scratch table: skipped, not an error.
#[test]
fn write_skips_ctas_table() -> R {
    let (out, file) = run_tx(
        &[
            "CREATE TABLE s AS SELECT k FROM tx",
            "INSERT INTO s VALUES (9)",
            "INSERT INTO tx VALUES (3, 30)",
        ],
        &["-w"],
    )?;
    assert_ok(&out);
    assert_eq!(file, "k,n\n1,10\n2,20\n3,30\n");
    Ok(())
}

/// A table modified and dropped in the same `-s` is not written.
#[test]
fn write_after_drop_in_one_script() -> R {
    let (out, file) = run_tx(&["INSERT INTO tx VALUES (3, 30); DROP TABLE tx"], &["-w"])?;
    assert_ok(&out);
    assert_eq!(file, TX);
    Ok(())
}

/// A table read from standard input still cannot be written back.
#[test]
fn write_of_stdin_table_is_error() -> R {
    let dir = create_temp_dir()?;
    let out = run(
        dir.path(),
        &["-w", "-s", "INSERT INTO stdin VALUES (2)", "-"],
        "x\n1\n",
    )?;
    assert_error(&out, "standard input");
    Ok(())
}

#[cfg(unix)]
fn perms_enforced(dir: &Path) -> bool {
    let p = dir.join(".perm_probe");
    fs::write(&p, "x").unwrap();
    chmod(&p, 0o000);
    let enforced = fs::File::open(&p).is_err();
    fs::remove_file(&p).unwrap();
    enforced
}

#[cfg(unix)]
fn chmod(p: &Path, mode: u32) {
    let mut perm = fs::metadata(p).unwrap().permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut perm, mode);
    fs::set_permissions(p, perm).unwrap();
}

/// A file in a directory that cannot be written is refused before any
/// file -- including a writable one sorted first -- is written.
#[cfg(unix)]
#[test]
fn write_into_read_only_directory_is_refused_up_front() -> R {
    let dir = create_temp_dir()?;
    if !perms_enforced(dir.path()) {
        return Ok(());
    }
    fs::write(dir.path().join("a.csv"), "x\n1\n")?;
    fs::create_dir(dir.path().join("rod"))?;
    fs::write(dir.path().join("rod/z.csv"), "x\n1\n")?;
    chmod(&dir.path().join("rod"), 0o555);
    let out = run(
        dir.path(),
        &[
            "-w",
            "-s",
            "INSERT INTO a VALUES (2)",
            "-s",
            "INSERT INTO z VALUES (2)",
            "a.csv",
            "rod/z.csv",
        ],
        "",
    );
    chmod(&dir.path().join("rod"), 0o755);
    let out = out?;
    assert_error(&out, "rod/z.csv");
    assert_eq!(fs::read_to_string(dir.path().join("a.csv"))?, "x\n1\n");
    assert_eq!(fs::read_to_string(dir.path().join("rod/z.csv"))?, "x\n1\n");
    Ok(())
}

/// A read-only file that was not modified does not block writeback.
#[cfg(unix)]
#[test]
fn unmodified_read_only_file_is_fine() -> R {
    let dir = create_temp_dir()?;
    fs::write(dir.path().join("rw.csv"), "x\n1\n")?;
    fs::write(dir.path().join("ro.csv"), "x\n1\n")?;
    chmod(&dir.path().join("ro.csv"), 0o444);
    let out = run(
        dir.path(),
        &["-w", "-s", "INSERT INTO rw VALUES (2)", "rw.csv", "ro.csv"],
        "",
    );
    chmod(&dir.path().join("ro.csv"), 0o644);
    assert_ok(&out?);
    assert_eq!(fs::read_to_string(dir.path().join("rw.csv"))?, "x\n1\n2\n");
    Ok(())
}

/// Writeback replaces the file (atomic rename), so a hard link to it keeps
/// the old contents.
#[test]
fn write_splits_hard_link() -> R {
    let dir = create_temp_dir()?;
    fs::write(dir.path().join("tx.csv"), TX)?;
    fs::hard_link(dir.path().join("tx.csv"), dir.path().join("link.csv"))?;
    let out = run(
        dir.path(),
        &["-w", "-s", "UPDATE tx SET n = 0", "tx.csv"],
        "",
    )?;
    assert_ok(&out);
    assert_eq!(
        fs::read_to_string(dir.path().join("tx.csv"))?,
        "k,n\n1,0\n2,0\n"
    );
    assert_eq!(fs::read_to_string(dir.path().join("link.csv"))?, TX);
    Ok(())
}

/// REPL `.save` of a read-only file is refused too.
#[cfg(unix)]
#[test]
fn repl_save_of_read_only_file_is_refused() -> R {
    let dir = create_temp_dir()?;
    if !perms_enforced(dir.path()) {
        return Ok(());
    }
    let ro = dir.path().join("ro.csv");
    fs::write(&ro, "a\n1\n")?;
    chmod(&ro, 0o444);
    let out = run(
        dir.path(),
        &["-i", "ro.csv"],
        "INSERT INTO ro VALUES (2);\n.save ro\n",
    );
    chmod(&ro, 0o644);
    let err = stderr(&out?);
    assert!(
        err.contains("cannot write 'ro.csv': file is read-only"),
        "{err}"
    );
    assert_eq!(fs::read_to_string(&ro)?, "a\n1\n");
    Ok(())
}

// ---------------------------------------------------------------------------
// --tabledef validation
// ---------------------------------------------------------------------------

#[test]
fn tabledef_errors() -> R {
    for (defs, needle) in [
        (&["--tabledef=:a,b"][..], "empty table name"),
        (&["--tabledef=tx:a,A"][..], "duplicate column name: A"),
        (&["--tabledef=tx:a,,b"][..], "empty column name"),
        (
            &["--tabledef=tx:a,b", "--tabledef=tx:c,d"][..],
            "defined twice",
        ),
        (&["--tabledef=zz:a"][..], "no input file loads"),
    ] {
        let (out, _) = run_tx(&["SELECT * FROM tx"], defs)?;
        assert_error(&out, needle);
    }
    Ok(())
}

/// The REPL applies the same check at startup.
#[test]
fn tabledef_unknown_table_in_repl_is_error() -> R {
    let dir = create_temp_dir()?;
    fs::write(dir.path().join("tx.csv"), TX)?;
    let out = run(
        dir.path(),
        &["-i", "--tabledef=zz:a", "tx.csv"],
        "SELECT 1;\n",
    )?;
    assert_error(
        &out,
        "--tabledef names table 'zz', which no input file loads",
    );
    Ok(())
}
