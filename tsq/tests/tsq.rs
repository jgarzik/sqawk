//! Tests for `tsq`, the test-data generator, differential tester and fuzzer.
//!
//! `tsq` is a local development tool (a `publish = false` workspace member),
//! not part of the published sqawk crate. These tests cover what makes it
//! useful: `gen` is deterministic (every output file), its foreign keys
//! hold, and sqawk reads every layout it writes; `diff`, `corpus`, `wb` and
//! `cli` run clean against the current sqawk (small budgets, so `cargo test
//! --workspace` stays fast; `stress` is not run here).

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

/// The sqawk binary from the shared workspace target directory.
///
/// sqawk is a different package, so `cargo_bin!` cannot name it. A test
/// executable lives in `target/<profile>/deps/`, and the workspace's binaries
/// in `target/<profile>/`. `cargo test --workspace` builds both packages
/// before running any test.
fn sqawk_path() -> PathBuf {
    let exe = std::env::current_exe().expect("current test executable");
    let profile_dir = exe
        .parent()
        .and_then(Path::parent)
        .expect("test executable is in target/<profile>/deps");
    let bin = profile_dir.join(format!("sqawk{}", std::env::consts::EXE_SUFFIX));
    assert!(
        bin.exists(),
        "{} not found: run `cargo test --workspace`, or build sqawk first",
        bin.display()
    );
    bin
}

fn sqawk() -> Command {
    Command::new(sqawk_path())
}

fn tsq() -> assert_cmd::Command {
    let mut c = assert_cmd::cargo::cargo_bin_cmd!("tsq");
    c.arg("--sqawk").arg(sqawk_path());
    c
}

fn ok(out: Output, what: &str) -> String {
    assert!(
        out.status.success(),
        "{what} failed ({}):\nstdout: {}\nstderr: {}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// Run `tsq gen` into a fresh directory with extra arguments.
fn generate(seed: u64, rows: usize, extra: &[&str]) -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("temp dir");
    let out = tsq()
        .args([
            "gen",
            "--seed",
            &seed.to_string(),
            "--rows",
            &rows.to_string(),
        ])
        .arg("--output-dir")
        .arg(dir.path())
        .args(extra)
        .output()
        .expect("run tsq");
    ok(out, "tsq gen");
    dir
}

/// Every file under `dir`, relative path -> bytes, sorted.
fn tree(dir: &Path) -> Vec<(String, Vec<u8>)> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in fs::read_dir(&d).expect("read dir") {
            let p = e.expect("dir entry").path();
            if p.is_dir() {
                stack.push(p);
            } else {
                let rel = p
                    .strip_prefix(dir)
                    .unwrap()
                    .to_string_lossy()
                    .replace('\\', "/");
                out.push((rel, fs::read(&p).expect("read file")));
            }
        }
    }
    out.sort();
    out
}

/// sqawk's complete stdout for `sql` over `files`.
fn query(args: &[&str], sql: &str, files: &[PathBuf]) -> String {
    let out = sqawk()
        .args(args)
        .arg("-s")
        .arg(sql)
        .args(files)
        .output()
        .expect("run sqawk");
    ok(out, sql)
}

const TABLES: [&str; 5] = ["customers", "products", "orders", "order_items", "reviews"];

/// Row counts for `rows` customers (see tsq/src/gen.rs).
fn counts(rows: usize) -> [usize; 5] {
    [
        rows,
        (rows / 100).max(rows.min(100)),
        rows * 3,
        rows * 10,
        (rows / 2).max(1),
    ]
}

#[test]
fn same_seed_reproduces_every_file() {
    // The whole point of the seed: data and metadata, byte for byte, with
    // edge values, layout options and a seed above 2^32 (the old generator
    // truncated it).
    let opts = ["--edge-rate", "0.1", "--ragged-rate", "0.3", "--crlf"];
    let a = generate(18_446_744_073_709_551_557, 25, &opts);
    let b = generate(18_446_744_073_709_551_557, 25, &opts);
    let (ta, tb) = (tree(a.path()), tree(b.path()));
    assert_eq!(
        ta.len(),
        6,
        "five tables and metadata.json: {:?}",
        ta.iter().map(|t| &t.0).collect::<Vec<_>>()
    );
    assert_eq!(ta, tb, "two runs with the same seed differ");
}

#[test]
fn different_seeds_produce_different_data() {
    let a = generate(1, 25, &[]);
    let b = generate(2, 25, &[]);
    for t in TABLES {
        let rel = format!("data/{t}.csv");
        assert_ne!(
            fs::read(a.path().join(&rel)).unwrap(),
            fs::read(b.path().join(&rel)).unwrap(),
            "{rel}: different seeds gave identical data"
        );
    }
}

#[test]
fn metadata_records_seed_rows_and_options() {
    let dir = generate(99, 30, &["--format", "tsv", "--bom"]);
    let meta = fs::read_to_string(dir.path().join("metadata.json")).unwrap();
    for want in [
        "\"seed\": 99",
        "\"rows\": 30",
        "\"format\": \"tsv\"",
        "\"bom\": true",
        "\"customers\": 30",
        "\"order_items\": 300",
    ] {
        assert!(meta.contains(want), "metadata lacks {want}: {meta}");
    }
}

#[test]
fn foreign_keys_hold() {
    // Every reference points at an existing row, so joins are meaningful.
    let dir = generate(11, 50, &[]);
    let f = |t: &str| dir.path().join(format!("data/{t}.csv"));
    let files: Vec<PathBuf> = TABLES.iter().map(|t| f(t)).collect();
    let checks = [
        "SELECT COUNT(*) FROM orders o LEFT JOIN customers c ON o.customer_id = c.customer_id WHERE c.customer_id IS NULL",
        "SELECT COUNT(*) FROM order_items i LEFT JOIN orders o ON i.order_id = o.order_id WHERE o.order_id IS NULL",
        "SELECT COUNT(*) FROM order_items i LEFT JOIN products p ON i.product_id = p.product_id WHERE p.product_id IS NULL",
        "SELECT COUNT(*) FROM reviews r LEFT JOIN customers c ON r.customer_id = c.customer_id WHERE c.customer_id IS NULL",
        "SELECT COUNT(*) FROM reviews r LEFT JOIN products p ON r.product_id = p.product_id WHERE p.product_id IS NULL",
        "SELECT COUNT(*) FROM customers c LEFT JOIN customers p ON c.referred_by = p.customer_id WHERE c.referred_by IS NOT NULL AND p.customer_id IS NULL",
    ];
    for sql in checks {
        assert_eq!(
            query(&[], sql, &files),
            "COUNT(*)\n0\n",
            "dangling reference: {sql}"
        );
    }
}

#[test]
fn sqawk_reads_every_layout() {
    // Each layout must load with the exact row count of every table.
    let variants: [(&[&str], &[&str], &str); 7] = [
        (&[], &[], "csv"),
        (&["--format", "tsv"], &[], "tsv"),
        (&["--crlf"], &[], "csv"),
        (&["--bom"], &[], "csv"),
        (&["--no-header"], &["--no-header"], "csv"),
        (&["--ragged-rate", "0.5"], &[], "csv"),
        (
            &["--edge-rate", "0.3", "--crlf", "--bom", "--format", "tsv"],
            &[],
            "tsv",
        ),
    ];
    for (gen_args, sqawk_args, ext) in variants {
        let dir = generate(7, 20, gen_args);
        for (t, n) in TABLES.iter().zip(counts(20)) {
            let file = dir.path().join(format!("data/{t}.{ext}"));
            let got = query(sqawk_args, &format!("SELECT COUNT(*) FROM {t}"), &[file]);
            assert_eq!(got, format!("COUNT(*)\n{n}\n"), "{t} with {gen_args:?}");
        }
    }
}

#[test]
fn diff_smoke_runs_clean() {
    // A small random run over generated tables and over the adversarial
    // set: any finding (wrong answer, unexpected error, crash) fails.
    for args in [
        &["diff", "--seed", "1", "--queries", "300"][..],
        &[
            "diff",
            "--seed",
            "1",
            "--queries",
            "150",
            "--tables",
            "edge",
        ][..],
    ] {
        let out = ok(tsq().args(args).output().expect("run tsq"), "tsq diff");
        let last = out.lines().last().unwrap_or("");
        assert!(
            last.starts_with("DIFF SUMMARY") && last.ends_with("findings 0"),
            "{args:?}:\n{out}"
        );
    }
}

/// The last line of a tsq run that must pass.
fn summary(args: &[&str]) -> String {
    let out = tsq().args(args).output().expect("run tsq");
    let text = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(
        out.status.success(),
        "tsq {args:?} failed ({}):\n{text}\nstderr: {}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    text.lines().last().unwrap_or("").to_string()
}

#[test]
fn wb_smoke_runs_clean() {
    // Random files and DML with --write: every file equals the oracle's
    // rendering, nothing stray is left (and, on Unix, killed runs leave the
    // old or the new file).
    let last = summary(&["wb", "--seed", "1", "--iterations", "50"]);
    assert!(
        last.starts_with("WB SUMMARY {ok: 50, bad: 0, crash: 0}"),
        "{last}"
    );
}

#[test]
fn cli_smoke_runs_clean() {
    // Random flags, operands, REPL scripts, malformed files, garbage and
    // huge SQL, closed stdout: clean exits and one-line errors only.
    let last = summary(&["cli", "--seed", "1", "--iterations", "100"]);
    assert_eq!(last, "CLI SUMMARY {ok: 100, bad: 0, crash: 0}");
}

#[test]
fn corpus_counts_match_the_python_harness() {
    // The same files gave these numbers under the retired Python harness
    // (harness.py / diffh.py); every non-pass is a documented difference.
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("corpus");
    let run = |files: &[&str]| {
        let out = tsq()
            .arg("corpus")
            .args(files.iter().map(|f| dir.join(f)))
            .output()
            .expect("run tsq");
        let s = ok(out, "tsq corpus");
        s.lines().last().unwrap_or("").to_string()
    };
    assert_eq!(
        run(&["queries.sql", "queries2.sql"]),
        "CORPUS SUMMARY {pass: 544, diff: 3, sqawk_err: 9, sqlite_err: 1, both_err: 7, crash: 0} \
         documented {column_types: 2, insert_or_replace: 1, parser: 3, sqawk_extensions: 1, \
         unsupported_syntax: 5, upper_lower: 1} oracle_timeouts 0 findings 0"
    );
    assert_eq!(
        run(&["hand1.sql", "hand2.sql", "hand3.sql"]),
        "CORPUS SUMMARY {pass: 359, diff: 13, sqawk_err: 15, sqlite_err: 0, both_err: 36, crash: 0} \
         documented {bare_column: 7, column_types: 9, derived_correlated: 1, parser: 5, \
         sum_avg_text: 2, unsupported_syntax: 2, update_snapshot: 1, upper_lower: 1} \
         oracle_timeouts 0 findings 0"
    );
}
