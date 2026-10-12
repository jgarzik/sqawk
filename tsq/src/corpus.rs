//! `tsq corpus`: hand-written test files, judged by the same oracle and
//! renderer as `tsq diff`.
//!
//! Format (from the retired Python harness, harness.py): one test per line; blank
//! lines and lines starting with `--` are skipped; a line prefixed `O:` is
//! compared in order, any other as a multiset of rows. A line may hold
//! several `;`-separated statements: both engines run all of them and every
//! result set is compared (header included). A comment line
//! `-- tsq-tables: NAME` selects the built-in table set the file runs
//! against (`harness` when absent; `--tables` overrides).

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context, Result};

use crate::diff::oracle::{split_statements, Oracle};
use crate::diff::{describe, run_all, tables, Cat, Env, Script, Tally};

pub struct CorpusOptions {
    pub files: Vec<PathBuf>,
    pub tables: Option<String>,
    pub jobs: usize,
    pub timeout: Duration,
    pub keep_going: bool,
}

pub struct Test {
    pub file: String,
    pub line: usize,
    pub ordered: bool,
    pub sql: String,
}

/// The tests in `text` and the table set it asks for.
pub fn parse(file: &str, text: &str) -> (Vec<Test>, Option<String>) {
    let mut tests = Vec::new();
    let mut set = None;
    for (n, line) in text.lines().enumerate() {
        let q = line.trim();
        if let Some(rest) = q.strip_prefix("-- tsq-tables:") {
            set = Some(rest.trim().to_string());
            continue;
        }
        if q.is_empty() || q.starts_with("--") {
            continue;
        }
        let (ordered, sql) = match q.strip_prefix("O:") {
            Some(r) => (true, r.trim()),
            None => (false, q),
        };
        tests.push(Test {
            file: file.to_string(),
            line: n + 1,
            ordered,
            sql: sql.to_string(),
        });
    }
    (tests, set)
}

pub fn run(sqawk: &Path, opts: &CorpusOptions, verbose: bool) -> Result<i32> {
    let mut groups: Vec<(String, Vec<Test>)> = Vec::new();
    for f in &opts.files {
        let text = fs::read_to_string(f).with_context(|| format!("read {}", f.display()))?;
        let name = f
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let (tests, set) = parse(&name, &text);
        let set = opts
            .tables
            .clone()
            .or(set)
            .unwrap_or_else(|| "harness".to_string());
        match groups.iter_mut().find(|(s, _)| *s == set) {
            Some((_, v)) => v.extend(tests),
            None => groups.push((set, tests)),
        }
    }
    let mut tally = Tally::default();
    for (set, tests) in &groups {
        let Some(tabs) = tables::builtin(set) else {
            bail!("unknown table set {set:?} (harness, edge)");
        };
        let tmp = tempfile::Builder::new().prefix("tsq-corpus-").tempdir()?;
        crate::gen::write_csv_tables(tmp.path(), &tabs)?;
        let oracle = Oracle::new(&tabs)?;
        let schema = tables::schema_of(&tabs, &oracle)?;
        let env = Env::new(sqawk, tmp.path(), &tabs, &schema, opts.timeout);
        let scripts: Vec<Script> = tests
            .iter()
            .map(|t| {
                let stmts = split_statements(&t.sql);
                let n = stmts.len();
                (t.sql.clone(), stmts, vec![t.ordered; n])
            })
            .collect();
        let results = run_all(&env, &tabs, &scripts, opts.jobs, opts.keep_going, verbose);
        for (t, v) in tests.iter().zip(&results) {
            let Some(v) = v else { continue };
            tally.add(v);
            if v.cat != Cat::Pass && (v.cat != Cat::BothErr || verbose) {
                let label = format!("{}:{}", t.file, t.line);
                print!("{}", describe(&label, &t.sql, v, t.ordered));
            }
        }
    }
    println!("{}", tally.line("CORPUS SUMMARY"));
    Ok(if tally.findings > 0 { 1 } else { 0 })
}
