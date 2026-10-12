//! `tsq diff` and the machinery `tsq corpus` shares: run a statement
//! sequence through sqawk and through the SQLite oracle, compare every
//! result set byte for byte, classify what does not pass.
//!
//! Categories (the `DIFF SUMMARY` line):
//!
//! * `pass`: identical result sets (headers included; rows in order when
//!   the statement's ORDER BY fixes the order, else as a multiset);
//! * `diff`: both ran, the output differs;
//! * `sqawk_err`: sqawk failed where SQLite ran it;
//! * `sqlite_err`: SQLite failed where sqawk ran it;
//! * `both_err`: both rejected it (an agreeing error);
//! * `crash`: sqawk panicked, died on a signal, exited abnormally or hit
//!   the time limit -- always a finding.
//!
//! A diff / sqawk_err / sqlite_err that a rule in `allow` covers is
//! *documented*; any other is a *finding*.

pub mod allow;
pub mod ast;
pub mod oracle;
pub mod render;
pub mod schema;
pub mod shrink;
pub mod sqlgen;
pub mod tables;

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};

use crate::data::Table;
use crate::run::{run_sqawk, Exit};
use oracle::{Oracle, SqlResult};
use render::{raw_records, same_rows, split_sets, ResultSet};
use schema::{Kind, Schema};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Cat {
    Pass,
    Diff,
    SqawkErr,
    SqliteErr,
    BothErr,
    Crash,
}

impl Cat {
    pub fn name(self) -> &'static str {
        match self {
            Cat::Pass => "pass",
            Cat::Diff => "diff",
            Cat::SqawkErr => "sqawk_err",
            Cat::SqliteErr => "sqlite_err",
            Cat::BothErr => "both_err",
            Cat::Crash => "crash",
        }
    }
}

/// Where and how statements run.
pub struct Env {
    pub sqawk: PathBuf,
    /// Directory holding `<table>.csv` for every table.
    pub dir: PathBuf,
    pub tables: Vec<String>,
    pub timeout: Duration,
    pub numtext_cols: BTreeSet<String>,
    pub text_cols: BTreeSet<String>,
    /// Scripts come from the random generator (see `allow::Ctx`).
    pub generated: bool,
}

impl Env {
    pub fn new(
        sqawk: &Path,
        dir: &Path,
        tables: &[Table],
        schema: &Schema,
        timeout: Duration,
    ) -> Self {
        let mut numtext_cols = BTreeSet::new();
        let mut text_cols = BTreeSet::new();
        for r in &schema.rels {
            for c in &r.cols {
                match c.props.kind {
                    Kind::Mixed => {
                        numtext_cols.insert(c.name.clone());
                        text_cols.insert(c.name.clone());
                    }
                    Kind::Text => {
                        text_cols.insert(c.name.clone());
                        if !c.props.nonnum {
                            numtext_cols.insert(c.name.clone());
                        }
                    }
                    _ => {}
                }
            }
        }
        Env {
            sqawk: sqawk.to_path_buf(),
            dir: dir.to_path_buf(),
            tables: tables.iter().map(|t| t.name.clone()).collect(),
            timeout,
            numtext_cols,
            text_cols,
            generated: false,
        }
    }

    /// The table files a script names (whole-word, any case), else the
    /// first table: sqawk needs at least one file.
    pub fn files_for(&self, sql: &str) -> Vec<PathBuf> {
        let mut words = BTreeSet::new();
        let mut cur = String::new();
        for c in sql.chars().chain(std::iter::once(' ')) {
            if c.is_alphanumeric() || c == '_' {
                cur.push(c.to_ascii_lowercase());
            } else if !cur.is_empty() {
                words.insert(std::mem::take(&mut cur));
            }
        }
        let mut files: Vec<PathBuf> = self
            .tables
            .iter()
            .filter(|t| words.contains(&t.to_ascii_lowercase()))
            .map(|t| self.dir.join(format!("{t}.csv")))
            .collect();
        if files.is_empty() {
            if let Some(t) = self.tables.first() {
                files.push(self.dir.join(format!("{t}.csv")));
            }
        }
        files
    }
}

#[derive(Clone, Debug)]
pub struct Verdict {
    pub cat: Cat,
    /// The documented difference that covers a non-pass, if any.
    pub rule: Option<&'static str>,
    pub sqawk_msg: String,
    pub sqlite_msg: String,
    /// sqawk's result sets (shaped like SQLite's when the counts fit).
    pub sqawk_sets: Vec<ResultSet>,
    pub sqlite_sets: Vec<ResultSet>,
    pub sqawk_stdout: String,
    pub elapsed: Duration,
}

impl Verdict {
    pub fn is_finding(&self) -> bool {
        match self.cat {
            Cat::Crash => true,
            Cat::SqliteErr if self.oracle_timeout() => false,
            Cat::Diff | Cat::SqawkErr | Cat::SqliteErr => self.rule.is_none(),
            _ => false,
        }
    }

    /// SQLite ran out of time: the generator made a query too heavy for the
    /// reference, which says nothing about sqawk (counted, not reported).
    pub fn oracle_timeout(&self) -> bool {
        self.sqlite_msg.starts_with("timeout after")
    }

    /// Is `other` the same failure (for shrinking)?
    pub fn same_failure(&self, other: &Verdict) -> bool {
        if self.cat != other.cat || self.rule != other.rule {
            return false;
        }
        match self.cat {
            Cat::SqawkErr => self.sqawk_msg == other.sqawk_msg,
            Cat::SqliteErr => self.sqlite_msg == other.sqlite_msg,
            _ => true,
        }
    }
}

/// Run `stmts` (the script sqawk reads is `script`) in both engines and
/// classify. `ordered[i]` says whether statement i's rows compare in order.
pub fn judge(
    env: &Env,
    oracle: &Oracle,
    script: &str,
    stmts: &[String],
    ordered: &[bool],
) -> Verdict {
    let files = env.files_for(script);
    let sk = run_sqawk(&env.sqawk, &[], script, &files, env.timeout);
    let sl = oracle.run(stmts, env.timeout);
    let mut v = Verdict {
        cat: Cat::Pass,
        rule: None,
        sqawk_msg: sk.message(),
        sqlite_msg: String::new(),
        sqawk_sets: Vec::new(),
        sqlite_sets: Vec::new(),
        sqawk_stdout: sk.stdout.clone(),
        elapsed: sk.elapsed,
    };
    if let SqlResult::Err(e) = &sl {
        v.sqlite_msg = e.clone();
    }
    v.cat = match (&sk.exit, &sl) {
        // Both out of time: the query is too heavy for either engine.
        (Exit::Crash, SqlResult::Err(e))
            if sk.stderr.contains("timeout after") && e.starts_with("timeout after") =>
        {
            Cat::BothErr
        }
        (Exit::Crash, _) => Cat::Crash,
        (Exit::Err, SqlResult::Err(_)) => Cat::BothErr,
        (Exit::Err, SqlResult::Ok(sets)) => {
            v.sqlite_sets = sets.clone();
            Cat::SqawkErr
        }
        (Exit::Ok, SqlResult::Err(_)) => {
            v.sqawk_sets = vec![ResultSet {
                header: String::new(),
                rows: raw_records(&sk.stdout),
                source: 0,
            }];
            Cat::SqliteErr
        }
        (Exit::Ok, SqlResult::Ok(sets)) => {
            v.sqlite_sets = sets.clone();
            let recs = raw_records(&sk.stdout);
            match split_sets(&recs, sets) {
                Some(got) => {
                    let ok = got.iter().zip(sets).all(|(a, b)| {
                        let ord = ordered.get(b.source).copied().unwrap_or(false);
                        a.header == b.header && same_rows(ord, &a.rows, &b.rows)
                    });
                    v.sqawk_sets = got;
                    if ok {
                        Cat::Pass
                    } else {
                        Cat::Diff
                    }
                }
                None => {
                    v.sqawk_sets = vec![ResultSet {
                        header: String::new(),
                        rows: recs,
                        source: 0,
                    }];
                    Cat::Diff
                }
            }
        }
    };
    let outcome = match v.cat {
        Cat::Diff => Some(allow::Outcome::Diff),
        Cat::SqawkErr => Some(allow::Outcome::SqawkErr),
        Cat::SqliteErr => Some(allow::Outcome::SqliteErr),
        _ => None,
    };
    if let Some(outcome) = outcome {
        let ctx = allow::Ctx {
            sql: script,
            outcome,
            sqawk_msg: &v.sqawk_msg,
            sqawk_sets: &v.sqawk_sets,
            sqlite_sets: &v.sqlite_sets,
            numtext_cols: &env.numtext_cols,
            text_cols: &env.text_cols,
            generated: env.generated,
        };
        v.rule = allow::classify(&ctx).map(|r| r.name);
    }
    v
}

fn show_rows(sets: &[ResultSet], ordered: bool) -> String {
    let mut rows: Vec<String> = Vec::new();
    for s in sets {
        if !s.header.is_empty() {
            rows.push(format!("[{}]", s.header));
        }
        let mut r = s.rows.clone();
        if !ordered {
            r.sort();
        }
        rows.extend(r);
    }
    let n = rows.len();
    let mut out: Vec<String> = rows
        .into_iter()
        .take(12)
        .map(|r| format!("{r:?}"))
        .collect();
    if n > 12 {
        out.push(format!("... {} more", n - 12));
    }
    out.join(" ")
}

/// A human-readable report of one non-passing result.
pub fn describe(label: &str, script: &str, v: &Verdict, ordered: bool) -> String {
    let what = match v.rule {
        Some(r) => format!("DOCUMENTED({r}: {}) {}", allow::doc_of(r), v.cat.name()),
        None => v.cat.name().to_uppercase(),
    };
    let sql = script.trim().replace('\n', " ");
    let mut s = format!(
        "{what}{} [{label}]: {sql}\n",
        if ordered { "(ordered)" } else { "" }
    );
    match v.cat {
        Cat::Diff => {
            s.push_str(&format!(
                "   sqawk : {}\n",
                show_rows(&v.sqawk_sets, ordered)
            ));
            s.push_str(&format!(
                "   sqlite: {}\n",
                show_rows(&v.sqlite_sets, ordered)
            ));
        }
        Cat::SqawkErr => {
            s.push_str(&format!("   sqawk : {}\n", v.sqawk_msg));
            s.push_str(&format!(
                "   sqlite: {}\n",
                show_rows(&v.sqlite_sets, ordered)
            ));
        }
        Cat::SqliteErr => {
            s.push_str(&format!("   sqlite: {}\n", v.sqlite_msg));
            s.push_str(&format!(
                "   sqawk : {}\n",
                show_rows(&v.sqawk_sets, ordered)
            ));
        }
        Cat::Crash => s.push_str(&format!("   sqawk : {}\n", v.sqawk_msg)),
        Cat::BothErr => s.push_str(&format!(
            "   sqawk : {}\n   sqlite: {}\n",
            v.sqawk_msg, v.sqlite_msg
        )),
        Cat::Pass => {}
    }
    s
}

/// Counts per category plus documented rules, and the summary line.
#[derive(Default)]
pub struct Tally {
    pub cats: BTreeMap<Cat, usize>,
    pub documented: BTreeMap<&'static str, usize>,
    pub findings: usize,
    pub oracle_timeouts: usize,
}

impl Tally {
    pub fn add(&mut self, v: &Verdict) {
        *self.cats.entry(v.cat).or_default() += 1;
        if let Some(r) = v.rule {
            *self.documented.entry(r).or_default() += 1;
        }
        if v.is_finding() {
            self.findings += 1;
        }
        if matches!(v.cat, Cat::SqliteErr | Cat::BothErr) && v.oracle_timeout() {
            self.oracle_timeouts += 1;
        }
    }

    pub fn get(&self, c: Cat) -> usize {
        self.cats.get(&c).copied().unwrap_or(0)
    }

    pub fn line(&self, title: &str) -> String {
        let cats = [
            Cat::Pass,
            Cat::Diff,
            Cat::SqawkErr,
            Cat::SqliteErr,
            Cat::BothErr,
            Cat::Crash,
        ];
        let body: Vec<String> = cats
            .iter()
            .map(|c| format!("{}: {}", c.name(), self.get(*c)))
            .collect();
        let doc: Vec<String> = self
            .documented
            .iter()
            .map(|(k, v)| format!("{k}: {v}"))
            .collect();
        format!(
            "{title} {{{}}} documented {{{}}} oracle_timeouts {} findings {}",
            body.join(", "),
            doc.join(", "),
            self.oracle_timeouts,
            self.findings
        )
    }
}

// ------------------------------------------------------------------ tsq diff

#[derive(Clone, Copy, Debug)]
pub enum Scale {
    Rows(usize),
}

impl std::str::FromStr for Scale {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "small" => Ok(Scale::Rows(12)),
            "large" => Ok(Scale::Rows(100_000)),
            n => n
                .parse::<usize>()
                .map(Scale::Rows)
                .map_err(|_| format!("--scale takes small, large or a row count, not {n:?}")),
        }
    }
}

pub struct DiffOptions {
    pub queries: usize,
    pub scale: Scale,
    /// `gen` (tsq gen's tables) or a built-in set (`edge`, `harness`).
    pub tables: String,
    pub edge_rate: f64,
    pub keep_going: bool,
    pub shrink: bool,
    pub out: Option<PathBuf>,
    pub jobs: usize,
    pub timeout: Duration,
    /// Run only test number `only` (its stream does not depend on the
    /// others).
    pub only: Option<usize>,
    /// Print each generated script instead of running it.
    pub print: bool,
}

/// The tables a diff run uses.
pub fn diff_tables(seed: u64, opts: &DiffOptions) -> Result<(Vec<Table>, bool)> {
    let Scale::Rows(rows) = opts.scale;
    match opts.tables.as_str() {
        "gen" => {
            let mut t = crate::gen::base_tables(seed, rows);
            crate::gen::inject_edges(&mut t, seed, opts.edge_rate);
            Ok((t, rows >= 5000))
        }
        name => match tables::builtin(name) {
            Some(t) => Ok((t, false)),
            None => bail!("unknown table set {name:?} (gen, edge, harness)"),
        },
    }
}

struct Job {
    index: usize,
    case: sqlgen::Case,
}

/// A script for `run_all`: the text sqawk reads, the statements SQLite
/// runs, and which statements compare in order.
pub type Script = (String, Vec<String>, Vec<bool>);

/// Judge every script on `jobs` threads (each with its own SQLite copy).
/// Without `keep_going`, no new script starts after the first finding.
pub fn run_all(
    env: &Env,
    tables: &[Table],
    scripts: &[Script],
    jobs: usize,
    keep_going: bool,
    verbose: bool,
) -> Vec<Option<Verdict>> {
    let started = Instant::now();
    let next = AtomicUsize::new(0);
    let stop = AtomicBool::new(false);
    let (tx, rx) = mpsc::channel::<(usize, Verdict)>();
    let mut results: Vec<Option<Verdict>> = vec![None; scripts.len()];
    std::thread::scope(|s| {
        for _ in 0..jobs.max(1) {
            let tx = tx.clone();
            let (next, stop) = (&next, &stop);
            s.spawn(move || {
                let oracle = match Oracle::new(tables) {
                    Ok(o) => o,
                    Err(e) => {
                        eprintln!("tsq: cannot load tables into SQLite: {e}");
                        return;
                    }
                };
                loop {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= scripts.len() {
                        break;
                    }
                    let (sql, stmts, ord) = &scripts[i];
                    let v = judge(env, &oracle, sql, stmts, ord);
                    if v.is_finding() && !keep_going {
                        stop.store(true, Ordering::Relaxed);
                    }
                    if tx.send((i, v)).is_err() {
                        break;
                    }
                }
            });
        }
        drop(tx);
        let mut done = 0;
        for (i, v) in rx {
            results[i] = Some(v);
            done += 1;
            if verbose && done % 1000 == 0 {
                eprintln!(
                    "[tsq] {done}/{} ({:.0}s)",
                    scripts.len(),
                    started.elapsed().as_secs_f64()
                );
            }
        }
    });
    results
}

pub fn run(seed: u64, sqawk: &Path, opts: &DiffOptions, verbose: bool) -> Result<i32> {
    let started = Instant::now();
    let (tables, large) = diff_tables(seed, opts)?;
    let tmp = tempfile::Builder::new().prefix("tsq-diff-").tempdir()?;
    crate::gen::write_csv_tables(tmp.path(), &tables)?;
    let oracle = Oracle::new(&tables).context("load tables into SQLite")?;
    let schema = tables::schema_of(&tables, &oracle)?;
    let cfg = sqlgen::GenConfig { large };
    let mut env = Env::new(sqawk, tmp.path(), &tables, &schema, opts.timeout);
    env.generated = true;

    let indexes: Vec<usize> = match opts.only {
        Some(i) => vec![i],
        None => (0..opts.queries).collect(),
    };
    let jobs: Vec<Job> = indexes
        .iter()
        .map(|&i| Job {
            index: i,
            case: sqlgen::Gen::new(seed, i as u64, &schema, &cfg).case(),
        })
        .collect();
    if opts.print {
        for j in &jobs {
            println!("-- #{}\n{}", j.index, j.case.sql());
        }
        return Ok(0);
    }
    if verbose {
        eprintln!(
            "[tsq] {} tests over {} tables ({} rows), {} jobs",
            jobs.len(),
            tables.len(),
            tables.iter().map(|t| t.rows.len()).sum::<usize>(),
            opts.jobs
        );
    }
    let scripts: Vec<Script> = jobs
        .iter()
        .map(|j| {
            let st = j.case.statements();
            (
                j.case.sql(),
                st.iter().map(|(s, _)| s.clone()).collect(),
                st.iter().map(|(_, o)| *o).collect(),
            )
        })
        .collect();

    let results = run_all(&env, &tables, &scripts, opts.jobs, opts.keep_going, verbose);

    let mut tally = Tally::default();
    let mut findings: Vec<usize> = Vec::new();
    let mut slow = Vec::new();
    for (i, v) in results.iter().enumerate() {
        let Some(v) = v else { continue };
        tally.add(v);
        let label = format!("#{} seed {seed}", jobs[i].index);
        let ordered = scripts[i].2.iter().any(|o| *o);
        if v.is_finding() {
            findings.push(i);
            print!("{}", describe(&label, &scripts[i].0, v, ordered));
        } else if v.cat != Cat::Pass && verbose {
            print!("{}", describe(&label, &scripts[i].0, v, ordered));
        }
        if v.elapsed > Duration::from_secs(2) && v.cat != Cat::Crash {
            slow.push((v.elapsed, jobs[i].index));
        }
    }
    for (t, i) in &slow {
        println!("SLOW {:.1}s: #{i}", t.as_secs_f64());
    }

    if opts.shrink && !findings.is_empty() {
        let out = opts
            .out
            .clone()
            .unwrap_or_else(|| PathBuf::from("tsq-repro"));
        fs::create_dir_all(&out)?;
        for &i in findings.iter().take(25) {
            let v = results[i].as_ref().expect("finding has a verdict");
            let dir = out.join(format!("seed{seed}-{}", jobs[i].index));
            let r = shrink::shrink(&env, &tables, &jobs[i].case, v)?;
            shrink::write_repro(&dir, &r, sqawk)?;
            println!(
                "SHRUNK #{} -> {} ({} rows):\n{}",
                jobs[i].index,
                dir.display(),
                r.tables.iter().map(|t| t.rows.len()).sum::<usize>(),
                r.case.sql()
            );
        }
    }
    println!("{}", tally.line("DIFF SUMMARY"));
    if verbose {
        eprintln!("[tsq] {:.1}s", started.elapsed().as_secs_f64());
    }
    Ok(if tally.findings > 0 { 1 } else { 0 })
}
