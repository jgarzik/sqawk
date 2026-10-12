//! Shrinking a failing test to a minimal reproduction.
//!
//! Greedy: try every cut of the SQL tree (drop a clause, a list item, a
//! join; replace an expression by a sub-expression or a literal) and keep
//! the first that still fails the same way; repeat until none does. Then
//! drop the tables the SQL does not name, delete rows in halving chunks and
//! drop columns the SQL does not mention, and cut the SQL once more. "The
//! same way" is `Verdict::same_failure`: same category, still undocumented,
//! and for errors the same message.

use std::fs;
use std::path::Path;

use anyhow::Result;

use super::ast::{apply, cuts};
use super::oracle::Oracle;
use super::render::ResultSet;
use super::sqlgen::Case;
use super::{judge, Env, Verdict};
use crate::data::Table;

pub struct Repro {
    pub case: Case,
    pub tables: Vec<Table>,
    pub verdict: Verdict,
}

/// Checks a candidate against fresh data, counting the attempts.
struct Checker<'a> {
    env: &'a Env,
    orig: &'a Verdict,
    budget: usize,
}

impl Checker<'_> {
    fn run(&mut self, case: &Case, tables: &[Table]) -> Option<Verdict> {
        if self.budget == 0 {
            return None;
        }
        self.budget -= 1;
        let tmp = tempfile::Builder::new()
            .prefix("tsq-shrink-")
            .tempdir()
            .ok()?;
        crate::gen::write_csv_tables(tmp.path(), tables).ok()?;
        let oracle = Oracle::new(tables).ok()?;
        let env = Env {
            sqawk: self.env.sqawk.clone(),
            dir: tmp.path().to_path_buf(),
            tables: tables.iter().map(|t| t.name.clone()).collect(),
            timeout: self.env.timeout,
            numtext_cols: self.env.numtext_cols.clone(),
            text_cols: self.env.text_cols.clone(),
            generated: self.env.generated,
        };
        let st = case.statements();
        let stmts: Vec<String> = st.iter().map(|(s, _)| s.clone()).collect();
        let ord: Vec<bool> = st.iter().map(|(_, o)| *o).collect();
        let v = judge(&env, &oracle, &case.sql(), &stmts, &ord);
        v.same_failure(self.orig).then_some(v)
    }
}

fn shrink_sql(ck: &mut Checker, case: &mut Case, tables: &[Table], last: &mut Verdict) {
    'outer: loop {
        let text = case.sql();
        for cut in cuts(&case.script) {
            let Some(script) = apply(&case.script, &cut) else {
                continue;
            };
            let cand = Case { script };
            let ct = cand.sql();
            if ct == text || ct.trim().is_empty() {
                continue;
            }
            if let Some(v) = ck.run(&cand, tables) {
                *case = cand;
                *last = v;
                continue 'outer;
            }
            if ck.budget == 0 {
                return;
            }
        }
        return;
    }
}

fn mentions(sql: &str, word: &str) -> bool {
    let w = word.to_ascii_lowercase();
    let mut cur = String::new();
    for c in sql.chars().chain(std::iter::once(' ')) {
        if c.is_alphanumeric() || c == '_' {
            cur.push(c.to_ascii_lowercase());
        } else if !cur.is_empty() {
            if cur == w {
                return true;
            }
            cur.clear();
        }
    }
    false
}

pub fn shrink(env: &Env, tables: &[Table], case: &Case, orig: &Verdict) -> Result<Repro> {
    // A time-limit crash costs the whole limit per failing check.
    let budget = if orig.cat == super::Cat::Crash {
        250
    } else {
        1500
    };
    let mut ck = Checker { env, orig, budget };
    let mut case = case.clone();
    let mut last = orig.clone();
    let mut data: Vec<Table> = tables.to_vec();
    // Big tables first: every check reloads them.
    for limit in [2000, 200, 30] {
        if data.iter().all(|t| t.rows.len() <= limit) {
            continue;
        }
        let mut cand = data.clone();
        for t in cand.iter_mut() {
            t.rows.truncate(limit);
            t.quoted_empty.retain(|(r, _)| *r < limit);
        }
        if let Some(v) = ck.run(&case, &cand) {
            data = cand;
            last = v;
        }
    }
    shrink_sql(&mut ck, &mut case, &data, &mut last);

    // Tables the SQL does not name (keep at least one: sqawk needs a file).
    let sql = case.sql();
    let named: Vec<Table> = data
        .iter()
        .filter(|t| mentions(&sql, &t.name))
        .cloned()
        .collect();
    if !named.is_empty() && named.len() < data.len() {
        if let Some(v) = ck.run(&case, &named) {
            data = named;
            last = v;
        }
    }

    // Rows, in halving chunks.
    for ti in 0..data.len() {
        let mut chunk = data[ti].rows.len().div_ceil(2);
        while chunk >= 1 && ck.budget > 0 {
            let mut start = 0;
            while start < data[ti].rows.len() && ck.budget > 0 {
                let mut cand = data.clone();
                let end = (start + chunk).min(cand[ti].rows.len());
                cand[ti].rows.drain(start..end);
                cand[ti].quoted_empty.clear();
                if let Some(v) = ck.run(&case, &cand) {
                    data = cand;
                    last = v;
                } else {
                    start += chunk;
                }
            }
            if chunk == 1 {
                break;
            }
            chunk = chunk.div_ceil(2);
        }
    }

    // Columns the SQL never names.
    for ti in 0..data.len() {
        let mut ci = data[ti].columns.len();
        while ci > 0 && ck.budget > 0 {
            ci -= 1;
            if data[ti].columns.len() == 1 || mentions(&case.sql(), &data[ti].columns[ci]) {
                continue;
            }
            let mut cand = data.clone();
            cand[ti].columns.remove(ci);
            for r in cand[ti].rows.iter_mut() {
                if ci < r.len() {
                    r.remove(ci);
                }
            }
            cand[ti].quoted_empty.clear();
            if let Some(v) = ck.run(&case, &cand) {
                data = cand;
                last = v;
            }
        }
    }
    shrink_sql(&mut ck, &mut case, &data, &mut last);
    Ok(Repro {
        case,
        tables: data,
        verdict: last,
    })
}

fn sets_text(sets: &[ResultSet]) -> String {
    let mut s = String::new();
    for set in sets {
        if !set.header.is_empty() {
            s.push_str(&set.header);
            s.push('\n');
        }
        for r in &set.rows {
            s.push_str(r);
            s.push('\n');
        }
    }
    s
}

/// Write the reproduction: `query.sql`, the tables, both outputs, and the
/// command that replays it.
pub fn write_repro(dir: &Path, r: &Repro, sqawk: &Path) -> Result<()> {
    fs::create_dir_all(dir)?;
    fs::write(dir.join("query.sql"), r.case.sql())?;
    crate::gen::write_csv_tables(dir, &r.tables)?;
    let v = &r.verdict;
    fs::write(
        dir.join("sqawk.out"),
        format!("{}\n--- stderr\n{}\n", v.sqawk_stdout, v.sqawk_msg),
    )?;
    let sqlite = if v.sqlite_msg.is_empty() {
        sets_text(&v.sqlite_sets)
    } else {
        format!("error: {}\n", v.sqlite_msg)
    };
    fs::write(dir.join("sqlite.out"), sqlite)?;
    let files: Vec<String> = r.tables.iter().map(|t| format!("{}.csv", t.name)).collect();
    fs::write(
        dir.join("command.txt"),
        format!(
            "{} -f query.sql {}\n# category: {}\n",
            sqawk.display(),
            files.join(" "),
            v.cat.name()
        ),
    )?;
    Ok(())
}
