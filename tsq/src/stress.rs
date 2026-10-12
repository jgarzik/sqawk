//! `tsq stress`: performance workloads over `tsq gen` data.
//!
//! `--rows N` is the row count of the largest table, `order_items`
//! (generated as `tsq gen --rows N/10`: customers N/10, orders 3N/10,
//! reviews N/20), plus a wide table of 1000 columns. Each workload is one
//! sqawk run; its wall time (spawn to exit) and, on Linux, the child's peak
//! resident set size (`VmHWM`, polled from `/proc/<pid>/status` until it
//! exits) are recorded, best of `--repeat` runs. Other systems record time
//! only (`peak_rss_kb` is null). Query output is drained and dropped.
//!
//! `--json OUT` writes the results; `--baseline FILE` compares with an
//! earlier file and exits 1 when a workload is slower or larger than the
//! baseline by more than `--tolerance` percent (with a floor of 50 ms and
//! 1 MiB, below which differences are noise), or fails where it passed.
//! Not part of `cargo test`.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context, Result};

use crate::data::{write_table, Format, Table};
use crate::gen;
use crate::rng::Rng;
use crate::run::{execute, Job, StdoutMode};

pub struct StressOptions {
    pub rows: usize,
    pub workloads: Option<String>,
    pub json: Option<PathBuf>,
    pub baseline: Option<PathBuf>,
    pub tolerance: f64,
    pub repeat: usize,
    pub timeout: Duration,
    pub data_dir: Option<PathBuf>,
}

/// What a workload runs.
enum Kind {
    /// A query over the named tables. `{n}` in the SQL is `--rows`.
    Query(&'static str, &'static [&'static str]),
    /// An UPDATE of order_items written back with --write (on a fresh copy
    /// each run; the copy is not timed).
    Writeback(&'static str),
}

pub struct Workload {
    pub name: &'static str,
    pub about: &'static str,
    kind: Kind,
}

const ITEMS: &[&str] = &["order_items"];

pub const WORKLOADS: &[Workload] = &[
    Workload {
        name: "load_count",
        about: "load order_items, COUNT(*)",
        kind: Kind::Query("SELECT COUNT(*) FROM order_items", ITEMS),
    },
    Workload {
        name: "filter",
        about: "WHERE on two columns, COUNT and SUM",
        kind: Kind::Query(
            "SELECT COUNT(*), SUM(line_total) FROM order_items WHERE quantity > 5 AND unit_price < 100.0",
            ITEMS,
        ),
    },
    Workload {
        name: "group_by",
        about: "GROUP BY product_id (one group per product)",
        kind: Kind::Query(
            "SELECT product_id, COUNT(*), SUM(line_total) FROM order_items GROUP BY product_id",
            ITEMS,
        ),
    },
    Workload {
        name: "group_multi",
        about: "GROUP BY two keys, AVG",
        kind: Kind::Query(
            "SELECT order_id % 100 AS b, quantity, COUNT(*), AVG(unit_price) FROM order_items GROUP BY 1, 2",
            ITEMS,
        ),
    },
    Workload {
        name: "join_equi",
        about: "order_items JOIN orders on the key, filtered",
        kind: Kind::Query(
            "SELECT COUNT(*), SUM(i.line_total) FROM order_items i JOIN orders o ON i.order_id = o.order_id WHERE o.status = 'shipped'",
            &["order_items", "orders"],
        ),
    },
    Workload {
        name: "join_3way",
        about: "order_items, orders, customers; GROUP BY country",
        kind: Kind::Query(
            "SELECT c.country, COUNT(*), SUM(i.line_total) FROM order_items i JOIN orders o ON i.order_id = o.order_id JOIN customers c ON o.customer_id = c.customer_id GROUP BY c.country",
            &["order_items", "orders", "customers"],
        ),
    },
    Workload {
        name: "sort_limit",
        about: "ORDER BY two keys, LIMIT 10",
        kind: Kind::Query(
            "SELECT * FROM order_items ORDER BY line_total DESC, item_id LIMIT 10",
            ITEMS,
        ),
    },
    Workload {
        name: "distinct",
        about: "SELECT DISTINCT of two columns, counted",
        kind: Kind::Query(
            "SELECT COUNT(*) FROM (SELECT DISTINCT order_id, quantity FROM order_items)",
            ITEMS,
        ),
    },
    Workload {
        name: "window_part",
        about: "ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)",
        kind: Kind::Query(
            "SELECT COUNT(*), MAX(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY item_id) AS rn FROM order_items)",
            ITEMS,
        ),
    },
    Workload {
        name: "window_slide",
        about: "SUM() OVER a 10-row sliding frame",
        kind: Kind::Query(
            "SELECT MAX(s) FROM (SELECT SUM(line_total) OVER (ORDER BY item_id ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) AS s FROM order_items)",
            ITEMS,
        ),
    },
    Workload {
        name: "cte",
        about: "WITH ... GROUP BY, read back filtered",
        kind: Kind::Query(
            "WITH big AS (SELECT order_id, SUM(line_total) AS t FROM order_items GROUP BY order_id) SELECT COUNT(*), MAX(t) FROM big WHERE t > 100",
            ITEMS,
        ),
    },
    Workload {
        name: "cte_recursive",
        about: "recursive counter to --rows",
        kind: Kind::Query(
            "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < {n}) SELECT COUNT(*), SUM(n) FROM c",
            &["customers"],
        ),
    },
    Workload {
        name: "in_subquery",
        about: "IN (SELECT ...) over order_items",
        kind: Kind::Query(
            "SELECT COUNT(*) FROM orders WHERE order_id IN (SELECT order_id FROM order_items WHERE quantity = 10)",
            &["order_items", "orders"],
        ),
    },
    Workload {
        name: "exists",
        about: "correlated EXISTS (customers with a returned order)",
        kind: Kind::Query(
            "SELECT COUNT(*) FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id AND o.status = 'returned')",
            &["customers", "orders"],
        ),
    },
    Workload {
        name: "union",
        about: "UNION of two key columns, counted",
        kind: Kind::Query(
            "SELECT COUNT(*) FROM (SELECT order_id FROM order_items UNION SELECT order_id FROM orders)",
            &["order_items", "orders"],
        ),
    },
    Workload {
        name: "intersect",
        about: "INTERSECT of two key columns, counted",
        kind: Kind::Query(
            "SELECT COUNT(*) FROM (SELECT product_id FROM order_items INTERSECT SELECT product_id FROM reviews)",
            &["order_items", "reviews"],
        ),
    },
    Workload {
        name: "update_write",
        about: "UPDATE a tenth of order_items, --write",
        kind: Kind::Writeback(
            "UPDATE order_items SET quantity = quantity + 1 WHERE item_id % 10 = 0",
        ),
    },
    Workload {
        name: "wide_select",
        about: "1000-column table: filter three columns, then SELECT *",
        kind: Kind::Query(
            "SELECT c0, c500, c999 FROM wide WHERE c1 % 7 = 0; SELECT * FROM wide",
            &["wide"],
        ),
    },
];

const WIDE_COLS: usize = 1000;

#[derive(Clone, Debug)]
struct Result1 {
    name: String,
    seconds: Option<f64>,
    peak_rss_kb: Option<u64>,
    status: String,
}

/// Write the stress tables into `dir`.
fn generate(seed: u64, rows: usize, dir: &Path, verbose: bool) -> Result<()> {
    fs::create_dir_all(dir)?;
    let mut layout = Rng::new(0);
    let tables = gen::base_tables(seed, (rows / 10).max(1));
    for t in &tables {
        let p = write_table(dir, t, &Format::default(), &mut layout)?;
        if verbose {
            eprintln!("[tsq] wrote {} ({} rows)", p.display(), t.rows.len());
        }
    }
    drop(tables);
    let wide_rows = (rows / 200).max(100);
    let cols: Vec<String> = (0..WIDE_COLS).map(|i| format!("c{i}")).collect();
    let mut wide = Table::new("wide", &[]);
    wide.columns = cols;
    let mut r = Rng::stream(seed, 11);
    for i in 0..wide_rows {
        let mut row = Vec::with_capacity(WIDE_COLS);
        row.push(i.to_string());
        for _ in 1..WIDE_COLS {
            row.push(r.range(0, 100_000).to_string());
        }
        wide.rows.push(row);
    }
    let p = write_table(dir, &wide, &Format::default(), &mut layout)?;
    if verbose {
        eprintln!("[tsq] wrote {} ({} rows)", p.display(), wide_rows);
    }
    Ok(())
}

fn run_workload(sqawk: &Path, w: &Workload, data: &Path, opts: &StressOptions) -> Result<Result1> {
    let mut best: Option<f64> = None;
    let mut rss: Option<u64> = None;
    for _ in 0..opts.repeat {
        let mut args: Vec<std::ffi::OsString> = Vec::new();
        match &w.kind {
            Kind::Query(sql, tables) => {
                args.push("-s".into());
                args.push(sql.replace("{n}", &opts.rows.to_string()).into());
                for t in *tables {
                    args.push(data.join(format!("{t}.csv")).into_os_string());
                }
            }
            Kind::Writeback(sql) => {
                let scratch = data.join("wb");
                fs::create_dir_all(&scratch)?;
                let target = scratch.join("order_items.csv");
                fs::copy(data.join("order_items.csv"), &target)?;
                args.push("-w".into());
                args.push("-s".into());
                args.push((*sql).into());
                args.push(target.into_os_string());
            }
        }
        let mut job = Job::new(sqawk, args);
        job.limit = opts.timeout;
        job.stdout = StdoutMode::Discard;
        job.watch_rss = true;
        let f = execute(&job);
        if let Some(e) = f.spawn_error {
            bail!(e);
        }
        if f.timed_out {
            return Ok(Result1 {
                name: w.name.to_string(),
                seconds: None,
                peak_rss_kb: rss,
                status: format!("timeout after {}s", opts.timeout.as_secs()),
            });
        }
        if f.code != Some(0) {
            let msg = f.stderr_text();
            return Ok(Result1 {
                name: w.name.to_string(),
                seconds: None,
                peak_rss_kb: None,
                status: format!(
                    "error ({:?}): {}",
                    f.code,
                    msg.trim().lines().last().unwrap_or("")
                ),
            });
        }
        let s = f.elapsed.as_secs_f64();
        best = Some(best.map_or(s, |b| b.min(s)));
        if let Some(kb) = f.peak_rss_kb {
            rss = Some(rss.map_or(kb, |p| p.min(kb)));
        }
    }
    Ok(Result1 {
        name: w.name.to_string(),
        seconds: best,
        peak_rss_kb: rss,
        status: "ok".to_string(),
    })
}

fn json_str(s: &str) -> String {
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn opt<T: std::fmt::Display>(v: Option<T>) -> String {
    v.map_or("null".to_string(), |x| x.to_string())
}

fn to_json(seed: u64, opts: &StressOptions, results: &[Result1]) -> String {
    let lines: Vec<String> = results
        .iter()
        .map(|r| {
            format!(
                "    {{\"name\": {}, \"seconds\": {}, \"peak_rss_kb\": {}, \"status\": {}}}",
                json_str(&r.name),
                opt(r.seconds.map(|s| format!("{s:.4}"))),
                opt(r.peak_rss_kb),
                json_str(&r.status)
            )
        })
        .collect();
    format!(
        "{{\n  \"tsq_version\": {},\n  \"seed\": {},\n  \"rows\": {},\n  \"repeat\": {},\n  \"os\": {},\n  \"workloads\": [\n{}\n  ]\n}}\n",
        json_str(env!("CARGO_PKG_VERSION")),
        seed,
        opts.rows,
        opts.repeat,
        json_str(std::env::consts::OS),
        lines.join(",\n")
    )
}

/// The workload lines of a file `to_json` wrote (one object per line).
fn parse_json(text: &str) -> Vec<Result1> {
    fn field<'a>(line: &'a str, key: &str) -> Option<&'a str> {
        let k = format!("\"{key}\": ");
        let i = line.find(&k)? + k.len();
        let rest = &line[i..];
        if let Some(s) = rest.strip_prefix('"') {
            let mut end = 0;
            let b = s.as_bytes();
            while end < b.len() && !(b[end] == b'"' && (end == 0 || b[end - 1] != b'\\')) {
                end += 1;
            }
            Some(&s[..end])
        } else {
            let end = rest.find([',', '}']).unwrap_or(rest.len());
            Some(rest[..end].trim())
        }
    }
    text.lines()
        .filter(|l| l.contains("\"name\": "))
        .filter_map(|l| {
            Some(Result1 {
                name: field(l, "name")?.to_string(),
                seconds: field(l, "seconds").and_then(|s| s.parse().ok()),
                peak_rss_kb: field(l, "peak_rss_kb").and_then(|s| s.parse().ok()),
                status: field(l, "status").unwrap_or("").to_string(),
            })
        })
        .collect()
}

/// Regressions of `now` against `base`.
fn compare(base: &[Result1], now: &[Result1], tol: f64) -> Vec<String> {
    let mut out = Vec::new();
    let f = 1.0 + tol / 100.0;
    for n in now {
        let Some(b) = base.iter().find(|b| b.name == n.name) else {
            continue;
        };
        if b.status == "ok" && n.status != "ok" {
            out.push(format!("{}: {} (baseline ok)", n.name, n.status));
            continue;
        }
        if let (Some(bs), Some(ns)) = (b.seconds, n.seconds) {
            if ns > bs * f && ns - bs > 0.05 {
                out.push(format!(
                    "{}: time {:.3}s vs {:.3}s (+{:.0}%)",
                    n.name,
                    ns,
                    bs,
                    (ns / bs - 1.0) * 100.0
                ));
            }
        }
        if let (Some(br), Some(nr)) = (b.peak_rss_kb, n.peak_rss_kb) {
            if nr as f64 > br as f64 * f && nr.saturating_sub(br) > 1024 {
                out.push(format!(
                    "{}: peak RSS {} KiB vs {} KiB (+{:.0}%)",
                    n.name,
                    nr,
                    br,
                    (nr as f64 / br as f64 - 1.0) * 100.0
                ));
            }
        }
    }
    out
}

pub fn run(seed: u64, sqawk: &Path, opts: &StressOptions, verbose: bool) -> Result<i32> {
    if !(1_000..=10_000_000).contains(&opts.rows) {
        bail!("--rows must be between 1000 and 10000000");
    }
    let chosen: Vec<&Workload> = match &opts.workloads {
        None => WORKLOADS.iter().collect(),
        Some(list) => {
            let mut v = Vec::new();
            for name in list.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                match WORKLOADS.iter().find(|w| w.name == name) {
                    Some(w) => v.push(w),
                    None => bail!("unknown workload {name:?} (see tsq stress --list)"),
                }
            }
            v
        }
    };
    let baseline = match &opts.baseline {
        Some(p) => {
            let text = fs::read_to_string(p).with_context(|| format!("read {}", p.display()))?;
            let rows = text
                .lines()
                .find_map(|l| l.trim().strip_prefix("\"rows\": "))
                .map(|v| v.trim_end_matches(',').to_string());
            if rows.as_deref() != Some(opts.rows.to_string().as_str()) {
                println!(
                    "tsq stress: warning: baseline was recorded with --rows {}",
                    rows.unwrap_or_else(|| "?".to_string())
                );
            }
            Some(parse_json(&text))
        }
        None => None,
    };
    let tmp;
    let data = match &opts.data_dir {
        Some(d) => d.clone(),
        None => {
            tmp = tempfile::Builder::new().prefix("tsq-stress-").tempdir()?;
            tmp.path().to_path_buf()
        }
    };
    // A reused --data-dir is regenerated unless it holds this seed and size.
    let stamp = format!("seed {seed} rows {}\n", opts.rows);
    let stamp_file = data.join("tsq-stress.txt");
    let have = fs::read_to_string(&stamp_file).ok().as_deref() == Some(stamp.as_str());
    if !have {
        let t = std::time::Instant::now();
        generate(seed, opts.rows, &data, verbose)?;
        fs::write(&stamp_file, &stamp)?;
        println!(
            "tsq stress: generated {} rows of order_items in {:.1}s ({})",
            opts.rows,
            t.elapsed().as_secs_f64(),
            data.display()
        );
    }
    let mut results = Vec::new();
    for w in chosen {
        let r = run_workload(sqawk, w, &data, opts)?;
        println!(
            "{:<14} {:>9} {:>12}  {}",
            r.name,
            r.seconds.map_or("-".to_string(), |s| format!("{s:.3}s")),
            r.peak_rss_kb
                .map_or("-".to_string(), |k| format!("{:.1} MiB", k as f64 / 1024.0)),
            r.status
        );
        results.push(r);
    }
    let json = to_json(seed, opts, &results);
    if let Some(p) = &opts.json {
        fs::write(p, &json).with_context(|| format!("write {}", p.display()))?;
    }
    let failed = results.iter().filter(|r| r.status != "ok").count();
    let mut code = if failed > 0 { 1 } else { 0 };
    if let Some(base) = baseline {
        let regs = compare(&base, &results, opts.tolerance);
        for r in &regs {
            println!("REGRESSION {r}");
        }
        println!(
            "STRESS BASELINE regressions {} (tolerance {}%)",
            regs.len(),
            opts.tolerance
        );
        if !regs.is_empty() {
            code = 1;
        }
    }
    println!("STRESS SUMMARY workloads {} failed {failed}", results.len());
    Ok(code)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_round_trip_and_compare() {
        let opts = StressOptions {
            rows: 1000,
            workloads: None,
            json: None,
            baseline: None,
            tolerance: 10.0,
            repeat: 1,
            timeout: Duration::from_secs(1),
            data_dir: None,
        };
        let base = vec![
            Result1 {
                name: "a".into(),
                seconds: Some(1.0),
                peak_rss_kb: Some(100_000),
                status: "ok".into(),
            },
            Result1 {
                name: "b".into(),
                seconds: Some(2.0),
                peak_rss_kb: None,
                status: "ok".into(),
            },
        ];
        let parsed = parse_json(&to_json(1, &opts, &base));
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].seconds, Some(1.0));
        assert_eq!(parsed[1].peak_rss_kb, None);
        let now = vec![
            Result1 {
                name: "a".into(),
                seconds: Some(1.05),
                peak_rss_kb: Some(130_000),
                status: "ok".into(),
            },
            Result1 {
                name: "b".into(),
                seconds: Some(2.5),
                peak_rss_kb: None,
                status: "ok".into(),
            },
        ];
        let regs = compare(&parsed, &now, 10.0);
        assert_eq!(regs.len(), 2, "{regs:?}");
        assert!(regs[0].starts_with("a: peak RSS"));
        assert!(regs[1].starts_with("b: time"));
    }
}
