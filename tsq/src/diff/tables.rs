//! Built-in table sets and the generator's view of any table set.
//!
//! `harness` is the retired Python harness.py's fixed TABLES (emp, dept, t1,
//! t2, t3, e, uni); `edge` is the retired diffh.py's `build_tables(7)` (a, b, c, d, e,
//! dup, u, dt: i64 extremes, numeric-looking text, mixed types, NULLs,
//! duplicates, Unicode, embedded quotes/commas/LF, a 5000-character string,
//! date strings), transcribed cell for cell from the Python output so the
//! corpora keep their meaning.

use std::collections::BTreeSet;
use std::rc::Rc;

use anyhow::Result;

use super::oracle::Oracle;
use super::schema::{Col, Kind, Props, Rel, Schema};
use crate::data::Table;

fn table(name: &str, cols: &[&str], rows: &[&[&str]]) -> Table {
    let mut t = Table::new(name, cols);
    for r in rows {
        t.rows.push(
            r.iter()
                .map(|c| {
                    if *c == "\u{0}x5000" {
                        "x".repeat(5000)
                    } else {
                        c.to_string()
                    }
                })
                .collect(),
        );
    }
    t
}

#[rustfmt::skip]
pub fn harness() -> Vec<Table> {
    vec![
        table(
            "emp",
            &["id", "name", "dept", "salary", "mgr", "bonus"],
            &[
                &["1", "Alice", "Eng", "100", "", "10.5"],
                &["2", "Bob", "Eng", "80", "1", ""],
                &["3", "Carol", "Sales", "70", "1", "5"],
                &["4", "Dave", "Sales", "70", "3", "-2.25"],
                &["5", "Eve", "", "50", "3", "0"],
                &["6", "Frank", "HR", "", "1", ""],
                &["7", "alice", "Eng", "100", "2", "3"],
                &["8", "Grace", "Ops", "-10", "", "1.5"],
                &["9", "Heidi", "HR", "60", "6", ""],
                &["10", "Bob", "Eng", "80", "1", "2"],
            ],
        ),
        table(
            "dept",
            &["dept", "dname", "budget"],
            &[
                &["Eng", "Engineering", "1000"],
                &["Sales", "Sales Team", "500.5"],
                &["HR", "Human Res", ""],
                &["Mkt", "Marketing", "200"],
            ],
        ),
        table(
            "t1",
            &["k", "a", "s"],
            &[
                &["1", "10", "x"],
                &["2", "", "y"],
                &["3", "30", ""],
                &["3", "30", ""],
                &["4", "-5", "x%y"],
                &["5", "0", "A_b"],
            ],
        ),
        table(
            "t2",
            &["k", "b"],
            &[
                &["1", "100"],
                &["3", "300"],
                &["3", "301"],
                &["6", "600"],
                &["", "999"],
            ],
        ),
        table(
            "t3",
            &["k", "c"],
            &[
                &["1", "one"],
                &["6", "six"],
                &["7", "seven"],
            ],
        ),
        table(
            "e",
            &["x"],
            &[
            ],
        ),
        table(
            "uni",
            &["id", "w"],
            &[
                &["1", "héllo"],
                &["2", "日本語"],
                &["3", "naïve café"],
                &["4", "  pad  "],
                &["5", ""],
                &["6", "ABC"],
            ],
        ),
    ]
}

#[rustfmt::skip]
pub fn edge() -> Vec<Table> {
    vec![
        table(
            "a",
            &["aid", "k", "x", "y", "s", "g", "n", "m"],
            &[
                &["1", "3", "1", "100.5", "Beta", "red", "+3", "abc"],
                &["2", "3", "9223372036854775807", "", "  sp  ", "blue", "007", "abc"],
                &["3", "4", "3", "1.5", "", "red", "+3", "Zed"],
                &["4", "1", "9223372036854775807", "1.5", "", "blue", "", "10"],
                &["5", "1", "9223372036854775807", "100.5", "Beta", "blue", "007", ""],
                &["6", "3", "1", "0.1", "a\nb", "blue", "+3", "abc"],
                &["7", "", "", "0.1", "ß", "blue", " 5", "abc"],
                &["8", "", "9223372036854775807", "2.75", "日本", "red", "+3", "abc"],
                &["9", "", "", "2.75", "beta", "blue", "+3", "Zed"],
                &["10", "3", "100", "-2.5", "ALPHA", "Green", "9", "007"],
                &["11", "3", "1", "2.75", "gamma", "red ", "9", ""],
                &["12", "4", "100", "-2.5", "comma,s", "red ", "1.50", "abc"],
                &["13", "", "3", "-0.25", "naïve", "blue", "-0", "Zed"],
                &["14", "1", "", "1.5", "ß", "red ", "1e3", "-3"],
                &["15", "2", "100", "-2.5", "ALPHA", "red", "1.50", ""],
                &["16", "4", "-9223372036854775808", "1.5", "Beta", "blue", "2.50", ""],
                &["17", "2", "9223372036854775807", "-2.5", "comma,s", "blue", "0x10", "-3"],
                &["18", "1", "3", "3.14159", "it's", "red ", "1.50", "0"],
                &["19", "1", "-7", "0.1", "A_b", "blue", "10", "Zed"],
                &["20", "4", "3", "1.5", "it's", "", "0x10", ""],
            ],
        ),
        table(
            "b",
            &["bid", "aid", "k", "v", "w"],
            &[
                &["101", "9", "2", "7", "ß"],
                &["102", "9", "5", "-5", "Ünïcode"],
                &["103", "8", "2", "0", "it's"],
                &["104", "5", "2", "2", "alpha"],
                &["105", "16", "", "1", "quote\"d"],
                &["106", "10", "1", "1", "a\nb"],
                &["107", "18", "3", "-5", "A_b"],
                &["108", "23", "", "", "ALPHA"],
                &["109", "22", "", "7", "Ünïcode"],
                &["110", "13", "5", "0", "beta"],
                &["111", "21", "5", "", ""],
                &["112", "3", "2", "7", "it's"],
                &["113", "4", "3", "", "x%y"],
                &["114", "1", "", "1", "ß"],
                &["115", "4", "3", "", "gamma"],
                &["116", "7", "", "7", "A_b"],
                &["117", "21", "3", "-5", "日本"],
                &["118", "16", "1", "0", "beta"],
                &["119", "15", "5", "7", "comma,s"],
                &["120", "3", "2", "0", "naïve"],
                &["121", "", "3", "7", "it's"],
                &["122", "17", "1", "2", "  sp  "],
                &["123", "12", "2", "1000000", "alpha"],
                &["124", "17", "3", "0", "quote\"d"],
                &["125", "17", "3", "1", "日本"],
            ],
        ),
        table(
            "c",
            &["cid", "bid", "k", "z", "g"],
            &[
                &["201", "125", "2", "2.5", "red "],
                &["202", "125", "", "1.25", "blue"],
                &["203", "108", "", "0.5", "blue"],
                &["204", "127", "6", "7.75", "blue"],
                &["205", "107", "", "-3.5", "Green"],
                &["206", "124", "1", "", "Green"],
                &["207", "116", "4", "0.5", "blue"],
                &["208", "120", "4", "-3.5", "blue"],
                &["209", "112", "4", "", "blue"],
                &["210", "104", "2", "-3.5", "blue"],
                &["211", "111", "2", "-3.5", "red "],
                &["212", "120", "1", "-3.5", "blue"],
                &["213", "112", "1", "7.75", "red"],
                &["214", "113", "2", "-3.5", "blue"],
                &["215", "114", "4", "", "blue"],
            ],
        ),
        table(
            "d",
            &["k", "label", "wt"],
            &[
                &["1", "one", "10"],
                &["2", "two", "20"],
                &["3", "three", ""],
                &["", "nil", "5"],
                &["5", "five", "50"],
                &["2", "deux", "21"],
            ],
        ),
        table(
            "e",
            &["eid", "k", "x"],
            &[
            ],
        ),
        table(
            "dup",
            &["p", "q"],
            &[
                &["1", "a"],
                &["1", "a"],
                &["2", ""],
                &["2", ""],
                &["", ""],
                &["", ""],
                &["3", "c"],
                &["1", "a"],
            ],
        ),
        table(
            "u",
            &["uid", "t"],
            &[
                &["1", "héllo"],
                &["2", "日本語テキスト"],
                &["3", "\u{0}x5000"],
                &["4", "émoji 🎉"],
                &["5", "MiXeD"],
                &["6", "tab\there"],
                &["7", "ÀÉÎ"],
                &["8", "  lead"],
                &["9", "trail  "],
                &["10", "%_\\"],
            ],
        ),
        table(
            "dt",
            &["did", "ts", "mod"],
            &[
                &["1", "2024-01-31", "+1 month"],
                &["2", "2024-02-29 12:34:56", "-1 year"],
                &["3", "2000-01-01T00:00:00", "start of month"],
                &["4", "1970-01-01", "+86400 seconds"],
                &["5", "2023-12-31 23:59:59.999", "+1 second"],
                &["6", "bad", "+1 day"],
                &["7", "2024-03-10", "weekday 0"],
                &["8", "12:30", "+90 minutes"],
                &["9", "2459000.5", "+0 days"],
                &["10", "", "+1 day"],
                &["11", "2024-06-15", "start of year"],
                &["12", "0000-01-01", "-1 day"],
                &["13", "9999-12-31", "+1 day"],
            ],
        ),
    ]
}

/// The named built-in set.
pub fn builtin(name: &str) -> Option<Vec<Table>> {
    match name {
        "harness" => Some(harness()),
        "edge" => Some(edge()),
        _ => None,
    }
}

/// Foreign keys and hierarchies of the known table sets, by table name:
/// (table, column, referenced table, referenced column).
const FKS: &[(&str, &str, &str, &str)] = &[
    ("orders", "customer_id", "customers", "customer_id"),
    ("order_items", "order_id", "orders", "order_id"),
    ("order_items", "product_id", "products", "product_id"),
    ("reviews", "customer_id", "customers", "customer_id"),
    ("reviews", "product_id", "products", "product_id"),
    ("customers", "referred_by", "customers", "customer_id"),
    ("b", "aid", "a", "aid"),
    ("c", "bid", "b", "bid"),
    ("a", "k", "d", "k"),
    ("b", "k", "d", "k"),
    ("c", "k", "d", "k"),
    ("e", "k", "d", "k"),
    ("emp", "dept", "dept", "dept"),
    ("emp", "mgr", "emp", "id"),
    ("t1", "k", "t2", "k"),
    ("t2", "k", "t3", "k"),
];

const TREES: &[(&str, &str, &str)] = &[
    ("customers", "customer_id", "referred_by"),
    ("a", "aid", "k"),
    ("emp", "id", "mgr"),
];

/// The generator's view of `tables` as `oracle` loaded them: each column's
/// properties from its values, a key where one column is distinct and never
/// NULL, the known foreign keys, and sample text values.
pub fn schema_of(tables: &[Table], oracle: &Oracle) -> Result<Schema> {
    let mut rels = Vec::new();
    let mut lits = BTreeSet::new();
    let mut big = BTreeSet::new();
    let names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
    for t in tables {
        let values = oracle.columns(&t.name, t.columns.len())?;
        let mut cols = Vec::new();
        let mut key = None;
        for (i, name) in t.columns.iter().enumerate() {
            let props = Props::of_values(values[i].iter());
            if values[i].iter().any(|v| match v {
                rusqlite::types::Value::Integer(x) => x.unsigned_abs() > 1 << 40,
                rusqlite::types::Value::Real(f) => f.abs() > 1e12,
                _ => false,
            }) {
                big.insert(name.clone());
            }
            if key.is_none() && props.kind == Kind::Int && !values[i].is_empty() {
                let mut seen = BTreeSet::new();
                let unique = values[i].iter().all(|v| match v {
                    rusqlite::types::Value::Integer(x) => seen.insert(*x),
                    _ => false,
                });
                if unique {
                    key = Some(i);
                }
            }
            for v in values[i].iter().take(400) {
                if let rusqlite::types::Value::Text(s) = v {
                    let p = Props::of_text(s);
                    if p.letter && p.ascii && s.len() <= 16 && !s.contains('\'') {
                        lits.insert(s.clone());
                    }
                }
            }
            cols.push(Col {
                name: name.clone(),
                props,
            });
        }
        let fks = FKS
            .iter()
            .filter(|(tt, _, rt, _)| *tt == t.name && names.contains(rt))
            .filter_map(|(_, c, rt, rc)| {
                let ci = t.columns.iter().position(|x| x == c)?;
                let target = tables.iter().find(|x| x.name == *rt)?;
                let ri = target.columns.iter().position(|x| x == rc)?;
                Some((ci, rt.to_string(), ri))
            })
            .collect();
        rels.push(Rc::new(Rel {
            name: t.name.clone(),
            cols,
            key,
            card: t.rows.len(),
            fks,
        }));
    }
    let trees = TREES
        .iter()
        .filter(|(t, id, parent)| {
            rels.iter().any(|r| {
                r.name == *t
                    && r.col_index(id)
                        .is_some_and(|i| r.cols[i].props.kind == Kind::Int)
                    && r.col_index(parent)
                        .is_some_and(|i| r.cols[i].props.kind == Kind::Int)
            })
        })
        .map(|(t, id, p)| (t.to_string(), id.to_string(), p.to_string()))
        .collect();
    // An even spread of the sorted values, at most 40.
    let all: Vec<String> = lits.into_iter().collect();
    let step = (all.len() / 40).max(1);
    let text_lits = all.into_iter().step_by(step).take(40).collect();
    Ok(Schema {
        rels,
        trees,
        text_lits,
        big_cols: big.into_iter().collect(),
    })
}
