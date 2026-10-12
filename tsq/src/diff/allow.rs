//! The documented differences from SQLite, one rule each.
//!
//! A non-passing result that a rule covers is counted as documented rather
//! than reported as a finding. Every rule cites the row or section of
//! doc/sql_reference.md it implements; a rule may only be added together
//! with the documentation of the difference. The random generator avoids
//! these cases by construction (sqlgen.rs), so for it a documented result
//! is rare; the hand corpora exercise them on purpose.

use std::collections::BTreeSet;

use super::render::ResultSet;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Diff,
    SqawkErr,
    SqliteErr,
}

pub struct Ctx<'a> {
    pub sql: &'a str,
    pub outcome: Outcome,
    pub sqawk_msg: &'a str,
    /// Both sides' result sets (for Diff), shaped alike when possible.
    pub sqawk_sets: &'a [ResultSet],
    pub sqlite_sets: &'a [ResultSet],
    /// Columns (by name) holding text with a numeric reading.
    pub numtext_cols: &'a BTreeSet<String>,
    /// Columns (by name) holding any text.
    pub text_cols: &'a BTreeSet<String>,
    /// The SQL came from the random generator, which never compares
    /// numeric-looking text, declares converting column types or sums text
    /// (sqlgen.rs). The rules for those differences recognize them from the
    /// SQL text alone, which could hide an unrelated wrong answer in the
    /// same query, so they apply to hand-written corpora only.
    pub generated: bool,
}

pub struct Rule {
    pub name: &'static str,
    /// Where doc/sql_reference.md documents it.
    pub doc: &'static str,
    matches: fn(&Ctx) -> bool,
}

fn upper(s: &str) -> String {
    s.to_ascii_uppercase()
}

fn has(sql: &str, needle: &str) -> bool {
    upper(sql).contains(&upper(needle))
}

/// Rough SQL tokens: identifiers (dotted), numbers, quoted strings,
/// operators.
fn tokens(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let cs: Vec<char> = sql.chars().collect();
    let mut i = 0;
    while i < cs.len() {
        let c = cs[i];
        if c.is_whitespace() {
            i += 1;
        } else if c == '\'' {
            let mut s = String::from("'");
            i += 1;
            while i < cs.len() {
                if cs[i] == '\'' {
                    if i + 1 < cs.len() && cs[i + 1] == '\'' {
                        s.push('\'');
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                s.push(cs[i]);
                i += 1;
            }
            s.push('\'');
            out.push(s);
        } else if c.is_alphanumeric() || c == '_' || c == '.' {
            let mut s = String::new();
            while i < cs.len() && (cs[i].is_alphanumeric() || cs[i] == '_' || cs[i] == '.') {
                s.push(cs[i]);
                i += 1;
            }
            out.push(s);
        } else {
            let two: String = cs[i..(i + 2).min(cs.len())].iter().collect();
            if ["<=", ">=", "<>", "!=", "==", "||"].contains(&two.as_str()) {
                out.push(two);
                i += 2;
            } else {
                out.push(c.to_string());
                i += 1;
            }
        }
    }
    out
}

fn is_compare_op(t: &str) -> bool {
    matches!(
        upper(t).as_str(),
        "=" | "==" | "<" | ">" | "<=" | ">=" | "<>" | "!=" | "IN" | "BETWEEN" | "IS" | "BY"
    )
}

fn last_segment(t: &str) -> &str {
    t.rsplit('.').next().unwrap_or(t)
}

fn rows_equal_after(ctx: &Ctx, f: fn(&str) -> String) -> bool {
    if ctx.sqawk_sets.len() != ctx.sqlite_sets.len() {
        return false;
    }
    ctx.sqawk_sets.iter().zip(ctx.sqlite_sets).all(|(a, b)| {
        let mut x: Vec<String> = a.rows.iter().map(|r| f(r)).collect();
        let mut y: Vec<String> = b.rows.iter().map(|r| f(r)).collect();
        x.sort();
        y.sort();
        x == y
    })
}

/// "Column types": numeric-looking text compares as a number in sqawk, as
/// text in SQLite; declared affinity converts values in SQLite only.
fn column_types(ctx: &Ctx) -> bool {
    if ctx.outcome != Outcome::Diff || ctx.generated {
        return false;
    }
    let toks = tokens(ctx.sql);
    // A quoted literal with a numeric reading next to a comparison.
    for (i, t) in toks.iter().enumerate() {
        let near_cmp = (i > 0 && is_compare_op(&toks[i - 1]))
            || toks.get(i + 1).is_some_and(|n| is_compare_op(n));
        if t.starts_with('\'') && near_cmp && super::schema::has_numeric_reading(&t[1..t.len() - 1])
        {
            return true;
        }
        // A column holding numeric-looking text being compared or ordered.
        if near_cmp && ctx.numtext_cols.contains(last_segment(t)) {
            return true;
        }
        if (upper(t) == "MIN" || upper(t) == "MAX")
            && toks.get(i + 1).is_some_and(|n| n == "(")
            && toks
                .get(i + 2)
                .is_some_and(|n| ctx.numtext_cols.contains(last_segment(n)))
        {
            return true;
        }
    }
    // CAST(... AS TEXT) gives TEXT affinity to a comparison.
    if has(ctx.sql, "AS TEXT)") && toks.iter().any(|t| is_compare_op(t)) {
        return true;
    }
    // A declared column type with affinity that converts values.
    for (i, t) in toks.iter().enumerate() {
        let u = upper(t);
        let declares = (u == "TABLE"
            && toks.get(i + 2).is_some_and(|n| n == "(")
            && i >= 1
            && upper(&toks[i - 1]) == "CREATE")
            || (u == "COLUMN" && i >= 1 && upper(&toks[i - 1]) == "ADD");
        if declares {
            let rest = toks[i + 1..]
                .iter()
                .take(40)
                .map(|x| upper(x))
                .collect::<Vec<_>>();
            if rest.iter().any(|x| {
                x.contains("TEXT")
                    || x.contains("CHAR")
                    || x.contains("REAL")
                    || x.contains("FLOA")
                    || x.contains("DOUB")
                    || x.contains("NUM")
                    || x.contains("DEC")
                    || x.contains("INT")
            }) {
                return true;
            }
        }
    }
    false
}

/// "NULL vs ''": both print as an empty field. The oracle renders NULL as
/// an empty field too, so this difference can never show up as a mismatch;
/// the rule exists to record that it was considered.
fn null_vs_empty(_ctx: &Ctx) -> bool {
    false
}

/// "CAST(x AS STRING)": TEXT in sqawk, NUMERIC in SQLite.
fn cast_as_string(ctx: &Ctx) -> bool {
    ctx.outcome == Outcome::Diff && has(ctx.sql, "AS STRING")
}

/// "UPPER / LOWER": all of Unicode in sqawk, ASCII only in SQLite.
fn upper_lower(ctx: &Ctx) -> bool {
    ctx.outcome == Outcome::Diff
        && (has(ctx.sql, "UPPER(") || has(ctx.sql, "LOWER("))
        && rows_equal_after(ctx, |s| s.to_lowercase())
}

/// "TRIM(s)": all whitespace in sqawk, spaces only in SQLite. (sqawk's
/// LTRIM / RTRIM with one argument follow TRIM.)
fn trim_whitespace(ctx: &Ctx) -> bool {
    ctx.outcome == Outcome::Diff
        && has(ctx.sql, "TRIM(")
        && rows_equal_after(ctx, |s| s.chars().filter(|c| !c.is_whitespace()).collect())
}

/// "SUM / AVG over text": non-numeric text is skipped by sqawk, counted
/// as 0 by SQLite.
fn sum_avg_text(ctx: &Ctx) -> bool {
    if ctx.outcome != Outcome::Diff || ctx.generated {
        return false;
    }
    let toks = tokens(ctx.sql);
    toks.iter().enumerate().any(|(i, t)| {
        if !matches!(upper(t).as_str(), "SUM" | "AVG") || toks.get(i + 1).is_none_or(|n| n != "(") {
            return false;
        }
        let mut j = i + 2;
        if toks.get(j).is_some_and(|n| upper(n) == "DISTINCT") {
            j += 1;
        }
        toks.get(j)
            .is_some_and(|n| ctx.text_cols.contains(last_segment(n)))
    })
}

/// The separators of the `CONCAT_WS('sep', ...)` calls in `sql` that are
/// non-empty string literals.
fn concat_ws_separators(sql: &str) -> Vec<String> {
    let toks = tokens(sql);
    toks.windows(3)
        .filter(|w| upper(&w[0]) == "CONCAT_WS" && w[1] == "(" && w[2].len() > 2)
        .filter_map(|w| {
            w[2].strip_prefix('\'')?
                .strip_suffix('\'')
                .map(str::to_string)
        })
        .collect()
}

/// "CONCAT_WS with an empty-string argument": SQLite 3.45 writes no
/// separator for it, sqawk does (`a,,b`). The rows must agree once every
/// separator is removed from both sides.
fn concat_ws_empty(ctx: &Ctx) -> bool {
    if ctx.outcome != Outcome::Diff || ctx.sqawk_sets.len() != ctx.sqlite_sets.len() {
        return false;
    }
    let seps = concat_ws_separators(ctx.sql);
    if seps.is_empty() {
        return false;
    }
    let strip = |r: &String| {
        let mut r = r.clone();
        for sep in &seps {
            r = r.replace(sep.as_str(), "");
        }
        r
    };
    ctx.sqawk_sets.iter().zip(ctx.sqlite_sets).all(|(a, b)| {
        let mut x: Vec<String> = a.rows.iter().map(strip).collect();
        let mut y: Vec<String> = b.rows.iter().map(strip).collect();
        x.sort();
        y.sort();
        x == y
    })
}

/// "Bare column in an aggregate query": rejected by sqawk.
fn bare_column(ctx: &Ctx) -> bool {
    ctx.outcome == Outcome::SqawkErr
        && ctx
            .sqawk_msg
            .contains("must appear in GROUP BY or be used in an aggregate")
}

/// "INSERT OR REPLACE, ON CONFLICT": rejected by sqawk.
fn insert_or_replace(ctx: &Ctx) -> bool {
    ctx.outcome == Outcome::SqawkErr
        && (ctx.sqawk_msg.contains("INSERT OR") || ctx.sqawk_msg.contains("ON CONFLICT"))
}

/// "Subqueries in UPDATE / DELETE": sqawk's see the table as it was before
/// the statement.
fn update_snapshot(ctx: &Ctx) -> bool {
    if ctx.outcome != Outcome::Diff {
        return false;
    }
    super::oracle::split_statements(ctx.sql).iter().any(|st| {
        let toks = tokens(st);
        let target = match toks.first().map(|t| upper(t)).as_deref() {
            Some("UPDATE") => toks.get(1),
            Some("DELETE") => toks.get(2),
            _ => None,
        };
        let Some(target) = target else { return false };
        has(st, "SELECT")
            && toks
                .iter()
                .filter(|t| upper(last_segment(t)) == upper(target) || upper(t) == upper(target))
                .count()
                >= 2
    })
}

/// "Aggregate of outer-query columns in a subquery": rejected by sqawk.
fn aggregate_outer(ctx: &Ctx) -> bool {
    ctx.outcome == Outcome::SqawkErr
        && ctx.sqawk_msg.contains("misuse of aggregate")
        && has(ctx.sql, "(SELECT")
}

/// "Derived table reading an outer-query column": rejected by sqawk.
fn derived_correlated(ctx: &Ctx) -> bool {
    ctx.outcome == Outcome::SqawkErr
        && ctx.sqawk_msg.contains("no such column")
        && (has(ctx.sql, "FROM (SELECT") || has(ctx.sql, "JOIN (SELECT"))
}

/// "CTE body reading an outer-query column": rejected by sqawk.
fn cte_correlated(ctx: &Ctx) -> bool {
    ctx.outcome == Outcome::SqawkErr
        && ctx.sqawk_msg.contains("no such column")
        && has(ctx.sql, "WITH ")
}

/// "ILIKE, LPAD, RPAD, LEFT, RIGHT, TRIM(... FROM ...)": sqawk extensions
/// SQLite lacks.
fn sqawk_extensions(ctx: &Ctx) -> bool {
    if ctx.outcome != Outcome::SqliteErr {
        return false;
    }
    let toks = tokens(ctx.sql);
    toks.windows(2).any(|w| {
        let u = upper(&w[0]);
        u == "ILIKE" || (matches!(u.as_str(), "LPAD" | "RPAD" | "LEFT" | "RIGHT") && w[1] == "(")
    }) || (has(ctx.sql, "TRIM(") && has(ctx.sql, " FROM '"))
        || (has(ctx.sql, "TRIM(")
            && toks.windows(3).any(|w| {
                upper(&w[0]) == "TRIM"
                    && matches!(upper(&w[2]).as_str(), "LEADING" | "TRAILING" | "BOTH")
            }))
}

/// "PRINTF width or precision past the length limit": an error in sqawk.
fn printf_limit(ctx: &Ctx) -> bool {
    ctx.outcome == Outcome::SqawkErr && ctx.sqawk_msg.contains("string or blob too big")
}

fn paren_depth(sql: &str) -> usize {
    let (mut d, mut m) = (0usize, 0usize);
    for c in sql.chars() {
        if c == '(' {
            d += 1;
            m = m.max(d);
        } else if c == ')' {
            d = d.saturating_sub(1);
        }
    }
    m
}

/// "Parser": `GLOB`, `LIMIT n, m`, `TRIM(s, chars)`, `REPLACE INTO`,
/// `x IN ()` do not parse, `REGEXP` is rejected, parentheses nest at most
/// 47 deep.
fn parser(ctx: &Ctx) -> bool {
    if ctx.outcome != Outcome::SqawkErr {
        return false;
    }
    let toks = tokens(ctx.sql);
    let u: Vec<String> = toks.iter().map(|t| upper(t)).collect();
    let construct = u.iter().any(|t| t == "GLOB" || t == "REGEXP")
        || u.windows(3).any(|w| w[0] == "LIMIT" && w[2] == ",")
        || u.windows(2).any(|w| w[0] == "REPLACE" && w[1] == "INTO")
        || u.windows(3)
            .any(|w| w[0] == "IN" && w[1] == "(" && w[2] == ")")
        || trim_two_args(&u)
        || paren_depth(ctx.sql) > 47;
    construct
}

/// `TRIM(x, y)` at the top level of the TRIM call.
fn trim_two_args(u: &[String]) -> bool {
    for (i, t) in u.iter().enumerate() {
        if t == "TRIM" && u.get(i + 1).is_some_and(|n| n == "(") {
            let mut depth = 0;
            for x in &u[i + 1..] {
                match x.as_str() {
                    "(" => depth += 1,
                    ")" => {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                    "," if depth == 1 => return true,
                    _ => {}
                }
            }
        }
    }
    false
}

/// "Unsupported syntax" and "Not implemented": parsed but rejected with
/// "... is not supported" (views, indexes, triggers, RETURNING, row values,
/// COLLATE, savepoints, ...).
fn unsupported_syntax(ctx: &Ctx) -> bool {
    let m = ctx.sqawk_msg.to_ascii_lowercase();
    ctx.outcome == Outcome::SqawkErr && (m.contains("not supported") || m.contains("unsupported"))
}

pub const RULES: &[Rule] = &[
    Rule {
        name: "column_types",
        doc: "Differences from SQLite: Column types",
        matches: column_types,
    },
    Rule {
        name: "null_vs_empty",
        doc: "Differences from SQLite: NULL vs ''",
        matches: null_vs_empty,
    },
    Rule {
        name: "cast_as_string",
        doc: "Differences from SQLite: CAST(x AS STRING)",
        matches: cast_as_string,
    },
    Rule {
        name: "upper_lower",
        doc: "Differences from SQLite: UPPER / LOWER",
        matches: upper_lower,
    },
    Rule {
        name: "trim",
        doc: "Differences from SQLite: TRIM(s)",
        matches: trim_whitespace,
    },
    Rule {
        name: "sum_avg_text",
        doc: "Differences from SQLite: SUM / AVG over text",
        matches: sum_avg_text,
    },
    Rule {
        name: "concat_ws_empty",
        doc: "Differences from SQLite: CONCAT_WS with an empty-string argument",
        matches: concat_ws_empty,
    },
    Rule {
        name: "bare_column",
        doc: "Differences from SQLite: Bare column in an aggregate query",
        matches: bare_column,
    },
    Rule {
        name: "insert_or_replace",
        doc: "Differences from SQLite: INSERT OR REPLACE, ON CONFLICT",
        matches: insert_or_replace,
    },
    Rule {
        name: "update_snapshot",
        doc: "Differences from SQLite: Subqueries in UPDATE / DELETE",
        matches: update_snapshot,
    },
    Rule {
        name: "aggregate_outer",
        doc: "Differences from SQLite: Aggregate of outer-query columns in a subquery",
        matches: aggregate_outer,
    },
    Rule {
        name: "derived_correlated",
        doc: "Differences from SQLite: Derived table reading an outer-query column",
        matches: derived_correlated,
    },
    Rule {
        name: "cte_correlated",
        doc: "Differences from SQLite: CTE body reading an outer-query column",
        matches: cte_correlated,
    },
    Rule {
        name: "sqawk_extensions",
        doc: "Differences from SQLite: ILIKE, LPAD, RPAD, LEFT, RIGHT, TRIM(... FROM ...)",
        matches: sqawk_extensions,
    },
    Rule {
        name: "printf_limit",
        doc: "Differences from SQLite: PRINTF width or precision past the length limit",
        matches: printf_limit,
    },
    Rule {
        name: "parser",
        doc: "Differences from SQLite: Parser",
        matches: parser,
    },
    Rule {
        name: "unsupported_syntax",
        doc: "Unsupported syntax; Not implemented",
        matches: unsupported_syntax,
    },
];

/// Where the documentation states rule `name`.
pub fn doc_of(name: &str) -> &'static str {
    RULES
        .iter()
        .find(|r| r.name == name)
        .map(|r| r.doc)
        .unwrap_or("")
}

/// The first rule covering `ctx`.
pub fn classify(ctx: &Ctx) -> Option<&'static Rule> {
    RULES.iter().find(|r| (r.matches)(ctx))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx<'a>(sql: &'a str, outcome: Outcome, msg: &'a str, e: &'a BTreeSet<String>) -> Ctx<'a> {
        Ctx {
            sql,
            outcome,
            sqawk_msg: msg,
            sqawk_sets: &[],
            sqlite_sets: &[],
            numtext_cols: e,
            text_cols: e,
            generated: false,
        }
    }

    #[test]
    fn rules_match_their_cases() {
        let e = BTreeSet::new();
        let r = |sql, o, m| classify(&ctx(sql, o, m, &e)).map(|r| r.name);
        assert_eq!(
            r("SELECT k FROM t1 WHERE a = '10'", Outcome::Diff, ""),
            Some("column_types")
        );
        assert_eq!(
            r("SELECT 1 LIMIT 2, 3", Outcome::SqawkErr, "Expected: end"),
            Some("parser")
        );
        assert_eq!(
            r(
                "SELECT TRIM(w, ' p') FROM u",
                Outcome::SqawkErr,
                "Expected: )"
            ),
            Some("parser")
        );
        assert_eq!(
            r(
                "CREATE VIEW v AS SELECT 1",
                Outcome::SqawkErr,
                "CREATE VIEW statements are not supported"
            ),
            Some("unsupported_syntax")
        );
        assert_eq!(
            r("SELECT LPAD('a', 3)", Outcome::SqliteErr, ""),
            Some("sqawk_extensions")
        );
        assert_eq!(r("SELECT k FROM t1 WHERE a = 10", Outcome::Diff, ""), None);
    }

    #[test]
    fn concat_ws_needs_rows_equal_without_separators() {
        let e = BTreeSet::new();
        let set = |rows: &[&str]| ResultSet {
            header: "c".into(),
            rows: rows.iter().map(|r| r.to_string()).collect(),
            source: 0,
        };
        let sql = "SELECT CONCAT_WS('-', a, b) AS c FROM t";
        let ours = [set(&["x--y", "z"])];
        let same = [set(&["x-y", "z"])];
        let other = [set(&["x-w", "z"])];
        let with = |theirs: &[ResultSet]| {
            let mut c = ctx(sql, Outcome::Diff, "", &e);
            c.sqawk_sets = &ours;
            c.sqlite_sets = theirs;
            classify(&c).map(|r| r.name)
        };
        assert_eq!(with(&same), Some("concat_ws_empty"));
        assert_eq!(with(&other), None);
        assert_eq!(
            concat_ws_separators("SELECT CONCAT_WS(', ', a), concat_ws('', b)"),
            vec![", ".to_string()]
        );
    }
}
