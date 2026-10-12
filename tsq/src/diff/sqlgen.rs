//! The random SQL generator: typed expressions over a schema, built as
//! shrinkable trees (`ast`).
//!
//! It descends from the retired Python diffh.py's `Gen` (typed expressions,
//! functions, CASE/CAST/COALESCE, LIKE/BETWEEN/IN, scalar/IN/EXISTS
//! subqueries with correlation, every join kind, GROUP BY/HAVING/FILTER,
//! windows with frames, compounds, DISTINCT, derived tables, DML) and adds
//! CTEs (plain, chained, recursive counters and tree walks), transactions,
//! INSERT/UPDATE/DELETE sequences, CREATE TABLE AS, ALTER TABLE ADD COLUMN,
//! printf and date functions.
//!
//! Every query it emits is meant to have one answer that sqawk and SQLite
//! must agree on:
//!
//! * comparisons, ORDER BY keys and MIN/MAX arguments are restricted by
//!   `Props::comparable` / `Props::sortable` (see schema.rs) so the numeric
//!   text rule ("Differences from SQLite", Column types row) never decides;
//! * UPPER/LOWER only see ASCII text, TRIM/LTRIM/RTRIM only text without
//!   tabs or line breaks, SUM/AVG/TOTAL only numbers (the UPPER, TRIM and
//!   SUM/AVG rows);
//! * no bare columns in aggregate queries, no outer columns inside derived
//!   tables or CTE bodies, no aggregate of outer columns in a subquery, no
//!   subquery on the table an UPDATE/DELETE changes, no unsupported syntax
//!   (RIGHT JOIN after an outer join, FULL JOIN before another join, RANGE
//!   offsets, GROUPS, sqawk-only functions);
//! * order-sensitive window functions get a total order (every source's
//!   key); LIMIT only follows an ORDER BY over every output column;
//! * a result set is compared in order only when its ORDER BY covers every
//!   output column with sortable values (`Node::Order`).

use std::rc::Rc;

use super::ast::{expr, list, nest, opt, seq, tok, Node};
use super::schema::{Col, Kind, Props, Rel, Schema};
use crate::rng::Rng;

#[derive(Clone)]
struct Src {
    alias: String,
    rel: Rc<Rel>,
}

/// Name scopes, outermost first; the last level is the current query's.
type Scope = Vec<Vec<Src>>;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum W {
    I,
    R,
    T,
    B,
}

/// A generated query (SELECT or compound) and its output columns.
struct Q {
    node: Node,
    props: Vec<Props>,
    card: usize,
}

/// One generated test: a sequence of statements.
#[derive(Clone, Debug)]
pub struct Case {
    /// A `List` of statements.
    pub script: Node,
}

impl Case {
    pub fn statements(&self) -> Vec<(String, bool)> {
        match &self.script {
            Node::List { items, .. } => items
                .iter()
                .map(|n| (n.text(), n.is_ordered()))
                .filter(|(s, _)| !s.trim().is_empty())
                .collect(),
            other => vec![(other.text(), other.is_ordered())],
        }
    }

    /// The script as sqawk reads it.
    pub fn sql(&self) -> String {
        let mut s = String::new();
        for (t, _) in self.statements() {
            s.push_str(&t);
            s.push_str(";\n");
        }
        s
    }
}

pub struct GenConfig {
    /// Large tables: no cross products, no correlated subqueries, bounded
    /// outputs.
    pub large: bool,
}

pub struct Gen<'s> {
    r: Rng,
    schema: &'s Schema,
    cfg: &'s GenConfig,
    n: usize,
    /// CTEs visible to the statement being generated.
    ctes: Vec<Rc<Rel>>,
    /// The table a DML statement changes: kept out of its subqueries.
    avoid: Option<String>,
    /// Rough row budget for cross products.
    max_cross: usize,
    /// Depth inside derived tables and CTE bodies. RIGHT / FULL joins stay
    /// at depth 0: SQLite 3.45 answers wrongly when it flattens a subquery
    /// holding one (`SELECT * FROM (... RIGHT JOIN ... UNION ALL ...)` gives
    /// a row the bare compound does not) or rejects a NATURAL join before
    /// it ("ON clause references tables to its right").
    nested: usize,
}

fn mk(kind: char, parts: Vec<Node>) -> Node {
    expr(kind, seq(parts))
}

fn kind_char(p: &Props) -> char {
    match p.kind {
        Kind::Int => 'i',
        Kind::Real => 'r',
        Kind::Text => 't',
        Kind::Mixed => 'm',
    }
}

/// Props of a SQL text literal's value; NULL satisfies every property.
fn lit_props(lit: &str) -> Props {
    if lit == "NULL" {
        return Props {
            kind: Kind::Text,
            ..Props::WORD
        };
    }
    let inner = &lit[1..lit.len() - 1];
    Props::of_text(&inner.replace("''", "'"))
}

fn sql_str(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

const TEXT_LITS: &[&str] = &[
    "'a'", "'b'", "''", "NULL", "'red'", "'x%y'", "'Beta'", "'日本'", "'abc'", "'Zed'",
];
const INT_LITS: &[&str] = &[
    "0",
    "1",
    "2",
    "-1",
    "3",
    "NULL",
    "10",
    "9223372036854775807",
    "1000000007",
];
const REAL_LITS: &[&str] = &[
    "0.5", "1.5", "-2.25", "NULL", "0.1", "1e10", "100.0", "-0.0",
];
const LIKE_PATS: &[&str] = &[
    "'a%'",
    "'%a%'",
    "'_'",
    "'%e_'",
    "'B%'",
    "'%'",
    "'%\\%%' ESCAPE '\\'",
    "'r_d%'",
    "'%o%'",
    "'S%'",
    "'%n'",
];
const PRINTF_FMTS: &[(&str, &str)] = &[
    ("'%5d|%s|%.2f'", "its"),
    ("'%d'", "i"),
    ("'%-6d|'", "i"),
    ("'%05d'", "i"),
    ("'%x'", "i"),
    ("'%o'", "i"),
    ("'%,d'", "i"),
    ("'%.3f'", "r"),
    ("'%e'", "r"),
    ("'%g'", "r"),
    ("'%10.4f'", "r"),
    ("'%!.15g'", "r"),
    ("'%s'", "t"),
    ("'%10s|'", "t"),
    ("'%-10s|'", "t"),
    ("'%.3s'", "t"),
    ("'%q'", "t"),
    ("'%Q'", "t"),
    ("'%c'", "t"),
    ("'%%|%d'", "i"),
];
const DATE_MODS: &[&str] = &[
    "'+1 day'",
    "'-1 month'",
    "'+1 year'",
    "'start of month'",
    "'start of year'",
    "'+12 hours'",
    "'weekday 0'",
    "'-7 days'",
    "'+90 minutes'",
    "'start of day'",
];
const DATE_FMTS: &[&str] = &[
    "'%Y-%m'",
    "'%d/%m/%Y'",
    "'%H:%M'",
    "'%j'",
    "'%W'",
    "'%w'",
    "'%s'",
    "'%Y'",
];

impl<'s> Gen<'s> {
    pub fn new(seed: u64, index: u64, schema: &'s Schema, cfg: &'s GenConfig) -> Self {
        Gen {
            r: Rng::stream(seed, index),
            schema,
            cfg,
            n: 0,
            ctes: Vec::new(),
            avoid: None,
            max_cross: if cfg.large { 0 } else { 6000 },
            nested: 0,
        }
    }

    fn alias(&mut self, prefix: &str) -> String {
        self.n += 1;
        format!("{prefix}{}", self.n)
    }

    fn chance(&mut self, p: f64) -> bool {
        self.r.chance(p)
    }

    fn pick<T: Clone>(&mut self, xs: &[T]) -> T {
        self.r.pick(xs).clone()
    }

    fn range(&mut self, lo: i64, hi: i64) -> i64 {
        self.r.range(lo, hi)
    }

    // ------------------------------------------------------------ names

    fn cols(&self, sc: &[Vec<Src>], pred: impl Fn(&Props) -> bool) -> Vec<(String, Props)> {
        let mut out = Vec::new();
        for level in sc {
            for s in level {
                for c in &s.rel.cols {
                    if pred(&c.props) {
                        out.push((format!("{}.{}", s.alias, c.name), c.props));
                    }
                }
            }
        }
        out
    }

    fn date_cols(&self, sc: &[Vec<Src>]) -> Vec<String> {
        let mut out = Vec::new();
        for level in sc {
            for s in level {
                for c in &s.rel.cols {
                    let n = c.name.as_str();
                    if (n.contains("date") || n == "ts") && c.props.kind != Kind::Int {
                        out.push(format!("{}.{}", s.alias, c.name));
                    }
                }
            }
        }
        out
    }

    /// Relations a FROM clause or subquery may name.
    fn rels(&self) -> Vec<Rc<Rel>> {
        let mut v: Vec<Rc<Rel>> = self
            .schema
            .rels
            .iter()
            .filter(|r| Some(&r.name) != self.avoid.as_ref())
            .cloned()
            .collect();
        v.extend(self.ctes.iter().cloned());
        v
    }

    // ------------------------------------------------------------ expressions

    fn expr(&mut self, sc: &Scope, w: W, d: u32) -> (Node, Props) {
        if w == W::B {
            return self.cond(sc, d);
        }
        if d == 0 || self.chance(0.3) {
            return self.leaf(sc, w);
        }
        match w {
            W::I | W::R => self.num_expr(sc, w, d),
            _ => self.text_expr(sc, d),
        }
    }

    fn leaf(&mut self, sc: &Scope, w: W) -> (Node, Props) {
        match w {
            W::B => self.cond(sc, 0),
            W::I => {
                let cs = self.cols(sc, |p| p.kind == Kind::Int);
                if !cs.is_empty() && self.chance(0.7) {
                    let (c, _) = self.pick(&cs);
                    return (expr('i', tok(c)), Props::INT);
                }
                let l = if self.chance(0.3) {
                    self.range(0, 60).to_string()
                } else {
                    self.pick(INT_LITS).to_string()
                };
                (expr('i', tok(l)), Props::INT)
            }
            W::R => {
                let cs = self.cols(sc, |p| p.is_num());
                if !cs.is_empty() && self.chance(0.7) {
                    let (c, p) = self.pick(&cs);
                    return (expr(kind_char(&p), tok(c)), Props::REAL);
                }
                (expr('r', tok(self.pick(REAL_LITS))), Props::REAL)
            }
            W::T => {
                let cs = self.cols(sc, |p| p.is_text());
                if !cs.is_empty() && self.chance(0.7) {
                    let (c, p) = self.pick(&cs);
                    return (expr('t', tok(c)), p);
                }
                self.text_lit()
            }
        }
    }

    fn text_lit(&mut self) -> (Node, Props) {
        let lit = if !self.schema.text_lits.is_empty() && self.chance(0.4) {
            sql_str(&self.pick(&self.schema.text_lits))
        } else {
            self.pick(TEXT_LITS).to_string()
        };
        let p = lit_props(&lit);
        (expr('t', tok(lit)), p)
    }

    /// A text expression whose properties satisfy `ok`, else a literal that
    /// does.
    fn text_with(&mut self, sc: &Scope, d: u32, ok: impl Fn(&Props) -> bool) -> (Node, Props) {
        for _ in 0..8 {
            let (n, p) = self.expr(sc, W::T, d);
            if ok(&p) {
                return (n, p);
            }
        }
        for _ in 0..20 {
            let (n, p) = self.text_lit();
            if ok(&p) {
                return (n, p);
            }
        }
        (expr('t', tok("'abc'")), Props::WORD)
    }

    fn num_expr(&mut self, sc: &Scope, w: W, d: u32) -> (Node, Props) {
        let k = if w == W::I { 'i' } else { 'r' };
        let props = if w == W::I { Props::INT } else { Props::REAL };
        let c = self.r.unit();
        if c < 0.30 {
            let ops: &[&str] = if w == W::I {
                &["+", "-", "*", "/", "%"]
            } else {
                &["+", "-", "*", "/"]
            };
            let op = self.pick(ops);
            // i64::MIN only as an arithmetic operand (overflow to real),
            // not where ABS / SUM would turn it into an error.
            let (l, _) = if w == W::I && self.chance(0.04) {
                (expr('i', tok("-9223372036854775808")), Props::INT)
            } else {
                self.expr(sc, w, d - 1)
            };
            let w2 = self.pick(&[W::I, w]);
            let (r, _) = self.expr(sc, w2, d - 1);
            return (
                mk(k, vec![tok("("), l, tok(format!(" {op} ")), r, tok(")")]),
                props,
            );
        }
        if c < 0.42 {
            let (b, _) = self.cond(sc, d - 1);
            let (x, _) = self.expr(sc, w, d - 1);
            let (y, _) = self.expr(sc, w, d - 1);
            let mut parts = vec![tok("CASE WHEN "), b, tok(" THEN "), x];
            if self.chance(0.8) {
                parts.push(opt(seq(vec![tok(" ELSE "), y])));
            }
            parts.push(tok(" END"));
            return (mk(k, parts), props);
        }
        if c < 0.52 {
            let f = self.pick(&["COALESCE", "IFNULL"]);
            let (x, _) = self.expr(sc, w, d - 1);
            let (y, _) = self.expr(sc, w, d - 1);
            return (
                mk(k, vec![tok(format!("{f}(")), x, tok(", "), y, tok(")")]),
                props,
            );
        }
        if c < 0.58 {
            let (b, _) = self.cond(sc, d - 1);
            let (x, _) = self.expr(sc, w, d - 1);
            let (y, _) = self.expr(sc, w, d - 1);
            return (
                mk(
                    k,
                    vec![tok("IIF("), b, tok(", "), x, tok(", "), y, tok(")")],
                ),
                props,
            );
        }
        if c < 0.64 {
            let (x, _) = self.expr(sc, w, d - 1);
            let (y, _) = self.expr(sc, w, d - 1);
            return (
                mk(k, vec![tok("NULLIF("), x, tok(", "), y, tok(")")]),
                props,
            );
        }
        if c < 0.74 {
            return self.scalar_sub(sc, w, d - 1);
        }
        if w == W::I {
            return (self.int_func(sc, d), Props::INT);
        }
        (self.real_func(sc, d), Props::REAL)
    }

    /// An integer expression without the i64 extremes, for SUM and ABS:
    /// their overflow is an error, and whether a query reaches it depends
    /// on what each engine's planner skips (SQLite omits unused LEFT JOINs
    /// and empty branches; sqawk computes every derived table and CTE it
    /// names), which is not a difference in answers.
    fn tame_int(&mut self, sc: &Scope, d: u32) -> Node {
        for _ in 0..6 {
            let (n, _) = self.expr(sc, W::I, d);
            let t = n.text();
            let big = self.schema.big_cols.iter().any(|c| {
                t.match_indices(&format!(".{c}")).any(|(i, m)| {
                    !t[i + m.len()..]
                        .chars()
                        .next()
                        .is_some_and(|ch| ch.is_alphanumeric() || ch == '_')
                })
            });
            if !t.contains("922337203685477580") && !big {
                return n;
            }
        }
        expr('i', tok("1"))
    }

    fn int_func(&mut self, sc: &Scope, d: u32) -> Node {
        let dates = self.date_cols(sc);
        match self.range(0, 17) {
            0 => {
                let (t, _) = self.expr(sc, W::T, d - 1);
                mk('i', vec![tok("LENGTH("), t, tok(")")])
            }
            1 => {
                let x = self.tame_int(sc, 0);
                mk('i', vec![tok("ABS("), x, tok(")")])
            }
            2 => {
                let (t, _) = self.expr(sc, W::T, d - 1);
                let needle = self.pick(&["'a'", "'e'", "''", "'o'", "' '"]);
                mk('i', vec![tok("INSTR("), t, tok(format!(", {needle})"))])
            }
            3 => {
                let cs = self.cols(sc, |p| !matches!(p.kind, Kind::Int));
                let c = if cs.is_empty() {
                    "'12x'".to_string()
                } else {
                    self.pick(&cs).0
                };
                mk('i', vec![tok(format!("CAST({c} AS INTEGER)"))])
            }
            4 | 5 => {
                let f = if self.chance(0.5) { "MAX" } else { "MIN" };
                let (x, _) = self.expr(sc, W::I, d - 1);
                let (y, _) = self.expr(sc, W::I, d - 1);
                mk('i', vec![tok(format!("{f}(")), x, tok(", "), y, tok(")")])
            }
            6 => {
                let (t, _) = self.expr(sc, W::T, d - 1);
                mk('i', vec![tok("UNICODE("), t, tok(")")])
            }
            7 => {
                let (x, _) = self.expr(sc, W::I, d - 1);
                mk('i', vec![tok("SIGN("), x, tok(")")])
            }
            8 => {
                let op = self.pick(&["&", "|"]);
                let (x, _) = self.expr(sc, W::I, d - 1);
                let (y, _) = self.expr(sc, W::I, d - 1);
                mk('i', vec![tok("("), x, tok(format!(" {op} ")), y, tok(")")])
            }
            9 => {
                let (x, _) = self.expr(sc, W::I, d - 1);
                let s = self.range(0, 3);
                let op = self.pick(&["<<", ">>"]);
                mk('i', vec![tok("("), x, tok(format!(" {op} {s})"))])
            }
            10 if !dates.is_empty() => {
                let c = self.pick(&dates);
                mk('i', vec![tok(format!("unixepoch({c})"))])
            }
            11 if !dates.is_empty() => {
                let c = self.pick(&dates);
                let f = self.pick(&["'%Y'", "'%m'", "'%d'", "'%j'"]);
                mk(
                    'i',
                    vec![tok(format!("CAST(strftime({f}, {c}) AS INTEGER)"))],
                )
            }
            12 => {
                let (x, _) = self.expr(sc, W::I, d - 1);
                let mut parts = vec![tok("CASE "), x];
                for _ in 0..self.range(1, 3) {
                    let v = self.range(-1, 4);
                    let (y, _) = self.expr(sc, W::I, d - 1);
                    parts.push(tok(format!(" WHEN {v} THEN ")));
                    parts.push(y);
                }
                let (e, _) = self.expr(sc, W::I, d - 1);
                parts.push(opt(seq(vec![tok(" ELSE "), e])));
                parts.push(tok(" END"));
                mk('i', parts)
            }
            13 => {
                let (b, _) = self.cond(sc, d - 1);
                mk('i', vec![tok("("), b, tok(")")])
            }
            14 => {
                let (x, _) = self.expr(sc, W::R, d - 1);
                mk('i', vec![tok("LENGTH(CAST("), x, tok(" AS TEXT))")])
            }
            15 => {
                let (x, _) = self.expr(sc, W::I, d - 1);
                mk('i', vec![tok("(~"), x, tok(")")])
            }
            _ => {
                let (x, _) = self.expr(sc, W::I, d - 1);
                // The space keeps `- -1` from reading as a `--` comment.
                mk('i', vec![tok("(- "), x, tok(")")])
            }
        }
    }

    fn real_func(&mut self, sc: &Scope, d: u32) -> Node {
        let dates = self.date_cols(sc);
        match self.range(0, 10) {
            0 => {
                let (x, _) = self.expr(sc, W::R, d - 1);
                let n = self.range(0, 3);
                mk('r', vec![tok("ROUND("), x, tok(format!(", {n})"))])
            }
            1 => {
                let cs = self.cols(sc, |p| matches!(p.kind, Kind::Mixed | Kind::Text));
                let c = if cs.is_empty() {
                    "'1.5'".to_string()
                } else {
                    self.pick(&cs).0
                };
                mk('r', vec![tok(format!("({c} + 0.5)"))])
            }
            2 => {
                let (x, _) = self.expr(sc, W::I, d - 1);
                mk('r', vec![tok("("), x, tok(" * 1.0)")])
            }
            3 => {
                let cs = self.cols(sc, |p| p.kind != Kind::Real);
                let c = if cs.is_empty() {
                    "'2.5x'".to_string()
                } else {
                    self.pick(&cs).0
                };
                mk('r', vec![tok(format!("CAST({c} AS REAL)"))])
            }
            4 => {
                let (x, _) = self.expr(sc, W::R, d - 1);
                mk('r', vec![tok("ABS("), x, tok(")")])
            }
            5 if !dates.is_empty() => {
                let c = self.pick(&dates);
                mk('r', vec![tok(format!("julianday({c})"))])
            }
            6 => {
                let (x, _) = self.expr(sc, W::I, d - 1);
                mk('r', vec![tok("("), x, tok(" / 2.0)")])
            }
            7 => {
                let f = self.pick(&["CEIL", "FLOOR"]);
                let (x, _) = self.expr(sc, W::R, d - 1);
                mk('r', vec![tok(format!("{f}(")), x, tok(")")])
            }
            8 => {
                let (x, _) = self.expr(sc, W::R, d - 1);
                mk('r', vec![tok("ROUND("), x, tok(")")])
            }
            _ => {
                let (x, _) = self.expr(sc, W::R, d - 1);
                let (y, _) = self.expr(sc, W::R, d - 1);
                mk('r', vec![tok("("), x, tok(" / "), y, tok(")")])
            }
        }
    }

    fn text_expr(&mut self, sc: &Scope, d: u32) -> (Node, Props) {
        let c = self.r.unit();
        if c < 0.22 {
            let (l, lp) = self.expr(sc, W::T, d - 1);
            let w2 = self.pick(&[W::T, W::I]);
            let (r, rp) = self.expr(sc, w2, d - 1);
            let p = Props {
                kind: Kind::Text,
                nonnum: lp.letter,
                letter: lp.letter,
                ascii: lp.ascii && rp.ascii,
                wssafe: lp.wssafe && rp.wssafe,
                taff: false,
            };
            return (mk('t', vec![tok("("), l, tok(" || "), r, tok(")")]), p);
        }
        if c < 0.32 {
            let (x, _) = self.expr(sc, W::I, d - 1);
            let v = self.range(0, 3);
            let (a, ap) = self.expr(sc, W::T, d - 1);
            let (b, bp) = self.expr(sc, W::T, d - 1);
            return (
                mk(
                    't',
                    vec![
                        tok("CASE "),
                        x,
                        tok(format!(" WHEN {v} THEN ")),
                        a,
                        tok(" ELSE "),
                        b,
                        tok(" END"),
                    ],
                ),
                Props::join(ap, bp),
            );
        }
        if c < 0.42 {
            let (a, ap) = self.expr(sc, W::T, d - 1);
            let (b, bp) = self.expr(sc, W::T, d - 1);
            let (l, lp) = self.leaf(sc, W::T);
            return (
                mk(
                    't',
                    vec![tok("COALESCE("), a, tok(", "), b, tok(", "), l, tok(")")],
                ),
                Props::join(Props::join(ap, bp), lp),
            );
        }
        if c < 0.48 {
            return self.scalar_sub(sc, W::T, d - 1);
        }
        self.text_func(sc, d)
    }

    fn text_func(&mut self, sc: &Scope, d: u32) -> (Node, Props) {
        let unknown = |ascii: bool, wssafe: bool| Props {
            ascii,
            wssafe,
            ..Props::TEXT
        };
        match self.range(0, 19) {
            0 => {
                let (t, p) = self.expr(sc, W::T, d - 1);
                let a = self.range(-3, 4);
                let b = self.range(-2, 4);
                let keep = a == 1 && b > 0 && p.letter;
                let q = Props {
                    nonnum: keep,
                    letter: keep,
                    ..unknown(p.ascii, p.wssafe)
                };
                (
                    mk('t', vec![tok("SUBSTR("), t, tok(format!(", {a}, {b})"))]),
                    q,
                )
            }
            1 => {
                let (t, p) = self.expr(sc, W::T, d - 1);
                let from = self.pick(&["'a'", "'%'", "''", "'ph'", "' '"]);
                let to = self.pick(&["'Z'", "''", "'aa'"]);
                (
                    mk(
                        't',
                        vec![tok("REPLACE("), t, tok(format!(", {from}, {to})"))],
                    ),
                    unknown(p.ascii, p.wssafe),
                )
            }
            2 | 3 => {
                let f = self.pick(&["LOWER", "UPPER"]);
                let (t, p) = self.text_with(sc, d - 1, |p| p.ascii);
                (mk('t', vec![tok(format!("{f}(")), t, tok(")")]), p)
            }
            4 => {
                let f = self.pick(&["LTRIM", "RTRIM", "TRIM"]);
                let (t, p) = self.text_with(sc, d - 1, |p| p.wssafe);
                let q = Props {
                    letter: p.letter && f == "RTRIM",
                    nonnum: p.letter && f == "RTRIM",
                    ..unknown(p.ascii, true)
                };
                (mk('t', vec![tok(format!("{f}(")), t, tok(")")]), q)
            }
            5 => {
                // TRIM(x, chars) does not parse in sqawk ("Differences",
                // Parser row); the one-sided forms do.
                let f = self.pick(&["LTRIM", "RTRIM"]);
                let (t, p) = self.expr(sc, W::T, d - 1);
                let set = self.pick(&["'a '", "'xyz'", "'0'", "'e'"]);
                (
                    mk('t', vec![tok(format!("{f}(")), t, tok(format!(", {set})"))]),
                    unknown(p.ascii, p.wssafe),
                )
            }
            6 | 7 => {
                let (fmt, args) = self.pick(PRINTF_FMTS);
                let mut parts = vec![tok(format!("PRINTF({fmt}"))];
                for a in args.chars() {
                    let w = match a {
                        'i' => W::I,
                        'r' => W::R,
                        _ => W::T,
                    };
                    let (x, _) = self.expr(sc, w, d - 1);
                    parts.push(tok(", "));
                    parts.push(x);
                }
                parts.push(tok(")"));
                (mk('t', parts), unknown(false, false))
            }
            8 => {
                let (x, _) = self.expr(sc, W::I, d - 1);
                (
                    mk('t', vec![tok("CAST("), x, tok(" AS TEXT)")]),
                    Props {
                        taff: true,
                        ..unknown(true, true)
                    },
                )
            }
            9 => {
                let cs = self.cols(sc, |_| true);
                let c = if cs.is_empty() {
                    "1".to_string()
                } else {
                    self.pick(&cs).0
                };
                (mk('t', vec![tok(format!("TYPEOF({c})"))]), Props::WORD)
            }
            10 => {
                let w = self.pick(&[W::T, W::I, W::R]);
                let (x, _) = self.expr(sc, w, d - 1);
                (mk('t', vec![tok("HEX("), x, tok(")")]), unknown(true, true))
            }
            11 => {
                let (b, _) = self.cond(sc, d - 1);
                let (l, p) = self.leaf(sc, W::T);
                (
                    mk('t', vec![tok("IIF("), b, tok(", "), l, tok(", NULL)")]),
                    p,
                )
            }
            12 => {
                let cs = self.cols(sc, |_| true);
                let (x, p) = if cs.is_empty() || self.chance(0.3) {
                    self.expr(sc, W::R, d - 1)
                } else {
                    let (c, p) = self.pick(&cs);
                    (tok(c), p)
                };
                let q = Props {
                    nonnum: p.is_text(),
                    ..unknown(p.ascii, p.wssafe)
                };
                (mk('t', vec![tok("QUOTE("), x, tok(")")]), q)
            }
            13 => {
                let n = self.range(1, 3);
                let codes: Vec<String> = (0..n)
                    .map(|_| {
                        let upper = self.chance(0.5);
                        let c = self.range(0, 25) + if upper { 65 } else { 97 };
                        c.to_string()
                    })
                    .collect();
                (
                    mk('t', vec![tok(format!("CHAR({})", codes.join(", ")))]),
                    Props::WORD,
                )
            }
            14 => {
                let f = self.pick(&["CONCAT", "CONCAT_WS"]);
                let mut parts = vec![tok(format!("{f}("))];
                if f == "CONCAT_WS" {
                    parts.push(tok(self.pick(&["'-'", "', '", "''"])));
                    parts.push(tok(", "));
                }
                let n = self.range(1, 3);
                let mut all_letter = true;
                let mut ascii = true;
                let mut ws = true;
                for i in 0..n {
                    if i > 0 {
                        parts.push(tok(", "));
                    }
                    let w = self.pick(&[W::T, W::T, W::I]);
                    let (x, p) = self.expr(sc, w, d - 1);
                    all_letter &= p.letter && w == W::T;
                    ascii &= p.ascii;
                    ws &= p.wssafe;
                    parts.push(x);
                }
                parts.push(tok(")"));
                let _ = all_letter;
                (mk('t', parts), unknown(ascii, ws))
            }
            15 | 16 => self.date_func(sc),
            _ => {
                let (x, _) = self.expr(sc, W::I, d - 1);
                let (t, p) = self.leaf(sc, W::T);
                (
                    mk(
                        't',
                        vec![tok("CASE WHEN "), x, tok(" > 0 THEN "), t, tok(" END")],
                    ),
                    p,
                )
            }
        }
    }

    fn date_func(&mut self, sc: &Scope) -> (Node, Props) {
        let dates = self.date_cols(sc);
        let arg = if !dates.is_empty() && self.chance(0.8) {
            self.pick(&dates)
        } else {
            self.pick(&[
                "'2024-01-31'",
                "'2024-02-29 12:34:56'",
                "'2000-01-01T00:00:00'",
                "'1970-01-01'",
                "'2459000.5'",
                "'12:30'",
                "'bad'",
            ])
            .to_string()
        };
        let p = Props {
            kind: Kind::Text,
            nonnum: true,
            letter: false,
            ascii: true,
            wssafe: true,
            taff: false,
        };
        match self.range(0, 3) {
            0 => {
                let f = self.pick(&["date", "datetime", "time"]);
                let mut s = format!("{f}({arg}");
                for _ in 0..self.range(0, 2) {
                    s.push_str(", ");
                    s.push_str(self.pick(DATE_MODS));
                }
                s.push(')');
                (mk('t', vec![tok(s)]), p)
            }
            1 => {
                let f = self.pick(DATE_FMTS);
                let numeric = matches!(f, "'%j'" | "'%W'" | "'%w'" | "'%s'" | "'%Y'");
                (
                    mk('t', vec![tok(format!("strftime({f}, {arg})"))]),
                    Props {
                        nonnum: !numeric,
                        ..p
                    },
                )
            }
            2 => (
                mk(
                    't',
                    vec![tok(format!(
                        "date({arg}, 'start of month', '+1 month', '-1 day')"
                    ))],
                ),
                p,
            ),
            _ => (
                mk('t', vec![tok(format!("timediff({arg}, '2024-01-01')"))]),
                Props { nonnum: false, ..p },
            ),
        }
    }

    // ------------------------------------------------------------ conditions

    fn cond(&mut self, sc: &Scope, d: u32) -> (Node, Props) {
        let c = self.r.unit();
        let b = |n: Node| (n, Props::INT);
        if d > 0 && c < 0.2 {
            let (x, _) = self.cond(sc, d - 1);
            let (y, _) = self.cond(sc, d - 1);
            let op = self.pick(&["AND", "OR"]);
            return b(mk(
                'b',
                vec![tok("("), x, tok(format!(" {op} ")), y, tok(")")],
            ));
        }
        if d > 0 && c < 0.25 {
            let (x, _) = self.cond(sc, d - 1);
            return b(mk('b', vec![tok("NOT ("), x, tok(")")]));
        }
        let dm = d.saturating_sub(1);
        if c < 0.5 {
            return b(self.comparison(sc, dm));
        }
        if c < 0.58 {
            let w = self.pick(&[W::I, W::R, W::T]);
            let (x, _) = if self.chance(0.3) {
                let cs = self.cols(sc, |_| true);
                if cs.is_empty() {
                    self.expr(sc, w, dm)
                } else {
                    let (c, p) = self.pick(&cs);
                    (expr(kind_char(&p), tok(c)), p)
                }
            } else {
                self.expr(sc, w, dm)
            };
            let not = self.pick(&["", "NOT "]);
            return b(mk('b', vec![x, tok(format!(" IS {not}NULL"))]));
        }
        if c < 0.65 {
            let (x, _) = self.expr(sc, W::I, dm);
            let (lo, _) = self.leaf(sc, W::I);
            let (hi, _) = self.leaf(sc, W::I);
            let not = self.pick(&["", "NOT "]);
            return b(mk(
                'b',
                vec![x, tok(format!(" {not}BETWEEN ")), lo, tok(" AND "), hi],
            ));
        }
        if c < 0.72 {
            let not = self.pick(&["", "NOT "]);
            if self.chance(0.7) {
                let (x, _) = self.expr(sc, W::I, dm);
                let n = self.range(1, 4);
                let items: Vec<Node> = (0..n)
                    .map(|_| tok(self.pick(&["1", "2", "3", "NULL", "42", "-7", "0"])))
                    .collect();
                return b(mk(
                    'b',
                    vec![
                        x,
                        tok(format!(" {not}IN (")),
                        list(", ", 1, items),
                        tok(")"),
                    ],
                ));
            }
            let (x, _) = self.expr(sc, W::T, dm);
            let n = self.range(1, 3);
            let mut items = Vec::new();
            for _ in 0..n {
                let (l, _) = self.text_with(sc, 0, |p| p.letter);
                items.push(l);
            }
            return b(mk(
                'b',
                vec![
                    x,
                    tok(format!(" {not}IN (")),
                    list(", ", 1, items),
                    tok(")"),
                ],
            ));
        }
        if c < 0.78 {
            let (x, _) = self.expr(sc, W::T, dm);
            let not = self.pick(&["", "NOT "]);
            let pat = self.pick(LIKE_PATS);
            return b(mk('b', vec![x, tok(format!(" {not}LIKE {pat}"))]));
        }
        if d > 0 && c < 0.9 {
            return b(self.in_sub(sc, d - 1));
        }
        if d > 0 {
            return b(self.exists_sub(sc, d - 1));
        }
        let (x, _) = self.leaf(sc, W::I);
        let (y, _) = self.leaf(sc, W::I);
        b(mk('b', vec![x, tok(" > "), y]))
    }

    fn comparison(&mut self, sc: &Scope, d: u32) -> Node {
        let op = self.pick(&[
            "=",
            "<>",
            "<",
            "<=",
            ">",
            ">=",
            "IS NOT DISTINCT FROM",
            "IS DISTINCT FROM",
            "IS",
            "IS NOT",
            "!=",
            "==",
        ]);
        let c = self.r.unit();
        let (l, r) = if c < 0.6 {
            let w1 = self.pick(&[W::I, W::I, W::R]);
            let (l, _) = self.expr(sc, w1, d);
            let w2 = self.pick(&[W::I, W::R]);
            let (r, _) = self.expr(sc, w2, d);
            (l, r)
        } else if c < 0.9 {
            let (l, lp) = self.expr(sc, W::T, d);
            let (r, _) = self.text_with(sc, d, |p| lp.comparable(p));
            (l, r)
        } else {
            let (l, _) = self.expr(sc, W::I, d);
            let (r, _) = self.text_with(sc, d, |p| Props::INT.comparable(p));
            (l, r)
        };
        mk('b', vec![tok("("), l, tok(format!(" {op} ")), r, tok(")")])
    }

    // ------------------------------------------------------------ subqueries

    /// The names a subquery's conditions may use: the enclosing scopes and
    /// its own table, or (large tables) its own table only -- a correlated
    /// subquery over 100k rows is a test of indexing, not of answers
    /// (SQLite builds an automatic index; sqawk rescans).
    fn sub_scope(&self, sc: &Scope, src: &Src) -> Scope {
        let mut full = if self.cfg.large {
            Vec::new()
        } else {
            sc.clone()
        };
        full.push(vec![src.clone()]);
        full
    }

    fn sub_src(&mut self) -> Src {
        let rels = self.rels();
        let rel = self.pick(&rels);
        Src {
            alias: self.alias("t"),
            rel,
        }
    }

    /// A correlation condition tying `inner` to the enclosing scopes, or an
    /// inner-only condition when correlation is off (large tables).
    fn corr(&mut self, sc: &Scope, inner: &Src) -> Node {
        let inner_i: Vec<String> = inner
            .rel
            .cols
            .iter()
            .filter(|c| c.props.kind == Kind::Int)
            .map(|c| format!("{}.{}", inner.alias, c.name))
            .collect();
        let outer = self.cols(sc, |p| p.kind == Kind::Int);
        if inner_i.is_empty() {
            return tok("1");
        }
        if outer.is_empty() || self.cfg.large {
            let c = self.pick(&inner_i);
            let v = self.range(0, 50);
            return tok(format!("{c} < {v}"));
        }
        let i = self.pick(&inner_i);
        let op = self.pick(&["=", "=", "<", ">="]);
        let o = self.pick(&outer).0;
        tok(format!("{i} {op} {o}"))
    }

    fn scalar_sub(&mut self, sc: &Scope, w: W, d: u32) -> (Node, Props) {
        let mut src = self.sub_src();
        if w == W::T {
            // Needs a sortable text column for MIN/MAX.
            for _ in 0..10 {
                if src
                    .rel
                    .cols
                    .iter()
                    .any(|c| c.props.is_text() && c.props.sortable())
                {
                    break;
                }
                src = self.sub_src();
            }
        }
        let inner: Scope = vec![vec![src.clone()]];
        let full = self.sub_scope(sc, &src);
        let mut wh = vec![self.corr(sc, &src)];
        if d > 0 && self.chance(0.5) {
            let (b, _) = self.cond(&full, d - 1);
            wh.push(opt(seq(vec![tok(" AND "), b])));
        }
        let from = format!(" FROM {} {} WHERE ", src.rel.name, src.alias);
        if w == W::T {
            let tc: Vec<(String, Props)> = self.cols(&inner, |p| p.is_text() && p.sortable());
            if tc.is_empty() {
                return (expr('t', tok("'none'")), Props::WORD);
            }
            let (c, p) = self.pick(&tc);
            let f = self.pick(&["MAX", "MIN"]);
            let mut parts = vec![tok(format!("(SELECT {f}({c}){from}"))];
            parts.extend(wh);
            parts.push(tok(")"));
            return (mk('t', vec![nest(seq(parts))]), p);
        }
        // A key lookup returns at most one row.
        if let Some(k) = src.rel.key {
            if self.chance(0.2) {
                let cs: Vec<&Col> = src.rel.cols.iter().filter(|c| c.props.is_num()).collect();
                if !cs.is_empty() {
                    let c = cs[self.r.below(cs.len())].name.clone();
                    let (o, _) = self.expr(sc, W::I, 0);
                    let kname = &src.rel.cols[k].name;
                    let props = if w == W::I { Props::INT } else { Props::REAL };
                    let col_props = src.rel.cols.iter().find(|x| x.name == c).unwrap().props;
                    if w == W::R || col_props.kind == Kind::Int {
                        return (
                            mk(
                                kind_char(&props),
                                vec![nest(seq(vec![
                                    tok(format!(
                                        "(SELECT {a}.{c} FROM {} {a} WHERE {a}.{kname} = ",
                                        src.rel.name,
                                        a = src.alias
                                    )),
                                    o,
                                    tok(")"),
                                ]))],
                            ),
                            props,
                        );
                    }
                }
            }
        }
        let mut aggs = vec![
            "COUNT(*)",
            "SUM(#)",
            "MAX(#)",
            "MIN(#)",
            "COUNT(DISTINCT #)",
        ];
        if w == W::R {
            aggs.extend(["TOTAL(#)", "AVG(#)"]);
        }
        let agg = self.pick(&aggs);
        let mut parts = Vec::new();
        if let Some((pre, post)) = agg.split_once('#') {
            let a = self.tame_int(&inner, d.min(1));
            parts.push(tok(format!("(SELECT {pre}")));
            parts.push(a);
            parts.push(tok(format!("{post}{from}")));
        } else {
            parts.push(tok(format!("(SELECT {agg}{from}")));
        }
        parts.extend(wh);
        parts.push(tok(")"));
        let props = if w == W::I { Props::INT } else { Props::REAL };
        (mk(kind_char(&props), vec![nest(seq(parts))]), props)
    }

    fn in_sub(&mut self, sc: &Scope, d: u32) -> Node {
        let src = self.sub_src();
        let full = self.sub_scope(sc, &src);
        let ic: Vec<String> = src
            .rel
            .cols
            .iter()
            .filter(|c| c.props.kind == Kind::Int)
            .map(|c| format!("{}.{}", src.alias, c.name))
            .collect();
        let tc: Vec<(String, Props)> = src
            .rel
            .cols
            .iter()
            .filter(|c| c.props.is_text())
            .map(|c| (format!("{}.{}", src.alias, c.name), c.props))
            .collect();
        let w = if self.chance(0.5) || self.cfg.large {
            self.corr(sc, &src)
        } else {
            self.cond(&full, d).0
        };
        let mut wh = vec![w];
        if d > 0 && self.chance(0.4) {
            let (b, _) = self.cond(&full, d - 1);
            wh.push(opt(seq(vec![tok(" AND "), b])));
        }
        let not = self.pick(&["", "NOT "]);
        let (lhs, col) = if !tc.is_empty() && (ic.is_empty() || self.chance(0.25)) {
            let (c, p) = self.pick(&tc);
            let (l, _) = self.text_with(sc, 0, |q| q.comparable(&p));
            (l, c)
        } else if !ic.is_empty() {
            let (l, _) = self.expr(sc, W::I, 0);
            (l, self.pick(&ic))
        } else {
            return mk('b', vec![tok("1")]);
        };
        let mut parts = vec![
            lhs,
            tok(format!(
                " {not}IN (SELECT {col} FROM {} {} WHERE ",
                src.rel.name, src.alias
            )),
        ];
        parts.extend(wh);
        parts.push(tok(")"));
        mk('b', vec![nest(seq(parts))])
    }

    fn exists_sub(&mut self, sc: &Scope, d: u32) -> Node {
        let src = self.sub_src();
        let full = self.sub_scope(sc, &src);
        let mut wh = vec![self.corr(sc, &src)];
        if d > 0 {
            let (b, _) = self.cond(&full, d - 1);
            wh.push(opt(seq(vec![tok(" AND "), b])));
        }
        let not = self.pick(&["", "NOT "]);
        let mut parts = vec![tok(format!(
            "{not}EXISTS (SELECT 1 FROM {} {} WHERE ",
            src.rel.name, src.alias
        ))];
        parts.extend(wh);
        parts.push(tok(")"));
        mk('b', vec![nest(seq(parts))])
    }

    // ------------------------------------------------------------ FROM

    /// Equi-join candidates between `left` sources and `right`: foreign
    /// keys first, else (small tables only) any two integer columns. A
    /// foreign key whose columns hold values that compare differently in
    /// the two engines (edge values such as `007` in a key column) is
    /// skipped: join equality follows the comparison rule.
    fn join_cond(&mut self, left: &[Src], right: &Src) -> Option<(String, bool)> {
        let mut fk = Vec::new();
        for l in left {
            for (c, t, tc) in &l.rel.fks {
                let (a, b) = (&l.rel.cols[*c], &right.rel.cols.get(*tc));
                if *t == right.rel.name && b.is_some_and(|b| a.props.comparable(&b.props)) {
                    fk.push(format!(
                        "{}.{} = {}.{}",
                        l.alias, l.rel.cols[*c].name, right.alias, right.rel.cols[*tc].name
                    ));
                }
            }
            for (c, t, tc) in &right.rel.fks {
                let (a, b) = (&right.rel.cols[*c], &l.rel.cols.get(*tc));
                if *t == l.rel.name && b.is_some_and(|b| a.props.comparable(&b.props)) {
                    fk.push(format!(
                        "{}.{} = {}.{}",
                        l.alias, l.rel.cols[*tc].name, right.alias, right.rel.cols[*c].name
                    ));
                }
            }
            if l.rel.name == right.rel.name {
                if let Some(k) = l.rel.key {
                    let n = &l.rel.cols[k].name;
                    fk.push(format!("{}.{n} = {}.{n}", l.alias, right.alias));
                }
            }
        }
        if !fk.is_empty() && (self.cfg.large || self.chance(0.7)) {
            return Some((self.pick(&fk), true));
        }
        if self.cfg.large {
            return None;
        }
        let li: Vec<String> = left
            .iter()
            .flat_map(|s| {
                s.rel
                    .cols
                    .iter()
                    .filter(|c| c.props.kind == Kind::Int)
                    .map(move |c| format!("{}.{}", s.alias, c.name))
            })
            .collect();
        let ri: Vec<String> = right
            .rel
            .cols
            .iter()
            .filter(|c| c.props.kind == Kind::Int)
            .map(|c| format!("{}.{}", right.alias, c.name))
            .collect();
        if li.is_empty() || ri.is_empty() {
            return None;
        }
        Some((format!("{} = {}", self.pick(&li), self.pick(&ri)), false))
    }

    /// A derived table usable as a FROM item.
    fn derived_rel(&mut self, d: u32) -> (Node, Rc<Rel>) {
        let alias = self.alias("d");
        self.nested += 1;
        let q = if self.chance(0.25) {
            self.compound_q(true)
        } else {
            self.select_q(&Vec::new(), d, true, false)
        };
        self.nested -= 1;
        let cols = q
            .props
            .iter()
            .enumerate()
            .map(|(i, p)| Col {
                name: format!("c{}", i + 1),
                props: *p,
            })
            .collect();
        let rel = Rc::new(Rel {
            name: alias.clone(),
            cols,
            key: None,
            card: q.card,
            fks: vec![],
        });
        (
            seq(vec![tok("("), nest(q.node), tok(format!(") {alias}"))]),
            rel,
        )
    }

    fn gen_from(&mut self, d: u32) -> (Node, Vec<Src>, usize) {
        let weights: &[usize] = if self.cfg.large {
            &[1, 1, 1, 2, 2, 3]
        } else {
            &[1, 1, 2, 2, 3, 3, 4]
        };
        let n = self.pick(weights);
        let mut tabs: Vec<Src> = Vec::new();
        let mut parts: Vec<Node> = Vec::new();
        let mut card: usize = 1;
        let mut outer_seen = false;
        for i in 0..n {
            let (item, src) = if d > 0 && !self.cfg.large && self.chance(0.12) {
                let (node, rel) = self.derived_rel(d - 1);
                let alias = rel.name.clone();
                (node, Src { alias, rel })
            } else {
                let rels = self.rels();
                let rel = self.pick(&rels);
                let alias = self.alias("t");
                (tok(format!("{} {}", rel.name, alias)), Src { alias, rel })
            };
            if i == 0 {
                card = src.rel.card.max(1);
                parts.push(item);
                tabs.push(src);
                continue;
            }
            let last = i == n - 1;
            let mut kinds = vec![
                "JOIN",
                "LEFT JOIN",
                "LEFT JOIN",
                "INNER JOIN",
                "JOIN",
                "LEFT JOIN",
            ];
            let cross_ok = card.saturating_mul(src.rel.card.max(1)) <= self.max_cross;
            if cross_ok {
                kinds.extend(["CROSS JOIN", ","]);
            }
            if last && !outer_seen && self.nested == 0 {
                kinds.extend(["RIGHT JOIN", "FULL JOIN"]);
            }
            let mut jt = self.pick(&kinds);
            let cond = if jt == "CROSS JOIN" || jt == "," {
                None
            } else {
                match self.join_cond(&tabs, &src) {
                    Some(c) => Some(c),
                    None if cross_ok => {
                        jt = "CROSS JOIN";
                        None
                    }
                    None => {
                        // No way to join this one cheaply: stop here.
                        break;
                    }
                }
            };
            if jt.starts_with("LEFT") || jt.starts_with("RIGHT") || jt.starts_with("FULL") {
                outer_seen = true;
            }
            match &cond {
                None => {
                    card = card.saturating_mul(src.rel.card.max(1));
                    parts.push(opt(seq(vec![tok(format!(" {jt} ")), item])));
                }
                Some((c, is_fk)) => {
                    card = if *is_fk {
                        card.max(src.rel.card)
                    } else {
                        card.saturating_mul(src.rel.card.max(1)) / 4 + 1
                    };
                    // USING / NATURAL when the condition is a same-named
                    // equality and the two columns compare alike.
                    let mut on = vec![tok(format!(" ON {c}"))];
                    let same_name = c
                        .split(" = ")
                        .map(|s| s.rsplit('.').next().unwrap_or(""))
                        .collect::<Vec<_>>();
                    let natural_ok = self.natural_ok(&tabs, &src);
                    if same_name.len() == 2 && same_name[0] == same_name[1] && self.chance(0.25) {
                        let col = same_name[0].to_string();
                        let unique_left = tabs
                            .iter()
                            .filter(|s| s.rel.col_index(&col).is_some())
                            .count()
                            == 1;
                        if unique_left {
                            on = vec![tok(format!(" USING ({col})"))];
                        }
                    } else if natural_ok
                        && tabs.len() == 1
                        && self.chance(0.08)
                        && jt != "FULL JOIN"
                    {
                        parts.push(opt(seq(vec![tok(format!(" NATURAL {jt} ")), item])));
                        tabs.push(src);
                        // No RIGHT / FULL join after a NATURAL one (see
                        // `nested`).
                        outer_seen = true;
                        continue;
                    }
                    if on.len() == 1
                        && self.chance(0.25)
                        && !matches!(on[0], Node::Tok(ref t) if t.starts_with(" USING"))
                    {
                        let mut both = tabs.clone();
                        both.push(src.clone());
                        let (b, _) = self.cond(&vec![both], 0);
                        on.push(opt(seq(vec![tok(" AND "), b])));
                    }
                    let mut j = vec![tok(format!(" {jt} ")), item];
                    j.extend(on);
                    // Inner joins may be dropped by the shrinker; dropping
                    // an outer join changes nothing else's meaning either.
                    parts.push(opt(seq(j)));
                }
            }
            tabs.push(src);
        }
        (seq(parts), tabs, card)
    }

    /// NATURAL join is safe when every shared column name compares alike.
    fn natural_ok(&self, left: &[Src], right: &Src) -> bool {
        let Some(l) = left.first() else { return false };
        let shared: Vec<(&Col, &Col)> = l
            .rel
            .cols
            .iter()
            .filter_map(|c| {
                right
                    .rel
                    .cols
                    .iter()
                    .find(|r| r.name == c.name)
                    .map(|r| (c, r))
            })
            .collect();
        !shared.is_empty()
            && shared.iter().all(|(a, b)| a.props.comparable(&b.props))
            && !self.cfg.large
    }

    // ------------------------------------------------------------ SELECT

    /// Keys giving the rows of `tabs` a total order, if there are such.
    fn total_order(&self, tabs: &[Src]) -> Option<Vec<String>> {
        let mut keys = Vec::new();
        for s in tabs {
            match s.rel.key {
                Some(k) if s.rel.cols[k].props.sortable() => {
                    keys.push(format!("{}.{}", s.alias, s.rel.cols[k].name))
                }
                _ => {
                    // Every column, when all sort alike: rows equal on all of
                    // them are indistinguishable.
                    if !s.rel.cols.iter().all(|c| c.props.sortable()) {
                        return None;
                    }
                    for c in &s.rel.cols {
                        keys.push(format!("{}.{}", s.alias, c.name));
                    }
                }
            }
        }
        Some(keys)
    }

    fn sortable_expr(&mut self, sc: &Scope, d: u32) -> (Node, Props) {
        for _ in 0..6 {
            let w = self.pick(&[W::I, W::R, W::T]);
            let (n, p) = self.expr(sc, w, d);
            if p.sortable() {
                return (n, p);
            }
        }
        self.leaf(sc, W::I)
    }

    fn window_item(&mut self, sc: &Scope, tabs: &[Src]) -> Option<(Node, Props)> {
        let total = self.total_order(tabs);
        let local: Scope = vec![tabs.to_vec()];
        let part = if self.chance(0.4) {
            let cs = self.cols(&local, |_| true);
            if cs.is_empty() {
                String::new()
            } else {
                format!("PARTITION BY {} ", self.pick(&cs).0)
            }
        } else {
            String::new()
        };
        let any_col = |g: &mut Self| -> String {
            let cs = g.cols(&local, |_| true);
            g.pick(&cs).0
        };
        let num_col = |g: &mut Self| -> Option<String> {
            let cs = g.cols(&local, |p| p.is_num());
            if cs.is_empty() {
                None
            } else {
                Some(g.pick(&cs).0)
            }
        };
        let _ = sc;
        let c = self.range(0, 11);
        let tot = total.map(|k| k.join(", "));
        let (f, props) = match c {
            0 => (
                format!("ROW_NUMBER() OVER ({part}ORDER BY {})", tot?),
                Props::INT,
            ),
            1..=3 => {
                let f = self.pick(&["RANK()", "DENSE_RANK()", "PERCENT_RANK()", "CUME_DIST()"]);
                let k = self.sortable_expr(&local, 1).0.text();
                let dir = self.pick(&["", " DESC", " NULLS LAST", " DESC NULLS FIRST"]);
                let p = if f.starts_with('P') || f.starts_with('C') {
                    Props::REAL
                } else {
                    Props::INT
                };
                (format!("{f} OVER ({part}ORDER BY {k}{dir})"), p)
            }
            4 => {
                let n = self.range(1, 4);
                (
                    format!("NTILE({n}) OVER ({part}ORDER BY {})", tot?),
                    Props::INT,
                )
            }
            5 => {
                let f = self.pick(&["LAG", "LEAD"]);
                let x = any_col(self);
                let extra = match self.range(0, 2) {
                    0 => String::new(),
                    1 => format!(", {}", self.range(1, 3)),
                    _ => format!(", {}, 0", self.range(1, 3)),
                };
                (
                    format!("{f}({x}{extra}) OVER ({part}ORDER BY {})", tot?),
                    Props::MIXED,
                )
            }
            6 => {
                let f = self.pick(&["FIRST_VALUE", "LAST_VALUE"]);
                let x = any_col(self);
                let frame = self.pick(&[
                    "",
                    " ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
                    " ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING",
                    " ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING",
                ]);
                (
                    format!("{f}({x}) OVER ({part}ORDER BY {}{frame})", tot?),
                    Props::MIXED,
                )
            }
            7 => {
                let x = any_col(self);
                let n = self.range(1, 3);
                (
                    format!(
                        "NTH_VALUE({x}, {n}) OVER ({part}ORDER BY {} ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)",
                        tot?
                    ),
                    Props::MIXED,
                )
            }
            8 | 9 => {
                let x = num_col(self)?;
                let f = self.pick(&["SUM", "TOTAL", "AVG", "COUNT", "MAX", "MIN"]);
                let order = if self.chance(0.6) {
                    let k = self.sortable_expr(&local, 0).0.text();
                    let frame = self.pick(&[
                        "",
                        " RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
                        " RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
                        " RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING",
                    ]);
                    format!("ORDER BY {k}{frame}")
                } else {
                    String::new()
                };
                let p = if f == "COUNT" {
                    Props::INT
                } else {
                    Props::REAL
                };
                (format!("{f}({x}) OVER ({part}{order})"), p)
            }
            10 => {
                let x = num_col(self)?;
                let f = self.pick(&["SUM", "MAX", "MIN", "COUNT", "AVG"]);
                let a = self.pick(&[
                    "UNBOUNDED PRECEDING",
                    "1 PRECEDING",
                    "2 PRECEDING",
                    "CURRENT ROW",
                ]);
                let b = self.pick(&["CURRENT ROW", "1 FOLLOWING", "UNBOUNDED FOLLOWING"]);
                (
                    format!(
                        "{f}({x}) OVER ({part}ORDER BY {} ROWS BETWEEN {a} AND {b})",
                        tot?
                    ),
                    Props::REAL,
                )
            }
            // A running GROUP_CONCAT is quadratic in output size by nature.
            _ if self.cfg.large => return None,
            _ => {
                let cs = self.cols(&local, |p| p.is_text());
                if cs.is_empty() {
                    return None;
                }
                let x = self.pick(&cs).0;
                (
                    format!(
                        "GROUP_CONCAT({x}, '|') OVER ({part}ORDER BY {} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
                        tot?
                    ),
                    Props::TEXT,
                )
            }
        };
        Some((expr(kind_char(&props), tok(f)), props))
    }

    /// Order-by-everything clause for `n` output columns when every column
    /// sorts alike; the node is an `Order` (compared in order).
    fn order_all(&mut self, props: &[Props], dirs: bool, limit: bool) -> Option<Node> {
        if !props.iter().all(|p| p.sortable()) {
            return None;
        }
        let terms: Vec<Node> = (0..props.len())
            .map(|i| {
                let dir = if dirs {
                    self.pick(&["", " DESC", " NULLS LAST", " ASC", " DESC NULLS FIRST"])
                } else {
                    ""
                };
                tok(format!("{}{dir}", i + 1))
            })
            .collect();
        let mut parts = vec![tok(" ORDER BY "), list(", ", terms.len(), terms)];
        if limit {
            let lim = if self.cfg.large {
                self.range(1, 30)
            } else {
                self.range(0, 6)
            };
            let mut l = vec![tok(format!(" LIMIT {lim}"))];
            if self.chance(0.4) {
                l.push(tok(format!(" OFFSET {}", self.range(0, 4))));
            }
            parts.push(opt(seq(l)));
        }
        Some(opt(Node::Order(Box::new(seq(parts)))))
    }

    /// A SELECT. `alias_cols` names every output column `cN` (derived
    /// tables, CTEs); `top` allows windows and the output stage.
    fn select_q(&mut self, outer: &Scope, d: u32, alias_cols: bool, top: bool) -> Q {
        let (from, tabs, card) = self.gen_from(d);
        let mut sc = outer.clone();
        sc.push(tabs.clone());
        let local: Scope = vec![tabs.clone()];
        let wh = if self.chance(0.7) {
            let (b, _) = self.cond(&sc, d);
            Some(opt(seq(vec![tok(" WHERE "), b])))
        } else {
            None
        };
        let name = |i: usize, n: Node| -> Node {
            if alias_cols {
                seq(vec![n, tok(format!(" AS c{}", i + 1))])
            } else {
                n
            }
        };
        if self.chance(0.33) {
            // Grouped.
            let mut keys: Vec<(Node, Props)> = Vec::new();
            for _ in 0..self.range(1, 2) {
                let k = if self.chance(0.5) {
                    let w = self.pick(&[W::I, W::T]);
                    self.expr(&local, w, 1)
                } else {
                    let cs = self.cols(&local, |_| true);
                    let (c, p) = self.pick(&cs);
                    (expr(kind_char(&p), tok(c)), p)
                };
                // An integer literal key would be a column position.
                let kt = k.0.text();
                let positional = kt
                    .trim_start_matches(['(', '-', '+', ' '])
                    .trim_end_matches(')')
                    .parse::<i64>()
                    .is_ok();
                if !positional && !keys.iter().any(|(n, _)| n.text() == kt) {
                    keys.push(k);
                }
            }
            if keys.is_empty() {
                let cs = self.cols(&local, |_| true);
                let (c, p) = self.pick(&cs);
                keys.push((expr(kind_char(&p), tok(c)), p));
            }
            let mut aggs: Vec<(Node, Props)> = Vec::new();
            for _ in 0..self.range(1, 3) {
                aggs.push(self.aggregate(&sc));
            }
            let mut items: Vec<(Node, Props)> = keys.clone();
            items.extend(aggs.iter().cloned());
            if top && self.chance(0.3) {
                let (a0, p0) = aggs[0].clone();
                if p0.sortable() {
                    items.push((
                        expr(
                            'i',
                            seq(vec![
                                tok("RANK() OVER (ORDER BY "),
                                a0.clone(),
                                tok(" DESC)"),
                            ]),
                        ),
                        Props::INT,
                    ));
                }
                if p0.is_num() {
                    items.push((
                        expr('r', seq(vec![tok("SUM("), a0, tok(") OVER ()")])),
                        Props::REAL,
                    ));
                }
            }
            let props: Vec<Props> = items.iter().map(|(_, p)| *p).collect();
            let sel: Vec<Node> = items
                .into_iter()
                .enumerate()
                .map(|(i, (n, _))| name(i, n))
                .collect();
            let nsel = sel.len();
            let mut parts = vec![tok("SELECT "), list(", ", nsel, sel), tok(" FROM "), from];
            if let Some(w) = wh {
                parts.push(w);
            }
            let key_nodes: Vec<Node> = keys.into_iter().map(|(n, _)| n).collect();
            let nk = key_nodes.len();
            parts.push(tok(" GROUP BY "));
            parts.push(list(", ", nk, key_nodes));
            if self.chance(0.4) {
                let l = self.tame_int(&local, 0);
                let h = match self.range(0, 2) {
                    0 => seq(vec![tok("COUNT(*)")]),
                    1 => seq(vec![tok("SUM("), l, tok(")")]),
                    _ => seq(vec![tok("MAX("), l, tok(")")]),
                };
                let op = self.pick(&[">", ">=", "<", "="]);
                let v = self.pick(&["1", "2", "0", "5"]);
                parts.push(opt(seq(vec![
                    tok(" HAVING "),
                    h,
                    tok(format!(" {op} {v}")),
                ])));
            }
            if top && self.chance(0.45) {
                let limit = self.chance(0.5);
                if let Some(o) = self.order_all(&props, false, limit) {
                    parts.push(o);
                }
            }
            return Q {
                node: seq(parts),
                props,
                card: (card / 2).max(1),
            };
        }
        let mut items: Vec<(Node, Props)> = Vec::new();
        // `*` over USING / NATURAL joins: which copies of the shared
        // columns it shows after a RIGHT/FULL join is where SQLite itself
        // errs ("ambiguous column name"); keep `*` to plain joins.
        let ft = from.text();
        let star = !alias_cols
            && top
            && !ft.contains(" USING ")
            && !ft.contains(" NATURAL ")
            && self.chance(0.06);
        if !star {
            for _ in 0..self.range(1, 4) {
                let w = self.pick(&[W::I, W::I, W::R, W::T]);
                items.push(self.expr(&sc, w, d));
            }
            if self.chance(0.12) {
                let cs = self.cols(&local, |p| p.kind == Kind::Mixed);
                if !cs.is_empty() {
                    let (c, p) = self.pick(&cs);
                    items.push((expr('m', tok(c)), p));
                }
            }
            if top && self.chance(0.25) {
                if let Some(w) = self.window_item(&sc, &tabs) {
                    items.push(w);
                }
            }
        }
        let distinct = self.chance(0.18);
        let props: Vec<Props> = if star {
            // `*` with USING/NATURAL hides columns; never ordered.
            vec![Props::MIXED]
        } else {
            items.iter().map(|(_, p)| *p).collect()
        };
        let sel = if star {
            tok("*")
        } else {
            let v: Vec<Node> = items
                .into_iter()
                .enumerate()
                .map(|(i, (n, _))| name(i, n))
                .collect();
            let n = v.len();
            list(", ", if alias_cols { n } else { 1 }, v)
        };
        let mut parts = vec![
            tok(if distinct {
                "SELECT DISTINCT "
            } else {
                "SELECT "
            }),
            sel,
            tok(" FROM "),
            from,
        ];
        if let Some(w) = wh {
            parts.push(w);
        }
        if top && !star {
            let want_order = self.chance(if self.cfg.large { 0.8 } else { 0.4 });
            if want_order {
                let limit = self.chance(if self.cfg.large { 0.9 } else { 0.6 });
                if let Some(o) = self.order_all(&props, true, limit) {
                    parts.push(o);
                }
            }
        }
        Q {
            node: seq(parts),
            props,
            card,
        }
    }

    fn aggregate(&mut self, sc: &Scope) -> (Node, Props) {
        let (head, arg, props): (&str, Option<Node>, Props) = match self.range(0, 8) {
            0 => ("COUNT(*", None, Props::INT),
            1 => {
                let w = self.pick(&[W::I, W::T, W::R]);
                ("COUNT(", Some(self.expr(sc, w, 1).0), Props::INT)
            }
            2 => ("SUM(", Some(self.tame_int(sc, 1)), Props::INT),
            3 | 4 => {
                let f = self.pick(&["MIN(", "MAX("]);
                let (n, p) = self.sortable_expr(sc, 1);
                (f, Some(n), p)
            }
            5 => {
                let w = self.pick(&[W::I, W::T]);
                ("COUNT(DISTINCT ", Some(self.expr(sc, w, 1).0), Props::INT)
            }
            6 => ("SUM(DISTINCT ", Some(self.tame_int(sc, 1)), Props::INT),
            7 => ("TOTAL(", Some(self.expr(sc, W::R, 1).0), Props::REAL),
            _ => ("AVG(", Some(self.expr(sc, W::R, 1).0), Props::REAL),
        };
        let mut parts = vec![tok(head)];
        if let Some(a) = arg {
            parts.push(a);
        }
        parts.push(tok(")"));
        if self.chance(0.15) {
            let (b, _) = self.cond(sc, 0);
            parts.push(opt(seq(vec![tok(" FILTER (WHERE "), b, tok(")")])));
        }
        (expr(kind_char(&props), seq(parts)), props)
    }

    fn compound_q(&mut self, alias_cols: bool) -> Q {
        let n = self.range(1, 2) as usize;
        let ws: Vec<W> = (0..n).map(|_| self.pick(&[W::I, W::T])).collect();
        let mut arms = Vec::new();
        let mut props: Vec<Props> = Vec::new();
        let mut card = 0usize;
        let narms = self.range(2, 3);
        for a in 0..narms {
            let (fr, tabs, c) = if self.chance(0.5) {
                self.gen_from(0)
            } else {
                let rels = self.rels();
                let rel = self.pick(&rels);
                let alias = self.alias("t");
                let card = rel.card;
                (
                    tok(format!("{} {}", rel.name, alias)),
                    vec![Src { alias, rel }],
                    card,
                )
            };
            card += c;
            let sc: Scope = vec![tabs];
            let mut sel = Vec::new();
            for (i, w) in ws.iter().enumerate() {
                let (e, p) = self.expr(&sc, *w, 1);
                if a == 0 {
                    props.push(p);
                } else {
                    props[i] = Props::join(props[i], p);
                }
                sel.push(if alias_cols && a == 0 {
                    seq(vec![e, tok(format!(" AS c{}", i + 1))])
                } else {
                    e
                });
            }
            let ns = sel.len();
            let mut arm = vec![tok("SELECT "), list(", ", ns, sel), tok(" FROM "), fr];
            if self.chance(0.6) {
                let (b, _) = self.cond(&sc, 1);
                arm.push(opt(seq(vec![tok(" WHERE "), b])));
            }
            if a > 0 {
                let ops: &[&str] = if a == 1 {
                    &["UNION", "UNION ALL", "EXCEPT", "UNION", "INTERSECT"]
                } else {
                    &["UNION", "UNION ALL", "EXCEPT", "UNION"]
                };
                arms.push(tok(format!(" {} ", self.pick(ops))));
            }
            arms.push(seq(arm));
        }
        if !alias_cols && self.chance(0.5) {
            let limit = self.chance(0.6);
            if let Some(o) = self.order_all(&props, true, limit) {
                arms.push(o);
            }
        }
        Q {
            node: seq(arms),
            props,
            card,
        }
    }

    // ------------------------------------------------------------ statements

    fn stmt_select(&mut self) -> Node {
        let q = self.select_q(&Vec::new(), 2, false, true);
        q.node
    }

    fn stmt_compound(&mut self) -> Node {
        self.compound_q(false).node
    }

    fn stmt_derived(&mut self) -> Node {
        let (item, rel) = self.derived_rel(1);
        let src = Src {
            alias: rel.name.clone(),
            rel: rel.clone(),
        };
        let sc: Scope = vec![vec![src]];
        if self.chance(0.4) {
            return seq(vec![tok("SELECT COUNT(*) FROM "), item]);
        }
        let mut items = Vec::new();
        let mut props = Vec::new();
        for _ in 0..self.range(1, 3) {
            let w = self.pick(&[W::I, W::T, W::R]);
            let (e, p) = self.expr(&sc, w, 1);
            items.push(e);
            props.push(p);
        }
        let n = items.len();
        let mut parts = vec![
            tok("SELECT "),
            list(", ", 1.min(n), items),
            tok(" FROM "),
            item,
        ];
        if self.chance(0.5) {
            let (b, _) = self.cond(&sc, 1);
            parts.push(opt(seq(vec![tok(" WHERE "), b])));
        }
        if self.chance(0.4) {
            let limit = self.chance(0.5);
            if let Some(o) = self.order_all(&props, true, limit) {
                parts.push(o);
            }
        }
        seq(parts)
    }

    /// WITH c1 AS (...), c2 AS (... FROM c1 ...) SELECT ...
    fn stmt_cte(&mut self) -> Node {
        let n = self.range(1, 3);
        let mut defs = Vec::new();
        for _ in 0..n {
            let name = self.alias("cte");
            self.nested += 1;
            let q = if self.chance(0.2) {
                self.compound_q(true)
            } else {
                self.select_q(&Vec::new(), 1, true, false)
            };
            self.nested -= 1;
            let cols: Vec<Col> = q
                .props
                .iter()
                .enumerate()
                .map(|(i, p)| Col {
                    name: format!("c{}", i + 1),
                    props: *p,
                })
                .collect();
            // Sometimes name the columns in a list instead of AS aliases
            // (the aliases stay; the list renames them).
            let def = if self.chance(0.3) {
                let names: Vec<String> = cols.iter().map(|c| c.name.clone()).collect();
                seq(vec![
                    tok(format!("{name}({}) AS (", names.join(", "))),
                    nest(q.node),
                    tok(")"),
                ])
            } else {
                seq(vec![tok(format!("{name} AS (")), nest(q.node), tok(")")])
            };
            defs.push(def);
            self.ctes.push(Rc::new(Rel {
                name,
                cols,
                key: None,
                card: q.card,
                fks: vec![],
            }));
        }
        let body = if self.chance(0.3) {
            self.compound_q(false).node
        } else {
            self.select_q(&Vec::new(), 2, false, true).node
        };
        let nd = defs.len();
        seq(vec![tok("WITH "), list(", ", nd, defs), tok(" "), body])
    }

    fn stmt_recursive(&mut self) -> Node {
        let trees = self.schema.trees.clone();
        let c = self.range(0, 4);
        if c <= 1 || trees.is_empty() {
            // Counter, optionally joined to a table on its key.
            let lim = self.range(1, if self.cfg.large { 200 } else { 30 });
            let name = self.alias("cnt");
            let def = if self.chance(0.5) {
                format!(
                    "WITH RECURSIVE {name}(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM {name} WHERE x < {lim}) "
                )
            } else {
                format!(
                    "WITH RECURSIVE {name}(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM {name} LIMIT {lim}) "
                )
            };
            let keyed: Vec<Rc<Rel>> = self
                .schema
                .rels
                .iter()
                .filter(|r| r.key.is_some())
                .cloned()
                .collect();
            if !keyed.is_empty() && self.chance(0.5) {
                let rel = self.pick(&keyed);
                let k = rel.cols[rel.key.unwrap()].name.clone();
                let a = self.alias("t");
                let src = Src {
                    alias: a.clone(),
                    rel: rel.clone(),
                };
                let sc: Scope = vec![vec![src]];
                let (e, p) = self.expr(&sc, W::T, 1);
                let props = [Props::INT, p];
                let mut parts = vec![
                    tok(def),
                    tok(format!("SELECT {name}.x, ")),
                    e,
                    tok(format!(
                        " FROM {name} {} JOIN {} {a} ON {a}.{k} = {name}.x",
                        self.pick(&["", "LEFT"]),
                        rel.name
                    )),
                ];
                if let Some(o) = self.order_all(&props, false, false) {
                    parts.push(o);
                }
                return seq(parts);
            }
            let agg = self.pick(&[
                "x, x * x, x % 3".to_string(),
                "COUNT(*), SUM(x), MAX(x)".to_string(),
                "x, (SELECT COUNT(*) FROM cntref)".to_string(),
            ]);
            if agg.contains("cntref") {
                let rels = self.rels();
                let rel = self.pick(&rels);
                let a = self.alias("t");
                let cs: Vec<String> = rel
                    .cols
                    .iter()
                    .filter(|c| c.props.kind == Kind::Int)
                    .map(|c| c.name.clone())
                    .collect();
                if cs.is_empty() {
                    return seq(vec![tok(def), tok(format!("SELECT x FROM {name}"))]);
                }
                let col = self.pick(&cs);
                return seq(vec![
                    tok(def),
                    tok(format!(
                        "SELECT x, (SELECT COUNT(*) FROM {} {a} WHERE {a}.{col} = {name}.x) FROM {name}",
                        rel.name
                    )),
                ]);
            }
            let ordered = !agg.starts_with("COUNT");
            let mut parts = vec![tok(def), tok(format!("SELECT {agg} FROM {name}"))];
            if ordered {
                parts.push(opt(Node::Order(Box::new(tok(" ORDER BY 1")))));
            }
            return seq(parts);
        }
        if c == 2 {
            let lim = self.range(5, 40);
            let name = self.alias("fib");
            return seq(vec![
                tok(format!(
                    "WITH RECURSIVE {name}(a, b) AS (SELECT 0, 1 UNION ALL SELECT b, a + b FROM {name} WHERE b < {lim}000) SELECT a, b FROM {name}"
                )),
                opt(Node::Order(Box::new(tok(" ORDER BY a, b")))),
            ]);
        }
        // Tree walk down a parent column, depth-bounded.
        let (table, id, parent) = self.pick(&trees);
        let name = self.alias("walk");
        let depth = self.range(1, 5);
        let roots = self.range(1, 5);
        let union = self.pick(&["UNION ALL", "UNION"]);
        let def = format!(
            "WITH RECURSIVE {name}(id, depth, path) AS (SELECT {id}, 0, 'n' || {id} FROM {table} WHERE {id} <= {roots} {union} SELECT c.{id}, {name}.depth + 1, {name}.path || '/n' || c.{id} FROM {table} c JOIN {name} ON c.{parent} = {name}.id WHERE {name}.depth < {depth}) "
        );
        match self.range(0, 2) {
            0 => seq(vec![
                tok(def),
                tok(format!("SELECT id, depth, path FROM {name}")),
                opt(Node::Order(Box::new(tok(" ORDER BY 3, 1, 2")))),
            ]),
            1 => seq(vec![
                tok(def),
                tok(format!(
                    "SELECT depth, COUNT(*), MIN(id), MAX(id) FROM {name} GROUP BY depth"
                )),
                opt(Node::Order(Box::new(tok(" ORDER BY 1")))),
            ]),
            _ => {
                let a = self.alias("t");
                seq(vec![
                    tok(def),
                    tok(format!(
                        "SELECT {name}.depth, COUNT(*) FROM {name} JOIN {table} {a} ON {a}.{id} = {name}.id GROUP BY {name}.depth"
                    )),
                ])
            }
        }
    }

    /// A value expression for column `col` of a table being changed, with
    /// properties at least as strong as the column's (so later statements'
    /// typing still holds).
    fn value_for(&mut self, sc: &Scope, col: &Props) -> Node {
        match col.kind {
            Kind::Int => self.expr(sc, W::I, 1).0,
            Kind::Real => self.expr(sc, W::R, 1).0,
            Kind::Mixed => {
                let w = self.pick(&[W::I, W::R, W::T]);
                self.expr(sc, w, 1).0
            }
            Kind::Text => {
                let want = *col;
                self.text_with(sc, 1, move |p| {
                    (!want.nonnum || p.nonnum)
                        && (!want.letter || p.letter)
                        && (!want.ascii || p.ascii)
                        && (!want.wssafe || p.wssafe)
                        && !p.taff
                })
                .0
            }
        }
    }

    fn dml_stmt(&mut self, target: &Rc<Rel>) -> Node {
        let others: Vec<Rc<Rel>> = self
            .schema
            .rels
            .iter()
            .filter(|r| r.name != target.name)
            .cloned()
            .collect();
        let me: Scope = vec![vec![Src {
            alias: target.name.clone(),
            rel: target.clone(),
        }]];
        let c = self.r.unit();
        if c < 0.3 && !others.is_empty() {
            let o = self.pick(&others);
            let a = self.alias("s");
            let src: Scope = vec![vec![Src {
                alias: a.clone(),
                rel: o.clone(),
            }]];
            let vals: Vec<Node> = target
                .cols
                .iter()
                .map(|col| self.value_for(&src, &col.props))
                .collect();
            let nv = vals.len();
            let (b, _) = self.cond(&src, 1);
            let lim = if self.cfg.large { " LIMIT 5" } else { "" };
            let mut parts = vec![
                tok(format!("INSERT INTO {} SELECT ", target.name)),
                list(", ", nv, vals),
                tok(format!(" FROM {} {a}", o.name)),
                opt(seq(vec![tok(" WHERE "), b])),
            ];
            if !lim.is_empty() {
                // LIMIT needs an order to pick the same rows.
                if let Some(k) = o.key {
                    parts.push(tok(format!(" ORDER BY {a}.{}{lim}", o.cols[k].name)));
                }
            }
            return seq(parts);
        }
        if c < 0.45 {
            let empty: Scope = vec![];
            let mut rows = Vec::new();
            for _ in 0..self.range(1, 3) {
                let vals: Vec<Node> = target
                    .cols
                    .iter()
                    .map(|col| self.value_for(&empty, &col.props))
                    .collect();
                let nv = vals.len();
                rows.push(seq(vec![tok("("), list(", ", nv, vals), tok(")")]));
            }
            return seq(vec![
                tok(format!("INSERT INTO {} VALUES ", target.name)),
                list(", ", 1, rows),
            ]);
        }
        if c < 0.75 {
            let settable: Vec<usize> = (0..target.cols.len())
                .filter(|&i| Some(i) != target.key)
                .collect();
            if settable.is_empty() {
                return tok(format!("DELETE FROM {} WHERE 0", target.name));
            }
            let mut sets = Vec::new();
            for _ in 0..self.range(1, 2) {
                let i = self.pick(&settable);
                let col = target.cols[i].clone();
                let v = if col.props.kind == Kind::Int && self.chance(0.4) && !others.is_empty() {
                    let o = self.pick(&others);
                    let a = self.alias("u");
                    let oi: Vec<String> = o
                        .cols
                        .iter()
                        .filter(|c| c.props.kind == Kind::Int)
                        .map(|c| c.name.clone())
                        .collect();
                    let ti: Vec<String> = target
                        .cols
                        .iter()
                        .filter(|c| c.props.kind == Kind::Int)
                        .map(|c| c.name.clone())
                        .collect();
                    if oi.is_empty() || ti.is_empty() {
                        self.value_for(&me, &col.props)
                    } else {
                        let f = self.pick(&["MAX", "MIN", "COUNT", "SUM"]);
                        let x = self.pick(&oi);
                        let y = self.pick(&oi);
                        let t = self.pick(&ti);
                        expr(
                            'i',
                            nest(tok(format!(
                                "(SELECT {f}({a}.{x}) FROM {} {a} WHERE {a}.{y} = {}.{t})",
                                o.name, target.name
                            ))),
                        )
                    }
                } else {
                    self.value_for(&me, &col.props)
                };
                sets.push(seq(vec![tok(format!("{} = ", col.name)), v]));
            }
            // A column set twice is legal in SQLite (last wins) but an
            // edge sqawk need not share; keep the first.
            let mut seen = Vec::new();
            sets.retain(|s| {
                let t = s.text();
                let name = t.split(" = ").next().unwrap_or("").to_string();
                if seen.contains(&name) {
                    false
                } else {
                    seen.push(name);
                    true
                }
            });
            let ns = sets.len();
            let (b, _) = self.cond(&me, 1);
            return seq(vec![
                tok(format!("UPDATE {} SET ", target.name)),
                list(", ", ns, sets),
                opt(seq(vec![tok(" WHERE "), b])),
            ]);
        }
        if c < 0.88 || others.is_empty() {
            let (b, _) = self.cond(&me, 1);
            return seq(vec![
                tok(format!("DELETE FROM {}", target.name)),
                opt(seq(vec![tok(" WHERE "), b])),
            ]);
        }
        let o = self.pick(&others);
        let a = self.alias("s");
        let oi: Vec<String> = o
            .cols
            .iter()
            .filter(|c| c.props.kind == Kind::Int)
            .map(|c| c.name.clone())
            .collect();
        let ti: Vec<String> = target
            .cols
            .iter()
            .filter(|c| c.props.kind == Kind::Int)
            .map(|c| c.name.clone())
            .collect();
        if oi.is_empty() || ti.is_empty() {
            return tok(format!("DELETE FROM {} WHERE 0", target.name));
        }
        let src: Scope = vec![vec![Src {
            alias: a.clone(),
            rel: o.clone(),
        }]];
        let (b, _) = self.cond(&src, 1);
        let not = self.pick(&["", "NOT "]);
        let t = self.pick(&ti);
        let x = self.pick(&oi);
        seq(vec![
            tok(format!(
                "DELETE FROM {tn} WHERE {tn}.{t} {not}IN (SELECT {a}.{x} FROM {} {a} WHERE ",
                o.name,
                tn = target.name
            )),
            b,
            tok(")"),
        ])
    }

    /// The SELECT that shows a changed table: all of it, or a generated
    /// query over it.
    fn show(&mut self, target: &Rc<Rel>) -> Node {
        if self.cfg.large {
            return match target.key {
                Some(k) => tok(format!(
                    "SELECT COUNT(*), COUNT(DISTINCT {c}), MIN({c}), MAX({c}) FROM {t}",
                    t = target.name,
                    c = target.cols[k].name
                )),
                None => tok(format!(
                    "SELECT COUNT(*), COUNT(DISTINCT {}) FROM {}",
                    target.cols[0].name, target.name
                )),
            };
        }
        if self.chance(0.6) {
            return tok(format!("SELECT * FROM {}", target.name));
        }
        let saved = std::mem::take(&mut self.ctes);
        self.ctes = vec![target.clone()];
        let a = self.alias("t");
        let sc: Scope = vec![vec![Src {
            alias: a.clone(),
            rel: target.clone(),
        }]];
        let mut items = Vec::new();
        let mut props = Vec::new();
        for _ in 0..self.range(1, 3) {
            let w = self.pick(&[W::I, W::T, W::R]);
            let (e, p) = self.expr(&sc, w, 1);
            items.push(e);
            props.push(p);
        }
        self.ctes = saved;
        let n = items.len();
        let mut parts = vec![
            tok("SELECT "),
            list(", ", n, items),
            tok(format!(" FROM {} {a}", target.name)),
        ];
        if self.chance(0.5) {
            let (b, _) = self.cond(&sc, 1);
            parts.push(opt(seq(vec![tok(" WHERE "), b])));
        }
        if let Some(o) = self.order_all(&props, false, false) {
            parts.push(o);
        }
        seq(parts)
    }

    fn pick_target(&mut self) -> Rc<Rel> {
        let rels: Vec<Rc<Rel>> = self.schema.rels.clone();
        self.pick(&rels)
    }

    fn stmts_dml(&mut self) -> Vec<Node> {
        let target = self.pick_target();
        self.avoid = Some(target.name.clone());
        let mut v = Vec::new();
        for _ in 0..self.range(1, 3) {
            v.push(self.dml_stmt(&target));
        }
        self.avoid = None;
        v.push(self.show(&target));
        v
    }

    fn stmts_txn(&mut self) -> Vec<Node> {
        let target = self.pick_target();
        self.avoid = Some(target.name.clone());
        let mut v = vec![tok(self.pick(&[
            "BEGIN",
            "BEGIN TRANSACTION",
            "BEGIN IMMEDIATE",
        ]))];
        for _ in 0..self.range(1, 2) {
            v.push(self.dml_stmt(&target));
        }
        if self.chance(0.3) {
            v.push(self.show(&target));
        }
        v.push(tok(self.pick(&[
            "ROLLBACK",
            "COMMIT",
            "END",
            "ROLLBACK TRANSACTION",
        ])));
        if self.chance(0.15) {
            // Errors both engines must raise.
            v.push(tok(self.pick(&["COMMIT", "ROLLBACK"])));
        }
        self.avoid = None;
        v.push(self.show(&target));
        v
    }

    fn stmts_ctas(&mut self) -> Vec<Node> {
        let name = self.alias("x");
        let q = if self.chance(0.3) {
            self.compound_q(true)
        } else {
            self.select_q(&Vec::new(), 1, true, false)
        };
        let cols: Vec<Col> = q
            .props
            .iter()
            .enumerate()
            .map(|(i, p)| Col {
                name: format!("c{}", i + 1),
                props: *p,
            })
            .collect();
        let rel = Rc::new(Rel {
            name: name.clone(),
            cols,
            key: None,
            card: q.card,
            fks: vec![],
        });
        let mut v = vec![seq(vec![tok(format!("CREATE TABLE {name} AS ")), q.node])];
        if self.chance(0.3) {
            v.push(self.dml_stmt(&rel));
        }
        v.push(self.show(&rel));
        if self.chance(0.2) {
            v.push(tok(format!("DROP TABLE {name}")));
        }
        v
    }

    fn stmts_alter(&mut self) -> Vec<Node> {
        let target = self.pick_target();
        let col = self.alias("z");
        let (ty, props) = self.pick(&[("BLOB", Props::MIXED), ("INTEGER", Props::INT)]);
        let mut cols = target.cols.clone();
        cols.push(Col {
            name: col.clone(),
            props,
        });
        let rel = Rc::new(Rel {
            name: target.name.clone(),
            cols,
            key: target.key,
            card: target.card,
            fks: vec![],
        });
        self.avoid = Some(target.name.clone());
        let me: Scope = vec![vec![Src {
            alias: target.name.clone(),
            rel: rel.clone(),
        }]];
        let v_expr = if ty == "INTEGER" {
            self.expr(&me, W::I, 1).0
        } else {
            let w = self.pick(&[W::I, W::T, W::R]);
            self.expr(&me, w, 1).0
        };
        let (b, _) = self.cond(&me, 1);
        let mut v = vec![
            tok(format!("ALTER TABLE {} ADD COLUMN {col} {ty}", target.name)),
            seq(vec![
                tok(format!("UPDATE {} SET {col} = ", target.name)),
                v_expr,
                opt(seq(vec![tok(" WHERE "), b])),
            ]),
        ];
        if self.chance(0.4) {
            v.push(self.dml_stmt(&rel));
        }
        self.avoid = None;
        v.push(self.show(&rel));
        v
    }

    /// One random test case.
    pub fn case(&mut self) -> Case {
        let r = self.r.unit();
        let dml_scale = if self.cfg.large { 0.25 } else { 1.0 };
        let stmts: Vec<Node> = if r < 0.40 {
            vec![self.stmt_select()]
        } else if r < 0.52 {
            vec![self.stmt_compound()]
        } else if r < 0.60 {
            vec![self.stmt_derived()]
        } else if r < 0.70 {
            vec![self.stmt_cte()]
        } else if r < 0.76 {
            vec![self.stmt_recursive()]
        } else if r < 0.76 + 0.12 * dml_scale {
            self.stmts_dml()
        } else if r < 0.80 + 0.12 * dml_scale {
            match self.range(0, 2) {
                0 => self.stmts_txn(),
                1 => self.stmts_ctas(),
                _ => self.stmts_alter(),
            }
        } else {
            vec![self.stmt_select()]
        };
        self.ctes.clear();
        let n = stmts.len();
        Case {
            script: list(";\n", n.min(1), stmts),
        }
    }
}
