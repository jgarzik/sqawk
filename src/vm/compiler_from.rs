//! The FROM planner: one loop nest for every SELECT.
//!
//! A FROM clause -- a single table, a comma list, or a chain of explicit
//! INNER / LEFT / RIGHT / FULL / CROSS joins, with ON, USING or NATURAL -- is
//! planned into a left-deep list of loop levels, one per source table. The
//! code generator emits one nested loop per level, modelled on SQLite's
//! `sqlite3WhereBegin`/`sqlite3WhereEnd`:
//!
//! * Every source keeps its own cursor. A column reference is a `Column` read
//!   from that cursor, in a join exactly as in a single-table scan, so the
//!   SELECT list, WHERE, ON, GROUP BY and aggregates all go through the one
//!   expression compiler against the same [`NameCtx`].
//! * ON conjuncts are evaluated at their own level, before that level's
//!   "match" point. WHERE conjuncts are evaluated after it, at the innermost
//!   level that has every table they mention, so the NULL-extended rows an
//!   outer join produces are filtered by WHERE like any other row. A
//!   conjunct with a subquery is placed by the columns its compiled code
//!   reads (a correlated subquery's references included), so `t1.k IN
//!   (SELECT ...)` filters at t1's loop. A conjunct reading no table of the
//!   FROM at all is tested once, before the nest: in a correlated subquery,
//!   `t1.b < t1.c` skips the scan for an outer row that fails it.
//! * An outer join's preserved side owns a match flag. When an inner loop
//!   finishes without a match, its cursors are put on a NULL row (`NullRow`)
//!   and control re-enters the loop body at the match point once, so the rest
//!   of the nest -- and WHERE -- runs for the NULL-extended row.
//! * RIGHT JOIN is a LEFT JOIN with the loop order swapped; FULL JOIN is a
//!   LEFT JOIN plus a second pass over the right table emitting the rows that
//!   never matched.
//! * A level whose ON (or, for an inner level, WHERE) has an equality between
//!   an expression of that level's table and an expression of outer tables
//!   only is a hash join: the inner table is indexed once on that key and
//!   each outer row probes the index instead of rescanning.
//!
//! The rest of the SELECT (projection, aggregation, sorting) is a "body"
//! callback emitted at the innermost point of the nest.

use std::collections::HashSet;

use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, JoinConstraint,
    JoinOperator, ObjectName, TableFactor, TableWithJoins,
};

use super::bytecode::OpCode;
use super::compiler::{ColumnRef, Label, NameCtx, SqlCompiler, Src, UsingCol, UsingCols};
use crate::database::Database;
use crate::error::{SqawkError, SqawkResult};

/// The join operator between the sources before a FROM item and the item.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JoinKind {
    /// INNER, CROSS, or a comma.
    Inner,
    Left,
    Right,
    Full,
}

/// Classify a `JoinOperator` into a join kind plus its constraint.
///
/// The match is deliberately EXHAUSTIVE -- no `_` arm. sqlparser treats any
/// AST change as breaking, and `JoinOperator` gains variants across versions:
/// newer releases parse a plain `LEFT JOIN` as `Left` rather than
/// `LeftOuter`. Behind a catch-all that rename would turn every `LEFT JOIN`
/// into a runtime error (or, worse, an INNER JOIN); with an exhaustive match
/// it is a compile error that names the site.
fn classify_join(op: &JoinOperator) -> SqawkResult<(JoinKind, &JoinConstraint)> {
    let unsupported = |what: &str| {
        Err(SqawkError::UnsupportedSqlFeature(format!(
            "{} not supported",
            what
        )))
    };
    Ok(match op {
        // A bare `JOIN ... ON` is an INNER JOIN; sqlparser distinguishes the
        // spelling, SQL does not. Likewise LEFT / LEFT OUTER and RIGHT /
        // RIGHT OUTER.
        JoinOperator::Join(c) | JoinOperator::Inner(c) => (JoinKind::Inner, c),
        JoinOperator::Left(c) | JoinOperator::LeftOuter(c) => (JoinKind::Left, c),
        JoinOperator::Right(c) | JoinOperator::RightOuter(c) => (JoinKind::Right, c),
        JoinOperator::FullOuter(c) => (JoinKind::Full, c),
        JoinOperator::CrossJoin(c) => (JoinKind::Inner, c),
        JoinOperator::Semi(_) | JoinOperator::LeftSemi(_) | JoinOperator::RightSemi(_) => {
            return unsupported("SEMI JOIN")
        }
        JoinOperator::Anti(_) | JoinOperator::LeftAnti(_) | JoinOperator::RightAnti(_) => {
            return unsupported("ANTI JOIN")
        }
        JoinOperator::CrossApply => return unsupported("CROSS APPLY"),
        JoinOperator::OuterApply => return unsupported("OUTER APPLY"),
        JoinOperator::AsOf { .. } => return unsupported("ASOF JOIN"),
        // MySQL optimizer hint: semantically an INNER JOIN that also forces
        // the join order. Rejected rather than silently ignoring the hint.
        JoinOperator::StraightJoin(_) => return unsupported("STRAIGHT_JOIN"),
        JoinOperator::ArrayJoin | JoinOperator::LeftArrayJoin | JoinOperator::InnerArrayJoin => {
            return unsupported("ARRAY JOIN")
        }
    })
}

/// Split an expression on top-level AND.
pub(crate) fn split_conjuncts(expr: &Expr, out: &mut Vec<Expr>) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            split_conjuncts(left, out);
            split_conjuncts(right, out);
        }
        Expr::Nested(inner)
            if matches!(
                **inner,
                Expr::BinaryOp {
                    op: BinaryOperator::And,
                    ..
                }
            ) =>
        {
            split_conjuncts(inner, out)
        }
        other => out.push(other.clone()),
    }
}

/// The direct sub-expressions of `expr`, or `None` for a node whose meaning
/// is not a pure function of its children and the current row (a subquery,
/// a window function, anything not listed).
///
/// Shared by the aggregate collector and the table-dependency analysis, so
/// both see the same tree.
pub(crate) fn expr_children(expr: &Expr) -> Option<Vec<&Expr>> {
    Some(match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) | Expr::Value(_) => Vec::new(),
        Expr::Nested(e)
        | Expr::UnaryOp { expr: e, .. }
        | Expr::Cast { expr: e, .. }
        | Expr::IsNull(e)
        | Expr::IsNotNull(e)
        | Expr::IsTrue(e)
        | Expr::IsFalse(e)
        | Expr::IsNotTrue(e)
        | Expr::IsNotFalse(e)
        | Expr::Ceil { expr: e, .. }
        | Expr::Floor { expr: e, .. } => vec![e],
        Expr::BinaryOp { left, right, .. }
        | Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right) => vec![left, right],
        Expr::Between {
            expr, low, high, ..
        } => vec![expr, low, high],
        Expr::InList { expr, list, .. } => {
            let mut v: Vec<&Expr> = vec![expr];
            v.extend(list.iter());
            v
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            vec![expr, pattern]
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            let mut v: Vec<&Expr> = Vec::new();
            if let Some(o) = operand {
                v.push(o);
            }
            for w in conditions {
                v.push(&w.condition);
                v.push(&w.result);
            }
            if let Some(e) = else_result {
                v.push(e);
            }
            v
        }
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            let mut v: Vec<&Expr> = vec![expr];
            v.extend(substring_from.iter().map(|b| &**b));
            v.extend(substring_for.iter().map(|b| &**b));
            v
        }
        Expr::Trim {
            expr, trim_what, ..
        } => {
            let mut v: Vec<&Expr> = vec![expr];
            v.extend(trim_what.iter().map(|b| &**b));
            v
        }
        Expr::Position { expr, r#in } => vec![expr, r#in],
        Expr::Function(f) => {
            if f.over.is_some() || f.filter.is_some() || !f.within_group.is_empty() {
                return None;
            }
            let list = match &f.args {
                FunctionArguments::None => return Some(Vec::new()),
                FunctionArguments::Subquery(_) => return None,
                FunctionArguments::List(list) => list,
            };
            let mut v = Vec::new();
            for arg in &list.args {
                match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e))
                    | FunctionArg::Named {
                        arg: FunctionArgExpr::Expr(e),
                        ..
                    }
                    | FunctionArg::ExprNamed {
                        arg: FunctionArgExpr::Expr(e),
                        ..
                    } => v.push(e),
                    FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {}
                    _ => return None,
                }
            }
            v
        }
        _ => return None,
    })
}

/// One loop of the nest.
struct Level {
    /// Index into [`FromPlan::srcs`].
    src: usize,
    /// Filters evaluated before this level's match point: ON conjuncts, and
    /// WHERE conjuncts at an inner level (where there is no match point).
    on_terms: Vec<Expr>,
    /// Filters evaluated after the match point: WHERE conjuncts that mention
    /// a NULL-extendable table, which must see the NULL-extended row.
    post_terms: Vec<Expr>,
    /// Equi-join key, when this level is a hash join: (inner expressions over
    /// this level's table, outer expressions over earlier levels).
    hash: Option<(Vec<Expr>, Vec<Expr>)>,
}

/// The NULL-extendable side of an outer join: levels `start..=end`.
///
/// One match flag covers the whole group. A LEFT JOIN's group is its single
/// right table; a RIGHT JOIN's group is everything to its left.
struct Group {
    start: usize,
    end: usize,
}

/// What every level of one loop nest shares (see `emit_level`).
#[derive(Clone, Copy)]
struct Levels<'r> {
    /// [`FromPlan::deferred_levels`].
    deferred: &'r [bool],
    /// Each outer-join group's match flag register.
    match_regs: &'r [i64],
    /// Where each group's NULL-extended row re-enters the nest.
    match_labels: &'r [Label],
}

/// A planned FROM clause.
pub(crate) struct FromPlan<'t> {
    /// Sources in FROM order. A statement-level query opens source `i` on
    /// cursor `i`; a subquery's sources get fresh cursor ids, so they never
    /// collide with an enclosing query's.
    pub srcs: Vec<Src<'t>>,
    /// Table to open for each source.
    table_names: Vec<String>,
    /// Loops, outermost first.
    levels: Vec<Level>,
    groups: Vec<Group>,
    /// USING / NATURAL merged columns.
    pub using: UsingCols,
    /// (source, column) pairs a USING merge hides from `*`.
    hidden: HashSet<(usize, usize)>,
    /// The FROM clause ends in a FULL JOIN: the WHERE conjuncts, re-applied
    /// in the pass over unmatched right rows.
    full_where: Option<Vec<Expr>>,
    /// WHERE conjuncts of a SELECT without FROM: no loop to place them in,
    /// so they decide whether its single row exists.
    const_where: Vec<Expr>,
    /// The enclosing query's scope, when this FROM belongs to a subquery.
    outer: Option<Box<NameCtx<'t>>>,
}

impl<'t> FromPlan<'t> {
    /// The name scope for expressions evaluated inside the loop nest.
    pub(crate) fn ctx(&self) -> NameCtx<'t> {
        let mut ctx = NameCtx::multi(self.srcs.clone()).with_outer(self.outer.clone());
        ctx.using = self.using.clone();
        ctx
    }

    /// Whether this FROM belongs to a subquery, whose loop nest may run once
    /// per row of an enclosing query.
    fn nested(&self) -> bool {
        self.outer.is_some()
    }

    /// Cursor of source `src`.
    fn cursor(&self, src: usize) -> i64 {
        self.srcs[src].cursor
    }

    /// Whether `(src, col)` is hidden from `*` by a USING merge.
    pub(crate) fn is_hidden(&self, src: usize, col: usize) -> bool {
        self.hidden.contains(&(src, col))
    }

    /// The USING column first appearing at `(src, col)`, if any.
    pub(crate) fn using_at(&self, src: usize, col: usize) -> Option<&UsingCol> {
        self.using.first_at((self.cursor(src), col))
    }

    /// The level holding source `src`.
    fn level_of(&self, src: usize) -> usize {
        self.levels
            .iter()
            .position(|l| l.src == src)
            .expect("every source has a level")
    }

    /// The outer-join group containing level `lvl`, if any.
    fn group_of(&self, lvl: usize) -> Option<&Group> {
        self.groups.iter().find(|g| g.start <= lvl && lvl <= g.end)
    }

    /// Per level: whether its table is opened (and its hash index built)
    /// only when the nest first reaches the level, as SQLite builds an
    /// automatic index there, rather than before the nest. Every level but
    /// the outermost, except one a RIGHT JOIN may NULL-extend without
    /// reaching (an outer-join group's levels after its first) and in a
    /// FULL JOIN, whose pass over unmatched right rows reads its tables
    /// after the nest.
    fn deferred_levels(&self) -> Vec<bool> {
        (0..self.levels.len())
            .map(|lvl| {
                lvl > 0
                    && self.full_where.is_none()
                    && !self.groups.iter().any(|g| g.start < lvl && lvl <= g.end)
            })
            .collect()
    }

    /// The deepest level among `srcs` (level 0 for none).
    fn max_level(&self, srcs: &[usize]) -> usize {
        srcs.iter().map(|s| self.level_of(*s)).max().unwrap_or(0)
    }
}

/// The sources an expression reads, or `None` when that cannot be determined
/// (an unresolvable or ambiguous name, a subquery, an aggregate...), in which
/// case the expression must be evaluated where every table is available.
pub(crate) fn expr_sources(expr: &Expr, ctx: &NameCtx<'_>) -> Option<Vec<usize>> {
    let mut out = Vec::new();
    collect_sources(expr, ctx, &mut out)?;
    out.sort_unstable();
    out.dedup();
    Some(out)
}

fn collect_sources(expr: &Expr, ctx: &NameCtx<'_>, out: &mut Vec<usize>) -> Option<()> {
    let found = match expr {
        Expr::Identifier(ident) => Some(ctx.lookup(None, &ident.value).ok()?),
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => Some(
            ctx.lookup(
                Some(&parts[parts.len() - 2].value),
                &parts[parts.len() - 1].value,
            )
            .ok()?,
        ),
        Expr::CompoundIdentifier(_) => return None,
        _ => None,
    };
    let position = |cursor: i64| ctx.srcs.iter().position(|s| s.cursor == cursor);
    match found {
        // A column of an enclosing query (a correlated reference) is fixed
        // while this query runs: a constant, as far as placement goes.
        Some((_, depth)) if depth > 0 => Some(()),
        Some((ColumnRef::Cursor { cursor, .. }, _)) => {
            out.push(position(cursor)?);
            Some(())
        }
        Some((ColumnRef::Using(i), _)) => {
            for (cursor, _) in &ctx.using_col(i).refs {
                out.push(position(*cursor)?);
            }
            Some(())
        }
        Some((ColumnRef::Register(_), _)) => None,
        None => {
            if SqlCompiler::is_aggregate_call(expr) {
                return None;
            }
            for child in expr_children(expr)? {
                collect_sources(child, ctx, out)?;
            }
            Some(())
        }
    }
}

impl<'a> SqlCompiler<'a> {
    /// The sources of this FROM that a WHERE conjunct reads when
    /// [`expr_sources`] cannot tell (a subquery -- whose correlated
    /// references count -- or another node it does not analyze): found by
    /// compiling the conjunct in a scratch compiler and collecting the
    /// cursors its `Column` reads name. `None` when the conjunct does not
    /// compile (the real compilation reports why).
    fn compiled_term_sources(&self, term: &Expr, ctx: &NameCtx<'a>) -> Option<Vec<usize>> {
        let mut scratch = self.scratch();
        scratch.code_expr(term, ctx, None).ok()?;
        let mut out: Vec<usize> = scratch
            .program
            .instructions
            .iter()
            .filter(|inst| inst.opcode == OpCode::Column)
            .filter_map(|inst| ctx.srcs.iter().position(|s| s.cursor == inst.p1))
            .collect();
        out.sort_unstable();
        out.dedup();
        Some(out)
    }
}

/// Whether `exprs` read at least one column and every column they read
/// belongs to an enclosing query. An expression that cannot be analyzed (a
/// subquery, an unresolvable name) does not count.
pub(crate) fn reads_only_outer(exprs: &[Expr], ctx: &NameCtx<'_>) -> bool {
    fn walk(expr: &Expr, ctx: &NameCtx<'_>, read_any: &mut bool) -> Option<()> {
        let name = match expr {
            Expr::Identifier(ident) => Some((None, &ident.value)),
            Expr::CompoundIdentifier(parts) if parts.len() >= 2 => Some((
                Some(parts[parts.len() - 2].value.as_str()),
                &parts[parts.len() - 1].value,
            )),
            _ => None,
        };
        if let Some((qualifier, name)) = name {
            let (_, depth) = ctx.lookup(qualifier, name).ok()?;
            *read_any = true;
            return (depth > 0).then_some(());
        }
        for child in expr_children(expr)? {
            walk(child, ctx, read_any)?;
        }
        Some(())
    }
    let mut read_any = false;
    exprs.iter().all(|e| walk(e, ctx, &mut read_any).is_some()) && read_any
}

/// Whether `expr` reads a column of an enclosing query. Unanalyzable
/// expressions (subqueries and the like) count as doing so.
fn mentions_outer(expr: &Expr, ctx: &NameCtx<'_>) -> bool {
    let name = match expr {
        Expr::Identifier(ident) => Some((None, &ident.value)),
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => Some((
            Some(parts[parts.len() - 2].value.as_str()),
            &parts[parts.len() - 1].value,
        )),
        _ => None,
    };
    if let Some((qualifier, name)) = name {
        return ctx.lookup(qualifier, name).map_or(true, |(_, d)| d > 0);
    }
    match expr_children(expr) {
        Some(children) => children.into_iter().any(|c| mentions_outer(c, ctx)),
        None => true,
    }
}

/// `left.col = right.col` as an expression over the given refnames.
fn column_equality(left_ref: &str, right_ref: &str, col: &str) -> Expr {
    let qualified = |r: &str| {
        Expr::CompoundIdentifier(vec![Ident::new(r.to_string()), Ident::new(col.to_string())])
    };
    Expr::BinaryOp {
        left: Box::new(qualified(left_ref)),
        op: BinaryOperator::Eq,
        right: Box::new(qualified(right_ref)),
    }
}

impl<'a> SqlCompiler<'a> {
    /// Plan a FROM clause together with the WHERE clause filtering it.
    pub(crate) fn plan_from(
        &mut self,
        from: &[TableWithJoins],
        selection: Option<&Expr>,
    ) -> SqawkResult<FromPlan<'a>> {
        if self.verbose {
            if from.len() > 1 {
                diag!("Processing multiple tables in FROM clause as CROSS JOINs");
            }
            for join in from.iter().flat_map(|twj| &twj.joins) {
                if let Ok((kind, constraint)) = classify_join(&join.join_operator) {
                    let kind = match (kind, constraint) {
                        (JoinKind::Inner, JoinConstraint::None) => "CROSS",
                        (JoinKind::Inner, _) => "INNER",
                        (JoinKind::Left, _) => "LEFT",
                        (JoinKind::Right, _) => "RIGHT",
                        (JoinKind::Full, _) => "FULL",
                    };
                    let mut title = kind.to_ascii_lowercase();
                    title[..1].make_ascii_uppercase();
                    diag!("Join type: {}", title);
                    diag!("Processing {} JOIN with ON condition", kind);
                }
            }
            if let Some(w) = selection {
                diag!("WHERE comparison: {:?}", w);
            }
        }

        // No FROM: zero sources, one row.
        if from.is_empty() {
            let mut const_where = Vec::new();
            if let Some(w) = selection {
                split_conjuncts(w, &mut const_where);
            }
            return Ok(FromPlan {
                srcs: Vec::new(),
                table_names: Vec::new(),
                levels: Vec::new(),
                groups: Vec::new(),
                using: UsingCols::default(),
                hidden: HashSet::new(),
                full_where: None,
                const_where,
                outer: self.outer_scope.clone().map(Box::new),
            });
        }

        // ---- Sources and the operator joining each to what precedes it.
        let mut factors: Vec<(&TableFactor, Option<(JoinKind, &JoinConstraint)>)> = Vec::new();
        for (i, twj) in from.iter().enumerate() {
            factors.push((
                &twj.relation,
                if i == 0 {
                    None
                } else {
                    Some((JoinKind::Inner, &JoinConstraint::None))
                },
            ));
            for join in &twj.joins {
                factors.push((&join.relation, Some(classify_join(&join.join_operator)?)));
            }
        }

        let database: &'a Database = self.database;
        // A subquery's sources take fresh cursor ids; a statement-level
        // query's are numbered from 0.
        let outer = self.outer_scope.clone().map(Box::new);
        let base = if outer.is_some() {
            let base = self.alloc_cursor();
            for _ in 1..factors.len() {
                self.alloc_cursor();
            }
            base
        } else {
            0
        };
        let mut srcs: Vec<Src<'a>> = Vec::with_capacity(factors.len());
        let mut table_names = Vec::with_capacity(factors.len());
        for (i, (factor, _)) in factors.iter().enumerate() {
            let (name, alias) = match factor {
                TableFactor::Table {
                    name,
                    alias,
                    args: None,
                    ..
                } => (name, alias),
                TableFactor::NestedJoin { .. } => {
                    return Err(SqawkError::UnsupportedSqlFeature(
                        "Parenthesized joins are not supported".into(),
                    ))
                }
                _ => {
                    return Err(SqawkError::UnsupportedSqlFeature(
                        "Only table references are supported in FROM".into(),
                    ))
                }
            };
            if alias.as_ref().is_some_and(|a| !a.columns.is_empty()) {
                return Err(SqawkError::UnsupportedSqlFeature(
                    "Column aliases on a FROM item are not supported".into(),
                ));
            }
            let table_name = self.get_table_name(name)?;
            if !database.has_table(&table_name) {
                return Err(SqawkError::TableNotFound(table_name));
            }
            let table = database.get_table(&table_name)?;
            let refname = alias
                .as_ref()
                .map(|a| a.name.value.clone())
                .unwrap_or_else(|| table_name.clone())
                .to_ascii_lowercase();
            if srcs.iter().any(|s| s.refname == refname) {
                return Err(SqawkError::InvalidSqlQuery(format!(
                    "Table '{}' appears more than once in FROM; give each an alias",
                    refname
                )));
            }
            srcs.push(Src {
                cursor: base + i as i64,
                table,
                refname,
            });
            table_names.push(table_name);
        }

        // ---- ON conjuncts per join, with USING / NATURAL lowered onto ON.
        let mut using = UsingCols::default();
        let mut hidden: HashSet<(usize, usize)> = HashSet::new();
        let mut join_terms: Vec<Vec<Expr>> = vec![Vec::new(); srcs.len()];
        for (i, (_, op)) in factors.iter().enumerate() {
            let Some((_, constraint)) = op else { continue };
            let names: Vec<String> = match constraint {
                JoinConstraint::On(expr) => {
                    split_conjuncts(expr, &mut join_terms[i]);
                    continue;
                }
                JoinConstraint::None => continue,
                JoinConstraint::Using(cols) => cols
                    .iter()
                    .map(|c: &ObjectName| super::ast_compat::object_name_last(c))
                    .collect(),
                // The columns the right table shares with any table to its
                // left, in the right table's order.
                JoinConstraint::Natural => srcs[i]
                    .table
                    .column_metadata()
                    .iter()
                    .filter(|c| {
                        srcs[..i]
                            .iter()
                            .any(|s| s.table.column_index(&c.name).is_some())
                    })
                    .map(|c| c.name.clone())
                    .collect(),
            };
            for name in names {
                let right_col = srcs[i].table.column_index(&name);
                // The leftmost source that has the column.
                let left = srcs[..i]
                    .iter()
                    .enumerate()
                    .find_map(|(j, s)| s.table.column_index(&name).map(|c| (j, c)));
                let (Some(right_col), Some((left_src, left_col))) = (right_col, left) else {
                    return Err(SqawkError::InvalidSqlQuery(format!(
                        "Cannot join using column '{}': it must exist on both sides",
                        name
                    )));
                };
                join_terms[i].push(column_equality(
                    &srcs[left_src].refname,
                    &srcs[i].refname,
                    &name,
                ));
                hidden.insert((i, right_col));
                let key = name.to_ascii_lowercase();
                let left_ref = (srcs[left_src].cursor, left_col);
                let right_ref = (srcs[i].cursor, right_col);
                using.merge(key, left_ref, right_ref);
            }
        }

        let mut plan = FromPlan {
            srcs,
            table_names,
            levels: Vec::new(),
            groups: Vec::new(),
            using,
            hidden,
            full_where: None,
            const_where: Vec::new(),
            outer,
        };
        let ctx = plan.ctx();

        // ---- Loop order and outer-join groups.
        let new_level = |src: usize| Level {
            src,
            on_terms: Vec::new(),
            post_terms: Vec::new(),
            hash: None,
        };
        plan.levels.push(new_level(0));
        let mut full = false;
        for (i, (_, op)) in factors.iter().enumerate().skip(1) {
            let kind = op.map(|(k, _)| k).unwrap_or(JoinKind::Inner);
            if full {
                return Err(SqawkError::UnsupportedSqlFeature(
                    "A FULL JOIN followed by further joins is not supported".into(),
                ));
            }
            let terms = std::mem::take(&mut join_terms[i]);
            match kind {
                JoinKind::Inner => {
                    let mut level = new_level(i);
                    level.on_terms = terms;
                    plan.levels.push(level);
                    continue;
                }
                JoinKind::Left | JoinKind::Full => {
                    plan.levels.push(new_level(i));
                    let at = plan.levels.len() - 1;
                    plan.groups.push(Group { start: at, end: at });
                    full = kind == JoinKind::Full;
                }
                JoinKind::Right => {
                    // `L RIGHT JOIN r` is `r LEFT JOIN L`: r becomes the
                    // outermost loop and everything to its left the
                    // NULL-extendable group. That is only a plain left-deep
                    // nest when L has no outer join of its own.
                    if !plan.groups.is_empty() {
                        return Err(SqawkError::UnsupportedSqlFeature(
                            "RIGHT JOIN after an outer join is not supported".into(),
                        ));
                    }
                    plan.levels.insert(0, new_level(i));
                    let end = plan.levels.len() - 1;
                    plan.groups.push(Group { start: 1, end });
                }
            }
            // Each ON conjunct of an outer join is evaluated before the
            // group's match point, at the first group level where all its
            // tables are available.
            let group = plan.groups.last().expect("just pushed");
            let (start, end) = (group.start, group.end);
            for term in terms {
                let at = match expr_sources(&term, &ctx) {
                    Some(s) => {
                        if s.iter().any(|src| *src > i) {
                            return Err(SqawkError::InvalidSqlQuery(format!(
                                "ON clause refers to a table joined later: {}",
                                term
                            )));
                        }
                        plan.max_level(&s).clamp(start, end)
                    }
                    None => end,
                };
                plan.levels[at].on_terms.push(term);
            }
        }
        // An ON may only mention tables up to its own join: a deeper level's
        // cursor is not positioned yet, so reading it would see a stale row.
        // (Outer-join conjuncts were placed at their deepest table already.)
        for (lvl, level) in plan.levels.iter().enumerate() {
            for term in &level.on_terms {
                if let Some(s) = expr_sources(term, &ctx) {
                    if s.iter().any(|src| plan.level_of(*src) > lvl) {
                        return Err(SqawkError::InvalidSqlQuery(format!(
                            "ON clause refers to a table joined later: {}",
                            term
                        )));
                    }
                }
            }
        }

        // ---- WHERE: each conjunct at the innermost level it needs.
        let mut where_terms = Vec::new();
        if let Some(w) = selection {
            split_conjuncts(w, &mut where_terms);
        }
        let last = plan.levels.len() - 1;
        if full {
            // FULL JOIN is defined as the join first, WHERE second: a left
            // row WHERE rejects still counts as a match for its right rows.
            // Filtering it early would wrongly resurface those right rows as
            // unmatched, so WHERE stays innermost, after the match point.
            plan.levels[last]
                .post_terms
                .extend(where_terms.iter().cloned());
            plan.full_where = Some(where_terms);
        } else {
            for term in where_terms {
                let sources =
                    expr_sources(&term, &ctx).or_else(|| self.compiled_term_sources(&term, &ctx));
                match sources {
                    None => plan.levels[last].post_terms.push(term),
                    // A conjunct reading no table of this FROM (a constant,
                    // a test of an enclosing query's row, an uncorrelated
                    // subquery) is tested once, before the nest: when it is
                    // false no row can qualify, and in a correlated
                    // subquery the scan is skipped for that outer row.
                    Some(s) if s.is_empty() => plan.const_where.push(term),
                    Some(s) => {
                        let at = plan.max_level(&s);
                        match plan.group_of(at).map(|g| g.end) {
                            Some(end) => plan.levels[end].post_terms.push(term),
                            None => plan.levels[at].on_terms.push(term),
                        }
                    }
                }
            }
        }

        // ---- Hash joins. In a subquery the outermost level qualifies too,
        // when its key is a correlated reference: the subquery reruns per
        // outer row, and probing an index built once beats rescanning.
        for lvl in 0..plan.levels.len() {
            let src = plan.levels[lvl].src;
            let outer: Vec<usize> = plan.levels[..lvl].iter().map(|l| l.src).collect();
            let mut inner_keys = Vec::new();
            let mut outer_keys = Vec::new();
            for term in &plan.levels[lvl].on_terms {
                let Expr::BinaryOp {
                    left,
                    op: BinaryOperator::Eq,
                    right,
                } = term
                else {
                    continue;
                };
                let (Some(ls), Some(rs)) = (expr_sources(left, &ctx), expr_sources(right, &ctx))
                else {
                    continue;
                };
                // The inner key must be fixed per inner row -- the index is
                // built once -- so it may not read an enclosing query.
                let only_inner = |s: &[usize], e: &Expr| s == [src] && !mentions_outer(e, &ctx);
                let only_outer = |s: &[usize], e: &Expr| {
                    if lvl == 0 {
                        plan.nested() && s.is_empty() && mentions_outer(e, &ctx)
                    } else {
                        s.iter().all(|x| outer.contains(x))
                    }
                };
                if only_inner(&ls, left) && only_outer(&rs, right) {
                    inner_keys.push((**left).clone());
                    outer_keys.push((**right).clone());
                } else if only_inner(&rs, right) && only_outer(&ls, left) {
                    inner_keys.push((**right).clone());
                    outer_keys.push((**left).clone());
                }
            }
            if !inner_keys.is_empty() {
                plan.levels[lvl].hash = Some((inner_keys, outer_keys));
            }
        }

        Ok(plan)
    }

    /// Emit the loop nest for `plan`, calling `body` at its innermost point
    /// (and again in a FULL JOIN's pass over unmatched right rows).
    ///
    /// `body` may jump to labels of its own; to stop the scan early (LIMIT)
    /// it jumps to a label the caller resolves after this returns.
    pub(crate) fn emit_scan(
        &mut self,
        plan: &FromPlan<'a>,
        body: &mut dyn FnMut(&mut Self, &NameCtx<'a>) -> SqawkResult<()>,
    ) -> SqawkResult<()> {
        let ctx = plan.ctx();
        // WHERE terms that read no table are tested before anything else, as
        // SQLite does: when one is false no table is opened, so a CTE or
        // derived table read only here is never computed (see
        // `Database::add_lazy_table`), and no hash index is built. Nothing
        // is output then, not even a FULL JOIN's unmatched rows.
        let skip = self.label();
        for term in &plan.const_where {
            let r = self.code_expr(term, &ctx, None)?;
            self.emit_jump_to(OpCode::IfZ, r, skip, 0, None, "WHERE: before the scan");
        }
        // A subquery's loop nest may run once per outer row. Its cursors and
        // hash indexes do not depend on the outer row, so they are set up
        // only the first time. (Not for FULL JOIN, whose per-row match marks
        // must start fresh on every run.)
        let once = plan.nested() && plan.full_where.is_none();
        let opened = self.label();
        if once {
            self.emit_jump_to(
                OpCode::Once,
                0,
                opened,
                0,
                None,
                "subquery cursors: open once",
            );
        }
        // An inner level's table is opened, and its hash index built, when
        // the nest first reaches that level (see `deferred_levels`); every
        // other table here, before the nest.
        let deferred = plan.deferred_levels();
        for (lvl, level) in plan.levels.iter().enumerate() {
            if !deferred[lvl] {
                self.emit_open(plan, level.src);
            }
        }
        for (lvl, level) in plan.levels.iter().enumerate() {
            if !deferred[lvl] {
                self.emit_hash_build(plan, &ctx, level)?;
            }
        }
        if once {
            self.resolve(opened);
        }

        let match_regs: Vec<i64> = plan
            .groups
            .iter()
            .map(|_| self.allocate_register())
            .collect();
        let match_labels: Vec<Label> = plan.groups.iter().map(|_| self.label()).collect();
        if plan.levels.is_empty() {
            // No FROM: the body runs once, if WHERE holds.
            body(self, &ctx)?;
        } else {
            let levels = Levels {
                deferred: &deferred,
                match_regs: &match_regs,
                match_labels: &match_labels,
            };
            self.emit_level(plan, &ctx, 0, &levels, body)?;
        }

        // FULL JOIN: right rows no left row matched, with the left side NULL.
        if let Some(where_terms) = &plan.full_where {
            let right = plan.levels.last().expect("FULL JOIN has a right level");
            let cursor = plan.cursor(right.src);
            let top = self.label();
            let next = self.label();
            let done = self.label();
            self.emit_jump_to(
                OpCode::Rewind,
                cursor,
                done,
                0,
                None,
                "FULL JOIN: unmatched right rows",
            );
            self.resolve(top);
            self.emit_jump_to(OpCode::IfRowMatched, cursor, next, 0, None, "matched: skip");
            for level in &plan.levels[..plan.levels.len() - 1] {
                self.emit(
                    OpCode::NullRow,
                    plan.cursor(level.src),
                    0,
                    0,
                    None,
                    "left side NULL",
                );
            }
            for term in where_terms {
                let r = self.code_expr(term, &ctx, None)?;
                self.emit_jump_to(OpCode::IfZ, r, next, 0, None, "WHERE");
            }
            body(self, &ctx)?;
            self.resolve(next);
            self.emit_jump_to(OpCode::Next, cursor, top, 0, None, "");
            self.resolve(done);
        }

        self.resolve(skip);
        if !once {
            for i in 0..plan.srcs.len() {
                self.emit(OpCode::Close, plan.cursor(i), 0, 0, None, "");
            }
        }
        Ok(())
    }

    /// Open source `src`'s table on its cursor.
    fn emit_open(&mut self, plan: &FromPlan<'a>, src: usize) {
        let cursor = plan.cursor(src);
        let name = &plan.table_names[src];
        self.emit(
            OpCode::OpenRead,
            cursor,
            0,
            0,
            Some(name.clone()),
            &format!("Open {} as cursor {}", name, cursor),
        );
    }

    /// Build `level`'s hash index, if it is a hash join: its key
    /// expressions read only the level's own table.
    fn emit_hash_build(
        &mut self,
        plan: &FromPlan<'a>,
        ctx: &NameCtx<'a>,
        level: &Level,
    ) -> SqawkResult<()> {
        let Some((inner, _)) = &level.hash else {
            return Ok(());
        };
        let cursor = plan.cursor(level.src);
        let top = self.label();
        let done = self.label();
        self.emit_jump_to(
            OpCode::Rewind,
            cursor,
            done,
            0,
            None,
            "hash build: scan inner",
        );
        self.resolve(top);
        let keys = self.allocate_registers(inner.len());
        for (j, e) in inner.iter().enumerate() {
            self.code_expr(e, ctx, Some(keys + j as i64))?;
        }
        self.emit(
            OpCode::HashInsert,
            cursor,
            keys,
            inner.len() as i64,
            None,
            "hash build: index row",
        );
        self.emit_jump_to(OpCode::Next, cursor, top, 0, None, "");
        self.resolve(done);
        Ok(())
    }

    fn emit_level(
        &mut self,
        plan: &FromPlan<'a>,
        ctx: &NameCtx<'a>,
        lvl: usize,
        levels: &Levels<'_>,
        body: &mut dyn FnMut(&mut Self, &NameCtx<'a>) -> SqawkResult<()>,
    ) -> SqawkResult<()> {
        let Levels {
            deferred,
            match_regs,
            match_labels,
        } = *levels;
        let level = &plan.levels[lvl];
        let cursor = plan.cursor(level.src);
        let starts = plan.groups.iter().position(|g| g.start == lvl);
        let ends = plan.groups.iter().position(|g| g.end == lvl);

        if deferred[lvl] {
            // First time here: open the table and build its hash index. A
            // table no outer row reaches is never opened, so a CTE or
            // derived table there is never computed.
            let ready = self.label();
            self.emit_jump_to(OpCode::Once, 0, ready, 0, None, "open on first use");
            self.emit_open(plan, level.src);
            self.emit_hash_build(plan, ctx, level)?;
            self.resolve(ready);
        }

        if let Some(g) = starts {
            self.emit(
                OpCode::Integer,
                0,
                match_regs[g],
                0,
                None,
                "outer join: no match yet",
            );
        }

        let top = self.label();
        let next = self.label();
        let end = self.label();
        match &level.hash {
            Some((_, outer)) => {
                let keys = self.allocate_registers(outer.len());
                for (j, e) in outer.iter().enumerate() {
                    self.code_expr(e, ctx, Some(keys + j as i64))?;
                }
                self.emit_jump_to(
                    OpCode::HashProbe,
                    cursor,
                    end,
                    keys,
                    Some(outer.len().to_string()),
                    &format!("hash join probe {}", plan.srcs[level.src].refname),
                );
            }
            None => self.emit_jump_to(
                OpCode::Rewind,
                cursor,
                end,
                0,
                None,
                &format!("scan {}", plan.srcs[level.src].refname),
            ),
        }
        self.resolve(top);
        for term in &level.on_terms {
            let r = self.code_expr(term, ctx, None)?;
            self.emit_jump_to(OpCode::IfZ, r, next, 0, None, "ON / WHERE");
        }
        if let Some(g) = ends {
            // A NULL-extended row re-enters here.
            self.resolve(match_labels[g]);
            self.emit(
                OpCode::Integer,
                1,
                match_regs[g],
                0,
                None,
                "outer join: matched",
            );
            if plan.full_where.is_some() && lvl + 1 == plan.levels.len() {
                self.emit(
                    OpCode::MarkRowMatched,
                    cursor,
                    0,
                    0,
                    None,
                    "FULL JOIN: row matched",
                );
            }
        }
        for term in &level.post_terms {
            let r = self.code_expr(term, ctx, None)?;
            self.emit_jump_to(OpCode::IfZ, r, next, 0, None, "WHERE");
        }

        if lvl + 1 < plan.levels.len() {
            self.emit_level(plan, ctx, lvl + 1, levels, body)?;
        } else {
            body(self, ctx)?;
        }

        self.resolve(next);
        let step = if level.hash.is_some() {
            OpCode::HashNext
        } else {
            OpCode::Next
        };
        self.emit_jump_to(step, cursor, top, 0, None, "");
        self.resolve(end);

        if let Some(g) = starts {
            // No row of the group matched: run the rest of the nest once
            // with the group's tables on a NULL row.
            let done = self.label();
            self.emit_jump_to(OpCode::IfPos, match_regs[g], done, 0, None, "matched: done");
            let group = &plan.groups[g];
            for l in &plan.levels[group.start..=group.end] {
                self.emit(
                    OpCode::NullRow,
                    plan.cursor(l.src),
                    0,
                    0,
                    None,
                    "NULL-extend",
                );
            }
            self.emit_jump_to(OpCode::Goto, 0, match_labels[g], 0, None, "");
            self.resolve(done);
        }
        Ok(())
    }
}
