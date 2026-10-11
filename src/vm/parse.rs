//! The one place SQL text becomes an AST.
//!
//! [`parse_sql`] is used by every statement sqawk runs. Beyond calling
//! sqlparser with [`SqawkDialect`] (SQLite operator precedence) it:
//!
//! - rewrites every compound SELECT left-deep in source order, because
//!   SQLite gives `UNION`, `UNION ALL`, `INTERSECT` and `EXCEPT` equal
//!   precedence, left to right, where sqlparser binds `INTERSECT` tighter
//!   (`A UNION ALL B INTERSECT C` is `(A UNION ALL B) INTERSECT C`). A
//!   parenthesized operand (`SetExpr::Query`) stays one operand;
//! - enforces SQLite's limits: an expression tree at most
//!   [`MAX_EXPR_DEPTH`] deep and a compound SELECT of at most
//!   [`MAX_COMPOUND_TERMS`] terms.
//!
//! The parser builds a chain such as `1+1+...+1` in a loop, so its depth is
//! unbounded, while everything downstream (compiler, `Clone`, `Display`)
//! recurses once per level. The limit walk therefore uses an explicit stack
//! and runs before anything else touches the tree.

use sqlparser::ast::{
    Expr, FromTable, FunctionArg, FunctionArgExpr, FunctionArgumentClause, FunctionArguments,
    GroupByExpr, JoinConstraint, JoinOperator, LimitClause, NamedWindowExpr, OrderByExpr,
    OrderByKind, Query, Select, SelectItem, SetExpr, SetOperator, SetQuantifier, Statement,
    TableFactor, TableWithJoins, Values, WindowFrameBound, WindowSpec, WindowType,
};
use sqlparser::parser::Parser;

use super::dialect::SqawkDialect;
use crate::error::{SqawkError, SqawkResult};

/// SQLite's `SQLITE_MAX_EXPR_DEPTH`.
pub const MAX_EXPR_DEPTH: usize = 1000;
/// SQLite's `SQLITE_MAX_COMPOUND_SELECT`.
pub const MAX_COMPOUND_TERMS: usize = 500;

/// Parse `sql` into statements: SQLite precedence, compound SELECTs
/// left-deep, depth and compound-term limits enforced.
pub fn parse_sql(sql: &str) -> SqawkResult<Vec<Statement>> {
    let mut statements =
        Parser::parse_sql(&SqawkDialect, sql).map_err(SqawkError::SqlParseError)?;
    for statement in &mut statements {
        normalize(statement)?;
    }
    Ok(statements)
}

fn too_deep() -> SqawkError {
    SqawkError::InvalidSqlQuery(format!(
        "expression tree is too large (maximum depth {})",
        MAX_EXPR_DEPTH
    ))
}

/// A node still to be visited. The `usize` is the depth of an `Expr` node
/// (1 for the root of an expression), or for the other kinds the depth of
/// the expression that contains them (0 at statement level).
enum Node<'a> {
    Expr(&'a mut Expr, usize),
    Query(&'a mut Query, usize),
    SetExpr(&'a mut SetExpr, usize),
    Select(&'a mut Select, usize),
    Table(&'a mut TableWithJoins, usize),
    Factor(&'a mut TableFactor, usize),
}

/// Rewrite compounds and check limits over one statement, iteratively.
fn normalize(statement: &mut Statement) -> SqawkResult<()> {
    let mut stack: Vec<Node> = Vec::new();
    match statement {
        Statement::Query(q) => stack.push(Node::Query(q, 0)),
        Statement::Insert(insert) => {
            if let Some(source) = insert.source.as_deref_mut() {
                stack.push(Node::Query(source, 0));
            }
        }
        Statement::Update(update) => {
            stack.push(Node::Table(&mut update.table, 0));
            for a in &mut update.assignments {
                stack.push(Node::Expr(&mut a.value, 1));
            }
            if let Some(e) = update.selection.as_mut() {
                stack.push(Node::Expr(e, 1));
            }
        }
        Statement::Delete(delete) => {
            let (FromTable::WithFromKeyword(from) | FromTable::WithoutKeyword(from)) =
                &mut delete.from;
            for t in from {
                stack.push(Node::Table(t, 0));
            }
            if let Some(e) = delete.selection.as_mut() {
                stack.push(Node::Expr(e, 1));
            }
        }
        Statement::CreateTable(create) => {
            if let Some(q) = create.query.as_deref_mut() {
                stack.push(Node::Query(q, 0));
            }
        }
        _ => {}
    }

    while let Some(node) = stack.pop() {
        match node {
            Node::Expr(e, d) => {
                if d > MAX_EXPR_DEPTH {
                    return Err(too_deep());
                }
                strip_main_schema(e)?;
                push_expr_children(e, d + 1, &mut stack);
            }
            Node::Query(q, d) => {
                rewrite_compound(&mut q.body)?;
                stack.push(Node::SetExpr(&mut q.body, d));
                if let Some(ob) = q.order_by.as_mut() {
                    if let OrderByKind::Expressions(exprs) = &mut ob.kind {
                        push_order_by(exprs, d + 1, &mut stack);
                    }
                }
                match q.limit_clause.as_mut() {
                    Some(LimitClause::LimitOffset {
                        limit,
                        offset,
                        limit_by,
                    }) => {
                        if let Some(l) = limit {
                            stack.push(Node::Expr(l, d + 1));
                        }
                        if let Some(o) = offset {
                            stack.push(Node::Expr(&mut o.value, d + 1));
                        }
                        for e in limit_by {
                            stack.push(Node::Expr(e, d + 1));
                        }
                    }
                    Some(LimitClause::OffsetCommaLimit { offset, limit }) => {
                        stack.push(Node::Expr(offset, d + 1));
                        stack.push(Node::Expr(limit, d + 1));
                    }
                    None => {}
                }
            }
            // After `rewrite_compound` a compound is left-deep, so walking it
            // with the stack (not recursion) keeps 500 terms cheap.
            Node::SetExpr(s, d) => match s {
                SetExpr::Select(sel) => stack.push(Node::Select(sel, d)),
                SetExpr::Query(q) => stack.push(Node::Query(q, d)),
                SetExpr::SetOperation { left, right, .. } => {
                    stack.push(Node::SetExpr(left, d));
                    stack.push(Node::SetExpr(right, d));
                }
                SetExpr::Values(Values { rows, .. }) => {
                    for row in rows {
                        for e in &mut row.content {
                            stack.push(Node::Expr(e, d + 1));
                        }
                    }
                }
                _ => {}
            },
            Node::Select(sel, d) => {
                let d1 = d + 1;
                for item in &mut sel.projection {
                    match item {
                        SelectItem::UnnamedExpr(e)
                        | SelectItem::ExprWithAlias { expr: e, .. }
                        | SelectItem::ExprWithAliases { expr: e, .. } => {
                            stack.push(Node::Expr(e, d1))
                        }
                        SelectItem::QualifiedWildcard(..) | SelectItem::Wildcard(_) => {}
                    }
                }
                for t in &mut sel.from {
                    stack.push(Node::Table(t, d));
                }
                if let Some(e) = sel.selection.as_mut() {
                    stack.push(Node::Expr(e, d1));
                }
                if let GroupByExpr::Expressions(exprs, _) = &mut sel.group_by {
                    for e in exprs {
                        stack.push(Node::Expr(e, d1));
                    }
                }
                if let Some(e) = sel.having.as_mut() {
                    stack.push(Node::Expr(e, d1));
                }
                if let Some(sqlparser::ast::Distinct::On(exprs)) = sel.distinct.as_mut() {
                    for e in exprs {
                        stack.push(Node::Expr(e, d1));
                    }
                }
                for w in &mut sel.named_window {
                    if let NamedWindowExpr::WindowSpec(spec) = &mut w.1 {
                        push_window_spec(spec, d1, &mut stack);
                    }
                }
                if let Some(e) = sel.qualify.as_mut() {
                    stack.push(Node::Expr(e, d1));
                }
            }
            Node::Table(t, d) => {
                stack.push(Node::Factor(&mut t.relation, d));
                for join in &mut t.joins {
                    stack.push(Node::Factor(&mut join.relation, d));
                    if let Some(JoinConstraint::On(e)) = join_constraint(&mut join.join_operator) {
                        stack.push(Node::Expr(e, d + 1));
                    }
                }
            }
            Node::Factor(f, d) => match f {
                TableFactor::Derived { subquery, .. } => stack.push(Node::Query(subquery, d)),
                TableFactor::NestedJoin {
                    table_with_joins, ..
                } => stack.push(Node::Table(table_with_joins, d)),
                _ => {}
            },
        }
    }
    Ok(())
}

fn join_constraint(op: &mut JoinOperator) -> Option<&mut JoinConstraint> {
    match op {
        JoinOperator::Join(c)
        | JoinOperator::Inner(c)
        | JoinOperator::Left(c)
        | JoinOperator::LeftOuter(c)
        | JoinOperator::Right(c)
        | JoinOperator::RightOuter(c)
        | JoinOperator::FullOuter(c)
        | JoinOperator::CrossJoin(c)
        | JoinOperator::Semi(c)
        | JoinOperator::LeftSemi(c)
        | JoinOperator::RightSemi(c)
        | JoinOperator::Anti(c)
        | JoinOperator::LeftAnti(c)
        | JoinOperator::RightAnti(c)
        | JoinOperator::StraightJoin(c)
        | JoinOperator::AsOf { constraint: c, .. } => Some(c),
        JoinOperator::CrossApply
        | JoinOperator::OuterApply
        | JoinOperator::ArrayJoin
        | JoinOperator::LeftArrayJoin
        | JoinOperator::InnerArrayJoin => None,
    }
}

fn push_order_by<'a>(exprs: &'a mut [OrderByExpr], d: usize, stack: &mut Vec<Node<'a>>) {
    for ob in exprs {
        stack.push(Node::Expr(&mut ob.expr, d));
    }
}

fn push_window_spec<'a>(spec: &'a mut WindowSpec, d: usize, stack: &mut Vec<Node<'a>>) {
    for e in &mut spec.partition_by {
        stack.push(Node::Expr(e, d));
    }
    push_order_by(&mut spec.order_by, d, stack);
    if let Some(frame) = spec.window_frame.as_mut() {
        for bound in std::iter::once(&mut frame.start_bound).chain(frame.end_bound.as_mut()) {
            if let WindowFrameBound::Preceding(Some(e)) | WindowFrameBound::Following(Some(e)) =
                bound
            {
                stack.push(Node::Expr(e, d));
            }
        }
    }
}

fn push_function_args<'a>(args: &'a mut FunctionArguments, d: usize, stack: &mut Vec<Node<'a>>) {
    match args {
        FunctionArguments::None => {}
        FunctionArguments::Subquery(q) => stack.push(Node::Query(q, d)),
        FunctionArguments::List(list) => {
            for arg in &mut list.args {
                let (FunctionArg::Named { arg, .. }
                | FunctionArg::ExprNamed { arg, .. }
                | FunctionArg::Unnamed(arg)) = arg;
                if let FunctionArgExpr::Expr(e) = arg {
                    stack.push(Node::Expr(e, d));
                }
            }
            for clause in &mut list.clauses {
                match clause {
                    FunctionArgumentClause::OrderBy(obs) => push_order_by(obs, d, stack),
                    FunctionArgumentClause::Limit(e) => stack.push(Node::Expr(e, d)),
                    _ => {}
                }
            }
        }
    }
}

/// Push the children of `e`, each at depth `d`. A subquery's expressions
/// continue from `d`, so nesting through subqueries counts too.
/// `main.t.c` is `t.c`: every table lives in SQLite's `main` schema. Any
/// other three-part name, or a longer one, names no column.
fn strip_main_schema(e: &mut Expr) -> SqawkResult<()> {
    let Expr::CompoundIdentifier(parts) = e else {
        return Ok(());
    };
    if parts.len() <= 2 {
        return Ok(());
    }
    if parts.len() == 3 && parts[0].value.eq_ignore_ascii_case("main") {
        parts.remove(0);
        return Ok(());
    }
    let name: Vec<&str> = parts.iter().map(|p| p.value.as_str()).collect();
    Err(SqawkError::ColumnNotFound(name.join(".")))
}

fn push_expr_children<'a>(e: &'a mut Expr, d: usize, stack: &mut Vec<Node<'a>>) {
    let push = |stack: &mut Vec<Node<'a>>, e: &'a mut Expr| stack.push(Node::Expr(e, d));
    match e {
        // Parentheses are not a node in SQLite's tree: `-(x)` is as deep as
        // `-x`.
        Expr::Nested(a) => stack.push(Node::Expr(a, d - 1)),
        Expr::IsFalse(a)
        | Expr::IsNotFalse(a)
        | Expr::IsTrue(a)
        | Expr::IsNotTrue(a)
        | Expr::IsNull(a)
        | Expr::IsNotNull(a)
        | Expr::IsUnknown(a)
        | Expr::IsNotUnknown(a)
        | Expr::IsNormalized { expr: a, .. }
        | Expr::UnaryOp { expr: a, .. }
        | Expr::Cast { expr: a, .. }
        | Expr::Extract { expr: a, .. }
        | Expr::Ceil { expr: a, .. }
        | Expr::Floor { expr: a, .. }
        | Expr::Collate { expr: a, .. }
        | Expr::Prefixed { value: a, .. }
        | Expr::Named { expr: a, .. }
        | Expr::OuterJoin(a)
        | Expr::Prior(a)
        | Expr::CompoundFieldAccess { root: a, .. }
        | Expr::JsonAccess { value: a, .. } => push(stack, a),
        Expr::IsDistinctFrom(a, b)
        | Expr::IsNotDistinctFrom(a, b)
        | Expr::BinaryOp {
            left: a, right: b, ..
        }
        | Expr::Like {
            expr: a,
            pattern: b,
            ..
        }
        | Expr::ILike {
            expr: a,
            pattern: b,
            ..
        }
        | Expr::SimilarTo {
            expr: a,
            pattern: b,
            ..
        }
        | Expr::RLike {
            expr: a,
            pattern: b,
            ..
        }
        | Expr::AnyOp {
            left: a, right: b, ..
        }
        | Expr::AllOp {
            left: a, right: b, ..
        }
        | Expr::InUnnest {
            expr: a,
            array_expr: b,
            ..
        }
        | Expr::AtTimeZone {
            timestamp: a,
            time_zone: b,
        }
        | Expr::Position { expr: a, r#in: b } => {
            push(stack, a);
            push(stack, b);
        }
        Expr::InList { expr, list, .. } => {
            push(stack, expr);
            for item in list {
                push(stack, item);
            }
        }
        Expr::InSubquery { expr, subquery, .. } => {
            push(stack, expr);
            stack.push(Node::Query(subquery, d));
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            push(stack, expr);
            push(stack, low);
            push(stack, high);
        }
        Expr::Convert { expr, styles, .. } => {
            push(stack, expr);
            for s in styles {
                push(stack, s);
            }
        }
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            push(stack, expr);
            for x in [substring_from, substring_for].into_iter().flatten() {
                push(stack, x);
            }
        }
        Expr::Trim {
            expr,
            trim_what,
            trim_characters,
            ..
        } => {
            push(stack, expr);
            if let Some(w) = trim_what {
                push(stack, w);
            }
            for c in trim_characters.iter_mut().flatten() {
                push(stack, c);
            }
        }
        Expr::Overlay {
            expr,
            overlay_what,
            overlay_from,
            overlay_for,
        } => {
            push(stack, expr);
            push(stack, overlay_what);
            push(stack, overlay_from);
            if let Some(f) = overlay_for {
                push(stack, f);
            }
        }
        Expr::Function(f) => {
            push_function_args(&mut f.parameters, d, stack);
            push_function_args(&mut f.args, d, stack);
            if let Some(filter) = f.filter.as_mut() {
                push(stack, filter);
            }
            if let Some(WindowType::WindowSpec(spec)) = f.over.as_mut() {
                push_window_spec(spec, d, stack);
            }
            push_order_by(&mut f.within_group, d, stack);
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(o) = operand {
                push(stack, o);
            }
            for cw in conditions {
                push(stack, &mut cw.condition);
                push(stack, &mut cw.result);
            }
            if let Some(e) = else_result {
                push(stack, e);
            }
        }
        Expr::Exists { subquery, .. } | Expr::Subquery(subquery) => {
            stack.push(Node::Query(subquery, d))
        }
        Expr::GroupingSets(sets) | Expr::Cube(sets) | Expr::Rollup(sets) => {
            for e in sets.iter_mut().flatten() {
                push(stack, e);
            }
        }
        Expr::Tuple(items) | Expr::Struct { values: items, .. } => {
            for e in items {
                push(stack, e);
            }
        }
        Expr::Array(arr) => {
            for e in &mut arr.elem {
                push(stack, e);
            }
        }
        Expr::Dictionary(fields) => {
            for f in fields {
                push(stack, &mut f.value);
            }
        }
        Expr::Map(map) => {
            for entry in &mut map.entries {
                push(stack, &mut entry.key);
                push(stack, &mut entry.value);
            }
        }
        Expr::Interval(i) => push(stack, &mut i.value),
        Expr::Lambda(l) => push(stack, &mut l.body),
        Expr::MemberOf(m) => {
            push(stack, &mut m.value);
            push(stack, &mut m.array);
        }
        Expr::Identifier(_)
        | Expr::CompoundIdentifier(_)
        | Expr::Value(_)
        | Expr::TypedString(_)
        | Expr::MatchAgainst { .. }
        | Expr::Wildcard(_)
        | Expr::QualifiedWildcard(..) => {}
    }
}

/// Rebuild a compound SELECT left-deep in source order and count its terms.
///
/// sqlparser nests a tighter-binding `INTERSECT` as a right operand; an
/// in-order walk of the `SetOperation` tree recovers the source sequence
/// `t0 op1 t1 op2 t2 ...`, which SQLite evaluates strictly left to right.
fn rewrite_compound(body: &mut Box<SetExpr>) -> SqawkResult<()> {
    if !matches!(**body, SetExpr::SetOperation { .. }) {
        return Ok(());
    }
    let placeholder = SetExpr::Values(Values {
        explicit_row: false,
        value_keyword: false,
        rows: Vec::new(),
    });
    let root = std::mem::replace(body, Box::new(placeholder));

    enum Work {
        Node(Box<SetExpr>),
        Op(SetOperator, SetQuantifier),
    }
    let mut terms: Vec<Box<SetExpr>> = Vec::new();
    let mut ops: Vec<(SetOperator, SetQuantifier)> = Vec::new();
    let mut work = vec![Work::Node(root)];
    while let Some(w) = work.pop() {
        match w {
            Work::Node(node) => match *node {
                SetExpr::SetOperation {
                    left,
                    op,
                    set_quantifier,
                    right,
                } => {
                    work.push(Work::Node(right));
                    work.push(Work::Op(op, set_quantifier));
                    work.push(Work::Node(left));
                }
                term => terms.push(Box::new(term)),
            },
            Work::Op(op, q) => ops.push((op, q)),
        }
        if terms.len() > MAX_COMPOUND_TERMS {
            return Err(SqawkError::InvalidSqlQuery(
                "too many terms in compound SELECT".to_string(),
            ));
        }
    }

    let mut terms = terms.into_iter();
    let mut acc = terms.next().expect("a compound has at least two terms");
    for ((op, set_quantifier), right) in ops.into_iter().zip(terms) {
        acc = Box::new(SetExpr::SetOperation {
            left: acc,
            op,
            set_quantifier,
            right,
        });
    }
    *body = acc;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one(sql: &str) -> Statement {
        parse_sql(sql).unwrap().pop().unwrap()
    }

    #[test]
    fn compound_is_left_deep_in_source_order() {
        let s = one("SELECT 1 UNION ALL SELECT 2 INTERSECT SELECT 3 EXCEPT SELECT 4");
        assert_eq!(
            s.to_string(),
            "SELECT 1 UNION ALL SELECT 2 INTERSECT SELECT 3 EXCEPT SELECT 4"
        );
        let Statement::Query(q) = s else { panic!() };
        let SetExpr::SetOperation { left, op, .. } = *q.body else {
            panic!()
        };
        assert_eq!(op, SetOperator::Except);
        let SetExpr::SetOperation { left, op, .. } = *left else {
            panic!()
        };
        assert_eq!(op, SetOperator::Intersect);
        assert!(matches!(
            *left,
            SetExpr::SetOperation {
                op: SetOperator::Union,
                ..
            }
        ));
    }

    #[test]
    fn parenthesized_compound_operand_is_kept() {
        let s = one("SELECT 1 UNION (SELECT 2 UNION SELECT 3) INTERSECT SELECT 4");
        let Statement::Query(q) = s else { panic!() };
        let SetExpr::SetOperation {
            left, right, op, ..
        } = *q.body
        else {
            panic!()
        };
        assert_eq!(op, SetOperator::Intersect);
        assert!(matches!(*right, SetExpr::Select(_)));
        let SetExpr::SetOperation { right, .. } = *left else {
            panic!()
        };
        assert!(matches!(*right, SetExpr::Query(_)));
    }

    #[test]
    fn precedence_matches_sqlite() {
        assert!(matches!(
            expr_of("2 = 1 < 2"),
            Expr::BinaryOp {
                op: sqlparser::ast::BinaryOperator::Eq,
                ..
            }
        ));
        assert!(matches!(
            expr_of("2 * 3 || 4"),
            Expr::BinaryOp {
                op: sqlparser::ast::BinaryOperator::Multiply,
                ..
            }
        ));
        assert!(matches!(
            expr_of("0 IS NOT DISTINCT FROM 0 AND 0"),
            Expr::BinaryOp {
                op: sqlparser::ast::BinaryOperator::And,
                ..
            }
        ));
        assert!(matches!(
            expr_of("'a' LIKE 'A' = 1"),
            Expr::BinaryOp {
                op: sqlparser::ast::BinaryOperator::Eq,
                ..
            }
        ));
        assert!(matches!(
            expr_of("-1 || 2"),
            Expr::BinaryOp {
                op: sqlparser::ast::BinaryOperator::StringConcat,
                ..
            }
        ));
        assert!(matches!(
            expr_of("1 < 2 = 2 > 1"),
            Expr::BinaryOp {
                op: sqlparser::ast::BinaryOperator::Eq,
                ..
            }
        ));
    }

    fn expr_of(sql: &str) -> Expr {
        let Statement::Query(q) = one(&format!("SELECT {sql}")) else {
            panic!()
        };
        let SetExpr::Select(sel) = *q.body else {
            panic!()
        };
        match sel.projection.into_iter().next().unwrap() {
            SelectItem::UnnamedExpr(e) => e,
            _ => panic!(),
        }
    }

    #[test]
    fn limits() {
        let chain = |n: usize| format!("SELECT {}", vec!["1"; n].join("+"));
        assert!(parse_sql(&chain(1000)).is_ok());
        let err = parse_sql(&chain(1001)).unwrap_err().to_string();
        assert!(err.contains("expression tree is too large (maximum depth 1000)"));
        let compound = |n: usize| vec!["SELECT 1"; n].join(" UNION ALL ");
        assert!(parse_sql(&compound(500)).is_ok());
        let err = parse_sql(&compound(501)).unwrap_err().to_string();
        assert!(err.contains("too many terms in compound SELECT"));
        // Inside a subquery, nesting continues to count.
        let sub = format!("SELECT (SELECT {})", vec!["1"; 1000].join("+"));
        assert!(parse_sql(&sub).is_err());
        // As in SQLite, parentheses add no depth: `-(chain)` is the chain
        // plus one.
        let neg = |n: usize| format!("SELECT -({})", vec!["1"; n].join("+"));
        assert!(parse_sql(&neg(999)).is_ok());
        assert!(parse_sql(&neg(1000)).is_err());
    }
}
