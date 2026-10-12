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
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Location, Token, TokenWithSpan, Tokenizer};

use super::dialect::SqawkDialect;
use crate::error::{SqawkError, SqawkResult};

/// SQLite's `SQLITE_MAX_EXPR_DEPTH`.
pub const MAX_EXPR_DEPTH: usize = 1000;
/// SQLite's `SQLITE_MAX_COMPOUND_SELECT`.
pub const MAX_COMPOUND_TERMS: usize = 500;

/// Parse `sql` into statements: SQLite precedence, hex integer literals,
/// compound SELECTs left-deep, depth and compound-term limits enforced.
pub fn parse_sql(sql: &str) -> SqawkResult<Vec<Statement>> {
    let mut tokens = Tokenizer::new(&SqawkDialect, sql)
        .tokenize_with_location()
        .map_err(|e| SqawkError::SqlParseError(e.into()))?;
    rewrite_hex_literals(sql, &mut tokens)?;
    drop_materialized_hints(&mut tokens);
    let mut statements = Parser::new(&SqawkDialect)
        .with_tokens_with_locations(tokens)
        .parse_statements()
        .map_err(SqawkError::SqlParseError)?;
    for statement in &mut statements {
        normalize(statement)?;
    }
    Ok(statements)
}

/// The byte range in `sql` of each token, from the tokens' (line, column)
/// locations.
fn token_offsets(sql: &str, tokens: &[TokenWithSpan]) -> Vec<(usize, usize)> {
    // Locations only grow, so one forward walk maps them all.
    let mut walk = (1u64, 1u64, sql.char_indices().peekable());
    let mut offset = |loc: Location| -> usize {
        let (line, col, chars) = &mut walk;
        while (*line, *col) < (loc.line, loc.column) {
            match chars.next() {
                Some((_, '\n')) => {
                    *line += 1;
                    *col = 1;
                }
                Some(_) => *col += 1,
                None => break,
            }
        }
        chars.peek().map_or(sql.len(), |(i, _)| *i)
    };
    tokens
        .iter()
        .map(|t| {
            let start = offset(t.span.start);
            (start, offset(t.span.end))
        })
        .collect()
}

/// SQLite's hexadecimal integer literals, `0x1F` / `0X1F`: up to 16
/// significant hex digits, read as a 64-bit two's-complement value
/// (`0xFFFFFFFFFFFFFFFF` is -1). Each becomes a decimal number token.
///
/// The tokenizer reads `0x1F` as a blob literal (the token `X'1F'` makes)
/// and `0X1F` as a word, so both are found here and told apart from a real
/// `X'1F'` by their source text. As in SQLite the literal ends at the last
/// hex digit, so `0x1g` is `0x1 AS g`; more than 16 significant digits are
/// the error "hex literal too big", and a `0x` with no hex digit after it
/// is an unrecognized token.
///
/// The tokenizer also reads `~~` as one (PostgreSQL) operator; SQLite reads
/// two `~`, so it is split here.
fn rewrite_hex_literals(sql: &str, tokens: &mut Vec<TokenWithSpan>) -> SqawkResult<()> {
    let hex_word = |t: &Token| {
        matches!(t, Token::Word(w) if w.quote_style.is_none()
            && (w.value.starts_with("0x") || w.value.starts_with("0X")))
    };
    if !tokens.iter().any(|t| {
        matches!(t.token, Token::HexStringLiteral(_) | Token::DoubleTilde) || hex_word(&t.token)
    }) {
        return Ok(());
    }
    let offsets = token_offsets(sql, tokens);
    let mut out = Vec::with_capacity(tokens.len());
    for (k, t) in std::mem::take(tokens).into_iter().enumerate() {
        let (start, end) = offsets[k];
        let text = sql.get(start..end).unwrap_or_default();
        let is_hex = match &t.token {
            Token::HexStringLiteral(_) => text.starts_with("0x") || text.starts_with("0X"),
            Token::DoubleTilde => {
                let mut second = t.clone();
                second.span.start.column += 1;
                let mut first = t;
                first.token = Token::Tilde;
                first.span.end.column = first.span.start.column + 1;
                second.token = Token::Tilde;
                out.push(first);
                out.push(second);
                continue;
            }
            other => hex_word(other),
        };
        if !is_hex {
            out.push(t);
            continue;
        }
        let after = &text[2..];
        let n = after.bytes().take_while(u8::is_ascii_hexdigit).count();
        if n == 0 {
            // `0x` alone, or `0xZZ` (the tokenizer splits that in two).
            let glued_end = match tokens_after(&offsets, k) {
                Some((s, e)) if s == end => e,
                _ => end,
            };
            return Err(SqawkError::InvalidSqlQuery(format!(
                "unrecognized token: \"{}\"",
                sql.get(start..glued_end).unwrap_or(text)
            )));
        }
        let (digits, rest) = after.split_at(n);
        let significant = digits.trim_start_matches('0');
        if significant.len() > 16 {
            return Err(SqawkError::InvalidSqlQuery(format!(
                "hex literal too big: {}{}",
                &text[..2],
                digits
            )));
        }
        let value = u64::from_str_radix(significant, 16).unwrap_or(0) as i64;
        let mut number = t.clone();
        number.token = Token::Number(value.to_string(), false);
        if !rest.is_empty() {
            number.span.end.column = number.span.start.column + 2 + n as u64;
        }
        out.push(number);
        if !rest.is_empty() {
            // `0X1G`, one word to the tokenizer: the rest is a word of its own.
            let mut word = t;
            word.span.start.column += 2 + n as u64;
            word.token = Token::make_word(rest, None);
            out.push(word);
        }
    }
    *tokens = out;
    Ok(())
}

/// SQLite's CTE hints `name AS MATERIALIZED (...)` and
/// `name AS NOT MATERIALIZED (...)` only choose a query plan, and the
/// dialect does not parse them, so they are dropped from the token stream.
///
/// Only an `AS` directly inside a WITH list counts: from `WITH` until the
/// list ends at its statement keyword (SELECT, VALUES, INSERT, UPDATE,
/// DELETE), at `;` or at the parenthesis enclosing the WITH, at the WITH's
/// own parenthesis depth. `CAST(x AS materialized)` is untouched.
fn drop_materialized_hints(tokens: &mut Vec<TokenWithSpan>) {
    let is_kw = |t: &Token, kw: Keyword| matches!(t, Token::Word(w) if w.keyword == kw && w.quote_style.is_none());
    if !tokens
        .iter()
        .any(|t| is_kw(&t.token, Keyword::MATERIALIZED))
    {
        return;
    }
    let significant: Vec<usize> = (0..tokens.len())
        .filter(|&i| !matches!(tokens[i].token, Token::Whitespace(_)))
        .collect();
    let mut drop = vec![false; tokens.len()];
    // The parenthesis depth of each WITH list being read.
    let mut lists: Vec<usize> = Vec::new();
    let mut depth = 0usize;
    for (k, &i) in significant.iter().enumerate() {
        let t = &tokens[i].token;
        match t {
            Token::LParen => depth += 1,
            Token::RParen => {
                if lists.last() == Some(&depth) {
                    lists.pop();
                }
                depth = depth.saturating_sub(1);
            }
            Token::SemiColon => lists.clear(),
            _ if is_kw(t, Keyword::WITH) => lists.push(depth),
            _ if lists.last() == Some(&depth) => {
                let ends = [
                    Keyword::SELECT,
                    Keyword::VALUES,
                    Keyword::INSERT,
                    Keyword::UPDATE,
                    Keyword::DELETE,
                ]
                .iter()
                .any(|&kw| is_kw(t, kw));
                if ends {
                    lists.pop();
                } else if is_kw(t, Keyword::AS) {
                    let next = |n: usize| significant.get(k + n).map(|&j| &tokens[j].token);
                    let skip = match (next(1), next(2), next(3)) {
                        (Some(m), Some(Token::LParen), _) if is_kw(m, Keyword::MATERIALIZED) => 1,
                        (Some(n), Some(m), Some(Token::LParen))
                            if is_kw(n, Keyword::NOT) && is_kw(m, Keyword::MATERIALIZED) =>
                        {
                            2
                        }
                        _ => 0,
                    };
                    for n in 1..=skip {
                        drop[significant[k + n]] = true;
                    }
                }
            }
            _ => {}
        }
    }
    let mut i = 0;
    tokens.retain(|_| {
        i += 1;
        !drop[i - 1]
    });
}

/// The byte range of the token after the `k`-th, if any.
fn tokens_after(offsets: &[(usize, usize)], k: usize) -> Option<(usize, usize)> {
    offsets.get(k + 1).copied()
}

fn too_deep() -> SqawkError {
    SqawkError::InvalidSqlQuery(format!(
        "expression tree is too large (maximum depth {})",
        MAX_EXPR_DEPTH
    ))
}

/// One node of a statement's tree: the unit of the walks that must reach
/// every query, FROM item and expression of a statement -- the limit and
/// compound walk here, and the derived-table / CTE pre-pass (`prepass.rs`).
/// Both enumerate children through [`for_each_child`], so they reach the
/// same places.
pub(crate) enum Node<'a> {
    Statement(&'a mut Statement),
    Query(&'a mut Query),
    SetExpr(&'a mut SetExpr),
    Select(&'a mut Select),
    Table(&'a mut TableWithJoins),
    Factor(&'a mut TableFactor),
    Expr(&'a mut Expr),
}

/// Rewrite compounds and check limits over one statement, iteratively.
///
/// Each stack entry carries a depth: for an `Expr` its depth (1 for the root
/// of an expression), for the other kinds the depth of the expression that
/// contains them (0 at statement level). After `rewrite_compound` a compound
/// is left-deep, so walking it with the stack (not recursion) keeps 500
/// terms cheap.
fn normalize(statement: &mut Statement) -> SqawkResult<()> {
    let mut stack: Vec<(Node, usize)> = vec![(Node::Statement(statement), 0)];
    while let Some((node, d)) = stack.pop() {
        match node {
            Node::Expr(e) => {
                if d > MAX_EXPR_DEPTH {
                    return Err(too_deep());
                }
                strip_main_schema(e)?;
                // Parentheses are not a node in SQLite's tree: `-(x)` is as
                // deep as `-x`.
                let child = if matches!(e, Expr::Nested(_)) {
                    d
                } else {
                    d + 1
                };
                for_each_child(Node::Expr(e), &mut |c| stack.push((c, child)));
            }
            Node::Query(q) => {
                rewrite_compound(&mut q.body)?;
                push_children(Node::Query(q), d, &mut stack);
            }
            other => push_children(other, d, &mut stack),
        }
    }
    Ok(())
}

/// Push the children of a node that is not an expression: an expression
/// child starts one level below `d`, anything else stays at `d`.
fn push_children<'a>(node: Node<'a>, d: usize, stack: &mut Vec<(Node<'a>, usize)>) {
    for_each_child(node, &mut |c| {
        let depth = if matches!(c, Node::Expr(_)) { d + 1 } else { d };
        stack.push((c, depth));
    });
}

/// Call `f` on each child of `node`: the queries, FROM items and
/// expressions it contains, including a query's CTE bodies and the
/// statement inside `WITH ... INSERT / UPDATE / DELETE`. FROM items come
/// before the expressions of a SELECT, so a walk resolves them first.
///
/// Expressions are enumerated exhaustively (a new `Expr` variant fails to
/// compile in [`expr_children`]); query clauses sqawk rejects (TOP, SORT BY,
/// LATERAL VIEW, ...) are not visited.
pub(crate) fn for_each_child<'a>(node: Node<'a>, f: &mut dyn FnMut(Node<'a>)) {
    match node {
        Node::Statement(s) => statement_children(s, f),
        Node::Query(q) => {
            if let Some(with) = q.with.as_mut() {
                for cte in &mut with.cte_tables {
                    f(Node::Query(&mut cte.query));
                }
            }
            f(Node::SetExpr(&mut q.body));
            if let Some(ob) = q.order_by.as_mut() {
                if let OrderByKind::Expressions(exprs) = &mut ob.kind {
                    order_by_children(exprs, f);
                }
            }
            match q.limit_clause.as_mut() {
                Some(LimitClause::LimitOffset {
                    limit,
                    offset,
                    limit_by,
                }) => {
                    if let Some(l) = limit {
                        f(Node::Expr(l));
                    }
                    if let Some(o) = offset {
                        f(Node::Expr(&mut o.value));
                    }
                    for e in limit_by {
                        f(Node::Expr(e));
                    }
                }
                Some(LimitClause::OffsetCommaLimit { offset, limit }) => {
                    f(Node::Expr(offset));
                    f(Node::Expr(limit));
                }
                None => {}
            }
        }
        Node::SetExpr(s) => match s {
            SetExpr::Select(sel) => f(Node::Select(sel)),
            SetExpr::Query(q) => f(Node::Query(q)),
            SetExpr::SetOperation { left, right, .. } => {
                f(Node::SetExpr(left));
                f(Node::SetExpr(right));
            }
            SetExpr::Values(Values { rows, .. }) => {
                for row in rows {
                    for e in &mut row.content {
                        f(Node::Expr(e));
                    }
                }
            }
            SetExpr::Insert(stmt) | SetExpr::Update(stmt) | SetExpr::Delete(stmt) => {
                f(Node::Statement(stmt))
            }
            SetExpr::Table(_) | SetExpr::Merge(_) => {}
        },
        Node::Select(sel) => {
            for t in &mut sel.from {
                f(Node::Table(t));
            }
            for item in &mut sel.projection {
                match item {
                    SelectItem::UnnamedExpr(e)
                    | SelectItem::ExprWithAlias { expr: e, .. }
                    | SelectItem::ExprWithAliases { expr: e, .. } => f(Node::Expr(e)),
                    SelectItem::QualifiedWildcard(..) | SelectItem::Wildcard(_) => {}
                }
            }
            if let Some(e) = sel.selection.as_mut() {
                f(Node::Expr(e));
            }
            if let GroupByExpr::Expressions(exprs, _) = &mut sel.group_by {
                for e in exprs {
                    f(Node::Expr(e));
                }
            }
            if let Some(e) = sel.having.as_mut() {
                f(Node::Expr(e));
            }
            if let Some(sqlparser::ast::Distinct::On(exprs)) = sel.distinct.as_mut() {
                for e in exprs {
                    f(Node::Expr(e));
                }
            }
            for w in &mut sel.named_window {
                if let NamedWindowExpr::WindowSpec(spec) = &mut w.1 {
                    window_spec_children(spec, f);
                }
            }
            if let Some(e) = sel.qualify.as_mut() {
                f(Node::Expr(e));
            }
        }
        Node::Table(t) => {
            f(Node::Factor(&mut t.relation));
            for join in &mut t.joins {
                f(Node::Factor(&mut join.relation));
                if let Some(JoinConstraint::On(e)) = join_constraint(&mut join.join_operator) {
                    f(Node::Expr(e));
                }
            }
        }
        Node::Factor(fac) => match fac {
            TableFactor::Derived { subquery, .. } => f(Node::Query(subquery)),
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => f(Node::Table(table_with_joins)),
            _ => {}
        },
        Node::Expr(e) => expr_children(e, f),
    }
}

/// The children of a statement: its query, its FROM items and expressions.
fn statement_children<'a>(statement: &'a mut Statement, f: &mut dyn FnMut(Node<'a>)) {
    match statement {
        Statement::Query(q) => f(Node::Query(q)),
        Statement::Insert(insert) => {
            if let Some(source) = insert.source.as_deref_mut() {
                f(Node::Query(source));
            }
        }
        Statement::Update(update) => {
            f(Node::Table(&mut update.table));
            if let Some(
                sqlparser::ast::UpdateTableFromKind::BeforeSet(from)
                | sqlparser::ast::UpdateTableFromKind::AfterSet(from),
            ) = update.from.as_mut()
            {
                for t in from {
                    f(Node::Table(t));
                }
            }
            for a in &mut update.assignments {
                f(Node::Expr(&mut a.value));
            }
            if let Some(e) = update.selection.as_mut() {
                f(Node::Expr(e));
            }
        }
        Statement::Delete(delete) => {
            let (FromTable::WithFromKeyword(from) | FromTable::WithoutKeyword(from)) =
                &mut delete.from;
            for t in from {
                f(Node::Table(t));
            }
            for t in delete.using.iter_mut().flatten() {
                f(Node::Table(t));
            }
            if let Some(e) = delete.selection.as_mut() {
                f(Node::Expr(e));
            }
        }
        Statement::CreateTable(create) => {
            if let Some(q) = create.query.as_deref_mut() {
                f(Node::Query(q));
            }
        }
        _ => {}
    }
}

pub(crate) fn join_constraint(op: &mut JoinOperator) -> Option<&mut JoinConstraint> {
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

fn order_by_children<'a>(exprs: &'a mut [OrderByExpr], f: &mut dyn FnMut(Node<'a>)) {
    for ob in exprs {
        f(Node::Expr(&mut ob.expr));
    }
}

fn window_spec_children<'a>(spec: &'a mut WindowSpec, f: &mut dyn FnMut(Node<'a>)) {
    for e in &mut spec.partition_by {
        f(Node::Expr(e));
    }
    order_by_children(&mut spec.order_by, f);
    if let Some(frame) = spec.window_frame.as_mut() {
        for bound in std::iter::once(&mut frame.start_bound).chain(frame.end_bound.as_mut()) {
            if let WindowFrameBound::Preceding(Some(e)) | WindowFrameBound::Following(Some(e)) =
                bound
            {
                f(Node::Expr(e));
            }
        }
    }
}

fn function_arg_children<'a>(args: &'a mut FunctionArguments, f: &mut dyn FnMut(Node<'a>)) {
    match args {
        FunctionArguments::None => {}
        FunctionArguments::Subquery(q) => f(Node::Query(q)),
        FunctionArguments::List(list) => {
            for arg in &mut list.args {
                let (FunctionArg::Named { arg, .. }
                | FunctionArg::ExprNamed { arg, .. }
                | FunctionArg::Unnamed(arg)) = arg;
                if let FunctionArgExpr::Expr(e) = arg {
                    f(Node::Expr(e));
                }
            }
            for clause in &mut list.clauses {
                match clause {
                    FunctionArgumentClause::OrderBy(obs) => order_by_children(obs, f),
                    FunctionArgumentClause::Limit(e) => f(Node::Expr(e)),
                    _ => {}
                }
            }
        }
    }
}

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

/// The children of an expression: its operand expressions and subqueries.
fn expr_children<'a>(e: &'a mut Expr, stack: &mut dyn FnMut(Node<'a>)) {
    let push = |stack: &mut dyn FnMut(Node<'a>), e: &'a mut Expr| stack(Node::Expr(e));
    match e {
        Expr::Nested(a)
        | Expr::IsFalse(a)
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
            stack(Node::Query(subquery));
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
            function_arg_children(&mut f.parameters, stack);
            function_arg_children(&mut f.args, stack);
            if let Some(filter) = f.filter.as_mut() {
                push(stack, filter);
            }
            if let Some(WindowType::WindowSpec(spec)) = f.over.as_mut() {
                window_spec_children(spec, stack);
            }
            order_by_children(&mut f.within_group, stack);
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
        Expr::Exists { subquery, .. } | Expr::Subquery(subquery) => stack(Node::Query(subquery)),
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

/// The text a statement was parsed from, tokenized with byte offsets, so a
/// result column can be named by its expression exactly as written.
///
/// sqlparser's expression spans do not cover a whole expression (`-x`
/// spans `x`, `CAST(x AS INT)` spans `x`, `(a)` spans `a`), so names are
/// cut from the token stream instead: the tokens between a SELECT keyword
/// and the end of its result list, split at top-level commas.
pub struct SqlSource {
    text: String,
    /// Every token in source order: its (line, column) location and its
    /// byte range in `text`.
    tokens: Vec<(Token, (u64, u64), usize, usize)>,
}

impl SqlSource {
    /// Tokenize `sql` as [`parse_sql`] does. A text that does not tokenize
    /// yields a source without tokens, which names nothing.
    pub fn new(sql: &str) -> Self {
        let mut tokens = Vec::new();
        if let Ok(toks) = Tokenizer::new(&SqawkDialect, sql).tokenize_with_location() {
            let offsets = token_offsets(sql, &toks);
            for (t, (start, end)) in toks.into_iter().zip(offsets) {
                tokens.push((
                    t.token,
                    (t.span.start.line, t.span.start.column),
                    start,
                    end,
                ));
            }
        }
        SqlSource {
            text: sql.to_string(),
            tokens,
        }
    }

    /// The source text of each item of `select`'s result list, from its
    /// first token to its last (an item's alias included), or `None` when
    /// the list cannot be located in the text.
    pub fn select_item_texts(&self, select: &Select) -> Option<Vec<&str>> {
        let start = select.select_token.0.span.start;
        if start.line == 0 {
            return None;
        }
        // The SELECT keyword's token: the one starting at that location.
        let mut i = self
            .tokens
            .binary_search_by_key(&(start.line, start.column), |t| t.1)
            .ok()?;
        if !matches!(&self.tokens[i].0, Token::Word(w) if w.keyword == Keyword::SELECT) {
            return None;
        }
        i += 1;
        let significant = |t: &Token| !matches!(t, Token::Whitespace(_));
        while i < self.tokens.len() && !significant(&self.tokens[i].0) {
            i += 1;
        }
        if matches!(&self.tokens.get(i)?.0,
            Token::Word(w) if matches!(w.keyword, Keyword::DISTINCT | Keyword::ALL))
        {
            i += 1;
        }

        let mut items = Vec::new();
        let mut first: Option<usize> = None;
        let mut last = 0;
        let mut depth = 0usize;
        let mut prev_distinct = false;
        let mut push = |first: &mut Option<usize>, last: usize| -> Option<()> {
            let f = (*first)?;
            items.push(&self.text[self.tokens[f].2..self.tokens[last].3]);
            *first = None;
            Some(())
        };
        while let Some((tok, ..)) = self.tokens.get(i) {
            if !significant(tok) {
                i += 1;
                continue;
            }
            match tok {
                Token::LParen | Token::LBracket | Token::LBrace => depth += 1,
                Token::RParen | Token::RBracket | Token::RBrace => {
                    if depth == 0 {
                        break;
                    }
                    depth -= 1;
                }
                Token::Comma if depth == 0 => {
                    push(&mut first, last)?;
                    i += 1;
                    continue;
                }
                Token::SemiColon if depth == 0 => break,
                Token::Word(w) if depth == 0 => {
                    let ends = match w.keyword {
                        // `a IS [NOT] DISTINCT FROM b` stays in the item.
                        Keyword::FROM => !prev_distinct,
                        Keyword::WHERE
                        | Keyword::GROUP
                        | Keyword::HAVING
                        | Keyword::WINDOW
                        | Keyword::QUALIFY
                        | Keyword::ORDER
                        | Keyword::LIMIT
                        | Keyword::OFFSET
                        | Keyword::UNION
                        | Keyword::INTERSECT
                        | Keyword::EXCEPT
                        | Keyword::INTO => w.quote_style.is_none(),
                        _ => false,
                    };
                    if ends {
                        break;
                    }
                }
                _ => {}
            }
            prev_distinct = matches!(tok, Token::Word(w) if w.keyword == Keyword::DISTINCT);
            first.get_or_insert(i);
            last = i;
            i += 1;
        }
        push(&mut first, last)?;
        (items.len() == select.projection.len()).then_some(items)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The item texts of the first SELECT of `sql`, and of the SELECT of the
    /// scalar subquery that is its first item when there is one.
    fn texts(sql: &str) -> Vec<String> {
        let source = SqlSource::new(sql);
        let Statement::Query(q) = one(sql) else {
            panic!("not a query")
        };
        let SetExpr::Select(sel) = q.body.as_ref() else {
            panic!("not a select")
        };
        let mut out: Vec<String> = source
            .select_item_texts(sel)
            .expect("located")
            .into_iter()
            .map(String::from)
            .collect();
        if let SelectItem::UnnamedExpr(Expr::Subquery(sub)) = &sel.projection[0] {
            if let SetExpr::Select(inner) = sub.body.as_ref() {
                out.extend(
                    source
                        .select_item_texts(inner)
                        .expect("inner")
                        .into_iter()
                        .map(String::from),
                );
            }
        }
        out
    }

    #[test]
    fn select_item_texts_are_the_source_spans() {
        assert_eq!(
            texts("SELECT n  +  1, COUNT( * ) FROM t"),
            ["n  +  1", "COUNT( * )"]
        );
        assert_eq!(
            texts("select distinct -a /* c */ , cast(a as text)\n from t where x"),
            ["-a", "cast(a as text)"]
        );
        assert_eq!(
            texts("SELECT a IS NOT DISTINCT FROM 5, b AS x FROM t"),
            ["a IS NOT DISTINCT FROM 5", "b AS x"]
        );
        assert_eq!(
            texts("SELECT 'é,', f(a, b) OVER (PARTITION BY c, d)"),
            ["'é,'", "f(a, b) OVER (PARTITION BY c, d)"]
        );
        assert_eq!(
            texts("SELECT (SELECT max(v), 2\nFROM u) FROM t ORDER BY 1"),
            ["(SELECT max(v), 2\nFROM u)", "max(v)", "2"]
        );
    }

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
    fn materialized_hints_are_dropped_only_in_a_with_list() {
        let s = one("WITH a AS MATERIALIZED (SELECT 1), b AS NOT MATERIALIZED (SELECT 2) SELECT 1");
        let Statement::Query(q) = s else { panic!() };
        assert_eq!(q.with.expect("a WITH").cte_tables.len(), 2);
        // A type name spelled MATERIALIZED is not a hint.
        let s = one("SELECT CAST(1 AS materialized), (WITH c AS (SELECT 1) SELECT * FROM c)");
        assert_eq!(
            s.to_string(),
            "SELECT CAST(1 AS materialized), (WITH c AS (SELECT 1) SELECT * FROM c)"
        );
    }

    #[test]
    fn limits_reach_cte_bodies() {
        let deep = format!(
            "WITH c AS (SELECT {}) SELECT * FROM c",
            vec!["1"; 1001].join("+")
        );
        assert!(parse_sql(&deep).is_err());
        let compound = format!(
            "WITH c AS ({}) SELECT * FROM c",
            vec!["SELECT 1"; 501].join(" UNION ALL ")
        );
        assert!(parse_sql(&compound).is_err());
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
