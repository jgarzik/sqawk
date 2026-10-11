//! Small accessors that normalize sqlparser AST shapes.
//!
//! sqlparser treats every AST change as breaking, and a few of its types have
//! grown structure that the compiler does not care about. Rather than spread
//! that structure across ~150 call sites, the compiler goes through the
//! accessors here.
//!
//! This is deliberately thin: it adapts *shape*, never *meaning*. Anything that
//! requires a semantic decision (which join kinds are supported, how DISTINCT
//! affects an aggregate) belongs in the compiler proper.

use sqlparser::ast::{
    Assignment, AssignmentTarget, CreateTableOptions, DuplicateTreatment, Expr, FromTable,
    Function, FunctionArg, FunctionArguments, GroupByExpr, LimitClause, ObjectName, OrderBy,
    OrderByExpr, OrderByKind, Query, SqlOption, TableObject, TableWithJoins,
};

use crate::error::{SqawkError, SqawkResult};

/// The positional argument list of a function call.
///
/// `FunctionArguments` distinguishes three cases that the compiler mostly does
/// not need to: no parentheses at all (`CURRENT_TIMESTAMP`), a bare subquery
/// argument, and an ordinary parenthesized list. Only the last carries
/// positional arguments; the other two yield an empty slice.
pub(crate) fn func_args(func: &Function) -> &[FunctionArg] {
    match &func.args {
        FunctionArguments::List(list) => &list.args,
        FunctionArguments::None | FunctionArguments::Subquery(_) => &[],
    }
}

/// Whether a function call was written with `DISTINCT`, as in
/// `COUNT(DISTINCT department)`.
///
/// Older sqlparser exposed this as a plain `Function::distinct` bool; it now
/// lives in the argument list as a `DuplicateTreatment`. `ALL` is the default
/// and means the same thing as omitting it.
pub(crate) fn func_is_distinct(func: &Function) -> bool {
    match &func.args {
        FunctionArguments::List(list) => {
            matches!(list.duplicate_treatment, Some(DuplicateTreatment::Distinct))
        }
        FunctionArguments::None | FunctionArguments::Subquery(_) => false,
    }
}

/// The GROUP BY key expressions.
///
/// `GROUP BY ALL` has no explicit key list; it yields an empty slice here and
/// is rejected by the caller that cares.
pub(crate) fn group_by_exprs(group_by: &GroupByExpr) -> &[sqlparser::ast::Expr] {
    match group_by {
        GroupByExpr::Expressions(exprs, _) => exprs,
        GroupByExpr::All(_) => &[],
    }
}

/// Whether this GROUP BY is the unsupported `GROUP BY ALL` form.
pub(crate) fn is_group_by_all(group_by: &GroupByExpr) -> bool {
    matches!(group_by, GroupByExpr::All(_))
}

/// The ORDER BY key expressions of a query.
///
/// `Query::order_by` is now an `Option<OrderBy>` whose kind distinguishes an
/// explicit key list from `ORDER BY ALL`. Absent ORDER BY and `ORDER BY ALL`
/// both yield an empty slice; the caller that cares rejects the latter.
pub(crate) fn query_order_by(query: &Query) -> &[OrderByExpr] {
    match &query.order_by {
        Some(OrderBy {
            kind: OrderByKind::Expressions(exprs),
            ..
        }) => exprs,
        _ => &[],
    }
}

/// Whether this query uses the unsupported `ORDER BY ALL` form.
pub(crate) fn is_order_by_all(query: &Query) -> bool {
    matches!(
        &query.order_by,
        Some(OrderBy {
            kind: OrderByKind::All(_),
            ..
        })
    )
}

/// Sort direction for one ORDER BY key: `true` ascending, `false` descending.
///
/// Ascending is the SQL default when the direction is omitted. `asc` moved
/// from `OrderByExpr` into a nested `OrderByOptions`.
pub(crate) fn order_by_is_asc(expr: &OrderByExpr) -> bool {
    expr.options.asc.unwrap_or(true)
}

/// The LIMIT expression, if any.
///
/// `Query::limit` and `Query::offset` were merged into a single
/// `limit_clause` that also models MySQL's reversed `LIMIT <offset>, <limit>`.
pub(crate) fn query_limit(query: &Query) -> Option<&Expr> {
    match &query.limit_clause {
        Some(LimitClause::LimitOffset { limit, .. }) => limit.as_ref(),
        Some(LimitClause::OffsetCommaLimit { limit, .. }) => Some(limit),
        None => None,
    }
}

/// The OFFSET expression, if any. See [`query_limit`].
pub(crate) fn query_offset(query: &Query) -> Option<&Expr> {
    match &query.limit_clause {
        Some(LimitClause::LimitOffset { offset, .. }) => offset.as_ref().map(|o| &o.value),
        Some(LimitClause::OffsetCommaLimit { offset, .. }) => Some(offset),
        None => None,
    }
}

/// The target table of an INSERT.
///
/// `Insert::table` is now a `TableObject`, which can also be a table function.
pub(crate) fn insert_target(table: &TableObject) -> SqawkResult<&ObjectName> {
    match table {
        TableObject::TableName(name) => Ok(name),
        TableObject::TableFunction(_) | TableObject::TableQuery(_) => Err(
            SqawkError::UnsupportedSqlFeature("INSERT target must be a plain table name".into()),
        ),
    }
}

/// The source tables of a DELETE.
///
/// `Delete::from` distinguishes `DELETE FROM t` from `DELETE t`; sqawk treats
/// them the same.
pub(crate) fn delete_from(from: &FromTable) -> &[TableWithJoins] {
    match from {
        FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => t,
    }
}

/// The assigned column of an UPDATE `SET` clause.
///
/// `Assignment::id` (a `Vec<Ident>`) became `target: AssignmentTarget`.
pub(crate) fn assignment_column(assignment: &Assignment) -> SqawkResult<String> {
    match &assignment.target {
        AssignmentTarget::ColumnName(name) => Ok(object_name_last(name)),
        AssignmentTarget::Tuple(_) => Err(SqawkError::UnsupportedSqlFeature(
            "Tuple assignment in UPDATE is not supported".into(),
        )),
    }
}

/// The `WITH (...)` options attached to a CREATE TABLE.
///
/// Previously a plain `Vec<SqlOption>`; the dialect-specific spellings are now
/// distinguished by `CreateTableOptions`, which sqawk does not care about.
pub(crate) fn create_table_options(options: &CreateTableOptions) -> &[SqlOption] {
    match options {
        CreateTableOptions::With(o)
        | CreateTableOptions::Options(o)
        | CreateTableOptions::Plain(o)
        | CreateTableOptions::TableProperties(o) => o,
        _ => &[],
    }
}

/// A CREATE TABLE option as a `(name, value)` pair, for the options sqawk
/// understands (`delimiter`, `header`, ...). Non key-value forms yield `None`.
pub(crate) fn sql_option_key_value(option: &SqlOption) -> Option<(String, String)> {
    match option {
        SqlOption::KeyValue { key, value } => {
            // Unwrap the literal rather than rendering it back to SQL.
            //
            // `value.to_string()` produces the SQL text INCLUDING quotes, so a
            // double-quoted option value kept its quote characters: `WITH
            // (delimiter = "|")` yielded the three-character delimiter `"|"`
            // and wrote rows no reader could parse back.
            let text = match value {
                Expr::Value(v) => match &v.value {
                    sqlparser::ast::Value::SingleQuotedString(s)
                    | sqlparser::ast::Value::DoubleQuotedString(s)
                    | sqlparser::ast::Value::Number(s, _) => s.clone(),
                    other => other.to_string(),
                },
                // A double-quoted option value parses as a QUOTED IDENTIFIER,
                // not a string literal, so `WITH (delimiter = "|")` arrives
                // here as an Ident. Its `value` is already unquoted.
                Expr::Identifier(ident) => ident.value.clone(),
                other => other.to_string(),
            };
            Some((key.value.clone(), text))
        }
        _ => None,
    }
}

/// The table an `ObjectName` names in a table position: `t` or `main.t`.
///
/// Every table lives in SQLite's `main` schema (matched case-insensitively),
/// so that is the one schema qualifier accepted; any other schema, or a
/// catalog, names a table that does not exist.
pub(crate) fn main_table_name(name: &ObjectName) -> SqawkResult<String> {
    match name.0.as_slice() {
        [_] => Ok(object_name_last(name)),
        [schema, _]
            if schema
                .as_ident()
                .is_some_and(|i| i.value.eq_ignore_ascii_case("main")) =>
        {
            Ok(object_name_last(name))
        }
        _ => Err(SqawkError::TableNotFound(name.to_string())),
    }
}

/// The final segment of an `ObjectName` -- the bare table or function name.
pub(crate) fn object_name_last(name: &ObjectName) -> String {
    match name.0.last() {
        Some(part) => match part.as_ident() {
            Some(ident) => ident.value.clone(),
            None => part.to_string(),
        },
        None => String::new(),
    }
}
