//! Rejecting syntax that parses but that sqawk does not implement.
//!
//! sqlparser accepts the union of many dialects, so a statement can carry
//! clauses (`RETURNING`, `ON CONFLICT`, `UPDATE ... FROM`, `QUALIFY`, `TOP`,
//! `FETCH`, ...) that the compiler never looks at. Ignoring such a clause is
//! the worst possible outcome -- `DELETE ... USING` deleting every row, or
//! `INSERT OR REPLACE` duplicating keys -- so every AST node the compiler
//! consumes is checked here first, and anything it would not honour is an
//! "Unsupported" error before a single row is touched.
//!
//! The checks destructure each node EXHAUSTIVELY (no `..`), so a field added
//! by a future sqlparser release fails to compile here until someone decides
//! whether sqawk honours it.

use sqlparser::ast::{
    CreateTable, Delete, FromTable, GroupByExpr, HiveDistributionStyle, HiveFormat, Insert,
    LimitClause, OrderBy, OrderByExpr, OrderByOptions, Query, Select, SelectFlavor, TableAlias,
    TableFactor, TableWithJoins, Update,
};

use crate::error::{SqawkError, SqawkResult};

fn unsupported<T>(what: &str) -> SqawkResult<T> {
    Err(SqawkError::UnsupportedSqlFeature(format!(
        "{} is not supported",
        what
    )))
}

/// Fail with "`what` is not supported" when `present`.
fn reject(present: bool, what: &str) -> SqawkResult<()> {
    if present {
        unsupported(what)
    } else {
        Ok(())
    }
}

/// INSERT: only `INSERT INTO t [(cols)] VALUES ... | SELECT ...`.
pub(crate) fn check_insert(insert: &Insert) -> SqawkResult<()> {
    let Insert {
        insert_token: _,
        optimizer_hints,
        or,
        ignore,
        into: _,
        table: _,
        table_alias,
        columns: _,
        overwrite,
        source: _,
        assignments,
        partitioned,
        after_columns,
        has_table_keyword: _,
        on,
        returning,
        output,
        replace_into,
        priority,
        insert_alias,
        settings,
        format_clause,
        multi_table_insert_type,
        multi_table_into_clauses,
        multi_table_when_clauses,
        multi_table_else_clause,
    } = insert;
    reject(or.is_some(), "INSERT OR <conflict action>")?;
    reject(*replace_into, "REPLACE INTO")?;
    reject(*ignore, "INSERT IGNORE")?;
    reject(on.is_some(), "INSERT ... ON CONFLICT / ON DUPLICATE KEY")?;
    reject(returning.is_some(), "INSERT ... RETURNING")?;
    reject(output.is_some(), "INSERT ... OUTPUT")?;
    reject(table_alias.is_some(), "An alias on the INSERT target")?;
    reject(*overwrite, "INSERT OVERWRITE")?;
    reject(!assignments.is_empty(), "INSERT ... SET")?;
    reject(partitioned.is_some(), "INSERT ... PARTITION")?;
    reject(
        !after_columns.is_empty(),
        "Columns after PARTITION in INSERT",
    )?;
    reject(priority.is_some(), "INSERT priority")?;
    reject(insert_alias.is_some(), "INSERT ... AS alias")?;
    reject(settings.is_some(), "INSERT ... SETTINGS")?;
    reject(format_clause.is_some(), "INSERT ... FORMAT")?;
    reject(!optimizer_hints.is_empty(), "Optimizer hints")?;
    reject(
        multi_table_insert_type.is_some()
            || !multi_table_into_clauses.is_empty()
            || !multi_table_when_clauses.is_empty()
            || multi_table_else_clause.is_some(),
        "Multi-table INSERT",
    )
}

/// UPDATE: only `UPDATE t SET ... [WHERE ...]`.
pub(crate) fn check_update(update: &Update) -> SqawkResult<()> {
    let Update {
        update_token: _,
        optimizer_hints,
        table,
        assignments: _,
        from,
        selection: _,
        returning,
        output,
        or,
        order_by,
        limit,
    } = update;
    reject(from.is_some(), "UPDATE ... FROM")?;
    reject(returning.is_some(), "UPDATE ... RETURNING")?;
    reject(output.is_some(), "UPDATE ... OUTPUT")?;
    reject(or.is_some(), "UPDATE OR <conflict action>")?;
    reject(!order_by.is_empty(), "UPDATE ... ORDER BY")?;
    reject(limit.is_some(), "UPDATE ... LIMIT")?;
    reject(!optimizer_hints.is_empty(), "Optimizer hints")?;
    reject(!table.joins.is_empty(), "UPDATE with a JOIN")?;
    check_dml_target(&table.relation, "UPDATE")
}

/// DELETE: only `DELETE FROM t [WHERE ...]`.
pub(crate) fn check_delete(delete: &Delete) -> SqawkResult<()> {
    let Delete {
        delete_token: _,
        optimizer_hints,
        tables,
        from,
        using,
        selection: _,
        returning,
        output,
        order_by,
        limit,
    } = delete;
    reject(
        !tables.is_empty(),
        "DELETE <tables> FROM (multi-table DELETE)",
    )?;
    reject(using.is_some(), "DELETE ... USING")?;
    reject(returning.is_some(), "DELETE ... RETURNING")?;
    reject(output.is_some(), "DELETE ... OUTPUT")?;
    reject(!order_by.is_empty(), "DELETE ... ORDER BY")?;
    reject(limit.is_some(), "DELETE ... LIMIT")?;
    reject(!optimizer_hints.is_empty(), "Optimizer hints")?;
    let from = match from {
        FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => t,
    };
    match from.as_slice() {
        [TableWithJoins { relation, joins }] => {
            reject(!joins.is_empty(), "DELETE with a JOIN")?;
            check_dml_target(relation, "DELETE")
        }
        _ => unsupported("DELETE from several tables"),
    }
}

/// The target of an UPDATE or DELETE: a plain table, optionally aliased.
fn check_dml_target(factor: &TableFactor, stmt: &str) -> SqawkResult<()> {
    match factor {
        TableFactor::Table { .. } => check_table_factor(factor),
        _ => unsupported(&format!("A {} target other than a table", stmt)),
    }
}

/// A query: its body is checked by the SELECT compiler; here, every
/// query-level clause besides ORDER BY and LIMIT/OFFSET.
pub(crate) fn check_query(query: &Query) -> SqawkResult<()> {
    let Query {
        with,
        body: _,
        order_by,
        limit_clause,
        fetch,
        locks,
        for_clause,
        settings,
        format_clause,
        pipe_operators,
    } = query;
    reject(with.is_some(), "WITH (common table expressions)")?;
    reject(fetch.is_some(), "FETCH FIRST")?;
    reject(!locks.is_empty(), "FOR UPDATE / FOR SHARE")?;
    reject(for_clause.is_some(), "FOR XML / FOR JSON")?;
    reject(settings.is_some(), "SETTINGS")?;
    reject(format_clause.is_some(), "FORMAT")?;
    reject(!pipe_operators.is_empty(), "Pipe operators")?;
    if let Some(OrderBy {
        kind: _,
        interpolate,
    }) = order_by
    {
        reject(interpolate.is_some(), "ORDER BY ... INTERPOLATE")?;
    }
    if let Some(LimitClause::LimitOffset { limit_by, .. }) = limit_clause {
        reject(!limit_by.is_empty(), "LIMIT ... BY")?;
    }
    Ok(())
}

/// A SELECT core: every clause besides the projection, FROM, WHERE,
/// GROUP BY, HAVING, DISTINCT and WINDOW.
pub(crate) fn check_select(select: &Select) -> SqawkResult<()> {
    let Select {
        select_token: _,
        optimizer_hints,
        distinct: _,
        select_modifiers,
        top,
        top_before_distinct: _,
        projection: _,
        exclude,
        into,
        from,
        lateral_views,
        prewhere,
        selection: _,
        connect_by,
        group_by,
        cluster_by,
        distribute_by,
        sort_by,
        having: _,
        named_window: _,
        qualify,
        window_before_qualify: _,
        value_table_mode,
        flavor,
    } = select;
    reject(top.is_some(), "SELECT TOP")?;
    reject(into.is_some(), "SELECT ... INTO")?;
    reject(exclude.is_some(), "SELECT * EXCLUDE")?;
    reject(select_modifiers.is_some(), "SELECT modifiers")?;
    reject(!lateral_views.is_empty(), "LATERAL VIEW")?;
    reject(prewhere.is_some(), "PREWHERE")?;
    reject(!connect_by.is_empty(), "CONNECT BY")?;
    reject(!cluster_by.is_empty(), "CLUSTER BY")?;
    reject(!distribute_by.is_empty(), "DISTRIBUTE BY")?;
    reject(!sort_by.is_empty(), "SORT BY")?;
    reject(qualify.is_some(), "QUALIFY")?;
    reject(value_table_mode.is_some(), "SELECT AS STRUCT / AS VALUE")?;
    reject(!optimizer_hints.is_empty(), "Optimizer hints")?;
    reject(
        !matches!(flavor, SelectFlavor::Standard),
        "FROM-first SELECT syntax",
    )?;
    if let GroupByExpr::Expressions(_, modifiers) = group_by {
        reject(
            !modifiers.is_empty(),
            "GROUP BY modifiers (ROLLUP / CUBE / TOTALS)",
        )?;
    }
    for twj in from {
        check_table_factor(&twj.relation)?;
        for join in &twj.joins {
            reject(join.global, "GLOBAL JOIN")?;
            check_table_factor(&join.relation)?;
        }
    }
    Ok(())
}

/// A FROM item. Only the parts of a table reference sqawk honours.
fn check_table_factor(factor: &TableFactor) -> SqawkResult<()> {
    match factor {
        TableFactor::Table {
            name: _,
            alias,
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
            sample,
            index_hints,
        } => {
            reject(args.is_some(), "Table-valued functions")?;
            reject(!with_hints.is_empty(), "Table hints (WITH (...))")?;
            reject(version.is_some(), "Time travel (FOR SYSTEM_TIME / AT)")?;
            reject(*with_ordinality, "WITH ORDINALITY")?;
            reject(!partitions.is_empty(), "PARTITION selection")?;
            reject(json_path.is_some(), "JSON paths in FROM")?;
            reject(sample.is_some(), "TABLESAMPLE")?;
            reject(!index_hints.is_empty(), "Index hints")?;
            check_alias(alias.as_ref())
        }
        TableFactor::Derived { lateral, alias, .. } => {
            reject(*lateral, "LATERAL subqueries")?;
            check_alias(alias.as_ref())
        }
        // Everything else is rejected by the FROM planner.
        _ => Ok(()),
    }
}

fn check_alias(alias: Option<&TableAlias>) -> SqawkResult<()> {
    if let Some(TableAlias {
        explicit: _,
        name: _,
        columns,
        at,
    }) = alias
    {
        reject(!columns.is_empty(), "Column aliases on a FROM item")?;
        reject(at.is_some(), "AT on a table alias")?;
    }
    Ok(())
}

/// An ORDER BY term (of a query or a window). `NULLS FIRST / LAST` is
/// honoured; `WITH FILL` is not.
pub(crate) fn check_order_by_expr(ob: &OrderByExpr) -> SqawkResult<()> {
    let OrderByExpr {
        expr: _,
        options: OrderByOptions {
            asc: _,
            nulls_first: _,
        },
        with_fill,
    } = ob;
    reject(with_fill.is_some(), "ORDER BY ... WITH FILL")
}

/// CREATE TABLE: column names and types, `LOCATION`, `STORED AS TEXTFILE`,
/// `WITH (...)` options, or `AS query`. Constraints are not enforced by
/// sqawk, so declaring one is an error rather than a silent no-op.
pub(crate) fn check_create_table(create: &CreateTable) -> SqawkResult<()> {
    reject(create.or_replace, "CREATE OR REPLACE TABLE")?;
    reject(create.if_not_exists, "CREATE TABLE IF NOT EXISTS")?;
    reject(create.external, "CREATE EXTERNAL TABLE")?;
    reject(!create.constraints.is_empty(), "Table constraints")?;
    reject(
        create.columns.iter().any(|c| !c.options.is_empty()),
        "Column constraints (NOT NULL, PRIMARY KEY, DEFAULT, ...)",
    )?;
    reject(create.like.is_some(), "CREATE TABLE ... LIKE")?;
    reject(create.clone.is_some(), "CREATE TABLE ... CLONE")?;
    reject(create.without_rowid, "WITHOUT ROWID")?;
    reject(create.strict, "STRICT tables")?;
    reject(create.primary_key.is_some(), "PRIMARY KEY")?;
    reject(create.order_by.is_some(), "CREATE TABLE ... ORDER BY")?;
    reject(
        create.partition_by.is_some(),
        "CREATE TABLE ... PARTITION BY",
    )?;
    reject(
        create.cluster_by.is_some() || create.clustered_by.is_some(),
        "CREATE TABLE ... CLUSTER BY",
    )?;
    reject(create.inherits.is_some(), "INHERITS")?;
    reject(
        !matches!(create.hive_distribution, HiveDistributionStyle::NONE),
        "PARTITIONED BY / SKEWED BY",
    )?;
    if let Some(HiveFormat {
        row_format,
        serde_properties,
        storage: _,
        location: _,
    }) = &create.hive_formats
    {
        reject(
            row_format.is_some() || serde_properties.is_some(),
            "ROW FORMAT (use WITH (DELIMITER='...'))",
        )?;
    }
    Ok(())
}
