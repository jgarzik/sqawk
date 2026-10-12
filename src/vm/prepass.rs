//! The pre-pass run on every statement before it compiles: derived tables
//! and common table expressions become ordinary named tables.
//!
//! The compiler only knows how to open a cursor on a named table. So the
//! pre-pass walks the statement ([`parse::for_each_child`], the same walk
//! the parser's limit check uses, so it reaches every query, FROM item and
//! expression) and, for each FROM item that is
//!
//! - a derived table `(SELECT ...) d`: walks its query, runs it, registers
//!   the result under an internal name (`sqawk_derived_N`) and rewrites the
//!   item to that table with alias `d`;
//! - a one-part name `c` that a CTE in scope declares: materializes the CTE
//!   (once, on its first reference) as `sqawk_cte_N` and rewrites the item
//!   to that table, aliased as written or else as `c`, so `c.col` works and
//!   the internal name never reaches the user;
//! - any other one-part name that some CTE of the statement declares: spells
//!   it `main.name`, the loaded table.
//!
//! The compiler is then given the set of CTE names, and a one-part table
//! name from that set is an internal error there (see
//! `SqlCompiler::cte_names`): a reference this walk missed can never
//! silently read the loaded table of the same name.
//!
//! CTE scoping follows SQLite: a WITH list's CTEs see every sibling (those
//! declared after them too) and themselves; an inner WITH shadows an outer one; a CTE body
//! resolves names in the scope of its WITH, not of its reference; an unused
//! CTE is never evaluated. A CTE whose body references itself directly in a
//! UNION [ALL] arm is recursive (with or without RECURSIVE) and is computed
//! here, one queue row at a time, as SQLite does.
//!
//! Every table registered lives until the statement finishes (the caller
//! drops [`Prepared::registered`]). A derived table or CTE that refers to a
//! column of an enclosing query cannot be computed up front; it fails to
//! resolve ("no such column").

use std::cell::RefCell;
use std::collections::{BinaryHeap, HashSet, VecDeque};
use std::rc::Rc;
use std::sync::Arc;

use sqlparser::ast::{
    Cte, Expr, GroupByExpr, Ident, LimitClause, ObjectName, ObjectNamePart, OrderByKind, Query,
    Select, SelectItem, SetExpr, SetOperator, SetQuantifier, Statement, TableAlias, TableFactor,
    TableObject, Values, With,
};

use super::bytecode::{KeyOrder, Program, Register, ResultSchema};
use super::compiler::SqlCompiler;
use super::compiler_check::check_order_by_expr;
use super::compiler_select::compound_order_key;
use super::compiler_window::contains_window;
use super::engine::{self, RowKey, VmEngine};
use super::parse::{self, for_each_child, Node, SqlSource};
use crate::database::Database;
use crate::error::{SqawkError, SqawkResult};
use crate::table::{ColumnDefinition, Row, Table};

/// The outcome of the pre-pass over one statement.
pub(super) struct Prepared {
    /// The tables registered for the statement, to drop when it finishes.
    pub registered: Vec<String>,
    /// The CTE names (lower case) the statement's compiler must refuse as
    /// one-part table names: those declared in the statement, plus those of
    /// the enclosing statement when this one is a derived table or CTE body.
    pub cte_names: Arc<HashSet<String>>,
}

/// Materialize the derived tables and CTEs of `statement` and rewrite it
/// to reference them by name (see the module documentation). A `WITH ...
/// INSERT / UPDATE / DELETE` is unwrapped to its DML statement.
///
/// `outer` holds the CTE names of the enclosing statement, when this one is
/// a derived table's or CTE's query.
pub(super) fn run(
    statement: &mut Statement,
    database: &mut Database,
    verbose: bool,
    source: &Arc<SqlSource>,
    outer: &Arc<HashSet<String>>,
) -> SqawkResult<Prepared> {
    let mut declared = HashSet::new();
    collect_cte_names(Node::Statement(statement), &mut declared);
    let cte_names = if declared.is_empty() {
        Arc::clone(outer)
    } else {
        Arc::new(declared.iter().chain(outer.iter()).cloned().collect())
    };
    let mut walker = Walker {
        database,
        verbose,
        source: Arc::clone(source),
        declared,
        cte_names: Arc::clone(&cte_names),
        registered: Vec::new(),
        scopes: Vec::new(),
    };
    if let Err(e) = walker.visit(Node::Statement(statement)) {
        for name in &walker.registered {
            walker.database.remove_table(name);
        }
        return Err(e);
    }
    let registered = walker.registered;
    unwrap_with_dml(statement);
    Ok(Prepared {
        registered,
        cte_names,
    })
}

/// `WITH ... INSERT / UPDATE / DELETE` parses as a query whose body is the
/// DML statement; once the WITH is resolved, the statement is the DML.
fn unwrap_with_dml(statement: &mut Statement) {
    let Statement::Query(q) = statement else {
        return;
    };
    if q.with.is_some() || q.order_by.is_some() || q.limit_clause.is_some() {
        return;
    }
    if !matches!(
        *q.body,
        SetExpr::Insert(_) | SetExpr::Update(_) | SetExpr::Delete(_)
    ) {
        return;
    }
    let body = std::mem::replace(&mut q.body, Box::new(empty_body()));
    if let SetExpr::Insert(dml) | SetExpr::Update(dml) | SetExpr::Delete(dml) = *body {
        *statement = dml;
    }
}

fn empty_body() -> SetExpr {
    SetExpr::Values(Values {
        explicit_row: false,
        value_keyword: false,
        rows: Vec::new(),
    })
}

/// Every CTE name declared anywhere in the statement, lower case.
fn collect_cte_names(node: Node<'_>, out: &mut HashSet<String>) {
    if let Node::Query(q) = &node {
        for cte in q.with.iter().flat_map(|w| &w.cte_tables) {
            out.insert(key_of(&cte.alias.name));
        }
    }
    for_each_child(node, &mut |c| collect_cte_names(c, out));
}

/// How names are matched: case-insensitively, as SQL identifiers.
fn key_of(ident: &Ident) -> String {
    ident.value.to_ascii_lowercase()
}

/// The error prefix for a reference to a CTE that is being computed.
const CIRCULAR: &str = "circular reference: ";
/// ... from a subquery of a recursive CTE's recursive arm.
const MULTIPLE_RECURSIVE: &str = "multiple recursive references: ";

#[derive(Clone)]
enum CteState {
    /// Not referenced yet.
    Pending,
    /// Being computed: a reference now is the error `prefix` + name.
    InProgress(&'static str),
    /// Materialized as this registered table.
    Done(String),
}

struct CteDef {
    /// The name as declared.
    name: Ident,
    /// The column list, if any.
    columns: Vec<Ident>,
    /// The body; taken when the CTE is computed.
    query: Option<Box<Query>>,
    state: CteState,
}

/// The CTEs of one WITH list.
struct Scope {
    ctes: Vec<CteDef>,
}

impl Scope {
    fn new(with: With) -> SqawkResult<Self> {
        let mut ctes: Vec<CteDef> = Vec::with_capacity(with.cte_tables.len());
        for cte in with.cte_tables {
            let Cte {
                alias,
                query,
                from,
                materialized: _,
                closing_paren_token: _,
            } = cte;
            if ctes
                .iter()
                .any(|c| c.name.value.eq_ignore_ascii_case(&alias.name.value))
            {
                return Err(SqawkError::InvalidSqlQuery(format!(
                    "duplicate WITH table name: {}",
                    alias.name.value
                )));
            }
            if from.is_some() {
                return Err(SqawkError::UnsupportedSqlFeature(
                    "WITH ... FROM is not supported".into(),
                ));
            }
            if alias.columns.iter().any(|c| c.data_type.is_some()) {
                return Err(SqawkError::UnsupportedSqlFeature(
                    "Typed CTE columns are not supported".into(),
                ));
            }
            ctes.push(CteDef {
                name: alias.name,
                columns: alias.columns.into_iter().map(|c| c.name).collect(),
                query: Some(query),
                state: CteState::Pending,
            });
        }
        Ok(Scope { ctes })
    }
}

struct Walker<'d> {
    database: &'d mut Database,
    verbose: bool,
    source: Arc<SqlSource>,
    /// The CTE names declared in this statement: another one-part table
    /// reference with one of these names is spelled `main.name`.
    declared: HashSet<String>,
    /// What the compilers of this statement's queries refuse (see
    /// [`Prepared::cte_names`]).
    cte_names: Arc<HashSet<String>>,
    registered: Vec<String>,
    /// The WITH lists in scope, innermost last.
    scopes: Vec<Rc<RefCell<Scope>>>,
}

impl Walker<'_> {
    fn visit(&mut self, node: Node<'_>) -> SqawkResult<()> {
        match node {
            Node::Query(q) => self.visit_query(q),
            Node::Factor(f) => self.visit_factor(f),
            Node::Statement(s) => {
                self.qualify_targets(s);
                self.children(Node::Statement(s))
            }
            Node::SetExpr(s) => {
                reject_with_in_compound_operand(s)?;
                self.children(Node::SetExpr(s))
            }
            other => self.children(other),
        }
    }

    fn children(&mut self, node: Node<'_>) -> SqawkResult<()> {
        let mut result = Ok(());
        for_each_child(node, &mut |c| {
            if result.is_ok() {
                result = self.visit(c);
            }
        });
        result
    }

    /// A query's WITH list is in scope for its body, ORDER BY and LIMIT.
    /// It is taken off the query: the compiler never sees it, and an unused
    /// CTE's body is never walked.
    fn visit_query(&mut self, q: &mut Query) -> SqawkResult<()> {
        let Some(with) = q.with.take() else {
            return self.children(Node::Query(q));
        };
        self.scopes.push(Rc::new(RefCell::new(Scope::new(with)?)));
        let result = self.children(Node::Query(q));
        self.scopes.pop();
        result
    }

    /// The table a DML statement or CREATE TABLE ... AS writes is always a
    /// loaded table: spelled `main.name` when a CTE shares its name.
    fn qualify_targets(&self, statement: &mut Statement) {
        match statement {
            Statement::Insert(insert) => {
                if let TableObject::TableName(name) = &mut insert.table {
                    self.qualify(name);
                }
            }
            Statement::Update(update) => {
                if let TableFactor::Table { name, .. } = &mut update.table.relation {
                    self.qualify(name);
                }
            }
            Statement::Delete(delete) => {
                for name in &mut delete.tables {
                    self.qualify(name);
                }
                let (sqlparser::ast::FromTable::WithFromKeyword(from)
                | sqlparser::ast::FromTable::WithoutKeyword(from)) = &mut delete.from;
                for t in from {
                    if let TableFactor::Table { name, .. } = &mut t.relation {
                        self.qualify(name);
                    }
                }
            }
            Statement::CreateTable(create) => self.qualify(&mut create.name),
            _ => {}
        }
    }

    /// Spell a one-part `name` that some CTE of the statement declares as
    /// `main.name`.
    fn qualify(&self, name: &mut ObjectName) {
        if let [part] = name.0.as_slice() {
            if part
                .as_ident()
                .is_some_and(|i| self.declared.contains(&key_of(i)))
            {
                name.0
                    .insert(0, ObjectNamePart::Identifier(Ident::new("main")));
            }
        }
    }

    fn visit_factor(&mut self, factor: &mut TableFactor) -> SqawkResult<()> {
        match factor {
            TableFactor::Table {
                name,
                alias,
                args: None,
                ..
            } => {
                let Some(ident) = one_part(name).cloned() else {
                    return Ok(());
                };
                let key = key_of(&ident);
                match self.lookup(&key) {
                    Some((k, i)) => {
                        let table = self.cte_table(k, i)?;
                        let alias = alias.take().unwrap_or(TableAlias {
                            explicit: false,
                            name: ident,
                            columns: Vec::new(),
                            at: None,
                        });
                        *factor = table_factor(table, Some(alias));
                    }
                    None => self.qualify(name),
                }
                Ok(())
            }
            TableFactor::Derived { lateral: true, .. } => Err(SqawkError::UnsupportedSqlFeature(
                "LATERAL subqueries are not supported".into(),
            )),
            TableFactor::Derived { .. } => self.materialize_derived(factor),
            _ => self.children(Node::Factor(factor)),
        }
    }

    /// The innermost CTE in scope named `key`: (scope index, CTE index).
    fn lookup(&self, key: &str) -> Option<(usize, usize)> {
        self.scopes.iter().enumerate().rev().find_map(|(k, s)| {
            s.borrow()
                .ctes
                .iter()
                .position(|c| key_of(&c.name) == key)
                .map(|i| (k, i))
        })
    }

    /// Run a derived table's query and replace the FROM item with the
    /// registered result, keeping its alias.
    fn materialize_derived(&mut self, factor: &mut TableFactor) -> SqawkResult<()> {
        let TableFactor::Derived {
            subquery, alias, ..
        } = factor
        else {
            return Ok(());
        };
        if alias.as_ref().is_some_and(|a| !a.columns.is_empty()) {
            return Err(SqawkError::UnsupportedSqlFeature(
                "Column aliases on a FROM item are not supported".into(),
            ));
        }
        let alias = alias.take();
        let mut inner = std::mem::replace(subquery, Box::new(empty_query()));
        // Nested derived tables and CTE references first.
        self.visit_query(&mut inner)?;
        let mut table = self.run_query(*inner)?;
        table.dedup_column_names();
        let name = self.register(table, "sqawk_derived_")?;
        *factor = table_factor(name, alias);
        Ok(())
    }

    /// The registered table of the CTE `(k, i)`, computing it on first use.
    fn cte_table(&mut self, k: usize, i: usize) -> SqawkResult<String> {
        let (name, state) = {
            let scope = self.scopes[k].borrow();
            let cte = &scope.ctes[i];
            (cte.name.clone(), cte.state.clone())
        };
        match state {
            CteState::Done(table) => Ok(table),
            CteState::InProgress(prefix) => Err(SqawkError::InvalidSqlQuery(format!(
                "{}{}",
                prefix, name.value
            ))),
            CteState::Pending => {
                let (columns, query) = {
                    let mut scope = self.scopes[k].borrow_mut();
                    let cte = &mut scope.ctes[i];
                    cte.state = CteState::InProgress(CIRCULAR);
                    (
                        cte.columns.clone(),
                        cte.query.take().expect("a pending CTE has its body"),
                    )
                };
                // The body sees the WITH lists up to and including its own,
                // not those around the reference.
                let inner = self.scopes[..=k].to_vec();
                let saved = std::mem::replace(&mut self.scopes, inner);
                let result = self.evaluate_cte(&name, &columns, *query, k, i);
                self.scopes = saved;
                let mut table = result?;
                if !columns.is_empty() {
                    if columns.len() != table.column_count() {
                        return Err(SqawkError::InvalidSqlQuery(format!(
                            "table {} has {} values for {} columns",
                            name.value,
                            table.column_count(),
                            columns.len()
                        )));
                    }
                    let names: Vec<String> = columns.iter().map(|c| c.value.clone()).collect();
                    table.rename_columns(&names);
                }
                table.dedup_column_names();
                let registered = self.register(table, "sqawk_cte_")?;
                self.scopes[k].borrow_mut().ctes[i].state = CteState::Done(registered.clone());
                Ok(registered)
            }
        }
    }

    /// Compute a CTE's rows: run its body, or iterate it when recursive.
    fn evaluate_cte(
        &mut self,
        name: &Ident,
        columns: &[Ident],
        mut query: Query,
        k: usize,
        i: usize,
    ) -> SqawkResult<Table> {
        if matches!(
            *query.body,
            SetExpr::Insert(_) | SetExpr::Update(_) | SetExpr::Delete(_)
        ) {
            return Err(SqawkError::UnsupportedSqlFeature(
                "A data-modifying statement in WITH is not supported".into(),
            ));
        }
        let recursive = recursive_arm_count(&query.body, name)?;
        if recursive == 0 {
            self.visit_query(&mut query)?;
            return self.run_query(query);
        }
        // The body's own WITH list is in scope for every arm.
        let with = query.with.take();
        let pushed = match with {
            Some(with) => {
                self.scopes.push(Rc::new(RefCell::new(Scope::new(with)?)));
                true
            }
            None => false,
        };
        let result = self.evaluate_recursive(name, columns, query, recursive, k, i);
        if pushed {
            self.scopes.pop();
        }
        result
    }

    /// SQLite's recursive CTE: the setup arms' rows seed a queue; each row
    /// taken from it is output (subject to OFFSET and LIMIT) and then is the
    /// whole content of the CTE for one run of every recursive arm, whose
    /// rows join the queue. UNION keeps a row out of the queue once seen.
    /// The queue is FIFO, or ordered by the body's ORDER BY (ties FIFO).
    fn evaluate_recursive(
        &mut self,
        name: &Ident,
        columns: &[Ident],
        mut shell: Query,
        recursive: usize,
        k: usize,
        i: usize,
    ) -> SqawkResult<Table> {
        let body = std::mem::replace(&mut shell.body, Box::new(empty_body()));
        let order_by = shell.order_by.take();
        let limit_clause = shell.limit_clause.take();
        let (mut arms, ops) = flatten_compound(*body);
        let distinct = matches!(
            ops.last(),
            Some((_, SetQuantifier::None | SetQuantifier::Distinct))
        );
        let recursive_arms = arms.split_off(arms.len() - recursive);
        let mut setup = arms.into_iter();
        let mut setup_body = setup.next().expect("a compound has a first arm");
        for ((op, quantifier), arm) in ops.into_iter().zip(setup) {
            setup_body = SetExpr::SetOperation {
                left: Box::new(setup_body),
                op,
                set_quantifier: quantifier,
                right: Box::new(arm),
            };
        }
        let with_body = |body: SetExpr| Query {
            body: Box::new(body),
            ..shell.clone()
        };

        // The setup arms: a reference to the CTE there is circular.
        let mut setup_query = with_body(setup_body);
        self.visit_query(&mut setup_query)?;
        let mut table = self.run_query(setup_query)?;
        let ncols = table.column_count();
        if !columns.is_empty() && columns.len() != ncols {
            return Err(SqawkError::InvalidSqlQuery(format!(
                "table {} has {} values for {} columns",
                name.value,
                ncols,
                columns.len()
            )));
        }

        // The body's ORDER BY names the setup's result columns.
        let mut schema = ResultSchema::new();
        for col in table.column_metadata() {
            schema.add_column(col.name.clone(), col.data_type);
        }
        let mut keys: Vec<KeyOrder> = Vec::new();
        match order_by.map(|ob| ob.kind) {
            None => {}
            Some(OrderByKind::All(_)) => {
                return Err(SqawkError::UnsupportedSqlFeature(
                    "ORDER BY ALL is not supported".into(),
                ))
            }
            Some(OrderByKind::Expressions(exprs)) => {
                for (n, ob) in exprs.iter().enumerate() {
                    check_order_by_expr(ob)?;
                    keys.push(compound_order_key(ob, n, &schema)?);
                }
            }
        }
        let (limit, offset) = self.limits(limit_clause)?;

        // A FIFO queue is breadth-first: it holds the rows of one
        // generation, each followed by its children, in order. So when every
        // recursive arm maps each working row independently (no DISTINCT
        // across rows) a whole generation can be the working table at once,
        // each row tagged with its position: one run per generation instead
        // of one per row, then the children are put back in queue order by
        // (position, arm). A wildcard would expose the tag, so it keeps the
        // row-at-a-time algorithm, as does an ordered queue.
        let by_generation = keys.is_empty()
            && recursive_arms.iter().all(|arm| {
                matches!(arm, SetExpr::Select(s)
                    if s.distinct.is_none()
                        && s.projection.iter().all(|item| matches!(item,
                            SelectItem::UnnamedExpr(_) | SelectItem::ExprWithAlias { .. })))
            });

        // The working table, under the CTE's column names (its column list,
        // else the setup's), plus the position tag in generation mode.
        let mut defs: Vec<ColumnDefinition> = table
            .column_metadata()
            .iter()
            .enumerate()
            .map(|(n, c)| ColumnDefinition {
                name: columns
                    .get(n)
                    .map_or_else(|| c.name.clone(), |i| i.value.clone()),
                data_type: c.data_type,
                decl_type: None,
            })
            .collect();
        if by_generation {
            defs.push(ColumnDefinition {
                name: POSITION.to_string(),
                data_type: crate::table::DataType::Integer,
                decl_type: None,
            });
        }
        let mut work = Table::new_with_schema("work", defs, None, None);
        work.dedup_column_names();
        let work_name = self.register(work, "sqawk_cte_work_")?;

        // Any reference besides the one direct FROM item of each recursive
        // arm is an error from here on.
        self.scopes[k].borrow_mut().ctes[i].state = CteState::InProgress(MULTIPLE_RECURSIVE);
        let op = if distinct { "UNION" } else { "UNION ALL" };
        let width = ncols + usize::from(by_generation);
        let mut programs: Vec<Program> = Vec::with_capacity(recursive_arms.len());
        for mut arm in recursive_arms {
            if let SetExpr::Select(select) = &mut arm {
                let bound = bind_working_table(select, name, &work_name);
                if by_generation {
                    select
                        .projection
                        .push(SelectItem::UnnamedExpr(Expr::CompoundIdentifier(vec![
                            bound,
                            Ident::new(POSITION),
                        ])));
                }
            }
            let mut query = with_body(arm);
            self.visit_query(&mut query)?;
            programs.push(self.compile_recursive_arm(query, width, op)?);
        }

        let setup_rows: Vec<Row> = table.rows().to_vec();
        let mut run = Recursion {
            seen: HashSet::new(),
            distinct,
            limit,
            offset,
            out: Vec::new(),
        };
        if limit != Some(0) {
            if by_generation {
                self.recurse_by_generation(&mut run, setup_rows, &work_name, &programs)?;
            } else {
                self.recurse_by_row(&mut run, setup_rows, keys, &work_name, &programs)?;
            }
        }
        table.clear_rows()?;
        for row in run.out {
            table.add_row(row)?;
        }
        Ok(table)
    }

    /// Run `program` over the database as it stands; its result rows.
    fn run_program(&self, program: &Program) -> SqawkResult<Vec<Row>> {
        let mut vm = VmEngine::new_mut(self.database, false);
        vm.init(program.clone());
        vm.execute()?;
        Ok(vm.take_results())
    }

    /// Make `rows` the content of the working table.
    fn fill_working_table(&mut self, work: &str, rows: Vec<Row>) -> SqawkResult<()> {
        let table = self.database.get_table_mut(work)?;
        table.clear_rows()?;
        for row in rows {
            table.add_row(row)?;
        }
        Ok(())
    }

    /// SQLite's algorithm as written: one queue row at a time.
    fn recurse_by_row(
        &mut self,
        run: &mut Recursion,
        setup: Vec<Row>,
        keys: Vec<KeyOrder>,
        work: &str,
        programs: &[Program],
    ) -> SqawkResult<()> {
        let mut queue = Queue::new(keys);
        for row in setup {
            if run.admit(&row) {
                queue.push(row);
            }
        }
        while let Some(row) = queue.pop() {
            if run.emit(&row) {
                break;
            }
            self.fill_working_table(work, vec![row])?;
            for program in programs {
                for r in self.run_program(program)? {
                    if run.admit(&r) {
                        queue.push(r);
                    }
                }
            }
        }
        Ok(())
    }

    /// The FIFO algorithm one generation at a time (see
    /// `evaluate_recursive`): the same rows in the same order.
    fn recurse_by_generation(
        &mut self,
        run: &mut Recursion,
        setup: Vec<Row>,
        work: &str,
        programs: &[Program],
    ) -> SqawkResult<()> {
        let mut generation: Vec<Row> = setup.into_iter().filter(|r| run.admit(r)).collect();
        while !generation.is_empty() {
            for row in &generation {
                if run.emit(row) {
                    return Ok(());
                }
            }
            let tagged = generation
                .into_iter()
                .enumerate()
                .map(|(n, mut row)| {
                    row.push(crate::table::Value::Integer(n as i64));
                    row
                })
                .collect();
            self.fill_working_table(work, tagged)?;
            let mut children: Vec<(i64, usize, Row)> = Vec::new();
            for (arm, program) in programs.iter().enumerate() {
                for mut r in self.run_program(program)? {
                    let Some(crate::table::Value::Integer(n)) = r.pop() else {
                        return Err(SqawkError::VmError(
                            "internal error: recursive row lost its position".into(),
                        ));
                    };
                    children.push((n, arm, r));
                }
            }
            // Stable: within one parent and arm, the rows keep their order.
            children.sort_by_key(|(n, arm, _)| (*n, *arm));
            generation = children
                .into_iter()
                .map(|(.., r)| r)
                .filter(|r| run.admit(r))
                .collect();
        }
        Ok(())
    }

    /// Compile one recursive arm, after SQLite's checks on it.
    fn compile_recursive_arm(
        &mut self,
        query: Query,
        ncols: usize,
        op: &str,
    ) -> SqawkResult<Program> {
        let mut compiler = SqlCompiler::new(self.database, self.verbose)
            .with_source(Some(Arc::clone(&self.source)), true)
            .with_cte_names(Arc::clone(&self.cte_names));
        if let SetExpr::Select(select) = &*query.body {
            let exprs = || select_items(select);
            if exprs().any(contains_window) {
                return Err(SqawkError::InvalidSqlQuery(
                    "cannot use window functions in recursive queries".into(),
                ));
            }
            let grouped =
                !matches!(&select.group_by, GroupByExpr::Expressions(e, _) if e.is_empty());
            if grouped || select.having.is_some() || exprs().any(|e| compiler.is_aggregate_expr(e))
            {
                return Err(SqawkError::InvalidSqlQuery(
                    "recursive aggregate queries not supported".into(),
                ));
            }
        }
        let program = compiler.compile_statement_program(&Statement::Query(Box::new(query)))?;
        if program.result_schema.columns.len() != ncols {
            return Err(SqawkError::InvalidSqlQuery(format!(
                "SELECTs to the left and right of {} do not have the same number of result columns",
                op
            )));
        }
        Ok(program)
    }

    /// A recursive CTE body's LIMIT and OFFSET, evaluated once: `None` for
    /// no limit (absent or negative), and an offset of at least 0.
    fn limits(&mut self, clause: Option<LimitClause>) -> SqawkResult<(Option<usize>, usize)> {
        let (limit, offset) = match clause {
            None => (None, None),
            Some(LimitClause::LimitOffset {
                limit,
                offset,
                limit_by,
            }) => {
                if !limit_by.is_empty() {
                    return Err(SqawkError::UnsupportedSqlFeature(
                        "LIMIT ... BY is not supported".into(),
                    ));
                }
                (limit, offset.map(|o| o.value))
            }
            Some(LimitClause::OffsetCommaLimit { offset, limit }) => (Some(limit), Some(offset)),
        };
        let limit = match limit {
            Some(e) => {
                let n = self.eval_integer(e, "LIMIT")?;
                usize::try_from(n).ok()
            }
            None => None,
        };
        let offset = match offset {
            Some(e) => usize::try_from(self.eval_integer(e, "OFFSET")?).unwrap_or(0),
            None => 0,
        };
        Ok((limit, offset))
    }

    /// Evaluate a constant expression (it may contain subqueries) as a
    /// LIMIT / OFFSET operand.
    fn eval_integer(&mut self, expr: Expr, what: &str) -> SqawkResult<i64> {
        let mut statement = parse::parse_sql("SELECT NULL")?
            .pop()
            .expect("one statement");
        if let Statement::Query(q) = &mut statement {
            if let SetExpr::Select(select) = q.body.as_mut() {
                select.projection = vec![SelectItem::UnnamedExpr(expr)];
            }
        }
        self.visit(Node::Statement(&mut statement))?;
        let Statement::Query(q) = statement else {
            unreachable!("a SELECT")
        };
        let table = self.run_query(*q)?;
        let value = table
            .rows()
            .first()
            .and_then(|r| r.first())
            .cloned()
            .unwrap_or(crate::table::Value::Null);
        engine::must_be_int(Register::from(value), what)
    }

    /// Run a query (already walked) and return its result.
    fn run_query(&mut self, query: Query) -> SqawkResult<Table> {
        let source = Arc::clone(&self.source);
        let result = super::execute_statements(
            vec![Statement::Query(Box::new(query))],
            self.database,
            self.verbose,
            &source,
            true,
            &self.cte_names,
        )?;
        result.tables.into_iter().next_back().ok_or_else(|| {
            SqawkError::InvalidSqlQuery("Subquery in FROM produced no result".to_string())
        })
    }

    /// Register `table` under a fresh internal name starting `prefix`.
    fn register(&mut self, mut table: Table, prefix: &str) -> SqawkResult<String> {
        let name = (1..)
            .map(|n| format!("{}{}", prefix, n))
            .find(|n| !self.database.has_table(n))
            .expect("an unused name");
        table.set_name(name.clone());
        self.database.add_table(name.clone(), table)?;
        self.registered.push(name.clone());
        Ok(name)
    }
}

/// WITH on a parenthesized compound operand is a syntax error in SQLite;
/// sqawk keeps rejecting it rather than resolve it.
fn reject_with_in_compound_operand(s: &SetExpr) -> SqawkResult<()> {
    if let SetExpr::SetOperation { left, right, .. } = s {
        for operand in [left, right] {
            if matches!(&**operand, SetExpr::Query(q) if q.with.is_some()) {
                return Err(SqawkError::UnsupportedSqlFeature(
                    "ORDER BY, LIMIT or WITH inside a compound SELECT operand is not supported"
                        .into(),
                ));
            }
        }
    }
    Ok(())
}

/// The name of a one-part table reference.
fn one_part(name: &ObjectName) -> Option<&Ident> {
    match name.0.as_slice() {
        [part] => part.as_ident(),
        _ => None,
    }
}

/// A FROM item reading the registered table `name`.
fn table_factor(name: String, alias: Option<TableAlias>) -> TableFactor {
    TableFactor::Table {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(name))]),
        alias,
        args: None,
        with_hints: Vec::new(),
        version: None,
        partitions: Vec::new(),
        with_ordinality: false,
        json_path: None,
        sample: None,
        index_hints: Vec::new(),
    }
}

fn empty_query() -> Query {
    Query {
        with: None,
        body: Box::new(empty_body()),
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks: Vec::new(),
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: Vec::new(),
    }
}

/// UNION and UNION ALL are told apart; other quantifiers are their own op.
fn same_op(a: (&SetOperator, &SetQuantifier), b: (&SetOperator, &SetQuantifier)) -> bool {
    let norm = |q: &SetQuantifier| match q {
        SetQuantifier::None | SetQuantifier::Distinct => SetQuantifier::Distinct,
        other => *other,
    };
    a.0 == b.0 && norm(a.1) == norm(b.1)
}

/// The number of recursive arms of a CTE body, as SQLite counts them: the
/// body must be a UNION [ALL] compound; walking its arms from the right
/// while they are joined by that same operator, each arm whose FROM names
/// the CTE directly is recursive, up to the first that does not. A
/// recursive arm may name it once. 0: not recursive.
fn recursive_arm_count(body: &SetExpr, name: &Ident) -> SqawkResult<usize> {
    let SetExpr::SetOperation {
        op: top_op,
        set_quantifier: top_q,
        ..
    } = body
    else {
        return Ok(0);
    };
    if *top_op != SetOperator::Union
        || !matches!(
            top_q,
            SetQuantifier::None | SetQuantifier::Distinct | SetQuantifier::All
        )
    {
        return Ok(0);
    }
    let key = key_of(name);
    let mut count = 0;
    let mut node = body;
    while let SetExpr::SetOperation {
        op,
        set_quantifier,
        left,
        right,
    } = node
    {
        if !same_op((op, set_quantifier), (top_op, top_q)) {
            break;
        }
        let refs = match &**right {
            SetExpr::Select(select) => direct_references(select, &key),
            _ => 0,
        };
        if refs > 1 {
            return Err(SqawkError::InvalidSqlQuery(format!(
                "multiple references to recursive table: {}",
                name.value
            )));
        }
        if refs == 0 {
            break;
        }
        count += 1;
        node = left;
    }
    Ok(count)
}

/// FROM items of `select` (joins included, not nested) naming `key`.
fn direct_references(select: &Select, key: &str) -> usize {
    select
        .from
        .iter()
        .flat_map(|t| std::iter::once(&t.relation).chain(t.joins.iter().map(|j| &j.relation)))
        .filter(|f| {
            matches!(f, TableFactor::Table { name, args: None, .. }
                if one_part(name).is_some_and(|i| key_of(i) == key))
        })
        .count()
}

/// The working table's position column in generation mode.
const POSITION: &str = "sqawk_position";

/// The output side of a recursive CTE: the rows UNION has seen, LIMIT and
/// OFFSET, and the rows output so far.
struct Recursion {
    seen: HashSet<Vec<RowKey>>,
    distinct: bool,
    limit: Option<usize>,
    offset: usize,
    out: Vec<Row>,
}

impl Recursion {
    /// Whether `row` joins the queue: under UNION, only the first time.
    fn admit(&mut self, row: &Row) -> bool {
        !self.distinct || self.seen.insert(row.iter().map(engine::row_key).collect())
    }

    /// Output a row taken from the queue, unless OFFSET skips it. True when
    /// LIMIT is reached, which ends the recursion.
    fn emit(&mut self, row: &Row) -> bool {
        if self.offset > 0 {
            self.offset -= 1;
            return false;
        }
        self.out.push(row.clone());
        self.limit == Some(self.out.len())
    }
}

/// Point a recursive arm's direct reference to the CTE at the working
/// table, keeping the name it is qualified by, which is returned.
fn bind_working_table(select: &mut Select, name: &Ident, work: &str) -> Ident {
    let key = key_of(name);
    let mut bound = name.clone();
    for t in &mut select.from {
        for factor in
            std::iter::once(&mut t.relation).chain(t.joins.iter_mut().map(|j| &mut j.relation))
        {
            if let TableFactor::Table {
                name: table,
                alias,
                args: None,
                ..
            } = factor
            {
                let Some(ident) = one_part(table).filter(|i| key_of(i) == key).cloned() else {
                    continue;
                };
                let alias = alias.take().unwrap_or(TableAlias {
                    explicit: false,
                    name: ident,
                    columns: Vec::new(),
                    at: None,
                });
                bound = alias.name.clone();
                *factor = table_factor(work.to_string(), Some(alias));
            }
        }
    }
    bound
}

/// The arms of a left-deep compound in order, and the operators between
/// them (`ops[j]` joins the first `j + 1` arms to arm `j + 1`).
fn flatten_compound(body: SetExpr) -> (Vec<SetExpr>, Vec<(SetOperator, SetQuantifier)>) {
    let mut arms = Vec::new();
    let mut ops = Vec::new();
    let mut node = body;
    loop {
        match node {
            SetExpr::SetOperation {
                left,
                op,
                set_quantifier,
                right,
            } => {
                arms.push(*right);
                ops.push((op, set_quantifier));
                node = *left;
            }
            other => {
                arms.push(other);
                break;
            }
        }
    }
    arms.reverse();
    ops.reverse();
    (arms, ops)
}

/// The expressions of a SELECT that decide whether it aggregates or uses
/// windows: its result columns.
fn select_items(select: &Select) -> impl Iterator<Item = &Expr> {
    select.projection.iter().filter_map(|item| match item {
        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => Some(e),
        _ => None,
    })
}

/// The queue of a recursive CTE: FIFO, or ordered by sort keys with ties in
/// arrival order.
enum Queue {
    Fifo(VecDeque<Row>),
    Ordered {
        heap: BinaryHeap<Queued>,
        keys: Rc<[KeyOrder]>,
        seq: u64,
    },
}

impl Queue {
    fn new(keys: Vec<KeyOrder>) -> Self {
        if keys.is_empty() {
            Queue::Fifo(VecDeque::new())
        } else {
            Queue::Ordered {
                heap: BinaryHeap::new(),
                keys: keys.into(),
                seq: 0,
            }
        }
    }

    fn push(&mut self, row: Row) {
        match self {
            Queue::Fifo(q) => q.push_back(row),
            Queue::Ordered { heap, keys, seq } => {
                heap.push(Queued {
                    row,
                    seq: *seq,
                    keys: Rc::clone(keys),
                });
                *seq += 1;
            }
        }
    }

    fn pop(&mut self) -> Option<Row> {
        match self {
            Queue::Fifo(q) => q.pop_front(),
            Queue::Ordered { heap, .. } => heap.pop().map(|q| q.row),
        }
    }
}

struct Queued {
    row: Row,
    seq: u64,
    keys: Rc<[KeyOrder]>,
}

impl Ord for Queued {
    /// Reversed, so the max-heap yields the smallest key, earliest first.
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        engine::compare_rows(&self.keys, &other.row, &self.row).then(other.seq.cmp(&self.seq))
    }
}

impl PartialOrd for Queued {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Queued {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for Queued {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::Value;

    fn database() -> Database {
        let mut db = Database::new();
        let mut emp = Table::new("emp", vec!["id".to_string()], None);
        emp.add_row(vec![Value::Integer(1)]).unwrap();
        db.add_table("emp".to_string(), emp).unwrap();
        db
    }

    fn one(sql: &str) -> Statement {
        parse::parse_sql(sql).unwrap().pop().unwrap()
    }

    fn names(list: &[&str]) -> Arc<HashSet<String>> {
        Arc::new(list.iter().map(|s| s.to_string()).collect())
    }

    /// The compile-time guard: a one-part table name that is a CTE name of
    /// the statement never reads the loaded table of that name.
    #[test]
    fn compiler_refuses_an_unresolved_cte_name() {
        let db = database();
        let compile = |sql: &str| {
            SqlCompiler::new(&db, false)
                .with_cte_names(names(&["emp"]))
                .compile_statement_program(&one(sql))
        };
        let err = compile("SELECT id FROM emp").unwrap_err().to_string();
        assert_eq!(err, "internal error: unresolved CTE emp");
        let err = compile("SELECT (SELECT 1 FROM EMP)")
            .unwrap_err()
            .to_string();
        assert_eq!(err, "internal error: unresolved CTE EMP");
        assert!(compile("SELECT id FROM main.emp").is_ok());
    }

    /// The pre-pass removes every WITH, points each CTE reference at the
    /// CTE's registered table under the CTE's name, and spells the loaded
    /// table `main.emp` wherever a CTE named `emp` is not in scope.
    #[test]
    fn rewrites_references() {
        let mut db = database();
        let source = Arc::new(SqlSource::new(""));
        let mut statement = one("SELECT (SELECT COUNT(*) FROM emp), \
             (WITH emp AS (SELECT 2 AS id) SELECT id FROM emp) FROM emp");
        let prepared = run(&mut statement, &mut db, false, &source, &Arc::default()).unwrap();
        assert_eq!(*prepared.cte_names, *names(&["emp"]));
        assert_eq!(prepared.registered, ["sqawk_cte_1"]);
        assert_eq!(
            statement.to_string(),
            "SELECT (SELECT COUNT(*) FROM main.emp), \
             (SELECT id FROM sqawk_cte_1 emp) FROM main.emp"
        );
    }

    /// `WITH ... DELETE` becomes the DELETE.
    #[test]
    fn unwraps_with_dml() {
        let mut db = database();
        let source = Arc::new(SqlSource::new(""));
        let mut statement =
            one("WITH r AS (SELECT 1 AS id) DELETE FROM emp WHERE id IN (SELECT id FROM r)");
        run(&mut statement, &mut db, false, &source, &Arc::default()).unwrap();
        assert!(matches!(statement, Statement::Delete(_)));
        assert_eq!(
            statement.to_string(),
            "DELETE FROM emp WHERE id IN (SELECT id FROM sqawk_cte_1 r)"
        );
    }
}
