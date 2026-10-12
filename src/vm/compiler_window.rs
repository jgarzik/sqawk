//! The window stage of the SELECT pipeline.
//!
//! A window function sees other rows of the query's result, so it can only
//! be evaluated once every row exists. As in SQLite, the stage runs after
//! FROM / WHERE / aggregation / HAVING:
//!
//! 1. Each finished row goes into an ephemeral table: the per-row values
//!    every window needs (partition keys, order keys, function arguments),
//!    a NULL placeholder per window call, and every value the row's scope
//!    holds -- all FROM columns for a plain query, the aggregates and GROUP
//!    BY keys for a grouped one -- so the output expressions can be evaluated
//!    later.
//! 2. For each distinct window definition (PARTITION BY + ORDER BY), one
//!    `WindowCompute` sorts the table by those keys, walks each partition and
//!    writes every function of that definition into its result column. Each
//!    function has its own state and its own frame.
//! 3. The output stage reads the rows back and evaluates the SELECT list with
//!    `code_expr` in a scope where each window call is bound, by its text, to
//!    the register holding its value (as `agg_bindings` binds aggregates). A
//!    window call may therefore appear inside any expression, and in ORDER BY.
//!
//! A window call anywhere else -- WHERE, GROUP BY, HAVING, an aggregate's or
//! another window's argument -- reaches `code_expr` unbound, which rejects it.

use std::collections::{HashMap, HashSet};

use sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, NamedWindowDefinition,
    NamedWindowExpr, Select, UnaryOperator, Value, ValueWithSpan, WindowFrameBound,
    WindowFrameUnits, WindowSpec, WindowType,
};

use super::ast_compat::{func_args, func_is_distinct, order_by_is_asc};
use super::bytecode::{
    FrameBound, KeyOrder, OpCode, WindowDef, WindowFrame, WindowFunc, WindowFuncKind, AGG_COUNT,
};
use super::compiler::{ColBindings, ColumnRef, NameCtx, OutCol, SqlCompiler};
use super::compiler_check::check_order_by_expr;
use super::compiler_from::expr_children;
use super::compiler_select::OutputStage;
use crate::aggregate::AggregateFunction;
use crate::error::{SqawkError, SqawkResult};

/// A planned window stage. Built by [`SqlCompiler::window_stage`], fed rows
/// by [`SqlCompiler::emit_window_row`], closed by
/// [`SqlCompiler::finish_window`].
pub(crate) struct WindowStage<'a> {
    /// The ephemeral table collecting the rows.
    eph: i64,
    /// Address of its OpenEphemeral, whose column count is known only once
    /// the first row's scope is.
    open_addr: usize,
    /// Per-row expressions (partition keys, order keys, arguments). Until
    /// the first row, the definitions' column numbers index this list.
    exprs: Vec<Expr>,
    /// The expressions that need a column of their own (by index into
    /// `exprs`), stored after the saved scope values. The others are bare
    /// references to a saved value and read its column. Set by the first row.
    own: Vec<usize>,
    /// Window calls; call `j`'s value is ephemeral column `j`.
    calls: Vec<Expr>,
    defs: Vec<WindowDef>,
    /// The pre-window scope with its registers renumbered to `slot_out`:
    /// the scope the output stage evaluates in. Set by the first row.
    template: Option<NameCtx<'a>>,
    /// Number of scope registers saved per row, in the columns after the
    /// call columns.
    nslots: usize,
    /// Register block one row is assembled in.
    row: i64,
    /// Registers the saved scope values are read back into.
    slot_out: i64,
}

/// The argument expressions of a call, and the expressions of its inline
/// window specification: where an aggregate inside a window call lives
/// (`SUM(COUNT(*)) OVER (ORDER BY MAX(x))`).
pub(crate) fn window_call_children(func: &Function) -> Vec<&Expr> {
    let mut v: Vec<&Expr> = func_args(func)
        .iter()
        .filter_map(|a| match a {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
            _ => None,
        })
        .collect();
    if let Some(WindowType::WindowSpec(spec)) = &func.over {
        v.extend(spec.partition_by.iter());
        v.extend(spec.order_by.iter().map(|o| &o.expr));
    }
    v
}

/// Whether `expr` contains a window call (not counting subqueries, whose
/// windows are their own).
pub(crate) fn contains_window(expr: &Expr) -> bool {
    match expr {
        Expr::Function(f) if f.over.is_some() => true,
        _ => expr_children(expr).is_some_and(|c| c.into_iter().any(contains_window)),
    }
}

/// Collect the window calls within `expr`, without duplicates.
fn collect_windows(expr: &Expr, out: &mut Vec<Expr>) {
    match expr {
        Expr::Function(f) if f.over.is_some() => {
            let key = NameCtx::agg_key(expr);
            if !out.iter().any(|e| NameCtx::agg_key(e) == key) {
                out.push(expr.clone());
            }
        }
        _ => {
            for c in expr_children(expr).into_iter().flatten() {
                collect_windows(c, out);
            }
        }
    }
}

/// The registers a scope's values live in: its bound aggregates and keys
/// (grouped) or its materialized columns (plain), in a fixed order.
fn scope_regs(ctx: &NameCtx<'_>) -> Vec<i64> {
    let mut regs: Vec<i64> = Vec::new();
    let mut seen = HashSet::new();
    let bound = ctx.agg_bindings.iter().map(|(_, r)| *r);
    let cols = ctx.col_bindings.iter().flat_map(|b| b.regs());
    for r in bound.chain(cols) {
        if seen.insert(r) {
            regs.push(r);
        }
    }
    regs
}

/// The register holding `expr` in `ctx`, if it is a bare reference to an
/// already-computed value: a materialized column, a GROUP BY key or an
/// aggregate.
fn bound_reg(ctx: &NameCtx<'_>, expr: &Expr) -> Option<i64> {
    if let Some(r) = ctx.lookup_agg(expr) {
        return Some(r);
    }
    let found = match expr {
        Expr::Identifier(i) => ctx.lookup(None, &i.value),
        Expr::CompoundIdentifier(p) if p.len() >= 2 => {
            ctx.lookup(Some(&p[p.len() - 2].value), &p[p.len() - 1].value)
        }
        _ => return None,
    };
    match found {
        Ok((ColumnRef::Register(r), 0)) => Some(r),
        _ => None,
    }
}

/// `ctx` with register `from[i]` renumbered to `to + i`.
fn remap<'a>(ctx: &NameCtx<'a>, from: &[i64], to: i64) -> NameCtx<'a> {
    let mut index = HashMap::with_capacity(from.len());
    for (i, f) in from.iter().enumerate() {
        index.entry(*f).or_insert(i);
    }
    let map = |r: &mut i64| {
        let i = index[r];
        *r = to + i as i64;
    };
    let mut c = ctx.clone();
    c.agg_bindings.iter_mut().for_each(|(_, r)| map(r));
    c.col_bindings
        .iter_mut()
        .flat_map(|b| b.regs_mut())
        .for_each(map);
    c
}

/// A frame offset: a non-negative integer literal.
fn frame_offset(e: &Expr, which: &str) -> SqawkResult<u64> {
    let bad = || {
        SqawkError::InvalidSqlQuery(format!(
            "frame {} offset must be a non-negative integer",
            which
        ))
    };
    match e {
        Expr::Value(ValueWithSpan {
            value: Value::Number(n, _),
            ..
        }) => n.parse::<u64>().map_err(|_| bad()),
        Expr::Nested(inner) => frame_offset(inner, which),
        Expr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        } => frame_offset(expr, which),
        _ => Err(bad()),
    }
}

/// Translate and validate a frame clause.
fn frame_of(spec: &WindowSpec) -> SqawkResult<WindowFrame> {
    let Some(f) = &spec.window_frame else {
        return Ok(WindowFrame::DEFAULT);
    };
    let rows = match f.units {
        WindowFrameUnits::Rows => true,
        WindowFrameUnits::Range => false,
        WindowFrameUnits::Groups => {
            return Err(SqawkError::UnsupportedSqlFeature(
                "GROUPS window frames are not supported".into(),
            ))
        }
    };
    let bound = |b: &WindowFrameBound, which: &str| -> SqawkResult<FrameBound> {
        Ok(match b {
            WindowFrameBound::CurrentRow => FrameBound::CurrentRow,
            WindowFrameBound::Preceding(None) => FrameBound::UnboundedPreceding,
            WindowFrameBound::Following(None) => FrameBound::UnboundedFollowing,
            WindowFrameBound::Preceding(Some(e)) => FrameBound::Preceding(frame_offset(e, which)?),
            WindowFrameBound::Following(Some(e)) => FrameBound::Following(frame_offset(e, which)?),
        })
    };
    let start = bound(&f.start_bound, "starting")?;
    let end = match &f.end_bound {
        Some(b) => bound(b, "ending")?,
        None => FrameBound::CurrentRow,
    };
    // SQLite's rule: the end may not come before the start in the list
    // UNBOUNDED PRECEDING, n PRECEDING, CURRENT ROW, n FOLLOWING,
    // UNBOUNDED FOLLOWING; nor may the start be UNBOUNDED FOLLOWING or the
    // end UNBOUNDED PRECEDING.
    let rank = |b: FrameBound| match b {
        FrameBound::UnboundedPreceding => 0,
        FrameBound::Preceding(_) => 1,
        FrameBound::CurrentRow => 2,
        FrameBound::Following(_) => 3,
        FrameBound::UnboundedFollowing => 4,
    };
    if start == FrameBound::UnboundedFollowing
        || end == FrameBound::UnboundedPreceding
        || rank(end) < rank(start)
    {
        return Err(SqawkError::InvalidSqlQuery(
            "unsupported frame specification".into(),
        ));
    }
    if !rows
        && [start, end]
            .iter()
            .any(|b| matches!(b, FrameBound::Preceding(_) | FrameBound::Following(_)))
    {
        return Err(SqawkError::UnsupportedSqlFeature(
            "RANGE window frames with an offset are not supported (use ROWS)".into(),
        ));
    }
    Ok(WindowFrame { rows, start, end })
}

/// The window specification an OVER clause means, with named windows
/// (`OVER w`, `OVER (w ORDER BY x)`) resolved against the WINDOW clause.
fn resolve_window(over: &WindowType, named: &[NamedWindowDefinition]) -> SqawkResult<WindowSpec> {
    fn lookup(
        name: &str,
        named: &[NamedWindowDefinition],
        depth: usize,
    ) -> SqawkResult<WindowSpec> {
        let def = named
            .iter()
            .find(|d| d.0.value.eq_ignore_ascii_case(name))
            .ok_or_else(|| SqawkError::InvalidSqlQuery(format!("no such window: {}", name)))?;
        if depth > named.len() {
            return Err(SqawkError::InvalidSqlQuery(format!(
                "circular window definition: {}",
                name
            )));
        }
        match &def.1 {
            NamedWindowExpr::NamedWindow(other) => lookup(&other.value, named, depth + 1),
            NamedWindowExpr::WindowSpec(spec) => inherit(spec, named, depth + 1),
        }
    }
    /// A spec naming a base window takes the base's PARTITION BY, and its
    /// ORDER BY unless it has its own; SQLite's override rules.
    fn inherit(
        spec: &WindowSpec,
        named: &[NamedWindowDefinition],
        depth: usize,
    ) -> SqawkResult<WindowSpec> {
        let Some(base_name) = &spec.window_name else {
            return Ok(spec.clone());
        };
        let base = lookup(&base_name.value, named, depth)?;
        let err = |what: &str| {
            Err(SqawkError::InvalidSqlQuery(format!(
                "cannot override {} of window: {}",
                what, base_name.value
            )))
        };
        if !spec.partition_by.is_empty() {
            return err("PARTITION BY clause");
        }
        if !spec.order_by.is_empty() && !base.order_by.is_empty() {
            return err("ORDER BY clause");
        }
        if base.window_frame.is_some() {
            return err("frame specification");
        }
        Ok(WindowSpec {
            window_name: None,
            partition_by: base.partition_by,
            order_by: if spec.order_by.is_empty() {
                base.order_by
            } else {
                spec.order_by.clone()
            },
            window_frame: spec.window_frame.clone(),
        })
    }
    match over {
        WindowType::NamedWindow(name) => lookup(&name.value, named, 0),
        WindowType::WindowSpec(spec) => inherit(spec, named, 0),
    }
}

/// A window function's kind and allowed argument counts.
fn window_kind(func: &Function, nargs: usize) -> SqawkResult<WindowFuncKind> {
    let name = func.name.to_string().to_ascii_uppercase();
    let (kind, min, max) = match name.as_str() {
        "ROW_NUMBER" => (WindowFuncKind::RowNumber, 0, 0),
        "RANK" => (WindowFuncKind::Rank, 0, 0),
        "DENSE_RANK" => (WindowFuncKind::DenseRank, 0, 0),
        "PERCENT_RANK" => (WindowFuncKind::PercentRank, 0, 0),
        "CUME_DIST" => (WindowFuncKind::CumeDist, 0, 0),
        "NTILE" => (WindowFuncKind::Ntile, 1, 1),
        "LAG" => (WindowFuncKind::Lag, 1, 3),
        "LEAD" => (WindowFuncKind::Lead, 1, 3),
        "FIRST_VALUE" => (WindowFuncKind::FirstValue, 1, 1),
        "LAST_VALUE" => (WindowFuncKind::LastValue, 1, 1),
        "NTH_VALUE" => (WindowFuncKind::NthValue, 2, 2),
        // COUNT() and COUNT(*) count rows.
        "COUNT" => (WindowFuncKind::Agg(AGG_COUNT), 0, 1),
        _ => match AggregateFunction::from_name(&name) {
            Some(agg) => {
                let (min, max) = agg.arity(&name);
                (WindowFuncKind::Agg(agg.vm_type()), min, max)
            }
            None => {
                return Err(SqawkError::UnsupportedSqlFeature(format!(
                    "{}() may not be used as a window function",
                    func.name
                )))
            }
        },
    };
    if nargs < min || nargs > max {
        return Err(SqawkError::InvalidSqlQuery(format!(
            "wrong number of arguments to function {}()",
            func.name
        )));
    }
    Ok(kind)
}

/// The argument expressions of a window call. `COUNT(*)` has none.
fn window_args(func: &Function) -> SqawkResult<Vec<Expr>> {
    let name = func.name.to_string();
    if func.filter.is_some() {
        return Err(SqawkError::UnsupportedSqlFeature(format!(
            "FILTER on window function {}() is not supported",
            name
        )));
    }
    if func.null_treatment.is_some() || !func.within_group.is_empty() {
        return Err(SqawkError::UnsupportedSqlFeature(format!(
            "IGNORE/RESPECT NULLS and WITHIN GROUP on {}() are not supported",
            name
        )));
    }
    if func_is_distinct(func) {
        return Err(SqawkError::InvalidSqlQuery(
            "DISTINCT is not supported for window functions".into(),
        ));
    }
    if let FunctionArguments::List(list) = &func.args {
        if !list.clauses.is_empty() {
            return Err(SqawkError::UnsupportedSqlFeature(format!(
                "Clauses inside the argument list of {}() are not supported",
                name
            )));
        }
    }
    let is_count = name.eq_ignore_ascii_case("COUNT");
    let mut out = Vec::new();
    for a in func_args(func) {
        match a {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => out.push(e.clone()),
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) if is_count => {}
            _ => {
                return Err(SqawkError::InvalidSqlQuery(format!(
                    "Unsupported argument to window function {}(): {}",
                    name, a
                )))
            }
        }
    }
    Ok(out)
}

impl<'a> SqlCompiler<'a> {
    /// Plan the window stage for the window calls in `outs` and in the
    /// output stage's hidden sort keys, and open its ephemeral table.
    pub(crate) fn window_stage(
        &mut self,
        select: &Select,
        outs: &[OutCol],
        st: &OutputStage,
    ) -> SqawkResult<WindowStage<'a>> {
        let mut calls = Vec::new();
        for o in outs {
            collect_windows(&o.expr, &mut calls);
        }
        for e in st.key_exprs() {
            collect_windows(e, &mut calls);
        }

        let mut exprs: Vec<Expr> = Vec::new();
        let mut push = |e: &Expr| {
            exprs.push(e.clone());
            exprs.len() - 1
        };
        // Definitions are keyed by the text of their sort keys: functions
        // that share PARTITION BY and ORDER BY share one sort.
        let mut def_keys: Vec<String> = Vec::new();
        let mut defs: Vec<WindowDef> = Vec::new();
        // (definition, function) per call, for the result columns.
        let mut placed: Vec<(usize, usize)> = Vec::new();
        for call in &calls {
            let Expr::Function(func) = call else {
                unreachable!("collect_windows collects calls")
            };
            let args = window_args(func)?;
            let kind = window_kind(func, args.len())?;
            let over = func.over.as_ref().expect("window call");
            let spec = resolve_window(over, &select.named_window)?;
            for ob in &spec.order_by {
                check_order_by_expr(ob)?;
            }
            let frame = frame_of(&spec)?;

            let key = format!(
                "{} | {}",
                spec.partition_by
                    .iter()
                    .map(NameCtx::agg_key)
                    .collect::<Vec<_>>()
                    .join(","),
                spec.order_by
                    .iter()
                    .map(|o| {
                        format!(
                            "{} {} {:?}",
                            NameCtx::agg_key(&o.expr),
                            order_by_is_asc(o),
                            o.options.nulls_first
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(",")
            );
            let d = match def_keys.iter().position(|k| *k == key) {
                Some(d) => d,
                None => {
                    def_keys.push(key);
                    defs.push(WindowDef {
                        partition: spec.partition_by.iter().map(&mut push).collect(),
                        order: spec
                            .order_by
                            .iter()
                            .map(|o| {
                                KeyOrder::new(
                                    push(&o.expr),
                                    order_by_is_asc(o),
                                    o.options.nulls_first,
                                )
                            })
                            .collect(),
                        funcs: Vec::new(),
                    });
                    defs.len() - 1
                }
            };
            let args = args.iter().map(&mut push).collect();
            defs[d].funcs.push(WindowFunc {
                kind,
                args,
                frame,
                result: 0,
            });
            placed.push((d, defs[d].funcs.len() - 1));
        }
        for (j, (d, f)) in placed.into_iter().enumerate() {
            defs[d].funcs[f].result = j;
        }

        let eph = self.alloc_cursor();
        let open_addr = self.program.len();
        self.emit(OpCode::OpenEphemeral, eph, 0, 0, None, "window stage rows");
        Ok(WindowStage {
            eph,
            open_addr,
            exprs,
            own: Vec::new(),
            calls,
            defs,
            template: None,
            nslots: 0,
            row: 0,
            slot_out: 0,
        })
    }

    /// A plain query's row scope with every column -- of every FROM source,
    /// and each USING-merged column -- read into a register, so the window
    /// stage can save the whole row.
    pub(crate) fn materialize_row(&mut self, ctx: &NameCtx<'a>) -> NameCtx<'a> {
        let mut cols = Vec::new();
        for src in &ctx.srcs {
            for col in 0..src.table.column_count() {
                let r = self.allocate_register();
                self.emit(OpCode::Column, src.cursor, col as i64, r, None, "");
                cols.push((
                    ColumnRef::Cursor {
                        cursor: src.cursor,
                        col,
                    },
                    r,
                ));
            }
        }
        for k in 0..ctx.using.len() {
            let col = ColumnRef::Using(ctx.using_base + k);
            let r = self.allocate_register();
            self.emit_column_ref(col, ctx, r, &ctx.using[k].name);
            cols.push((col, r));
        }
        let mut row = ctx.clone();
        row.col_bindings = Some(ColBindings::new(cols));
        row
    }

    /// Add one row to the window stage. `ctx` is the pre-window scope, in
    /// which every value lives in a register: a [`Self::materialize_row`]
    /// scope, or a grouped query's post-aggregation scope. Rows failing
    /// `filter` (a grouped query's HAVING) are skipped.
    pub(crate) fn emit_window_row(
        &mut self,
        ws: &mut WindowStage<'a>,
        ctx: &NameCtx<'a>,
        filter: Option<&Expr>,
    ) -> SqawkResult<()> {
        let skip = self.label();
        if let Some(f) = filter {
            let r = self.code_expr(f, ctx, None)?;
            self.emit_jump_to(OpCode::IfZ, r, skip, 0, None, "HAVING failed");
        }
        let regs = scope_regs(ctx);
        let nc = ws.calls.len();
        match &ws.template {
            None => {
                ws.nslots = regs.len();
                // Lay the expressions out: a bare reference to a saved value
                // reads that value's column, anything else gets its own.
                let mut col_of = Vec::with_capacity(ws.exprs.len());
                for (i, e) in ws.exprs.iter().enumerate() {
                    match bound_reg(ctx, e).and_then(|r| regs.iter().position(|x| *x == r)) {
                        Some(k) => col_of.push(nc + k),
                        None => {
                            col_of.push(nc + ws.nslots + ws.own.len());
                            ws.own.push(i);
                        }
                    }
                }
                for def in &mut ws.defs {
                    def.partition.iter_mut().for_each(|c| *c = col_of[*c]);
                    def.order.iter_mut().for_each(|k| k.col = col_of[k.col]);
                    for f in &mut def.funcs {
                        f.args.iter_mut().for_each(|c| *c = col_of[*c]);
                    }
                }
                let n = nc + ws.nslots + ws.own.len();
                ws.row = self.allocate_registers(n.max(1));
                ws.slot_out = self.allocate_registers(ws.nslots.max(1));
                ws.template = Some(remap(ctx, &regs, ws.slot_out));
                self.program.instructions[ws.open_addr].p2 = n as i64;
            }
            // A grouped query emits rows from two places, with different
            // registers but the same scope shape.
            Some(_) if regs.len() != ws.nslots => {
                return Err(SqawkError::VmError(
                    "window stage: inconsistent row scope".into(),
                ))
            }
            Some(_) => {}
        }
        for j in 0..nc {
            let r = ws.row + j as i64;
            self.emit(OpCode::Null, 0, r, 0, None, "window value: computed later");
        }
        for (k, r) in regs.iter().enumerate() {
            let to = ws.row + (nc + k) as i64;
            self.emit(OpCode::Copy, *r, to, 0, None, "save row value");
        }
        let base = nc + ws.nslots;
        for (m, &i) in ws.own.iter().enumerate() {
            self.code_expr(&ws.exprs[i], ctx, Some(ws.row + (base + m) as i64))?;
        }
        let n = (base + ws.own.len()) as i64;
        self.emit(
            OpCode::IdxInsert,
            ws.eph,
            ws.row,
            n,
            None,
            "window stage row",
        );
        self.resolve(skip);
        Ok(())
    }

    /// Compute every window definition over the collected rows, then send
    /// each row through the output stage.
    pub(crate) fn finish_window(
        &mut self,
        ws: WindowStage<'a>,
        st: &OutputStage,
        outs: &[OutCol],
    ) -> SqawkResult<()> {
        let template = ws
            .template
            .ok_or_else(|| SqawkError::VmError("window stage received no rows".into()))?;
        // The last definition is computed first, so the rows end up in the
        // first definition's order -- the order SQLite returns them in when
        // there is no ORDER BY. Each sort is stable.
        for def in ws.defs.into_iter().rev() {
            let idx = self.program.windows.len() as i64;
            self.program.windows.push(def);
            self.emit(OpCode::WindowCompute, ws.eph, idx, 0, None, "window pass");
        }

        let nc = ws.calls.len();
        let vals = self.allocate_registers(nc.max(1));
        let mut ctx = template;
        for (j, call) in ws.calls.iter().enumerate() {
            ctx.bind_agg(call, vals + j as i64);
        }
        let top = self.label();
        let done = self.label();
        self.emit_jump_to(OpCode::Rewind, ws.eph, done, 0, None, "window rows");
        self.resolve(top);
        for k in 0..ws.nslots {
            let col = (nc + k) as i64;
            self.emit(
                OpCode::Column,
                ws.eph,
                col,
                ws.slot_out + k as i64,
                None,
                "",
            );
        }
        for j in 0..nc {
            self.emit(OpCode::Column, ws.eph, j as i64, vals + j as i64, None, "");
        }
        self.emit_stage_row(st, outs, &ctx, None)?;
        self.emit_jump_to(OpCode::Next, ws.eph, top, 0, None, "");
        self.resolve(done);
        Ok(())
    }
}
