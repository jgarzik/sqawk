//! Window function evaluation: the run-time half of the window stage
//! (`WindowCompute`). See `compiler_window.rs` for the compile-time half.
//!
//! The rows of an ephemeral table are sorted by one window definition's
//! partition and order keys (stably, so ties keep their earlier order), split
//! into partitions, and within each partition into peer groups -- rows equal
//! on every ORDER BY key, or the whole partition when there is no ORDER BY.
//! Every function of the definition is then evaluated for each row and its
//! value stored in the function's result column. An aggregate is evaluated
//! as SQLite evaluates it: rows are added to and removed from one running
//! state as the frame moves, in SQLite's order (see [`aggregate`]).

use std::cmp::Ordering;
use std::collections::VecDeque;

use super::agg::AggAccumulator;
use super::arith::text_of;
use super::bytecode::{FrameBound, WindowDef, WindowFrame, WindowFunc, WindowFuncKind};
use super::bytecode::{Register, AGG_GROUP_CONCAT, AGG_MAX, AGG_MIN, AGG_TYPE_MASK};
use super::engine::{compare_rows, compare_values};
use crate::error::{SqawkError, SqawkResult};
use crate::table::Value;

/// Evaluate window definition `def` over `rows`, leaving them sorted by its
/// keys.
pub(super) fn compute(rows: &mut [Vec<Value>], def: &WindowDef) -> SqawkResult<()> {
    for f in &def.funcs {
        let ranged = [f.frame.start, f.frame.end]
            .iter()
            .any(|b| matches!(b, FrameBound::Preceding(_) | FrameBound::Following(_)));
        if !f.frame.rows && ranged {
            return Err(SqawkError::VmError(
                "RANGE frame with an offset reached the VM".into(),
            ));
        }
    }
    if !def.partition.is_empty() || !def.order.is_empty() {
        rows.sort_by(|a, b| {
            for &c in &def.partition {
                let o = compare_values(&a[c], &b[c]);
                if o != Ordering::Equal {
                    return o;
                }
            }
            compare_rows(&def.order, a, b)
        });
    }
    fn same(a: &[Value], b: &[Value], mut cols: impl Iterator<Item = usize>) -> bool {
        cols.all(|c| compare_values(&a[c], &b[c]) == Ordering::Equal)
    }
    let mut ps = 0;
    while ps < rows.len() {
        let mut pe = ps + 1;
        while pe < rows.len() && same(&rows[ps], &rows[pe], def.partition.iter().copied()) {
            pe += 1;
        }
        let part = &mut rows[ps..pe];
        let n = part.len();

        // Peer groups: rows [start[i], end[i]) are row i's peers.
        let mut start = vec![0; n];
        let mut end = vec![0; n];
        let mut gs = 0;
        while gs < n {
            let mut ge = gs + 1;
            while ge < n && same(&part[gs], &part[ge], def.order.iter().map(|o| o.col)) {
                ge += 1;
            }
            start[gs..ge].fill(gs);
            end[gs..ge].fill(ge);
            gs = ge;
        }
        let peers = Peers { start, end };

        for f in &def.funcs {
            let vals = evaluate(part, f, &peers)?;
            for (row, v) in part.iter_mut().zip(vals) {
                row[f.result] = v;
            }
        }
        ps = pe;
    }
    Ok(())
}

/// Peer-group extents within one partition.
struct Peers {
    start: Vec<usize>,
    end: Vec<usize>,
}

/// Row `i`'s frame within a partition of `n` rows, as `[start, end)`.
fn frame(f: &WindowFrame, i: usize, n: usize, p: &Peers) -> (usize, usize) {
    let k = |x: u64| usize::try_from(x).unwrap_or(usize::MAX);
    let s = match f.start {
        FrameBound::UnboundedPreceding => 0,
        FrameBound::Preceding(x) => i.saturating_sub(k(x)),
        FrameBound::CurrentRow if f.rows => i,
        FrameBound::CurrentRow => p.start[i],
        FrameBound::Following(x) => i.saturating_add(k(x)),
        FrameBound::UnboundedFollowing => n,
    };
    let e = match f.end {
        FrameBound::UnboundedPreceding => 0,
        FrameBound::Preceding(x) => (i + 1).saturating_sub(k(x)),
        FrameBound::CurrentRow if f.rows => i + 1,
        FrameBound::CurrentRow => p.end[i],
        FrameBound::Following(x) => (i + 1).saturating_add(k(x)),
        FrameBound::UnboundedFollowing => n,
    };
    let s = s.min(n);
    (s, e.min(n).max(s))
}

/// A value as a whole number, if it reads as one (`2`, `2.0`, `'2'`).
fn as_whole(v: &Value) -> Option<i64> {
    match v.as_numeric()? {
        Value::Integer(i) => Some(i),
        Value::Float(f) if f.fract() == 0.0 && f.abs() < 9.0e18 => Some(f as i64),
        _ => None,
    }
}

/// Evaluate one function for every row of a partition.
fn evaluate(part: &[Vec<Value>], f: &WindowFunc, p: &Peers) -> SqawkResult<Vec<Value>> {
    let n = part.len();
    let arg = |row: usize, a: usize| part[row][f.args[a]].clone();
    let mut out = Vec::with_capacity(n);
    match f.kind {
        WindowFuncKind::RowNumber => out.extend((1..=n as i64).map(Value::Integer)),
        WindowFuncKind::Rank => {
            out.extend((0..n).map(|i| Value::Integer(p.start[i] as i64 + 1)));
        }
        WindowFuncKind::DenseRank => {
            let mut rank = 0;
            for i in 0..n {
                if p.start[i] == i {
                    rank += 1;
                }
                out.push(Value::Integer(rank));
            }
        }
        WindowFuncKind::PercentRank => {
            for i in 0..n {
                out.push(Value::Float(if n > 1 {
                    p.start[i] as f64 / (n - 1) as f64
                } else {
                    0.0
                }));
            }
        }
        WindowFuncKind::CumeDist => {
            out.extend((0..n).map(|i| Value::Float(p.end[i] as f64 / n as f64)));
        }
        WindowFuncKind::Ntile => {
            // The bucket count, from the partition's first row; SQLite
            // truncates a fractional count.
            let buckets = match arg(0, 0).as_numeric() {
                Some(Value::Integer(b)) if b > 0 => b as usize,
                Some(Value::Float(b)) if b >= 1.0 => b as usize,
                _ => {
                    return Err(SqawkError::InvalidSqlQuery(
                        "argument of ntile must be a positive integer".into(),
                    ))
                }
            };
            // The first `n % buckets` buckets hold one row more.
            let size = n / buckets;
            let extra = n % buckets;
            let big = extra * (size + 1);
            for i in 0..n {
                let b = if i < big {
                    i / (size + 1)
                } else {
                    extra + (i - big) / size
                };
                out.push(Value::Integer(b as i64 + 1));
            }
        }
        WindowFuncKind::Lag | WindowFuncKind::Lead => {
            for i in 0..n {
                // A NULL or fractional offset gives NULL; non-numeric text
                // is offset 0 (both as SQLite).
                let off = match f.args.get(1).map(|_| arg(i, 1)) {
                    None => Some(1),
                    Some(Value::Null) => None,
                    Some(v) => match v.as_numeric() {
                        None => Some(0),
                        Some(_) => as_whole(&v),
                    },
                };
                let Some(off) = off else {
                    out.push(Value::Null);
                    continue;
                };
                let off = if f.kind == WindowFuncKind::Lag {
                    off.checked_neg()
                } else {
                    Some(off)
                };
                let target = off.and_then(|o| (i as i64).checked_add(o));
                out.push(match target {
                    Some(t) if t >= 0 && (t as usize) < n => arg(t as usize, 0),
                    _ if f.args.len() > 2 => arg(i, 2),
                    _ => Value::Null,
                });
            }
        }
        WindowFuncKind::FirstValue | WindowFuncKind::LastValue | WindowFuncKind::NthValue => {
            for i in 0..n {
                let (s, e) = frame(&f.frame, i, n, p);
                let pos = match f.kind {
                    WindowFuncKind::FirstValue => Some(s),
                    WindowFuncKind::LastValue => e.checked_sub(1),
                    _ => match as_whole(&arg(i, 1)) {
                        Some(k) if k > 0 => s.checked_add(k as usize - 1),
                        _ => {
                            return Err(SqawkError::InvalidSqlQuery(
                                "second argument to nth_value must be a positive integer".into(),
                            ))
                        }
                    },
                };
                out.push(match pos {
                    Some(r) if r >= s && r < e => arg(r, 0),
                    _ => Value::Null,
                });
            }
        }
        WindowFuncKind::Agg(func_type) => return aggregate(part, f, func_type, p),
    }
    Ok(out)
}

/// An aggregate's running state over a moving frame.
enum FrameAcc {
    /// COUNT, SUM, AVG, TOTAL, and MIN / MAX over a frame that starts at
    /// UNBOUNDED PRECEDING (which never removes a row).
    Std(AggAccumulator),
    /// MIN / MAX over a frame whose start moves: the frame's values, the
    /// ones that can still become the result, oldest first (SQLite keeps an
    /// ordered index of them). Of equal values the newest wins, as in
    /// SQLite's index, whose last entry among equals is the newest.
    MinMax {
        min: bool,
        live: VecDeque<(usize, Value)>,
        next: usize,
        removed: usize,
    },
    GroupConcat(GroupConcat),
}

impl FrameAcc {
    fn new(func_type: i64, moving_start: bool) -> FrameAcc {
        match func_type & AGG_TYPE_MASK {
            t @ (AGG_MIN | AGG_MAX) if moving_start => FrameAcc::MinMax {
                min: t == AGG_MIN,
                live: VecDeque::new(),
                next: 0,
                removed: 0,
            },
            AGG_GROUP_CONCAT => FrameAcc::GroupConcat(GroupConcat::default()),
            _ => FrameAcc::Std(AggAccumulator::new(func_type)),
        }
    }

    fn step(&mut self, value: Option<Value>, sep: Option<Option<Value>>) -> SqawkResult<()> {
        match self {
            FrameAcc::Std(acc) => acc.step(value, sep.flatten()),
            FrameAcc::MinMax {
                min, live, next, ..
            } => {
                let id = *next;
                *next += 1;
                let Some(v) = value.filter(|v| !matches!(v, Value::Null)) else {
                    return Ok(());
                };
                // A value no better than the new one can never be the
                // result again: the new one is in every frame it is in.
                let worse = if *min {
                    Ordering::Less
                } else {
                    Ordering::Greater
                };
                while live
                    .back()
                    .is_some_and(|(_, b)| compare_values(b, &v) != worse)
                {
                    live.pop_back();
                }
                live.push_back((id, v));
                Ok(())
            }
            FrameAcc::GroupConcat(g) => {
                g.step(value, sep);
                Ok(())
            }
        }
    }

    /// Remove the oldest value still in the frame.
    fn inverse(&mut self, value: Option<Value>) {
        match self {
            FrameAcc::Std(acc) => acc.inverse(value),
            FrameAcc::MinMax { live, removed, .. } => {
                if live.front().is_some_and(|(id, _)| id == removed) {
                    live.pop_front();
                }
                *removed += 1;
            }
            FrameAcc::GroupConcat(g) => g.inverse(value),
        }
    }

    fn value(&self) -> SqawkResult<Value> {
        match self {
            FrameAcc::Std(acc) => acc.finalize(),
            FrameAcc::MinMax { live, .. } => {
                Ok(live.front().map_or(Value::Null, |(_, v)| v.clone()))
            }
            FrameAcc::GroupConcat(g) => Ok(g.value()),
        }
    }
}

/// GROUP_CONCAT over a moving frame, as SQLite's `groupConcatStep` /
/// `groupConcatInverse` / `groupConcatValue` keep it: one byte string; a
/// row leaving the frame removes its value's bytes and the separator after
/// it from the front (the separator's length tracked per element once the
/// lengths vary). An emptied string starts over (the next value is a
/// first term, with no separator) and its value is NULL.
#[derive(Default)]
struct GroupConcat {
    buf: Vec<u8>,
    /// Bytes of `buf` already removed from the front.
    head: usize,
    /// Values presently concatenated.
    n_accum: i64,
    first_sep_len: usize,
    /// The separators' lengths, oldest first, once they have varied.
    sep_lens: Option<VecDeque<usize>>,
    /// Something has been accumulated since the string was last empty.
    active: bool,
}

impl GroupConcat {
    fn text(v: &Value) -> Vec<u8> {
        text_of(&Register::from(v.clone())).into_bytes()
    }

    /// `sep`: `None` for GROUP_CONCAT(x), `Some(separator)` with two
    /// arguments.
    fn step(&mut self, value: Option<Value>, sep: Option<Option<Value>>) {
        let Some(v) = value.filter(|v| !matches!(v, Value::Null)) else {
            return;
        };
        let first = !self.active;
        self.active = true;
        match sep {
            None => {
                if first {
                    self.first_sep_len = 1;
                } else {
                    self.buf.push(b',');
                }
            }
            Some(sep) => {
                let sep = sep
                    .filter(|s| !matches!(s, Value::Null))
                    .map(|s| Self::text(&s));
                let n_sep = sep.as_ref().map_or(0, Vec::len);
                if first {
                    self.first_sep_len = n_sep;
                } else {
                    if let Some(s) = &sep {
                        self.buf.extend_from_slice(s);
                    }
                    if n_sep != self.first_sep_len || self.sep_lens.is_some() {
                        let in_use = usize::try_from(self.n_accum - 1).unwrap_or(0);
                        let first_len = self.first_sep_len;
                        let lens = self.sep_lens.get_or_insert_with(|| {
                            std::iter::repeat_n(first_len, in_use).collect()
                        });
                        lens.resize(in_use, first_len);
                        lens.push_back(n_sep);
                    }
                }
            }
        }
        self.n_accum += 1;
        self.buf.extend_from_slice(&Self::text(&v));
    }

    fn inverse(&mut self, value: Option<Value>) {
        let Some(v) = value.filter(|v| !matches!(v, Value::Null)) else {
            return;
        };
        let mut n = Self::text(&v).len();
        self.n_accum -= 1;
        match self.sep_lens.as_mut() {
            Some(lens) => {
                if self.n_accum > 0 {
                    n += lens.pop_front().unwrap_or(0);
                }
            }
            None => n += self.first_sep_len,
        }
        let len = self.buf.len() - self.head;
        if n >= len {
            self.buf.clear();
            self.head = 0;
        } else {
            self.head += n;
            // Drop the dead prefix once it dominates, keeping removal
            // amortized O(1) per byte.
            if self.head > 4096 && self.head * 2 > self.buf.len() {
                self.buf.drain(..self.head);
                self.head = 0;
            }
        }
        if self.buf.len() == self.head {
            self.active = false;
            self.sep_lens = None;
        }
    }

    fn value(&self) -> Value {
        let bytes = &self.buf[self.head..];
        if bytes.is_empty() {
            Value::Null
        } else {
            Value::String(String::from_utf8_lossy(bytes).into_owned().into())
        }
    }
}

/// A frame bound's offset as SQLite's countdown registers hold it.
fn offset(b: FrameBound) -> Option<i64> {
    match b {
        FrameBound::Preceding(x) | FrameBound::Following(x) => {
            Some(i64::try_from(x).unwrap_or(i64::MAX))
        }
        _ => None,
    }
}

/// SQLite's `OP_IfPos` countdown: `None` (no countdown) always runs;
/// otherwise a positive count is decremented and the operation skipped.
fn countdown(c: &mut Option<i64>) -> bool {
    match c {
        Some(x) if *x > 0 => {
            *x -= 1;
            false
        }
        _ => true,
    }
}

/// An aggregate window function over one partition, computed as SQLite's
/// `sqlite3WindowCodeStep` computes it: three cursors over the rows -- the
/// next row to add to the frame (step), the next to remove from it
/// (inverse) and the next to return -- advanced in SQLite's order for the
/// frame's shape, so each row's value comes from the same sequence of
/// steps and inverses as in SQLite (which decides rounding, and whether a
/// SUM is still an integer). Every row is added and removed at most once:
/// O(n) for any frame. A RANGE frame (bounds CURRENT ROW or UNBOUNDED)
/// moves by peer groups.
fn aggregate(
    part: &[Vec<Value>],
    f: &WindowFunc,
    func_type: i64,
    peers: &Peers,
) -> SqawkResult<Vec<Value>> {
    let n = part.len();
    let mut out = vec![Value::Null; n];
    if n == 0 {
        return Ok(out);
    }
    let fr = &f.frame;
    // Units: rows for ROWS, peer groups for RANGE.
    let units: Vec<(usize, usize)> = if fr.rows {
        (0..n).map(|i| (i, i + 1)).collect()
    } else {
        let mut u = Vec::new();
        let mut i = 0;
        while i < n {
            u.push((i, peers.end[i]));
            i = peers.end[i];
        }
        u
    };
    let nu = units.len();
    let unbounded_start = fr.start == FrameBound::UnboundedPreceding;
    let is_following = |b: FrameBound| matches!(b, FrameBound::Following(_));
    let is_preceding = |b: FrameBound| matches!(b, FrameBound::Preceding(_));
    let end_unbounded = fr.end == FrameBound::UnboundedFollowing;

    let mut reg_start = offset(fr.start);
    let mut reg_end = offset(fr.end);
    // ROWS BETWEEN a FOLLOWING AND b FOLLOWING with b < a (or a PRECEDING
    // AND b PRECEDING with b > a): every frame is empty.
    if fr.rows {
        if let (Some(s), Some(e)) = (reg_start, reg_end) {
            let empty = (is_following(fr.start) && is_following(fr.end) && e < s)
                || (is_preceding(fr.start) && is_preceding(fr.end) && e > s);
            if empty {
                let v = FrameAcc::new(func_type, !unbounded_start).value()?;
                out.fill(v);
                return Ok(out);
            }
        }
    }
    if is_following(fr.start) && is_following(fr.end) {
        reg_start = reg_end.zip(reg_start).map(|(e, s)| e - s);
    }

    let mut sim = Sim {
        part,
        f,
        units: &units,
        acc: FrameAcc::new(func_type, !unbounded_start),
        removes: !unbounded_start,
        out: &mut out,
        end_c: 0,
        cur_c: 0,
        start_c: 0,
    };

    // The loop over input units: unit k has just been added to the table.
    for _k in 1..nu {
        if is_following(fr.start) {
            sim.step()?;
            if !end_unbounded {
                if countdown(&mut reg_end) {
                    sim.ret()?;
                }
                if countdown(&mut reg_start) {
                    sim.inverse();
                }
            }
        } else if is_preceding(fr.end) {
            if countdown(&mut reg_end) {
                sim.step()?;
            }
            sim.ret()?;
            if countdown(&mut reg_start) {
                sim.inverse();
            }
        } else {
            sim.step()?;
            if !end_unbounded {
                if matches!(reg_end, Some(x) if x > 0) {
                    reg_end = reg_end.map(|x| x - 1);
                } else {
                    sim.ret()?;
                    if countdown(&mut reg_start) {
                        sim.inverse();
                    }
                }
            }
        }
    }

    // Flush: the partition is complete.
    if is_preceding(fr.end) {
        if countdown(&mut reg_end) {
            sim.step()?;
        }
        sim.ret()?;
    } else if is_following(fr.start) {
        sim.step()?;
        let mut tail = false;
        loop {
            if end_unbounded {
                if countdown(&mut reg_start) {
                    sim.ret()?;
                    if sim.cur_c >= nu {
                        break;
                    }
                }
                sim.inverse();
                if sim.start_c >= nu {
                    tail = true;
                    break;
                }
            } else {
                if countdown(&mut reg_end) {
                    sim.ret()?;
                    if sim.cur_c >= nu {
                        break;
                    }
                }
                if countdown(&mut reg_start) {
                    sim.inverse();
                    if sim.start_c >= nu {
                        tail = true;
                        break;
                    }
                }
            }
        }
        if tail {
            while sim.cur_c < nu {
                sim.ret()?;
            }
        }
    } else {
        sim.step()?;
        loop {
            sim.ret()?;
            if sim.cur_c >= nu {
                break;
            }
            if countdown(&mut reg_start) {
                sim.inverse();
            }
        }
    }
    Ok(out)
}

/// The three cursors of [`aggregate`] over one partition's units (rows, or
/// peer groups).
struct Sim<'p> {
    part: &'p [Vec<Value>],
    f: &'p WindowFunc,
    units: &'p [(usize, usize)],
    acc: FrameAcc,
    /// The frame's start moves (it is not UNBOUNDED PRECEDING).
    removes: bool,
    out: &'p mut [Value],
    /// Next unit to add to the frame, to return, to remove from it.
    end_c: usize,
    cur_c: usize,
    start_c: usize,
}

impl Sim<'_> {
    fn arg(&self, row: usize) -> Option<Value> {
        self.f.args.first().map(|&c| self.part[row][c].clone())
    }

    fn step(&mut self) -> SqawkResult<()> {
        if let Some(&(a, b)) = self.units.get(self.end_c) {
            for row in a..b {
                let sep = self.f.args.get(1).map(|&c| Some(self.part[row][c].clone()));
                self.acc.step(self.arg(row), sep)?;
            }
            self.end_c += 1;
        }
        Ok(())
    }

    fn inverse(&mut self) {
        if !self.removes {
            return;
        }
        if let Some(&(a, b)) = self.units.get(self.start_c) {
            for row in a..b {
                self.acc.inverse(self.arg(row));
            }
            self.start_c += 1;
        }
    }

    fn ret(&mut self) -> SqawkResult<()> {
        if let Some(&(a, b)) = self.units.get(self.cur_c) {
            let v = self.acc.value()?;
            self.out[a..b].fill(v);
            self.cur_c += 1;
        }
        Ok(())
    }
}
