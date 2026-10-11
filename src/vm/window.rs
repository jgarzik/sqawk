//! Window function evaluation: the run-time half of the window stage
//! (`WindowCompute`). See `compiler_window.rs` for the compile-time half.
//!
//! The rows of an ephemeral table are sorted by one window definition's
//! partition and order keys (stably, so ties keep their earlier order), split
//! into partitions, and within each partition into peer groups -- rows equal
//! on every ORDER BY key, or the whole partition when there is no ORDER BY.
//! Every function of the definition is then evaluated for each row and its
//! value stored in the function's result column.

use std::cmp::Ordering;

use super::bytecode::{FrameBound, WindowDef, WindowFrame, WindowFunc, WindowFuncKind};
use super::engine::{compare_values, AggAccumulator};
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
            for &(c, asc) in &def.order {
                let o = compare_values(&a[c], &b[c]);
                if o != Ordering::Equal {
                    return if asc { o } else { o.reverse() };
                }
            }
            Ordering::Equal
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
            while ge < n && same(&part[gs], &part[ge], def.order.iter().map(|o| o.0)) {
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
        WindowFuncKind::Agg(func_type) => {
            // The accumulator covers rows [lo, hi). A frame that keeps its
            // start and does not shrink (the default frame, ROWS UNBOUNDED
            // PRECEDING, the whole partition) only steps the new rows; any
            // other frame is accumulated afresh.
            let mut acc = AggAccumulator::new(func_type);
            let (mut lo, mut hi) = (0, 0);
            for i in 0..n {
                let (s, e) = frame(&f.frame, i, n, p);
                if s != lo || e < hi {
                    acc = AggAccumulator::new(func_type);
                    lo = s;
                    hi = s;
                }
                for row in &part[hi..e] {
                    acc.step(f.args.first().map(|&c| row[c].clone()))?;
                }
                hi = e;
                out.push(acc.finalize());
            }
        }
    }
    Ok(out)
}
