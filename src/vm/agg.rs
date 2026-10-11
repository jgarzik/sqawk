//! Aggregate accumulators: the running state of one aggregate over one
//! group (AggStep / AggFinal) or one window frame (the window pass).

use std::collections::HashSet;

use super::arith::{self, text_of};
use super::bytecode::{
    Register, AGG_AVG, AGG_COUNT, AGG_DISTINCT, AGG_GROUP_CONCAT, AGG_MAX, AGG_MIN, AGG_SUM,
    AGG_TOTAL, AGG_TYPE_MASK,
};
use super::engine::{compare_values, row_key, RowKey};
use crate::error::{SqawkError, SqawkResult};
use crate::table::Value;

/// A running sum: exact while every addend is an integer and the total fits,
/// else a Kahan-Babuska-Neumaier compensated sum (as SQLite's SUM, TOTAL
/// and AVG), so `0.1 + 0.2 + 0.3` is 0.6.
#[derive(Debug, Clone, Copy)]
enum Sum {
    Int(i64),
    /// Running sum and the accumulated rounding error.
    Real {
        sum: f64,
        err: f64,
    },
}

/// Integers at or beyond 2^52 in magnitude are split before being added in
/// floating point, so their low bits survive in the error term (SQLite's
/// `kahanBabuskaNeumaierStepInt64`).
const SPLIT_INT: i64 = 4_503_599_627_370_496;

impl Sum {
    /// The compensated sum of an integer total, ready for real addends.
    fn from_int(i: i64) -> Sum {
        if i <= -SPLIT_INT || i >= SPLIT_INT {
            let small = i % 16384;
            Sum::Real {
                sum: (i - small) as f64,
                err: small as f64,
            }
        } else {
            Sum::Real {
                sum: i as f64,
                err: 0.0,
            }
        }
    }

    /// Add a real (Neumaier's step). Only called on `Sum::Real`.
    fn add_real(&mut self, r: f64) {
        if let Sum::Real { sum, err } = self {
            let t = *sum + r;
            if sum.abs() > r.abs() {
                *err += (*sum - t) + r;
            } else {
                *err += (r - t) + *sum;
            }
            *sum = t;
        }
    }

    /// Add an integer to a compensated sum.
    fn add_int(&mut self, i: i64) {
        if i <= -SPLIT_INT || i >= SPLIT_INT {
            let small = i % 16384;
            self.add_real((i - small) as f64);
            self.add_real(small as f64);
        } else {
            self.add_real(i as f64);
        }
    }

    fn as_f64(self) -> f64 {
        match self {
            Sum::Int(i) => i as f64,
            // An infinite or NaN error term is dropped, as SQLite drops it.
            Sum::Real { sum, err } if err.is_finite() => sum + err,
            Sum::Real { sum, .. } => sum,
        }
    }
}

/// SUM / AVG operand.
enum Addend {
    Int(i64),
    Real(f64),
}

/// An aggregate's running state.
#[derive(Debug, Clone)]
pub(super) struct AggAccumulator {
    /// Function type (`AGG_*`, without flags).
    func_type: i64,
    /// Count of values accumulated (COUNT, and AVG's divisor).
    count: i64,
    /// SUM / AVG / TOTAL.
    sum: Option<Sum>,
    /// SUM: the integer sum overflowed and no real has been added since
    /// (SQLite reports "integer overflow" only if that is still so at the
    /// end).
    overflow: bool,
    /// MIN / MAX.
    best: Option<Value>,
    /// GROUP_CONCAT.
    text: Option<String>,
    /// Values already accumulated, for DISTINCT aggregates, keyed as SELECT
    /// DISTINCT keys them (1 and 1.0 are one value; text '1' another). `None`
    /// when the aggregate is not DISTINCT, so the ordinary path allocates
    /// nothing.
    seen: Option<HashSet<RowKey>>,
}

/// SUM / AVG operand: numbers as they are, numeric text as its number.
/// `None` (skipped) for NULL and for text with no numeric reading.
fn sum_operand(v: &Value) -> Option<Addend> {
    match v.as_numeric()? {
        Value::Integer(i) => Some(Addend::Int(i)),
        Value::Float(f) => Some(Addend::Real(f)),
        _ => None,
    }
}

impl AggAccumulator {
    /// A fresh accumulator for function-type word `func_type` (which may
    /// carry flags).
    pub(super) fn new(func_type: i64) -> Self {
        AggAccumulator {
            func_type: func_type & AGG_TYPE_MASK,
            count: 0,
            sum: None,
            overflow: false,
            best: None,
            text: None,
            seen: (func_type & AGG_DISTINCT != 0).then(Default::default),
        }
    }

    /// Accumulate one value; `None` is COUNT(*)'s "a row". `sep` is
    /// GROUP_CONCAT's separator for this row, if it has one.
    pub(super) fn step(&mut self, value: Option<Value>, sep: Option<Value>) -> SqawkResult<()> {
        // DISTINCT: ignore a value already accumulated. NULLs are not
        // tracked -- every aggregate already skips them.
        if let (Some(seen), Some(val)) = (self.seen.as_mut(), value.as_ref()) {
            if !matches!(val, Value::Null) && !seen.insert(row_key(val)) {
                return Ok(());
            }
        }
        match self.func_type {
            AGG_COUNT => {
                if !matches!(value, Some(Value::Null)) {
                    self.count += 1;
                }
            }
            AGG_SUM | AGG_AVG | AGG_TOTAL => {
                let Some(v) = value.as_ref().and_then(sum_operand) else {
                    return Ok(());
                };
                self.count += 1;
                // SQLite's sumStep.
                let sum = self.sum.get_or_insert(Sum::Int(0));
                match (*sum, v) {
                    (Sum::Int(a), Addend::Int(b)) => match a.checked_add(b) {
                        Some(s) => *sum = Sum::Int(s),
                        None => {
                            self.overflow = true;
                            *sum = Sum::from_int(a);
                            sum.add_int(b);
                        }
                    },
                    (Sum::Int(a), Addend::Real(r)) => {
                        *sum = Sum::from_int(a);
                        sum.add_real(r);
                    }
                    (Sum::Real { .. }, Addend::Int(b)) => sum.add_int(b),
                    (Sum::Real { .. }, Addend::Real(r)) => {
                        self.overflow = false;
                        sum.add_real(r);
                    }
                }
            }
            AGG_MIN | AGG_MAX => {
                if let Some(val) = value.filter(|v| !matches!(v, Value::Null)) {
                    self.count += 1;
                    let want = if self.func_type == AGG_MIN {
                        std::cmp::Ordering::Less
                    } else {
                        std::cmp::Ordering::Greater
                    };
                    let replace = match &self.best {
                        None => true,
                        Some(existing) => compare_values(&val, existing) == want,
                    };
                    if replace {
                        self.best = Some(val);
                    }
                }
            }
            AGG_GROUP_CONCAT => {
                let Some(val) = value.filter(|v| !matches!(v, Value::Null)) else {
                    return Ok(());
                };
                let piece = text_of(&Register::from(val));
                match &mut self.text {
                    None => self.text = Some(piece),
                    Some(t) => {
                        match sep {
                            None => t.push(','),
                            Some(Value::Null) => {}
                            Some(s) => t.push_str(&text_of(&Register::from(s))),
                        }
                        t.push_str(&piece);
                    }
                }
            }
            other => {
                return Err(SqawkError::VmError(format!(
                    "Unknown aggregate function type: {}",
                    other
                )))
            }
        }
        Ok(())
    }

    /// The aggregate's value. Of no rows, COUNT is 0, TOTAL 0.0 and the
    /// rest NULL. A SUM of integers that overflowed (with no real added
    /// after the overflow) is an "integer overflow" error, as in SQLite;
    /// AVG and TOTAL carry on in floating point.
    pub(super) fn finalize(&self) -> SqawkResult<Value> {
        if self.func_type == AGG_SUM && self.overflow {
            return Err(arith::overflow());
        }
        Ok(match self.func_type {
            AGG_COUNT => Value::Integer(self.count),
            AGG_AVG => match self.sum {
                Some(s) if self.count > 0 => Value::Float(s.as_f64() / self.count as f64),
                _ => Value::Null,
            },
            AGG_TOTAL => Value::Float(self.sum.map_or(0.0, Sum::as_f64)),
            AGG_SUM => match self.sum {
                Some(Sum::Int(i)) => Value::Integer(i),
                Some(s) => Value::Float(s.as_f64()),
                None => Value::Null,
            },
            AGG_GROUP_CONCAT => match &self.text {
                Some(t) => Value::String(t.clone().into()),
                None => Value::Null,
            },
            _ => self.best.clone().unwrap_or(Value::Null),
        })
    }
}
