//! Aggregate accumulators: the running state of one aggregate over one
//! group (AggStep / AggFinal) or one window frame (the window pass).

use std::collections::HashSet;

use super::arith::text_of;
use super::bytecode::{
    Register, AGG_AVG, AGG_COUNT, AGG_DISTINCT, AGG_GROUP_CONCAT, AGG_MAX, AGG_MIN, AGG_SUM,
    AGG_TOTAL, AGG_TYPE_MASK,
};
use super::engine::compare_values;
use crate::error::{SqawkError, SqawkResult};
use crate::table::Value;

/// A running sum: exact while every addend is an integer and the total fits.
#[derive(Debug, Clone, Copy)]
enum Sum {
    Int(i64),
    Real(f64),
}

impl Sum {
    fn as_f64(self) -> f64 {
        match self {
            Sum::Int(i) => i as f64,
            Sum::Real(f) => f,
        }
    }
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
    /// MIN / MAX.
    best: Option<Value>,
    /// GROUP_CONCAT.
    text: Option<String>,
    /// Values already accumulated, for DISTINCT aggregates. `None` when the
    /// aggregate is not DISTINCT, so the ordinary path allocates nothing.
    seen: Option<HashSet<String>>,
}

/// SUM / AVG operand: numbers as they are, numeric text as its number.
/// `None` (skipped) for NULL and for text with no numeric reading.
fn sum_operand(v: &Value) -> Option<Sum> {
    match v.as_numeric()? {
        Value::Integer(i) => Some(Sum::Int(i)),
        Value::Float(f) => Some(Sum::Real(f)),
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
            if !matches!(val, Value::Null) && !seen.insert(format!("{:?}", val)) {
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
                self.sum = Some(match (self.sum, v) {
                    (None, v) => v,
                    (Some(Sum::Int(a)), Sum::Int(b)) => match a.checked_add(b) {
                        Some(s) => Sum::Int(s),
                        // SQLite: SUM of integers that overflows is an
                        // error; AVG and TOTAL carry on in floating point.
                        None if self.func_type == AGG_SUM => {
                            return Err(SqawkError::InvalidSqlQuery("integer overflow".into()))
                        }
                        None => Sum::Real(a as f64 + b as f64),
                    },
                    (Some(a), b) => Sum::Real(a.as_f64() + b.as_f64()),
                });
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
    /// rest NULL.
    pub(super) fn finalize(&self) -> Value {
        match self.func_type {
            AGG_COUNT => Value::Integer(self.count),
            AGG_AVG => match self.sum {
                Some(s) if self.count > 0 => Value::Float(s.as_f64() / self.count as f64),
                _ => Value::Null,
            },
            AGG_TOTAL => Value::Float(self.sum.map_or(0.0, Sum::as_f64)),
            AGG_SUM => match self.sum {
                Some(Sum::Int(i)) => Value::Integer(i),
                Some(Sum::Real(f)) => Value::Float(f),
                None => Value::Null,
            },
            AGG_GROUP_CONCAT => match &self.text {
                Some(t) => Value::String(t.clone().into()),
                None => Value::Null,
            },
            _ => self.best.clone().unwrap_or(Value::Null),
        }
    }
}
