//! Aggregate function names.
//!
//! Aggregates are compiled by `vm::compiler_aggregate` and evaluated by the
//! VM's `AggStep` / `AggFinal` opcodes; this module only recognizes the names
//! and maps them to their VM function type.

/// Supported aggregate functions
#[derive(Debug, Clone, Copy)]
pub enum AggregateFunction {
    /// COUNT function - counts the number of rows
    Count,
    /// SUM function - sums numeric values in a column
    Sum,
    /// AVG function - calculates the average of numeric values in a column
    Avg,
    /// MIN function - finds the minimum value in a column
    Min,
    /// MAX function - finds the maximum value in a column
    Max,
    /// TOTAL function - SUM as a real, 0.0 over no rows
    Total,
    /// GROUP_CONCAT / STRING_AGG - values joined by a separator
    GroupConcat,
}

impl AggregateFunction {
    /// Convert a function name string to an AggregateFunction enum
    ///
    /// # Arguments
    /// * `name` - The function name (case-insensitive)
    ///
    /// # Returns
    /// * Some(AggregateFunction) if the name is a valid aggregate function, None otherwise
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_uppercase().as_str() {
            "COUNT" => Some(AggregateFunction::Count),
            "SUM" => Some(AggregateFunction::Sum),
            "AVG" => Some(AggregateFunction::Avg),
            "MIN" => Some(AggregateFunction::Min),
            "MAX" => Some(AggregateFunction::Max),
            "TOTAL" => Some(AggregateFunction::Total),
            "GROUP_CONCAT" | "STRING_AGG" => Some(AggregateFunction::GroupConcat),
            _ => None,
        }
    }

    /// The AggStep/AggFinal function-type constant for this aggregate.
    pub fn vm_type(self) -> i64 {
        use crate::vm::bytecode::{
            AGG_AVG, AGG_COUNT, AGG_GROUP_CONCAT, AGG_MAX, AGG_MIN, AGG_SUM, AGG_TOTAL,
        };
        match self {
            AggregateFunction::Count => AGG_COUNT,
            AggregateFunction::Sum => AGG_SUM,
            AggregateFunction::Avg => AGG_AVG,
            AggregateFunction::Min => AGG_MIN,
            AggregateFunction::Max => AGG_MAX,
            AggregateFunction::Total => AGG_TOTAL,
            AggregateFunction::GroupConcat => AGG_GROUP_CONCAT,
        }
    }

    /// The argument counts the aggregate accepts (COUNT(*) counts as one).
    pub fn arity(self, name: &str) -> (usize, usize) {
        match self {
            AggregateFunction::GroupConcat if name.eq_ignore_ascii_case("STRING_AGG") => (2, 2),
            AggregateFunction::GroupConcat => (1, 2),
            _ => (1, 1),
        }
    }

    /// Whether `name` (any case) is an aggregate function.
    ///
    /// The single source of the aggregate name list: every site that needs to
    /// recognise an aggregate asks here.
    pub fn is_aggregate_name(name: &str) -> bool {
        Self::from_name(name).is_some()
    }

    /// Whether a call of `name` with `nargs` arguments is an aggregate.
    /// MIN and MAX with two or more arguments are the scalar functions.
    pub fn is_aggregate_call(name: &str, nargs: usize) -> bool {
        match Self::from_name(name) {
            Some(AggregateFunction::Min | AggregateFunction::Max) => nargs < 2,
            Some(_) => true,
            None => false,
        }
    }
}
