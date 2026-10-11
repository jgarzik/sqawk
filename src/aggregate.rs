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
            _ => None,
        }
    }

    /// The AggStep/AggFinal function-type constant for this aggregate.
    pub fn vm_type(self) -> i64 {
        use crate::vm::bytecode::{AGG_AVG, AGG_COUNT, AGG_MAX, AGG_MIN, AGG_SUM};
        match self {
            AggregateFunction::Count => AGG_COUNT,
            AggregateFunction::Sum => AGG_SUM,
            AggregateFunction::Avg => AGG_AVG,
            AggregateFunction::Min => AGG_MIN,
            AggregateFunction::Max => AGG_MAX,
        }
    }

    /// Whether `name` (any case) is an aggregate function.
    ///
    /// The single source of the aggregate name list: every site that needs to
    /// recognise an aggregate asks here.
    pub fn is_aggregate_name(name: &str) -> bool {
        Self::from_name(name).is_some()
    }
}
