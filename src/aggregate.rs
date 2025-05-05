//! Aggregate function module for sqawk
//!
//! This module contains implementations of SQL aggregate functions.

use crate::error::SqawkResult;
use crate::table::Value;

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

    /// Execute the aggregate function on a column of values
    ///
    /// # Arguments
    /// * `values` - The column values to aggregate
    ///
    /// # Returns
    /// * The result of the aggregate function
    pub fn execute(&self, values: &[Value]) -> SqawkResult<Value> {
        match self {
            AggregateFunction::Count => self.count(values),
            AggregateFunction::Sum => self.sum(values),
            AggregateFunction::Avg => self.avg(values),
            AggregateFunction::Min => self.min(values),
            AggregateFunction::Max => self.max(values),
        }
    }

    /// COUNT function implementation
    ///
    /// Counts non-NULL values in the column
    fn count(&self, values: &[Value]) -> SqawkResult<Value> {
        let count = values.iter()
            .filter(|v| !matches!(v, Value::Null))
            .count();

        Ok(Value::Integer(count as i64))
    }

    /// SUM function implementation
    ///
    /// Sums numeric values in the column
    fn sum(&self, values: &[Value]) -> SqawkResult<Value> {
        // Keep track of whether we need to return an integer or float
        let mut is_float = false;
        let mut int_sum: i64 = 0;
        let mut float_sum: f64 = 0.0;

        // Count non-null values
        let mut count = 0;

        for value in values {
            match value {
                Value::Integer(i) => {
                    if is_float {
                        float_sum += *i as f64;
                    } else {
                        int_sum += *i;
                    }
                    count += 1;
                }
                Value::Float(f) => {
                    if !is_float {
                        // Convert accumulated integer sum to float
                        float_sum = int_sum as f64;
                        is_float = true;
                    }
                    float_sum += *f;
                    count += 1;
                }
                // Ignore non-numeric values
                _ => {}
            }
        }

        // If no numeric values were found, return NULL
        if count == 0 {
            return Ok(Value::Null);
        }

        // Return the sum in the appropriate type
        if is_float {
            Ok(Value::Float(float_sum))
        } else {
            Ok(Value::Integer(int_sum))
        }
    }

    /// AVG function implementation
    ///
    /// Calculates the average of numeric values in the column
    fn avg(&self, values: &[Value]) -> SqawkResult<Value> {
        // Get the sum first
        let sum = self.sum(values)?;
        
        // Count numeric values
        let count = values.iter()
            .filter(|v| matches!(v, Value::Integer(_) | Value::Float(_)))
            .count();
            
        if count == 0 {
            return Ok(Value::Null);
        }
            
        // Convert to float and divide by count
        match sum {
            Value::Integer(i) => Ok(Value::Float(i as f64 / count as f64)),
            Value::Float(f) => Ok(Value::Float(f / count as f64)),
            _ => Ok(Value::Null),  // This shouldn't happen, but handle it just in case
        }
    }

    /// MIN function implementation
    ///
    /// Finds the minimum value in the column
    fn min(&self, values: &[Value]) -> SqawkResult<Value> {
        // Filter out NULL values
        let non_null_values: Vec<&Value> = values.iter()
            .filter(|v| !matches!(v, Value::Null))
            .collect();
            
        if non_null_values.is_empty() {
            return Ok(Value::Null);
        }
        
        // Start with the first value as our minimum
        let mut min_value = non_null_values[0].clone();
        
        // Compare with the rest of the values
        for value in &non_null_values[1..] {
            if value < &&min_value {
                min_value = (*value).clone();
            }
        }
        
        Ok(min_value)
    }

    /// MAX function implementation
    ///
    /// Finds the maximum value in the column
    fn max(&self, values: &[Value]) -> SqawkResult<Value> {
        // Filter out NULL values
        let non_null_values: Vec<&Value> = values.iter()
            .filter(|v| !matches!(v, Value::Null))
            .collect();
            
        if non_null_values.is_empty() {
            return Ok(Value::Null);
        }
        
        // Start with the first value as our maximum
        let mut max_value = non_null_values[0].clone();
        
        // Compare with the rest of the values
        for value in &non_null_values[1..] {
            if value > &&max_value {
                max_value = (*value).clone();
            }
        }
        
        Ok(max_value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_function() {
        let values = vec![
            Value::Integer(10),
            Value::Null,
            Value::Integer(20),
            Value::String("test".to_string()),
        ];
        
        let count = AggregateFunction::Count.execute(&values).unwrap();
        assert_eq!(count, Value::Integer(3));
    }

    #[test]
    fn test_sum_function() {
        let values = vec![
            Value::Integer(10),
            Value::Null,
            Value::Integer(20),
            Value::Float(5.5),
        ];
        
        let sum = AggregateFunction::Sum.execute(&values).unwrap();
        assert_eq!(sum, Value::Float(35.5));
    }

    #[test]
    fn test_sum_integers_only() {
        let values = vec![
            Value::Integer(10),
            Value::Integer(20),
            Value::Integer(30),
        ];
        
        let sum = AggregateFunction::Sum.execute(&values).unwrap();
        assert_eq!(sum, Value::Integer(60));
    }

    #[test]
    fn test_avg_function() {
        let values = vec![
            Value::Integer(10),
            Value::Null,
            Value::Integer(20),
            Value::Float(30.0),
        ];
        
        let avg = AggregateFunction::Avg.execute(&values).unwrap();
        if let Value::Float(f) = avg {
            assert!((f - 20.0).abs() < f64::EPSILON);
        } else {
            panic!("Expected Float, got {:?}", avg);
        }
    }

    #[test]
    fn test_min_function() {
        let values = vec![
            Value::Integer(30),
            Value::Integer(10),
            Value::Null,
            Value::Integer(20),
        ];
        
        let min = AggregateFunction::Min.execute(&values).unwrap();
        assert_eq!(min, Value::Integer(10));
    }

    #[test]
    fn test_min_with_mixed_types() {
        let values = vec![
            Value::Integer(30),
            Value::Float(5.5),
            Value::String("abc".to_string()),
            Value::Null,
        ];
        
        let min = AggregateFunction::Min.execute(&values).unwrap();
        assert_eq!(min, Value::Float(5.5));
    }

    #[test]
    fn test_max_function() {
        let values = vec![
            Value::Integer(30),
            Value::Integer(10),
            Value::Null,
            Value::Integer(20),
        ];
        
        let max = AggregateFunction::Max.execute(&values).unwrap();
        assert_eq!(max, Value::Integer(30));
    }

    #[test]
    fn test_max_with_mixed_types() {
        let values = vec![
            Value::Integer(30),
            Value::Float(50.5),
            Value::String("xyz".to_string()),
            Value::Null,
        ];
        
        let max = AggregateFunction::Max.execute(&values).unwrap();
        assert_eq!(max, Value::Float(50.5));
    }
}