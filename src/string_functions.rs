//! SQL string function implementations
//!
//! This module contains implementations of common SQL string manipulation functions.
//! It handles UPPER, LOWER, TRIM, SUBSTR, and REPLACE functions with appropriate
//! error handling for invalid arguments.

use crate::error::{SqawkError, SqawkResult};
use crate::table::Value;

/// Enum of supported string functions
#[derive(Debug, PartialEq, Clone)]
pub enum StringFunction {
    /// Convert string to uppercase - UPPER(str)
    Upper,
    /// Convert string to lowercase - LOWER(str)
    Lower,
    /// Remove leading/trailing whitespace - TRIM(str)
    Trim,
    /// Extract substring - SUBSTR(str, start_pos[, length])
    Substr,
    /// Replace occurrences of a substring - REPLACE(str, search, replace)
    Replace,
}

impl StringFunction {
    /// Create a StringFunction from its name
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_uppercase().as_str() {
            "UPPER" => Some(StringFunction::Upper),
            "LOWER" => Some(StringFunction::Lower),
            "TRIM" => Some(StringFunction::Trim),
            "SUBSTR" => Some(StringFunction::Substr),
            "SUBSTRING" => Some(StringFunction::Substr), // Common alias
            "REPLACE" => Some(StringFunction::Replace),
            _ => None,
        }
    }

    /// Apply the string function to the given arguments
    ///
    /// # Arguments
    /// * `args` - Function arguments (values to operate on)
    ///
    /// # Returns
    /// * `Ok(Value)` - Result of the string function
    /// * `Err` - If arguments are invalid (wrong type or count)
    pub fn apply(&self, args: &[Value]) -> SqawkResult<Value> {
        match self {
            StringFunction::Upper => self.apply_upper(args),
            StringFunction::Lower => self.apply_lower(args),
            StringFunction::Trim => self.apply_trim(args),
            StringFunction::Substr => self.apply_substr(args),
            StringFunction::Replace => self.apply_replace(args),
        }
    }

    /// Apply UPPER function: convert a string to uppercase
    ///
    /// # Arguments
    /// * `args` - Function arguments, expecting exactly one string argument
    ///
    /// # Returns
    /// * `Ok(Value::String)` - The uppercase version of the input string
    /// * `Err` - If wrong number of arguments or not a string
    fn apply_upper(&self, args: &[Value]) -> SqawkResult<Value> {
        if args.len() != 1 {
            return Err(SqawkError::InvalidArgumentCount {
                function: "UPPER".to_string(),
                expected: 1,
                actual: args.len(),
            });
        }

        match &args[0] {
            Value::String(s) => Ok(Value::String(s.to_uppercase())),
            Value::Null => Ok(Value::Null),
            _ => Err(SqawkError::InvalidArgument(format!(
                "UPPER function requires a string argument, got {:?}",
                args[0]
            ))),
        }
    }

    /// Apply LOWER function: convert a string to lowercase
    ///
    /// # Arguments
    /// * `args` - Function arguments, expecting exactly one string argument
    ///
    /// # Returns
    /// * `Ok(Value::String)` - The lowercase version of the input string
    /// * `Err` - If wrong number of arguments or not a string
    fn apply_lower(&self, args: &[Value]) -> SqawkResult<Value> {
        if args.len() != 1 {
            return Err(SqawkError::InvalidArgumentCount {
                function: "LOWER".to_string(),
                expected: 1,
                actual: args.len(),
            });
        }

        match &args[0] {
            Value::String(s) => Ok(Value::String(s.to_lowercase())),
            Value::Null => Ok(Value::Null),
            _ => Err(SqawkError::InvalidArgument(format!(
                "LOWER function requires a string argument, got {:?}",
                args[0]
            ))),
        }
    }

    /// Apply TRIM function: remove leading/trailing whitespace
    ///
    /// # Arguments
    /// * `args` - Function arguments, expecting exactly one string argument
    ///
    /// # Returns
    /// * `Ok(Value::String)` - The trimmed version of the input string
    /// * `Err` - If wrong number of arguments or not a string
    fn apply_trim(&self, args: &[Value]) -> SqawkResult<Value> {
        if args.len() != 1 {
            return Err(SqawkError::InvalidArgumentCount {
                function: "TRIM".to_string(),
                expected: 1,
                actual: args.len(),
            });
        }

        match &args[0] {
            Value::String(s) => Ok(Value::String(s.trim().to_string())),
            Value::Null => Ok(Value::Null),
            _ => Err(SqawkError::InvalidArgument(format!(
                "TRIM function requires a string argument, got {:?}",
                args[0]
            ))),
        }
    }

    /// Apply SUBSTR function: extract a substring
    ///
    /// # Arguments
    /// * `args` - Function arguments, expecting:
    ///   * args[0]: string to extract from
    ///   * args[1]: start position (1-based as in SQL standard)
    ///   * args[2]: optional length of substring
    ///
    /// # Returns
    /// * `Ok(Value::String)` - The extracted substring
    /// * `Err` - If wrong number/type of arguments
    fn apply_substr(&self, args: &[Value]) -> SqawkResult<Value> {
        if args.len() < 2 || args.len() > 3 {
            return Err(SqawkError::InvalidArgumentCount {
                function: "SUBSTR".to_string(),
                expected: 2,
                actual: args.len(),
            });
        }

        // Handle NULL input
        if args[0] == Value::Null {
            return Ok(Value::Null);
        }

        // Get the source string
        let src = match &args[0] {
            Value::String(s) => s,
            _ => {
                return Err(SqawkError::InvalidArgument(format!(
                    "SUBSTR first argument must be a string, got {:?}",
                    args[0]
                )))
            }
        };

        // Get the start position (1-based index in SQL, convert to 0-based for Rust)
        let start_pos = match &args[1] {
            Value::Integer(i) => {
                if *i <= 0 {
                    // In SQL, positions start at 1, not 0
                    return Err(SqawkError::InvalidArgument(
                        "SUBSTR start position must be >= 1".to_string(),
                    ));
                }
                (*i as usize) - 1 // Convert to 0-based for Rust
            }
            _ => {
                return Err(SqawkError::InvalidArgument(format!(
                    "SUBSTR second argument must be an integer, got {:?}",
                    args[1]
                )))
            }
        };

        // If start position is beyond the string length, return empty string
        if start_pos >= src.len() {
            return Ok(Value::String("".to_string()));
        }

        // If we have 3 args, use the third as length
        if args.len() == 3 {
            let length = match &args[2] {
                Value::Integer(i) => {
                    if *i < 0 {
                        return Err(SqawkError::InvalidArgument(
                            "SUBSTR length must be >= 0".to_string(),
                        ));
                    }
                    *i as usize
                }
                _ => {
                    return Err(SqawkError::InvalidArgument(format!(
                        "SUBSTR third argument must be an integer, got {:?}",
                        args[2]
                    )))
                }
            };

            // Extract substring with specified length
            let end_pos = start_pos + length;
            let end_pos = end_pos.min(src.len()); // Don't go past the end of string
            Ok(Value::String(src[start_pos..end_pos].to_string()))
        } else {
            // No length specified, return from start_pos to end
            Ok(Value::String(src[start_pos..].to_string()))
        }
    }

    /// Apply REPLACE function: replace occurrences of a substring
    ///
    /// # Arguments
    /// * `args` - Function arguments, expecting:
    ///   * args[0]: source string
    ///   * args[1]: search string
    ///   * args[2]: replacement string
    ///
    /// # Returns
    /// * `Ok(Value::String)` - The string with replacements
    /// * `Err` - If wrong number/type of arguments
    fn apply_replace(&self, args: &[Value]) -> SqawkResult<Value> {
        if args.len() != 3 {
            return Err(SqawkError::InvalidArgumentCount {
                function: "REPLACE".to_string(),
                expected: 3,
                actual: args.len(),
            });
        }

        // Handle NULL input
        if args[0] == Value::Null {
            return Ok(Value::Null);
        }

        // Get the source string
        let src = match &args[0] {
            Value::String(s) => s,
            _ => {
                return Err(SqawkError::InvalidArgument(format!(
                    "REPLACE first argument must be a string, got {:?}",
                    args[0]
                )))
            }
        };

        // Get the search string
        let search = match &args[1] {
            Value::String(s) => s,
            _ => {
                return Err(SqawkError::InvalidArgument(format!(
                    "REPLACE second argument must be a string, got {:?}",
                    args[1]
                )))
            }
        };

        // Get the replacement string
        let replace = match &args[2] {
            Value::String(s) => s,
            _ => {
                return Err(SqawkError::InvalidArgument(format!(
                    "REPLACE third argument must be a string, got {:?}",
                    args[2]
                )))
            }
        };

        // Perform the replacement
        Ok(Value::String(src.replace(search, replace)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_function_from_name() {
        assert_eq!(StringFunction::from_name("UPPER"), Some(StringFunction::Upper));
        assert_eq!(StringFunction::from_name("upper"), Some(StringFunction::Upper));
        assert_eq!(StringFunction::from_name("Upper"), Some(StringFunction::Upper));
        assert_eq!(StringFunction::from_name("LOWER"), Some(StringFunction::Lower));
        assert_eq!(StringFunction::from_name("TRIM"), Some(StringFunction::Trim));
        assert_eq!(StringFunction::from_name("SUBSTR"), Some(StringFunction::Substr));
        assert_eq!(
            StringFunction::from_name("SUBSTRING"),
            Some(StringFunction::Substr)
        );
        assert_eq!(
            StringFunction::from_name("REPLACE"),
            Some(StringFunction::Replace)
        );
        assert_eq!(StringFunction::from_name("UNKNOWN"), None);
    }

    #[test]
    fn test_upper_function() {
        let func = StringFunction::Upper;
        
        // Test with valid string
        let result = func.apply(&[Value::String("hello".to_string())]);
        assert!(match &result {
            Ok(Value::String(s)) => s == "HELLO",
            _ => false
        });
        
        // Test with NULL
        let result = func.apply(&[Value::Null]);
        assert_eq!(result, Ok(Value::Null));
        
        // Test with wrong argument type
        let result = func.apply(&[Value::Integer(42)]);
        assert!(result.is_err());
        
        // Test with wrong argument count
        let result = func.apply(&[]);
        assert!(result.is_err());
        let result = func.apply(&[Value::String("a".to_string()), Value::String("b".to_string())]);
        assert!(result.is_err());
    }

    #[test]
    fn test_lower_function() {
        let func = StringFunction::Lower;
        
        // Test with valid string
        let result = func.apply(&[Value::String("HELLO".to_string())]);
        assert!(match &result {
            Ok(Value::String(s)) => s == "hello",
            _ => false
        });
        
        // Test with mixed case
        let result = func.apply(&[Value::String("HeLLo".to_string())]);
        assert!(match &result {
            Ok(Value::String(s)) => s == "hello",
            _ => false
        });
        
        // Test with NULL
        let result = func.apply(&[Value::Null]);
        assert_eq!(result, Ok(Value::Null));
        
        // Test with wrong argument type
        let result = func.apply(&[Value::Integer(42)]);
        assert!(result.is_err());
        
        // Test with wrong argument count
        let result = func.apply(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_trim_function() {
        let func = StringFunction::Trim;
        
        // Test with spaces on both sides
        let result = func.apply(&[Value::String("  hello  ".to_string())]);
        assert_eq!(result, Ok(Value::String("hello".to_string())));
        
        // Test with spaces on left side only
        let result = func.apply(&[Value::String("  hello".to_string())]);
        assert_eq!(result, Ok(Value::String("hello".to_string())));
        
        // Test with spaces on right side only
        let result = func.apply(&[Value::String("hello  ".to_string())]);
        assert_eq!(result, Ok(Value::String("hello".to_string())));
        
        // Test with no spaces
        let result = func.apply(&[Value::String("hello".to_string())]);
        assert_eq!(result, Ok(Value::String("hello".to_string())));
        
        // Test with NULL
        let result = func.apply(&[Value::Null]);
        assert_eq!(result, Ok(Value::Null));
        
        // Test with wrong argument type
        let result = func.apply(&[Value::Integer(42)]);
        assert!(result.is_err());
        
        // Test with wrong argument count
        let result = func.apply(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_substr_function() {
        let func = StringFunction::Substr;
        
        // Test with start position only
        let result = func.apply(&[
            Value::String("hello world".to_string()),
            Value::Integer(7),
        ]);
        assert_eq!(result, Ok(Value::String("world".to_string())));
        
        // Test with start position and length
        let result = func.apply(&[
            Value::String("hello world".to_string()),
            Value::Integer(1),
            Value::Integer(5),
        ]);
        assert_eq!(result, Ok(Value::String("hello".to_string())));
        
        // Test with start position beyond string length
        let result = func.apply(&[
            Value::String("hello".to_string()),
            Value::Integer(10),
        ]);
        assert_eq!(result, Ok(Value::String("".to_string())));
        
        // Test with invalid negative start position
        let result = func.apply(&[
            Value::String("hello".to_string()),
            Value::Integer(-1),
        ]);
        assert!(result.is_err());
        
        // Test with invalid negative length
        let result = func.apply(&[
            Value::String("hello".to_string()),
            Value::Integer(1),
            Value::Integer(-1),
        ]);
        assert!(result.is_err());
        
        // Test with NULL input
        let result = func.apply(&[Value::Null, Value::Integer(1)]);
        assert_eq!(result, Ok(Value::Null));
        
        // Test with wrong argument types
        let result = func.apply(&[
            Value::Integer(123),
            Value::Integer(1),
        ]);
        assert!(result.is_err());
        
        let result = func.apply(&[
            Value::String("hello".to_string()),
            Value::String("1".to_string()),
        ]);
        assert!(result.is_err());
        
        // Test with wrong argument count
        let result = func.apply(&[Value::String("hello".to_string())]);
        assert!(result.is_err());
        
        let result = func.apply(&[
            Value::String("hello".to_string()),
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_replace_function() {
        let func = StringFunction::Replace;
        
        // Test basic replacement
        let result = func.apply(&[
            Value::String("hello world".to_string()),
            Value::String("world".to_string()),
            Value::String("Rust".to_string()),
        ]);
        assert_eq!(result, Ok(Value::String("hello Rust".to_string())));
        
        // Test multiple replacements
        let result = func.apply(&[
            Value::String("hello hello hello".to_string()),
            Value::String("hello".to_string()),
            Value::String("hi".to_string()),
        ]);
        assert_eq!(result, Ok(Value::String("hi hi hi".to_string())));
        
        // Test with pattern not found
        let result = func.apply(&[
            Value::String("hello world".to_string()),
            Value::String("xyz".to_string()),
            Value::String("abc".to_string()),
        ]);
        assert_eq!(result, Ok(Value::String("hello world".to_string())));
        
        // Test with empty replacement
        let result = func.apply(&[
            Value::String("hello world".to_string()),
            Value::String("world".to_string()),
            Value::String("".to_string()),
        ]);
        assert_eq!(result, Ok(Value::String("hello ".to_string())));
        
        // Test with NULL input
        let result = func.apply(&[
            Value::Null,
            Value::String("world".to_string()),
            Value::String("Rust".to_string()),
        ]);
        assert_eq!(result, Ok(Value::Null));
        
        // Test with wrong argument types
        let result = func.apply(&[
            Value::Integer(123),
            Value::String("world".to_string()),
            Value::String("Rust".to_string()),
        ]);
        assert!(result.is_err());
        
        let result = func.apply(&[
            Value::String("hello world".to_string()),
            Value::Integer(123),
            Value::String("Rust".to_string()),
        ]);
        assert!(result.is_err());
        
        let result = func.apply(&[
            Value::String("hello world".to_string()),
            Value::String("world".to_string()),
            Value::Integer(123),
        ]);
        assert!(result.is_err());
        
        // Test with wrong argument count
        let result = func.apply(&[
            Value::String("hello".to_string()),
            Value::String("world".to_string()),
        ]);
        assert!(result.is_err());
        
        let result = func.apply(&[
            Value::String("hello".to_string()),
            Value::String("world".to_string()),
            Value::String("Rust".to_string()),
            Value::String("extra".to_string()),
        ]);
        assert!(result.is_err());
    }
}