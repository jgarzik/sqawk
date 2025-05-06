//! String function implementation module for sqawk
//!
//! This module implements standard SQL string functions including:
//! - UPPER(): Convert string to uppercase
//! - LOWER(): Convert string to lowercase
//! - TRIM(): Remove leading and trailing whitespace
//! - SUBSTR(): Extract a substring
//! - REPLACE(): Replace occurrences of a substring

use crate::error::{SqawkError, SqawkResult};
use crate::table::Value;

/// Enum of supported string functions
#[derive(Debug, Clone, PartialEq)]
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
            "REPLACE" => Some(StringFunction::Replace),
            _ => None,
        }
    }

    /// Apply the string function to its arguments
    pub fn apply(&self, args: &[Value]) -> SqawkResult<Value> {
        match self {
            StringFunction::Lower => self.apply_lower(args),
            StringFunction::Upper => self.apply_upper(args),
            StringFunction::Trim => self.apply_trim(args),
            StringFunction::Substr => self.apply_substr(args),
            StringFunction::Replace => self.apply_replace(args),
        }
    }

    /// Apply LOWER function - convert to lowercase
    fn apply_lower(&self, args: &[Value]) -> SqawkResult<Value> {
        // Validate argument count
        if args.len() != 1 {
            return Err(SqawkError::InvalidFunctionArguments(
                "LOWER requires exactly one argument".to_string(),
            ));
        }

        // Handle input value
        match &args[0] {
            // Pass NULL through
            Value::Null => Ok(Value::Null),
            
            // Convert string to lowercase
            Value::String(s) => Ok(Value::String(s.to_lowercase())),
            
            // Error for non-string inputs
            _ => Err(SqawkError::TypeError(format!(
                "LOWER function requires a string argument, got {:?}",
                args[0]
            ))),
        }
    }

    /// Apply UPPER function - convert to uppercase
    fn apply_upper(&self, args: &[Value]) -> SqawkResult<Value> {
        // Validate argument count
        if args.len() != 1 {
            return Err(SqawkError::InvalidFunctionArguments(
                "UPPER requires exactly one argument".to_string(),
            ));
        }

        // Handle input value
        match &args[0] {
            // Pass NULL through
            Value::Null => Ok(Value::Null),
            
            // Convert string to uppercase
            Value::String(s) => Ok(Value::String(s.to_uppercase())),
            
            // Error for non-string inputs
            _ => Err(SqawkError::TypeError(format!(
                "UPPER function requires a string argument, got {:?}",
                args[0]
            ))),
        }
    }

    /// Apply TRIM function - remove leading/trailing whitespace
    fn apply_trim(&self, args: &[Value]) -> SqawkResult<Value> {
        // Validate argument count
        if args.len() != 1 {
            return Err(SqawkError::InvalidFunctionArguments(
                "TRIM requires exactly one argument".to_string(),
            ));
        }

        // Handle input value
        match &args[0] {
            // Pass NULL through
            Value::Null => Ok(Value::Null),
            
            // Trim string
            Value::String(s) => Ok(Value::String(s.trim().to_string())),
            
            // Error for non-string inputs
            _ => Err(SqawkError::TypeError(format!(
                "TRIM function requires a string argument, got {:?}",
                args[0]
            ))),
        }
    }

    /// Apply SUBSTR function - extract substring
    fn apply_substr(&self, args: &[Value]) -> SqawkResult<Value> {
        // Validate argument count
        if args.len() < 2 || args.len() > 3 {
            return Err(SqawkError::InvalidFunctionArguments(
                "SUBSTR requires two or three arguments: (string, start_pos[, length])".to_string(),
            ));
        }

        // Handle NULL input
        if let Value::Null = args[0] {
            return Ok(Value::Null);
        }

        // Get the string
        let string = match &args[0] {
            Value::String(s) => s,
            _ => {
                return Err(SqawkError::TypeError(format!(
                    "First argument to SUBSTR must be a string, got {:?}",
                    args[0]
                )))
            }
        };

        // Get the start position (1-indexed in SQL)
        let start_pos = match &args[1] {
            Value::Integer(n) => {
                if *n < 1 {
                    return Err(SqawkError::InvalidFunctionArguments(
                        "Start position in SUBSTR must be at least 1".to_string(),
                    ));
                }
                *n as usize
            }
            _ => {
                return Err(SqawkError::TypeError(format!(
                    "Second argument to SUBSTR must be an integer, got {:?}",
                    args[1]
                )))
            }
        };

        // Convert to 0-indexed for Rust
        let start_index = start_pos - 1;

        // If start_index is beyond the string length, return empty string
        if start_index >= string.len() {
            return Ok(Value::String("".to_string()));
        }

        // Get the optional length argument
        let result = if args.len() == 3 {
            match &args[2] {
                Value::Integer(n) => {
                    if *n < 0 {
                        return Err(SqawkError::InvalidFunctionArguments(
                            "Length in SUBSTR must be non-negative".to_string(),
                        ));
                    }
                    
                    // Get the substring of specified length
                    let end_index = string.len().min(start_index + *n as usize);
                    string[start_index..end_index].to_string()
                }
                _ => {
                    return Err(SqawkError::TypeError(format!(
                        "Third argument to SUBSTR must be an integer, got {:?}",
                        args[2]
                    )))
                }
            }
        } else {
            // No length argument, take everything from start_index to the end
            string[start_index..].to_string()
        };

        Ok(Value::String(result))
    }

    /// Apply REPLACE function - replace substring occurrences
    fn apply_replace(&self, args: &[Value]) -> SqawkResult<Value> {
        // Validate argument count
        if args.len() != 3 {
            return Err(SqawkError::InvalidFunctionArguments(
                "REPLACE requires exactly three arguments: (string, search, replace)".to_string(),
            ));
        }

        // Handle NULL input
        if let Value::Null = args[0] {
            return Ok(Value::Null);
        }

        // Get the source string
        let string = match &args[0] {
            Value::String(s) => s,
            _ => {
                return Err(SqawkError::TypeError(format!(
                    "First argument to REPLACE must be a string, got {:?}",
                    args[0]
                )))
            }
        };

        // Get the search pattern
        let pattern = match &args[1] {
            Value::String(s) => s,
            _ => {
                return Err(SqawkError::TypeError(format!(
                    "Second argument to REPLACE must be a string, got {:?}",
                    args[1]
                )))
            }
        };

        // Get the replacement string
        let replacement = match &args[2] {
            Value::String(s) => s,
            _ => {
                return Err(SqawkError::TypeError(format!(
                    "Third argument to REPLACE must be a string, got {:?}",
                    args[2]
                )))
            }
        };

        // Perform the replacement
        let result = string.replace(pattern, replacement);
        Ok(Value::String(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(match &result {
            Ok(Value::Null) => true,
            _ => false
        });
        
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
        assert!(match &result {
            Ok(Value::Null) => true,
            _ => false
        });
        
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
        assert!(match &result {
            Ok(Value::String(s)) => s == "hello",
            _ => false
        });
        
        // Test with spaces on left side only
        let result = func.apply(&[Value::String("  hello".to_string())]);
        assert!(match &result {
            Ok(Value::String(s)) => s == "hello",
            _ => false
        });
        
        // Test with spaces on right side only
        let result = func.apply(&[Value::String("hello  ".to_string())]);
        assert!(match &result {
            Ok(Value::String(s)) => s == "hello",
            _ => false
        });
        
        // Test with no spaces
        let result = func.apply(&[Value::String("hello".to_string())]);
        assert!(match &result {
            Ok(Value::String(s)) => s == "hello",
            _ => false
        });
        
        // Test with NULL
        let result = func.apply(&[Value::Null]);
        assert!(match &result {
            Ok(Value::Null) => true,
            _ => false
        });
        
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
        
        // Test basic substring (start, no length)
        let result = func.apply(&[
            Value::String("hello world".to_string()),
            Value::Integer(7),
        ]);
        assert!(match &result {
            Ok(Value::String(s)) => s == "world",
            _ => false
        });
        
        // Test with start and length
        let result = func.apply(&[
            Value::String("hello world".to_string()),
            Value::Integer(1),
            Value::Integer(5),
        ]);
        assert!(match &result {
            Ok(Value::String(s)) => s == "hello",
            _ => false
        });
        
        // Test with start beyond string length
        let result = func.apply(&[
            Value::String("hello".to_string()),
            Value::Integer(10),
        ]);
        assert!(match &result {
            Ok(Value::String(s)) => s.is_empty(),
            _ => false
        });
        
        // Test with length beyond string end
        let result = func.apply(&[
            Value::String("hello".to_string()),
            Value::Integer(1),
            Value::Integer(100),
        ]);
        assert!(match &result {
            Ok(Value::String(s)) => s == "hello",
            _ => false
        });
        
        // Test with NULL
        let result = func.apply(&[Value::Null, Value::Integer(1)]);
        assert!(match &result {
            Ok(Value::Null) => true,
            _ => false
        });
        
        // Test with invalid start position
        let result = func.apply(&[
            Value::String("hello".to_string()),
            Value::Integer(0),
        ]);
        assert!(result.is_err());
        
        // Test with negative length
        let result = func.apply(&[
            Value::String("hello".to_string()),
            Value::Integer(1),
            Value::Integer(-5),
        ]);
        assert!(result.is_err());
        
        // Test with wrong argument types
        let result = func.apply(&[
            Value::String("hello".to_string()),
            Value::String("world".to_string()),
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
        assert!(match &result {
            Ok(Value::String(s)) => s == "hello Rust",
            _ => false
        });
        
        // Test replacement with empty string (deletion)
        let result = func.apply(&[
            Value::String("hello world".to_string()),
            Value::String("o".to_string()),
            Value::String("".to_string()),
        ]);
        assert!(match &result {
            Ok(Value::String(s)) => s == "hell wrld",
            _ => false
        });
        
        // Test with pattern not found
        let result = func.apply(&[
            Value::String("hello world".to_string()),
            Value::String("xyz".to_string()),
            Value::String("abc".to_string()),
        ]);
        assert!(match &result {
            Ok(Value::String(s)) => s == "hello world",
            _ => false
        });
        
        // Test with NULL
        let result = func.apply(&[
            Value::Null,
            Value::String("a".to_string()),
            Value::String("b".to_string()),
        ]);
        assert!(match &result {
            Ok(Value::Null) => true,
            _ => false
        });
        
        // Test with wrong argument types
        let result = func.apply(&[
            Value::String("hello".to_string()),
            Value::Integer(123),
            Value::String("world".to_string()),
        ]);
        assert!(result.is_err());
        
        // Test with wrong argument count
        let result = func.apply(&[
            Value::String("hello".to_string()),
            Value::String("world".to_string()),
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_name() {
        // Test exact matches
        assert_eq!(StringFunction::from_name("UPPER"), Some(StringFunction::Upper));
        assert_eq!(StringFunction::from_name("LOWER"), Some(StringFunction::Lower));
        assert_eq!(StringFunction::from_name("TRIM"), Some(StringFunction::Trim));
        assert_eq!(StringFunction::from_name("SUBSTR"), Some(StringFunction::Substr));
        assert_eq!(StringFunction::from_name("REPLACE"), Some(StringFunction::Replace));
        
        // Test case insensitivity
        assert_eq!(StringFunction::from_name("upper"), Some(StringFunction::Upper));
        assert_eq!(StringFunction::from_name("Lower"), Some(StringFunction::Lower));
        assert_eq!(StringFunction::from_name("trim"), Some(StringFunction::Trim));
        
        // Test non-existent function
        assert_eq!(StringFunction::from_name("UNKNOWN"), None);
        assert_eq!(StringFunction::from_name(""), None);
    }
}