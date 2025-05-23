//! Bytecode definitions for the SQL VM
//!
//! This module defines the bytecode instruction set used by the SQL virtual machine.
//! The design is inspired by SQLite's approach, with instructions consisting of an
//! opcode and up to 3 parameters (P1, P2, P3), plus an optional P4 parameter for strings.
//!
//! Each instruction has a specific semantics that controls how data is loaded,
//! manipulated, and stored during SQL query execution.

use crate::table::Value;
use std::fmt;

/// Opcodes for VM instructions
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OpCode {
    // Program flow control
    Init, // Initialize VM
    Goto, // Jump to address
    Halt, // Stop execution

    // Table operations
    OpenRead,  // Open a table for reading
    OpenWrite, // Open a table for writing
    Close,     // Close a cursor

    // Cursor operations
    Rewind, // Move cursor to first row
    Next,   // Move cursor to next row
    Column, // Read column value into register

    // Data manipulation
    Integer,   // Load integer constant
    String,    // Load string constant
    Null,      // Load NULL value
    ResultRow, // Return result row to client

    // Transaction operations
    Begin,     // Begin a transaction - marks the start of a set of changes that can be committed or rolled back
    Commit,    // Commit a transaction - permanently applies all changes made since the Begin operation
    Rollback,  // Rollback a transaction - discards all changes made since the Begin operation
    SavePoint, // Create a savepoint in the transaction - establishes a point to which a transaction can be partially rolled back
    Release,   // Release a savepoint - confirms changes up to the specified savepoint

    // Comparison operations
    Lt,      // Less than comparison (P1 < P2, result in P3)
    Le,      // Less than or equal comparison (P1 <= P2, result in P3)
    Eq,      // Equal comparison (P1 == P2, result in P3)
    Ne,      // Not equal comparison (P1 != P2, result in P3)
    Gt,      // Greater than comparison (P1 > P2, result in P3)
    Ge,      // Greater than or equal comparison (P1 >= P2, result in P3)
    
    // Conditional jumps
    IfZ,    // Jump to P2 if register P1 contains 0
    IfPos,  // Jump to P2 if register P1 is positive (> 0)
    IfNeg,  // Jump to P2 if register P1 is negative (< 0)
    
    // Utility opcodes
    Noop, // No operation
}

/// A SQL VM instruction with opcode and parameters
#[derive(Debug, Clone)]
pub struct Instruction {
    /// The operation code
    pub opcode: OpCode,

    /// P1 parameter (typically a register, cursor, or value index)
    pub p1: i64,

    /// P2 parameter (typically a jump address, register, or count)
    pub p2: i64,

    /// P3 parameter (typically a register)
    pub p3: i64,

    /// P4 parameter (typically a string parameter)
    pub p4: Option<String>,

    // Removed unused p5 parameter
    /// Comment describing the instruction
    pub comment: Option<String>,
}

impl Instruction {
    /// Create a new instruction with the given opcode and parameters
    pub fn new(
        opcode: OpCode,
        p1: i64,
        p2: i64,
        p3: i64,
        p4: Option<String>,
        _p5: i64, // Keep parameter for compatibility but don't use it
        comment: Option<String>,
    ) -> Self {
        Self {
            opcode,
            p1,
            p2,
            p3,
            p4,
            comment,
        }
    }
}

impl fmt::Display for Instruction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:?} p1={} p2={} p3={}{}{}",
            self.opcode,
            self.p1,
            self.p2,
            self.p3,
            if let Some(p4) = &self.p4 {
                format!(" p4=\"{}\"", p4)
            } else {
                String::new()
            },
            if let Some(comment) = &self.comment {
                format!(" /* {} */", comment)
            } else {
                String::new()
            }
        )
    }
}

/// A program of bytecode instructions
#[derive(Debug, Clone)]
pub struct Program {
    /// The list of instructions
    pub instructions: Vec<Instruction>,
}

impl Default for Program {
    fn default() -> Self {
        Self::new()
    }
}

impl Program {
    /// Create a new empty program
    pub fn new() -> Self {
        Self {
            instructions: Vec::new(),
        }
    }

    /// Add an instruction to the program
    pub fn add_instruction(&mut self, instruction: Instruction) {
        self.instructions.push(instruction);
    }

    /// Get the length of the program in instructions
    pub fn len(&self) -> usize {
        self.instructions.len()
    }

    /// Check if the program is empty
    pub fn is_empty(&self) -> bool {
        self.instructions.is_empty()
    }

    /// Get an instruction at a specific address
    pub fn get(&self, addr: usize) -> Option<&Instruction> {
        self.instructions.get(addr)
    }
}

impl fmt::Display for Program {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Program ({} instructions):", self.instructions.len())?;
        for (i, instruction) in self.instructions.iter().enumerate() {
            writeln!(f, "{:3}: {}", i, instruction)?;
        }
        Ok(())
    }
}

/// Register value for VM execution
#[derive(Debug, Clone)]
pub enum Register {
    /// Integer value
    Integer(i64),
    /// String value
    String(String),
    /// Floating point value
    Float(f64),
    /// Boolean value
    Boolean(bool),
    /// Null value
    Null,
}

impl From<Value> for Register {
    fn from(value: Value) -> Self {
        match value {
            Value::Integer(i) => Register::Integer(i),
            Value::Float(f) => Register::Float(f),
            Value::String(s) => Register::String(s),
            Value::Boolean(b) => Register::Boolean(b),
            Value::Null => Register::Null,
        }
    }
}

impl From<Register> for Value {
    fn from(register: Register) -> Self {
        match register {
            Register::Integer(i) => Value::Integer(i),
            Register::Float(f) => Value::Float(f),
            Register::String(s) => Value::String(s),
            Register::Boolean(b) => Value::Boolean(b),
            Register::Null => Value::Null,
        }
    }
}
