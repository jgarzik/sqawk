//! Bytecode definitions for the SQL VM
//!
//! This module defines the bytecode instruction set used by the SQL virtual machine.
//! The design is inspired by SQLite's approach, with instructions consisting of an
//! opcode and up to 3 parameters (P1, P2, P3), plus an optional P4 parameter for strings.
//!
//! Each instruction has a specific semantics that controls how data is loaded,
//! manipulated, and stored during SQL query execution.

use crate::capacity::{DEFAULT_COLUMN_CAPACITY, DEFAULT_INSTRUCTION_CAPACITY};
use crate::table::{DataType, Value};
use std::fmt;
use std::rc::Rc;

// Aggregate function type constants for AggStep/AggFinal opcodes
/// COUNT aggregate function type
pub const AGG_COUNT: i64 = 0;
/// SUM aggregate function type
pub const AGG_SUM: i64 = 1;
/// AVG aggregate function type
pub const AGG_AVG: i64 = 2;
/// MIN aggregate function type
pub const AGG_MIN: i64 = 3;
/// MAX aggregate function type
pub const AGG_MAX: i64 = 4;

/// Flag bit OR-ed into an AggStep/AggFinal function type to request DISTINCT.
///
/// Carried as a flag rather than as five extra constants so that DISTINCT
/// composes with every aggregate: `COUNT(DISTINCT x)`, `SUM(DISTINCT x)` and
/// the rest all use the same mechanism. Mask with `AGG_TYPE_MASK` to recover
/// the base function.
pub const AGG_DISTINCT: i64 = 0x100;

/// Mask selecting the base aggregate function out of a function-type word.
pub const AGG_TYPE_MASK: i64 = 0xFF;

/// A column in a result schema
#[derive(Debug, Clone)]
pub struct ResultColumn {
    /// Name of the column (may include table prefix like "users.name")
    pub name: String,
    /// Data type of the column
    pub data_type: DataType,
}

impl ResultColumn {
    pub fn new(name: String, data_type: DataType) -> Self {
        Self { name, data_type }
    }
}

/// Schema definition for query results
///
/// This is built at compile time and carries both column names and types,
/// allowing the VM to construct properly-typed result tables.
#[derive(Debug, Clone, Default)]
pub struct ResultSchema {
    /// Columns in the result set
    pub columns: Vec<ResultColumn>,
}

impl ResultSchema {
    pub fn new() -> Self {
        Self {
            columns: Vec::with_capacity(DEFAULT_COLUMN_CAPACITY),
        }
    }

    /// Add a column to the schema
    pub fn add_column(&mut self, name: String, data_type: DataType) {
        self.columns.push(ResultColumn::new(name, data_type));
    }

    /// Check if schema is empty
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }
}

/// Opcodes for VM instructions
///
/// Note: Some opcodes are defined but not yet used by the compiler.
/// They are kept for planned features in the SQL expansion roadmap.
#[derive(Debug, Clone, Copy, PartialEq)]
#[allow(dead_code)]
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
    Rewind,    // Move cursor to first row
    Next,      // Move cursor to next row
    Column,    // Read column value into register
    InsertRow, // Insert row from registers P2..P2+P3 into cursor P1
    DeleteRow, // Delete current row at cursor P1

    // Data manipulation
    Integer,   // Load integer constant
    String,    // Load string constant
    Null,      // Load NULL value
    ResultRow, // Return result row to client
    Copy,      // Copy register P1 to register P2 (SCopy in SQLite)

    // Transaction operations
    Begin, // Begin a transaction - marks the start of a set of changes that can be committed or rolled back
    Commit, // Commit a transaction - permanently applies all changes made since the Begin operation
    Rollback, // Rollback a transaction - discards all changes made since the Begin operation
    SavePoint, // Create a savepoint in the transaction - establishes a point to which a transaction can be partially rolled back
    Release,   // Release a savepoint - confirms changes up to the specified savepoint

    // Comparison operations
    Lt, // Less than comparison (P1 < P2, result in P3)
    Le, // Less than or equal comparison (P1 <= P2, result in P3)
    Eq, // Equal comparison (P1 == P2, result in P3)
    Ne, // Not equal comparison (P1 != P2, result in P3)
    Gt, // Greater than comparison (P1 > P2, result in P3)
    Ge, // Greater than or equal comparison (P1 >= P2, result in P3)

    // Pattern matching operations
    Like, // LIKE pattern match (P1 LIKE P4 pattern, result in P3). Case-sensitive.
    Glob, // GLOB pattern match (P1 GLOB P4 pattern, result in P3). Unix-style wildcards.

    // Type conversion
    Cast, // Cast value in P1 to type in P4, result in P2. Types: "INTEGER", "TEXT", "REAL"

    // Null operations
    IsNull, // Set P2 to 1 if P1 is NULL, 0 otherwise

    // Logical negation
    //
    // Distinct from the comparison opcodes because it must propagate NULL:
    // NOT NULL is NULL (UNKNOWN), not true. Replaces the several hand-rolled
    // inversions used for NOT LIKE / NOT IN / IS NOT NULL and makes a bare
    // `NOT <expr>` expressible at all.
    Not, // P2 = NULL if P1 is NULL, else 1 if P1 is falsy, else 0

    // Vector comparison, modelled on SQLite's OP_Compare / OP_Jump.
    //
    // Compare sets an internal flag from a pairwise comparison of two register
    // vectors; Jump then branches three ways on that flag. Together they make
    // multi-column key comparison two instructions regardless of key width,
    // which is what GROUP BY needs to detect a group change across ALL key
    // columns rather than just the first.
    //
    // These use INTERNAL ordering, not SQL comparison: NULL sorts equal to
    // NULL, so a NULL group key groups with other NULL keys. SQL `=` must not
    // behave that way, which is exactly why this is a separate opcode rather
    // than a reuse of Eq.
    Compare, // Compare P3 registers starting at P1 against P3 starting at P2
    Jump,    // Branch on the last Compare: P1 if Less, P2 if Equal, P3 if Greater

    // Conditional jumps
    IfZ,   // Jump to P2 if register P1 contains 0
    IfPos, // Jump to P2 if register P1 is positive (> 0)
    IfNeg, // Jump to P2 if register P1 is negative (< 0)

    // Utility opcodes
    Noop, // No operation

    // Set operations
    Distinct,  // Remove duplicate rows from results
    Limit, // Limit results to P1 rows. P2 = offset (rows to skip, already applied). Post-processing opcode.
    Intersect, // Keep only rows that exist in both left and right result sets
    Except, // Keep only rows from left that don't exist in right result set

    // Join operations
    NullRow,     // Load NULL values into registers P1 through P1+P2-1 (P2 = count)
    RewindInner, // Rewind cursor P1, used for inner loop of nested join
    MarkMatch,   // Set register P1 to 1 to indicate a match was found
    CheckMatch,  // If register P1 is 0 (no match), jump to P2; reset register to 0

    // Subquery/Coroutine operations (inspired by SQLite)
    InitCoroutine, // P1 = register to store return address, P2 = jump over coroutine, P3 = coroutine entry
    Yield,         // Swap program counter with register P1 (coroutine yield)
    EndCoroutine,  // Jump back to Yield that invoked this coroutine, set P1 for next Yield return
    Once,          // First time: continue. Subsequently: jump to P2. Used for one-time init.

    // Subquery result operations
    AggStep,   // P1 = function type, P2 = value reg, P3 = accumulator reg. Step aggregate.
    AggFinal,  // P1 = accumulator reg, P2 = result reg. Finalize aggregate.
    AggReset,  // P1 = accumulator reg. Reset accumulator to initial state.
    Exists,    // P1 = flag reg (set to 1 if cursor P2 has rows, else 0)
    NotExists, // P1 = flag reg (set to 1 if cursor P2 has no rows, else 0)

    // Counter operations (LIMIT/OFFSET)
    DecrJumpZero, // Decrement register P1. If result is zero, jump to P2.

    // Sorter operations (ORDER BY)
    SorterOpen, // Open a sorter. P1 = sorter ID, P2 = number of columns, P4 = sort key spec
    SorterInsert, // Insert row into sorter P1. P2 = start register, P3 = column count
    SorterSort, // Sort the sorter P1. Must be called before SorterNext.
    SorterData, // Copy current sorter P1 row to registers starting at P2. P3 = column count.
    SorterNext, // Advance sorter P1. Jump to P2 if more rows, else continue.

    // Ephemeral table operations (for ORDER BY results, temp storage)
    // Ephemeral tables use cursor IDs and work with Column/Next/Rewind opcodes
    OpenEphemeral, // Open ephemeral cursor P1 with P2 columns. P4 = sort key spec for ordering.
    IdxInsert,     // Insert into ephemeral cursor P1. P2 = start register, P3 = column count.
    Sort,          // Sort ephemeral cursor P1. Jump to P2 when empty, else continue.
    Sequence,      // P1 = ephemeral cursor, P2 = dest register. Generate next sequence number.

    // DDL operations
    CreateTable,   // Create a table. P4 = "table_name:col1:type1,col2:type2,..."
    DropTable,     // Drop a table. P4 = table name
    AlterTableAdd, // Add column to table. P4 = "table_name:col_name:col_type"
    Truncate,      // Remove all rows from table. P4 = table name

    // String functions
    StringFunc, // P1 = src reg, P2 = dest reg, P3 = arg2 reg (for SUBSTR/REPLACE), P4 = function name

    // Math functions
    MathFunc, // P1 = src reg, P2 = dest reg, P4 = function name (ABS, ROUND, CEIL, FLOOR)

    // Date/Time functions
    DateFunc, // P1 = src reg (optional), P2 = dest reg, P4 = function name (DATE, TIME, NOW)

    // Arithmetic operations
    Add,       // P1 = left reg, P2 = right reg, P3 = dest reg. dest = left + right
    Subtract,  // P1 = left reg, P2 = right reg, P3 = dest reg. dest = left - right
    Multiply,  // P1 = left reg, P2 = right reg, P3 = dest reg. dest = left * right
    Divide,    // P1 = left reg, P2 = right reg, P3 = dest reg. dest = left / right
    Remainder, // P1 = left reg, P2 = right reg, P3 = dest reg. dest = left % right

    // Window function operations
    WindowAggStep, // P1 = func type, P2 = value reg, P3 = accum reg, P4 = window spec. Step window aggregate.
    WindowValue,   // P1 = accum reg, P2 = dest reg, P3 = func type. Get current window value.
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
    /// Uses Rc<str> for O(1) clone in execution loop
    pub p4: Option<Rc<str>>,

    /// Comment describing the instruction
    /// Uses Rc<str> for O(1) clone in execution loop
    pub comment: Option<Rc<str>>,
}

impl Instruction {
    /// Create a new instruction with the given opcode and parameters
    /// Converts String to Rc<str> for O(1) clone in execution loop
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
            p4: p4.map(Rc::from),
            comment: comment.map(Rc::from),
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
    pub instructions: Vec<Instruction>,
    /// Schema for the result set (built at compile time)
    pub result_schema: ResultSchema,
    /// Number of registers the compiler allocated for this program.
    ///
    /// The engine sizes its register file from this. It must not be inferred
    /// from the instruction stream: p1/p2/p3 hold literals as often as they
    /// hold register numbers (`Integer 999999 -> r` being the obvious case),
    /// so scanning for the maximum both over- and under-estimates.
    pub register_count: i64,
}

impl Default for Program {
    fn default() -> Self {
        Self::new()
    }
}

impl Program {
    pub fn new() -> Self {
        Self {
            instructions: Vec::with_capacity(DEFAULT_INSTRUCTION_CAPACITY),
            result_schema: ResultSchema::new(),
            register_count: 0,
        }
    }

    /// Set the result schema for this program
    pub fn set_result_schema(&mut self, schema: ResultSchema) {
        self.result_schema = schema;
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
            Value::String(s) => Register::String(s.into_owned()),
            Value::Boolean(b) => Register::Boolean(b),
            Value::Null => Register::Null,
        }
    }
}

impl From<Register> for Value {
    fn from(register: Register) -> Self {
        use std::borrow::Cow;
        match register {
            Register::Integer(i) => Value::Integer(i),
            Register::Float(f) => Value::Float(f),
            Register::String(s) => Value::String(Cow::Owned(s)),
            Register::Boolean(b) => Value::Boolean(b),
            Register::Null => Value::Null,
        }
    }
}
