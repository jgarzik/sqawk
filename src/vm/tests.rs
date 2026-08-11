//! Unit tests for the VM engine
//!
//! These tests verify that the VM's two-phase execution approach works correctly:
//! 1. Compilation of SQL to bytecode
//! 2. Execution of bytecode by the VM engine

use crate::database::Database;
use crate::error::SqawkResult;
use crate::table::{Table, Value};
use crate::vm::bytecode::{Instruction, OpCode, Program};
use crate::vm::engine::VmEngine;

/// Bytecode testing module
mod bytecode_tests {
    use super::*;

    /// Helper function to create a program with given instructions and execute it
    pub fn execute_bytecode_program(
        instructions: Vec<Instruction>,
        database: &Database,
    ) -> SqawkResult<Option<Table>> {
        // Create a program from the instructions
        let mut program = Program::new();
        for instruction in instructions {
            program.add_instruction(instruction);
        }

        // Execute the program
        let mut engine = VmEngine::new(database, false);
        engine.init(program);
        engine.execute()?;

        // Return the result table
        engine.create_result_table()
    }

    /// Create an instruction with the given opcode and parameters
    pub fn create_instruction(
        opcode: OpCode,
        p1: i64,
        p2: i64,
        p3: i64,
        p4: Option<String>,
        comment: Option<String>,
    ) -> Instruction {
        Instruction::new(opcode, p1, p2, p3, p4, 0, comment)
    }

    /// Test Init, Integer, ResultRow, and Halt opcodes
    #[test]
    fn test_basic_flow_opcodes() {
        // This test verifies that the basic program flow opcodes work together:
        // - Init: Initialize VM
        // - Integer: Load an integer value
        // - ResultRow: Return a result row
        // - Halt: Stop execution

        let database = Database::new();

        // Create a program that loads integer 42 into register 1 and returns it
        let instructions = vec![
            // Initialize VM and jump to instruction 1
            create_instruction(
                OpCode::Init,
                0,
                1,
                0,
                None,
                Some("Initialize VM".to_string()),
            ),
            // Load integer 42 into register 1
            create_instruction(
                OpCode::Integer,
                42,
                1,
                0,
                None,
                Some("Load value 42".to_string()),
            ),
            // Return result row with register 1
            create_instruction(
                OpCode::ResultRow,
                1,
                1,
                0,
                None,
                Some("Return result".to_string()),
            ),
            // Halt execution
            create_instruction(
                OpCode::Halt,
                0,
                0,
                0,
                None,
                Some("Stop execution".to_string()),
            ),
        ];

        // Execute the program
        let result =
            execute_bytecode_program(instructions, &database).expect("Failed to execute program");

        // Verify the result
        assert!(result.is_some(), "Expected a result table");

        let table = result.unwrap();
        assert_eq!(table.row_count(), 1, "Expected 1 row");
        assert_eq!(table.column_count(), 1, "Expected 1 column");

        let rows = table.rows();
        let first_row = &rows[0];

        match &first_row[0] {
            Value::Integer(val) => assert_eq!(*val, 42, "Expected value 42, got {}", val),
            other => panic!("Expected Integer type, got {:?}", other),
        }
    }

    /// Test String opcode
    #[test]
    fn test_string_opcode() {
        // This test verifies that the String opcode works correctly

        let database = Database::new();

        // Create a program that loads a string into register 1 and returns it
        let instructions = vec![
            // Initialize VM
            create_instruction(
                OpCode::Init,
                0,
                1,
                0,
                None,
                Some("Initialize VM".to_string()),
            ),
            // Load string "test" into register 1
            create_instruction(
                OpCode::String,
                0,
                1,
                0,
                Some("test".to_string()),
                Some("Load string value".to_string()),
            ),
            // Return result row with register 1
            create_instruction(
                OpCode::ResultRow,
                1,
                1,
                0,
                None,
                Some("Return result".to_string()),
            ),
            // Halt execution
            create_instruction(
                OpCode::Halt,
                0,
                0,
                0,
                None,
                Some("Stop execution".to_string()),
            ),
        ];

        // Execute the program
        let result =
            execute_bytecode_program(instructions, &database).expect("Failed to execute program");

        // Verify the result
        assert!(result.is_some(), "Expected a result table");

        let table = result.unwrap();
        assert_eq!(table.row_count(), 1, "Expected 1 row");
        assert_eq!(table.column_count(), 1, "Expected 1 column");

        let rows = table.rows();
        let first_row = &rows[0];

        match &first_row[0] {
            Value::String(val) => assert_eq!(val, "test", "Expected value 'test', got '{}'", val),
            other => panic!("Expected String type, got {:?}", other),
        }
    }

    /// Test Null opcode
    #[test]
    fn test_null_opcode() {
        // This test verifies that the Null opcode works correctly

        let database = Database::new();

        // Create a program that loads NULL into register 1 and returns it
        let instructions = vec![
            // Initialize VM
            create_instruction(
                OpCode::Init,
                0,
                1,
                0,
                None,
                Some("Initialize VM".to_string()),
            ),
            // Load NULL into register 1
            create_instruction(
                OpCode::Null,
                0,
                1,
                0,
                None,
                Some("Load NULL value".to_string()),
            ),
            // Return result row with register 1
            create_instruction(
                OpCode::ResultRow,
                1,
                1,
                0,
                None,
                Some("Return result".to_string()),
            ),
            // Halt execution
            create_instruction(
                OpCode::Halt,
                0,
                0,
                0,
                None,
                Some("Stop execution".to_string()),
            ),
        ];

        // Execute the program
        let result =
            execute_bytecode_program(instructions, &database).expect("Failed to execute program");

        // Verify the result
        assert!(result.is_some(), "Expected a result table");

        let table = result.unwrap();
        assert_eq!(table.row_count(), 1, "Expected 1 row");
        assert_eq!(table.column_count(), 1, "Expected 1 column");

        let rows = table.rows();
        let first_row = &rows[0];

        match &first_row[0] {
            Value::Null => {} // Success
            other => panic!("Expected Null type, got {:?}", other),
        }
    }

    /// Test ResultRow with multiple registers
    #[test]
    fn test_result_row_with_multiple_registers() {
        // This test verifies that ResultRow correctly handles multiple registers

        let database = Database::new();

        // Create a program that loads values into registers 1, 2, 3 and returns them
        let instructions = vec![
            // Initialize VM
            create_instruction(
                OpCode::Init,
                0,
                1,
                0,
                None,
                Some("Initialize VM".to_string()),
            ),
            // Load integer 42 into register 1
            create_instruction(
                OpCode::Integer,
                42,
                1,
                0,
                None,
                Some("Load value 42".to_string()),
            ),
            // Load string "test" into register 2
            create_instruction(
                OpCode::String,
                0,
                2,
                0,
                Some("test".to_string()),
                Some("Load string value".to_string()),
            ),
            // Load NULL into register 3
            create_instruction(
                OpCode::Null,
                0,
                3,
                0,
                None,
                Some("Load NULL value".to_string()),
            ),
            // Return result row with registers 1, 2, 3
            create_instruction(
                OpCode::ResultRow,
                1,
                3,
                0,
                None,
                Some("Return result".to_string()),
            ),
            // Halt execution
            create_instruction(
                OpCode::Halt,
                0,
                0,
                0,
                None,
                Some("Stop execution".to_string()),
            ),
        ];

        // Execute the program
        let result =
            execute_bytecode_program(instructions, &database).expect("Failed to execute program");

        // Verify the result
        assert!(result.is_some(), "Expected a result table");

        let table = result.unwrap();
        assert_eq!(table.row_count(), 1, "Expected 1 row");
        assert_eq!(table.column_count(), 3, "Expected 3 columns");

        let rows = table.rows();
        let first_row = &rows[0];

        // Check first column (register 1)
        match &first_row[0] {
            Value::Integer(val) => assert_eq!(*val, 42, "Expected value 42, got {}", val),
            other => panic!("Expected Integer type, got {:?}", other),
        }

        // Check second column (register 2)
        match &first_row[1] {
            Value::String(val) => assert_eq!(val, "test", "Expected value 'test', got '{}'", val),
            other => panic!("Expected String type, got {:?}", other),
        }

        // Check third column (register 3)
        match &first_row[2] {
            Value::Null => {} // Success
            other => panic!("Expected Null type, got {:?}", other),
        }
    }

    /// Test table operations: OpenRead, Rewind, Column, Next, and Close
    #[test]
    fn test_table_operations() {
        // This test verifies that table operation opcodes work correctly:
        // - OpenRead: Open a table for reading
        // - Rewind: Move cursor to first row
        // - Column: Read column value into register
        // - Next: Move cursor to next row
        // - Close: Close a cursor

        // Create a database with a test table
        let mut database = Database::new();

        // Create a simple table with test data
        let mut table = Table::new("test_table", vec![], None);
        table.add_column("id".to_string(), "INT".to_string());
        table.add_column("name".to_string(), "TEXT".to_string());

        // Add some test rows
        table
            .add_row(vec![
                Value::Integer(1),
                Value::String("Alice".to_string().into()),
            ])
            .expect("Failed to add row");

        table
            .add_row(vec![
                Value::Integer(2),
                Value::String("Bob".to_string().into()),
            ])
            .expect("Failed to add row");

        // Add the table to the database
        let _ = database.add_table("test_table".to_string(), table);

        // Create a program that reads from the table and returns both rows
        let instructions = vec![
            // Initialize VM
            create_instruction(
                OpCode::Init,
                0,
                1,
                0,
                None,
                Some("Initialize VM".to_string()),
            ),
            // Open test_table with cursor 1
            create_instruction(
                OpCode::OpenRead,
                1,
                0,
                0,
                Some("test_table".to_string()),
                Some("Open table".to_string()),
            ),
            // Rewind cursor 1, jump to end (instruction 7) if empty
            create_instruction(
                OpCode::Rewind,
                1,
                7,
                0,
                None,
                Some("Move to first row".to_string()),
            ),
            // Loop start:
            // Read column 0 (id) into register 1
            create_instruction(
                OpCode::Column,
                1,
                0,
                1,
                None,
                Some("Read ID column".to_string()),
            ),
            // Read column 1 (name) into register 2
            create_instruction(
                OpCode::Column,
                1,
                1,
                2,
                None,
                Some("Read name column".to_string()),
            ),
            // Return result row with registers 1-2
            create_instruction(
                OpCode::ResultRow,
                1,
                2,
                0,
                None,
                Some("Return result".to_string()),
            ),
            // Next row for cursor 1, jump to loop start (instruction 3) if more rows
            create_instruction(
                OpCode::Next,
                1,
                3,
                0,
                None,
                Some("Move to next row".to_string()),
            ),
            // Close cursor 1
            create_instruction(
                OpCode::Close,
                1,
                0,
                0,
                None,
                Some("Close cursor".to_string()),
            ),
            // Halt execution
            create_instruction(
                OpCode::Halt,
                0,
                0,
                0,
                None,
                Some("Stop execution".to_string()),
            ),
        ];

        // Execute the program
        let result =
            execute_bytecode_program(instructions, &database).expect("Failed to execute program");

        // Verify the result
        assert!(result.is_some(), "Expected a result table");

        let table = result.unwrap();
        assert_eq!(table.row_count(), 2, "Expected 2 rows");
        assert_eq!(table.column_count(), 2, "Expected 2 columns");

        let rows = table.rows();

        // Check first row
        assert_eq!(
            rows[0][0],
            Value::Integer(1),
            "First row, first column should be 1"
        );
        assert_eq!(
            rows[0][1],
            Value::String("Alice".to_string().into()),
            "First row, second column should be 'Alice'"
        );

        // Check second row
        assert_eq!(
            rows[1][0],
            Value::Integer(2),
            "Second row, first column should be 2"
        );
        assert_eq!(
            rows[1][1],
            Value::String("Bob".to_string().into()),
            "Second row, second column should be 'Bob'"
        );
    }

    /// Test the Goto operation
    #[test]
    fn test_goto_opcode() {
        // This test verifies that the Goto opcode works correctly for program flow control

        let database = Database::new();

        // Create a program with a jump in the middle
        let instructions = vec![
            // Initialize VM, jump to instruction 1
            create_instruction(
                OpCode::Init,
                0,
                1,
                0,
                None,
                Some("Initialize VM".to_string()),
            ),
            // Load integer 10 into register 1
            create_instruction(
                OpCode::Integer,
                10,
                1,
                0,
                None,
                Some("Load value 10".to_string()),
            ),
            // Jump unconditionally to instruction 4, skipping the next instruction
            create_instruction(
                OpCode::Goto,
                0,
                4,
                0,
                None,
                Some("Jump to instruction 4".to_string()),
            ),
            // Load integer 20 into register 1 (should be skipped)
            create_instruction(
                OpCode::Integer,
                20,
                1,
                0,
                None,
                Some("Load value 20 (should be skipped)".to_string()),
            ),
            // Return result row with register 1
            create_instruction(
                OpCode::ResultRow,
                1,
                1,
                0,
                None,
                Some("Return result".to_string()),
            ),
            // Halt execution
            create_instruction(
                OpCode::Halt,
                0,
                0,
                0,
                None,
                Some("Stop execution".to_string()),
            ),
        ];

        // Execute the program
        let result =
            execute_bytecode_program(instructions, &database).expect("Failed to execute program");

        // Verify the result
        assert!(result.is_some(), "Expected a result table");

        let table = result.unwrap();
        assert_eq!(table.row_count(), 1, "Expected 1 row");
        assert_eq!(table.column_count(), 1, "Expected 1 column");

        let rows = table.rows();
        let first_row = &rows[0];

        match &first_row[0] {
            Value::Integer(val) => assert_eq!(*val, 10, "Expected value 10 (not 20), got {}", val),
            other => panic!("Expected Integer type, got {:?}", other),
        }
    }

    /// Test multiple result rows
    #[test]
    fn test_multiple_result_rows() {
        // This test verifies that the VM can produce multiple result rows

        let database = Database::new();

        // Create a program that generates multiple result rows
        let instructions = vec![
            // Initialize VM
            create_instruction(
                OpCode::Init,
                0,
                1,
                0,
                None,
                Some("Initialize VM".to_string()),
            ),
            // Load integer 1 into register 1
            create_instruction(
                OpCode::Integer,
                1,
                1,
                0,
                None,
                Some("Load value 1".to_string()),
            ),
            // Return first result row
            create_instruction(
                OpCode::ResultRow,
                1,
                1,
                0,
                None,
                Some("Return first result".to_string()),
            ),
            // Load integer 2 into register 1
            create_instruction(
                OpCode::Integer,
                2,
                1,
                0,
                None,
                Some("Load value 2".to_string()),
            ),
            // Return second result row
            create_instruction(
                OpCode::ResultRow,
                1,
                1,
                0,
                None,
                Some("Return second result".to_string()),
            ),
            // Load integer 3 into register 1
            create_instruction(
                OpCode::Integer,
                3,
                1,
                0,
                None,
                Some("Load value 3".to_string()),
            ),
            // Return third result row
            create_instruction(
                OpCode::ResultRow,
                1,
                1,
                0,
                None,
                Some("Return third result".to_string()),
            ),
            // Halt execution
            create_instruction(
                OpCode::Halt,
                0,
                0,
                0,
                None,
                Some("Stop execution".to_string()),
            ),
        ];

        // Execute the program
        let result =
            execute_bytecode_program(instructions, &database).expect("Failed to execute program");

        // Verify the result
        assert!(result.is_some(), "Expected a result table");

        let table = result.unwrap();
        assert_eq!(table.row_count(), 3, "Expected 3 rows");
        assert_eq!(table.column_count(), 1, "Expected 1 column");

        let rows = table.rows();

        // Check first row
        match &rows[0][0] {
            Value::Integer(val) => assert_eq!(*val, 1, "Expected value 1, got {}", val),
            other => panic!("Expected Integer type, got {:?}", other),
        }

        // Check second row
        match &rows[1][0] {
            Value::Integer(val) => assert_eq!(*val, 2, "Expected value 2, got {}", val),
            other => panic!("Expected Integer type, got {:?}", other),
        }

        // Check third row
        match &rows[2][0] {
            Value::Integer(val) => assert_eq!(*val, 3, "Expected value 3, got {}", val),
            other => panic!("Expected Integer type, got {:?}", other),
        }
    }

    /// Test transaction opcodes (Begin, Commit, Rollback)
    #[test]
    fn test_transaction_opcodes() {
        // This test verifies that the transaction opcodes work correctly
        let database = Database::new();

        // Create a program that uses transaction opcodes
        let instructions = vec![
            // Initialize VM
            create_instruction(
                OpCode::Init,
                0,
                1,
                0,
                None,
                Some("Initialize VM".to_string()),
            ),
            // Begin transaction
            create_instruction(
                OpCode::Begin,
                0,
                0,
                0,
                None,
                Some("Begin transaction".to_string()),
            ),
            // Load integer 42 into register 1
            create_instruction(
                OpCode::Integer,
                42,
                1,
                0,
                None,
                Some("Load value 42".to_string()),
            ),
            // Commit transaction
            create_instruction(
                OpCode::Commit,
                0,
                0,
                0,
                None,
                Some("Commit transaction".to_string()),
            ),
            // Return result row with register 1
            create_instruction(
                OpCode::ResultRow,
                1,
                1,
                0,
                None,
                Some("Return result".to_string()),
            ),
            // Halt execution
            create_instruction(
                OpCode::Halt,
                0,
                0,
                0,
                None,
                Some("Stop execution".to_string()),
            ),
        ];

        // Execute the program
        let result = execute_bytecode_program(instructions, &database)
            .expect("Failed to execute program with Begin/Commit");

        // Verify the result
        assert!(result.is_some(), "Expected a result table");
        let table = result.unwrap();
        assert_eq!(table.row_count(), 1, "Expected 1 row");
        assert_eq!(table.column_count(), 1, "Expected 1 column");

        // Check the value
        let rows = table.rows();
        let first_row = &rows[0];
        match &first_row[0] {
            Value::Integer(val) => assert_eq!(*val, 42, "Expected value 42, got {}", val),
            other => panic!("Expected Integer type, got {:?}", other),
        }

        // Test rollback
        let instructions = vec![
            // Initialize VM
            create_instruction(
                OpCode::Init,
                0,
                1,
                0,
                None,
                Some("Initialize VM".to_string()),
            ),
            // Begin transaction
            create_instruction(
                OpCode::Begin,
                0,
                0,
                0,
                None,
                Some("Begin transaction".to_string()),
            ),
            // Load integer 42 into register 1
            create_instruction(
                OpCode::Integer,
                42,
                1,
                0,
                None,
                Some("Load value in transaction".to_string()),
            ),
            // Rollback transaction
            create_instruction(
                OpCode::Rollback,
                0,
                0,
                0,
                None,
                Some("Rollback transaction".to_string()),
            ),
            // Load integer 99 into register 1 (after rollback)
            create_instruction(
                OpCode::Integer,
                99,
                1,
                0,
                None,
                Some("Load value after rollback".to_string()),
            ),
            // Return result row with register 1
            create_instruction(
                OpCode::ResultRow,
                1,
                1,
                0,
                None,
                Some("Return result".to_string()),
            ),
            // Halt execution
            create_instruction(
                OpCode::Halt,
                0,
                0,
                0,
                None,
                Some("Stop execution".to_string()),
            ),
        ];

        // Execute the program
        let result = execute_bytecode_program(instructions, &database)
            .expect("Failed to execute program with Begin/Rollback");

        // Verify the result
        assert!(result.is_some(), "Expected a result table");
        let table = result.unwrap();
        assert_eq!(table.row_count(), 1, "Expected 1 row");
        assert_eq!(table.column_count(), 1, "Expected 1 column");

        // Check that we got the post-rollback value
        let rows = table.rows();
        let first_row = &rows[0];
        match &first_row[0] {
            Value::Integer(val) => {
                assert_eq!(*val, 99, "Expected value 99 (after rollback), got {}", val)
            }
            other => panic!("Expected Integer type, got {:?}", other),
        }
    }
}

// The compiler tests and other VM tests have been removed to simplify
// our implementation and focus on the transaction tests.

/// Test comparison opcodes and conditional jumps
mod comparison_tests {
    use super::*;
    use crate::vm::tests::bytecode_tests::{create_instruction, execute_bytecode_program};

    /// Test the Lt (Less Than) comparison opcode
    #[test]
    fn test_lt_opcode() {
        let database = Database::new();

        // Test cases: [(first_value, second_value, expected_result)]
        let test_cases = vec![
            (10, 20, 1), // 10 < 20 = true (1)
            (20, 10, 0), // 20 < 10 = false (0)
            (10, 10, 0), // 10 < 10 = false (0)
        ];

        for (val1, val2, expected) in test_cases {
            let instructions = vec![
                // Initialize VM
                create_instruction(
                    OpCode::Init,
                    0,
                    1,
                    0,
                    None,
                    Some("Initialize VM".to_string()),
                ),
                // Load first value into register 1
                create_instruction(
                    OpCode::Integer,
                    val1,
                    1,
                    0,
                    None,
                    Some(format!("Load first value: {}", val1)),
                ),
                // Load second value into register 2
                create_instruction(
                    OpCode::Integer,
                    val2,
                    2,
                    0,
                    None,
                    Some(format!("Load second value: {}", val2)),
                ),
                // Compare r1 < r2, result in r3
                create_instruction(
                    OpCode::Lt,
                    1,
                    2,
                    3,
                    None,
                    Some(format!("Compare {} < {}", val1, val2)),
                ),
                // Return the comparison result
                create_instruction(
                    OpCode::ResultRow,
                    3,
                    1,
                    0,
                    None,
                    Some("Return result".to_string()),
                ),
                // Halt VM
                create_instruction(
                    OpCode::Halt,
                    0,
                    0,
                    0,
                    None,
                    Some("Stop execution".to_string()),
                ),
            ];

            // Execute the program
            let result = execute_bytecode_program(instructions, &database)
                .expect("Failed to execute program with Lt opcode");

            // Verify the result
            assert!(result.is_some(), "Expected a result table");
            let table = result.unwrap();
            assert_eq!(table.row_count(), 1, "Expected 1 row");

            // Check the result value
            match &table.rows()[0][0] {
                Value::Integer(val) => assert_eq!(
                    *val, expected as i64,
                    "Expected {} < {} to be {}, got {}",
                    val1, val2, expected, val
                ),
                other => panic!("Expected Integer type, got {:?}", other),
            }
        }
    }

    /// Test the Le (Less Than or Equal) comparison opcode
    #[test]
    fn test_le_opcode() {
        let database = Database::new();

        // Test cases: [(first_value, second_value, expected_result)]
        let test_cases = vec![
            (10, 20, 1), // 10 <= 20 = true (1)
            (20, 10, 0), // 20 <= 10 = false (0)
            (10, 10, 1), // 10 <= 10 = true (1)
        ];

        for (val1, val2, expected) in test_cases {
            let instructions = vec![
                // Initialize VM
                create_instruction(
                    OpCode::Init,
                    0,
                    1,
                    0,
                    None,
                    Some("Initialize VM".to_string()),
                ),
                // Load first value into register 1
                create_instruction(
                    OpCode::Integer,
                    val1,
                    1,
                    0,
                    None,
                    Some(format!("Load first value: {}", val1)),
                ),
                // Load second value into register 2
                create_instruction(
                    OpCode::Integer,
                    val2,
                    2,
                    0,
                    None,
                    Some(format!("Load second value: {}", val2)),
                ),
                // Compare r1 <= r2, result in r3
                create_instruction(
                    OpCode::Le,
                    1,
                    2,
                    3,
                    None,
                    Some(format!("Compare {} <= {}", val1, val2)),
                ),
                // Return the comparison result
                create_instruction(
                    OpCode::ResultRow,
                    3,
                    1,
                    0,
                    None,
                    Some("Return result".to_string()),
                ),
                // Halt VM
                create_instruction(
                    OpCode::Halt,
                    0,
                    0,
                    0,
                    None,
                    Some("Stop execution".to_string()),
                ),
            ];

            // Execute the program
            let result = execute_bytecode_program(instructions, &database)
                .expect("Failed to execute program with Le opcode");

            // Verify the result
            assert!(result.is_some(), "Expected a result table");
            let table = result.unwrap();
            assert_eq!(table.row_count(), 1, "Expected 1 row");

            // Check the result value
            match &table.rows()[0][0] {
                Value::Integer(val) => assert_eq!(
                    *val, expected as i64,
                    "Expected {} <= {} to be {}, got {}",
                    val1, val2, expected, val
                ),
                other => panic!("Expected Integer type, got {:?}", other),
            }
        }
    }

    /// Test conditional jump using SQLite-style jump mechanism
    #[test]
    fn test_conditional_jump() {
        let database = Database::new();

        // Test cases for conditional jumps
        // (first_value, second_value, expected_result_after_jump)
        let test_cases = vec![
            (10, 20, 555), // 10 < 20 = true, result = 1, so NOT Jump (555)
            (20, 10, 999), // 20 < 10 = false, result = 0, so Jump (999)
        ];

        for (val1, val2, expected) in test_cases {
            let instructions = vec![
                // Initialize VM
                create_instruction(
                    OpCode::Init,
                    0,
                    1,
                    0,
                    None,
                    Some("Initialize VM".to_string()),
                ),
                // Load first value into register 1
                create_instruction(
                    OpCode::Integer,
                    val1,
                    1,
                    0,
                    None,
                    Some(format!("Load value {}", val1)),
                ),
                // Load second value into register 2
                create_instruction(
                    OpCode::Integer,
                    val2,
                    2,
                    0,
                    None,
                    Some(format!("Load value {}", val2)),
                ),
                // Compare register 1 < register 2, result in register 3
                create_instruction(
                    OpCode::Lt,
                    1,
                    2,
                    3,
                    None,
                    Some(format!("Compare {} < {}", val1, val2)),
                ),
                // Jump to instruction 7 if reg3 contains 0 (SQLite-style jump if zero)
                create_instruction(
                    OpCode::IfZ,
                    3,
                    7,
                    0,
                    None,
                    Some("Jump to instruction 7 if result is 0 (false)".to_string()),
                ),
                // True path (Not jumped) - load 555 into register 4
                create_instruction(
                    OpCode::Integer,
                    555,
                    4,
                    0,
                    None,
                    Some("Load true path value (555)".to_string()),
                ),
                // Jump to instruction 8 (end)
                create_instruction(OpCode::Goto, 0, 8, 0, None, Some("Jump to end".to_string())),
                // False path (Jumped) - load 999 into register 4
                create_instruction(
                    OpCode::Integer,
                    999,
                    4,
                    0,
                    None,
                    Some("Load false path value (999)".to_string()),
                ),
                // Return result from register 4
                create_instruction(
                    OpCode::ResultRow,
                    4,
                    1,
                    0,
                    None,
                    Some("Return result".to_string()),
                ),
                // Halt execution
                create_instruction(
                    OpCode::Halt,
                    0,
                    0,
                    0,
                    None,
                    Some("Stop execution".to_string()),
                ),
            ];

            // Execute the program
            let result = execute_bytecode_program(instructions, &database)
                .expect("Failed to execute program with conditional jump");

            // Verify the result
            assert!(result.is_some(), "Expected a result table");
            let table = result.unwrap();
            assert_eq!(table.row_count(), 1, "Expected 1 row");

            // Check the conditional jump outcome
            match &table.rows()[0][0] {
                Value::Integer(val) => assert_eq!(
                    *val, expected as i64,
                    "Expected {} for comparison {} < {}, got {}",
                    expected, val1, val2, val
                ),
                other => panic!("Expected Integer type, got {:?}", other),
            }
        }
    }

    /// Test the IfPos conditional jump opcode
    #[test]
    fn test_if_pos_opcode() {
        let database = Database::new();

        // Test cases: [(register_value, expected_result_after_jump)]
        let test_cases = vec![
            (5, 777),  // 5 > 0 = true, should jump to 777
            (0, 888),  // 0 = 0, should NOT jump, continue to 888
            (-3, 888), // -3 < 0, should NOT jump, continue to 888
        ];

        for (reg_value, expected) in test_cases {
            let instructions = vec![
                // Initialize VM
                create_instruction(
                    OpCode::Init,
                    0,
                    1,
                    0,
                    None,
                    Some("Initialize VM".to_string()),
                ),
                // Load test value into register 1
                create_instruction(
                    OpCode::Integer,
                    reg_value,
                    1,
                    0,
                    None,
                    Some(format!("Load test value {}", reg_value)),
                ),
                // Jump to instruction 5 if reg1 is positive (> 0)
                create_instruction(
                    OpCode::IfPos,
                    1,
                    5,
                    0,
                    None,
                    Some("Jump if positive".to_string()),
                ),
                // Not jumped path - load 888 into register 2
                create_instruction(
                    OpCode::Integer,
                    888,
                    2,
                    0,
                    None,
                    Some("Load not-jumped value (888)".to_string()),
                ),
                // Jump to end
                create_instruction(OpCode::Goto, 0, 6, 0, None, Some("Jump to end".to_string())),
                // Jumped path - load 777 into register 2
                create_instruction(
                    OpCode::Integer,
                    777,
                    2,
                    0,
                    None,
                    Some("Load jumped value (777)".to_string()),
                ),
                // Return result from register 2
                create_instruction(
                    OpCode::ResultRow,
                    2,
                    1,
                    0,
                    None,
                    Some("Return result".to_string()),
                ),
                // Halt execution
                create_instruction(
                    OpCode::Halt,
                    0,
                    0,
                    0,
                    None,
                    Some("Stop execution".to_string()),
                ),
            ];

            // Execute the program
            let result = execute_bytecode_program(instructions, &database)
                .expect("Failed to execute program with IfPos");

            // Verify the result
            assert!(result.is_some(), "Expected a result table");
            let table = result.unwrap();
            assert_eq!(table.row_count(), 1, "Expected 1 row");

            // Check the conditional jump outcome
            match &table.rows()[0][0] {
                Value::Integer(val) => assert_eq!(
                    *val, expected as i64,
                    "Expected {} for IfPos test with value {}, got {}",
                    expected, reg_value, val
                ),
                other => panic!("Expected Integer type, got {:?}", other),
            }
        }
    }

    /// Test the IfNeg conditional jump opcode
    #[test]
    fn test_if_neg_opcode() {
        let database = Database::new();

        // Test cases: [(register_value, expected_result_after_jump)]
        let test_cases = vec![
            (-5, 333), // -5 < 0 = true, should jump to 333
            (0, 444),  // 0 = 0, should NOT jump, continue to 444
            (3, 444),  // 3 > 0, should NOT jump, continue to 444
        ];

        for (reg_value, expected) in test_cases {
            let instructions = vec![
                // Initialize VM
                create_instruction(
                    OpCode::Init,
                    0,
                    1,
                    0,
                    None,
                    Some("Initialize VM".to_string()),
                ),
                // Load test value into register 1
                create_instruction(
                    OpCode::Integer,
                    reg_value,
                    1,
                    0,
                    None,
                    Some(format!("Load test value {}", reg_value)),
                ),
                // Jump to instruction 5 if reg1 is negative (< 0)
                create_instruction(
                    OpCode::IfNeg,
                    1,
                    5,
                    0,
                    None,
                    Some("Jump if negative".to_string()),
                ),
                // Not jumped path - load 444 into register 2
                create_instruction(
                    OpCode::Integer,
                    444,
                    2,
                    0,
                    None,
                    Some("Load not-jumped value (444)".to_string()),
                ),
                // Jump to end
                create_instruction(OpCode::Goto, 0, 6, 0, None, Some("Jump to end".to_string())),
                // Jumped path - load 333 into register 2
                create_instruction(
                    OpCode::Integer,
                    333,
                    2,
                    0,
                    None,
                    Some("Load jumped value (333)".to_string()),
                ),
                // Return result from register 2
                create_instruction(
                    OpCode::ResultRow,
                    2,
                    1,
                    0,
                    None,
                    Some("Return result".to_string()),
                ),
                // Halt execution
                create_instruction(
                    OpCode::Halt,
                    0,
                    0,
                    0,
                    None,
                    Some("Stop execution".to_string()),
                ),
            ];

            // Execute the program
            let result = execute_bytecode_program(instructions, &database)
                .expect("Failed to execute program with IfNeg");

            // Verify the result
            assert!(result.is_some(), "Expected a result table");
            let table = result.unwrap();
            assert_eq!(table.row_count(), 1, "Expected 1 row");

            // Check the conditional jump outcome
            match &table.rows()[0][0] {
                Value::Integer(val) => assert_eq!(
                    *val, expected as i64,
                    "Expected {} for IfNeg test with value {}, got {}",
                    expected, reg_value, val
                ),
                other => panic!("Expected Integer type, got {:?}", other),
            }
        }
    }

    /// Test Column opcode with simple cursor operations
    #[test]
    fn test_column_opcode_basic() {
        use crate::database::Database;
        use crate::table::Table;

        // Create a simple test database
        let mut database = Database::new();
        let mut test_table = Table::new("simple_test", vec![], None);

        // Add columns: id, value
        test_table.add_column("id".to_string(), "INTEGER".to_string());
        test_table.add_column("value".to_string(), "INTEGER".to_string());

        // Add one test row
        test_table
            .add_row(vec![
                crate::table::Value::Integer(1),
                crate::table::Value::Integer(42),
            ])
            .expect("Failed to add test row");

        database
            .add_table("simple_test".to_string(), test_table)
            .expect("Failed to add table");

        // Simple test: Read the value column (index 1)
        let instructions = vec![
            create_instruction(OpCode::Init, 0, 1, 0, None, Some("Initialize".to_string())),
            create_instruction(
                OpCode::OpenRead,
                0,
                0,
                0,
                Some("simple_test".to_string()),
                Some("Open table".to_string()),
            ),
            create_instruction(OpCode::Rewind, 0, 5, 0, None, Some("Rewind".to_string())),
            create_instruction(
                OpCode::Column,
                0,
                1,
                1,
                None,
                Some("Read value column".to_string()),
            ),
            create_instruction(
                OpCode::ResultRow,
                1,
                1,
                0,
                None,
                Some("Output value".to_string()),
            ),
            create_instruction(OpCode::Halt, 0, 0, 0, None, Some("Stop".to_string())),
        ];

        let result = execute_bytecode_program(instructions, &database)
            .expect("Failed to execute column test");

        // Verify we got the expected value
        assert!(result.is_some(), "Expected a result table");
        let table = result.unwrap();
        assert_eq!(table.row_count(), 1, "Expected 1 row");

        match &table.rows()[0][0] {
            crate::table::Value::Integer(value) => {
                assert_eq!(*value, 42, "Expected value 42, got {}", value);
            }
            other => panic!("Expected Integer value, got {:?}", other),
        }
    }

    /// Test comparison operations with Column data
    #[test]
    fn test_column_with_comparison() {
        use crate::database::Database;
        use crate::table::Table;

        // Create test database with two values: one matches, one doesn't
        let mut database = Database::new();
        let mut test_table = Table::new("comparison_test", vec![], None);

        test_table.add_column("id".to_string(), "INTEGER".to_string());
        test_table.add_column("score".to_string(), "INTEGER".to_string());

        // Add two rows: score 25 (should not match > 30), score 35 (should match > 30)
        test_table
            .add_row(vec![
                crate::table::Value::Integer(1),
                crate::table::Value::Integer(25),
            ])
            .expect("Failed to add test row");

        test_table
            .add_row(vec![
                crate::table::Value::Integer(2),
                crate::table::Value::Integer(35),
            ])
            .expect("Failed to add test row");

        database
            .add_table("comparison_test".to_string(), test_table)
            .expect("Failed to add table");

        // Test first row (score = 25, should fail > 30 test)
        let instructions_row1 = vec![
            create_instruction(OpCode::Init, 0, 1, 0, None, Some("Initialize".to_string())),
            create_instruction(
                OpCode::OpenRead,
                0,
                0,
                0,
                Some("comparison_test".to_string()),
                Some("Open table".to_string()),
            ),
            create_instruction(OpCode::Rewind, 0, 7, 0, None, Some("Rewind".to_string())),
            create_instruction(
                OpCode::Column,
                0,
                1,
                1,
                None,
                Some("Read score".to_string()),
            ),
            create_instruction(OpCode::Integer, 30, 2, 0, None, Some("Load 30".to_string())),
            create_instruction(
                OpCode::Gt,
                1,
                2,
                3,
                None,
                Some("Test score > 30".to_string()),
            ),
            create_instruction(
                OpCode::ResultRow,
                3,
                1,
                0,
                None,
                Some("Output comparison result".to_string()),
            ),
            create_instruction(OpCode::Halt, 0, 0, 0, None, Some("Stop".to_string())),
        ];

        let result = execute_bytecode_program(instructions_row1, &database)
            .expect("Failed to execute comparison test");

        assert!(result.is_some(), "Expected a result");
        let table = result.unwrap();
        assert_eq!(table.row_count(), 1, "Expected 1 result row");

        // Should be 0 (false) since 25 > 30 is false
        match &table.rows()[0][0] {
            crate::table::Value::Integer(result_val) => {
                assert_eq!(
                    *result_val, 0,
                    "Expected comparison result 0 (false), got {}",
                    result_val
                );
            }
            other => panic!("Expected Integer comparison result, got {:?}", other),
        }
    }
}

/// Tests for the Not / Compare / Jump opcodes.
///
/// These exist to support the compiler unification: `Not` makes `NOT <expr>`
/// expressible and replaces several hand-rolled inversions, while
/// `Compare`/`Jump` make multi-column key comparison two instructions
/// regardless of key width -- which is what GROUP BY needs in order to detect
/// a group change across ALL key columns instead of only the first.
mod compare_jump_not_tests {
    use super::bytecode_tests::{create_instruction, execute_bytecode_program};
    use super::*;

    /// Run a program and return the single result row.
    fn run_row(instructions: Vec<Instruction>) -> Vec<Value> {
        let database = Database::new();
        let table = execute_bytecode_program(instructions, &database)
            .expect("program failed")
            .expect("expected a result table");
        assert_eq!(table.row_count(), 1, "expected exactly one result row");
        table.rows()[0].clone()
    }

    fn init() -> Instruction {
        create_instruction(OpCode::Init, 0, 1, 0, None, None)
    }

    fn halt() -> Instruction {
        create_instruction(OpCode::Halt, 0, 0, 0, None, None)
    }

    #[test]
    fn not_inverts_truthy_and_falsy() {
        // r1 = 5 -> NOT r1 = 0 ; r3 = 0 -> NOT r3 = 1
        let row = run_row(vec![
            init(),
            create_instruction(OpCode::Integer, 5, 1, 0, None, None),
            create_instruction(OpCode::Not, 1, 2, 0, None, None),
            create_instruction(OpCode::Integer, 0, 3, 0, None, None),
            create_instruction(OpCode::Not, 3, 4, 0, None, None),
            // Emit r2, r4 as a two-column row (registers must be contiguous).
            create_instruction(OpCode::Copy, 2, 10, 0, None, None),
            create_instruction(OpCode::Copy, 4, 11, 0, None, None),
            create_instruction(OpCode::ResultRow, 10, 2, 0, None, None),
            halt(),
        ]);
        assert_eq!(row[0], Value::Integer(0), "NOT 5 should be 0");
        assert_eq!(row[1], Value::Integer(1), "NOT 0 should be 1");
    }

    #[test]
    fn not_propagates_null() {
        // NOT NULL must be NULL (SQL UNKNOWN), not true.
        let row = run_row(vec![
            init(),
            create_instruction(OpCode::Null, 0, 1, 0, None, None),
            create_instruction(OpCode::Not, 1, 2, 0, None, None),
            create_instruction(OpCode::ResultRow, 2, 1, 0, None, None),
            halt(),
        ]);
        assert_eq!(
            row[0],
            Value::Null,
            "NOT NULL must stay NULL, not become true"
        );
    }

    /// Build a program that compares two 2-register vectors and reports which
    /// branch Jump takes: 1 = Less, 2 = Equal, 3 = Greater.
    fn compare_branch(lhs: [i64; 2], rhs: [i64; 2]) -> Value {
        let row = run_row(vec![
            init(),
            // lhs in r1,r2 ; rhs in r3,r4
            create_instruction(OpCode::Integer, lhs[0], 1, 0, None, None),
            create_instruction(OpCode::Integer, lhs[1], 2, 0, None, None),
            create_instruction(OpCode::Integer, rhs[0], 3, 0, None, None),
            create_instruction(OpCode::Integer, rhs[1], 4, 0, None, None),
            // Compare r1..r2 against r3..r4 (2 columns)
            create_instruction(OpCode::Compare, 1, 3, 2, None, None),
            // Jump: less -> 7, equal -> 9, greater -> 11
            create_instruction(OpCode::Jump, 7, 9, 11, None, None),
            // 7: less
            create_instruction(OpCode::Integer, 1, 5, 0, None, None),
            create_instruction(OpCode::Goto, 0, 13, 0, None, None),
            // 9: equal
            create_instruction(OpCode::Integer, 2, 5, 0, None, None),
            create_instruction(OpCode::Goto, 0, 13, 0, None, None),
            // 11: greater
            create_instruction(OpCode::Integer, 3, 5, 0, None, None),
            create_instruction(OpCode::Goto, 0, 13, 0, None, None),
            // 13: emit
            create_instruction(OpCode::ResultRow, 5, 1, 0, None, None),
            halt(),
        ]);
        row[0].clone()
    }

    #[test]
    fn compare_jump_branches_three_ways() {
        assert_eq!(compare_branch([1, 1], [1, 1]), Value::Integer(2), "equal");
        assert_eq!(compare_branch([1, 1], [1, 2]), Value::Integer(1), "less");
        assert_eq!(compare_branch([1, 2], [1, 1]), Value::Integer(3), "greater");
    }

    #[test]
    fn compare_uses_all_key_columns() {
        // The whole point: a difference in the SECOND column must be detected.
        // The old single-Ne group-change check compared only the first, which
        // is why multi-column GROUP BY collapsed to one key.
        assert_ne!(
            compare_branch([7, 1], [7, 2]),
            Value::Integer(2),
            "vectors differing only in the second column must not compare equal"
        );
    }

    #[test]
    fn compare_treats_nulls_as_equal() {
        // Internal ordering, not SQL `=`: two NULL group keys must group
        // together. SQL equality must yield UNKNOWN for NULL, which is exactly
        // why Compare is a separate opcode from Eq.
        let row = run_row(vec![
            init(),
            create_instruction(OpCode::Null, 0, 1, 0, None, None),
            create_instruction(OpCode::Null, 0, 2, 0, None, None),
            create_instruction(OpCode::Compare, 1, 2, 1, None, None),
            create_instruction(OpCode::Jump, 5, 7, 9, None, None),
            // 5: less
            create_instruction(OpCode::Integer, 1, 3, 0, None, None),
            create_instruction(OpCode::Goto, 0, 11, 0, None, None),
            // 7: equal
            create_instruction(OpCode::Integer, 2, 3, 0, None, None),
            create_instruction(OpCode::Goto, 0, 11, 0, None, None),
            // 9: greater
            create_instruction(OpCode::Integer, 3, 3, 0, None, None),
            create_instruction(OpCode::Goto, 0, 11, 0, None, None),
            // 11: emit
            create_instruction(OpCode::ResultRow, 3, 1, 0, None, None),
            halt(),
        ]);
        assert_eq!(
            row[0],
            Value::Integer(2),
            "NULL should compare equal to NULL"
        );
    }
}
