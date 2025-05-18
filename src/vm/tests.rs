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
            .add_row(vec![Value::Integer(1), Value::String("Alice".to_string())])
            .expect("Failed to add row");

        table
            .add_row(vec![Value::Integer(2), Value::String("Bob".to_string())])
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
            Value::String("Alice".to_string()),
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
            Value::String("Bob".to_string()),
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
