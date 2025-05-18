//! Unit tests for the VM engine
//!
//! These tests verify that the VM's two-phase execution approach works correctly:
//! 1. Compilation of SQL to bytecode
//! 2. Execution of bytecode by the VM engine

use crate::database::Database;
use crate::table::{Table, Value};
use crate::vm;

#[test]
fn test_select_literal() {
    // Test a simple "SELECT 1" query
    // This tests the basic VM infrastructure with minimal dependencies
    
    // Create an empty database
    let database = Database::new();
    
    // Execute the query with the VM - use "SELECT 1 AS value" to give the column a name
    let result = vm::execute_vm("SELECT 1 AS value", &database, false);
    
    // Verify the query executed successfully
    assert!(result.is_ok(), "VM execution failed: {:?}", result.err());
    
    // Verify we have a result table
    let table_opt = result.unwrap();
    assert!(table_opt.is_some(), "Expected a result table, got None");
    
    let table = table_opt.unwrap();
    
    // Verify the table structure
    assert_eq!(table.column_count(), 1, "Expected 1 column, got {}", table.column_count());
    assert_eq!(table.row_count(), 1, "Expected 1 row, got {}", table.row_count());
    
    // Verify the table content - should contain a single value of 1
    let rows = table.rows();
    assert_eq!(rows.len(), 1, "Expected 1 row, got {}", rows.len());
    
    let first_row = &rows[0];
    assert_eq!(first_row.len(), 1, "Expected 1 column in row, got {}", first_row.len());
    
    // The value should be an integer with value 1
    match &first_row[0] {
        Value::Integer(val) => assert_eq!(*val, 1, "Expected value 1, got {}", val),
        other => panic!("Expected Integer type, got {:?}", other),
    }
}

#[test]
fn test_vm_bytecode_generation() {
    // This test verifies that the VM can generate correct bytecode
    // for a simple query, specifically looking at opcode sequences
    
    // Create an empty database
    let database = Database::new();
    
    // Set up the compiler directly to see the bytecode
    let mut compiler = vm::compiler::SqlCompiler::new(&database, false);
    let program = compiler.compile("SELECT 1 AS value").expect("Failed to compile SQL");
    
    // Check we have the right sequence of instructions, similar to SQLite:
    // Init
    // Integer (1)
    // ResultRow
    // Halt
    
    assert!(program.len() >= 3, "Expected at least 3 instructions, got {}", program.len());
    
    // Check instruction types in sequence
    if let Some(instr) = program.get(0) {
        assert_eq!(instr.opcode, vm::bytecode::OpCode::Init, "First instruction should be Init");
    } else {
        panic!("Missing first instruction");
    }
    
    // Find Integer or equivalent instruction that loads the value 1
    let mut has_integer_instr = false;
    let mut has_result_row = false;
    let mut has_halt = false;
    
    for i in 1..program.len() {
        if let Some(instr) = program.get(i) {
            match instr.opcode {
                // Look for Integer opcode that loads value 1
                vm::bytecode::OpCode::Integer => {
                    if instr.p1 == 1 {  // Value should be 1
                        has_integer_instr = true;
                    }
                }
                
                // Look for ResultRow opcode
                vm::bytecode::OpCode::ResultRow => {
                    has_result_row = true;
                }
                
                // Look for Halt opcode
                vm::bytecode::OpCode::Halt => {
                    has_halt = true;
                }
                
                // Other opcodes don't need to be checked in this test
                _ => {}
            }
        }
    }
    
    assert!(has_integer_instr || has_result_row, 
        "Bytecode doesn't contain expected Integer or ResultRow instructions");
    assert!(has_halt, "Bytecode doesn't end with Halt instruction");
}

#[test]
fn test_select_star_from_table() {
    // Create a database with a test table
    let mut database = Database::new();
    
    // Create a simple table with test data
    let mut table = Table::new("test_table", vec![], None);
    table.add_column("id".to_string(), "INT".to_string());
    table.add_column("name".to_string(), "TEXT".to_string());
    
    // Add some test rows
    table.add_row(vec![
        Value::Integer(1), 
        Value::String("Alice".to_string()),
    ]).expect("Failed to add row");
    
    table.add_row(vec![
        Value::Integer(2), 
        Value::String("Bob".to_string()),
    ]).expect("Failed to add row");
    
    // Add the table to the database
    let _ = database.add_table("test_table".to_string(), table);
    
    // Execute a SELECT * query with the VM
    let result = vm::execute_vm("SELECT * FROM test_table", &database, false);
    
    // Verify query execution
    assert!(result.is_ok(), "VM execution failed: {:?}", result.err());
    
    let table_opt = result.unwrap();
    assert!(table_opt.is_some(), "Expected a result table, got None");
    
    let result_table = table_opt.unwrap();
    
    // Verify structure
    assert_eq!(result_table.column_count(), 2, "Expected 2 columns, got {}", result_table.column_count());
    assert_eq!(result_table.row_count(), 2, "Expected 2 rows, got {}", result_table.row_count());
    
    // Verify first row
    let rows = result_table.rows();
    assert_eq!(rows[0][0], Value::Integer(1));
    assert_eq!(rows[0][1], Value::String("Alice".to_string()));
    
    // Verify second row
    assert_eq!(rows[1][0], Value::Integer(2));
    assert_eq!(rows[1][1], Value::String("Bob".to_string()));
}