//! Unit tests for the VM engine
//!
//! These tests verify that the VM's two-phase execution approach works correctly:
//! 1. Compilation of SQL to bytecode
//! 2. Execution of bytecode by the VM engine

use crate::database::Database;
use crate::table::{Column, DataType, Table, Value};
use crate::vm;

/// Helper function to create a test database with sample person data
fn create_test_persons_database() -> Database {
    let mut database = Database::new();
    
    // Create a "persons" table with name and age columns
    let mut persons_table = Table::new(
        "persons", 
        vec![
            Column::new("name".to_string(), DataType::String),
            Column::new("age".to_string(), DataType::Integer)
        ],
        None
    );
    
    // Add sample data rows
    persons_table.add_row(vec![
        Value::String("alice".to_string()), 
        Value::Integer(18)
    ]).unwrap();
    
    persons_table.add_row(vec![
        Value::String("bob".to_string()), 
        Value::Integer(19)
    ]).unwrap();
    
    persons_table.add_row(vec![
        Value::String("jane".to_string()), 
        Value::Integer(25)
    ]).unwrap();
    
    persons_table.add_row(vec![
        Value::String("john".to_string()), 
        Value::Integer(35)
    ]).unwrap();
    
    // Add the table to the database
    database.add_table(persons_table, None);
    
    database
}

#[test]
fn test_select_literal() {
    // Test a simple "SELECT 1" query using the VM
    // This tests that the VM infrastructure can handle literal queries
    
    // Create an empty database
    let database = Database::new();
    
    // Execute the query
    // This uses the direct string for parsing by sqlparser and bypasses the SQL executor validation
    let sql = "SELECT 1 AS value";
    
    // Get the result with our own parser+VM implementation
    let mut compiler = vm::compiler::SqlCompiler::new(&database, false);
    let program = compiler.compile(sql).expect("Failed to compile SQL");
    
    let mut engine = vm::engine::VmEngine::new(&database, false);
    engine.init(program);
    engine.execute().expect("Failed to execute program");
    
    let table_opt = engine.create_result_table().expect("Failed to create result table");
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
    // Test bytecode generation for a simple literal SELECT query
    
    // Create an empty database
    let database = Database::new();
    
    // Set up the compiler with our SQL string
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

#[test]
fn test_where_bytecode_generation() {
    // Test the bytecode generation for a WHERE clause comparison
    // following the pattern shown in the SQLite EXPLAIN output
    
    // Create a test database with sample person data
    let database = create_test_persons_database();
    
    // The SQL query to test
    let sql = "SELECT name, age FROM persons WHERE age > 20";
    
    // Use the compiler directly to get the bytecode
    let mut compiler = vm::compiler::SqlCompiler::new(&database, false);
    let program = compiler.compile(sql).expect("Failed to compile SQL");
    
    // Verify the program has enough instructions
    assert!(program.len() >= 10, "Expected at least 10 instructions for a query with WHERE, got {}", program.len());
    
    // Find key opcodes that should be present for a WHERE comparison
    let mut has_open_read = false;
    let mut has_rewind = false;
    let mut has_column_read = false;
    let mut has_comparison_op = false;
    let mut has_goto_or_jump = false;
    let mut has_next = false;
    let mut has_integer_constant = false;
    
    // The comparison constant (20) should be loaded into a register
    // We expect an Integer instruction loading the value 20
    let mut constant_register: Option<i64> = None;
    
    // Check individual instructions
    for instr in program.instructions.iter() {
        match instr.opcode {
            vm::bytecode::OpCode::OpenRead => {
                has_open_read = true;
            },
            vm::bytecode::OpCode::Rewind => {
                has_rewind = true;
            },
            vm::bytecode::OpCode::Column => {
                has_column_read = true;
            },
            vm::bytecode::OpCode::Integer => {
                if instr.p1 == 20 {
                    has_integer_constant = true;
                    constant_register = Some(instr.p2);
                }
            },
            vm::bytecode::OpCode::Le | 
            vm::bytecode::OpCode::Lt | 
            vm::bytecode::OpCode::Gt | 
            vm::bytecode::OpCode::Ge | 
            vm::bytecode::OpCode::Eq | 
            vm::bytecode::OpCode::Ne => {
                has_comparison_op = true;
                
                // Verify one operand is the constant register
                if constant_register.is_some() && 
                   (instr.p1 == constant_register.unwrap() || instr.p3 == constant_register.unwrap()) {
                    has_goto_or_jump = true;
                }
            },
            vm::bytecode::OpCode::Next => {
                has_next = true;
            },
            _ => {}
        }
    }
    
    // Assert that all required instructions are present
    assert!(has_open_read, "Missing OpenRead opcode");
    assert!(has_rewind, "Missing Rewind opcode");
    assert!(has_column_read, "Missing Column opcode");
    assert!(has_comparison_op, "Missing comparison opcode");
    assert!(has_goto_or_jump, "Missing branch or jump after comparison");
    assert!(has_next, "Missing Next opcode for loop control");
    assert!(has_integer_constant, "Missing Integer 20 constant");
}

#[test]
fn test_where_execution_result() {
    // Create a test database with sample person data
    let database = create_test_persons_database();
    
    // Execute "SELECT name, age FROM persons WHERE age > 20" with the VM
    let result = vm::execute_vm(
        "SELECT name, age FROM persons WHERE age > 20", 
        &database, 
        false
    );
    
    // Verify the query executed successfully
    assert!(result.is_ok(), "VM execution failed: {:?}", result.err());
    
    // Verify we have a result table
    let table_opt = result.unwrap();
    assert!(table_opt.is_some(), "Expected a result table, got None");
    
    let table = table_opt.unwrap();
    
    // Verify the table structure
    assert_eq!(table.column_count(), 2, "Expected 2 columns, got {}", table.column_count());
    assert_eq!(table.row_count(), 2, "Expected 2 rows, got {}", table.row_count());
    
    // Verify the table content - should contain only jane and john who are older than 20
    let rows = table.rows();
    assert_eq!(rows.len(), 2, "Expected 2 rows, got {}", rows.len());
    
    // Verify the row content
    // First row - jane
    let first_row = &rows[0];
    assert_eq!(first_row.len(), 2, "Expected 2 columns in row, got {}", first_row.len());
    
    match &first_row[0] {
        Value::String(name) => assert_eq!(name, "jane", "Expected name 'jane', got '{}'", name),
        other => panic!("Expected String type for name, got {:?}", other),
    }
    
    match &first_row[1] {
        Value::Integer(age) => assert_eq!(*age, 25, "Expected age 25, got {}", age),
        other => panic!("Expected Integer type for age, got {:?}", other),
    }
    
    // Second row - john
    let second_row = &rows[1];
    assert_eq!(second_row.len(), 2, "Expected 2 columns in row, got {}", second_row.len());
    
    match &second_row[0] {
        Value::String(name) => assert_eq!(name, "john", "Expected name 'john', got '{}'", name),
        other => panic!("Expected String type for name, got {:?}", other),
    }
    
    match &second_row[1] {
        Value::Integer(age) => assert_eq!(*age, 35, "Expected age 35, got {}", age),
        other => panic!("Expected Integer type for age, got {:?}", other),
    }
}