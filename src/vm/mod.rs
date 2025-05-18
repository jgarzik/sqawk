//! SQL Virtual Machine (VM) bytecode execution engine
//!
//! This module implements a bytecode-based SQL execution engine inspired by SQLite's architecture.
//! The engine operates in two phases:
//! 1. Compile SQL statements into bytecode instructions
//! 2. Execute bytecode instructions in a virtual machine (VM)
//!
//! For more information on this approach, see: https://www.sqlite.org/opcode.html

pub mod bytecode;
pub mod compiler;
pub mod engine;
pub mod executor;

use crate::error::SqawkResult;
use crate::database::Database;
use crate::table::{Table, Value};

/// Execute SQL using the VM execution engine
///
/// This is the main entry point for VM-based SQL execution.
/// It implements a two-phase approach:
/// 1. Compile SQL to bytecode using the compiler
/// 2. Execute bytecode in a VM engine
pub fn execute_vm(
    sql: &str, 
    database: &Database,
    verbose: bool
) -> SqawkResult<Option<Table>> {
    if verbose {
        println!("VM Engine: Executing SQL via bytecode: {}", sql);
    }

    // Phase 1: Compile SQL to bytecode
    // Parse the SQL statement
    let dialect = sqlparser::dialect::HiveDialect {};
    let statements = sqlparser::parser::Parser::parse_sql(&dialect, sql)
        .map_err(|e| SqawkError::SqlParseError(e))?;
    
    if statements.is_empty() {
        return Err(SqawkError::InvalidSqlQuery("No SQL statements found".to_string()));
    }
    
    // Get the first statement
    let stmt = &statements[0];
    
    // Set up a compiler and generate bytecode for the statement
    let mut compiler = compiler::SqlCompiler::new(database, verbose);
    let mut program = bytecode::Program::new();
    
    // Based on statement type, generate appropriate bytecode
    match stmt {
        sqlparser::ast::Statement::Query(query) => {
            // Extract table and columns from the query
            if let sqlparser::ast::SetExpr::Select(select) = &query.body.as_ref() {
                if select.from.is_empty() {
                    return Err(SqawkError::InvalidSqlQuery(
                        "SELECT query must have at least one table".to_string(),
                    ));
                }
                
                // Get the table name from the query
                let table_with_joins = &select.from[0];
                let table_name = match &table_with_joins.relation {
                    sqlparser::ast::TableFactor::Table { name, .. } => {
                        name.0[0].value.clone()
                    },
                    _ => return Err(SqawkError::UnsupportedSqlFeature(
                        "Only simple table references are supported".to_string(),
                    )),
                };
                
                // Check if table exists
                if !database.has_table(&table_name) {
                    return Err(SqawkError::TableNotFound(table_name));
                }
                
                // Get the table
                let table = database.get_table(&table_name).unwrap();
                
                // Generate bytecode for basic SELECT * FROM table
                
                // 1. Add Init instruction
                program.add_instruction(bytecode::Instruction::new(
                    bytecode::OpCode::Init,
                    0, 0, 0, None, 0,
                    Some("Initialize VM".to_string()),
                ));
                
                // 2. Open the table for reading (cursor 0)
                program.add_instruction(bytecode::Instruction::new(
                    bytecode::OpCode::OpenRead,
                    0, 0, 0, 
                    Some(table_name.clone()),
                    0,
                    Some(format!("Open table '{}'", table_name)),
                ));
                
                // 3. Rewind cursor to start of table
                program.add_instruction(bytecode::Instruction::new(
                    bytecode::OpCode::Rewind,
                    0, 0, 0, None, 0,
                    Some("Move to first row".to_string()),
                ));
                
                // Set up a loop to read rows - the jump target will be this instruction's address
                let loop_addr = program.len();
                
                // 4. For each column, add a Column instruction to read it into a register
                let column_count = table.column_count();
                for i in 0..column_count {
                    program.add_instruction(bytecode::Instruction::new(
                        bytecode::OpCode::Column,
                        0, i as i64, i as i64, None, 0,
                        Some(format!("Read column {} into register {}", i, i)),
                    ));
                }
                
                // 5. Add ResultRow instruction to return a row of results
                program.add_instruction(bytecode::Instruction::new(
                    bytecode::OpCode::ResultRow,
                    0, column_count as i64, 0, None, 0,
                    Some(format!("Return {} columns as a result row", column_count)),
                ));
                
                // 6. Add Next instruction to move to next row or exit loop
                program.add_instruction(bytecode::Instruction::new(
                    bytecode::OpCode::Next,
                    0, loop_addr as i64, 0, None, 0,
                    Some("Move to next row or exit loop".to_string()),
                ));
                
                // 7. Add Close instruction to close the cursor
                program.add_instruction(bytecode::Instruction::new(
                    bytecode::OpCode::Close,
                    0, 0, 0, None, 0,
                    Some("Close cursor".to_string()),
                ));
            } else {
                return Err(SqawkError::UnsupportedSqlFeature(
                    "Only simple SELECT statements are supported".to_string(),
                ));
            }
        },
        _ => return Err(SqawkError::UnsupportedSqlFeature(
            format!("Unsupported SQL statement: {:?}", stmt)
        )),
    }
    
    // Add a final Halt instruction
    program.add_instruction(bytecode::Instruction::new(
        bytecode::OpCode::Halt,
        0, 0, 0, None, 0,
        Some("End execution".to_string()),
    ));
    
    if verbose {
        println!("Generated bytecode:");
        println!("{}", program);
    }
    
    // Phase 2: Execute bytecode in VM engine
    let mut vm = engine::VmEngine::new(database, verbose);
    vm.init(program);
    
    // Execute program
    vm.execute()?;
    
    // Get results as a table
    vm.create_result_table()
}