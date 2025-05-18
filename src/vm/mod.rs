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
use crate::table::Table;

/// Execute SQL using the VM execution engine
///
/// This is the main entry point for VM-based SQL execution.
/// It follows a two-phase approach:
/// 1. Compile SQL to bytecode using the compiler
/// 2. Execute bytecode with the VM engine
pub fn execute_vm(
    sql: &str, 
    database: &Database,
    verbose: bool
) -> SqawkResult<Option<Table>> {
    // Phase 1: Compile SQL to bytecode
    let mut compiler = compiler::SqlCompiler::new(database, verbose);
    let program = compiler.compile(sql)?;
    
    if verbose {
        println!("Compiled program:");
        println!("{}", program);
    }
    
    // Phase 2: Execute bytecode with VM engine
    let mut engine = engine::VmEngine::new(database, verbose);
    engine.init(program);
    engine.execute()?;
    
    // Get results as a table
    engine.create_result_table()
}