//! SQL Virtual Machine (VM) bytecode execution engine
//!
//! This module implements a bytecode-based SQL execution engine inspired by SQLite's architecture.
//! The engine operates in two phases:
//! 1. Compile SQL statements into bytecode instructions
//! 2. Execute bytecode instructions in a virtual machine (VM)
//!
//! For more information on this approach, see: https://www.sqlite.org/opcode.html

pub mod executor;