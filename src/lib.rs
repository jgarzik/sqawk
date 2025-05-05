//! Sqawk library crate
//!
//! This is the library component of Sqawk, containing all the core functionality
//! for an SQL-based command-line utility that processes CSV files. The library provides:
//!
//! - CSV file loading and saving with automatic type inference
//! - SQL query parsing and execution against in-memory tables
//! - Support for SELECT, INSERT, UPDATE, and DELETE operations
//! - Multi-table operations including joins and cross-joins
//! - Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
//! - Column aliases and ORDER BY sorting
//! - Type-aware comparisons and value handling
//!
//! The library is designed to be compact yet powerful, prioritizing query correctness,
//! flexible CSV handling, and a consistent SQL experience for data manipulation.

pub mod aggregate;
pub mod cli;
pub mod csv_handler;
pub mod error;
pub mod join;
pub mod sql_executor;
pub mod table;