//! sqawk library
//!
//! This crate provides a library for the sqawk CLI utility.
//! It facilitates SQL querying against in-memory CSV tables.

pub mod aggregate;
pub mod cli;
pub mod config;
pub mod csv_handler;
pub mod database;
pub mod delim_handler;
pub mod error;
pub mod file_handler;
pub mod repl;
pub mod sql_executor;
pub mod string_functions;
pub mod table;
pub mod vm;
