//! Top-level test module for sqawk
//!
//! This file organizes all tests into functional categories for parallel execution.

// Test modules organized by functionality
mod basic; // Basic end-to-end tests
mod comparison; // Comparison operator tests
mod distinct; // DISTINCT keyword tests
mod join; // JOIN operations tests
mod order_by; // ORDER BY clause tests
mod update; // UPDATE statement tests

// New test modules for enhanced coverage
mod advanced; // Tests for advanced SQL features and edge cases
mod aggregate; // Tests for aggregate functions (COUNT, SUM, AVG, MIN, MAX)
mod alias; // Tests for column aliases (AS keyword)
mod csv_handler; // Tests for CSV handler features (comments, error recovery)
mod delimiter; // Tests for delimiter options (-F flag)
mod group_by; // Tests for GROUP BY functionality
mod join_on; // Tests for JOIN ON syntax (as opposed to WHERE for joins)
mod limit_offset; // Tests for LIMIT and OFFSET clauses
mod string_functions; // Tests for string functions (UPPER, LOWER, TRIM, SUBSTR, REPLACE)

// Support modules
mod helpers; // Test helpers and utilities
