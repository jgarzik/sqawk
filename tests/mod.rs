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
mod between; // Tests for BETWEEN operator (VM implementation)
mod case; // Tests for CASE WHEN expressions (VM implementation)
mod cast; // Tests for CAST type conversion (VM implementation)
mod coalesce; // Tests for COALESCE and NULLIF functions (VM implementation)
mod csv_handler; // Tests for CSV handler features (comments, error recovery)
mod delimiter; // Tests for delimiter options (-F flag)
mod group_by; // Tests for GROUP BY functionality
mod in_list; // Tests for IN list operator (VM implementation)
mod join_on; // Tests for JOIN ON syntax (as opposed to WHERE for joins)
mod like; // Tests for LIKE/ILIKE pattern matching (VM implementation)
mod limit_offset; // Tests for LIMIT and OFFSET clauses

// Phase 2: Set Operations (VM implementation)
mod except; // Tests for EXCEPT set operation
mod intersect; // Tests for INTERSECT set operation
mod union; // Tests for UNION/UNION ALL set operations

// Phase 3: Outer Joins (VM implementation)
mod full_join; // Tests for FULL OUTER JOIN
mod left_join; // Tests for LEFT OUTER JOIN
mod right_join; // Tests for RIGHT OUTER JOIN

// Phase 4F: Subquery Support (VM implementation)
mod exists; // Tests for EXISTS/NOT EXISTS subqueries
mod in_subquery; // Tests for IN (SELECT ...) subqueries
mod subquery; // Tests for scalar subqueries with aggregates

mod repl; // Tests for interactive REPL functionality with pre-generated input
mod string_functions; // Tests for string functions (UPPER, LOWER, TRIM, SUBSTR, REPLACE)

// Support modules
mod helpers; // Test helpers and utilities

// Common test utilities
pub mod common;
