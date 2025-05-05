//! Top-level test module for sqawk
//!
//! This file organizes all tests into functional categories for parallel execution.

// Test modules organized by functionality
mod basic;       // Basic end-to-end tests
mod comparison;  // Comparison operator tests
mod distinct;    // DISTINCT keyword tests
mod join;        // JOIN operations tests
mod order_by;    // ORDER BY clause tests
mod update;      // UPDATE statement tests

// Support modules
mod helpers;     // Test helpers and utilities
