//! Database module for sqawk
//!
//! This module provides the central database structure for managing tables in sqawk.
//! It serves as the single source of truth for all table operations and maintains
//! the state of tables throughout the application.
//!
//! The Database struct is responsible for:
//! - Storing all tables with their names
//! - Providing a unified interface for table operations

use crate::capacity::DEFAULT_TABLE_CAPACITY;
use crate::config::AppConfig;
use crate::error::{SqawkError, SqawkResult};
use crate::table::Table;
use std::collections::{HashMap, HashSet};

/// Represents the central database that holds all tables
pub struct Database {
    /// In-memory tables indexed by their names
    tables: HashMap<String, Table>,

    /// Names of the tables whose contents or schema changed since they were
    /// loaded or created: the tables `--write` writes back. DROP TABLE takes
    /// a name out again.
    modified: HashSet<String>,

    /// The open transaction, if any (BEGIN ... COMMIT / ROLLBACK).
    transaction: Option<Transaction>,
}

/// What ROLLBACK needs to put the database back as it was at BEGIN.
///
/// Copy-on-write: a table is copied the first time the transaction is about
/// to change it (`get_table_mut`), moved here when the transaction drops it,
/// and recorded as absent when the transaction creates it. BEGIN itself
/// copies nothing, and a table the transaction only reads is never copied.
struct Transaction {
    /// Each table the transaction touched, as it was at BEGIN (`None`: it
    /// did not exist).
    originals: HashMap<String, Option<Table>>,
    /// The modified-table set at BEGIN.
    modified: HashSet<String>,
}

/// Methods for direct table manipulation - used only in special cases
impl Database {
    /// Remove a table from the database by name
    ///
    /// # Arguments
    /// * `name` - The name of the table to remove
    ///
    /// # Returns
    /// * `true` if the table was found and removed, `false` otherwise
    pub fn remove_table(&mut self, name: &str) -> bool {
        let Some(table) = self.tables.remove(name) else {
            return false;
        };
        self.modified.remove(name);
        if let Some(txn) = &mut self.transaction {
            txn.originals.entry(name.to_string()).or_insert(Some(table));
        }
        true
    }
}

/// Transactions and the modified-table set.
impl Database {
    /// BEGIN: open a transaction.
    pub fn begin(&mut self) -> SqawkResult<()> {
        if self.transaction.is_some() {
            return Err(SqawkError::InvalidSqlQuery(
                "cannot start a transaction within a transaction".to_string(),
            ));
        }
        self.transaction = Some(Transaction {
            originals: HashMap::new(),
            modified: self.modified.clone(),
        });
        Ok(())
    }

    /// COMMIT / END: keep the transaction's changes.
    pub fn commit(&mut self) -> SqawkResult<()> {
        self.transaction.take().map(drop).ok_or_else(|| {
            SqawkError::InvalidSqlQuery("cannot commit - no transaction is active".to_string())
        })
    }

    /// ROLLBACK: put every table, and the modified-table set, back as they
    /// were at BEGIN.
    pub fn rollback(&mut self) -> SqawkResult<()> {
        let txn = self.transaction.take().ok_or_else(|| {
            SqawkError::InvalidSqlQuery("cannot rollback - no transaction is active".to_string())
        })?;
        for (name, original) in txn.originals {
            self.tables.remove(&name);
            if let Some(table) = original {
                self.tables.insert(name, table);
            }
        }
        self.modified = txn.modified;
        Ok(())
    }

    /// Whether a transaction is open.
    pub fn in_transaction(&self) -> bool {
        self.transaction.is_some()
    }

    /// Record that `name` changed and must be written back by `--write`.
    pub fn mark_modified(&mut self, name: &str) {
        if !self.modified.contains(name) {
            self.modified.insert(name.to_string());
        }
    }

    /// Whether `name` changed since it was loaded or created.
    pub fn is_modified(&self, name: &str) -> bool {
        self.modified.contains(name)
    }

    /// The modified tables, in ascending name order.
    pub fn modified_tables(&self) -> Vec<String> {
        let mut names: Vec<String> = self.modified.iter().cloned().collect();
        names.sort();
        names
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

impl Database {
    /// Create a new, empty database
    pub fn new() -> Self {
        Database {
            tables: HashMap::with_capacity(DEFAULT_TABLE_CAPACITY),
            modified: HashSet::new(),
            transaction: None,
        }
    }

    /// Add a table to the database
    ///
    /// # Arguments
    /// * `name` - The name of the table
    /// * `table` - The table to add
    ///
    /// # Returns
    /// * `Ok(())` if the table was successfully added
    /// * `Err` if a table with the same name already exists
    pub fn add_table(&mut self, name: String, table: Table) -> SqawkResult<()> {
        if self.tables.contains_key(&name) {
            return Err(SqawkError::TableAlreadyExists(name));
        }

        if let Some(txn) = &mut self.transaction {
            txn.originals.entry(name.clone()).or_insert(None);
        }
        self.tables.insert(name, table);
        Ok(())
    }

    /// Get a reference to a table by name
    ///
    /// # Arguments
    /// * `name` - The name of the table to get
    ///
    /// # Returns
    /// * `Ok(&Table)` reference to the requested table
    /// * `Err` if the table doesn't exist
    pub fn get_table(&self, name: &str) -> SqawkResult<&Table> {
        self.tables
            .get(name)
            .ok_or_else(|| SqawkError::TableNotFound(name.to_string()))
    }

    /// Get a mutable reference to a table by name
    ///
    /// # Arguments
    /// * `name` - The name of the table to get
    ///
    /// # Returns
    /// * `Ok(&mut Table)` mutable reference to the requested table
    /// * `Err` if the table doesn't exist
    ///
    /// Inside a transaction, the first call for a table saves a copy of it
    /// for ROLLBACK.
    pub fn get_table_mut(&mut self, name: &str) -> SqawkResult<&mut Table> {
        if let Some(txn) = &mut self.transaction {
            if !txn.originals.contains_key(name) {
                if let Some(table) = self.tables.get(name) {
                    txn.originals.insert(name.to_string(), Some(table.clone()));
                }
            }
        }
        self.tables
            .get_mut(name)
            .ok_or_else(|| SqawkError::TableNotFound(name.to_string()))
    }

    /// Get the names of all tables in the database
    ///
    /// Sorted, because the backing store is a `HashMap`: without this the
    /// order varied between runs, which reached the user through `.tables`,
    /// `.schema` with no argument, and verbose load output.
    ///
    /// # Returns
    /// * Vector of table names, in ascending name order
    pub fn table_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.tables.keys().cloned().collect();
        names.sort();
        names
    }

    /// Get the number of tables in the database
    ///
    /// # Returns
    /// * The number of tables
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    /// Check if a table with the given name exists
    ///
    /// # Arguments
    /// * `name` - The name of the table to check
    ///
    /// # Returns
    /// * `true` if the table exists
    /// * `false` if the table doesn't exist
    pub fn has_table(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    /// Compile table definitions from the application configuration
    ///
    /// This method processes the table definitions provided through command-line
    /// arguments and creates corresponding table schemas in the database.
    /// It serves as the central mechanism for converting raw tabledef strings
    /// into properly structured table objects.
    ///
    /// # Arguments
    /// * `config` - The application configuration containing table definitions
    ///
    /// Each definition is `NAME:COL1,COL2,...` and becomes a placeholder
    /// table that the input file loaded as NAME fills in (see
    /// [`Database::check_table_definitions_used`]).
    ///
    /// # Returns
    /// * `Ok(())` if all table definitions were compiled successfully
    /// * `Err` for a definition without `:`, an empty table or column name,
    ///   a column named twice, or a table defined twice
    pub fn compile_table_definitions(&mut self, config: &AppConfig) -> SqawkResult<()> {
        for tabledef in config.table_definitions() {
            let bad = |why: &str| {
                SqawkError::InvalidFileSpec(format!("invalid --tabledef '{}': {}", tabledef, why))
            };
            let (table_name, columns_str) = tabledef
                .split_once(':')
                .ok_or_else(|| bad("expected NAME:COL1,COL2,..."))?;
            let table_name = table_name.trim();
            if table_name.is_empty() {
                return Err(bad("empty table name"));
            }
            let columns: Vec<String> = columns_str
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();
            for (i, col) in columns.iter().enumerate() {
                if col.is_empty() {
                    return Err(bad("empty column name"));
                }
                if columns[..i].iter().any(|c| c.eq_ignore_ascii_case(col)) {
                    return Err(bad(&format!("duplicate column name: {}", col)));
                }
            }
            if self.has_table(table_name) {
                return Err(bad(&format!("table {} is defined twice", table_name)));
            }

            let columns_len = columns.len();
            let mut table = Table::new(table_name, columns, None);
            table.set_placeholder(true);
            self.add_table(table_name.to_string(), table)?;

            if config.verbose() {
                diag!(
                    "Compiled table definition for '{}' with {} columns",
                    table_name,
                    columns_len
                );
            }
        }

        Ok(())
    }

    /// Fail if a `--tabledef` names a table that no input file loaded: its
    /// placeholder is still in the database once every file is loaded.
    pub fn check_table_definitions_used(&self) -> SqawkResult<()> {
        match self
            .table_names()
            .into_iter()
            .find(|name| self.tables[name].is_placeholder())
        {
            Some(name) => Err(SqawkError::InvalidFileSpec(format!(
                "--tabledef names table '{}', which no input file loads",
                name
            ))),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_and_get_table() {
        let mut db = Database::new();

        // Create a table directly
        let table = Table::new("test", vec!["id".to_string(), "name".to_string()], None);
        db.add_table("test".to_string(), table).unwrap();

        // Get the table
        let table = db.get_table("test").unwrap();
        assert_eq!(table.name(), "test");
        assert_eq!(table.columns(), &["id", "name"]);
    }

    #[test]
    fn test_table_operations() {
        let mut db = Database::new();

        // Create some tables directly
        let table1 = Table::new("table1", vec!["col1".to_string()], None);
        let table2 = Table::new("table2", vec!["col2".to_string()], None);
        db.add_table("table1".to_string(), table1).unwrap();
        db.add_table("table2".to_string(), table2).unwrap();

        // Check table count
        assert_eq!(db.table_count(), 2);

        // Check table names
        let names = db.table_names();
        assert!(names.contains(&"table1".to_string()));
        assert!(names.contains(&"table2".to_string()));

        // Check if table exists
        assert!(db.has_table("table1"));
        assert!(db.has_table("table2"));

        // Remove a table manually
        db.remove_table("table1");
        assert_eq!(db.table_count(), 1);
        assert!(!db.has_table("table1"));
        assert!(db.has_table("table2"));
    }
}
