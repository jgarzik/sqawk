//! Table module for sqawk
//!
//! This module provides the in-memory table representation for the sqawk utility.
//! It handles all data storage, manipulation, and table operations including:
//!
//! - Dynamic type inference for data from delimiter-separated files
//! - In-memory data storage with column mapping
//! - Table operations (select, project, update, delete)
//! - Table joins (cross joins and inner joins via WHERE conditions)
//! - Column resolution with qualified names (table.column)

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;

use crate::error::{SqawkError, SqawkResult};

/// Represents a value in a table cell
///
/// This enum provides the possible data types for a cell value in a table.
/// It supports the common SQL data types and allows for type conversions
/// between numeric types (Integer <-> Float) for comparison operations.
///
/// String values are `Cow<'static, str>`; every stored string is owned.
#[derive(Debug, Clone)]
pub enum Value {
    /// Represents a NULL or missing value
    Null,
    /// 64-bit signed integer
    Integer(i64),
    /// 64-bit floating point number
    Float(f64),
    /// UTF-8 string (owned or borrowed via Cow)
    String(Cow<'static, str>),
    /// Boolean value (true/false)
    Boolean(bool),
}

/// Implementation of equality comparison for Value
///
/// This implementation allows comparison between different types with appropriate
/// type coercion, such as comparing integers with floating point numbers.
/// Other type combinations are considered not equal, following SQL comparison rules.
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => a == b,
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Boolean(a), Value::Boolean(b)) => a == b,
            // Handle comparisons between Integer and Float
            (Value::Integer(a), Value::Float(b)) => *a as f64 == *b,
            (Value::Float(a), Value::Integer(b)) => *a == *b as f64,
            // All other combinations are not equal
            _ => false,
        }
    }
}

// Implement the Eq trait for Value
// This is necessary for HashMap keys
impl Eq for Value {}

// Implement the Hash trait for Value
// This is necessary for HashMap keys
impl std::hash::Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Value::Null => {
                // Hash a special value for Null
                0_i32.hash(state);
            }
            Value::Integer(i) => {
                // Hash the integer value
                1_i32.hash(state);
                i.hash(state);
            }
            Value::Float(f) => {
                // A float equal to an integer must hash like that integer,
                // because `Integer(1) == Float(1.0)` under PartialEq.
                if f.fract() == 0.0 && *f >= i64::MIN as f64 && *f < i64::MAX as f64 {
                    1_i32.hash(state);
                    (*f as i64).hash(state);
                } else {
                    // Bits avoid NaN issues
                    2_i32.hash(state);
                    f.to_bits().hash(state);
                }
            }
            Value::String(s) => {
                // Hash the string value
                3_i32.hash(state);
                s.hash(state);
            }
            Value::Boolean(b) => {
                // Hash the boolean value
                4_i32.hash(state);
                b.hash(state);
            }
        }
    }
}

/// Implementation of ordering comparison for Value
///
/// This implementation allows ordering comparison between different types with appropriate
/// type coercion, following SQL comparison rules:
/// - NULL values are considered less than any non-NULL value
/// - Numbers (Integer and Float) can be compared with each other
/// - Strings are compared lexicographically
/// - Booleans compare false < true
/// - Different types follow a precedence order: NULL < Boolean < Number < String
impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        use std::cmp::Ordering;

        match (self, other) {
            // NULL handling: NULL is less than anything but equal to NULL
            (Value::Null, Value::Null) => Some(Ordering::Equal),
            (Value::Null, _) => Some(Ordering::Less),
            (_, Value::Null) => Some(Ordering::Greater),

            // Same types comparison
            (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::String(a), Value::String(b)) => a.partial_cmp(b),
            (Value::Boolean(a), Value::Boolean(b)) => a.partial_cmp(b),

            // Mixed number types
            (Value::Integer(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
            (Value::Float(a), Value::Integer(b)) => a.partial_cmp(&(*b as f64)),

            // Different types follow precedence order:
            // Boolean < Number < String
            (Value::Boolean(_), Value::Integer(_) | Value::Float(_) | Value::String(_)) => {
                Some(Ordering::Less)
            }
            (Value::Integer(_) | Value::Float(_), Value::String(_)) => Some(Ordering::Less),
            (Value::String(_), Value::Boolean(_) | Value::Integer(_) | Value::Float(_)) => {
                Some(Ordering::Greater)
            }
            (Value::Integer(_) | Value::Float(_), Value::Boolean(_)) => Some(Ordering::Greater),
        }
    }
}

/// Implementation of string formatting for Value
///
/// This implementation provides human-readable string representations of all value types.
/// It ensures values are properly displayed when printing tables or generating output in delimited format.
impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Integer(i) => write!(f, "{}", i),
            Value::Float(float) => f.write_str(&crate::vm::arith::real_text(*float)),
            Value::String(s) => write!(f, "{}", s),
            Value::Boolean(b) => write!(f, "{}", b),
        }
    }
}

/// Parse `s` as an integer only if `s` is that integer's canonical spelling.
///
/// Canonical means `i.to_string() == s`: an optional `-`, no `+`, no leading
/// zeros (`007`), no `-0`, no surrounding whitespace. Checked by hand rather
/// than by formatting, since this runs once per loaded cell.
fn canonical_integer(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    let digits = match b.first()? {
        b'-' => &b[1..],
        _ => b,
    };
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    if digits[0] == b'0' && (digits.len() > 1 || digits.len() != b.len()) {
        return None;
    }
    s.parse::<i64>().ok()
}

/// Parse `s` as a float only if rendering the float back (the rendering
/// `Value`'s Display uses, SQLite's `CAST(x AS TEXT)`) reproduces `s`
/// exactly.
///
/// So `1.5`, `2.0` and `1.0e+20` are floats but `1.50`, `2`, `1e3`, `.5`,
/// `0.30000000000000004` (17 digits; rendered `0.3`), `NaN` and `Inf` are
/// not: loading them as numbers would change their spelling on output and on
/// writeback. Every rendering contains a point and only digits, `-`, `+`,
/// `.` and `e`; anything else is rejected before parsing.
fn canonical_float(s: &str) -> Option<f64> {
    if !s.contains('.')
        || !s
            .bytes()
            .all(|c| c.is_ascii_digit() || matches!(c, b'-' | b'.' | b'e' | b'+'))
    {
        return None;
    }
    let f = s.parse::<f64>().ok()?;
    if !f.is_finite() {
        return None;
    }
    if let Some(canonical) = short_decimal_is_canonical(s) {
        return canonical.then_some(f);
    }
    (crate::vm::arith::real_text(f) == s).then_some(f)
}

/// Decide canonicity of a plain decimal without rendering, when possible.
///
/// The rendering is `%.15g` with `.0` kept on integral values: positional
/// when the leading digit's exponent is in [-4, 15), trailing zeros dropped
/// except the one in `.0`. A decimal of at most 15 significant digits comes
/// back unchanged from 15-digit rounding of its nearest double, so such a
/// string is canonical iff it has the rendering's shape. Returns None when
/// the fast rule does not apply (more digits, or exponent notation), leaving
/// the caller to render and compare.
fn short_decimal_is_canonical(s: &str) -> Option<bool> {
    let (neg, body) = match s.strip_prefix('-') {
        Some(b) => (true, b),
        None => (false, s),
    };
    if body.contains('e') {
        return None;
    }
    let (int, frac) = body.split_once('.')?;
    let all_digits = |t: &str| !t.is_empty() && t.bytes().all(|c| c.is_ascii_digit());
    if !all_digits(int) || !all_digits(frac) {
        return Some(false);
    }
    if int.len() > 1 && int.starts_with('0') {
        return Some(false);
    }
    let lead_zeros = frac.bytes().take_while(|&c| c == b'0').count();
    if int == "0" && lead_zeros == frac.len() {
        // Zero renders `0.0`, negative zero included.
        return Some(!neg && frac == "0");
    }
    let significant = if int == "0" {
        frac.len() - lead_zeros
    } else {
        int.len() + frac.len()
    };
    if significant > 15 {
        return None;
    }
    let exponent = if int == "0" {
        -(lead_zeros as i64) - 1
    } else {
        int.len() as i64 - 1
    };
    if !(-4..15).contains(&exponent) {
        return Some(false);
    }
    Some(frac == "0" || !frac.ends_with('0'))
}

/// Infer the type of a field read from an input file.
///
/// The rule is chosen so that every loaded cell renders back to exactly the
/// text it was read from -- untouched cells survive `--write` byte for byte:
///
/// - empty field: NULL (rendered back as an empty field)
/// - canonical integer (`42`, `-7`): Integer
/// - canonical float (`1.5`, `-0.25`, `2.0`, `1.0e+20`): Float
/// - anything else, verbatim and untrimmed: String. That includes `007`,
///   `1.50`, `1e3`, `0.30000000000000004`, ` 42`, `true`, `NaN`.
///
/// Numeric-looking strings still behave as numbers where a number is needed
/// (arithmetic, SUM/AVG, comparison with a number, ordering); see the VM.
fn infer_value(s: &str) -> Option<Value> {
    if s.is_empty() {
        return Some(Value::Null);
    }
    if let Some(i) = canonical_integer(s) {
        return Some(Value::Integer(i));
    }
    canonical_float(s).map(Value::Float)
}

/// Field text to Value, with canonical type inference (see `infer_value`).
impl From<&str> for Value {
    fn from(s: &str) -> Self {
        infer_value(s).unwrap_or_else(|| Value::String(Cow::Owned(s.to_string())))
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        infer_value(&s).unwrap_or(Value::String(Cow::Owned(s)))
    }
}

impl Value {
    /// The text of this value as a delimited-file field.
    ///
    /// Identical to Display except that NULL is the empty field, which is
    /// what NULL loads from. Used for stdout and for writeback.
    pub fn field_str(&self) -> Cow<'_, str> {
        match self {
            Value::Null => Cow::Borrowed(""),
            Value::String(s) => Cow::Borrowed(s.as_ref()),
            other => Cow::Owned(other.to_string()),
        }
    }

    /// This value as a number, if it is one or is text spelling one.
    ///
    /// Integers stay integers (text such as `007` included); other numeric
    /// text becomes a float. Surrounding whitespace is allowed, as awk and
    /// SQLite allow it. Returns None for NULL, booleans and non-numeric text.
    pub fn as_numeric(&self) -> Option<Value> {
        match self {
            Value::Integer(_) | Value::Float(_) => Some(self.clone()),
            Value::String(s) => numeric_text(s),
            _ => None,
        }
    }
}

/// Make `names` unique, case-insensitively, the way SQLite names the
/// columns of a subquery in FROM or a CREATE TABLE ... AS: a name equal to
/// an earlier one gets `:1`, `:2`, ... in place of any `:N` suffix it had,
/// the first that is free (`id, id, id` becomes `id, id:1, id:2`).
pub fn unique_column_names(names: &mut [String]) {
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    for name in names.iter_mut() {
        let mut cnt = 0u32;
        while seen.contains(&name.to_lowercase()) {
            let digits = name.len() - name.trim_end_matches(|c: char| c.is_ascii_digit()).len();
            let mut base = name.len();
            if digits > 0 && digits < name.len() && name.as_bytes()[name.len() - digits - 1] == b':'
            {
                base = name.len() - digits - 1;
            }
            cnt += 1;
            *name = format!("{}:{}", &name[..base], cnt);
        }
        seen.insert(name.to_lowercase());
    }
}

/// Interpret text as a number: an i64 if it parses as one, else a finite f64.
///
/// This is the single "numeric reading" of text, used by every comparison
/// (see the VM's `sql_compare`), by SUM/AVG and by ordering. Surrounding
/// ASCII whitespace is allowed.
pub fn numeric_text(s: &str) -> Option<Value> {
    let t = s.trim_matches(|c: char| c.is_ascii_whitespace());
    // Cheap rejection first: this runs inside sort comparators. A finite
    // number starts with a digit, a sign or a point.
    if !t
        .bytes()
        .next()
        .is_some_and(|c| c.is_ascii_digit() || matches!(c, b'-' | b'+' | b'.'))
    {
        return None;
    }
    if let Ok(i) = t.parse::<i64>() {
        return Some(Value::Integer(i));
    }
    // `inf` and `NaN` parse but are not finite, so they stay text.
    match t.parse::<f64>() {
        Ok(f) if f.is_finite() => Some(Value::Float(f)),
        _ => None,
    }
}

/// Represents a row in a table
pub type Row = Vec<Value>;

// =============================================================================
// Index-Based Result Types (Phase 4A)
// =============================================================================
// These types enable efficient query processing by storing row indices instead
// of cloning entire rows. Materialization only happens at output time.
//
// Note: These types are defined in Phase 4A and will be integrated in Phases 4B-4F.
// The #[allow(dead_code)] attributes will be removed as each type is adopted.

/// Reference to a row within a specific table
///
/// This lightweight struct (16 bytes) replaces cloning entire rows (~100+ bytes).
/// Used for tracking which rows match query conditions.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RowRef {
    /// Index of the table in the source table list (for JOINs)
    pub table_idx: usize,
    /// Index of the row within that table
    pub row_idx: usize,
}

#[allow(dead_code)]
impl RowRef {
    /// Create a new row reference
    pub fn new(table_idx: usize, row_idx: usize) -> Self {
        Self { table_idx, row_idx }
    }

    /// Create a row reference for single-table operations (table_idx = 0)
    pub fn single(row_idx: usize) -> Self {
        Self {
            table_idx: 0,
            row_idx,
        }
    }
}

/// Set of row references for single-table operations
///
/// Instead of `Vec<Vec<Value>>` (clones all row data), this stores only
/// indices into the original table. Memory usage: O(n * 8 bytes) vs O(n * row_size).
#[allow(dead_code)]
#[derive(Debug)]
pub struct RowSet<'a> {
    /// Reference to the source table (borrowed, not owned)
    source: &'a Table,
    /// Indices of selected rows (into source.rows)
    indices: Vec<usize>,
}

#[allow(dead_code)]
impl<'a> RowSet<'a> {
    /// Create a new empty RowSet referencing the given table
    pub fn new(source: &'a Table) -> Self {
        Self {
            source,
            indices: Vec::new(),
        }
    }

    /// Create a RowSet with pre-allocated capacity
    pub fn with_capacity(source: &'a Table, capacity: usize) -> Self {
        Self {
            source,
            indices: Vec::with_capacity(capacity),
        }
    }

    /// Add a row index to the set
    pub fn push(&mut self, row_idx: usize) {
        self.indices.push(row_idx);
    }

    /// Get the number of rows in this set
    pub fn len(&self) -> usize {
        self.indices.len()
    }

    /// Check if the set is empty
    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    /// Get the source table
    pub fn source(&self) -> &'a Table {
        self.source
    }

    /// Get the row indices
    pub fn indices(&self) -> &[usize] {
        &self.indices
    }

    /// Get a value from a specific row and column (no clone)
    pub fn get_value(&self, row_idx: usize, col_idx: usize) -> Option<&Value> {
        let actual_row = *self.indices.get(row_idx)?;
        self.source.rows().get(actual_row)?.get(col_idx)
    }

    /// Materialize a single row (clones values - use sparingly)
    pub fn materialize_row(&self, row_idx: usize) -> Option<Vec<Value>> {
        let actual_row = *self.indices.get(row_idx)?;
        self.source.rows().get(actual_row).cloned()
    }

    /// Iterate over row indices
    pub fn iter(&self) -> impl Iterator<Item = usize> + '_ {
        self.indices.iter().copied()
    }
}

/// Row references for multi-table operations (JOINs)
///
/// For JOIN operations, we need to track which rows from each table
/// form a result row. Option<usize> handles NULL rows in outer joins.
#[allow(dead_code)]
#[derive(Debug)]
pub struct JoinedRowSet<'a> {
    /// References to source tables (borrowed)
    sources: Vec<&'a Table>,
    /// Each entry is row indices from each source table
    /// Option<usize> allows NULL rows for outer joins
    row_pairs: Vec<Vec<Option<usize>>>,
}

#[allow(dead_code)]
impl<'a> JoinedRowSet<'a> {
    /// Create a new JoinedRowSet for two tables
    pub fn new(left: &'a Table, right: &'a Table) -> Self {
        Self {
            sources: vec![left, right],
            row_pairs: Vec::new(),
        }
    }

    /// Create with pre-allocated capacity
    pub fn with_capacity(left: &'a Table, right: &'a Table, capacity: usize) -> Self {
        Self {
            sources: vec![left, right],
            row_pairs: Vec::with_capacity(capacity),
        }
    }

    /// Add a matched row pair (INNER JOIN)
    pub fn add_match(&mut self, left_idx: usize, right_idx: usize) {
        self.row_pairs.push(vec![Some(left_idx), Some(right_idx)]);
    }

    /// Add unmatched left row with NULL right (LEFT JOIN)
    pub fn add_left_only(&mut self, left_idx: usize) {
        self.row_pairs.push(vec![Some(left_idx), None]);
    }

    /// Add unmatched right row with NULL left (RIGHT JOIN)
    pub fn add_right_only(&mut self, right_idx: usize) {
        self.row_pairs.push(vec![None, Some(right_idx)]);
    }

    /// Get the number of result rows
    pub fn len(&self) -> usize {
        self.row_pairs.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.row_pairs.is_empty()
    }

    /// Get value from a joined row (returns &Value or NULL for outer join misses)
    pub fn get_value(&self, row_idx: usize, table_idx: usize, col_idx: usize) -> &Value {
        // Static NULL for outer join misses
        static NULL_VALUE: Value = Value::Null;

        if let Some(row_pair) = self.row_pairs.get(row_idx) {
            if let Some(Some(actual_row)) = row_pair.get(table_idx) {
                if let Some(table) = self.sources.get(table_idx) {
                    if let Some(row) = table.rows().get(*actual_row) {
                        if let Some(value) = row.get(col_idx) {
                            return value;
                        }
                    }
                }
            }
        }
        &NULL_VALUE
    }

    /// Get the source tables
    pub fn sources(&self) -> &[&'a Table] {
        &self.sources
    }

    /// Materialize a single result row (clones values)
    pub fn materialize_row(
        &self,
        row_idx: usize,
        left_cols: usize,
        right_cols: usize,
    ) -> Option<Vec<Value>> {
        // Verify row exists
        let _ = self.row_pairs.get(row_idx)?;
        let mut result = Vec::with_capacity(left_cols + right_cols);

        // Left table columns
        for col_idx in 0..left_cols {
            result.push(self.get_value(row_idx, 0, col_idx).clone());
        }

        // Right table columns
        for col_idx in 0..right_cols {
            result.push(self.get_value(row_idx, 1, col_idx).clone());
        }

        Some(result)
    }
}

/// Unified VM result type - either single-table indices, joined indices, or materialized
///
/// This enum allows the VM to work with index-based results throughout query
/// processing, only materializing to actual values when necessary.
#[allow(dead_code)]
#[derive(Debug)]
pub enum IndexedResult<'a> {
    /// Result from single table scan/filter
    Single(RowSet<'a>),
    /// Result from JOIN operations
    Joined(JoinedRowSet<'a>),
    /// Materialized result (for complex expressions, aggregates, or final output)
    Materialized(Vec<Vec<Value>>),
}

#[allow(dead_code)]
impl<'a> IndexedResult<'a> {
    /// Get the number of result rows
    pub fn len(&self) -> usize {
        match self {
            IndexedResult::Single(rs) => rs.len(),
            IndexedResult::Joined(js) => js.len(),
            IndexedResult::Materialized(rows) => rows.len(),
        }
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Check if this result is already materialized
    pub fn is_materialized(&self) -> bool {
        matches!(self, IndexedResult::Materialized(_))
    }
}

/// Lazy row view - provides access to row values without immediate cloning
///
/// This struct allows iteration over result rows while deferring value
/// cloning until actually needed (e.g., for output).
#[allow(dead_code)]
#[derive(Debug)]
pub struct LazyRow<'a, 'b> {
    /// Reference to the indexed result
    result: &'b IndexedResult<'a>,
    /// Index within the result
    row_idx: usize,
}

#[allow(dead_code)]
impl<'a, 'b> LazyRow<'a, 'b> {
    /// Create a new lazy row view
    pub fn new(result: &'b IndexedResult<'a>, row_idx: usize) -> Self {
        Self { result, row_idx }
    }

    /// Get the row index
    pub fn index(&self) -> usize {
        self.row_idx
    }

    /// Materialize this row to owned values (clones - use when needed for output)
    pub fn materialize(&self) -> Option<Vec<Value>> {
        match self.result {
            IndexedResult::Single(rs) => rs.materialize_row(self.row_idx),
            IndexedResult::Joined(js) => {
                // For joined rows, we need column counts
                let left_cols = js.sources().first().map(|t| t.column_count()).unwrap_or(0);
                let right_cols = js.sources().get(1).map(|t| t.column_count()).unwrap_or(0);
                js.materialize_row(self.row_idx, left_cols, right_cols)
            }
            IndexedResult::Materialized(rows) => rows.get(self.row_idx).cloned(),
        }
    }
}

/// Builder for constructing IndexedResult during VM execution
///
/// This builder accumulates row indices during query processing and
/// produces an IndexedResult at the end.
#[allow(dead_code)]
#[derive(Debug)]
pub struct IndexedResultBuilder<'a> {
    /// Source tables being queried
    sources: Vec<&'a Table>,
    /// Accumulated row indices (single table: one index per row)
    single_indices: Vec<usize>,
    /// Accumulated row pairs (joins: multiple indices per row)
    join_indices: Vec<Vec<Option<usize>>>,
    /// Whether this is a join operation
    is_join: bool,
}

#[allow(dead_code)]
impl<'a> IndexedResultBuilder<'a> {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            sources: Vec::new(),
            single_indices: Vec::new(),
            join_indices: Vec::new(),
            is_join: false,
        }
    }

    /// Add a source table
    pub fn add_source(&mut self, table: &'a Table) {
        self.sources.push(table);
        if self.sources.len() > 1 {
            self.is_join = true;
        }
    }

    /// Add a single row index (for single-table operations)
    pub fn add_row_single(&mut self, row_idx: usize) {
        self.single_indices.push(row_idx);
    }

    /// Add a joined row (for multi-table operations)
    pub fn add_row_joined(&mut self, indices: Vec<Option<usize>>) {
        self.join_indices.push(indices);
    }

    /// Check if any rows have been added
    pub fn is_empty(&self) -> bool {
        self.single_indices.is_empty() && self.join_indices.is_empty()
    }

    /// Get the number of result rows
    pub fn len(&self) -> usize {
        if self.is_join {
            self.join_indices.len()
        } else {
            self.single_indices.len()
        }
    }

    /// Clear accumulated results (for reuse)
    pub fn clear(&mut self) {
        self.single_indices.clear();
        self.join_indices.clear();
    }

    /// Build the final IndexedResult
    pub fn build(self) -> Option<IndexedResult<'a>> {
        if self.sources.is_empty() {
            return None;
        }

        if self.is_join && self.sources.len() >= 2 {
            let joined = JoinedRowSet {
                sources: self.sources,
                row_pairs: self.join_indices,
            };
            Some(IndexedResult::Joined(joined))
        } else if let Some(source) = self.sources.into_iter().next() {
            let row_set = RowSet {
                source,
                indices: self.single_indices,
            };
            Some(IndexedResult::Single(row_set))
        } else {
            None
        }
    }
}

impl<'a> Default for IndexedResultBuilder<'a> {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// End Index-Based Result Types
// =============================================================================

/// Represents an in-memory table
#[derive(Debug)]
pub struct Table {
    /// Name of the table
    name: String,

    /// Complete column metadata including name and type
    cols: Vec<Column>,

    /// Row data
    rows: Vec<Row>,

    /// File path associated with this table (for loading or saving)
    file_path: Option<PathBuf>,

    /// Whether the table was modified since loading
    modified: bool,

    /// Custom delimiter for this table's file (default is comma)
    delimiter: String,

    /// How the source file was laid out, so writeback reproduces it.
    format: FileLayout,

    /// The source file's `#` comment lines, so writeback reproduces them.
    comments: CommentLines,

    /// A `--tabledef` schema with no data yet, waiting for its file
    placeholder: bool,

    /// The source bytes of the header and of each loaded row, so writeback
    /// reproduces untouched rows byte-for-byte (see [`RawRows`]).
    raw: Option<Box<RawRows>>,
}

/// The bytes each record of a table's source file was read from.
///
/// The whole file is kept in one buffer and every row points into it by
/// [`RawSpan`], parallel to `Table::rows`. A row whose span is
/// [`RawSpan::NONE`] (inserted, or changed by UPDATE) is written by the
/// [`RecordWriter`]; every other row is written as the exact bytes it was read
/// from, line ending included -- quoting, padding, invalid UTF-8 and all.
#[derive(Debug)]
pub struct RawRows {
    /// The file contents as read (byte order mark included).
    pub buf: Vec<u8>,
    /// The header record's bytes, if the file has a header.
    pub header: RawSpan,
    /// One span per row of the table.
    pub rows: Vec<RawSpan>,
}

/// A byte range `start..end` of [`RawRows::buf`]; `NONE` when there is none.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawSpan {
    /// First byte of the record
    pub start: usize,
    /// One past the last byte (the line ending, when there is one)
    pub end: usize,
}

impl RawSpan {
    /// No source bytes: the record is written from its values.
    pub const NONE: RawSpan = RawSpan {
        start: usize::MAX,
        end: usize::MAX,
    };

    fn get(self, buf: &[u8]) -> Option<&[u8]> {
        buf.get(self.start..self.end)
    }
}

/// Whether two values are the same value of the same type, so a row that
/// UPDATE rewrote with them is unchanged and keeps its source bytes. Stricter
/// than `==`, which equates 1 and 1.0.
fn identical(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, Value::Null) => true,
        (Value::Integer(x), Value::Integer(y)) => x == y,
        (Value::Float(x), Value::Float(y)) => x.to_bits() == y.to_bits(),
        (Value::String(x), Value::String(y)) => x == y,
        (Value::Boolean(x), Value::Boolean(y)) => x == y,
        _ => false,
    }
}

/// Layout details of a table's source file that writeback must reproduce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileLayout {
    /// Whether the file has a header line. Headerless files (`--no-header`
    /// or `--tabledef`) are written back without one.
    pub has_header: bool,
    /// Whether the file starts with a UTF-8 byte order mark.
    pub bom: bool,
    /// Whether records end in CRLF rather than LF.
    pub crlf: bool,
    /// Whether `#` lines are comments when the file is read. If so, a data
    /// row whose first field starts with `#` must be quoted on writeback, or
    /// it would be skipped as a comment when read again.
    pub comments: bool,
}

/// The lines of a source file between its records -- blank lines, and the
/// `#` comment lines of a file read with `--comments` -- each kept verbatim
/// (with its own line ending) and anchored to the data around it, so that
/// writeback puts it back where it was.
///
/// A line is anchored to the row that follows it. Rows keep their identity
/// through UPDATE, so their lines stay with them; a deleted row leaves its
/// lines in place, in front of the next surviving row (or of the trailing
/// lines); rows appended by INSERT go after the last row and before the
/// trailing lines.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommentLines {
    /// Lines before the first record (the header, or the first row).
    pub head: Vec<u8>,
    /// Lines before row `i`, by row index.
    pub before: BTreeMap<usize, Vec<u8>>,
    /// Lines after the last record.
    pub tail: Vec<u8>,
}

impl CommentLines {
    /// Move the comments of rows `from..` to the front of the tail, in order:
    /// those rows are gone.
    fn flush_from(&mut self, from: usize) {
        let gone = self.before.split_off(&from);
        if gone.is_empty() {
            return;
        }
        let mut lines: Vec<u8> = gone.into_values().flatten().collect();
        lines.append(&mut self.tail);
        self.tail = lines;
    }
}

impl Default for FileLayout {
    fn default() -> Self {
        FileLayout {
            has_header: true,
            bom: false,
            crlf: false,
            comments: false,
        }
    }
}

/// Data type for a column in a table schema
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    /// Integer (i64)
    Integer,
    /// Floating point (f64)
    Float,
    /// Text/String
    Text,
    /// Boolean
    Boolean,
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Integer => write!(f, "INTEGER"),
            DataType::Float => write!(f, "REAL"),
            DataType::Text => write!(f, "TEXT"),
            DataType::Boolean => write!(f, "BOOLEAN"),
        }
    }
}

/// Represents a column in a table with name and type information
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    /// Name of the column
    pub name: String,
    /// Data type of the column
    pub data_type: DataType,
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.name, self.data_type)
    }
}

/// Column definition for a table schema (used during table creation)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDefinition {
    /// Name of the column
    pub name: String,
    /// Data type of the column
    pub data_type: DataType,
}

impl Table {
    /// Create a new table with the given name and column names
    ///
    /// This constructor defaults all columns to Text type
    pub fn new(name: &str, column_names: Vec<String>, file_path: Option<PathBuf>) -> Self {
        // Create full column metadata objects
        let cols = column_names
            .iter()
            .map(|name| Column {
                name: name.clone(),
                data_type: DataType::Text,
            })
            .collect();

        Table {
            name: name.to_string(),
            cols,
            rows: Vec::new(),
            file_path,
            modified: false,
            delimiter: ",".to_string(), // Default to comma delimiter
            format: FileLayout::default(),
            comments: CommentLines::default(),
            placeholder: false,
            raw: None,
        }
    }

    /// Create a new table with the given name, column names, and a specific delimiter
    pub fn new_with_delimiter(
        name: &str,
        column_names: Vec<String>,
        file_path: Option<PathBuf>,
        delimiter: String,
    ) -> Self {
        let mut table = Self::new(name, column_names, file_path);
        table.delimiter = delimiter;
        table
    }

    /// Create a new table with a schema
    pub fn new_with_schema(
        name: &str,
        schema: Vec<ColumnDefinition>,
        file_path: Option<PathBuf>,
        delimiter: Option<String>,
    ) -> Self {
        // Create full column metadata objects
        let cols = schema
            .iter()
            .map(|col_def| Column {
                name: col_def.name.clone(),
                data_type: col_def.data_type,
            })
            .collect();

        Table {
            name: name.to_string(),
            cols,
            rows: Vec::new(),
            file_path,
            modified: true, // Tables created with schema are considered modified
            delimiter: delimiter.unwrap_or_else(|| ",".to_string()),
            format: FileLayout::default(),
            comments: CommentLines::default(),
            placeholder: false,
            raw: None,
        }
    }

    /// Get the columns of the table
    ///
    /// Returns a vector containing all column names in the table.
    /// The column names maintain their original order as specified when
    /// the table was created or loaded from a file.
    pub fn columns(&self) -> Vec<String> {
        self.cols.iter().map(|col| col.name.clone()).collect()
    }

    /// Get all columns with their metadata
    ///
    /// Returns a slice containing all column metadata including names and types.
    pub fn column_metadata(&self) -> &[Column] {
        &self.cols
    }

    /// Get the column count
    ///
    /// Returns the number of columns in the table. This is useful for
    /// validation when adding rows or performing operations that need to
    /// check column bounds.
    pub fn column_count(&self) -> usize {
        self.columns().len()
    }

    /// Get the rows of the table
    ///
    /// Returns a slice containing all rows in the table. Each row is a vector
    /// of Value enums representing the cell values. This provides read-only
    /// access to the table data for processing or querying.
    pub fn rows(&self) -> &[Row] {
        &self.rows
    }

    /// Get the name of the table
    ///
    /// Returns the name of the table as a string slice.
    /// This is useful for operations that need to access the table's name
    /// such as logging, error messages, or generating SQL output.
    /// Rename the table.
    ///
    /// Used when a derived table is materialized and registered under its
    /// alias, so the table reports the name the outer query refers to.
    pub fn set_name(&mut self, name: String) {
        self.name = name;
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the row count
    ///
    /// Returns the number of rows in the table. This is useful for
    /// determining the size of the result set or for validation.
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Add a row to the table
    ///
    /// Adds a new row (vector of values) to the table, verifying that the
    /// number of columns in the row matches the table definition. This operation
    /// marks the table as modified, indicating it should be written back to disk
    /// if changes are being saved.
    ///
    /// # Arguments
    /// * `row` - Vector of values to add as a new row
    ///
    /// # Returns
    /// * `Ok(())` if the row was successfully added
    /// * `Err` if the row doesn't match the table schema
    pub fn add_row(&mut self, row: Row) -> SqawkResult<()> {
        if row.len() != self.column_count() {
            return Err(SqawkError::InvalidSqlQuery(format!(
                "Row has {} columns, but table '{}' has {} columns",
                row.len(),
                self.name,
                self.column_count()
            )));
        }

        self.rows.push(row);
        if let Some(raw) = &mut self.raw {
            raw.rows.push(RawSpan::NONE);
        }
        self.modified = true;
        Ok(())
    }

    /// Clear the modified flag (a freshly loaded table is unmodified)
    pub fn mark_unmodified(&mut self) {
        self.modified = false;
    }

    /// Whether this is a `--tabledef` placeholder awaiting its file
    pub fn is_placeholder(&self) -> bool {
        self.placeholder
    }

    /// Mark this table as a `--tabledef` placeholder
    pub fn set_placeholder(&mut self, placeholder: bool) {
        self.placeholder = placeholder;
    }

    /// Get the file path associated with this table
    ///
    /// Returns the path to the file associated with this table,
    /// either as the source file it was loaded from or the destination
    /// file specified in CREATE TABLE. This is used when writing
    /// changes back to disk.
    ///
    /// # Returns
    /// * `Some(PathBuf)` containing the file path
    /// * `None` if the table has no associated file
    pub fn file_path(&self) -> Option<&PathBuf> {
        self.file_path.as_ref()
    }

    /// Get the delimiter for this table
    ///
    /// Returns the delimiter used for this table.
    /// This is used when writing the table to a file.
    ///
    /// # Returns
    /// * A string reference to the delimiter (always defined, defaults to comma)
    pub fn delimiter(&self) -> &String {
        &self.delimiter
    }

    /// Set the delimiter for this table
    ///
    /// # Arguments
    /// * `delimiter` - The new delimiter string to use
    pub fn set_delimiter(&mut self, delimiter: String) {
        self.delimiter = delimiter;
    }

    /// Layout of the source file (header, line endings, comments)
    pub fn file_layout(&self) -> FileLayout {
        self.format
    }

    /// Record the layout of the source file, for writeback
    pub fn set_file_layout(&mut self, format: FileLayout) {
        self.format = format;
    }

    /// Record the source file's comment lines, for writeback. Comments
    /// anchored past the last row are moved to the tail.
    pub fn set_comment_lines(&mut self, comments: CommentLines) {
        self.comments = comments;
        self.comments.flush_from(self.rows.len());
    }

    /// The index of the column called `name`.
    ///
    /// Column names are case-insensitive (ASCII), as in SQLite; this is the
    /// one lookup every statement uses, so a name resolves the same way in a
    /// SELECT, an INSERT column list and an UPDATE SET. A loaded or created
    /// table cannot have two columns that match the same name.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.cols
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
    }

    /// Make the column names unique as SQLite names the columns of a
    /// derived table or CREATE TABLE ... AS (see [`unique_column_names`]).
    pub fn dedup_column_names(&mut self) {
        let mut names: Vec<String> = self.cols.iter().map(|c| c.name.clone()).collect();
        unique_column_names(&mut names);
        for (col, name) in self.cols.iter_mut().zip(names) {
            col.name = name;
        }
        self.raw = None;
    }

    /// Print the table to stdout as a delimited file
    ///
    /// Header line, then one record per row, using this table's delimiter
    /// with RFC 4180 quoting where a field needs it. NULL prints as an empty
    /// field. Returns the raw io::Error so the caller can recognise a closed
    /// pipe (`sqawk ... | head`) and stop quietly.
    pub fn print_to_stdout(&self) -> std::io::Result<()> {
        let stdout = std::io::stdout();
        let mut out = std::io::BufWriter::new(stdout.lock());
        self.write_delimited(&mut out, true, false, false, None)?;
        std::io::Write::flush(&mut out)
    }

    /// Write the table as a delimited file to `out`.
    ///
    /// * `header` - emit the column names first
    /// * `crlf` - end records with CRLF instead of LF
    /// * `comments` - the file will be read with `#` comment skipping, so a
    ///   leading `#` in a record's first field must be quoted
    /// * `lines` - comment lines to put back around the records
    pub fn write_delimited<W: std::io::Write>(
        &self,
        out: &mut W,
        header: bool,
        crlf: bool,
        comments: bool,
        lines: Option<&CommentLines>,
    ) -> std::io::Result<()> {
        self.write_records(out, header, crlf, comments, lines, None)
    }

    /// `write_delimited`, writing each record that has source bytes in
    /// `raw` as those bytes.
    fn write_records<W: std::io::Write>(
        &self,
        out: &mut W,
        header: bool,
        crlf: bool,
        comments: bool,
        lines: Option<&CommentLines>,
        raw: Option<&RawRows>,
    ) -> std::io::Result<()> {
        let delim = delimiter_byte(&self.delimiter);
        let terminator: &'static [u8] = if crlf { b"\r\n" } else { b"\n" };
        let w = RecordWriter {
            delimiter: delim,
            terminator,
            comments,
        };
        let empty = CommentLines::default();
        let lines = lines.unwrap_or(&empty);
        let source = |span: RawSpan| raw.and_then(|r| span.get(&r.buf));
        // The last record of a file may have no line ending. Its bytes are
        // written as they were, so anything written after it must first end
        // its line.
        let mut open_line = false;
        let put = |out: &mut W, bytes: &[u8], open: &mut bool| -> std::io::Result<()> {
            if bytes.is_empty() {
                return Ok(());
            }
            if *open {
                out.write_all(terminator)?;
            }
            out.write_all(bytes)?;
            *open = !matches!(bytes.last(), Some(b'\n' | b'\r'));
            Ok(())
        };
        put(out, &lines.head, &mut open_line)?;
        if header {
            match source(raw.map_or(RawSpan::NONE, |r| r.header)) {
                Some(bytes) => put(out, bytes, &mut open_line)?,
                None => {
                    let names = self.columns();
                    let mut rec = Vec::new();
                    w.write(&mut rec, names.iter().map(String::as_str))?;
                    put(out, &rec, &mut open_line)?;
                }
            }
        }
        let mut fields: Vec<std::borrow::Cow<'_, str>> = Vec::with_capacity(self.cols.len());
        for (i, row) in self.rows.iter().enumerate() {
            if let Some(before) = lines.before.get(&i) {
                put(out, before, &mut open_line)?;
            }
            if let Some(bytes) = source(raw.map_or(RawSpan::NONE, |r| r.rows[i])) {
                put(out, bytes, &mut open_line)?;
                continue;
            }
            if open_line {
                out.write_all(terminator)?;
                open_line = false;
            }
            fields.clear();
            fields.extend(row.iter().map(Value::field_str));
            w.write(out, fields.iter().map(|f| f.as_ref()))?;
        }
        // A comment line anchored past the last row (none should be) is
        // still written rather than lost.
        for (_, before) in lines.before.range(self.rows.len()..) {
            put(out, before, &mut open_line)?;
        }
        put(out, &lines.tail, &mut open_line)
    }

    /// Write the table back to its file format: the recorded layout (byte
    /// order mark, header, line endings) and comment lines, with every row
    /// that still has its source bytes written as those bytes.
    pub fn write_source_layout<W: std::io::Write>(&self, out: &mut W) -> std::io::Result<()> {
        let layout = self.file_layout();
        if layout.bom {
            out.write_all(b"\xef\xbb\xbf")?;
        }
        self.write_records(
            out,
            layout.has_header,
            layout.crlf,
            layout.comments,
            Some(&self.comments),
            self.raw.as_deref(),
        )
    }

    /// Record the source bytes of the header and rows (see [`RawRows`]).
    /// `raw.rows` must have one span per row.
    pub fn set_raw(&mut self, raw: RawRows) {
        debug_assert_eq!(raw.rows.len(), self.rows.len());
        self.raw = Some(Box::new(raw));
    }

    /// Replace all rows with a new set
    ///
    /// This method is useful for operations like DELETE that need to replace
    /// the content of the table with a filtered subset of rows.
    ///
    /// # Arguments
    /// * `new_rows` - The new set of rows to replace the existing ones
    pub fn replace_rows(&mut self, new_rows: Vec<Row>) {
        self.rows = new_rows;
        self.raw = None;
        self.comments.flush_from(self.rows.len());
        self.modified = true;
    }

    /// Keep only the rows whose index satisfies `keep` (DELETE).
    ///
    /// Comment lines anchored to a removed row stay where they were: they
    /// move to the next surviving row, or to the tail.
    pub fn retain_rows(&mut self, keep: impl Fn(usize) -> bool) {
        let old = std::mem::take(&mut self.rows);
        if let Some(raw) = &mut self.raw {
            let mut i = 0;
            raw.rows.retain(|_| {
                i += 1;
                keep(i - 1)
            });
        }
        let mut before = std::mem::take(&mut self.comments.before);
        let mut pending: Vec<u8> = Vec::new();
        for (i, row) in old.into_iter().enumerate() {
            if let Some(mut lines) = before.remove(&i) {
                pending.append(&mut lines);
            }
            if keep(i) {
                if !pending.is_empty() {
                    self.comments
                        .before
                        .insert(self.rows.len(), std::mem::take(&mut pending));
                }
                self.rows.push(row);
            }
        }
        for (_, mut lines) in before {
            pending.append(&mut lines);
        }
        pending.append(&mut self.comments.tail);
        self.comments.tail = pending;
        self.modified = true;
    }

    /// Replace a single row in place, preserving its position.
    ///
    /// Used by UPDATE. Expressing an update as delete-plus-insert appends the
    /// new row, which reorders the table -- and with --write, the user's file.
    pub fn replace_row(&mut self, row_index: usize, row: Row) -> SqawkResult<()> {
        let rows = &mut self.rows;
        if row_index >= rows.len() {
            return Err(SqawkError::InvalidSqlQuery(format!(
                "Row index {} out of range for table '{}'",
                row_index, self.name
            )));
        }
        if let Some(raw) = &mut self.raw {
            let same = rows[row_index].len() == row.len()
                && rows[row_index]
                    .iter()
                    .zip(&row)
                    .all(|(a, b)| identical(a, b));
            if !same {
                raw.rows[row_index] = RawSpan::NONE;
            }
        }
        rows[row_index] = row;
        self.modified = true;
        Ok(())
    }

    /// Add a column to the table with a specified data type
    #[cfg(test)]
    pub fn add_column(&mut self, name: String, data_type_str: String) {
        let data_type = match data_type_str.to_uppercase().as_str() {
            "INT" | "INTEGER" => DataType::Integer,
            "FLOAT" | "REAL" | "DOUBLE" => DataType::Float,
            "BOOL" | "BOOLEAN" => DataType::Boolean,
            _ => DataType::Text,
        };
        let column = Column { name, data_type };
        self.cols.push(column);
        self.raw = None;
        self.modified = true;
    }

    /// Add a new column with a default value for all existing rows
    pub fn add_column_with_default(
        &mut self,
        name: String,
        data_type: DataType,
        default_value: Value,
    ) -> crate::error::SqawkResult<()> {
        let column = Column { name, data_type };
        self.cols.push(column);
        // Every line of the file gains a field.
        self.raw = None;

        // Add default value to all existing rows
        for row in &mut self.rows {
            row.push(default_value.clone());
        }

        self.modified = true;
        Ok(())
    }

    /// Clear all rows from the table (TRUNCATE TABLE)
    pub fn clear_rows(&mut self) -> crate::error::SqawkResult<()> {
        self.rows.clear();
        self.raw = None;
        self.comments.flush_from(0);
        self.modified = true;
        Ok(())
    }

    /// Convert all rows to string representation
    ///
    /// This method converts all table rows to a vector of string vectors,
    /// useful for displaying in the REPL interface.
    ///
    /// # Returns
    /// * Vec<Vec<String>> - All rows converted to strings
    pub fn rows_as_strings(&self) -> Vec<Vec<String>> {
        self.rows()
            .iter()
            .map(|row| {
                row.iter()
                    .map(|value| value.field_str().into_owned())
                    .collect()
            })
            .collect()
    }
}

/// The delimiter byte for a table's delimiter string.
///
/// The CLI validates `-F` to a single ASCII byte (or the two-character escape
/// `\t`), so anything else can only come from a bug; it falls back to comma.
pub fn delimiter_byte(delimiter: &str) -> u8 {
    match delimiter.as_bytes() {
        [b] => *b,
        b"\\t" => b'\t',
        _ => b',',
    }
}

/// Writes delimited records with RFC 4180 quoting.
///
/// A field is quoted when it contains the delimiter, a quote, CR or LF, or
/// has leading or trailing whitespace (which a reader that trims would
/// lose); embedded quotes are doubled. A record consisting of one empty field
/// is written as `""`, since a blank line is skipped on read. The csv crate's
/// writer cannot express the whitespace rule, hence this small writer.
pub struct RecordWriter {
    /// Field separator byte
    pub delimiter: u8,
    /// Record terminator (`\n` or `\r\n`)
    pub terminator: &'static [u8],
    /// Quote a first field that starts with `#` (see `FileLayout::comments`)
    pub comments: bool,
}

impl RecordWriter {
    fn needs_quotes(&self, field: &str, first: bool) -> bool {
        let b = field.as_bytes();
        if b.is_empty() {
            return false;
        }
        (first && self.comments && b[0] == b'#')
            || b.iter()
                .any(|&c| c == self.delimiter || c == b'"' || c == b'\n' || c == b'\r')
            || b[0] == b' '
            || b[0] == b'\t'
            || b[b.len() - 1] == b' '
            || b[b.len() - 1] == b'\t'
    }

    /// Write one record.
    pub fn write<'a, W: std::io::Write>(
        &self,
        out: &mut W,
        fields: impl Iterator<Item = &'a str>,
    ) -> std::io::Result<()> {
        let mut n = 0;
        let mut lone_empty = false;
        for (i, field) in fields.enumerate() {
            if i > 0 {
                out.write_all(&[self.delimiter])?;
            }
            if self.needs_quotes(field, i == 0) {
                out.write_all(b"\"")?;
                let mut rest = field;
                while let Some(pos) = rest.find('"') {
                    out.write_all(&rest.as_bytes()[..=pos])?;
                    out.write_all(b"\"")?;
                    rest = &rest[pos + 1..];
                }
                out.write_all(rest.as_bytes())?;
                out.write_all(b"\"")?;
            } else {
                out.write_all(field.as_bytes())?;
            }
            lone_empty = i == 0 && field.is_empty();
            n = i + 1;
        }
        if n == 1 && lone_empty {
            out.write_all(b"\"\"")?;
        }
        out.write_all(self.terminator)
    }
}

#[cfg(test)]
mod tests {
    use super::unique_column_names;

    #[test]
    fn unique_column_names_like_sqlite() {
        let mut names: Vec<String> = ["Id", "id", "id+1", "ID", "id+1", "a:1", "a:1", "x:"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        unique_column_names(&mut names);
        assert_eq!(
            names,
            ["Id", "id:1", "id+1", "ID:2", "id+1:1", "a:1", "a:2", "x:"]
        );
    }
}
