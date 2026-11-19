//! Collating sequences: SQLite's built-in BINARY, NOCASE and RTRIM, named
//! with an explicit `COLLATE`.
//!
//! A collation is applied as a key transform: NOCASE folds ASCII upper case
//! to lower case and RTRIM drops trailing spaces, and comparing the
//! transformed texts byte by byte is exactly comparing the originals under
//! the collation. Only text is transformed, and the transform never changes
//! a text's numeric reading, so sqawk's per-cell comparison rules are
//! unaffected.
//!
//! A collation is honoured in comparisons, BETWEEN and IN lists (the left
//! operand's, else the right's, as in SQLite) and in ORDER BY and GROUP BY
//! terms. Anywhere else `COLLATE` is an error rather than silently ignored.

use std::borrow::Cow;

use sqlparser::ast::Expr;

use crate::error::{SqawkError, SqawkResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Collation {
    #[default]
    Binary,
    NoCase,
    Rtrim,
}

impl Collation {
    /// The collation `name` (any case); SQLite's error for any other.
    pub fn parse(name: &str) -> SqawkResult<Self> {
        match name.to_ascii_uppercase().as_str() {
            "BINARY" => Ok(Collation::Binary),
            "NOCASE" => Ok(Collation::NoCase),
            "RTRIM" => Ok(Collation::Rtrim),
            _ => Err(SqawkError::InvalidSqlQuery(format!(
                "no such collation sequence: {name}"
            ))),
        }
    }

    /// The function-table entry that applies this collation's transform to
    /// a register; None for BINARY, which has none.
    pub fn func_name(self) -> Option<&'static str> {
        match self {
            Collation::Binary => None,
            Collation::NoCase => Some("COLLATE NOCASE"),
            Collation::Rtrim => Some("COLLATE RTRIM"),
        }
    }

    /// The suffix naming this collation in a sort key spec.
    pub fn spec(self) -> &'static str {
        match self {
            Collation::Binary => "",
            Collation::NoCase => "nocase",
            Collation::Rtrim => "rtrim",
        }
    }

    pub fn from_spec(s: &str) -> Option<Self> {
        match s {
            "nocase" => Some(Collation::NoCase),
            "rtrim" => Some(Collation::Rtrim),
            _ => None,
        }
    }

    /// `s` transformed so that byte order is this collation's order.
    pub fn fold(self, s: &str) -> Cow<'_, str> {
        match self {
            Collation::Binary => Cow::Borrowed(s),
            Collation::NoCase if s.bytes().any(|b| b.is_ascii_uppercase()) => {
                Cow::Owned(s.to_ascii_lowercase())
            }
            Collation::NoCase => Cow::Borrowed(s),
            Collation::Rtrim => Cow::Borrowed(s.trim_end_matches(' ')),
        }
    }
}

/// `e` without a `COLLATE` at its top (looking through parentheses), and
/// the collation named there.
pub fn split(e: &Expr) -> SqawkResult<(&Expr, Option<Collation>)> {
    let mut cur = e;
    loop {
        match cur {
            Expr::Nested(inner) => cur = inner,
            Expr::Collate { expr, collation } => {
                let name = collation.to_string();
                let name = name.trim_matches(|c| c == '"' || c == '`' || c == '\'');
                let coll = Collation::parse(name)?;
                // `x COLLATE a COLLATE b`: the outermost applies.
                let (inner, _) = split(expr)?;
                return Ok((inner, Some(coll)));
            }
            _ => return Ok((e, None)),
        }
    }
}

/// The error for `COLLATE` where it is not applied.
pub fn unsupported() -> SqawkError {
    SqawkError::UnsupportedSqlFeature(
        "COLLATE is not supported here, only in comparisons, BETWEEN, IN lists, ORDER BY and \
         GROUP BY"
            .into(),
    )
}
