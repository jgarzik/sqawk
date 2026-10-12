//! What the SQL generator knows about each table and expression: kind and
//! the text properties that decide whether sqawk and SQLite must agree.
//!
//! sqawk compares numeric-looking text as a number (doc/sql_reference.md,
//! "Type coercion"; "Differences from SQLite", row "Column types"); SQLite
//! compares text with text byte by byte and puts every number before every
//! text. The two rules give the same answer when
//!
//! * both sides are numbers;
//! * a number meets text that has no numeric reading (number first in both);
//! * two texts meet and neither has a numeric reading (bytes in both); or
//! * one text is "letter-led" (first byte at or above `:` = 0x3A): every
//!   numeric reading starts with whitespace, a sign, a point or a digit, all
//!   below 0x3A, so byte order and sqawk's "number before text" agree.
//!
//! The generator only emits comparisons, orderings and MIN/MAX that one of
//! these cases covers, which it decides from `Props`.

use std::rc::Rc;

use rusqlite::types::Value;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// Integers (or NULL) only.
    Int,
    /// Numbers (or NULL) only.
    Real,
    /// Text (or NULL) only.
    Text,
    /// Anything: used only where every type behaves alike.
    Mixed,
}

#[derive(Clone, Copy, Debug)]
pub struct Props {
    pub kind: Kind,
    /// Text: no value has a numeric reading.
    pub nonnum: bool,
    /// Text: every value is non-empty and starts with a byte >= 0x3A.
    pub letter: bool,
    /// Every value is ASCII (UPPER / LOWER agree: "Differences", UPPER row).
    pub ascii: bool,
    /// No whitespace but the space character (TRIM agrees: "Differences",
    /// TRIM row).
    pub wssafe: bool,
    /// TEXT affinity (`CAST(x AS TEXT)`): SQLite converts a number it is
    /// compared with to text.
    pub taff: bool,
}

impl Props {
    pub const INT: Props = Props {
        kind: Kind::Int,
        nonnum: false,
        letter: false,
        ascii: true,
        wssafe: true,
        taff: false,
    };
    pub const REAL: Props = Props {
        kind: Kind::Real,
        ..Props::INT
    };
    pub const MIXED: Props = Props {
        kind: Kind::Mixed,
        ascii: false,
        wssafe: false,
        ..Props::INT
    };
    /// Text known to be a lower/upper-case ASCII word.
    pub const WORD: Props = Props {
        kind: Kind::Text,
        nonnum: true,
        letter: true,
        ascii: true,
        wssafe: true,
        taff: false,
    };
    /// Text about which nothing is known.
    pub const TEXT: Props = Props {
        kind: Kind::Text,
        nonnum: false,
        letter: false,
        ascii: false,
        wssafe: false,
        taff: false,
    };

    pub fn is_num(&self) -> bool {
        matches!(self.kind, Kind::Int | Kind::Real)
    }

    pub fn is_text(&self) -> bool {
        self.kind == Kind::Text
    }

    /// Sorts the same in both engines (ORDER BY key, MIN/MAX argument).
    pub fn sortable(&self) -> bool {
        self.is_num() || (self.is_text() && self.nonnum)
    }

    /// The properties of one text value.
    pub fn of_text(s: &str) -> Props {
        Props {
            kind: Kind::Text,
            nonnum: !has_numeric_reading(s),
            letter: s.as_bytes().first().is_some_and(|&b| b >= 0x3A),
            ascii: s.is_ascii(),
            wssafe: !s.chars().any(|c| c.is_whitespace() && c != ' '),
            taff: false,
        }
    }

    /// Properties holding for both `a` and `b` (a value from either).
    pub fn join(a: Props, b: Props) -> Props {
        let kind = match (a.kind, b.kind) {
            (x, y) if x == y => x,
            (Kind::Int, Kind::Real) | (Kind::Real, Kind::Int) => Kind::Real,
            _ => Kind::Mixed,
        };
        Props {
            kind,
            nonnum: a.nonnum && b.nonnum,
            letter: a.letter && b.letter,
            ascii: a.ascii && b.ascii,
            wssafe: a.wssafe && b.wssafe,
            taff: a.taff || b.taff,
        }
    }

    /// The properties of a column holding `values` (NULLs ignored).
    pub fn of_values<'a>(values: impl Iterator<Item = &'a Value>) -> Props {
        let mut acc: Option<Props> = None;
        for v in values {
            let p = match v {
                Value::Null => continue,
                Value::Integer(_) => Props::INT,
                Value::Real(_) => Props::REAL,
                Value::Text(s) => Props::of_text(s),
                Value::Blob(_) => Props::MIXED,
            };
            acc = Some(match acc {
                None => p,
                Some(a) => Props::join(a, p),
            });
        }
        // An all-NULL column compares like an integer one.
        acc.unwrap_or(Props::INT)
    }

    /// May a value of `self` be compared with a value of `other`?
    pub fn comparable(&self, other: &Props) -> bool {
        let (a, b) = (self, other);
        if a.kind == Kind::Mixed || b.kind == Kind::Mixed {
            return false;
        }
        if a.is_num() && b.is_num() {
            return true;
        }
        if a.is_text() && b.is_text() {
            return (a.nonnum && b.nonnum) || a.letter || b.letter;
        }
        // One number, one text.
        let t = if a.is_text() { a } else { b };
        t.letter || (t.nonnum && !t.taff)
    }
}

/// sqawk's numeric reading of text (doc "Type coercion"): optional ASCII
/// whitespace, sign, digits with an optional point (at least one digit),
/// optional exponent, optional whitespace.
pub fn has_numeric_reading(s: &str) -> bool {
    let b = s.trim_matches(|c: char| c.is_ascii_whitespace()).as_bytes();
    let mut i = 0;
    if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
        i += 1;
    }
    let mut digits = 0;
    while i < b.len() && b[i].is_ascii_digit() {
        i += 1;
        digits += 1;
    }
    if i < b.len() && b[i] == b'.' {
        i += 1;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
            digits += 1;
        }
    }
    if digits == 0 {
        return false;
    }
    if i < b.len() && (b[i] == b'e' || b[i] == b'E') {
        i += 1;
        if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
            i += 1;
        }
        let start = i;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        if i == start {
            return false;
        }
    }
    i == b.len()
}

#[derive(Clone, Debug)]
pub struct Col {
    pub name: String,
    pub props: Props,
}

/// A table, derived table or CTE the generator can put in a FROM clause.
#[derive(Clone, Debug)]
pub struct Rel {
    /// The name in SQL (a table or CTE name; for a derived table, unused).
    pub name: String,
    pub cols: Vec<Col>,
    /// A column whose values are distinct and never NULL.
    pub key: Option<usize>,
    /// Row count (an upper bound for derived relations).
    pub card: usize,
    /// (column, referenced table, referenced column) join paths.
    pub fks: Vec<(usize, String, usize)>,
}

impl Rel {
    pub fn col_index(&self, name: &str) -> Option<usize> {
        self.cols.iter().position(|c| c.name == name)
    }
}

pub struct Schema {
    pub rels: Vec<Rc<Rel>>,
    /// Hierarchies for recursive walks: (table, id column, parent column).
    pub trees: Vec<(String, String, String)>,
    /// Text values from the data, for WHERE clauses that select something.
    pub text_lits: Vec<String>,
    /// Columns holding numbers near the i64 limits (SUM / ABS of them can
    /// overflow).
    pub big_cols: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numeric_reading() {
        for s in ["007", " 1.5 ", "1e3", "+3", "-0", ".5", "5.", "1E+2"] {
            assert!(has_numeric_reading(s), "{s}");
        }
        for s in ["12abc", "0x10", "inf", "NaN", "", " ", ".", "1e", "e3", "-"] {
            assert!(!has_numeric_reading(s), "{s}");
        }
    }

    #[test]
    fn comparability() {
        let num_text = Props::of_text("10");
        let empty = Props::of_text("");
        let word = Props::of_text("abc");
        assert!(num_text.comparable(&word));
        assert!(!num_text.comparable(&empty));
        assert!(!num_text.comparable(&Props::INT));
        assert!(Props::INT.comparable(&word));
        assert!(Props::INT.comparable(&empty));
        assert!(!Props::MIXED.comparable(&Props::INT));
    }
}
