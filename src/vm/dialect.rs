//! `SqawkDialect`: the sqlparser dialect every statement is parsed with.
//!
//! It is Hive's dialect (identifier rules, `FILTER (WHERE ...)`, `LOAD DATA`,
//! ...) with SQLite's operator precedence. sqlparser's default precedence
//! table is PostgreSQL's, which differs from SQLite in ways that silently
//! change answers: `LIKE` binds looser than `=`, `<` binds as tightly as `=`,
//! `||` binds as tightly as `*`, and the right side of `IS [NOT] DISTINCT
//! FROM` swallows a whole expression (`a IS NOT DISTINCT FROM b AND c`
//! parsed as `a IS NOT DISTINCT FROM (b AND c)`).
//!
//! SQLite's levels, lowest first (each binary level left-associative):
//!
//! | level | operators                                                     |
//! |-------|---------------------------------------------------------------|
//! | 5     | `OR`                                                          |
//! | 10    | `AND`                                                         |
//! | 15    | `NOT` (prefix)                                                |
//! | 20    | `= == != <> IS [NOT] [DISTINCT FROM] IN LIKE MATCH REGEXP BETWEEN` |
//! | 22    | `< <= > >=`                                                   |
//! | 24    | `& \| << >>`                                                  |
//! | 30    | `+ -`                                                         |
//! | 40    | `* / %`                                                       |
//! | 45    | `\|\|`                                                        |
//! | 50    | `- + ~` (prefix)                                              |
//!
//! The dialect reports Hive's `TypeId`, so the parser's internal
//! `dialect_of!(self is HiveDialect)` checks keep selecting Hive syntax.

use std::any::TypeId;

use sqlparser::ast::Expr;
use sqlparser::dialect::{Dialect, HiveDialect, Precedence};
use sqlparser::keywords::Keyword;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::Token;

const PREC_OR: u8 = 5;
const PREC_AND: u8 = 10;
const PREC_NOT: u8 = 15;
/// `=`, `IS`, `IN`, `LIKE`, `BETWEEN`, ...: one level in SQLite.
const PREC_EQ: u8 = 20;
const PREC_CMP: u8 = 22;
const PREC_BITWISE: u8 = 24;
const PREC_ADD: u8 = 30;
const PREC_MUL: u8 = 40;
const PREC_CONCAT: u8 = 45;
/// Prefix `-`, `+`, `~`: tighter than every binary operator.
const PREC_UNARY: u8 = 50;

/// Hive's syntax with SQLite's operator precedence.
#[derive(Debug, Default, Clone, Copy)]
pub struct SqawkDialect;

const HIVE: HiveDialect = HiveDialect {};

impl Dialect for SqawkDialect {
    fn dialect(&self) -> TypeId {
        TypeId::of::<HiveDialect>()
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        HIVE.is_delimited_identifier_start(ch)
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        HIVE.is_identifier_start(ch)
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        HIVE.is_identifier_part(ch)
    }

    fn supports_filter_during_aggregation(&self) -> bool {
        HIVE.supports_filter_during_aggregation()
    }

    fn supports_numeric_prefix(&self) -> bool {
        HIVE.supports_numeric_prefix()
    }

    fn require_interval_qualifier(&self) -> bool {
        HIVE.require_interval_qualifier()
    }

    fn supports_bang_not_operator(&self) -> bool {
        HIVE.supports_bang_not_operator()
    }

    fn supports_load_data(&self) -> bool {
        HIVE.supports_load_data()
    }

    fn supports_table_sample_before_alias(&self) -> bool {
        HIVE.supports_table_sample_before_alias()
    }

    fn supports_group_by_with_modifier(&self) -> bool {
        HIVE.supports_group_by_with_modifier()
    }

    fn supports_from_first_insert(&self) -> bool {
        HIVE.supports_from_first_insert()
    }

    /// SQLite's `BEGIN DEFERRED | IMMEDIATE | EXCLUSIVE`.
    fn supports_start_transaction_modifier(&self) -> bool {
        true
    }

    fn prec_value(&self, prec: Precedence) -> u8 {
        match prec {
            Precedence::Period => 100,
            Precedence::DoubleColon => PREC_UNARY,
            Precedence::AtTz => PREC_MUL + 1,
            Precedence::MulDivModOp => PREC_MUL,
            Precedence::PlusMinus => PREC_ADD,
            Precedence::Xor | Precedence::Ampersand | Precedence::Caret | Precedence::Pipe => {
                PREC_BITWISE
            }
            Precedence::Colon => PREC_BITWISE,
            Precedence::Between | Precedence::Eq | Precedence::Like | Precedence::Is => PREC_EQ,
            Precedence::PgOther => PREC_NOT + 1,
            Precedence::UnaryNot => PREC_NOT,
            Precedence::And => PREC_AND,
            Precedence::Or => PREC_OR,
        }
    }

    /// The levels sqlparser's `Precedence` cannot express: `< <= > >=` above
    /// `=`, and `||` above `*`. Everything else maps through `prec_value`.
    fn get_next_precedence(&self, parser: &Parser) -> Option<Result<u8, ParserError>> {
        match parser.peek_token_ref().token {
            Token::Lt | Token::LtEq | Token::Gt | Token::GtEq => Some(Ok(PREC_CMP)),
            Token::StringConcat => Some(Ok(PREC_CONCAT)),
            Token::ShiftLeft | Token::ShiftRight => Some(Ok(PREC_BITWISE)),
            _ => None,
        }
    }

    /// Prefix `-`, `+` and `~` bind tighter than every binary operator
    /// (`-1 || 2` is `(-1) || 2`); sqlparser binds them at the `*` level.
    fn parse_prefix(&self, parser: &mut Parser) -> Option<Result<Expr, ParserError>> {
        use sqlparser::ast::UnaryOperator;
        let op = match parser.peek_token_ref().token {
            Token::Minus => UnaryOperator::Minus,
            Token::Plus => UnaryOperator::Plus,
            Token::Tilde => UnaryOperator::BitwiseNot,
            _ => return None,
        };
        parser.advance_token();
        Some(parser.parse_subexpr(PREC_UNARY).map(|expr| Expr::UnaryOp {
            op,
            expr: Box::new(expr),
        }))
    }

    /// `IS`, SQLite style: `x IS [NOT] [DISTINCT FROM] y` with `y` parsed at
    /// the `=` level. sqlparser parses a whole expression after `DISTINCT
    /// FROM` (`a IS NOT DISTINCT FROM b AND c` became `a IS NOT DISTINCT FROM
    /// (b AND c)`), and accepts only `NULL`, `TRUE`, `FALSE` and `UNKNOWN`
    /// after a bare `IS`, ending the operand there (`x IS NULL / 2` became
    /// `(x IS NULL) / 2`; SQLite reads `x IS (NULL / 2)`).
    ///
    /// `x IS y` is `x IS NOT DISTINCT FROM y` and `x IS NOT y` is `x IS
    /// DISTINCT FROM y`; a right side that is exactly `NULL`, `TRUE` or
    /// `FALSE` keeps its dedicated node (`IsNull`, `IsTrue`, ...).
    fn parse_infix(
        &self,
        parser: &mut Parser,
        expr: &Expr,
        _precedence: u8,
    ) -> Option<Result<Expr, ParserError>> {
        use sqlparser::ast::Value;
        let word = |parser: &Parser, n: usize, kw: Keyword| matches!(&parser.peek_nth_token_ref(n).token, Token::Word(w) if w.keyword == kw);
        if !word(parser, 0, Keyword::IS) {
            return None;
        }
        let negated = word(parser, 1, Keyword::NOT);
        let mut n = 1 + usize::from(negated);
        let distinct = word(parser, n, Keyword::DISTINCT) && word(parser, n + 1, Keyword::FROM);
        if distinct {
            n += 2;
        } else if let Token::Word(w) = &parser.peek_nth_token_ref(n).token {
            // `IS [NOT] UNKNOWN` and `IS [NOT] [form] NORMALIZED` stay
            // sqlparser's.
            if matches!(
                w.keyword,
                Keyword::UNKNOWN
                    | Keyword::NORMALIZED
                    | Keyword::NFC
                    | Keyword::NFD
                    | Keyword::NFKC
                    | Keyword::NFKD
            ) {
                return None;
            }
        }
        // The left operand must be cloned (sqlparser lends it), so a chain
        // `x IS y IS z ...` costs O(n^2). The depth limit bounds the chain:
        // fail before cloning a left operand that is already too deep.
        if left_spine_exceeds(expr, super::parse::MAX_EXPR_DEPTH) {
            return Some(Err(ParserError::ParserError(format!(
                "expression tree is too large (maximum depth {})",
                super::parse::MAX_EXPR_DEPTH
            ))));
        }
        for _ in 0..n {
            parser.advance_token();
        }
        let right = match parser.parse_subexpr(PREC_EQ) {
            Ok(r) => r,
            Err(e) => return Some(Err(e)),
        };
        let left = Box::new(expr.clone());
        let literal = match &right {
            Expr::Value(v) if !distinct => Some(&v.value),
            _ => None,
        };
        Some(Ok(match (literal, negated) {
            (Some(Value::Null), false) => Expr::IsNull(left),
            (Some(Value::Null), true) => Expr::IsNotNull(left),
            (Some(Value::Boolean(true)), false) => Expr::IsTrue(left),
            (Some(Value::Boolean(true)), true) => Expr::IsNotTrue(left),
            (Some(Value::Boolean(false)), false) => Expr::IsFalse(left),
            (Some(Value::Boolean(false)), true) => Expr::IsNotFalse(left),
            // `IS NOT DISTINCT FROM` and `IS` are both equality-with-NULLs.
            _ if negated != distinct => Expr::IsDistinctFrom(left, Box::new(right)),
            _ => Expr::IsNotDistinctFrom(left, Box::new(right)),
        }))
    }
}

/// Whether the leftmost path down from `expr` is longer than `max` nodes.
///
/// The parser builds a left-associative chain in a loop, so this path is the
/// one that can grow without bound; everything else nests by recursion and
/// is capped by the parser's recursion limit.
fn left_spine_exceeds(mut expr: &Expr, max: usize) -> bool {
    for _ in 0..=max {
        expr = match expr {
            Expr::BinaryOp { left, .. }
            | Expr::IsDistinctFrom(left, _)
            | Expr::IsNotDistinctFrom(left, _)
            | Expr::IsNull(left)
            | Expr::IsNotNull(left)
            | Expr::IsTrue(left)
            | Expr::IsNotTrue(left)
            | Expr::IsFalse(left)
            | Expr::IsNotFalse(left)
            | Expr::IsUnknown(left)
            | Expr::IsNotUnknown(left)
            | Expr::Like { expr: left, .. }
            | Expr::ILike { expr: left, .. }
            | Expr::SimilarTo { expr: left, .. }
            | Expr::RLike { expr: left, .. }
            | Expr::InList { expr: left, .. }
            | Expr::InSubquery { expr: left, .. }
            | Expr::Between { expr: left, .. }
            | Expr::Collate { expr: left, .. }
            | Expr::Cast { expr: left, .. }
            | Expr::AtTimeZone {
                timestamp: left, ..
            } => left,
            _ => return false,
        };
    }
    true
}
