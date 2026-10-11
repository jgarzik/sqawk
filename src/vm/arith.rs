//! Numeric semantics, following SQLite: how text becomes a number, how the
//! arithmetic operators combine integers and reals, and what CAST does.
//!
//! Integers stay 64-bit integers through `+ - * / %`; an operation whose
//! exact result does not fit is computed in floating point instead (SQLite's
//! rule), so `9223372036854775807 + 1` is a REAL rather than a wrapped or
//! saturated integer. Integer `/` truncates toward zero, `%` is the integer
//! remainder, and dividing by zero yields NULL.

use super::bytecode::Register;
use crate::error::{SqawkError, SqawkResult};

/// A number: SQLite's INTEGER or REAL.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum Num {
    Int(i64),
    Real(f64),
}

impl Num {
    pub(crate) fn as_f64(self) -> f64 {
        match self {
            Num::Int(i) => i as f64,
            Num::Real(f) => f,
        }
    }

    pub(crate) fn into_register(self) -> Register {
        match self {
            Num::Int(i) => Register::Integer(i),
            Num::Real(f) => Register::Float(f),
        }
    }
}

/// Length in bytes of the longest prefix of `b` (after leading whitespace)
/// that reads as a number, and whether that prefix is a real (has a point
/// or an exponent). `(start, end, is_real)`; `end == start` when there is no
/// number at all.
fn numeric_prefix(b: &[u8]) -> (usize, usize, bool) {
    let mut i = 0;
    while i < b.len() && b[i].is_ascii_whitespace() {
        i += 1;
    }
    let start = i;
    if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
        i += 1;
    }
    let int_start = i;
    while i < b.len() && b[i].is_ascii_digit() {
        i += 1;
    }
    let mut digits = i - int_start;
    let mut real = false;
    if i < b.len() && b[i] == b'.' {
        let frac_start = i + 1;
        let mut j = frac_start;
        while j < b.len() && b[j].is_ascii_digit() {
            j += 1;
        }
        if digits > 0 || j > frac_start {
            digits += j - frac_start;
            real = true;
            i = j;
        }
    }
    if digits == 0 {
        return (start, start, false);
    }
    // An exponent counts only when at least one digit follows it.
    if i < b.len() && (b[i] == b'e' || b[i] == b'E') {
        let mut j = i + 1;
        if j < b.len() && (b[j] == b'+' || b[j] == b'-') {
            j += 1;
        }
        let exp_start = j;
        while j < b.len() && b[j].is_ascii_digit() {
            j += 1;
        }
        if j > exp_start {
            real = true;
            i = j;
        }
    }
    (start, i, real)
}

/// Text as a number, the way SQLite's arithmetic reads it: the longest
/// numeric prefix, so `'12abc'` is 12, `' 5 '` is 5, `'1e3'` is the real
/// 1000 and `'abc'` is 0. An integer too large for i64 becomes a real.
pub(crate) fn text_to_num(s: &str) -> Num {
    let b = s.as_bytes();
    let (start, end, real) = numeric_prefix(b);
    if start == end {
        return Num::Int(0);
    }
    let text = &s[start..end];
    if !real {
        if let Ok(i) = text.parse::<i64>() {
            return Num::Int(i);
        }
    }
    Num::Real(text.parse::<f64>().unwrap_or(0.0))
}

/// Text as an integer, the way `CAST(text AS INTEGER)` reads it: an optional
/// sign and the digits after it, ignoring anything from the first non-digit
/// on (`'3.9'` is 3, `'1e3'` is 1). Out-of-range values saturate.
pub(crate) fn text_to_int(s: &str) -> i64 {
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() && b[i].is_ascii_whitespace() {
        i += 1;
    }
    let neg = i < b.len() && b[i] == b'-';
    if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
        i += 1;
    }
    let mut acc: i64 = 0;
    while i < b.len() && b[i].is_ascii_digit() {
        let d = (b[i] - b'0') as i64;
        acc = match acc.checked_mul(10).and_then(|v| {
            if neg {
                v.checked_sub(d)
            } else {
                v.checked_add(d)
            }
        }) {
            Some(v) => v,
            None => return if neg { i64::MIN } else { i64::MAX },
        };
        i += 1;
    }
    acc
}

/// 2^63, exactly representable as an f64.
const TWO_63: f64 = 9_223_372_036_854_775_808.0;

/// A real truncated toward zero, saturating at the ends of the i64 range
/// (SQLite's `CAST(real AS INTEGER)`).
pub(crate) fn real_to_int(f: f64) -> i64 {
    if f.is_nan() {
        0
    } else if f >= TWO_63 {
        i64::MAX
    } else if f <= -TWO_63 {
        i64::MIN
    } else {
        f as i64
    }
}

/// A register as a number for arithmetic; `None` for NULL.
pub(crate) fn to_num(r: &Register) -> Option<Num> {
    match r {
        Register::Integer(i) => Some(Num::Int(*i)),
        Register::Float(f) => Some(Num::Real(*f)),
        Register::Boolean(b) => Some(Num::Int(*b as i64)),
        Register::String(s) => Some(text_to_num(s)),
        Register::Null => None,
    }
}

/// A register as a 64-bit integer, as SQLite's `sqlite3VdbeIntValue` reads
/// it for the bitwise operators: text by its integer prefix (`'12abc'` is
/// 12, `'1e3'` is 1), reals truncated toward zero (saturating), NULL 0.
pub(crate) fn int_value(r: &Register) -> i64 {
    match r {
        Register::Integer(i) => *i,
        Register::Boolean(b) => *b as i64,
        Register::Float(f) => real_to_int(*f),
        Register::String(s) => text_to_int(s),
        Register::Null => 0,
    }
}

/// A bitwise operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BitOp {
    And,
    Or,
    ShiftLeft,
    ShiftRight,
}

/// `a op b` for `&`, `|`, `<<`, `>>` with SQLite semantics: NULL if either
/// side is NULL, else both sides as integers ([`int_value`]). A negative
/// shift count shifts the other way; shifting by 64 or more gives 0, or -1
/// for `>>` of a negative number; `>>` extends the sign.
pub(crate) fn bitwise(op: BitOp, a: &Register, b: &Register) -> Register {
    if matches!(a, Register::Null) || matches!(b, Register::Null) {
        return Register::Null;
    }
    let ia = int_value(a);
    let mut ib = int_value(b);
    Register::Integer(match op {
        BitOp::And => ia & ib,
        BitOp::Or => ia | ib,
        BitOp::ShiftLeft | BitOp::ShiftRight => {
            let mut left = op == BitOp::ShiftLeft;
            if ib < 0 {
                left = !left;
                ib = if ib > -64 { -ib } else { 64 };
            }
            if ib >= 64 {
                if ia >= 0 || left {
                    0
                } else {
                    -1
                }
            } else if left {
                ((ia as u64) << ib) as i64
            } else {
                ia >> ib
            }
        }
    })
}

/// The truth value of a register in a condition, as in SQLite: a number is
/// true when non-zero, and text is read as a number first, so `'x'` is false
/// and `'1x'` true. NULL is not true.
pub(crate) fn is_true(r: &Register) -> bool {
    match r {
        Register::Integer(v) => *v != 0,
        Register::Float(v) => *v != 0.0,
        Register::Boolean(v) => *v,
        Register::String(s) => text_to_num(s).as_f64() != 0.0,
        Register::Null => false,
    }
}

/// An arithmetic operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
    Rem,
}

/// `a op b` with SQLite semantics; NULL if either side is NULL, or on
/// division by zero.
pub(crate) fn arith(op: ArithOp, a: &Register, b: &Register) -> Register {
    let (Some(x), Some(y)) = (to_num(a), to_num(b)) else {
        return Register::Null;
    };
    match (x, y) {
        (Num::Int(x), Num::Int(y)) => {
            let exact = match op {
                ArithOp::Add => x.checked_add(y),
                ArithOp::Sub => x.checked_sub(y),
                ArithOp::Mul => x.checked_mul(y),
                ArithOp::Div => {
                    if y == 0 {
                        return Register::Null;
                    }
                    x.checked_div(y)
                }
                ArithOp::Rem => {
                    if y == 0 {
                        return Register::Null;
                    }
                    // i64::MIN % -1 is mathematically 0.
                    Some(x.checked_rem(y).unwrap_or(0))
                }
            };
            match exact {
                Some(v) => Register::Integer(v),
                None => real_arith(op, x as f64, y as f64),
            }
        }
        (x, y) => real_arith(op, x.as_f64(), y.as_f64()),
    }
}

fn real_arith(op: ArithOp, x: f64, y: f64) -> Register {
    let r = match op {
        ArithOp::Add => x + y,
        ArithOp::Sub => x - y,
        ArithOp::Mul => x * y,
        ArithOp::Div => {
            if y == 0.0 {
                return Register::Null;
            }
            x / y
        }
        // SQLite takes the remainder of the operands truncated to integers,
        // but returns it as a real: 5.5 % 2 is 1.0.
        ArithOp::Rem => {
            let (ix, iy) = (real_to_int(x), real_to_int(y));
            if iy == 0 {
                return Register::Null;
            }
            let iy = if iy == -1 { 1 } else { iy };
            return Register::Float((ix % iy) as f64);
        }
    };
    if r.is_nan() {
        Register::Null
    } else {
        Register::Float(r)
    }
}

/// `CAST(value AS type)`. `type` is the compiler's normalized name.
pub(crate) fn cast(r: Register, target: &str) -> Register {
    if matches!(r, Register::Null) {
        return Register::Null;
    }
    match target {
        "INTEGER" => Register::Integer(match r {
            Register::Integer(i) => i,
            Register::Float(f) => real_to_int(f),
            Register::Boolean(b) => b as i64,
            Register::String(s) => text_to_int(&s),
            Register::Null => unreachable!(),
        }),
        "REAL" => Register::Float(match r {
            Register::String(s) => text_to_num(&s).as_f64(),
            other => to_num(&other).map_or(0.0, Num::as_f64),
        }),
        // NUMERIC: text that is a number in full becomes that number (a real
        // with no fractional part becomes an integer); other text its numeric
        // prefix. BOOLEAN has NUMERIC affinity in SQLite, so it yields 1/0
        // for true/false values.
        "NUMERIC" | "BOOLEAN" => match r {
            Register::String(s) => match text_to_num(&s) {
                Num::Real(f) if f.fract() == 0.0 && f.abs() < 9.2e18 => Register::Integer(f as i64),
                n => n.into_register(),
            },
            Register::Boolean(b) => Register::Integer(b as i64),
            other => other,
        },
        _ => Register::String(text_of(&r)),
    }
}

/// The first `p` (>= 1) significant decimal digits of the finite, nonzero
/// `v`'s magnitude, rounded half up on its exact decimal expansion as
/// SQLite's `sqlite3FpDecode` rounds (so 0.25 to one digit is 3, where C's
/// printf gives 2), and the decimal exponent of the first digit.
fn decimal_digits(v: f64, p: usize) -> (Vec<u8>, i32) {
    // Fast path: the shortest digits that read back as `v`, when there are
    // at most `p` <= 15 of them, are also its rounding to `p` digits (`v`
    // is within half an ulp of them, far less than half a unit in the 15th
    // digit), and need no exact expansion. Not for subnormals, whose ulp
    // is large relative to them (5e-324 is 4.94065645841247e-324).
    if p <= 15 && v.abs() >= f64::MIN_POSITIVE {
        let s = format!("{:e}", v.abs());
        if let Some((mant, exp)) = s.split_once('e') {
            let d: Vec<u8> = mant
                .bytes()
                .filter(u8::is_ascii_digit)
                .map(|b| b - b'0')
                .collect();
            if d.len() <= p {
                return (d, exp.parse().unwrap_or(0));
            }
        }
    }
    // Ten guard digits decide the rounding: only a run of ten 9s after the
    // cut could carry into the digit that decides it.
    let s = format!("{:.*e}", p + 10, v.abs());
    let (mant, exp) = s.split_once('e').unwrap_or((&s, "0"));
    let mut exp: i32 = exp.parse().unwrap_or(0);
    let all: Vec<u8> = mant
        .bytes()
        .filter(u8::is_ascii_digit)
        .map(|b| b - b'0')
        .collect();
    let mut d = all[..p].to_vec();
    if all[p] >= 5 {
        let mut i = p;
        loop {
            if i == 0 {
                // 99..9 rounded up: 100..0, one place higher.
                d.insert(0, 1);
                d.truncate(p);
                exp += 1;
                break;
            }
            i -= 1;
            if d[i] == 9 {
                d[i] = 0;
            } else {
                d[i] += 1;
                break;
            }
        }
    }
    (d, exp)
}

/// Digits past which `%g` precision is not honoured (every digit past a
/// double's exact expansion is a zero, which `%g` strips anyway).
const MAX_GENERAL_DIGITS: usize = 1100;

/// `%g` with precision `p` (significant digits, at least 1) of a finite
/// `v`, as SQLite's printf renders it: trailing zeros removed, exponent
/// form when the exponent after rounding is below -4 or at least `p` (so
/// 999999.5 at six digits is `1e+06`), digits rounded half up.
pub(crate) fn c_general(v: f64, p: usize, upper: bool) -> String {
    if v == 0.0 {
        return "0".into();
    }
    let p = p.clamp(1, MAX_GENERAL_DIGITS);
    let (mut d, x) = decimal_digits(v, p);
    while d.len() > 1 && d.last() == Some(&0) {
        d.pop();
    }
    let digit = |b: u8| (b'0' + b) as char;
    let mut out = String::with_capacity(d.len() + 8);
    if v < 0.0 {
        out.push('-');
    }
    if x < -4 || x >= p as i32 {
        out.push(digit(d[0]));
        if d.len() > 1 {
            out.push('.');
            out.extend(d[1..].iter().map(|&b| digit(b)));
        }
        out.push(if upper { 'E' } else { 'e' });
        out.push(if x < 0 { '-' } else { '+' });
        out.push_str(&format!("{:02}", x.unsigned_abs()));
    } else if x < 0 {
        out.push_str("0.");
        out.extend(std::iter::repeat_n('0', (-x - 1) as usize));
        out.extend(d.iter().map(|&b| digit(b)));
    } else {
        let int_len = x as usize + 1;
        for i in 0..int_len {
            out.push(digit(d.get(i).copied().unwrap_or(0)));
        }
        if d.len() > int_len {
            out.push('.');
            out.extend(d[int_len..].iter().map(|&b| digit(b)));
        }
    }
    out
}

/// A REAL's text, exactly as SQLite's `CAST(x AS TEXT)` renders it
/// (`%!.15g`): 15 significant digits, an integral value keeps a `.0`
/// (`2.0`, `1.0e+20`), infinities are `Inf` / `-Inf`, and negative zero is
/// `0.0`. NaN, which SQLite turns into NULL, renders as the empty string.
pub(crate) fn real_text(f: f64) -> String {
    if f.is_nan() {
        return String::new();
    }
    if f.is_infinite() {
        return if f > 0.0 { "Inf" } else { "-Inf" }.into();
    }
    if f == 0.0 {
        return "0.0".into();
    }
    let mut s = c_general(f, 15, false);
    if !s.contains('.') {
        match s.find('e') {
            Some(at) => s.insert_str(at, ".0"),
            None => s.push_str(".0"),
        }
    }
    s
}

/// A non-NULL register's text form, as functions and `||` see it.
pub(crate) fn text_of(r: &Register) -> String {
    match r {
        Register::String(s) => s.clone(),
        Register::Integer(i) => i.to_string(),
        Register::Float(f) => real_text(*f),
        Register::Boolean(b) => (*b as i64).to_string(),
        Register::Null => String::new(),
    }
}

/// Exact comparison of an integer with a real (SQLite's
/// `sqlite3IntFloatCompare`): no rounding of the integer to f64, so
/// i64::MAX is less than the real 2^63.
pub(crate) fn cmp_int_real(i: i64, f: f64) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    if f.is_nan() {
        return Ordering::Greater;
    }
    if f >= TWO_63 {
        return Ordering::Less;
    }
    if f < -TWO_63 {
        return Ordering::Greater;
    }
    let t = f.trunc();
    match i.cmp(&(t as i64)) {
        Ordering::Equal => 0.0f64.partial_cmp(&(f - t)).unwrap_or(Ordering::Equal),
        o => o,
    }
}

/// Exact ordering of two numbers.
pub(crate) fn cmp_num(a: Num, b: Num) -> std::cmp::Ordering {
    match (a, b) {
        (Num::Int(x), Num::Int(y)) => x.cmp(&y),
        (Num::Int(x), Num::Real(y)) => cmp_int_real(x, y),
        (Num::Real(x), Num::Int(y)) => cmp_int_real(y, x).reverse(),
        (Num::Real(x), Num::Real(y)) => x.partial_cmp(&y).unwrap_or(std::cmp::Ordering::Equal),
    }
}

/// The "integer overflow" error SQLite raises for SUM and ABS.
pub(crate) fn overflow() -> SqawkError {
    SqawkError::InvalidSqlQuery("integer overflow".into())
}

/// `checked` or the integer-overflow error.
pub(crate) fn checked(v: Option<i64>) -> SqawkResult<i64> {
    v.ok_or_else(overflow)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn n(s: &str) -> Num {
        text_to_num(s)
    }

    #[test]
    fn text_prefixes() {
        assert_eq!(n("12abc"), Num::Int(12));
        assert_eq!(n(" 5 "), Num::Int(5));
        assert_eq!(n("abc"), Num::Int(0));
        assert_eq!(n("1e3"), Num::Real(1000.0));
        assert_eq!(n("1e"), Num::Int(1));
        assert_eq!(n("007"), Num::Int(7));
        assert_eq!(n(".5"), Num::Real(0.5));
        assert_eq!(n("-"), Num::Int(0));
        assert_eq!(n("0x10"), Num::Int(0));
        assert_eq!(n("3.9x"), Num::Real(3.9));
        assert_eq!(n("99999999999999999999"), Num::Real(1e20));
    }

    #[test]
    fn cast_int_prefix() {
        assert_eq!(text_to_int("3.9"), 3);
        assert_eq!(text_to_int("12abc"), 12);
        assert_eq!(text_to_int(" 5 "), 5);
        assert_eq!(text_to_int("1e3"), 1);
        assert_eq!(text_to_int("-3.9"), -3);
        assert_eq!(text_to_int("9999999999999999999"), i64::MAX);
        assert_eq!(text_to_int("-9999999999999999999"), i64::MIN);
        assert_eq!(real_to_int(1e30), i64::MAX);
        assert_eq!(real_to_int(-1e30), i64::MIN);
    }

    #[test]
    fn integer_ops() {
        let i = Register::Integer;
        let r = |op, a, b| arith(op, &i(a), &i(b));
        assert!(matches!(r(ArithOp::Div, 5, 2), Register::Integer(2)));
        assert!(matches!(r(ArithOp::Div, -7, 2), Register::Integer(-3)));
        assert!(matches!(r(ArithOp::Rem, -7, 3), Register::Integer(-1)));
        assert!(matches!(r(ArithOp::Div, 5, 0), Register::Null));
        assert!(matches!(r(ArithOp::Add, i64::MAX, 1), Register::Float(_)));
        assert!(matches!(r(ArithOp::Div, i64::MIN, -1), Register::Float(_)));
        assert!(matches!(
            r(ArithOp::Rem, i64::MIN, -1),
            Register::Integer(0)
        ));
        assert!(matches!(
            arith(ArithOp::Rem, &Register::Float(5.5), &i(2)),
            Register::Float(f) if f == 1.0
        ));
        assert!(matches!(
            arith(ArithOp::Rem, &i(5), &Register::Float(0.5)),
            Register::Null
        ));
    }
}
