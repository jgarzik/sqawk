//! Scalar functions: one table of every built-in, run by the `Function`
//! opcode (the analogue of SQLite's OP_Function and its FuncDef table).
//!
//! Each function receives its evaluated arguments as a register slice and
//! returns one register. The compiler checks the argument count against the
//! table, so the bodies may index their arguments freely within the declared
//! range. Functions that must not evaluate every argument (COALESCE, IFNULL,
//! IIF, NULLIF) are compiled to control flow instead and are not here.
//!
//! Semantics follow SQLite 3.45 unless a comment says otherwise. String
//! functions count characters, not bytes.

use super::arith::{self, text_of, text_to_num, to_num, Num};
use super::bytecode::Register;
use super::datetime;
use super::engine::{compare_values, VmEngine};
use super::printf;
use crate::error::{SqawkError, SqawkResult};
use crate::table::Value;

/// A scalar function body.
pub(crate) type ScalarFn = fn(&[Register]) -> SqawkResult<Register>;

/// One built-in scalar function.
pub(crate) struct FuncDef {
    pub name: &'static str,
    pub min_args: usize,
    /// `usize::MAX` for variadic.
    pub max_args: usize,
    pub body: ScalarFn,
}

const VARIADIC: usize = usize::MAX;

macro_rules! f {
    ($name:literal, $min:expr, $max:expr, $body:expr) => {
        FuncDef {
            name: $name,
            min_args: $min,
            max_args: $max,
            body: $body,
        }
    };
}

/// Every scalar function, by upper-case name.
static FUNCS: &[FuncDef] = &[
    // Strings
    f!("UPPER", 1, 1, upper),
    f!("LOWER", 1, 1, lower),
    f!("LENGTH", 1, 1, length),
    f!("TRIM", 1, 2, trim),
    f!("LTRIM", 1, 2, ltrim),
    f!("RTRIM", 1, 2, rtrim),
    f!("SUBSTR", 2, 3, substr),
    f!("SUBSTRING", 2, 3, substr),
    f!("REPLACE", 3, 3, replace),
    f!("INSTR", 2, 2, instr),
    f!("CONCAT", 1, VARIADIC, concat),
    f!("CONCAT_WS", 2, VARIADIC, concat_ws),
    f!("||", 2, 2, concat_op),
    f!("&", 2, 2, bit_and),
    f!("|", 2, 2, bit_or),
    f!("<<", 2, 2, shift_left),
    f!(">>", 2, 2, shift_right),
    f!("~", 1, 1, bit_not),
    // `a IS NOT DISTINCT FROM b`: NULL-safe equality.
    f!("IS", 2, 2, is_same),
    f!("LEFT", 2, 2, left),
    f!("RIGHT", 2, 2, right),
    f!("LPAD", 2, 3, lpad),
    f!("RPAD", 2, 3, rpad),
    f!("HEX", 1, 1, hex),
    f!("CHAR", 0, VARIADIC, char_fn),
    f!("UNICODE", 1, 1, unicode),
    f!("PRINTF", 1, VARIADIC, printf),
    f!("FORMAT", 1, VARIADIC, printf),
    f!("QUOTE", 1, 1, quote),
    // Numbers
    f!("ABS", 1, 1, abs),
    f!("ROUND", 1, 2, round),
    f!("CEIL", 1, 1, ceil),
    f!("CEILING", 1, 1, ceil),
    f!("FLOOR", 1, 1, floor),
    f!("SIGN", 1, 1, sign),
    // Multi-argument MIN / MAX (one argument is the aggregate).
    f!("MIN", 2, VARIADIC, min_scalar),
    f!("MAX", 2, VARIADIC, max_scalar),
    // Types
    f!("TYPEOF", 1, 1, type_of),
    // Dates
    f!("DATE", 0, VARIADIC, datetime::date),
    f!("TIME", 0, VARIADIC, datetime::time),
    f!("DATETIME", 0, VARIADIC, datetime::datetime),
    f!("JULIANDAY", 0, VARIADIC, datetime::julianday),
    f!("UNIXEPOCH", 0, VARIADIC, datetime::unixepoch),
    f!("STRFTIME", 1, VARIADIC, datetime::strftime),
    f!("TIMEDIFF", 2, 2, datetime::timediff),
    f!("CURRENT_DATE", 0, 0, datetime::current_date),
    f!("CURRENT_TIME", 0, 0, datetime::current_time),
    f!("CURRENT_TIMESTAMP", 0, 0, datetime::current_timestamp),
    f!("NOW", 0, 0, datetime::current_timestamp),
];

/// The names of the built-in scalar functions callable by name (upper case;
/// operator entries such as `||` are left out).
pub(crate) fn names() -> impl Iterator<Item = &'static str> {
    FUNCS
        .iter()
        .map(|f| f.name)
        .filter(|n| n.chars().all(|c| c.is_ascii_alphabetic() || c == '_'))
}

/// The built-in scalar function named `name` (upper case).
pub(crate) fn lookup(name: &str) -> Option<&'static FuncDef> {
    FUNCS.iter().find(|f| f.name == name)
}

/// Check a call's argument count against the function's definition.
pub(crate) fn check_arity(def: &FuncDef, n: usize) -> SqawkResult<()> {
    if n < def.min_args || n > def.max_args {
        let want = match (def.min_args, def.max_args) {
            (a, b) if a == b => format!("{}", a),
            (a, VARIADIC) => format!("at least {}", a),
            (a, b) => format!("{} to {}", a, b),
        };
        return Err(SqawkError::InvalidSqlQuery(format!(
            "wrong number of arguments to function {}(): expected {}, got {}",
            def.name, want, n
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// NULL if any argument is NULL.
fn any_null(args: &[Register]) -> bool {
    args.iter().any(|a| matches!(a, Register::Null))
}

macro_rules! null_in_null_out {
    ($args:expr) => {
        if any_null($args) {
            return Ok(Register::Null);
        }
    };
}

/// An integer argument (SUBSTR positions, LPAD lengths, ...): numbers are
/// truncated, text read by its numeric prefix.
fn int_arg(r: &Register) -> i64 {
    match to_num(r) {
        Some(Num::Int(i)) => i,
        Some(Num::Real(f)) => arith::real_to_int(f),
        None => 0,
    }
}

fn text(s: String) -> SqawkResult<Register> {
    Ok(Register::String(s))
}

/// The longest string a function may build, in bytes: SQLite's default
/// SQLITE_MAX_LENGTH. Lengths are checked BEFORE allocating, so a request
/// for ten gigabytes is an error rather than an allocation failure.
pub(crate) const MAX_LENGTH: usize = 1_000_000_000;

/// SQLite's error for a result longer than [`MAX_LENGTH`].
pub(crate) fn too_big() -> SqawkError {
    SqawkError::InvalidSqlQuery("string or blob too big".into())
}

/// Ok if a result of `len` bytes is within [`MAX_LENGTH`].
fn check_len(len: usize) -> SqawkResult<()> {
    if len > MAX_LENGTH {
        Err(too_big())
    } else {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Strings
// ---------------------------------------------------------------------------

/// UPPER / LOWER fold all of Unicode (SQLite without ICU folds only ASCII).
fn upper(a: &[Register]) -> SqawkResult<Register> {
    null_in_null_out!(a);
    text(text_of(&a[0]).to_uppercase())
}

fn lower(a: &[Register]) -> SqawkResult<Register> {
    null_in_null_out!(a);
    text(text_of(&a[0]).to_lowercase())
}

/// Characters in the text form of the value.
fn length(a: &[Register]) -> SqawkResult<Register> {
    null_in_null_out!(a);
    let n = match &a[0] {
        Register::String(s) => s.chars().count(),
        other => text_of(other).len(),
    };
    Ok(Register::Integer(n as i64))
}

/// With one argument TRIM removes leading and trailing whitespace (SQLite:
/// spaces only); with two, any of the characters of the second argument.
fn trim_impl(a: &[Register], lead: bool, trail: bool) -> SqawkResult<Register> {
    null_in_null_out!(a);
    let s = text_of(&a[0]);
    let out = match a.get(1) {
        None => match (lead, trail) {
            (true, true) => s.trim(),
            (true, false) => s.trim_start(),
            _ => s.trim_end(),
        },
        Some(set) => {
            let set: Vec<char> = text_of(set).chars().collect();
            let pred = |c: char| set.contains(&c);
            match (lead, trail) {
                (true, true) => s.trim_matches(pred),
                (true, false) => s.trim_start_matches(pred),
                _ => s.trim_end_matches(pred),
            }
        }
    };
    text(out.to_string())
}

fn trim(a: &[Register]) -> SqawkResult<Register> {
    trim_impl(a, true, true)
}

fn ltrim(a: &[Register]) -> SqawkResult<Register> {
    trim_impl(a, true, false)
}

fn rtrim(a: &[Register]) -> SqawkResult<Register> {
    trim_impl(a, false, true)
}

/// SUBSTR(s, start[, len]) with SQLite's rules: positions are 1-based; a
/// negative start counts from the end; start 0 is a position before the
/// first character (so `SUBSTR(s, 0, 2)` yields one character); a negative
/// length takes the characters BEFORE start.
fn substr(a: &[Register]) -> SqawkResult<Register> {
    null_in_null_out!(a);
    let s = text_of(&a[0]);
    let chars: Vec<char> = s.chars().collect();
    let n = chars.len() as i64;
    let mut p1 = int_arg(&a[1]);
    let mut p2 = match a.get(2) {
        Some(l) => int_arg(l),
        None => i64::MAX / 4,
    };
    let negative_len = p2 < 0;
    if negative_len {
        // -i64::MIN does not fit; i64::MAX already covers the whole string.
        p2 = p2.checked_neg().unwrap_or(i64::MAX);
    }
    if p1 < 0 {
        p1 += n;
        if p1 < 0 {
            p2 += p1;
            p1 = 0;
        }
    } else if p1 > 0 {
        p1 -= 1;
    } else if p2 > 0 {
        p2 -= 1;
    }
    if negative_len {
        p1 -= p2;
        if p1 < 0 {
            p2 += p1;
            p1 = 0;
        }
    }
    let start = p1.clamp(0, n);
    let end = p1.saturating_add(p2.max(0)).clamp(start, n);
    text(chars[start as usize..end as usize].iter().collect())
}

/// REPLACE(s, from, to). An empty `from` leaves `s` unchanged.
fn replace(a: &[Register]) -> SqawkResult<Register> {
    null_in_null_out!(a);
    let s = text_of(&a[0]);
    let from = text_of(&a[1]);
    if from.is_empty() {
        return text(s);
    }
    let to = text_of(&a[2]);
    if to.len() > from.len() {
        let grow = (to.len() - from.len()).saturating_mul(s.matches(&from).count());
        check_len(s.len().saturating_add(grow))?;
    }
    text(s.replace(&from, &to))
}

/// INSTR(haystack, needle): 1-based character position of the first
/// occurrence, 0 if absent.
fn instr(a: &[Register]) -> SqawkResult<Register> {
    null_in_null_out!(a);
    let hay = text_of(&a[0]);
    let needle = text_of(&a[1]);
    let pos = match hay.find(&needle) {
        Some(byte) => hay[..byte].chars().count() as i64 + 1,
        None => 0,
    };
    Ok(Register::Integer(pos))
}

/// CONCAT(...): NULL arguments count as empty strings (SQLite 3.44+).
fn concat(a: &[Register]) -> SqawkResult<Register> {
    let mut out = String::new();
    for r in a {
        if !matches!(r, Register::Null) {
            out.push_str(&text_of(r));
        }
    }
    check_len(out.len())?;
    text(out)
}

/// CONCAT_WS(sep, ...): NULL arguments are skipped; a NULL separator gives NULL.
fn concat_ws(a: &[Register]) -> SqawkResult<Register> {
    if matches!(a[0], Register::Null) {
        return Ok(Register::Null);
    }
    let sep = text_of(&a[0]);
    let parts: Vec<String> = a[1..]
        .iter()
        .filter(|r| !matches!(r, Register::Null))
        .map(text_of)
        .collect();
    let len = parts.iter().map(String::len).sum::<usize>()
        + sep.len().saturating_mul(parts.len().saturating_sub(1));
    check_len(len)?;
    text(parts.join(&sep))
}

/// `a || b`: NULL if either side is NULL (unlike CONCAT).
fn concat_op(a: &[Register]) -> SqawkResult<Register> {
    null_in_null_out!(a);
    let mut s = text_of(&a[0]);
    let t = text_of(&a[1]);
    check_len(s.len() + t.len())?;
    s.push_str(&t);
    text(s)
}

/// `a & b`, `a | b`, `a << b`, `a >> b`, `~a`: see [`arith::bitwise`].
fn bit_and(a: &[Register]) -> SqawkResult<Register> {
    Ok(arith::bitwise(arith::BitOp::And, &a[0], &a[1]))
}

fn bit_or(a: &[Register]) -> SqawkResult<Register> {
    Ok(arith::bitwise(arith::BitOp::Or, &a[0], &a[1]))
}

fn shift_left(a: &[Register]) -> SqawkResult<Register> {
    Ok(arith::bitwise(arith::BitOp::ShiftLeft, &a[0], &a[1]))
}

fn shift_right(a: &[Register]) -> SqawkResult<Register> {
    Ok(arith::bitwise(arith::BitOp::ShiftRight, &a[0], &a[1]))
}

fn bit_not(a: &[Register]) -> SqawkResult<Register> {
    Ok(match &a[0] {
        Register::Null => Register::Null,
        r => Register::Integer(!arith::int_value(r)),
    })
}

/// `a IS NOT DISTINCT FROM b`: 1 when both are NULL or they are equal (as
/// `=` decides), else 0. Never NULL.
fn is_same(a: &[Register]) -> SqawkResult<Register> {
    let same = match (&a[0], &a[1]) {
        (Register::Null, Register::Null) => true,
        (Register::Null, _) | (_, Register::Null) => false,
        (x, y) => VmEngine::sql_compare(x, y) == Some(std::cmp::Ordering::Equal),
    };
    Ok(Register::Integer(same as i64))
}

/// LEFT(s, n): the first n characters (none for n <= 0).
fn left(a: &[Register]) -> SqawkResult<Register> {
    null_in_null_out!(a);
    let n = int_arg(&a[1]).max(0) as usize;
    text(text_of(&a[0]).chars().take(n).collect())
}

/// RIGHT(s, n): the last n characters (none for n <= 0).
fn right(a: &[Register]) -> SqawkResult<Register> {
    null_in_null_out!(a);
    let s = text_of(&a[0]);
    let n = int_arg(&a[1]).max(0) as usize;
    let count = s.chars().count();
    text(s.chars().skip(count.saturating_sub(n)).collect())
}

/// LPAD / RPAD (not in SQLite; PostgreSQL semantics): pad `s` to `len`
/// characters with repetitions of `fill` (default a space). A string longer
/// than `len` is truncated to its first `len` characters; an empty `fill`
/// leaves a short string as it is; `len <= 0` gives ''.
fn pad(a: &[Register], on_left: bool) -> SqawkResult<Register> {
    null_in_null_out!(a);
    let s: Vec<char> = text_of(&a[0]).chars().collect();
    let len = int_arg(&a[1]).max(0) as usize;
    let fill: Vec<char> = match a.get(2) {
        Some(f) => text_of(f).chars().collect(),
        None => vec![' '],
    };
    if s.len() >= len || fill.is_empty() {
        return text(s.iter().take(len).collect());
    }
    // The result's size in bytes, before building it.
    let n = len - s.len();
    let fill_bytes: usize = fill.iter().map(|c| c.len_utf8()).sum();
    let rest: usize = fill[..n % fill.len()].iter().map(|c| c.len_utf8()).sum();
    let body_bytes: usize = s.iter().map(|c| c.len_utf8()).sum();
    check_len(
        (n / fill.len())
            .saturating_mul(fill_bytes)
            .saturating_add(rest)
            .saturating_add(body_bytes),
    )?;
    let padding: String = fill.iter().cycle().take(n).collect();
    let body: String = s.iter().collect();
    text(if on_left {
        padding + &body
    } else {
        body + &padding
    })
}

fn lpad(a: &[Register]) -> SqawkResult<Register> {
    pad(a, true)
}

fn rpad(a: &[Register]) -> SqawkResult<Register> {
    pad(a, false)
}

/// HEX(x): upper-case hex of the UTF-8 bytes of x's text form; '' for NULL.
fn hex(a: &[Register]) -> SqawkResult<Register> {
    if matches!(a[0], Register::Null) {
        return text(String::new());
    }
    let src = text_of(&a[0]);
    check_len(src.len().saturating_mul(2))?;
    let mut out = String::with_capacity(src.len() * 2);
    for b in src.bytes() {
        out.push_str(&format!("{:02X}", b));
    }
    text(out)
}

/// CHAR(n, ...): the characters with the given code points. NULL and
/// invalid code points are skipped.
fn char_fn(a: &[Register]) -> SqawkResult<Register> {
    let s: String = a
        .iter()
        .filter(|r| !matches!(r, Register::Null))
        .filter_map(|r| u32::try_from(int_arg(r)).ok().and_then(char::from_u32))
        .collect();
    text(s)
}

/// UNICODE(s): the code point of the first character; NULL for ''.
fn unicode(a: &[Register]) -> SqawkResult<Register> {
    null_in_null_out!(a);
    Ok(text_of(&a[0])
        .chars()
        .next()
        .map_or(Register::Null, |c| Register::Integer(c as i64)))
}

/// PRINTF(format, ...) / FORMAT: SQLite's printf (see `printf.rs`). A NULL
/// format gives NULL.
fn printf(a: &[Register]) -> SqawkResult<Register> {
    if matches!(a[0], Register::Null) {
        return Ok(Register::Null);
    }
    Ok(printf::format_str(&text_of(&a[0]), &a[1..])?.map_or(Register::Null, Register::String))
}

/// QUOTE(x): the SQL literal for x. Text is single-quoted with quotes
/// doubled, NULL is `NULL`, an integer its digits, and a real its `%!.15g`
/// form, or `%!.20e` when that does not read back as the same value (so
/// infinities are `Inf` / `-Inf`).
fn quote(a: &[Register]) -> SqawkResult<Register> {
    text(match &a[0] {
        Register::Null => "NULL".to_string(),
        Register::Integer(i) => i.to_string(),
        Register::Boolean(b) => (*b as i64).to_string(),
        Register::Float(f) => {
            let s = printf::real_str("%!.15g", *f);
            if f.is_finite() && arith::atof(&s) == *f {
                s
            } else {
                printf::real_str("%!.20e", *f)
            }
        }
        Register::String(s) => {
            check_len(s.len().saturating_mul(2).saturating_add(2))?;
            format!("'{}'", s.replace('\'', "''"))
        }
    })
}

// ---------------------------------------------------------------------------
// Numbers
// ---------------------------------------------------------------------------

/// ABS: an integer stays an integer (ABS(i64::MIN) is an overflow error);
/// text is read as a number and gives a real, as in SQLite.
fn abs(a: &[Register]) -> SqawkResult<Register> {
    Ok(match &a[0] {
        Register::Null => Register::Null,
        Register::Integer(i) => Register::Integer(arith::checked(i.checked_abs())?),
        Register::Boolean(b) => Register::Integer(*b as i64),
        Register::Float(f) => Register::Float(f.abs()),
        Register::String(s) => Register::Float(text_to_num(s).as_f64().abs()),
    })
}

/// ROUND(x[, d]): always a real, SQLite's `roundFunc`. Halves are judged
/// on the value actually stored (2.675 is 2.67499..., so it rounds to
/// 2.67 as in SQLite). A negative `d` is 0 and one above 30 is 30, as in
/// SQLite: ROUND(1234.5, -2) is 1235.0.
fn round(a: &[Register]) -> SqawkResult<Register> {
    if matches!(a[0], Register::Null) {
        return Ok(Register::Null);
    }
    let digits = match a.get(1) {
        None => 0,
        Some(Register::Null) => return Ok(Register::Null),
        Some(Register::String(s)) => match crate::table::numeric_text(s) {
            Some(Value::Integer(i)) => i,
            Some(Value::Float(f)) => f as i64,
            _ => {
                return Err(SqawkError::InvalidSqlQuery(format!(
                    "ROUND precision must be a number, got '{}'",
                    s
                )))
            }
        },
        Some(r) => int_arg(r),
    };
    let x = to_num(&a[0]).map_or(0.0, Num::as_f64);
    Ok(Register::Float(round_decimal(x, digits)))
}

/// Round `x` to `digits` decimal places as SQLite's `roundFunc` does: a
/// magnitude above 2^52 has no fraction and is returned as is; to 0
/// places, `x + 0.5` (`- 0.5` when negative) truncated toward zero; else
/// `%!.*f` of x (digits from SQLite's decimal expansion, rounded half up)
/// read back as a real.
pub(crate) fn round_decimal(x: f64, digits: i64) -> f64 {
    const TWO_52: f64 = 4_503_599_627_370_496.0;
    let n = digits.clamp(0, 30);
    if !(-TWO_52..=TWO_52).contains(&x) {
        // Also NaN, which SQLite passes through the same way.
        x
    } else if n == 0 {
        (x + if x < 0.0 { -0.5 } else { 0.5 }) as i64 as f64
    } else {
        arith::atof(&printf::real_str(&format!("%!.{n}f"), x))
    }
}

/// CEIL / FLOOR: an integer is returned as is; a real stays a real.
fn ceil_floor(a: &[Register], up: bool) -> SqawkResult<Register> {
    Ok(match to_num(&a[0]) {
        None => Register::Null,
        Some(Num::Int(i)) => Register::Integer(i),
        Some(Num::Real(f)) => Register::Float(if up { f.ceil() } else { f.floor() }),
    })
}

fn ceil(a: &[Register]) -> SqawkResult<Register> {
    ceil_floor(a, true)
}

fn floor(a: &[Register]) -> SqawkResult<Register> {
    ceil_floor(a, false)
}

/// SIGN(x): -1, 0 or 1; NULL for NULL and for text that is not a number.
fn sign(a: &[Register]) -> SqawkResult<Register> {
    let n = match &a[0] {
        Register::String(s) => match crate::table::numeric_text(s) {
            Some(Value::Integer(i)) => i as f64,
            Some(Value::Float(f)) => f,
            _ => return Ok(Register::Null),
        },
        other => match to_num(other) {
            Some(n) => n.as_f64(),
            None => return Ok(Register::Null),
        },
    };
    Ok(Register::Integer(if n > 0.0 {
        1
    } else if n < 0.0 {
        -1
    } else {
        0
    }))
}

/// Multi-argument MIN / MAX: NULL if any argument is NULL, otherwise the
/// least / greatest argument in sort order.
fn extreme(a: &[Register], want: std::cmp::Ordering) -> SqawkResult<Register> {
    null_in_null_out!(a);
    let mut best = &a[0];
    for r in &a[1..] {
        let (x, y) = (Value::from(r.clone()), Value::from(best.clone()));
        if compare_values(&x, &y) == want {
            best = r;
        }
    }
    Ok(best.clone())
}

fn min_scalar(a: &[Register]) -> SqawkResult<Register> {
    extreme(a, std::cmp::Ordering::Less)
}

fn max_scalar(a: &[Register]) -> SqawkResult<Register> {
    extreme(a, std::cmp::Ordering::Greater)
}

/// TYPEOF(x): 'integer', 'real', 'text' or 'null'.
fn type_of(a: &[Register]) -> SqawkResult<Register> {
    text(
        match &a[0] {
            Register::Integer(_) | Register::Boolean(_) => "integer",
            Register::Float(_) => "real",
            Register::String(_) => "text",
            Register::Null => "null",
        }
        .to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> Register {
        Register::String(v.into())
    }
    fn i(v: i64) -> Register {
        Register::Integer(v)
    }
    fn txt(r: SqawkResult<Register>) -> String {
        match r.unwrap() {
            Register::String(s) => s,
            Register::Null => "NULL".into(),
            other => format!("{:?}", other),
        }
    }

    #[test]
    fn substr_sqlite_rules() {
        let cases: &[(i64, Option<i64>, &str)] = &[
            (-3, None, "llo"),
            (0, Some(2), "h"),
            (3, Some(-2), "he"),
            (10, None, ""),
            (-10, Some(3), ""),
            (0, None, "hello"),
            (2, Some(0), ""),
            (-3, Some(2), "ll"),
            (0, Some(-1), ""),
            (1, Some(-1), ""),
            (6, Some(-2), "lo"),
        ];
        for &(start, len, want) in cases {
            let mut args = vec![s("hello"), i(start)];
            if let Some(l) = len {
                args.push(i(l));
            }
            assert_eq!(txt(substr(&args)), want, "substr(hello, {start}, {len:?})");
        }
        assert_eq!(txt(substr(&[s("日本語"), i(2)])), "本語");
    }

    #[test]
    fn round_like_sqlite() {
        assert_eq!(round_decimal(2.675, 2), 2.67);
        assert_eq!(round_decimal(0.125, 2), 0.13);
        assert_eq!(round_decimal(1.005, 2), 1.0);
        assert_eq!(round_decimal(0.285, 2), 0.28);
        assert_eq!(round_decimal(2.5, 0), 3.0);
        assert_eq!(round_decimal(-2.5, 0), -3.0);
        assert_eq!(round_decimal(-2.25, 1), -2.3);
        assert_eq!(round_decimal(9.995, 2), 9.99);
        assert_eq!(round_decimal(99.5, 0), 100.0);
        assert_eq!(round_decimal(0.0004, 2), 0.0);
        assert_eq!(round_decimal(1234.5, -2), 1235.0);
        assert_eq!(round_decimal(1058.05, 1), 1058.0);
        assert_eq!(round_decimal(-1058.05, 1), -1058.0);
        assert_eq!(round_decimal(0.49999999999999994, 0), 1.0);
        assert_eq!(round_decimal(1e300, 2), 1e300);
        assert_eq!(round_decimal(f64::INFINITY, 2), f64::INFINITY);
    }

    #[test]
    fn printf_basics() {
        let p = |args: &[Register]| txt(printf(args));
        assert_eq!(p(&[s("%5.2f"), Register::Float(3.14259)]), " 3.14");
        assert_eq!(
            p(&[s("%5d|%-5d|%05d"), i(42), i(42), i(42)]),
            "   42|42   |00042"
        );
        assert_eq!(p(&[s("%d"), s("12abc")]), "12");
        assert_eq!(p(&[s("%s"), Register::Null]), "");
        assert_eq!(p(&[s("%x"), i(255)]), "ff");
        assert_eq!(p(&[s("%%")]), "%");
        assert_eq!(p(&[s("%c"), s("hello")]), "h");
        assert_eq!(p(&[s("%05.1f"), Register::Float(-2.25)]), "-02.3");
    }
}
