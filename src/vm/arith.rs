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
    Num::Real(atof(text))
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

// The error of each power of ten as a double (the exact power minus the
// double), the low halves of SQLite's double-double constants, spelled as
// SQLite spells them.
#[allow(clippy::excessive_precision)]
const E100_LO: f64 = -1.5902891109759918046e+83;
#[allow(clippy::excessive_precision)]
const EM100_LO: f64 = -1.99918998026028836196e-117;
#[allow(clippy::excessive_precision)]
const EM10_LO: f64 = -3.6432197315497741579e-27;
#[allow(clippy::excessive_precision)]
const EM1_LO: f64 = -5.5511151231257827021e-18;

/// Double-double multiplication `x *= (y + yy)` (T. J. Dekker, "A
/// Floating-Point Technique for Extending the Available Precision", 1971),
/// SQLite's `dekkerMul2`. Every operation rounds to binary64 (Rust never
/// fuses or widens f64 arithmetic), as the algorithm requires.
fn dekker_mul2(x: &mut [f64; 2], y: f64, yy: f64) {
    const HIGH: u64 = 0xffff_ffff_fc00_0000;
    let hx = f64::from_bits(x[0].to_bits() & HIGH);
    let tx = x[0] - hx;
    let hy = f64::from_bits(y.to_bits() & HIGH);
    let ty = y - hy;
    let p = hx * hy;
    let q = hx * ty + tx * hy;
    let c = p + q;
    let cc = p - c + q + tx * ty;
    let cc = x[0] * yy + x[1] * y + cc;
    x[0] = c + cc;
    x[1] = c - x[0];
    x[1] += cc;
}

/// The numeric prefix of `s` (leading whitespace, sign, digits, point,
/// exponent) as a real, computed as SQLite's `sqlite3AtoF` (3.50; the
/// double-double path of 3.45 builds without long double) computes it:
/// up to 19 significant digits as an integer, then scaled by powers of
/// ten in double-double arithmetic. The result is the same on every
/// platform and is what SQLite reads for the same text, which is not
/// always the correctly rounded double (`1e-300`'s neighbour can come
/// back for 17-digit input near the ends of the range). Text without a
/// number reads as 0.0.
// The bounds are spelled as SQLite spells them.
#[allow(clippy::excessive_precision)]
pub(crate) fn atof(s: &str) -> f64 {
    const LIMIT: u64 = (u64::MAX - 9) / 10;
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() && is_space(b[i]) {
        i += 1;
    }
    let mut neg = false;
    if i < b.len() && (b[i] == b'-' || b[i] == b'+') {
        neg = b[i] == b'-';
        i += 1;
    }
    let mut sig: u64 = 0;
    let mut d: i64 = 0;
    while i < b.len() && b[i].is_ascii_digit() {
        sig = sig * 10 + u64::from(b[i] - b'0');
        i += 1;
        if sig >= LIMIT {
            // Further integer digits only shift the point.
            while i < b.len() && b[i].is_ascii_digit() {
                i += 1;
                d += 1;
            }
        }
    }
    if i < b.len() && b[i] == b'.' {
        i += 1;
        while i < b.len() && b[i].is_ascii_digit() {
            if sig < LIMIT {
                sig = sig * 10 + u64::from(b[i] - b'0');
                d -= 1;
            }
            i += 1;
        }
    }
    let mut e: i64 = 0;
    if i < b.len() && (b[i] == b'e' || b[i] == b'E') {
        i += 1;
        let mut eneg = false;
        if i < b.len() && (b[i] == b'-' || b[i] == b'+') {
            eneg = b[i] == b'-';
            i += 1;
        }
        while i < b.len() && b[i].is_ascii_digit() {
            e = if e < 10000 {
                e * 10 + i64::from(b[i] - b'0')
            } else {
                10000
            };
            i += 1;
        }
        if eneg {
            e = -e;
        }
    }
    let sign = |f: f64| if neg { -f } else { f };
    if sig == 0 {
        return sign(0.0);
    }
    e += d;
    while e > 0 && sig < (u64::MAX - 0x7ff) / 10 {
        sig *= 10;
        e -= 1;
    }
    while e < 0 && sig.is_multiple_of(10) {
        sig /= 10;
        e += 1;
    }
    let mut rr = [sig as f64, 0.0];
    // The largest double that converts to u64 safely.
    if rr[0] <= 18_446_744_073_709_549_568.0 {
        let s2 = rr[0] as u64;
        rr[1] = if sig >= s2 {
            (sig - s2) as f64
        } else {
            -((s2 - sig) as f64)
        };
    }
    if e > 0 {
        while e >= 100 {
            e -= 100;
            dekker_mul2(&mut rr, 1.0e+100, E100_LO);
        }
        while e >= 10 {
            e -= 10;
            dekker_mul2(&mut rr, 1.0e+10, 0.0);
        }
        while e >= 1 {
            e -= 1;
            dekker_mul2(&mut rr, 1.0e+01, 0.0);
        }
    } else {
        while e <= -100 {
            e += 100;
            dekker_mul2(&mut rr, 1.0e-100, EM100_LO);
        }
        while e <= -10 {
            e += 10;
            dekker_mul2(&mut rr, 1.0e-10, EM10_LO);
        }
        while e <= -1 {
            e += 1;
            dekker_mul2(&mut rr, 1.0e-01, EM1_LO);
        }
    }
    let mut r = rr[0] + rr[1];
    if r.is_nan() {
        r = f64::INFINITY;
    }
    sign(r)
}

/// SQLite's `sqlite3Isspace`: ASCII space, \t \n \v \f \r.
fn is_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

/// A real decoded to decimal digits: SQLite's `FpDecode`.
pub(crate) struct FpDecode {
    /// `b'+'` or `b'-'`.
    pub(crate) sign: u8,
    /// 0: finite; 1: infinity; 2: NaN.
    pub(crate) special: u8,
    buf: [u8; 24],
    start: usize,
    /// Number of significant digits (trailing zeros removed).
    n: usize,
    /// Position of the decimal point relative to the first digit.
    pub(crate) idp: i64,
}

impl FpDecode {
    /// The significant digits, ASCII, without trailing zeros.
    pub(crate) fn digits(&self) -> &[u8] {
        &self.buf[self.start..self.start + self.n]
    }
}

/// SQLite's `sqlite3FpDecode` (3.50, which always uses the portable
/// double-double path that 3.45 builds without long double use): the
/// magnitude is scaled by powers of ten in double-double arithmetic until
/// it lies near 1e18, read as an integer of 18 or 19 digits, and those
/// digits are rounded half up to `iround` significant digits (when
/// `iround` is negative, to `-iround` digits after the point; 0 means no
/// rounding) and to at most `mxround` digits. The result is the same on
/// every platform.
// The bounds are spelled as SQLite spells them.
#[allow(clippy::excessive_precision)]
pub(crate) fn fp_decode(r: f64, iround: i64, mxround: i64) -> FpDecode {
    let mut p = FpDecode {
        sign: b'+',
        special: 0,
        buf: [b'0'; 24],
        start: 0,
        n: 1,
        idp: 1,
    };
    let mut r = r;
    if r < 0.0 {
        p.sign = b'-';
        r = -r;
    } else if r == 0.0 {
        return p;
    }
    if !r.is_finite() {
        p.special = if r.is_nan() { 2 } else { 1 };
        p.n = 0;
        p.idp = 0;
        return p;
    }
    // Scale into [9.2e17, 9.2e18]; the low-order constants are the error
    // of each power of ten as a double, as SQLite has them.
    let mut exp: i64 = 0;
    let mut rr = [r, 0.0];
    if rr[0] > 9.223_372_036_854_774_784e18 {
        while rr[0] > 9.223_372_036_854_774_784e118 {
            exp += 100;
            dekker_mul2(&mut rr, 1.0e-100, EM100_LO);
        }
        while rr[0] > 9.223_372_036_854_774_784e28 {
            exp += 10;
            dekker_mul2(&mut rr, 1.0e-10, EM10_LO);
        }
        while rr[0] > 9.223_372_036_854_774_784e18 {
            exp += 1;
            dekker_mul2(&mut rr, 1.0e-01, EM1_LO);
        }
    } else {
        while rr[0] < 9.223_372_036_854_774_784e-83 {
            exp -= 100;
            dekker_mul2(&mut rr, 1.0e+100, E100_LO);
        }
        while rr[0] < 9.223_372_036_854_774_784e7 {
            exp -= 10;
            dekker_mul2(&mut rr, 1.0e+10, 0.0);
        }
        while rr[0] < 9.223_372_036_854_774_78e17 {
            exp -= 1;
            dekker_mul2(&mut rr, 1.0e+01, 0.0);
        }
    }
    let mut v: u64 = if rr[1] < 0.0 {
        (rr[0] as u64).wrapping_sub((-rr[1]) as u64)
    } else {
        (rr[0] as u64).wrapping_add(rr[1] as u64)
    };

    // The digits of v, right-aligned in buf; buf[i] is the free slot
    // before the first one.
    let mut i = p.buf.len() - 1;
    while v != 0 {
        p.buf[i] = b'0' + (v % 10) as u8;
        i -= 1;
        v /= 10;
    }
    let mut n = (p.buf.len() - 1 - i) as i64;
    p.idp = n + exp;
    let mut iround = iround;
    if iround < 0 {
        iround = p.idp - iround;
        if iround == 0 && p.buf[i + 1] >= b'5' {
            iround = 1;
            p.buf[i] = b'0';
            i -= 1;
            n += 1;
            p.idp += 1;
        }
    }
    if iround > 0 && (iround < n || n > mxround) {
        let z = i + 1;
        if iround > mxround {
            iround = mxround;
        }
        n = iround;
        if p.buf[z + iround as usize] >= b'5' {
            let mut j = z + iround as usize - 1;
            loop {
                p.buf[j] += 1;
                if p.buf[j] <= b'9' {
                    break;
                }
                p.buf[j] = b'0';
                if j == z {
                    p.buf[i] = b'1';
                    i -= 1;
                    n += 1;
                    p.idp += 1;
                    break;
                }
                j -= 1;
            }
        }
    }
    p.start = i + 1;
    let mut n = n as usize;
    while n > 1 && p.buf[p.start + n - 1] == b'0' {
        n -= 1;
    }
    p.n = n;
    p
}

/// A REAL's text, exactly as SQLite's `CAST(x AS TEXT)` renders it
/// (`%!.15g`): 15 significant digits rounded half up from the
/// [`fp_decode`] expansion, an integral value keeps a `.0` (`2.0`,
/// `1.0e+20`), infinities are `Inf` / `-Inf`, and negative zero is `0.0`.
/// NaN, which SQLite turns into NULL, renders as the empty string.
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
    let d = fp_decode(f, 15, 26);
    let z = d.digits();
    let x = d.idp - 1;
    let mut out = String::with_capacity(24);
    if d.sign == b'-' {
        out.push('-');
    }
    let push = |out: &mut String, b: &[u8]| out.extend(b.iter().map(|&c| c as char));
    // At most 15 digits, all of which fit in the 14 places `%.15g` shows
    // after the first, so no digit is cut here.
    if !(-4..=14).contains(&x) {
        push(&mut out, &z[..1]);
        out.push('.');
        push(&mut out, if z.len() > 1 { &z[1..] } else { b"0" });
        out.push('e');
        out.push(if x < 0 { '-' } else { '+' });
        out.push_str(&format!("{:02}", x.unsigned_abs()));
    } else if x < 0 {
        out.push_str("0.");
        out.extend(std::iter::repeat_n('0', (-x - 1) as usize));
        push(&mut out, z);
    } else {
        let int_len = x as usize + 1;
        for k in 0..int_len {
            out.push(z.get(k).map_or('0', |&c| c as char));
        }
        out.push('.');
        push(
            &mut out,
            if z.len() > int_len {
                &z[int_len..]
            } else {
                b"0"
            },
        );
    }
    out
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

    /// Values checked against SQLite 3.50.2 and 3.45.0 built without long
    /// double (tsq's oracle).
    #[test]
    fn double_double_conversions() {
        assert_eq!(real_text(443.85 / 5867.0), "0.0756519515936595");
        assert_eq!(real_text(100.0 / 476.42), "0.209898828764536");
        assert_eq!(real_text(1e20), "1.0e+20");
        assert_eq!(real_text(-1e-5), "-1.0e-05");
        assert_eq!(real_text(5e-324), "4.94065645841247e-324");
        assert_eq!(real_text(f64::MAX), "1.79769313486232e+308");
        assert_eq!(real_text(123456789012345.0), "123456789012345.0");
        assert_eq!(real_text(0.0001), "0.0001");
        assert_eq!(atof("1e-300").to_bits(), 0x01a5_6e1f_c2f8_f358);
        assert_eq!(atof("1.0e-300"), atof("1e-300"));
        assert_eq!(atof(" -2.5e3x"), -2500.0);
        assert_eq!(atof("0.1"), 0.1);
        assert_eq!(atof("1e400"), f64::INFINITY);
        assert_eq!(atof("1e-400"), 0.0);
        assert_eq!(atof("."), 0.0);
        let d = fp_decode(17.0 / 1.5, 21, 26);
        assert_eq!(d.digits(), b"1133333333333333392");
        assert_eq!(d.idp, 2);
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
