//! SQLite's printf: the formatter behind PRINTF / FORMAT, QUOTE and the
//! numeric fields of the date functions. A port of `sqlite3_str_vappendf`
//! (printf.c, SQLite 3.45) as SQLite's SQL function drives it, conversion
//! for conversion, quirks included:
//!
//! - conversions `d i u f e E g G x X o c s z q Q w p r n %`; flags
//!   `- + space # ! 0 ,`; width and precision, either one `*` (taken from
//!   the arguments; a negative width left-justifies); the C length
//!   modifiers `l` / `ll` are accepted and ignored;
//! - an unknown conversion, or a format ending inside a conversion, ends
//!   the output there (what was formatted so far is the result, NULL when
//!   that is nothing at all; an empty format is NULL too);
//! - a width or precision past the length limit is the error "string or
//!   blob too big" (SQLite wraps it to 31 bits or returns NULL);
//! - reals are decoded by [`fp_decode`] (SQLite's double-double
//!   `sqlite3FpDecode`, identical on every platform) to at most 16
//!   significant digits (26 with `!`), rounded half up -- so `%.20f` of 0.1 is
//!   `0.10000000000000000000`, and `%.0f` truncates (`%.0f` of 2.5 is `2`);
//! - `%s` widths and precisions count bytes unless the `!` flag is given;
//! - a missing argument is NULL; NULL is 0 for numeric conversions, `''`
//!   for `%s`, `(NULL)` for `%q` / `%w` and `NULL` for `%Q`.

use super::arith::{fp_decode, real_to_int, text_of, text_to_int, text_to_num};
use super::bytecode::Register;
use super::func::{too_big, MAX_LENGTH};
use crate::error::SqawkResult;

/// SQLite's SQLITE_FP_PRECISION_LIMIT: the largest precision a real
/// conversion honours.
const FP_PRECISION_LIMIT: i64 = 100_000_000;

/// The arguments of one call, consumed in order (SQLite's
/// `PrintfArguments`).
struct Args<'a> {
    args: &'a [Register],
    used: usize,
}

impl Args<'_> {
    fn next(&mut self) -> Option<&Register> {
        let r = self.args.get(self.used);
        if r.is_some() {
            self.used += 1;
        }
        r
    }

    /// `sqlite3_value_int64`: text by its integer prefix, reals truncated.
    fn int(&mut self) -> i64 {
        match self.next() {
            None | Some(Register::Null) => 0,
            Some(Register::Integer(i)) => *i,
            Some(Register::Boolean(b)) => *b as i64,
            Some(Register::Float(f)) => real_to_int(*f),
            Some(Register::String(s)) => text_to_int(s),
        }
    }

    /// `sqlite3_value_double`: text by its numeric prefix.
    fn double(&mut self) -> f64 {
        match self.next() {
            None | Some(Register::Null) => 0.0,
            Some(Register::Integer(i)) => *i as f64,
            Some(Register::Boolean(b)) => *b as i64 as f64,
            Some(Register::Float(f)) => *f,
            Some(Register::String(s)) => text_to_num(s).as_f64(),
        }
    }

    /// `sqlite3_value_text`: `None` for NULL or a missing argument.
    fn text(&mut self) -> Option<Vec<u8>> {
        match self.next() {
            None | Some(Register::Null) => None,
            Some(r) => Some(text_of(r).into_bytes()),
        }
    }
}

/// The output, never longer than [`MAX_LENGTH`]. `touched` records whether
/// anything was appended, even nothing: SQLite's result is NULL when no
/// append ever happened (an empty format, or one that stops at its first
/// conversion), but `''` after an empty one (`printf('%s', '')`).
struct Out {
    buf: Vec<u8>,
    touched: bool,
}

impl Out {
    fn push(&mut self, b: &[u8]) -> SqawkResult<()> {
        if self.buf.len().saturating_add(b.len()) > MAX_LENGTH {
            return Err(too_big());
        }
        self.buf.extend_from_slice(b);
        self.touched = true;
        Ok(())
    }

    fn pad(&mut self, n: i64, ch: u8) -> SqawkResult<()> {
        if n <= 0 {
            return Ok(());
        }
        let n = n as usize;
        if self.buf.len().saturating_add(n) > MAX_LENGTH {
            return Err(too_big());
        }
        self.buf.resize(self.buf.len() + n, ch);
        self.touched = true;
        Ok(())
    }

    fn finish(self) -> Option<Vec<u8>> {
        self.touched.then_some(self.buf)
    }
}

/// A width or precision written in the format, or taken from an argument,
/// past the length limit is an error (SQLite wraps it to 31 bits, or gives
/// NULL once the result is too long).
fn check_size(n: u64) -> SqawkResult<()> {
    if n > MAX_LENGTH as u64 {
        Err(too_big())
    } else {
        Ok(())
    }
}

/// Ok if a work buffer of `n` bytes is within the length limit (SQLite's
/// `printfTempBuf` fails with SQLITE_TOOBIG past it).
fn temp_buf(n: i64) -> SqawkResult<()> {
    if n > MAX_LENGTH as i64 {
        Err(too_big())
    } else {
        Ok(())
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Decimal,
    Radix,
    Ordinal,
    Pointer,
    Float,
    Exp,
    Generic,
    Size,
    Str,
    SqlEscape,
    SqlEscape2,
    SqlEscape3,
    CharX,
    Percent,
    /// `%T`, `%S` (internal to SQLite) and anything unknown: stop.
    Stop,
}

/// One conversion's table entry: kind, base, signed, upper-case digits,
/// and the `#` prefix.
struct Info {
    kind: Kind,
    base: u64,
    signed: bool,
    upper: bool,
    alt_prefix: &'static [u8],
}

fn info(c: u8) -> Info {
    let i = |kind, base, signed, upper, alt_prefix| Info {
        kind,
        base,
        signed,
        upper,
        alt_prefix,
    };
    match c {
        b'd' | b'i' => i(Kind::Decimal, 10, true, false, b""),
        b'u' => i(Kind::Decimal, 10, false, false, b""),
        b's' | b'z' => i(Kind::Str, 0, false, false, b""),
        b'g' => i(Kind::Generic, 0, true, false, b""),
        b'G' => i(Kind::Generic, 0, true, true, b""),
        b'q' => i(Kind::SqlEscape, 0, false, false, b""),
        b'Q' => i(Kind::SqlEscape2, 0, false, false, b""),
        b'w' => i(Kind::SqlEscape3, 0, false, false, b""),
        b'c' => i(Kind::CharX, 0, false, false, b""),
        b'o' => i(Kind::Radix, 8, false, false, b"0"),
        b'x' => i(Kind::Radix, 16, false, false, b"0x"),
        b'X' => i(Kind::Radix, 16, false, true, b"0X"),
        b'f' => i(Kind::Float, 0, true, false, b""),
        b'e' => i(Kind::Exp, 0, true, false, b""),
        b'E' => i(Kind::Exp, 0, true, true, b""),
        b'n' => i(Kind::Size, 0, false, false, b""),
        b'%' => i(Kind::Percent, 0, false, false, b""),
        b'p' => i(Kind::Pointer, 16, false, true, b"0x"),
        b'r' => i(Kind::Ordinal, 10, true, false, b""),
        _ => i(Kind::Stop, 0, false, false, b""),
    }
}

/// `printf(fmt, args...)` as SQLite's SQL function formats it, as bytes
/// (a `%s` precision counts bytes, so it may cut a character in two);
/// `None` is SQL NULL.
pub(crate) fn format(fmt: &[u8], args: &[Register]) -> SqawkResult<Option<Vec<u8>>> {
    let mut args = Args { args, used: 0 };
    let mut out = Out {
        buf: Vec::new(),
        touched: false,
    };
    let at = |k: usize| fmt.get(k).copied().unwrap_or(0);
    let mut i = 0usize;
    while i < fmt.len() {
        if fmt[i] != b'%' {
            let start = i;
            while i < fmt.len() && fmt[i] != b'%' {
                i += 1;
            }
            out.push(&fmt[start..i])?;
            if i >= fmt.len() {
                break;
            }
        }
        // fmt[i] is '%'.
        i += 1;
        let mut c = at(i);
        if c == 0 {
            out.push(b"%")?;
            break;
        }
        let mut left = false;
        let mut prefix: u8 = 0;
        let mut thousand = false;
        let mut alt = false;
        let mut alt2 = false;
        let mut zeropad = false;
        let mut width: i64 = 0;
        let mut precision: i64 = -1;
        loop {
            let mut done = false;
            match c {
                b'-' => left = true,
                b'+' => prefix = b'+',
                b' ' => prefix = b' ',
                b'#' => alt = true,
                b'!' => alt2 = true,
                b'0' => zeropad = true,
                b',' => thousand = true,
                b'l' => {
                    i += 1;
                    c = at(i);
                    if c == b'l' {
                        i += 1;
                        c = at(i);
                    }
                    done = true;
                }
                b'1'..=b'9' => {
                    let mut wx = u64::from(c - b'0');
                    loop {
                        i += 1;
                        c = at(i);
                        if !c.is_ascii_digit() {
                            break;
                        }
                        wx = wx.saturating_mul(10).saturating_add(u64::from(c - b'0'));
                    }
                    check_size(wx)?;
                    width = wx as i64;
                    if c != b'.' && c != b'l' {
                        done = true;
                    } else {
                        i -= 1;
                    }
                }
                b'*' => {
                    let w = args.int();
                    check_size(w.unsigned_abs())?;
                    width = w;
                    if w < 0 {
                        left = true;
                        width = -w;
                    }
                    let c1 = at(i + 1);
                    if c1 != b'.' && c1 != b'l' {
                        i += 1;
                        c = at(i);
                        done = true;
                    }
                }
                b'.' => {
                    i += 1;
                    c = at(i);
                    if c == b'*' {
                        // A negative precision counts as its magnitude.
                        let p = args.int();
                        check_size(p.unsigned_abs())?;
                        precision = p.abs();
                        i += 1;
                        c = at(i);
                    } else {
                        let mut px: u64 = 0;
                        while c.is_ascii_digit() {
                            px = px.saturating_mul(10).saturating_add(u64::from(c - b'0'));
                            i += 1;
                            c = at(i);
                        }
                        check_size(px)?;
                        precision = px as i64;
                    }
                    if c == b'l' {
                        i -= 1;
                    } else {
                        done = true;
                    }
                }
                _ => done = true,
            }
            if done {
                break;
            }
            i += 1;
            c = at(i);
            if c == 0 {
                break;
            }
        }
        // fmt[i] is the conversion character (or the end of the format).
        i += 1;
        let inf = info(c);
        let mut body: Vec<u8> = Vec::new();
        match inf.kind {
            Kind::Stop => return Ok(out.finish()),
            Kind::Size => width = 0,
            Kind::Percent => body.push(b'%'),
            Kind::Decimal | Kind::Radix | Kind::Ordinal | Kind::Pointer => {
                if inf.kind != Kind::Decimal {
                    thousand = false;
                }
                let (longvalue, sign) = if inf.signed {
                    let v = args.int();
                    if v < 0 {
                        (v.unsigned_abs(), b'-')
                    } else {
                        (v as u64, prefix)
                    }
                } else {
                    (args.int() as u64, 0)
                };
                body = integer(
                    longvalue, sign, &inf, precision, width, zeropad, alt, thousand,
                )?;
            }
            Kind::Float | Kind::Exp | Kind::Generic => {
                let v = args.double();
                body = real(
                    v, inf.kind, inf.upper, precision, width, prefix, left, zeropad, alt, alt2,
                    thousand,
                )?;
            }
            Kind::CharX => {
                let mut ch: Vec<u8> = vec![0];
                if let Some(t) = args.text() {
                    if let Some(&first) = t.first() {
                        ch[0] = first;
                        if first & 0xc0 == 0xc0 {
                            ch.extend(t[1..].iter().take(3).take_while(|&&b| b & 0xc0 == 0x80));
                        }
                    }
                }
                if precision > 1 {
                    width -= precision - 1;
                    if width > 1 && !left {
                        out.pad(width - 1, b' ')?;
                        width = 0;
                    }
                    let reps = (precision - 1) as usize;
                    if (ch.len() as u64).saturating_mul(reps as u64) > MAX_LENGTH as u64 {
                        return Err(too_big());
                    }
                    out.push(&ch.repeat(reps))?;
                }
                width += utf8_extra(&ch, true, width);
                body = ch;
            }
            Kind::Str => {
                let t = args.text().unwrap_or_default();
                // The text ends at a NUL, as a C string does.
                let t = match t.iter().position(|&b| b == 0) {
                    Some(n) => t[..n].to_vec(),
                    None => t,
                };
                let len = if precision >= 0 {
                    if alt2 {
                        char_prefix_len(&t, precision as usize)
                    } else {
                        t.len().min(precision as usize)
                    }
                } else {
                    t.len()
                };
                body = t[..len].to_vec();
                width += utf8_extra(&body, alt2, width);
            }
            Kind::SqlEscape | Kind::SqlEscape2 | Kind::SqlEscape3 => {
                let q = if inf.kind == Kind::SqlEscape3 {
                    b'"'
                } else {
                    b'\''
                };
                let arg = args.text();
                let isnull = arg.is_none();
                let esc: Vec<u8> = match arg {
                    Some(t) => match t.iter().position(|&b| b == 0) {
                        Some(n) => t[..n].to_vec(),
                        None => t,
                    },
                    None if inf.kind == Kind::SqlEscape2 => b"NULL".to_vec(),
                    None => b"(NULL)".to_vec(),
                };
                // The precision counts input bytes (characters with `!`).
                let mut k = precision;
                let mut n = 0usize;
                while k != 0 && n < esc.len() {
                    if alt2 && esc[n] & 0xc0 == 0xc0 {
                        while n + 1 < esc.len() && esc[n + 1] & 0xc0 == 0x80 {
                            n += 1;
                        }
                    }
                    n += 1;
                    k -= 1;
                }
                let need_quote = !isnull && inf.kind == Kind::SqlEscape2;
                if need_quote {
                    body.push(q);
                }
                for &ch in &esc[..n] {
                    body.push(ch);
                    if ch == q {
                        body.push(ch);
                    }
                }
                if need_quote {
                    body.push(q);
                }
                width += utf8_extra(&body, alt2, width);
            }
        }
        let length = body.len() as i64;
        width -= length;
        if width > 0 {
            if !left {
                out.pad(width, b' ')?;
            }
            out.push(&body)?;
            if left {
                out.pad(width, b' ')?;
            }
        } else {
            out.push(&body)?;
        }
    }
    Ok(out.finish())
}

/// With `!` and a positive width, the width counts characters: one more
/// for every UTF-8 continuation byte of the text.
fn utf8_extra(text: &[u8], alt2: bool, width: i64) -> i64 {
    if alt2 && width > 0 {
        text.iter().filter(|&&b| b & 0xc0 == 0x80).count() as i64
    } else {
        0
    }
}

/// Bytes of the first `chars` characters of `t`.
fn char_prefix_len(t: &[u8], chars: usize) -> usize {
    let mut z = 0usize;
    let mut left = chars;
    while left > 0 && z < t.len() {
        let b = t[z];
        z += 1;
        if b >= 0xc0 {
            while z < t.len() && t[z] & 0xc0 == 0x80 {
                z += 1;
            }
        }
        left -= 1;
    }
    z
}

/// The integer conversions (`d i u x X o p r`).
#[allow(clippy::too_many_arguments)]
fn integer(
    mut longvalue: u64,
    sign: u8,
    inf: &Info,
    mut precision: i64,
    width: i64,
    zeropad: bool,
    mut alt: bool,
    thousand: bool,
) -> SqawkResult<Vec<u8>> {
    if longvalue == 0 {
        alt = false;
    }
    let has_sign = i64::from(sign != 0);
    if zeropad && precision < width - has_sign {
        precision = width - has_sign;
    }
    if precision >= 37 {
        temp_buf(precision + 10 + if thousand { precision / 3 } else { 0 })?;
    }
    // Built backwards, as SQLite builds it.
    let mut rev: Vec<u8> = Vec::new();
    if inf.kind == Kind::Ordinal {
        const ORD: &[u8; 8] = b"thstndrd";
        let mut x = (longvalue % 10) as usize;
        if x >= 4 || (longvalue / 10) % 10 == 1 {
            x = 0;
        }
        rev.push(ORD[x * 2 + 1]);
        rev.push(ORD[x * 2]);
    }
    let digits: &[u8; 16] = if inf.upper {
        b"0123456789ABCDEF"
    } else {
        b"0123456789abcdef"
    };
    loop {
        rev.push(digits[(longvalue % inf.base) as usize]);
        longvalue /= inf.base;
        if longvalue == 0 {
            break;
        }
    }
    while precision > rev.len() as i64 {
        rev.push(b'0');
    }
    let mut fwd: Vec<u8> = rev.into_iter().rev().collect();
    if thousand {
        let mut grouped = Vec::with_capacity(fwd.len() + fwd.len() / 3);
        let first = (fwd.len() - 1) % 3 + 1;
        for (k, &b) in fwd.iter().enumerate() {
            if k >= first && (k - first).is_multiple_of(3) {
                grouped.push(b',');
            }
            grouped.push(b);
        }
        fwd = grouped;
    }
    let mut body = Vec::with_capacity(fwd.len() + 3);
    if alt {
        body.extend_from_slice(inf.alt_prefix);
    }
    if sign != 0 {
        body.push(sign);
    }
    body.extend_from_slice(&fwd);
    Ok(body)
}

/// The real conversions (`f e E g G`).
#[allow(clippy::too_many_arguments)]
fn real(
    v: f64,
    kind: Kind,
    upper: bool,
    precision: i64,
    width: i64,
    flag_prefix: u8,
    left: bool,
    zeropad: bool,
    alt: bool,
    alt2: bool,
    thousand: bool,
) -> SqawkResult<Vec<u8>> {
    let mut precision = if precision < 0 { 6 } else { precision };
    precision = precision.min(FP_PRECISION_LIMIT);
    // A `%g` precision of 0 rounds nothing (`%.0g` of 2.5 is `2`), as
    // `%.0f` does.
    let iround = match kind {
        Kind::Float => -precision,
        Kind::Generic => precision,
        _ => precision + 1,
    };
    let s = fp_decode(v, iround, if alt2 { 26 } else { 16 });
    let mut z: &[u8] = s.digits();
    let mut idp = s.idp;
    if s.special == 2 {
        return Ok(if zeropad {
            b"null".to_vec()
        } else {
            b"NaN".to_vec()
        });
    }
    if s.special == 1 {
        if zeropad {
            z = b"9";
            idp = 1000;
        } else {
            let mut b = b"-Inf".to_vec();
            if s.sign == b'-' {
            } else if flag_prefix != 0 {
                b[0] = flag_prefix;
            } else {
                b.remove(0);
            }
            return Ok(b);
        }
    }
    let prefix = if s.sign == b'-' { b'-' } else { flag_prefix };
    let exp = idp - 1;
    let mut kind = kind;
    let flag_rtz;
    if kind == Kind::Generic && precision > 0 {
        precision -= 1;
    }
    if kind == Kind::Generic {
        flag_rtz = !alt;
        if exp < -4 || exp > precision {
            kind = Kind::Exp;
        } else {
            precision -= exp;
            kind = Kind::Float;
        }
    } else {
        flag_rtz = alt2;
    }
    let mut e2 = if kind == Kind::Exp { 0 } else { idp - 1 };
    let mut need = e2.max(0) + precision + width + 15;
    if thousand && e2 > 0 {
        need += (e2 + 2) / 3;
    }
    temp_buf(need)?;
    let flag_dp = precision > 0 || alt || alt2;
    let mut b: Vec<u8> = Vec::new();
    if prefix != 0 {
        b.push(prefix);
    }
    let mut j = 0usize;
    let digit = |j: &mut usize| {
        if *j < z.len() {
            *j += 1;
            z[*j - 1]
        } else {
            b'0'
        }
    };
    if e2 < 0 {
        b.push(b'0');
    } else {
        while e2 >= 0 {
            b.push(digit(&mut j));
            if thousand && e2 % 3 == 0 && e2 > 1 {
                b.push(b',');
            }
            e2 -= 1;
        }
    }
    if flag_dp {
        b.push(b'.');
    }
    e2 += 1;
    while e2 < 0 && precision > 0 {
        b.push(b'0');
        precision -= 1;
        e2 += 1;
    }
    while precision > 0 {
        b.push(digit(&mut j));
        precision -= 1;
    }
    if flag_rtz && flag_dp {
        while b.last() == Some(&b'0') {
            b.pop();
        }
        if b.last() == Some(&b'.') {
            if alt2 {
                b.push(b'0');
            } else {
                b.pop();
            }
        }
    }
    if kind == Kind::Exp {
        let mut exp = idp - 1;
        b.push(if upper { b'E' } else { b'e' });
        if exp < 0 {
            b.push(b'-');
            exp = -exp;
        } else {
            b.push(b'+');
        }
        if exp >= 100 {
            b.push(b'0' + (exp / 100) as u8);
            exp %= 100;
        }
        b.push(b'0' + (exp / 10) as u8);
        b.push(b'0' + (exp % 10) as u8);
    }
    let length = b.len() as i64;
    if zeropad && !left && length < width {
        let at = usize::from(prefix != 0);
        let pad = (width - length) as usize;
        b.splice(at..at, std::iter::repeat_n(b'0', pad));
    }
    Ok(b)
}

/// `printf` over a `&str` format, as text (bytes cut mid-character by a
/// `%s` precision become U+FFFD); `None` is SQL NULL.
pub(crate) fn format_str(fmt: &str, args: &[Register]) -> SqawkResult<Option<String>> {
    Ok(
        format(fmt.as_bytes(), args)?.map(|bytes| match String::from_utf8(bytes) {
            Ok(s) => s,
            Err(e) => String::from_utf8_lossy(e.as_bytes()).into_owned(),
        }),
    )
}

/// One real formatted by a single conversion (`"%.16g"`, `"%06.3f"`, ...).
pub(crate) fn real_str(fmt: &str, v: f64) -> String {
    format_str(fmt, &[Register::Float(v)])
        .ok()
        .flatten()
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(fmt: &str, args: &[Register]) -> String {
        format_str(fmt, args)
            .unwrap()
            .unwrap_or_else(|| "NULL".into())
    }
    fn i(v: i64) -> Register {
        Register::Integer(v)
    }
    fn f(v: f64) -> Register {
        Register::Float(v)
    }
    fn s(v: &str) -> Register {
        Register::String(v.into())
    }

    #[test]
    fn integers() {
        assert_eq!(p("%.3d", &[i(7)]), "007");
        assert_eq!(p("%u", &[i(-1)]), "18446744073709551615");
        assert_eq!(p("%,d", &[i(-1234567)]), "-1,234,567");
        assert_eq!(p("%#x|%#o|%#X", &[i(255), i(8), i(255)]), "0xff|010|0XFF");
        assert_eq!(
            p("%r %r %r %r", &[i(1), i(2), i(13), i(-23)]),
            "1st 2nd 13th -23rd"
        );
        assert_eq!(p("%*d|%-*d|", &[i(4), i(7), i(-4), i(7)]), "   7|7   |");
        assert_eq!(p("%.*d", &[i(-3), i(7)]), "007");
        assert_eq!(p("%lld %ld", &[i(5), i(6)]), "5 6");
        assert_eq!(p("%d", &[s(" 1e3")]), "1");
    }

    #[test]
    fn reals() {
        assert_eq!(p("%.0f %.0f %.0f", &[f(0.5), f(1.5), f(2.5)]), "0 1 2");
        assert_eq!(p("%.20f", &[f(0.1)]), "0.10000000000000000000");
        assert_eq!(p("%!.20f", &[f(0.1)]), "0.1000000000000000055");
        assert_eq!(
            p("%g|%#g|%G", &[f(0.00001), f(1.0), f(1e20)]),
            "1e-05|1.00000|1E+20"
        );
        assert_eq!(p("%e", &[f(0.0)]), "0.000000e+00");
        assert_eq!(p("%,.2f", &[f(1234567.891)]), "1,234,567.89");
        assert_eq!(p("%08.3f|%+.1e", &[f(-1.5), i(12345)]), "-001.500|+1.2e+04");
        assert_eq!(
            p(
                "%f|%5.1f|%010f",
                &[f(f64::INFINITY), f(f64::NEG_INFINITY), f(1.0)]
            ),
            "Inf| -Inf|001.000000"
        );
        assert_eq!(p("%!.15g", &[f(f64::INFINITY)]), "Inf");
        assert_eq!(p("%!0.15g", &[f(f64::INFINITY)]), "9.0e+999");
    }

    #[test]
    fn strings() {
        assert_eq!(
            p(
                "%q|%Q|%Q|%w",
                &[s("it's"), s("a'b"), Register::Null, s("a\"b")]
            ),
            "it''s|'a''b'|NULL|a\"\"b"
        );
        assert_eq!(
            p("%5s|%-5s|%.2s", &[s("ab"), s("ab"), s("abc")]),
            "   ab|ab   |ab"
        );
        assert_eq!(p("%-4s|%!-4s|", &[s("é"), s("é")]), "é  |é   |");
        assert_eq!(p("%.3c|%c", &[s("xyz"), s("")]), "xxx|\0");
        assert_eq!(p("ab%5", &[]), "ab");
        assert_eq!(p("a%yb", &[]), "a");
        assert_eq!(p("50%", &[]), "50%");
        assert_eq!(p("%5%|%n|", &[]), "    %||");
        assert_eq!(p("", &[]), "NULL");
        assert_eq!(p("%5", &[]), "NULL");
        assert_eq!(p("%s", &[s("")]), "");
    }
}
