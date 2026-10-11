//! Date and time functions: DATE, TIME, DATETIME, JULIANDAY, UNIXEPOCH,
//! STRFTIME, TIMEDIFF and the CURRENT_* values -- a port of SQLite 3.45's
//! `date.c`, so time strings, modifiers, rounding and the NULL results for
//! input SQLite rejects all match it.
//!
//! A moment is held as SQLite holds it: whole milliseconds since the Julian
//! day epoch (noon UTC, 24 November 4714 BC proleptic Gregorian), with the
//! broken-down calendar date and time of day computed from it (or it from
//! them) on demand. The calendar arithmetic is Meeus's algorithm exactly as
//! SQLite writes it, C integer truncation included.
//!
//! Times are UTC. `localtime` converts with the operating system's time
//! zone rules (the C library's `localtime_r` / `localtime_s`, as SQLite
//! does) and `utc` converts back.

use super::arith::text_of;
use super::bytecode::Register;
use super::printf;
use crate::error::{SqawkError, SqawkResult};

/// Julian day number of 1970-01-01 00:00 UTC, in milliseconds.
const UNIX_EPOCH_JD_MS: i64 = 210_866_760_000_000;
/// Largest valid moment: 9999-12-31 23:59:59.999.
const MAX_JD_MS: i64 = 464_269_060_799_999;

/// SQLite's `DateTime`.
#[derive(Debug, Clone, Copy, Default)]
struct DateTime {
    /// The Julian day number times 86400000.
    ijd: i64,
    y: i64,
    m: i64,
    d: i64,
    h: i64,
    mi: i64,
    /// Time zone offset in minutes.
    tz: i64,
    /// Seconds (with the fraction), or the raw number of a numeric time
    /// value while `raw_s`.
    s: f64,
    valid_jd: bool,
    raw_s: bool,
    valid_ymd: bool,
    valid_hms: bool,
    valid_tz: bool,
    tz_set: bool,
    is_error: bool,
    use_subsec: bool,
}

fn valid_julian_day(ijd: i64) -> bool {
    (0..=MAX_JD_MS).contains(&ijd)
}

/// The byte at `i`, or 0 (C's terminating NUL) past the end.
fn at(z: &[u8], i: usize) -> u8 {
    z.get(i).copied().unwrap_or(0)
}

/// `sqlite3Isspace`.
fn is_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

fn eq_ic(z: &[u8], s: &str) -> bool {
    z.eq_ignore_ascii_case(s.as_bytes())
}

/// `getDigits`: read fixed-width numbers, each `(digits, min, max, next)`
/// with `next` the separator that must follow (0 for none). Returns how
/// many were read.
fn get_digits(z: &[u8], mut pos: usize, specs: &[(usize, i64, i64, u8)], out: &mut [i64]) -> usize {
    for (k, &(n, min, max, next)) in specs.iter().enumerate() {
        let mut val = 0i64;
        for _ in 0..n {
            let c = at(z, pos);
            if !c.is_ascii_digit() {
                return k;
            }
            val = val * 10 + i64::from(c - b'0');
            pos += 1;
        }
        if val < min || val > max || (next != 0 && next != at(z, pos)) {
            return k;
        }
        out[k] = val;
        pos += 1;
    }
    specs.len()
}

/// `sqlite3AtoF(...) > 0`: the value of text that is entirely one number
/// (surrounding whitespace allowed).
fn atof_pure(z: &[u8]) -> Option<f64> {
    let n = z.len();
    let mut i = 0;
    while i < n && is_space(z[i]) {
        i += 1;
    }
    let start = i;
    if i < n && (z[i] == b'+' || z[i] == b'-') {
        i += 1;
    }
    let mut digits = 0;
    while i < n && z[i].is_ascii_digit() {
        i += 1;
        digits += 1;
    }
    if i < n && z[i] == b'.' {
        i += 1;
        while i < n && z[i].is_ascii_digit() {
            i += 1;
            digits += 1;
        }
    }
    let mut exp_valid = true;
    if i < n && (z[i] == b'e' || z[i] == b'E') {
        i += 1;
        exp_valid = false;
        if i < n && (z[i] == b'+' || z[i] == b'-') {
            i += 1;
        }
        while i < n && z[i].is_ascii_digit() {
            i += 1;
            exp_valid = true;
        }
    }
    let end = i;
    while i < n && is_space(z[i]) {
        i += 1;
    }
    if i == n && digits > 0 && exp_valid {
        std::str::from_utf8(&z[start..end]).ok()?.parse().ok()
    } else {
        None
    }
}

fn datetime_error(p: &mut DateTime) {
    *p = DateTime {
        is_error: true,
        ..DateTime::default()
    };
}

/// `computeJD`: the moment from the calendar fields.
fn compute_jd(p: &mut DateTime) {
    if p.valid_jd {
        return;
    }
    let (mut y, mut m, d) = if p.valid_ymd {
        (p.y, p.m, p.d)
    } else {
        (2000, 1, 1)
    };
    if !(-4713..=9999).contains(&y) || p.raw_s {
        datetime_error(p);
        return;
    }
    if m <= 2 {
        y -= 1;
        m += 12;
    }
    let a = y / 100;
    let b = 2 - a + (a / 4);
    let x1 = 36525 * (y + 4716) / 100;
    let x2 = 306001 * (m + 1) / 10000;
    p.ijd = (((x1 + x2 + d + b) as f64 - 1524.5) * 86_400_000.0) as i64;
    p.valid_jd = true;
    if p.valid_hms {
        p.ijd += p.h * 3_600_000 + p.mi * 60_000 + (p.s * 1000.0 + 0.5) as i64;
        if p.valid_tz {
            p.ijd -= p.tz * 60_000;
            p.valid_ymd = false;
            p.valid_hms = false;
            p.valid_tz = false;
        }
    }
}

/// `computeYMD`: the calendar date from the moment.
fn compute_ymd(p: &mut DateTime) {
    if p.valid_ymd {
        return;
    }
    if !p.valid_jd {
        p.y = 2000;
        p.m = 1;
        p.d = 1;
    } else if !valid_julian_day(p.ijd) {
        datetime_error(p);
        return;
    } else {
        let z = (p.ijd + 43_200_000) / 86_400_000;
        let a = ((z as f64 - 1_867_216.25) / 36_524.25) as i64;
        let a = z + 1 + a - (a / 4);
        let b = a + 1524;
        let c = ((b as f64 - 122.1) / 365.25) as i64;
        let d = (36525 * (c & 32767)) / 100;
        let e = ((b - d) as f64 / 30.6001) as i64;
        let x1 = (30.6001 * e as f64) as i64;
        p.d = b - d - x1;
        p.m = if e < 14 { e - 1 } else { e - 13 };
        p.y = if p.m > 2 { c - 4716 } else { c - 4715 };
    }
    p.valid_ymd = true;
}

/// `computeHMS`: the time of day from the moment.
fn compute_hms(p: &mut DateTime) {
    if p.valid_hms {
        return;
    }
    compute_jd(p);
    let day_ms = (p.ijd + 43_200_000) % 86_400_000;
    p.s = (day_ms % 60_000) as f64 / 1000.0;
    let day_min = day_ms / 60_000;
    p.mi = day_min % 60;
    p.h = day_min / 60;
    p.raw_s = false;
    p.valid_hms = true;
}

fn compute_ymd_hms(p: &mut DateTime) {
    compute_ymd(p);
    compute_hms(p);
}

fn clear_ymd_hms_tz(p: &mut DateTime) {
    p.valid_ymd = false;
    p.valid_hms = false;
    p.valid_tz = false;
}

/// `parseTimezone`: an optional `Z` or `[+-]HH:MM` after the time, then
/// only whitespace. True on error.
fn parse_timezone(z: &[u8], mut pos: usize, p: &mut DateTime) -> bool {
    while is_space(at(z, pos)) {
        pos += 1;
    }
    p.tz = 0;
    let c = at(z, pos);
    if c == b'Z' || c == b'z' {
        pos += 1;
    } else {
        let sgn = match c {
            b'-' => -1,
            b'+' => 1,
            _ => return c != 0,
        };
        pos += 1;
        let mut v = [0i64; 2];
        if get_digits(z, pos, &[(2, 0, 14, b':'), (2, 0, 59, 0)], &mut v) != 2 {
            return true;
        }
        pos += 5;
        p.tz = sgn * (v[1] + v[0] * 60);
    }
    while is_space(at(z, pos)) {
        pos += 1;
    }
    p.tz_set = true;
    at(z, pos) != 0
}

/// `parseHhMmSs`: `HH:MM[:SS[.FFF]]` and an optional zone. True on error.
fn parse_hhmmss(z: &[u8], mut pos: usize, p: &mut DateTime) -> bool {
    let mut v = [0i64; 2];
    if get_digits(z, pos, &[(2, 0, 24, b':'), (2, 0, 59, 0)], &mut v) != 2 {
        return true;
    }
    pos += 5;
    let mut s = 0i64;
    let mut ms = 0.0f64;
    if at(z, pos) == b':' {
        pos += 1;
        let mut sv = [0i64; 1];
        if get_digits(z, pos, &[(2, 0, 59, 0)], &mut sv) != 1 {
            return true;
        }
        s = sv[0];
        pos += 2;
        if at(z, pos) == b'.' && at(z, pos + 1).is_ascii_digit() {
            let mut scale = 1.0;
            pos += 1;
            while at(z, pos).is_ascii_digit() {
                ms = ms * 10.0 + f64::from(at(z, pos) - b'0');
                scale *= 10.0;
                pos += 1;
            }
            ms /= scale;
        }
    }
    p.valid_jd = false;
    p.raw_s = false;
    p.valid_hms = true;
    p.h = v[0];
    p.mi = v[1];
    p.s = s as f64 + ms;
    if parse_timezone(z, pos, p) {
        return true;
    }
    p.valid_tz = p.tz != 0;
    false
}

/// `parseYyyyMmDd`: `[-]YYYY-MM-DD`, then optionally whitespace or `T`
/// and a time. True on error.
fn parse_yyyymmdd(z: &[u8], p: &mut DateTime) -> bool {
    let neg = at(z, 0) == b'-';
    let mut pos = usize::from(neg);
    let mut v = [0i64; 3];
    if get_digits(
        z,
        pos,
        &[(4, 0, 14712, b'-'), (2, 1, 12, b'-'), (2, 1, 31, 0)],
        &mut v,
    ) != 3
    {
        return true;
    }
    pos += 10;
    while is_space(at(z, pos)) || at(z, pos) == b'T' {
        pos += 1;
    }
    if !parse_hhmmss(z, pos, p) {
        // The time was read.
    } else if at(z, pos) == 0 {
        p.valid_hms = false;
    } else {
        return true;
    }
    p.valid_jd = false;
    p.valid_ymd = true;
    p.y = if neg { -v[0] } else { v[0] };
    p.m = v[1];
    p.d = v[2];
    if p.valid_tz {
        compute_jd(p);
    }
    false
}

/// The current moment. SQLite fixes it once per statement; here it is
/// read at each call.
fn set_now(p: &mut DateTime) -> bool {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    p.ijd = UNIX_EPOCH_JD_MS + ms;
    p.valid_jd = true;
    false
}

/// `setRawDateNumber`: a numeric time value, a Julian day number unless a
/// later `unixepoch` / `auto` modifier says otherwise.
fn set_raw_date_number(p: &mut DateTime, r: f64) {
    p.s = r;
    p.raw_s = true;
    if (0.0..5_373_484.5).contains(&r) {
        p.ijd = (r * 86_400_000.0 + 0.5) as i64;
        p.valid_jd = true;
    }
}

/// `parseDateOrTime`: a time string. True on error.
fn parse_date_or_time(z: &[u8], p: &mut DateTime) -> bool {
    if !parse_yyyymmdd(z, p) || !parse_hhmmss(z, 0, p) {
        return false;
    }
    if eq_ic(z, "now") {
        return set_now(p);
    }
    if let Some(r) = atof_pure(z) {
        set_raw_date_number(p, r);
        return false;
    }
    if eq_ic(z, "subsec") || eq_ic(z, "subsecond") {
        p.use_subsec = true;
        return set_now(p);
    }
    true
}

/// SQLite's `aXformType`: unit name, the largest magnitude accepted (a C
/// `float`), and the unit in seconds.
const XFORM: [(&str, f32, f64); 6] = [
    ("second", 4.6427e14, 1.0),
    ("minute", 7.7379e12, 60.0),
    ("hour", 1.2897e11, 3600.0),
    ("day", 5_373_485.0, 86_400.0),
    ("month", 176_546.0, 30.0 * 86_400.0),
    ("year", 14_713.0, 365.0 * 86_400.0),
];

/// Months past December or before January carry into the year.
fn normalize_month(p: &mut DateTime) {
    let x = if p.m > 0 {
        (p.m - 1) / 12
    } else {
        (p.m - 12) / 12
    };
    p.y += x;
    p.m -= x * 12;
}

/// `parseModifier`: apply one modifier. `Ok(true)` when it is not
/// understood (the result is NULL); `idx` is its 1-based position.
fn parse_modifier(z: &[u8], p: &mut DateTime, idx: usize) -> SqawkResult<bool> {
    let Some(&first) = z.first() else {
        return Ok(true);
    };
    let mut rc = true;
    match first.to_ascii_lowercase() {
        b'a' => {
            if eq_ic(z, "auto") {
                if idx > 1 {
                    return Ok(true);
                }
                if !p.raw_s || p.valid_jd {
                    rc = false;
                    p.raw_s = false;
                } else if p.s >= -210_866_760_000.0 && p.s <= 253_402_300_799.0 {
                    let r = p.s * 1000.0 + UNIX_EPOCH_JD_MS as f64;
                    clear_ymd_hms_tz(p);
                    p.ijd = (r + 0.5) as i64;
                    p.valid_jd = true;
                    p.raw_s = false;
                    rc = false;
                }
            }
        }
        b'j' => {
            if eq_ic(z, "julianday") {
                if idx > 1 {
                    return Ok(true);
                }
                if p.valid_jd && p.raw_s {
                    rc = false;
                    p.raw_s = false;
                }
            }
        }
        b'l' => {
            if eq_ic(z, "localtime") {
                to_localtime(p)?;
                rc = false;
            }
        }
        b'u' => {
            if eq_ic(z, "unixepoch") && p.raw_s {
                if idx > 1 {
                    return Ok(true);
                }
                let r = p.s * 1000.0 + UNIX_EPOCH_JD_MS as f64;
                if (0.0..464_269_060_800_000.0).contains(&r) {
                    clear_ymd_hms_tz(p);
                    p.ijd = (r + 0.5) as i64;
                    p.valid_jd = true;
                    p.raw_s = false;
                    rc = false;
                }
            } else if eq_ic(z, "utc") {
                if !p.tz_set {
                    // Find the UTC moment whose local time is this one.
                    compute_jd(p);
                    let orig = p.ijd;
                    let mut guess = orig;
                    let mut err = 0i64;
                    let mut cnt = 0;
                    loop {
                        let mut new = DateTime::default();
                        guess -= err;
                        new.ijd = guess;
                        new.valid_jd = true;
                        to_localtime(&mut new)?;
                        compute_jd(&mut new);
                        err = new.ijd - orig;
                        let again = err != 0 && cnt < 3;
                        cnt += 1;
                        if !again {
                            break;
                        }
                    }
                    *p = DateTime::default();
                    p.ijd = guess;
                    p.valid_jd = true;
                    p.tz_set = true;
                }
                rc = false;
            }
        }
        b'w' => {
            if z.len() >= 8 && z[..8].eq_ignore_ascii_case(b"weekday ") {
                if let Some(r) = atof_pure(&z[8..]) {
                    if (0.0..7.0).contains(&r) && (r as i64) as f64 == r {
                        let n = r as i64;
                        compute_ymd_hms(p);
                        p.valid_tz = false;
                        p.valid_jd = false;
                        compute_jd(p);
                        let mut wd = ((p.ijd + 129_600_000) / 86_400_000) % 7;
                        if wd > n {
                            wd -= 7;
                        }
                        p.ijd += (n - wd) * 86_400_000;
                        clear_ymd_hms_tz(p);
                        rc = false;
                    }
                }
            }
        }
        b's' => {
            if !(z.len() >= 9 && z[..9].eq_ignore_ascii_case(b"start of ")) {
                if eq_ic(z, "subsec") || eq_ic(z, "subsecond") {
                    p.use_subsec = true;
                    rc = false;
                }
            } else if p.valid_jd || p.valid_ymd || p.valid_hms {
                let unit = &z[9..];
                compute_ymd(p);
                p.valid_hms = true;
                p.h = 0;
                p.mi = 0;
                p.s = 0.0;
                p.raw_s = false;
                p.valid_tz = false;
                p.valid_jd = false;
                if eq_ic(unit, "month") {
                    p.d = 1;
                    rc = false;
                } else if eq_ic(unit, "year") {
                    p.m = 1;
                    p.d = 1;
                    rc = false;
                } else if eq_ic(unit, "day") {
                    rc = false;
                }
            }
        }
        b'+' | b'-' | b'0'..=b'9' => rc = numeric_modifier(z, p),
        _ => {}
    }
    Ok(rc)
}

/// The modifiers that start with a sign or a digit: `±NNN unit`,
/// `±HH:MM[:SS[.FFF]]`, `±YYYY-MM-DD` and `±YYYY-MM-DD HH:MM[:SS[.FFF]]`.
/// True when not understood.
fn numeric_modifier(z: &[u8], p: &mut DateTime) -> bool {
    let z0 = z[0];
    let mut n = 1;
    while n < z.len() {
        let c = z[n];
        if c == b':' || is_space(c) {
            break;
        }
        if c == b'-' {
            let mut y = [0i64; 1];
            if n == 5 && get_digits(z, 1, &[(4, 0, 14712, 0)], &mut y) == 1 {
                break;
            }
            if n == 6 && get_digits(z, 1, &[(5, 0, 14712, 0)], &mut y) == 1 {
                break;
            }
        }
        n += 1;
    }
    let Some(mut r) = atof_pure(&z[..n]) else {
        return true;
    };
    // Where a time offset starts: `z` itself, or the time after a date.
    let mut z2 = 0usize;
    if at(z, n) == b'-' {
        // (+|-)YYYY-MM-DD: years, months (0-11) and days (0-30).
        if z0 != b'+' && z0 != b'-' {
            return true;
        }
        let mut v = [0i64; 3];
        let year_digits = if n == 5 { 4 } else { 5 };
        if get_digits(
            z,
            1,
            &[
                (year_digits, 0, 14712, b'-'),
                (2, 0, 12, b'-'),
                (2, 0, 31, 0),
            ],
            &mut v,
        ) != 3
        {
            return true;
        }
        let off = usize::from(n == 6);
        let (years, months, mut days) = (v[0], v[1], v[2]);
        if months >= 12 || days >= 31 {
            return true;
        }
        compute_ymd_hms(p);
        p.valid_jd = false;
        if z0 == b'-' {
            p.y -= years;
            p.m -= months;
            days = -days;
        } else {
            p.y += years;
            p.m += months;
        }
        normalize_month(p);
        compute_jd(p);
        p.valid_hms = false;
        p.valid_ymd = false;
        p.ijd += days * 86_400_000;
        if at(z, off + 11) == 0 {
            return false;
        }
        let mut hm = [0i64; 2];
        if is_space(at(z, off + 11))
            && get_digits(z, off + 12, &[(2, 0, 24, b':'), (2, 0, 59, 0)], &mut hm) == 2
        {
            z2 = off + 12;
            n = 2;
        } else {
            return true;
        }
    }
    if at(z, z2 + n) == b':' {
        // (+|-)HH:MM[:SS[.FFF]] moves the time by that much.
        if !at(z, z2).is_ascii_digit() {
            z2 += 1;
        }
        let mut tx = DateTime::default();
        if parse_hhmmss(z, z2, &mut tx) {
            return true;
        }
        compute_jd(&mut tx);
        tx.ijd -= 43_200_000;
        let day = tx.ijd / 86_400_000;
        tx.ijd -= day * 86_400_000;
        if z0 == b'-' {
            tx.ijd = -tx.ijd;
        }
        compute_jd(p);
        clear_ymd_hms_tz(p);
        p.ijd += tx.ijd;
        return false;
    }
    // `±NNN unit`.
    let mut unit = &z[n..];
    while unit.first().is_some_and(|&c| is_space(c)) {
        unit = &unit[1..];
    }
    let mut len = unit.len();
    if !(3..=10).contains(&len) {
        return true;
    }
    if unit[len - 1].eq_ignore_ascii_case(&b's') {
        len -= 1;
    }
    compute_jd(p);
    let rounder = if r < 0.0 { -0.5 } else { 0.5 };
    let mut rc = true;
    for (i, &(name, limit, xform)) in XFORM.iter().enumerate() {
        let limit = f64::from(limit);
        if name.len() == len
            && unit[..len].eq_ignore_ascii_case(name.as_bytes())
            && r > -limit
            && r < limit
        {
            match i {
                4 => {
                    // Months move the calendar date by their whole part.
                    compute_ymd_hms(p);
                    p.m += r as i64;
                    normalize_month(p);
                    p.valid_jd = false;
                    r -= (r as i64) as f64;
                }
                5 => {
                    let y = r as i64;
                    compute_ymd_hms(p);
                    p.y += y;
                    p.valid_jd = false;
                    r -= y as f64;
                }
                _ => {}
            }
            compute_jd(p);
            p.ijd += (r * 1000.0 * xform + rounder) as i64;
            rc = false;
            break;
        }
    }
    clear_ymd_hms_tz(p);
    rc
}

/// One local broken-down time from the C library.
struct LocalTm {
    year: i64,
    mon: i64,
    mday: i64,
    hour: i64,
    min: i64,
    sec: i64,
}

/// `t` as the C library's `time_t`, which is 32 bits on some targets.
#[cfg(any(unix, windows))]
#[allow(clippy::useless_conversion)]
fn to_time_t(t: i64) -> Option<libc::time_t> {
    libc::time_t::try_from(t).ok()
}

#[cfg(unix)]
fn os_localtime(t: i64) -> Option<LocalTm> {
    extern "C" {
        fn tzset();
    }
    static TZSET: std::sync::Once = std::sync::Once::new();
    // SAFETY: tzset only (re)reads the TZ environment variable into the C
    // library's time zone state; it is called once, before any conversion.
    TZSET.call_once(|| unsafe { tzset() });
    let tt = to_time_t(t)?;
    // SAFETY: `tm` is plain old data, so all-zero is a valid value.
    let mut tm: libc::tm = unsafe { std::mem::zeroed() };
    // SAFETY: both pointers are valid for the duration of the call, and
    // localtime_r writes only into `tm`.
    let r = unsafe { libc::localtime_r(&tt, &mut tm) };
    if r.is_null() {
        return None;
    }
    Some(LocalTm {
        year: i64::from(tm.tm_year),
        mon: i64::from(tm.tm_mon),
        mday: i64::from(tm.tm_mday),
        hour: i64::from(tm.tm_hour),
        min: i64::from(tm.tm_min),
        sec: i64::from(tm.tm_sec),
    })
}

#[cfg(windows)]
fn os_localtime(t: i64) -> Option<LocalTm> {
    let tt = to_time_t(t)?;
    // SAFETY: `tm` is plain old data, so all-zero is a valid value.
    let mut tm: libc::tm = unsafe { std::mem::zeroed() };
    // SAFETY: both pointers are valid for the duration of the call, and
    // localtime_s writes only into `tm`.
    if unsafe { libc::localtime_s(&mut tm, &tt) } != 0 {
        return None;
    }
    Some(LocalTm {
        year: i64::from(tm.tm_year),
        mon: i64::from(tm.tm_mon),
        mday: i64::from(tm.tm_mday),
        hour: i64::from(tm.tm_hour),
        min: i64::from(tm.tm_min),
        sec: i64::from(tm.tm_sec),
    })
}

#[cfg(not(any(unix, windows)))]
fn os_localtime(_t: i64) -> Option<LocalTm> {
    None
}

/// `toLocaltime`: shift a UTC moment to local time. Outside 1970-2037 the
/// year is mapped into that range, converted, and mapped back, as SQLite
/// does.
fn to_localtime(p: &mut DateTime) -> SqawkResult<()> {
    compute_jd(p);
    let (t, year_diff) = if p.ijd < 210_866_760_000_000 || p.ijd > 213_014_145_600_000 {
        let mut x = *p;
        compute_ymd_hms(&mut x);
        let year_diff = (2000 + x.y % 4) - x.y;
        x.y += year_diff;
        x.valid_jd = false;
        compute_jd(&mut x);
        (x.ijd / 1000 - 210_866_760_000, year_diff)
    } else {
        (p.ijd / 1000 - 210_866_760_000, 0)
    };
    let tm = os_localtime(t)
        .ok_or_else(|| SqawkError::InvalidSqlQuery("local time unavailable".into()))?;
    p.y = tm.year + 1900 - year_diff;
    p.m = tm.mon + 1;
    p.d = tm.mday;
    p.h = tm.hour;
    p.mi = tm.min;
    p.s = tm.sec as f64 + (p.ijd % 1000) as f64 * 0.001;
    p.valid_ymd = true;
    p.valid_hms = true;
    p.valid_jd = false;
    p.raw_s = false;
    p.valid_tz = false;
    p.is_error = false;
    Ok(())
}

/// `isDate`: a date function's arguments (time value, then modifiers) to a
/// moment; `None` for NULL. No arguments means now.
fn is_date(args: &[Register]) -> SqawkResult<Option<DateTime>> {
    let mut p = DateTime::default();
    let Some(first) = args.first() else {
        set_now(&mut p);
        return Ok(Some(p));
    };
    match first {
        Register::Null => return Ok(None),
        Register::Integer(i) => set_raw_date_number(&mut p, *i as f64),
        Register::Boolean(b) => set_raw_date_number(&mut p, f64::from(u8::from(*b))),
        Register::Float(f) => set_raw_date_number(&mut p, *f),
        Register::String(s) => {
            if parse_date_or_time(c_str(s.as_bytes()), &mut p) {
                return Ok(None);
            }
        }
    }
    for (k, m) in args[1..].iter().enumerate() {
        if matches!(m, Register::Null) {
            return Ok(None);
        }
        let text = text_of(m);
        if parse_modifier(c_str(text.as_bytes()), &mut p, k + 1)? {
            return Ok(None);
        }
    }
    compute_jd(&mut p);
    if p.is_error || !valid_julian_day(p.ijd) {
        return Ok(None);
    }
    if args.len() == 1 && p.valid_ymd && p.d > 28 {
        // A day past the end of its month rolls over: 2023-02-31 is
        // 2023-03-03.
        p.valid_ymd = false;
    }
    Ok(Some(p))
}

/// Text up to its first NUL, as C sees it.
fn c_str(z: &[u8]) -> &[u8] {
    &z[..z.iter().position(|&b| b == 0).unwrap_or(z.len())]
}

/// `[-]YYYY-MM-DD`: the year as four digits (of its magnitude), signed.
fn fmt_date(p: &DateTime) -> String {
    let y = p.y.abs();
    format!(
        "{}{}{}{}{}-{}{}-{}{}",
        if p.y < 0 { "-" } else { "" },
        (y / 1000) % 10,
        (y / 100) % 10,
        (y / 10) % 10,
        y % 10,
        (p.m / 10) % 10,
        p.m % 10,
        (p.d / 10) % 10,
        p.d % 10
    )
}

/// `HH:MM:SS`, or `HH:MM:SS.SSS` with `subsec`.
fn fmt_time(p: &DateTime) -> String {
    let mut out = format!(
        "{}{}:{}{}:",
        (p.h / 10) % 10,
        p.h % 10,
        (p.mi / 10) % 10,
        p.mi % 10
    );
    if p.use_subsec {
        let s = (1000.0 * p.s + 0.5) as i64;
        out.push_str(&format!(
            "{}{}.{}{}{}",
            (s / 10000) % 10,
            (s / 1000) % 10,
            (s / 100) % 10,
            (s / 10) % 10,
            s % 10
        ));
    } else {
        let s = p.s as i64;
        out.push_str(&format!("{}{}", (s / 10) % 10, s % 10));
    }
    out
}

fn text_or_null(v: Option<String>) -> Register {
    v.map_or(Register::Null, Register::String)
}

/// `DATE(timevalue, modifier...)`.
pub(crate) fn date(args: &[Register]) -> SqawkResult<Register> {
    Ok(text_or_null(is_date(args)?.map(|mut p| {
        compute_ymd(&mut p);
        fmt_date(&p)
    })))
}

/// `TIME(timevalue, modifier...)`.
pub(crate) fn time(args: &[Register]) -> SqawkResult<Register> {
    Ok(text_or_null(is_date(args)?.map(|mut p| {
        compute_hms(&mut p);
        fmt_time(&p)
    })))
}

/// `DATETIME(timevalue, modifier...)`.
pub(crate) fn datetime(args: &[Register]) -> SqawkResult<Register> {
    Ok(text_or_null(is_date(args)?.map(|mut p| {
        compute_ymd_hms(&mut p);
        format!("{} {}", fmt_date(&p), fmt_time(&p))
    })))
}

/// `JULIANDAY(timevalue, modifier...)`: a real.
pub(crate) fn julianday(args: &[Register]) -> SqawkResult<Register> {
    Ok(is_date(args)?.map_or(Register::Null, |mut p| {
        compute_jd(&mut p);
        Register::Float(p.ijd as f64 / 86_400_000.0)
    }))
}

/// `UNIXEPOCH(timevalue, modifier...)`: whole seconds, or a real with
/// milliseconds after `subsec`.
pub(crate) fn unixepoch(args: &[Register]) -> SqawkResult<Register> {
    Ok(is_date(args)?.map_or(Register::Null, |mut p| {
        compute_jd(&mut p);
        if p.use_subsec {
            Register::Float((p.ijd - UNIX_EPOCH_JD_MS) as f64 / 1000.0)
        } else {
            Register::Integer(p.ijd / 1000 - UNIX_EPOCH_JD_MS / 1000)
        }
    }))
}

/// `STRFTIME(format, timevalue, modifier...)` with SQLite 3.45's
/// conversions: `%d %e %f %F %H %I %j %J %k %l %m %M %p %P %R %s %S %T %u
/// %w %W %Y %%`. Any other conversion (a trailing `%` included) makes the
/// result NULL, as in SQLite.
pub(crate) fn strftime(args: &[Register]) -> SqawkResult<Register> {
    let fmt = match args.first() {
        Some(Register::Null) | None => return Ok(Register::Null),
        Some(r) => text_of(r),
    };
    let Some(mut x) = is_date(&args[1..])? else {
        return Ok(Register::Null);
    };
    compute_jd(&mut x);
    compute_ymd_hms(&mut x);
    let fmt = c_str(fmt.as_bytes());
    let mut out: Vec<u8> = Vec::with_capacity(fmt.len() + 8);
    let mut i = 0;
    while i < fmt.len() {
        if fmt[i] != b'%' {
            out.push(fmt[i]);
            i += 1;
            continue;
        }
        let cf = at(fmt, i + 1);
        i += 2;
        let piece = match cf {
            b'd' => format!("{:02}", x.d),
            b'e' => format!("{:2}", x.d),
            b'f' => printf::real_str("%06.3f", x.s.min(59.999)),
            b'F' => format!("{:04}-{:02}-{:02}", x.y, x.m, x.d),
            b'H' => format!("{:02}", x.h),
            b'k' => format!("{:2}", x.h),
            b'I' | b'l' => {
                let mut h = x.h;
                if h > 12 {
                    h -= 12;
                }
                if h == 0 {
                    h = 12;
                }
                if cf == b'I' {
                    format!("{:02}", h)
                } else {
                    format!("{:2}", h)
                }
            }
            b'W' | b'j' => {
                let mut y = x;
                y.valid_jd = false;
                y.m = 1;
                y.d = 1;
                compute_jd(&mut y);
                let n_day = (x.ijd - y.ijd + 43_200_000) / 86_400_000;
                if cf == b'W' {
                    // 0 = Monday.
                    let wd = ((x.ijd + 43_200_000) / 86_400_000) % 7;
                    format!("{:02}", (n_day + 7 - wd) / 7)
                } else {
                    format!("{:03}", n_day + 1)
                }
            }
            b'J' => printf::real_str("%.16g", x.ijd as f64 / 86_400_000.0),
            b'm' => format!("{:02}", x.m),
            b'M' => format!("{:02}", x.mi),
            b'p' => (if x.h >= 12 { "PM" } else { "AM" }).to_string(),
            b'P' => (if x.h >= 12 { "pm" } else { "am" }).to_string(),
            b'R' => format!("{:02}:{:02}", x.h, x.mi),
            b's' => {
                if x.use_subsec {
                    printf::real_str("%.3f", (x.ijd - UNIX_EPOCH_JD_MS) as f64 / 1000.0)
                } else {
                    format!("{}", x.ijd / 1000 - UNIX_EPOCH_JD_MS / 1000)
                }
            }
            b'S' => format!("{:02}", x.s as i64),
            b'T' => format!("{:02}:{:02}:{:02}", x.h, x.mi, x.s as i64),
            b'u' | b'w' => {
                let wd = ((x.ijd + 129_600_000) / 86_400_000) % 7;
                format!("{}", if wd == 0 && cf == b'u' { 7 } else { wd })
            }
            b'Y' => format!("{:04}", x.y),
            b'%' => "%".to_string(),
            _ => return Ok(Register::Null),
        };
        out.extend_from_slice(piece.as_bytes());
    }
    Ok(Register::String(String::from_utf8_lossy(&out).into_owned()))
}

/// `TIMEDIFF(a, b)`: the time from `b` to `a` as `±YYYY-MM-DD HH:MM:SS.SSS`
/// -- whole years and months first, then days and time.
pub(crate) fn timediff(args: &[Register]) -> SqawkResult<Register> {
    let (Some(mut d1), Some(mut d2)) = (is_date(&args[..1])?, is_date(&args[1..2])?) else {
        return Ok(Register::Null);
    };
    compute_ymd_hms(&mut d1);
    compute_ymd_hms(&mut d2);
    let sign;
    let mut y;
    let mut m;
    if d1.ijd >= d2.ijd {
        sign = '+';
        y = d1.y - d2.y;
        if y != 0 {
            d2.y = d1.y;
            d2.valid_jd = false;
            compute_jd(&mut d2);
        }
        m = d1.m - d2.m;
        if m < 0 {
            y -= 1;
            m += 12;
        }
        if m != 0 {
            d2.m = d1.m;
            d2.valid_jd = false;
            compute_jd(&mut d2);
        }
        while d1.ijd < d2.ijd {
            m -= 1;
            if m < 0 {
                m = 11;
                y -= 1;
            }
            d2.m -= 1;
            if d2.m < 1 {
                d2.m = 12;
                d2.y -= 1;
            }
            d2.valid_jd = false;
            compute_jd(&mut d2);
        }
        d1.ijd -= d2.ijd;
    } else {
        sign = '-';
        y = d2.y - d1.y;
        if y != 0 {
            d2.y = d1.y;
            d2.valid_jd = false;
            compute_jd(&mut d2);
        }
        m = d2.m - d1.m;
        if m < 0 {
            y -= 1;
            m += 12;
        }
        if m != 0 {
            d2.m = d1.m;
            d2.valid_jd = false;
            compute_jd(&mut d2);
        }
        while d1.ijd > d2.ijd {
            m -= 1;
            if m < 0 {
                m = 11;
                y -= 1;
            }
            d2.m += 1;
            if d2.m > 12 {
                d2.m = 1;
                d2.y += 1;
            }
            d2.valid_jd = false;
            compute_jd(&mut d2);
        }
        d1.ijd = d2.ijd - d1.ijd;
    }
    d1.ijd += 148_699_540_800_000;
    clear_ymd_hms_tz(&mut d1);
    compute_ymd_hms(&mut d1);
    Ok(Register::String(format!(
        "{}{:04}-{:02}-{:02} {:02}:{:02}:{}",
        sign,
        y,
        m,
        d1.d - 1,
        d1.h,
        d1.mi,
        printf::real_str("%06.3f", d1.s)
    )))
}

/// `CURRENT_DATE`.
pub(crate) fn current_date(_: &[Register]) -> SqawkResult<Register> {
    date(&[])
}

/// `CURRENT_TIME`.
pub(crate) fn current_time(_: &[Register]) -> SqawkResult<Register> {
    time(&[])
}

/// `CURRENT_TIMESTAMP`, `NOW()`.
pub(crate) fn current_timestamp(_: &[Register]) -> SqawkResult<Register> {
    datetime(&[])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> Register {
        Register::String(v.to_string())
    }

    fn txt(r: SqawkResult<Register>) -> Option<String> {
        match r.unwrap() {
            Register::String(s) => Some(s),
            Register::Null => None,
            other => Some(format!("{:?}", other)),
        }
    }

    #[test]
    fn sqlite_examples() {
        assert_eq!(txt(date(&[s("2024-02-30")])).as_deref(), Some("2024-03-01"));
        assert_eq!(txt(date(&[s("2024-02-32")])), None);
        assert_eq!(txt(date(&[s("2024-1-5")])), None);
        assert_eq!(txt(date(&[s("2100-02-29")])).as_deref(), Some("2100-03-01"));
        assert_eq!(
            txt(datetime(&[s("10:30")])).as_deref(),
            Some("2000-01-01 10:30:00")
        );
        assert_eq!(
            txt(time(&[s("2024-01-15 10:30:59.9999")])).as_deref(),
            Some("10:30:59")
        );
        assert_eq!(
            txt(date(&[s("2024-01-31"), s("+1 month")])).as_deref(),
            Some("2024-03-02")
        );
        assert_eq!(
            txt(date(&[s("2024-01-15"), s("weekday 0")])).as_deref(),
            Some("2024-01-21")
        );
        assert_eq!(
            txt(date(&[Register::Integer(1_705_312_200), s("unixepoch")])).as_deref(),
            Some("2024-01-15")
        );
        assert_eq!(
            txt(date(&[Register::Float(2_460_324.5)])).as_deref(),
            Some("2024-01-15")
        );
        assert_eq!(
            txt(datetime(&[Register::Integer(0)])).as_deref(),
            Some("-4713-11-24 12:00:00")
        );
        assert_eq!(
            txt(strftime(&[s("%j %w %u %s %%"), s("2024-01-07")])).as_deref(),
            Some("007 0 7 1704585600 %")
        );
        assert_eq!(
            txt(date(&[s("2024-01-15 10:30:00+02:00")])).as_deref(),
            Some("2024-01-15")
        );
        assert_eq!(
            txt(date(&[s("-0001-12-31")])).as_deref(),
            Some("-0001-12-31")
        );
        assert_eq!(
            txt(date(&[s("2024-01-01 ")])).as_deref(),
            Some("2024-01-01")
        );
        assert_eq!(
            txt(date(&[s("2024-01-01"), s("+0001-02-03")])).as_deref(),
            Some("2025-03-04")
        );
        assert_eq!(
            txt(datetime(&[s("2024-01-01 10:00:00"), s("+01:30")])).as_deref(),
            Some("2024-01-01 11:30:00")
        );
        assert_eq!(
            txt(timediff(&[s("2024-03-01"), s("2023-01-15 12:00")])).as_deref(),
            Some("+0001-01-14 12:00:00.000")
        );
    }
}
