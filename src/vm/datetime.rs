//! Date and time functions, following SQLite's: DATE, TIME, DATETIME,
//! JULIANDAY, UNIXEPOCH, STRFTIME and the CURRENT_* values. All in UTC.
//!
//! A moment is held as SQLite holds it: whole milliseconds since the Julian
//! day epoch (noon UTC, 24 November 4714 BC proleptic Gregorian). Calendar
//! conversion uses Howard Hinnant's `days_from_civil` / `civil_from_days`,
//! which are exact over the whole proleptic Gregorian calendar.
//!
//! As in SQLite, a time string or modifier that cannot be understood yields
//! NULL rather than an error; a day-of-month past the end of the month (up
//! to 31) rolls over into the next month, so `DATE('2024-02-30')` is
//! `2024-03-01`.

use super::arith::Num;
use super::bytecode::Register;
use crate::error::{SqawkError, SqawkResult};

const MS_PER_DAY: i64 = 86_400_000;
/// Julian day number of 1970-01-01 00:00 UTC, in milliseconds.
const UNIX_EPOCH_JD_MS: i64 = 210_866_760_000_000;
/// Largest valid moment: 9999-12-31 23:59:59.999.
const MAX_JD_MS: i64 = 464_269_060_799_999;

/// Days since 1970-01-01 of a proleptic Gregorian date.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The proleptic Gregorian date `days` after 1970-01-01.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

/// A broken-down moment.
#[derive(Debug, Clone, Copy)]
struct Parts {
    y: i64,
    m: i64,
    d: i64,
    h: i64,
    mi: i64,
    /// Milliseconds within the minute.
    ms: i64,
}

impl Parts {
    fn from_jd(jd: i64) -> Parts {
        let unix = jd - UNIX_EPOCH_JD_MS;
        let days = unix.div_euclid(MS_PER_DAY);
        let in_day = unix.rem_euclid(MS_PER_DAY);
        let (y, m, d) = civil_from_days(days);
        Parts {
            y,
            m,
            d,
            h: in_day / 3_600_000,
            mi: in_day / 60_000 % 60,
            ms: in_day % 60_000,
        }
    }

    /// Back to a moment. Days and months past their range roll over, which
    /// is how `+1 month` from January 31st lands in March.
    fn to_jd(self) -> i64 {
        let y = self.y + (self.m - 1).div_euclid(12);
        let m = (self.m - 1).rem_euclid(12) + 1;
        let days = days_from_civil(y, m, 1) + self.d - 1;
        UNIX_EPOCH_JD_MS + days * MS_PER_DAY + self.h * 3_600_000 + self.mi * 60_000 + self.ms
    }
}

fn now_jd() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    UNIX_EPOCH_JD_MS + ms
}

/// Parse exactly `n` ASCII digits at the front of `s`.
fn digits(s: &[u8], n: usize) -> Option<i64> {
    if s.len() < n || !s[..n].iter().all(u8::is_ascii_digit) {
        return None;
    }
    Some(s[..n].iter().fold(0, |a, &c| a * 10 + (c - b'0') as i64))
}

/// `HH:MM[:SS[.fff]]` followed by an optional `Z` or `[+-]HH:MM` zone.
/// Returns (milliseconds into the day, zone offset in ms, rest).
fn parse_time(s: &[u8]) -> Option<(i64, i64)> {
    let h = digits(s, 2)?;
    if s.get(2) != Some(&b':') {
        return None;
    }
    let mi = digits(&s[3..], 2)?;
    let mut i = 5;
    let mut ms = 0;
    if s.get(i) == Some(&b':') {
        let sec = digits(&s[i + 1..], 2)?;
        i += 3;
        let mut frac = 0.0;
        if s.get(i) == Some(&b'.') && s.get(i + 1).is_some_and(u8::is_ascii_digit) {
            i += 1;
            let mut scale = 1.0;
            while let Some(c) = s.get(i).filter(|c| c.is_ascii_digit()) {
                frac = frac * 10.0 + (c - b'0') as f64;
                scale *= 10.0;
                i += 1;
            }
            frac /= scale;
        }
        if sec > 59 {
            return None;
        }
        ms = sec * 1000 + ((frac * 1000.0).round() as i64).min(999);
    }
    if h > 24 || mi > 59 {
        return None;
    }
    while s.get(i) == Some(&b' ') {
        i += 1;
    }
    let mut zone = 0;
    match s.get(i) {
        None => {}
        Some(b'Z') | Some(b'z') if i + 1 == s.len() => {}
        Some(&c @ (b'+' | b'-')) => {
            let zh = digits(&s[i + 1..], 2)?;
            if s.get(i + 3) != Some(&b':') {
                return None;
            }
            let zm = digits(&s[i + 4..], 2)?;
            if i + 6 != s.len() || zm > 59 {
                return None;
            }
            let off = (zh * 60 + zm) * 60_000;
            zone = if c == b'-' { -off } else { off };
        }
        _ => return None,
    }
    Some((h * 3_600_000 + mi * 60_000 + ms, zone))
}

/// A time string to a moment: `now`, `YYYY-MM-DD[( |T)HH:MM[:SS[.fff]]]`,
/// `HH:MM[:SS]` (on 2000-01-01), or a number (a Julian day number).
fn parse_timestring(s: &str) -> Option<i64> {
    if s.eq_ignore_ascii_case("now") {
        return Some(now_jd());
    }
    let b = s.as_bytes();
    if b.len() >= 10 && b[4] == b'-' && b[7] == b'-' {
        let y = digits(b, 4)?;
        let m = digits(&b[5..], 2)?;
        let d = digits(&b[8..], 2)?;
        if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
            return None;
        }
        let (in_day, zone) = match b.get(10) {
            None => (0, 0),
            Some(b' ') | Some(b'T') => parse_time(&b[11..])?,
            _ => return None,
        };
        let p = Parts {
            y,
            m,
            d,
            h: 0,
            mi: 0,
            ms: 0,
        };
        return Some(p.to_jd() + in_day - zone);
    }
    if b.len() >= 5 && b[2] == b':' {
        let (in_day, zone) = parse_time(b)?;
        let base = Parts {
            y: 2000,
            m: 1,
            d: 1,
            h: 0,
            mi: 0,
            ms: 0,
        };
        return Some(base.to_jd() + in_day - zone);
    }
    julian(numeric(s)?.as_f64())
}

/// Text that is entirely a number (surrounding blanks allowed).
fn numeric(s: &str) -> Option<Num> {
    match crate::table::numeric_text(s)? {
        crate::table::Value::Integer(i) => Some(Num::Int(i)),
        crate::table::Value::Float(f) => Some(Num::Real(f)),
        _ => None,
    }
}

/// A Julian day number to a moment.
fn julian(jd: f64) -> Option<i64> {
    let ms = (jd * MS_PER_DAY as f64).round();
    (ms >= 0.0 && ms <= MAX_JD_MS as f64).then_some(ms as i64)
}

/// The first argument of a date function as a moment, with
/// `unixepoch_next` telling whether the first modifier is `unixepoch`.
fn initial(arg: &Register, unixepoch_next: bool) -> Option<i64> {
    let num = match arg {
        Register::Integer(i) => Some(Num::Int(*i)),
        Register::Float(f) => Some(Num::Real(*f)),
        Register::Boolean(b) => Some(Num::Int(*b as i64)),
        Register::String(s) if unixepoch_next => numeric(s),
        Register::String(s) => return parse_timestring(s),
        Register::Null => return None,
    };
    let n = num?;
    if unixepoch_next {
        let ms = (n.as_f64() * 1000.0).round();
        let jd = UNIX_EPOCH_JD_MS as f64 + ms;
        (jd >= 0.0 && jd <= MAX_JD_MS as f64).then_some(jd as i64)
    } else {
        julian(n.as_f64())
    }
}

/// Apply one modifier. `None`: not understood, so the result is NULL.
///
/// As in SQLite the moment may leave the valid range in between modifiers
/// (it is checked once, at the end), but a modifier that works on the
/// calendar date needs a valid moment to start from.
fn modify(jd: i64, m: &str) -> SqawkResult<Option<i64>> {
    let lower = m.to_ascii_lowercase();
    let valid = (0..=MAX_JD_MS).contains(&jd);
    if !valid && (lower.starts_with("start of ") || lower.starts_with("weekday ")) {
        return Ok(None);
    }
    let simple = match lower.as_str() {
        "utc" => Some(jd),
        "localtime" => {
            return Err(SqawkError::UnsupportedSqlFeature(
                "the localtime date modifier is not supported (sqawk works in UTC)".into(),
            ))
        }
        "start of day" => {
            let p = Parts::from_jd(jd);
            Some(
                Parts {
                    h: 0,
                    mi: 0,
                    ms: 0,
                    ..p
                }
                .to_jd(),
            )
        }
        "start of month" => {
            let p = Parts::from_jd(jd);
            Some(
                Parts {
                    d: 1,
                    h: 0,
                    mi: 0,
                    ms: 0,
                    ..p
                }
                .to_jd(),
            )
        }
        "start of year" => {
            let p = Parts::from_jd(jd);
            Some(
                Parts {
                    m: 1,
                    d: 1,
                    h: 0,
                    mi: 0,
                    ms: 0,
                    ..p
                }
                .to_jd(),
            )
        }
        _ => None,
    };
    if simple.is_some() {
        return Ok(simple);
    }
    if let Some(n) = lower.strip_prefix("weekday ") {
        let Ok(want) = n.parse::<i64>() else {
            return Ok(None);
        };
        if !(0..=6).contains(&want) {
            return Ok(None);
        }
        let mut day = (jd - UNIX_EPOCH_JD_MS).div_euclid(MS_PER_DAY);
        // 1970-01-01 was a Thursday (4).
        let wd = (day + 4).rem_euclid(7);
        day += (want - wd).rem_euclid(7);
        let in_day = (jd - UNIX_EPOCH_JD_MS).rem_euclid(MS_PER_DAY);
        return Ok(Some(UNIX_EPOCH_JD_MS + day * MS_PER_DAY + in_day));
    }
    // `[+-]N unit`.
    let Some((num, unit)) = lower.split_once(' ') else {
        return Ok(None);
    };
    if !num.starts_with(['+', '-']) && !num.starts_with(|c: char| c.is_ascii_digit()) {
        return Ok(None);
    }
    let Ok(n) = num.parse::<f64>() else {
        return Ok(None);
    };
    let unit = unit.strip_suffix('s').unwrap_or(unit);
    // SQLite's aXformType: each unit's size in seconds and the largest
    // magnitude accepted (beyond it, or NaN, the modifier is not understood).
    let (secs, limit) = match unit {
        "second" => (1.0, 4.6427e14),
        "minute" => (60.0, 7.7379e12),
        "hour" => (3600.0, 1.2897e11),
        "day" => (86_400.0, 5_373_485.0),
        "month" => (30.0 * 86_400.0, 176_546.0),
        "year" => (365.0 * 86_400.0, 14_713.0),
        _ => return Ok(None),
    };
    if !(n > -limit && n < limit) {
        return Ok(None);
    }
    // Months and years: the whole part moves the calendar date (a day past
    // the end of the month rolls over), and the fraction is then added as
    // 30-day months or 365-day years, exactly as SQLite does.
    let (jd, frac) = match unit {
        "month" | "year" => {
            if !(0..=MAX_JD_MS).contains(&jd) {
                return Ok(None);
            }
            let p = Parts::from_jd(jd);
            let k = n.trunc() as i64;
            let p = if unit == "month" {
                Parts { m: p.m + k, ..p }
            } else {
                Parts { y: p.y + k, ..p }
            };
            (p.to_jd(), n - n.trunc())
        }
        _ => (jd, n),
    };
    // Round half away from zero, as SQLite's `+ rRounder` truncation does.
    let add = (frac * 1000.0 * secs).round();
    Ok(jd.checked_add(add as i64))
}

/// Evaluate a date function's arguments (time string, then modifiers) to a
/// moment; `None` for NULL. No arguments means now.
fn moment(args: &[Register]) -> SqawkResult<Option<i64>> {
    let Some(first) = args.first() else {
        return Ok(Some(now_jd()));
    };
    let mods = &args[1..];
    let unixepoch =
        matches!(mods.first(), Some(Register::String(s)) if s.eq_ignore_ascii_case("unixepoch"));
    let Some(mut jd) = initial(first, unixepoch) else {
        return Ok(None);
    };
    for (i, m) in mods.iter().enumerate() {
        let Register::String(m) = m else {
            return Ok(None);
        };
        if i == 0 && unixepoch {
            continue;
        }
        match modify(jd, m)? {
            Some(v) => jd = v,
            None => return Ok(None),
        }
    }
    Ok((0..=MAX_JD_MS).contains(&jd).then_some(jd))
}

fn fmt_date(p: &Parts) -> String {
    format!("{:04}-{:02}-{:02}", p.y, p.m, p.d)
}

fn fmt_time(p: &Parts) -> String {
    format!("{:02}:{:02}:{:02}", p.h, p.mi, p.ms / 1000)
}

fn text_or_null(v: Option<String>) -> Register {
    v.map_or(Register::Null, Register::String)
}

/// `DATE(timestring, modifier...)`.
pub(crate) fn date(args: &[Register]) -> SqawkResult<Register> {
    Ok(text_or_null(
        moment(args)?.map(|jd| fmt_date(&Parts::from_jd(jd))),
    ))
}

/// `TIME(timestring, modifier...)`.
pub(crate) fn time(args: &[Register]) -> SqawkResult<Register> {
    Ok(text_or_null(
        moment(args)?.map(|jd| fmt_time(&Parts::from_jd(jd))),
    ))
}

/// `DATETIME(timestring, modifier...)`.
pub(crate) fn datetime(args: &[Register]) -> SqawkResult<Register> {
    Ok(text_or_null(moment(args)?.map(|jd| {
        let p = Parts::from_jd(jd);
        format!("{} {}", fmt_date(&p), fmt_time(&p))
    })))
}

/// `JULIANDAY(timestring, modifier...)`: a real.
pub(crate) fn julianday(args: &[Register]) -> SqawkResult<Register> {
    Ok(moment(args)?.map_or(Register::Null, |jd| {
        Register::Float(jd as f64 / MS_PER_DAY as f64)
    }))
}

/// `UNIXEPOCH(timestring, modifier...)`: whole seconds.
pub(crate) fn unixepoch(args: &[Register]) -> SqawkResult<Register> {
    Ok(moment(args)?.map_or(Register::Null, |jd| {
        Register::Integer((jd - UNIX_EPOCH_JD_MS).div_euclid(1000))
    }))
}

/// `STRFTIME(format, timestring, modifier...)`, with SQLite 3.45's
/// conversions: `%d %f %H %j %J %m %M %s %S %w %W %Y %%`. Any other
/// conversion makes the result NULL, as in SQLite.
pub(crate) fn strftime(args: &[Register]) -> SqawkResult<Register> {
    let fmt = match args.first() {
        Some(Register::Null) | None => return Ok(Register::Null),
        Some(r) => super::arith::text_of(r),
    };
    let Some(jd) = moment(&args[1..])? else {
        return Ok(Register::Null);
    };
    let p = Parts::from_jd(jd);
    let day = (jd - UNIX_EPOCH_JD_MS).div_euclid(MS_PER_DAY);
    let yday = day - days_from_civil(p.y, 1, 1); // 0-based
    let wday = (day + 4).rem_euclid(7); // 0 = Sunday
    let mut out = String::with_capacity(fmt.len() + 8);
    let mut chars = fmt.chars();
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        let piece = match chars.next() {
            Some('d') => format!("{:02}", p.d),
            Some('f') => format!("{:02}.{:03}", p.ms / 1000, p.ms % 1000),
            Some('H') => format!("{:02}", p.h),
            Some('j') => format!("{:03}", yday + 1),
            Some('J') => format!("{}", jd as f64 / MS_PER_DAY as f64),
            Some('m') => format!("{:02}", p.m),
            Some('M') => format!("{:02}", p.mi),
            Some('s') => format!("{}", (jd - UNIX_EPOCH_JD_MS).div_euclid(1000)),
            Some('S') => format!("{:02}", p.ms / 1000),
            Some('w') => format!("{}", wday),
            Some('W') => format!("{:02}", (yday + 7 - (wday + 6) % 7) / 7),
            Some('Y') => format!("{:04}", p.y),
            Some('%') => "%".to_string(),
            _ => return Ok(Register::Null),
        };
        out.push_str(&piece);
    }
    Ok(Register::String(out))
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
    fn civil_roundtrip() {
        for days in [-800_000i64, -719_468, -1, 0, 1, 19_737, 2_932_896] {
            let (y, m, d) = civil_from_days(days);
            assert_eq!(days_from_civil(y, m, d), days);
        }
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(19_737), (2024, 1, 15));
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
            txt(strftime(&[s("%j %w %s %%"), s("2024-01-15")])).as_deref(),
            Some("015 1 1705276800 %")
        );
        assert_eq!(
            txt(date(&[s("2024-01-15 10:30:00+02:00")])).as_deref(),
            Some("2024-01-15")
        );
    }
}
