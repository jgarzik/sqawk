//! Regular expressions: the `REGEXP` operator (as the sqlite3 shell
//! defines it) and DuckDB's `regexp_matches`, `regexp_replace` and
//! `regexp_extract`, on the `regex` crate's syntax.
//!
//! Every function searches (a match may start anywhere; anchor with `^` /
//! `$`), gives NULL when an argument is NULL, and reports an invalid
//! pattern as an error. Compiled patterns are cached per thread, so a
//! constant pattern is compiled once per query, not once per row.

use std::cell::RefCell;
use std::collections::HashMap;

use regex::{Captures, Regex, RegexBuilder};

use super::arith::{int_value, text_of};
use super::bytecode::Register;
use crate::error::{SqawkError, SqawkResult};

/// Patterns kept compiled; the cache is emptied when it fills.
const CACHE_SIZE: usize = 64;

thread_local! {
    static CACHE: RefCell<HashMap<(String, bool), Regex>> = RefCell::new(HashMap::new());
}

fn error(msg: String) -> SqawkError {
    SqawkError::InvalidSqlQuery(msg)
}

/// `pattern` compiled (case-insensitively under `nocase`), from the cache
/// when it is there.
fn compile(pattern: &str, nocase: bool) -> SqawkResult<Regex> {
    let key = (pattern.to_string(), nocase);
    if let Some(re) = CACHE.with(|c| c.borrow().get(&key).cloned()) {
        return Ok(re);
    }
    let re = RegexBuilder::new(pattern)
        .case_insensitive(nocase)
        .build()
        .map_err(|e| {
            // The crate's message spans lines (the pattern, a caret, then
            // `error: ...`); keep the last.
            let text = e.to_string();
            let why = text.lines().last().unwrap_or("").trim();
            let why = why.strip_prefix("error: ").unwrap_or(why);
            error(format!("invalid regular expression '{pattern}': {why}"))
        })?;
    CACHE.with(|c| {
        let mut c = c.borrow_mut();
        if c.len() >= CACHE_SIZE {
            c.clear();
        }
        c.insert(key, re.clone());
    });
    Ok(re)
}

fn any_null(a: &[Register]) -> bool {
    a.iter().any(|r| matches!(r, Register::Null))
}

/// REGEXP(pattern, x), i.e. `x REGEXP pattern`: 1 if the pattern matches
/// somewhere in x, else 0.
pub(crate) fn regexp(a: &[Register]) -> SqawkResult<Register> {
    if any_null(a) {
        return Ok(Register::Null);
    }
    let re = compile(&text_of(&a[0]), false)?;
    Ok(Register::Integer(re.is_match(&text_of(&a[1])) as i64))
}

/// REGEXP_MATCHES(x, pattern): `x REGEXP pattern` with the arguments the
/// other way round.
pub(crate) fn regexp_matches(a: &[Register]) -> SqawkResult<Register> {
    regexp(&[a[1].clone(), a[0].clone()])
}

/// REGEXP_REPLACE(x, pattern, replacement[, flags]): x with the first match
/// replaced, every match under flag `g`; flag `i` ignores case. In the
/// replacement `\1` ... `\9` (and `\0`, the whole match) insert a group
/// (empty when it did not take part) and `\\` is a backslash.
pub(crate) fn regexp_replace(a: &[Register]) -> SqawkResult<Register> {
    if any_null(a) {
        return Ok(Register::Null);
    }
    let (mut global, mut nocase) = (false, false);
    if let Some(flags) = a.get(3) {
        for f in text_of(flags).chars() {
            match f {
                'g' => global = true,
                'i' => nocase = true,
                other => return Err(error(format!("regexp_replace: unknown flag '{other}'"))),
            }
        }
    }
    let re = compile(&text_of(&a[1]), nocase)?;
    let text = text_of(&a[0]);
    let rep = text_of(&a[2]);
    let expand = |caps: &Captures| {
        let mut out = String::new();
        let mut chars = rep.chars().peekable();
        while let Some(c) = chars.next() {
            if c != '\\' {
                out.push(c);
                continue;
            }
            match chars.peek().copied() {
                Some(d @ '0'..='9') => {
                    chars.next();
                    let group = d as usize - '0' as usize;
                    out.push_str(caps.get(group).map_or("", |m| m.as_str()));
                }
                Some('\\') => {
                    chars.next();
                    out.push('\\');
                }
                _ => out.push('\\'),
            }
        }
        out
    };
    let limit = if global { 0 } else { 1 };
    Ok(Register::String(
        re.replacen(&text, limit, |caps: &Captures| expand(caps))
            .into_owned()
            .into(),
    ))
}

/// REGEXP_EXTRACT(x, pattern[, group]): the first match's group `group`
/// (default 0, the whole match); empty text when nothing matches or the
/// group did not take part. A group the pattern does not have is an
/// error.
pub(crate) fn regexp_extract(a: &[Register]) -> SqawkResult<Register> {
    if any_null(a) {
        return Ok(Register::Null);
    }
    let pattern = text_of(&a[1]);
    let re = compile(&pattern, false)?;
    let group = a.get(2).map_or(0, int_value);
    let groups = re.captures_len() - 1;
    if group < 0 || group as usize > groups {
        return Err(error(format!(
            "regexp_extract: group {group} out of range (the pattern has {groups})"
        )));
    }
    let text = text_of(&a[0]);
    let found = re
        .captures(&text)
        .and_then(|c| c.get(group as usize))
        .map_or("", |m| m.as_str());
    Ok(Register::String(found.into()))
}
