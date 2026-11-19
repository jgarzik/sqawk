//! GLOB pattern matching: a port of SQLite's `patternCompare` (func.c)
//! with GLOB's settings.
//!
//! `*` matches any run of characters (including none), `?` exactly one,
//! and `[...]` one character of a set: `[^...]` is its complement, `a-z` a
//! range, a `]` first in the set (after any `^`) is literal, and so is a
//! `-` first or last. A set that is never closed matches nothing. Matching
//! is per character and case-sensitive. As in SQLite, which reads both
//! sides as C strings, a NUL character ends the text and the pattern.

/// How a (sub)match ended.
#[derive(PartialEq, Eq, Clone, Copy)]
enum Outcome {
    Match,
    NoMatch,
    /// No match, and none is possible by consuming more of the text after a
    /// `*` (lets a `*` stop searching early, as in SQLite).
    NoWildcardMatch,
}

/// The character at byte offset `i` of `s` and the offset after it; `'\0'`
/// at the end (or at a NUL).
fn read(s: &str, i: usize) -> (char, usize) {
    match s[i..].chars().next() {
        Some(c) => (c, i + c.len_utf8()),
        None => ('\0', i),
    }
}

/// Whether `text` matches the GLOB `pattern`.
pub(crate) fn glob(pattern: &str, text: &str) -> bool {
    compare(pattern, 0, text, 0) == Outcome::Match
}

fn compare(pat: &str, mut p: usize, s: &str, mut t: usize) -> Outcome {
    loop {
        let (c, np) = read(pat, p);
        p = np;
        if c == '\0' {
            break;
        }
        if c == '*' {
            // Skip further `*`, and `?` too, consuming one character of
            // text for each `?`.
            let mut c;
            loop {
                let (c2, np) = read(pat, p);
                p = np;
                c = c2;
                if c == '*' {
                    continue;
                }
                if c == '?' {
                    let (tc, nt) = read(s, t);
                    if tc == '\0' {
                        return Outcome::NoWildcardMatch;
                    }
                    t = nt;
                    continue;
                }
                break;
            }
            if c == '\0' {
                // A `*` at the end of the pattern matches the rest.
                return Outcome::Match;
            }
            if c == '[' {
                // A set right after the `*`: try it at every position.
                let set = p - 1;
                loop {
                    let (tc, nt) = read(s, t);
                    if tc == '\0' {
                        return Outcome::NoWildcardMatch;
                    }
                    let m = compare(pat, set, s, t);
                    if m != Outcome::NoMatch {
                        return m;
                    }
                    t = nt;
                }
            }
            // Find each occurrence of `c` in the text and match the rest of
            // the pattern after it.
            loop {
                let (tc, nt) = read(s, t);
                if tc == '\0' {
                    return Outcome::NoWildcardMatch;
                }
                t = nt;
                if tc != c {
                    continue;
                }
                let m = compare(pat, p, s, t);
                if m != Outcome::NoMatch {
                    return m;
                }
            }
        }
        if c == '[' {
            let (tc, nt) = read(s, t);
            if tc == '\0' {
                return Outcome::NoMatch;
            }
            t = nt;
            let mut seen = false;
            let mut invert = false;
            let mut prior: Option<char> = None;
            let (mut c2, mut np) = read(pat, p);
            p = np;
            if c2 == '^' {
                invert = true;
                (c2, np) = read(pat, p);
                p = np;
            }
            if c2 == ']' {
                if tc == ']' {
                    seen = true;
                }
                (c2, np) = read(pat, p);
                p = np;
            }
            while c2 != '\0' && c2 != ']' {
                let next = read(pat, p).0;
                match prior {
                    Some(lo) if c2 == '-' && next != ']' && next != '\0' => {
                        let (hi, np2) = read(pat, p);
                        p = np2;
                        if tc >= lo && tc <= hi {
                            seen = true;
                        }
                        prior = None;
                    }
                    _ => {
                        if tc == c2 {
                            seen = true;
                        }
                        prior = Some(c2);
                    }
                }
                (c2, np) = read(pat, p);
                p = np;
            }
            if c2 == '\0' || seen == invert {
                return Outcome::NoMatch;
            }
            continue;
        }
        let (tc, nt) = read(s, t);
        t = nt;
        if c == tc || (c == '?' && tc != '\0') {
            continue;
        }
        return Outcome::NoMatch;
    }
    if read(s, t).0 == '\0' {
        Outcome::Match
    } else {
        Outcome::NoMatch
    }
}

#[cfg(test)]
mod tests {
    use super::glob;

    #[test]
    fn sqlite_cases() {
        for (p, s, want) in [
            ("a*", "abc", true),
            ("A*", "abc", false),
            ("?b?", "abc", true),
            ("a?", "abc", false),
            ("*.c", "a.c", true),
            ("[a-c]", "b", true),
            ("[a-c]", "d", false),
            ("[^a-c]", "d", true),
            ("[]]", "]", true),
            ("[a-]", "-", true),
            ("[-a]", "-", true),
            ("[abc", "x", false),
            ("[abc", "a", false),
            ("*", "", true),
            ("?", "", false),
            ("h?llo", "h\u{e9}llo", true),
            ("*X*X*", "aXbXc", true),
            ("*[b]", "ab", true),
            ("*[b]", "abc", false),
            ("a[*]b", "a*b", true),
            ("*?", "", false),
            ("**?*", "x", true),
            ("a", "a\0b", true),
            ("a\0x", "a", true),
            ("*a*a*a*a*a*a*a*a*b", &"a".repeat(200), false),
        ] {
            assert_eq!(glob(p, s), want, "{s:?} GLOB {p:?}");
        }
    }
}
