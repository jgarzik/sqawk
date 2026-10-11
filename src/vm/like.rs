//! LIKE pattern matching, without regular expressions.
//!
//! `%` matches any run of characters (including none) and `_` exactly one
//! character -- newlines included. An ESCAPE character makes the character
//! after it literal. Matching is per character, not per byte, and for LIKE
//! is ASCII case-insensitive (SQLite's default); ILIKE folds Unicode case.
//!
//! A pattern is compiled once into tokens; the engine caches the compiled
//! form per instruction, so a constant pattern is never recompiled per row.

/// One element of a compiled pattern.
#[derive(Debug, Clone, PartialEq)]
enum Tok {
    /// A literal character (already case-folded for case-insensitive modes).
    Lit(char),
    /// `_`: any one character.
    One,
    /// `%`: any run of characters. Consecutive `%` collapse into one.
    Any,
}

/// How letters compare.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Case {
    /// ASCII letters only (LIKE).
    Ascii,
    /// Unicode simple case folding (ILIKE).
    Unicode,
}

impl Case {
    fn fold(self, c: char) -> char {
        match self {
            Case::Ascii => c.to_ascii_lowercase(),
            Case::Unicode => c.to_lowercase().next().unwrap_or(c),
        }
    }
}

/// A compiled LIKE pattern.
#[derive(Debug, Clone)]
pub(crate) struct Pattern {
    toks: Vec<Tok>,
    case: Case,
    /// Set for a pattern ending in a lone escape character, which SQLite
    /// treats as matching nothing.
    never: bool,
}

impl Pattern {
    /// Compile `pattern`, with `escape` as the escape character if any.
    pub(crate) fn new(pattern: &str, escape: Option<char>, case: Case) -> Pattern {
        let mut toks = Vec::with_capacity(pattern.len());
        let mut never = false;
        let mut chars = pattern.chars();
        while let Some(c) = chars.next() {
            let tok = if Some(c) == escape {
                match chars.next() {
                    Some(lit) => Tok::Lit(case.fold(lit)),
                    None => {
                        never = true;
                        break;
                    }
                }
            } else if c == '%' {
                if toks.last() == Some(&Tok::Any) {
                    continue;
                }
                Tok::Any
            } else if c == '_' {
                Tok::One
            } else {
                Tok::Lit(case.fold(c))
            };
            toks.push(tok);
        }
        Pattern { toks, case, never }
    }

    /// Whether `text` matches.
    ///
    /// The classic greedy wildcard algorithm: on a mismatch, resume after the
    /// most recent `%` with that `%` absorbing one more character. Linear for
    /// patterns with at most one `%`, and never exponential.
    pub(crate) fn matches(&self, text: &str) -> bool {
        if self.never {
            return false;
        }
        let toks = &self.toks;
        let case = self.case;
        let mut ti = 0usize; // token index
        let mut pos = 0usize; // byte offset in text
        let mut star: Option<(usize, usize)> = None; // (token after %, text pos)
        loop {
            let next = text[pos..].chars().next();
            match (toks.get(ti), next) {
                (Some(Tok::Any), _) => {
                    ti += 1;
                    star = Some((ti, pos));
                    continue;
                }
                (Some(Tok::One), Some(c)) => {
                    ti += 1;
                    pos += c.len_utf8();
                    continue;
                }
                (Some(Tok::Lit(l)), Some(c)) if *l == case.fold(c) => {
                    ti += 1;
                    pos += c.len_utf8();
                    continue;
                }
                (None, None) => return true,
                _ => {}
            }
            // Mismatch: let the last % absorb one more character.
            match star {
                Some((st, sp)) => match text[sp..].chars().next() {
                    Some(c) => {
                        let np = sp + c.len_utf8();
                        star = Some((st, np));
                        ti = st;
                        pos = np;
                    }
                    None => return false,
                },
                None => return false,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn like(t: &str, p: &str) -> bool {
        Pattern::new(p, None, Case::Ascii).matches(t)
    }

    #[test]
    fn basics() {
        assert!(like("Alice", "a%"));
        assert!(like("Alice", "%CE"));
        assert!(!like("Alice", "b%"));
        assert!(like("a\nb", "a_b"));
        assert!(like("a\nb", "a%b"));
        assert!(like("日本語", "_本_"));
        assert!(like("", "%"));
        assert!(!like("", "_"));
        assert!(like("abcabc", "%bc"));
        assert!(like("mississippi", "%iss%ppi"));
        assert!(!like("mississippi", "%iss%ppx"));
        assert!(!like("é", "É"));
        assert!(Pattern::new("É", None, Case::Unicode).matches("é"));
        assert!(!like("Abc", "abc_"));
    }

    #[test]
    fn escapes() {
        let p = Pattern::new("x\\%y", Some('\\'), Case::Ascii);
        assert!(p.matches("x%y"));
        assert!(!p.matches("xzy"));
        let p = Pattern::new("%\\_%", Some('\\'), Case::Ascii);
        assert!(p.matches("A_b"));
        assert!(!p.matches("Ab"));
        let p = Pattern::new("ab\\", Some('\\'), Case::Ascii);
        assert!(!p.matches("ab"));
        assert!(!p.matches("ab\\"));
    }
}
