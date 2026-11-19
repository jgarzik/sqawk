//! Records as sqawk prints them, and sqawk's stdout cut back into records.
//!
//! sqawk's writer (src/table.rs `RecordWriter`) quotes a field when it holds
//! a comma, quote, CR or LF or has leading or trailing space or tab, doubles
//! quotes, and prints a record of one empty field as `""`. The oracle builds
//! SQLite's records with the same rules so the raw text can be compared
//! byte for byte, quoting included.

pub fn needs_quotes(s: &str) -> bool {
    !s.is_empty()
        && (s.contains([',', '"', '\r', '\n'])
            || s.starts_with([' ', '\t'])
            || s.ends_with([' ', '\t']))
}

/// One output record (no terminator).
pub fn record<S: AsRef<str>>(fields: &[S]) -> String {
    if fields.len() == 1 && fields[0].as_ref().is_empty() {
        return "\"\"".to_string();
    }
    let mut out = String::new();
    for (i, f) in fields.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        let f = f.as_ref();
        if needs_quotes(f) {
            out.push('"');
            out.push_str(&f.replace('"', "\"\""));
            out.push('"');
        } else {
            out.push_str(f);
        }
    }
    out
}

/// Split CSV text into raw records at LF (or CRLF) outside quotes.
pub fn raw_records(text: &str) -> Vec<String> {
    let mut recs = Vec::new();
    let mut cur = String::new();
    let mut quoted = false;
    for ch in text.chars() {
        if ch == '"' {
            quoted = !quoted;
        }
        if ch == '\n' && !quoted {
            if cur.ends_with('\r') {
                cur.pop();
            }
            recs.push(std::mem::take(&mut cur));
        } else {
            cur.push(ch);
        }
    }
    if !cur.is_empty() {
        recs.push(cur);
    }
    recs
}

/// One result set: the header record and the data records.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResultSet {
    pub header: String,
    pub rows: Vec<String>,
    /// Index of the statement that produced it.
    pub source: usize,
}

/// Cut sqawk's records into result sets shaped like `like` (each set is a
/// header record followed by its rows). None when the record count does not
/// fit.
pub fn split_sets(recs: &[String], like: &[ResultSet]) -> Option<Vec<ResultSet>> {
    let want: usize = like.iter().map(|s| 1 + s.rows.len()).sum();
    if recs.len() != want {
        return None;
    }
    let mut out = Vec::new();
    let mut i = 0;
    for s in like {
        out.push(ResultSet {
            header: recs[i].clone(),
            rows: recs[i + 1..i + 1 + s.rows.len()].to_vec(),
            source: s.source,
        });
        i += 1 + s.rows.len();
    }
    Some(out)
}

/// Equal as sequences when `ordered`, else as multisets.
///
/// An ordered comparison still lets rows that tie on every column swap
/// places. The engines sort ties in the order their join loops produced
/// them, which may differ. A tie can be told apart only when equal numbers
/// render differently: the integer `-9223372036854775808` and the real
/// `-9.22337203685478e+18` compare equal. Each run of tied rows must still
/// match exactly as a multiset, so a rendering difference is still caught.
pub fn same_rows(ordered: bool, a: &[String], b: &[String]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    if !ordered {
        return same_multiset(a, b);
    }
    let mut i = 0;
    while i < a.len() {
        if a[i] == b[i] {
            i += 1;
            continue;
        }
        let head = fields(&a[i]);
        let mut j = i + 1;
        while j < a.len() && ties(&head, &fields(&a[j])) && ties(&head, &fields(&b[j])) {
            j += 1;
        }
        if !ties(&head, &fields(&b[i])) || !same_multiset(&a[i..j], &b[i..j]) {
            return false;
        }
        i = j;
    }
    true
}

fn same_multiset(a: &[String], b: &[String]) -> bool {
    let mut x: Vec<&String> = a.iter().collect();
    let mut y: Vec<&String> = b.iter().collect();
    x.sort();
    y.sort();
    x == y
}

/// Rows that compare equal on every column: equal text, or numbers equal
/// up to the 15 significant digits a real is printed with.
fn ties(a: &[String], b: &[String]) -> bool {
    a.len() == b.len()
        && a.iter().zip(b).all(|(x, y)| {
            x == y
                || match (x.parse::<f64>(), y.parse::<f64>()) {
                    (Ok(p), Ok(q)) => p == q || (p - q).abs() <= 1e-14 * p.abs().max(q.abs()),
                    _ => false,
                }
        })
}

/// The fields of one RFC 4180 record (as written by `record`).
fn fields(rec: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut quoted = false;
    let mut chars = rec.chars().peekable();
    while let Some(c) = chars.next() {
        match (quoted, c) {
            (true, '"') if chars.peek() == Some(&'"') => {
                cur.push('"');
                chars.next();
            }
            (true, '"') => quoted = false,
            (false, '"') => quoted = true,
            (false, ',') => out.push(std::mem::take(&mut cur)),
            (_, c) => cur.push(c),
        }
    }
    out.push(cur);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quoting_rules() {
        assert_eq!(
            record(&["a", "b,c", " x", "q\""]),
            "a,\"b,c\",\" x\",\"q\"\"\""
        );
        assert_eq!(record(&[""]), "\"\"");
        assert_eq!(record(&["", ""]), ",");
    }

    fn rows(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn ordered_rows_may_swap_ties_only() {
        // Equal int and real keys may come out in either order.
        let a = rows(&["0,-9.22337203685478e+18", "0,-9223372036854775808", "1,5"]);
        let b = rows(&["0,-9223372036854775808", "0,-9.22337203685478e+18", "1,5"]);
        assert!(same_rows(true, &a, &b));
        // Rows that do not tie must stay in order.
        let c = rows(&["1,5", "0,-9223372036854775808", "0,-9.22337203685478e+18"]);
        assert!(!same_rows(true, &a, &c));
        // A tie run must still match exactly: `1` printed as `1.0` is caught.
        let d = rows(&["0,-9.22337203685478e+18", "0,-9.22337203685478e+18", "1,5"]);
        assert!(!same_rows(true, &a, &d));
        assert!(!same_rows(true, &rows(&["1"]), &rows(&["1.0"])));
    }

    #[test]
    fn fields_follow_quoting() {
        assert_eq!(fields("a,\"b,c\",\"q\"\"\""), vec!["a", "b,c", "q\""]);
        assert_eq!(fields(""), vec![""]);
    }

    #[test]
    fn records_split_outside_quotes() {
        let r = raw_records("h\r\n\"a\nb\",1\nlast");
        assert_eq!(r, vec!["h", "\"a\nb\",1", "last"]);
    }
}
