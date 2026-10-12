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
pub fn same_rows(ordered: bool, a: &[String], b: &[String]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    if ordered {
        return a == b;
    }
    let mut x: Vec<&String> = a.iter().collect();
    let mut y: Vec<&String> = b.iter().collect();
    x.sort();
    y.sort();
    x == y
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

    #[test]
    fn records_split_outside_quotes() {
        let r = raw_records("h\r\n\"a\nb\",1\nlast");
        assert_eq!(r, vec!["h", "\"a\nb\",1", "last"]);
    }
}
