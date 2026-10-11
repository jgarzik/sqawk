#!/usr/bin/env python3
"""Differential tester: sqawk vs SQLite (python3's stdlib sqlite3 module).

Usage (from anywhere; paths resolve from this file's location):

    cargo build --release
    python3 -I tests/sqlite_diff/harness.py tests/sqlite_diff/queries.sql tests/sqlite_diff/queries2.sql

SQAWK overrides the binary (default: <repo>/target/release/sqawk).  Not
part of `cargo test`.  The fixed tables below are written as CSV files into
a temporary directory, removed at exit.

Query files: one test per line; blank lines and lines starting with `--`
are skipped.  A line prefixed `O:` is compared in order; any other line is
compared as a multiset of rows.  A line may hold several `;`-separated
statements (DML before a SELECT): both engines run all of them, every
result set is compared, and SQLite rolls its database back afterwards.

Like-for-like setup
-------------------
* Values.  sqawk types each loaded cell by itself (src/table.rs
  `infer_value`): an empty field is NULL; canonical integer text (what i64
  formatting prints back, so not `007`, `-0`, `+3`) is INTEGER; text that
  equals SQLite's CAST(x AS TEXT) rendering of its double (`1.5`, `2.0`,
  `1.0e+20`, but not `1.50`, `1e3`, `.5`) is REAL; everything else is TEXT,
  verbatim.  `cell_value` below mirrors that rule and each cell is inserted
  into SQLite as the resulting Python value (None / int / float / str).
* Columns are declared with no type, which gives them no affinity (BLOB
  affinity in SQLite's terms): SQLite stores exactly the value inserted, so
  one column can hold integers, reals and text per cell, as in sqawk.
  Declaring INTEGER/REAL/TEXT would make SQLite convert values (`'007'`
  stored as 7 in an INTEGER column) and the two engines would hold
  different data.
* Output.  SQLite results are rendered as sqawk prints them: NULL as an
  empty field, integers in decimal, reals via SQLite's own CAST(x AS TEXT)
  (`1.0`, `0.3`, `1.0e+20`, `Inf`), text verbatim; then records are built
  with sqawk's RFC 4180 rules (src/table.rs `RecordWriter`): a field is
  quoted when it holds a comma, quote, CR or LF or has leading/trailing
  space or tab, quotes are doubled, and a record of one empty field prints
  as `""`.  The raw record text is compared, so quoting is checked too.
  sqawk's header line before each result set is skipped (column names are
  not compared).

Limitation: NULL and the empty string both print as an empty field in
sqawk, so the comparison cannot tell them apart.

Output: one block per non-passing test (DIFF shows the first rows of each
side, sorted for unordered tests), then
    SUMMARY {'pass': N, 'DIFF': N, 'sqawk_err': N, ...}
pass = identical output; DIFF = both engines ran but printed different
rows; sqawk_err = sqawk exited non-zero (unsupported syntax or a rejected
query) where SQLite ran it; both_err = both engines rejected the query
(counted, not printed; an agreeing error is a pass in substance);
sqlite_err = SQLite rejected a query sqawk ran (sqawk extensions such as
`TRIM(... FROM ...)`); panic / hang = sqawk crashed or exceeded 10 s.  A clean run has
only pass, plus DIFF / sqawk_err entries that are documented differences
from SQLite (doc/sql_reference.md).
"""
import collections, csv, os, re, sqlite3, subprocess, sys, tempfile, time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
SQAWK = os.environ.get("SQAWK", os.path.join(REPO, "target", "release", "sqawk"))

TABLES = {
    "emp": (["id", "name", "dept", "salary", "mgr", "bonus"], [
        [1, "Alice", "Eng", 100, None, 10.5],
        [2, "Bob", "Eng", 80, 1, None],
        [3, "Carol", "Sales", 70, 1, 5],
        [4, "Dave", "Sales", 70, 3, -2.25],
        [5, "Eve", None, 50, 3, 0],
        [6, "Frank", "HR", None, 1, None],
        [7, "alice", "Eng", 100, 2, 3],
        [8, "Grace", "Ops", -10, None, 1.5],
        [9, "Heidi", "HR", 60, 6, None],
        [10, "Bob", "Eng", 80, 1, 2],
    ]),
    "dept": (["dept", "dname", "budget"], [
        ["Eng", "Engineering", 1000],
        ["Sales", "Sales Team", 500.5],
        ["HR", "Human Res", None],
        ["Mkt", "Marketing", 200],
    ]),
    "t1": (["k", "a", "s"], [
        [1, 10, "x"], [2, None, "y"], [3, 30, None], [3, 30, None], [4, -5, "x%y"], [5, 0, "A_b"],
    ]),
    "t2": (["k", "b"], [
        [1, 100], [3, 300], [3, 301], [6, 600], [None, 999],
    ]),
    "t3": (["k", "c"], [
        [1, "one"], [6, "six"], [7, "seven"],
    ]),
    "e": (["x"], []),   # empty table (header only)
    "uni": (["id", "w"], [
        [1, "héllo"], [2, "日本語"], [3, "naïve café"], [4, "  pad  "], [5, ""], [6, "ABC"],
    ]),
}

# ---------------------------------------------------------------- values
_RDB = sqlite3.connect(":memory:")


def real_text(f):
    """SQLite's CAST(x AS TEXT) of a real -- exactly what sqawk prints."""
    return _RDB.execute("SELECT CAST(? AS TEXT)", (f,)).fetchone()[0] or ""


def field_text(v):
    """A Python value as sqawk prints it (NULL -> empty field)."""
    if v is None:
        return ""
    if isinstance(v, bool):
        return str(int(v))
    if isinstance(v, float):
        return real_text(v)
    if isinstance(v, bytes):
        return "<blob %s>" % v.hex()
    return str(v)


_INT = re.compile(r"-?(0|[1-9][0-9]*)")


def cell_value(text):
    """The value sqawk loads from field `text` (src/table.rs infer_value)."""
    if text == "":
        return None
    if _INT.fullmatch(text) and text != "-0":
        v = int(text)
        if -2**63 <= v < 2**63:
            return v
        return text
    if "." in text and re.fullmatch(r"[-+.e0-9]+", text):
        try:
            f = float(text)
        except ValueError:
            return text
        if f == f and f not in (float("inf"), float("-inf")) and real_text(f) == text:
            return f
    return text


# ---------------------------------------------------------------- records
def needs_quotes(s):
    return s != "" and (any(c in s for c in ',"\r\n') or s[0] in " \t" or s[-1] in " \t")


def record(fields):
    """One output record, as sqawk's RecordWriter writes it (no terminator)."""
    out = ['"%s"' % f.replace('"', '""') if needs_quotes(f) else f for f in fields]
    if len(out) == 1 and out[0] == "":
        return '""'
    return ",".join(out)


def raw_records(text):
    """Split CSV text into raw record strings (LF/CRLF outside quotes)."""
    recs, cur, q = [], [], False
    for ch in text:
        if ch == '"':
            q = not q
        if ch == "\n" and not q:
            r = "".join(cur)
            recs.append(r[:-1] if r.endswith("\r") else r)
            cur = []
        else:
            cur.append(ch)
    if cur:
        recs.append("".join(cur))
    return recs


# ---------------------------------------------------------------- engines
def write_data(data_dir):
    for t, (cols, rows) in TABLES.items():
        with open(os.path.join(data_dir, t + ".csv"), "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(cols)
            for r in rows:
                w.writerow([field_text(x) for x in r])


def sqlite_db():
    db = sqlite3.connect(":memory:")
    db.isolation_level = None
    # printf('%.1s', '日本') cuts inside a UTF-8 sequence in both engines;
    # decode as sqawk's output is decoded instead of raising.
    db.text_factory = lambda b: b.decode("utf-8", "replace")
    for t, (cols, rows) in TABLES.items():
        db.execute("CREATE TABLE %s (%s)" % (t, ",".join('"%s"' % c for c in cols)))
        db.executemany("INSERT INTO %s VALUES (%s)" % (t, ",".join("?" * len(cols))),
                       [[cell_value(field_text(x)) for x in r] for r in rows])
    return db


def split_stmts(sql):
    out, cur, q = [], "", None
    for ch in sql:
        if q:
            if ch == q:
                q = None
        elif ch in "'\"":
            q = ch
        if ch == ";" and not q:
            if cur.strip():
                out.append(cur)
            cur = ""
        else:
            cur += ch
    if cur.strip():
        out.append(cur)
    return out


def run_sqlite(db, sql):
    """('ok', [result set, ...]) with each result set a list of records."""
    db.execute("SAVEPOINT sp")
    try:
        sets = []
        for stmt in split_stmts(sql):
            cur = db.execute(stmt)
            if cur.description:
                sets.append([record([field_text(v) for v in row]) for row in cur.fetchall()])
        return ("ok", sets)
    except Exception as ex:
        return ("err", str(ex))
    finally:
        db.execute("ROLLBACK TO sp")
        db.execute("RELEASE sp")


def tables_in(sql):
    return [t for t in TABLES if re.search(r"\b%s\b" % t, sql, re.IGNORECASE)]


def run_sqawk(data_dir, sql):
    """('ok', [raw record, ...]) of stdout, headers included."""
    files = [os.path.join(data_dir, t + ".csv") for t in tables_in(sql)] or [os.path.join(data_dir, "t1.csv")]
    t0 = time.time()
    try:
        p = subprocess.run([SQAWK, "-s", sql, *files], capture_output=True, timeout=10)
    except subprocess.TimeoutExpired:
        return ("hang", "timeout", 10)
    dt = time.time() - t0
    err = p.stderr.decode("utf-8", "replace").strip()
    if p.returncode != 0 or "panicked" in err:
        kind = "panic" if ("panicked" in err or p.returncode >= 100 or p.returncode < 0) else "err"
        return (kind, err.splitlines()[-1] if err else "rc=%d" % p.returncode, dt)
    return ("ok", raw_records(p.stdout.decode("utf-8", "replace")), dt)


def split_sets(recs, sets):
    """Cut sqawk's records into result sets shaped like SQLite's, dropping
    each header line.  None when the record count does not match."""
    if len(recs) != sum(1 + len(s) for s in sets):
        return None
    out, i = [], 0
    for s in sets:
        out.append(recs[i + 1:i + 1 + len(s)])
        i += 1 + len(s)
    return out


def same(ordered, a, b):
    if ordered:
        return a == b
    return collections.Counter(a) == collections.Counter(b)


def show(ordered, rows):
    return rows[:12] if ordered else sorted(rows)[:12]


def main():
    files = sys.argv[1:] or [os.path.join(HERE, "queries.sql")]
    stats = collections.Counter()
    with tempfile.TemporaryDirectory(prefix="sqawk-diff-") as data_dir:
        write_data(data_dir)
        db = sqlite_db()
        for qf in files:
            for line in open(qf, encoding="utf-8"):
                q = line.strip()
                if not q or q.startswith("--"):
                    continue
                ordered = q.startswith("O:")
                if ordered:
                    q = q[2:].strip()
                sk = run_sqawk(data_dir, q)
                sl = run_sqlite(db, q)
                if sk[0] in ("hang", "panic"):
                    stats[sk[0]] += 1
                    print("%s: %s\n   %s" % (sk[0].upper(), q, sk[1]))
                elif sk[0] == "err" and sl[0] == "err":
                    stats["both_err"] += 1
                elif sk[0] == "err":
                    stats["sqawk_err"] += 1
                    print("SQAWK-ERR: %s\n   sqawk : %s\n   sqlite: ok %s" % (
                        q, sk[1], [show(ordered, s) for s in sl[1]]))
                elif sl[0] == "err":
                    stats["sqlite_err"] += 1
                    print("SQLITE-ERR: %s\n   sqlite: %s\n   sqawk : %s" % (q, sl[1], sk[1][:8]))
                else:
                    got = split_sets(sk[1], sl[1])
                    if got is not None and all(same(ordered, a, b) for a, b in zip(got, sl[1])):
                        stats["pass"] += 1
                    else:
                        stats["DIFF"] += 1
                        exp = [r for s in sl[1] for r in s]
                        if got is not None:
                            mine = [r for s in got for r in s]
                        else:  # counts differ: show sqawk's records minus its first header
                            mine = sk[1][1:]
                        print("DIFF%s: %s\n   sqawk : %s\n   sqlite: %s" % (
                            "(ordered)" if ordered else "", q, show(ordered, mine), show(ordered, exp)))
                if sk[0] == "ok" and sk[2] > 2:
                    print("SLOW %.1fs: %s" % (sk[2], q))
    print("\nSUMMARY", dict(stats))


if __name__ == "__main__":
    main()
