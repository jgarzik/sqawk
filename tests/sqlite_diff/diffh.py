#!/usr/bin/env python3
"""Adversarial differential harness: sqawk vs SQLite (python3's sqlite3).

What it tests
-------------
SELECT / DML results of sqawk against SQLite on a fixed set of gnarly tables
(i64 extremes, numeric-looking text such as `007` / `1.50` / `1e3`, mixed
types per column, NULLs, duplicates, Unicode, embedded quotes/commas/LF, a
5000-character string, date strings).  Two sources of queries:
  * hand corpora (hand1.sql: types, precedence, arithmetic, strings, printf,
    LIKE; hand2.sql: aggregates, windows, joins, subqueries, compounds;
    hand3.sql: DML sequences, CTAS/ALTER, NULLs, ORDER BY/LIMIT, dates);
    one test per line, `--` lines skipped, `O:` prefix = ordered compare,
    `;`-separated statements allowed (the last result set is compared);
  * a seeded random generator (joins of every kind, correlated subqueries,
    GROUP BY/HAVING/FILTER, windows, compounds, derived tables, DML).

Usage (from anywhere; paths resolve from this file's location):

    cargo build --release
    python3 -I tests/sqlite_diff/diffh.py --only-hand tsq/corpus/hand1.sql tsq/corpus/hand2.sql tsq/corpus/hand3.sql
    python3 -I tests/sqlite_diff/diffh.py --only-random --seed 1 --random 700

Options: --seed N (random generator seed, default 12345), --random N (number
of random queries, default 700), --only-hand, --only-random; remaining
arguments are hand corpus files.  SQAWK overrides the binary (default
<repo>/target/release/sqawk).  Table data goes into a temporary directory,
removed at exit.

Like-for-like setup
-------------------
Every cell is written as CSV text and inserted into SQLite as the value
sqawk loads from that text (`cell_value`, mirroring src/table.rs
`infer_value`: empty -> NULL, canonical integer -> int, text equal to
SQLite's CAST(x AS TEXT) of its double -> float, anything else -> verbatim
text).  SQLite columns are declared without a type (no affinity), so SQLite
stores exactly those values.  SQLite results are rendered as sqawk prints
them: NULL as empty, reals via SQLite's CAST(x AS TEXT) (`1.0`, `0.3`,
`1.0e+20`, `Inf`), integers and text verbatim; sqawk's stdout is parsed as
CSV (its header line dropped).  Limitation: NULL and the empty string both
print as an empty field, so they cannot be told apart.

Output
------
One block per non-passing test on stdout, then
    SUMMARY {'pass': N, 'diff': N, 'loose': N, 'sqawk_err': N, ...}
pass = identical rows (multiset, or in order for ordered tests); diff =
different rows; loose = rows equal only after reading numeric-looking
fields as numbers compared to 9 significant digits (a rendering-only
difference, still a difference); sqawk_err = sqawk failed where SQLite ran
the query; sqlite_err = SQLite failed where sqawk ran it; both_err = both
rejected it (counted, not printed); panic / hang = sqawk crashed or took over
10 s; total / hand / rand count tests by source.  A clean run has no panic
or hang, and every diff / loose / sqawk_err is a documented difference from
SQLite (doc/sql_reference.md "Differences from SQLite" / "Unsupported
syntax").
"""
import collections, csv, io, os, random, re, sqlite3, subprocess, sys, tempfile, time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
SQAWK = os.environ.get("SQAWK", os.path.join(REPO, "target", "release", "sqawk"))
DATA = None  # temporary directory holding the CSV files, set by main()

# ---------------------------------------------------------------- data
_RDB = sqlite3.connect(":memory:")


def sqlite_real(f):
    """SQLite's CAST(x AS TEXT) of a real -- exactly what sqawk prints."""
    return _RDB.execute("SELECT CAST(? AS TEXT)", (f,)).fetchone()[0] or ""


def field_text(x):
    """A Python value as CSV field text (NULL -> empty)."""
    if x is None:
        return ""
    if isinstance(x, float):
        return sqlite_real(x)
    return str(x)


def cell_value(text):
    """Mirror sqawk's load-time inference (src/table.rs infer_value)."""
    if text == "":
        return None
    if re.fullmatch(r"-?(0|[1-9][0-9]*)", text) and text != "-0":
        v = int(text)
        if -2**63 <= v < 2**63:
            return v
        return text
    if "." in text and re.fullmatch(r"[-+.e0-9]+", text):
        try:
            f = float(text)
        except ValueError:
            return text
        if f == f and f not in (float("inf"), float("-inf")) and sqlite_real(f) == text:
            return f
    return text

def build_tables(seed):
    R = random.Random(seed)
    T = {}
    cats = ["red", "blue", "Green", None, "red ", "blue"]
    ntext = ["007", "1.50", " 5", "10", "9", "1e3", "0x10", "-0", "+3", "12abc", "", "2.50"]
    mixed = [5, "abc", 2.5, "007", None, -3, "Zed", 0, "", 10]
    words = ["alpha", "Beta", "gamma", "x%y", "A_b", "it's", "", None, "quote\"d", "comma,s",
             "naïve", "日本", "Ünïcode", "a\nb", "ALPHA", "beta", "  sp  ", "ß"]
    rows = []
    for i in range(1, 21):
        rows.append([i, R.choice([1, 2, 3, 4, None, 2, 3]),
                     R.choice([None, 0, 1, -7, 42, 100, 3, 3, -1, 9223372036854775807, -9223372036854775808]) if i % 7 else None,
                     R.choice([None, 1.5, -0.25, 2.75, 0.1, 3.14159, 100.5, -2.5, 1e-5]),
                     R.choice(words), R.choice(cats), R.choice(ntext), R.choice(mixed)])
    T["a"] = (["aid", "k", "x", "y", "s", "g", "n", "m"], rows)
    rows = []
    for i in range(1, 26):
        rows.append([100 + i, R.choice(list(range(1, 24)) + [None]), R.choice([1, 2, 3, 5, None]),
                     R.choice([None, 0, 1, 2, 10, -5, 7, 7, 1000000]), R.choice(words)])
    T["b"] = (["bid", "aid", "k", "v", "w"], rows)
    rows = []
    for i in range(1, 16):
        rows.append([200 + i, R.choice(list(range(101, 128)) + [None]), R.choice([1, 2, 4, 6, None]),
                     R.choice([None, 0.5, 1.25, -3.5, 2.0 if False else 2.5, 7.75]), R.choice(cats)])
    T["c"] = (["cid", "bid", "k", "z", "g"], rows)
    T["d"] = (["k", "label", "wt"], [[1, "one", 10], [2, "two", 20], [3, "three", None],
                                      [None, "nil", 5], [5, "five", 50], [2, "deux", 21]])
    T["e"] = (["eid", "k", "x"], [])
    T["dup"] = (["p", "q"], [[1, "a"], [1, "a"], [2, None], [2, None], [None, None], [None, None], [3, "c"], [1, "a"]])
    T["u"] = (["uid", "t"], [[1, "héllo"], [2, "日本語テキスト"], [3, "x" * 5000], [4, "émoji 🎉"], [5, "MiXeD"],
                             [6, "tab\there"], [7, "ÀÉÎ"], [8, "  lead"], [9, "trail  "], [10, "%_\\"]])
    T["dt"] = (["did", "ts", "mod"], [[1, "2024-01-31", "+1 month"], [2, "2024-02-29 12:34:56", "-1 year"],
                                       [3, "2000-01-01T00:00:00", "start of month"], [4, "1970-01-01", "+86400 seconds"],
                                       [5, "2023-12-31 23:59:59.999", "+1 second"], [6, "bad", "+1 day"],
                                       [7, "2024-03-10", "weekday 0"], [8, "12:30", "+90 minutes"],
                                       [9, "2459000.5", "+0 days"], [10, None, "+1 day"], [11, "2024-06-15", "start of year"],
                                       [12, "0000-01-01", "-1 day"], [13, "9999-12-31", "+1 day"]])
    return T

def write_data(T):
    for t, (cols, rows) in T.items():
        with open(os.path.join(DATA, t + ".csv"), "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(cols)
            for r in rows:
                w.writerow([field_text(x) for x in r])

def sqlite_db(T):
    db = sqlite3.connect(":memory:")
    db.isolation_level = None
    # printf('%.1s', '日本') cuts inside a UTF-8 sequence in both engines;
    # decode as sqawk's output is decoded instead of raising.
    db.text_factory = lambda b: b.decode("utf-8", "replace")
    for t, (cols, rows) in T.items():
        db.execute("CREATE TABLE %s (%s)" % (t, ",".join(cols)))
        for r in rows:
            db.execute("INSERT INTO %s VALUES (%s)" % (t, ",".join("?" * len(cols))),
                       [cell_value(field_text(x)) for x in r])
    return db

# ---------------------------------------------------------------- compare
NUMRE = re.compile(r"[-+]?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?|[-+]?inf")

def strict_tok(v):
    """A SQLite result value rendered as sqawk prints it."""
    if v is None or v == "":
        return ""
    if isinstance(v, bool):
        return str(int(v))
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        return sqlite_real(v)
    if isinstance(v, bytes):
        return "<blob>"
    return v

def loose_tok(s):
    if s == "":
        return ""
    if NUMRE.fullmatch(s.strip()):
        try:
            f = float(s)
            if f == 0:
                return "N0"
            return "N%.9g" % f
        except ValueError:
            pass
    return "S" + s

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
    db.execute("SAVEPOINT sp")
    try:
        last = None
        for st in split_stmts(sql):
            cur = db.execute(st)
            if cur.description:
                last = [tuple(strict_tok(v) for v in row) for row in cur.fetchall()]
        return ("ok", last if last is not None else [])
    except Exception as ex:
        return ("err", str(ex))
    finally:
        db.execute("ROLLBACK TO sp")
        db.execute("RELEASE sp")

def run_sqawk(sql, tables):
    files = [os.path.join(DATA, t + ".csv") for t in tables]
    t0 = time.time()
    try:
        p = subprocess.run([SQAWK, "-s", sql, *files], capture_output=True, timeout=10)
    except subprocess.TimeoutExpired:
        return ("hang", "timeout", 10)
    dt = time.time() - t0
    err = p.stderr.decode("utf-8", "replace")
    if p.returncode != 0 or "panicked" in err:
        kind = "panic" if ("panicked" in err or p.returncode < 0 or p.returncode >= 100) else "err"
        return (kind, (err.strip().splitlines() or ["rc=%d" % p.returncode])[-1][:200], dt)
    out = p.stdout.decode("utf-8", "replace")
    recs = list(csv.reader(io.StringIO(out, newline="")))
    return ("ok", [tuple(r) for r in recs[1:]], dt)

def compare(ordered, sk, sl):
    if len(sk) != len(sl):
        return "diff"
    if ordered:
        if sk == sl:
            return "pass"
        a = [tuple(loose_tok(x) for x in r) for r in sk]
        b = [tuple(loose_tok(x) for x in r) for r in sl]
        return "loose" if a == b else "diff"
    if collections.Counter(sk) == collections.Counter(sl):
        return "pass"
    a = collections.Counter(tuple(loose_tok(x) for x in r) for r in sk)
    b = collections.Counter(tuple(loose_tok(x) for x in r) for r in sl)
    return "loose" if a == b else "diff"

# ---------------------------------------------------------------- random generator
SCHEMA = {
    "a": {"aid": "i", "k": "i", "x": "i", "y": "r", "s": "t", "g": "t", "n": "n", "m": "m"},
    "b": {"bid": "i", "aid": "i", "k": "i", "v": "i", "w": "t"},
    "c": {"cid": "i", "bid": "i", "k": "i", "z": "r", "g": "t"},
    "d": {"k": "i", "label": "t", "wt": "i"},
    "e": {"eid": "i", "k": "i", "x": "i"},
    "dup": {"p": "i", "q": "t"},
}
PK = {"a": "aid", "b": "bid", "c": "cid", "e": "eid"}

class Gen:
    def __init__(self, seed):
        self.R = random.Random(seed)
        self.n = 0

    def alias(self):
        self.n += 1
        return "t%d" % self.n

    def ch(self, xs):
        return self.R.choice(xs)

    def cols(self, scopes, kinds):
        out = []
        for sc in scopes:
            for (al, t) in sc:
                for c, k in SCHEMA[t].items():
                    if k in kinds:
                        out.append("%s.%s" % (al, c))
        return out

    # kinds: i int, r real, t text, b bool
    def expr(self, scopes, kind, d):
        R = self.R
        if d <= 0 or R.random() < 0.3:
            return self.leaf(scopes, kind)
        if kind == "b":
            return self.boolexpr(scopes, d)
        choice = R.random()
        if kind in "ir":
            if choice < 0.30:
                op = self.ch(["+", "-", "*", "/", "%"] if kind == "i" else ["+", "-", "*", "/"])
                l = self.expr(scopes, kind, d - 1)
                r = self.expr(scopes, self.ch(["i", kind]), d - 1)
                return "(%s %s %s)" % (l, op, r)
            if choice < 0.42:
                return "CASE WHEN %s THEN %s ELSE %s END" % (self.boolexpr(scopes, d - 1), self.expr(scopes, kind, d - 1), self.expr(scopes, kind, d - 1))
            if choice < 0.52:
                return "COALESCE(%s, %s)" % (self.expr(scopes, kind, d - 1), self.expr(scopes, kind, d - 1))
            if choice < 0.58:
                return "IIF(%s, %s, %s)" % (self.boolexpr(scopes, d - 1), self.expr(scopes, kind, d - 1), self.expr(scopes, kind, d - 1))
            if choice < 0.64:
                return "NULLIF(%s, %s)" % (self.expr(scopes, kind, d - 1), self.expr(scopes, kind, d - 1))
            if choice < 0.74:
                return self.scalar_sub(scopes, kind, d - 1)
            if kind == "i":
                return self.ch(["LENGTH(%s)" % self.expr(scopes, "t", d - 1),
                                "ABS(%s)" % self.leaf(scopes, "i"),
                                "INSTR(%s, %s)" % (self.expr(scopes, "t", d - 1), self.ch(["'a'", "'e'", "''"])),
                                "CAST(%s AS INTEGER)" % self.ch(self.cols(scopes, "nrt") or ["'12x'"]),
                                "MAX(%s, %s)" % (self.expr(scopes, "i", d - 1), self.expr(scopes, "i", d - 1)),
                                "MIN(%s, %s)" % (self.expr(scopes, "i", d - 1), self.expr(scopes, "i", d - 1)),
                                "UNICODE(%s)" % self.expr(scopes, "t", d - 1),
                                "SIGN(%s)" % self.expr(scopes, "i", d - 1)])
            return self.ch(["ROUND(%s, %d)" % (self.expr(scopes, "r", d - 1), R.randint(0, 3)),
                            "(%s + 0.5)" % self.ch(self.cols(scopes, "n") or ["'1.5'"]),
                            "(%s * 1.0)" % self.expr(scopes, "i", d - 1),
                            "CAST(%s AS REAL)" % self.ch(self.cols(scopes, "nit") or ["'2.5x'"]),
                            "ABS(%s)" % self.expr(scopes, "r", d - 1)])
        # text
        if choice < 0.25:
            return "(%s || %s)" % (self.expr(scopes, "t", d - 1), self.expr(scopes, self.ch(["t", "i"]), d - 1))
        if choice < 0.35:
            return "CASE %s WHEN %s THEN %s ELSE %s END" % (self.expr(scopes, "i", d - 1), R.randint(0, 3), self.expr(scopes, "t", d - 1), self.expr(scopes, "t", d - 1))
        if choice < 0.45:
            return "COALESCE(%s, %s, %s)" % (self.expr(scopes, "t", d - 1), self.expr(scopes, "t", d - 1), self.leaf(scopes, "t"))
        if choice < 0.52:
            return self.scalar_sub(scopes, "t", d - 1)
        return self.ch(["SUBSTR(%s, %d, %d)" % (self.expr(scopes, "t", d - 1), R.randint(-3, 4), R.randint(-2, 4)),
                        "REPLACE(%s, %s, %s)" % (self.expr(scopes, "t", d - 1), self.ch(["'a'", "'%'", "''", "'ph'"]), self.ch(["'Z'", "''", "'aa'"])),
                        "LOWER(%s)" % self.ch(["'ABC'", "'MiXeD'", "NULL"]), "UPPER(%s)" % self.ch(["'abc'", "'x%y'"]),
                        "LTRIM(%s)" % self.expr(scopes, "t", d - 1), "RTRIM(%s, 'a ')" % self.expr(scopes, "t", d - 1),
                        "PRINTF('%%5d|%%s|%%.2f', %s, %s, %s)" % (self.expr(scopes, "i", d - 1), self.expr(scopes, "t", d - 1), self.expr(scopes, "r", d - 1)),
                        "CAST(%s AS TEXT)" % self.expr(scopes, "i", d - 1),
                        "TYPEOF(%s)" % self.ch(self.cols(scopes, "irtnm") or ["1"]),
                        "HEX(%s)" % self.expr(scopes, "t", d - 1),
                        "IIF(%s, %s, NULL)" % (self.boolexpr(scopes, d - 1), self.leaf(scopes, "t"))])

    def leaf(self, scopes, kind):
        R = self.R
        cs = self.cols(scopes, {"i": "i", "r": "r", "t": "t", "b": ""}[kind])
        if kind == "b":
            return self.boolexpr(scopes, 0)
        if cs and R.random() < 0.7:
            return self.ch(cs)
        if kind == "i":
            return self.ch(["0", "1", "2", "-1", "3", "NULL", "10", "9223372036854775807", "-9223372036854775808", "1000000007"])
        if kind == "r":
            return self.ch(["0.5", "1.5", "-2.25", "NULL", "0.1", "1e10"])
        return self.ch(["'a'", "'b'", "''", "NULL", "'red'", "'x%y'", "'Beta'", "'日本'"])

    def boolexpr(self, scopes, d):
        R = self.R
        c = R.random()
        if d > 0 and c < 0.2:
            return "(%s %s %s)" % (self.boolexpr(scopes, d - 1), self.ch(["AND", "OR"]), self.boolexpr(scopes, d - 1))
        if d > 0 and c < 0.25:
            return "NOT (%s)" % self.boolexpr(scopes, d - 1)
        if c < 0.5:
            k = self.ch(["i", "i", "r", "t"])
            return "(%s %s %s)" % (self.expr(scopes, k, d - 1), self.ch(["=", "<>", "<", "<=", ">", ">=", "IS NOT DISTINCT FROM", "IS DISTINCT FROM"]), self.expr(scopes, "i" if k == "r" else k, d - 1))
        if c < 0.58:
            return "%s IS %sNULL" % (self.expr(scopes, self.ch("irt"), d - 1), self.ch(["", "NOT "]))
        if c < 0.65:
            return "%s %sBETWEEN %s AND %s" % (self.expr(scopes, "i", d - 1), self.ch(["", "NOT "]), self.leaf(scopes, "i"), self.leaf(scopes, "i"))
        if c < 0.72:
            return "%s %sIN (%s)" % (self.expr(scopes, "i", d - 1), self.ch(["", "NOT "]), ", ".join(self.ch(["1", "2", "3", "NULL", "42", "-7"]) for _ in range(R.randint(1, 4))))
        if c < 0.78:
            return "%s %sLIKE %s" % (self.expr(scopes, "t", d - 1), self.ch(["", "NOT "]), self.ch(["'a%'", "'%a%'", "'_'", "'%e_'", "'B%'", "'%'", "'%\\%%' ESCAPE '\\'", "'r_d%'"]))
        if d > 0 and c < 0.9:
            return self.in_sub(scopes, d - 1)
        if d > 0:
            return self.exists_sub(scopes, d - 1)
        return "%s > %s" % (self.leaf(scopes, "i"), self.leaf(scopes, "i"))

    def sub_from(self):
        t = self.ch(["a", "b", "c", "d", "e", "dup"])
        al = self.alias()
        return t, al

    def corr(self, scopes, al, t):
        # correlate with some outer column of matching int kind
        outer = self.cols(scopes, "i")
        inner = ["%s.%s" % (al, c) for c, k in SCHEMA[t].items() if k == "i"]
        if not outer or not inner:
            return "1"
        return "%s %s %s" % (self.ch(inner), self.ch(["=", "=", "<", ">="]), self.ch(outer))

    def scalar_sub(self, scopes, kind, d):
        t, al = self.sub_from()
        sc = scopes + [[(al, t)]]
        where = self.corr(scopes, al, t)
        if d > 0 and self.R.random() < 0.5:
            where += " AND " + self.boolexpr(sc, d - 1)
        if kind == "t":
            while not self.cols([[(al, t)]], "t"):
                t = self.ch(["a", "b", "c", "d", "dup"])
                al = self.alias()
                sc = scopes + [[(al, t)]]
                where = self.corr(scopes, al, t)
            tc = self.cols([[(al, t)]], "t")
            if not tc:
                return "(SELECT MAX(%s) FROM %s %s WHERE %s)" % (self.expr(sc, "t", 0), t, al, where)
            return "(SELECT %s(%s) FROM %s %s WHERE %s)" % (self.ch(["MAX", "MIN"]), self.ch(tc), t, al, where)
        agg = self.ch(["COUNT(*)", "SUM(%s)", "MAX(%s)", "MIN(%s)", "COUNT(DISTINCT %s)", "TOTAL(%s)", "AVG(%s)"])
        if "%s" in agg:
            agg = agg % self.expr([[(al, t)]], "i", min(d, 1))
        if kind == "i" and agg.startswith(("TOTAL", "AVG")):
            agg = "COUNT(*)"
        return "(SELECT %s FROM %s %s WHERE %s)" % (agg, t, al, where)

    def in_sub(self, scopes, d):
        t, al = self.sub_from()
        sc = scopes + [[(al, t)]]
        ic = self.cols([[(al, t)]], "i")
        w = self.corr(scopes, al, t) if self.R.random() < 0.5 else self.boolexpr(sc, d)
        if d > 0 and self.R.random() < 0.4:
            w += " AND " + self.boolexpr(sc, d - 1)
        return "%s %sIN (SELECT %s FROM %s %s WHERE %s)" % (self.expr(scopes, "i", 0), self.ch(["", "NOT "]), self.ch(ic), t, al, w)

    def exists_sub(self, scopes, d):
        t, al = self.sub_from()
        sc = scopes + [[(al, t)]]
        w = self.corr(scopes, al, t)
        if d > 0:
            w += " AND " + self.boolexpr(sc, d - 1)
        return "%sEXISTS (SELECT 1 FROM %s %s WHERE %s)" % (self.ch(["", "NOT "]), t, al, w)

    def from_clause(self):
        R = self.R
        n = self.ch([1, 1, 2, 2, 3, 3, 4])
        tabs = []
        sql = ""
        for i in range(n):
            t = self.ch(["a", "b", "c", "d", "e", "dup", "a", "b"])
            al = self.alias()
            if i == 0:
                sql = "%s %s" % (t, al)
            else:
                jt = self.ch(["JOIN", "LEFT JOIN", "LEFT JOIN", "CROSS JOIN", "INNER JOIN", ",", "JOIN", "LEFT JOIN"] + (["RIGHT JOIN", "FULL JOIN"] if i == n - 1 else []))
                if jt in ("CROSS JOIN", ","):
                    sql += " %s %s %s" % (jt, t, al)
                else:
                    prev = tabs[:]
                    left_i = [(pa, c) for (pa, pt) in prev for c, k in SCHEMA[pt].items() if k == "i"]
                    right_i = [c for c, k in SCHEMA[t].items() if k == "i"]
                    pa, pc = self.ch(left_i)
                    cond = "%s.%s = %s.%s" % (pa, pc, al, self.ch(right_i))
                    if R.random() < 0.25:
                        cond += " AND " + self.boolexpr([prev + [(al, t)]], 0)
                    sql += " %s %s %s ON %s" % (jt, t, al, cond)
            tabs.append((al, t))
        return sql, tabs

    def select(self, scopes=None, d=2):
        R = self.R
        scopes = scopes or []
        fr, tabs = self.from_clause()
        sc = scopes + [tabs]
        where = " WHERE " + self.boolexpr(sc, d) if R.random() < 0.7 else ""
        mode = R.random()
        if mode < 0.35:
            # grouped
            keys = []
            for _ in range(R.randint(1, 2)):
                keys.append(self.ch([self.expr([tabs], self.ch("it"), 1), self.ch(self.cols([tabs], "it"))]))
            keys = list(dict.fromkeys(keys))
            aggs = []
            for _ in range(R.randint(1, 3)):
                a = self.ch(["COUNT(*)", "COUNT(%s)", "SUM(%s)", "MIN(%s)", "MAX(%s)", "COUNT(DISTINCT %s)", "SUM(DISTINCT %s)", "TOTAL(%s)", "AVG(%s)"])
                if "%s" in a:
                    a = a % self.expr(sc, "i", 1)
                if R.random() < 0.15:
                    a += " FILTER (WHERE %s)" % self.boolexpr(sc, 0)
                aggs.append(a)
            sel = keys + aggs
            win = ""
            if R.random() < 0.3:
                sel.append("RANK() OVER (ORDER BY %s DESC)" % aggs[0])
                sel.append("SUM(%s) OVER ()" % aggs[0].split(" FILTER")[0] if not aggs[0].startswith("AVG") else "COUNT(*) OVER ()")
            having = " HAVING %s %s %s" % (self.ch(["COUNT(*)", "SUM(%s)" % self.leaf([tabs], "i"), "MAX(%s)" % self.leaf([tabs], "i")]), self.ch([">", ">=", "<", "="]), self.ch(["1", "2", "0", "5"])) if R.random() < 0.4 else ""
            sql = "SELECT %s FROM %s%s GROUP BY %s%s" % (", ".join(sel), fr, where, ", ".join(keys), having)
            ordered = False
            if R.random() < 0.4:
                sql += " ORDER BY " + ", ".join(str(i + 1) for i in range(len(sel)) if True)
                if R.random() < 0.5:
                    sql += " LIMIT %d" % R.randint(0, 4)
                    if R.random() < 0.5:
                        sql += " OFFSET %d" % R.randint(0, 3)
                ordered = not any(("AVG" in s or "TOTAL" in s) for s in sel)
            return sql, ordered
        sel = []
        for _ in range(R.randint(1, 4)):
            sel.append(self.expr(sc, self.ch("iirt"), d))
        if R.random() < 0.25:
            pks = ["%s.%s" % (al, PK.get(t, list(SCHEMA[t])[0])) for al, t in tabs]
            f = self.ch(["ROW_NUMBER() OVER (ORDER BY %s)", "RANK() OVER (PARTITION BY %s ORDER BY %s)",
                         "SUM(%s) OVER (ORDER BY %s)", "LAG(%s) OVER (ORDER BY %s)", "DENSE_RANK() OVER (ORDER BY %s)",
                         "COUNT(*) OVER (PARTITION BY %s)", "MAX(%s) OVER (ORDER BY %s ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)"])
            ic = self.cols([tabs], "i")
            if f.startswith("ROW_NUMBER") or f.startswith("DENSE"):
                f = f % ", ".join(pks)
            elif f.startswith("RANK"):
                f = f % (self.ch(ic), ", ".join(pks))
            elif f.startswith("COUNT"):
                f = f % self.ch(ic)
            else:
                f = f % (self.ch(ic), ", ".join(pks))
            sel.append(f)
        dist = "DISTINCT " if R.random() < 0.2 else ""
        sql = "SELECT %s%s FROM %s%s" % (dist, ", ".join(sel), fr, where)
        ordered = False
        if R.random() < 0.35:
            sql += " ORDER BY " + ", ".join("%d%s" % (i + 1, self.ch(["", " DESC", " NULLS LAST"])) for i in range(len(sel)))
            if R.random() < 0.6:
                sql += " LIMIT %d" % R.randint(0, 5)
                if R.random() < 0.4:
                    sql += " OFFSET %d" % R.randint(0, 4)
            ordered = True
        return sql, ordered

    def compound(self):
        R = self.R
        kinds = [self.ch("it") for _ in range(R.randint(1, 2))]
        arms = []
        for _ in range(R.randint(2, 3)):
            fr, tabs = self.from_clause() if R.random() < 0.5 else ("%s %s" % ((lambda t: (t, self.alias()))(self.ch(["a", "b", "c", "d", "dup", "e"])))
                                                                    , None)
            if tabs is None:
                t, al = fr.split()
                tabs = [(al, t)]
            sel = ", ".join(self.expr([tabs], k, 1) for k in kinds)
            w = " WHERE " + self.boolexpr([tabs], 1) if R.random() < 0.6 else ""
            arms.append("SELECT %s FROM %s%s" % (sel, fr, w))
        sql = arms[0]
        for j, a in enumerate(arms[1:]):
            sql += " %s %s" % (self.ch(["UNION", "UNION ALL", "EXCEPT", "UNION"] + (["INTERSECT"] if j == 0 else [])), a)
        ordered = False
        if R.random() < 0.5:
            sql += " ORDER BY " + ", ".join("%d%s" % (i + 1, self.ch(["", " DESC"])) for i in range(len(kinds)))
            if R.random() < 0.6:
                sql += " LIMIT %d" % R.randint(1, 5)
            ordered = True
        return sql, ordered

    def derived(self):
        inner, _ = self.select(d=1)
        # wrap: count and checksum over derived table
        return "SELECT COUNT(*) FROM (%s) sub" % inner, False

    def dml(self):
        R = self.R
        target = self.ch(["a", "b", "c", "d", "dup"])
        stmts = []
        for _ in range(R.randint(1, 3)):
            c = R.random()
            al = "tt"
            others = [x for x in ["a", "b", "c", "d", "e"] if x != target]
            if c < 0.35:
                o = self.ch(others)
                o2 = self.ch([x for x in others if x != o])
                cols = list(SCHEMA[target])
                kinds = list(SCHEMA[target].values())
                scope = [[("s1", o), ("s2", o2)]]
                exprs = [self.expr(scope, k if k in "irt" else "t", 1) for k in kinds]
                stmts.append("INSERT INTO %s SELECT %s FROM %s s1 %s JOIN %s s2 ON %s WHERE %s" % (
                    target, ", ".join(exprs), o, self.ch(["", "LEFT"]), o2,
                    "s1.k = s2.k", self.boolexpr(scope, 1)))
            elif c < 0.7:
                o = self.ch(others)
                col, k = self.ch([(cc, kk) for cc, kk in SCHEMA[target].items() if kk in "irt"])
                al2 = self.alias()
                kc = "p" if target == "dup" else "k"
                sub = "(SELECT %s(%s.%s) FROM %s %s WHERE %s.k = %s." % (self.ch(["MAX", "MIN", "COUNT"]) if k != "t" else self.ch(["MAX", "MIN"]),
                                                                       al2, self.ch([cc for cc, kk in SCHEMA[o].items() if kk == ("i" if k != "t" else "t")] or ["k"]), o, al2, al2, target) + kc + ")"
                if k == "t" and not [cc for cc, kk in SCHEMA[o].items() if kk == "t"]:
                    sub = "'none'"
                stmts.append("UPDATE %s SET %s = %s WHERE %s" % (target, col, self.ch([sub, "COALESCE(%s, %s)" % (sub, self.leaf([[(target, target)]], k))]),
                                                                 self.boolexpr([[(target, target)]], 1)))
            else:
                o = self.ch(others)
                al2 = self.alias()
                stmts.append("DELETE FROM %s WHERE %s.%s %sIN (SELECT %s.k FROM %s %s WHERE %s)" % (
                    target, target, "p" if target == "dup" else "k", self.ch(["", "NOT "]), al2, o, al2, self.boolexpr([[(al2, o)]], 1)))
        stmts.append("SELECT * FROM %s" % target)
        return "; ".join(stmts), False

    def query(self):
        r = self.R.random()
        if r < 0.55:
            return self.select()
        if r < 0.72:
            return self.compound()
        if r < 0.82:
            return self.derived()
        return self.dml()

# ---------------------------------------------------------------- main
def tables_in(sql, T):
    return [t for t in T if re.search(r"\b%s\b" % t, sql, re.I)] or ["a"]

def main():
    global DATA
    args = sys.argv[1:]
    seed, nrand, mode = 12345, 700, "both"
    files = []
    i = 0
    while i < len(args):
        if args[i] == "--seed":
            seed = int(args[i + 1]); i += 2
        elif args[i] == "--random":
            nrand = int(args[i + 1]); i += 2
        elif args[i] == "--only-hand":
            mode = "hand"; i += 1
        elif args[i] == "--only-random":
            mode = "random"; i += 1
        elif args[i] in ("-h", "--help"):
            print(__doc__)
            return
        else:
            files.append(args[i]); i += 1
    T = build_tables(7)
    tests = []
    if mode != "random":
        for f in files:
            for line in open(f, encoding="utf-8"):
                q = line.rstrip("\n")
                if not q.strip() or q.startswith("--"):
                    continue
                ordered = q.startswith("O:")
                tests.append(("hand", q[2:] if ordered else q, ordered))
    if mode != "hand":
        g = Gen(seed)
        for _ in range(nrand):
            q, o = g.query()
            tests.append(("rand", q, o))
    stats = collections.Counter()
    with tempfile.TemporaryDirectory(prefix="sqawk-diffh-") as DATA:
        write_data(T)
        db = sqlite_db(T)
        for src, q, ordered in tests:
            sk = run_sqawk(q, tables_in(q, T))
            sl = run_sqlite(db, q)
            key = None
            if sk[0] in ("hang", "panic"):
                key = sk[0]
                msg = "%s [%s]: %s\n   %s" % (key.upper(), src, q, sk[1])
            elif sk[0] == "err" and sl[0] == "err":
                key = "both_err"; msg = None
            elif sk[0] == "err":
                key = "sqawk_err"; msg = "SQAWK-ERR [%s]: %s\n   sqawk: %s" % (src, q, sk[1])
            elif sl[0] == "err":
                key = "sqlite_err"; msg = "SQLITE-ERR [%s]: %s\n   sqlite: %s\n   sqawk: %s" % (src, q, sl[1], sk[1][:5])
            else:
                r = compare(ordered, sk[1], sl[1])
                key = r
                msg = None if r == "pass" else "%s%s [%s]: %s\n   sqawk : %s\n   sqlite: %s" % (
                    r.upper(), "(ord)" if ordered else "", src, q, sorted(sk[1])[:10] if not ordered else sk[1][:10],
                    sorted(sl[1])[:10] if not ordered else sl[1][:10])
            stats[key] += 1
            stats["total"] += 1
            stats[src] += 1
            if msg:
                print(msg)
    print("SUMMARY", dict(stats))

if __name__ == "__main__":
    main()
