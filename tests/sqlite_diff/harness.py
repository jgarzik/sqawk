#!/usr/bin/env python3
"""Differential tester: sqawk vs sqlite3.

Usage: python3 -I tests/sqlite_diff/harness.py [queries_file ...]
Set SQAWK to override the binary (default: target/release/sqawk; run
`cargo build --release` first).  Not part of `cargo test`.
Each query file: one test per line (blank / '--' lines skipped).
Prefix 'O:' => compare ordered; otherwise multiset compare.
Multiple statements separated by ';' are allowed (DML); only the
final SELECT's result is compared (sqlite runs all, collects last).
"""
import csv, io, os, re, sqlite3, subprocess, sys, tempfile, time, collections

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
SQAWK = os.environ.get("SQAWK", os.path.join(REPO, "target", "release", "sqawk"))
DATA = tempfile.mkdtemp(prefix="sqawk-diff-")

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


def infer(v):
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        return v
    return v


def write_data():
    os.makedirs(DATA, exist_ok=True)
    for t, (cols, rows) in TABLES.items():
        with open(os.path.join(DATA, t + ".csv"), "w", newline="") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(cols)
            for r in rows:
                w.writerow(["" if x is None else x for x in r])


def sqlite_db():
    db = sqlite3.connect(":memory:")
    for t, (cols, rows) in TABLES.items():
        db.execute("CREATE TABLE %s (%s)" % (t, ",".join(cols)))
        db.executemany("INSERT INTO %s VALUES (%s)" % (t, ",".join("?" * len(cols))),
                       [[infer(x) for x in r] for r in rows])
    return db


def norm(x):
    if x is None:
        return "NULL"
    if isinstance(x, bool):
        return str(int(x))
    if isinstance(x, int):
        return str(x)
    if isinstance(x, float):
        if x != x:
            return "NaN"
        if x == int(x) and abs(x) < 1e15:
            return str(int(x))
        return "%.9g" % x
    s = str(x)
    if s == "NULL":
        return "NULL"
    if re.fullmatch(r"-?\d+(\.\d+)?([eE][-+]?\d+)?", s):
        try:
            return norm(float(s)) if not re.fullmatch(r"-?\d+", s) else str(int(s))
        except ValueError:
            pass
    return s


def run_sqlite(db, sql):
    db.execute("SAVEPOINT sp")
    try:
        last = None
        for stmt in split_stmts(sql):
            cur = db.execute(stmt)
            if cur.description:
                last = [tuple(norm(v) for v in row) for row in cur.fetchall()]
        return ("ok", last)
    except Exception as ex:
        return ("err", str(ex))
    finally:
        db.execute("ROLLBACK TO sp")
        db.execute("RELEASE sp")


def split_stmts(sql):
    out, cur, q = [], "", False
    for ch in sql:
        if ch == "'":
            q = not q
        if ch == ";" and not q:
            if cur.strip():
                out.append(cur)
            cur = ""
        else:
            cur += ch
    if cur.strip():
        out.append(cur)
    return out


def tables_in(sql):
    return [t for t in TABLES if re.search(r"\b%s\b" % t, sql)]


def run_sqawk(sql, extra_args=()):
    files = [os.path.join(DATA, t + ".csv") for t in tables_in(sql)] or [os.path.join(DATA, "t1.csv")]
    t0 = time.time()
    try:
        p = subprocess.run([SQAWK, *extra_args, "-s", sql, *files], capture_output=True,
                           text=True, timeout=10)
    except subprocess.TimeoutExpired:
        return ("hang", None, 10)
    dt = time.time() - t0
    if p.returncode != 0:
        kind = "panic" if ("panicked" in p.stderr or p.returncode >= 100 or p.returncode < 0) else "err"
        return (kind, p.stderr.strip().splitlines()[-1] if p.stderr.strip() else "rc=%d" % p.returncode, dt)
    lines = list(csv.reader(io.StringIO(p.stdout)))
    # result sets each begin with a header; keep the last one.  A header is
    # hard to detect generically, so for multi-statement SQL we rely on the
    # final SELECT being the only SELECT.
    # sqawk renders NULL as an empty field
    rows = [tuple("NULL" if v == "" else norm(v) for v in r) for r in lines[1:]]
    return ("ok", rows, dt)


def compare(ordered, a, b):
    if ordered:
        return a == b
    return collections.Counter(a) == collections.Counter(b)


def main():
    write_data()
    db = sqlite_db()
    files = sys.argv[1:] or [os.path.join(HERE, "queries.sql")]
    stats = collections.Counter()
    for qf in files:
        for line in open(qf, encoding="utf-8"):
            q = line.strip()
            if not q or q.startswith("--"):
                continue
            ordered = q.startswith("O:")
            if ordered:
                q = q[2:].strip()
            sk = run_sqawk(q)
            sl = run_sqlite(db, q)
            if sk[0] in ("hang", "panic"):
                stats[sk[0]] += 1
                print("%s: %s\n   %s" % (sk[0].upper(), q, sk[1]))
            elif sk[0] == "err":
                stats["sqawk_err"] += 1
                print("UNSUPPORTED/ERR: %s\n   sqawk: %s\n   sqlite: %s" % (
                    q, sk[1], "ok" if sl[0] == "ok" else sl[1]))
            elif sl[0] == "err":
                stats["sqlite_err"] += 1
                print("SQLITE-ERR(sqawk ok): %s\n   sqlite: %s\n   sqawk: %s" % (q, sl[1], sk[1][:8]))
            elif compare(ordered, sk[1], sl[1] or []):
                stats["pass"] += 1
            else:
                stats["DIFF"] += 1
                print("DIFF%s: %s\n   sqawk : %s\n   sqlite: %s" % (
                    "(ordered)" if ordered else "", q, sk[1][:12], (sl[1] or [])[:12]))
            if sk[0] == "ok" and sk[2] > 2:
                print("SLOW %.1fs: %s" % (sk[2], q))
    print("\nSUMMARY", dict(stats))


if __name__ == "__main__":
    main()
