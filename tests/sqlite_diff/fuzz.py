#!/usr/bin/env python3
"""Crash / hang fuzzing for sqawk's SQL front end and VM.

What it tests
-------------
* garbage SQL: random bytes and random token soup (NGARB runs, default
  1500), over the diffh.py tables a, b, d, e, dup;
* ~50 targeted scalar calls at the limits (printf widths of 2^31, substr /
  lag / ntile / LIMIT / round with i64 extremes, char() past U+10FFFF,
  dates past year 9999, i64 MIN / -1, 50k-character LIKE patterns, 8-way
  cross joins);
* deep nesting and huge statements: 20,000 nested parentheses, 50,000-term
  + / AND / || chains, 10,000 NOTs, nested CASE / COALESCE / subqueries /
  derived tables, 100k-element IN lists, 5000-arm UNIONs, 20,000-column
  select lists, a 10 MB literal, 10,000 statements, a 64-table join
  (statements over 100 kB go through the REPL on stdin).

Usage (from anywhere):

    cargo build --release
    python3 -I tests/sqlite_diff/fuzz.py

SQAWK overrides the binary (default <repo>/target/release/sqawk); NGARB sets
the garbage-SQL run count; SKIPNASTY=1 skips the targeted scalar calls.
Table data (diffh.py's tables) goes into a temporary directory, removed at
exit.

Output
------
One line per targeted / nesting case (status `ok`, `err` = clean error,
`PANIC`, `SIGNAL<n>`, `HANG` = over the timeout, `RC<n>` = unexpected exit
code), garbage-SQL lines only for non-ok/err results, then
    FUZZ SUMMARY {'garbage': {...}, 'cases': N, 'findings': N}
A pass is findings == 0: sqawk answered or failed with a clean error every
time.  Findings are repeated under FINDINGS.
"""
import importlib.util, os, random, subprocess, sys, tempfile, time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
SQAWK = os.environ.get("SQAWK", os.path.join(REPO, "target", "release", "sqawk"))

def run(args, timeout=10, stdin=None):
    t0 = time.time()
    try:
        p = subprocess.run([SQAWK, *args], capture_output=True, timeout=timeout, input=stdin)
    except subprocess.TimeoutExpired:
        return ("HANG", time.time() - t0, "")
    dt = time.time() - t0
    err = p.stderr.decode("utf-8", "replace")
    if p.returncode < 0:
        return ("SIGNAL%d" % -p.returncode, dt, err[-300:])
    if "panicked" in err or ("overflow" in err.lower() and "stack" in err.lower()):
        return ("PANIC", dt, err[-300:])
    if p.returncode not in (0, 1, 2):
        return ("RC%d" % p.returncode, dt, err[-300:])
    return ("ok" if p.returncode == 0 else "err", dt, (err or p.stdout.decode("utf-8", "replace"))[:200])

findings = []
ncases = 0
def check(label, res):
    global ncases
    ncases += 1
    status, dt, msg = res
    line = "%-48s %-8s %6.2fs %s" % (label[:48], status, dt, msg.strip().replace("\n", " | ")[:160])
    print(line, flush=True)
    if status not in ("ok", "err"):
        findings.append(line)

def load_tables(D):
    sys.dont_write_bytecode = True  # no __pycache__ next to diffh.py
    spec = importlib.util.spec_from_file_location("diffh", os.path.join(HERE, "diffh.py"))
    diffh = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(diffh)
    diffh.DATA = D
    diffh.write_data(diffh.build_tables(7))

def main():
    with tempfile.TemporaryDirectory(prefix="sqawk-fuzz-") as D:
        load_tables(D)
        statuses = fuzz(D)
    print("\nFINDINGS:")
    for f in findings:
        print(" ", f)
    print("FUZZ SUMMARY", {"garbage": statuses, "cases": ncases, "findings": len(findings)})

def fuzz(D):
    R = random.Random(99)
    files = [os.path.join(D, f) for f in ("a.csv", "b.csv", "d.csv", "e.csv", "dup.csv")]
    # 1. garbage SQL
    toks = ["SELECT", "FROM", "WHERE", "(", ")", ",", "a", "b", "d", "e", "*", "+", "-", "/", "%", "||", "=", "<", ">",
            "AND", "OR", "NOT", "NULL", "1", "0", "-9223372036854775808", "9223372036854775807", "'x'", "''", "1e999",
            "GROUP BY", "ORDER BY", "HAVING", "LIMIT", "OFFSET", "UNION", "JOIN", "ON", "LEFT", "CASE", "WHEN", "THEN",
            "ELSE", "END", "IN", "EXISTS", "OVER", "PARTITION BY", "ROWS", "count(*)", "sum(", "substr(", "printf(",
            "'%999999999d'", "'%.999999999f'", "INSERT INTO d VALUES", "UPDATE d SET k =", "DELETE FROM d", ";", "k", "x",
            "aid", "DISTINCT", "AS", "CAST(", "AS INTEGER)", "lag(", "ntile(", "row_number()", "date(", "'+1e18 days'",
            "zeroblob(", "char(", "1114112", "-1", "IS", "BETWEEN", "LIKE", "ESCAPE", "'%'", "WITH", "VALUES", "(SELECT"]
    statuses = {}
    t0 = time.time()
    for i in range(int(os.environ.get("NGARB", "1500"))):
        if i % 3 == 0:
            sql = bytes(R.randrange(256) for _ in range(R.randint(1, 80))).decode("latin-1")
        else:
            sql = " ".join(R.choice(toks) for _ in range(R.randint(2, 30)))
        sql = sql.replace("\x00", "")
        res = run(["-s", sql, *files], timeout=10)
        statuses[res[0]] = statuses.get(res[0], 0) + 1
        if res[0] not in ("ok", "err"):
            check("garbage#%d %r" % (i, sql[:200]), res)
    print("garbage SQL statuses:", statuses, "%.1fs" % (time.time() - t0))
    # 1b. targeted nasty scalar calls
    nasty = ["SELECT printf('%999999999d', 1)", "SELECT printf('%.999999999f', 1)", "SELECT printf('%2147483648d', 1)",
             "SELECT printf('%.2147483648f', 1)", "SELECT substr('abc', -9223372036854775808, 9223372036854775807)",
             "SELECT substr('abc', 9223372036854775807, 9223372036854775807)", "SELECT char(1114112)", "SELECT char(-1)", "SELECT char(55296)",
             "SELECT lpad('a', 9223372036854775807)", "SELECT rpad('a', 1000000000, 'xy')", "SELECT replace(printf('%1000000d', 1), ' ', printf('%1000d', 1))",
             "SELECT date('2024-01-01', '+1e18 days')", "SELECT date('2024-01-01', '+9223372036854775807 seconds')",
             "SELECT datetime(1e300, 'unixepoch')", "SELECT datetime(-1e300)", "SELECT julianday(9e18)", "SELECT strftime('%J', 1e308)",
             "SELECT ntile(9223372036854775807) OVER () FROM d", "SELECT lag(k, 9223372036854775807) OVER () FROM d",
             "SELECT lag(k, -9223372036854775808) OVER () FROM d", "SELECT nth_value(k, 9223372036854775807) OVER () FROM d",
             "SELECT sum(k) OVER (ROWS BETWEEN 9223372036854775807 PRECEDING AND 9223372036854775807 FOLLOWING) FROM d",
             "SELECT k FROM d LIMIT 9223372036854775807 OFFSET 9223372036854775807", "SELECT k FROM d LIMIT -9223372036854775808",
             "SELECT k FROM d ORDER BY 9223372036854775807", "SELECT k FROM d GROUP BY -9223372036854775808",
             "SELECT round(1.5, 9223372036854775807)", "SELECT round(1.5, -9223372036854775808)", "SELECT round(1e308, -308)",
             "SELECT 1 % -9223372036854775808, -9223372036854775808 % -1, -9223372036854775808 / -1",
             "SELECT abs(-9223372036854775808)", "SELECT -9223372036854775808 * -1", "SELECT CAST(1e400 AS INTEGER)",
             "SELECT CAST('nan' AS REAL)", "SELECT 0.0 / 0.0, 1e308 * 10 - 1e308 * 10", "SELECT max(1e308*10 - 1e308*10, 1)",
             "SELECT hex(printf('%100000000d', 1))", "SELECT length(printf('%999999999s', ''))",
             "SELECT unicode(char(0))", "SELECT 'a' LIKE printf('%5000s', '') || 'b'", "SELECT printf('%5000s', '') LIKE replace(printf('%60s', ''), ' ', '%_') ",
             "SELECT instr(printf('%1000000s', 'a'), printf('%500000s', 'a'))", "SELECT trim(printf('%1000000s', ''))",
             "SELECT group_concat(k, printf('%1000s', '')) FROM d", "SELECT 1 FROM d a1, d a2, d a3, d a4, d a5, d a6, d a7, d a8 LIMIT 1",
             "SELECT count(*) FROM d a1, d a2, d a3, d a4, d a5, d a6, d a7",
             "SELECT unixepoch('9999-12-31 23:59:59', '+1 second')", "SELECT strftime('%s', '0000-01-01', '-1 day')",
             "SELECT date(-1e18)", "SELECT date(1e18)", "SELECT time(-0.5)"]
    for q in (nasty if not os.environ.get("SKIPNASTY") else []):
        check(q, run(["-s", q, os.path.join(D, "d.csv")], timeout=15))
    # 2. deep nesting
    def big(label, sql, timeout=15):
        if len(sql) < 100000:
            check(label + " (%d bytes)" % len(sql), run(["-s", sql, os.path.join(D, "d.csv")], timeout=timeout))
        else:
            check(label + " (%d bytes, repl)" % len(sql), run(["-i", os.path.join(D, "d.csv")], timeout=timeout, stdin=(sql + ";\n.exit\n").encode()))
    for n in (100, 1000, 5000, 20000):
        big("parens %d" % n, "SELECT " + "(" * n + "1" + ")" * n)
    for n in (1000, 5000, 10000, 50000):
        big("plus chain %d" % n, "SELECT " + "+".join(["1"] * n))
        big("and chain %d" % n, "SELECT count(*) FROM d WHERE " + " AND ".join(["k IS NOT NULL OR 1"] * n))
        big("concat chain %d" % n, "SELECT " + "||".join(["'a'"] * n))
    big("NOT x10000", "SELECT " + "NOT " * 10000 + "1")
    big("unary minus x10000", "SELECT " + "- " * 10000 + "1")
    big("coalesce nest 3000", "SELECT " + "coalesce(" * 3000 + "1" + ", 2)" * 3000)
    big("case nest 2000", "SELECT " + "CASE WHEN 1 THEN " * 2000 + "1" + " END" * 2000)
    big("subquery nest 300", "SELECT " + "(SELECT " * 300 + "1" + ")" * 300)
    big("subquery nest 2000", "SELECT " + "(SELECT " * 2000 + "1" + ")" * 2000)
    big("from-subquery nest 500", "SELECT * FROM " + "(SELECT * FROM " * 500 + "d" + ")" * 500)
    big("IN list 100k", "SELECT count(*) FROM d WHERE k IN (" + ",".join(str(i) for i in range(100000)) + ")")
    big("IN list 100k strings", "SELECT count(*) FROM d WHERE label IN (" + ",".join("'s%d'" % i for i in range(100000)) + ")")
    big("UNION ALL 5000", " UNION ALL ".join("SELECT %d" % i for i in range(5000)))
    big("UNION 5000", " UNION ".join("SELECT %d" % i for i in range(5000)))
    big("select list 20000 cols", "SELECT " + ",".join("k+%d" % i for i in range(20000)) + " FROM d")
    big("10MB string literal", "SELECT length('" + "x" * 10_000_000 + "')")
    big("10k statements", ";".join(["SELECT 1"] * 10000))
    big("OR chain 10000", "SELECT count(*) FROM d WHERE " + " OR ".join("k = %d" % i for i in range(10000)))
    big("nested func 200", "SELECT " + "abs(" * 200 + "1" + ")" * 200)
    big("2000 inserts", ";".join(["INSERT INTO d VALUES (%d,'x',1)" % i for i in range(2000)]) + "; SELECT count(*) FROM d")
    big("join 64 tables", "SELECT count(*) FROM " + ", ".join("d t%d" % i for i in range(64)) + " WHERE " + " AND ".join("t%d.k = 1" % i for i in range(64)))
    big("window over 5000 partitions expr", "SELECT sum(k) OVER (PARTITION BY " + ",".join(["k"] * 2000) + ") FROM d")
    return statuses

if __name__ == "__main__":
    main()
