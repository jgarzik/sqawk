#!/usr/bin/env python3
"""Malformed and huge input robustness for sqawk's loader.

What it tests
-------------
* ~25 malformed files (empty, header only, ragged rows, unclosed or stray
  quotes, NUL bytes, invalid UTF-8, BOM, mixed CR/LF/CRLF, duplicate or empty
  header names, blank lines, i64 overflow / NaN / inf text, a .tsv with
  comments, a headerless .txt), each run with SELECT * and SELECT count(*);
* stdin input (CSV, empty, binary), the same file twice, a missing file, a
  directory, a table-name collision, a file name with a space, /dev/null;
* a 50 MB field, a 10,000-column table (count, SELECT *, GROUP BY, self
  join), and a --write UPDATE of one cell in that table, which must change
  exactly one line of the file.

Usage (from anywhere):

    cargo build --release
    python3 -I tests/sqlite_diff/csvfuzz.py

SQAWK overrides the binary (default <repo>/target/release/sqawk).  Files are
written into a temporary directory, removed at exit.

Output
------
One line per case: label, status (`ok`, `errN` = clean error exit N,
`PANIC`, `SIGNAL`, `HANG` = over 60 s), seconds, the start of stdout and the
last stderr line.  Then
    CSVFUZZ SUMMARY {'cases': N, 'ok': N, 'err': N, 'crash': N, 'writeback_bad': N}
A clean error (`sqawk: ...`, exit 1) on malformed input is expected
behaviour; a pass is crash == 0 (no PANIC / SIGNAL / HANG) and
writeback_bad == 0.  Crashing cases are listed under FAILURES.
"""
import os, re, subprocess, sys, tempfile, time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
SQAWK = os.environ.get("SQAWK", os.path.join(REPO, "target", "release", "sqawk"))

stats = {"cases": 0, "ok": 0, "err": 0, "crash": 0, "writeback_bad": 0}
failures = []

def run(args, timeout=60, stdin=None):
    t0 = time.time()
    try:
        p = subprocess.run([SQAWK, *args], capture_output=True, timeout=timeout, input=stdin)
    except subprocess.TimeoutExpired:
        return "HANG", time.time() - t0, b"", ""
    err = p.stderr.decode("utf-8", "replace")
    st = "ok" if p.returncode == 0 else ("SIGNAL" if p.returncode < 0 else "err%d" % p.returncode)
    if "panicked" in err or "stack overflow" in err:
        st = "PANIC"
    return st, time.time() - t0, p.stdout, err

def show(label, res, outlim=160):
    st, dt, out, err = res
    o = out.decode("utf-8", "replace").replace("\n", "\\n")[:outlim]
    e = [l for l in err.splitlines() if l.strip() and not l.startswith("\t")][-1:] if err else []
    line = "%-34s %-6s %6.2fs out=%r err=%r" % (label, st, dt, o, (e[0][:150] if e else ""))
    print(line, flush=True)
    stats["cases"] += 1
    if st == "ok":
        stats["ok"] += 1
    elif st.startswith("err") and st != "err101":
        stats["err"] += 1
    else:
        stats["crash"] += 1
        failures.append(line)

cases = {
    "empty.csv": b"",
    "newline_only.csv": b"\n",
    "header_only.csv": b"a,b,c\n",
    "header_only_nonl.csv": b"a,b,c",
    "ragged_short.csv": b"a,b,c\n1,2\n3,4,5\n",
    "ragged_long.csv": b"a,b\n1,2,3,4\n5,6\n",
    "unclosed_quote.csv": b"a,b\n1,\"abc\n2,3\n",
    "quote_mid.csv": b"a,b\n1,ab\"c\n2,\"x\"y\n",
    "nul_bytes.csv": b"a,b\n1,x\x00y\n2,\x00\n",
    "invalid_utf8.csv": b"a,b\n1,\xff\xfe\n2,caf\xe9\n",
    "bom.csv": b"\xef\xbb\xbfid,name\n1,x\n",
    "crlf_mixed.csv": b"a,b\r\n1,x\r\n2,y\n3,z\r\n",
    "cr_only.csv": b"a,b\r1,x\r2,y\r",
    "dup_header.csv": b"a,a,b\n1,2,3\n",
    "empty_header.csv": b",b,\n1,2,3\n",
    "space_header.csv": b"first name, age ,\"we,ird\"\n x , 5 ,q\n",
    "blank_lines.csv": b"a,b\n1,2\n\n3,4\n\n",
    "trailing_comma.csv": b"a,b,\n1,2,\n",
    "embedded_nl.csv": b"a,b\n1,\"line1\nline2\"\n2,\"x\"\"y\"\n",
    "only_quotes.csv": b"\"\"\n\"\"\n",
    "numeric_header.csv": b"1,2,3\n4,5,6\n",
    "comment_hash.csv": b"a,b\n#c,d\n1,2\n",
    "huge_int.csv": b"a\n99999999999999999999\n-9223372036854775809\n9223372036854775807\n1e999\nNaN\ninf\n-0\n0.1000\n",
    "tabs.tsv": b"a\tb\n1\t\"q\tq\"\n# comment\n2\tx\n",
    "noheader.txt": b"1 2 3\n4 5 6\n",
}

def main():
    with tempfile.TemporaryDirectory(prefix="sqawk-csvfuzz-") as W:
        def w(name, data):
            p = os.path.join(W, name)
            with open(p, "wb") as f:
                f.write(data)
            return p

        d_csv = w("d.csv", b"k,label,wt\n1,one,10\n2,two,20\n3,three,\n,nil,5\n")
        a_csv = w("a.csv", b"aid,k\n1,1\n2,2\n")
        print("== malformed inputs: SELECT *, then count(*)")
        for name, data in cases.items():
            p = w(name, data)
            t = re.sub(r"\..*", "", name)
            show(name + " *", run(["-s", "SELECT * FROM %s" % t, p]))
            show(name + " count", run(["-s", "SELECT count(*) FROM %s" % t, p]))
        print("== stdin and file arguments")
        show("stdin csv", run(["-s", "SELECT * FROM stdin", "-"], stdin=b"a,b\n1,2\n"))
        show("stdin empty", run(["-s", "SELECT * FROM stdin", "-"], stdin=b""))
        show("stdin binary", run(["-s", "SELECT count(*) FROM stdin", "-"], stdin=bytes(range(256)) * 100))
        show("same file twice", run(["-s", "SELECT count(*) FROM d", d_csv, d_csv]))
        show("missing file", run(["-s", "SELECT 1", os.path.join(W, "nope.csv")]))
        show("directory as file", run(["-s", "SELECT 1", W]))
        show("table name collision", run(["-s", "SELECT count(*) FROM x", "x=" + d_csv, "x=" + a_csv]))
        show("weird table name", run(["-s", "SELECT count(*) FROM \"my file\"", w("my file.csv", b"a\n1\n")]))
        show("dev/null", run(["-s", "SELECT count(*) FROM null", "/dev/null"]))
        print("== size")
        p = w("hugefield.csv", b"a,b\n1," + b"x" * 50_000_000 + b"\n")
        show("50MB field length", run(["-s", "SELECT length(b) FROM hugefield", p]))
        ncol = 10000
        hdr = ",".join("c%d" % i for i in range(ncol))
        rows = "\n".join(",".join(str(r * i) for i in range(ncol)) for r in range(20))
        p = w("wide.csv", (hdr + "\n" + rows + "\n").encode())
        show("10k cols count", run(["-s", "SELECT count(*), sum(c9999) FROM wide", p]))
        show("10k cols select *", run(["-s", "SELECT * FROM wide", p]), outlim=40)
        show("10k cols group", run(["-s", "SELECT c1 % 3, count(*) FROM wide GROUP BY 1", p]))
        show("10k cols self join", run(["-s", "SELECT count(*) FROM wide x JOIN wide y ON x.c5 = y.c5", p]))
        q = w("wide_copy.csv", open(p, "rb").read())
        show("10k cols update --write", run(["-w", "-s", "UPDATE wide_copy SET c5000 = 0 WHERE c1 = 3", q]))
        a, b = open(p, "rb").read().split(b"\n"), open(q, "rb").read().split(b"\n")
        ndiff = sum(1 for x, y in zip(a, b) if x != y)
        print("   wide writeback: lines differ = %d (expected 1), line count %d vs %d" % (ndiff, len(a), len(b)))
        if ndiff != 1 or len(a) != len(b):
            stats["writeback_bad"] += 1
    if failures:
        print("FAILURES:")
        for f in failures:
            print(" ", f)
    print("CSVFUZZ SUMMARY", stats)

if __name__ == "__main__":
    main()
