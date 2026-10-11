#!/usr/bin/env python3
"""Writeback (--write) round-trip fuzzer for sqawk.

What it tests
-------------
Each iteration writes a random delimited file (.csv comma, .tsv / .txt tab;
LF or CRLF; optional UTF-8 BOM; optional missing final newline; ragged
rows; `#` comment lines under --comments; cells with quotes, embedded
delimiters / LF / CRLF, leading/trailing space, numeric-looking text such as
`007`, `1.50`, `-0`, `1e3`, `NaN`, i64 overflow, Unicode), then runs random
UPDATE / DELETE / INSERT statements on it with `-w` and checks that:
  * every record no statement touched is still in the file byte for byte,
    in order (and parses to the same values);
  * the BOM and the `#` comment lines are kept;
  * re-reading the file gives exactly the expected ids (deleted rows gone,
    the inserted row present);
  * no LF-only line ending appears in a CRLF file.

Usage (from anywhere):

    cargo build --release
    python3 -I tests/sqlite_diff/wb.py [SEED [ITERATIONS]]

SEED defaults to 5, ITERATIONS to 300.  SQAWK overrides the binary
(default <repo>/target/release/sqawk).  Files are written into a temporary
directory, removed at exit.

Output
------
One BAD block per failing iteration (the problems found plus the start of
the original and rewritten file), one ERR line per iteration where sqawk
exited non-zero, then
    WRITEBACK SUMMARY {'runs': N, 'ok': N, 'bad': N, 'sqawk_err': N}
A pass is bad == 0 and sqawk_err == 0 (ok == runs); `value_changed`
appears when an untouched row parsed to different values.
"""
import csv, io, os, random, subprocess, sys, tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
SQAWK = os.environ.get("SQAWK", os.path.join(REPO, "target", "release", "sqawk"))
BOM = chr(0xFEFF)

CELLS = ["abc", "\"abc\"", "\"a,b\"", "\"line1\nline2\"", "\"q\"\"q\"", "007", "1.50", "\" 5\"", " 5", "trail ", "", "\"\"",
         "héllo", "日本", "-0", "1e3", "NaN", "inf", "0x10", "+3", "12.0", "1.5", "42", "-7", "9223372036854775808",
         "\"\"\"\"", "a\"b", "\t", "#notcomment", "\"multi\r\nline\"", "NULL", "true"]

def gen_file(R, path, delim, eol, bom, header, final_eol, ragged, tsv_comments):
    ncols = R.randint(2, 6)
    recs = []  # (id, raw_text_with_eol)
    hdr = delim.join(["id"] + ["c%d" % i for i in range(1, ncols)])
    out = (BOM if bom else "") + (hdr + eol if header else "")
    n = R.randint(5, 25)
    for i in range(1, n + 1):
        cells = [str(i)]
        k = ncols - 1 - (R.randint(0, 1) if ragged and R.random() < 0.3 else 0)
        for _ in range(k):
            c = R.choice(CELLS)
            if delim == "\t":
                c = c.replace("\t", "x")
            c = c.replace(",", ";") if delim == "\t" and not c.startswith('"') else c
            if delim != "," and c.startswith('"'):
                c = c.replace(",", ";")
            cells.append(c)
        raw = delim.join(cells)
        term = eol if (i < n or final_eol) else ""
        if tsv_comments and R.random() < 0.2:
            out += "# a comment, " + str(i) + eol
        recs.append((i, raw + term))
        out += raw + term
    with open(path, "w", newline="", encoding="utf-8") as f:
        f.write(out)
    return out, recs, ncols

def run(args):
    p = subprocess.run([SQAWK, *args], capture_output=True, timeout=30)
    return p.returncode, p.stdout.decode("utf-8", "replace"), p.stderr.decode("utf-8", "replace")

def main():
    R = random.Random(int(sys.argv[1]) if len(sys.argv) > 1 else 5)
    iterations = int(sys.argv[2]) if len(sys.argv) > 2 else 300
    stats = {"runs": 0, "ok": 0, "bad": 0, "sqawk_err": 0}
    with tempfile.TemporaryDirectory(prefix="sqawk-wb-") as W:
        for it in range(iterations):
            one(R, W, it, stats)
    print("WRITEBACK SUMMARY", stats)

def one(R, W, it, stats):
    kind = R.choice(["csv", "tsv", "txt"])
    delim = "," if kind == "csv" else "\t"
    eol = R.choice(["\n", "\r\n"])
    bom = R.random() < 0.15
    header = True
    final_eol = R.random() < 0.8
    ragged = R.random() < 0.2
    comments = kind != "csv" and R.random() < 0.4
    path = os.path.join(W, "t%d.%s" % (it, kind))
    tname = "t%d" % it
    orig, recs, ncols = gen_file(R, path, delim, eol, bom, header, final_eol, ragged, comments)
    ids = [i for i, _ in recs]
    upd = set(R.sample(ids, R.randint(0, min(3, len(ids)))))
    dele = set(R.sample([i for i in ids if i not in upd], R.randint(0, 2)))
    stmts = []
    col = "c%d" % R.randint(1, ncols - 1)
    if upd:
        stmts.append("UPDATE %s SET %s = %s WHERE id IN (%s)" % (tname, col, R.choice(["'X'", "NULL", "1.0", "'a,\"b'", "'007'", "length(%s)" % col, col]), ",".join(map(str, upd))))
    if dele:
        stmts.append("DELETE FROM %s WHERE id IN (%s)" % (tname, ",".join(map(str, dele))))
    ins = R.random() < 0.5
    if ins:
        stmts.append("INSERT INTO %s (id) VALUES (999)" % tname)
    if not stmts:
        stmts.append("UPDATE %s SET id = id WHERE 0" % tname)
    # File-type rules: .tsv is tab, everything else comma unless -F;
    # '#' lines are comments only under --comments.
    extra = ["-F", "\t"] if kind == "txt" or (kind == "tsv" and R.random() < 0.5) else []
    if comments:
        extra.append("--comments")
    rc, out, err = run(extra + ["-w", "-s", "; ".join(stmts), "%s=%s" % (tname, path)])
    stats["runs"] += 1
    if rc != 0:
        stats["sqawk_err"] += 1
        print("ERR it=%d %s: %s" % (it, kind, err.strip().splitlines()[-1] if err.strip() else rc))
        return
    new = open(path, "r", newline="", encoding="utf-8", errors="replace").read()
    problems = []
    if bom and not new.startswith(BOM):
        problems.append("BOM lost")
    # untouched records, byte for byte and in order
    pos = 0
    for i, raw in recs:
        if i in upd or i in dele:
            continue
        j = new.find(raw, pos)
        if j < 0:
            problems.append("record id=%d not byte-identical: %r" % (i, raw[:80]))
            break
        pos = j + len(raw)
    # value-level comparison of untouched rows
    def parse(txt):
        txt = txt.lstrip(BOM)
        lines = txt if kind == "csv" else "".join(l for l in io.StringIO(txt, newline="") if not l.startswith("#"))
        rows = list(csv.reader(io.StringIO(lines, newline=""), delimiter=delim))
        out = {}
        for r in rows[1:]:
            if r:
                while r and r[-1] == "":
                    r = r[:-1]
                out[r[0]] = r
        return out
    po, pn = parse(orig), parse(new)
    for i in ids:
        if i in upd or i in dele:
            continue
        if po.get(str(i)) != pn.get(str(i)):
            problems.append("VALUE CHANGED id=%d: %r -> %r" % (i, po.get(str(i)), pn.get(str(i))))
            stats["value_changed"] = stats.get("value_changed", 0) + 1
            break
    # comments kept
    if comments:
        for line in orig.split(eol):
            if line.startswith("# a comment") and line not in new:
                problems.append("comment lost: %r" % line)
    # deleted rows gone, inserted row present
    rc2, out2, _ = run(extra + ["-s", "SELECT id FROM %s" % tname, "%s=%s" % (tname, path)])
    got = [r[0] for r in csv.reader(io.StringIO(out2))][1:]
    exp = [str(i) for i in ids if i not in dele] + (["999"] if ins else [])
    if got != exp:
        problems.append("ids after write %s != %s" % (got[:30], exp[:30]))
    if eol == "\r\n" and "\n" in new.replace("\r\n", "") and "line1\nline2" not in orig:
        problems.append("mixed line endings introduced")
    if problems:
        stats["bad"] += 1
        print("BAD it=%d kind=%s eol=%r bom=%s final_eol=%s ragged=%s comments=%s\n  stmts=%s\n  %s" % (
            it, kind, eol, bom, final_eol, ragged, comments, stmts, "\n  ".join(problems[:4])))
        print("  ORIG:", repr(orig[:400]))
        print("  NEW :", repr(new[:400]))
    else:
        stats["ok"] += 1

if __name__ == "__main__":
    main()
