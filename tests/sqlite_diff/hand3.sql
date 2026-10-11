-- DML sequences
INSERT INTO d SELECT a.k, b.w, b.v FROM a JOIN b ON a.aid = b.aid WHERE b.v > 0; SELECT * FROM d
INSERT INTO d (label) VALUES ('only'); SELECT * FROM d
INSERT INTO d (wt, k) VALUES (1, 2), (3, 4), (NULL, NULL); SELECT * FROM d
INSERT INTO d VALUES (9, 'nine', 90); INSERT INTO d SELECT k + 100, label || '!', wt FROM d; SELECT * FROM d
INSERT INTO d SELECT * FROM d; INSERT INTO d SELECT * FROM d; SELECT count(*), sum(wt) FROM d
INSERT INTO d SELECT k, label, wt FROM d ORDER BY wt DESC LIMIT 2; SELECT * FROM d
INSERT INTO d SELECT k, max(label), sum(wt) FROM d GROUP BY k; SELECT * FROM d
INSERT INTO d SELECT 1, 2; SELECT * FROM d
INSERT INTO d VALUES ((SELECT max(aid) FROM a), (SELECT min(s) FROM a), (SELECT count(*) FROM d)); SELECT * FROM d
INSERT INTO e SELECT aid, k, x FROM a WHERE x > 0; SELECT * FROM e
INSERT INTO e SELECT aid, k, x FROM a WHERE x > 0; DELETE FROM e WHERE k IN (SELECT k FROM d WHERE wt > 15); SELECT * FROM e
UPDATE d SET wt = (SELECT max(v) FROM b WHERE b.k = d.k); SELECT * FROM d
UPDATE d SET wt = (SELECT count(*) FROM a WHERE a.k = d.k), label = upper(label) WHERE k IS NOT NULL; SELECT * FROM d
UPDATE d SET k = wt, wt = k; SELECT * FROM d
UPDATE d SET wt = wt + 1, wt = wt + 2; SELECT * FROM d
UPDATE d SET wt = NULL WHERE wt > 100; SELECT * FROM d
UPDATE d SET label = label || label WHERE k IN (SELECT k FROM a WHERE x > 0); SELECT * FROM d
UPDATE d SET wt = wt * 2 WHERE EXISTS (SELECT 1 FROM b WHERE b.k = d.k AND b.v > 5); SELECT * FROM d
UPDATE d SET wt = 9223372036854775807 + wt; SELECT * FROM d
UPDATE d SET wt = (SELECT max(wt) FROM d); SELECT * FROM d
UPDATE d SET wt = (SELECT sum(x.wt) FROM d x WHERE x.k = d.k); SELECT * FROM d
UPDATE d SET nosuch = 1; SELECT * FROM d
DELETE FROM d WHERE k IN (SELECT k FROM a WHERE g = 'red'); SELECT * FROM d
DELETE FROM d WHERE k NOT IN (SELECT k FROM a); SELECT * FROM d
DELETE FROM d WHERE k NOT IN (SELECT k FROM a WHERE k IS NOT NULL); SELECT * FROM d
DELETE FROM d WHERE wt > (SELECT avg(wt) FROM d); SELECT * FROM d
DELETE FROM d WHERE wt = (SELECT max(x.wt) FROM d x WHERE x.k = d.k); SELECT * FROM d
DELETE FROM d; SELECT count(*), sum(wt) FROM d
DELETE FROM d; INSERT INTO d VALUES (1, 'x', 2); SELECT * FROM d
DELETE FROM dup WHERE p = 1; SELECT * FROM dup
DELETE FROM dup WHERE q IS NULL; INSERT INTO dup SELECT p, q FROM dup; SELECT * FROM dup
DELETE FROM a WHERE aid IN (SELECT aid FROM b) OR x IS NULL; SELECT aid FROM a
UPDATE a SET s = NULL WHERE s LIKE '%a%'; SELECT aid, s FROM a
UPDATE a SET y = y / 0, x = -x; SELECT aid, x, y FROM a
-- CTAS / ALTER
CREATE TABLE t9 AS SELECT k, count(*) AS c, sum(x) AS sx FROM a GROUP BY k; SELECT * FROM t9
CREATE TABLE t9 AS SELECT a.aid, b.bid, a.s FROM a JOIN b ON a.aid = b.aid; SELECT * FROM t9
CREATE TABLE t9 AS SELECT * FROM e; INSERT INTO t9 VALUES (1, 2, 3); SELECT * FROM t9
CREATE TABLE t9 AS SELECT aid, x * 1.5 AS v FROM a; SELECT aid, v, typeof(v) FROM t9
CREATE TABLE t9 AS SELECT 1 AS one, 'x' AS ex, NULL AS nn; SELECT * FROM t9
CREATE TABLE t9 AS SELECT * FROM a WHERE 0; SELECT count(*) FROM t9
CREATE TABLE t9 AS SELECT k, k FROM d; SELECT * FROM t9
CREATE TABLE t9 AS SELECT aid, row_number() OVER (ORDER BY aid DESC) AS rn FROM a; SELECT * FROM t9
CREATE TABLE t9 AS SELECT * FROM d; UPDATE t9 SET wt = 0; SELECT * FROM d
CREATE TABLE t9 (a INTEGER, b TEXT); INSERT INTO t9 VALUES (1, 'x'), (2, NULL); SELECT * FROM t9
CREATE TABLE t9 (a INTEGER, b TEXT); INSERT INTO t9 VALUES ('10', 5); SELECT a, typeof(a), b, typeof(b) FROM t9
CREATE TABLE t9 (a INTEGER, b TEXT); INSERT INTO t9 SELECT k, label FROM d; SELECT * FROM t9 WHERE a > 1
CREATE TABLE d (z INT); SELECT * FROM d
ALTER TABLE d ADD COLUMN extra INTEGER; SELECT * FROM d
ALTER TABLE d ADD COLUMN extra TEXT; UPDATE d SET extra = label || k; SELECT * FROM d
ALTER TABLE d ADD COLUMN extra INT; INSERT INTO d VALUES (7, 'seven', 70, 700); SELECT * FROM d
ALTER TABLE d ADD COLUMN extra INT; INSERT INTO d (k) VALUES (8); SELECT * FROM d
ALTER TABLE d ADD COLUMN label TEXT; SELECT * FROM d
ALTER TABLE e ADD COLUMN y REAL; INSERT INTO e VALUES (1, 2, 3, 4.5); SELECT * FROM e
DROP TABLE d; SELECT * FROM d
DROP TABLE IF EXISTS nosuch; SELECT 1
-- NULL heavy
SELECT NULL + 1, NULL || 'a', NULL = NULL, NULL AND 0, NULL OR 1, NULL AND 1, NULL OR 0, NOT NULL
SELECT coalesce(NULL, NULL), coalesce(NULL, 1, 2), ifnull(NULL, 'x'), nullif(1, 1), nullif(1, 2), nullif(NULL, 1), iif(NULL, 1, 2)
SELECT coalesce(x, y, k, 0), ifnull(s, '<null>'), nullif(g, 'red'), iif(x > 0, 'pos', iif(x < 0, 'neg', iif(x = 0, 'zero', 'null'))) FROM a
SELECT CASE WHEN x IS NULL THEN coalesce(y, -1) WHEN x > 0 THEN CASE WHEN k > 2 THEN x ELSE -x END ELSE nullif(x, 0) END FROM a
SELECT coalesce(nullif(coalesce(s, g), ''), iif(n IS NULL, 'nn', n)) FROM a
SELECT count(*), count(x), count(y), count(s), count(g), count(n), count(m) FROM a
SELECT x, count(*) FROM a GROUP BY x
O:SELECT aid, x FROM a ORDER BY x, aid
O:SELECT aid, x FROM a ORDER BY x DESC, aid
O:SELECT aid, x FROM a ORDER BY x NULLS LAST, aid
O:SELECT aid, x FROM a ORDER BY x DESC NULLS FIRST, aid
O:SELECT aid, s FROM a ORDER BY s, aid
O:SELECT aid, y FROM a ORDER BY y DESC, aid LIMIT 5
O:SELECT aid, m FROM a ORDER BY m, aid
O:SELECT aid, n FROM a ORDER BY n, aid
O:SELECT DISTINCT g FROM a ORDER BY g
O:SELECT aid FROM a ORDER BY aid LIMIT -1
O:SELECT aid FROM a ORDER BY aid LIMIT 3 OFFSET -2
O:SELECT aid FROM a ORDER BY aid LIMIT 0
O:SELECT aid FROM a ORDER BY aid LIMIT '3'
O:SELECT aid FROM a ORDER BY aid LIMIT 2.5
O:SELECT aid FROM a ORDER BY aid LIMIT 2, 3
O:SELECT aid, k FROM a ORDER BY k COLLATE NOCASE, aid
O:SELECT s FROM a WHERE s IS NOT NULL ORDER BY lower(s), s
O:SELECT aid FROM a ORDER BY -aid LIMIT 3
O:SELECT aid AS x FROM a ORDER BY x LIMIT 3
O:SELECT aid, x AS aid FROM a ORDER BY aid LIMIT 3
O:SELECT k, aid FROM a ORDER BY 1 DESC, 2 DESC
SELECT aid FROM a ORDER BY 9
SELECT max(x) FROM a WHERE x < 9223372036854775807
SELECT min(m), max(m) FROM a
SELECT DISTINCT m FROM a
SELECT DISTINCT n FROM a
SELECT n, count(*) FROM a GROUP BY n
SELECT m, count(*) FROM a GROUP BY m
SELECT DISTINCT y * 0 FROM a
SELECT DISTINCT k, k * 1.0 FROM a
SELECT k * 1.0 FROM a UNION SELECT k FROM a
-- date / time
SELECT ts, mod, date(ts), time(ts), datetime(ts), date(ts, mod), datetime(ts, mod), julianday(ts), unixepoch(ts) FROM dt
SELECT ts, strftime('%Y-%m-%d %H:%M:%S %j %w %W %s', ts), strftime('%f %J', ts) FROM dt
SELECT date('2024-02-29', '+1 year'), date('2024-03-31', '-1 month'), date('2024-01-31', '+1 month'), date('2023-01-31', '+1 month', 'start of month')
SELECT date('2024-13-01'), date('2024-02-30'), date('2024-02-31'), date('2024-02-32'), date('2024-00-10'), date('2023-02-29'), time('24:00'), time('23:60')
SELECT datetime('2024-01-01 10:00:00+05:30'), datetime('2024-01-01T10:00Z'), datetime('2024-01-01 10:00:00-01:15'), datetime('2024-01-01 10:00:00.123456')
SELECT datetime(1700000000, 'unixepoch'), datetime(1700000000.5, 'unixepoch'), datetime(-1, 'unixepoch'), date(2460000.5), datetime(0), julianday('2000-01-01 12:00')
SELECT strftime('%s', '1969-12-31'), strftime('%s', '2038-01-19 03:14:08'), unixepoch('1970-01-01 00:00:01.9'), strftime('%f', '2024-01-01 00:00:01.999')
SELECT date('2024-06-15', 'weekday 0'), date('2024-06-16', 'weekday 0'), date('2024-06-15', 'weekday 6'), date('2024-06-15', 'weekday 7'), date('2024-06-15', 'weekday -1')
SELECT date('2024-01-01', '+1.5 days'), datetime('2024-01-01', '+1.5 months'), datetime('2024-01-01', '-0.5 years'), datetime('2024-01-01', '+25 hours'), datetime('2024-01-01', '-90 minutes'), datetime('2024-01-01', '+3661 seconds')
SELECT date('2024-06-15', 'start of year', '-1 day'), date('2024-06-15', 'start of month', '+1 month', '-1 day'), datetime('2024-06-15 13:14:15', 'start of day')
SELECT date('2024-01-01', '+1 days', '+1 days', '+1 months', '-1 years'), date('2024-01-01', '+0001-02-03'), datetime('2024-01-01', '+01:30'), date('2024-01-01', '+1 fortnight')
SELECT date('0000-01-01', '-1 day'), date('9999-12-31', '+1 day'), date('-0001-01-01'), date('10000-01-01'), datetime('2024-01-01 25:00'), date(''), date(NULL), date('now', 'xyz')
SELECT date('2024-01-01', 'unixepoch'), datetime(1e10, 'unixepoch'), datetime(253402300800, 'unixepoch'), datetime(5373484.5), datetime(5373485), datetime(-1)
SELECT strftime('%Y', '2024-01-01', '+1 year'), strftime('%H:%M', '12:34:56'), strftime('%%|%d', '2024-05-06'), strftime('%Q', '2024-01-01'), strftime(NULL, '2024-01-01'), strftime('%Y', NULL)
SELECT strftime('%W', '2024-01-01'), strftime('%W', '2023-01-01'), strftime('%W', '2024-12-31'), strftime('%j', '2024-12-31'), strftime('%w', '2024-12-31'), strftime('%u', '2024-12-31')
SELECT julianday('2024-01-01') - julianday('2023-01-01'), julianday('-4713-11-24 12:00'), julianday('2024-01-01', 'utc'), datetime('2024-01-01', 'localtime')
SELECT time('12:30:45.678'), time('12:30'), time('2024-01-01 12:30'), datetime('12:30'), date('12:30'), time(0.5), time('T12:30')
SELECT date(20240101), date('20240101'), date('2024-1-1'), date(' 2024-01-01'), date('2024-01-01 '), datetime('2024-01-01  10:00'), date('2024/01/01')
SELECT datetime('2024-03-10 12:00', '+0.000001 days'), unixepoch('2024-01-01', 'subsec'), datetime('2024-01-01', 'auto'), date(2460000.5, 'julianday'), datetime(1700000000, 'auto')
SELECT date(ts, '+1 day', 'start of month') FROM dt WHERE did < 6
