-- aggregates / group by
SELECT count(DISTINCT p), count(DISTINCT q), count(p), count(*), sum(DISTINCT p), avg(DISTINCT p), group_concat(DISTINCT q) FROM dup
SELECT p, q, count(*) FROM dup GROUP BY p, q
SELECT DISTINCT p, q FROM dup
SELECT DISTINCT q FROM dup
SELECT p FROM dup UNION SELECT p FROM dup
SELECT p, q FROM dup INTERSECT SELECT p, q FROM dup
SELECT p, q FROM dup EXCEPT SELECT 1, 'a'
SELECT count(*) FROM (SELECT DISTINCT p, q FROM dup)
SELECT g, count(*), sum(x), min(x), max(x), count(DISTINCT k) FROM a GROUP BY g
SELECT g, count(*) FROM a GROUP BY 1 HAVING count(*) > 2
SELECT k % 2 AS par, count(*), sum(y) FROM a GROUP BY par
SELECT k % 2, count(*) FROM a GROUP BY k % 2 HAVING sum(y) > 0
SELECT upper(g) AS ug, count(*) FROM a GROUP BY ug
SELECT k, max(y), s FROM a GROUP BY k
SELECT k, min(aid), s FROM a GROUP BY k
SELECT g, max(aid), s, n FROM a GROUP BY g
SELECT max(aid), s FROM a
SELECT min(y), aid FROM a
SELECT count(*), aid FROM a WHERE k = 99
SELECT g, avg(x) FROM a GROUP BY g HAVING avg(x) IS NULL
SELECT k, group_concat(aid) FROM (SELECT * FROM a ORDER BY aid) GROUP BY k
SELECT k, group_concat(aid, '; ') FROM (SELECT * FROM a ORDER BY aid DESC) GROUP BY k
SELECT group_concat(s, '') FROM (SELECT s FROM a ORDER BY aid)
SELECT string_agg(g, '|') FROM (SELECT g FROM a ORDER BY aid)
SELECT k, count(*) FILTER (WHERE x > 0), sum(x) FILTER (WHERE g = 'red'), count(*) FROM a GROUP BY k
SELECT count(*) FILTER (WHERE 0), sum(1) FILTER (WHERE NULL), total(1) FILTER (WHERE 0) FROM a
SELECT g, count(*) FROM a WHERE g IS NOT NULL GROUP BY g ORDER BY count(*) DESC, g
O:SELECT g, count(*) AS c FROM a GROUP BY g ORDER BY c DESC, g NULLS LAST
O:SELECT k, sum(x) AS sx FROM a GROUP BY k ORDER BY sx NULLS FIRST, k
SELECT count(*) FROM a GROUP BY NULL
SELECT 1 FROM a GROUP BY k HAVING 1
SELECT sum(x) + count(*), max(y) - min(y), avg(k) * 2 FROM a
SELECT k, sum(x) / count(*), sum(y) / count(y) FROM a GROUP BY k
SELECT (SELECT count(*) FROM b WHERE b.aid = a.aid) AS nb, count(*) FROM a GROUP BY nb
SELECT k, count(*) FROM a GROUP BY k HAVING max(y) > (SELECT avg(z) FROM c)
SELECT min(s), max(s), min(g), max(g) FROM a
SELECT min(w), max(w) FROM b
SELECT max(t), min(t) FROM u
SELECT k, avg(y), sum(y), total(y) FROM a GROUP BY k
SELECT aid, count(*) FROM a LEFT JOIN b USING (aid) GROUP BY aid
SELECT count(*), count(b.bid), count(DISTINCT b.k) FROM a LEFT JOIN b ON a.aid = b.aid
-- window functions
O:SELECT aid, k, row_number() OVER (PARTITION BY k ORDER BY aid), rank() OVER (ORDER BY k), dense_rank() OVER (ORDER BY k) FROM a ORDER BY aid
O:SELECT aid, k, sum(x) OVER (ORDER BY k) FROM a ORDER BY aid
O:SELECT aid, sum(aid) OVER (ORDER BY aid ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), avg(aid) OVER (ORDER BY aid ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM a ORDER BY aid
O:SELECT aid, sum(aid) OVER (ORDER BY aid ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), count(*) OVER (ORDER BY aid ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM a ORDER BY aid
O:SELECT aid, sum(aid) OVER (ORDER BY aid ROWS BETWEEN 3 FOLLOWING AND 5 FOLLOWING), max(aid) OVER (ORDER BY aid ROWS BETWEEN 5 PRECEDING AND 2 PRECEDING) FROM a ORDER BY aid
O:SELECT aid, k, sum(aid) OVER (PARTITION BY k ORDER BY aid RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM a ORDER BY aid
O:SELECT aid, k, sum(aid) OVER (ORDER BY k RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM a ORDER BY aid
O:SELECT aid, lag(aid) OVER (ORDER BY aid), lead(aid, 2) OVER (ORDER BY aid), lag(aid, 3, -1) OVER (ORDER BY aid), lead(s, 1, 'none') OVER (ORDER BY aid) FROM a ORDER BY aid
O:SELECT aid, ntile(3) OVER (ORDER BY aid), ntile(7) OVER (ORDER BY aid), ntile(100) OVER (ORDER BY aid) FROM a ORDER BY aid
O:SELECT aid, k, percent_rank() OVER (ORDER BY k), cume_dist() OVER (ORDER BY k) FROM a ORDER BY aid
O:SELECT aid, k, first_value(aid) OVER (PARTITION BY k ORDER BY aid), last_value(aid) OVER (PARTITION BY k ORDER BY aid), nth_value(aid, 2) OVER (PARTITION BY k ORDER BY aid) FROM a ORDER BY aid
O:SELECT aid, last_value(aid) OVER (ORDER BY aid ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING), nth_value(aid, 3) OVER (ORDER BY aid ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM a ORDER BY aid
O:SELECT aid, k, min(y) OVER (PARTITION BY k), max(y) OVER (PARTITION BY k), count(y) OVER (PARTITION BY k), avg(y) OVER (PARTITION BY k) FROM a ORDER BY aid
O:SELECT aid, x, rank() OVER (ORDER BY x), rank() OVER (ORDER BY x DESC), rank() OVER (ORDER BY x NULLS LAST), dense_rank() OVER (ORDER BY x DESC NULLS FIRST) FROM a ORDER BY aid
O:SELECT k, count(*), rank() OVER (ORDER BY count(*) DESC), sum(count(*)) OVER (), sum(sum(x)) OVER (ORDER BY k) FROM a GROUP BY k ORDER BY k
O:SELECT g, sum(y), row_number() OVER (ORDER BY sum(y) DESC) FROM a GROUP BY g HAVING count(*) > 1 ORDER BY 3
O:SELECT aid, row_number() OVER w, rank() OVER (w ORDER BY aid DESC) FROM a WINDOW w AS (PARTITION BY k) ORDER BY aid
O:SELECT aid FROM a ORDER BY row_number() OVER (ORDER BY y DESC, aid)
O:SELECT aid, round(avg(y) OVER (ORDER BY aid), 3), sum(y) OVER () - y FROM a ORDER BY aid
SELECT eid, row_number() OVER (ORDER BY eid), sum(x) OVER () FROM e
SELECT count(*) OVER () FROM a WHERE 0
O:SELECT a.aid, b.bid, row_number() OVER (PARTITION BY a.aid ORDER BY b.bid) FROM a JOIN b ON a.aid = b.aid ORDER BY 1, 2
SELECT * FROM (SELECT aid, rank() OVER (ORDER BY k) AS r FROM a) WHERE r = 1
SELECT k, max(r) FROM (SELECT k, row_number() OVER (PARTITION BY k ORDER BY aid) AS r FROM a) GROUP BY k
O:SELECT p, q, row_number() OVER (ORDER BY p, q), rank() OVER (ORDER BY p, q), dense_rank() OVER (ORDER BY p, q) FROM dup ORDER BY 4, 1, 2
O:SELECT aid, sum(x) OVER (ORDER BY aid) FROM a ORDER BY aid
SELECT sum(x) OVER (ORDER BY aid ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM a
O:SELECT aid, count(DISTINCT k) OVER (ORDER BY aid) FROM a ORDER BY aid
O:SELECT aid, lag(aid, 0) OVER (ORDER BY aid), lag(aid, -1) OVER (ORDER BY aid), nth_value(aid, 1) OVER (ORDER BY aid) FROM a ORDER BY aid
O:SELECT aid, ntile(0) OVER (ORDER BY aid) FROM a ORDER BY aid
O:SELECT aid, lag(aid, NULL) OVER (ORDER BY aid) FROM a ORDER BY aid
-- joins
SELECT * FROM a JOIN b USING (aid)
SELECT * FROM a NATURAL JOIN b
SELECT * FROM a NATURAL LEFT JOIN b
SELECT * FROM b NATURAL JOIN c
SELECT * FROM a JOIN b USING (aid) JOIN c USING (bid)
SELECT * FROM a JOIN b USING (aid, k)
SELECT * FROM a LEFT JOIN b USING (aid) LEFT JOIN c USING (bid)
SELECT * FROM d RIGHT JOIN a ON a.k = d.k
SELECT * FROM d FULL JOIN a ON a.k = d.k
SELECT * FROM d FULL JOIN a USING (k)
SELECT k, label, aid FROM d FULL JOIN a USING (k)
SELECT * FROM d NATURAL FULL JOIN dup
SELECT * FROM d NATURAL RIGHT JOIN a
SELECT d.k, a.k, k FROM d RIGHT JOIN a USING (k)
SELECT k FROM d LEFT JOIN a USING (k)
SELECT * FROM a, d WHERE a.k = d.k AND d.wt > 10
SELECT * FROM a CROSS JOIN e
SELECT * FROM e LEFT JOIN a ON 1
SELECT * FROM a LEFT JOIN e ON 1
SELECT * FROM a FULL JOIN e ON a.k = e.k
SELECT * FROM e FULL JOIN e AS e2 ON 1
SELECT a.aid, d.label FROM a LEFT JOIN d ON a.k = d.k AND d.wt > 15
SELECT a.aid, d.label FROM a LEFT JOIN d ON a.k = d.k WHERE d.wt > 15
SELECT a.aid FROM a LEFT JOIN d ON a.k = d.k WHERE d.k IS NULL
SELECT a.aid, d.label FROM a LEFT JOIN d ON 0
SELECT x1.aid, x2.aid FROM a x1 JOIN a x2 ON x1.k = x2.k AND x1.aid < x2.aid
SELECT a.aid, s.kr FROM a JOIN (SELECT k * 1.0 AS kr FROM d) s ON a.k = s.kr
SELECT a.aid, s.kr FROM a JOIN (SELECT k * 1.0 AS kr FROM d) s ON a.k = s.kr OR 0
SELECT a.aid, s.kt FROM a JOIN (SELECT CAST(k AS TEXT) AS kt FROM d) s ON a.k = s.kt
SELECT a.aid, s.kt FROM a JOIN (SELECT CAST(k AS TEXT) AS kt FROM d) s ON a.k = s.kt OR 0
SELECT a.aid, d.k FROM a JOIN d ON a.n = d.k
SELECT a.aid, d.k FROM a JOIN d ON a.n = d.k OR 0
SELECT a.aid, d.k FROM a, d WHERE a.n = d.k
SELECT a.aid, b.bid FROM a JOIN b ON a.aid = b.aid JOIN c ON b.bid = c.bid JOIN d ON c.k = d.k
SELECT a.aid, b.bid, c.cid, d.k FROM a LEFT JOIN b ON a.aid = b.aid LEFT JOIN c ON b.bid = c.bid LEFT JOIN d ON c.k = d.k
SELECT a.aid, b.bid, c.cid FROM a JOIN b ON a.aid = b.aid RIGHT JOIN c ON b.bid = c.bid
SELECT a.aid, b.bid, c.cid FROM a LEFT JOIN b ON a.aid = b.aid CROSS JOIN c WHERE c.cid < 203
SELECT a.aid, b.bid FROM a JOIN b ON b.aid = a.aid AND b.k = a.k
SELECT a.aid, b.bid FROM a JOIN b ON a.aid = b.aid + 0
SELECT count(*) FROM a JOIN b ON a.y = b.v
SELECT count(*) FROM a JOIN c ON a.y = c.z
SELECT count(*) FROM a JOIN a AS a2 ON a.x = a2.x
SELECT count(*) FROM a JOIN a AS a2 ON a.s = a2.s
SELECT count(*) FROM a JOIN a AS a2 ON a.m = a2.m
SELECT count(*) FROM a JOIN a AS a2 ON a.m = a2.m OR 0
SELECT count(*) FROM a JOIN a AS a2 ON a.n = a2.n
SELECT count(*) FROM a JOIN a AS a2 ON a.n = a2.n OR 0
SELECT count(*) FROM dup x JOIN dup y ON x.p = y.p
SELECT count(*) FROM dup x JOIN dup y ON x.q = y.q
SELECT count(*) FROM dup x JOIN dup y ON x.q IS y.q
SELECT * FROM a JOIN b ON c.k = a.k
SELECT * FROM a JOIN b USING (nosuch)
SELECT aid FROM a JOIN b USING (aid) WHERE b.aid > 15
SELECT k FROM a JOIN d USING (k)
SELECT k FROM a JOIN d ON a.k = d.k
-- subqueries
SELECT aid, (SELECT count(*) FROM b WHERE b.aid = a.aid AND EXISTS (SELECT 1 FROM c WHERE c.bid = b.bid AND c.k = a.k)) FROM a
SELECT aid FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.aid = a.aid AND b.v > (SELECT min(z) FROM c WHERE c.bid = b.bid OR c.k = a.k))
SELECT aid, (SELECT max(v) FROM b WHERE b.k = a.k AND b.bid IN (SELECT bid FROM c WHERE c.k = a.k OR c.g = a.g)) FROM a
SELECT aid, (SELECT (SELECT count(*) FROM c WHERE c.k = a.k) FROM d LIMIT 1) FROM a
SELECT aid, (SELECT label FROM d WHERE d.k = a.k ORDER BY label LIMIT 1) FROM a
SELECT aid, (SELECT label FROM d WHERE d.k = a.k ORDER BY label DESC LIMIT 1 OFFSET 1) FROM a
SELECT aid FROM a WHERE k IN (SELECT k FROM d)
SELECT aid FROM a WHERE k NOT IN (SELECT k FROM d)
SELECT aid FROM a WHERE k NOT IN (SELECT k FROM d WHERE k IS NOT NULL)
SELECT aid FROM a WHERE k IN (SELECT k FROM e)
SELECT aid FROM a WHERE k NOT IN (SELECT k FROM e)
SELECT aid, k IN (SELECT k FROM d), k NOT IN (SELECT k FROM d), NULL IN (SELECT k FROM e), NULL NOT IN (SELECT k FROM e) FROM a
SELECT aid FROM a WHERE (SELECT count(*) FROM b WHERE b.aid = a.aid) > 1
SELECT aid FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.aid = a.aid)
SELECT aid FROM a WHERE EXISTS (SELECT 1 FROM b LIMIT 0)
SELECT aid FROM a WHERE EXISTS (SELECT NULL)
SELECT (SELECT k FROM d), (SELECT k FROM d WHERE 0), (SELECT max(k) FROM e)
SELECT (SELECT k, label FROM d)
SELECT aid FROM a WHERE aid IN (SELECT aid, k FROM a)
SELECT aid FROM a ORDER BY (SELECT count(*) FROM b WHERE b.aid = a.aid) DESC, aid LIMIT 5
SELECT aid FROM a LIMIT (SELECT count(*) FROM d)
SELECT aid FROM a LIMIT (SELECT 2) OFFSET (SELECT 18)
SELECT * FROM (SELECT * FROM (SELECT aid, k FROM a WHERE k > 1) WHERE k < 4) t WHERE aid > 5
SELECT t.k, t.c FROM (SELECT k, count(*) AS c FROM a GROUP BY k) t JOIN d ON t.k = d.k
SELECT * FROM (SELECT aid FROM a ORDER BY aid DESC LIMIT 3) ORDER BY 1
SELECT count(*) FROM (SELECT * FROM a UNION ALL SELECT * FROM a)
SELECT aid, (SELECT sum(x2.y) FROM (SELECT y FROM a x3 WHERE x3.k = a.k) x2) FROM a
SELECT aid FROM a WHERE x > ALL (SELECT v FROM b)
SELECT aid FROM a WHERE (k, x) IN (SELECT k, v FROM b)
SELECT aid, (SELECT count(*) FROM b WHERE b.aid = a.aid) + (SELECT count(*) FROM c WHERE c.k = a.k) FROM a
SELECT aid FROM a WHERE k = (SELECT k FROM d ORDER BY wt DESC LIMIT 1)
SELECT aid, CASE WHEN EXISTS (SELECT 1 FROM b WHERE b.aid = a.aid) THEN 'y' ELSE 'n' END FROM a
SELECT aid FROM a WHERE aid IN (SELECT aid FROM b UNION SELECT k FROM d)
SELECT aid FROM a WHERE aid IN (SELECT b.aid FROM b WHERE b.k = a.k INTERSECT SELECT aid FROM a WHERE x > 0)
SELECT count(*), (SELECT count(*) FROM b WHERE b.k = a.k) FROM a
SELECT k, (SELECT count(*) FROM b WHERE b.k = a.k) FROM a GROUP BY k
SELECT k, (SELECT sum(a.x) FROM d WHERE d.k = a.k) FROM a GROUP BY k
SELECT aid FROM a WHERE x IN (SELECT max(v) FROM b GROUP BY k)
-- compounds with order/limit
O:SELECT k FROM a UNION SELECT k FROM d ORDER BY 1 DESC LIMIT 3
O:SELECT k, s FROM a UNION ALL SELECT k, label FROM d ORDER BY 1, 2 LIMIT 5 OFFSET 2
O:SELECT aid AS z FROM a EXCEPT SELECT bid - 100 FROM b ORDER BY z
O:SELECT x FROM a INTERSECT SELECT v FROM b ORDER BY 1
SELECT p, q FROM dup UNION SELECT p, q FROM dup
SELECT NULL UNION SELECT NULL
SELECT 1, 'a' UNION SELECT 1.0, 'a'
SELECT 1 UNION SELECT '1'
SELECT 1 INTERSECT SELECT 1.0
SELECT k FROM e UNION ALL SELECT k FROM e
SELECT k FROM d EXCEPT SELECT k FROM e
SELECT k FROM e INTERSECT SELECT k FROM d
O:SELECT k FROM d UNION SELECT NULL ORDER BY 1 NULLS LAST
SELECT k FROM d UNION SELECT k, k FROM d
O:SELECT k FROM d UNION ALL SELECT wt FROM d ORDER BY k
O:SELECT k AS kk FROM d UNION SELECT wt FROM d ORDER BY kk DESC
SELECT k FROM d ORDER BY 1 UNION SELECT 1
