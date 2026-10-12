SELECT emp.id FROM emp LEFT JOIN dept ON emp.dept = dept.dept WHERE dept.budget IS NULL
SELECT emp.id FROM emp LEFT JOIN dept ON emp.dept = dept.dept WHERE dept.dept IS NULL
SELECT emp.id FROM emp LEFT JOIN dept ON emp.dept = dept.dept WHERE dept.budget > 300
SELECT emp.id FROM emp LEFT JOIN dept ON emp.dept = dept.dept WHERE emp.salary > 70
SELECT t1.k FROM t1 LEFT JOIN t2 ON t1.k = t2.k WHERE t2.k IS NULL
SELECT t1.k, t2.b FROM t1 LEFT JOIN t2 ON t1.k = t2.k WHERE t2.b IS NOT NULL
SELECT t1.k, t2.b FROM t1 JOIN t2 ON t1.k = t2.k WHERE t2.b IS NULL OR t2.b > 300
SELECT t2.k, t1.a FROM t1 RIGHT JOIN t2 ON t1.k = t2.k WHERE t1.a IS NULL
SELECT t1.k, t2.k FROM t1 FULL JOIN t2 ON t1.k = t2.k WHERE t1.k IS NULL OR t2.k IS NULL
SELECT COUNT(*) FROM t1 JOIN t2 ON t1.k = t2.k
SELECT COUNT(*) FROM t1 LEFT JOIN t2 ON t1.k = t2.k
SELECT t1.k, t2.b FROM t1 JOIN t2 ON t2.k = t1.k
SELECT t1.k, t2.b FROM t1 JOIN t2 ON t1.k = t2.k WHERE t1.a > 5 AND t2.b < 301
SELECT t1.k, t2.b FROM t1 JOIN t2 ON t1.k = t2.k AND t1.a = 30
SELECT t1.k, t2.b FROM t1 JOIN t2 ON t1.k + 0 = t2.k
SELECT t1.s, t2.b FROM t1, t2 WHERE t1.k = t2.k AND t2.b > 100
SELECT t1.k, t2.b FROM t1, t2 WHERE t1.k = t2.k OR t2.b = 999
SELECT t1.k, COUNT(*) FROM t1 JOIN t2 ON t1.k = t2.k GROUP BY t1.k
SELECT t1.k, SUM(t2.b) FROM t1 JOIN t2 ON t1.k = t2.k GROUP BY t1.k
SELECT t1.k, t2.b * 2 FROM t1 JOIN t2 ON t1.k = t2.k
SELECT t1.k, UPPER(t3.c) FROM t1 JOIN t3 ON t1.k = t3.k
SELECT DISTINCT t1.k FROM t1 JOIN t2 ON t1.k = t2.k
O:SELECT t1.k, t2.b FROM t1 JOIN t2 ON t1.k = t2.k ORDER BY t2.b DESC
O:SELECT t1.k, t2.b FROM t1 JOIN t2 ON t1.k = t2.k ORDER BY t2.b LIMIT 2
SELECT id FROM emp o WHERE (SELECT COUNT(*) FROM emp i WHERE i.dept = o.dept) > 2
SELECT id FROM emp o WHERE (SELECT COUNT(*) FROM emp i WHERE i.salary > o.salary) = 0
SELECT id FROM emp o WHERE 3 > (SELECT COUNT(*) FROM emp i WHERE i.salary > o.salary)
SELECT id, (SELECT COUNT(*) FROM emp i WHERE i.salary > o.salary) FROM emp o
SELECT id, (SELECT SUM(b) FROM t2 WHERE t2.k = emp.id) FROM emp
SELECT id FROM emp WHERE salary > (SELECT AVG(salary) FROM emp) AND dept = 'Eng'
SELECT dept, COUNT(*) FROM emp GROUP BY dept HAVING COUNT(*) = (SELECT MAX(c) FROM (SELECT COUNT(*) AS c FROM emp GROUP BY dept) z)
SELECT id FROM emp WHERE id IN (SELECT k FROM t2) OR id IN (SELECT k FROM t3)
SELECT id FROM emp WHERE id IN (1, 2) AND id NOT IN (SELECT k FROM t3)
SELECT id FROM emp WHERE dept = (SELECT dept FROM dept WHERE budget = 1000)
SELECT name FROM emp WHERE dept IS NULL OR salary IS NULL
SELECT id FROM emp WHERE NOT (salary > 70) OR salary IS NULL
SELECT id FROM emp WHERE NOT NOT (salary > 70)
SELECT id FROM emp WHERE NOT (salary IN (70, 100))
SELECT id FROM emp WHERE NOT (salary BETWEEN 60 AND 80)
SELECT id FROM emp WHERE NOT (name LIKE 'A%')
SELECT id FROM emp WHERE dept NOT LIKE 'E%' OR dept IS NULL
SELECT id FROM emp WHERE NOT (mgr = 1 AND bonus > 0)
SELECT id FROM emp WHERE (salary > 70) = (bonus > 0)
SELECT id FROM emp WHERE salary > 70 AND (dept = 'Eng' OR dept = 'HR')
SELECT id FROM emp WHERE salary > 70 AND dept = 'Eng' OR dept = 'HR'
SELECT id FROM emp WHERE id % 2 = 0
SELECT id FROM emp WHERE bonus BETWEEN -3 AND 3
SELECT id FROM emp WHERE bonus IN (0, 1.5, 3)
SELECT id FROM emp WHERE salary IN (SELECT salary FROM emp WHERE salary IS NULL)
SELECT id FROM emp WHERE salary NOT IN (SELECT salary FROM emp WHERE salary IS NULL)
SELECT id FROM emp WHERE salary NOT IN (SELECT x FROM e)
SELECT id FROM emp WHERE salary IN (SELECT x FROM e)
SELECT dept, MIN(salary), MAX(salary) FROM emp GROUP BY dept
SELECT dept, COUNT(*) FROM emp WHERE salary > 1000 GROUP BY dept
SELECT COUNT(*) FROM emp WHERE salary > 1000 GROUP BY dept
SELECT mgr, COUNT(*) FROM emp GROUP BY mgr
SELECT dept, mgr, COUNT(*), SUM(bonus) FROM emp GROUP BY dept, mgr
SELECT DISTINCT dept, mgr FROM emp
SELECT DISTINCT mgr FROM emp
SELECT COUNT(DISTINCT mgr) FROM emp
SELECT COUNT(DISTINCT dept) FROM emp WHERE id > 100
SELECT AVG(bonus) FROM emp WHERE bonus IS NULL
SELECT MIN(bonus) FROM emp WHERE dept = 'Ops'
SELECT name, salary FROM emp WHERE salary = (SELECT MIN(salary) FROM emp)
O:SELECT name FROM emp ORDER BY salary DESC, name LIMIT 3
O:SELECT name FROM emp WHERE id > 2 ORDER BY id DESC
O:SELECT id, name FROM emp ORDER BY dept DESC, salary, id
O:SELECT DISTINCT dept FROM emp ORDER BY dept DESC
O:SELECT dept, COUNT(*) FROM emp GROUP BY dept ORDER BY dept
O:SELECT dept, COUNT(*) FROM emp GROUP BY dept ORDER BY COUNT(*), dept LIMIT 2
O:SELECT dept, AVG(salary) a FROM emp GROUP BY dept ORDER BY a DESC, dept
O:SELECT id FROM emp ORDER BY id LIMIT 2 OFFSET 0
O:SELECT id FROM emp ORDER BY id LIMIT 1 OFFSET 9
O:SELECT id FROM emp ORDER BY id LIMIT -1 OFFSET 8
O:SELECT id, bonus FROM emp ORDER BY bonus NULLS LAST, id
O:SELECT id, bonus FROM emp ORDER BY bonus DESC NULLS LAST, id
O:SELECT id, name FROM emp ORDER BY name DESC, id
O:SELECT id FROM emp ORDER BY dept IS NULL, dept, id
O:SELECT w FROM uni ORDER BY w
SELECT id FROM emp WHERE name < 'a'
SELECT id FROM emp WHERE name >= 'alice'
SELECT id FROM emp WHERE dept < 'F'
SELECT k, s FROM t1 WHERE s <> 'x'
SELECT k, s FROM t1 WHERE s IS NOT NULL AND s <> 'x'
SELECT id, salary * 1.1 FROM emp
SELECT id, salary / 2.0 FROM emp
SELECT id, salary + 0.5 FROM emp
SELECT id, bonus * bonus FROM emp
SELECT id, -bonus FROM emp
SELECT id, +salary FROM emp
SELECT id, 10 % 3, -10 % 3, 10 % -3 FROM emp WHERE id = 1
SELECT id, 5.5 % 2 FROM emp WHERE id = 1
SELECT id, 2147483647 + 1, 9223372036854775807 + 0 FROM emp WHERE id = 1
SELECT id, 1000000 * 1000000 FROM emp WHERE id = 1
SELECT id, 9223372036854775807 + 1 FROM emp WHERE id = 1
SELECT id, ABS(-9223372036854775807) FROM emp WHERE id = 1
SELECT id, 0.1 + 0.2 FROM emp WHERE id = 1
SELECT id, 1.0 / 0 FROM emp WHERE id = 1
SELECT id, 0 / 0 FROM emp WHERE id = 1
SELECT id, 5 % 0 FROM emp WHERE id = 1
SELECT id, CEIL(bonus), FLOOR(bonus) FROM emp
SELECT id, ROUND(-2.25, 1), ROUND(2.675, 2), ROUND(1234.5, -2) FROM emp WHERE id = 1
SELECT id, ROUND(salary) FROM emp
SELECT id, LENGTH(name || dept) FROM emp
SELECT id, UPPER(SUBSTR(name, 1, 1)) || LOWER(SUBSTR(name, 2)) FROM emp
SELECT id, REPLACE(name, 'l', '') FROM emp
SELECT id, COALESCE(NULL, NULL, 'z') FROM emp WHERE id = 1
SELECT id, COALESCE(dept, mgr) FROM emp
SELECT id, NULLIF(bonus, 0) FROM emp
SELECT id, CASE WHEN bonus > 0 THEN 'pos' WHEN bonus < 0 THEN 'neg' WHEN bonus = 0 THEN 'zero' END FROM emp
SELECT id, CASE WHEN salary IS NULL THEN 0 ELSE salary END FROM emp
SELECT id, CASE mgr WHEN 1 THEN 'one' WHEN 3 THEN 'three' ELSE 'other' END FROM emp
SELECT id, CASE WHEN NULL THEN 'a' ELSE 'b' END FROM emp WHERE id = 1
SELECT SUM(CASE WHEN bonus IS NULL THEN 1 END) FROM emp
SELECT COUNT(CASE WHEN salary > 70 THEN 1 END) FROM emp
SELECT dept, SUM(CASE WHEN salary > 70 THEN salary ELSE 0 END) FROM emp GROUP BY dept
SELECT id, CAST(salary AS INTEGER) / 3 FROM emp
SELECT id, CAST(bonus AS TEXT) FROM emp
SELECT id, CAST('abc' AS REAL) FROM emp WHERE id = 1
SELECT id, CAST(1 AS BOOLEAN), CAST(0 AS BOOLEAN) FROM emp WHERE id = 1
SELECT id, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC, id) FROM emp
SELECT id, DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary) FROM emp
SELECT id, RANK() OVER (ORDER BY bonus) FROM emp
SELECT id, COUNT(*) OVER () FROM emp
SELECT id, MAX(salary) OVER (PARTITION BY dept) FROM emp
SELECT id, AVG(bonus) OVER (PARTITION BY dept ORDER BY id) FROM emp
SELECT id, LAG(name) OVER (PARTITION BY dept ORDER BY id) FROM emp
SELECT id, LEAD(name, 2) OVER (ORDER BY id) FROM emp
SELECT id, SUM(bonus) OVER (PARTITION BY dept) FROM emp
SELECT id, COUNT(bonus) OVER (PARTITION BY dept ORDER BY id) FROM emp
SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM emp WHERE salary > 60
SELECT id, ROW_NUMBER() OVER (ORDER BY id DESC) FROM emp
SELECT k FROM t1 INTERSECT SELECT k FROM t3
SELECT k, s FROM t1 EXCEPT SELECT k, c FROM t3
SELECT s FROM t1 UNION SELECT c FROM t3
SELECT k FROM t2 UNION ALL SELECT k FROM t2
SELECT k FROM t2 INTERSECT SELECT k FROM t2
SELECT k FROM t2 EXCEPT SELECT k FROM t1
SELECT a FROM t1 UNION SELECT NULL FROM t3
SELECT k, COUNT(*) FROM t2 GROUP BY k
SELECT name, dept FROM emp WHERE id IN (SELECT id FROM emp WHERE dept = 'Eng') AND salary < 100
UPDATE emp SET salary = salary * 2 WHERE dept = 'Eng'; SELECT id, salary FROM emp
UPDATE emp SET dept = 'X' WHERE dept IS NULL; SELECT dept, COUNT(*) FROM emp GROUP BY dept
UPDATE emp SET bonus = COALESCE(bonus, 0) + 1; SELECT id, bonus FROM emp
UPDATE emp SET name = UPPER(name) WHERE name LIKE 'B%'; SELECT id, name FROM emp
UPDATE emp SET salary = (SELECT AVG(salary) FROM emp); SELECT id, salary FROM emp
UPDATE emp SET salary = (SELECT MAX(budget) FROM dept WHERE dept.dept = emp.dept); SELECT id, salary FROM emp
UPDATE emp SET mgr = NULL WHERE mgr NOT IN (SELECT id FROM emp WHERE salary > 60); SELECT id, mgr FROM emp
DELETE FROM emp WHERE salary IS NULL OR bonus IS NULL; SELECT id FROM emp
DELETE FROM emp WHERE NOT (salary > 60); SELECT id FROM emp
DELETE FROM emp WHERE dept NOT IN ('Eng'); SELECT id FROM emp
DELETE FROM emp WHERE id IN (SELECT mgr FROM emp); SELECT id FROM emp
DELETE FROM emp WHERE EXISTS (SELECT 1 FROM dept WHERE dept.dept = emp.dept AND budget > 600); SELECT id FROM emp
DELETE FROM emp WHERE salary < (SELECT AVG(salary) FROM emp); SELECT id FROM emp
INSERT INTO emp (id, name) VALUES (11, 'Zed'); SELECT * FROM emp WHERE id = 11
INSERT INTO emp (name, id) VALUES ('Zed', 11); SELECT * FROM emp WHERE id = 11
INSERT INTO emp (salary, id, dept) VALUES (1, 11, 'Q'); SELECT * FROM emp WHERE id = 11
INSERT INTO emp SELECT * FROM emp WHERE id < 3; SELECT COUNT(*), SUM(salary) FROM emp
INSERT INTO emp (id, name, dept) SELECT k + 100, s, 'T' FROM t1; SELECT * FROM emp WHERE id > 100
INSERT INTO t2 VALUES (NULL, NULL); SELECT COUNT(*), COUNT(k), COUNT(b) FROM t2
INSERT INTO t3 VALUES (8, 'eight'); DELETE FROM t3 WHERE k = 1; UPDATE t3 SET c = 'SIX' WHERE k = 6; SELECT * FROM t3
INSERT INTO t3 VALUES (8, 'eight'); SELECT t1.k, t3.c FROM t1 RIGHT JOIN t3 ON t1.k = t3.k
DELETE FROM t2 WHERE k IS NULL; SELECT t1.k FROM t1 WHERE k NOT IN (SELECT k FROM t2)
UPDATE t1 SET s = s || '!' WHERE s IS NOT NULL; SELECT * FROM t1
UPDATE t1 SET a = -a WHERE a < 0; SELECT * FROM t1
UPDATE t1 SET a = a / 4; SELECT * FROM t1
-- common table expressions
WITH b AS (SELECT id FROM a WHERE salary > 70), a AS (SELECT id, salary FROM emp) SELECT id FROM b
WITH emp AS (SELECT * FROM main.emp WHERE salary < 80) SELECT COUNT(*), SUM(salary) FROM emp
WITH c AS (SELECT 1 AS v) SELECT (WITH c AS (SELECT 2 AS v) SELECT v FROM c), v FROM c
WITH m AS (SELECT dept, MAX(salary) AS mx FROM emp GROUP BY dept) SELECT name FROM emp e WHERE salary = (SELECT mx FROM m WHERE m.dept = e.dept)
SELECT (SELECT COUNT(*) FROM emp), (WITH emp AS (SELECT 1 AS id) SELECT COUNT(*) FROM emp)
WITH u AS (SELECT k FROM t1 UNION ALL SELECT k FROM t2 EXCEPT SELECT k FROM t3) SELECT k FROM u
WITH r AS (SELECT id FROM emp WHERE salary < 70) DELETE FROM emp WHERE id IN (SELECT id FROM r); SELECT id FROM emp
INSERT INTO t3 WITH r AS (SELECT k + 50, 'w' FROM t2 WHERE k IS NOT NULL) SELECT * FROM r; SELECT k, c FROM t3
CREATE TABLE z AS WITH r AS (SELECT id, salary FROM emp WHERE salary > 70) SELECT * FROM r; SELECT id, salary FROM z
O:WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n + 2 FROM c WHERE n < 9) SELECT n FROM c
O:WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c LIMIT 4 OFFSET 2) SELECT n FROM c
O:WITH RECURSIVE c(n) AS (SELECT 3 UNION SELECT (n * 3) % 11 FROM c) SELECT n FROM c
O:WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < 3 UNION ALL SELECT n * 10 FROM c WHERE n < 3) SELECT n FROM c
O:WITH RECURSIVE sub(id, lvl) AS (SELECT id, 0 FROM emp WHERE id = 1 UNION ALL SELECT emp.id, sub.lvl + 1 FROM emp JOIN sub ON emp.mgr = sub.id) SELECT id, lvl FROM sub
O:WITH RECURSIVE sub(id, lvl) AS (SELECT id, 0 FROM emp WHERE id = 1 UNION ALL SELECT emp.id, sub.lvl + 1 FROM emp, sub WHERE emp.mgr = sub.id ORDER BY 2 DESC) SELECT id, lvl FROM sub
O:WITH RECURSIVE anc(id, mgr) AS (SELECT id, mgr FROM emp WHERE id = 9 UNION ALL SELECT emp.id, emp.mgr FROM emp, anc WHERE emp.id = anc.mgr) SELECT id, mgr FROM anc
WITH RECURSIVE sub(id) AS (SELECT 3 UNION ALL SELECT emp.id FROM emp JOIN sub ON emp.mgr = sub.id) UPDATE emp SET bonus = -1 WHERE id IN (SELECT id FROM sub); SELECT id, bonus FROM emp
