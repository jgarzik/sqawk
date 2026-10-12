-- basics
SELECT * FROM emp
SELECT id, name FROM emp WHERE salary > 70
SELECT id FROM emp WHERE salary >= 70 AND dept = 'Eng'
SELECT id FROM emp WHERE salary < 70 OR dept IS NULL
SELECT id FROM emp WHERE NOT (salary > 70)
SELECT id FROM emp WHERE salary <> 70
SELECT id FROM emp WHERE salary != 70
SELECT id FROM emp WHERE bonus = 0
SELECT id FROM emp WHERE bonus > -3
SELECT id FROM emp WHERE salary BETWEEN 60 AND 80
SELECT id FROM emp WHERE salary NOT BETWEEN 60 AND 80
SELECT id FROM emp WHERE salary IN (70, 100)
SELECT id FROM emp WHERE salary NOT IN (70, 100)
SELECT id FROM emp WHERE dept IN ('Eng', NULL)
SELECT id FROM emp WHERE dept NOT IN ('Eng', NULL)
SELECT id FROM emp WHERE salary NOT IN (SELECT mgr FROM emp)
SELECT id FROM emp WHERE id NOT IN (SELECT mgr FROM emp)
SELECT id FROM emp WHERE id NOT IN (SELECT mgr FROM emp WHERE mgr IS NOT NULL)
SELECT id FROM emp WHERE id IN (SELECT mgr FROM emp)
SELECT id FROM emp WHERE NOT (dept = 'Eng')
SELECT id FROM emp WHERE NOT (dept = 'Eng' AND salary > 90)
SELECT id FROM emp WHERE NOT (dept = 'Eng' OR salary > 90)
SELECT id FROM emp WHERE dept = 'Eng' OR NULL
SELECT id FROM emp WHERE NOT (salary > 1000 AND dept IS NULL)
SELECT id FROM emp WHERE mgr = NULL
SELECT id FROM emp WHERE mgr IS NULL
SELECT id FROM emp WHERE mgr IS NOT NULL AND bonus IS NULL
SELECT id FROM emp WHERE salary = bonus * 10
SELECT id FROM emp WHERE -salary > -80
SELECT id FROM emp WHERE salary + bonus > 70
SELECT id, salary + bonus FROM emp
SELECT id, salary - bonus, salary * 2, salary / 3, salary % 7 FROM emp
SELECT id, bonus / 2, bonus * 2 FROM emp
SELECT id, salary / 0 FROM emp
SELECT id, salary % 0 FROM emp
SELECT id, -salary % 7, salary % -7 FROM emp
SELECT id, bonus % 2 FROM emp
SELECT id, 7 / 2, 7.0 / 2, -7 / 2, 1 / 3.0 FROM emp WHERE id = 1
SELECT id, salary / 3 * 3 FROM emp
SELECT id, 2 + 3 * 4, (2 + 3) * 4, 10 - 2 - 3 FROM emp WHERE id = 1
SELECT id, name || '-' || dept FROM emp
SELECT id, name || NULL FROM emp WHERE id < 3
SELECT id, CONCAT(name, dept) FROM emp
SELECT id, name || salary FROM emp
-- LIKE
SELECT id FROM emp WHERE name LIKE 'a%'
SELECT id FROM emp WHERE name LIKE 'A%'
SELECT id FROM emp WHERE name LIKE '%e'
SELECT id FROM emp WHERE name LIKE '_o_'
SELECT id FROM emp WHERE name LIKE '%'
SELECT id FROM emp WHERE name NOT LIKE '%a%'
SELECT id FROM emp WHERE dept LIKE '%'
SELECT id FROM emp WHERE dept NOT LIKE 'E%'
SELECT k FROM t1 WHERE s LIKE 'x%y'
SELECT k FROM t1 WHERE s LIKE 'a_b'
SELECT k FROM t1 WHERE s LIKE '%\_%' ESCAPE '\'
SELECT k FROM t1 WHERE s LIKE 'x\%y' ESCAPE '\'
SELECT id FROM emp WHERE name LIKE '______'
SELECT id FROM uni WHERE w LIKE '_____'
SELECT id FROM uni WHERE w LIKE 'h_llo'
-- CASE / COALESCE / NULLIF / CAST
SELECT id, CASE WHEN salary > 75 THEN 'hi' WHEN salary > 55 THEN 'mid' ELSE 'lo' END FROM emp
SELECT id, CASE dept WHEN 'Eng' THEN 1 WHEN 'HR' THEN 2 END FROM emp
SELECT id, CASE WHEN dept IS NULL THEN 'none' ELSE dept END FROM emp
SELECT id, CASE NULL WHEN NULL THEN 'match' ELSE 'nomatch' END FROM emp WHERE id = 1
SELECT id, COALESCE(bonus, mgr, -1) FROM emp
SELECT id, COALESCE(dept, 'none') FROM emp
SELECT id, NULLIF(salary, 70) FROM emp
SELECT id, NULLIF(dept, 'Eng') FROM emp
SELECT id, CAST(bonus AS INTEGER) FROM emp
SELECT id, CAST(salary AS REAL) / 3 FROM emp
SELECT id, CAST(salary AS TEXT) || 'x' FROM emp
SELECT id, CAST('12abc' AS INTEGER) FROM emp WHERE id = 1
SELECT id, CAST('3.9' AS INTEGER) FROM emp WHERE id = 1
SELECT id, CAST(-2.25 AS INTEGER) FROM emp WHERE id = 1
SELECT id, CAST(name AS INTEGER) FROM emp WHERE id = 1
SELECT id, CAST(' 5 ' AS INTEGER) FROM emp WHERE id = 1
SELECT id, CAST('1e3' AS REAL) FROM emp WHERE id = 1
SELECT id, CAST(NULL AS INTEGER) FROM emp WHERE id = 1
-- aggregates
SELECT COUNT(*), COUNT(dept), COUNT(salary), COUNT(bonus) FROM emp
SELECT SUM(salary), AVG(salary), MIN(salary), MAX(salary) FROM emp
SELECT SUM(bonus), AVG(bonus), MIN(bonus), MAX(bonus) FROM emp
SELECT COUNT(DISTINCT dept), COUNT(DISTINCT salary), COUNT(DISTINCT name) FROM emp
SELECT SUM(DISTINCT salary), AVG(DISTINCT salary) FROM emp
SELECT COUNT(*), SUM(salary), AVG(salary), MIN(salary), MAX(salary) FROM emp WHERE id > 100
SELECT COUNT(x), SUM(x), MAX(x) FROM e
SELECT COUNT(*) FROM e
SELECT AVG(id) FROM emp WHERE id IN (1, 2)
SELECT AVG(salary) FROM emp WHERE dept = 'Sales'
SELECT SUM(salary) / COUNT(*) FROM emp
SELECT MIN(name), MAX(name) FROM emp
SELECT MIN(dept), MAX(dept) FROM emp
SELECT MAX(salary) - MIN(salary) FROM emp
SELECT COUNT(*) + 1 FROM emp
SELECT SUM(salary * 2) FROM emp
SELECT SUM(CASE WHEN dept = 'Eng' THEN 1 ELSE 0 END) FROM emp
SELECT dept, COUNT(*) FROM emp GROUP BY dept
SELECT dept, COUNT(salary), SUM(salary), AVG(salary) FROM emp GROUP BY dept
SELECT dept, salary, COUNT(*) FROM emp GROUP BY dept, salary
SELECT dept, MAX(bonus) FROM emp GROUP BY dept HAVING MAX(bonus) > 0
SELECT dept, COUNT(*) FROM emp GROUP BY dept HAVING COUNT(*) > 1
SELECT dept, COUNT(*) AS c FROM emp GROUP BY dept HAVING c > 1
SELECT dept, SUM(salary) FROM emp GROUP BY dept HAVING SUM(salary) IS NULL
SELECT dept, AVG(salary) FROM emp GROUP BY dept HAVING AVG(salary) > 60
SELECT dept FROM emp GROUP BY dept HAVING COUNT(*) >= 2 AND MIN(salary) > 60
O:SELECT dept, COUNT(*) FROM emp WHERE dept IS NOT NULL GROUP BY dept ORDER BY COUNT(*) DESC, dept
O:SELECT dept, COUNT(*) AS c FROM emp WHERE dept IS NOT NULL GROUP BY dept ORDER BY c DESC, dept
O:SELECT dept, SUM(salary) AS s FROM emp GROUP BY dept ORDER BY 2 DESC, 1
O:SELECT dept, SUM(salary) AS s FROM emp GROUP BY dept ORDER BY s, dept
O:SELECT dept, MAX(salary) FROM emp GROUP BY dept ORDER BY MAX(salary), dept
O:SELECT dept, salary, COUNT(*) FROM emp GROUP BY dept, salary ORDER BY dept, salary
O:SELECT dept, salary, COUNT(*) FROM emp GROUP BY 1, 2 ORDER BY 1, 2
SELECT dept, COUNT(*) FROM emp GROUP BY 1
SELECT salary % 20, COUNT(*) FROM emp GROUP BY salary % 20
SELECT UPPER(dept), COUNT(*) FROM emp GROUP BY UPPER(dept)
SELECT COUNT(*) FROM emp GROUP BY dept HAVING dept = 'Eng'
SELECT dept, COUNT(*) FROM e GROUP BY dept
SELECT x, COUNT(*) FROM e GROUP BY x
SELECT dept, COUNT(DISTINCT salary) FROM emp GROUP BY dept
SELECT name, COUNT(*) FROM emp GROUP BY name HAVING COUNT(*) > 1
SELECT dept, SUM(bonus) FROM emp GROUP BY dept
SELECT dept, MIN(bonus), MAX(name) FROM emp GROUP BY dept
SELECT SUM(salary) FROM emp HAVING SUM(salary) > 0
SELECT dept, COUNT(*) * 10 FROM emp GROUP BY dept
SELECT dept, ROUND(AVG(salary), 1) FROM emp GROUP BY dept
-- DISTINCT
SELECT DISTINCT dept FROM emp
SELECT DISTINCT dept, salary FROM emp
SELECT DISTINCT name FROM emp
SELECT DISTINCT bonus FROM emp
SELECT DISTINCT k, a, s FROM t1
O:SELECT DISTINCT salary FROM emp ORDER BY salary DESC
O:SELECT DISTINCT dept FROM emp ORDER BY dept
-- ORDER BY / LIMIT
O:SELECT id, salary FROM emp ORDER BY salary, id
O:SELECT id, salary FROM emp ORDER BY salary DESC, id
O:SELECT id, dept FROM emp ORDER BY dept, id
O:SELECT id, dept FROM emp ORDER BY dept DESC, id
O:SELECT id, bonus FROM emp ORDER BY bonus, id
O:SELECT id, bonus FROM emp ORDER BY bonus DESC, id
O:SELECT id, salary FROM emp ORDER BY salary NULLS LAST, id
O:SELECT id, salary FROM emp ORDER BY salary DESC NULLS FIRST, id
O:SELECT id, name FROM emp ORDER BY name, id
O:SELECT id, name FROM emp ORDER BY LOWER(name), id
O:SELECT id FROM emp ORDER BY id LIMIT 3
O:SELECT id FROM emp ORDER BY id LIMIT 3 OFFSET 2
O:SELECT id FROM emp ORDER BY id LIMIT 3 OFFSET 20
O:SELECT id FROM emp ORDER BY id LIMIT 0
O:SELECT id FROM emp ORDER BY id DESC LIMIT 2, 3
O:SELECT id, salary * -1 AS neg FROM emp ORDER BY neg, id
O:SELECT id, salary FROM emp ORDER BY 2, 1
O:SELECT id FROM emp ORDER BY salary + COALESCE(bonus, 0), id
O:SELECT id FROM emp ORDER BY CASE WHEN dept = 'HR' THEN 0 ELSE 1 END, id
O:SELECT name, id FROM emp WHERE salary > 50 ORDER BY dept, name, id
O:SELECT id FROM emp ORDER BY mgr, id
O:SELECT emp.id FROM emp ORDER BY emp.salary DESC, emp.id
SELECT COUNT(*) FROM (SELECT id FROM emp ORDER BY id LIMIT 4) sub
-- string functions
SELECT id, UPPER(name), LOWER(name), LENGTH(name) FROM emp
SELECT id, LENGTH(w), UPPER(w), LOWER(w) FROM uni
SELECT id, SUBSTR(name, 2), SUBSTR(name, 2, 3) FROM emp
SELECT id, SUBSTR(name, -3), SUBSTR(name, -3, 2) FROM emp
SELECT id, SUBSTR(name, 0, 2), SUBSTR(name, 10) FROM emp
SELECT id, SUBSTR(w, 2, 2) FROM uni
SELECT id, SUBSTR(w, -2) FROM uni
SELECT id, SUBSTR(name, 3, -2) FROM emp
SELECT id, TRIM(w), LENGTH(TRIM(w)) FROM uni
SELECT id, LTRIM(w), RTRIM(w) FROM uni
SELECT id, TRIM('x' FROM 'xxaxx') FROM uni WHERE id = 1
SELECT id, TRIM(w, ' p') FROM uni
SELECT id, REPLACE(name, 'a', 'AA') FROM emp
SELECT id, REPLACE(w, '', 'z') FROM uni
SELECT id, INSTR(name, 'a') FROM emp
SELECT id, INSTR(w, 'é') FROM uni
SELECT id, LENGTH(salary), LENGTH(bonus) FROM emp
SELECT id, LENGTH(NULL) FROM emp WHERE id = 1
SELECT id, UPPER(NULL), LOWER(dept) FROM emp
SELECT id, w FROM uni WHERE w = ''
SELECT id, w FROM uni WHERE w IS NULL
SELECT id FROM uni WHERE LENGTH(w) = 3
SELECT id, ABS(salary), ABS(bonus) FROM emp
SELECT id, ROUND(bonus), ROUND(bonus, 1) FROM emp
SELECT id, ROUND(2.5), ROUND(-2.5), ROUND(0.5), ROUND(1.005, 2) FROM emp WHERE id = 1
SELECT id, ROUND(salary / 3.0, 2) FROM emp
SELECT id, ROUND(NULL) FROM emp WHERE id = 1
SELECT id, MAX(salary, bonus) FROM emp
SELECT id, MIN(salary, 75) FROM emp
SELECT id, typeof(salary) FROM emp
SELECT id, HEX(name) FROM emp WHERE id = 1
SELECT id, IFNULL(bonus, 0) FROM emp
SELECT id, IIF(salary > 70, 'y', 'n') FROM emp
SELECT id, printf('%05.1f', bonus) FROM emp
SELECT id, SIGN(bonus) FROM emp
-- subqueries
SELECT id FROM emp WHERE salary = (SELECT MAX(salary) FROM emp)
SELECT id FROM emp WHERE salary > (SELECT AVG(salary) FROM emp)
SELECT id, (SELECT COUNT(*) FROM emp) FROM emp
SELECT id, (SELECT MAX(salary) FROM emp) - salary FROM emp
SELECT id FROM emp WHERE salary = (SELECT MAX(salary) FROM emp WHERE id > 100)
SELECT id FROM emp WHERE dept IN (SELECT dept FROM dept)
SELECT id FROM emp WHERE dept NOT IN (SELECT dept FROM dept)
SELECT id FROM emp WHERE dept NOT IN (SELECT dept FROM dept WHERE budget IS NULL)
SELECT id FROM emp WHERE dept IN (SELECT dept FROM dept WHERE budget > 400)
SELECT id FROM emp WHERE EXISTS (SELECT 1 FROM dept WHERE dept.dept = emp.dept)
SELECT id FROM emp WHERE NOT EXISTS (SELECT 1 FROM dept WHERE dept.dept = emp.dept)
SELECT id FROM emp WHERE EXISTS (SELECT 1 FROM e)
SELECT id FROM emp WHERE NOT EXISTS (SELECT 1 FROM e)
SELECT id FROM emp o WHERE salary > (SELECT AVG(salary) FROM emp i WHERE i.dept = o.dept)
SELECT id FROM emp o WHERE salary = (SELECT MAX(salary) FROM emp i WHERE i.dept = o.dept)
SELECT id, (SELECT COUNT(*) FROM emp i WHERE i.mgr = o.id) FROM emp o
SELECT id, (SELECT dname FROM dept WHERE dept.dept = emp.dept) FROM emp
SELECT id, (SELECT MAX(dname) FROM dept WHERE dept.dept = emp.dept) FROM emp
SELECT id FROM emp o WHERE EXISTS (SELECT 1 FROM emp i WHERE i.mgr = o.id)
SELECT id FROM emp WHERE id IN (SELECT mgr FROM emp WHERE dept = 'Sales')
SELECT id FROM emp WHERE (SELECT COUNT(*) FROM emp i WHERE i.salary > emp.salary) < 3
SELECT dept, cnt FROM (SELECT dept, COUNT(*) AS cnt FROM emp GROUP BY dept) sub WHERE cnt > 1
SELECT MAX(s) FROM (SELECT dept, SUM(salary) AS s FROM emp GROUP BY dept) sub
SELECT sub.n FROM (SELECT name AS n FROM emp WHERE salary > 70) AS sub
SELECT id FROM emp WHERE salary IN (SELECT salary FROM emp WHERE dept = 'Sales')
SELECT id FROM emp WHERE salary > ALL (SELECT salary FROM emp WHERE dept = 'Sales')
SELECT id FROM emp WHERE salary > ANY (SELECT salary FROM emp WHERE dept = 'Sales')
SELECT id FROM emp WHERE (salary, dept) IN (SELECT salary, dept FROM emp WHERE id = 3)
-- joins
SELECT emp.id, dept.dname FROM emp JOIN dept ON emp.dept = dept.dept
SELECT emp.id, dept.dname FROM emp INNER JOIN dept ON emp.dept = dept.dept AND dept.budget > 600
SELECT emp.id, dept.dname FROM emp LEFT JOIN dept ON emp.dept = dept.dept
SELECT emp.id, dept.dname FROM emp RIGHT JOIN dept ON emp.dept = dept.dept
SELECT emp.id, dept.dname FROM emp FULL OUTER JOIN dept ON emp.dept = dept.dept
SELECT emp.id, dept.dname FROM emp LEFT JOIN dept ON emp.dept = dept.dept WHERE dept.dname IS NULL
SELECT emp.id, dept.dname FROM emp LEFT JOIN dept ON emp.dept = dept.dept AND dept.budget > 600
SELECT emp.id, dept.dname FROM emp, dept WHERE emp.dept = dept.dept
SELECT COUNT(*) FROM emp, dept
SELECT COUNT(*) FROM emp CROSS JOIN dept
SELECT t1.k, t2.b FROM t1 JOIN t2 ON t1.k = t2.k
SELECT t1.k, t2.b FROM t1 LEFT JOIN t2 ON t1.k = t2.k
SELECT t1.k, t2.k, t2.b FROM t1 RIGHT JOIN t2 ON t1.k = t2.k
SELECT t1.k, t2.k, t2.b FROM t1 FULL JOIN t2 ON t1.k = t2.k
SELECT t1.k, t2.b, t3.c FROM t1 JOIN t2 ON t1.k = t2.k JOIN t3 ON t2.k = t3.k
SELECT t1.k, t2.b, t3.c FROM t1 LEFT JOIN t2 ON t1.k = t2.k LEFT JOIN t3 ON t1.k = t3.k
SELECT t1.k, t2.b, t3.c FROM t1 LEFT JOIN t2 ON t1.k = t2.k JOIN t3 ON t2.k = t3.k
SELECT t1.k, t3.c FROM t1 FULL JOIN t3 ON t1.k = t3.k
SELECT t1.k, t2.b FROM t1 JOIN t2 ON t1.k < t2.k
SELECT t1.k, t2.b FROM t1 JOIN t2 ON t1.k = t2.k OR t2.k IS NULL
SELECT k, a, b FROM t1 JOIN t2 USING (k)
SELECT k, a, b FROM t1 LEFT JOIN t2 USING (k)
SELECT * FROM t1 JOIN t2 USING (k)
SELECT * FROM t1 NATURAL JOIN t2
SELECT * FROM t1 NATURAL JOIN t3
SELECT * FROM t1 JOIN t2 ON t1.k = t2.k
SELECT a.name, b.name FROM emp a JOIN emp b ON a.mgr = b.id
SELECT a.name, b.name FROM emp a LEFT JOIN emp b ON a.mgr = b.id
SELECT a.id, b.id FROM emp a JOIN emp b ON a.salary = b.salary AND a.id < b.id
SELECT a.name, COUNT(b.id) FROM emp a LEFT JOIN emp b ON b.mgr = a.id GROUP BY a.id, a.name
SELECT dept.dname, COUNT(emp.id) FROM dept LEFT JOIN emp ON emp.dept = dept.dept GROUP BY dept.dname
SELECT dept.dname, SUM(emp.salary) FROM dept LEFT JOIN emp ON emp.dept = dept.dept GROUP BY dept.dname
O:SELECT emp.name, dept.dname FROM emp JOIN dept ON emp.dept = dept.dept ORDER BY dept.budget DESC, emp.id
SELECT x.id FROM emp x JOIN dept y ON x.dept = y.dept WHERE y.budget > 300 AND x.salary > 70
SELECT emp.id, d.dname FROM emp JOIN (SELECT * FROM dept WHERE budget > 300) d ON emp.dept = d.dept
SELECT emp.id, t3.c FROM emp JOIN t3 ON emp.id = t3.k
SELECT emp.id FROM emp JOIN t1 ON emp.id = t1.k JOIN t2 ON t1.k = t2.k JOIN t3 ON t2.k = t3.k
-- set operations
SELECT dept FROM emp UNION SELECT dept FROM dept
SELECT dept FROM emp UNION ALL SELECT dept FROM dept
SELECT dept FROM emp INTERSECT SELECT dept FROM dept
SELECT dept FROM emp EXCEPT SELECT dept FROM dept
SELECT dept FROM dept EXCEPT SELECT dept FROM emp
SELECT k FROM t1 UNION SELECT k FROM t2
SELECT k FROM t1 INTERSECT SELECT k FROM t2
SELECT k FROM t1 EXCEPT SELECT k FROM t2
SELECT k, a FROM t1 UNION SELECT k, b FROM t2
O:SELECT k FROM t1 UNION SELECT k FROM t2 ORDER BY k
O:SELECT k FROM t1 UNION SELECT k FROM t2 ORDER BY k DESC LIMIT 3
O:SELECT k FROM t1 UNION ALL SELECT k FROM t2 ORDER BY 1 LIMIT 4 OFFSET 2
SELECT k FROM t1 UNION SELECT k FROM t2 UNION SELECT k FROM t3
SELECT k FROM t1 UNION ALL SELECT k FROM t2 EXCEPT SELECT k FROM t3
SELECT k FROM t1 EXCEPT SELECT k FROM t2 UNION SELECT k FROM t3
SELECT COUNT(*) FROM (SELECT k FROM t1 UNION SELECT k FROM t2) u
SELECT x FROM e UNION SELECT k FROM t3
-- window functions
SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM emp
SELECT id, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY id) FROM emp
SELECT id, RANK() OVER (ORDER BY salary) FROM emp
SELECT id, RANK() OVER (ORDER BY salary DESC) FROM emp
SELECT id, DENSE_RANK() OVER (ORDER BY salary) FROM emp
SELECT id, RANK() OVER (PARTITION BY dept ORDER BY salary DESC) FROM emp
SELECT id, LAG(salary) OVER (ORDER BY id) FROM emp
SELECT id, LEAD(salary) OVER (ORDER BY id) FROM emp
SELECT id, LAG(salary, 2) OVER (ORDER BY id) FROM emp
SELECT id, LAG(salary, 1, -1) OVER (ORDER BY id) FROM emp
SELECT id, LEAD(salary, 1, 0) OVER (PARTITION BY dept ORDER BY id) FROM emp
SELECT id, SUM(salary) OVER () FROM emp
SELECT id, SUM(salary) OVER (PARTITION BY dept) FROM emp
SELECT id, SUM(salary) OVER (ORDER BY id) FROM emp
SELECT id, SUM(salary) OVER (ORDER BY salary) FROM emp
SELECT id, SUM(salary) OVER (PARTITION BY dept ORDER BY id) FROM emp
SELECT id, COUNT(*) OVER (PARTITION BY dept) FROM emp
SELECT id, COUNT(salary) OVER (ORDER BY id) FROM emp
SELECT id, AVG(salary) OVER (PARTITION BY dept) FROM emp
SELECT id, MIN(salary) OVER (ORDER BY id), MAX(salary) OVER (ORDER BY id) FROM emp
SELECT id, SUM(salary) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM emp
SELECT id, SUM(salary) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM emp
SELECT id, SUM(salary) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) FROM emp
SELECT id, NTILE(3) OVER (ORDER BY id) FROM emp
SELECT id, FIRST_VALUE(name) OVER (PARTITION BY dept ORDER BY id) FROM emp
SELECT id, LAST_VALUE(name) OVER (PARTITION BY dept ORDER BY id) FROM emp
SELECT id, PERCENT_RANK() OVER (ORDER BY salary) FROM emp
O:SELECT id, ROW_NUMBER() OVER (ORDER BY salary DESC, id) AS rn FROM emp ORDER BY rn
SELECT id, salary - LAG(salary) OVER (ORDER BY id) FROM emp
SELECT * FROM (SELECT id, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC, id) AS rn FROM emp) w WHERE rn = 1
SELECT dept, SUM(salary), RANK() OVER (ORDER BY SUM(salary) DESC) FROM emp GROUP BY dept
-- CTE
WITH h AS (SELECT * FROM emp WHERE salary > 70) SELECT id FROM h
WITH h AS (SELECT dept, COUNT(*) AS c FROM emp GROUP BY dept) SELECT dept FROM h WHERE c > 1
WITH a AS (SELECT 1 AS n), b AS (SELECT n + 1 AS n FROM a) SELECT n FROM b
WITH RECURSIVE cnt(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM cnt WHERE n < 5) SELECT n FROM cnt
WITH RECURSIVE ch(id, lvl) AS (SELECT id, 0 FROM emp WHERE mgr IS NULL UNION ALL SELECT emp.id, ch.lvl + 1 FROM emp JOIN ch ON emp.mgr = ch.id) SELECT id, lvl FROM ch
-- no-FROM
SELECT 1
SELECT 1 + 1, 'a' || 'b'
SELECT 1 = 1
SELECT 1 WHERE 0
SELECT 5 IN (1, 5)
SELECT NULL IS NULL
SELECT 'a' < 'b'
SELECT k, a = 10 FROM t1
SELECT k, a > 5 AND s = 'x' FROM t1
SELECT k, a IS NULL FROM t1
SELECT k, s LIKE 'x%' FROM t1
SELECT k, a BETWEEN 0 AND 20 FROM t1
SELECT k, a IN (10, 30) FROM t1
SELECT k, NOT a FROM t1
SELECT k FROM t1 WHERE a
SELECT k FROM t1 WHERE NOT a
SELECT k FROM t1 WHERE s
-- misc
SELECT k, a FROM t1 WHERE a = '10'
SELECT k, a FROM t1 WHERE a > '9'
SELECT k, a FROM t1 WHERE s > 'x'
SELECT k, a FROM t1 WHERE s < 'a'
SELECT id FROM emp WHERE name = 'alice'
SELECT id FROM emp WHERE name > 'B' AND name < 'D'
SELECT id FROM emp WHERE dept = 'eng'
SELECT emp.* FROM emp WHERE id = 3
SELECT *, id FROM emp WHERE id = 3
SELECT id AS "my id", name FROM emp WHERE id = 1
SELECT "name" FROM emp WHERE id = 1
SELECT EMP.ID FROM EMP WHERE ID = 2
SELECT Name FROM emp WHERE Id = 2
-- DML
INSERT INTO t1 VALUES (9, 90, 'new'); SELECT * FROM t1
INSERT INTO t1 (k, s) VALUES (9, 'new'); SELECT * FROM t1
INSERT INTO t1 VALUES (9, 90, 'n1'), (10, NULL, 'n2'); SELECT * FROM t1
INSERT INTO t1 SELECT k, b, 'from2' FROM t2; SELECT * FROM t1
INSERT INTO t1 (k, a) SELECT k, b FROM t2 WHERE b > 300; SELECT * FROM t1
UPDATE t1 SET a = a + 1; SELECT * FROM t1
UPDATE t1 SET a = a * 2, s = 'u' WHERE k > 2; SELECT * FROM t1
UPDATE t1 SET a = NULL WHERE a IS NOT NULL AND k = 1; SELECT * FROM t1
UPDATE t1 SET k = k + 1, a = k; SELECT * FROM t1
UPDATE t1 SET a = (SELECT MAX(b) FROM t2); SELECT * FROM t1
UPDATE t1 SET a = (SELECT MAX(b) FROM t2 WHERE t2.k = t1.k); SELECT * FROM t1
UPDATE t1 SET a = 0 WHERE k IN (SELECT k FROM t2); SELECT * FROM t1
UPDATE t1 SET a = 0 WHERE k NOT IN (SELECT k FROM t2); SELECT * FROM t1
UPDATE t1 SET a = 0 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.k = t1.k); SELECT * FROM t1
DELETE FROM t1 WHERE a IS NULL; SELECT * FROM t1
DELETE FROM t1 WHERE a > 5; SELECT * FROM t1
DELETE FROM t1 WHERE k IN (SELECT k FROM t2); SELECT * FROM t1
DELETE FROM t1 WHERE k NOT IN (SELECT k FROM t2); SELECT * FROM t1
DELETE FROM t1; SELECT * FROM t1
DELETE FROM t1; SELECT COUNT(*), SUM(a) FROM t1
DELETE FROM t1 WHERE k = 3; INSERT INTO t1 VALUES (3, 33, 'z'); UPDATE t1 SET s = 'w' WHERE k = 3; SELECT * FROM t1
INSERT INTO t1 VALUES (11, 1, 'a;b'); SELECT * FROM t1
INSERT INTO t1 VALUES (12, 1, 'it''s'); SELECT * FROM t1
CREATE TABLE nt (a INTEGER, b TEXT); INSERT INTO nt VALUES (1, 'x'), (2, NULL); SELECT * FROM nt
CREATE TABLE nt AS SELECT dept, COUNT(*) AS c FROM emp GROUP BY dept; SELECT * FROM nt
INSERT INTO t1 VALUES (13, 2.5, 'f'); SELECT SUM(a), AVG(a) FROM t1
INSERT INTO t1 VALUES (14, '15', 'str'); SELECT k FROM t1 WHERE a > 12
INSERT INTO t1 VALUES (1, 2); SELECT * FROM t1
INSERT INTO t1 (k, nosuch) VALUES (1, 2); SELECT * FROM t1
INSERT INTO t1 VALUES (1, 2, 3, 4); SELECT * FROM t1
INSERT OR REPLACE INTO t1 VALUES (1, 2, 'x'); SELECT * FROM t1
INSERT INTO t1 VALUES (99, 1, 'r') RETURNING k
DELETE FROM t1 WHERE k = 1 RETURNING k
CREATE VIEW v AS SELECT * FROM t1; SELECT * FROM v
CREATE INDEX ix ON t1 (k); SELECT k FROM t1
SELECT k FROM t1 WHERE s GLOB 'x*'
SELECT k FROM t1 WHERE s REGEXP 'x'
SELECT GROUP_CONCAT(name) FROM emp WHERE id < 3
SELECT dept, GROUP_CONCAT(id) FROM emp GROUP BY dept
SELECT STRING_AGG(name, ';') FROM emp
SELECT TOTAL(salary) FROM emp
SELECT k, a FROM t1 WHERE a IS NOT DISTINCT FROM NULL
SELECT k FROM t1 WHERE a IS 10
SELECT DATE('2024-01-15')
SELECT strftime('%Y', '2024-01-15')
