-- loader type inference cross-check
SELECT aid, typeof(k), typeof(x), typeof(y), typeof(s), typeof(g), typeof(n), typeof(m) FROM a
SELECT bid, typeof(aid), typeof(k), typeof(v), typeof(w) FROM b
SELECT cid, typeof(bid), typeof(z), typeof(g) FROM c
SELECT uid, typeof(t), length(t) FROM u
SELECT did, typeof(ts), ts FROM dt
SELECT * FROM a
SELECT * FROM u
SELECT * FROM dup
SELECT * FROM e
-- precedence
SELECT 1 IS DISTINCT FROM 2 OR 1
SELECT 0 IS NOT DISTINCT FROM 0 AND 0
SELECT count(*) FROM a WHERE k IS NOT DISTINCT FROM 1 AND x > 0
SELECT count(*) FROM a WHERE k IS DISTINCT FROM NULL OR x IS NULL
SELECT NULL IS NOT DISTINCT FROM NULL, 1 IS DISTINCT FROM 1.0, 'a' IS DISTINCT FROM 'A'
SELECT k FROM d UNION ALL SELECT k FROM d INTERSECT SELECT wt FROM d
SELECT k FROM d UNION SELECT wt FROM d EXCEPT SELECT 5
SELECT k FROM d EXCEPT SELECT 1 UNION SELECT 1
SELECT p FROM dup UNION ALL SELECT p FROM dup EXCEPT SELECT 2
SELECT p FROM dup INTERSECT SELECT k FROM d UNION ALL SELECT 99
SELECT 2 + 3 * 4, (2 + 3) * 4, 2 - 3 - 4, 2 * 3 % 4, 7 % 4 * 3, 100 / 10 / 5
SELECT - - 1, 1 - -1, -(-5), +5, - 'a'
SELECT 'a' || 1 + 2, 1 + 2 || 'x', 'x' || NULL || 'y'
SELECT 1 < 2 = 1, 2 > 1 = 0, 1 = 1 = 1, NOT 1 = 2, NOT NULL IS NULL
SELECT NOT 0 AND 0, NOT (0 AND 0), 1 OR 0 AND 0, (1 OR 0) AND 0
SELECT 5 BETWEEN 1 AND 10 AND 0, 5 NOT BETWEEN 1 AND 3 OR 0, NULL BETWEEN 1 AND 2, 1 BETWEEN NULL AND 0
SELECT 'abc' LIKE 'A%' = 1, 'abc' LIKE 'x%' OR 1
SELECT 1 IN (1, 2) = 1, 3 NOT IN (1, NULL), 1 IN (NULL, 1), NULL IN (1), NULL IN ()
SELECT 5 & 3, 5 | 3, 1 << 4, 256 >> 2, ~5
SELECT -9223372036854775808 >> 1, 1 << 63, 1 << 64, 1 << -1
SELECT CASE NULL WHEN NULL THEN 1 ELSE 0 END, CASE WHEN NULL THEN 1 ELSE 2 END, CASE 1 WHEN 1.0 THEN 'eq' ELSE 'ne' END
SELECT CASE WHEN 'x' THEN 1 ELSE 0 END, CASE WHEN '1x' THEN 1 ELSE 0 END, CASE WHEN 0.1 THEN 1 ELSE 0 END, CASE WHEN '0.0' THEN 1 ELSE 0 END
SELECT CASE 'a' WHEN 'A' THEN 1 WHEN 'a' THEN 2 END, CASE 3 WHEN 1 THEN 'x' END
-- arithmetic and overflow
SELECT 9223372036854775807 + 1, -9223372036854775808 - 1, 9223372036854775807 * 2, -9223372036854775807 - 1
SELECT -9223372036854775808 / -1, -9223372036854775808 % -1, 9223372036854775807 / -1
SELECT -(-9223372036854775808), - -9223372036854775808, 0 - -9223372036854775808
SELECT 9223372036854775808, -9223372036854775809, typeof(9223372036854775808), typeof(-9223372036854775808)
SELECT abs(-9223372036854775807), abs(-3), abs(-2.5), abs(NULL), abs('x'), abs('-7')
SELECT abs(-9223372036854775808)
SELECT CAST('9223372036854775808' AS INTEGER), CAST('-9223372036854775809' AS INTEGER), CAST(9.3e18 AS INTEGER), CAST(-9.3e18 AS INTEGER)
SELECT CAST(1e19 AS INTEGER), CAST('1e19' AS INTEGER), CAST(' -12 ' AS INTEGER), CAST('+5' AS INTEGER), CAST('' AS INTEGER), CAST(NULL AS INTEGER)
SELECT CAST('3.9' AS INTEGER), CAST(-3.9 AS INTEGER), CAST('0x1A' AS INTEGER), CAST('1e3' AS REAL), CAST('.5' AS REAL), CAST('5.' AS REAL), CAST('-.5e1x' AS REAL)
SELECT CAST('3.0' AS NUMERIC), CAST('3.5' AS NUMERIC), CAST('abc' AS NUMERIC), CAST('12abc' AS NUMERIC), CAST('1e2' AS NUMERIC), CAST(' 7 ' AS NUMERIC)
SELECT typeof(CAST('3.0' AS NUMERIC)), typeof(CAST('1e2' AS NUMERIC)), typeof(CAST('9223372036854775808' AS NUMERIC)), CAST('9223372036854775808' AS NUMERIC)
SELECT CAST(12 AS TEXT), typeof(CAST(12 AS TEXT)), CAST('x' AS BLOB) IS NULL, CAST(5 AS VARCHAR(2)), CAST('7' AS BIGINT) + 1
SELECT 5 / 0, 5 % 0, 5.0 / 0, 0 / 0, 5 / 0.0, -5 % 0.0, NULL / 0
SELECT -7 / 2, -7 % 2, 7 % -2, -7 % -2, 7.5 % 2, -7.5 % 2, 7 % 2.5, 1 / 3.0, 2.0 * 3, typeof(2.0 * 3)
SELECT '7' / '2', '7.0' / '2', '3' + '4', typeof('3' + '4'), '3.0' + 1, typeof('3.0' + 1), ' 5' + 1, '5 ' + 1
SELECT '1e3' * 1, '0x10' + 0, '-' + 1, '.5' + 0, '5.' + 0, 'inf' + 0, '1e400' + 0, '-1e400' + 0, '1e-400' + 0
SELECT '12abc' * 2, 'abc' + 1, '' + 1, '  ' + 1, '+3' + 0, '--3' + 0, '1.5e' + 0, '1.5e+' + 0, '1.5e+2x' + 0
SELECT typeof('1e3' * 1), typeof('9223372036854775807' + 0), typeof('9223372036854775808' + 0), '9223372036854775808' + 0
SELECT 0.1 + 0.2, 1e308 * 10, -1e308 * 10, 1e308 * 10 - 1e308 * 10, typeof(1e308 * 10 - 1e308 * 10)
SELECT 4.0 / 2, typeof(4.0 / 2), 1e15 + 0.5, 123456789012345678, 1.5e300 * 1.5e300
SELECT round(-0.5), round(0.5), round(2.5), round(-2.5), round(1.005, 2), round(2.675, 2), round(1e20, 2), round(-1e-20, 5)
SELECT round(123.456, 1), round(123.456, 10), round(NULL), round('3.7'), round('x'), round(9223372036854775807), round(5, 2)
SELECT ceil(-0.5), floor(-0.5), ceil(1.2), floor(1.8), ceil(5), floor(-5), ceil('2.1'), ceiling(NULL)
SELECT sign(-3), sign(0), sign(2.5), sign(NULL), sign('-4'), sign('x'), sign('')
SELECT max(1, 2.5, '3'), min(1, 2.5, '3'), max(1, NULL), min('a', 'B'), max('a', 'B', 1), typeof(max(1, 2.5))
SELECT 1 = 1.0, 1 < 1.5, 2 < 10, 'a' < 'B', 'abc' < 'abd', 'Z' < 'a', '' < 'a', NULL = NULL, NULL <> 1
SELECT 9223372036854775807 = 9223372036854775806.0, 9223372036854775807 < 9223372036854775808.0, 9007199254740993 = 9007199254740992.0
SELECT 9223372036854775807 > 9.223372036854775807e18, -9223372036854775808 = -9.223372036854775808e18
SELECT count(*) FROM a WHERE x = 9223372036854775807
SELECT count(*) FROM a WHERE x > 9223372036854775806
SELECT aid, x + 1, x - 1, x * x FROM a WHERE x IS NOT NULL
SELECT aid, x / 0, y / 0, x % 7, y * 2, -x, -y FROM a
SELECT aid, n + 0, n * 1, n || '', typeof(n + 0), length(n), CAST(n AS INTEGER), CAST(n AS REAL) FROM a
SELECT aid, m + 0, typeof(m), m || '|', length(m), CAST(m AS INTEGER) FROM a
SELECT sum(x) FROM a
SELECT sum(x) FROM a WHERE x > 0
SELECT total(x), avg(x) FROM a
SELECT sum(9223372036854775807) FROM d
SELECT total(9223372036854775807), avg(9223372036854775807) FROM d
SELECT sum(-9223372036854775808) FROM dup WHERE p = 2
SELECT sum(x) FROM (SELECT 9223372036854775807 AS x UNION ALL SELECT 1 UNION ALL SELECT -5)
SELECT sum(x) FROM (SELECT 9223372036854775807 AS x UNION ALL SELECT -5 UNION ALL SELECT 1)
SELECT sum(n), total(n), avg(n), count(n), min(n), max(n) FROM a
SELECT sum(m), total(m), avg(m) FROM a
SELECT sum(s), total(s), avg(s) FROM a
SELECT sum(y), total(y), avg(y), sum(DISTINCT y), avg(DISTINCT y), count(DISTINCT y) FROM a
SELECT sum(x), avg(x), min(x), max(x), count(x), count(*), total(x), group_concat(x) FROM e
SELECT k, sum(x) FROM e GROUP BY k
SELECT count(*) FROM e HAVING count(*) = 0
SELECT count(*) FROM a HAVING count(*) > 100
SELECT count(*), sum(p) FROM dup HAVING sum(p) > 1
-- string functions
SELECT substr('hello', 0), substr('hello', 0, 2), substr('hello', -3), substr('hello', -3, 2), substr('hello', 3, -2), substr('hello', -10, 3), substr('hello', 2, 0), substr('hello', 10)
SELECT substr('héllo日本', 2, 3), substr('日本語テキスト', -2), substr(NULL, 1), substr('abc', NULL), substr('abc', 2, NULL), substr(12345, 2, 2)
SELECT substr('hello', 1.7, 2.9), substr('hello', '2', '2'), substr('hello', -1, -1), substr('hello', 0, -1), substr('hello', 6, -3)
SELECT length('héllo'), length('日本語'), length(''), length(NULL), length(123), length(1.5), length(-0.25), length('🎉')
SELECT instr('日本語', '語'), instr('hello', ''), instr('', ''), instr(NULL, 'a'), instr('abcabc', 'c'), instr(12345, 34)
SELECT replace('aaa', 'a', 'bb'), replace('abc', '', 'x'), replace('abc', 'b', ''), replace(NULL, 'a', 'b'), replace('abc', 'b', NULL), replace(1212, 1, 3)
SELECT trim('  x  '), ltrim('  x  '), rtrim('  x  '), trim('xxaxx', 'x'), ltrim('abcab', 'ab'), rtrim('abcab', 'ab'), trim('', 'a'), trim(NULL)
SELECT trim('日本日', '日'), rtrim('héé', 'é'), trim(12321, 1), ltrim('aaa', 'a')
SELECT upper('abc'), lower('ABC'), upper(NULL), lower(12), upper('x%y')
SELECT hex('abc'), hex(''), hex(NULL), hex(12), hex('é'), hex(-1.5), hex(1e100)
SELECT char(72, 105), char(26085, 0x672C), char(), unicode('é'), unicode(''), unicode(NULL), unicode('日本')
SELECT concat('a', NULL, 'b', 1, 2.5), concat_ws('-', 'a', NULL, 'b'), concat_ws(NULL, 'a', 'b'), concat_ws(',', NULL, NULL), concat()
SELECT 'a' || 1.5, 'a' || 2.0, 'a' || -0.0, 'a' || 1e20, 'a' || 1e-7
SELECT quote('a''b'), quote(1), quote(NULL), quote(1.5)
SELECT length(t), substr(t, 4990), upper(substr(t, 1, 3)), instr(t, 'y'), length(t || t), length(replace(t, 'x', 'yy')) FROM u WHERE uid = 3
SELECT uid, length(t), hex(substr(t, 1, 2)), upper(t), lower(t), trim(t), instr(t, 'e') FROM u WHERE uid <> 3
SELECT uid, t LIKE '%e%', t LIKE '_é%', t LIKE 'h_llo', t LIKE '%\%%' ESCAPE '\', t LIKE '%\_%' ESCAPE '\', t LIKE '%\\' ESCAPE '\' FROM u WHERE uid <> 3
SELECT group_concat(t, '|') FROM (SELECT t FROM u WHERE uid < 3 ORDER BY uid)
-- printf
SELECT printf('%5.2f|%-5d|%+d|%05d|%x|%X|%o', 3.14159, 42, 7, 42, 255, 255, 8)
SELECT printf('%e|%E|%g|%G', 12345.678, 0.000123, 100000, 1e-5)
SELECT printf('%g', 1234567), printf('%g', 123456), printf('%g', 0.0001), printf('%g', 0.00001), printf('%.3g', 1234.5), printf('%#g', 1.0)
SELECT printf('%.0f', 0.5), printf('%.0f', 1.5), printf('%.0f', 2.5), printf('%.2f', 1.005), printf('%.1f', 0.05), printf('%.10f', 0.1)
SELECT printf('%d', 3.9), printf('%d', '12abc'), printf('%d', 'x'), printf('%d', NULL), printf('%s', NULL), printf('%d', 9223372036854775807), printf('%d', -9223372036854775808)
SELECT printf('%5s|%-5s|%.2s|%10.3s|', 'abc', 'abc', 'abc', 'abcdef'), printf('%c', 'hello'), printf('%%'), printf('%s %s', 'a')
SELECT printf('%x', -1), printf('%o', -1), printf('%u', -1), printf('%i', 42), printf('% d', 42), printf('%+.2e', 12345)
SELECT printf('%5.1f%%', 45.678), printf('%-8.3f|', -1.5), printf('%08.3f', -1.5), printf('%+08d', 42), printf('%.3d', 7)
SELECT printf('%s', 1.0), printf('%s', 0.1), printf('%s', 1e20), printf('%f', 1e20), printf('%d', 1e20), printf('%.2f', 'abc'), printf('%f', '2.5x')
SELECT printf('%10d|%-10d|', -42, -42), printf('%lld', 5), printf('%z', 5), printf('%q', 'it''s'), printf('%Q', NULL), printf('%w', 'a"b')
SELECT printf('%.*f', 2, 3.14159), printf('%*d', 5, 42), printf('%!s', 'x')
SELECT printf('%s|%s', 'ß', '日本'), printf('%-6s|', 'ß'), printf('%.1s', '日本'), printf('%3s|', '日')
SELECT printf(NULL), printf(''), printf('abc'), printf('%d %d', 1), format('%d-%s', 5, 'x')
-- LIKE edge cases
SELECT 'abc' LIKE 'ABC', 'ABC' LIKE 'abc', 'é' LIKE 'É', 'É' LIKE 'é', 'aé' LIKE 'a_', 'a日b' LIKE 'a_b'
SELECT 'a' || char(10) || 'b' LIKE 'a_b', 'a' || char(10) || 'b' LIKE 'a%', '' LIKE '', '' LIKE '%', '' LIKE '_', NULL LIKE '%', 'a' LIKE NULL
SELECT 123 LIKE '1%', 1.5 LIKE '1._', 'aXb' LIKE 'a%%b', 'ab' LIKE 'a%%%b', 'abc' LIKE '%b', 'abc' LIKE '%c%', 'abc' LIKE 'a%c%'
SELECT '100%' LIKE '100\%' ESCAPE '\', '100' LIKE '100\%' ESCAPE '\', 'a_c' LIKE 'a\_c' ESCAPE '\', 'abc' LIKE 'a\_c' ESCAPE '\', 'a\c' LIKE 'a\\c' ESCAPE '\'
SELECT 'abc' LIKE 'a%' ESCAPE 'a', 'a%' LIKE 'aa%' ESCAPE 'a', 'x' LIKE 'x' ESCAPE NULL
SELECT 'abc' LIKE 'a%' ESCAPE 'ab'
SELECT 'abc' NOT LIKE 'a%', NULL NOT LIKE 'a', 'AbC' NOT LIKE '%b%'
SELECT aid, s FROM a WHERE s LIKE '%a%'
SELECT aid, s FROM a WHERE s LIKE '_'
SELECT aid, s FROM a WHERE s LIKE '%' || g || '%'
SELECT aid, s FROM a WHERE s NOT LIKE '%a%'
SELECT 'abcdefghij' LIKE '%a%b%c%d%e%f%g%h%i%j%', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaab' LIKE '%a%a%a%a%a%a%a%a%a%c'
SELECT 'x' GLOB 'x'
SELECT 'x' REGEXP 'x'
