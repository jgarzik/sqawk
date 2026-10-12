//! Extended string function tests for sqawk VM
//!
//! Tests for CONCAT, LEFT, RIGHT functions.

use crate::helpers::{create_custom_csv, create_temp_dir};

/// Test CONCAT function with multiple arguments
#[test]
fn test_concat() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,first,last\n1,John,Doe\n2,Jane,Smith\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT CONCAT(first, ' ', last) AS full_name FROM data")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("John Doe"))
        .stdout(predicates::str::contains("Jane Smith"));

    Ok(())
}

/// Test LEFT function
#[test]
fn test_left() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name\n1,Alexander\n2,Bob\n3,Charlotte\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT LEFT(name, 3) AS short FROM data")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("Ale"))
        .stdout(predicates::str::contains("Bob"))
        .stdout(predicates::str::contains("Cha"));

    Ok(())
}

/// Test RIGHT function
#[test]
fn test_right() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = create_temp_dir()?;
    let content = "id,name\n1,Alexander\n2,Bob\n3,Charlotte\n";
    let file_path = create_custom_csv(temp_dir.path(), "data.csv", content)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.arg("-s")
        .arg("SELECT RIGHT(name, 3) AS ending FROM data")
        .arg(file_path.to_str().unwrap());

    cmd.assert()
        .success()
        .stdout(predicates::str::contains("der"))
        .stdout(predicates::str::contains("Bob"))
        .stdout(predicates::str::contains("tte"));

    Ok(())
}

// ---------------------------------------------------------------------------
// SQLite string semantics: character-based, NULL-propagating. Expected values
// computed with SQLite 3.45 unless the comment says otherwise.
// ---------------------------------------------------------------------------

mod sqlite_semantics {
    use crate::helpers::{assert_query_on, assert_query_on_fails, ONE};

    type R = Result<(), Box<dyn std::error::Error>>;

    const U: (&str, &str) = ("u.csv", "id,w\n1,héllo\n2,日本語\n3,ABC\n4,a_b\n5,x%y\n");
    const T: (&str, &str) = ("t.csv", "k,a,b\n1,7,2.5\n2,-7,\n3,10,0\n");

    /// `||` is NULL when either side is; CONCAT treats NULL as ''.
    #[test]
    fn concat_operator_and_function_null_handling() -> R {
        assert_query_on(
            &[ONE],
            "SELECT 'a' || NULL IS NULL AS a, NULL || 'a' IS NULL AS b, \
             concat('a', NULL, 'b') AS c, concat_ws('-', 'a', NULL, 'b') AS d, \
             1 || 2 AS e, 1.5 || 'x' AS f FROM one",
            "a,b,c,d,e,f\n1,1,ab,a-b,12,1.5x",
        )
    }

    #[test]
    fn length_counts_characters_of_text_form() -> R {
        assert_query_on(
            &[ONE],
            "SELECT length('日本語') AS a, length(123) AS b, length(1.5) AS c, \
             length(NULL) AS d, length('') AS e, length(-12) AS f FROM one",
            "a,b,c,d,e,f\n3,3,3,,0,3",
        )
    }

    #[test]
    fn unicode_columns() -> R {
        assert_query_on(
            &[U],
            "SELECT id, length(w) AS n, substr(w, 2) AS s2, substr(w, -2) AS sm2, \
             instr(w, '本') AS i FROM u",
            "id,n,s2,sm2,i\n\
             1,5,éllo,lo,0\n\
             2,3,本語,本語,2\n\
             3,3,BC,BC,0\n\
             4,3,_b,_b,0\n\
             5,3,%y,%y,0",
        )
    }

    /// Negative start counts from the end; start 0 sits before the first
    /// character; a negative length takes the characters before start.
    #[test]
    fn substr_follows_sqlite_rules() -> R {
        assert_query_on(
            &[ONE],
            "SELECT substr('hello', -3) AS a, substr('hello', 0, 2) AS b, \
             substr('hello', 3, -2) AS c, substr('hello', 10) AS d, \
             substr('hello', -10, 3) AS e, substr('hello', 0) AS f, substr('hello', 2, 0) AS g, \
             substr('hello', -3, 2) AS h, substr('hello', 6, -2) AS i, \
             substr(12345, 2, 2) AS j FROM one",
            "a,b,c,d,e,f,g,h,i,j\nllo,h,he,,,hello,,ll,lo,23",
        )
    }

    #[test]
    fn instr_and_replace() -> R {
        assert_query_on(
            &[ONE],
            "SELECT instr('hello', 'l') AS a, instr('hello', '') AS b, instr('hello', 'z') AS c, \
             instr(12345, 34) AS d, instr(NULL, 'a') AS e FROM one",
            "a,b,c,d,e\n3,1,0,3,",
        )?;
        assert_query_on(
            &[ONE],
            "SELECT replace('abc', '', 'z') AS a, replace('aaa', 'a', 'bb') AS b, \
             replace(123, 2, 9) AS c, replace('abc', 'b', NULL) AS d FROM one",
            "a,b,c,d\nabc,bbbbbb,193,",
        )
    }

    /// Two-argument LTRIM/RTRIM strip any of the given characters. The
    /// standard `TRIM(LEADING 'x' FROM s)` form (which SQLite does not parse)
    /// is the same thing.
    #[test]
    fn trim_with_character_set() -> R {
        assert_query_on(
            &[ONE],
            "SELECT ltrim('xxaxx', 'x') AS a, rtrim('xxaxx', 'x') AS b, trim('  x  ') AS c, \
             ltrim('  x  ') AS d, rtrim('  x  ') AS e FROM one",
            "a,b,c,d,e\naxx,xxa,x,\"x  \",\"  x\"",
        )?;
        assert_query_on(
            &[ONE],
            "SELECT TRIM(LEADING 'x' FROM 'xxaxx') AS a, TRIM(TRAILING 'x' FROM 'xxaxx') AS b, \
             TRIM(BOTH 'x' FROM 'xxaxx') AS c, TRIM('x' FROM 'xxaxx') AS d FROM one",
            "a,b,c,d\naxx,xxa,a,a",
        )
    }

    /// LPAD/RPAD are not in SQLite; they follow PostgreSQL: truncate to the
    /// length, pad with repetitions of the fill (default a space), leave a
    /// short string alone when the fill is empty.
    #[test]
    fn lpad_rpad_postgres_semantics() -> R {
        assert_query_on(
            &[ONE],
            "SELECT LPAD('ab', 5, 'xy') AS a, RPAD('ab', 5, 'xy') AS b, LPAD('hello', 2) AS c, \
             RPAD('hello', 2, '*') AS d, '[' || LPAD('a', 3) || ']' AS e, \
             LPAD('ab', 4, '') AS f, LPAD('日本', 4, '*') AS g, LPAD(NULL, 3) AS h, \
             LPAD('ab', -1) AS i FROM one",
            "a,b,c,d,e,f,g,h,i\nxyxab,abxyx,he,he,[  a],ab,**日本,,",
        )
    }

    #[test]
    fn hex_char_unicode() -> R {
        assert_query_on(
            &[ONE],
            "SELECT hex('abc') AS a, hex(12) AS b, hex('日') AS c, hex(NULL) AS d, \
             char(72, 105) AS e, unicode('A') AS f, unicode('日') AS g FROM one",
            "a,b,c,d,e,f,g\n616263,3132,E697A5,,Hi,65,26085",
        )
    }

    #[test]
    fn printf_conversions() -> R {
        assert_query_on(
            &[ONE],
            "SELECT printf('%5.2f', 3.14159) AS a, printf('%5d|%-5d|%05d', 42, 42, 42) AS b, \
             printf('%s-%s', 'x', NULL) AS c, printf('%d%%', '12abc') AS d, \
             printf('%x', 255) AS e, printf('%.3f', 2) AS f, format('%d items', 3) AS g FROM one",
            "a,b,c,d,e,f,g\n\" 3.14\",\"   42|42   |00042\",x-,12%,ff,2.000,3 items",
        )
    }

    /// The rest of SQLite's printf: flags `# , !`, `*` width and precision,
    /// `%q %Q %w %c %r %u %.Nd`, real rounding to 16 digits, and NULL for a
    /// format that produces nothing.
    #[test]
    fn printf_sqlite_conversions() -> R {
        assert_query_on(
            &[ONE],
            "SELECT printf('%#x|%#o|%,d|%.3d|%u', 255, 8, -1234567, 7, -1) AS a, \
             printf('%*d|%-*d|%.*f', 4, 7, 4, 7, 2, 3.14159) AS b, \
             printf('%q|%Q|%Q|%w', 'it''s', 'x', NULL, 'a\"b') AS c, \
             printf('%.3c|%r|%r|%r', 'ab', 1, 12, 23) AS d, \
             printf('%.20f|%!.20f|%.0f|%g|%#g', 0.1, 0.1, 2.5, 0.00001, 1.0) AS e, \
             printf('%-4s|%!-4s|', 'é', 'é') AS f, printf('') AS g, printf('%5') AS h, \
             printf('a%yb') AS i FROM one",
            "a,b,c,d,e,f,g,h,i\n\"0xff|010|-1,234,567|007|18446744073709551615\",\
             \"   7|7   |3.14\",\"it''s|'x'|NULL|a\"\"\"\"b\",aaa|1st|12th|23rd,\
             0.10000000000000000000|0.1000000000000000055|2|1e-05|1.00000,é  |é   |,,,a",
        )
    }

    /// QUOTE gives the SQL literal of a value.
    #[test]
    fn quote_values() -> R {
        assert_query_on(
            &[ONE],
            "SELECT quote('a''b') AS a, quote(12) AS b, quote(NULL) AS c, quote(1.5) AS d, \
             quote(0.1) AS e, quote(1e999) AS f, quote(1.0/3) AS g FROM one",
            "a,b,c,d,e,f,g\n'a''b',12,NULL,1.5,0.1,Inf,3.333333333333333148e-01",
        )
    }

    /// TYPEOF sees NULL (it must not short-circuit to NULL itself).
    #[test]
    fn typeof_values() -> R {
        assert_query_on(
            &[T],
            "SELECT typeof(1) AS a, typeof(1.5) AS b, typeof('x') AS c, typeof(NULL) AS d, \
             typeof(1/0) AS e, typeof(k) AS f, typeof(b) AS g FROM t WHERE k = 2",
            "a,b,c,d,e,f,g\ninteger,real,text,null,null,integer,null",
        )
    }

    #[test]
    fn ifnull_iif_sign_scalar_min_max() -> R {
        assert_query_on(
            &[T],
            "SELECT k, ifnull(b, -1) AS i, iif(a > 0, 'pos', 'neg') AS p, sign(a) AS s, \
             max(a, b) AS mx, min(a, 5) AS mn FROM t",
            "k,i,p,s,mx,mn\n1,2.5,pos,1,7,5\n2,-1,neg,-1,,-7\n3,0,pos,1,10,5",
        )?;
        assert_query_on(
            &[ONE],
            "SELECT sign(-3) AS a, sign(0) AS b, sign(2.5) AS c, sign('x') AS d, \
             sign('-2') AS e, max(1, '2') AS f, typeof(max(1, '2')) AS g, max('a', 1) AS h, \
             min(3, 2.5, 4) AS i FROM one",
            "a,b,c,d,e,f,g,h,i\n-1,0,1,,-1,2,text,a,2.5",
        )
    }

    /// IIF and IFNULL evaluate only the branch they return.
    #[test]
    fn iif_and_ifnull_are_lazy() -> R {
        assert_query_on(
            &[ONE],
            "SELECT IIF(1, 'ok', ABS(-9223372036854775807 - 1)) AS a, \
             IFNULL(1, ABS(-9223372036854775807 - 1)) AS b FROM one",
            "a,b\nok,1",
        )
    }

    #[test]
    fn wrong_argument_count_is_an_error() -> R {
        assert_query_on_fails(
            &[ONE],
            "SELECT INSTR('a') FROM one",
            "wrong number of arguments to function INSTR()",
        )?;
        assert_query_on_fails(
            &[ONE],
            "SELECT IIF(1, 2) FROM one",
            "wrong number of arguments to function IIF()",
        )
    }
}
