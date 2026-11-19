//! Compressed input: gzip and zstd files (and standard input), detected by
//! their magic bytes, loaded under the name without `.gz` / `.zst`, and
//! recompressed in the same format by --write.

use std::fs;
use std::io::{Read, Write};
use std::path::Path;

use crate::helpers::create_temp_dir;

type R = Result<(), Box<dyn std::error::Error>>;

/// A file's data with untouched rows that only a byte-for-byte writeback
/// keeps: CRLF, a quoted field, a padded number.
const DATA: &str = "id,name,n\r\n1,\"Smith, J\",007\r\n2,Lee,5\r\n3,Kim,6\r\n";

fn gzip(data: &[u8]) -> Vec<u8> {
    let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    e.write_all(data).unwrap();
    e.finish().unwrap()
}

fn gunzip(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    flate2::read::MultiGzDecoder::new(data)
        .read_to_end(&mut out)
        .unwrap();
    out
}

fn zstd(data: &[u8]) -> Vec<u8> {
    ruzstd::encoding::compress_to_vec(data, ruzstd::encoding::CompressionLevel::Fastest)
}

fn unzstd(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    ruzstd::decoding::StreamingDecoder::new(data)
        .unwrap()
        .read_to_end(&mut out)
        .unwrap();
    out
}

fn sqawk(args: &[&str]) -> assert_cmd::assert::Assert {
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.args(args);
    cmd.assert()
}

fn path(p: &Path) -> &str {
    p.to_str().unwrap()
}

#[test]
fn gzip_file_loads_under_its_plain_name() -> R {
    let dir = create_temp_dir()?;
    let f = dir.path().join("people.csv.gz");
    fs::write(&f, gzip(DATA.as_bytes()))?;
    sqawk(&["-s", "SELECT name, n FROM people WHERE id > 1", path(&f)])
        .success()
        .stdout("name,n\nLee,5\nKim,6\n");
    Ok(())
}

#[test]
fn zstd_file_and_tab_separated_inner_extension() -> R {
    let dir = create_temp_dir()?;
    let f = dir.path().join("t.tsv.zst");
    fs::write(&f, zstd(b"a\tb\n1\tx y\n"))?;
    sqawk(&["-s", "SELECT b FROM t", path(&f)])
        .success()
        .stdout("b\nx y\n");
    Ok(())
}

/// --write recompresses in the same format; untouched rows come back byte
/// for byte once decompressed.
#[test]
fn write_recompresses_gzip_and_zstd() -> R {
    let dir = create_temp_dir()?;
    let g = dir.path().join("g.csv.gz");
    let z = dir.path().join("z.csv.zst");
    fs::write(&g, gzip(DATA.as_bytes()))?;
    fs::write(&z, zstd(DATA.as_bytes()))?;
    sqawk(&[
        "--write",
        "-s",
        "UPDATE g SET n = 50 WHERE id = 2; DELETE FROM z WHERE id = 3",
        path(&g),
        path(&z),
    ])
    .success();
    let gb = fs::read(&g)?;
    assert_eq!(&gb[..2], b"\x1f\x8b", "still gzip");
    assert_eq!(
        String::from_utf8(gunzip(&gb))?,
        "id,name,n\r\n1,\"Smith, J\",007\r\n2,Lee,50\r\n3,Kim,6\r\n"
    );
    let zb = fs::read(&z)?;
    assert_eq!(&zb[..4], b"\x28\xb5\x2f\xfd", "still zstd");
    assert_eq!(
        String::from_utf8(unzstd(&zb))?,
        "id,name,n\r\n1,\"Smith, J\",007\r\n2,Lee,5\r\n"
    );
    Ok(())
}

/// Concatenated gzip members and zstd frames read as one stream.
#[test]
fn multi_member_streams() -> R {
    let dir = create_temp_dir()?;
    let g = dir.path().join("m.csv.gz");
    let mut bytes = gzip(b"k\n1\n");
    bytes.extend(gzip(b"2\n"));
    fs::write(&g, bytes)?;
    let z = dir.path().join("n.csv.zst");
    let mut bytes = zstd(b"k\n1\n");
    bytes.extend(zstd(b"2\n"));
    fs::write(&z, bytes)?;
    sqawk(&[
        "-s",
        "SELECT COUNT(*) AS c FROM m; SELECT COUNT(*) AS c FROM n",
        path(&g),
        path(&z),
    ])
    .success()
    .stdout("c\n2\nc\n2\n");
    Ok(())
}

#[test]
fn compressed_standard_input() -> R {
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("sqawk");
    cmd.args(["-s", "SELECT SUM(id) AS s FROM stdin", "-"])
        .write_stdin(gzip(DATA.as_bytes()));
    cmd.assert().success().stdout("s\n6\n");
    Ok(())
}

/// A corrupt compressed file is one clean error naming the file.
#[test]
fn corrupt_compressed_input_is_an_error() -> R {
    let dir = create_temp_dir()?;
    let g = dir.path().join("bad.csv.gz");
    let mut bytes = gzip(DATA.as_bytes());
    let mid = bytes.len() / 2;
    bytes.truncate(mid);
    fs::write(&g, bytes)?;
    let z = dir.path().join("bad2.csv.zst");
    fs::write(&z, b"\x28\xb5\x2f\xfdgarbage")?;
    for f in [&g, &z] {
        let out = sqawk(&["-s", "SELECT * FROM bad", path(f)])
            .code(1)
            .get_output()
            .clone();
        let err = String::from_utf8(out.stderr)?;
        assert!(err.starts_with("sqawk: cannot open"), "{err}");
        assert!(err.contains("bad"), "{err}");
        assert_eq!(err.lines().count(), 1, "{err}");
    }
    Ok(())
}

/// CREATE TABLE ... LOCATION on a `.gz` path: an existing file is read
/// compressed, a new one is created compressed.
#[test]
fn location_tables_compress_too() -> R {
    let dir = create_temp_dir()?;
    let old = dir.path().join("old.csv.gz");
    fs::write(&old, gzip(b"a,b\n1,2\n"))?;
    let new = dir.path().join("new.csv.gz");
    let sql = format!(
        "CREATE TABLE o (a INT, b INT) LOCATION '{}'; \
         CREATE TABLE w (a INT, b INT) LOCATION '{}'; \
         INSERT INTO w SELECT b, a FROM o; SELECT * FROM w",
        path(&old).replace('\'', "''"),
        path(&new).replace('\'', "''")
    );
    sqawk(&["--write", "-s", &sql])
        .success()
        .stdout("a,b\n2,1\n");
    assert_eq!(gunzip(&fs::read(&new)?), b"a,b\n2,1\n");
    Ok(())
}
