//! Compressed input files: gzip and zstd, detected by their magic bytes,
//! decoded as a stream (concatenated gzip members and zstd frames read as
//! one), and encoded again in the same format when such a table is written
//! back. Both codecs are pure Rust (flate2 on miniz_oxide, ruzstd); ruzstd
//! encodes at its fastest level, the only one it implements.

use std::io::{self, Cursor, Read};
use std::path::Path;

/// How a table's file is compressed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Compression {
    #[default]
    None,
    Gzip,
    Zstd,
}

const GZIP_MAGIC: &[u8] = b"\x1f\x8b";
const ZSTD_MAGIC: &[u8] = b"\x28\xb5\x2f\xfd";

impl Compression {
    /// The compression a file name's extension names (`.gz`, `.zst`), for
    /// a file that does not exist yet.
    pub fn from_extension(path: &Path) -> Self {
        match path.extension().map(|e| e.to_ascii_lowercase()) {
            Some(e) if e == "gz" => Compression::Gzip,
            Some(e) if e == "zst" => Compression::Zstd,
            _ => Compression::None,
        }
    }

    /// `data` encoded in this format.
    pub fn encode(self, data: &[u8]) -> io::Result<Vec<u8>> {
        match self {
            Compression::None => Ok(data.to_vec()),
            Compression::Gzip => {
                let mut e = flate2::write::GzEncoder::new(
                    Vec::with_capacity(data.len() / 4),
                    flate2::Compression::default(),
                );
                io::Write::write_all(&mut e, data)?;
                e.finish()
            }
            Compression::Zstd => Ok(ruzstd::encoding::compress_to_vec(
                data,
                ruzstd::encoding::CompressionLevel::Fastest,
            )),
        }
    }
}

/// `name` without a trailing `.gz` / `.zst` (any case).
pub fn strip_extension(name: &str) -> &str {
    let lower = name.to_ascii_lowercase();
    for ext in [".gz", ".zst"] {
        if lower.ends_with(ext) && name.len() > ext.len() {
            return &name[..name.len() - ext.len()];
        }
    }
    name
}

/// `input`, decoded when it starts with a gzip or zstd magic number, and
/// which compression that was. A decoding error surfaces as an I/O error
/// from the returned reader.
pub fn decoder<'r, R: Read + 'r>(mut input: R) -> io::Result<(Box<dyn Read + 'r>, Compression)> {
    let mut head = [0u8; 4];
    let mut n = 0;
    while n < head.len() {
        match input.read(&mut head[n..]) {
            Ok(0) => break,
            Ok(k) => n += k,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    let whole: Box<dyn Read + 'r> = Box::new(Cursor::new(head[..n].to_vec()).chain(input));
    if head[..n].starts_with(GZIP_MAGIC) {
        Ok((
            Box::new(flate2::read::MultiGzDecoder::new(whole)),
            Compression::Gzip,
        ))
    } else if head[..n].starts_with(ZSTD_MAGIC) {
        Ok((Box::new(ZstdFrames::new(whole)?), Compression::Zstd))
    } else {
        Ok((whole, Compression::None))
    }
}

fn zstd_error(e: impl std::fmt::Display) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("invalid zstd data: {e}"),
    )
}

/// A zstd stream of one or more frames.
struct ZstdFrames<'r> {
    frame: Option<
        ruzstd::decoding::StreamingDecoder<Box<dyn Read + 'r>, ruzstd::decoding::FrameDecoder>,
    >,
}

impl<'r> ZstdFrames<'r> {
    fn new(source: Box<dyn Read + 'r>) -> io::Result<Self> {
        let frame = ruzstd::decoding::StreamingDecoder::new(source).map_err(zstd_error)?;
        Ok(ZstdFrames { frame: Some(frame) })
    }
}

impl Read for ZstdFrames<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            let Some(frame) = self.frame.as_mut() else {
                return Ok(0);
            };
            let n = frame.read(buf)?;
            if n > 0 || buf.is_empty() {
                return Ok(n);
            }
            // This frame is done: another may follow.
            let mut source = self.frame.take().expect("frame").into_inner();
            let mut first = [0u8; 1];
            let k = loop {
                match source.read(&mut first) {
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                    other => break other?,
                }
            };
            if k == 0 {
                return Ok(0);
            }
            let rest: Box<dyn Read + '_> = Box::new(Cursor::new(first).chain(source));
            self.frame = Some(ruzstd::decoding::StreamingDecoder::new(rest).map_err(zstd_error)?);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_and_detection() {
        let data = b"a,b\n1,2\n".repeat(1000);
        for c in [Compression::None, Compression::Gzip, Compression::Zstd] {
            let enc = c.encode(&data).unwrap();
            let (mut r, found) = decoder(&enc[..]).unwrap();
            assert_eq!(found, c);
            let mut out = Vec::new();
            r.read_to_end(&mut out).unwrap();
            assert_eq!(out, data, "{c:?}");
        }
        // Shorter than a magic number.
        let (mut r, found) = decoder(&b"x"[..]).unwrap();
        assert_eq!(found, Compression::None);
        let mut out = Vec::new();
        r.read_to_end(&mut out).unwrap();
        assert_eq!(out, b"x");
    }

    #[test]
    fn names() {
        assert_eq!(strip_extension("a.csv.gz"), "a.csv");
        assert_eq!(strip_extension("a.TSV.ZST"), "a.TSV");
        assert_eq!(strip_extension(".gz"), ".gz");
        assert_eq!(strip_extension("a.csv"), "a.csv");
        assert_eq!(
            Compression::from_extension(Path::new("x.csv.gz")),
            Compression::Gzip
        );
    }
}
