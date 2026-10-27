//! Compressione compatibile con `kombu.compression` (stessi content type/alias).

use std::io::{Read, Write};

/// Algoritmo di compressione selezionato da un nome o alias.
#[derive(Clone, Copy)]
enum Method {
    Zlib,
    Bzip2,
    Lzma,
    Brotli,
    Zstd,
}

fn method(name: &str) -> Option<(Method, &'static str)> {
    Some(match name {
        "application/x-gzip" | "gzip" | "zlib" => (Method::Zlib, "application/x-gzip"),
        "application/x-bz2" | "bzip2" | "bzip" => (Method::Bzip2, "application/x-bz2"),
        "application/x-lzma" | "lzma" | "xz" => (Method::Lzma, "application/x-lzma"),
        "application/x-brotli" | "brotli" => (Method::Brotli, "application/x-brotli"),
        "application/zstd" | "zstd" | "zstandard" => (Method::Zstd, "application/zstd"),
        _ => return None,
    })
}

/// Content type canonico per un nome o alias, come `get_encoder`.
pub fn canonical_content_type(name: &str) -> Option<&'static str> {
    method(name).map(|(_, content_type)| content_type)
}

#[derive(Debug, thiserror::Error)]
pub enum CompressionError {
    /// Equivale al `KeyError` che solleva kombu.
    #[error("{0}")]
    UnknownMethod(String),
    #[error("compression error: {0}")]
    Codec(String),
}

/// Comprime `data`; ritorna anche il content type canonico.
pub fn compress(name: &str, data: &[u8]) -> Result<(Vec<u8>, &'static str), CompressionError> {
    let (method, content_type) =
        method(name).ok_or_else(|| CompressionError::UnknownMethod(name.to_string()))?;
    let out = match method {
        Method::Zlib => {
            let mut enc = flate2::write::ZlibEncoder::new(
                Vec::with_capacity(data.len() / 2 + 32),
                flate2::Compression::default(),
            );
            enc.write_all(data)
                .and_then(|()| enc.finish())
                .map_err(|e| CompressionError::Codec(e.to_string()))?
        }
        Method::Bzip2 => {
            let mut enc = bzip2::write::BzEncoder::new(
                Vec::with_capacity(data.len() / 2 + 32),
                bzip2::Compression::new(9),
            );
            enc.write_all(data)
                .and_then(|()| enc.finish())
                .map_err(|e| CompressionError::Codec(e.to_string()))?
        }
        Method::Lzma => {
            let mut enc = xz2::write::XzEncoder::new(Vec::with_capacity(data.len() / 2 + 64), 6);
            enc.write_all(data)
                .and_then(|()| enc.finish())
                .map_err(|e| CompressionError::Codec(e.to_string()))?
        }
        Method::Brotli => {
            let mut enc = brotli::CompressorWriter::new(
                Vec::with_capacity(data.len() / 2 + 32),
                4096,
                11,
                22,
            );
            enc.write_all(data)
                .map_err(|e| CompressionError::Codec(e.to_string()))?;
            enc.into_inner()
        }
        Method::Zstd => {
            zstd::stream::encode_all(data, 3).map_err(|e| CompressionError::Codec(e.to_string()))?
        }
    };
    Ok((out, content_type))
}

/// Limite del body decompresso e della memoria del decoder (64 MiB).
pub const MAX_DECOMPRESSED_SIZE: usize = 64 * 1024 * 1024;

/// Decomprime `data` secondo il content type (o alias).
pub fn decompress(content_type: &str, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
    decompress_limited(content_type, data, MAX_DECOMPRESSED_SIZE)
}

fn read_limited(reader: impl Read, limit: usize) -> Result<Vec<u8>, CompressionError> {
    let mut out = Vec::new();
    reader
        .take(limit as u64 + 1)
        .read_to_end(&mut out)
        .map_err(|e| CompressionError::Codec(e.to_string()))?;
    if out.len() > limit {
        return Err(CompressionError::Codec(
            "decompressed body exceeds limit".into(),
        ));
    }
    Ok(out)
}

fn decompress_limited(
    content_type: &str,
    data: &[u8],
    limit: usize,
) -> Result<Vec<u8>, CompressionError> {
    let (method, _) = method(content_type)
        .ok_or_else(|| CompressionError::UnknownMethod(content_type.to_string()))?;
    if data.len() > MAX_DECOMPRESSED_SIZE {
        return Err(CompressionError::Codec(
            "compressed body exceeds limit".into(),
        ));
    }
    match method {
        Method::Zlib => read_limited(flate2::read::ZlibDecoder::new(data), limit),
        Method::Bzip2 => read_limited(bzip2::read::BzDecoder::new(data), limit),
        Method::Lzma => {
            let stream = xz2::stream::Stream::new_stream_decoder(MAX_DECOMPRESSED_SIZE as u64, 0)
                .map_err(|e| CompressionError::Codec(e.to_string()))?;
            read_limited(xz2::read::XzDecoder::new_stream(data, stream), limit)
        }
        Method::Brotli => read_limited(brotli::Decompressor::new(data, 4096), limit),
        Method::Zstd => {
            let mut decoder = zstd::stream::read::Decoder::new(data)
                .map_err(|e| CompressionError::Codec(e.to_string()))?;
            decoder
                .window_log_max(26)
                .map_err(|e| CompressionError::Codec(e.to_string()))?;
            read_limited(decoder, limit)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alias_canonici() {
        for (alias, ct) in [
            ("gzip", "application/x-gzip"),
            ("zlib", "application/x-gzip"),
            ("bzip2", "application/x-bz2"),
            ("bzip", "application/x-bz2"),
            ("xz", "application/x-lzma"),
            ("lzma", "application/x-lzma"),
            ("brotli", "application/x-brotli"),
            ("zstd", "application/zstd"),
            ("zstandard", "application/zstd"),
        ] {
            assert_eq!(canonical_content_type(alias), Some(ct));
        }
        assert_eq!(canonical_content_type("sconosciuto"), None);
    }

    #[test]
    fn roundtrip_tutti_gli_algoritmi() {
        let payloads: [&[u8]; 4] = [b"", b"a", b"hello world hello world", &[0u8; 4096]];
        for name in ["gzip", "bzip2", "xz", "brotli", "zstd"] {
            for payload in payloads {
                let (compressed, ct) = compress(name, payload).unwrap();
                let back = decompress(ct, &compressed).unwrap();
                assert_eq!(back, payload, "algoritmo {name}");
            }
        }
    }

    #[test]
    fn decompression_bombs_are_bounded() {
        for name in ["gzip", "bzip2", "xz", "brotli", "zstd"] {
            let (data, _) = compress(name, &[0; 8192]).unwrap();
            assert!(decompress_limited(name, &data, 1024).is_err(), "{name}");
            assert_eq!(decompress_limited(name, &data, 8192).unwrap().len(), 8192);
        }
    }

    #[test]
    fn nome_sconosciuto() {
        assert!(matches!(
            compress("nope", b"x"),
            Err(CompressionError::UnknownMethod(_))
        ));
        assert!(matches!(
            decompress("nope", b"x"),
            Err(CompressionError::UnknownMethod(_))
        ));
    }
}
