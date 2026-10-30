use parquet::basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel};
use parquet::errors::ParquetError;

pub const SUPPORTED_CODECS: &str = "zstd, gzip, brotli, snappy, lz4_raw or uncompressed";

/// Codecs that take a level. The others reject one.
pub const LEVELED_CODECS: &str = "zstd, gzip or brotli";

#[derive(Debug)]
pub enum CompressionError {
    UnknownCodec,
    InvalidLevel(ParquetError),
    /// A level was given for a codec that has none.
    LevelNotSupported,
}

/// Maps a case-insensitive codec name and optional level to a Parquet
/// compression. An absent level takes the parquet-java default (zstd 3,
/// gzip 6, brotli 1). A level given to a codec without one is an error, so a
/// misconfiguration surfaces at startup instead of being dropped.
///
/// `lz4` (the Hadoop-framed codec, deprecated by the Parquet spec) is not
/// offered; `lz4_raw` is the spec codec.
pub fn parquet_compression(
    codec: &str,
    level: Option<u32>,
) -> Result<Compression, CompressionError> {
    let compression = match codec.trim().to_ascii_lowercase().as_str() {
        "zstd" => Compression::ZSTD(
            ZstdLevel::try_new(level.unwrap_or(3) as i32)
                .map_err(CompressionError::InvalidLevel)?,
        ),
        "gzip" => Compression::GZIP(
            GzipLevel::try_new(level.unwrap_or(6)).map_err(CompressionError::InvalidLevel)?,
        ),
        "brotli" => Compression::BROTLI(
            BrotliLevel::try_new(level.unwrap_or(1)).map_err(CompressionError::InvalidLevel)?,
        ),
        "snappy" | "lz4_raw" | "uncompressed" if level.is_some() => {
            return Err(CompressionError::LevelNotSupported);
        }
        "snappy" => Compression::SNAPPY,
        "lz4_raw" => Compression::LZ4_RAW,
        "uncompressed" => Compression::UNCOMPRESSED,
        _ => return Err(CompressionError::UnknownCodec),
    };
    Ok(compression)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn level_for_codec_without_one_is_rejected() {
        for codec in ["snappy", "lz4_raw", "uncompressed"] {
            assert!(parquet_compression(codec, None).is_ok(), "{codec}");
            assert!(
                matches!(
                    parquet_compression(codec, Some(1)),
                    Err(CompressionError::LevelNotSupported)
                ),
                "{codec}"
            );
        }
    }

    #[test]
    fn dropped_aliases_are_unknown() {
        for codec in ["none", "lz4"] {
            assert!(
                matches!(
                    parquet_compression(codec, None),
                    Err(CompressionError::UnknownCodec)
                ),
                "{codec}"
            );
        }
    }
}
