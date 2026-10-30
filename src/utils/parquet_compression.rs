use parquet::basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel};
use parquet::errors::ParquetError;

pub const SUPPORTED_CODECS: &str = "zstd, gzip, brotli, snappy, lz4, lz4_raw or uncompressed";

#[derive(Debug)]
pub enum CompressionError {
    UnknownCodec,
    InvalidLevel(ParquetError),
}

/// Maps a case-insensitive codec name and optional level to a Parquet
/// compression. A level is ignored for codecs without one; an absent level
/// takes the parquet-java default (zstd 3, gzip 6, brotli 1).
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
        "snappy" => Compression::SNAPPY,
        "lz4" => Compression::LZ4,
        "lz4_raw" => Compression::LZ4_RAW,
        "uncompressed" | "none" => Compression::UNCOMPRESSED,
        _ => return Err(CompressionError::UnknownCodec),
    };
    Ok(compression)
}
