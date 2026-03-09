//! LZ4 compression for cold-tier storage.
//!
//! Wraps `lz4_flex` to provide transparent compression and decompression of
//! values that have been demoted to the cold storage tier. The compressed
//! representation prepends the original size so decompression can allocate
//! the output buffer in a single pass.
//!
//! LZ4 was chosen for its decompression speed — cold-tier reads are latency-
//! sensitive since they serve cache misses, and LZ4 decompresses at memory-
//! bandwidth speeds while still achieving meaningful size reduction.

use crate::error::{Result, StorageError};

/// Compress data using LZ4.
pub fn compress(data: &[u8]) -> Vec<u8> {
    lz4_flex::compress_prepend_size(data)
}

/// Decompress LZ4-compressed data.
pub fn decompress(data: &[u8]) -> Result<Vec<u8>> {
    lz4_flex::decompress_size_prepended(data).map_err(|e| StorageError::Compression(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip() {
        let data = b"hello world, this is a test of compression!";
        let compressed = compress(data);
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_empty() {
        let compressed = compress(b"");
        let decompressed = decompress(&compressed).unwrap();
        assert!(decompressed.is_empty());
    }

    #[test]
    fn test_large_data() {
        // Repetitive data should compress well
        let data: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();
        let compressed = compress(&data);
        assert!(compressed.len() < data.len());
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_corrupt_data() {
        let compressed = compress(b"valid data");
        let mut corrupt = compressed.clone();
        corrupt[4] ^= 0xFF;
        assert!(decompress(&corrupt).is_err());
    }

    #[test]
    fn test_compression_ratio() {
        // Highly compressible data
        let data = vec![b'A'; 10_000];
        let compressed = compress(&data);
        // LZ4 should compress this significantly
        assert!(compressed.len() < data.len() / 10);
    }
}
