//! Storage error types for the `kora-storage` crate.
//!
//! All fallible operations in this crate return [`Result<T>`], which uses
//! [`StorageError`] as the error type. Variants cover I/O failures,
//! data-integrity violations (CRC mismatches, corrupt entries), and
//! internal lock poisoning.

use thiserror::Error;

/// Errors that can occur during storage operations.
#[derive(Debug, Error)]
pub enum StorageError {
    /// An I/O error occurred.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// A CRC checksum mismatch was detected (data corruption).
    #[error("CRC mismatch: expected {expected:#010x}, got {actual:#010x}")]
    CrcMismatch {
        /// The expected CRC value.
        expected: u32,
        /// The actual CRC value.
        actual: u32,
    },

    /// The WAL entry could not be decoded.
    #[error("corrupt WAL entry: {0}")]
    CorruptEntry(String),

    /// The RDB file has an invalid format.
    #[error("invalid RDB format: {0}")]
    InvalidRdb(String),

    /// A compression/decompression error occurred.
    #[error("compression error: {0}")]
    Compression(String),

    /// A mutex lock was poisoned (another thread panicked while holding it).
    #[error("lock poisoned: {0}")]
    LockPoisoned(String),

    /// The index file has an invalid format.
    #[error("corrupt index: {0}")]
    CorruptIndex(String),
}

/// Convenience result type for storage operations.
pub type Result<T> = std::result::Result<T, StorageError>;
