//! # kora-storage
//!
//! Tiered storage engine for Kōra.
//!
//! Provides write-ahead logging, RDB-compatible snapshots, and a three-tier
//! storage system (hot RAM → warm NVMe/mmap → cold compressed disk) with
//! automatic migration based on access frequency.
//!
//! ## Modules
//!
//! - [`wal`] — Write-ahead log for crash recovery
//! - [`rdb`] — Point-in-time binary snapshots
//! - [`backend`] — Cold-tier storage backend (file-based with LZ4 compression)
//! - [`compressor`] — LZ4 compression utilities
//! - [`manager`] — Unified storage coordinator
//! - [`warm_tier`] — Memory-mapped warm tier storage
//! - [`shard_storage`] — Per-shard WAL and RDB isolation
//! - [`error`] — Storage error types

#![warn(clippy::all)]
#![warn(missing_docs)]

pub mod backend;
pub mod compressor;
pub mod error;
pub mod iouring;
pub mod manager;
pub mod rdb;
pub mod shard_storage;
#[cfg(feature = "io-uring")]
pub mod uring_backend;
pub mod wal;
pub mod warm_tier;
