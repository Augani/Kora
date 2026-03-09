//! # kora-storage
//!
//! Tiered persistence engine for Kōra.
//!
//! This crate provides the durable storage stack that sits beneath Kōra's
//! in-memory shard engine. It implements a two-tier hierarchy — hot RAM
//! and cold LZ4-compressed disk — with write-ahead
//! logging and point-in-time snapshots for crash recovery.
//!
//! ## Architecture
//!
//! Persistence is split into independent, composable layers:
//!
//! - **WAL** ([`wal`]) — Append-only log with CRC-32C integrity and
//!   configurable sync policies. Every mutation is logged before
//!   acknowledgement; on restart the log is replayed to rebuild state.
//!
//! - **RDB snapshots** ([`rdb`]) — Kōra's own compact binary format for
//!   capturing a complete point-in-time database image. Snapshots are
//!   written atomically (temp file + rename) and verified with a trailing
//!   CRC-32C checksum.
//!
//! - **Cold backend** ([`backend`]) — LZ4-compressed, append-only data file
//!   with an in-memory hash index. Used for infrequently accessed data that
//!   has been evicted from hot memory.
//!
//! - **Manager** ([`manager`]) — Coordinates WAL, RDB, and cold backend
//!   through a single entry point, handling WAL rotation, snapshot-triggered
//!   truncation, and concurrent snapshot guards.
//!
//! - **Per-shard storage** ([`shard_storage`]) — Gives each shard its own
//!   WAL and RDB files in an isolated subdirectory, so shard workers can
//!   perform I/O without cross-shard contention.
//!
//! ## Modules
//!
//! - [`wal`] — Write-ahead log for crash recovery
//! - [`rdb`] — Point-in-time binary snapshots
//! - [`backend`] — Cold-tier storage backend (file-based with LZ4 compression)
//! - [`compressor`] — LZ4 compression utilities
//! - [`manager`] — Unified storage coordinator
//! - [`shard_storage`] — Per-shard WAL and RDB isolation
//! - [`iouring`] — Async I/O abstraction with platform-specific backends
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
