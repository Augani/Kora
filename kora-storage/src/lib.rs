//! # kora-storage
//!
//! Tiered storage engine for Kōra.
//!
//! Provides write-ahead logging, RDB-compatible snapshots, and a three-tier
//! storage system (hot RAM → warm NVMe/mmap → cold compressed disk) with
//! automatic migration based on access frequency.

#![warn(clippy::all)]
#![warn(missing_docs)]
