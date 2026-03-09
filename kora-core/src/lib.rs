//! # kora-core
//!
//! Core data structures, shard engine, and memory management for Kōra.
//!
//! This crate has **zero** workspace dependencies — it sits at the bottom of
//! the workspace dependency graph and every other Kōra crate builds on top of
//! it. The main abstractions are:
//!
//! - `types::Value` — the polymorphic value representation (inline strings,
//!   heap strings, integers, lists, sets, hashes, sorted sets, streams, vectors).
//! - `types::CompactKey` / `types::KeyEntry` — compact key storage with
//!   per-key metadata (TTL, LFU counter, storage tier).
//! - `shard::ShardStore` — a single-threaded key-value store that executes
//!   commands against its partition of the keyspace.
//! - `shard::ShardEngine` — coordinates N worker threads, each owning one
//!   `ShardStore`, and routes commands by key hash.
//! - `command::Command` / `command::CommandResponse` — the full command
//!   vocabulary and response types that bridge the protocol layer and the engine.
//!
//! The `simulation` feature gate enables a deterministic simulation testing
//! framework (see the `sim` module).

#![warn(clippy::all)]
#![warn(missing_docs)]

/// Core error types.
pub mod error;

/// Key and value types for the cache engine.
pub mod types;

/// Command and response types.
pub mod command;

/// Key hashing and shard routing.
pub mod hash;

/// Per-thread shard storage and the coordinating engine.
pub mod shard;

/// Deterministic simulation testing framework.
#[cfg(any(test, feature = "simulation"))]
pub mod sim;
