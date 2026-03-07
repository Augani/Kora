//! # kora-core
//!
//! Core data structures, shard engine, and memory management for Kōra.
//!
//! This crate has **zero** workspace dependencies. It provides the foundational
//! types and threading model that all other Kōra crates build upon.

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
