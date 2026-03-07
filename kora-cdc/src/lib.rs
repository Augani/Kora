//! # kora-cdc
//!
//! Change Data Capture (CDC) for Kōra.
//!
//! Provides per-shard ring buffers that capture every mutation, with support
//! for pattern-based subscriptions and consumer groups.

#![warn(clippy::all)]
#![warn(missing_docs)]
