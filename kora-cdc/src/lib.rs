//! # kora-cdc
//!
//! Change Data Capture (CDC) for Kōra.
//!
//! Provides per-shard ring buffers that capture every mutation, with support
//! for pattern-based subscriptions and consumer groups.
//!
//! ## Modules
//!
//! - [`ring`] — Per-shard ring buffer for mutation events
//! - [`subscription`] — Consumer subscription management
//! - [`consumer`] — Consumer group tracking with ack and redelivery

#![warn(clippy::all)]
#![warn(missing_docs)]

pub mod consumer;
pub mod ring;
pub mod subscription;
