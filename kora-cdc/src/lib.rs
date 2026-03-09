//! # kora-cdc
//!
//! Change Data Capture (CDC) for Kōra.
//!
//! Every write operation in a Kōra shard is recorded as a [`ring::CdcEvent`] in
//! a fixed-size, per-shard ring buffer. Downstream consumers read these events
//! through cursor-tracked subscriptions or through consumer groups that provide
//! at-least-once delivery with acknowledgement tracking.
//!
//! ## Modules
//!
//! - [`ring`] — Per-shard circular buffer that stores mutation events with
//!   monotonic sequence numbers and automatic eviction of the oldest entries.
//! - [`subscription`] — Lightweight cursor-based consumers with optional
//!   glob-pattern key filtering.
//! - [`consumer`] — Consumer groups with independent per-consumer cursors,
//!   pending-entry tracking, acknowledgement, and idle-timeout redelivery.

#![warn(clippy::all)]
#![warn(missing_docs)]

pub mod consumer;
pub mod ring;
pub mod subscription;
