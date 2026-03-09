//! Key and value types for the Kōra cache engine.
//!
//! This module defines the core data representations:
//!
//! - `CompactKey` — keys ≤ 64 bytes stored inline, larger keys heap-allocated
//!   via `Arc<[u8]>`. Implements `Borrow<[u8]>` for zero-copy lookups.
//! - `KeyEntry` — a key bundled with its value, TTL, LFU counter, and storage
//!   tier.
//! - `Value` — the polymorphic value enum covering strings, integers, lists,
//!   sets, hashes, sorted sets, streams, and vectors.
//! - Stream types (`StreamLog`, `StreamEntry`, `StreamId`,
//!   `StreamConsumerGroup`) — the append-only log data model with consumer
//!   group support.

mod key;
mod value;

pub use key::{CompactKey, KeyEntry, TIER_COLD, TIER_HOT, TIER_WARM};
pub use value::{
    ConsumerState, PendingEntry, StreamConsumerGroup, StreamEntry, StreamId, StreamLog, Value,
};
