//! # kora-pubsub
//!
//! Sharded Pub/Sub broker for Kōra.
//!
//! Provides a multi-threaded Pub/Sub message broker with per-channel sharding.
//! Channels are hashed to independent shards so that publishes to different
//! channels proceed in parallel without contention. Pattern subscriptions
//! (glob-style) are replicated across all shards, since any publish could
//! potentially match.
//!
//! ## Crate Layout
//!
//! - [`broker`] — the [`PubSubBroker`] and [`MessageSink`] trait.
//! - [`message`] — the [`PubSubMessage`] enum delivered to subscribers.
//! - [`glob`] — byte-level glob matching used by pattern subscriptions.

pub mod broker;
pub mod glob;
pub mod message;

pub use broker::{MessageSink, PubSubBroker};
pub use message::PubSubMessage;
