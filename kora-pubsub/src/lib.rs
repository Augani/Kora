//! Sharded Pub/Sub broker for Kōra.
//!
//! Provides a multi-threaded Pub/Sub message broker with per-channel sharding.
//! Channels are hashed to independent shards, enabling lock-free parallelism
//! for publishes to different channels.

pub mod broker;
pub mod glob;
pub mod message;

pub use broker::{MessageSink, PubSubBroker};
pub use message::PubSubMessage;
