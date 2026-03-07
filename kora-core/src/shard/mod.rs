//! Per-thread shard storage and the coordinating shard engine.
//!
//! Each worker thread owns a `ShardStore` containing a partition of the keyspace.
//! The `ShardEngine` coordinates routing commands to the correct shard.

mod engine;
mod store;

pub use engine::{ShardEngine, SharedEngine};
pub use store::ShardStore;
