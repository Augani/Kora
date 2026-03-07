//! Per-thread shard storage and the coordinating shard engine.
//!
//! Each worker thread owns a `ShardStore` containing a partition of the keyspace.
//! The `ShardEngine` coordinates routing commands to the correct shard.

mod store;

pub use store::ShardStore;
