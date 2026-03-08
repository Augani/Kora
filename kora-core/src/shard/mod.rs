//! Per-thread shard storage and the coordinating shard engine.
//!
//! Each worker thread owns a `ShardStore` containing a partition of the keyspace.
//! The `ShardEngine` coordinates routing commands to the correct shard.

pub mod allocator;
mod engine;
pub mod migration;
mod store;
mod wal_trait;

pub use allocator::ShardAllocator;
pub use engine::{command_to_wal_record, ResponseReceiver, ShardEngine, SharedEngine};
pub use migration::{MigrationStats, TierConfig, TierMigrator};
pub use store::ShardStore;
pub use wal_trait::{WalRecord, WalWriter};
