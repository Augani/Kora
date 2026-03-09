//! Per-thread shard storage and the coordinating shard engine.
//!
//! Kōra partitions the keyspace into N shards. Each shard is owned by a
//! dedicated worker thread that runs a `ShardStore` — a single-threaded
//! key-value store requiring no synchronization. The `ShardEngine` sits above
//! the workers: it hashes keys to determine the owning shard, dispatches
//! commands over crossbeam channels, and fans out multi-key and keyless
//! operations across all shards.
//!
//! Sub-modules:
//! - `engine` — `ShardEngine`, worker thread loop, WAL integration.
//! - `store` — `ShardStore` and the full command execution logic.
//! - `migration` — tier migration (hot → warm → cold) with configurable
//!   thresholds.

mod engine;
pub mod migration;
mod store;
mod wal_trait;

pub use engine::{command_to_wal_record, ResponseReceiver, ShardEngine, SharedEngine};
pub use migration::{MigrationStats, TierConfig, TierMigrator};
pub use store::ShardStore;
pub use wal_trait::{WalRecord, WalWriter};
