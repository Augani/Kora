//! # kora-embedded
//!
//! Embeddable library mode for the Kōra cache engine.
//!
//! This crate provides [`Database`], a high-level API for using Kōra as an
//! in-process library rather than a standalone network server. It wraps the
//! same multi-threaded [`ShardEngine`](kora_core::shard::ShardEngine) that
//! powers `kora-server`, but eliminates all network overhead: commands are
//! serialised into the engine's internal `Command` enum and dispatched through
//! per-shard channels directly from the calling thread.
//!
//! ## How it works
//!
//! Each [`Database`] instance owns a [`ShardEngine`](kora_core::shard::ShardEngine)
//! with a configurable number of shard worker threads. Keys are hashed to a
//! shard, and the corresponding command is sent over an `mpsc` channel to that
//! shard's worker. The calling thread blocks on a `oneshot` channel until the
//! worker replies, giving synchronous semantics with lock-free, shared-nothing
//! execution on the data path.
//!
//! ## Embedded vs. server mode
//!
//! | Aspect | `kora-embedded` | `kora-server` |
//! |--------|----------------|---------------|
//! | Transport | Direct function calls | TCP / Unix socket (RESP2) |
//! | Latency | Sub-microsecond dispatch | Network round-trip |
//! | Deployment | Linked into your binary | Separate process |
//! | Protocol | Rust types (`Command` / `CommandResponse`) | Wire-compatible RESP2 |
//!
//! ## Hybrid mode
//!
//! With the `server` Cargo feature enabled, [`Database::start_listener`] spawns
//! a TCP server alongside the embedded instance, allowing external clients to
//! connect while the host process retains direct API access.
//!
//! ## Quick start
//!
//! ```rust
//! use kora_embedded::{Config, Database};
//!
//! let db = Database::open(Config::default());
//! db.set("greeting", b"hello world");
//! assert_eq!(db.get("greeting"), Some(b"hello world".to_vec()));
//! ```

#![warn(clippy::all)]

use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::time::Duration;

use kora_core::command::{Command, CommandResponse};
use kora_core::shard::{ShardEngine, ShardStore, WalRecord, WalWriter};
use kora_doc::{
    CollectionConfig, CollectionInfo, DictionaryInfo, DocEngine, DocError, DocMutation, IndexType,
    SetResult, StorageInfo,
};
use kora_storage::shard_storage::ShardStorage;
use kora_storage::wal::{SyncPolicy, WalEntry};
use parking_lot::{Mutex, RwLock};
use serde_json::Value;

#[cfg(feature = "server")]
use tokio::task::JoinHandle;

/// Configuration for creating or updating a document collection.
pub type DocCollectionConfig = CollectionConfig;
/// Metadata snapshot for a document collection (document count, field count, etc.).
pub type DocCollectionInfo = CollectionInfo;
/// Outcome of a [`Database::doc_set`] call, indicating whether the document was created or replaced.
pub type DocSetResult = SetResult;
/// A single field-level mutation applied by [`Database::doc_update`].
pub type DocUpdateMutation = DocMutation;
/// Statistics about a collection's internal dictionary (term counts, memory usage).
pub type DocDictionaryInfo = DictionaryInfo;
/// Statistics about a collection's packed storage layer (byte counts, compaction state).
pub type DocStorageInfo = StorageInfo;

/// Configuration for opening an embedded [`Database`].
///
/// The default configuration sets `shard_count` to the number of available
/// hardware threads, which is a good starting point for most workloads.
pub struct Config {
    /// Number of shard worker threads to spawn.
    ///
    /// Each shard owns an independent key-space partition and runs on its own
    /// OS thread, so this value controls both parallelism and memory partitioning.
    pub shard_count: usize,
    /// Optional data directory for persistence. When set, WAL and RDB snapshots
    /// are stored under `{data_dir}/shard-{N}/`. Data is recovered automatically
    /// on startup.
    pub data_dir: Option<PathBuf>,
    /// WAL sync policy (only used when `data_dir` is set).
    pub wal_sync: SyncPolicy,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            shard_count: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4),
            data_dir: None,
            wal_sync: SyncPolicy::EverySecond,
        }
    }
}

/// An embedded Kōra database instance.
///
/// `Database` is the primary entry point for the embedded API. It owns a
/// multi-threaded [`ShardEngine`](kora_core::shard::ShardEngine) and a
/// [`DocEngine`](kora_doc::DocEngine), exposing typed methods for key-value,
/// list, hash, set, vector, and document operations.
///
/// All methods are safe to call from multiple threads simultaneously.
/// Key-value operations route through per-shard channels and block the caller
/// until the owning shard worker completes the command. Document operations
/// are coordinated by a `RwLock` over the in-memory document engine.
///
/// # Examples
///
/// ```rust
/// use kora_embedded::{Config, Database};
/// use std::sync::Arc;
///
/// let db = Arc::new(Database::open(Config { shard_count: 4, ..Config::default() }));
/// db.set("counter", b"0");
/// db.incr("counter").unwrap();
/// assert_eq!(db.get("counter"), Some(b"1".to_vec()));
/// ```
pub struct Database {
    engine: Arc<ShardEngine>,
    doc_engine: Arc<RwLock<DocEngine>>,
    doc_wal: Option<Mutex<Box<dyn WalWriter>>>,
}

impl Database {
    /// Open a new database with the given configuration.
    ///
    /// This spawns `config.shard_count` background worker threads that remain
    /// alive for the lifetime of the returned `Database`.
    #[allow(clippy::type_complexity)]
    pub fn open(config: Config) -> Self {
        let doc_engine = Arc::new(RwLock::new(DocEngine::new()));

        let (engine, doc_wal) = match config.data_dir {
            Some(ref data_dir) => {
                let mut wal_writers: Vec<Option<Box<dyn WalWriter>>> =
                    Vec::with_capacity(config.shard_count);
                let mut recovery_fns: Vec<Box<dyn FnOnce(usize, &mut ShardStore) + Send>> =
                    Vec::with_capacity(config.shard_count);
                let barrier = Arc::new(Barrier::new(config.shard_count + 1));

                for i in 0..config.shard_count {
                    match ShardStorage::open_with_config(
                        i as u16,
                        data_dir,
                        config.wal_sync,
                        true,
                        true,
                        0,
                    ) {
                        Ok(storage) => {
                            let doc_eng = doc_engine.clone();
                            let shard_id = i as u16;
                            let barrier_clone = barrier.clone();
                            recovery_fns.push(Box::new(move |_idx, store| {
                                recover_embedded_shard(shard_id, &storage, store, &doc_eng);
                                barrier_clone.wait();
                            }));
                            let storage2 = ShardStorage::open_with_config(
                                i as u16,
                                data_dir,
                                config.wal_sync,
                                true,
                                true,
                                0,
                            )
                            .expect("failed to reopen shard storage for WAL writing");
                            wal_writers.push(Some(Box::new(storage2)));
                        }
                        Err(e) => {
                            tracing::error!("Failed to open shard {} storage: {}", i, e);
                            wal_writers.push(None);
                            let barrier_clone = barrier.clone();
                            recovery_fns.push(Box::new(move |_, _| {
                                barrier_clone.wait();
                            }));
                        }
                    }
                }

                let doc_wal_writer: Option<Mutex<Box<dyn WalWriter>>> =
                    match ShardStorage::open_with_config(
                        0,
                        data_dir,
                        config.wal_sync,
                        true,
                        true,
                        0,
                    ) {
                        Ok(storage) => Some(Mutex::new(Box::new(storage))),
                        Err(e) => {
                            tracing::error!("Failed to open doc WAL writer: {}", e);
                            None
                        }
                    };

                let engine = Arc::new(ShardEngine::new_with_recovery(
                    config.shard_count,
                    wal_writers,
                    Some(recovery_fns),
                ));
                barrier.wait();

                (engine, doc_wal_writer)
            }
            None => (Arc::new(ShardEngine::new(config.shard_count)), None),
        };

        Self {
            engine,
            doc_engine,
            doc_wal,
        }
    }

    /// Return a reference to the underlying [`ShardEngine`](kora_core::shard::ShardEngine).
    pub fn engine(&self) -> &ShardEngine {
        &self.engine
    }

    /// Return a shared handle to the underlying [`ShardEngine`](kora_core::shard::ShardEngine).
    ///
    /// Useful when integrating with components that need their own `Arc` clone,
    /// such as hybrid server mode or custom command dispatch layers.
    pub fn shared_engine(&self) -> Arc<ShardEngine> {
        self.engine.clone()
    }

    /// Return a shared handle to the embedded [`DocEngine`](kora_doc::DocEngine).
    ///
    /// Callers are responsible for acquiring the inner `RwLock` appropriately.
    #[must_use]
    pub fn shared_doc_engine(&self) -> Arc<RwLock<DocEngine>> {
        self.doc_engine.clone()
    }

    // ─── Document operations ─────────────────────────────────────

    /// Create a document collection.
    pub fn doc_create_collection(
        &self,
        collection: &str,
        config: DocCollectionConfig,
    ) -> Result<u16, DocError> {
        self.doc_engine
            .write()
            .create_collection(collection, config)
    }

    /// Drop a document collection and all documents in it.
    pub fn doc_drop_collection(&self, collection: &str) -> bool {
        self.doc_engine.write().drop_collection(collection)
    }

    /// Return collection metadata, or `None` if the collection does not exist.
    #[must_use]
    pub fn doc_collection_info(&self, collection: &str) -> Option<DocCollectionInfo> {
        self.doc_engine.read().collection_info(collection)
    }

    /// Return dictionary statistics for a collection.
    pub fn doc_dictionary_info(&self, collection: &str) -> Result<DocDictionaryInfo, DocError> {
        self.doc_engine.read().dictionary_info(collection)
    }

    /// Return packed storage statistics for a collection.
    pub fn doc_storage_info(&self, collection: &str) -> Result<DocStorageInfo, DocError> {
        self.doc_engine.read().storage_info(collection)
    }

    /// Insert or replace one JSON document.
    pub fn doc_set(
        &self,
        collection: &str,
        doc_id: &str,
        json: &Value,
    ) -> Result<DocSetResult, DocError> {
        let result = self.doc_engine.write().set(collection, doc_id, json)?;
        self.append_doc_wal(WalRecord::DocSet {
            collection: collection.as_bytes().to_vec(),
            doc_id: doc_id.as_bytes().to_vec(),
            json: serde_json::to_vec(json).unwrap_or_default(),
        });
        Ok(result)
    }

    /// Insert or replace multiple JSON documents in one collection.
    pub fn doc_mset(
        &self,
        collection: &str,
        entries: &[(&str, &Value)],
    ) -> Result<Vec<DocSetResult>, DocError> {
        let mut engine = self.doc_engine.write();
        let results: Result<Vec<DocSetResult>, DocError> = entries
            .iter()
            .map(|(doc_id, json)| engine.set(collection, doc_id, json))
            .collect();
        drop(engine);
        if let Ok(ref _res) = results {
            for (doc_id, json) in entries {
                self.append_doc_wal(WalRecord::DocSet {
                    collection: collection.as_bytes().to_vec(),
                    doc_id: doc_id.as_bytes().to_vec(),
                    json: serde_json::to_vec(json).unwrap_or_default(),
                });
            }
        }
        results
    }

    /// Read one JSON document, optionally projecting a subset of fields.
    ///
    /// `projection` accepts dot-separated paths (e.g. `"address.city"`) to return
    /// only the requested fields. Pass `None` to return the full document.
    pub fn doc_get(
        &self,
        collection: &str,
        doc_id: &str,
        projection: Option<&[&str]>,
    ) -> Result<Option<Value>, DocError> {
        self.doc_engine.read().get(collection, doc_id, projection)
    }

    /// Read multiple JSON documents from a collection in one call.
    ///
    /// Missing documents appear as `None` in the returned vector, preserving
    /// positional correspondence with `doc_ids`.
    pub fn doc_mget(
        &self,
        collection: &str,
        doc_ids: &[&str],
    ) -> Result<Vec<Option<Value>>, DocError> {
        let engine = self.doc_engine.read();
        doc_ids
            .iter()
            .map(|doc_id| engine.get(collection, doc_id, None))
            .collect()
    }

    /// Apply one or more field-level mutations to an existing document.
    ///
    /// Returns `Ok(true)` when the document existed and was rewritten, `Ok(false)` when the
    /// target document is missing.
    pub fn doc_update(
        &self,
        collection: &str,
        doc_id: &str,
        mutations: &[DocUpdateMutation],
    ) -> Result<bool, DocError> {
        let mut engine = self.doc_engine.write();
        let updated = engine.update(collection, doc_id, mutations)?;
        if updated {
            if let Ok(Some(doc)) = engine.get(collection, doc_id, None) {
                drop(engine);
                self.append_doc_wal(WalRecord::DocSet {
                    collection: collection.as_bytes().to_vec(),
                    doc_id: doc_id.as_bytes().to_vec(),
                    json: serde_json::to_vec(&doc).unwrap_or_default(),
                });
            }
        }
        Ok(updated)
    }

    /// Delete a document. Returns `Ok(true)` if the document existed.
    pub fn doc_del(&self, collection: &str, doc_id: &str) -> Result<bool, DocError> {
        let deleted = self.doc_engine.write().del(collection, doc_id)?;
        if deleted {
            self.append_doc_wal(WalRecord::DocDel {
                collection: collection.as_bytes().to_vec(),
                doc_id: doc_id.as_bytes().to_vec(),
            });
        }
        Ok(deleted)
    }

    /// Check whether a document exists in a collection.
    pub fn doc_exists(&self, collection: &str, doc_id: &str) -> Result<bool, DocError> {
        self.doc_engine.read().exists(collection, doc_id)
    }

    /// Create a secondary index on a collection field.
    pub fn doc_create_index(
        &self,
        collection: &str,
        field: &str,
        index_type: &str,
    ) -> Result<(), DocError> {
        let idx_type = parse_index_type_str(index_type)?;
        self.doc_engine
            .write()
            .create_index(collection, field, idx_type)
    }

    /// Drop a secondary index from a collection field.
    pub fn doc_drop_index(&self, collection: &str, field: &str) -> Result<(), DocError> {
        self.doc_engine.write().drop_index(collection, field)
    }

    /// List all secondary indexes for a collection.
    ///
    /// Returns `(field_path, index_type_name)` pairs where `index_type_name`
    /// is one of `"hash"`, `"sorted"`, `"array"`, or `"unique"`.
    pub fn doc_indexes(&self, collection: &str) -> Result<Vec<(String, String)>, DocError> {
        let indexes = self.doc_engine.read().indexes(collection)?;
        Ok(indexes
            .into_iter()
            .map(|(path, idx_type)| {
                let type_name = match idx_type {
                    IndexType::Hash => "hash",
                    IndexType::Sorted => "sorted",
                    IndexType::Array => "array",
                    IndexType::Unique => "unique",
                };
                (path, type_name.to_string())
            })
            .collect())
    }

    /// Find documents matching a WHERE clause with optional projection, limit, offset, and sorting.
    #[allow(clippy::too_many_arguments)]
    pub fn doc_find(
        &self,
        collection: &str,
        where_clause: &str,
        projection: Option<&[&str]>,
        limit: Option<usize>,
        offset: usize,
        order_by: Option<&str>,
        order_desc: bool,
    ) -> Result<Vec<Value>, DocError> {
        self.doc_engine.read().find(
            collection,
            where_clause,
            projection,
            limit,
            offset,
            order_by,
            order_desc,
        )
    }

    /// Count documents matching a WHERE clause.
    pub fn doc_count(&self, collection: &str, where_clause: &str) -> Result<u64, DocError> {
        self.doc_engine.read().count(collection, where_clause)
    }

    // ─── String operations ───────────────────────────────────────

    /// Return the value stored at `key`, or `None` if the key does not exist.
    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        match self.engine.dispatch_blocking(Command::Get {
            key: key.as_bytes().to_vec(),
        }) {
            CommandResponse::BulkString(v) => Some(v),
            _ => None,
        }
    }

    /// Store a key-value pair, overwriting any existing value.
    pub fn set(&self, key: &str, value: &[u8]) {
        self.engine.dispatch_blocking(Command::Set {
            key: key.as_bytes().to_vec(),
            value: value.to_vec(),
            ex: None,
            px: None,
            nx: false,
            xx: false,
        });
    }

    /// Store a key-value pair that expires after `ttl`.
    pub fn set_ex(&self, key: &str, value: &[u8], ttl: Duration) {
        self.engine.dispatch_blocking(Command::Set {
            key: key.as_bytes().to_vec(),
            value: value.to_vec(),
            ex: Some(ttl.as_secs()),
            px: None,
            nx: false,
            xx: false,
        });
    }

    /// Delete a key. Returns `true` if the key existed.
    pub fn del(&self, key: &str) -> bool {
        matches!(
            self.engine.dispatch_blocking(Command::Del {
                keys: vec![key.as_bytes().to_vec()],
            }),
            CommandResponse::Integer(n) if n > 0
        )
    }

    /// Check whether `key` exists in the store.
    pub fn exists(&self, key: &str) -> bool {
        matches!(
            self.engine.dispatch_blocking(Command::Exists {
                keys: vec![key.as_bytes().to_vec()],
            }),
            CommandResponse::Integer(n) if n > 0
        )
    }

    /// Atomically increment a key's integer value by 1, returning the new value.
    pub fn incr(&self, key: &str) -> Result<i64, String> {
        match self.engine.dispatch_blocking(Command::Incr {
            key: key.as_bytes().to_vec(),
        }) {
            CommandResponse::Integer(n) => Ok(n),
            CommandResponse::Error(e) => Err(e),
            _ => Err("unexpected response".into()),
        }
    }

    /// Atomically set `key` to `value` and return the previous value, if any.
    pub fn getset(&self, key: &str, value: &[u8]) -> Option<Vec<u8>> {
        match self.engine.dispatch_blocking(Command::GetSet {
            key: key.as_bytes().to_vec(),
            value: value.to_vec(),
        }) {
            CommandResponse::BulkString(v) => Some(v),
            _ => None,
        }
    }

    /// Append `value` to the string stored at `key`, returning the new byte length.
    pub fn append(&self, key: &str, value: &[u8]) -> i64 {
        match self.engine.dispatch_blocking(Command::Append {
            key: key.as_bytes().to_vec(),
            value: value.to_vec(),
        }) {
            CommandResponse::Integer(n) => n,
            _ => 0,
        }
    }

    /// Get the length of the string value stored at key.
    pub fn strlen(&self, key: &str) -> i64 {
        match self.engine.dispatch_blocking(Command::Strlen {
            key: key.as_bytes().to_vec(),
        }) {
            CommandResponse::Integer(n) => n,
            _ => 0,
        }
    }

    /// Atomically decrement a key's integer value by 1, returning the new value.
    pub fn decr(&self, key: &str) -> Result<i64, String> {
        match self.engine.dispatch_blocking(Command::Decr {
            key: key.as_bytes().to_vec(),
        }) {
            CommandResponse::Integer(n) => Ok(n),
            CommandResponse::Error(e) => Err(e),
            _ => Err("unexpected response".into()),
        }
    }

    /// Atomically increment a key's integer value by `delta`, returning the new value.
    pub fn incrby(&self, key: &str, delta: i64) -> Result<i64, String> {
        match self.engine.dispatch_blocking(Command::IncrBy {
            key: key.as_bytes().to_vec(),
            delta,
        }) {
            CommandResponse::Integer(n) => Ok(n),
            CommandResponse::Error(e) => Err(e),
            _ => Err("unexpected response".into()),
        }
    }

    /// Atomically decrement a key's integer value by `delta`, returning the new value.
    pub fn decrby(&self, key: &str, delta: i64) -> Result<i64, String> {
        match self.engine.dispatch_blocking(Command::DecrBy {
            key: key.as_bytes().to_vec(),
            delta,
        }) {
            CommandResponse::Integer(n) => Ok(n),
            CommandResponse::Error(e) => Err(e),
            _ => Err("unexpected response".into()),
        }
    }

    /// Return the values of multiple keys in a single call.
    ///
    /// Missing keys appear as `None`, preserving positional correspondence with `keys`.
    pub fn mget(&self, keys: &[&str]) -> Vec<Option<Vec<u8>>> {
        let cmd_keys: Vec<Vec<u8>> = keys.iter().map(|k| k.as_bytes().to_vec()).collect();
        match self
            .engine
            .dispatch_blocking(Command::MGet { keys: cmd_keys })
        {
            CommandResponse::Array(items) => items
                .into_iter()
                .map(|r| match r {
                    CommandResponse::BulkString(v) => Some(v),
                    _ => None,
                })
                .collect(),
            _ => vec![None; keys.len()],
        }
    }

    /// Store multiple key-value pairs in a single call.
    pub fn mset(&self, entries: &[(&str, &[u8])]) {
        let cmd_entries: Vec<(Vec<u8>, Vec<u8>)> = entries
            .iter()
            .map(|(k, v)| (k.as_bytes().to_vec(), v.to_vec()))
            .collect();
        self.engine.dispatch_blocking(Command::MSet {
            entries: cmd_entries,
        });
    }

    /// Store a key-value pair only if the key does not already exist.
    ///
    /// Returns `true` if the key was set, `false` if it already existed.
    pub fn setnx(&self, key: &str, value: &[u8]) -> bool {
        matches!(
            self.engine.dispatch_blocking(Command::SetNx {
                key: key.as_bytes().to_vec(),
                value: value.to_vec(),
            }),
            CommandResponse::Integer(1)
        )
    }

    /// Set a time-to-live on `key`. Returns `true` if the key exists.
    pub fn expire(&self, key: &str, seconds: u64) -> bool {
        matches!(
            self.engine.dispatch_blocking(Command::Expire {
                key: key.as_bytes().to_vec(),
                seconds,
            }),
            CommandResponse::Integer(1)
        )
    }

    /// Remove the time-to-live on `key`, making it persistent.
    ///
    /// Returns `true` if the key existed and had a TTL.
    pub fn persist(&self, key: &str) -> bool {
        matches!(
            self.engine.dispatch_blocking(Command::Persist {
                key: key.as_bytes().to_vec(),
            }),
            CommandResponse::Integer(1)
        )
    }

    /// Return the remaining time-to-live (in seconds) for `key`.
    ///
    /// Returns `None` if the key does not exist or has no TTL set.
    pub fn ttl(&self, key: &str) -> Option<i64> {
        match self.engine.dispatch_blocking(Command::Ttl {
            key: key.as_bytes().to_vec(),
        }) {
            CommandResponse::Integer(n) if n >= 0 => Some(n),
            _ => None,
        }
    }

    /// Return the data type of the value stored at `key` (e.g. `"string"`, `"list"`, `"none"`).
    pub fn key_type(&self, key: &str) -> String {
        match self.engine.dispatch_blocking(Command::Type {
            key: key.as_bytes().to_vec(),
        }) {
            CommandResponse::SimpleString(s) => s,
            _ => "none".into(),
        }
    }

    /// Find all keys matching a glob pattern.
    pub fn keys(&self, pattern: &str) -> Vec<Vec<u8>> {
        match self.engine.dispatch_blocking(Command::Keys {
            pattern: pattern.to_string(),
        }) {
            CommandResponse::Array(items) => items
                .into_iter()
                .filter_map(|r| match r {
                    CommandResponse::BulkString(v) => Some(v),
                    _ => None,
                })
                .collect(),
            _ => vec![],
        }
    }

    // ─── List operations ─────────────────────────────────────────

    /// Prepend one or more values to a list, returning the new length.
    pub fn lpush(&self, key: &str, values: &[&[u8]]) -> i64 {
        match self.engine.dispatch_blocking(Command::LPush {
            key: key.as_bytes().to_vec(),
            values: values.iter().map(|v| v.to_vec()).collect(),
        }) {
            CommandResponse::Integer(n) => n,
            _ => 0,
        }
    }

    /// Append one or more values to a list, returning the new length.
    pub fn rpush(&self, key: &str, values: &[&[u8]]) -> i64 {
        match self.engine.dispatch_blocking(Command::RPush {
            key: key.as_bytes().to_vec(),
            values: values.iter().map(|v| v.to_vec()).collect(),
        }) {
            CommandResponse::Integer(n) => n,
            _ => 0,
        }
    }

    /// Return a contiguous range of elements from a list.
    ///
    /// Negative indices count from the end (`-1` is the last element).
    pub fn lrange(&self, key: &str, start: i64, stop: i64) -> Vec<Vec<u8>> {
        match self.engine.dispatch_blocking(Command::LRange {
            key: key.as_bytes().to_vec(),
            start,
            stop,
        }) {
            CommandResponse::Array(items) => items
                .into_iter()
                .filter_map(|r| match r {
                    CommandResponse::BulkString(v) => Some(v),
                    _ => None,
                })
                .collect(),
            _ => vec![],
        }
    }

    /// Remove and return the first element of a list.
    pub fn lpop(&self, key: &str) -> Option<Vec<u8>> {
        match self.engine.dispatch_blocking(Command::LPop {
            key: key.as_bytes().to_vec(),
        }) {
            CommandResponse::BulkString(v) => Some(v),
            _ => None,
        }
    }

    /// Remove and return the last element of a list.
    pub fn rpop(&self, key: &str) -> Option<Vec<u8>> {
        match self.engine.dispatch_blocking(Command::RPop {
            key: key.as_bytes().to_vec(),
        }) {
            CommandResponse::BulkString(v) => Some(v),
            _ => None,
        }
    }

    /// Return the number of elements in a list.
    pub fn llen(&self, key: &str) -> i64 {
        match self.engine.dispatch_blocking(Command::LLen {
            key: key.as_bytes().to_vec(),
        }) {
            CommandResponse::Integer(n) => n,
            _ => 0,
        }
    }

    /// Return the element at `index` in a list, or `None` if out of range.
    pub fn lindex(&self, key: &str, index: i64) -> Option<Vec<u8>> {
        match self.engine.dispatch_blocking(Command::LIndex {
            key: key.as_bytes().to_vec(),
            index,
        }) {
            CommandResponse::BulkString(v) => Some(v),
            _ => None,
        }
    }

    // ─── Hash operations ─────────────────────────────────────────

    /// Set a field in a hash, creating the hash if it does not exist.
    pub fn hset(&self, key: &str, field: &str, value: &[u8]) {
        self.engine.dispatch_blocking(Command::HSet {
            key: key.as_bytes().to_vec(),
            fields: vec![(field.as_bytes().to_vec(), value.to_vec())],
        });
    }

    /// Return the value of a hash field, or `None` if the field or hash does not exist.
    pub fn hget(&self, key: &str, field: &str) -> Option<Vec<u8>> {
        match self.engine.dispatch_blocking(Command::HGet {
            key: key.as_bytes().to_vec(),
            field: field.as_bytes().to_vec(),
        }) {
            CommandResponse::BulkString(v) => Some(v),
            _ => None,
        }
    }

    /// Remove one or more fields from a hash, returning the number of fields removed.
    pub fn hdel(&self, key: &str, fields: &[&str]) -> i64 {
        match self.engine.dispatch_blocking(Command::HDel {
            key: key.as_bytes().to_vec(),
            fields: fields.iter().map(|f| f.as_bytes().to_vec()).collect(),
        }) {
            CommandResponse::Integer(n) => n,
            _ => 0,
        }
    }

    /// Return all field-value pairs from a hash.
    pub fn hgetall(&self, key: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
        match self.engine.dispatch_blocking(Command::HGetAll {
            key: key.as_bytes().to_vec(),
        }) {
            CommandResponse::Array(items) => {
                let mut result = Vec::new();
                let mut iter = items.into_iter();
                while let (
                    Some(CommandResponse::BulkString(k)),
                    Some(CommandResponse::BulkString(v)),
                ) = (iter.next(), iter.next())
                {
                    result.push((k, v));
                }
                result
            }
            _ => vec![],
        }
    }

    /// Return the number of fields in a hash.
    pub fn hlen(&self, key: &str) -> i64 {
        match self.engine.dispatch_blocking(Command::HLen {
            key: key.as_bytes().to_vec(),
        }) {
            CommandResponse::Integer(n) => n,
            _ => 0,
        }
    }

    /// Check whether a field exists in a hash.
    pub fn hexists(&self, key: &str, field: &str) -> bool {
        matches!(
            self.engine.dispatch_blocking(Command::HExists {
                key: key.as_bytes().to_vec(),
                field: field.as_bytes().to_vec(),
            }),
            CommandResponse::Integer(1)
        )
    }

    /// Atomically increment a hash field's integer value by `delta`, returning the new value.
    pub fn hincrby(&self, key: &str, field: &str, delta: i64) -> Result<i64, String> {
        match self.engine.dispatch_blocking(Command::HIncrBy {
            key: key.as_bytes().to_vec(),
            field: field.as_bytes().to_vec(),
            delta,
        }) {
            CommandResponse::Integer(n) => Ok(n),
            CommandResponse::Error(e) => Err(e),
            _ => Err("unexpected response".into()),
        }
    }

    // ─── Set operations ──────────────────────────────────────────

    /// Add one or more members to a set, returning the number of new members added.
    pub fn sadd(&self, key: &str, members: &[&[u8]]) -> i64 {
        match self.engine.dispatch_blocking(Command::SAdd {
            key: key.as_bytes().to_vec(),
            members: members.iter().map(|m| m.to_vec()).collect(),
        }) {
            CommandResponse::Integer(n) => n,
            _ => 0,
        }
    }

    /// Return all members of a set.
    pub fn smembers(&self, key: &str) -> Vec<Vec<u8>> {
        match self.engine.dispatch_blocking(Command::SMembers {
            key: key.as_bytes().to_vec(),
        }) {
            CommandResponse::Array(items) => items
                .into_iter()
                .filter_map(|r| match r {
                    CommandResponse::BulkString(v) => Some(v),
                    _ => None,
                })
                .collect(),
            _ => vec![],
        }
    }

    /// Remove one or more members from a set, returning the number of members removed.
    pub fn srem(&self, key: &str, members: &[&[u8]]) -> i64 {
        match self.engine.dispatch_blocking(Command::SRem {
            key: key.as_bytes().to_vec(),
            members: members.iter().map(|m| m.to_vec()).collect(),
        }) {
            CommandResponse::Integer(n) => n,
            _ => 0,
        }
    }

    /// Check whether `member` belongs to the set stored at `key`.
    pub fn sismember(&self, key: &str, member: &[u8]) -> bool {
        matches!(
            self.engine.dispatch_blocking(Command::SIsMember {
                key: key.as_bytes().to_vec(),
                member: member.to_vec(),
            }),
            CommandResponse::Integer(1)
        )
    }

    /// Return the number of members in a set.
    pub fn scard(&self, key: &str) -> i64 {
        match self.engine.dispatch_blocking(Command::SCard {
            key: key.as_bytes().to_vec(),
        }) {
            CommandResponse::Integer(n) => n,
            _ => 0,
        }
    }

    // ─── Server operations ───────────────────────────────────────

    /// Return the total number of keys across all shards.
    pub fn db_size(&self) -> i64 {
        match self.engine.dispatch_blocking(Command::DbSize) {
            CommandResponse::Integer(n) => n,
            _ => 0,
        }
    }

    /// Remove all keys from every shard.
    pub fn flush_db(&self) {
        self.engine.dispatch_blocking(Command::FlushDb);
    }

    // ─── Vector operations ──────────────────────────────────────

    /// Insert a vector into a named index, returning the vector ID.
    ///
    /// Creates the index if it does not exist.
    pub fn vector_set(&self, index: &str, dim: usize, vector: &[f32]) -> Result<u64, String> {
        match self.engine.dispatch_blocking(Command::VecSet {
            key: index.as_bytes().to_vec(),
            dimensions: dim,
            vector: vector.to_vec(),
        }) {
            CommandResponse::Integer(id) => Ok(id as u64),
            CommandResponse::Error(e) => Err(e),
            _ => Err("unexpected response".into()),
        }
    }

    /// Search for the K nearest neighbors of a query vector.
    ///
    /// Returns a list of `(id, distance)` pairs sorted by distance.
    pub fn vector_search(
        &self,
        index: &str,
        query: &[f32],
        k: usize,
    ) -> Result<Vec<(u64, f32)>, String> {
        match self.engine.dispatch_blocking(Command::VecQuery {
            key: index.as_bytes().to_vec(),
            k,
            vector: query.to_vec(),
        }) {
            CommandResponse::Array(items) => {
                let mut results = Vec::with_capacity(items.len());
                for item in items {
                    if let CommandResponse::Array(pair) = item {
                        if pair.len() == 2 {
                            if let (
                                CommandResponse::Integer(id),
                                CommandResponse::BulkString(dist_bytes),
                            ) = (&pair[0], &pair[1])
                            {
                                let dist: f32 = String::from_utf8_lossy(dist_bytes)
                                    .parse()
                                    .unwrap_or(f32::MAX);
                                results.push((*id as u64, dist));
                            }
                        }
                    }
                }
                Ok(results)
            }
            CommandResponse::Error(e) => Err(e),
            _ => Err("unexpected response".into()),
        }
    }

    /// Delete an entire vector index. Returns true if it existed.
    pub fn vector_del(&self, index: &str) -> Result<bool, String> {
        match self.engine.dispatch_blocking(Command::VecDel {
            key: index.as_bytes().to_vec(),
        }) {
            CommandResponse::Integer(n) => Ok(n > 0),
            CommandResponse::Error(e) => Err(e),
            _ => Err("unexpected response".into()),
        }
    }

    fn append_doc_wal(&self, record: WalRecord) {
        if let Some(ref wal) = self.doc_wal {
            wal.lock().append(&record);
        }
    }

    // ─── Hybrid mode ────────────────────────────────────────────

    /// Start a TCP listener on `addr` for hybrid embedded + network mode.
    ///
    /// Returns a `JoinHandle` for the background server task and a
    /// `watch::Sender` that shuts the server down when `true` is sent.
    ///
    /// **Note:** the server creates its own shard stores -- data is not shared
    /// with the embedded `Database` key-value store. This is intended for
    /// scenarios where external clients need independent access.
    ///
    /// Requires the `server` Cargo feature.
    #[cfg(feature = "server")]
    pub fn start_listener(
        &self,
        addr: &str,
    ) -> Result<(JoinHandle<()>, tokio::sync::watch::Sender<bool>), String> {
        let config = kora_server::ServerConfig {
            bind_address: addr.to_string(),
            worker_count: self.engine.shard_count(),
            ..Default::default()
        };
        let server = kora_server::KoraServer::new(config);
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let handle = tokio::spawn(async move {
            if let Err(e) = server.run(shutdown_rx).await {
                tracing::error!("Hybrid server error: {}", e);
            }
        });

        Ok((handle, shutdown_tx))
    }
}

fn parse_index_type_str(raw: &str) -> Result<IndexType, DocError> {
    match raw.to_ascii_lowercase().as_str() {
        "hash" => Ok(IndexType::Hash),
        "sorted" => Ok(IndexType::Sorted),
        "array" => Ok(IndexType::Array),
        "unique" => Ok(IndexType::Unique),
        _ => Err(DocError::InvalidMutation(format!(
            "unknown index type '{}' (expected hash|sorted|array|unique)",
            raw
        ))),
    }
}

fn recover_embedded_shard(
    shard_id: u16,
    storage: &ShardStorage,
    store: &mut ShardStore,
    doc_engine: &RwLock<DocEngine>,
) {
    match storage.rdb_load() {
        Ok(entries) if !entries.is_empty() => {
            for entry in &entries {
                let cmd = rdb_entry_to_restore_command(entry);
                store.execute(cmd);
            }
            tracing::info!(
                "Shard {} recovered {} entries from RDB",
                shard_id,
                entries.len()
            );
        }
        Ok(_) => {}
        Err(e) => tracing::error!("Shard {} RDB load failed: {}", shard_id, e),
    }

    match storage.wal_replay(|entry| match entry {
        WalEntry::DocSet {
            collection,
            doc_id,
            json,
        } => {
            let col = String::from_utf8_lossy(&collection);
            let did = String::from_utf8_lossy(&doc_id);
            if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&json) {
                let mut engine = doc_engine.write();
                let _ = engine.create_collection(&col, Default::default());
                let _ = engine.set(&col, &did, &value);
            }
        }
        WalEntry::DocDel { collection, doc_id } => {
            let col = String::from_utf8_lossy(&collection);
            let did = String::from_utf8_lossy(&doc_id);
            let mut engine = doc_engine.write();
            let _ = engine.del(&col, &did);
        }
        WalEntry::VecSet {
            key,
            dimensions,
            vector,
        } => {
            let floats: Vec<f32> = vector
                .chunks_exact(4)
                .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect();
            store.execute(Command::VecSet {
                key,
                dimensions,
                vector: floats,
            });
        }
        WalEntry::VecDel { key } => {
            store.execute(Command::VecDel { key });
        }
        other => {
            store.execute(wal_entry_to_restore_command(other));
        }
    }) {
        Ok(count) if count > 0 => {
            tracing::info!("Shard {} replayed {} WAL entries", shard_id, count);
        }
        Ok(_) => {}
        Err(e) => tracing::error!("Shard {} WAL replay failed: {}", shard_id, e),
    }
}

fn rdb_entry_to_restore_command(entry: &kora_storage::rdb::RdbEntry) -> Command {
    use kora_storage::rdb::RdbValue;
    match &entry.value {
        RdbValue::String(v) => Command::Set {
            key: entry.key.clone(),
            value: v.clone(),
            ex: None,
            px: entry.ttl_ms,
            nx: false,
            xx: false,
        },
        RdbValue::Int(n) => Command::Set {
            key: entry.key.clone(),
            value: n.to_string().into_bytes(),
            ex: None,
            px: entry.ttl_ms,
            nx: false,
            xx: false,
        },
        RdbValue::List(items) => Command::RPush {
            key: entry.key.clone(),
            values: items.clone(),
        },
        RdbValue::Set(members) => Command::SAdd {
            key: entry.key.clone(),
            members: members.clone(),
        },
        RdbValue::Hash(fields) => Command::HSet {
            key: entry.key.clone(),
            fields: fields.clone(),
        },
    }
}

fn wal_entry_to_restore_command(entry: WalEntry) -> Command {
    match entry {
        WalEntry::Set { key, value, ttl_ms } => Command::Set {
            key,
            value,
            ex: None,
            px: ttl_ms,
            nx: false,
            xx: false,
        },
        WalEntry::Del { key } => Command::Del { keys: vec![key] },
        WalEntry::Expire { key, ttl_ms } => Command::PExpire {
            key,
            millis: ttl_ms,
        },
        WalEntry::LPush { key, values } => Command::LPush { key, values },
        WalEntry::RPush { key, values } => Command::RPush { key, values },
        WalEntry::HSet { key, fields } => Command::HSet { key, fields },
        WalEntry::SAdd { key, members } => Command::SAdd { key, members },
        WalEntry::FlushDb => Command::FlushDb,
        _ => unreachable!("doc/vec entries handled separately"),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_basic_set_get() {
        let db = Database::open(Config {
            shard_count: 2,
            ..Config::default()
        });
        db.set("hello", b"world");
        assert_eq!(db.get("hello"), Some(b"world".to_vec()));
        assert_eq!(db.get("nonexistent"), None);
    }

    #[test]
    fn test_del() {
        let db = Database::open(Config {
            shard_count: 2,
            ..Config::default()
        });
        db.set("k", b"v");
        assert!(db.del("k"));
        assert!(!db.del("k"));
        assert_eq!(db.get("k"), None);
    }

    #[test]
    fn test_incr() {
        let db = Database::open(Config {
            shard_count: 2,
            ..Config::default()
        });
        assert_eq!(db.incr("counter").unwrap(), 1);
        assert_eq!(db.incr("counter").unwrap(), 2);
        assert_eq!(db.incr("counter").unwrap(), 3);
    }

    #[test]
    fn test_list_operations() {
        let db = Database::open(Config {
            shard_count: 2,
            ..Config::default()
        });
        db.rpush("list", &[b"a", b"b", b"c"]);
        let items = db.lrange("list", 0, -1);
        assert_eq!(items, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }

    #[test]
    fn test_hash_operations() {
        let db = Database::open(Config {
            shard_count: 2,
            ..Config::default()
        });
        db.hset("user", "name", b"Alice");
        assert_eq!(db.hget("user", "name"), Some(b"Alice".to_vec()));
        assert_eq!(db.hget("user", "age"), None);
    }

    #[test]
    fn test_set_operations() {
        let db = Database::open(Config {
            shard_count: 2,
            ..Config::default()
        });
        db.sadd("tags", &[b"rust", b"cache", b"rust"]);
        let members = db.smembers("tags");
        assert_eq!(members.len(), 2); // "rust" deduplicated
    }

    #[test]
    fn test_db_size_and_flush() {
        let db = Database::open(Config {
            shard_count: 2,
            ..Config::default()
        });
        db.set("a", b"1");
        db.set("b", b"2");
        assert_eq!(db.db_size(), 2);
        db.flush_db();
        assert_eq!(db.db_size(), 0);
    }

    #[test]
    fn test_concurrent_access() {
        let db = std::sync::Arc::new(Database::open(Config {
            shard_count: 4,
            ..Config::default()
        }));
        let mut handles = vec![];
        for t in 0..4 {
            let db = db.clone();
            handles.push(std::thread::spawn(move || {
                for i in 0..100 {
                    let key = format!("t{}:k{}", t, i);
                    let val = format!("v{}", i);
                    db.set(&key, val.as_bytes());
                    assert_eq!(db.get(&key), Some(val.into_bytes()));
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn test_vector_set_search_del() {
        let db = Database::open(Config {
            shard_count: 2,
            ..Config::default()
        });

        let v1 = vec![1.0f32, 0.0, 0.0, 0.0];
        let v2 = vec![0.0f32, 1.0, 0.0, 0.0];
        let v3 = vec![1.0f32, 1.0, 0.0, 0.0];

        let id1 = db.vector_set("my_idx", 4, &v1).unwrap();
        let id2 = db.vector_set("my_idx", 4, &v2).unwrap();
        let id3 = db.vector_set("my_idx", 4, &v3).unwrap();
        assert_ne!(id1, id2);
        assert_ne!(id2, id3);

        let results = db.vector_search("my_idx", &v1, 3).unwrap();
        assert!(!results.is_empty());
        assert!(
            results[0].1 < 0.001,
            "first result should be near-exact match"
        );

        assert!(db.vector_del("my_idx").unwrap());
        assert!(!db.vector_del("my_idx").unwrap());

        let results = db.vector_search("my_idx", &v1, 3).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_vector_dimension_mismatch() {
        let db = Database::open(Config {
            shard_count: 2,
            ..Config::default()
        });
        db.vector_set("idx", 3, &[1.0, 2.0, 3.0]).unwrap();
        let result = db.vector_set("idx", 5, &[1.0, 2.0, 3.0, 4.0, 5.0]);
        assert!(result.is_err());
    }

    #[test]
    fn test_vector_search_nonexistent_index() {
        let db = Database::open(Config {
            shard_count: 2,
            ..Config::default()
        });
        let results = db.vector_search("nonexistent", &[1.0, 2.0], 5).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_document_crud() {
        let db = Database::open(Config {
            shard_count: 2,
            ..Config::default()
        });
        db.doc_create_collection("users", DocCollectionConfig::default())
            .expect("collection should be created");

        let set = db
            .doc_set(
                "users",
                "doc:1",
                &json!({
                    "name": "Augustus",
                    "age": 30,
                    "address": {"city": "Accra"}
                }),
            )
            .expect("set should succeed");
        assert!(set.created);
        assert!(db
            .doc_exists("users", "doc:1")
            .expect("exists should succeed"));

        let full = db
            .doc_get("users", "doc:1", None)
            .expect("get should succeed")
            .expect("document should exist");
        assert_eq!(
            full,
            json!({
                "name": "Augustus",
                "age": 30,
                "address": {"city": "Accra"}
            })
        );

        let projection = db
            .doc_get("users", "doc:1", Some(&["name", "address.city"]))
            .expect("projected get should succeed")
            .expect("document should exist");
        assert_eq!(
            projection,
            json!({"name": "Augustus", "address": {"city": "Accra"}})
        );

        assert!(db.doc_del("users", "doc:1").expect("delete should succeed"));
        assert!(!db
            .doc_exists("users", "doc:1")
            .expect("exists should succeed"));
    }

    #[test]
    fn test_document_collection_drop() {
        let db = Database::open(Config {
            shard_count: 2,
            ..Config::default()
        });
        db.doc_create_collection("users", DocCollectionConfig::default())
            .expect("collection should be created");
        db.doc_set("users", "doc:1", &json!({"name": "Augustus"}))
            .expect("set should succeed");

        let info = db
            .doc_collection_info("users")
            .expect("collection info should exist");
        assert_eq!(info.doc_count, 1);

        assert!(db.doc_drop_collection("users"));
        assert!(!db.doc_drop_collection("users"));
        assert!(db.doc_collection_info("users").is_none());
    }

    #[test]
    fn test_document_batch_ops() {
        let db = Database::open(Config {
            shard_count: 2,
            ..Config::default()
        });
        db.doc_create_collection("users", DocCollectionConfig::default())
            .expect("collection should be created");

        let first = json!({"name": "Augustus", "age": 30});
        let second = json!({"name": "Ada", "age": 28});
        let results = db
            .doc_mset("users", &[("doc:1", &first), ("doc:2", &second)])
            .expect("mset should succeed");
        assert_eq!(results.len(), 2);
        assert!(results[0].created);
        assert!(results[1].created);

        let docs = db
            .doc_mget("users", &["doc:1", "doc:2", "doc:missing"])
            .expect("mget should succeed");
        assert_eq!(docs, vec![Some(first), Some(second), None]);

        let dictinfo = db
            .doc_dictionary_info("users")
            .expect("dictionary info should succeed");
        assert_eq!(dictinfo.collection_name, "users");
        assert!(dictinfo.dictionary_entries >= 1);

        let storage = db
            .doc_storage_info("users")
            .expect("storage info should succeed");
        assert_eq!(storage.collection_name, "users");
        assert_eq!(storage.doc_count, 2);
        assert!(storage.total_packed_bytes > 0);
    }

    #[test]
    fn test_document_update_ops() {
        let db = Database::open(Config {
            shard_count: 2,
            ..Config::default()
        });
        db.doc_create_collection("users", DocCollectionConfig::default())
            .expect("collection should be created");
        db.doc_set(
            "users",
            "doc:1",
            &json!({
                "name": "Augustus",
                "score": 10,
                "address": {"city": "Accra"},
                "tags": ["rust", "systems", "rust"]
            }),
        )
        .expect("set should succeed");

        let updated = db
            .doc_update(
                "users",
                "doc:1",
                &[
                    DocUpdateMutation::Set {
                        path: "address.city".to_string(),
                        value: json!("London"),
                    },
                    DocUpdateMutation::Incr {
                        path: "score".to_string(),
                        delta: 0.5,
                    },
                    DocUpdateMutation::Push {
                        path: "tags".to_string(),
                        value: json!("cache"),
                    },
                    DocUpdateMutation::Pull {
                        path: "tags".to_string(),
                        value: json!("rust"),
                    },
                ],
            )
            .expect("update should succeed");
        assert!(updated);

        let doc = db
            .doc_get("users", "doc:1", None)
            .expect("get should succeed")
            .expect("doc should exist");
        assert_eq!(
            doc,
            json!({
                "name": "Augustus",
                "score": 10.5,
                "address": {"city": "London"},
                "tags": ["systems", "cache"]
            })
        );
    }

    #[test]
    fn test_persistence_survives_restart() {
        let dir = tempfile::TempDir::new().unwrap();
        let data_dir = dir.path().to_path_buf();

        {
            let db = Database::open(Config {
                shard_count: 2,
                data_dir: Some(data_dir.clone()),
                wal_sync: SyncPolicy::EveryWrite,
            });
            db.set("greeting", b"hello world");
            db.set("counter", b"42");
            db.hset("user:1", "name", b"Augustus");
            db.hset("user:1", "city", b"Accra");
            db.rpush("tasks", &[b"task-1", b"task-2"]);
            db.sadd("tags", &[b"rust", b"cache"]);
        }

        {
            let db = Database::open(Config {
                shard_count: 2,
                data_dir: Some(data_dir.clone()),
                wal_sync: SyncPolicy::EveryWrite,
            });
            assert_eq!(db.get("greeting"), Some(b"hello world".to_vec()));
            assert_eq!(db.get("counter"), Some(b"42".to_vec()));
            assert_eq!(db.hget("user:1", "name"), Some(b"Augustus".to_vec()));
            assert_eq!(db.hget("user:1", "city"), Some(b"Accra".to_vec()));
            assert_eq!(
                db.lrange("tasks", 0, -1),
                vec![b"task-1".to_vec(), b"task-2".to_vec()]
            );
            let members = db.smembers("tags");
            assert_eq!(members.len(), 2);
            assert!(db.sismember("tags", b"rust"));
            assert!(db.sismember("tags", b"cache"));
        }
    }

    #[test]
    fn test_persistence_doc_survives_restart() {
        let dir = tempfile::TempDir::new().unwrap();
        let data_dir = dir.path().to_path_buf();

        {
            let db = Database::open(Config {
                shard_count: 1,
                data_dir: Some(data_dir.clone()),
                wal_sync: SyncPolicy::EveryWrite,
            });
            db.doc_create_collection("users", DocCollectionConfig::default())
                .unwrap();
            db.doc_set(
                "users",
                "user:1",
                &json!({"name": "Augustus", "city": "Accra"}),
            )
            .unwrap();
        }

        {
            let db = Database::open(Config {
                shard_count: 1,
                data_dir: Some(data_dir.clone()),
                wal_sync: SyncPolicy::EveryWrite,
            });
            let doc = db
                .doc_get("users", "user:1", None)
                .expect("get should succeed")
                .expect("doc should exist");
            assert_eq!(doc["name"], "Augustus");
            assert_eq!(doc["city"], "Accra");
        }
    }
}
