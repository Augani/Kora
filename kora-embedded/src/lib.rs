//! # kora-embedded
//!
//! Embeddable library mode for Kōra.
//!
//! Provides a `Database` struct that wraps the same `ShardEngine` the server
//! uses, but routes commands through direct channel sends instead of TCP.

#![warn(clippy::all)]

use std::sync::Arc;
use std::time::Duration;

use kora_core::command::{Command, CommandResponse};
use kora_core::shard::ShardEngine;

#[cfg(feature = "server")]
use tokio::task::JoinHandle;

/// Configuration for the embedded database.
pub struct Config {
    /// Number of shard worker threads.
    pub shard_count: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            shard_count: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4),
        }
    }
}

/// An embedded Kōra database instance.
///
/// Uses the same multi-threaded shard engine as the server, but accessed
/// via direct function calls instead of TCP.
pub struct Database {
    engine: Arc<ShardEngine>,
}

impl Database {
    /// Open a new database with the given configuration.
    pub fn open(config: Config) -> Self {
        let engine = Arc::new(ShardEngine::new(config.shard_count));
        Self { engine }
    }

    /// Get a reference to the underlying engine.
    pub fn engine(&self) -> &ShardEngine {
        &self.engine
    }

    /// Get a shared reference to the engine (for hybrid mode).
    pub fn shared_engine(&self) -> Arc<ShardEngine> {
        self.engine.clone()
    }

    // ─── String operations ───────────────────────────────────────

    /// Get the value of a key.
    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        match self.engine.dispatch_blocking(Command::Get {
            key: key.as_bytes().to_vec(),
        }) {
            CommandResponse::BulkString(v) => Some(v),
            _ => None,
        }
    }

    /// Set a key-value pair.
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

    /// Set a key-value pair with an expiry duration.
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

    /// Delete a key, returning true if it existed.
    pub fn del(&self, key: &str) -> bool {
        matches!(
            self.engine.dispatch_blocking(Command::Del {
                keys: vec![key.as_bytes().to_vec()],
            }),
            CommandResponse::Integer(n) if n > 0
        )
    }

    /// Check if a key exists.
    pub fn exists(&self, key: &str) -> bool {
        matches!(
            self.engine.dispatch_blocking(Command::Exists {
                keys: vec![key.as_bytes().to_vec()],
            }),
            CommandResponse::Integer(n) if n > 0
        )
    }

    /// Increment a key's integer value by 1.
    pub fn incr(&self, key: &str) -> Result<i64, String> {
        match self.engine.dispatch_blocking(Command::Incr {
            key: key.as_bytes().to_vec(),
        }) {
            CommandResponse::Integer(n) => Ok(n),
            CommandResponse::Error(e) => Err(e),
            _ => Err("unexpected response".into()),
        }
    }

    /// Get the old value and set a new one.
    pub fn getset(&self, key: &str, value: &[u8]) -> Option<Vec<u8>> {
        match self.engine.dispatch_blocking(Command::GetSet {
            key: key.as_bytes().to_vec(),
            value: value.to_vec(),
        }) {
            CommandResponse::BulkString(v) => Some(v),
            _ => None,
        }
    }

    /// Append a value to a key, returning the new length.
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

    /// Decrement a key's integer value by 1.
    pub fn decr(&self, key: &str) -> Result<i64, String> {
        match self.engine.dispatch_blocking(Command::Decr {
            key: key.as_bytes().to_vec(),
        }) {
            CommandResponse::Integer(n) => Ok(n),
            CommandResponse::Error(e) => Err(e),
            _ => Err("unexpected response".into()),
        }
    }

    /// Increment a key's integer value by a given amount.
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

    /// Decrement a key's integer value by a given amount.
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

    /// Get the values of multiple keys.
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

    /// Set multiple key-value pairs.
    pub fn mset(&self, entries: &[(&str, &[u8])]) {
        let cmd_entries: Vec<(Vec<u8>, Vec<u8>)> = entries
            .iter()
            .map(|(k, v)| (k.as_bytes().to_vec(), v.to_vec()))
            .collect();
        self.engine.dispatch_blocking(Command::MSet {
            entries: cmd_entries,
        });
    }

    /// Set a key only if it does not already exist. Returns true if set.
    pub fn setnx(&self, key: &str, value: &[u8]) -> bool {
        matches!(
            self.engine.dispatch_blocking(Command::SetNx {
                key: key.as_bytes().to_vec(),
                value: value.to_vec(),
            }),
            CommandResponse::Integer(1)
        )
    }

    /// Set a TTL on a key in seconds. Returns true if the key exists.
    pub fn expire(&self, key: &str, seconds: u64) -> bool {
        matches!(
            self.engine.dispatch_blocking(Command::Expire {
                key: key.as_bytes().to_vec(),
                seconds,
            }),
            CommandResponse::Integer(1)
        )
    }

    /// Remove the TTL on a key. Returns true if the key exists and had a TTL.
    pub fn persist(&self, key: &str) -> bool {
        matches!(
            self.engine.dispatch_blocking(Command::Persist {
                key: key.as_bytes().to_vec(),
            }),
            CommandResponse::Integer(1)
        )
    }

    /// Get the TTL of a key in seconds. Returns None if no TTL or key doesn't exist.
    pub fn ttl(&self, key: &str) -> Option<i64> {
        match self.engine.dispatch_blocking(Command::Ttl {
            key: key.as_bytes().to_vec(),
        }) {
            CommandResponse::Integer(n) if n >= 0 => Some(n),
            _ => None,
        }
    }

    /// Get the type of a key as a string.
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

    /// Push values to the left of a list, returning the new length.
    pub fn lpush(&self, key: &str, values: &[&[u8]]) -> i64 {
        match self.engine.dispatch_blocking(Command::LPush {
            key: key.as_bytes().to_vec(),
            values: values.iter().map(|v| v.to_vec()).collect(),
        }) {
            CommandResponse::Integer(n) => n,
            _ => 0,
        }
    }

    /// Push values to the right of a list, returning the new length.
    pub fn rpush(&self, key: &str, values: &[&[u8]]) -> i64 {
        match self.engine.dispatch_blocking(Command::RPush {
            key: key.as_bytes().to_vec(),
            values: values.iter().map(|v| v.to_vec()).collect(),
        }) {
            CommandResponse::Integer(n) => n,
            _ => 0,
        }
    }

    /// Get a range of elements from a list.
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

    /// Pop from the left of a list.
    pub fn lpop(&self, key: &str) -> Option<Vec<u8>> {
        match self.engine.dispatch_blocking(Command::LPop {
            key: key.as_bytes().to_vec(),
        }) {
            CommandResponse::BulkString(v) => Some(v),
            _ => None,
        }
    }

    /// Pop from the right of a list.
    pub fn rpop(&self, key: &str) -> Option<Vec<u8>> {
        match self.engine.dispatch_blocking(Command::RPop {
            key: key.as_bytes().to_vec(),
        }) {
            CommandResponse::BulkString(v) => Some(v),
            _ => None,
        }
    }

    /// Get the length of a list.
    pub fn llen(&self, key: &str) -> i64 {
        match self.engine.dispatch_blocking(Command::LLen {
            key: key.as_bytes().to_vec(),
        }) {
            CommandResponse::Integer(n) => n,
            _ => 0,
        }
    }

    /// Get an element from a list by index.
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

    /// Set a hash field.
    pub fn hset(&self, key: &str, field: &str, value: &[u8]) {
        self.engine.dispatch_blocking(Command::HSet {
            key: key.as_bytes().to_vec(),
            fields: vec![(field.as_bytes().to_vec(), value.to_vec())],
        });
    }

    /// Get a hash field value.
    pub fn hget(&self, key: &str, field: &str) -> Option<Vec<u8>> {
        match self.engine.dispatch_blocking(Command::HGet {
            key: key.as_bytes().to_vec(),
            field: field.as_bytes().to_vec(),
        }) {
            CommandResponse::BulkString(v) => Some(v),
            _ => None,
        }
    }

    /// Delete hash fields, returning the number removed.
    pub fn hdel(&self, key: &str, fields: &[&str]) -> i64 {
        match self.engine.dispatch_blocking(Command::HDel {
            key: key.as_bytes().to_vec(),
            fields: fields.iter().map(|f| f.as_bytes().to_vec()).collect(),
        }) {
            CommandResponse::Integer(n) => n,
            _ => 0,
        }
    }

    /// Get all field-value pairs from a hash.
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

    /// Get the number of fields in a hash.
    pub fn hlen(&self, key: &str) -> i64 {
        match self.engine.dispatch_blocking(Command::HLen {
            key: key.as_bytes().to_vec(),
        }) {
            CommandResponse::Integer(n) => n,
            _ => 0,
        }
    }

    /// Check if a hash field exists.
    pub fn hexists(&self, key: &str, field: &str) -> bool {
        matches!(
            self.engine.dispatch_blocking(Command::HExists {
                key: key.as_bytes().to_vec(),
                field: field.as_bytes().to_vec(),
            }),
            CommandResponse::Integer(1)
        )
    }

    /// Increment a hash field by a given amount.
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

    /// Add members to a set, returning the number added.
    pub fn sadd(&self, key: &str, members: &[&[u8]]) -> i64 {
        match self.engine.dispatch_blocking(Command::SAdd {
            key: key.as_bytes().to_vec(),
            members: members.iter().map(|m| m.to_vec()).collect(),
        }) {
            CommandResponse::Integer(n) => n,
            _ => 0,
        }
    }

    /// Get all members of a set.
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

    /// Remove members from a set, returning the number removed.
    pub fn srem(&self, key: &str, members: &[&[u8]]) -> i64 {
        match self.engine.dispatch_blocking(Command::SRem {
            key: key.as_bytes().to_vec(),
            members: members.iter().map(|m| m.to_vec()).collect(),
        }) {
            CommandResponse::Integer(n) => n,
            _ => 0,
        }
    }

    /// Check if a member exists in a set.
    pub fn sismember(&self, key: &str, member: &[u8]) -> bool {
        matches!(
            self.engine.dispatch_blocking(Command::SIsMember {
                key: key.as_bytes().to_vec(),
                member: member.to_vec(),
            }),
            CommandResponse::Integer(1)
        )
    }

    /// Get the number of members in a set.
    pub fn scard(&self, key: &str) -> i64 {
        match self.engine.dispatch_blocking(Command::SCard {
            key: key.as_bytes().to_vec(),
        }) {
            CommandResponse::Integer(n) => n,
            _ => 0,
        }
    }

    // ─── Server operations ───────────────────────────────────────

    /// Get the total number of keys.
    pub fn db_size(&self) -> i64 {
        match self.engine.dispatch_blocking(Command::DbSize) {
            CommandResponse::Integer(n) => n,
            _ => 0,
        }
    }

    /// Remove all keys.
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

    // ─── Hybrid mode ────────────────────────────────────────────

    /// Start a TCP listener on the given address.
    ///
    /// Returns a `JoinHandle` for the background server task. The server runs
    /// until the returned shutdown sender is signaled or the handle is dropped.
    ///
    /// Note: the server creates its own shard stores — data is not shared with
    /// the embedded `Database` instance.
    ///
    /// Requires the `server` feature.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_set_get() {
        let db = Database::open(Config { shard_count: 2 });
        db.set("hello", b"world");
        assert_eq!(db.get("hello"), Some(b"world".to_vec()));
        assert_eq!(db.get("nonexistent"), None);
    }

    #[test]
    fn test_del() {
        let db = Database::open(Config { shard_count: 2 });
        db.set("k", b"v");
        assert!(db.del("k"));
        assert!(!db.del("k"));
        assert_eq!(db.get("k"), None);
    }

    #[test]
    fn test_incr() {
        let db = Database::open(Config { shard_count: 2 });
        assert_eq!(db.incr("counter").unwrap(), 1);
        assert_eq!(db.incr("counter").unwrap(), 2);
        assert_eq!(db.incr("counter").unwrap(), 3);
    }

    #[test]
    fn test_list_operations() {
        let db = Database::open(Config { shard_count: 2 });
        db.rpush("list", &[b"a", b"b", b"c"]);
        let items = db.lrange("list", 0, -1);
        assert_eq!(items, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }

    #[test]
    fn test_hash_operations() {
        let db = Database::open(Config { shard_count: 2 });
        db.hset("user", "name", b"Alice");
        assert_eq!(db.hget("user", "name"), Some(b"Alice".to_vec()));
        assert_eq!(db.hget("user", "age"), None);
    }

    #[test]
    fn test_set_operations() {
        let db = Database::open(Config { shard_count: 2 });
        db.sadd("tags", &[b"rust", b"cache", b"rust"]);
        let members = db.smembers("tags");
        assert_eq!(members.len(), 2); // "rust" deduplicated
    }

    #[test]
    fn test_db_size_and_flush() {
        let db = Database::open(Config { shard_count: 2 });
        db.set("a", b"1");
        db.set("b", b"2");
        assert_eq!(db.db_size(), 2);
        db.flush_db();
        assert_eq!(db.db_size(), 0);
    }

    #[test]
    fn test_concurrent_access() {
        let db = std::sync::Arc::new(Database::open(Config { shard_count: 4 }));
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
        let db = Database::open(Config { shard_count: 2 });

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
        let db = Database::open(Config { shard_count: 2 });
        db.vector_set("idx", 3, &[1.0, 2.0, 3.0]).unwrap();
        let result = db.vector_set("idx", 5, &[1.0, 2.0, 3.0, 4.0, 5.0]);
        assert!(result.is_err());
    }

    #[test]
    fn test_vector_search_nonexistent_index() {
        let db = Database::open(Config { shard_count: 2 });
        let results = db.vector_search("nonexistent", &[1.0, 2.0], 5).unwrap();
        assert!(results.is_empty());
    }
}
