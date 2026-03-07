//! # kora-embedded
//!
//! Embeddable library mode for Kōra.
//!
//! Provides a `Database` struct that wraps the same `ShardEngine` the server
//! uses, but routes commands through direct channel sends instead of TCP.

#![warn(clippy::all)]

use std::time::Duration;

use kora_core::command::{Command, CommandResponse};
use kora_core::shard::ShardEngine;

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
    engine: ShardEngine,
}

impl Database {
    /// Open a new database with the given configuration.
    pub fn open(config: Config) -> Self {
        let engine = ShardEngine::new(config.shard_count);
        Self { engine }
    }

    /// Get a reference to the underlying engine.
    pub fn engine(&self) -> &ShardEngine {
        &self.engine
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
}
