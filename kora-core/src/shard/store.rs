//! Per-shard key-value store.

use std::collections::HashMap;

use crate::types::{CompactKey, KeyEntry, Value};

/// A single shard's key-value store.
///
/// Each worker thread owns exactly one `ShardStore`. All operations on it
/// are single-threaded — no locking required.
pub struct ShardStore {
    entries: HashMap<CompactKey, KeyEntry>,
    shard_id: u16,
}

impl ShardStore {
    /// Create a new empty shard store.
    pub fn new(shard_id: u16) -> Self {
        Self {
            entries: HashMap::new(),
            shard_id,
        }
    }

    /// Get the shard ID.
    pub fn shard_id(&self) -> u16 {
        self.shard_id
    }

    /// Get a value by key, returning None if not found or expired.
    pub fn get(&self, key: &[u8]) -> Option<&Value> {
        let compact = CompactKey::new(key);
        self.entries.get(&compact).and_then(|entry| {
            if entry.is_expired() {
                None
            } else {
                Some(&entry.value)
            }
        })
    }

    /// Set a key-value pair, returning the old value if one existed.
    pub fn set(&mut self, key: &[u8], value: Value) -> Option<Value> {
        let compact = CompactKey::new(key);
        let entry = KeyEntry::new(compact.clone(), value);
        self.entries.insert(compact, entry).map(|old| old.value)
    }

    /// Delete a key, returning true if it existed.
    pub fn del(&mut self, key: &[u8]) -> bool {
        let compact = CompactKey::new(key);
        self.entries.remove(&compact).is_some()
    }

    /// Check if a key exists (and is not expired).
    pub fn exists(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    /// Get the number of keys in this shard (including possibly expired keys).
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the shard is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Remove all expired keys, returning the count removed.
    pub fn evict_expired(&mut self) -> usize {
        let before = self.entries.len();
        self.entries.retain(|_, entry| !entry.is_expired());
        before - self.entries.len()
    }

    /// Remove all keys from this shard.
    pub fn flush(&mut self) {
        self.entries.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_and_get() {
        let mut store = ShardStore::new(0);
        store.set(b"key1", Value::from_bytes(b"value1"));
        let val = store.get(b"key1").unwrap();
        assert_eq!(val.as_bytes().unwrap(), b"value1");
    }

    #[test]
    fn test_get_nonexistent() {
        let store = ShardStore::new(0);
        assert!(store.get(b"nope").is_none());
    }

    #[test]
    fn test_set_overwrites() {
        let mut store = ShardStore::new(0);
        store.set(b"key", Value::from_bytes(b"v1"));
        let old = store.set(b"key", Value::from_bytes(b"v2"));
        assert!(old.is_some());
        assert_eq!(store.get(b"key").unwrap().as_bytes().unwrap(), b"v2");
    }

    #[test]
    fn test_del() {
        let mut store = ShardStore::new(0);
        store.set(b"key", Value::Int(42));
        assert!(store.del(b"key"));
        assert!(!store.del(b"key"));
        assert!(store.get(b"key").is_none());
    }

    #[test]
    fn test_exists() {
        let mut store = ShardStore::new(0);
        assert!(!store.exists(b"key"));
        store.set(b"key", Value::Int(1));
        assert!(store.exists(b"key"));
    }

    #[test]
    fn test_flush() {
        let mut store = ShardStore::new(0);
        store.set(b"a", Value::Int(1));
        store.set(b"b", Value::Int(2));
        assert_eq!(store.len(), 2);
        store.flush();
        assert!(store.is_empty());
    }
}
