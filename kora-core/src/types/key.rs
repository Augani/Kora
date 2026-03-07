//! Compact key representation and key-entry metadata.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::types::Value;

/// A compact key representation that stores small keys inline.
///
/// Keys ≤ 64 bytes are stored directly in the enum variant, avoiding a heap
/// allocation and pointer chase. Larger keys use an `Arc<[u8]>` for shared
/// ownership.
#[derive(Clone, Debug)]
pub enum CompactKey {
    /// Key stored inline (≤ 64 bytes).
    Inline {
        /// The inline byte storage.
        data: [u8; 64],
        /// The length of the stored key.
        len: u8,
    },
    /// Key stored on the heap.
    Heap(Arc<[u8]>),
}

impl CompactKey {
    /// Create a new CompactKey from a byte slice.
    pub fn new(key: &[u8]) -> Self {
        if key.len() <= 64 {
            let mut data = [0u8; 64];
            data[..key.len()].copy_from_slice(key);
            CompactKey::Inline {
                data,
                len: key.len() as u8,
            }
        } else {
            CompactKey::Heap(Arc::from(key))
        }
    }

    /// Get the key as a byte slice.
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            CompactKey::Inline { data, len } => &data[..*len as usize],
            CompactKey::Heap(arc) => arc.as_ref(),
        }
    }
}

impl AsRef<[u8]> for CompactKey {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl PartialEq for CompactKey {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl Eq for CompactKey {}

impl std::hash::Hash for CompactKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_bytes().hash(state);
    }
}

impl PartialOrd for CompactKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CompactKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_bytes().cmp(other.as_bytes())
    }
}

/// A complete key entry stored in a shard, including metadata.
pub struct KeyEntry {
    /// The key.
    pub key: CompactKey,
    /// The value.
    pub value: Value,
    /// Optional expiry time.
    pub ttl: Option<Instant>,
    /// Probabilistic LFU counter (logarithmic, Redis-compatible).
    pub lfu_counter: u8,
    /// Seconds since shard epoch (compact timestamp for last access).
    pub last_access: u32,
    /// Encoding hints, dirty bit, tier marker.
    pub flags: u8,
}

impl KeyEntry {
    /// Create a new KeyEntry with default metadata.
    pub fn new(key: CompactKey, value: Value) -> Self {
        Self {
            key,
            value,
            ttl: None,
            lfu_counter: 5, // Initial LFU value (same as Redis default)
            last_access: 0,
            flags: 0,
        }
    }

    /// Check whether this key has expired.
    pub fn is_expired(&self) -> bool {
        self.ttl.is_some_and(|t| Instant::now() >= t)
    }

    /// Set a TTL duration from now.
    pub fn set_ttl(&mut self, duration: Duration) {
        self.ttl = Some(Instant::now() + duration);
    }

    /// Get the remaining TTL, or None if no expiry is set.
    pub fn remaining_ttl(&self) -> Option<Duration> {
        self.ttl
            .and_then(|t| t.checked_duration_since(Instant::now()))
    }

    /// Remove the TTL (persist the key).
    pub fn clear_ttl(&mut self) {
        self.ttl = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inline_key() {
        let key = CompactKey::new(b"user:123");
        assert!(matches!(key, CompactKey::Inline { .. }));
        assert_eq!(key.as_bytes(), b"user:123");
    }

    #[test]
    fn test_heap_key() {
        let long_key = vec![b'x'; 100];
        let key = CompactKey::new(&long_key);
        assert!(matches!(key, CompactKey::Heap(_)));
        assert_eq!(key.as_bytes(), long_key.as_slice());
    }

    #[test]
    fn test_key_at_boundary() {
        let key_64 = vec![b'a'; 64];
        let key = CompactKey::new(&key_64);
        assert!(matches!(key, CompactKey::Inline { .. }));

        let key_65 = vec![b'a'; 65];
        let key = CompactKey::new(&key_65);
        assert!(matches!(key, CompactKey::Heap(_)));
    }

    #[test]
    fn test_key_equality_across_representations() {
        let data = b"test-key";
        let inline = CompactKey::new(data);
        let heap = CompactKey::Heap(Arc::from(data.as_slice()));
        assert_eq!(inline, heap);
    }

    #[test]
    fn test_key_entry_ttl() {
        let entry = KeyEntry::new(CompactKey::new(b"k"), Value::Int(1));
        assert!(!entry.is_expired());
        assert!(entry.remaining_ttl().is_none());
    }

    #[test]
    fn test_key_entry_expired() {
        let mut entry = KeyEntry::new(CompactKey::new(b"k"), Value::Int(1));
        entry.ttl = Some(Instant::now() - Duration::from_secs(1));
        assert!(entry.is_expired());
    }
}
