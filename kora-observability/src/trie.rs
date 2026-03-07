//! Prefix trie for memory attribution by key prefix.
//!
//! Tracks cumulative memory usage per key prefix, allowing queries like
//! "how much memory do keys starting with `user:` consume?"

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};

use parking_lot::RwLock;

/// A trie that tracks memory usage per key prefix.
///
/// Thread-safe: the trie structure is protected by a `RwLock`, and leaf
/// counters use `AtomicI64` for lock-free updates on existing prefixes.
pub struct PrefixTrie {
    root: RwLock<TrieNode>,
}

struct TrieNode {
    size: AtomicI64,
    children: HashMap<u8, TrieNode>,
}

impl TrieNode {
    fn new() -> Self {
        Self {
            size: AtomicI64::new(0),
            children: HashMap::new(),
        }
    }
}

impl PrefixTrie {
    /// Create a new empty prefix trie.
    pub fn new() -> Self {
        Self {
            root: RwLock::new(TrieNode::new()),
        }
    }

    /// Update the memory attribution for a key.
    ///
    /// `size_delta` is added to every prefix node along the key's path.
    /// Use positive values for insertions and negative for deletions.
    pub fn track(&self, key: &[u8], size_delta: i64) {
        let mut root = self.root.write();
        root.size.fetch_add(size_delta, Ordering::Relaxed);
        let mut node = &mut *root;
        for &byte in key {
            node = node.children.entry(byte).or_insert_with(TrieNode::new);
            node.size.fetch_add(size_delta, Ordering::Relaxed);
        }
    }

    /// Query the total memory attributed to keys matching a prefix.
    ///
    /// Returns 0 if the prefix has never been tracked.
    pub fn query(&self, prefix: &[u8]) -> i64 {
        let root = self.root.read();
        let mut node = &*root;
        if prefix.is_empty() {
            return node.size.load(Ordering::Relaxed);
        }
        for &byte in prefix {
            match node.children.get(&byte) {
                Some(child) => node = child,
                None => return 0,
            }
        }
        node.size.load(Ordering::Relaxed)
    }

    /// Return the top N prefixes by memory usage at the first level of the trie.
    ///
    /// Collects single-byte prefix aggregations and returns them sorted by
    /// descending size. Each entry is `(prefix_bytes, total_size)`.
    pub fn top_prefixes(&self, n: usize) -> Vec<(Vec<u8>, i64)> {
        let root = self.root.read();
        let mut entries: Vec<(Vec<u8>, i64)> = root
            .children
            .iter()
            .map(|(&byte, node)| (vec![byte], node.size.load(Ordering::Relaxed)))
            .collect();
        entries.sort_by(|a, b| b.1.cmp(&a.1));
        entries.truncate(n);
        entries
    }

    /// Reset all counters to zero by replacing the root.
    pub fn reset(&self) {
        let mut root = self.root.write();
        *root = TrieNode::new();
    }
}

impl Default for PrefixTrie {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_track_and_query() {
        let trie = PrefixTrie::new();
        trie.track(b"user:1", 100);
        trie.track(b"user:2", 200);
        trie.track(b"order:1", 50);

        assert_eq!(trie.query(b"user:"), 300);
        assert_eq!(trie.query(b"user:1"), 100);
        assert_eq!(trie.query(b"user:2"), 200);
        assert_eq!(trie.query(b"order"), 50);
        assert_eq!(trie.query(b""), 350);
    }

    #[test]
    fn test_negative_delta() {
        let trie = PrefixTrie::new();
        trie.track(b"key", 100);
        trie.track(b"key", -30);
        assert_eq!(trie.query(b"key"), 70);
    }

    #[test]
    fn test_unknown_prefix() {
        let trie = PrefixTrie::new();
        assert_eq!(trie.query(b"nonexistent"), 0);
    }

    #[test]
    fn test_top_prefixes() {
        let trie = PrefixTrie::new();
        trie.track(b"aaa", 100);
        trie.track(b"abc", 200);
        trie.track(b"bbb", 50);
        trie.track(b"ccc", 300);

        let top = trie.top_prefixes(2);
        assert_eq!(top.len(), 2);
        assert_eq!(top[0].1, 300);
        assert_eq!(top[1].1, 300);
    }

    #[test]
    fn test_reset() {
        let trie = PrefixTrie::new();
        trie.track(b"key", 100);
        assert_eq!(trie.query(b"key"), 100);
        trie.reset();
        assert_eq!(trie.query(b"key"), 0);
    }

    #[test]
    fn test_concurrent_tracking() {
        use std::sync::Arc;
        let trie = Arc::new(PrefixTrie::new());
        let mut handles = Vec::new();
        for _ in 0..4 {
            let t = trie.clone();
            handles.push(std::thread::spawn(move || {
                for _ in 0..500 {
                    t.track(b"user:1", 1);
                }
            }));
        }
        for h in handles {
            h.join().expect("thread join");
        }
        assert_eq!(trie.query(b"user:1"), 2000);
    }
}
