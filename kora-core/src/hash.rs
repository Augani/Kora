//! Key hashing and shard routing.

use ahash::AHasher;
use std::hash::{Hash, Hasher};

/// Hash a key to a u64 using ahash (fast, non-cryptographic).
#[inline]
pub fn hash_key(key: &[u8]) -> u64 {
    let mut hasher = AHasher::default();
    key.hash(&mut hasher);
    hasher.finish()
}

/// Determine which shard owns a given key.
#[inline]
pub fn shard_for_key(key: &[u8], shard_count: usize) -> u16 {
    (hash_key(key) % shard_count as u64) as u16
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deterministic_hashing() {
        let h1 = hash_key(b"test-key");
        let h2 = hash_key(b"test-key");
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_different_keys_different_hashes() {
        let h1 = hash_key(b"key-a");
        let h2 = hash_key(b"key-b");
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_shard_routing_in_range() {
        for i in 0..1000u32 {
            let key = format!("key:{}", i);
            let shard = shard_for_key(key.as_bytes(), 8);
            assert!(shard < 8);
        }
    }
}
