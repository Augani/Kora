//! Compact key representation and key-entry metadata.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::tenant::TenantId;
use crate::types::Value;

/// Tier marker: key is in hot (RAM) tier.
pub const TIER_HOT: u8 = 0;
/// Tier marker: key is in warm (mmap) tier.
pub const TIER_WARM: u8 = 1;
/// Tier marker: key is in cold (LZ4 disk) tier.
pub const TIER_COLD: u8 = 2;

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
    /// Per-key xorshift PRNG state for LFU probabilistic increment.
    pub lfu_seed: u32,
    /// The tenant that owns this key.
    pub tenant_id: TenantId,
}

impl KeyEntry {
    /// Create a new KeyEntry with default metadata.
    pub fn new(key: CompactKey, value: Value) -> Self {
        let seed = key.as_bytes().iter().fold(0x1234_5678u32, |acc, &b| {
            acc.wrapping_mul(31).wrapping_add(b as u32)
        });
        Self {
            key,
            value,
            ttl: None,
            lfu_counter: 5,
            last_access: 0,
            flags: 0,
            lfu_seed: seed,
            tenant_id: TenantId(0),
        }
    }

    /// Create a new KeyEntry with a specific tenant owner.
    pub fn new_with_tenant(key: CompactKey, value: Value, tenant_id: TenantId) -> Self {
        let seed = key.as_bytes().iter().fold(0x1234_5678u32, |acc, &b| {
            acc.wrapping_mul(31).wrapping_add(b as u32)
        });
        Self {
            key,
            value,
            ttl: None,
            lfu_counter: 5,
            last_access: 0,
            flags: 0,
            lfu_seed: seed,
            tenant_id,
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

    /// Update the LFU counter on access using Redis's probabilistic increment.
    ///
    /// P(increment) = 1 / (counter * LFU\_LOG\_FACTOR + 1), where LFU\_LOG\_FACTOR = 10.
    pub fn touch_lfu(&mut self) {
        let counter = self.lfu_counter;
        if counter < 255 {
            self.lfu_seed ^= self.lfu_seed << 13;
            self.lfu_seed ^= self.lfu_seed >> 17;
            self.lfu_seed ^= self.lfu_seed << 5;
            let p = 1.0 / ((counter as f64) * 10.0 + 1.0);
            let r = (self.lfu_seed % 1000) as f64 / 1000.0;
            if r < p {
                self.lfu_counter = counter.saturating_add(1);
            }
        }
    }

    /// Decay the LFU counter based on elapsed time since last access.
    ///
    /// Decrements the counter by `elapsed_minutes` (like Redis lfu-decay-time=1).
    pub fn decay_lfu(&mut self, elapsed_minutes: u32) {
        self.lfu_counter = self.lfu_counter.saturating_sub(elapsed_minutes as u8);
    }

    /// Get the storage tier of this key (TIER\_HOT, TIER\_WARM, or TIER\_COLD).
    pub fn tier(&self) -> u8 {
        self.flags & 0x03
    }

    /// Set the storage tier of this key.
    pub fn set_tier(&mut self, tier: u8) {
        self.flags = (self.flags & 0xFC) | (tier & 0x03);
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

    #[test]
    fn test_touch_lfu() {
        let mut entry = KeyEntry::new(CompactKey::new(b"k"), Value::Int(1));
        assert_eq!(entry.lfu_counter, 5);
        for _ in 0..1000 {
            entry.touch_lfu();
        }
        assert!(entry.lfu_counter >= 5);
    }

    #[test]
    fn test_decay_lfu() {
        let mut entry = KeyEntry::new(CompactKey::new(b"k"), Value::Int(1));
        entry.lfu_counter = 20;
        entry.decay_lfu(5);
        assert_eq!(entry.lfu_counter, 15);
        entry.decay_lfu(100);
        assert_eq!(entry.lfu_counter, 0);
    }

    #[test]
    fn test_tier_roundtrip() {
        let mut entry = KeyEntry::new(CompactKey::new(b"k"), Value::Int(1));
        assert_eq!(entry.tier(), TIER_HOT);

        entry.set_tier(TIER_WARM);
        assert_eq!(entry.tier(), TIER_WARM);

        entry.set_tier(TIER_COLD);
        assert_eq!(entry.tier(), TIER_COLD);

        entry.set_tier(TIER_HOT);
        assert_eq!(entry.tier(), TIER_HOT);
    }

    #[test]
    fn test_tier_preserves_other_flags() {
        let mut entry = KeyEntry::new(CompactKey::new(b"k"), Value::Int(1));
        entry.flags = 0xFC;
        entry.set_tier(TIER_WARM);
        assert_eq!(entry.tier(), TIER_WARM);
        assert_eq!(entry.flags & 0xFC, 0xFC);
    }
}
