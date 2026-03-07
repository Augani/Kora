//! Automatic tier migration: hot (RAM) → warm (mmap) → cold (LZ4 disk).
//!
//! The [`TierMigrator`] scans keys in a shard and identifies candidates for
//! demotion based on their LFU counter. Promotion happens synchronously on
//! read via [`ShardStore::promote`].

use crate::types::{CompactKey, TIER_COLD, TIER_HOT, TIER_WARM};

/// Configuration for automatic tier migration thresholds.
pub struct TierConfig {
    /// LFU counter below this threshold triggers demotion from hot to warm.
    pub warm_threshold: u8,
    /// LFU counter below this threshold triggers demotion from warm to cold.
    pub cold_threshold: u8,
    /// Number of keys to scan per migration cycle.
    pub scan_batch_size: usize,
}

impl Default for TierConfig {
    fn default() -> Self {
        Self {
            warm_threshold: 3,
            cold_threshold: 1,
            scan_batch_size: 100,
        }
    }
}

/// Statistics returned from a single migration scan cycle.
pub struct MigrationStats {
    /// Number of keys demoted from hot to warm.
    pub demoted_to_warm: usize,
    /// Number of keys demoted from warm to cold.
    pub demoted_to_cold: usize,
    /// Total keys scanned in this cycle.
    pub keys_scanned: usize,
}

/// Drives background tier migration for a single shard.
///
/// Call [`TierMigrator::scan_and_collect`] periodically to identify keys
/// eligible for demotion. The actual data movement is performed by the
/// server/engine layer which has access to the warm and cold backends.
pub struct TierMigrator {
    config: TierConfig,
    scan_cursor: usize,
}

impl TierMigrator {
    /// Create a new migrator with the given configuration.
    pub fn new(config: TierConfig) -> Self {
        Self {
            config,
            scan_cursor: 0,
        }
    }

    /// Get a reference to the tier configuration.
    pub fn config(&self) -> &TierConfig {
        &self.config
    }

    /// Set a new tier configuration.
    pub fn set_config(&mut self, config: TierConfig) {
        self.config = config;
    }

    /// Scan keys and return those eligible for demotion.
    ///
    /// Returns a vec of `(key, serialized_value, target_tier)` tuples.
    /// The caller is responsible for storing the data in the target tier
    /// and then calling [`super::ShardStore::mark_demoted`].
    pub fn scan_and_collect(
        &mut self,
        store: &mut super::ShardStore,
    ) -> Vec<(CompactKey, Vec<u8>, u8)> {
        let total = store.len();
        if total == 0 {
            return Vec::new();
        }

        let batch = self.config.scan_batch_size.min(total);
        let start = self.scan_cursor % total;

        let mut candidates = Vec::new();
        let mut scanned = 0;

        let keys: Vec<CompactKey> = store
            .entries_iter()
            .skip(start)
            .take(batch)
            .map(|(k, _)| k.clone())
            .collect();

        for key in &keys {
            scanned += 1;
            if let Some(entry) = store.get_entry_mut(key) {
                entry.decay_lfu(1);

                let tier = entry.tier();
                let lfu = entry.lfu_counter;

                if tier == TIER_HOT && lfu < self.config.warm_threshold {
                    let serialized = entry.value.to_bytes();
                    candidates.push((key.clone(), serialized, TIER_WARM));
                } else if tier == TIER_WARM && lfu < self.config.cold_threshold {
                    candidates.push((key.clone(), Vec::new(), TIER_COLD));
                }
            }
        }

        if scanned > 0 {
            self.scan_cursor = (start + scanned) % total;
        }

        candidates
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shard::ShardStore;
    use crate::types::{CompactKey, KeyEntry, Value};

    fn make_store_with_keys(keys: &[(&[u8], u8)]) -> ShardStore {
        let mut store = ShardStore::new(0);
        for (key, lfu) in keys {
            let compact = CompactKey::new(key);
            let mut entry = KeyEntry::new(compact.clone(), Value::from_bytes(b"value"));
            entry.lfu_counter = *lfu;
            store.insert_entry(compact, entry);
        }
        store
    }

    #[test]
    fn test_tier_config_defaults() {
        let config = TierConfig::default();
        assert_eq!(config.warm_threshold, 3);
        assert_eq!(config.cold_threshold, 1);
        assert_eq!(config.scan_batch_size, 100);
    }

    #[test]
    fn test_scan_identifies_low_lfu_keys() {
        let mut store = make_store_with_keys(&[(b"hot-key", 10), (b"cold-key", 1)]);
        let mut migrator = TierMigrator::new(TierConfig {
            warm_threshold: 3,
            cold_threshold: 1,
            scan_batch_size: 100,
        });

        let candidates = migrator.scan_and_collect(&mut store);

        let demoted_keys: Vec<&[u8]> = candidates.iter().map(|(k, _, _)| k.as_bytes()).collect();

        assert!(demoted_keys.contains(&b"cold-key".as_slice()));

        for (key, _, target) in &candidates {
            if key.as_bytes() == b"cold-key" {
                assert_eq!(*target, TIER_WARM);
            }
        }
    }

    #[test]
    fn test_hot_keys_not_demoted() {
        let mut store = make_store_with_keys(&[(b"hot1", 10), (b"hot2", 20), (b"hot3", 255)]);
        let mut migrator = TierMigrator::new(TierConfig::default());

        let candidates = migrator.scan_and_collect(&mut store);
        assert!(candidates.is_empty());
    }

    #[test]
    fn test_mark_demoted_replaces_with_warm_ref() {
        let mut store = make_store_with_keys(&[(b"mykey", 1)]);
        let key = CompactKey::new(b"mykey");

        store.mark_demoted(&key, TIER_WARM, 0xDEAD);

        let entry = store.get_entry(&key).expect("entry should exist");
        assert_eq!(entry.tier(), TIER_WARM);
        assert!(matches!(entry.value, Value::WarmRef(0xDEAD)));
    }

    #[test]
    fn test_mark_demoted_replaces_with_cold_ref() {
        let mut store = make_store_with_keys(&[(b"mykey", 0)]);
        let key = CompactKey::new(b"mykey");

        store.mark_demoted(&key, TIER_COLD, 0xBEEF);

        let entry = store.get_entry(&key).expect("entry should exist");
        assert_eq!(entry.tier(), TIER_COLD);
        assert!(matches!(entry.value, Value::ColdRef(0xBEEF)));
    }

    #[test]
    fn test_promote_restores_value() {
        let mut store = make_store_with_keys(&[(b"mykey", 1)]);
        let key = CompactKey::new(b"mykey");

        store.mark_demoted(&key, TIER_WARM, 0xDEAD);

        let restored_value = Value::from_bytes(b"restored");
        store.promote(&key, restored_value.clone());

        let entry = store.get_entry(&key).expect("entry should exist");
        assert_eq!(entry.tier(), TIER_HOT);
        assert_eq!(entry.value, restored_value);
        assert_eq!(entry.lfu_counter, 5);
    }

    #[test]
    fn test_empty_store_scan() {
        let mut store = ShardStore::new(0);
        let mut migrator = TierMigrator::new(TierConfig::default());
        let candidates = migrator.scan_and_collect(&mut store);
        assert!(candidates.is_empty());
    }
}
