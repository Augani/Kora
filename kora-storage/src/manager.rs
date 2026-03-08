//! Storage manager — coordinates WAL, RDB snapshots, and cold-tier backend.
//!
//! The `StorageManager` provides a unified interface for persistence:
//! - WAL logging of mutations for crash recovery
//! - RDB snapshot save/load for point-in-time backups
//! - Cold-tier backend for spilling infrequently-accessed data to disk

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use crate::backend::FileBackend;
use crate::error::{Result, StorageError};
use crate::rdb::{self, RdbEntry};
use crate::wal::{SyncPolicy, WalEntry, WriteAheadLog};

/// Configuration for the storage subsystem.
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Base directory for all storage files.
    pub data_dir: PathBuf,
    /// WAL sync policy.
    pub wal_sync_policy: SyncPolicy,
    /// Enable WAL.
    pub wal_enabled: bool,
    /// Enable RDB snapshots.
    pub rdb_enabled: bool,
    /// Periodic snapshot interval in seconds. `None` disables automatic snapshots.
    pub snapshot_interval_secs: Option<u64>,
    /// Number of timestamped snapshot backups to retain per shard. `None` keeps all.
    pub snapshot_retain: Option<usize>,
    /// Maximum WAL size before auto-rotation (bytes). 0 = no limit.
    pub wal_max_bytes: u64,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./kora-data"),
            wal_sync_policy: SyncPolicy::EverySecond,
            wal_enabled: true,
            rdb_enabled: true,
            snapshot_interval_secs: None,
            snapshot_retain: Some(24),
            wal_max_bytes: 64 * 1024 * 1024, // 64 MB
        }
    }
}

/// Coordinates WAL, RDB, and cold-tier storage.
pub struct StorageManager {
    config: StorageConfig,
    wal: Option<Mutex<WriteAheadLog>>,
    cold_backend: Option<FileBackend>,
    snapshot_in_progress: AtomicBool,
}

impl StorageManager {
    /// Open the storage manager with the given configuration.
    pub fn open(config: StorageConfig) -> Result<Self> {
        std::fs::create_dir_all(&config.data_dir)?;

        let wal = if config.wal_enabled {
            let wal_path = config.data_dir.join("kora.wal");
            Some(Mutex::new(WriteAheadLog::open(
                wal_path,
                config.wal_sync_policy,
            )?))
        } else {
            None
        };

        let cold_backend = {
            let cold_dir = config.data_dir.join("cold");
            Some(FileBackend::open(cold_dir)?)
        };

        Ok(Self {
            config,
            wal,
            cold_backend,
            snapshot_in_progress: AtomicBool::new(false),
        })
    }

    /// Log a mutation to the WAL.
    pub fn wal_append(&self, entry: &WalEntry) -> Result<()> {
        if let Some(ref wal) = self.wal {
            let mut wal = wal
                .lock()
                .map_err(|e| StorageError::LockPoisoned(e.to_string()))?;
            wal.append(entry)?;

            // Auto-rotate if WAL exceeds size limit
            if self.config.wal_max_bytes > 0 && wal.bytes_written() >= self.config.wal_max_bytes {
                tracing::info!(
                    "WAL size {} exceeds limit {}, rotating",
                    wal.bytes_written(),
                    self.config.wal_max_bytes
                );
                wal.rotate()?;
            }
        }
        Ok(())
    }

    /// Fsync the WAL.
    pub fn wal_sync(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.lock()
                .map_err(|e| StorageError::LockPoisoned(e.to_string()))?
                .sync()?;
        }
        Ok(())
    }

    /// Replay the WAL, calling the callback for each entry.
    pub fn wal_replay<F>(&self, callback: F) -> Result<u64>
    where
        F: FnMut(WalEntry),
    {
        if !self.config.wal_enabled {
            return Ok(0);
        }
        let wal_path = self.config.data_dir.join("kora.wal");
        WriteAheadLog::replay(wal_path, callback)
    }

    /// Truncate the WAL (e.g., after a successful snapshot).
    pub fn wal_truncate(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.lock()
                .map_err(|e| StorageError::LockPoisoned(e.to_string()))?
                .truncate()?;
        }
        Ok(())
    }

    /// Save an RDB snapshot.
    pub fn rdb_save(&self, entries: &[RdbEntry]) -> Result<()> {
        if !self.config.rdb_enabled {
            return Ok(());
        }

        if self
            .snapshot_in_progress
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            tracing::warn!("Snapshot already in progress, skipping");
            return Ok(());
        }

        let rdb_path = self.config.data_dir.join("dump.rdb");
        let result = rdb::save(&rdb_path, entries);

        self.snapshot_in_progress.store(false, Ordering::SeqCst);

        if result.is_ok() {
            tracing::info!("RDB snapshot saved: {} entries", entries.len());
            // After a successful snapshot, truncate the WAL
            self.wal_truncate()?;
        }

        result
    }

    /// Load an RDB snapshot.
    pub fn rdb_load(&self) -> Result<Vec<RdbEntry>> {
        let rdb_path = self.config.data_dir.join("dump.rdb");
        rdb::load(&rdb_path)
    }

    /// Get a reference to the cold-tier backend.
    pub fn cold_backend(&self) -> Option<&FileBackend> {
        self.cold_backend.as_ref()
    }

    /// Check whether a snapshot is currently in progress.
    pub fn is_snapshot_in_progress(&self) -> bool {
        self.snapshot_in_progress.load(Ordering::Relaxed)
    }

    /// Get the WAL path.
    pub fn wal_path(&self) -> PathBuf {
        self.config.data_dir.join("kora.wal")
    }

    /// Get the RDB path.
    pub fn rdb_path(&self) -> PathBuf {
        self.config.data_dir.join("dump.rdb")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::StorageBackend;
    use std::path::Path;
    use tempfile::TempDir;

    fn test_config(dir: &Path) -> StorageConfig {
        StorageConfig {
            data_dir: dir.to_path_buf(),
            wal_sync_policy: SyncPolicy::OsManaged,
            wal_enabled: true,
            rdb_enabled: true,
            snapshot_interval_secs: None,
            snapshot_retain: Some(24),
            wal_max_bytes: 0,
        }
    }

    #[test]
    fn test_wal_append_and_replay() {
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());

        {
            let mgr = StorageManager::open(config.clone()).unwrap();
            mgr.wal_append(&WalEntry::Set {
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                ttl_ms: None,
            })
            .unwrap();
            mgr.wal_append(&WalEntry::Set {
                key: b"k2".to_vec(),
                value: b"v2".to_vec(),
                ttl_ms: Some(5000),
            })
            .unwrap();
            mgr.wal_sync().unwrap();
        }

        {
            let mgr = StorageManager::open(config).unwrap();
            let mut entries = Vec::new();
            let count = mgr.wal_replay(|e| entries.push(e)).unwrap();
            assert_eq!(count, 2);
        }
    }

    #[test]
    fn test_rdb_save_and_load() {
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let mgr = StorageManager::open(config).unwrap();

        let entries = vec![
            RdbEntry {
                key: b"hello".to_vec(),
                value: crate::rdb::RdbValue::String(b"world".to_vec()),
                ttl_ms: None,
            },
            RdbEntry {
                key: b"counter".to_vec(),
                value: crate::rdb::RdbValue::Int(42),
                ttl_ms: Some(10000),
            },
        ];

        mgr.rdb_save(&entries).unwrap();

        let loaded = mgr.rdb_load().unwrap();
        assert_eq!(loaded, entries);
    }

    #[test]
    fn test_rdb_save_truncates_wal() {
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let mgr = StorageManager::open(config).unwrap();

        // Write to WAL
        mgr.wal_append(&WalEntry::Set {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            ttl_ms: None,
        })
        .unwrap();

        // Save RDB (should truncate WAL)
        mgr.rdb_save(&[RdbEntry {
            key: b"k".to_vec(),
            value: crate::rdb::RdbValue::String(b"v".to_vec()),
            ttl_ms: None,
        }])
        .unwrap();

        // WAL should now be empty
        let count = mgr.wal_replay(|_| {}).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_cold_backend() {
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let mgr = StorageManager::open(config).unwrap();

        let backend = mgr.cold_backend().unwrap();
        backend.write(42, b"cold data").unwrap();
        let result = backend.read(42).unwrap();
        assert_eq!(result, Some(b"cold data".to_vec()));
    }

    #[test]
    fn test_disabled_wal() {
        let dir = TempDir::new().unwrap();
        let mut config = test_config(dir.path());
        config.wal_enabled = false;

        let mgr = StorageManager::open(config).unwrap();

        // Should be no-op, not error
        mgr.wal_append(&WalEntry::Set {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            ttl_ms: None,
        })
        .unwrap();

        let count = mgr.wal_replay(|_| {}).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_full_recovery_flow() {
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());

        // Simulate: RDB has some data, WAL has newer mutations
        {
            let mgr = StorageManager::open(config.clone()).unwrap();

            // Save a baseline RDB
            mgr.rdb_save(&[
                RdbEntry {
                    key: b"baseline1".to_vec(),
                    value: crate::rdb::RdbValue::String(b"v1".to_vec()),
                    ttl_ms: None,
                },
                RdbEntry {
                    key: b"baseline2".to_vec(),
                    value: crate::rdb::RdbValue::Int(100),
                    ttl_ms: None,
                },
            ])
            .unwrap();

            // Now write more mutations to WAL (simulating post-snapshot activity)
            mgr.wal_append(&WalEntry::Set {
                key: b"new_key".to_vec(),
                value: b"new_val".to_vec(),
                ttl_ms: None,
            })
            .unwrap();
            mgr.wal_append(&WalEntry::Del {
                key: b"baseline1".to_vec(),
            })
            .unwrap();
            mgr.wal_sync().unwrap();
        }

        // Simulate restart: load RDB + replay WAL
        {
            let mgr = StorageManager::open(config).unwrap();

            let rdb_entries = mgr.rdb_load().unwrap();
            assert_eq!(rdb_entries.len(), 2);

            let mut wal_entries = Vec::new();
            let wal_count = mgr.wal_replay(|e| wal_entries.push(e)).unwrap();
            assert_eq!(wal_count, 2);

            // Verify WAL has the newer mutations
            assert!(matches!(
                &wal_entries[0],
                WalEntry::Set { key, .. } if key == b"new_key"
            ));
            assert!(matches!(
                &wal_entries[1],
                WalEntry::Del { key } if key == b"baseline1"
            ));
        }
    }
}
