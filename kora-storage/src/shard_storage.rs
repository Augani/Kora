//! Per-shard storage isolation.
//!
//! In Kōra's shared-nothing architecture each shard worker owns its data and
//! its I/O. This module provides [`ShardStorage`], which gives every shard an
//! independent subdirectory (`{base_dir}/shard-{N}/`) containing its own WAL
//! file and RDB snapshot.
//!
//! Because each [`ShardStorage`] instance is owned by exactly one shard
//! worker thread, WAL appends require only `&mut self` — no locks, no
//! cross-shard contention. The shard worker can also trigger RDB snapshots
//! independently; a successful snapshot automatically truncates that shard's
//! WAL.
//!
//! [`ShardStorage`] also implements `kora_core::shard::WalWriter`, so the
//! core engine can log mutations without knowing about the storage layer's
//! wire format.

use std::path::{Path, PathBuf};

use crate::error::Result;
use crate::rdb::{self, RdbEntry};
use crate::wal::{SyncPolicy, WalEntry, WriteAheadLog};

/// Per-shard storage with its own WAL file and RDB snapshot.
///
/// Each shard gets a directory at `{base_dir}/shard-{id}/` containing:
/// - `shard.wal` — Write-ahead log for crash recovery
/// - `shard.rdb` — Point-in-time RDB snapshot
pub struct ShardStorage {
    shard_id: u16,
    wal: Option<WriteAheadLog>,
    data_dir: PathBuf,
    wal_enabled: bool,
    rdb_enabled: bool,
    wal_max_bytes: u64,
}

impl ShardStorage {
    /// Create per-shard storage under `{base_dir}/shard-{shard_id}/`.
    pub fn open(shard_id: u16, base_dir: &Path, sync_policy: SyncPolicy) -> Result<Self> {
        Self::open_with_config(shard_id, base_dir, sync_policy, true, true, 0)
    }

    /// Create per-shard storage with full configuration control.
    pub fn open_with_config(
        shard_id: u16,
        base_dir: &Path,
        sync_policy: SyncPolicy,
        wal_enabled: bool,
        rdb_enabled: bool,
        wal_max_bytes: u64,
    ) -> Result<Self> {
        let data_dir = base_dir.join(format!("shard-{}", shard_id));
        std::fs::create_dir_all(&data_dir)?;

        let wal = if wal_enabled {
            let wal_path = data_dir.join("shard.wal");
            Some(WriteAheadLog::open(wal_path, sync_policy)?)
        } else {
            None
        };

        Ok(Self {
            shard_id,
            wal,
            data_dir,
            wal_enabled,
            rdb_enabled,
            wal_max_bytes,
        })
    }

    /// Get the shard ID.
    pub fn shard_id(&self) -> u16 {
        self.shard_id
    }

    /// Get the shard data directory.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Append an entry to this shard's WAL.
    pub fn wal_append(&mut self, entry: &WalEntry) -> Result<()> {
        if let Some(ref mut wal) = self.wal {
            wal.append(entry)?;

            if self.wal_max_bytes > 0 && wal.bytes_written() >= self.wal_max_bytes {
                tracing::info!(
                    "Shard {} WAL size {} exceeds limit {}, rotating",
                    self.shard_id,
                    wal.bytes_written(),
                    self.wal_max_bytes
                );
                wal.rotate()?;
            }
        }
        Ok(())
    }

    /// Replay this shard's WAL, calling the callback for each entry.
    pub fn wal_replay<F>(&self, callback: F) -> Result<u64>
    where
        F: FnMut(WalEntry),
    {
        if !self.wal_enabled {
            return Ok(0);
        }
        let wal_path = self.data_dir.join("shard.wal");
        WriteAheadLog::replay(wal_path, callback)
    }

    /// Truncate this shard's WAL (e.g., after a successful RDB snapshot).
    pub fn wal_truncate(&mut self) -> Result<()> {
        if let Some(ref mut wal) = self.wal {
            wal.truncate()?;
        }
        Ok(())
    }

    /// Fsync this shard's WAL.
    pub fn wal_sync(&mut self) -> Result<()> {
        if let Some(ref mut wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    /// Save an RDB snapshot for this shard.
    pub fn rdb_save(&mut self, entries: &[RdbEntry]) -> Result<()> {
        if !self.rdb_enabled {
            return Ok(());
        }

        let rdb_path = self.data_dir.join("shard.rdb");
        rdb::save(&rdb_path, entries)?;
        tracing::info!(
            "Shard {} RDB snapshot saved: {} entries",
            self.shard_id,
            entries.len()
        );
        self.wal_truncate()?;
        Ok(())
    }

    /// Load the RDB snapshot for this shard.
    pub fn rdb_load(&self) -> Result<Vec<RdbEntry>> {
        if !self.rdb_enabled {
            return Ok(vec![]);
        }
        let rdb_path = self.data_dir.join("shard.rdb");
        rdb::load(&rdb_path)
    }

    /// Get the WAL path for this shard.
    pub fn wal_path(&self) -> PathBuf {
        self.data_dir.join("shard.wal")
    }

    /// Get the RDB path for this shard.
    pub fn rdb_path(&self) -> PathBuf {
        self.data_dir.join("shard.rdb")
    }
}

impl kora_core::shard::WalWriter for ShardStorage {
    fn append(&mut self, entry: &kora_core::shard::WalRecord) {
        let wal_entry = wal_record_to_entry(entry);
        if let Err(e) = self.wal_append(&wal_entry) {
            tracing::error!("Shard {} WAL append failed: {}", self.shard_id, e);
        }
    }

    fn truncate(&mut self) -> std::result::Result<(), String> {
        self.wal_truncate().map_err(|e| e.to_string())
    }
}

/// Convert a core `WalRecord` to a storage `WalEntry`.
fn wal_record_to_entry(record: &kora_core::shard::WalRecord) -> WalEntry {
    match record {
        kora_core::shard::WalRecord::Set { key, value, ttl_ms } => WalEntry::Set {
            key: key.clone(),
            value: value.clone(),
            ttl_ms: *ttl_ms,
        },
        kora_core::shard::WalRecord::Del { key } => WalEntry::Del { key: key.clone() },
        kora_core::shard::WalRecord::Expire { key, ttl_ms } => WalEntry::Expire {
            key: key.clone(),
            ttl_ms: *ttl_ms,
        },
        kora_core::shard::WalRecord::LPush { key, values } => WalEntry::LPush {
            key: key.clone(),
            values: values.clone(),
        },
        kora_core::shard::WalRecord::RPush { key, values } => WalEntry::RPush {
            key: key.clone(),
            values: values.clone(),
        },
        kora_core::shard::WalRecord::HSet { key, fields } => WalEntry::HSet {
            key: key.clone(),
            fields: fields.clone(),
        },
        kora_core::shard::WalRecord::SAdd { key, members } => WalEntry::SAdd {
            key: key.clone(),
            members: members.clone(),
        },
        kora_core::shard::WalRecord::FlushDb => WalEntry::FlushDb,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_shard_storage_creates_directory() {
        let dir = TempDir::new().unwrap();
        let storage = ShardStorage::open(3, dir.path(), SyncPolicy::OsManaged).unwrap();
        assert!(storage.data_dir().exists());
        assert!(storage.data_dir().ends_with("shard-3"));
    }

    #[test]
    fn test_shard_wal_write_and_replay() {
        let dir = TempDir::new().unwrap();
        {
            let mut storage = ShardStorage::open(0, dir.path(), SyncPolicy::OsManaged).unwrap();
            storage
                .wal_append(&WalEntry::Set {
                    key: b"k1".to_vec(),
                    value: b"v1".to_vec(),
                    ttl_ms: None,
                })
                .unwrap();
            storage
                .wal_append(&WalEntry::Del {
                    key: b"k2".to_vec(),
                })
                .unwrap();
            storage.wal_sync().unwrap();
        }

        let storage = ShardStorage::open(0, dir.path(), SyncPolicy::OsManaged).unwrap();
        let mut entries = Vec::new();
        let count = storage.wal_replay(|e| entries.push(e)).unwrap();
        assert_eq!(count, 2);
        assert_eq!(
            entries[0],
            WalEntry::Set {
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                ttl_ms: None,
            }
        );
        assert_eq!(
            entries[1],
            WalEntry::Del {
                key: b"k2".to_vec(),
            }
        );
    }

    #[test]
    fn test_shard_rdb_save_and_load() {
        let dir = TempDir::new().unwrap();
        let mut storage = ShardStorage::open(1, dir.path(), SyncPolicy::OsManaged).unwrap();

        let entries = vec![
            RdbEntry {
                key: b"hello".to_vec(),
                value: crate::rdb::RdbValue::String(b"world".to_vec()),
                ttl_ms: None,
            },
            RdbEntry {
                key: b"num".to_vec(),
                value: crate::rdb::RdbValue::Int(42),
                ttl_ms: Some(5000),
            },
        ];

        storage.rdb_save(&entries).unwrap();
        let loaded = storage.rdb_load().unwrap();
        assert_eq!(loaded, entries);
    }

    #[test]
    fn test_rdb_save_truncates_wal() {
        let dir = TempDir::new().unwrap();
        let mut storage = ShardStorage::open(2, dir.path(), SyncPolicy::OsManaged).unwrap();

        storage
            .wal_append(&WalEntry::Set {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                ttl_ms: None,
            })
            .unwrap();

        storage
            .rdb_save(&[RdbEntry {
                key: b"k".to_vec(),
                value: crate::rdb::RdbValue::String(b"v".to_vec()),
                ttl_ms: None,
            }])
            .unwrap();

        let count = storage.wal_replay(|_| {}).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_multiple_shards_independent() {
        let dir = TempDir::new().unwrap();

        let mut s0 = ShardStorage::open(0, dir.path(), SyncPolicy::OsManaged).unwrap();
        let mut s1 = ShardStorage::open(1, dir.path(), SyncPolicy::OsManaged).unwrap();

        s0.wal_append(&WalEntry::Set {
            key: b"shard0-key".to_vec(),
            value: b"v0".to_vec(),
            ttl_ms: None,
        })
        .unwrap();
        s0.wal_sync().unwrap();

        s1.wal_append(&WalEntry::Set {
            key: b"shard1-key".to_vec(),
            value: b"v1".to_vec(),
            ttl_ms: None,
        })
        .unwrap();
        s1.wal_sync().unwrap();

        let mut e0 = Vec::new();
        let mut e1 = Vec::new();
        s0.wal_replay(|e| e0.push(e)).unwrap();
        s1.wal_replay(|e| e1.push(e)).unwrap();

        assert_eq!(e0.len(), 1);
        assert_eq!(e1.len(), 1);
        assert!(matches!(&e0[0], WalEntry::Set { key, .. } if key == b"shard0-key"));
        assert!(matches!(&e1[0], WalEntry::Set { key, .. } if key == b"shard1-key"));
    }

    #[test]
    fn test_disabled_wal() {
        let dir = TempDir::new().unwrap();
        let mut storage =
            ShardStorage::open_with_config(0, dir.path(), SyncPolicy::OsManaged, false, true, 0)
                .unwrap();

        storage
            .wal_append(&WalEntry::Set {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                ttl_ms: None,
            })
            .unwrap();

        let count = storage.wal_replay(|_| {}).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_disabled_rdb() {
        let dir = TempDir::new().unwrap();
        let mut storage =
            ShardStorage::open_with_config(0, dir.path(), SyncPolicy::OsManaged, true, false, 0)
                .unwrap();

        storage
            .rdb_save(&[RdbEntry {
                key: b"k".to_vec(),
                value: crate::rdb::RdbValue::String(b"v".to_vec()),
                ttl_ms: None,
            }])
            .unwrap();

        let loaded = storage.rdb_load().unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_idempotent_replay() {
        let dir = TempDir::new().unwrap();
        {
            let mut storage = ShardStorage::open(0, dir.path(), SyncPolicy::OsManaged).unwrap();
            storage
                .wal_append(&WalEntry::Set {
                    key: b"k".to_vec(),
                    value: b"v1".to_vec(),
                    ttl_ms: None,
                })
                .unwrap();
            storage
                .wal_append(&WalEntry::Set {
                    key: b"k".to_vec(),
                    value: b"v2".to_vec(),
                    ttl_ms: None,
                })
                .unwrap();
            storage.wal_sync().unwrap();
        }

        let storage = ShardStorage::open(0, dir.path(), SyncPolicy::OsManaged).unwrap();

        let mut entries1 = Vec::new();
        let count1 = storage.wal_replay(|e| entries1.push(e)).unwrap();

        let mut entries2 = Vec::new();
        let count2 = storage.wal_replay(|e| entries2.push(e)).unwrap();

        assert_eq!(count1, count2);
        assert_eq!(entries1, entries2);
    }
}
