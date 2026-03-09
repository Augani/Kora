//! Cold-tier storage backend trait and implementations.
//!
//! This module defines [`StorageBackend`], the trait that all cold-tier
//! persistence implementations must satisfy, and provides [`FileBackend`] as
//! the default implementation.
//!
//! ## Design
//!
//! The cold tier stores infrequently accessed data that has been evicted from
//! hot RAM. Values are addressed by a `u64` key hash (not by the original key
//! bytes) so the backend remains decoupled from Kōra's key encoding.
//!
//! [`FileBackend`] uses a single append-only data file with LZ4 compression
//! and an in-memory index that maps each key hash to its file offset and
//! record length. Overwrites and deletes are lazy — stale records accumulate
//! until an explicit [`compact`](FileBackend::compact) pass rewrites the file.
//!
//! The index can be persisted to a sidecar `.idx` file for fast recovery
//! without scanning the data file.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::compressor;
use crate::error::{Result, StorageError};

/// A persistent storage backend for cold-tier data.
///
/// Values are stored compressed and addressed by a u64 key hash.
pub trait StorageBackend: Send + Sync {
    /// Read a value by its key hash. Returns None if not found.
    fn read(&self, key_hash: u64) -> Result<Option<Vec<u8>>>;

    /// Write a value, associating it with the given key hash.
    fn write(&self, key_hash: u64, value: &[u8]) -> Result<()>;

    /// Delete a value by its key hash.
    fn delete(&self, key_hash: u64) -> Result<bool>;

    /// Sync all pending writes to stable storage.
    fn sync(&self) -> Result<()>;
}

/// A file-based storage backend using a simple append-only data file
/// with an in-memory index.
///
/// Data layout:
/// ```text
/// [len: u32][compressed_data...]
/// ```
///
/// The index maps key_hash → (offset, length) in the data file.
pub struct FileBackend {
    data_path: PathBuf,
    inner: Mutex<FileBackendInner>,
}

struct FileBackendInner {
    file: File,
    index: HashMap<u64, (u64, u32)>,
    write_offset: u64,
    deleted: usize,
}

impl FileBackend {
    /// Open or create a file-based storage backend at the given directory.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref();
        fs::create_dir_all(dir)?;

        let data_path = dir.join("cold.dat");
        let index_path = dir.join("cold.idx");

        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&data_path)?;

        let write_offset = file.metadata()?.len();

        let index = if index_path.exists() {
            load_index(&index_path)?
        } else {
            HashMap::new()
        };

        Ok(Self {
            data_path,
            inner: Mutex::new(FileBackendInner {
                file,
                index,
                write_offset,
                deleted: 0,
            }),
        })
    }

    /// Get the number of entries stored.
    pub fn len(&self) -> Result<usize> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| StorageError::LockPoisoned(e.to_string()))?;
        Ok(inner.index.len())
    }

    /// Check if the backend is empty.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Save the index to disk for fast recovery.
    pub fn save_index(&self) -> Result<()> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| StorageError::LockPoisoned(e.to_string()))?;
        let index_path = self.data_path.with_extension("idx");
        save_index(&index_path, &inner.index)
    }

    /// Compact the data file, removing deleted entries.
    pub fn compact(&self) -> Result<usize> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| StorageError::LockPoisoned(e.to_string()))?;
        if inner.deleted == 0 {
            return Ok(0);
        }

        let tmp_path = self.data_path.with_extension("dat.tmp");
        let mut tmp_file = File::create(&tmp_path)?;
        let mut new_index = HashMap::new();
        let mut new_offset = 0u64;

        let entries: Vec<(u64, u64, u32)> = inner
            .index
            .iter()
            .map(|(&kh, &(off, len))| (kh, off, len))
            .collect();

        for (key_hash, offset, length) in entries {
            inner.file.seek(SeekFrom::Start(offset))?;
            let mut buf = vec![0u8; length as usize];
            inner.file.read_exact(&mut buf)?;

            tmp_file.write_all(&buf)?;
            new_index.insert(key_hash, (new_offset, length));
            new_offset += length as u64;
        }

        tmp_file.sync_all()?;
        drop(tmp_file);

        fs::rename(&tmp_path, &self.data_path)?;

        inner.file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.data_path)?;
        let reclaimed = inner.deleted;
        inner.index = new_index;
        inner.write_offset = new_offset;
        inner.deleted = 0;

        Ok(reclaimed)
    }
}

impl StorageBackend for FileBackend {
    fn read(&self, key_hash: u64) -> Result<Option<Vec<u8>>> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| StorageError::LockPoisoned(e.to_string()))?;
        let (offset, _length) = match inner.index.get(&key_hash) {
            Some(&entry) => entry,
            None => return Ok(None),
        };

        inner.file.seek(SeekFrom::Start(offset))?;

        let mut len_buf = [0u8; 4];
        inner.file.read_exact(&mut len_buf)?;
        let compressed_len = u32::from_le_bytes(len_buf) as usize;

        let mut compressed = vec![0u8; compressed_len];
        inner.file.read_exact(&mut compressed)?;

        let data = compressor::decompress(&compressed)?;
        Ok(Some(data))
    }

    fn write(&self, key_hash: u64, value: &[u8]) -> Result<()> {
        let compressed = compressor::compress(value);
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| StorageError::LockPoisoned(e.to_string()))?;

        if inner.index.contains_key(&key_hash) {
            inner.deleted += 1;
        }

        let offset = inner.write_offset;
        let len_bytes = (compressed.len() as u32).to_le_bytes();

        inner.file.seek(SeekFrom::Start(offset))?;
        inner.file.write_all(&len_bytes)?;
        inner.file.write_all(&compressed)?;

        let total_len = 4 + compressed.len() as u32;
        inner.index.insert(key_hash, (offset, total_len));
        inner.write_offset += total_len as u64;

        Ok(())
    }

    fn delete(&self, key_hash: u64) -> Result<bool> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| StorageError::LockPoisoned(e.to_string()))?;
        if inner.index.remove(&key_hash).is_some() {
            inner.deleted += 1;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn sync(&self) -> Result<()> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| StorageError::LockPoisoned(e.to_string()))?;
        inner.file.sync_data()?;
        Ok(())
    }
}

fn save_index(path: &Path, index: &HashMap<u64, (u64, u32)>) -> Result<()> {
    let tmp_path = path.with_extension("idx.tmp");
    let mut file = File::create(&tmp_path)?;

    let count = index.len() as u64;
    file.write_all(&count.to_le_bytes())?;

    for (&key_hash, &(offset, length)) in index {
        file.write_all(&key_hash.to_le_bytes())?;
        file.write_all(&offset.to_le_bytes())?;
        file.write_all(&length.to_le_bytes())?;
    }

    file.sync_all()?;
    fs::rename(&tmp_path, path)?;
    Ok(())
}

fn load_index(path: &Path) -> Result<HashMap<u64, (u64, u32)>> {
    let data = fs::read(path)?;
    if data.len() < 8 {
        return Ok(HashMap::new());
    }

    let mut cursor = 0usize;
    let count = u64::from_le_bytes(
        data[cursor..cursor + 8]
            .try_into()
            .map_err(|_| StorageError::CorruptIndex("invalid entry count".into()))?,
    ) as usize;
    cursor += 8;

    let mut index = HashMap::with_capacity(count);
    for _ in 0..count {
        if cursor + 20 > data.len() {
            return Err(StorageError::CorruptIndex(format!(
                "truncated index at offset {cursor}, need 20 bytes but only {} remain",
                data.len() - cursor
            )));
        }
        let key_hash = u64::from_le_bytes(
            data[cursor..cursor + 8]
                .try_into()
                .map_err(|_| StorageError::CorruptIndex("invalid key_hash".into()))?,
        );
        cursor += 8;
        let offset = u64::from_le_bytes(
            data[cursor..cursor + 8]
                .try_into()
                .map_err(|_| StorageError::CorruptIndex("invalid offset".into()))?,
        );
        cursor += 8;
        let length = u32::from_le_bytes(
            data[cursor..cursor + 4]
                .try_into()
                .map_err(|_| StorageError::CorruptIndex("invalid length".into()))?,
        );
        cursor += 4;
        index.insert(key_hash, (offset, length));
    }

    Ok(index)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_write_and_read() {
        let dir = TempDir::new().unwrap();
        let backend = FileBackend::open(dir.path()).unwrap();

        backend.write(1, b"hello world").unwrap();
        let result = backend.read(1).unwrap();
        assert_eq!(result, Some(b"hello world".to_vec()));
    }

    #[test]
    fn test_read_nonexistent() {
        let dir = TempDir::new().unwrap();
        let backend = FileBackend::open(dir.path()).unwrap();

        let result = backend.read(999).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_overwrite() {
        let dir = TempDir::new().unwrap();
        let backend = FileBackend::open(dir.path()).unwrap();

        backend.write(1, b"first").unwrap();
        backend.write(1, b"second").unwrap();

        let result = backend.read(1).unwrap();
        assert_eq!(result, Some(b"second".to_vec()));
    }

    #[test]
    fn test_delete() {
        let dir = TempDir::new().unwrap();
        let backend = FileBackend::open(dir.path()).unwrap();

        backend.write(1, b"hello").unwrap();
        assert!(backend.delete(1).unwrap());
        assert!(!backend.delete(1).unwrap());
        assert_eq!(backend.read(1).unwrap(), None);
    }

    #[test]
    fn test_multiple_keys() {
        let dir = TempDir::new().unwrap();
        let backend = FileBackend::open(dir.path()).unwrap();

        for i in 0..100u64 {
            let value = format!("value-{}", i);
            backend.write(i, value.as_bytes()).unwrap();
        }

        for i in 0..100u64 {
            let expected = format!("value-{}", i);
            let result = backend.read(i).unwrap().unwrap();
            assert_eq!(result, expected.as_bytes());
        }

        assert_eq!(backend.len().unwrap(), 100);
    }

    #[test]
    fn test_index_persistence() {
        let dir = TempDir::new().unwrap();

        // Write data and save index
        {
            let backend = FileBackend::open(dir.path()).unwrap();
            backend.write(42, b"persistent data").unwrap();
            backend.write(99, b"more data").unwrap();
            backend.save_index().unwrap();
        }

        // Reopen and verify data is still accessible
        {
            let backend = FileBackend::open(dir.path()).unwrap();
            assert_eq!(backend.len().unwrap(), 2);
            assert_eq!(backend.read(42).unwrap(), Some(b"persistent data".to_vec()));
            assert_eq!(backend.read(99).unwrap(), Some(b"more data".to_vec()));
        }
    }

    #[test]
    fn test_compact() {
        let dir = TempDir::new().unwrap();
        let backend = FileBackend::open(dir.path()).unwrap();

        // Write and delete some entries to create waste
        for i in 0..10u64 {
            backend.write(i, b"some data").unwrap();
        }
        for i in 0..5u64 {
            backend.delete(i).unwrap();
        }

        let reclaimed = backend.compact().unwrap();
        assert_eq!(reclaimed, 5);

        // Remaining entries should still be accessible
        for i in 5..10u64 {
            assert_eq!(backend.read(i).unwrap(), Some(b"some data".to_vec()));
        }
        for i in 0..5u64 {
            assert_eq!(backend.read(i).unwrap(), None);
        }
    }

    #[test]
    fn test_sync() {
        let dir = TempDir::new().unwrap();
        let backend = FileBackend::open(dir.path()).unwrap();
        backend.write(1, b"data").unwrap();
        backend.sync().unwrap(); // should not panic
    }

    #[test]
    fn test_large_values() {
        let dir = TempDir::new().unwrap();
        let backend = FileBackend::open(dir.path()).unwrap();

        let big_value = vec![0xABu8; 1_000_000];
        backend.write(1, &big_value).unwrap();

        let result = backend.read(1).unwrap().unwrap();
        assert_eq!(result, big_value);
    }
}
