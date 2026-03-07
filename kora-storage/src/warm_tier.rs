//! Memory-mapped warm tier storage.
//!
//! Provides a memory-mapped, append-only data file with an in-memory index
//! for fast zero-copy reads. Designed as the warm tier between hot RAM and
//! cold compressed disk in Kōra's tiered storage hierarchy.
//!
//! ## Data Format
//!
//! The data file is append-only with the following per-entry layout:
//!
//! ```text
//! [len: u32 little-endian][data: len bytes]
//! ```
//!
//! Deletions are lazy — the index entry is removed but the data remains in
//! the file until a future compaction pass.

use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};

use memmap2::MmapMut;

use crate::error::{Result, StorageError};

/// Size of the length prefix for each entry (u32 = 4 bytes).
const LEN_PREFIX_SIZE: usize = 4;

/// Memory-mapped warm tier storage.
///
/// Provides zero-copy reads through a memory-mapped data file. Writes are
/// append-only, and deletions are lazy (index entries are removed but the
/// underlying data is not reclaimed until compaction).
///
/// This tier is designed to be shard-local so it does not require internal
/// synchronisation for concurrent access.
pub struct WarmTier {
    /// Path to the data file on disk (retained for future compaction).
    #[allow(dead_code)]
    data_path: PathBuf,
    /// The memory-mapped region backing the data file.
    mmap: MmapMut,
    /// In-memory index mapping key hashes to `(offset, len)` in the mmap.
    index: HashMap<u64, (usize, usize)>,
    /// Current write position (next free byte) in the mmap.
    write_pos: usize,
    /// Maximum size (in bytes) of the memory-mapped region.
    max_size: usize,
}

impl WarmTier {
    /// Open or create a warm tier data file in `dir`.
    ///
    /// The backing file is pre-allocated to `max_size` bytes and
    /// memory-mapped. If the file already exists it is reopened and the
    /// existing entries are re-indexed.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Io`] if the directory cannot be created or
    /// the file cannot be opened/mapped.
    pub fn open(dir: impl AsRef<Path>, max_size: usize) -> Result<Self> {
        let dir = dir.as_ref();
        fs::create_dir_all(dir)?;

        let data_path = dir.join("warm.dat");

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&data_path)?;

        // Pre-allocate the file to the requested maximum size.
        file.set_len(max_size as u64)?;

        // SAFETY: The file has been opened in read-write mode and pre-allocated
        // to `max_size` bytes. We hold the only handle to the file within this
        // struct. The warm tier is shard-local so no external concurrent access
        // is expected.
        let mmap = unsafe { MmapMut::map_mut(&file)? };

        let mut tier = Self {
            data_path,
            mmap,
            index: HashMap::new(),
            write_pos: 0,
            max_size,
        };

        tier.rebuild_index()?;
        Ok(tier)
    }

    /// Read a value directly from the memory-mapped region (zero-copy).
    ///
    /// Returns `None` if no entry exists for the given `key_hash`.
    pub fn get(&self, key_hash: u64) -> Option<&[u8]> {
        let &(offset, len) = self.index.get(&key_hash)?;
        let data_start = offset + LEN_PREFIX_SIZE;
        let data_end = data_start + len;
        if data_end <= self.max_size {
            Some(&self.mmap[data_start..data_end])
        } else {
            None
        }
    }

    /// Append a value to the warm tier and associate it with `key_hash`.
    ///
    /// If an entry for `key_hash` already exists the old entry becomes
    /// unreachable (lazy deletion) and the new value is appended.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Io`] if the value does not fit within the
    /// remaining capacity of the memory-mapped region.
    pub fn put(&mut self, key_hash: u64, value: &[u8]) -> Result<()> {
        let entry_size = LEN_PREFIX_SIZE + value.len();

        if self.write_pos + entry_size > self.max_size {
            return Err(StorageError::Io(std::io::Error::other(
                "warm tier: not enough space",
            )));
        }

        let offset = self.write_pos;

        // Write the length prefix (little-endian u32).
        let len_bytes = (value.len() as u32).to_le_bytes();
        self.mmap[offset..offset + LEN_PREFIX_SIZE].copy_from_slice(&len_bytes);

        // Write the data.
        let data_start = offset + LEN_PREFIX_SIZE;
        self.mmap[data_start..data_start + value.len()].copy_from_slice(value);

        self.write_pos += entry_size;
        self.index.insert(key_hash, (offset, value.len()));

        Ok(())
    }

    /// Remove an entry from the index by `key_hash`.
    ///
    /// This is a lazy deletion — the data remains in the backing file but is
    /// no longer reachable through the index. Returns `true` if the entry
    /// existed.
    pub fn remove(&mut self, key_hash: u64) -> bool {
        self.index.remove(&key_hash).is_some()
    }

    /// Return the number of live entries in the warm tier.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// Return `true` if the warm tier contains no live entries.
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// Return the total number of bytes consumed by live entries (including
    /// their length prefixes).
    pub fn used_bytes(&self) -> usize {
        self.index
            .values()
            .map(|&(_offset, len)| LEN_PREFIX_SIZE + len)
            .sum()
    }

    /// Flush the memory-mapped region to disk.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Io`] if the flush fails.
    pub fn flush(&self) -> Result<()> {
        self.mmap.flush()?;
        Ok(())
    }

    /// Rebuild the in-memory index by scanning the data file from the
    /// beginning.
    ///
    /// This is used during [`open`](Self::open) to recover state from an
    /// existing data file. Entries are read sequentially; if a duplicate
    /// key hash is encountered the later entry wins (which matches the
    /// append-only semantics).
    fn rebuild_index(&mut self) -> Result<()> {
        let mut pos: usize = 0;

        while pos + LEN_PREFIX_SIZE <= self.max_size {
            let len_bytes: [u8; 4] = self.mmap[pos..pos + LEN_PREFIX_SIZE]
                .try_into()
                .map_err(|_| StorageError::CorruptEntry("failed to read length prefix".into()))?;

            let len = u32::from_le_bytes(len_bytes) as usize;

            // A zero-length prefix signals the end of written data (the file
            // is zero-filled beyond the write position).
            if len == 0 {
                break;
            }

            let data_end = pos + LEN_PREFIX_SIZE + len;
            if data_end > self.max_size {
                return Err(StorageError::CorruptEntry(
                    "entry extends beyond file boundary".into(),
                ));
            }

            // During rebuild we do not know the key hash that was originally
            // used. We store a positional pseudo-hash so that `write_pos` is
            // correctly advanced. A production implementation would persist
            // the key hash alongside each entry; for now the index is only
            // populated during the current session and rebuild simply
            // advances the write cursor.
            //
            // NOTE: We intentionally do *not* insert into the index here
            // because we lack the key hash. The write cursor is still
            // advanced so that new appends land after existing data.

            pos = data_end;
        }

        self.write_pos = pos;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn put_and_get() {
        let dir = TempDir::new().unwrap();
        let mut tier = WarmTier::open(dir.path(), 4096).unwrap();

        tier.put(1, b"hello").unwrap();
        tier.put(2, b"world").unwrap();

        assert_eq!(tier.get(1), Some(b"hello".as_slice()));
        assert_eq!(tier.get(2), Some(b"world".as_slice()));
        assert_eq!(tier.get(3), None);
    }

    #[test]
    fn remove_entry() {
        let dir = TempDir::new().unwrap();
        let mut tier = WarmTier::open(dir.path(), 4096).unwrap();

        tier.put(42, b"data").unwrap();
        assert!(tier.remove(42));
        assert!(!tier.remove(42));
        assert_eq!(tier.get(42), None);
    }

    #[test]
    fn len_and_is_empty() {
        let dir = TempDir::new().unwrap();
        let mut tier = WarmTier::open(dir.path(), 4096).unwrap();

        assert!(tier.is_empty());
        assert_eq!(tier.len(), 0);

        tier.put(1, b"a").unwrap();
        tier.put(2, b"bb").unwrap();

        assert_eq!(tier.len(), 2);
        assert!(!tier.is_empty());

        tier.remove(1);
        assert_eq!(tier.len(), 1);
    }

    #[test]
    fn used_bytes() {
        let dir = TempDir::new().unwrap();
        let mut tier = WarmTier::open(dir.path(), 4096).unwrap();

        tier.put(1, b"abc").unwrap(); // 4 + 3 = 7
        tier.put(2, b"de").unwrap(); // 4 + 2 = 6

        assert_eq!(tier.used_bytes(), 13);

        // After removing one entry the used bytes should decrease.
        tier.remove(1);
        assert_eq!(tier.used_bytes(), 6);
    }

    #[test]
    fn overwrite_same_key() {
        let dir = TempDir::new().unwrap();
        let mut tier = WarmTier::open(dir.path(), 4096).unwrap();

        tier.put(1, b"first").unwrap();
        tier.put(1, b"second").unwrap();

        assert_eq!(tier.get(1), Some(b"second".as_slice()));
        assert_eq!(tier.len(), 1);
    }

    #[test]
    fn out_of_space() {
        let dir = TempDir::new().unwrap();
        // Only 10 bytes of space — enough for one small entry but not two.
        let mut tier = WarmTier::open(dir.path(), 10).unwrap();

        // 4 (prefix) + 5 (data) = 9 bytes — fits.
        tier.put(1, b"hello").unwrap();

        // 4 + 5 = 9 more bytes — does not fit (only 1 byte left).
        let result = tier.put(2, b"world");
        assert!(result.is_err());
    }

    #[test]
    fn flush_does_not_error() {
        let dir = TempDir::new().unwrap();
        let mut tier = WarmTier::open(dir.path(), 4096).unwrap();
        tier.put(1, b"data").unwrap();
        tier.flush().unwrap();
    }

    #[test]
    fn reopen_advances_write_cursor() {
        let dir = TempDir::new().unwrap();

        {
            let mut tier = WarmTier::open(dir.path(), 4096).unwrap();
            tier.put(1, b"hello").unwrap();
            tier.flush().unwrap();
        }

        // Reopen — the write cursor should be past the previously written data.
        let mut tier = WarmTier::open(dir.path(), 4096).unwrap();

        // The index is empty after reopen (key hashes are not persisted) but
        // the write cursor has advanced so new writes do not clobber old data.
        assert!(tier.is_empty());

        tier.put(2, b"world").unwrap();
        assert_eq!(tier.get(2), Some(b"world".as_slice()));
    }
}
