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
//! [key_hash: u64 little-endian][length: u32 little-endian][data: length bytes]
//! ```
//!
//! Including the key hash makes the file self-describing, enabling full index
//! rebuild on reopen without external metadata.
//!
//! Deletions are lazy — the index entry is removed but the data remains in
//! the file until a future compaction pass.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

use memmap2::MmapMut;

const HASH_SIZE: usize = 8;
const LEN_PREFIX_SIZE: usize = 4;
const ENTRY_HEADER_SIZE: usize = HASH_SIZE + LEN_PREFIX_SIZE;
const INITIAL_FILE_SIZE: u64 = 1024 * 1024;

/// Describes a single entry's location within the mmap.
pub struct WarmEntry {
    /// Byte offset from the start of the file.
    pub offset: u64,
    /// Length of the data (not including the header).
    pub length: u32,
}

/// Memory-mapped warm tier storage.
///
/// Provides zero-copy reads through a memory-mapped data file. Writes are
/// append-only, and deletions are lazy (index entries are removed but the
/// underlying data is not reclaimed until compaction).
///
/// This tier is designed to be shard-local so it does not require internal
/// synchronisation for concurrent access.
pub struct WarmTier {
    file: File,
    mmap: Option<MmapMut>,
    index: HashMap<u64, WarmEntry>,
    write_offset: u64,
    file_path: PathBuf,
    max_size: u64,
    file_size: u64,
}

impl WarmTier {
    /// Open or create a warm tier data file at `path`.
    ///
    /// The backing file starts at a small initial size and grows as needed
    /// up to `max_size`. If the file already exists, the index is rebuilt
    /// by scanning existing entries.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Io`] if the file cannot be opened or mapped.
    pub fn open(path: &Path, max_size: u64) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        let metadata = file.metadata()?;
        let existing_len = metadata.len();

        let file_size = if existing_len == 0 {
            let initial = INITIAL_FILE_SIZE.min(max_size);
            file.set_len(initial)?;
            initial
        } else {
            existing_len
        };

        // SAFETY: The file is opened in read-write mode and we hold the only
        // handle. The warm tier is shard-local with no concurrent external access.
        let mmap = unsafe { MmapMut::map_mut(&file)? };

        let mut tier = Self {
            file,
            mmap: Some(mmap),
            index: HashMap::new(),
            write_offset: 0,
            file_path: path.to_path_buf(),
            max_size,
            file_size,
        };

        tier.rebuild_index_from_file()?;
        Ok(tier)
    }

    /// Store data associated with `key_hash`.
    ///
    /// If an entry for `key_hash` already exists, the old entry becomes
    /// unreachable (lazy deletion) and the new value is appended.
    ///
    /// The file and mmap are automatically grown (doubled) when space is
    /// insufficient, up to `max_size`.
    ///
    /// # Errors
    ///
    /// Returns an error if the data does not fit within `max_size`.
    pub fn store(&mut self, key_hash: u64, data: &[u8]) -> io::Result<()> {
        let entry_size = ENTRY_HEADER_SIZE + data.len();
        let needed = self.write_offset + entry_size as u64;

        if needed > self.max_size {
            return Err(io::Error::other("warm tier: exceeds max_size"));
        }

        if needed > self.file_size {
            self.grow_file(needed)?;
        }

        let mmap = self
            .mmap
            .as_mut()
            .ok_or_else(|| io::Error::other("warm tier: mmap not available"))?;

        let off = self.write_offset as usize;
        mmap[off..off + HASH_SIZE].copy_from_slice(&key_hash.to_le_bytes());
        mmap[off + HASH_SIZE..off + ENTRY_HEADER_SIZE]
            .copy_from_slice(&(data.len() as u32).to_le_bytes());
        mmap[off + ENTRY_HEADER_SIZE..off + entry_size].copy_from_slice(data);

        self.index.insert(
            key_hash,
            WarmEntry {
                offset: self.write_offset,
                length: data.len() as u32,
            },
        );
        self.write_offset += entry_size as u64;

        Ok(())
    }

    /// Load data associated with `key_hash`.
    ///
    /// Returns a zero-copy slice from the memory-mapped region, or `None`
    /// if the key is not present.
    pub fn load(&self, key_hash: u64) -> Option<&[u8]> {
        let entry = self.index.get(&key_hash)?;
        let mmap = self.mmap.as_ref()?;
        let data_start = entry.offset as usize + ENTRY_HEADER_SIZE;
        let data_end = data_start + entry.length as usize;
        if data_end <= self.file_size as usize {
            Some(&mmap[data_start..data_end])
        } else {
            None
        }
    }

    /// Remove an entry from the index. Does not reclaim disk space.
    ///
    /// Returns `true` if the entry existed.
    pub fn remove(&mut self, key_hash: u64) -> bool {
        self.index.remove(&key_hash).is_some()
    }

    /// Returns `true` if an entry exists for `key_hash`.
    pub fn contains(&self, key_hash: u64) -> bool {
        self.index.contains_key(&key_hash)
    }

    /// Compact the data file by rewriting only live entries.
    ///
    /// Creates a temporary file, writes all live entries, replaces the
    /// original file, and remaps.
    ///
    /// # Errors
    ///
    /// Returns an error if I/O fails during compaction.
    pub fn compact(&mut self) -> io::Result<()> {
        let tmp_path = self.file_path.with_extension("tmp");

        let live_entries: Vec<(u64, u64, u32)> = self
            .index
            .iter()
            .map(|(&hash, entry)| (hash, entry.offset, entry.length))
            .collect();

        let mut new_data: Vec<u8> = Vec::new();
        let mut new_index: HashMap<u64, WarmEntry> = HashMap::new();

        let mmap = self
            .mmap
            .as_ref()
            .ok_or_else(|| io::Error::other("warm tier: mmap not available"))?;

        for (hash, offset, length) in &live_entries {
            let new_offset = new_data.len() as u64;
            new_data.extend_from_slice(&hash.to_le_bytes());
            new_data.extend_from_slice(&length.to_le_bytes());

            let src_start = *offset as usize + ENTRY_HEADER_SIZE;
            let src_end = src_start + *length as usize;
            new_data.extend_from_slice(&mmap[src_start..src_end]);

            new_index.insert(
                *hash,
                WarmEntry {
                    offset: new_offset,
                    length: *length,
                },
            );
        }

        fs::write(&tmp_path, &new_data)?;

        drop(self.mmap.take());

        fs::rename(&tmp_path, &self.file_path)?;

        let new_write_offset = new_data.len() as u64;
        let new_file_size = INITIAL_FILE_SIZE.min(self.max_size).max(new_write_offset);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.file_path)?;

        file.set_len(new_file_size)?;

        // SAFETY: File opened read-write, shard-local access only.
        let new_mmap = unsafe { MmapMut::map_mut(&file)? };

        self.file = file;
        self.mmap = Some(new_mmap);
        self.index = new_index;
        self.write_offset = new_write_offset;
        self.file_size = new_file_size;

        Ok(())
    }

    /// Number of stored entries.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// Returns `true` if there are no entries.
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// Bytes of data written (including dead entries not yet compacted).
    pub fn disk_usage(&self) -> u64 {
        self.write_offset
    }

    /// Flush the mmap to disk.
    ///
    /// # Errors
    ///
    /// Returns an error if the flush fails.
    pub fn sync(&self) -> io::Result<()> {
        if let Some(ref mmap) = self.mmap {
            mmap.flush()?;
        }
        Ok(())
    }

    fn grow_file(&mut self, needed: u64) -> io::Result<()> {
        drop(self.mmap.take());

        let mut new_size = self.file_size;
        while new_size < needed {
            new_size = (new_size * 2).min(self.max_size);
            if new_size < needed && new_size == self.max_size {
                return Err(io::Error::other("warm tier: exceeds max_size"));
            }
        }

        self.file.set_len(new_size)?;
        self.file_size = new_size;

        // SAFETY: File opened read-write, shard-local access only.
        let mmap = unsafe { MmapMut::map_mut(&self.file)? };
        self.mmap = Some(mmap);

        Ok(())
    }

    fn rebuild_index_from_file(&mut self) -> io::Result<()> {
        let mmap = match self.mmap.as_ref() {
            Some(m) => m,
            None => return Ok(()),
        };

        let mut pos: u64 = 0;
        let file_len = self.file_size;

        while pos + ENTRY_HEADER_SIZE as u64 <= file_len {
            let off = pos as usize;

            let hash_bytes: [u8; 8] = mmap[off..off + HASH_SIZE]
                .try_into()
                .map_err(|_| io::Error::other("warm tier: corrupt hash"))?;
            let key_hash = u64::from_le_bytes(hash_bytes);

            let len_bytes: [u8; 4] = mmap[off + HASH_SIZE..off + ENTRY_HEADER_SIZE]
                .try_into()
                .map_err(|_| io::Error::other("warm tier: corrupt length"))?;
            let length = u32::from_le_bytes(len_bytes);

            if key_hash == 0 && length == 0 {
                break;
            }

            let entry_end = pos + ENTRY_HEADER_SIZE as u64 + length as u64;
            if entry_end > file_len {
                return Err(io::Error::other(
                    "warm tier: entry extends beyond file boundary",
                ));
            }

            self.index.insert(
                key_hash,
                WarmEntry {
                    offset: pos,
                    length,
                },
            );

            pos = entry_end;
        }

        self.write_offset = pos;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn warm_path(dir: &TempDir) -> PathBuf {
        dir.path().join("warm.dat")
    }

    #[test]
    fn store_and_load_single() {
        let dir = TempDir::new().unwrap();
        let mut tier = WarmTier::open(&warm_path(&dir), 4096).unwrap();

        tier.store(1, b"hello").unwrap();
        assert_eq!(tier.load(1), Some(b"hello".as_slice()));
        assert_eq!(tier.load(999), None);
    }

    #[test]
    fn store_multiple_and_load_all() {
        let dir = TempDir::new().unwrap();
        let mut tier = WarmTier::open(&warm_path(&dir), 4096).unwrap();

        tier.store(10, b"alpha").unwrap();
        tier.store(20, b"bravo").unwrap();
        tier.store(30, b"charlie").unwrap();

        assert_eq!(tier.load(10), Some(b"alpha".as_slice()));
        assert_eq!(tier.load(20), Some(b"bravo".as_slice()));
        assert_eq!(tier.load(30), Some(b"charlie".as_slice()));
        assert_eq!(tier.len(), 3);
    }

    #[test]
    fn remove_entry() {
        let dir = TempDir::new().unwrap();
        let mut tier = WarmTier::open(&warm_path(&dir), 4096).unwrap();

        tier.store(42, b"data").unwrap();
        assert!(tier.contains(42));
        assert!(tier.remove(42));
        assert!(!tier.remove(42));
        assert!(!tier.contains(42));
        assert_eq!(tier.load(42), None);
    }

    #[test]
    fn compact_reclaims_space() {
        let dir = TempDir::new().unwrap();
        let mut tier = WarmTier::open(&warm_path(&dir), 1024 * 1024).unwrap();

        tier.store(1, b"aaa").unwrap();
        tier.store(2, b"bbb").unwrap();
        tier.store(3, b"ccc").unwrap();

        let usage_before = tier.disk_usage();
        tier.remove(2);
        tier.compact().unwrap();
        let usage_after = tier.disk_usage();

        assert!(usage_after < usage_before);
        assert_eq!(tier.len(), 2);
        assert_eq!(tier.load(1), Some(b"aaa".as_slice()));
        assert_eq!(tier.load(3), Some(b"ccc".as_slice()));
        assert_eq!(tier.load(2), None);
    }

    #[test]
    fn file_growth() {
        let dir = TempDir::new().unwrap();
        let max_size = 1024 * 1024 * 4;
        let mut tier = WarmTier::open(&warm_path(&dir), max_size).unwrap();

        let big_data = vec![0xABu8; 512 * 1024];
        tier.store(1, &big_data).unwrap();
        tier.store(2, &big_data).unwrap();

        assert_eq!(tier.load(1).map(|s| s.len()), Some(512 * 1024));
        assert_eq!(tier.load(2).map(|s| s.len()), Some(512 * 1024));
        assert!(tier.file_size > INITIAL_FILE_SIZE);
    }

    #[test]
    fn sync_no_error() {
        let dir = TempDir::new().unwrap();
        let mut tier = WarmTier::open(&warm_path(&dir), 4096).unwrap();
        tier.store(1, b"data").unwrap();
        tier.sync().unwrap();
    }

    #[test]
    fn reopen_rebuilds_index() {
        let dir = TempDir::new().unwrap();
        let path = warm_path(&dir);

        {
            let mut tier = WarmTier::open(&path, 1024 * 1024).unwrap();
            tier.store(100, b"persist_me").unwrap();
            tier.store(200, b"and_me_too").unwrap();
            tier.sync().unwrap();
        }

        let tier = WarmTier::open(&path, 1024 * 1024).unwrap();
        assert_eq!(tier.len(), 2);
        assert_eq!(tier.load(100), Some(b"persist_me".as_slice()));
        assert_eq!(tier.load(200), Some(b"and_me_too".as_slice()));
    }

    #[test]
    fn len_and_is_empty() {
        let dir = TempDir::new().unwrap();
        let mut tier = WarmTier::open(&warm_path(&dir), 4096).unwrap();

        assert!(tier.is_empty());
        assert_eq!(tier.len(), 0);

        tier.store(1, b"a").unwrap();
        tier.store(2, b"bb").unwrap();
        assert_eq!(tier.len(), 2);
        assert!(!tier.is_empty());

        tier.remove(1);
        assert_eq!(tier.len(), 1);
    }

    #[test]
    fn disk_usage_tracks_writes() {
        let dir = TempDir::new().unwrap();
        let mut tier = WarmTier::open(&warm_path(&dir), 4096).unwrap();

        assert_eq!(tier.disk_usage(), 0);
        tier.store(1, b"abc").unwrap();
        assert_eq!(tier.disk_usage(), (ENTRY_HEADER_SIZE + 3) as u64);
    }

    #[test]
    fn overwrite_same_key() {
        let dir = TempDir::new().unwrap();
        let mut tier = WarmTier::open(&warm_path(&dir), 4096).unwrap();

        tier.store(1, b"first").unwrap();
        tier.store(1, b"second").unwrap();

        assert_eq!(tier.load(1), Some(b"second".as_slice()));
        assert_eq!(tier.len(), 1);
    }

    #[test]
    fn exceeds_max_size() {
        let dir = TempDir::new().unwrap();
        let mut tier = WarmTier::open(&warm_path(&dir), 30).unwrap();

        tier.store(1, b"hello").unwrap();
        let result = tier.store(2, b"this_is_too_long_to_fit");
        assert!(result.is_err());
    }
}
