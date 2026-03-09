//! io_uring-based storage backend for Linux.
//!
//! Provides an [`IoUringBackend`] that implements [`StorageBackend`] using the
//! Linux `io_uring` interface for high-performance asynchronous I/O. This
//! backend uses the same on-disk data layout as [`FileBackend`]:
//!
//! ```text
//! [len: u32][compressed_data...]
//! ```
//!
//! Enable this module with the `io-uring` Cargo feature (Linux only).
//!
//! [`StorageBackend`]: crate::backend::StorageBackend
//! [`FileBackend`]: crate::backend::FileBackend

#![cfg(feature = "io-uring")]

use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use io_uring::{opcode, types, IoUring};

use crate::backend::StorageBackend;
use crate::compressor;
use crate::error::{Result, StorageError};

/// Default io_uring submission queue depth.
const DEFAULT_QUEUE_DEPTH: u32 = 32;

/// An io_uring-based storage backend for cold-tier data on Linux.
///
/// This backend keeps an in-memory index mapping each key hash to its
/// `(offset, total_length)` in the append-only data file, identical to
/// [`FileBackend`](crate::backend::FileBackend). I/O operations are
/// submitted through a per-backend `io_uring` instance for reduced
/// syscall overhead.
///
/// A [`Mutex`] protects the mutable inner state (index, write offset,
/// and the `io_uring` instance) since io_uring submission and completion
/// must be coordinated.
pub struct IoUringBackend {
    data_path: PathBuf,
    inner: Mutex<IoUringInner>,
}

struct IoUringInner {
    fd: std::fs::File,
    index: HashMap<u64, (u64, u32)>,
    write_offset: u64,
    deleted: usize,
    ring: IoUring,
}

impl IoUringBackend {
    /// Open or create an io_uring-backed storage backend at the given directory.
    ///
    /// Creates the directory if it does not exist, then opens (or creates)
    /// `cold_uring.dat` inside it. If a matching index file exists it is
    /// loaded for fast recovery.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref();
        fs::create_dir_all(dir)?;

        let data_path = dir.join("cold_uring.dat");
        let index_path = dir.join("cold_uring.idx");

        let fd = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&data_path)?;

        let write_offset = fd.metadata()?.len();

        let index = if index_path.exists() {
            load_index(&index_path)?
        } else {
            HashMap::new()
        };

        let ring = IoUring::new(DEFAULT_QUEUE_DEPTH).map_err(|e| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("failed to create io_uring: {e}"),
            ))
        })?;

        Ok(Self {
            data_path,
            inner: Mutex::new(IoUringInner {
                fd,
                index,
                write_offset,
                deleted: 0,
                ring,
            }),
        })
    }

    /// Return the number of live entries in the index.
    pub fn len(&self) -> Result<usize> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| StorageError::LockPoisoned(e.to_string()))?;
        Ok(inner.index.len())
    }

    /// Return `true` if the backend contains no entries.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Persist the in-memory index to disk for fast recovery on restart.
    pub fn save_index(&self) -> Result<()> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| StorageError::LockPoisoned(e.to_string()))?;
        let index_path = self.data_path.with_extension("idx");
        save_index(&index_path, &inner.index)
    }
}

/// Submit a single read SQE and wait for its completion, returning the
/// number of bytes read.
fn uring_read(ring: &mut IoUring, fd: i32, buf: &mut [u8], offset: u64) -> Result<usize> {
    let read_e = opcode::Read::new(types::Fd(fd), buf.as_mut_ptr(), buf.len() as _)
        .offset(offset)
        .build()
        .user_data(0x01);

    // SAFETY: The buffer (`buf`) lives on the caller's stack and remains valid
    // until we call `submit_and_wait` and consume the CQE below, so the kernel
    // will not write into freed memory.
    unsafe {
        ring.submission().push(&read_e).map_err(|_| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "io_uring submission queue full",
            ))
        })?;
    }

    ring.submit_and_wait(1).map_err(StorageError::Io)?;

    let cqe = ring.completion().next().ok_or_else(|| {
        StorageError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            "io_uring: no completion entry after wait",
        ))
    })?;

    let ret = cqe.result();
    if ret < 0 {
        return Err(StorageError::Io(std::io::Error::from_raw_os_error(-ret)));
    }
    Ok(ret as usize)
}

/// Submit a single write SQE and wait for its completion.
fn uring_write(ring: &mut IoUring, fd: i32, buf: &[u8], offset: u64) -> Result<usize> {
    let write_e = opcode::Write::new(types::Fd(fd), buf.as_ptr(), buf.len() as _)
        .offset(offset)
        .build()
        .user_data(0x02);

    // SAFETY: The buffer (`buf`) is borrowed from the caller and remains valid
    // until we consume the CQE below.
    unsafe {
        ring.submission().push(&write_e).map_err(|_| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "io_uring submission queue full",
            ))
        })?;
    }

    ring.submit_and_wait(1).map_err(StorageError::Io)?;

    let cqe = ring.completion().next().ok_or_else(|| {
        StorageError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            "io_uring: no completion entry after wait",
        ))
    })?;

    let ret = cqe.result();
    if ret < 0 {
        return Err(StorageError::Io(std::io::Error::from_raw_os_error(-ret)));
    }
    Ok(ret as usize)
}

/// Submit an fsync SQE and wait for its completion.
fn uring_fsync(ring: &mut IoUring, fd: i32) -> Result<()> {
    let fsync_e = opcode::Fsync::new(types::Fd(fd)).build().user_data(0x03);

    // SAFETY: Fsync does not reference any user-space buffer.
    unsafe {
        ring.submission().push(&fsync_e).map_err(|_| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "io_uring submission queue full",
            ))
        })?;
    }

    ring.submit_and_wait(1).map_err(StorageError::Io)?;

    let cqe = ring.completion().next().ok_or_else(|| {
        StorageError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            "io_uring: no completion entry after wait",
        ))
    })?;

    let ret = cqe.result();
    if ret < 0 {
        return Err(StorageError::Io(std::io::Error::from_raw_os_error(-ret)));
    }
    Ok(())
}

impl StorageBackend for IoUringBackend {
    fn read(&self, key_hash: u64) -> Result<Option<Vec<u8>>> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| StorageError::LockPoisoned(e.to_string()))?;

        let (offset, _total_len) = match inner.index.get(&key_hash) {
            Some(&entry) => entry,
            None => return Ok(None),
        };

        let raw_fd = inner.fd.as_raw_fd();

        let mut len_buf = [0u8; 4];
        let n = uring_read(&mut inner.ring, raw_fd, &mut len_buf, offset)?;
        if n < 4 {
            return Err(StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "short read on length prefix",
            )));
        }
        let compressed_len = u32::from_le_bytes(len_buf) as usize;

        let mut compressed = vec![0u8; compressed_len];
        let n = uring_read(&mut inner.ring, raw_fd, &mut compressed, offset + 4)?;
        if n < compressed_len {
            return Err(StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "short read on compressed data",
            )));
        }

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
        let raw_fd = inner.fd.as_raw_fd();

        let len_bytes = (compressed.len() as u32).to_le_bytes();
        let mut record = Vec::with_capacity(4 + compressed.len());
        record.extend_from_slice(&len_bytes);
        record.extend_from_slice(&compressed);

        let n = uring_write(&mut inner.ring, raw_fd, &record, offset)?;
        if n < record.len() {
            return Err(StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "short write via io_uring",
            )));
        }

        let total_len = record.len() as u32;
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
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| StorageError::LockPoisoned(e.to_string()))?;
        let raw_fd = inner.fd.as_raw_fd();
        uring_fsync(&mut inner.ring, raw_fd)
    }
}

fn save_index(path: &Path, index: &HashMap<u64, (u64, u32)>) -> Result<()> {
    use std::io::Write;

    let tmp_path = path.with_extension("idx.tmp");
    let mut file = fs::File::create(&tmp_path)?;

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
