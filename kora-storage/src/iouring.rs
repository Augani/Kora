//! Async storage I/O abstraction with platform-specific backends.
//!
//! Defines [`AsyncStorageIo`] for submitting non-blocking reads and writes,
//! with a [`SyncFallbackBackend`] for all platforms and a stub
//! [`IoUringBackend`] for Linux.

use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// An opaque token identifying a submitted I/O operation.
pub struct IoToken(pub u64);

/// The result of a completed I/O operation.
pub enum IoCompletion {
    /// A read completed successfully.
    Read {
        /// Token identifying the original submission.
        token: IoToken,
        /// Data that was read.
        data: Vec<u8>,
    },
    /// A write completed successfully.
    Write {
        /// Token identifying the original submission.
        token: IoToken,
        /// Number of bytes written.
        bytes_written: usize,
    },
    /// An I/O operation failed.
    Error {
        /// Token identifying the original submission.
        token: IoToken,
        /// The error that occurred.
        error: std::io::Error,
    },
}

/// Trait for async storage I/O backends.
///
/// Implementations accept read/write submissions and produce completions
/// that can be polled. This allows swapping between io_uring on Linux
/// and synchronous fallback on other platforms.
pub trait AsyncStorageIo: Send {
    /// Submit an asynchronous read starting at `offset` for `len` bytes.
    fn submit_read(&mut self, offset: u64, len: u32) -> IoToken;

    /// Submit an asynchronous write of `data` starting at `offset`.
    fn submit_write(&mut self, offset: u64, data: &[u8]) -> IoToken;

    /// Poll for completed I/O operations.
    fn poll_completions(&mut self) -> Vec<IoCompletion>;

    /// Flush all pending data to durable storage.
    fn sync(&mut self) -> std::io::Result<()>;

    /// Return the number of pending (unpolled) completions.
    fn pending_count(&self) -> usize;
}

/// Synchronous fallback backend that executes I/O inline.
///
/// Used on all non-Linux platforms. Reads and writes are performed
/// synchronously and results are buffered for the next `poll_completions` call.
pub struct SyncFallbackBackend {
    file: File,
    next_token: u64,
    pending: VecDeque<IoCompletion>,
}

impl SyncFallbackBackend {
    /// Open or create a file at `path` for async-style I/O.
    pub fn open(path: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;

        Ok(Self {
            file,
            next_token: 0,
            pending: VecDeque::new(),
        })
    }

    fn next_token(&mut self) -> IoToken {
        let token = IoToken(self.next_token);
        self.next_token += 1;
        token
    }
}

impl AsyncStorageIo for SyncFallbackBackend {
    fn submit_read(&mut self, offset: u64, len: u32) -> IoToken {
        let token = self.next_token();
        let completion = match self.do_read(offset, len) {
            Ok(data) => IoCompletion::Read { token, data },
            Err(error) => IoCompletion::Error { token, error },
        };
        self.pending.push_back(completion);
        token
    }

    fn submit_write(&mut self, offset: u64, data: &[u8]) -> IoToken {
        let token = self.next_token();
        let completion = match self.do_write(offset, data) {
            Ok(bytes_written) => IoCompletion::Write {
                token,
                bytes_written,
            },
            Err(error) => IoCompletion::Error { token, error },
        };
        self.pending.push_back(completion);
        token
    }

    fn poll_completions(&mut self) -> Vec<IoCompletion> {
        self.pending.drain(..).collect()
    }

    fn sync(&mut self) -> std::io::Result<()> {
        self.file.sync_all()
    }

    fn pending_count(&self) -> usize {
        self.pending.len()
    }
}

impl SyncFallbackBackend {
    fn do_read(&mut self, offset: u64, len: u32) -> std::io::Result<Vec<u8>> {
        self.file.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; len as usize];
        let bytes_read = self.file.read(&mut buf)?;
        buf.truncate(bytes_read);
        Ok(buf)
    }

    fn do_write(&mut self, offset: u64, data: &[u8]) -> std::io::Result<usize> {
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(data)?;
        Ok(data.len())
    }
}

/// io_uring-based async storage backend (Linux only).
///
/// TODO: Replace inner `SyncFallbackBackend` delegation with actual `io-uring`
/// crate calls for true async I/O on Linux. Currently delegates to sync I/O
/// as a stub implementation.
#[cfg(target_os = "linux")]
pub struct IoUringBackend {
    inner: SyncFallbackBackend,
}

#[cfg(target_os = "linux")]
impl IoUringBackend {
    /// Open or create a file at `path` for io_uring-based I/O.
    pub fn open(path: &Path) -> std::io::Result<Self> {
        Ok(Self {
            inner: SyncFallbackBackend::open(path)?,
        })
    }
}

#[cfg(target_os = "linux")]
impl AsyncStorageIo for IoUringBackend {
    fn submit_read(&mut self, offset: u64, len: u32) -> IoToken {
        self.inner.submit_read(offset, len)
    }

    fn submit_write(&mut self, offset: u64, data: &[u8]) -> IoToken {
        self.inner.submit_write(offset, data)
    }

    fn poll_completions(&mut self) -> Vec<IoCompletion> {
        self.inner.poll_completions()
    }

    fn sync(&mut self) -> std::io::Result<()> {
        self.inner.sync()
    }

    fn pending_count(&self) -> usize {
        self.inner.pending_count()
    }
}

/// Create the platform-appropriate async storage I/O backend.
///
/// On Linux, returns an [`IoUringBackend`]. On all other platforms,
/// returns a [`SyncFallbackBackend`].
pub fn create_async_backend(path: &Path) -> std::io::Result<Box<dyn AsyncStorageIo>> {
    #[cfg(target_os = "linux")]
    {
        Ok(Box::new(IoUringBackend::open(path)?))
    }
    #[cfg(not(target_os = "linux"))]
    {
        Ok(Box::new(SyncFallbackBackend::open(path)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::fs;

    fn temp_file_path(name: &str) -> std::path::PathBuf {
        env::temp_dir().join(format!("kora_iouring_test_{name}_{}", std::process::id()))
    }

    struct TempFile(std::path::PathBuf);
    impl TempFile {
        fn new(name: &str) -> Self {
            let path = temp_file_path(name);
            Self(path)
        }
        fn path(&self) -> &Path {
            &self.0
        }
    }
    impl Drop for TempFile {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }

    #[test]
    fn write_then_read_roundtrip() {
        let tf = TempFile::new("roundtrip");
        let mut backend = SyncFallbackBackend::open(tf.path()).unwrap();

        let data = b"hello kora io";
        backend.submit_write(0, data);
        let completions = backend.poll_completions();
        assert_eq!(completions.len(), 1);
        match &completions[0] {
            IoCompletion::Write { bytes_written, .. } => assert_eq!(*bytes_written, data.len()),
            _ => panic!("expected Write completion"),
        }

        let token = backend.submit_read(0, data.len() as u32);
        let completions = backend.poll_completions();
        assert_eq!(completions.len(), 1);
        match &completions[0] {
            IoCompletion::Read {
                token: t,
                data: read_data,
            } => {
                assert_eq!(*t, token);
                assert_eq!(read_data, data);
            }
            _ => panic!("expected Read completion"),
        }
    }

    #[test]
    fn multiple_sequential_writes() {
        let tf = TempFile::new("multi_write");
        let mut backend = SyncFallbackBackend::open(tf.path()).unwrap();

        let chunks: Vec<&[u8]> = vec![b"aaa", b"bbb", b"ccc"];
        let mut offset = 0u64;

        for chunk in &chunks {
            backend.submit_write(offset, chunk);
            offset += chunk.len() as u64;
        }

        let completions = backend.poll_completions();
        assert_eq!(completions.len(), 3);

        let mut read_offset = 0u64;
        for chunk in &chunks {
            backend.submit_read(read_offset, chunk.len() as u32);
            read_offset += chunk.len() as u64;
        }

        let completions = backend.poll_completions();
        assert_eq!(completions.len(), 3);

        let expected_data: Vec<&[u8]> = vec![b"aaa", b"bbb", b"ccc"];
        for (completion, expected) in completions.iter().zip(expected_data.iter()) {
            match completion {
                IoCompletion::Read { data, .. } => assert_eq!(data, expected),
                _ => panic!("expected Read completion"),
            }
        }
    }

    #[test]
    fn sync_does_not_error() {
        let tf = TempFile::new("sync");
        let mut backend = SyncFallbackBackend::open(tf.path()).unwrap();
        backend.submit_write(0, b"test");
        let _ = backend.poll_completions();
        assert!(backend.sync().is_ok());
    }

    #[test]
    fn token_increments() {
        let tf = TempFile::new("tokens");
        let mut backend = SyncFallbackBackend::open(tf.path()).unwrap();

        let t0 = backend.submit_write(0, b"a");
        let t1 = backend.submit_write(1, b"b");
        let t2 = backend.submit_read(0, 1);

        assert_eq!(t0, IoToken(0));
        assert_eq!(t1, IoToken(1));
        assert_eq!(t2, IoToken(2));

        let _ = backend.poll_completions();
    }

    #[test]
    fn read_beyond_file_returns_empty() {
        let tf = TempFile::new("beyond");
        let mut backend = SyncFallbackBackend::open(tf.path()).unwrap();

        backend.submit_read(1000, 64);
        let completions = backend.poll_completions();
        assert_eq!(completions.len(), 1);
        match &completions[0] {
            IoCompletion::Read { data, .. } => assert!(data.is_empty()),
            _ => panic!("expected Read completion"),
        }
    }

    #[test]
    fn create_async_backend_works() {
        let tf = TempFile::new("factory");
        let mut backend = create_async_backend(tf.path()).unwrap();
        let token = backend.submit_write(0, b"factory test");
        assert_eq!(token, IoToken(0));
        let completions = backend.poll_completions();
        assert_eq!(completions.len(), 1);
    }
}
