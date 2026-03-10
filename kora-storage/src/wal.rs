//! Write-Ahead Log (WAL) for crash recovery.
//!
//! Every mutation is appended to the WAL before being acknowledged. On restart,
//! the WAL is replayed to reconstruct the in-memory state. The WAL supports
//! configurable sync policies and log rotation.
//!
//! ## Wire format (per entry)
//!
//! ```text
//! [len: u32][type: u8][payload...][crc32: u32]
//! ```
//!
//! - `len` — total byte length of `type + payload` (does NOT include the len or crc fields)
//! - `type` — discriminant of `WalEntry`
//! - `payload` — entry-specific data (see each variant)
//! - `crc32` — CRC-32C of `type + payload`

use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Seek, Write};
use std::path::{Path, PathBuf};

use crate::error::{Result, StorageError};

/// How often the WAL is fsynced to disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncPolicy {
    /// Fsync after every write (safest, slowest).
    EveryWrite,
    /// Fsync once per second (good balance).
    EverySecond,
    /// Let the OS decide when to flush (fastest, least durable).
    OsManaged,
}

/// A mutation that can be recorded in the WAL.
#[derive(Debug, Clone, PartialEq)]
pub enum WalEntry {
    /// SET key value \[ttl_ms\]
    Set {
        /// The key.
        key: Vec<u8>,
        /// The value.
        value: Vec<u8>,
        /// Optional TTL in milliseconds.
        ttl_ms: Option<u64>,
    },
    /// DEL key
    Del {
        /// The key to delete.
        key: Vec<u8>,
    },
    /// EXPIRE key ttl_ms
    Expire {
        /// The key.
        key: Vec<u8>,
        /// TTL in milliseconds.
        ttl_ms: u64,
    },
    /// LPUSH key values...
    LPush {
        /// The key.
        key: Vec<u8>,
        /// Values to push.
        values: Vec<Vec<u8>>,
    },
    /// RPUSH key values...
    RPush {
        /// The key.
        key: Vec<u8>,
        /// Values to push.
        values: Vec<Vec<u8>>,
    },
    /// HSET key field value
    HSet {
        /// The key.
        key: Vec<u8>,
        /// The field-value pairs.
        fields: Vec<(Vec<u8>, Vec<u8>)>,
    },
    /// SADD key members...
    SAdd {
        /// The key.
        key: Vec<u8>,
        /// Members to add.
        members: Vec<Vec<u8>>,
    },
    /// FLUSHDB — clear all keys.
    FlushDb,
    /// DOC.SET collection doc_id json
    DocSet {
        /// Collection name.
        collection: Vec<u8>,
        /// External document ID.
        doc_id: Vec<u8>,
        /// JSON payload bytes.
        json: Vec<u8>,
    },
    /// DOC.DEL collection doc_id
    DocDel {
        /// Collection name.
        collection: Vec<u8>,
        /// External document ID.
        doc_id: Vec<u8>,
    },
    /// VECSET key dimensions vector\_bytes
    VecSet {
        /// The key.
        key: Vec<u8>,
        /// Vector dimensions.
        dimensions: usize,
        /// Raw f32 values as LE bytes (each f32 is 4 bytes).
        vector: Vec<u8>,
    },
    /// VECDEL key
    VecDel {
        /// The key.
        key: Vec<u8>,
    },
}

const WAL_SET: u8 = 1;
const WAL_DEL: u8 = 2;
const WAL_EXPIRE: u8 = 3;
const WAL_LPUSH: u8 = 4;
const WAL_RPUSH: u8 = 5;
const WAL_HSET: u8 = 6;
const WAL_SADD: u8 = 7;
const WAL_FLUSH: u8 = 8;
const WAL_DOC_SET: u8 = 9;
const WAL_DOC_DEL: u8 = 10;
const WAL_VEC_SET: u8 = 11;
const WAL_VEC_DEL: u8 = 12;

/// The Write-Ahead Log.
pub struct WriteAheadLog {
    writer: BufWriter<File>,
    path: PathBuf,
    sync_policy: SyncPolicy,
    bytes_written: u64,
}

impl WriteAheadLog {
    /// Open or create a WAL file at the given path.
    pub fn open(path: impl AsRef<Path>, sync_policy: SyncPolicy) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let bytes_written = file.metadata()?.len();
        Ok(Self {
            writer: BufWriter::new(file),
            path,
            sync_policy,
            bytes_written,
        })
    }

    /// Append an entry to the WAL.
    pub fn append(&mut self, entry: &WalEntry) -> Result<()> {
        let payload = encode_entry(entry);
        let crc = crc32fast::hash(&payload);

        let len = payload.len() as u32;
        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(&payload)?;
        self.writer.write_all(&crc.to_le_bytes())?;

        self.bytes_written += 4 + payload.len() as u64 + 4;

        if self.sync_policy == SyncPolicy::EveryWrite {
            self.sync()?;
        }

        Ok(())
    }

    /// Fsync the WAL to disk.
    pub fn sync(&mut self) -> Result<()> {
        use std::io::Write;
        self.writer.flush()?;
        self.writer.get_ref().sync_data()?;
        Ok(())
    }

    /// Get the number of bytes written to the WAL.
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// Replay all entries in the WAL, calling the callback for each valid entry.
    ///
    /// Stops at the first corrupt or truncated entry (crash recovery).
    pub fn replay<F>(path: impl AsRef<Path>, mut callback: F) -> Result<u64>
    where
        F: FnMut(WalEntry),
    {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(0);
        }

        let mut file = File::open(path)?;
        let file_len = file.metadata()?.len();
        let mut count = 0u64;

        loop {
            let pos = file.stream_position()?;
            if pos >= file_len {
                break;
            }

            if file_len - pos < 4 {
                tracing::warn!("WAL truncated at position {}: incomplete length", pos);
                break;
            }

            let mut len_buf = [0u8; 4];
            if file.read_exact(&mut len_buf).is_err() {
                break;
            }
            let len = u32::from_le_bytes(len_buf) as usize;

            if len > 512 * 1024 * 1024 {
                tracing::warn!(
                    "WAL corrupt at position {}: entry length {} too large",
                    pos,
                    len
                );
                break;
            }

            if file_len - file.stream_position()? < (len as u64 + 4) {
                tracing::warn!("WAL truncated at position {}: incomplete entry", pos);
                break;
            }

            let mut payload = vec![0u8; len];
            if file.read_exact(&mut payload).is_err() {
                break;
            }

            let mut crc_buf = [0u8; 4];
            if file.read_exact(&mut crc_buf).is_err() {
                break;
            }
            let stored_crc = u32::from_le_bytes(crc_buf);
            let computed_crc = crc32fast::hash(&payload);

            if stored_crc != computed_crc {
                tracing::warn!(
                    "WAL CRC mismatch at position {}: expected {:#010x}, got {:#010x}",
                    pos,
                    stored_crc,
                    computed_crc
                );
                break;
            }

            match decode_entry(&payload) {
                Ok(entry) => {
                    callback(entry);
                    count += 1;
                }
                Err(e) => {
                    tracing::warn!("WAL decode error at position {}: {}", pos, e);
                    break;
                }
            }
        }

        Ok(count)
    }

    /// Rotate the WAL: rename current to `.old`, start a fresh one.
    pub fn rotate(&mut self) -> Result<()> {
        self.sync()?;

        let old_path = self.path.with_extension("wal.old");
        fs::rename(&self.path, &old_path)?;

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        self.writer = BufWriter::new(file);
        self.bytes_written = 0;

        Ok(())
    }

    /// Delete the WAL file (e.g., after a successful RDB snapshot).
    pub fn truncate(&mut self) -> Result<()> {
        self.sync()?;
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        self.writer = BufWriter::new(file);
        self.bytes_written = 0;
        Ok(())
    }
}

fn encode_entry(entry: &WalEntry) -> Vec<u8> {
    let mut buf = Vec::new();
    match entry {
        WalEntry::Set { key, value, ttl_ms } => {
            buf.push(WAL_SET);
            write_bytes(&mut buf, key);
            write_bytes(&mut buf, value);
            match ttl_ms {
                Some(ms) => {
                    buf.push(1);
                    buf.extend_from_slice(&ms.to_le_bytes());
                }
                None => buf.push(0),
            }
        }
        WalEntry::Del { key } => {
            buf.push(WAL_DEL);
            write_bytes(&mut buf, key);
        }
        WalEntry::Expire { key, ttl_ms } => {
            buf.push(WAL_EXPIRE);
            write_bytes(&mut buf, key);
            buf.extend_from_slice(&ttl_ms.to_le_bytes());
        }
        WalEntry::LPush { key, values } => {
            buf.push(WAL_LPUSH);
            write_bytes(&mut buf, key);
            write_vec_of_bytes(&mut buf, values);
        }
        WalEntry::RPush { key, values } => {
            buf.push(WAL_RPUSH);
            write_bytes(&mut buf, key);
            write_vec_of_bytes(&mut buf, values);
        }
        WalEntry::HSet { key, fields } => {
            buf.push(WAL_HSET);
            write_bytes(&mut buf, key);
            let count = fields.len() as u32;
            buf.extend_from_slice(&count.to_le_bytes());
            for (field, value) in fields {
                write_bytes(&mut buf, field);
                write_bytes(&mut buf, value);
            }
        }
        WalEntry::SAdd { key, members } => {
            buf.push(WAL_SADD);
            write_bytes(&mut buf, key);
            write_vec_of_bytes(&mut buf, members);
        }
        WalEntry::FlushDb => {
            buf.push(WAL_FLUSH);
        }
        WalEntry::DocSet {
            collection,
            doc_id,
            json,
        } => {
            buf.push(WAL_DOC_SET);
            write_bytes(&mut buf, collection);
            write_bytes(&mut buf, doc_id);
            write_bytes(&mut buf, json);
        }
        WalEntry::DocDel { collection, doc_id } => {
            buf.push(WAL_DOC_DEL);
            write_bytes(&mut buf, collection);
            write_bytes(&mut buf, doc_id);
        }
        WalEntry::VecSet {
            key,
            dimensions,
            vector,
        } => {
            buf.push(WAL_VEC_SET);
            write_bytes(&mut buf, key);
            buf.extend_from_slice(&(*dimensions as u32).to_le_bytes());
            write_bytes(&mut buf, vector);
        }
        WalEntry::VecDel { key } => {
            buf.push(WAL_VEC_DEL);
            write_bytes(&mut buf, key);
        }
    }
    buf
}

fn decode_entry(data: &[u8]) -> Result<WalEntry> {
    if data.is_empty() {
        return Err(StorageError::CorruptEntry("empty payload".into()));
    }

    let mut cursor = 1usize; // skip type byte
    match data[0] {
        WAL_SET => {
            let key = read_bytes(data, &mut cursor)?;
            let value = read_bytes(data, &mut cursor)?;
            let ttl_ms = if cursor < data.len() && data[cursor] == 1 {
                cursor += 1;
                if cursor + 8 > data.len() {
                    return Err(StorageError::CorruptEntry("truncated TTL".into()));
                }
                let ms = u64::from_le_bytes(
                    data[cursor..cursor + 8]
                        .try_into()
                        .map_err(|_| StorageError::CorruptEntry("invalid TTL bytes".into()))?,
                );
                cursor += 8;
                Some(ms)
            } else {
                if cursor < data.len() {
                    cursor += 1; // skip the 0 byte
                }
                None
            };
            let _ = cursor;
            Ok(WalEntry::Set { key, value, ttl_ms })
        }
        WAL_DEL => {
            let key = read_bytes(data, &mut cursor)?;
            let _ = cursor;
            Ok(WalEntry::Del { key })
        }
        WAL_EXPIRE => {
            let key = read_bytes(data, &mut cursor)?;
            if cursor + 8 > data.len() {
                return Err(StorageError::CorruptEntry("truncated expire TTL".into()));
            }
            let ttl_ms = u64::from_le_bytes(
                data[cursor..cursor + 8]
                    .try_into()
                    .map_err(|_| StorageError::CorruptEntry("invalid expire TTL bytes".into()))?,
            );
            Ok(WalEntry::Expire { key, ttl_ms })
        }
        WAL_LPUSH => {
            let key = read_bytes(data, &mut cursor)?;
            let values = read_vec_of_bytes(data, &mut cursor)?;
            Ok(WalEntry::LPush { key, values })
        }
        WAL_RPUSH => {
            let key = read_bytes(data, &mut cursor)?;
            let values = read_vec_of_bytes(data, &mut cursor)?;
            Ok(WalEntry::RPush { key, values })
        }
        WAL_HSET => {
            let key = read_bytes(data, &mut cursor)?;
            if cursor + 4 > data.len() {
                return Err(StorageError::CorruptEntry("truncated hset count".into()));
            }
            let count = u32::from_le_bytes(
                data[cursor..cursor + 4]
                    .try_into()
                    .map_err(|_| StorageError::CorruptEntry("invalid hset count bytes".into()))?,
            ) as usize;
            cursor += 4;
            let mut fields = Vec::with_capacity(count);
            for _ in 0..count {
                let field = read_bytes(data, &mut cursor)?;
                let value = read_bytes(data, &mut cursor)?;
                fields.push((field, value));
            }
            Ok(WalEntry::HSet { key, fields })
        }
        WAL_SADD => {
            let key = read_bytes(data, &mut cursor)?;
            let members = read_vec_of_bytes(data, &mut cursor)?;
            Ok(WalEntry::SAdd { key, members })
        }
        WAL_FLUSH => Ok(WalEntry::FlushDb),
        WAL_DOC_SET => {
            let collection = read_bytes(data, &mut cursor)?;
            let doc_id = read_bytes(data, &mut cursor)?;
            let json = read_bytes(data, &mut cursor)?;
            Ok(WalEntry::DocSet {
                collection,
                doc_id,
                json,
            })
        }
        WAL_DOC_DEL => {
            let collection = read_bytes(data, &mut cursor)?;
            let doc_id = read_bytes(data, &mut cursor)?;
            Ok(WalEntry::DocDel { collection, doc_id })
        }
        WAL_VEC_SET => {
            let key = read_bytes(data, &mut cursor)?;
            if cursor + 4 > data.len() {
                return Err(StorageError::CorruptEntry(
                    "truncated vec dimensions".into(),
                ));
            }
            let dimensions = u32::from_le_bytes(
                data[cursor..cursor + 4]
                    .try_into()
                    .map_err(|_| StorageError::CorruptEntry("invalid dimensions bytes".into()))?,
            ) as usize;
            cursor += 4;
            let vector = read_bytes(data, &mut cursor)?;
            Ok(WalEntry::VecSet {
                key,
                dimensions,
                vector,
            })
        }
        WAL_VEC_DEL => {
            let key = read_bytes(data, &mut cursor)?;
            let _ = cursor;
            Ok(WalEntry::VecDel { key })
        }
        other => Err(StorageError::CorruptEntry(format!(
            "unknown entry type: {}",
            other
        ))),
    }
}

fn write_bytes(buf: &mut Vec<u8>, data: &[u8]) {
    let len = data.len() as u32;
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(data);
}

fn read_bytes(data: &[u8], cursor: &mut usize) -> Result<Vec<u8>> {
    if *cursor + 4 > data.len() {
        return Err(StorageError::CorruptEntry("truncated length".into()));
    }
    let len = u32::from_le_bytes(
        data[*cursor..*cursor + 4]
            .try_into()
            .map_err(|_| StorageError::CorruptEntry("invalid length bytes".into()))?,
    ) as usize;
    *cursor += 4;
    if *cursor + len > data.len() {
        return Err(StorageError::CorruptEntry("truncated data".into()));
    }
    let result = data[*cursor..*cursor + len].to_vec();
    *cursor += len;
    Ok(result)
}

fn write_vec_of_bytes(buf: &mut Vec<u8>, items: &[Vec<u8>]) {
    let count = items.len() as u32;
    buf.extend_from_slice(&count.to_le_bytes());
    for item in items {
        write_bytes(buf, item);
    }
}

fn read_vec_of_bytes(data: &[u8], cursor: &mut usize) -> Result<Vec<Vec<u8>>> {
    if *cursor + 4 > data.len() {
        return Err(StorageError::CorruptEntry("truncated vec count".into()));
    }
    let count = u32::from_le_bytes(
        data[*cursor..*cursor + 4]
            .try_into()
            .map_err(|_| StorageError::CorruptEntry("invalid vec count bytes".into()))?,
    ) as usize;
    *cursor += 4;
    let mut result = Vec::with_capacity(count);
    for _ in 0..count {
        result.push(read_bytes(data, cursor)?);
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn temp_wal() -> (TempDir, PathBuf) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.wal");
        (dir, path)
    }

    #[test]
    fn test_append_and_replay() {
        let (_dir, path) = temp_wal();

        {
            let mut wal = WriteAheadLog::open(&path, SyncPolicy::EveryWrite).unwrap();
            wal.append(&WalEntry::Set {
                key: b"hello".to_vec(),
                value: b"world".to_vec(),
                ttl_ms: None,
            })
            .unwrap();
            wal.append(&WalEntry::Set {
                key: b"foo".to_vec(),
                value: b"bar".to_vec(),
                ttl_ms: Some(5000),
            })
            .unwrap();
            wal.append(&WalEntry::Del {
                key: b"hello".to_vec(),
            })
            .unwrap();
        }

        let mut entries = Vec::new();
        let count = WriteAheadLog::replay(&path, |e| entries.push(e)).unwrap();
        assert_eq!(count, 3);
        assert_eq!(
            entries[0],
            WalEntry::Set {
                key: b"hello".to_vec(),
                value: b"world".to_vec(),
                ttl_ms: None,
            }
        );
        assert_eq!(
            entries[1],
            WalEntry::Set {
                key: b"foo".to_vec(),
                value: b"bar".to_vec(),
                ttl_ms: Some(5000),
            }
        );
        assert_eq!(
            entries[2],
            WalEntry::Del {
                key: b"hello".to_vec(),
            }
        );
    }

    #[test]
    fn test_all_entry_types() {
        let (_dir, path) = temp_wal();

        let entries_in = vec![
            WalEntry::Set {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                ttl_ms: Some(1000),
            },
            WalEntry::Del { key: b"k".to_vec() },
            WalEntry::Expire {
                key: b"k2".to_vec(),
                ttl_ms: 3000,
            },
            WalEntry::LPush {
                key: b"list".to_vec(),
                values: vec![b"a".to_vec(), b"b".to_vec()],
            },
            WalEntry::RPush {
                key: b"list".to_vec(),
                values: vec![b"c".to_vec()],
            },
            WalEntry::HSet {
                key: b"hash".to_vec(),
                fields: vec![
                    (b"f1".to_vec(), b"v1".to_vec()),
                    (b"f2".to_vec(), b"v2".to_vec()),
                ],
            },
            WalEntry::SAdd {
                key: b"set".to_vec(),
                members: vec![b"m1".to_vec(), b"m2".to_vec()],
            },
            WalEntry::FlushDb,
        ];

        {
            let mut wal = WriteAheadLog::open(&path, SyncPolicy::OsManaged).unwrap();
            for entry in &entries_in {
                wal.append(entry).unwrap();
            }
            wal.sync().unwrap();
        }

        let mut entries_out = Vec::new();
        let count = WriteAheadLog::replay(&path, |e| entries_out.push(e)).unwrap();
        assert_eq!(count, entries_in.len() as u64);
        assert_eq!(entries_out, entries_in);
    }

    #[test]
    fn test_truncated_wal_recovery() {
        let (_dir, path) = temp_wal();

        {
            let mut wal = WriteAheadLog::open(&path, SyncPolicy::EveryWrite).unwrap();
            wal.append(&WalEntry::Set {
                key: b"good".to_vec(),
                value: b"data".to_vec(),
                ttl_ms: None,
            })
            .unwrap();
            wal.append(&WalEntry::Set {
                key: b"also_good".to_vec(),
                value: b"data2".to_vec(),
                ttl_ms: None,
            })
            .unwrap();
        }

        // Truncate the file to simulate a crash mid-write
        let file_len = fs::metadata(&path).unwrap().len();
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(file_len - 5).unwrap(); // remove last 5 bytes

        let mut entries = Vec::new();
        let count = WriteAheadLog::replay(&path, |e| entries.push(e)).unwrap();
        // Should recover the first entry but not the corrupted second one
        assert_eq!(count, 1);
        assert_eq!(
            entries[0],
            WalEntry::Set {
                key: b"good".to_vec(),
                value: b"data".to_vec(),
                ttl_ms: None,
            }
        );
    }

    #[test]
    fn test_crc_corruption_detection() {
        let (_dir, path) = temp_wal();

        {
            let mut wal = WriteAheadLog::open(&path, SyncPolicy::EveryWrite).unwrap();
            wal.append(&WalEntry::Set {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                ttl_ms: None,
            })
            .unwrap();
            wal.append(&WalEntry::Set {
                key: b"k2".to_vec(),
                value: b"v2".to_vec(),
                ttl_ms: None,
            })
            .unwrap();
        }

        // Corrupt a byte in the middle of the first entry
        let mut data = fs::read(&path).unwrap();
        data[6] ^= 0xFF; // flip a byte in the payload
        fs::write(&path, &data).unwrap();

        let mut entries = Vec::new();
        let count = WriteAheadLog::replay(&path, |e| entries.push(e)).unwrap();
        // CRC mismatch should stop replay before any entries
        assert_eq!(count, 0);
    }

    #[test]
    fn test_rotation() {
        let (_dir, path) = temp_wal();

        {
            let mut wal = WriteAheadLog::open(&path, SyncPolicy::OsManaged).unwrap();
            wal.append(&WalEntry::Set {
                key: b"before".to_vec(),
                value: b"rotation".to_vec(),
                ttl_ms: None,
            })
            .unwrap();

            wal.rotate().unwrap();

            wal.append(&WalEntry::Set {
                key: b"after".to_vec(),
                value: b"rotation".to_vec(),
                ttl_ms: None,
            })
            .unwrap();
        }

        // The current WAL should only have the post-rotation entry
        let mut entries = Vec::new();
        WriteAheadLog::replay(&path, |e| entries.push(e)).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0],
            WalEntry::Set {
                key: b"after".to_vec(),
                value: b"rotation".to_vec(),
                ttl_ms: None,
            }
        );

        // The old WAL should have the pre-rotation entry
        let old_path = path.with_extension("wal.old");
        let mut old_entries = Vec::new();
        WriteAheadLog::replay(&old_path, |e| old_entries.push(e)).unwrap();
        assert_eq!(old_entries.len(), 1);
    }

    #[test]
    fn test_empty_wal_replay() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("nonexistent.wal");

        let count = WriteAheadLog::replay(&path, |_| panic!("should not be called")).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_truncate() {
        let (_dir, path) = temp_wal();

        {
            let mut wal = WriteAheadLog::open(&path, SyncPolicy::OsManaged).unwrap();
            wal.append(&WalEntry::Set {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                ttl_ms: None,
            })
            .unwrap();
            assert!(wal.bytes_written() > 0);

            wal.truncate().unwrap();
            assert_eq!(wal.bytes_written(), 0);
        }

        let count = WriteAheadLog::replay(&path, |_| panic!("should not be called")).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let entries = vec![
            WalEntry::Set {
                key: vec![0u8; 1000],
                value: vec![255u8; 5000],
                ttl_ms: Some(u64::MAX),
            },
            WalEntry::Del { key: vec![] },
            WalEntry::FlushDb,
        ];

        for entry in &entries {
            let encoded = encode_entry(entry);
            let decoded = decode_entry(&encoded).unwrap();
            assert_eq!(&decoded, entry);
        }
    }
}
