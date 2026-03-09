//! RDB-style point-in-time snapshots.
//!
//! Saves and loads the complete database state to/from a compact binary file.
//! This is Kōra's own binary format, optimized for fast serialization of
//! the engine's internal types.
//!
//! ## File format
//!
//! ```text
//! [magic: 8 bytes "KORA_RDB"][version: u32][num_entries: u64]
//! [entry]*
//! [checksum: u32 CRC-32C of everything before this]
//! ```
//!
//! Each entry:
//! ```text
//! [key_len: u32][key_bytes][ttl_flag: u8][ttl_ms: u64 if flag=1]
//! [value_type: u8][value_data...]
//! ```

use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::Path;

use crate::error::{Result, StorageError};

const MAGIC: &[u8; 8] = b"KORA_RDB";
const VERSION: u32 = 1;

const TYPE_INLINE_STR: u8 = 0;
const TYPE_HEAP_STR: u8 = 1;
const TYPE_INT: u8 = 2;
const TYPE_LIST: u8 = 3;
const TYPE_SET: u8 = 4;
const TYPE_HASH: u8 = 5;

/// A snapshot entry for serialization.
///
/// This is a simplified representation decoupled from internal types so the
/// storage layer doesn't need to depend on `Instant` (which is not portable).
#[derive(Debug, Clone, PartialEq)]
pub struct RdbEntry {
    /// The key bytes.
    pub key: Vec<u8>,
    /// The value.
    pub value: RdbValue,
    /// Remaining TTL in milliseconds, if any.
    pub ttl_ms: Option<u64>,
}

/// A value in the RDB file.
#[derive(Debug, Clone, PartialEq)]
pub enum RdbValue {
    /// A string/bytes value.
    String(Vec<u8>),
    /// An integer value.
    Int(i64),
    /// A list of byte strings.
    List(Vec<Vec<u8>>),
    /// A set of byte strings.
    Set(Vec<Vec<u8>>),
    /// A hash of field→value byte strings.
    Hash(Vec<(Vec<u8>, Vec<u8>)>),
}

/// Write a complete RDB snapshot to the given path.
pub fn save(path: impl AsRef<Path>, entries: &[RdbEntry]) -> Result<()> {
    let path = path.as_ref();
    let tmp_path = path.with_extension("rdb.tmp");

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let file = File::create(&tmp_path)?;
    let mut writer = BufWriter::new(file);
    let mut hasher = crc32fast::Hasher::new();

    write_and_hash(&mut writer, &mut hasher, MAGIC)?;
    write_and_hash(&mut writer, &mut hasher, &VERSION.to_le_bytes())?;
    write_and_hash(
        &mut writer,
        &mut hasher,
        &(entries.len() as u64).to_le_bytes(),
    )?;

    for entry in entries {
        encode_rdb_entry(&mut writer, &mut hasher, entry)?;
    }

    let crc = hasher.finalize();
    writer.write_all(&crc.to_le_bytes())?;
    writer.flush()?;
    writer.get_ref().sync_all()?;

    fs::rename(&tmp_path, path)?;

    Ok(())
}

/// Load an RDB snapshot from the given path.
pub fn load(path: impl AsRef<Path>) -> Result<Vec<RdbEntry>> {
    let path = path.as_ref();
    if !path.exists() {
        return Ok(vec![]);
    }

    let data = fs::read(path)?;
    if data.len() < 8 + 4 + 8 + 4 {
        return Err(StorageError::InvalidRdb("file too small".into()));
    }

    // Verify CRC-32C before parsing to reject corrupted files early.
    let payload = &data[..data.len() - 4];
    let stored_crc = u32::from_le_bytes(
        data[data.len() - 4..]
            .try_into()
            .map_err(|_| StorageError::InvalidRdb("invalid CRC bytes".into()))?,
    );
    let computed_crc = crc32fast::hash(payload);
    if stored_crc != computed_crc {
        return Err(StorageError::CrcMismatch {
            expected: stored_crc,
            actual: computed_crc,
        });
    }

    let mut cursor = 0usize;

    if &data[cursor..cursor + 8] != MAGIC {
        return Err(StorageError::InvalidRdb("bad magic bytes".into()));
    }
    cursor += 8;

    let version = u32::from_le_bytes(
        data[cursor..cursor + 4]
            .try_into()
            .map_err(|_| StorageError::InvalidRdb("invalid version bytes".into()))?,
    );
    if version != VERSION {
        return Err(StorageError::InvalidRdb(format!(
            "unsupported version: {}",
            version
        )));
    }
    cursor += 4;

    let num_entries = u64::from_le_bytes(
        data[cursor..cursor + 8]
            .try_into()
            .map_err(|_| StorageError::InvalidRdb("invalid entry count bytes".into()))?,
    ) as usize;
    cursor += 8;

    let mut entries = Vec::with_capacity(num_entries);
    for _ in 0..num_entries {
        let entry = decode_rdb_entry(&data, &mut cursor)?;
        entries.push(entry);
    }

    Ok(entries)
}

fn encode_rdb_entry(
    w: &mut BufWriter<File>,
    h: &mut crc32fast::Hasher,
    entry: &RdbEntry,
) -> Result<()> {
    write_and_hash(w, h, &(entry.key.len() as u32).to_le_bytes())?;
    write_and_hash(w, h, &entry.key)?;

    match entry.ttl_ms {
        Some(ms) => {
            write_and_hash(w, h, &[1])?;
            write_and_hash(w, h, &ms.to_le_bytes())?;
        }
        None => {
            write_and_hash(w, h, &[0])?;
        }
    }

    match &entry.value {
        RdbValue::String(data) => {
            write_and_hash(w, h, &[TYPE_INLINE_STR])?;
            write_and_hash(w, h, &(data.len() as u32).to_le_bytes())?;
            write_and_hash(w, h, data)?;
        }
        RdbValue::Int(i) => {
            write_and_hash(w, h, &[TYPE_INT])?;
            write_and_hash(w, h, &i.to_le_bytes())?;
        }
        RdbValue::List(items) => {
            write_and_hash(w, h, &[TYPE_LIST])?;
            write_and_hash(w, h, &(items.len() as u32).to_le_bytes())?;
            for item in items {
                write_and_hash(w, h, &(item.len() as u32).to_le_bytes())?;
                write_and_hash(w, h, item)?;
            }
        }
        RdbValue::Set(members) => {
            write_and_hash(w, h, &[TYPE_SET])?;
            write_and_hash(w, h, &(members.len() as u32).to_le_bytes())?;
            for member in members {
                write_and_hash(w, h, &(member.len() as u32).to_le_bytes())?;
                write_and_hash(w, h, member)?;
            }
        }
        RdbValue::Hash(fields) => {
            write_and_hash(w, h, &[TYPE_HASH])?;
            write_and_hash(w, h, &(fields.len() as u32).to_le_bytes())?;
            for (field, value) in fields {
                write_and_hash(w, h, &(field.len() as u32).to_le_bytes())?;
                write_and_hash(w, h, field)?;
                write_and_hash(w, h, &(value.len() as u32).to_le_bytes())?;
                write_and_hash(w, h, value)?;
            }
        }
    }

    Ok(())
}

fn decode_rdb_entry(data: &[u8], cursor: &mut usize) -> Result<RdbEntry> {
    let key_len = read_u32(data, cursor)? as usize;
    let key = read_exact(data, cursor, key_len)?;

    let ttl_flag = read_u8(data, cursor)?;
    let ttl_ms = if ttl_flag == 1 {
        Some(read_u64(data, cursor)?)
    } else {
        None
    };

    let value_type = read_u8(data, cursor)?;
    let value = match value_type {
        TYPE_INLINE_STR | TYPE_HEAP_STR => {
            let len = read_u32(data, cursor)? as usize;
            let bytes = read_exact(data, cursor, len)?;
            RdbValue::String(bytes)
        }
        TYPE_INT => {
            let i = read_i64(data, cursor)?;
            RdbValue::Int(i)
        }
        TYPE_LIST => {
            let count = read_u32(data, cursor)? as usize;
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                let len = read_u32(data, cursor)? as usize;
                items.push(read_exact(data, cursor, len)?);
            }
            RdbValue::List(items)
        }
        TYPE_SET => {
            let count = read_u32(data, cursor)? as usize;
            let mut members = Vec::with_capacity(count);
            for _ in 0..count {
                let len = read_u32(data, cursor)? as usize;
                members.push(read_exact(data, cursor, len)?);
            }
            RdbValue::Set(members)
        }
        TYPE_HASH => {
            let count = read_u32(data, cursor)? as usize;
            let mut fields = Vec::with_capacity(count);
            for _ in 0..count {
                let flen = read_u32(data, cursor)? as usize;
                let field = read_exact(data, cursor, flen)?;
                let vlen = read_u32(data, cursor)? as usize;
                let value = read_exact(data, cursor, vlen)?;
                fields.push((field, value));
            }
            RdbValue::Hash(fields)
        }
        other => {
            return Err(StorageError::InvalidRdb(format!(
                "unknown value type: {}",
                other
            )));
        }
    };

    Ok(RdbEntry { key, value, ttl_ms })
}

fn write_and_hash(w: &mut BufWriter<File>, h: &mut crc32fast::Hasher, data: &[u8]) -> Result<()> {
    w.write_all(data)?;
    h.update(data);
    Ok(())
}

fn read_u8(data: &[u8], cursor: &mut usize) -> Result<u8> {
    if *cursor >= data.len() {
        return Err(StorageError::InvalidRdb("unexpected EOF".into()));
    }
    let val = data[*cursor];
    *cursor += 1;
    Ok(val)
}

fn read_u32(data: &[u8], cursor: &mut usize) -> Result<u32> {
    if *cursor + 4 > data.len() {
        return Err(StorageError::InvalidRdb("unexpected EOF".into()));
    }
    let val = u32::from_le_bytes(
        data[*cursor..*cursor + 4]
            .try_into()
            .map_err(|_| StorageError::InvalidRdb("invalid u32 bytes".into()))?,
    );
    *cursor += 4;
    Ok(val)
}

fn read_u64(data: &[u8], cursor: &mut usize) -> Result<u64> {
    if *cursor + 8 > data.len() {
        return Err(StorageError::InvalidRdb("unexpected EOF".into()));
    }
    let val = u64::from_le_bytes(
        data[*cursor..*cursor + 8]
            .try_into()
            .map_err(|_| StorageError::InvalidRdb("invalid u64 bytes".into()))?,
    );
    *cursor += 8;
    Ok(val)
}

fn read_i64(data: &[u8], cursor: &mut usize) -> Result<i64> {
    if *cursor + 8 > data.len() {
        return Err(StorageError::InvalidRdb("unexpected EOF".into()));
    }
    let val = i64::from_le_bytes(
        data[*cursor..*cursor + 8]
            .try_into()
            .map_err(|_| StorageError::InvalidRdb("invalid i64 bytes".into()))?,
    );
    *cursor += 8;
    Ok(val)
}

fn read_exact(data: &[u8], cursor: &mut usize, len: usize) -> Result<Vec<u8>> {
    if *cursor + len > data.len() {
        return Err(StorageError::InvalidRdb("unexpected EOF".into()));
    }
    let result = data[*cursor..*cursor + len].to_vec();
    *cursor += len;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_save_and_load_empty() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.rdb");

        save(&path, &[]).unwrap();
        let entries = load(&path).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_save_and_load_strings() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.rdb");

        let entries = vec![
            RdbEntry {
                key: b"hello".to_vec(),
                value: RdbValue::String(b"world".to_vec()),
                ttl_ms: None,
            },
            RdbEntry {
                key: b"foo".to_vec(),
                value: RdbValue::String(b"bar".to_vec()),
                ttl_ms: Some(5000),
            },
            RdbEntry {
                key: b"counter".to_vec(),
                value: RdbValue::Int(42),
                ttl_ms: None,
            },
        ];

        save(&path, &entries).unwrap();
        let loaded = load(&path).unwrap();
        assert_eq!(loaded, entries);
    }

    #[test]
    fn test_save_and_load_collections() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.rdb");

        let entries = vec![
            RdbEntry {
                key: b"mylist".to_vec(),
                value: RdbValue::List(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]),
                ttl_ms: None,
            },
            RdbEntry {
                key: b"myset".to_vec(),
                value: RdbValue::Set(vec![b"x".to_vec(), b"y".to_vec()]),
                ttl_ms: Some(10000),
            },
            RdbEntry {
                key: b"myhash".to_vec(),
                value: RdbValue::Hash(vec![
                    (b"name".to_vec(), b"Alice".to_vec()),
                    (b"age".to_vec(), b"30".to_vec()),
                ]),
                ttl_ms: None,
            },
        ];

        save(&path, &entries).unwrap();
        let loaded = load(&path).unwrap();
        assert_eq!(loaded, entries);
    }

    #[test]
    fn test_load_nonexistent() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("nonexistent.rdb");
        let entries = load(&path).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_corruption_detection() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.rdb");

        save(
            &path,
            &[RdbEntry {
                key: b"k".to_vec(),
                value: RdbValue::String(b"v".to_vec()),
                ttl_ms: None,
            }],
        )
        .unwrap();

        // Corrupt a byte
        let mut data = fs::read(&path).unwrap();
        data[12] ^= 0xFF;
        fs::write(&path, &data).unwrap();

        assert!(load(&path).is_err());
    }

    #[test]
    fn test_atomic_write() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.rdb");

        // First save
        save(
            &path,
            &[RdbEntry {
                key: b"v1".to_vec(),
                value: RdbValue::String(b"first".to_vec()),
                ttl_ms: None,
            }],
        )
        .unwrap();

        // Second save (should atomically replace)
        save(
            &path,
            &[RdbEntry {
                key: b"v2".to_vec(),
                value: RdbValue::String(b"second".to_vec()),
                ttl_ms: None,
            }],
        )
        .unwrap();

        let loaded = load(&path).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].key, b"v2");

        // Temp file should not exist
        assert!(!path.with_extension("rdb.tmp").exists());
    }

    #[test]
    fn test_large_entries() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.rdb");

        let big_value = vec![0xABu8; 100_000];
        let entries = vec![RdbEntry {
            key: b"big".to_vec(),
            value: RdbValue::String(big_value.clone()),
            ttl_ms: None,
        }];

        save(&path, &entries).unwrap();
        let loaded = load(&path).unwrap();
        assert_eq!(loaded[0].value, RdbValue::String(big_value));
    }
}
