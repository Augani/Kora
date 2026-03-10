//! WAL writer trait for per-shard storage integration.
//!
//! This trait allows the storage layer to be injected into the shard engine
//! without creating a circular dependency between `kora-core` and `kora-storage`.

/// A mutation record that can be logged to a WAL.
///
/// This is a core-level type that mirrors the storage layer's `WalEntry`
/// without depending on the storage crate.
#[derive(Debug, Clone, PartialEq)]
pub enum WalRecord {
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

/// Trait for writing WAL entries from a shard worker thread.
///
/// Implementations are expected to be `Send` so they can be moved into
/// worker threads. WAL writes happen synchronously on the shard's own thread.
pub trait WalWriter: Send {
    /// Append a mutation record to the WAL.
    fn append(&mut self, record: &WalRecord);

    /// Truncate the WAL file as part of maintenance operations.
    ///
    /// Default implementation is a no-op for in-memory or non-persistent backends.
    fn truncate(&mut self) -> Result<(), String> {
        Ok(())
    }
}
