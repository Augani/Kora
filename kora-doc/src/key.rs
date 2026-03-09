//! Binary key encoding helpers for document and index records.

use thiserror::Error;

use crate::registry::{CollectionId, DocId, FieldId};

/// Key tags used in compact binary key encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum KeyTag {
    /// Packed document key.
    Doc = 0x01,
    /// Collection metadata key.
    Collection = 0x02,
    /// Collection schema key.
    Schema = 0x03,
    /// Collection dictionary key.
    Dictionary = 0x04,
    /// Collection registry key.
    Registry = 0x05,
    /// Cold pack key for tiered storage.
    ColdDoc = 0x06,
    /// Hash index bucket key.
    HashIndex = 0x10,
    /// Sorted index key.
    SortedIndex = 0x11,
    /// Array index bucket key.
    ArrayIndex = 0x12,
    /// Unique index bucket key.
    UniqueIndex = 0x13,
    /// Compound index bucket key.
    CompoundIndex = 0x14,
    /// CDC stream head key.
    CdcHead = 0x20,
    /// CDC event key.
    CdcEvent = 0x21,
}

/// Binary key decode errors.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum KeyDecodeError {
    /// Key has wrong length for the expected type.
    #[error("invalid key length: expected {expected}, got {actual}")]
    InvalidLength {
        /// Required key length.
        expected: usize,
        /// Actual key length.
        actual: usize,
    },
    /// Key has an unexpected tag.
    #[error("unexpected key tag: expected 0x{expected:02x}, got 0x{actual:02x}")]
    UnexpectedTag {
        /// Expected tag byte.
        expected: u8,
        /// Actual tag byte.
        actual: u8,
    },
}

/// Encode `[0x01][col:2][doc:4]`.
#[must_use]
pub fn encode_doc_key(collection_id: CollectionId, doc_id: DocId) -> [u8; 7] {
    let mut key = [0u8; 7];
    key[0] = KeyTag::Doc as u8;
    key[1..3].copy_from_slice(&collection_id.to_le_bytes());
    key[3..7].copy_from_slice(&doc_id.to_le_bytes());
    key
}

/// Decode `[0x01][col:2][doc:4]`.
pub fn decode_doc_key(key: &[u8]) -> Result<(CollectionId, DocId), KeyDecodeError> {
    decode_collection_doc_key(key, KeyTag::Doc)
}

/// Encode `[0x06][col:2][doc:4]`.
#[must_use]
pub fn encode_cold_doc_key(collection_id: CollectionId, doc_id: DocId) -> [u8; 7] {
    let mut key = [0u8; 7];
    key[0] = KeyTag::ColdDoc as u8;
    key[1..3].copy_from_slice(&collection_id.to_le_bytes());
    key[3..7].copy_from_slice(&doc_id.to_le_bytes());
    key
}

/// Decode `[0x06][col:2][doc:4]`.
pub fn decode_cold_doc_key(key: &[u8]) -> Result<(CollectionId, DocId), KeyDecodeError> {
    decode_collection_doc_key(key, KeyTag::ColdDoc)
}

/// Encode `[tag][col:2]` keys (`0x02`..`0x05`, `0x20`).
#[must_use]
pub fn encode_collection_key(tag: KeyTag, collection_id: CollectionId) -> [u8; 3] {
    let mut key = [0u8; 3];
    key[0] = tag as u8;
    key[1..3].copy_from_slice(&collection_id.to_le_bytes());
    key
}

/// Decode `[tag][col:2]`.
pub fn decode_collection_key(
    key: &[u8],
    expected_tag: KeyTag,
) -> Result<CollectionId, KeyDecodeError> {
    ensure_key_shape(key, 3, expected_tag)?;
    Ok(u16::from_le_bytes([key[1], key[2]]))
}

/// Encode `[tag][col:2][field:2][vhash:4]`.
#[must_use]
pub fn encode_hashed_bucket_key(
    tag: KeyTag,
    collection_id: CollectionId,
    field_id: FieldId,
    value_hash: u32,
) -> [u8; 10] {
    let mut key = [0u8; 10];
    key[0] = tag as u8;
    key[1..3].copy_from_slice(&collection_id.to_le_bytes());
    key[3..5].copy_from_slice(&field_id.to_le_bytes());
    key[5..9].copy_from_slice(&value_hash.to_le_bytes());
    key
}

/// Decode `[tag][col:2][field:2][vhash:4]`.
pub fn decode_hashed_bucket_key(
    key: &[u8],
    expected_tag: KeyTag,
) -> Result<(CollectionId, FieldId, u32), KeyDecodeError> {
    ensure_key_shape(key, 10, expected_tag)?;
    let collection_id = u16::from_le_bytes([key[1], key[2]]);
    let field_id = u16::from_le_bytes([key[3], key[4]]);
    let value_hash = u32::from_le_bytes([key[5], key[6], key[7], key[8]]);
    Ok((collection_id, field_id, value_hash))
}

/// Encode `[0x11][col:2][field:2]`.
#[must_use]
pub fn encode_sorted_index_key(collection_id: CollectionId, field_id: FieldId) -> [u8; 5] {
    let mut key = [0u8; 5];
    key[0] = KeyTag::SortedIndex as u8;
    key[1..3].copy_from_slice(&collection_id.to_le_bytes());
    key[3..5].copy_from_slice(&field_id.to_le_bytes());
    key
}

/// Decode `[0x11][col:2][field:2]`.
pub fn decode_sorted_index_key(key: &[u8]) -> Result<(CollectionId, FieldId), KeyDecodeError> {
    ensure_key_shape(key, 5, KeyTag::SortedIndex)?;
    Ok((
        u16::from_le_bytes([key[1], key[2]]),
        u16::from_le_bytes([key[3], key[4]]),
    ))
}

/// Encode `[0x14][col:2][f1:2][f2:2][vhash:4]`.
#[must_use]
pub fn encode_compound_index_key(
    collection_id: CollectionId,
    first_field_id: FieldId,
    second_field_id: FieldId,
    value_hash: u32,
) -> [u8; 11] {
    let mut key = [0u8; 11];
    key[0] = KeyTag::CompoundIndex as u8;
    key[1..3].copy_from_slice(&collection_id.to_le_bytes());
    key[3..5].copy_from_slice(&first_field_id.to_le_bytes());
    key[5..7].copy_from_slice(&second_field_id.to_le_bytes());
    key[7..11].copy_from_slice(&value_hash.to_le_bytes());
    key
}

/// Decode `[0x14][col:2][f1:2][f2:2][vhash:4]`.
pub fn decode_compound_index_key(
    key: &[u8],
) -> Result<(CollectionId, FieldId, FieldId, u32), KeyDecodeError> {
    ensure_key_shape(key, 11, KeyTag::CompoundIndex)?;
    Ok((
        u16::from_le_bytes([key[1], key[2]]),
        u16::from_le_bytes([key[3], key[4]]),
        u16::from_le_bytes([key[5], key[6]]),
        u32::from_le_bytes([key[7], key[8], key[9], key[10]]),
    ))
}

/// Encode `[0x21][col:2][seq:8]`.
#[must_use]
pub fn encode_cdc_event_key(collection_id: CollectionId, sequence: u64) -> [u8; 11] {
    let mut key = [0u8; 11];
    key[0] = KeyTag::CdcEvent as u8;
    key[1..3].copy_from_slice(&collection_id.to_le_bytes());
    key[3..11].copy_from_slice(&sequence.to_le_bytes());
    key
}

/// Decode `[0x21][col:2][seq:8]`.
pub fn decode_cdc_event_key(key: &[u8]) -> Result<(CollectionId, u64), KeyDecodeError> {
    ensure_key_shape(key, 11, KeyTag::CdcEvent)?;
    Ok((
        u16::from_le_bytes([key[1], key[2]]),
        u64::from_le_bytes([
            key[3], key[4], key[5], key[6], key[7], key[8], key[9], key[10],
        ]),
    ))
}

fn decode_collection_doc_key(
    key: &[u8],
    expected_tag: KeyTag,
) -> Result<(CollectionId, DocId), KeyDecodeError> {
    ensure_key_shape(key, 7, expected_tag)?;
    let collection_id = u16::from_le_bytes([key[1], key[2]]);
    let doc_id = u32::from_le_bytes([key[3], key[4], key[5], key[6]]);
    Ok((collection_id, doc_id))
}

fn ensure_key_shape(
    key: &[u8],
    expected_len: usize,
    expected_tag: KeyTag,
) -> Result<(), KeyDecodeError> {
    if key.len() != expected_len {
        return Err(KeyDecodeError::InvalidLength {
            expected: expected_len,
            actual: key.len(),
        });
    }
    if key[0] != expected_tag as u8 {
        return Err(KeyDecodeError::UnexpectedTag {
            expected: expected_tag as u8,
            actual: key[0],
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_doc_key() {
        let encoded = encode_doc_key(10, 1234);
        let decoded = decode_doc_key(&encoded).expect("doc key should decode");
        assert_eq!(decoded, (10, 1234));
    }

    #[test]
    fn round_trip_hashed_bucket_key() {
        let encoded = encode_hashed_bucket_key(KeyTag::HashIndex, 4, 9, 0xAABBCCDD);
        let decoded =
            decode_hashed_bucket_key(&encoded, KeyTag::HashIndex).expect("hash key should decode");
        assert_eq!(decoded, (4, 9, 0xAABBCCDD));
    }

    #[test]
    fn rejects_invalid_tag() {
        let mut encoded = encode_doc_key(1, 2);
        encoded[0] = KeyTag::ColdDoc as u8;
        let err = decode_doc_key(&encoded).expect_err("wrong tag should fail");
        assert_eq!(
            err,
            KeyDecodeError::UnexpectedTag {
                expected: KeyTag::Doc as u8,
                actual: KeyTag::ColdDoc as u8
            }
        );
    }
}
