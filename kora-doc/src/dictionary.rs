//! Dictionary encoding for low-cardinality string field values.
//!
//! When a field's unique value count stays below a configurable threshold,
//! storing the same string bytes in every packed document wastes space.
//! [`ValueDictionary`] assigns a compact `u32` dictionary ID to each unique
//! byte sequence and returns a [`StoredValue::DictRef`] instead of inlining
//! the full payload. High-cardinality fields (or values shorter than
//! `min_len_for_dictionary`) fall back to [`StoredValue::Inline`].
//!
//! ## Cardinality Estimation
//!
//! Per-field cardinality is tracked with a `HashSet<u64>` of value hashes,
//! giving exact counts up to the threshold and constant-space tracking
//! thereafter. Once the cardinality of a field exceeds
//! `low_cardinality_threshold`, all subsequent values for that field are
//! stored inline.
//!
//! ## Encoding / Decoding
//!
//! - **Encode:** `encode(field_id, value_bytes)` returns `DictRef(id)` or
//!   `Inline(bytes)`.
//! - **Decode:** `decode(stored)` maps `DictRef(id)` back to owned bytes, or
//!   clones `Inline(bytes)` directly.

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use thiserror::Error;

use crate::registry::FieldId;

/// Stored field value representation after dictionary encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoredValue {
    /// Reference to a dictionary value.
    DictRef(u32),
    /// Inline bytes stored directly in the packed payload.
    Inline(Vec<u8>),
}

/// Value dictionary tuning parameters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValueDictionaryConfig {
    /// Maximum unique cardinality per field before dictionary encoding is disabled.
    pub low_cardinality_threshold: usize,
    /// Minimum byte length before dictionary encoding is considered.
    pub min_len_for_dictionary: usize,
}

impl Default for ValueDictionaryConfig {
    fn default() -> Self {
        Self {
            low_cardinality_threshold: 10_000,
            min_len_for_dictionary: 5,
        }
    }
}

/// Errors returned by dictionary operations.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum DictionaryError {
    /// Dictionary ID space is exhausted.
    #[error("dictionary id space exhausted")]
    DictionaryIdOverflow,
    /// The provided dictionary reference does not exist.
    #[error("unknown dictionary id {0}")]
    UnknownDictionaryId(u32),
}

/// Per-collection value dictionary.
#[derive(Debug, Default)]
pub struct ValueDictionary {
    config: ValueDictionaryConfig,
    id_by_value: HashMap<Vec<u8>, u32>,
    value_by_id: HashMap<u32, Vec<u8>>,
    field_unique_hashes: HashMap<FieldId, HashSet<u64>>,
    next_id: u64,
}

impl ValueDictionary {
    /// Create a dictionary with custom configuration.
    #[must_use]
    pub fn new(config: ValueDictionaryConfig) -> Self {
        Self {
            config,
            ..Self::default()
        }
    }

    /// Encode a value for a field.
    ///
    /// Values are dictionary-encoded only when:
    /// - observed field cardinality is below `low_cardinality_threshold`, and
    /// - value length is at least `min_len_for_dictionary`.
    pub fn encode(
        &mut self,
        field_id: FieldId,
        value: &[u8],
    ) -> Result<StoredValue, DictionaryError> {
        let cardinality = self.record_and_estimate_cardinality(field_id, value);
        if cardinality <= self.config.low_cardinality_threshold
            && value.len() >= self.config.min_len_for_dictionary
        {
            let id = self.get_or_insert(value)?;
            return Ok(StoredValue::DictRef(id));
        }

        Ok(StoredValue::Inline(value.to_vec()))
    }

    /// Decode a stored value back to owned bytes.
    pub fn decode(&self, stored: &StoredValue) -> Result<Vec<u8>, DictionaryError> {
        match stored {
            StoredValue::DictRef(id) => self
                .value_by_id
                .get(id)
                .cloned()
                .ok_or(DictionaryError::UnknownDictionaryId(*id)),
            StoredValue::Inline(bytes) => Ok(bytes.clone()),
        }
    }

    /// Return the configured cardinality estimate for a field.
    #[must_use]
    pub fn cardinality_estimate(&self, field_id: FieldId) -> usize {
        self.field_unique_hashes
            .get(&field_id)
            .map_or(0, HashSet::len)
    }

    /// Number of unique values stored in the dictionary.
    #[must_use]
    pub fn len(&self) -> usize {
        self.id_by_value.len()
    }

    /// Returns true when the dictionary has no values.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.id_by_value.is_empty()
    }

    fn get_or_insert(&mut self, value: &[u8]) -> Result<u32, DictionaryError> {
        if let Some(id) = self.id_by_value.get(value) {
            return Ok(*id);
        }

        let id = u32::try_from(self.next_id).map_err(|_| DictionaryError::DictionaryIdOverflow)?;
        self.next_id += 1;

        let owned = value.to_vec();
        self.id_by_value.insert(owned.clone(), id);
        self.value_by_id.insert(id, owned);
        Ok(id)
    }

    fn record_and_estimate_cardinality(&mut self, field_id: FieldId, value: &[u8]) -> usize {
        let hash = stable_hash(value);
        let set = self.field_unique_hashes.entry(field_id).or_default();
        set.insert(hash);
        set.len()
    }
}

fn stable_hash<T: Hash>(value: T) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn low_cardinality_values_use_dictionary() {
        let mut dict = ValueDictionary::new(ValueDictionaryConfig {
            low_cardinality_threshold: 10,
            min_len_for_dictionary: 5,
        });

        let first = dict.encode(1, b"accra").expect("encoding should succeed");
        let second = dict.encode(1, b"accra").expect("encoding should succeed");

        assert_eq!(first, second);
        assert_eq!(dict.len(), 1);
    }

    #[test]
    fn high_cardinality_values_fall_back_to_inline() {
        let mut dict = ValueDictionary::new(ValueDictionaryConfig {
            low_cardinality_threshold: 1,
            min_len_for_dictionary: 3,
        });

        let first = dict.encode(7, b"one").expect("encoding should succeed");
        let second = dict.encode(7, b"two").expect("encoding should succeed");

        assert!(matches!(first, StoredValue::DictRef(_)));
        assert_eq!(second, StoredValue::Inline(b"two".to_vec()));
    }

    #[test]
    fn decode_round_trip() {
        let mut dict = ValueDictionary::default();
        let encoded = dict.encode(11, b"london").expect("encoding should succeed");
        let decoded = dict.decode(&encoded).expect("decode should succeed");
        assert_eq!(decoded, b"london".to_vec());
    }
}
