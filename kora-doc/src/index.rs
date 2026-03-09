//! Index data structures and configuration for document collections.

use std::collections::{BTreeMap, HashMap};

use thiserror::Error;

use crate::registry::{DocId, FieldId};

/// Type of secondary index on a collection field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IndexType {
    /// Hash-based equality index for fast point lookups.
    Hash,
    /// B-tree sorted index for range queries on numeric fields.
    Sorted,
    /// Hash-based index for array element membership queries.
    Array,
    /// Hash-based unique constraint index.
    Unique,
}

/// Errors from index operations.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum IndexError {
    /// An index already exists for this field.
    #[error("index already exists for field {0}")]
    AlreadyExists(FieldId),
    /// No index exists for this field.
    #[error("no index found for field {0}")]
    NotFound(FieldId),
    /// A unique constraint violation occurred.
    #[error("unique index violation: hash {hash} already maps to doc_id {existing_doc_id}")]
    UniqueViolation {
        /// The colliding hash bucket key.
        hash: u32,
        /// The document that already occupies the slot.
        existing_doc_id: DocId,
    },
}

/// Per-collection mapping of field IDs to their configured index types.
#[derive(Debug, Default)]
pub struct IndexConfig {
    entries: HashMap<FieldId, IndexType>,
}

impl IndexConfig {
    /// Create an empty index configuration.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register an index for a field. Returns an error if one already exists.
    pub fn add(&mut self, field_id: FieldId, index_type: IndexType) -> Result<(), IndexError> {
        if self.entries.contains_key(&field_id) {
            return Err(IndexError::AlreadyExists(field_id));
        }
        self.entries.insert(field_id, index_type);
        Ok(())
    }

    /// Remove an index configuration for a field. Returns an error if not found.
    pub fn remove(&mut self, field_id: FieldId) -> Result<IndexType, IndexError> {
        self.entries
            .remove(&field_id)
            .ok_or(IndexError::NotFound(field_id))
    }

    /// Look up the index type for a field.
    #[must_use]
    pub fn lookup(&self, field_id: FieldId) -> Option<IndexType> {
        self.entries.get(&field_id).copied()
    }

    /// Return all configured field-to-index-type pairs.
    #[must_use]
    pub fn entries(&self) -> &HashMap<FieldId, IndexType> {
        &self.entries
    }
}

/// FNV-1a 32-bit hash for index bucket keys.
#[must_use]
pub fn hash32(data: &[u8]) -> u32 {
    let mut hash: u32 = 0x811c_9dc5;
    for &byte in data {
        hash ^= u32::from(byte);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}

/// Hash-based equality index mapping FNV-1a bucket keys to sorted doc ID lists.
#[derive(Debug, Default)]
pub struct HashIndex {
    buckets: HashMap<u32, Vec<DocId>>,
}

impl HashIndex {
    /// Create an empty hash index.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a document to a hash bucket, maintaining sorted order. Duplicate adds are idempotent.
    pub fn add(&mut self, hash: u32, doc_id: DocId) {
        let bucket = self.buckets.entry(hash).or_default();
        match bucket.binary_search(&doc_id) {
            Ok(_) => {}
            Err(pos) => bucket.insert(pos, doc_id),
        }
    }

    /// Remove a document from a hash bucket. No-op if not present.
    pub fn remove(&mut self, hash: u32, doc_id: DocId) {
        if let Some(bucket) = self.buckets.get_mut(&hash) {
            if let Ok(pos) = bucket.binary_search(&doc_id) {
                bucket.remove(pos);
            }
            if bucket.is_empty() {
                self.buckets.remove(&hash);
            }
        }
    }

    /// Look up all document IDs in a hash bucket.
    #[must_use]
    pub fn lookup(&self, hash: u32) -> &[DocId] {
        self.buckets.get(&hash).map_or(&[], Vec::as_slice)
    }

    /// Remove all entries from the index.
    pub fn clear(&mut self) {
        self.buckets.clear();
    }
}

/// Wrapper around `f64` providing total ordering suitable for `BTreeMap` keys.
///
/// Ordering: all negative values (ordered normally) < negative zero < positive zero <
/// all positive values (ordered normally) < NaN.
#[derive(Debug, Clone, Copy)]
pub struct OrderedF64(f64);

impl OrderedF64 {
    /// Wrap a raw `f64` value.
    #[must_use]
    pub fn new(value: f64) -> Self {
        Self(value)
    }

    /// Return the inner `f64` value.
    #[must_use]
    pub fn value(self) -> f64 {
        self.0
    }

    fn to_sort_key(self) -> u64 {
        let bits = self.0.to_bits();
        if self.0.is_nan() {
            return u64::MAX;
        }
        if bits >> 63 == 1 {
            !bits
        } else {
            bits | (1 << 63)
        }
    }
}

impl PartialEq for OrderedF64 {
    fn eq(&self, other: &Self) -> bool {
        self.to_sort_key() == other.to_sort_key()
    }
}

impl Eq for OrderedF64 {}

impl PartialOrd for OrderedF64 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedF64 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.to_sort_key().cmp(&other.to_sort_key())
    }
}

/// Sorted index backed by a `BTreeMap` for numeric range queries.
#[derive(Debug, Default)]
pub struct SortedIndex {
    tree: BTreeMap<OrderedF64, Vec<DocId>>,
}

impl SortedIndex {
    /// Create an empty sorted index.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a document to a sorted bucket, maintaining sorted order within the bucket.
    pub fn add(&mut self, key: f64, doc_id: DocId) {
        let bucket = self.tree.entry(OrderedF64::new(key)).or_default();
        match bucket.binary_search(&doc_id) {
            Ok(_) => {}
            Err(pos) => bucket.insert(pos, doc_id),
        }
    }

    /// Remove a document from a sorted bucket. No-op if not present.
    pub fn remove(&mut self, key: f64, doc_id: DocId) {
        let ordered = OrderedF64::new(key);
        if let Some(bucket) = self.tree.get_mut(&ordered) {
            if let Ok(pos) = bucket.binary_search(&doc_id) {
                bucket.remove(pos);
            }
            if bucket.is_empty() {
                self.tree.remove(&ordered);
            }
        }
    }

    /// Return all document IDs whose keys fall within `[min, max]` (inclusive), sorted.
    #[must_use]
    pub fn range_query(&self, min: f64, max: f64) -> Vec<DocId> {
        let lo = OrderedF64::new(min);
        let hi = OrderedF64::new(max);
        let mut result = Vec::new();
        for (_key, bucket) in self.tree.range(lo..=hi) {
            result.extend_from_slice(bucket);
        }
        result.sort_unstable();
        result.dedup();
        result
    }

    /// Remove all entries from the index.
    pub fn clear(&mut self) {
        self.tree.clear();
    }
}

/// Unique constraint index mapping a single hash bucket key to exactly one document.
#[derive(Debug, Default)]
pub struct UniqueIndex {
    entries: HashMap<u32, DocId>,
}

impl UniqueIndex {
    /// Create an empty unique index.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a document to the unique index. Returns `UniqueViolation` if the hash bucket
    /// is already occupied by a different document.
    pub fn add(&mut self, hash: u32, doc_id: DocId) -> Result<(), IndexError> {
        if let Some(&existing) = self.entries.get(&hash) {
            if existing != doc_id {
                return Err(IndexError::UniqueViolation {
                    hash,
                    existing_doc_id: existing,
                });
            }
            return Ok(());
        }
        self.entries.insert(hash, doc_id);
        Ok(())
    }

    /// Remove a document from the unique index. No-op if not present.
    pub fn remove(&mut self, hash: u32) {
        self.entries.remove(&hash);
    }

    /// Look up the document occupying a hash bucket.
    #[must_use]
    pub fn lookup(&self, hash: u32) -> Option<DocId> {
        self.entries.get(&hash).copied()
    }

    /// Remove all entries from the index.
    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

/// Container holding all index instances for a single collection.
#[derive(Debug, Default)]
pub struct CollectionIndexes {
    hash_indexes: HashMap<FieldId, HashIndex>,
    sorted_indexes: HashMap<FieldId, SortedIndex>,
    array_indexes: HashMap<FieldId, HashIndex>,
    unique_indexes: HashMap<FieldId, UniqueIndex>,
}

impl CollectionIndexes {
    /// Create an empty collection indexes container.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Return a mutable reference to the hash index for a field, creating one if absent.
    pub fn get_or_create_hash(&mut self, field_id: FieldId) -> &mut HashIndex {
        self.hash_indexes.entry(field_id).or_default()
    }

    /// Return a mutable reference to the sorted index for a field, creating one if absent.
    pub fn get_or_create_sorted(&mut self, field_id: FieldId) -> &mut SortedIndex {
        self.sorted_indexes.entry(field_id).or_default()
    }

    /// Return a mutable reference to the array index for a field, creating one if absent.
    pub fn get_or_create_array(&mut self, field_id: FieldId) -> &mut HashIndex {
        self.array_indexes.entry(field_id).or_default()
    }

    /// Return a mutable reference to the unique index for a field, creating one if absent.
    pub fn get_or_create_unique(&mut self, field_id: FieldId) -> &mut UniqueIndex {
        self.unique_indexes.entry(field_id).or_default()
    }

    /// Return an immutable reference to the hash index for a field.
    #[must_use]
    pub fn hash(&self, field_id: FieldId) -> Option<&HashIndex> {
        self.hash_indexes.get(&field_id)
    }

    /// Return an immutable reference to the sorted index for a field.
    #[must_use]
    pub fn sorted(&self, field_id: FieldId) -> Option<&SortedIndex> {
        self.sorted_indexes.get(&field_id)
    }

    /// Return an immutable reference to the array index for a field.
    #[must_use]
    pub fn array(&self, field_id: FieldId) -> Option<&HashIndex> {
        self.array_indexes.get(&field_id)
    }

    /// Return an immutable reference to the unique index for a field.
    #[must_use]
    pub fn unique(&self, field_id: FieldId) -> Option<&UniqueIndex> {
        self.unique_indexes.get(&field_id)
    }

    /// Remove all indexes for a field across all index types.
    pub fn remove_field(&mut self, field_id: FieldId) {
        self.hash_indexes.remove(&field_id);
        self.sorted_indexes.remove(&field_id);
        self.array_indexes.remove(&field_id);
        self.unique_indexes.remove(&field_id);
    }
}

/// Compute the sorted intersection of two sorted `DocId` slices (merge-join).
#[must_use]
pub fn intersect_sorted(a: &[DocId], b: &[DocId]) -> Vec<DocId> {
    let mut result = Vec::new();
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                result.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    result
}

/// Compute the sorted union of two sorted `DocId` slices (merge-union).
#[must_use]
pub fn union_sorted(a: &[DocId], b: &[DocId]) -> Vec<DocId> {
    let mut result = Vec::with_capacity(a.len() + b.len());
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => {
                result.push(a[i]);
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                result.push(b[j]);
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                result.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    result.extend_from_slice(&a[i..]);
    result.extend_from_slice(&b[j..]);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_index_add_remove_lookup() {
        let mut idx = HashIndex::new();
        idx.add(100, 1);
        idx.add(100, 3);
        idx.add(100, 2);
        assert_eq!(idx.lookup(100), &[1, 2, 3]);

        idx.remove(100, 2);
        assert_eq!(idx.lookup(100), &[1, 3]);

        idx.remove(100, 1);
        idx.remove(100, 3);
        assert_eq!(idx.lookup(100), &[] as &[DocId]);
    }

    #[test]
    fn hash_index_duplicate_add_is_idempotent() {
        let mut idx = HashIndex::new();
        idx.add(42, 7);
        idx.add(42, 7);
        idx.add(42, 7);
        assert_eq!(idx.lookup(42), &[7]);
    }

    #[test]
    fn hash_index_remove_nonexistent_is_noop() {
        let mut idx = HashIndex::new();
        idx.remove(999, 1);
        idx.add(10, 5);
        idx.remove(10, 99);
        assert_eq!(idx.lookup(10), &[5]);
    }

    #[test]
    fn sorted_index_add_remove_range_query() {
        let mut idx = SortedIndex::new();
        idx.add(1.0, 10);
        idx.add(3.0, 30);
        idx.add(2.0, 20);
        idx.add(2.0, 25);

        let result = idx.range_query(1.5, 3.0);
        assert_eq!(result, vec![20, 25, 30]);

        idx.remove(2.0, 20);
        let result = idx.range_query(1.5, 3.0);
        assert_eq!(result, vec![25, 30]);
    }

    #[test]
    fn ordered_f64_ordering_negative_zero_positive() {
        let neg = OrderedF64::new(-5.0);
        let zero = OrderedF64::new(0.0);
        let pos = OrderedF64::new(10.0);
        let nan = OrderedF64::new(f64::NAN);

        assert!(neg < zero);
        assert!(zero < pos);
        assert!(pos < nan);
        assert!(neg < pos);
        assert!(neg < nan);
    }

    #[test]
    fn unique_index_add_lookup() {
        let mut idx = UniqueIndex::new();
        idx.add(42, 1).expect("first add should succeed");
        assert_eq!(idx.lookup(42), Some(1));
    }

    #[test]
    fn unique_index_violation_on_different_doc_id() {
        let mut idx = UniqueIndex::new();
        idx.add(42, 1).expect("first add should succeed");
        let err = idx
            .add(42, 2)
            .expect_err("different doc_id same hash must fail");
        assert!(matches!(
            err,
            IndexError::UniqueViolation {
                hash: 42,
                existing_doc_id: 1,
            }
        ));
    }

    #[test]
    fn unique_index_same_doc_id_same_hash_is_ok() {
        let mut idx = UniqueIndex::new();
        idx.add(42, 1).expect("first add should succeed");
        idx.add(42, 1)
            .expect("same doc_id same hash should succeed");
        assert_eq!(idx.lookup(42), Some(1));
    }

    #[test]
    fn index_config_add_remove_entries() {
        let mut config = IndexConfig::new();
        config.add(0, IndexType::Hash).expect("add should succeed");
        config
            .add(1, IndexType::Sorted)
            .expect("add should succeed");

        assert_eq!(config.lookup(0), Some(IndexType::Hash));
        assert_eq!(config.lookup(1), Some(IndexType::Sorted));
        assert_eq!(config.entries().len(), 2);

        let removed = config.remove(0).expect("remove should succeed");
        assert_eq!(removed, IndexType::Hash);
        assert_eq!(config.lookup(0), None);
    }

    #[test]
    fn index_config_duplicate_add_rejected() {
        let mut config = IndexConfig::new();
        config.add(0, IndexType::Hash).expect("add should succeed");
        let err = config
            .add(0, IndexType::Sorted)
            .expect_err("duplicate add must fail");
        assert!(matches!(err, IndexError::AlreadyExists(0)));
    }

    #[test]
    fn index_config_remove_nonexistent_rejected() {
        let mut config = IndexConfig::new();
        let err = config
            .remove(99)
            .expect_err("remove non-existent must fail");
        assert!(matches!(err, IndexError::NotFound(99)));
    }

    #[test]
    fn intersect_sorted_disjoint() {
        let result = intersect_sorted(&[1, 3, 5], &[2, 4, 6]);
        assert!(result.is_empty());
    }

    #[test]
    fn intersect_sorted_overlapping() {
        let result = intersect_sorted(&[1, 2, 3, 5], &[2, 3, 4, 5]);
        assert_eq!(result, vec![2, 3, 5]);
    }

    #[test]
    fn intersect_sorted_empty_input() {
        assert!(intersect_sorted(&[], &[1, 2, 3]).is_empty());
        assert!(intersect_sorted(&[1, 2, 3], &[]).is_empty());
        assert!(intersect_sorted(&[], &[]).is_empty());
    }

    #[test]
    fn union_sorted_disjoint() {
        let result = union_sorted(&[1, 3, 5], &[2, 4, 6]);
        assert_eq!(result, vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn union_sorted_overlapping() {
        let result = union_sorted(&[1, 2, 3], &[2, 3, 4]);
        assert_eq!(result, vec![1, 2, 3, 4]);
    }

    #[test]
    fn union_sorted_empty_input() {
        assert_eq!(union_sorted(&[], &[1, 2]), vec![1, 2]);
        assert_eq!(union_sorted(&[1, 2], &[]), vec![1, 2]);
        assert!(union_sorted(&[], &[]).is_empty());
    }
}
