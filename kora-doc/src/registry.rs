//! Integer-keyed ID registry for collections, fields, and documents.
//!
//! The document layer works exclusively with compact numeric identifiers
//! internally -- [`CollectionId`] (`u16`), [`FieldId`] (`u16`), and
//! [`DocId`] (`u32`). The [`IdRegistry`] is the single source of truth for
//! mapping between human-readable names/paths and these numeric IDs.
//!
//! ## Two-Level Structure
//!
//! - **Global directory** -- maps collection names to `CollectionId` values
//!   and owns a [`RegistrySegment`] per collection.
//! - **Per-collection segment** ([`RegistrySegment`]) -- manages bidirectional
//!   mappings between dotted field paths and `FieldId`, and between external
//!   document ID strings and internal `DocId` values.
//!
//! All IDs are allocated monotonically and never reused. Lookups are O(1)
//! hash-map operations; creation is idempotent (returning the existing ID if
//! the name/path was already registered).
//!
//! ## Segment References
//!
//! Each collection also carries a [`RegistrySegmentRef`] tuple `(shard, key)`
//! that records where the segment's persistent representation lives in the
//! shard-affinity storage layer. This pointer is opaque to the registry
//! itself and managed by the engine.

use std::collections::HashMap;

use thiserror::Error;

/// Compact collection identifier.
pub type CollectionId = u16;
/// Compact field identifier.
pub type FieldId = u16;
/// Compact internal document identifier.
pub type DocId = u32;

/// Pointer to a collection-owned registry segment.
///
/// Tuple layout: `(owner_shard, segment_key)`.
pub type RegistrySegmentRef = (u16, Vec<u8>);

/// Errors returned by registry operations.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum RegistryError {
    /// The registry cannot allocate a new collection ID.
    #[error("collection id space exhausted")]
    CollectionOverflow,
    /// The collection is unknown.
    #[error("unknown collection id {0}")]
    UnknownCollection(CollectionId),
    /// The collection cannot allocate a new field ID.
    #[error("field id space exhausted for collection {collection_id}")]
    FieldOverflow {
        /// Collection ID that exhausted field IDs.
        collection_id: CollectionId,
    },
    /// The collection cannot allocate a new internal document ID.
    #[error("document id space exhausted for collection {collection_id}")]
    DocOverflow {
        /// Collection ID that exhausted document IDs.
        collection_id: CollectionId,
    },
}

/// Global registry directory plus collection-owned segments.
#[derive(Debug, Default)]
pub struct IdRegistry {
    collections_by_name: HashMap<String, CollectionId>,
    collection_names_by_id: HashMap<CollectionId, String>,
    segments_by_collection: HashMap<CollectionId, RegistrySegment>,
    segment_refs: HashMap<CollectionId, RegistrySegmentRef>,
    next_collection_id: u32,
}

impl IdRegistry {
    /// Create an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Return a collection ID for `name`, creating one when needed.
    pub fn get_or_create_collection_id(
        &mut self,
        name: &str,
    ) -> Result<CollectionId, RegistryError> {
        if let Some(id) = self.collections_by_name.get(name) {
            return Ok(*id);
        }

        let id = CollectionId::try_from(self.next_collection_id)
            .map_err(|_| RegistryError::CollectionOverflow)?;
        self.next_collection_id += 1;

        self.collections_by_name.insert(name.to_owned(), id);
        self.collection_names_by_id.insert(id, name.to_owned());
        self.segments_by_collection
            .insert(id, RegistrySegment::new());
        self.segment_refs.insert(id, (0, Vec::new()));

        Ok(id)
    }

    /// Return the collection ID for `name` if it exists.
    #[must_use]
    pub fn collection_id(&self, name: &str) -> Option<CollectionId> {
        self.collections_by_name.get(name).copied()
    }

    /// Return the collection name for `collection_id` if it exists.
    #[must_use]
    pub fn collection_name(&self, collection_id: CollectionId) -> Option<&str> {
        self.collection_names_by_id
            .get(&collection_id)
            .map(String::as_str)
    }

    /// Return the shard/key pointer for a collection segment.
    #[must_use]
    pub fn segment_ref(&self, collection_id: CollectionId) -> Option<&RegistrySegmentRef> {
        self.segment_refs.get(&collection_id)
    }

    /// Update the shard/key pointer for a collection segment.
    pub fn set_segment_ref(
        &mut self,
        collection_id: CollectionId,
        segment_ref: RegistrySegmentRef,
    ) -> Result<(), RegistryError> {
        if !self.segments_by_collection.contains_key(&collection_id) {
            return Err(RegistryError::UnknownCollection(collection_id));
        }
        self.segment_refs.insert(collection_id, segment_ref);
        Ok(())
    }

    /// Return an immutable segment for `collection_id`.
    #[must_use]
    pub fn segment(&self, collection_id: CollectionId) -> Option<&RegistrySegment> {
        self.segments_by_collection.get(&collection_id)
    }

    /// Return a mutable segment for `collection_id`.
    #[must_use]
    pub fn segment_mut(&mut self, collection_id: CollectionId) -> Option<&mut RegistrySegment> {
        self.segments_by_collection.get_mut(&collection_id)
    }

    /// Return or create a field ID in a collection segment.
    pub fn get_or_create_field_id(
        &mut self,
        collection_id: CollectionId,
        path: &str,
    ) -> Result<FieldId, RegistryError> {
        let segment = self
            .segments_by_collection
            .get_mut(&collection_id)
            .ok_or(RegistryError::UnknownCollection(collection_id))?;
        segment.get_or_create_field_id(collection_id, path)
    }

    /// Return or create an internal document ID in a collection segment.
    pub fn get_or_create_doc_internal_id(
        &mut self,
        collection_id: CollectionId,
        external_doc_id: &str,
    ) -> Result<DocId, RegistryError> {
        let segment = self
            .segments_by_collection
            .get_mut(&collection_id)
            .ok_or(RegistryError::UnknownCollection(collection_id))?;
        segment.get_or_create_doc_internal_id(collection_id, external_doc_id)
    }

    /// Resolve field path by field ID.
    #[must_use]
    pub fn field_path(&self, collection_id: CollectionId, field_id: FieldId) -> Option<&str> {
        self.segments_by_collection
            .get(&collection_id)
            .and_then(|s| s.field_path(field_id))
    }

    /// Resolve external document ID by internal document ID.
    #[must_use]
    pub fn doc_external_id(&self, collection_id: CollectionId, doc_id: DocId) -> Option<&str> {
        self.segments_by_collection
            .get(&collection_id)
            .and_then(|s| s.doc_external_id(doc_id))
    }

    /// Number of registered collections.
    #[must_use]
    pub fn collection_count(&self) -> usize {
        self.collections_by_name.len()
    }

    /// Remove a collection and all associated registry state.
    ///
    /// Returns the removed collection ID when present.
    pub fn remove_collection(&mut self, name: &str) -> Option<CollectionId> {
        let collection_id = self.collections_by_name.remove(name)?;
        self.collection_names_by_id.remove(&collection_id);
        self.segments_by_collection.remove(&collection_id);
        self.segment_refs.remove(&collection_id);
        Some(collection_id)
    }
}

/// Per-collection registry segment.
#[derive(Debug, Default)]
pub struct RegistrySegment {
    fields_by_path: HashMap<String, FieldId>,
    paths_by_field: HashMap<FieldId, String>,
    docs_by_external: HashMap<String, DocId>,
    external_by_doc: HashMap<DocId, String>,
    next_field_id: u32,
    next_doc_id: u64,
}

impl RegistrySegment {
    /// Create an empty segment.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Return or create a field ID for `path`.
    pub fn get_or_create_field_id(
        &mut self,
        collection_id: CollectionId,
        path: &str,
    ) -> Result<FieldId, RegistryError> {
        if let Some(field_id) = self.fields_by_path.get(path) {
            return Ok(*field_id);
        }

        let field_id = FieldId::try_from(self.next_field_id)
            .map_err(|_| RegistryError::FieldOverflow { collection_id })?;
        self.next_field_id += 1;

        self.fields_by_path.insert(path.to_owned(), field_id);
        self.paths_by_field.insert(field_id, path.to_owned());
        Ok(field_id)
    }

    /// Return or create an internal document ID for `external_doc_id`.
    pub fn get_or_create_doc_internal_id(
        &mut self,
        collection_id: CollectionId,
        external_doc_id: &str,
    ) -> Result<DocId, RegistryError> {
        if let Some(doc_id) = self.docs_by_external.get(external_doc_id) {
            return Ok(*doc_id);
        }

        let doc_id = DocId::try_from(self.next_doc_id)
            .map_err(|_| RegistryError::DocOverflow { collection_id })?;
        self.next_doc_id += 1;

        self.docs_by_external
            .insert(external_doc_id.to_owned(), doc_id);
        self.external_by_doc
            .insert(doc_id, external_doc_id.to_owned());
        Ok(doc_id)
    }

    /// Resolve field ID by field path.
    #[must_use]
    pub fn field_id(&self, path: &str) -> Option<FieldId> {
        self.fields_by_path.get(path).copied()
    }

    /// Resolve field path by field ID.
    #[must_use]
    pub fn field_path(&self, field_id: FieldId) -> Option<&str> {
        self.paths_by_field.get(&field_id).map(String::as_str)
    }

    /// Resolve internal document ID by external document ID.
    #[must_use]
    pub fn doc_internal_id(&self, external_doc_id: &str) -> Option<DocId> {
        self.docs_by_external.get(external_doc_id).copied()
    }

    /// Resolve external document ID by internal document ID.
    #[must_use]
    pub fn doc_external_id(&self, doc_id: DocId) -> Option<&str> {
        self.external_by_doc.get(&doc_id).map(String::as_str)
    }

    /// Number of field IDs in this segment.
    #[must_use]
    pub fn field_count(&self) -> usize {
        self.fields_by_path.len()
    }

    /// Number of document IDs in this segment.
    #[must_use]
    pub fn doc_count(&self) -> usize {
        self.docs_by_external.len()
    }

    /// Return all field mappings as `(field_id, field_path)` sorted by field ID.
    #[must_use]
    pub fn field_mappings(&self) -> Vec<(FieldId, String)> {
        let mut mappings: Vec<(FieldId, String)> = self
            .paths_by_field
            .iter()
            .map(|(field_id, path)| (*field_id, path.clone()))
            .collect();
        mappings.sort_by_key(|(field_id, _)| *field_id);
        mappings
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collection_ids_are_stable() {
        let mut registry = IdRegistry::new();
        let a = registry
            .get_or_create_collection_id("users")
            .expect("collection id should be allocated");
        let b = registry
            .get_or_create_collection_id("users")
            .expect("collection id should be reused");
        let c = registry
            .get_or_create_collection_id("orders")
            .expect("collection id should be allocated");

        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_eq!(registry.collection_name(a), Some("users"));
        assert_eq!(registry.collection_name(c), Some("orders"));
    }

    #[test]
    fn field_and_doc_ids_are_stable() {
        let mut registry = IdRegistry::new();
        let collection_id = registry
            .get_or_create_collection_id("users")
            .expect("collection should be created");

        let city_a = registry
            .get_or_create_field_id(collection_id, "address.city")
            .expect("field id should be created");
        let city_b = registry
            .get_or_create_field_id(collection_id, "address.city")
            .expect("field id should be reused");
        let zip = registry
            .get_or_create_field_id(collection_id, "address.zip")
            .expect("field id should be created");

        assert_eq!(city_a, city_b);
        assert_ne!(city_a, zip);
        assert_eq!(
            registry.field_path(collection_id, city_a),
            Some("address.city")
        );

        let doc_a = registry
            .get_or_create_doc_internal_id(collection_id, "doc:1")
            .expect("doc id should be created");
        let doc_b = registry
            .get_or_create_doc_internal_id(collection_id, "doc:1")
            .expect("doc id should be reused");
        let doc_c = registry
            .get_or_create_doc_internal_id(collection_id, "doc:2")
            .expect("doc id should be created");

        assert_eq!(doc_a, doc_b);
        assert_ne!(doc_a, doc_c);
        assert_eq!(
            registry.doc_external_id(collection_id, doc_a),
            Some("doc:1")
        );
    }

    #[test]
    fn unknown_collection_returns_error() {
        let mut registry = IdRegistry::new();
        let err = registry
            .get_or_create_field_id(42, "city")
            .expect_err("unknown collection should return error");
        assert_eq!(err, RegistryError::UnknownCollection(42));
    }

    #[test]
    fn collection_can_be_removed() {
        let mut registry = IdRegistry::new();
        let collection_id = registry
            .get_or_create_collection_id("users")
            .expect("collection should be created");
        assert_eq!(registry.collection_count(), 1);

        let removed = registry.remove_collection("users");
        assert_eq!(removed, Some(collection_id));
        assert_eq!(registry.collection_count(), 0);
        assert_eq!(registry.collection_id("users"), None);
    }

    #[test]
    fn field_mappings_are_sorted_by_field_id() {
        let mut registry = IdRegistry::new();
        let collection_id = registry
            .get_or_create_collection_id("users")
            .expect("collection should be created");
        registry
            .get_or_create_field_id(collection_id, "zeta")
            .expect("field should be created");
        registry
            .get_or_create_field_id(collection_id, "alpha")
            .expect("field should be created");

        let segment = registry
            .segment(collection_id)
            .expect("segment should exist");
        let mappings = segment.field_mappings();

        assert_eq!(mappings.len(), 2);
        assert_eq!(mappings[0].0, 0);
        assert_eq!(mappings[1].0, 1);
    }
}
