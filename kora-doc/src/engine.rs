//! In-memory document engine for collection and CRUD operations.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;
use thiserror::Error;

use crate::collection::{Collection, CollectionConfig, CollectionError, CompressionProfile};
use crate::decompose::{DecomposeError, Decomposer};
use crate::dictionary::{ValueDictionary, ValueDictionaryConfig};
use crate::expr::{parse_where, Expr, ExprValue};
use crate::index::{
    hash32, intersect_sorted, union_sorted, CollectionIndexes, IndexConfig, IndexError, IndexType,
};
use crate::packed::PackedDoc;
use crate::recompose::{RecomposeError, Recomposer};
use crate::registry::{CollectionId, DocId, FieldId, IdRegistry, RegistryError};

/// Result of a successful `set` operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SetResult {
    /// Internal document ID assigned in the collection.
    pub internal_id: DocId,
    /// True if this write inserted a new document key.
    pub created: bool,
}

/// Mutation operation used by [`DocEngine::update`].
#[derive(Debug, Clone, PartialEq)]
pub enum DocMutation {
    /// Set a field path to a JSON value, creating missing intermediate objects.
    Set {
        /// Dotted field path (for example `address.city`).
        path: String,
        /// New JSON value for the path.
        value: Value,
    },
    /// Delete one field path when present.
    Del {
        /// Dotted field path (for example `tags`).
        path: String,
    },
    /// Increment an existing numeric field by `delta`.
    Incr {
        /// Dotted field path to increment.
        path: String,
        /// Increment amount.
        delta: f64,
    },
    /// Append one JSON value to an array field, creating the array when missing.
    Push {
        /// Dotted field path to an array.
        path: String,
        /// Value to append.
        value: Value,
    },
    /// Remove all array items that exactly match the supplied value.
    Pull {
        /// Dotted field path to an array.
        path: String,
        /// Value to remove.
        value: Value,
    },
}

/// Snapshot of collection metadata and current storage counters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionInfo {
    /// Collection ID.
    pub id: CollectionId,
    /// Collection name.
    pub name: String,
    /// Creation timestamp (seconds since UNIX epoch).
    pub created_at: u64,
    /// Compression profile.
    pub compression: CompressionProfile,
    /// Number of documents currently stored in this engine.
    pub doc_count: u64,
    /// Number of entries in the collection dictionary.
    pub dictionary_entries: usize,
}

/// Cardinality details for one collection field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DictionaryFieldInfo {
    /// Field ID assigned by the registry.
    pub field_id: u16,
    /// Dotted field path.
    pub path: String,
    /// Estimated unique value count observed for the field.
    pub cardinality_estimate: usize,
}

/// Snapshot of collection dictionary statistics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DictionaryInfo {
    /// Collection ID.
    pub collection_id: CollectionId,
    /// Collection name.
    pub collection_name: String,
    /// Number of unique dictionary values.
    pub dictionary_entries: usize,
    /// Per-field cardinality estimates.
    pub fields: Vec<DictionaryFieldInfo>,
}

/// Snapshot of collection storage footprint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageInfo {
    /// Collection ID.
    pub collection_id: CollectionId,
    /// Collection name.
    pub collection_name: String,
    /// Number of documents stored.
    pub doc_count: usize,
    /// Total packed bytes across all stored documents.
    pub total_packed_bytes: usize,
    /// Smallest packed document size in bytes.
    pub min_doc_bytes: usize,
    /// Largest packed document size in bytes.
    pub max_doc_bytes: usize,
    /// Average packed document size in bytes.
    pub avg_doc_bytes: usize,
}

/// Errors returned by `DocEngine`.
#[derive(Debug, Error)]
pub enum DocError {
    /// Collection management error.
    #[error(transparent)]
    Collection(#[from] CollectionError),
    /// Registry operation failed.
    #[error(transparent)]
    Registry(#[from] RegistryError),
    /// JSON decomposition failed.
    #[error(transparent)]
    Decompose(#[from] DecomposeError),
    /// Packed document reconstruction failed.
    #[error(transparent)]
    Recompose(#[from] RecomposeError),
    /// Referenced collection does not exist.
    #[error("unknown collection '{0}'")]
    UnknownCollection(String),
    /// Mutation payload or target path is invalid.
    #[error("invalid document mutation: {0}")]
    InvalidMutation(String),
    /// Index operation failed.
    #[error(transparent)]
    Index(#[from] IndexError),
    /// WHERE expression parse error.
    #[error("invalid WHERE expression: {0}")]
    InvalidExpression(String),
}

#[derive(Debug)]
#[allow(dead_code)]
struct CollectionState {
    collection: Collection,
    dictionary: ValueDictionary,
    docs_by_internal_id: HashMap<DocId, PackedDoc>,
    index_config: IndexConfig,
    indexes: CollectionIndexes,
}

/// Document engine with collection-local dictionaries and packed docs.
#[derive(Debug)]
pub struct DocEngine {
    registry: IdRegistry,
    collections: HashMap<CollectionId, CollectionState>,
    packed_version: u16,
}

impl DocEngine {
    /// Create a document engine with packed format version `1`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            registry: IdRegistry::new(),
            collections: HashMap::new(),
            packed_version: 1,
        }
    }

    /// Create a collection.
    pub fn create_collection(
        &mut self,
        name: &str,
        config: CollectionConfig,
    ) -> Result<CollectionId, DocError> {
        if self.registry.collection_id(name).is_some() {
            return Err(DocError::Collection(CollectionError::AlreadyExists(
                name.to_string(),
            )));
        }

        let collection_id = self.registry.get_or_create_collection_id(name)?;
        let state = CollectionState {
            collection: Collection::new(name.to_string(), collection_id, config),
            dictionary: ValueDictionary::new(ValueDictionaryConfig::default()),
            docs_by_internal_id: HashMap::new(),
            index_config: IndexConfig::new(),
            indexes: CollectionIndexes::new(),
        };
        self.collections.insert(collection_id, state);

        Ok(collection_id)
    }

    /// Drop a collection and all its documents.
    pub fn drop_collection(&mut self, name: &str) -> bool {
        if let Some(collection_id) = self.registry.remove_collection(name) {
            self.collections.remove(&collection_id);
            return true;
        }
        false
    }

    /// Return collection info when present.
    #[must_use]
    pub fn collection_info(&self, name: &str) -> Option<CollectionInfo> {
        let collection_id = self.registry.collection_id(name)?;
        let state = self.collections.get(&collection_id)?;
        Some(CollectionInfo {
            id: state.collection.id(),
            name: state.collection.name().to_string(),
            created_at: state.collection.created_at(),
            compression: state.collection.compression(),
            doc_count: state.collection.doc_count(),
            dictionary_entries: state.dictionary.len(),
        })
    }

    /// Return dictionary statistics for one collection.
    pub fn dictionary_info(&self, name: &str) -> Result<DictionaryInfo, DocError> {
        let collection_id = self.collection_id(name)?;
        let state = self
            .collections
            .get(&collection_id)
            .ok_or_else(|| DocError::UnknownCollection(name.to_string()))?;
        let segment = self
            .registry
            .segment(collection_id)
            .ok_or_else(|| DocError::UnknownCollection(name.to_string()))?;

        let fields = segment
            .field_mappings()
            .into_iter()
            .map(|(field_id, path)| DictionaryFieldInfo {
                field_id,
                cardinality_estimate: state.dictionary.cardinality_estimate(field_id),
                path,
            })
            .collect();

        Ok(DictionaryInfo {
            collection_id,
            collection_name: state.collection.name().to_string(),
            dictionary_entries: state.dictionary.len(),
            fields,
        })
    }

    /// Return packed storage statistics for one collection.
    pub fn storage_info(&self, name: &str) -> Result<StorageInfo, DocError> {
        let collection_id = self.collection_id(name)?;
        let state = self
            .collections
            .get(&collection_id)
            .ok_or_else(|| DocError::UnknownCollection(name.to_string()))?;

        let mut total_packed_bytes = 0usize;
        let mut min_doc_bytes = usize::MAX;
        let mut max_doc_bytes = 0usize;

        for packed in state.docs_by_internal_id.values() {
            let bytes = packed.byte_size();
            total_packed_bytes += bytes;
            min_doc_bytes = min_doc_bytes.min(bytes);
            max_doc_bytes = max_doc_bytes.max(bytes);
        }

        let doc_count = state.docs_by_internal_id.len();
        if doc_count == 0 {
            min_doc_bytes = 0;
        }
        let avg_doc_bytes = if doc_count == 0 {
            0
        } else {
            total_packed_bytes / doc_count
        };

        Ok(StorageInfo {
            collection_id,
            collection_name: state.collection.name().to_string(),
            doc_count,
            total_packed_bytes,
            min_doc_bytes,
            max_doc_bytes,
            avg_doc_bytes,
        })
    }

    /// Create a secondary index on a collection field.
    ///
    /// Backfills all existing documents. For `Unique` indexes, if a duplicate
    /// value is detected during backfill the index is rolled back and an error
    /// is returned.
    pub fn create_index(
        &mut self,
        collection: &str,
        field_path: &str,
        index_type: IndexType,
    ) -> Result<(), DocError> {
        let collection_id = self.collection_id(collection)?;
        let field_id = self
            .registry
            .get_or_create_field_id(collection_id, field_path)?;

        let state = self
            .collections
            .get_mut(&collection_id)
            .ok_or_else(|| DocError::UnknownCollection(collection.to_string()))?;

        state.index_config.add(field_id, index_type)?;

        if let Err(err) = Self::backfill_index(
            &self.registry,
            &state.dictionary,
            &state.docs_by_internal_id,
            &mut state.indexes,
            collection_id,
            field_id,
            field_path,
            index_type,
        ) {
            state.index_config.remove(field_id).ok();
            state.indexes.remove_field(field_id);
            return Err(err);
        }

        Ok(())
    }

    /// Remove a secondary index from a collection field.
    pub fn drop_index(&mut self, collection: &str, field_path: &str) -> Result<(), DocError> {
        let collection_id = self.collection_id(collection)?;
        let state = self
            .collections
            .get_mut(&collection_id)
            .ok_or_else(|| DocError::UnknownCollection(collection.to_string()))?;

        let segment = self
            .registry
            .segment(collection_id)
            .ok_or_else(|| DocError::UnknownCollection(collection.to_string()))?;

        let field_id = segment
            .field_id(field_path)
            .ok_or(IndexError::NotFound(0))?;

        state.index_config.remove(field_id)?;
        state.indexes.remove_field(field_id);

        Ok(())
    }

    /// Return all configured indexes for a collection.
    pub fn indexes(&self, collection: &str) -> Result<Vec<(String, IndexType)>, DocError> {
        let collection_id = self.collection_id(collection)?;
        let state = self
            .collections
            .get(&collection_id)
            .ok_or_else(|| DocError::UnknownCollection(collection.to_string()))?;

        let segment = self
            .registry
            .segment(collection_id)
            .ok_or_else(|| DocError::UnknownCollection(collection.to_string()))?;

        let mut result = Vec::new();
        for (&field_id, &idx_type) in state.index_config.entries() {
            if let Some(path) = segment.field_path(field_id) {
                result.push((path.to_string(), idx_type));
            }
        }
        result.sort_by(|(a, _), (b, _)| a.cmp(b));
        Ok(result)
    }

    /// Insert or replace one JSON document.
    pub fn set(
        &mut self,
        collection: &str,
        external_doc_id: &str,
        json: &Value,
    ) -> Result<SetResult, DocError> {
        let collection_id = self.collection_id(collection)?;
        let internal_id = self
            .registry
            .get_or_create_doc_internal_id(collection_id, external_doc_id)?;

        let state = self
            .collections
            .get_mut(&collection_id)
            .ok_or_else(|| DocError::UnknownCollection(collection.to_string()))?;

        let is_update = state.docs_by_internal_id.contains_key(&internal_id);

        Self::check_unique_constraints(
            &self.registry,
            &state.index_config,
            &state.indexes,
            collection_id,
            internal_id,
            json,
        )?;

        if is_update {
            if let Some(old_packed) = state.docs_by_internal_id.get(&internal_id) {
                if let Ok(old_json) = Recomposer::recompose(
                    old_packed,
                    &self.registry,
                    &state.dictionary,
                    collection_id,
                ) {
                    Self::remove_index_entries(
                        &self.registry,
                        &state.index_config,
                        &mut state.indexes,
                        collection_id,
                        internal_id,
                        &old_json,
                    );
                }
            }
        }

        let (registry, collections) = (&mut self.registry, &mut self.collections);
        let state = collections
            .get_mut(&collection_id)
            .ok_or_else(|| DocError::UnknownCollection(collection.to_string()))?;

        let mut decomposer = Decomposer::new(
            collection_id,
            registry,
            &mut state.dictionary,
            self.packed_version,
        );
        let packed = decomposer.decompose(json, current_unix_seconds_u32())?;

        let created = state
            .docs_by_internal_id
            .insert(internal_id, packed)
            .is_none();
        if created {
            state.collection.increment_doc_count();
        }

        Self::add_index_entries(
            &self.registry,
            &state.index_config,
            &mut state.indexes,
            collection_id,
            internal_id,
            json,
        );

        Ok(SetResult {
            internal_id,
            created,
        })
    }

    /// Get a full document or a projected subset of fields.
    pub fn get(
        &self,
        collection: &str,
        external_doc_id: &str,
        projection: Option<&[&str]>,
    ) -> Result<Option<Value>, DocError> {
        let collection_id = self.collection_id(collection)?;
        let Some(internal_id) = self
            .registry
            .segment(collection_id)
            .and_then(|segment| segment.doc_internal_id(external_doc_id))
        else {
            return Ok(None);
        };

        let Some(state) = self.collections.get(&collection_id) else {
            return Ok(None);
        };
        let Some(packed) = state.docs_by_internal_id.get(&internal_id) else {
            return Ok(None);
        };

        match projection {
            Some(paths) => {
                let field_ids = self.resolve_field_ids(collection_id, paths);
                let value = Recomposer::project(
                    packed,
                    &field_ids,
                    &self.registry,
                    &state.dictionary,
                    collection_id,
                )?;
                Ok(Some(value))
            }
            None => {
                let value = Recomposer::recompose(
                    packed,
                    &self.registry,
                    &state.dictionary,
                    collection_id,
                )?;
                Ok(Some(value))
            }
        }
    }

    /// Apply field-level mutations to an existing document.
    ///
    /// Returns `Ok(true)` when the document existed and was rewritten, `Ok(false)` when the
    /// target document does not exist.
    pub fn update(
        &mut self,
        collection: &str,
        external_doc_id: &str,
        mutations: &[DocMutation],
    ) -> Result<bool, DocError> {
        if mutations.is_empty() {
            return Err(DocError::InvalidMutation(
                "update requires at least one mutation".to_string(),
            ));
        }

        let Some(mut doc) = self.get(collection, external_doc_id, None)? else {
            return Ok(false);
        };

        for mutation in mutations {
            match mutation {
                DocMutation::Set { path, value } => {
                    set_path(&mut doc, path, value.clone())?;
                }
                DocMutation::Del { path } => {
                    del_path(&mut doc, path)?;
                }
                DocMutation::Incr { path, delta } => {
                    incr_path(&mut doc, path, *delta)?;
                }
                DocMutation::Push { path, value } => {
                    push_path(&mut doc, path, value.clone())?;
                }
                DocMutation::Pull { path, value } => {
                    pull_path(&mut doc, path, value)?;
                }
            }
        }

        self.set(collection, external_doc_id, &doc)?;
        Ok(true)
    }

    /// Delete a document by external ID.
    pub fn del(&mut self, collection: &str, external_doc_id: &str) -> Result<bool, DocError> {
        let collection_id = self.collection_id(collection)?;
        let Some(internal_id) = self
            .registry
            .segment(collection_id)
            .and_then(|segment| segment.doc_internal_id(external_doc_id))
        else {
            return Ok(false);
        };

        let Some(state) = self.collections.get_mut(&collection_id) else {
            return Ok(false);
        };

        if let Some(packed) = state.docs_by_internal_id.get(&internal_id) {
            if let Ok(old_json) =
                Recomposer::recompose(packed, &self.registry, &state.dictionary, collection_id)
            {
                Self::remove_index_entries(
                    &self.registry,
                    &state.index_config,
                    &mut state.indexes,
                    collection_id,
                    internal_id,
                    &old_json,
                );
            }
        }

        let removed = state.docs_by_internal_id.remove(&internal_id).is_some();
        if removed {
            state.collection.decrement_doc_count();
        }
        Ok(removed)
    }

    /// Check whether a document exists.
    pub fn exists(&self, collection: &str, external_doc_id: &str) -> Result<bool, DocError> {
        let collection_id = self.collection_id(collection)?;
        let Some(internal_id) = self
            .registry
            .segment(collection_id)
            .and_then(|segment| segment.doc_internal_id(external_doc_id))
        else {
            return Ok(false);
        };

        Ok(self
            .collections
            .get(&collection_id)
            .is_some_and(|state| state.docs_by_internal_id.contains_key(&internal_id)))
    }

    fn collection_id(&self, name: &str) -> Result<CollectionId, DocError> {
        self.registry
            .collection_id(name)
            .ok_or_else(|| DocError::UnknownCollection(name.to_string()))
    }

    fn resolve_field_ids(&self, collection_id: CollectionId, paths: &[&str]) -> Vec<u16> {
        let Some(segment) = self.registry.segment(collection_id) else {
            return Vec::new();
        };
        paths
            .iter()
            .filter_map(|path| segment.field_id(path))
            .collect()
    }

    #[allow(clippy::too_many_arguments)]
    fn backfill_index(
        registry: &IdRegistry,
        dictionary: &ValueDictionary,
        docs: &HashMap<DocId, PackedDoc>,
        indexes: &mut CollectionIndexes,
        collection_id: CollectionId,
        field_id: FieldId,
        field_path: &str,
        index_type: IndexType,
    ) -> Result<(), DocError> {
        for (&doc_id, packed) in docs {
            let json = Recomposer::recompose(packed, registry, dictionary, collection_id)?;
            if let Some(field_value) = resolve_json_path(&json, field_path) {
                add_single_field_entry(indexes, field_id, index_type, doc_id, field_value)?;
            }
        }
        Ok(())
    }

    fn check_unique_constraints(
        registry: &IdRegistry,
        index_config: &IndexConfig,
        indexes: &CollectionIndexes,
        collection_id: CollectionId,
        doc_id: DocId,
        json: &Value,
    ) -> Result<(), DocError> {
        let Some(segment) = registry.segment(collection_id) else {
            return Ok(());
        };

        for (&field_id, &idx_type) in index_config.entries() {
            if idx_type != IndexType::Unique {
                continue;
            }
            let Some(path) = segment.field_path(field_id) else {
                continue;
            };
            let Some(field_value) = resolve_json_path(json, path) else {
                continue;
            };
            let hashed = value_to_hash(field_value);
            let Some(hashed) = hashed else {
                continue;
            };
            if let Some(unique_idx) = indexes.unique(field_id) {
                if let Some(existing) = unique_idx.lookup(hashed) {
                    if existing != doc_id {
                        return Err(DocError::Index(IndexError::UniqueViolation {
                            hash: hashed,
                            existing_doc_id: existing,
                        }));
                    }
                }
            }
        }
        Ok(())
    }

    fn add_index_entries(
        registry: &IdRegistry,
        index_config: &IndexConfig,
        indexes: &mut CollectionIndexes,
        collection_id: CollectionId,
        doc_id: DocId,
        json: &Value,
    ) {
        let Some(segment) = registry.segment(collection_id) else {
            return;
        };

        for (&field_id, &idx_type) in index_config.entries() {
            let Some(path) = segment.field_path(field_id) else {
                continue;
            };
            let Some(field_value) = resolve_json_path(json, path) else {
                continue;
            };
            let _ = add_single_field_entry(indexes, field_id, idx_type, doc_id, field_value);
        }
    }

    fn remove_index_entries(
        registry: &IdRegistry,
        index_config: &IndexConfig,
        indexes: &mut CollectionIndexes,
        collection_id: CollectionId,
        doc_id: DocId,
        json: &Value,
    ) {
        let Some(segment) = registry.segment(collection_id) else {
            return;
        };

        for (&field_id, &idx_type) in index_config.entries() {
            let Some(path) = segment.field_path(field_id) else {
                continue;
            };
            let Some(field_value) = resolve_json_path(json, path) else {
                continue;
            };
            remove_single_field_entry(indexes, field_id, idx_type, doc_id, field_value);
        }
    }

    /// Execute a WHERE query and return matching documents.
    pub fn find(
        &self,
        collection: &str,
        where_clause: &str,
        projection: Option<&[&str]>,
        limit: Option<usize>,
        offset: usize,
    ) -> Result<Vec<Value>, DocError> {
        let collection_id = self.collection_id(collection)?;
        let state = self
            .collections
            .get(&collection_id)
            .ok_or_else(|| DocError::UnknownCollection(collection.to_string()))?;

        let expr = parse_where(where_clause)
            .map_err(|err| DocError::InvalidExpression(err.to_string()))?;

        let doc_ids = self.execute_expr(collection_id, state, &expr)?;

        let end = match limit {
            Some(lim) => (offset.saturating_add(lim)).min(doc_ids.len()),
            None => doc_ids.len(),
        };
        let start = offset.min(doc_ids.len());
        let page = &doc_ids[start..end];

        let mut results = Vec::with_capacity(page.len());
        for &doc_id in page {
            let Some(packed) = state.docs_by_internal_id.get(&doc_id) else {
                continue;
            };
            let value = match projection {
                Some(paths) => {
                    let field_ids = self.resolve_field_ids(collection_id, paths);
                    Recomposer::project(
                        packed,
                        &field_ids,
                        &self.registry,
                        &state.dictionary,
                        collection_id,
                    )?
                }
                None => {
                    Recomposer::recompose(packed, &self.registry, &state.dictionary, collection_id)?
                }
            };
            results.push(value);
        }

        Ok(results)
    }

    /// Count documents matching a WHERE clause.
    pub fn count(&self, collection: &str, where_clause: &str) -> Result<u64, DocError> {
        let collection_id = self.collection_id(collection)?;
        let state = self
            .collections
            .get(&collection_id)
            .ok_or_else(|| DocError::UnknownCollection(collection.to_string()))?;

        let expr = parse_where(where_clause)
            .map_err(|err| DocError::InvalidExpression(err.to_string()))?;

        let doc_ids = self.execute_expr(collection_id, state, &expr)?;
        Ok(doc_ids.len() as u64)
    }

    fn execute_expr(
        &self,
        collection_id: CollectionId,
        state: &CollectionState,
        expr: &Expr,
    ) -> Result<Vec<DocId>, DocError> {
        match expr {
            Expr::And(left, right) => {
                let left_ids = self.execute_expr(collection_id, state, left)?;
                let right_ids = self.execute_expr(collection_id, state, right)?;
                Ok(intersect_sorted(&left_ids, &right_ids))
            }
            Expr::Or(left, right) => {
                let left_ids = self.execute_expr(collection_id, state, left)?;
                let right_ids = self.execute_expr(collection_id, state, right)?;
                Ok(union_sorted(&left_ids, &right_ids))
            }
            _ => self.execute_leaf(collection_id, state, expr),
        }
    }

    fn execute_leaf(
        &self,
        collection_id: CollectionId,
        state: &CollectionState,
        expr: &Expr,
    ) -> Result<Vec<DocId>, DocError> {
        let field_path = expr_field(expr);
        let segment = self.registry.segment(collection_id);
        let field_id = segment.and_then(|seg| seg.field_id(field_path));
        let index_type = field_id.and_then(|fid| state.index_config.lookup(fid));

        match (expr, index_type, field_id) {
            (Expr::Eq(_, ExprValue::Null), _, _) => Ok(Vec::new()),

            (Expr::Eq(_, value), Some(IndexType::Hash), Some(fid)) => {
                let Some(hashed) = expr_value_to_hash(value) else {
                    return Ok(Vec::new());
                };
                Ok(state
                    .indexes
                    .hash(fid)
                    .map_or_else(Vec::new, |idx| idx.lookup(hashed).to_vec()))
            }

            (Expr::Eq(_, ExprValue::Number(n)), Some(IndexType::Sorted), Some(fid)) => Ok(state
                .indexes
                .sorted(fid)
                .map_or_else(Vec::new, |idx| idx.range_query(*n, *n))),

            (Expr::Gte(_, n), Some(IndexType::Sorted), Some(fid)) => Ok(state
                .indexes
                .sorted(fid)
                .map_or_else(Vec::new, |idx| idx.range_query(*n, f64::MAX))),

            (Expr::Lte(_, n), Some(IndexType::Sorted), Some(fid)) => Ok(state
                .indexes
                .sorted(fid)
                .map_or_else(Vec::new, |idx| idx.range_query(f64::MIN, *n))),

            (Expr::Gt(_, n), Some(IndexType::Sorted), Some(fid)) => {
                let candidates = state
                    .indexes
                    .sorted(fid)
                    .map_or_else(Vec::new, |idx| idx.range_query(*n, f64::MAX));
                self.filter_numeric_boundary(
                    collection_id,
                    state,
                    field_path,
                    candidates,
                    *n,
                    |v, boundary| v > boundary,
                )
            }

            (Expr::Lt(_, n), Some(IndexType::Sorted), Some(fid)) => {
                let candidates = state
                    .indexes
                    .sorted(fid)
                    .map_or_else(Vec::new, |idx| idx.range_query(f64::MIN, *n));
                self.filter_numeric_boundary(
                    collection_id,
                    state,
                    field_path,
                    candidates,
                    *n,
                    |v, boundary| v < boundary,
                )
            }

            (Expr::Contains(_, value), Some(IndexType::Array), Some(fid)) => {
                let Some(hashed) = expr_value_to_hash(value) else {
                    return Ok(Vec::new());
                };
                Ok(state
                    .indexes
                    .array(fid)
                    .map_or_else(Vec::new, |idx| idx.lookup(hashed).to_vec()))
            }

            _ => self.fallback_scan(collection_id, state, expr),
        }
    }

    fn filter_numeric_boundary(
        &self,
        collection_id: CollectionId,
        state: &CollectionState,
        field_path: &str,
        candidates: Vec<DocId>,
        boundary: f64,
        cmp: fn(f64, f64) -> bool,
    ) -> Result<Vec<DocId>, DocError> {
        let mut result = Vec::with_capacity(candidates.len());
        for doc_id in candidates {
            let Some(packed) = state.docs_by_internal_id.get(&doc_id) else {
                continue;
            };
            let json =
                Recomposer::recompose(packed, &self.registry, &state.dictionary, collection_id)?;
            if let Some(field_val) = resolve_json_path(&json, field_path) {
                if let Some(num) = field_val.as_f64() {
                    if cmp(num, boundary) {
                        result.push(doc_id);
                    }
                }
            }
        }
        Ok(result)
    }

    fn fallback_scan(
        &self,
        collection_id: CollectionId,
        state: &CollectionState,
        expr: &Expr,
    ) -> Result<Vec<DocId>, DocError> {
        let mut result = Vec::new();
        let mut doc_ids: Vec<DocId> = state.docs_by_internal_id.keys().copied().collect();
        doc_ids.sort_unstable();

        for doc_id in doc_ids {
            let Some(packed) = state.docs_by_internal_id.get(&doc_id) else {
                continue;
            };
            let json =
                Recomposer::recompose(packed, &self.registry, &state.dictionary, collection_id)?;
            if eval_expr_on_json(&json, expr) {
                result.push(doc_id);
            }
        }

        Ok(result)
    }
}

impl Default for DocEngine {
    fn default() -> Self {
        Self::new()
    }
}

fn current_unix_seconds_u32() -> u32 {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs());
    u32::try_from(seconds).unwrap_or(u32::MAX)
}

fn resolve_json_path<'a>(root: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = root;
    for part in path.split('.') {
        current = current.as_object()?.get(part)?;
    }
    Some(current)
}

fn value_to_hash(value: &Value) -> Option<u32> {
    match value {
        Value::String(s) => Some(hash32(s.as_bytes())),
        Value::Bool(true) => Some(hash32(b"true")),
        Value::Bool(false) => Some(hash32(b"false")),
        _ => None,
    }
}

fn value_to_score(value: &Value) -> Option<f64> {
    value.as_f64()
}

fn add_single_field_entry(
    indexes: &mut CollectionIndexes,
    field_id: FieldId,
    index_type: IndexType,
    doc_id: DocId,
    value: &Value,
) -> Result<(), DocError> {
    if value.is_null() {
        return Ok(());
    }

    match index_type {
        IndexType::Hash => {
            if let Some(hashed) = value_to_hash(value) {
                indexes.get_or_create_hash(field_id).add(hashed, doc_id);
            }
        }
        IndexType::Sorted => {
            if let Some(score) = value_to_score(value) {
                indexes.get_or_create_sorted(field_id).add(score, doc_id);
            }
        }
        IndexType::Array => {
            if let Value::Array(items) = value {
                let array_idx = indexes.get_or_create_array(field_id);
                for item in items {
                    if let Value::String(s) = item {
                        array_idx.add(hash32(s.as_bytes()), doc_id);
                    }
                }
            }
        }
        IndexType::Unique => {
            if let Some(hashed) = value_to_hash(value) {
                indexes.get_or_create_unique(field_id).add(hashed, doc_id)?;
            }
        }
    }
    Ok(())
}

fn remove_single_field_entry(
    indexes: &mut CollectionIndexes,
    field_id: FieldId,
    index_type: IndexType,
    doc_id: DocId,
    value: &Value,
) {
    if value.is_null() {
        return;
    }

    match index_type {
        IndexType::Hash => {
            if let Some(hashed) = value_to_hash(value) {
                indexes.get_or_create_hash(field_id).remove(hashed, doc_id);
            }
        }
        IndexType::Sorted => {
            if let Some(score) = value_to_score(value) {
                indexes.get_or_create_sorted(field_id).remove(score, doc_id);
            }
        }
        IndexType::Array => {
            if let Value::Array(items) = value {
                let array_idx = indexes.get_or_create_array(field_id);
                for item in items {
                    if let Value::String(s) = item {
                        array_idx.remove(hash32(s.as_bytes()), doc_id);
                    }
                }
            }
        }
        IndexType::Unique => {
            if let Some(hashed) = value_to_hash(value) {
                indexes.get_or_create_unique(field_id).remove(hashed);
            }
        }
    }
}

fn expr_field(expr: &Expr) -> &str {
    match expr {
        Expr::Eq(f, _)
        | Expr::Neq(f, _)
        | Expr::Gt(f, _)
        | Expr::Gte(f, _)
        | Expr::Lt(f, _)
        | Expr::Lte(f, _)
        | Expr::Contains(f, _) => f.as_str(),
        Expr::And(_, _) | Expr::Or(_, _) => "",
    }
}

fn expr_value_to_hash(value: &ExprValue) -> Option<u32> {
    match value {
        ExprValue::String(s) => Some(hash32(s.as_bytes())),
        ExprValue::Bool(true) => Some(hash32(b"true")),
        ExprValue::Bool(false) => Some(hash32(b"false")),
        ExprValue::Number(_) | ExprValue::Null => None,
    }
}

fn eval_expr_on_json(doc: &Value, expr: &Expr) -> bool {
    match expr {
        Expr::Eq(path, value) => {
            let Some(field_val) = resolve_json_path(doc, path) else {
                return false;
            };
            json_matches_expr_value(field_val, value)
        }
        Expr::Neq(path, value) => {
            let Some(field_val) = resolve_json_path(doc, path) else {
                return true;
            };
            !json_matches_expr_value(field_val, value)
        }
        Expr::Gt(path, n) => resolve_json_path(doc, path)
            .and_then(|v| v.as_f64())
            .is_some_and(|v| v > *n),
        Expr::Gte(path, n) => resolve_json_path(doc, path)
            .and_then(|v| v.as_f64())
            .is_some_and(|v| v >= *n),
        Expr::Lt(path, n) => resolve_json_path(doc, path)
            .and_then(|v| v.as_f64())
            .is_some_and(|v| v < *n),
        Expr::Lte(path, n) => resolve_json_path(doc, path)
            .and_then(|v| v.as_f64())
            .is_some_and(|v| v <= *n),
        Expr::Contains(path, value) => {
            let Some(Value::Array(items)) = resolve_json_path(doc, path) else {
                return false;
            };
            items
                .iter()
                .any(|item| json_matches_expr_value(item, value))
        }
        Expr::And(left, right) => eval_expr_on_json(doc, left) && eval_expr_on_json(doc, right),
        Expr::Or(left, right) => eval_expr_on_json(doc, left) || eval_expr_on_json(doc, right),
    }
}

fn json_matches_expr_value(json_val: &Value, expr_val: &ExprValue) -> bool {
    match (json_val, expr_val) {
        (Value::String(a), ExprValue::String(b)) => a == b,
        (Value::Number(a), ExprValue::Number(b)) => a.as_f64().is_some_and(|v| v == *b),
        (Value::Bool(a), ExprValue::Bool(b)) => a == b,
        (Value::Null, ExprValue::Null) => true,
        _ => false,
    }
}

fn set_path(root: &mut Value, path: &str, value: Value) -> Result<(), DocError> {
    let parts = parse_path(path)?;
    let leaf = parts[parts.len() - 1];
    let Some(parent) = resolve_parent_object_mut(root, &parts, path, true)? else {
        return Err(DocError::InvalidMutation(format!(
            "SET path '{path}' is invalid"
        )));
    };
    parent.insert(leaf.to_string(), value);
    Ok(())
}

fn del_path(root: &mut Value, path: &str) -> Result<(), DocError> {
    let parts = parse_path(path)?;
    let leaf = parts[parts.len() - 1];
    let Some(parent) = resolve_parent_object_mut(root, &parts, path, false)? else {
        return Ok(());
    };
    parent.remove(leaf);
    Ok(())
}

fn incr_path(root: &mut Value, path: &str, delta: f64) -> Result<(), DocError> {
    if !delta.is_finite() {
        return Err(DocError::InvalidMutation(format!(
            "INCR delta for path '{path}' must be finite"
        )));
    }

    let parts = parse_path(path)?;
    let Some(target) = resolve_existing_path_mut(root, &parts, path)? else {
        return Err(DocError::InvalidMutation(format!(
            "INCR path '{path}' does not exist"
        )));
    };

    let Value::Number(number) = target else {
        return Err(DocError::InvalidMutation(format!(
            "INCR path '{path}' targets a non-numeric value"
        )));
    };

    let Some(base) = number.as_f64() else {
        return Err(DocError::InvalidMutation(format!(
            "INCR path '{path}' contains an unsupported number representation"
        )));
    };
    let updated = base + delta;
    if !updated.is_finite() {
        return Err(DocError::InvalidMutation(format!(
            "INCR path '{path}' overflowed to a non-finite value"
        )));
    }

    *target = if updated.fract() == 0.0 && updated >= i64::MIN as f64 && updated <= i64::MAX as f64
    {
        Value::Number((updated as i64).into())
    } else {
        let Some(number) = serde_json::Number::from_f64(updated) else {
            return Err(DocError::InvalidMutation(format!(
                "INCR path '{path}' produced an invalid float value"
            )));
        };
        Value::Number(number)
    };

    Ok(())
}

fn push_path(root: &mut Value, path: &str, value: Value) -> Result<(), DocError> {
    let parts = parse_path(path)?;
    if let Some(target) = resolve_existing_path_mut(root, &parts, path)? {
        let Value::Array(items) = target else {
            return Err(DocError::InvalidMutation(format!(
                "PUSH path '{path}' targets a non-array value"
            )));
        };
        items.push(value);
        return Ok(());
    }

    set_path(root, path, Value::Array(vec![value]))
}

fn pull_path(root: &mut Value, path: &str, value: &Value) -> Result<(), DocError> {
    let parts = parse_path(path)?;
    let Some(target) = resolve_existing_path_mut(root, &parts, path)? else {
        return Ok(());
    };
    let Value::Array(items) = target else {
        return Err(DocError::InvalidMutation(format!(
            "PULL path '{path}' targets a non-array value"
        )));
    };
    items.retain(|candidate| candidate != value);
    Ok(())
}

fn parse_path(path: &str) -> Result<Vec<&str>, DocError> {
    if path.is_empty() {
        return Err(DocError::InvalidMutation(
            "path cannot be empty".to_string(),
        ));
    }
    let parts: Vec<&str> = path.split('.').collect();
    if parts.iter().any(|part| part.is_empty()) {
        return Err(DocError::InvalidMutation(format!(
            "path '{path}' contains an empty segment"
        )));
    }
    Ok(parts)
}

fn resolve_parent_object_mut<'a>(
    root: &'a mut Value,
    parts: &[&str],
    full_path: &str,
    create_missing: bool,
) -> Result<Option<&'a mut serde_json::Map<String, Value>>, DocError> {
    let mut current = root;
    if !current.is_object() {
        return Err(DocError::InvalidMutation(
            "document root must be a JSON object".to_string(),
        ));
    }

    for part in &parts[..parts.len() - 1] {
        let map = current.as_object_mut().ok_or_else(|| {
            DocError::InvalidMutation(format!(
                "path '{full_path}' traverses through a non-object segment"
            ))
        })?;

        if create_missing {
            current = map
                .entry((*part).to_string())
                .or_insert_with(|| Value::Object(serde_json::Map::new()));
            if !current.is_object() {
                return Err(DocError::InvalidMutation(format!(
                    "path '{full_path}' traverses through a non-object segment"
                )));
            }
            continue;
        }

        let Some(next) = map.get_mut(*part) else {
            return Ok(None);
        };
        if !next.is_object() {
            return Err(DocError::InvalidMutation(format!(
                "path '{full_path}' traverses through a non-object segment"
            )));
        }
        current = next;
    }

    let map = current.as_object_mut().ok_or_else(|| {
        DocError::InvalidMutation(format!(
            "path '{full_path}' traverses through a non-object segment"
        ))
    })?;
    Ok(Some(map))
}

fn resolve_existing_path_mut<'a>(
    root: &'a mut Value,
    parts: &[&str],
    full_path: &str,
) -> Result<Option<&'a mut Value>, DocError> {
    let mut current = root;
    for part in parts {
        let map = current.as_object_mut().ok_or_else(|| {
            DocError::InvalidMutation(format!(
                "path '{full_path}' traverses through a non-object segment"
            ))
        })?;
        let Some(next) = map.get_mut(*part) else {
            return Ok(None);
        };
        current = next;
    }
    Ok(Some(current))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn set_get_projection_delete_flow() {
        let mut engine = DocEngine::new();
        engine
            .create_collection("users", CollectionConfig::default())
            .expect("collection create should work");

        let set = engine
            .set(
                "users",
                "doc:1",
                &json!({
                    "name": "Augustus",
                    "age": 30,
                    "active": true,
                    "address": {"city": "Accra", "zip": "00233"},
                    "tags": ["rust", "systems"]
                }),
            )
            .expect("set should work");
        assert!(set.created);
        assert!(engine.exists("users", "doc:1").expect("exists should work"));

        let full = engine
            .get("users", "doc:1", None)
            .expect("get should work")
            .expect("doc should exist");
        assert_eq!(
            full,
            json!({
                "name": "Augustus",
                "age": 30,
                "active": true,
                "address": {"city": "Accra", "zip": "00233"},
                "tags": ["rust", "systems"]
            })
        );

        let projected = engine
            .get("users", "doc:1", Some(&["name", "address.city"]))
            .expect("projection should work")
            .expect("doc should exist");
        assert_eq!(
            projected,
            json!({"name": "Augustus", "address": {"city": "Accra"}})
        );

        assert!(engine.del("users", "doc:1").expect("delete should work"));
        assert!(!engine.exists("users", "doc:1").expect("exists should work"));
        assert_eq!(
            engine.get("users", "doc:1", None).expect("get should work"),
            None
        );
    }

    #[test]
    fn duplicate_collection_name_is_rejected() {
        let mut engine = DocEngine::new();
        engine
            .create_collection("users", CollectionConfig::default())
            .expect("create should work");
        let err = engine
            .create_collection("users", CollectionConfig::default())
            .expect_err("duplicate should fail");
        assert!(matches!(
            err,
            DocError::Collection(CollectionError::AlreadyExists(_))
        ));
    }

    #[test]
    fn get_missing_document_returns_none() {
        let mut engine = DocEngine::new();
        engine
            .create_collection("users", CollectionConfig::default())
            .expect("create should work");
        let doc = engine
            .get("users", "doc:missing", None)
            .expect("get should work");
        assert_eq!(doc, None);
    }

    #[test]
    fn collection_info_reflects_state() {
        let mut engine = DocEngine::new();
        engine
            .create_collection(
                "users",
                CollectionConfig {
                    compression: CompressionProfile::Dictionary,
                },
            )
            .expect("create should work");
        engine
            .set("users", "doc:1", &json!({"city": "Accra"}))
            .expect("set should work");
        engine
            .set("users", "doc:2", &json!({"city": "Accra"}))
            .expect("set should work");

        let info = engine
            .collection_info("users")
            .expect("collection should exist");
        assert_eq!(info.compression, CompressionProfile::Dictionary);
        assert_eq!(info.doc_count, 2);
        assert_eq!(info.dictionary_entries, 1);
    }

    #[test]
    fn unknown_collection_returns_error() {
        let engine = DocEngine::new();
        let err = engine
            .exists("users", "doc:1")
            .expect_err("unknown collection should fail");
        assert!(matches!(err, DocError::UnknownCollection(name) if name == "users"));
    }

    #[test]
    fn dictionary_info_reports_field_cardinality() {
        let mut engine = DocEngine::new();
        engine
            .create_collection("users", CollectionConfig::default())
            .expect("create should work");
        engine
            .set(
                "users",
                "doc:1",
                &json!({"city": "Accra", "status": "active"}),
            )
            .expect("set should work");
        engine
            .set(
                "users",
                "doc:2",
                &json!({"city": "Accra", "status": "inactive"}),
            )
            .expect("set should work");

        let info = engine
            .dictionary_info("users")
            .expect("dictionary info should work");
        assert_eq!(info.collection_name, "users");
        assert!(info.dictionary_entries >= 2);

        let city = info
            .fields
            .iter()
            .find(|field| field.path == "city")
            .expect("city field should be present");
        assert_eq!(city.cardinality_estimate, 1);

        let status = info
            .fields
            .iter()
            .find(|field| field.path == "status")
            .expect("status field should be present");
        assert_eq!(status.cardinality_estimate, 2);
    }

    #[test]
    fn storage_info_reports_packed_sizes() {
        let mut engine = DocEngine::new();
        engine
            .create_collection("users", CollectionConfig::default())
            .expect("create should work");
        engine
            .set("users", "doc:1", &json!({"name": "A"}))
            .expect("set should work");
        engine
            .set(
                "users",
                "doc:2",
                &json!({"name": "Augustus", "city": "Accra"}),
            )
            .expect("set should work");

        let info = engine
            .storage_info("users")
            .expect("storage info should work");
        assert_eq!(info.collection_name, "users");
        assert_eq!(info.doc_count, 2);
        assert!(info.total_packed_bytes > 0);
        assert!(info.max_doc_bytes >= info.min_doc_bytes);
        assert!(info.avg_doc_bytes >= info.min_doc_bytes);
        assert!(info.avg_doc_bytes <= info.max_doc_bytes);
    }

    #[test]
    fn update_applies_mutations() {
        let mut engine = DocEngine::new();
        engine
            .create_collection("users", CollectionConfig::default())
            .expect("create should work");
        engine
            .set(
                "users",
                "doc:1",
                &json!({
                    "name": "Augustus",
                    "score": 10,
                    "active": true,
                    "address": {"city": "Accra"},
                    "tags": ["rust", "systems", "rust"]
                }),
            )
            .expect("set should work");

        let updated = engine
            .update(
                "users",
                "doc:1",
                &[
                    DocMutation::Set {
                        path: "address.city".to_string(),
                        value: json!("London"),
                    },
                    DocMutation::Incr {
                        path: "score".to_string(),
                        delta: 2.5,
                    },
                    DocMutation::Push {
                        path: "tags".to_string(),
                        value: json!("cache"),
                    },
                    DocMutation::Pull {
                        path: "tags".to_string(),
                        value: json!("rust"),
                    },
                    DocMutation::Del {
                        path: "active".to_string(),
                    },
                ],
            )
            .expect("update should work");
        assert!(updated);

        let doc = engine
            .get("users", "doc:1", None)
            .expect("get should work")
            .expect("doc should exist");
        assert_eq!(
            doc,
            json!({
                "name": "Augustus",
                "score": 12.5,
                "address": {"city": "London"},
                "tags": ["systems", "cache"]
            })
        );
    }

    #[test]
    fn update_missing_document_returns_false() {
        let mut engine = DocEngine::new();
        engine
            .create_collection("users", CollectionConfig::default())
            .expect("create should work");
        let updated = engine
            .update(
                "users",
                "doc:missing",
                &[DocMutation::Set {
                    path: "name".to_string(),
                    value: json!("A"),
                }],
            )
            .expect("update should not fail");
        assert!(!updated);
    }

    #[test]
    fn update_rejects_non_numeric_incr_target() {
        let mut engine = DocEngine::new();
        engine
            .create_collection("users", CollectionConfig::default())
            .expect("create should work");
        engine
            .set("users", "doc:1", &json!({"score": "high"}))
            .expect("set should work");

        let err = engine
            .update(
                "users",
                "doc:1",
                &[DocMutation::Incr {
                    path: "score".to_string(),
                    delta: 1.0,
                }],
            )
            .expect_err("non-numeric increment must fail");
        assert!(matches!(err, DocError::InvalidMutation(_)));
    }

    #[test]
    fn create_index_backfills_existing_docs() {
        let mut engine = DocEngine::new();
        engine
            .create_collection("users", CollectionConfig::default())
            .expect("create should work");

        engine
            .set("users", "doc:1", &json!({"city": "Accra"}))
            .expect("set should work");
        engine
            .set("users", "doc:2", &json!({"city": "London"}))
            .expect("set should work");
        engine
            .set("users", "doc:3", &json!({"city": "Accra"}))
            .expect("set should work");

        engine
            .create_index("users", "city", IndexType::Hash)
            .expect("create_index should work");

        let collection_id = engine.collection_id("users").unwrap();
        let state = engine.collections.get(&collection_id).unwrap();
        let field_id = engine
            .registry
            .segment(collection_id)
            .unwrap()
            .field_id("city")
            .unwrap();

        let hash_idx = state
            .indexes
            .hash(field_id)
            .expect("hash index should exist");
        let accra_hash = hash32(b"Accra");
        let london_hash = hash32(b"London");
        let accra_docs = hash_idx.lookup(accra_hash);
        let london_docs = hash_idx.lookup(london_hash);

        assert_eq!(accra_docs.len(), 2);
        assert_eq!(london_docs.len(), 1);
    }

    #[test]
    fn index_maintained_on_set() {
        let mut engine = DocEngine::new();
        engine
            .create_collection("users", CollectionConfig::default())
            .expect("create should work");

        engine
            .create_index("users", "city", IndexType::Hash)
            .expect("create_index should work");

        engine
            .set("users", "doc:1", &json!({"city": "Accra"}))
            .expect("set should work");
        engine
            .set("users", "doc:2", &json!({"city": "London"}))
            .expect("set should work");

        let collection_id = engine.collection_id("users").unwrap();
        let state = engine.collections.get(&collection_id).unwrap();
        let field_id = engine
            .registry
            .segment(collection_id)
            .unwrap()
            .field_id("city")
            .unwrap();

        let hash_idx = state
            .indexes
            .hash(field_id)
            .expect("hash index should exist");
        assert_eq!(hash_idx.lookup(hash32(b"Accra")).len(), 1);
        assert_eq!(hash_idx.lookup(hash32(b"London")).len(), 1);
    }

    #[test]
    fn index_maintained_on_update() {
        let mut engine = DocEngine::new();
        engine
            .create_collection("users", CollectionConfig::default())
            .expect("create should work");

        engine
            .set("users", "doc:1", &json!({"city": "Accra"}))
            .expect("set should work");

        engine
            .create_index("users", "city", IndexType::Hash)
            .expect("create_index should work");

        engine
            .update(
                "users",
                "doc:1",
                &[DocMutation::Set {
                    path: "city".to_string(),
                    value: json!("London"),
                }],
            )
            .expect("update should work");

        let collection_id = engine.collection_id("users").unwrap();
        let state = engine.collections.get(&collection_id).unwrap();
        let field_id = engine
            .registry
            .segment(collection_id)
            .unwrap()
            .field_id("city")
            .unwrap();

        let hash_idx = state
            .indexes
            .hash(field_id)
            .expect("hash index should exist");
        assert!(hash_idx.lookup(hash32(b"Accra")).is_empty());
        assert_eq!(hash_idx.lookup(hash32(b"London")).len(), 1);
    }

    #[test]
    fn index_maintained_on_delete() {
        let mut engine = DocEngine::new();
        engine
            .create_collection("users", CollectionConfig::default())
            .expect("create should work");

        engine
            .set("users", "doc:1", &json!({"city": "Accra"}))
            .expect("set should work");

        engine
            .create_index("users", "city", IndexType::Hash)
            .expect("create_index should work");

        let collection_id = engine.collection_id("users").unwrap();
        let field_id = engine
            .registry
            .segment(collection_id)
            .unwrap()
            .field_id("city")
            .unwrap();

        {
            let state = engine.collections.get(&collection_id).unwrap();
            let hash_idx = state
                .indexes
                .hash(field_id)
                .expect("hash index should exist");
            assert_eq!(hash_idx.lookup(hash32(b"Accra")).len(), 1);
        }

        engine.del("users", "doc:1").expect("del should work");

        let state = engine.collections.get(&collection_id).unwrap();
        let hash_idx = state
            .indexes
            .hash(field_id)
            .expect("hash index should exist");
        assert!(hash_idx.lookup(hash32(b"Accra")).is_empty());
    }

    #[test]
    fn unique_constraint_violation_on_set() {
        let mut engine = DocEngine::new();
        engine
            .create_collection("users", CollectionConfig::default())
            .expect("create should work");

        engine
            .create_index("users", "email", IndexType::Unique)
            .expect("create_index should work");

        engine
            .set("users", "doc:1", &json!({"email": "alice@example.com"}))
            .expect("first set should work");

        let err = engine
            .set("users", "doc:2", &json!({"email": "alice@example.com"}))
            .expect_err("duplicate unique value must fail");

        assert!(matches!(
            err,
            DocError::Index(IndexError::UniqueViolation { .. })
        ));
    }

    #[test]
    fn drop_index_clears_data() {
        let mut engine = DocEngine::new();
        engine
            .create_collection("users", CollectionConfig::default())
            .expect("create should work");

        engine
            .create_index("users", "city", IndexType::Hash)
            .expect("create_index should work");

        engine
            .set("users", "doc:1", &json!({"city": "Accra"}))
            .expect("set should work");
        engine
            .set("users", "doc:2", &json!({"city": "London"}))
            .expect("set should work");

        engine
            .drop_index("users", "city")
            .expect("drop_index should work");

        let indexes = engine.indexes("users").expect("indexes should work");
        assert!(indexes.is_empty());

        let collection_id = engine.collection_id("users").unwrap();
        let field_id = engine
            .registry
            .segment(collection_id)
            .unwrap()
            .field_id("city")
            .unwrap();
        let state = engine.collections.get(&collection_id).unwrap();
        assert!(state.indexes.hash(field_id).is_none());
    }

    #[test]
    fn sorted_index_range_query_works() {
        let mut engine = DocEngine::new();
        engine
            .create_collection("products", CollectionConfig::default())
            .expect("create should work");

        engine
            .create_index("products", "price", IndexType::Sorted)
            .expect("create_index should work");

        engine
            .set("products", "p1", &json!({"price": 10.0}))
            .expect("set should work");
        engine
            .set("products", "p2", &json!({"price": 25.0}))
            .expect("set should work");
        engine
            .set("products", "p3", &json!({"price": 50.0}))
            .expect("set should work");
        engine
            .set("products", "p4", &json!({"price": 5.0}))
            .expect("set should work");

        let collection_id = engine.collection_id("products").unwrap();
        let field_id = engine
            .registry
            .segment(collection_id)
            .unwrap()
            .field_id("price")
            .unwrap();
        let state = engine.collections.get(&collection_id).unwrap();
        let sorted_idx = state
            .indexes
            .sorted(field_id)
            .expect("sorted index should exist");

        let range_10_30 = sorted_idx.range_query(10.0, 30.0);
        assert_eq!(range_10_30.len(), 2);

        let range_all = sorted_idx.range_query(0.0, 100.0);
        assert_eq!(range_all.len(), 4);

        let range_high = sorted_idx.range_query(40.0, 100.0);
        assert_eq!(range_high.len(), 1);
    }

    #[test]
    fn find_by_hash_index() {
        let mut engine = DocEngine::new();
        engine
            .create_collection("users", CollectionConfig::default())
            .expect("create should work");
        engine
            .create_index("users", "city", IndexType::Hash)
            .expect("index should work");

        engine
            .set("users", "d1", &json!({"name": "Kwame", "city": "Accra"}))
            .expect("set");
        engine
            .set("users", "d2", &json!({"name": "Ama", "city": "Kumasi"}))
            .expect("set");
        engine
            .set("users", "d3", &json!({"name": "Kofi", "city": "Accra"}))
            .expect("set");

        let results = engine
            .find("users", r#"city = "Accra""#, None, None, 0)
            .expect("find should work");
        assert_eq!(results.len(), 2);
        for doc in &results {
            assert_eq!(doc["city"], "Accra");
        }
    }

    #[test]
    fn find_by_sorted_index_range() {
        let mut engine = DocEngine::new();
        engine
            .create_collection("users", CollectionConfig::default())
            .expect("create");
        engine
            .create_index("users", "age", IndexType::Sorted)
            .expect("index");

        engine
            .set("users", "d1", &json!({"name": "A", "age": 20}))
            .expect("set");
        engine
            .set("users", "d2", &json!({"name": "B", "age": 25}))
            .expect("set");
        engine
            .set("users", "d3", &json!({"name": "C", "age": 30}))
            .expect("set");
        engine
            .set("users", "d4", &json!({"name": "D", "age": 35}))
            .expect("set");
        engine
            .set("users", "d5", &json!({"name": "E", "age": 40}))
            .expect("set");

        let results = engine
            .find("users", "age >= 25 AND age <= 35", None, None, 0)
            .expect("find should work");
        assert_eq!(results.len(), 3);
        for doc in &results {
            let age = doc["age"].as_f64().unwrap();
            assert!((25.0..=35.0).contains(&age));
        }
    }

    #[test]
    fn find_by_array_index() {
        let mut engine = DocEngine::new();
        engine
            .create_collection("posts", CollectionConfig::default())
            .expect("create");
        engine
            .create_index("posts", "tags", IndexType::Array)
            .expect("index");

        engine
            .set(
                "posts",
                "p1",
                &json!({"title": "A", "tags": ["rust", "systems"]}),
            )
            .expect("set");
        engine
            .set("posts", "p2", &json!({"title": "B", "tags": ["go", "web"]}))
            .expect("set");
        engine
            .set(
                "posts",
                "p3",
                &json!({"title": "C", "tags": ["rust", "wasm"]}),
            )
            .expect("set");

        let results = engine
            .find("posts", r#"tags CONTAINS "rust""#, None, None, 0)
            .expect("find should work");
        assert_eq!(results.len(), 2);
        for doc in &results {
            let tags = doc["tags"].as_array().unwrap();
            assert!(tags.contains(&json!("rust")));
        }
    }

    #[test]
    fn find_compound_and() {
        let mut engine = DocEngine::new();
        engine
            .create_collection("users", CollectionConfig::default())
            .expect("create");
        engine
            .create_index("users", "city", IndexType::Hash)
            .expect("index");
        engine
            .create_index("users", "age", IndexType::Sorted)
            .expect("index");

        engine
            .set("users", "d1", &json!({"city": "Accra", "age": 20}))
            .expect("set");
        engine
            .set("users", "d2", &json!({"city": "Accra", "age": 30}))
            .expect("set");
        engine
            .set("users", "d3", &json!({"city": "Lagos", "age": 30}))
            .expect("set");
        engine
            .set("users", "d4", &json!({"city": "Accra", "age": 40}))
            .expect("set");

        let results = engine
            .find("users", r#"city = "Accra" AND age >= 25"#, None, None, 0)
            .expect("find should work");
        assert_eq!(results.len(), 2);
        for doc in &results {
            assert_eq!(doc["city"], "Accra");
            assert!(doc["age"].as_f64().unwrap() >= 25.0);
        }
    }

    #[test]
    fn find_compound_or() {
        let mut engine = DocEngine::new();
        engine
            .create_collection("users", CollectionConfig::default())
            .expect("create");
        engine
            .create_index("users", "city", IndexType::Hash)
            .expect("index");

        engine
            .set("users", "d1", &json!({"city": "Accra"}))
            .expect("set");
        engine
            .set("users", "d2", &json!({"city": "Lagos"}))
            .expect("set");
        engine
            .set("users", "d3", &json!({"city": "Kumasi"}))
            .expect("set");
        engine
            .set("users", "d4", &json!({"city": "Lagos"}))
            .expect("set");

        let results = engine
            .find(
                "users",
                r#"city = "Accra" OR city = "Lagos""#,
                None,
                None,
                0,
            )
            .expect("find should work");
        assert_eq!(results.len(), 3);
        for doc in &results {
            let city = doc["city"].as_str().unwrap();
            assert!(city == "Accra" || city == "Lagos");
        }
    }

    #[test]
    fn find_with_projection() {
        let mut engine = DocEngine::new();
        engine
            .create_collection("users", CollectionConfig::default())
            .expect("create");
        engine
            .create_index("users", "city", IndexType::Hash)
            .expect("index");

        engine
            .set(
                "users",
                "d1",
                &json!({"name": "Kwame", "city": "Accra", "age": 30}),
            )
            .expect("set");
        engine
            .set(
                "users",
                "d2",
                &json!({"name": "Ama", "city": "Accra", "age": 25}),
            )
            .expect("set");

        let results = engine
            .find("users", r#"city = "Accra""#, Some(&["name"]), None, 0)
            .expect("find should work");
        assert_eq!(results.len(), 2);
        for doc in &results {
            assert!(doc.get("name").is_some());
            assert!(doc.get("city").is_none());
            assert!(doc.get("age").is_none());
        }
    }

    #[test]
    fn find_with_limit_offset() {
        let mut engine = DocEngine::new();
        engine
            .create_collection("users", CollectionConfig::default())
            .expect("create");
        engine
            .create_index("users", "active", IndexType::Hash)
            .expect("index");

        for idx in 0..5 {
            engine
                .set(
                    "users",
                    &format!("d{idx}"),
                    &json!({"n": idx, "active": true}),
                )
                .expect("set");
        }

        let results = engine
            .find("users", "active = true", None, Some(2), 1)
            .expect("find should work");
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn count_query() {
        let mut engine = DocEngine::new();
        engine
            .create_collection("users", CollectionConfig::default())
            .expect("create");
        engine
            .create_index("users", "city", IndexType::Hash)
            .expect("index");

        engine
            .set("users", "d1", &json!({"city": "Accra"}))
            .expect("set");
        engine
            .set("users", "d2", &json!({"city": "Accra"}))
            .expect("set");
        engine
            .set("users", "d3", &json!({"city": "Lagos"}))
            .expect("set");

        let count = engine
            .count("users", r#"city = "Accra""#)
            .expect("count should work");
        assert_eq!(count, 2);
    }

    #[test]
    fn find_unindexed_falls_back_to_scan() {
        let mut engine = DocEngine::new();
        engine
            .create_collection("users", CollectionConfig::default())
            .expect("create");

        engine
            .set("users", "d1", &json!({"name": "Kwame", "city": "Accra"}))
            .expect("set");
        engine
            .set("users", "d2", &json!({"name": "Ama", "city": "Kumasi"}))
            .expect("set");
        engine
            .set("users", "d3", &json!({"name": "Kofi", "city": "Accra"}))
            .expect("set");

        let results = engine
            .find("users", r#"city = "Accra""#, None, None, 0)
            .expect("find should work");
        assert_eq!(results.len(), 2);
        for doc in &results {
            assert_eq!(doc["city"], "Accra");
        }
    }

    #[test]
    fn find_empty_result() {
        let mut engine = DocEngine::new();
        engine
            .create_collection("users", CollectionConfig::default())
            .expect("create");
        engine
            .create_index("users", "city", IndexType::Hash)
            .expect("index");

        engine
            .set("users", "d1", &json!({"city": "Accra"}))
            .expect("set");

        let results = engine
            .find("users", r#"city = "NonExistent""#, None, None, 0)
            .expect("find should work");
        assert!(results.is_empty());
    }
}
