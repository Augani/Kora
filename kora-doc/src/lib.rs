//! # kora-doc
//!
//! Document layer that turns Kora from a key-value cache engine into a
//! JSON-native document database. Documents are stored in a compact binary
//! packed format, queried via a WHERE expression parser, and indexed through
//! four secondary index types (hash, sorted, array, unique).
//!
//! ## Architecture
//!
//! A JSON document passes through a pipeline of transformations before it
//! reaches storage:
//!
//! 1. **Decomposition** ([`decompose`]) -- a JSON object is recursively walked,
//!    each leaf field is assigned a numeric [`FieldId`](registry::FieldId) via
//!    the [`IdRegistry`](registry::IdRegistry), string values are
//!    dictionary-encoded when their field cardinality is low, and the result is
//!    assembled into a [`PackedDoc`](packed::PackedDoc).
//!
//! 2. **Packed encoding** ([`packed`]) -- fields are stored in a flat binary
//!    buffer with an offset table sorted by field ID, enabling O(log F)
//!    single-field reads via binary search.
//!
//! 3. **Recomposition** ([`recompose`]) -- the inverse of decomposition,
//!    rebuilding a `serde_json::Value` from packed bytes, dictionary lookups,
//!    and registry path resolution. Supports full reconstruction or
//!    field-level projection.
//!
//! ## Key Modules
//!
//! | Module | Purpose |
//! |---|---|
//! | [`packed`] | Binary packed document format and builder |
//! | [`registry`] | Integer-keyed ID mapping for collections, fields, and docs |
//! | [`dictionary`] | Dictionary encoding for low-cardinality string values |
//! | [`decompose`] | JSON to packed document conversion |
//! | [`recompose`] | Packed document to JSON reconstruction |
//! | [`engine`] | Collection CRUD, index maintenance, and query execution |
//! | [`expr`] | WHERE clause recursive-descent parser |
//! | [`index`] | Hash, sorted, array, and unique secondary indexes |
//! | [`collection`] | Collection metadata and configuration |
//! | [`key`] | Binary key encoding for storage and index records |

#![warn(clippy::all)]
#![warn(missing_docs)]

pub mod collection;
pub mod decompose;
pub mod dictionary;
pub mod engine;
pub mod expr;
pub mod index;
pub mod key;
pub mod packed;
pub mod recompose;
pub mod registry;

pub use collection::{Collection, CollectionConfig, CollectionError, CompressionProfile};
pub use decompose::{DecomposeError, Decomposer};
pub use dictionary::{DictionaryError, StoredValue, ValueDictionary, ValueDictionaryConfig};
pub use engine::{
    CollectionInfo, DictionaryFieldInfo, DictionaryInfo, DocEngine, DocError, DocMutation,
    InsertResult, SetResult, StorageInfo,
};
pub use expr::{Expr, ExprError, ExprValue};
pub use index::{hash32, CollectionIndexes, IndexConfig, IndexError, IndexType};
pub use key::{
    decode_cdc_event_key, decode_cold_doc_key, decode_collection_key, decode_compound_index_key,
    decode_doc_key, decode_hashed_bucket_key, decode_sorted_index_key, encode_cdc_event_key,
    encode_cold_doc_key, encode_collection_key, encode_compound_index_key, encode_doc_key,
    encode_hashed_bucket_key, encode_sorted_index_key, KeyDecodeError, KeyTag,
};
pub use packed::{FieldValue, PackedDoc, PackedDocBuilder, PackedDocError, PackedDocIter};
pub use recompose::{RecomposeError, Recomposer};
pub use registry::{
    CollectionId, DocId, FieldId, IdRegistry, RegistryError, RegistrySegment, RegistrySegmentRef,
};
