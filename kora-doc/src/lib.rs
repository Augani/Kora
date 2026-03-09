//! # kora-doc
//!
//! Foundation components for Kora's document layer.
//!
//! This initial implementation provides:
//! - Packed document encoding/decoding primitives
//! - ID registry primitives for collection/field/document IDs
//! - Dictionary encoding primitives for low-cardinality string values
//! - Core collection and document CRUD engine primitives

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
    SetResult, StorageInfo,
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
