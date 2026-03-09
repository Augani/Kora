//! Packed document to JSON reconstruction pipeline.
//!
//! [`Recomposer`] is the inverse of
//! [`Decomposer`](crate::decompose::Decomposer). Given a [`PackedDoc`], the
//! [`IdRegistry`], and the collection's [`ValueDictionary`], it rebuilds a
//! `serde_json::Value` by:
//!
//! 1. Iterating over packed fields in field-ID order.
//! 2. Resolving each numeric `FieldId` back to its dot-separated path string
//!    via the registry.
//! 3. Decoding the [`FieldValue`](crate::packed::FieldValue) into a JSON
//!    primitive, looking up `DictRef` values through the dictionary.
//! 4. Inserting each value into a nested `serde_json::Map` tree by splitting
//!    the dotted path and creating intermediate objects as needed.
//!
//! ## Projection
//!
//! [`Recomposer::project`] reconstructs only a caller-specified subset of
//! fields, leveraging [`PackedDoc::read_field`]'s O(log F) binary search to
//! skip unneeded data. This is the fast path for queries that select a small
//! number of fields from large documents.

use std::str;

use serde_json::{Map, Value};
use thiserror::Error;

use crate::dictionary::{DictionaryError, ValueDictionary};
use crate::packed::{FieldValue, PackedDoc, PackedDocError};
use crate::registry::{CollectionId, FieldId, IdRegistry};

/// Errors returned when reconstructing JSON from a packed document.
#[derive(Debug, Error)]
pub enum RecomposeError {
    /// Registry does not know one of the field IDs in the packed payload.
    #[error("unknown field id {field_id} in collection {collection_id}")]
    UnknownFieldId {
        /// Collection that owns the field.
        collection_id: CollectionId,
        /// Unknown field id.
        field_id: FieldId,
    },
    /// Dictionary operation failed.
    #[error(transparent)]
    Dictionary(#[from] DictionaryError),
    /// Packed document read failed.
    #[error(transparent)]
    Packed(#[from] PackedDocError),
    /// Stored UTF-8 bytes are invalid.
    #[error("invalid utf-8 string at field id {field_id}: {message}")]
    InvalidUtf8 {
        /// Field ID with invalid bytes.
        field_id: FieldId,
        /// UTF-8 decoder error.
        message: String,
    },
    /// Structured payload bytes are invalid JSON.
    #[error("invalid structured payload at field id {field_id}: {message}")]
    InvalidStructuredPayload {
        /// Field ID with invalid payload bytes.
        field_id: FieldId,
        /// serde_json parser error string.
        message: String,
    },
    /// Field path conflicts with an existing scalar/object shape.
    #[error("path conflict while inserting '{path}'")]
    PathConflict {
        /// Dotted path being inserted.
        path: String,
    },
}

/// Rebuild full or partial JSON documents from `PackedDoc`.
pub struct Recomposer;

impl Recomposer {
    /// Reconstruct a full JSON document.
    pub fn recompose(
        packed: &PackedDoc,
        registry: &IdRegistry,
        dictionary: &ValueDictionary,
        collection_id: CollectionId,
    ) -> Result<Value, RecomposeError> {
        let mut root = Value::Object(Map::new());
        for entry in packed.iter_fields()? {
            let (field_id, field_value) = entry?;
            let path = registry.field_path(collection_id, field_id).ok_or(
                RecomposeError::UnknownFieldId {
                    collection_id,
                    field_id,
                },
            )?;
            let json_value = field_to_json(field_id, field_value, dictionary)?;
            insert_at_path(&mut root, path, json_value)?;
        }
        Ok(root)
    }

    /// Reconstruct only selected fields by field ID.
    pub fn project(
        packed: &PackedDoc,
        field_ids: &[FieldId],
        registry: &IdRegistry,
        dictionary: &ValueDictionary,
        collection_id: CollectionId,
    ) -> Result<Value, RecomposeError> {
        let mut root = Value::Object(Map::new());
        for field_id in field_ids {
            if let Some(field_value) = packed.read_field(*field_id)? {
                let path = registry.field_path(collection_id, *field_id).ok_or(
                    RecomposeError::UnknownFieldId {
                        collection_id,
                        field_id: *field_id,
                    },
                )?;
                let json_value = field_to_json(*field_id, field_value, dictionary)?;
                insert_at_path(&mut root, path, json_value)?;
            }
        }
        Ok(root)
    }
}

fn field_to_json(
    field_id: FieldId,
    value: FieldValue,
    dictionary: &ValueDictionary,
) -> Result<Value, RecomposeError> {
    match value {
        FieldValue::Null => Ok(Value::Null),
        FieldValue::Bool(value) => Ok(Value::Bool(value)),
        FieldValue::I64(value) => Ok(Value::Number(value.into())),
        FieldValue::F64(value) => serde_json::Number::from_f64(value)
            .map(Value::Number)
            .ok_or_else(|| RecomposeError::InvalidStructuredPayload {
                field_id,
                message: "non-finite float cannot be represented in JSON".to_string(),
            }),
        FieldValue::InlineBytes(bytes) => {
            let string = str::from_utf8(&bytes).map_err(|err| RecomposeError::InvalidUtf8 {
                field_id,
                message: err.to_string(),
            })?;
            Ok(Value::String(string.to_string()))
        }
        FieldValue::DictRef(dict_id) => {
            let bytes = dictionary.decode(&crate::dictionary::StoredValue::DictRef(dict_id))?;
            let string = str::from_utf8(&bytes).map_err(|err| RecomposeError::InvalidUtf8 {
                field_id,
                message: err.to_string(),
            })?;
            Ok(Value::String(string.to_string()))
        }
        FieldValue::ArrayBytes(bytes) => serde_json::from_slice::<Value>(&bytes).map_err(|err| {
            RecomposeError::InvalidStructuredPayload {
                field_id,
                message: err.to_string(),
            }
        }),
    }
}

fn insert_at_path(root: &mut Value, path: &str, value: Value) -> Result<(), RecomposeError> {
    if path.is_empty() {
        *root = value;
        return Ok(());
    }

    let parts: Vec<&str> = path.split('.').collect();
    insert_path_parts(root, &parts, value, path)
}

fn insert_path_parts(
    current: &mut Value,
    parts: &[&str],
    value: Value,
    full_path: &str,
) -> Result<(), RecomposeError> {
    if parts.is_empty() {
        *current = value;
        return Ok(());
    }

    let key = parts[0];
    if parts.len() == 1 {
        let map = current
            .as_object_mut()
            .ok_or_else(|| RecomposeError::PathConflict {
                path: full_path.to_string(),
            })?;
        map.insert(key.to_string(), value);
        return Ok(());
    }

    let map = current
        .as_object_mut()
        .ok_or_else(|| RecomposeError::PathConflict {
            path: full_path.to_string(),
        })?;

    let child = map
        .entry(key.to_string())
        .or_insert_with(|| Value::Object(Map::new()));

    if !child.is_object() {
        return Err(RecomposeError::PathConflict {
            path: full_path.to_string(),
        });
    }

    insert_path_parts(child, &parts[1..], value, full_path)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::decompose::Decomposer;
    use crate::dictionary::ValueDictionary;
    use crate::registry::IdRegistry;

    use super::*;

    #[test]
    fn projection_returns_requested_fields_only() {
        let mut registry = IdRegistry::new();
        let mut dictionary = ValueDictionary::default();
        let collection_id = registry
            .get_or_create_collection_id("users")
            .expect("collection id should allocate");

        let mut decomposer = Decomposer::new(collection_id, &mut registry, &mut dictionary, 1);
        let packed = decomposer
            .decompose(
                &json!({
                    "name": "Augustus",
                    "age": 30,
                    "address": {"city": "Accra", "zip": "00233"}
                }),
                1,
            )
            .expect("decompose should work");

        let city_id = registry
            .segment(collection_id)
            .and_then(|segment| segment.field_id("address.city"))
            .expect("city field id should exist");
        let age_id = registry
            .segment(collection_id)
            .and_then(|segment| segment.field_id("age"))
            .expect("age field id should exist");

        let projected = Recomposer::project(
            &packed,
            &[city_id, age_id],
            &registry,
            &dictionary,
            collection_id,
        )
        .expect("projection should work");
        assert_eq!(projected, json!({"address": {"city": "Accra"}, "age": 30}));
    }
}
