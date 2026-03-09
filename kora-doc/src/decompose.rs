//! JSON-to-packed-document decomposition pipeline.
//!
//! [`Decomposer`] walks a `serde_json::Value` tree depth-first, flattening
//! nested objects into dot-separated field paths (e.g., `address.city`). Each
//! leaf field is:
//!
//! 1. Assigned a compact [`FieldId`](crate::registry::FieldId) via the
//!    [`IdRegistry`](crate::registry::IdRegistry), creating the mapping on
//!    first encounter.
//! 2. Optionally dictionary-encoded through the collection's
//!    [`ValueDictionary`](crate::dictionary::ValueDictionary) when the field's
//!    observed cardinality is low and the value meets the minimum length
//!    threshold.
//! 3. Emitted as a [`FieldValue`](crate::packed::FieldValue) into a
//!    [`PackedDocBuilder`](crate::packed::PackedDocBuilder).
//!
//! Arrays and empty objects are stored as opaque JSON byte payloads
//! (`FieldValue::ArrayBytes`) to preserve exact round-trip fidelity. Field
//! names containing dots are rejected because dots serve as the path
//! separator in the flattened schema.

use serde_json::Value;
use thiserror::Error;

use crate::dictionary::{DictionaryError, StoredValue, ValueDictionary};
use crate::packed::{FieldValue, PackedDoc, PackedDocBuilder, PackedDocError};
use crate::registry::{CollectionId, IdRegistry, RegistryError};

/// Errors returned when decomposing JSON into a packed document.
#[derive(Debug, Error)]
pub enum DecomposeError {
    /// Root documents must be JSON objects.
    #[error("root document must be a JSON object")]
    RootMustBeObject,
    /// Field names cannot be empty or contain reserved separator characters.
    #[error("invalid field name '{field_name}' at path '{path}'")]
    InvalidFieldName {
        /// Invalid field name.
        field_name: String,
        /// Parent path where the field appeared.
        path: String,
    },
    /// Numbers that cannot be represented in packed format are rejected.
    #[error("number at path '{0}' is not representable as i64/f64")]
    UnsupportedNumber(String),
    /// Registry operation failed.
    #[error(transparent)]
    Registry(#[from] RegistryError),
    /// Dictionary operation failed.
    #[error(transparent)]
    Dictionary(#[from] DictionaryError),
    /// Packed document encoding failed.
    #[error(transparent)]
    Packed(#[from] PackedDocError),
    /// Structured value payload serialization failed.
    #[error("failed to serialize structured value for path '{path}': {message}")]
    StructuredValueEncode {
        /// Dotted field path.
        path: String,
        /// serde_json error string.
        message: String,
    },
}

/// JSON decomposer for one collection.
pub struct Decomposer<'a> {
    collection_id: CollectionId,
    registry: &'a mut IdRegistry,
    dictionary: &'a mut ValueDictionary,
    packed_version: u16,
}

impl<'a> Decomposer<'a> {
    /// Create a decomposer for one collection.
    #[must_use]
    pub fn new(
        collection_id: CollectionId,
        registry: &'a mut IdRegistry,
        dictionary: &'a mut ValueDictionary,
        packed_version: u16,
    ) -> Self {
        Self {
            collection_id,
            registry,
            dictionary,
            packed_version,
        }
    }

    /// Decompose one JSON object into `PackedDoc`.
    pub fn decompose(
        &mut self,
        json: &Value,
        updated_at: u32,
    ) -> Result<PackedDoc, DecomposeError> {
        let Value::Object(map) = json else {
            return Err(DecomposeError::RootMustBeObject);
        };

        let mut builder = PackedDocBuilder::new(self.packed_version);
        for (key, value) in map {
            validate_field_name("", key)?;
            self.walk(key, value, &mut builder)?;
        }
        builder.build(updated_at).map_err(DecomposeError::from)
    }

    fn walk(
        &mut self,
        path: &str,
        value: &Value,
        builder: &mut PackedDocBuilder,
    ) -> Result<(), DecomposeError> {
        match value {
            Value::Object(map) => {
                if map.is_empty() {
                    let field_id = self
                        .registry
                        .get_or_create_field_id(self.collection_id, path)?;
                    builder.add_field(field_id, FieldValue::ArrayBytes(b"{}".to_vec()))?;
                    return Ok(());
                }

                for (key, nested_value) in map {
                    validate_field_name(path, key)?;
                    let nested_path = join_path(path, key);
                    self.walk(&nested_path, nested_value, builder)?;
                }
                Ok(())
            }
            Value::Array(_) => {
                let field_id = self
                    .registry
                    .get_or_create_field_id(self.collection_id, path)?;
                let payload = serde_json::to_vec(value).map_err(|err| {
                    DecomposeError::StructuredValueEncode {
                        path: path.to_string(),
                        message: err.to_string(),
                    }
                })?;
                builder.add_field(field_id, FieldValue::ArrayBytes(payload))?;
                Ok(())
            }
            Value::String(string) => {
                let field_id = self
                    .registry
                    .get_or_create_field_id(self.collection_id, path)?;
                let stored = self.dictionary.encode(field_id, string.as_bytes())?;
                let encoded = match stored {
                    StoredValue::DictRef(id) => FieldValue::DictRef(id),
                    StoredValue::Inline(bytes) => FieldValue::InlineBytes(bytes),
                };
                builder.add_field(field_id, encoded)?;
                Ok(())
            }
            Value::Number(number) => {
                let field_id = self
                    .registry
                    .get_or_create_field_id(self.collection_id, path)?;
                if let Some(int) = number.as_i64() {
                    builder.add_field(field_id, FieldValue::I64(int))?;
                    return Ok(());
                }
                if let Some(float) = number.as_f64() {
                    builder.add_field(field_id, FieldValue::F64(float))?;
                    return Ok(());
                }
                Err(DecomposeError::UnsupportedNumber(path.to_string()))
            }
            Value::Bool(value) => {
                let field_id = self
                    .registry
                    .get_or_create_field_id(self.collection_id, path)?;
                builder.add_field(field_id, FieldValue::Bool(*value))?;
                Ok(())
            }
            Value::Null => {
                let field_id = self
                    .registry
                    .get_or_create_field_id(self.collection_id, path)?;
                builder.add_field(field_id, FieldValue::Null)?;
                Ok(())
            }
        }
    }
}

fn join_path(parent: &str, child: &str) -> String {
    format!("{parent}.{child}")
}

fn validate_field_name(path: &str, field_name: &str) -> Result<(), DecomposeError> {
    if field_name.is_empty() || field_name.contains('.') {
        return Err(DecomposeError::InvalidFieldName {
            field_name: field_name.to_string(),
            path: path.to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::dictionary::ValueDictionaryConfig;
    use crate::recompose::Recomposer;

    use super::*;

    #[test]
    fn decomposes_and_recomposes_nested_document() {
        let mut registry = IdRegistry::new();
        let mut dictionary = ValueDictionary::new(ValueDictionaryConfig {
            low_cardinality_threshold: 1_000,
            min_len_for_dictionary: 2,
        });
        let collection_id = registry
            .get_or_create_collection_id("users")
            .expect("collection id should allocate");

        let mut decomposer = Decomposer::new(collection_id, &mut registry, &mut dictionary, 1);
        let source = json!({
            "name": "Augustus",
            "active": true,
            "address": {"city": "Accra"},
            "tags": ["rust", "systems"]
        });

        let packed = decomposer
            .decompose(&source, 42)
            .expect("decomposition should work");
        let recomposed = Recomposer::recompose(&packed, &registry, &dictionary, collection_id)
            .expect("recompose should work");
        assert_eq!(recomposed, source);
    }

    #[test]
    fn rejects_non_object_root() {
        let mut registry = IdRegistry::new();
        let mut dictionary = ValueDictionary::default();
        let collection_id = registry
            .get_or_create_collection_id("users")
            .expect("collection id should allocate");
        let mut decomposer = Decomposer::new(collection_id, &mut registry, &mut dictionary, 1);

        let err = decomposer
            .decompose(&json!("string"), 0)
            .expect_err("non-object root should fail");
        assert!(matches!(err, DecomposeError::RootMustBeObject));
    }

    #[test]
    fn rejects_field_names_with_dots() {
        let mut registry = IdRegistry::new();
        let mut dictionary = ValueDictionary::default();
        let collection_id = registry
            .get_or_create_collection_id("users")
            .expect("collection id should allocate");
        let mut decomposer = Decomposer::new(collection_id, &mut registry, &mut dictionary, 1);

        let err = decomposer
            .decompose(&json!({ "address.city": "Accra" }), 0)
            .expect_err("dotted field names are ambiguous and must fail");

        assert!(matches!(
            err,
            DecomposeError::InvalidFieldName { field_name, .. } if field_name == "address.city"
        ));
    }
}
