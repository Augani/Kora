//! Collection metadata and configuration.
//!
//! Every document belongs to exactly one collection. A [`Collection`] record
//! stores the collection's name, its compact [`CollectionId`], the creation
//! timestamp, the chosen [`CompressionProfile`], and a running document count
//! that is maintained by the engine on each insert or delete.
//!
//! [`CollectionConfig`] is the user-facing input when creating a collection;
//! it currently controls only the compression profile, but is designed to
//! absorb future per-collection tuning knobs (e.g., index defaults, TTL
//! policies) without breaking the public API.

use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;

use crate::registry::CollectionId;

/// Collection-level compression policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompressionProfile {
    /// No additional compression.
    None,
    /// Dictionary compression for low-cardinality strings.
    Dictionary,
    /// Dictionary + lightweight entropy compression.
    #[default]
    Balanced,
    /// Most aggressive profile with higher CPU cost.
    Compact,
}

/// Parameters used when creating a collection.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CollectionConfig {
    /// Compression policy for the collection.
    pub compression: CompressionProfile,
}

/// Stored collection metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Collection {
    name: String,
    id: CollectionId,
    created_at: u64,
    compression: CompressionProfile,
    doc_count: u64,
}

impl Collection {
    /// Create new metadata for a collection.
    #[must_use]
    pub fn new(name: String, id: CollectionId, config: CollectionConfig) -> Self {
        Self {
            name,
            id,
            created_at: current_unix_seconds(),
            compression: config.compression,
            doc_count: 0,
        }
    }

    /// Collection name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Collection ID.
    #[must_use]
    pub fn id(&self) -> CollectionId {
        self.id
    }

    /// Creation timestamp (seconds since UNIX epoch).
    #[must_use]
    pub fn created_at(&self) -> u64 {
        self.created_at
    }

    /// Compression profile.
    #[must_use]
    pub fn compression(&self) -> CompressionProfile {
        self.compression
    }

    /// Number of stored documents.
    #[must_use]
    pub fn doc_count(&self) -> u64 {
        self.doc_count
    }

    /// Increase document count by one.
    pub fn increment_doc_count(&mut self) {
        self.doc_count = self.doc_count.saturating_add(1);
    }

    /// Decrease document count by one.
    pub fn decrement_doc_count(&mut self) {
        self.doc_count = self.doc_count.saturating_sub(1);
    }
}

/// Errors for collection management operations.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum CollectionError {
    /// The named collection already exists.
    #[error("collection '{0}' already exists")]
    AlreadyExists(String),
}

fn current_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collection_doc_count_tracks_mutations() {
        let mut collection = Collection::new(
            "users".to_string(),
            7,
            CollectionConfig {
                compression: CompressionProfile::Dictionary,
            },
        );

        assert_eq!(collection.doc_count(), 0);
        collection.increment_doc_count();
        collection.increment_doc_count();
        assert_eq!(collection.doc_count(), 2);
        collection.decrement_doc_count();
        assert_eq!(collection.doc_count(), 1);
        collection.decrement_doc_count();
        collection.decrement_doc_count();
        assert_eq!(collection.doc_count(), 0);
        assert_eq!(collection.name(), "users");
        assert_eq!(collection.id(), 7);
        assert_eq!(collection.compression(), CompressionProfile::Dictionary);
    }
}
