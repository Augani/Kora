//! # kora-vector
//!
//! HNSW (Hierarchical Navigable Small World) vector index for Kōra.
//!
//! Provides per-shard vector indexing with support for cosine, L2, and inner
//! product distance metrics.
//!
//! ## Modules
//!
//! - [`distance`] — Distance metric implementations
//! - [`hnsw`] — HNSW graph index
//! - [`quantizer`] — Product quantizer for vector compression

#![warn(clippy::all)]
#![warn(missing_docs)]

pub mod distance;
pub mod hnsw;
pub mod quantizer;
