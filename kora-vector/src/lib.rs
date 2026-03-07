//! # kora-vector
//!
//! HNSW (Hierarchical Navigable Small World) vector index for Kōra.
//!
//! Provides per-shard vector indexing with support for cosine, L2, and inner
//! product distance metrics, plus optional product quantization for memory
//! efficiency.

#![warn(clippy::all)]
#![warn(missing_docs)]
