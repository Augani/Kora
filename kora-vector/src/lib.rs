//! # kora-vector
//!
//! Vector similarity search for the Kōra cache engine.
//!
//! This crate provides an in-process, approximate nearest neighbor (ANN) index
//! based on the HNSW algorithm. Components are designed to live inside a single
//! shard with no cross-thread synchronisation requirements.
//!
//! ## Modules
//!
//! - [`distance`] — Distance metric implementations (Cosine, L2, Inner Product)
//! - [`hnsw`] — HNSW graph index with configurable connectivity and search width

#![warn(clippy::all)]
#![warn(missing_docs)]

pub mod distance;
pub mod hnsw;
