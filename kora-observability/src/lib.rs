//! # kora-observability
//!
//! Observability subsystem for Kōra.
//!
//! Provides always-on, near-zero overhead statistics including per-command
//! counters, hot key detection via Count-Min Sketch, and memory attribution.
//!
//! ## Modules
//!
//! - [`histogram`] — HDR histograms for per-command latency distributions
//! - [`prometheus`] — Prometheus exposition format output
//! - [`sketch`] — Count-Min Sketch for probabilistic frequency estimation
//! - [`stats`] — Per-shard statistics, command timers, and snapshot merging
//! - [`trie`] — Prefix trie for memory attribution by key prefix

#![warn(clippy::all)]
#![warn(missing_docs)]

pub mod histogram;
pub mod prometheus;
pub mod sketch;
pub mod stats;
pub mod trie;
