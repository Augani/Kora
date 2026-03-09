//! # kora-observability
//!
//! Observability subsystem for the Kōra cache engine.
//!
//! Provides always-on, near-zero overhead instrumentation for production
//! monitoring: per-command counters, HDR latency histograms, hot key
//! detection via Count-Min Sketch, memory attribution by key prefix, and
//! Prometheus-compatible metrics exposition.
//!
//! All primitives are designed for concurrent access from shard worker
//! threads using atomic counters, lock-free reads, and fine-grained
//! mutexes where necessary.
//!
//! ## Modules
//!
//! - [`histogram`] — HDR histograms for per-command latency distributions
//! - [`prometheus`] — Prometheus text exposition format renderer
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
