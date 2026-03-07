//! # kora-observability
//!
//! Observability subsystem for Kōra.
//!
//! Provides always-on, near-zero overhead statistics including per-command
//! counters, hot key detection via Count-Min Sketch, and memory attribution.
//!
//! ## Modules
//!
//! - [`sketch`] — Count-Min Sketch for probabilistic frequency estimation
//! - [`stats`] — Per-shard statistics, command timers, and snapshot merging

#![warn(clippy::all)]
#![warn(missing_docs)]

pub mod sketch;
pub mod stats;
