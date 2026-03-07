//! # kora-observability
//!
//! Observability subsystem for Kōra.
//!
//! Provides always-on, near-zero overhead statistics including per-command
//! counters, hot key detection via Count-Min Sketch, memory attribution by
//! key prefix, and latency histograms.

#![warn(clippy::all)]
#![warn(missing_docs)]
