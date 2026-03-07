//! # kora-embedded
//!
//! Embeddable library mode for Kōra.
//!
//! Provides a `Database` struct that wraps the same `ShardEngine` the server
//! uses, but routes commands through direct channel sends instead of TCP.

#![warn(clippy::all)]
#![warn(missing_docs)]
