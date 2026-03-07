//! # kora-server
//!
//! TCP/Unix socket server, connection handling, and command dispatch for Kōra.
//!
//! Accepts client connections, parses RESP commands via `kora-protocol`,
//! routes them to the appropriate shard in `kora-core`, and writes back responses.

#![warn(clippy::all)]
#![warn(missing_docs)]
