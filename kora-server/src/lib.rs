//! # kora-server
//!
//! TCP/Unix socket server, connection handling, and command dispatch for Kōra.
//!
//! Accepts client connections, parses RESP commands via `kora-protocol`,
//! routes them to the appropriate shard in `kora-core`, and writes back responses.
//!
//! Uses a shard-affinity I/O architecture where each shard worker runs its own
//! single-threaded tokio runtime and owns both data and connection I/O.

#![warn(clippy::all)]

mod shard_io;

use kora_core::shard::WalWriter;
use kora_storage::manager::StorageConfig;
use kora_storage::shard_storage::ShardStorage;

/// Configuration for the Kōra server.
pub struct ServerConfig {
    /// Address to bind to (e.g., "127.0.0.1:6379").
    pub bind_address: String,
    /// Number of shard worker threads.
    pub worker_count: usize,
    /// Optional storage configuration for persistence.
    pub storage: Option<StorageConfig>,
    /// CDC ring buffer capacity per shard (0 = disabled).
    pub cdc_capacity: usize,
    /// WASM scripting fuel budget (0 = disabled).
    pub script_max_fuel: u64,
    /// Port for Prometheus metrics HTTP endpoint (0 = disabled).
    pub metrics_port: u16,
    /// Optional Unix socket path.
    pub unix_socket: Option<std::path::PathBuf>,
    /// Optional password for AUTH command validation.
    pub password: Option<String>,
    /// Enable tenant resource-limit checks on command dispatch.
    pub tenant_limits_enabled: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        let worker_count = std::thread::available_parallelism()
            .map(|n| (n.get() / 3).max(1))
            .unwrap_or(4);
        Self {
            bind_address: "127.0.0.1:6379".into(),
            worker_count,
            storage: None,
            cdc_capacity: 0,
            script_max_fuel: 0,
            metrics_port: 0,
            unix_socket: None,
            password: None,
            tenant_limits_enabled: false,
        }
    }
}

/// The Kōra TCP server.
pub struct KoraServer {
    engine: shard_io::ShardIoEngine,
    bind_address: String,
    unix_socket: Option<std::path::PathBuf>,
}

fn create_shard_storage(config: &ServerConfig) -> Vec<Option<Box<dyn WalWriter>>> {
    match config.storage {
        Some(ref sc) => {
            let mut writers: Vec<Option<Box<dyn WalWriter>>> =
                Vec::with_capacity(config.worker_count);
            for i in 0..config.worker_count {
                match ShardStorage::open_with_config(
                    i as u16,
                    &sc.data_dir,
                    sc.wal_sync_policy,
                    sc.wal_enabled,
                    sc.rdb_enabled,
                    sc.wal_max_bytes,
                ) {
                    Ok(storage) => {
                        tracing::info!("Opened per-shard storage for shard {}", i);
                        writers.push(Some(Box::new(storage)));
                    }
                    Err(e) => {
                        tracing::error!("Failed to open shard {} storage: {}", i, e);
                        writers.push(None);
                    }
                }
            }
            writers
        }
        None => (0..config.worker_count).map(|_| None).collect(),
    }
}

impl KoraServer {
    /// Create a new server with the given configuration.
    pub fn new(config: ServerConfig) -> Self {
        let wal_writers = create_shard_storage(&config);
        let bind_address = config.bind_address.clone();
        let unix_socket = config.unix_socket.clone();

        let engine = shard_io::ShardIoEngine::new(
            config.worker_count,
            wal_writers,
            config.cdc_capacity,
            config.script_max_fuel,
            config.metrics_port,
            config.password.clone(),
            config.tenant_limits_enabled,
            config.storage,
        );

        Self {
            engine,
            bind_address,
            unix_socket,
        }
    }

    /// Run the server, accepting connections until the shutdown signal.
    pub async fn run(&self, shutdown: tokio::sync::watch::Receiver<bool>) -> std::io::Result<()> {
        self.engine
            .run(&self.bind_address, self.unix_socket.as_ref(), shutdown)
            .await
    }
}
