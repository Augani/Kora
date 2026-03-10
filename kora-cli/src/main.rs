//! CLI entrypoint for the Kōra cache server.
//!
//! This binary parses configuration from three sources, applied in order of
//! increasing precedence:
//!
//! 1. **Built-in defaults** — sensible values for a single-node deployment
//!    (bind `127.0.0.1:6379`, worker count auto-detected from CPU cores,
//!    log level `info`, persistence disabled).
//! 2. **TOML config file** — loaded from the path given by `--config`
//!    (defaults to `kora.toml` in the working directory). Missing files are
//!    silently ignored.
//! 3. **CLI arguments** — any flag passed on the command line overrides the
//!    corresponding value from the config file.
//!
//! After merging configuration the binary initialises a `current_thread` Tokio
//! runtime, constructs a [`kora_server::KoraServer`], and runs it until a
//! `SIGINT` (Ctrl-C) triggers graceful shutdown.

mod config;

use std::path::PathBuf;

use clap::Parser;
use kora_server::{KoraServer, ServerConfig};
use kora_storage::manager::StorageConfig;
use kora_storage::wal::SyncPolicy;

use crate::config::FileConfig;

/// Command-line arguments for the Kōra server binary.
///
/// Every optional field mirrors a key in the TOML config file. When present,
/// the CLI value takes precedence over the file value.
#[derive(Parser)]
#[command(name = "kora", version, about)]
struct Args {
    /// Path to config file (TOML format).
    #[arg(short, long, default_value = "kora.toml")]
    config: PathBuf,

    /// Address to bind to.
    #[arg(long)]
    bind: Option<String>,

    /// Port to listen on.
    #[arg(short, long)]
    port: Option<u16>,

    /// Number of shard worker threads (defaults to available CPU cores).
    #[arg(short, long)]
    workers: Option<usize>,

    /// Log level (trace, debug, info, warn, error).
    #[arg(long)]
    log_level: Option<String>,

    /// Optional AUTH password (`requirepass` equivalent).
    #[arg(long)]
    password: Option<String>,

    /// Data directory for persistence.
    #[arg(long)]
    data_dir: Option<String>,

    /// Automatic snapshot interval in seconds.
    #[arg(long)]
    snapshot_interval_secs: Option<u64>,

    /// Number of timestamped snapshots to retain per shard.
    #[arg(long)]
    snapshot_retain: Option<usize>,

    /// CDC ring buffer capacity per shard (0 = disabled).
    #[arg(long)]
    cdc_capacity: Option<usize>,

    /// Port for Prometheus metrics HTTP endpoint (0 = disabled).
    #[arg(long)]
    metrics_port: Option<u16>,

    /// Unix socket path.
    #[arg(long)]
    unix_socket: Option<String>,
}

/// Parses configuration, initialises the Tokio runtime, and runs the server
/// until shutdown is signalled.
fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let file_config = FileConfig::load(&args.config)?;

    let bind = args
        .bind
        .or(file_config.bind)
        .unwrap_or_else(|| "127.0.0.1".into());
    let port = args.port.or(file_config.port).unwrap_or(6379);
    let log_level = args
        .log_level
        .or(file_config.log_level)
        .unwrap_or_else(|| "info".into());
    let password = args.password.or(file_config.password);
    let worker_count = args
        .workers
        .or(file_config.workers)
        .unwrap_or_else(kora_server::optimal_worker_count);

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&log_level)),
        )
        .init();

    let data_dir = args.data_dir.or(file_config.storage.data_dir.clone());
    let storage = data_dir.map(|dir| {
        let wal_sync = match file_config
            .storage
            .wal_sync
            .as_deref()
            .unwrap_or("every_second")
        {
            "every_write" => SyncPolicy::EveryWrite,
            "os_managed" => SyncPolicy::OsManaged,
            _ => SyncPolicy::EverySecond,
        };
        StorageConfig {
            data_dir: PathBuf::from(dir),
            wal_sync_policy: wal_sync,
            wal_enabled: file_config.storage.wal_enabled.unwrap_or(true),
            rdb_enabled: file_config.storage.rdb_enabled.unwrap_or(true),
            snapshot_interval_secs: args
                .snapshot_interval_secs
                .or(file_config.storage.snapshot_interval_secs),
            snapshot_retain: args
                .snapshot_retain
                .or(file_config.storage.snapshot_retain)
                .or(Some(24)),
            wal_max_bytes: file_config
                .storage
                .wal_max_bytes
                .unwrap_or(64 * 1024 * 1024),
        }
    });

    let cdc_capacity = args.cdc_capacity.or(file_config.cdc_capacity).unwrap_or(0);

    let metrics_port = args.metrics_port.or(file_config.metrics_port).unwrap_or(0);
    let unix_socket = args
        .unix_socket
        .or(file_config.unix_socket)
        .map(PathBuf::from);
    let config = ServerConfig {
        bind_address: format!("{}:{}", bind, port),
        worker_count,
        storage,
        cdc_capacity,

        metrics_port,
        unix_socket,
        password,
    };

    tracing::info!(
        "Starting Kōra v{} with {} shard-IO workers",
        env!("CARGO_PKG_VERSION"),
        worker_count,
    );

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    runtime.block_on(async move {
        let server = KoraServer::new(config);
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.ok();
            tracing::info!("Received shutdown signal");
            let _ = shutdown_tx.send(true);
        });

        server.run(shutdown_rx).await
    })?;

    Ok(())
}
