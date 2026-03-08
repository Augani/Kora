//! Kōra — A multi-threaded, embeddable, memory-safe cache engine.
//!
//! This is the CLI entrypoint that starts the Kōra server.

mod config;

use std::path::PathBuf;

use clap::Parser;
use kora_server::{KoraServer, ServerConfig};
use kora_storage::manager::StorageConfig;
use kora_storage::wal::SyncPolicy;

use crate::config::FileConfig;

/// Kōra — A multi-threaded, embeddable, memory-safe cache engine.
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

    /// Address to bind the optional memcached listener.
    #[arg(long)]
    memcached_bind: Option<String>,

    /// Port for the optional memcached listener.
    #[arg(long)]
    memcached_port: Option<u16>,

    /// Log level (trace, debug, info, warn, error).
    #[arg(long)]
    log_level: Option<String>,

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

    /// WASM scripting fuel budget (0 = disabled).
    #[arg(long)]
    script_max_fuel: Option<u64>,

    /// Port for Prometheus metrics HTTP endpoint (0 = disabled).
    #[arg(long)]
    metrics_port: Option<u16>,

    /// Unix socket path.
    #[arg(long)]
    unix_socket: Option<String>,

    /// Enable tenant resource-limit checks on command dispatch.
    #[arg(long)]
    tenant_limits: bool,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Load config file (defaults to kora.toml, ignored if not found)
    let file_config = FileConfig::load(&args.config)?;

    // CLI args override config file, which overrides defaults
    let bind = args
        .bind
        .or(file_config.bind)
        .unwrap_or_else(|| "127.0.0.1".into());
    let port = args.port.or(file_config.port).unwrap_or(6379);
    let memcached_bind = args.memcached_bind.or(file_config.memcached_bind);
    let memcached_port = args.memcached_port.or(file_config.memcached_port);
    let log_level = args
        .log_level
        .or(file_config.log_level)
        .unwrap_or_else(|| "info".into());
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

    // Build storage config from file + CLI
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
    let script_max_fuel = args
        .script_max_fuel
        .or(file_config.script_max_fuel)
        .unwrap_or(0);

    let metrics_port = args.metrics_port.or(file_config.metrics_port).unwrap_or(0);
    let unix_socket = args
        .unix_socket
        .or(file_config.unix_socket)
        .map(PathBuf::from);
    let memcached_bind_address = memcached_port.map(|port| {
        let host = memcached_bind.unwrap_or_else(|| bind.clone());
        format!("{}:{}", host, port)
    });
    let tenant_limits_enabled =
        args.tenant_limits || file_config.tenant_limits_enabled.unwrap_or(false);

    let config = ServerConfig {
        bind_address: format!("{}:{}", bind, port),
        memcached_bind_address,
        worker_count,
        storage,
        cdc_capacity,
        script_max_fuel,
        metrics_port,
        unix_socket,
        password: None,
        tenant_limits_enabled,
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

        // Handle Ctrl+C
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.ok();
            tracing::info!("Received shutdown signal");
            let _ = shutdown_tx.send(true);
        });

        server.run(shutdown_rx).await
    })?;

    Ok(())
}
