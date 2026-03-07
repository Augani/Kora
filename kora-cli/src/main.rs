//! Kōra — A multi-threaded, embeddable, memory-safe cache engine.
//!
//! This is the CLI entrypoint that starts the Kōra server.

mod config;

use std::path::PathBuf;

use clap::Parser;
use kora_server::{KoraServer, ServerConfig};

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

    /// Number of worker threads (defaults to CPU count).
    #[arg(short, long)]
    workers: Option<usize>,

    /// Log level (trace, debug, info, warn, error).
    #[arg(long)]
    log_level: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Load config file (defaults to kora.toml, ignored if not found)
    let file_config = FileConfig::load(&args.config)?;

    // CLI args override config file, which overrides defaults
    let bind = args
        .bind
        .or(file_config.bind)
        .unwrap_or_else(|| "127.0.0.1".into());
    let port = args.port.or(file_config.port).unwrap_or(6379);
    let log_level = args
        .log_level
        .or(file_config.log_level)
        .unwrap_or_else(|| "info".into());
    let worker_count = args.workers.or(file_config.workers).unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
    });

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&log_level)),
        )
        .init();

    let config = ServerConfig {
        bind_address: format!("{}:{}", bind, port),
        worker_count,
    };

    tracing::info!(
        "Starting Kōra v{} with {} worker threads",
        env!("CARGO_PKG_VERSION"),
        worker_count
    );

    let server = KoraServer::new(config);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // Handle Ctrl+C
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        tracing::info!("Received shutdown signal");
        let _ = shutdown_tx.send(true);
    });

    server.run(shutdown_rx).await?;

    Ok(())
}
