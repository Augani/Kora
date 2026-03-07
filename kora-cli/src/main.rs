//! Kōra — A multi-threaded, embeddable, memory-safe cache engine.
//!
//! This is the CLI entrypoint that starts the Kōra server.

use clap::Parser;
use kora_server::{KoraServer, ServerConfig};

/// Kōra — A multi-threaded, embeddable, memory-safe cache engine.
#[derive(Parser)]
#[command(name = "kora", version, about)]
struct Args {
    /// Address to bind to.
    #[arg(long, default_value = "127.0.0.1")]
    bind: String,

    /// Port to listen on.
    #[arg(short, long, default_value_t = 6379)]
    port: u16,

    /// Number of worker threads (defaults to CPU count).
    #[arg(short, long)]
    workers: Option<usize>,

    /// Log level (trace, debug, info, warn, error).
    #[arg(long, default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&args.log_level)),
        )
        .init();

    let worker_count = args.workers.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
    });

    let config = ServerConfig {
        bind_address: format!("{}:{}", args.bind, args.port),
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
