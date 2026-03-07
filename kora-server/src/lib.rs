//! # kora-server
//!
//! TCP/Unix socket server, connection handling, and command dispatch for Kōra.
//!
//! Accepts client connections, parses RESP commands via `kora-protocol`,
//! routes them to the appropriate shard in `kora-core`, and writes back responses.

#![warn(clippy::all)]

use std::sync::Arc;

use bytes::BytesMut;
use kora_core::command::CommandResponse;
use kora_core::shard::ShardEngine;
use kora_protocol::{parse_command, serialize_response, RespParser};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Configuration for the Kōra server.
pub struct ServerConfig {
    /// Address to bind to (e.g., "127.0.0.1:6379").
    pub bind_address: String,
    /// Number of shard worker threads.
    pub worker_count: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_address: "127.0.0.1:6379".into(),
            worker_count: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4),
        }
    }
}

/// The Kōra TCP server.
pub struct KoraServer {
    engine: Arc<ShardEngine>,
    config: ServerConfig,
}

impl KoraServer {
    /// Create a new server with the given configuration.
    pub fn new(config: ServerConfig) -> Self {
        let engine = Arc::new(ShardEngine::new(config.worker_count));
        Self { engine, config }
    }

    /// Create a server wrapping an existing engine.
    pub fn with_engine(engine: Arc<ShardEngine>, config: ServerConfig) -> Self {
        Self { engine, config }
    }

    /// Get a reference to the engine.
    pub fn engine(&self) -> &Arc<ShardEngine> {
        &self.engine
    }

    /// Run the server, accepting connections until the shutdown signal.
    pub async fn run(
        &self,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> std::io::Result<()> {
        let listener = TcpListener::bind(&self.config.bind_address).await?;
        tracing::info!("Kōra listening on {}", self.config.bind_address);

        loop {
            tokio::select! {
                result = listener.accept() => {
                    let (stream, addr) = result?;
                    let engine = self.engine.clone();
                    tracing::debug!("New connection from {}", addr);
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(stream, engine).await {
                            tracing::debug!("Connection {} error: {}", addr, e);
                        }
                    });
                }
                _ = shutdown.changed() => {
                    tracing::info!("Shutting down server");
                    break;
                }
            }
        }

        Ok(())
    }
}

async fn handle_connection(mut stream: TcpStream, engine: Arc<ShardEngine>) -> std::io::Result<()> {
    let mut parser = RespParser::new();
    let mut write_buf = BytesMut::with_capacity(4096);
    let mut read_buf = vec![0u8; 8192];

    loop {
        let n = stream.read(&mut read_buf).await?;
        if n == 0 {
            return Ok(()); // Client disconnected
        }

        parser.feed(&read_buf[..n]);

        loop {
            match parser.try_parse() {
                Ok(Some(frame)) => {
                    let response = match parse_command(frame) {
                        Ok(cmd) => {
                            let rx = engine.dispatch(cmd);
                            // Use spawn_blocking to avoid blocking the tokio runtime
                            // on the synchronous crossbeam channel recv.
                            match tokio::task::spawn_blocking(move || rx.recv()).await {
                                Ok(Ok(resp)) => resp,
                                _ => CommandResponse::Error("ERR internal error".into()),
                            }
                        }
                        Err(e) => CommandResponse::Error(e.to_string()),
                    };
                    serialize_response(&response, &mut write_buf);
                }
                Ok(None) => break, // Need more data
                Err(e) => {
                    let err_resp = CommandResponse::Error(e.to_string());
                    serialize_response(&err_resp, &mut write_buf);
                    break;
                }
            }
        }

        if !write_buf.is_empty() {
            stream.write_all(&write_buf).await?;
            write_buf.clear();
        }
    }
}
