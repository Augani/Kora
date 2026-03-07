//! # kora-server
//!
//! TCP/Unix socket server, connection handling, and command dispatch for Kōra.
//!
//! Accepts client connections, parses RESP commands via `kora-protocol`,
//! routes them to the appropriate shard in `kora-core`, and writes back responses.

#![warn(clippy::all)]

use std::collections::HashMap;
use std::hash::Hasher;
use std::sync::Arc;

use bytes::BytesMut;
use kora_cdc::ring::CdcRing;
use kora_core::command::{Command, CommandResponse};
use kora_core::shard::ShardEngine;
use kora_protocol::{parse_command, serialize_response_versioned, RespParser};
use kora_scripting::{FunctionRegistry, WasmRuntime};
use kora_storage::manager::{StorageConfig, StorageManager};
use kora_storage::wal::WalEntry;
use kora_vector::distance::DistanceMetric;
use kora_vector::hnsw::HnswIndex;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

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
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_address: "127.0.0.1:6379".into(),
            worker_count: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4),
            storage: None,
            cdc_capacity: 0,
            script_max_fuel: 0,
        }
    }
}

/// Shared server state accessible from connection handlers.
struct ServerState {
    engine: Arc<ShardEngine>,
    storage: Option<StorageManager>,
    cdc_rings: Vec<Mutex<CdcRing>>,
    scripts: Mutex<Option<FunctionRegistry>>,
    vectors: Mutex<HashMap<Vec<u8>, HnswIndex>>,
}

/// The Kōra TCP server.
pub struct KoraServer {
    state: Arc<ServerState>,
    bind_address: String,
}

impl KoraServer {
    /// Create a new server with the given configuration.
    pub fn new(config: ServerConfig) -> Self {
        let engine = Arc::new(ShardEngine::new(config.worker_count));
        Self::build(engine, config)
    }

    /// Create a server wrapping an existing engine.
    pub fn with_engine(engine: Arc<ShardEngine>, config: ServerConfig) -> Self {
        Self::build(engine, config)
    }

    fn build(engine: Arc<ShardEngine>, config: ServerConfig) -> Self {
        let storage =
            config
                .storage
                .as_ref()
                .and_then(|sc| match StorageManager::open(sc.clone()) {
                    Ok(mgr) => Some(mgr),
                    Err(e) => {
                        tracing::error!("Failed to open storage: {}", e);
                        None
                    }
                });

        let cdc_rings = if config.cdc_capacity > 0 {
            (0..config.worker_count)
                .map(|_| Mutex::new(CdcRing::new(config.cdc_capacity)))
                .collect()
        } else {
            Vec::new()
        };

        let scripts = if config.script_max_fuel > 0 {
            let runtime = Arc::new(WasmRuntime::new(config.script_max_fuel));
            Mutex::new(Some(FunctionRegistry::new(runtime)))
        } else {
            Mutex::new(None)
        };

        let bind_address = config.bind_address.clone();

        let state = Arc::new(ServerState {
            engine,
            storage,
            cdc_rings,
            scripts,
            vectors: Mutex::new(HashMap::new()),
        });

        Self {
            state,
            bind_address,
        }
    }

    /// Get a reference to the engine.
    pub fn engine(&self) -> &Arc<ShardEngine> {
        &self.state.engine
    }

    /// Run the server, accepting connections until the shutdown signal.
    pub async fn run(
        &self,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> std::io::Result<()> {
        let listener = TcpListener::bind(&self.bind_address).await?;
        tracing::info!("Kōra listening on {}", self.bind_address);

        loop {
            tokio::select! {
                result = listener.accept() => {
                    let (stream, addr) = result?;
                    let state = self.state.clone();
                    tracing::debug!("New connection from {}", addr);
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(stream, state).await {
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

/// Per-connection state tracking RESP protocol version.
struct ConnectionState {
    /// True if RESP3 mode, false for RESP2.
    resp3: bool,
}

async fn handle_connection(mut stream: TcpStream, state: Arc<ServerState>) -> std::io::Result<()> {
    let mut parser = RespParser::new();
    let mut write_buf = BytesMut::with_capacity(4096);
    let mut read_buf = vec![0u8; 8192];
    let mut conn = ConnectionState { resp3: false };

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
                        Ok(cmd) => handle_command(cmd, &state, &mut conn).await,
                        Err(e) => CommandResponse::Error(e.to_string()),
                    };
                    serialize_response_versioned(&response, &mut write_buf, conn.resp3);
                }
                Ok(None) => break, // Need more data
                Err(e) => {
                    let err_resp = CommandResponse::Error(e.to_string());
                    serialize_response_versioned(&err_resp, &mut write_buf, conn.resp3);
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

/// Handle a single command, dispatching to engine or handling server-level commands.
async fn handle_command(
    cmd: Command,
    state: &ServerState,
    conn: &mut ConnectionState,
) -> CommandResponse {
    // Server-level commands that don't go to shards
    match &cmd {
        Command::Hello { version } => {
            return handle_hello(version, conn);
        }
        Command::Auth { .. } => {
            // Stub: accept any auth for now (multi-tenancy Phase 10)
            return CommandResponse::Ok;
        }
        Command::BgSave => {
            return handle_bgsave(state).await;
        }
        Command::BgRewriteAof => {
            if let Some(ref storage) = state.storage {
                if let Err(e) = storage.wal_truncate() {
                    return CommandResponse::Error(format!("ERR {}", e));
                }
            }
            return CommandResponse::SimpleString("Background AOF rewrite started".into());
        }
        Command::CommandInfo => {
            return CommandResponse::Array(vec![]);
        }
        Command::CdcPoll { cursor, count } => {
            return handle_cdc_poll(state, *cursor, *count);
        }
        Command::ScriptLoad { name, wasm_bytes } => {
            return handle_script_load(state, name, wasm_bytes);
        }
        Command::ScriptCall { name, args } => {
            return handle_script_call(state, name, args);
        }
        Command::ScriptDel { name } => {
            return handle_script_del(state, name);
        }
        Command::VecSet {
            key,
            dimensions,
            vector,
        } => {
            return handle_vec_set(state, key, *dimensions, vector);
        }
        Command::VecQuery { key, k, vector } => {
            return handle_vec_query(state, key, *k, vector);
        }
        Command::VecDel { key } => {
            return handle_vec_del(state, key);
        }
        _ => {}
    }

    // WAL logging for mutations
    if cmd.is_mutation() {
        if let Some(ref storage) = state.storage {
            if let Some(wal_entry) = command_to_wal_entry(&cmd) {
                if let Err(e) = storage.wal_append(&wal_entry) {
                    tracing::error!("WAL append failed: {}", e);
                }
            }
        }
    }

    // Dispatch to shard engine
    let engine = state.engine.clone();
    let rx = engine.dispatch(cmd);
    match tokio::task::spawn_blocking(move || rx.recv()).await {
        Ok(Ok(resp)) => resp,
        _ => CommandResponse::Error("ERR internal error".into()),
    }
}

/// Handle the HELLO command for RESP3 negotiation.
fn handle_hello(version: &Option<u8>, conn: &mut ConnectionState) -> CommandResponse {
    let ver = version.unwrap_or(2);
    match ver {
        2 => {
            conn.resp3 = false;
        }
        3 => {
            conn.resp3 = true;
        }
        _ => {
            return CommandResponse::Error(format!(
                "NOPROTO unsupported protocol version: {}",
                ver
            ));
        }
    }

    // Return server info as a map
    let info = vec![
        (
            CommandResponse::SimpleString("server".into()),
            CommandResponse::SimpleString("kora".into()),
        ),
        (
            CommandResponse::SimpleString("version".into()),
            CommandResponse::SimpleString("0.1.0".into()),
        ),
        (
            CommandResponse::SimpleString("proto".into()),
            CommandResponse::Integer(ver as i64),
        ),
    ];
    CommandResponse::Map(info)
}

/// Handle BGSAVE by collecting all shard data and triggering RDB save.
async fn handle_bgsave(state: &ServerState) -> CommandResponse {
    let storage = match state.storage {
        Some(ref s) => s,
        None => return CommandResponse::Error("ERR persistence not configured".into()),
    };

    if storage.is_snapshot_in_progress() {
        return CommandResponse::Error("ERR snapshot already in progress".into());
    }

    // Collect entries from all shards via Dump command
    let engine = state.engine.clone();
    let dump_resp =
        match tokio::task::spawn_blocking(move || engine.dispatch_blocking(Command::Dump)).await {
            Ok(resp) => resp,
            Err(_) => return CommandResponse::Error("ERR dump failed".into()),
        };

    // Convert dump response to RDB entries
    match dump_resp {
        CommandResponse::Array(entries) => {
            let mut rdb_entries = Vec::new();
            for entry in entries.chunks(2) {
                if entry.len() == 2 {
                    if let (CommandResponse::BulkString(key), CommandResponse::BulkString(value)) =
                        (&entry[0], &entry[1])
                    {
                        rdb_entries.push(kora_storage::rdb::RdbEntry {
                            key: key.clone(),
                            value: kora_storage::rdb::RdbValue::String(value.clone()),
                            ttl_ms: None,
                        });
                    }
                }
            }
            if let Err(e) = storage.rdb_save(&rdb_entries) {
                return CommandResponse::Error(format!("ERR rdb save failed: {}", e));
            }
            CommandResponse::SimpleString("Background saving started".into())
        }
        _ => CommandResponse::Error("ERR dump returned unexpected response".into()),
    }
}

/// Handle CDCPOLL by reading from all shard CDC rings.
fn handle_cdc_poll(state: &ServerState, cursor: u64, count: usize) -> CommandResponse {
    if state.cdc_rings.is_empty() {
        return CommandResponse::Error("ERR CDC not enabled".into());
    }

    let mut all_events = Vec::new();
    let mut max_next_seq = cursor;

    for ring in &state.cdc_rings {
        let ring = ring.lock();
        let result = ring.read(cursor, count);
        for event in result.events {
            all_events.push(CommandResponse::Array(vec![
                CommandResponse::Integer(event.seq as i64),
                CommandResponse::BulkString(format!("{:?}", event.op).into_bytes()),
                CommandResponse::BulkString(event.key),
            ]));
        }
        if result.next_seq > max_next_seq {
            max_next_seq = result.next_seq;
        }
    }

    // Return [next_cursor, [events...]]
    CommandResponse::Array(vec![
        CommandResponse::Integer(max_next_seq as i64),
        CommandResponse::Array(all_events),
    ])
}

/// Handle SCRIPTLOAD.
fn handle_script_load(state: &ServerState, name: &[u8], wasm_bytes: &[u8]) -> CommandResponse {
    let mut scripts = state.scripts.lock();
    let registry = match scripts.as_mut() {
        Some(r) => r,
        None => return CommandResponse::Error("ERR scripting not enabled".into()),
    };

    let name_str = String::from_utf8_lossy(name);
    match registry.register(&name_str, wasm_bytes) {
        Ok(()) => CommandResponse::Ok,
        Err(e) => CommandResponse::Error(format!("ERR {}", e)),
    }
}

/// Handle SCRIPTCALL.
fn handle_script_call(state: &ServerState, name: &[u8], args: &[i64]) -> CommandResponse {
    let scripts = state.scripts.lock();
    let registry = match scripts.as_ref() {
        Some(r) => r,
        None => return CommandResponse::Error("ERR scripting not enabled".into()),
    };

    let name_str = String::from_utf8_lossy(name);
    match registry.call(&name_str, args) {
        Ok(results) => {
            let items: Vec<CommandResponse> =
                results.into_iter().map(CommandResponse::Integer).collect();
            CommandResponse::Array(items)
        }
        Err(e) => CommandResponse::Error(format!("ERR {}", e)),
    }
}

/// Handle SCRIPTDEL.
fn handle_script_del(state: &ServerState, name: &[u8]) -> CommandResponse {
    let mut scripts = state.scripts.lock();
    let registry = match scripts.as_mut() {
        Some(r) => r,
        None => return CommandResponse::Error("ERR scripting not enabled".into()),
    };

    let name_str = String::from_utf8_lossy(name);
    if registry.remove(&name_str) {
        CommandResponse::Integer(1)
    } else {
        CommandResponse::Integer(0)
    }
}

/// Handle VECSET — insert a vector into a named index.
fn handle_vec_set(
    state: &ServerState,
    key: &[u8],
    dimensions: usize,
    vector: &[f32],
) -> CommandResponse {
    let mut indexes = state.vectors.lock();
    let index = indexes
        .entry(key.to_vec())
        .or_insert_with(|| HnswIndex::new(dimensions, DistanceMetric::L2, 16, 200));

    if index.dim() != dimensions {
        return CommandResponse::Error(format!(
            "ERR dimension mismatch: index has {}, got {}",
            index.dim(),
            dimensions
        ));
    }

    let id = {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        for &v in vector {
            hasher.write(&v.to_le_bytes());
        }
        hasher.finish()
    };

    index.insert(id, vector);
    CommandResponse::Integer(id as i64)
}

/// Handle VECQUERY — search nearest neighbors in a named index.
fn handle_vec_query(state: &ServerState, key: &[u8], k: usize, vector: &[f32]) -> CommandResponse {
    let indexes = state.vectors.lock();
    let index = match indexes.get(key) {
        Some(idx) => idx,
        None => return CommandResponse::Array(vec![]),
    };

    let results = index.search(vector, k, k.max(50));
    let items: Vec<CommandResponse> = results
        .into_iter()
        .map(|r| {
            CommandResponse::Array(vec![
                CommandResponse::Integer(r.id as i64),
                CommandResponse::BulkString(format!("{}", r.distance).into_bytes()),
            ])
        })
        .collect();
    CommandResponse::Array(items)
}

/// Handle VECDEL — delete a vector index.
fn handle_vec_del(state: &ServerState, key: &[u8]) -> CommandResponse {
    let mut indexes = state.vectors.lock();
    if indexes.remove(key).is_some() {
        CommandResponse::Integer(1)
    } else {
        CommandResponse::Integer(0)
    }
}

/// Convert a Command to a WalEntry for logging, if applicable.
fn command_to_wal_entry(cmd: &Command) -> Option<WalEntry> {
    match cmd {
        Command::Set {
            key, value, ex, px, ..
        } => {
            let ttl_ms = if let Some(s) = ex {
                Some(s * 1000)
            } else {
                *px
            };
            Some(WalEntry::Set {
                key: key.clone(),
                value: value.clone(),
                ttl_ms,
            })
        }
        Command::SetNx { key, value } => Some(WalEntry::Set {
            key: key.clone(),
            value: value.clone(),
            ttl_ms: None,
        }),
        Command::GetSet { key, value } => Some(WalEntry::Set {
            key: key.clone(),
            value: value.clone(),
            ttl_ms: None,
        }),
        Command::Append { key, value } => Some(WalEntry::Set {
            key: key.clone(),
            value: value.clone(),
            ttl_ms: None,
        }),
        Command::Del { keys } => {
            // Log first key; in practice, multi-key DEL is fanned out per-shard
            keys.first().map(|key| WalEntry::Del { key: key.clone() })
        }
        Command::Expire { key, seconds } => Some(WalEntry::Expire {
            key: key.clone(),
            ttl_ms: seconds * 1000,
        }),
        Command::PExpire { key, millis } => Some(WalEntry::Expire {
            key: key.clone(),
            ttl_ms: *millis,
        }),
        Command::LPush { key, values } => Some(WalEntry::LPush {
            key: key.clone(),
            values: values.clone(),
        }),
        Command::RPush { key, values } => Some(WalEntry::RPush {
            key: key.clone(),
            values: values.clone(),
        }),
        Command::HSet { key, fields } => Some(WalEntry::HSet {
            key: key.clone(),
            fields: fields.clone(),
        }),
        Command::SAdd { key, members } => Some(WalEntry::SAdd {
            key: key.clone(),
            members: members.clone(),
        }),
        Command::FlushDb | Command::FlushAll => Some(WalEntry::FlushDb),
        _ => None,
    }
}
