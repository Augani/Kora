//! # kora-server
//!
//! TCP/Unix socket server, connection handling, and command dispatch for Kōra.
//!
//! Accepts client connections, parses RESP commands via `kora-protocol`,
//! routes them to the appropriate shard in `kora-core`, and writes back responses.

#![warn(clippy::all)]

use std::convert::Infallible;
use std::sync::Arc;

use bytes::BytesMut;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use kora_cdc::consumer::ConsumerGroupManager;
use kora_cdc::ring::CdcRing;
use kora_core::command::{Command, CommandResponse};
use kora_core::shard::{ShardEngine, WalWriter};
use kora_core::tenant::{TenantConfig, TenantId, TenantRegistry};
use kora_observability::histogram::CommandHistograms;
use kora_observability::prometheus::format_metrics;
use kora_observability::trie::PrefixTrie;
use kora_protocol::{parse_command, serialize_response_versioned, RespParser};
use kora_scripting::{FunctionRegistry, WasmRuntime};
use kora_storage::manager::StorageConfig;
use kora_storage::shard_storage::ShardStorage;
use parking_lot::{Mutex, RwLock};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UnixListener};

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
            metrics_port: 0,
            unix_socket: None,
            password: None,
        }
    }
}

/// Shared server state accessible from connection handlers.
struct ServerState {
    engine: Arc<ShardEngine>,
    storage_config: Option<StorageConfig>,
    cdc_rings: Vec<Mutex<CdcRing>>,
    cdc_groups: Mutex<ConsumerGroupManager>,
    scripts: Mutex<Option<FunctionRegistry>>,
    histograms: Arc<CommandHistograms>,
    memory_trie: Arc<PrefixTrie>,
    tenants: RwLock<TenantRegistry>,
    auth_password: Option<String>,
}

/// The Kōra TCP server.
pub struct KoraServer {
    state: Arc<ServerState>,
    bind_address: String,
    unix_socket: Option<std::path::PathBuf>,
}

/// Create per-shard WAL writers from server configuration.
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
        let engine = Arc::new(ShardEngine::new_with_storage(
            config.worker_count,
            wal_writers,
        ));
        Self::build(engine, config)
    }

    /// Create a server wrapping an existing engine.
    ///
    /// The engine should already have per-shard storage configured.
    pub fn with_engine(engine: Arc<ShardEngine>, config: ServerConfig) -> Self {
        Self::build(engine, config)
    }

    fn build(engine: Arc<ShardEngine>, config: ServerConfig) -> Self {
        let cdc_rings = if config.cdc_capacity > 0 {
            (0..config.worker_count)
                .map(|_| Mutex::new(CdcRing::new(config.cdc_capacity)))
                .collect()
        } else {
            Vec::new()
        };

        let scripts = if config.script_max_fuel > 0 {
            let runtime = Arc::new(WasmRuntime::new(config.script_max_fuel));
            let mut registry = FunctionRegistry::new(runtime);
            if let Err(e) = registry.set_engine(engine.clone()) {
                tracing::error!("Failed to attach engine to scripting: {}", e);
            }
            Mutex::new(Some(registry))
        } else {
            Mutex::new(None)
        };

        let bind_address = config.bind_address.clone();
        let unix_socket = config.unix_socket.clone();
        let auth_password = config.password.clone();
        let metrics_port = config.metrics_port;
        let histograms = Arc::new(CommandHistograms::new());
        let memory_trie = Arc::new(PrefixTrie::new());

        let mut tenant_reg = TenantRegistry::new();
        tenant_reg.register(TenantId(0), TenantConfig::default());

        let state = Arc::new(ServerState {
            engine,
            storage_config: config.storage,
            cdc_rings,
            cdc_groups: Mutex::new(ConsumerGroupManager::default()),
            scripts,
            histograms,
            memory_trie,
            tenants: RwLock::new(tenant_reg),
            auth_password,
        });

        if metrics_port > 0 {
            let metrics_state = state.clone();
            tokio::spawn(async move {
                if let Err(e) = run_metrics_server(metrics_port, metrics_state).await {
                    tracing::error!("Metrics server error: {}", e);
                }
            });
        }

        Self {
            state,
            bind_address,
            unix_socket,
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

        let unix_listener = if let Some(ref path) = self.unix_socket {
            let _ = std::fs::remove_file(path);
            let ul = UnixListener::bind(path)?;
            tracing::info!("Kōra listening on unix:{}", path.display());
            Some(ul)
        } else {
            None
        };

        loop {
            tokio::select! {
                result = listener.accept() => {
                    let (stream, addr) = result?;
                    let state = self.state.clone();
                    tracing::debug!("New TCP connection from {}", addr);
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(stream, state).await {
                            tracing::debug!("Connection {} error: {}", addr, e);
                        }
                    });
                }
                result = async {
                    match &unix_listener {
                        Some(ul) => ul.accept().await,
                        None => std::future::pending().await,
                    }
                } => {
                    let (stream, _addr) = result?;
                    let state = self.state.clone();
                    tracing::debug!("New Unix socket connection");
                    tokio::spawn(async move {
                        if let Err(e) = handle_unix_connection(stream, state).await {
                            tracing::debug!("Unix connection error: {}", e);
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

struct ConnectionState {
    resp3: bool,
    tenant_id: TenantId,
}

use kora_core::shard::ResponseReceiver;

enum PendingResponse {
    Ready(CommandResponse),
    Dispatched {
        rx: ResponseReceiver,
        cmd_id: usize,
        key: Option<Vec<u8>>,
    },
}

async fn handle_stream<S: AsyncReadExt + AsyncWriteExt + Unpin>(
    mut stream: S,
    state: Arc<ServerState>,
) -> std::io::Result<()> {
    let mut parser = RespParser::new();
    let mut write_buf = BytesMut::with_capacity(4096);
    let mut read_buf = vec![0u8; 8192];
    let mut conn = ConnectionState {
        resp3: false,
        tenant_id: TenantId(0),
    };

    loop {
        let n = stream.read(&mut read_buf).await?;
        if n == 0 {
            return Ok(());
        }

        parser.feed(&read_buf[..n]);

        let mut pending: Vec<PendingResponse> = Vec::new();
        let mut has_dispatched = false;

        loop {
            match parser.try_parse() {
                Ok(Some(frame)) => match parse_command(frame) {
                    Ok(cmd) => {
                        if let Some(resp) = try_handle_local(&cmd, &state, &mut conn) {
                            pending.push(PendingResponse::Ready(resp));
                        } else {
                            match prepare_dispatch(&cmd, &state, &conn) {
                                Ok((rx, cmd_id, key)) => {
                                    has_dispatched = true;
                                    pending.push(PendingResponse::Dispatched { rx, cmd_id, key });
                                }
                                Err(resp) => {
                                    pending.push(PendingResponse::Ready(resp));
                                }
                            }
                        }
                    }
                    Err(e) => {
                        pending.push(PendingResponse::Ready(CommandResponse::Error(
                            e.to_string(),
                        )));
                    }
                },
                Ok(None) => break,
                Err(e) => {
                    pending.push(PendingResponse::Ready(CommandResponse::Error(
                        e.to_string(),
                    )));
                    break;
                }
            }
        }

        if has_dispatched {
            let histograms = state.histograms.clone();
            let memory_trie = state.memory_trie.clone();
            let resolved = tokio::task::spawn_blocking(move || {
                let mut responses = Vec::with_capacity(pending.len());
                for item in pending {
                    match item {
                        PendingResponse::Ready(resp) => responses.push(resp),
                        PendingResponse::Dispatched { rx, cmd_id, key } => {
                            let start = std::time::Instant::now();
                            let resp = rx
                                .recv()
                                .unwrap_or(CommandResponse::Error("ERR internal error".into()));
                            let duration_ns = start.elapsed().as_nanos() as u64;
                            histograms.record(cmd_id, duration_ns);
                            if let Some(k) = key {
                                memory_trie.track(&k, 1);
                            }
                            responses.push(resp);
                        }
                    }
                }
                responses
            })
            .await
            .unwrap_or_default();

            for resp in &resolved {
                serialize_response_versioned(resp, &mut write_buf, conn.resp3);
            }
        } else {
            for item in &pending {
                if let PendingResponse::Ready(resp) = item {
                    serialize_response_versioned(resp, &mut write_buf, conn.resp3);
                }
            }
        }

        if !write_buf.is_empty() {
            stream.write_all(&write_buf).await?;
            write_buf.clear();
        }
    }
}

async fn handle_connection(stream: TcpStream, state: Arc<ServerState>) -> std::io::Result<()> {
    handle_stream(stream, state).await
}

async fn handle_unix_connection(
    stream: tokio::net::UnixStream,
    state: Arc<ServerState>,
) -> std::io::Result<()> {
    handle_stream(stream, state).await
}

fn try_handle_local(
    cmd: &Command,
    state: &ServerState,
    conn: &mut ConnectionState,
) -> Option<CommandResponse> {
    match cmd {
        Command::Hello { version } => Some(handle_hello(version, conn)),
        Command::Auth { tenant, password } => {
            if let Some(ref expected) = state.auth_password {
                let provided = String::from_utf8_lossy(password);
                if provided.as_ref() != expected.as_str() {
                    return Some(CommandResponse::Error("ERR invalid password".into()));
                }
            }
            if let Some(ref tenant_bytes) = tenant {
                if let Ok(s) = std::str::from_utf8(tenant_bytes) {
                    if let Ok(id) = s.parse::<u32>() {
                        conn.tenant_id = TenantId(id);
                    }
                }
            }
            Some(CommandResponse::Ok)
        }
        Command::BgSave => Some(handle_bgsave_sync(state)),
        Command::BgRewriteAof => Some(CommandResponse::SimpleString(
            "Background AOF rewrite started".into(),
        )),
        Command::CommandInfo => Some(CommandResponse::Array(vec![])),
        Command::CdcPoll { cursor, count } => Some(handle_cdc_poll(state, *cursor, *count)),
        Command::CdcGroupCreate {
            group, start_seq, ..
        } => Some(handle_cdc_group_create(state, group, *start_seq)),
        Command::CdcGroupRead {
            group,
            consumer,
            count,
            ..
        } => Some(handle_cdc_group_read(state, group, consumer, *count)),
        Command::CdcAck { group, seqs, .. } => Some(handle_cdc_ack(state, group, seqs)),
        Command::CdcPending { group, .. } => Some(handle_cdc_pending(state, group)),
        Command::ScriptLoad { name, wasm_bytes } => {
            Some(handle_script_load(state, name, wasm_bytes))
        }
        Command::ScriptCall {
            name,
            args,
            byte_args,
        } => Some(handle_script_call(state, name, args, byte_args)),
        Command::ScriptDel { name } => Some(handle_script_del(state, name)),
        Command::StatsHotkeys { count } => Some(handle_stats_hotkeys(state, *count)),
        Command::StatsLatency {
            command,
            percentiles,
        } => Some(handle_stats_latency(state, command, percentiles)),
        Command::StatsMemory { prefixes } => Some(handle_stats_memory(state, prefixes)),
        _ => None,
    }
}

fn prepare_dispatch(
    cmd: &Command,
    state: &ServerState,
    conn: &ConnectionState,
) -> Result<(ResponseReceiver, usize, Option<Vec<u8>>), CommandResponse> {
    {
        let registry = state.tenants.read();
        let key_count_delta = if cmd.is_mutation() { 1 } else { 0 };
        if let Err(e) = registry.check_limits(conn.tenant_id, key_count_delta, 0) {
            return Err(CommandResponse::Error(format!("ERR {}", e)));
        }
        registry.record_operation(conn.tenant_id);
    }

    let cmd_id = cmd.cmd_type() as usize;
    let key_clone = cmd.key().map(|k| k.to_vec());
    let rx = state.engine.dispatch(cmd.clone());
    Ok((rx, cmd_id, key_clone))
}

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

fn handle_bgsave_sync(state: &ServerState) -> CommandResponse {
    let sc = match state.storage_config {
        Some(ref s) => s.clone(),
        None => return CommandResponse::Error("ERR persistence not configured".into()),
    };

    let engine = state.engine.clone();
    let shard_count = engine.shard_count();

    let mut errors = Vec::new();
    let dump_resp = engine.dispatch_blocking(Command::Dump);

    let all_entries = match dump_resp {
        CommandResponse::Array(entries) => entries,
        _ => return CommandResponse::Error("ERR dump returned unexpected response".into()),
    };

    let mut rdb_entries = Vec::new();
    for chunk in all_entries.chunks(2) {
        if chunk.len() == 2 {
            if let (CommandResponse::BulkString(key), CommandResponse::BulkString(value)) =
                (&chunk[0], &chunk[1])
            {
                rdb_entries.push(kora_storage::rdb::RdbEntry {
                    key: key.clone(),
                    value: kora_storage::rdb::RdbValue::String(value.clone()),
                    ttl_ms: None,
                });
            }
        }
    }

    for shard_id in 0..shard_count {
        let shard_entries: Vec<_> = rdb_entries
            .iter()
            .filter(|e| kora_core::hash::shard_for_key(&e.key, shard_count) as usize == shard_id)
            .cloned()
            .collect();

        let mut shard_storage = match ShardStorage::open_with_config(
            shard_id as u16,
            &sc.data_dir,
            sc.wal_sync_policy,
            sc.wal_enabled,
            sc.rdb_enabled,
            sc.wal_max_bytes,
        ) {
            Ok(s) => s,
            Err(e) => {
                errors.push(format!("shard {}: {}", shard_id, e));
                continue;
            }
        };

        if let Err(e) = shard_storage.rdb_save(&shard_entries) {
            errors.push(format!("shard {}: {}", shard_id, e));
        }
    }

    if errors.is_empty() {
        CommandResponse::SimpleString("Background saving started".into())
    } else {
        CommandResponse::Error(format!("ERR partial save: {}", errors.join("; ")))
    }
}

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

    CommandResponse::Array(vec![
        CommandResponse::Integer(max_next_seq as i64),
        CommandResponse::Array(all_events),
    ])
}

fn handle_cdc_group_create(state: &ServerState, group: &str, start_seq: u64) -> CommandResponse {
    if state.cdc_rings.is_empty() {
        return CommandResponse::Error("ERR CDC not enabled".into());
    }
    let mut mgr = state.cdc_groups.lock();
    match mgr.create_group(group, start_seq) {
        Ok(()) => CommandResponse::Ok,
        Err(e) => CommandResponse::Error(e.to_string()),
    }
}

fn handle_cdc_group_read(
    state: &ServerState,
    group: &str,
    consumer: &str,
    count: usize,
) -> CommandResponse {
    if state.cdc_rings.is_empty() {
        return CommandResponse::Error("ERR CDC not enabled".into());
    }

    let mut mgr = state.cdc_groups.lock();
    let mut all_events = Vec::new();
    let mut any_gap = false;

    for ring_mutex in &state.cdc_rings {
        let ring = ring_mutex.lock();
        match mgr.read_group(&ring, group, consumer, count) {
            Ok(result) => {
                if result.gap {
                    any_gap = true;
                }
                for event in result.events {
                    all_events.push(CommandResponse::Array(vec![
                        CommandResponse::Integer(event.seq as i64),
                        CommandResponse::BulkString(format!("{:?}", event.op).into_bytes()),
                        CommandResponse::BulkString(event.key),
                    ]));
                }
            }
            Err(e) => return CommandResponse::Error(e.to_string()),
        }
    }

    CommandResponse::Array(vec![
        CommandResponse::Boolean(any_gap),
        CommandResponse::Array(all_events),
    ])
}

fn handle_cdc_ack(state: &ServerState, group: &str, seqs: &[u64]) -> CommandResponse {
    let mut mgr = state.cdc_groups.lock();
    match mgr.ack(group, seqs) {
        Ok(count) => CommandResponse::Integer(count as i64),
        Err(e) => CommandResponse::Error(e.to_string()),
    }
}

fn handle_cdc_pending(state: &ServerState, group: &str) -> CommandResponse {
    let mgr = state.cdc_groups.lock();
    match mgr.pending(group) {
        Ok(entries) => {
            let items: Vec<CommandResponse> = entries
                .into_iter()
                .map(|p| {
                    CommandResponse::Array(vec![
                        CommandResponse::Integer(p.seq as i64),
                        CommandResponse::BulkString(p.consumer.into_bytes()),
                        CommandResponse::Integer(p.idle_ms as i64),
                        CommandResponse::Integer(p.delivery_count as i64),
                    ])
                })
                .collect();
            CommandResponse::Array(items)
        }
        Err(e) => CommandResponse::Error(e.to_string()),
    }
}

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

fn handle_script_call(
    state: &ServerState,
    name: &[u8],
    args: &[i64],
    byte_args: &[Vec<u8>],
) -> CommandResponse {
    let scripts = state.scripts.lock();
    let registry = match scripts.as_ref() {
        Some(r) => r,
        None => return CommandResponse::Error("ERR scripting not enabled".into()),
    };

    let name_str = String::from_utf8_lossy(name);
    let result = if !byte_args.is_empty() && registry.has_engine() {
        registry.call_with_byte_args(&name_str, byte_args)
    } else {
        registry.call(&name_str, args)
    };
    match result {
        Ok(results) => {
            let items: Vec<CommandResponse> =
                results.into_iter().map(CommandResponse::Integer).collect();
            CommandResponse::Array(items)
        }
        Err(e) => CommandResponse::Error(format!("ERR {}", e)),
    }
}

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

fn handle_stats_hotkeys(state: &ServerState, count: usize) -> CommandResponse {
    state
        .engine
        .dispatch_blocking(Command::StatsHotkeys { count })
}

fn handle_stats_latency(
    state: &ServerState,
    command: &[u8],
    percentiles: &[f64],
) -> CommandResponse {
    let cmd_str = String::from_utf8_lossy(command).to_uppercase();
    let cmd_id = command_name_to_id(&cmd_str);
    let values: Vec<CommandResponse> = percentiles
        .iter()
        .map(|p| {
            let nanos = state.histograms.percentile(cmd_id, *p);
            CommandResponse::BulkString(format!("{:.3}", nanos as f64 / 1_000_000.0).into_bytes())
        })
        .collect();
    CommandResponse::Array(values)
}

fn command_name_to_id(name: &str) -> usize {
    match name {
        "GET" => 0,
        "SET" => 1,
        "DEL" => 2,
        "INCR" | "INCRBY" => 3,
        "MGET" => 4,
        _ => 255,
    }
}

fn handle_stats_memory(state: &ServerState, prefixes: &[Vec<u8>]) -> CommandResponse {
    let items: Vec<CommandResponse> = prefixes
        .iter()
        .map(|prefix| {
            let count = state.memory_trie.query(prefix);
            CommandResponse::Integer(count)
        })
        .collect();
    CommandResponse::Array(items)
}

async fn run_metrics_server(
    port: u16,
    state: Arc<ServerState>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: std::net::SocketAddr = ([0, 0, 0, 0], port).into();
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("Prometheus metrics endpoint on :{}", port);

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let state = state.clone();

        tokio::spawn(async move {
            let service = service_fn(move |_req: Request<hyper::body::Incoming>| {
                let state = state.clone();
                async move {
                    let snapshot = kora_observability::stats::StatsSnapshot::merge(&[]);
                    let body = format_metrics(&snapshot, &state.histograms);
                    Ok::<_, Infallible>(
                        Response::builder()
                            .header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
                            .body(Full::new(Bytes::from(body)))
                            .unwrap_or_else(|_| {
                                Response::new(Full::new(Bytes::from("internal error")))
                            }),
                    )
                }
            });
            if let Err(e) = http1::Builder::new().serve_connection(io, service).await {
                tracing::debug!("Metrics connection error: {}", e);
            }
        });
    }
}
