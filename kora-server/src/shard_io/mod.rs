pub(crate) mod connection;
pub(crate) mod dispatch;

use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

use kora_cdc::consumer::ConsumerGroupManager;
use kora_cdc::ring::CdcRing;
use kora_core::command::{Command, CommandResponse};
use kora_core::hash::shard_for_key;
use kora_core::shard::{command_to_wal_record, ShardStore, WalWriter};
use kora_core::tenant::{TenantConfig, TenantId, TenantRegistry};
use kora_core::types::Value;
use kora_observability::histogram::CommandHistograms;
use kora_pubsub::PubSubBroker;
use kora_scripting::{FunctionRegistry, WasmRuntime};
use kora_storage::manager::StorageConfig;
use kora_storage::rdb::{self, RdbEntry, RdbValue};
use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot, watch};

pub(crate) const CROSS_SHARD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

#[allow(dead_code)]
pub(crate) enum ShardRequest {
    Execute {
        command: Command,
        response_tx: oneshot::Sender<CommandResponse>,
    },
    ExecuteBatch {
        commands: Vec<(usize, Command)>,
        response_tx: oneshot::Sender<Vec<(usize, CommandResponse)>>,
    },
    NewConnection(NewConnectionMsg),
    BgSave {
        data_dir: PathBuf,
        response_tx: oneshot::Sender<Result<(), String>>,
    },
}

pub(crate) struct NewConnectionMsg {
    pub stream: RawStream,
    pub conn_id: u64,
}

pub(crate) enum RawStream {
    Tcp(std::net::TcpStream),
    #[cfg(unix)]
    Unix(std::os::unix::net::UnixStream),
}

#[derive(Clone)]
pub(crate) struct ShardRouter {
    pub(crate) shard_senders: Arc<[mpsc::Sender<ShardRequest>]>,
    shard_count: usize,
}

impl ShardRouter {
    pub fn shard_for_key(&self, key: &[u8]) -> u16 {
        shard_for_key(key, self.shard_count)
    }

    pub fn shard_count(&self) -> usize {
        self.shard_count
    }

    #[allow(dead_code)]
    pub async fn dispatch_foreign(&self, shard_id: u16, cmd: Command) -> CommandResponse {
        let (tx, rx) = oneshot::channel();
        if self.shard_senders[shard_id as usize]
            .send(ShardRequest::Execute {
                command: cmd,
                response_tx: tx,
            })
            .await
            .is_err()
        {
            return CommandResponse::Error("ERR shard unavailable".into());
        }
        match tokio::time::timeout(CROSS_SHARD_TIMEOUT, rx).await {
            Ok(Ok(resp)) => resp,
            Ok(Err(_)) => CommandResponse::Error("ERR shard channel closed".into()),
            Err(_) => CommandResponse::Error("ERR shard request timeout".into()),
        }
    }

    pub fn try_dispatch_foreign(
        &self,
        shard_id: u16,
        cmd: Command,
    ) -> Result<oneshot::Receiver<CommandResponse>, CommandResponse> {
        let (tx, rx) = oneshot::channel();
        self.shard_senders[shard_id as usize]
            .try_send(ShardRequest::Execute {
                command: cmd,
                response_tx: tx,
            })
            .map_err(|_| CommandResponse::Error("ERR shard unavailable".into()))?;
        Ok(rx)
    }

    pub fn try_dispatch_foreign_broadcast<F>(
        &self,
        exclude_shard: u16,
        cmd_factory: F,
    ) -> Vec<oneshot::Receiver<CommandResponse>>
    where
        F: Fn() -> Command,
    {
        let mut receivers = Vec::with_capacity(self.shard_count - 1);
        for (i, sender) in self.shard_senders.iter().enumerate() {
            if i as u16 == exclude_shard {
                continue;
            }
            let (tx, rx) = oneshot::channel();
            let _ = sender.try_send(ShardRequest::Execute {
                command: cmd_factory(),
                response_tx: tx,
            });
            receivers.push(rx);
        }
        receivers
    }

    pub async fn dispatch_broadcast<F>(&self, cmd_factory: F) -> Vec<CommandResponse>
    where
        F: Fn() -> Command,
    {
        let mut receivers = Vec::with_capacity(self.shard_count);
        for sender in self.shard_senders.iter() {
            let (tx, rx) = oneshot::channel();
            let _ = sender
                .send(ShardRequest::Execute {
                    command: cmd_factory(),
                    response_tx: tx,
                })
                .await;
            receivers.push(rx);
        }
        let mut results = Vec::with_capacity(self.shard_count);
        for rx in receivers {
            match tokio::time::timeout(CROSS_SHARD_TIMEOUT, rx).await {
                Ok(Ok(resp)) => results.push(resp),
                _ => results.push(CommandResponse::Error("ERR shard error".into())),
            }
        }
        results
    }
}

pub(crate) struct AffinitySharedState {
    pub pub_sub: PubSubBroker,
    pub cdc_rings: Vec<Mutex<CdcRing>>,
    pub cdc_groups: Mutex<ConsumerGroupManager>,
    pub scripts: Mutex<Option<FunctionRegistry>>,
    pub histograms: Arc<CommandHistograms>,
    pub track_latency: AtomicBool,
    pub latency_sums_ns: Arc<[AtomicU64; 32]>,
    pub tenants: TenantRegistry,
    pub auth_password: Option<String>,
    pub tenant_limits_enabled: bool,
    pub storage_config: Option<StorageConfig>,
    pub next_conn_id: AtomicU64,
    pub router: ShardRouter,
    pub conn_counts: Arc<[AtomicUsize]>,
}

pub(crate) struct ShardIoEngine {
    pub shared: Arc<AffinitySharedState>,
    shutdown_tx: watch::Sender<bool>,
    shard_threads: Mutex<Vec<thread::JoinHandle<()>>>,
    metrics_port: u16,
}

impl ShardIoEngine {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        shard_count: usize,
        wal_writers: Vec<Option<Box<dyn WalWriter>>>,
        cdc_capacity: usize,
        script_max_fuel: u64,
        metrics_port: u16,
        auth_password: Option<String>,
        tenant_limits_enabled: bool,
        storage_config: Option<StorageConfig>,
    ) -> Self {
        assert_eq!(wal_writers.len(), shard_count);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let mut shard_senders = Vec::with_capacity(shard_count);
        let mut shard_receivers = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            let (tx, rx) = mpsc::channel(4096);
            shard_senders.push(tx);
            shard_receivers.push(rx);
        }

        let router = ShardRouter {
            shard_senders: shard_senders.into(),
            shard_count,
        };

        let conn_counts: Arc<[AtomicUsize]> = (0..shard_count)
            .map(|_| AtomicUsize::new(0))
            .collect::<Vec<_>>()
            .into();

        let cdc_rings = if cdc_capacity > 0 {
            (0..shard_count)
                .map(|_| Mutex::new(CdcRing::new(cdc_capacity)))
                .collect()
        } else {
            Vec::new()
        };

        let scripts = if script_max_fuel > 0 {
            let runtime = Arc::new(WasmRuntime::new(script_max_fuel));
            Mutex::new(Some(FunctionRegistry::new(runtime)))
        } else {
            Mutex::new(None)
        };

        let histograms = Arc::new(CommandHistograms::new());
        let latency_sums_ns = Arc::new(std::array::from_fn(|_| AtomicU64::new(0)));

        let mut tenant_reg = TenantRegistry::new();
        tenant_reg.register(TenantId(0), TenantConfig::default());

        let pub_sub = PubSubBroker::new(shard_count);

        let shared = Arc::new(AffinitySharedState {
            pub_sub,
            cdc_rings,
            cdc_groups: Mutex::new(ConsumerGroupManager::default()),
            scripts,
            histograms,
            track_latency: AtomicBool::new(metrics_port > 0),
            latency_sums_ns,
            tenants: tenant_reg,
            auth_password,
            tenant_limits_enabled,
            storage_config,
            next_conn_id: AtomicU64::new(1),
            router: router.clone(),
            conn_counts: conn_counts.clone(),
        });

        let mut thread_handles = Vec::with_capacity(shard_count);
        for (i, (rx, wal_writer)) in shard_receivers
            .into_iter()
            .zip(wal_writers.into_iter())
            .enumerate()
        {
            let shared = shared.clone();
            let router = router.clone();
            let conn_counts = conn_counts.clone();
            let shutdown_rx = shutdown_rx.clone();

            let handle = thread::Builder::new()
                .name(format!("kora-shard-io-{}", i))
                .spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("failed to create shard tokio runtime");

                    let local = tokio::task::LocalSet::new();
                    local.block_on(&rt, async move {
                        let store = Rc::new(RefCell::new(ShardStore::new(i as u16)));
                        let wal = Rc::new(RefCell::new(wal_writer));
                        shard_worker_loop(
                            i as u16,
                            store,
                            wal,
                            rx,
                            router,
                            shared,
                            conn_counts,
                            shutdown_rx,
                        )
                        .await;
                    });
                })
                .expect("failed to spawn shard-io thread");

            thread_handles.push(handle);
        }

        ShardIoEngine {
            shared,
            shutdown_tx,
            shard_threads: Mutex::new(thread_handles),
            metrics_port,
        }
    }

    fn least_loaded_shard(&self) -> usize {
        let counts = &self.shared.conn_counts;
        let mut min_idx = 0;
        let mut min_count = counts[0].load(Ordering::Relaxed);
        for i in 1..counts.len() {
            let count = counts[i].load(Ordering::Relaxed);
            if count < min_count {
                min_count = count;
                min_idx = i;
            }
        }
        min_idx
    }

    pub async fn run(
        &self,
        bind_address: &str,
        unix_socket: Option<&PathBuf>,
        mut shutdown: watch::Receiver<bool>,
    ) -> std::io::Result<()> {
        let listener = tokio::net::TcpListener::bind(bind_address).await?;
        tracing::info!("Kōra listening on {} (shard-affinity mode)", bind_address);

        let unix_listener = if let Some(path) = unix_socket {
            let _ = std::fs::remove_file(path);
            let ul = tokio::net::UnixListener::bind(path)?;
            tracing::info!("Kōra listening on unix:{}", path.display());
            Some(ul)
        } else {
            None
        };

        if self.metrics_port > 0 {
            let shared = self.shared.clone();
            let port = self.metrics_port;
            tokio::spawn(async move {
                if let Err(e) = run_metrics_affinity(port, shared).await {
                    tracing::error!("Metrics server error: {}", e);
                }
            });
        }

        loop {
            tokio::select! {
                result = listener.accept() => {
                    let (stream, addr) = result?;
                    let conn_id = self.shared.next_conn_id.fetch_add(1, Ordering::Relaxed);
                    let shard_id = self.least_loaded_shard();
                    tracing::debug!("New TCP connection from {} -> shard {}", addr, shard_id);
                    if let Ok(std_stream) = stream.into_std() {
                        let _ = self.shared.router.shard_senders[shard_id]
                            .try_send(ShardRequest::NewConnection(NewConnectionMsg {
                                stream: RawStream::Tcp(std_stream),
                                conn_id,
                            }));
                    }
                }
                result = async {
                    match &unix_listener {
                        Some(ul) => ul.accept().await.map(|(s, _)| s),
                        None => std::future::pending().await,
                    }
                } => {
                    let stream = result?;
                    let conn_id = self.shared.next_conn_id.fetch_add(1, Ordering::Relaxed);
                    let shard_id = self.least_loaded_shard();
                    if let Ok(std_stream) = stream.into_std() {
                        let _ = self.shared.router.shard_senders[shard_id]
                            .try_send(ShardRequest::NewConnection(NewConnectionMsg {
                                stream: RawStream::Unix(std_stream),
                                conn_id,
                            }));
                    }
                }
                _ = shutdown.changed() => {
                    tracing::info!("Shutting down server (shard-affinity mode)");
                    let _ = self.shutdown_tx.send(true);
                    break;
                }
            }
        }

        let threads: Vec<_> = self.shard_threads.lock().drain(..).collect();
        for handle in threads {
            let _ = handle.join();
        }

        Ok(())
    }
}

const EXPIRE_SWEEP_INTERVAL_OPS: u32 = 4096;
const EXPIRE_SWEEP_SAMPLE_SIZE: usize = 64;

#[allow(clippy::too_many_arguments)]
async fn shard_worker_loop(
    shard_id: u16,
    store: Rc<RefCell<ShardStore>>,
    wal_writer: Rc<RefCell<Option<Box<dyn WalWriter>>>>,
    mut rx: mpsc::Receiver<ShardRequest>,
    router: ShardRouter,
    shared: Arc<AffinitySharedState>,
    conn_counts: Arc<[AtomicUsize]>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut ops_since_expire = 0u32;

    loop {
        tokio::select! {
            req = rx.recv() => {
                let Some(req) = req else { break };
                match req {
                    ShardRequest::Execute { command, response_tx } => {
                        maybe_sweep_inline(&store, &mut ops_since_expire);
                        let resp = execute_with_wal_inline(&store, &wal_writer, command);
                        let _ = response_tx.send(resp);
                    }
                    ShardRequest::ExecuteBatch { commands, response_tx } => {
                        let results: Vec<_> = commands
                            .into_iter()
                            .map(|(idx, cmd)| {
                                maybe_sweep_inline(&store, &mut ops_since_expire);
                                let resp = execute_with_wal_inline(&store, &wal_writer, cmd);
                                (idx, resp)
                            })
                            .collect();
                        let _ = response_tx.send(results);
                    }
                    ShardRequest::BgSave { data_dir, response_tx } => {
                        let entries = store_to_rdb_entries(&store.borrow());
                        let shard_dir = data_dir.join(format!("shard-{}", shard_id));
                        let _ = std::fs::create_dir_all(&shard_dir);
                        let rdb_path = shard_dir.join("shard.rdb");
                        let result = rdb::save(&rdb_path, &entries)
                            .map_err(|e| e.to_string());
                        if result.is_ok() {
                            tracing::info!(
                                "Shard {} RDB snapshot saved: {} entries",
                                shard_id, entries.len()
                            );
                        }
                        let _ = response_tx.send(result);
                    }
                    ShardRequest::NewConnection(conn_msg) => {
                        let store = store.clone();
                        let wal = wal_writer.clone();
                        let router = router.clone();
                        let shared = shared.clone();
                        let counts = conn_counts.clone();
                        let sid = shard_id;

                        tokio::task::spawn_local(async move {
                            counts[sid as usize].fetch_add(1, Ordering::Relaxed);
                            match conn_msg.stream {
                                RawStream::Tcp(std_stream) => {
                                    if let Ok(stream) = tokio::net::TcpStream::from_std(std_stream)
                                    {
                                        let _ = stream.set_nodelay(true);
                                        let _ = connection::handle_connection_affinity(
                                            stream,
                                            sid,
                                            store,
                                            wal,
                                            router,
                                            shared,
                                            conn_msg.conn_id,
                                        )
                                        .await;
                                    }
                                }
                                #[cfg(unix)]
                                RawStream::Unix(std_stream) => {
                                    if let Ok(stream) =
                                        tokio::net::UnixStream::from_std(std_stream)
                                    {
                                        let _ = connection::handle_connection_affinity(
                                            stream,
                                            sid,
                                            store,
                                            wal,
                                            router,
                                            shared,
                                            conn_msg.conn_id,
                                        )
                                        .await;
                                    }
                                }
                            }
                            counts[sid as usize].fetch_sub(1, Ordering::Relaxed);
                        });
                    }
                }
            }
            _ = shutdown.changed() => {
                break;
            }
        }
    }
}

pub(crate) fn execute_with_wal_inline(
    store: &Rc<RefCell<ShardStore>>,
    wal_writer: &Rc<RefCell<Option<Box<dyn WalWriter>>>>,
    command: Command,
) -> CommandResponse {
    {
        let mut wal = wal_writer.borrow_mut();
        if let Some(ref mut writer) = *wal {
            if command.is_mutation() {
                if let Some(record) = command_to_wal_record(&command) {
                    writer.append(&record);
                }
            }
        }
    }
    store.borrow_mut().execute(command)
}

fn maybe_sweep_inline(store: &Rc<RefCell<ShardStore>>, ops_since_expire: &mut u32) {
    *ops_since_expire += 1;
    if *ops_since_expire >= EXPIRE_SWEEP_INTERVAL_OPS {
        let _ = store
            .borrow_mut()
            .evict_expired_sample(EXPIRE_SWEEP_SAMPLE_SIZE);
        *ops_since_expire = 0;
    }
}

fn store_to_rdb_entries(store: &ShardStore) -> Vec<RdbEntry> {
    let mut entries = Vec::new();
    let now = std::time::Instant::now();
    for (key, entry) in store.entries_iter() {
        let ttl_ms = entry.ttl.and_then(|exp| {
            let remaining = exp.saturating_duration_since(now);
            if remaining.is_zero() {
                None
            } else {
                Some(remaining.as_millis() as u64)
            }
        });
        if let Some(rdb_val) = value_to_rdb(&entry.value) {
            entries.push(RdbEntry {
                key: key.as_bytes().to_vec(),
                value: rdb_val,
                ttl_ms,
            });
        }
    }
    entries
}

fn value_to_rdb(value: &Value) -> Option<RdbValue> {
    match value {
        Value::InlineStr { data, len } => Some(RdbValue::String(data[..*len as usize].to_vec())),
        Value::HeapStr(arc) => Some(RdbValue::String(arc.to_vec())),
        Value::Int(i) => Some(RdbValue::Int(*i)),
        Value::List(deque) => {
            let items: Vec<Vec<u8>> = deque.iter().filter_map(|v| v.as_bytes()).collect();
            Some(RdbValue::List(items))
        }
        Value::Set(set) => {
            let items: Vec<Vec<u8>> = set.iter().filter_map(|v| v.as_bytes()).collect();
            Some(RdbValue::Set(items))
        }
        Value::Hash(map) => {
            let pairs: Vec<(Vec<u8>, Vec<u8>)> = map
                .iter()
                .filter_map(|(k, v)| v.as_bytes().map(|vb| (k.as_bytes().to_vec(), vb)))
                .collect();
            Some(RdbValue::Hash(pairs))
        }
        _ => None,
    }
}

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use kora_observability::prometheus::format_metrics;
use kora_observability::stats::StatsSnapshot;
use std::convert::Infallible;

async fn run_metrics_affinity(
    port: u16,
    shared: Arc<AffinitySharedState>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: std::net::SocketAddr = ([0, 0, 0, 0], port).into();
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("Prometheus metrics endpoint on :{}", port);

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let shared = shared.clone();

        tokio::spawn(async move {
            let service = service_fn(move |_req: Request<hyper::body::Incoming>| {
                let shared = shared.clone();
                async move {
                    let snapshot = build_affinity_metrics(&shared).await;
                    let body = format_metrics(&snapshot, &shared.histograms);
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

async fn build_affinity_metrics(shared: &AffinitySharedState) -> StatsSnapshot {
    let mut cmd_counts = [0u64; 32];
    let mut cmd_durations_ns = [0u64; 32];
    let mut total_commands = 0u64;
    for i in 0..32 {
        let count = shared.histograms.count(i);
        cmd_counts[i] = count;
        cmd_durations_ns[i] = shared.latency_sums_ns[i].load(Ordering::Relaxed);
        total_commands = total_commands.saturating_add(count);
    }

    let responses = shared.router.dispatch_broadcast(|| Command::DbSize).await;
    let mut key_count = 0u64;
    for resp in responses {
        if let CommandResponse::Integer(n) = resp {
            if n > 0 {
                key_count += n as u64;
            }
        }
    }

    StatsSnapshot {
        total_commands,
        cmd_counts,
        cmd_durations_ns,
        key_count,
        memory_used: 0,
        bytes_in: 0,
        bytes_out: 0,
    }
}
