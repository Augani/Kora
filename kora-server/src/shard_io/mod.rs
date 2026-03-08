pub(crate) mod connection;
pub(crate) mod dispatch;
pub(crate) mod memcache;

use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use kora_cdc::consumer::ConsumerGroupManager;
use kora_cdc::ring::CdcRing;
use kora_core::command::{Command, CommandResponse};
use kora_core::hash::shard_for_key;
use kora_core::shard::{command_to_wal_record, ShardStore, WalWriter};
use kora_core::tenant::{TenantConfig, TenantId, TenantRegistry};
use kora_core::types::Value;
use kora_observability::histogram::CommandHistograms;
use kora_protocol::{MemcacheResponse, MemcacheStoreMode};
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
    ExecuteMemcache {
        request: MemcacheRequest,
        response_tx: oneshot::Sender<MemcacheResponse>,
    },
    ExecuteBatch {
        commands: Vec<(usize, Command)>,
        response_tx: oneshot::Sender<Vec<(usize, CommandResponse)>>,
    },
    BgSave {
        request: SnapshotRequest,
        response_tx: oneshot::Sender<Result<(), String>>,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct SnapshotRequest {
    pub data_dir: PathBuf,
    pub write_latest: bool,
    pub backup_name: Option<String>,
    pub retain_backups: Option<usize>,
}

#[derive(Debug, Clone)]
pub(crate) enum MemcacheRequest {
    Get {
        key: Vec<u8>,
    },
    Store {
        mode: MemcacheStoreMode,
        key: Vec<u8>,
        flags: u32,
        ttl_seconds: Option<u64>,
        value: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
    },
    Incr {
        key: Vec<u8>,
        value: u64,
    },
    Decr {
        key: Vec<u8>,
        value: u64,
    },
    Touch {
        key: Vec<u8>,
        ttl_seconds: Option<u64>,
    },
}

impl MemcacheRequest {
    fn key(&self) -> &[u8] {
        match self {
            MemcacheRequest::Get { key }
            | MemcacheRequest::Delete { key }
            | MemcacheRequest::Incr { key, .. }
            | MemcacheRequest::Decr { key, .. }
            | MemcacheRequest::Touch { key, .. }
            | MemcacheRequest::Store { key, .. } => key,
        }
    }
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
    pub memcached_bind_address: Option<String>,
    pub started_at: Instant,
    pub memcache_total_connections: AtomicU64,
    pub memcache_current_connections: AtomicUsize,
    pub memcache_cmd_get: AtomicU64,
    pub memcache_cmd_set: AtomicU64,
    pub memcache_get_hits: AtomicU64,
    pub memcache_get_misses: AtomicU64,
    pub snapshot_in_progress: AtomicBool,
    pub next_conn_id: AtomicU64,
    pub router: ShardRouter,
    #[allow(dead_code)]
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
        bind_address: String,
        memcached_bind_address: Option<String>,
        unix_socket: Option<PathBuf>,
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
            memcached_bind_address,
            started_at: Instant::now(),
            memcache_total_connections: AtomicU64::new(0),
            memcache_current_connections: AtomicUsize::new(0),
            memcache_cmd_get: AtomicU64::new(0),
            memcache_cmd_set: AtomicU64::new(0),
            memcache_get_hits: AtomicU64::new(0),
            memcache_get_misses: AtomicU64::new(0),
            snapshot_in_progress: AtomicBool::new(false),
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
            let bind_addr = bind_address.clone();
            let memcached_bind_addr = shared.memcached_bind_address.clone();
            let unix_path = if i == 0 { unix_socket.clone() } else { None };

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
                            &bind_addr,
                            memcached_bind_addr.as_deref(),
                            unix_path.as_ref(),
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

    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) -> std::io::Result<()> {
        if self.metrics_port > 0 {
            let shared = self.shared.clone();
            let port = self.metrics_port;
            tokio::spawn(async move {
                if let Err(e) = run_metrics_affinity(port, shared).await {
                    tracing::error!("Metrics server error: {}", e);
                }
            });
        }

        if let Some(interval_secs) = self
            .shared
            .storage_config
            .as_ref()
            .and_then(|config| {
                config
                    .rdb_enabled
                    .then_some(config.snapshot_interval_secs)
                    .flatten()
            })
            .filter(|interval| *interval > 0)
        {
            let shared = self.shared.clone();
            let mut scheduler_shutdown = self.shutdown_tx.subscribe();
            tokio::spawn(async move {
                run_periodic_snapshot_scheduler(shared, interval_secs, &mut scheduler_shutdown)
                    .await;
            });
        }

        let _ = shutdown.changed().await;
        tracing::info!("Shutting down server (shard-affinity mode)");
        let _ = self.shutdown_tx.send(true);

        let threads: Vec<_> = self.shard_threads.lock().drain(..).collect();
        for handle in threads {
            let _ = handle.join();
        }

        Ok(())
    }
}

const EXPIRE_SWEEP_INTERVAL_OPS: u32 = 4096;
const EXPIRE_SWEEP_SAMPLE_SIZE: usize = 64;

fn create_reuseport_listener(addr: &str) -> std::io::Result<tokio::net::TcpListener> {
    let socket_addr: std::net::SocketAddr = addr
        .parse()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    let socket = if socket_addr.is_ipv6() {
        tokio::net::TcpSocket::new_v6()?
    } else {
        tokio::net::TcpSocket::new_v4()?
    };
    socket.set_reuseaddr(true)?;
    #[cfg(not(windows))]
    socket.set_reuseport(true)?;
    socket.bind(socket_addr)?;
    socket.listen(65535)
}

fn spawn_connection_handler<
    S: tokio::io::AsyncReadExt + tokio::io::AsyncWriteExt + Unpin + 'static,
>(
    stream: S,
    shard_id: u16,
    store: &Rc<RefCell<ShardStore>>,
    wal_writer: &Rc<RefCell<Option<Box<dyn WalWriter>>>>,
    router: &ShardRouter,
    shared: &Arc<AffinitySharedState>,
    conn_counts: &Arc<[AtomicUsize]>,
) {
    let conn_id = shared.next_conn_id.fetch_add(1, Ordering::Relaxed);
    let store = store.clone();
    let wal = wal_writer.clone();
    let router = router.clone();
    let shared = shared.clone();
    let counts = conn_counts.clone();
    let shard_idx = shard_id as usize;

    counts[shard_idx].fetch_add(1, Ordering::Relaxed);

    tokio::task::spawn_local(async move {
        let result = connection::handle_connection_affinity(
            stream, shard_id, store, wal, router, shared, conn_id,
        )
        .await;

        if let Err(e) = result {
            if e.kind() != std::io::ErrorKind::UnexpectedEof
                && e.kind() != std::io::ErrorKind::ConnectionReset
            {
                tracing::debug!("Connection {} error: {}", conn_id, e);
            }
        }

        counts[shard_idx].fetch_sub(1, Ordering::Relaxed);
    });
}

fn spawn_memcache_connection_handler<
    S: tokio::io::AsyncReadExt + tokio::io::AsyncWriteExt + Unpin + 'static,
>(
    stream: S,
    shard_id: u16,
    store: &Rc<RefCell<ShardStore>>,
    wal_writer: &Rc<RefCell<Option<Box<dyn WalWriter>>>>,
    router: &ShardRouter,
    shared: &Arc<AffinitySharedState>,
) {
    shared
        .memcache_total_connections
        .fetch_add(1, Ordering::Relaxed);
    shared
        .memcache_current_connections
        .fetch_add(1, Ordering::Relaxed);

    let store = store.clone();
    let wal = wal_writer.clone();
    let router = router.clone();
    let shared = shared.clone();

    tokio::task::spawn_local(async move {
        let result = memcache::handle_connection_affinity(
            stream,
            shard_id,
            store,
            wal,
            router,
            shared.clone(),
        )
        .await;

        if let Err(e) = result {
            if e.kind() != std::io::ErrorKind::UnexpectedEof
                && e.kind() != std::io::ErrorKind::ConnectionReset
            {
                tracing::debug!("Memcached connection error: {}", e);
            }
        }

        shared
            .memcache_current_connections
            .fetch_sub(1, Ordering::Relaxed);
    });
}

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
    bind_address: &str,
    memcached_bind_address: Option<&str>,
    unix_socket: Option<&PathBuf>,
) {
    let listener = match create_reuseport_listener(bind_address) {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("Shard {} failed to bind {}: {}", shard_id, bind_address, e);
            return;
        }
    };

    if shard_id == 0 {
        tracing::info!("Kōra listening on {} (shard-affinity mode)", bind_address);
    }

    let memcached_listener = if let Some(addr) = memcached_bind_address {
        match create_reuseport_listener(addr) {
            Ok(listener) => {
                if shard_id == 0 {
                    tracing::info!("Kōra memcached listener on {}", addr);
                }
                Some(listener)
            }
            Err(e) => {
                if shard_id == 0 {
                    tracing::error!("Failed to bind memcached listener {}: {}", addr, e);
                }
                None
            }
        }
    } else {
        None
    };

    let unix_listener: Option<tokio::net::UnixListener> = if let Some(path) = unix_socket {
        if shard_id == 0 {
            let _ = std::fs::remove_file(path);
            match tokio::net::UnixListener::bind(path) {
                Ok(ul) => {
                    tracing::info!("Kōra listening on unix:{}", path.display());
                    Some(ul)
                }
                Err(e) => {
                    tracing::error!("Failed to bind unix socket: {}", e);
                    None
                }
            }
        } else {
            None
        }
    } else {
        None
    };

    let mut ops_since_expire = 0u32;

    loop {
        tokio::select! {
            result = listener.accept() => {
                let Ok((stream, _addr)) = result else { continue };
                let _ = stream.set_nodelay(true);
                spawn_connection_handler(
                    stream, shard_id, &store, &wal_writer, &router, &shared, &conn_counts,
                );
            }
            result = async {
                match &memcached_listener {
                    Some(listener) => listener.accept().await,
                    None => std::future::pending().await,
                }
            } => {
                let Ok((stream, _addr)) = result else { continue };
                let _ = stream.set_nodelay(true);
                spawn_memcache_connection_handler(
                    stream, shard_id, &store, &wal_writer, &router, &shared,
                );
            }
            result = async {
                match &unix_listener {
                    Some(ul) => ul.accept().await.map(|(s, _)| s),
                    None => std::future::pending().await,
                }
            } => {
                if let Ok(stream) = result {
                    spawn_connection_handler(
                        stream, shard_id, &store, &wal_writer, &router, &shared, &conn_counts,
                    );
                }
            }
            req = rx.recv() => {
                let Some(req) = req else { break };
                match req {
                    ShardRequest::Execute { command, response_tx } => {
                        maybe_sweep_inline(&store, &mut ops_since_expire);
                        let resp = execute_with_wal_inline(&store, &wal_writer, command);
                        let _ = response_tx.send(resp);
                    }
                    ShardRequest::ExecuteMemcache { request, response_tx } => {
                        maybe_sweep_inline(&store, &mut ops_since_expire);
                        let resp = memcache::execute_memcache_inline(
                            &store,
                            &wal_writer,
                            &shared,
                            request,
                        );
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
                    ShardRequest::BgSave { request, response_tx } => {
                        let entries = store_to_rdb_entries(&store.borrow());
                        let shard_dir = request.data_dir.join(format!("shard-{}", shard_id));
                        let result = save_snapshot_files(&shard_dir, &entries, &request)
                            .map_err(|e| e.to_string());
                        if result.is_ok() {
                            tracing::info!(
                                "Shard {} RDB snapshot saved: {} entries",
                                shard_id, entries.len()
                            );
                        }
                        let _ = response_tx.send(result);
                    }
                }
            }
            _ = shutdown.changed() => {
                break;
            }
        }
    }
}

pub(crate) fn start_manual_snapshot(shared: &Arc<AffinitySharedState>) -> Result<(), String> {
    let config = shared
        .storage_config
        .clone()
        .ok_or_else(|| "ERR persistence not configured".to_string())?;
    if !config.rdb_enabled {
        return Err("ERR RDB snapshots disabled".into());
    }
    start_snapshot(
        shared,
        SnapshotRequest {
            data_dir: config.data_dir,
            write_latest: true,
            backup_name: None,
            retain_backups: None,
        },
        "manual",
    )
}

fn start_snapshot(
    shared: &Arc<AffinitySharedState>,
    request: SnapshotRequest,
    trigger: &'static str,
) -> Result<(), String> {
    if shared
        .snapshot_in_progress
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return Err("ERR snapshot already in progress".into());
    }

    let mut receivers = Vec::with_capacity(shared.router.shard_count());
    for sender in shared.router.shard_senders.iter() {
        let (tx, rx) = oneshot::channel();
        if sender
            .try_send(ShardRequest::BgSave {
                request: request.clone(),
                response_tx: tx,
            })
            .is_err()
        {
            shared.snapshot_in_progress.store(false, Ordering::SeqCst);
            return Err("ERR shard unavailable".into());
        }
        receivers.push(rx);
    }

    let shared = shared.clone();
    tokio::spawn(async move {
        let mut errors = Vec::new();
        for rx in receivers {
            match rx.await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => errors.push(err),
                Err(_) => errors.push("snapshot response channel closed".into()),
            }
        }

        if errors.is_empty() {
            tracing::info!("{} snapshot completed", trigger);
        } else {
            tracing::error!("{} snapshot failed: {}", trigger, errors.join("; "));
        }
        shared.snapshot_in_progress.store(false, Ordering::SeqCst);
    });

    Ok(())
}

async fn run_periodic_snapshot_scheduler(
    shared: Arc<AffinitySharedState>,
    interval_secs: u64,
    shutdown: &mut watch::Receiver<bool>,
) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval.tick().await;

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let Some(config) = shared.storage_config.clone() else { break };
                let backup_name = if config.snapshot_retain == Some(0) {
                    None
                } else {
                    Some(generate_snapshot_name())
                };
                let request = SnapshotRequest {
                    data_dir: config.data_dir,
                    write_latest: true,
                    backup_name,
                    retain_backups: config.snapshot_retain,
                };
                if let Err(err) = start_snapshot(&shared, request, "scheduled") {
                    tracing::warn!("Scheduled snapshot skipped: {}", err);
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
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

fn save_snapshot_files(
    shard_dir: &std::path::Path,
    entries: &[RdbEntry],
    request: &SnapshotRequest,
) -> std::io::Result<()> {
    std::fs::create_dir_all(shard_dir)?;

    if request.write_latest {
        let rdb_path = shard_dir.join("shard.rdb");
        rdb::save(&rdb_path, entries).map_err(to_io_error)?;
    }

    if let Some(ref backup_name) = request.backup_name {
        let backup_dir = shard_dir.join("snapshots");
        std::fs::create_dir_all(&backup_dir)?;
        let backup_path = backup_dir.join(format!("{}.rdb", backup_name));
        rdb::save(&backup_path, entries).map_err(to_io_error)?;
        if let Some(retain) = request.retain_backups {
            prune_snapshot_backups(&backup_dir, retain)?;
        }
    }

    Ok(())
}

fn prune_snapshot_backups(backup_dir: &std::path::Path, retain: usize) -> std::io::Result<()> {
    if retain == 0 {
        return Ok(());
    }

    let mut entries: Vec<_> = std::fs::read_dir(backup_dir)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().map(|ft| ft.is_file()).unwrap_or(false))
        .collect();
    entries.sort_by_key(|entry| entry.file_name());

    let remove_count = entries.len().saturating_sub(retain);
    for entry in entries.into_iter().take(remove_count) {
        std::fs::remove_file(entry.path())?;
    }

    Ok(())
}

fn generate_snapshot_name() -> String {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("snapshot-{:020}", millis)
}

fn to_io_error(error: kora_storage::error::StorageError) -> std::io::Error {
    std::io::Error::other(error.to_string())
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
