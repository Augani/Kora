//! Realistic application-traffic comparison benchmark (Redis vs Kōra).
//!
//! This is an ignored integration test intended for manual performance runs:
//! `cargo test -p kora-server --test real_app_traffic --release -- --ignored --nocapture`

use std::io;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use kora_protocol::{RespParser, RespValue};
use kora_server::{KoraServer, ServerConfig};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const CMD_GET: &[u8] = b"GET";
const CMD_SET: &[u8] = b"SET";
const CMD_INCR: &[u8] = b"INCR";
const CMD_PUBLISH: &[u8] = b"PUBLISH";
const CMD_FLUSHDB: &[u8] = b"FLUSHDB";
const CMD_SUBSCRIBE: &[u8] = b"SUBSCRIBE";
const CMD_EX: &[u8] = b"EX";
const TTL_60: &[u8] = b"60";

const GET_WEIGHT: u32 = 600;
const SET_WEIGHT: u32 = 250;
const INCR_WEIGHT: u32 = 100;
const PUBLISH_WEIGHT: u32 = 50;
const TOTAL_WEIGHT: u32 = GET_WEIGHT + SET_WEIGHT + INCR_WEIGHT + PUBLISH_WEIGHT;

const DEFAULT_SECONDS: u64 = 15;
const DEFAULT_CLIENTS: usize = 64;
const DEFAULT_SUBSCRIBERS: usize = 24;
const DEFAULT_KEY_COUNT: usize = 200_000;
const DEFAULT_HOT_KEYS: usize = 20_000;
const DEFAULT_PRELOAD_KEYS: usize = 100_000;
const DEFAULT_CHANNELS: usize = 32;

#[derive(Clone, Copy)]
enum OpKind {
    Get = 0,
    Set = 1,
    Incr = 2,
    Publish = 3,
}

impl OpKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Set => "SET",
            Self::Incr => "INCR",
            Self::Publish => "PUBLISH",
        }
    }
}

#[derive(Clone)]
struct WorkloadConfig {
    duration: Duration,
    clients: usize,
    subscribers: usize,
    key_count: usize,
    hot_keys: usize,
    preload_keys: usize,
    channel_count: usize,
    kora_workers: usize,
    kora_runtime_workers: usize,
    redis_addr: Option<String>,
}

impl WorkloadConfig {
    fn from_env() -> Self {
        let duration_secs = env_usize("REAL_BENCH_SECONDS", DEFAULT_SECONDS as usize) as u64;
        let clients = env_usize("REAL_BENCH_CLIENTS", DEFAULT_CLIENTS);
        let subscribers = env_usize("REAL_BENCH_SUBSCRIBERS", DEFAULT_SUBSCRIBERS);
        let key_count = env_usize("REAL_BENCH_KEYS", DEFAULT_KEY_COUNT);
        let hot_keys = env_usize("REAL_BENCH_HOT_KEYS", DEFAULT_HOT_KEYS).min(key_count.max(1));
        let preload_keys =
            env_usize("REAL_BENCH_PRELOAD", DEFAULT_PRELOAD_KEYS).min(key_count.max(1));
        let channel_count = env_usize("REAL_BENCH_CHANNELS", DEFAULT_CHANNELS).max(1);
        let default_workers = std::thread::available_parallelism()
            .map(|n| (n.get() / 3).max(1))
            .unwrap_or(4);
        let kora_workers = env_usize("REAL_BENCH_KORA_WORKERS", default_workers).max(1);
        let default_runtime_workers = std::thread::available_parallelism()
            .map(|n| n.get().saturating_sub(kora_workers).max(1))
            .unwrap_or(4);
        let kora_runtime_workers = std::env::var("REAL_BENCH_KORA_RUNTIME_WORKERS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(default_runtime_workers)
            .max(1);
        let redis_addr = std::env::var("REAL_BENCH_REDIS_ADDR").ok();

        Self {
            duration: Duration::from_secs(duration_secs.max(1)),
            clients: clients.max(1),
            subscribers,
            key_count: key_count.max(1),
            hot_keys: hot_keys.max(1),
            preload_keys,
            channel_count,
            kora_workers,
            kora_runtime_workers,
            redis_addr,
        }
    }
}

struct WorkloadData {
    keys: Vec<Vec<u8>>,
    counters: Vec<Vec<u8>>,
    channels: Vec<Vec<u8>>,
    set_values: Vec<Vec<u8>>,
    publish_payloads: Vec<Vec<u8>>,
    hot_keys: usize,
}

impl WorkloadData {
    fn new(cfg: &WorkloadConfig) -> Self {
        let keys = (0..cfg.key_count)
            .map(|i| format!("app:key:{i}").into_bytes())
            .collect();
        let counters = (0..1024)
            .map(|i| format!("app:counter:{i}").into_bytes())
            .collect();
        let channels = (0..cfg.channel_count)
            .map(|i| format!("app:channel:{i}").into_bytes())
            .collect();

        let set_values = vec![vec![b'a'; 64], vec![b'b'; 256], vec![b'c'; 1024]];
        let publish_payloads = vec![vec![b'p'; 32], vec![b'q'; 96], vec![b'r'; 256]];

        Self {
            keys,
            counters,
            channels,
            set_values,
            publish_payloads,
            hot_keys: cfg.hot_keys,
        }
    }

    fn pick_key<'a>(&'a self, rng: &mut StdRng) -> &'a [u8] {
        // 80/20 style skew: 80% of accesses to hot-key subset.
        let hot = rng.gen_range(0..100) < 80;
        if hot {
            let idx = rng.gen_range(0..self.hot_keys);
            &self.keys[idx]
        } else {
            let idx = rng.gen_range(self.hot_keys..self.keys.len());
            &self.keys[idx]
        }
    }

    fn pick_counter<'a>(&'a self, rng: &mut StdRng) -> &'a [u8] {
        let idx = rng.gen_range(0..self.counters.len());
        &self.counters[idx]
    }

    fn pick_channel<'a>(&'a self, rng: &mut StdRng) -> &'a [u8] {
        let idx = rng.gen_range(0..self.channels.len());
        &self.channels[idx]
    }

    fn pick_set_value<'a>(&'a self, rng: &mut StdRng) -> &'a [u8] {
        let roll = rng.gen_range(0..100);
        if roll < 70 {
            &self.set_values[0]
        } else if roll < 90 {
            &self.set_values[1]
        } else {
            &self.set_values[2]
        }
    }

    fn pick_publish_payload<'a>(&'a self, rng: &mut StdRng) -> &'a [u8] {
        let idx = rng.gen_range(0..self.publish_payloads.len());
        &self.publish_payloads[idx]
    }
}

struct BenchConnection {
    stream: TcpStream,
    parser: RespParser,
    read_buf: [u8; 16 * 1024],
}

impl BenchConnection {
    async fn connect(addr: &str) -> io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(Self {
            stream,
            parser: RespParser::new(),
            read_buf: [0u8; 16 * 1024],
        })
    }

    async fn write_all(&mut self, data: &[u8]) -> io::Result<()> {
        self.stream.write_all(data).await
    }

    async fn read_value(&mut self) -> io::Result<RespValue> {
        loop {
            match self.parser.try_parse().map_err(protocol_err)? {
                Some(v) => return Ok(v),
                None => {
                    let n = self.stream.read(&mut self.read_buf).await?;
                    if n == 0 {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "connection closed",
                        ));
                    }
                    self.parser.feed(&self.read_buf[..n]);
                }
            }
        }
    }

    async fn send_single(&mut self, parts: &[&[u8]]) -> io::Result<RespValue> {
        let mut cmd = Vec::with_capacity(128);
        write_command(parts, &mut cmd);
        self.write_all(&cmd).await?;
        self.read_value().await
    }
}

#[derive(Debug)]
struct WorkerStats {
    ops: u64,
    errors: u64,
    op_counts: [u64; 4],
    latency_us: Histogram<u64>,
    op_latency_us: [Histogram<u64>; 4],
}

impl WorkerStats {
    fn new() -> Self {
        Self {
            ops: 0,
            errors: 0,
            op_counts: [0; 4],
            latency_us: Histogram::new_with_bounds(1, 120_000_000, 3).expect("histogram bounds"),
            op_latency_us: std::array::from_fn(|_| {
                Histogram::new_with_bounds(1, 120_000_000, 3).expect("histogram bounds")
            }),
        }
    }
}

#[derive(Debug)]
struct RunStats {
    ops: u64,
    errors: u64,
    op_counts: [u64; 4],
    latency_us: Histogram<u64>,
    op_latency_us: [Histogram<u64>; 4],
    elapsed: Duration,
}

impl RunStats {
    fn new(elapsed: Duration) -> Self {
        Self {
            ops: 0,
            errors: 0,
            op_counts: [0; 4],
            latency_us: Histogram::new_with_bounds(1, 120_000_000, 3).expect("histogram bounds"),
            op_latency_us: std::array::from_fn(|_| {
                Histogram::new_with_bounds(1, 120_000_000, 3).expect("histogram bounds")
            }),
            elapsed,
        }
    }

    fn absorb(&mut self, worker: WorkerStats) {
        self.ops += worker.ops;
        self.errors += worker.errors;
        for (dst, src) in self.op_counts.iter_mut().zip(worker.op_counts) {
            *dst += src;
        }
        let _ = self.latency_us.add(&worker.latency_us);
        for (dst, src) in self
            .op_latency_us
            .iter_mut()
            .zip(worker.op_latency_us.iter())
        {
            let _ = dst.add(src);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "manual benchmark: requires external Redis and release mode"]
async fn real_app_traffic_compare() {
    let cfg = WorkloadConfig::from_env();
    let mut redis_isolated = None;
    let redis_addr = if let Some(addr) = cfg.redis_addr.clone() {
        addr
    } else {
        match RedisIsolated::start().await {
            Some(server) => {
                let addr = server.addr.clone();
                redis_isolated = Some(server);
                addr
            }
            None => {
                eprintln!(
                    "Skipping real_app_traffic_compare: could not launch isolated redis-server \
                     (set REAL_BENCH_REDIS_ADDR to use an existing instance)"
                );
                return;
            }
        }
    };
    if BenchConnection::connect(&redis_addr).await.is_err() {
        eprintln!(
            "Skipping real_app_traffic_compare: Redis unavailable at {}",
            redis_addr
        );
        return;
    }

    let data = Arc::new(WorkloadData::new(&cfg));

    let kora_port = free_port().await;
    let kora_addr = format!("127.0.0.1:{kora_port}");
    let kora = start_kora_isolated(
        kora_addr.clone(),
        cfg.kora_workers,
        cfg.kora_runtime_workers,
    );
    tokio::time::sleep(Duration::from_millis(200)).await;

    println!("=== Real App Traffic Config ===");
    println!("duration={}s", cfg.duration.as_secs());
    println!("clients={}", cfg.clients);
    println!("subscribers={}", cfg.subscribers);
    println!("keys={} (hot={})", cfg.key_count, cfg.hot_keys);
    println!("preload_keys={}", cfg.preload_keys);
    println!("channels={}", cfg.channel_count);
    println!("kora_workers={}", cfg.kora_workers);
    println!("redis={}", redis_addr);
    println!("kora={}", kora_addr);

    let redis_stats = run_workload(&redis_addr, data.clone(), &cfg, 0xC0FFEE01)
        .await
        .expect("redis workload");
    let kora_stats = run_workload(&kora_addr, data.clone(), &cfg, 0xC0FFEE01)
        .await
        .expect("kora workload");

    kora.stop();
    if let Some(server) = redis_isolated {
        server.stop();
    }

    print_summary(&redis_stats, &kora_stats);
}

async fn run_workload(
    addr: &str,
    data: Arc<WorkloadData>,
    cfg: &WorkloadConfig,
    seed: u64,
) -> io::Result<RunStats> {
    flush_db(addr).await?;
    preload_keys(addr, &data, cfg.preload_keys).await?;

    let subscribers = spawn_subscribers(addr.to_string(), data.clone(), cfg.subscribers);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let start = Instant::now();
    let deadline = start + cfg.duration;

    let mut handles = Vec::with_capacity(cfg.clients);
    for worker_id in 0..cfg.clients {
        let addr = addr.to_string();
        let data = data.clone();
        let worker_seed = seed ^ ((worker_id as u64 + 1) * 0x9E37_79B9_7F4A_7C15);
        handles.push(tokio::spawn(async move {
            run_worker(addr, data, deadline, worker_seed).await
        }));
    }

    let mut stats = RunStats::new(start.elapsed());
    for handle in handles {
        if let Ok(worker) = handle.await {
            stats.absorb(worker);
        }
    }
    stats.elapsed = start.elapsed();

    for handle in subscribers {
        handle.abort();
    }

    Ok(stats)
}

async fn run_worker(
    addr: String,
    data: Arc<WorkloadData>,
    deadline: Instant,
    seed: u64,
) -> WorkerStats {
    let mut stats = WorkerStats::new();
    let mut conn = match BenchConnection::connect(&addr).await {
        Ok(conn) => conn,
        Err(_) => {
            stats.errors += 1;
            return stats;
        }
    };

    let mut rng = StdRng::seed_from_u64(seed);
    let mut write_buf = Vec::with_capacity(8 * 1024);
    let mut pending: Vec<(Instant, OpKind)> = Vec::with_capacity(8);

    while Instant::now() < deadline {
        write_buf.clear();
        pending.clear();

        let depth = choose_pipeline_depth(&mut rng);
        for _ in 0..depth {
            if Instant::now() >= deadline {
                break;
            }
            let start = Instant::now();
            let kind = encode_random_op(&data, &mut rng, &mut write_buf);
            pending.push((start, kind));
        }

        if pending.is_empty() {
            break;
        }

        if conn.write_all(&write_buf).await.is_err() {
            stats.errors += pending.len() as u64;
            break;
        }

        for (started_at, kind) in pending.drain(..) {
            match conn.read_value().await {
                Ok(resp) => {
                    if matches!(resp, RespValue::Error(_)) {
                        stats.errors += 1;
                    }
                    let latency_us = started_at.elapsed().as_micros().max(1) as u64;
                    let _ = stats.latency_us.record(latency_us);
                    stats.ops += 1;
                    stats.op_counts[kind as usize] += 1;
                    let _ = stats.op_latency_us[kind as usize].record(latency_us);
                }
                Err(_) => {
                    stats.errors += 1;
                    return stats;
                }
            }
        }
    }

    stats
}

fn encode_random_op(data: &WorkloadData, rng: &mut StdRng, out: &mut Vec<u8>) -> OpKind {
    let pick = rng.gen_range(0..TOTAL_WEIGHT);
    if pick < GET_WEIGHT {
        let key = data.pick_key(rng);
        write_command(&[CMD_GET, key], out);
        OpKind::Get
    } else if pick < GET_WEIGHT + SET_WEIGHT {
        let key = data.pick_key(rng);
        let value = data.pick_set_value(rng);
        if rng.gen_range(0..100) < 10 {
            write_command(&[CMD_SET, key, value, CMD_EX, TTL_60], out);
        } else {
            write_command(&[CMD_SET, key, value], out);
        }
        OpKind::Set
    } else if pick < GET_WEIGHT + SET_WEIGHT + INCR_WEIGHT {
        let key = data.pick_counter(rng);
        write_command(&[CMD_INCR, key], out);
        OpKind::Incr
    } else {
        let channel = data.pick_channel(rng);
        let payload = data.pick_publish_payload(rng);
        write_command(&[CMD_PUBLISH, channel, payload], out);
        OpKind::Publish
    }
}

fn choose_pipeline_depth(rng: &mut StdRng) -> usize {
    let roll = rng.gen_range(0..100);
    if roll < 45 {
        1
    } else if roll < 75 {
        2
    } else if roll < 93 {
        4
    } else {
        8
    }
}

async fn flush_db(addr: &str) -> io::Result<()> {
    let mut conn = BenchConnection::connect(addr).await?;
    let _ = conn.send_single(&[CMD_FLUSHDB]).await?;
    Ok(())
}

async fn preload_keys(addr: &str, data: &WorkloadData, count: usize) -> io::Result<()> {
    if count == 0 {
        return Ok(());
    }

    let mut conn = BenchConnection::connect(addr).await?;
    let mut write_buf = Vec::with_capacity(64 * 1024);
    let mut inserted = 0usize;
    let batch_size = 256usize;
    let value = &data.set_values[0];

    while inserted < count {
        write_buf.clear();
        let remaining = count - inserted;
        let batch = remaining.min(batch_size);
        for idx in inserted..inserted + batch {
            write_command(&[CMD_SET, &data.keys[idx], value], &mut write_buf);
        }
        conn.write_all(&write_buf).await?;
        for _ in 0..batch {
            let _ = conn.read_value().await?;
        }
        inserted += batch;
    }

    Ok(())
}

fn spawn_subscribers(
    addr: String,
    data: Arc<WorkloadData>,
    subscribers: usize,
) -> Vec<tokio::task::JoinHandle<()>> {
    let mut handles = Vec::with_capacity(subscribers);
    for idx in 0..subscribers {
        let addr = addr.clone();
        let data = data.clone();
        handles.push(tokio::spawn(async move {
            let mut conn = match BenchConnection::connect(&addr).await {
                Ok(c) => c,
                Err(_) => return,
            };

            let ch1 = &data.channels[idx % data.channels.len()];
            let ch2 = &data.channels[(idx * 7 + 3) % data.channels.len()];

            let mut cmd = Vec::with_capacity(128);
            write_command(&[CMD_SUBSCRIBE, ch1, ch2], &mut cmd);
            if conn.write_all(&cmd).await.is_err() {
                return;
            }

            // Consume initial subscription acknowledgements.
            for _ in 0..2 {
                if conn.read_value().await.is_err() {
                    return;
                }
            }

            while conn.read_value().await.is_ok() {}
        }));
    }
    handles
}

fn write_command(parts: &[&[u8]], out: &mut Vec<u8>) {
    out.push(b'*');
    append_usize(out, parts.len());
    out.extend_from_slice(b"\r\n");
    for part in parts {
        out.push(b'$');
        append_usize(out, part.len());
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(part);
        out.extend_from_slice(b"\r\n");
    }
}

fn append_usize(out: &mut Vec<u8>, mut value: usize) {
    let mut tmp = [0u8; 20];
    let mut pos = tmp.len();
    loop {
        pos -= 1;
        tmp[pos] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            break;
        }
    }
    out.extend_from_slice(&tmp[pos..]);
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default)
}

fn protocol_err(err: kora_protocol::ProtocolError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err.to_string())
}

fn percentile_ms(hist: &Histogram<u64>, p: f64) -> f64 {
    hist.value_at_quantile(p) as f64 / 1000.0
}

fn rps(stats: &RunStats) -> f64 {
    if stats.elapsed.as_secs_f64() <= 0.0 {
        return 0.0;
    }
    stats.ops as f64 / stats.elapsed.as_secs_f64()
}

fn print_summary(redis: &RunStats, kora: &RunStats) {
    println!();
    println!("=== Real App Traffic Results ===");
    println!(
        "{:<10} {:>12} {:>10} {:>10} {:>10} {:>10}",
        "Target", "Ops/s", "p50(ms)", "p95(ms)", "p99(ms)", "Errors"
    );
    for (name, stats) in [("Redis", redis), ("Kora", kora)] {
        println!(
            "{:<10} {:>12.0} {:>10.3} {:>10.3} {:>10.3} {:>10}",
            name,
            rps(stats),
            percentile_ms(&stats.latency_us, 0.50),
            percentile_ms(&stats.latency_us, 0.95),
            percentile_ms(&stats.latency_us, 0.99),
            stats.errors
        );
    }

    let redis_rps = rps(redis);
    let kora_rps = rps(kora);
    let delta_pct = if redis_rps > 0.0 {
        ((kora_rps - redis_rps) / redis_rps) * 100.0
    } else {
        0.0
    };
    println!("Throughput delta (Kora vs Redis): {delta_pct:+.2}%");
    println!();
    println!("Per-op p99 latency (ms):");
    for kind in [OpKind::Get, OpKind::Set, OpKind::Incr, OpKind::Publish] {
        let idx = kind as usize;
        println!(
            "{:<8} Redis={:.3}  Kora={:.3}",
            kind.as_str(),
            percentile_ms(&redis.op_latency_us[idx], 0.99),
            percentile_ms(&kora.op_latency_us[idx], 0.99)
        );
    }
}

struct KoraIsolated {
    shutdown: tokio::sync::watch::Sender<bool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl KoraIsolated {
    fn stop(mut self) {
        let _ = self.shutdown.send(true);
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
    }
}

fn start_kora_isolated(
    bind_address: String,
    workers: usize,
    runtime_workers: usize,
) -> KoraIsolated {
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let thread = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(runtime_workers)
            .enable_all()
            .build()
            .expect("kora runtime");

        runtime.block_on(async move {
            let server = KoraServer::new(ServerConfig {
                bind_address,
                worker_count: workers.max(1),
                ..Default::default()
            });
            let _ = server.run(shutdown_rx).await;
        });
    });

    KoraIsolated {
        shutdown: shutdown_tx,
        thread: Some(thread),
    }
}

async fn free_port() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind free port");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);
    port
}

struct RedisIsolated {
    child: Child,
    addr: String,
}

impl Drop for RedisIsolated {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl RedisIsolated {
    async fn start() -> Option<Self> {
        let port = free_port().await;
        let addr = format!("127.0.0.1:{port}");

        let child = Command::new("redis-server")
            .arg("--bind")
            .arg("127.0.0.1")
            .arg("--port")
            .arg(port.to_string())
            .arg("--save")
            .arg("")
            .arg("--appendonly")
            .arg("no")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .ok()?;

        for _ in 0..80 {
            if BenchConnection::connect(&addr).await.is_ok() {
                return Some(Self { child, addr });
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        let mut child = child;
        let _ = child.kill();
        let _ = child.wait();
        None
    }

    fn stop(mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// ---------------------------------------------------------------------------
// Pub/Sub Throughput Benchmark
// ---------------------------------------------------------------------------

struct SubResult {
    received: u64,
    delivery_us: Histogram<u64>,
}

impl SubResult {
    fn new() -> Self {
        Self {
            received: 0,
            delivery_us: Histogram::new_with_bounds(1, 60_000_000, 3).expect("histogram"),
        }
    }
}

struct PubSubBenchStats {
    published: u64,
    received: u64,
    delivery_us: Histogram<u64>,
    elapsed: Duration,
}

fn extract_pubsub_payload(value: &RespValue) -> Option<&[u8]> {
    if let RespValue::Array(Some(items)) = value {
        if items.len() >= 3 {
            if let RespValue::BulkString(Some(payload)) = &items[2] {
                return Some(payload);
            }
        }
    }
    None
}

async fn pubsub_sub_task(
    addr: String,
    channel: Vec<u8>,
    epoch: Instant,
    deadline: Instant,
) -> SubResult {
    let mut result = SubResult::new();
    let Ok(mut conn) = BenchConnection::connect(&addr).await else {
        return result;
    };

    let mut cmd = Vec::new();
    write_command(&[CMD_SUBSCRIBE, &channel], &mut cmd);
    if conn.write_all(&cmd).await.is_err() {
        return result;
    }
    if conn.read_value().await.is_err() {
        return result;
    }

    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, conn.read_value()).await {
            Ok(Ok(value)) => {
                let recv_ns = epoch.elapsed().as_nanos() as u64;
                result.received += 1;
                if let Some(payload) = extract_pubsub_payload(&value) {
                    if let Ok(s) = std::str::from_utf8(payload) {
                        if let Ok(sent_ns) = s.parse::<u64>() {
                            let delivery_us = recv_ns.saturating_sub(sent_ns) / 1000;
                            let _ = result.delivery_us.record(delivery_us.max(1));
                        }
                    }
                }
            }
            _ => break,
        }
    }

    result
}

async fn pubsub_pub_task(
    addr: String,
    channels: Arc<Vec<Vec<u8>>>,
    epoch: Instant,
    deadline: Instant,
    seed: u64,
) -> u64 {
    let Ok(mut conn) = BenchConnection::connect(&addr).await else {
        return 0;
    };
    let mut rng = StdRng::seed_from_u64(seed);
    let mut published = 0u64;

    while Instant::now() < deadline {
        let ch = &channels[rng.gen_range(0..channels.len())];
        let nanos = epoch.elapsed().as_nanos() as u64;
        let payload = nanos.to_string().into_bytes();

        let mut cmd = Vec::with_capacity(128);
        write_command(&[CMD_PUBLISH, ch, &payload], &mut cmd);
        if conn.write_all(&cmd).await.is_err() {
            break;
        }
        match conn.read_value().await {
            Ok(_) => published += 1,
            Err(_) => break,
        }
    }

    published
}

async fn run_pubsub_bench(
    addr: &str,
    channels: &Arc<Vec<Vec<u8>>>,
    num_publishers: usize,
    subs_per_channel: usize,
    duration: Duration,
) -> PubSubBenchStats {
    let epoch = Instant::now();
    let setup_delay = Duration::from_millis(500);
    let grace = Duration::from_millis(500);
    let sub_deadline = epoch + setup_delay + duration + grace;
    let pub_deadline = epoch + setup_delay + duration;

    let mut sub_handles = Vec::new();
    for ch in channels.iter() {
        for _ in 0..subs_per_channel {
            let addr = addr.to_string();
            let channel = ch.clone();
            sub_handles.push(tokio::spawn(pubsub_sub_task(
                addr,
                channel,
                epoch,
                sub_deadline,
            )));
        }
    }

    tokio::time::sleep(setup_delay).await;

    let mut pub_handles = Vec::new();
    for i in 0..num_publishers {
        let addr = addr.to_string();
        let chs = channels.clone();
        let seed = 0xDEAD_BEEF ^ ((i as u64 + 1) * 0x9E37_79B9);
        pub_handles.push(tokio::spawn(pubsub_pub_task(
            addr,
            chs,
            epoch,
            pub_deadline,
            seed,
        )));
    }

    let mut total_published = 0u64;
    for handle in pub_handles {
        if let Ok(count) = handle.await {
            total_published += count;
        }
    }

    tokio::time::sleep(grace).await;

    let mut total_received = 0u64;
    let mut delivery_us = Histogram::new_with_bounds(1, 60_000_000, 3).expect("histogram");
    for handle in sub_handles {
        if let Ok(result) = handle.await {
            total_received += result.received;
            let _ = delivery_us.add(&result.delivery_us);
        }
    }

    PubSubBenchStats {
        published: total_published,
        received: total_received,
        delivery_us,
        elapsed: duration,
    }
}

fn print_pubsub_results(redis: &PubSubBenchStats, kora: &PubSubBenchStats) {
    println!();
    println!("=== Pub/Sub Results ===");
    println!(
        "{:<10} {:>12} {:>12} {:>10} {:>10} {:>10} {:>10}",
        "Target", "Pub/s", "Delivered/s", "p50(ms)", "p95(ms)", "p99(ms)", "Fan-out"
    );
    for (name, stats) in [("Redis", redis), ("Kora", kora)] {
        let pub_rate = stats.published as f64 / stats.elapsed.as_secs_f64();
        let recv_rate = stats.received as f64 / stats.elapsed.as_secs_f64();
        let fanout = if stats.published > 0 {
            stats.received as f64 / stats.published as f64
        } else {
            0.0
        };
        println!(
            "{:<10} {:>12.0} {:>12.0} {:>10.3} {:>10.3} {:>10.3} {:>10.2}x",
            name,
            pub_rate,
            recv_rate,
            percentile_ms(&stats.delivery_us, 0.50),
            percentile_ms(&stats.delivery_us, 0.95),
            percentile_ms(&stats.delivery_us, 0.99),
            fanout,
        );
    }

    let redis_rate = redis.published as f64 / redis.elapsed.as_secs_f64();
    let kora_rate = kora.published as f64 / kora.elapsed.as_secs_f64();
    let delta = if redis_rate > 0.0 {
        ((kora_rate - redis_rate) / redis_rate) * 100.0
    } else {
        0.0
    };
    println!("Publish throughput delta (Kora vs Redis): {delta:+.2}%");
    println!(
        "Total published:  Redis={}  Kora={}",
        redis.published, kora.published
    );
    println!(
        "Total delivered:  Redis={}  Kora={}",
        redis.received, kora.received
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "manual benchmark: requires external Redis and release mode"]
async fn pubsub_throughput_compare() {
    let num_channels = env_usize("PUBSUB_BENCH_CHANNELS", 8);
    let num_publishers = env_usize("PUBSUB_BENCH_PUBLISHERS", 8);
    let subs_per_channel = env_usize("PUBSUB_BENCH_SUBS_PER_CH", 3);
    let bench_secs = env_usize("PUBSUB_BENCH_SECONDS", 10) as u64;
    let cfg = WorkloadConfig::from_env();

    let redis_isolated = RedisIsolated::start().await;
    let redis_addr = match redis_isolated {
        Some(ref r) => r.addr.clone(),
        None => {
            println!("Skipping pubsub benchmark: Redis not available");
            return;
        }
    };

    let kora_port = free_port().await;
    let kora_addr = format!("127.0.0.1:{kora_port}");
    let kora = start_kora_isolated(
        kora_addr.clone(),
        cfg.kora_workers,
        cfg.kora_runtime_workers,
    );
    tokio::time::sleep(Duration::from_millis(200)).await;

    let channels: Arc<Vec<Vec<u8>>> = Arc::new(
        (0..num_channels)
            .map(|i| format!("bench:ch:{i}").into_bytes())
            .collect(),
    );
    let total_subs = num_channels * subs_per_channel;

    println!("\n=== Pub/Sub Benchmark Config ===");
    println!(
        "channels={num_channels}  publishers={num_publishers}  subs/ch={subs_per_channel}  total_subs={total_subs}  duration={bench_secs}s"
    );

    let redis_stats = run_pubsub_bench(
        &redis_addr,
        &channels,
        num_publishers,
        subs_per_channel,
        Duration::from_secs(bench_secs),
    )
    .await;

    let kora_stats = run_pubsub_bench(
        &kora_addr,
        &channels,
        num_publishers,
        subs_per_channel,
        Duration::from_secs(bench_secs),
    )
    .await;

    kora.stop();
    if let Some(r) = redis_isolated {
        r.stop();
    }

    print_pubsub_results(&redis_stats, &kora_stats);
}
