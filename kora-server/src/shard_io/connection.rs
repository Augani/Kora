//! RESP connection handler for shard-affinity I/O.
//!
//! Each accepted TCP or Unix socket connection is driven by
//! [`handle_connection_affinity`], which runs as a `spawn_local` task on the
//! shard worker that accepted it. The handler implements:
//!
//! - **Pipeline batching** — multiple commands parsed from a single read are
//!   routed, executed, and serialized as a batch before flushing the write buffer.
//! - **Hot-command fast path** — GET, SET, and INCR bypass full `Command` parsing
//!   via [`HotCommandRef`] for lower per-op overhead.
//! - **Cross-shard fan-out** — commands whose key hashes to a different shard are
//!   queued into per-shard batches, dispatched in one shot, and resolved before
//!   the write buffer is flushed.
//! - **Pub/Sub mode** — once a connection subscribes it enters a push loop that
//!   multiplexes data reads with message delivery.
//! - **MULTI/EXEC** — transaction commands are queued locally and executed
//!   atomically at EXEC time.
//! - **Blocking commands** — BLPOP, BRPOP, etc. poll with back-off until the
//!   timeout expires or data appears.

use std::cell::RefCell;
use std::mem;
use std::rc::Rc;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use ahash::AHashSet;
use bytes::BytesMut;
use kora_cdc::consumer::ConsumerGroupManager;
use kora_cdc::ring::CdcRing;
use kora_core::command::{
    command_docs_response, command_help_response, command_info_response, command_list_response,
    supported_command_count, Command, CommandResponse, DocUpdateMutation,
};
use kora_core::shard::{ShardStore, WalRecord, WalWriter};
use kora_doc::{CollectionConfig, CompressionProfile, DocEngine, DocMutation, IndexType};
use kora_protocol::{parse_command, serialize_response_versioned, HotCommandRef, RespParser};
use kora_pubsub::{MessageSink, PubSubMessage};
use parking_lot::{Mutex, RwLock};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use super::dispatch;
use super::stage_metrics::{format_stage_metrics_info, BatchStage, BatchStageMetrics};
use super::{execute_with_wal_inline, AffinitySharedState, ShardRouter, CROSS_SHARD_TIMEOUT};

const PUBSUB_CHANNEL_CAPACITY: usize = 8192;

struct TokioSink {
    tx: tokio::sync::mpsc::Sender<PubSubMessage>,
}

impl MessageSink for TokioSink {
    fn send(&self, msg: PubSubMessage) -> bool {
        self.tx.try_send(msg).is_ok()
    }
}

struct ConnState {
    resp3: bool,
    conn_id: u64,
    shard_id: u16,
    pubsub_tx: Option<Arc<TokioSink>>,
    pubsub_rx: Option<tokio::sync::mpsc::Receiver<PubSubMessage>>,
    subscription_count: usize,
    pattern_count: usize,
    subscribed_channels: AHashSet<Vec<u8>>,
    subscribed_patterns: AHashSet<Vec<u8>>,
    in_multi: bool,
    multi_queue: Vec<Command>,
    client_name: Option<Vec<u8>>,
    quit_requested: bool,
    authenticated: bool,
}

type PendingForeignBatch = Vec<(usize, Command)>;
type BatchedResponse = (u64, Instant, Vec<(usize, CommandResponse)>);
type BatchedResponseReceiver = mpsc::UnboundedReceiver<BatchedResponse>;
type BatchedResponseSender = mpsc::UnboundedSender<BatchedResponse>;

#[derive(Default)]
struct BatchLoopDurations {
    route_ns: u64,
    execute_ns: u64,
    foreign_wait_ns: u64,
    serialize_ns: u64,
}

#[derive(Default)]
struct FlushDurations {
    foreign_wait_ns: u64,
    serialize_ns: u64,
}

fn internal_shard_error() -> CommandResponse {
    CommandResponse::Error("ERR internal error".into())
}

fn auth_required_for_connection(shared: &Arc<AffinitySharedState>, conn: &ConnState) -> bool {
    shared.runtime_config.read().requirepass.is_some() && !conn.authenticated
}

fn is_pre_auth_allowed_command(cmd: &Command) -> bool {
    matches!(
        cmd,
        Command::Auth { .. } | Command::Hello { .. } | Command::Quit
    )
}

fn noauth_error() -> CommandResponse {
    CommandResponse::Error("NOAUTH Authentication required".into())
}

async fn handle_wait_command(numreplicas: i64, timeout: i64) -> CommandResponse {
    if timeout < 0 {
        return CommandResponse::Error("ERR timeout is negative".into());
    }

    if numreplicas <= 0 {
        return CommandResponse::Integer(0);
    }

    if timeout == 0 {
        std::future::pending::<()>().await;
        unreachable!("pending future should never resolve");
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(timeout as u64)).await;
    CommandResponse::Integer(0)
}

async fn resolve_response_slots_in_place(
    responses: &mut [Option<CommandResponse>],
    batch_receiver: &mut BatchedResponseReceiver,
    metrics: &BatchStageMetrics,
    track_stages: bool,
    flush_id: u64,
    expected_batches: usize,
) {
    let deadline = tokio::time::Instant::now() + CROSS_SHARD_TIMEOUT;
    let mut received_batches = 0usize;

    while received_batches < expected_batches {
        let now = tokio::time::Instant::now();
        let Some(remaining) = deadline.checked_duration_since(now) else {
            break;
        };

        match tokio::time::timeout(remaining, batch_receiver.recv()).await {
            Ok(Some((response_flush_id, sent_at, batch))) => {
                if track_stages {
                    metrics.record(
                        BatchStage::RemoteDelivery,
                        sent_at.elapsed().as_nanos() as u64,
                    );
                }
                if response_flush_id != flush_id {
                    continue;
                }
                for (index, resp) in batch {
                    responses[index] = Some(resp);
                }
                received_batches += 1;
            }
            Ok(None) | Err(_) => break,
        }
    }

    if received_batches != expected_batches {
        for resp in responses.iter_mut() {
            if resp.is_none() {
                *resp = Some(internal_shard_error());
            }
        }
    }
}

/// Drives a single RESP client connection on the owning shard worker.
///
/// Reads are parsed incrementally; each batch of commands from a single read is
/// dispatched, cross-shard responses are awaited, and the write buffer is flushed
/// once per batch. The handler exits when the stream closes or the client sends QUIT.
pub(crate) async fn handle_connection_affinity<S: AsyncReadExt + AsyncWriteExt + Unpin>(
    mut stream: S,
    shard_id: u16,
    store: Rc<RefCell<ShardStore>>,
    wal_writer: Rc<RefCell<Option<Box<dyn WalWriter>>>>,
    router: ShardRouter,
    shared: Arc<AffinitySharedState>,
    conn_id: u64,
) -> std::io::Result<()> {
    let mut parser = RespParser::new();
    let mut write_buf = BytesMut::with_capacity(4096);
    let mut read_buf = vec![0u8; 8192];
    let mut conn = ConnState {
        resp3: false,
        conn_id,
        shard_id,
        pubsub_tx: None,
        pubsub_rx: None,
        subscription_count: 0,
        pattern_count: 0,
        subscribed_channels: AHashSet::new(),
        subscribed_patterns: AHashSet::new(),
        in_multi: false,
        multi_queue: Vec::new(),
        client_name: None,
        quit_requested: false,
        authenticated: shared.runtime_config.read().requirepass.is_none(),
    };

    let result = handle_stream_loop(
        &mut stream,
        &store,
        &wal_writer,
        &router,
        &shared,
        &mut parser,
        &mut write_buf,
        &mut read_buf,
        &mut conn,
    )
    .await;

    if conn.subscription_count > 0 || conn.pattern_count > 0 {
        shared.pub_sub.remove_connection(conn.conn_id);
    }

    result
}

#[allow(clippy::too_many_arguments)]
async fn handle_stream_loop<S: AsyncReadExt + AsyncWriteExt + Unpin>(
    stream: &mut S,
    store: &Rc<RefCell<ShardStore>>,
    wal_writer: &Rc<RefCell<Option<Box<dyn WalWriter>>>>,
    router: &ShardRouter,
    shared: &Arc<AffinitySharedState>,
    parser: &mut RespParser,
    write_buf: &mut BytesMut,
    read_buf: &mut [u8],
    conn: &mut ConnState,
) -> std::io::Result<()> {
    let (batch_tx, mut batch_rx) = mpsc::unbounded_channel();
    let mut next_flush_id = 1u64;
    let mut responses: Vec<Option<CommandResponse>> = Vec::with_capacity(16);
    let mut pending_foreign_batches: Vec<PendingForeignBatch> =
        vec![Vec::new(); router.shard_count()];

    loop {
        let in_pubsub_mode = conn.subscription_count > 0 || conn.pattern_count > 0;
        let mut should_yield_after_batch = false;

        if in_pubsub_mode {
            let rx = conn.pubsub_rx.as_mut().unwrap();
            tokio::select! {
                result = stream.read(read_buf) => {
                    let n = result?;
                    if n == 0 {
                        return Ok(());
                    }
                    parser.feed(&read_buf[..n]);
                    loop {
                        match parser.try_parse() {
                            Ok(Some(frame)) => match parse_command(frame) {
                                Ok(cmd) => {
                                    if !is_pubsub_allowed_command(&cmd) {
                                        let resp = CommandResponse::Error(
                                            "ERR Can't execute in pub/sub mode".into(),
                                        );
                                        serialize_response_versioned(&resp, write_buf, conn.resp3);
                                    } else if handle_pubsub_command(&cmd, shared, conn, write_buf) {
                                    } else if let Command::Ping { ref message } = cmd {
                                        let resp = match message {
                                            Some(m) => CommandResponse::BulkString(m.clone()),
                                            None => CommandResponse::SimpleString("PONG".into()),
                                        };
                                        serialize_response_versioned(&resp, write_buf, conn.resp3);
                                    }
                                }
                                Err(e) => {
                                    let resp = CommandResponse::Error(e.to_string());
                                    serialize_response_versioned(&resp, write_buf, conn.resp3);
                                }
                            },
                            Ok(None) => break,
                            Err(e) => {
                                let resp = CommandResponse::Error(e.to_string());
                                serialize_response_versioned(&resp, write_buf, conn.resp3);
                                break;
                            }
                        }
                    }
                }
                msg = rx.recv() => {
                    match msg {
                        Some(pubsub_msg) => {
                            serialize_pubsub_message(&pubsub_msg, write_buf);
                            while let Ok(extra) = rx.try_recv() {
                                serialize_pubsub_message(&extra, write_buf);
                            }
                        }
                        None => return Ok(()),
                    }
                }
            }
        } else {
            let n = stream.read(read_buf).await?;
            if n == 0 {
                return Ok(());
            }
            parser.feed(&read_buf[..n]);
            let track_stages = shared.batch_stage_metrics.is_enabled();
            let batch_start = maybe_now(track_stages);

            let mut processed_batch = false;
            let mut stage_durations = BatchLoopDurations::default();

            loop {
                if !conn.in_multi
                    && !auth_required_for_connection(shared, conn)
                    && parser
                        .try_parse_hot_command_with(|hot_cmd| {
                            process_hot_command(
                                hot_cmd,
                                conn.shard_id,
                                store,
                                wal_writer,
                                router,
                                shared,
                                conn,
                                &mut responses,
                                &mut pending_foreign_batches,
                                &mut stage_durations,
                                track_stages,
                            );
                        })
                        .is_some()
                {
                    processed_batch = true;
                    continue;
                }

                match parser.try_parse() {
                    Ok(Some(frame)) => match parse_command(frame) {
                        Ok(cmd) => {
                            processed_batch = true;
                            if let Some(resp) = try_handle_connection_command(
                                &cmd,
                                conn,
                                shared,
                                store,
                                wal_writer,
                                router,
                                &mut responses,
                                &mut pending_foreign_batches,
                                &batch_tx,
                                &mut batch_rx,
                                &mut next_flush_id,
                                write_buf,
                                &mut stage_durations,
                                track_stages,
                            )
                            .await
                            {
                                push_ready_response(&mut responses, resp);
                                continue;
                            }
                            if conn.in_multi {
                                conn.multi_queue.push(cmd);
                                push_ready_response(
                                    &mut responses,
                                    CommandResponse::SimpleString("QUEUED".into()),
                                );
                                continue;
                            }
                            if handle_pubsub_command(&cmd, shared, conn, write_buf) {
                                continue;
                            }
                            let should_signal = should_notify_blocking_waiters(&cmd);
                            if is_complex_command(&cmd) {
                                let flush_durations = flush_pending_responses(
                                    router,
                                    &shared.batch_stage_metrics,
                                    track_stages,
                                    &mut pending_foreign_batches,
                                    &mut responses,
                                    &batch_tx,
                                    &mut batch_rx,
                                    &mut next_flush_id,
                                    write_buf,
                                    conn.resp3,
                                )
                                .await;
                                stage_durations.foreign_wait_ns = stage_durations
                                    .foreign_wait_ns
                                    .saturating_add(flush_durations.foreign_wait_ns);
                                stage_durations.serialize_ns = stage_durations
                                    .serialize_ns
                                    .saturating_add(flush_durations.serialize_ns);

                                let execute_start = maybe_now(track_stages);
                                let resp = dispatch::handle_complex_command(
                                    cmd,
                                    conn.shard_id,
                                    store,
                                    wal_writer,
                                    router,
                                )
                                .await;
                                maybe_elapsed(execute_start, &mut stage_durations.execute_ns);
                                push_ready_response(&mut responses, resp);
                            } else {
                                process_simple_command(
                                    cmd,
                                    conn.shard_id,
                                    store,
                                    wal_writer,
                                    router,
                                    shared,
                                    conn,
                                    &mut responses,
                                    &mut pending_foreign_batches,
                                    &mut stage_durations,
                                    track_stages,
                                );
                            }
                            if should_signal {
                                signal_blocking_waiters(shared, conn.shard_id);
                            }
                        }
                        Err(e) => {
                            processed_batch = true;
                            push_ready_response(
                                &mut responses,
                                CommandResponse::Error(e.to_string()),
                            );
                        }
                    },
                    Ok(None) => break,
                    Err(e) => {
                        processed_batch = true;
                        push_ready_response(&mut responses, CommandResponse::Error(e.to_string()));
                        break;
                    }
                }
            }

            let flush_durations = flush_pending_responses(
                router,
                &shared.batch_stage_metrics,
                track_stages,
                &mut pending_foreign_batches,
                &mut responses,
                &batch_tx,
                &mut batch_rx,
                &mut next_flush_id,
                write_buf,
                conn.resp3,
            )
            .await;
            stage_durations.foreign_wait_ns = stage_durations
                .foreign_wait_ns
                .saturating_add(flush_durations.foreign_wait_ns);
            stage_durations.serialize_ns = stage_durations
                .serialize_ns
                .saturating_add(flush_durations.serialize_ns);

            // Each shard runtime multiplexes many connection tasks plus the
            // shard request loop. Yield between processed batches so hot
            // sockets do not starve forwarded-shard work or accept handling.
            should_yield_after_batch = processed_batch;

            if processed_batch && track_stages {
                let batch_total_ns =
                    batch_start.map_or(0, |t: Instant| t.elapsed().as_nanos() as u64);
                let accounted_ns = stage_durations
                    .route_ns
                    .saturating_add(stage_durations.execute_ns)
                    .saturating_add(stage_durations.foreign_wait_ns)
                    .saturating_add(stage_durations.serialize_ns);
                let parse_dispatch_ns = batch_total_ns.saturating_sub(accounted_ns);
                record_batch_stage_metrics(
                    &shared.batch_stage_metrics,
                    &stage_durations,
                    parse_dispatch_ns,
                );
            }
        }

        if !write_buf.is_empty() {
            let write_start = maybe_now(shared.batch_stage_metrics.is_enabled());
            stream.write_all(write_buf).await?;
            if let Some(t) = write_start {
                shared
                    .batch_stage_metrics
                    .record(BatchStage::SocketWrite, t.elapsed().as_nanos() as u64);
            }
            write_buf.clear();
        }

        if conn.quit_requested {
            return Ok(());
        }

        if should_yield_after_batch {
            tokio::task::yield_now().await;
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn try_handle_connection_command(
    cmd: &Command,
    conn: &mut ConnState,
    shared: &Arc<AffinitySharedState>,
    store: &Rc<RefCell<ShardStore>>,
    wal_writer: &Rc<RefCell<Option<Box<dyn WalWriter>>>>,
    router: &ShardRouter,
    responses: &mut Vec<Option<CommandResponse>>,
    pending_foreign_batches: &mut [PendingForeignBatch],
    batch_tx: &BatchedResponseSender,
    batch_rx: &mut BatchedResponseReceiver,
    next_flush_id: &mut u64,
    write_buf: &mut BytesMut,
    stage_durations: &mut BatchLoopDurations,
    track_stages: bool,
) -> Option<CommandResponse> {
    if auth_required_for_connection(shared, conn) && !is_pre_auth_allowed_command(cmd) {
        return Some(noauth_error());
    }

    match cmd {
        Command::Multi => {
            if conn.in_multi {
                return Some(CommandResponse::Error(
                    "ERR MULTI calls can not be nested".into(),
                ));
            }
            conn.in_multi = true;
            conn.multi_queue.clear();
            Some(CommandResponse::Ok)
        }
        Command::Exec => {
            if !conn.in_multi {
                return Some(CommandResponse::Error("ERR EXEC without MULTI".into()));
            }
            conn.in_multi = false;
            let queued = mem::take(&mut conn.multi_queue);
            let flush_durations = flush_pending_responses(
                router,
                &shared.batch_stage_metrics,
                track_stages,
                pending_foreign_batches,
                responses,
                batch_tx,
                batch_rx,
                next_flush_id,
                write_buf,
                conn.resp3,
            )
            .await;
            stage_durations.foreign_wait_ns = stage_durations
                .foreign_wait_ns
                .saturating_add(flush_durations.foreign_wait_ns);
            stage_durations.serialize_ns = stage_durations
                .serialize_ns
                .saturating_add(flush_durations.serialize_ns);

            let mut exec_results = Vec::with_capacity(queued.len());
            for queued_cmd in queued {
                let should_signal = should_notify_blocking_waiters(&queued_cmd);
                let resp = execute_single_command(
                    queued_cmd,
                    conn.shard_id,
                    store,
                    wal_writer,
                    router,
                    shared,
                    conn,
                )
                .await;
                if should_signal {
                    signal_blocking_waiters(shared, conn.shard_id);
                }
                exec_results.push(resp);
            }
            Some(CommandResponse::Array(exec_results))
        }
        Command::Discard => {
            if !conn.in_multi {
                return Some(CommandResponse::Error("ERR DISCARD without MULTI".into()));
            }
            conn.in_multi = false;
            conn.multi_queue.clear();
            Some(CommandResponse::Ok)
        }
        Command::Watch { .. } => {
            if conn.in_multi {
                return Some(CommandResponse::Error(
                    "ERR WATCH inside MULTI is not allowed".into(),
                ));
            }
            Some(CommandResponse::Ok)
        }
        Command::Unwatch => Some(CommandResponse::Ok),
        Command::Quit => {
            conn.quit_requested = true;
            Some(CommandResponse::Ok)
        }
        Command::ClientId => Some(CommandResponse::Integer(conn.conn_id as i64)),
        Command::ClientGetName => match &conn.client_name {
            Some(name) => Some(CommandResponse::BulkString(name.clone())),
            None => Some(CommandResponse::Nil),
        },
        Command::ClientSetName { name } => {
            conn.client_name = Some(name.clone());
            Some(CommandResponse::Ok)
        }
        Command::ClientList | Command::ClientInfo => {
            let name_str = conn
                .client_name
                .as_ref()
                .map(|n| String::from_utf8_lossy(n).into_owned())
                .unwrap_or_default();
            let info = format!(
                "id={} fd=0 name={} db=0 flags=N\r\n",
                conn.conn_id, name_str
            );
            Some(CommandResponse::BulkString(info.into_bytes()))
        }
        Command::Select { db } => {
            if *db < 0 {
                Some(CommandResponse::Error(
                    "ERR DB index is out of range".into(),
                ))
            } else if *db == 0 {
                Some(CommandResponse::Ok)
            } else {
                Some(CommandResponse::Error(
                    "ERR SELECT is not allowed in cluster mode".into(),
                ))
            }
        }
        Command::Time => {
            use std::time::SystemTime;
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default();
            let secs = now.as_secs();
            let micros = now.subsec_micros() as u64;
            Some(CommandResponse::Array(vec![
                CommandResponse::BulkString(secs.to_string().into_bytes()),
                CommandResponse::BulkString(micros.to_string().into_bytes()),
            ]))
        }
        Command::Wait {
            numreplicas,
            timeout,
        } => Some(handle_wait_command(*numreplicas, *timeout).await),
        Command::CommandInfo { names } => Some(command_info_response(names)),
        Command::CommandCount => Some(CommandResponse::Integer(supported_command_count())),
        Command::CommandList => Some(command_list_response()),
        Command::CommandHelp => Some(command_help_response()),
        Command::CommandDocs { names } => Some(command_docs_response(names)),
        Command::ConfigGet { pattern } => Some(handle_config_get(pattern, shared)),
        Command::ConfigSet { parameter, value } => {
            Some(handle_config_set(parameter, value, shared).await)
        }
        Command::ConfigResetStat => Some(CommandResponse::Ok),
        cmd if is_blocking_command(cmd) => {
            let flush_durations = flush_pending_responses(
                router,
                &shared.batch_stage_metrics,
                track_stages,
                pending_foreign_batches,
                responses,
                batch_tx,
                batch_rx,
                next_flush_id,
                write_buf,
                conn.resp3,
            )
            .await;
            stage_durations.foreign_wait_ns = stage_durations
                .foreign_wait_ns
                .saturating_add(flush_durations.foreign_wait_ns);
            stage_durations.serialize_ns = stage_durations
                .serialize_ns
                .saturating_add(flush_durations.serialize_ns);

            let timeout_secs = blocking_timeout(cmd);
            let resp = if is_complex_command(cmd) {
                dispatch::handle_complex_command(
                    cmd.clone(),
                    conn.shard_id,
                    store,
                    wal_writer,
                    router,
                )
                .await
            } else {
                execute_with_wal_inline(store, wal_writer, cmd.clone())
            };

            if !matches!(resp, CommandResponse::Nil) || timeout_secs <= 0.0 {
                return Some(resp);
            }

            let deadline =
                tokio::time::Instant::now() + tokio::time::Duration::from_secs_f64(timeout_secs);
            loop {
                if !wait_for_blocking_signal(shared, deadline).await {
                    return Some(CommandResponse::Nil);
                }
                let retry_resp = if is_complex_command(cmd) {
                    dispatch::handle_complex_command(
                        cmd.clone(),
                        conn.shard_id,
                        store,
                        wal_writer,
                        router,
                    )
                    .await
                } else {
                    execute_with_wal_inline(store, wal_writer, cmd.clone())
                };
                if !matches!(retry_resp, CommandResponse::Nil) {
                    return Some(retry_resp);
                }
            }
        }
        _ => None,
    }
}

async fn execute_single_command(
    cmd: Command,
    shard_id: u16,
    store: &Rc<RefCell<ShardStore>>,
    wal_writer: &Rc<RefCell<Option<Box<dyn WalWriter>>>>,
    router: &ShardRouter,
    shared: &Arc<AffinitySharedState>,
    conn: &mut ConnState,
) -> CommandResponse {
    if let Command::Wait {
        numreplicas,
        timeout,
    } = &cmd
    {
        return handle_wait_command(*numreplicas, *timeout).await;
    }

    if let Some(resp) = try_handle_local_affinity(&cmd, shared, conn) {
        return resp;
    }

    if is_complex_command(&cmd) {
        return dispatch::handle_complex_command(cmd, shard_id, store, wal_writer, router).await;
    }

    if let Some(key) = cmd.key() {
        let target_shard = router.shard_for_key(key);
        if target_shard == shard_id {
            execute_with_wal_inline(store, wal_writer, cmd)
        } else {
            match router.try_dispatch_foreign(target_shard, cmd) {
                Ok(rx) => match tokio::time::timeout(CROSS_SHARD_TIMEOUT, rx).await {
                    Ok(Ok(resp)) => resp,
                    _ => internal_shard_error(),
                },
                Err(resp) => resp,
            }
        }
    } else {
        store.borrow_mut().execute(cmd)
    }
}

fn handle_config_get(pattern: &str, shared: &Arc<AffinitySharedState>) -> CommandResponse {
    let pat = pattern.to_ascii_lowercase();
    let cfg = shared.runtime_config.read().clone();
    let mut result = Vec::new();

    let matches_pattern = |name: &str| -> bool {
        if pat == "*" {
            return true;
        }
        if pat.contains('*') {
            let prefix = pat.trim_end_matches('*');
            return name.starts_with(prefix);
        }
        name == pat
    };

    if matches_pattern("bind") {
        result.push(CommandResponse::BulkString(b"bind".to_vec()));
        result.push(CommandResponse::BulkString(cfg.bind.into_bytes()));
    }
    if matches_pattern("maxmemory") {
        result.push(CommandResponse::BulkString(b"maxmemory".to_vec()));
        result.push(CommandResponse::BulkString(
            cfg.maxmemory.to_string().into_bytes(),
        ));
    }
    if matches_pattern("save") {
        result.push(CommandResponse::BulkString(b"save".to_vec()));
        let save = cfg
            .save_interval_secs
            .map(|secs| format!("{secs} 1"))
            .unwrap_or_default();
        result.push(CommandResponse::BulkString(save.into_bytes()));
    }
    if matches_pattern("dir") {
        result.push(CommandResponse::BulkString(b"dir".to_vec()));
        let dir = cfg
            .data_dir
            .as_ref()
            .map(|path| path.to_string_lossy().to_string())
            .unwrap_or_else(|| ".".to_string());
        result.push(CommandResponse::BulkString(dir.into_bytes()));
    }
    if matches_pattern("requirepass") {
        result.push(CommandResponse::BulkString(b"requirepass".to_vec()));
        result.push(CommandResponse::BulkString(
            cfg.requirepass.unwrap_or_default().into_bytes(),
        ));
    }

    CommandResponse::Array(result)
}

fn invalid_maxmemory_error() -> CommandResponse {
    CommandResponse::Error("ERR invalid maxmemory value".into())
}

fn parse_maxmemory_bytes(value: &str) -> Result<usize, CommandResponse> {
    let normalized = value.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return Err(invalid_maxmemory_error());
    }

    let (digits, multiplier) = if let Some(v) = normalized.strip_suffix("kb") {
        (v, 1024u64)
    } else if let Some(v) = normalized.strip_suffix("mb") {
        (v, 1024u64 * 1024)
    } else if let Some(v) = normalized.strip_suffix("gb") {
        (v, 1024u64 * 1024 * 1024)
    } else if let Some(v) = normalized.strip_suffix('b') {
        (v, 1u64)
    } else {
        (normalized.as_str(), 1u64)
    };

    if digits.is_empty() {
        return Err(invalid_maxmemory_error());
    }

    let base = digits
        .parse::<u64>()
        .map_err(|_| invalid_maxmemory_error())?;
    let bytes = base
        .checked_mul(multiplier)
        .ok_or_else(invalid_maxmemory_error)?;
    usize::try_from(bytes).map_err(|_| invalid_maxmemory_error())
}

async fn handle_config_set(
    parameter: &str,
    value: &str,
    shared: &Arc<AffinitySharedState>,
) -> CommandResponse {
    let param_lower = parameter.to_ascii_lowercase();
    match param_lower.as_str() {
        "maxmemory" => {
            let bytes = match parse_maxmemory_bytes(value) {
                Ok(parsed) => parsed,
                Err(err) => return err,
            };
            if let Err(err) = super::apply_max_memory(shared, bytes).await {
                return CommandResponse::Error(err);
            }
            shared.runtime_config.write().maxmemory = bytes;
            CommandResponse::Ok
        }
        "requirepass" => {
            let requirepass = if value.is_empty() {
                None
            } else {
                Some(value.to_string())
            };
            shared.runtime_config.write().requirepass = requirepass;
            CommandResponse::Ok
        }
        _ => CommandResponse::Error("ERR Unsupported CONFIG parameter".into()),
    }
}

fn is_complex_command(cmd: &Command) -> bool {
    cmd.is_multi_key()
        || matches!(
            cmd,
            Command::DbSize
                | Command::FlushDb
                | Command::FlushAll
                | Command::Keys { .. }
                | Command::Scan { .. }
                | Command::Dump
                | Command::VecQuery { .. }
                | Command::StatsHotkeys { .. }
                | Command::StatsMemory { .. }
                | Command::RandomKey
                | Command::RPopLPush { .. }
                | Command::LMove { .. }
                | Command::SMove { .. }
                | Command::PfMerge { .. }
                | Command::BitOp { .. }
                | Command::BLMove { .. }
        )
}

fn is_blocking_command(cmd: &Command) -> bool {
    matches!(
        cmd,
        Command::BLPop { .. }
            | Command::BRPop { .. }
            | Command::BLMove { .. }
            | Command::BZPopMin { .. }
            | Command::BZPopMax { .. }
    )
}

fn blocking_timeout(cmd: &Command) -> f64 {
    match cmd {
        Command::BLPop { timeout, .. }
        | Command::BRPop { timeout, .. }
        | Command::BLMove { timeout, .. }
        | Command::BZPopMin { timeout, .. }
        | Command::BZPopMax { timeout, .. } => *timeout,
        _ => 0.0,
    }
}

fn should_notify_blocking_waiters(cmd: &Command) -> bool {
    matches!(
        cmd,
        Command::LPush { .. }
            | Command::RPush { .. }
            | Command::LPop { .. }
            | Command::RPop { .. }
            | Command::LRem { .. }
            | Command::LSet { .. }
            | Command::LMove { .. }
            | Command::BLMove { .. }
            | Command::ZAdd { .. }
            | Command::ZRem { .. }
            | Command::ZIncrBy { .. }
            | Command::ZPopMin { .. }
            | Command::ZPopMax { .. }
    )
}

fn signal_blocking_waiters(shared: &Arc<AffinitySharedState>, shard_id: u16) {
    if let Some(notifier) = shared.blocking_notifiers.get(shard_id as usize) {
        notifier.notify_waiters();
    }
}

async fn wait_for_blocking_signal(
    shared: &Arc<AffinitySharedState>,
    deadline: tokio::time::Instant,
) -> bool {
    if tokio::time::Instant::now() >= deadline {
        return false;
    }
    let mut waiters = JoinSet::new();
    for notifier in shared.blocking_notifiers.iter() {
        let notifier = notifier.clone();
        waiters.spawn(async move {
            notifier.notified().await;
        });
    }

    let woke = matches!(
        tokio::time::timeout_at(deadline, waiters.join_next()).await,
        Ok(Some(Ok(())))
    );
    waiters.abort_all();
    woke
}

fn push_ready_response(responses: &mut Vec<Option<CommandResponse>>, resp: CommandResponse) {
    responses.push(Some(resp));
}

fn queue_foreign_command(
    target_shard: u16,
    cmd: Command,
    responses: &mut Vec<Option<CommandResponse>>,
    pending_foreign_batches: &mut [PendingForeignBatch],
) {
    let slot_index = responses.len();
    responses.push(None);
    pending_foreign_batches[target_shard as usize].push((slot_index, cmd));
}

fn append_wal_set(
    wal_writer: &Rc<RefCell<Option<Box<dyn WalWriter>>>>,
    key: &[u8],
    value: &[u8],
    ttl_ms: Option<u64>,
) {
    let mut wal = wal_writer.borrow_mut();
    if let Some(ref mut writer) = *wal {
        writer.append(&WalRecord::Set {
            key: key.to_vec(),
            value: value.to_vec(),
            ttl_ms,
        });
    }
}

#[inline(always)]
fn maybe_now(track: bool) -> Option<Instant> {
    if track {
        Some(Instant::now())
    } else {
        None
    }
}

#[inline(always)]
fn maybe_elapsed(start: Option<Instant>, acc: &mut u64) {
    if let Some(t) = start {
        *acc = (*acc).saturating_add(t.elapsed().as_nanos() as u64);
    }
}

fn flush_pending_foreign_batches(
    router: &ShardRouter,
    pending_foreign_batches: &mut [PendingForeignBatch],
    flush_id: u64,
    batch_tx: &BatchedResponseSender,
    responses: &mut [Option<CommandResponse>],
) -> usize {
    let mut expected_batches = 0usize;

    for (target_shard, commands) in pending_foreign_batches.iter_mut().enumerate() {
        if commands.is_empty() {
            continue;
        }

        let batch = mem::take(commands);
        let pending_indices: Vec<usize> = batch.iter().map(|(index, _)| *index).collect();
        match router.try_dispatch_foreign_batch_stream(
            target_shard as u16,
            batch,
            flush_id,
            batch_tx.clone(),
        ) {
            Ok(()) => expected_batches += 1,
            Err(resp) => {
                for index in pending_indices {
                    responses[index] = Some(resp.clone());
                }
            }
        }
    }

    expected_batches
}

fn take_single_foreign_command(
    pending_foreign_batches: &mut [PendingForeignBatch],
) -> Option<(u16, usize, Command)> {
    let mut target: Option<usize> = None;

    for (idx, commands) in pending_foreign_batches.iter().enumerate() {
        if commands.is_empty() {
            continue;
        }
        if commands.len() != 1 || target.is_some() {
            return None;
        }
        target = Some(idx);
    }

    let target_idx = target?;
    let (slot_idx, cmd) = pending_foreign_batches[target_idx].pop()?;
    Some((target_idx as u16, slot_idx, cmd))
}

#[allow(clippy::too_many_arguments)]
async fn flush_pending_responses(
    router: &ShardRouter,
    metrics: &BatchStageMetrics,
    track_stages: bool,
    pending_foreign_batches: &mut [PendingForeignBatch],
    responses: &mut Vec<Option<CommandResponse>>,
    batch_tx: &BatchedResponseSender,
    batch_rx: &mut BatchedResponseReceiver,
    next_flush_id: &mut u64,
    write_buf: &mut BytesMut,
    resp3: bool,
) -> FlushDurations {
    if responses.is_empty() {
        return FlushDurations::default();
    }

    // Fast path only for true single-command flushes so pipelined batches
    // keep using the shard-batched stream path.
    if responses.len() == 1 && responses[0].is_none() {
        if let Some((target_shard, slot_idx, cmd)) =
            take_single_foreign_command(pending_foreign_batches)
        {
            let mut durations = FlushDurations::default();

            let foreign_wait_start = maybe_now(track_stages);
            let resp = match router.try_dispatch_foreign(target_shard, cmd) {
                Ok(rx) => match tokio::time::timeout(CROSS_SHARD_TIMEOUT, rx).await {
                    Ok(Ok(resp)) => resp,
                    _ => internal_shard_error(),
                },
                Err(resp) => resp,
            };
            responses[slot_idx] = Some(resp);
            maybe_elapsed(foreign_wait_start, &mut durations.foreign_wait_ns);
            if track_stages && durations.foreign_wait_ns > 0 {
                metrics.record(BatchStage::RemoteDelivery, durations.foreign_wait_ns);
            }

            let serialize_start = maybe_now(track_stages);
            for resp in responses.drain(..) {
                serialize_response_versioned(
                    &resp.unwrap_or_else(internal_shard_error),
                    write_buf,
                    resp3,
                );
            }
            maybe_elapsed(serialize_start, &mut durations.serialize_ns);

            return durations;
        }
    }

    let flush_id = *next_flush_id;
    *next_flush_id = (*next_flush_id).wrapping_add(1);
    let expected_batches = flush_pending_foreign_batches(
        router,
        pending_foreign_batches,
        flush_id,
        batch_tx,
        responses,
    );

    let foreign_wait_start = maybe_now(track_stages);
    if expected_batches > 0 {
        resolve_response_slots_in_place(
            responses,
            batch_rx,
            metrics,
            track_stages,
            flush_id,
            expected_batches,
        )
        .await;
    }
    let mut durations = FlushDurations::default();
    maybe_elapsed(foreign_wait_start, &mut durations.foreign_wait_ns);

    let serialize_start = maybe_now(track_stages);
    for resp in responses.drain(..) {
        serialize_response_versioned(&resp.unwrap_or_else(internal_shard_error), write_buf, resp3);
    }
    maybe_elapsed(serialize_start, &mut durations.serialize_ns);

    durations
}

#[allow(clippy::too_many_arguments)]
fn process_simple_command(
    cmd: Command,
    shard_id: u16,
    store: &Rc<RefCell<ShardStore>>,
    wal_writer: &Rc<RefCell<Option<Box<dyn WalWriter>>>>,
    router: &ShardRouter,
    shared: &Arc<AffinitySharedState>,
    conn: &mut ConnState,
    responses: &mut Vec<Option<CommandResponse>>,
    pending_foreign_batches: &mut [PendingForeignBatch],
    stage_durations: &mut BatchLoopDurations,
    track_stages: bool,
) {
    if let Some(resp) = try_handle_local_affinity(&cmd, shared, conn) {
        push_ready_response(responses, resp);
        return;
    }

    if let Some(key) = cmd.key() {
        let route_start = maybe_now(track_stages);
        let target_shard = router.shard_for_key(key);
        maybe_elapsed(route_start, &mut stage_durations.route_ns);
        if target_shard == shard_id {
            let execute_start = maybe_now(track_stages);
            let resp = execute_with_wal_inline(store, wal_writer, cmd);
            maybe_elapsed(execute_start, &mut stage_durations.execute_ns);
            push_ready_response(responses, resp);
        } else {
            queue_foreign_command(target_shard, cmd, responses, pending_foreign_batches);
        }
    } else {
        let execute_start = maybe_now(track_stages);
        let resp = store.borrow_mut().execute(cmd);
        maybe_elapsed(execute_start, &mut stage_durations.execute_ns);
        push_ready_response(responses, resp);
    }
}

#[allow(clippy::too_many_arguments)]
fn process_hot_command(
    hot_cmd: HotCommandRef<'_>,
    shard_id: u16,
    store: &Rc<RefCell<ShardStore>>,
    wal_writer: &Rc<RefCell<Option<Box<dyn WalWriter>>>>,
    router: &ShardRouter,
    shared: &Arc<AffinitySharedState>,
    _conn: &ConnState,
    responses: &mut Vec<Option<CommandResponse>>,
    pending_foreign_batches: &mut [PendingForeignBatch],
    stage_durations: &mut BatchLoopDurations,
    track_stages: bool,
) {
    match hot_cmd {
        HotCommandRef::Publish { channel, message } => {
            let count = shared.pub_sub.publish(channel, message);
            push_ready_response(responses, CommandResponse::Integer(count as i64));
        }
        HotCommandRef::Get { key } => {
            let route_start = maybe_now(track_stages);
            let target = router.shard_for_key(key);
            maybe_elapsed(route_start, &mut stage_durations.route_ns);
            if target == shard_id {
                let execute_start = maybe_now(track_stages);
                let resp = store.borrow_mut().get_bytes(key);
                maybe_elapsed(execute_start, &mut stage_durations.execute_ns);
                push_ready_response(responses, resp);
            } else {
                queue_foreign_command(
                    target,
                    Command::Get { key: key.to_vec() },
                    responses,
                    pending_foreign_batches,
                );
            }
        }
        HotCommandRef::Set { key, value } => {
            let route_start = maybe_now(track_stages);
            let target = router.shard_for_key(key);
            maybe_elapsed(route_start, &mut stage_durations.route_ns);
            if target == shard_id {
                let execute_start = maybe_now(track_stages);
                append_wal_set(wal_writer, key, value, None);
                let resp = store
                    .borrow_mut()
                    .set_bytes(key, value, None, None, false, false);
                maybe_elapsed(execute_start, &mut stage_durations.execute_ns);
                push_ready_response(responses, resp);
            } else {
                queue_foreign_command(
                    target,
                    Command::Set {
                        key: key.to_vec(),
                        value: value.to_vec(),
                        ex: None,
                        px: None,
                        nx: false,
                        xx: false,
                    },
                    responses,
                    pending_foreign_batches,
                );
            }
        }
        HotCommandRef::Incr { key } => {
            let route_start = maybe_now(track_stages);
            let target = router.shard_for_key(key);
            maybe_elapsed(route_start, &mut stage_durations.route_ns);
            if target == shard_id {
                let execute_start = maybe_now(track_stages);
                let resp = store.borrow_mut().incr_by_bytes(key, 1);
                maybe_elapsed(execute_start, &mut stage_durations.execute_ns);
                push_ready_response(responses, resp);
            } else {
                queue_foreign_command(
                    target,
                    Command::Incr { key: key.to_vec() },
                    responses,
                    pending_foreign_batches,
                );
            }
        }
    }
}

fn record_batch_stage_metrics(
    metrics: &BatchStageMetrics,
    stage_durations: &BatchLoopDurations,
    parse_dispatch_ns: u64,
) {
    metrics.record(BatchStage::ParseDispatch, parse_dispatch_ns);
    metrics.record(BatchStage::Route, stage_durations.route_ns);
    metrics.record(BatchStage::Execute, stage_durations.execute_ns);
    metrics.record(BatchStage::ForeignWait, stage_durations.foreign_wait_ns);
    metrics.record(BatchStage::Serialize, stage_durations.serialize_ns);
}

fn try_handle_local_affinity(
    cmd: &Command,
    shared: &Arc<AffinitySharedState>,
    conn: &mut ConnState,
) -> Option<CommandResponse> {
    match cmd {
        Command::Info {
            section: Some(section),
        } if matches!(
            section.to_ascii_lowercase().as_str(),
            "perf" | "profile" | "stages"
        ) =>
        {
            shared.batch_stage_metrics.enable();
            Some(CommandResponse::BulkString(
                format_stage_metrics_info(&shared.batch_stage_metrics).into_bytes(),
            ))
        }
        Command::Hello { version } => Some(handle_hello(version, conn)),
        Command::Auth { ref password } => {
            if let Some(expected) = shared.runtime_config.read().requirepass.clone() {
                let provided = String::from_utf8_lossy(password);
                if provided.as_ref() != expected {
                    return Some(CommandResponse::Error("ERR invalid password".into()));
                }
            }
            conn.authenticated = true;
            Some(CommandResponse::Ok)
        }
        Command::BgSave => Some(handle_bgsave_affinity(shared)),
        Command::BgRewriteAof => Some(handle_bgrewriteaof_affinity(shared)),
        Command::CommandInfo { names } => Some(command_info_response(names)),
        Command::CommandCount => Some(CommandResponse::Integer(supported_command_count())),
        Command::CommandList => Some(command_list_response()),
        Command::CommandHelp => Some(command_help_response()),
        Command::CommandDocs { names } => Some(command_docs_response(names)),
        Command::DocCreate {
            collection,
            compression,
        } => Some(handle_doc_create(
            &shared.doc_engine,
            collection,
            compression,
        )),
        Command::DocDrop { collection } => Some(handle_doc_drop(&shared.doc_engine, collection)),
        Command::DocInfo { collection } => Some(handle_doc_info(&shared.doc_engine, collection)),
        Command::DocDictInfo { collection } => {
            Some(handle_doc_dictinfo(&shared.doc_engine, collection))
        }
        Command::DocStorage { collection } => {
            Some(handle_doc_storage(&shared.doc_engine, collection))
        }
        Command::DocSet {
            collection,
            doc_id,
            json,
        } => {
            let resp = handle_doc_set(&shared.doc_engine, collection, doc_id, json);
            if matches!(resp, CommandResponse::Ok) {
                if let Some(ref wal) = shared.doc_wal {
                    wal.lock().append(&WalRecord::DocSet {
                        collection: collection.to_vec(),
                        doc_id: doc_id.to_vec(),
                        json: json.to_vec(),
                    });
                }
            }
            Some(resp)
        }
        Command::DocInsert { collection, json } => Some(handle_doc_insert(
            &shared.doc_engine,
            &shared.doc_wal,
            collection,
            json,
        )),
        Command::DocMSet {
            collection,
            entries,
        } => {
            let resp = handle_doc_mset(&shared.doc_engine, collection, entries);
            if matches!(resp, CommandResponse::Ok) {
                if let Some(ref wal) = shared.doc_wal {
                    let mut w = wal.lock();
                    for (doc_id, json) in entries {
                        w.append(&WalRecord::DocSet {
                            collection: collection.to_vec(),
                            doc_id: doc_id.clone(),
                            json: json.clone(),
                        });
                    }
                }
            }
            Some(resp)
        }
        Command::DocGet {
            collection,
            doc_id,
            fields,
        } => Some(handle_doc_get(
            &shared.doc_engine,
            collection,
            doc_id,
            fields,
        )),
        Command::DocMGet {
            collection,
            doc_ids,
        } => Some(handle_doc_mget(&shared.doc_engine, collection, doc_ids)),
        Command::DocUpdate {
            collection,
            doc_id,
            mutations,
        } => {
            let resp = handle_doc_update(&shared.doc_engine, collection, doc_id, mutations);
            if matches!(resp, CommandResponse::Ok) {
                if let Some(ref wal) = shared.doc_wal {
                    let col = String::from_utf8_lossy(collection);
                    let did = String::from_utf8_lossy(doc_id);
                    if let Ok(Some(doc)) = shared.doc_engine.read().get(&col, &did, None) {
                        if let Ok(json_bytes) = serde_json::to_vec(&doc) {
                            wal.lock().append(&WalRecord::DocSet {
                                collection: collection.to_vec(),
                                doc_id: doc_id.to_vec(),
                                json: json_bytes,
                            });
                        }
                    }
                }
            }
            Some(resp)
        }
        Command::DocDel { collection, doc_id } => {
            let resp = handle_doc_del(&shared.doc_engine, collection, doc_id);
            if matches!(resp, CommandResponse::Integer(1)) {
                if let Some(ref wal) = shared.doc_wal {
                    wal.lock().append(&WalRecord::DocDel {
                        collection: collection.to_vec(),
                        doc_id: doc_id.to_vec(),
                    });
                }
            }
            Some(resp)
        }
        Command::DocExists { collection, doc_id } => {
            Some(handle_doc_exists(&shared.doc_engine, collection, doc_id))
        }
        Command::DocCreateIndex {
            collection,
            field,
            index_type,
        } => Some(handle_doc_createindex(
            &shared.doc_engine,
            collection,
            field,
            index_type,
        )),
        Command::DocDropIndex { collection, field } => {
            Some(handle_doc_dropindex(&shared.doc_engine, collection, field))
        }
        Command::DocIndexes { collection } => {
            Some(handle_doc_indexes(&shared.doc_engine, collection))
        }
        Command::DocFind {
            collection,
            where_clause,
            fields,
            limit,
            offset,
            order_by,
            order_desc,
        } => Some(handle_doc_find(
            &shared.doc_engine,
            collection,
            where_clause,
            fields,
            *limit,
            *offset,
            order_by.as_deref(),
            *order_desc,
        )),
        Command::DocCount {
            collection,
            where_clause,
        } => Some(handle_doc_count(
            &shared.doc_engine,
            collection,
            where_clause,
        )),
        Command::CdcPoll { cursor, count } => {
            Some(handle_cdc_poll(&shared.cdc_rings, *cursor, *count))
        }
        Command::CdcGroupCreate {
            group, start_seq, ..
        } => Some(handle_cdc_group_create(
            &shared.cdc_rings,
            &shared.cdc_groups,
            group,
            *start_seq,
        )),
        Command::CdcGroupRead {
            group,
            consumer,
            count,
            ..
        } => Some(handle_cdc_group_read(
            &shared.cdc_rings,
            &shared.cdc_groups,
            group,
            consumer,
            *count,
        )),
        Command::CdcAck { group, seqs, .. } => {
            Some(handle_cdc_ack(&shared.cdc_groups, group, seqs))
        }
        Command::CdcPending { group, .. } => Some(handle_cdc_pending(&shared.cdc_groups, group)),

        Command::StatsLatency {
            command,
            percentiles,
        } => {
            shared.track_latency.store(true, Ordering::Relaxed);
            Some(handle_stats_latency(shared, command, percentiles))
        }
        _ => None,
    }
}

fn handle_hello(version: &Option<u8>, conn: &mut ConnState) -> CommandResponse {
    let ver = version.unwrap_or(2);
    match ver {
        2 => conn.resp3 = false,
        3 => conn.resp3 = true,
        _ => {
            return CommandResponse::Error(format!("NOPROTO unsupported protocol version: {}", ver))
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

fn handle_bgsave_affinity(shared: &Arc<AffinitySharedState>) -> CommandResponse {
    match super::start_manual_snapshot(shared) {
        Ok(()) => CommandResponse::SimpleString("Background saving started".into()),
        Err(err) => CommandResponse::Error(err),
    }
}

fn handle_bgrewriteaof_affinity(shared: &Arc<AffinitySharedState>) -> CommandResponse {
    match super::start_manual_aof_rewrite(shared) {
        Ok(()) => CommandResponse::SimpleString("Background AOF rewrite started".into()),
        Err(err) => CommandResponse::Error(err),
    }
}

fn handle_doc_create(
    doc_engine: &RwLock<DocEngine>,
    collection: &[u8],
    compression: &Option<Vec<u8>>,
) -> CommandResponse {
    let collection = match parse_utf8_arg(collection, "collection") {
        Ok(value) => value,
        Err(err) => return err,
    };
    let compression = match compression
        .as_ref()
        .map(|value| parse_compression_profile(value))
        .transpose()
    {
        Ok(profile) => profile.unwrap_or(CompressionProfile::Balanced),
        Err(err) => return err,
    };

    let config = CollectionConfig { compression };
    match doc_engine.write().create_collection(&collection, config) {
        Ok(_) => CommandResponse::Ok,
        Err(err) => CommandResponse::Error(format!("ERR {}", err)),
    }
}

fn handle_doc_drop(doc_engine: &RwLock<DocEngine>, collection: &[u8]) -> CommandResponse {
    let collection = match parse_utf8_arg(collection, "collection") {
        Ok(value) => value,
        Err(err) => return err,
    };

    if doc_engine.write().drop_collection(&collection) {
        CommandResponse::Integer(1)
    } else {
        CommandResponse::Integer(0)
    }
}

fn handle_doc_info(doc_engine: &RwLock<DocEngine>, collection: &[u8]) -> CommandResponse {
    let collection = match parse_utf8_arg(collection, "collection") {
        Ok(value) => value,
        Err(err) => return err,
    };

    let Some(info) = doc_engine.read().collection_info(&collection) else {
        return CommandResponse::Nil;
    };

    CommandResponse::Map(vec![
        (
            CommandResponse::BulkString(b"id".to_vec()),
            CommandResponse::Integer(info.id as i64),
        ),
        (
            CommandResponse::BulkString(b"name".to_vec()),
            CommandResponse::BulkString(info.name.into_bytes()),
        ),
        (
            CommandResponse::BulkString(b"created_at".to_vec()),
            CommandResponse::Integer(info.created_at as i64),
        ),
        (
            CommandResponse::BulkString(b"compression".to_vec()),
            CommandResponse::BulkString(compression_name(info.compression).as_bytes().to_vec()),
        ),
        (
            CommandResponse::BulkString(b"doc_count".to_vec()),
            CommandResponse::Integer(info.doc_count as i64),
        ),
        (
            CommandResponse::BulkString(b"dictionary_entries".to_vec()),
            CommandResponse::Integer(info.dictionary_entries as i64),
        ),
    ])
}

fn handle_doc_dictinfo(doc_engine: &RwLock<DocEngine>, collection: &[u8]) -> CommandResponse {
    let collection = match parse_utf8_arg(collection, "collection") {
        Ok(value) => value,
        Err(err) => return err,
    };

    let info = match doc_engine.read().dictionary_info(&collection) {
        Ok(info) => info,
        Err(err) => return CommandResponse::Error(format!("ERR {}", err)),
    };

    let mut fields = Vec::with_capacity(info.fields.len());
    for field in info.fields {
        fields.push(CommandResponse::Map(vec![
            (
                CommandResponse::BulkString(b"field_id".to_vec()),
                CommandResponse::Integer(field.field_id as i64),
            ),
            (
                CommandResponse::BulkString(b"path".to_vec()),
                CommandResponse::BulkString(field.path.into_bytes()),
            ),
            (
                CommandResponse::BulkString(b"cardinality_estimate".to_vec()),
                CommandResponse::Integer(field.cardinality_estimate as i64),
            ),
        ]));
    }

    CommandResponse::Map(vec![
        (
            CommandResponse::BulkString(b"id".to_vec()),
            CommandResponse::Integer(info.collection_id as i64),
        ),
        (
            CommandResponse::BulkString(b"name".to_vec()),
            CommandResponse::BulkString(info.collection_name.into_bytes()),
        ),
        (
            CommandResponse::BulkString(b"dictionary_entries".to_vec()),
            CommandResponse::Integer(info.dictionary_entries as i64),
        ),
        (
            CommandResponse::BulkString(b"fields".to_vec()),
            CommandResponse::Array(fields),
        ),
    ])
}

fn handle_doc_storage(doc_engine: &RwLock<DocEngine>, collection: &[u8]) -> CommandResponse {
    let collection = match parse_utf8_arg(collection, "collection") {
        Ok(value) => value,
        Err(err) => return err,
    };

    let info = match doc_engine.read().storage_info(&collection) {
        Ok(info) => info,
        Err(err) => return CommandResponse::Error(format!("ERR {}", err)),
    };

    CommandResponse::Map(vec![
        (
            CommandResponse::BulkString(b"id".to_vec()),
            CommandResponse::Integer(info.collection_id as i64),
        ),
        (
            CommandResponse::BulkString(b"name".to_vec()),
            CommandResponse::BulkString(info.collection_name.into_bytes()),
        ),
        (
            CommandResponse::BulkString(b"doc_count".to_vec()),
            CommandResponse::Integer(info.doc_count as i64),
        ),
        (
            CommandResponse::BulkString(b"total_packed_bytes".to_vec()),
            CommandResponse::Integer(info.total_packed_bytes as i64),
        ),
        (
            CommandResponse::BulkString(b"min_doc_bytes".to_vec()),
            CommandResponse::Integer(info.min_doc_bytes as i64),
        ),
        (
            CommandResponse::BulkString(b"max_doc_bytes".to_vec()),
            CommandResponse::Integer(info.max_doc_bytes as i64),
        ),
        (
            CommandResponse::BulkString(b"avg_doc_bytes".to_vec()),
            CommandResponse::Integer(info.avg_doc_bytes as i64),
        ),
    ])
}

fn handle_doc_set(
    doc_engine: &RwLock<DocEngine>,
    collection: &[u8],
    doc_id: &[u8],
    json: &[u8],
) -> CommandResponse {
    let collection = match parse_utf8_arg(collection, "collection") {
        Ok(value) => value,
        Err(err) => return err,
    };
    let doc_id = match parse_utf8_arg(doc_id, "doc_id") {
        Ok(value) => value,
        Err(err) => return err,
    };
    let json = match serde_json::from_slice::<serde_json::Value>(json) {
        Ok(value) => value,
        Err(err) => return CommandResponse::Error(format!("ERR invalid JSON: {}", err)),
    };

    match doc_engine.write().set(&collection, &doc_id, &json) {
        Ok(_) => CommandResponse::Ok,
        Err(err) => CommandResponse::Error(format!("ERR {}", err)),
    }
}

fn handle_doc_insert(
    doc_engine: &RwLock<DocEngine>,
    doc_wal: &Option<parking_lot::Mutex<Box<dyn WalWriter>>>,
    collection: &[u8],
    json: &[u8],
) -> CommandResponse {
    let collection_str = match parse_utf8_arg(collection, "collection") {
        Ok(value) => value,
        Err(err) => return err,
    };
    let json_value = match serde_json::from_slice::<serde_json::Value>(json) {
        Ok(value) => value,
        Err(err) => return CommandResponse::Error(format!("ERR invalid JSON: {}", err)),
    };

    match doc_engine.write().insert(&collection_str, &json_value) {
        Ok(result) => {
            if let Some(ref wal) = doc_wal {
                wal.lock().append(&WalRecord::DocSet {
                    collection: collection.to_vec(),
                    doc_id: result.id.as_bytes().to_vec(),
                    json: json.to_vec(),
                });
            }
            CommandResponse::BulkString(result.id.into_bytes())
        }
        Err(err) => CommandResponse::Error(format!("ERR {}", err)),
    }
}

fn handle_doc_mset(
    doc_engine: &RwLock<DocEngine>,
    collection: &[u8],
    entries: &[(Vec<u8>, Vec<u8>)],
) -> CommandResponse {
    let collection = match parse_utf8_arg(collection, "collection") {
        Ok(value) => value,
        Err(err) => return err,
    };

    let mut parsed_entries = Vec::with_capacity(entries.len());
    for (doc_id, json_bytes) in entries {
        let doc_id = match parse_utf8_arg(doc_id, "doc_id") {
            Ok(value) => value,
            Err(err) => return err,
        };
        let json = match serde_json::from_slice::<serde_json::Value>(json_bytes) {
            Ok(value) => value,
            Err(err) => return CommandResponse::Error(format!("ERR invalid JSON: {}", err)),
        };
        parsed_entries.push((doc_id, json));
    }

    let mut engine = doc_engine.write();
    for (doc_id, json) in &parsed_entries {
        if let Err(err) = engine.set(&collection, doc_id, json) {
            return CommandResponse::Error(format!("ERR {}", err));
        }
    }

    CommandResponse::Ok
}

fn handle_doc_get(
    doc_engine: &RwLock<DocEngine>,
    collection: &[u8],
    doc_id: &[u8],
    fields: &[Vec<u8>],
) -> CommandResponse {
    let collection = match parse_utf8_arg(collection, "collection") {
        Ok(value) => value,
        Err(err) => return err,
    };
    let doc_id = match parse_utf8_arg(doc_id, "doc_id") {
        Ok(value) => value,
        Err(err) => return err,
    };
    let field_strings = match parse_utf8_args(fields, "field") {
        Ok(values) => values,
        Err(err) => return err,
    };
    let projection_vec = (!field_strings.is_empty())
        .then(|| field_strings.iter().map(String::as_str).collect::<Vec<_>>());

    let engine = doc_engine.read();
    let doc = match engine.get(&collection, &doc_id, projection_vec.as_deref()) {
        Ok(value) => value,
        Err(err) => return CommandResponse::Error(format!("ERR {}", err)),
    };

    match doc {
        Some(value) => match serde_json::to_vec(&value) {
            Ok(bytes) => CommandResponse::BulkString(bytes),
            Err(err) => CommandResponse::Error(format!("ERR JSON serialization failed: {}", err)),
        },
        None => CommandResponse::Nil,
    }
}

fn handle_doc_mget(
    doc_engine: &RwLock<DocEngine>,
    collection: &[u8],
    doc_ids: &[Vec<u8>],
) -> CommandResponse {
    let collection = match parse_utf8_arg(collection, "collection") {
        Ok(value) => value,
        Err(err) => return err,
    };
    let doc_ids = match parse_utf8_args(doc_ids, "doc_id") {
        Ok(values) => values,
        Err(err) => return err,
    };

    let engine = doc_engine.read();
    let mut results = Vec::with_capacity(doc_ids.len());
    for doc_id in doc_ids {
        match engine.get(&collection, &doc_id, None) {
            Ok(Some(value)) => match serde_json::to_vec(&value) {
                Ok(bytes) => results.push(CommandResponse::BulkString(bytes)),
                Err(err) => {
                    return CommandResponse::Error(format!(
                        "ERR JSON serialization failed: {}",
                        err
                    ));
                }
            },
            Ok(None) => results.push(CommandResponse::Nil),
            Err(err) => return CommandResponse::Error(format!("ERR {}", err)),
        }
    }

    CommandResponse::Array(results)
}

fn handle_doc_update(
    doc_engine: &RwLock<DocEngine>,
    collection: &[u8],
    doc_id: &[u8],
    mutations: &[DocUpdateMutation],
) -> CommandResponse {
    if mutations.is_empty() {
        return CommandResponse::Error("ERR DOC.UPDATE requires at least one mutation".into());
    }

    let collection = match parse_utf8_arg(collection, "collection") {
        Ok(value) => value,
        Err(err) => return err,
    };
    let doc_id = match parse_utf8_arg(doc_id, "doc_id") {
        Ok(value) => value,
        Err(err) => return err,
    };

    let mut parsed = Vec::with_capacity(mutations.len());
    for mutation in mutations {
        match mutation {
            DocUpdateMutation::Set { path, value } => {
                let path = match parse_utf8_arg(path, "path") {
                    Ok(value) => value,
                    Err(err) => return err,
                };
                let value = match serde_json::from_slice::<serde_json::Value>(value) {
                    Ok(value) => value,
                    Err(err) => {
                        return CommandResponse::Error(format!("ERR invalid JSON: {}", err))
                    }
                };
                parsed.push(DocMutation::Set { path, value });
            }
            DocUpdateMutation::Del { path } => {
                let path = match parse_utf8_arg(path, "path") {
                    Ok(value) => value,
                    Err(err) => return err,
                };
                parsed.push(DocMutation::Del { path });
            }
            DocUpdateMutation::Incr { path, delta } => {
                if !delta.is_finite() {
                    return CommandResponse::Error(
                        "ERR DOC.UPDATE INCR delta must be finite".into(),
                    );
                }
                let path = match parse_utf8_arg(path, "path") {
                    Ok(value) => value,
                    Err(err) => return err,
                };
                parsed.push(DocMutation::Incr {
                    path,
                    delta: *delta,
                });
            }
            DocUpdateMutation::Push { path, value } => {
                let path = match parse_utf8_arg(path, "path") {
                    Ok(value) => value,
                    Err(err) => return err,
                };
                let value = match serde_json::from_slice::<serde_json::Value>(value) {
                    Ok(value) => value,
                    Err(err) => {
                        return CommandResponse::Error(format!("ERR invalid JSON: {}", err))
                    }
                };
                parsed.push(DocMutation::Push { path, value });
            }
            DocUpdateMutation::Pull { path, value } => {
                let path = match parse_utf8_arg(path, "path") {
                    Ok(value) => value,
                    Err(err) => return err,
                };
                let value = match serde_json::from_slice::<serde_json::Value>(value) {
                    Ok(value) => value,
                    Err(err) => {
                        return CommandResponse::Error(format!("ERR invalid JSON: {}", err))
                    }
                };
                parsed.push(DocMutation::Pull { path, value });
            }
        }
    }

    match doc_engine.write().update(&collection, &doc_id, &parsed) {
        Ok(true) => CommandResponse::Integer(1),
        Ok(false) => CommandResponse::Integer(0),
        Err(err) => CommandResponse::Error(format!("ERR {}", err)),
    }
}

fn handle_doc_del(
    doc_engine: &RwLock<DocEngine>,
    collection: &[u8],
    doc_id: &[u8],
) -> CommandResponse {
    let collection = match parse_utf8_arg(collection, "collection") {
        Ok(value) => value,
        Err(err) => return err,
    };
    let doc_id = match parse_utf8_arg(doc_id, "doc_id") {
        Ok(value) => value,
        Err(err) => return err,
    };

    match doc_engine.write().del(&collection, &doc_id) {
        Ok(true) => CommandResponse::Integer(1),
        Ok(false) => CommandResponse::Integer(0),
        Err(err) => CommandResponse::Error(format!("ERR {}", err)),
    }
}

fn handle_doc_exists(
    doc_engine: &RwLock<DocEngine>,
    collection: &[u8],
    doc_id: &[u8],
) -> CommandResponse {
    let collection = match parse_utf8_arg(collection, "collection") {
        Ok(value) => value,
        Err(err) => return err,
    };
    let doc_id = match parse_utf8_arg(doc_id, "doc_id") {
        Ok(value) => value,
        Err(err) => return err,
    };

    match doc_engine.read().exists(&collection, &doc_id) {
        Ok(true) => CommandResponse::Integer(1),
        Ok(false) => CommandResponse::Integer(0),
        Err(err) => CommandResponse::Error(format!("ERR {}", err)),
    }
}

fn parse_index_type(raw: &[u8]) -> Result<IndexType, CommandResponse> {
    let value = parse_utf8_arg(raw, "index_type")?;
    match value.to_ascii_lowercase().as_str() {
        "hash" => Ok(IndexType::Hash),
        "sorted" => Ok(IndexType::Sorted),
        "array" => Ok(IndexType::Array),
        "unique" => Ok(IndexType::Unique),
        _ => Err(CommandResponse::Error(
            "ERR invalid index type (expected hash|sorted|array|unique)".into(),
        )),
    }
}

fn index_type_name(idx_type: IndexType) -> &'static str {
    match idx_type {
        IndexType::Hash => "hash",
        IndexType::Sorted => "sorted",
        IndexType::Array => "array",
        IndexType::Unique => "unique",
    }
}

fn handle_doc_createindex(
    doc_engine: &RwLock<DocEngine>,
    collection: &[u8],
    field: &[u8],
    index_type: &[u8],
) -> CommandResponse {
    let collection = match parse_utf8_arg(collection, "collection") {
        Ok(value) => value,
        Err(err) => return err,
    };
    let field = match parse_utf8_arg(field, "field") {
        Ok(value) => value,
        Err(err) => return err,
    };
    let idx_type = match parse_index_type(index_type) {
        Ok(value) => value,
        Err(err) => return err,
    };

    match doc_engine
        .write()
        .create_index(&collection, &field, idx_type)
    {
        Ok(()) => CommandResponse::Ok,
        Err(err) => CommandResponse::Error(format!("ERR {}", err)),
    }
}

fn handle_doc_dropindex(
    doc_engine: &RwLock<DocEngine>,
    collection: &[u8],
    field: &[u8],
) -> CommandResponse {
    let collection = match parse_utf8_arg(collection, "collection") {
        Ok(value) => value,
        Err(err) => return err,
    };
    let field = match parse_utf8_arg(field, "field") {
        Ok(value) => value,
        Err(err) => return err,
    };

    match doc_engine.write().drop_index(&collection, &field) {
        Ok(()) => CommandResponse::Ok,
        Err(err) => CommandResponse::Error(format!("ERR {}", err)),
    }
}

fn handle_doc_indexes(doc_engine: &RwLock<DocEngine>, collection: &[u8]) -> CommandResponse {
    let collection = match parse_utf8_arg(collection, "collection") {
        Ok(value) => value,
        Err(err) => return err,
    };

    let indexes = match doc_engine.read().indexes(&collection) {
        Ok(value) => value,
        Err(err) => return CommandResponse::Error(format!("ERR {}", err)),
    };

    let entries: Vec<CommandResponse> = indexes
        .into_iter()
        .map(|(field_path, idx_type)| {
            CommandResponse::Array(vec![
                CommandResponse::BulkString(field_path.into_bytes()),
                CommandResponse::BulkString(index_type_name(idx_type).as_bytes().to_vec()),
            ])
        })
        .collect();

    CommandResponse::Array(entries)
}

#[allow(clippy::too_many_arguments)]
fn handle_doc_find(
    doc_engine: &RwLock<DocEngine>,
    collection: &[u8],
    where_clause: &[u8],
    fields: &[Vec<u8>],
    limit: Option<usize>,
    offset: usize,
    order_by: Option<&[u8]>,
    order_desc: bool,
) -> CommandResponse {
    let collection = match parse_utf8_arg(collection, "collection") {
        Ok(value) => value,
        Err(err) => return err,
    };
    let where_clause = match parse_utf8_arg(where_clause, "where_clause") {
        Ok(value) => value,
        Err(err) => return err,
    };
    let field_strings = match parse_utf8_args(fields, "field") {
        Ok(values) => values,
        Err(err) => return err,
    };
    let projection = (!field_strings.is_empty())
        .then(|| field_strings.iter().map(String::as_str).collect::<Vec<_>>());

    let order_by_str = match order_by {
        Some(bytes) => match std::str::from_utf8(bytes) {
            Ok(s) => Some(s),
            Err(_) => return CommandResponse::Error("ERR invalid UTF-8 in ORDER BY field".into()),
        },
        None => None,
    };

    let docs = match doc_engine.read().find(
        &collection,
        &where_clause,
        projection.as_deref(),
        limit,
        offset,
        order_by_str,
        order_desc,
    ) {
        Ok(value) => value,
        Err(err) => return CommandResponse::Error(format!("ERR {}", err)),
    };

    let results: Vec<CommandResponse> = docs
        .into_iter()
        .map(|value| match serde_json::to_vec(&value) {
            Ok(bytes) => CommandResponse::BulkString(bytes),
            Err(err) => CommandResponse::Error(format!("ERR JSON serialization failed: {}", err)),
        })
        .collect();

    CommandResponse::Array(results)
}

fn handle_doc_count(
    doc_engine: &RwLock<DocEngine>,
    collection: &[u8],
    where_clause: &[u8],
) -> CommandResponse {
    let collection = match parse_utf8_arg(collection, "collection") {
        Ok(value) => value,
        Err(err) => return err,
    };
    let where_clause = match parse_utf8_arg(where_clause, "where_clause") {
        Ok(value) => value,
        Err(err) => return err,
    };

    match doc_engine.read().count(&collection, &where_clause) {
        Ok(count) => CommandResponse::Integer(count as i64),
        Err(err) => CommandResponse::Error(format!("ERR {}", err)),
    }
}

fn parse_utf8_arg(bytes: &[u8], arg_name: &str) -> Result<String, CommandResponse> {
    std::str::from_utf8(bytes)
        .map(|value| value.to_string())
        .map_err(|_| CommandResponse::Error(format!("ERR invalid UTF-8 in {}", arg_name)))
}

fn parse_utf8_args(values: &[Vec<u8>], arg_name: &str) -> Result<Vec<String>, CommandResponse> {
    values
        .iter()
        .map(|value| parse_utf8_arg(value, arg_name))
        .collect()
}

fn parse_compression_profile(raw: &[u8]) -> Result<CompressionProfile, CommandResponse> {
    let value = parse_utf8_arg(raw, "compression profile")?;
    match value.to_ascii_lowercase().as_str() {
        "none" => Ok(CompressionProfile::None),
        "dictionary" => Ok(CompressionProfile::Dictionary),
        "balanced" => Ok(CompressionProfile::Balanced),
        "compact" => Ok(CompressionProfile::Compact),
        _ => Err(CommandResponse::Error(
            "ERR invalid compression profile (expected none|dictionary|balanced|compact)".into(),
        )),
    }
}

fn compression_name(profile: CompressionProfile) -> &'static str {
    match profile {
        CompressionProfile::None => "none",
        CompressionProfile::Dictionary => "dictionary",
        CompressionProfile::Balanced => "balanced",
        CompressionProfile::Compact => "compact",
    }
}

fn handle_cdc_poll(rings: &[Mutex<CdcRing>], cursor: u64, count: usize) -> CommandResponse {
    if rings.is_empty() {
        return CommandResponse::Error("ERR CDC not enabled".into());
    }
    let mut all_events = Vec::new();
    let mut max_next_seq = cursor;
    for ring in rings {
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

fn handle_cdc_group_create(
    rings: &[Mutex<CdcRing>],
    groups: &Mutex<ConsumerGroupManager>,
    group: &str,
    start_seq: u64,
) -> CommandResponse {
    if rings.is_empty() {
        return CommandResponse::Error("ERR CDC not enabled".into());
    }
    let mut mgr = groups.lock();
    match mgr.create_group(group, start_seq) {
        Ok(()) => CommandResponse::Ok,
        Err(e) => CommandResponse::Error(e.to_string()),
    }
}

fn handle_cdc_group_read(
    rings: &[Mutex<CdcRing>],
    groups: &Mutex<ConsumerGroupManager>,
    group: &str,
    consumer: &str,
    count: usize,
) -> CommandResponse {
    if rings.is_empty() {
        return CommandResponse::Error("ERR CDC not enabled".into());
    }
    let mut mgr = groups.lock();
    let mut all_events = Vec::new();
    let mut any_gap = false;
    for ring_mutex in rings {
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

fn handle_cdc_ack(
    groups: &Mutex<ConsumerGroupManager>,
    group: &str,
    seqs: &[u64],
) -> CommandResponse {
    let mut mgr = groups.lock();
    match mgr.ack(group, seqs) {
        Ok(count) => CommandResponse::Integer(count as i64),
        Err(e) => CommandResponse::Error(e.to_string()),
    }
}

fn handle_cdc_pending(groups: &Mutex<ConsumerGroupManager>, group: &str) -> CommandResponse {
    let mgr = groups.lock();
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

fn command_name_to_id(name: &str) -> usize {
    match name {
        "GET" => 0,
        "SET" => 1,
        "DEL" => 2,
        "INCR" | "INCRBY" => 3,
        "DECR" | "DECRBY" => 4,
        "MGET" => 5,
        "MSET" => 6,
        "EXISTS" => 7,
        "EXPIRE" | "PEXPIRE" => 8,
        "TTL" | "PTTL" => 9,
        _ => 255,
    }
}

fn handle_stats_latency(
    shared: &AffinitySharedState,
    command: &[u8],
    percentiles: &[f64],
) -> CommandResponse {
    let cmd_str = String::from_utf8_lossy(command).to_uppercase();
    let cmd_id = command_name_to_id(&cmd_str);
    let values: Vec<CommandResponse> = percentiles
        .iter()
        .map(|p| {
            let nanos = shared.histograms.percentile(cmd_id, *p);
            CommandResponse::BulkString(format!("{:.3}", nanos as f64 / 1_000_000.0).into_bytes())
        })
        .collect();
    CommandResponse::Array(values)
}

fn ensure_pubsub_channel(conn: &mut ConnState) -> Arc<TokioSink> {
    if let Some(ref tx) = conn.pubsub_tx {
        return tx.clone();
    }
    let (tx, rx) = tokio::sync::mpsc::channel(PUBSUB_CHANNEL_CAPACITY);
    let sink = Arc::new(TokioSink { tx });
    conn.pubsub_tx = Some(sink.clone());
    conn.pubsub_rx = Some(rx);
    sink
}

fn handle_pubsub_command(
    cmd: &Command,
    shared: &AffinitySharedState,
    conn: &mut ConnState,
    write_buf: &mut BytesMut,
) -> bool {
    match cmd {
        Command::Subscribe { channels } => {
            let sink = ensure_pubsub_channel(conn);
            for ch in channels {
                if conn.subscribed_channels.insert(ch.clone()) {
                    shared.pub_sub.subscribe(ch, conn.conn_id, sink.clone());
                    conn.subscription_count += 1;
                }
                serialize_pubsub_reply(
                    b"subscribe",
                    ch,
                    conn.subscription_count + conn.pattern_count,
                    write_buf,
                );
            }
            true
        }
        Command::Unsubscribe { channels } => {
            if channels.is_empty() {
                let all = std::mem::take(&mut conn.subscribed_channels);
                conn.subscription_count = 0;
                if all.is_empty() {
                    serialize_pubsub_reply(b"unsubscribe", b"", conn.pattern_count, write_buf);
                } else {
                    for ch in all {
                        shared.pub_sub.unsubscribe(&ch, conn.conn_id);
                        serialize_pubsub_reply(b"unsubscribe", &ch, conn.pattern_count, write_buf);
                    }
                }
            } else {
                for ch in channels {
                    if conn.subscribed_channels.remove(ch) {
                        shared.pub_sub.unsubscribe(ch, conn.conn_id);
                        conn.subscription_count = conn.subscription_count.saturating_sub(1);
                    }
                    serialize_pubsub_reply(
                        b"unsubscribe",
                        ch,
                        conn.subscription_count + conn.pattern_count,
                        write_buf,
                    );
                }
            }
            true
        }
        Command::PSubscribe { patterns } => {
            let sink = ensure_pubsub_channel(conn);
            for pat in patterns {
                if conn.subscribed_patterns.insert(pat.clone()) {
                    shared.pub_sub.psubscribe(pat, conn.conn_id, sink.clone());
                    conn.pattern_count += 1;
                }
                serialize_pubsub_reply(
                    b"psubscribe",
                    pat,
                    conn.subscription_count + conn.pattern_count,
                    write_buf,
                );
            }
            true
        }
        Command::PUnsubscribe { patterns } => {
            if patterns.is_empty() {
                let all = std::mem::take(&mut conn.subscribed_patterns);
                conn.pattern_count = 0;
                if all.is_empty() {
                    serialize_pubsub_reply(
                        b"punsubscribe",
                        b"",
                        conn.subscription_count,
                        write_buf,
                    );
                } else {
                    for pat in all {
                        shared.pub_sub.punsubscribe(&pat, conn.conn_id);
                        serialize_pubsub_reply(
                            b"punsubscribe",
                            &pat,
                            conn.subscription_count,
                            write_buf,
                        );
                    }
                }
            } else {
                for pat in patterns {
                    if conn.subscribed_patterns.remove(pat) {
                        shared.pub_sub.punsubscribe(pat, conn.conn_id);
                        conn.pattern_count = conn.pattern_count.saturating_sub(1);
                    }
                    serialize_pubsub_reply(
                        b"punsubscribe",
                        pat,
                        conn.subscription_count + conn.pattern_count,
                        write_buf,
                    );
                }
            }
            true
        }
        Command::Publish { channel, message } => {
            let count = shared.pub_sub.publish(channel, message);
            write_resp_integer_usize(write_buf, count);
            true
        }
        _ => false,
    }
}

fn is_pubsub_allowed_command(cmd: &Command) -> bool {
    matches!(
        cmd,
        Command::Subscribe { .. }
            | Command::Unsubscribe { .. }
            | Command::PSubscribe { .. }
            | Command::PUnsubscribe { .. }
            | Command::Ping { .. }
    )
}

fn write_usize(buf: &mut BytesMut, n: usize) {
    let mut tmp = [0u8; 20];
    let mut pos = tmp.len();
    let mut value = n;
    loop {
        pos -= 1;
        tmp[pos] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            break;
        }
    }
    buf.extend_from_slice(&tmp[pos..]);
}

fn write_resp_bulk(buf: &mut BytesMut, data: &[u8]) {
    buf.extend_from_slice(b"$");
    write_usize(buf, data.len());
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(data);
    buf.extend_from_slice(b"\r\n");
}

fn write_resp_integer_usize(buf: &mut BytesMut, value: usize) {
    if value == 0 {
        buf.extend_from_slice(b":0\r\n");
        return;
    }
    buf.extend_from_slice(b":");
    write_usize(buf, value);
    buf.extend_from_slice(b"\r\n");
}

fn serialize_pubsub_reply(kind: &[u8], name: &[u8], count: usize, buf: &mut BytesMut) {
    buf.extend_from_slice(b"*3\r\n");
    write_resp_bulk(buf, kind);
    write_resp_bulk(buf, name);
    write_resp_integer_usize(buf, count);
}

fn serialize_pubsub_message(msg: &PubSubMessage, buf: &mut BytesMut) {
    match msg {
        PubSubMessage::Message { channel, data } => {
            buf.extend_from_slice(b"*3\r\n");
            buf.extend_from_slice(b"$7\r\nmessage\r\n");
            write_resp_bulk(buf, channel);
            write_resp_bulk(buf, data);
        }
        PubSubMessage::PatternMessage {
            pattern,
            channel,
            data,
        } => {
            buf.extend_from_slice(b"*4\r\n");
            buf.extend_from_slice(b"$8\r\npmessage\r\n");
            write_resp_bulk(buf, pattern);
            write_resp_bulk(buf, channel);
            write_resp_bulk(buf, data);
        }
    }
}
