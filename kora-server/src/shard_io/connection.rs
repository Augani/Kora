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
use kora_core::command::{Command, CommandResponse};
use kora_core::shard::{ShardStore, WalRecord, WalWriter};
use kora_core::tenant::TenantId;
use kora_protocol::{parse_command, serialize_response_versioned, HotCommandRef, RespParser};
use kora_pubsub::{MessageSink, PubSubMessage};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

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
    tenant_id: TenantId,
    conn_id: u64,
    shard_id: u16,
    pubsub_tx: Option<Arc<TokioSink>>,
    pubsub_rx: Option<tokio::sync::mpsc::Receiver<PubSubMessage>>,
    subscription_count: usize,
    pattern_count: usize,
    subscribed_channels: AHashSet<Vec<u8>>,
    subscribed_patterns: AHashSet<Vec<u8>>,
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

async fn resolve_response_slots(
    mut responses: Vec<Option<CommandResponse>>,
    batch_receiver: &mut BatchedResponseReceiver,
    metrics: &BatchStageMetrics,
    flush_id: u64,
    expected_batches: usize,
) -> Vec<CommandResponse> {
    if expected_batches > 0 {
        let deadline = tokio::time::Instant::now() + CROSS_SHARD_TIMEOUT;
        let mut received_batches = 0usize;

        while received_batches < expected_batches {
            let now = tokio::time::Instant::now();
            let Some(remaining) = deadline.checked_duration_since(now) else {
                break;
            };

            match tokio::time::timeout(remaining, batch_receiver.recv()).await {
                Ok(Some((response_flush_id, sent_at, batch))) => {
                    metrics.record(
                        BatchStage::RemoteDelivery,
                        sent_at.elapsed().as_nanos() as u64,
                    );
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
            for resp in &mut responses {
                if resp.is_none() {
                    *resp = Some(internal_shard_error());
                }
            }
        }
    }

    responses
        .into_iter()
        .map(|resp| resp.unwrap_or_else(internal_shard_error))
        .collect()
}

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
        tenant_id: TenantId(0),
        conn_id,
        shard_id,
        pubsub_tx: None,
        pubsub_rx: None,
        subscription_count: 0,
        pattern_count: 0,
        subscribed_channels: AHashSet::new(),
        subscribed_patterns: AHashSet::new(),
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
            let batch_start = Instant::now();

            let mut responses: Vec<Option<CommandResponse>> = Vec::with_capacity(16);
            let mut pending_foreign_batches: Vec<PendingForeignBatch> =
                vec![Vec::new(); router.shard_count()];
            let mut processed_batch = false;
            let mut stage_durations = BatchLoopDurations::default();

            loop {
                if parser
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
                            if handle_pubsub_command(&cmd, shared, conn, write_buf) {
                                continue;
                            }
                            if is_complex_command(&cmd) {
                                let flush_durations = flush_pending_responses(
                                    router,
                                    &shared.batch_stage_metrics,
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

                                let execute_start = Instant::now();
                                let resp = dispatch::handle_complex_command(
                                    cmd,
                                    conn.shard_id,
                                    store,
                                    wal_writer,
                                    router,
                                )
                                .await;
                                stage_durations.execute_ns = stage_durations
                                    .execute_ns
                                    .saturating_add(execute_start.elapsed().as_nanos() as u64);
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
                                );
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

            if processed_batch {
                let batch_total_ns = batch_start.elapsed().as_nanos() as u64;
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
            let write_start = Instant::now();
            stream.write_all(write_buf).await?;
            shared.batch_stage_metrics.record(
                BatchStage::SocketWrite,
                write_start.elapsed().as_nanos() as u64,
            );
            write_buf.clear();
        }

        if should_yield_after_batch {
            tokio::task::yield_now().await;
        }
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
        )
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

fn append_wal_record(wal_writer: &Rc<RefCell<Option<Box<dyn WalWriter>>>>, record: WalRecord) {
    let mut wal = wal_writer.borrow_mut();
    if let Some(ref mut writer) = *wal {
        writer.append(&record);
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

#[allow(clippy::too_many_arguments)]
async fn flush_pending_responses(
    router: &ShardRouter,
    metrics: &BatchStageMetrics,
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

    let flush_id = *next_flush_id;
    *next_flush_id = (*next_flush_id).wrapping_add(1);
    let expected_batches = flush_pending_foreign_batches(
        router,
        pending_foreign_batches,
        flush_id,
        batch_tx,
        responses,
    );

    let foreign_wait_start = Instant::now();
    let resolved = resolve_response_slots(
        mem::take(responses),
        batch_rx,
        metrics,
        flush_id,
        expected_batches,
    )
    .await;
    let foreign_wait_ns = foreign_wait_start.elapsed().as_nanos() as u64;

    let serialize_start = Instant::now();
    for resp in resolved {
        serialize_response_versioned(&resp, write_buf, resp3);
    }
    let serialize_ns = serialize_start.elapsed().as_nanos() as u64;

    FlushDurations {
        foreign_wait_ns,
        serialize_ns,
    }
}

fn authorize_hot_dispatch(
    is_mutation: bool,
    shared: &AffinitySharedState,
    conn: &ConnState,
) -> Result<(), CommandResponse> {
    let key_count_delta = if is_mutation { 1 } else { 0 };
    if let Err(e) = shared
        .tenants
        .check_limits(conn.tenant_id, key_count_delta, 0)
    {
        return Err(CommandResponse::Error(format!("ERR {}", e)));
    }
    shared.tenants.record_operation(conn.tenant_id);
    Ok(())
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
) {
    if let Some(resp) = try_handle_local_affinity(&cmd, shared, conn) {
        push_ready_response(responses, resp);
        return;
    }

    if shared.tenant_limits_enabled {
        if let Err(resp) = authorize_dispatch(&cmd, shared, conn) {
            push_ready_response(responses, resp);
            return;
        }
    }

    if let Some(key) = cmd.key() {
        let route_start = Instant::now();
        let target_shard = router.shard_for_key(key);
        stage_durations.route_ns = stage_durations
            .route_ns
            .saturating_add(route_start.elapsed().as_nanos() as u64);
        if target_shard == shard_id {
            let execute_start = Instant::now();
            let resp = execute_with_wal_inline(store, wal_writer, cmd);
            stage_durations.execute_ns = stage_durations
                .execute_ns
                .saturating_add(execute_start.elapsed().as_nanos() as u64);
            push_ready_response(responses, resp);
        } else {
            queue_foreign_command(target_shard, cmd, responses, pending_foreign_batches);
        }
    } else {
        let execute_start = Instant::now();
        let resp = store.borrow_mut().execute(cmd);
        stage_durations.execute_ns = stage_durations
            .execute_ns
            .saturating_add(execute_start.elapsed().as_nanos() as u64);
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
    conn: &ConnState,
    responses: &mut Vec<Option<CommandResponse>>,
    pending_foreign_batches: &mut [PendingForeignBatch],
    stage_durations: &mut BatchLoopDurations,
) {
    match hot_cmd {
        HotCommandRef::Publish { channel, message } => {
            let count = shared.pub_sub.publish(channel, message);
            push_ready_response(responses, CommandResponse::Integer(count as i64));
        }
        HotCommandRef::Get { key } => {
            if shared.tenant_limits_enabled {
                if let Err(resp) = authorize_hot_dispatch(false, shared, conn) {
                    push_ready_response(responses, resp);
                    return;
                }
            }
            let route_start = Instant::now();
            let target = router.shard_for_key(key);
            stage_durations.route_ns = stage_durations
                .route_ns
                .saturating_add(route_start.elapsed().as_nanos() as u64);
            if target == shard_id {
                let execute_start = Instant::now();
                let resp = store.borrow_mut().get_bytes(key);
                stage_durations.execute_ns = stage_durations
                    .execute_ns
                    .saturating_add(execute_start.elapsed().as_nanos() as u64);
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
            if shared.tenant_limits_enabled {
                if let Err(resp) = authorize_hot_dispatch(true, shared, conn) {
                    push_ready_response(responses, resp);
                    return;
                }
            }
            let route_start = Instant::now();
            let target = router.shard_for_key(key);
            stage_durations.route_ns = stage_durations
                .route_ns
                .saturating_add(route_start.elapsed().as_nanos() as u64);
            if target == shard_id {
                let execute_start = Instant::now();
                append_wal_record(
                    wal_writer,
                    WalRecord::Set {
                        key: key.to_vec(),
                        value: value.to_vec(),
                        ttl_ms: None,
                    },
                );
                let resp = store
                    .borrow_mut()
                    .set_bytes(key, value, None, None, false, false);
                stage_durations.execute_ns = stage_durations
                    .execute_ns
                    .saturating_add(execute_start.elapsed().as_nanos() as u64);
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
            if shared.tenant_limits_enabled {
                if let Err(resp) = authorize_hot_dispatch(true, shared, conn) {
                    push_ready_response(responses, resp);
                    return;
                }
            }
            let route_start = Instant::now();
            let target = router.shard_for_key(key);
            stage_durations.route_ns = stage_durations
                .route_ns
                .saturating_add(route_start.elapsed().as_nanos() as u64);
            if target == shard_id {
                let execute_start = Instant::now();
                let resp = store.borrow_mut().incr_by_bytes(key, 1);
                stage_durations.execute_ns = stage_durations
                    .execute_ns
                    .saturating_add(execute_start.elapsed().as_nanos() as u64);
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
        Command::Auth { tenant, password } => {
            if let Some(ref expected) = shared.auth_password {
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
        Command::BgSave => Some(handle_bgsave_affinity(shared)),
        Command::BgRewriteAof => Some(CommandResponse::SimpleString(
            "Background AOF rewrite started".into(),
        )),
        Command::CommandInfo => Some(CommandResponse::Array(vec![])),
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
        Command::ScriptLoad { name, wasm_bytes } => {
            Some(handle_script_load(&shared.scripts, name, wasm_bytes))
        }
        Command::ScriptCall {
            name,
            args,
            byte_args,
        } => Some(handle_script_call(&shared.scripts, name, args, byte_args)),
        Command::ScriptDel { name } => Some(handle_script_del(&shared.scripts, name)),
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

fn authorize_dispatch(
    cmd: &Command,
    shared: &AffinitySharedState,
    conn: &ConnState,
) -> Result<(), CommandResponse> {
    let key_count_delta = if cmd.is_mutation() { 1 } else { 0 };
    if let Err(e) = shared
        .tenants
        .check_limits(conn.tenant_id, key_count_delta, 0)
    {
        return Err(CommandResponse::Error(format!("ERR {}", e)));
    }
    shared.tenants.record_operation(conn.tenant_id);
    Ok(())
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

fn handle_script_load(
    scripts: &Mutex<Option<kora_scripting::FunctionRegistry>>,
    name: &[u8],
    wasm_bytes: &[u8],
) -> CommandResponse {
    let mut scripts = scripts.lock();
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
    scripts: &Mutex<Option<kora_scripting::FunctionRegistry>>,
    name: &[u8],
    args: &[i64],
    byte_args: &[Vec<u8>],
) -> CommandResponse {
    let scripts = scripts.lock();
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

fn handle_script_del(
    scripts: &Mutex<Option<kora_scripting::FunctionRegistry>>,
    name: &[u8],
) -> CommandResponse {
    let mut scripts = scripts.lock();
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
