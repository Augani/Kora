use std::cell::RefCell;
use std::rc::Rc;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use kora_core::command::{Command, CommandResponse};
use kora_core::shard::{ShardStore, WalWriter};
use kora_core::types::CompactKey;
use kora_protocol::{
    serialize_memcache_response, MemcacheCommand, MemcacheParser, MemcacheResponse,
    MemcacheStoreMode, MemcacheValue, ProtocolError,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::oneshot;

use super::{
    execute_with_wal_inline, AffinitySharedState, MemcacheRequest, ShardRequest, ShardRouter,
    CROSS_SHARD_TIMEOUT,
};

enum ResponseSlot {
    Ready(Option<MemcacheResponse>),
    Pending {
        rx: oneshot::Receiver<MemcacheResponse>,
        noreply: bool,
    },
}

pub(crate) async fn handle_connection_affinity<S: AsyncReadExt + AsyncWriteExt + Unpin>(
    mut stream: S,
    shard_id: u16,
    store: Rc<RefCell<ShardStore>>,
    wal_writer: Rc<RefCell<Option<Box<dyn WalWriter>>>>,
    router: ShardRouter,
    shared: Arc<AffinitySharedState>,
) -> std::io::Result<()> {
    let mut parser = MemcacheParser::new();
    let mut write_buf = BytesMut::with_capacity(4096);
    let mut read_buf = vec![0u8; 8192];

    loop {
        let n = stream.read(&mut read_buf).await?;
        if n == 0 {
            return Ok(());
        }
        parser.feed(&read_buf[..n]);

        let mut slots = Vec::with_capacity(16);
        loop {
            match parser.try_parse() {
                Ok(Some(MemcacheCommand::Quit)) => {
                    if !write_buf.is_empty() {
                        stream.write_all(&write_buf).await?;
                    }
                    return Ok(());
                }
                Ok(Some(cmd)) => {
                    process_command(
                        cmd,
                        shard_id,
                        &store,
                        &wal_writer,
                        &router,
                        &shared,
                        &mut slots,
                    )
                    .await;
                }
                Ok(None) => break,
                Err(err) => {
                    let resp = protocol_error_response(err);
                    serialize_memcache_response(&resp, &mut write_buf);
                    stream.write_all(&write_buf).await?;
                    return Ok(());
                }
            }
        }

        for slot in slots {
            match slot {
                ResponseSlot::Ready(Some(resp)) => {
                    serialize_memcache_response(&resp, &mut write_buf);
                }
                ResponseSlot::Ready(None) => {}
                ResponseSlot::Pending { rx, noreply } => {
                    let resp = match tokio::time::timeout(CROSS_SHARD_TIMEOUT, rx).await {
                        Ok(Ok(resp)) => resp,
                        _ => MemcacheResponse::ServerError("internal error".into()),
                    };
                    if !noreply {
                        serialize_memcache_response(&resp, &mut write_buf);
                    }
                }
            }
        }

        if !write_buf.is_empty() {
            stream.write_all(&write_buf).await?;
            write_buf.clear();
        }
    }
}

pub(crate) fn execute_memcache_inline(
    store: &Rc<RefCell<ShardStore>>,
    wal_writer: &Rc<RefCell<Option<Box<dyn WalWriter>>>>,
    shared: &AffinitySharedState,
    request: MemcacheRequest,
) -> MemcacheResponse {
    match request {
        MemcacheRequest::Get { key } => {
            shared.memcache_cmd_get.fetch_add(1, Ordering::Relaxed);
            let response = {
                let mut store_ref = store.borrow_mut();
                store_ref.execute(Command::Get { key: key.clone() })
            };
            match response.bulk_string_bytes().map(|data| data.to_vec()) {
                Some(data) => {
                    shared.memcache_get_hits.fetch_add(1, Ordering::Relaxed);
                    let flags = store
                        .borrow()
                        .get_entry(&CompactKey::new(&key))
                        .map(|entry| entry.client_flags)
                        .unwrap_or(0);
                    MemcacheResponse::Values(vec![MemcacheValue { key, flags, data }])
                }
                None if matches!(response, CommandResponse::Nil) => {
                    shared.memcache_get_misses.fetch_add(1, Ordering::Relaxed);
                    MemcacheResponse::Values(vec![])
                }
                None => match response {
                    CommandResponse::Error(msg) => {
                        MemcacheResponse::ServerError(sanitize_error_message(&msg))
                    }
                    _ => MemcacheResponse::ServerError("unexpected get response".into()),
                },
            }
        }
        MemcacheRequest::Store {
            mode,
            key,
            flags,
            ttl_seconds,
            value,
        } => {
            shared.memcache_cmd_set.fetch_add(1, Ordering::Relaxed);
            let resp = execute_with_wal_inline(
                store,
                wal_writer,
                Command::Set {
                    key: key.clone(),
                    value,
                    ex: ttl_seconds,
                    px: None,
                    nx: matches!(mode, MemcacheStoreMode::Add),
                    xx: matches!(mode, MemcacheStoreMode::Replace),
                },
            );
            match resp {
                CommandResponse::Ok => {
                    if let Some(entry) = store.borrow_mut().get_entry_mut(&CompactKey::new(&key)) {
                        entry.client_flags = flags;
                    }
                    MemcacheResponse::Stored
                }
                CommandResponse::Nil => MemcacheResponse::NotStored,
                CommandResponse::Error(msg) => {
                    MemcacheResponse::ServerError(sanitize_error_message(&msg))
                }
                _ => MemcacheResponse::ServerError("unexpected store response".into()),
            }
        }
        MemcacheRequest::Delete { key } => match execute_with_wal_inline(
            store,
            wal_writer,
            Command::Del {
                keys: vec![key.clone()],
            },
        ) {
            CommandResponse::Integer(count) if count > 0 => MemcacheResponse::Deleted,
            CommandResponse::Integer(_) => MemcacheResponse::NotFound,
            CommandResponse::Error(msg) => {
                MemcacheResponse::ServerError(sanitize_error_message(&msg))
            }
            _ => MemcacheResponse::ServerError("unexpected delete response".into()),
        },
        MemcacheRequest::Touch { key, ttl_seconds } => {
            let cmd = match ttl_seconds {
                Some(seconds) => Command::Expire {
                    key: key.clone(),
                    seconds,
                },
                None => Command::Persist { key: key.clone() },
            };
            match execute_with_wal_inline(store, wal_writer, cmd) {
                CommandResponse::Integer(count) if count > 0 => MemcacheResponse::Touched,
                CommandResponse::Integer(_) => MemcacheResponse::NotFound,
                CommandResponse::Error(msg) => {
                    MemcacheResponse::ServerError(sanitize_error_message(&msg))
                }
                _ => MemcacheResponse::ServerError("unexpected touch response".into()),
            }
        }
        MemcacheRequest::Incr { key, value } => mutate_counter(store, wal_writer, key, value, true),
        MemcacheRequest::Decr { key, value } => {
            mutate_counter(store, wal_writer, key, value, false)
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn process_command(
    cmd: MemcacheCommand,
    shard_id: u16,
    store: &Rc<RefCell<ShardStore>>,
    wal_writer: &Rc<RefCell<Option<Box<dyn WalWriter>>>>,
    router: &ShardRouter,
    shared: &Arc<AffinitySharedState>,
    slots: &mut Vec<ResponseSlot>,
) {
    match cmd {
        MemcacheCommand::Get { keys } if keys.len() == 1 => {
            dispatch_request(
                MemcacheRequest::Get {
                    key: keys.into_iter().next().expect("len checked"),
                },
                false,
                shard_id,
                store,
                wal_writer,
                router,
                shared,
                slots,
            );
        }
        MemcacheCommand::Get { keys } => {
            let resp = handle_multi_get(keys, shard_id, store, wal_writer, router, shared).await;
            slots.push(ResponseSlot::Ready(Some(resp)));
        }
        MemcacheCommand::Store {
            mode,
            key,
            flags,
            ttl_seconds,
            value,
            noreply,
        } => {
            dispatch_request(
                MemcacheRequest::Store {
                    mode,
                    key,
                    flags,
                    ttl_seconds,
                    value,
                },
                noreply,
                shard_id,
                store,
                wal_writer,
                router,
                shared,
                slots,
            );
        }
        MemcacheCommand::Delete { key, noreply } => {
            dispatch_request(
                MemcacheRequest::Delete { key },
                noreply,
                shard_id,
                store,
                wal_writer,
                router,
                shared,
                slots,
            );
        }
        MemcacheCommand::Incr {
            key,
            value,
            noreply,
        } => {
            dispatch_request(
                MemcacheRequest::Incr { key, value },
                noreply,
                shard_id,
                store,
                wal_writer,
                router,
                shared,
                slots,
            );
        }
        MemcacheCommand::Decr {
            key,
            value,
            noreply,
        } => {
            dispatch_request(
                MemcacheRequest::Decr { key, value },
                noreply,
                shard_id,
                store,
                wal_writer,
                router,
                shared,
                slots,
            );
        }
        MemcacheCommand::Touch {
            key,
            ttl_seconds,
            noreply,
        } => {
            dispatch_request(
                MemcacheRequest::Touch { key, ttl_seconds },
                noreply,
                shard_id,
                store,
                wal_writer,
                router,
                shared,
                slots,
            );
        }
        MemcacheCommand::FlushAll {
            delay_seconds,
            noreply,
        } => {
            let resp = handle_flush_all(shared.clone(), delay_seconds).await;
            slots.push(ResponseSlot::Ready((!noreply).then_some(resp)));
        }
        MemcacheCommand::Stats { section } => {
            let resp = handle_stats(shared, section).await;
            slots.push(ResponseSlot::Ready(Some(resp)));
        }
        MemcacheCommand::Version => {
            slots.push(ResponseSlot::Ready(Some(MemcacheResponse::Version(
                env!("CARGO_PKG_VERSION").into(),
            ))));
        }
        MemcacheCommand::Quit => {}
    }
}

#[allow(clippy::too_many_arguments)]
fn dispatch_request(
    request: MemcacheRequest,
    noreply: bool,
    shard_id: u16,
    store: &Rc<RefCell<ShardStore>>,
    wal_writer: &Rc<RefCell<Option<Box<dyn WalWriter>>>>,
    router: &ShardRouter,
    shared: &Arc<AffinitySharedState>,
    slots: &mut Vec<ResponseSlot>,
) {
    let key = request.key().to_vec();
    let target_shard = router.shard_for_key(&key);
    if target_shard == shard_id {
        let resp = execute_memcache_inline(store, wal_writer, shared, request);
        slots.push(ResponseSlot::Ready((!noreply).then_some(resp)));
        return;
    }

    let (tx, rx) = oneshot::channel();
    match router.shard_senders[target_shard as usize].try_send(ShardRequest::ExecuteMemcache {
        request,
        response_tx: tx,
    }) {
        Ok(()) => slots.push(ResponseSlot::Pending { rx, noreply }),
        Err(_) => {
            let resp = MemcacheResponse::ServerError("shard unavailable".into());
            slots.push(ResponseSlot::Ready((!noreply).then_some(resp)));
        }
    }
}

async fn handle_multi_get(
    keys: Vec<Vec<u8>>,
    shard_id: u16,
    store: &Rc<RefCell<ShardStore>>,
    wal_writer: &Rc<RefCell<Option<Box<dyn WalWriter>>>>,
    router: &ShardRouter,
    shared: &Arc<AffinitySharedState>,
) -> MemcacheResponse {
    let mut ready_values: Vec<(usize, MemcacheValue)> = Vec::new();
    let mut pending = Vec::new();

    for (idx, key) in keys.into_iter().enumerate() {
        let target_shard = router.shard_for_key(&key);
        if target_shard == shard_id {
            if let MemcacheResponse::Values(values) =
                execute_memcache_inline(store, wal_writer, shared, MemcacheRequest::Get { key })
            {
                if let Some(value) = values.into_iter().next() {
                    ready_values.push((idx, value));
                }
            }
            continue;
        }

        let (tx, rx) = oneshot::channel();
        if router.shard_senders[target_shard as usize]
            .try_send(ShardRequest::ExecuteMemcache {
                request: MemcacheRequest::Get { key },
                response_tx: tx,
            })
            .is_ok()
        {
            pending.push((idx, rx));
        }
    }

    for (idx, rx) in pending {
        if let Ok(Ok(MemcacheResponse::Values(values))) =
            tokio::time::timeout(CROSS_SHARD_TIMEOUT, rx).await
        {
            if let Some(value) = values.into_iter().next() {
                ready_values.push((idx, value));
            }
        }
    }

    ready_values.sort_by_key(|(idx, _)| *idx);
    MemcacheResponse::Values(ready_values.into_iter().map(|(_, value)| value).collect())
}

async fn handle_flush_all(
    shared: Arc<AffinitySharedState>,
    delay_seconds: Option<u64>,
) -> MemcacheResponse {
    if let Some(delay) = delay_seconds.filter(|delay| *delay > 0) {
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(delay)).await;
            let _ = shared.router.dispatch_broadcast(|| Command::FlushAll).await;
        });
        return MemcacheResponse::Ok;
    }

    let responses = shared.router.dispatch_broadcast(|| Command::FlushAll).await;
    if responses
        .iter()
        .any(|resp| matches!(resp, CommandResponse::Error(_)))
    {
        MemcacheResponse::ServerError("flush_all failed".into())
    } else {
        MemcacheResponse::Ok
    }
}

async fn handle_stats(
    shared: &Arc<AffinitySharedState>,
    section: Option<String>,
) -> MemcacheResponse {
    if let Some(section) = section {
        let normalized = section.to_ascii_lowercase();
        if normalized != "settings" && normalized != "items" && normalized != "slabs" {
            return MemcacheResponse::Stats(vec![]);
        }
        if normalized != "settings" {
            return MemcacheResponse::Stats(vec![]);
        }
        return MemcacheResponse::Stats(vec![
            ("threads".into(), shared.router.shard_count().to_string()),
            (
                "tcpport".into(),
                shared
                    .memcached_bind_address
                    .as_deref()
                    .unwrap_or("")
                    .rsplit(':')
                    .next()
                    .unwrap_or("")
                    .to_string(),
            ),
        ]);
    }

    let responses = shared.router.dispatch_broadcast(|| Command::DbSize).await;
    let curr_items: u64 = responses
        .into_iter()
        .filter_map(|resp| match resp {
            CommandResponse::Integer(value) if value > 0 => Some(value as u64),
            _ => None,
        })
        .sum();
    let uptime = shared.started_at.elapsed().as_secs();
    let unix_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs();

    MemcacheResponse::Stats(vec![
        ("pid".into(), std::process::id().to_string()),
        ("uptime".into(), uptime.to_string()),
        ("time".into(), unix_time.to_string()),
        ("version".into(), env!("CARGO_PKG_VERSION").into()),
        ("curr_items".into(), curr_items.to_string()),
        (
            "curr_connections".into(),
            shared
                .memcache_current_connections
                .load(Ordering::Relaxed)
                .to_string(),
        ),
        (
            "total_connections".into(),
            shared
                .memcache_total_connections
                .load(Ordering::Relaxed)
                .to_string(),
        ),
        (
            "cmd_get".into(),
            shared.memcache_cmd_get.load(Ordering::Relaxed).to_string(),
        ),
        (
            "cmd_set".into(),
            shared.memcache_cmd_set.load(Ordering::Relaxed).to_string(),
        ),
        (
            "get_hits".into(),
            shared.memcache_get_hits.load(Ordering::Relaxed).to_string(),
        ),
        (
            "get_misses".into(),
            shared
                .memcache_get_misses
                .load(Ordering::Relaxed)
                .to_string(),
        ),
        ("threads".into(), shared.router.shard_count().to_string()),
    ])
}

fn mutate_counter(
    store: &Rc<RefCell<ShardStore>>,
    wal_writer: &Rc<RefCell<Option<Box<dyn WalWriter>>>>,
    key: Vec<u8>,
    delta: u64,
    incr: bool,
) -> MemcacheResponse {
    let get_resp = store
        .borrow_mut()
        .execute(Command::Get { key: key.clone() });
    let Some(current_bytes) = get_resp.bulk_string_bytes().map(|data| data.to_vec()) else {
        return match get_resp {
            CommandResponse::Nil => MemcacheResponse::NotFound,
            CommandResponse::Error(msg) => {
                MemcacheResponse::ServerError(sanitize_error_message(&msg))
            }
            _ => MemcacheResponse::ServerError("unexpected counter response".into()),
        };
    };

    let compact = CompactKey::new(&key);
    let (ttl_ms, flags) = {
        let store_ref = store.borrow();
        let Some(entry) = store_ref.get_entry(&compact) else {
            return MemcacheResponse::NotFound;
        };
        let ttl_ms = entry.remaining_ttl().map(duration_to_millis);
        (ttl_ms, entry.client_flags)
    };

    let current = match parse_counter_value(&current_bytes) {
        Ok(value) => value,
        Err(msg) => return MemcacheResponse::ClientError(msg),
    };
    let next = if incr {
        current.saturating_add(delta)
    } else {
        current.saturating_sub(delta)
    };
    let next_bytes = next.to_string().into_bytes();

    let resp = execute_with_wal_inline(
        store,
        wal_writer,
        Command::Set {
            key: key.clone(),
            value: next_bytes,
            ex: None,
            px: ttl_ms,
            nx: false,
            xx: false,
        },
    );
    match resp {
        CommandResponse::Ok => {
            if let Some(entry) = store.borrow_mut().get_entry_mut(&compact) {
                entry.client_flags = flags;
            }
            MemcacheResponse::Numeric(next)
        }
        CommandResponse::Error(msg) => MemcacheResponse::ServerError(sanitize_error_message(&msg)),
        _ => MemcacheResponse::ServerError("unexpected counter write response".into()),
    }
}

fn parse_counter_value(bytes: &[u8]) -> Result<u64, String> {
    let s = std::str::from_utf8(bytes)
        .map_err(|_| "cannot increment or decrement non-numeric value".to_string())?;
    s.parse::<u64>()
        .map_err(|_| "cannot increment or decrement non-numeric value".to_string())
}

fn duration_to_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u64::MAX as u128) as u64
}

fn protocol_error_response(err: ProtocolError) -> MemcacheResponse {
    match err {
        ProtocolError::UnknownCommand(_) => MemcacheResponse::Error,
        ProtocolError::WrongArity(msg) | ProtocolError::InvalidData(msg) => {
            MemcacheResponse::ClientError(sanitize_error_message(&msg))
        }
        ProtocolError::Incomplete => MemcacheResponse::ClientError("incomplete command".into()),
    }
}

fn sanitize_error_message(message: &str) -> String {
    message.replace(['\r', '\n'], " ")
}
