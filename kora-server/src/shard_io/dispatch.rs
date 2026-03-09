use std::cell::RefCell;
use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::rc::Rc;
use std::task::Poll;

use kora_core::command::{BitOperation, Command, CommandResponse};
use kora_core::hash::shard_for_key;
use kora_core::shard::{ShardStore, WalWriter};
use tokio::sync::oneshot;

use super::{execute_with_wal_inline, ShardRouter, CROSS_SHARD_TIMEOUT};

pub(crate) async fn handle_complex_command(
    cmd: Command,
    shard_id: u16,
    store: &Rc<RefCell<ShardStore>>,
    wal_writer: &Rc<RefCell<Option<Box<dyn WalWriter>>>>,
    router: &ShardRouter,
) -> CommandResponse {
    match cmd {
        Command::MGet { keys } => handle_mget(keys, shard_id, store, router).await,
        Command::MSet { entries } => {
            handle_mset(entries, shard_id, store, wal_writer, router).await
        }
        Command::Del { keys } => handle_del_multi(keys, shard_id, store, wal_writer, router).await,
        Command::Exists { keys } => handle_exists_multi(keys, shard_id, store, router).await,
        Command::DbSize => handle_broadcast_sum(Command::DbSize, shard_id, store, router).await,
        Command::FlushDb | Command::FlushAll => {
            handle_broadcast_ok(Command::FlushDb, shard_id, store, wal_writer, router).await
        }
        Command::Keys { ref pattern } => {
            handle_broadcast_keys(pattern.clone(), shard_id, store, router).await
        }
        Command::Scan {
            cursor,
            pattern,
            count,
        } => handle_broadcast_scan(cursor, pattern, count, shard_id, store, router).await,
        Command::Dump => handle_broadcast_merge(Command::Dump, shard_id, store, router).await,
        Command::VecQuery {
            ref key,
            k,
            ref vector,
        } => handle_vec_query(key.clone(), k, vector.clone(), shard_id, store, router).await,
        Command::StatsHotkeys { count } => {
            handle_stats_hotkeys(count, shard_id, store, router).await
        }
        Command::StatsMemory { ref prefixes } => {
            handle_stats_memory(prefixes.clone(), shard_id, store, router).await
        }
        Command::Unlink { keys } => {
            handle_del_multi(keys, shard_id, store, wal_writer, router).await
        }
        Command::Touch { keys } => handle_touch_multi(keys, shard_id, store, router).await,
        Command::MSetNx { entries } => {
            handle_msetnx(entries, shard_id, store, wal_writer, router).await
        }
        Command::RandomKey => handle_randomkey(shard_id, store, router).await,
        Command::RPopLPush {
            source,
            destination,
        } => {
            handle_lmove(
                source,
                destination,
                false,
                true,
                shard_id,
                store,
                wal_writer,
                router,
            )
            .await
        }
        Command::LMove {
            source,
            destination,
            from_left,
            to_left,
        } => {
            handle_lmove(
                source,
                destination,
                from_left,
                to_left,
                shard_id,
                store,
                wal_writer,
                router,
            )
            .await
        }
        Command::SUnion { keys } => {
            handle_set_op(keys, SetOp::Union, shard_id, store, router).await
        }
        Command::SUnionStore { destination, keys } => {
            handle_set_store_op(
                destination,
                keys,
                SetOp::Union,
                shard_id,
                store,
                wal_writer,
                router,
            )
            .await
        }
        Command::SInter { keys } => {
            handle_set_op(keys, SetOp::Inter, shard_id, store, router).await
        }
        Command::SInterStore { destination, keys } => {
            handle_set_store_op(
                destination,
                keys,
                SetOp::Inter,
                shard_id,
                store,
                wal_writer,
                router,
            )
            .await
        }
        Command::SDiff { keys } => handle_set_op(keys, SetOp::Diff, shard_id, store, router).await,
        Command::SDiffStore { destination, keys } => {
            handle_set_store_op(
                destination,
                keys,
                SetOp::Diff,
                shard_id,
                store,
                wal_writer,
                router,
            )
            .await
        }
        Command::SInterCard {
            numkeys: _,
            keys,
            limit,
        } => handle_sintercard(keys, limit, shard_id, store, router).await,
        Command::PfCount { keys } if keys.len() > 1 => {
            handle_pfcount_multi(keys, shard_id, store, router).await
        }
        Command::PfMerge {
            destkey,
            sourcekeys,
        } => handle_pfmerge(destkey, sourcekeys, shard_id, store, wal_writer, router).await,
        Command::BitOp {
            operation,
            destkey,
            keys,
        } => {
            handle_bitop(
                operation, destkey, keys, shard_id, store, wal_writer, router,
            )
            .await
        }
        Command::SMove {
            source,
            destination,
            member,
        } => {
            handle_smove(
                source,
                destination,
                member,
                shard_id,
                store,
                wal_writer,
                router,
            )
            .await
        }
        Command::BLPop { keys, timeout } => {
            handle_blocking_pop(keys, timeout, true, shard_id, store, wal_writer, router).await
        }
        Command::BRPop { keys, timeout } => {
            handle_blocking_pop(keys, timeout, false, shard_id, store, wal_writer, router).await
        }
        Command::BZPopMin { keys, timeout } => {
            handle_blocking_zpop(keys, timeout, true, shard_id, store, wal_writer, router).await
        }
        Command::BZPopMax { keys, timeout } => {
            handle_blocking_zpop(keys, timeout, false, shard_id, store, wal_writer, router).await
        }
        Command::BLMove {
            source,
            destination,
            from_left,
            to_left,
            ..
        } => {
            handle_lmove(
                source,
                destination,
                from_left,
                to_left,
                shard_id,
                store,
                wal_writer,
                router,
            )
            .await
        }
        Command::XRead { keys, ids, count } => {
            handle_xread(keys, ids, count, shard_id, store, router).await
        }
        Command::XReadGroup {
            group,
            consumer,
            count,
            keys,
            ids,
        } => {
            handle_xreadgroup(
                group, consumer, count, keys, ids, shard_id, store, wal_writer, router,
            )
            .await
        }
        _ => execute_with_wal_inline(store, wal_writer, cmd),
    }
}

const WRONGTYPE_ERROR: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";

fn wrongtype_response() -> CommandResponse {
    CommandResponse::Error(WRONGTYPE_ERROR.into())
}

async fn ensure_destination_type(
    destination: &[u8],
    expected: &str,
    shard_id: u16,
    store: &Rc<RefCell<ShardStore>>,
    router: &ShardRouter,
) -> Result<(), CommandResponse> {
    let destination_shard = shard_for_key(destination, router.shard_count());
    let type_cmd = Command::Type {
        key: destination.to_vec(),
    };
    let type_resp = if destination_shard == shard_id {
        store.borrow_mut().execute(type_cmd)
    } else {
        route_and_wait(destination_shard, type_cmd, router).await
    };

    match type_resp {
        CommandResponse::SimpleString(kind) if kind == "none" || kind == expected => Ok(()),
        CommandResponse::SimpleString(_) => Err(wrongtype_response()),
        CommandResponse::Error(e) => Err(CommandResponse::Error(e)),
        _ => Err(CommandResponse::Error("ERR internal error".into())),
    }
}

async fn restore_list_source(
    source: Vec<u8>,
    value: Vec<u8>,
    from_left: bool,
    shard_id: u16,
    store: &Rc<RefCell<ShardStore>>,
    wal_writer: &Rc<RefCell<Option<Box<dyn WalWriter>>>>,
    router: &ShardRouter,
) {
    let source_shard = shard_for_key(&source, router.shard_count());
    let rollback_cmd = if from_left {
        Command::LPush {
            key: source,
            values: vec![value],
        }
    } else {
        Command::RPush {
            key: source,
            values: vec![value],
        }
    };

    if source_shard == shard_id {
        let _ = execute_with_wal_inline(store, wal_writer, rollback_cmd);
    } else {
        let _ = route_and_wait(source_shard, rollback_cmd, router).await;
    }
}

async fn restore_set_source(
    source: Vec<u8>,
    member: Vec<u8>,
    shard_id: u16,
    store: &Rc<RefCell<ShardStore>>,
    wal_writer: &Rc<RefCell<Option<Box<dyn WalWriter>>>>,
    router: &ShardRouter,
) {
    let source_shard = shard_for_key(&source, router.shard_count());
    let rollback_cmd = Command::SAdd {
        key: source,
        members: vec![member],
    };

    if source_shard == shard_id {
        let _ = execute_with_wal_inline(store, wal_writer, rollback_cmd);
    } else {
        let _ = route_and_wait(source_shard, rollback_cmd, router).await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_blocking_pop(
    keys: Vec<Vec<u8>>,
    _timeout: f64,
    is_left: bool,
    shard_id: u16,
    store: &Rc<RefCell<ShardStore>>,
    wal_writer: &Rc<RefCell<Option<Box<dyn WalWriter>>>>,
    router: &ShardRouter,
) -> CommandResponse {
    for key in &keys {
        let target_shard = shard_for_key(key, router.shard_count());
        let cmd = if is_left {
            Command::LPop { key: key.clone() }
        } else {
            Command::RPop { key: key.clone() }
        };

        let resp = if target_shard == shard_id {
            execute_with_wal_inline(store, wal_writer, cmd)
        } else {
            route_and_wait(target_shard, cmd, router).await
        };

        if !matches!(resp, CommandResponse::Nil) {
            return CommandResponse::Array(vec![CommandResponse::BulkString(key.clone()), resp]);
        }
    }
    CommandResponse::Nil
}

#[allow(clippy::too_many_arguments)]
async fn handle_blocking_zpop(
    keys: Vec<Vec<u8>>,
    _timeout: f64,
    is_min: bool,
    shard_id: u16,
    store: &Rc<RefCell<ShardStore>>,
    wal_writer: &Rc<RefCell<Option<Box<dyn WalWriter>>>>,
    router: &ShardRouter,
) -> CommandResponse {
    for key in &keys {
        let target_shard = shard_for_key(key, router.shard_count());
        let cmd = if is_min {
            Command::ZPopMin {
                key: key.clone(),
                count: Some(1),
            }
        } else {
            Command::ZPopMax {
                key: key.clone(),
                count: Some(1),
            }
        };

        let resp = if target_shard == shard_id {
            execute_with_wal_inline(store, wal_writer, cmd)
        } else {
            route_and_wait(target_shard, cmd, router).await
        };

        if let CommandResponse::Array(ref items) = resp {
            if !items.is_empty() {
                return CommandResponse::Array(vec![
                    CommandResponse::BulkString(key.clone()),
                    resp,
                ]);
            }
        }
    }
    CommandResponse::Nil
}

async fn handle_xread(
    keys: Vec<Vec<u8>>,
    ids: Vec<Vec<u8>>,
    count: Option<usize>,
    shard_id: u16,
    store: &Rc<RefCell<ShardStore>>,
    router: &ShardRouter,
) -> CommandResponse {
    if keys.len() != ids.len() {
        return CommandResponse::Error(
            "ERR Unbalanced XREAD list of streams: for each stream key an ID must be specified"
                .into(),
        );
    }

    let mut results: Vec<CommandResponse> = Vec::new();
    for (key, id) in keys.iter().zip(ids.iter()) {
        let target_shard = shard_for_key(key, router.shard_count());
        let cmd = Command::XRead {
            keys: vec![key.clone()],
            ids: vec![id.clone()],
            count,
        };

        let resp = if target_shard == shard_id {
            store.borrow_mut().execute(cmd)
        } else {
            route_and_wait(target_shard, cmd, router).await
        };

        match resp {
            CommandResponse::Array(items) => results.extend(items),
            CommandResponse::Nil => {}
            CommandResponse::Error(e) => return CommandResponse::Error(e),
            _ => return CommandResponse::Error("ERR internal error".into()),
        }
    }

    if results.is_empty() {
        CommandResponse::Nil
    } else {
        CommandResponse::Array(results)
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_xreadgroup(
    group: String,
    consumer: String,
    count: Option<usize>,
    keys: Vec<Vec<u8>>,
    ids: Vec<Vec<u8>>,
    shard_id: u16,
    store: &Rc<RefCell<ShardStore>>,
    wal_writer: &Rc<RefCell<Option<Box<dyn WalWriter>>>>,
    router: &ShardRouter,
) -> CommandResponse {
    let mut results: Vec<CommandResponse> = Vec::new();

    for (key, id) in keys.iter().zip(ids.iter()) {
        let target_shard = shard_for_key(key, router.shard_count());
        let cmd = Command::XReadGroup {
            group: group.clone(),
            consumer: consumer.clone(),
            count,
            keys: vec![key.clone()],
            ids: vec![id.clone()],
        };

        let resp = if target_shard == shard_id {
            execute_with_wal_inline(store, wal_writer, cmd)
        } else {
            route_and_wait(target_shard, cmd, router).await
        };

        if let CommandResponse::Array(items) = resp {
            results.extend(items);
        }
    }

    if results.is_empty() {
        CommandResponse::Nil
    } else {
        CommandResponse::Array(results)
    }
}

async fn route_and_wait(target_shard: u16, cmd: Command, router: &ShardRouter) -> CommandResponse {
    match router.try_dispatch_foreign(target_shard, cmd) {
        Ok(rx) => match tokio::time::timeout(CROSS_SHARD_TIMEOUT, rx).await {
            Ok(Ok(resp)) => resp,
            _ => CommandResponse::Error("ERR internal error".into()),
        },
        Err(e) => e,
    }
}

enum SetOp {
    Union,
    Inter,
    Diff,
}

async fn resolve_receivers(
    receivers: Vec<oneshot::Receiver<CommandResponse>>,
) -> Vec<CommandResponse> {
    resolve_tagged_receivers(receivers.into_iter().map(|rx| ((), rx)).collect())
        .await
        .into_iter()
        .map(|(_, resp)| resp)
        .collect()
}

async fn resolve_tagged_receivers<T>(
    receivers: Vec<(T, oneshot::Receiver<CommandResponse>)>,
) -> Vec<(T, CommandResponse)> {
    let mut pending: Vec<(Option<T>, Option<oneshot::Receiver<CommandResponse>>)> = receivers
        .into_iter()
        .map(|(tag, rx)| (Some(tag), Some(rx)))
        .collect();
    let mut responses: Vec<Option<CommandResponse>> = vec![None; pending.len()];

    if !pending.is_empty() {
        let wait_for_pending = poll_fn(|cx| {
            let mut any_pending = false;

            for (index, (_, receiver)) in pending.iter_mut().enumerate() {
                let Some(rx) = receiver.as_mut() else {
                    continue;
                };

                match Pin::new(rx).poll(cx) {
                    Poll::Ready(Ok(resp)) => {
                        responses[index] = Some(resp);
                        *receiver = None;
                    }
                    Poll::Ready(Err(_)) => {
                        responses[index] = Some(CommandResponse::Error("ERR shard error".into()));
                        *receiver = None;
                    }
                    Poll::Pending => any_pending = true,
                }
            }

            if any_pending {
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        });

        if tokio::time::timeout(CROSS_SHARD_TIMEOUT, wait_for_pending)
            .await
            .is_err()
        {
            for response in &mut responses {
                if response.is_none() {
                    *response = Some(CommandResponse::Error("ERR shard error".into()));
                }
            }
        }
    }

    pending
        .into_iter()
        .zip(responses)
        .map(|((tag, _), response)| {
            (
                tag.expect("tag missing before resolution"),
                response.unwrap_or_else(|| CommandResponse::Error("ERR shard error".into())),
            )
        })
        .collect()
}

async fn handle_mget(
    keys: Vec<Vec<u8>>,
    shard_id: u16,
    store: &Rc<RefCell<ShardStore>>,
    router: &ShardRouter,
) -> CommandResponse {
    let shard_count = router.shard_count();
    let mut results = vec![CommandResponse::Nil; keys.len()];
    let mut shard_requests: Vec<Vec<(usize, Vec<u8>)>> = vec![vec![]; shard_count];

    for (i, key) in keys.iter().enumerate() {
        let target = shard_for_key(key, shard_count) as usize;
        shard_requests[target].push((i, key.clone()));
    }

    let mut receivers: Vec<(Vec<usize>, oneshot::Receiver<CommandResponse>)> = Vec::new();

    for (target, reqs) in shard_requests.into_iter().enumerate() {
        if reqs.is_empty() {
            continue;
        }
        let indices: Vec<usize> = reqs.iter().map(|(i, _)| *i).collect();
        let shard_keys: Vec<Vec<u8>> = reqs.into_iter().map(|(_, k)| k).collect();

        if target as u16 == shard_id {
            let resp = store
                .borrow_mut()
                .execute(Command::MGet { keys: shard_keys });
            if let CommandResponse::Array(values) = resp {
                for (idx, val) in indices.into_iter().zip(values) {
                    results[idx] = val;
                }
            }
        } else if let Ok(rx) =
            router.try_dispatch_foreign(target as u16, Command::MGet { keys: shard_keys })
        {
            receivers.push((indices, rx));
        }
    }

    for (indices, resp) in resolve_tagged_receivers(receivers).await {
        if let CommandResponse::Array(values) = resp {
            for (idx, val) in indices.into_iter().zip(values) {
                results[idx] = val;
            }
        }
    }

    CommandResponse::Array(results)
}

async fn handle_mset(
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    shard_id: u16,
    store: &Rc<RefCell<ShardStore>>,
    wal_writer: &Rc<RefCell<Option<Box<dyn WalWriter>>>>,
    router: &ShardRouter,
) -> CommandResponse {
    let shard_count = router.shard_count();
    let mut shard_entries: Vec<Vec<(Vec<u8>, Vec<u8>)>> = vec![vec![]; shard_count];

    for (key, value) in entries {
        let target = shard_for_key(&key, shard_count) as usize;
        shard_entries[target].push((key, value));
    }

    let mut receivers = Vec::new();

    for (target, entries) in shard_entries.into_iter().enumerate() {
        if entries.is_empty() {
            continue;
        }
        if target as u16 == shard_id {
            execute_with_wal_inline(store, wal_writer, Command::MSet { entries });
        } else if let Ok(rx) = router.try_dispatch_foreign(target as u16, Command::MSet { entries })
        {
            receivers.push(rx);
        }
    }

    let _ = resolve_receivers(receivers).await;
    CommandResponse::Ok
}

async fn handle_del_multi(
    keys: Vec<Vec<u8>>,
    shard_id: u16,
    store: &Rc<RefCell<ShardStore>>,
    wal_writer: &Rc<RefCell<Option<Box<dyn WalWriter>>>>,
    router: &ShardRouter,
) -> CommandResponse {
    let shard_count = router.shard_count();
    let mut shard_keys: Vec<Vec<Vec<u8>>> = vec![vec![]; shard_count];

    for key in keys {
        let target = shard_for_key(&key, shard_count) as usize;
        shard_keys[target].push(key);
    }

    let mut total = 0i64;
    let mut receivers = Vec::new();

    for (target, keys) in shard_keys.into_iter().enumerate() {
        if keys.is_empty() {
            continue;
        }
        if target as u16 == shard_id {
            let resp = execute_with_wal_inline(store, wal_writer, Command::Del { keys });
            if let CommandResponse::Integer(n) = resp {
                total += n;
            }
        } else if let Ok(rx) = router.try_dispatch_foreign(target as u16, Command::Del { keys }) {
            receivers.push(rx);
        }
    }

    for resp in resolve_receivers(receivers).await {
        if let CommandResponse::Integer(n) = resp {
            total += n;
        }
    }

    CommandResponse::Integer(total)
}

async fn handle_exists_multi(
    keys: Vec<Vec<u8>>,
    shard_id: u16,
    store: &Rc<RefCell<ShardStore>>,
    router: &ShardRouter,
) -> CommandResponse {
    let shard_count = router.shard_count();
    let mut shard_keys: Vec<Vec<Vec<u8>>> = vec![vec![]; shard_count];

    for key in keys {
        let target = shard_for_key(&key, shard_count) as usize;
        shard_keys[target].push(key);
    }

    let mut total = 0i64;
    let mut receivers = Vec::new();

    for (target, keys) in shard_keys.into_iter().enumerate() {
        if keys.is_empty() {
            continue;
        }
        if target as u16 == shard_id {
            let resp = store.borrow_mut().execute(Command::Exists { keys });
            if let CommandResponse::Integer(n) = resp {
                total += n;
            }
        } else if let Ok(rx) = router.try_dispatch_foreign(target as u16, Command::Exists { keys })
        {
            receivers.push(rx);
        }
    }

    for resp in resolve_receivers(receivers).await {
        if let CommandResponse::Integer(n) = resp {
            total += n;
        }
    }

    CommandResponse::Integer(total)
}

async fn handle_broadcast_sum(
    cmd: Command,
    shard_id: u16,
    store: &Rc<RefCell<ShardStore>>,
    router: &ShardRouter,
) -> CommandResponse {
    let local_resp = store.borrow_mut().execute(cmd.clone());
    let local_val = match local_resp {
        CommandResponse::Integer(n) => n,
        _ => 0,
    };

    let receivers = router.try_dispatch_foreign_broadcast(shard_id, || cmd.clone());
    let mut total = local_val;
    for resp in resolve_receivers(receivers).await {
        if let CommandResponse::Integer(n) = resp {
            total += n;
        }
    }

    CommandResponse::Integer(total)
}

async fn handle_broadcast_ok(
    cmd: Command,
    shard_id: u16,
    store: &Rc<RefCell<ShardStore>>,
    wal_writer: &Rc<RefCell<Option<Box<dyn WalWriter>>>>,
    router: &ShardRouter,
) -> CommandResponse {
    execute_with_wal_inline(store, wal_writer, cmd.clone());
    let receivers = router.try_dispatch_foreign_broadcast(shard_id, || cmd.clone());
    let _ = resolve_receivers(receivers).await;
    CommandResponse::Ok
}

async fn handle_broadcast_keys(
    pattern: String,
    shard_id: u16,
    store: &Rc<RefCell<ShardStore>>,
    router: &ShardRouter,
) -> CommandResponse {
    let local_resp = store.borrow_mut().execute(Command::Keys {
        pattern: pattern.clone(),
    });
    let mut all_keys = match local_resp {
        CommandResponse::Array(keys) => keys,
        _ => Vec::new(),
    };

    let receivers = router.try_dispatch_foreign_broadcast(shard_id, || Command::Keys {
        pattern: pattern.clone(),
    });

    for resp in resolve_receivers(receivers).await {
        if let CommandResponse::Array(keys) = resp {
            all_keys.extend(keys);
        }
    }

    CommandResponse::Array(all_keys)
}

async fn handle_broadcast_scan(
    cursor: u64,
    pattern: Option<String>,
    count: Option<usize>,
    shard_id: u16,
    store: &Rc<RefCell<ShardStore>>,
    router: &ShardRouter,
) -> CommandResponse {
    let pat = pattern.unwrap_or_else(|| "*".to_string());
    let local_resp = store.borrow_mut().execute(Command::Keys {
        pattern: pat.clone(),
    });
    let mut merged_keys: Vec<Vec<u8>> = Vec::new();

    if let CommandResponse::Array(keys) = local_resp {
        for key in keys {
            if let CommandResponse::BulkString(raw) = key {
                merged_keys.push(raw);
            }
        }
    }

    let receivers = router.try_dispatch_foreign_broadcast(shard_id, || Command::Keys {
        pattern: pat.clone(),
    });

    for resp in resolve_receivers(receivers).await {
        if let CommandResponse::Array(keys) = resp {
            for key in keys {
                if let CommandResponse::BulkString(raw) = key {
                    merged_keys.push(raw);
                }
            }
        }
    }

    merged_keys.sort();
    let start = cursor as usize;
    let limit = count.unwrap_or(10).max(1);
    if start >= merged_keys.len() {
        return CommandResponse::Array(vec![
            CommandResponse::BulkString(b"0".to_vec()),
            CommandResponse::Array(vec![]),
        ]);
    }
    let end = start.saturating_add(limit).min(merged_keys.len());
    let result_keys: Vec<CommandResponse> = merged_keys[start..end]
        .iter()
        .map(|k| CommandResponse::BulkString(k.clone()))
        .collect();
    let next_cursor = if end >= merged_keys.len() { 0 } else { end };
    CommandResponse::Array(vec![
        CommandResponse::BulkString(next_cursor.to_string().into_bytes()),
        CommandResponse::Array(result_keys),
    ])
}

async fn handle_broadcast_merge(
    cmd: Command,
    shard_id: u16,
    store: &Rc<RefCell<ShardStore>>,
    router: &ShardRouter,
) -> CommandResponse {
    let local_resp = store.borrow_mut().execute(cmd.clone());
    let mut all_entries = match local_resp {
        CommandResponse::Array(entries) => entries,
        _ => Vec::new(),
    };

    let receivers = router.try_dispatch_foreign_broadcast(shard_id, || cmd.clone());
    for resp in resolve_receivers(receivers).await {
        if let CommandResponse::Array(entries) = resp {
            all_entries.extend(entries);
        }
    }

    CommandResponse::Array(all_entries)
}

async fn handle_vec_query(
    key: Vec<u8>,
    k: usize,
    vector: Vec<f32>,
    shard_id: u16,
    store: &Rc<RefCell<ShardStore>>,
    router: &ShardRouter,
) -> CommandResponse {
    let local_resp = store.borrow_mut().execute(Command::VecQuery {
        key: key.clone(),
        k,
        vector: vector.clone(),
    });

    let mut all_results: Vec<(i64, Vec<u8>)> = Vec::new();
    extract_vec_results(&local_resp, &mut all_results);

    let receivers = router.try_dispatch_foreign_broadcast(shard_id, || Command::VecQuery {
        key: key.clone(),
        k,
        vector: vector.clone(),
    });

    for resp in resolve_receivers(receivers).await {
        extract_vec_results(&resp, &mut all_results);
    }

    all_results.sort_by(|a, b| {
        let da: f64 = String::from_utf8_lossy(&a.1).parse().unwrap_or(f64::MAX);
        let db: f64 = String::from_utf8_lossy(&b.1).parse().unwrap_or(f64::MAX);
        da.partial_cmp(&db).unwrap_or(std::cmp::Ordering::Equal)
    });
    all_results.truncate(k);

    let items: Vec<CommandResponse> = all_results
        .into_iter()
        .map(|(id, dist)| {
            CommandResponse::Array(vec![
                CommandResponse::Integer(id),
                CommandResponse::BulkString(dist),
            ])
        })
        .collect();
    CommandResponse::Array(items)
}

fn extract_vec_results(resp: &CommandResponse, out: &mut Vec<(i64, Vec<u8>)>) {
    if let CommandResponse::Array(items) = resp {
        for item in items {
            if let CommandResponse::Array(pair) = item {
                if pair.len() == 2 {
                    if let (CommandResponse::Integer(id), CommandResponse::BulkString(dist)) =
                        (&pair[0], &pair[1])
                    {
                        out.push((*id, dist.clone()));
                    }
                }
            }
        }
    }
}

async fn handle_stats_hotkeys(
    count: usize,
    shard_id: u16,
    store: &Rc<RefCell<ShardStore>>,
    router: &ShardRouter,
) -> CommandResponse {
    let local_resp = store.borrow_mut().execute(Command::StatsHotkeys { count });

    let mut all_hot: Vec<(Vec<u8>, i64)> = Vec::new();
    extract_hotkey_results(&local_resp, &mut all_hot);

    let receivers =
        router.try_dispatch_foreign_broadcast(shard_id, || Command::StatsHotkeys { count });

    for resp in resolve_receivers(receivers).await {
        extract_hotkey_results(&resp, &mut all_hot);
    }

    all_hot.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    all_hot.truncate(count);

    let items = all_hot
        .into_iter()
        .map(|(key, freq)| {
            CommandResponse::Array(vec![
                CommandResponse::BulkString(key),
                CommandResponse::Integer(freq),
            ])
        })
        .collect();
    CommandResponse::Array(items)
}

fn extract_hotkey_results(resp: &CommandResponse, out: &mut Vec<(Vec<u8>, i64)>) {
    if let CommandResponse::Array(hotkeys) = resp {
        for hotkey in hotkeys {
            if let CommandResponse::Array(pair) = hotkey {
                if pair.len() == 2 {
                    if let (CommandResponse::BulkString(key), CommandResponse::Integer(freq)) =
                        (&pair[0], &pair[1])
                    {
                        out.push((key.clone(), *freq));
                    }
                }
            }
        }
    }
}

async fn handle_stats_memory(
    prefixes: Vec<Vec<u8>>,
    shard_id: u16,
    store: &Rc<RefCell<ShardStore>>,
    router: &ShardRouter,
) -> CommandResponse {
    let local_resp = store.borrow_mut().execute(Command::StatsMemory {
        prefixes: prefixes.clone(),
    });

    let mut totals = vec![0i64; prefixes.len()];
    if let CommandResponse::Array(items) = &local_resp {
        for (idx, item) in items.iter().enumerate().take(totals.len()) {
            if let CommandResponse::Integer(val) = item {
                totals[idx] = totals[idx].saturating_add(*val);
            }
        }
    }

    let receivers = router.try_dispatch_foreign_broadcast(shard_id, || Command::StatsMemory {
        prefixes: prefixes.clone(),
    });

    for resp in resolve_receivers(receivers).await {
        if let CommandResponse::Array(items) = resp {
            for (idx, item) in items.into_iter().enumerate().take(totals.len()) {
                if let CommandResponse::Integer(val) = item {
                    totals[idx] = totals[idx].saturating_add(val);
                }
            }
        }
    }

    let merged = totals.into_iter().map(CommandResponse::Integer).collect();
    CommandResponse::Array(merged)
}

async fn handle_touch_multi(
    keys: Vec<Vec<u8>>,
    shard_id: u16,
    store: &Rc<RefCell<ShardStore>>,
    router: &ShardRouter,
) -> CommandResponse {
    let shard_count = router.shard_count();
    let mut shard_keys: Vec<Vec<Vec<u8>>> = vec![vec![]; shard_count];

    for key in keys {
        let target = shard_for_key(&key, shard_count) as usize;
        shard_keys[target].push(key);
    }

    let mut total = 0i64;
    let mut receivers = Vec::new();

    for (target, keys) in shard_keys.into_iter().enumerate() {
        if keys.is_empty() {
            continue;
        }
        if target as u16 == shard_id {
            let resp = store.borrow_mut().execute(Command::Touch { keys });
            if let CommandResponse::Integer(n) = resp {
                total += n;
            }
        } else if let Ok(rx) = router.try_dispatch_foreign(target as u16, Command::Touch { keys }) {
            receivers.push(rx);
        }
    }

    for resp in resolve_receivers(receivers).await {
        if let CommandResponse::Integer(n) = resp {
            total += n;
        }
    }

    CommandResponse::Integer(total)
}

async fn handle_msetnx(
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    shard_id: u16,
    store: &Rc<RefCell<ShardStore>>,
    wal_writer: &Rc<RefCell<Option<Box<dyn WalWriter>>>>,
    router: &ShardRouter,
) -> CommandResponse {
    let shard_count = router.shard_count();
    let mut shard_entries: Vec<Vec<(Vec<u8>, Vec<u8>)>> = vec![vec![]; shard_count];

    for (key, value) in entries {
        let target = shard_for_key(&key, shard_count) as usize;
        shard_entries[target].push((key, value));
    }

    let mut exists_receivers = Vec::new();
    for (target, entries) in shard_entries.iter().enumerate() {
        if entries.is_empty() {
            continue;
        }
        let keys: Vec<Vec<u8>> = entries.iter().map(|(key, _)| key.clone()).collect();
        if target as u16 == shard_id {
            let exists_resp = store.borrow_mut().execute(Command::Exists { keys });
            match exists_resp {
                CommandResponse::Integer(n) if n > 0 => return CommandResponse::Integer(0),
                CommandResponse::Integer(_) => {}
                CommandResponse::Error(e) => return CommandResponse::Error(e),
                _ => return CommandResponse::Error("ERR internal error".into()),
            }
        } else {
            match router.try_dispatch_foreign(target as u16, Command::Exists { keys }) {
                Ok(rx) => exists_receivers.push(rx),
                Err(e) => return e,
            }
        }
    }

    for exists_resp in resolve_receivers(exists_receivers).await {
        match exists_resp {
            CommandResponse::Integer(n) if n > 0 => return CommandResponse::Integer(0),
            CommandResponse::Integer(_) => {}
            CommandResponse::Error(e) => return CommandResponse::Error(e),
            _ => return CommandResponse::Error("ERR internal error".into()),
        }
    }

    let mut set_receivers = Vec::new();
    for (target, entries) in shard_entries.into_iter().enumerate() {
        if entries.is_empty() {
            continue;
        }
        let set_cmd = Command::MSet { entries };
        if target as u16 == shard_id {
            let set_resp = execute_with_wal_inline(store, wal_writer, set_cmd);
            if let CommandResponse::Error(e) = set_resp {
                return CommandResponse::Error(e);
            }
        } else {
            match router.try_dispatch_foreign(target as u16, set_cmd) {
                Ok(rx) => set_receivers.push(rx),
                Err(e) => return e,
            }
        }
    }

    for set_resp in resolve_receivers(set_receivers).await {
        if let CommandResponse::Error(e) = set_resp {
            return CommandResponse::Error(e);
        }
    }

    CommandResponse::Integer(1)
}

async fn handle_randomkey(
    shard_id: u16,
    store: &Rc<RefCell<ShardStore>>,
    router: &ShardRouter,
) -> CommandResponse {
    let local_resp = store.borrow_mut().execute(Command::RandomKey);
    if !matches!(local_resp, CommandResponse::Nil) {
        return local_resp;
    }

    let receivers = router.try_dispatch_foreign_broadcast(shard_id, || Command::RandomKey);
    for resp in resolve_receivers(receivers).await {
        if !matches!(resp, CommandResponse::Nil) {
            return resp;
        }
    }

    CommandResponse::Nil
}

#[allow(clippy::too_many_arguments)]
async fn handle_lmove(
    source: Vec<u8>,
    destination: Vec<u8>,
    from_left: bool,
    to_left: bool,
    shard_id: u16,
    store: &Rc<RefCell<ShardStore>>,
    wal_writer: &Rc<RefCell<Option<Box<dyn WalWriter>>>>,
    router: &ShardRouter,
) -> CommandResponse {
    if let Err(resp) = ensure_destination_type(&destination, "list", shard_id, store, router).await
    {
        return resp;
    }

    let src_shard = shard_for_key(&source, router.shard_count());
    let dst_shard = shard_for_key(&destination, router.shard_count());
    let source_key = source.clone();

    let pop_cmd = if from_left {
        Command::LPop { key: source }
    } else {
        Command::RPop { key: source }
    };

    let popped = if src_shard == shard_id {
        execute_with_wal_inline(store, wal_writer, pop_cmd)
    } else {
        match router.try_dispatch_foreign(src_shard, pop_cmd) {
            Ok(rx) => match tokio::time::timeout(CROSS_SHARD_TIMEOUT, rx).await {
                Ok(Ok(resp)) => resp,
                _ => return CommandResponse::Error("ERR cross-shard timeout".into()),
            },
            Err(e) => return e,
        }
    };

    let value_bytes = match &popped {
        CommandResponse::BulkString(b) => b.clone(),
        CommandResponse::BulkStringShared(b) => b.to_vec(),
        CommandResponse::Nil => return CommandResponse::Nil,
        CommandResponse::Error(_) => return popped,
        _ => return CommandResponse::Nil,
    };

    let push_value = value_bytes.clone();
    let push_cmd = if to_left {
        Command::LPush {
            key: destination,
            values: vec![push_value],
        }
    } else {
        Command::RPush {
            key: destination,
            values: vec![push_value],
        }
    };

    let push_resp = if dst_shard == shard_id {
        execute_with_wal_inline(store, wal_writer, push_cmd)
    } else if let Ok(rx) = router.try_dispatch_foreign(dst_shard, push_cmd) {
        match tokio::time::timeout(CROSS_SHARD_TIMEOUT, rx).await {
            Ok(Ok(resp)) => resp,
            _ => CommandResponse::Error("ERR cross-shard timeout".into()),
        }
    } else {
        CommandResponse::Error("ERR shard unavailable".into())
    };

    match push_resp {
        CommandResponse::Integer(_) => popped,
        CommandResponse::Error(e) => {
            restore_list_source(
                source_key,
                value_bytes,
                from_left,
                shard_id,
                store,
                wal_writer,
                router,
            )
            .await;
            CommandResponse::Error(e)
        }
        _ => {
            restore_list_source(
                source_key,
                value_bytes,
                from_left,
                shard_id,
                store,
                wal_writer,
                router,
            )
            .await;
            CommandResponse::Error("ERR internal error".into())
        }
    }
}

fn extract_set_members(resp: &CommandResponse) -> Vec<Vec<u8>> {
    match resp {
        CommandResponse::Array(items) => items
            .iter()
            .filter_map(|item| match item {
                CommandResponse::BulkString(b) => Some(b.clone()),
                CommandResponse::BulkStringShared(b) => Some(b.to_vec()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

async fn gather_set_members(
    keys: Vec<Vec<u8>>,
    shard_id: u16,
    store: &Rc<RefCell<ShardStore>>,
    router: &ShardRouter,
) -> Result<Vec<std::collections::HashSet<Vec<u8>>>, CommandResponse> {
    let shard_count = router.shard_count();
    let mut shard_keys: Vec<Vec<(usize, Vec<u8>)>> = vec![vec![]; shard_count];

    for (i, key) in keys.iter().enumerate() {
        let target = shard_for_key(key, shard_count) as usize;
        shard_keys[target].push((i, key.clone()));
    }

    let mut per_key_members: Vec<std::collections::HashSet<Vec<u8>>> =
        vec![std::collections::HashSet::new(); keys.len()];
    let mut receivers: Vec<(Vec<usize>, oneshot::Receiver<CommandResponse>)> = Vec::new();

    for (target, reqs) in shard_keys.into_iter().enumerate() {
        if reqs.is_empty() {
            continue;
        }
        if target as u16 == shard_id {
            for (key_idx, key) in &reqs {
                let resp = store
                    .borrow_mut()
                    .execute(Command::SMembers { key: key.clone() });
                let members = extract_set_members(&resp);
                for m in members {
                    per_key_members[*key_idx].insert(m);
                }
            }
        } else {
            for (key_idx, key) in reqs {
                if let Ok(rx) =
                    router.try_dispatch_foreign(target as u16, Command::SMembers { key })
                {
                    receivers.push((vec![key_idx], rx));
                }
            }
        }
    }

    for (indices, resp) in resolve_tagged_receivers(receivers).await {
        let members = extract_set_members(&resp);
        for idx in &indices {
            for m in &members {
                per_key_members[*idx].insert(m.clone());
            }
        }
    }

    Ok(per_key_members)
}

fn merge_sets(
    per_key_members: Vec<std::collections::HashSet<Vec<u8>>>,
    op: &SetOp,
) -> std::collections::HashSet<Vec<u8>> {
    if per_key_members.is_empty() {
        return std::collections::HashSet::new();
    }
    match op {
        SetOp::Union => {
            let mut result = std::collections::HashSet::new();
            for set in per_key_members {
                for m in set {
                    result.insert(m);
                }
            }
            result
        }
        SetOp::Inter => {
            let mut iter = per_key_members.into_iter();
            let mut result = iter.next().unwrap_or_default();
            for set in iter {
                result = result.intersection(&set).cloned().collect();
            }
            result
        }
        SetOp::Diff => {
            let mut iter = per_key_members.into_iter();
            let mut result = iter.next().unwrap_or_default();
            for set in iter {
                result = result.difference(&set).cloned().collect();
            }
            result
        }
    }
}

async fn handle_set_op(
    keys: Vec<Vec<u8>>,
    op: SetOp,
    shard_id: u16,
    store: &Rc<RefCell<ShardStore>>,
    router: &ShardRouter,
) -> CommandResponse {
    let per_key_members = match gather_set_members(keys, shard_id, store, router).await {
        Ok(m) => m,
        Err(e) => return e,
    };
    let result = merge_sets(per_key_members, &op);
    CommandResponse::Array(
        result
            .into_iter()
            .map(CommandResponse::BulkString)
            .collect(),
    )
}

#[allow(clippy::too_many_arguments)]
async fn handle_set_store_op(
    destination: Vec<u8>,
    keys: Vec<Vec<u8>>,
    op: SetOp,
    shard_id: u16,
    store: &Rc<RefCell<ShardStore>>,
    wal_writer: &Rc<RefCell<Option<Box<dyn WalWriter>>>>,
    router: &ShardRouter,
) -> CommandResponse {
    let per_key_members = match gather_set_members(keys, shard_id, store, router).await {
        Ok(m) => m,
        Err(e) => return e,
    };
    let result = merge_sets(per_key_members, &op);
    let count = result.len() as i64;

    let members: Vec<Vec<u8>> = result.into_iter().collect();

    let dst_shard = shard_for_key(&destination, router.shard_count);

    let del_cmd = Command::Del {
        keys: vec![destination.clone()],
    };
    if dst_shard == shard_id {
        execute_with_wal_inline(store, wal_writer, del_cmd);
    } else if let Ok(rx) = router.try_dispatch_foreign(dst_shard, del_cmd) {
        let _ = tokio::time::timeout(CROSS_SHARD_TIMEOUT, rx).await;
    }

    if !members.is_empty() {
        let sadd_cmd = Command::SAdd {
            key: destination,
            members,
        };
        if dst_shard == shard_id {
            execute_with_wal_inline(store, wal_writer, sadd_cmd);
        } else if let Ok(rx) = router.try_dispatch_foreign(dst_shard, sadd_cmd) {
            let _ = tokio::time::timeout(CROSS_SHARD_TIMEOUT, rx).await;
        }
    }

    CommandResponse::Integer(count)
}

async fn handle_sintercard(
    keys: Vec<Vec<u8>>,
    limit: Option<usize>,
    shard_id: u16,
    store: &Rc<RefCell<ShardStore>>,
    router: &ShardRouter,
) -> CommandResponse {
    let per_key_members = match gather_set_members(keys, shard_id, store, router).await {
        Ok(m) => m,
        Err(e) => return e,
    };
    let result = merge_sets(per_key_members, &SetOp::Inter);
    let count = match limit {
        Some(lim) if lim > 0 => result.len().min(lim),
        _ => result.len(),
    };
    CommandResponse::Integer(count as i64)
}

#[allow(clippy::too_many_arguments)]
async fn handle_smove(
    source: Vec<u8>,
    destination: Vec<u8>,
    member: Vec<u8>,
    shard_id: u16,
    store: &Rc<RefCell<ShardStore>>,
    wal_writer: &Rc<RefCell<Option<Box<dyn WalWriter>>>>,
    router: &ShardRouter,
) -> CommandResponse {
    let src_shard = shard_for_key(&source, router.shard_count());
    let dst_shard = shard_for_key(&destination, router.shard_count());

    if src_shard == dst_shard {
        let cmd = Command::SMove {
            source,
            destination,
            member,
        };
        if src_shard == shard_id {
            return execute_with_wal_inline(store, wal_writer, cmd);
        }
        return match router.try_dispatch_foreign(src_shard, cmd) {
            Ok(rx) => match tokio::time::timeout(CROSS_SHARD_TIMEOUT, rx).await {
                Ok(Ok(resp)) => resp,
                _ => CommandResponse::Error("ERR cross-shard timeout".into()),
            },
            Err(e) => e,
        };
    }

    if let Err(resp) = ensure_destination_type(&destination, "set", shard_id, store, router).await {
        return resp;
    }

    let rem_cmd = Command::SRem {
        key: source.clone(),
        members: vec![member.clone()],
    };
    let removed = if src_shard == shard_id {
        execute_with_wal_inline(store, wal_writer, rem_cmd)
    } else {
        match router.try_dispatch_foreign(src_shard, rem_cmd) {
            Ok(rx) => match tokio::time::timeout(CROSS_SHARD_TIMEOUT, rx).await {
                Ok(Ok(resp)) => resp,
                _ => return CommandResponse::Error("ERR cross-shard timeout".into()),
            },
            Err(e) => return e,
        }
    };

    match removed {
        CommandResponse::Integer(1) => {}
        CommandResponse::Integer(0) => return CommandResponse::Integer(0),
        CommandResponse::Error(e) => return CommandResponse::Error(e),
        _ => return CommandResponse::Error("ERR internal error".into()),
    }

    let member_for_rollback = member.clone();
    let add_cmd = Command::SAdd {
        key: destination.clone(),
        members: vec![member],
    };

    let add_resp = if dst_shard == shard_id {
        execute_with_wal_inline(store, wal_writer, add_cmd)
    } else if let Ok(rx) = router.try_dispatch_foreign(dst_shard, add_cmd) {
        match tokio::time::timeout(CROSS_SHARD_TIMEOUT, rx).await {
            Ok(Ok(resp)) => resp,
            _ => CommandResponse::Error("ERR cross-shard timeout".into()),
        }
    } else {
        CommandResponse::Error("ERR shard unavailable".into())
    };

    match add_resp {
        CommandResponse::Integer(_) => CommandResponse::Integer(1),
        CommandResponse::Error(e) => {
            restore_set_source(
                source,
                member_for_rollback,
                shard_id,
                store,
                wal_writer,
                router,
            )
            .await;
            CommandResponse::Error(e)
        }
        _ => {
            restore_set_source(
                source,
                member_for_rollback,
                shard_id,
                store,
                wal_writer,
                router,
            )
            .await;
            CommandResponse::Error("ERR internal error".into())
        }
    }
}

async fn handle_pfcount_multi(
    keys: Vec<Vec<u8>>,
    shard_id: u16,
    store: &Rc<RefCell<ShardStore>>,
    router: &ShardRouter,
) -> CommandResponse {
    let shard_count = router.shard_count();
    let mut shard_keys: Vec<Vec<Vec<u8>>> = vec![vec![]; shard_count];

    for key in keys {
        let target = shard_for_key(&key, shard_count) as usize;
        shard_keys[target].push(key);
    }

    let mut hll_datas: Vec<Vec<u8>> = Vec::new();
    let mut receivers: Vec<oneshot::Receiver<CommandResponse>> = Vec::new();

    for (target, keys) in shard_keys.into_iter().enumerate() {
        if keys.is_empty() {
            continue;
        }
        for key in &keys {
            if target as u16 == shard_id {
                if let CommandResponse::BulkString(data) = store
                    .borrow_mut()
                    .execute(Command::Get { key: key.clone() })
                {
                    hll_datas.push(data);
                }
            } else if let Ok(rx) =
                router.try_dispatch_foreign(target as u16, Command::Get { key: key.clone() })
            {
                receivers.push(rx);
            }
        }
    }

    for resp in resolve_receivers(receivers).await {
        if let CommandResponse::BulkString(data) = resp {
            hll_datas.push(data);
        } else if let CommandResponse::BulkStringShared(data) = resp {
            hll_datas.push(data.to_vec());
        }
    }

    let register_bytes = 16384 * 6 / 8;
    let mut merged = vec![0u8; register_bytes];

    for data in &hll_datas {
        if data.len() == register_bytes {
            for i in 0..16384 {
                let src_val = hll_register_get(data, i);
                let dst_val = hll_register_get(&merged, i);
                if src_val > dst_val {
                    hll_register_set(&mut merged, i, src_val);
                }
            }
        }
    }

    CommandResponse::Integer(hll_count_registers(&merged) as i64)
}

fn hll_register_get(registers: &[u8], index: usize) -> u8 {
    let bit_offset = index * 6;
    let byte_offset = bit_offset / 8;
    let bit_shift = bit_offset % 8;

    if bit_shift <= 2 {
        (registers[byte_offset] >> bit_shift) & 0x3F
    } else {
        let lo = registers[byte_offset] >> bit_shift;
        let hi = if byte_offset + 1 < registers.len() {
            registers[byte_offset + 1] << (8 - bit_shift)
        } else {
            0
        };
        (lo | hi) & 0x3F
    }
}

fn hll_register_set(registers: &mut [u8], index: usize, value: u8) {
    let bit_offset = index * 6;
    let byte_offset = bit_offset / 8;
    let bit_shift = bit_offset % 8;

    let mask_lo = !(0x3F_u8 << bit_shift);
    registers[byte_offset] = (registers[byte_offset] & mask_lo) | ((value & 0x3F) << bit_shift);

    if bit_shift > 2 && byte_offset + 1 < registers.len() {
        let bits_in_first = 8 - bit_shift;
        let mask_hi = !((0x3F >> bits_in_first) as u8);
        registers[byte_offset + 1] =
            (registers[byte_offset + 1] & mask_hi) | ((value & 0x3F) >> bits_in_first);
    }
}

fn hll_count_registers(registers: &[u8]) -> u64 {
    let m = 16384.0f64;
    let alpha = 0.7213 / (1.0 + 1.079 / m);

    let mut sum = 0.0f64;
    let mut zeros = 0u32;

    for i in 0..16384 {
        let val = hll_register_get(registers, i);
        sum += 1.0 / (1u64 << val) as f64;
        if val == 0 {
            zeros += 1;
        }
    }

    let mut estimate = alpha * m * m / sum;

    if estimate <= 2.5 * m && zeros > 0 {
        estimate = m * (m / zeros as f64).ln();
    }

    estimate.round() as u64
}

#[allow(clippy::too_many_arguments)]
async fn handle_pfmerge(
    destkey: Vec<u8>,
    sourcekeys: Vec<Vec<u8>>,
    shard_id: u16,
    store: &Rc<RefCell<ShardStore>>,
    wal_writer: &Rc<RefCell<Option<Box<dyn WalWriter>>>>,
    router: &ShardRouter,
) -> CommandResponse {
    let shard_count = router.shard_count();
    let register_bytes = 16384 * 6 / 8;

    let mut all_keys = vec![destkey.clone()];
    all_keys.extend(sourcekeys);

    let mut hll_datas: Vec<Vec<u8>> = Vec::new();
    let mut receivers: Vec<oneshot::Receiver<CommandResponse>> = Vec::new();

    for key in &all_keys {
        let target = shard_for_key(key, shard_count);
        if target == shard_id {
            let resp = store
                .borrow_mut()
                .execute(Command::Get { key: key.clone() });
            match resp {
                CommandResponse::BulkString(data) => hll_datas.push(data),
                CommandResponse::BulkStringShared(data) => hll_datas.push(data.to_vec()),
                _ => {}
            }
        } else if let Ok(rx) =
            router.try_dispatch_foreign(target, Command::Get { key: key.clone() })
        {
            receivers.push(rx);
        }
    }

    for resp in resolve_receivers(receivers).await {
        match resp {
            CommandResponse::BulkString(data) => hll_datas.push(data),
            CommandResponse::BulkStringShared(data) => hll_datas.push(data.to_vec()),
            _ => {}
        }
    }

    let mut merged = vec![0u8; register_bytes];
    for data in &hll_datas {
        if data.len() == register_bytes {
            for i in 0..16384 {
                let src_val = hll_register_get(data, i);
                let dst_val = hll_register_get(&merged, i);
                if src_val > dst_val {
                    hll_register_set(&mut merged, i, src_val);
                }
            }
        }
    }

    let dst_shard = shard_for_key(&destkey, shard_count);
    let set_cmd = Command::Set {
        key: destkey,
        value: merged,
        ex: None,
        px: None,
        nx: false,
        xx: false,
    };

    if dst_shard == shard_id {
        execute_with_wal_inline(store, wal_writer, set_cmd);
    } else if let Ok(rx) = router.try_dispatch_foreign(dst_shard, set_cmd) {
        let _ = tokio::time::timeout(CROSS_SHARD_TIMEOUT, rx).await;
    }

    CommandResponse::Ok
}

#[allow(clippy::too_many_arguments)]
async fn handle_bitop(
    operation: BitOperation,
    destkey: Vec<u8>,
    keys: Vec<Vec<u8>>,
    shard_id: u16,
    store: &Rc<RefCell<ShardStore>>,
    wal_writer: &Rc<RefCell<Option<Box<dyn WalWriter>>>>,
    router: &ShardRouter,
) -> CommandResponse {
    let shard_count = router.shard_count();
    let mut buffers: Vec<Vec<u8>> = Vec::with_capacity(keys.len());
    let mut receivers: Vec<(usize, oneshot::Receiver<CommandResponse>)> = Vec::new();
    let mut buffer_slots: Vec<Option<Vec<u8>>> = vec![None; keys.len()];

    for (idx, key) in keys.iter().enumerate() {
        let target = shard_for_key(key, shard_count);
        if target == shard_id {
            let resp = store
                .borrow_mut()
                .execute(Command::Get { key: key.clone() });
            match resp {
                CommandResponse::BulkString(data) => buffer_slots[idx] = Some(data),
                CommandResponse::BulkStringShared(data) => buffer_slots[idx] = Some(data.to_vec()),
                _ => buffer_slots[idx] = Some(Vec::new()),
            }
        } else if let Ok(rx) =
            router.try_dispatch_foreign(target, Command::Get { key: key.clone() })
        {
            receivers.push((idx, rx));
        } else {
            buffer_slots[idx] = Some(Vec::new());
        }
    }

    for (idx, resp) in resolve_tagged_receivers(receivers).await {
        match resp {
            CommandResponse::BulkString(data) => buffer_slots[idx] = Some(data),
            CommandResponse::BulkStringShared(data) => buffer_slots[idx] = Some(data.to_vec()),
            _ => buffer_slots[idx] = Some(Vec::new()),
        }
    }

    for slot in buffer_slots {
        buffers.push(slot.unwrap_or_default());
    }

    let mut max_len = 0usize;
    for buf in &buffers {
        max_len = max_len.max(buf.len());
    }

    let result = match operation {
        BitOperation::And => {
            let mut result = vec![0xFF; max_len];
            for buf in &buffers {
                for (i, byte) in result.iter_mut().enumerate() {
                    let src = if i < buf.len() { buf[i] } else { 0 };
                    *byte &= src;
                }
            }
            result
        }
        BitOperation::Or => {
            let mut result = vec![0u8; max_len];
            for buf in &buffers {
                for (i, &src) in buf.iter().enumerate() {
                    result[i] |= src;
                }
            }
            result
        }
        BitOperation::Xor => {
            let mut result = vec![0u8; max_len];
            for buf in &buffers {
                for (i, &src) in buf.iter().enumerate() {
                    result[i] ^= src;
                }
            }
            result
        }
        BitOperation::Not => {
            let buf = &buffers[0];
            buf.iter().map(|&b| !b).collect()
        }
    };

    let len = result.len() as i64;

    let dst_shard = shard_for_key(&destkey, shard_count);
    let set_cmd = Command::Set {
        key: destkey,
        value: result,
        ex: None,
        px: None,
        nx: false,
        xx: false,
    };

    if dst_shard == shard_id {
        execute_with_wal_inline(store, wal_writer, set_cmd);
    } else if let Ok(rx) = router.try_dispatch_foreign(dst_shard, set_cmd) {
        let _ = tokio::time::timeout(CROSS_SHARD_TIMEOUT, rx).await;
    }

    CommandResponse::Integer(len)
}
