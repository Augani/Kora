use std::cell::RefCell;
use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::rc::Rc;
use std::task::Poll;

use kora_core::command::{Command, CommandResponse};
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
        _ => execute_with_wal_inline(store, wal_writer, cmd),
    }
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
