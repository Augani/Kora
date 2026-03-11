//! Shard engine — coordinates worker threads and routes commands.
//!
//! `ShardEngine` spawns N OS threads, each running a tight event loop over a
//! crossbeam channel. Incoming commands are routed by key hash to the owning
//! shard worker. Multi-key commands (MGET, MSET, DEL, EXISTS, etc.) are fanned
//! out to all relevant shards and their responses merged. Keyless commands
//! (PING, DBSIZE, FLUSHDB, KEYS, SCAN) are either broadcast or delegated to
//! shard 0.
//!
//! Each worker loop periodically samples expired keys (lazy + sweep) and,
//! when a WAL writer is attached, appends mutation records before execution.

use std::sync::Arc;
use std::thread;

use crossbeam_channel::{Receiver, Sender};

use crate::command::{Command, CommandResponse};
use crate::hash::shard_for_key;
use crate::shard::store::ShardStore;
use crate::shard::wal_trait::{WalRecord, WalWriter};

/// Message sent from the dispatcher to a shard worker.
pub enum ShardMessage {
    /// Execute a single command.
    Single {
        /// The command to execute.
        command: Command,
        /// Channel to send the response back.
        response_tx: ResponseSender,
    },
    /// Execute multiple commands on the same shard in order.
    Batch {
        /// Commands tagged with their original position in the batch.
        commands: Vec<(usize, Command)>,
        /// Channel to send all responses back.
        response_tx: BatchResponseSender,
    },
}

/// A oneshot-like sender for command responses. Uses a crossbeam channel with capacity 1.
pub type ResponseSender = Sender<CommandResponse>;
/// A oneshot-like receiver for command responses.
pub type ResponseReceiver = Receiver<CommandResponse>;
/// Sender for batched command responses.
type BatchResponseSender = Sender<Vec<(usize, CommandResponse)>>;
/// Receiver for batched command responses.
type BatchResponseReceiver = Receiver<Vec<(usize, CommandResponse)>>;

/// Create a response channel pair.
pub fn response_channel() -> (ResponseSender, ResponseReceiver) {
    crossbeam_channel::bounded(1)
}

/// Create a response channel pair for batched command responses.
fn batch_response_channel() -> (BatchResponseSender, BatchResponseReceiver) {
    crossbeam_channel::bounded(1)
}

struct WorkerHandle {
    tx: Sender<ShardMessage>,
    thread: Option<thread::JoinHandle<()>>,
}

/// The shard engine coordinates N worker threads, each owning a `ShardStore`.
///
/// Commands are routed to the appropriate worker based on key hash.
pub struct ShardEngine {
    workers: Vec<WorkerHandle>,
    shard_count: usize,
}

impl ShardEngine {
    /// Create a new ShardEngine with the given number of worker threads.
    pub fn new(shard_count: usize) -> Self {
        let wal_writers: Vec<Option<Box<dyn WalWriter>>> = (0..shard_count).map(|_| None).collect();
        Self::new_with_storage(shard_count, wal_writers)
    }

    /// Create a new ShardEngine with per-shard WAL writers.
    ///
    /// Each element in `wal_writers` corresponds to one shard. The WAL writer
    /// is moved into the shard's worker thread and called after every mutation.
    pub fn new_with_storage(
        shard_count: usize,
        wal_writers: Vec<Option<Box<dyn WalWriter>>>,
    ) -> Self {
        Self::new_with_recovery(shard_count, wal_writers, None)
    }

    /// Create a new ShardEngine with per-shard WAL writers and optional
    /// per-shard recovery callbacks.
    ///
    /// Each recovery callback receives the shard index and a mutable reference
    /// to the freshly created `ShardStore`, allowing callers to replay RDB
    /// snapshots and WAL entries before the worker starts accepting commands.
    #[allow(clippy::type_complexity)]
    pub fn new_with_recovery(
        shard_count: usize,
        wal_writers: Vec<Option<Box<dyn WalWriter>>>,
        recovery_fns: Option<Vec<Box<dyn FnOnce(usize, &mut ShardStore) + Send>>>,
    ) -> Self {
        assert_eq!(
            wal_writers.len(),
            shard_count,
            "wal_writers length must match shard_count"
        );

        let mut recovery_iter: Vec<Option<Box<dyn FnOnce(usize, &mut ShardStore) + Send>>> =
            match recovery_fns {
                Some(fns) => {
                    assert_eq!(fns.len(), shard_count);
                    fns.into_iter().map(Some).collect()
                }
                None => (0..shard_count).map(|_| None).collect(),
            };

        let mut workers = Vec::with_capacity(shard_count);

        for (i, wal_writer) in wal_writers.into_iter().enumerate() {
            let (tx, rx) = crossbeam_channel::unbounded::<ShardMessage>();
            let recovery_fn = recovery_iter[i].take();
            let handle = thread::Builder::new()
                .name(format!("kora-shard-{}", i))
                .spawn(move || {
                    let mut store = ShardStore::new(i as u16);
                    if let Some(recover) = recovery_fn {
                        recover(i, &mut store);
                    }
                    worker_loop(&mut store, &rx, wal_writer);
                })
                .expect("failed to spawn shard worker thread");

            workers.push(WorkerHandle {
                tx,
                thread: Some(handle),
            });
        }

        ShardEngine {
            workers,
            shard_count,
        }
    }

    /// Get the number of shards.
    pub fn shard_count(&self) -> usize {
        self.shard_count
    }

    /// Dispatch a command and return a receiver for the response.
    pub fn dispatch(&self, cmd: Command) -> ResponseReceiver {
        let (tx, rx) = response_channel();

        if let Some(key) = cmd.key() {
            let shard_id = shard_for_key(key, self.shard_count) as usize;
            let _ = self.workers[shard_id].tx.send(ShardMessage::Single {
                command: cmd,
                response_tx: tx,
            });
        } else if cmd.is_multi_key() {
            self.dispatch_multi_key(cmd, tx);
        } else {
            self.dispatch_keyless(cmd, tx);
        }

        rx
    }

    /// Dispatch a command and block until the response is received.
    pub fn dispatch_blocking(&self, cmd: Command) -> CommandResponse {
        let rx = self.dispatch(cmd);
        rx.recv()
            .unwrap_or(CommandResponse::Error("ERR internal error".into()))
    }

    /// Dispatch a batch of commands and block until all responses are received.
    ///
    /// Commands that target a single key are grouped per shard and executed in-order,
    /// reducing response-channel overhead for pipelined workloads.
    pub fn dispatch_batch_blocking(&self, commands: Vec<Command>) -> Vec<CommandResponse> {
        let total = commands.len();
        if total == 0 {
            return Vec::new();
        }

        let mut responses = vec![None; total];
        let mut segment = Vec::new();

        for (idx, command) in commands.into_iter().enumerate() {
            if command.key().is_some() {
                segment.push((idx, command));
            } else {
                if !segment.is_empty() {
                    self.execute_shard_batch(std::mem::take(&mut segment), &mut responses);
                }
                responses[idx] = Some(self.dispatch_blocking(command));
            }
        }

        if !segment.is_empty() {
            self.execute_shard_batch(segment, &mut responses);
        }

        responses
            .into_iter()
            .map(|resp| resp.unwrap_or(CommandResponse::Error("ERR internal error".into())))
            .collect()
    }

    fn execute_shard_batch(
        &self,
        commands: Vec<(usize, Command)>,
        responses: &mut [Option<CommandResponse>],
    ) {
        if commands.is_empty() {
            return;
        }

        let mut shard_batches: Vec<Vec<(usize, Command)>> = vec![Vec::new(); self.shard_count];
        for (idx, command) in commands {
            let Some(key) = command.key() else {
                responses[idx] = Some(self.dispatch_blocking(command));
                continue;
            };
            let shard_id = shard_for_key(key, self.shard_count) as usize;
            shard_batches[shard_id].push((idx, command));
        }

        let mut receivers = Vec::new();
        for (shard_id, commands) in shard_batches.into_iter().enumerate() {
            if commands.is_empty() {
                continue;
            }

            let (resp_tx, resp_rx) = batch_response_channel();
            let _ = self.workers[shard_id].tx.send(ShardMessage::Batch {
                commands,
                response_tx: resp_tx,
            });
            receivers.push(resp_rx);
        }

        for rx in receivers {
            if let Ok(items) = rx.recv() {
                for (idx, response) in items {
                    if let Some(slot) = responses.get_mut(idx) {
                        *slot = Some(response);
                    }
                }
            }
        }
    }

    fn dispatch_multi_key(&self, cmd: Command, tx: ResponseSender) {
        match cmd {
            Command::MGet { keys } => {
                let mut results = vec![CommandResponse::Nil; keys.len()];
                let mut shard_requests: Vec<Vec<(usize, Vec<u8>)>> = vec![vec![]; self.shard_count];
                for (i, key) in keys.iter().enumerate() {
                    let shard_id = shard_for_key(key, self.shard_count) as usize;
                    shard_requests[shard_id].push((i, key.clone()));
                }

                let mut receivers = Vec::new();
                for (shard_id, reqs) in shard_requests.into_iter().enumerate() {
                    if reqs.is_empty() {
                        continue;
                    }
                    let shard_keys: Vec<Vec<u8>> = reqs.iter().map(|(_, k)| k.clone()).collect();
                    let indices: Vec<usize> = reqs.iter().map(|(i, _)| *i).collect();
                    let (resp_tx, resp_rx) = response_channel();
                    let _ = self.workers[shard_id].tx.send(ShardMessage::Single {
                        command: Command::MGet { keys: shard_keys },
                        response_tx: resp_tx,
                    });
                    receivers.push((indices, resp_rx));
                }

                for (indices, rx) in receivers {
                    if let Ok(CommandResponse::Array(values)) = rx.recv() {
                        for (idx, val) in indices.into_iter().zip(values) {
                            results[idx] = val;
                        }
                    }
                }
                let _ = tx.send(CommandResponse::Array(results));
            }
            Command::MSet { entries } => {
                let mut shard_entries: Vec<Vec<(Vec<u8>, Vec<u8>)>> =
                    vec![vec![]; self.shard_count];
                for (key, value) in entries {
                    let shard_id = shard_for_key(&key, self.shard_count) as usize;
                    shard_entries[shard_id].push((key, value));
                }

                let mut receivers = Vec::new();
                for (shard_id, entries) in shard_entries.into_iter().enumerate() {
                    if entries.is_empty() {
                        continue;
                    }
                    let (resp_tx, resp_rx) = response_channel();
                    let _ = self.workers[shard_id].tx.send(ShardMessage::Single {
                        command: Command::MSet { entries },
                        response_tx: resp_tx,
                    });
                    receivers.push(resp_rx);
                }
                for rx in receivers {
                    let _ = rx.recv();
                }
                let _ = tx.send(CommandResponse::Ok);
            }
            Command::Del { keys } => {
                let mut shard_keys: Vec<Vec<Vec<u8>>> = vec![vec![]; self.shard_count];
                for key in keys {
                    let shard_id = shard_for_key(&key, self.shard_count) as usize;
                    shard_keys[shard_id].push(key);
                }

                let mut total = 0i64;
                let mut receivers = Vec::new();
                for (shard_id, keys) in shard_keys.into_iter().enumerate() {
                    if keys.is_empty() {
                        continue;
                    }
                    let (resp_tx, resp_rx) = response_channel();
                    let _ = self.workers[shard_id].tx.send(ShardMessage::Single {
                        command: Command::Del { keys },
                        response_tx: resp_tx,
                    });
                    receivers.push(resp_rx);
                }
                for rx in receivers {
                    if let Ok(CommandResponse::Integer(n)) = rx.recv() {
                        total += n;
                    }
                }
                let _ = tx.send(CommandResponse::Integer(total));
            }
            Command::Exists { keys } => {
                let mut shard_keys: Vec<Vec<Vec<u8>>> = vec![vec![]; self.shard_count];
                for key in keys {
                    let shard_id = shard_for_key(&key, self.shard_count) as usize;
                    shard_keys[shard_id].push(key);
                }

                let mut total = 0i64;
                let mut receivers = Vec::new();
                for (shard_id, keys) in shard_keys.into_iter().enumerate() {
                    if keys.is_empty() {
                        continue;
                    }
                    let (resp_tx, resp_rx) = response_channel();
                    let _ = self.workers[shard_id].tx.send(ShardMessage::Single {
                        command: Command::Exists { keys },
                        response_tx: resp_tx,
                    });
                    receivers.push(resp_rx);
                }
                for rx in receivers {
                    if let Ok(CommandResponse::Integer(n)) = rx.recv() {
                        total += n;
                    }
                }
                let _ = tx.send(CommandResponse::Integer(total));
            }
            Command::Unlink { keys } => {
                let mut shard_keys: Vec<Vec<Vec<u8>>> = vec![vec![]; self.shard_count];
                for key in keys {
                    let shard_id = shard_for_key(&key, self.shard_count) as usize;
                    shard_keys[shard_id].push(key);
                }

                let mut total = 0i64;
                let mut receivers = Vec::new();
                for (shard_id, keys) in shard_keys.into_iter().enumerate() {
                    if keys.is_empty() {
                        continue;
                    }
                    let (resp_tx, resp_rx) = response_channel();
                    let _ = self.workers[shard_id].tx.send(ShardMessage::Single {
                        command: Command::Unlink { keys },
                        response_tx: resp_tx,
                    });
                    receivers.push(resp_rx);
                }
                for rx in receivers {
                    if let Ok(CommandResponse::Integer(n)) = rx.recv() {
                        total += n;
                    }
                }
                let _ = tx.send(CommandResponse::Integer(total));
            }
            Command::Touch { keys } => {
                let mut shard_keys: Vec<Vec<Vec<u8>>> = vec![vec![]; self.shard_count];
                for key in keys {
                    let shard_id = shard_for_key(&key, self.shard_count) as usize;
                    shard_keys[shard_id].push(key);
                }

                let mut total = 0i64;
                let mut receivers = Vec::new();
                for (shard_id, keys) in shard_keys.into_iter().enumerate() {
                    if keys.is_empty() {
                        continue;
                    }
                    let (resp_tx, resp_rx) = response_channel();
                    let _ = self.workers[shard_id].tx.send(ShardMessage::Single {
                        command: Command::Touch { keys },
                        response_tx: resp_tx,
                    });
                    receivers.push(resp_rx);
                }
                for rx in receivers {
                    if let Ok(CommandResponse::Integer(n)) = rx.recv() {
                        total += n;
                    }
                }
                let _ = tx.send(CommandResponse::Integer(total));
            }
            Command::MSetNx { entries } => {
                let mut shard_entries: Vec<Vec<(Vec<u8>, Vec<u8>)>> =
                    vec![vec![]; self.shard_count];
                for (key, value) in entries {
                    let shard_id = shard_for_key(&key, self.shard_count) as usize;
                    shard_entries[shard_id].push((key, value));
                }

                let mut all_set = true;
                let mut receivers = Vec::new();
                for (shard_id, entries) in shard_entries.into_iter().enumerate() {
                    if entries.is_empty() {
                        continue;
                    }
                    let (resp_tx, resp_rx) = response_channel();
                    let _ = self.workers[shard_id].tx.send(ShardMessage::Single {
                        command: Command::MSetNx { entries },
                        response_tx: resp_tx,
                    });
                    receivers.push(resp_rx);
                }
                for rx in receivers {
                    if let Ok(CommandResponse::Integer(0)) = rx.recv() {
                        all_set = false;
                    }
                }
                let _ = tx.send(CommandResponse::Integer(if all_set { 1 } else { 0 }));
            }
            _ => {
                let _ = tx.send(CommandResponse::Error(
                    "ERR unsupported multi-key command".into(),
                ));
            }
        }
    }

    fn dispatch_vec_query(&self, cmd: Command, tx: ResponseSender) {
        let (key, k, vector) = match cmd {
            Command::VecQuery { key, k, vector } => (key, k, vector),
            _ => {
                let _ = tx.send(CommandResponse::Error(
                    "ERR internal: not a VecQuery".into(),
                ));
                return;
            }
        };

        let mut receivers = Vec::with_capacity(self.shard_count);
        for worker in &self.workers {
            let (resp_tx, resp_rx) = response_channel();
            let _ = worker.tx.send(ShardMessage::Single {
                command: Command::VecQuery {
                    key: key.clone(),
                    k,
                    vector: vector.clone(),
                },
                response_tx: resp_tx,
            });
            receivers.push(resp_rx);
        }

        let mut all_results: Vec<(i64, Vec<u8>)> = Vec::new();
        for rx in receivers {
            if let Ok(CommandResponse::Array(items)) = rx.recv() {
                for item in items {
                    if let CommandResponse::Array(pair) = item {
                        if pair.len() == 2 {
                            if let (
                                CommandResponse::Integer(id),
                                CommandResponse::BulkString(dist),
                            ) = (&pair[0], &pair[1])
                            {
                                all_results.push((*id, dist.clone()));
                            }
                        }
                    }
                }
            }
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
        let _ = tx.send(CommandResponse::Array(items));
    }

    fn dispatch_keyless(&self, cmd: Command, tx: ResponseSender) {
        match cmd {
            Command::VecQuery { .. } => self.dispatch_vec_query(cmd, tx),
            Command::DbSize => {
                let mut total = 0i64;
                let mut receivers = Vec::new();
                for worker in &self.workers {
                    let (resp_tx, resp_rx) = response_channel();
                    let _ = worker.tx.send(ShardMessage::Single {
                        command: Command::DbSize,
                        response_tx: resp_tx,
                    });
                    receivers.push(resp_rx);
                }
                for rx in receivers {
                    if let Ok(CommandResponse::Integer(n)) = rx.recv() {
                        total += n;
                    }
                }
                let _ = tx.send(CommandResponse::Integer(total));
            }
            Command::FlushDb | Command::FlushAll => {
                let mut receivers = Vec::new();
                for worker in &self.workers {
                    let (resp_tx, resp_rx) = response_channel();
                    let _ = worker.tx.send(ShardMessage::Single {
                        command: Command::FlushDb,
                        response_tx: resp_tx,
                    });
                    receivers.push(resp_rx);
                }
                for rx in receivers {
                    let _ = rx.recv();
                }
                let _ = tx.send(CommandResponse::Ok);
            }
            Command::Dump => {
                let mut all_entries = Vec::new();
                let mut receivers = Vec::new();
                for worker in &self.workers {
                    let (resp_tx, resp_rx) = response_channel();
                    let _ = worker.tx.send(ShardMessage::Single {
                        command: Command::Dump,
                        response_tx: resp_tx,
                    });
                    receivers.push(resp_rx);
                }
                for rx in receivers {
                    if let Ok(CommandResponse::Array(entries)) = rx.recv() {
                        all_entries.extend(entries);
                    }
                }
                let _ = tx.send(CommandResponse::Array(all_entries));
            }
            Command::Keys { pattern } => {
                let mut all_keys = Vec::new();
                let mut receivers = Vec::new();
                for worker in &self.workers {
                    let (resp_tx, resp_rx) = response_channel();
                    let _ = worker.tx.send(ShardMessage::Single {
                        command: Command::Keys {
                            pattern: pattern.clone(),
                        },
                        response_tx: resp_tx,
                    });
                    receivers.push(resp_rx);
                }
                for rx in receivers {
                    if let Ok(CommandResponse::Array(keys)) = rx.recv() {
                        all_keys.extend(keys);
                    }
                }
                let _ = tx.send(CommandResponse::Array(all_keys));
            }
            Command::Scan {
                cursor,
                pattern,
                count,
            } => {
                let pattern = pattern.unwrap_or_else(|| "*".to_string());
                let mut merged_keys: Vec<Vec<u8>> = Vec::new();
                let mut receivers = Vec::new();
                for worker in &self.workers {
                    let (resp_tx, resp_rx) = response_channel();
                    let _ = worker.tx.send(ShardMessage::Single {
                        command: Command::Keys {
                            pattern: pattern.clone(),
                        },
                        response_tx: resp_tx,
                    });
                    receivers.push(resp_rx);
                }
                for rx in receivers {
                    if let Ok(CommandResponse::Array(keys)) = rx.recv() {
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
                    let _ = tx.send(CommandResponse::Array(vec![
                        CommandResponse::BulkString(b"0".to_vec()),
                        CommandResponse::Array(vec![]),
                    ]));
                    return;
                }
                let end = start.saturating_add(limit).min(merged_keys.len());
                let result_keys: Vec<CommandResponse> = merged_keys[start..end]
                    .iter()
                    .map(|k| CommandResponse::BulkString(k.clone()))
                    .collect();
                let next_cursor = if end >= merged_keys.len() { 0 } else { end };
                let _ = tx.send(CommandResponse::Array(vec![
                    CommandResponse::BulkString(next_cursor.to_string().into_bytes()),
                    CommandResponse::Array(result_keys),
                ]));
            }
            Command::StatsHotkeys { count } => {
                let mut all_hot: Vec<(Vec<u8>, i64)> = Vec::new();
                let mut receivers = Vec::new();
                for worker in &self.workers {
                    let (resp_tx, resp_rx) = response_channel();
                    let _ = worker.tx.send(ShardMessage::Single {
                        command: Command::StatsHotkeys { count },
                        response_tx: resp_tx,
                    });
                    receivers.push(resp_rx);
                }
                for rx in receivers {
                    if let Ok(CommandResponse::Array(hotkeys)) = rx.recv() {
                        for hotkey in hotkeys {
                            if let CommandResponse::Array(pair) = hotkey {
                                if pair.len() != 2 {
                                    continue;
                                }
                                if let (
                                    CommandResponse::BulkString(key),
                                    CommandResponse::Integer(freq),
                                ) = (&pair[0], &pair[1])
                                {
                                    all_hot.push((key.clone(), *freq));
                                }
                            }
                        }
                    }
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
                let _ = tx.send(CommandResponse::Array(items));
            }
            Command::StatsMemory { prefixes } => {
                let mut totals = vec![0i64; prefixes.len()];
                let mut receivers = Vec::new();
                for worker in &self.workers {
                    let (resp_tx, resp_rx) = response_channel();
                    let _ = worker.tx.send(ShardMessage::Single {
                        command: Command::StatsMemory {
                            prefixes: prefixes.clone(),
                        },
                        response_tx: resp_tx,
                    });
                    receivers.push(resp_rx);
                }
                for rx in receivers {
                    if let Ok(CommandResponse::Array(items)) = rx.recv() {
                        for (idx, item) in items.into_iter().enumerate().take(totals.len()) {
                            if let CommandResponse::Integer(val) = item {
                                totals[idx] = totals[idx].saturating_add(val);
                            }
                        }
                    }
                }
                let merged = totals.into_iter().map(CommandResponse::Integer).collect();
                let _ = tx.send(CommandResponse::Array(merged));
            }
            Command::RandomKey => {
                for worker in &self.workers {
                    let (resp_tx, resp_rx) = response_channel();
                    let _ = worker.tx.send(ShardMessage::Single {
                        command: Command::RandomKey,
                        response_tx: resp_tx,
                    });
                    if let Ok(resp) = resp_rx.recv() {
                        if !matches!(resp, CommandResponse::Nil) {
                            let _ = tx.send(resp);
                            return;
                        }
                    }
                }
                let _ = tx.send(CommandResponse::Nil);
            }
            _ => {
                let _ = self.workers[0].tx.send(ShardMessage::Single {
                    command: cmd,
                    response_tx: tx,
                });
            }
        }
    }
}

impl Drop for ShardEngine {
    fn drop(&mut self) {
        for worker in &mut self.workers {
            let (dummy, _) = crossbeam_channel::unbounded();
            let old_tx = std::mem::replace(&mut worker.tx, dummy);
            drop(old_tx);
        }
        for worker in &mut self.workers {
            if let Some(handle) = worker.thread.take() {
                let _ = handle.join();
            }
        }
    }
}

/// Worker thread event loop.
fn worker_loop(
    store: &mut ShardStore,
    rx: &Receiver<ShardMessage>,
    mut wal_writer: Option<Box<dyn WalWriter>>,
) {
    const EXPIRE_SWEEP_INTERVAL_OPS: u32 = 4096;
    const EXPIRE_SWEEP_SAMPLE_SIZE: usize = 64;

    fn maybe_sweep(store: &mut ShardStore, ops_since_expire: &mut u32) {
        *ops_since_expire += 1;
        if *ops_since_expire >= EXPIRE_SWEEP_INTERVAL_OPS {
            let _ = store.evict_expired_sample(EXPIRE_SWEEP_SAMPLE_SIZE);
            *ops_since_expire = 0;
        }
    }

    fn execute_with_wal(
        store: &mut ShardStore,
        wal_writer: Option<&mut Box<dyn WalWriter>>,
        command: Command,
    ) -> CommandResponse {
        if let Some(writer) = wal_writer {
            if command.is_mutation() {
                if let Some(record) = command_to_wal_record(&command) {
                    writer.append(&record);
                }
            }
        }
        store.execute(command)
    }

    let mut ops_since_expire = 0u32;
    while let Ok(msg) = rx.recv() {
        match msg {
            ShardMessage::Single {
                command,
                response_tx,
            } => {
                maybe_sweep(store, &mut ops_since_expire);
                let response = execute_with_wal(store, wal_writer.as_mut(), command);
                let _ = response_tx.send(response);
            }
            ShardMessage::Batch {
                commands,
                response_tx,
            } => {
                let mut responses = Vec::with_capacity(commands.len());
                for (idx, command) in commands {
                    maybe_sweep(store, &mut ops_since_expire);
                    let response = execute_with_wal(store, wal_writer.as_mut(), command);
                    responses.push((idx, response));
                }
                let _ = response_tx.send(responses);
            }
        }
    }
}

/// Convert a `Command` to a `WalRecord` for WAL logging.
pub fn command_to_wal_record(cmd: &Command) -> Option<WalRecord> {
    match cmd {
        Command::Set {
            key, value, ex, px, ..
        } => {
            let ttl_ms = if let Some(s) = ex {
                Some(s * 1000)
            } else {
                *px
            };
            Some(WalRecord::Set {
                key: key.clone(),
                value: value.clone(),
                ttl_ms,
            })
        }
        Command::SetNx { key, value } => Some(WalRecord::Set {
            key: key.clone(),
            value: value.clone(),
            ttl_ms: None,
        }),
        Command::GetSet { key, value } => Some(WalRecord::Set {
            key: key.clone(),
            value: value.clone(),
            ttl_ms: None,
        }),
        Command::Append { key, value } => Some(WalRecord::Set {
            key: key.clone(),
            value: value.clone(),
            ttl_ms: None,
        }),
        Command::Del { keys } => keys.first().map(|key| WalRecord::Del { key: key.clone() }),
        Command::Expire { key, seconds } => Some(WalRecord::Expire {
            key: key.clone(),
            ttl_ms: seconds * 1000,
        }),
        Command::PExpire { key, millis } => Some(WalRecord::Expire {
            key: key.clone(),
            ttl_ms: *millis,
        }),
        Command::LPush { key, values } => Some(WalRecord::LPush {
            key: key.clone(),
            values: values.clone(),
        }),
        Command::RPush { key, values } => Some(WalRecord::RPush {
            key: key.clone(),
            values: values.clone(),
        }),
        Command::HSet { key, fields } => Some(WalRecord::HSet {
            key: key.clone(),
            fields: fields.clone(),
        }),
        Command::SAdd { key, members } => Some(WalRecord::SAdd {
            key: key.clone(),
            members: members.clone(),
        }),
        Command::FlushDb | Command::FlushAll => Some(WalRecord::FlushDb),
        Command::DocSet {
            collection,
            doc_id,
            json,
        } => Some(WalRecord::DocSet {
            collection: collection.clone(),
            doc_id: doc_id.clone(),
            json: json.clone(),
        }),
        Command::DocInsert { .. } => None,
        Command::DocDel { collection, doc_id } => Some(WalRecord::DocDel {
            collection: collection.clone(),
            doc_id: doc_id.clone(),
        }),
        Command::DocMSet { .. } => None,
        Command::VecSet {
            key,
            dimensions,
            vector,
        } => {
            let mut vec_bytes = Vec::with_capacity(vector.len() * 4);
            for &f in vector {
                vec_bytes.extend_from_slice(&f.to_le_bytes());
            }
            Some(WalRecord::VecSet {
                key: key.clone(),
                dimensions: *dimensions,
                vector: vec_bytes,
            })
        }
        Command::VecDel { key } => Some(WalRecord::VecDel { key: key.clone() }),
        _ => None,
    }
}

/// A handle to the engine that can be shared across async tasks.
pub type SharedEngine = Arc<ShardEngine>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_engine_basic() {
        let engine = ShardEngine::new(4);
        let resp = engine.dispatch_blocking(Command::Ping { message: None });
        assert!(matches!(resp, CommandResponse::SimpleString(s) if s == "PONG"));
    }

    #[test]
    fn test_engine_set_get() {
        let engine = ShardEngine::new(4);
        engine.dispatch_blocking(Command::Set {
            key: b"hello".to_vec(),
            value: b"world".to_vec(),
            ex: None,
            px: None,
            nx: false,
            xx: false,
        });
        match engine.dispatch_blocking(Command::Get {
            key: b"hello".to_vec(),
        }) {
            CommandResponse::BulkString(v) => assert_eq!(v, b"world"),
            other => panic!("Expected 'world', got {:?}", other),
        }
    }

    #[test]
    fn test_engine_mget_across_shards() {
        let engine = ShardEngine::new(4);
        // Set multiple keys that will hash to different shards
        for i in 0..20 {
            engine.dispatch_blocking(Command::Set {
                key: format!("key:{}", i).into_bytes(),
                value: format!("val:{}", i).into_bytes(),
                ex: None,
                px: None,
                nx: false,
                xx: false,
            });
        }
        let keys: Vec<Vec<u8>> = (0..20).map(|i| format!("key:{}", i).into_bytes()).collect();
        match engine.dispatch_blocking(Command::MGet { keys }) {
            CommandResponse::Array(values) => {
                assert_eq!(values.len(), 20);
                for (i, v) in values.iter().enumerate() {
                    match v {
                        CommandResponse::BulkString(b) => {
                            assert_eq!(*b, format!("val:{}", i).into_bytes());
                        }
                        other => panic!("Expected BulkString for key:{}, got {:?}", i, other),
                    }
                }
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_engine_dbsize() {
        let engine = ShardEngine::new(4);
        for i in 0..10 {
            engine.dispatch_blocking(Command::Set {
                key: format!("k{}", i).into_bytes(),
                value: b"v".to_vec(),
                ex: None,
                px: None,
                nx: false,
                xx: false,
            });
        }
        match engine.dispatch_blocking(Command::DbSize) {
            CommandResponse::Integer(10) => {}
            other => panic!("Expected 10, got {:?}", other),
        }
    }

    #[test]
    fn test_engine_concurrent_access() {
        let engine = Arc::new(ShardEngine::new(4));
        let mut handles = Vec::new();

        for t in 0..8 {
            let eng = engine.clone();
            handles.push(thread::spawn(move || {
                for i in 0..100 {
                    let key = format!("t{}:k{}", t, i).into_bytes();
                    let val = format!("v{}", i).into_bytes();
                    eng.dispatch_blocking(Command::Set {
                        key: key.clone(),
                        value: val.clone(),
                        ex: None,
                        px: None,
                        nx: false,
                        xx: false,
                    });
                    match eng.dispatch_blocking(Command::Get { key }) {
                        CommandResponse::BulkString(v) => assert_eq!(v, val),
                        CommandResponse::Nil => {} // race with other thread is ok
                        other => panic!("Unexpected: {:?}", other),
                    }
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn test_dispatch_batch_blocking_preserves_barrier_order() {
        let engine = ShardEngine::new(4);
        let responses = engine.dispatch_batch_blocking(vec![
            Command::Set {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                ex: None,
                px: None,
                nx: false,
                xx: false,
            },
            Command::FlushDb,
            Command::Get { key: b"k".to_vec() },
        ]);

        assert_eq!(responses.len(), 3);
        assert!(matches!(responses[0], CommandResponse::Ok));
        assert!(matches!(responses[1], CommandResponse::Ok));
        assert!(matches!(responses[2], CommandResponse::Nil));
    }

    #[test]
    fn test_engine_shutdown() {
        let engine = ShardEngine::new(2);
        engine.dispatch_blocking(Command::Set {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            ex: None,
            px: None,
            nx: false,
            xx: false,
        });
        drop(engine); // should not hang
    }

    #[test]
    fn test_vec_set_and_query_per_shard() {
        let engine = ShardEngine::new(4);

        let vector1 = vec![1.0f32, 0.0, 0.0, 0.0];
        let vector2 = vec![0.0f32, 1.0, 0.0, 0.0];
        let vector3 = vec![1.0f32, 1.0, 0.0, 0.0];

        let resp1 = engine.dispatch_blocking(Command::VecSet {
            key: b"idx".to_vec(),
            dimensions: 4,
            vector: vector1.clone(),
        });
        assert!(matches!(resp1, CommandResponse::Integer(_)));

        let resp2 = engine.dispatch_blocking(Command::VecSet {
            key: b"idx".to_vec(),
            dimensions: 4,
            vector: vector2,
        });
        assert!(matches!(resp2, CommandResponse::Integer(_)));

        let resp3 = engine.dispatch_blocking(Command::VecSet {
            key: b"idx".to_vec(),
            dimensions: 4,
            vector: vector3,
        });
        assert!(matches!(resp3, CommandResponse::Integer(_)));

        let query_resp = engine.dispatch_blocking(Command::VecQuery {
            key: b"idx".to_vec(),
            k: 3,
            vector: vector1,
        });
        match query_resp {
            CommandResponse::Array(results) => {
                assert!(!results.is_empty(), "VecQuery should return results");
                assert!(results.len() <= 3);
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_vec_del() {
        let engine = ShardEngine::new(2);

        engine.dispatch_blocking(Command::VecSet {
            key: b"myidx".to_vec(),
            dimensions: 3,
            vector: vec![1.0, 2.0, 3.0],
        });

        let del_resp = engine.dispatch_blocking(Command::VecDel {
            key: b"myidx".to_vec(),
        });
        assert!(matches!(del_resp, CommandResponse::Integer(1)));

        let del_again = engine.dispatch_blocking(Command::VecDel {
            key: b"myidx".to_vec(),
        });
        assert!(matches!(del_again, CommandResponse::Integer(0)));

        let query_resp = engine.dispatch_blocking(Command::VecQuery {
            key: b"myidx".to_vec(),
            k: 5,
            vector: vec![1.0, 2.0, 3.0],
        });
        match query_resp {
            CommandResponse::Array(results) => assert!(results.is_empty()),
            other => panic!("Expected empty Array, got {:?}", other),
        }
    }

    #[test]
    fn test_vec_query_fan_out() {
        let engine = ShardEngine::new(4);

        for i in 0..10 {
            let v: Vec<f32> = (0..8).map(|d| (i * 8 + d) as f32 * 0.1).collect();
            engine.dispatch_blocking(Command::VecSet {
                key: b"fanout-idx".to_vec(),
                dimensions: 8,
                vector: v,
            });
        }

        let query = vec![0.0f32; 8];
        let resp = engine.dispatch_blocking(Command::VecQuery {
            key: b"fanout-idx".to_vec(),
            k: 5,
            vector: query,
        });
        match resp {
            CommandResponse::Array(results) => {
                assert!(results.len() <= 5);
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }
}
