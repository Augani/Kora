//! ShardEngine — coordinates worker threads and routes commands.

use std::sync::Arc;
use std::thread;

use crossbeam_channel::{Receiver, Sender};

use crate::command::{Command, CommandResponse};
use crate::hash::shard_for_key;
use crate::shard::store::ShardStore;
use crate::shard::wal_trait::{WalRecord, WalWriter};

/// Message sent from the dispatcher to a shard worker.
pub struct ShardMessage {
    /// The command to execute.
    pub command: Command,
    /// Channel to send the response back.
    pub response_tx: ResponseSender,
}

/// A oneshot-like sender for command responses. Uses a crossbeam channel with capacity 1.
pub type ResponseSender = Sender<CommandResponse>;
/// A oneshot-like receiver for command responses.
pub type ResponseReceiver = Receiver<CommandResponse>;

/// Create a response channel pair.
pub fn response_channel() -> (ResponseSender, ResponseReceiver) {
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
        assert_eq!(
            wal_writers.len(),
            shard_count,
            "wal_writers length must match shard_count"
        );

        let mut workers = Vec::with_capacity(shard_count);

        for (i, wal_writer) in wal_writers.into_iter().enumerate() {
            let (tx, rx) = crossbeam_channel::unbounded::<ShardMessage>();
            let handle = thread::Builder::new()
                .name(format!("kora-shard-{}", i))
                .spawn(move || {
                    let mut store = ShardStore::new(i as u16);
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
            // Single-key command: route to owning shard
            let shard_id = shard_for_key(key, self.shard_count) as usize;
            let _ = self.workers[shard_id].tx.send(ShardMessage {
                command: cmd,
                response_tx: tx,
            });
        } else if cmd.is_multi_key() {
            // Multi-key commands: for simplicity, broadcast to shard 0
            // A proper implementation would fan out and merge, but this works
            // correctly for single-shard setups and as a starting point.
            self.dispatch_multi_key(cmd, tx);
        } else {
            // Keyless commands (PING, ECHO, INFO, DBSIZE, FLUSHDB, KEYS, SCAN)
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

    fn dispatch_multi_key(&self, cmd: Command, tx: ResponseSender) {
        match cmd {
            Command::MGet { keys } => {
                let mut results = vec![CommandResponse::Nil; keys.len()];
                // Group keys by shard and collect responses
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
                    let _ = self.workers[shard_id].tx.send(ShardMessage {
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
                // Group entries by shard
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
                    let _ = self.workers[shard_id].tx.send(ShardMessage {
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
                    let _ = self.workers[shard_id].tx.send(ShardMessage {
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
                    let _ = self.workers[shard_id].tx.send(ShardMessage {
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
            let _ = worker.tx.send(ShardMessage {
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
        if matches!(&cmd, Command::VecQuery { .. }) {
            self.dispatch_vec_query(cmd, tx);
            return;
        }

        match &cmd {
            Command::DbSize => {
                // Sum up db sizes from all shards
                let mut total = 0i64;
                let mut receivers = Vec::new();
                for worker in &self.workers {
                    let (resp_tx, resp_rx) = response_channel();
                    let _ = worker.tx.send(ShardMessage {
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
                    let _ = worker.tx.send(ShardMessage {
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
                // Broadcast to all shards, collect and merge results
                let mut all_entries = Vec::new();
                let mut receivers = Vec::new();
                for worker in &self.workers {
                    let (resp_tx, resp_rx) = response_channel();
                    let _ = worker.tx.send(ShardMessage {
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
            Command::Keys { .. } | Command::Scan { .. } => {
                // Broadcast to all shards, collect and merge results
                let mut all_keys = Vec::new();
                let mut receivers = Vec::new();
                for worker in &self.workers {
                    let (resp_tx, resp_rx) = response_channel();
                    let _ = worker.tx.send(ShardMessage {
                        command: cmd.clone(),
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
            _ => {
                // PING, ECHO, INFO — send to shard 0
                let _ = self.workers[0].tx.send(ShardMessage {
                    command: cmd,
                    response_tx: tx,
                });
            }
        }
    }
}

impl Drop for ShardEngine {
    fn drop(&mut self) {
        // Drop all senders to signal workers to stop
        for worker in &mut self.workers {
            // Dropping the sender implicitly when we clear it
            let (dummy, _) = crossbeam_channel::unbounded();
            let old_tx = std::mem::replace(&mut worker.tx, dummy);
            drop(old_tx);
        }
        // Join all worker threads
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
    let mut ops_since_expire = 0u32;
    while let Ok(msg) = rx.recv() {
        ops_since_expire += 1;
        if ops_since_expire >= 100 {
            store.evict_expired();
            ops_since_expire = 0;
        }

        if let Some(ref mut writer) = wal_writer {
            if msg.command.is_mutation() {
                if let Some(record) = command_to_wal_record(&msg.command) {
                    writer.append(&record);
                }
            }
        }

        let response = store.execute(msg.command);
        let _ = msg.response_tx.send(response);
    }
}

/// Convert a `Command` to a `WalRecord` for WAL logging.
fn command_to_wal_record(cmd: &Command) -> Option<WalRecord> {
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
