use super::*;

impl ShardStore {
    pub(super) fn cmd_lpush(&mut self, key: &[u8], values: &[Vec<u8>]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        let is_new = !self.entries.contains_key(&compact);
        let mut mem_delta: usize = 0;
        if is_new {
            mem_delta +=
                key.len() + std::mem::size_of::<CompactKey>() + std::mem::size_of::<KeyEntry>();
        }
        let prepared_values: Vec<Value> = values
            .iter()
            .map(|v| {
                let val = Value::from_raw_bytes(v);
                mem_delta += val.estimated_size();
                val
            })
            .collect();
        let list = self.get_or_create_list(&compact);
        match list {
            Ok(deque) => {
                for val in prepared_values {
                    deque.push_front(val);
                }
                let len = deque.len() as i64;
                self.memory_used += mem_delta;
                CommandResponse::Integer(len)
            }
            Err(e) => e,
        }
    }

    pub(super) fn cmd_rpush(&mut self, key: &[u8], values: &[Vec<u8>]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        let is_new = !self.entries.contains_key(&compact);
        let mut mem_delta: usize = 0;
        if is_new {
            mem_delta +=
                key.len() + std::mem::size_of::<CompactKey>() + std::mem::size_of::<KeyEntry>();
        }
        let prepared_values: Vec<Value> = values
            .iter()
            .map(|v| {
                let val = Value::from_raw_bytes(v);
                mem_delta += val.estimated_size();
                val
            })
            .collect();
        let list = self.get_or_create_list(&compact);
        match list {
            Ok(deque) => {
                for val in prepared_values {
                    deque.push_back(val);
                }
                let len = deque.len() as i64;
                self.memory_used += mem_delta;
                CommandResponse::Integer(len)
            }
            Err(e) => e,
        }
    }

    pub(super) fn cmd_lpop(&mut self, key: &[u8]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        let (result, is_empty) = match self.entries.get_mut(&compact) {
            Some(entry) => match &mut entry.value {
                Value::List(deque) => {
                    let val = deque.pop_front();
                    let empty = deque.is_empty();
                    match val {
                        Some(v) => match v.bulk_response() {
                            Some(resp) => (resp, empty),
                            None => (CommandResponse::Nil, empty),
                        },
                        None => (CommandResponse::Nil, false),
                    }
                }
                _ => (
                    CommandResponse::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    ),
                    false,
                ),
            },
            None => (CommandResponse::Nil, false),
        };
        if is_empty {
            self.entries.remove(&compact);
        }
        result
    }

    pub(super) fn cmd_rpop(&mut self, key: &[u8]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        let (result, is_empty) = match self.entries.get_mut(&compact) {
            Some(entry) => match &mut entry.value {
                Value::List(deque) => {
                    let val = deque.pop_back();
                    let empty = deque.is_empty();
                    match val {
                        Some(v) => match v.bulk_response() {
                            Some(resp) => (resp, empty),
                            None => (CommandResponse::Nil, empty),
                        },
                        None => (CommandResponse::Nil, false),
                    }
                }
                _ => (
                    CommandResponse::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    ),
                    false,
                ),
            },
            None => (CommandResponse::Nil, false),
        };
        if is_empty {
            self.entries.remove(&compact);
        }
        result
    }

    pub(super) fn cmd_llen(&mut self, key: &[u8]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::List(deque) => CommandResponse::Integer(deque.len() as i64),
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Integer(0),
        }
    }

    pub(super) fn cmd_lrange(&mut self, key: &[u8], start: i64, stop: i64) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::List(deque) => {
                    let len = deque.len() as i64;
                    let s = normalize_index(start, len);
                    let e = normalize_index(stop, len);
                    if s > e || s >= len as usize {
                        return CommandResponse::Array(vec![]);
                    }
                    let e = e.min(len as usize - 1);
                    let results: Vec<CommandResponse> = deque
                        .iter()
                        .skip(s)
                        .take(e - s + 1)
                        .map(|v| match v.bulk_response() {
                            Some(resp) => resp,
                            None => CommandResponse::Nil,
                        })
                        .collect();
                    CommandResponse::Array(results)
                }
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Array(vec![]),
        }
    }

    pub(super) fn cmd_lindex(&mut self, key: &[u8], index: i64) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::List(deque) => {
                    let idx = normalize_index(index, deque.len() as i64);
                    match deque.get(idx) {
                        Some(val) => match val.bulk_response() {
                            Some(resp) => resp,
                            None => CommandResponse::Nil,
                        },
                        None => CommandResponse::Nil,
                    }
                }
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Nil,
        }
    }

    pub(super) fn cmd_lset(&mut self, key: &[u8], index: i64, value: &[u8]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get_mut(&compact) {
            Some(entry) => match &mut entry.value {
                Value::List(deque) => {
                    let len = deque.len() as i64;
                    let idx = if index < 0 { len + index } else { index };
                    if idx < 0 || idx >= len {
                        return CommandResponse::Error("ERR index out of range".into());
                    }
                    deque[idx as usize] = Value::from_raw_bytes(value);
                    CommandResponse::Ok
                }
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Error("ERR no such key".into()),
        }
    }

    pub(super) fn cmd_linsert(
        &mut self,
        key: &[u8],
        before: bool,
        pivot: &[u8],
        value: &[u8],
    ) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get_mut(&compact) {
            Some(entry) => match &mut entry.value {
                Value::List(deque) => {
                    let pos = deque
                        .iter()
                        .position(|v| v.as_bytes().is_some_and(|b| b == pivot));
                    match pos {
                        Some(idx) => {
                            let insert_idx = if before { idx } else { idx + 1 };
                            let new_val = Value::from_raw_bytes(value);
                            self.memory_used += new_val.estimated_size();
                            deque.insert(insert_idx, new_val);
                            CommandResponse::Integer(deque.len() as i64)
                        }
                        None => CommandResponse::Integer(-1),
                    }
                }
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Integer(0),
        }
    }

    pub(super) fn cmd_lrem(&mut self, key: &[u8], count: i64, value: &[u8]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        let (removed, is_empty) = match self.entries.get_mut(&compact) {
            Some(entry) => match &mut entry.value {
                Value::List(deque) => {
                    let mut removed = 0i64;
                    let abs_count = count.unsigned_abs() as usize;
                    let max_removals = if count == 0 { usize::MAX } else { abs_count };

                    if count >= 0 {
                        let mut i = 0;
                        while i < deque.len() && (removed as usize) < max_removals {
                            if deque[i].as_bytes().is_some_and(|b| b == value) {
                                deque.remove(i);
                                removed += 1;
                            } else {
                                i += 1;
                            }
                        }
                    } else {
                        let mut i = deque.len();
                        while i > 0 && (removed as usize) < max_removals {
                            i -= 1;
                            if deque[i].as_bytes().is_some_and(|b| b == value) {
                                deque.remove(i);
                                removed += 1;
                            }
                        }
                    }
                    (removed, deque.is_empty())
                }
                _ => {
                    return CommandResponse::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    )
                }
            },
            None => return CommandResponse::Integer(0),
        };
        if is_empty {
            self.entries.remove(&CompactKey::new(key));
        }
        CommandResponse::Integer(removed)
    }

    pub(super) fn cmd_ltrim(&mut self, key: &[u8], start: i64, stop: i64) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        let is_empty = match self.entries.get_mut(&compact) {
            Some(entry) => match &mut entry.value {
                Value::List(deque) => {
                    let len = deque.len() as i64;
                    let s = normalize_index(start, len);
                    let e = normalize_index(stop, len);
                    if s > e || s >= deque.len() {
                        deque.clear();
                    } else {
                        let e = e.min(deque.len() - 1);
                        deque.truncate(e + 1);
                        deque.drain(..s);
                    }
                    deque.is_empty()
                }
                _ => {
                    return CommandResponse::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    )
                }
            },
            None => return CommandResponse::Ok,
        };
        if is_empty {
            self.entries.remove(&CompactKey::new(key));
        }
        CommandResponse::Ok
    }

    pub(super) fn cmd_lpos(
        &mut self,
        key: &[u8],
        value: &[u8],
        rank: Option<i64>,
        count: Option<i64>,
        maxlen: Option<i64>,
    ) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::List(deque) => {
                    let rank_val = rank.unwrap_or(1);
                    if rank_val == 0 {
                        return CommandResponse::Error("ERR RANK can't be zero".into());
                    }
                    let reverse = rank_val < 0;
                    let skip = (rank_val.unsigned_abs() - 1) as usize;
                    let return_array = count.is_some();
                    let max_count = match count {
                        Some(0) => usize::MAX,
                        Some(c) => c.unsigned_abs() as usize,
                        None => 1,
                    };
                    let max_scan = maxlen
                        .map(|m| m.unsigned_abs() as usize)
                        .unwrap_or(deque.len());

                    let mut matches = Vec::new();
                    let mut skipped = 0usize;

                    if reverse {
                        for (i, elem) in deque.iter().enumerate().rev().take(max_scan) {
                            if elem.as_bytes().is_some_and(|b| b == value) {
                                if skipped < skip {
                                    skipped += 1;
                                } else {
                                    matches.push(i as i64);
                                    if matches.len() >= max_count {
                                        break;
                                    }
                                }
                            }
                        }
                    } else {
                        for (i, elem) in deque.iter().enumerate().take(max_scan) {
                            if elem.as_bytes().is_some_and(|b| b == value) {
                                if skipped < skip {
                                    skipped += 1;
                                } else {
                                    matches.push(i as i64);
                                    if matches.len() >= max_count {
                                        break;
                                    }
                                }
                            }
                        }
                    }

                    if return_array {
                        CommandResponse::Array(
                            matches.into_iter().map(CommandResponse::Integer).collect(),
                        )
                    } else {
                        matches
                            .first()
                            .map_or(CommandResponse::Nil, |&pos| CommandResponse::Integer(pos))
                    }
                }
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => {
                if count.is_some() {
                    CommandResponse::Array(vec![])
                } else {
                    CommandResponse::Nil
                }
            }
        }
    }

    pub(super) fn cmd_rpoplpush(&mut self, source: &[u8], destination: &[u8]) -> CommandResponse {
        self.cmd_lmove(source, destination, false, true)
    }

    pub(super) fn cmd_lmove(
        &mut self,
        source: &[u8],
        destination: &[u8],
        from_left: bool,
        to_left: bool,
    ) -> CommandResponse {
        self.lazy_expire(source);
        let src_compact = CompactKey::new(source);

        let (popped, src_empty) = match self.entries.get_mut(&src_compact) {
            Some(entry) => match &mut entry.value {
                Value::List(deque) => {
                    let val = if from_left {
                        deque.pop_front()
                    } else {
                        deque.pop_back()
                    };
                    let empty = deque.is_empty();
                    match val {
                        Some(v) => (Some(v), empty),
                        None => (None, false),
                    }
                }
                _ => {
                    return CommandResponse::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    )
                }
            },
            None => return CommandResponse::Nil,
        };

        if src_empty {
            self.entries.remove(&src_compact);
        }

        let popped = match popped {
            Some(v) => v,
            None => return CommandResponse::Nil,
        };

        let response = popped.bulk_response().unwrap_or(CommandResponse::Nil);

        self.lazy_expire(destination);
        let dest_compact = CompactKey::new(destination);
        let dest_list = self.get_or_create_list(&dest_compact);
        match dest_list {
            Ok(deque) => {
                if to_left {
                    deque.push_front(popped);
                } else {
                    deque.push_back(popped);
                }
            }
            Err(e) => return e,
        }

        response
    }

    // ─── Hash operations ─────────────────────────────────────────────
}
