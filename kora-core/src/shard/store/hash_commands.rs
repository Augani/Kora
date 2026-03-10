use super::*;

impl ShardStore {
    pub(super) fn cmd_hset(
        &mut self,
        key: &[u8],
        fields: &[(Vec<u8>, Vec<u8>)],
    ) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        let is_new = !self.entries.contains_key(&compact);
        let hash = self.get_or_create_hash(&compact);
        match hash {
            Ok(map) => {
                let mut memory_delta: isize = 0;
                if is_new {
                    memory_delta += (key.len()
                        + std::mem::size_of::<CompactKey>()
                        + std::mem::size_of::<KeyEntry>())
                        as isize;
                }
                let mut count = 0i64;
                for (f, v) in fields {
                    let fk = CompactKey::new(f);
                    let new_val = Value::from_raw_bytes(v);
                    memory_delta += (fk.as_bytes().len() + new_val.estimated_size()) as isize;
                    if let Some(old_val) = map.insert(fk, new_val) {
                        memory_delta -= (f.len() + old_val.estimated_size()) as isize;
                    } else {
                        count += 1;
                    }
                }
                if memory_delta >= 0 {
                    self.memory_used += memory_delta as usize;
                } else {
                    self.memory_used = self.memory_used.saturating_sub(memory_delta.unsigned_abs());
                }
                CommandResponse::Integer(count)
            }
            Err(e) => e,
        }
    }

    pub(super) fn cmd_hget(&mut self, key: &[u8], field: &[u8]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::Hash(map) => {
                    let fk = CompactKey::new(field);
                    match map.get(&fk) {
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

    pub(super) fn cmd_hdel(&mut self, key: &[u8], fields: &[Vec<u8>]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        let (count, is_empty) = match self.entries.get_mut(&compact) {
            Some(entry) => match &mut entry.value {
                Value::Hash(map) => {
                    let c = fields
                        .iter()
                        .filter(|f| map.remove(&CompactKey::new(f)).is_some())
                        .count();
                    (c as i64, map.is_empty())
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
            self.entries.remove(&compact);
        }
        CommandResponse::Integer(count)
    }

    pub(super) fn cmd_hgetall(&mut self, key: &[u8]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::Hash(map) => {
                    let mut results = Vec::with_capacity(map.len() * 2);
                    for (k, v) in map {
                        results.push(CommandResponse::BulkString(k.as_bytes().to_vec()));
                        results.push(match v.bulk_response() {
                            Some(resp) => resp,
                            None => CommandResponse::Nil,
                        });
                    }
                    CommandResponse::Array(results)
                }
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Array(vec![]),
        }
    }

    pub(super) fn cmd_hlen(&mut self, key: &[u8]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::Hash(map) => CommandResponse::Integer(map.len() as i64),
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Integer(0),
        }
    }

    pub(super) fn cmd_hexists(&mut self, key: &[u8], field: &[u8]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::Hash(map) => {
                    let fk = CompactKey::new(field);
                    CommandResponse::Integer(if map.contains_key(&fk) { 1 } else { 0 })
                }
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Integer(0),
        }
    }

    pub(super) fn cmd_hincrby(&mut self, key: &[u8], field: &[u8], delta: i64) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        let hash = self.get_or_create_hash(&compact);
        match hash {
            Ok(map) => {
                let fk = CompactKey::new(field);
                let current = match map.get(&fk) {
                    Some(val) => match val.as_bytes().and_then(|b| {
                        std::str::from_utf8(&b)
                            .ok()
                            .and_then(|s| s.parse::<i64>().ok())
                    }) {
                        Some(i) => i,
                        None => {
                            return CommandResponse::Error(
                                "ERR hash value is not an integer".into(),
                            )
                        }
                    },
                    None => 0,
                };
                match current.checked_add(delta) {
                    Some(result) => {
                        map.insert(fk, Value::Int(result));
                        CommandResponse::Integer(result)
                    }
                    None => {
                        CommandResponse::Error("ERR increment or decrement would overflow".into())
                    }
                }
            }
            Err(e) => e,
        }
    }

    pub(super) fn cmd_hmget(&mut self, key: &[u8], fields: &[Vec<u8>]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::Hash(map) => {
                    let results: Vec<CommandResponse> = fields
                        .iter()
                        .map(|f| {
                            let fk = CompactKey::new(f);
                            match map.get(&fk) {
                                Some(val) => val.bulk_response().unwrap_or(CommandResponse::Nil),
                                None => CommandResponse::Nil,
                            }
                        })
                        .collect();
                    CommandResponse::Array(results)
                }
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Array(fields.iter().map(|_| CommandResponse::Nil).collect()),
        }
    }

    pub(super) fn cmd_hkeys(&mut self, key: &[u8]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::Hash(map) => CommandResponse::Array(
                    map.keys()
                        .map(|k| CommandResponse::BulkString(k.as_bytes().to_vec()))
                        .collect(),
                ),
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Array(vec![]),
        }
    }

    pub(super) fn cmd_hvals(&mut self, key: &[u8]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::Hash(map) => CommandResponse::Array(
                    map.values()
                        .map(|v| v.bulk_response().unwrap_or(CommandResponse::Nil))
                        .collect(),
                ),
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Array(vec![]),
        }
    }

    pub(super) fn cmd_hsetnx(&mut self, key: &[u8], field: &[u8], value: &[u8]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        let is_new = !self.entries.contains_key(&compact);
        let hash = self.get_or_create_hash(&compact);
        match hash {
            Ok(map) => {
                let fk = CompactKey::new(field);
                if map.contains_key(&fk) {
                    return CommandResponse::Integer(0);
                }
                let new_val = Value::from_raw_bytes(value);
                let mut mem_delta = fk.as_bytes().len() + new_val.estimated_size();
                if is_new {
                    mem_delta += key.len()
                        + std::mem::size_of::<CompactKey>()
                        + std::mem::size_of::<KeyEntry>();
                }
                map.insert(fk, new_val);
                self.memory_used += mem_delta;
                CommandResponse::Integer(1)
            }
            Err(e) => e,
        }
    }

    pub(super) fn cmd_hincrbyfloat(
        &mut self,
        key: &[u8],
        field: &[u8],
        delta: f64,
    ) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        let hash = self.get_or_create_hash(&compact);
        match hash {
            Ok(map) => {
                let fk = CompactKey::new(field);
                let current = match map.get(&fk) {
                    Some(val) => {
                        match val.as_bytes().and_then(|b| {
                            std::str::from_utf8(&b)
                                .ok()
                                .and_then(|s| s.parse::<f64>().ok())
                        }) {
                            Some(f) => f,
                            None => {
                                return CommandResponse::Error(
                                    "ERR hash value is not a valid float".into(),
                                )
                            }
                        }
                    }
                    None => 0.0,
                };
                let result = current + delta;
                if result.is_infinite() || result.is_nan() {
                    return CommandResponse::Error(
                        "ERR increment would produce NaN or Infinity".into(),
                    );
                }
                let formatted = format_float(result);
                map.insert(fk, Value::from_raw_bytes(formatted.as_bytes()));
                CommandResponse::BulkString(formatted.into_bytes())
            }
            Err(e) => e,
        }
    }

    pub(super) fn cmd_hrandfield(
        &mut self,
        key: &[u8],
        count: Option<i64>,
        withvalues: bool,
    ) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::Hash(map) => {
                    if map.is_empty() {
                        return if count.is_some() {
                            CommandResponse::Array(vec![])
                        } else {
                            CommandResponse::Nil
                        };
                    }
                    let keys: Vec<&CompactKey> = map.keys().collect();
                    match count {
                        None => {
                            let idx = (self.eviction_counter as usize) % keys.len();
                            self.eviction_counter = self.eviction_counter.wrapping_add(1);
                            CommandResponse::BulkString(keys[idx].as_bytes().to_vec())
                        }
                        Some(c) if c >= 0 => {
                            let take = (c as usize).min(keys.len());
                            let mut result =
                                Vec::with_capacity(if withvalues { take * 2 } else { take });
                            let start = (self.eviction_counter as usize) % keys.len();
                            self.eviction_counter = self.eviction_counter.wrapping_add(take as u64);
                            for i in 0..take {
                                let idx = (start + i) % keys.len();
                                result.push(CommandResponse::BulkString(
                                    keys[idx].as_bytes().to_vec(),
                                ));
                                if withvalues {
                                    let val = map.get(keys[idx]).unwrap();
                                    result
                                        .push(val.bulk_response().unwrap_or(CommandResponse::Nil));
                                }
                            }
                            CommandResponse::Array(result)
                        }
                        Some(c) => {
                            let abs_count = c.unsigned_abs() as usize;
                            let mut result = Vec::with_capacity(if withvalues {
                                abs_count * 2
                            } else {
                                abs_count
                            });
                            for i in 0..abs_count {
                                let idx = ((self.eviction_counter as usize) + i) % keys.len();
                                result.push(CommandResponse::BulkString(
                                    keys[idx].as_bytes().to_vec(),
                                ));
                                if withvalues {
                                    let val = map.get(keys[idx]).unwrap();
                                    result
                                        .push(val.bulk_response().unwrap_or(CommandResponse::Nil));
                                }
                            }
                            self.eviction_counter =
                                self.eviction_counter.wrapping_add(abs_count as u64);
                            CommandResponse::Array(result)
                        }
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

    pub(super) fn cmd_hscan(
        &mut self,
        key: &[u8],
        cursor: u64,
        pattern: Option<&str>,
        count: usize,
    ) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::Hash(map) => {
                    let mut fields: Vec<(&CompactKey, &Value)> = map
                        .iter()
                        .filter(|(k, _)| pattern.map_or(true, |p| glob_match(p, k.as_bytes())))
                        .collect();
                    fields.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));

                    let start = cursor as usize;
                    let end = (start + count).min(fields.len());

                    if start >= fields.len() {
                        return CommandResponse::Array(vec![
                            CommandResponse::BulkString(b"0".to_vec()),
                            CommandResponse::Array(vec![]),
                        ]);
                    }

                    let mut results = Vec::with_capacity((end - start) * 2);
                    for (k, v) in &fields[start..end] {
                        results.push(CommandResponse::BulkString(k.as_bytes().to_vec()));
                        results.push(v.bulk_response().unwrap_or(CommandResponse::Nil));
                    }

                    let next_cursor = if end >= fields.len() { 0 } else { end as u64 };

                    CommandResponse::Array(vec![
                        CommandResponse::BulkString(next_cursor.to_string().into_bytes()),
                        CommandResponse::Array(results),
                    ])
                }
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Array(vec![
                CommandResponse::BulkString(b"0".to_vec()),
                CommandResponse::Array(vec![]),
            ]),
        }
    }

    // ─── Set operations ──────────────────────────────────────────────
}
