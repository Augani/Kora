# Kōra Document Layer — Architecture

> **"Faster reads, smaller footprint, no compromises."**

The document layer (`kora-doc`) turns Kōra from a key-value engine into a JSON-native document database where point reads resolve to direct key/index lookups with bounded decode work. Documents are stored as compact packed buffers with offset tables for O(log F) field access (`F = fields per doc`), indexes are pre-built on write, and integer-keyed metadata minimizes string repetition.

---

## 1. Design Principles

1. **No general query planner on the hot read path.** Reads use direct key/index lookups plus bounded set operations.
2. **Packed storage.** All fields for a document live in one contiguous allocation with an offset table. Single-field reads use O(log F) offset lookup, full-doc reads are sequential scans. No per-field shard entry overhead.
3. **Integer-keyed everything.** Collection names, field paths, and document IDs map to compact 2-4 byte integer IDs. Strings never appear in the hot storage path.
4. **Dictionary-compressed values.** Repeated string values (cities, countries, statuses) stored once, referenced by 4-byte ID everywhere.
5. **Per-field thermal management.** Hot fields stay in RAM, cold fields sink to NVMe/disk — within the same document — via hot/cold pack splitting.
6. **Write amplification is acceptable.** Writes produce compact output and pre-build all indexes. A 20-field document with 5 indexes typically costs ~6 shard writes total.
7. **Redis compatibility preserved.** The document layer is additive — existing RESP commands continue to work. DOC.* commands are a new namespace.
8. **Crash-safe write ordering.** Document packs and secondary indexes are committed via WAL intent + idempotent apply + manifest publish.

---

## 2. Crate Structure

```
kora/
├── Cargo.toml                  # workspace root
├── kora-core/                  # shard engine, data structures, memory management
├── kora-protocol/              # RESP2/RESP3 parser and serializer
├── kora-server/                # TCP/Unix listener, connection dispatch
├── kora-embedded/              # library mode — direct API, no network
├── kora-storage/               # tiered storage, NVMe spill, io_uring async I/O
├── kora-vector/                # HNSW index, similarity search
├── kora-cdc/                   # change data capture, stream subscriptions
├── kora-scripting/             # WASM runtime (wasmtime)
├── kora-observability/         # per-key stats, hot-key detection
├── kora-cli/                   # CLI binary, config parsing, entrypoint
│
└── kora-doc/                   # document database layer
    ├── Cargo.toml
    └── src/
        ├── lib.rs              # public API surface
        ├── collection.rs       # collection management, optional schemas
        ├── packed.rs           # PackedDoc format: builder, reader, field access
        ├── decompose.rs        # JSON → PackedDoc builder pipeline
        ├── recompose.rs        # PackedDoc → JSON reconstruction
        ├── registry.rs         # ID registries: collections, fields, doc IDs
        ├── dictionary.rs       # value dictionary for low-cardinality strings
        ├── index.rs            # write-time index maintenance, bucket format
        ├── query.rs            # read-path query resolution (lookups, not scans)
        ├── commands.rs         # DOC.* RESP command handlers
        ├── schema.rs           # field type definitions, validation
        ├── mutation.rs         # partial updates, atomic field-level writes
        ├── thermal.rs          # per-field access tracking, hot/cold splitting
        ├── compress.rs         # compression profiles (dictionary, LZ4, varint)
        └── expr.rs             # WHERE clause expression parser + evaluator
```

### Dependency Position

```
kora-doc depends on:
  → kora-core          (shard engine, data structures)
  → kora-storage       (tiered storage hints)
  → kora-cdc           (stream mutations for subscriptions)
  → kora-observability  (access frequency tracking)

kora-server depends on:
  → kora-doc           (DOC.* command handlers)

kora-embedded depends on:
  → kora-doc           (direct document API)
```

`kora-doc` never depends on `kora-server` or `kora-protocol`. It operates on the shard engine directly.

### Ownership and Concurrency Model

- `kora-doc` follows Kōra's shared-nothing model: each shard owns a disjoint subset of document keys and index buckets.
- Registry/dictionary mutations are routed to the owning shard and applied there; no cross-shard locks on the data path.
- Read-side metadata uses immutable snapshots (`Arc`) swapped by shard-local writers.
- Cross-shard reads (for broad queries) are fan-out + merge, not shared mutable state.

---

## 3. Data Model

### 3.1 Collections

A collection is a named namespace that groups related documents. Collections are lightweight — they're metadata entries in the shard engine, not separate storage regions.

```rust
/// Collection metadata, stored at key [0x02][col_id:2]
struct Collection {
    name: String,
    id: u16,
    created_at: u64,
    schema: Option<Schema>,
    index_config: IndexConfig,
    compression: CompressionProfile,
    doc_count: AtomicU64,
    field_registry: FieldRegistry,
}

/// Optional schema — not required, but unlocks validation + storage optimizations
struct Schema {
    fields: Vec<FieldDef>,
    strict: bool, // if true, reject documents with unknown fields
}

struct FieldDef {
    path: String,           // e.g. "address.city", "tags[]"
    field_type: FieldType,
    indexed: IndexType,
    required: bool,
    default: Option<Value>,
}

enum FieldType {
    String,
    Number,    // stored as f64 internally (covers i64 range for integers)
    Bool,
    Array,     // homogeneous element type tracked separately
    Object,    // nested fields decomposed recursively
    Null,
    Any,       // schemaless — type inferred per document
}

enum IndexType {
    None,      // field stored but not indexed — lookup by doc_id only
    Hash,      // equality lookups: WHERE city = "Accra"
    Sorted,    // range queries: WHERE age >= 25 AND age <= 35
    FullText,  // token-based: WHERE bio CONTAINS "rust"
    Array,     // set membership: WHERE tags CONTAINS "systems"
    Unique,    // uniqueness constraint + direct lookup by value
}
```

### 3.2 ID Registries

All string identifiers are assigned compact integer IDs on first use. Strings never appear in the hot storage path.

```rust
/// Registry directory plus shard-owned collection segments.
/// Logical global namespace, physical ownership by shard.
struct IdRegistry {
    /// Collection name → 2-byte ID (max 65K collections).
    /// Updated through routed commands; read via immutable snapshot.
    collections: BiMap<String, u16>,

    /// Per-collection registry segment location.
    segments: HashMap<u16, RegistrySegmentRef>,

    /// Monotonic counters for ID assignment
    next_collection_id: AtomicU16,
}

/// Pointer to the owning shard and storage key for a collection registry segment.
type RegistrySegmentRef = (u16, Vec<u8>); // (owner_shard, segment_key)

/// Stored and mutated on the shard that owns `collection_id`.
struct RegistrySegment {
    /// Per-collection field path → 2-byte ID (max 65K fields per collection)
    /// "address.city" → 0x000A
    fields: BiMap<String, u16>,

    /// Per-collection document ID → 4-byte internal ID (max 4B docs per collection)
    /// "user_abc123" → 0x00000001
    doc_ids: BiMap<String, u32>,

    next_field_id: AtomicU16,
    next_doc_id: AtomicU32,
}

impl IdRegistry {
    /// Register or retrieve a field path ID
    /// Called on the owning shard for this collection.
    fn field_id(segment: &mut RegistrySegment, path: &str) -> u16 {
        if let Some(id) = segment.fields.get_by_left(path) {
            return *id;
        }
        let id = segment.next_field_id.fetch_add(1, Ordering::Relaxed);
        segment.fields.insert(path.to_string(), id);
        id
    }

    /// Register or retrieve a document internal ID
    /// Called on the owning shard for this collection.
    fn doc_internal_id(segment: &mut RegistrySegment, doc_id: &str) -> u32 {
        if let Some(id) = segment.doc_ids.get_by_left(doc_id) {
            return *id;
        }
        let id = segment.next_doc_id.fetch_add(1, Ordering::Relaxed);
        segment.doc_ids.insert(doc_id.to_string(), id);
        id
    }

    /// Reverse lookup — internal ID → user-facing doc_id string
    fn doc_external_id(segment: &RegistrySegment, internal_id: u32) -> Option<&str> {
        segment
            .doc_ids
            .get_by_right(&internal_id)
            .map(|s| s.as_str())
    }
}
```

**Cost:** 1 million documents with 50 unique field paths and average 16-char doc_ids = ~20-30MB of registry data. Loaded into RAM on startup, persisted periodically.

### 3.3 Value Dictionary

Repeated string values are stored once in a dictionary and referenced by a 4-byte ID everywhere else.

```rust
/// Per-collection value dictionary for low-cardinality string fields
/// "Accra" appears in 100K documents but is stored exactly once
struct ValueDictionary {
    /// value_string → 4-byte dictionary ID
    values: BiMap<Vec<u8>, u32>,
    next_id: AtomicU32,

    /// Cardinality tracker — only dictionary-encode fields with < 10K unique values
    field_cardinality: HashMap<u16, HyperLogLog>,
}

impl ValueDictionary {
    /// Encode a value — returns DictRef if low-cardinality, raw bytes if high-cardinality
    fn encode(&self, field_id: u16, value: &[u8]) -> StoredValue {
        let cardinality = self.field_cardinality
            .entry(field_id)
            .or_default()
            .estimate();

        if cardinality < 10_000 && value.len() > 4 {
            let dict_id = self.get_or_insert(value);
            StoredValue::DictRef(dict_id)
        } else {
            StoredValue::Inline(value.to_vec())
        }
    }

    fn decode(&self, stored: &StoredValue) -> Vec<u8> {
        match stored {
            StoredValue::DictRef(id) => self.values.get_by_right(id).unwrap().clone(),
            StoredValue::Inline(bytes) => bytes.clone(),
        }
    }
}

enum StoredValue {
    DictRef(u32),       // 4 bytes — reference to dictionary
    Inline(Vec<u8>),    // variable — stored directly
}
```

**Savings example:** A "city" field with 500 unique cities across 1M documents. Without dictionary: 1M × ~8 bytes avg = 8MB. With dictionary: 500 × 8 bytes + 1M × 4 bytes = 4MB. **50% savings** on that field. For "country" (< 200 values) or "status" (< 10 values), savings exceed 80%.

---

## 4. Packed Document Format

All fields for a single document live in one contiguous byte buffer. The format is designed for fast O(log F) field access without JSON parsing.

```
┌───────────────────────────────────────────────────────────────┐
│                    PACKED DOCUMENT FORMAT                      │
│                                                               │
│  Bytes 0-7:    Header                                         │
│    [0-1]       version (u16)                                  │
│    [2-3]       field_count (u16)                               │
│    [4-7]       updated_at (u32, seconds since epoch offset)    │
│                                                               │
│  Bytes 8..N:   Offset Table (4 bytes per field, sorted by     │
│                field_id for binary search)                     │
│    Per entry:                                                 │
│      [0-1]     field_id (u16)                                 │
│      [2-3]     data_offset (u16, relative to data region)      │
│                v1 cap: data region <= 65,535 bytes             │
│                                                               │
│  Bytes N..M:   Type Table (1 byte per field)                  │
│    Per entry:                                                 │
│      [0]       value_type + length encoding                   │
│                0x00 = null (0 bytes)                           │
│                0x01 = bool_false (0 bytes)                     │
│                0x02 = bool_true (0 bytes)                      │
│                0x03 = i64 (8 bytes, or varint)                 │
│                0x04 = f64 (8 bytes)                            │
│                0x05 = string_inline (next 2 bytes = length)    │
│                0x06 = string_dictref (4 bytes)                 │
│                0x07 = array (next 2 bytes = byte length)       │
│                                                               │
│  Bytes M..end: Data Region (variable-length field values,     │
│                packed contiguously, no padding)                │
│                                                               │
└───────────────────────────────────────────────────────────────┘

Example: {"name":"Augustus","city":"Accra","age":30,"active":true}

Header:          [00 01] [00 04] [xx xx xx xx]        = 8 bytes
Offset table:    [00 00 00 00]  field 0 → offset 0    
                 [00 01 00 08]  field 1 → offset 8    
                 [00 02 00 0C]  field 2 → offset 12   
                 [00 03 00 14]  field 3 → offset 20   = 16 bytes
Type table:      [05] [06] [03] [02]                   = 4 bytes
Data region:     [00 08] "Augustus"                    = 10 bytes (inline string)
                 [00 00 00 03]                         = 4 bytes  (dict ref → "Accra")
                 [00 00 00 00 00 00 00 1E]             = 8 bytes  (i64 = 30)
                 (bool_true = 0 bytes in data)         = 0 bytes

Total: 8 + 16 + 4 + 22 = 50 bytes
vs raw JSON: 57 bytes — packed format is 12% smaller than the JSON itself
```

**v1 size guard:** packed docs are capped to 64KB data-region offsets (`u16`). Larger documents use overflow packs (multiple segment keys) and are marked in document metadata so reads can stitch segments deterministically.

```rust
/// Packed document — single contiguous allocation
struct PackedDoc {
    data: Vec<u8>,
}

impl PackedDoc {
    /// Read a single field by field_id — O(log N) binary search, ~10ns
    fn read_field(&self, field_id: u16) -> Option<FieldValue> {
        let header = self.header();
        let offset_table = self.offset_table(header.field_count);

        // Binary search the offset table by field_id
        let idx = offset_table.binary_search_by_key(&field_id, |e| e.field_id).ok()?;
        let entry = &offset_table[idx];

        let type_byte = self.type_table()[idx];
        let data_start = self.data_region_offset() + entry.data_offset as usize;

        Some(decode_field_value(type_byte, &self.data[data_start..]))
    }

    /// Read multiple fields — single pass through offset table
    fn read_fields(&self, field_ids: &[u16]) -> Vec<Option<FieldValue>> {
        field_ids.iter().map(|id| self.read_field(*id)).collect()
    }

    /// Full document iteration — sequential scan, no search needed
    fn iter_fields(&self) -> impl Iterator<Item = (u16, FieldValue)> + '_ {
        let header = self.header();
        (0..header.field_count as usize).map(move |i| {
            let offset_entry = self.offset_entry(i);
            let type_byte = self.type_table()[i];
            let data_start = self.data_region_offset() + offset_entry.data_offset as usize;
            let value = decode_field_value(type_byte, &self.data[data_start..]);
            (offset_entry.field_id, value)
        })
    }

    /// Size in bytes
    fn byte_size(&self) -> usize { self.data.len() }
}
```

### Packed Document Builder

Used on the write path to construct packed documents efficiently.

```rust
struct PackedDocBuilder {
    version: u16,
    fields: Vec<(u16, u8, Vec<u8>)>,  // (field_id, type_tag, data_bytes)
    dict: Arc<ValueDictionary>,
    collection_id: u16,
}

impl PackedDocBuilder {
    fn add_field(&mut self, field_id: u16, value: &serde_json::Value) {
        match value {
            Value::Null => {
                self.fields.push((field_id, TYPE_NULL, vec![]));
            }
            Value::Bool(b) => {
                let tag = if *b { TYPE_BOOL_TRUE } else { TYPE_BOOL_FALSE };
                self.fields.push((field_id, tag, vec![]));
            }
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    self.fields.push((field_id, TYPE_I64, i.to_le_bytes().to_vec()));
                } else {
                    let f = n.as_f64().unwrap();
                    self.fields.push((field_id, TYPE_F64, f.to_le_bytes().to_vec()));
                }
            }
            Value::String(s) => {
                let stored = self.dict.encode(field_id, s.as_bytes());
                match stored {
                    StoredValue::DictRef(id) => {
                        self.fields.push((field_id, TYPE_STR_DICT, id.to_le_bytes().to_vec()));
                    }
                    StoredValue::Inline(bytes) => {
                        let mut data = (bytes.len() as u16).to_le_bytes().to_vec();
                        data.extend_from_slice(&bytes);
                        self.fields.push((field_id, TYPE_STR_INLINE, data));
                    }
                }
            }
            Value::Array(arr) => {
                let encoded = encode_array_compact(arr, &self.dict, field_id);
                self.fields.push((field_id, TYPE_ARRAY, encoded));
            }
            Value::Object(_) => {
                // Nested objects are flattened by the decomposer before reaching the builder.
                // "address": {"city": "Accra"} → field_id for "address.city" added as a string.
            }
        }
    }

    fn build(mut self, updated_at: u32) -> PackedDoc {
        // Sort fields by field_id for binary search at read time
        self.fields.sort_by_key(|(id, _, _)| *id);

        let field_count = self.fields.len() as u16;
        let header_size = 8;
        let offset_table_size = field_count as usize * 4;
        let type_table_size = field_count as usize;
        let data_size: usize = self.fields.iter().map(|(_, _, d)| d.len()).sum();
        let total = header_size + offset_table_size + type_table_size + data_size;

        let mut buf = Vec::with_capacity(total);

        // Header
        buf.extend_from_slice(&self.version.to_le_bytes());
        buf.extend_from_slice(&field_count.to_le_bytes());
        buf.extend_from_slice(&updated_at.to_le_bytes());

        // Offset table
        let mut data_offset: u16 = 0;
        for (field_id, _, data) in &self.fields {
            buf.extend_from_slice(&field_id.to_le_bytes());
            buf.extend_from_slice(&data_offset.to_le_bytes());
            data_offset = data_offset
                .checked_add(data.len() as u16)
                .expect("packed doc data region exceeds 64KB; route to overflow-pack writer");
        }

        // Type table
        for (_, type_tag, _) in &self.fields {
            buf.push(*type_tag);
        }

        // Data region
        for (_, _, data) in &self.fields {
            buf.extend_from_slice(data);
        }

        PackedDoc { data: buf }
    }
}
```

---

## 5. Shard Key Layout

All keys are compact binary — no string repetition.

```
Tag  Format                              Size   Description
──────────────────────────────────────────────────────────────
0x01 [col:2][doc:4]                      7B     packed document
0x02 [col:2]                             3B     collection metadata
0x03 [col:2]                             3B     schema definition
0x04 [col:2]                             3B     value dictionary
0x05 [col:2]                             3B     ID registry segment
0x06 [col:2][doc:4]                      7B     cold pack (split doc)
0x10 [col:2][field:2][vhash:4]           10B    hash index bucket
0x11 [col:2][field:2]                    6B     sorted index
0x12 [col:2][field:2][vhash:4]           10B    array index bucket
0x13 [col:2][field:2][vhash:4]           10B    unique index
0x14 [col:2][f1:2][f2:2][vhash:4]       12B    compound index
0x20 [col:2]                             3B     CDC stream head
0x21 [col:2][seq:8]                      11B    CDC event
```

Default path uses one shard entry per document plus one entry per index bucket. Split documents add hot/cold pack keys and a small manifest key. A 20-field document with 5 indexes typically costs 6 shard updates (1 packed doc + 5 index buckets) before optional split metadata.

---

## 6. Write Path

### 6.1 Pipeline

```
                    ┌──────────────────────────────────────────────────┐
   DOC.SET          │                   WRITE PATH                     │
   JSON in ─────►   │                                                  │
                    │  1. Parse JSON (serde_json)                ~200ns│
                    │                │                                  │
                    │  2. Resolve IDs                                   │
                    │     collection → col_id (2 bytes)                 │
                    │     doc_id → internal_id (4 bytes)                │
                    │     field paths → field_ids (2 bytes each)        │
                    │                │                                  │
                    │  3. Dictionary-encode string values               │
                    │     low-cardinality → dict_ref (4 bytes)          │
                    │     high-cardinality → inline                     │
                    │                │                                  │
                    │  4. Build PackedDoc                               │
                    │     offset table + type table + data region       │
                    │     single contiguous allocation                  │
                    │                │                                  │
                    │  5. Compute index deltas                          │
                    │     (if update: diff old packed doc vs new)        │
                    │                │                                  │
                    │  6. Append WAL intent + apply idempotent updates  │
                    │     doc pack writes + index bucket updates        │
                    │                │                                  │
                    │  7. Publish manifest epoch (commit) + emit CDC    │
                    │                                                  │
                    └──────────────────────────────────────────────────┘
```

### 6.2 Decomposer

Flattens a JSON value into field entries for the packed doc builder, and computes index operations.

```rust
struct Decomposer {
    collection_id: u16,
    doc_internal_id: u32,
    registry: Arc<IdRegistry>,
    dict: Arc<ValueDictionary>,
}

impl Decomposer {
    fn decompose(
        &self,
        doc_id: &str,
        json: &serde_json::Value,
        index_config: &IndexConfig,
    ) -> (PackedDoc, Vec<IndexOp>) {
        let mut builder = PackedDocBuilder::new(
            self.collection_id,
            self.dict.clone(),
        );
        let mut index_ops = Vec::new();

        self.walk("", json, &mut builder, &mut index_ops, index_config);

        let packed = builder.build(now_seconds());
        (packed, index_ops)
    }

    fn walk(
        &self,
        prefix: &str,
        value: &serde_json::Value,
        builder: &mut PackedDocBuilder,
        index_ops: &mut Vec<IndexOp>,
        index_config: &IndexConfig,
    ) {
        match value {
            Value::Object(map) => {
                for (k, v) in map {
                    let path = if prefix.is_empty() {
                        k.clone()
                    } else {
                        format!("{}.{}", prefix, k)
                    };
                    self.walk(&path, v, builder, index_ops, index_config);
                }
            }
            Value::Array(arr) => {
                let field_id = self.registry.field_id(self.collection_id, prefix);
                builder.add_field(field_id, value);

                if index_config.is_indexed(prefix) {
                    for elem in arr {
                        if let Some(s) = elem.as_str() {
                            index_ops.push(IndexOp::ArrayAdd {
                                field_id,
                                value_hash: hash32(s.as_bytes()),
                                doc_id: self.doc_internal_id,
                            });
                        }
                    }
                }
            }
            Value::String(s) => {
                let field_id = self.registry.field_id(self.collection_id, prefix);
                builder.add_field(field_id, value);

                if index_config.is_indexed(prefix) {
                    index_ops.push(IndexOp::HashAdd {
                        field_id,
                        value_hash: hash32(s.as_bytes()),
                        doc_id: self.doc_internal_id,
                    });
                }
            }
            Value::Number(n) => {
                let field_id = self.registry.field_id(self.collection_id, prefix);
                builder.add_field(field_id, value);

                if index_config.is_indexed(prefix) {
                    index_ops.push(IndexOp::SortedAdd {
                        field_id,
                        doc_id: self.doc_internal_id,
                        score: n.as_f64().unwrap_or(0.0),
                    });
                }
            }
            Value::Bool(_) | Value::Null => {
                let field_id = self.registry.field_id(self.collection_id, prefix);
                builder.add_field(field_id, value);

                if index_config.is_indexed(prefix) {
                    let val_bytes = match value {
                        Value::Bool(b) => if *b { b"true" } else { b"false" },
                        Value::Null => b"null",
                        _ => unreachable!(),
                    };
                    index_ops.push(IndexOp::HashAdd {
                        field_id,
                        value_hash: hash32(val_bytes),
                        doc_id: self.doc_internal_id,
                    });
                }
            }
        }
    }
}
```

### 6.3 Write Amplification Analysis

| Document Shape | Fields | Shard Writes | Breakdown |
|---|---|---|---|
| Flat, 5 fields, 2 indexed | 5 | 3 | 1 packed doc + 2 index buckets |
| Flat, 20 fields, 5 indexed | 20 | 6 | 1 packed doc + 5 index buckets |
| Nested 2 levels, 10 leaf fields, 3 indexed | 10 | 4 | 1 packed doc + 3 index buckets |
| Mixed, 30 fields + 2 arrays, 8 indexed | 34 | 9 | 1 packed doc + 8 index buckets |

Target model: each shard write is a hash table insert with bounded serialization overhead. With 8 workers, a 6-write document can fan out across workers in parallel in the low-microsecond range.

**Target envelope (validation pending):**

| Operation | Kōra Doc | Redis (JSON module) | MongoDB | Postgres JSONB |
|---|---|---|---|---|
| Write 20-field doc | target ~800ns (embedded) | target ~2μs | ~50-200μs | ~100-500μs |
| Read single field | target ~50ns (embedded) | target ~500ns (JSONPath) | ~50-200μs | ~50-200μs |
| Find by indexed field | target ~100ns | N/A (manual) | ~1-10μs | ~5-50μs |

---

## 7. Read Path

Point reads and indexed finds resolve to a small number of key/index lookups. There is no general query planner; decode and set operations are bounded by result size.

```
┌──────────────────────────────────────────────────────────────────────┐
│                           READ PATH                                  │
│                                                                      │
│  DOC.GET users doc:1 city                                            │
│    → resolve IDs: users→col_id, doc:1→internal_id, city→field_id     │
│    → shard lookup: [0x01][col_id][internal_id]               ~30ns   │
│    → offset table binary search for field_id                  ~10ns  │
│    → slice data region → decode value                          ~5ns  │
│    → dict lookup if DictRef                                    ~5ns  │
│    Total:                                             target ~50ns   │
│                                                                      │
│  DOC.GET users doc:1                                                 │
│    → shard lookup: get packed doc                             ~30ns  │
│    → sequential scan offset table + data region               ~20ns  │
│    → recompose JSON (resolve dict refs, rebuild nesting)     ~100ns  │
│    Total:                                                   ~150ns   │
│                                                                      │
│  DOC.FIND users WHERE city = "Accra"                                 │
│    → resolve: city→field_id, "Accra"→value_hash                      │
│    → shard lookup: [0x10][col][field][vhash]                  ~30ns  │
│    → decode delta-compressed doc_id list                      ~50ns  │
│    → resolve internal IDs to external doc_ids                 ~20ns  │
│    Total (IDs only):                                  target ~100ns  │
│                                                                      │
│  DOC.FIND users WHERE city = "Accra" AND age >= 25 AND age <= 35     │
│    → hash bucket lookup for city=Accra                        ~80ns  │
│    → sorted range scan for age [25,35]                       ~100ns  │
│    → intersect two sorted doc_id arrays                       ~50ns  │
│    Total (IDs only):                                        ~230ns   │
│                                                                      │
│  DOC.FIND users WHERE city = "Accra" PROJECT name, age               │
│    → index lookup → get doc_ids                               ~80ns  │
│    → for each doc: read packed doc → extract 2 fields         ~60ns  │
│    Total per result:                                        ~140ns   │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

### Recomposer

Reconstructs a JSON document from a packed buffer.

```rust
struct Recomposer;

impl Recomposer {
    /// Full document reconstruction from packed format
    fn recompose(
        packed: &PackedDoc,
        registry: &IdRegistry,
        dict: &ValueDictionary,
        collection_id: u16,
    ) -> serde_json::Value {
        let mut root = serde_json::Map::new();

        for (field_id, field_value) in packed.iter_fields() {
            let path = registry.field_path(collection_id, field_id);
            let json_value = field_value.to_json(dict);
            insert_at_path(&mut root, &path, json_value);
        }

        Value::Object(root)
    }

    /// Partial projection — only decode requested fields
    fn project(
        packed: &PackedDoc,
        fields: &[u16],
        registry: &IdRegistry,
        dict: &ValueDictionary,
        collection_id: u16,
    ) -> serde_json::Value {
        let mut root = serde_json::Map::new();

        for &field_id in fields {
            if let Some(field_value) = packed.read_field(field_id) {
                let path = registry.field_path(collection_id, field_id);
                let json_value = field_value.to_json(dict);
                insert_at_path(&mut root, &path, json_value);
            }
        }

        Value::Object(root)
    }
}

/// Insert a value at a dotted path into a JSON map
/// e.g., insert_at_path(root, "address.city", "Accra")
/// creates {"address": {"city": "Accra"}}
fn insert_at_path(root: &mut serde_json::Map<String, Value>, path: &str, value: Value) {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = root;

    for (i, part) in parts.iter().enumerate() {
        if i == parts.len() - 1 {
            current.insert(part.to_string(), value);
            return;
        }
        current = current
            .entry(part.to_string())
            .or_insert_with(|| Value::Object(serde_json::Map::new()))
            .as_object_mut()
            .unwrap();
    }
}
```

### Version Consistency

For unsplit documents (`[0x01][col][doc]` only), single-key replacement provides atomic document reads.
For split packs (`[0x01]` hot + `[0x06]` cold) and secondary indexes, consistency is enforced through a manifest epoch and WAL intent.

```rust
/// Per-document commit record, stored at key [0x07][col:2][doc:4]
struct DocManifest {
    epoch: u64,              // monotonically increasing commit epoch
    hot_version: u16,
    cold_version: Option<u16>,
    split: bool,
    index_epoch: u64,        // index state that corresponds to this doc epoch
}
```

**Write protocol (crash-safe):**
1. Build new hot/cold packs and index deltas for `epoch + 1`.
2. Append WAL intent: `{txid, col_id, doc_id, target_epoch}`.
3. Apply index bucket updates idempotently (tagged by `txid`/epoch).
4. Write doc pack(s) (`[0x01]`, optional `[0x06]`) tagged with target epoch.
5. Atomically publish manifest `[0x07]` to `target_epoch` (commit point).
6. Mark WAL intent complete and emit CDC event.

**Read protocol:**
1. Read manifest `[0x07]`.
2. Read pack keys referenced by manifest epoch.
3. If pack epoch mismatch is detected, retry from manifest (or serve previous committed epoch).

**Recovery:**
- On startup, replay incomplete WAL intents:
  - complete missing doc/index writes for a known `target_epoch`, or
  - roll forward to the last fully committed manifest epoch.
- All index operations are idempotent, so replay is safe.

---

## 8. Indexing

### 8.1 Index Types

```
┌─────────────────────────────────────────────────────────────────────┐
│                          INDEX TYPES                                │
│                                                                     │
│  HASH INDEX (equality)                                              │
│  Key: [0x10][col][field][value_hash]                                │
│  Value: delta-encoded sorted array of internal doc_ids              │
│  Ops: O(1) bucket lookup, O(log N) within bucket                    │
│  Use: WHERE city = "Accra"                                          │
│                                                                     │
│  SORTED INDEX (range)                                               │
│  Key: [0x11][col][field]                                            │
│  Value: score-grouped, delta-encoded doc_ids                        │
│  Ops: O(log G + K), where G = score groups, K = matched docs        │
│  Use: WHERE age >= 25 AND age <= 35                                 │
│                                                                     │
│  ARRAY INDEX (set membership)                                       │
│  Key: [0x12][col][field][element_hash]                              │
│  Value: delta-encoded sorted array of internal doc_ids              │
│  Ops: O(1) bucket lookup                                            │
│  Use: WHERE tags CONTAINS "rust"                                    │
│                                                                     │
│  UNIQUE INDEX (constraint + direct lookup)                          │
│  Key: [0x13][col][field][value_hash]                                │
│  Value: single internal doc_id (4 bytes)                            │
│  Ops: O(1) lookup, O(1) existence check on write                    │
│  Use: enforce uniqueness, direct doc lookup by field value           │
│                                                                     │
│  COMPOUND INDEX (multi-field)                                       │
│  Key: [0x14][col][f1][f2][combined_hash]                            │
│  Value: delta-encoded sorted array of internal doc_ids              │
│  Ops: combined equality + range in one lookup                       │
│  Use: WHERE city = "Accra" AND age >= 25                            │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 8.2 Compact Index Storage

Indexes reference documents by 4-byte internal IDs, stored in delta-encoded sorted arrays.

```rust
/// Hash index bucket — stored at key [0x10][col][field][value_hash]
struct HashIndexBucket {
    doc_ids: Vec<u32>,  // sorted, delta-encoded on disk
}

/// On-disk format:
/// [count:u32][delta-encoded doc_ids using varint]
///
/// Example: doc_ids = [1, 5, 8, 1000, 1003]
/// Deltas: [1, 4, 3, 992, 3]
/// Varint encoded: [0x01, 0x04, 0x03, 0x87 0x60, 0x03]
/// Total: 4 (count) + 5 (varint bytes) = 9 bytes for 5 doc references
/// Without delta encoding: 5 × 4 = 20 bytes → 55% savings
impl HashIndexBucket {
    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(self.doc_ids.len() as u32).to_le_bytes());

        let mut prev = 0u32;
        for &id in &self.doc_ids {
            let delta = id - prev;
            encode_varint(delta, &mut buf);
            prev = id;
        }
        buf
    }

    fn decode(bytes: &[u8]) -> Self {
        let count = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let mut doc_ids = Vec::with_capacity(count);
        let mut cursor = 4;
        let mut prev = 0u32;
        for _ in 0..count {
            let (delta, consumed) = decode_varint(&bytes[cursor..]);
            prev += delta;
            doc_ids.push(prev);
            cursor += consumed;
        }
        HashIndexBucket { doc_ids }
    }

    fn contains(&self, doc_id: u32) -> bool {
        self.doc_ids.binary_search(&doc_id).is_ok()
    }

    fn add(&mut self, doc_id: u32) {
        match self.doc_ids.binary_search(&doc_id) {
            Ok(_) => {}
            Err(pos) => self.doc_ids.insert(pos, doc_id),
        }
    }

    fn remove(&mut self, doc_id: u32) {
        if let Ok(pos) = self.doc_ids.binary_search(&doc_id) {
            self.doc_ids.remove(pos);
        }
    }
}

/// Sorted index — stored at key [0x11][col][field]
/// Score-grouped, delta-encoded doc_ids
struct SortedIndex {
    groups: Vec<ScoreGroup>,
}

struct ScoreGroup {
    score: f64,
    doc_ids: Vec<u32>,  // sorted, delta-encoded on disk
}

impl SortedIndex {
    fn range_query(&self, min: f64, max: f64) -> Vec<u32> {
        let start = self.groups.partition_point(|g| g.score < min);
        let end = self.groups.partition_point(|g| g.score <= max);
        self.groups[start..end]
            .iter()
            .flat_map(|g| g.doc_ids.iter().copied())
            .collect()
    }
}
```

### 8.3 Index Size Analysis

For 1M documents:

```
"city" field (hash, 500 unique values):
  500 buckets × (10B key + ~3KB delta-encoded payload) ≈ 1.5MB

"age" field (sorted, 62 score groups):
  62 groups × (8B score + ~32KB delta-encoded doc_ids) ≈ 2MB

"tags" field (array, 1000 unique tags):
  1000 buckets × (10B key + ~3KB payload) ≈ 3MB

Total index storage for 5 indexed fields: ~12.5MB
```

### 8.4 Index Maintenance

Indexes are maintained **synchronously on the write path**. When `DOC.SET` returns, committed manifest epoch and index epoch are aligned. Crash recovery replays idempotent index ops from WAL intents.

```rust
struct IndexManager {
    collection_id: u16,
    config: IndexConfig,
}

impl IndexManager {
    fn index_insert(
        &self,
        doc_internal_id: u32,
        packed: &PackedDoc,
        registry: &IdRegistry,
    ) -> Vec<IndexOp> {
        let mut ops = Vec::new();

        for (field_id, value) in packed.iter_fields() {
            if let Some(index_type) = self.config.index_for_field(field_id) {
                match index_type {
                    IndexType::Hash => {
                        ops.push(IndexOp::HashAdd {
                            field_id,
                            value_hash: hash32(&value.to_bytes()),
                            doc_id: doc_internal_id,
                        });
                    }
                    IndexType::Sorted => {
                        if let Some(score) = value.as_f64() {
                            ops.push(IndexOp::SortedAdd {
                                field_id,
                                doc_id: doc_internal_id,
                                score,
                            });
                        }
                    }
                    IndexType::Array => {
                        for elem in value.array_elements() {
                            ops.push(IndexOp::ArrayAdd {
                                field_id,
                                value_hash: hash32(&elem.to_bytes()),
                                doc_id: doc_internal_id,
                            });
                        }
                    }
                    IndexType::Unique => {
                        ops.push(IndexOp::UniqueSet {
                            field_id,
                            value_hash: hash32(&value.to_bytes()),
                            doc_id: doc_internal_id,
                        });
                    }
                    _ => {}
                }
            }
        }
        ops
    }

    fn index_update(
        &self,
        doc_internal_id: u32,
        old_packed: &PackedDoc,
        new_packed: &PackedDoc,
    ) -> Vec<IndexOp> {
        let mut ops = Vec::new();

        for (field_id, new_value) in new_packed.iter_fields() {
            if let Some(index_type) = self.config.index_for_field(field_id) {
                let old_value = old_packed.read_field(field_id);

                match (old_value.as_ref(), &new_value) {
                    (Some(old), new) if old.to_bytes() == new.to_bytes() => {
                        // Unchanged — skip
                    }
                    (Some(old), new) => {
                        ops.extend(self.remove_from_index(field_id, index_type, doc_internal_id, old));
                        ops.extend(self.add_to_index(field_id, index_type, doc_internal_id, new));
                    }
                    (None, new) => {
                        ops.extend(self.add_to_index(field_id, index_type, doc_internal_id, new));
                    }
                }
            }
        }

        // Handle deleted fields
        for (field_id, old_value) in old_packed.iter_fields() {
            if new_packed.read_field(field_id).is_none() {
                if let Some(index_type) = self.config.index_for_field(field_id) {
                    ops.extend(self.remove_from_index(field_id, index_type, doc_internal_id, &old_value));
                }
            }
        }

        ops
    }
}
```

### 8.5 Compound Query Resolution

```rust
struct QueryExecutor;

impl QueryExecutor {
    async fn execute(
        engine: &ShardEngine,
        collection_id: u16,
        expr: &Expr,
    ) -> Result<Vec<u32>> {
        match expr {
            Expr::Eq(field_id, value) => {
                let key = make_hash_index_key(collection_id, *field_id, hash32(value));
                let bucket = engine.get(&key).await?;
                Ok(HashIndexBucket::decode(&bucket).doc_ids)
            }
            Expr::Range(field_id, min, max) => {
                let key = make_sorted_index_key(collection_id, *field_id);
                let index = engine.get(&key).await?;
                Ok(SortedIndex::decode(&index).range_query(*min, *max))
            }
            Expr::Contains(field_id, value) => {
                let key = make_array_index_key(collection_id, *field_id, hash32(value));
                let bucket = engine.get(&key).await?;
                Ok(HashIndexBucket::decode(&bucket).doc_ids)
            }
            Expr::And(left, right) => {
                let left_ids = Box::pin(Self::execute(engine, collection_id, left)).await?;
                let right_ids = Box::pin(Self::execute(engine, collection_id, right)).await?;
                Ok(intersect_sorted(&left_ids, &right_ids))
            }
            Expr::Or(left, right) => {
                let left_ids = Box::pin(Self::execute(engine, collection_id, left)).await?;
                let right_ids = Box::pin(Self::execute(engine, collection_id, right)).await?;
                Ok(union_sorted(&left_ids, &right_ids))
            }
        }
    }
}
```

---

## 9. Mutation Model

### 9.1 Full Replace

`DOC.SET` replaces the entire document:

1. Read existing packed doc (for index diff)
2. Build new PackedDoc from new JSON
3. Compute index deltas (diff old packed vs new packed)
4. Append WAL intent for target epoch
5. Apply idempotent index bucket updates + pack writes
6. Publish manifest epoch (commit point)

### 9.2 Partial Update

`DOC.UPDATE` modifies specific fields without touching unrelated data. Reads the existing packed doc, applies mutations, rebuilds the buffer.

```
DOC.UPDATE users doc:1 SET age 31
DOC.UPDATE users doc:1 SET address.city "London"
DOC.UPDATE users doc:1 DEL tags
DOC.UPDATE users doc:1 PUSH tags "go"
DOC.UPDATE users doc:1 INCR score 1.0
```

```rust
enum MutationOp {
    Set { path: String, value: Value },
    Del { path: String },
    Push { path: String, value: Value },
    Pull { path: String, value: Value },
    Incr { path: String, delta: f64 },
}

impl DocEngine {
    async fn partial_update(
        &self,
        collection_id: u16,
        doc_internal_id: u32,
        mutations: Vec<MutationOp>,
    ) -> Result<u16> {
        // 1. Read existing packed document
        let key = make_doc_key(collection_id, doc_internal_id);
        let existing = self.engine.get(&key).await?
            .ok_or(Error::DocNotFound)?;
        let old_packed = PackedDoc { data: existing };

        // 2. Build new packed doc with mutations applied
        let mut builder = PackedDocBuilder::from_existing(&old_packed, &self.registry);
        let mut index_deltas = Vec::new();

        for mutation in mutations {
            match mutation {
                MutationOp::Set { path, value } => {
                    let field_id = self.registry.field_id(collection_id, &path);
                    let old_value = old_packed.read_field(field_id);
                    builder.set_field(field_id, &value);

                    if let Some(index_type) = self.index_config.index_for_field(field_id) {
                        if let Some(ref old) = old_value {
                            index_deltas.extend(
                                self.index_mgr.remove_from_index(
                                    field_id, index_type, doc_internal_id, old
                                )
                            );
                        }
                        let new_encoded = FieldValue::from_json(&value);
                        index_deltas.extend(
                            self.index_mgr.add_to_index(
                                field_id, index_type, doc_internal_id, &new_encoded
                            )
                        );
                    }
                }
                MutationOp::Incr { path, delta } => {
                    let field_id = self.registry.field_id(collection_id, &path);
                    let old_val = old_packed.read_field(field_id)
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0);
                    let new_val = old_val + delta;
                    builder.set_field(field_id, &Value::from(new_val));

                    index_deltas.push(IndexOp::SortedUpdate {
                        field_id,
                        doc_id: doc_internal_id,
                        old_score: old_val,
                        new_score: new_val,
                    });
                }
                // PUSH, PULL, DEL ...
            }
        }

        // 3. Rebuild packed doc with incremented version
        let new_packed = builder.build(now_seconds());

        // 4. Commit flow:
        //    WAL intent -> idempotent index deltas -> pack write -> manifest publish
        self.append_wal_intent(collection_id, doc_internal_id, &index_deltas).await?;
        self.apply_index_deltas(&index_deltas).await?;
        self.engine.set(&key, &new_packed.data).await?;
        self.publish_manifest_epoch(collection_id, doc_internal_id).await?;

        Ok(new_packed.version())
    }
}
```

**Cost model:** Changing 1 field on a 20-field doc with that field indexed: 1 read + 2 writes (packed doc + 1 index bucket). Packed rebuild CPU cost is typically small relative to storage/index update cost and network overhead.

---

## 10. Tiered Storage

### 10.1 Hot/Cold Pack Splitting

The packed format stores all fields in one shard entry, which lives in one storage tier. To preserve per-field tiering, documents with divergent access patterns are split into a hot pack and a cold pack.

```
Key [0x01][col][doc] → Hot Pack  (RAM)    — frequently accessed fields
Key [0x06][col][doc] → Cold Pack (NVMe)   — rarely accessed fields
```

Most documents won't be split — only those where field access patterns clearly diverge. The decision is made by background thermal analysis, not on the write path.

```rust
struct PackSplitter;

impl PackSplitter {
    fn should_split(
        &self,
        collection_id: u16,
        packed: &PackedDoc,
        thermal: &ThermalTracker,
    ) -> Option<SplitPlan> {
        let mut hot_fields = Vec::new();
        let mut cold_fields = Vec::new();

        for (field_id, _) in packed.iter_fields() {
            let heat = thermal.field_heat(collection_id, field_id);
            if heat.reads_per_sec > 10.0 {
                hot_fields.push(field_id);
            } else if heat.reads_per_sec < 0.1
                && heat.last_read.elapsed() > Duration::from_secs(3600)
            {
                cold_fields.push(field_id);
            } else {
                hot_fields.push(field_id);
            }
        }

        // Only split if cold fields represent significant savings
        if cold_fields.len() >= 3
            && cold_fields.len() as f64 / packed.field_count() as f64 > 0.3
        {
            Some(SplitPlan { hot_fields, cold_fields })
        } else {
            None
        }
    }
}

impl PackedDoc {
    /// Read a field from a potentially split document
    async fn read_field_tiered(
        engine: &ShardEngine,
        collection_id: u16,
        doc_internal_id: u32,
        field_id: u16,
    ) -> Option<FieldValue> {
        // Try hot pack first (RAM, ~30ns)
        let hot_key = make_doc_key(collection_id, doc_internal_id);
        let hot_pack = PackedDoc { data: engine.get(&hot_key).await? };

        if let Some(value) = hot_pack.read_field(field_id) {
            return Some(value);
        }

        // Field not in hot pack — check cold pack (NVMe, ~1-5μs)
        let cold_key = make_cold_key(collection_id, doc_internal_id);
        if let Some(cold_data) = engine.get(&cold_key).await {
            let cold_pack = PackedDoc { data: cold_data };
            return cold_pack.read_field(field_id);
        }

        None
    }
}
```

### 10.2 Thermal Tracking

```rust
struct ThermalTracker {
    /// Per-field access frequency (exponential moving average)
    field_heat: DashMap<(u16, u16), FieldHeat>,  // (collection_id, field_id) → heat
}

struct FieldHeat {
    reads_per_sec: f64,     // EMA with α = 0.01
    last_read: Instant,
    total_reads: u64,
}

impl ThermalTracker {
    fn record_access(&self, collection_id: u16, field_id: u16) {
        self.field_heat
            .entry((collection_id, field_id))
            .and_modify(|h| {
                h.total_reads += 1;
                h.last_read = Instant::now();
            })
            .or_insert(FieldHeat {
                reads_per_sec: 1.0,
                last_read: Instant::now(),
                total_reads: 1,
            });
    }

    fn recommend_splits(&self, collection_id: u16) -> Vec<SplitRecommendation> {
        // Background task: identify documents with divergent field access patterns
        todo!()
    }
}
```

### 10.3 Tier Behavior

```
Data Type          Default Tier    Promotion Trigger       Demotion Trigger
──────────────────────────────────────────────────────────────────────────────
Index buckets      Hot (RAM)       Always hot              Never
Hot packs          Hot (RAM)       reads/sec > 10          Auto-split if fields diverge
Cold packs         Warm (NVMe)     Field promoted to hot   reads/sec < 0.1 for 1hr
Unsplit docs       Auto            reads/sec > 10 → RAM    reads/sec < 1 → NVMe
Value dictionary   Hot (RAM)       Always hot              Never
ID registry        Hot (RAM)       Always hot              Never
```

### 10.4 Capacity with Tiering

1M documents, 20 fields each, avg 200 bytes user data per doc:

```
Without tiering (all RAM):
  Packed docs:              ~284MB
  Indexes:                  ~12.5MB
  Registry + dictionary:    ~30MB
  Total RAM:                ~327MB

With hot/cold splitting (5 hot fields, 15 cold):
  Hot packs:                ~85MB   (RAM)
  Cold packs:               ~200MB  (NVMe at $0.08/GB)
  Indexes:                  ~12.5MB (RAM)
  Registry + dictionary:    ~30MB   (RAM)
  Total RAM:                ~128MB
  Total NVMe:               ~200MB
```

---

## 11. Compression Profiles

Different collections have different characteristics. Choose a profile per collection:

```rust
enum CompressionProfile {
    /// No compression. Fastest writes, fastest reads.
    /// Best for: small docs, latency-critical hot data
    None,

    /// Dictionary encoding only. Minimal write overhead.
    /// Best for: docs with low-cardinality string fields
    Dictionary,

    /// Dictionary + LZ4 on the data region of packed docs.
    /// Target: ~30% space savings with low read-side decompression overhead.
    /// Best for: larger docs, mixed hot/cold access
    Balanced,

    /// Dictionary + LZ4 + varint integers + shared offset tables.
    /// Target: ~50% space savings with higher decode cost than Balanced.
    /// Best for: archival, cold data, cost-sensitive deployments
    Compact,
}
```

---

## 12. Space Accounting

Modeled byte-level estimate for the benchmark shape: **1 million documents, 20 fields each, 5 indexed.** Actual values vary by field cardinality, array fanout, dictionary hit rate, and compression profile.

Field composition: 8 string fields (avg 10 chars, 3 low-cardinality), 5 number fields, 3 bool fields, 2 array fields (avg 5 elements each), 2 nested objects (4 leaf string fields counted above).

### Per-Document Cost (PackedDoc)

```
Header:                                        8 bytes
Offset table: 20 fields × 4 bytes          = 80 bytes
Type table:   20 fields × 1 byte           = 20 bytes
Data region:
  5 strings inline:  avg (2 + 10)           = 60 bytes
  3 strings dictref: 3 × 4                  = 12 bytes
  5 numbers: 5 × 8                          = 40 bytes
  3 bools:                                  = 0 bytes
  2 arrays: 2 × (2 + 5 × 6)                = 64 bytes
                                            ─────────
Per-document packed size:                     ~284 bytes
Shard key:                                      7 bytes
HashMap entry overhead:                       ~64 bytes
                                            ─────────
Per-document total:                           ~355 bytes
```

Note: this model assumes unsplit documents and excludes optional manifest/split-pack metadata overhead.

### Grand Total

```
Component              Size          % of total
───────────────────────────────────────────────
Packed documents       355MB         88.4%
Indexes                12.5MB        3.1%
ID Registry            30MB          7.5%
Value Dictionary       0.02MB        0.0%
Other overhead         4MB           1.0%
───────────────────────────────────────────────
TOTAL                  ~401MB

Raw user data:         ~200MB
Storage overhead:      2.0x raw data
```

### Comparison

```
Database              Storage / 1M docs   vs Raw    vs Postgres
──────────────────────────────────────────────────────────────
Raw JSON              200MB               1.0x      —
Kōra (Dictionary)     401MB               2.0x      0.80x ✓
Kōra (Balanced)       340MB               1.7x      0.68x ✓
Kōra (Compact)        280MB               1.4x      0.56x ✓
Postgres JSONB        500MB               2.5x      1.0x
MongoDB BSON          600MB               3.0x      1.2x
```

---

## 13. Command Surface

### 13.1 RESP Commands

```
# Collection management
DOC.CREATE <collection> [SCHEMA <json>] [COMPRESSION none|dictionary|balanced|compact]
DOC.DROP <collection>
DOC.INFO <collection>

# Document CRUD
DOC.SET <collection> <doc_id> <json>
DOC.GET <collection> <doc_id> [FIELDS <field1> <field2> ...]
DOC.DEL <collection> <doc_id>
DOC.EXISTS <collection> <doc_id>

# Partial mutations
DOC.UPDATE <collection> <doc_id> SET <path> <value> [SET <path> <value> ...]
DOC.UPDATE <collection> <doc_id> DEL <path> [DEL <path> ...]
DOC.UPDATE <collection> <doc_id> INCR <path> <delta>
DOC.UPDATE <collection> <doc_id> PUSH <path> <value>
DOC.UPDATE <collection> <doc_id> PULL <path> <value>

# Queries
DOC.FIND <collection> WHERE <expr> [PROJECT <f1> <f2> ...] [LIMIT <n>] [OFFSET <n>]
DOC.COUNT <collection> WHERE <expr>

# Index management
DOC.CREATEINDEX <collection> <field> <type:hash|sorted|text|array|unique>
DOC.DROPINDEX <collection> <field>
DOC.INDEXES <collection>

# Batch
DOC.MSET <collection> <id1> <json1> [<id2> <json2> ...]
DOC.MGET <collection> <id1> [<id2> ...]

# Observability
DOC.STORAGE <collection>      → per-field size breakdown, compression ratio
DOC.THERMAL <collection>      → hot/warm/cold field breakdown
DOC.DICTINFO <collection>     → dictionary stats, cardinality per field

# CDC
DOC.SUBSCRIBE <collection> [doc_id] [WHERE <expr>]
```

### 13.2 WHERE Expression Syntax

Minimal, unambiguous, fast to parse:

```
# Equality
WHERE city = "Accra"
WHERE active = true

# Range (numeric fields)
WHERE age > 25
WHERE age >= 25 AND age <= 35

# Array containment
WHERE tags CONTAINS "rust"

# Compound
WHERE city = "Accra" AND age >= 25
WHERE city = "Accra" OR city = "Lagos"
WHERE city = "Accra" AND tags CONTAINS "rust" AND age >= 25

# Nested field access
WHERE address.city = "Accra"
WHERE metadata.score >= 0.8
```

```rust
enum Expr {
    Eq(u16, Value),       // field_id, value
    Neq(u16, Value),
    Gt(u16, f64),
    Gte(u16, f64),
    Lt(u16, f64),
    Lte(u16, f64),
    Contains(u16, Value), // array membership
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
}
```

---

## 14. Embedded API

```rust
use kora_embedded::Database;
use kora_doc::{DocEngine, CompressionProfile, Mutation, IndexType};

let db = Database::open(Config::default())?;
let docs = db.doc_engine();

// Create collection with compression
docs.create_collection("users", CollectionConfig {
    compression: CompressionProfile::Dictionary,
    ..Default::default()
})?;

// Create indexes
docs.create_index("users", "city", IndexType::Hash)?;
docs.create_index("users", "age", IndexType::Sorted)?;
docs.create_index("users", "tags", IndexType::Array)?;
docs.create_index("users", "email", IndexType::Unique)?;

// Insert — target ~800ns embedded, all indexes pre-built
docs.set("users", "doc:1", json!({
    "name": "Augustus",
    "city": "Accra",
    "age": 30,
    "email": "aug@example.com",
    "tags": ["rust", "systems"],
    "address": { "street": "Ring Rd", "zip": "00233" }
}))?;

// Single field read — target ~50ns
let city: String = docs.get_field("users", "doc:1", "city")?;

// Full document — target ~150ns (single shard lookup + sequential scan)
let user = docs.get("users", "doc:1", None)?;

// Projection — target ~60ns per field
let partial = docs.get("users", "doc:1", Some(&["name", "age"]))?;

// Query by index — target ~100ns
let accra_users = docs.find("users", "city = 'Accra'")?;

// Compound query — target ~230ns
let filtered = docs.find("users", "city = 'Accra' AND age >= 25 AND age <= 35")?;

// Surgical partial update — target ~200ns
docs.update("users", "doc:1", vec![
    Mutation::set("age", json!(31)),
    Mutation::push("tags", json!("go")),
])?;

// Atomic increment — target ~200ns
docs.update("users", "doc:1", vec![
    Mutation::incr("score", 1.0),
])?;

// Storage stats
let stats = docs.storage_info("users")?;
// StorageInfo {
//   doc_count: 1_000_000,
//   raw_data_bytes: 200_000_000,
//   packed_bytes: 284_000_000,
//   index_bytes: 12_500_000,
//   registry_bytes: 30_000_000,
//   compression_ratio: 0.51,
//   avg_doc_bytes: 284,
// }
```

---

## 15. CDC Integration

Document mutations emit CDC events through `kora-cdc`, enabling real-time subscriptions.

```rust
struct DocCdcEvent {
    collection_id: u16,
    doc_internal_id: u32,
    version: u16,
    op: DocCdcOp,
    timestamp: u64,
}

enum DocCdcOp {
    Insert {
        fields: Vec<(u16, FieldValue)>,
    },
    Update {
        changed_fields: Vec<(u16, Option<FieldValue>, FieldValue)>, // (field_id, old, new)
    },
    Delete,
}
```

**Subscription:**
```
DOC.SUBSCRIBE users                          # all changes in collection
DOC.SUBSCRIBE users doc:1                    # changes to specific document
DOC.SUBSCRIBE users WHERE city = "Accra"     # filtered by index
```

Use cases: real-time UI updates, materialized views, cross-service sync, cache invalidation, event sourcing.

---

## 16. What We Deliberately Don't Support

| Feature | Why Not |
|---|---|
| Joins | Cross-document joins require scanning. Denormalize at write time instead. |
| Full SQL | SQL parsing + planning adds microseconds per query. Our WHERE syntax covers 90% of real-time app needs. |
| Multi-document transactions | Would require distributed locking across shards. Single-document atomicity covers most use cases. |
| Aggregation pipeline | Compute at write time via CDC + materialized views, not at read time. |
| Schema migrations | Schemaless by default. New fields appear on write. Old documents return null for new fields. |
| Cursor-based iteration | Initial API uses LIMIT/OFFSET. Cursor pagination may be added when deep pagination becomes a bottleneck. |

---

## 17. Performance Targets

The numbers below are target envelopes, not final measured guarantees. They must be validated with Phase 7 benchmarks under documented hardware/workload conditions (CPU model, storage class, dataset shape, cache warmup, and concurrency level).

### Latency

| Operation | Embedded | Network (RESP) | Postgres |
|---|---|---|---|
| Single field read | ~50ns | 5-10μs | 50-200μs |
| Full doc read (20 fields) | ~150ns | 8-15μs | 50-200μs |
| Indexed find (single condition) | ~100ns | 5-10μs | 5-50μs |
| Compound find (2 conditions) | ~230ns | 8-15μs | 10-100μs |
| Projected find (3 fields) | ~140ns | 8-15μs | 50-200μs |
| Full doc write (20 fields, 5 indexed) | ~800ns | 10-20μs | 100-500μs |
| Partial update (1 field) | ~200ns | 5-10μs | 50-200μs |

### Throughput (8-core machine)

| Workload | Embedded QPS | Network QPS |
|---|---|---|
| Read-heavy (95% reads) | 5-10M | 500K-1M |
| Mixed (70/30 read/write) | 2-5M | 200-500K |
| Write-heavy (30/70 read/write) | 1-2M | 100-200K |

### Storage

| Configuration | Per 1M docs (20 fields, 5 indexed) | vs Postgres |
|---|---|---|
| Dictionary compression | ~401MB | 20% smaller |
| Balanced (dict + LZ4) | ~340MB | 32% smaller |
| Compact (dict + LZ4 + varint) | ~280MB | 44% smaller |

---

## 18. Implementation Roadmap

### Phase 1 — Foundation (Weeks 1-2)
- [x] `kora-doc` crate scaffold
- [x] ID Registry (collection IDs, field IDs, doc internal IDs)
- [x] Value Dictionary with cardinality tracking (current implementation uses shard-local cardinality sets)
- [x] PackedDoc format: builder, reader, field access by ID
- [x] Binary key encoding (7-10 byte compact keys)

### Phase 2 — Core CRUD (Weeks 3-4)
- [x] Collection management (DOC.CREATE, DOC.DROP, DOC.INFO)
- [x] JSON decomposer → PackedDoc builder pipeline
- [x] Document write path (DOC.SET)
- [x] Document read path: full, projected, single field (DOC.GET)
- [x] Document delete (DOC.DEL)
- [x] RESP command handlers

### Phase 3 — Indexing (Weeks 5-6)
- [x] Hash index buckets with delta encoding
- [x] Sorted index with score-grouped storage
- [x] Array membership index
- [x] Unique index with constraint enforcement
- [x] Index maintenance on write path (insert/update/delete)
- [x] Query executor: single condition, compound AND/OR
- [x] DOC.FIND, DOC.COUNT commands

### Phase 4 — Mutations + Batch (Week 7)
- [x] Partial update engine (SET, DEL, INCR, PUSH, PULL)
- [x] PackedDoc rebuild from existing + mutations
- [x] Batch operations (DOC.MSET, DOC.MGET)

### Phase 5 — Compression + Tiering (Weeks 8-9)
- [x] Compression profiles (None, Dictionary, Balanced, Compact)
- [ ] LZ4 integration for data region compression
- [ ] Thermal tracker with per-field heat measurement
- [ ] Hot/cold pack splitting
- [x] DOC.STORAGE command
- [ ] DOC.THERMAL command
- [x] DOC.DICTINFO command

### Phase 6 — CDC + API + Polish (Weeks 10-11)
- [ ] CDC event emission for document mutations
- [ ] DOC.SUBSCRIBE command
- [x] Ergonomic embedded Rust API
- [ ] Schema validation (optional)
- [ ] Compound indexes

### Phase 7 — Benchmarks + Launch (Week 12)
- [ ] Benchmarks vs Redis JSON, MongoDB, Postgres JSONB, DragonflyDB
- [ ] Storage efficiency benchmarks
- [ ] Documentation and README

---

## Appendix A: Key Encoding Reference

```
Tag  Format                              Size   Description
──────────────────────────────────────────────────────────────
0x01 [col:2][doc:4]                      7B     packed document
0x02 [col:2]                             3B     collection metadata
0x03 [col:2]                             3B     schema
0x04 [col:2]                             3B     value dictionary
0x05 [col:2]                             3B     ID registry segment
0x06 [col:2][doc:4]                      7B     cold pack (split doc)
0x10 [col:2][field:2][vhash:4]           10B    hash index bucket
0x11 [col:2][field:2]                    6B     sorted index
0x12 [col:2][field:2][vhash:4]           10B    array index bucket
0x13 [col:2][field:2][vhash:4]           10B    unique index
0x14 [col:2][f1:2][f2:2][vhash:4]       12B    compound index
0x20 [col:2]                             3B     CDC stream head
0x21 [col:2][seq:8]                      11B    CDC event
```

## Appendix B: Target Latency/Storage Envelope

```
                   READ LATENCY — SINGLE FIELD, HOT PATH

  Postgres    ┤████████████████████████████████████████████████│ 50-200μs
              │ parse │ plan │ latch │ buffer │ MVCC │ deser  │

  MongoDB     ┤██████████████████████████████████████████│      50-150μs
              │ parse │ plan │ lock │ BSON walk │ deser │

  Redis JSON  ┤█████████│                                       2-5μs
              │ RESP │ JSONPath traverse │

  Kōra        ┤█│                                        target ~50ns
  (embedded)  │ shard lookup │ offset scan │
              ╰─no parse, no plan, no lock, no decode──╯


                   STORAGE — 1M DOCUMENTS, 20 FIELDS, 5 INDEXED

  Kōra Compact ┤██████████████│                                 280MB
  Kōra Balanced┤█████████████████│                              340MB
  Kōra Dict    ┤████████████████████│                           401MB
  Postgres     ┤█████████████████████████│                      500MB
  MongoDB      ┤██████████████████████████████│                 600MB
```
