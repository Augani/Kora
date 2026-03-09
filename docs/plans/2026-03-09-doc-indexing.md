# kora-doc Phase 3: Indexing Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add secondary indexing, query execution, and DOC.FIND/DOC.COUNT to the kora-doc document layer.

**Architecture:** Index data structures (hash/sorted/array/unique buckets) live in `CollectionState` alongside docs. Indexes are maintained synchronously on every write/update/delete. A WHERE expression parser converts query strings into an `Expr` AST that the query executor resolves via index lookups and set operations. New RESP commands (DOC.CREATEINDEX, DOC.DROPINDEX, DOC.INDEXES, DOC.FIND, DOC.COUNT) are wired through kora-core Command enum → kora-protocol parser → kora-server handler.

**Tech Stack:** Rust, serde_json, thiserror. No new dependencies.

---

### Task 1: Index Configuration and Data Structures

**Files:**
- Create: `kora-doc/src/index.rs`
- Modify: `kora-doc/src/lib.rs`
- Modify: `kora-doc/src/engine.rs`

**Goal:** Define `IndexType`, `IndexConfig`, and in-memory index bucket structures.

**Step 1: Create `kora-doc/src/index.rs` with index types and bucket structures**

```rust
// kora-doc/src/index.rs

use std::collections::{BTreeMap, HashMap};

use thiserror::Error;

use crate::registry::{DocId, FieldId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IndexType {
    Hash,
    Sorted,
    Array,
    Unique,
}

#[derive(Debug, Error)]
pub enum IndexError {
    #[error("index already exists on field '{0}'")]
    AlreadyExists(String),
    #[error("no index on field '{0}'")]
    NotFound(String),
    #[error("unique constraint violation on field '{field_path}': value already exists")]
    UniqueViolation { field_path: String },
}

#[derive(Debug, Default)]
pub struct IndexConfig {
    // field_id → IndexType
    indexes: HashMap<FieldId, IndexType>,
    // field path → field_id for reverse lookup
    paths: HashMap<String, FieldId>,
}

impl IndexConfig {
    pub fn add(&mut self, field_id: FieldId, path: String, index_type: IndexType) -> Result<(), IndexError> {
        if self.indexes.contains_key(&field_id) {
            return Err(IndexError::AlreadyExists(path));
        }
        self.indexes.insert(field_id, index_type);
        self.paths.insert(path, field_id);
        Ok(())
    }

    pub fn remove(&mut self, path: &str) -> Result<(FieldId, IndexType), IndexError> {
        let field_id = self.paths.remove(path)
            .ok_or_else(|| IndexError::NotFound(path.to_string()))?;
        let index_type = self.indexes.remove(&field_id)
            .ok_or_else(|| IndexError::NotFound(path.to_string()))?;
        Ok((field_id, index_type))
    }

    pub fn index_for_field(&self, field_id: FieldId) -> Option<IndexType> {
        self.indexes.get(&field_id).copied()
    }

    pub fn is_empty(&self) -> bool {
        self.indexes.is_empty()
    }

    pub fn entries(&self) -> Vec<(String, FieldId, IndexType)> {
        let mut result: Vec<_> = self.paths.iter()
            .filter_map(|(path, &field_id)| {
                self.indexes.get(&field_id).map(|&idx_type| {
                    (path.clone(), field_id, idx_type)
                })
            })
            .collect();
        result.sort_by(|a, b| a.0.cmp(&b.0));
        result
    }
}

// Hash index: value_hash → sorted vec of doc_ids
#[derive(Debug, Default)]
pub struct HashIndex {
    buckets: HashMap<u32, Vec<DocId>>,
}

impl HashIndex {
    pub fn add(&mut self, value_hash: u32, doc_id: DocId) {
        let bucket = self.buckets.entry(value_hash).or_default();
        if let Err(pos) = bucket.binary_search(&doc_id) {
            bucket.insert(pos, doc_id);
        }
    }

    pub fn remove(&mut self, value_hash: u32, doc_id: DocId) {
        if let Some(bucket) = self.buckets.get_mut(&value_hash) {
            if let Ok(pos) = bucket.binary_search(&doc_id) {
                bucket.remove(pos);
            }
            if bucket.is_empty() {
                self.buckets.remove(&value_hash);
            }
        }
    }

    pub fn lookup(&self, value_hash: u32) -> &[DocId] {
        self.buckets.get(&value_hash).map_or(&[], Vec::as_slice)
    }

    pub fn clear(&mut self) {
        self.buckets.clear();
    }
}

// Sorted index: score → sorted vec of doc_ids, using BTreeMap for range queries
#[derive(Debug, Default)]
pub struct SortedIndex {
    // Use ordered bits for f64 to get correct BTreeMap ordering
    groups: BTreeMap<OrderedF64, Vec<DocId>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OrderedF64(u64);

impl OrderedF64 {
    pub fn new(val: f64) -> Self {
        let bits = val.to_bits();
        // flip sign bit, and if negative, flip all bits for correct ordering
        let ordered = if bits >> 63 == 1 { !bits } else { bits ^ (1 << 63) };
        Self(ordered)
    }

    pub fn value(self) -> f64 {
        let bits = if self.0 >> 63 == 0 { !self.0 } else { self.0 ^ (1 << 63) };
        f64::from_bits(bits)
    }
}

impl SortedIndex {
    pub fn add(&mut self, score: f64, doc_id: DocId) {
        let key = OrderedF64::new(score);
        let bucket = self.groups.entry(key).or_default();
        if let Err(pos) = bucket.binary_search(&doc_id) {
            bucket.insert(pos, doc_id);
        }
    }

    pub fn remove(&mut self, score: f64, doc_id: DocId) {
        let key = OrderedF64::new(score);
        if let Some(bucket) = self.groups.get_mut(&key) {
            if let Ok(pos) = bucket.binary_search(&doc_id) {
                bucket.remove(pos);
            }
            if bucket.is_empty() {
                self.groups.remove(&key);
            }
        }
    }

    pub fn range_query(&self, min: f64, max: f64) -> Vec<DocId> {
        let min_key = OrderedF64::new(min);
        let max_key = OrderedF64::new(max);
        self.groups.range(min_key..=max_key)
            .flat_map(|(_, ids)| ids.iter().copied())
            .collect()
    }

    pub fn clear(&mut self) {
        self.groups.clear();
    }
}

// Unique index: value_hash → single doc_id
#[derive(Debug, Default)]
pub struct UniqueIndex {
    entries: HashMap<u32, DocId>,
}

impl UniqueIndex {
    pub fn add(&mut self, value_hash: u32, doc_id: DocId, field_path: &str) -> Result<(), IndexError> {
        if let Some(&existing) = self.entries.get(&value_hash) {
            if existing != doc_id {
                return Err(IndexError::UniqueViolation {
                    field_path: field_path.to_string(),
                });
            }
        }
        self.entries.insert(value_hash, doc_id);
        Ok(())
    }

    pub fn remove(&mut self, value_hash: u32, doc_id: DocId) {
        if let Some(&existing) = self.entries.get(&value_hash) {
            if existing == doc_id {
                self.entries.remove(&value_hash);
            }
        }
    }

    pub fn lookup(&self, value_hash: u32) -> Option<DocId> {
        self.entries.get(&value_hash).copied()
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

// Per-collection index store holding all indexes for all fields
#[derive(Debug, Default)]
pub struct CollectionIndexes {
    hash_indexes: HashMap<FieldId, HashIndex>,
    sorted_indexes: HashMap<FieldId, SortedIndex>,
    array_indexes: HashMap<FieldId, HashIndex>, // same structure as hash
    unique_indexes: HashMap<FieldId, UniqueIndex>,
}

impl CollectionIndexes {
    pub fn get_or_create_hash(&mut self, field_id: FieldId) -> &mut HashIndex {
        self.hash_indexes.entry(field_id).or_default()
    }

    pub fn get_or_create_sorted(&mut self, field_id: FieldId) -> &mut SortedIndex {
        self.sorted_indexes.entry(field_id).or_default()
    }

    pub fn get_or_create_array(&mut self, field_id: FieldId) -> &mut HashIndex {
        self.array_indexes.entry(field_id).or_default()
    }

    pub fn get_or_create_unique(&mut self, field_id: FieldId) -> &mut UniqueIndex {
        self.unique_indexes.entry(field_id).or_default()
    }

    pub fn hash(&self, field_id: FieldId) -> Option<&HashIndex> {
        self.hash_indexes.get(&field_id)
    }

    pub fn sorted(&self, field_id: FieldId) -> Option<&SortedIndex> {
        self.sorted_indexes.get(&field_id)
    }

    pub fn array(&self, field_id: FieldId) -> Option<&HashIndex> {
        self.array_indexes.get(&field_id)
    }

    pub fn unique(&self, field_id: FieldId) -> Option<&UniqueIndex> {
        self.unique_indexes.get(&field_id)
    }

    pub fn remove_field(&mut self, field_id: FieldId) {
        self.hash_indexes.remove(&field_id);
        self.sorted_indexes.remove(&field_id);
        self.array_indexes.remove(&field_id);
        self.unique_indexes.remove(&field_id);
    }
}

/// Compute a 32-bit hash for index bucket keys.
pub fn hash32(data: &[u8]) -> u32 {
    let mut h: u32 = 0x811c_9dc5;
    for &byte in data {
        h ^= byte as u32;
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}
```

**Step 2: Register the module and add IndexConfig/CollectionIndexes to CollectionState**

In `kora-doc/src/lib.rs`, add `pub mod index;` and re-export key types.

In `kora-doc/src/engine.rs`:
- Add `index_config: IndexConfig` and `indexes: CollectionIndexes` to `CollectionState`
- Initialize them in `create_collection()`
- Clear them in `drop_collection()`

**Step 3: Write tests for index data structures**

Add tests in `kora-doc/src/index.rs` covering:
- HashIndex add/remove/lookup
- SortedIndex add/remove/range_query
- OrderedF64 ordering correctness (negative, zero, positive, NaN)
- UniqueIndex add/remove/violation
- IndexConfig add/remove/entries

**Step 4: Run tests**

```bash
cargo test -p kora-doc
```

**Step 5: Commit**

```bash
git add kora-doc/src/index.rs kora-doc/src/lib.rs kora-doc/src/engine.rs
git commit -m "feat(kora-doc): add index data structures and configuration"
```

---

### Task 2: Index Maintenance on Write Path

**Files:**
- Modify: `kora-doc/src/engine.rs`
- Modify: `kora-doc/src/index.rs`

**Goal:** When `set()`, `update()`, or `del()` is called, maintain all configured indexes.

**Step 1: Add helper to extract indexable values from a JSON document**

In `engine.rs`, add a method that walks a JSON Value and extracts `(field_path, indexable_value)` pairs for all indexed fields. The indexable value is either:
- String → hash32 of UTF-8 bytes (for Hash/Unique indexes)
- Number → f64 score (for Sorted indexes)
- Array → each string element's hash32 (for Array indexes)
- Bool → hash32 of "true"/"false" bytes (for Hash indexes)

**Step 2: Update `set()` to maintain indexes**

After storing the packed doc:
1. If this is an update (not created), remove old index entries by reading the old packed doc's values
2. Add new index entries from the new JSON value
3. For Unique indexes, check constraint BEFORE storing the doc

**Step 3: Update `del()` to remove index entries**

Before removing the packed doc, read the document JSON and remove all index entries.

**Step 4: Update `update()` to maintain indexes**

The current `update()` calls `get()` then `set()`. Since `set()` now handles index maintenance, this should work. But verify the old→new diff path is correct.

**Step 5: Add `create_index` and `drop_index` methods to DocEngine**

```rust
pub fn create_index(&mut self, collection: &str, field_path: &str, index_type: IndexType) -> Result<(), DocError>
pub fn drop_index(&mut self, collection: &str, field_path: &str) -> Result<(), DocError>
pub fn indexes(&self, collection: &str) -> Result<Vec<(String, IndexType)>, DocError>
```

`create_index` must backfill: iterate all existing docs and add their values to the new index.

**Step 6: Write tests**

- Insert docs, create index, verify index lookups work
- Create index before inserting, verify index is maintained on set
- Update a doc, verify old value removed from index and new value added
- Delete a doc, verify index entries removed
- Unique constraint violation on insert
- Drop index clears data

**Step 7: Run tests and commit**

```bash
cargo test -p kora-doc
git add kora-doc/src/engine.rs kora-doc/src/index.rs
git commit -m "feat(kora-doc): maintain indexes on write/update/delete"
```

---

### Task 3: WHERE Expression Parser

**Files:**
- Create: `kora-doc/src/expr.rs`
- Modify: `kora-doc/src/lib.rs`

**Goal:** Parse WHERE clause strings into an `Expr` AST.

**Syntax to support:**
```
field = "value"         → Eq
field != "value"        → Neq
field > 25              → Gt
field >= 25             → Gte
field < 35              → Lt
field <= 35             → Lte
field CONTAINS "value"  → Contains (array membership)
expr AND expr           → And
expr OR expr            → Or
```

Field names support dotted paths: `address.city`.
String values are single or double quoted.
Numeric values are bare integers or floats.
Boolean values are bare `true`/`false`.
`AND` binds tighter than `OR`.

**Step 1: Define the Expr AST**

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Eq(String, ExprValue),
    Neq(String, ExprValue),
    Gt(String, f64),
    Gte(String, f64),
    Lt(String, f64),
    Lte(String, f64),
    Contains(String, ExprValue),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExprValue {
    String(String),
    Number(f64),
    Bool(bool),
    Null,
}
```

**Step 2: Implement a recursive-descent parser**

Tokenize → parse OR-level → parse AND-level → parse comparison.

**Step 3: Write comprehensive parser tests**

- Simple equality: `city = "Accra"`
- Range: `age >= 25 AND age <= 35`
- Array containment: `tags CONTAINS "rust"`
- Compound: `city = "Accra" AND age >= 25`
- OR: `city = "Accra" OR city = "Lagos"`
- Nested dotted path: `address.city = "Accra"`
- Boolean: `active = true`
- Mixed AND/OR precedence: `a = "x" OR b = "y" AND c = "z"` → `a = "x" OR (b = "y" AND c = "z")`
- Error cases: malformed, missing quotes, incomplete expression

**Step 4: Run tests and commit**

```bash
cargo test -p kora-doc
git add kora-doc/src/expr.rs kora-doc/src/lib.rs
git commit -m "feat(kora-doc): add WHERE expression parser"
```

---

### Task 4: Query Executor (DOC.FIND / DOC.COUNT)

**Files:**
- Modify: `kora-doc/src/engine.rs`
- Modify: `kora-doc/src/index.rs`

**Goal:** Execute parsed `Expr` against collection indexes and return matching doc IDs.

**Step 1: Add query execution to DocEngine**

```rust
pub fn find(
    &self,
    collection: &str,
    where_clause: &str,
    projection: Option<&[&str]>,
    limit: Option<usize>,
    offset: usize,
) -> Result<Vec<Value>, DocError>

pub fn count(
    &self,
    collection: &str,
    where_clause: &str,
) -> Result<u64, DocError>
```

The flow:
1. Parse the WHERE clause into an `Expr`
2. Resolve field paths to field_ids
3. Execute the Expr recursively:
   - `Eq` → hash index lookup with `hash32(value_bytes)`
   - `Neq` → scan all hash buckets excluding the matching one (fallback to full scan if no index)
   - `Gt/Gte/Lt/Lte` → sorted index range query
   - `Contains` → array index lookup with `hash32(element_bytes)`
   - `And` → intersect two sorted doc_id arrays
   - `Or` → union two sorted doc_id arrays
4. Apply OFFSET/LIMIT
5. For each result doc_id, fetch and optionally project the document

**Step 2: Add sorted set intersection and union helpers to `index.rs`**

```rust
pub fn intersect_sorted(a: &[DocId], b: &[DocId]) -> Vec<DocId>
pub fn union_sorted(a: &[DocId], b: &[DocId]) -> Vec<DocId>
```

**Step 3: Handle fallback for unindexed fields**

If a field in the WHERE clause has no index, fall back to a full collection scan, evaluating the expression against each document's JSON.

**Step 4: Write comprehensive query tests**

- Find by hash index (city = "Accra")
- Find by sorted index range (age >= 25 AND age <= 35)
- Find by array index (tags CONTAINS "rust")
- Compound AND query
- Compound OR query
- Projection on find results
- LIMIT and OFFSET
- COUNT query
- Unindexed field falls back to scan
- Empty result set
- Unique index lookup returns single doc

**Step 5: Run tests and commit**

```bash
cargo test -p kora-doc
git add kora-doc/src/engine.rs kora-doc/src/index.rs
git commit -m "feat(kora-doc): add query executor for DOC.FIND and DOC.COUNT"
```

---

### Task 5: RESP Command Integration

**Files:**
- Modify: `kora-core/src/command.rs` — add DocCreateIndex, DocDropIndex, DocIndexes, DocFind, DocCount variants
- Modify: `kora-protocol/src/command.rs` — parse DOC.CREATEINDEX, DOC.DROPINDEX, DOC.INDEXES, DOC.FIND, DOC.COUNT
- Modify: `kora-server/src/shard_io/connection.rs` — add handlers

**Step 1: Add Command variants to `kora-core/src/command.rs`**

```rust
/// DOC.CREATEINDEX collection field type — create a secondary index.
DocCreateIndex {
    collection: Vec<u8>,
    field: Vec<u8>,
    index_type: Vec<u8>,  // "hash", "sorted", "array", "unique"
},
/// DOC.DROPINDEX collection field — drop a secondary index.
DocDropIndex {
    collection: Vec<u8>,
    field: Vec<u8>,
},
/// DOC.INDEXES collection — list indexes.
DocIndexes {
    collection: Vec<u8>,
},
/// DOC.FIND collection WHERE expr [PROJECT f1 f2 ...] [LIMIT n] [OFFSET n]
DocFind {
    collection: Vec<u8>,
    where_clause: Vec<u8>,
    fields: Vec<Vec<u8>>,
    limit: Option<usize>,
    offset: usize,
},
/// DOC.COUNT collection WHERE expr
DocCount {
    collection: Vec<u8>,
    where_clause: Vec<u8>,
},
```

Add these to `is_write_command()`, `is_multi_key()` (false), and `command_position()` (return 50 for doc commands).

**Step 2: Parse new commands in `kora-protocol/src/command.rs`**

- `DOC.CREATEINDEX collection field hash|sorted|array|unique`
- `DOC.DROPINDEX collection field`
- `DOC.INDEXES collection`
- `DOC.FIND collection WHERE <clause> [PROJECT f1 f2 ...] [LIMIT n] [OFFSET n]`
- `DOC.COUNT collection WHERE <clause>`

For DOC.FIND/DOC.COUNT, everything after "WHERE" until "PROJECT"/"LIMIT"/"OFFSET" keyword is the where clause (joined with spaces).

**Step 3: Add handlers in `kora-server/src/shard_io/connection.rs`**

Wire each new Command variant to a `handle_doc_*` function that calls the DocEngine method.

- `handle_doc_createindex` → `doc_engine.create_index(collection, field, index_type)`
- `handle_doc_dropindex` → `doc_engine.drop_index(collection, field)`
- `handle_doc_indexes` → `doc_engine.indexes(collection)` → Array of arrays
- `handle_doc_find` → `doc_engine.find(collection, where_clause, projection, limit, offset)` → Array of JSON bulk strings
- `handle_doc_count` → `doc_engine.count(collection, where_clause)` → Integer

**Step 4: Add embedded API methods in `kora-embedded/src/lib.rs`**

```rust
pub fn doc_create_index(&self, collection: &str, field: &str, index_type: &str) -> Result<(), DocError>
pub fn doc_drop_index(&self, collection: &str, field: &str) -> Result<(), DocError>
pub fn doc_indexes(&self, collection: &str) -> Result<Vec<(String, String)>, DocError>
pub fn doc_find(&self, collection: &str, where_clause: &str, projection: Option<&[&str]>, limit: Option<usize>, offset: usize) -> Result<Vec<Value>, DocError>
pub fn doc_count(&self, collection: &str, where_clause: &str) -> Result<u64, DocError>
```

**Step 5: Add integration tests in `kora-server/tests/integration.rs`**

- DOC.CREATEINDEX + DOC.FIND via RESP
- DOC.COUNT via RESP
- DOC.INDEXES returns created indexes
- DOC.DROPINDEX removes the index
- Error cases (unknown collection, invalid index type, malformed WHERE)

**Step 6: Run all tests**

```bash
cargo test --workspace --skip cmp_
cargo clippy --workspace --all-targets
```

**Step 7: Commit**

```bash
git add kora-core/src/command.rs kora-protocol/src/command.rs \
  kora-server/src/shard_io/connection.rs kora-embedded/src/lib.rs \
  kora-server/tests/integration.rs
git commit -m "feat: add DOC.CREATEINDEX, DOC.DROPINDEX, DOC.INDEXES, DOC.FIND, DOC.COUNT commands"
```

---

### Task 6: Final Verification and Architecture Doc Update

**Step 1: Run full test suite**

```bash
cargo test --workspace --skip cmp_
cargo clippy --workspace --all-targets
cargo fmt --all -- --check
```

**Step 2: Update roadmap checkboxes in `kora-doc-arch.md`**

Mark Phase 3 items as complete:
- [x] Hash index buckets with delta encoding
- [x] Sorted index with score-grouped storage
- [x] Array membership index
- [x] Unique index with constraint enforcement
- [x] Index maintenance on write path (insert/update/delete)
- [x] Query executor: single condition, compound AND/OR
- [x] DOC.FIND, DOC.COUNT commands

**Step 3: Commit**

```bash
git add kora-doc-arch.md
git commit -m "docs: mark Phase 3 indexing as complete"
```
