# Document Query Language Enhancements Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend kora-doc's query language with `IN`, `EXISTS`, parenthesized grouping, and `ORDER BY` to make adoption frictionless for developers coming from MongoDB/SQL backgrounds.

**Architecture:** All changes are additive. The WHERE expression parser (`kora-doc/src/expr.rs`) gets new `Expr` variants, tokens, and grammar rules. The `DocEngine::find` method gains an `order_by` parameter threaded from protocol parsing through the server handler. No breaking changes to existing queries.

**Tech Stack:** Rust, kora-doc (expr parser + engine), kora-protocol (RESP command parsing), kora-server (handler), kora-core (Command enum)

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `kora-doc/src/expr.rs` | Modify | Add `In`, `Exists`, `Not` Expr variants; `LParen`/`RParen`/`In`/`Exists`/`Not` tokens; update grammar |
| `kora-doc/src/engine.rs` | Modify | Add `execute_leaf` arms for `In`/`Exists`/`Not`; add `order_by` sorting to `find`; update `eval_expr_on_json` |
| `kora-core/src/command.rs` | Modify | Add `order_by` + `order_desc` fields to `DocFind` variant |
| `kora-protocol/src/command/doc_parsing.rs` | Modify | Parse `ORDER BY field ASC/DESC` clause in `parse_doc_find` |
| `kora-server/src/shard_io/connection.rs` | Modify | Thread `order_by`/`order_desc` through `handle_doc_find` |

---

## Chunk 1: WHERE Expression Language Extensions

### Task 1: Add `IN` operator to expression parser

**Files:**
- Modify: `kora-doc/src/expr.rs`

The `IN` operator allows matching a field against a set of values:
`status IN ("active", "pending", "review")`

- [ ] **Step 1: Write failing tests for IN parsing**

Add these tests to the `mod tests` block in `kora-doc/src/expr.rs`:

```rust
#[test]
fn parse_in_strings() {
    let expr = parse_where(r#"status IN ("active", "pending")"#).unwrap();
    assert_eq!(
        expr,
        Expr::In(
            "status".into(),
            vec![
                ExprValue::String("active".into()),
                ExprValue::String("pending".into()),
            ],
        )
    );
}

#[test]
fn parse_in_numbers() {
    let expr = parse_where("age IN (25, 30, 35)").unwrap();
    assert_eq!(
        expr,
        Expr::In(
            "age".into(),
            vec![
                ExprValue::Number(25.0),
                ExprValue::Number(30.0),
                ExprValue::Number(35.0),
            ],
        )
    );
}

#[test]
fn parse_in_single_value() {
    let expr = parse_where(r#"status IN ("active")"#).unwrap();
    assert_eq!(
        expr,
        Expr::In("status".into(), vec![ExprValue::String("active".into())])
    );
}

#[test]
fn parse_in_mixed_with_and() {
    let expr = parse_where(r#"status IN ("active", "pending") AND age > 25"#).unwrap();
    assert_eq!(
        expr,
        Expr::And(
            Box::new(Expr::In(
                "status".into(),
                vec![
                    ExprValue::String("active".into()),
                    ExprValue::String("pending".into()),
                ],
            )),
            Box::new(Expr::Gt("age".into(), 25.0)),
        )
    );
}

#[test]
fn parse_in_case_insensitive() {
    let expr = parse_where(r#"status in ("active")"#).unwrap();
    assert_eq!(
        expr,
        Expr::In("status".into(), vec![ExprValue::String("active".into())])
    );
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p kora-doc -- parse_in`
Expected: compilation errors — `Expr::In` doesn't exist yet.

- [ ] **Step 3: Add `In` variant to `Expr` enum and `In`/`LParen`/`RParen`/`Comma` tokens**

In `kora-doc/src/expr.rs`, add to `Expr` enum after the `Contains` variant:

```rust
    /// Set membership test: `field IN (value1, value2, ...)`.
    In(String, Vec<ExprValue>),
```

Add to `Token` enum:

```rust
    In,
    LParen,
    RParen,
    Comma,
```

In `Lexer::next_token`, add handling for `(`, `)`, and `,` before the catch-all error arm:

```rust
            b'(' => {
                self.pos += 1;
                Ok(Token::LParen)
            }
            b')' => {
                self.pos += 1;
                Ok(Token::RParen)
            }
            b',' => {
                self.pos += 1;
                Ok(Token::Comma)
            }
```

In `lex_ident_or_keyword`, add the `IN` keyword:

```rust
            "IN" => Ok(Token::In),
```

In `Parser::parse_compare`, after the `Token::Contains` match arm, add:

```rust
            Token::In => {
                if self.advance() != Some(Token::LParen) {
                    return Err(ExprError::UnexpectedToken("expected '(' after IN".into()));
                }
                let mut values = vec![self.parse_value()?];
                while self.peek() == Some(&Token::Comma) {
                    self.advance();
                    values.push(self.parse_value()?);
                }
                if self.advance() != Some(Token::RParen) {
                    return Err(ExprError::UnexpectedToken("expected ')' to close IN list".into()));
                }
                Ok(Expr::In(field, values))
            }
```

In `engine.rs`, update `expr_field` to handle `In`:

```rust
        | Expr::In(f, _) => f.as_str(),
```

In `engine.rs`, update `eval_expr_on_json` to handle `In`:

```rust
        Expr::In(path, values) => {
            let Some(field_val) = resolve_json_path(doc, path) else {
                return false;
            };
            values.iter().any(|v| json_matches_expr_value(field_val, v))
        }
```

In `engine.rs`, update `execute_leaf` — add a case before the `_ =>` fallback for `Expr::In` with a hash index. If the field has a `Hash` or `Unique` index, look up all values and union the results:

```rust
            (Expr::In(_, values), Some(IndexType::Hash), Some(fid)) => {
                let mut all_candidates = Vec::new();
                for value in values {
                    let Some(hashed) = expr_value_to_hash(value) else {
                        continue;
                    };
                    if let Some(idx) = state.indexes.hash(fid) {
                        all_candidates.extend_from_slice(idx.lookup(hashed));
                    }
                }
                all_candidates.sort_unstable();
                all_candidates.dedup();
                self.filter_candidates_by_expr(collection_id, state, expr, all_candidates)
            }

            (Expr::In(_, values), Some(IndexType::Unique), Some(fid)) => {
                let mut all_candidates = Vec::new();
                for value in values {
                    let Some(hashed) = expr_value_to_hash(value) else {
                        continue;
                    };
                    if let Some(idx) = state.indexes.unique(fid) {
                        all_candidates.extend_from_slice(idx.lookup(hashed));
                    }
                }
                all_candidates.sort_unstable();
                all_candidates.dedup();
                self.filter_candidates_by_expr(collection_id, state, expr, all_candidates)
            }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p kora-doc -- parse_in`
Expected: all 5 tests PASS.

- [ ] **Step 5: Run full kora-doc test suite**

Run: `cargo test -p kora-doc`
Expected: all existing + new tests PASS.

- [ ] **Step 6: Commit**

```bash
git add kora-doc/src/expr.rs kora-doc/src/engine.rs
git commit -m "feat(kora-doc): add IN operator to WHERE expression language"
```

---

### Task 2: Add `EXISTS` field predicate

**Files:**
- Modify: `kora-doc/src/expr.rs`
- Modify: `kora-doc/src/engine.rs`

The `EXISTS` predicate checks for field presence: `address.city EXISTS`

- [ ] **Step 1: Write failing tests**

Add to `mod tests` in `expr.rs`:

```rust
#[test]
fn parse_exists() {
    let expr = parse_where("address.city EXISTS").unwrap();
    assert_eq!(expr, Expr::Exists("address.city".into()));
}

#[test]
fn parse_exists_case_insensitive() {
    let expr = parse_where("name exists").unwrap();
    assert_eq!(expr, Expr::Exists("name".into()));
}

#[test]
fn parse_exists_with_and() {
    let expr = parse_where(r#"email EXISTS AND status = "active""#).unwrap();
    assert_eq!(
        expr,
        Expr::And(
            Box::new(Expr::Exists("email".into())),
            Box::new(Expr::Eq("status".into(), ExprValue::String("active".into()))),
        )
    );
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p kora-doc -- parse_exists`
Expected: compilation error — `Expr::Exists` doesn't exist.

- [ ] **Step 3: Implement EXISTS**

Add to `Expr` enum:

```rust
    /// Field existence test: `field EXISTS`.
    Exists(String),
```

Add to `Token` enum:

```rust
    Exists,
```

In `lex_ident_or_keyword`, add:

```rust
            "EXISTS" => Ok(Token::Exists),
```

In `Parser::parse_compare`, the current code expects `IDENT op value`. For `EXISTS`, the pattern is `IDENT EXISTS` (no value). Change `parse_compare` to check if the operator is `Exists` before attempting to parse a value:

```rust
            Token::Exists => Ok(Expr::Exists(field)),
```

This slots in alongside the other `Token::*` arms in the `match op` block.

In `engine.rs`, update `expr_field`:

```rust
        | Expr::Exists(f) => f.as_str(),
```

In `eval_expr_on_json`:

```rust
        Expr::Exists(path) => resolve_json_path(doc, path).is_some(),
```

In `execute_leaf`, EXISTS always falls through to `fallback_scan` via the `_ =>` arm (no index optimization needed — it's a structural check).

- [ ] **Step 4: Run tests**

Run: `cargo test -p kora-doc -- parse_exists`
Expected: all 3 tests PASS.

- [ ] **Step 5: Run full suite**

Run: `cargo test -p kora-doc`
Expected: all tests PASS.

- [ ] **Step 6: Commit**

```bash
git add kora-doc/src/expr.rs kora-doc/src/engine.rs
git commit -m "feat(kora-doc): add EXISTS field predicate to WHERE expressions"
```

---

### Task 3: Add `NOT` operator and parenthesized grouping

**Files:**
- Modify: `kora-doc/src/expr.rs`
- Modify: `kora-doc/src/engine.rs`

Parentheses allow explicit grouping: `(a = 1 OR b = 2) AND c = 3`
`NOT` inverts an expression: `NOT status = "deleted"` or `NOT (a = 1 AND b = 2)`

- [ ] **Step 1: Write failing tests**

Add to `mod tests` in `expr.rs`:

```rust
#[test]
fn parse_parenthesized_grouping() {
    let expr = parse_where(r#"(city = "Accra" OR city = "Lagos") AND age > 25"#).unwrap();
    assert_eq!(
        expr,
        Expr::And(
            Box::new(Expr::Or(
                Box::new(Expr::Eq("city".into(), ExprValue::String("Accra".into()))),
                Box::new(Expr::Eq("city".into(), ExprValue::String("Lagos".into()))),
            )),
            Box::new(Expr::Gt("age".into(), 25.0)),
        )
    );
}

#[test]
fn parse_nested_parentheses() {
    let expr = parse_where(r#"(a = "x" AND (b = "y" OR c = "z"))"#).unwrap();
    assert_eq!(
        expr,
        Expr::And(
            Box::new(Expr::Eq("a".into(), ExprValue::String("x".into()))),
            Box::new(Expr::Or(
                Box::new(Expr::Eq("b".into(), ExprValue::String("y".into()))),
                Box::new(Expr::Eq("c".into(), ExprValue::String("z".into()))),
            )),
        )
    );
}

#[test]
fn parse_not_simple() {
    let expr = parse_where(r#"NOT status = "deleted""#).unwrap();
    assert_eq!(
        expr,
        Expr::Not(Box::new(Expr::Eq(
            "status".into(),
            ExprValue::String("deleted".into()),
        )))
    );
}

#[test]
fn parse_not_parenthesized() {
    let expr = parse_where(r#"NOT (a = "x" AND b = "y")"#).unwrap();
    assert_eq!(
        expr,
        Expr::Not(Box::new(Expr::And(
            Box::new(Expr::Eq("a".into(), ExprValue::String("x".into()))),
            Box::new(Expr::Eq("b".into(), ExprValue::String("y".into()))),
        )))
    );
}

#[test]
fn parse_not_case_insensitive() {
    let expr = parse_where(r#"not active = true"#).unwrap();
    assert_eq!(
        expr,
        Expr::Not(Box::new(Expr::Eq("active".into(), ExprValue::Bool(true))))
    );
}

#[test]
fn parse_not_with_and() {
    let expr = parse_where(r#"NOT status = "deleted" AND age > 18"#).unwrap();
    assert_eq!(
        expr,
        Expr::And(
            Box::new(Expr::Not(Box::new(Expr::Eq(
                "status".into(),
                ExprValue::String("deleted".into()),
            )))),
            Box::new(Expr::Gt("age".into(), 18.0)),
        )
    );
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p kora-doc -- parse_parenthesi parse_nested parse_not`
Expected: compilation errors.

- [ ] **Step 3: Implement NOT and parenthesized grouping**

Add to `Expr` enum:

```rust
    /// Logical NOT of an expression.
    Not(Box<Expr>),
```

Add to `Token` enum:

```rust
    Not,
```

In `lex_ident_or_keyword`, add:

```rust
            "NOT" => Ok(Token::Not),
```

The grammar changes. Currently `parse_compare` expects `IDENT op value`. We need `parse_compare` to also handle:
- `( expr )` — parenthesized sub-expression
- `NOT primary` — negation
- `IDENT EXISTS` — already handled in Task 2

**Updated grammar:**

```text
expr     = or_expr
or_expr  = and_expr ( "OR" and_expr )*
and_expr = primary  ( "AND" primary )*
primary  = "NOT" primary
         | "(" expr ")"
         | IDENT "EXISTS"
         | IDENT "IN" "(" value ("," value)* ")"
         | IDENT op value
```

Rename `parse_compare` to `parse_primary`. Replace its body:

```rust
    fn parse_primary(&mut self) -> Result<Expr, ExprError> {
        match self.peek() {
            Some(Token::Not) => {
                self.advance();
                let inner = self.parse_primary()?;
                Ok(Expr::Not(Box::new(inner)))
            }
            Some(Token::LParen) => {
                self.advance();
                let inner = self.parse_expr()?;
                if self.advance() != Some(Token::RParen) {
                    return Err(ExprError::UnexpectedToken("expected ')'".into()));
                }
                Ok(inner)
            }
            _ => self.parse_comparison(),
        }
    }

    fn parse_comparison(&mut self) -> Result<Expr, ExprError> {
        let field = match self.advance() {
            Some(Token::Ident(name)) => name,
            Some(other) => return Err(ExprError::UnexpectedToken(format!("{other:?}"))),
            None => return Err(ExprError::UnexpectedEnd),
        };

        let op = match self.advance() {
            Some(t) => t,
            None => return Err(ExprError::UnexpectedEnd),
        };

        match op {
            Token::Eq => {
                let value = self.parse_value()?;
                Ok(Expr::Eq(field, value))
            }
            Token::Neq => {
                let value = self.parse_value()?;
                Ok(Expr::Neq(field, value))
            }
            Token::Gt => {
                let n = self.parse_number_value()?;
                Ok(Expr::Gt(field, n))
            }
            Token::Gte => {
                let n = self.parse_number_value()?;
                Ok(Expr::Gte(field, n))
            }
            Token::Lt => {
                let n = self.parse_number_value()?;
                Ok(Expr::Lt(field, n))
            }
            Token::Lte => {
                let n = self.parse_number_value()?;
                Ok(Expr::Lte(field, n))
            }
            Token::Contains => {
                let value = self.parse_value()?;
                Ok(Expr::Contains(field, value))
            }
            Token::In => {
                if self.advance() != Some(Token::LParen) {
                    return Err(ExprError::UnexpectedToken("expected '(' after IN".into()));
                }
                let mut values = vec![self.parse_value()?];
                while self.peek() == Some(&Token::Comma) {
                    self.advance();
                    values.push(self.parse_value()?);
                }
                if self.advance() != Some(Token::RParen) {
                    return Err(ExprError::UnexpectedToken("expected ')' to close IN list".into()));
                }
                Ok(Expr::In(field, values))
            }
            Token::Exists => Ok(Expr::Exists(field)),
            other => Err(ExprError::UnexpectedToken(format!("{other:?}"))),
        }
    }
```

Update `parse_and` to call `parse_primary` instead of `parse_compare`:

```rust
    fn parse_and(&mut self) -> Result<Expr, ExprError> {
        let mut left = self.parse_primary()?;
        while self.peek() == Some(&Token::And) {
            self.advance();
            let right = self.parse_primary()?;
            left = Expr::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }
```

In `engine.rs`, update `expr_field` for `Not`:

```rust
        Expr::Not(inner) => expr_field(inner),
```

In `eval_expr_on_json`:

```rust
        Expr::Not(inner) => !eval_expr_on_json(doc, inner),
```

In `execute_expr`, add handling for `Not` — it falls through to `fallback_scan` since set complement requires full knowledge:

```rust
            Expr::Not(_) => self.fallback_scan(collection_id, state, expr),
```

Add this arm before the `_ => self.execute_leaf(...)` line.

- [ ] **Step 4: Run tests**

Run: `cargo test -p kora-doc`
Expected: all tests PASS (old + new).

- [ ] **Step 5: Run clippy**

Run: `cargo clippy -p kora-doc --all-targets`
Expected: no warnings.

- [ ] **Step 6: Commit**

```bash
git add kora-doc/src/expr.rs kora-doc/src/engine.rs
git commit -m "feat(kora-doc): add NOT operator and parenthesized grouping to WHERE expressions"
```

---

## Chunk 2: ORDER BY Support

### Task 4: Add ORDER BY to DocFind command definition

**Files:**
- Modify: `kora-core/src/command.rs` (lines ~835-845, the `DocFind` variant)

- [ ] **Step 1: Add `order_by` and `order_desc` fields to `DocFind`**

In the `DocFind` variant of the `Command` enum in `kora-core/src/command.rs`, add two new fields after `offset`:

```rust
    DocFind {
        /// Collection name.
        collection: Vec<u8>,
        /// WHERE expression string.
        where_clause: Vec<u8>,
        /// Optional projection field paths.
        fields: Vec<Vec<u8>>,
        /// Optional result limit.
        limit: Option<usize>,
        /// Result offset (default 0).
        offset: usize,
        /// Optional field path to sort results by.
        order_by: Option<Vec<u8>>,
        /// True if sort order is descending (default ascending).
        order_desc: bool,
    },
```

- [ ] **Step 2: Fix all compilation errors from the new fields**

Every place that constructs or pattern-matches `Command::DocFind` needs updating. These are:
- `kora-protocol/src/command/doc_parsing.rs` line 271 — add `order_by: None, order_desc: false`
- `kora-server/src/shard_io/connection.rs` line ~1432 — destructure `order_by` and `order_desc`
- `kora-core/src/command.rs` — any `DocFind { .. }` patterns (these use `..` so they're fine)

Run: `cargo build --workspace`
Expected: compiles clean.

- [ ] **Step 3: Commit**

```bash
git add kora-core/src/command.rs kora-protocol/src/command/doc_parsing.rs kora-server/src/shard_io/connection.rs
git commit -m "feat(kora-core): add order_by and order_desc fields to DocFind command"
```

---

### Task 5: Parse ORDER BY clause in RESP protocol

**Files:**
- Modify: `kora-protocol/src/command/doc_parsing.rs`

Syntax: `DOC.FIND collection WHERE expr [ORDER BY field [ASC|DESC]] [PROJECT ...] [LIMIT n] [OFFSET n]`

- [ ] **Step 1: Write failing test**

Add a test in `kora-protocol/src/command/` tests (or in the existing test module for doc_parsing). If there's no test module for doc_parsing, add tests to the crate's integration tests or the `mod tests` in `command.rs`. For now, we'll verify via the integration test in Task 7.

- [ ] **Step 2: Update `parse_doc_find` to handle ORDER BY**

In the keyword scanning loop (the `while idx < args.len()` block starting around line 231), the current code checks for `PROJECT`, `LIMIT`, `OFFSET`. Add `ORDER` as a stop-word in the WHERE token collector (line 209-211):

```rust
        if token.as_slice() == b"PROJECT"
            || token.as_slice() == b"LIMIT"
            || token.as_slice() == b"OFFSET"
            || token.as_slice() == b"ORDER"
        {
            break;
        }
```

Then add `order_by` and `order_desc` variables alongside `fields`, `limit`, `offset`:

```rust
    let mut order_by: Option<Vec<u8>> = None;
    let mut order_desc: bool = false;
```

Add an `ORDER` arm in the keyword match block:

```rust
            b"ORDER" => {
                if idx >= args.len() {
                    return Err(ProtocolError::WrongArity("DOC.FIND".into()));
                }
                let by_kw = extract_string(&args[idx])?.to_ascii_uppercase();
                if by_kw.as_slice() != b"BY" {
                    return Err(ProtocolError::InvalidData(
                        "DOC.FIND ORDER expects BY keyword".into(),
                    ));
                }
                idx += 1;
                if idx >= args.len() {
                    return Err(ProtocolError::WrongArity("DOC.FIND".into()));
                }
                order_by = Some(extract_bytes(&args[idx])?);
                idx += 1;
                if idx < args.len() {
                    let dir = extract_string(&args[idx])?.to_ascii_uppercase();
                    match dir.as_slice() {
                        b"ASC" => {
                            idx += 1;
                        }
                        b"DESC" => {
                            order_desc = true;
                            idx += 1;
                        }
                        _ => {}
                    }
                }
            }
```

Update the `Ok(Command::DocFind { ... })` construction to include the new fields:

```rust
    Ok(Command::DocFind {
        collection,
        where_clause,
        fields,
        limit,
        offset,
        order_by,
        order_desc,
    })
```

- [ ] **Step 3: Verify compilation**

Run: `cargo build -p kora-protocol`
Expected: compiles clean.

- [ ] **Step 4: Commit**

```bash
git add kora-protocol/src/command/doc_parsing.rs
git commit -m "feat(kora-protocol): parse ORDER BY clause in DOC.FIND"
```

---

### Task 6: Implement ORDER BY in DocEngine::find and server handler

**Files:**
- Modify: `kora-doc/src/engine.rs`
- Modify: `kora-server/src/shard_io/connection.rs`

- [ ] **Step 1: Write failing test for ORDER BY in DocEngine**

Add to `mod tests` in `engine.rs`:

```rust
#[test]
fn find_order_by_ascending() {
    let engine = DocEngine::new();
    engine.create_collection("users", CompressionProfile::None).unwrap();
    engine.set("users", "alice", &serde_json::json!({"name": "Alice", "age": 30})).unwrap();
    engine.set("users", "bob", &serde_json::json!({"name": "Bob", "age": 20})).unwrap();
    engine.set("users", "charlie", &serde_json::json!({"name": "Charlie", "age": 25})).unwrap();

    let results = engine.find("users", "age > 0", None, None, 0, Some("age"), false).unwrap();
    let ages: Vec<i64> = results.iter().map(|v| v["age"].as_i64().unwrap()).collect();
    assert_eq!(ages, vec![20, 25, 30]);
}

#[test]
fn find_order_by_descending() {
    let engine = DocEngine::new();
    engine.create_collection("users", CompressionProfile::None).unwrap();
    engine.set("users", "alice", &serde_json::json!({"name": "Alice", "age": 30})).unwrap();
    engine.set("users", "bob", &serde_json::json!({"name": "Bob", "age": 20})).unwrap();
    engine.set("users", "charlie", &serde_json::json!({"name": "Charlie", "age": 25})).unwrap();

    let results = engine.find("users", "age > 0", None, None, 0, Some("age"), true).unwrap();
    let ages: Vec<i64> = results.iter().map(|v| v["age"].as_i64().unwrap()).collect();
    assert_eq!(ages, vec![30, 25, 20]);
}

#[test]
fn find_order_by_string_field() {
    let engine = DocEngine::new();
    engine.create_collection("users", CompressionProfile::None).unwrap();
    engine.set("users", "alice", &serde_json::json!({"name": "Charlie"})).unwrap();
    engine.set("users", "bob", &serde_json::json!({"name": "Alice"})).unwrap();
    engine.set("users", "charlie", &serde_json::json!({"name": "Bob"})).unwrap();

    let results = engine.find("users", "name EXISTS", None, None, 0, Some("name"), false).unwrap();
    let names: Vec<&str> = results.iter().map(|v| v["name"].as_str().unwrap()).collect();
    assert_eq!(names, vec!["Alice", "Bob", "Charlie"]);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p kora-doc -- find_order_by`
Expected: compilation error — `find` doesn't accept `order_by` parameter yet.

- [ ] **Step 3: Add `order_by` parameter to `DocEngine::find`**

Update the `find` method signature in `engine.rs`:

```rust
    pub fn find(
        &self,
        collection: &str,
        where_clause: &str,
        projection: Option<&[&str]>,
        limit: Option<usize>,
        offset: usize,
        order_by: Option<&str>,
        order_desc: bool,
    ) -> Result<Vec<Value>, DocError> {
```

After collecting `doc_ids` from `execute_expr`, and **before** applying pagination (LIMIT/OFFSET), add sorting:

```rust
        let doc_ids = self.execute_expr(collection_id, state, &expr)?;

        let doc_ids = if let Some(sort_field) = order_by {
            self.sort_doc_ids(collection_id, state, doc_ids, sort_field, order_desc)?
        } else {
            doc_ids
        };
```

Add the `sort_doc_ids` method:

```rust
    fn sort_doc_ids(
        &self,
        collection_id: CollectionId,
        state: &CollectionState,
        doc_ids: Vec<DocId>,
        sort_field: &str,
        descending: bool,
    ) -> Result<Vec<DocId>, DocError> {
        let mut keyed: Vec<(DocId, Option<Value>)> = Vec::with_capacity(doc_ids.len());
        for &doc_id in &doc_ids {
            let sort_val = state
                .docs_by_internal_id
                .get(&doc_id)
                .and_then(|packed| {
                    Recomposer::recompose(packed, &self.registry, &state.dictionary, collection_id)
                        .ok()
                })
                .and_then(|json| resolve_json_path(&json, sort_field).cloned());
            keyed.push((doc_id, sort_val));
        }

        keyed.sort_by(|a, b| {
            let ordering = cmp_json_values(&a.1, &b.1);
            if descending {
                ordering.reverse()
            } else {
                ordering
            }
        });

        Ok(keyed.into_iter().map(|(id, _)| id).collect())
    }
```

Add the comparison helper as a free function:

```rust
fn cmp_json_values(a: &Option<Value>, b: &Option<Value>) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (Some(a), Some(b)) => cmp_json_value_inner(a, b),
    }
}

fn cmp_json_value_inner(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (Value::Number(a), Value::Number(b)) => {
            let fa = a.as_f64().unwrap_or(0.0);
            let fb = b.as_f64().unwrap_or(0.0);
            fa.partial_cmp(&fb).unwrap_or(Ordering::Equal)
        }
        (Value::String(a), Value::String(b)) => a.cmp(b),
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Null, Value::Null) => Ordering::Equal,
        _ => Ordering::Equal,
    }
}
```

Also update the `count` method call — it calls `find` indirectly, but actually `count` has its own implementation that doesn't call `find`, so no change needed there.

Update **all existing callers** of `engine.find()` — search for `.find(` in the engine.rs test module and update them to add `None, false` for the new parameters. Also update:

- `kora-server/src/shard_io/connection.rs` `handle_doc_find` function — thread `order_by` and `order_desc` from the destructured `Command::DocFind`.

In `connection.rs`, update the match arm for `Command::DocFind`:

```rust
        Command::DocFind {
            collection,
            where_clause,
            fields,
            limit,
            offset,
            order_by,
            order_desc,
        } => Some(handle_doc_find(
            &shared.doc_engine,
            collection,
            where_clause,
            fields,
            *limit,
            *offset,
            order_by.as_deref(),
            *order_desc,
        )),
```

Update `handle_doc_find` signature:

```rust
fn handle_doc_find(
    doc_engine: &RwLock<DocEngine>,
    collection: &[u8],
    where_clause: &[u8],
    fields: &[Vec<u8>],
    limit: Option<usize>,
    offset: usize,
    order_by: Option<&[u8]>,
    order_desc: bool,
) -> CommandResponse {
```

Add order_by parsing inside the function:

```rust
    let order_by_str = match order_by {
        Some(bytes) => match std::str::from_utf8(bytes) {
            Ok(s) => Some(s),
            Err(_) => return CommandResponse::Error("ERR invalid UTF-8 in ORDER BY field".into()),
        },
        None => None,
    };
```

Update the `doc_engine.read().find(...)` call to pass the new parameters:

```rust
    let docs = match doc_engine.read().find(
        &collection,
        &where_clause,
        projection.as_deref(),
        limit,
        offset,
        order_by_str,
        order_desc,
    ) {
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p kora-doc -- find_order_by`
Expected: all 3 ORDER BY tests PASS.

- [ ] **Step 5: Run full workspace build and test**

Run: `cargo build --workspace && cargo test --workspace --skip cmp_`
Expected: everything compiles and passes.

- [ ] **Step 6: Commit**

```bash
git add kora-doc/src/engine.rs kora-server/src/shard_io/connection.rs
git commit -m "feat(kora-doc): implement ORDER BY sorting in DOC.FIND queries"
```

---

### Task 7: Integration test for ORDER BY via RESP protocol

**Files:**
- Modify: `kora-server/tests/integration.rs`

- [ ] **Step 1: Add integration test**

Add a new test function to the integration test file:

```rust
#[tokio::test]
async fn test_doc_find_order_by() {
    let port = get_free_port();
    let (server, _tmpdir) = start_test_server(port).await;

    let mut conn = connect(port).await;

    // Create collection
    send_command(&mut conn, &["DOC.CREATE", "users"]).await;
    read_response(&mut conn).await;

    // Insert docs with varying ages
    send_command(&mut conn, &["DOC.SET", "users", "alice", r#"{"name":"Alice","age":30}"#]).await;
    read_response(&mut conn).await;
    send_command(&mut conn, &["DOC.SET", "users", "bob", r#"{"name":"Bob","age":20}"#]).await;
    read_response(&mut conn).await;
    send_command(&mut conn, &["DOC.SET", "users", "charlie", r#"{"name":"Charlie","age":25}"#]).await;
    read_response(&mut conn).await;

    // Find with ORDER BY age ASC
    send_command(&mut conn, &["DOC.FIND", "users", "WHERE", "age", ">", "0", "ORDER", "BY", "age", "ASC"]).await;
    let resp = read_response(&mut conn).await;
    // Verify first result is Bob (age 20)
    assert!(resp.contains("Bob"));

    // Find with ORDER BY age DESC
    send_command(&mut conn, &["DOC.FIND", "users", "WHERE", "age", ">", "0", "ORDER", "BY", "age", "DESC"]).await;
    let resp = read_response(&mut conn).await;
    // Verify first result is Alice (age 30)
    assert!(resp.contains("Alice"));

    server.shutdown();
}
```

Note: Adapt this test to match the existing integration test helper patterns (use whatever `start_test_server`, `send_command`, `read_response`, `get_free_port` patterns already exist in the file).

- [ ] **Step 2: Run integration test**

Run: `cargo test -p kora-server --test integration test_doc_find_order_by`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add kora-server/tests/integration.rs
git commit -m "test(kora-server): add integration test for DOC.FIND ORDER BY"
```

---

## Chunk 3: Engine-level expression evaluation tests

### Task 8: Add engine-level tests for IN, EXISTS, NOT in query execution

**Files:**
- Modify: `kora-doc/src/engine.rs` (test module)

- [ ] **Step 1: Write engine-level query tests**

Add these tests to the existing test module in `engine.rs`:

```rust
#[test]
fn find_with_in_operator() {
    let engine = DocEngine::new();
    engine.create_collection("users", CompressionProfile::None).unwrap();
    engine.set("users", "u1", &serde_json::json!({"name": "Alice", "status": "active"})).unwrap();
    engine.set("users", "u2", &serde_json::json!({"name": "Bob", "status": "pending"})).unwrap();
    engine.set("users", "u3", &serde_json::json!({"name": "Charlie", "status": "deleted"})).unwrap();

    let results = engine.find("users", r#"status IN ("active", "pending")"#, None, None, 0, None, false).unwrap();
    assert_eq!(results.len(), 2);
}

#[test]
fn find_with_exists() {
    let engine = DocEngine::new();
    engine.create_collection("users", CompressionProfile::None).unwrap();
    engine.set("users", "u1", &serde_json::json!({"name": "Alice", "email": "alice@test.com"})).unwrap();
    engine.set("users", "u2", &serde_json::json!({"name": "Bob"})).unwrap();

    let results = engine.find("users", "email EXISTS", None, None, 0, None, false).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["name"], "Alice");
}

#[test]
fn find_with_not() {
    let engine = DocEngine::new();
    engine.create_collection("users", CompressionProfile::None).unwrap();
    engine.set("users", "u1", &serde_json::json!({"name": "Alice", "status": "active"})).unwrap();
    engine.set("users", "u2", &serde_json::json!({"name": "Bob", "status": "deleted"})).unwrap();
    engine.set("users", "u3", &serde_json::json!({"name": "Charlie", "status": "active"})).unwrap();

    let results = engine.find("users", r#"NOT status = "deleted""#, None, None, 0, None, false).unwrap();
    assert_eq!(results.len(), 2);
}

#[test]
fn find_with_parenthesized_grouping() {
    let engine = DocEngine::new();
    engine.create_collection("users", CompressionProfile::None).unwrap();
    engine.set("users", "u1", &serde_json::json!({"city": "Accra", "age": 30})).unwrap();
    engine.set("users", "u2", &serde_json::json!({"city": "Lagos", "age": 20})).unwrap();
    engine.set("users", "u3", &serde_json::json!({"city": "Nairobi", "age": 35})).unwrap();

    let results = engine.find(
        "users",
        r#"(city = "Accra" OR city = "Lagos") AND age > 18"#,
        None, None, 0, None, false,
    ).unwrap();
    assert_eq!(results.len(), 2);
}

#[test]
fn find_in_with_hash_index() {
    let engine = DocEngine::new();
    engine.create_collection("users", CompressionProfile::None).unwrap();
    engine.create_index("users", "status", IndexType::Hash).unwrap();
    engine.set("users", "u1", &serde_json::json!({"name": "Alice", "status": "active"})).unwrap();
    engine.set("users", "u2", &serde_json::json!({"name": "Bob", "status": "pending"})).unwrap();
    engine.set("users", "u3", &serde_json::json!({"name": "Charlie", "status": "deleted"})).unwrap();

    let results = engine.find("users", r#"status IN ("active", "pending")"#, None, None, 0, None, false).unwrap();
    assert_eq!(results.len(), 2);
}
```

- [ ] **Step 2: Run the tests**

Run: `cargo test -p kora-doc -- find_with_in find_with_exists find_with_not find_with_parenthesized find_in_with_hash`
Expected: all PASS.

- [ ] **Step 3: Commit**

```bash
git add kora-doc/src/engine.rs
git commit -m "test(kora-doc): add engine-level tests for IN, EXISTS, NOT, and parenthesized queries"
```

---

### Task 9: Final verification

- [ ] **Step 1: Run full workspace build**

Run: `cargo build --workspace`
Expected: clean compilation, zero errors.

- [ ] **Step 2: Run clippy**

Run: `cargo clippy --workspace --all-targets`
Expected: zero warnings.

- [ ] **Step 3: Run all tests**

Run: `cargo test --workspace --skip cmp_`
Expected: all tests pass.

- [ ] **Step 4: Run fmt**

Run: `cargo fmt --all`
Expected: no formatting changes (or apply them).
