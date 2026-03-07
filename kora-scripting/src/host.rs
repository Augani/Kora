//! Host function bridge between WASM scripts and the Kōra shard engine.
//!
//! Provides host functions that WASM modules can import to read/write
//! keys in the data store via the `ShardEngine`.

use std::sync::Arc;

use kora_core::command::{Command, CommandResponse};
use kora_core::shard::ShardEngine;
use wasmtime::{Caller, Engine, Linker};

/// Execution context stored in `wasmtime::Store` data, giving host functions
/// access to the shard engine.
pub struct HostContext {
    /// Reference to the shard engine for dispatching commands.
    pub engine: Arc<ShardEngine>,
}

impl HostContext {
    /// Create a new host context wrapping the given engine.
    pub fn new(engine: Arc<ShardEngine>) -> Self {
        Self { engine }
    }
}

/// Read a byte slice from WASM linear memory at `(ptr, len)`.
///
/// Returns `None` if the memory export is missing or the range is out of bounds.
fn read_guest_bytes(caller: &mut Caller<'_, HostContext>, ptr: i32, len: i32) -> Option<Vec<u8>> {
    if ptr < 0 || len < 0 {
        return None;
    }
    let memory = caller.get_export("memory")?.into_memory()?;
    let start = ptr as usize;
    let end = start.checked_add(len as usize)?;
    let data = memory.data(caller);
    if end > data.len() {
        return None;
    }
    Some(data[start..end].to_vec())
}

/// Write a byte slice into WASM linear memory at `ptr`.
///
/// Returns `false` if the memory export is missing or the destination is out of bounds.
fn write_guest_bytes(caller: &mut Caller<'_, HostContext>, ptr: i32, bytes: &[u8]) -> bool {
    if ptr < 0 {
        return false;
    }
    let memory = match caller.get_export("memory") {
        Some(ext) => match ext.into_memory() {
            Some(m) => m,
            None => return false,
        },
        None => return false,
    };
    let start = ptr as usize;
    let end = match start.checked_add(bytes.len()) {
        Some(e) => e,
        None => return false,
    };
    let data = memory.data_mut(caller);
    if end > data.len() {
        return false;
    }
    data[start..end].copy_from_slice(bytes);
    true
}

/// Register all Kōra host functions on the given linker.
///
/// The following imports are defined under the `"kora"` module namespace:
/// - `kora_get(key_ptr, key_len) -> i64` — returns packed `(ptr << 32 | len)` or `-1`
/// - `kora_set(key_ptr, key_len, val_ptr, val_len) -> i32` — returns `1` on success, `0` on failure
/// - `kora_del(key_ptr, key_len) -> i32` — returns number of keys deleted
/// - `kora_hget(key_ptr, key_len, field_ptr, field_len) -> i64` — returns packed result or `-1`
/// - `kora_hset(key_ptr, key_len, field_ptr, field_len, val_ptr, val_len) -> i32` — returns fields added
///
/// WASM modules that call `kora_get` or `kora_hget` must also export a
/// `kora_alloc(size: i32) -> i32` function so the host can allocate guest memory
/// for writing return values.
pub fn register_host_functions(linker: &mut Linker<HostContext>) -> Result<(), wasmtime::Error> {
    linker.func_wrap(
        "kora",
        "kora_get",
        |mut caller: Caller<'_, HostContext>, key_ptr: i32, key_len: i32| -> i64 {
            let key = match read_guest_bytes(&mut caller, key_ptr, key_len) {
                Some(k) => k,
                None => return -1,
            };
            let resp = caller.data().engine.dispatch_blocking(Command::Get { key });
            match resp {
                CommandResponse::BulkString(val) => {
                    let alloc = match caller.get_export("kora_alloc") {
                        Some(ext) => ext,
                        None => return -1,
                    };
                    let alloc_fn = match alloc.into_func() {
                        Some(f) => f,
                        None => return -1,
                    };
                    let mut result = [wasmtime::Val::I32(0)];
                    if alloc_fn
                        .call(
                            &mut caller,
                            &[wasmtime::Val::I32(val.len() as i32)],
                            &mut result,
                        )
                        .is_err()
                    {
                        return -1;
                    }
                    let dest_ptr = match &result[0] {
                        wasmtime::Val::I32(p) => *p,
                        _ => return -1,
                    };
                    if !write_guest_bytes(&mut caller, dest_ptr, &val) {
                        return -1;
                    }
                    ((dest_ptr as i64) << 32) | (val.len() as i64)
                }
                CommandResponse::Nil => -1,
                _ => -1,
            }
        },
    )?;

    linker.func_wrap(
        "kora",
        "kora_set",
        |mut caller: Caller<'_, HostContext>,
         key_ptr: i32,
         key_len: i32,
         val_ptr: i32,
         val_len: i32|
         -> i32 {
            let key = match read_guest_bytes(&mut caller, key_ptr, key_len) {
                Some(k) => k,
                None => return 0,
            };
            let value = match read_guest_bytes(&mut caller, val_ptr, val_len) {
                Some(v) => v,
                None => return 0,
            };
            let resp = caller.data().engine.dispatch_blocking(Command::Set {
                key,
                value,
                ex: None,
                px: None,
                nx: false,
                xx: false,
            });
            match resp {
                CommandResponse::Ok => 1,
                _ => 0,
            }
        },
    )?;

    linker.func_wrap(
        "kora",
        "kora_del",
        |mut caller: Caller<'_, HostContext>, key_ptr: i32, key_len: i32| -> i32 {
            let key = match read_guest_bytes(&mut caller, key_ptr, key_len) {
                Some(k) => k,
                None => return 0,
            };
            let resp = caller
                .data()
                .engine
                .dispatch_blocking(Command::Del { keys: vec![key] });
            match resp {
                CommandResponse::Integer(n) => n as i32,
                _ => 0,
            }
        },
    )?;

    linker.func_wrap(
        "kora",
        "kora_hget",
        |mut caller: Caller<'_, HostContext>,
         key_ptr: i32,
         key_len: i32,
         field_ptr: i32,
         field_len: i32|
         -> i64 {
            let key = match read_guest_bytes(&mut caller, key_ptr, key_len) {
                Some(k) => k,
                None => return -1,
            };
            let field = match read_guest_bytes(&mut caller, field_ptr, field_len) {
                Some(f) => f,
                None => return -1,
            };
            let resp = caller
                .data()
                .engine
                .dispatch_blocking(Command::HGet { key, field });
            match resp {
                CommandResponse::BulkString(val) => {
                    let alloc = match caller.get_export("kora_alloc") {
                        Some(ext) => ext,
                        None => return -1,
                    };
                    let alloc_fn = match alloc.into_func() {
                        Some(f) => f,
                        None => return -1,
                    };
                    let mut result = [wasmtime::Val::I32(0)];
                    if alloc_fn
                        .call(
                            &mut caller,
                            &[wasmtime::Val::I32(val.len() as i32)],
                            &mut result,
                        )
                        .is_err()
                    {
                        return -1;
                    }
                    let dest_ptr = match &result[0] {
                        wasmtime::Val::I32(p) => *p,
                        _ => return -1,
                    };
                    if !write_guest_bytes(&mut caller, dest_ptr, &val) {
                        return -1;
                    }
                    ((dest_ptr as i64) << 32) | (val.len() as i64)
                }
                CommandResponse::Nil => -1,
                _ => -1,
            }
        },
    )?;

    linker.func_wrap(
        "kora",
        "kora_hset",
        |mut caller: Caller<'_, HostContext>,
         key_ptr: i32,
         key_len: i32,
         field_ptr: i32,
         field_len: i32,
         val_ptr: i32,
         val_len: i32|
         -> i32 {
            let key = match read_guest_bytes(&mut caller, key_ptr, key_len) {
                Some(k) => k,
                None => return 0,
            };
            let field = match read_guest_bytes(&mut caller, field_ptr, field_len) {
                Some(f) => f,
                None => return 0,
            };
            let value = match read_guest_bytes(&mut caller, val_ptr, val_len) {
                Some(v) => v,
                None => return 0,
            };
            let resp = caller.data().engine.dispatch_blocking(Command::HSet {
                key,
                fields: vec![(field, value)],
            });
            match resp {
                CommandResponse::Integer(n) => n as i32,
                _ => 0,
            }
        },
    )?;

    Ok(())
}

/// Create a new `Linker<HostContext>` with all Kōra host functions pre-registered.
pub fn create_host_linker(wasm_engine: &Engine) -> Result<Linker<HostContext>, wasmtime::Error> {
    let mut linker = Linker::new(wasm_engine);
    register_host_functions(&mut linker)?;
    Ok(linker)
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasmtime::{Config, Store};

    fn make_engine_and_linker() -> (Arc<ShardEngine>, Engine, Linker<HostContext>) {
        let shard_engine = Arc::new(ShardEngine::new(2));
        let mut config = Config::new();
        config.consume_fuel(true);
        let wasm_engine = Engine::new(&config).expect("failed to create engine");
        let linker = create_host_linker(&wasm_engine).expect("failed to create linker");
        (shard_engine, wasm_engine, linker)
    }

    #[test]
    fn test_host_functions_register() {
        let (_shard_engine, _wasm_engine, _linker) = make_engine_and_linker();
    }

    #[test]
    fn test_kora_set_and_get_via_wasm() {
        let (shard_engine, wasm_engine, linker) = make_engine_and_linker();

        let wat = r#"
            (module
                (import "kora" "kora_set" (func $kora_set (param i32 i32 i32 i32) (result i32)))
                (import "kora" "kora_get" (func $kora_get (param i32 i32) (result i64)))
                (memory (export "memory") 1)
                (data (i32.const 0) "mykey")
                (data (i32.const 5) "myval")

                (func $alloc (param $size i32) (result i32)
                    i32.const 100)
                (export "kora_alloc" (func $alloc))

                (func $do_set (export "do_set") (result i32)
                    i32.const 0  ;; key_ptr
                    i32.const 5  ;; key_len
                    i32.const 5  ;; val_ptr
                    i32.const 5  ;; val_len
                    call $kora_set)

                (func $do_get (export "do_get") (result i64)
                    i32.const 0  ;; key_ptr
                    i32.const 5  ;; key_len
                    call $kora_get))
        "#;

        let module =
            wasmtime::Module::new(&wasm_engine, wat).expect("failed to compile test module");
        let ctx = HostContext::new(shard_engine);
        let mut store = Store::new(&wasm_engine, ctx);
        store.set_fuel(1_000_000).expect("set fuel");

        let instance = linker
            .instantiate(&mut store, &module)
            .expect("failed to instantiate");

        let do_set = instance
            .get_typed_func::<(), i32>(&mut store, "do_set")
            .expect("get do_set");
        let result = do_set.call(&mut store, ()).expect("call do_set");
        assert_eq!(result, 1);

        store.set_fuel(1_000_000).expect("reset fuel");
        let do_get = instance
            .get_typed_func::<(), i64>(&mut store, "do_get")
            .expect("get do_get");
        let packed = do_get.call(&mut store, ()).expect("call do_get");
        assert_ne!(packed, -1, "kora_get should return packed ptr+len");
        let ret_ptr = (packed >> 32) as i32;
        let ret_len = (packed & 0xFFFF_FFFF) as i32;
        assert_eq!(ret_ptr, 100);
        assert_eq!(ret_len, 5);
    }

    #[test]
    fn test_kora_del_via_wasm() {
        let (shard_engine, wasm_engine, linker) = make_engine_and_linker();

        shard_engine.dispatch_blocking(Command::Set {
            key: b"delme".to_vec(),
            value: b"val".to_vec(),
            ex: None,
            px: None,
            nx: false,
            xx: false,
        });

        let wat = r#"
            (module
                (import "kora" "kora_del" (func $kora_del (param i32 i32) (result i32)))
                (memory (export "memory") 1)
                (data (i32.const 0) "delme")
                (func $do_del (export "do_del") (result i32)
                    i32.const 0
                    i32.const 5
                    call $kora_del))
        "#;

        let module = wasmtime::Module::new(&wasm_engine, wat).expect("compile");
        let ctx = HostContext::new(shard_engine.clone());
        let mut store = Store::new(&wasm_engine, ctx);
        store.set_fuel(1_000_000).expect("fuel");

        let instance = linker
            .instantiate(&mut store, &module)
            .expect("instantiate");
        let do_del = instance
            .get_typed_func::<(), i32>(&mut store, "do_del")
            .expect("get do_del");
        let deleted = do_del.call(&mut store, ()).expect("call do_del");
        assert_eq!(deleted, 1);

        let resp = shard_engine.dispatch_blocking(Command::Get {
            key: b"delme".to_vec(),
        });
        assert!(matches!(resp, CommandResponse::Nil));
    }

    #[test]
    fn test_kora_hset_hget_via_wasm() {
        let (shard_engine, wasm_engine, linker) = make_engine_and_linker();

        let wat = r#"
            (module
                (import "kora" "kora_hset" (func $kora_hset (param i32 i32 i32 i32 i32 i32) (result i32)))
                (import "kora" "kora_hget" (func $kora_hget (param i32 i32 i32 i32) (result i64)))
                (memory (export "memory") 1)
                (data (i32.const 0) "hash")
                (data (i32.const 4) "field")
                (data (i32.const 9) "value")

                (func $alloc (param $size i32) (result i32)
                    i32.const 200)
                (export "kora_alloc" (func $alloc))

                (func $do_hset (export "do_hset") (result i32)
                    i32.const 0   ;; key_ptr
                    i32.const 4   ;; key_len ("hash")
                    i32.const 4   ;; field_ptr
                    i32.const 5   ;; field_len ("field")
                    i32.const 9   ;; val_ptr
                    i32.const 5   ;; val_len ("value")
                    call $kora_hset)

                (func $do_hget (export "do_hget") (result i64)
                    i32.const 0
                    i32.const 4
                    i32.const 4
                    i32.const 5
                    call $kora_hget))
        "#;

        let module = wasmtime::Module::new(&wasm_engine, wat).expect("compile");
        let ctx = HostContext::new(shard_engine);
        let mut store = Store::new(&wasm_engine, ctx);
        store.set_fuel(1_000_000).expect("fuel");

        let instance = linker
            .instantiate(&mut store, &module)
            .expect("instantiate");

        let do_hset = instance
            .get_typed_func::<(), i32>(&mut store, "do_hset")
            .expect("get do_hset");
        let added = do_hset.call(&mut store, ()).expect("call do_hset");
        assert_eq!(added, 1);

        store.set_fuel(1_000_000).expect("reset fuel");
        let do_hget = instance
            .get_typed_func::<(), i64>(&mut store, "do_hget")
            .expect("get do_hget");
        let packed = do_hget.call(&mut store, ()).expect("call do_hget");
        assert_ne!(packed, -1);
        let ret_len = (packed & 0xFFFF_FFFF) as i32;
        assert_eq!(ret_len, 5);
    }
}
