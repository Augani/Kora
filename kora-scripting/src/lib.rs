//! # kora-scripting
//!
//! WASM scripting runtime for Kora.
//!
//! Allows server-side functions written in any WASM-targeting language (Rust,
//! Go, TypeScript, etc.) with fuel-metered execution and memory sandboxing.
//!
//! Modules that import functions from the `"kora"` namespace can read/write
//! keys in the shard engine. See [`host`] for details.

#![warn(clippy::all)]
#![warn(missing_docs)]

pub mod host;

use std::collections::HashMap;
use std::sync::Arc;

use kora_core::shard::ShardEngine;
use thiserror::Error;
use tracing::{debug, info, warn};
use wasmtime::{Config, Engine, Func, Instance, Linker, Module, Store, Trap, Val};

use crate::host::{create_host_linker, HostContext};

/// Errors that can occur during WASM script compilation and execution.
#[derive(Debug, Error)]
pub enum ScriptError {
    /// WASM module failed to compile.
    #[error("compilation error: {0}")]
    CompileError(String),

    /// WASM function execution failed at runtime.
    #[error("execution error: {0}")]
    ExecutionError(String),

    /// The script exhausted its fuel budget.
    #[error("fuel exhausted: script exceeded its execution budget")]
    FuelExhausted,

    /// The requested function was not found in the registry.
    #[error("function not found: {0}")]
    NotFound(String),
}

/// A WASM execution runtime backed by `wasmtime::Engine` with fuel metering.
///
/// The engine is configured to consume fuel so that runaway scripts can be
/// terminated after a bounded number of instructions.
pub struct WasmRuntime {
    engine: Engine,
    max_fuel: u64,
}

impl WasmRuntime {
    /// Create a new runtime with the given maximum fuel budget per invocation.
    ///
    /// Fuel consumption is enabled on the underlying `wasmtime::Engine`.
    pub fn new(max_fuel: u64) -> Self {
        let mut config = Config::new();
        config.consume_fuel(true);

        let engine = Engine::new(&config).expect("failed to create wasmtime engine");

        info!(max_fuel, "created WasmRuntime with fuel metering");

        Self { engine, max_fuel }
    }

    /// Returns a reference to the underlying `wasmtime::Engine`.
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Returns the maximum fuel budget configured for this runtime.
    pub fn max_fuel(&self) -> u64 {
        self.max_fuel
    }
}

/// A registry of named WASM functions (compiled modules).
///
/// Each registered function is a compiled `wasmtime::Module`. When called, the
/// module is instantiated in a fresh `Store` with the configured fuel budget.
///
/// If a `ShardEngine` is attached via [`FunctionRegistry::set_engine`], modules
/// that import `"kora"` host functions can read/write keys in the store.
pub struct FunctionRegistry {
    runtime: Arc<WasmRuntime>,
    modules: HashMap<String, Module>,
    shard_engine: Option<Arc<ShardEngine>>,
    host_linker: Option<Linker<HostContext>>,
}

impl FunctionRegistry {
    /// Create a new, empty function registry backed by the given runtime.
    pub fn new(runtime: Arc<WasmRuntime>) -> Self {
        Self {
            runtime,
            modules: HashMap::new(),
            shard_engine: None,
            host_linker: None,
        }
    }

    /// Attach a shard engine, enabling host function imports for WASM modules.
    ///
    /// This creates an internal `Linker` with all `kora_*` host functions registered.
    pub fn set_engine(&mut self, engine: Arc<ShardEngine>) -> Result<(), ScriptError> {
        let linker = create_host_linker(self.runtime.engine())
            .map_err(|e| ScriptError::ExecutionError(e.to_string()))?;
        self.shard_engine = Some(engine);
        self.host_linker = Some(linker);
        Ok(())
    }

    /// Returns `true` if a shard engine is attached for host function support.
    pub fn has_engine(&self) -> bool {
        self.shard_engine.is_some()
    }

    /// Compile and register a WASM module under the given name.
    ///
    /// The `wasm_bytes` may be either the binary WASM format or WAT text format.
    /// Returns an error if compilation fails.
    pub fn register(&mut self, name: &str, wasm_bytes: &[u8]) -> Result<(), ScriptError> {
        debug!(name, "compiling WASM module");

        let module = Module::new(self.runtime.engine(), wasm_bytes)
            .map_err(|e| ScriptError::CompileError(e.to_string()))?;

        self.modules.insert(name.to_string(), module);

        info!(name, "registered WASM function");
        Ok(())
    }

    /// Invoke a registered function by name with the given `i64` arguments.
    ///
    /// The function is looked up by first trying the `"execute"` export, then
    /// falling back to an export matching `name`. The module is instantiated in
    /// a fresh store with the runtime's fuel budget.
    ///
    /// If a shard engine is attached, host functions are available to the module.
    ///
    /// Returns the `i64` results produced by the function.
    pub fn call(&self, name: &str, args: &[i64]) -> Result<Vec<i64>, ScriptError> {
        let module = self
            .modules
            .get(name)
            .ok_or_else(|| ScriptError::NotFound(name.to_string()))?;

        let params: Vec<Val> = args.iter().map(|&v| Val::I64(v)).collect();

        if let (Some(ref linker), Some(ref shard_engine)) = (&self.host_linker, &self.shard_engine)
        {
            let ctx = HostContext::new(shard_engine.clone());
            let mut store = Store::new(self.runtime.engine(), ctx);
            store
                .set_fuel(self.runtime.max_fuel())
                .map_err(|e| ScriptError::ExecutionError(e.to_string()))?;

            let instance = linker.instantiate(&mut store, module).map_err(|e| {
                if e.downcast_ref::<Trap>() == Some(&Trap::OutOfFuel) {
                    return ScriptError::FuelExhausted;
                }
                ScriptError::ExecutionError(e.to_string())
            })?;

            return self.invoke_func(&mut store, &instance, name, &params);
        }

        let mut store: Store<()> = Store::new(self.runtime.engine(), ());
        store
            .set_fuel(self.runtime.max_fuel())
            .map_err(|e| ScriptError::ExecutionError(e.to_string()))?;

        let instance = Instance::new(&mut store, module, &[]).map_err(|e| {
            if e.downcast_ref::<Trap>() == Some(&Trap::OutOfFuel) {
                return ScriptError::FuelExhausted;
            }
            ScriptError::ExecutionError(e.to_string())
        })?;

        self.invoke_func(&mut store, &instance, name, &params)
    }

    /// Invoke a registered function with byte arguments written to WASM linear memory.
    ///
    /// Each byte argument is written into the module's linear memory and passed
    /// as `(ptr, len)` pairs of `i32` values. Requires a shard engine to be attached.
    pub fn call_with_byte_args(
        &self,
        name: &str,
        byte_args: &[Vec<u8>],
    ) -> Result<Vec<i64>, ScriptError> {
        let module = self
            .modules
            .get(name)
            .ok_or_else(|| ScriptError::NotFound(name.to_string()))?;

        let (linker, shard_engine) = match (&self.host_linker, &self.shard_engine) {
            (Some(l), Some(e)) => (l, e),
            _ => {
                return Err(ScriptError::ExecutionError(
                    "byte args require a shard engine to be attached".into(),
                ))
            }
        };

        let ctx = HostContext::new(shard_engine.clone());
        let mut store = Store::new(self.runtime.engine(), ctx);
        store
            .set_fuel(self.runtime.max_fuel())
            .map_err(|e| ScriptError::ExecutionError(e.to_string()))?;

        let instance = linker.instantiate(&mut store, module).map_err(|e| {
            if e.downcast_ref::<Trap>() == Some(&Trap::OutOfFuel) {
                return ScriptError::FuelExhausted;
            }
            ScriptError::ExecutionError(e.to_string())
        })?;

        let alloc_fn = instance.get_func(&mut store, "kora_alloc").ok_or_else(|| {
            ScriptError::ExecutionError("module must export kora_alloc for byte args".into())
        })?;

        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| ScriptError::ExecutionError("module must export memory".into()))?;

        let mut params: Vec<Val> = Vec::with_capacity(byte_args.len() * 2);
        for arg in byte_args {
            let mut alloc_result = [Val::I32(0)];
            alloc_fn
                .call(&mut store, &[Val::I32(arg.len() as i32)], &mut alloc_result)
                .map_err(|e| ScriptError::ExecutionError(e.to_string()))?;

            let ptr = match &alloc_result[0] {
                Val::I32(p) => *p,
                _ => {
                    return Err(ScriptError::ExecutionError(
                        "kora_alloc returned non-i32".into(),
                    ))
                }
            };

            let start = ptr as usize;
            let end = start + arg.len();
            let mem_data = memory.data_mut(&mut store);
            if end > mem_data.len() {
                return Err(ScriptError::ExecutionError(
                    "allocated memory out of bounds".into(),
                ));
            }
            mem_data[start..end].copy_from_slice(arg);

            params.push(Val::I32(ptr));
            params.push(Val::I32(arg.len() as i32));
        }

        self.invoke_func(&mut store, &instance, name, &params)
    }

    fn invoke_func<T>(
        &self,
        store: &mut Store<T>,
        instance: &Instance,
        name: &str,
        params: &[Val],
    ) -> Result<Vec<i64>, ScriptError> {
        let func: Func = instance
            .get_func(&mut *store, "execute")
            .or_else(|| instance.get_func(&mut *store, name))
            .ok_or_else(|| {
                ScriptError::ExecutionError(format!(
                    "module has no export named \"execute\" or \"{name}\""
                ))
            })?;

        let ty = func.ty(&*store);
        let result_count = ty.results().len();
        let mut results = vec![Val::I64(0); result_count];

        debug!(name, "calling WASM function");

        func.call(&mut *store, params, &mut results).map_err(|e| {
            if e.downcast_ref::<Trap>() == Some(&Trap::OutOfFuel) {
                warn!(name, "function exhausted fuel budget");
                return ScriptError::FuelExhausted;
            }
            ScriptError::ExecutionError(e.to_string())
        })?;

        let i64_results: Vec<i64> = results
            .iter()
            .map(|v| match v {
                Val::I64(n) => *n,
                Val::I32(n) => *n as i64,
                other => {
                    warn!(?other, "unexpected return type, coercing to 0");
                    0
                }
            })
            .collect();

        debug!(name, ?i64_results, "WASM function returned");
        Ok(i64_results)
    }

    /// Remove a function from the registry. Returns `true` if it existed.
    pub fn remove(&mut self, name: &str) -> bool {
        let removed = self.modules.remove(name).is_some();
        if removed {
            info!(name, "removed WASM function");
        }
        removed
    }

    /// List the names of all registered functions.
    pub fn list(&self) -> Vec<String> {
        self.modules.keys().cloned().collect()
    }

    /// Check whether a function with the given name is registered.
    pub fn contains(&self, name: &str) -> bool {
        self.modules.contains_key(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ADD_WAT: &str = r#"
        (module
          (func $execute (param i64 i64) (result i64)
            local.get 0
            local.get 1
            i64.add)
          (export "execute" (func $execute)))
    "#;

    const LOOP_WAT: &str = r#"
        (module
          (func $execute (param i64 i64) (result i64)
            (loop $l (br $l))
            i64.const 0)
          (export "execute" (func $execute)))
    "#;

    fn make_registry(max_fuel: u64) -> FunctionRegistry {
        let runtime = Arc::new(WasmRuntime::new(max_fuel));
        FunctionRegistry::new(runtime)
    }

    #[test]
    fn test_register_and_call() {
        let mut registry = make_registry(100_000);
        registry
            .register("add", ADD_WAT.as_bytes())
            .expect("register should succeed");

        let result = registry.call("add", &[3, 7]).expect("call should succeed");
        assert_eq!(result, vec![10]);
    }

    #[test]
    fn test_not_found() {
        let registry = make_registry(100_000);
        let err = registry.call("missing", &[]).unwrap_err();
        assert!(
            matches!(err, ScriptError::NotFound(ref name) if name == "missing"),
            "expected NotFound, got: {err:?}"
        );
    }

    #[test]
    fn test_remove() {
        let mut registry = make_registry(100_000);
        registry
            .register("add", ADD_WAT.as_bytes())
            .expect("register should succeed");

        assert!(registry.contains("add"));
        assert!(registry.remove("add"));
        assert!(!registry.contains("add"));
        assert!(!registry.remove("add"));
    }

    #[test]
    fn test_list_functions() {
        let mut registry = make_registry(100_000);
        registry
            .register("add", ADD_WAT.as_bytes())
            .expect("register add");
        registry
            .register("add2", ADD_WAT.as_bytes())
            .expect("register add2");

        let mut names = registry.list();
        names.sort();
        assert_eq!(names, vec!["add", "add2"]);
    }

    #[test]
    fn test_invalid_wasm() {
        let mut registry = make_registry(100_000);
        let err = registry.register("bad", b"this is not wasm").unwrap_err();
        assert!(
            matches!(err, ScriptError::CompileError(_)),
            "expected CompileError, got: {err:?}"
        );
    }

    #[test]
    fn test_fuel_exhaustion() {
        let mut registry = make_registry(100);
        registry
            .register("looper", LOOP_WAT.as_bytes())
            .expect("register should succeed");

        let err = registry.call("looper", &[0, 0]).unwrap_err();
        assert!(
            matches!(err, ScriptError::FuelExhausted),
            "expected FuelExhausted, got: {err:?}"
        );
    }

    #[test]
    fn test_call_with_engine_attached() {
        let shard_engine = Arc::new(ShardEngine::new(2));
        let runtime = Arc::new(WasmRuntime::new(1_000_000));
        let mut registry = FunctionRegistry::new(runtime);
        registry.set_engine(shard_engine).expect("set engine");

        registry
            .register("add", ADD_WAT.as_bytes())
            .expect("register");
        let result = registry.call("add", &[5, 3]).expect("call");
        assert_eq!(result, vec![8]);
    }

    #[test]
    fn test_call_with_host_functions() {
        let shard_engine = Arc::new(ShardEngine::new(2));
        let runtime = Arc::new(WasmRuntime::new(1_000_000));
        let mut registry = FunctionRegistry::new(runtime);
        registry
            .set_engine(shard_engine.clone())
            .expect("set engine");

        let wat = r#"
            (module
                (import "kora" "kora_set" (func $kora_set (param i32 i32 i32 i32) (result i32)))
                (memory (export "memory") 1)
                (data (i32.const 0) "testkey")
                (data (i32.const 7) "testval")
                (func $execute (export "execute") (result i64)
                    i32.const 0  ;; key_ptr
                    i32.const 7  ;; key_len
                    i32.const 7  ;; val_ptr
                    i32.const 7  ;; val_len
                    call $kora_set
                    i64.extend_i32_s))
        "#;

        registry
            .register("setter", wat.as_bytes())
            .expect("register");
        let result = registry.call("setter", &[]).expect("call");
        assert_eq!(result, vec![1]);

        use kora_core::command::{Command, CommandResponse};
        let resp = shard_engine.dispatch_blocking(Command::Get {
            key: b"testkey".to_vec(),
        });
        assert!(matches!(resp, CommandResponse::BulkString(v) if v == b"testval"));
    }
}
