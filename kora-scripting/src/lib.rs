//! # kora-scripting
//!
//! WASM scripting runtime for Kora.
//!
//! Allows server-side functions written in any WASM-targeting language (Rust,
//! Go, TypeScript, etc.) with fuel-metered execution and memory sandboxing.

#![warn(clippy::all)]
#![warn(missing_docs)]

use std::collections::HashMap;
use std::sync::Arc;

use thiserror::Error;
use tracing::{debug, info, warn};
use wasmtime::{Config, Engine, Func, Instance, Module, Store, Trap, Val};

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
pub struct FunctionRegistry {
    runtime: Arc<WasmRuntime>,
    modules: HashMap<String, Module>,
}

impl FunctionRegistry {
    /// Create a new, empty function registry backed by the given runtime.
    pub fn new(runtime: Arc<WasmRuntime>) -> Self {
        Self {
            runtime,
            modules: HashMap::new(),
        }
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
    /// Returns the `i64` results produced by the function.
    pub fn call(&self, name: &str, args: &[i64]) -> Result<Vec<i64>, ScriptError> {
        let module = self
            .modules
            .get(name)
            .ok_or_else(|| ScriptError::NotFound(name.to_string()))?;

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

        // Try "execute" first, then fall back to function named `name`.
        let func: Func = instance
            .get_func(&mut store, "execute")
            .or_else(|| instance.get_func(&mut store, name))
            .ok_or_else(|| {
                ScriptError::ExecutionError(format!(
                    "module has no export named \"execute\" or \"{name}\""
                ))
            })?;

        let params: Vec<Val> = args.iter().map(|&v| Val::I64(v)).collect();
        let ty = func.ty(&store);
        let result_count = ty.results().len();
        let mut results = vec![Val::I64(0); result_count];

        debug!(name, ?args, "calling WASM function");

        func.call(&mut store, &params, &mut results).map_err(|e| {
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

    /// WAT source for a module exporting `execute(i64, i64) -> i64` that adds
    /// its two parameters.
    const ADD_WAT: &str = r#"
        (module
          (func $execute (param i64 i64) (result i64)
            local.get 0
            local.get 1
            i64.add)
          (export "execute" (func $execute)))
    "#;

    /// WAT source for a module with an infinite loop (for fuel exhaustion).
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
        let mut registry = make_registry(100); // very low fuel
        registry
            .register("looper", LOOP_WAT.as_bytes())
            .expect("register should succeed");

        let err = registry.call("looper", &[0, 0]).unwrap_err();
        assert!(
            matches!(err, ScriptError::FuelExhausted),
            "expected FuelExhausted, got: {err:?}"
        );
    }
}
