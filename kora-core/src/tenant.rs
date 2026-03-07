//! Multi-tenancy support for the Kōra cache engine.
//!
//! Provides tenant isolation through per-tenant resource limits, rate limiting,
//! command blocking, and resource accounting.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use parking_lot::RwLock;

/// Unique tenant identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TenantId(pub u32);

/// Resource limits and permissions for a tenant.
#[derive(Debug, Clone)]
pub struct TenantConfig {
    /// Maximum memory usage in bytes allowed for this tenant.
    pub max_memory_bytes: usize,
    /// Maximum number of keys this tenant may store.
    pub max_keys: usize,
    /// Optional operations-per-second rate limit.
    pub ops_per_second: Option<u32>,
    /// Commands that this tenant is not permitted to execute.
    pub blocked_commands: Vec<String>,
}

impl Default for TenantConfig {
    fn default() -> Self {
        Self {
            max_memory_bytes: 1024 * 1024 * 1024, // 1 GiB
            max_keys: 10_000_000,
            ops_per_second: None,
            blocked_commands: Vec::new(),
        }
    }
}

/// Per-tenant resource accounting.
///
/// All counters use atomic operations so that accounting can be updated
/// concurrently from multiple shard workers without locking.
pub struct TenantAccounting {
    /// Current number of keys owned by this tenant.
    pub key_count: AtomicU64,
    /// Current memory usage in bytes for this tenant.
    pub memory_bytes: AtomicU64,
    /// Total number of requests recorded in the current rate-limit window.
    pub request_count: AtomicU64,
    /// Start of the current rate-limit window.
    pub window_start: RwLock<Instant>,
}

impl TenantAccounting {
    /// Creates a new, zeroed accounting instance.
    pub fn new() -> Self {
        Self {
            key_count: AtomicU64::new(0),
            memory_bytes: AtomicU64::new(0),
            request_count: AtomicU64::new(0),
            window_start: RwLock::new(Instant::now()),
        }
    }
}

impl Default for TenantAccounting {
    fn default() -> Self {
        Self::new()
    }
}

/// Errors that can occur during tenant operations.
#[derive(Debug, thiserror::Error)]
pub enum TenantError {
    /// The specified tenant does not exist in the registry.
    #[error("tenant not found: {0}")]
    NotFound(u32),
    /// The tenant has reached its maximum key count.
    #[error("key limit exceeded for tenant {0}: max {1}")]
    KeyLimitExceeded(u32, usize),
    /// The tenant has reached its maximum memory allocation.
    #[error("memory limit exceeded for tenant {0}: max {1} bytes")]
    MemoryLimitExceeded(u32, usize),
    /// The tenant has exceeded its operations-per-second rate limit.
    #[error("rate limit exceeded for tenant {0}")]
    RateLimitExceeded(u32),
    /// The tenant is not permitted to execute the given command.
    #[error("command '{1}' blocked for tenant {0}")]
    CommandBlocked(u32, String),
}

/// Registry of tenant configurations and their accounting.
///
/// Provides methods for registering tenants, enforcing resource limits,
/// recording operations, and updating accounting counters.
pub struct TenantRegistry {
    tenants: HashMap<TenantId, (TenantConfig, TenantAccounting)>,
}

impl TenantRegistry {
    /// Creates an empty tenant registry.
    pub fn new() -> Self {
        Self {
            tenants: HashMap::new(),
        }
    }

    /// Registers a tenant with the given configuration.
    ///
    /// If the tenant already exists, its configuration is replaced and
    /// accounting is reset.
    pub fn register(&mut self, id: TenantId, config: TenantConfig) {
        self.tenants.insert(id, (config, TenantAccounting::new()));
    }

    /// Removes a tenant from the registry.
    ///
    /// Returns `true` if the tenant was present, `false` otherwise.
    pub fn unregister(&mut self, id: TenantId) -> bool {
        self.tenants.remove(&id).is_some()
    }

    /// Returns the configuration for a tenant, if it exists.
    pub fn get_config(&self, id: TenantId) -> Option<&TenantConfig> {
        self.tenants.get(&id).map(|(config, _)| config)
    }

    /// Checks whether a proposed change in key count and memory usage would
    /// stay within the tenant's configured limits.
    ///
    /// Also checks the rate limit if one is configured. Returns `Ok(())` if
    /// the operation is permitted, or an appropriate [`TenantError`] otherwise.
    pub fn check_limits(
        &self,
        id: TenantId,
        key_count_delta: i64,
        memory_delta: i64,
    ) -> Result<(), TenantError> {
        let (config, accounting) = self.tenants.get(&id).ok_or(TenantError::NotFound(id.0))?;

        // Check key limit
        let current_keys = accounting.key_count.load(Ordering::Relaxed) as i64;
        let projected_keys = current_keys.saturating_add(key_count_delta);
        if projected_keys > config.max_keys as i64 {
            return Err(TenantError::KeyLimitExceeded(id.0, config.max_keys));
        }

        // Check memory limit
        let current_memory = accounting.memory_bytes.load(Ordering::Relaxed) as i64;
        let projected_memory = current_memory.saturating_add(memory_delta);
        if projected_memory > config.max_memory_bytes as i64 {
            return Err(TenantError::MemoryLimitExceeded(
                id.0,
                config.max_memory_bytes,
            ));
        }

        // Check rate limit
        if let Some(max_ops) = config.ops_per_second {
            let now = Instant::now();
            let window_start = *accounting.window_start.read();
            let elapsed = now.duration_since(window_start);

            if elapsed.as_secs() >= 1 {
                // Window expired; the next `record_operation` call will reset it.
                // No rate-limit violation in a fresh window.
            } else {
                let current_ops = accounting.request_count.load(Ordering::Relaxed);
                if current_ops >= u64::from(max_ops) {
                    return Err(TenantError::RateLimitExceeded(id.0));
                }
            }
        }

        Ok(())
    }

    /// Records a single operation for rate-limiting purposes.
    ///
    /// Resets the counter when the one-second window has elapsed.
    /// If the tenant does not exist, this is a no-op.
    pub fn record_operation(&self, id: TenantId) {
        if let Some((_, accounting)) = self.tenants.get(&id) {
            let now = Instant::now();
            let should_reset = {
                let window_start = accounting.window_start.read();
                now.duration_since(*window_start).as_secs() >= 1
            };

            if should_reset {
                let mut window_start = accounting.window_start.write();
                // Double-check after acquiring write lock to avoid races.
                if now.duration_since(*window_start).as_secs() >= 1 {
                    *window_start = now;
                    accounting.request_count.store(1, Ordering::Relaxed);
                    return;
                }
            }

            accounting.request_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Returns `true` if the given command is permitted for the tenant.
    ///
    /// If the tenant is not registered, returns `true` (permissive default).
    pub fn is_command_allowed(&self, id: TenantId, command_name: &str) -> bool {
        match self.tenants.get(&id) {
            Some((config, _)) => {
                let upper = command_name.to_ascii_uppercase();
                !config
                    .blocked_commands
                    .iter()
                    .any(|blocked| blocked.eq_ignore_ascii_case(&upper))
            }
            None => true,
        }
    }

    /// Updates the accounting counters for a tenant.
    ///
    /// Positive deltas increase the counters; negative deltas decrease them.
    /// If the tenant does not exist, this is a no-op.
    pub fn update_accounting(&self, id: TenantId, key_count_delta: i64, memory_delta: i64) {
        if let Some((_, accounting)) = self.tenants.get(&id) {
            if key_count_delta >= 0 {
                accounting
                    .key_count
                    .fetch_add(key_count_delta as u64, Ordering::Relaxed);
            } else {
                let abs = key_count_delta.unsigned_abs();
                // Saturate at zero to avoid underflow.
                let _ = accounting.key_count.fetch_update(
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                    |current| Some(current.saturating_sub(abs)),
                );
            }

            if memory_delta >= 0 {
                accounting
                    .memory_bytes
                    .fetch_add(memory_delta as u64, Ordering::Relaxed);
            } else {
                let abs = memory_delta.unsigned_abs();
                let _ = accounting.memory_bytes.fetch_update(
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                    |current| Some(current.saturating_sub(abs)),
                );
            }
        }
    }
}

impl Default for TenantRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_get_config() {
        let mut registry = TenantRegistry::new();
        let id = TenantId(1);
        let config = TenantConfig {
            max_keys: 100,
            ..TenantConfig::default()
        };
        registry.register(id, config);

        let retrieved = registry.get_config(id);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().max_keys, 100);
    }

    #[test]
    fn unregister_existing_tenant() {
        let mut registry = TenantRegistry::new();
        let id = TenantId(42);
        registry.register(id, TenantConfig::default());

        assert!(registry.unregister(id));
        assert!(registry.get_config(id).is_none());
    }

    #[test]
    fn unregister_nonexistent_tenant() {
        let mut registry = TenantRegistry::new();
        assert!(!registry.unregister(TenantId(999)));
    }

    #[test]
    fn check_limits_key_count() {
        let mut registry = TenantRegistry::new();
        let id = TenantId(1);
        registry.register(
            id,
            TenantConfig {
                max_keys: 10,
                ..TenantConfig::default()
            },
        );

        // Within limit
        assert!(registry.check_limits(id, 10, 0).is_ok());

        // Exactly at limit after accounting for existing keys
        registry.update_accounting(id, 5, 0);
        assert!(registry.check_limits(id, 5, 0).is_ok());

        // Over limit
        assert!(registry.check_limits(id, 6, 0).is_err());
        match registry.check_limits(id, 6, 0) {
            Err(TenantError::KeyLimitExceeded(tid, max)) => {
                assert_eq!(tid, 1);
                assert_eq!(max, 10);
            }
            other => panic!("expected KeyLimitExceeded, got {:?}", other),
        }
    }

    #[test]
    fn check_limits_memory() {
        let mut registry = TenantRegistry::new();
        let id = TenantId(2);
        registry.register(
            id,
            TenantConfig {
                max_memory_bytes: 1024,
                ..TenantConfig::default()
            },
        );

        assert!(registry.check_limits(id, 0, 1024).is_ok());
        assert!(registry.check_limits(id, 0, 1025).is_err());

        match registry.check_limits(id, 0, 1025) {
            Err(TenantError::MemoryLimitExceeded(tid, max)) => {
                assert_eq!(tid, 2);
                assert_eq!(max, 1024);
            }
            other => panic!("expected MemoryLimitExceeded, got {:?}", other),
        }
    }

    #[test]
    fn check_limits_unknown_tenant() {
        let registry = TenantRegistry::new();
        match registry.check_limits(TenantId(99), 0, 0) {
            Err(TenantError::NotFound(tid)) => assert_eq!(tid, 99),
            other => panic!("expected NotFound, got {:?}", other),
        }
    }

    #[test]
    fn command_blocking() {
        let mut registry = TenantRegistry::new();
        let id = TenantId(1);
        registry.register(
            id,
            TenantConfig {
                blocked_commands: vec!["FLUSHDB".to_string(), "DEBUG".to_string()],
                ..TenantConfig::default()
            },
        );

        assert!(!registry.is_command_allowed(id, "flushdb"));
        assert!(!registry.is_command_allowed(id, "FLUSHDB"));
        assert!(!registry.is_command_allowed(id, "FlushDb"));
        assert!(registry.is_command_allowed(id, "GET"));
        assert!(registry.is_command_allowed(id, "SET"));
    }

    #[test]
    fn command_allowed_for_unknown_tenant() {
        let registry = TenantRegistry::new();
        assert!(registry.is_command_allowed(TenantId(999), "FLUSHDB"));
    }

    #[test]
    fn accounting_updates() {
        let mut registry = TenantRegistry::new();
        let id = TenantId(1);
        registry.register(id, TenantConfig::default());

        registry.update_accounting(id, 5, 1024);
        let (_, accounting) = registry.tenants.get(&id).unwrap();
        assert_eq!(accounting.key_count.load(Ordering::Relaxed), 5);
        assert_eq!(accounting.memory_bytes.load(Ordering::Relaxed), 1024);

        // Decrease
        registry.update_accounting(id, -3, -512);
        assert_eq!(accounting.key_count.load(Ordering::Relaxed), 2);
        assert_eq!(accounting.memory_bytes.load(Ordering::Relaxed), 512);
    }

    #[test]
    fn accounting_saturates_at_zero() {
        let mut registry = TenantRegistry::new();
        let id = TenantId(1);
        registry.register(id, TenantConfig::default());

        registry.update_accounting(id, 2, 100);
        registry.update_accounting(id, -10, -500);

        let (_, accounting) = registry.tenants.get(&id).unwrap();
        assert_eq!(accounting.key_count.load(Ordering::Relaxed), 0);
        assert_eq!(accounting.memory_bytes.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn record_operation_increments_counter() {
        let mut registry = TenantRegistry::new();
        let id = TenantId(1);
        registry.register(id, TenantConfig::default());

        registry.record_operation(id);
        registry.record_operation(id);
        registry.record_operation(id);

        let (_, accounting) = registry.tenants.get(&id).unwrap();
        assert_eq!(accounting.request_count.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn rate_limit_enforcement() {
        let mut registry = TenantRegistry::new();
        let id = TenantId(1);
        registry.register(
            id,
            TenantConfig {
                ops_per_second: Some(2),
                ..TenantConfig::default()
            },
        );

        // First two operations within limit
        registry.record_operation(id);
        registry.record_operation(id);

        // Third should be rejected by check_limits
        match registry.check_limits(id, 0, 0) {
            Err(TenantError::RateLimitExceeded(tid)) => assert_eq!(tid, 1),
            other => panic!("expected RateLimitExceeded, got {:?}", other),
        }
    }

    #[test]
    fn default_config_has_generous_limits() {
        let config = TenantConfig::default();
        assert_eq!(config.max_memory_bytes, 1024 * 1024 * 1024);
        assert_eq!(config.max_keys, 10_000_000);
        assert!(config.ops_per_second.is_none());
        assert!(config.blocked_commands.is_empty());
    }
}
