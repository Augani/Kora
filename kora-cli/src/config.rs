//! Configuration file parsing for Kōra.
//!
//! Supports TOML configuration files with CLI argument overrides.
//!
//! ## Example config file (kora.toml)
//!
//! ```toml
//! bind = "0.0.0.0"
//! port = 6379
//! workers = 8
//! log_level = "info"
//!
//! [storage]
//! data_dir = "/var/lib/kora"
//! wal_sync = "every_second"   # every_write | every_second | os_managed
//! ```

use std::path::Path;

use serde::Deserialize;

/// Configuration loaded from a TOML file.
#[derive(Debug, Deserialize, Default)]
#[serde(default)]
pub struct FileConfig {
    /// Address to bind to.
    pub bind: Option<String>,
    /// Port to listen on.
    pub port: Option<u16>,
    /// Number of worker threads.
    pub workers: Option<usize>,
    /// Log level.
    pub log_level: Option<String>,
    /// Storage configuration.
    pub storage: StorageFileConfig,
    /// CDC ring buffer capacity per shard (0 = disabled).
    pub cdc_capacity: Option<usize>,
    /// WASM scripting fuel budget (0 = disabled).
    pub script_max_fuel: Option<u64>,
    /// Port for Prometheus metrics HTTP endpoint (0 = disabled).
    pub metrics_port: Option<u16>,
}

/// Storage section of the config file.
#[derive(Debug, Deserialize, Default)]
#[serde(default)]
pub struct StorageFileConfig {
    /// Data directory for WAL, RDB, and cold storage.
    pub data_dir: Option<String>,
    /// WAL sync policy: "every_write", "every_second", or "os_managed".
    pub wal_sync: Option<String>,
    /// Enable WAL.
    pub wal_enabled: Option<bool>,
    /// Enable RDB snapshots.
    pub rdb_enabled: Option<bool>,
    /// Maximum WAL size in bytes before auto-rotation.
    pub wal_max_bytes: Option<u64>,
}

impl FileConfig {
    /// Load a config file from the given path. Returns default if file doesn't exist.
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }

        let content = std::fs::read_to_string(path)?;
        let config: FileConfig = toml::from_str(&content)?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_full_config() {
        let toml = r#"
bind = "0.0.0.0"
port = 7379
workers = 8
log_level = "debug"

[storage]
data_dir = "/var/lib/kora"
wal_sync = "every_write"
wal_enabled = true
rdb_enabled = true
wal_max_bytes = 134217728
"#;
        let config: FileConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.bind, Some("0.0.0.0".into()));
        assert_eq!(config.port, Some(7379));
        assert_eq!(config.workers, Some(8));
        assert_eq!(config.log_level, Some("debug".into()));
        assert_eq!(config.storage.data_dir, Some("/var/lib/kora".into()));
        assert_eq!(config.storage.wal_sync, Some("every_write".into()));
        assert_eq!(config.storage.wal_max_bytes, Some(134217728));
    }

    #[test]
    fn test_parse_minimal_config() {
        let toml = "port = 8080\n";
        let config: FileConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.port, Some(8080));
        assert_eq!(config.bind, None);
        assert_eq!(config.workers, None);
    }

    #[test]
    fn test_parse_empty_config() {
        let config: FileConfig = toml::from_str("").unwrap();
        assert_eq!(config.port, None);
        assert_eq!(config.bind, None);
    }

    #[test]
    fn test_load_nonexistent() {
        let config = FileConfig::load(Path::new("/nonexistent/kora.toml")).unwrap();
        assert_eq!(config.port, None);
    }
}
