//! Configuration loading from TOML

use crate::error::{Result, SyncError};
use serde::Deserialize;
use std::path::Path;
use tracing::{debug, info};

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub cloudflare_account_id: String,
    pub cloudflare_api_token: String,
    pub database_id: String,

    /// Path to local SQLite database (optional - auto-detected from wrangler if not set)
    pub local_db: Option<String>,

    #[serde(default)]
    pub sync: SyncConfig,

    /// Optional stash config for S3-compatible relay sync
    pub stash: Option<StashConfig>,
}

/// Configuration for S3-compatible relay sync (stash/retrieve).
///
/// Supports S3, R2, GCS, Azure, and local filesystem URLs.
#[derive(Debug, Clone, Deserialize)]
pub struct StashConfig {
    /// Object store URL: s3://bucket/path/relay.sqlite, file:///local/path, etc.
    pub url: String,

    /// AWS access key ID (optional if using instance roles or env vars)
    pub access_key_id: Option<String>,

    /// AWS secret access key
    pub secret_access_key: Option<String>,

    /// AWS region (default: us-east-1)
    pub region: Option<String>,

    /// Custom endpoint for R2, MinIO, etc.
    pub endpoint: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SyncConfig {
    /// Tables to sync (empty = all non-excluded tables)
    #[serde(default)]
    pub tables: Vec<String>,

    /// Tables to always exclude
    #[serde(default = "default_exclude_tables")]
    pub exclude_tables: Vec<String>,

    /// Column used for timestamp-based change detection
    #[serde(default = "default_timestamp_column")]
    pub timestamp_column: String,

    /// How to resolve conflicts
    #[serde(default)]
    pub conflict_resolution: ConflictResolution,

    /// Maximum number of retry attempts for transient failures
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    /// Initial delay in milliseconds before first retry (doubles each attempt)
    #[serde(default = "default_initial_retry_delay_ms")]
    pub initial_retry_delay_ms: u64,

    /// Maximum delay in milliseconds between retries (cap for exponential backoff)
    #[serde(default = "default_max_retry_delay_ms")]
    pub max_retry_delay_ms: u64,

    /// Backoff multiplier for exponential backoff (default: 2.0)
    #[serde(default = "default_backoff_multiplier")]
    pub backoff_multiplier: f64,

    /// Maximum number of rows per batch for upsert operations
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Maximum bytes per SQL statement (D1 has limits)
    #[serde(default = "default_max_statement_bytes")]
    pub max_statement_bytes: usize,
}

fn default_batch_size() -> usize {
    100
}

fn default_max_statement_bytes() -> usize {
    // D1 has a 100KB limit per statement, use 90KB to be safe
    90 * 1024
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            tables: Vec::new(),
            exclude_tables: default_exclude_tables(),
            timestamp_column: default_timestamp_column(),
            conflict_resolution: ConflictResolution::default(),
            max_retries: default_max_retries(),
            initial_retry_delay_ms: default_initial_retry_delay_ms(),
            max_retry_delay_ms: default_max_retry_delay_ms(),
            backoff_multiplier: default_backoff_multiplier(),
            batch_size: default_batch_size(),
            max_statement_bytes: default_max_statement_bytes(),
        }
    }
}

/// Configuration for batch operations
#[derive(Debug, Clone, Copy)]
pub struct BatchConfig {
    /// Maximum number of rows per batch
    pub batch_size: usize,
    /// Maximum bytes per SQL statement
    pub max_statement_bytes: usize,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            batch_size: default_batch_size(),
            max_statement_bytes: default_max_statement_bytes(),
        }
    }
}

impl BatchConfig {
    /// Create BatchConfig from SyncConfig
    #[allow(dead_code)]
    pub fn from_sync_config(sync: &SyncConfig) -> Self {
        Self {
            batch_size: sync.batch_size,
            max_statement_bytes: sync.max_statement_bytes,
        }
    }
}

fn default_max_retries() -> u32 {
    5
}

fn default_initial_retry_delay_ms() -> u64 {
    100 // per issue #3 spec
}

fn default_max_retry_delay_ms() -> u64 {
    30_000 // per issue #3 spec
}

fn default_backoff_multiplier() -> f64 {
    2.0
}

fn default_exclude_tables() -> Vec<String> {
    vec![
        "sqlite_sequence".to_string(),
        "_cf_KV".to_string(),
        "__drizzle_migrations".to_string(),
    ]
}

fn default_timestamp_column() -> String {
    "updated_at".to_string()
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[allow(clippy::enum_variant_names)]
pub enum ConflictResolution {
    /// Local changes always win
    #[default]
    LocalWins,
    /// Remote changes always win
    RemoteWins,
    /// Newer timestamp wins
    NewerWins,
}

/// Retry configuration for D1 API calls
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    pub max_retries: u32,
    /// Initial delay in milliseconds before first retry
    pub initial_delay_ms: u64,
    /// Maximum delay in milliseconds (cap for exponential backoff)
    pub max_delay_ms: u64,
    /// Backoff multiplier for exponential backoff
    pub backoff_multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: default_max_retries(),
            initial_delay_ms: default_initial_retry_delay_ms(),
            max_delay_ms: default_max_retry_delay_ms(),
            backoff_multiplier: default_backoff_multiplier(),
        }
    }
}

#[allow(dead_code)]
impl RetryConfig {
    /// Create RetryConfig from SyncConfig settings.
    ///
    /// Validates that backoff_multiplier >= 1.0 (clamps invalid values).
    pub fn from_sync_config(sync: &SyncConfig) -> Self {
        Self {
            max_retries: sync.max_retries.min(100), // cap at reasonable max
            initial_delay_ms: sync.initial_retry_delay_ms,
            max_delay_ms: sync.max_retry_delay_ms,
            // Ensure multiplier is at least 1.0 to avoid zero/negative delays
            backoff_multiplier: sync.backoff_multiplier.max(1.0),
        }
    }

    /// Calculate delay for a given attempt (0-indexed)
    pub fn delay_for_attempt(&self, attempt: u32) -> u64 {
        let delay = self.initial_delay_ms as f64 * self.backoff_multiplier.powi(attempt as i32);
        (delay as u64).min(self.max_delay_ms)
    }
}

impl Config {
    /// Load config from a TOML file
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            return Err(SyncError::ConfigNotFound(path.display().to_string()));
        }

        let content = std::fs::read_to_string(path)?;
        let mut config: Config =
            toml::from_str(&content).map_err(|e| SyncError::Config(e.to_string()))?;

        // Auto-detect local_db if not specified
        if config.local_db.is_none() {
            config.local_db = Some(detect_local_db()?);
        }

        Ok(config)
    }

    /// Get the local database path (guaranteed to be Some after load)
    pub fn local_db_path(&self) -> &str {
        self.local_db
            .as_deref()
            .expect("local_db should be set after load")
    }

    /// Check if a table should be synced
    pub fn should_sync_table(&self, table: &str) -> bool {
        // Always exclude certain tables
        if self.sync.exclude_tables.iter().any(|t| t == table) {
            return false;
        }

        // If specific tables are configured, only sync those
        if !self.sync.tables.is_empty() {
            return self.sync.tables.iter().any(|t| t == table);
        }

        // Otherwise sync all non-excluded tables
        true
    }

    /// Get retry configuration from sync settings
    #[allow(dead_code)]
    pub fn retry_config(&self) -> RetryConfig {
        RetryConfig::from_sync_config(&self.sync)
    }
}

/// Auto-detect the local D1 database from wrangler's state directory
fn detect_local_db() -> Result<String> {
    let miniflare_dir = Path::new(".wrangler/state/v3/d1/miniflare-D1DatabaseObject");

    if !miniflare_dir.exists() {
        return Err(SyncError::Config(
            "No .wrangler/state/v3/d1/miniflare-D1DatabaseObject directory found. Run 'wrangler dev' first.".to_string()
        ));
    }

    // Find the largest non-empty sqlite file (the active database)
    let mut best_file: Option<(std::path::PathBuf, u64)> = None;

    for entry in std::fs::read_dir(miniflare_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.extension().is_some_and(|ext| ext == "sqlite") {
            let metadata = std::fs::metadata(&path)?;
            let size = metadata.len();

            // Skip empty files
            if size == 0 {
                continue;
            }

            debug!("Found sqlite: {} ({} bytes)", path.display(), size);

            // Prefer larger files (more data = more likely the active one)
            if best_file
                .as_ref()
                .map_or(true, |(_, best_size)| size > *best_size)
            {
                best_file = Some((path, size));
            }
        }
    }

    match best_file {
        Some((path, size)) => {
            let path_str = path.display().to_string();
            info!(
                "Auto-detected local database: {} ({} bytes)",
                path_str, size
            );
            Ok(path_str)
        }
        None => Err(SyncError::Config(
            "No non-empty sqlite files found in wrangler state. Run 'wrangler dev' and make some queries first.".to_string()
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_excludes() {
        let config = Config {
            cloudflare_account_id: "test".into(),
            cloudflare_api_token: "test".into(),
            database_id: "test".into(),
            local_db: Some("test.db".into()),
            sync: SyncConfig::default(),
            stash: None,
        };

        assert!(!config.should_sync_table("sqlite_sequence"));
        assert!(!config.should_sync_table("_cf_KV"));
        assert!(config.should_sync_table("abilities"));
    }

    #[test]
    fn test_specific_tables() {
        let config = Config {
            cloudflare_account_id: "test".into(),
            cloudflare_api_token: "test".into(),
            database_id: "test".into(),
            local_db: Some("test.db".into()),
            sync: SyncConfig {
                tables: vec!["abilities".into(), "talents".into()],
                ..Default::default()
            },
            stash: None,
        };

        assert!(config.should_sync_table("abilities"));
        assert!(config.should_sync_table("talents"));
        assert!(!config.should_sync_table("disciplines"));
    }

    #[test]
    fn test_retry_config_defaults() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.initial_delay_ms, 100); // per issue #3 spec
        assert_eq!(config.max_delay_ms, 30_000); // per issue #3 spec
        assert_eq!(config.backoff_multiplier, 2.0);
    }

    #[test]
    fn test_backoff_multiplier_clamped() {
        let sync = SyncConfig {
            backoff_multiplier: 0.5, // invalid - less than 1.0
            ..Default::default()
        };
        let retry = RetryConfig::from_sync_config(&sync);
        assert_eq!(retry.backoff_multiplier, 1.0); // should be clamped to 1.0
    }

    #[test]
    fn test_max_retries_capped() {
        let sync = SyncConfig {
            max_retries: 1000, // unreasonably high
            ..Default::default()
        };
        let retry = RetryConfig::from_sync_config(&sync);
        assert_eq!(retry.max_retries, 100); // should be capped at 100
    }

    #[test]
    fn test_retry_config_from_sync_config() {
        let sync = SyncConfig {
            max_retries: 3,
            initial_retry_delay_ms: 500,
            max_retry_delay_ms: 30000,
            backoff_multiplier: 1.5,
            ..Default::default()
        };
        let retry = RetryConfig::from_sync_config(&sync);
        assert_eq!(retry.max_retries, 3);
        assert_eq!(retry.initial_delay_ms, 500);
        assert_eq!(retry.max_delay_ms, 30000);
        assert_eq!(retry.backoff_multiplier, 1.5);
    }

    #[test]
    fn test_delay_for_attempt_exponential() {
        let config = RetryConfig {
            max_retries: 5,
            initial_delay_ms: 1000,
            max_delay_ms: 60000,
            backoff_multiplier: 2.0,
        };

        assert_eq!(config.delay_for_attempt(0), 1000); // 1000 * 2^0 = 1000
        assert_eq!(config.delay_for_attempt(1), 2000); // 1000 * 2^1 = 2000
        assert_eq!(config.delay_for_attempt(2), 4000); // 1000 * 2^2 = 4000
        assert_eq!(config.delay_for_attempt(3), 8000); // 1000 * 2^3 = 8000
        assert_eq!(config.delay_for_attempt(4), 16000); // 1000 * 2^4 = 16000
    }

    #[test]
    fn test_delay_capped_at_max() {
        let config = RetryConfig {
            max_retries: 10,
            initial_delay_ms: 1000,
            max_delay_ms: 5000,
            backoff_multiplier: 2.0,
        };

        assert_eq!(config.delay_for_attempt(0), 1000);
        assert_eq!(config.delay_for_attempt(1), 2000);
        assert_eq!(config.delay_for_attempt(2), 4000);
        assert_eq!(config.delay_for_attempt(3), 5000); // capped at max
        assert_eq!(config.delay_for_attempt(4), 5000); // still capped
    }

    #[test]
    fn test_config_retry_config() {
        let config = Config {
            cloudflare_account_id: "test".into(),
            cloudflare_api_token: "test".into(),
            database_id: "test".into(),
            local_db: Some("test.db".into()),
            sync: SyncConfig {
                max_retries: 10,
                initial_retry_delay_ms: 500,
                ..Default::default()
            },
            stash: None,
        };

        let retry = config.retry_config();
        assert_eq!(retry.max_retries, 10);
        assert_eq!(retry.initial_delay_ms, 500);
    }
}
