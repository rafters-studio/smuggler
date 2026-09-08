//! Error types for d1-sync

use thiserror::Error;

#[derive(Error, Debug)]
pub enum SyncError {
    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Config references unset environment variable ${{{0}}}")]
    ConfigEnvVar(String),

    #[cfg(feature = "native")]
    #[error("Local database error: {0}")]
    LocalDb(#[from] rusqlite::Error),

    #[error("Remote API error: {0}")]
    Remote(String),

    #[cfg(feature = "native")]
    #[error("HTTP request error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("No primary key found for table: {0}")]
    NoPrimaryKey(String),

    #[error("D1 API error: {message} (code: {code:?})")]
    D1Api { message: String, code: Option<i64> },

    #[error("Config file not found: {0}")]
    ConfigNotFound(String),

    /// HTTP 429 rate limit response
    #[error("Rate limited (HTTP 429){}", retry_after.map(|s| format!(", retry after {}s", s)).unwrap_or_default())]
    RateLimited { retry_after: Option<u64> },

    /// HTTP 5xx server error
    #[error("Server error (HTTP {status}): {message}")]
    ServerError { status: u16, message: String },

    /// Connection timeout
    #[error("Connection timeout")]
    ConnectionTimeout,

    /// HTTP 4xx client error (non-retryable)
    #[error("Bad request (HTTP {status}): {message}")]
    BadRequest { status: u16, message: String },

    /// Retry exhausted after max attempts
    #[error("Retry exhausted after {attempts} attempts: {last_error}")]
    RetryExhausted { attempts: u32, last_error: String },

    #[error("Invalid table name '{name}'. Available tables: [{available}]")]
    InvalidTableName { name: String, available: String },

    #[cfg(feature = "native")]
    #[error("Object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("Stash error: {0}")]
    Stash(String),

    #[error("Invalid URL: {0}")]
    InvalidUrl(String),

    #[error("Relay not found at {0}")]
    RelayNotFound(String),

    #[error(
        "Concurrent write conflict: the relay was modified by another machine \
         between download and upload. Re-run the stash command to download the \
         latest relay and merge."
    )]
    ConcurrentWrite,

    #[error(
        "D1 bind parameter limit exceeded for table '{table}': \
         query needs {} params ({row_count} rows x {col_count} columns), \
         but D1 allows at most {limit}. This is a smuggler bug -- please report it.",
        row_count * col_count
    )]
    ParamLimitExceeded {
        table: String,
        row_count: usize,
        col_count: usize,
        limit: usize,
    },

    #[error("Broadcast error: {0}")]
    Broadcast(String),

    #[error("Plugin error: {0}")]
    Plugin(String),

    /// A migrate-subsystem failure (manifest checksum mismatch, envelope
    /// open/seal failure, serialization). Bridges `migrate::MigrateError` into
    /// the crate's one error type. `MigrateError` is always compiled (no
    /// `native` gate), so this bridge exists on every target.
    #[error("Migrate error: {0}")]
    Migrate(#[from] crate::migrate::MigrateError),

    /// Two rows in one table render the same `__pk` text (#269).
    ///
    /// smugglr keys every sync path by the rendered primary key, so a metadata
    /// map can hold only one of them -- the second evicts the first, and the
    /// evicted row becomes invisible to the diff. Under the globally-unique-PK
    /// precondition that means two nodes minted the same key for different
    /// logical rows, so the sync refuses before any row is upserted rather than
    /// silently overwriting one with the other. Governed by
    /// [`crate::config::SyncConfig::duplicate_pk`]; carries both content hashes
    /// so the operator can identify which two rows collided.
    #[error(
        "duplicate primary key '{pk}' in table '{table}': two rows render the same __pk \
         (content hashes {first_hash} and {second_hash}). smugglr matches rows by primary \
         key, so continuing would silently drop one of them -- the cross-node data loss \
         that globally-unique primary keys exist to prevent. No rows were written. Give \
         the two rows distinct primary keys, or set [sync] duplicate_pk = \"warn\" to \
         restore the previous overwrite-and-continue behavior."
    )]
    DuplicatePrimaryKey {
        table: String,
        pk: String,
        first_hash: String,
        second_hash: String,
    },

    /// The migration ledger's chain-hash is broken -- an out-of-band
    /// `UPDATE`/`DELETE` altered or removed a `_smugglr_migrations` row. This is
    /// the resurrected `_journal.json` hand-edit failure the ledger exists to
    /// catch; it needs a human decision, so it shares the conflict exit code (4).
    #[error("Migration ledger tamper detected: {0}")]
    LedgerTampered(String),

    /// A plugin-side permanent conflict needing a human decision -- today
    /// this is exactly the duplicate-primary-key collision `adapter.rs`'s
    /// `get_row_metadata` refuses on an http-sql target, the plugin-side
    /// counterpart to [`SyncError::DuplicatePrimaryKey`] (#269, #444).
    ///
    /// The JSON-RPC wire carries only `{code, message}` -- not
    /// `DuplicatePrimaryKey`'s four typed fields (`table`, `pk`,
    /// `first_hash`, `second_hash`), which cannot be reconstructed from a
    /// `{code, message}` pair without parsing the message string, the exact
    /// string-matching anti-pattern #333 rejected. So this variant carries
    /// only what the wire can actually carry: which plugin, and its message.
    /// Shares `DuplicatePrimaryKey`'s exit code (4) in `exit_code()` -- both
    /// are "a human must reconcile data before this sync can proceed" -- but
    /// is a distinct variant, so `DuplicatePrimaryKey`'s existing mapping and
    /// the #181 regression test pinning `Plugin` to exit 1 are untouched.
    #[error("Plugin '{plugin}' reported a conflict: {message}")]
    PluginConflict { plugin: String, message: String },
}

impl SyncError {
    /// Check if this error is retryable with exponential backoff.
    ///
    /// This is the canonical retry classifier for `SyncError`: the native
    /// one-shot retry loop (`sync::upsert_with_retry`) and the daemon's
    /// `daemon::is_transient_error` both key off this method so the same
    /// error carries the same retry verdict everywhere (issue #226). Anyone
    /// adding a caller-specific delta (like the daemon's `ConcurrentWrite`
    /// carve-out) must build on top of this, not maintain a second
    /// independent classification.
    ///
    /// Retryable errors:
    /// - 429 rate limits
    /// - 5xx server errors
    /// - Connection timeouts
    /// - Network connectivity issues (HTTP timeout/connect failures)
    pub fn is_retryable(&self) -> bool {
        match self {
            SyncError::RateLimited { .. } => true,
            SyncError::ServerError { status, .. } if *status >= 500 => true,
            SyncError::ConnectionTimeout => true,
            #[cfg(feature = "native")]
            SyncError::Http(e) => e.is_timeout() || e.is_connect(),
            _ => false,
        }
    }

    /// Get the retry-after delay in milliseconds, if specified by the server.
    ///
    /// Returns `Some(ms)` for 429 responses with Retry-After header,
    /// `None` for other errors (use exponential backoff default).
    pub fn retry_after_ms(&self) -> Option<u64> {
        match self {
            // Use saturating_mul to prevent overflow on large retry_after values
            SyncError::RateLimited { retry_after } => retry_after.map(|s| s.saturating_mul(1000)),
            _ => None,
        }
    }
}

impl SyncError {
    /// Map error variants to structured exit codes for agent consumption.
    ///
    /// - 0: success (not an error)
    /// - 1: general/unknown error
    /// - 2: configuration error (don't retry, fix config)
    /// - 3: connection/network error (transient, safe to retry)
    /// - 4: conflict error (needs human decision)
    /// - 5: target not found (database missing, API unreachable)
    pub fn exit_code(&self) -> i32 {
        match self {
            SyncError::Config(_) | SyncError::ConfigNotFound(_) => 2,
            SyncError::ConfigEnvVar(_) => 2,
            SyncError::InvalidTableName { .. } | SyncError::NoPrimaryKey(_) => 2,
            SyncError::ParamLimitExceeded { .. } => 2,

            #[cfg(feature = "native")]
            SyncError::Http(_) => 3,
            SyncError::RateLimited { .. }
            | SyncError::ServerError { .. }
            | SyncError::ConnectionTimeout
            | SyncError::RetryExhausted { .. } => 3,

            // Migrate failures (checksum mismatch / tamper, envelope open
            // failure) need a human decision -- classify as conflict (4), the
            // same bucket the sequencing doc reserves for the migrate bridge.
            SyncError::ConcurrentWrite | SyncError::Migrate(_) | SyncError::LedgerTampered(_) => 4,

            // A duplicate `__pk` is data the operator must reconcile -- smugglr
            // cannot pick a winner without discarding a row -- so it shares the
            // conflict bucket (4) rather than reading as a config error. The
            // remedy is to re-key the rows, not to edit smugglr.toml.
            //
            // PluginConflict is the same remedy class arriving over the
            // plugin wire (#444) -- an explicit arm because exit_code() has a
            // `_ => 1` arm below: without it the variant would silently read
            // as general/unknown instead of conflict.
            SyncError::DuplicatePrimaryKey { .. } | SyncError::PluginConflict { .. } => 4,

            SyncError::TableNotFound(_)
            | SyncError::RelayNotFound(_)
            | SyncError::InvalidUrl(_) => 5,

            SyncError::D1Api { .. } | SyncError::BadRequest { .. } => 5,

            // Plugin/adapter failures are non-retryable and have no dedicated
            // class in the documented 0-5 contract, so they fold into the
            // general/unknown bucket rather than emitting an out-of-range code.
            SyncError::Plugin(_) => 1,

            _ => 1,
        }
    }
}

pub type Result<T> = std::result::Result<T, SyncError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limited_is_retryable() {
        let err = SyncError::RateLimited {
            retry_after: Some(30),
        };
        assert!(err.is_retryable());
        assert_eq!(err.retry_after_ms(), Some(30_000));
    }

    #[test]
    fn test_rate_limited_without_retry_after() {
        let err = SyncError::RateLimited { retry_after: None };
        assert!(err.is_retryable());
        assert_eq!(err.retry_after_ms(), None);
    }

    #[test]
    fn test_server_error_is_retryable() {
        let err = SyncError::ServerError {
            status: 503,
            message: "Service Unavailable".to_string(),
        };
        assert!(err.is_retryable());
        assert_eq!(err.retry_after_ms(), None);
    }

    #[test]
    fn test_connection_timeout_is_retryable() {
        let err = SyncError::ConnectionTimeout;
        assert!(err.is_retryable());
    }

    #[test]
    fn test_bad_request_not_retryable() {
        let err = SyncError::BadRequest {
            status: 400,
            message: "Invalid SQL".to_string(),
        };
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_config_error_not_retryable() {
        let err = SyncError::Config("bad config".to_string());
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_d1_api_error_not_retryable() {
        let err = SyncError::D1Api {
            message: "SQL syntax error".to_string(),
            code: Some(1),
        };
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_param_limit_exceeded_not_retryable() {
        let err = SyncError::ParamLimitExceeded {
            table: "wide_table".to_string(),
            row_count: 1,
            col_count: 150,
            limit: 100,
        };
        assert!(!err.is_retryable());
        let msg = format!("{}", err);
        assert!(msg.contains("wide_table"));
        assert!(msg.contains("150 params"));
        assert!(msg.contains("1 rows x 150 columns"));
    }

    #[test]
    fn test_retry_exhausted_display() {
        let err = SyncError::RetryExhausted {
            attempts: 5,
            last_error: "Server error (HTTP 503): Service Unavailable".to_string(),
        };
        let msg = format!("{}", err);
        assert!(msg.contains("5 attempts"));
        assert!(msg.contains("503"));
    }

    #[test]
    fn test_exit_code_config_errors() {
        assert_eq!(SyncError::Config("bad".into()).exit_code(), 2);
        assert_eq!(SyncError::ConfigNotFound("x".into()).exit_code(), 2);
        assert_eq!(
            SyncError::InvalidTableName {
                name: "x".into(),
                available: "a, b".into()
            }
            .exit_code(),
            2
        );
        assert_eq!(SyncError::NoPrimaryKey("t".into()).exit_code(), 2);
        assert_eq!(
            SyncError::ParamLimitExceeded {
                table: "t".into(),
                row_count: 1,
                col_count: 100,
                limit: 50
            }
            .exit_code(),
            2
        );
    }

    #[test]
    fn test_exit_code_network_errors() {
        assert_eq!(
            SyncError::RateLimited {
                retry_after: Some(30)
            }
            .exit_code(),
            3
        );
        assert_eq!(
            SyncError::ServerError {
                status: 503,
                message: "down".into()
            }
            .exit_code(),
            3
        );
        assert_eq!(SyncError::ConnectionTimeout.exit_code(), 3);
        assert_eq!(
            SyncError::RetryExhausted {
                attempts: 5,
                last_error: "err".into()
            }
            .exit_code(),
            3
        );
    }

    #[test]
    fn test_exit_code_conflict() {
        assert_eq!(SyncError::ConcurrentWrite.exit_code(), 4);
        assert_eq!(
            SyncError::LedgerTampered("broken chain".into()).exit_code(),
            4
        );
    }

    #[test]
    fn test_ledger_tampered_not_retryable() {
        assert!(!SyncError::LedgerTampered("x".into()).is_retryable());
    }

    #[test]
    fn test_exit_code_not_found() {
        assert_eq!(SyncError::TableNotFound("t".into()).exit_code(), 5);
        assert_eq!(SyncError::RelayNotFound("r".into()).exit_code(), 5);
        assert_eq!(SyncError::InvalidUrl("u".into()).exit_code(), 5);
        assert_eq!(
            SyncError::D1Api {
                message: "err".into(),
                code: None
            }
            .exit_code(),
            5
        );
        assert_eq!(
            SyncError::BadRequest {
                status: 400,
                message: "bad".into()
            }
            .exit_code(),
            5
        );
    }

    #[test]
    fn test_exit_code_duplicate_primary_key() {
        // #269: a duplicate __pk needs a human to re-key a row -- smugglr cannot
        // pick a winner without discarding data -- so it shares the conflict
        // bucket (4). Pinned because exit_code() has a `_ => 1` arm: without an
        // explicit case the variant would silently read as general/unknown.
        let err = SyncError::DuplicatePrimaryKey {
            table: "items".into(),
            pk: "1".into(),
            first_hash: "aaaa".into(),
            second_hash: "bbbb".into(),
        };
        assert_eq!(err.exit_code(), 4);
        // Re-running collides again on the same two rows, so retrying is never
        // productive. `is_retryable` has a `_ => false` arm; pin the verdict so a
        // future refactor cannot flip it into the retry loop.
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_exit_code_plugin_conflict() {
        // #444: a plugin-side duplicate-PK collision needs the same human
        // decision as the native path's DuplicatePrimaryKey, so it shares
        // exit code 4 -- pinned because exit_code() has a `_ => 1` arm:
        // without an explicit case the variant would silently read as
        // general/unknown.
        let err = SyncError::PluginConflict {
            plugin: "http-sql".into(),
            message: "duplicate primary key '1' in table 'items'".into(),
        };
        assert_eq!(err.exit_code(), 4);
        // Re-running collides again on the same two rows, so retrying is
        // never productive -- pin the verdict so a future refactor cannot
        // flip it into the retry loop.
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_exit_code_plugin() {
        // Plugin failures map into the documented 0-5 contract (general/1),
        // never an out-of-range code. Regression guard for #181.
        let code = SyncError::Plugin("err".into()).exit_code();
        assert_eq!(code, 1);
        assert!(
            (0..=5).contains(&code),
            "exit_code must stay within the documented 0-5 contract, got {code}"
        );
        assert!(!SyncError::Plugin("err".into()).is_retryable());
    }

    #[test]
    fn test_exit_code_general() {
        assert_eq!(SyncError::Remote("err".into()).exit_code(), 1);
        assert_eq!(SyncError::Stash("err".into()).exit_code(), 1);
    }
}
