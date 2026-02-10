//! Error types for d1-sync

use thiserror::Error;

#[derive(Error, Debug)]
pub enum SyncError {
    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Local database error: {0}")]
    LocalDb(#[from] rusqlite::Error),

    #[error("Remote API error: {0}")]
    Remote(String),

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

    /// Reserved for strict conflict detection mode
    #[allow(dead_code)]
    #[error("Sync conflict on table {table}, row {pk}: local={local_ts}, remote={remote_ts}")]
    Conflict {
        table: String,
        pk: String,
        local_ts: String,
        remote_ts: String,
    },

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

    #[allow(dead_code)]
    #[error("Invalid table name '{name}'. Available tables: [{available}]")]
    InvalidTableName { name: String, available: String },

    #[error("Failed to query database schema: {0}")]
    #[allow(dead_code)]
    SchemaQueryFailed(String),
}

impl SyncError {
    /// Check if this error is retryable with exponential backoff.
    ///
    /// Retryable errors:
    /// - 429 rate limits
    /// - 5xx server errors
    /// - Connection timeouts
    /// - Network connectivity issues
    pub fn is_retryable(&self) -> bool {
        match self {
            SyncError::RateLimited { .. } => true,
            SyncError::ServerError { status, .. } if *status >= 500 => true,
            SyncError::ConnectionTimeout => true,
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
    fn test_retry_exhausted_display() {
        let err = SyncError::RetryExhausted {
            attempts: 5,
            last_error: "Server error (HTTP 503): Service Unavailable".to_string(),
        };
        let msg = format!("{}", err);
        assert!(msg.contains("5 attempts"));
        assert!(msg.contains("503"));
    }
}
