//! Cloudflare D1 HTTP API client

use crate::config::RetryConfig;
use crate::error::{Result, SyncError};
use crate::local::RowMeta;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;
use tracing::{debug, info, warn};

/// D1 API client
pub struct D1Client {
    client: Client,
    account_id: String,
    database_id: String,
    api_token: String,
    retry_config: RetryConfig,
}

/// D1 query request
#[derive(Debug, Serialize)]
struct D1Request {
    sql: String,
    params: Vec<JsonValue>,
}

/// D1 API response
#[derive(Debug, Deserialize)]
struct D1Response {
    result: Option<Vec<D1Result>>,
    success: bool,
    errors: Option<Vec<D1Error>>,
}

#[derive(Debug, Deserialize)]
struct D1Result {
    results: Option<Vec<HashMap<String, JsonValue>>>,
    #[allow(dead_code)]
    success: bool,
    meta: Option<D1Meta>,
}

#[derive(Debug, Deserialize)]
struct D1Meta {
    #[allow(dead_code)]
    changed_db: Option<bool>,
    changes: Option<i64>,
    #[allow(dead_code)]
    duration: Option<f64>,
    #[allow(dead_code)]
    rows_read: Option<i64>,
    #[allow(dead_code)]
    rows_written: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct D1Error {
    code: Option<i64>,
    message: String,
}

/// Execute an async operation with exponential backoff retry.
///
/// Retries on:
/// - HTTP 429 (rate limited)
/// - HTTP 5xx (server errors)
/// - Connection timeouts
/// - Network errors
///
/// Respects Retry-After header when present.
async fn with_retry<F, Fut, T>(retry_config: &RetryConfig, operation: F) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut last_error: Option<SyncError> = None;

    for attempt in 0..=retry_config.max_retries {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(e) => {
                if !e.is_retryable() || attempt == retry_config.max_retries {
                    // Not retryable or exhausted retries
                    if attempt > 0 {
                        return Err(SyncError::RetryExhausted {
                            attempts: attempt.saturating_add(1),
                            last_error: e.to_string(),
                        });
                    }
                    return Err(e);
                }

                // Calculate delay: use Retry-After if provided, otherwise exponential backoff
                let delay_ms = e
                    .retry_after_ms()
                    .unwrap_or_else(|| retry_config.delay_for_attempt(attempt));

                warn!(
                    "Attempt {} failed: {}. Retrying in {}ms...",
                    attempt + 1,
                    e,
                    delay_ms
                );

                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                last_error = Some(e);
            }
        }
    }

    // Should not reach here, but handle it gracefully
    Err(last_error.unwrap_or_else(|| SyncError::Remote("Unknown retry error".to_string())))
}

/// Parse HTTP response status into appropriate SyncError.
///
/// `retry_after_header` should be the value of the HTTP Retry-After header if present.
fn parse_http_error(
    status: reqwest::StatusCode,
    body: &str,
    retry_after_header: Option<u64>,
) -> SyncError {
    let status_code = status.as_u16();
    let truncated_body = body.chars().take(500).collect::<String>();

    match status_code {
        429 => {
            // Prefer HTTP Retry-After header, fall back to JSON body
            let retry_after = retry_after_header.or_else(|| extract_retry_after(body));
            SyncError::RateLimited { retry_after }
        }
        400..=499 => SyncError::BadRequest {
            status: status_code,
            message: truncated_body,
        },
        500..=599 => SyncError::ServerError {
            status: status_code,
            message: truncated_body,
        },
        _ => SyncError::Remote(format!("HTTP {}: {}", status_code, truncated_body)),
    }
}

/// Extract Retry-After value from HTTP headers.
fn extract_retry_after_header(headers: &reqwest::header::HeaderMap) -> Option<u64> {
    headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
}

/// Try to extract Retry-After value from error response.
fn extract_retry_after(body: &str) -> Option<u64> {
    // Try to parse as JSON and look for retry_after field
    if let Ok(json) = serde_json::from_str::<serde_json::Value>(body) {
        if let Some(retry_after) = json.get("retry_after").and_then(|v| v.as_u64()) {
            return Some(retry_after);
        }
    }
    None
}

impl D1Client {
    /// Create a new D1 client with custom retry configuration
    pub fn with_retry_config(
        account_id: String,
        database_id: String,
        api_token: String,
        retry_config: RetryConfig,
    ) -> Self {
        Self {
            client: Client::new(),
            account_id,
            database_id,
            api_token,
            retry_config,
        }
    }

    /// Get the API endpoint URL
    fn endpoint(&self) -> String {
        format!(
            "https://api.cloudflare.com/client/v4/accounts/{}/d1/database/{}/query",
            self.account_id, self.database_id
        )
    }

    /// Execute a SQL query (internal, without retry)
    async fn query_internal(
        &self,
        sql: &str,
        params: Vec<JsonValue>,
    ) -> Result<Vec<HashMap<String, JsonValue>>> {
        let request = D1Request {
            sql: sql.to_string(),
            params,
        };

        debug!("D1 query: {}", sql);

        let response = self
            .client
            .post(self.endpoint())
            .bearer_auth(&self.api_token)
            .json(&request)
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    SyncError::ConnectionTimeout
                } else {
                    SyncError::Http(e)
                }
            })?;

        let status = response.status();
        // Extract Retry-After header before consuming response body
        let retry_after = extract_retry_after_header(response.headers());
        let body = response.text().await?;

        if !status.is_success() {
            return Err(parse_http_error(status, &body, retry_after));
        }

        let d1_response: D1Response = serde_json::from_str(&body)?;

        if !d1_response.success {
            if let Some(errors) = d1_response.errors {
                if let Some(err) = errors.first() {
                    return Err(SyncError::D1Api {
                        message: err.message.clone(),
                        code: err.code,
                    });
                }
            }
            return Err(SyncError::Remote("Unknown D1 error".to_string()));
        }

        let results = d1_response
            .result
            .and_then(|r| r.into_iter().next())
            .and_then(|r| r.results)
            .unwrap_or_default();

        Ok(results)
    }

    /// Execute a SQL query with retry
    async fn query(
        &self,
        sql: &str,
        params: Vec<JsonValue>,
    ) -> Result<Vec<HashMap<String, JsonValue>>> {
        let sql = sql.to_string();
        with_retry(&self.retry_config, || {
            let sql = sql.clone();
            let params = params.clone();
            async move { self.query_internal(&sql, params).await }
        })
        .await
    }

    /// Execute a write query (internal, without retry)
    async fn execute_internal(&self, sql: &str, params: Vec<JsonValue>) -> Result<i64> {
        let request = D1Request {
            sql: sql.to_string(),
            params,
        };

        debug!("D1 execute: {}", sql);

        let response = self
            .client
            .post(self.endpoint())
            .bearer_auth(&self.api_token)
            .json(&request)
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    SyncError::ConnectionTimeout
                } else {
                    SyncError::Http(e)
                }
            })?;

        let status = response.status();
        // Extract Retry-After header before consuming response body
        let retry_after = extract_retry_after_header(response.headers());
        let body = response.text().await?;

        if !status.is_success() {
            return Err(parse_http_error(status, &body, retry_after));
        }

        let d1_response: D1Response = serde_json::from_str(&body)?;

        if !d1_response.success {
            if let Some(errors) = d1_response.errors {
                if let Some(err) = errors.first() {
                    return Err(SyncError::D1Api {
                        message: err.message.clone(),
                        code: err.code,
                    });
                }
            }
            return Err(SyncError::Remote("Unknown D1 error".to_string()));
        }

        let changes = d1_response
            .result
            .and_then(|r| r.into_iter().next())
            .and_then(|r| r.meta)
            .and_then(|m| m.changes)
            .unwrap_or(0);

        Ok(changes)
    }

    /// Execute a write query (INSERT, UPDATE, DELETE) with retry
    async fn execute(&self, sql: &str, params: Vec<JsonValue>) -> Result<i64> {
        let sql = sql.to_string();
        with_retry(&self.retry_config, || {
            let sql = sql.clone();
            let params = params.clone();
            async move { self.execute_internal(&sql, params).await }
        })
        .await
    }

    /// Test connection to D1
    pub async fn test_connection(&self) -> Result<()> {
        info!("Testing D1 connection...");
        let results = self.query("SELECT 1 as test", vec![]).await?;
        if results.is_empty() {
            return Err(SyncError::Remote("Empty response from D1".to_string()));
        }
        info!("D1 connection successful");
        Ok(())
    }

    /// List all tables in D1
    pub async fn list_tables(&self) -> Result<Vec<String>> {
        let results = self
            .query(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
                vec![],
            )
            .await?;

        let tables: Vec<String> = results
            .into_iter()
            .filter_map(|row| {
                row.get("name")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .collect();

        debug!("Found {} tables in D1", tables.len());
        Ok(tables)
    }

    /// Get table column info
    pub async fn table_columns(&self, table: &str) -> Result<Vec<String>> {
        let results = self
            .query(&format!("PRAGMA table_info(\"{}\")", table), vec![])
            .await?;

        let columns: Vec<String> = results
            .into_iter()
            .filter_map(|row| {
                row.get("name")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .collect();

        Ok(columns)
    }

    /// Get primary key columns for a table
    pub async fn primary_key_columns(&self, table: &str) -> Result<Vec<String>> {
        let results = self
            .query(&format!("PRAGMA table_info(\"{}\")", table), vec![])
            .await?;

        let pk_cols: Vec<String> = results
            .into_iter()
            .filter(|row| {
                row.get("pk")
                    .and_then(|v| v.as_i64())
                    .map(|n| n > 0)
                    .unwrap_or(false)
            })
            .filter_map(|row| {
                row.get("name")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .collect();

        if pk_cols.is_empty() {
            return Err(SyncError::NoPrimaryKey(table.to_string()));
        }

        Ok(pk_cols)
    }

    /// Get row metadata for change detection
    pub async fn get_row_metadata(
        &self,
        table: &str,
        timestamp_column: &str,
    ) -> Result<HashMap<String, RowMeta>> {
        let pk_cols = self.primary_key_columns(table).await?;
        let columns = self.table_columns(table).await?;
        let has_timestamp = columns.iter().any(|c| c == timestamp_column);

        // Build the SELECT query
        let pk_select = pk_cols
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect::<Vec<_>>()
            .join(" || '|' || ");

        let all_cols = columns
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect::<Vec<_>>()
            .join(", ");

        let sql = format!(
            "SELECT {} as __pk, {} FROM \"{}\"",
            pk_select, all_cols, table
        );

        let results = self.query(&sql, vec![]).await?;
        let mut metadata = HashMap::new();

        for row in results {
            let pk_value = row
                .get("__pk")
                .and_then(|v| match v {
                    JsonValue::String(s) => Some(s.clone()),
                    JsonValue::Number(n) => Some(n.to_string()),
                    _ => None,
                })
                .unwrap_or_default();

            // Build content hash from columns (excluding timestamp columns)
            let timestamp_columns = ["updated_at", "created_at"];
            let mut hasher = Sha256::new();
            for col in &columns {
                // Skip timestamp columns in content hash
                if timestamp_columns.contains(&col.as_str()) {
                    continue;
                }
                let val = row
                    .get(col)
                    .map(|v| match v {
                        JsonValue::Null => String::new(),
                        JsonValue::String(s) => s.clone(),
                        _ => v.to_string(),
                    })
                    .unwrap_or_default();
                hasher.update(val.as_bytes());
                hasher.update(b"|");
            }
            let content_hash = hex::encode(hasher.finalize());

            // Handle updated_at as either integer (Unix timestamp) or string
            let updated_at = if has_timestamp {
                row.get(timestamp_column).and_then(|v| {
                    // Try integer first, then string
                    if let Some(n) = v.as_i64() {
                        Some(n.to_string())
                    } else {
                        v.as_str().map(|s| s.to_string())
                    }
                })
            } else {
                None
            };

            metadata.insert(
                pk_value.clone(),
                RowMeta {
                    pk_value,
                    updated_at,
                    content_hash,
                },
            );
        }

        debug!("Got {} rows from D1 table {}", metadata.len(), table);
        Ok(metadata)
    }

    /// Get full row data for specific primary keys
    pub async fn get_rows(
        &self,
        table: &str,
        pk_values: &[String],
    ) -> Result<Vec<HashMap<String, JsonValue>>> {
        if pk_values.is_empty() {
            return Ok(vec![]);
        }

        let pk_cols = self.primary_key_columns(table).await?;
        let columns = self.table_columns(table).await?;

        let pk_expr = pk_cols
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect::<Vec<_>>()
            .join(" || '|' || ");

        let cols = columns
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect::<Vec<_>>()
            .join(", ");

        // D1 has a limit on query size, so we batch
        let batch_size = 100;
        let mut all_results = Vec::new();

        for chunk in pk_values.chunks(batch_size) {
            let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
            let sql = format!(
                "SELECT {} FROM \"{}\" WHERE {} IN ({})",
                cols, table, pk_expr, placeholders
            );

            let params: Vec<JsonValue> =
                chunk.iter().map(|v| JsonValue::String(v.clone())).collect();
            let results = self.query(&sql, params).await?;
            all_results.extend(results);
        }

        Ok(all_results)
    }

    /// Insert or replace rows using multi-row INSERT statements.
    ///
    /// Uses the default batch configuration (100 rows, 90KB max).
    pub async fn upsert_rows(
        &self,
        table: &str,
        rows: &[HashMap<String, JsonValue>],
    ) -> Result<usize> {
        use crate::config::BatchConfig;
        self.upsert_rows_batched(table, rows, &BatchConfig::default())
            .await
    }

    /// Insert or replace rows with custom batch configuration.
    ///
    /// Groups rows into batches respecting both count and size limits,
    /// then executes multi-row INSERT statements for better performance.
    pub async fn upsert_rows_batched(
        &self,
        table: &str,
        rows: &[HashMap<String, JsonValue>],
        batch_config: &crate::config::BatchConfig,
    ) -> Result<usize> {
        use crate::batch::{batch_rows, generate_batch_insert};

        if rows.is_empty() {
            return Ok(0);
        }

        let columns = self.table_columns(table).await?;
        let batches = batch_rows(rows, &columns, batch_config);

        let mut total_changes = 0;

        debug!(
            "Upserting {} rows in {} batches to table {}",
            rows.len(),
            batches.len(),
            table
        );

        for (i, batch) in batches.iter().enumerate() {
            let (sql, params) = generate_batch_insert(table, &columns, &batch.rows);

            if sql.is_empty() {
                continue;
            }

            debug!(
                "Batch {}/{}: {} rows, ~{} bytes",
                i + 1,
                batches.len(),
                batch.rows.len(),
                batch.estimated_bytes
            );

            let changes = self.execute(&sql, params).await?;
            total_changes += changes as usize;
        }

        info!(
            "Upserted {} rows into D1 table {} ({} batches)",
            total_changes,
            table,
            batches.len()
        );
        Ok(total_changes)
    }

    /// Delete rows by primary key
    #[allow(dead_code)]
    pub async fn delete_rows(&self, table: &str, pk_values: &[String]) -> Result<usize> {
        if pk_values.is_empty() {
            return Ok(0);
        }

        let pk_cols = self.primary_key_columns(table).await?;
        let pk_expr = pk_cols
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect::<Vec<_>>()
            .join(" || '|' || ");

        let mut total_changes = 0;

        // Delete in batches
        let batch_size = 100;
        for chunk in pk_values.chunks(batch_size) {
            let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
            let sql = format!(
                "DELETE FROM \"{}\" WHERE {} IN ({})",
                table, pk_expr, placeholders
            );

            let params: Vec<JsonValue> =
                chunk.iter().map(|v| JsonValue::String(v.clone())).collect();
            let changes = self.execute(&sql, params).await?;
            total_changes += changes as usize;
        }

        info!("Deleted {} rows from D1 table {}", total_changes, table);
        Ok(total_changes)
    }

    /// Get row count for a table
    pub async fn row_count(&self, table: &str) -> Result<usize> {
        let results = self
            .query(
                &format!("SELECT COUNT(*) as count FROM \"{}\"", table),
                vec![],
            )
            .await?;

        let count = results
            .first()
            .and_then(|r| r.get("count"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0) as usize;

        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_http_error_429() {
        let err = parse_http_error(reqwest::StatusCode::TOO_MANY_REQUESTS, "rate limited", None);
        assert!(matches!(err, SyncError::RateLimited { .. }));
    }

    #[test]
    fn test_parse_http_error_429_with_retry_after_header() {
        // HTTP header takes precedence
        let err = parse_http_error(reqwest::StatusCode::TOO_MANY_REQUESTS, "{}", Some(60));
        match err {
            SyncError::RateLimited { retry_after } => {
                assert_eq!(retry_after, Some(60));
            }
            _ => panic!("Expected RateLimited"),
        }
    }

    #[test]
    fn test_parse_http_error_429_with_retry_after_body() {
        // Falls back to JSON body if no header
        let body = r#"{"retry_after": 30}"#;
        let err = parse_http_error(reqwest::StatusCode::TOO_MANY_REQUESTS, body, None);
        match err {
            SyncError::RateLimited { retry_after } => {
                assert_eq!(retry_after, Some(30));
            }
            _ => panic!("Expected RateLimited"),
        }
    }

    #[test]
    fn test_parse_http_error_500() {
        let err = parse_http_error(
            reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            "server error",
            None,
        );
        match err {
            SyncError::ServerError { status, message } => {
                assert_eq!(status, 500);
                assert!(message.contains("server error"));
            }
            _ => panic!("Expected ServerError"),
        }
    }

    #[test]
    fn test_parse_http_error_400() {
        let err = parse_http_error(reqwest::StatusCode::BAD_REQUEST, "invalid sql", None);
        match err {
            SyncError::BadRequest { status, message } => {
                assert_eq!(status, 400);
                assert!(message.contains("invalid sql"));
            }
            _ => panic!("Expected BadRequest"),
        }
    }

    #[test]
    fn test_extract_retry_after_json() {
        let body = r#"{"retry_after": 60, "error": "rate limited"}"#;
        assert_eq!(extract_retry_after(body), Some(60));
    }

    #[test]
    fn test_extract_retry_after_none() {
        let body = "plain text error";
        assert_eq!(extract_retry_after(body), None);
    }

    #[test]
    fn test_extract_retry_after_missing_field() {
        let body = r#"{"error": "rate limited"}"#;
        assert_eq!(extract_retry_after(body), None);
    }

    #[test]
    fn test_extract_retry_after_header() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(reqwest::header::RETRY_AFTER, "120".parse().unwrap());
        assert_eq!(extract_retry_after_header(&headers), Some(120));
    }

    #[test]
    fn test_extract_retry_after_header_missing() {
        let headers = reqwest::header::HeaderMap::new();
        assert_eq!(extract_retry_after_header(&headers), None);
    }

    #[test]
    fn test_extract_retry_after_header_invalid() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::RETRY_AFTER,
            "not-a-number".parse().unwrap(),
        );
        assert_eq!(extract_retry_after_header(&headers), None);
    }

    // Integration tests for with_retry function
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_with_retry_retries_on_429() {
        let config = RetryConfig {
            max_retries: 3,
            initial_delay_ms: 1, // minimal delay for tests
            max_delay_ms: 10,
            backoff_multiplier: 2.0,
        };

        let call_count = Arc::new(AtomicU32::new(0));
        let call_count_clone = call_count.clone();

        let result: Result<()> = with_retry(&config, || {
            let count = call_count_clone.clone();
            async move {
                let calls = count.fetch_add(1, Ordering::SeqCst);
                if calls < 2 {
                    // First two calls fail with 429 (retryable)
                    Err(SyncError::RateLimited { retry_after: None })
                } else {
                    // Third call succeeds
                    Ok(())
                }
            }
        })
        .await;

        assert!(result.is_ok(), "Should succeed after retries");
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            3,
            "Should have called operation 3 times"
        );
    }

    #[tokio::test]
    async fn test_with_retry_does_not_retry_on_400() {
        let config = RetryConfig {
            max_retries: 3,
            initial_delay_ms: 1,
            max_delay_ms: 10,
            backoff_multiplier: 2.0,
        };

        let call_count = Arc::new(AtomicU32::new(0));
        let call_count_clone = call_count.clone();

        let result: Result<()> = with_retry(&config, || {
            let count = call_count_clone.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                // 400 Bad Request is not retryable
                Err(SyncError::BadRequest {
                    status: 400,
                    message: "invalid sql".to_string(),
                })
            }
        })
        .await;

        assert!(result.is_err(), "Should fail immediately on 400");
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            1,
            "Should only call operation once"
        );

        match result.unwrap_err() {
            SyncError::BadRequest { status, .. } => assert_eq!(status, 400),
            _ => panic!("Expected BadRequest error"),
        }
    }

    #[tokio::test]
    async fn test_with_retry_respects_max_retries() {
        let config = RetryConfig {
            max_retries: 2,
            initial_delay_ms: 1,
            max_delay_ms: 10,
            backoff_multiplier: 2.0,
        };

        let call_count = Arc::new(AtomicU32::new(0));
        let call_count_clone = call_count.clone();

        let result: Result<()> = with_retry(&config, || {
            let count = call_count_clone.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                // Always fail with retryable error
                Err(SyncError::RateLimited { retry_after: None })
            }
        })
        .await;

        assert!(result.is_err(), "Should fail after exhausting retries");
        // Initial attempt + max_retries = 1 + 2 = 3 calls
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            3,
            "Should call operation max_retries + 1 times"
        );

        match result.unwrap_err() {
            SyncError::RetryExhausted { attempts, .. } => {
                assert_eq!(attempts, 3, "Should report 3 attempts");
            }
            _ => panic!("Expected RetryExhausted error"),
        }
    }
}
