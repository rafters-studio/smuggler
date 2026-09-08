//! HTTP SQL adapter implementing the PluginAdapter trait.
//!
//! `HttpSqlAdapter` is a thin wrapper: the only thing it owns is
//! `ReqwestTransport` (how a request actually reaches the network) and the
//! `SyncError -> PluginError` wire mapping. Every other method -- schema
//! discovery, row metadata, batching, blob canonicalization -- delegates to
//! `smugglr_core::http_sql::HttpSqlSource`, the one shared implementation
//! this crate and the wasm fetch adapter both build on (#461). Before this,
//! `adapter.rs:183-350` carried a byte-equivalent copy of what is now
//! `HttpSqlSource`'s body, which is how #436 and #458 each landed a fix here
//! and not on `crates/smugglr-wasm/src/fetch_adapter.rs`.

use crate::profile::{AuthFormat, Profile};
use reqwest::Client;
use serde_json::Value;
use smugglr_core::error::SyncError;
use smugglr_core::http_sql::{HttpResponse, HttpSqlSource, HttpTransport, HttpTransportError};
use smugglr_plugin_sdk::{PluginAdapter, PluginError, RowMeta, TableInfo};
use std::collections::HashMap;

/// The plugin's `HttpTransport`: a POST over reqwest. Everything else about
/// how this adapter answers a `PluginAdapter` call lives in `HttpSqlSource`.
pub struct ReqwestTransport {
    client: Client,
    url: String,
    auth_token: String,
    auth_format: AuthFormat,
}

impl ReqwestTransport {
    pub fn new(url: String, auth_token: String, auth_format: AuthFormat) -> Self {
        Self {
            client: Client::new(),
            url,
            auth_token,
            auth_format,
        }
    }
}

impl HttpTransport for ReqwestTransport {
    fn endpoint(&self) -> &str {
        &self.url
    }

    async fn post(&self, body: Value) -> Result<HttpResponse, HttpTransportError> {
        let mut req = self.client.post(&self.url).json(&body);

        req = match self.auth_format {
            AuthFormat::Bearer if !self.auth_token.is_empty() => req.bearer_auth(&self.auth_token),
            AuthFormat::Basic if !self.auth_token.is_empty() => {
                req.basic_auth(&self.auth_token, None::<&str>)
            }
            _ => req,
        };

        let resp = req.send().await.map_err(|e| {
            // A timeout or failed connect against a hosted backend is the
            // same "try again later" condition a 5xx is -- classify it
            // transient so upsert_with_retry backs off instead of failing
            // fast on what may be a momentary network blip (#444).
            if e.is_timeout() || e.is_connect() {
                HttpTransportError::Transient(format!("HTTP request failed: {}", e))
            } else {
                HttpTransportError::Permanent(format!("HTTP request failed: {}", e))
            }
        })?;

        let status = resp.status().as_u16();
        if !resp.status().is_success() {
            // Read Retry-After before consuming `resp` for the body -- a
            // malformed or absent header degrades to "no retry-after known",
            // not a parse failure, since classification still stands on the
            // status code alone.
            let retry_after_ms = resp
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok())
                .map(|secs| secs.saturating_mul(1000));
            let body = resp.text().await.unwrap_or_default();
            return Ok(HttpResponse {
                status,
                retry_after_ms,
                body,
            });
        }

        let body = resp.text().await.map_err(|e| {
            HttpTransportError::Permanent(format!("failed to read response body: {}", e))
        })?;
        Ok(HttpResponse {
            status,
            retry_after_ms: None,
            body,
        })
    }
}

/// Map the shared core result onto this plugin's wire type. The judgement
/// (which statuses retry, what a duplicate-PK collision means) lives in
/// `smugglr_core` so this adapter and the wasm fetch adapter cannot answer
/// differently; only the rendering -- `SyncError` vs. `PluginError` -- is
/// adapter-local.
fn sync_error_to_plugin_error(err: SyncError) -> PluginError {
    let rendered = err.to_string();
    match err {
        SyncError::RateLimited { retry_after } => {
            PluginError::rate_limited(retry_after.map(|s| s.saturating_mul(1000)), String::new())
        }
        SyncError::ServerError { message, .. } => PluginError::transient(message),
        SyncError::Remote(message) | SyncError::Config(message) => PluginError::new(message),
        SyncError::DuplicatePrimaryKey { .. } => PluginError::conflict(rendered),
        _ => PluginError::new(rendered),
    }
}

pub struct HttpSqlAdapter {
    source: Option<HttpSqlSource<ReqwestTransport>>,
}

impl HttpSqlAdapter {
    pub fn new() -> Self {
        Self { source: None }
    }

    fn source(&self) -> Result<&HttpSqlSource<ReqwestTransport>, PluginError> {
        self.source
            .as_ref()
            .ok_or_else(|| PluginError::new("not initialized"))
    }

    /// Build an adapter pointed directly at a capture endpoint, bypassing
    /// `initialize`'s own `SELECT 1` connection probe -- which would
    /// otherwise consume the one queued response a test endpoint has to give.
    #[cfg(test)]
    fn for_test(url: String) -> Self {
        Self {
            source: Some(HttpSqlSource::new(
                ReqwestTransport::new(url, String::new(), AuthFormat::Bearer),
                Profile::generic(),
            )),
        }
    }
}

impl PluginAdapter for HttpSqlAdapter {
    async fn initialize(&mut self, config: HashMap<String, String>) -> Result<(), PluginError> {
        let url = config
            .get("url")
            .ok_or_else(|| PluginError::new("missing config: url"))?
            .clone();
        let auth_token = config.get("auth_token").cloned().unwrap_or_default();

        let profile_name = config
            .get("profile")
            .map(String::as_str)
            .unwrap_or("generic");
        let profile = Profile::from_name(profile_name)
            .ok_or_else(|| PluginError::new(format!("unknown profile: {}", profile_name)))?;
        let auth_format = profile.auth_format.clone();

        let source =
            HttpSqlSource::new(ReqwestTransport::new(url, auth_token, auth_format), profile);
        source.probe().await.map_err(sync_error_to_plugin_error)?;
        self.source = Some(source);
        Ok(())
    }

    async fn list_tables(&self) -> Result<Vec<String>, PluginError> {
        self.source()?
            .list_tables()
            .await
            .map_err(sync_error_to_plugin_error)
    }

    async fn table_info(&self, table: &str) -> Result<TableInfo, PluginError> {
        self.source()?
            .table_info(table)
            .await
            .map_err(sync_error_to_plugin_error)
    }

    async fn get_row_metadata(
        &self,
        table: &str,
        timestamp_column: &str,
        exclude_columns: &[String],
    ) -> Result<HashMap<String, RowMeta>, PluginError> {
        let (meta, warnings) = self
            .source()?
            .get_row_metadata(table, timestamp_column, exclude_columns)
            .await
            .map_err(sync_error_to_plugin_error)?;
        for w in warnings {
            eprintln!("smugglr-http-sql: {}", w);
        }
        Ok(meta)
    }

    async fn get_rows(
        &self,
        table: &str,
        pk_values: &[String],
    ) -> Result<Vec<HashMap<String, Value>>, PluginError> {
        self.source()?
            .get_rows(table, pk_values)
            .await
            .map_err(sync_error_to_plugin_error)
    }

    async fn upsert_rows(
        &self,
        table: &str,
        rows: &[HashMap<String, Value>],
    ) -> Result<usize, PluginError> {
        self.source()?
            .upsert_rows(table, rows)
            .await
            .map_err(sync_error_to_plugin_error)
    }

    async fn row_count(&self, table: &str) -> Result<usize, PluginError> {
        self.source()?
            .row_count(table)
            .await
            .map_err(sync_error_to_plugin_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pin the four wire codes `sync_error_to_plugin_error` produces --
    /// this is the one adapter-local piece of judgement left after the
    /// collapse, so it gets its own direct test rather than relying only on
    /// the end-to-end HTTP tests below to exercise every arm.
    #[test]
    fn sync_error_to_plugin_error_pins_wire_codes() {
        assert_eq!(
            sync_error_to_plugin_error(SyncError::RateLimited {
                retry_after: Some(30)
            })
            .code,
            smugglr_plugin_sdk::RATE_LIMITED_ERROR_CODE
        );
        assert_eq!(
            sync_error_to_plugin_error(SyncError::ServerError {
                status: 503,
                message: "down".into()
            })
            .code,
            smugglr_plugin_sdk::TRANSIENT_ERROR_CODE
        );
        assert_eq!(
            sync_error_to_plugin_error(SyncError::DuplicatePrimaryKey {
                table: "items".into(),
                pk: "1".into(),
                first_hash: "aaaa".into(),
                second_hash: "bbbb".into(),
            })
            .code,
            smugglr_plugin_sdk::CONFLICT_ERROR_CODE
        );
        assert_eq!(
            sync_error_to_plugin_error(SyncError::Remote("boom".into())).code,
            smugglr_plugin_sdk::DEFAULT_ERROR_CODE
        );
    }

    #[test]
    fn sync_error_to_plugin_error_unwraps_remote_and_config_bare() {
        // Remote/Config carry their own message with no adapter-added
        // context; the plugin surfaces it verbatim rather than through
        // SyncError's Display (which would add a "Remote API error: " /
        // "Configuration error: " prefix).
        assert_eq!(
            sync_error_to_plugin_error(SyncError::Remote("rows not found in response".into()))
                .message,
            "rows not found in response"
        );
        assert_eq!(
            sync_error_to_plugin_error(SyncError::Config("no primary key for table: t".into()))
                .message,
            "no primary key for table: t"
        );
    }

    /// #444: before the fix, every non-2xx status (429 and 5xx included) went
    /// through `PluginError::new`, the default (permanent, non-retryable)
    /// code. A 429/5xx from a hosted backend must instead classify as
    /// retryable so `upsert_with_retry` backs off -- this drives it through
    /// `upsert_rows`, the actual sync-write path, not just a raw POST, since
    /// batch-context wrapping is exactly where a classified error can get
    /// silently downgraded back to permanent.
    #[tokio::test]
    async fn upsert_rows_classifies_503_as_transient() {
        let (endpoint, _server) = capture_error_response(503, "Service Unavailable", None).await;
        let adapter = HttpSqlAdapter::for_test(endpoint);

        let mut row = HashMap::new();
        row.insert("id".to_string(), Value::from(1));
        let err = adapter
            .upsert_rows("items", &[row])
            .await
            .expect_err("a 503 must surface as an error");

        assert_eq!(
            err.code,
            smugglr_plugin_sdk::TRANSIENT_ERROR_CODE,
            "a 5xx must be tagged transient so the host retries it, not code {} (message: {})",
            err.code,
            err.message
        );
    }

    /// #444: HTTP 429 must classify as rate-limited (not the general
    /// transient code) and carry any `Retry-After` header value across the
    /// wire, so the host can recover it into `SyncError::RateLimited {
    /// retry_after }` instead of a fixed 503-shaped backoff.
    #[tokio::test]
    async fn upsert_rows_classifies_429_as_rate_limited_with_retry_after() {
        let (endpoint, _server) =
            capture_error_response(429, "Too Many Requests", Some("30")).await;
        let adapter = HttpSqlAdapter::for_test(endpoint);

        let mut row = HashMap::new();
        row.insert("id".to_string(), Value::from(1));
        let err = adapter
            .upsert_rows("items", &[row])
            .await
            .expect_err("a 429 must surface as an error");

        assert_eq!(
            err.code,
            smugglr_plugin_sdk::RATE_LIMITED_ERROR_CODE,
            "a 429 must be tagged rate-limited, not code {} (message: {})",
            err.code,
            err.message
        );
        assert!(
            err.message.starts_with("retry_after_ms=30000;"),
            "the Retry-After: 30 header must cross as a retry_after_ms= prefix, got: {}",
            err.message
        );
    }

    /// A 429 with no `Retry-After` header still classifies as rate-limited --
    /// the host just falls back to its own backoff schedule.
    #[tokio::test]
    async fn upsert_rows_classifies_429_without_retry_after_header() {
        let (endpoint, _server) = capture_error_response(429, "Too Many Requests", None).await;
        let adapter = HttpSqlAdapter::for_test(endpoint);

        let mut row = HashMap::new();
        row.insert("id".to_string(), Value::from(1));
        let err = adapter
            .upsert_rows("items", &[row])
            .await
            .expect_err("a 429 must surface as an error");

        assert_eq!(err.code, smugglr_plugin_sdk::RATE_LIMITED_ERROR_CODE);
        assert!(!err.message.starts_with("retry_after_ms="));
    }

    /// A 4xx that is not 429 (a bad request, say) stays permanent -- retrying
    /// it would never succeed, so it must not pick up a transient/rate-limited
    /// code.
    #[tokio::test]
    async fn upsert_rows_keeps_other_4xx_permanent() {
        let (endpoint, _server) = capture_error_response(400, "Bad Request", None).await;
        let adapter = HttpSqlAdapter::for_test(endpoint);

        let mut row = HashMap::new();
        row.insert("id".to_string(), Value::from(1));
        let err = adapter
            .upsert_rows("items", &[row])
            .await
            .expect_err("a 400 must surface as an error");

        assert_eq!(err.code, smugglr_plugin_sdk::DEFAULT_ERROR_CODE);
    }

    #[tokio::test]
    async fn test_content_hash_excludes_timestamp() {
        let cols = vec!["id".into(), "name".into(), "updated_at".into()];
        let mut row = HashMap::new();
        row.insert("id".into(), Value::from(1));
        row.insert("name".into(), Value::from("alice"));
        row.insert("updated_at".into(), Value::from("2026-01-01"));

        let hash1 = smugglr_core::rowhash::content_hash(&row, &cols, &[], "updated_at");

        row.insert("updated_at".into(), Value::from("2026-12-31"));
        let hash2 = smugglr_core::rowhash::content_hash(&row, &cols, &[], "updated_at");

        assert_eq!(hash1, hash2);
    }

    #[tokio::test]
    async fn test_content_hash_excludes_columns() {
        let cols = vec!["id".into(), "name".into(), "embedding".into()];
        let mut row = HashMap::new();
        row.insert("id".into(), Value::from(1));
        row.insert("name".into(), Value::from("alice"));
        row.insert("embedding".into(), Value::from("big blob"));

        let hash1 =
            smugglr_core::rowhash::content_hash(&row, &cols, &["embedding".into()], "updated_at");

        row.insert("embedding".into(), Value::from("different blob"));
        let hash2 =
            smugglr_core::rowhash::content_hash(&row, &cols, &["embedding".into()], "updated_at");

        assert_eq!(hash1, hash2);
    }

    #[tokio::test]
    async fn test_content_hash_changes_on_data_change() {
        let cols = vec!["id".into(), "name".into()];
        let mut row = HashMap::new();
        row.insert("id".into(), Value::from(1));
        row.insert("name".into(), Value::from("alice"));

        let hash1 = smugglr_core::rowhash::content_hash(&row, &cols, &[], "updated_at");

        row.insert("name".into(), Value::from("bob"));
        let hash2 = smugglr_core::rowhash::content_hash(&row, &cols, &[], "updated_at");

        assert_ne!(hash1, hash2);
    }

    // build_row_metadata, batch-size/param-limit chunking, blob
    // canonicalization, and the duplicate-PK/NULL-__pk regression tests moved
    // to smugglr_core::http_sql (#461) alongside the hoisted implementations
    // -- this plugin no longer owns a private copy of any of it to test.
}

/// The request an [`HttpSqlAdapter`] actually put on the wire, as read off a
/// socket rather than as the adapter reports it (#429).
#[cfg(test)]
struct CapturedRequest {
    path: String,
    host: Option<String>,
    authorization: Option<String>,
}

/// Stand up a one-shot HTTP endpoint, answer a single request with `body`, and
/// hand back what arrived.
///
/// Hand-rolled on a `TcpListener` because the point is to read the bytes the
/// adapter sent. A mock built on reqwest's own types would agree with reqwest
/// about what was sent, which is the thing under test.
#[cfg(test)]
async fn capture_one_request(
    body: &'static str,
) -> (String, tokio::task::JoinHandle<CapturedRequest>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind an ephemeral port");
    let addr = listener.local_addr().expect("read back the bound port");

    let handle = tokio::spawn(async move {
        // Bounded so a future change that stops terminating the header block
        // fails by name in seconds instead of hanging CI with no diagnostic --
        // review of #447 flagged the unbounded version as a footgun.
        let deadline = std::time::Duration::from_secs(10);
        let (mut socket, _) = tokio::time::timeout(deadline, listener.accept())
            .await
            .expect("the adapter must connect within the deadline")
            .expect("accept the adapter");

        // Read until the header block ends. The adapter sends a small JSON body
        // with Content-Length, so one read is not guaranteed to cover it; loop
        // until the blank line so the request line and headers are complete.
        let mut buf = Vec::new();
        let mut chunk = [0u8; 1024];
        loop {
            let n = tokio::time::timeout(deadline, socket.read(&mut chunk))
                .await
                .expect("the adapter must finish its header block within the deadline")
                .expect("read the request");
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
            if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
        let text = String::from_utf8_lossy(&buf).to_string();

        let path = text
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .unwrap_or_default()
            .to_string();
        let header = |name: &str| -> Option<String> {
            text.lines()
                .find(|l| l.to_ascii_lowercase().starts_with(name))
                .map(|l| l[name.len()..].trim().to_string())
        };
        let authorization = header("authorization:");
        // Host plus the request path is the whole URL as it went out. Without
        // it a test asserting only the path cannot tell one endpoint from
        // another, which review of #447 flagged as overstating what is checked.
        let host = header("host:");

        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        socket
            .write_all(response.as_bytes())
            .await
            .expect("answer the adapter");
        socket.flush().await.expect("flush the response");

        CapturedRequest {
            path,
            host,
            authorization,
        }
    });

    (format!("http://{}", addr), handle)
}

/// Stand up a one-shot HTTP endpoint that answers a single request with a
/// non-2xx status, optionally carrying a `Retry-After` header, and hand back
/// the URL. For the HTTP-error-classification tests (#444) -- the transport
/// takes the non-success branch before ever needing a JSON-parseable body, so
/// unlike [`capture_one_request`] the body here is fixed and only the status
/// line and headers vary per test.
#[cfg(test)]
async fn capture_error_response(
    status: u16,
    reason: &'static str,
    retry_after: Option<&'static str>,
) -> (String, tokio::task::JoinHandle<()>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind an ephemeral port");
    let addr = listener.local_addr().expect("read back the bound port");

    let handle = tokio::spawn(async move {
        let deadline = std::time::Duration::from_secs(10);
        let (mut socket, _) = tokio::time::timeout(deadline, listener.accept())
            .await
            .expect("the adapter must connect within the deadline")
            .expect("accept the adapter");

        let mut buf = Vec::new();
        let mut chunk = [0u8; 1024];
        loop {
            let n = tokio::time::timeout(deadline, socket.read(&mut chunk))
                .await
                .expect("the adapter must finish its header block within the deadline")
                .expect("read the request");
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
            if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }

        let retry_after_header = retry_after
            .map(|v| format!("Retry-After: {}\r\n", v))
            .unwrap_or_default();
        let body = "{}";
        let response = format!(
            "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\n{}Content-Length: {}\r\n\r\n{}",
            status,
            reason,
            retry_after_header,
            body.len(),
            body
        );
        socket
            .write_all(response.as_bytes())
            .await
            .expect("answer the adapter");
        socket.flush().await.expect("flush the response");
    });

    (format!("http://{}", addr), handle)
}

#[cfg(test)]
mod d1_target_reaches_d1 {
    use super::*;
    use smugglr_core::config::{d1_plugin_config, Config, TargetConfig};

    /// A D1-shaped answer to `initialize`'s own `SELECT 1`.
    ///
    /// `initialize` ends by executing that statement, so a capture endpoint that
    /// answers with something the profile cannot parse fails the connection test
    /// before any assertion runs. These tests assert on the REQUEST -- URL and
    /// Authorization -- so the response only has to be parseable. Whether the d1
    /// profile reads such a response CORRECTLY is #436, and is deliberately not
    /// asserted here.
    const D1_SELECT_1: &str = r#"{"result":[{"results":[{"1":1}],"success":true}],"success":true}"#;

    /// The plugin config for the documented `[target] type = "d1"` TOML.
    ///
    /// Parses the TOML an operator actually writes, so the field names are
    /// exercised, then hands the fields to core's own synthesis rather than
    /// restating what it produces.
    fn documented_d1_plugin_config(url: Option<&str>) -> HashMap<String, String> {
        let mut toml_text = String::from(
            "local_db = \"app.db\"\n[target]\ntype = \"d1\"\naccount_id = \"acct\"\ndatabase_id = \"db\"\napi_token = \"tok\"\n",
        );
        if let Some(u) = url {
            toml_text.push_str(&format!("url = \"{}\"\n", u));
        }
        let config: Config =
            toml::from_str(&toml_text).expect("the documented d1 config must parse");

        let Some(TargetConfig::D1 {
            account_id,
            database_id,
            api_token,
            url,
        }) = config.target
        else {
            panic!("type = \"d1\" must parse as TargetConfig::D1");
        };
        d1_plugin_config(&account_id, &database_id, &api_token, url.as_deref())
    }

    /// Point a real adapter at a capture endpoint using the config core built,
    /// preserving the resolved path so the assertion covers what core produced.
    async fn initialize_against(plugin_config: &HashMap<String, String>, endpoint: &str) {
        let resolved_url = plugin_config
            .get("url")
            .expect("core must hand the adapter a url")
            .clone();
        let path = match resolved_url.split_once("://") {
            Some((_, rest)) => match rest.split_once('/') {
                Some((_, p)) => format!("/{}", p),
                None => String::new(),
            },
            None => String::new(),
        };

        let mut on_the_wire = plugin_config.clone();
        on_the_wire.insert("url".to_string(), format!("{}{}", endpoint, path));

        let mut adapter = HttpSqlAdapter::new();
        adapter
            .initialize(on_the_wire)
            .await
            .expect("initialize must reach the endpoint and parse SELECT 1");
    }

    #[tokio::test]
    async fn the_documented_d1_config_reaches_cloudflare_with_its_token() {
        // #429: `[target] type = "d1"` with account_id/database_id/api_token --
        // the shape the README, config.example.toml and the get-started page all
        // teach. Before the fix this failed at `missing config: url`, and with a
        // url supplied by hand the request arrived as `Authorization: None`.
        let (endpoint, server) = capture_one_request(D1_SELECT_1).await;
        let plugin_config = documented_d1_plugin_config(None);

        assert_eq!(
            plugin_config.get("url").map(String::as_str),
            Some("https://api.cloudflare.com/client/v4/accounts/acct/d1/database/db/query"),
            "core must derive Cloudflare's own D1 endpoint when no url is configured"
        );
        assert_eq!(
            plugin_config.get("api_token"),
            None,
            "#429: the key nothing read is gone"
        );

        initialize_against(&plugin_config, &endpoint).await;

        let captured = server.await.expect("the capture task must finish");
        assert_eq!(
            captured.path, "/client/v4/accounts/acct/d1/database/db/query",
            "the adapter must POST to the path core resolved"
        );
        assert_eq!(
            captured.authorization.as_deref(),
            Some("Bearer tok"),
            "the api_token must arrive as a bearer token"
        );
    }

    #[tokio::test]
    async fn a_configured_url_still_wins_and_carries_the_token() {
        // The DO-bridge case in config.example.toml: type = "d1" with an
        // explicit url. It was broken the same way -- the token never reached
        // the adapter -- and the fix must not take the custom url away.
        let (endpoint, server) = capture_one_request(D1_SELECT_1).await;
        let plugin_config =
            documented_d1_plugin_config(Some("https://do-bridge.example.workers.dev/query"));

        assert_eq!(
            plugin_config.get("url").map(String::as_str),
            Some("https://do-bridge.example.workers.dev/query"),
            "a configured url must survive resolution untouched"
        );

        initialize_against(&plugin_config, &endpoint).await;

        let captured = server.await.expect("the capture task must finish");
        assert_eq!(captured.path, "/query");
        assert_eq!(captured.authorization.as_deref(), Some("Bearer tok"));
    }

    #[tokio::test]
    async fn the_legacy_flat_keys_reach_d1_too() {
        // cloudflare_account_id / database_id / cloudflare_api_token funnel
        // through the same synthesis and failed identically. Parsed from TOML so
        // the legacy field names are exercised, not just the values.
        let (endpoint, server) = capture_one_request(D1_SELECT_1).await;
        let config: Config = toml::from_str(
            "local_db = \"app.db\"\ncloudflare_account_id = \"acct\"\ndatabase_id = \"db\"\ncloudflare_api_token = \"tok\"\n",
        )
        .expect("the legacy flat config must parse");

        let plugin_config = d1_plugin_config(
            config.cloudflare_account_id.as_deref().expect("account id"),
            config.database_id.as_deref().expect("database id"),
            config.cloudflare_api_token.as_deref().expect("api token"),
            None,
        );
        assert_eq!(
            plugin_config.get("url").map(String::as_str),
            Some("https://api.cloudflare.com/client/v4/accounts/acct/d1/database/db/query")
        );

        initialize_against(&plugin_config, &endpoint).await;

        let captured = server.await.expect("the capture task must finish");
        assert_eq!(
            captured.path,
            "/client/v4/accounts/acct/d1/database/db/query"
        );
        assert_eq!(
            captured.host.as_deref(),
            Some(endpoint.trim_start_matches("http://"))
        );
        assert_eq!(captured.authorization.as_deref(), Some("Bearer tok"));
    }
}

#[cfg(test)]
mod d1_table_discovery {
    use super::*;
    use smugglr_core::config::d1_plugin_config;

    /// Cloudflare's documented D1 query response, wrapping `rows`.
    ///
    /// Derived from Cloudflare's published response format, NOT captured from a
    /// live database -- this repository has no D1 account or token. #436 asks
    /// for recorded responses from the live service; see the PR body, where that
    /// gap is stated rather than papered over.
    fn d1_body(rows: &str) -> String {
        format!(
            r#"{{"result":[{{"results":{},"success":true}}],"success":true,"errors":[],"messages":[]}}"#,
            rows
        )
    }

    /// A real adapter, initialized against a capture endpoint, then driven
    /// through one discovery call. `initialize` itself issues `SELECT 1`, so the
    /// endpoint must answer twice.
    async fn d1_adapter_against(bodies: Vec<String>) -> HttpSqlAdapter {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind an ephemeral port");
        let addr = listener.local_addr().expect("read back the bound port");

        tokio::spawn(async move {
            let deadline = std::time::Duration::from_secs(10);
            for body in bodies {
                let Ok(Ok((mut socket, _))) =
                    tokio::time::timeout(deadline, listener.accept()).await
                else {
                    return;
                };
                let mut buf = Vec::new();
                let mut chunk = [0u8; 1024];
                loop {
                    let Ok(Ok(n)) = tokio::time::timeout(deadline, socket.read(&mut chunk)).await
                    else {
                        return;
                    };
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                    if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.flush().await;
            }
        });

        let mut plugin_config = d1_plugin_config("acct", "db", "tok", None);
        plugin_config.insert("url".to_string(), format!("http://{}", addr));

        let mut adapter = HttpSqlAdapter::new();
        adapter
            .initialize(plugin_config)
            .await
            .expect("initialize must reach the endpoint");
        adapter
    }

    #[tokio::test]
    async fn list_tables_reports_the_tables_d1_reports() {
        // #436, end to end on the plugin path: before the fix the table names
        // were read as the COLUMN list and list_tables returned nothing usable,
        // so `smugglr status` against D1 could not name a single table.
        let adapter = d1_adapter_against(vec![
            d1_body(r#"[{"1":1}]"#),
            d1_body(r#"[{"name":"customers"},{"name":"orders"}]"#),
        ])
        .await;

        let tables = adapter.list_tables().await.expect("list_tables");
        assert_eq!(
            tables,
            vec!["customers".to_string(), "orders".to_string()],
            "the rows of the sqlite_master listing are the table names"
        );
    }

    #[tokio::test]
    async fn table_info_finds_the_primary_key_d1_reports() {
        // The second half of discovery. A table whose primary key is not found
        // is refused by sync (#332), so this is what stood between a working
        // config and a push.
        let adapter = d1_adapter_against(vec![
            d1_body(r#"[{"1":1}]"#),
            d1_body(
                r#"[{"cid":0,"name":"id","type":"TEXT","notnull":1,"dflt_value":null,"pk":1},
                    {"cid":1,"name":"updated_at","type":"INTEGER","notnull":0,"dflt_value":null,"pk":0}]"#,
            ),
        ])
        .await;

        let info = adapter.table_info("customers").await.expect("table_info");
        assert_eq!(info.primary_key, vec!["id".to_string()]);
        assert_eq!(info.columns.len(), 2);
        assert_eq!(info.columns[0].name, "id");
        assert!(info.columns[0].pk);
        assert!(!info.columns[1].pk);
    }
}
