//! HTTP SQL adapter implementing the PluginAdapter trait.

use crate::profile::{AuthFormat, Profile};
use reqwest::Client;
use serde_json::Value;
use smugglr_core::config::DuplicatePkPolicy;
use smugglr_core::error::SyncError;
use smugglr_plugin_sdk::{ColumnInfo, PluginAdapter, PluginError, RowMeta, TableInfo};
use std::collections::HashMap;
use std::sync::Mutex;

/// Classify a non-2xx HTTP response into the wire error class the host
/// should treat it as (#444):
///
/// - 429 is rate-limited -- retryable, carrying any `Retry-After` value so
///   the host recovers a real delay instead of falling back to its own
///   backoff schedule.
/// - Other 5xx is general-transient -- retryable, no delay to carry.
/// - Everything else (4xx other than 429) is permanent -- retrying it would
///   never succeed.
///
/// A free function, not a method, so it is unit-testable without a live HTTP
/// round trip.
fn classify_http_error(
    status: reqwest::StatusCode,
    retry_after_ms: Option<u64>,
    detail: String,
) -> PluginError {
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        PluginError::rate_limited(retry_after_ms, detail)
    } else if status.is_server_error() {
        PluginError::transient(detail)
    } else {
        PluginError::new(detail)
    }
}

pub struct HttpSqlAdapter {
    client: Option<Client>,
    url: String,
    auth_token: String,
    profile: Profile,
    table_info_cache: Mutex<HashMap<String, TableInfo>>,
}

impl HttpSqlAdapter {
    pub fn new() -> Self {
        Self {
            client: None,
            url: String::new(),
            auth_token: String::new(),
            profile: Profile::generic(),
            table_info_cache: Mutex::new(HashMap::new()),
        }
    }

    async fn cached_table_info(&self, table: &str) -> Result<TableInfo, PluginError> {
        if let Some(info) = self.table_info_cache.lock().unwrap().get(table) {
            return Ok(info.clone());
        }
        let info = self.table_info(table).await?;
        self.table_info_cache
            .lock()
            .unwrap()
            .insert(table.to_string(), info.clone());
        Ok(info)
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<Value, PluginError> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| PluginError::new("not initialized"))?;

        let body = self
            .profile
            .build_request(sql, params)
            .map_err(|e| PluginError::new(e.to_string()))?;
        let mut req = client.post(&self.url).json(&body);

        req = match self.profile.auth_format {
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
                PluginError::transient(format!("HTTP request failed: {}", e))
            } else {
                PluginError::new(format!("HTTP request failed: {}", e))
            }
        })?;

        let status = resp.status();
        if !status.is_success() {
            // Read Retry-After before consuming `resp` for the body -- a
            // malformed or absent header degrades to "no retry-after known",
            // not a parse failure, since the classification below still
            // stands on the status code alone.
            let retry_after_ms = resp
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok())
                .map(|secs| secs.saturating_mul(1000));
            let body = resp.text().await.unwrap_or_default();
            let detail = format!("HTTP {} from {}: {}", status, self.url, body);
            return Err(classify_http_error(status, retry_after_ms, detail));
        }

        resp.json::<Value>()
            .await
            .map_err(|e| PluginError::new(format!("Failed to parse response JSON: {}", e)))
    }

    /// Rows for a response, delegated to the profile (#436).
    ///
    /// This and `extract_columns` were byte-identical copies here and in the
    /// wasm fetch adapter, which is why the d1 column-source defect broke both
    /// paths at once. One implementation now, in `Profile`.
    fn extract_rows(
        &self,
        response: &Value,
        columns: &[String],
    ) -> Result<Vec<Vec<Value>>, PluginError> {
        self.profile
            .extract_rows(response, columns)
            .ok_or_else(|| PluginError::new("rows not found in response"))
    }

    fn extract_columns(&self, response: &Value) -> Result<Vec<String>, PluginError> {
        self.profile
            .extract_columns(response)
            .ok_or_else(|| PluginError::new("columns not found in response"))
    }

    /// Maximum rows per batch for a given column count and bind param limit.
    /// Returns `None` when there is no limit (max_bind_params == 0).
    fn max_rows_per_batch(num_columns: usize, max_bind_params: usize) -> Option<usize> {
        if max_bind_params > 0 && num_columns > 0 {
            Some((max_bind_params / num_columns).max(1))
        } else {
            None
        }
    }
}

impl PluginAdapter for HttpSqlAdapter {
    async fn initialize(&mut self, config: HashMap<String, String>) -> Result<(), PluginError> {
        self.url = config
            .get("url")
            .ok_or_else(|| PluginError::new("missing config: url"))?
            .clone();

        self.auth_token = config.get("auth_token").cloned().unwrap_or_default();

        let profile_name = config
            .get("profile")
            .map(String::as_str)
            .unwrap_or("generic");
        self.profile = Profile::from_name(profile_name)
            .ok_or_else(|| PluginError::new(format!("unknown profile: {}", profile_name)))?;

        self.client = Some(Client::new());

        // Test the connection with a simple query
        self.execute("SELECT 1", &[]).await?;
        Ok(())
    }

    async fn list_tables(&self) -> Result<Vec<String>, PluginError> {
        let response = self
            .execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_cf_%' ORDER BY name",
                &[],
            )
            .await?;

        let columns = self.extract_columns(&response)?;
        let rows = self.extract_rows(&response, &columns)?;

        let name_idx = columns.iter().position(|c| c == "name").unwrap_or(0);
        Ok(rows
            .iter()
            .filter_map(|row| row.get(name_idx).and_then(|v| v.as_str()).map(String::from))
            .collect())
    }

    async fn table_info(&self, table: &str) -> Result<TableInfo, PluginError> {
        let response = self
            .execute(&format!("PRAGMA table_info('{}')", table), &[])
            .await?;

        let columns = self.extract_columns(&response)?;
        let rows = self.extract_rows(&response, &columns)?;

        let name_idx = columns.iter().position(|c| c == "name").unwrap_or(1);
        let type_idx = columns.iter().position(|c| c == "type").unwrap_or(2);
        let notnull_idx = columns.iter().position(|c| c == "notnull").unwrap_or(3);
        let pk_idx = columns.iter().position(|c| c == "pk").unwrap_or(5);

        let mut col_infos = Vec::new();
        let mut primary_key = Vec::new();

        for row in &rows {
            let name = row
                .get(name_idx)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let col_type = row
                .get(type_idx)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let notnull = row.get(notnull_idx).and_then(|v| v.as_i64()).unwrap_or(0) != 0;
            let pk = row.get(pk_idx).and_then(|v| v.as_i64()).unwrap_or(0) != 0;

            if pk {
                primary_key.push(name.clone());
            }

            col_infos.push(ColumnInfo {
                name,
                col_type,
                notnull,
                pk,
            });
        }

        Ok(TableInfo {
            name: table.to_string(),
            columns: col_infos,
            primary_key,
        })
    }

    async fn get_row_metadata(
        &self,
        table: &str,
        timestamp_column: &str,
        exclude_columns: &[String],
    ) -> Result<HashMap<String, RowMeta>, PluginError> {
        let info = self.cached_table_info(table).await?;
        if info.primary_key.is_empty() {
            return Err(PluginError::new(format!(
                "no primary key for table: {}",
                table
            )));
        }

        let pk_expr = smugglr_core::rowhash::pk_text_expr(&info.primary_key);

        // Column order from table_info -- must match local.rs hashing order
        let column_order: Vec<String> = info.columns.iter().map(|c| c.name.clone()).collect();

        let sql = format!("SELECT *, {} AS __pk FROM \"{}\"", pk_expr, table);
        let response = self.execute(&sql, &[]).await?;
        let columns = self.extract_columns(&response)?;
        let rows = self.extract_rows(&response, &columns)?;
        let mut maps = smugglr_core::batch_sql::rows_to_maps(&columns, &rows);
        // Canonicalize BLOB columns (base64 on the wire) to the lowercase hex the
        // content hash pins, so blob columns converge with the native path (#292).
        canonicalize_row_blobs(&mut maps, &info);
        // The `[sync].duplicate_pk` knob is host-side config; the plugin wire
        // request carries only (table, timestamp_column, exclude_columns), so
        // the plugin always takes the safe default and refuses (#269). The
        // refusal crosses back as a non-transient PluginError, which the host
        // surfaces as SyncError::Plugin carrying this same message text.
        build_row_metadata(
            &maps,
            &column_order,
            timestamp_column,
            exclude_columns,
            table,
            DuplicatePkPolicy::default(),
        )
        .map_err(classify_row_metadata_error)
    }

    async fn get_rows(
        &self,
        table: &str,
        pk_values: &[String],
    ) -> Result<Vec<HashMap<String, Value>>, PluginError> {
        if pk_values.is_empty() {
            return Ok(vec![]);
        }

        let info = self.cached_table_info(table).await?;
        let sql = smugglr_core::rowhash::pk_in_query(table, &info.primary_key, pk_values.len())
            .map_err(|e| PluginError::new(e.to_string()))?;
        let params: Vec<Value> = pk_values.iter().map(|v| Value::String(v.clone())).collect();

        let response = self.execute(&sql, &params).await?;
        let columns = self.extract_columns(&response)?;
        let rows = self.extract_rows(&response, &columns)?;
        Ok(smugglr_core::batch_sql::rows_to_maps(&columns, &rows))
    }

    async fn upsert_rows(
        &self,
        table: &str,
        rows: &[HashMap<String, Value>],
    ) -> Result<usize, PluginError> {
        if rows.is_empty() {
            return Ok(0);
        }

        let columns: Vec<String> = rows[0].keys().cloned().collect();
        let batch_size = Self::max_rows_per_batch(columns.len(), self.profile.max_bind_params)
            .unwrap_or(rows.len());

        let mut total = 0;
        for batch in rows.chunks(batch_size) {
            let (sql, params) = smugglr_core::batch_sql::generate_batch_sql(table, &columns, batch);

            self.execute(&sql, &params)
                .await
                .map_err(|e| batch_context(e, table, batch.len()))?;
            total += batch.len();
        }

        Ok(total)
    }

    async fn row_count(&self, table: &str) -> Result<usize, PluginError> {
        let sql = format!("SELECT COUNT(*) AS cnt FROM \"{}\"", table);
        let response = self.execute(&sql, &[]).await?;
        let columns = self.extract_columns(&response)?;
        let rows = self.extract_rows(&response, &columns)?;
        let count = rows
            .first()
            .and_then(|r| r.first())
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        Ok(count as usize)
    }
}

/// Build the PK-keyed RowMeta map from result rows carrying a synthetic `__pk`
/// column. Extracted from the async/HTTP-bound `get_row_metadata` so the
/// NULL-/duplicate-PK guard is host-testable. Plugin-local only -- this is NOT
/// the cross-crate dedup tracked by #222.
///
/// Parity with core `local.rs`: a NULL rendered `__pk` (a NULL part of a
/// composite PK propagates through `||`) cannot key pk-based sync, and coercing
/// it to "" would collapse every such row onto one entry -- silently dropping
/// rows and provoking spurious deletes. Skip and warn; likewise surface a
/// duplicate PK-text that overwrites an existing entry.
/// Canonicalize every declared BLOB column across `maps` from the backend's
/// base64 rendering to the lowercase hex the content hash pins, so a blob column
/// converges with the native (hex) reference instead of reading `content_differs`
/// forever (#292). BLOB columns are detected from `info` via the shared
/// `rowhash::is_blob_column` so this crate and the wasm/native paths cannot drift
/// on what counts as a blob. A value that fails to decode is left untouched and
/// warned about -- it hashes divergently and the operator should `exclude` it.
///
/// NOTE: base64 is assumed as the wire encoding (per spike S). A remote that
/// renders blobs as hex instead would be corrupted by this decode; per-endpoint
/// encoding belongs in `Profile` (future work).
fn canonicalize_row_blobs(maps: &mut [HashMap<String, Value>], info: &TableInfo) {
    let blob_columns: Vec<String> = info
        .columns
        .iter()
        .filter(|c| smugglr_core::rowhash::is_blob_column(&c.col_type))
        .map(|c| c.name.clone())
        .collect();
    if blob_columns.is_empty() {
        return;
    }
    for row in maps.iter_mut() {
        for col in smugglr_core::rowhash::canonicalize_blob_columns(
            row,
            &blob_columns,
            smugglr_core::rowhash::BlobEncoding::Base64,
        ) {
            eprintln!(
                "smugglr-http-sql: blob column '{}' in {} did not decode as base64 -- it hashes \
                 divergently across backends; add it to exclude_columns",
                col, info.name
            );
        }
    }
}

/// Add batch context (which table, how many rows) to an `execute()` failure
/// without disturbing its wire error class (#444). Only the default
/// (permanent, uncategorized) code gets the extra detail prefixed onto
/// `message` -- a classified error's code, and for a rate-limited error its
/// leading `retry_after_ms=` prefix, must survive unchanged all the way to
/// the host. Prefixing batch context in front of that marker would both hide
/// it from the host's parser and silently downgrade a retryable 429/5xx into
/// a permanent failure the moment a batch upsert hit one.
fn batch_context(err: PluginError, table: &str, batch_len: usize) -> PluginError {
    if err.code != smugglr_plugin_sdk::DEFAULT_ERROR_CODE {
        return err;
    }
    PluginError::new(format!(
        "batch upsert failed for table '{}' ({} rows in batch): {}",
        table, batch_len, err.message
    ))
}

/// Map a `build_row_metadata` failure onto the wire error the plugin sends
/// back. A duplicate-PK collision (#269, #444) is a permanent conflict that
/// needs a human decision, the same remedy class as the native path's
/// `SyncError::DuplicatePrimaryKey`, so it is tagged with the conflict wire
/// code instead of the default. Every other `build_row_metadata` failure
/// (currently none besides the duplicate-PK refusal, but this stays open for
/// whatever `DuplicatePkPolicy::check` grows) keeps the default permanent
/// code, unchanged from before.
fn classify_row_metadata_error(err: SyncError) -> PluginError {
    match err {
        SyncError::DuplicatePrimaryKey { .. } => PluginError::conflict(err.to_string()),
        other => PluginError::new(other.to_string()),
    }
}

fn build_row_metadata(
    maps: &[HashMap<String, Value>],
    column_order: &[String],
    timestamp_column: &str,
    exclude_columns: &[String],
    table: &str,
    duplicate_pk: DuplicatePkPolicy,
) -> Result<HashMap<String, RowMeta>, SyncError> {
    let mut result: HashMap<String, RowMeta> = HashMap::new();
    for row in maps {
        let pk = match row.get("__pk").and_then(|v| v.as_str()) {
            Some(pk) => pk.to_string(),
            None => {
                eprintln!(
                    "smugglr-http-sql: skipping row in {} with NULL primary key",
                    table
                );
                continue;
            }
        };
        let updated_at = smugglr_core::datasource::extract_updated_at(row.get(timestamp_column));
        let content_hash = smugglr_core::rowhash::content_hash(
            row,
            column_order,
            exclude_columns,
            timestamp_column,
        );

        // Parity with core local.rs: checked BEFORE the insert so a `Refuse`
        // returns while metadata is still being built -- upstream of the diff
        // and of every write, so no row is upserted (#269).
        if let Some(prev) = result.get(&pk) {
            if let Some(message) =
                duplicate_pk.check(table, &pk, &prev.content_hash, &content_hash)?
            {
                eprintln!("smugglr-http-sql: {}", message);
            }
        }

        result.insert(
            pk.clone(),
            RowMeta {
                pk_value: pk.clone(),
                updated_at,
                content_hash,
            },
        );
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// #444: before the fix, every non-2xx status (429 and 5xx included) went
    /// through `PluginError::new`, the default (permanent, non-retryable)
    /// code. A 429/5xx from a hosted backend must instead classify as
    /// retryable so `upsert_with_retry` backs off instead of failing fast --
    /// this drives it through `upsert_rows`, the actual sync-write path, not
    /// just `execute()` directly, since batch-context wrapping is exactly
    /// where a classified error can get silently downgraded back to
    /// permanent.
    #[tokio::test]
    async fn upsert_rows_classifies_503_as_transient() {
        let (endpoint, _server) = capture_error_response(503, "Service Unavailable", None).await;
        let mut adapter = HttpSqlAdapter::new();
        let mut config = HashMap::new();
        config.insert("url".to_string(), endpoint);
        // initialize()'s own SELECT 1 would consume the one queued response;
        // point it at a client with no connection test by constructing the
        // adapter fields directly instead of calling initialize().
        adapter.client = Some(reqwest::Client::new());
        adapter.url = config.get("url").unwrap().clone();

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
        let mut adapter = HttpSqlAdapter::new();
        adapter.client = Some(reqwest::Client::new());
        adapter.url = endpoint;

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
        let mut adapter = HttpSqlAdapter::new();
        adapter.client = Some(reqwest::Client::new());
        adapter.url = endpoint;

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
        let mut adapter = HttpSqlAdapter::new();
        adapter.client = Some(reqwest::Client::new());
        adapter.url = endpoint;

        let mut row = HashMap::new();
        row.insert("id".to_string(), Value::from(1));
        let err = adapter
            .upsert_rows("items", &[row])
            .await
            .expect_err("a 400 must surface as an error");

        assert_eq!(err.code, smugglr_plugin_sdk::DEFAULT_ERROR_CODE);
    }

    /// #444: `get_row_metadata`'s duplicate-PK refusal must reach the host as
    /// the new conflict wire code, not the default permanent code -- so it
    /// maps to a distinct exit-code bucket instead of collapsing into the
    /// same "general/unknown" bucket as any other plugin failure.
    #[test]
    fn duplicate_pk_error_maps_to_conflict_code() {
        let err = SyncError::DuplicatePrimaryKey {
            table: "items".into(),
            pk: "1".into(),
            first_hash: "aaaa".into(),
            second_hash: "bbbb".into(),
        };
        let mapped = classify_row_metadata_error(err);
        assert_eq!(mapped.code, smugglr_plugin_sdk::CONFLICT_ERROR_CODE);
    }

    #[test]
    fn other_row_metadata_errors_stay_permanent() {
        let err = SyncError::Config("bad config".into());
        let mapped = classify_row_metadata_error(err);
        assert_eq!(mapped.code, smugglr_plugin_sdk::DEFAULT_ERROR_CODE);
    }

    #[test]
    fn build_row_metadata_skips_null_primary_key() {
        // Regression for #231 (plugin path): a NULL rendered __pk must be skipped,
        // not coerced to "". Two NULL-pk rows previously collapsed onto a single
        // "" key -- silently dropping one. Pre-fix this returns 1 entry (keyed
        // ""); after the fix it returns 0.
        let mut a = HashMap::new();
        a.insert("__pk".to_string(), Value::Null);
        a.insert("name".to_string(), Value::from("alice"));
        let mut b = HashMap::new();
        b.insert("__pk".to_string(), Value::Null);
        b.insert("name".to_string(), Value::from("bob"));

        let meta = build_row_metadata(
            &[a, b],
            &["name".to_string()],
            "updated_at",
            &[],
            "items",
            DuplicatePkPolicy::default(),
        )
        .expect("NULL __pk rows are skipped, so no duplicate can be detected");

        assert!(
            meta.is_empty(),
            "NULL-__pk rows must be skipped, not collapsed onto one key; got {} entries",
            meta.len()
        );
    }

    /// Two row maps rendering the same `__pk`. The plugin receives `__pk`
    /// already rendered by the backend, so the collision arrives as JSON rather
    /// than being produced by a CAST here -- but it is the same condition the
    /// native builder refuses, and it must refuse identically or the guard is
    /// transport-dependent (the #231 failure shape on this exact code path).
    fn colliding_maps() -> Vec<HashMap<String, Value>> {
        let mut a = HashMap::new();
        a.insert("__pk".to_string(), Value::from("1"));
        a.insert("name".to_string(), Value::from("alice"));
        let mut b = HashMap::new();
        b.insert("__pk".to_string(), Value::from("1"));
        b.insert("name".to_string(), Value::from("bob"));
        vec![a, b]
    }

    #[test]
    fn duplicate_pk_refuses_by_default() {
        // #269 plugin path: parity with native local.rs.
        let err = build_row_metadata(
            &colliding_maps(),
            &["name".to_string()],
            "updated_at",
            &[],
            "items",
            DuplicatePkPolicy::Refuse,
        )
        .expect_err("two rows sharing __pk must refuse");

        match err {
            SyncError::DuplicatePrimaryKey {
                table,
                pk,
                first_hash,
                second_hash,
            } => {
                assert_eq!(table, "items");
                assert_eq!(pk, "1");
                assert_ne!(first_hash, second_hash);
            }
            other => panic!("expected DuplicatePrimaryKey, got {other:?}"),
        }
    }

    #[test]
    fn duplicate_pk_warn_policy_keeps_the_previous_behavior() {
        let meta = build_row_metadata(
            &colliding_maps(),
            &["name".to_string()],
            "updated_at",
            &[],
            "items",
            DuplicatePkPolicy::Warn,
        )
        .expect("warn must not stop the sync");

        assert_eq!(
            meta.len(),
            1,
            "warn collapses the collision, as it did before"
        );
    }

    #[test]
    fn canonicalize_row_blobs_converges_with_native_hex() {
        // #292: a JSON backend renders a BLOB column as base64 ("SGU="); the
        // native rusqlite path renders lowercase hex ("4865"). Before the fix the
        // two content hashes diverge and the row reads content_differs forever.
        // After canonicalizing the base64 blob to hex, the plugin's content hash
        // for the row equals the native (hex) rendering's hash -- they converge.
        // Exercises is_blob_column detection (BLOB canonicalized, INTEGER left).
        let info = TableInfo {
            name: "t".to_string(),
            columns: vec![
                ColumnInfo {
                    name: "id".into(),
                    col_type: "INTEGER".into(),
                    notnull: true,
                    pk: true,
                },
                ColumnInfo {
                    name: "data".into(),
                    col_type: "BLOB".into(),
                    notnull: false,
                    pk: false,
                },
            ],
            primary_key: vec!["id".into()],
        };
        let column_order = vec!["id".to_string(), "data".to_string()];

        // Native (hex) reference row hash.
        let mut native = HashMap::new();
        native.insert("__pk".to_string(), Value::from("1"));
        native.insert("id".to_string(), Value::from(1));
        native.insert("data".to_string(), Value::from("4865"));
        let native_meta = build_row_metadata(
            &[native],
            &column_order,
            "updated_at",
            &[],
            "t",
            DuplicatePkPolicy::default(),
        )
        .expect("single row cannot collide");

        // JSON backend (base64) row: diverges until canonicalized.
        let mut json = HashMap::new();
        json.insert("__pk".to_string(), Value::from("1"));
        json.insert("id".to_string(), Value::from(1));
        json.insert("data".to_string(), Value::from("SGU="));
        let mut maps = vec![json];

        let raw_meta = build_row_metadata(
            &maps,
            &column_order,
            "updated_at",
            &[],
            "t",
            DuplicatePkPolicy::default(),
        )
        .expect("single row cannot collide");
        assert_ne!(
            native_meta.get("1").unwrap().content_hash,
            raw_meta.get("1").unwrap().content_hash,
            "raw base64 vs native hex must diverge before canonicalization"
        );

        canonicalize_row_blobs(&mut maps, &info);
        let json_meta = build_row_metadata(
            &maps,
            &column_order,
            "updated_at",
            &[],
            "t",
            DuplicatePkPolicy::default(),
        )
        .expect("single row cannot collide");
        assert_eq!(
            native_meta.get("1").unwrap().content_hash,
            json_meta.get("1").unwrap().content_hash,
            "blob column must converge after base64->hex canonicalization"
        );
    }

    #[test]
    fn build_row_metadata_keeps_valid_rows() {
        // Guard against over-skipping: a normal string __pk is retained.
        let mut a = HashMap::new();
        a.insert("__pk".to_string(), Value::from("k1"));
        a.insert("name".to_string(), Value::from("alice"));

        let meta = build_row_metadata(
            &[a],
            &["name".to_string()],
            "updated_at",
            &[],
            "items",
            DuplicatePkPolicy::default(),
        )
        .expect("single row cannot collide");

        assert_eq!(meta.len(), 1);
        assert!(meta.contains_key("k1"));
    }

    #[test]
    fn test_content_hash_excludes_timestamp() {
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

    #[test]
    fn test_content_hash_excludes_columns() {
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

    #[test]
    fn test_content_hash_changes_on_data_change() {
        let cols = vec!["id".into(), "name".into()];
        let mut row = HashMap::new();
        row.insert("id".into(), Value::from(1));
        row.insert("name".into(), Value::from("alice"));

        let hash1 = smugglr_core::rowhash::content_hash(&row, &cols, &[], "updated_at");

        row.insert("name".into(), Value::from("bob"));
        let hash2 = smugglr_core::rowhash::content_hash(&row, &cols, &[], "updated_at");

        assert_ne!(hash1, hash2);
    }

    // test_rows_to_maps and the generate_batch_sql_* unit tests moved to
    // smugglr_core::batch_sql (#222) alongside the hoisted implementations --
    // the plugin no longer owns a private copy of either function to test.

    #[test]
    fn batch_size_no_limit() {
        assert_eq!(HttpSqlAdapter::max_rows_per_batch(2, 0), None);
    }

    #[test]
    fn batch_size_d1_limit() {
        // 2 columns, 100 param limit -> 50 rows per batch
        assert_eq!(HttpSqlAdapter::max_rows_per_batch(2, 100), Some(50));
    }

    #[test]
    fn batch_size_wide_table() {
        // 50 columns, 100 param limit -> 2 rows per batch
        assert_eq!(HttpSqlAdapter::max_rows_per_batch(50, 100), Some(2));
    }

    #[test]
    fn batch_size_wider_than_limit() {
        // 150 columns, 100 param limit -> 1 row per batch (never zero)
        assert_eq!(HttpSqlAdapter::max_rows_per_batch(150, 100), Some(1));
    }

    #[test]
    fn batch_size_zero_columns() {
        assert_eq!(HttpSqlAdapter::max_rows_per_batch(0, 100), None);
    }

    #[test]
    fn batch_splitting_respects_param_limit() {
        // 10 columns, 100 param limit -> max 10 rows per batch
        let columns: Vec<String> = (0..10).map(|i| format!("col_{}", i)).collect();
        let rows: Vec<HashMap<String, Value>> = (0..25)
            .map(|i| {
                columns
                    .iter()
                    .map(|c| (c.clone(), Value::from(i)))
                    .collect()
            })
            .collect();

        let batch_size =
            HttpSqlAdapter::max_rows_per_batch(columns.len(), 100).unwrap_or(rows.len());
        assert_eq!(batch_size, 10);

        let batches: Vec<&[HashMap<String, Value>]> = rows.chunks(batch_size).collect();
        assert_eq!(batches.len(), 3); // 10 + 10 + 5
        assert_eq!(batches[0].len(), 10);
        assert_eq!(batches[1].len(), 10);
        assert_eq!(batches[2].len(), 5);

        // Verify no batch exceeds param limit
        for batch in &batches {
            let (_, params) = smugglr_core::batch_sql::generate_batch_sql("test", &columns, batch);
            assert!(params.len() <= 100);
        }
    }
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
/// the URL. For the HTTP-error-classification tests (#444) -- `execute()`
/// takes the non-success branch before ever touching the response body, so
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
