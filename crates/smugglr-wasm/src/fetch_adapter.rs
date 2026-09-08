//! HTTP SQL adapter using browser fetch API.
//!
//! Implements the DataSource trait from smugglr-core using web-sys fetch
//! instead of reqwest. Shares profile definitions with the native http-sql plugin.

use smugglr_core::config::DuplicatePkPolicy;
use smugglr_core::datasource::{DataSource, RowMeta, TableInfo};
use smugglr_core::error::{Result, SyncError};
use smugglr_core::profile::{AuthFormat, Profile};
use std::collections::HashMap;

use crate::adapter_common;

use serde_json::Value;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{Request, RequestInit, RequestMode, Response};

pub struct FetchDataSource {
    url: String,
    // Interior-mutable so `Smugglr.updateAuth(...)` can swap the token at
    // runtime without touching the table_info_cache or the source endpoint.
    auth_token: std::sync::Mutex<String>,
    profile: Profile,
    table_info_cache: std::sync::Mutex<HashMap<String, TableInfo>>,
}

impl FetchDataSource {
    pub fn new(url: String, auth_token: String, profile: Profile) -> Self {
        Self {
            url,
            auth_token: std::sync::Mutex::new(auth_token),
            profile,
            table_info_cache: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Replace the auth token. Subsequent requests use the new value.
    /// Safe to call mid-flight; no in-flight request is mutated.
    pub fn set_auth_token(&self, token: String) {
        *self.auth_token.lock().unwrap() = token;
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<Value> {
        let body = self.profile.build_request(sql, params)?;
        let body_str =
            serde_json::to_string(&body).map_err(|e| SyncError::Remote(e.to_string()))?;

        let opts = RequestInit::new();
        opts.set_method("POST");
        opts.set_mode(RequestMode::Cors);
        opts.set_body(&wasm_bindgen::JsValue::from_str(&body_str));

        let request = Request::new_with_str_and_init(&self.url, &opts)
            .map_err(|e| SyncError::Remote(format!("failed to create request: {:?}", e)))?;

        let headers = request.headers();
        headers
            .set("Content-Type", "application/json")
            .map_err(|e| SyncError::Remote(format!("failed to set header: {:?}", e)))?;

        let token = self.auth_token.lock().unwrap().clone();
        if !token.is_empty() {
            match self.profile.auth_format {
                AuthFormat::Bearer => {
                    headers
                        .set("Authorization", &format!("Bearer {}", token))
                        .map_err(|e| {
                            SyncError::Remote(format!("failed to set auth header: {:?}", e))
                        })?;
                }
                AuthFormat::Basic => {
                    headers
                        .set("Authorization", &format!("Basic {}", token))
                        .map_err(|e| {
                            SyncError::Remote(format!("failed to set auth header: {:?}", e))
                        })?;
                }
                AuthFormat::None => {}
            }
        }

        let global = js_sys::global();
        let resp_value = if let Some(window) = global.dyn_ref::<web_sys::Window>() {
            JsFuture::from(window.fetch_with_request(&request)).await
        } else {
            // WorkerGlobalScope or other environments
            let fetch_fn = js_sys::Reflect::get(&global, &"fetch".into())
                .map_err(|e| SyncError::Remote(format!("fetch not available: {:?}", e)))?;
            let fetch_fn = fetch_fn
                .dyn_into::<js_sys::Function>()
                .map_err(|_| SyncError::Remote("fetch is not a function".into()))?;
            JsFuture::from(
                fetch_fn
                    .call1(&global, &request)
                    .map_err(|e| SyncError::Remote(format!("fetch call failed: {:?}", e)))?
                    .dyn_into::<js_sys::Promise>()
                    .map_err(|_| SyncError::Remote("fetch did not return a promise".into()))?,
            )
            .await
        }
        .map_err(|e| SyncError::Remote(format!("fetch failed: {:?}", e)))?;

        let resp: Response = resp_value
            .dyn_into()
            .map_err(|_| SyncError::Remote("response is not a Response".into()))?;

        if !resp.ok() {
            let status = resp.status();
            let body_text = JsFuture::from(
                resp.text()
                    .map_err(|e| SyncError::Remote(format!("failed to read body: {:?}", e)))?,
            )
            .await
            .map_err(|e| SyncError::Remote(format!("failed to read body: {:?}", e)))?;
            let body_str = body_text.as_string().unwrap_or_default();
            let message = format!("HTTP {} from {}: {}", status, self.url, body_str);

            // Classify by status, the same way the http-sql plugin adapter does
            // (#444). Returning `SyncError::Remote` for every non-2xx -- which
            // this path did until now -- makes `is_retryable` fall to its
            // `_ => false` arm, so a browser or Node client got ONE attempt at a
            // 503 while the CLI backed off and retried the same endpoint.
            //
            // The audit that found it also found why: this adapter and
            // `plugins/smugglr-http-sql/src/adapter.rs` are the same DataSource
            // over the same Profile, written twice, so #444 landed on one copy.
            // The same shape produced #436. Classifying here closes the gap;
            // collapsing the two adapters is the actual fix and is not this
            // change's job.
            // A malformed or absent Retry-After collapses to None rather than
            // failing: the engine then falls back to its configured backoff, so
            // the request still retries.
            let retry_after = resp
                .headers()
                .get("retry-after")
                .ok()
                .flatten()
                .and_then(|v| v.parse::<u64>().ok());

            return Err(
                smugglr_core::error::http_retry_class(status).into_sync_error(
                    status,
                    message,
                    retry_after,
                ),
            );
        }

        let json_promise = resp
            .json()
            .map_err(|e| SyncError::Remote(format!("failed to parse JSON: {:?}", e)))?;
        let json_value = JsFuture::from(json_promise)
            .await
            .map_err(|e| SyncError::Remote(format!("failed to parse JSON: {:?}", e)))?;

        serde_wasm_bindgen::from_value(json_value)
            .map_err(|e| SyncError::Remote(format!("JSON deserialization failed: {}", e)))
    }

    /// Rows for a response, delegated to the profile (#436).
    ///
    /// This and `extract_columns` were byte-identical copies here and in the
    /// http-sql plugin, which is why the d1 column-source defect broke both
    /// paths at once. One implementation now, in `Profile`.
    fn extract_rows(&self, response: &Value, columns: &[String]) -> Result<Vec<Vec<Value>>> {
        self.profile
            .extract_rows(response, columns)
            .ok_or_else(|| SyncError::Remote("rows not found in response".into()))
    }

    fn extract_columns(&self, response: &Value) -> Result<Vec<String>> {
        self.profile
            .extract_columns(response)
            .ok_or_else(|| SyncError::Remote("columns not found in response".into()))
    }

    /// Query row metadata for rows with `timestamp_column > since_timestamp`.
    ///
    /// Used by the incremental diff path to fetch only changed rows instead of
    /// the full table.
    pub async fn get_row_metadata_since(
        &self,
        table: &str,
        timestamp_column: &str,
        exclude_columns: &[String],
        since_timestamp: &str,
    ) -> Result<HashMap<String, RowMeta>> {
        let info = self.cached_table_info(table).await?;
        if info.primary_key.is_empty() {
            return Err(SyncError::Config(format!(
                "no primary key for table: {}",
                table
            )));
        }

        let column_order: Vec<String> = info.columns.iter().map(|c| c.name.clone()).collect();
        let sql =
            adapter_common::incremental_metadata_sql(table, &info.primary_key, timestamp_column);
        let params = vec![Value::String(since_timestamp.to_string())];
        let response = self.execute(&sql, &params).await?;
        let columns = self.extract_columns(&response)?;
        let rows = self.extract_rows(&response, &columns)?;
        let mut maps = adapter_common::rows_to_maps(&columns, &rows);
        adapter_common::canonicalize_json_blobs(&mut maps, &info);

        adapter_common::row_maps_to_metadata(
            &maps,
            &column_order,
            timestamp_column,
            exclude_columns,
            table,
            DuplicatePkPolicy::default(),
        )
    }

    async fn cached_table_info(&self, table: &str) -> Result<TableInfo> {
        adapter_common::cached_table_info(&self.table_info_cache, table, self.table_info(table))
            .await
    }

    fn max_rows_per_batch(num_columns: usize, max_bind_params: usize) -> Option<usize> {
        if max_bind_params > 0 && num_columns > 0 {
            Some((max_bind_params / num_columns).max(1))
        } else {
            None
        }
    }
}

impl DataSource for FetchDataSource {
    async fn list_tables(&self) -> Result<Vec<String>> {
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

    async fn table_info(&self, table: &str) -> Result<TableInfo> {
        let response = self
            .execute(&format!("PRAGMA table_info('{}')", table), &[])
            .await?;

        let columns = self.extract_columns(&response)?;
        let rows = self.extract_rows(&response, &columns)?;
        Ok(adapter_common::parse_table_info(table, &columns, &rows))
    }

    async fn get_row_metadata(
        &self,
        table: &str,
        timestamp_column: &str,
        exclude_columns: &[String],
    ) -> Result<HashMap<String, RowMeta>> {
        let info = self.cached_table_info(table).await?;
        if info.primary_key.is_empty() {
            return Err(SyncError::Config(format!(
                "no primary key for table: {}",
                table
            )));
        }

        let pk_expr = adapter_common::build_pk_text_expr(&info.primary_key);
        let column_order: Vec<String> = info.columns.iter().map(|c| c.name.clone()).collect();
        let sql = format!("SELECT *, {} AS __pk FROM \"{}\"", pk_expr, table);
        let response = self.execute(&sql, &[]).await?;
        let columns = self.extract_columns(&response)?;
        let rows = self.extract_rows(&response, &columns)?;
        let mut maps = adapter_common::rows_to_maps(&columns, &rows);
        adapter_common::canonicalize_json_blobs(&mut maps, &info);

        adapter_common::row_maps_to_metadata(
            &maps,
            &column_order,
            timestamp_column,
            exclude_columns,
            table,
            DuplicatePkPolicy::default(),
        )
    }

    async fn get_rows(
        &self,
        table: &str,
        pk_values: &[String],
    ) -> Result<Vec<HashMap<String, Value>>> {
        if pk_values.is_empty() {
            return Ok(vec![]);
        }

        let info = self.cached_table_info(table).await?;
        let sql = smugglr_core::rowhash::pk_in_query(table, &info.primary_key, pk_values.len())?;
        let params: Vec<Value> = pk_values.iter().map(|v| Value::String(v.clone())).collect();

        let response = self.execute(&sql, &params).await?;
        let columns = self.extract_columns(&response)?;
        let rows = self.extract_rows(&response, &columns)?;
        Ok(adapter_common::rows_to_maps(&columns, &rows))
    }

    async fn upsert_rows(&self, table: &str, rows: &[HashMap<String, Value>]) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }

        let columns: Vec<String> = rows[0].keys().cloned().collect();
        let batch_size = Self::max_rows_per_batch(columns.len(), self.profile.max_bind_params)
            .unwrap_or(rows.len());

        let mut total = 0;
        for batch in rows.chunks(batch_size) {
            let (sql, params) = adapter_common::generate_batch_sql(table, &columns, batch);

            self.execute(&sql, &params).await.map_err(|e| {
                SyncError::Remote(format!(
                    "batch upsert failed for table '{}' ({} rows in batch): {}",
                    table,
                    batch.len(),
                    e
                ))
            })?;
            total += batch.len();
        }

        Ok(total)
    }

    async fn row_count(&self, table: &str) -> Result<usize> {
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
