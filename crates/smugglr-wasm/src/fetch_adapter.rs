//! HTTP SQL adapter using browser fetch API.
//!
//! `FetchDataSource` is a thin wrapper: the only thing it owns is
//! `FetchTransport` (how a request actually reaches the network via web-sys
//! fetch). Every other method -- schema discovery, row metadata, batching,
//! blob canonicalization -- delegates to `smugglr_core::http_sql::HttpSqlSource`,
//! the one shared implementation this crate and the native http-sql plugin
//! both build on (#461). Before this, `fetch_adapter.rs:232-330` carried a
//! byte-equivalent copy of what is now `HttpSqlSource`'s body, which is how
//! #444's retry-classification fix landed on the plugin and not here.

use smugglr_core::datasource::{DataSource, RowMeta, TableInfo};
use smugglr_core::error::Result;
use smugglr_core::http_sql::{HttpResponse, HttpSqlSource, HttpTransport, HttpTransportError};
use smugglr_core::profile::{AuthFormat, Profile};
use std::collections::HashMap;

use serde_json::Value;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{Request, RequestInit, RequestMode, Response};

/// The wasm crate's `HttpTransport`: a POST over `web_sys::fetch`. Stores no
/// `JsValue` -- everything needed to build a `Request` is plain owned data
/// (`String`/`AuthFormat`), so this type is `Send`/`Sync` by construction and
/// needs no `unsafe impl` the way `LocalSqlDataSource` (which does hold a
/// `JsValue` executor handle) does.
pub struct FetchTransport {
    url: String,
    // Interior-mutable so `Smugglr.updateAuth(...)` can swap the token at
    // runtime without touching the table_info_cache or the source endpoint.
    auth_token: std::sync::Mutex<String>,
    auth_format: AuthFormat,
}

impl FetchTransport {
    pub fn new(url: String, auth_token: String, auth_format: AuthFormat) -> Self {
        Self {
            url,
            auth_token: std::sync::Mutex::new(auth_token),
            auth_format,
        }
    }

    /// Replace the auth token. Subsequent requests use the new value.
    /// Safe to call mid-flight; no in-flight request is mutated.
    pub fn set_auth_token(&self, token: String) {
        *self.auth_token.lock().unwrap() = token;
    }
}

impl HttpTransport for FetchTransport {
    fn endpoint(&self) -> &str {
        &self.url
    }

    async fn post(&self, body: Value) -> std::result::Result<HttpResponse, HttpTransportError> {
        let body_str = serde_json::to_string(&body).map_err(|e| {
            HttpTransportError::Permanent(format!("failed to serialize body: {}", e))
        })?;

        let opts = RequestInit::new();
        opts.set_method("POST");
        opts.set_mode(RequestMode::Cors);
        opts.set_body(&wasm_bindgen::JsValue::from_str(&body_str));

        let request = Request::new_with_str_and_init(&self.url, &opts).map_err(|e| {
            HttpTransportError::Permanent(format!("failed to create request: {:?}", e))
        })?;

        let headers = request.headers();
        headers
            .set("Content-Type", "application/json")
            .map_err(|e| HttpTransportError::Permanent(format!("failed to set header: {:?}", e)))?;

        let token = self.auth_token.lock().unwrap().clone();
        if !token.is_empty() {
            match self.auth_format {
                AuthFormat::Bearer => {
                    headers
                        .set("Authorization", &format!("Bearer {}", token))
                        .map_err(|e| {
                            HttpTransportError::Permanent(format!(
                                "failed to set auth header: {:?}",
                                e
                            ))
                        })?;
                }
                AuthFormat::Basic => {
                    headers
                        .set("Authorization", &format!("Basic {}", token))
                        .map_err(|e| {
                            HttpTransportError::Permanent(format!(
                                "failed to set auth header: {:?}",
                                e
                            ))
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
            let fetch_fn = js_sys::Reflect::get(&global, &"fetch".into()).map_err(|e| {
                HttpTransportError::Permanent(format!("fetch not available: {:?}", e))
            })?;
            let fetch_fn = fetch_fn
                .dyn_into::<js_sys::Function>()
                .map_err(|_| HttpTransportError::Permanent("fetch is not a function".into()))?;
            JsFuture::from(
                fetch_fn
                    .call1(&global, &request)
                    .map_err(|e| {
                        HttpTransportError::Permanent(format!("fetch call failed: {:?}", e))
                    })?
                    .dyn_into::<js_sys::Promise>()
                    .map_err(|_| {
                        HttpTransportError::Permanent("fetch did not return a promise".into())
                    })?,
            )
            .await
        }
        .map_err(|e| HttpTransportError::Permanent(format!("fetch failed: {:?}", e)))?;

        let resp: Response = resp_value
            .dyn_into()
            .map_err(|_| HttpTransportError::Permanent("response is not a Response".into()))?;

        let status = resp.status();
        let retry_after_ms = if resp.ok() {
            None
        } else {
            resp.headers()
                .get("retry-after")
                .ok()
                .flatten()
                .and_then(|v| v.parse::<u64>().ok())
                .map(|secs| secs.saturating_mul(1000))
        };

        let body_text =
            JsFuture::from(resp.text().map_err(|e| {
                HttpTransportError::Permanent(format!("failed to read body: {:?}", e))
            })?)
            .await
            .map_err(|e| HttpTransportError::Permanent(format!("failed to read body: {:?}", e)))?
            .as_string()
            .unwrap_or_default();

        Ok(HttpResponse {
            status,
            retry_after_ms,
            body: body_text,
        })
    }
}

pub struct FetchDataSource {
    source: HttpSqlSource<FetchTransport>,
}

impl FetchDataSource {
    pub fn new(url: String, auth_token: String, profile: Profile) -> Self {
        let transport = FetchTransport::new(url, auth_token, profile.auth_format.clone());
        Self {
            source: HttpSqlSource::new(transport, profile),
        }
    }

    /// Replace the auth token. Subsequent requests use the new value.
    /// Safe to call mid-flight; no in-flight request is mutated.
    pub fn set_auth_token(&self, token: String) {
        self.source.transport().set_auth_token(token);
    }

    /// Query row metadata for rows with `timestamp_column >= since_timestamp`.
    ///
    /// Used by the incremental diff path to fetch only changed rows instead of
    /// the full table. The plugin wire has no equivalent method, so this stays
    /// `FetchDataSource`'s one extra method beyond the `DataSource` surface;
    /// the logic itself lives in `HttpSqlSource` so it is host-testable.
    pub async fn get_row_metadata_since(
        &self,
        table: &str,
        timestamp_column: &str,
        exclude_columns: &[String],
        since_timestamp: &str,
    ) -> Result<HashMap<String, RowMeta>> {
        let (meta, warnings) = self
            .source
            .get_row_metadata_since(table, timestamp_column, exclude_columns, since_timestamp)
            .await?;
        for w in warnings {
            web_sys::console::warn_1(&format!("smugglr: {}", w).into());
        }
        Ok(meta)
    }
}

impl DataSource for FetchDataSource {
    async fn list_tables(&self) -> Result<Vec<String>> {
        self.source.list_tables().await
    }

    async fn table_info(&self, table: &str) -> Result<TableInfo> {
        self.source.table_info(table).await
    }

    async fn get_row_metadata(
        &self,
        table: &str,
        timestamp_column: &str,
        exclude_columns: &[String],
    ) -> Result<HashMap<String, RowMeta>> {
        let (meta, warnings) = self
            .source
            .get_row_metadata(table, timestamp_column, exclude_columns)
            .await?;
        for w in warnings {
            web_sys::console::warn_1(&format!("smugglr: {}", w).into());
        }
        Ok(meta)
    }

    async fn get_rows(
        &self,
        table: &str,
        pk_values: &[String],
    ) -> Result<Vec<HashMap<String, Value>>> {
        self.source.get_rows(table, pk_values).await
    }

    async fn upsert_rows(&self, table: &str, rows: &[HashMap<String, Value>]) -> Result<usize> {
        self.source.upsert_rows(table, rows).await
    }

    async fn row_count(&self, table: &str) -> Result<usize> {
        self.source.row_count(table).await
    }
}
