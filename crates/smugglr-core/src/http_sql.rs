//! The one canonical implementation of the HTTP-SQL `DataSource`/`PluginAdapter`
//! shape shared by `plugins/smugglr-http-sql`'s reqwest-backed plugin and
//! `crates/smugglr-wasm`'s fetch-backed browser adapter (#461).
//!
//! Before this module the two adapters carried byte-equivalent copies of
//! `list_tables`, `table_info`, `get_row_metadata`, `get_rows`, `upsert_rows`,
//! `row_count`, and `max_rows_per_batch`, plus the four helpers that used to
//! live in `smugglr-wasm::adapter_common` (`parse_table_info`,
//! `row_maps_to_metadata`, `canonicalize_json_blobs`, `cached_table_info`).
//! Two copies meant a fix could land on one and not the other -- #436 (column
//! extraction), #444 (retry classification), and #458 (wasm retry parity) all
//! did exactly that. [`HttpSqlSource`] is the one implementation; the only
//! thing left to vary per caller is [`HttpTransport`], the actual POST.
//!
//! `crates/smugglr-wasm/src/local_adapter.rs`'s `LocalSqlDataSource` (a JS-
//! executor-backed source, not HTTP) is a second caller of the four moved
//! helpers -- it was already sharing correctly before this issue and stays a
//! direct caller of the free functions below, never of [`HttpSqlSource`]
//! itself.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Mutex;

use serde_json::Value;

use crate::config::DuplicatePkPolicy;
use crate::datasource::{extract_updated_at, ColumnInfo, MaybeSend, RowMeta, TableInfo};
use crate::error::{http_retry_class, SyncError};
use crate::profile::Profile;

/// The one thing that genuinely differs between the native plugin (reqwest)
/// and the browser adapter (web-sys fetch): putting a JSON body on the wire
/// and reading back enough of the response to classify it.
///
/// Implementors decide their own transport-level failure classification
/// (connect/timeout vs. everything else) in [`HttpTransportError`] -- that
/// judgement genuinely depends on the transport's own error types (reqwest's
/// `is_timeout()`/`is_connect()` vs. a rejected fetch `Promise`) and cannot be
/// shared the way the HTTP-status judgement in [`crate::error::http_retry_class`]
/// can.
pub trait HttpTransport {
    /// The endpoint this transport POSTs to. Used only to build a classified
    /// error's detail message (`HTTP {status} from {endpoint}: {body}`) in
    /// exactly one place ([`HttpSqlSource`]'s internal `execute`) instead of
    /// duplicating that format string in both transports.
    fn endpoint(&self) -> &str;

    /// POST `body` to the configured endpoint.
    ///
    /// `Ok` covers every response actually received, success or not --
    /// [`HttpSqlSource`] classifies the status via
    /// [`crate::error::http_retry_class`]. `Err` is reserved for failing to
    /// get a response at all.
    fn post(
        &self,
        body: Value,
    ) -> impl Future<Output = Result<HttpResponse, HttpTransportError>> + MaybeSend;
}

/// What a transport hands back after POSTing: the response status, any
/// `Retry-After` delay in milliseconds (429 only), and the raw response text
/// -- parsed as JSON on success, used as the classified error's detail on
/// failure.
pub struct HttpResponse {
    pub status: u16,
    pub retry_after_ms: Option<u64>,
    pub body: String,
}

/// A transport-level failure: no response was received at all (connect
/// failure, timeout, a connection dropped mid-body). Distinct from a
/// well-formed non-2xx response, which arrives as `Ok(HttpResponse)` and is
/// classified by [`crate::error::http_retry_class`] instead.
///
/// Each transport picks `Transient` vs `Permanent` using its own idioms --
/// only the transport can tell a recoverable connection failure from one that
/// is not.
#[derive(Debug, thiserror::Error)]
pub enum HttpTransportError {
    #[error("{0}")]
    Transient(String),
    #[error("{0}")]
    Permanent(String),
}

/// The one HTTP-SQL `DataSource` implementation, generic over how a request
/// actually reaches the network.
///
/// `T::post` is the only method either caller (`HttpSqlAdapter` in the
/// native plugin, `FetchDataSource` in the wasm crate) implements for
/// itself; every other method lives here once.
pub struct HttpSqlSource<T: HttpTransport> {
    transport: T,
    profile: Profile,
    table_info_cache: Mutex<HashMap<String, TableInfo>>,
}

impl<T: HttpTransport> HttpSqlSource<T> {
    pub fn new(transport: T, profile: Profile) -> Self {
        Self {
            transport,
            profile,
            table_info_cache: Mutex::new(HashMap::new()),
        }
    }

    /// The wrapped transport. `FetchDataSource::set_auth_token` reaches
    /// through this to update the token stored on `FetchTransport`; a plugin
    /// test build reaches through it to build a `HttpSqlAdapter` pointed at a
    /// capture endpoint without running `initialize`'s own connection probe.
    pub fn transport(&self) -> &T {
        &self.transport
    }

    /// Test the connection with a trivial query. `initialize()` calls this;
    /// nothing else needs to.
    pub async fn probe(&self) -> Result<(), SyncError> {
        self.execute("SELECT 1", &[]).await?;
        Ok(())
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<Value, SyncError> {
        let body = self.profile.build_request(sql, params)?;

        let resp = self.transport.post(body).await.map_err(|e| match e {
            // A general transient transport failure has no real HTTP status
            // to carry, so it borrows the same synthetic-503 convention
            // `smugglr_core::plugin::rpc_error_to_sync_error` already uses
            // for a plugin's own general-transient wire code: retryable,
            // without claiming a status the response never had.
            HttpTransportError::Transient(msg) => SyncError::ServerError {
                status: 503,
                message: msg,
            },
            HttpTransportError::Permanent(msg) => SyncError::Remote(msg),
        })?;

        if (200..300).contains(&resp.status) {
            return serde_json::from_str::<Value>(&resp.body)
                .map_err(|e| SyncError::Remote(format!("Failed to parse response JSON: {}", e)));
        }

        let detail = format!(
            "HTTP {} from {}: {}",
            resp.status,
            self.transport.endpoint(),
            resp.body
        );
        // The wire carries milliseconds; `into_sync_error`'s `retry_after` is
        // seconds (see its doc and `SyncError::retry_after_ms`). `div_ceil`
        // is the exact inverse of the `*1000` a transport used to build
        // `retry_after_ms` from a whole-second `Retry-After` header, so this
        // round-trips losslessly for any header value a real server sends.
        let retry_after_secs = resp.retry_after_ms.map(|ms| ms.div_ceil(1000));
        Err(http_retry_class(resp.status).into_sync_error(resp.status, detail, retry_after_secs))
    }

    fn extract_columns(&self, response: &Value) -> Result<Vec<String>, SyncError> {
        self.profile
            .extract_columns(response)
            .ok_or_else(|| SyncError::Remote("columns not found in response".into()))
    }

    fn extract_rows(
        &self,
        response: &Value,
        columns: &[String],
    ) -> Result<Vec<Vec<Value>>, SyncError> {
        self.profile
            .extract_rows(response, columns)
            .ok_or_else(|| SyncError::Remote("rows not found in response".into()))
    }

    pub async fn list_tables(&self) -> Result<Vec<String>, SyncError> {
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

    pub async fn table_info(&self, table: &str) -> Result<TableInfo, SyncError> {
        let response = self
            .execute(&format!("PRAGMA table_info('{}')", table), &[])
            .await?;
        let columns = self.extract_columns(&response)?;
        let rows = self.extract_rows(&response, &columns)?;
        Ok(parse_table_info(table, &columns, &rows))
    }

    pub async fn cached_table_info(&self, table: &str) -> Result<TableInfo, SyncError> {
        cached_table_info(&self.table_info_cache, table, self.table_info(table)).await
    }

    /// Row metadata for every row in `table`. Returns any non-fatal warnings
    /// (a skipped NULL-`__pk` row, a duplicate-`__pk` collision under
    /// `DuplicatePkPolicy::Warn`, an undecodable blob column) alongside the
    /// map -- the caller renders these on its own sink (`eprintln!`,
    /// `console::warn_1`) and decides the prefix, since that is the one thing
    /// that must stay per-adapter.
    pub async fn get_row_metadata(
        &self,
        table: &str,
        timestamp_column: &str,
        exclude_columns: &[String],
    ) -> Result<(HashMap<String, RowMeta>, Vec<String>), SyncError> {
        let info = self.cached_table_info(table).await?;
        if info.primary_key.is_empty() {
            return Err(SyncError::Config(format!(
                "no primary key for table: {}",
                table
            )));
        }

        let pk_expr = crate::rowhash::pk_text_expr(&info.primary_key);
        let column_order: Vec<String> = info.columns.iter().map(|c| c.name.clone()).collect();
        let sql = format!("SELECT *, {} AS __pk FROM \"{}\"", pk_expr, table);
        let response = self.execute(&sql, &[]).await?;
        let columns = self.extract_columns(&response)?;
        let rows = self.extract_rows(&response, &columns)?;
        let mut maps = crate::batch_sql::rows_to_maps(&columns, &rows);
        let mut warnings = canonicalize_json_blobs(&mut maps, &info);

        let (meta, more) = row_maps_to_metadata(
            &maps,
            &column_order,
            timestamp_column,
            exclude_columns,
            table,
            DuplicatePkPolicy::default(),
        )?;
        warnings.extend(more);
        Ok((meta, warnings))
    }

    /// Row metadata for rows with `timestamp_column >= since_timestamp` --
    /// the incremental diff path. Was `FetchDataSource`'s one method beyond
    /// the `DataSource` trait surface (the plugin wire has no equivalent);
    /// lives here now so its logic is host-testable, matching every other
    /// method on this type (#458's lesson: `smugglr-wasm` compiles to nothing
    /// on the host, so logic left inside it cannot be covered by `cargo test`).
    pub async fn get_row_metadata_since(
        &self,
        table: &str,
        timestamp_column: &str,
        exclude_columns: &[String],
        since_timestamp: &str,
    ) -> Result<(HashMap<String, RowMeta>, Vec<String>), SyncError> {
        let info = self.cached_table_info(table).await?;
        if info.primary_key.is_empty() {
            return Err(SyncError::Config(format!(
                "no primary key for table: {}",
                table
            )));
        }

        let column_order: Vec<String> = info.columns.iter().map(|c| c.name.clone()).collect();
        let sql = incremental_metadata_sql(table, &info.primary_key, timestamp_column);
        let params = vec![Value::String(since_timestamp.to_string())];
        let response = self.execute(&sql, &params).await?;
        let columns = self.extract_columns(&response)?;
        let rows = self.extract_rows(&response, &columns)?;
        let mut maps = crate::batch_sql::rows_to_maps(&columns, &rows);
        let mut warnings = canonicalize_json_blobs(&mut maps, &info);

        let (meta, more) = row_maps_to_metadata(
            &maps,
            &column_order,
            timestamp_column,
            exclude_columns,
            table,
            DuplicatePkPolicy::default(),
        )?;
        warnings.extend(more);
        Ok((meta, warnings))
    }

    pub async fn get_rows(
        &self,
        table: &str,
        pk_values: &[String],
    ) -> Result<Vec<HashMap<String, Value>>, SyncError> {
        if pk_values.is_empty() {
            return Ok(vec![]);
        }

        let info = self.cached_table_info(table).await?;
        let sql = crate::rowhash::pk_in_query(table, &info.primary_key, pk_values.len())?;
        let params: Vec<Value> = pk_values.iter().map(|v| Value::String(v.clone())).collect();

        let response = self.execute(&sql, &params).await?;
        let columns = self.extract_columns(&response)?;
        let rows = self.extract_rows(&response, &columns)?;
        Ok(crate::batch_sql::rows_to_maps(&columns, &rows))
    }

    pub async fn upsert_rows(
        &self,
        table: &str,
        rows: &[HashMap<String, Value>],
    ) -> Result<usize, SyncError> {
        if rows.is_empty() {
            return Ok(0);
        }

        let columns: Vec<String> = rows[0].keys().cloned().collect();
        let batch_size =
            max_rows_per_batch(columns.len(), self.profile.max_bind_params).unwrap_or(rows.len());

        let mut total = 0;
        for batch in rows.chunks(batch_size) {
            let (sql, params) = crate::batch_sql::generate_batch_sql(table, &columns, batch);
            self.execute(&sql, &params)
                .await
                .map_err(|e| batch_context(e, table, batch.len()))?;
            total += batch.len();
        }

        Ok(total)
    }

    pub async fn row_count(&self, table: &str) -> Result<usize, SyncError> {
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

/// Add batch context (which table, how many rows) to an `execute()` failure
/// without disturbing a classified error's variant. Only the two
/// "uncategorized" shapes (`Remote`, `Config`) get the extra detail prefixed
/// -- a `ServerError`/`RateLimited` error's retry classification must survive
/// unchanged all the way to the host, or a batch upsert that hits one
/// silently downgrades a retryable 429/5xx into a permanent failure.
fn batch_context(err: SyncError, table: &str, batch_len: usize) -> SyncError {
    let prefixed = |message: String| {
        format!(
            "batch upsert failed for table '{}' ({} rows in batch): {}",
            table, batch_len, message
        )
    };
    match err {
        SyncError::Remote(message) => SyncError::Remote(prefixed(message)),
        SyncError::Config(message) => SyncError::Config(prefixed(message)),
        other => other,
    }
}

/// Largest number of rows that fit under `max_bind_params` for a statement
/// with `num_columns` columns per row. `None` when there is no limit
/// (`max_bind_params == 0`).
pub fn max_rows_per_batch(num_columns: usize, max_bind_params: usize) -> Option<usize> {
    if max_bind_params > 0 && num_columns > 0 {
        Some((max_bind_params / num_columns).max(1))
    } else {
        None
    }
}

/// Parse the rows of a `PRAGMA table_info(...)` result into a [`TableInfo`].
pub fn parse_table_info(table: &str, columns: &[String], rows: &[Vec<Value>]) -> TableInfo {
    let name_idx = columns.iter().position(|c| c == "name").unwrap_or(1);
    let type_idx = columns.iter().position(|c| c == "type").unwrap_or(2);
    let notnull_idx = columns.iter().position(|c| c == "notnull").unwrap_or(3);
    let pk_idx = columns.iter().position(|c| c == "pk").unwrap_or(5);

    let mut col_infos = Vec::new();
    let mut primary_key = Vec::new();

    for row in rows {
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

    TableInfo {
        name: table.to_string(),
        columns: col_infos,
        primary_key,
    }
}

/// Convert result rows (each with a synthetic `__pk` column) into RowMeta
/// entries keyed by primary key, plus any non-fatal warnings the caller
/// should render on its own sink: a skipped NULL-`__pk` row, or a
/// duplicate-`__pk` collision under [`DuplicatePkPolicy::Warn`].
///
/// Returns `Err` when two rows render the same `__pk` and `duplicate_pk` is
/// [`DuplicatePkPolicy::Refuse`] (#269). Every HTTP-SQL caller passes the
/// default: the `[sync].duplicate_pk` knob is native-config-only, since
/// neither the plugin wire nor the browser adapters ever see a `SyncConfig`.
pub fn row_maps_to_metadata(
    maps: &[HashMap<String, Value>],
    column_order: &[String],
    timestamp_column: &str,
    exclude_columns: &[String],
    table: &str,
    duplicate_pk: DuplicatePkPolicy,
) -> Result<(HashMap<String, RowMeta>, Vec<String>), SyncError> {
    let mut result: HashMap<String, RowMeta> = HashMap::with_capacity(maps.len());
    let mut warnings = Vec::new();
    for row in maps {
        // Parity with core local.rs: a NULL rendered __pk (a NULL part of a
        // composite PK propagates through `||`) cannot key pk-based sync.
        // Coercing it to "" would collapse every such row onto one entry --
        // silently dropping rows and provoking spurious deletes. Skip and warn.
        let pk = match row.get("__pk").and_then(|v| v.as_str()) {
            Some(pk) => pk.to_string(),
            None => {
                warnings.push(format!("skipping row in {} with NULL primary key", table));
                continue;
            }
        };
        let updated_at = extract_updated_at(row.get(timestamp_column));
        let content_hash =
            crate::rowhash::content_hash(row, column_order, exclude_columns, timestamp_column);

        // Parity with core local.rs: checked BEFORE the insert so a `Refuse`
        // returns while metadata is still being built -- upstream of the diff
        // and of every write, so no row is upserted (#269).
        if let Some(prev) = result.get(&pk) {
            if let Some(message) =
                duplicate_pk.check(table, &pk, &prev.content_hash, &content_hash)?
            {
                warnings.push(message);
            }
        }

        result.insert(
            pk.clone(),
            RowMeta {
                pk_value: pk,
                updated_at,
                content_hash,
            },
        );
    }
    Ok((result, warnings))
}

/// Canonicalize every declared BLOB column across `maps` from the backend's
/// base64 rendering to the lowercase hex the content hash pins, so a blob
/// column converges with the native (hex) reference instead of reading
/// `content_differs` forever (#292). Returns the names of any columns that
/// failed to decode -- the caller MUST warn on these, on its own sink.
pub fn canonicalize_json_blobs(
    maps: &mut [HashMap<String, Value>],
    info: &TableInfo,
) -> Vec<String> {
    let blob_columns: Vec<String> = info
        .columns
        .iter()
        .filter(|c| crate::rowhash::is_blob_column(&c.col_type))
        .map(|c| c.name.clone())
        .collect();
    if blob_columns.is_empty() {
        return Vec::new();
    }
    let mut warnings = Vec::new();
    for row in maps.iter_mut() {
        for col in crate::rowhash::canonicalize_blob_columns(
            row,
            &blob_columns,
            crate::rowhash::BlobEncoding::Base64,
        ) {
            warnings.push(format!(
                "blob column '{}' in {} did not decode as base64 -- it hashes divergently \
                 across backends; add it to exclude_columns",
                col, info.name
            ));
        }
    }
    warnings
}

/// Build the incremental-metadata query: every row whose `timestamp_column`
/// is at or after the cursor, with a synthetic `__pk` text column. `>=` (not
/// `>`) -- see bug #199, the predicate must stay inclusive.
pub fn incremental_metadata_sql(
    table: &str,
    primary_key: &[String],
    timestamp_column: &str,
) -> String {
    format!(
        "SELECT *, {} AS __pk FROM \"{}\" WHERE \"{}\" >= ?",
        crate::rowhash::pk_text_expr(primary_key),
        table,
        timestamp_column
    )
}

/// Look up `table` in `cache`, computing and memoizing it via `fetch` on a
/// miss. Shared by [`HttpSqlSource::cached_table_info`] and
/// `LocalSqlDataSource::cached_table_info` (a non-HTTP caller in
/// `smugglr-wasm`), which differ only in how they obtain a fresh
/// [`TableInfo`] on a cache miss.
pub async fn cached_table_info<F>(
    cache: &Mutex<HashMap<String, TableInfo>>,
    table: &str,
    fetch: F,
) -> Result<TableInfo, SyncError>
where
    F: Future<Output = Result<TableInfo, SyncError>>,
{
    if let Some(info) = cache.lock().unwrap().get(table) {
        return Ok(info.clone());
    }
    let info = fetch.await?;
    cache
        .lock()
        .unwrap()
        .insert(table.to_string(), info.clone());
    Ok(info)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::Mutex as StdMutex;

    /// A transport that answers from a queued list of responses and records
    /// every request body it was asked to send, so a test can assert on
    /// request COUNT (batching, caching) without a real network.
    struct MockTransport {
        endpoint: String,
        responses: StdMutex<VecDeque<Result<HttpResponse, HttpTransportError>>>,
        requests: StdMutex<Vec<Value>>,
    }

    impl MockTransport {
        fn new(responses: Vec<Result<HttpResponse, HttpTransportError>>) -> Self {
            Self {
                endpoint: "https://mock.example/query".to_string(),
                responses: StdMutex::new(responses.into()),
                requests: StdMutex::new(Vec::new()),
            }
        }

        fn request_count(&self) -> usize {
            self.requests.lock().unwrap().len()
        }
    }

    impl HttpTransport for MockTransport {
        fn endpoint(&self) -> &str {
            &self.endpoint
        }

        async fn post(&self, body: Value) -> Result<HttpResponse, HttpTransportError> {
            self.requests.lock().unwrap().push(body);
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| panic!("MockTransport: no queued response left"))
        }
    }

    fn ok_json(body: &str) -> Result<HttpResponse, HttpTransportError> {
        Ok(HttpResponse {
            status: 200,
            retry_after_ms: None,
            body: body.to_string(),
        })
    }

    fn status_only(
        code: u16,
        retry_after_ms: Option<u64>,
    ) -> Result<HttpResponse, HttpTransportError> {
        Ok(HttpResponse {
            status: code,
            retry_after_ms,
            body: "{}".to_string(),
        })
    }

    fn source(
        responses: Vec<Result<HttpResponse, HttpTransportError>>,
    ) -> HttpSqlSource<MockTransport> {
        HttpSqlSource::new(MockTransport::new(responses), Profile::generic())
    }

    fn one_row(id: i64) -> HashMap<String, Value> {
        let mut row = HashMap::new();
        row.insert("id".to_string(), Value::from(id));
        row
    }

    // -- Pipeline tests, run through the mock transport end to end --

    #[tokio::test]
    async fn list_tables_reads_table_names_from_rows_not_columns() {
        // The #436 shape: D1 sends no column list, so table names must be
        // read from the ROWS of the sqlite_master listing (FirstRowKeys),
        // never misread as the column list itself.
        let responses = vec![ok_json(
            r#"{"result":[{"results":[{"name":"customers"},{"name":"orders"}]}]}"#,
        )];
        let src = HttpSqlSource::new(MockTransport::new(responses), Profile::d1());
        let tables = src.list_tables().await.expect("list_tables");
        assert_eq!(tables, vec!["customers".to_string(), "orders".to_string()]);
    }

    #[tokio::test]
    async fn upsert_rows_503_is_retryable() {
        let src = source(vec![status_only(503, None)]);
        let err = src
            .upsert_rows("items", &[one_row(1)])
            .await
            .expect_err("503 must error");
        assert!(err.is_retryable(), "a 503 must be retryable, got {err:?}");
        assert!(matches!(err, SyncError::ServerError { status: 503, .. }));
    }

    #[tokio::test]
    async fn upsert_rows_429_retry_after_round_trips_to_seconds() {
        let src = source(vec![status_only(429, Some(30_000))]);
        let err = src
            .upsert_rows("items", &[one_row(1)])
            .await
            .expect_err("429 must error");
        assert!(
            matches!(
                err,
                SyncError::RateLimited {
                    retry_after: Some(30)
                }
            ),
            "30000ms Retry-After must round-trip to 30s, got {err:?}"
        );
        assert!(err.is_retryable());
    }

    #[tokio::test]
    async fn upsert_rows_400_is_permanent() {
        let src = source(vec![status_only(400, None)]);
        let err = src
            .upsert_rows("items", &[one_row(1)])
            .await
            .expect_err("400 must error");
        assert!(!err.is_retryable(), "a 400 must not be retryable");
    }

    #[tokio::test]
    async fn transport_transient_failure_is_retryable() {
        let src = source(vec![Err(HttpTransportError::Transient(
            "connect timed out".into(),
        ))]);
        let err = src.list_tables().await.expect_err("transient must error");
        assert!(err.is_retryable());
    }

    #[tokio::test]
    async fn transport_permanent_failure_is_not_retryable() {
        let src = source(vec![Err(HttpTransportError::Permanent(
            "dns failure".into(),
        ))]);
        let err = src.list_tables().await.expect_err("permanent must error");
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn get_rows_empty_pk_values_makes_no_request() {
        let src = source(vec![]);
        let rows = src
            .get_rows("items", &[])
            .await
            .expect("empty pk_values short-circuits");
        assert!(rows.is_empty());
        assert_eq!(src.transport().request_count(), 0);
    }

    #[tokio::test]
    async fn upsert_rows_chunks_within_max_bind_params() {
        // 2 columns, max_bind_params 4 -> 2 rows/batch; 5 rows -> 3 requests
        // (2 + 2 + 1), and no request may exceed the bind-param limit.
        let mut profile = Profile::generic();
        profile.max_bind_params = 4;
        let responses: Vec<_> = (0..3).map(|_| ok_json(r#"{"rows":[]}"#)).collect();
        let src = HttpSqlSource::new(MockTransport::new(responses), profile);

        let rows: Vec<HashMap<String, Value>> = (0..5)
            .map(|i| {
                let mut r = HashMap::new();
                r.insert("id".to_string(), Value::from(i));
                r.insert("name".to_string(), Value::from(format!("n{i}")));
                r
            })
            .collect();

        let total = src.upsert_rows("items", &rows).await.expect("upsert");
        assert_eq!(total, 5);
        assert_eq!(
            src.transport().request_count(),
            3,
            "5 rows at 2/batch must issue 3 requests"
        );
    }

    #[tokio::test]
    async fn cached_table_info_issues_one_request_across_two_calls() {
        let responses = vec![ok_json(
            r#"{"columns":["cid","name","type","notnull","dflt_value","pk"],"rows":[[0,"id","INTEGER",1,null,1]]}"#,
        )];
        let src = source(responses);
        let a = src.cached_table_info("items").await.expect("first fetch");
        let b = src
            .cached_table_info("items")
            .await
            .expect("second (cached) fetch");
        assert_eq!(a.primary_key, b.primary_key);
        assert_eq!(a.primary_key, vec!["id".to_string()]);
        assert_eq!(src.transport().request_count(), 1);
    }

    // -- Moved from crates/smugglr-wasm/src/adapter_common.rs, which is
    // #![cfg(target_arch = "wasm32")] and compiles to nothing on the host --
    // these `wasm_bindgen_test`s never ran under `cargo test`. Now plain
    // #[test]s, reachable by the mandated pipeline.

    #[test]
    fn incremental_metadata_sql_uses_inclusive_predicate() {
        // Regression for #199: the incremental cursor predicate must be `>=`,
        // not `>`, so a boundary-tick row is re-admitted.
        let sql = incremental_metadata_sql("items", &["id".to_string()], "updated_at");
        assert_eq!(
            sql,
            "SELECT *, CAST(\"id\" AS TEXT) AS __pk FROM \"items\" WHERE \"updated_at\" >= ?"
        );
    }

    #[test]
    fn row_maps_to_metadata_round_trips_integer_timestamp() {
        // Regression for #177: an integer Unix timestamp arriving as a JSON
        // number must round-trip to its decimal string, matching how
        // local.rs renders the same value.
        let mut row = HashMap::new();
        row.insert("__pk".to_string(), Value::String("k1".to_string()));
        row.insert(
            "updated_at".to_string(),
            Value::Number(serde_json::Number::from(1_700_000_000_i64)),
        );
        row.insert("name".to_string(), Value::String("alice".to_string()));

        let (meta, warnings) = row_maps_to_metadata(
            &[row],
            &["name".to_string(), "updated_at".to_string()],
            "updated_at",
            &[],
            "items",
            DuplicatePkPolicy::default(),
        )
        .expect("single row cannot collide");

        assert!(warnings.is_empty());
        assert_eq!(
            meta.get("k1").expect("row keyed by __pk").updated_at,
            Some("1700000000".to_string())
        );
    }

    #[test]
    fn row_maps_to_metadata_skips_null_primary_key() {
        // Regression for #231: a NULL rendered __pk must be skipped, not
        // coerced to "" -- two such rows previously collapsed onto a single
        // "" key, silently dropping one.
        let mut a = HashMap::new();
        a.insert("__pk".to_string(), Value::Null);
        a.insert("name".to_string(), Value::String("alice".to_string()));
        let mut b = HashMap::new();
        b.insert("__pk".to_string(), Value::Null);
        b.insert("name".to_string(), Value::String("bob".to_string()));

        let (meta, warnings) = row_maps_to_metadata(
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
        assert_eq!(
            warnings.len(),
            2,
            "each skipped NULL-__pk row must warn once"
        );
    }

    fn colliding_maps() -> Vec<HashMap<String, Value>> {
        let mut a = HashMap::new();
        a.insert("__pk".to_string(), Value::String("1".to_string()));
        a.insert("name".to_string(), Value::String("alice".to_string()));
        let mut b = HashMap::new();
        b.insert("__pk".to_string(), Value::String("1".to_string()));
        b.insert("name".to_string(), Value::String("bob".to_string()));
        vec![a, b]
    }

    #[test]
    fn duplicate_pk_refuses_by_default() {
        // #269: the browser/plugin adapters have no SyncConfig, so they take
        // the default -- pinned here as Refuse, not Warn.
        let err = row_maps_to_metadata(
            &colliding_maps(),
            &["name".to_string()],
            "updated_at",
            &[],
            "items",
            DuplicatePkPolicy::default(),
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
        let (meta, warnings) = row_maps_to_metadata(
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
        assert_eq!(warnings.len(), 1, "the collision must still warn once");
    }

    #[test]
    fn canonicalize_json_blobs_converges_with_native_hex() {
        // #292: a JSON backend renders a BLOB column as base64 ("SGU="); the
        // native rusqlite path renders lowercase hex ("4865"). Before
        // canonicalization the two content hashes diverge; after, they
        // converge.
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

        let mut native = HashMap::new();
        native.insert("__pk".to_string(), Value::from("1"));
        native.insert("id".to_string(), Value::from(1));
        native.insert("data".to_string(), Value::from("4865"));
        let (native_meta, _) = row_maps_to_metadata(
            &[native],
            &column_order,
            "updated_at",
            &[],
            "t",
            DuplicatePkPolicy::default(),
        )
        .expect("single row cannot collide");

        let mut json = HashMap::new();
        json.insert("__pk".to_string(), Value::from("1"));
        json.insert("id".to_string(), Value::from(1));
        json.insert("data".to_string(), Value::from("SGU="));
        let mut maps = vec![json];

        let (raw_meta, _) = row_maps_to_metadata(
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

        let warnings = canonicalize_json_blobs(&mut maps, &info);
        assert!(warnings.is_empty(), "SGU= must decode as base64");
        let (json_meta, _) = row_maps_to_metadata(
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
    fn canonicalize_json_blobs_warns_on_undecodable_value() {
        let info = TableInfo {
            name: "t".to_string(),
            columns: vec![ColumnInfo {
                name: "data".into(),
                col_type: "BLOB".into(),
                notnull: false,
                pk: false,
            }],
            primary_key: vec![],
        };
        let mut row = HashMap::new();
        row.insert("data".to_string(), Value::from("!!!not-base64!!!"));
        let mut maps = vec![row];

        let warnings = canonicalize_json_blobs(&mut maps, &info);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("blob column 'data' in t did not decode as base64"));
    }

    #[test]
    fn batch_size_no_limit() {
        assert_eq!(max_rows_per_batch(2, 0), None);
    }

    #[test]
    fn batch_size_d1_limit() {
        assert_eq!(max_rows_per_batch(2, 100), Some(50));
    }

    #[test]
    fn batch_size_wide_table() {
        assert_eq!(max_rows_per_batch(50, 100), Some(2));
    }

    #[test]
    fn batch_size_wider_than_limit() {
        assert_eq!(max_rows_per_batch(150, 100), Some(1));
    }

    #[test]
    fn batch_size_zero_columns() {
        assert_eq!(max_rows_per_batch(0, 100), None);
    }

    #[test]
    fn batch_splitting_respects_param_limit() {
        let columns: Vec<String> = (0..10).map(|i| format!("col_{}", i)).collect();
        let rows: Vec<HashMap<String, Value>> = (0..25)
            .map(|i| {
                columns
                    .iter()
                    .map(|c| (c.clone(), Value::from(i)))
                    .collect()
            })
            .collect();

        let batch_size = max_rows_per_batch(columns.len(), 100).unwrap_or(rows.len());
        assert_eq!(batch_size, 10);

        let batches: Vec<&[HashMap<String, Value>]> = rows.chunks(batch_size).collect();
        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].len(), 10);
        assert_eq!(batches[1].len(), 10);
        assert_eq!(batches[2].len(), 5);

        for batch in &batches {
            let (_, params) = crate::batch_sql::generate_batch_sql("test", &columns, batch);
            assert!(params.len() <= 100);
        }
    }
}
