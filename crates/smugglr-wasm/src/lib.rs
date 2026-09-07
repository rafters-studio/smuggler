//! smugglr WASM package -- sync engine for browser and Node.js.
//!
//! Provides push, pull, sync, and diff operations between HTTP SQL endpoints
//! using the browser fetch API. Same diff engine as the CLI, compiled to WASM.
//!
//! The WASM adapter bypasses smugglr-core's generic sync orchestration (which
//! requires Send futures for tokio) and drives the diff engine directly against
//! FetchDataSource. The diff algorithm, conflict resolution, and batch planning
//! are all from smugglr-core.
//!
//! Compilation gate: this crate requires `target_arch = "wasm32"`. FetchDataSource
//! uses `!Send` types (JsFuture, Rc, RefCell) which are incompatible with the
//! `Send` futures that smugglr-core's DataSource trait requires on multi-threaded
//! targets. Under `wasm32-unknown-unknown` the single-threaded target relaxes
//! `Send` bounds and the crate builds cleanly. See issue #93 for details.

#![cfg(target_arch = "wasm32")]

mod adapter_common;
mod fetch_adapter;
mod local_adapter;

use fetch_adapter::FetchDataSource;
use local_adapter::LocalSqlDataSource;
use smugglr_core::config::{column_excluded, ConflictResolution, SyncConfig};
use smugglr_core::datasource::{DataSource, RowMeta, TableInfo};
use smugglr_core::diff::{classify_diff, TableDiff};
use smugglr_core::error::Result as SmugglrResult;
use smugglr_core::profile::Profile;

use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use wasm_bindgen::closure::Closure;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

/// Build the `TableChangedEvent` JS object for a single table.
fn build_table_changed_event(
    table: &str,
    changed_pks: &[String],
    origin: &str,
) -> Result<JsValue, JsValue> {
    let obj = js_sys::Object::new();
    js_sys::Reflect::set(&obj, &JsValue::from_str("table"), &JsValue::from_str(table))?;
    let arr = js_sys::Array::new();
    for pk in changed_pks {
        arr.push(&JsValue::from_str(pk));
    }
    js_sys::Reflect::set(&obj, &JsValue::from_str("changedPks"), &arr)?;
    js_sys::Reflect::set(
        &obj,
        &JsValue::from_str("removedPks"),
        &js_sys::Array::new(),
    )?;
    js_sys::Reflect::set(
        &obj,
        &JsValue::from_str("source"),
        &JsValue::from_str(origin),
    )?;
    Ok(obj.into())
}

/// Per-table cached row metadata used for incremental diff.
///
/// After each full or incremental scan, the Smugglr instance stores the
/// complete hash map for each table alongside the maximum timestamp seen.
/// On subsequent calls, only rows newer than `max_timestamp` are fetched,
/// and the result is merged into the cached map before diffing.
struct CachedMeta {
    hashes: HashMap<String, RowMeta>,
    max_timestamp: Option<String>,
}

impl CachedMeta {
    fn new() -> Self {
        Self {
            hashes: HashMap::new(),
            max_timestamp: None,
        }
    }

    /// Observe a timestamp, advancing max_timestamp if it's larger.
    ///
    /// Lexicographic comparison is correct for ISO 8601 strings, which is the
    /// documented format for `timestamp_column` values in the sync config.
    fn observe_timestamp(&mut self, ts: &str) {
        if self.max_timestamp.as_deref().unwrap_or("") < ts {
            self.max_timestamp = Some(ts.to_string());
        }
    }

    /// Merge incremental results into the cache.
    ///
    /// Rows present in `incremental` overwrite existing cache entries.
    /// Rows absent from `incremental` are preserved (unchanged rows).
    fn merge(&mut self, incremental: HashMap<String, RowMeta>) {
        for (pk, meta) in incremental {
            if let Some(ref ts) = meta.updated_at {
                self.observe_timestamp(ts);
            }
            self.hashes.insert(pk, meta);
        }
    }

    /// Seed the cache from a full scan result.
    fn seed(&mut self, full: HashMap<String, RowMeta>) {
        self.max_timestamp = None;
        for meta in full.values() {
            if let Some(ref ts) = meta.updated_at {
                self.observe_timestamp(ts);
            }
        }
        self.hashes = full;
    }
}

/// Either a remote HTTP SQL endpoint or a local SQLite executor.
///
/// Both implement `DataSource`; lib.rs holds an `AnyDataSource` for source
/// and dest and dispatches at the trait surface so the diff/sync paths
/// don't care which side is local vs remote.
pub(crate) enum AnyDataSource {
    Fetch(FetchDataSource),
    Local(LocalSqlDataSource),
}

impl AnyDataSource {
    /// Incremental row metadata query, delegated to the underlying adapter.
    /// Mirrors the inherent method on each adapter.
    pub async fn get_row_metadata_since(
        &self,
        table: &str,
        timestamp_column: &str,
        exclude_columns: &[String],
        since_timestamp: &str,
    ) -> SmugglrResult<HashMap<String, RowMeta>> {
        match self {
            Self::Fetch(d) => {
                d.get_row_metadata_since(table, timestamp_column, exclude_columns, since_timestamp)
                    .await
            }
            Self::Local(d) => {
                d.get_row_metadata_since(table, timestamp_column, exclude_columns, since_timestamp)
                    .await
            }
        }
    }
}

impl DataSource for AnyDataSource {
    async fn list_tables(&self) -> SmugglrResult<Vec<String>> {
        match self {
            Self::Fetch(d) => d.list_tables().await,
            Self::Local(d) => d.list_tables().await,
        }
    }

    async fn table_info(&self, table: &str) -> SmugglrResult<TableInfo> {
        match self {
            Self::Fetch(d) => d.table_info(table).await,
            Self::Local(d) => d.table_info(table).await,
        }
    }

    async fn get_row_metadata(
        &self,
        table: &str,
        timestamp_column: &str,
        exclude_columns: &[String],
    ) -> SmugglrResult<HashMap<String, RowMeta>> {
        match self {
            Self::Fetch(d) => {
                d.get_row_metadata(table, timestamp_column, exclude_columns)
                    .await
            }
            Self::Local(d) => {
                d.get_row_metadata(table, timestamp_column, exclude_columns)
                    .await
            }
        }
    }

    async fn get_rows(
        &self,
        table: &str,
        pk_values: &[String],
    ) -> SmugglrResult<Vec<HashMap<String, serde_json::Value>>> {
        match self {
            Self::Fetch(d) => d.get_rows(table, pk_values).await,
            Self::Local(d) => d.get_rows(table, pk_values).await,
        }
    }

    async fn upsert_rows(
        &self,
        table: &str,
        rows: &[HashMap<String, serde_json::Value>],
    ) -> SmugglrResult<usize> {
        match self {
            Self::Fetch(d) => d.upsert_rows(table, rows).await,
            Self::Local(d) => d.upsert_rows(table, rows).await,
        }
    }

    async fn row_count(&self, table: &str) -> SmugglrResult<usize> {
        match self {
            Self::Fetch(d) => d.row_count(table).await,
            Self::Local(d) => d.row_count(table).await,
        }
    }
}

#[derive(Deserialize)]
struct JsHttpEndpoint {
    url: String,
    #[serde(default, alias = "authToken")]
    auth_token: String,
    #[serde(default = "default_profile")]
    profile: String,
}

fn default_profile() -> String {
    "generic".to_string()
}

#[derive(Deserialize, Default)]
#[serde(default, rename_all = "camelCase")]
struct JsSyncConfig {
    tables: Vec<String>,
    exclude_tables: Vec<String>,
    exclude_columns: Vec<String>,
    timestamp_column: Option<String>,
    conflict_resolution: Option<String>,
    batch_size: Option<usize>,
}

#[derive(Serialize)]
struct JsSyncResult {
    command: String,
    status: String,
    tables: Vec<JsTableResult>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsTableResult {
    name: String,
    #[serde(skip_serializing_if = "is_zero")]
    rows_pushed: usize,
    #[serde(skip_serializing_if = "is_zero")]
    rows_pulled: usize,
}

fn is_zero(n: &usize) -> bool {
    *n == 0
}

#[derive(Serialize)]
struct JsDiffResult {
    command: String,
    status: String,
    tables: Vec<JsTableDiff>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsTableDiff {
    name: String,
    local_only: usize,
    remote_only: usize,
    local_newer: usize,
    remote_newer: usize,
    content_differs: usize,
    identical: usize,
}

fn parse_conflict_resolution(s: Option<&str>) -> Result<ConflictResolution, JsValue> {
    match s {
        None | Some("local_wins") => Ok(ConflictResolution::LocalWins),
        Some("remote_wins") => Ok(ConflictResolution::RemoteWins),
        Some("newer_wins") => Ok(ConflictResolution::NewerWins),
        Some("uuid_v7_wins") => Ok(ConflictResolution::UuidV7Wins),
        // Reject unknown values loudly: silently defaulting to LocalWins would
        // sync the wrong direction (data-loss-shaped) on a typo like "remoteWins".
        Some(other) => Err(JsValue::from_str(&format!(
            "unknown conflict_resolution '{}'; expected one of: local_wins, remote_wins, newer_wins, uuid_v7_wins",
            other
        ))),
    }
}

fn build_sync_config(js: &JsSyncConfig) -> Result<SyncConfig, JsValue> {
    let mut sync = SyncConfig::default();
    if !js.tables.is_empty() {
        sync.tables = js.tables.clone();
    }
    if !js.exclude_tables.is_empty() {
        sync.exclude_tables = js.exclude_tables.clone();
    }
    if !js.exclude_columns.is_empty() {
        sync.exclude_columns = js.exclude_columns.clone();
    }
    if let Some(ref ts) = js.timestamp_column {
        sync.timestamp_column = ts.clone();
    }
    sync.conflict_resolution = parse_conflict_resolution(js.conflict_resolution.as_deref())?;
    if let Some(bs) = js.batch_size {
        sync.batch_size = bs;
    }

    // This crate never goes through `parse_with_env`, so it does not inherit the
    // config validation `Config::load` / `Config::from_toml_str` get. Call it
    // explicitly.
    //
    // A no-op today: `JsSyncConfig` exposes no `convergeColumns` field, so
    // `converge_columns` is always empty here and the exclude/converge overlap
    // cannot arise. It is wired anyway, because "the guard is unreachable from
    // this path" and "the field is not exposed yet" are different facts, and only
    // the second one is true. Whoever adds `convergeColumns` to `JsSyncConfig`
    // for parity would otherwise reopen the overlap bug for every npm consumer
    // and have nothing force them to notice (#293).
    sync.validate_column_lists()
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    Ok(sync)
}

/// Dispatch endpoint config -> adapter.
///
/// Local endpoint shape: `{ type: "local", executor: SqlExecutor }` where
/// the executor is any JS object with a `run(sql, params): Promise<{columns, rows}>` method.
/// Anything else is treated as a remote HTTP endpoint and parsed as `JsHttpEndpoint`.
fn build_datasource(endpoint_js: &JsValue) -> Result<AnyDataSource, JsValue> {
    let kind = js_sys::Reflect::get(endpoint_js, &JsValue::from_str("type"))
        .ok()
        .and_then(|v| v.as_string());

    if kind.as_deref() == Some("local") {
        let executor = js_sys::Reflect::get(endpoint_js, &JsValue::from_str("executor"))
            .map_err(|e| JsValue::from_str(&format!("local endpoint missing executor: {:?}", e)))?;
        if executor.is_undefined() || executor.is_null() {
            return Err(JsValue::from_str(
                "local endpoint missing executor: must be an object with a run(sql, params) method",
            ));
        }
        Ok(AnyDataSource::Local(LocalSqlDataSource::new(executor)))
    } else {
        let http: JsHttpEndpoint = serde_wasm_bindgen::from_value(endpoint_js.clone())
            .map_err(|e| JsValue::from_str(&format!("invalid endpoint config: {}", e)))?;
        let profile = Profile::from_name(&http.profile)
            .ok_or_else(|| JsValue::from_str(&format!("unknown profile: {}", http.profile)))?;
        Ok(AnyDataSource::Fetch(FetchDataSource::new(
            http.url,
            http.auth_token,
            profile,
        )))
    }
}

/// Get tables to sync by finding the intersection of both sides,
/// filtering by config, and requiring a primary key.
async fn get_sync_tables(
    source: &AnyDataSource,
    dest: &AnyDataSource,
    sync_config: &SyncConfig,
) -> Result<Vec<String>, JsValue> {
    let source_tables: HashSet<String> = source
        .list_tables()
        .await
        .map_err(|e| JsValue::from_str(&e.to_string()))?
        .into_iter()
        .collect();
    let dest_tables: HashSet<String> = dest
        .list_tables()
        .await
        .map_err(|e| JsValue::from_str(&e.to_string()))?
        .into_iter()
        .collect();

    let considered = |t: &String| {
        (sync_config.tables.is_empty() || sync_config.tables.contains(t))
            && !sync_config.exclude_tables.iter().any(|ex| ex == t)
    };

    let mut tables = Vec::new();
    for t in source_tables.intersection(&dest_tables) {
        if !considered(t) {
            continue;
        }
        let info = source
            .table_info(t)
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        if !info.primary_key.is_empty() {
            tables.push(t.clone());
        }
    }
    tables.sort();

    // Same rule as the native engine (#438): an empty intersection with
    // tables on either side is a configuration error, not a silent no-op.
    // The lists are each side's tables unfiltered, so the message describes
    // the databases rather than the config's view of them.
    let mut source_all: Vec<String> = source_tables.iter().cloned().collect();
    let mut dest_all: Vec<String> = dest_tables.iter().cloned().collect();
    source_all.sort();
    dest_all.sort();
    smugglr_core::sync::check_table_set(&sync_config.tables, &source_all, &dest_all, &tables)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    Ok(tables)
}

/// Strip excluded columns from row data before transfer.
fn strip_excluded(
    rows: Vec<HashMap<String, serde_json::Value>>,
    exclude: &[String],
) -> Vec<HashMap<String, serde_json::Value>> {
    if exclude.is_empty() {
        return rows;
    }
    rows.into_iter()
        .map(|row| {
            row.into_iter()
                .filter(|(col, _)| !column_excluded(col, exclude))
                .collect()
        })
        .collect()
}

/// Transfer rows from source to dest in batches.
async fn transfer_rows(
    source: &AnyDataSource,
    dest: &AnyDataSource,
    table: &str,
    pk_values: &[String],
    batch_size: usize,
    exclude_columns: &[String],
) -> Result<usize, JsValue> {
    if pk_values.is_empty() {
        return Ok(0);
    }
    let rows = source
        .get_rows(table, pk_values)
        .await
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    let rows = strip_excluded(rows, exclude_columns);

    let mut total = 0;
    for chunk in rows.chunks(batch_size) {
        let count = dest
            .upsert_rows(table, chunk)
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        total += count;
    }
    Ok(total)
}

/// Populate the cache entry for `table` on `ds`, fetching only changed rows
/// when possible.
///
/// Strategy:
/// - No cache entry or no timestamp column: full scan via `get_row_metadata`,
///   seeds the cache.
/// - Cache entry with a usable cursor: incremental scan via
///   `get_row_metadata_since`, merges into the existing entry.
/// - Incremental scan failure: falls back to a full scan so a stale cache
///   never blocks sync.
///
/// After this call returns `Ok`, `cache` is guaranteed to contain an entry
/// for `table`. The caller borrows the cache to access the metadata,
/// avoiding a full clone of the hash map on the sync hot path.
async fn ensure_cached_metadata(
    ds: &AnyDataSource,
    cache: &RefCell<HashMap<String, CachedMeta>>,
    table: &str,
    timestamp_column: &str,
    exclude_columns: &[String],
) -> Result<(), JsValue> {
    let since = {
        let borrowed = cache.borrow();
        borrowed
            .get(table)
            .and_then(|c| c.max_timestamp.clone())
            .filter(|_| !timestamp_column.is_empty())
    };

    if let Some(ref ts) = since {
        if let Ok(incremental) = ds
            .get_row_metadata_since(table, timestamp_column, exclude_columns, ts)
            .await
        {
            let mut borrowed = cache.borrow_mut();
            let entry = borrowed
                .entry(table.to_string())
                .or_insert_with(CachedMeta::new);
            entry.merge(incremental);
            return Ok(());
        }
        // Incremental query failed (e.g. schema changed, timestamp column
        // missing on remote). Fall through to a full scan.
    }

    let full = ds
        .get_row_metadata(table, timestamp_column, exclude_columns)
        .await
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    let mut borrowed = cache.borrow_mut();
    let entry = borrowed
        .entry(table.to_string())
        .or_insert_with(CachedMeta::new);
    entry.seed(full);
    Ok(())
}

/// Compute the diff between source and dest for a table, using cached
/// metadata. This is the incremental diff entry point for the WASM path.
///
/// Unlike `smugglr_core::diff::diff_table`, this path does not full-scan on
/// every call: `ensure_cached_metadata` uses a per-table timestamp cursor to
/// fetch only changed rows, and the diff classification is delegated to the
/// shared `classify_diff` helper in core.
async fn diff_table_cached(
    source: &AnyDataSource,
    dest: &AnyDataSource,
    source_cache: &RefCell<HashMap<String, CachedMeta>>,
    dest_cache: &RefCell<HashMap<String, CachedMeta>>,
    table: &str,
    sync_config: &SyncConfig,
) -> Result<TableDiff, JsValue> {
    // Takes the whole SyncConfig rather than timestamp_column + exclude_columns +
    // converge_columns separately. Those three must come from one config: the
    // hash-exclusion set is the union of the latter two, and every producer of a
    // content hash has to cover the identical set or identical rows hash
    // differently and never converge (#293).
    let timestamp_column = &sync_config.timestamp_column;
    let hash_excluded = sync_config.hash_excluded_columns();

    ensure_cached_metadata(
        source,
        source_cache,
        table,
        timestamp_column,
        &hash_excluded,
    )
    .await?;
    ensure_cached_metadata(dest, dest_cache, table, timestamp_column, &hash_excluded).await?;

    let source_ref = source_cache.borrow();
    let dest_ref = dest_cache.borrow();
    let source_meta = source_ref
        .get(table)
        .ok_or_else(|| JsValue::from_str("source cache missing after populate"))?;
    let dest_meta = dest_ref
        .get(table)
        .ok_or_else(|| JsValue::from_str("dest cache missing after populate"))?;

    Ok(classify_diff(
        &source_meta.hashes,
        &dest_meta.hashes,
        table,
        sync_config.converge_columns.is_empty(),
    ))
}

#[wasm_bindgen]
pub struct Smugglr {
    sync_config: SyncConfig,
    source: AnyDataSource,
    // Interior-mutable so `updateDest()` can swap the entire endpoint and
    // `updateAuth()` can reach into the underlying FetchDataSource. The
    // borrow is held across awaits inside push/pull/sync; do not call
    // updateAuth/updateDest while a sync future is pending.
    //
    // `None` means "anonymous-first" -- the instance was constructed with
    // no dest. push/pull/sync return a clear error in that state; diff
    // degrades to a local-only inventory. Attach a dest later via
    // `updateDest()`.
    dest: RefCell<Option<AnyDataSource>>,
    /// Per-table metadata cache for the source endpoint.
    source_cache: RefCell<HashMap<String, CachedMeta>>,
    /// Per-table metadata cache for the dest endpoint.
    dest_cache: RefCell<HashMap<String, CachedMeta>>,
    /// Registered `table-changed` listeners. Each entry is (id, callback);
    /// the id lets the JS-side unsubscribe closure remove its own slot.
    listeners: RefCell<Vec<(u64, js_sys::Function)>>,
    next_listener_id: RefCell<u64>,
}

#[wasm_bindgen]
impl Smugglr {
    /// Initialize smugglr with source and destination endpoints.
    ///
    /// Each call to `init` returns a new Smugglr instance with empty caches.
    /// Passing a different endpoint config automatically invalidates the old
    /// caches because the old instance is discarded.
    #[wasm_bindgen]
    pub fn init(config_js: JsValue) -> Result<Smugglr, JsValue> {
        let source_js = js_sys::Reflect::get(&config_js, &JsValue::from_str("source"))
            .map_err(|e| JsValue::from_str(&format!("config.source missing: {:?}", e)))?;
        // dest is optional -- "anonymous-first" mode runs entirely locally.
        let dest_js = js_sys::Reflect::get(&config_js, &JsValue::from_str("dest"))
            .unwrap_or(JsValue::UNDEFINED);
        let sync_js = js_sys::Reflect::get(&config_js, &JsValue::from_str("sync"))
            .unwrap_or(JsValue::UNDEFINED);

        let js_sync: JsSyncConfig = if sync_js.is_undefined() || sync_js.is_null() {
            JsSyncConfig::default()
        } else {
            serde_wasm_bindgen::from_value(sync_js)
                .map_err(|e| JsValue::from_str(&format!("invalid sync config: {}", e)))?
        };

        let sync_config = build_sync_config(&js_sync)?;
        let source = build_datasource(&source_js)?;
        let dest = if dest_js.is_undefined() || dest_js.is_null() {
            None
        } else {
            Some(build_datasource(&dest_js)?)
        };

        Ok(Smugglr {
            sync_config,
            source,
            dest: RefCell::new(dest),
            source_cache: RefCell::new(HashMap::new()),
            dest_cache: RefCell::new(HashMap::new()),
            listeners: RefCell::new(Vec::new()),
            next_listener_id: RefCell::new(0),
        })
    }

    /// Subscribe to events emitted by this instance.
    ///
    /// Currently the only supported event is `"table-changed"`, which fires
    /// once per affected table after a `pull` or `sync` operation completes
    /// the local write. The handler receives a `TableChangedEvent`:
    ///
    /// ```ts
    /// interface TableChangedEvent {
    ///   table: string;
    ///   changedPks: string[];
    ///   removedPks: string[];   // reserved; always [] until delete propagation lands
    ///   source: 'pull' | 'sync';
    /// }
    /// ```
    ///
    /// Returns an unsubscribe function. Calling it removes the listener;
    /// subsequent invocations are no-ops.
    #[wasm_bindgen]
    pub fn on(&self, event: &str, callback: js_sys::Function) -> Result<js_sys::Function, JsValue> {
        if event != "table-changed" {
            return Err(JsValue::from_str(&format!(
                "unknown event '{}': only 'table-changed' is supported",
                event
            )));
        }
        let id = {
            let mut next = self.next_listener_id.borrow_mut();
            let v = *next;
            *next = next.wrapping_add(1);
            v
        };
        self.listeners.borrow_mut().push((id, callback));

        // Build the unsubscribe closure. We reach back into the same Smugglr
        // via a raw pointer because wasm-bindgen cannot capture `&self` into
        // a JS-callable closure with a non-'static lifetime. This is safe in
        // single-threaded WASM as long as the Smugglr instance outlives the
        // unsubscribe call -- which it must, since the JS-side wrapper holds
        // it and dispose() consumes it explicitly.
        let self_ptr: *const Smugglr = self;
        let cb = Closure::wrap(Box::new(move || {
            // Safety: see comment above.
            let s = unsafe { &*self_ptr };
            s.listeners.borrow_mut().retain(|(other, _)| *other != id);
        }) as Box<dyn Fn()>);
        let func: js_sys::Function = cb.as_ref().unchecked_ref::<js_sys::Function>().clone();
        cb.forget();
        Ok(func)
    }

    fn emit_table_changed(&self, table: &str, changed_pks: &[String], origin: &str) {
        if changed_pks.is_empty() {
            return;
        }
        let listeners = self.listeners.borrow().clone();
        if listeners.is_empty() {
            return;
        }
        let event = match build_table_changed_event(table, changed_pks, origin) {
            Ok(v) => v,
            Err(_) => return,
        };
        let this = JsValue::NULL;
        for (_id, cb) in listeners.iter() {
            let _ = cb.call1(&this, &event);
        }
    }

    /// Clear all cached row metadata for both endpoints.
    ///
    /// The next call to push/pull/sync/diff will perform a full scan on both
    /// sides before resuming incremental mode. Use this when you know the
    /// remote schema or data has changed in a way the incremental path cannot
    /// detect (e.g. mass deletes, schema migration).
    #[wasm_bindgen(js_name = clearCache)]
    pub fn clear_cache(&self) {
        self.source_cache.borrow_mut().clear();
        self.dest_cache.borrow_mut().clear();
    }

    /// Erase local state. Issues `DELETE FROM <table>` against the local
    /// SQLite database for every configured sync table, then clears the
    /// in-memory metadata caches. The schema and any non-synced tables are
    /// untouched.
    ///
    /// The dest endpoint is not touched -- server-side erasure is the app's
    /// concern, typically through its auth/account system.
    ///
    /// At least one of source / dest must be a local executor; calling this
    /// on a HTTP-only configuration errors out.
    #[allow(clippy::await_holding_refcell_ref)]
    #[wasm_bindgen(js_name = eraseLocal)]
    pub async fn erase_local(&self) -> Result<JsValue, JsValue> {
        let dest = self.dest.borrow();
        let local = if let AnyDataSource::Local(d) = &self.source {
            d
        } else if let Some(AnyDataSource::Local(d)) = &*dest {
            d
        } else {
            return Err(JsValue::from_str(
                "eraseLocal: neither source nor dest is a local executor",
            ));
        };

        let tables = if !self.sync_config.tables.is_empty() {
            self.sync_config.tables.clone()
        } else {
            local
                .list_tables()
                .await
                .map_err(|e| JsValue::from_str(&format!("eraseLocal: list_tables failed: {}", e)))?
        };

        let mut erased = Vec::new();
        for table in &tables {
            if self.sync_config.exclude_tables.iter().any(|t| t == table) {
                continue;
            }
            local.delete_all_rows(table).await.map_err(|e| {
                JsValue::from_str(&format!("eraseLocal: delete from {} failed: {}", table, e))
            })?;
            erased.push(table.clone());
        }

        drop(dest);
        self.source_cache.borrow_mut().clear();
        self.dest_cache.borrow_mut().clear();

        let arr = js_sys::Array::new();
        for t in &erased {
            arr.push(&JsValue::from_str(t));
        }
        let obj = js_sys::Object::new();
        js_sys::Reflect::set(&obj, &JsValue::from_str("erasedTables"), &arr)?;
        Ok(obj.into())
    }

    /// Replace the dest auth token. The dest URL, profile, and metadata
    /// cache are unchanged -- the next request just uses the new token.
    /// Errors if the dest is not an HTTP endpoint.
    ///
    /// **Do not call while a sync future is pending.** Await any in-flight
    /// push/pull/sync first; the implementation borrows the dest across
    /// awaits and a concurrent mutation will panic.
    #[wasm_bindgen(js_name = updateAuth)]
    pub fn update_auth(&self, auth_token: String) -> Result<(), JsValue> {
        let dest = self.dest.borrow();
        match dest.as_ref() {
            Some(AnyDataSource::Fetch(d)) => {
                d.set_auth_token(auth_token);
                Ok(())
            }
            Some(AnyDataSource::Local(_)) => Err(JsValue::from_str(
                "updateAuth: dest is a local executor, not an HTTP endpoint",
            )),
            None => Err(JsValue::from_str(
                "updateAuth: no dest configured. Call updateDest() first to attach an endpoint.",
            )),
        }
    }

    /// Replace the entire dest endpoint. Accepts the same shape as the
    /// `dest` field of `Smugglr.init({...})`. Clears the dest metadata
    /// cache so the next sync re-scans against the new endpoint; the
    /// source cache is untouched.
    ///
    /// Use this for the anonymous -> account-upgrade flow: start with
    /// an anonymous ingress dest, swap to the account-bound dest after
    /// the user signs in. Local OPFS data and the source cache survive.
    ///
    /// **Do not call while a sync future is pending.** Same caveat as
    /// `updateAuth` -- await first.
    #[wasm_bindgen(js_name = updateDest)]
    pub fn update_dest(&self, dest_js: JsValue) -> Result<(), JsValue> {
        let new_dest = build_datasource(&dest_js)?;
        *self.dest.borrow_mut() = Some(new_dest);
        self.dest_cache.borrow_mut().clear();
        Ok(())
    }

    /// Push source rows to destination.
    // RefCell borrow held across await is safe here: WASM is single-threaded
    // and `updateAuth` / `updateDest` are documented as not safe to call
    // concurrently with sync futures. See struct docstring on `dest`.
    #[allow(clippy::await_holding_refcell_ref)]
    #[wasm_bindgen]
    pub async fn push(&self, dry_run: Option<bool>) -> Result<JsValue, JsValue> {
        let dest_ref = self.dest.borrow();
        let dest = dest_ref.as_ref().ok_or_else(|| {
            JsValue::from_str(
                "push: no dest configured. Pass `dest` to Smugglr.init() to enable network sync.",
            )
        })?;
        let dry_run = dry_run.unwrap_or(false);
        let tables = get_sync_tables(&self.source, dest, &self.sync_config).await?;
        let conflict = self.sync_config.conflict_resolution;
        let batch_size = self.sync_config.batch_size;

        let mut results = Vec::new();
        for table in &tables {
            let diff = diff_table_cached(
                &self.source,
                dest,
                &self.source_cache,
                &self.dest_cache,
                table,
                &self.sync_config,
            )
            .await?;

            let to_push = diff.rows_to_push(conflict);
            let pushed = if dry_run || to_push.is_empty() {
                to_push.len()
            } else {
                transfer_rows(
                    &self.source,
                    dest,
                    table,
                    &to_push,
                    batch_size,
                    &self.sync_config.exclude_columns,
                )
                .await?
            };

            results.push(JsTableResult {
                name: table.clone(),
                rows_pushed: pushed,
                rows_pulled: 0,
            });
        }

        let output = JsSyncResult {
            command: "push".into(),
            status: if dry_run { "dry_run" } else { "ok" }.into(),
            tables: results,
        };
        serde_wasm_bindgen::to_value(&output)
            .map_err(|e| JsValue::from_str(&format!("serialization failed: {}", e)))
    }

    /// Pull destination rows to source.
    #[allow(clippy::await_holding_refcell_ref)]
    #[wasm_bindgen]
    pub async fn pull(&self, dry_run: Option<bool>) -> Result<JsValue, JsValue> {
        let dest_ref = self.dest.borrow();
        let dest = dest_ref.as_ref().ok_or_else(|| {
            JsValue::from_str(
                "pull: no dest configured. Pass `dest` to Smugglr.init() to enable network sync.",
            )
        })?;
        let dry_run = dry_run.unwrap_or(false);
        let tables = get_sync_tables(&self.source, dest, &self.sync_config).await?;
        let conflict = self.sync_config.conflict_resolution;
        let batch_size = self.sync_config.batch_size;

        let mut results = Vec::new();
        for table in &tables {
            let diff = diff_table_cached(
                &self.source,
                dest,
                &self.source_cache,
                &self.dest_cache,
                table,
                &self.sync_config,
            )
            .await?;

            let to_pull = diff.rows_to_pull(conflict);
            let pulled = if dry_run || to_pull.is_empty() {
                to_pull.len()
            } else {
                let n = transfer_rows(
                    dest,
                    &self.source,
                    table,
                    &to_pull,
                    batch_size,
                    &self.sync_config.exclude_columns,
                )
                .await?;
                if n > 0 {
                    self.emit_table_changed(table, &to_pull, "pull");
                }
                n
            };

            results.push(JsTableResult {
                name: table.clone(),
                rows_pushed: 0,
                rows_pulled: pulled,
            });
        }

        let output = JsSyncResult {
            command: "pull".into(),
            status: if dry_run { "dry_run" } else { "ok" }.into(),
            tables: results,
        };
        serde_wasm_bindgen::to_value(&output)
            .map_err(|e| JsValue::from_str(&format!("serialization failed: {}", e)))
    }

    /// Bidirectional sync.
    #[allow(clippy::await_holding_refcell_ref)]
    #[wasm_bindgen]
    pub async fn sync(&self, dry_run: Option<bool>) -> Result<JsValue, JsValue> {
        let dest_ref = self.dest.borrow();
        let dest = dest_ref.as_ref().ok_or_else(|| {
            JsValue::from_str(
                "sync: no dest configured. Pass `dest` to Smugglr.init() to enable network sync.",
            )
        })?;
        let dry_run = dry_run.unwrap_or(false);
        let tables = get_sync_tables(&self.source, dest, &self.sync_config).await?;
        let conflict = self.sync_config.conflict_resolution;
        let batch_size = self.sync_config.batch_size;

        let mut results = Vec::new();
        for table in &tables {
            let diff = diff_table_cached(
                &self.source,
                dest,
                &self.source_cache,
                &self.dest_cache,
                table,
                &self.sync_config,
            )
            .await?;

            let to_push = diff.rows_to_push(conflict);
            let to_pull = diff.rows_to_pull(conflict);

            let pushed = if dry_run || to_push.is_empty() {
                to_push.len()
            } else {
                transfer_rows(
                    &self.source,
                    dest,
                    table,
                    &to_push,
                    batch_size,
                    &self.sync_config.exclude_columns,
                )
                .await?
            };

            let pulled = if dry_run || to_pull.is_empty() {
                to_pull.len()
            } else {
                let n = transfer_rows(
                    dest,
                    &self.source,
                    table,
                    &to_pull,
                    batch_size,
                    &self.sync_config.exclude_columns,
                )
                .await?;
                if n > 0 {
                    self.emit_table_changed(table, &to_pull, "sync");
                }
                n
            };

            results.push(JsTableResult {
                name: table.clone(),
                rows_pushed: pushed,
                rows_pulled: pulled,
            });
        }

        let output = JsSyncResult {
            command: "sync".into(),
            status: if dry_run { "dry_run" } else { "ok" }.into(),
            tables: results,
        };
        serde_wasm_bindgen::to_value(&output)
            .map_err(|e| JsValue::from_str(&format!("serialization failed: {}", e)))
    }

    /// Read-only diff between source and destination.
    #[allow(clippy::await_holding_refcell_ref)]
    #[wasm_bindgen]
    pub async fn diff(&self) -> Result<JsValue, JsValue> {
        let dest_ref = self.dest.borrow();

        // Local-only mode: enumerate source tables and report row counts as
        // `local_only`. No network is touched.
        let Some(dest) = dest_ref.as_ref() else {
            let local_tables = self
                .source
                .list_tables()
                .await
                .map_err(|e| JsValue::from_str(&format!("list_tables failed: {}", e)))?;
            let mut table_diffs = Vec::new();
            for table in local_tables {
                if !self.sync_config.tables.is_empty() && !self.sync_config.tables.contains(&table)
                {
                    continue;
                }
                if self.sync_config.exclude_tables.iter().any(|t| t == &table) {
                    continue;
                }
                let n = self
                    .source
                    .row_count(&table)
                    .await
                    .map_err(|e| JsValue::from_str(&format!("row_count failed: {}", e)))?;
                table_diffs.push(JsTableDiff {
                    name: table,
                    local_only: n,
                    remote_only: 0,
                    local_newer: 0,
                    remote_newer: 0,
                    content_differs: 0,
                    identical: 0,
                });
            }
            let output = JsDiffResult {
                command: "diff".into(),
                status: "ok".into(),
                tables: table_diffs,
            };
            return serde_wasm_bindgen::to_value(&output)
                .map_err(|e| JsValue::from_str(&format!("serialization failed: {}", e)));
        };

        let tables = get_sync_tables(&self.source, dest, &self.sync_config).await?;

        let mut table_diffs = Vec::new();
        for table in &tables {
            let diff = diff_table_cached(
                &self.source,
                dest,
                &self.source_cache,
                &self.dest_cache,
                table,
                &self.sync_config,
            )
            .await?;

            let stats = diff.stats();
            table_diffs.push(JsTableDiff {
                name: table.clone(),
                local_only: stats.local_only,
                remote_only: stats.remote_only,
                local_newer: stats.local_newer,
                remote_newer: stats.remote_newer,
                content_differs: stats.content_differs,
                identical: stats.identical,
            });
        }

        let output = JsDiffResult {
            command: "diff".into(),
            status: "ok".into(),
            tables: table_diffs,
        };
        serde_wasm_bindgen::to_value(&output)
            .map_err(|e| JsValue::from_str(&format!("serialization failed: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use smugglr_core::datasource::RowMeta;

    // ---------------------------------------------------------------------------
    // CachedMeta unit tests (no network, no WASM runtime needed)
    // ---------------------------------------------------------------------------

    fn make_meta(pk: &str, ts: Option<&str>, hash: &str) -> RowMeta {
        RowMeta {
            pk_value: pk.to_string(),
            updated_at: ts.map(String::from),
            content_hash: hash.to_string(),
        }
    }

    #[test]
    fn cached_meta_seed_computes_max_timestamp() {
        let mut cache = CachedMeta::new();
        let mut full = HashMap::new();
        full.insert("pk1".into(), make_meta("pk1", Some("2024-01-01"), "hash1"));
        full.insert("pk2".into(), make_meta("pk2", Some("2024-06-15"), "hash2"));
        full.insert("pk3".into(), make_meta("pk3", Some("2024-03-20"), "hash3"));
        cache.seed(full);

        assert_eq!(cache.max_timestamp.as_deref(), Some("2024-06-15"));
        assert_eq!(cache.hashes.len(), 3);
    }

    #[test]
    fn cached_meta_seed_with_no_timestamps_leaves_max_timestamp_none() {
        let mut cache = CachedMeta::new();
        let mut full = HashMap::new();
        full.insert("pk1".into(), make_meta("pk1", None, "hash1"));
        cache.seed(full);

        assert!(cache.max_timestamp.is_none());
        assert_eq!(cache.hashes.len(), 1);
    }

    #[test]
    fn cached_meta_merge_updates_existing_rows() {
        let mut cache = CachedMeta::new();
        let mut full = HashMap::new();
        full.insert(
            "pk1".into(),
            make_meta("pk1", Some("2024-01-01"), "old_hash"),
        );
        full.insert("pk2".into(), make_meta("pk2", Some("2024-01-01"), "hash2"));
        cache.seed(full);

        // pk1 changes, pk3 appears
        let mut incremental = HashMap::new();
        incremental.insert(
            "pk1".into(),
            make_meta("pk1", Some("2024-06-15"), "new_hash"),
        );
        incremental.insert("pk3".into(), make_meta("pk3", Some("2024-07-01"), "hash3"));
        cache.merge(incremental);

        assert_eq!(cache.hashes.len(), 3);
        assert_eq!(
            cache.hashes["pk1"].content_hash, "new_hash",
            "existing row should be overwritten"
        );
        assert_eq!(cache.max_timestamp.as_deref(), Some("2024-07-01"));
    }

    #[test]
    fn cached_meta_merge_advances_max_timestamp() {
        let mut cache = CachedMeta::new();
        let mut full = HashMap::new();
        full.insert("pk1".into(), make_meta("pk1", Some("2024-01-01"), "h1"));
        cache.seed(full);

        let mut inc = HashMap::new();
        inc.insert("pk2".into(), make_meta("pk2", Some("2025-01-01"), "h2"));
        cache.merge(inc);

        assert_eq!(cache.max_timestamp.as_deref(), Some("2025-01-01"));
    }

    // `ensure_cached_metadata` and `diff_table_cached` are not exercised in host
    // tests: the crate is gated on target_arch="wasm32" (see crate-level cfg),
    // so FetchDataSource and the async paths only compile under wasm-pack test.
    // The network-level fetch-count acceptance test lives in that suite.
    //
    // The classification logic used by the cached path lives in
    // `smugglr_core::diff::classify_diff`, which is unit-tested via the core
    // integration tests. The tests below verify the WASM-local pieces: cache
    // seeding and merging behavior, and a sanity check on the exported
    // classification for the inputs the cached path feeds it.

    #[test]
    fn warm_cache_with_no_timestamp_column_stays_as_seed() {
        let mut entry = CachedMeta::new();
        let mut full = HashMap::new();
        full.insert("pk1".into(), make_meta("pk1", None, "h1"));
        entry.seed(full);

        assert!(entry.max_timestamp.is_none());
        assert_eq!(entry.hashes.len(), 1);
    }

    #[wasm_bindgen_test::wasm_bindgen_test]
    fn empty_intersection_is_a_config_error_in_the_wasm_client_too() {
        // #438: the browser and Node paths apply the same rule as the CLI.
        let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        let err =
            smugglr_core::sync::check_table_set(&[], &s(&["orders"]), &s(&["unrelated"]), &[])
                .expect_err("no shared table must be an error");
        let msg = err.to_string();
        assert!(msg.contains("Configuration error"), "{msg}");
        assert!(msg.contains("Local tables: [\"orders\"]"), "{msg}");
        assert!(msg.contains("Remote tables: [\"unrelated\"]"), "{msg}");
        assert!(smugglr_core::sync::check_table_set(&[], &[], &[], &[]).is_ok());
    }

    // Was `#[test]`, which NEVER RAN: this crate is `#![cfg(target_arch =
    // "wasm32")]`, so host `cargo test` compiles it to nothing, and `wasm-pack
    // test` collects only `#[wasm_bindgen_test]`. The assertions type-checked and
    // never executed. Surfaced by review of #293 (which changed this call site).
    #[wasm_bindgen_test::wasm_bindgen_test]
    fn classify_diff_local_only_remote_only() {
        let mut source_meta = HashMap::new();
        source_meta.insert("pk1".into(), make_meta("pk1", Some("2024-01-01"), "h1"));
        source_meta.insert("pk2".into(), make_meta("pk2", Some("2024-01-01"), "h2"));

        let mut dest_meta = HashMap::new();
        dest_meta.insert("pk2".into(), make_meta("pk2", Some("2024-01-01"), "h2"));
        dest_meta.insert("pk3".into(), make_meta("pk3", Some("2024-01-01"), "h3"));

        let diff = classify_diff(&source_meta, &dest_meta, "test_table", true);

        assert_eq!(diff.local_only, vec!["pk1".to_string()]);
        assert_eq!(diff.remote_only, vec!["pk3".to_string()]);
        assert_eq!(diff.identical, vec!["pk2".to_string()]);
        assert!(diff.content_differs.is_empty());
    }

    // Same as above: previously `#[test]` and therefore never executed.
    #[wasm_bindgen_test::wasm_bindgen_test]
    fn classify_diff_timestamp_newer_wins() {
        let mut source_meta = HashMap::new();
        source_meta.insert(
            "pk1".into(),
            make_meta("pk1", Some("2024-06-01"), "hash_new"),
        );

        let mut dest_meta = HashMap::new();
        dest_meta.insert(
            "pk1".into(),
            make_meta("pk1", Some("2024-01-01"), "hash_old"),
        );

        let diff = classify_diff(&source_meta, &dest_meta, "test_table", true);

        assert_eq!(diff.local_newer, vec!["pk1".to_string()]);
        assert!(diff.remote_newer.is_empty());
    }

    #[test]
    fn clear_cache_empties_all_entries() {
        // Simulate what Smugglr::clear_cache does on the RefCells.
        let cache: RefCell<HashMap<String, CachedMeta>> = RefCell::new(HashMap::new());
        {
            let mut entry = CachedMeta::new();
            let mut full = HashMap::new();
            full.insert("pk1".into(), make_meta("pk1", Some("2024-01-01"), "h1"));
            entry.seed(full);
            cache.borrow_mut().insert("users".into(), entry);
        }
        assert_eq!(cache.borrow().len(), 1);

        cache.borrow_mut().clear();
        assert_eq!(cache.borrow().len(), 0);
    }

    #[test]
    fn incremental_merge_does_not_lose_rows_absent_from_incremental() {
        // Rows not returned by the incremental query (unchanged) stay in cache.
        let mut cache = CachedMeta::new();
        let mut full = HashMap::new();
        // 5 rows in initial full scan
        for i in 1..=5u32 {
            full.insert(
                format!("pk{}", i),
                make_meta(&format!("pk{}", i), Some("2024-01-01"), &format!("h{}", i)),
            );
        }
        cache.seed(full);
        assert_eq!(cache.hashes.len(), 5);

        // Only pk6 is new in the incremental scan (pk1..5 are unchanged and absent)
        let mut incremental = HashMap::new();
        incremental.insert("pk6".into(), make_meta("pk6", Some("2024-06-01"), "h6"));
        cache.merge(incremental);

        assert_eq!(
            cache.hashes.len(),
            6,
            "merge must preserve rows absent from incremental result"
        );
        assert_eq!(cache.max_timestamp.as_deref(), Some("2024-06-01"));
    }

    // Guards the cache-merge half of #199 (same-tick row admitted). The `>=`
    // SQL predicate itself is pinned by
    // `adapter_common::tests::incremental_metadata_sql_uses_inclusive_predicate`.
    #[wasm_bindgen_test::wasm_bindgen_test]
    fn boundary_tick_row_is_admitted_via_inclusive_incremental() {
        // Regression for #199: a row written at exactly the cached cursor tick
        // AFTER the seed scan must still be admitted on the next incremental
        // pass. The `get_row_metadata_since` predicate is now `>=` (inclusive),
        // so the incremental result includes the boundary tick. This test
        // models the cache half of that contract: merging a same-tick new row
        // admits it without disturbing existing boundary rows.
        let mut cache = CachedMeta::new();
        let mut full = HashMap::new();
        // Seed established the cursor at "2024-06-01" (whole-second granularity).
        full.insert("pk1".into(), make_meta("pk1", Some("2024-06-01"), "h1"));
        cache.seed(full);
        assert_eq!(cache.max_timestamp.as_deref(), Some("2024-06-01"));

        // An inclusive (`>=`) incremental scan against the cursor re-fetches the
        // boundary tick: it returns the already-seen pk1 plus a genuinely-new
        // pk2 written in the SAME tick after the seed. A strict `>` scan would
        // have returned neither, silently dropping pk2 forever.
        let mut incremental = HashMap::new();
        incremental.insert("pk1".into(), make_meta("pk1", Some("2024-06-01"), "h1"));
        incremental.insert("pk2".into(), make_meta("pk2", Some("2024-06-01"), "h2_new"));
        cache.merge(incremental);

        assert_eq!(
            cache.hashes.len(),
            2,
            "same-tick boundary row must be admitted, not dropped"
        );
        assert_eq!(
            cache.hashes["pk2"].content_hash, "h2_new",
            "new boundary-tick row must land in the cache"
        );
        // Re-fetching pk1 at the same tick is idempotent: no duplication, cursor
        // unchanged.
        assert_eq!(cache.max_timestamp.as_deref(), Some("2024-06-01"));
        assert_eq!(cache.hashes["pk1"].content_hash, "h1");
    }
}
