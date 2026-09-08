//! Local SQLite adapter -- runs SQL against a JS-provided executor.
//!
//! Rust speaks SQL strings + parameter arrays; the JS side provides any
//! object satisfying `{ run(sql, params): Promise<{columns, rows}> }`.
//! First shipped executor is wa-sqlite + OPFS; the same adapter accepts
//! better-sqlite3 (Node), official sqlite-wasm, or sql.js without
//! changes here.

use smugglr_core::config::DuplicatePkPolicy;
use smugglr_core::datasource::{DataSource, RowMeta, TableInfo};
use smugglr_core::error::{Result, SyncError};
use std::collections::HashMap;

use crate::adapter_common;

use serde_json::Value;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;

/// Conservative per-statement bind-parameter cap for the local executor.
///
/// The JS executor may be backed by sql.js, official sqlite-wasm,
/// better-sqlite3, or wa-sqlite. The most restrictive historical default for
/// `SQLITE_MAX_VARIABLE_NUMBER` is 999, so we chunk a batch upsert so no single
/// `INSERT OR REPLACE` exceeds it. wa-sqlite (32766) and newer builds survive
/// larger batches, but capping at the lowest common limit keeps the adapter
/// correct against every documented executor. Mirrors the `max_bind_params`
/// guard in `FetchDataSource::upsert_rows`.
const LOCAL_MAX_BIND_PARAMS: usize = 999;

/// Largest number of rows that fit under `LOCAL_MAX_BIND_PARAMS` for a
/// statement with `num_columns` columns per row. Always at least 1 so a very
/// wide single row is still attempted (and fails loudly at the executor rather
/// than silently emitting an empty batch).
fn max_rows_per_batch(num_columns: usize) -> usize {
    if num_columns == 0 {
        return 1;
    }
    (LOCAL_MAX_BIND_PARAMS / num_columns).max(1)
}

pub struct LocalSqlDataSource {
    executor: JsValue,
    table_info_cache: std::sync::Mutex<HashMap<String, TableInfo>>,
}

// wasm32 is single-threaded; the !Sync of JsValue would be load-bearing on
// multi-threaded targets, but here no cross-thread sharing is possible.
// Same pattern the rest of the wasm-bindgen ecosystem uses for storing
// JsValues in long-lived structs.
unsafe impl Send for LocalSqlDataSource {}
unsafe impl Sync for LocalSqlDataSource {}

impl LocalSqlDataSource {
    pub fn new(executor: JsValue) -> Self {
        Self {
            executor,
            table_info_cache: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Issue `DELETE FROM "<table>"`, leaving the schema in place.
    /// Used by `Smugglr.eraseLocal()` for GDPR-style local wipes.
    pub(crate) async fn delete_all_rows(&self, table: &str) -> Result<()> {
        let sql = format!("DELETE FROM \"{}\"", table);
        self.run(&sql, &[]).await?;
        Ok(())
    }

    async fn run(&self, sql: &str, params: &[Value]) -> Result<RunResult> {
        let run_fn = js_sys::Reflect::get(&self.executor, &JsValue::from_str("run"))
            .map_err(|e| SyncError::Remote(format!("executor.run missing: {:?}", e)))?
            .dyn_into::<js_sys::Function>()
            .map_err(|_| SyncError::Remote("executor.run is not a function".into()))?;

        let params_js = serde_wasm_bindgen::to_value(params)
            .map_err(|e| SyncError::Remote(format!("failed to serialize params: {}", e)))?;

        let promise = run_fn
            .call2(&self.executor, &JsValue::from_str(sql), &params_js)
            .map_err(|e| SyncError::Remote(format!("executor.run threw: {:?}", e)))?
            .dyn_into::<js_sys::Promise>()
            .map_err(|_| SyncError::Remote("executor.run did not return a Promise".into()))?;

        let result_js = JsFuture::from(promise)
            .await
            .map_err(|e| SyncError::Remote(format!("executor.run rejected: {:?}", e)))?;

        serde_wasm_bindgen::from_value(result_js)
            .map_err(|e| SyncError::Remote(format!("invalid executor result shape: {}", e)))
    }

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
        let sql = smugglr_core::http_sql::incremental_metadata_sql(
            table,
            &info.primary_key,
            timestamp_column,
        );
        let params = vec![Value::String(since_timestamp.to_string())];
        let result = self.run(&sql, &params).await?;
        let mut maps = adapter_common::rows_to_maps(&result.columns, &result.rows);
        for w in smugglr_core::http_sql::canonicalize_json_blobs(&mut maps, &info) {
            web_sys::console::warn_1(&format!("smugglr: {}", w).into());
        }

        let (meta, warnings) = smugglr_core::http_sql::row_maps_to_metadata(
            &maps,
            &column_order,
            timestamp_column,
            exclude_columns,
            table,
            DuplicatePkPolicy::default(),
        )?;
        for w in warnings {
            web_sys::console::warn_1(&format!("smugglr: {}", w).into());
        }
        Ok(meta)
    }

    async fn cached_table_info(&self, table: &str) -> Result<TableInfo> {
        smugglr_core::http_sql::cached_table_info(
            &self.table_info_cache,
            table,
            self.table_info(table),
        )
        .await
    }
}

#[derive(serde::Deserialize)]
struct RunResult {
    #[serde(default)]
    columns: Vec<String>,
    #[serde(default)]
    rows: Vec<Vec<Value>>,
}

impl DataSource for LocalSqlDataSource {
    async fn list_tables(&self) -> Result<Vec<String>> {
        let result = self
            .run(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
                &[],
            )
            .await?;

        let name_idx = result.columns.iter().position(|c| c == "name").unwrap_or(0);
        Ok(result
            .rows
            .iter()
            .filter_map(|row| row.get(name_idx).and_then(|v| v.as_str()).map(String::from))
            .collect())
    }

    async fn table_info(&self, table: &str) -> Result<TableInfo> {
        let result = self
            .run(&format!("PRAGMA table_info('{}')", table), &[])
            .await?;
        Ok(smugglr_core::http_sql::parse_table_info(
            table,
            &result.columns,
            &result.rows,
        ))
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
        let result = self.run(&sql, &[]).await?;
        let mut maps = adapter_common::rows_to_maps(&result.columns, &result.rows);
        for w in smugglr_core::http_sql::canonicalize_json_blobs(&mut maps, &info) {
            web_sys::console::warn_1(&format!("smugglr: {}", w).into());
        }

        let (meta, warnings) = smugglr_core::http_sql::row_maps_to_metadata(
            &maps,
            &column_order,
            timestamp_column,
            exclude_columns,
            table,
            DuplicatePkPolicy::default(),
        )?;
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
        if pk_values.is_empty() {
            return Ok(vec![]);
        }

        let info = self.cached_table_info(table).await?;
        let sql = smugglr_core::rowhash::pk_in_query(table, &info.primary_key, pk_values.len())?;
        let params: Vec<Value> = pk_values.iter().map(|v| Value::String(v.clone())).collect();

        let result = self.run(&sql, &params).await?;
        Ok(adapter_common::rows_to_maps(&result.columns, &result.rows))
    }

    async fn upsert_rows(&self, table: &str, rows: &[HashMap<String, Value>]) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        let columns: Vec<String> = rows[0].keys().cloned().collect();
        let batch_size = max_rows_per_batch(columns.len());

        let mut total = 0;
        for batch in rows.chunks(batch_size) {
            let (sql, params) = adapter_common::generate_batch_sql(table, &columns, batch);
            self.run(&sql, &params).await.map_err(|e| {
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
        let result = self.run(&sql, &[]).await?;
        let count = result
            .rows
            .first()
            .and_then(|r| r.first())
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        Ok(count as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use wasm_bindgen_test::wasm_bindgen_test;

    fn make_rows(n: usize, num_columns: usize) -> Vec<HashMap<String, Value>> {
        (0..n)
            .map(|r| {
                (0..num_columns)
                    .map(|c| (format!("col{}", c), Value::from(r as i64 * 100 + c as i64)))
                    .collect()
            })
            .collect()
    }

    #[wasm_bindgen_test]
    fn max_rows_per_batch_respects_variable_limit() {
        // 10 columns -> 99 rows -> 990 binds (<= 999), 100 rows would be 1000.
        assert_eq!(max_rows_per_batch(10), 99);
        // single wide row always attempted.
        assert_eq!(max_rows_per_batch(2000), 1);
        // degenerate zero-column input does not divide by zero.
        assert_eq!(max_rows_per_batch(0), 1);
    }

    #[wasm_bindgen_test]
    fn upsert_batching_keeps_each_statement_under_sqlite_var_limit() {
        // Regression for #196: a default batch_size of 100 rows at >9 columns
        // exceeds the historical 999-variable limit when emitted as ONE
        // INSERT OR REPLACE. The chunking must keep every emitted statement's
        // bind-parameter count <= LOCAL_MAX_BIND_PARAMS.
        let num_columns = 10usize;
        let rows = make_rows(100, num_columns);
        let columns: Vec<String> = rows[0].keys().cloned().collect();

        // Pre-fix behavior would emit 100 * 10 = 1000 binds in one statement.
        let single = adapter_common::generate_batch_sql("t", &columns, &rows);
        assert!(
            single.1.len() > LOCAL_MAX_BIND_PARAMS,
            "unchunked statement should exceed the limit (proves the bug exists)"
        );

        // Post-fix: chunk and assert every statement stays within the cap, and
        // the total rows covered equals the input.
        let batch_size = max_rows_per_batch(num_columns);
        let mut covered = 0;
        for batch in rows.chunks(batch_size) {
            let (_sql, params) = adapter_common::generate_batch_sql("t", &columns, batch);
            assert!(
                params.len() <= LOCAL_MAX_BIND_PARAMS,
                "each chunk must stay <= {} binds, got {}",
                LOCAL_MAX_BIND_PARAMS,
                params.len()
            );
            covered += batch.len();
        }
        assert_eq!(covered, rows.len(), "every row must be covered by a chunk");
    }
}
