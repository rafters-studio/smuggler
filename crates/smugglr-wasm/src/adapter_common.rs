//! Shared pure re-exports for the wasm DataSource adapters.
//!
//! `LocalSqlDataSource` (JS executor, `local_adapter.rs`) still needs the
//! primary-key text expression and batch SQL generation/row reshaping --
//! transport-agnostic pieces that live in `smugglr-core`, re-exported here
//! under the names this crate already uses.
//!
//! Everything that WAS here beyond these re-exports -- `parse_table_info`,
//! `row_maps_to_metadata`, `canonicalize_json_blobs`, `cached_table_info`,
//! and `incremental_metadata_sql` -- moved to `smugglr_core::http_sql` (#461),
//! since `FetchDataSource` (`fetch_adapter.rs`) needed them too and this
//! crate is `#![cfg(target_arch = "wasm32")]`: `cargo test` cannot reach
//! logic left in here. `LocalSqlDataSource` now calls those directly as
//! `smugglr_core::http_sql::<fn>`. The row content hash moved out entirely --
//! nothing in this crate calls it directly any more, only indirectly through
//! `smugglr_core::http_sql::row_maps_to_metadata`.

// Primary-key text expression -- the one canonical definition lives in
// smugglr-core::rowhash so the native, plugin, and wasm paths cannot drift.
// Re-exported under the name this crate already uses.
pub(crate) use smugglr_core::rowhash::pk_text_expr as build_pk_text_expr;

// Batch-SQL generation and row reshaping -- the one canonical definition
// lives in smugglr-core::batch_sql so the http-sql plugin and wasm adapters
// cannot drift (#222). Re-exported under the names this crate already uses.
pub(crate) use smugglr_core::batch_sql::generate_batch_sql;
pub(crate) use smugglr_core::batch_sql::rows_to_maps;
