//! DataSource trait for abstracting database backends
//!
//! This module defines the [`DataSource`] trait that [`LocalDb`](crate::local::LocalDb)
//! and [`PluginDataSource`](crate::plugin::PluginDataSource) implement, allowing the diff
//! and sync engines to work with any pair of data sources. Remote backends (D1, turso,
//! rqlite, datasette, sqlitecloud, starbasedb) are reached via the http-sql plugin.

use crate::error::Result;
use serde_json::Value as JsonValue;
use std::collections::HashMap;

/// Table schema information, row metadata, and column info.
///
/// Re-exported from `smugglr-wire`, the canonical definition shared with
/// `smugglr-plugin-sdk` and the plugin JSON-RPC wire protocol -- see #228.
/// This module keeps re-exporting them under their original names so
/// `smugglr_core::datasource::{TableInfo, ColumnInfo, RowMeta}` importers
/// (local.rs, plugin.rs, the wasm crate, the CLI) are unaffected.
pub use smugglr_wire::{ColumnInfo, RowMeta, TableInfo};

/// Normalize a row's timestamp-column value to the `RowMeta.updated_at` string.
///
/// Integer Unix timestamps arrive as JSON numbers from remote SQL-over-HTTP
/// endpoints and as rusqlite integers locally; both must render to the same
/// decimal string so the two sides compare equal. Extracting via `as_str()`
/// alone (the old remote path) silently dropped an integer timestamp to `None`,
/// which forced every genuinely-changed row into `content_differs` -- skipped in
/// both directions under `newer_wins`/`uuid_v7_wins`. This is the single
/// canonical extractor for the native, plugin, and wasm metadata builders so
/// the three cannot drift.
pub fn extract_updated_at(value: Option<&JsonValue>) -> Option<String> {
    match value {
        Some(JsonValue::Number(n)) => Some(n.to_string()),
        Some(JsonValue::String(s)) => Some(s.clone()),
        _ => None,
    }
}

/// Conditional Send bound: required on native (for tokio), absent on WASM (single-threaded).
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSend: Send {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send> MaybeSend for T {}

#[cfg(target_arch = "wasm32")]
pub trait MaybeSend {}
#[cfg(target_arch = "wasm32")]
impl<T> MaybeSend for T {}

/// Abstraction over a database that can be used as a sync source or destination.
///
/// `LocalDb` and `PluginDataSource` implement this trait, letting the diff
/// and sync engines work generically with any pair of data sources. Remote
/// backends (D1, Turso, rqlite, Datasette, SQLiteCloud, StarbaseDB) are
/// reached through the http-sql plugin via `PluginDataSource`.
pub trait DataSource: Sync {
    /// List all user tables (excluding internal/system tables).
    fn list_tables(&self) -> impl std::future::Future<Output = Result<Vec<String>>> + MaybeSend;

    /// Get schema info for a table (columns, primary keys).
    fn table_info(
        &self,
        table: &str,
    ) -> impl std::future::Future<Output = Result<TableInfo>> + MaybeSend;

    /// Get row metadata for change detection.
    ///
    /// Returns a map of primary key value -> RowMeta containing the content
    /// hash and optional timestamp for each row. Columns matching any pattern
    /// in `exclude_columns` are omitted from the content hash.
    fn get_row_metadata(
        &self,
        table: &str,
        timestamp_column: &str,
        exclude_columns: &[String],
    ) -> impl std::future::Future<Output = Result<HashMap<String, RowMeta>>> + MaybeSend;

    /// Get full row data for specific primary key values.
    fn get_rows(
        &self,
        table: &str,
        pk_values: &[String],
    ) -> impl std::future::Future<Output = Result<Vec<HashMap<String, JsonValue>>>> + MaybeSend;

    /// Insert or update rows in the table. Returns the number of rows written.
    ///
    /// # What an absent column means
    ///
    /// A column a row map does not mention is **left alone**: an existing
    /// destination row keeps whatever it already stored there, and a row that
    /// does not yet exist takes that column's schema default. It does NOT mean
    /// NULL.
    ///
    /// The distinction is load-bearing because `[sync].exclude_columns` strips
    /// matching columns from every row before transfer (`sync::transfer_rows`),
    /// so an implementation that writes NULL for an absent column destroys the
    /// value the operator configured to stay off the wire -- silently, since an
    /// excluded column is out of the content hash and nothing downstream
    /// compares it. That was #324, on the `pull` path, where the destination is
    /// the local database.
    ///
    /// **This is a contract on implementors, not a guarantee smugglr enforces
    /// for every backend it ships.** `LocalDb` honors it as of #324. The
    /// bundled http-sql adapter and the wasm adapters derive their column list
    /// from `rows[0].keys()` and emit `INSERT OR REPLACE`, which SQLite
    /// executes as DELETE+INSERT -- so a column absent from a row pushed to one
    /// of those destinations is reset to its schema default there. Excluding a
    /// column is not yet safe in the push direction.
    ///
    /// A column that is `NOT NULL` with no `DEFAULT` cannot be omitted from a
    /// row that does not yet exist at the destination; that insert fails, and
    /// the failure should name the table and the column.
    fn upsert_rows(
        &self,
        table: &str,
        rows: &[HashMap<String, JsonValue>],
    ) -> impl std::future::Future<Output = Result<usize>> + MaybeSend;

    /// Get the number of rows in a table.
    fn row_count(
        &self,
        table: &str,
    ) -> impl std::future::Future<Output = Result<usize>> + MaybeSend;

    /// Names of the tables `table` declares a `FOREIGN KEY ... REFERENCES` to
    /// -- its parents, which a write-ordered sync must land before `table`
    /// itself on any target that enforces referential integrity (see
    /// `sync::topological_table_order`, #435).
    ///
    /// Default is an empty list: only a source that can introspect a real
    /// foreign-key graph overrides this (`LocalDb`, via SQLite's `PRAGMA
    /// foreign_key_list`). A source that always returns `Ok(vec![])` simply
    /// leaves write order at its alphabetical tiebreak -- the same order sync
    /// produced before this method existed -- so `PluginDataSource`, the wasm
    /// adapters, and third-party `DataSource` implementors are unaffected
    /// unless they choose to override it.
    fn foreign_key_parents(
        &self,
        _table: &str,
    ) -> impl std::future::Future<Output = Result<Vec<String>>> + MaybeSend {
        async { Ok(Vec::new()) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extract_updated_at_renders_integer_timestamps() {
        // The whole point of #177: a JSON integer timestamp must survive as its
        // decimal string, not get dropped to None by an as_str()-only path.
        let v = json!(1_700_000_000_i64);
        assert_eq!(extract_updated_at(Some(&v)), Some("1700000000".to_string()));
    }

    #[test]
    fn extract_updated_at_renders_string_timestamps() {
        let v = json!("2023-06-01T00:00:00Z");
        assert_eq!(
            extract_updated_at(Some(&v)),
            Some("2023-06-01T00:00:00Z".to_string())
        );
    }

    #[test]
    fn extract_updated_at_is_none_for_null_and_missing() {
        assert_eq!(extract_updated_at(Some(&JsonValue::Null)), None);
        assert_eq!(extract_updated_at(None), None);
    }

    #[test]
    fn extract_updated_at_renders_float_as_number_string() {
        // Float timestamps are out of scope for numeric ordering but must still
        // round-trip a stable string (both sides render via this one path).
        let v = json!(1.5);
        assert_eq!(extract_updated_at(Some(&v)), Some("1.5".to_string()));
    }
}
