//! DataSource trait for abstracting database backends
//!
//! This module defines the [`DataSource`] trait that both [`LocalDb`](crate::local::LocalDb)
//! and [`D1Client`](crate::remote::D1Client) implement, allowing the diff and sync engines
//! to work with any pair of data sources.

use crate::error::Result;
use serde_json::Value as JsonValue;
use std::collections::HashMap;

/// Table schema information
#[derive(Debug, Clone)]
pub struct TableInfo {
    #[allow(dead_code)]
    pub name: String,
    pub columns: Vec<ColumnInfo>,
    pub primary_key: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    #[allow(dead_code)]
    pub col_type: String,
    #[allow(dead_code)]
    pub notnull: bool,
    pub pk: bool,
}

/// A row with its primary key and optional timestamp
#[derive(Debug, Clone)]
pub struct RowMeta {
    #[allow(dead_code)]
    pub pk_value: String,
    pub updated_at: Option<String>,
    pub content_hash: String,
}

/// Abstraction over a database that can be used as a sync source or destination.
///
/// Both local SQLite databases and remote Cloudflare D1 instances implement
/// this trait, allowing the diff and sync engines to work generically with
/// any pair of data sources.
pub trait DataSource: Sync {
    /// List all user tables (excluding internal/system tables).
    fn list_tables(&self) -> impl std::future::Future<Output = Result<Vec<String>>> + Send;

    /// Get schema info for a table (columns, primary keys).
    fn table_info(
        &self,
        table: &str,
    ) -> impl std::future::Future<Output = Result<TableInfo>> + Send;

    /// Check if a table has a specific column.
    ///
    /// Default implementation delegates to `table_info`.
    #[allow(dead_code)]
    fn has_column(
        &self,
        table: &str,
        column: &str,
    ) -> impl std::future::Future<Output = Result<bool>> + Send {
        async move {
            let info = self.table_info(table).await?;
            Ok(info.columns.iter().any(|c| c.name == column))
        }
    }

    /// Get row metadata for change detection.
    ///
    /// Returns a map of primary key value -> RowMeta containing the content
    /// hash and optional timestamp for each row.
    fn get_row_metadata(
        &self,
        table: &str,
        timestamp_column: &str,
    ) -> impl std::future::Future<Output = Result<HashMap<String, RowMeta>>> + Send;

    /// Get full row data for specific primary key values.
    fn get_rows(
        &self,
        table: &str,
        pk_values: &[String],
    ) -> impl std::future::Future<Output = Result<Vec<HashMap<String, JsonValue>>>> + Send;

    /// Insert or replace rows in the table. Returns the number of rows written.
    fn upsert_rows(
        &self,
        table: &str,
        rows: &[HashMap<String, JsonValue>],
    ) -> impl std::future::Future<Output = Result<usize>> + Send;

    /// Get the number of rows in a table.
    fn row_count(&self, table: &str) -> impl std::future::Future<Output = Result<usize>> + Send;
}
