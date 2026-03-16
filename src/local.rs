//! Local SQLite database operations

use crate::datasource::{ColumnInfo, DataSource, RowMeta, TableInfo};
use crate::error::{Result, SyncError};
use crate::table::TableSchema;
use rusqlite::{Connection, OpenFlags, Row};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Mutex, MutexGuard};
use tracing::{debug, info};

/// Wrapper for local SQLite database
pub struct LocalDb {
    conn: Mutex<Connection>,
}

impl LocalDb {
    pub(crate) fn conn(&self) -> MutexGuard<'_, Connection> {
        self.conn.lock().expect("mutex poisoned")
    }

    /// Open a local SQLite database
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        info!("Opening local database: {}", path.display());

        let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;

        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Open read-only for diff operations
    pub fn open_readonly(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        info!("Opening local database (read-only): {}", path.display());

        let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;

        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Get the database schema for table name validation.
    ///
    /// Queries sqlite_master to get all user tables.
    pub fn get_schema(&self) -> Result<TableSchema> {
        let conn = self.conn();
        let tables = list_tables_inner(&conn)?;
        Ok(TableSchema::new(tables))
    }

    /// Delete rows by primary key
    #[allow(dead_code)]
    pub fn delete_rows(&self, table: &str, pk_values: &[String]) -> Result<usize> {
        if pk_values.is_empty() {
            return Ok(0);
        }

        let conn = self.conn();
        let info = table_info_inner(&conn, table)?;
        let pk_cols = &info.primary_key;

        let pk_expr = pk_cols
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect::<Vec<_>>()
            .join(" || '|' || ");

        let placeholders = pk_values.iter().map(|_| "?").collect::<Vec<_>>().join(", ");

        let sql = format!(
            "DELETE FROM \"{}\" WHERE {} IN ({})",
            table, pk_expr, placeholders
        );

        debug!("Deleting {} rows from {}", pk_values.len(), table);

        let params: Vec<&dyn rusqlite::ToSql> = pk_values
            .iter()
            .map(|v| v as &dyn rusqlite::ToSql)
            .collect();

        let count = conn.execute(&sql, params.as_slice())?;
        info!("Deleted {} rows from {}", count, table);
        Ok(count)
    }
}

impl DataSource for LocalDb {
    async fn list_tables(&self) -> Result<Vec<String>> {
        let conn = self.conn();
        list_tables_inner(&conn)
    }

    async fn table_info(&self, table: &str) -> Result<TableInfo> {
        let conn = self.conn();
        table_info_inner(&conn, table)
    }

    async fn get_row_metadata(
        &self,
        table: &str,
        timestamp_column: &str,
    ) -> Result<HashMap<String, RowMeta>> {
        let conn = self.conn();
        get_row_metadata_inner(&conn, table, timestamp_column)
    }

    async fn get_rows(
        &self,
        table: &str,
        pk_values: &[String],
    ) -> Result<Vec<HashMap<String, JsonValue>>> {
        let conn = self.conn();
        get_rows_inner(&conn, table, pk_values)
    }

    async fn upsert_rows(&self, table: &str, rows: &[HashMap<String, JsonValue>]) -> Result<usize> {
        let conn = self.conn();
        upsert_rows_inner(&conn, table, rows)
    }

    async fn row_count(&self, table: &str) -> Result<usize> {
        let conn = self.conn();
        let sql = format!("SELECT COUNT(*) FROM \"{}\"", table);
        let count: usize = conn.query_row(&sql, [], |row| row.get(0))?;
        Ok(count)
    }
}

// -- Internal functions that operate on a borrowed Connection --

fn list_tables_inner(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT name FROM sqlite_master
         WHERE type = 'table'
         AND name NOT LIKE 'sqlite_%'
         ORDER BY name",
    )?;

    let tables: Vec<String> = stmt
        .query_map([], |row| row.get(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    debug!("Found {} tables", tables.len());
    Ok(tables)
}

fn table_info_inner(conn: &Connection, table: &str) -> Result<TableInfo> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info(\"{}\")", table))?;

    let columns: Vec<ColumnInfo> = stmt
        .query_map([], |row| {
            Ok(ColumnInfo {
                name: row.get(1)?,
                col_type: row.get(2)?,
                notnull: row.get::<_, i32>(3)? != 0,
                pk: row.get::<_, i32>(5)? != 0,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    if columns.is_empty() {
        return Err(SyncError::TableNotFound(table.to_string()));
    }

    let primary_key: Vec<String> = columns
        .iter()
        .filter(|c| c.pk)
        .map(|c| c.name.clone())
        .collect();

    if primary_key.is_empty() {
        return Err(SyncError::NoPrimaryKey(table.to_string()));
    }

    Ok(TableInfo {
        name: table.to_string(),
        columns,
        primary_key,
    })
}

fn get_row_metadata_inner(
    conn: &Connection,
    table: &str,
    timestamp_column: &str,
) -> Result<HashMap<String, RowMeta>> {
    let info = table_info_inner(conn, table)?;
    let pk_cols = &info.primary_key;
    let has_timestamp = info.columns.iter().any(|c| c.name == timestamp_column);

    // Build the SELECT query
    let pk_select = pk_cols
        .iter()
        .map(|c| format!("\"{}\"", c))
        .collect::<Vec<_>>()
        .join(" || '|' || ");

    let timestamp_select = if has_timestamp {
        format!(", \"{}\"", timestamp_column)
    } else {
        String::new()
    };

    // For content hash, exclude timestamp columns (updated_at, created_at)
    let timestamp_columns = ["updated_at", "created_at"];
    let hash_cols: Vec<_> = info
        .columns
        .iter()
        .filter(|c| !timestamp_columns.contains(&c.name.as_str()))
        .collect();
    let all_cols = hash_cols
        .iter()
        .map(|c| format!("\"{}\"", c.name))
        .collect::<Vec<_>>()
        .join(", ");

    let sql = format!(
        "SELECT {}, {} {} FROM \"{}\"",
        pk_select, all_cols, timestamp_select, table
    );

    debug!("Executing: {}", sql);

    let mut stmt = conn.prepare(&sql)?;
    let hash_col_count = hash_cols.len();

    let mut result = HashMap::new();

    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        // Primary key can be integer or string - handle both
        let pk_value = get_value_as_string(row, 0)?;

        // Collect column values for hashing (excluding timestamp columns)
        let mut hasher = Sha256::new();
        for i in 0..hash_col_count {
            let val = get_value_as_string(row, i + 1)?;
            hasher.update(val.as_bytes());
            hasher.update(b"|");
        }
        let content_hash = hex::encode(hasher.finalize());

        // Handle updated_at as either integer (Unix timestamp) or string
        // Index is 1 (pk) + hash_col_count (content columns) = hash_col_count + 1
        let updated_at: Option<String> = if has_timestamp {
            let ts_idx = hash_col_count + 1;
            // Try integer first (Unix timestamp), then string
            if let Ok(ts) = row.get::<_, Option<i64>>(ts_idx) {
                ts.map(|t| t.to_string())
            } else {
                row.get::<_, Option<String>>(ts_idx).unwrap_or_default()
            }
        } else {
            None
        };

        result.insert(
            pk_value.clone(),
            RowMeta {
                pk_value,
                updated_at,
                content_hash,
            },
        );
    }

    debug!("Got {} rows from {}", result.len(), table);
    Ok(result)
}

fn get_rows_inner(
    conn: &Connection,
    table: &str,
    pk_values: &[String],
) -> Result<Vec<HashMap<String, JsonValue>>> {
    if pk_values.is_empty() {
        return Ok(vec![]);
    }

    let info = table_info_inner(conn, table)?;
    let pk_cols = &info.primary_key;

    // Build primary key expression
    let pk_expr = pk_cols
        .iter()
        .map(|c| format!("\"{}\"", c))
        .collect::<Vec<_>>()
        .join(" || '|' || ");

    // Build column list
    let cols = info
        .columns
        .iter()
        .map(|c| format!("\"{}\"", c.name))
        .collect::<Vec<_>>()
        .join(", ");

    // Build IN clause
    let placeholders = pk_values.iter().map(|_| "?").collect::<Vec<_>>().join(", ");

    let sql = format!(
        "SELECT {} FROM \"{}\" WHERE {} IN ({})",
        cols, table, pk_expr, placeholders
    );

    debug!("Fetching {} rows from {}", pk_values.len(), table);

    let mut stmt = conn.prepare(&sql)?;

    let params: Vec<&dyn rusqlite::ToSql> = pk_values
        .iter()
        .map(|v| v as &dyn rusqlite::ToSql)
        .collect();

    let mut rows = stmt.query(params.as_slice())?;
    let mut result = Vec::new();

    while let Some(row) = rows.next()? {
        let mut row_data = HashMap::new();
        for (i, col) in info.columns.iter().enumerate() {
            let value = get_json_value(row, i)?;
            row_data.insert(col.name.clone(), value);
        }
        result.push(row_data);
    }

    Ok(result)
}

fn upsert_rows_inner(
    conn: &Connection,
    table: &str,
    rows: &[HashMap<String, JsonValue>],
) -> Result<usize> {
    if rows.is_empty() {
        return Ok(0);
    }

    let info = table_info_inner(conn, table)?;
    let cols: Vec<&str> = info.columns.iter().map(|c| c.name.as_str()).collect();

    // Build INSERT OR REPLACE statement
    let col_list = cols
        .iter()
        .map(|c| format!("\"{}\"", c))
        .collect::<Vec<_>>()
        .join(", ");
    let placeholders = cols.iter().map(|_| "?").collect::<Vec<_>>().join(", ");

    let sql = format!(
        "INSERT OR REPLACE INTO \"{}\" ({}) VALUES ({})",
        table, col_list, placeholders
    );

    debug!("Upserting {} rows into {}", rows.len(), table);

    let tx = conn.unchecked_transaction()?;
    let mut count = 0;

    {
        let mut stmt = tx.prepare(&sql)?;
        for row in rows {
            let params: Vec<JsonToSql> = cols
                .iter()
                .map(|col| JsonToSql(row.get(*col).cloned().unwrap_or(JsonValue::Null)))
                .collect();

            let param_refs: Vec<&dyn rusqlite::ToSql> =
                params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();

            stmt.execute(param_refs.as_slice())?;
            count += 1;
        }
    }

    tx.commit()?;
    info!("Upserted {} rows into {}", count, table);
    Ok(count)
}

/// Helper to get a row value as string for hashing
fn get_value_as_string(row: &Row, idx: usize) -> Result<String> {
    // Try different types
    if let Ok(v) = row.get::<_, Option<i64>>(idx) {
        return Ok(v.map(|n| n.to_string()).unwrap_or_default());
    }
    if let Ok(v) = row.get::<_, Option<f64>>(idx) {
        return Ok(v.map(|n| n.to_string()).unwrap_or_default());
    }
    if let Ok(v) = row.get::<_, Option<String>>(idx) {
        return Ok(v.unwrap_or_default());
    }
    if let Ok(v) = row.get::<_, Option<Vec<u8>>>(idx) {
        return Ok(v.map(hex::encode).unwrap_or_default());
    }
    Ok(String::new())
}

/// Helper to convert row value to JSON
fn get_json_value(row: &Row, idx: usize) -> Result<JsonValue> {
    // Try in order: NULL, integer, real, text, blob
    if let Ok(v) = row.get::<_, Option<i64>>(idx) {
        return Ok(v.map(JsonValue::from).unwrap_or(JsonValue::Null));
    }
    if let Ok(v) = row.get::<_, Option<f64>>(idx) {
        return Ok(v.map(JsonValue::from).unwrap_or(JsonValue::Null));
    }
    if let Ok(v) = row.get::<_, Option<String>>(idx) {
        return Ok(v.map(JsonValue::from).unwrap_or(JsonValue::Null));
    }
    if let Ok(v) = row.get::<_, Option<Vec<u8>>>(idx) {
        // Encode blobs as base64
        return Ok(v
            .map(|b| JsonValue::String(hex::encode(b)))
            .unwrap_or(JsonValue::Null));
    }
    Ok(JsonValue::Null)
}

/// Wrapper to allow JSON values as SQL parameters
struct JsonToSql(JsonValue);

impl rusqlite::ToSql for JsonToSql {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        use rusqlite::types::{ToSqlOutput, Value};
        match &self.0 {
            JsonValue::Null => Ok(ToSqlOutput::Owned(Value::Null)),
            JsonValue::Bool(b) => Ok(ToSqlOutput::Owned(Value::Integer(if *b { 1 } else { 0 }))),
            JsonValue::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(ToSqlOutput::Owned(Value::Integer(i)))
                } else if let Some(f) = n.as_f64() {
                    Ok(ToSqlOutput::Owned(Value::Real(f)))
                } else {
                    Ok(ToSqlOutput::Owned(Value::Text(n.to_string())))
                }
            }
            JsonValue::String(s) => Ok(ToSqlOutput::Owned(Value::Text(s.clone()))),
            JsonValue::Array(_) | JsonValue::Object(_) => {
                Ok(ToSqlOutput::Owned(Value::Text(self.0.to_string())))
            }
        }
    }
}
