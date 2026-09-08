//! The one canonical home for building multi-row `INSERT OR REPLACE` batch
//! statements and reshaping positional result rows into column->value maps.
//!
//! Both `generate_batch_sql` and `rows_to_maps` were byte-identical copies in
//! the http-sql plugin (`plugins/smugglr-http-sql/src/adapter.rs`) and the
//! wasm adapter (`crates/smugglr-wasm/src/adapter_common.rs`) -- pure,
//! self-free functions with no reason to differ. Hoisted here (#222) so the
//! emitted SQL and row-reshaping logic cannot drift between adapters the way
//! `rowhash.rs` already prevents for content hashing.
//!
//! This module is always compiled (no `native` deps): both the plugin and the
//! wasm adapter depend on smugglr-core with `default-features = false` and
//! call straight into here.

use std::collections::HashMap;

use serde_json::Value;

/// Build an `INSERT OR REPLACE` batch statement plus its flattened params.
pub fn generate_batch_sql(
    table: &str,
    columns: &[String],
    rows: &[HashMap<String, Value>],
) -> (String, Vec<Value>) {
    let col_list = columns
        .iter()
        .map(|c| format!("\"{}\"", c))
        .collect::<Vec<_>>()
        .join(", ");

    let row_placeholder = format!("({})", vec!["?"; columns.len()].join(", "));
    let all_placeholders = vec![row_placeholder.as_str(); rows.len()].join(", ");

    let sql = format!(
        "INSERT OR REPLACE INTO \"{}\" ({}) VALUES {}",
        table, col_list, all_placeholders
    );

    let params: Vec<Value> = rows
        .iter()
        .flat_map(|row| {
            columns
                .iter()
                .map(|c| row.get(c).cloned().unwrap_or(Value::Null))
        })
        .collect();

    (sql, params)
}

/// Reshape positional result rows into per-row column->value maps.
pub fn rows_to_maps(columns: &[String], rows: &[Vec<Value>]) -> Vec<HashMap<String, Value>> {
    rows.iter()
        .map(|row| {
            columns
                .iter()
                .zip(row.iter())
                .map(|(col, val)| (col.clone(), val.clone()))
                .collect()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_row(id: i64, name: &str) -> HashMap<String, Value> {
        let mut row = HashMap::new();
        row.insert("id".to_string(), Value::from(id));
        row.insert("name".to_string(), Value::from(name));
        row
    }

    #[test]
    fn generate_batch_sql_single_row() {
        let rows = vec![make_row(1, "alice")];
        let columns = vec!["id".to_string(), "name".to_string()];
        let (sql, params) = generate_batch_sql("users", &columns, &rows);

        assert!(sql.starts_with("INSERT OR REPLACE INTO \"users\""));
        assert!(sql.contains("(?, ?)"));
        assert!(!sql.contains("), ("));
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn generate_batch_sql_multi_row() {
        let rows = vec![
            make_row(1, "alice"),
            make_row(2, "bob"),
            make_row(3, "charlie"),
        ];
        let columns = vec!["id".to_string(), "name".to_string()];
        let (sql, params) = generate_batch_sql("users", &columns, &rows);

        assert!(sql.contains("(?, ?), (?, ?), (?, ?)"));
        assert_eq!(params.len(), 6);
    }

    #[test]
    fn generate_batch_sql_null_for_missing_column() {
        let mut row = HashMap::new();
        row.insert("id".to_string(), Value::from(1));
        // "name" is missing from this row
        let columns = vec!["id".to_string(), "name".to_string()];
        let (_, params) = generate_batch_sql("users", &columns, &[row]);

        assert_eq!(params[0], Value::from(1));
        assert_eq!(params[1], Value::Null);
    }

    /// Snapshot test pinning the exact emitted SQL string for issue #222: the
    /// hoist from the plugin and wasm-adapter duplicates must not change a
    /// single byte of output. This is the byte-identical string both
    /// duplicated copies produced before the hoist.
    #[test]
    fn generate_batch_sql_snapshot_two_rows_three_columns() {
        let columns = vec!["id".to_string(), "name".to_string(), "score".to_string()];
        let mut row1 = HashMap::new();
        row1.insert("id".to_string(), Value::from(1));
        row1.insert("name".to_string(), Value::from("alice"));
        row1.insert("score".to_string(), Value::from(10));
        let mut row2 = HashMap::new();
        row2.insert("id".to_string(), Value::from(2));
        row2.insert("name".to_string(), Value::from("bob"));
        row2.insert("score".to_string(), Value::from(20));

        let (sql, params) = generate_batch_sql("scores", &columns, &[row1, row2]);

        assert_eq!(
            sql,
            "INSERT OR REPLACE INTO \"scores\" (\"id\", \"name\", \"score\") VALUES (?, ?, ?), (?, ?, ?)"
        );
        assert_eq!(
            params,
            vec![
                Value::from(1),
                Value::from("alice"),
                Value::from(10),
                Value::from(2),
                Value::from("bob"),
                Value::from(20),
            ]
        );
    }

    // Batch-size/param-limit chunking for the HTTP-SQL adapters is
    // `http_sql::max_rows_per_batch` (#461) -- tested alongside it in
    // `http_sql.rs`. `LocalSqlDataSource`'s own chunking constant
    // (`LOCAL_MAX_BIND_PARAMS`) is a different limit for a different, non-HTTP
    // executor, and stays tested in `crates/smugglr-wasm/src/local_adapter.rs`.
    // This module only owns SQL generation for an already-chunked batch,
    // which the snapshot test above and the tests below cover.

    #[test]
    fn rows_to_maps_basic() {
        let columns = vec!["id".to_string(), "name".to_string()];
        let rows = vec![
            vec![Value::from(1), Value::from("alice")],
            vec![Value::from(2), Value::from("bob")],
        ];
        let maps = rows_to_maps(&columns, &rows);
        assert_eq!(maps.len(), 2);
        assert_eq!(maps[0]["name"], "alice");
        assert_eq!(maps[1]["id"], 2);
    }
}
