//! Batch operations for efficient multi-row upserts
//!
//! Groups rows into batches respecting count, size, and D1 bind parameter limits,
//! then generates multi-row INSERT statements.

use crate::config::BatchConfig;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use tracing::warn;

/// Cloudflare D1 maximum bind parameters per query.
/// See: https://developers.cloudflare.com/d1/platform/limits/
pub const D1_MAX_BIND_PARAMS: usize = 100;

/// A batch of rows ready for insertion
#[derive(Debug)]
pub struct Batch {
    pub rows: Vec<HashMap<String, JsonValue>>,
    pub estimated_bytes: usize,
}

/// Group rows into batches respecting count, size, and D1 bind parameter limits.
///
/// Each batch will have at most `batch_config.batch_size` rows,
/// the generated SQL statement will be at most `batch_config.max_statement_bytes`,
/// and the total bind parameters (`rows * columns`) will not exceed [`D1_MAX_BIND_PARAMS`].
pub fn batch_rows(
    rows: &[HashMap<String, JsonValue>],
    columns: &[String],
    batch_config: &BatchConfig,
) -> Vec<Batch> {
    if rows.is_empty() || columns.is_empty() {
        return vec![];
    }

    // D1 caps bind params at 100. Each row needs one param per column.
    // Use max(1, ...) so tables wider than 100 columns still produce
    // single-row batches rather than zero batches. The primary caller
    // (upsert_rows_batched) guards against this case earlier, but
    // batch_rows remains defensive for direct callers.
    let max_rows_by_params = (D1_MAX_BIND_PARAMS / columns.len()).max(1);
    if columns.len() > D1_MAX_BIND_PARAMS {
        warn!(
            "Table has {} columns which exceeds D1's {} bind-parameter limit; \
             single-row inserts will fail",
            columns.len(),
            D1_MAX_BIND_PARAMS,
        );
    }

    let effective_max = batch_config.batch_size.min(max_rows_by_params);

    let mut batches = Vec::new();
    let mut current_batch = Vec::new();
    let mut current_bytes = 0usize;

    // Estimate base SQL overhead: "INSERT OR REPLACE INTO \"table\" (cols) VALUES "
    // We use a conservative estimate of 200 bytes for this
    let base_overhead = 200;

    for row in rows {
        // Estimate size of this row when serialized
        let row_bytes = estimate_row_size(row, columns);

        // Check if adding this row would exceed limits
        let would_exceed_count = current_batch.len() >= effective_max;
        let would_exceed_size = current_bytes + row_bytes + base_overhead
            > batch_config.max_statement_bytes
            && !current_batch.is_empty();

        if would_exceed_count || would_exceed_size {
            // Start a new batch
            batches.push(Batch {
                rows: std::mem::take(&mut current_batch),
                estimated_bytes: current_bytes,
            });
            current_bytes = 0;
        }

        current_batch.push(row.clone());
        current_bytes += row_bytes;
    }

    // Don't forget the last batch
    if !current_batch.is_empty() {
        batches.push(Batch {
            rows: current_batch,
            estimated_bytes: current_bytes,
        });
    }

    batches
}

/// Estimate the serialized size of a row
fn estimate_row_size(row: &HashMap<String, JsonValue>, columns: &[String]) -> usize {
    let mut size = 0;
    for col in columns {
        if let Some(value) = row.get(col) {
            // Add column overhead (quotes, comma)
            size += 5;
            // Add value size
            size += match value {
                JsonValue::Null => 4,
                JsonValue::Bool(_) => 5,
                JsonValue::Number(n) => n.to_string().len(),
                JsonValue::String(s) => {
                    // Account for quotes plus ~20% overhead for SQL/JSON escaping
                    // (quotes, backslashes, special characters)
                    let escaped_overhead = s.len() / 5;
                    s.len() + escaped_overhead + 2
                }
                JsonValue::Array(a) => match serde_json::to_string(a) {
                    Ok(s) => s.len(),
                    Err(e) => {
                        warn!("Failed to serialize array for size estimation: {}", e);
                        1024 // Conservative fallback for failed serialization
                    }
                },
                JsonValue::Object(o) => match serde_json::to_string(o) {
                    Ok(s) => s.len(),
                    Err(e) => {
                        warn!("Failed to serialize object for size estimation: {}", e);
                        1024 // Conservative fallback for failed serialization
                    }
                },
            };
        }
    }
    size
}

/// Generate a multi-row INSERT OR REPLACE statement.
///
/// Returns the SQL string and flattened parameters.
///
/// Warns if the generated parameter count would exceed D1's bind limit.
/// Callers should use [`batch_rows`] first to keep batches within bounds.
pub fn generate_batch_insert(
    table: &str,
    columns: &[String],
    rows: &[HashMap<String, JsonValue>],
) -> (String, Vec<JsonValue>) {
    if rows.is_empty() {
        return (String::new(), vec![]);
    }

    let param_count = rows.len() * columns.len();
    if param_count > D1_MAX_BIND_PARAMS {
        warn!(
            "Batch for table '{}' has {} bind params ({} rows x {} cols), \
             exceeding D1 limit of {}. This query will likely fail.",
            table,
            param_count,
            rows.len(),
            columns.len(),
            D1_MAX_BIND_PARAMS,
        );
    }

    let col_list = columns
        .iter()
        .map(|c| format!("\"{}\"", c))
        .collect::<Vec<_>>()
        .join(", ");

    let placeholders_per_row = vec!["?"; columns.len()].join(", ");
    let all_placeholders = rows
        .iter()
        .map(|_| format!("({})", placeholders_per_row))
        .collect::<Vec<_>>()
        .join(", ");

    let sql = format!(
        "INSERT OR REPLACE INTO \"{}\" ({}) VALUES {}",
        table, col_list, all_placeholders
    );

    // Flatten all row values into a single params vector
    let params: Vec<JsonValue> = rows
        .iter()
        .flat_map(|row| {
            columns
                .iter()
                .map(|col| row.get(col).cloned().unwrap_or(JsonValue::Null))
        })
        .collect();

    (sql, params)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_row(id: i64, name: &str) -> HashMap<String, JsonValue> {
        let mut row = HashMap::new();
        row.insert("id".to_string(), JsonValue::Number(id.into()));
        row.insert("name".to_string(), JsonValue::String(name.to_string()));
        row
    }

    #[test]
    fn batch_by_count() {
        let rows: Vec<_> = (0..10).map(|i| make_row(i, "test")).collect();
        let columns = vec!["id".to_string(), "name".to_string()];
        let config = BatchConfig {
            batch_size: 3,
            max_statement_bytes: 100_000,
        };

        let batches = batch_rows(&rows, &columns, &config);

        assert_eq!(batches.len(), 4); // 3 + 3 + 3 + 1
        assert_eq!(batches[0].rows.len(), 3);
        assert_eq!(batches[1].rows.len(), 3);
        assert_eq!(batches[2].rows.len(), 3);
        assert_eq!(batches[3].rows.len(), 1);
    }

    #[test]
    fn batch_respects_d1_param_limit() {
        // 10 columns means max 10 rows per batch (100 / 10)
        let columns: Vec<String> = (0..10).map(|i| format!("col_{}", i)).collect();
        let rows: Vec<_> = (0..25)
            .map(|i| {
                let mut row = HashMap::new();
                for col in &columns {
                    row.insert(col.clone(), JsonValue::Number(i.into()));
                }
                row
            })
            .collect();

        let config = BatchConfig {
            batch_size: 100, // would allow 100, but param limit caps at 10
            max_statement_bytes: 100_000,
        };

        let batches = batch_rows(&rows, &columns, &config);

        // 25 rows / 10 per batch = 3 batches (10 + 10 + 5)
        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].rows.len(), 10);
        assert_eq!(batches[1].rows.len(), 10);
        assert_eq!(batches[2].rows.len(), 5);

        // Verify no batch exceeds D1 param limit
        for batch in &batches {
            assert!(batch.rows.len() * columns.len() <= D1_MAX_BIND_PARAMS);
        }
    }

    #[test]
    fn batch_wide_table_respects_param_limit() {
        // 50 columns means max 2 rows per batch (100 / 50)
        let columns: Vec<String> = (0..50).map(|i| format!("col_{}", i)).collect();
        let rows: Vec<_> = (0..5)
            .map(|i| {
                let mut row = HashMap::new();
                for col in &columns {
                    row.insert(col.clone(), JsonValue::Number(i.into()));
                }
                row
            })
            .collect();

        let config = BatchConfig {
            batch_size: 100,
            max_statement_bytes: 100_000,
        };

        let batches = batch_rows(&rows, &columns, &config);

        // 5 rows / 2 per batch = 3 batches (2 + 2 + 1)
        assert_eq!(batches.len(), 3);
        for batch in &batches {
            assert!(batch.rows.len() * columns.len() <= D1_MAX_BIND_PARAMS);
        }
    }

    #[test]
    fn batch_over_100_columns_still_emits_rows() {
        // 101 columns exceeds D1's param limit -- batch_rows should still
        // produce single-row batches rather than dropping data
        let columns: Vec<String> = (0..101).map(|i| format!("col_{}", i)).collect();
        let rows: Vec<_> = (0..3)
            .map(|i| {
                let mut row = HashMap::new();
                for col in &columns {
                    row.insert(col.clone(), JsonValue::Number(i.into()));
                }
                row
            })
            .collect();

        let config = BatchConfig {
            batch_size: 100,
            max_statement_bytes: 100_000,
        };

        let batches = batch_rows(&rows, &columns, &config);

        // Should produce 3 single-row batches (no data dropped)
        assert_eq!(batches.len(), 3);
        for batch in &batches {
            assert_eq!(batch.rows.len(), 1);
        }
    }

    #[test]
    fn empty_columns_returns_empty() {
        let rows = vec![make_row(1, "test")];
        let columns: Vec<String> = vec![];
        let config = BatchConfig::default();

        let batches = batch_rows(&rows, &columns, &config);
        assert!(batches.is_empty());
    }

    #[test]
    fn batch_by_size() {
        // Create rows with large strings to trigger size limit
        let rows: Vec<_> = (0..5).map(|i| make_row(i, &"x".repeat(1000))).collect();
        let columns = vec!["id".to_string(), "name".to_string()];
        let config = BatchConfig {
            batch_size: 100,
            max_statement_bytes: 2500, // Force multiple batches due to size
        };

        let batches = batch_rows(&rows, &columns, &config);

        // Should split into multiple batches due to size, not count
        assert!(batches.len() > 1);
        for batch in &batches {
            assert!(batch.rows.len() < 5); // Each batch should be smaller than total
        }
    }

    #[test]
    fn empty_rows_returns_empty() {
        let rows: Vec<HashMap<String, JsonValue>> = vec![];
        let columns = vec!["id".to_string()];
        let config = BatchConfig::default();

        let batches = batch_rows(&rows, &columns, &config);
        assert!(batches.is_empty());
    }

    #[test]
    fn single_row_batch() {
        let rows = vec![make_row(1, "test")];
        let columns = vec!["id".to_string(), "name".to_string()];
        let config = BatchConfig::default();

        let batches = batch_rows(&rows, &columns, &config);

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].rows.len(), 1);
    }

    #[test]
    fn generate_multi_row_insert() {
        let rows = vec![make_row(1, "alice"), make_row(2, "bob")];
        let columns = vec!["id".to_string(), "name".to_string()];

        let (sql, params) = generate_batch_insert("users", &columns, &rows);

        assert!(sql.starts_with("INSERT OR REPLACE INTO \"users\""));
        assert!(sql.contains("(?, ?)"));
        assert!(sql.contains("), (")); // Multiple value sets
        assert_eq!(params.len(), 4); // 2 rows x 2 columns
    }

    #[test]
    fn generate_empty_insert() {
        let rows: Vec<HashMap<String, JsonValue>> = vec![];
        let columns = vec!["id".to_string()];

        let (sql, params) = generate_batch_insert("users", &columns, &rows);

        assert!(sql.is_empty());
        assert!(params.is_empty());
    }

    #[test]
    fn batch_config_defaults() {
        let config = BatchConfig::default();
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.max_statement_bytes, 90 * 1024);
    }

    #[test]
    fn oversized_single_row_still_batched() {
        // A single row that exceeds max_statement_bytes should still be included
        // (the D1 API will reject it, but we don't silently drop data)
        let mut row = HashMap::new();
        row.insert("id".to_string(), JsonValue::Number(1.into()));
        row.insert("data".to_string(), JsonValue::String("x".repeat(10_000)));

        let rows = vec![row];
        let columns = vec!["id".to_string(), "data".to_string()];
        let config = BatchConfig {
            batch_size: 100,
            max_statement_bytes: 1000, // Deliberately small
        };

        let batches = batch_rows(&rows, &columns, &config);

        // Should still produce one batch with the oversized row
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].rows.len(), 1);
    }

    #[test]
    fn string_escaping_overhead() {
        // Verify that strings with special characters get extra overhead
        let row_size = estimate_row_size(
            &{
                let mut row = HashMap::new();
                row.insert(
                    "text".to_string(),
                    JsonValue::String("hello\"world".to_string()),
                );
                row
            },
            &["text".to_string()],
        );

        // Should be more than just len + 2 due to escaping overhead
        assert!(row_size > "hello\"world".len() + 2);
    }
}
