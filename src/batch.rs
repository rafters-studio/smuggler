//! Batch operations for efficient multi-row upserts
//!
//! Groups rows into batches respecting both count and size limits,
//! then generates multi-row INSERT statements.

use crate::config::BatchConfig;
use serde_json::Value as JsonValue;
use std::collections::HashMap;

/// A batch of rows ready for insertion
#[derive(Debug)]
pub struct Batch {
    pub rows: Vec<HashMap<String, JsonValue>>,
    pub estimated_bytes: usize,
}

/// Group rows into batches respecting count and size limits.
///
/// Each batch will have at most `batch_config.batch_size` rows and
/// the generated SQL statement will be at most `batch_config.max_statement_bytes`.
pub fn batch_rows(
    rows: &[HashMap<String, JsonValue>],
    columns: &[String],
    batch_config: &BatchConfig,
) -> Vec<Batch> {
    if rows.is_empty() {
        return vec![];
    }

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
        let would_exceed_count = current_batch.len() >= batch_config.batch_size;
        let would_exceed_size = current_bytes + row_bytes + base_overhead > batch_config.max_statement_bytes
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
                JsonValue::String(s) => s.len() + 2, // quotes
                JsonValue::Array(a) => serde_json::to_string(a).map(|s| s.len()).unwrap_or(10),
                JsonValue::Object(o) => serde_json::to_string(o).map(|s| s.len()).unwrap_or(10),
            };
        }
    }
    size
}

/// Generate a multi-row INSERT OR REPLACE statement.
///
/// Returns the SQL string and flattened parameters.
pub fn generate_batch_insert(
    table: &str,
    columns: &[String],
    rows: &[HashMap<String, JsonValue>],
) -> (String, Vec<JsonValue>) {
    if rows.is_empty() {
        return (String::new(), vec![]);
    }

    let col_list = columns
        .iter()
        .map(|c| format!("\"{}\"", c))
        .collect::<Vec<_>>()
        .join(", ");

    let placeholders_per_row = columns.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
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
    fn batch_by_size() {
        // Create rows with large strings to trigger size limit
        let rows: Vec<_> = (0..5)
            .map(|i| make_row(i, &"x".repeat(1000)))
            .collect();
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
        let rows = vec![
            make_row(1, "alice"),
            make_row(2, "bob"),
        ];
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
}
