//! Change detection between local and remote databases

use crate::config::ConflictResolution;
use crate::datasource::DataSource;
use crate::error::Result;
use std::collections::HashSet;
use tracing::{debug, info};

/// Represents the differences between local and remote for a table
#[derive(Debug, Default)]
pub struct TableDiff {
    #[allow(dead_code)]
    pub table: String,
    /// Rows that exist only in local
    pub local_only: Vec<String>,
    /// Rows that exist only in remote
    pub remote_only: Vec<String>,
    /// Rows where local is newer (by timestamp)
    pub local_newer: Vec<String>,
    /// Rows where remote is newer (by timestamp)
    pub remote_newer: Vec<String>,
    /// Rows that differ but have no timestamp (use content hash)
    pub content_differs: Vec<String>,
    /// Rows that are identical
    pub identical: Vec<String>,
}

impl TableDiff {
    pub fn new(table: &str) -> Self {
        Self {
            table: table.to_string(),
            ..Default::default()
        }
    }

    /// Check if there are any differences
    pub fn has_changes(&self) -> bool {
        !self.local_only.is_empty()
            || !self.remote_only.is_empty()
            || !self.local_newer.is_empty()
            || !self.remote_newer.is_empty()
            || !self.content_differs.is_empty()
    }

    /// Get rows that should be pushed (local -> remote)
    pub fn rows_to_push(&self, conflict_resolution: ConflictResolution) -> Vec<String> {
        let mut rows = Vec::new();
        rows.extend(self.local_only.clone());
        rows.extend(self.local_newer.clone());

        match conflict_resolution {
            ConflictResolution::LocalWins => {
                rows.extend(self.content_differs.clone());
            }
            ConflictResolution::RemoteWins => {
                // Don't push content_differs
            }
            ConflictResolution::NewerWins => {
                // content_differs have no timestamp, so we skip them
                // (or could use local as default)
            }
        }

        rows
    }

    /// Get rows that should be pulled (remote -> local)
    pub fn rows_to_pull(&self, conflict_resolution: ConflictResolution) -> Vec<String> {
        let mut rows = Vec::new();
        rows.extend(self.remote_only.clone());
        rows.extend(self.remote_newer.clone());

        match conflict_resolution {
            ConflictResolution::LocalWins => {
                // Don't pull content_differs
            }
            ConflictResolution::RemoteWins => {
                rows.extend(self.content_differs.clone());
            }
            ConflictResolution::NewerWins => {
                // content_differs have no timestamp, so we skip them
            }
        }

        rows
    }

    /// Get rows to delete from remote (for push)
    /// Reserved for full sync mode
    #[allow(dead_code)]
    pub fn rows_to_delete_remote(&self) -> Vec<String> {
        // Rows that only exist in remote but not in local
        // This is only for full sync mode, not incremental
        vec![]
    }

    /// Get rows to delete from local (for pull)
    /// Reserved for full sync mode
    #[allow(dead_code)]
    pub fn rows_to_delete_local(&self) -> Vec<String> {
        // Rows that only exist in local but not in remote
        // This is only for full sync mode, not incremental
        vec![]
    }

    /// Summary string for display
    pub fn summary(&self) -> String {
        let mut parts = Vec::new();

        if !self.local_only.is_empty() {
            parts.push(format!("+{} local only", self.local_only.len()));
        }
        if !self.remote_only.is_empty() {
            parts.push(format!("+{} remote only", self.remote_only.len()));
        }
        if !self.local_newer.is_empty() {
            parts.push(format!("{} local newer", self.local_newer.len()));
        }
        if !self.remote_newer.is_empty() {
            parts.push(format!("{} remote newer", self.remote_newer.len()));
        }
        if !self.content_differs.is_empty() {
            parts.push(format!("{} content differs", self.content_differs.len()));
        }
        if !self.identical.is_empty() {
            parts.push(format!("{} identical", self.identical.len()));
        }

        if parts.is_empty() {
            "no data".to_string()
        } else {
            parts.join(", ")
        }
    }
}

/// Compare two data sources for a table
pub async fn diff_table<A: DataSource, B: DataSource>(
    local: &A,
    remote: &B,
    table: &str,
    timestamp_column: &str,
) -> Result<TableDiff> {
    info!("Computing diff for table: {}", table);

    // Get metadata from both sides
    let local_meta = local.get_row_metadata(table, timestamp_column).await?;
    let remote_meta = remote.get_row_metadata(table, timestamp_column).await?;

    let local_keys: HashSet<&String> = local_meta.keys().collect();
    let remote_keys: HashSet<&String> = remote_meta.keys().collect();

    let mut diff = TableDiff::new(table);

    // Find rows only in local
    for pk in local_keys.difference(&remote_keys) {
        diff.local_only.push((*pk).clone());
    }

    // Find rows only in remote
    for pk in remote_keys.difference(&local_keys) {
        diff.remote_only.push((*pk).clone());
    }

    // Compare rows that exist in both
    for pk in local_keys.intersection(&remote_keys) {
        let local_row = &local_meta[*pk];
        let remote_row = &remote_meta[*pk];

        // First check content hash - if identical, no change needed
        if local_row.content_hash == remote_row.content_hash {
            diff.identical.push((*pk).clone());
            continue;
        }

        // Content differs - check timestamps if available
        match (&local_row.updated_at, &remote_row.updated_at) {
            (Some(local_ts), Some(remote_ts)) => {
                if local_ts > remote_ts {
                    diff.local_newer.push((*pk).clone());
                } else if remote_ts > local_ts {
                    diff.remote_newer.push((*pk).clone());
                } else {
                    // Same timestamp but different content - conflict
                    diff.content_differs.push((*pk).clone());
                }
            }
            _ => {
                // No timestamps available - mark as content differs
                diff.content_differs.push((*pk).clone());
            }
        }
    }

    debug!(
        "Diff for {}: local_only={}, remote_only={}, local_newer={}, remote_newer={}, content_differs={}, identical={}",
        table,
        diff.local_only.len(),
        diff.remote_only.len(),
        diff.local_newer.len(),
        diff.remote_newer.len(),
        diff.content_differs.len(),
        diff.identical.len()
    );

    Ok(diff)
}

/// Compare all tables
/// Reserved for batch operations
#[allow(dead_code)]
pub async fn diff_all<A: DataSource, B: DataSource>(
    local: &A,
    remote: &B,
    tables: &[String],
    timestamp_column: &str,
) -> Result<Vec<TableDiff>> {
    let mut diffs = Vec::new();

    for table in tables {
        let diff = diff_table(local, remote, table, timestamp_column).await?;
        diffs.push(diff);
    }

    Ok(diffs)
}
