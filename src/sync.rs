//! Sync orchestration

use crate::config::{Config, ConflictResolution};
use crate::datasource::DataSource;
use crate::diff::{diff_table, TableDiff};
use crate::error::Result;
use crate::local::LocalDb;
use crate::remote::D1Client;
use indicatif::{ProgressBar, ProgressStyle};
use tracing::{info, warn};

/// Result of a sync operation
#[derive(Debug)]
pub struct SyncResult {
    pub table: String,
    pub rows_pushed: usize,
    pub rows_pulled: usize,
}

impl SyncResult {
    pub fn new(table: &str) -> Self {
        Self {
            table: table.to_string(),
            rows_pushed: 0,
            rows_pulled: 0,
        }
    }

    pub fn has_changes(&self) -> bool {
        self.rows_pushed > 0 || self.rows_pulled > 0
    }
}

/// Push local changes to remote (local -> D1).
///
/// Takes concrete types rather than `DataSource` generics because
/// `push` uses `D1Client::upsert_rows_batched` with progress callbacks,
/// which is transport-specific and not part of the `DataSource` trait.
pub async fn push_table(
    local: &LocalDb,
    remote: &D1Client,
    table: &str,
    diff: &TableDiff,
    conflict_resolution: ConflictResolution,
    dry_run: bool,
) -> Result<SyncResult> {
    let mut result = SyncResult::new(table);
    let rows_to_push = diff.rows_to_push(conflict_resolution);

    if rows_to_push.is_empty() {
        info!("No changes to push for table: {}", table);
        return Ok(result);
    }

    info!(
        "Pushing {} rows to table: {} (dry_run={})",
        rows_to_push.len(),
        table,
        dry_run
    );

    if dry_run {
        result.rows_pushed = rows_to_push.len();
        return Ok(result);
    }

    // Fetch full row data from local
    let rows = local.get_rows(table, &rows_to_push).await?;

    if rows.is_empty() {
        warn!("No rows found locally for push");
        return Ok(result);
    }

    // Upsert to remote (batch system handles D1 param limits)
    let pb = ProgressBar::new(rows.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
            .unwrap(),
    );
    pb.set_message(format!("Pushing {}", table));

    let batch_config = crate::config::BatchConfig::default();
    let count = remote
        .upsert_rows_batched(table, &rows, &batch_config, |batch_len| {
            pb.inc(batch_len as u64);
        })
        .await?;
    result.rows_pushed = count;

    pb.finish_with_message(format!("Pushed {} rows", result.rows_pushed));

    Ok(result)
}

/// Pull remote changes to local (D1 -> local)
pub async fn pull_table(
    local: &LocalDb,
    remote: &D1Client,
    table: &str,
    diff: &TableDiff,
    conflict_resolution: ConflictResolution,
    dry_run: bool,
) -> Result<SyncResult> {
    let mut result = SyncResult::new(table);
    let rows_to_pull = diff.rows_to_pull(conflict_resolution);

    if rows_to_pull.is_empty() {
        info!("No changes to pull for table: {}", table);
        return Ok(result);
    }

    info!(
        "Pulling {} rows to table: {} (dry_run={})",
        rows_to_pull.len(),
        table,
        dry_run
    );

    if dry_run {
        result.rows_pulled = rows_to_pull.len();
        return Ok(result);
    }

    // Fetch full row data from remote
    let rows = remote.get_rows(table, &rows_to_pull).await?;

    if rows.is_empty() {
        warn!("No rows found remotely for pull");
        return Ok(result);
    }

    // Upsert to local
    let pb = ProgressBar::new(rows.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
            .unwrap(),
    );
    pb.set_message(format!("Pulling {}", table));

    let count = local.upsert_rows(table, &rows).await?;
    result.rows_pulled = count;

    pb.finish_with_message(format!("Pulled {} rows", result.rows_pulled));

    Ok(result)
}

/// Get list of tables to sync based on config
pub async fn get_tables_to_sync<A: DataSource, B: DataSource>(
    local: &A,
    remote: &B,
    config: &Config,
) -> Result<Vec<String>> {
    // Get tables from both sides
    let local_tables: std::collections::HashSet<_> =
        local.list_tables().await?.into_iter().collect();
    let remote_tables: std::collections::HashSet<_> =
        remote.list_tables().await?.into_iter().collect();

    // Find common tables
    let common: Vec<String> = local_tables
        .intersection(&remote_tables)
        .filter(|t| config.should_sync_table(t))
        .cloned()
        .collect();

    // Filter out tables without primary keys (required for change detection)
    let mut syncable = Vec::new();
    for table in common {
        match local.table_info(&table).await {
            Ok(info) if !info.primary_key.is_empty() => {
                syncable.push(table);
            }
            Ok(_) => {
                warn!(
                    "Skipping table '{}': no primary key (required for change detection)",
                    table
                );
            }
            Err(e) => {
                warn!("Skipping table '{}': {}", table, e);
            }
        }
    }

    // Warn about tables only on one side
    for table in local_tables.difference(&remote_tables) {
        if config.should_sync_table(table) {
            warn!("Table '{}' exists only in local database", table);
        }
    }

    for table in remote_tables.difference(&local_tables) {
        if config.should_sync_table(table) {
            warn!("Table '{}' exists only in remote D1", table);
        }
    }

    info!("Found {} tables to sync", syncable.len());
    Ok(syncable)
}

/// Push all tables
pub async fn push_all(
    local: &LocalDb,
    remote: &D1Client,
    config: &Config,
    tables: Option<Vec<String>>,
    dry_run: bool,
) -> Result<Vec<SyncResult>> {
    let tables_to_sync = match tables {
        Some(t) => t,
        None => get_tables_to_sync(local, remote, config).await?,
    };

    let mut results = Vec::new();

    for table in &tables_to_sync {
        let diff = diff_table(local, remote, table, &config.sync.timestamp_column).await?;

        if !diff.has_changes() {
            info!("Table {} is in sync", table);
            results.push(SyncResult::new(table));
            continue;
        }

        let result = push_table(
            local,
            remote,
            table,
            &diff,
            config.sync.conflict_resolution,
            dry_run,
        )
        .await?;
        results.push(result);
    }

    Ok(results)
}

/// Pull all tables
pub async fn pull_all(
    local: &LocalDb,
    remote: &D1Client,
    config: &Config,
    tables: Option<Vec<String>>,
    dry_run: bool,
) -> Result<Vec<SyncResult>> {
    let tables_to_sync = match tables {
        Some(t) => t,
        None => get_tables_to_sync(local, remote, config).await?,
    };

    let mut results = Vec::new();

    for table in &tables_to_sync {
        let diff = diff_table(local, remote, table, &config.sync.timestamp_column).await?;

        if !diff.has_changes() {
            info!("Table {} is in sync", table);
            results.push(SyncResult::new(table));
            continue;
        }

        let result = pull_table(
            local,
            remote,
            table,
            &diff,
            config.sync.conflict_resolution,
            dry_run,
        )
        .await?;
        results.push(result);
    }

    Ok(results)
}
