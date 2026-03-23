//! Sync orchestration
//!
//! All sync functions are generic over the [`DataSource`] trait, enabling
//! any pair of data sources (local<->local, local<->D1, local<->S3, etc.)
//! to sync using the same diff-and-apply engine.

use crate::config::{BatchConfig, Config, ConflictResolution};
use crate::datasource::DataSource;
use crate::diff::{diff_table, TableDiff};
use crate::error::Result;
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

fn progress_bar(total: u64, message: String) -> ProgressBar {
    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
            .unwrap(),
    );
    pb.set_message(message);
    pb
}

/// Transfer rows from one DataSource to another.
///
/// Fetches rows by primary key from `source`, then upserts into `dest`
/// in chunks (sized by `batch_config.batch_size`) with progress display.
async fn transfer_rows<Src: DataSource, Dst: DataSource>(
    source: &Src,
    dest: &Dst,
    table: &str,
    pk_values: &[String],
    batch_config: &BatchConfig,
    label: &str,
) -> Result<usize> {
    let rows = source.get_rows(table, pk_values).await?;

    if rows.is_empty() {
        warn!("No rows found in source for {}", label);
        return Ok(0);
    }

    let pb = progress_bar(rows.len() as u64, format!("{} {}", label, table));
    let mut total = 0;

    for chunk in rows.chunks(batch_config.batch_size) {
        let count = dest.upsert_rows(table, chunk).await?;
        total += count;
        pb.inc(chunk.len() as u64);
    }

    pb.finish_with_message(format!("{} {} rows", label, total));
    Ok(total)
}

/// Push changes from source to destination for a single table.
pub async fn push_table<Src: DataSource, Dst: DataSource>(
    source: &Src,
    dest: &Dst,
    table: &str,
    diff: &TableDiff,
    conflict_resolution: ConflictResolution,
    batch_config: &BatchConfig,
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

    result.rows_pushed =
        transfer_rows(source, dest, table, &rows_to_push, batch_config, "Pushing").await?;
    Ok(result)
}

/// Pull changes from source to destination for a single table.
pub async fn pull_table<Src: DataSource, Dst: DataSource>(
    local: &Dst,
    remote: &Src,
    table: &str,
    diff: &TableDiff,
    conflict_resolution: ConflictResolution,
    batch_config: &BatchConfig,
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

    result.rows_pulled =
        transfer_rows(remote, local, table, &rows_to_pull, batch_config, "Pulling").await?;
    Ok(result)
}

/// Bidirectional sync of a single table: push source->dest and pull dest->source.
pub async fn sync_table<A: DataSource, B: DataSource>(
    a: &A,
    b: &B,
    table: &str,
    timestamp_column: &str,
    conflict_resolution: ConflictResolution,
    batch_config: &BatchConfig,
    dry_run: bool,
) -> Result<SyncResult> {
    let diff = diff_table(a, b, table, timestamp_column).await?;

    if !diff.has_changes() {
        info!("Table {} is in sync", table);
        return Ok(SyncResult::new(table));
    }

    let push_result =
        push_table(a, b, table, &diff, conflict_resolution, batch_config, dry_run).await?;
    let pull_result =
        pull_table(a, b, table, &diff, conflict_resolution, batch_config, dry_run).await?;

    Ok(SyncResult {
        table: table.to_string(),
        rows_pushed: push_result.rows_pushed,
        rows_pulled: pull_result.rows_pulled,
    })
}

/// Get list of tables to sync based on config
pub async fn get_tables_to_sync<A: DataSource, B: DataSource>(
    local: &A,
    remote: &B,
    config: &Config,
) -> Result<Vec<String>> {
    let local_tables: std::collections::HashSet<_> =
        local.list_tables().await?.into_iter().collect();
    let remote_tables: std::collections::HashSet<_> =
        remote.list_tables().await?.into_iter().collect();

    let common: Vec<String> = local_tables
        .intersection(&remote_tables)
        .filter(|t| config.should_sync_table(t))
        .cloned()
        .collect();

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

    for table in local_tables.difference(&remote_tables) {
        if config.should_sync_table(table) {
            warn!("Table '{}' exists only in source", table);
        }
    }

    for table in remote_tables.difference(&local_tables) {
        if config.should_sync_table(table) {
            warn!("Table '{}' exists only in destination", table);
        }
    }

    info!("Found {} tables to sync", syncable.len());
    Ok(syncable)
}

/// Push all tables from source to destination.
pub async fn push_all<Src: DataSource, Dst: DataSource>(
    source: &Src,
    dest: &Dst,
    config: &Config,
    tables: Option<Vec<String>>,
    dry_run: bool,
) -> Result<Vec<SyncResult>> {
    let tables_to_sync = match tables {
        Some(t) => t,
        None => get_tables_to_sync(source, dest, config).await?,
    };

    let batch_config = BatchConfig::from_sync_config(&config.sync);
    let mut results = Vec::new();

    for table in &tables_to_sync {
        let diff = diff_table(source, dest, table, &config.sync.timestamp_column).await?;

        if !diff.has_changes() {
            info!("Table {} is in sync", table);
            results.push(SyncResult::new(table));
            continue;
        }

        let result = push_table(
            source,
            dest,
            table,
            &diff,
            config.sync.conflict_resolution,
            &batch_config,
            dry_run,
        )
        .await?;
        results.push(result);
    }

    Ok(results)
}

/// Pull all tables from source to destination.
pub async fn pull_all<Src: DataSource, Dst: DataSource>(
    local: &Dst,
    remote: &Src,
    config: &Config,
    tables: Option<Vec<String>>,
    dry_run: bool,
) -> Result<Vec<SyncResult>> {
    let tables_to_sync = match tables {
        Some(t) => t,
        None => get_tables_to_sync(local, remote, config).await?,
    };

    let batch_config = BatchConfig::from_sync_config(&config.sync);
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
            &batch_config,
            dry_run,
        )
        .await?;
        results.push(result);
    }

    Ok(results)
}

/// Bidirectional sync of all tables.
pub async fn sync_all<A: DataSource, B: DataSource>(
    a: &A,
    b: &B,
    config: &Config,
    tables: Option<Vec<String>>,
    dry_run: bool,
) -> Result<Vec<SyncResult>> {
    let tables_to_sync = match tables {
        Some(t) => t,
        None => get_tables_to_sync(a, b, config).await?,
    };

    let batch_config = BatchConfig::from_sync_config(&config.sync);
    let mut results = Vec::new();

    for table in &tables_to_sync {
        let result = sync_table(
            a,
            b,
            table,
            &config.sync.timestamp_column,
            config.sync.conflict_resolution,
            &batch_config,
            dry_run,
        )
        .await?;
        results.push(result);
    }

    Ok(results)
}
