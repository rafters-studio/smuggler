//! Sync orchestration
//!
//! All sync functions are generic over the [`DataSource`] trait, enabling
//! any pair of data sources (local<->local, local<->D1, local<->S3, etc.)
//! to sync using the same diff-and-apply engine.

use crate::config::{column_excluded, BatchConfig, Config, ConflictResolution, RetryConfig};
use crate::datasource::DataSource;
use crate::diff::{diff_table, DiffStats, TableDiff};
use crate::error::{Result, SyncError};

/// Per-table primary key lists from the diff, for verbose dry-run output.
#[derive(Debug, Clone)]
pub struct DiffDetail {
    pub local_only: Vec<String>,
    pub remote_only: Vec<String>,
    pub local_newer: Vec<String>,
    pub remote_newer: Vec<String>,
    pub content_differs: Vec<String>,
}

impl DiffDetail {
    pub fn from_diff(diff: &TableDiff) -> Self {
        Self {
            local_only: diff.local_only.clone(),
            remote_only: diff.remote_only.clone(),
            local_newer: diff.local_newer.clone(),
            remote_newer: diff.remote_newer.clone(),
            content_differs: diff.content_differs.clone(),
        }
    }
}
use std::collections::HashMap;
use tracing::{info, warn};

/// Trait for reporting sync progress to the UI layer.
///
/// The core library uses this trait instead of depending on indicatif directly,
/// allowing CLI and non-CLI consumers to provide their own progress rendering.
pub trait SyncProgress: Send + Sync {
    /// Called when a row transfer begins.
    fn on_transfer_start(&self, total_rows: usize, label: &str, table: &str);
    /// Called after each batch of rows is transferred.
    fn on_batch_complete(&self, rows_in_batch: usize);
    /// Called when a row transfer finishes.
    fn on_transfer_finish(&self, total_rows: usize, label: &str);
}

/// No-op progress reporter for headless or library usage.
pub struct NoProgress;

impl SyncProgress for NoProgress {
    fn on_transfer_start(&self, _: usize, _: &str, _: &str) {}
    fn on_batch_complete(&self, _: usize) {}
    fn on_transfer_finish(&self, _: usize, _: &str) {}
}

/// Result of a sync operation
#[derive(Debug)]
pub struct SyncResult {
    pub table: String,
    pub rows_pushed: usize,
    pub rows_pulled: usize,
    /// Per-table diff breakdown, populated when diff was computed.
    pub diff_stats: Option<DiffStats>,
    /// Per-table PK lists from diff, populated for verbose dry-run output.
    pub diff_detail: Option<DiffDetail>,
}

impl SyncResult {
    pub fn new(table: &str) -> Self {
        Self {
            table: table.to_string(),
            rows_pushed: 0,
            rows_pulled: 0,
            diff_stats: None,
            diff_detail: None,
        }
    }

    pub fn has_changes(&self) -> bool {
        self.rows_pushed > 0 || self.rows_pulled > 0
    }
}

/// Strip excluded columns from row data before transfer.
///
/// Returns rows unchanged if `exclude_columns` is empty (fast path).
fn strip_excluded_columns(
    rows: Vec<HashMap<String, serde_json::Value>>,
    exclude_columns: &[String],
) -> Vec<HashMap<String, serde_json::Value>> {
    if exclude_columns.is_empty() {
        return rows;
    }

    rows.into_iter()
        .map(|row| {
            row.into_iter()
                .filter(|(col, _)| !column_excluded(col, exclude_columns))
                .collect()
        })
        .collect()
}

/// Delay (ms) before the next retry: the server's Retry-After if present (capped
/// by `max_delay_ms` so a misbehaving server can't make us sleep unboundedly),
/// otherwise the computed exponential backoff for `attempt`.
#[cfg(feature = "native")]
fn retry_delay_ms(err: &crate::error::SyncError, attempt: u32, retry: &RetryConfig) -> u64 {
    err.retry_after_ms()
        .map(|ms| ms.min(retry.max_delay_ms))
        .unwrap_or_else(|| retry.delay_for_attempt(attempt))
}

/// Upsert one chunk into `dest`, retrying transient failures with exponential
/// backoff. Permanent errors fail fast; transient errors (see
/// [`crate::error::SyncError::is_retryable`]) retry up to `retry.max_retries`;
/// exhaustion returns `RetryExhausted` (exit 3).
#[cfg(feature = "native")]
async fn upsert_with_retry<Dst: DataSource>(
    dest: &Dst,
    table: &str,
    chunk: &[HashMap<String, serde_json::Value>],
    retry: &RetryConfig,
) -> Result<usize> {
    use crate::error::SyncError;
    let mut attempt: u32 = 0;
    loop {
        match dest.upsert_rows(table, chunk).await {
            Ok(n) => return Ok(n),
            Err(e) if e.is_retryable() && attempt < retry.max_retries => {
                let delay = retry_delay_ms(&e, attempt, retry);
                warn!(
                    "Upsert to '{}' failed (attempt {}), retrying in {}ms: {}",
                    table,
                    attempt + 1,
                    delay,
                    e
                );
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                attempt += 1;
            }
            Err(e) if e.is_retryable() => {
                return Err(SyncError::RetryExhausted {
                    attempts: attempt,
                    last_error: e.to_string(),
                });
            }
            Err(e) => return Err(e), // permanent -> fast fail
        }
    }
}

/// On WASM the engine upserts directly: retry/backoff for the browser is the JS
/// autoSync layer's job, and there is no tokio timer in the wasm build.
#[cfg(not(feature = "native"))]
async fn upsert_with_retry<Dst: DataSource>(
    dest: &Dst,
    table: &str,
    chunk: &[HashMap<String, serde_json::Value>],
    _retry: &RetryConfig,
) -> Result<usize> {
    dest.upsert_rows(table, chunk).await
}

/// Fetches rows by primary key from `source`, then upserts into `dest`
/// in chunks (sized by `batch_config.batch_size`) with progress reporting
/// via the provided [`SyncProgress`] implementation.
/// Excluded columns are stripped before upserting.
#[allow(clippy::too_many_arguments)]
async fn transfer_rows<Src: DataSource, Dst: DataSource>(
    source: &Src,
    dest: &Dst,
    table: &str,
    pk_values: &[String],
    batch_config: &BatchConfig,
    exclude_columns: &[String],
    label: &str,
    progress: &dyn SyncProgress,
) -> Result<usize> {
    let rows = source.get_rows(table, pk_values).await?;

    if rows.is_empty() {
        warn!("No rows found in source for {}", label);
        return Ok(0);
    }

    let rows = strip_excluded_columns(rows, exclude_columns);

    progress.on_transfer_start(rows.len(), label, table);
    let mut total = 0;

    for chunk in rows.chunks(batch_config.batch_size) {
        let count = upsert_with_retry(dest, table, chunk, &batch_config.retry).await?;
        total += count;
        // Advance progress by the upsert-reported count, not the attempted
        // chunk size, so the running progress total always matches the final
        // `total` reported to on_transfer_finish even if an adapter returns
        // affected-rows (< submitted) rather than submitted-rows.
        progress.on_batch_complete(count);
    }

    progress.on_transfer_finish(total, label);
    Ok(total)
}

/// Push changes from source to destination for a single table.
#[allow(clippy::too_many_arguments)]
pub async fn push_table<Src: DataSource, Dst: DataSource>(
    source: &Src,
    dest: &Dst,
    table: &str,
    diff: &TableDiff,
    conflict_resolution: ConflictResolution,
    batch_config: &BatchConfig,
    exclude_columns: &[String],
    dry_run: bool,
    progress: &dyn SyncProgress,
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

    result.rows_pushed = transfer_rows(
        source,
        dest,
        table,
        &rows_to_push,
        batch_config,
        exclude_columns,
        "Pushing",
        progress,
    )
    .await?;
    Ok(result)
}

/// Pull changes from source to destination for a single table.
#[allow(clippy::too_many_arguments)]
pub async fn pull_table<Src: DataSource, Dst: DataSource>(
    local: &Dst,
    remote: &Src,
    table: &str,
    diff: &TableDiff,
    conflict_resolution: ConflictResolution,
    batch_config: &BatchConfig,
    exclude_columns: &[String],
    dry_run: bool,
    progress: &dyn SyncProgress,
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

    result.rows_pulled = transfer_rows(
        remote,
        local,
        table,
        &rows_to_pull,
        batch_config,
        exclude_columns,
        "Pulling",
        progress,
    )
    .await?;
    Ok(result)
}

/// Build the terminal [`SyncResult`] for a table that is already in sync.
///
/// Logs the "in sync" line and attaches the dry-run `stats`/`detail` (both
/// `None` outside dry-run, so the transferred-row counts stay zero). Shared by
/// the directional driver ([`run_directional`]), the bidirectional
/// [`sync_table`], and `stash::sync_table` so the no-op-table result cannot
/// drift between what were three separate copies of this block.
pub(crate) fn finalize_in_sync(
    table: &str,
    stats: Option<DiffStats>,
    detail: Option<DiffDetail>,
) -> SyncResult {
    info!("Table {} is in sync", table);
    let mut r = SyncResult::new(table);
    r.diff_stats = stats;
    r.diff_detail = detail;
    r
}

/// Bidirectional sync of a single table: push source->dest and pull dest->source.
#[allow(clippy::too_many_arguments)]
pub async fn sync_table<A: DataSource, B: DataSource>(
    a: &A,
    b: &B,
    table: &str,
    timestamp_column: &str,
    conflict_resolution: ConflictResolution,
    batch_config: &BatchConfig,
    exclude_columns: &[String],
    converge_columns: &[String],
    dry_run: bool,
    progress: &dyn SyncProgress,
) -> Result<SyncResult> {
    let diff = diff_table(
        a,
        b,
        table,
        timestamp_column,
        exclude_columns,
        converge_columns,
    )
    .await?;
    diff.warn_unresolved_conflicts(conflict_resolution);
    let (stats, detail) = if dry_run {
        (Some(diff.stats()), Some(DiffDetail::from_diff(&diff)))
    } else {
        (None, None)
    };

    if !diff.has_changes() {
        return Ok(finalize_in_sync(table, stats, detail));
    }

    let push_result = push_table(
        a,
        b,
        table,
        &diff,
        conflict_resolution,
        batch_config,
        exclude_columns,
        dry_run,
        progress,
    )
    .await?;
    let pull_result = pull_table(
        a,
        b,
        table,
        &diff,
        conflict_resolution,
        batch_config,
        exclude_columns,
        dry_run,
        progress,
    )
    .await?;

    Ok(SyncResult {
        table: table.to_string(),
        rows_pushed: push_result.rows_pushed,
        rows_pulled: pull_result.rows_pulled,
        diff_stats: stats,
        diff_detail: detail,
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

    // The message names what each side actually has, unfiltered, so an
    // operator reading "Remote tables: [...]" sees the remote, not the
    // remote after their own config was applied to it. Sorted so two runs
    // print the same message.
    let mut local_all: Vec<String> = local_tables.iter().cloned().collect();
    let mut remote_all: Vec<String> = remote_tables.iter().cloned().collect();
    local_all.sort();
    remote_all.sort();
    check_table_set(&config.sync.tables, &local_all, &remote_all, &syncable)?;

    info!("Found {} tables to sync", syncable.len());
    Ok(syncable)
}

/// Refuse a table set that would make a run report success while moving
/// nothing (#438).
///
/// An empty intersection used to fall through to `status: ok` with an empty
/// `tables` list and exit 0, which is what "already in sync" also looks like.
/// Pointing smugglr at the wrong database, or at an unrelated server that
/// answers 200, therefore looked like success. Three cases are errors, one is
/// not:
///
/// 1. A configured `[sync].tables` entry that exists on neither side is a
///    typo or a wrong target; name it.
/// 2. Configured tables that all miss one side: name the config and both
///    sides' tables.
/// 3. No configured list, and no table shared by both sides, while at least
///    one side has tables: name both sides.
/// 4. Both sides have no tables at all: a warning and an empty set, because
///    there is nothing the operator could have meant.
///
/// `local` and `remote` are every table each side reports, unfiltered, so the
/// message describes the databases rather than the config's view of them.
/// `syncable` is the resolved set (shared, passing the config, with a primary
/// key). Pure, so the wasm client applies the same rule.
pub fn check_table_set(
    configured: &[String],
    local: &[String],
    remote: &[String],
    syncable: &[String],
) -> Result<()> {
    let missing: Vec<&str> = configured
        .iter()
        .filter(|t| !local.contains(t) && !remote.contains(t))
        .map(String::as_str)
        .collect();
    if !missing.is_empty() {
        return Err(SyncError::Config(format!(
            "configured table(s) {:?} exist on neither side. Local tables: {:?}. Remote tables: {:?}",
            missing, local, remote
        )));
    }
    if !syncable.is_empty() {
        return Ok(());
    }
    if !configured.is_empty() {
        return Err(SyncError::Config(format!(
            "none of the configured tables {:?} exists on both sides with a primary key. Local tables: {:?}. Remote tables: {:?}",
            configured, local, remote
        )));
    }
    if !local.is_empty() || !remote.is_empty() {
        return Err(SyncError::Config(format!(
            "no table exists on both sides with a primary key, so nothing would sync. Local tables: {:?}. Remote tables: {:?}. Is the target the database you meant?",
            local, remote
        )));
    }
    warn!("both sides have no tables; nothing to sync");
    Ok(())
}

/// Direction of a single-orientation, all-tables sync run.
///
/// Selects which per-table transfer the shared [`run_directional`] driver
/// applies. The diff is computed identically in both directions
/// (`diff_table(first, second)`); only the transferred rows and the populated
/// [`SyncResult`] count field differ, and that difference lives entirely in the
/// per-table transfer selected here.
#[derive(Debug, Clone, Copy)]
enum Direction {
    /// `first -> second`: [`push_table`], recording `rows_pushed`.
    Push,
    /// `second -> first`: [`pull_table`], recording `rows_pulled`.
    Pull,
}

/// Single-orientation sync of every table between `first` and `second`.
///
/// Backs both [`push_all`] and [`pull_all`]: they were byte-identical per-table
/// loops (resolve tables, diff, warn on unresolved conflicts, capture dry-run
/// stats/detail, skip in-sync tables, transfer, attach stats/detail) differing
/// only by which per-table transfer ran. `direction` injects that one
/// difference so the loop cannot drift between the two callers.
#[allow(clippy::too_many_arguments)]
async fn run_directional<A: DataSource, B: DataSource>(
    first: &A,
    second: &B,
    config: &Config,
    tables: Option<Vec<String>>,
    dry_run: bool,
    progress: &dyn SyncProgress,
    direction: Direction,
) -> Result<Vec<SyncResult>> {
    let tables_to_sync = match tables {
        Some(t) => t,
        None => get_tables_to_sync(first, second, config).await?,
    };

    let batch_config = BatchConfig::from_sync_config(&config.sync);
    let mut results = Vec::new();

    for table in &tables_to_sync {
        let diff = diff_table(
            first,
            second,
            table,
            &config.sync.timestamp_column,
            &config.sync.exclude_columns,
            &config.sync.converge_columns,
        )
        .await?;
        diff.warn_unresolved_conflicts(config.sync.conflict_resolution);

        let (stats, detail) = if dry_run {
            (Some(diff.stats()), Some(DiffDetail::from_diff(&diff)))
        } else {
            (None, None)
        };

        if !diff.has_changes() {
            results.push(finalize_in_sync(table, stats, detail));
            continue;
        }

        let mut result = match direction {
            Direction::Push => {
                push_table(
                    first,
                    second,
                    table,
                    &diff,
                    config.sync.conflict_resolution,
                    &batch_config,
                    &config.sync.exclude_columns,
                    dry_run,
                    progress,
                )
                .await?
            }
            Direction::Pull => {
                pull_table(
                    first,
                    second,
                    table,
                    &diff,
                    config.sync.conflict_resolution,
                    &batch_config,
                    &config.sync.exclude_columns,
                    dry_run,
                    progress,
                )
                .await?
            }
        };
        result.diff_stats = stats;
        result.diff_detail = detail;
        results.push(result);
    }

    Ok(results)
}

/// Push all tables from source to destination.
pub async fn push_all<Src: DataSource, Dst: DataSource>(
    source: &Src,
    dest: &Dst,
    config: &Config,
    tables: Option<Vec<String>>,
    dry_run: bool,
    progress: &dyn SyncProgress,
) -> Result<Vec<SyncResult>> {
    run_directional(
        source,
        dest,
        config,
        tables,
        dry_run,
        progress,
        Direction::Push,
    )
    .await
}

/// Pull all tables from source to destination.
pub async fn pull_all<Src: DataSource, Dst: DataSource>(
    local: &Dst,
    remote: &Src,
    config: &Config,
    tables: Option<Vec<String>>,
    dry_run: bool,
    progress: &dyn SyncProgress,
) -> Result<Vec<SyncResult>> {
    run_directional(
        local,
        remote,
        config,
        tables,
        dry_run,
        progress,
        Direction::Pull,
    )
    .await
}

/// Bidirectional sync of all tables.
pub async fn sync_all<A: DataSource, B: DataSource>(
    a: &A,
    b: &B,
    config: &Config,
    tables: Option<Vec<String>>,
    dry_run: bool,
    progress: &dyn SyncProgress,
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
            &config.sync.exclude_columns,
            &config.sync.converge_columns,
            dry_run,
            progress,
        )
        .await?;
        results.push(result);
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn a_configured_table_on_neither_side_is_named() {
        let err = check_table_set(
            &s(&["users", "postz"]),
            &s(&["users"]),
            &s(&["users"]),
            &s(&["users"]),
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(matches!(err, SyncError::Config(_)), "{msg}");
        assert!(msg.contains("[\"postz\"] exist on neither side"), "{msg}");
    }

    #[test]
    fn configured_tables_that_miss_one_side_are_an_error() {
        let err =
            check_table_set(&s(&["orders"]), &s(&["orders"]), &s(&["customers"]), &[]).unwrap_err();
        let msg = err.to_string();
        assert!(matches!(err, SyncError::Config(_)), "{msg}");
        assert!(msg.contains("none of the configured tables"), "{msg}");
        assert!(msg.contains("Remote tables: [\"customers\"]"), "{msg}");
    }

    #[test]
    fn an_empty_intersection_with_tables_on_a_side_is_an_error() {
        let err = check_table_set(&[], &s(&["orders", "customers"]), &[], &[]).unwrap_err();
        let msg = err.to_string();
        assert!(matches!(err, SyncError::Config(_)), "{msg}");
        assert!(
            msg.contains("Is the target the database you meant?"),
            "{msg}"
        );
        assert!(
            msg.contains("Local tables: [\"orders\", \"customers\"]"),
            "{msg}"
        );
    }

    #[test]
    fn two_empty_databases_are_a_warned_no_op() {
        assert!(check_table_set(&[], &[], &[], &[]).is_ok());
    }

    #[cfg(feature = "native")]
    #[tokio::test]
    async fn pointing_at_a_database_with_no_shared_table_is_a_config_error() {
        use crate::local::LocalDb;
        let dir = std::env::temp_dir().join(format!("smugglr-438-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let a = dir.join("a.db");
        let b = dir.join("b.db");
        for (path, ddl) in [
            (&a, "CREATE TABLE orders (id TEXT PRIMARY KEY NOT NULL, updated_at INTEGER NOT NULL);"),
            (&b, "CREATE TABLE unrelated (id TEXT PRIMARY KEY NOT NULL, updated_at INTEGER NOT NULL);"),
        ] {
            let conn = rusqlite::Connection::open(path).expect("open");
            conn.execute_batch(ddl).expect("ddl");
        }
        let local = LocalDb::open(&a).expect("local");
        let remote = LocalDb::open(&b).expect("remote");
        let config: Config = toml::from_str(
            "local_db = \"a.db\"\n[target]\ntype = \"sqlite\"\ndatabase = \"b.db\"\n",
        )
        .expect("minimal config");
        let err = get_tables_to_sync(&local, &remote, &config)
            .await
            .expect_err("no shared table must be an error, not an empty list");
        let msg = err.to_string();
        assert!(matches!(err, SyncError::Config(_)), "{msg}");
        assert!(msg.contains("\"orders\""), "{msg}");
        assert!(msg.contains("\"unrelated\""), "{msg}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_shared_table_passes() {
        assert!(check_table_set(&[], &s(&["a", "b"]), &s(&["b", "c"]), &s(&["b"])).is_ok());
        assert!(check_table_set(&s(&["b"]), &s(&["a", "b"]), &s(&["b", "c"]), &s(&["b"])).is_ok());
    }

    #[test]
    fn test_excluded_columns_stripped_from_rows() {
        let rows = vec![
            HashMap::from([
                ("id".to_string(), json!(1)),
                ("name".to_string(), json!("Alice")),
                ("title_embedding".to_string(), json!([0.1, 0.2, 0.3])),
                ("vector".to_string(), json!([1, 2, 3])),
            ]),
            HashMap::from([
                ("id".to_string(), json!(2)),
                ("name".to_string(), json!("Bob")),
                ("title_embedding".to_string(), json!([0.4, 0.5, 0.6])),
                ("vector".to_string(), json!([4, 5, 6])),
            ]),
        ];

        let exclude = vec!["*_embedding".to_string(), "vector".to_string()];
        let stripped = strip_excluded_columns(rows, &exclude);

        assert_eq!(stripped.len(), 2);
        for row in &stripped {
            assert!(row.contains_key("id"));
            assert!(row.contains_key("name"));
            assert!(!row.contains_key("title_embedding"));
            assert!(!row.contains_key("vector"));
        }
    }

    #[test]
    fn test_empty_exclusion_preserves_all_columns() {
        let rows = vec![HashMap::from([
            ("id".to_string(), json!(1)),
            ("name".to_string(), json!("Alice")),
            ("embedding".to_string(), json!([0.1, 0.2])),
        ])];

        let stripped = strip_excluded_columns(rows.clone(), &[]);
        assert_eq!(stripped, rows);
    }

    #[test]
    fn test_strip_with_prefix_pattern() {
        let rows = vec![HashMap::from([
            ("id".to_string(), json!(1)),
            ("blob_image".to_string(), json!("base64data")),
            ("blob_thumb".to_string(), json!("base64thumb")),
            ("title".to_string(), json!("Photo")),
        ])];

        let exclude = vec!["blob_*".to_string()];
        let stripped = strip_excluded_columns(rows, &exclude);

        assert_eq!(stripped[0].len(), 2);
        assert!(stripped[0].contains_key("id"));
        assert!(stripped[0].contains_key("title"));
    }

    // Retry/backoff is the native write path; the wasm variant passes through.
    #[cfg(feature = "native")]
    mod retry {
        use super::super::upsert_with_retry;
        use crate::config::RetryConfig;
        use crate::datasource::{DataSource, RowMeta, TableInfo};
        use crate::error::{Result, SyncError};
        use serde_json::{json, Value};
        use std::collections::HashMap;
        use std::sync::atomic::{AtomicU32, Ordering};

        /// A dest whose `upsert_rows` fails `fails_before_success` times with a
        /// transient (retryable) error, then succeeds -- or, if `permanent`,
        /// always returns a non-retryable error. Counts calls.
        struct FlakyDest {
            fails_before_success: u32,
            permanent: bool,
            calls: AtomicU32,
        }

        impl DataSource for FlakyDest {
            async fn list_tables(&self) -> Result<Vec<String>> {
                Ok(vec![])
            }
            async fn table_info(&self, _t: &str) -> Result<TableInfo> {
                Err(SyncError::TableNotFound("unused".into()))
            }
            async fn get_row_metadata(
                &self,
                _t: &str,
                _ts: &str,
                _ex: &[String],
            ) -> Result<HashMap<String, RowMeta>> {
                Ok(HashMap::new())
            }
            async fn get_rows(
                &self,
                _t: &str,
                _pks: &[String],
            ) -> Result<Vec<HashMap<String, Value>>> {
                Ok(vec![])
            }
            async fn upsert_rows(
                &self,
                _t: &str,
                rows: &[HashMap<String, Value>],
            ) -> Result<usize> {
                let n = rows.len();
                let call = self.calls.fetch_add(1, Ordering::SeqCst);
                if self.permanent {
                    return Err(SyncError::BadRequest {
                        status: 400,
                        message: "bad sql".into(),
                    });
                }
                if call < self.fails_before_success {
                    return Err(SyncError::ServerError {
                        status: 503,
                        message: "unavailable".into(),
                    });
                }
                Ok(n)
            }
            async fn row_count(&self, _t: &str) -> Result<usize> {
                Ok(0)
            }
        }

        fn fast_retry(max_retries: u32) -> RetryConfig {
            RetryConfig {
                max_retries,
                initial_delay_ms: 1,
                max_delay_ms: 5,
                backoff_multiplier: 2.0,
            }
        }

        fn one_chunk() -> Vec<HashMap<String, Value>> {
            vec![HashMap::from([("id".to_string(), json!("1"))])]
        }

        #[tokio::test]
        async fn retries_transient_then_succeeds() {
            let dest = FlakyDest {
                fails_before_success: 2,
                permanent: false,
                calls: AtomicU32::new(0),
            };
            let n = upsert_with_retry(&dest, "t", &one_chunk(), &fast_retry(5))
                .await
                .unwrap();
            assert_eq!(n, 1);
            assert_eq!(
                dest.calls.load(Ordering::SeqCst),
                3,
                "2 failures + 1 success"
            );
        }

        #[tokio::test]
        async fn exhaustion_returns_retry_exhausted_exit_3() {
            let dest = FlakyDest {
                fails_before_success: 100,
                permanent: false,
                calls: AtomicU32::new(0),
            };
            let err = upsert_with_retry(&dest, "t", &one_chunk(), &fast_retry(2))
                .await
                .unwrap_err();
            assert!(matches!(err, SyncError::RetryExhausted { attempts: 2, .. }));
            assert_eq!(err.exit_code(), 3);
            assert_eq!(dest.calls.load(Ordering::SeqCst), 3, "initial + 2 retries");
        }

        #[test]
        fn retry_after_is_capped_by_max_delay() {
            use super::super::retry_delay_ms;
            let retry = fast_retry(5); // max_delay_ms = 5
                                       // A hostile/buggy server asking for a 24h Retry-After is capped.
            let rl = SyncError::RateLimited {
                retry_after: Some(86_400),
            };
            assert_eq!(retry_delay_ms(&rl, 0, &retry), 5);
            // Without a server hint, computed backoff is used (delay_for_attempt(0)).
            let se = SyncError::ServerError {
                status: 503,
                message: "x".into(),
            };
            assert_eq!(retry_delay_ms(&se, 0, &retry), retry.delay_for_attempt(0));
        }

        #[tokio::test]
        async fn permanent_error_fails_fast_no_retry() {
            let dest = FlakyDest {
                fails_before_success: 0,
                permanent: true,
                calls: AtomicU32::new(0),
            };
            let err = upsert_with_retry(&dest, "t", &one_chunk(), &fast_retry(5))
                .await
                .unwrap_err();
            assert!(matches!(err, SyncError::BadRequest { .. }));
            assert_eq!(
                dest.calls.load(Ordering::SeqCst),
                1,
                "no retry on permanent error"
            );
        }
    }

    // Regression for #189: progress accounting must use the upsert-reported
    // count, not the attempted chunk size, so the running progress total agrees
    // with the `total` reported to on_transfer_finish for adapters that return
    // affected-rows (< submitted).
    #[cfg(feature = "native")]
    mod transfer_accounting {
        use super::super::{transfer_rows, SyncProgress};
        use crate::config::BatchConfig;
        use crate::datasource::{DataSource, RowMeta, TableInfo};
        use crate::error::{Result, SyncError};
        use serde_json::{json, Value};
        use std::collections::HashMap;
        use std::sync::atomic::{AtomicUsize, Ordering};

        /// Source returning `n` single-column rows keyed by the requested pks.
        struct CountedSource {
            n: usize,
        }

        impl DataSource for CountedSource {
            async fn list_tables(&self) -> Result<Vec<String>> {
                Ok(vec![])
            }
            async fn table_info(&self, _t: &str) -> Result<TableInfo> {
                Err(SyncError::TableNotFound("unused".into()))
            }
            async fn get_row_metadata(
                &self,
                _t: &str,
                _ts: &str,
                _ex: &[String],
            ) -> Result<HashMap<String, RowMeta>> {
                Ok(HashMap::new())
            }
            async fn get_rows(
                &self,
                _t: &str,
                _pks: &[String],
            ) -> Result<Vec<HashMap<String, Value>>> {
                Ok((0..self.n)
                    .map(|i| HashMap::from([("id".to_string(), json!(i.to_string()))]))
                    .collect())
            }
            async fn upsert_rows(&self, _t: &str, _r: &[HashMap<String, Value>]) -> Result<usize> {
                Ok(0)
            }
            async fn row_count(&self, _t: &str) -> Result<usize> {
                Ok(self.n)
            }
        }

        /// Dest whose upsert reports fewer rows than submitted (affected-rows
        /// semantics): returns `submitted - 1`, flooring at 0.
        struct UndercountDest;

        impl DataSource for UndercountDest {
            async fn list_tables(&self) -> Result<Vec<String>> {
                Ok(vec![])
            }
            async fn table_info(&self, _t: &str) -> Result<TableInfo> {
                Err(SyncError::TableNotFound("unused".into()))
            }
            async fn get_row_metadata(
                &self,
                _t: &str,
                _ts: &str,
                _ex: &[String],
            ) -> Result<HashMap<String, RowMeta>> {
                Ok(HashMap::new())
            }
            async fn get_rows(
                &self,
                _t: &str,
                _pks: &[String],
            ) -> Result<Vec<HashMap<String, Value>>> {
                Ok(vec![])
            }
            async fn upsert_rows(
                &self,
                _t: &str,
                rows: &[HashMap<String, Value>],
            ) -> Result<usize> {
                Ok(rows.len().saturating_sub(1))
            }
            async fn row_count(&self, _t: &str) -> Result<usize> {
                Ok(0)
            }
        }

        /// Sums the rows reported to on_batch_complete and on_transfer_finish.
        struct RecordingProgress {
            batch_sum: AtomicUsize,
            finish_total: AtomicUsize,
        }

        impl SyncProgress for RecordingProgress {
            fn on_transfer_start(&self, _: usize, _: &str, _: &str) {}
            fn on_batch_complete(&self, rows_in_batch: usize) {
                self.batch_sum.fetch_add(rows_in_batch, Ordering::SeqCst);
            }
            fn on_transfer_finish(&self, total_rows: usize, _: &str) {
                self.finish_total.store(total_rows, Ordering::SeqCst);
            }
        }

        #[tokio::test]
        async fn progress_sum_matches_finish_total_on_undercount() {
            // 5 rows, batch_size 2 -> chunks of 2, 2, 1. Each chunk upsert
            // reports len-1, so total = 1 + 1 + 0 = 2.
            let source = CountedSource { n: 5 };
            let dest = UndercountDest;
            let pks: Vec<String> = (0..5).map(|i| i.to_string()).collect();
            let batch = BatchConfig {
                batch_size: 2,
                ..Default::default()
            };

            let progress = RecordingProgress {
                batch_sum: AtomicUsize::new(0),
                finish_total: AtomicUsize::new(0),
            };

            let total = transfer_rows(
                &source,
                &dest,
                "items",
                &pks,
                &batch,
                &[],
                "push",
                &progress,
            )
            .await
            .unwrap();

            assert_eq!(total, 2, "returned total is the sum of upsert counts");
            // The bug: progress advanced by chunk.len() (5) while finish total
            // was 2. After the fix both equal the returned total.
            assert_eq!(
                progress.batch_sum.load(Ordering::SeqCst),
                total,
                "progress batch sum must equal the reported total"
            );
            assert_eq!(progress.finish_total.load(Ordering::SeqCst), total);
        }
    }

    // Regression for #324: `pull` with `exclude_columns` set nulled the
    // excluded column in the LOCAL database. `transfer_rows` strips the column
    // from every row it sends, and on `pull` the destination is the local DB --
    // which derived its column list from its own PRAGMA and bound the absent
    // column as an explicit NULL. Silent, and every step reported success.
    //
    // The assertions read the destination row's contents after the pull, not
    // the pull's own report. Nothing downstream compares an excluded column --
    // it is out of the content hash by definition -- so a test that asserts
    // "the sync succeeded" cannot see this failure at all.
    #[cfg(feature = "native")]
    mod pull_with_exclude_columns {
        use super::super::{pull_table, NoProgress};
        use crate::config::{BatchConfig, ConflictResolution};
        use crate::diff::TableDiff;
        use crate::local::LocalDb;
        use rusqlite::Connection;
        use tempfile::TempDir;

        fn seed(path: &std::path::Path, rows: &[(&str, &str, &str, &str)]) {
            let conn = Connection::open(path).unwrap();
            conn.execute_batch(
                "CREATE TABLE notes (
                     id TEXT PRIMARY KEY,
                     body TEXT,
                     ssn TEXT DEFAULT 'unset',
                     updated_at TEXT
                 );",
            )
            .unwrap();
            for (id, body, ssn, ts) in rows {
                conn.execute(
                    "INSERT INTO notes (id, body, ssn, updated_at) VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![id, body, ssn, ts],
                )
                .unwrap();
            }
        }

        fn read(path: &std::path::Path, id: &str) -> (String, Option<String>) {
            let conn = Connection::open(path).unwrap();
            conn.query_row("SELECT body, ssn FROM notes WHERE id = ?", [id], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap()
        }

        #[tokio::test]
        async fn pull_does_not_null_the_local_value_of_an_excluded_column() {
            let dir = TempDir::new().unwrap();
            let local_path = dir.path().join("local.sqlite");
            let remote_path = dir.path().join("remote.sqlite");
            seed(
                &local_path,
                &[("n1", "local body", "local secret", "2026-01-01")],
            );
            seed(
                &remote_path,
                &[("n1", "remote body", "remote secret", "2026-02-01")],
            );

            let local = LocalDb::open(&local_path).unwrap();
            let remote = LocalDb::open(&remote_path).unwrap();

            let mut diff = TableDiff::new("notes");
            diff.remote_newer.push("n1".to_string());

            let result = pull_table(
                &local,
                &remote,
                "notes",
                &diff,
                ConflictResolution::NewerWins,
                &BatchConfig::default(),
                &["ssn".to_string()],
                false,
                &NoProgress,
            )
            .await
            .unwrap();
            assert_eq!(result.rows_pulled, 1, "the row did transfer");
            drop(local);

            let (body, ssn) = read(&local_path, "n1");
            assert_eq!(body, "remote body", "the carried columns applied");
            assert_eq!(
                ssn.as_deref(),
                Some("local secret"),
                "the excluded column's LOCAL value must survive the pull"
            );
        }

        #[tokio::test]
        async fn pulling_a_row_that_does_not_exist_locally_takes_the_schema_default() {
            // The other half of #324: with no prior local row there is no value
            // to destroy, so the excluded column landing at its schema default
            // is correct rather than a loss. Pinned so a future change to the
            // subset path cannot turn an insert into a rejection.
            let dir = TempDir::new().unwrap();
            let local_path = dir.path().join("local.sqlite");
            let remote_path = dir.path().join("remote.sqlite");
            seed(&local_path, &[]);
            seed(
                &remote_path,
                &[("n2", "remote body", "remote secret", "2026-02-01")],
            );

            let local = LocalDb::open(&local_path).unwrap();
            let remote = LocalDb::open(&remote_path).unwrap();

            let mut diff = TableDiff::new("notes");
            diff.remote_only.push("n2".to_string());

            let result = pull_table(
                &local,
                &remote,
                "notes",
                &diff,
                ConflictResolution::NewerWins,
                &BatchConfig::default(),
                &["ssn".to_string()],
                false,
                &NoProgress,
            )
            .await
            .unwrap();
            assert_eq!(result.rows_pulled, 1);
            drop(local);

            let (body, ssn) = read(&local_path, "n2");
            assert_eq!(body, "remote body");
            assert_eq!(ssn.as_deref(), Some("unset"), "schema default, not a loss");
        }
    }

    // Drive both push_all and pull_all through the shared `run_directional`
    // driver over two real LocalDb sources, pinning the observable contract the
    // refactor must preserve: same rows selected per direction, dry-run
    // stats/detail populated, and in-sync tables skipped with stats still
    // attached (the `finalize_in_sync` path). Uses LocalDb, so native-only.
    #[cfg(feature = "native")]
    mod directional {
        use super::super::{pull_all, push_all, NoProgress};
        use crate::config::Config;
        use crate::local::LocalDb;
        use rusqlite::Connection;
        use tempfile::TempDir;

        /// Create `items` and `insync` tables and seed `items` with the given rows.
        /// `insync` is seeded identically on both sides by the caller.
        fn seed(path: &std::path::Path, items: &[(i64, &str, &str)], insync: &[(i64, &str, &str)]) {
            let conn = Connection::open(path).unwrap();
            conn.execute_batch(
                "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL, updated_at TEXT);
                 CREATE TABLE insync (id INTEGER PRIMARY KEY, name TEXT NOT NULL, updated_at TEXT);",
            )
            .unwrap();
            for (id, name, ts) in items {
                conn.execute(
                    "INSERT INTO items (id, name, updated_at) VALUES (?1, ?2, ?3)",
                    rusqlite::params![id, name, ts],
                )
                .unwrap();
            }
            for (id, name, ts) in insync {
                conn.execute(
                    "INSERT INTO insync (id, name, updated_at) VALUES (?1, ?2, ?3)",
                    rusqlite::params![id, name, ts],
                )
                .unwrap();
            }
        }

        #[tokio::test]
        async fn push_and_pull_share_one_driver() {
            let dir = TempDir::new().unwrap();
            let local_path = dir.path().join("local.sqlite");
            let remote_path = dir.path().join("remote.sqlite");

            // items: local-only {1}, remote-only {3}, identical {2}.
            // insync: byte-identical on both sides -> no changes.
            let insync = [(1i64, "same", "100")];
            seed(
                &local_path,
                &[(1, "a", "100"), (2, "shared", "100")],
                &insync,
            );
            seed(
                &remote_path,
                &[(2, "shared", "100"), (3, "c", "100")],
                &insync,
            );

            let local = LocalDb::open(&local_path).unwrap();
            let remote = LocalDb::open(&remote_path).unwrap();
            let config = Config::from_toml_str("").unwrap();
            let tables = Some(vec!["items".to_string(), "insync".to_string()]);

            // Dry-run keeps the fixtures fixed so both directions observe the
            // same diff; it also exercises the stats/detail-population branch.
            let push = push_all(&local, &remote, &config, tables.clone(), true, &NoProgress)
                .await
                .unwrap();
            let pull = pull_all(&local, &remote, &config, tables, true, &NoProgress)
                .await
                .unwrap();

            let push_items = push.iter().find(|r| r.table == "items").unwrap();
            let pull_items = pull.iter().find(|r| r.table == "items").unwrap();

            // Direction picks the rows: push moves the local-only row (1),
            // pull moves the remote-only row (3). One row each.
            assert_eq!(push_items.rows_pushed, 1, "push selects local_only row");
            assert_eq!(push_items.rows_pulled, 0);
            assert_eq!(pull_items.rows_pulled, 1, "pull selects remote_only row");
            assert_eq!(pull_items.rows_pushed, 0);

            // Dry-run stats/detail are populated and identical in both
            // directions (the diff is computed the same way regardless).
            let stats = push_items.diff_stats.as_ref().unwrap();
            assert_eq!(stats.local_only, 1);
            assert_eq!(stats.remote_only, 1);
            assert_eq!(stats.identical, 1);
            let detail = push_items.diff_detail.as_ref().unwrap();
            assert_eq!(detail.local_only, vec!["1".to_string()]);
            assert_eq!(detail.remote_only, vec!["3".to_string()]);

            // In-sync table: skipped with zero transfer, but stats still attached
            // (the finalize_in_sync path shared across the sync loops).
            let push_insync = push.iter().find(|r| r.table == "insync").unwrap();
            assert_eq!(push_insync.rows_pushed, 0);
            assert_eq!(push_insync.rows_pulled, 0);
            let insync_stats = push_insync.diff_stats.as_ref().unwrap();
            assert_eq!(insync_stats.identical, 1);
            assert_eq!(insync_stats.local_only, 0);
            assert_eq!(insync_stats.remote_only, 0);
        }
    }
}
