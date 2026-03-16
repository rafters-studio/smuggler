//! S3-compatible relay sync for cross-machine SQLite synchronization.
//!
//! The `stash` command uploads local SQLite state to an S3-compatible relay file.
//! The `retrieve` command downloads the relay and syncs it with the local database.
//! Both use the existing diff engine since both sides are SQLite.

use crate::config::{ConflictResolution, StashConfig};
use crate::datasource::DataSource;
use crate::diff::diff_table;
use crate::error::{Result, SyncError};
use crate::local::LocalDb;
use crate::sync::SyncResult;
use object_store::path::Path as ObjectPath;
use object_store::PutMode;
use object_store::{ObjectStore, PutOptions, PutPayload};
use std::path::Path;
use std::sync::Arc;
use tempfile::NamedTempFile;
use tracing::{debug, info, warn};
use url::Url;

/// Build an `ObjectStore` client and object path from the stash config URL.
///
/// Supports:
/// - `s3://bucket/key` -- Amazon S3 or S3-compatible (R2, MinIO)
/// - `file:///absolute/path` -- Local filesystem (for testing)
fn build_store(config: &StashConfig) -> Result<(Arc<dyn ObjectStore>, ObjectPath)> {
    let url = Url::parse(&config.url).map_err(|e| SyncError::InvalidUrl(e.to_string()))?;

    match url.scheme() {
        "s3" => {
            let bucket = url
                .host_str()
                .ok_or_else(|| SyncError::InvalidUrl("S3 URL must have a bucket name".into()))?;

            let key = url.path().trim_start_matches('/');
            if key.is_empty() {
                return Err(SyncError::InvalidUrl(
                    "S3 URL must have an object key path".into(),
                ));
            }

            let mut builder = object_store::aws::AmazonS3Builder::new()
                .with_bucket_name(bucket)
                .with_region(config.region.as_deref().unwrap_or("us-east-1"));

            if let Some(ref access_key) = config.access_key_id {
                builder = builder.with_access_key_id(access_key);
            }
            if let Some(ref secret_key) = config.secret_access_key {
                builder = builder.with_secret_access_key(secret_key);
            }
            if let Some(ref endpoint) = config.endpoint {
                builder = builder.with_endpoint(endpoint);
            }

            let store = builder
                .build()
                .map_err(|e| SyncError::Stash(format!("Failed to build S3 client: {}", e)))?;

            Ok((Arc::new(store), ObjectPath::from(key)))
        }
        "file" => {
            let file_path = url
                .to_file_path()
                .map_err(|_| SyncError::InvalidUrl("Invalid file URL".into()))?;

            // object_store LocalFileSystem operates on a root directory,
            // and the ObjectPath is relative to that root.
            let parent = file_path.parent().ok_or_else(|| {
                SyncError::InvalidUrl("File URL must have a parent directory".into())
            })?;
            let filename = file_path
                .file_name()
                .ok_or_else(|| SyncError::InvalidUrl("File URL must have a filename".into()))?
                .to_str()
                .ok_or_else(|| {
                    SyncError::InvalidUrl("File URL filename is not valid UTF-8".into())
                })?;

            let store =
                object_store::local::LocalFileSystem::new_with_prefix(parent).map_err(|e| {
                    SyncError::Stash(format!("Failed to build local file store: {}", e))
                })?;

            Ok((Arc::new(store), ObjectPath::from(filename)))
        }
        scheme => Err(SyncError::InvalidUrl(format!(
            "Unsupported URL scheme '{}'. Use s3://, or file:///",
            scheme
        ))),
    }
}

/// Download the relay SQLite file from the object store to a temporary file.
///
/// Returns `(temp_file, Some(etag))` if the relay exists,
/// or `(temp_file, None)` if the relay does not exist (creates empty temp file).
async fn download_relay(
    store: &dyn ObjectStore,
    path: &ObjectPath,
    create_if_missing: bool,
) -> Result<(NamedTempFile, Option<String>)> {
    let temp_file = NamedTempFile::new()
        .map_err(|e| SyncError::Stash(format!("Failed to create temp file: {}", e)))?;

    match store.get(path).await {
        Ok(result) => {
            let etag = result.meta.e_tag.clone();

            let bytes = result.bytes().await.map_err(|e| {
                SyncError::Stash(format!(
                    "Failed to download relay body from {}: {}",
                    path, e
                ))
            })?;
            std::fs::write(temp_file.path(), &bytes).map_err(|e| {
                SyncError::Stash(format!("Failed to write relay to temp file: {}", e))
            })?;

            debug!("Downloaded relay ({} bytes, etag={:?})", bytes.len(), etag);

            Ok((temp_file, etag))
        }
        Err(object_store::Error::NotFound { .. }) => {
            if create_if_missing {
                debug!("Relay not found, will create new one");
                Ok((temp_file, None))
            } else {
                Err(SyncError::RelayNotFound(path.to_string()))
            }
        }
        Err(e) => Err(SyncError::ObjectStore(e)),
    }
}

/// Upload the relay SQLite file to the object store.
///
/// Uses ETag conditional write when `etag` is provided to prevent
/// concurrent overwrites from other machines.
async fn upload_relay(
    store: &dyn ObjectStore,
    path: &ObjectPath,
    local_path: &Path,
    etag: Option<&str>,
) -> Result<()> {
    let data = std::fs::read(local_path)
        .map_err(|e| SyncError::Stash(format!("Failed to read relay file for upload: {}", e)))?;

    let payload = PutPayload::from(data);

    // First upload (no existing relay) -- unconditional put
    let Some(tag) = etag else {
        debug!("Uploading relay (first upload)");
        store
            .put(path, payload)
            .await
            .map_err(|e| SyncError::Stash(format!("Failed to upload relay to {}: {}", path, e)))?;
        info!("Relay uploaded successfully");
        return Ok(());
    };

    // Subsequent upload -- conditional put with ETag to prevent concurrent overwrites
    debug!("Uploading relay with ETag condition: {}", tag);
    let opts = PutOptions {
        mode: PutMode::Update(object_store::UpdateVersion {
            e_tag: Some(tag.to_string()),
            version: None,
        }),
        ..Default::default()
    };

    match store.put_opts(path, payload.clone(), opts).await {
        Ok(_) => {
            info!("Relay uploaded successfully");
            Ok(())
        }
        Err(object_store::Error::Precondition { .. }) => Err(SyncError::ConcurrentWrite),
        Err(object_store::Error::NotImplemented) => {
            // Backend does not support conditional writes (e.g., local filesystem).
            warn!(
                "Backend does not support conditional writes (ETag). \
                 Concurrent stash from multiple machines may overwrite each other."
            );
            store.put(path, payload).await.map_err(|e| {
                SyncError::Stash(format!("Failed to upload relay to {}: {}", path, e))
            })?;
            info!("Relay uploaded successfully (unconditional)");
            Ok(())
        }
        Err(e) => Err(SyncError::ObjectStore(e)),
    }
}

/// Sync rows from source DataSource to destination DataSource for a single table.
///
/// This is a generic version that works with any two DataSource implementations,
/// unlike the D1-specific push_table/pull_table in sync.rs.
///
/// `label` is used for logging (e.g. "stash" or "retrieve").
/// `set_count` receives the number of synced rows and records it in the result.
#[allow(clippy::too_many_arguments)]
async fn sync_table<S: DataSource, D: DataSource>(
    source: &S,
    dest: &D,
    table: &str,
    timestamp_column: &str,
    conflict_resolution: ConflictResolution,
    dry_run: bool,
    label: &str,
    set_count: impl FnOnce(&mut SyncResult, usize),
) -> Result<SyncResult> {
    let mut result = SyncResult::new(table);
    let diff = diff_table(source, dest, table, timestamp_column).await?;

    if !diff.has_changes() {
        info!("Table {} is in sync", table);
        return Ok(result);
    }

    // Both stash and retrieve use "push" semantics: source-only + source-newer rows go to dest.
    let rows_to_sync = diff.rows_to_push(conflict_resolution);

    if rows_to_sync.is_empty() {
        info!("No changes to sync for table: {}", table);
        return Ok(result);
    }

    info!(
        "{}: {} rows for table {} (dry_run={})",
        label,
        rows_to_sync.len(),
        table,
        dry_run
    );

    if dry_run {
        set_count(&mut result, rows_to_sync.len());
        return Ok(result);
    }

    let rows = source.get_rows(table, &rows_to_sync).await?;

    if rows.is_empty() {
        warn!("No rows found in source for sync");
        return Ok(result);
    }

    let count = dest.upsert_rows(table, &rows).await?;
    set_count(&mut result, count);

    info!("{}: synced {} rows for table {}", label, count, table);
    Ok(result)
}

/// Get tables to sync between two local databases, filtered by config.
///
/// Only returns tables that exist in both source and dest, pass the
/// include/exclude filters, and have a primary key.
async fn get_stash_tables(
    source: &LocalDb,
    dest: &LocalDb,
    tables_filter: Option<&[String]>,
    exclude_tables: &[String],
) -> Result<Vec<String>> {
    let source_tables: std::collections::HashSet<_> =
        source.list_tables().await?.into_iter().collect();
    let dest_tables: std::collections::HashSet<_> = dest.list_tables().await?.into_iter().collect();

    let mut syncable = Vec::new();

    for table in &source_tables {
        if exclude_tables.iter().any(|t| t == table) {
            continue;
        }

        if let Some(filter) = tables_filter {
            if !filter.iter().any(|t| t == table) {
                continue;
            }
        }

        if !dest_tables.contains(table) {
            debug!(
                "Skipping table '{}': exists only in source, not in destination",
                table
            );
            continue;
        }

        match source.table_info(table).await {
            Ok(info) if !info.primary_key.is_empty() => {
                syncable.push(table.clone());
            }
            Ok(_) => {
                warn!(
                    "Skipping table '{}': no primary key (required for change detection)",
                    table
                );
            }
            Err(SyncError::NoPrimaryKey(_)) => {
                warn!(
                    "Skipping table '{}': no primary key (required for change detection)",
                    table
                );
            }
            Err(e) => {
                return Err(SyncError::Stash(format!(
                    "Failed to inspect table '{}': {}",
                    table, e
                )));
            }
        }
    }

    syncable.sort();
    info!("Found {} tables to sync via stash", syncable.len());
    Ok(syncable)
}

/// Initialize a relay SQLite database with the same schema as the source.
///
/// Creates tables in the relay that exist in the source but not yet in the relay.
fn init_relay_schema(source: &LocalDb, relay_path: &Path) -> Result<()> {
    let source_conn = source.conn();
    let relay_conn = rusqlite::Connection::open(relay_path)?;

    // Get CREATE TABLE statements from source
    let mut stmt = source_conn.prepare(
        "SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )?;

    let tables: Vec<(String, String)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    for (name, create_sql) in &tables {
        // Check if table already exists in relay
        let exists: bool = relay_conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name=?1",
                [name],
                |row| row.get(0),
            )
            .map_err(|e| {
                SyncError::Stash(format!(
                    "Failed to check if table '{}' exists in relay: {}",
                    name, e
                ))
            })?;

        if !exists {
            debug!("Creating table '{}' in relay", name);
            relay_conn.execute_batch(create_sql)?;
        }
    }

    Ok(())
}

/// Execute the `stash` command: upload local state to S3 relay.
pub async fn stash(
    config: &StashConfig,
    local_db_path: &str,
    timestamp_column: &str,
    conflict_resolution: ConflictResolution,
    table_filter: Option<String>,
    dry_run: bool,
    exclude_tables: &[String],
) -> Result<Vec<SyncResult>> {
    let (store, obj_path) = build_store(config)?;

    // Open local database (read-only -- we only read from it)
    let local = LocalDb::open_readonly(local_db_path)?;

    // Download existing relay (or create new one).
    // IMPORTANT: temp_file must live until end of function -- dropping it deletes the file.
    let (temp_file, etag) = download_relay(store.as_ref(), &obj_path, true).await?;
    let relay_path = temp_file.path().to_path_buf();

    // Initialize relay schema from local (creates missing tables)
    init_relay_schema(&local, &relay_path)?;

    // Open relay as writable LocalDb
    let relay = LocalDb::open(&relay_path)?;

    // Determine tables to sync
    let filter_vec: Option<Vec<String>> = table_filter.map(|t| vec![t]);
    let tables = get_stash_tables(&local, &relay, filter_vec.as_deref(), exclude_tables).await?;

    let mut results = Vec::new();

    for table in &tables {
        let result = sync_table(
            &local,
            &relay,
            table,
            timestamp_column,
            conflict_resolution,
            dry_run,
            "stash",
            |r, n| r.rows_pushed = n,
        )
        .await?;
        results.push(result);
    }

    // Upload modified relay back to S3 (unless dry run)
    let has_changes = results.iter().any(|r| r.has_changes());
    if has_changes && !dry_run {
        // Drop the relay LocalDb to release file handles before uploading
        drop(relay);
        upload_relay(store.as_ref(), &obj_path, &relay_path, etag.as_deref()).await?;
    } else if dry_run {
        debug!("Dry run -- skipping upload");
    } else {
        debug!("No changes -- skipping upload");
    }

    Ok(results)
}

/// Execute the `retrieve` command: download and sync from S3 relay.
pub async fn retrieve(
    config: &StashConfig,
    local_db_path: &str,
    timestamp_column: &str,
    conflict_resolution: ConflictResolution,
    table_filter: Option<String>,
    dry_run: bool,
    exclude_tables: &[String],
) -> Result<Vec<SyncResult>> {
    let (store, obj_path) = build_store(config)?;

    // Download relay (must exist for retrieve).
    // IMPORTANT: temp_file must live until end of function -- dropping it deletes the file.
    let (temp_file, _etag) = download_relay(store.as_ref(), &obj_path, false).await?;
    let relay_path = temp_file.path().to_path_buf();

    // Open relay as read-only
    let relay = LocalDb::open_readonly(&relay_path)?;

    // Open local database as writable
    let local = LocalDb::open(local_db_path)?;

    // Determine tables to sync (relay is source, local is dest)
    let filter_vec: Option<Vec<String>> = table_filter.map(|t| vec![t]);
    let tables = get_stash_tables(&relay, &local, filter_vec.as_deref(), exclude_tables).await?;

    let mut results = Vec::new();

    for table in &tables {
        let result = sync_table(
            &relay,
            &local,
            table,
            timestamp_column,
            conflict_resolution,
            dry_run,
            "retrieve",
            |r, n| r.rows_pulled = n,
        )
        .await?;
        results.push(result);
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use tempfile::TempDir;

    /// Create a test SQLite database with a simple schema and some rows.
    fn create_test_db(path: &Path, rows: &[(i64, &str, &str)]) {
        let conn = Connection::open(path).unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS items (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                updated_at TEXT
            )",
        )
        .unwrap();

        for (id, name, updated_at) in rows {
            conn.execute(
                "INSERT OR REPLACE INTO items (id, name, updated_at) VALUES (?1, ?2, ?3)",
                rusqlite::params![id, name, updated_at],
            )
            .unwrap();
        }
    }

    /// Helper to read all rows from the items table.
    fn read_items(path: &Path) -> Vec<(i64, String, String)> {
        let conn =
            Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        let mut stmt = conn
            .prepare("SELECT id, name, updated_at FROM items ORDER BY id")
            .unwrap();
        stmt.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap()
    }

    fn make_file_stash_config(dir: &Path) -> StashConfig {
        let relay_path = dir.join("relay.sqlite");
        StashConfig {
            url: format!("file://{}", relay_path.display()),
            access_key_id: None,
            secret_access_key: None,
            region: None,
            endpoint: None,
        }
    }

    #[test]
    fn test_build_store_file_url() {
        let dir = TempDir::new().unwrap();
        let config = make_file_stash_config(dir.path());
        let result = build_store(&config);
        assert!(result.is_ok());

        let (_store, path) = result.unwrap();
        assert_eq!(path.as_ref(), "relay.sqlite");
    }

    #[test]
    fn test_build_store_s3_url() {
        let config = StashConfig {
            url: "s3://my-bucket/path/to/relay.sqlite".into(),
            access_key_id: Some("test-key".into()),
            secret_access_key: Some("test-secret".into()),
            region: Some("us-west-2".into()),
            endpoint: None,
        };
        let result = build_store(&config);
        assert!(result.is_ok());

        let (_store, path) = result.unwrap();
        assert_eq!(path.as_ref(), "path/to/relay.sqlite");
    }

    #[test]
    fn test_build_store_invalid_scheme() {
        let config = StashConfig {
            url: "ftp://bucket/key".into(),
            access_key_id: None,
            secret_access_key: None,
            region: None,
            endpoint: None,
        };
        let result = build_store(&config);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SyncError::InvalidUrl(_)));
    }

    #[test]
    fn test_build_store_invalid_url() {
        let config = StashConfig {
            url: "not a url at all".into(),
            access_key_id: None,
            secret_access_key: None,
            region: None,
            endpoint: None,
        };
        let result = build_store(&config);
        assert!(result.is_err());
    }

    #[test]
    fn test_build_store_s3_missing_key() {
        let config = StashConfig {
            url: "s3://my-bucket/".into(),
            access_key_id: None,
            secret_access_key: None,
            region: None,
            endpoint: None,
        };
        let result = build_store(&config);
        assert!(result.is_err());
    }

    #[test]
    fn test_init_relay_schema() {
        let dir = TempDir::new().unwrap();
        let source_path = dir.path().join("source.sqlite");
        let relay_path = dir.path().join("relay.sqlite");

        // Create source with schema
        create_test_db(&source_path, &[]);

        // Create empty relay
        Connection::open(&relay_path).unwrap();

        let source = LocalDb::open_readonly(&source_path).unwrap();
        init_relay_schema(&source, &relay_path).unwrap();

        // Verify relay has the items table
        let relay_conn = Connection::open(&relay_path).unwrap();
        let tables: Vec<String> = relay_conn
            .prepare(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();

        assert!(tables.contains(&"items".to_string()));
    }

    #[test]
    fn test_init_relay_schema_idempotent() {
        let dir = TempDir::new().unwrap();
        let source_path = dir.path().join("source.sqlite");
        let relay_path = dir.path().join("relay.sqlite");

        create_test_db(&source_path, &[(1, "first", "2024-01-01")]);
        create_test_db(&relay_path, &[(1, "first", "2024-01-01")]);

        let source = LocalDb::open_readonly(&source_path).unwrap();

        // Should not fail when tables already exist
        init_relay_schema(&source, &relay_path).unwrap();
        init_relay_schema(&source, &relay_path).unwrap();
    }

    #[tokio::test]
    async fn test_stash_new_relay() {
        let dir = TempDir::new().unwrap();
        let local_path = dir.path().join("local.sqlite");
        let relay_dir = dir.path().join("relay_store");
        std::fs::create_dir_all(&relay_dir).unwrap();

        // Create local db with data
        create_test_db(
            &local_path,
            &[(1, "alpha", "2024-01-01"), (2, "beta", "2024-01-02")],
        );

        let config = StashConfig {
            url: format!("file://{}/relay.sqlite", relay_dir.display()),
            access_key_id: None,
            secret_access_key: None,
            region: None,
            endpoint: None,
        };

        let results = stash(
            &config,
            local_path.to_str().unwrap(),
            "updated_at",
            ConflictResolution::LocalWins,
            None,
            false,
            &default_excludes(),
        )
        .await
        .unwrap();

        // Verify results
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].table, "items");
        assert_eq!(results[0].rows_pushed, 2);

        // Verify relay has the data
        let relay_path = relay_dir.join("relay.sqlite");
        assert!(relay_path.exists());
        let items = read_items(&relay_path);
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].1, "alpha");
        assert_eq!(items[1].1, "beta");
    }

    #[tokio::test]
    async fn test_retrieve_from_relay() {
        let dir = TempDir::new().unwrap();
        let local_path = dir.path().join("local.sqlite");
        let relay_dir = dir.path().join("relay_store");
        std::fs::create_dir_all(&relay_dir).unwrap();
        let relay_path = relay_dir.join("relay.sqlite");

        // Create local db with partial data
        create_test_db(&local_path, &[(1, "alpha", "2024-01-01")]);

        // Create relay with more data
        create_test_db(
            &relay_path,
            &[
                (1, "alpha", "2024-01-01"),
                (2, "beta", "2024-01-02"),
                (3, "gamma", "2024-01-03"),
            ],
        );

        let config = StashConfig {
            url: format!("file://{}/relay.sqlite", relay_dir.display()),
            access_key_id: None,
            secret_access_key: None,
            region: None,
            endpoint: None,
        };

        let results = retrieve(
            &config,
            local_path.to_str().unwrap(),
            "updated_at",
            ConflictResolution::LocalWins,
            None,
            false,
            &default_excludes(),
        )
        .await
        .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].table, "items");
        assert_eq!(results[0].rows_pulled, 2); // beta + gamma

        // Verify local has all 3 rows now
        let items = read_items(&local_path);
        assert_eq!(items.len(), 3);
    }

    #[tokio::test]
    async fn test_stash_dry_run() {
        let dir = TempDir::new().unwrap();
        let local_path = dir.path().join("local.sqlite");
        let relay_dir = dir.path().join("relay_store");
        std::fs::create_dir_all(&relay_dir).unwrap();

        create_test_db(&local_path, &[(1, "alpha", "2024-01-01")]);

        let config = StashConfig {
            url: format!("file://{}/relay.sqlite", relay_dir.display()),
            access_key_id: None,
            secret_access_key: None,
            region: None,
            endpoint: None,
        };

        let results = stash(
            &config,
            local_path.to_str().unwrap(),
            "updated_at",
            ConflictResolution::LocalWins,
            None,
            true, // dry run
            &default_excludes(),
        )
        .await
        .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].rows_pushed, 1);

        // Relay should NOT have been uploaded
        let relay_path = relay_dir.join("relay.sqlite");
        assert!(!relay_path.exists());
    }

    #[tokio::test]
    async fn test_retrieve_missing_relay() {
        let dir = TempDir::new().unwrap();
        let local_path = dir.path().join("local.sqlite");
        let relay_dir = dir.path().join("relay_store");
        std::fs::create_dir_all(&relay_dir).unwrap();

        create_test_db(&local_path, &[]);

        let config = StashConfig {
            url: format!("file://{}/relay.sqlite", relay_dir.display()),
            access_key_id: None,
            secret_access_key: None,
            region: None,
            endpoint: None,
        };

        let result = retrieve(
            &config,
            local_path.to_str().unwrap(),
            "updated_at",
            ConflictResolution::LocalWins,
            None,
            false,
            &default_excludes(),
        )
        .await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SyncError::RelayNotFound(_)));
    }

    #[tokio::test]
    async fn test_stash_table_filter() {
        let dir = TempDir::new().unwrap();
        let local_path = dir.path().join("local.sqlite");
        let relay_dir = dir.path().join("relay_store");
        std::fs::create_dir_all(&relay_dir).unwrap();

        // Create local db with two tables
        let conn = Connection::open(&local_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, updated_at TEXT);
             CREATE TABLE other (id INTEGER PRIMARY KEY, value TEXT, updated_at TEXT);
             INSERT INTO items VALUES (1, 'alpha', '2024-01-01');
             INSERT INTO other VALUES (1, 'data', '2024-01-01');",
        )
        .unwrap();
        drop(conn);

        let config = StashConfig {
            url: format!("file://{}/relay.sqlite", relay_dir.display()),
            access_key_id: None,
            secret_access_key: None,
            region: None,
            endpoint: None,
        };

        // Only stash the "items" table
        let results = stash(
            &config,
            local_path.to_str().unwrap(),
            "updated_at",
            ConflictResolution::LocalWins,
            Some("items".to_string()),
            false,
            &default_excludes(),
        )
        .await
        .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].table, "items");
    }

    #[tokio::test]
    async fn test_stash_no_changes() {
        let dir = TempDir::new().unwrap();
        let local_path = dir.path().join("local.sqlite");
        let relay_dir = dir.path().join("relay_store");
        std::fs::create_dir_all(&relay_dir).unwrap();
        let relay_path = relay_dir.join("relay.sqlite");

        let rows = &[(1, "alpha", "2024-01-01"), (2, "beta", "2024-01-02")];
        create_test_db(&local_path, rows);
        create_test_db(&relay_path, rows);

        let config = StashConfig {
            url: format!("file://{}/relay.sqlite", relay_dir.display()),
            access_key_id: None,
            secret_access_key: None,
            region: None,
            endpoint: None,
        };

        let results = stash(
            &config,
            local_path.to_str().unwrap(),
            "updated_at",
            ConflictResolution::LocalWins,
            None,
            false,
            &default_excludes(),
        )
        .await
        .unwrap();

        assert_eq!(results.len(), 1);
        assert!(!results[0].has_changes());
    }

    #[tokio::test]
    async fn test_stash_then_retrieve_roundtrip() {
        let dir = TempDir::new().unwrap();
        let machine_a = dir.path().join("machine_a.sqlite");
        let machine_b = dir.path().join("machine_b.sqlite");
        let relay_dir = dir.path().join("relay_store");
        std::fs::create_dir_all(&relay_dir).unwrap();

        // Machine A has some data
        create_test_db(
            &machine_a,
            &[(1, "alpha", "2024-01-01"), (2, "beta", "2024-01-02")],
        );

        // Machine B has the same schema but no data
        create_test_db(&machine_b, &[]);

        let config = StashConfig {
            url: format!("file://{}/relay.sqlite", relay_dir.display()),
            access_key_id: None,
            secret_access_key: None,
            region: None,
            endpoint: None,
        };

        // Machine A stashes
        stash(
            &config,
            machine_a.to_str().unwrap(),
            "updated_at",
            ConflictResolution::LocalWins,
            None,
            false,
            &default_excludes(),
        )
        .await
        .unwrap();

        // Machine B retrieves
        let results = retrieve(
            &config,
            machine_b.to_str().unwrap(),
            "updated_at",
            ConflictResolution::LocalWins,
            None,
            false,
            &default_excludes(),
        )
        .await
        .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].rows_pulled, 2);

        // Machine B should now have all data
        let items = read_items(&machine_b);
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].1, "alpha");
        assert_eq!(items[1].1, "beta");
    }

    #[tokio::test]
    async fn test_retrieve_dry_run() {
        let dir = TempDir::new().unwrap();
        let local_path = dir.path().join("local.sqlite");
        let relay_dir = dir.path().join("relay_store");
        std::fs::create_dir_all(&relay_dir).unwrap();
        let relay_path = relay_dir.join("relay.sqlite");

        create_test_db(&local_path, &[]);
        create_test_db(&relay_path, &[(1, "alpha", "2024-01-01")]);

        let config = StashConfig {
            url: format!("file://{}/relay.sqlite", relay_dir.display()),
            access_key_id: None,
            secret_access_key: None,
            region: None,
            endpoint: None,
        };

        let results = retrieve(
            &config,
            local_path.to_str().unwrap(),
            "updated_at",
            ConflictResolution::LocalWins,
            None,
            true, // dry run
            &default_excludes(),
        )
        .await
        .unwrap();

        assert_eq!(results[0].rows_pulled, 1);

        // Local should still be empty (dry run)
        let items = read_items(&local_path);
        assert_eq!(items.len(), 0);
    }

    #[tokio::test]
    async fn test_stash_updates_existing_rows() {
        let dir = TempDir::new().unwrap();
        let local_path = dir.path().join("local.sqlite");
        let relay_dir = dir.path().join("relay_store");
        std::fs::create_dir_all(&relay_dir).unwrap();
        let relay_path = relay_dir.join("relay.sqlite");

        // Relay has old data
        create_test_db(&relay_path, &[(1, "old_name", "2024-01-01")]);

        // Local has updated data
        create_test_db(&local_path, &[(1, "new_name", "2024-01-02")]);

        let config = StashConfig {
            url: format!("file://{}/relay.sqlite", relay_dir.display()),
            access_key_id: None,
            secret_access_key: None,
            region: None,
            endpoint: None,
        };

        let results = stash(
            &config,
            local_path.to_str().unwrap(),
            "updated_at",
            ConflictResolution::LocalWins,
            None,
            false,
            &default_excludes(),
        )
        .await
        .unwrap();

        assert_eq!(results[0].rows_pushed, 1);

        // Relay should have updated data
        let items = read_items(&relay_path);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].1, "new_name");
        assert_eq!(items[0].2, "2024-01-02");
    }

    #[tokio::test]
    async fn test_retrieve_conflict_resolution_newer_wins() {
        let dir = TempDir::new().unwrap();
        let local_path = dir.path().join("local.sqlite");
        let relay_dir = dir.path().join("relay_store");
        std::fs::create_dir_all(&relay_dir).unwrap();
        let relay_path = relay_dir.join("relay.sqlite");

        // Local has older data
        create_test_db(&local_path, &[(1, "local_version", "2024-01-01")]);

        // Relay has newer data
        create_test_db(&relay_path, &[(1, "relay_version", "2024-01-02")]);

        let config = StashConfig {
            url: format!("file://{}/relay.sqlite", relay_dir.display()),
            access_key_id: None,
            secret_access_key: None,
            region: None,
            endpoint: None,
        };

        // NewerWins: relay is newer, so it should win
        let results = retrieve(
            &config,
            local_path.to_str().unwrap(),
            "updated_at",
            ConflictResolution::NewerWins,
            None,
            false,
            &default_excludes(),
        )
        .await
        .unwrap();

        assert_eq!(results[0].rows_pulled, 1);
        let items = read_items(&local_path);
        assert_eq!(items[0].1, "relay_version");
    }

    #[tokio::test]
    async fn test_retrieve_conflict_resolution_remote_wins() {
        let dir = TempDir::new().unwrap();
        let local_path = dir.path().join("local.sqlite");
        let relay_dir = dir.path().join("relay_store");
        std::fs::create_dir_all(&relay_dir).unwrap();
        let relay_path = relay_dir.join("relay.sqlite");

        // Both have same row, different content, same timestamp
        create_test_db(&local_path, &[(1, "local_version", "2024-01-01")]);
        create_test_db(&relay_path, &[(1, "relay_version", "2024-01-01")]);

        let config = StashConfig {
            url: format!("file://{}/relay.sqlite", relay_dir.display()),
            access_key_id: None,
            secret_access_key: None,
            region: None,
            endpoint: None,
        };

        // RemoteWins in retrieve context: local (dest) wins, relay changes are NOT applied
        let _results = retrieve(
            &config,
            local_path.to_str().unwrap(),
            "updated_at",
            ConflictResolution::RemoteWins,
            None,
            false,
            &default_excludes(),
        )
        .await
        .unwrap();

        // RemoteWins means the destination (local) wins for content_differs
        let items = read_items(&local_path);
        assert_eq!(items[0].1, "local_version");
    }

    #[tokio::test]
    async fn test_upload_relay_first_upload() {
        let dir = TempDir::new().unwrap();
        let relay_dir = dir.path().join("relay_store");
        std::fs::create_dir_all(&relay_dir).unwrap();

        // Create a file to upload
        let source_path = dir.path().join("source.sqlite");
        create_test_db(&source_path, &[(1, "test", "2024-01-01")]);

        let store: Arc<dyn ObjectStore> =
            Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&relay_dir).unwrap());
        let path = ObjectPath::from("relay.sqlite");

        // First upload -- no etag
        upload_relay(&*store, &path, &source_path, None)
            .await
            .unwrap();

        // File should exist
        assert!(relay_dir.join("relay.sqlite").exists());
    }

    #[tokio::test]
    async fn test_upload_relay_with_etag_fallback() {
        let dir = TempDir::new().unwrap();
        let relay_dir = dir.path().join("relay_store");
        std::fs::create_dir_all(&relay_dir).unwrap();

        let source_path = dir.path().join("source.sqlite");
        create_test_db(&source_path, &[(1, "test", "2024-01-01")]);

        let store: Arc<dyn ObjectStore> =
            Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&relay_dir).unwrap());
        let path = ObjectPath::from("relay.sqlite");

        // Upload with an etag -- local filesystem doesn't support conditional writes,
        // so this exercises the NotImplemented fallback path
        upload_relay(&*store, &path, &source_path, Some("fake-etag"))
            .await
            .unwrap();

        assert!(relay_dir.join("relay.sqlite").exists());
    }

    #[tokio::test]
    async fn test_retrieve_source_only_table_skipped() {
        let dir = TempDir::new().unwrap();
        let local_path = dir.path().join("local.sqlite");
        let relay_dir = dir.path().join("relay_store");
        std::fs::create_dir_all(&relay_dir).unwrap();
        let relay_path = relay_dir.join("relay.sqlite");

        // Local has one table
        create_test_db(&local_path, &[(1, "alpha", "2024-01-01")]);

        // Relay has two tables
        let conn = Connection::open(&relay_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, updated_at TEXT);
             CREATE TABLE extra (id INTEGER PRIMARY KEY, value TEXT, updated_at TEXT);
             INSERT INTO items VALUES (1, 'alpha', '2024-01-01');
             INSERT INTO items VALUES (2, 'beta', '2024-01-02');
             INSERT INTO extra VALUES (1, 'data', '2024-01-01');",
        )
        .unwrap();
        drop(conn);

        let config = StashConfig {
            url: format!("file://{}/relay.sqlite", relay_dir.display()),
            access_key_id: None,
            secret_access_key: None,
            region: None,
            endpoint: None,
        };

        // Should only sync "items" (shared table), skip "extra" (relay-only)
        let results = retrieve(
            &config,
            local_path.to_str().unwrap(),
            "updated_at",
            ConflictResolution::LocalWins,
            None,
            false,
            &default_excludes(),
        )
        .await
        .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].table, "items");
        assert_eq!(results[0].rows_pulled, 1); // only "beta" is new
    }

    fn default_excludes() -> Vec<String> {
        vec![
            "sqlite_sequence".to_string(),
            "_cf_KV".to_string(),
            "__drizzle_migrations".to_string(),
        ]
    }
}
