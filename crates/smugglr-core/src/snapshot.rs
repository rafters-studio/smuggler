//! Point-in-time snapshots for disaster recovery.
//!
//! `snapshot` creates a timestamped full copy of the local database in the relay store.
//! `restore` downloads a snapshot and replaces the local database.
//! `list_snapshots` enumerates available snapshots with metadata.

use crate::config::StashConfig;
use crate::datasource::DataSource;
use crate::error::{Result, SyncError};
use crate::local::LocalDb;
use crate::stash::build_store;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use serde::{Deserialize, Serialize};
use std::path::Path;
use tracing::{debug, info, warn};

/// A snapshot's metadata: the sidecar stored alongside each snapshot, and the
/// value returned by `snapshot`, `restore`, and `list_snapshots` (the JSON shape
/// is identical in all three roles).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMeta {
    pub timestamp: String,
    pub size_bytes: u64,
    pub tables: Vec<SnapshotTableMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotTableMeta {
    pub name: String,
    pub row_count: usize,
}

/// Derive the snapshots prefix from the stash config URL.
///
/// If the relay path is `path/to/relay.sqlite`, snapshots go under `path/to/snapshots/`.
fn snapshots_prefix(relay_path: &ObjectPath) -> ObjectPath {
    let relay_str = relay_path.as_ref();
    let parent = match relay_str.rfind('/') {
        Some(idx) => &relay_str[..idx],
        None => "",
    };
    if parent.is_empty() {
        ObjectPath::from("snapshots")
    } else {
        ObjectPath::from(format!("{}/snapshots", parent))
    }
}

/// A short random hex suffix used to make snapshot keys unique within a
/// millisecond. Kept short (8 hex chars) since it only needs to break ties
/// between snapshots taken in the same instant, not be globally unique.
fn snapshot_suffix() -> String {
    use rand::RngCore;
    let mut bytes = [0u8; 4];
    rand::rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}

/// Build the `VACUUM INTO` destination path for a snapshot: alongside the
/// source database, named with the current pid and a random suffix so
/// concurrent snapshots (even from the same process) never collide.
///
/// `VACUUM INTO` refuses to write to a path that already exists, so unlike
/// `tempfile::NamedTempFile` (which pre-creates the file) this only computes
/// a path -- the file is created by SQLite itself.
fn snapshot_temp_path(local_db_path: &str) -> std::path::PathBuf {
    let local_path = Path::new(local_db_path);
    let parent = local_path.parent().unwrap_or(Path::new("."));
    parent.join(format!(
        ".smugglr-snapshot-{}-{}.tmp",
        std::process::id(),
        snapshot_suffix()
    ))
}

/// Render a snapshot timestamp as a filename-safe object-key component.
///
/// Snapshot object keys are used verbatim as filenames by the LocalFileSystem
/// relay backend, and the ISO-8601 timestamp's `HH:MM:SS` colons are illegal in
/// Windows filenames -- a colon starts an NTFS alternate-data-stream, so a `put`
/// silently misbehaves rather than erroring. Replacing `:` with `-` keeps the
/// key filename-safe on every platform while staying deterministic, so `restore`
/// can reconstruct it. The colon-bearing timestamp is preserved untouched inside
/// the `.meta.json` sidecar (`SnapshotMeta.timestamp`) -- what listing and restore
/// sort/match on -- so sort order is unchanged across old and new keys. (#238)
fn key_component(timestamp: &str) -> String {
    timestamp.replace(':', "-")
}

/// Create a point-in-time snapshot of the local database.
pub async fn snapshot(
    config: &StashConfig,
    local_db_path: &str,
    dry_run: bool,
) -> Result<SnapshotMeta> {
    let (store, relay_path) = build_store(config)?;
    let prefix = snapshots_prefix(&relay_path);

    let local = LocalDb::open_readonly(local_db_path)?;
    // Millisecond resolution alone collides when two snapshots fire in the same
    // ms (watch loop, scripted double-fire); a short random suffix disambiguates
    // while keeping the timestamp prefix lexically sortable.
    let timestamp = format!(
        "{}-{}",
        chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ"),
        snapshot_suffix()
    );

    // Gather table metadata
    let all_tables = local.list_tables().await?;
    let mut tables = Vec::new();
    for table in &all_tables {
        let count = local.row_count(table).await?;
        tables.push(SnapshotTableMeta {
            name: table.clone(),
            row_count: count,
        });
    }

    // Materialize the snapshot bytes via `VACUUM INTO`, on the SAME connection
    // that just counted the rows above. A raw `std::fs::read` of the database
    // file (the old approach) only sees the base file -- on a WAL-mode
    // database, committed rows still sitting in `<db>-wal` are silently
    // missing from the uploaded bytes even though `row_count` above (which
    // reads through the connection, and therefore already merges the WAL)
    // counted them. `VACUUM INTO` also reads through the connection, so it
    // captures exactly what `row_count` counted, WAL or not -- and unlike a
    // concurrent raw file read, it can never observe a torn/in-progress page
    // set. See #433.
    //
    // The temp file is removed on every exit from this block, success or
    // error: the `VACUUM INTO` failure path and the read failure path both
    // clean it up before propagating, and the success path removes it right
    // after the bytes are loaded into memory.
    let temp_path = snapshot_temp_path(local_db_path);
    {
        let conn = local.conn();
        conn.execute(
            "VACUUM INTO ?1",
            rusqlite::params![temp_path.to_string_lossy().as_ref()],
        )
        .map_err(|e| {
            let _ = std::fs::remove_file(&temp_path);
            SyncError::Stash(format!("Failed to vacuum snapshot to temp file: {}", e))
        })?;
    }

    let db_bytes = std::fs::read(&temp_path).map_err(|e| {
        let _ = std::fs::remove_file(&temp_path);
        SyncError::Stash(format!("Failed to read snapshot temp file: {}", e))
    })?;
    let _ = std::fs::remove_file(&temp_path);
    let size_bytes = db_bytes.len() as u64;

    if dry_run {
        debug!("Dry run -- skipping upload");
        return Ok(SnapshotMeta {
            timestamp,
            size_bytes,
            tables,
        });
    }

    // Upload snapshot database
    let snap_path = ObjectPath::from(format!("{}/{}.sqlite", prefix, key_component(&timestamp)));
    info!("Uploading snapshot to {}", snap_path);
    store
        .put(&snap_path, PutPayload::from(db_bytes))
        .await
        .map_err(|e| SyncError::Stash(format!("Failed to upload snapshot: {}", e)))?;

    // Upload metadata sidecar
    let meta = SnapshotMeta {
        timestamp,
        size_bytes,
        tables,
    };
    let meta_json = serde_json::to_vec(&meta)?;
    let meta_path = ObjectPath::from(format!(
        "{}/{}.meta.json",
        prefix,
        key_component(&meta.timestamp)
    ));
    store
        .put(&meta_path, PutPayload::from(meta_json))
        .await
        .map_err(|e| SyncError::Stash(format!("Failed to upload snapshot metadata: {}", e)))?;

    info!(
        "Snapshot created: {} ({} bytes)",
        meta.timestamp, meta.size_bytes
    );

    Ok(meta)
}

/// List available snapshots from the relay store.
pub async fn list_snapshots(config: &StashConfig) -> Result<Vec<SnapshotMeta>> {
    let (store, relay_path) = build_store(config)?;
    let prefix = snapshots_prefix(&relay_path);

    let list_result = store
        .list_with_delimiter(Some(&prefix))
        .await
        .map_err(|e| SyncError::Stash(format!("Failed to list snapshots: {}", e)))?;

    let mut entries = Vec::new();

    for obj in &list_result.objects {
        let path_str = obj.location.as_ref();
        if !path_str.ends_with(".meta.json") {
            continue;
        }

        // Fetch the sidecar bytes, folding the get() and bytes() errors into one
        // result so a single classifier decides skip-vs-propagate.
        let bytes_result = match store.get(&obj.location).await {
            Ok(result) => result.bytes().await.map(|b| b.to_vec()),
            Err(e) => Err(e),
        };
        if let Some(meta) = parse_snapshot_meta_entry(path_str, bytes_result)? {
            entries.push(meta);
        }
    }

    entries.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
    Ok(entries)
}

/// Classify one snapshot sidecar fetch into "use it", "skip it", or "fail the
/// whole listing".
///
/// - A missing sidecar (`NotFound`) is skipped: the snapshot blob may exist
///   without its metadata, and that should not abort the list.
/// - Any OTHER fetch error (throttling, 5xx, timeout) is propagated: silently
///   skipping it could drive `restore` to an older snapshot or a false "none
///   found" -- the exact silent-drop #187 exists to prevent.
/// - A malformed sidecar (valid fetch, bad JSON) is skipped but WARNED, so a
///   corrupt metadata file is visible rather than invisible.
fn parse_snapshot_meta_entry(
    path_str: &str,
    bytes_result: std::result::Result<Vec<u8>, object_store::Error>,
) -> Result<Option<SnapshotMeta>> {
    match bytes_result {
        Ok(bytes) => match serde_json::from_slice::<SnapshotMeta>(&bytes) {
            Ok(meta) => Ok(Some(meta)),
            Err(e) => {
                warn!(
                    "Skipping malformed snapshot metadata at {}: {}",
                    path_str, e
                );
                Ok(None)
            }
        },
        Err(object_store::Error::NotFound { .. }) => {
            debug!("Snapshot metadata missing at {}, skipping", path_str);
            Ok(None)
        }
        Err(e) => Err(SyncError::Stash(format!(
            "Failed to read snapshot metadata at {}: {}",
            path_str, e
        ))),
    }
}

/// Restore a snapshot to the local database path.
///
/// Finds the snapshot closest to `target_timestamp` and replaces the local database.
/// If `dry_run`, reports what would be restored without writing.
pub async fn restore(
    config: &StashConfig,
    local_db_path: &str,
    target_timestamp: &str,
    dry_run: bool,
) -> Result<SnapshotMeta> {
    let (store, relay_path) = build_store(config)?;
    let prefix = snapshots_prefix(&relay_path);

    // Find the matching snapshot
    let snapshots = list_snapshots(config).await?;
    if snapshots.is_empty() {
        return Err(SyncError::Stash("No snapshots available".into()));
    }

    // Find closest match: exact match first, then closest before the target
    let entry = snapshots
        .iter()
        .find(|s| s.timestamp == target_timestamp)
        .or_else(|| {
            snapshots
                .iter()
                .filter(|s| s.timestamp.as_str() <= target_timestamp)
                .max_by_key(|s| &s.timestamp)
        })
        .ok_or_else(|| {
            SyncError::Stash(format!(
                "No snapshot found at or before {}. Earliest available: {}",
                target_timestamp,
                snapshots
                    .last()
                    .map(|s| s.timestamp.as_str())
                    .unwrap_or("none")
            ))
        })?;

    let result = entry.clone();

    if dry_run {
        info!(
            "Would restore snapshot {} ({} bytes)",
            entry.timestamp, entry.size_bytes
        );
        return Ok(result);
    }

    // Download the snapshot. New keys are filename-safe (`key_component`); a relay
    // that predates #238 (e.g. S3, where colons are legal) still holds the blob at
    // the raw colon-bearing key, so fall back to it on NotFound.
    let snap_path = ObjectPath::from(format!(
        "{}/{}.sqlite",
        prefix,
        key_component(&entry.timestamp)
    ));
    info!("Downloading snapshot from {}", snap_path);

    let get_result = match store.get(&snap_path).await {
        Ok(r) => r,
        Err(object_store::Error::NotFound { .. }) => {
            let legacy_path = ObjectPath::from(format!("{}/{}.sqlite", prefix, entry.timestamp));
            store.get(&legacy_path).await.map_err(|e| {
                SyncError::Stash(format!(
                    "Failed to download snapshot {}: {}",
                    entry.timestamp, e
                ))
            })?
        }
        Err(e) => {
            return Err(SyncError::Stash(format!(
                "Failed to download snapshot {}: {}",
                entry.timestamp, e
            )))
        }
    };

    let bytes = get_result
        .bytes()
        .await
        .map_err(|e| SyncError::Stash(format!("Failed to read snapshot body: {}", e)))?;

    // Write to local database path (atomic via temp file + rename)
    let local_path = Path::new(local_db_path);
    let parent = local_path.parent().unwrap_or(Path::new("."));
    let temp_path = parent.join(format!(".smugglr-restore-{}.tmp", std::process::id()));

    std::fs::write(&temp_path, &bytes)
        .map_err(|e| SyncError::Stash(format!("Failed to write snapshot temp file: {}", e)))?;

    // Validate the downloaded file is a valid SQLite database.
    // Explicitly drop the connection before rename to release file handles (Windows).
    {
        let conn = rusqlite::Connection::open_with_flags(
            &temp_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .map_err(|e| {
            let _ = std::fs::remove_file(&temp_path);
            SyncError::Stash(format!(
                "Downloaded snapshot is not a valid SQLite database: {}",
                e
            ))
        })?;
        // Walk the b-trees, not just the header: quick_check is far cheaper
        // than integrity_check but still detects truncation and corrupt pages.
        let check: String = conn
            .query_row("PRAGMA quick_check", [], |r| r.get(0))
            .map_err(|e| {
                let _ = std::fs::remove_file(&temp_path);
                SyncError::Stash(format!(
                    "Downloaded snapshot is not a valid SQLite database: {}",
                    e
                ))
            })?;
        if check != "ok" {
            let _ = std::fs::remove_file(&temp_path);
            return Err(SyncError::Stash(format!(
                "Downloaded snapshot failed integrity check: {}",
                check
            )));
        }
    }

    std::fs::rename(&temp_path, local_path).map_err(|e| {
        let _ = std::fs::remove_file(&temp_path);
        SyncError::Stash(format!("Failed to replace local database: {}", e))
    })?;

    info!(
        "Restored snapshot {} ({} bytes) to {}",
        entry.timestamp, entry.size_bytes, local_db_path
    );

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use tempfile::TempDir;

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

    /// List any `.smugglr-snapshot-*.tmp` files left behind in `dir` --
    /// `snapshot_temp_path`'s `VACUUM INTO` destinations. Used to assert the
    /// temp file is actually removed, on both the success and error paths
    /// (#433), rather than trusting the source comment.
    fn leftover_snapshot_temp_files(dir: &Path) -> Vec<String> {
        std::fs::read_dir(dir)
            .map(|entries| {
                entries
                    .filter_map(|e| e.ok())
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .filter(|name| name.starts_with(".smugglr-snapshot-") && name.ends_with(".tmp"))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Create a WAL-mode test database with `row_count` rows in `items`, and
    /// return the still-open connection. Both #433 WAL regression tests need
    /// the writer kept open through the `snapshot()` call so SQLite's
    /// close-time auto-checkpoint cannot quietly merge the WAL and hide the
    /// bug being tested -- returning the live connection (instead of
    /// dropping it here) is what makes that possible.
    fn create_wal_test_db(path: &Path, row_count: i64) -> Connection {
        let conn = Connection::open(path).unwrap();
        let mode: String = conn
            .query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))
            .unwrap();
        assert_eq!(mode.to_lowercase(), "wal", "journal_mode must be WAL");
        conn.execute_batch(
            "CREATE TABLE items (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                updated_at TEXT
            )",
        )
        .unwrap();
        for i in 1..=row_count {
            conn.execute(
                "INSERT INTO items (id, name, updated_at) VALUES (?1, ?2, ?3)",
                rusqlite::params![i, format!("item-{}", i), "2024-01-01"],
            )
            .unwrap();
        }
        conn
    }

    #[tokio::test]
    async fn test_snapshot_creates_files() {
        let dir = TempDir::new().unwrap();
        let local_path = dir.path().join("local.sqlite");
        let snap_dir = dir.path().join("snap_store");
        std::fs::create_dir_all(&snap_dir).unwrap();

        create_test_db(
            &local_path,
            &[(1, "alpha", "2024-01-01"), (2, "beta", "2024-01-02")],
        );

        let config = make_file_stash_config(&snap_dir);

        let result = snapshot(&config, local_path.to_str().unwrap(), false)
            .await
            .unwrap();

        assert!(!result.timestamp.is_empty());
        assert!(result.size_bytes > 0);
        assert_eq!(result.tables.len(), 1);
        assert_eq!(result.tables[0].name, "items");
        assert_eq!(result.tables[0].row_count, 2);

        // Verify files were created in the snapshots/ directory
        let snapshots_dir = snap_dir.join("snapshots");
        assert!(snapshots_dir.exists());
        let entries: Vec<_> = std::fs::read_dir(&snapshots_dir)
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(entries.len(), 2); // .sqlite + .meta.json
    }

    #[tokio::test]
    async fn snapshot_object_keys_are_filename_safe() {
        // Regression for #238: object keys are used verbatim as filenames by the
        // LocalFileSystem relay, so they must contain NO colon -- a colon is
        // illegal in a Windows filename (it opens an NTFS alternate-data-stream,
        // so `put` silently misbehaves rather than erroring, which is exactly why
        // the multi-key snapshot tests were gated off Windows). Assert the on-disk
        // names directly, then confirm the colon-free key still round-trips.
        let dir = TempDir::new().unwrap();
        let local_path = dir.path().join("local.sqlite");
        let snap_dir = dir.path().join("snap_store");
        std::fs::create_dir_all(&snap_dir).unwrap();
        create_test_db(&local_path, &[(1, "alpha", "2024-01-01")]);
        let config = make_file_stash_config(&snap_dir);

        snapshot(&config, local_path.to_str().unwrap(), false)
            .await
            .unwrap();

        let names: Vec<String> = std::fs::read_dir(snap_dir.join("snapshots"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            names.len(),
            2,
            "expected .sqlite + .meta.json, got {:?}",
            names
        );
        for name in &names {
            assert!(
                !name.contains(':'),
                "snapshot key must be filename-safe (no colon), got {}",
                name
            );
        }

        // The colon-free key must still be fetchable by restore.
        let restored = restore(
            &config,
            local_path.to_str().unwrap(),
            "2099-01-01T00:00:00.000Z",
            false,
        )
        .await
        .unwrap();
        assert!(!restored.timestamp.is_empty());
    }

    // A pre-#238 relay (e.g. S3, where colons are legal) holds the blob at the raw
    // colon-bearing key; restore must fall back to it. Gated off Windows because
    // the simulated legacy layout writes a colon-keyed file, which is itself
    // impossible on a Windows filesystem -- exactly why such keys only ever exist
    // on S3-style relays.
    #[cfg(not(target_os = "windows"))]
    #[tokio::test]
    async fn restore_finds_legacy_colon_keyed_snapshot() {
        let dir = TempDir::new().unwrap();
        let local_path = dir.path().join("local.sqlite");
        let snap_dir = dir.path().join("snap_store");
        let snapshots_dir = snap_dir.join("snapshots");
        std::fs::create_dir_all(&snapshots_dir).unwrap();
        create_test_db(&local_path, &[(1, "alpha", "2024-01-01")]);

        // A valid sqlite db to serve as the legacy snapshot blob (id=9 marks it).
        let blob_src = dir.path().join("blob.sqlite");
        create_test_db(&blob_src, &[(9, "restored", "2020-01-01")]);
        let blob = std::fs::read(&blob_src).unwrap();

        // Old on-disk layout: colon-bearing key for both blob and sidecar.
        let legacy_ts = "2020-06-15T12:30:45.000Z-deadbeef";
        std::fs::write(snapshots_dir.join(format!("{}.sqlite", legacy_ts)), &blob).unwrap();
        let meta = SnapshotMeta {
            timestamp: legacy_ts.to_string(),
            size_bytes: blob.len() as u64,
            tables: vec![],
        };
        std::fs::write(
            snapshots_dir.join(format!("{}.meta.json", legacy_ts)),
            serde_json::to_vec(&meta).unwrap(),
        )
        .unwrap();

        let config = make_file_stash_config(&snap_dir);
        let restored = restore(
            &config,
            local_path.to_str().unwrap(),
            "2099-01-01T00:00:00.000Z",
            false,
        )
        .await
        .unwrap();
        assert_eq!(restored.timestamp, legacy_ts);

        // The local db was replaced by the legacy blob (id=9 present).
        let conn = rusqlite::Connection::open(&local_path).unwrap();
        let id: i64 = conn
            .query_row("SELECT id FROM items WHERE id = 9", [], |r| r.get(0))
            .unwrap();
        assert_eq!(id, 9);
    }

    // Regression for #433: a WAL-mode database's committed rows can sit in
    // `<db>-wal` until checkpointed. The old snapshot path read the base file
    // directly with `std::fs::read`, which never sees those rows even though
    // the counting connection (which merges the WAL like any other reader)
    // already counted them. `conn` (the writer) is kept open through the
    // `snapshot()` call so SQLite's close-time auto-checkpoint cannot quietly
    // merge the WAL into the base file and hide the bug being tested.
    #[tokio::test]
    async fn test_snapshot_captures_wal_committed_rows() {
        let dir = TempDir::new().unwrap();
        let local_path = dir.path().join("local.sqlite");
        let snap_dir = dir.path().join("snap_store");
        std::fs::create_dir_all(&snap_dir).unwrap();

        let conn = create_wal_test_db(&local_path, 25);

        // Confirm the WAL sidecar actually holds uncheckpointed bytes --
        // otherwise this test would not be exercising the bug at all.
        let wal_path = dir.path().join("local.sqlite-wal");
        let wal_len = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
        assert!(
            wal_len > 0,
            "expected uncheckpointed WAL content, got {} bytes",
            wal_len
        );

        let pre_snapshot_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM items", [], |r| r.get(0))
            .unwrap();
        assert_eq!(pre_snapshot_count, 25);

        let config = make_file_stash_config(&snap_dir);

        // `conn` (the writer) stays open through this call.
        let snap_result = snapshot(&config, local_path.to_str().unwrap(), false)
            .await
            .unwrap();

        let metadata_count = snap_result
            .tables
            .iter()
            .find(|t| t.name == "items")
            .map(|t| t.row_count)
            .unwrap();
        assert_eq!(
            metadata_count, 25,
            "metadata row_count should see WAL-resident rows"
        );

        // The `VACUUM INTO` temp file must not survive a successful snapshot.
        assert!(
            leftover_snapshot_temp_files(dir.path()).is_empty(),
            "snapshot temp file leaked on the success path"
        );

        drop(conn);

        let restore_path = dir.path().join("restored.sqlite");
        let restore_result = restore(
            &config,
            restore_path.to_str().unwrap(),
            &snap_result.timestamp,
            false,
        )
        .await
        .unwrap();
        assert_eq!(restore_result.timestamp, snap_result.timestamp);

        let restored_conn =
            Connection::open_with_flags(&restore_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .unwrap();
        // `unwrap_or(0)` rather than `unwrap()`: on the pre-fix code the whole
        // `items` table can be missing from the restored file (nothing had
        // been checkpointed to the base file yet, so a raw `std::fs::read`
        // copies an effectively schema-less database), which fails the query
        // itself rather than just undercounting. Folding that into 0 keeps
        // the assertion below reporting an actual observed count instead of
        // panicking inside the query.
        let restored_count: i64 = restored_conn
            .query_row("SELECT COUNT(*) FROM items", [], |r| r.get(0))
            .unwrap_or(0);

        assert_eq!(
            restored_count as usize, metadata_count,
            "restored row count must equal the snapshot metadata's row_count"
        );
        assert_eq!(
            restored_count, pre_snapshot_count,
            "restored row count must equal the pre-snapshot row count"
        );
    }

    // Regression for #433 (restore side): the production recovery flow does
    // NOT restore into a fresh path -- `smugglr restore` always targets
    // `config.local_db_path()`, the same live path the snapshot was taken
    // from. This confirms the fix holds under that exact shape: every
    // WAL-resident row comes back when restoring in place over a corrupted
    // live database.
    //
    // It also pins down and documents a real, deliberate side effect: the
    // `VACUUM INTO` output is always a rollback-journal database (verified
    // directly against SQLite -- `VACUUM INTO` does not carry the source's
    // journal_mode to the new file), so a restore silently drops the
    // operator's WAL setting back to the default. AGENTS.md's "smugglr
    // never touches journal_mode" line is about smugglr's OWN databases,
    // not a promise to preserve an arbitrary operator database's setting
    // across a full-file replace -- and restoring to rollback-journal mode
    // cannot itself hide committed rows the way the original bug did. This
    // is called out in the PR body rather than silently shipped; preserving
    // the operator's journal_mode across restore is not in #433's
    // acceptance criteria and is left for a follow-up if wanted.
    #[tokio::test]
    async fn test_snapshot_then_restore_in_place_wal_mode() {
        let dir = TempDir::new().unwrap();
        let local_path = dir.path().join("local.sqlite");
        let snap_dir = dir.path().join("snap_store");
        std::fs::create_dir_all(&snap_dir).unwrap();

        let conn = create_wal_test_db(&local_path, 25);

        let config = make_file_stash_config(&snap_dir);
        let snap_result = snapshot(&config, local_path.to_str().unwrap(), false)
            .await
            .unwrap();

        // Release the writer (and its WAL/SHM handles) before simulating a
        // bad migration and restoring over the same path.
        drop(conn);

        let bad_conn = Connection::open(&local_path).unwrap();
        bad_conn.execute_batch("DELETE FROM items;").unwrap();
        drop(bad_conn);

        // Restore IN PLACE -- the real recovery flow.
        let restore_result = restore(
            &config,
            local_path.to_str().unwrap(),
            &snap_result.timestamp,
            false,
        )
        .await
        .unwrap();
        assert_eq!(restore_result.timestamp, snap_result.timestamp);

        let restored_conn =
            Connection::open_with_flags(&local_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .unwrap();
        let restored_count: i64 = restored_conn
            .query_row("SELECT COUNT(*) FROM items", [], |r| r.get(0))
            .unwrap_or(0);
        assert_eq!(
            restored_count, 25,
            "in-place restore must return every WAL-resident row"
        );

        // Documented side effect (see comment above): the restored file is
        // rollback-journal, not WAL, regardless of the operator's prior
        // setting.
        let restored_mode: String = restored_conn
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            restored_mode.to_lowercase(),
            "delete",
            "VACUUM INTO output is rollback-journal; restore does not carry WAL forward"
        );
    }

    // Regression for #433: the temp file must be removed on the `VACUUM
    // INTO` error path, not just on success. Makes the source database's
    // directory unwritable so `VACUUM INTO` cannot create its destination
    // file there, then asserts the failure is surfaced as `SyncError::Stash`
    // and no `.smugglr-snapshot-*.tmp` file is left behind. Gated off
    // Windows (chmod-based write restriction is not portable there) and
    // self-skips if this sandbox does not actually enforce the permission
    // (e.g. running as root), rather than asserting a false negative.
    #[cfg(not(target_os = "windows"))]
    #[tokio::test]
    async fn test_snapshot_vacuum_failure_leaves_no_temp_file() {
        use std::os::unix::fs::PermissionsExt;

        let dir = TempDir::new().unwrap();
        let local_path = dir.path().join("local.sqlite");
        let snap_dir = dir.path().join("snap_store");
        std::fs::create_dir_all(&snap_dir).unwrap();

        create_test_db(&local_path, &[(1, "alpha", "2024-01-01")]);

        // `snap_dir` already exists (created above) and keeps its own
        // permissions when the parent is locked down, so the relay upload
        // path stays writable -- only `dir.path()` itself (where
        // `snapshot_temp_path` places the `VACUUM INTO` destination,
        // alongside `local_path`) becomes read-only.
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o555)).unwrap();

        let probe_path = dir.path().join(".write-probe");
        let probe_result = std::fs::write(&probe_path, b"probe");
        let permissions_enforced = probe_result.is_err();
        if probe_result.is_ok() {
            let _ = std::fs::remove_file(&probe_path);
        }

        let config = make_file_stash_config(&snap_dir);
        let result = snapshot(&config, local_path.to_str().unwrap(), false).await;

        // Always restore write permission so `TempDir` can clean up on drop.
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();

        if !permissions_enforced {
            // This sandbox does not enforce directory write permissions
            // (e.g. running as root) -- cannot exercise the failure here.
            return;
        }

        assert!(
            result.is_err(),
            "VACUUM INTO into an unwritable directory must fail"
        );
        assert!(matches!(result.unwrap_err(), SyncError::Stash(_)));
        assert!(
            leftover_snapshot_temp_files(dir.path()).is_empty(),
            "no snapshot temp file should remain after a VACUUM INTO failure"
        );
    }

    #[tokio::test]
    async fn test_snapshot_dry_run() {
        let dir = TempDir::new().unwrap();
        let local_path = dir.path().join("local.sqlite");
        let snap_dir = dir.path().join("snap_store");
        std::fs::create_dir_all(&snap_dir).unwrap();

        create_test_db(&local_path, &[(1, "alpha", "2024-01-01")]);

        let config = make_file_stash_config(&snap_dir);

        let result = snapshot(&config, local_path.to_str().unwrap(), true)
            .await
            .unwrap();

        assert!(!result.timestamp.is_empty());
        assert!(result.size_bytes > 0);

        // No files should have been created
        let snapshots_dir = snap_dir.join("snapshots");
        assert!(!snapshots_dir.exists());

        // Dry run still runs `VACUUM INTO` to gather `size_bytes` -- its temp
        // file must not survive either (#433).
        assert!(
            leftover_snapshot_temp_files(dir.path()).is_empty(),
            "snapshot temp file leaked on the dry-run path"
        );
    }

    #[tokio::test]
    async fn test_list_snapshots_empty() {
        let dir = TempDir::new().unwrap();
        let snap_dir = dir.path().join("snap_store");
        std::fs::create_dir_all(&snap_dir).unwrap();

        let config = make_file_stash_config(&snap_dir);

        let list = list_snapshots(&config).await.unwrap();
        assert!(list.is_empty());
    }

    #[tokio::test]
    async fn test_snapshot_then_list() {
        let dir = TempDir::new().unwrap();
        let local_path = dir.path().join("local.sqlite");
        let snap_dir = dir.path().join("snap_store");
        std::fs::create_dir_all(&snap_dir).unwrap();

        create_test_db(&local_path, &[(1, "alpha", "2024-01-01")]);

        let config = make_file_stash_config(&snap_dir);

        let snap_result = snapshot(&config, local_path.to_str().unwrap(), false)
            .await
            .unwrap();

        let list = list_snapshots(&config).await.unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].timestamp, snap_result.timestamp);
        assert_eq!(list[0].size_bytes, snap_result.size_bytes);
    }

    #[tokio::test]
    async fn test_snapshot_then_restore() {
        let dir = TempDir::new().unwrap();
        let local_path = dir.path().join("local.sqlite");
        let snap_dir = dir.path().join("snap_store");
        std::fs::create_dir_all(&snap_dir).unwrap();

        create_test_db(
            &local_path,
            &[(1, "alpha", "2024-01-01"), (2, "beta", "2024-01-02")],
        );

        let config = make_file_stash_config(&snap_dir);

        // Take snapshot
        let snap_result = snapshot(&config, local_path.to_str().unwrap(), false)
            .await
            .unwrap();

        // Modify local database (simulate bad migration)
        let conn = Connection::open(&local_path).unwrap();
        conn.execute_batch(
            "DELETE FROM items; INSERT INTO items VALUES (99, 'corrupt', '2024-06-01');",
        )
        .unwrap();
        drop(conn);

        // Restore
        let restore_result = restore(
            &config,
            local_path.to_str().unwrap(),
            &snap_result.timestamp,
            false,
        )
        .await
        .unwrap();

        assert_eq!(restore_result.timestamp, snap_result.timestamp);

        // Verify local has original data
        let conn =
            Connection::open_with_flags(&local_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM items", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 2);

        let name: String = conn
            .query_row("SELECT name FROM items WHERE id = 1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(name, "alpha");
    }

    #[tokio::test]
    async fn test_restore_dry_run() {
        let dir = TempDir::new().unwrap();
        let local_path = dir.path().join("local.sqlite");
        let snap_dir = dir.path().join("snap_store");
        std::fs::create_dir_all(&snap_dir).unwrap();

        create_test_db(&local_path, &[(1, "alpha", "2024-01-01")]);

        let config = make_file_stash_config(&snap_dir);

        let snap_result = snapshot(&config, local_path.to_str().unwrap(), false)
            .await
            .unwrap();

        // Corrupt local
        let conn = Connection::open(&local_path).unwrap();
        conn.execute_batch("DELETE FROM items").unwrap();
        drop(conn);

        // Dry run restore
        let result = restore(
            &config,
            local_path.to_str().unwrap(),
            &snap_result.timestamp,
            true,
        )
        .await
        .unwrap();

        assert_eq!(result.timestamp, snap_result.timestamp);

        // Local should still be empty (dry run)
        let conn =
            Connection::open_with_flags(&local_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM items", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_restore_no_snapshots() {
        let dir = TempDir::new().unwrap();
        let local_path = dir.path().join("local.sqlite");
        let snap_dir = dir.path().join("snap_store");
        std::fs::create_dir_all(&snap_dir).unwrap();

        create_test_db(&local_path, &[]);

        let config = make_file_stash_config(&snap_dir);

        let result = restore(
            &config,
            local_path.to_str().unwrap(),
            "2024-01-01T00:00:00Z",
            false,
        )
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, SyncError::Stash(_)));
    }

    #[tokio::test]
    async fn test_restore_closest_timestamp() {
        let dir = TempDir::new().unwrap();
        let local_path = dir.path().join("local.sqlite");
        let snap_dir = dir.path().join("snap_store");
        std::fs::create_dir_all(&snap_dir).unwrap();

        create_test_db(&local_path, &[(1, "v1", "2024-01-01")]);
        let config = make_file_stash_config(&snap_dir);

        // Take first snapshot
        let snap1 = snapshot(&config, local_path.to_str().unwrap(), false)
            .await
            .unwrap();

        // Modify and take second snapshot
        let conn = Connection::open(&local_path).unwrap();
        conn.execute(
            "UPDATE items SET name = 'v2', updated_at = '2024-02-01' WHERE id = 1",
            [],
        )
        .unwrap();
        drop(conn);

        let snap2 = snapshot(&config, local_path.to_str().unwrap(), false)
            .await
            .unwrap();

        // Restore to first snapshot by exact timestamp
        let result = restore(
            &config,
            local_path.to_str().unwrap(),
            &snap1.timestamp,
            false,
        )
        .await
        .unwrap();

        assert_eq!(result.timestamp, snap1.timestamp);

        {
            let conn = Connection::open_with_flags(
                &local_path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )
            .unwrap();
            let name: String = conn
                .query_row("SELECT name FROM items WHERE id = 1", [], |r| r.get(0))
                .unwrap();
            assert_eq!(name, "v1");
        }

        // Restore to second snapshot
        let result = restore(
            &config,
            local_path.to_str().unwrap(),
            &snap2.timestamp,
            false,
        )
        .await
        .unwrap();

        assert_eq!(result.timestamp, snap2.timestamp);
    }

    #[test]
    fn test_snapshots_prefix() {
        let path = ObjectPath::from("path/to/relay.sqlite");
        let prefix = snapshots_prefix(&path);
        assert_eq!(prefix.as_ref(), "path/to/snapshots");

        let path = ObjectPath::from("relay.sqlite");
        let prefix = snapshots_prefix(&path);
        assert_eq!(prefix.as_ref(), "snapshots");
    }

    // Regression for #186: restore must reject a structurally-broken database
    // (corrupt b-tree pages) that passes a bare header/`SELECT 1` check but
    // fails PRAGMA quick_check, rather than renaming it over the live db.
    #[tokio::test]
    async fn test_restore_rejects_corrupt_snapshot() {
        let dir = TempDir::new().unwrap();
        let local_path = dir.path().join("local.sqlite");
        let snap_dir = dir.path().join("snap_store");
        std::fs::create_dir_all(&snap_dir).unwrap();

        create_test_db(&local_path, &[(1, "good", "2024-01-01")]);
        let config = make_file_stash_config(&snap_dir);

        // Take a real snapshot so list/select succeed, then overwrite the
        // uploaded .sqlite payload with a corrupt-but-header-valid file:
        // copy a valid header from a real db, then truncate/zero the body so
        // the b-tree pages are torn. `SELECT 1` passes; quick_check must not.
        let snap = snapshot(&config, local_path.to_str().unwrap(), false)
            .await
            .unwrap();

        let snap_obj = snap_dir
            .join("snapshots")
            .join(format!("{}.sqlite", key_component(&snap.timestamp)));
        let mut good = std::fs::read(&snap_obj).unwrap();
        // Keep the first SQLite page header (100 bytes) intact, corrupt the rest.
        assert!(good.len() > 200, "snapshot should be larger than a header");
        for b in good.iter_mut().skip(100) {
            *b = 0xFF;
        }
        std::fs::write(&snap_obj, &good).unwrap();

        let result = restore(
            &config,
            local_path.to_str().unwrap(),
            &snap.timestamp,
            false,
        )
        .await;

        assert!(result.is_err(), "corrupt snapshot must not restore");
        assert!(matches!(result.unwrap_err(), SyncError::Stash(_)));

        // The live database must be untouched (temp file removed, no rename).
        let conn =
            Connection::open_with_flags(&local_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM items", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1, "live db preserved when snapshot is corrupt");
    }

    // Regression for #188: two snapshots taken back-to-back (potentially within
    // the same millisecond) must produce distinct object keys so neither
    // clobbers the other.
    #[tokio::test]
    async fn test_consecutive_snapshots_have_unique_keys() {
        let dir = TempDir::new().unwrap();
        let local_path = dir.path().join("local.sqlite");
        let snap_dir = dir.path().join("snap_store");
        std::fs::create_dir_all(&snap_dir).unwrap();

        create_test_db(&local_path, &[(1, "alpha", "2024-01-01")]);
        let config = make_file_stash_config(&snap_dir);

        let a = snapshot(&config, local_path.to_str().unwrap(), false)
            .await
            .unwrap();
        let b = snapshot(&config, local_path.to_str().unwrap(), false)
            .await
            .unwrap();

        assert_ne!(
            a.timestamp, b.timestamp,
            "snapshot keys must be unique even within the same millisecond"
        );

        // Both snapshots' files must coexist (4 objects: 2x .sqlite + 2x .meta.json).
        let list = list_snapshots(&config).await.unwrap();
        assert_eq!(list.len(), 2, "both snapshots survive without overwrite");
    }

    // Regression for #187: a malformed (parse-failing) sidecar is skipped, but
    // that is the *only* swallow path -- a non-NotFound read error now
    // propagates. Here we assert the skip path still works so a single bad
    // sidecar does not abort the whole list.
    #[tokio::test]
    async fn test_list_snapshots_skips_malformed_sidecar() {
        let dir = TempDir::new().unwrap();
        let local_path = dir.path().join("local.sqlite");
        let snap_dir = dir.path().join("snap_store");
        std::fs::create_dir_all(&snap_dir).unwrap();

        create_test_db(&local_path, &[(1, "alpha", "2024-01-01")]);
        let config = make_file_stash_config(&snap_dir);

        let good = snapshot(&config, local_path.to_str().unwrap(), false)
            .await
            .unwrap();

        // Drop a malformed sidecar alongside the good one.
        let snaps = snap_dir.join("snapshots");
        std::fs::write(snaps.join("9999-garbage.meta.json"), b"not json").unwrap();

        let list = list_snapshots(&config).await.unwrap();
        assert_eq!(list.len(), 1, "malformed sidecar skipped, good one kept");
        assert_eq!(list[0].timestamp, good.timestamp);
    }

    fn sample_meta_bytes() -> Vec<u8> {
        serde_json::to_vec(&SnapshotMeta {
            timestamp: "2026-01-01T00:00:00.000Z-abcd1234".into(),
            size_bytes: 10,
            tables: vec![SnapshotTableMeta {
                name: "items".into(),
                row_count: 1,
            }],
        })
        .unwrap()
    }

    #[test]
    fn meta_entry_good_json_is_parsed() {
        let got = parse_snapshot_meta_entry("p.meta.json", Ok(sample_meta_bytes())).unwrap();
        assert_eq!(got.unwrap().timestamp, "2026-01-01T00:00:00.000Z-abcd1234");
    }

    #[test]
    fn meta_entry_malformed_json_is_skipped() {
        let got = parse_snapshot_meta_entry("p.meta.json", Ok(b"not json".to_vec())).unwrap();
        assert!(got.is_none());
    }

    #[test]
    fn meta_entry_not_found_is_skipped() {
        let err = object_store::Error::NotFound {
            path: "p.meta.json".into(),
            source: "missing".into(),
        };
        let got = parse_snapshot_meta_entry("p.meta.json", Err(err)).unwrap();
        assert!(got.is_none());
    }

    #[test]
    fn meta_entry_transient_error_propagates() {
        // THE #187 guard: a non-NotFound fetch error must fail the listing, not
        // silently drop the snapshot. Fails on the pre-fix code, which debug-logged
        // and continued.
        let err = object_store::Error::Generic {
            store: "test",
            source: "503 slow down".into(),
        };
        let got = parse_snapshot_meta_entry("p.meta.json", Err(err));
        assert!(matches!(got, Err(SyncError::Stash(_))));
    }
}
