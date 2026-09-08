//! Daemon utilities shared by watch and broadcast daemon loops.
//!
//! Contains PID locking, timestamp formatting, last_sync persistence,
//! and transient error classification.

use crate::error::{Result, SyncError};
use std::fs;
use std::path::{Path, PathBuf};
use tracing::{info, warn};

/// PID lock to prevent multiple daemon instances.
#[derive(Debug)]
pub struct PidLock {
    path: PathBuf,
}

impl PidLock {
    /// Acquire a PID lock. Returns an error if another instance is running.
    pub fn acquire(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        if path.exists() {
            let contents = fs::read_to_string(&path)?;
            if let Ok(pid) = contents.trim().parse::<u32>() {
                if is_process_running(pid) {
                    return Err(SyncError::Config(format!(
                        "Another smuggler watch instance is running (PID {}). \
                         If this is stale, remove {}",
                        pid,
                        path.display()
                    )));
                }
                warn!("Removing stale PID file (PID {} is not running)", pid);
            }
        }

        let pid = std::process::id();
        fs::write(&path, pid.to_string())?;
        info!("Acquired PID lock: {} (PID {})", path.display(), pid);

        Ok(Self { path })
    }

    /// Release the PID lock.
    pub fn release(&self) {
        if self.path.exists() {
            if let Err(e) = fs::remove_file(&self.path) {
                warn!("Failed to remove PID file: {}", e);
            } else {
                info!("Released PID lock: {}", self.path.display());
            }
        }
    }
}

impl Drop for PidLock {
    fn drop(&mut self) {
        self.release();
    }
}

/// Check if a process with the given PID is running.
pub fn is_process_running(pid: u32) -> bool {
    // On Unix, kill(pid, 0) checks existence without sending a signal
    #[cfg(unix)]
    {
        unsafe { libc::kill(pid as i32, 0) == 0 }
    }
    // On Windows, use the command-based check
    #[cfg(windows)]
    {
        std::process::Command::new("tasklist")
            .args(["/FI", &format!("PID eq {}", pid), "/NH"])
            .output()
            .map(|o| {
                let stdout = String::from_utf8_lossy(&o.stdout);
                stdout.contains(&pid.to_string())
            })
            .unwrap_or(false)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = pid;
        false
    }
}

/// Resolve the PID lock file path.
///
/// Uses `~/.smugglr/smuggler.pid` or a path next to the config file.
pub fn pid_lock_path(config_path: &Path) -> PathBuf {
    if let Some(parent) = config_path.parent() {
        if parent.as_os_str().is_empty() {
            PathBuf::from(".smugglr.pid")
        } else {
            parent.join(".smugglr.pid")
        }
    } else {
        PathBuf::from(".smugglr.pid")
    }
}

/// Update the `last_sync` timestamp in the config file.
///
/// Reads the file, updates or inserts the `last_sync` field under `[sync]`,
/// and writes it back. Uses string manipulation to preserve comments and
/// formatting in the user's TOML.
pub fn update_last_sync(config_path: &Path, timestamp: &str) -> Result<()> {
    let content = fs::read_to_string(config_path)?;
    let new_content = set_last_sync_in_toml(&content, timestamp);
    fs::write(config_path, new_content)?;
    info!("Updated last_sync to {}", timestamp);
    Ok(())
}

/// Insert or update `last_sync` in a TOML string.
pub fn set_last_sync_in_toml(content: &str, timestamp: &str) -> String {
    let last_sync_line = format!("last_sync = \"{}\"", timestamp);
    let mut lines: Vec<String> = content.lines().map(String::from).collect();

    // Look for existing last_sync line
    for line in &mut lines {
        let trimmed = line.trim();
        if trimmed.starts_with("last_sync") && trimmed.contains('=') {
            *line = last_sync_line;
            return lines.join("\n") + "\n";
        }
    }

    // No existing last_sync -- add it under [sync] section
    let mut found_sync_section = false;
    let mut insert_idx = None;

    for (i, line) in lines.iter().enumerate() {
        let trimmed = line.trim();
        if trimmed == "[sync]" {
            found_sync_section = true;
            continue;
        }
        if found_sync_section {
            // Insert after last key in [sync] section, or right after [sync]
            if trimmed.starts_with('[') || trimmed.is_empty() {
                insert_idx = Some(i);
                break;
            }
        }
    }

    if let Some(idx) = insert_idx {
        lines.insert(idx, last_sync_line);
    } else if found_sync_section {
        // [sync] was the last section, append to end
        lines.push(last_sync_line);
    } else {
        // No [sync] section at all, add one
        lines.push(String::new());
        lines.push("[sync]".to_string());
        lines.push(last_sync_line);
    }

    lines.join("\n") + "\n"
}

/// Get the current UTC timestamp in ISO 8601 format.
pub fn now_iso8601() -> String {
    use std::time::SystemTime;
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    // Format as ISO 8601 without external dep
    let secs = now.as_secs();
    let days = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    // Simple epoch-to-date conversion
    let (year, month, day) = epoch_days_to_date(days);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        year, month, day, hours, minutes, seconds
    )
}

/// Convert days since Unix epoch to (year, month, day).
pub fn epoch_days_to_date(days: u64) -> (u64, u64, u64) {
    // Algorithm from http://howardhinnant.github.io/date_algorithms.html
    let z = days + 719468;
    let era = z / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

/// Check if an error is transient (should retry on next tick) vs fatal (should exit).
///
/// This delegates to [`SyncError::is_retryable`], the canonical retry policy also
/// used by the native one-shot retry loop (see `sync::upsert_with_retry`), so the
/// same error is never "retry" in the daemon and "fatal" in the CLI (issue #226).
/// Delegating narrows two cases that this function previously matched more
/// broadly on its own: `SyncError::Http(_)` is now transient only for the
/// timeout/connect subset `is_retryable` recognizes (a decode, builder, or
/// other non-transport HTTP failure is now fatal instead of looping forever),
/// and `SyncError::ServerError { status, .. }` is transient only for `status
/// >= 500`, matching the variant's documented "5xx" contract instead of
/// treating every `ServerError` as transient regardless of status.
///
/// One documented daemon-specific delta: `ConcurrentWrite` is additionally treated
/// as transient here, even though `is_retryable` reports it as non-retryable. In
/// the native path a concurrent-write conflict is surfaced directly to the
/// operator, who must manually re-run the stash command to pull the latest relay
/// and merge -- there's no automatic "next attempt" within a single invocation.
/// The daemon, by contrast, already has a wait-and-try-again tick loop: skipping to
/// the next tick after a backoff will re-download the latest relay and can resolve
/// the conflict without operator intervention, so retrying on `ConcurrentWrite` is
/// the right call for this loop specifically, and only this loop.
pub fn is_transient_error(err: &SyncError) -> bool {
    err.is_retryable() || matches!(err, SyncError::ConcurrentWrite)
}

/// Compile-time guard: an exhaustive, non-wildcard match over every
/// `SyncError` variant. This function is never called -- its only purpose is
/// that adding a new `SyncError` variant makes it fail to compile, forcing
/// whoever adds the variant to also add a case to
/// `test_retry_verdict_enumerates_every_variant` below rather than letting it
/// silently fall on the wrong side of the retry policy (issue #226).
#[allow(dead_code, clippy::match_same_arms)]
fn _assert_every_variant_is_enumerated_in_retry_verdict_test(err: &SyncError) {
    match err {
        SyncError::Config(_) => {}
        SyncError::ConfigEnvVar(_) => {}
        SyncError::LocalDb(_) => {}
        SyncError::Remote(_) => {}
        SyncError::Http(_) => {}
        SyncError::Json(_) => {}
        SyncError::Io(_) => {}
        SyncError::TableNotFound(_) => {}
        SyncError::NoPrimaryKey(_) => {}
        SyncError::D1Api { .. } => {}
        SyncError::ConfigNotFound(_) => {}
        SyncError::RateLimited { .. } => {}
        SyncError::ServerError { .. } => {}
        SyncError::ConnectionTimeout => {}
        SyncError::BadRequest { .. } => {}
        SyncError::RetryExhausted { .. } => {}
        SyncError::InvalidTableName { .. } => {}
        SyncError::ObjectStore(_) => {}
        SyncError::Stash(_) => {}
        SyncError::InvalidUrl(_) => {}
        SyncError::RelayNotFound(_) => {}
        SyncError::ConcurrentWrite => {}
        SyncError::ParamLimitExceeded { .. } => {}
        SyncError::Broadcast(_) => {}
        SyncError::Plugin(_) => {}
        SyncError::Migrate(_) => {}
        SyncError::LedgerTampered(_) => {}
        SyncError::DuplicatePrimaryKey { .. } => {}
        SyncError::PluginConflict { .. } => {}
    }
    // No `_` arm above: a new SyncError variant must be added to the match
    // (and to the enumeration test) or this file fails to compile.
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_pid_lock_acquire_release() {
        let dir = tempfile::tempdir().unwrap();
        let pid_path = dir.path().join("test.pid");

        let lock = PidLock::acquire(&pid_path).unwrap();
        assert!(pid_path.exists());

        let contents = fs::read_to_string(&pid_path).unwrap();
        assert_eq!(contents, std::process::id().to_string());

        lock.release();
        assert!(!pid_path.exists());
    }

    #[test]
    fn test_pid_lock_stale_removed() {
        let dir = tempfile::tempdir().unwrap();
        let pid_path = dir.path().join("test.pid");

        // Write a PID that definitely doesn't exist
        fs::write(&pid_path, "99999999").unwrap();

        // Should succeed by removing stale lock
        let lock = PidLock::acquire(&pid_path).unwrap();
        let contents = fs::read_to_string(&pid_path).unwrap();
        assert_eq!(contents, std::process::id().to_string());
        lock.release();
    }

    #[test]
    fn test_pid_lock_self_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let pid_path = dir.path().join("test.pid");

        // Write our own PID -- should detect as running
        fs::write(&pid_path, std::process::id().to_string()).unwrap();

        let result = PidLock::acquire(&pid_path);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Another smuggler watch instance"));
    }

    #[test]
    fn test_pid_lock_drop_releases() {
        let dir = tempfile::tempdir().unwrap();
        let pid_path = dir.path().join("test.pid");

        {
            let _lock = PidLock::acquire(&pid_path).unwrap();
            assert!(pid_path.exists());
        }
        // Drop should have released
        assert!(!pid_path.exists());
    }

    #[test]
    fn test_set_last_sync_insert_under_sync() {
        let input = r#"local_db = "game.db"

[sync]
tables = ["abilities"]

[target]
type = "sqlite"
database = "backup.db"
"#;
        let result = set_last_sync_in_toml(input, "2026-03-31T12:00:00Z");
        assert!(result.contains("last_sync = \"2026-03-31T12:00:00Z\""));
        // Should be under [sync], before [target]
        let sync_pos = result.find("[sync]").unwrap();
        let last_sync_pos = result.find("last_sync").unwrap();
        let target_pos = result.find("[target]").unwrap();
        assert!(last_sync_pos > sync_pos);
        assert!(last_sync_pos < target_pos);
    }

    #[test]
    fn test_set_last_sync_update_existing() {
        let input = r#"local_db = "game.db"

[sync]
last_sync = "2026-03-30T00:00:00Z"
tables = ["abilities"]
"#;
        let result = set_last_sync_in_toml(input, "2026-03-31T12:00:00Z");
        assert!(result.contains("last_sync = \"2026-03-31T12:00:00Z\""));
        // Should not have duplicate
        assert_eq!(result.matches("last_sync").count(), 1);
    }

    #[test]
    fn test_set_last_sync_no_sync_section() {
        let input = r#"local_db = "game.db"

[target]
type = "sqlite"
database = "backup.db"
"#;
        let result = set_last_sync_in_toml(input, "2026-03-31T12:00:00Z");
        assert!(result.contains("[sync]"));
        assert!(result.contains("last_sync = \"2026-03-31T12:00:00Z\""));
    }

    #[test]
    fn test_update_last_sync_file() {
        let mut f = NamedTempFile::new().unwrap();
        writeln!(
            f,
            r#"local_db = "game.db"

[sync]
tables = ["abilities"]

[target]
type = "sqlite"
database = "backup.db""#
        )
        .unwrap();

        update_last_sync(f.path(), "2026-03-31T15:30:00Z").unwrap();

        let content = fs::read_to_string(f.path()).unwrap();
        assert!(content.contains("last_sync = \"2026-03-31T15:30:00Z\""));
    }

    #[test]
    fn test_now_iso8601_format() {
        let ts = now_iso8601();
        // Should match YYYY-MM-DDTHH:MM:SSZ pattern
        assert_eq!(ts.len(), 20);
        assert!(ts.ends_with('Z'));
        assert_eq!(&ts[4..5], "-");
        assert_eq!(&ts[7..8], "-");
        assert_eq!(&ts[10..11], "T");
        assert_eq!(&ts[13..14], ":");
        assert_eq!(&ts[16..17], ":");
    }

    #[test]
    fn test_epoch_days_to_date_known() {
        // 2026-03-31 = day 20543 since epoch (verified)
        let (y, m, d) = epoch_days_to_date(0);
        assert_eq!((y, m, d), (1970, 1, 1));

        // 2000-01-01 = day 10957
        let (y, m, d) = epoch_days_to_date(10957);
        assert_eq!((y, m, d), (2000, 1, 1));
    }

    #[test]
    fn test_pid_lock_path_with_config() {
        let p = pid_lock_path(Path::new("/home/user/project/config.toml"));
        assert_eq!(p, PathBuf::from("/home/user/project/.smugglr.pid"));
    }

    #[test]
    fn test_pid_lock_path_bare_filename() {
        let p = pid_lock_path(Path::new("config.toml"));
        assert_eq!(p, PathBuf::from(".smugglr.pid"));
    }

    #[test]
    fn test_is_transient_error() {
        assert!(is_transient_error(&SyncError::RateLimited {
            retry_after: None
        }));
        assert!(is_transient_error(&SyncError::ServerError {
            status: 503,
            message: "down".into()
        }));
        assert!(is_transient_error(&SyncError::ConnectionTimeout));
        assert!(is_transient_error(&SyncError::ConcurrentWrite));

        assert!(!is_transient_error(&SyncError::Config("bad".into())));
        assert!(!is_transient_error(&SyncError::TableNotFound("x".into())));
    }

    /// Build a `reqwest::Error` that is neither `is_timeout()` nor `is_connect()`.
    ///
    /// Requesting an unsupported URL scheme fails at request-build time inside
    /// `send()`, before any socket is touched, so this is deterministic and
    /// works fully offline -- it never performs network I/O.
    async fn non_network_http_error() -> reqwest::Error {
        let err = reqwest::Client::new()
            .get("not-a-real-scheme://host")
            .send()
            .await
            .expect_err("unsupported scheme must fail request construction");
        assert!(
            !err.is_timeout() && !err.is_connect(),
            "test fixture must produce a non-timeout, non-connect HTTP error"
        );
        err
    }

    /// Regression guard for #226.
    ///
    /// Before the fix, `is_transient_error` matched `SyncError::Http(_)`
    /// unconditionally, while `is_retryable` only treated timeout/connect
    /// `reqwest::Error`s as retryable. That meant a non-network HTTP failure
    /// (bad scheme, decode error, builder error, etc.) was "retry forever" in
    /// the daemon but "fatal" in the native retry loop. Confirmed this test
    /// fails on the pre-change `is_transient_error` (see PR description for
    /// the observed failure).
    #[tokio::test]
    async fn test_is_transient_error_matches_is_retryable_for_non_network_http_error() {
        let err = SyncError::Http(non_network_http_error().await);

        assert!(
            !err.is_retryable(),
            "fixture must be non-retryable per is_retryable's timeout/connect-only policy"
        );
        assert_eq!(
            is_transient_error(&err),
            err.is_retryable(),
            "is_transient_error must delegate to is_retryable for Http errors"
        );
    }

    /// Enumerates every `SyncError` variant and pins both classifiers' verdicts,
    /// so a newly added variant -- or a change to either function -- cannot
    /// silently fall on the wrong side of the retry policy (issue #226).
    ///
    /// `is_transient_error` is defined as `is_retryable() || ConcurrentWrite`,
    /// so every variant here must agree between the two functions except
    /// `ConcurrentWrite`, which is the one documented daemon-specific delta.
    #[tokio::test]
    async fn test_retry_verdict_enumerates_every_variant() {
        let db_err = {
            let conn = rusqlite::Connection::open_in_memory().unwrap();
            conn.execute("SELECT * FROM no_such_table_226", [])
                .unwrap_err()
        };
        let store_err = object_store::parse_url(&url::Url::parse("bogus-scheme://bucket").unwrap())
            .expect_err("unsupported object store scheme must fail to parse");
        let http_err = non_network_http_error().await;

        // (name, error, expected is_retryable() verdict)
        let cases: Vec<(&str, SyncError, bool)> = vec![
            ("Config", SyncError::Config("x".into()), false),
            ("ConfigEnvVar", SyncError::ConfigEnvVar("x".into()), false),
            ("LocalDb", SyncError::LocalDb(db_err), false),
            ("Remote", SyncError::Remote("x".into()), false),
            ("Http (non-network)", SyncError::Http(http_err), false),
            (
                "Json",
                SyncError::Json(serde_json::from_str::<i32>("not json").unwrap_err()),
                false,
            ),
            ("Io", SyncError::Io(std::io::Error::other("x")), false),
            ("TableNotFound", SyncError::TableNotFound("t".into()), false),
            ("NoPrimaryKey", SyncError::NoPrimaryKey("t".into()), false),
            (
                "D1Api",
                SyncError::D1Api {
                    message: "x".into(),
                    code: Some(1),
                },
                false,
            ),
            (
                "ConfigNotFound",
                SyncError::ConfigNotFound("x".into()),
                false,
            ),
            (
                "RateLimited",
                SyncError::RateLimited { retry_after: None },
                true,
            ),
            (
                "ServerError 5xx",
                SyncError::ServerError {
                    status: 503,
                    message: "down".into(),
                },
                true,
            ),
            (
                // is_retryable's status >= 500 guard is the canonical policy;
                // a mislabeled ServerError below 500 must not be retried.
                "ServerError below 500",
                SyncError::ServerError {
                    status: 404,
                    message: "not actually a server error".into(),
                },
                false,
            ),
            ("ConnectionTimeout", SyncError::ConnectionTimeout, true),
            (
                "BadRequest",
                SyncError::BadRequest {
                    status: 400,
                    message: "x".into(),
                },
                false,
            ),
            (
                "RetryExhausted",
                SyncError::RetryExhausted {
                    attempts: 3,
                    last_error: "x".into(),
                },
                false,
            ),
            (
                "InvalidTableName",
                SyncError::InvalidTableName {
                    name: "x".into(),
                    available: "a, b".into(),
                },
                false,
            ),
            ("ObjectStore", SyncError::ObjectStore(store_err), false),
            ("Stash", SyncError::Stash("x".into()), false),
            ("InvalidUrl", SyncError::InvalidUrl("x".into()), false),
            ("RelayNotFound", SyncError::RelayNotFound("x".into()), false),
            (
                "ParamLimitExceeded",
                SyncError::ParamLimitExceeded {
                    table: "t".into(),
                    row_count: 1,
                    col_count: 1,
                    limit: 1,
                },
                false,
            ),
            ("Broadcast", SyncError::Broadcast("x".into()), false),
            ("Plugin", SyncError::Plugin("x".into()), false),
            (
                "Migrate",
                SyncError::Migrate(crate::migrate::MigrateError::Checksum {
                    expected: "a".into(),
                    actual: "b".into(),
                }),
                false,
            ),
            (
                "LedgerTampered",
                SyncError::LedgerTampered("broken chain".into()),
                false,
            ),
            // Never retryable: re-running the sync re-reads the same two rows
            // and collides again. Only the operator re-keying a row clears it.
            (
                "DuplicatePrimaryKey",
                SyncError::DuplicatePrimaryKey {
                    table: "items".into(),
                    pk: "dup".into(),
                    first_hash: "aaaa".into(),
                    second_hash: "bbbb".into(),
                },
                false,
            ),
            // Same remedy class as DuplicatePrimaryKey, arriving over the
            // plugin wire instead of the native path (#444): never retryable,
            // re-running collides again until a human re-keys the row.
            (
                "PluginConflict",
                SyncError::PluginConflict {
                    plugin: "http-sql".into(),
                    message: "duplicate primary key '1' in table 'items'".into(),
                },
                false,
            ),
        ];

        for (name, err, expected_retryable) in cases {
            assert_eq!(
                err.is_retryable(),
                expected_retryable,
                "is_retryable() verdict changed for {name}"
            );
            assert_eq!(
                is_transient_error(&err),
                expected_retryable,
                "is_transient_error() diverged from is_retryable() for {name} \
                 with no documented delta"
            );
        }

        // The one documented delta: ConcurrentWrite is fatal per is_retryable
        // (the native path can't retry within a single invocation) but
        // transient per is_transient_error (the daemon's tick loop can).
        assert!(!SyncError::ConcurrentWrite.is_retryable());
        assert!(is_transient_error(&SyncError::ConcurrentWrite));
    }
}
