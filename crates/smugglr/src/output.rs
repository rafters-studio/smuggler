//! Structured output for agent-friendly JSON responses.
//!
//! When `--output json` is passed, commands emit a single JSON object to stdout
//! instead of human-readable text. The watch daemon emits one JSON line per tick
//! (JSONL format).

use serde::Serialize;
use smugglr_core::diff::TableDiff;
use smugglr_core::sync::SyncResult;

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
#[clap(rename_all = "lower")]
pub enum OutputFormat {
    Text,
    Json,
}

/// Machine-readable status carried by every JSON output.
///
/// Serializes to exactly "ok" / "error" / "dry_run" -- a public wire contract.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Ok,
    Error,
    DryRun,
}

#[derive(Serialize)]
pub struct CommandOutput {
    pub command: &'static str,
    pub status: Status,
    pub tables: Vec<TableOutput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Serialize)]
pub struct TableOutput {
    pub name: String,
    #[serde(skip_serializing_if = "is_zero")]
    pub rows_pushed: usize,
    #[serde(skip_serializing_if = "is_zero")]
    pub rows_pulled: usize,
    /// How many times the write to this table backed off and retried (#444).
    /// Omitted (not merely `0`) when no retry happened, matching
    /// `rows_pushed`/`rows_pulled`'s convention, so a run where every batch
    /// succeeded on its first attempt stays byte-identical on the wire to a
    /// build before this field existed.
    #[serde(skip_serializing_if = "is_zero")]
    pub retry_count: usize,
}

fn is_zero(n: &usize) -> bool {
    *n == 0
}

/// Build the per-table output rows shared by [`CommandOutput::from_sync_results`]
/// and [`WatchTickOutput::from_results`]: tables with no pushed/pulled rows are
/// dropped so JSON output only reports what actually changed.
fn table_outputs(results: &[SyncResult]) -> Vec<TableOutput> {
    results
        .iter()
        .filter(|r| r.has_changes())
        .map(|r| TableOutput {
            name: r.table.clone(),
            rows_pushed: r.rows_pushed,
            rows_pulled: r.rows_pulled,
            retry_count: r.retry_count,
        })
        .collect()
}

#[derive(Serialize)]
pub struct DiffOutput {
    pub command: &'static str,
    pub status: Status,
    pub tables: Vec<TableDiffOutput>,
}

/// The five per-table primary-key lists shared by diff and verbose dry-run output.
///
/// Embedded via `#[serde(flatten)]` so the keys appear inline at the parent level,
/// keeping the wire byte-identical with the previously-duplicated fields.
#[derive(Serialize)]
pub struct DiffBreakdown {
    pub local_only: Vec<String>,
    pub remote_only: Vec<String>,
    pub local_newer: Vec<String>,
    pub remote_newer: Vec<String>,
    pub content_differs: Vec<String>,
}

#[derive(Serialize)]
pub struct TableDiffOutput {
    pub name: String,
    #[serde(flatten)]
    pub breakdown: DiffBreakdown,
    pub identical_count: usize,
}

#[derive(Serialize)]
pub struct StatusOutput {
    pub command: &'static str,
    pub status: Status,
    pub config: StatusConfig,
    pub local: StatusDb,
    pub target: StatusDb,
}

#[derive(Serialize)]
pub struct StatusConfig {
    pub local_db: String,
    pub target_type: String,
    /// The endpoint a plugin target will actually talk to (#429).
    ///
    /// For a `d1` target this is derived rather than configured, so an operator
    /// who cannot see it cannot tell a working config from one pointed at the
    /// wrong account. Only the URL: the token in the same plugin config stays
    /// out of the output.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_url: Option<String>,
    pub timestamp_column: String,
    pub conflict_resolution: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tables: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub exclude_tables: Vec<String>,
}

#[derive(Serialize)]
pub struct StatusDb {
    pub connected: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub tables: Vec<StatusTable>,
}

#[derive(Serialize)]
pub struct StatusTable {
    pub name: String,
    pub rows: usize,
}

#[derive(Serialize)]
pub struct WatchTickOutput {
    pub command: &'static str,
    pub tick: u64,
    pub status: Status,
    pub tables: Vec<TableOutput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Serialize)]
pub struct SnapshotOutput {
    pub command: &'static str,
    pub status: Status,
    pub timestamp: String,
    pub size_bytes: u64,
    pub tables: Vec<SnapshotTableInfo>,
}

#[derive(Serialize)]
pub struct SnapshotTableInfo {
    pub name: String,
    pub row_count: usize,
}

#[derive(Serialize)]
pub struct SnapshotListOutput {
    pub command: &'static str,
    pub status: Status,
    pub snapshots: Vec<SnapshotListEntry>,
}

#[derive(Serialize)]
pub struct SnapshotListEntry {
    pub timestamp: String,
    pub size_bytes: u64,
    pub tables: Vec<SnapshotTableInfo>,
}

#[derive(Serialize)]
pub struct ErrorOutput {
    pub command: &'static str,
    pub status: Status,
    pub error: String,
    pub exit_code: i32,
}

impl CommandOutput {
    pub fn from_sync_results(command: &'static str, results: &[SyncResult]) -> Self {
        Self {
            command,
            status: Status::Ok,
            tables: table_outputs(results),
            error: None,
        }
    }
}

/// Structured dry-run output with per-table diff breakdown.
///
/// Emitted by `--dry-run --output json`. Contains the same diff data
/// used by the actual sync so agents can use it as an approval gate.
/// Generic over the table detail type (compact counts vs verbose PK lists).
#[derive(Serialize)]
pub struct DryRunOutput<T: Serialize> {
    pub command: &'static str,
    pub status: Status,
    pub tables: Vec<T>,
    pub total_rows_to_push: usize,
    pub total_rows_to_pull: usize,
    pub exit_code: i32,
}

#[derive(Serialize)]
pub struct DryRunTableOutput {
    pub name: String,
    pub local_only: usize,
    pub remote_only: usize,
    pub local_newer: usize,
    pub remote_newer: usize,
    pub content_differs: usize,
    pub identical: usize,
    pub rows_to_push: usize,
    pub rows_to_pull: usize,
}

#[derive(Serialize)]
pub struct DryRunVerboseTableOutput {
    pub name: String,
    #[serde(flatten)]
    pub breakdown: DiffBreakdown,
    pub identical: usize,
    pub rows_to_push: usize,
    pub rows_to_pull: usize,
}

impl<T: Serialize> DryRunOutput<T> {
    fn build(
        command: &'static str,
        results: &[SyncResult],
        map_table: impl Fn(&SyncResult) -> T,
    ) -> Self {
        let mut total_push = 0;
        let mut total_pull = 0;
        let tables: Vec<_> = results
            .iter()
            .map(|r| {
                total_push += r.rows_pushed;
                total_pull += r.rows_pulled;
                map_table(r)
            })
            .collect();

        Self {
            command,
            status: Status::DryRun,
            tables,
            total_rows_to_push: total_push,
            total_rows_to_pull: total_pull,
            exit_code: 0,
        }
    }
}

impl DryRunOutput<DryRunTableOutput> {
    pub fn from_sync_results(command: &'static str, results: &[SyncResult]) -> Self {
        Self::build(command, results, |r| {
            let stats = r.diff_stats.as_ref();
            DryRunTableOutput {
                name: r.table.clone(),
                local_only: stats.map(|s| s.local_only).unwrap_or(0),
                remote_only: stats.map(|s| s.remote_only).unwrap_or(0),
                local_newer: stats.map(|s| s.local_newer).unwrap_or(0),
                remote_newer: stats.map(|s| s.remote_newer).unwrap_or(0),
                content_differs: stats.map(|s| s.content_differs).unwrap_or(0),
                identical: stats.map(|s| s.identical).unwrap_or(0),
                rows_to_push: r.rows_pushed,
                rows_to_pull: r.rows_pulled,
            }
        })
    }
}

impl DryRunOutput<DryRunVerboseTableOutput> {
    pub fn from_sync_results(command: &'static str, results: &[SyncResult]) -> Self {
        Self::build(command, results, |r| {
            let detail = r.diff_detail.as_ref();
            DryRunVerboseTableOutput {
                name: r.table.clone(),
                breakdown: DiffBreakdown {
                    local_only: detail.map(|d| d.local_only.clone()).unwrap_or_default(),
                    remote_only: detail.map(|d| d.remote_only.clone()).unwrap_or_default(),
                    local_newer: detail.map(|d| d.local_newer.clone()).unwrap_or_default(),
                    remote_newer: detail.map(|d| d.remote_newer.clone()).unwrap_or_default(),
                    content_differs: detail
                        .map(|d| d.content_differs.clone())
                        .unwrap_or_default(),
                },
                identical: r.diff_stats.as_ref().map(|s| s.identical).unwrap_or(0),
                rows_to_push: r.rows_pushed,
                rows_to_pull: r.rows_pulled,
            }
        })
    }
}

impl DiffOutput {
    pub fn from_diffs(diffs: Vec<(String, TableDiff)>) -> Self {
        Self {
            command: "diff",
            status: Status::Ok,
            tables: diffs
                .into_iter()
                .map(|(name, d)| TableDiffOutput {
                    name,
                    identical_count: d.identical.len(),
                    breakdown: DiffBreakdown {
                        local_only: d.local_only,
                        remote_only: d.remote_only,
                        local_newer: d.local_newer,
                        remote_newer: d.remote_newer,
                        content_differs: d.content_differs,
                    },
                })
                .collect(),
        }
    }
}

impl WatchTickOutput {
    pub fn from_results(tick: u64, results: &[SyncResult], dry_run: bool) -> Self {
        Self {
            command: "watch",
            tick,
            status: if dry_run { Status::DryRun } else { Status::Ok },
            tables: table_outputs(results),
            error: None,
        }
    }

    pub fn from_error(tick: u64, err: &str) -> Self {
        Self {
            command: "watch",
            tick,
            status: Status::Error,
            tables: vec![],
            error: Some(err.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use smugglr_core::diff::TableDiff;
    use smugglr_core::sync::SyncResult;

    #[test]
    fn test_output_format_parse() {
        use clap::ValueEnum;
        assert_eq!(
            OutputFormat::from_str("text", true).unwrap(),
            OutputFormat::Text
        );
        assert_eq!(
            OutputFormat::from_str("json", true).unwrap(),
            OutputFormat::Json
        );
        assert!(OutputFormat::from_str("xml", true).is_err());
    }

    #[test]
    fn test_command_output_json_serialization() {
        let results = vec![
            SyncResult {
                table: "abilities".into(),
                rows_pushed: 42,
                rows_pulled: 0,
                retry_count: 0,
                diff_stats: None,
                diff_detail: None,
            },
            SyncResult {
                table: "items".into(),
                rows_pushed: 0,
                rows_pulled: 0,
                retry_count: 0,
                diff_stats: None,
                diff_detail: None,
            },
        ];

        let out = CommandOutput::from_sync_results("push", &results);
        let json = serde_json::to_string(&out).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(v["command"], "push");
        assert_eq!(v["status"], "ok");
        // Only abilities should appear (items had 0 changes)
        assert_eq!(v["tables"].as_array().unwrap().len(), 1);
        assert_eq!(v["tables"][0]["name"], "abilities");
        assert_eq!(v["tables"][0]["rows_pushed"], 42);
        // error should be absent (skip_serializing_if)
        assert!(v.get("error").is_none());
    }

    // #444: the retry count a table's write needed must be visible in
    // --output json exactly when it is nonzero, matching the
    // rows_pushed/rows_pulled skip_serializing_if convention -- so an agent
    // can tell a sync that backed off twice from one that sailed through
    // without any change to the wire shape for the common (no-retry) case.
    #[test]
    fn test_command_output_reports_retry_count_when_nonzero() {
        let results = vec![SyncResult {
            table: "abilities".into(),
            rows_pushed: 42,
            rows_pulled: 0,
            retry_count: 2,
            diff_stats: None,
            diff_detail: None,
        }];

        let out = CommandOutput::from_sync_results("push", &results);
        let json = serde_json::to_string(&out).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(v["tables"][0]["retry_count"], 2);
    }

    #[test]
    fn golden_command_output_with_retries() {
        // Companion to golden_command_output (which pins the zero-retry wire
        // shape carrying no retry_count key at all): this pins the exact
        // shape once a retry happened, key included.
        let results = vec![SyncResult {
            table: "abilities".into(),
            rows_pushed: 42,
            rows_pulled: 0,
            retry_count: 2,
            diff_stats: None,
            diff_detail: None,
        }];
        let out = CommandOutput::from_sync_results("push", &results);
        assert_eq!(
            serde_json::to_value(&out).unwrap(),
            json!({
                "command": "push",
                "status": "ok",
                "tables": [
                    { "name": "abilities", "rows_pushed": 42, "retry_count": 2 }
                ]
            })
        );
    }

    #[test]
    fn test_diff_output_json_serialization() {
        let mut diff = TableDiff::new("abilities");
        diff.local_only = vec!["pk1".into(), "pk2".into()];
        diff.remote_only = vec!["pk3".into()];
        diff.identical = vec!["pk4".into(), "pk5".into()];

        let out = DiffOutput::from_diffs(vec![("abilities".into(), diff)]);
        let json = serde_json::to_string(&out).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(v["command"], "diff");
        assert_eq!(v["tables"][0]["name"], "abilities");
        assert_eq!(v["tables"][0]["local_only"].as_array().unwrap().len(), 2);
        assert_eq!(v["tables"][0]["remote_only"].as_array().unwrap().len(), 1);
        assert_eq!(v["tables"][0]["identical_count"], 2);
    }

    #[test]
    fn test_watch_tick_output() {
        let results = vec![SyncResult {
            table: "t".into(),
            rows_pushed: 3,
            rows_pulled: 7,
            retry_count: 0,
            diff_stats: None,
            diff_detail: None,
        }];

        let out = WatchTickOutput::from_results(5, &results, false);
        let json = serde_json::to_string(&out).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(v["command"], "watch");
        assert_eq!(v["tick"], 5);
        assert_eq!(v["status"], "ok");
        assert!(v.get("error").is_none());
    }

    #[test]
    fn test_watch_tick_error_output() {
        let out = WatchTickOutput::from_error(3, "connection timeout");
        let json = serde_json::to_string(&out).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(v["status"], "error");
        assert_eq!(v["tick"], 3);
        assert_eq!(v["error"], "connection timeout");
    }

    #[test]
    fn test_error_output_json() {
        let out = ErrorOutput {
            command: "push",
            status: Status::Error,
            error: "Config file not found".into(),
            exit_code: 2,
        };
        let json = serde_json::to_string(&out).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(v["command"], "push");
        assert_eq!(v["status"], "error");
        assert_eq!(v["exit_code"], 2);
    }

    #[test]
    fn test_status_output_json() {
        let out = StatusOutput {
            command: "status",
            status: Status::Ok,
            config: StatusConfig {
                local_db: "game.db".into(),
                target_type: "sqlite".into(),
                target_url: None,
                timestamp_column: "updated_at".into(),
                conflict_resolution: "NewerWins".into(),
                tables: vec![],
                exclude_tables: vec![],
            },
            local: StatusDb {
                connected: true,
                error: None,
                tables: vec![StatusTable {
                    name: "abilities".into(),
                    rows: 100,
                }],
            },
            target: StatusDb {
                connected: true,
                error: None,
                tables: vec![StatusTable {
                    name: "abilities".into(),
                    rows: 95,
                }],
            },
        };
        let json = serde_json::to_string(&out).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(v["command"], "status");
        assert_eq!(v["config"]["local_db"], "game.db");
        assert_eq!(v["local"]["connected"], true);
        assert_eq!(v["local"]["tables"][0]["rows"], 100);
        assert_eq!(v["target"]["tables"][0]["rows"], 95);
    }

    #[test]
    fn test_dry_run_output_json_structure() {
        use smugglr_core::diff::DiffStats;

        let results = vec![
            SyncResult {
                table: "abilities".into(),
                rows_pushed: 8,
                rows_pulled: 3,
                retry_count: 0,
                diff_stats: Some(DiffStats {
                    local_only: 3,
                    remote_only: 1,
                    local_newer: 5,
                    remote_newer: 2,
                    content_differs: 0,
                    identical: 142,
                }),
                diff_detail: None,
            },
            SyncResult {
                table: "items".into(),
                rows_pushed: 0,
                rows_pulled: 0,
                retry_count: 0,
                diff_stats: Some(DiffStats {
                    local_only: 0,
                    remote_only: 0,
                    local_newer: 0,
                    remote_newer: 0,
                    content_differs: 0,
                    identical: 50,
                }),
                diff_detail: None,
            },
        ];

        let out = DryRunOutput::<DryRunTableOutput>::from_sync_results("sync", &results);
        let json = serde_json::to_string(&out).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(v["command"], "sync");
        assert_eq!(v["status"], "dry_run");
        assert_eq!(v["total_rows_to_push"], 8);
        assert_eq!(v["total_rows_to_pull"], 3);
        assert_eq!(v["exit_code"], 0);

        let tables = v["tables"].as_array().unwrap();
        assert_eq!(tables.len(), 2);

        assert_eq!(tables[0]["name"], "abilities");
        assert_eq!(tables[0]["local_only"], 3);
        assert_eq!(tables[0]["remote_only"], 1);
        assert_eq!(tables[0]["local_newer"], 5);
        assert_eq!(tables[0]["remote_newer"], 2);
        assert_eq!(tables[0]["content_differs"], 0);
        assert_eq!(tables[0]["identical"], 142);
        assert_eq!(tables[0]["rows_to_push"], 8);
        assert_eq!(tables[0]["rows_to_pull"], 3);

        assert_eq!(tables[1]["name"], "items");
        assert_eq!(tables[1]["rows_to_push"], 0);
        assert_eq!(tables[1]["rows_to_pull"], 0);
        assert_eq!(tables[1]["identical"], 50);
    }

    #[test]
    fn test_dry_run_no_side_effects_matches_structure() {
        let results = vec![SyncResult {
            table: "t".into(),
            rows_pushed: 5,
            rows_pulled: 2,
            retry_count: 0,
            diff_stats: None,
            diff_detail: None,
        }];

        let out = DryRunOutput::<DryRunTableOutput>::from_sync_results("push", &results);
        let json = serde_json::to_string(&out).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(v["status"], "dry_run");
        assert_eq!(v["total_rows_to_push"], 5);
        assert_eq!(v["total_rows_to_pull"], 2);
        // Without diff_stats, zeros are used for breakdown
        assert_eq!(v["tables"][0]["local_only"], 0);
        assert_eq!(v["tables"][0]["identical"], 0);
    }

    #[test]
    fn test_dry_run_verbose_output_includes_pk_values() {
        use smugglr_core::diff::DiffStats;
        use smugglr_core::sync::DiffDetail;

        let results = vec![SyncResult {
            table: "abilities".into(),
            rows_pushed: 3,
            rows_pulled: 1,
            retry_count: 0,
            diff_stats: Some(DiffStats {
                local_only: 2,
                remote_only: 1,
                local_newer: 1,
                remote_newer: 0,
                content_differs: 0,
                identical: 10,
            }),
            diff_detail: Some(DiffDetail {
                local_only: vec!["pk1".into(), "pk2".into()],
                remote_only: vec!["pk3".into()],
                local_newer: vec!["pk4".into()],
                remote_newer: vec![],
                content_differs: vec![],
            }),
        }];

        let out = DryRunOutput::<DryRunVerboseTableOutput>::from_sync_results("push", &results);
        let json = serde_json::to_string(&out).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(v["command"], "push");
        assert_eq!(v["status"], "dry_run");
        assert_eq!(v["total_rows_to_push"], 3);
        assert_eq!(v["total_rows_to_pull"], 1);

        let t = &v["tables"][0];
        assert_eq!(t["name"], "abilities");
        assert_eq!(t["local_only"].as_array().unwrap().len(), 2);
        assert_eq!(t["local_only"][0], "pk1");
        assert_eq!(t["local_only"][1], "pk2");
        assert_eq!(t["remote_only"].as_array().unwrap().len(), 1);
        assert_eq!(t["remote_only"][0], "pk3");
        assert_eq!(t["local_newer"].as_array().unwrap().len(), 1);
        assert_eq!(t["remote_newer"].as_array().unwrap().len(), 0);
        assert_eq!(t["content_differs"].as_array().unwrap().len(), 0);
        assert_eq!(t["identical"], 10);
        assert_eq!(t["rows_to_push"], 3);
        assert_eq!(t["rows_to_pull"], 1);
    }

    // ---- Golden wire tests ----
    //
    // These capture the FULL serialized shape of every output struct via
    // `serde_json::to_value` compared against a `json!` literal. Any field
    // add/remove/rename breaks them. `serde_json::Value` map comparison is
    // order-insensitive, so they guard keys+values, not field order.

    use serde_json::json;

    #[test]
    fn golden_command_output() {
        let results = vec![
            SyncResult {
                table: "abilities".into(),
                rows_pushed: 42,
                rows_pulled: 0,
                retry_count: 0,
                diff_stats: None,
                diff_detail: None,
            },
            SyncResult {
                table: "items".into(),
                rows_pushed: 0,
                rows_pulled: 0,
                retry_count: 0,
                diff_stats: None,
                diff_detail: None,
            },
        ];
        let out = CommandOutput::from_sync_results("push", &results);
        assert_eq!(
            serde_json::to_value(&out).unwrap(),
            json!({
                "command": "push",
                "status": "ok",
                "tables": [
                    { "name": "abilities", "rows_pushed": 42 }
                ]
            })
        );
    }

    #[test]
    fn golden_command_output_no_changed_table() {
        let results = vec![SyncResult {
            table: "items".into(),
            rows_pushed: 0,
            rows_pulled: 0,
            retry_count: 0,
            diff_stats: None,
            diff_detail: None,
        }];
        let out = CommandOutput::from_sync_results("pull", &results);
        assert_eq!(
            serde_json::to_value(&out).unwrap(),
            json!({
                "command": "pull",
                "status": "ok",
                "tables": []
            })
        );
    }

    #[test]
    fn golden_diff_output() {
        let mut diff = TableDiff::new("abilities");
        diff.local_only = vec!["pk1".into(), "pk2".into()];
        diff.remote_only = vec!["pk3".into()];
        diff.local_newer = vec!["pk4".into()];
        diff.remote_newer = vec!["pk5".into()];
        diff.content_differs = vec!["pk6".into()];
        diff.identical = vec!["pk7".into(), "pk8".into()];

        let out = DiffOutput::from_diffs(vec![("abilities".into(), diff)]);
        assert_eq!(
            serde_json::to_value(&out).unwrap(),
            json!({
                "command": "diff",
                "status": "ok",
                "tables": [{
                    "name": "abilities",
                    "local_only": ["pk1", "pk2"],
                    "remote_only": ["pk3"],
                    "local_newer": ["pk4"],
                    "remote_newer": ["pk5"],
                    "content_differs": ["pk6"],
                    "identical_count": 2
                }]
            })
        );
    }

    #[test]
    fn golden_status_output() {
        let out = StatusOutput {
            command: "status",
            status: Status::Ok,
            config: StatusConfig {
                local_db: "game.db".into(),
                target_type: "sqlite".into(),
                target_url: None,
                timestamp_column: "updated_at".into(),
                conflict_resolution: "NewerWins".into(),
                tables: vec!["abilities".into(), "items".into()],
                exclude_tables: vec!["_migrations".into()],
            },
            local: StatusDb {
                connected: true,
                error: None,
                tables: vec![StatusTable {
                    name: "abilities".into(),
                    rows: 100,
                }],
            },
            target: StatusDb {
                connected: false,
                error: Some("connection refused".into()),
                tables: vec![],
            },
        };
        assert_eq!(
            serde_json::to_value(&out).unwrap(),
            json!({
                "command": "status",
                "status": "ok",
                "config": {
                    "local_db": "game.db",
                    "target_type": "sqlite",
                    "timestamp_column": "updated_at",
                    "conflict_resolution": "NewerWins",
                    "tables": ["abilities", "items"],
                    "exclude_tables": ["_migrations"]
                },
                "local": {
                    "connected": true,
                    "tables": [{ "name": "abilities", "rows": 100 }]
                },
                "target": {
                    "connected": false,
                    "error": "connection refused",
                    "tables": []
                }
            })
        );
    }

    #[test]
    fn golden_watch_tick_output() {
        let results = vec![SyncResult {
            table: "t".into(),
            rows_pushed: 3,
            rows_pulled: 7,
            retry_count: 0,
            diff_stats: None,
            diff_detail: None,
        }];
        let out = WatchTickOutput::from_results(5, &results, false);
        assert_eq!(
            serde_json::to_value(&out).unwrap(),
            json!({
                "command": "watch",
                "tick": 5,
                "status": "ok",
                "tables": [{ "name": "t", "rows_pushed": 3, "rows_pulled": 7 }]
            })
        );
    }

    #[test]
    fn golden_watch_tick_dry_run_output() {
        // Regression for #193: a dry-run watch tick must report status
        // "dry_run", matching every other command's dry-run JSON contract.
        let results = vec![SyncResult {
            table: "t".into(),
            rows_pushed: 3,
            rows_pulled: 7,
            retry_count: 0,
            diff_stats: None,
            diff_detail: None,
        }];
        let out = WatchTickOutput::from_results(5, &results, true);
        assert_eq!(
            serde_json::to_value(&out).unwrap(),
            json!({
                "command": "watch",
                "tick": 5,
                "status": "dry_run",
                "tables": [{ "name": "t", "rows_pushed": 3, "rows_pulled": 7 }]
            })
        );
    }

    #[test]
    fn golden_watch_tick_error_output() {
        let out = WatchTickOutput::from_error(3, "connection timeout");
        assert_eq!(
            serde_json::to_value(&out).unwrap(),
            json!({
                "command": "watch",
                "tick": 3,
                "status": "error",
                "tables": [],
                "error": "connection timeout"
            })
        );
    }

    #[test]
    fn golden_snapshot_output() {
        let out = SnapshotOutput {
            command: "snapshot",
            status: Status::Ok,
            timestamp: "2026-05-30T00:00:00Z".into(),
            size_bytes: 4096,
            tables: vec![SnapshotTableInfo {
                name: "abilities".into(),
                row_count: 100,
            }],
        };
        assert_eq!(
            serde_json::to_value(&out).unwrap(),
            json!({
                "command": "snapshot",
                "status": "ok",
                "timestamp": "2026-05-30T00:00:00Z",
                "size_bytes": 4096,
                "tables": [{ "name": "abilities", "row_count": 100 }]
            })
        );
    }

    #[test]
    fn golden_snapshot_list_output() {
        let out = SnapshotListOutput {
            command: "snapshots",
            status: Status::Ok,
            snapshots: vec![SnapshotListEntry {
                timestamp: "2026-05-30T00:00:00Z".into(),
                size_bytes: 4096,
                tables: vec![SnapshotTableInfo {
                    name: "abilities".into(),
                    row_count: 100,
                }],
            }],
        };
        assert_eq!(
            serde_json::to_value(&out).unwrap(),
            json!({
                "command": "snapshots",
                "status": "ok",
                "snapshots": [{
                    "timestamp": "2026-05-30T00:00:00Z",
                    "size_bytes": 4096,
                    "tables": [{ "name": "abilities", "row_count": 100 }]
                }]
            })
        );
    }

    #[test]
    fn golden_error_output() {
        let out = ErrorOutput {
            command: "push",
            status: Status::Error,
            error: "Config file not found".into(),
            exit_code: 2,
        };
        assert_eq!(
            serde_json::to_value(&out).unwrap(),
            json!({
                "command": "push",
                "status": "error",
                "error": "Config file not found",
                "exit_code": 2
            })
        );
    }

    #[test]
    fn golden_dry_run_output_compact() {
        use smugglr_core::diff::DiffStats;
        let results = vec![SyncResult {
            table: "abilities".into(),
            rows_pushed: 8,
            rows_pulled: 3,
            retry_count: 0,
            diff_stats: Some(DiffStats {
                local_only: 3,
                remote_only: 1,
                local_newer: 5,
                remote_newer: 2,
                content_differs: 0,
                identical: 142,
            }),
            diff_detail: None,
        }];
        let out = DryRunOutput::<DryRunTableOutput>::from_sync_results("sync", &results);
        assert_eq!(
            serde_json::to_value(&out).unwrap(),
            json!({
                "command": "sync",
                "status": "dry_run",
                "tables": [{
                    "name": "abilities",
                    "local_only": 3,
                    "remote_only": 1,
                    "local_newer": 5,
                    "remote_newer": 2,
                    "content_differs": 0,
                    "identical": 142,
                    "rows_to_push": 8,
                    "rows_to_pull": 3
                }],
                "total_rows_to_push": 8,
                "total_rows_to_pull": 3,
                "exit_code": 0
            })
        );
    }

    #[test]
    fn golden_dry_run_output_verbose() {
        use smugglr_core::diff::DiffStats;
        use smugglr_core::sync::DiffDetail;
        let results = vec![SyncResult {
            table: "abilities".into(),
            rows_pushed: 3,
            rows_pulled: 1,
            retry_count: 0,
            diff_stats: Some(DiffStats {
                local_only: 2,
                remote_only: 1,
                local_newer: 1,
                remote_newer: 0,
                content_differs: 0,
                identical: 10,
            }),
            diff_detail: Some(DiffDetail {
                local_only: vec!["pk1".into(), "pk2".into()],
                remote_only: vec!["pk3".into()],
                local_newer: vec!["pk4".into()],
                remote_newer: vec![],
                content_differs: vec![],
            }),
        }];
        let out = DryRunOutput::<DryRunVerboseTableOutput>::from_sync_results("push", &results);
        assert_eq!(
            serde_json::to_value(&out).unwrap(),
            json!({
                "command": "push",
                "status": "dry_run",
                "tables": [{
                    "name": "abilities",
                    "local_only": ["pk1", "pk2"],
                    "remote_only": ["pk3"],
                    "local_newer": ["pk4"],
                    "remote_newer": [],
                    "content_differs": [],
                    "identical": 10,
                    "rows_to_push": 3,
                    "rows_to_pull": 1
                }],
                "total_rows_to_push": 3,
                "total_rows_to_pull": 1,
                "exit_code": 0
            })
        );
    }
}
