//! Runtime plugin adapter for the DataSource trait.
//!
//! Plugins are standalone binaries that implement the DataSource interface
//! via JSON-RPC over stdin/stdout. This enables adapter development in any
//! language without recompiling smugglr.
//!
//! ## Protocol
//!
//! Each request is a single JSON line on stdin:
//! ```json
//! {"jsonrpc":"2.0","method":"list_tables","params":{},"id":1}
//! ```
//!
//! Each response is a single JSON line on stdout:
//! ```json
//! {"jsonrpc":"2.0","result":["users","posts"],"id":1}
//! ```
//!
//! ## Methods
//!
//! - `initialize` - params: `{config: {key: value, ...}}`
//! - `list_tables` - params: `{}`
//! - `table_info` - params: `{table: string}`
//! - `get_row_metadata` - params: `{table, timestamp_column, exclude_columns}`
//! - `get_rows` - params: `{table, pk_values}`
//! - `upsert_rows` - params: `{table, rows}`
//! - `row_count` - params: `{table}`
//!
//! ## Error classes
//!
//! A JSON-RPC error response carries only `{code: i64, message: String}` --
//! no structured params. A plugin signals *error class* (transient vs.
//! permanent, and for permanent errors, which exit-code bucket) entirely
//! through `code`, using one of a small set of codes reserved in the
//! JSON-RPC "server error" range (`-32099..=-32000`):
//!
//! - `PLUGIN_TRANSIENT_ERROR_CODE` (-32010) -- general transient failure
//!   (5xx, timeout). Retried; maps to `SyncError::ServerError { status: 503 }`.
//! - `PLUGIN_RATE_LIMITED_ERROR_CODE` (-32011) -- rate limited (HTTP 429).
//!   Retried; maps to `SyncError::RateLimited { retry_after }`.
//! - `PLUGIN_CONFLICT_ERROR_CODE` (-32012) -- permanent conflict needing a
//!   human decision (e.g. duplicate primary key). Not retried; maps to
//!   `SyncError::PluginConflict`, exit code 4.
//! - Any other code -- permanent, uncategorized. Not retried; maps to
//!   `SyncError::Plugin`, exit code 1 (unchanged since #181).
//!
//! `message` is otherwise free text with exactly one documented exception:
//! a `PLUGIN_RATE_LIMITED_ERROR_CODE` error may prefix `message` with
//! `retry_after_ms=<millis>;` to carry a `Retry-After` delay (see
//! `RETRY_AFTER_MS_PREFIX`, `parse_retry_after_ms`). That prefix is the only
//! part of `message` this module parses. Nothing else in `message` is
//! host-parsed, and it must never become so -- a richer error (structured
//! typed fields, like `SyncError::DuplicatePrimaryKey`'s `table`/`pk`/
//! `first_hash`/`second_hash`) cannot cross this wire without reconstructing
//! it by parsing free text, which #333 named and rejected as an
//! anti-pattern. A future error type that needs more than "which bucket" and
//! "how long to wait" needs either a new reserved code (cheap, if the class
//! is genuinely new) or a wire protocol change (structured error params),
//! not a second string pattern bolted onto `message`.
//!
//! `LedgerTampered` and `SchemaDrift` (#290) are the next two `SyncError`
//! variants that might need a plugin-side path; each gets its own reserved
//! code here when that path exists, following this same shape.
//!
//! ## `upsert_rows`: what an absent column means
//!
//! A row object in `rows` is not required to carry every column the
//! destination table has, and a column it does not mention must be **left
//! alone** -- an existing row keeps its stored value for that column, and a new
//! row takes the column's schema default. An absent key does not mean NULL, and
//! an adapter must not derive its column list from the destination's schema (or
//! from `rows[0]`) and then bind NULL for whatever the row is missing.
//!
//! smugglr strips `[sync].exclude_columns` from every row before it is sent, so
//! an adapter that writes NULL for an absent column destroys the value the
//! operator configured to stay off the wire -- and does it invisibly, because an
//! excluded column is out of the content hash and nothing downstream compares
//! it. That was #324 in smugglr's own native apply path.
//!
//! Rows within one `upsert_rows` call may carry different key sets. An adapter
//! that builds one statement per batch must group by key set rather than take
//! the first row's keys as the batch's column list.

use crate::datasource::{DataSource, RowMeta, TableInfo};
use crate::error::{Result, SyncError};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::Mutex;
use tracing::debug;

/// A DataSource backed by an external plugin process communicating via JSON-RPC.
pub struct PluginDataSource {
    io: Mutex<PluginIo>,
    plugin_name: String,
}

struct PluginIo {
    child: Child,
    stdin: BufWriter<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    next_id: u64,
    read_buf: String,
}

#[derive(Serialize)]
struct RpcRequest<'a> {
    jsonrpc: &'static str,
    method: &'a str,
    params: JsonValue,
    id: u64,
}

#[derive(Deserialize)]
struct RpcResponse {
    result: Option<JsonValue>,
    error: Option<RpcError>,
    id: u64,
}

#[derive(Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

/// JSON-RPC error code a plugin uses to signal a general transient/retryable
/// failure (5xx, timeout).
///
/// MUST match `smugglr_plugin_sdk::TRANSIENT_ERROR_CODE` -- it is the wire
/// contract between a plugin (which constructs the error via
/// `PluginError::transient`) and the host (which routes it here). Duplicated
/// rather than shared because core does not depend on the SDK crate; a shared
/// wire crate would unify the two (see #228). `smugglr_plugin_sdk`'s own
/// `test_reserved_wire_codes_are_pinned` and this module's
/// `plugin_error_codes_match_the_sdk` test pin the same literals on both
/// sides, since nothing else enforces they stay in lockstep.
const PLUGIN_TRANSIENT_ERROR_CODE: i64 = -32010;

/// JSON-RPC error code a plugin uses to signal HTTP 429 specifically,
/// distinct from [`PLUGIN_TRANSIENT_ERROR_CODE`] so a `Retry-After` delay has
/// somewhere to travel: this maps to [`SyncError::RateLimited`] instead of
/// the fixed `ServerError { status: 503 }` the general transient code
/// produces.
///
/// MUST match `smugglr_plugin_sdk::RATE_LIMITED_ERROR_CODE` -- see
/// `PLUGIN_TRANSIENT_ERROR_CODE`'s doc for why this is duplicated rather than
/// shared. Constructed plugin-side with `PluginError::rate_limited`.
const PLUGIN_RATE_LIMITED_ERROR_CODE: i64 = -32011;

/// JSON-RPC error code a plugin uses to signal a permanent conflict needing a
/// human decision -- today, a duplicate-primary-key collision on the
/// plugin's target (#269, #444). Maps to [`SyncError::PluginConflict`],
/// sharing [`SyncError::DuplicatePrimaryKey`]'s exit code (4).
///
/// MUST match `smugglr_plugin_sdk::CONFLICT_ERROR_CODE` -- see
/// `PLUGIN_TRANSIENT_ERROR_CODE`'s doc for why this is duplicated rather than
/// shared. Constructed plugin-side with `PluginError::conflict`.
const PLUGIN_CONFLICT_ERROR_CODE: i64 = -32012;

/// The `message` prefix a plugin uses to carry a `Retry-After` delay, in
/// milliseconds, across the wire on a [`PLUGIN_RATE_LIMITED_ERROR_CODE`]
/// error: `"retry_after_ms=<millis>;<detail>"`.
///
/// MUST match `smugglr_plugin_sdk::RETRY_AFTER_MS_PREFIX`. Only this fixed
/// prefix is parsed here -- the remainder of `message` is carried through
/// unparsed as a human-readable detail folded into `SyncError::Plugin`'s
/// message elsewhere, never matched on. Extending what crosses the wire means
/// reserving a new code (or, if genuinely necessary, a new documented
/// prefix), not adding a second string pattern to match on here -- the
/// string-matching shape #333 rejected.
const RETRY_AFTER_MS_PREFIX: &str = "retry_after_ms=";

/// Parse a [`RETRY_AFTER_MS_PREFIX`]-prefixed message into the carried delay,
/// in milliseconds. Returns `None` if the prefix is absent or the number
/// before the first `;` fails to parse -- a malformed or missing prefix
/// degrades to "no retry-after known" (the host falls back to its own
/// backoff schedule), not a parse error, since the error's *class*
/// (rate-limited) is already established by the wire code alone.
fn parse_retry_after_ms(message: &str) -> Option<u64> {
    message
        .strip_prefix(RETRY_AFTER_MS_PREFIX)?
        .split_once(';')?
        .0
        .parse::<u64>()
        .ok()
}

/// Map a plugin's JSON-RPC error onto a [`SyncError`], by wire error class
/// (see the module doc's "Error classes" section for what a plugin can and
/// cannot communicate this way).
///
/// - [`PLUGIN_TRANSIENT_ERROR_CODE`] maps to a retryable `ServerError` so
///   `upsert_with_retry` backs off and retries.
/// - [`PLUGIN_RATE_LIMITED_ERROR_CODE`] maps to `RateLimited`, recovering any
///   `Retry-After` delay the plugin carried in `message`.
/// - [`PLUGIN_CONFLICT_ERROR_CODE`] maps to `PluginConflict`, sharing
///   `DuplicatePrimaryKey`'s exit code (4).
/// - Every other code stays `Plugin` (permanent, exit code 1) -- unchanged
///   from before this module carried any error class at all, and pinned by
///   `error.rs`'s `test_exit_code_plugin` regression test (#181).
fn rpc_error_to_sync_error(plugin_name: &str, err: &RpcError) -> SyncError {
    match err.code {
        PLUGIN_TRANSIENT_ERROR_CODE => SyncError::ServerError {
            status: 503,
            message: format!("plugin '{}': {}", plugin_name, err.message),
        },
        PLUGIN_RATE_LIMITED_ERROR_CODE => SyncError::RateLimited {
            // The wire prefix is milliseconds; SyncError::RateLimited's
            // retry_after (and retry_after_ms()'s *1000 back-conversion,
            // error.rs:175-181) is seconds. Round up rather than down so a
            // sub-second Retry-After never gets rounded away to an
            // immediate retry.
            retry_after: parse_retry_after_ms(&err.message).map(|ms| ms.div_ceil(1000)),
        },
        PLUGIN_CONFLICT_ERROR_CODE => SyncError::PluginConflict {
            plugin: plugin_name.to_string(),
            message: err.message.clone(),
        },
        _ => SyncError::Plugin(format!(
            "Plugin '{}' error (code {}): {}",
            plugin_name, err.code, err.message
        )),
    }
}

impl PluginDataSource {
    /// Spawn a plugin process and send the `initialize` handshake.
    pub async fn start(
        plugin_path: &Path,
        plugin_name: &str,
        config: &HashMap<String, String>,
    ) -> Result<Self> {
        let mut child = Command::new(plugin_path)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .spawn()
            .map_err(|e| {
                SyncError::Plugin(format!(
                    "Failed to spawn plugin '{}' at {}: {}",
                    plugin_name,
                    plugin_path.display(),
                    e
                ))
            })?;

        let stdin = child.stdin.take().expect("stdin was piped");
        let stdout = child.stdout.take().expect("stdout was piped");

        let ds = Self {
            io: Mutex::new(PluginIo {
                child,
                stdin: BufWriter::new(stdin),
                stdout: BufReader::new(stdout),
                next_id: 0,
                read_buf: String::new(),
            }),
            plugin_name: plugin_name.to_string(),
        };

        let config_value = serde_json::to_value(config)
            .map_err(|e| SyncError::Plugin(format!("Failed to serialize plugin config: {}", e)))?;

        let _: JsonValue = ds
            .call("initialize", serde_json::json!({ "config": config_value }))
            .await?;

        Ok(ds)
    }

    /// Send a JSON-RPC request and read the response.
    async fn call<T: serde::de::DeserializeOwned>(
        &self,
        method: &str,
        params: JsonValue,
    ) -> Result<T> {
        let mut io = self.io.lock().await;

        let id = io.next_id;
        io.next_id += 1;

        let request = RpcRequest {
            jsonrpc: "2.0",
            method,
            params,
            id,
        };

        let mut line = serde_json::to_string(&request)
            .map_err(|e| SyncError::Plugin(format!("Failed to serialize request: {}", e)))?;
        line.push('\n');

        debug!("Plugin {} request: {} id={}", self.plugin_name, method, id);

        io.stdin
            .write_all(line.as_bytes())
            .await
            .map_err(|e| SyncError::Plugin(format!("Failed to write to plugin: {}", e)))?;
        io.stdin
            .flush()
            .await
            .map_err(|e| SyncError::Plugin(format!("Failed to flush plugin stdin: {}", e)))?;

        let PluginIo {
            ref mut stdout,
            ref mut read_buf,
            ..
        } = *io;
        read_buf.clear();
        stdout
            .read_line(read_buf)
            .await
            .map_err(|e| SyncError::Plugin(format!("Failed to read from plugin: {}", e)))?;

        if read_buf.is_empty() {
            return Err(SyncError::Plugin(format!(
                "Plugin '{}' closed stdout unexpectedly",
                self.plugin_name
            )));
        }

        let response: RpcResponse = serde_json::from_str(read_buf).map_err(|e| {
            SyncError::Plugin(format!(
                "Invalid JSON-RPC response from plugin '{}': {}\nResponse: {}",
                self.plugin_name,
                e,
                read_buf.trim()
            ))
        })?;

        if response.id != id {
            return Err(SyncError::Plugin(format!(
                "Response ID mismatch from plugin '{}': expected {}, got {}",
                self.plugin_name, id, response.id
            )));
        }

        if let Some(err) = response.error {
            return Err(rpc_error_to_sync_error(&self.plugin_name, &err));
        }

        let result = response.result.ok_or_else(|| {
            SyncError::Plugin(format!(
                "Plugin '{}' returned neither result nor error",
                self.plugin_name
            ))
        })?;

        serde_json::from_value(result)
            .map_err(|e| SyncError::Plugin(format!("Failed to deserialize plugin response: {}", e)))
    }
}

impl Drop for PluginDataSource {
    fn drop(&mut self) {
        let io = self.io.get_mut();
        let _ = io.child.start_kill();
    }
}

impl DataSource for PluginDataSource {
    async fn list_tables(&self) -> Result<Vec<String>> {
        self.call("list_tables", serde_json::json!({})).await
    }

    async fn table_info(&self, table: &str) -> Result<TableInfo> {
        self.call("table_info", serde_json::json!({ "table": table }))
            .await
    }

    async fn get_row_metadata(
        &self,
        table: &str,
        timestamp_column: &str,
        exclude_columns: &[String],
    ) -> Result<HashMap<String, RowMeta>> {
        self.call(
            "get_row_metadata",
            serde_json::json!({
                "table": table,
                "timestamp_column": timestamp_column,
                "exclude_columns": exclude_columns,
            }),
        )
        .await
    }

    async fn get_rows(
        &self,
        table: &str,
        pk_values: &[String],
    ) -> Result<Vec<HashMap<String, JsonValue>>> {
        self.call(
            "get_rows",
            serde_json::json!({
                "table": table,
                "pk_values": pk_values,
            }),
        )
        .await
    }

    async fn upsert_rows(&self, table: &str, rows: &[HashMap<String, JsonValue>]) -> Result<usize> {
        self.call(
            "upsert_rows",
            serde_json::json!({
                "table": table,
                "rows": rows,
            }),
        )
        .await
    }

    async fn row_count(&self, table: &str) -> Result<usize> {
        self.call("row_count", serde_json::json!({ "table": table }))
            .await
    }
}

/// Directory under `$HOME` where smugglr looks for plugin binaries.
///
/// Single source of truth so the path we search and the path we name in errors
/// cannot drift apart -- that drift was bug #140 (the join said `.smuggler`
/// while the error message said `.smugglr`).
const PLUGIN_HOME_SUBDIR: &str = ".smugglr/plugins";

/// How to obtain the one plugin smugglr ships. Named in every "plugin not
/// found" error, because the CLI alone reaches no hosted target and a reader
/// who installed only `smugglr` has no other way to learn that (#430).
pub(crate) const HTTP_SQL_INSTALL_HINT: &str =
    "Get it with `cargo install smugglr-http-sql`, or from the \
     release archive, which carries it beside `smugglr` since v0.5.1.";

/// Resolve a plugin name to its binary path.
///
/// Search order:
/// 1. `~/.smugglr/plugins/smugglr-{name}`
/// 2. `smugglr-{name}` on `$PATH`
pub fn resolve_plugin_path(name: &str) -> Result<PathBuf> {
    let binary_name = format!("smugglr-{}", name);

    // Check ~/.smugglr/plugins/
    if let Ok(home) = std::env::var("HOME") {
        let candidate = PathBuf::from(home)
            .join(PLUGIN_HOME_SUBDIR)
            .join(&binary_name);
        if candidate.is_file() {
            return Ok(candidate);
        }
    }

    // Check $PATH
    if let Some(path) = find_in_path(&binary_name) {
        return Ok(path);
    }

    Err(SyncError::Plugin(not_found_message(name, &binary_name)))
}

/// The "plugin not found" text: where smugglr looked, and, for the one plugin
/// smugglr ships, how to get it. Pure so the message can be tested without
/// depending on what happens to be installed on the test machine.
fn not_found_message(name: &str, binary_name: &str) -> String {
    let hint = if name == "http-sql" {
        format!(" {}", HTTP_SQL_INSTALL_HINT)
    } else {
        String::new()
    };
    format!(
        "Plugin '{}' not found. Searched: ~/{}/{}, $PATH/{}.{}",
        name, PLUGIN_HOME_SUBDIR, binary_name, binary_name, hint
    )
}

fn find_in_path(name: &str) -> Option<PathBuf> {
    std::env::var_os("PATH").and_then(|paths| {
        std::env::split_paths(&paths).find_map(|dir| {
            let candidate = dir.join(name);
            if candidate.is_file() {
                Some(candidate)
            } else {
                None
            }
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_plugin_not_found() {
        let result = resolve_plugin_path("nonexistent-plugin-abc123");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, SyncError::Plugin(_)));
        assert!(err.to_string().contains("nonexistent-plugin-abc123"));
    }

    #[test]
    fn not_found_names_how_to_get_the_shipped_plugin() {
        // The plugin every remote target needs: the message must say how to
        // obtain it, both ways (#430).
        let msg = not_found_message("http-sql", "smugglr-http-sql");
        assert!(msg.contains("cargo install smugglr-http-sql"), "{msg}");
        assert!(msg.contains("release archive"), "{msg}");
        assert!(msg.contains("~/.smugglr/plugins/smugglr-http-sql"), "{msg}");
        // A third-party plugin gets the search paths and no smugglr-specific hint.
        let other = not_found_message("acme", "smugglr-acme");
        assert!(other.contains("$PATH/smugglr-acme"), "{other}");
        assert!(!other.contains("cargo install"), "{other}");
    }

    #[test]
    fn test_find_in_path_nonexistent() {
        assert!(find_in_path("smugglr-totally-fake-binary").is_none());
    }

    #[test]
    fn test_rpc_request_serialization() {
        let req = RpcRequest {
            jsonrpc: "2.0",
            method: "list_tables",
            params: serde_json::json!({}),
            id: 42,
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"jsonrpc\":\"2.0\""));
        assert!(json.contains("\"method\":\"list_tables\""));
        assert!(json.contains("\"id\":42"));
    }

    #[test]
    fn test_rpc_response_deserialization() {
        let json = r#"{"jsonrpc":"2.0","result":["users","posts"],"id":1}"#;
        let resp: RpcResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.id, 1);
        assert!(resp.error.is_none());
        let tables: Vec<String> = serde_json::from_value(resp.result.unwrap()).unwrap();
        assert_eq!(tables, vec!["users", "posts"]);
    }

    #[test]
    fn test_rpc_error_response_deserialization() {
        let json =
            r#"{"jsonrpc":"2.0","error":{"code":-32000,"message":"table not found"},"id":2}"#;
        let resp: RpcResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.id, 2);
        assert!(resp.result.is_none());
        let err = resp.error.unwrap();
        assert_eq!(err.code, -32000);
        assert_eq!(err.message, "table not found");
    }

    // These two deserialize into the canonical `smugglr_wire::{TableInfo,
    // RowMeta}` (re-exported here as `TableInfo`/`RowMeta`) rather than a
    // host-local `Wire*` shim -- see #228. `smugglr-wire` itself carries the
    // byte-level JSON snapshot tests; these confirm the host's `call()` path
    // deserializes plugin responses into the same canonical types unchanged.
    #[test]
    fn test_wire_table_info_deserialization() {
        let json = r#"{
            "name": "users",
            "columns": [
                {"name": "id", "col_type": "INTEGER", "notnull": true, "pk": true},
                {"name": "email", "col_type": "TEXT"}
            ],
            "primary_key": ["id"]
        }"#;
        let info: TableInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.name, "users");
        assert_eq!(info.columns.len(), 2);
        assert!(info.columns[0].pk);
        assert!(!info.columns[1].pk);
        assert_eq!(info.columns[1].col_type, "TEXT");
    }

    #[test]
    fn test_wire_row_meta_deserialization() {
        let json = r#"{
            "pk_value": "42",
            "updated_at": "2026-04-03T12:00:00Z",
            "content_hash": "abc123"
        }"#;
        let meta: RowMeta = serde_json::from_str(json).unwrap();
        assert_eq!(meta.pk_value, "42");
        assert_eq!(meta.updated_at.unwrap(), "2026-04-03T12:00:00Z");
        assert_eq!(meta.content_hash, "abc123");
    }

    // Pin the literal wire codes against the SDK's own pinned literals
    // (`smugglr_plugin_sdk::tests::test_reserved_wire_codes_are_pinned`).
    // Core does not depend on the SDK crate, so nothing else enforces the
    // two sides staying in lockstep -- if either literal moves, both tests
    // must be updated deliberately.
    #[test]
    fn plugin_error_codes_match_the_sdk() {
        assert_eq!(PLUGIN_TRANSIENT_ERROR_CODE, -32010);
        assert_eq!(PLUGIN_RATE_LIMITED_ERROR_CODE, -32011);
        assert_eq!(PLUGIN_CONFLICT_ERROR_CODE, -32012);
        assert_eq!(RETRY_AFTER_MS_PREFIX, "retry_after_ms=");
    }

    #[test]
    fn rate_limited_plugin_error_maps_to_retryable_with_retry_after() {
        // #444: a 429 with a carried Retry-After must reach
        // SyncError::RateLimited with the delay recovered (converted from
        // the wire's milliseconds to RateLimited's seconds field), not the
        // fixed ServerError{503} the general transient code produces.
        let rate_limited = rpc_error_to_sync_error(
            "http-sql",
            &RpcError {
                code: PLUGIN_RATE_LIMITED_ERROR_CODE,
                message: "retry_after_ms=30000;429 from backend".into(),
            },
        );
        assert!(rate_limited.is_retryable());
        assert_eq!(rate_limited.exit_code(), 3);
        match rate_limited {
            SyncError::RateLimited { retry_after } => assert_eq!(retry_after, Some(30)),
            other => panic!("expected RateLimited, got {other:?}"),
        }
    }

    #[test]
    fn rate_limited_plugin_error_rounds_up_a_sub_second_retry_after() {
        let rate_limited = rpc_error_to_sync_error(
            "http-sql",
            &RpcError {
                code: PLUGIN_RATE_LIMITED_ERROR_CODE,
                message: "retry_after_ms=500;429 from backend".into(),
            },
        );
        match rate_limited {
            SyncError::RateLimited { retry_after } => assert_eq!(retry_after, Some(1)),
            other => panic!("expected RateLimited, got {other:?}"),
        }
    }

    #[test]
    fn rate_limited_plugin_error_without_retry_after_still_retries() {
        // No Retry-After on the response: still classified rate-limited and
        // retryable, just without a recovered delay -- the host falls back
        // to its own backoff schedule.
        let rate_limited = rpc_error_to_sync_error(
            "http-sql",
            &RpcError {
                code: PLUGIN_RATE_LIMITED_ERROR_CODE,
                message: "429 from backend".into(),
            },
        );
        assert!(rate_limited.is_retryable());
        match rate_limited {
            SyncError::RateLimited { retry_after } => assert_eq!(retry_after, None),
            other => panic!("expected RateLimited, got {other:?}"),
        }
    }

    #[test]
    fn conflict_plugin_error_maps_to_exit_code_four() {
        // #444, #269: a duplicate-PK collision on an http-sql target must
        // exit 4, matching the native and direct paths, not collapse into
        // the general/unknown bucket (exit 1) every other plugin error uses.
        let conflict = rpc_error_to_sync_error(
            "http-sql",
            &RpcError {
                code: PLUGIN_CONFLICT_ERROR_CODE,
                message: "duplicate primary key '1' in table 'items'".into(),
            },
        );
        assert!(!conflict.is_retryable());
        assert_eq!(conflict.exit_code(), 4);
        match conflict {
            SyncError::PluginConflict { plugin, message } => {
                assert_eq!(plugin, "http-sql");
                assert_eq!(message, "duplicate primary key '1' in table 'items'");
            }
            other => panic!("expected PluginConflict, got {other:?}"),
        }
    }

    #[test]
    fn transient_plugin_error_maps_to_retryable() {
        // A plugin signalling the transient code becomes a retryable ServerError
        // (so upsert_with_retry backs off), exit code 3.
        let transient = rpc_error_to_sync_error(
            "turso",
            &RpcError {
                code: PLUGIN_TRANSIENT_ERROR_CODE,
                message: "429 from backend".into(),
            },
        );
        assert!(transient.is_retryable());
        assert_eq!(transient.exit_code(), 3);

        // Any other code stays a permanent Plugin error.
        let permanent = rpc_error_to_sync_error(
            "turso",
            &RpcError {
                code: -32000,
                message: "bad sql".into(),
            },
        );
        assert!(!permanent.is_retryable());
        assert!(matches!(permanent, SyncError::Plugin(_)));
    }
}
