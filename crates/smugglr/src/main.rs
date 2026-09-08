//! Smuggler CLI binary.
//!
//! This is the command-line interface for smuggler. All core sync logic
//! lives in `smugglr_core`; this crate provides the CLI argument parsing,
//! progress display, and human/JSON output formatting.

mod broadcast;
mod migrate_cli;
mod output;
mod watch;

use migrate_cli::MigrateCommand;

use output::{
    CommandOutput, DiffOutput, DryRunOutput, DryRunTableOutput, DryRunVerboseTableOutput,
    ErrorOutput, OutputFormat, SnapshotListEntry, SnapshotListOutput, SnapshotOutput,
    SnapshotTableInfo, Status, StatusConfig, StatusDb, StatusOutput, StatusTable,
};
use serde_json::Value as JsonValue;
use smugglr_core::config::{Config, DuplicatePkPolicy, ResolvedTarget};
use smugglr_core::datasource::{DataSource, RowMeta, TableInfo};
use smugglr_core::diff::diff_table;
use smugglr_core::error;
use smugglr_core::local::LocalDb;
use smugglr_core::plugin::PluginDataSource;
use smugglr_core::sync::{
    get_tables_to_sync, pull_all, push_all, sync_all, NoProgress, SyncProgress, SyncResult,
};
use std::collections::HashMap;

use clap::{Parser, Subcommand};
use indicatif::{ProgressBar, ProgressStyle};
use std::path::PathBuf;
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

/// CLI progress reporter using indicatif progress bars.
struct IndicatifProgress {
    bar: std::sync::Mutex<Option<ProgressBar>>,
}

impl IndicatifProgress {
    fn new() -> Self {
        Self {
            bar: std::sync::Mutex::new(None),
        }
    }
}

impl SyncProgress for IndicatifProgress {
    fn on_transfer_start(&self, total_rows: usize, label: &str, table: &str) {
        let pb = ProgressBar::new(total_rows as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
                .expect("valid progress template"),
        );
        pb.set_message(format!("{} {}", label, table));
        *self.bar.lock().expect("mutex poisoned") = Some(pb);
    }

    fn on_batch_complete(&self, rows_in_batch: usize) {
        if let Some(ref pb) = *self.bar.lock().expect("mutex poisoned") {
            pb.inc(rows_in_batch as u64);
        }
    }

    fn on_transfer_finish(&self, total_rows: usize, label: &str) {
        if let Some(ref pb) = *self.bar.lock().expect("mutex poisoned") {
            pb.finish_with_message(format!("{} {} rows", label, total_rows));
        }
    }
}

fn make_progress(fmt: OutputFormat) -> Box<dyn SyncProgress> {
    if fmt == OutputFormat::Text {
        Box::new(IndicatifProgress::new())
    } else {
        Box::new(NoProgress)
    }
}

/// An opened sync target: either a local SQLite file or a running plugin.
///
/// Both `LocalDb` and `PluginDataSource` implement [`DataSource`], but the
/// trait uses RPITIT (not object-safe), so we unify them with this enum and
/// delegate each method. This lets `run_push`/`run_pull`/`run_sync`/`run_diff`
/// share a single engine call instead of duplicating a per-variant `match`.
enum TargetSource {
    Sqlite(LocalDb),
    Plugin(Box<PluginDataSource>),
}

impl TargetSource {
    /// Open the resolved target. `writable` selects read-write vs read-only for
    /// SQLite targets (plugins manage their own access mode).
    ///
    /// `duplicate_pk` comes from `[sync].duplicate_pk` and must match the policy
    /// the local side was opened with: the diff builds metadata for BOTH sides,
    /// so a target opened with a different policy would refuse a collision the
    /// local side tolerates (or vice versa) and the knob would read as
    /// half-applied (#269). Plugin targets are unaffected -- the plugin wire
    /// request carries no policy, so they always refuse.
    async fn open(
        target: &ResolvedTarget,
        writable: bool,
        duplicate_pk: DuplicatePkPolicy,
    ) -> error::Result<Self> {
        match target {
            ResolvedTarget::Sqlite { database } => {
                let db = if writable {
                    LocalDb::open(database)?
                } else {
                    LocalDb::open_readonly(database)?
                };
                Ok(TargetSource::Sqlite(db.with_duplicate_pk(duplicate_pk)))
            }
            ResolvedTarget::Plugin {
                path,
                name,
                config: plugin_config,
            } => {
                let plugin = PluginDataSource::start(path, name, plugin_config).await?;
                Ok(TargetSource::Plugin(Box::new(plugin)))
            }
        }
    }
}

impl DataSource for TargetSource {
    async fn list_tables(&self) -> error::Result<Vec<String>> {
        match self {
            TargetSource::Sqlite(db) => db.list_tables().await,
            TargetSource::Plugin(p) => p.list_tables().await,
        }
    }

    async fn table_info(&self, table: &str) -> error::Result<TableInfo> {
        match self {
            TargetSource::Sqlite(db) => db.table_info(table).await,
            TargetSource::Plugin(p) => p.table_info(table).await,
        }
    }

    async fn get_row_metadata(
        &self,
        table: &str,
        timestamp_column: &str,
        exclude_columns: &[String],
    ) -> error::Result<HashMap<String, RowMeta>> {
        match self {
            TargetSource::Sqlite(db) => {
                db.get_row_metadata(table, timestamp_column, exclude_columns)
                    .await
            }
            TargetSource::Plugin(p) => {
                p.get_row_metadata(table, timestamp_column, exclude_columns)
                    .await
            }
        }
    }

    async fn get_rows(
        &self,
        table: &str,
        pk_values: &[String],
    ) -> error::Result<Vec<HashMap<String, JsonValue>>> {
        match self {
            TargetSource::Sqlite(db) => db.get_rows(table, pk_values).await,
            TargetSource::Plugin(p) => p.get_rows(table, pk_values).await,
        }
    }

    async fn upsert_rows(
        &self,
        table: &str,
        rows: &[HashMap<String, JsonValue>],
    ) -> error::Result<usize> {
        match self {
            TargetSource::Sqlite(db) => db.upsert_rows(table, rows).await,
            TargetSource::Plugin(p) => p.upsert_rows(table, rows).await,
        }
    }

    async fn row_count(&self, table: &str) -> error::Result<usize> {
        match self {
            TargetSource::Sqlite(db) => db.row_count(table).await,
            TargetSource::Plugin(p) => p.row_count(table).await,
        }
    }
}

/// Emit the JSON form of a sync-style result, handling the dry-run vs plain
/// split. Returns `true` if it printed (Json mode); `false` means the caller
/// should render its own text summary.
fn emit_command_json(
    fmt: OutputFormat,
    command: &'static str,
    results: &[SyncResult],
    dry_run: bool,
    verbose: bool,
) -> bool {
    match fmt {
        OutputFormat::Json if dry_run => print_dry_run_json(command, results, verbose),
        OutputFormat::Json => {
            let out = CommandOutput::from_sync_results(command, results);
            println!(
                "{}",
                serde_json::to_string(&out).expect("CommandOutput serialization")
            );
        }
        OutputFormat::Text => return false,
    }
    true
}

/// Emit a push/pull/stash/retrieve result: JSON if requested, else a
/// one-line-per-table text summary. The heading/verb are derived from `command`.
fn emit_command_output(
    fmt: OutputFormat,
    command: &'static str,
    results: &[SyncResult],
    dry_run: bool,
    verbose: bool,
    row_accessor: impl Fn(&SyncResult) -> usize,
) {
    if emit_command_json(fmt, command, results, dry_run, verbose) {
        return;
    }
    print_summary(results, row_accessor, command, dry_run);
}

#[derive(Parser)]
#[command(name = "smugglr")]
#[command(author, version, about = "Smuggle data between SQLite-shaped things")]
#[command(after_help = "\
EXIT CODES:
  0  success
  1  general error
  2  configuration error (fix the config, do not retry)
  3  connection/network error (transient, safe to retry)
  4  conflict (needs a human decision -- pick a conflict_resolution)
  5  target not found (database missing or API unreachable)

Config is read from config.toml (override with --config). Use --output json \
for machine-readable output; the exit code is the scripting contract.")]
struct Cli {
    /// Path to config file
    #[arg(short, long, default_value = "config.toml")]
    config: PathBuf,

    /// Enable verbose output
    #[arg(short, long)]
    verbose: bool,

    /// Output format: text (default) or json
    #[arg(short, long, default_value = "text")]
    output: OutputFormat,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Push local changes to the remote target (local -> remote)
    Push {
        /// Specific table to push (default: all configured tables)
        #[arg(short, long)]
        table: Option<String>,

        /// Show what would be pushed without actually pushing
        #[arg(long)]
        dry_run: bool,
    },

    /// Pull changes from the remote target to local (remote -> local)
    Pull {
        /// Specific table to pull (default: all configured tables)
        #[arg(short, long)]
        table: Option<String>,

        /// Show what would be pulled without actually pulling
        #[arg(long)]
        dry_run: bool,
    },

    /// Show differences between local and remote
    Diff {
        /// Specific table to diff (default: all configured tables)
        #[arg(short, long)]
        table: Option<String>,
    },

    /// Show configuration and connection status
    Status,

    /// Bidirectional sync (push + pull in one operation)
    Sync {
        /// Specific table to sync (default: all configured tables)
        #[arg(short, long)]
        table: Option<String>,

        /// Show what would be synced without actually syncing
        #[arg(long)]
        dry_run: bool,
    },

    /// Stash local state to an S3-compatible relay (local -> S3)
    Stash {
        /// Specific table to stash (default: all configured tables)
        #[arg(short, long)]
        table: Option<String>,

        /// Show what would be stashed without actually uploading
        #[arg(long)]
        dry_run: bool,
    },

    /// Retrieve state from an S3-compatible relay (S3 -> local)
    Retrieve {
        /// Specific table to retrieve (default: all configured tables)
        #[arg(short, long)]
        table: Option<String>,

        /// Show what would be retrieved without actually applying
        #[arg(long)]
        dry_run: bool,
    },

    /// Watch for changes and sync periodically (daemon mode)
    Watch {
        /// Sync interval in seconds
        #[arg(short, long, default_value = "30", value_parser = clap::value_parser!(u64).range(1..=86400))]
        interval: u64,

        /// Show what would be synced without actually syncing
        #[arg(long)]
        dry_run: bool,
    },

    /// Create a point-in-time snapshot of the local database
    Snapshot {
        /// Show what would be snapshotted without uploading
        #[arg(long)]
        dry_run: bool,
    },

    /// List available snapshots
    Snapshots,

    /// Restore local database from a snapshot
    Restore {
        /// Timestamp of snapshot to restore (exact or closest before)
        timestamp: String,

        /// Show what would be restored without applying
        #[arg(long)]
        dry_run: bool,
    },

    /// LAN broadcast sync with peer discovery
    Broadcast {
        /// Override broadcast port
        #[arg(short = 'p', long)]
        port: Option<u16>,

        /// Sync interval in seconds
        #[arg(short, long, value_parser = clap::value_parser!(u64).range(1..=86400))]
        interval: Option<u64>,

        /// Run a single sync cycle and exit
        #[arg(long)]
        once: bool,

        /// Show what would sync without applying
        #[arg(long)]
        dry_run: bool,
    },

    /// Schema-and-data migrations (scaffold, apply, ...)
    Migrate {
        #[command(subcommand)]
        command: MigrateCommand,
    },
}

/// Print a JSON error and exit with the appropriate code.
fn exit_json_error(command: &'static str, err: &error::SyncError) -> ! {
    let out = ErrorOutput {
        command,
        status: Status::Error,
        error: err.to_string(),
        exit_code: err.exit_code(),
    };
    println!(
        "{}",
        serde_json::to_string(&out).expect("ErrorOutput serialization")
    );
    std::process::exit(err.exit_code());
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let fmt = cli.output;

    // Set up logging -- suppress tracing output in JSON mode so stdout is clean
    let level = match fmt {
        OutputFormat::Json => Level::WARN,
        OutputFormat::Text if cli.verbose => Level::DEBUG,
        OutputFormat::Text => Level::INFO,
    };

    let subscriber = FmtSubscriber::builder()
        .with_max_level(level)
        .with_target(false)
        .with_writer(std::io::stderr)
        .compact()
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("Failed to set tracing subscriber");

    // Determine command name for JSON output
    let command_name: &'static str = match &cli.command {
        Commands::Push { .. } => "push",
        Commands::Pull { .. } => "pull",
        Commands::Sync { .. } => "sync",
        Commands::Diff { .. } => "diff",
        Commands::Status => "status",
        Commands::Stash { .. } => "stash",
        Commands::Retrieve { .. } => "retrieve",
        Commands::Snapshot { .. } => "snapshot",
        Commands::Snapshots => "snapshots",
        Commands::Restore { .. } => "restore",
        Commands::Watch { .. } => "watch",
        Commands::Broadcast { .. } => "broadcast",
        Commands::Migrate { .. } => "migrate",
    };

    // Migrate commands scaffold and inspect migration manifests; they need
    // neither the sync config nor a resolved target, so they dispatch here --
    // before the config load every sync command requires (scaffolding a
    // migration must work in a fresh project with no config.toml yet).
    if let Commands::Migrate { command } = &cli.command {
        if let Err(e) = migrate_cli::run(command, fmt) {
            match fmt {
                OutputFormat::Json => exit_json_error(command_name, &e),
                OutputFormat::Text => error!("Error: {}", e),
            }
            std::process::exit(e.exit_code());
        }
        return;
    }

    // Load config
    let config = match Config::load(&cli.config) {
        Ok(c) => c,
        Err(e) => {
            match fmt {
                OutputFormat::Json => exit_json_error(command_name, &e),
                OutputFormat::Text => {
                    error!("Failed to load config from {}: {}", cli.config.display(), e)
                }
            }
            std::process::exit(e.exit_code());
        }
    };

    // Resolve target once upfront (stash/retrieve/broadcast don't need it)
    let config_path = cli.config.clone();
    let target = match &cli.command {
        Commands::Stash { .. }
        | Commands::Retrieve { .. }
        | Commands::Snapshot { .. }
        | Commands::Snapshots
        | Commands::Restore { .. }
        | Commands::Broadcast { .. } => None,
        _ => Some(config.resolve_target().unwrap_or_else(|e| {
            match fmt {
                OutputFormat::Json => exit_json_error(command_name, &e),
                OutputFormat::Text => error!("Failed to resolve target: {}", e),
            }
            std::process::exit(e.exit_code());
        })),
    };

    // Execute command
    let result = match cli.command {
        Commands::Push { table, dry_run } => {
            run_push(
                &config,
                target.expect("target resolved"),
                table,
                dry_run,
                fmt,
                cli.verbose,
            )
            .await
        }
        Commands::Pull { table, dry_run } => {
            run_pull(
                &config,
                target.expect("target resolved"),
                table,
                dry_run,
                fmt,
                cli.verbose,
            )
            .await
        }
        Commands::Sync { table, dry_run } => {
            run_sync(
                &config,
                target.expect("target resolved"),
                table,
                dry_run,
                fmt,
                cli.verbose,
            )
            .await
        }
        Commands::Diff { table } => {
            run_diff(&config, target.expect("target resolved"), table, fmt).await
        }
        Commands::Status => run_status(&config, target.expect("target resolved"), fmt).await,
        Commands::Stash { table, dry_run } => {
            run_stash(&config, table, dry_run, fmt, cli.verbose).await
        }
        Commands::Retrieve { table, dry_run } => {
            run_retrieve(&config, table, dry_run, fmt, cli.verbose).await
        }
        Commands::Snapshot { dry_run } => run_snapshot(&config, dry_run, fmt).await,
        Commands::Snapshots => run_snapshots(&config, fmt).await,
        Commands::Restore { timestamp, dry_run } => {
            run_restore(&config, &timestamp, dry_run, fmt).await
        }
        Commands::Watch { interval, dry_run } => {
            watch::run_watch(
                &config,
                &config_path,
                target.expect("target resolved"),
                interval,
                dry_run,
                fmt,
            )
            .await
        }
        Commands::Broadcast {
            port,
            interval,
            once,
            dry_run,
        } => {
            let mut bc = config
                .broadcast
                .clone()
                .unwrap_or_else(smugglr_core::broadcast::BroadcastConfig::default);
            if let Some(p) = port {
                bc.port = p;
            }
            if let Some(i) = interval {
                bc.interval_secs = i;
            }
            broadcast::run_broadcast(&config, &config_path, &bc, once, dry_run).await
        }
        // Migrate is dispatched above, before config load, since it needs no
        // config or resolved target; it never reaches this match.
        Commands::Migrate { .. } => unreachable!("migrate dispatched before config load"),
    };

    if let Err(e) = result {
        match fmt {
            OutputFormat::Json => exit_json_error(command_name, &e),
            OutputFormat::Text => error!("Error: {}", e),
        }
        std::process::exit(e.exit_code());
    }
}

fn print_dry_run_json(
    command: &'static str,
    results: &[smugglr_core::sync::SyncResult],
    verbose: bool,
) {
    if verbose {
        let out = DryRunOutput::<DryRunVerboseTableOutput>::from_sync_results(command, results);
        println!(
            "{}",
            serde_json::to_string(&out).expect("DryRunOutput serialization")
        );
    } else {
        let out = DryRunOutput::<DryRunTableOutput>::from_sync_results(command, results);
        println!(
            "{}",
            serde_json::to_string(&out).expect("DryRunOutput serialization")
        );
    }
}

/// Resolve table filter from CLI --table arg using local schema validation.
fn resolve_tables(local: &LocalDb, table: Option<String>) -> error::Result<Option<Vec<String>>> {
    match table {
        Some(t) => {
            let schema = local.get_schema()?;
            schema.validate(&t)?;
            Ok(Some(vec![t]))
        }
        None => Ok(None),
    }
}

async fn run_push(
    config: &Config,
    target: ResolvedTarget,
    table: Option<String>,
    dry_run: bool,
    fmt: OutputFormat,
    verbose: bool,
) -> error::Result<()> {
    let local =
        LocalDb::open_readonly(config.local_db_path())?.with_duplicate_pk(config.sync.duplicate_pk);
    let tables = resolve_tables(&local, table)?;
    let progress = make_progress(fmt);

    match &target {
        ResolvedTarget::Sqlite { database } => info!("Push mode: local -> SQLite ({})", database),
        ResolvedTarget::Plugin { name, .. } => info!("Push mode: local -> plugin ({})", name),
    }
    let target = TargetSource::open(&target, true, config.sync.duplicate_pk).await?;
    let results = push_all(&local, &target, config, tables, dry_run, progress.as_ref()).await?;

    emit_command_output(fmt, "push", &results, dry_run, verbose, |r| r.rows_pushed);
    Ok(())
}

async fn run_pull(
    config: &Config,
    target: ResolvedTarget,
    table: Option<String>,
    dry_run: bool,
    fmt: OutputFormat,
    verbose: bool,
) -> error::Result<()> {
    let local = if dry_run {
        LocalDb::open_readonly(config.local_db_path())?.with_duplicate_pk(config.sync.duplicate_pk)
    } else {
        LocalDb::open(config.local_db_path())?.with_duplicate_pk(config.sync.duplicate_pk)
    };
    let tables = resolve_tables(&local, table)?;
    let progress = make_progress(fmt);

    match &target {
        ResolvedTarget::Sqlite { database } => info!("Pull mode: SQLite ({}) -> local", database),
        ResolvedTarget::Plugin { name, .. } => info!("Pull mode: plugin ({}) -> local", name),
    }
    // Pull reads from the target, so it is opened read-only.
    let target = TargetSource::open(&target, false, config.sync.duplicate_pk).await?;
    let results = pull_all(&local, &target, config, tables, dry_run, progress.as_ref()).await?;

    emit_command_output(fmt, "pull", &results, dry_run, verbose, |r| r.rows_pulled);
    Ok(())
}

async fn run_sync(
    config: &Config,
    target: ResolvedTarget,
    table: Option<String>,
    dry_run: bool,
    fmt: OutputFormat,
    verbose: bool,
) -> error::Result<()> {
    let local = if dry_run {
        LocalDb::open_readonly(config.local_db_path())?.with_duplicate_pk(config.sync.duplicate_pk)
    } else {
        LocalDb::open(config.local_db_path())?.with_duplicate_pk(config.sync.duplicate_pk)
    };
    let tables = resolve_tables(&local, table)?;
    let progress = make_progress(fmt);

    match &target {
        ResolvedTarget::Sqlite { database } => {
            info!("Sync mode: bidirectional (local <-> SQLite {})", database)
        }
        ResolvedTarget::Plugin { name, .. } => {
            info!("Sync mode: bidirectional (local <-> plugin {})", name)
        }
    }
    let target = TargetSource::open(&target, true, config.sync.duplicate_pk).await?;
    let results = sync_all(&local, &target, config, tables, dry_run, progress.as_ref()).await?;

    // JSON output is identical to the other commands; only sync's text summary
    // is bespoke (per-table pushed/pulled counts), so it can't use print_summary.
    if emit_command_json(fmt, "sync", &results, dry_run, verbose) {
        return Ok(());
    }

    println!("\n--- Sync Summary ---");
    let mut total_pushed = 0;
    let mut total_pulled = 0;
    for result in &results {
        if result.has_changes() {
            println!(
                "  {}: {} pushed, {} pulled",
                result.table, result.rows_pushed, result.rows_pulled
            );
            total_pushed += result.rows_pushed;
            total_pulled += result.rows_pulled;
        }
    }
    if total_pushed == 0 && total_pulled == 0 {
        println!("  No changes to sync");
    } else if dry_run {
        println!("\n  (dry run - no actual changes made)");
    }

    Ok(())
}

async fn run_diff(
    config: &Config,
    target: ResolvedTarget,
    table: Option<String>,
    fmt: OutputFormat,
) -> error::Result<()> {
    info!("Computing differences...");
    let local =
        LocalDb::open_readonly(config.local_db_path())?.with_duplicate_pk(config.sync.duplicate_pk);
    let remote = TargetSource::open(&target, false, config.sync.duplicate_pk).await?;

    let tables = match table {
        Some(t) => {
            let schema = local.get_schema()?;
            schema.validate(&t)?;
            vec![t]
        }
        None => get_tables_to_sync(&local, &remote, config).await?,
    };

    output_diffs(
        &local,
        &remote,
        &tables,
        &config.sync.timestamp_column,
        &config.sync.exclude_columns,
        &config.sync.converge_columns,
        fmt,
    )
    .await
}

async fn output_diffs<A: DataSource, B: DataSource>(
    local: &A,
    remote: &B,
    tables: &[String],
    timestamp_column: &str,
    exclude_columns: &[String],
    converge_columns: &[String],
    fmt: OutputFormat,
) -> error::Result<()> {
    let mut diffs = Vec::new();
    for table_name in tables {
        let diff = diff_table(
            local,
            remote,
            table_name,
            timestamp_column,
            exclude_columns,
            converge_columns,
        )
        .await?;
        diffs.push((table_name.clone(), diff));
    }

    match fmt {
        OutputFormat::Json => {
            let out = DiffOutput::from_diffs(diffs);
            println!(
                "{}",
                serde_json::to_string(&out).expect("DiffOutput serialization")
            );
        }
        OutputFormat::Text => {
            println!("\n--- Differences ---");
            let mut has_any_changes = false;

            for (table_name, diff) in &diffs {
                if diff.has_changes() {
                    has_any_changes = true;
                    println!("\n{}", table_name);
                    println!("  {}", diff.summary());

                    print_diff_category("Local only", &diff.local_only);
                    print_diff_category("Remote only", &diff.remote_only);
                    print_diff_category("Local newer", &diff.local_newer);
                    print_diff_category("Remote newer", &diff.remote_newer);
                    print_diff_category("Content differs", &diff.content_differs);
                } else {
                    println!("\n{}: in sync ({} rows)", table_name, diff.identical.len());
                }
            }

            if !has_any_changes {
                println!("\nAll tables are in sync!");
            }
        }
    }

    Ok(())
}

/// Print a sync summary (push, pull, stash, or retrieve).
///
/// `verb` is the lowercase action name used in the no-changes message
/// (e.g. "No changes to push").
/// Capitalize the first character of an ASCII command verb ("push" -> "Push").
fn capitalize(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

fn print_summary(
    results: &[smugglr_core::sync::SyncResult],
    get_count: impl Fn(&smugglr_core::sync::SyncResult) -> usize,
    verb: &str,
    dry_run: bool,
) {
    println!("\n--- {} Summary ---", capitalize(verb));
    let mut total = 0;
    for result in results {
        let count = get_count(result);
        if count > 0 {
            println!("  {}: {} rows", result.table, count);
            total += count;
        }
    }

    if total == 0 {
        println!("  No changes to {}", verb);
    } else if dry_run {
        println!("\n  (dry run - no actual changes made)");
    }
}

/// Print a diff category (e.g. "Local only") with up to 5 sample keys.
fn print_diff_category(label: &str, keys: &[String]) {
    if keys.is_empty() {
        return;
    }
    let preview: Vec<_> = keys.iter().take(5).map(String::as_str).collect();
    println!("    {}: {}", label, preview.join(", "));
    if keys.len() > 5 {
        println!("      ... and {} more", keys.len() - 5);
    }
}

/// Collect per-table row counts for `status` from an already-open data source.
///
/// The connection has already succeeded by the time this is called, so a
/// post-connection failure (locked table, transient plugin RPC error, etc.)
/// must NOT abort the whole status report -- `status` is designed to degrade
/// gracefully and always report both sides. A list_tables/row_count failure
/// here is captured as `connected: true` with an `error` string and whatever
/// table counts were gathered before the failure, mirroring the disconnected
/// `StatusDb` the caller builds when `open` itself fails (#192).
async fn gather_status<D: DataSource>(db: &D, config: &Config) -> StatusDb {
    let tables = match db.list_tables().await {
        Ok(tables) => tables,
        Err(e) => {
            return StatusDb {
                connected: true,
                error: Some(e.to_string()),
                tables: vec![],
            }
        }
    };
    let mut table_rows = Vec::new();
    for table in &tables {
        if config.should_sync_table(table) {
            match db.row_count(table).await {
                Ok(count) => table_rows.push(StatusTable {
                    name: table.clone(),
                    rows: count,
                }),
                Err(e) => {
                    return StatusDb {
                        connected: true,
                        error: Some(e.to_string()),
                        tables: table_rows,
                    }
                }
            }
        }
    }
    StatusDb {
        connected: true,
        error: None,
        tables: table_rows,
    }
}

/// The endpoint a resolved target will actually talk to, when there is one.
///
/// For a `d1` target the URL is derived from `account_id` and `database_id`
/// (#429), so it appears in no file the operator wrote. `status` shows it, which
/// is the only place a wrong account or database is visible before a push goes
/// somewhere unintended. Returns only the URL -- the same plugin config carries
/// the auth token, which stays out of every output.
fn plugin_endpoint(target: &ResolvedTarget) -> Option<String> {
    match target {
        ResolvedTarget::Sqlite { .. } => None,
        ResolvedTarget::Plugin { config, .. } => config.get("url").cloned(),
    }
}

async fn run_status(
    config: &Config,
    target: ResolvedTarget,
    fmt: OutputFormat,
) -> error::Result<()> {
    let target_type = match &target {
        ResolvedTarget::Sqlite { .. } => "sqlite",
        ResolvedTarget::Plugin { ref name, .. } => name.as_str(),
    };

    let target_url = plugin_endpoint(&target);

    // Gather local DB info
    let local_status = match LocalDb::open_readonly(config.local_db_path()) {
        Ok(local) => gather_status(&local, config).await,
        Err(e) => StatusDb {
            connected: false,
            error: Some(e.to_string()),
            tables: vec![],
        },
    };

    // Gather target info
    let target_status = match TargetSource::open(&target, false, config.sync.duplicate_pk).await {
        Ok(remote) => gather_status(&remote, config).await,
        Err(e) => StatusDb {
            connected: false,
            error: Some(e.to_string()),
            tables: vec![],
        },
    };

    match fmt {
        OutputFormat::Json => {
            let out = StatusOutput {
                command: "status",
                status: Status::Ok,
                config: StatusConfig {
                    local_db: config.local_db_path().to_string(),
                    target_type: target_type.to_string(),
                    target_url: target_url.clone(),
                    timestamp_column: config.sync.timestamp_column.clone(),
                    conflict_resolution: format!("{:?}", config.sync.conflict_resolution),
                    tables: config.sync.tables.clone(),
                    exclude_tables: config.sync.exclude_tables.clone(),
                },
                local: local_status,
                target: target_status,
            };
            println!(
                "{}",
                serde_json::to_string(&out).expect("StatusOutput serialization")
            );
        }
        OutputFormat::Text => {
            println!("--- Configuration ---");
            println!("  Config file: loaded");
            println!("  Local DB: {}", config.local_db_path());

            match &target {
                ResolvedTarget::Sqlite { database } => {
                    println!("  Target: SQLite ({})", database);
                }
                ResolvedTarget::Plugin {
                    ref name, ref path, ..
                } => {
                    println!("  Target: Plugin ({})", name);
                    println!("  Plugin path: {}", path.display());
                    if let Some(url) = &target_url {
                        println!("  Endpoint: {}", url);
                    }
                }
            }

            println!("  Timestamp column: {}", config.sync.timestamp_column);
            println!(
                "  Conflict resolution: {:?}",
                config.sync.conflict_resolution
            );

            if !config.sync.tables.is_empty() {
                println!("  Tables (explicit): {}", config.sync.tables.join(", "));
            }
            if !config.sync.exclude_tables.is_empty() {
                println!(
                    "  Excluded tables: {}",
                    config.sync.exclude_tables.join(", ")
                );
            }

            // Local DB
            println!("\n--- Local Database ---");
            if local_status.connected {
                println!("  Connection: OK");
                println!("  Tables: {}", local_status.tables.len());
                for t in &local_status.tables {
                    println!("    {}: {} rows", t.name, t.rows);
                }
            } else {
                println!(
                    "  Connection: FAILED - {}",
                    local_status.error.as_deref().unwrap_or("unknown")
                );
            }

            // Target
            match &target {
                ResolvedTarget::Sqlite { .. } => println!("\n--- Target SQLite ---"),
                ResolvedTarget::Plugin { ref name, .. } => {
                    println!("\n--- Target Plugin ({}) ---", name)
                }
            }
            if target_status.connected {
                println!("  Connection: OK");
                println!("  Tables: {}", target_status.tables.len());
                for t in &target_status.tables {
                    println!("    {}: {} rows", t.name, t.rows);
                }
            } else {
                println!(
                    "  Connection: FAILED - {}",
                    target_status.error.as_deref().unwrap_or("unknown")
                );
            }
        }
    }

    Ok(())
}

fn require_stash_config(config: &Config) -> error::Result<&smugglr_core::config::StashConfig> {
    config
        .stash
        .as_ref()
        .ok_or_else(|| error::SyncError::Config("No [stash] section in config".into()))
}

async fn run_stash(
    config: &Config,
    table: Option<String>,
    dry_run: bool,
    fmt: OutputFormat,
    verbose: bool,
) -> error::Result<()> {
    let stash_config = require_stash_config(config)?;
    info!("Stash mode: local -> S3 relay");

    let results = smugglr_core::stash::stash(
        stash_config,
        &config.sync,
        config.local_db_path(),
        table,
        dry_run,
    )
    .await?;

    emit_command_output(fmt, "stash", &results, dry_run, verbose, |r| r.rows_pushed);
    Ok(())
}

async fn run_retrieve(
    config: &Config,
    table: Option<String>,
    dry_run: bool,
    fmt: OutputFormat,
    verbose: bool,
) -> error::Result<()> {
    let stash_config = require_stash_config(config)?;
    info!("Retrieve mode: S3 relay -> local");

    let results = smugglr_core::stash::retrieve(
        stash_config,
        &config.sync,
        config.local_db_path(),
        table,
        dry_run,
    )
    .await?;

    emit_command_output(fmt, "retrieve", &results, dry_run, verbose, |r| {
        r.rows_pulled
    });
    Ok(())
}

async fn run_snapshot(config: &Config, dry_run: bool, fmt: OutputFormat) -> error::Result<()> {
    let stash_config = require_stash_config(config)?;
    info!("Snapshot mode: local -> S3 relay");

    let result =
        smugglr_core::snapshot::snapshot(stash_config, config.local_db_path(), dry_run).await?;

    match fmt {
        OutputFormat::Json => {
            let out = SnapshotOutput {
                command: "snapshot",
                status: if dry_run { Status::DryRun } else { Status::Ok },
                timestamp: result.timestamp,
                size_bytes: result.size_bytes,
                tables: result
                    .tables
                    .into_iter()
                    .map(|t| SnapshotTableInfo {
                        name: t.name,
                        row_count: t.row_count,
                    })
                    .collect(),
            };
            println!(
                "{}",
                serde_json::to_string(&out).expect("SnapshotOutput serialization")
            );
        }
        OutputFormat::Text => {
            println!("\n--- Snapshot ---");
            println!("  Timestamp: {}", result.timestamp);
            println!("  Size: {} bytes", result.size_bytes);
            for t in &result.tables {
                println!("  {}: {} rows", t.name, t.row_count);
            }
            if dry_run {
                println!("\n  (dry run - no snapshot created)");
            }
        }
    }
    Ok(())
}

async fn run_snapshots(config: &Config, fmt: OutputFormat) -> error::Result<()> {
    let stash_config = require_stash_config(config)?;
    info!("Listing snapshots");

    let entries = smugglr_core::snapshot::list_snapshots(stash_config).await?;

    match fmt {
        OutputFormat::Json => {
            let out = SnapshotListOutput {
                command: "snapshots",
                status: Status::Ok,
                snapshots: entries
                    .into_iter()
                    .map(|e| SnapshotListEntry {
                        timestamp: e.timestamp,
                        size_bytes: e.size_bytes,
                        tables: e
                            .tables
                            .into_iter()
                            .map(|t| SnapshotTableInfo {
                                name: t.name,
                                row_count: t.row_count,
                            })
                            .collect(),
                    })
                    .collect(),
            };
            println!(
                "{}",
                serde_json::to_string(&out).expect("SnapshotListOutput serialization")
            );
        }
        OutputFormat::Text => {
            if entries.is_empty() {
                println!("No snapshots available");
            } else {
                println!("\n--- Snapshots ---");
                for entry in &entries {
                    let total_rows: usize = entry.tables.iter().map(|t| t.row_count).sum();
                    println!(
                        "  {} ({} bytes, {} tables, {} rows)",
                        entry.timestamp,
                        entry.size_bytes,
                        entry.tables.len(),
                        total_rows
                    );
                }
                println!("\n  {} snapshot(s) available", entries.len());
            }
        }
    }
    Ok(())
}

async fn run_restore(
    config: &Config,
    timestamp: &str,
    dry_run: bool,
    fmt: OutputFormat,
) -> error::Result<()> {
    let stash_config = require_stash_config(config)?;
    info!("Restore mode: S3 relay -> local");

    let result =
        smugglr_core::snapshot::restore(stash_config, config.local_db_path(), timestamp, dry_run)
            .await?;

    match fmt {
        OutputFormat::Json => {
            let out = SnapshotOutput {
                command: "restore",
                status: if dry_run { Status::DryRun } else { Status::Ok },
                timestamp: result.timestamp,
                size_bytes: result.size_bytes,
                tables: result
                    .tables
                    .into_iter()
                    .map(|t| SnapshotTableInfo {
                        name: t.name,
                        row_count: t.row_count,
                    })
                    .collect(),
            };
            println!(
                "{}",
                serde_json::to_string(&out).expect("SnapshotOutput serialization")
            );
        }
        OutputFormat::Text => {
            println!("\n--- Restore ---");
            println!("  Restored snapshot: {}", result.timestamp);
            println!("  Size: {} bytes", result.size_bytes);
            for t in &result.tables {
                println!("  {}: {} rows", t.name, t.row_count);
            }
            if dry_run {
                println!("\n  (dry run - no changes applied)");
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    /// Regression for #190/#194: `watch --interval 0` must be rejected at parse
    /// time rather than reaching `tokio::time::interval`, which panics on a zero
    /// period. The clap range also caps the upper bound that drives peer_ttl.
    #[test]
    fn watch_interval_zero_is_rejected_at_parse() {
        let result = Cli::try_parse_from(["smugglr", "watch", "--interval", "0"]);
        let err = match result {
            Ok(_) => panic!("interval 0 must be rejected"),
            Err(e) => e,
        };
        assert_eq!(err.kind(), clap::error::ErrorKind::ValueValidation);

        // A sane interval still parses.
        let cli = Cli::try_parse_from(["smugglr", "watch", "--interval", "30"])
            .unwrap_or_else(|e| panic!("interval 30 parses: {e}"));
        match cli.command {
            Commands::Watch { interval, .. } => assert_eq!(interval, 30),
            _ => panic!("expected watch command"),
        }
    }

    /// #270: `migrate new <name> <col...>` parses the migration name and the
    /// trailing column specs so the generator receives them verbatim.
    #[test]
    fn migrate_new_parses_name_and_columns() {
        let cli = Cli::try_parse_from([
            "smugglr",
            "migrate",
            "new",
            "create_contacts",
            "id:pk",
            "email:text:pii",
        ])
        .unwrap_or_else(|e| panic!("migrate new should parse: {e}"));
        match cli.command {
            Commands::Migrate {
                command: MigrateCommand::New { name, columns },
            } => {
                assert_eq!(name, "create_contacts");
                assert_eq!(
                    columns,
                    vec!["id:pk".to_string(), "email:text:pii".to_string()]
                );
            }
            _ => panic!("expected migrate new command"),
        }
    }

    /// Regression for #190/#194: `broadcast --interval 0` is likewise rejected at
    /// parse time; the same construction lives in run_broadcast's loop.
    #[test]
    fn broadcast_interval_zero_is_rejected_at_parse() {
        let result = Cli::try_parse_from(["smugglr", "broadcast", "--interval", "0"]);
        let err = match result {
            Ok(_) => panic!("interval 0 must be rejected"),
            Err(e) => e,
        };
        assert_eq!(err.kind(), clap::error::ErrorKind::ValueValidation);
    }

    /// A `DataSource` whose connection has succeeded (`list_tables` works) but
    /// whose `row_count` fails on a specific table -- modelling a locked table or
    /// a transient plugin RPC error after a successful open.
    struct FlakyRowCount {
        tables: Vec<String>,
        fail_on: &'static str,
    }

    impl DataSource for FlakyRowCount {
        async fn list_tables(&self) -> error::Result<Vec<String>> {
            Ok(self.tables.clone())
        }

        async fn table_info(&self, _table: &str) -> error::Result<TableInfo> {
            unreachable!("status does not call table_info")
        }

        async fn get_row_metadata(
            &self,
            _table: &str,
            _timestamp_column: &str,
            _exclude_columns: &[String],
        ) -> error::Result<HashMap<String, RowMeta>> {
            unreachable!("status does not call get_row_metadata")
        }

        async fn get_rows(
            &self,
            _table: &str,
            _pk_values: &[String],
        ) -> error::Result<Vec<HashMap<String, JsonValue>>> {
            unreachable!("status does not call get_rows")
        }

        async fn upsert_rows(
            &self,
            _table: &str,
            _rows: &[HashMap<String, JsonValue>],
        ) -> error::Result<usize> {
            unreachable!("status does not call upsert_rows")
        }

        async fn row_count(&self, table: &str) -> error::Result<usize> {
            if table == self.fail_on {
                Err(error::SyncError::Config(format!(
                    "table '{table}' is locked"
                )))
            } else {
                Ok(7)
            }
        }
    }

    fn test_config() -> Config {
        Config {
            cloudflare_account_id: None,
            cloudflare_api_token: None,
            database_id: None,
            local_db: Some("game.db".to_string()),
            sync: smugglr_core::config::SyncConfig::default(),
            stash: None,
            target: None,
            broadcast: None,
        }
    }

    /// Regression for #192: a post-connection failure (row_count erroring) must
    /// not abort the whole status report. `gather_status` reports the side as
    /// connected with an error string instead of bubbling the error out.
    #[tokio::test]
    async fn gather_status_reports_partial_on_row_count_error() {
        let db = FlakyRowCount {
            tables: vec!["abilities".into(), "items".into()],
            fail_on: "items",
        };
        let config = test_config();

        let status = gather_status(&db, &config).await;

        assert!(
            status.connected,
            "connection succeeded, so connected stays true"
        );
        assert!(
            status.error.is_some(),
            "the row_count failure must surface as an error string"
        );
        // The table counted before the failure is preserved.
        assert_eq!(status.tables.len(), 1);
        assert_eq!(status.tables[0].name, "abilities");
        assert_eq!(status.tables[0].rows, 7);
    }

    /// Companion to #192: when every table counts cleanly, the status is fully
    /// connected with no error.
    #[tokio::test]
    async fn gather_status_reports_all_tables_on_success() {
        let db = FlakyRowCount {
            tables: vec!["abilities".into(), "items".into()],
            fail_on: "__none__",
        };
        let config = test_config();

        let status = gather_status(&db, &config).await;

        assert!(status.connected);
        assert!(status.error.is_none());
        assert_eq!(status.tables.len(), 2);
    }
}

#[cfg(test)]
mod status_shows_the_endpoint {
    use super::*;
    use smugglr_core::config::d1_plugin_config;
    use std::path::PathBuf;

    fn plugin_target(config: std::collections::HashMap<String, String>) -> ResolvedTarget {
        ResolvedTarget::Plugin {
            path: PathBuf::from("/fake/smugglr-http-sql"),
            name: "smugglr-http-sql".to_string(),
            config,
        }
    }

    #[test]
    fn a_d1_target_shows_the_endpoint_it_derived() {
        // #429: the D1 URL is built from account_id and database_id and written
        // in no config file, so status is the only place an operator can see
        // that a push is aimed at the account they meant.
        let target = plugin_target(d1_plugin_config("acct", "db", "tok", None));
        assert_eq!(
            plugin_endpoint(&target).as_deref(),
            Some("https://api.cloudflare.com/client/v4/accounts/acct/d1/database/db/query")
        );
    }

    #[test]
    fn a_configured_endpoint_is_shown_as_configured() {
        let target = plugin_target(d1_plugin_config(
            "acct",
            "db",
            "tok",
            Some("https://do-bridge.example.workers.dev/query"),
        ));
        assert_eq!(
            plugin_endpoint(&target).as_deref(),
            Some("https://do-bridge.example.workers.dev/query")
        );
    }

    #[test]
    fn the_token_never_reaches_the_endpoint_line() {
        // The plugin config status reads from also carries the bearer token.
        // Only the url is surfaced, and this pins that.
        let target = plugin_target(d1_plugin_config("acct", "db", "s3cr3t", None));
        let shown = plugin_endpoint(&target).expect("a d1 target has an endpoint");
        assert!(
            !shown.contains("s3cr3t"),
            "status must not print the token: {shown}"
        );
    }

    #[test]
    fn a_sqlite_target_has_no_endpoint() {
        let target = ResolvedTarget::Sqlite {
            database: "backup.db".into(),
        };
        assert_eq!(plugin_endpoint(&target), None);
    }
}
