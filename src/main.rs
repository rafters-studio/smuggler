//! # Smuggler
//!
//! Smuggle data between SQLite and Cloudflare D1.
//!
//! A fast, stateless CLI tool for bidirectional synchronization between
//! local SQLite databases and Cloudflare D1.
//!
//! ## Features
//!
//! - **True change detection** - Compares actual row content via SHA256 hashing
//! - **Delta sync** - Only transfers rows that differ
//! - **Bidirectional** - Push, pull, or both with configurable conflict resolution
//! - **No state files** - Every run compares live data
//!
//! ## Architecture
//!
//! - [`config`] - Configuration loading from TOML
//! - [`local`] - Local SQLite database operations
//! - [`remote`] - Cloudflare D1 HTTP API client
//! - [`diff`] - Change detection algorithm
//! - [`sync`] - Push/pull orchestration
//! - [`error`] - Error types
//! - [`table`] - Table name validation
//! - [`timestamp`] - Timestamp parsing and comparison
//! - [`batch`] - Batch operations for multi-row upserts

mod batch;
mod config;
mod diff;
mod error;
mod local;
mod remote;
mod sync;
mod table;
mod timestamp;

use crate::config::Config;
use crate::diff::diff_table;
use crate::local::LocalDb;
use crate::remote::D1Client;
use crate::sync::{get_tables_to_sync, pull_all, push_all};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser)]
#[command(name = "smuggler")]
#[command(
    author,
    version,
    about = "Smuggle data between SQLite and Cloudflare D1"
)]
struct Cli {
    /// Path to config file
    #[arg(short, long, default_value = "config.toml")]
    config: PathBuf,

    /// Enable verbose output
    #[arg(short, long)]
    verbose: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Push local changes to D1 (local -> remote)
    Push {
        /// Specific table to push (default: all configured tables)
        #[arg(short, long)]
        table: Option<String>,

        /// Show what would be pushed without actually pushing
        #[arg(long)]
        dry_run: bool,
    },

    /// Pull remote changes to local (D1 -> local)
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
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Set up logging
    let level = if cli.verbose {
        Level::DEBUG
    } else {
        Level::INFO
    };

    let subscriber = FmtSubscriber::builder()
        .with_max_level(level)
        .with_target(false)
        .compact()
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("Failed to set tracing subscriber");

    // Load config
    let config = match Config::load(&cli.config) {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to load config from {}: {}", cli.config.display(), e);
            std::process::exit(1);
        }
    };

    // Execute command
    let result = match cli.command {
        Commands::Push { table, dry_run } => run_push(&config, table, dry_run).await,
        Commands::Pull { table, dry_run } => run_pull(&config, table, dry_run).await,
        Commands::Diff { table } => run_diff(&config, table).await,
        Commands::Status => run_status(&config).await,
    };

    if let Err(e) = result {
        error!("Error: {}", e);
        std::process::exit(1);
    }
}

async fn run_push(config: &Config, table: Option<String>, dry_run: bool) -> error::Result<()> {
    info!("Push mode: local -> D1");

    let local = LocalDb::open_readonly(config.local_db_path())?;
    let remote = D1Client::with_retry_config(
        config.cloudflare_account_id.clone(),
        config.database_id.clone(),
        config.cloudflare_api_token.clone(),
        config.retry_config(),
    );

    // Test connection
    remote.test_connection().await?;

    let tables = table.map(|t| vec![t]);
    let results = push_all(&local, &remote, config, tables, dry_run).await?;

    // Print summary
    println!("\n--- Push Summary ---");
    let mut total_pushed = 0;
    for result in &results {
        if result.has_changes() {
            println!("  {}: {} rows pushed", result.table, result.rows_pushed);
            total_pushed += result.rows_pushed;
        }
    }

    if total_pushed == 0 {
        println!("  No changes to push");
    } else if dry_run {
        println!("\n  (dry run - no actual changes made)");
    }

    Ok(())
}

async fn run_pull(config: &Config, table: Option<String>, dry_run: bool) -> error::Result<()> {
    info!("Pull mode: D1 -> local");

    let mut local = LocalDb::open(config.local_db_path())?;
    let remote = D1Client::with_retry_config(
        config.cloudflare_account_id.clone(),
        config.database_id.clone(),
        config.cloudflare_api_token.clone(),
        config.retry_config(),
    );

    // Test connection
    remote.test_connection().await?;

    let tables = table.map(|t| vec![t]);
    let results = pull_all(&mut local, &remote, config, tables, dry_run).await?;

    // Print summary
    println!("\n--- Pull Summary ---");
    let mut total_pulled = 0;
    for result in &results {
        if result.has_changes() {
            println!("  {}: {} rows pulled", result.table, result.rows_pulled);
            total_pulled += result.rows_pulled;
        }
    }

    if total_pulled == 0 {
        println!("  No changes to pull");
    } else if dry_run {
        println!("\n  (dry run - no actual changes made)");
    }

    Ok(())
}

async fn run_diff(config: &Config, table: Option<String>) -> error::Result<()> {
    info!("Computing differences...");

    let local = LocalDb::open_readonly(config.local_db_path())?;
    let remote = D1Client::with_retry_config(
        config.cloudflare_account_id.clone(),
        config.database_id.clone(),
        config.cloudflare_api_token.clone(),
        config.retry_config(),
    );

    // Test connection
    remote.test_connection().await?;

    let tables = match table {
        Some(t) => vec![t],
        None => get_tables_to_sync(&local, &remote, config).await?,
    };

    println!("\n--- Differences ---");

    let mut has_any_changes = false;

    for table_name in &tables {
        let diff = diff_table(&local, &remote, table_name, &config.sync.timestamp_column).await?;

        if diff.has_changes() {
            has_any_changes = true;
            println!("\n{}", table_name);
            println!("  {}", diff.summary());

            // Show details
            if !diff.local_only.is_empty() {
                println!(
                    "    Local only: {}",
                    diff.local_only
                        .iter()
                        .take(5)
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                if diff.local_only.len() > 5 {
                    println!("      ... and {} more", diff.local_only.len() - 5);
                }
            }

            if !diff.remote_only.is_empty() {
                println!(
                    "    Remote only: {}",
                    diff.remote_only
                        .iter()
                        .take(5)
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                if diff.remote_only.len() > 5 {
                    println!("      ... and {} more", diff.remote_only.len() - 5);
                }
            }

            if !diff.local_newer.is_empty() {
                println!(
                    "    Local newer: {}",
                    diff.local_newer
                        .iter()
                        .take(5)
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                if diff.local_newer.len() > 5 {
                    println!("      ... and {} more", diff.local_newer.len() - 5);
                }
            }

            if !diff.remote_newer.is_empty() {
                println!(
                    "    Remote newer: {}",
                    diff.remote_newer
                        .iter()
                        .take(5)
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                if diff.remote_newer.len() > 5 {
                    println!("      ... and {} more", diff.remote_newer.len() - 5);
                }
            }

            if !diff.content_differs.is_empty() {
                println!(
                    "    Content differs: {}",
                    diff.content_differs
                        .iter()
                        .take(5)
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                if diff.content_differs.len() > 5 {
                    println!("      ... and {} more", diff.content_differs.len() - 5);
                }
            }
        } else {
            println!("\n{}: in sync ({} rows)", table_name, diff.identical.len());
        }
    }

    if !has_any_changes {
        println!("\nAll tables are in sync!");
    }

    Ok(())
}

async fn run_status(config: &Config) -> error::Result<()> {
    println!("--- Configuration ---");
    println!("  Config file: loaded");
    println!("  Local DB: {}", config.local_db_path());
    println!(
        "  Account ID: {}...",
        &config.cloudflare_account_id[..8.min(config.cloudflare_account_id.len())]
    );
    println!(
        "  Database ID: {}...",
        &config.database_id[..8.min(config.database_id.len())]
    );
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

    // Test local connection
    println!("\n--- Local Database ---");
    match LocalDb::open_readonly(config.local_db_path()) {
        Ok(local) => {
            println!("  Connection: OK");
            let tables = local.list_tables()?;
            println!("  Tables: {}", tables.len());

            for table in &tables {
                if config.should_sync_table(table) {
                    let count = local.row_count(table)?;
                    println!("    {}: {} rows", table, count);
                }
            }
        }
        Err(e) => {
            println!("  Connection: FAILED - {}", e);
        }
    }

    // Test remote connection
    println!("\n--- Remote D1 ---");
    let remote = D1Client::with_retry_config(
        config.cloudflare_account_id.clone(),
        config.database_id.clone(),
        config.cloudflare_api_token.clone(),
        config.retry_config(),
    );

    match remote.test_connection().await {
        Ok(()) => {
            println!("  Connection: OK");
            let tables = remote.list_tables().await?;
            println!("  Tables: {}", tables.len());

            for table in &tables {
                if config.should_sync_table(table) {
                    let count = remote.row_count(table).await?;
                    println!("    {}: {} rows", table, count);
                }
            }
        }
        Err(e) => {
            println!("  Connection: FAILED - {}", e);
        }
    }

    Ok(())
}
