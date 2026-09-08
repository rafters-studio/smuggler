//! smugglr: SQLite replication across platforms.
//!
//! Core sync engine: content-hash diffing, delta computation, bidirectional
//! replication. Use as a library from Rust or compile to WASM for browser use.
//!
//! The `native` feature (default) enables platform-specific backends:
//! LocalDb (rusqlite), PluginDataSource, broadcast, stash, daemon. Remote
//! adapters like D1, turso, or rqlite live in the http-sql plugin and are
//! reached through `PluginDataSource`. Without `native`, only the diff/sync
//! engine and trait definitions are available.

pub mod batch_sql;
pub mod config;
pub mod datasource;
pub mod diff;
pub mod error;
pub mod http_sql;
pub mod migrate;
pub mod pk_check;
pub mod profile;
pub mod rowhash;
pub(crate) mod sql_ddl;
pub mod sync;
pub mod table;

#[cfg(feature = "native")]
pub mod broadcast;
#[cfg(feature = "native")]
pub mod daemon;
#[cfg(feature = "native")]
pub mod local;
#[cfg(feature = "native")]
pub mod multicast;
#[cfg(feature = "native")]
pub mod plugin;
#[cfg(feature = "native")]
pub mod snapshot;
#[cfg(feature = "native")]
pub mod stash;

pub use config::Config;
pub use datasource::DataSource;
pub use diff::{diff_table, DiffStats, TableDiff};
pub use error::{Result, SyncError};
pub use sync::{pull_all, push_all, sync_all, DiffDetail, NoProgress, SyncProgress, SyncResult};

#[cfg(feature = "native")]
pub use local::LocalDb;
#[cfg(feature = "native")]
pub use plugin::PluginDataSource;
