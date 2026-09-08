//! Configuration loading from TOML

use crate::error::{Result, SyncError};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tracing::{debug, info};

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// Legacy flat D1 fields (use [target] section instead for new configs)
    pub cloudflare_account_id: Option<String>,
    pub cloudflare_api_token: Option<String>,
    pub database_id: Option<String>,

    /// Path to local SQLite database (optional - auto-detected from wrangler if not set)
    pub local_db: Option<String>,

    #[serde(default)]
    pub sync: SyncConfig,

    /// Optional stash config for S3-compatible relay sync
    pub stash: Option<StashConfig>,

    /// Target database configuration (sqlite or d1)
    pub target: Option<TargetConfig>,

    /// LAN broadcast sync configuration
    #[cfg(feature = "native")]
    pub broadcast: Option<crate::broadcast::BroadcastConfig>,
}

/// Target database configuration
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum TargetConfig {
    /// Cloudflare D1 target (or any D1-compatible HTTP SQL endpoint)
    D1 {
        account_id: String,
        database_id: String,
        api_token: String,
        /// Custom endpoint URL. Omit it and the target resolves to Cloudflare's
        /// own D1 query endpoint for `account_id` and `database_id`; set it to
        /// point at a DO bridge or other D1-compatible endpoint instead.
        url: Option<String>,
    },
    /// Local SQLite target
    Sqlite { database: String },
    /// External plugin adapter
    Plugin {
        /// Plugin name (resolved from ~/.smugglr/plugins/smugglr-{name} or $PATH)
        name: Option<String>,
        /// Explicit path to plugin binary
        path: Option<String>,
        /// Plugin-specific configuration passed to initialize
        #[serde(default)]
        config: HashMap<String, String>,
    },
}

/// Resolved target after merging legacy fields with [target] section.
///
/// D1 config (both `[target] type = "d1"` and the legacy flat fields) resolves
/// to `Plugin` with a synthesized http-sql profile. The CLI and watch loop only
/// ever see `Sqlite` or `Plugin`; there is no dedicated D1 variant.
#[derive(Debug, Clone)]
pub enum ResolvedTarget {
    Sqlite {
        database: String,
    },
    Plugin {
        path: PathBuf,
        name: String,
        config: HashMap<String, String>,
    },
}

/// Configuration for S3-compatible relay sync (stash/retrieve).
///
/// Supports S3, R2, GCS, Azure, and local filesystem URLs.
#[derive(Debug, Clone, Deserialize)]
pub struct StashConfig {
    /// Object store URL: s3://bucket/path/relay.sqlite, file:///local/path, etc.
    pub url: String,

    /// AWS access key ID (optional if using instance roles or env vars)
    pub access_key_id: Option<String>,

    /// AWS secret access key
    pub secret_access_key: Option<String>,

    /// AWS region (default: us-east-1)
    pub region: Option<String>,

    /// Custom endpoint for R2, MinIO, etc.
    pub endpoint: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SyncConfig {
    /// Tables to sync (empty = all non-excluded tables)
    #[serde(default)]
    pub tables: Vec<String>,

    /// Tables to always exclude
    #[serde(default = "default_exclude_tables")]
    pub exclude_tables: Vec<String>,

    /// Column name patterns to exclude from sync entirely (glob-style:
    /// "*_embedding", "vector").
    ///
    /// These are stripped from the content hash, and from the rows that cross the
    /// wire **on the directional push/pull path**. Use for values that are
    /// derived, huge, or recomputed per node -- embeddings being the motivating
    /// case. If you need a column kept out of the hash but still *synced*, that
    /// is [`SyncConfig::converge_columns`], not this.
    ///
    /// # Stripping is NOT universal across transports -- do not read this as a
    /// privacy guarantee
    ///
    /// Only the directional path applies it. Two transports fetch and ship whole
    /// rows without consulting this list, so a column excluded here still leaves
    /// the machine on them:
    ///
    /// - **`stash` / `retrieve`** -- rows go to the S3-compatible relay unfiltered.
    /// - **`[broadcast]` multicast `Want` responses** -- a peer that asks for rows
    ///   receives every column (AEAD-sealed to the cluster key, but decrypted and
    ///   persisted by any peer holding it).
    ///
    /// Both predate `converge_columns` and are tracked separately; they are named
    /// here because the natural reading of "excluded from sync" is "never leaves
    /// the machine," and on those two paths that is currently false. If you are
    /// excluding a column for confidentiality rather than for size or
    /// recomputability, do not rely on this field alone today.
    #[serde(default)]
    pub exclude_columns: Vec<String>,

    /// Column name patterns excluded from the content hash but still synced
    /// (glob-style, same syntax as [`SyncConfig::exclude_columns`]).
    ///
    /// The distinction from `exclude_columns` is what crosses the wire. Both are
    /// omitted from the content hash; `exclude_columns` are also stripped from
    /// the transferred row, while these are transferred normally and converge by
    /// timestamp.
    ///
    /// # Why this exists (#293)
    ///
    /// Omitting a column from the hash means a change confined to that column
    /// produces a hash MATCH, and a hash match is the diff's skip condition. So
    /// an edit to an excluded-but-synced column was silently dropped: the row
    /// looked identical, was classified `identical`, and never transferred --
    /// even with a newer `updated_at`. For any table with a pattern in this
    /// list, a hash match is no longer treated as proof of equality; the diff
    /// falls through to comparing [`SyncConfig::timestamp_column`] and takes the
    /// newer row.
    ///
    /// # Every peer in a mesh must configure this identically
    ///
    /// The hash-exclusion set is local config, and it is never negotiated on the
    /// wire -- there is no handshake, no config fingerprint, and
    /// `PROTOCOL_VERSION` does not cover it. Two `[broadcast]` peers on the same
    /// group and table with DIFFERENT `converge_columns` therefore hash the same
    /// row over different column sets, and their hashes will essentially never
    /// coincide. The digest advertises a hash the peer can never match, the peer
    /// asks for the row on every heartbeat, and the mesh never quiesces: no
    /// error, no warning, just permanent Want/Delta churn and rows that read as
    /// divergent forever.
    ///
    /// This is not new to `converge_columns` -- `exclude_columns` has always had
    /// the same property, since it feeds the same hash. It is stated here because
    /// this field is the one that makes an operator think about the hash-input
    /// set for the first time, and because "converge" in the name invites exactly
    /// the wrong assumption. Roll a change to these lists out to every peer, and
    /// expect churn on any table where the rollout is partway through.
    ///
    /// # This is not automatic for existing configs
    ///
    /// Nothing moves columns here for you. A deployment relying on
    /// `exclude_columns` for a column it actually wants synced (a PII column
    /// kept out of the hash, say) keeps losing those edits until an operator
    /// moves the pattern into this list. That migration is deliberate: the two
    /// lists mean different things, and guessing which one a given pattern
    /// wanted would silently start transmitting a column an operator may have
    /// excluded precisely so it would never leave the machine.
    #[serde(default)]
    pub converge_columns: Vec<String>,

    /// Column used for timestamp-based change detection
    #[serde(default = "default_timestamp_column")]
    pub timestamp_column: String,

    /// How to resolve conflicts
    #[serde(default)]
    pub conflict_resolution: ConflictResolution,

    /// What to do when two rows in one table render the same `__pk` (#269).
    ///
    /// Defaults to [`DuplicatePkPolicy::Refuse`]. This is distinct from
    /// [`SyncConfig::conflict_resolution`], which decides between a local and a
    /// remote row that legitimately share a key: this field is about two rows
    /// **in the same table on the same node** colliding, which no resolution
    /// policy can fix because there is no second side to prefer.
    ///
    /// Scope: consulted by the diff/sync metadata builders. The multicast
    /// `on_delta` apply path does not build metadata, so a duplicate arriving
    /// as a Delta is not covered by this field (#278).
    #[serde(default)]
    pub duplicate_pk: DuplicatePkPolicy,

    /// Retry policy for transient failures. Flattened into the `[sync]` table,
    /// so the TOML keys stay `max_retries`, `initial_retry_delay_ms`,
    /// `max_retry_delay_ms`, and `backoff_multiplier`. These are the raw
    /// (unvalidated) values; [`RetryConfig::clamped`] applies the caps.
    #[serde(flatten)]
    pub retry: RetryConfig,

    /// Maximum number of rows per batch for upsert operations
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Maximum bytes per SQL statement (D1 has limits)
    #[serde(default = "default_max_statement_bytes")]
    pub max_statement_bytes: usize,
}

fn default_batch_size() -> usize {
    100
}

fn default_max_statement_bytes() -> usize {
    // D1 has a 100KB limit per statement, use 90KB to be safe
    90 * 1024
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            tables: Vec::new(),
            exclude_tables: default_exclude_tables(),
            exclude_columns: Vec::new(),
            converge_columns: Vec::new(),
            timestamp_column: default_timestamp_column(),
            conflict_resolution: ConflictResolution::default(),
            duplicate_pk: DuplicatePkPolicy::default(),
            retry: RetryConfig::default(),
            batch_size: default_batch_size(),
            max_statement_bytes: default_max_statement_bytes(),
        }
    }
}

impl SyncConfig {
    /// Every pattern omitted from the content hash: [`SyncConfig::exclude_columns`]
    /// plus [`SyncConfig::converge_columns`].
    ///
    /// One accessor rather than a union assembled at each call site, because
    /// every producer of a content hash MUST agree on this set. Two nodes (or
    /// two code paths on one node -- the diff and the multicast digest) that
    /// hash different column sets for the same row produce different hashes for
    /// identical data, and the row then reads as divergent on every sync,
    /// forever, with no error to point at. That is the #292 blob-encoding bug
    /// with a different cause, and splitting this union across call sites is how
    /// it would come back.
    ///
    /// Borrows in the common case: with no `converge_columns` configured this is
    /// exactly `exclude_columns` and allocates nothing.
    pub fn hash_excluded_columns(&self) -> std::borrow::Cow<'_, [String]> {
        hash_excluded_columns(&self.exclude_columns, &self.converge_columns)
    }

    /// Refuse a config where a column could match BOTH
    /// [`SyncConfig::exclude_columns`] and [`SyncConfig::converge_columns`].
    ///
    /// # Why this is a hard error rather than a precedence rule (#293)
    ///
    /// An overlapping column is not merely ambiguous, it silently destroys the
    /// edit it was configured to protect, and reports success while doing it:
    ///
    /// 1. The hash excludes it (the union covers both lists), so both sides hash
    ///    identically and the row looks unchanged.
    /// 2. `converge_columns` is non-empty, so the diff stops treating a hash
    ///    match as proof of equality and selects the row on its newer timestamp.
    /// 3. The transfer strips it, because stripping honors `exclude_columns`
    ///    alone -- so the row is sent WITHOUT the one column that caused it to be
    ///    sent.
    /// 4. The destination applies it, without the column. On the
    ///    `INSERT OR REPLACE` backends (http-sql, and any adapter using
    ///    `batch_sql::generate_batch_sql`) that is a DELETE+INSERT, so the
    ///    destination's value for that column is NULLED. The native apply path
    ///    leaves what it already stored alone (#324). Either way the edit does
    ///    not arrive.
    /// 5. `updated_at` did cross, so the timestamps now match. On the next sync
    ///    the hashes match and the timestamps tie, which resolves to `identical`.
    ///    The edit is gone permanently, and every step reported success.
    ///
    /// That is the bug #293 exists to fix, re-entered through the exact
    /// migration this feature documents -- an operator COPYING a pattern into
    /// `converge_columns` instead of MOVING it. So the config is refused at load
    /// rather than resolved by a precedence rule: any precedence answer is wrong
    /// for somebody (exclude-wins silently keeps losing the edit, converge-wins
    /// silently transmits a column an operator may have excluded precisely so it
    /// would never leave the machine), and this is cheap to state and impossible
    /// to hit by accident once stated.
    ///
    /// No existing deployment can trip this: `converge_columns` is new, so an
    /// overlap can only be introduced by a config written against this release.
    ///
    /// # Limits, stated rather than implied
    ///
    /// Glob-vs-glob intersection is not decidable in general, and this does not
    /// attempt it. It catches the realistic cases: an identical pattern in both
    /// lists, and a pattern in one list that matches the other's pattern text
    /// (`*email*` in `exclude_columns` against `email` in `converge_columns`).
    /// Two partially-overlapping globs (`email*` and `*mail`) pass this check and
    /// would still be ambiguous for the names in their intersection.
    pub fn validate_column_lists(&self) -> Result<()> {
        for converge in &self.converge_columns {
            for exclude in &self.exclude_columns {
                let overlaps = exclude == converge
                    || column_glob_match(exclude, converge)
                    || column_glob_match(converge, exclude);
                if overlaps {
                    return Err(SyncError::Config(format!(
                        "[sync] column pattern '{converge}' appears in converge_columns while \
                         '{exclude}' in exclude_columns also matches it. A column matching both \
                         is excluded from the content hash, selected for transfer by its \
                         timestamp, then stripped before it is sent -- the edit is lost and, on \
                         INSERT OR REPLACE backends, the destination's existing value is nulled. \
                         The two lists mean different things: exclude_columns never leaves the \
                         machine, converge_columns is synced but not hashed. MOVE the pattern \
                         into exactly one of them rather than copying it."
                    )));
                }
            }
        }
        Ok(())
    }
}

/// The hash-exclusion union, for callers holding the two lists rather than a
/// whole [`SyncConfig`]. See [`SyncConfig::hash_excluded_columns`] for why this
/// must have exactly one implementation.
pub fn hash_excluded_columns<'a>(
    exclude_columns: &'a [String],
    converge_columns: &[String],
) -> std::borrow::Cow<'a, [String]> {
    if converge_columns.is_empty() {
        return std::borrow::Cow::Borrowed(exclude_columns);
    }
    let mut all = exclude_columns.to_vec();
    all.extend(converge_columns.iter().cloned());
    std::borrow::Cow::Owned(all)
}

/// Check if a column name matches any exclusion pattern in the given list.
pub fn column_excluded(column: &str, patterns: &[String]) -> bool {
    patterns
        .iter()
        .any(|pattern| column_glob_match(pattern, column))
}

/// Simple glob matching for column name patterns.
///
/// Supports `*` at start, end, or both. No `?` or character classes --
/// these patterns are intentionally simple for config ergonomics.
fn column_glob_match(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    let starts_star = pattern.starts_with('*');
    let ends_star = pattern.ends_with('*');

    match (starts_star, ends_star) {
        (false, false) => {
            // Exact match: "vector"
            pattern == value
        }
        (true, true) => {
            // Contains: "*embed*". The pattern == "*" case is intercepted
            // above, so every pattern reaching here has len() >= 2 and the
            // slice below never panics.
            let inner = &pattern[1..pattern.len() - 1];
            value.contains(inner)
        }
        (true, false) => {
            // Suffix: "*_embedding"
            value.ends_with(&pattern[1..])
        }
        (false, true) => {
            // Prefix: "embedding_*"
            value.starts_with(&pattern[..pattern.len() - 1])
        }
    }
}

/// Configuration for batch operations
#[derive(Debug, Clone, Copy)]
pub struct BatchConfig {
    /// Maximum number of rows per batch
    pub batch_size: usize,
    /// Maximum bytes per SQL statement
    pub max_statement_bytes: usize,
    /// Retry policy for transient write failures
    pub retry: RetryConfig,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            batch_size: default_batch_size(),
            max_statement_bytes: default_max_statement_bytes(),
            retry: RetryConfig::default(),
        }
    }
}

impl BatchConfig {
    /// Create BatchConfig from SyncConfig
    pub fn from_sync_config(sync: &SyncConfig) -> Self {
        Self {
            batch_size: sync.batch_size,
            max_statement_bytes: sync.max_statement_bytes,
            retry: RetryConfig::clamped(&sync.retry),
        }
    }
}

fn default_max_retries() -> u32 {
    5
}

fn default_initial_retry_delay_ms() -> u64 {
    100 // per issue #3 spec
}

fn default_max_retry_delay_ms() -> u64 {
    30_000 // per issue #3 spec
}

fn default_backoff_multiplier() -> f64 {
    2.0
}

fn default_exclude_tables() -> Vec<String> {
    vec![
        "sqlite_sequence".to_string(),
        "_cf_KV".to_string(),
        "__drizzle_migrations".to_string(),
        // migrate's own ledger -- control-plane apply-state, invisible to
        // `validate` and app introspection/reset.
        //
        // A literal because `migrate::ledger` is native-gated and this module is
        // not. Pinned to `LEDGER_TABLE` by
        // `tests::the_excluded_ledger_name_is_the_ledger_name`, so a rename
        // that misses this line is a red build rather than a ledger that shows
        // up as user schema (#370).
        "_smugglr_migrations".to_string(),
    ]
}

fn default_timestamp_column() -> String {
    "updated_at".to_string()
}

/// What to do when two rows in one table render the same `__pk` text.
///
/// # Why this is a refusal and not a warning (#269)
///
/// smugglr's identity **is** the primary key -- every path matches rows by the
/// rendered `__pk`, so the metadata map is keyed by it. Two rows rendering the
/// same key means the map can only hold one of them: the second silently
/// evicts the first, and from that moment the evicted row is invisible to the
/// diff. It is never compared, never transferred, and -- because the
/// destination is reconciled against a metadata map that does not mention it --
/// it reads as a row that should not exist.
///
/// Under the globally-unique-PK precondition this is not a benign event. A
/// duplicate `__pk` means two nodes minted the same key for different logical
/// rows, which is exactly the cross-node data loss the precondition exists to
/// prevent. Overwriting one with the other **is** the data loss, so the default
/// is to stop before any row is written rather than to log and continue.
///
/// This is the runtime half of the precondition. The structural half (#268)
/// checks the *declared* primary-key shape at first run; only runtime can see
/// that actual *values* collide.
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DuplicatePkPolicy {
    /// Stop the sync and return [`SyncError::DuplicatePrimaryKey`], naming the
    /// colliding key, the table, and both rows' content hashes. Nothing is
    /// upserted -- the refusal happens while metadata is still being built,
    /// before the diff runs and before any write is issued.
    #[default]
    Refuse,
    /// Log the collision and keep the pre-#269 behavior: the later row
    /// overwrites the earlier one in the metadata map and the sync continues.
    /// The escape hatch for a deployment that cannot fix its keys immediately;
    /// it does not make the collision safe, it only defers the failure.
    Warn,
}

impl DuplicatePkPolicy {
    /// Decide what a duplicate rendered `__pk` means for this policy.
    ///
    /// Returns `Err` under [`DuplicatePkPolicy::Refuse`], and
    /// `Ok(Some(message))` under [`DuplicatePkPolicy::Warn`] -- the caller
    /// emits that message on its own sink (`tracing::warn`, `console.warn`, or
    /// `eprintln`, depending on transport). Both strings are built here so the
    /// native, wasm, and http-sql builders cannot drift on what a collision
    /// says, which is the failure mode #231 already hit once on this exact
    /// code path.
    ///
    /// `first_hash` is the content hash of the row already in the map,
    /// `second_hash` the one that would evict it.
    pub fn check(
        self,
        table: &str,
        pk: &str,
        first_hash: &str,
        second_hash: &str,
    ) -> Result<Option<String>> {
        match self {
            Self::Refuse => Err(SyncError::DuplicatePrimaryKey {
                table: table.to_string(),
                pk: pk.to_string(),
                first_hash: first_hash.to_string(),
                second_hash: second_hash.to_string(),
            }),
            Self::Warn => Ok(Some(format!(
                "duplicate primary key {pk} in {table} -- a row was overwritten in change \
                 metadata (kept hash {second_hash}, lost hash {first_hash}). The lost row is \
                 invisible to this sync. Set [sync] duplicate_pk = \"refuse\" to stop instead."
            ))),
        }
    }
}

/// How a same-primary-key collision resolves.
///
/// # One enum, two paths, different guarantees
///
/// These variants name a *preference* -- which side is kept. On the directional
/// push/pull path (`[sync].conflict_resolution`) that is the whole story: there
/// is one local row, one remote row, and one exchange, so every variant is
/// deterministic and the name says what happens.
///
/// Under masterless multicast (`[broadcast].conflict_resolution`,
/// [`crate::broadcast::BroadcastConfig::conflict_resolution`]) the same names
/// carry a second property they do not mention: whether the mesh **converges**.
/// N peers apply independently with no coordinator, so a preference that reads
/// as decisive can still leave two nodes permanently holding different rows for
/// one primary key. Only [`ConflictResolution::NewerWins`] is a total order.
/// Per-variant detail below; read it before choosing one for a mesh.
///
/// If you need ordering guarantees the transport does not give you, either opt
/// into `newer_wins` on **both** peers, or apply your own last-write-wins at
/// apply time (legion does the latter, and their tombstones converge because of
/// their code, not because of this setting).
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[allow(clippy::enum_variant_names)]
pub enum ConflictResolution {
    /// Keep the local row. **Directional:** deterministic -- the remote row is
    /// not applied. **`[broadcast]`:** does not converge, by construction. It
    /// lowers to `UpsertGuard::KeepLocal` in `local.rs`
    /// (`ON CONFLICT DO NOTHING`), so on a
    /// mesh where every node prefers its own row, a contended primary key stays
    /// divergent forever -- each node keeps what it has and rejects every peer's
    /// copy.
    ///
    /// This is why [`crate::broadcast::BroadcastConfig::conflict_resolution`] is
    /// scoped to `[broadcast]` and does NOT inherit this field, which defaults
    /// here to `local_wins`: inheriting it would have flipped every existing
    /// multicast deployment to never-accept-a-peer-row (#310).
    #[default]
    LocalWins,
    /// Apply the incoming row. **Directional:** deterministic -- one remote, one
    /// local, one exchange, so "the remote wins" names a single outcome.
    ///
    /// **`[broadcast]`: last-*received*-wins, not last-*written*-wins.** It
    /// lowers to `UpsertGuard::Replace` in `local.rs` -- an unconditional
    /// `INSERT OR REPLACE` that reads no timestamp at all. The winner is
    /// whichever datagram arrives last, evaluated independently at each node, so
    /// two peers that receive the same two writes in different orders converge to
    /// **different rows for the same primary key, permanently**, with no error
    /// and no anomaly counter. Choosing this on a mesh is choosing that.
    RemoteWins,
    /// Higher ordering value wins, compared as the `max` across the configured
    /// ordering columns. The **only** variant that is a total order: every node
    /// independently picks the same winner, so a mesh actually converges.
    ///
    /// Apply-side and not negotiated on the wire, so **both** peers must opt in
    /// -- a mesh mixing this with `remote_wins` converges toward the permissive
    /// node.
    NewerWins,
    /// UUIDv7 primary key with higher embedded timestamp wins.
    /// Falls back to NewerWins when PKs are not valid UUIDv7.
    ///
    /// Degenerates to [`ConflictResolution::NewerWins`] under `[broadcast]`: a
    /// same-primary-key collision means both rows carry the *same* UUID, so the
    /// key has nothing to break the tie with.
    UuidV7Wins,
}

/// Retry configuration for transient write failures.
///
/// Deserialized (flattened) from the `[sync]` table; the `rename`s map the
/// TOML keys onto the runtime field names.
#[derive(Debug, Clone, Copy, Deserialize)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    /// Initial delay in milliseconds before first retry
    #[serde(
        default = "default_initial_retry_delay_ms",
        rename = "initial_retry_delay_ms"
    )]
    pub initial_delay_ms: u64,
    /// Maximum delay in milliseconds (cap for exponential backoff)
    #[serde(default = "default_max_retry_delay_ms", rename = "max_retry_delay_ms")]
    pub max_delay_ms: u64,
    /// Backoff multiplier for exponential backoff
    #[serde(default = "default_backoff_multiplier")]
    pub backoff_multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: default_max_retries(),
            initial_delay_ms: default_initial_retry_delay_ms(),
            max_delay_ms: default_max_retry_delay_ms(),
            backoff_multiplier: default_backoff_multiplier(),
        }
    }
}

impl RetryConfig {
    /// Build a validated RetryConfig from a raw one.
    ///
    /// Validates that backoff_multiplier >= 1.0 (clamps invalid values).
    pub fn clamped(raw: &RetryConfig) -> Self {
        Self {
            max_retries: raw.max_retries.min(100), // cap at reasonable max
            initial_delay_ms: raw.initial_delay_ms,
            max_delay_ms: raw.max_delay_ms,
            // Ensure multiplier is at least 1.0 to avoid zero/negative delays
            backoff_multiplier: raw.backoff_multiplier.max(1.0),
        }
    }

    /// Calculate delay for a given attempt (0-indexed)
    pub fn delay_for_attempt(&self, attempt: u32) -> u64 {
        let delay = self.initial_delay_ms as f64 * self.backoff_multiplier.powi(attempt as i32);
        (delay as u64).min(self.max_delay_ms)
    }
}

/// Parse `content` as TOML, then expand `${VAR}` references inside string
/// *values only*, returning the typed `Config`.
///
/// Expansion happens AFTER structural parse, on the parsed `toml::Value` tree --
/// never on raw text. This is deliberate and load-bearing for a secrets path:
/// a substituted value (a token) can therefore never inject TOML structure, and
/// no `toml` parser ever sees expanded secret text, so a malformed secret cannot
/// echo into a parse error (which would leak it to logs/stderr). The only parse
/// runs on the user's literal config (placeholders, not secrets).
fn parse_with_env(content: &str) -> Result<Config> {
    let mut value: toml::Value =
        toml::from_str(content).map_err(|e| SyncError::Config(e.to_string()))?;
    expand_value(&mut value)?;
    let config: Config = value
        .try_into()
        .map_err(|e| SyncError::Config(e.to_string()))?;
    config.sync.validate_column_lists()?;
    Ok(config)
}

/// Recursively expand `${VAR}` in every string leaf of a parsed TOML tree.
fn expand_value(value: &mut toml::Value) -> Result<()> {
    match value {
        toml::Value::String(s) => *s = expand_env_vars(s)?,
        toml::Value::Array(items) => {
            for item in items {
                expand_value(item)?;
            }
        }
        toml::Value::Table(table) => {
            for (_, v) in table.iter_mut() {
                expand_value(v)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Expand `${VAR}` / `${VAR:-default}` references in a single string value
/// against the process environment. `$$` is a literal `$`. An unset var with no
/// default is a hard error naming the variable -- never a silent empty
/// substitution (which would send a blank credential).
fn expand_env_vars(input: &str) -> Result<String> {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '$' {
            out.push(c);
            continue;
        }
        match chars.peek() {
            Some('$') => {
                chars.next();
                out.push('$'); // $$ -> literal $
            }
            Some('{') => {
                chars.next(); // consume '{'
                let mut spec = String::new();
                let mut closed = false;
                for ch in chars.by_ref() {
                    if ch == '}' {
                        closed = true;
                        break;
                    }
                    spec.push(ch);
                }
                if !closed {
                    return Err(SyncError::Config(
                        "unterminated ${...} reference in config".to_string(),
                    ));
                }
                let (name, default) = match spec.split_once(":-") {
                    // Trim the default symmetrically with the name so a default
                    // cannot inject leading/trailing whitespace into a credential.
                    Some((n, d)) => (n.trim(), Some(d.trim())),
                    None => (spec.trim(), None),
                };
                if name.is_empty() {
                    return Err(SyncError::Config(
                        "empty ${} reference in config".to_string(),
                    ));
                }
                match std::env::var(name) {
                    Ok(v) => out.push_str(&v),
                    Err(_) => match default {
                        Some(d) => out.push_str(d),
                        None => return Err(SyncError::ConfigEnvVar(name.to_string())),
                    },
                }
            }
            // A lone '$' not starting an escape or reference is preserved.
            _ => out.push('$'),
        }
    }
    Ok(out)
}

impl Config {
    /// Parse config from a TOML string without filesystem access.
    ///
    /// Skips local_db auto-detection and target validation.
    /// Use this for WASM or library consumers that construct config programmatically.
    /// `${VAR}` references inside string values are expanded from the environment
    /// (see [`parse_with_env`]).
    pub fn from_toml_str(content: &str) -> Result<Self> {
        parse_with_env(content)
    }

    /// Load config from a TOML file
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            return Err(SyncError::ConfigNotFound(path.display().to_string()));
        }

        let content = std::fs::read_to_string(path).map_err(|e| {
            SyncError::Config(format!("failed to read config {}: {}", path.display(), e))
        })?;
        let mut config: Config = parse_with_env(&content)?;

        // Auto-detect local_db if not specified
        if config.local_db.is_none() {
            match detect_local_db() {
                Ok(path) => config.local_db = Some(path),
                Err(e) => {
                    // local_db is always required (it's the source database)
                    return Err(e);
                }
            }
        }

        // Target resolution is deferred to command dispatch: some commands
        // (broadcast, stash, retrieve, snapshot, restore) never touch the
        // target and must not fail at load time if the plugin binary is
        // missing. See crates/smugglr/src/main.rs for where target resolution
        // actually runs.
        Ok(config)
    }

    /// Resolve the target from either [target] section or legacy flat fields.
    pub fn resolve_target(&self) -> Result<ResolvedTarget> {
        if let Some(ref target) = self.target {
            return Ok(match target {
                TargetConfig::D1 {
                    account_id,
                    database_id,
                    api_token,
                    url,
                } => resolve_d1_plugin_target(account_id, database_id, api_token, url.as_deref())?,
                TargetConfig::Sqlite { database } => ResolvedTarget::Sqlite {
                    database: database.clone(),
                },
                TargetConfig::Plugin { name, path, config } => {
                    let (resolved_path, resolved_name) = match (name, path) {
                        (_, Some(p)) => {
                            let pb = PathBuf::from(p);
                            let n = pb
                                .file_name()
                                .map(|f| f.to_string_lossy().into_owned())
                                .unwrap_or_else(|| p.clone());
                            (pb, n)
                        }
                        (Some(_n), None) => {
                            #[cfg(feature = "native")]
                            {
                                let pb = crate::plugin::resolve_plugin_path(_n)?;
                                (pb, _n.clone())
                            }
                            #[cfg(not(feature = "native"))]
                            {
                                return Err(SyncError::Config(
                                    "Plugin targets require the 'native' feature".into(),
                                ));
                            }
                        }
                        (None, None) => {
                            return Err(SyncError::Config(
                                "Plugin target requires either 'name' or 'path'".into(),
                            ));
                        }
                    };
                    ResolvedTarget::Plugin {
                        path: resolved_path,
                        name: resolved_name,
                        config: config.clone(),
                    }
                }
            });
        }

        // Fall back to legacy flat fields
        match (
            &self.cloudflare_account_id,
            &self.database_id,
            &self.cloudflare_api_token,
        ) {
            (Some(account_id), Some(database_id), Some(api_token)) => {
                resolve_d1_plugin_target(account_id, database_id, api_token, None)
            }
            _ => Err(SyncError::Config(
                "No target configured. Add a [target] section or set cloudflare_account_id, database_id, and cloudflare_api_token.".to_string()
            )),
        }
    }

    /// Get the local database path (guaranteed to be Some after load)
    pub fn local_db_path(&self) -> &str {
        self.local_db
            .as_deref()
            .expect("local_db should be set after load")
    }

    /// Check if a table should be synced
    pub fn should_sync_table(&self, table: &str) -> bool {
        // Always exclude certain tables
        if self.sync.exclude_tables.iter().any(|t| t == table) {
            return false;
        }

        // If specific tables are configured, only sync those
        if !self.sync.tables.is_empty() {
            return self.sync.tables.iter().any(|t| t == table);
        }

        // Otherwise sync all non-excluded tables
        true
    }
}

/// Build a `ResolvedTarget::Plugin` for the http-sql plugin with a d1 profile.
///
/// Both the explicit `[target] type = "d1"` branch and the legacy flat-fields branch
/// funnel through here -- D1 is just another http-sql profile at runtime.
fn resolve_d1_plugin_target(
    account_id: &str,
    database_id: &str,
    api_token: &str,
    url: Option<&str>,
) -> Result<ResolvedTarget> {
    let plugin_config = d1_plugin_config(account_id, database_id, api_token, url);
    let plugin_path = resolve_http_sql_plugin_path()?;

    Ok(ResolvedTarget::Plugin {
        path: plugin_path,
        name: "smugglr-http-sql".to_string(),
        config: plugin_config,
    })
}

/// The plugin config a `d1` target hands the http-sql adapter.
///
/// Split out of [`resolve_d1_plugin_target`] and public because it is the seam
/// #429 was a defect in: core synthesized keys the adapter did not read. The
/// adapter's own tests drive this function and then assert what reaches the
/// wire, which they cannot do through `resolve_target` -- that also resolves
/// the plugin BINARY, which needs the `native` feature and an installed file,
/// neither of which a wire-format test should require. Keeping the synthesis
/// pure means both sides of the seam are asserted against the same code.
pub fn d1_plugin_config(
    account_id: &str,
    database_id: &str,
    api_token: &str,
    url: Option<&str>,
) -> HashMap<String, String> {
    let mut plugin_config = HashMap::new();
    plugin_config.insert("profile".to_string(), "d1".to_string());
    plugin_config.insert("account_id".to_string(), account_id.to_string());
    plugin_config.insert("database_id".to_string(), database_id.to_string());

    // The adapter reads `auth_token`, never `api_token` (#429). `api_token`
    // stays the name of the TOML field an operator writes; only the key handed
    // across the plugin boundary changes here.
    plugin_config.insert("auth_token".to_string(), api_token.to_string());

    // A caller-supplied URL wins -- that is the DO-bridge case documented in
    // `config.example.toml`. With none, derive Cloudflare's own endpoint, which
    // is what the README's quickstart has always claimed happens (#429).
    let resolved_url = match url {
        Some(u) => u.to_string(),
        None => crate::profile::Profile::d1_query_url(account_id, database_id),
    };
    plugin_config.insert("url".to_string(), resolved_url);

    plugin_config
}

/// Resolve the path to the smugglr-http-sql plugin binary.
///
/// Under `cfg(test)` a placeholder path is returned so unit tests can assert on
/// config synthesis without requiring the binary to be installed.
fn resolve_http_sql_plugin_path() -> Result<PathBuf> {
    #[cfg(test)]
    {
        Ok(PathBuf::from("/fake/smugglr-http-sql"))
    }
    #[cfg(all(not(test), feature = "native"))]
    {
        crate::plugin::resolve_plugin_path("http-sql").map_err(|_| {
            SyncError::Config(format!(
                "d1 target requires the smugglr-http-sql plugin. {}",
                crate::plugin::HTTP_SQL_INSTALL_HINT
            ))
        })
    }
    #[cfg(all(not(test), not(feature = "native")))]
    {
        Err(SyncError::Config(
            "d1 target requires the 'native' feature".into(),
        ))
    }
}

/// Auto-detect the local D1 database from wrangler's state directory
fn detect_local_db() -> Result<String> {
    let miniflare_dir = Path::new(".wrangler/state/v3/d1/miniflare-D1DatabaseObject");

    if !miniflare_dir.exists() {
        return Err(SyncError::Config(
            "No .wrangler/state/v3/d1/miniflare-D1DatabaseObject directory found. Run 'wrangler dev' first.".to_string()
        ));
    }

    // Find the largest non-empty sqlite file (the active database)
    let mut best_file: Option<(std::path::PathBuf, u64)> = None;

    for entry in std::fs::read_dir(miniflare_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.extension().is_some_and(|ext| ext == "sqlite") {
            let metadata = std::fs::metadata(&path)?;
            let size = metadata.len();

            // Skip empty files
            if size == 0 {
                continue;
            }

            debug!("Found sqlite: {} ({} bytes)", path.display(), size);

            // Prefer larger files (more data = more likely the active one)
            if best_file
                .as_ref()
                .map_or(true, |(_, best_size)| size > *best_size)
            {
                best_file = Some((path, size));
            }
        }
    }

    match best_file {
        Some((path, size)) => {
            let path_str = path.display().to_string();
            info!(
                "Auto-detected local database: {} ({} bytes)",
                path_str, size
            );
            Ok(path_str)
        }
        None => Err(SyncError::Config(
            "No non-empty sqlite files found in wrangler state. Run 'wrangler dev' and make some queries first.".to_string()
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `default_exclude_tables` names the ledger by literal, and this is what
    /// keeps that literal honest (#370).
    ///
    /// The duplication is deliberate rather than sloppy: `config` is not
    /// native-gated and `migrate::ledger` is, so referencing the constant here
    /// would drag the gate into this module or fail to compile without the
    /// feature. That is a real constraint.
    ///
    /// What was NOT acceptable is the two strings being linked by a comment
    /// alone. Renaming the ledger and missing this line would leave
    /// `default_exclude_tables` no longer excluding the ledger, and the symptom
    /// -- the ledger appearing as user schema in drift detection and in
    /// forger's inventory comparison -- reads as a data problem rather than as
    /// a stale constant.
    ///
    /// So the assertion lives on the side of the gate that can see both.
    #[cfg(feature = "native")]
    #[test]
    fn the_excluded_ledger_name_is_the_ledger_name() {
        assert!(
            default_exclude_tables().contains(&crate::migrate::ledger::LEDGER_TABLE.to_string()),
            "default_exclude_tables must exclude the ledger by its real name; it lists {:?} and \
             the ledger is {:?}",
            default_exclude_tables(),
            crate::migrate::ledger::LEDGER_TABLE
        );
    }

    fn test_config_d1() -> Config {
        Config {
            cloudflare_account_id: Some("test_acct".into()),
            cloudflare_api_token: Some("test_token".into()),
            database_id: Some("test_db".into()),
            local_db: Some("test.db".into()),
            sync: SyncConfig::default(),
            stash: None,
            target: None,
            broadcast: None,
        }
    }

    fn test_config_sqlite_target() -> Config {
        Config {
            cloudflare_account_id: None,
            cloudflare_api_token: None,
            database_id: None,
            local_db: Some("source.db".into()),
            sync: SyncConfig::default(),
            stash: None,
            target: Some(TargetConfig::Sqlite {
                database: "backup.db".into(),
            }),
            broadcast: None,
        }
    }

    /// Assert that `target` is a `ResolvedTarget::Plugin` pointing at the
    /// http-sql plugin with a synthesized d1 profile config. Used by every
    /// test that exercises the D1-to-plugin routing in `resolve_target`.
    ///
    /// `expected_url` is written out in full at each call site rather than
    /// derived here (#429). A helper that built the Cloudflare URL the same way
    /// `resolve_d1_plugin_target` does would agree with any derivation,
    /// including a wrong one, which is the whole thing this test exists to
    /// catch.
    fn assert_d1_plugin(
        target: &ResolvedTarget,
        account_id: &str,
        database_id: &str,
        auth_token: &str,
        expected_url: &str,
    ) {
        let ResolvedTarget::Plugin { name, config, .. } = target else {
            panic!("expected plugin target, got {:?}", target);
        };
        assert_eq!(name, "smugglr-http-sql");
        assert_eq!(config.get("profile").map(String::as_str), Some("d1"));
        assert_eq!(
            config.get("account_id").map(String::as_str),
            Some(account_id)
        );
        assert_eq!(
            config.get("database_id").map(String::as_str),
            Some(database_id)
        );

        // The adapter reads `auth_token`. `api_token` was the key core used to
        // send and nothing ever read (#429); assert it is gone, so a revert
        // cannot pass by adding the old key back alongside the new one.
        assert_eq!(
            config.get("auth_token").map(String::as_str),
            Some(auth_token)
        );
        assert_eq!(config.get("api_token"), None);

        assert_eq!(config.get("url").map(String::as_str), Some(expected_url));
    }

    #[test]
    fn test_default_excludes() {
        let config = test_config_d1();

        assert!(!config.should_sync_table("sqlite_sequence"));
        assert!(!config.should_sync_table("_cf_KV"));
        // migrate's own ledger is control-plane -- never synced or validated.
        assert!(!config.should_sync_table("_smugglr_migrations"));
        assert!(config.should_sync_table("abilities"));
    }

    #[test]
    fn test_specific_tables() {
        let mut config = test_config_d1();
        config.sync.tables = vec!["abilities".into(), "talents".into()];

        assert!(config.should_sync_table("abilities"));
        assert!(config.should_sync_table("talents"));
        assert!(!config.should_sync_table("disciplines"));
    }

    #[test]
    fn test_resolve_target_legacy_d1() {
        let config = test_config_d1();
        let target = config.resolve_target().unwrap();
        assert_d1_plugin(
            &target,
            "test_acct",
            "test_db",
            "test_token",
            "https://api.cloudflare.com/client/v4/accounts/test_acct/d1/database/test_db/query",
        );
    }

    #[test]
    fn test_resolve_target_sqlite() {
        let config = test_config_sqlite_target();
        let target = config.resolve_target().unwrap();
        match target {
            ResolvedTarget::Sqlite { database } => assert_eq!(database, "backup.db"),
            _ => panic!("expected SQLite target"),
        }
    }

    #[test]
    fn test_resolve_target_explicit_d1() {
        let config = Config {
            cloudflare_account_id: None,
            cloudflare_api_token: None,
            database_id: None,
            local_db: Some("test.db".into()),
            sync: SyncConfig::default(),
            stash: None,
            target: Some(TargetConfig::D1 {
                account_id: "acct".into(),
                database_id: "db".into(),
                api_token: "tok".into(),
                url: None,
            }),
            broadcast: None,
        };
        let target = config.resolve_target().unwrap();
        assert_d1_plugin(
            &target,
            "acct",
            "db",
            "tok",
            "https://api.cloudflare.com/client/v4/accounts/acct/d1/database/db/query",
        );
    }

    #[test]
    fn test_resolve_target_d1_with_url() {
        // Covers the `if let Some(u) = url` branch in resolve_d1_plugin_target.
        // A D1 config with an explicit url (e.g. for a self-hosted HTTP bridge
        // via the Durable Objects template) must round-trip through the plugin
        // config map so the http-sql adapter can pick it up.
        let config = Config {
            cloudflare_account_id: None,
            cloudflare_api_token: None,
            database_id: None,
            local_db: Some("test.db".into()),
            sync: SyncConfig::default(),
            stash: None,
            target: Some(TargetConfig::D1 {
                account_id: "acct".into(),
                database_id: "db".into(),
                api_token: "tok".into(),
                url: Some("https://bridge.example.com".into()),
            }),
            broadcast: None,
        };
        let target = config.resolve_target().unwrap();
        assert_d1_plugin(&target, "acct", "db", "tok", "https://bridge.example.com");
    }

    #[test]
    fn test_resolve_target_explicit_overrides_legacy() {
        let config = Config {
            cloudflare_account_id: Some("old_acct".into()),
            cloudflare_api_token: Some("old_token".into()),
            database_id: Some("old_db".into()),
            local_db: Some("test.db".into()),
            sync: SyncConfig::default(),
            stash: None,
            target: Some(TargetConfig::Sqlite {
                database: "backup.db".into(),
            }),
            broadcast: None,
        };
        let target = config.resolve_target().unwrap();
        assert!(matches!(target, ResolvedTarget::Sqlite { .. }));
    }

    #[test]
    fn test_resolve_target_no_config() {
        let config = Config {
            cloudflare_account_id: None,
            cloudflare_api_token: None,
            database_id: None,
            local_db: Some("test.db".into()),
            sync: SyncConfig::default(),
            stash: None,
            target: None,
            broadcast: None,
        };
        assert!(config.resolve_target().is_err());
    }

    #[test]
    fn test_retry_config_defaults() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.initial_delay_ms, 100);
        assert_eq!(config.max_delay_ms, 30_000);
        assert_eq!(config.backoff_multiplier, 2.0);
    }

    #[test]
    fn test_backoff_multiplier_clamped() {
        let sync = SyncConfig {
            retry: RetryConfig {
                backoff_multiplier: 0.5,
                ..Default::default()
            },
            ..Default::default()
        };
        let retry = RetryConfig::clamped(&sync.retry);
        assert_eq!(retry.backoff_multiplier, 1.0);
    }

    #[test]
    fn test_max_retries_capped() {
        let sync = SyncConfig {
            retry: RetryConfig {
                max_retries: 1000,
                ..Default::default()
            },
            ..Default::default()
        };
        let retry = RetryConfig::clamped(&sync.retry);
        assert_eq!(retry.max_retries, 100);
    }

    #[test]
    fn test_retry_config_from_sync_config() {
        let sync = SyncConfig {
            retry: RetryConfig {
                max_retries: 3,
                initial_delay_ms: 500,
                max_delay_ms: 30000,
                backoff_multiplier: 1.5,
            },
            ..Default::default()
        };
        let retry = RetryConfig::clamped(&sync.retry);
        assert_eq!(retry.max_retries, 3);
        assert_eq!(retry.initial_delay_ms, 500);
        assert_eq!(retry.max_delay_ms, 30000);
        assert_eq!(retry.backoff_multiplier, 1.5);
    }

    #[test]
    fn test_parse_toml_retry_keys_flattened() {
        // The retry fields are flattened into [sync]; the TOML keys must keep
        // their original names and map onto RetryConfig's runtime field names.
        let toml_str = r#"
local_db = "game.db"

[sync]
max_retries = 7
initial_retry_delay_ms = 250
max_retry_delay_ms = 12000
backoff_multiplier = 3.0
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.sync.retry.max_retries, 7);
        assert_eq!(config.sync.retry.initial_delay_ms, 250);
        assert_eq!(config.sync.retry.max_delay_ms, 12000);
        assert_eq!(config.sync.retry.backoff_multiplier, 3.0);
    }

    #[test]
    fn test_parse_toml_retry_keys_default_when_absent() {
        let toml_str = r#"
local_db = "game.db"

[sync]
tables = ["abilities"]
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.sync.retry.max_retries, 5);
        assert_eq!(config.sync.retry.initial_delay_ms, 100);
        assert_eq!(config.sync.retry.max_delay_ms, 30_000);
        assert_eq!(config.sync.retry.backoff_multiplier, 2.0);
    }

    #[test]
    fn test_delay_for_attempt_exponential() {
        let config = RetryConfig {
            max_retries: 5,
            initial_delay_ms: 1000,
            max_delay_ms: 60000,
            backoff_multiplier: 2.0,
        };

        assert_eq!(config.delay_for_attempt(0), 1000);
        assert_eq!(config.delay_for_attempt(1), 2000);
        assert_eq!(config.delay_for_attempt(2), 4000);
        assert_eq!(config.delay_for_attempt(3), 8000);
        assert_eq!(config.delay_for_attempt(4), 16000);
    }

    #[test]
    fn test_delay_capped_at_max() {
        let config = RetryConfig {
            max_retries: 10,
            initial_delay_ms: 1000,
            max_delay_ms: 5000,
            backoff_multiplier: 2.0,
        };

        assert_eq!(config.delay_for_attempt(0), 1000);
        assert_eq!(config.delay_for_attempt(1), 2000);
        assert_eq!(config.delay_for_attempt(2), 4000);
        assert_eq!(config.delay_for_attempt(3), 5000);
        assert_eq!(config.delay_for_attempt(4), 5000);
    }

    #[test]
    fn test_parse_toml_sqlite_target() {
        let toml_str = r#"
local_db = "game.db"

[target]
type = "sqlite"
database = "backup.db"

[sync]
tables = ["abilities"]
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert!(matches!(config.target, Some(TargetConfig::Sqlite { .. })));
        let target = config.resolve_target().unwrap();
        match target {
            ResolvedTarget::Sqlite { database } => assert_eq!(database, "backup.db"),
            _ => panic!("expected sqlite"),
        }
    }

    #[test]
    fn test_parse_toml_d1_target() {
        let toml_str = r#"
local_db = "game.db"

[target]
type = "d1"
account_id = "acct123"
database_id = "db456"
api_token = "tok789"
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        let target = config.resolve_target().unwrap();
        assert_d1_plugin(
            &target,
            "acct123",
            "db456",
            "tok789",
            "https://api.cloudflare.com/client/v4/accounts/acct123/d1/database/db456/query",
        );
    }

    #[test]
    fn test_parse_toml_legacy_d1() {
        let toml_str = r#"
cloudflare_account_id = "acct"
cloudflare_api_token = "tok"
database_id = "db"
local_db = "game.db"
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert!(config.target.is_none());
        let target = config.resolve_target().unwrap();
        assert_d1_plugin(
            &target,
            "acct",
            "db",
            "tok",
            "https://api.cloudflare.com/client/v4/accounts/acct/d1/database/db/query",
        );
    }

    // -- Column exclusion tests --

    #[test]
    fn test_column_excluded_standalone() {
        let patterns = vec!["*_embedding".to_string(), "blob_*".to_string()];
        assert!(column_excluded("title_embedding", &patterns));
        assert!(column_excluded("blob_data", &patterns));
        assert!(!column_excluded("name", &patterns));
        assert!(!column_excluded("id", &patterns));
    }

    // Pins column_glob_match's behavior across the (starts_star, ends_star)
    // boundary cases -- in particular the bare "*" short-circuit and the
    // (true, true) "contains" arm, whose now-unreachable length guard and
    // dead fallback arm were removed as part of #214. This is a
    // behavior-preservation pin, not a fails-before-the-fix regression test:
    // the removed guard and arm were provably unreachable, so there is no
    // prior state in which these assertions could have failed.
    #[test]
    fn test_column_glob_match_boundary_cases() {
        // Bare "*" matches anything, including the empty string.
        assert!(column_glob_match("*", "anything"));
        assert!(column_glob_match("*", ""));

        // Leading star: suffix match.
        assert!(column_glob_match("*_embedding", "title_embedding"));
        assert!(!column_glob_match("*_embedding", "embedding_title"));

        // Trailing star: prefix match.
        assert!(column_glob_match("embedding_*", "embedding_title"));
        assert!(!column_glob_match("embedding_*", "title_embedding"));

        // Both star (len >= 2): contains match, including the len == 2
        // "**" case where inner is empty and matches everything.
        assert!(column_glob_match("*embed*", "title_embedding"));
        assert!(!column_glob_match("*embed*", "title_vector"));
        assert!(column_glob_match("**", "anything"));
        assert!(column_glob_match("**", ""));

        // No star: exact match only.
        assert!(column_glob_match("vector", "vector"));
        assert!(!column_glob_match("vector", "vectors"));
    }

    #[test]
    fn test_parse_toml_with_exclude_columns() {
        let toml_str = r#"
cloudflare_account_id = "acct"
cloudflare_api_token = "tok"
database_id = "db"
local_db = "game.db"

[sync]
exclude_columns = ["*_embedding", "vector"]
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.sync.exclude_columns.len(), 2);
        let patterns = &config.sync.exclude_columns;
        assert!(column_excluded("title_embedding", patterns));
        assert!(column_excluded("vector", patterns));
        assert!(!column_excluded("name", patterns));
    }

    // #293: converge_columns parses from the same [sync] table and is a SEPARATE
    // list from exclude_columns -- the two mean different things (both leave the
    // hash; only exclude_columns leaves the wire), so a config setting one must
    // not populate the other.
    #[test]
    fn test_parse_toml_with_converge_columns() {
        let toml_str = r#"
local_db = "game.db"

[sync]
exclude_columns = ["*_embedding"]
converge_columns = ["email", "phone_*"]
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.sync.exclude_columns, vec!["*_embedding".to_string()]);
        assert_eq!(
            config.sync.converge_columns,
            vec!["email".to_string(), "phone_*".to_string()]
        );

        let converge = &config.sync.converge_columns;
        assert!(column_excluded("email", converge));
        assert!(column_excluded("phone_mobile", converge));
        assert!(!column_excluded("name", converge));
        // The excluded pattern must not have leaked into the converge list.
        assert!(!column_excluded("title_embedding", converge));
    }

    // Absent from the config, converge_columns defaults empty, which is what
    // keeps the hash-match fast path on for every existing deployment.
    #[test]
    fn converge_columns_defaults_empty_and_borrows() {
        let config: Config = toml::from_str("local_db = \"game.db\"\n").unwrap();
        assert!(config.sync.converge_columns.is_empty());
        assert!(matches!(
            config.sync.hash_excluded_columns(),
            std::borrow::Cow::Borrowed(_)
        ));
    }

    // #269: absent from the config, duplicate_pk defaults to REFUSE. This is the
    // shipped-behavior change -- an existing config that never mentioned the key
    // now refuses a collision it previously overwrote-and-warned through -- so
    // the default is pinned here rather than left to the derive.
    #[test]
    fn duplicate_pk_defaults_to_refuse() {
        let config: Config = toml::from_str("local_db = \"game.db\"\n").unwrap();
        assert_eq!(config.sync.duplicate_pk, DuplicatePkPolicy::Refuse);
        // The Default impl and serde's default must agree; a field added to the
        // struct without a matching Default arm does not compile, but a
        // MISMATCHED arm would, and would make behavior depend on how the config
        // was constructed.
        assert_eq!(
            SyncConfig::default().duplicate_pk,
            DuplicatePkPolicy::Refuse
        );
    }

    /// Both spellings parse. This pins the serde surface only -- it says nothing
    /// about the error message; that cross-check is the test below.
    #[test]
    fn duplicate_pk_parses_both_spellings() {
        let parse = |v: &str| -> DuplicatePkPolicy {
            let config: Config = toml::from_str(&format!(
                "local_db = \"game.db\"\n[sync]\nduplicate_pk = \"{v}\"\n"
            ))
            .unwrap();
            config.sync.duplicate_pk
        };
        assert_eq!(parse("warn"), DuplicatePkPolicy::Warn);
        assert_eq!(parse("refuse"), DuplicatePkPolicy::Refuse);
    }

    /// The remedy the refusal prints must be a config the parser accepts.
    ///
    /// `SyncError::DuplicatePrimaryKey` hardcodes `set [sync] duplicate_pk =
    /// "warn"` in its message. That string is the operator's only instruction
    /// for getting unstuck, and nothing structural ties it to the serde
    /// spelling -- rename the variant or change `rename_all` and the message
    /// keeps confidently printing an incantation that no longer parses. So this
    /// lifts the value straight out of the rendered message and feeds it to the
    /// TOML parser, rather than asserting the two look alike by eye.
    #[test]
    fn the_remedy_the_refusal_prints_is_a_config_that_actually_parses() {
        let rendered = SyncError::DuplicatePrimaryKey {
            table: "items".into(),
            pk: "1".into(),
            first_hash: "aaaa".into(),
            second_hash: "bbbb".into(),
        }
        .to_string();

        // Pull the value out of `duplicate_pk = "<value>"` as the message prints it.
        let marker = "duplicate_pk = \"";
        let start = rendered
            .find(marker)
            .expect("the refusal must tell the operator which key to set")
            + marker.len();
        let value = &rendered[start..][..rendered[start..]
            .find('"')
            .expect("the remedy value must be quoted")];

        let config: Config = toml::from_str(&format!(
            "local_db = \"game.db\"\n[sync]\nduplicate_pk = \"{value}\"\n"
        ))
        .unwrap_or_else(|e| {
            panic!("the refusal prints duplicate_pk = \"{value}\", which does not parse: {e}")
        });
        assert_eq!(
            config.sync.duplicate_pk,
            DuplicatePkPolicy::Warn,
            "the refusal offers `warn` as the escape hatch, so its printed value must mean Warn"
        );
    }

    // The hash-exclusion union is the invariant every hash producer depends on:
    // the diff path, the multicast digest, and the wasm cached diff must all
    // cover the same columns or identical rows hash differently and never
    // converge.
    #[test]
    fn hash_excluded_columns_unions_both_lists() {
        let sync = SyncConfig {
            exclude_columns: vec!["*_embedding".to_string()],
            converge_columns: vec!["email".to_string()],
            ..Default::default()
        };

        let all = sync.hash_excluded_columns();
        assert!(matches!(all, std::borrow::Cow::Owned(_)));
        assert!(column_excluded("title_embedding", &all));
        assert!(column_excluded("email", &all));
        assert!(!column_excluded("name", &all));
    }

    // #293 review finding: an overlapping pattern is silent destructive data
    // loss (hash-excluded -> selected on timestamp -> stripped before send ->
    // destination nulled on INSERT OR REPLACE backends -> timestamps now tie ->
    // classified identical forever), so it is refused at load, not resolved.
    #[test]
    fn overlapping_exclude_and_converge_patterns_are_refused() {
        // The literal copy-instead-of-move case the migration note invites.
        let exact = r#"
local_db = "game.db"

[sync]
exclude_columns = ["email"]
converge_columns = ["email"]
"#;
        let err = Config::from_toml_str(exact).unwrap_err().to_string();
        assert!(
            err.contains("converge_columns") && err.contains("exclude_columns"),
            "error must name both lists so the operator knows what to move: {err}"
        );

        // A broader glob left behind in exclude_columns while the specific name
        // is moved -- the same trap with one more step of indirection.
        let glob = r#"
local_db = "game.db"

[sync]
exclude_columns = ["*email*"]
converge_columns = ["email"]
"#;
        assert!(
            Config::from_toml_str(glob).is_err(),
            "a glob in exclude_columns that matches a converge pattern must be refused"
        );

        // Symmetric: the glob on the converge side.
        let glob_other = r#"
local_db = "game.db"

[sync]
exclude_columns = ["email"]
converge_columns = ["*email*"]
"#;
        assert!(Config::from_toml_str(glob_other).is_err());
    }

    // The guard must not fire on legitimately disjoint lists, or it blocks the
    // feature it exists to protect.
    #[test]
    fn disjoint_exclude_and_converge_lists_are_accepted() {
        let toml_str = r#"
local_db = "game.db"

[sync]
exclude_columns = ["*_embedding", "vector"]
converge_columns = ["email", "phone_*"]
"#;
        let config = Config::from_toml_str(toml_str).expect("disjoint lists must parse");
        assert_eq!(config.sync.converge_columns.len(), 2);
        assert_eq!(config.sync.exclude_columns.len(), 2);
    }

    #[test]
    fn test_parse_toml_plugin_target_with_name() {
        let toml_str = r#"
local_db = "game.db"

[target]
type = "plugin"
name = "turso"

[target.config]
url = "libsql://my-db.turso.io"
auth_token = "tok123"
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        match &config.target {
            Some(TargetConfig::Plugin { name, path, config }) => {
                assert_eq!(name.as_deref(), Some("turso"));
                assert!(path.is_none());
                assert_eq!(config.get("url").unwrap(), "libsql://my-db.turso.io");
                assert_eq!(config.get("auth_token").unwrap(), "tok123");
            }
            _ => panic!("expected plugin target"),
        }
    }

    #[test]
    fn test_parse_toml_plugin_target_with_path() {
        let toml_str = r#"
local_db = "game.db"

[target]
type = "plugin"
path = "/usr/local/bin/smugglr-turso"

[target.config]
url = "libsql://my-db.turso.io"
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        match &config.target {
            Some(TargetConfig::Plugin { name, path, config }) => {
                assert!(name.is_none());
                assert_eq!(path.as_deref(), Some("/usr/local/bin/smugglr-turso"));
                assert_eq!(config.get("url").unwrap(), "libsql://my-db.turso.io");
            }
            _ => panic!("expected plugin target"),
        }
    }

    #[test]
    fn test_parse_toml_plugin_target_empty_config() {
        let toml_str = r#"
local_db = "game.db"

[target]
type = "plugin"
path = "/usr/local/bin/smuggler-custom"
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        match &config.target {
            Some(TargetConfig::Plugin { config, .. }) => {
                assert!(config.is_empty());
            }
            _ => panic!("expected plugin target"),
        }
    }

    #[test]
    fn test_resolve_plugin_target_with_path() {
        let config = Config {
            cloudflare_account_id: None,
            cloudflare_api_token: None,
            database_id: None,
            local_db: Some("test.db".into()),
            sync: SyncConfig::default(),
            stash: None,
            target: Some(TargetConfig::Plugin {
                name: None,
                path: Some("/usr/local/bin/smugglr-turso".into()),
                config: HashMap::new(),
            }),
            broadcast: None,
        };
        let target = config.resolve_target().unwrap();
        match target {
            ResolvedTarget::Plugin { path, name, .. } => {
                assert_eq!(path, PathBuf::from("/usr/local/bin/smugglr-turso"));
                assert_eq!(name, "smugglr-turso");
            }
            _ => panic!("expected plugin target"),
        }
    }

    #[test]
    fn test_resolve_plugin_target_no_name_or_path() {
        let config = Config {
            cloudflare_account_id: None,
            cloudflare_api_token: None,
            database_id: None,
            local_db: Some("test.db".into()),
            sync: SyncConfig::default(),
            stash: None,
            target: Some(TargetConfig::Plugin {
                name: None,
                path: None,
                config: HashMap::new(),
            }),
            broadcast: None,
        };
        assert!(config.resolve_target().is_err());
    }

    #[test]
    fn env_expand_substitutes_set_var() {
        std::env::set_var("SMUGGLR_TEST_ENV_TOKEN", "s3cr3t");
        let cfg = Config::from_toml_str(
            "local_db = \"x.db\"\ncloudflare_api_token = \"${SMUGGLR_TEST_ENV_TOKEN}\"",
        )
        .unwrap();
        assert_eq!(cfg.cloudflare_api_token.as_deref(), Some("s3cr3t"));
        std::env::remove_var("SMUGGLR_TEST_ENV_TOKEN");
    }

    #[test]
    fn env_expand_uses_default_when_unset() {
        let out = expand_env_vars("${SMUGGLR_TEST_ENV_UNSET_X:-fallback}").unwrap();
        assert_eq!(out, "fallback");
    }

    #[test]
    fn env_expand_unset_no_default_errors_with_exit_2() {
        let err = expand_env_vars("${SMUGGLR_TEST_ENV_DEFINITELY_UNSET_Y}").unwrap_err();
        assert!(
            matches!(err, SyncError::ConfigEnvVar(ref v) if v == "SMUGGLR_TEST_ENV_DEFINITELY_UNSET_Y")
        );
        assert_eq!(err.exit_code(), 2);
    }

    #[test]
    fn env_expand_double_dollar_is_literal() {
        assert_eq!(expand_env_vars("$${HOME}/x").unwrap(), "${HOME}/x");
    }

    #[test]
    fn env_expand_lone_dollar_preserved() {
        assert_eq!(expand_env_vars("costs $5 total").unwrap(), "costs $5 total");
    }

    #[test]
    fn env_expand_value_with_toml_metachars_is_safe() {
        // Expansion happens post-parse on string values, so a secret containing
        // a quote/newline is inserted verbatim -- it cannot inject TOML structure
        // and never reaches a parser (so a malformed secret can't leak via a
        // parse error). The value must round-trip exactly.
        let nasty = "ab\"c\ninjected = \"x";
        std::env::set_var("SMUGGLR_TEST_ENV_NASTY", nasty);
        let cfg = Config::from_toml_str(
            "local_db = \"x.db\"\ncloudflare_api_token = \"${SMUGGLR_TEST_ENV_NASTY}\"",
        )
        .unwrap();
        assert_eq!(cfg.cloudflare_api_token.as_deref(), Some(nasty));
        // No `injected` key leaked into the config as structure.
        assert!(cfg.database_id.is_none());
        std::env::remove_var("SMUGGLR_TEST_ENV_NASTY");
    }

    #[test]
    fn env_expand_default_is_trimmed_symmetrically() {
        // Regression for #184: surrounding whitespace around the default must be
        // stripped (matching the name's trim) so a default cannot inject blanks
        // into a credential.
        let out = expand_env_vars("${ SMUGGLR_TEST_ENV_UNSET_Z :- s3cr3t }").unwrap();
        assert_eq!(out, "s3cr3t");
    }

    #[test]
    fn load_read_error_maps_to_exit_2() {
        // Regression for #182: a config-phase I/O failure (here, the path is a
        // directory, which read_to_string rejects) must classify as a config
        // error (exit 2), not the general/unknown bucket (exit 1).
        let dir = std::env::temp_dir();
        let err = Config::load(&dir).unwrap_err();
        assert!(
            matches!(err, SyncError::Config(_)),
            "expected SyncError::Config, got {err:?}"
        );
        assert_eq!(err.exit_code(), 2);
    }
}
