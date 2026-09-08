//! Local SQLite database operations

use crate::config::DuplicatePkPolicy;
use crate::datasource::{ColumnInfo, DataSource, RowMeta, TableInfo};
use crate::error::{Result, SyncError};
use crate::table::TableSchema;
use rusqlite::{Connection, OpenFlags, Row};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Mutex, MutexGuard};
use tracing::{debug, info, warn};

/// Wrapper for local SQLite database
pub struct LocalDb {
    conn: Mutex<Connection>,
    /// What a duplicate rendered `__pk` means when building row metadata.
    ///
    /// A field rather than a [`DataSource::get_row_metadata`] parameter: that
    /// method is also the plugin-SDK wire contract (`table`, `timestamp_column`,
    /// `exclude_columns` are decoded from a JSON-RPC request), so widening it
    /// would be a plugin protocol change for every implementor. Defaults to
    /// [`DuplicatePkPolicy::Refuse`] so a `LocalDb` opened without config is
    /// safe by default; the CLI overrides it from `[sync].duplicate_pk` via
    /// [`LocalDb::with_duplicate_pk`].
    duplicate_pk: DuplicatePkPolicy,
}

impl LocalDb {
    pub(crate) fn conn(&self) -> MutexGuard<'_, Connection> {
        self.conn.lock().expect("mutex poisoned")
    }

    /// Set the duplicate-`__pk` policy from `[sync].duplicate_pk`.
    ///
    /// Chainable at the open site so `open`/`open_readonly` keep their
    /// signatures and every caller that does not care keeps the safe default.
    pub fn with_duplicate_pk(mut self, policy: DuplicatePkPolicy) -> Self {
        self.duplicate_pk = policy;
        self
    }

    /// Open a local SQLite database
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        info!("Opening local database: {}", path.display());

        let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;

        Ok(Self {
            conn: Mutex::new(conn),
            duplicate_pk: DuplicatePkPolicy::default(),
        })
    }

    /// Open read-only for diff operations
    pub fn open_readonly(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        info!("Opening local database (read-only): {}", path.display());

        let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;

        Ok(Self {
            conn: Mutex::new(conn),
            duplicate_pk: DuplicatePkPolicy::default(),
        })
    }

    /// Get the database schema for table name validation.
    ///
    /// Queries sqlite_master to get all user tables.
    pub fn get_schema(&self) -> Result<TableSchema> {
        let conn = self.conn();
        let tables = list_tables_inner(&conn)?;
        Ok(TableSchema::new(tables))
    }

    /// The SQLite storage class actually stored in `table`.`column` -- one of
    /// `integer`, `real`, `text`, `blob` -- or `None` when the column does not
    /// exist or holds only NULLs.
    ///
    /// Bounded: the first non-NULL value decides. Used to detect two peers
    /// disagreeing about how a timestamp is represented, which SQLite will order
    /// confidently (its cross-class ordering is total) but not chronologically.
    pub fn stored_storage_class(&self, table: &str, column: &str) -> Result<Option<String>> {
        let conn = self.conn();
        let sql = format!(
            "SELECT typeof(\"{}\") FROM \"{}\" WHERE \"{}\" IS NOT NULL LIMIT 1",
            column, table, column
        );
        let mut stmt = conn.prepare(&sql)?;
        let mut rows = stmt.query([])?;
        match rows.next()? {
            Some(row) => Ok(Some(row.get::<_, String>(0)?)),
            None => Ok(None),
        }
    }

    /// Upsert rows under an explicit same-PK conflict [`UpsertGuard`].
    ///
    /// The unguarded [`DataSource::upsert_rows`] is `INSERT OR REPLACE` -- the
    /// received row always wins. This is the same batch write with the
    /// resolution policy compiled into the statement, so a peer's stale row can
    /// be turned away without a read-modify-write.
    ///
    /// Inherent to `LocalDb` rather than added to [`DataSource`]: a guarded
    /// write needs SQLite's `ON CONFLICT` and has no general remote analogue, so
    /// plugin-SDK implementors are unaffected.
    pub fn upsert_rows_guarded(
        &self,
        table: &str,
        rows: &[HashMap<String, JsonValue>],
        guard: UpsertGuard<'_>,
    ) -> Result<UpsertOutcome> {
        let conn = self.conn();
        upsert_rows_guarded_inner(&conn, table, rows, guard)
    }
}

impl DataSource for LocalDb {
    async fn list_tables(&self) -> Result<Vec<String>> {
        let conn = self.conn();
        list_tables_inner(&conn)
    }

    async fn table_info(&self, table: &str) -> Result<TableInfo> {
        let conn = self.conn();
        table_info_inner(&conn, table)
    }

    async fn get_row_metadata(
        &self,
        table: &str,
        timestamp_column: &str,
        exclude_columns: &[String],
    ) -> Result<HashMap<String, RowMeta>> {
        let conn = self.conn();
        get_row_metadata_inner(
            &conn,
            table,
            timestamp_column,
            exclude_columns,
            self.duplicate_pk,
        )
    }

    async fn get_rows(
        &self,
        table: &str,
        pk_values: &[String],
    ) -> Result<Vec<HashMap<String, JsonValue>>> {
        let conn = self.conn();
        get_rows_inner(&conn, table, pk_values)
    }

    async fn upsert_rows(&self, table: &str, rows: &[HashMap<String, JsonValue>]) -> Result<usize> {
        let conn = self.conn();
        upsert_rows_inner(&conn, table, rows)
    }

    async fn row_count(&self, table: &str) -> Result<usize> {
        let conn = self.conn();
        let sql = format!("SELECT COUNT(*) FROM \"{}\"", table);
        let count: usize = conn.query_row(&sql, [], |row| row.get(0))?;
        Ok(count)
    }

    async fn foreign_key_parents(&self, table: &str) -> Result<Vec<String>> {
        let conn = self.conn();
        foreign_key_parents_inner(&conn, table)
    }
}

// -- Internal functions that operate on a borrowed Connection --

fn list_tables_inner(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT name FROM sqlite_master
         WHERE type = 'table'
         AND name NOT LIKE 'sqlite_%'
         ORDER BY name",
    )?;

    let tables: Vec<String> = stmt
        .query_map([], |row| row.get(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    debug!("Found {} tables", tables.len());
    Ok(tables)
}

/// Parent tables `table` references, read straight from SQLite's own
/// dependency tracking (`PRAGMA foreign_key_list`) rather than parsed out of
/// the DDL text. Column 2 of each returned row is the referenced table name;
/// a composite foreign key spanning several columns to the same parent
/// produces one row per column, so callers that need a deduplicated edge set
/// (`sync::topological_table_order`) dedupe on their side.
///
/// Works on a read-only connection: it is a read-only pragma, not a write.
fn foreign_key_parents_inner(conn: &Connection, table: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(&format!("PRAGMA foreign_key_list(\"{}\")", table))?;
    let parents: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(2))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(parents)
}

fn table_info_inner(conn: &Connection, table: &str) -> Result<TableInfo> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info(\"{}\")", table))?;

    let columns: Vec<ColumnInfo> = stmt
        .query_map([], |row| {
            Ok(ColumnInfo {
                name: row.get(1)?,
                col_type: row.get(2)?,
                notnull: row.get::<_, i32>(3)? != 0,
                pk: row.get::<_, i32>(5)? != 0,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    if columns.is_empty() {
        return Err(SyncError::TableNotFound(table.to_string()));
    }

    let primary_key: Vec<String> = columns
        .iter()
        .filter(|c| c.pk)
        .map(|c| c.name.clone())
        .collect();

    if primary_key.is_empty() {
        return Err(SyncError::NoPrimaryKey(table.to_string()));
    }

    Ok(TableInfo {
        name: table.to_string(),
        columns,
        primary_key,
    })
}

fn get_row_metadata_inner(
    conn: &Connection,
    table: &str,
    timestamp_column: &str,
    exclude_columns: &[String],
    duplicate_pk: DuplicatePkPolicy,
) -> Result<HashMap<String, RowMeta>> {
    let info = table_info_inner(conn, table)?;
    let pk_cols = &info.primary_key;
    let has_timestamp = info.columns.iter().any(|c| c.name == timestamp_column);

    // Column definition order -- the hash folds these (minus timestamp/excluded
    // columns) in exactly this order, shared with the plugin and wasm paths.
    let column_order: Vec<String> = info.columns.iter().map(|c| c.name.clone()).collect();

    // Select the PK rendered as TEXT (`__pk`) plus every column. Hashing each
    // column's JSON value through `crate::rowhash` is what keeps the native hash
    // byte-identical to the JSON-based plugin/wasm hashes.
    let pk_expr = crate::rowhash::pk_text_expr(pk_cols);
    let col_list = column_order
        .iter()
        .map(|c| format!("\"{}\"", c))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT {} AS __pk, {} FROM \"{}\"",
        pk_expr, col_list, table
    );

    debug!("Executing: {}", sql);

    let mut stmt = conn.prepare(&sql)?;
    let mut result: HashMap<String, RowMeta> = HashMap::new();

    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        // A NULL `__pk` (a NULL part in a composite PK, since `||` propagates
        // NULL) cannot key pk-based sync. Coercing it to "" would collapse every
        // such row onto one map entry, silently dropping rows and provoking
        // spurious deletes. Skip and warn instead.
        let pk_value = match row.get::<_, Option<String>>(0)? {
            Some(pk) => pk,
            None => {
                warn!("skipping row in {} with NULL primary key", table);
                continue;
            }
        };

        // Build the column->JSON map (cols start at index 1, after __pk).
        let mut row_map: HashMap<String, JsonValue> = HashMap::with_capacity(column_order.len());
        for (i, name) in column_order.iter().enumerate() {
            row_map.insert(name.clone(), get_json_value(row, i + 1)?);
        }

        let content_hash = crate::rowhash::content_hash(
            &row_map,
            &column_order,
            exclude_columns,
            timestamp_column,
        );

        // updated_at carries either an integer Unix timestamp or a string.
        // Shared with the plugin and wasm metadata builders via the one
        // canonical extractor so the three renderings cannot drift.
        let updated_at: Option<String> = if has_timestamp {
            crate::datasource::extract_updated_at(row_map.get(timestamp_column))
        } else {
            None
        };

        // Two rows rendering the same PK text means the PK is not unique as
        // encoded, so the map can keep only one of them. Checked BEFORE the
        // insert, not after: under `Refuse` this returns while the metadata map
        // is still being built, which is upstream of the diff and of every
        // write, so no row is upserted (#269).
        if let Some(prev) = result.get(&pk_value) {
            if let Some(message) =
                duplicate_pk.check(table, &pk_value, &prev.content_hash, &content_hash)?
            {
                warn!("{}", message);
            }
        }

        result.insert(
            pk_value.clone(),
            RowMeta {
                pk_value: pk_value.clone(),
                updated_at,
                content_hash,
            },
        );
    }

    debug!("Got {} rows from {}", result.len(), table);
    Ok(result)
}

fn get_rows_inner(
    conn: &Connection,
    table: &str,
    pk_values: &[String],
) -> Result<Vec<HashMap<String, JsonValue>>> {
    if pk_values.is_empty() {
        return Ok(vec![]);
    }

    let info = table_info_inner(conn, table)?;
    let pk_cols = &info.primary_key;

    // Build primary key expression
    let pk_expr = pk_cols
        .iter()
        .map(|c| format!("\"{}\"", c))
        .collect::<Vec<_>>()
        .join(" || '|' || ");

    // Build column list
    let cols = info
        .columns
        .iter()
        .map(|c| format!("\"{}\"", c.name))
        .collect::<Vec<_>>()
        .join(", ");

    // Build IN clause
    let placeholders = pk_values.iter().map(|_| "?").collect::<Vec<_>>().join(", ");

    let sql = format!(
        "SELECT {} FROM \"{}\" WHERE {} IN ({})",
        cols, table, pk_expr, placeholders
    );

    debug!("Fetching {} rows from {}", pk_values.len(), table);

    let mut stmt = conn.prepare(&sql)?;

    let params: Vec<&dyn rusqlite::ToSql> = pk_values
        .iter()
        .map(|v| v as &dyn rusqlite::ToSql)
        .collect();

    let mut rows = stmt.query(params.as_slice())?;
    let mut result = Vec::new();

    while let Some(row) = rows.next()? {
        let mut row_data = HashMap::new();
        for (i, col) in info.columns.iter().enumerate() {
            let value = get_json_value(row, i)?;
            row_data.insert(col.name.clone(), value);
        }
        result.push(row_data);
    }

    Ok(result)
}

fn upsert_rows_inner(
    conn: &Connection,
    table: &str,
    rows: &[HashMap<String, JsonValue>],
) -> Result<usize> {
    Ok(upsert_rows_guarded_inner(conn, table, rows, UpsertGuard::Replace)?.applied)
}

/// How a received row resolves against an existing local row with the same
/// primary key.
///
/// The variant selects the SQL shape; every shape is a single statement per row
/// so the resolution rides *inside* the write and is atomic against a concurrent
/// local writer. A read-then-write predicate would be racy and would cost a
/// round trip per row on the hot apply path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpsertGuard<'a> {
    /// Overwrite unconditionally -- `INSERT OR REPLACE`. The historical
    /// multicast behavior, and what `remote_wins` means on the apply side.
    Replace,
    /// Keep the local row -- `ON CONFLICT DO NOTHING`. Rows absent locally are
    /// still inserted; an existing row is never touched. `local_wins`.
    KeepLocal,
    /// Accept only when the incoming row's ordering value is strictly greater
    /// than the stored row's -- `ON CONFLICT DO UPDATE ... WHERE`. `newer_wins`.
    ///
    /// The ordering value is `max` across the listed columns that actually exist
    /// on the table, computed identically for both sides of the comparison.
    NewerBy(&'a [String]),
}

/// Result of a guarded upsert batch.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UpsertOutcome {
    /// Rows the guard admitted (inserted or updated).
    pub applied: usize,
    /// Rows the guard turned away -- an older or equally-ordered incoming row
    /// under [`UpsertGuard::NewerBy`], or any colliding row under
    /// [`UpsertGuard::KeepLocal`]. Not an error: a rejection is the guard doing
    /// its job. Surfaced so an embedder can see that peer rows are losing.
    pub rejected: usize,
    /// The ordering columns requested were all absent from the table, so the
    /// batch fell back to [`UpsertGuard::Replace`]. The caller should say so
    /// once per table: the configuration claims an ordering that the schema
    /// cannot supply, and silently applying blind is how "I believe I am
    /// ordered and I am not" happens.
    pub ordering_unavailable: bool,
}

/// A null-tolerant `max` over `cols`, each qualified by `qualifier`.
///
/// SQLite's scalar `max(a, b, c)` returns NULL if *any* argument is NULL, which
/// would be fatal here: a live row with `deleted_at IS NULL` would produce a
/// NULL ordering value and lose every comparison. Rotating the column list
/// through `coalesce` gives each position a non-NULL fallback, so the `max` sees
/// NULLs only when *every* column is NULL -- which is exactly when the row has
/// no ordering signal at all.
///
/// One column degenerates to a bare column reference, which is the
/// single-`timestamp_column` behavior.
fn ordering_max_expr(qualifier: &str, cols: &[String]) -> String {
    let qualified: Vec<String> = cols
        .iter()
        .map(|c| format!("{}.\"{}\"", qualifier, c))
        .collect();

    if qualified.len() == 1 {
        return qualified[0].clone();
    }

    let args: Vec<String> = (0..qualified.len())
        .map(|i| {
            let rotated: Vec<&str> = (0..qualified.len())
                .map(|j| qualified[(i + j) % qualified.len()].as_str())
                .collect();
            format!("coalesce({})", rotated.join(", "))
        })
        .collect();

    format!("max({})", args.join(", "))
}

/// The conflict clause a [`UpsertGuard`] lowers to, once the destination
/// table's ordering support is known.
///
/// Separate from the guard because [`UpsertGuard::NewerBy`] on a table that
/// carries none of its ordering columns is indistinguishable from
/// [`UpsertGuard::Replace`] at SQL-generation time, and collapsing the two here
/// is what keeps the `ordering_unavailable` fallback from quietly missing a
/// change made to the `Replace` shape. (#324)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConflictShape {
    /// Overwrite an existing row unconditionally.
    Overwrite,
    /// Never touch an existing row.
    Keep,
    /// Overwrite only when the incoming ordering value is strictly greater.
    Newer,
}

/// The destination columns one row is written over: the columns the table has,
/// filtered to those the row actually carries, plus every primary-key column.
///
/// A column the row does not mention is left out of the statement entirely, so
/// the destination keeps whatever it already stored there. Deriving this from
/// the destination's `PRAGMA table_info` alone -- which is what this did before
/// #324 -- bound an absent column as an explicit NULL and destroyed the stored
/// value on every write arm that writes. `sync::transfer_rows` strips
/// `[sync].exclude_columns` from every row it sends, so on `pull` that was
/// silent local data loss.
///
/// Primary-key columns are forced in even when absent. They are the conflict
/// target; omitting one would let SQLite supply a default (a fresh rowid, for
/// an `INTEGER PRIMARY KEY`) and insert a new row rather than resolve against
/// the intended one. Binding NULL, as before, is the conservative behavior.
fn applied_columns<'a>(
    cols: &[&'a str],
    primary_key: &[String],
    row: &HashMap<String, JsonValue>,
) -> Vec<&'a str> {
    cols.iter()
        .copied()
        .filter(|c| row.contains_key(*c) || primary_key.iter().any(|pk| pk == c))
        .collect()
}

/// Build the upsert statement for one column set.
///
/// `apply_cols` covering every column of the table reproduces the pre-#324 SQL
/// byte for byte, so a deployment with no `exclude_columns` -- where every row
/// carries every column -- pays nothing and takes on none of the risks below.
///
/// A *subset* trades `INSERT OR REPLACE` for `ON CONFLICT(pk) DO UPDATE SET`
/// over the carried columns, because `INSERT OR REPLACE` is a DELETE followed
/// by an INSERT and therefore resets an unmentioned column to its schema
/// default. Two things are given up on that path, which is why it is confined
/// to rows that actually omit a column: `INSERT OR REPLACE` resolves a conflict
/// on ANY uniqueness constraint while `ON CONFLICT(pk)` resolves only the
/// primary key, and `INSERT OR REPLACE` fires DELETE triggers and
/// `ON DELETE CASCADE` where `DO UPDATE` does not.
fn build_upsert_sql(
    table: &str,
    info: &TableInfo,
    apply_cols: &[&str],
    shape: ConflictShape,
    ordering: &[String],
) -> String {
    let col_list = apply_cols
        .iter()
        .map(|c| format!("\"{}\"", c))
        .collect::<Vec<_>>()
        .join(", ");
    let placeholders = apply_cols
        .iter()
        .map(|_| "?")
        .collect::<Vec<_>>()
        .join(", ");

    // The conflict target must be the table's declared PRIMARY KEY columns.
    // `table_info_inner` already rejects a table without one (`NoPrimaryKey`),
    // so every table that reaches here has a target `ON CONFLICT` can bind to.
    let pk_target = info
        .primary_key
        .iter()
        .map(|c| format!("\"{}\"", c))
        .collect::<Vec<_>>()
        .join(", ");

    // The row carries every column the destination has, so nothing can be
    // preserved by omission and the historical statement is exactly right.
    let complete = apply_cols.len() == info.columns.len();

    // Only non-PK columns are assigned: the PK columns are equal by definition
    // of the conflict.
    let assignments = apply_cols
        .iter()
        .filter(|c| !info.primary_key.iter().any(|pk| pk == *c))
        .map(|c| format!("\"{0}\" = excluded.\"{0}\"", c))
        .collect::<Vec<_>>()
        .join(", ");

    // Three arms reach the same statement -- `KeepLocal`, and either of the
    // writing shapes on a row that carries nothing but the primary key -- so the
    // shape is written once.
    let do_nothing = || {
        format!(
            "INSERT INTO \"{}\" ({}) VALUES ({}) ON CONFLICT({}) DO NOTHING",
            table, col_list, placeholders, pk_target
        )
    };

    match shape {
        ConflictShape::Overwrite if complete => format!(
            "INSERT OR REPLACE INTO \"{}\" ({}) VALUES ({})",
            table, col_list, placeholders
        ),
        ConflictShape::Keep => do_nothing(),
        // A row carrying nothing but the primary key has nothing to assign, and
        // `DO UPDATE SET` with an empty assignment list is not valid SQL. There
        // is also nothing to write: every non-PK column is being preserved.
        // Such a row reports as `rejected` rather than `applied` -- the
        // statement changes no rows, and the counters read the statement.
        ConflictShape::Overwrite if assignments.is_empty() => do_nothing(),
        ConflictShape::Overwrite => format!(
            "INSERT INTO \"{}\" ({}) VALUES ({}) ON CONFLICT({}) DO UPDATE SET {}",
            table, col_list, placeholders, pk_target, assignments
        ),
        ConflictShape::Newer => {
            // Only the ordering columns THIS row carries contribute to the
            // incoming value. SQLite resolves `excluded."c"` for a column left
            // out of the INSERT to that column's schema DEFAULT, not to NULL
            // (measured -- see `excluded_reads_the_schema_default_for_an_omitted_column`),
            // so reading a stripped ordering column off `excluded` would let a
            // `DEFAULT CURRENT_TIMESTAMP` win comparisons the pre-#324 NULL bind
            // lost. The rotated-coalesce max is unchanged by dropping columns
            // that would have been bound NULL, so this is exactly the pre-#324
            // comparison.
            let incoming_cols: Vec<String> = ordering
                .iter()
                .filter(|c| apply_cols.contains(&c.as_str()))
                .cloned()
                .collect();

            // No ordering column survived the strip, so the row carries no
            // ordering signal and must never displace a stored row -- which is
            // what the `{incoming} IS NOT NULL` arm did when every ordering
            // column was bound NULL. A new row is still inserted.
            //
            // The empty-assignment arm is the same reasoning as under
            // `Overwrite`, and is deliberately confined to the subset path: a
            // table whose every column is part of its primary key emits an
            // empty `DO UPDATE SET` here, which SQLite rejects. That is a
            // pre-existing defect of this arm, not one this change introduces,
            // and leaving the complete path untouched is what keeps its SQL
            // byte-identical.
            if incoming_cols.is_empty() || (!complete && assignments.is_empty()) {
                return do_nothing();
            }

            let incoming = ordering_max_expr("excluded", &incoming_cols);
            let stored = ordering_max_expr(&format!("\"{}\"", table), ordering);
            // An incoming row with no ordering value never displaces a stored
            // row; a stored row with no ordering value never blocks one. Both
            // arms are needed and both are symmetric across peers, so exactly
            // one side of any pair accepts and the mesh quiesces.
            format!(
                "INSERT INTO \"{table}\" ({col_list}) VALUES ({placeholders}) \
                 ON CONFLICT({pk_target}) DO UPDATE SET {assignments} \
                 WHERE {incoming} IS NOT NULL \
                 AND ({stored} IS NULL OR {incoming} > {stored})"
            )
        }
    }
}

/// Rows of one batch bucketed by the destination columns they are written over.
type ColumnGroups<'a> = Vec<(Vec<&'a str>, Vec<&'a HashMap<String, JsonValue>>)>;

fn upsert_rows_guarded_inner(
    conn: &Connection,
    table: &str,
    rows: &[HashMap<String, JsonValue>],
    guard: UpsertGuard<'_>,
) -> Result<UpsertOutcome> {
    if rows.is_empty() {
        return Ok(UpsertOutcome::default());
    }

    let info = table_info_inner(conn, table)?;
    let cols: Vec<&str> = info.columns.iter().map(|c| c.name.as_str()).collect();

    // Ordering columns the table does not have cannot participate. If none of
    // them exist, there is no ordering signal and the guard degrades to
    // Replace -- reported, never silent. Derived from the table's schema, so it
    // is the same answer for every row in the batch.
    let mut ordering_unavailable = false;
    let ordering: Vec<String> = match guard {
        UpsertGuard::NewerBy(want) => {
            let present: Vec<String> = want
                .iter()
                .filter(|c| info.columns.iter().any(|ci| &&ci.name == c))
                .cloned()
                .collect();
            if present.is_empty() {
                ordering_unavailable = true;
            }
            present
        }
        _ => Vec::new(),
    };

    let shape = match guard {
        UpsertGuard::Replace => ConflictShape::Overwrite,
        UpsertGuard::KeepLocal => ConflictShape::Keep,
        UpsertGuard::NewerBy(_) if ordering_unavailable => ConflictShape::Overwrite,
        UpsertGuard::NewerBy(_) => ConflictShape::Newer,
    };

    // A sender that strips columns strips them uniformly, so this is one group
    // in practice -- but nothing in the trait promises it, and a batch mixing
    // key sets must not be written under one row's column list.
    let mut groups: ColumnGroups = Vec::new();
    for row in rows {
        let apply_cols = applied_columns(&cols, &info.primary_key, row);
        match groups.iter_mut().find(|(k, _)| *k == apply_cols) {
            Some((_, members)) => members.push(row),
            None => groups.push((apply_cols, vec![row])),
        }
    }

    let tx = conn.unchecked_transaction()?;
    let mut applied = 0;
    let mut rejected = 0;

    for (apply_cols, members) in &groups {
        let sql = build_upsert_sql(table, &info, apply_cols, shape, &ordering);
        debug!("Upserting {} rows into {}: {}", members.len(), table, sql);

        let mut stmt = tx.prepare(&sql)?;
        for row in members {
            let params: Vec<JsonToSql> = apply_cols
                .iter()
                .map(|col| JsonToSql(row.get(*col).cloned().unwrap_or(JsonValue::Null)))
                .collect();

            let param_refs: Vec<&dyn rusqlite::ToSql> =
                params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();

            // A guard that turns a row away reports 0 changed rows, so the
            // statement's own count -- not the batch length -- is the truth
            // about what landed.
            if stmt.execute(param_refs.as_slice())? > 0 {
                applied += 1;
            } else {
                rejected += 1;
            }
        }
    }

    tx.commit()?;
    if rejected > 0 {
        info!(
            "Upserted {} rows into {} ({} turned away by the conflict guard)",
            applied, table, rejected
        );
    } else {
        info!("Upserted {} rows into {}", applied, table);
    }
    Ok(UpsertOutcome {
        applied,
        rejected,
        ordering_unavailable,
    })
}

/// Convert a row value to JSON by its SQLite storage class.
///
/// Inspecting the `ValueRef` once distinguishes a genuine SQL NULL (-> JSON
/// null) from a value that fails to decode. Previously every branch was a
/// `get::<T>` whose `Err` was discarded, so a TEXT column holding non-UTF-8
/// bytes fell through all four and was returned as NULL -- folding into the
/// content hash indistinguishably from a real NULL (a stable-but-wrong hash,
/// silent divergence). Such a value now surfaces a conversion error instead of
/// being silently swallowed. (#180)
///
/// Behavior is otherwise unchanged: Integer/Real become JSON numbers, valid
/// UTF-8 Text becomes a JSON string, Blob becomes lowercase hex (the rowhash
/// wire contract) -- identical to the previous typed reads.
fn get_json_value(row: &Row, idx: usize) -> Result<JsonValue> {
    use rusqlite::types::{Type, ValueRef};
    match row.get_ref(idx)? {
        ValueRef::Null => Ok(JsonValue::Null),
        ValueRef::Integer(i) => Ok(JsonValue::from(i)),
        ValueRef::Real(f) => Ok(JsonValue::from(f)),
        ValueRef::Text(bytes) => match std::str::from_utf8(bytes) {
            Ok(s) => Ok(JsonValue::from(s.to_string())),
            Err(e) => {
                Err(rusqlite::Error::FromSqlConversionFailure(idx, Type::Text, Box::new(e)).into())
            }
        },
        ValueRef::Blob(b) => Ok(JsonValue::String(hex::encode(b))),
    }
}

/// Wrapper to allow JSON values as SQL parameters
struct JsonToSql(JsonValue);

impl rusqlite::ToSql for JsonToSql {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        use rusqlite::types::{ToSqlOutput, Value};
        match &self.0 {
            JsonValue::Null => Ok(ToSqlOutput::Owned(Value::Null)),
            JsonValue::Bool(b) => Ok(ToSqlOutput::Owned(Value::Integer(if *b { 1 } else { 0 }))),
            JsonValue::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(ToSqlOutput::Owned(Value::Integer(i)))
                } else if let Some(f) = n.as_f64() {
                    Ok(ToSqlOutput::Owned(Value::Real(f)))
                } else {
                    Ok(ToSqlOutput::Owned(Value::Text(n.to_string())))
                }
            }
            JsonValue::String(s) => Ok(ToSqlOutput::Owned(Value::Text(s.clone()))),
            JsonValue::Array(_) | JsonValue::Object(_) => {
                Ok(ToSqlOutput::Owned(Value::Text(self.0.to_string())))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn one_row_conn(value_sql: &str) -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(&format!(
            "CREATE TABLE t (v); INSERT INTO t VALUES ({});",
            value_sql
        ))
        .unwrap();
        conn
    }

    fn get_col0(conn: &Connection) -> Result<JsonValue> {
        let mut stmt = conn.prepare("SELECT v FROM t").unwrap();
        let mut rows = stmt.query([]).unwrap();
        let row = rows.next().unwrap().unwrap();
        get_json_value(row, 0)
    }

    #[test]
    fn get_json_value_errors_on_non_utf8_text() {
        // Regression for #180: a TEXT column holding non-UTF-8 bytes (a lone 0xFF
        // cast to TEXT) previously fell through every typed read and was returned
        // as NULL -- hashing identically to a real NULL. It must now surface an
        // error instead of being silently swallowed.
        let conn = one_row_conn("CAST(x'ff' AS TEXT)");
        assert!(
            get_col0(&conn).is_err(),
            "non-UTF-8 text must error, not fold to NULL"
        );
    }

    #[test]
    fn get_json_value_null_stays_null() {
        // The other side of the distinction: a genuine SQL NULL is still Ok(Null),
        // not an error -- the fix does not conflate empty with unreadable.
        let conn = one_row_conn("NULL");
        assert_eq!(get_col0(&conn).unwrap(), JsonValue::Null);
    }

    // -- Guarded upsert (#310) --

    /// A table shaped like legion's: an ordering key that is the max over three
    /// columns, one of which (`deleted_at`) is NULL on every live row.
    /// A table holding two DISTINCT rows whose `__pk` renders identically.
    ///
    /// The column is declared `BLOB`, so it has SQLite affinity NONE and stores
    /// each value in the storage class it arrives as. The integer `1` and the
    /// text `'1'` are different values to the primary-key unique index (SQLite
    /// orders INTEGER before TEXT, so they never compare equal), which is why
    /// SQLite accepts both -- but `pk_text_expr` renders a single-column PK as
    /// `CAST(col AS TEXT)`, and both cast to `"1"`.
    ///
    /// This is the real cross-node shape, not a contrivance: node A minted the
    /// key as a number and node B minted the same key as a string, so the two
    /// nodes hold different logical rows under one `__pk`. Composite PKs cannot
    /// reach this state -- `pk_text_expr` escapes the delimiter and is injective.
    fn colliding_pk_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE items (
                 id BLOB PRIMARY KEY,
                 name TEXT,
                 updated_at TEXT
             );
             INSERT INTO items VALUES (1, 'alice', '2026-01-01');
             INSERT INTO items VALUES ('1', 'bob', '2026-01-02');",
        )
        .unwrap();
        conn
    }

    #[test]
    fn duplicate_pk_refuses_by_default() {
        // #269: the default policy stops the sync. Before this, the second row
        // silently evicted the first from the metadata map and the sync
        // continued -- the evicted row then read as a row to delete.
        let conn = colliding_pk_conn();

        let err =
            get_row_metadata_inner(&conn, "items", "updated_at", &[], DuplicatePkPolicy::Refuse)
                .expect_err("two rows rendering __pk '1' must refuse under the default policy");

        match err {
            SyncError::DuplicatePrimaryKey {
                table,
                pk,
                first_hash,
                second_hash,
            } => {
                assert_eq!(table, "items");
                assert_eq!(pk, "1");
                // Both hashes must be present and distinct: they are what lets an
                // operator tell WHICH two rows collided. Reporting one (or the
                // same one twice) would name the collision without locating it.
                assert_ne!(
                    first_hash, second_hash,
                    "the two colliding rows differ, so their content hashes must differ"
                );
                assert!(!first_hash.is_empty() && !second_hash.is_empty());
            }
            other => panic!("expected DuplicatePrimaryKey, got {other:?}"),
        }
    }

    #[test]
    fn duplicate_pk_refusal_names_both_rows_in_its_message() {
        // The rendered message is the whole remedy surface -- an operator with
        // only "duplicate primary key" cannot find the two rows. Pin that the
        // table, the key, and both hashes reach the text a user actually sees.
        let conn = colliding_pk_conn();
        let err =
            get_row_metadata_inner(&conn, "items", "updated_at", &[], DuplicatePkPolicy::Refuse)
                .expect_err("must refuse");
        let (first, second) = match &err {
            SyncError::DuplicatePrimaryKey {
                first_hash,
                second_hash,
                ..
            } => (first_hash.clone(), second_hash.clone()),
            other => panic!("expected DuplicatePrimaryKey, got {other:?}"),
        };

        let msg = err.to_string();
        assert!(msg.contains("items"), "message must name the table: {msg}");
        assert!(msg.contains('1'), "message must name the key: {msg}");
        assert!(
            msg.contains(&first),
            "message must carry both hashes: {msg}"
        );
        assert!(
            msg.contains(&second),
            "message must carry both hashes: {msg}"
        );
    }

    #[test]
    fn duplicate_pk_warn_policy_keeps_the_previous_behavior() {
        // The escape hatch: `warn` restores pre-#269 overwrite-and-continue, so
        // a deployment that cannot re-key immediately is not hard-stopped. The
        // map keeps ONE entry for the colliding key -- that is the data loss the
        // default now refuses, made explicit here so nobody reads `warn` as safe.
        let conn = colliding_pk_conn();

        let meta =
            get_row_metadata_inner(&conn, "items", "updated_at", &[], DuplicatePkPolicy::Warn)
                .expect("warn policy must not stop the sync");

        assert_eq!(
            meta.len(),
            1,
            "warn keeps the old behavior: the colliding rows collapse onto one entry"
        );
        assert!(meta.contains_key("1"));
    }

    #[test]
    fn distinct_pks_are_unaffected_by_the_refusal() {
        // Guard against over-refusing: the default policy must be invisible to
        // every table whose keys are actually unique, which is all of them.
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT, updated_at TEXT);
             INSERT INTO items VALUES ('a', 'alice', '2026-01-01');
             INSERT INTO items VALUES ('b', 'bob', '2026-01-02');",
        )
        .unwrap();

        let meta =
            get_row_metadata_inner(&conn, "items", "updated_at", &[], DuplicatePkPolicy::Refuse)
                .expect("unique keys must not refuse");

        assert_eq!(meta.len(), 2);
    }

    fn ordered_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE rows_t (
                 id TEXT PRIMARY KEY,
                 v TEXT,
                 created_at TEXT,
                 updated_at TEXT,
                 deleted_at TEXT
             );",
        )
        .unwrap();
        conn
    }

    fn row(
        id: &str,
        v: &str,
        created: Option<&str>,
        updated: Option<&str>,
        deleted: Option<&str>,
    ) -> HashMap<String, JsonValue> {
        let j = |o: Option<&str>| o.map(JsonValue::from).unwrap_or(JsonValue::Null);
        HashMap::from([
            ("id".to_string(), JsonValue::from(id)),
            ("v".to_string(), JsonValue::from(v)),
            ("created_at".to_string(), j(created)),
            ("updated_at".to_string(), j(updated)),
            ("deleted_at".to_string(), j(deleted)),
        ])
    }

    fn read_v(conn: &Connection, id: &str) -> String {
        conn.query_row("SELECT v FROM rows_t WHERE id = ?1", [id], |r| r.get(0))
            .unwrap()
    }

    fn legion_ordering() -> Vec<String> {
        vec![
            "created_at".to_string(),
            "updated_at".to_string(),
            "deleted_at".to_string(),
        ]
    }

    #[test]
    fn ordering_max_expr_single_column_is_a_bare_reference() {
        // One entry must degenerate to exactly the single-timestamp_column
        // behavior -- no coalesce/max wrapper to change its meaning.
        assert_eq!(
            ordering_max_expr("excluded", &["updated_at".to_string()]),
            "excluded.\"updated_at\""
        );
    }

    #[test]
    fn ordering_max_expr_tolerates_nulls_via_rotation() {
        // The rotation exists because SQLite's scalar max() returns NULL if ANY
        // argument is NULL. Every position must appear first in some coalesce,
        // or a NULL in that position would poison the result.
        let e = ordering_max_expr("excluded", &legion_ordering());
        for c in ["created_at", "updated_at", "deleted_at"] {
            assert!(
                e.contains(&format!("coalesce(excluded.\"{}\"", c)),
                "{} never leads a coalesce in {}",
                c,
                e
            );
        }
    }

    #[test]
    fn newer_by_rejects_an_older_row_and_accepts_a_newer_one() {
        let conn = ordered_conn();
        let ordering = legion_ordering();
        upsert_rows_guarded_inner(
            &conn,
            "rows_t",
            &[row(
                "a",
                "local",
                Some("2026-01-01"),
                Some("2026-05-01"),
                None,
            )],
            UpsertGuard::NewerBy(&ordering),
        )
        .unwrap();

        // Older peer row: turned away, counted, local content intact.
        let older = upsert_rows_guarded_inner(
            &conn,
            "rows_t",
            &[row(
                "a",
                "stale",
                Some("2026-01-01"),
                Some("2026-04-01"),
                None,
            )],
            UpsertGuard::NewerBy(&ordering),
        )
        .unwrap();
        assert_eq!((older.applied, older.rejected), (0, 1));
        assert_eq!(read_v(&conn, "a"), "local");

        // Newer peer row: accepted.
        let newer = upsert_rows_guarded_inner(
            &conn,
            "rows_t",
            &[row(
                "a",
                "fresh",
                Some("2026-01-01"),
                Some("2026-06-01"),
                None,
            )],
            UpsertGuard::NewerBy(&ordering),
        )
        .unwrap();
        assert_eq!((newer.applied, newer.rejected), (1, 0));
        assert_eq!(read_v(&conn, "a"), "fresh");
    }

    #[test]
    fn newer_by_keeps_the_local_row_on_an_exact_tie() {
        // Strict greater-than: two writes at the identical instant stay
        // divergent rather than flapping. Matches the remote path's tie rule.
        let conn = ordered_conn();
        let ordering = legion_ordering();
        let base = row("a", "local", Some("2026-01-01"), Some("2026-05-01"), None);
        upsert_rows_guarded_inner(&conn, "rows_t", &[base], UpsertGuard::NewerBy(&ordering))
            .unwrap();

        let tie = upsert_rows_guarded_inner(
            &conn,
            "rows_t",
            &[row(
                "a",
                "peer",
                Some("2026-01-01"),
                Some("2026-05-01"),
                None,
            )],
            UpsertGuard::NewerBy(&ordering),
        )
        .unwrap();
        assert_eq!((tie.applied, tie.rejected), (0, 1));
        assert_eq!(read_v(&conn, "a"), "local");
    }

    #[test]
    fn tombstone_setting_only_deleted_at_wins() {
        // legion's exact shape, and the reason the ordering signal is a LIST:
        // the tombstone stamps deleted_at and leaves updated_at alone. Under a
        // single-column compare on updated_at this is a tie and the delete is
        // rejected -- the row resurrects.
        let conn = ordered_conn();
        let ordering = legion_ordering();
        upsert_rows_guarded_inner(
            &conn,
            "rows_t",
            &[row(
                "a",
                "live",
                Some("2026-01-01"),
                Some("2026-05-01"),
                None,
            )],
            UpsertGuard::NewerBy(&ordering),
        )
        .unwrap();

        let tombstone = upsert_rows_guarded_inner(
            &conn,
            "rows_t",
            &[row(
                "a",
                "gone",
                Some("2026-01-01"),
                Some("2026-05-01"),
                Some("2026-06-01"),
            )],
            UpsertGuard::NewerBy(&ordering),
        )
        .unwrap();
        assert_eq!((tombstone.applied, tombstone.rejected), (1, 0));
        assert_eq!(read_v(&conn, "a"), "gone");

        // And the same tombstone must not be undone by a later live row whose
        // max is lower.
        let resurrect = upsert_rows_guarded_inner(
            &conn,
            "rows_t",
            &[row(
                "a",
                "back",
                Some("2026-01-01"),
                Some("2026-05-02"),
                None,
            )],
            UpsertGuard::NewerBy(&ordering),
        )
        .unwrap();
        assert_eq!((resurrect.applied, resurrect.rejected), (0, 1));
        assert_eq!(read_v(&conn, "a"), "gone");
    }

    #[test]
    fn newer_by_handles_a_missing_ordering_value_on_either_side() {
        let conn = ordered_conn();
        let ordering = legion_ordering();

        // Stored row has no ordering signal at all -> it cannot block anything.
        upsert_rows_guarded_inner(
            &conn,
            "rows_t",
            &[row("a", "unstamped", None, None, None)],
            UpsertGuard::NewerBy(&ordering),
        )
        .unwrap();
        let over = upsert_rows_guarded_inner(
            &conn,
            "rows_t",
            &[row("a", "stamped", None, Some("2026-01-01"), None)],
            UpsertGuard::NewerBy(&ordering),
        )
        .unwrap();
        assert_eq!((over.applied, over.rejected), (1, 0));
        assert_eq!(read_v(&conn, "a"), "stamped");

        // Incoming row has no ordering signal -> it never displaces one that
        // does. The two arms are symmetric, so across a pair of peers exactly
        // one side accepts and the exchange terminates.
        let blind = upsert_rows_guarded_inner(
            &conn,
            "rows_t",
            &[row("a", "unstamped-peer", None, None, None)],
            UpsertGuard::NewerBy(&ordering),
        )
        .unwrap();
        assert_eq!((blind.applied, blind.rejected), (0, 1));
        assert_eq!(read_v(&conn, "a"), "stamped");
    }

    #[test]
    fn newer_by_falls_back_to_replace_when_the_table_has_no_ordering_column() {
        // Configuration claims an ordering the schema cannot supply. Applying
        // blind is the terminal choice, but it must be REPORTED -- a user who
        // believes they are ordered and is not is the failure this guard exists
        // to remove.
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE plain (id TEXT PRIMARY KEY, v TEXT);")
            .unwrap();
        let mk = |v: &str| {
            HashMap::from([
                ("id".to_string(), JsonValue::from("a")),
                ("v".to_string(), JsonValue::from(v)),
            ])
        };
        upsert_rows_guarded_inner(&conn, "plain", &[mk("first")], UpsertGuard::Replace).unwrap();

        let ordering = legion_ordering();
        let out = upsert_rows_guarded_inner(
            &conn,
            "plain",
            &[mk("second")],
            UpsertGuard::NewerBy(&ordering),
        )
        .unwrap();
        assert!(out.ordering_unavailable, "the caller must be told");
        assert_eq!(out.applied, 1);
        let v: String = conn
            .query_row("SELECT v FROM plain WHERE id = 'a'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, "second", "fallback is blind overwrite, as before");
    }

    #[test]
    fn newer_by_uses_only_the_ordering_columns_the_table_actually_has() {
        // A table with updated_at but no created_at/deleted_at still orders --
        // the absent columns are dropped from the comparison rather than
        // disabling it.
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE partial (id TEXT PRIMARY KEY, v TEXT, updated_at TEXT);")
            .unwrap();
        let mk = |v: &str, ts: &str| {
            HashMap::from([
                ("id".to_string(), JsonValue::from("a")),
                ("v".to_string(), JsonValue::from(v)),
                ("updated_at".to_string(), JsonValue::from(ts)),
            ])
        };
        let ordering = legion_ordering();
        upsert_rows_guarded_inner(
            &conn,
            "partial",
            &[mk("local", "2026-05-01")],
            UpsertGuard::NewerBy(&ordering),
        )
        .unwrap();
        let out = upsert_rows_guarded_inner(
            &conn,
            "partial",
            &[mk("stale", "2026-04-01")],
            UpsertGuard::NewerBy(&ordering),
        )
        .unwrap();
        assert!(!out.ordering_unavailable);
        assert_eq!((out.applied, out.rejected), (0, 1));
    }

    #[test]
    fn newer_by_guards_a_composite_primary_key() {
        // The conflict target is the table's declared PK columns, not the
        // rendered `__pk` text, which is a lookup key and not a constraint.
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE pair (p TEXT, q TEXT, v TEXT, updated_at TEXT, PRIMARY KEY (p, q));",
        )
        .unwrap();
        let mk = |v: &str, ts: &str| {
            HashMap::from([
                ("p".to_string(), JsonValue::from("x")),
                ("q".to_string(), JsonValue::from("y")),
                ("v".to_string(), JsonValue::from(v)),
                ("updated_at".to_string(), JsonValue::from(ts)),
            ])
        };
        let ordering = vec!["updated_at".to_string()];
        upsert_rows_guarded_inner(
            &conn,
            "pair",
            &[mk("local", "2026-05-01")],
            UpsertGuard::NewerBy(&ordering),
        )
        .unwrap();
        let out = upsert_rows_guarded_inner(
            &conn,
            "pair",
            &[mk("stale", "2026-01-01")],
            UpsertGuard::NewerBy(&ordering),
        )
        .unwrap();
        assert_eq!((out.applied, out.rejected), (0, 1));
        let v: String = conn
            .query_row("SELECT v FROM pair WHERE p='x' AND q='y'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, "local");
    }

    #[test]
    fn keep_local_inserts_new_rows_but_never_overwrites() {
        let conn = ordered_conn();
        upsert_rows_guarded_inner(
            &conn,
            "rows_t",
            &[row("a", "local", None, Some("2026-01-01"), None)],
            UpsertGuard::Replace,
        )
        .unwrap();

        let out = upsert_rows_guarded_inner(
            &conn,
            "rows_t",
            &[
                row("a", "peer", None, Some("2027-01-01"), None),
                row("b", "peer", None, Some("2027-01-01"), None),
            ],
            UpsertGuard::KeepLocal,
        )
        .unwrap();
        assert_eq!((out.applied, out.rejected), (1, 1));
        assert_eq!(read_v(&conn, "a"), "local", "even a newer peer row loses");
        assert_eq!(read_v(&conn, "b"), "peer", "absent rows still arrive");
    }

    #[test]
    fn replace_still_overwrites_blindly() {
        // The default multicast policy is remote_wins, and it must behave
        // exactly as INSERT OR REPLACE always did -- including accepting a row
        // that is older by every ordering column.
        let conn = ordered_conn();
        upsert_rows_guarded_inner(
            &conn,
            "rows_t",
            &[row("a", "local", None, Some("2026-05-01"), None)],
            UpsertGuard::Replace,
        )
        .unwrap();
        let out = upsert_rows_guarded_inner(
            &conn,
            "rows_t",
            &[row("a", "older-peer", None, Some("2020-01-01"), None)],
            UpsertGuard::Replace,
        )
        .unwrap();
        assert_eq!((out.applied, out.rejected), (1, 0));
        assert_eq!(read_v(&conn, "a"), "older-peer");
    }

    #[test]
    fn deleted_at_only_write_changes_the_content_hash() {
        // The seam that would break legion silently: `deleted_at` must stay
        // VISIBLE to the content hash. `rowhash` excludes updated_at/created_at
        // and the configured timestamp_column; if the ordering-column list were
        // ever folded into that exclusion set, a tombstone that only stamps
        // deleted_at would hash identically to the live row, produce no digest
        // mismatch, and never be gossiped at all -- while the config says
        // newer_wins.
        let conn = ordered_conn();
        conn.execute(
            "INSERT INTO rows_t VALUES ('a','v','2026-01-01','2026-05-01',NULL)",
            [],
        )
        .unwrap();
        let before = get_row_metadata_inner(
            &conn,
            "rows_t",
            "updated_at",
            &[],
            DuplicatePkPolicy::default(),
        )
        .unwrap();

        conn.execute(
            "UPDATE rows_t SET deleted_at = '2026-06-01' WHERE id = 'a'",
            [],
        )
        .unwrap();
        let after = get_row_metadata_inner(
            &conn,
            "rows_t",
            "updated_at",
            &[],
            DuplicatePkPolicy::default(),
        )
        .unwrap();

        assert_ne!(
            before["a"].content_hash, after["a"].content_hash,
            "a deleted_at-only write must change the digest, or tombstones never propagate"
        );
    }

    /// #310 asks that both transports order identically. The remote path orders
    /// with `diff::compare_ts` in Rust; the LAN path cannot, because its
    /// comparison has to ride inside the write to stay atomic against a
    /// concurrent local writer. So the guarantee is pinned here instead: for
    /// every pair the documented precondition admits -- both sides the same
    /// storage class -- the SQL guard and `compare_ts` must reach the same
    /// verdict, or a row would sync one way over D1 and the other way over the
    /// LAN.
    #[test]
    fn sql_guard_agrees_with_compare_ts_on_same_class_pairs() {
        use std::cmp::Ordering;

        // Same-class pairs only. Numeric pairs are where lexical comparison
        // would be wrong ("1000" sorts before "999"), text pairs are where
        // numeric parsing would be.
        let cases: [(JsonValue, JsonValue, &str, &str); 6] = [
            (
                JsonValue::from(999i64),
                JsonValue::from(1000i64),
                "999",
                "1000",
            ),
            (
                JsonValue::from(1000i64),
                JsonValue::from(999i64),
                "1000",
                "999",
            ),
            (
                JsonValue::from(2.5f64),
                JsonValue::from(10.5f64),
                "2.5",
                "10.5",
            ),
            (
                JsonValue::from("2026-01-01T00:00:00+00:00"),
                JsonValue::from("2026-02-01T00:00:00+00:00"),
                "2026-01-01T00:00:00+00:00",
                "2026-02-01T00:00:00+00:00",
            ),
            (
                JsonValue::from("2026-02-01T00:00:00+00:00"),
                JsonValue::from("2026-01-01T00:00:00+00:00"),
                "2026-02-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
            ),
            (
                JsonValue::from("2026-01-01T00:00:00+00:00"),
                JsonValue::from("2026-01-01T00:00:00+00:00"),
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
            ),
        ];

        let ordering = vec!["updated_at".to_string()];
        for (stored, incoming, stored_s, incoming_s) in cases {
            let conn = Connection::open_in_memory().unwrap();
            conn.execute_batch("CREATE TABLE t (id TEXT PRIMARY KEY, updated_at);")
                .unwrap();
            let mk = |ts: &JsonValue| {
                HashMap::from([
                    ("id".to_string(), JsonValue::from("a")),
                    ("updated_at".to_string(), ts.clone()),
                ])
            };
            upsert_rows_guarded_inner(&conn, "t", &[mk(&stored)], UpsertGuard::Replace).unwrap();
            let out = upsert_rows_guarded_inner(
                &conn,
                "t",
                &[mk(&incoming)],
                UpsertGuard::NewerBy(&ordering),
            )
            .unwrap();

            // The guard accepts exactly when the incoming value is strictly
            // greater, which is what Ordering::Greater means to compare_ts.
            let sql_accepted = out.applied == 1;
            let rust_accepted =
                crate::diff::compare_ts(incoming_s, stored_s) == Some(Ordering::Greater);
            assert_eq!(
                sql_accepted, rust_accepted,
                "guard and compare_ts disagree on incoming {incoming_s} vs stored {stored_s}"
            );
        }
    }

    /// The one pair where they deliberately differ, recorded so the divergence
    /// is a decision rather than a surprise. `compare_ts` returns `None` on a
    /// numeric-vs-text pair rather than inventing a winner; SQLite's ordering
    /// across storage classes is total and picks one. That difference is what
    /// makes the LAN path terminate -- exactly one peer accepts, so the mesh
    /// quiesces instead of re-firing Want/Delta forever -- at the cost of a
    /// winner that is arbitrary rather than chronological, which is why
    /// `Gossip::ordering_notes` reports it.
    #[test]
    fn sql_guard_orders_a_mixed_representation_that_compare_ts_abstains_on() {
        assert_eq!(
            crate::diff::compare_ts("1800000000", "2026-01-01T00:00:00+00:00"),
            None,
            "compare_ts must not invent a winner across representations"
        );

        let ordering = vec!["updated_at".to_string()];
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE t (id TEXT PRIMARY KEY, updated_at);")
            .unwrap();
        let mk = |ts: JsonValue| {
            HashMap::from([
                ("id".to_string(), JsonValue::from("a")),
                ("updated_at".to_string(), ts),
            ])
        };

        // Integer stored, ISO text incoming: TEXT sorts above INTEGER, accepted.
        upsert_rows_guarded_inner(
            &conn,
            "t",
            &[mk(JsonValue::from(1_800_000_000i64))],
            UpsertGuard::Replace,
        )
        .unwrap();
        let text_over_int = upsert_rows_guarded_inner(
            &conn,
            "t",
            &[mk(JsonValue::from("2026-01-01T00:00:00+00:00"))],
            UpsertGuard::NewerBy(&ordering),
        )
        .unwrap();
        assert_eq!(text_over_int.applied, 1);

        // The mirror image is rejected -- so across a pair of peers exactly one
        // side accepts and the exchange terminates.
        let int_over_text = upsert_rows_guarded_inner(
            &conn,
            "t",
            &[mk(JsonValue::from(1_800_000_000i64))],
            UpsertGuard::NewerBy(&ordering),
        )
        .unwrap();
        assert_eq!(int_over_text.applied, 0);
        assert_eq!(int_over_text.rejected, 1);
    }

    #[test]
    fn stored_storage_class_reports_the_first_non_null_class() {
        let conn = ordered_conn();
        conn.execute(
            "INSERT INTO rows_t VALUES ('a','v',NULL,'2026-05-01',NULL)",
            [],
        )
        .unwrap();
        // Bypass LocalDb::open (which needs a file) by exercising the same SQL.
        let class: Option<String> = conn
            .query_row(
                "SELECT typeof(\"updated_at\") FROM \"rows_t\" WHERE \"updated_at\" IS NOT NULL LIMIT 1",
                [],
                |r| r.get(0),
            )
            .ok();
        assert_eq!(class.as_deref(), Some("text"));
        let all_null: Option<String> = conn
            .query_row(
                "SELECT typeof(\"deleted_at\") FROM \"rows_t\" WHERE \"deleted_at\" IS NOT NULL LIMIT 1",
                [],
                |r| r.get(0),
            )
            .ok();
        assert_eq!(all_null, None, "an all-NULL column has no class to compare");
    }

    #[test]
    fn get_json_value_preserves_normal_types() {
        // Behavior preservation: int/real/text/blob render exactly as the prior
        // typed reads did, so content hashes are unchanged.
        assert_eq!(
            get_col0(&one_row_conn("42")).unwrap(),
            JsonValue::from(42i64)
        );
        assert_eq!(
            get_col0(&one_row_conn("2.5")).unwrap(),
            JsonValue::from(2.5f64)
        );
        assert_eq!(
            get_col0(&one_row_conn("'hello'")).unwrap(),
            JsonValue::from("hello")
        );
        // Blob x'01ff' hashes as lowercase hex "01ff".
        assert_eq!(
            get_col0(&one_row_conn("x'01ff'")).unwrap(),
            JsonValue::from("01ff")
        );
    }

    // -- An absent column means "leave the destination's value alone" (#324) --

    /// A table with one column an operator would put in `exclude_columns`, and
    /// a stored row that already has a value there.
    fn stripping_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE notes (
                 id TEXT PRIMARY KEY,
                 body TEXT,
                 ssn TEXT,
                 updated_at TEXT
             );
             INSERT INTO notes VALUES ('n1', 'old body', 'secret', '2026-01-01');",
        )
        .unwrap();
        conn
    }

    /// The row `sync::transfer_rows` produces after stripping `ssn`: newer,
    /// changed, and missing exactly the excluded column.
    fn stripped_row() -> HashMap<String, JsonValue> {
        HashMap::from([
            ("id".to_string(), JsonValue::from("n1")),
            ("body".to_string(), JsonValue::from("new body")),
            ("updated_at".to_string(), JsonValue::from("2026-02-01")),
        ])
    }

    fn read_note(conn: &Connection, id: &str) -> (Option<String>, Option<String>) {
        conn.query_row("SELECT body, ssn FROM notes WHERE id = ?", [id], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap()
    }

    #[test]
    fn replace_leaves_a_column_the_row_omits_untouched() {
        // The arm `DataSource::upsert_rows` uses unconditionally, and therefore
        // the arm the directional pull path writes through. Before #324 this
        // bound `ssn` as an explicit NULL and destroyed the local value.
        let conn = stripping_conn();
        let out =
            upsert_rows_guarded_inner(&conn, "notes", &[stripped_row()], UpsertGuard::Replace)
                .unwrap();
        assert_eq!(out.applied, 1);

        let (body, ssn) = read_note(&conn, "n1");
        assert_eq!(body.as_deref(), Some("new body"), "carried column applies");
        assert_eq!(
            ssn.as_deref(),
            Some("secret"),
            "omitted column is preserved"
        );
    }

    #[test]
    fn newer_by_leaves_a_column_the_row_omits_untouched() {
        let conn = stripping_conn();
        let ordering = vec!["updated_at".to_string()];
        let out = upsert_rows_guarded_inner(
            &conn,
            "notes",
            &[stripped_row()],
            UpsertGuard::NewerBy(&ordering),
        )
        .unwrap();
        assert_eq!(out.applied, 1, "2026-02-01 is newer than 2026-01-01");
        assert!(!out.ordering_unavailable);

        let (body, ssn) = read_note(&conn, "n1");
        assert_eq!(body.as_deref(), Some("new body"));
        assert_eq!(
            ssn.as_deref(),
            Some("secret"),
            "omitted column is preserved"
        );
    }

    #[test]
    fn newer_by_without_an_ordering_column_leaves_a_column_the_row_omits_untouched() {
        // The `ordering_unavailable` fallback: `NewerBy` on a table carrying
        // none of its ordering columns degrades to a blind overwrite. It is the
        // easiest arm to miss and exactly where the protection would vanish
        // without anyone noticing.
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT, ssn TEXT);
             INSERT INTO notes VALUES ('n1', 'old body', 'secret');",
        )
        .unwrap();

        let ordering = vec!["updated_at".to_string()];
        let row = HashMap::from([
            ("id".to_string(), JsonValue::from("n1")),
            ("body".to_string(), JsonValue::from("new body")),
        ]);
        let out =
            upsert_rows_guarded_inner(&conn, "notes", &[row], UpsertGuard::NewerBy(&ordering))
                .unwrap();
        assert!(
            out.ordering_unavailable,
            "the table has none of the ordering columns, so this IS the fallback"
        );
        assert_eq!(out.applied, 1);

        let (body, ssn) = read_note(&conn, "n1");
        assert_eq!(body.as_deref(), Some("new body"));
        assert_eq!(
            ssn.as_deref(),
            Some("secret"),
            "omitted column is preserved"
        );
    }

    #[test]
    fn keep_local_leaves_a_column_the_row_omits_untouched() {
        let conn = stripping_conn();
        let out =
            upsert_rows_guarded_inner(&conn, "notes", &[stripped_row()], UpsertGuard::KeepLocal)
                .unwrap();
        assert_eq!(out.applied, 0);
        assert_eq!(out.rejected, 1, "the guard declines the write entirely");

        let (body, ssn) = read_note(&conn, "n1");
        assert_eq!(body.as_deref(), Some("old body"), "the local row stands");
        assert_eq!(ssn.as_deref(), Some("secret"));
    }

    #[test]
    fn newer_by_with_the_ordering_column_stripped_never_displaces_a_stored_row() {
        // A row that carries no ordering value has no claim on the stored row.
        // This is the pre-#324 behavior (every ordering column bound NULL made
        // `{incoming} IS NOT NULL` false) and it must survive the subset path,
        // where reading `excluded."updated_at"` would instead have produced the
        // column's schema DEFAULT -- see the next test.
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE notes (
                 id TEXT PRIMARY KEY,
                 body TEXT,
                 ssn TEXT,
                 updated_at TEXT DEFAULT '9999-01-01'
             );
             INSERT INTO notes VALUES ('n1', 'old body', 'secret', '2026-01-01');",
        )
        .unwrap();

        let ordering = vec!["updated_at".to_string()];
        let row = HashMap::from([
            ("id".to_string(), JsonValue::from("n1")),
            ("body".to_string(), JsonValue::from("new body")),
        ]);
        let out =
            upsert_rows_guarded_inner(&conn, "notes", &[row], UpsertGuard::NewerBy(&ordering))
                .unwrap();
        assert_eq!(out.applied, 0);
        assert_eq!(out.rejected, 1);

        let (body, ssn) = read_note(&conn, "n1");
        assert_eq!(
            body.as_deref(),
            Some("old body"),
            "a DEFAULT '9999-01-01' must not become an ordering claim"
        );
        assert_eq!(ssn.as_deref(), Some("secret"));
    }

    #[test]
    fn newer_by_ignores_a_stripped_ordering_columns_default_when_another_survives() {
        // The case the single-column test above cannot reach: two ordering
        // columns, one stripped, one carried. Reading the stripped one off
        // `excluded` yields its schema DEFAULT ('9999-01-01' here, and
        // `DEFAULT CURRENT_TIMESTAMP` in the wild), which would beat any stored
        // value and let a row with no real ordering claim overwrite a newer one.
        // The incoming ordering value must be computed over the columns the row
        // actually carries.
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE notes (
                 id TEXT PRIMARY KEY,
                 body TEXT,
                 ssn TEXT,
                 updated_at TEXT DEFAULT '9999-01-01',
                 deleted_at TEXT
             );
             INSERT INTO notes VALUES ('n1', 'old body', 'secret', '2026-06-01', NULL);",
        )
        .unwrap();

        let ordering = vec!["updated_at".to_string(), "deleted_at".to_string()];
        let row = HashMap::from([
            ("id".to_string(), JsonValue::from("n1")),
            ("body".to_string(), JsonValue::from("new body")),
            ("deleted_at".to_string(), JsonValue::from("2026-01-01")),
        ]);
        let out =
            upsert_rows_guarded_inner(&conn, "notes", &[row], UpsertGuard::NewerBy(&ordering))
                .unwrap();
        assert_eq!(out.applied, 0);
        assert_eq!(
            out.rejected, 1,
            "incoming 2026-01-01 loses to stored 2026-06-01"
        );

        let (body, ssn) = read_note(&conn, "n1");
        assert_eq!(body.as_deref(), Some("old body"));
        assert_eq!(ssn.as_deref(), Some("secret"));
    }

    #[test]
    fn excluded_reads_the_schema_default_for_an_omitted_column() {
        // The measured SQLite fact the `Newer` arm is built around: a column
        // left out of the INSERT column list is still readable through
        // `excluded`, and it reads as that column's schema DEFAULT rather than
        // NULL. That is why the incoming ordering expression is computed over
        // the columns the row actually carries instead of all of them.
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE t (id TEXT PRIMARY KEY, v TEXT, ts TEXT DEFAULT '9999');
             INSERT INTO t VALUES ('a', 'old', '1000');",
        )
        .unwrap();

        let changed = conn
            .execute(
                "INSERT INTO t (\"id\", \"v\") VALUES (?, ?) \
                 ON CONFLICT(\"id\") DO UPDATE SET \"v\" = excluded.\"v\" \
                 WHERE excluded.\"ts\" IS NOT NULL",
                rusqlite::params!["a", "new"],
            )
            .expect("excluded.<omitted column> is legal, not a prepare error");
        assert_eq!(
            changed, 1,
            "excluded.\"ts\" is the DEFAULT '9999', not NULL -- so the guard passed"
        );
    }

    #[test]
    fn a_new_row_missing_a_column_takes_its_schema_default() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE notes (
                 id TEXT PRIMARY KEY,
                 body TEXT,
                 ssn TEXT DEFAULT 'unset'
             );",
        )
        .unwrap();

        let row = HashMap::from([
            ("id".to_string(), JsonValue::from("fresh")),
            ("body".to_string(), JsonValue::from("hello")),
        ]);
        let out = upsert_rows_guarded_inner(&conn, "notes", &[row], UpsertGuard::Replace).unwrap();
        assert_eq!(out.applied, 1);

        let (body, ssn) = read_note(&conn, "fresh");
        assert_eq!(body.as_deref(), Some("hello"));
        assert_eq!(
            ssn.as_deref(),
            Some("unset"),
            "no prior value existed, so the schema default destroys nothing"
        );
    }

    #[test]
    fn a_new_row_missing_a_not_null_column_errors_naming_the_table_and_column() {
        // Not papered over: a NOT NULL column with no DEFAULT, excluded from a
        // row that does not yet exist at the destination, still fails the
        // insert. What matters is that the failure is legible.
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT, ssn TEXT NOT NULL);",
        )
        .unwrap();

        let row = HashMap::from([
            ("id".to_string(), JsonValue::from("fresh")),
            ("body".to_string(), JsonValue::from("hello")),
        ]);
        let err = upsert_rows_guarded_inner(&conn, "notes", &[row], UpsertGuard::Replace)
            .expect_err("a NOT NULL column with no default cannot be omitted from a new row");
        let msg = err.to_string();
        assert!(
            msg.contains("notes") && msg.contains("ssn"),
            "the error must name the table and the column, got: {msg}"
        );
    }

    #[test]
    fn rows_carrying_different_column_sets_group_by_column_set() {
        // A stripping sender is uniform in practice, so this is one group in
        // practice -- but nothing in the trait promises it, and writing a
        // mixed batch under one row's column list would null what the other
        // rows preserve.
        let conn = stripping_conn();
        conn.execute(
            "INSERT INTO notes VALUES ('n2', 'old two', 'secret two', '2026-01-01')",
            [],
        )
        .unwrap();

        let mut with_ssn = stripped_row();
        with_ssn.insert("id".to_string(), JsonValue::from("n2"));
        with_ssn.insert("ssn".to_string(), JsonValue::from("rewritten"));

        let out = upsert_rows_guarded_inner(
            &conn,
            "notes",
            &[stripped_row(), with_ssn],
            UpsertGuard::Replace,
        )
        .unwrap();
        assert_eq!(out.applied, 2);

        assert_eq!(
            read_note(&conn, "n1").1.as_deref(),
            Some("secret"),
            "the row that omitted ssn keeps its stored value"
        );
        assert_eq!(
            read_note(&conn, "n2").1.as_deref(),
            Some("rewritten"),
            "the row that carried ssn writes it"
        );
    }

    // -- SQL shape --

    fn shape_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE t (id TEXT PRIMARY KEY, ts TEXT, v TEXT);")
            .unwrap();
        conn
    }

    #[test]
    fn a_complete_row_emits_the_pre_324_sql_byte_for_byte() {
        // The expected strings are transcribed from the pre-#324
        // `upsert_rows_guarded_inner` (46ea374, local.rs:509-544), NOT generated
        // from the builder under test -- generating them would make this
        // circular. A deployment without `exclude_columns` sends every column
        // and must keep the historical statement, so it takes on none of the
        // `ON CONFLICT(pk)` trade-offs.
        let conn = shape_conn();
        let info = table_info_inner(&conn, "t").unwrap();
        let all = ["id", "ts", "v"];
        let ordering = vec!["ts".to_string()];

        assert_eq!(
            build_upsert_sql("t", &info, &all, ConflictShape::Overwrite, &[]),
            "INSERT OR REPLACE INTO \"t\" (\"id\", \"ts\", \"v\") VALUES (?, ?, ?)"
        );
        assert_eq!(
            build_upsert_sql("t", &info, &all, ConflictShape::Keep, &[]),
            "INSERT INTO \"t\" (\"id\", \"ts\", \"v\") VALUES (?, ?, ?) \
             ON CONFLICT(\"id\") DO NOTHING"
        );
        assert_eq!(
            build_upsert_sql("t", &info, &all, ConflictShape::Newer, &ordering),
            "INSERT INTO \"t\" (\"id\", \"ts\", \"v\") VALUES (?, ?, ?) \
             ON CONFLICT(\"id\") DO UPDATE SET \"ts\" = excluded.\"ts\", \"v\" = excluded.\"v\" \
             WHERE excluded.\"ts\" IS NOT NULL \
             AND (\"t\".\"ts\" IS NULL OR excluded.\"ts\" > \"t\".\"ts\")"
        );
        // `NewerBy` with no usable ordering column lowers to Overwrite, which on
        // the complete path is the same INSERT OR REPLACE as before.
        assert_eq!(
            build_upsert_sql("t", &info, &all, ConflictShape::Overwrite, &[]),
            "INSERT OR REPLACE INTO \"t\" (\"id\", \"ts\", \"v\") VALUES (?, ?, ?)"
        );
    }

    #[test]
    fn a_row_omitting_a_column_emits_on_conflict_do_update() {
        let conn = shape_conn();
        let info = table_info_inner(&conn, "t").unwrap();
        let ordering = vec!["ts".to_string()];

        // `v` omitted: INSERT OR REPLACE would DELETE+INSERT and reset it.
        let no_v = ["id", "ts"];
        assert_eq!(
            build_upsert_sql("t", &info, &no_v, ConflictShape::Overwrite, &[]),
            "INSERT INTO \"t\" (\"id\", \"ts\") VALUES (?, ?) \
             ON CONFLICT(\"id\") DO UPDATE SET \"ts\" = excluded.\"ts\""
        );
        assert_eq!(
            build_upsert_sql("t", &info, &no_v, ConflictShape::Keep, &[]),
            "INSERT INTO \"t\" (\"id\", \"ts\") VALUES (?, ?) ON CONFLICT(\"id\") DO NOTHING"
        );
        assert_eq!(
            build_upsert_sql("t", &info, &no_v, ConflictShape::Newer, &ordering),
            "INSERT INTO \"t\" (\"id\", \"ts\") VALUES (?, ?) \
             ON CONFLICT(\"id\") DO UPDATE SET \"ts\" = excluded.\"ts\" \
             WHERE excluded.\"ts\" IS NOT NULL \
             AND (\"t\".\"ts\" IS NULL OR excluded.\"ts\" > \"t\".\"ts\")"
        );

        // The ordering column itself stripped: no incoming ordering signal, so
        // the row may insert but must never displace.
        let no_ts = ["id", "v"];
        assert_eq!(
            build_upsert_sql("t", &info, &no_ts, ConflictShape::Newer, &ordering),
            "INSERT INTO \"t\" (\"id\", \"v\") VALUES (?, ?) ON CONFLICT(\"id\") DO NOTHING"
        );

        // Nothing but the primary key: nothing to assign, nothing to write.
        let pk_only = ["id"];
        assert_eq!(
            build_upsert_sql("t", &info, &pk_only, ConflictShape::Overwrite, &[]),
            "INSERT INTO \"t\" (\"id\") VALUES (?) ON CONFLICT(\"id\") DO NOTHING"
        );
    }

    #[test]
    fn a_primary_key_column_is_applied_even_when_the_row_omits_it() {
        // The conflict target has to be bound. Omitting it would let SQLite
        // supply a default -- a fresh rowid for an INTEGER PRIMARY KEY -- and
        // insert a new row instead of resolving against the intended one.
        let cols = ["id", "body", "ssn"];
        let pk = vec!["id".to_string()];
        let row = HashMap::from([("body".to_string(), JsonValue::from("x"))]);
        assert_eq!(applied_columns(&cols, &pk, &row), vec!["id", "body"]);
    }
}
