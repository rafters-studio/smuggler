//! Reverse / rollback (#274).
//!
//! Rollback is a **compensating forward step**, never a transactional undo
//! (design decisions 3/4, "Recovery"). Reversing `vN` assembles the inverse of
//! its `up` ops and applies them as a *new* ledgered `vN+1` step; the reversed
//! `vN` ledger row stays byte-unchanged, because the ledger is append-only and
//! chain-hashed and an edit would trip [`crate::migrate::ledger::Ledger::verify_chain`]
//! ([`crate::error::SyncError::LedgerTampered`]).
//!
//! Two reverse classes (decision 4):
//!
//! - **Additive** (`CreateTable` / `AddColumn` / `CreateIndex` / rename): the
//!   reverse is the *structural inverse* -- drop what was added, swap a rename --
//!   carrying no data. [`structural_inverse`] / [`additive_down_ops`].
//! - **Destructive** (`DropTable` / `DropColumn`): reversible only from a
//!   **delta-scoped pre-image** (rows-INTERSECT-columns, OQ1) captured *during*
//!   the forward apply, before the op mutates. The capture rides the `pre_op`
//!   write-ahead hook [`apply_ops`](crate::migrate::apply::apply_ops) already
//!   exposes ([`PreimageCapturer::capture_before`]). Both restores reconstruct the
//!   structure from the **verbatim pre-mutation DDL** captured from
//!   `sqlite_master` -- a re-created table runs the captured `CREATE TABLE`
//!   directly, a re-added column rebuilds through
//!   [`crate::migrate::apply::rebuild_to_schema`] with a
//!   [`RebuildTarget::Verbatim`](crate::migrate::apply::RebuildTarget) target --
//!   so every surviving `CHECK` / `UNIQUE` / `COLLATE` / generated column and
//!   table-level constraint survives the round-trip. The lost cells are refilled
//!   with an idempotent PK-keyed UPSERT ([`restore_payload`], spike C).
//!
//! # Two compilation surfaces
//!
//! Like `apply`, this module is declared `pub mod reverse;` with no `native`
//! gate, so the pure inverse ([`structural_inverse`]) and the serializable
//! pre-image types compile on `wasm32`. Everything touching `rusqlite`,
//! `object_store`, or the ledger is internally `#[cfg(feature = "native")]`.
//!
//! # Pre-image storage (resolved)
//!
//! The captured pre-image serializes to JSON and is carried as a
//! [`Preimage`](crate::migrate::Preimage): **small payloads inline**
//! ([`Preimage::Inline`], no relay) and **large payloads in a content-addressed
//! store** ([`Preimage::Ref`], keyed by the SHA-256 of the payload, written
//! through [`crate::stash::build_store`]). [`INLINE_MAX_BYTES`] pins the boundary
//! where inline refuses and a relay becomes mandatory ([`store_preimage`]).
//!
//! The ledger's forward-compat `preimage_ref` column is **read-only from here**
//! (OQ2) via [`preimage_ref_of`] -- and in 0.5.0 **nothing writes it**. No ledger
//! setter for the column exists, and the forward driver (#296) hands the captured
//! payload back in its apply outcome rather than stashing a key on the row, so the
//! column stays NULL and [`preimage_ref_of`] returns `None` on every real ledger
//! row. It is a reader waiting on a writer, kept so the column's meaning is fixed
//! before something starts populating it (#326 corrected an earlier claim here
//! that named #296 as the writer).
//!
//! # Capture is wired into the apply loop, not checked before it
//!
//! [`apply_with_capture`] is the sanctioned forward path for a manifest with any
//! destructive op: it binds [`PreimageCapturer::capture_before`] as `apply_ops`'
//! `pre_op` hook, so capture-then-mutate is structural per op rather than an
//! ordering a caller has to remember. The manifest-level gate
//! ([`crate::migrate::lint::enforce_preimage`]) necessarily runs *before* that
//! loop and so cannot see a capture that does not exist yet; the "no mutation
//! without a snapshot" guarantee lives in [`PreimageCapturer::capture_before`],
//! which refuses a destructive op it cannot capture *before* the op runs.
//!
//! That guarantee is scoped to the **forward `up` path**, for the same reason the
//! lint is (`lint.rs`, "Scope"): a `down` op is a legitimately-destructive-by-
//! structure inverse -- the `DropColumn` that undoes an `AddColumn` -- whose data
//! was never there to lose. So [`apply_compensating`] running `apply_ops` with a
//! no-op hook over [`additive_down_ops`] is correct, not a hole: there is nothing
//! to snapshot on the way back out.

use crate::migrate::{ClassifiedOp, MigrateError, Op};
use serde::{Deserialize, Serialize};

#[cfg(feature = "native")]
use crate::config::StashConfig;
#[cfg(feature = "native")]
use crate::migrate::apply::{apply_ops, rebuild_to_schema, RebuildSpec, RebuildTarget};
#[cfg(feature = "native")]
use crate::migrate::ledger::{Election, LedgerEntry};
#[cfg(feature = "native")]
use crate::migrate::lint::classify_op;
#[cfg(feature = "native")]
use crate::migrate::Preimage;
#[cfg(feature = "native")]
use object_store::{path::Path as ObjectPath, ObjectStore, PutPayload};
#[cfg(feature = "native")]
use rusqlite::{params_from_iter, types::Value as SqlValue, Connection};
#[cfg(feature = "native")]
use sha2::{Digest, Sha256};

// ===========================================================================
// Structural inverse (always compiled -- no rusqlite)
// ===========================================================================

/// The structural inverse of an **additive** op (decision 4): drop what was
/// added, swap a rename. Carries no data.
///
/// Returns [`MigrateError::Apply`] for an op whose reverse needs a captured
/// pre-image (`DropTable`, `DropColumn`) or is not structurally recoverable in
/// 0.5.0 (`DropIndex` -- the index definition is not carried by the op). Those
/// reverse through the pre-image path, not this pure inverse.
pub fn structural_inverse(op: &Op) -> Result<Op, MigrateError> {
    match op {
        Op::CreateTable { table, .. } => Ok(Op::DropTable {
            table: table.clone(),
        }),
        Op::AddColumn { table, column } => Ok(Op::DropColumn {
            table: table.clone(),
            column: column.name.clone(),
        }),
        Op::CreateIndex { name, .. } => Ok(Op::DropIndex { name: name.clone() }),
        Op::RenameTable { from, to } => Ok(Op::RenameTable {
            from: to.clone(),
            to: from.clone(),
        }),
        Op::RenameColumn { table, from, to } => Ok(Op::RenameColumn {
            table: table.clone(),
            from: to.clone(),
            to: from.clone(),
        }),
        Op::DropTable { table } => Err(MigrateError::Apply(format!(
            "reverse of DROP TABLE {table:?} needs a captured pre-image, not a structural inverse"
        ))),
        Op::DropColumn { table, column } => Err(MigrateError::Apply(format!(
            "reverse of DROP COLUMN {column:?} on {table:?} needs a captured pre-image, \
             not a structural inverse"
        ))),
        Op::DropIndex { name } => Err(MigrateError::Apply(format!(
            "reverse of DROP INDEX {name:?} is not structurally recoverable in 0.5.0 \
             (the op does not carry the index definition)"
        ))),
    }
}

/// The additive `down` ops for a manifest's `up`: the [`structural_inverse`] of
/// each op, in **reverse order** (the last thing applied is the first thing
/// undone). Errors if any op is destructive -- those need the pre-image path.
pub fn additive_down_ops(up: &[ClassifiedOp]) -> Result<Vec<ClassifiedOp>, MigrateError> {
    up.iter()
        .rev()
        .map(|c| structural_inverse(&c.op).map(ClassifiedOp::new))
        .collect()
}

// ===========================================================================
// Pre-image payload (serializable; always compiled)
// ===========================================================================

/// The inline-vs-relay boundary (64 KiB of serialized payload).
///
/// At or under this, a pre-image rides inline in the manifest
/// ([`Preimage::Inline`]); over it, inline **refuses** and a content-addressed
/// relay store becomes mandatory ([`Preimage::Ref`]) -- otherwise a large
/// `DROP COLUMN` pre-image would silently fail to travel. Pinned in
/// [`store_preimage`].
pub const INLINE_MAX_BYTES: usize = 64 * 1024;

/// A single captured SQLite cell value, losslessly serializable.
///
/// Mirrors [`rusqlite::types::Value`]'s five storage classes; `Blob` round-trips
/// as a byte array so binary keys/columns survive JSON.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "t", content = "v", rename_all = "snake_case")]
pub enum CapturedValue {
    /// SQL `NULL`.
    Null,
    /// A 64-bit integer.
    Int(i64),
    /// A floating-point value.
    Real(f64),
    /// UTF-8 text.
    Text(String),
    /// Opaque bytes.
    Blob(Vec<u8>),
}

/// One table's delta-scoped pre-image: exactly the rows-INTERSECT-columns a
/// destructive op is about to lose, plus the structure needed to put them back.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "loss", rename_all = "snake_case")]
pub enum TablePreimage {
    /// A dropped column. Restore rebuilds the table from the verbatim pre-drop
    /// `CREATE TABLE` (so every surviving constraint is intact), copying the
    /// surviving columns and leaving the re-added column to its default/NULL, then
    /// refills the one lost column by PK-keyed UPSERT.
    Column {
        /// The table the column was dropped from.
        table: String,
        /// The dropped column's name.
        dropped: String,
        /// The verbatim pre-drop `CREATE TABLE` DDL (all constraints intact). The
        /// rebuild splices in the temp-table name, so the re-added column and
        /// every surviving `CHECK` / `UNIQUE` / `COLLATE` / generated column and
        /// table-level constraint are carried byte-for-byte.
        create_sql: String,
        /// Verbatim pre-drop index / trigger DDL to replay after the rebuild
        /// (including any index that referenced the dropped column).
        aux_ddl: Vec<String>,
        /// Whether the dropped column was `NOT NULL` with no default. Such a
        /// column cannot survive the rebuild-then-fill gap (it is null between the
        /// copy and the UPSERT), so restore rejects it with a clear error.
        dropped_requires_value: bool,
        /// The primary-key column names, in key order.
        pk: Vec<String>,
        /// The columns present in each captured row (`pk` ++ `dropped`).
        captured_columns: Vec<String>,
        /// Captured rows, each aligned to `captured_columns`.
        rows: Vec<Vec<CapturedValue>>,
    },
    /// A dropped table. Restore re-creates it from the captured DDL (verbatim, so
    /// indexes / triggers / `CHECK` / `WITHOUT ROWID` survive), then re-inserts
    /// every row by PK-keyed UPSERT.
    Table {
        /// The dropped table's name.
        table: String,
        /// The verbatim `CREATE TABLE` DDL captured from `sqlite_master`.
        create_sql: String,
        /// Verbatim index / trigger DDL to replay after the table is recreated.
        aux_ddl: Vec<String>,
        /// The primary-key column names, in key order.
        pk: Vec<String>,
        /// Every column name, in table order (the row layout).
        columns: Vec<String>,
        /// Captured rows, each aligned to `columns`.
        rows: Vec<Vec<CapturedValue>>,
    },
}

/// The whole captured pre-image for one destructive migration: one
/// [`TablePreimage`] per destructive op, in apply order.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct PreimagePayload {
    /// Per-op captures, in the order the destructive ops were applied.
    pub tables: Vec<TablePreimage>,
}

impl PreimagePayload {
    /// Whether nothing was captured (no destructive op ran).
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }
}

// ===========================================================================
// Native: capture, restore, storage, rollback
// ===========================================================================

/// Quote a SQL identifier, escaping embedded double-quotes by doubling.
#[cfg(feature = "native")]
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// One `PRAGMA table_info` row, tolerant of absent tables.
#[cfg(feature = "native")]
struct ColRow {
    name: String,
    notnull: bool,
    dflt: Option<String>,
    /// 1-based PK position, `0` if not part of the primary key.
    pk: i64,
}

/// Read `PRAGMA table_info` for a table (empty if absent).
#[cfg(feature = "native")]
fn table_info(conn: &Connection, table: &str) -> Result<Vec<ColRow>, MigrateError> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({})", quote_ident(table)))?;
    let rows = stmt
        .query_map([], |r| {
            Ok(ColRow {
                name: r.get::<_, String>(1)?,
                notnull: r.get::<_, i64>(3)? != 0,
                dflt: r.get::<_, Option<String>>(4)?,
                pk: r.get::<_, i64>(5)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// The primary-key column names of a table, in key order.
#[cfg(feature = "native")]
fn primary_key(info: &[ColRow]) -> Vec<String> {
    let mut pk: Vec<&ColRow> = info.iter().filter(|c| c.pk > 0).collect();
    pk.sort_by_key(|c| c.pk);
    pk.into_iter().map(|c| c.name.clone()).collect()
}

/// The generated (`GENERATED ALWAYS AS`) column names of a table. A rebuild
/// must never project into these -- inserting a value into a generated column is
/// an error; they self-populate from the copied base columns.
///
/// The `PRAGMA table_xinfo` walk itself lives in
/// [`apply`](crate::migrate::apply), which needs the same answer with the
/// storage class attached in order to name the loss in a warning (#342). This
/// module only needs the names, and asking the shared one keeps a single
/// statement of which `hidden` values mean generated.
#[cfg(feature = "native")]
fn generated_columns(conn: &Connection, table: &str) -> Result<Vec<String>, MigrateError> {
    Ok(crate::migrate::apply::generated_columns(conn, table)?
        .into_iter()
        .map(|(name, _storage)| name)
        .collect())
}

/// The verbatim `CREATE` DDL of a `sqlite_master` object, if present.
#[cfg(feature = "native")]
fn object_sql(conn: &Connection, kind: &str, name: &str) -> Result<Option<String>, MigrateError> {
    use rusqlite::OptionalExtension;
    let sql = conn
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = ?1 AND name = ?2",
            rusqlite::params![kind, name],
            |r| r.get::<_, Option<String>>(0),
        )
        .optional()?;
    Ok(sql.flatten())
}

/// Verbatim `CREATE INDEX` / `CREATE TRIGGER` DDL attached to a table.
#[cfg(feature = "native")]
fn aux_object_ddl(conn: &Connection, table: &str) -> Result<Vec<String>, MigrateError> {
    let mut stmt = conn.prepare(
        "SELECT sql FROM sqlite_master \
         WHERE tbl_name = ?1 AND type IN ('index', 'trigger') AND sql IS NOT NULL \
         ORDER BY type, name",
    )?;
    let rows = stmt
        .query_map(rusqlite::params![table], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// Read the captured rows for `columns` from `table`, in table order.
#[cfg(feature = "native")]
fn capture_rows(
    conn: &Connection,
    table: &str,
    columns: &[String],
) -> Result<Vec<Vec<CapturedValue>>, MigrateError> {
    let list = columns
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");
    let mut stmt = conn.prepare(&format!("SELECT {} FROM {}", list, quote_ident(table)))?;
    let n = columns.len();
    let rows = stmt
        .query_map([], |r| {
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                out.push(captured_from_sql(r.get::<_, SqlValue>(i)?));
            }
            Ok(out)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// Lower a rusqlite value into the serializable [`CapturedValue`].
#[cfg(feature = "native")]
fn captured_from_sql(v: SqlValue) -> CapturedValue {
    match v {
        SqlValue::Null => CapturedValue::Null,
        SqlValue::Integer(i) => CapturedValue::Int(i),
        SqlValue::Real(r) => CapturedValue::Real(r),
        SqlValue::Text(s) => CapturedValue::Text(s),
        SqlValue::Blob(b) => CapturedValue::Blob(b),
    }
}

/// Raise a serializable [`CapturedValue`] back into a bindable rusqlite value.
#[cfg(feature = "native")]
fn captured_to_sql(v: &CapturedValue) -> SqlValue {
    match v {
        CapturedValue::Null => SqlValue::Null,
        CapturedValue::Int(i) => SqlValue::Integer(*i),
        CapturedValue::Real(r) => SqlValue::Real(*r),
        CapturedValue::Text(s) => SqlValue::Text(s.clone()),
        CapturedValue::Blob(b) => SqlValue::Blob(b.clone()),
    }
}

/// Accumulates the delta-scoped pre-image while a destructive forward apply runs.
///
/// Drive it as the `pre_op` write-ahead hook of
/// [`apply_ops`](crate::migrate::apply::apply_ops): the hook fires *before* each
/// op's own transaction, so [`Self::capture_before`] snapshots committed,
/// pre-mutation state. Additive ops are ignored (their reverse carries no data).
/// [`apply_with_capture`] does that wiring; prefer it over hand-binding the hook.
///
/// ```ignore
/// let payload = apply_with_capture(conn, &manifest.up)?;
/// ```
#[cfg(feature = "native")]
#[derive(Debug, Default)]
pub struct PreimageCapturer {
    payload: PreimagePayload,
}

#[cfg(feature = "native")]
impl PreimageCapturer {
    /// A fresh, empty capturer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot the pre-image of `op` if it is destructive; no-op otherwise.
    ///
    /// Must run **before** `op` mutates (it reads the current table). Additive
    /// ops carry no data, so they are skipped.
    ///
    /// This is where "a destructive op never mutates without a pre-image" is
    /// actually enforced, and it is enforced write-ahead: driven as `apply_ops`'
    /// `pre_op` hook, an `Err` here aborts before the op's own transaction opens,
    /// so a destructive op whose pre-image cannot be captured never runs. The
    /// manifest-level gate [`crate::migrate::lint::enforce_preimage`] cannot make
    /// that guarantee -- it runs before this loop, when no capture exists yet.
    ///
    /// The final refusal is the reason the op match is not a bare catch-all: an
    /// op that [`classify_op`] calls destructive but no arm above knows how to
    /// snapshot is a data-loss hole, so it is refused rather than silently skipped
    /// into the additive lane. Unreachable in 0.5.0 (`DropTable` and `DropColumn`
    /// are the only destructive ops, and both are handled); it exists so adding a
    /// destructive op without teaching capture about it fails loudly.
    pub fn capture_before(
        &mut self,
        conn: &Connection,
        op: &ClassifiedOp,
    ) -> Result<(), MigrateError> {
        let captured_before = self.payload.tables.len();
        match &op.op {
            Op::DropColumn { table, column } => {
                let capture = self.capture_drop_column(conn, table, column)?;
                self.payload.tables.push(capture);
            }
            Op::DropTable { table } => {
                let capture = self.capture_drop_table(conn, table)?;
                self.payload.tables.push(capture);
            }
            _ => {}
        }
        if classify_op(&op.op).destructive && self.payload.tables.len() == captured_before {
            return Err(MigrateError::Apply(format!(
                "refusing to apply destructive op {:?}: pre-image capture does not know how to \
                 snapshot it, so the loss would be irreversible",
                op.op
            )));
        }
        Ok(())
    }

    /// Consume the capturer, yielding the accumulated pre-image.
    ///
    /// An empty payload is a legitimate outcome (no destructive op ran); so is a
    /// capture with zero rows (a drop against an empty table loses nothing). What
    /// is *not* legitimate -- a destructive op that ran with no capture -- cannot
    /// reach here, because [`Self::capture_before`] refuses it before the mutation.
    pub fn into_payload(self) -> PreimagePayload {
        self.payload
    }

    #[cfg(feature = "native")]
    fn capture_drop_column(
        &self,
        conn: &Connection,
        table: &str,
        column: &str,
    ) -> Result<TablePreimage, MigrateError> {
        let info = table_info(conn, table)?;
        if info.is_empty() {
            return Err(MigrateError::Apply(format!(
                "cannot capture pre-image: table {table:?} does not exist"
            )));
        }
        let pk = primary_key(&info);
        if pk.is_empty() {
            return Err(MigrateError::Apply(format!(
                "cannot capture pre-image for {table:?}: no primary key (needed to key the \
                 surgical restore)"
            )));
        }
        if pk.iter().any(|k| k == column) {
            return Err(MigrateError::Apply(format!(
                "cannot reverse dropping primary-key column {column:?} on {table:?} via a \
                 surgical pre-image"
            )));
        }

        // Capture the VERBATIM pre-drop DDL: the column still exists here (capture
        // runs before the drop), so every surviving constraint -- CHECK, UNIQUE,
        // COLLATE, generated columns, table-level constraints, WITHOUT ROWID -- is
        // carried faithfully, exactly as the DropTable path does. Rebuilding from
        // inferred columns would strip them silently.
        let create_sql = object_sql(conn, "table", table)?.ok_or_else(|| {
            MigrateError::Apply(format!(
                "cannot capture pre-image: no DDL for table {table:?}"
            ))
        })?;
        let aux_ddl = aux_object_ddl(conn, table)?;

        // A NOT NULL column with no default cannot survive the rebuild-then-fill
        // gap (it is null between the copy and the UPSERT); record that now so the
        // restore fails with a clear message rather than a raw SQL error.
        let dropped_requires_value = info
            .iter()
            .find(|c| c.name == *column)
            .map(|c| c.notnull && c.dflt.is_none())
            .unwrap_or(false);

        let mut captured_columns = pk.clone();
        captured_columns.push(column.to_string());
        let rows = capture_rows(conn, table, &captured_columns)?;

        Ok(TablePreimage::Column {
            table: table.to_string(),
            dropped: column.to_string(),
            create_sql,
            aux_ddl,
            dropped_requires_value,
            pk,
            captured_columns,
            rows,
        })
    }

    #[cfg(feature = "native")]
    fn capture_drop_table(
        &self,
        conn: &Connection,
        table: &str,
    ) -> Result<TablePreimage, MigrateError> {
        let info = table_info(conn, table)?;
        if info.is_empty() {
            return Err(MigrateError::Apply(format!(
                "cannot capture pre-image: table {table:?} does not exist"
            )));
        }
        let create_sql = object_sql(conn, "table", table)?.ok_or_else(|| {
            MigrateError::Apply(format!(
                "cannot capture pre-image: no DDL for table {table:?}"
            ))
        })?;
        let pk = primary_key(&info);
        let columns: Vec<String> = info.iter().map(|c| c.name.clone()).collect();
        let rows = capture_rows(conn, table, &columns)?;
        let aux_ddl = aux_object_ddl(conn, table)?;

        Ok(TablePreimage::Table {
            table: table.to_string(),
            create_sql,
            aux_ddl,
            pk,
            columns,
            rows,
        })
    }
}

/// Run a forward apply of `up` with pre-image capture wired in, returning what was
/// captured.
///
/// This is [`apply_ops`] with [`PreimageCapturer::capture_before`] bound as its
/// `pre_op` write-ahead hook -- not a second apply loop, the same one with the hook
/// supplied. It exists so the ordering that makes a destructive apply reversible is
/// **structural** instead of a convention each caller re-implements:
///
/// ```text
/// per op:  capture pre-image (or refuse)  ->  mutate
/// ```
///
/// Use it for any manifest whose `up` may contain a destructive op. A caller that
/// binds its own `pre_op` -- to also log intent (#289), say -- must call
/// `capture_before` from inside that hook; one that passes a no-op hook has opted
/// out of reversibility, which is why the destructive forward path should come
/// through here.
///
/// The returned payload is empty when nothing destructive ran. Errors propagate
/// from capture (refusing before the op mutates) or from the op itself; on a
/// mid-loop failure the already-applied ops stay applied, exactly as [`apply_ops`]
/// documents. Ledger election and settlement are *not* here -- composing this with
/// the ledger is the driver's job (#296).
#[cfg(feature = "native")]
pub fn apply_with_capture(
    conn: &Connection,
    up: &[ClassifiedOp],
) -> Result<PreimagePayload, MigrateError> {
    let mut capturer = PreimageCapturer::new();
    {
        let mut pre_op =
            |op: &ClassifiedOp| -> Result<(), MigrateError> { capturer.capture_before(conn, op) };
        apply_ops(conn, up, &mut pre_op)?;
    }
    Ok(capturer.into_payload())
}

/// Restore a captured pre-image: reconstruct the lost structure, then refill the
/// lost cells with an idempotent PK-keyed UPSERT (surgical, re-runnable -- spike
/// C). Concurrent rows not in the pre-image are never touched.
///
/// Captures are undone in **reverse capture order** (last dropped, first
/// restored). Two ops on one table must unwind LIFO: restoring an earlier
/// capture whose schema still references a column a later drop removed would try
/// to copy a column the current table no longer has. Reverse order restores the
/// later (narrower) capture first, so each rebuild's projection matches the live
/// table.
#[cfg(feature = "native")]
pub fn restore_payload(conn: &Connection, payload: &PreimagePayload) -> Result<(), MigrateError> {
    for table in payload.tables.iter().rev() {
        match table {
            TablePreimage::Column {
                table,
                dropped,
                create_sql,
                aux_ddl,
                dropped_requires_value,
                pk,
                captured_columns,
                rows,
            } => restore_column(
                conn,
                table,
                dropped,
                create_sql,
                aux_ddl,
                *dropped_requires_value,
                pk,
                captured_columns,
                rows,
            )?,
            TablePreimage::Table {
                table,
                create_sql,
                aux_ddl,
                pk,
                columns,
                rows,
            } => restore_table(conn, table, create_sql, aux_ddl, pk, columns, rows)?,
        }
    }
    Ok(())
}

/// Restore a dropped column: rebuild the table from the verbatim pre-drop
/// `CREATE TABLE` (re-adding the column and carrying every surviving constraint
/// byte-for-byte via a [`RebuildTarget::Verbatim`] rebuild), then UPSERT the
/// captured values back into it.
#[cfg(feature = "native")]
#[allow(clippy::too_many_arguments)]
fn restore_column(
    conn: &Connection,
    table: &str,
    dropped: &str,
    create_sql: &str,
    aux_ddl: &[String],
    dropped_requires_value: bool,
    pk: &[String],
    captured_columns: &[String],
    rows: &[Vec<CapturedValue>],
) -> Result<(), MigrateError> {
    // The rebuild copies the surviving columns and leaves the re-added column
    // empty until the UPSERT fills it. A NOT NULL column with no default cannot
    // survive that gap -- fail with a clear message, not a raw SQL error.
    if dropped_requires_value {
        return Err(MigrateError::Apply(format!(
            "cannot restore NOT NULL column {dropped:?} on {table:?} without a default \
             (rebuild-then-fill leaves it null until the row-level UPSERT)"
        )));
    }

    // Only rebuild if the column is actually gone (idempotent re-run: a second
    // restore finds the column present and skips straight to the UPSERT).
    let present: Vec<String> = table_info(conn, table)?
        .into_iter()
        .map(|c| c.name)
        .collect();
    if !present.iter().any(|c| c == dropped) {
        // Copy every surviving, non-generated column identity-wise; the re-added
        // column is left out of the projection so it takes its default/NULL, and
        // generated columns self-populate (inserting into them is an error).
        let generated = generated_columns(conn, table)?;
        let projection: Vec<(String, String)> = present
            .iter()
            .filter(|c| !generated.iter().any(|g| g == *c))
            .map(|c| (c.clone(), quote_ident(c)))
            .collect();
        let spec = RebuildSpec {
            table: table.to_string(),
            target: RebuildTarget::Verbatim {
                create_sql: create_sql.to_string(),
            },
            projection,
            post_ddl: aux_ddl.to_vec(),
        };
        rebuild_to_schema(conn, &spec)?;
    }

    upsert_rows(
        conn,
        table,
        captured_columns,
        pk,
        &[dropped.to_string()],
        rows,
    )
}

/// Restore a dropped table: recreate it from the verbatim captured DDL if it is
/// absent, then UPSERT every captured row back in.
#[cfg(feature = "native")]
fn restore_table(
    conn: &Connection,
    table: &str,
    create_sql: &str,
    aux_ddl: &[String],
    pk: &[String],
    columns: &[String],
    rows: &[Vec<CapturedValue>],
) -> Result<(), MigrateError> {
    let exists: i64 = conn.query_row(
        "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
        rusqlite::params![table],
        |r| r.get(0),
    )?;
    if exists == 0 {
        let tx = conn.unchecked_transaction()?;
        tx.execute_batch(create_sql)?;
        for ddl in aux_ddl {
            tx.execute_batch(ddl)?;
        }
        tx.commit()?;
    }
    let non_pk: Vec<String> = columns
        .iter()
        .filter(|c| !pk.contains(c))
        .cloned()
        .collect();
    upsert_rows(conn, table, columns, pk, &non_pk, rows)
}

/// Idempotent PK-keyed UPSERT: insert each row, and on a PK conflict update only
/// `update_columns` from the incoming row (`excluded`). Surgical -- an untouched
/// column of a matched row, and any row not in `rows`, is left exactly as it was.
#[cfg(feature = "native")]
fn upsert_rows(
    conn: &Connection,
    table: &str,
    insert_columns: &[String],
    pk: &[String],
    update_columns: &[String],
    rows: &[Vec<CapturedValue>],
) -> Result<(), MigrateError> {
    if rows.is_empty() {
        return Ok(());
    }
    let cols = insert_columns
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");
    let placeholders = (0..insert_columns.len())
        .map(|_| "?")
        .collect::<Vec<_>>()
        .join(", ");
    let conflict = pk
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");
    let action = if update_columns.is_empty() {
        "DO NOTHING".to_string()
    } else {
        let sets = update_columns
            .iter()
            .map(|c| format!("{q} = excluded.{q}", q = quote_ident(c)))
            .collect::<Vec<_>>()
            .join(", ");
        format!("DO UPDATE SET {sets}")
    };
    let sql = format!(
        "INSERT INTO {t} ({cols}) VALUES ({placeholders}) ON CONFLICT ({conflict}) {action}",
        t = quote_ident(table)
    );

    let tx = conn.unchecked_transaction()?;
    {
        let mut stmt = tx.prepare(&sql)?;
        for row in rows {
            if row.len() != insert_columns.len() {
                return Err(MigrateError::Apply(format!(
                    "pre-image row width {} does not match {} captured columns for {table:?}",
                    row.len(),
                    insert_columns.len()
                )));
            }
            stmt.execute(params_from_iter(row.iter().map(captured_to_sql)))?;
        }
    }
    tx.commit()?;
    Ok(())
}

// ===========================================================================
// Native: content-addressed pre-image storage (inline vs relay)
// ===========================================================================

/// Persist a captured pre-image as a [`Preimage`].
///
/// Small payloads (serialized `<= `[`INLINE_MAX_BYTES`]) ride inline; larger ones
/// **require** a relay -- the pre-image is written to the content-addressed store
/// (`config`) keyed by its SHA-256 and returned as [`Preimage::Ref`]. A payload
/// over the boundary with no `config` is a hard error (a large pre-image must not
/// silently fail to travel).
#[cfg(feature = "native")]
pub async fn store_preimage(
    payload: &PreimagePayload,
    config: Option<&StashConfig>,
) -> Result<Preimage, MigrateError> {
    let bytes =
        serde_json::to_vec(payload).map_err(|e| MigrateError::Serialization(e.to_string()))?;
    if bytes.len() <= INLINE_MAX_BYTES {
        // Fold the inline branch onto the already-serialized bytes: parse them
        // back into a `Value` rather than serializing the payload a second time.
        let rows = serde_json::from_slice(&bytes)
            .map_err(|e| MigrateError::Serialization(e.to_string()))?;
        return Ok(Preimage::Inline { rows });
    }
    let config = config.ok_or_else(|| {
        MigrateError::Apply(format!(
            "pre-image is {} bytes, over the {} byte inline limit; a relay store (StashConfig) \
             is required to carry it",
            bytes.len(),
            INLINE_MAX_BYTES
        ))
    })?;
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    let key = hex::encode(hasher.finalize());
    let (store, _) =
        crate::stash::build_store(config).map_err(|e| MigrateError::Apply(e.to_string()))?;
    let path = ObjectPath::from(format!("{key}.preimage"));
    store
        .put(&path, PutPayload::from(bytes))
        .await
        .map_err(|e| MigrateError::Apply(format!("pre-image store put failed: {e}")))?;
    Ok(Preimage::Ref { key })
}

/// Load a pre-image back from its [`Preimage`] reference. An inline payload
/// deserializes directly; a [`Preimage::Ref`] is fetched from the content-
/// addressed store (`config` required).
#[cfg(feature = "native")]
pub async fn load_preimage(
    preimage: &Preimage,
    config: Option<&StashConfig>,
) -> Result<PreimagePayload, MigrateError> {
    match preimage {
        Preimage::Inline { rows } => serde_json::from_value(rows.clone())
            .map_err(|e| MigrateError::Serialization(e.to_string())),
        Preimage::Ref { key } => {
            let config = config.ok_or_else(|| {
                MigrateError::Apply(
                    "pre-image is a store reference but no StashConfig was provided".to_string(),
                )
            })?;
            let (store, _) = crate::stash::build_store(config)
                .map_err(|e| MigrateError::Apply(e.to_string()))?;
            let path = ObjectPath::from(format!("{key}.preimage"));
            let got = store
                .get(&path)
                .await
                .map_err(|e| MigrateError::Apply(format!("pre-image store get failed: {e}")))?;
            let bytes = got
                .bytes()
                .await
                .map_err(|e| MigrateError::Apply(format!("pre-image store read failed: {e}")))?;
            serde_json::from_slice(&bytes).map_err(|e| MigrateError::Serialization(e.to_string()))
        }
    }
}

/// The pre-image reference recorded on a ledger row, if any (OQ2, read-only).
///
/// **Nothing writes `preimage_ref` in 0.5.0**, so this returns `None` for every
/// row a 0.5.0 apply produces. The ledger exposes no setter for the column -- it
/// appears in `ledger.rs`'s `CREATE TABLE` and in the `SELECT` projections, and in
/// no write path: the election insert does not list it, so every row is born NULL,
/// and none of [`Ledger::mark_success`](crate::migrate::ledger::Ledger::mark_success),
/// [`Ledger::mark_failed`](crate::migrate::ledger::Ledger::mark_failed), or either lease
/// reclaim touches it afterwards. The forward driver (#296) returns the
/// captured payload in its apply outcome instead of stashing a key on the row.
/// Until some component takes ownership of writing it, a `Ref` pre-image reaches a
/// reverse by travelling on the manifest, not by being looked up here.
///
/// Kept as a reader anyway, and named as unwritten rather than deleted, so the
/// column has one agreed interpretation -- the stored string is the
/// content-addressed key of a [`Preimage::Ref`], feedable straight to
/// [`load_preimage`] -- before anything starts populating it. An earlier version
/// of this doc asserted #296 was the writer; it is not, and a doc naming an owner
/// that does not do the thing is worse than silence (#326).
#[cfg(feature = "native")]
pub fn preimage_ref_of(entry: &LedgerEntry) -> Option<Preimage> {
    entry
        .preimage_ref
        .as_ref()
        .map(|key| Preimage::Ref { key: key.clone() })
}

// ===========================================================================
// Native: append-only compensating rollback step
// ===========================================================================

/// Apply a reverse as a **new, append-only compensating `version` step**.
///
/// Elects `version` (`vN+1`) and settles it through
/// [`elect_apply_settle`](crate::migrate::driver::elect_apply_settle) -- the
/// same claim-run-settle skeleton [`apply_migration`](crate::migrate::driver::apply_migration)
/// calls, so this crate has exactly the one forward-apply loop `driver.rs`'s
/// module doc requires, not a second hand-rolled copy of it (#463). The
/// reversed `vN` row is never popped, marked, or edited (that would trip the
/// chain-hash tamper check). Additive reverses ride `down_ops` through
/// [`apply_ops`](crate::migrate::apply::apply_ops); a destructive reverse's
/// structural + data restore rides `payload` through [`restore_payload`]. On a
/// mid-apply failure the row is marked failed so it is immediately reclaimable.
///
/// Scope: this composes a **pure-additive** reverse (`down_ops`, empty `payload`)
/// or a **pure-destructive** one (empty `down_ops`, `payload`). A reverse that
/// interleaves the two -- dropping a column whose data a later restore's captured
/// schema still references -- cannot be expressed as "all `down_ops`, then all
/// restores" and is the composing driver's concern (#296). The seam already
/// enforces this: [`additive_down_ops`] errors on any destructive op, so a caller
/// cannot assemble a mixed `down_ops` from a mixed `up` here.
///
/// # No manifest-level guards run here, deliberately (#463)
///
/// `apply_migration` runs five guards against a full authored manifest:
/// checksum verification and the `applied_version_of` already-applied
/// short-circuit and the #427 rowid-alias refusal, all *before* it ever
/// elects a version, plus `lint::lint_manifest` / `lint::enforce_preimage`,
/// which run *after* election succeeds (inside the closure
/// [`elect_apply_settle`](crate::migrate::driver::elect_apply_settle) calls,
/// unchanged from before this extraction). This function runs none of the
/// five, at either timing, and that is not an oversight this issue's
/// extraction papers over -- each was checked against what a reverse
/// actually is and rejected on its own terms:
///
/// - **Checksum verification** verifies a [`ChecksummedManifest`](crate::migrate::ChecksummedManifest)
///   travelling as a sealed unit; `down_ops` and `payload` are not a sealed
///   manifest, they are values this crate itself derived from one that
///   already verified when it applied forward. There is nothing here shaped
///   like the thing that check verifies.
/// - **The `applied_version_of` already-applied check** keys on a checksum
///   identifying the migration being applied. A reversal deliberately carries
///   a *different* checksum than the manifest it reverses (that distinction
///   is #419's whole subject); running this check here would not detect a
///   double-reverse, since the "already applied" row it would need to find is
///   the *original* `up`'s row, not one this reversal ever writes. Adding it
///   would be cargo-culting the guard's shape onto a check it cannot perform.
/// - **`lint::lint_manifest` / `lint::enforce_preimage`** judge freshly
///   user-authored `up` ops for destructive-without-preimage shape. A
///   `down_ops` assembled by [`additive_down_ops`] -- the only producer of
///   `down_ops` in this crate -- is never user-authored: it is the
///   structural inverse of `up` ops that already passed this exact lint when
///   they applied forward. A `payload` restore likewise carries its own
///   captured pre-image and needs no fresh one enforced. Re-linting an
///   inverse the crate generated from already-linted input judges nothing
///   new.
/// - **The #427 rowid-alias refusal** rejects a `CreateTable` minting a
///   rowid alias / `AUTOINCREMENT` primary key. [`structural_inverse`] never
///   produces a `CreateTable` -- a `CreateTable` in `up` inverts to
///   `DropTable`, never to another `CreateTable` -- so this shape cannot
///   occur in a `down_ops` built by [`additive_down_ops`].
///
/// Both of the last two claims are scoped to `down_ops` as
/// [`additive_down_ops`] assembles it, not to this function's parameter in
/// general: `down_ops: &[ClassifiedOp]` is caller-supplied, and nothing in
/// this function's signature stops a caller handing it a hand-rolled
/// `Op::CreateTable` with a rowid-alias key, or an op that never passed
/// lint. No guard here checks a hand-assembled `down_ops` -- the same is
/// true of `apply_ops` itself, which this function already calls with no
/// guard between the caller's `down_ops` and the database. A reversal
/// composed the sanctioned way (through [`additive_down_ops`] or a captured
/// `payload`) is applying the inverse of a manifest that already succeeded,
/// not a fresh authored one, and every guard above is scoped to the latter.
/// If a future op inversion changes that (a `down_ops` inverse that *can*
/// mint fresh DDL, say), the guard it needs is a fresh judgement call
/// against that op, not a mechanical import of this list.
///
/// # #419 is unchanged by this extraction
///
/// This function still does not, and this change does not make it, link a
/// reversal back to the version it reversed -- #419 (a reversed migration can
/// never be re-applied) is untouched in both directions. The already-applied
/// check `apply_migration` runs is one of the five guards deliberately not
/// carried onto this path (see above), so this extraction cannot resolve
/// #419 by omission, and nothing here adds a new way to trip it either.
///
/// Returns the [`Election`] outcome: `Won` means the step applied, anything else
/// means it was already applied or is held by another node (the caller backs off).
#[cfg(feature = "native")]
pub fn apply_compensating(
    conn: &Connection,
    version: u64,
    checksum: &str,
    down_ops: &[ClassifiedOp],
    payload: Option<&PreimagePayload>,
    lease_secs: i64,
) -> crate::error::Result<Election> {
    let (election, _) = crate::migrate::driver::elect_apply_settle(
        conn,
        version,
        checksum,
        lease_secs,
        |conn| -> crate::error::Result<()> {
            let mut noop = |_: &ClassifiedOp| -> Result<(), MigrateError> { Ok(()) };
            apply_ops(conn, down_ops, &mut noop)?;
            if let Some(p) = payload {
                restore_payload(conn, p)?;
            }
            Ok(())
        },
    )?;
    Ok(election)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrate::{Column, ColumnKind};

    fn col(name: &str, kind: ColumnKind) -> Column {
        Column {
            name: name.to_string(),
            kind,
            constraints: Vec::new(),
            tags: Vec::new(),
        }
    }

    // -- Structural inverse (pure, both feature sets) -----------------------

    /// A `DropIndex` in an `up` list fails, and the failure says why (#329).
    ///
    /// The doc on the variant used to say the index definition was recreatable.
    /// It is not -- the op carries only a name -- so this is the honest
    /// behaviour and the doc now matches it.
    ///
    /// At the `additive_down_ops` level rather than on `structural_inverse`
    /// alone, because the defect was that a hand-authored manifest passed every
    /// gate and only failed here.
    #[test]
    fn a_drop_index_in_up_fails_and_names_the_missing_definition() {
        let up = vec![
            ClassifiedOp::new(Op::CreateTable {
                table: "t".into(),
                columns: vec![col("id", ColumnKind::Text)],
                without_rowid: false,
            }),
            ClassifiedOp::new(Op::DropIndex { name: "idx".into() }),
        ];

        let err = additive_down_ops(&up).expect_err("a DropIndex in up has no inverse");
        let said = err.to_string();
        assert!(
            said.contains("does not carry the index definition"),
            "the refusal has to say why rather than merely refuse; it said {said:?}"
        );
    }

    #[test]
    fn create_table_inverts_to_drop_table() {
        let inv = structural_inverse(&Op::CreateTable {
            table: "t".into(),
            columns: vec![col("id", ColumnKind::Text)],
            without_rowid: false,
        })
        .unwrap();
        assert_eq!(inv, Op::DropTable { table: "t".into() });
    }

    #[test]
    fn add_column_inverts_to_drop_column() {
        let inv = structural_inverse(&Op::AddColumn {
            table: "t".into(),
            column: col("email", ColumnKind::Text),
        })
        .unwrap();
        assert_eq!(
            inv,
            Op::DropColumn {
                table: "t".into(),
                column: "email".into(),
            }
        );
    }

    #[test]
    fn create_index_inverts_to_drop_index() {
        let inv = structural_inverse(&Op::CreateIndex {
            name: "idx".into(),
            table: "t".into(),
            columns: vec!["a".into()],
            unique: false,
        })
        .unwrap();
        assert_eq!(inv, Op::DropIndex { name: "idx".into() });
    }

    #[test]
    fn renames_invert_by_swapping() {
        assert_eq!(
            structural_inverse(&Op::RenameTable {
                from: "a".into(),
                to: "b".into(),
            })
            .unwrap(),
            Op::RenameTable {
                from: "b".into(),
                to: "a".into(),
            }
        );
        assert_eq!(
            structural_inverse(&Op::RenameColumn {
                table: "t".into(),
                from: "a".into(),
                to: "b".into(),
            })
            .unwrap(),
            Op::RenameColumn {
                table: "t".into(),
                from: "b".into(),
                to: "a".into(),
            }
        );
    }

    #[test]
    fn destructive_ops_have_no_structural_inverse() {
        assert!(structural_inverse(&Op::DropTable { table: "t".into() }).is_err());
        assert!(structural_inverse(&Op::DropColumn {
            table: "t".into(),
            column: "c".into(),
        })
        .is_err());
        assert!(structural_inverse(&Op::DropIndex { name: "i".into() }).is_err());
    }

    #[test]
    fn additive_down_ops_reverses_order() {
        let up = vec![
            ClassifiedOp::new(Op::CreateTable {
                table: "a".into(),
                columns: vec![col("id", ColumnKind::Text)],
                without_rowid: false,
            }),
            ClassifiedOp::new(Op::AddColumn {
                table: "a".into(),
                column: col("email", ColumnKind::Text),
            }),
        ];
        let down = additive_down_ops(&up).unwrap();
        // Last applied (AddColumn) is undone first.
        assert_eq!(
            down[0].op,
            Op::DropColumn {
                table: "a".into(),
                column: "email".into(),
            }
        );
        assert_eq!(down[1].op, Op::DropTable { table: "a".into() });
    }

    #[test]
    fn additive_down_ops_rejects_destructive() {
        let up = vec![ClassifiedOp::new(Op::DropTable { table: "t".into() })];
        assert!(additive_down_ops(&up).is_err());
    }

    // -- Pre-image payload serde (pure, both feature sets) -------------------

    #[test]
    fn captured_value_round_trips_every_class() {
        let payload = PreimagePayload {
            tables: vec![TablePreimage::Column {
                table: "t".into(),
                dropped: "c".into(),
                create_sql: "CREATE TABLE t (id TEXT PRIMARY KEY, c BLOB)".into(),
                aux_ddl: vec![],
                dropped_requires_value: false,
                pk: vec!["id".into()],
                captured_columns: vec!["id".into(), "c".into()],
                rows: vec![
                    vec![CapturedValue::Text("k1".into()), CapturedValue::Null],
                    vec![
                        CapturedValue::Text("k2".into()),
                        CapturedValue::Blob(vec![0, 1, 2, 255]),
                    ],
                    vec![CapturedValue::Int(7), CapturedValue::Real(1.5)],
                ],
            }],
        };
        let json = serde_json::to_vec(&payload).unwrap();
        let back: PreimagePayload = serde_json::from_slice(&json).unwrap();
        assert_eq!(payload, back);
    }

    #[test]
    fn inline_boundary_is_pinned() {
        assert_eq!(INLINE_MAX_BYTES, 64 * 1024);
    }

    // -- Native round trips -------------------------------------------------
    #[cfg(feature = "native")]
    mod native {
        use super::*;
        use crate::migrate::apply::apply_ops;
        use crate::migrate::generator;
        use crate::migrate::ledger::{Ledger, MigrationStatus};
        use crate::migrate::lint::{enforce_preimage, lint_manifest};
        use crate::migrate::{ChecksummedManifest, Flags, Manifest};
        use rusqlite::Connection;

        fn noop(_: &ClassifiedOp) -> Result<(), MigrateError> {
            Ok(())
        }

        // -- The honest destructive apply, end to end (#326) -----------------

        /// The path that could not run before #326: author a destructive
        /// migration the way the tooling authors one -- derived op classes, no
        /// pre-image, because nothing has captured yet -- and drive it through the
        /// whole forward sequence (seal, verify, lint, gate, apply-with-capture),
        /// then reverse it from what the apply captured.
        ///
        /// The old gate refused this at the `enforce_preimage` line below, and the
        /// only way past it was to paste an empty `Inline` placeholder -- which
        /// applied the drop with no pre-image at all.
        ///
        /// The generator itself cannot author the destructive half: `generate`
        /// emits only `CreateTable`/`CreateIndex` in 0.5.0. So the schema is stood
        /// up by a real generated manifest and the drop is authored in exactly the
        /// shape `generate` produces (`ClassifiedOp::new`, `preimage: None`, honest
        /// `flags`).
        #[test]
        fn a_generator_shaped_destructive_migration_applies_captures_and_reverses() {
            let conn = Connection::open_in_memory().unwrap();

            // v1: a genuinely generated additive migration.
            let created = generator::generate(
                "create_contacts",
                &["id:pk".to_string(), "email:text".to_string()],
            )
            .expect("generator scaffolds the create migration");
            assert!(created.preimage.is_none());
            let sealed = ChecksummedManifest::seal(created).unwrap();
            sealed.verify().unwrap();
            lint_manifest(&sealed.manifest).expect("a generated manifest lints clean");
            enforce_preimage(&sealed.manifest).expect("an additive manifest needs no pre-image");
            let captured = apply_with_capture(&conn, &sealed.manifest.up).unwrap();
            assert!(
                captured.is_empty(),
                "an additive migration captures nothing"
            );

            conn.execute_batch(
                "INSERT INTO contacts (id, email) VALUES ('c1', 'a@x'), ('c2', 'b@x');",
            )
            .unwrap();

            // v2: the destructive migration, authored honestly -- and therefore
            // carrying no pre-image, because the pre-image is captured during the
            // apply this manifest has not had yet.
            let drop = Manifest {
                version: 2,
                target_schema: String::new(),
                up: vec![ClassifiedOp::new(Op::DropColumn {
                    table: "contacts".into(),
                    column: "email".into(),
                })],
                down: Vec::new(),
                preimage: None,
                flags: Flags {
                    destructive: true,
                    hash_rewriting: false,
                },
                author: None,
            };
            let sealed = ChecksummedManifest::seal(drop).unwrap();
            sealed.verify().unwrap();
            lint_manifest(&sealed.manifest).expect("an honest destructive manifest lints clean");
            enforce_preimage(&sealed.manifest)
                .expect("the gate must not demand a pre-image that only the apply can capture");

            let payload = apply_with_capture(&conn, &sealed.manifest.up)
                .expect("the destructive apply captures as it goes");

            // The op really applied...
            assert!(
                table_info(&conn, "contacts")
                    .unwrap()
                    .iter()
                    .all(|c| c.name != "email"),
                "the column was actually dropped"
            );

            // ...and the pre-image it captured is real, not a placeholder: one
            // capture, naming the dropped column, carrying both rows' values.
            assert_eq!(payload.tables.len(), 1);
            match &payload.tables[0] {
                TablePreimage::Column {
                    table,
                    dropped,
                    rows,
                    ..
                } => {
                    assert_eq!(table, "contacts");
                    assert_eq!(dropped, "email");
                    assert_eq!(rows.len(), 2);
                }
                other => panic!("expected a Column capture, got {other:?}"),
            }

            // What the apply captured also satisfies the gate on its own terms --
            // the same gate that refuses an empty placeholder accepts this.
            let mut carried = sealed.manifest.clone();
            carried.preimage = Some(Preimage::Inline {
                rows: serde_json::to_value(&payload).unwrap(),
            });
            enforce_preimage(&carried).expect("a captured pre-image is not a placeholder");

            // And the reverse is honest: the dropped data comes back.
            restore_payload(&conn, &payload).unwrap();
            let email = |id: &str| -> Option<String> {
                conn.query_row(
                    "SELECT email FROM contacts WHERE id = ?1",
                    rusqlite::params![id],
                    |r| r.get::<_, Option<String>>(0),
                )
                .unwrap()
            };
            assert_eq!(email("c1").as_deref(), Some("a@x"));
            assert_eq!(email("c2").as_deref(), Some("b@x"));
        }

        #[test]
        fn apply_with_capture_snapshots_before_the_op_mutates() {
            // The ordering apply_with_capture exists to make structural: the
            // capture reads pre-mutation state, so a drop of a table whose rows are
            // gone by capture time would come back empty. It does not.
            let conn = Connection::open_in_memory().unwrap();
            seed_users(&conn);
            let payload = apply_with_capture(
                &conn,
                &[ClassifiedOp::new(Op::DropTable {
                    table: "users".into(),
                })],
            )
            .unwrap();
            match payload.tables.as_slice() {
                [TablePreimage::Table { table, rows, .. }] => {
                    assert_eq!(table, "users");
                    assert_eq!(
                        rows.len(),
                        2,
                        "captured the rows the drop was about to lose"
                    );
                }
                other => panic!("expected one Table capture, got {other:?}"),
            }
            let exists: i64 = conn
                .query_row(
                    "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='users'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(exists, 0, "and the table is gone");
        }

        fn apply(conn: &Connection, op: Op) {
            apply_ops(conn, &[ClassifiedOp::new(op)], &mut noop).unwrap();
        }

        fn seed_users(conn: &Connection) {
            conn.execute_batch(
                "CREATE TABLE users (id TEXT PRIMARY KEY, email TEXT, keep TEXT);
                 INSERT INTO users VALUES ('u1', 'a@x', 'ka');
                 INSERT INTO users VALUES ('u2', 'b@x', 'kb');",
            )
            .unwrap();
        }

        fn email_of(conn: &Connection, id: &str) -> Option<String> {
            conn.query_row(
                "SELECT email FROM users WHERE id = ?1",
                rusqlite::params![id],
                |r| r.get::<_, Option<String>>(0),
            )
            .unwrap()
        }

        fn keep_of(conn: &Connection, id: &str) -> Option<String> {
            conn.query_row(
                "SELECT keep FROM users WHERE id = ?1",
                rusqlite::params![id],
                |r| r.get::<_, Option<String>>(0),
            )
            .unwrap()
        }

        #[test]
        fn additive_reverse_drops_created_table() {
            let conn = Connection::open_in_memory().unwrap();
            apply(
                &conn,
                Op::CreateTable {
                    table: "t".into(),
                    columns: vec![col("id", ColumnKind::Text)],
                    without_rowid: false,
                },
            );
            let up = vec![ClassifiedOp::new(Op::CreateTable {
                table: "t".into(),
                columns: vec![col("id", ColumnKind::Text)],
                without_rowid: false,
            })];
            let down = additive_down_ops(&up).unwrap();
            apply_ops(&conn, &down, &mut noop).unwrap();
            let n: i64 = conn
                .query_row(
                    "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='t'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(n, 0);
        }

        #[test]
        fn drop_column_reverse_restores_data_surgically() {
            let conn = Connection::open_in_memory().unwrap();
            seed_users(&conn);

            // Capture the delta-scoped pre-image DURING the destructive apply.
            let drop = ClassifiedOp::new(Op::DropColumn {
                table: "users".into(),
                column: "email".into(),
            });
            let mut cap = PreimageCapturer::new();
            apply_ops(&conn, std::slice::from_ref(&drop), &mut |op| {
                cap.capture_before(&conn, op)
            })
            .unwrap();
            let payload = cap.into_payload();

            // The column really is gone.
            assert!(table_info(&conn, "users")
                .unwrap()
                .iter()
                .all(|c| c.name != "email"));

            // A concurrent write lands after the drop: a new row, and an edit to
            // a surviving column of an existing row. Neither is in the pre-image.
            conn.execute_batch(
                "INSERT INTO users (id, keep) VALUES ('u3', 'kc');
                 UPDATE users SET keep = 'kb-edited' WHERE id = 'u2';",
            )
            .unwrap();

            restore_payload(&conn, &payload).unwrap();

            // Lost column restored for captured rows...
            assert_eq!(email_of(&conn, "u1").as_deref(), Some("a@x"));
            assert_eq!(email_of(&conn, "u2").as_deref(), Some("b@x"));
            // ...the concurrent edit to a surviving column survives untouched...
            assert_eq!(keep_of(&conn, "u2").as_deref(), Some("kb-edited"));
            assert_eq!(keep_of(&conn, "u1").as_deref(), Some("ka"));
            // ...and the concurrent row keeps its data with a null restored column.
            assert_eq!(keep_of(&conn, "u3").as_deref(), Some("kc"));
            assert_eq!(email_of(&conn, "u3"), None);
        }

        #[test]
        fn restore_is_idempotent_on_rerun() {
            let conn = Connection::open_in_memory().unwrap();
            seed_users(&conn);
            let drop = ClassifiedOp::new(Op::DropColumn {
                table: "users".into(),
                column: "email".into(),
            });
            let mut cap = PreimageCapturer::new();
            apply_ops(&conn, std::slice::from_ref(&drop), &mut |op| {
                cap.capture_before(&conn, op)
            })
            .unwrap();
            let payload = cap.into_payload();

            restore_payload(&conn, &payload).unwrap();
            // Re-running the restore (rebuild is skipped, UPSERT re-keys) is a no-op.
            restore_payload(&conn, &payload).unwrap();

            assert_eq!(email_of(&conn, "u1").as_deref(), Some("a@x"));
            assert_eq!(email_of(&conn, "u2").as_deref(), Some("b@x"));
            let n: i64 = conn
                .query_row("SELECT count(*) FROM users", [], |r| r.get(0))
                .unwrap();
            assert_eq!(n, 2);
        }

        #[test]
        fn drop_column_reverse_preserves_primary_key() {
            // The faithful rebuild must re-add the column AND keep the PK, so the
            // idempotent UPSERT has a conflict target to key on.
            let conn = Connection::open_in_memory().unwrap();
            seed_users(&conn);
            let drop = ClassifiedOp::new(Op::DropColumn {
                table: "users".into(),
                column: "email".into(),
            });
            let mut cap = PreimageCapturer::new();
            apply_ops(&conn, std::slice::from_ref(&drop), &mut |op| {
                cap.capture_before(&conn, op)
            })
            .unwrap();
            let payload = cap.into_payload();
            restore_payload(&conn, &payload).unwrap();

            let pk_cols: Vec<String> = table_info(&conn, "users")
                .unwrap()
                .into_iter()
                .filter(|c| c.pk > 0)
                .map(|c| c.name)
                .collect();
            assert_eq!(pk_cols, vec!["id".to_string()]);
        }

        #[test]
        fn two_drops_on_one_table_restore_lifo() {
            // Two DropColumns on the same table capture progressively narrower
            // schemas. restore_payload must unwind them LIFO (last dropped first),
            // or the earlier capture's rebuild would copy a column already gone.
            let conn = Connection::open_in_memory().unwrap();
            seed_users(&conn); // (id PK, email, keep)
            let drops = vec![
                ClassifiedOp::new(Op::DropColumn {
                    table: "users".into(),
                    column: "email".into(),
                }),
                ClassifiedOp::new(Op::DropColumn {
                    table: "users".into(),
                    column: "keep".into(),
                }),
            ];
            let mut cap = PreimageCapturer::new();
            apply_ops(&conn, &drops, &mut |op| cap.capture_before(&conn, op)).unwrap();
            let payload = cap.into_payload();
            assert_eq!(payload.tables.len(), 2);

            // Both columns are gone.
            let cols: Vec<String> = table_info(&conn, "users")
                .unwrap()
                .into_iter()
                .map(|c| c.name)
                .collect();
            assert_eq!(cols, vec!["id".to_string()]);

            restore_payload(&conn, &payload).unwrap();

            assert_eq!(email_of(&conn, "u1").as_deref(), Some("a@x"));
            assert_eq!(email_of(&conn, "u2").as_deref(), Some("b@x"));
            assert_eq!(keep_of(&conn, "u1").as_deref(), Some("ka"));
            assert_eq!(keep_of(&conn, "u2").as_deref(), Some("kb"));
        }

        #[test]
        fn drop_table_reverse_restores_rows_and_leaves_others_untouched() {
            let conn = Connection::open_in_memory().unwrap();
            conn.execute_batch(
                "CREATE TABLE gone (id TEXT PRIMARY KEY, v TEXT);
                 INSERT INTO gone VALUES ('g1', 'one'), ('g2', 'two');
                 CREATE TABLE other (id TEXT PRIMARY KEY, v TEXT);
                 INSERT INTO other VALUES ('o1', 'keep');",
            )
            .unwrap();

            let drop = ClassifiedOp::new(Op::DropTable {
                table: "gone".into(),
            });
            let mut cap = PreimageCapturer::new();
            apply_ops(&conn, std::slice::from_ref(&drop), &mut |op| {
                cap.capture_before(&conn, op)
            })
            .unwrap();
            let payload = cap.into_payload();
            assert!(!table_exists(&conn, "gone"));

            restore_payload(&conn, &payload).unwrap();

            let v1: String = conn
                .query_row("SELECT v FROM gone WHERE id = 'g1'", [], |r| r.get(0))
                .unwrap();
            let v2: String = conn
                .query_row("SELECT v FROM gone WHERE id = 'g2'", [], |r| r.get(0))
                .unwrap();
            assert_eq!(v1, "one");
            assert_eq!(v2, "two");
            // The untouched table is exactly as it was.
            let other: String = conn
                .query_row("SELECT v FROM other WHERE id = 'o1'", [], |r| r.get(0))
                .unwrap();
            assert_eq!(other, "keep");
        }

        #[test]
        fn not_null_column_without_default_is_a_clear_error() {
            let conn = Connection::open_in_memory().unwrap();
            conn.execute_batch(
                "CREATE TABLE t (id TEXT PRIMARY KEY, needed TEXT NOT NULL);
                 INSERT INTO t VALUES ('a', 'x');",
            )
            .unwrap();
            let drop = ClassifiedOp::new(Op::DropColumn {
                table: "t".into(),
                column: "needed".into(),
            });
            let mut cap = PreimageCapturer::new();
            apply_ops(&conn, std::slice::from_ref(&drop), &mut |op| {
                cap.capture_before(&conn, op)
            })
            .unwrap();
            let payload = cap.into_payload();
            let err = restore_payload(&conn, &payload).unwrap_err();
            assert!(matches!(err, MigrateError::Apply(m) if m.contains("NOT NULL")));
        }

        #[test]
        fn drop_column_reverse_preserves_surviving_and_table_constraints() {
            // HIGH-1: dropping an UNCONSTRAINED column then reversing must not
            // silently strip the surviving-column UNIQUE / COLLATE or the
            // table-level CHECK. The verbatim-DDL rebuild carries them all.
            let conn = Connection::open_in_memory().unwrap();
            conn.execute_batch(
                "CREATE TABLE t (
                     id TEXT PRIMARY KEY,
                     handle TEXT UNIQUE COLLATE NOCASE,
                     score INTEGER,
                     doomed TEXT,
                     CHECK (score >= 0)
                 );
                 INSERT INTO t (id, handle, score, doomed) VALUES ('u1','alice',5,'x');",
            )
            .unwrap();

            let drop = ClassifiedOp::new(Op::DropColumn {
                table: "t".into(),
                column: "doomed".into(),
            });
            let mut cap = PreimageCapturer::new();
            apply_ops(&conn, std::slice::from_ref(&drop), &mut |op| {
                cap.capture_before(&conn, op)
            })
            .unwrap();
            let payload = cap.into_payload();
            assert!(table_info(&conn, "t")
                .unwrap()
                .iter()
                .all(|c| c.name != "doomed"));

            restore_payload(&conn, &payload).unwrap();

            // Surviving-column UNIQUE + COLLATE NOCASE survived: a case-
            // insensitive duplicate is REJECTED.
            let dup = conn.execute(
                "INSERT INTO t (id, handle, score) VALUES ('u2','ALICE',1)",
                [],
            );
            assert!(
                dup.is_err(),
                "UNIQUE + COLLATE NOCASE must reject a case-insensitive duplicate"
            );

            // Table-level CHECK survived: a CHECK-violating row is REJECTED.
            let bad = conn.execute(
                "INSERT INTO t (id, handle, score) VALUES ('u3','bob',-5)",
                [],
            );
            assert!(
                bad.is_err(),
                "table-level CHECK(score >= 0) must reject a negative score"
            );

            // The dropped column's data round-tripped.
            let doomed: Option<String> = conn
                .query_row("SELECT doomed FROM t WHERE id = 'u1'", [], |r| r.get(0))
                .unwrap();
            assert_eq!(doomed.as_deref(), Some("x"));
        }

        #[test]
        fn drop_column_reverse_with_surviving_generated_column() {
            // A surviving GENERATED column must not be projected into during the
            // rebuild (inserting into a generated column errors); it self-populates
            // from the copied base columns. Exercises the generated_columns filter.
            let conn = Connection::open_in_memory().unwrap();
            conn.execute_batch(
                "CREATE TABLE t (
                     id TEXT PRIMARY KEY,
                     n INTEGER,
                     n2 INTEGER GENERATED ALWAYS AS (n * 2) STORED,
                     doomed TEXT
                 );
                 INSERT INTO t (id, n, doomed) VALUES ('u1', 21, 'x');",
            )
            .unwrap();

            let drop = ClassifiedOp::new(Op::DropColumn {
                table: "t".into(),
                column: "doomed".into(),
            });
            let mut cap = PreimageCapturer::new();
            apply_ops(&conn, std::slice::from_ref(&drop), &mut |op| {
                cap.capture_before(&conn, op)
            })
            .unwrap();
            let payload = cap.into_payload();

            restore_payload(&conn, &payload).unwrap();

            // The generated column survives and recomputes correctly...
            let n2: i64 = conn
                .query_row("SELECT n2 FROM t WHERE id = 'u1'", [], |r| r.get(0))
                .unwrap();
            assert_eq!(n2, 42, "generated column must recompute as n*2");
            // ...and the dropped column round-tripped.
            let doomed: Option<String> = conn
                .query_row("SELECT doomed FROM t WHERE id = 'u1'", [], |r| r.get(0))
                .unwrap();
            assert_eq!(doomed.as_deref(), Some("x"));
        }

        #[test]
        fn drop_table_reverse_restores_index_and_without_rowid() {
            // MED-2: restoring a dropped table must replay its aux DDL (an
            // explicit index) and recreate it WITHOUT ROWID from the verbatim DDL.
            let conn = Connection::open_in_memory().unwrap();
            conn.execute_batch(
                "CREATE TABLE gone (id TEXT PRIMARY KEY, v TEXT) WITHOUT ROWID;
                 CREATE INDEX gone_v ON gone(v);
                 INSERT INTO gone VALUES ('g1','one'),('g2','two');",
            )
            .unwrap();

            let drop = ClassifiedOp::new(Op::DropTable {
                table: "gone".into(),
            });
            let mut cap = PreimageCapturer::new();
            apply_ops(&conn, std::slice::from_ref(&drop), &mut |op| {
                cap.capture_before(&conn, op)
            })
            .unwrap();
            let payload = cap.into_payload();
            assert!(!table_exists(&conn, "gone"));

            restore_payload(&conn, &payload).unwrap();

            // The explicit index is back.
            let idx: i64 = conn
                .query_row(
                    "SELECT count(*) FROM sqlite_master WHERE type='index' AND name='gone_v'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(idx, 1, "the dropped table's index must be restored");

            // The table is WITHOUT ROWID again.
            let ddl: String = conn
                .query_row(
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name='gone'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert!(
                ddl.to_ascii_uppercase().contains("WITHOUT ROWID"),
                "table must be restored WITHOUT ROWID: {ddl}"
            );

            // And the rows came back.
            let v1: String = conn
                .query_row("SELECT v FROM gone WHERE id='g1'", [], |r| r.get(0))
                .unwrap();
            assert_eq!(v1, "one");
        }

        #[test]
        fn compensating_step_marks_failed_and_reelectable_on_error() {
            // MED-3: a compensating step whose restore ERRORS mid-apply must
            // propagate the Err, mark the vN+1 ledger row Failed, and leave it
            // re-electable (mark_failed recovery path).
            let conn = Connection::open_in_memory().unwrap();
            Ledger::ensure_schema(&conn).unwrap();
            conn.execute_batch(
                "CREATE TABLE t (id TEXT PRIMARY KEY, needed TEXT NOT NULL);
                 INSERT INTO t VALUES ('a','x');",
            )
            .unwrap();

            let drop = ClassifiedOp::new(Op::DropColumn {
                table: "t".into(),
                column: "needed".into(),
            });
            let mut cap = PreimageCapturer::new();
            apply_ops(&conn, std::slice::from_ref(&drop), &mut |op| {
                cap.capture_before(&conn, op)
            })
            .unwrap();
            let payload = cap.into_payload();

            // Restoring a NOT NULL column with no default errors mid-apply.
            let res = apply_compensating(&conn, 7, "c7", &[], Some(&payload), 300);
            assert!(res.is_err(), "the mid-apply error must propagate");

            // The vN+1 row is Failed...
            let v7 = Ledger::entry(&conn, 7).unwrap().unwrap();
            assert_eq!(v7.status, MigrationStatus::Failed);
            // ...and immediately re-electable (a fresh election wins the slot).
            assert_eq!(
                Ledger::try_elect(&conn, 7, "c7", 300).unwrap(),
                Election::Won
            );
        }

        #[test]
        fn additive_reverse_removes_added_column_from_live_schema() {
            // MED-4: an AddColumn down-op (its inverse, DropColumn) must leave the
            // live schema without the added column.
            let conn = Connection::open_in_memory().unwrap();
            conn.execute_batch("CREATE TABLE t (id TEXT PRIMARY KEY, extra TEXT);")
                .unwrap();
            let up = vec![ClassifiedOp::new(Op::AddColumn {
                table: "t".into(),
                column: col("extra", ColumnKind::Text),
            })];
            let down = additive_down_ops(&up).unwrap();
            apply_ops(&conn, &down, &mut noop).unwrap();
            let cols: Vec<String> = table_info(&conn, "t")
                .unwrap()
                .into_iter()
                .map(|c| c.name)
                .collect();
            assert_eq!(cols, vec!["id".to_string()]);
        }

        #[test]
        fn additive_reverse_drops_created_index_from_live_schema() {
            // MED-4: a CreateIndex down-op (its inverse, DropIndex) must remove
            // the index from the live schema.
            let conn = Connection::open_in_memory().unwrap();
            conn.execute_batch(
                "CREATE TABLE t (id TEXT PRIMARY KEY, v TEXT);
                 CREATE INDEX t_v ON t(v);",
            )
            .unwrap();
            let up = vec![ClassifiedOp::new(Op::CreateIndex {
                name: "t_v".into(),
                table: "t".into(),
                columns: vec!["v".into()],
                unique: false,
            })];
            let down = additive_down_ops(&up).unwrap();
            apply_ops(&conn, &down, &mut noop).unwrap();
            let n: i64 = conn
                .query_row(
                    "SELECT count(*) FROM sqlite_master WHERE type='index' AND name='t_v'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(n, 0);
        }

        fn table_exists(conn: &Connection, table: &str) -> bool {
            let n: i64 = conn
                .query_row(
                    "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?1",
                    rusqlite::params![table],
                    |r| r.get(0),
                )
                .unwrap();
            n > 0
        }

        // -- Compensating vN+1 ledger step ----------------------------------

        #[test]
        fn compensating_step_is_append_only() {
            let conn = Connection::open_in_memory().unwrap();
            Ledger::ensure_schema(&conn).unwrap();
            // v1 lands: create a table.
            apply(
                &conn,
                Op::CreateTable {
                    table: "t".into(),
                    columns: vec![col("id", ColumnKind::Text)],
                    without_rowid: false,
                },
            );
            assert_eq!(
                Ledger::try_elect(&conn, 1, "c1", 300).unwrap(),
                Election::Won
            );
            Ledger::mark_success(&conn, 1).unwrap();
            let v1_before = Ledger::entry(&conn, 1).unwrap().unwrap();

            // Reverse v1 as an append-only compensating v2 (drop the table).
            let down = additive_down_ops(&[ClassifiedOp::new(Op::CreateTable {
                table: "t".into(),
                columns: vec![col("id", ColumnKind::Text)],
                without_rowid: false,
            })])
            .unwrap();
            let outcome =
                apply_compensating(&conn, 2, "c2-compensating", &down, None, 300).unwrap();
            assert_eq!(outcome, Election::Won);

            // The reversed v1 row is byte-for-byte unchanged...
            let v1_after = Ledger::entry(&conn, 1).unwrap().unwrap();
            assert_eq!(v1_before, v1_after);
            // ...a v2 row exists and succeeded...
            let v2 = Ledger::entry(&conn, 2).unwrap().unwrap();
            assert_eq!(v2.status, MigrationStatus::Success);
            // ...the chain is intact...
            Ledger::verify_chain(&conn).unwrap();
            // ...and the table is gone.
            assert!(!table_exists(&conn, "t"));
        }

        #[test]
        fn compensating_step_skips_when_already_applied() {
            let conn = Connection::open_in_memory().unwrap();
            Ledger::ensure_schema(&conn).unwrap();
            Ledger::try_elect(&conn, 2, "c2", 300).unwrap();
            Ledger::mark_success(&conn, 2).unwrap();
            let outcome = apply_compensating(&conn, 2, "c2", &[], None, 300).unwrap();
            assert_eq!(outcome, Election::AlreadyApplied);
        }

        #[test]
        fn destructive_compensating_step_restores_via_payload() {
            let conn = Connection::open_in_memory().unwrap();
            Ledger::ensure_schema(&conn).unwrap();
            seed_users(&conn);

            let drop = ClassifiedOp::new(Op::DropColumn {
                table: "users".into(),
                column: "email".into(),
            });
            let mut cap = PreimageCapturer::new();
            apply_ops(&conn, std::slice::from_ref(&drop), &mut |op| {
                cap.capture_before(&conn, op)
            })
            .unwrap();
            let payload = cap.into_payload();

            // The compensating step's structural restore rides the payload (there
            // is no simple `down` op that faithfully re-adds a constrained column).
            let outcome = apply_compensating(&conn, 5, "c5", &[], Some(&payload), 300).unwrap();
            assert_eq!(outcome, Election::Won);
            assert_eq!(email_of(&conn, "u1").as_deref(), Some("a@x"));
            let v5 = Ledger::entry(&conn, 5).unwrap().unwrap();
            assert_eq!(v5.status, MigrationStatus::Success);
        }

        // -- Inline vs Ref storage boundary ---------------------------------

        fn big_payload(rows: usize) -> PreimagePayload {
            let data: Vec<Vec<CapturedValue>> = (0..rows)
                .map(|i| {
                    vec![
                        CapturedValue::Text(format!("key-{i:08}")),
                        CapturedValue::Text("x".repeat(64)),
                    ]
                })
                .collect();
            PreimagePayload {
                tables: vec![TablePreimage::Column {
                    table: "t".into(),
                    dropped: "c".into(),
                    create_sql: "CREATE TABLE t (id TEXT PRIMARY KEY, c TEXT)".into(),
                    aux_ddl: vec![],
                    dropped_requires_value: false,
                    pk: vec!["id".into()],
                    captured_columns: vec!["id".into(), "c".into()],
                    rows: data,
                }],
            }
        }

        #[tokio::test]
        async fn small_payload_stores_inline() {
            let payload = big_payload(1);
            let stored = store_preimage(&payload, None).await.unwrap();
            assert!(matches!(stored, Preimage::Inline { .. }));
            let back = load_preimage(&stored, None).await.unwrap();
            assert_eq!(back, payload);
        }

        #[tokio::test]
        async fn inline_boundary_is_pinned_behaviorally() {
            // MED-1: pin the boundary by BEHAVIOR, not just the const. A payload
            // serializing to exactly INLINE_MAX_BYTES rides inline; one byte over
            // is refused. Pad a single ASCII text value (1 JSON byte per char, no
            // escaping) so the serialized length lands precisely on the boundary.
            let mk = |pad: usize| PreimagePayload {
                tables: vec![TablePreimage::Table {
                    table: "t".into(),
                    create_sql: "CREATE TABLE t (id TEXT PRIMARY KEY)".into(),
                    aux_ddl: vec![],
                    pk: vec!["id".into()],
                    columns: vec!["id".into()],
                    rows: vec![vec![CapturedValue::Text("a".repeat(pad))]],
                }],
            };
            let base = serde_json::to_vec(&mk(0)).unwrap().len();
            let pad = INLINE_MAX_BYTES - base;

            let at = mk(pad);
            assert_eq!(serde_json::to_vec(&at).unwrap().len(), INLINE_MAX_BYTES);
            assert!(
                matches!(
                    store_preimage(&at, None).await.unwrap(),
                    Preimage::Inline { .. }
                ),
                "a payload exactly at the boundary must ride inline"
            );

            let over = mk(pad + 1);
            assert_eq!(
                serde_json::to_vec(&over).unwrap().len(),
                INLINE_MAX_BYTES + 1
            );
            let err = store_preimage(&over, None).await.unwrap_err();
            assert!(
                matches!(err, MigrateError::Apply(m) if m.contains("inline limit")),
                "one byte over the boundary must be refused without a relay"
            );
        }

        #[tokio::test]
        async fn large_payload_without_relay_is_refused() {
            let payload = big_payload(2000);
            let bytes = serde_json::to_vec(&payload).unwrap();
            assert!(
                bytes.len() > INLINE_MAX_BYTES,
                "payload must exceed the boundary"
            );
            let err = store_preimage(&payload, None).await.unwrap_err();
            assert!(matches!(err, MigrateError::Apply(m) if m.contains("inline limit")));
        }

        #[tokio::test]
        async fn large_payload_round_trips_through_relay() {
            let dir = tempfile::TempDir::new().unwrap();
            std::fs::create_dir_all(dir.path()).unwrap();
            let config = StashConfig {
                url: format!("file://{}/manifest.json", dir.path().display()),
                access_key_id: None,
                secret_access_key: None,
                region: None,
                endpoint: None,
            };
            let payload = big_payload(2000);
            let stored = store_preimage(&payload, Some(&config)).await.unwrap();
            let key = match &stored {
                Preimage::Ref { key } => key.clone(),
                other => panic!("expected a Ref, got {other:?}"),
            };
            // The reference is the content hash of the payload.
            assert_eq!(key.len(), 64);
            let back = load_preimage(&stored, Some(&config)).await.unwrap();
            assert_eq!(back, payload);
        }

        #[tokio::test]
        async fn large_ref_without_config_is_refused() {
            let stored = Preimage::Ref {
                key: "deadbeef".into(),
            };
            let err = load_preimage(&stored, None).await.unwrap_err();
            assert!(matches!(err, MigrateError::Apply(_)));
        }

        // -- Ledger preimage_ref is read-only for reverse -------------------

        #[test]
        fn preimage_ref_reads_from_the_ledger_row() {
            let conn = Connection::open_in_memory().unwrap();
            Ledger::ensure_schema(&conn).unwrap();
            Ledger::try_elect(&conn, 1, "c1", 300).unwrap();
            Ledger::mark_success(&conn, 1).unwrap();
            // No ref yet.
            let entry = Ledger::entry(&conn, 1).unwrap().unwrap();
            assert!(preimage_ref_of(&entry).is_none());
            // The driver (#296) writes the forward-compat column out of band.
            conn.execute(
                &format!(
                    "UPDATE \"{}\" SET preimage_ref = 'abc123' WHERE version = 1",
                    crate::migrate::ledger::LEDGER_TABLE
                ),
                [],
            )
            .unwrap();
            let entry = Ledger::entry(&conn, 1).unwrap().unwrap();
            assert_eq!(
                preimage_ref_of(&entry),
                Some(Preimage::Ref {
                    key: "abc123".into()
                })
            );
            // Writing the column did not break the chain.
            Ledger::verify_chain(&conn).unwrap();
        }

        // -- #463: apply_compensating shares driver.rs's elect_apply_settle -

        /// Non-vacuity for #463's extraction: before it, `apply_compensating`
        /// settled success with a bare `Ledger::mark_success(conn, version)?`
        /// and no fallback -- if THAT call itself errored (as opposed to the
        /// down_ops/payload apply above it), the row was left `pending` with
        /// a live lease, exactly the abandoned-pending state the ledger's
        /// crash table (`driver.rs` module doc) exists to rule out.
        /// `apply_migration` already funnelled a `mark_success` failure into
        /// the same best-effort `mark_failed` as any other mid-run error;
        /// `apply_compensating` did not. Sharing `elect_apply_settle` gives
        /// it that fallback for free -- this test is the guard that did not
        /// fire before the extraction and does now.
        ///
        /// A trigger that fails only the `UPDATE ... SET status = 'success'`
        /// isolates the settle step alone: `down_ops` and `payload` are both
        /// empty, so there is nothing else in this run that could fail.
        #[test]
        fn compensating_step_falls_back_to_mark_failed_when_mark_success_itself_errors() {
            let conn = Connection::open_in_memory().unwrap();
            Ledger::ensure_schema(&conn).unwrap();
            conn.execute_batch(&format!(
                "CREATE TRIGGER poison_success
                     BEFORE UPDATE OF status ON \"{}\"
                     WHEN NEW.status = 'success'
                 BEGIN
                     SELECT RAISE(ABORT, 'forced mark_success failure for #463 test');
                 END;",
                crate::migrate::ledger::LEDGER_TABLE
            ))
            .unwrap();

            let err = apply_compensating(&conn, 9, "c9", &[], None, 300).unwrap_err();
            assert!(
                err.to_string().contains("forced mark_success failure"),
                "the poisoned mark_success's own error must surface, not a different one: {err}"
            );

            let entry = Ledger::entry(&conn, 9).unwrap().unwrap();
            assert_eq!(
                entry.status,
                MigrationStatus::Failed,
                "a mark_success failure must fall back to mark_failed via the shared \
                 elect_apply_settle funnel, not leave the row pending with a live lease"
            );
            // And, exactly like any other failed row, immediately re-electable.
            assert_eq!(
                Ledger::try_elect(&conn, 9, "c9", 300).unwrap(),
                Election::Won
            );
        }
    }
}
