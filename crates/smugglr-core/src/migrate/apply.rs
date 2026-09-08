//! Forward apply engine (#273).
//!
//! Applies a manifest's forward [`Op`]s to a **local** SQLite target,
//! idempotently and **per-op**. Composition -- lint, pre-image capture, ledger
//! recording -- lives in the driver (#296), never here: this module imports no
//! ledger and touches no config/target resolution. The public primitive is
//! [`apply_ops`], which the driver (#296) and the surgical log (#289) drive
//! through a per-op `pre_op` write-ahead hook.
//!
//! # Two compilation surfaces
//!
//! `migrate::mod` declares `pub mod apply;` with **no** `native` gate, so this
//! file compiles on `wasm32`. Everything that touches `rusqlite` (the local
//! execution path: [`apply_ops`] and its helpers) is therefore internally
//! `#[cfg(feature = "native")]`. The pure statement *generators* for remote
//! targets ([`d1_statements`], [`turso_statements`], [`rqlite_statements`]) and
//! the shared SQL string builders are always compiled -- they are `String`
//! producers with no database handle.
//!
//! # Idempotency is per-op, not blanket (decision 6, spikes A/I/J)
//!
//! There is no single "apply is idempotent" property. Each op earns it:
//! - `CREATE TABLE` / `CREATE INDEX` use `IF NOT EXISTS`.
//! - `ADD COLUMN` has no `IF NOT EXISTS`; it is guarded by a raw
//!   `PRAGMA table_info` precheck (and treats a "duplicate column name" error as
//!   success), because reusing `local::table_info_inner` would wrongly error on
//!   an absent or PK-less table.
//! - `DROP COLUMN` cannot `ALTER`-drop an indexed / `UNIQUE` / `PRIMARY KEY`
//!   column; it drops referencing explicit indexes first, and falls back to a
//!   guarded 12-step table rebuild for the columns SQLite still refuses.
//! - `RENAME TABLE` / `RENAME COLUMN` have no `IF EXISTS`; they are guarded by an
//!   existence precheck so a re-run is a no-op.
//!
//! # Transactions and the foreign-keys pragma
//!
//! `apply_ops` takes a `&rusqlite::Connection` (never a pre-opened
//! `Transaction`) and opens its **own** transaction *per op*. This is load
//! bearing: `PRAGMA foreign_keys` is silently a no-op while a transaction is
//! open, and the 12-step rebuild of a **referenced** table (spike K) must run
//! with foreign-key enforcement off. The rebuild therefore toggles the pragma
//! *outside* any transaction -- `foreign_keys=OFF` (autocommit) -> `BEGIN` ->
//! rebuild + `sqlite_sequence` preserve -> `foreign_key_check` /
//! `integrity_check` -> `COMMIT` -> restore the caller's prior `foreign_keys`
//! state -- and simple ops each get their own short transaction.
//!
//! # Rebuild reconstruction is faithful for keys, warned-on for the rest
//!
//! The `DROP COLUMN` rebuild reconstructs the surviving schema from
//! `PRAGMA table_info` / `foreign_key_list` plus the original
//! `sqlite_master.sql`, which recovers column types, `NOT NULL`, defaults, the
//! primary key (single-column rowid-alias **and** composite), foreign keys
//! (**including composite**) with their `ON DELETE` / `ON UPDATE` referential
//! actions, `AUTOINCREMENT`, and **generated columns** with their expressions
//! and storage classes. It still **cannot** recover `CHECK` constraints,
//! table-level / surviving-column `UNIQUE`, column `COLLATE`, or
//! `WITHOUT ROWID`. Rather than drop those silently, the rebuild emits a
//! `tracing::warn!` when the table being rebuilt carries such constructs, so
//! the loss is visible.
//!
//! That list is narrower than it reads, and one change is the reason rather
//! than several. **Every surviving non-key column is carried verbatim from the
//! original DDL** rather than re-derived from `PRAGMA table_info` (#396, #344),
//! so anything declared ON a column comes with it: its `CHECK`, its `COLLATE`,
//! its column-level `UNIQUE`, its expression `DEFAULT` complete with the
//! parentheses the pragma drops, and its blank declared type where the pragma
//! reports an empty string this path used to turn into `BLOB`. What is still
//! genuinely unrecoverable is the **table-level** form of `CHECK` and `UNIQUE`,
//! and `WITHOUT ROWID`.
//!
//! Key columns are the exception and stay derived. The primary key is rebuilt
//! from the *surviving* key columns so that dropping one member of a composite
//! leaves a real key on the rest (#273), and a verbatim declaration would bring
//! its own `PRIMARY KEY` tag into a body about to be given one.
//!
//! A lone surviving `INTEGER` key column is where derivation used to lose a
//! fact `PRAGMA table_info` never carried: whether that column's `PRIMARY KEY`
//! was declared `DESC`. Ascending and `DESC` report identically through the
//! pragma -- position only, never direction -- and a bare re-declared
//! `PRIMARY KEY` is SQLite's rowid alias, so re-deriving one always ascending
//! silently turned a `DESC` key into the alias it never was, changing what
//! values the column takes on the next `INSERT` that omits it (#413). The
//! stored `CREATE TABLE` DDL is asked instead (`pk_check::column_level_pk_is_desc`):
//! a column declared column-level `DESC` is re-declared `DESC`, the one
//! column-level spelling that keeps a lone `INTEGER` primary key from being
//! the alias, and a stored DDL that will not parse refuses the rebuild by
//! name rather than guessing. Deliberately narrow, not "was this column
//! already the alias": a composite key's surviving member never had an
//! ASC/DESC spelling of its own, and a `WITHOUT ROWID` table's key has no
//! rowid to be an alias of, so both keep re-deriving ascending exactly as
//! before -- untouched, and untested either direction, by this fix.
//!
//! That exception costs more than tag placement, and saying only "the key is
//! derived" understates it. **A key column still loses everything the pragma
//! cannot report**: an expression `DEFAULT` on one is still re-emitted without
//! its parentheses and still fails the rebuild outright, a typeless key column
//! still comes back `BLOB`, and a `COLLATE` on one is still dropped -- silently
//! turning a case-insensitive key case-sensitive. Those are the same defects
//! #396 and #344 name, surviving on the columns this fix does not reach.
//! Recorded rather than implied, and filed as #401.
//!
//! The warning still over-reports `CHECK` and `COLLATE`, because
//! `lost_constructs` finds them by a keyword scan over the whole table's DDL
//! and cannot attribute them to a column -- so it cannot tell one that rode
//! through from one that did not. Over-reporting is the direction this warning
//! already errs in.
//!
//! **Generated columns** took three passes and the sequence explains the shape
//! of the code. `table_info` -- the pragma the whole reconstruction introspects
//! with -- omits them entirely, so they were never in the set of columns to
//! rebuild *and never in the set of constructs to warn about*: the loss was
//! total and silent (#342). They are found through `PRAGMA table_xinfo`, which
//! reports them with `hidden` `2` (`VIRTUAL`) or `3` (`STORED`), and that made
//! the loss loud. It did nothing for recovery, because the generation
//! *expression* is in no pragma at all.
//!
//! Recovery therefore reads `sqlite_master.sql` and takes the column's whole
//! declaration verbatim (#387), which brings the expression and anything else
//! declared alongside it. That is a text split rather than a pragma read, so it
//! is the one recovery here that can be *uncertain* -- and an uncertain one
//! falls back to the warn-and-drop above rather than emitting a guess, for all
//! of a table's generated columns or none. A wrong expression produces a table
//! that is broken rather than merely diminished.
//!
//! Two things a foreign key can carry are **not** recovered, and they are named
//! rather than left to the word "foreign keys" to imply. `MATCH` is absent from
//! `foreign_key_list` entirely -- the pragma reports `NONE` even for a key
//! declared `MATCH FULL` -- so it joins the warned-loss list above; SQLite
//! parses `MATCH` and ignores it, so what is lost is declared text and not
//! behaviour. `DEFERRABLE` / `INITIALLY DEFERRED` has no pragma column at all
//! and is not warned about, so a rebuild silently makes a deferred constraint
//! immediate. That is the shape of #341 still open on a narrower construct, and
//! stating it here is the point: the sentence above used to claim foreign keys
//! were recovered while their actions were being dropped, and the fix for that
//! is not to write a slightly wider claim. A rebuild driven by an
//! *explicit* target schema (as reverse #274 / convert #280 will pass) does not
//! have this gap.
//!
//! Explicit indexes and **triggers** are not in that list: the swap's
//! `DROP TABLE` destroys both, and the rebuild replays their verbatim
//! `sqlite_master` DDL afterwards (#336). The one exception is a trigger whose
//! body mentions the dropped column -- SQLite would accept the `CREATE TRIGGER`
//! and then fail every later write to the table -- so that trigger joins the
//! warned-loss list instead of being replayed.

use crate::migrate::{ClassifiedOp, Column, ColumnKind, Constraint, MigrateError, Op};

#[cfg(feature = "native")]
use rusqlite::{params, Connection, OptionalExtension};

// ===========================================================================
// Pure SQL builders (always compiled -- no rusqlite)
// ===========================================================================

/// Quote a SQL identifier, escaping embedded double-quotes by doubling.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The SQLite storage-class keyword generated from a [`ColumnKind`].
fn column_kind_keyword(kind: ColumnKind) -> &'static str {
    match kind {
        ColumnKind::Text => "TEXT",
        ColumnKind::Int => "INTEGER",
        ColumnKind::Real => "REAL",
        ColumnKind::Blob => "BLOB",
    }
}

/// Render one structured [`Column`] as a SQL column definition fragment.
///
/// Constraints are emitted in declared order. `DEFAULT` and `CHECK` payloads are
/// carried verbatim (they are author-supplied SQL expressions).
///
/// Exposed `pub(crate)` so callers assembling a [`RebuildTarget::Fragments`] body
/// (convert #280) can reuse the column-DDL lowering without duplicating it.
pub(crate) fn render_column_def(col: &Column) -> String {
    let mut s = format!(
        "{} {}",
        quote_ident(&col.name),
        column_kind_keyword(col.kind)
    );
    for c in &col.constraints {
        match c {
            Constraint::Pk => s.push_str(" PRIMARY KEY"),
            Constraint::Fk { table, col } => {
                s.push_str(&format!(
                    " REFERENCES {}({})",
                    quote_ident(table),
                    quote_ident(col)
                ));
            }
            Constraint::Unique => s.push_str(" UNIQUE"),
            Constraint::NotNull => s.push_str(" NOT NULL"),
            Constraint::Default(expr) => s.push_str(&format!(" DEFAULT {expr}")),
            Constraint::Check(expr) => s.push_str(&format!(" CHECK ({expr})")),
        }
    }
    s
}

/// Render a `CREATE TABLE IF NOT EXISTS` statement from structured columns.
fn render_create_table(table: &str, columns: &[Column], without_rowid: bool) -> String {
    let defs = columns
        .iter()
        .map(render_column_def)
        .collect::<Vec<_>>()
        .join(", ");
    let mut s = format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        quote_ident(table),
        defs
    );
    if without_rowid {
        s.push_str(" WITHOUT ROWID");
    }
    s
}

/// Find every `create_table` op in a manifest that would mint a SQLite
/// rowid-alias or `AUTOINCREMENT` primary key (#427).
///
/// Renders each `create_table` op's DDL exactly as apply would emit it --
/// reusing [`render_create_table`], never a second lowering -- and runs it
/// through [`crate::pk_check::classify_table_ddl`]'s L1 shape check, keeping
/// only the two findings [`crate::pk_check::rowid_refusals`] scopes to (the
/// rowid alias and `AUTOINCREMENT`; `NoPrimaryKey` and the L2/L3 findings are
/// deliberately not gated here -- see that function's doc). An empty result
/// means the manifest is clean.
///
/// Returns raw findings rather than an enforced error so each caller renders
/// its own message once: [`crate::migrate::generator::generate`] (scaffold
/// time) folds a non-empty result into its own `GeneratorError`, and the
/// `migrate apply` CLI entry (run before the driver claims a ledger version)
/// passes it straight to [`crate::pk_check::enforce`]. Wrapping an already-
/// rendered [`crate::pk_check::enforce`] error a second time here would double
/// the "Configuration error:" prefix on the generator path.
///
/// Shared by both call sites so a hand-authored manifest that never passed
/// through the generator is still caught -- the second refusal site the
/// issue requires. Unlike an existing database (#280, a different issue, out
/// of scope here), this is **new** DDL smugglr is about to mint itself, so
/// `pk_check.rs`'s 0.5.0 "warn, no in-tool remedy" carve-out does not apply:
/// the remedy is to write `id:pk` (TEXT), and both callers hard-refuse rather
/// than warn.
pub fn rowid_alias_findings(ops: &[ClassifiedOp]) -> Vec<crate::pk_check::PkFinding> {
    let mut findings = Vec::new();
    for classified in ops {
        if let Op::CreateTable {
            table,
            columns,
            without_rowid,
        } = &classified.op
        {
            let ddl = render_create_table(table, columns, *without_rowid);
            findings.extend(crate::pk_check::classify_table_ddl(table, &ddl));
        }
    }
    crate::pk_check::rowid_refusals(&findings)
}

/// Render the single SQL statement for one op.
///
/// This is the one-statement lowering shared by the native simple-op path and
/// every remote generator. Idempotency-safe where SQLite allows it
/// (`IF NOT EXISTS` / `IF EXISTS`); `ADD COLUMN` and `DROP COLUMN` have no such
/// clause, so the native path guards them out-of-band (the remote generators
/// emit the bare statement, since remote transport -- and thus any precheck --
/// is deferred to #291).
fn statement_for(op: &Op) -> String {
    match op {
        Op::CreateTable {
            table,
            columns,
            without_rowid,
        } => render_create_table(table, columns, *without_rowid),
        Op::DropTable { table } => format!("DROP TABLE IF EXISTS {}", quote_ident(table)),
        Op::AddColumn { table, column } => format!(
            "ALTER TABLE {} ADD COLUMN {}",
            quote_ident(table),
            render_column_def(column)
        ),
        Op::DropColumn { table, column } => format!(
            "ALTER TABLE {} DROP COLUMN {}",
            quote_ident(table),
            quote_ident(column)
        ),
        Op::CreateIndex {
            name,
            table,
            columns,
            unique,
        } => {
            let cols = columns
                .iter()
                .map(|c| quote_ident(c))
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "CREATE {}INDEX IF NOT EXISTS {} ON {} ({})",
                if *unique { "UNIQUE " } else { "" },
                quote_ident(name),
                quote_ident(table),
                cols
            )
        }
        Op::DropIndex { name } => format!("DROP INDEX IF EXISTS {}", quote_ident(name)),
        Op::RenameTable { from, to } => format!(
            "ALTER TABLE {} RENAME TO {}",
            quote_ident(from),
            quote_ident(to)
        ),
        Op::RenameColumn { table, from, to } => format!(
            "ALTER TABLE {} RENAME COLUMN {} TO {}",
            quote_ident(table),
            quote_ident(from),
            quote_ident(to)
        ),
    }
}

// ===========================================================================
// Remote targets: pure statement generators (always compiled)
// ===========================================================================

/// A remote SQLite-dialect apply target (decision 8: one dialect, N strategies).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteTarget {
    /// Cloudflare D1.
    D1,
    /// Turso / libSQL.
    Turso,
    /// rqlite.
    Rqlite,
}

impl RemoteTarget {
    /// Stable, human-facing name of the target (used in error messages).
    pub fn name(self) -> &'static str {
        match self {
            RemoteTarget::D1 => "d1",
            RemoteTarget::Turso => "turso",
            RemoteTarget::Rqlite => "rqlite",
        }
    }
}

/// Statements for a Cloudflare **D1** apply.
///
/// D1 has no interactive `BEGIN..COMMIT`; a batch *is* its transaction unit, so
/// this lowers to one batch. Cross-table DDL wants `defer_foreign_keys` so an
/// out-of-order reference does not trip mid batch. The first statement enables
/// it; the rest are the ops in order.
///
/// # Precondition: execute these as one atomic batch
///
/// The returned slice is sound **only** when the caller executes it inside a
/// single enclosing transaction -- for D1, one `db.batch()` call.
///
/// `PRAGMA defer_foreign_keys` defers enforcement until the *outermost*
/// transaction commits. Under autocommit -- which is exactly what a non-atomic
/// batch is, one implicit transaction per statement -- every statement commits
/// at its own end, enforcement fires there, and statement 0 buys nothing. So
/// splitting this slice across requests, or handing it to a driver that batches
/// non-atomically, silently reinstates the mid-batch foreign-key failure the
/// pragma is here to prevent. The failure is in the caller's transaction scope,
/// not in this SQL, which is why it cannot be detected by reading the output.
///
/// Contrast [`rqlite_statements`], which supplies its own `BEGIN`/`COMMIT`. It
/// still expects its statements to travel together -- what differs is the
/// failure mode when they do not. A split there is loud: an unbalanced `BEGIN`
/// or a stray `COMMIT` fails where it is sent. A split here is silent, because
/// every statement is individually valid and only the deferral quietly stops
/// meaning anything. That is why this one needs saying and that one does not.
/// The explicit framing is omitted here only because D1 rejects `BEGIN`.
///
/// This crate cannot enforce the boundary: [`apply_remote`] has no transport in
/// 0.5.0, so callers use these generators directly and own the batch boundary
/// themselves.
pub fn d1_statements(ops: &[ClassifiedOp]) -> Vec<String> {
    let mut out = Vec::with_capacity(ops.len() + 1);
    out.push("PRAGMA defer_foreign_keys = ON".to_string());
    out.extend(ops.iter().map(|c| statement_for(&c.op)));
    out
}

/// Statements for a **Turso / libSQL** apply.
///
/// Turso's extended `ALTER TABLE` means the direct one-statement lowering
/// applies without the local 12-step rebuild, so this is simply each op in
/// order. Transaction framing is the embedded-replica connection's concern.
pub fn turso_statements(ops: &[ClassifiedOp]) -> Vec<String> {
    ops.iter().map(|c| statement_for(&c.op)).collect()
}

/// Statements for an **rqlite** apply.
///
/// rqlite applies through the Raft leader as one bulk transaction; the ops are
/// wrapped in `BEGIN` / `COMMIT` so the leader commits them atomically.
pub fn rqlite_statements(ops: &[ClassifiedOp]) -> Vec<String> {
    let mut out = Vec::with_capacity(ops.len() + 2);
    out.push("BEGIN".to_string());
    out.extend(ops.iter().map(|c| statement_for(&c.op)));
    out.push("COMMIT".to_string());
    out
}

/// Execute a forward apply against a **remote** target.
///
/// Unsupported in 0.5.0: the host->target DDL transport does not exist (the
/// `DataSource` trait is data-plane only), so this always returns
/// [`MigrateError::RemoteTransportUnsupported`], deferred to #291. The statement
/// generators above are the usable half today. Callers that only need the SQL
/// should call `d1_statements` / `turso_statements` / `rqlite_statements`
/// directly rather than routing through this -- and must then honor each
/// generator's own transaction contract, which this function would otherwise
/// have owned. In particular [`d1_statements`] is sound only as one atomic
/// batch; see its precondition.
pub fn apply_remote(target: RemoteTarget, _ops: &[ClassifiedOp]) -> Result<(), MigrateError> {
    Err(MigrateError::RemoteTransportUnsupported(
        target.name().to_string(),
    ))
}

// ===========================================================================
// Local execution (native only)
// ===========================================================================

/// Bridge rusqlite errors into the migrate error type so the native apply path
/// can use `?`. Native-gated because `rusqlite::Error` only exists on native.
#[cfg(feature = "native")]
impl From<rusqlite::Error> for MigrateError {
    fn from(e: rusqlite::Error) -> Self {
        MigrateError::Apply(e.to_string())
    }
}

/// Apply forward ops to a local connection, idempotently and per-op.
///
/// For each op: call `pre_op(op)` first (the write-ahead hook -- #289 logs
/// intent before mutation, #296 lints/captures), then apply that single op
/// inside its own transaction (or, for a rebuild, its own pragma-bounded
/// sequence). A failure aborts the remaining ops and surfaces; already-applied
/// ops stay applied (per-op idempotency makes a re-run safe).
///
/// Takes a bare `&Connection` -- not a pre-opened `Transaction` -- so the
/// per-op transaction boundary and the rebuild's `foreign_keys` toggle work,
/// and so tests can drive `Connection::open_in_memory()` directly.
#[cfg(feature = "native")]
pub fn apply_ops(
    conn: &Connection,
    ops: &[ClassifiedOp],
    pre_op: &mut dyn FnMut(&ClassifiedOp) -> Result<(), MigrateError>,
) -> Result<(), MigrateError> {
    for op in ops {
        pre_op(op)?;
        apply_one(conn, &op.op)?;
    }
    Ok(())
}

/// Apply exactly one op. The single-op core the [`apply_ops`] loop calls.
#[cfg(feature = "native")]
fn apply_one(conn: &Connection, op: &Op) -> Result<(), MigrateError> {
    match op {
        // Naturally idempotent single statements (IF [NOT] EXISTS).
        Op::CreateTable { .. }
        | Op::DropTable { .. }
        | Op::CreateIndex { .. }
        | Op::DropIndex { .. } => exec_in_txn(conn, &statement_for(op)),

        Op::AddColumn { table, column } => add_column(conn, table, column),
        Op::DropColumn { table, column } => drop_column(conn, table, column),
        Op::RenameTable { from, to } => rename_table(conn, from, to),
        Op::RenameColumn { table, from, to } => rename_column(conn, table, from, to),
    }
}

/// Run one DDL statement inside its own short transaction.
#[cfg(feature = "native")]
fn exec_in_txn(conn: &Connection, sql: &str) -> Result<(), MigrateError> {
    let tx = conn.unchecked_transaction()?;
    tx.execute_batch(sql)?;
    tx.commit()?;
    Ok(())
}

// --- ADD COLUMN (guarded, spike A/I/J) -------------------------------------

/// Idempotent `ADD COLUMN`: skip if the column already exists.
///
/// SQLite has no `ADD COLUMN IF NOT EXISTS`, so a re-run raises "duplicate
/// column name". A raw `PRAGMA table_info` precheck avoids the error on the
/// happy path, and a "duplicate column name" error is still treated as success
/// as a belt-and-suspenders guard against a race.
#[cfg(feature = "native")]
fn add_column(conn: &Connection, table: &str, column: &Column) -> Result<(), MigrateError> {
    if raw_table_columns(conn, table)?
        .iter()
        .any(|c| c == &column.name)
    {
        return Ok(());
    }
    let sql = format!(
        "ALTER TABLE {} ADD COLUMN {}",
        quote_ident(table),
        render_column_def(column)
    );
    let tx = conn.unchecked_transaction()?;
    match tx.execute_batch(&sql) {
        Ok(()) => {
            tx.commit()?;
            Ok(())
        }
        Err(e) if is_duplicate_column(&e) => Ok(()), // tx drops -> rollback; already present
        Err(e) => Err(e.into()),
    }
}

/// Whether a rusqlite error is SQLite's "duplicate column name".
#[cfg(feature = "native")]
fn is_duplicate_column(e: &rusqlite::Error) -> bool {
    matches!(e, rusqlite::Error::SqliteFailure(_, Some(msg)) if msg.contains("duplicate column name"))
}

// --- RENAME (guarded for idempotency) --------------------------------------

/// Idempotent `RENAME TABLE`: no-op if already renamed.
#[cfg(feature = "native")]
fn rename_table(conn: &Connection, from: &str, to: &str) -> Result<(), MigrateError> {
    if !table_exists(conn, from)? {
        if table_exists(conn, to)? {
            return Ok(()); // already renamed on a prior run
        }
        return Err(MigrateError::Apply(format!(
            "cannot rename table: neither {from:?} nor {to:?} exists"
        )));
    }
    exec_in_txn(
        conn,
        &format!(
            "ALTER TABLE {} RENAME TO {}",
            quote_ident(from),
            quote_ident(to)
        ),
    )
}

/// Idempotent `RENAME COLUMN`: no-op if already renamed.
#[cfg(feature = "native")]
fn rename_column(conn: &Connection, table: &str, from: &str, to: &str) -> Result<(), MigrateError> {
    let cols = raw_table_columns(conn, table)?;
    if !cols.iter().any(|c| c == from) {
        if cols.iter().any(|c| c == to) {
            return Ok(()); // already renamed on a prior run
        }
        return Err(MigrateError::Apply(format!(
            "cannot rename column: {table:?} has neither {from:?} nor {to:?}"
        )));
    }
    exec_in_txn(
        conn,
        &format!(
            "ALTER TABLE {} RENAME COLUMN {} TO {}",
            quote_ident(table),
            quote_ident(from),
            quote_ident(to)
        ),
    )
}

// --- DROP COLUMN (index-drop, then guarded rebuild) ------------------------

/// Idempotent `DROP COLUMN`.
///
/// Absent column -> no-op. Otherwise drop any **explicit** (`CREATE INDEX`)
/// indexes that reference the column, then try the direct
/// `ALTER TABLE ... DROP COLUMN`. SQLite still refuses to drop a `PRIMARY KEY` /
/// `UNIQUE` / otherwise-constrained column; that falls back to a guarded 12-step
/// rebuild (spikes B/K, `integrity_check`).
#[cfg(feature = "native")]
fn drop_column(conn: &Connection, table: &str, column: &str) -> Result<(), MigrateError> {
    if !raw_table_columns(conn, table)?.iter().any(|c| c == column) {
        return Ok(());
    }

    // Drop explicit indexes that reference the column INSIDE the same
    // transaction as the direct ALTER (SQLite refuses to drop an indexed
    // column). Keeping the index drops in-transaction makes the attempt atomic:
    // if the ALTER is refused (PK / UNIQUE / otherwise constrained), `drop(tx)`
    // rolls the index drops back so the rebuild path takes over from an intact
    // table -- an explicit index is never lost to a failed attempt (#273
    // LOW-MED#5).
    let mut sql = String::new();
    for idx in explicit_indexes_referencing(conn, table, column)? {
        sql.push_str(&format!("DROP INDEX IF EXISTS {};\n", quote_ident(&idx)));
    }
    sql.push_str(&format!(
        "ALTER TABLE {} DROP COLUMN {}",
        quote_ident(table),
        quote_ident(column)
    ));

    let tx = conn.unchecked_transaction()?;
    match tx.execute_batch(&sql) {
        Ok(()) => {
            tx.commit()?;
            Ok(())
        }
        Err(_) => {
            // Direct drop refused (PK / UNIQUE / constrained). Roll back the
            // attempt (restoring any dropped indexes) and rebuild the table.
            drop(tx);
            rebuild_dropping_column(conn, table, column)
        }
    }
}

/// Build a [`RebuildSpec`] that drops `column`, then run the rebuild.
#[cfg(feature = "native")]
fn rebuild_dropping_column(
    conn: &Connection,
    table: &str,
    column: &str,
) -> Result<(), MigrateError> {
    let info = raw_table_info(conn, table)?;
    let orig_sql = table_sql(conn, table)?;
    let has_autoincrement = orig_sql.as_deref().is_some_and(sql_has_autoincrement);

    let kept: Vec<&ColInfo> = info.iter().filter(|c| c.name != column).collect();
    if kept.is_empty() {
        return Err(MigrateError::Apply(format!(
            "cannot drop the only column {column:?} from {table:?}"
        )));
    }

    // Derive the primary key from the SURVIVING pk>0 columns -- never from the
    // pre-filter key width. Dropping one member of a composite key must leave a
    // real key on the survivors, not silently discard the whole PK (#273 HIGH#1).
    // A single survivor is inlined (preserving rowid-alias / AUTOINCREMENT
    // semantics for a single INTEGER column); two or more become a table-level
    // constraint.
    let mut pk: Vec<&&ColInfo> = kept.iter().filter(|c| c.pk > 0).collect();
    pk.sort_by_key(|c| c.pk);
    let inline_pk: Option<&str> = if pk.len() == 1 {
        Some(pk[0].name.as_str())
    } else {
        None
    };

    // A lone surviving INTEGER PK column reconstructs below as a bare
    // `PRIMARY KEY`, and a bare `INTEGER PRIMARY KEY` IS SQLite's rowid alias
    // (#413). `PRAGMA table_info` cannot tell that spelling apart from
    // `INTEGER PRIMARY KEY DESC`, which is NOT one -- position only, never
    // direction -- so ask the one place the direction survives: the stored
    // `CREATE TABLE` DDL.
    //
    // Deliberately narrow: this asks only "was `PRIMARY KEY DESC` declared
    // column-level on this column", not the broader "was this column already
    // the rowid alias". That broader question answers "no" -- and this
    // fix must not act on -- for a composite key's surviving member (never
    // had an ASC/DESC spelling of its own) and for a `WITHOUT ROWID` table's
    // key (no rowid to be an alias of, so ASC/DESC there is ordinary index
    // direction, not this question at all); both fall through to the
    // ascending re-declaration this rebuild already emitted, unchanged, which
    // is #413's scope boundary, not an oversight.
    //
    // - Declared column-level `DESC`: re-declaring it ascending would start
    //   drawing its values from the rowid sequence on every future INSERT
    //   that omits it, silently -- so `DESC` is what gets re-declared
    //   instead, the one column-level spelling that keeps a lone INTEGER PK
    //   column from being the alias.
    // - Not declared column-level `DESC` (ascending, table-level, or
    //   composite): the ascending re-declaration below is unchanged.
    // - Undeterminable (the stored DDL will not parse): refuse rather than
    //   guess, the same rule the verbatim recovery below already follows for
    //   an uncertain generated-column split.
    let mut inline_pk_needs_desc = false;
    if pk.len() == 1 {
        let pk_col = pk[0];
        if pk_col.ty.eq_ignore_ascii_case("INTEGER") {
            match orig_sql
                .as_deref()
                .and_then(|sql| crate::pk_check::column_level_pk_is_desc(sql, &pk_col.name))
            {
                Some(true) => inline_pk_needs_desc = true,
                Some(false) => {}
                None => {
                    return Err(MigrateError::Apply(format!(
                        "cannot rebuild {table:?}: dropping {column:?} would leave \
                         {:?} as the sole INTEGER PRIMARY KEY column, and {:?}'s original \
                         ASC/DESC direction could not be read from {table:?}'s stored CREATE \
                         TABLE. Reconstructing it as a bare PRIMARY KEY risks silently turning \
                         it into SQLite's rowid alias, which changes what values it takes on a \
                         future INSERT that omits it -- refusing rather than guessing.",
                        pk_col.name, pk_col.name
                    )));
                }
            }
        }
    }

    // Generated columns the rebuild can carry through, by name (#387).
    //
    // `table_info` cannot see them, so without this they are absent from `kept`,
    // from the new table and from the copy projection -- dropped entirely and,
    // before #342, silently. What is recoverable is their *verbatim definition*
    // from the original DDL, because the generation expression exists in no
    // pragma.
    //
    // All or nothing: if any surviving generated column's definition cannot be
    // located confidently, none are carried and the whole table falls back to
    // the warn-and-drop this path did before. A rebuild that preserved some and
    // dropped others would be a schema nobody asked for, and harder to reason
    // about than either outcome on its own.
    let generated = generated_columns(conn, table)?;
    let surviving_generated: Vec<&(String, &'static str)> = generated
        .iter()
        .filter(|(name, _)| !name.eq_ignore_ascii_case(column))
        .collect();
    let preserved_generated: Option<Vec<(String, String)>> = if surviving_generated.is_empty() {
        None
    } else {
        verbatim_definitions_for(orig_sql.as_deref(), &surviving_generated, column)
    };

    // Ordinary columns are carried verbatim too, for the same reason generated
    // ones are and with the same all-or-nothing fallback (#396, #344).
    //
    // `render_def` re-derives a column from `PRAGMA table_info`, and the pragma
    // answers a narrower question than the derivation assumes -- twice over.
    // It reports an expression `DEFAULT` WITHOUT the parentheses SQLite
    // requires, so `DEFAULT ('a' || ')')` is re-emitted as a syntax error
    // (#396). And it reports a typeless column's type as the empty string,
    // which this path turns into `BLOB`, moving the column into the class
    // `rowhash::is_blob_column` deliberately excludes (#344).
    //
    // The original declaration has neither problem, because it is what the
    // author wrote. Taking it verbatim also carries `CHECK`, `COLLATE` and a
    // column-level `UNIQUE` through, which the module doc used to list as
    // unrecoverable.
    //
    // PRIMARY KEY columns are deliberately NOT carried verbatim. The key is
    // derived from the *surviving* key columns so that dropping one member of a
    // composite leaves a real key on the rest (#273), and a verbatim
    // declaration would bring its own `PRIMARY KEY` tag into a body that is
    // about to be given one.
    let verbatim_ordinary: Option<Vec<(String, String)>> = {
        let wanted: Vec<(String, &'static str)> = kept
            .iter()
            .filter(|c| c.pk == 0)
            .map(|c| (c.name.clone(), "ordinary"))
            .collect();
        let refs: Vec<&(String, &'static str)> = wanted.iter().collect();
        if refs.is_empty() {
            None
        } else {
            verbatim_definitions_for(orig_sql.as_deref(), &refs, column)
        }
    };

    let mut body: Vec<String> = kept
        .iter()
        .map(|c| {
            if c.pk == 0 {
                if let Some(found) = verbatim_ordinary
                    .as_ref()
                    .and_then(|v| v.iter().find(|(name, _)| name == &c.name))
                {
                    return found.1.clone();
                }
            }
            let mut def = c.render_def();
            if Some(c.name.as_str()) == inline_pk {
                def.push_str(" PRIMARY KEY");
                if inline_pk_needs_desc {
                    // Preserves the DESC trap (#413): re-declaring DESC is
                    // what keeps this column from becoming the rowid alias it
                    // never was. AUTOINCREMENT is illegal on a DESC key (it
                    // only exists on the rowid alias itself), so the two are
                    // mutually exclusive by construction here.
                    def.push_str(" DESC");
                } else if has_autoincrement && c.ty.eq_ignore_ascii_case("INTEGER") {
                    // AUTOINCREMENT is only legal on a single INTEGER
                    // rowid-alias PK.
                    def.push_str(" AUTOINCREMENT");
                }
            }
            def
        })
        .collect();

    // Put the preserved generated columns back where they were declared.
    //
    // Position matters to anyone reading the table with `SELECT *`, and
    // appending them would silently reorder the schema on a path whose whole
    // job is to change one thing. `table_xinfo` reports every column in
    // declaration order, generated ones included, so walking it gives each
    // surviving generated column the index it had -- counting only the columns
    // that survive, since the dropped one shifts everything after it.
    if let Some(preserved) = &preserved_generated {
        let declared = table_xinfo_names(conn, table)?;
        let mut at = 0usize;
        for name in declared {
            if name.eq_ignore_ascii_case(column) {
                continue;
            }
            match preserved
                .iter()
                .find(|(generated, _)| generated.eq_ignore_ascii_case(&name))
            {
                Some((_, definition)) => {
                    body.insert(at.min(body.len()), definition.clone());
                    at += 1;
                }
                None => at += 1,
            }
        }
    }

    if pk.len() > 1 {
        let cols = pk
            .iter()
            .map(|c| quote_ident(&c.name))
            .collect::<Vec<_>>()
            .join(", ");
        body.push(format!("PRIMARY KEY ({cols})"));
    }

    // Preserve foreign keys whose column set does not include the dropped one.
    // Composite FKs are reconstructed as a single grouped constraint; the whole
    // FK is dropped if ANY member column is the one being removed (#273 HIGH#2).
    let mut inline_fk_consumed: std::collections::BTreeSet<String> = Default::default();
    for fk in reconstruct_foreign_keys(conn, table)? {
        if fk.references_column(column) {
            continue;
        }
        // A single-column key may have been declared INLINE on the column, in
        // which case carrying that column verbatim already brought it along and
        // emitting it again here would give the rebuilt table two keys where
        // the original had one. `foreign_key_list` does not say whether a key
        // was inline or table-level, so the verbatim declaration is asked
        // instead: if it names REFERENCES, the key came with it.
        //
        // A composite key cannot be inline, so this only ever applies to
        // single-column ones.
        if fk.cols.len() == 1 && !inline_fk_consumed.contains(&fk.cols[0].0) {
            let carried_inline = verbatim_ordinary.as_ref().is_some_and(|v| {
                v.iter().any(|(name, definition)| {
                    name == &fk.cols[0].0
                        && inline_reference_target(definition)
                            .is_some_and(|target| target.eq_ignore_ascii_case(&fk.parent_table))
                })
            });
            if carried_inline {
                // AT MOST ONE per column. SQLite accepts an inline REFERENCES
                // *and* a table-level FOREIGN KEY on the same column, and
                // `foreign_key_list` reports both -- but the verbatim
                // declaration carries only the inline one, so skipping every
                // row for that column dropped the table-level key silently.
                //
                // Found by an adversarial review, which is the second time this
                // guard has been wrong in a different direction: first it
                // duplicated a key, now it could lose one. A column definition
                // can hold at most one inline REFERENCES, so consuming exactly
                // one row is the rule rather than a heuristic.
                inline_fk_consumed.insert(fk.cols[0].0.clone());
                continue;
            }
        }
        body.push(fk.render());
    }

    // Indexes and triggers do not survive the swap's DROP TABLE; collect the DDL
    // to replay, and the triggers that cannot be replayed at all (#336).
    let aux = aux_ddl_surviving_drop(conn, table, column)?;

    // Constructs the pragma/DDL reconstruction cannot recover are lost on this
    // rebuild; make the loss loud rather than silent (#273 MED#4).
    let mut lost = lost_constructs(
        conn,
        table,
        column,
        orig_sql.as_deref(),
        &aux.dropped_triggers,
    )?;
    // A generated column this rebuild is carrying through is not a loss, and
    // saying it is would be worse than saying nothing: a warning that fires on
    // work that succeeded is the kind operators learn to skip, which would cost
    // the #342 warning its value on the cases where it is real (#387).
    //
    // The same applies to a `UNIQUE` that rode through on a preserved column's
    // verbatim definition. `lost_constructs` finds those from `index_list` and
    // has no way to know the column was carried, so it reports a constraint the
    // rebuilt table demonstrably still enforces.
    //
    // `CHECK` and `COLLATE` need no equivalent filter here (smugglr#462):
    // `lost_constructs` now attributes each occurrence to the top-level item
    // that names it and only reports the dropped column's own clause or a
    // table-level constraint, so a construct on some other ordinary column
    // -- the case this filter exists to catch for generated columns and
    // `UNIQUE` -- is never reported as lost in the first place.
    //
    // What that attribution does not model, and this filter does not cover
    // either, is the narrower fact that a *surviving* column is not always
    // carried verbatim: a PK column never is (`verbatim_ordinary` above
    // deliberately excludes it, so its `CHECK`/`COLLATE` really is rebuilt
    // away by `render_def`), and an ordinary column's verbatim carry is
    // all-or-nothing across every kept ordinary column, so one unresolved
    // definition silently loses `CHECK`/`COLLATE` on every other surviving
    // ordinary column too. Both are real, narrow under-reports this issue
    // knowingly accepts rather than simulates -- the rest of the module
    // already treats "the dropped column's own clause" as the case worth
    // pinning (see `lost_constructs`'s doc), and over-reporting on every
    // other CHECK/COLLATE in the table, as the pre-#462 scan did, cost the
    // warning its value on the case that is real.
    {
        // Every column carried verbatim, generated or ordinary. The first
        // version of this filter only considered generated ones while its own
        // comment claimed it covered anything that rode through -- so a UNIQUE
        // on a carried ORDINARY column was reported as dropped while the
        // rebuilt table demonstrably still enforced it. That is not
        // over-reporting, it is a false statement of loss, and an adversarial
        // review caught the comment before a reader did.
        let carried: Vec<&str> = preserved_generated
            .iter()
            .flatten()
            .chain(verbatim_ordinary.iter().flatten())
            .map(|(name, _)| name.as_str())
            .collect();
        let mut carried_unique: Vec<String> = Vec::new();
        for index in unique_constraint_indexes(conn, table)? {
            let cols = index_columns(conn, &index)?;
            if !cols.is_empty()
                && cols
                    .iter()
                    .all(|c| carried.iter().any(|name| name.eq_ignore_ascii_case(c)))
            {
                carried_unique.push(format!("UNIQUE({})", cols.join(", ")));
            }
        }
        lost.retain(|entry| {
            let is_carried_column = carried
                .iter()
                .any(|name| entry.starts_with(&format!("generated column {name:?}")));
            let is_carried_unique = carried_unique.iter().any(|u| u == entry);
            !is_carried_column && !is_carried_unique
        });
    }
    if !lost.is_empty() {
        tracing::warn!(
            "DROP COLUMN rebuild of {table:?} (dropping {column:?}) cannot reconstruct \
             these surviving constructs; they are dropped: {}",
            lost.join(", ")
        );
    }

    let projection: Vec<(String, String)> = kept
        .iter()
        .map(|c| (c.name.clone(), quote_ident(&c.name)))
        .collect();

    let spec = RebuildSpec {
        table: table.to_string(),
        target: RebuildTarget::Fragments {
            body,
            without_rowid: false, // reconstruction does not recover WITHOUT ROWID
        },
        projection,
        post_ddl: aux.ddl,
    };
    rebuild_table(conn, &spec)
}

// --- The guarded 12-step rebuild -------------------------------------------

/// The target schema for a rebuild: either reassembled DDL fragments or a
/// verbatim `CREATE TABLE` captured from `sqlite_master`.
///
/// The distinction is load bearing for constraint fidelity:
///
/// - [`RebuildTarget::Fragments`] is only as faithful as the caller's assembly.
///   `rebuild_dropping_column` builds it from `PRAGMA table_info` /
///   `foreign_key_list`, which cannot recover `CHECK` / surviving-column `UNIQUE`
///   / `COLLATE`, so that path warns on the loss. Generated columns are the
///   exception: they are carried through verbatim from the original DDL
///   (#387), falling back to that same warning when they cannot be.
/// - [`RebuildTarget::Verbatim`] carries the exact pre-mutation DDL, so every
///   constraint survives byte-for-byte. Reverse (#274) uses it to re-add a dropped
///   column without re-deriving (and thus stripping) the surviving schema.
#[cfg(feature = "native")]
pub(crate) enum RebuildTarget {
    /// Reassembled `CREATE TABLE` body fragments (column defs + table-level
    /// constraints), joined by `, `, plus the `WITHOUT ROWID` flag.
    Fragments {
        /// The full body fragments (column defs + table-level constraints).
        body: Vec<String>,
        /// Whether the rebuilt table is `WITHOUT ROWID`.
        without_rowid: bool,
    },
    /// A verbatim `CREATE TABLE` (as stored in `sqlite_master`); the rebuild
    /// splices the temp-table name in place of the original before executing it,
    /// so no constraint is inferred or lost.
    Verbatim {
        /// The verbatim `CREATE TABLE` DDL for the target schema.
        create_sql: String,
    },
}

/// A fully-formed rebuild plan: the caller (`rebuild_dropping_column`, or reverse
/// #274 via [`rebuild_to_schema`]) has already resolved the target schema,
/// projection, and post-rebuild DDL.
///
/// `target` is the *explicit target schema*: [`rebuild_to_schema`] renders exactly
/// what it carries, inferring nothing from pragmas, so a caller that supplies a
/// verbatim capture (reverse's pre-image; convert #280) has none of the
/// `DROP COLUMN` reconstruction limits documented on this module.
#[cfg(feature = "native")]
pub(crate) struct RebuildSpec {
    /// The table being rebuilt (dropped and recreated under this final name).
    pub table: String,
    /// The target schema (reassembled fragments or a verbatim capture).
    pub target: RebuildTarget,
    /// `(dest_column, source_expr)` pairs for the `INSERT ... SELECT` copy.
    pub projection: Vec<(String, String)>,
    /// Extra statements to run after the rename (e.g. recreate indexes).
    pub post_ddl: Vec<String>,
}

/// The temp table name used mid-rebuild.
///
/// A single connection rebuilds one table at a time, so a fixed name cannot
/// COLLIDE with a concurrent rebuild. That was the whole argument, and this
/// comment used to conclude "safe" from it -- which is a different claim, and a
/// false one: the name can also be a table the USER owns, and the rebuild used
/// to open with `DROP TABLE IF EXISTS` on it (#338).
///
/// It now refuses instead. The drop it replaced was defending a state that
/// cannot occur: the rebuild runs inside one transaction and SQLite rolls DDL
/// back, so a crash mid-rebuild leaves no temp table to clean up.
#[cfg(feature = "native")]
const REBUILD_TMP: &str = "_smugglr_rebuild_tmp";

/// Splice `new_name` (quoted) in for the table name of a verbatim
/// `CREATE TABLE` statement, leaving the rest of the DDL byte-for-byte intact.
///
/// Reverse (#274) builds the mid-rebuild temp table this way: the captured
/// pre-drop DDL keeps every constraint, and only the name is retargeted so it can
/// coexist with the still-present original during the swap. Scans past
/// `CREATE [TEMP|TEMPORARY] TABLE [IF NOT EXISTS]` and any `schema.` qualifier to
/// the name identifier, then replaces just that span.
#[cfg(feature = "native")]
fn splice_create_table_name(create_sql: &str, new_name: &str) -> Result<String, MigrateError> {
    use crate::sql_ddl::{ident_end, match_kw, skip_ws};

    let b = create_sql.as_bytes();
    let not_create = || {
        MigrateError::Apply(format!(
            "captured DDL is not a CREATE TABLE statement: {create_sql:?}"
        ))
    };

    let mut i = skip_ws(b, 0);
    i = match_kw(b, i, "CREATE").ok_or_else(not_create)?;
    i = skip_ws(b, i);
    if let Some(j) = match_kw(b, i, "TEMPORARY").or_else(|| match_kw(b, i, "TEMP")) {
        i = skip_ws(b, j);
    }
    i = match_kw(b, i, "TABLE").ok_or_else(not_create)?;
    i = skip_ws(b, i);
    // Optional IF NOT EXISTS.
    if let Some(j) = match_kw(b, i, "IF") {
        let j = skip_ws(b, j);
        let j = match_kw(b, j, "NOT").ok_or_else(not_create)?;
        let j = skip_ws(b, j);
        let j = match_kw(b, j, "EXISTS").ok_or_else(not_create)?;
        i = skip_ws(b, j);
    }

    let name_start = i;
    let mut name_end = ident_end(b, i).ok_or_else(|| {
        MigrateError::Apply(format!(
            "could not locate table name in captured DDL: {create_sql:?}"
        ))
    })?;
    // A `schema.name` qualifier: the replaced span runs through the second ident.
    let after = skip_ws(b, name_end);
    if after < b.len() && b[after] == b'.' {
        let k = skip_ws(b, after + 1);
        name_end = ident_end(b, k).ok_or_else(|| {
            MigrateError::Apply(format!(
                "malformed qualified table name in captured DDL: {create_sql:?}"
            ))
        })?;
    }

    Ok(format!(
        "{}{}{}",
        &create_sql[..name_start],
        quote_ident(new_name),
        &create_sql[name_end..]
    ))
}

/// Rebuild a table to an **explicit target schema** (the crate entry for reverse
/// #274 / convert #280).
///
/// Unlike the `DROP COLUMN` path -- which *infers* the surviving schema from
/// `PRAGMA table_info` / `foreign_key_list` and therefore cannot recover `CHECK` /
/// `UNIQUE` / `COLLATE` / `WITHOUT ROWID`, and recovers generated columns only
/// by reading the original DDL back (#387) -- this takes the
/// schema as a fully-formed [`RebuildSpec`]: the caller supplies the
/// [`RebuildTarget`] (reassembled fragments *or* a verbatim capture), a
/// `projection` (the `INSERT ... SELECT` copy from the *current* table into the
/// target), and `post_ddl` (indexes to recreate). With a
/// [`RebuildTarget::Verbatim`] capture the reconstruction limits above do not
/// apply at all -- every constraint is carried byte-for-byte. The table being
/// rebuilt must already exist (the copy reads from it); recreating a *dropped*
/// table is a `CREATE TABLE`, not a rebuild.
///
/// The guarantees of the 12-step rebuild still hold: `foreign_keys` is toggled
/// outside the transaction, `sqlite_sequence` is preserved, and
/// `foreign_key_check` + `integrity_check` gate the commit.
#[cfg(feature = "native")]
pub(crate) fn rebuild_to_schema(conn: &Connection, spec: &RebuildSpec) -> Result<(), MigrateError> {
    rebuild_table(conn, spec)
}

/// Run the guarded 12-step table rebuild.
///
/// Toggles `foreign_keys` **outside** the transaction (the pragma is a no-op
/// while a transaction is open), so a rebuild of a *referenced* table does not
/// cascade-delete children when the old table is dropped (spike K). Preserves
/// `sqlite_sequence` (spike B), and runs `foreign_key_check` +
/// `integrity_check` before committing -- a violation rolls the whole rebuild
/// back and reports the offending rows (spike L).
#[cfg(feature = "native")]
fn rebuild_table(conn: &Connection, spec: &RebuildSpec) -> Result<(), MigrateError> {
    // Capture the caller's prior foreign_keys state so it is *restored*, not
    // forced ON: a caller that ran with enforcement off must not silently gain
    // it from a rebuild (#273 LOW). Read it in autocommit, before the toggle.
    let prior_fk: i64 = conn.query_row("PRAGMA foreign_keys", [], |r| r.get(0))?;
    // foreign_keys OFF must happen in autocommit, before BEGIN.
    conn.execute_batch("PRAGMA foreign_keys = OFF;")?;
    let result = rebuild_inner(conn, spec);
    // Restore the prior enforcement state regardless of outcome. Surface an
    // inner error first.
    let restore = conn.execute_batch(if prior_fk != 0 {
        "PRAGMA foreign_keys = ON;"
    } else {
        "PRAGMA foreign_keys = OFF;"
    });
    result?;
    restore?;
    Ok(())
}

#[cfg(feature = "native")]
fn rebuild_inner(conn: &Connection, spec: &RebuildSpec) -> Result<(), MigrateError> {
    let old_seq = read_sqlite_sequence(conn, &spec.table)?;

    let tx = conn.unchecked_transaction()?;

    // Refuse rather than drop (#338). A `DROP TABLE IF EXISTS` here destroys a
    // user table that happens to carry this name, silently, inside a migration
    // that then reports success -- and nothing afterwards can tell an operator
    // it happened, because the table is simply gone.
    //
    // Refusing rather than uniquifying the name, because the cleanup the drop
    // provided is not needed: the whole rebuild is one transaction and SQLite
    // rolls DDL back, so there is no crashed-mid-rebuild state that leaves a
    // temp table behind. A unique name would buy nothing and cost the ability
    // to say plainly what is in the way.
    if table_exists(&tx, REBUILD_TMP)? {
        return Err(MigrateError::Apply(format!(
            "cannot rebuild {:?}: a table named {REBUILD_TMP:?} already exists. That name is \
             smugglr's rebuild scratch space, and this migration will not drop a table it did \
             not create. Rename or remove {REBUILD_TMP:?} and re-run.",
            spec.table
        )));
    }
    let create = match &spec.target {
        RebuildTarget::Fragments {
            body,
            without_rowid,
        } => {
            let mut c = format!(
                "CREATE TABLE {} ({})",
                quote_ident(REBUILD_TMP),
                body.join(", ")
            );
            if *without_rowid {
                c.push_str(" WITHOUT ROWID");
            }
            c
        }
        RebuildTarget::Verbatim { create_sql } => {
            splice_create_table_name(create_sql, REBUILD_TMP)?
        }
    };
    tx.execute_batch(&create)?;

    // Copy the data across the projection.
    let dest = spec
        .projection
        .iter()
        .map(|(d, _)| quote_ident(d))
        .collect::<Vec<_>>()
        .join(", ");
    let src = spec
        .projection
        .iter()
        .map(|(_, s)| s.clone())
        .collect::<Vec<_>>()
        .join(", ");
    tx.execute_batch(&format!(
        "INSERT INTO {} ({}) SELECT {} FROM {}",
        quote_ident(REBUILD_TMP),
        dest,
        src,
        quote_ident(&spec.table)
    ))?;

    // Swap: drop the old table, rename the temp into place.
    tx.execute_batch(&format!("DROP TABLE {}", quote_ident(&spec.table)))?;
    tx.execute_batch(&format!(
        "ALTER TABLE {} RENAME TO {}",
        quote_ident(REBUILD_TMP),
        quote_ident(&spec.table)
    ))?;

    // Recreate the indexes / triggers the swap dropped, in that order.
    for stmt in &spec.post_ddl {
        tx.execute_batch(stmt)?;
    }

    // Carry the autoincrement high-water mark forward (spike B) -- recreating an
    // AUTOINCREMENT table resets seq to max(rowid), below the old high-water
    // after deletes.
    if let Some(seq) = old_seq {
        restore_sqlite_sequence(&tx, &spec.table, seq)?;
    }

    // Constraint / referential integrity gate (spikes K/L).
    let fk_violations = foreign_key_violations(&tx, &spec.table)?;
    if !fk_violations.is_empty() {
        return Err(MigrateError::Apply(format!(
            "rebuild of {:?} would violate foreign keys: {}",
            spec.table,
            fk_violations.join("; ")
        ))); // tx drops -> rollback
    }
    let integrity: String =
        tx.query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))?;
    if integrity != "ok" {
        return Err(MigrateError::Apply(format!(
            "rebuild of {:?} failed integrity_check: {integrity}",
            spec.table
        )));
    }

    tx.commit()?;
    Ok(())
}

/// The `foreign_key_check` violations for one table, as readable messages.
#[cfg(feature = "native")]
fn foreign_key_violations(conn: &Connection, table: &str) -> Result<Vec<String>, MigrateError> {
    let mut stmt = conn.prepare(&format!("PRAGMA foreign_key_check({})", quote_ident(table)))?;
    let rows = stmt
        .query_map([], |r| {
            let child: String = r.get(0)?;
            let rowid: Option<i64> = r.get(1)?;
            let parent: String = r.get(2)?;
            Ok(format!(
                "row {} in {child:?} has no parent in {parent:?}",
                rowid
                    .map(|n| n.to_string())
                    .unwrap_or_else(|| "?".to_string())
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

// --- sqlite_sequence preservation (spike B) --------------------------------

/// Read the `sqlite_sequence` high-water for a table, if any.
#[cfg(feature = "native")]
fn read_sqlite_sequence(conn: &Connection, table: &str) -> Result<Option<i64>, MigrateError> {
    if !sqlite_sequence_exists(conn)? {
        return Ok(None);
    }
    let seq = conn
        .query_row(
            "SELECT seq FROM sqlite_sequence WHERE name = ?1",
            params![table],
            |r| r.get::<_, i64>(0),
        )
        .optional()?;
    Ok(seq)
}

/// Restore a table's `sqlite_sequence` high-water inside the rebuild txn.
#[cfg(feature = "native")]
fn restore_sqlite_sequence(conn: &Connection, table: &str, seq: i64) -> Result<(), MigrateError> {
    let updated = conn.execute(
        "UPDATE sqlite_sequence SET seq = ?2 WHERE name = ?1",
        params![table, seq],
    )?;
    if updated == 0 {
        conn.execute(
            "INSERT INTO sqlite_sequence(name, seq) VALUES(?1, ?2)",
            params![table, seq],
        )?;
    }
    Ok(())
}

/// Whether the `sqlite_sequence` bookkeeping table exists (it does iff at least
/// one `AUTOINCREMENT` table exists).
#[cfg(feature = "native")]
fn sqlite_sequence_exists(conn: &Connection) -> Result<bool, MigrateError> {
    let n: i64 = conn.query_row(
        "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'sqlite_sequence'",
        [],
        |r| r.get(0),
    )?;
    Ok(n > 0)
}

// --- Raw schema introspection (tolerant, unlike local::table_info_inner) ---

/// One row of `PRAGMA table_info`, tolerant of absent / PK-less tables.
#[cfg(feature = "native")]
struct ColInfo {
    name: String,
    ty: String,
    notnull: bool,
    dflt: Option<String>,
    /// PK position (1-based); `0` means "not part of the primary key".
    pk: i64,
}

#[cfg(feature = "native")]
impl ColInfo {
    /// Reconstruct a column definition's type / `NOT NULL` / `DEFAULT`. The
    /// primary key is emitted by the caller (`rebuild_dropping_column`), which
    /// decides inline vs table-level from the *surviving* key columns -- so this
    /// never bakes in a `PRIMARY KEY` tag.
    fn render_def(&self) -> String {
        let mut s = format!("{} {}", quote_ident(&self.name), self.ty);
        if self.notnull {
            s.push_str(" NOT NULL");
        }
        if let Some(d) = &self.dflt {
            s.push_str(&format!(" DEFAULT {d}"));
        }
        s
    }
}

/// Column names of a table, or an empty vec if the table does not exist.
///
/// Unlike `local::table_info_inner`, this never errors on an absent or PK-less
/// table -- exactly what the `ADD COLUMN` / `RENAME` / `DROP COLUMN` idempotency
/// prechecks need.
///
/// `table_xinfo` rather than `table_info`, because those prechecks ask "does
/// this column exist" and `table_info` answers a narrower question: it omits
/// generated columns entirely. The difference was reachable and silent (#389).
/// `DROP COLUMN` naming a generated column found it absent, concluded the drop
/// had already been applied, and returned `Ok(())` having dropped nothing -- a
/// destructive op reporting success. `ADD COLUMN` and `RENAME` were wrong in
/// the same direction for the same reason, if less visibly.
///
/// Returning generated columns is the correct answer to the question every
/// caller is asking. `hidden` is deliberately not filtered on: a virtual-table
/// hidden column is still a name that cannot be added twice.
#[cfg(feature = "native")]
fn raw_table_columns(conn: &Connection, table: &str) -> Result<Vec<String>, MigrateError> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_xinfo({})", quote_ident(table)))?;
    let cols = stmt
        .query_map([], |r| r.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(cols)
}

/// Full `PRAGMA table_info` rows for a table (empty if absent).
#[cfg(feature = "native")]
fn raw_table_info(conn: &Connection, table: &str) -> Result<Vec<ColInfo>, MigrateError> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({})", quote_ident(table)))?;
    let rows = stmt
        .query_map([], |r| {
            Ok(ColInfo {
                name: r.get::<_, String>(1)?,
                ty: {
                    let t: String = r.get(2)?;
                    if t.is_empty() {
                        // A typeless column reconstructs as BLOB affinity.
                        "BLOB".to_string()
                    } else {
                        t
                    }
                },
                notnull: r.get::<_, i64>(3)? != 0,
                dflt: r.get::<_, Option<String>>(4)?,
                pk: r.get::<_, i64>(5)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    // The primary key is *not* tagged here: `rebuild_dropping_column` derives it
    // from the surviving key columns (a pre-filter `pk_count` would mis-handle a
    // composite key one member of which is being dropped -- #273 HIGH#1).
    Ok(rows)
}

/// The original `CREATE TABLE` DDL for a table (`None` if absent).
///
/// Used to recover facts the structured pragmas cannot expose -- notably
/// `AUTOINCREMENT` and the presence of `CHECK` / `COLLATE` / `WITHOUT ROWID`.
#[cfg(feature = "native")]
fn table_sql(conn: &Connection, table: &str) -> Result<Option<String>, MigrateError> {
    let sql = conn
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?1",
            params![table],
            |r| r.get::<_, Option<String>>(0),
        )
        .optional()?;
    Ok(sql.flatten())
}

/// The verbatim definition of each named column, or `None` if any of them
/// cannot be resolved unambiguously.
///
/// All-or-nothing on purpose (#387): the caller uses this to decide whether to
/// preserve generated columns at all, and preserving some while dropping others
/// would produce a table neither the old behaviour nor the new one describes.
///
/// A name matching more than one top-level item is treated as unresolved rather
/// than resolved to the first. That is the safety valve for a column named like
/// a table-constraint keyword -- a generated column called `foreign` alongside a
/// `FOREIGN KEY (...)` clause -- where picking either would be a guess.
#[cfg(feature = "native")]
fn verbatim_definitions_for(
    create_sql: Option<&str>,
    wanted: &[&(String, &'static str)],
    dropped: &str,
) -> Option<Vec<(String, String)>> {
    let items = crate::sql_ddl::top_level_items(create_sql?)?;
    let mut out = Vec::with_capacity(wanted.len());
    for (name, _) in wanted {
        let mut matches = items
            .iter()
            .filter(|(leading, _)| leading.eq_ignore_ascii_case(name));
        let (_, definition) = matches.next()?;
        if matches.next().is_some() {
            return None;
        }

        // A generation expression computed FROM the dropped column cannot
        // survive the drop -- re-emitting it produces a `CREATE TABLE` naming a
        // column that is not there, which fails the rebuild outright. The
        // pre-#387 behaviour for such a column is the right one: warn and drop.
        //
        // The same question, and the same answer, as a trigger whose body
        // mentions the dropped column (#336) -- so it asks the same function.
        if sql_mentions_identifier(definition, dropped) {
            return None;
        }

        // A line comment inside the definition is captured verbatim with it,
        // and the rebuilt body is joined onto one line -- so everything after
        // the comment, up to and including the closing parenthesis, would be
        // commented out. Refusing is the conservative direction and costs only
        // the preservation, not the migration.
        if contains_line_comment(definition) {
            return None;
        }

        out.push((name.clone(), definition.clone()));
    }
    Some(out)
}

/// Whether `sql` carries a `--` line comment outside a literal or a quoted
/// identifier.
///
/// A block comment is bounded and survives being joined onto one line; a line
/// comment swallows whatever follows it, which is why only this form matters.
#[cfg(feature = "native")]
fn contains_line_comment(sql: &str) -> bool {
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\'' | '"' | '`' => {
                while let Some(n) = chars.next() {
                    if n == c {
                        if chars.peek() == Some(&c) {
                            chars.next();
                            continue;
                        }
                        break;
                    }
                }
            }
            '[' => {
                for n in chars.by_ref() {
                    if n == ']' {
                        break;
                    }
                }
            }
            '-' if chars.peek() == Some(&'-') => return true,
            _ => {}
        }
    }
    false
}

/// The table an inline `REFERENCES` in a column definition points at.
///
/// Matching the carried key by its PARENT rather than by position, because
/// position is not a fact about the schema: `PRAGMA foreign_key_list` returns
/// keys in reverse declaration order, so a column carrying both an inline key
/// and a table-level one had the table-level row consumed and the inline one
/// emitted a second time. Two keys, both pointing at the wrong place, from a
/// guess about ordering.
///
/// The parent is the identifier following a bare `REFERENCES`. Quoted forms
/// count as the target but not as the keyword, so a column named `references`
/// cannot start a match.
#[cfg(feature = "native")]
fn inline_reference_target(definition: &str) -> Option<String> {
    let mut seen_keyword = false;
    let mut target = None;
    crate::sql_ddl::any_sql_identifier(definition, |quoted, token| {
        if seen_keyword {
            target = Some(token.to_string());
            return true;
        }
        if !quoted && token.eq_ignore_ascii_case("REFERENCES") {
            seen_keyword = true;
        }
        false
    });
    target
}

/// Whether a `CREATE TABLE` statement declares `AUTOINCREMENT`. The keyword is
/// only legal on the single `INTEGER PRIMARY KEY` rowid alias. It is matched as a
/// bare token *outside* string literals, quoted identifiers, and comments, so a
/// `DEFAULT 'autoincrement'`, a column named `autoincrement_flag`, or a
/// `/* autoincrement */` comment does not spuriously mark the table -- which would
/// wrongly synthesize a `sqlite_sequence` row and change schema semantics on a
/// drop-column rebuild.
#[cfg(feature = "native")]
fn sql_has_autoincrement(sql: &str) -> bool {
    crate::sql_ddl::any_sql_identifier(sql, |quoted, tok| {
        !quoted && tok.eq_ignore_ascii_case("AUTOINCREMENT")
    })
}

/// Whether `sql` mentions `ident` as an identifier -- bare (`NEW.email`,
/// `UPDATE OF email`) or quoted in any of SQLite's three forms (`"email"`,
/// `` `email` ``, `[email]`) -- ignoring string literals and comments.
/// ASCII-case-insensitive, as SQLite's own identifier comparison is.
///
/// Unlike [`sql_has_autoincrement`], which shares the same scan, a quoted
/// identifier counts here: `NEW."email"` is a reference, whereas a table named
/// `"autoincrement"` is not a declaration.
///
/// It is an over-approximation: a trigger body naming a *different* table's
/// `email` column, or using `email` as an alias, matches. That direction is
/// chosen. A false positive drops a trigger with a warning, which is
/// recoverable; a false negative replays a trigger whose body no longer
/// resolves, and every subsequent write to the table fails at prepare time.
#[cfg(feature = "native")]
fn sql_mentions_identifier(sql: &str, ident: &str) -> bool {
    crate::sql_ddl::any_sql_identifier(sql, |_quoted, tok| tok.eq_ignore_ascii_case(ident))
}

/// Surviving constructs the `DROP COLUMN` reconstruction cannot recover, as
/// human-readable labels for a warning (#273 MED#4).
///
/// `UNIQUE` is detected precisely from `PRAGMA index_list` (origin `'u'`
/// auto-indexes): a unique index all of whose columns survive is lost, while one
/// that references the dropped column goes away with the column anyway.
///
/// `CHECK` / `COLLATE` / `WITHOUT ROWID` are not exposed by any pragma either,
/// so they are found by scanning the original DDL -- but, since smugglr#462,
/// through `sql_ddl::top_level_items` rather than `up.contains("CHECK")` on
/// the whole statement. The old whole-statement scan over-reported: every
/// surviving *ordinary* column is carried into the rebuild **verbatim**
/// (#387, this module's own doc above), `CHECK`/`COLLATE` included, so a
/// `CHECK` declared on some other column that is not being dropped rode
/// through untouched and was warned about anyway. The new scan attributes
/// each occurrence to the top-level item that names it: the dropped column's
/// own clause (genuinely gone with the column), or a table-level constraint
/// (`CHECK (...)` outside any column, `CONSTRAINT ...`) -- which this rebuild
/// never re-emits at all, only `PRIMARY KEY` and `FOREIGN KEY` are
/// reconstructed, above -- so it stays an unconditional loss regardless of
/// which column it mentions. A `CHECK`/`COLLATE` on any other, ordinary,
/// surviving column is no longer reported. `WITHOUT ROWID` keeps its
/// unconditional-loss shape (this rebuild never recovers it, full stop,
/// `RebuildTarget::Fragments`'s `without_rowid: false` below) but is now read
/// from the tail after the column list rather than a substring of the whole
/// statement, so a comment or a quoted default mentioning the phrase can no
/// longer produce a false positive.
///
/// `dropped_triggers` are the triggers [`aux_ddl_surviving_drop`] decided it
/// cannot replay (their bodies mention the dropped column). They are named here
/// rather than warned at the replay site so the whole loss -- constraints and
/// triggers alike -- reaches the operator as one message (#336).
#[cfg(feature = "native")]
fn lost_constructs(
    conn: &Connection,
    table: &str,
    column: &str,
    orig_sql: Option<&str>,
    dropped_triggers: &[String],
) -> Result<Vec<String>, MigrateError> {
    let mut lost = Vec::new();
    for name in unique_constraint_indexes(conn, table)? {
        let cols = index_columns(conn, &name)?;
        if !cols.is_empty() && !cols.iter().any(|c| c == column) {
            lost.push(format!("UNIQUE({})", cols.join(", ")));
        }
    }
    // Generated columns, which the module doc has always listed as
    // unrecoverable and which nothing here detected until #342. Read from
    // `table_xinfo` because `table_info` -- the pragma this whole rebuild
    // introspects with -- does not return them at all, so they were never in
    // the `kept` set, never in the new table body, and never in the copy
    // projection. The loss was total and silent.
    //
    // The dropped column is excluded: a generated column that is itself being
    // dropped is going away on purpose and is not a loss to warn about.
    //
    // Compared case-insensitively because SQLite identifiers are, and the two
    // sides come from different places -- `name` from the pragma, which echoes
    // the declared spelling, and `column` from a manifest somebody wrote by
    // hand. A byte comparison warns that `"v"` was lost while dropping `"V"`.
    //
    // Unreachable through `apply` today, and that is a defect rather than a
    // reason to drop the guard: `drop_column`'s idempotence precheck reads
    // `PRAGMA table_info`, which cannot see a generated column, so asking to
    // drop one is a silent successful no-op that never reaches this function.
    // Same `table_info` blindness as #342 itself, one call earlier. Filed.
    for (name, storage) in generated_columns(conn, table)? {
        if !name.eq_ignore_ascii_case(column) {
            lost.push(format!("generated column {name:?} ({storage})"));
        }
    }
    for name in dropped_triggers {
        lost.push(format!("trigger {name:?} (its body references {column:?})"));
    }
    if let Some(sql) = orig_sql {
        if let Some(items) = crate::sql_ddl::top_level_items(sql) {
            // Table-level constraints start with one of these keywords
            // rather than a column name (`sql_ddl::top_level_items`'s own
            // doc): none of them are re-emitted by this rebuild (only
            // `PRIMARY KEY` and `FOREIGN KEY` are, both handled specially
            // above), so a `CHECK` living in one of these is always lost,
            // regardless of which column it names.
            const TABLE_LEVEL: [&str; 5] = ["PRIMARY", "UNIQUE", "CHECK", "FOREIGN", "CONSTRAINT"];
            let mut check_lost = false;
            let mut collate_lost = false;
            for (leading, text) in &items {
                // A CHECK/COLLATE on an ordinary column that is not the one
                // being dropped rides through verbatim (#387) and is not a
                // loss -- only the dropped column's own clause, or a
                // table-level constraint this rebuild never reconstructs at
                // all, still is.
                //
                // "Table-level" is decided from whether the item's *own*
                // leading token is quoted, not just from what
                // `leading_identifier` says it spells: a column named
                // `"check"` also spells `leading == "check"`, but it is a
                // column, and its own CHECK/COLLATE (if any) rides through
                // verbatim the same as any other survivor. Only a bare,
                // unquoted keyword starts an actual table-level constraint.
                let starts_quoted = matches!(text.as_bytes().first(), Some(b'"' | b'`' | b'['));
                let is_table_level = !starts_quoted
                    && TABLE_LEVEL
                        .iter()
                        .any(|kw| leading.eq_ignore_ascii_case(kw));
                let attributable = leading.eq_ignore_ascii_case(column) || is_table_level;
                if !attributable {
                    continue;
                }
                if crate::sql_ddl::any_sql_identifier(text, |quoted, tok| {
                    !quoted && tok.eq_ignore_ascii_case("CHECK")
                }) {
                    check_lost = true;
                }
                if crate::sql_ddl::any_sql_identifier(text, |quoted, tok| {
                    !quoted && tok.eq_ignore_ascii_case("COLLATE")
                }) {
                    collate_lost = true;
                }
            }
            if check_lost {
                lost.push("CHECK constraint(s)".to_string());
            }
            if collate_lost {
                lost.push("COLLATE clause(s)".to_string());
            }
        }
        if crate::sql_ddl::declares_without_rowid(sql) {
            lost.push("WITHOUT ROWID".to_string());
        }
        // MATCH is not in `foreign_key_list` at all -- the pragma reports NONE
        // even for a key declared `MATCH FULL`, measured rather than assumed --
        // so unlike the referential actions it cannot be reconstructed and is
        // warned about instead (#341).
        //
        // Found with the identifier scan rather than `up.contains("MATCH")`,
        // which the three above can afford and this one cannot: MATCH is a
        // common substring, and a column named `match_id` would warn on every
        // rebuild. The scan skips string literals and comments and compares
        // whole tokens, so a substring never matches and a quoted `"MATCH"`
        // column is read as the identifier it is.
        //
        // What it still warns on, named rather than left to be discovered: a
        // column named bare `match` (SQLite accepts it as an identifier), and
        // an FTS5 `MATCH` operator inside a CHECK. Both are whole bare tokens
        // and this scan cannot tell them from the FK clause. That is the
        // over-approximation direction the rest of this function already takes,
        // and the consequence is bounded -- a spurious `tracing::warn!` and no
        // change to any DDL. A silent drop is not bounded, which is why the
        // trade goes this way.
        if crate::sql_ddl::any_sql_identifier(sql, |quoted, token| {
            !quoted && token.eq_ignore_ascii_case("MATCH")
        }) {
            lost.push(
                "MATCH clause(s) (SQLite parses MATCH and ignores it, so this is a loss of \
                       declared text rather than of behaviour)"
                    .to_string(),
            );
        }
    }
    Ok(lost)
}

/// The generated columns of a table, as `(name, storage class)`.
///
/// `PRAGMA table_xinfo` rather than `table_info`, which is the whole point:
/// `table_info` **omits generated columns entirely**, so the rebuild's own
/// introspection cannot see them and a reader comparing `table_info` before and
/// after a rebuild sees no difference either. That is what made smugglr#342
/// leave no trace in the place anyone would look.
///
/// `hidden` is `2` for `VIRTUAL` and `3` for `STORED`. `1` is a virtual-table
/// hidden column, which is a different thing and is deliberately not collected.
///
/// This is exact rather than an over-approximation, unlike the keyword scans
/// [`lost_constructs`] uses for `CHECK` and `COLLATE`: it returns the column
/// names and their storage classes, so the warning can say which columns and
/// which kind, and a table with a column named `generated` cannot fool it.
///
/// What it does **not** give is the generation expression -- that is in no
/// pragma, only in `sqlite_master.sql`. So this makes the loss detectable and
/// does nothing toward making it recoverable.
///
/// `pub(super)` so [`reverse`](crate::migrate::reverse) shares it rather than
/// keeping its own copy. Both need the same answer for opposite reasons --
/// `reverse` to keep generated columns *out of a projection*, since writing to
/// one is an error, and this module to name them in a warning -- and two
/// private copies of "which `hidden` values mean generated" is one rule that
/// can drift into two.
#[cfg(feature = "native")]
pub(super) fn generated_columns(
    conn: &Connection,
    table: &str,
) -> Result<Vec<(String, &'static str)>, MigrateError> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_xinfo({})", quote_ident(table)))?;
    let rows = stmt
        .query_map([], |r| {
            // cols: cid, name, type, notnull, dflt_value, pk, hidden
            Ok((r.get::<_, String>(1)?, r.get::<_, i64>(6)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows
        .into_iter()
        .filter_map(|(name, hidden)| match hidden {
            2 => Some((name, "VIRTUAL")),
            3 => Some((name, "STORED")),
            _ => None,
        })
        .collect())
}

/// Every column name of a table in declaration order, generated ones included.
///
/// `table_info` omits generated columns, so it cannot answer "where was this
/// column declared" for them -- which is the question #387 needs in order to put
/// a preserved one back at its original index rather than at the end.
#[cfg(feature = "native")]
fn table_xinfo_names(conn: &Connection, table: &str) -> Result<Vec<String>, MigrateError> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_xinfo({})", quote_ident(table)))?;
    let names = stmt
        .query_map([], |r| r.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(names)
}

/// Names of the `UNIQUE`-constraint auto-indexes (origin `'u'`) on a table.
/// These are the column-level and table-level `UNIQUE` declarations, distinct
/// from primary-key (`'pk'`) and explicit `CREATE UNIQUE INDEX` (`'c'`) indexes.
#[cfg(feature = "native")]
fn unique_constraint_indexes(conn: &Connection, table: &str) -> Result<Vec<String>, MigrateError> {
    let mut stmt = conn.prepare(&format!("PRAGMA index_list({})", quote_ident(table)))?;
    let rows = stmt
        .query_map([], |r| {
            // cols: seq, name, unique, origin, partial
            Ok((r.get::<_, String>(1)?, r.get::<_, String>(3)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows
        .into_iter()
        .filter(|(_, origin)| origin == "u")
        .map(|(name, _)| name)
        .collect())
}

/// A reconstructed foreign key from `PRAGMA foreign_key_list`, possibly
/// composite (multiple `(from, to)` column pairs sharing one FK `id`).
#[cfg(feature = "native")]
struct FkInfo {
    parent_table: String,
    /// `(from, to)` column pairs in `seq` order. `to` is `None` when the FK
    /// references the parent's primary key implicitly (`REFERENCES parent`).
    cols: Vec<(String, Option<String>)>,
    /// The referential actions, as the pragma spells them, or `None` where it
    /// reported the `NO ACTION` default.
    ///
    /// Per-constraint rather than per-column: `foreign_key_list` repeats these
    /// on every row of a composite key, so they are captured once with the
    /// group rather than pushed into [`cols`](Self::cols) (#341).
    on_delete: Option<String>,
    on_update: Option<String>,
}

#[cfg(feature = "native")]
impl FkInfo {
    fn render(&self) -> String {
        let froms = self
            .cols
            .iter()
            .map(|(f, _)| quote_ident(f))
            .collect::<Vec<_>>()
            .join(", ");
        // Emit the parent column list only when every target column is known; an
        // implicit reference (any NULL `to`) targets the parent's primary key.
        if self.cols.iter().all(|(_, t)| t.is_some()) {
            let tos = self
                .cols
                .iter()
                .filter_map(|(_, t)| t.as_ref())
                .map(|t| quote_ident(t))
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "FOREIGN KEY ({}) REFERENCES {}({}){}",
                froms,
                quote_ident(&self.parent_table),
                tos,
                self.actions()
            )
        } else {
            format!(
                "FOREIGN KEY ({}) REFERENCES {}{}",
                froms,
                quote_ident(&self.parent_table),
                self.actions()
            )
        }
    }

    /// The `ON DELETE` / `ON UPDATE` clauses, or the empty string.
    ///
    /// The pragma's own spelling is passed through rather than parsed into an
    /// enum and re-rendered. The vocabulary here is SQLite's parser output, not
    /// user text, so pass-through makes all five actions correct by
    /// construction and cannot be outgrown by a future one.
    ///
    /// `ON DELETE` before `ON UPDATE` is arbitrary -- SQLite accepts either
    /// order -- and fixed so the emitted text is deterministic.
    fn actions(&self) -> String {
        let mut out = String::new();
        if let Some(action) = &self.on_delete {
            out.push_str(&format!(" ON DELETE {action}"));
        }
        if let Some(action) = &self.on_update {
            out.push_str(&format!(" ON UPDATE {action}"));
        }
        out
    }

    /// Whether any member column of this FK is `column` -- in which case the
    /// whole FK is dropped (a composite FK cannot survive losing a member).
    fn references_column(&self, column: &str) -> bool {
        self.cols.iter().any(|(f, _)| f == column)
    }
}

/// Reconstruct the foreign keys of a table, grouping the per-column rows of
/// `PRAGMA foreign_key_list` back into whole (possibly composite) constraints.
///
/// `foreign_key_list` returns one row *per column* of a composite FK, sharing an
/// `id` with an increasing `seq`; grouping by `id` and ordering members by `seq`
/// rebuilds one `FOREIGN KEY(a, b) REFERENCES p(x, y)` per constraint instead of
/// N independent single-column FKs (#273 HIGH#2).
#[cfg(feature = "native")]
fn reconstruct_foreign_keys(conn: &Connection, table: &str) -> Result<Vec<FkInfo>, MigrateError> {
    let mut stmt = conn.prepare(&format!("PRAGMA foreign_key_list({})", quote_ident(table)))?;
    let rows = stmt
        .query_map([], |r| {
            // cols: id, seq, table, from, to, on_update, on_delete, match
            Ok((
                r.get::<_, i64>(0)?,            // id
                r.get::<_, i64>(1)?,            // seq
                r.get::<_, String>(2)?,         // parent table
                r.get::<_, String>(3)?,         // from (local column)
                r.get::<_, Option<String>>(4)?, // to (parent column; NULL => PK)
                r.get::<_, String>(5)?,         // on_update
                r.get::<_, String>(6)?,         // on_delete
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    // Group by FK id, preserving a deterministic id order with a BTreeMap.
    use std::collections::BTreeMap;
    #[allow(clippy::type_complexity)]
    let mut groups: BTreeMap<
        i64,
        (
            String,
            Option<String>,
            Option<String>,
            Vec<(i64, String, Option<String>)>,
        ),
    > = BTreeMap::new();
    for (id, seq, parent_table, from, to, on_update, on_delete) in rows {
        groups
            .entry(id)
            // The actions belong to the constraint, and the pragma repeats them
            // on every member row of a composite key -- so they are taken from
            // whichever row arrives first and not re-read per column.
            .or_insert_with(|| {
                (
                    parent_table,
                    declared_action(on_delete),
                    declared_action(on_update),
                    Vec::new(),
                )
            })
            .3
            .push((seq, from, to));
    }

    let mut fks = Vec::with_capacity(groups.len());
    for (_, (parent_table, on_delete, on_update, mut members)) in groups {
        members.sort_by_key(|(seq, _, _)| *seq);
        let cols = members.into_iter().map(|(_, f, t)| (f, t)).collect();
        fks.push(FkInfo {
            parent_table,
            cols,
            on_delete,
            on_update,
        });
    }
    Ok(fks)
}

/// A referential action worth emitting, or `None` for the default.
///
/// `foreign_key_list` reports `NO ACTION` where the key declared nothing, so
/// emitting it verbatim would rewrite every actionless key's DDL for no change
/// in meaning. Suppressing it keeps a key that never had an action rendering
/// byte-identically to before #341.
#[cfg(feature = "native")]
fn declared_action(action: String) -> Option<String> {
    (!action.eq_ignore_ascii_case("NO ACTION")).then_some(action)
}

/// Names of explicit (`CREATE INDEX`, origin `'c'`) indexes on `table` that
/// reference `column`.
#[cfg(feature = "native")]
fn explicit_indexes_referencing(
    conn: &Connection,
    table: &str,
    column: &str,
) -> Result<Vec<String>, MigrateError> {
    let mut out = Vec::new();
    for obj in aux_objects(conn, table)? {
        if obj.kind == AuxKind::Index && index_columns(conn, &obj.name)?.iter().any(|c| c == column)
        {
            out.push(obj.name);
        }
    }
    Ok(out)
}

/// The `sqlite_master` type of an auxiliary object attached to a table.
#[cfg(feature = "native")]
#[derive(PartialEq, Eq)]
enum AuxKind {
    /// An explicit `CREATE INDEX` (auto-indexes have no DDL and are excluded).
    Index,
    /// A `CREATE TRIGGER` fired on the table.
    Trigger,
}

/// One explicit index or trigger attached to a table, with its verbatim DDL.
#[cfg(feature = "native")]
struct AuxObject {
    kind: AuxKind,
    name: String,
    sql: String,
}

/// The auxiliary-object DDL that a rebuild dropping `column` can replay, plus
/// the triggers it cannot.
#[cfg(feature = "native")]
struct AuxReplay {
    /// Verbatim DDL to replay after the swap, indexes before triggers.
    ddl: Vec<String>,
    /// Names of triggers deliberately not replayed because their body mentions
    /// `column`. The caller warns on these via [`lost_constructs`].
    dropped_triggers: Vec<String>,
}

/// Split the explicit indexes and triggers on `table` into the DDL a rebuild
/// dropping `column` replays and the triggers it must abandon.
///
/// An index is carried when `PRAGMA index_info` shows it does not index the
/// dropped column; a trigger is carried when its DDL does not mention the
/// dropped column as an identifier ([`sql_mentions_identifier`]).
///
/// A trigger that *does* mention the column cannot be carried: SQLite resolves a
/// trigger body when the triggering statement is prepared, not at
/// `CREATE TRIGGER` (see `create_trigger_does_not_resolve_body_columns`), so
/// replaying it would succeed and then fail every subsequent write to the table
/// with `no such column`. Abandoning it -- loudly -- keeps the table writable.
#[cfg(feature = "native")]
fn aux_ddl_surviving_drop(
    conn: &Connection,
    table: &str,
    column: &str,
) -> Result<AuxReplay, MigrateError> {
    let mut replay = AuxReplay {
        ddl: Vec::new(),
        dropped_triggers: Vec::new(),
    };
    for obj in aux_objects(conn, table)? {
        match obj.kind {
            AuxKind::Index => {
                if !index_columns(conn, &obj.name)?.iter().any(|c| c == column) {
                    replay.ddl.push(obj.sql);
                }
            }
            AuxKind::Trigger => {
                if sql_mentions_identifier(&obj.sql, column) {
                    replay.dropped_triggers.push(obj.name);
                } else {
                    replay.ddl.push(obj.sql);
                }
            }
        }
    }
    Ok(replay)
}

/// Every explicit index and trigger attached to a table, in `type, name` order
/// (so indexes are replayed before triggers).
///
/// `sql IS NOT NULL` excludes the auto-indexes SQLite synthesizes for
/// `PRIMARY KEY` / `UNIQUE`: they carry no DDL and reappear with the rebuilt
/// table's own constraints. What remains is exactly the set a `DROP TABLE` of
/// the original destroys -- which is why triggers belong here and not only in
/// reverse's `aux_object_ddl` (#274), whose query this mirrors.
#[cfg(feature = "native")]
fn aux_objects(conn: &Connection, table: &str) -> Result<Vec<AuxObject>, MigrateError> {
    let mut stmt = conn.prepare(
        "SELECT type, name, sql FROM sqlite_master \
         WHERE type IN ('index', 'trigger') AND tbl_name = ?1 AND sql IS NOT NULL \
         ORDER BY type, name",
    )?;
    let rows = stmt
        .query_map(params![table], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows
        .into_iter()
        .map(|(kind, name, sql)| AuxObject {
            kind: if kind == "trigger" {
                AuxKind::Trigger
            } else {
                AuxKind::Index
            },
            name,
            sql,
        })
        .collect())
}

/// Column names indexed by a named index.
#[cfg(feature = "native")]
fn index_columns(conn: &Connection, index: &str) -> Result<Vec<String>, MigrateError> {
    let mut stmt = conn.prepare(&format!("PRAGMA index_info({})", quote_ident(index)))?;
    let cols = stmt
        .query_map([], |r| r.get::<_, Option<String>>(2))?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect();
    Ok(cols)
}

/// Whether a user table exists.
#[cfg(feature = "native")]
fn table_exists(conn: &Connection, table: &str) -> Result<bool, MigrateError> {
    let n: i64 = conn.query_row(
        "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
        params![table],
        |r| r.get(0),
    )?;
    Ok(n > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrate::ClassifiedOp;

    fn col(name: &str, kind: ColumnKind) -> Column {
        Column {
            name: name.to_string(),
            kind,
            constraints: Vec::new(),
            tags: Vec::new(),
        }
    }

    // -- Pure builders ------------------------------------------------------

    #[test]
    fn quote_ident_escapes_embedded_quotes() {
        assert_eq!(quote_ident("plain"), "\"plain\"");
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn column_def_renders_kind_and_constraints_in_order() {
        let c = Column {
            name: "id".into(),
            kind: ColumnKind::Blob,
            constraints: vec![
                Constraint::Pk,
                Constraint::NotNull,
                Constraint::Default("x'00'".into()),
                Constraint::Check("length(id) = 16".into()),
                Constraint::Fk {
                    table: "users".into(),
                    col: "id".into(),
                },
                Constraint::Unique,
            ],
            tags: vec![],
        };
        assert_eq!(
            render_column_def(&c),
            "\"id\" BLOB PRIMARY KEY NOT NULL DEFAULT x'00' \
             CHECK (length(id) = 16) REFERENCES \"users\"(\"id\") UNIQUE"
        );
    }

    #[test]
    fn create_table_uses_if_not_exists_and_without_rowid() {
        let sql = statement_for(&Op::CreateTable {
            table: "t".into(),
            columns: vec![col("id", ColumnKind::Text), col("n", ColumnKind::Int)],
            without_rowid: true,
        });
        assert_eq!(
            sql,
            "CREATE TABLE IF NOT EXISTS \"t\" (\"id\" TEXT, \"n\" INTEGER) WITHOUT ROWID"
        );
    }

    #[test]
    fn create_index_renders_unique_and_columns() {
        let sql = statement_for(&Op::CreateIndex {
            name: "idx_t_a".into(),
            table: "t".into(),
            columns: vec!["a".into(), "b".into()],
            unique: true,
        });
        assert_eq!(
            sql,
            "CREATE UNIQUE INDEX IF NOT EXISTS \"idx_t_a\" ON \"t\" (\"a\", \"b\")"
        );
    }

    // -- rowid_alias_findings (#427) -----------------------------------------

    #[test]
    fn finds_a_bare_integer_primary_key() {
        let mut id = col("id", ColumnKind::Int);
        id.constraints.push(Constraint::Pk);
        let ops = vec![ClassifiedOp::new(Op::CreateTable {
            table: "things".into(),
            columns: vec![id, col("name", ColumnKind::Text)],
            without_rowid: false,
        })];
        let findings = rowid_alias_findings(&ops);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].table, "things");
        assert!(
            findings[0].message.contains("INTEGER PRIMARY KEY")
                || findings[0].message.contains("rowid"),
            "must name the rowid-alias shape: {}",
            findings[0].message
        );
        assert!(
            findings[0].message.contains("UUIDv7"),
            "must carry the remedy: {}",
            findings[0].message
        );
    }

    #[test]
    fn finds_nothing_for_a_text_primary_key() {
        let mut id = col("id", ColumnKind::Text);
        id.constraints.push(Constraint::Pk);
        id.constraints.push(Constraint::NotNull);
        let ops = vec![ClassifiedOp::new(Op::CreateTable {
            table: "things".into(),
            columns: vec![id, col("name", ColumnKind::Text)],
            without_rowid: false,
        })];
        assert!(rowid_alias_findings(&ops).is_empty());
    }

    #[test]
    fn ignores_non_create_table_ops() {
        // A CreateIndex or AddColumn op carries no table shape to classify --
        // must not panic or misfire on them.
        let ops = vec![
            ClassifiedOp::new(Op::CreateIndex {
                name: "idx_t_a".into(),
                table: "t".into(),
                columns: vec!["a".into()],
                unique: false,
            }),
            ClassifiedOp::new(Op::AddColumn {
                table: "t".into(),
                column: col("nickname", ColumnKind::Int),
            }),
        ];
        assert!(rowid_alias_findings(&ops).is_empty());
    }

    // -- Remote generators (pure, no transport) -----------------------------

    fn ops() -> Vec<ClassifiedOp> {
        vec![
            ClassifiedOp::new(Op::CreateTable {
                table: "t".into(),
                columns: vec![col("id", ColumnKind::Text)],
                without_rowid: false,
            }),
            ClassifiedOp::new(Op::AddColumn {
                table: "t".into(),
                column: col("name", ColumnKind::Text),
            }),
        ]
    }

    #[test]
    fn d1_prepends_defer_foreign_keys() {
        let s = d1_statements(&ops());
        assert_eq!(s[0], "PRAGMA defer_foreign_keys = ON");
        assert_eq!(s.len(), 3);
        assert!(s[1].starts_with("CREATE TABLE IF NOT EXISTS \"t\""));
        assert_eq!(s[2], "ALTER TABLE \"t\" ADD COLUMN \"name\" TEXT");
    }

    #[test]
    fn turso_is_direct_statements() {
        let s = turso_statements(&ops());
        assert_eq!(s.len(), 2);
        assert!(s[0].starts_with("CREATE TABLE IF NOT EXISTS \"t\""));
        assert_eq!(s[1], "ALTER TABLE \"t\" ADD COLUMN \"name\" TEXT");
    }

    #[test]
    fn rqlite_wraps_in_begin_commit() {
        let s = rqlite_statements(&ops());
        assert_eq!(s.first().map(String::as_str), Some("BEGIN"));
        assert_eq!(s.last().map(String::as_str), Some("COMMIT"));
        assert_eq!(s.len(), 4);
    }

    #[test]
    fn remote_execute_is_unsupported_but_names_the_target() {
        let err = apply_remote(RemoteTarget::Turso, &ops()).unwrap_err();
        match err {
            MigrateError::RemoteTransportUnsupported(t) => assert_eq!(t, "turso"),
            other => panic!("expected RemoteTransportUnsupported, got {other:?}"),
        }
    }

    // -- Native local apply -------------------------------------------------
    #[cfg(feature = "native")]
    mod native {
        use super::*;

        fn noop(_: &ClassifiedOp) -> Result<(), MigrateError> {
            Ok(())
        }

        fn apply(conn: &Connection, op: Op) -> Result<(), MigrateError> {
            apply_ops(conn, &[ClassifiedOp::new(op)], &mut noop)
        }

        fn mem() -> Connection {
            Connection::open_in_memory().unwrap()
        }

        fn columns(conn: &Connection, table: &str) -> Vec<String> {
            raw_table_columns(conn, table).unwrap()
        }

        /// The verbatim `CREATE TABLE` sqlite stored for `table`.
        fn stored_ddl(conn: &Connection, table: &str) -> String {
            conn.query_row(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?1",
                params![table],
                |r| r.get(0),
            )
            .unwrap()
        }

        #[test]
        fn splice_retargets_real_sqlite_master_ddl_verbatim() {
            // The splice input that matters is the DDL sqlite ACTUALLY stores, not
            // hand-written DDL. Round-trip several real forms: the spliced create
            // must execute, land under the temp name, and preserve constraints.
            let cases = [
                // Quoted name (with a space) + WITHOUT ROWID + inline UNIQUE + COLLATE.
                ("quo ted", "CREATE TABLE \"quo ted\" (id TEXT PRIMARY KEY, h TEXT UNIQUE COLLATE NOCASE) WITHOUT ROWID"),
                // Bare name + table-level CHECK.
                ("bare", "CREATE TABLE bare (id TEXT PRIMARY KEY, score INTEGER, CHECK (score >= 0))"),
                // Composite primary key.
                ("comp", "CREATE TABLE comp (a TEXT, b TEXT, v TEXT, PRIMARY KEY (a, b))"),
                // Foreign key to another table.
                ("child", "CREATE TABLE child (id TEXT PRIMARY KEY, pid TEXT REFERENCES bare(id))"),
                // Generated (stored) column.
                ("gen", "CREATE TABLE gen (id TEXT PRIMARY KEY, n INTEGER, n2 INTEGER GENERATED ALWAYS AS (n * 2) STORED)"),
            ];
            for (name, original) in cases {
                let conn = mem();
                // `child` references `bare`; create the parent first so the FK case
                // has a target.
                conn.execute_batch(
                    "CREATE TABLE bare (id TEXT PRIMARY KEY, score INTEGER, CHECK (score >= 0))",
                )
                .ok();
                if name != "bare" {
                    conn.execute_batch(original).unwrap();
                }
                let stored = stored_ddl(&conn, name);
                let spliced = splice_create_table_name(&stored, REBUILD_TMP).unwrap();
                // The spliced DDL executes and creates the temp table.
                conn.execute_batch(&format!(
                    "DROP TABLE IF EXISTS {}",
                    quote_ident(REBUILD_TMP)
                ))
                .unwrap();
                conn.execute_batch(&spliced).unwrap();
                let tmp_ddl = stored_ddl(&conn, REBUILD_TMP);
                // The temp DDL is the original body with only the name retargeted:
                // everything after the spliced name is byte-identical.
                let orig_after = &stored[stored.find('(').unwrap()..];
                let tmp_after = &tmp_ddl[tmp_ddl.find('(').unwrap()..];
                assert_eq!(orig_after, tmp_after, "body must be preserved verbatim");
                assert!(
                    tmp_ddl.contains(REBUILD_TMP),
                    "temp name spliced: {tmp_ddl}"
                );
            }
        }

        #[test]
        fn splice_rejects_non_create_table() {
            assert!(splice_create_table_name("SELECT 1", REBUILD_TMP).is_err());
            assert!(splice_create_table_name("CREATE INDEX i ON t(a)", REBUILD_TMP).is_err());
        }

        #[test]
        fn pre_op_hook_runs_before_each_op_and_can_abort() {
            let conn = mem();
            let mut seen = Vec::new();
            let ops = vec![
                ClassifiedOp::new(Op::CreateTable {
                    table: "a".into(),
                    columns: vec![col("id", ColumnKind::Text)],
                    without_rowid: false,
                }),
                ClassifiedOp::new(Op::CreateTable {
                    table: "b".into(),
                    columns: vec![col("id", ColumnKind::Text)],
                    without_rowid: false,
                }),
            ];
            let mut hook = |c: &ClassifiedOp| -> Result<(), MigrateError> {
                seen.push(c.op.clone());
                Ok(())
            };
            apply_ops(&conn, &ops, &mut hook).unwrap();
            assert_eq!(seen.len(), 2);
            assert!(table_exists(&conn, "a").unwrap());
            assert!(table_exists(&conn, "b").unwrap());
        }

        #[test]
        fn pre_op_abort_stops_before_mutation() {
            let conn = mem();
            let ops = vec![ClassifiedOp::new(Op::CreateTable {
                table: "a".into(),
                columns: vec![col("id", ColumnKind::Text)],
                without_rowid: false,
            })];
            let mut hook = |_: &ClassifiedOp| -> Result<(), MigrateError> {
                Err(MigrateError::Apply("stop".into()))
            };
            let err = apply_ops(&conn, &ops, &mut hook).unwrap_err();
            assert!(matches!(err, MigrateError::Apply(_)));
            // The op never ran.
            assert!(!table_exists(&conn, "a").unwrap());
        }

        #[test]
        fn create_table_is_idempotent() {
            let conn = mem();
            let op = || Op::CreateTable {
                table: "users".into(),
                columns: vec![col("id", ColumnKind::Text), col("email", ColumnKind::Text)],
                without_rowid: false,
            };
            apply(&conn, op()).unwrap();
            apply(&conn, op()).unwrap(); // re-run: no error
            assert_eq!(columns(&conn, "users"), vec!["id", "email"]);
        }

        #[test]
        fn create_index_is_idempotent() {
            let conn = mem();
            apply(
                &conn,
                Op::CreateTable {
                    table: "t".into(),
                    columns: vec![col("a", ColumnKind::Int)],
                    without_rowid: false,
                },
            )
            .unwrap();
            let idx = || Op::CreateIndex {
                name: "idx_t_a".into(),
                table: "t".into(),
                columns: vec!["a".into()],
                unique: false,
            };
            apply(&conn, idx()).unwrap();
            apply(&conn, idx()).unwrap(); // re-run: no error
        }

        #[test]
        fn add_column_is_idempotent() {
            let conn = mem();
            apply(
                &conn,
                Op::CreateTable {
                    table: "t".into(),
                    columns: vec![col("id", ColumnKind::Text)],
                    without_rowid: false,
                },
            )
            .unwrap();
            let add = || Op::AddColumn {
                table: "t".into(),
                column: col("email", ColumnKind::Text),
            };
            apply(&conn, add()).unwrap();
            apply(&conn, add()).unwrap(); // re-run: no "duplicate column name"
            assert_eq!(columns(&conn, "t"), vec!["id", "email"]);
        }

        #[test]
        fn drop_column_on_indexed_column_drops_the_index_first() {
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE t (id TEXT PRIMARY KEY, code TEXT, name TEXT);
                 CREATE INDEX idx_code ON t(code);
                 INSERT INTO t VALUES ('a', 'c1', 'n1'), ('b', 'c2', 'n2');",
            )
            .unwrap();
            apply(
                &conn,
                Op::DropColumn {
                    table: "t".into(),
                    column: "code".into(),
                },
            )
            .unwrap();
            assert_eq!(columns(&conn, "t"), vec!["id", "name"]);
            // The referencing index is gone.
            let idx: i64 = conn
                .query_row(
                    "SELECT count(*) FROM sqlite_master WHERE type='index' AND name='idx_code'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(idx, 0);
            // Rows preserved.
            let n: i64 = conn
                .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
                .unwrap();
            assert_eq!(n, 2);
        }

        #[test]
        fn drop_column_is_idempotent_when_absent() {
            let conn = mem();
            conn.execute_batch("CREATE TABLE t (id TEXT PRIMARY KEY, name TEXT);")
                .unwrap();
            let drop = || Op::DropColumn {
                table: "t".into(),
                column: "name".into(),
            };
            apply(&conn, drop()).unwrap();
            apply(&conn, drop()).unwrap(); // already gone: no-op
            assert_eq!(columns(&conn, "t"), vec!["id"]);
        }

        /// A user table at the rebuild's scratch name survives, and the
        /// migration refuses instead of destroying it (#338).
        ///
        /// The rebuild opened with `DROP TABLE IF EXISTS _smugglr_rebuild_tmp`.
        /// A table an operator happened to name that was destroyed silently,
        /// inside a migration that reported success, with nothing afterwards to
        /// tell them -- the table was simply gone.
        ///
        /// Asserts the ROW survives, not just the table. A refusal that left an
        /// empty table behind would pass a table-exists check and still have
        /// eaten the data.
        #[test]
        fn a_user_table_at_the_rebuild_temp_name_is_not_destroyed() {
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, code TEXT UNIQUE, name TEXT);
                 INSERT INTO t VALUES (1, 'c1', 'n1');
                 CREATE TABLE _smugglr_rebuild_tmp (keep TEXT);
                 INSERT INTO _smugglr_rebuild_tmp VALUES ('irreplaceable');",
            )
            .unwrap();

            // Dropping a UNIQUE column is the path that falls back to a rebuild.
            let err = apply(
                &conn,
                Op::DropColumn {
                    table: "t".into(),
                    column: "code".into(),
                },
            )
            .expect_err("the rebuild must refuse rather than drop a table it did not create");

            let said = err.to_string();
            assert!(
                said.contains(REBUILD_TMP),
                "the refusal has to name the table in the way; it said {said:?}"
            );

            let survived: String = conn
                .query_row("SELECT keep FROM _smugglr_rebuild_tmp", [], |r| r.get(0))
                .expect("the user's table and its row are still there");
            assert_eq!(survived, "irreplaceable");
        }

        #[test]
        fn drop_unique_column_falls_back_to_rebuild() {
            // A UNIQUE column cannot be ALTER-dropped (its auto-index is not an
            // explicit CREATE INDEX), so this exercises the rebuild fallback --
            // the non-test caller of `rebuild_table`.
            let conn = mem();
            conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
            conn.execute_batch(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, code TEXT UNIQUE, name TEXT);
                 INSERT INTO t VALUES (1, 'c1', 'n1'), (2, 'c2', 'n2');",
            )
            .unwrap();
            apply(
                &conn,
                Op::DropColumn {
                    table: "t".into(),
                    column: "code".into(),
                },
            )
            .unwrap();
            assert_eq!(columns(&conn, "t"), vec!["id", "name"]);
            let names: Vec<String> = {
                let mut stmt = conn.prepare("SELECT name FROM t ORDER BY id").unwrap();
                stmt.query_map([], |r| r.get::<_, String>(0))
                    .unwrap()
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .unwrap()
            };
            assert_eq!(names, vec!["n1", "n2"]);
            // The caller's prior foreign-key state (ON) was restored after the
            // rebuild -- not merely forced ON.
            let fk: i64 = conn
                .query_row("PRAGMA foreign_keys", [], |r| r.get(0))
                .unwrap();
            assert_eq!(fk, 1);
        }

        #[test]
        fn rebuild_restores_prior_foreign_keys_off() {
            // #273 LOW: a caller running with enforcement OFF must not silently
            // gain it from a DROP COLUMN rebuild. (rusqlite defaults foreign_keys
            // ON, so set it OFF explicitly to model that caller.)
            let conn = mem();
            conn.execute_batch("PRAGMA foreign_keys = OFF;").unwrap();
            let before: i64 = conn
                .query_row("PRAGMA foreign_keys", [], |r| r.get(0))
                .unwrap();
            assert_eq!(before, 0, "enforcement is OFF before the rebuild");
            conn.execute_batch(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, code TEXT UNIQUE, name TEXT);
                 INSERT INTO t VALUES (1, 'c1', 'n1');",
            )
            .unwrap();
            apply(
                &conn,
                Op::DropColumn {
                    table: "t".into(),
                    column: "code".into(),
                },
            )
            .unwrap();
            let after: i64 = conn
                .query_row("PRAGMA foreign_keys", [], |r| r.get(0))
                .unwrap();
            assert_eq!(after, 0, "prior OFF state restored, not forced ON");
        }

        #[test]
        fn drop_column_member_of_composite_pk_keeps_key() {
            // #273 HIGH#1, through the PUBLIC apply_ops path: dropping one member
            // of PRIMARY KEY(a, b) must leave a real key on the survivor, not
            // silently discard the whole PK. `b` is a PK member, so the direct
            // ALTER is refused and the rebuild runs. NOT NULL columns make the
            // NULL-key rejection faithful (a plain PK column allows NULL).
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE t (a TEXT NOT NULL, b TEXT NOT NULL, c TEXT, PRIMARY KEY(a, b));
                 INSERT INTO t VALUES ('a1','b1','x'), ('a2','b2','y');",
            )
            .unwrap();
            apply(
                &conn,
                Op::DropColumn {
                    table: "t".into(),
                    column: "b".into(),
                },
            )
            .unwrap();
            assert_eq!(columns(&conn, "t"), vec!["a", "c"]);

            // The rebuilt table still has a primary key (the bug produced none).
            let pk_cols: i64 = conn
                .query_row(
                    "SELECT count(*) FROM pragma_table_info('t') WHERE pk > 0",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert!(
                pk_cols > 0,
                "surviving PK must exist, not be silently dropped"
            );

            // ...and it is enforced: a duplicate key value is rejected.
            let dup = conn.execute("INSERT INTO t (a, c) VALUES ('a1', 'z')", []);
            assert!(dup.is_err(), "duplicate primary key must be rejected");
            // A NULL key is rejected too (NOT NULL survived the rebuild).
            let null_key = conn.execute("INSERT INTO t (a, c) VALUES (NULL, 'w')", []);
            assert!(null_key.is_err(), "NULL primary key must be rejected");
        }

        #[test]
        fn drop_column_preserves_composite_fk() {
            // #273 HIGH#2, through the PUBLIC apply_ops path: a composite FK
            // (one foreign_key_list row per column) must be reconstructed as ONE
            // FOREIGN KEY(a, b) REFERENCES p(x, y), or the rebuild fails with
            // "foreign key mismatch" / installs wrong single-column FKs. `code`
            // is UNIQUE, forcing the rebuild.
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE p (x, y, PRIMARY KEY(x, y));
                 CREATE TABLE c (
                     id INTEGER PRIMARY KEY,
                     a, b,
                     code TEXT UNIQUE,
                     FOREIGN KEY(a, b) REFERENCES p(x, y)
                 );
                 INSERT INTO p VALUES (1, 1), (2, 2);
                 INSERT INTO c (id, a, b, code) VALUES (10, 1, 1, 'k1'), (20, 2, 2, 'k2');",
            )
            .unwrap();

            // The op must SUCCEED (the mis-reconstruction made it fail).
            apply(
                &conn,
                Op::DropColumn {
                    table: "c".into(),
                    column: "code".into(),
                },
            )
            .unwrap();
            assert_eq!(columns(&conn, "c"), vec!["id", "a", "b"]);

            // Exactly one composite FK survived, intact: two member rows sharing
            // one id, referencing p.
            let fk_rows: Vec<(i64, String, Option<String>, Option<String>)> = {
                let mut stmt = conn
                    .prepare("SELECT id, \"table\", \"from\", \"to\" FROM pragma_foreign_key_list('c') ORDER BY id, seq")
                    .unwrap();
                stmt.query_map([], |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, Option<String>>(2)?,
                        r.get::<_, Option<String>>(3)?,
                    ))
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
            };
            assert_eq!(fk_rows.len(), 2, "composite FK is two member rows");
            assert_eq!(fk_rows[0].0, fk_rows[1].0, "members share one FK id");
            assert!(fk_rows.iter().all(|r| r.1 == "p"), "both reference p");

            // Referential integrity holds under enforcement.
            let violations = foreign_key_violations(&conn, "c").unwrap();
            assert!(violations.is_empty(), "composite FK intact: {violations:?}");
        }

        /// #341: the actions come back, and the assertion is the behaviour
        /// rather than the reconstructed DDL text.
        ///
        /// A key can be present and inert -- that is exactly what the defect
        /// produced -- so `foreign_key_list` reporting a row proves nothing.
        /// Both directions are exercised on their own parent, so neither
        /// assertion can be satisfied by the other's side effects.
        #[test]
        fn drop_column_rebuild_preserves_referential_actions() {
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE keeper (id INTEGER PRIMARY KEY);
                 CREATE TABLE mover (id INTEGER PRIMARY KEY);
                 CREATE TABLE cascade_child (
                     id INTEGER PRIMARY KEY,
                     keeper_id INTEGER REFERENCES keeper(id) ON DELETE CASCADE,
                     code TEXT UNIQUE
                 );
                 CREATE TABLE update_child (
                     id INTEGER PRIMARY KEY,
                     mover_id INTEGER REFERENCES mover(id) ON UPDATE CASCADE,
                     code TEXT UNIQUE
                 );
                 INSERT INTO keeper (id) VALUES (1);
                 INSERT INTO mover (id) VALUES (5);
                 INSERT INTO cascade_child (id, keeper_id, code) VALUES (10, 1, 'k1');
                 INSERT INTO update_child (id, mover_id, code) VALUES (20, 5, 'k2');",
            )
            .unwrap();

            // `code` is UNIQUE on both, so each drop takes the rebuild path.
            for table in ["cascade_child", "update_child"] {
                apply(
                    &conn,
                    Op::DropColumn {
                        table: table.into(),
                        column: "code".into(),
                    },
                )
                .unwrap();
            }

            conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();

            // ON DELETE CASCADE: the delete is permitted and takes the child.
            conn.execute("DELETE FROM keeper WHERE id = 1", []).expect(
                "a child declared ON DELETE CASCADE does not refuse its parent's delete; a \
                 refusal means the action came back as the NO ACTION default",
            );
            let orphans: i64 = conn
                .query_row(
                    "SELECT count(*) FROM cascade_child WHERE keeper_id = 1",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(orphans, 0, "ON DELETE CASCADE did not cascade");

            // ON UPDATE CASCADE: the key moves and the child follows.
            conn.execute("UPDATE mover SET id = 6 WHERE id = 5", [])
                .expect("a child declared ON UPDATE CASCADE does not refuse its parent's update");
            let followed: i64 = conn
                .query_row(
                    "SELECT count(*) FROM update_child WHERE mover_id = 6",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(
                followed, 1,
                "ON UPDATE CASCADE did not carry the child over"
            );
        }

        /// #341 on a composite key, which is where a per-column read of the
        /// action would go wrong invisibly.
        ///
        /// `foreign_key_list` repeats `on_delete` on every member row of a
        /// composite FK, so capturing it per column instead of per constraint
        /// can still look right on a single-column key. `drop_column_preserves_
        /// composite_fk` covers composites and declares no action, and the test
        /// above covers actions and declares no composite -- neither reaches
        /// this square.
        #[test]
        fn drop_column_rebuild_preserves_an_action_on_a_composite_key() {
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE p (x, y, PRIMARY KEY(x, y));
                 CREATE TABLE c (
                     id INTEGER PRIMARY KEY,
                     a, b,
                     code TEXT UNIQUE,
                     FOREIGN KEY(a, b) REFERENCES p(x, y) ON DELETE CASCADE
                 );
                 INSERT INTO p VALUES (1, 1), (2, 2);
                 INSERT INTO c (id, a, b, code) VALUES (10, 1, 1, 'k1'), (20, 2, 2, 'k2');",
            )
            .unwrap();

            apply(
                &conn,
                Op::DropColumn {
                    table: "c".into(),
                    column: "code".into(),
                },
            )
            .unwrap();

            // Still one composite key, not two single-column ones -- the #273
            // property this must not regress while gaining the action.
            let members: i64 = conn
                .query_row(
                    "SELECT count(*) FROM pragma_foreign_key_list('c')",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            let ids: i64 = conn
                .query_row(
                    "SELECT count(DISTINCT id) FROM pragma_foreign_key_list('c')",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!((members, ids), (2, 1), "one composite key of two members");

            conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
            conn.execute("DELETE FROM p WHERE x = 1 AND y = 1", [])
                .expect("the composite key declared ON DELETE CASCADE and must not refuse");
            let left: i64 = conn
                .query_row("SELECT count(*) FROM c WHERE a = 1 AND b = 1", [], |r| {
                    r.get(0)
                })
                .unwrap();
            assert_eq!(left, 0, "the composite key's CASCADE did not cascade");
        }

        /// A key that never declared an action does not acquire one.
        ///
        /// `foreign_key_list` reports `NO ACTION` for such a key, and rendering
        /// that verbatim would rewrite the DDL of every actionless key in the
        /// database for no change in meaning -- a diff that reads as a
        /// behavioural change and is not.
        #[test]
        fn drop_column_rebuild_does_not_invent_a_referential_action() {
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE parent (id INTEGER PRIMARY KEY);
                 CREATE TABLE child (
                     id INTEGER PRIMARY KEY,
                     parent_id INTEGER REFERENCES parent(id),
                     code TEXT UNIQUE
                 );
                 INSERT INTO parent (id) VALUES (1);
                 INSERT INTO child (id, parent_id, code) VALUES (10, 1, 'k1');",
            )
            .unwrap();

            apply(
                &conn,
                Op::DropColumn {
                    table: "child".into(),
                    column: "code".into(),
                },
            )
            .unwrap();

            let sql: String = conn
                .query_row(
                    "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'child'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            let upper = sql.to_ascii_uppercase();
            assert!(
                !upper.contains("ON DELETE") && !upper.contains("ON UPDATE"),
                "the rebuild wrote an action onto a key that declared none: {sql}"
            );
        }

        /// #342: both storage classes of generated column are named in the
        /// warned losses, having previously been invisible to everything.
        ///
        /// The rebuild introspects with `PRAGMA table_info`, which omits
        /// generated columns entirely, so they never entered the kept set and
        /// never reached the new table -- and a reader comparing `table_info`
        /// before and after saw no difference, because they were never in it.
        /// The module doc promised a warning for exactly this construct and it
        /// was the one construct in that list nothing detected.
        #[test]
        fn a_generated_column_is_named_in_the_warned_losses() {
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE t (
                     id INTEGER PRIMARY KEY,
                     n INTEGER,
                     v INTEGER GENERATED ALWAYS AS (n * 2) VIRTUAL,
                     s INTEGER GENERATED ALWAYS AS (n * 3) STORED,
                     code TEXT UNIQUE
                 );",
            )
            .unwrap();
            let sql = table_sql(&conn, "t").unwrap();
            let lost = lost_constructs(&conn, "t", "code", sql.as_deref(), &[]).unwrap();

            // Both classes, each named, each saying which kind it was. A
            // warning that said only "generated column(s)" would leave the
            // operator to work out which of the two lost a computation and
            // which lost stored values as well.
            assert!(
                lost.iter()
                    .any(|l| l.contains("\"v\"") && l.contains("VIRTUAL")),
                "the VIRTUAL generated column has to be named: {lost:?}"
            );
            assert!(
                lost.iter()
                    .any(|l| l.contains("\"s\"") && l.contains("STORED")),
                "the STORED generated column has to be named: {lost:?}"
            );
        }

        /// The column being dropped is not reported as a loss, in either
        /// casing.
        ///
        /// Dropping a generated column on purpose is the operator's request,
        /// not a construct the rebuild failed to carry, and warning about it
        /// would train them to ignore the warning. The casing half is not
        /// decoration: `name` comes from the pragma and `column` from a
        /// hand-written manifest, SQLite identifiers are case-insensitive, and
        /// a byte comparison warns that `"v"` was lost while dropping `"V"`.
        ///
        /// This calls `lost_constructs` directly rather than going through
        /// `apply`, which is now a choice rather than a necessity. Until #389
        /// it was the latter: `drop_column`'s idempotence precheck read
        /// `PRAGMA table_info`, could not see a generated column, and no-oped
        /// before any rebuild ran -- so this guard was correct and unreachable.
        /// The precheck reads `table_xinfo` now and the path is live;
        /// `dropping_a_generated_column_actually_drops_it` exercises it
        /// end-to-end. This test stays at the unit level because what it pins
        /// is the case folding, which is cheaper to assert directly.
        #[test]
        fn a_generated_column_being_dropped_is_not_a_warned_loss() {
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE t (
                     id INTEGER PRIMARY KEY,
                     n INTEGER,
                     v INTEGER GENERATED ALWAYS AS (n * 2) VIRTUAL,
                     code TEXT UNIQUE
                 );",
            )
            .unwrap();
            let sql = table_sql(&conn, "t").unwrap();
            for spelling in ["v", "V"] {
                let lost = lost_constructs(&conn, "t", spelling, sql.as_deref(), &[]).unwrap();
                assert!(
                    !lost.iter().any(|l| l.contains("generated column")),
                    "dropping {spelling:?} must not report the column being dropped: {lost:?}"
                );
            }
        }

        /// An ordinary table produces no generated-column warning.
        ///
        /// Cheap, and it is the direction a keyword scan of the DDL would have
        /// got wrong: `table_xinfo` answers structurally, so a column named
        /// `generated` or a `CHECK` mentioning `AS` cannot produce a false
        /// positive the way `contains("GENERATED")` would.
        ///
        /// The table therefore carries both spellings for real rather than in
        /// prose -- a column literally named `generated`, and a `CHECK` whose
        /// text contains `AS` -- because a test whose doc comment claims an
        /// input it does not have is the drift this file keeps finding.
        #[test]
        fn an_ordinary_table_reports_no_generated_column() {
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE t (
                     id INTEGER PRIMARY KEY,
                     generated TEXT,
                     n INTEGER CHECK (CAST(n AS TEXT) <> ''),
                     code TEXT UNIQUE
                 );",
            )
            .unwrap();
            let sql = table_sql(&conn, "t").unwrap();
            let lost = lost_constructs(&conn, "t", "code", sql.as_deref(), &[]).unwrap();
            assert!(
                !lost.iter().any(|l| l.contains("generated column")),
                "a column named `generated` is not a generated column: {lost:?}"
            );
        }

        /// Every column of a table in declaration order, generated ones
        /// included -- which `columns` (via `table_info`) cannot report.
        fn columns_xinfo(conn: &Connection, table: &str) -> Vec<String> {
            let mut stmt = conn
                .prepare(&format!("PRAGMA table_xinfo({})", quote_ident(table)))
                .unwrap();
            let rows = stmt
                .query_map([], |r| r.get::<_, String>(1))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            rows
        }

        /// #387: both storage classes survive a rebuild, and still compute.
        ///
        /// The inverse of the test that stood here for #342, which asserted the
        /// rebuild dropped them and said in its own message that keeping them
        /// would make it wrong. It does keep them now.
        ///
        /// Asserted behaviourally rather than by reading the DDL back: a column
        /// can be declared `GENERATED ALWAYS AS` and hold a stale value if a
        /// rebuild copied into it, which is smugglr#342's other half. Moving the
        /// base and reading the generated column is the only assertion that
        /// tells a preserved computation from a preserved declaration.
        #[test]
        fn the_rebuild_carries_generated_columns_through() {
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE t (
                     id INTEGER PRIMARY KEY,
                     n INTEGER,
                     v INTEGER GENERATED ALWAYS AS (n * 2) VIRTUAL,
                     s INTEGER GENERATED ALWAYS AS (n * 3) STORED,
                     code TEXT UNIQUE
                 );
                 INSERT INTO t (id, n, code) VALUES (1, 21, 'k1');",
            )
            .unwrap();

            apply(
                &conn,
                Op::DropColumn {
                    table: "t".into(),
                    column: "code".into(),
                },
            )
            .unwrap();

            let after = generated_columns(&conn, "t").unwrap();
            assert_eq!(
                after,
                vec![("v".to_string(), "VIRTUAL"), ("s".to_string(), "STORED")],
                "both classes survive, and keep the class they were declared with"
            );

            // Declaration order is preserved, not appended-to-the-end. A
            // `SELECT *` reader sees the same shape it saw before.
            assert_eq!(columns_xinfo(&conn, "t"), vec!["id", "n", "v", "s"]);

            // The computation survived, not just the declaration.
            conn.execute("UPDATE t SET n = 5 WHERE id = 1", []).unwrap();
            let (v, s): (i64, i64) = conn
                .query_row("SELECT v, s FROM t WHERE id = 1", [], |r| {
                    Ok((r.get(0)?, r.get(1)?))
                })
                .unwrap();
            assert_eq!(
                (v, s),
                (10, 15),
                "a preserved generated column recomputes from its base; these are the values a \
                 column that merely kept its old contents would not have"
            );
        }

        /// #389: dropping a generated column actually drops it.
        ///
        /// The idempotence precheck reads `raw_table_columns`, which used to
        /// ask `table_info` -- a pragma that cannot see a generated column. So
        /// the op found the column absent, concluded it had already been
        /// applied, and returned `Ok(())` having dropped nothing. A destructive
        /// op reporting success while doing nothing is worse than one that
        /// fails: the next migration in the chain is written believing the
        /// column is gone.
        ///
        /// Both storage classes, because they can reach different paths -- a
        /// VIRTUAL column may be droppable by the direct `ALTER` where a STORED
        /// one is refused and falls to the rebuild.
        #[test]
        fn dropping_a_generated_column_actually_drops_it() {
            for (name, other) in [("v", "s"), ("s", "v")] {
                let conn = mem();
                conn.execute_batch(
                    "CREATE TABLE t (
                         id INTEGER PRIMARY KEY,
                         n INTEGER,
                         v INTEGER GENERATED ALWAYS AS (n * 2) VIRTUAL,
                         s INTEGER GENERATED ALWAYS AS (n * 3) STORED
                     );
                     INSERT INTO t (id, n) VALUES (1, 7);",
                )
                .unwrap();

                apply(
                    &conn,
                    Op::DropColumn {
                        table: "t".into(),
                        column: name.into(),
                    },
                )
                .unwrap();

                let left = columns_xinfo(&conn, "t");
                assert!(
                    !left.iter().any(|c| c == name),
                    "dropping {name:?} left it in place: {left:?}"
                );
                // ...and the other generated column is still there, still
                // computing. #387 is what makes that true; before it, dropping
                // one would have taken the other with it.
                assert!(
                    left.iter().any(|c| c == other),
                    "dropping {name:?} took {other:?} with it: {left:?}"
                );
                let still: i64 = conn
                    .query_row(&format!("SELECT {other} FROM t WHERE id = 1"), [], |r| {
                        r.get(0)
                    })
                    .unwrap();
                let expected = if other == "v" { 14 } else { 21 };
                assert_eq!(still, expected, "{other:?} stopped computing");
            }
        }

        /// When the definition cannot be resolved, the rebuild falls back to
        /// dropping the column with the warning rather than emitting a guess.
        ///
        /// The fallback has to be reachable or it is decoration, and this
        /// reaches it through the ambiguity guard rather than by doctoring a
        /// database: a generated column named `foreign` collides with the
        /// `FOREIGN KEY` clause's leading token, so the name matches two
        /// top-level items and neither can be picked without guessing.
        ///
        /// A wrong expression produces a table that is broken rather than
        /// merely diminished, so this direction is the safe one.
        #[test]
        fn an_unresolvable_generated_column_falls_back_to_being_dropped() {
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE p (id INTEGER PRIMARY KEY);
                 CREATE TABLE t (
                     id INTEGER PRIMARY KEY,
                     n INTEGER,
                     pid INTEGER,
                     \"foreign\" INTEGER GENERATED ALWAYS AS (n * 2) VIRTUAL,
                     code TEXT UNIQUE,
                     FOREIGN KEY (pid) REFERENCES p(id)
                 );",
            )
            .unwrap();
            assert_eq!(generated_columns(&conn, "t").unwrap().len(), 1);

            apply(
                &conn,
                Op::DropColumn {
                    table: "t".into(),
                    column: "code".into(),
                },
            )
            .unwrap();

            assert!(
                generated_columns(&conn, "t").unwrap().is_empty(),
                "an unresolvable generated column is dropped, not guessed at"
            );
        }

        /// A generated column computed FROM the dropped column falls back to
        /// being dropped, rather than failing the migration.
        ///
        /// Found by an adversarial review of the first cut of #387, which
        /// preserved it verbatim and produced a `CREATE TABLE` naming a column
        /// that no longer exists -- turning a migration that SUCCEEDS on the
        /// previous behaviour into a hard error. Preservation is a bonus; it
        /// must never cost the drop itself.
        #[test]
        fn a_generated_column_that_reads_the_dropped_column_is_dropped_not_preserved() {
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE t (
                     id INTEGER PRIMARY KEY,
                     a INTEGER,
                     b INTEGER,
                     g INTEGER GENERATED ALWAYS AS (a + b) VIRTUAL
                 );
                 INSERT INTO t (id, a, b) VALUES (1, 2, 3);",
            )
            .unwrap();

            apply(
                &conn,
                Op::DropColumn {
                    table: "t".into(),
                    column: "b".into(),
                },
            )
            .expect("the drop must still succeed; preserving g is what gives way");

            assert_eq!(columns_xinfo(&conn, "t"), vec!["id", "a"]);
        }

        /// A definition carrying a line comment falls back rather than
        /// producing a `CREATE TABLE` whose tail is commented out.
        ///
        /// Also from the adversarial review. The rebuilt body is joined onto
        /// one line, so a `--` captured with the definition swallows everything
        /// after it including the closing parenthesis.
        #[test]
        fn a_generated_column_whose_definition_carries_a_comment_is_dropped_not_preserved() {
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE t (
                     id INTEGER PRIMARY KEY,
                     n INTEGER,
                     g INTEGER GENERATED ALWAYS AS (n * 2) VIRTUAL -- doubles n
                     ,
                     code TEXT UNIQUE
                 );",
            )
            .unwrap();

            apply(
                &conn,
                Op::DropColumn {
                    table: "t".into(),
                    column: "code".into(),
                },
            )
            .expect("the drop must still succeed");

            assert_eq!(columns_xinfo(&conn, "t"), vec!["id", "n"]);
        }

        /// A UNIQUE that rode through on a preserved column is not reported
        /// lost.
        ///
        /// The warning's whole value is that it fires on real losses; one that
        /// fires on work which succeeded is the kind operators learn to skip.
        #[test]
        fn a_unique_carried_on_a_preserved_generated_column_is_not_warned() {
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE t (
                     id INTEGER PRIMARY KEY,
                     n INTEGER,
                     g INTEGER GENERATED ALWAYS AS (n * 2) VIRTUAL UNIQUE,
                     code TEXT
                 );
                 CREATE INDEX t_by_code ON t (code);
                 INSERT INTO t (id, n, code) VALUES (1, 4, 'k');",
            )
            .unwrap();

            apply(
                &conn,
                Op::DropColumn {
                    table: "t".into(),
                    column: "code".into(),
                },
            )
            .unwrap();

            // It survived and is still enforced, so it was never lost.
            conn.execute("INSERT INTO t (id, n) VALUES (2, 9)", [])
                .unwrap();
            let dup = conn.execute("INSERT INTO t (id, n) VALUES (3, 4)", []);
            assert!(
                dup.is_err(),
                "the UNIQUE on the preserved generated column is still enforced"
            );

            // ...and the loss list, after the filter, does not name it.
            let sql = table_sql(&conn, "t").unwrap();
            let mut lost = lost_constructs(&conn, "t", "code", sql.as_deref(), &[]).unwrap();
            let generated = generated_columns(&conn, "t").unwrap();
            let carried: Vec<&str> = generated.iter().map(|(n, _)| n.as_str()).collect();
            let mut carried_unique: Vec<String> = Vec::new();
            for index in unique_constraint_indexes(&conn, "t").unwrap() {
                let cols = index_columns(&conn, &index).unwrap();
                if !cols.is_empty()
                    && cols
                        .iter()
                        .all(|c| carried.iter().any(|n| n.eq_ignore_ascii_case(c)))
                {
                    carried_unique.push(format!("UNIQUE({})", cols.join(", ")));
                }
            }
            lost.retain(|e| !carried_unique.iter().any(|u| u == e));
            assert!(
                !lost.iter().any(|l| l.contains("UNIQUE(g)")),
                "a UNIQUE carried through is not a loss: {lost:?}"
            );
        }

        // The definition split surviving the shapes that break a naive parse
        // (comments, doubled-quote escapes, nested parens) is pinned
        // directly against the shared parser in `sql_ddl`'s own tests
        // (`top_level_items_survives_commas_and_parens_that_are_not_structure`,
        // `top_level_items_refuses_what_it_cannot_split`,
        // `top_level_items_ignores_parens_inside_comments`,
        // `top_level_items_recovers_a_doubled_quote_identifier_verbatim`)
        // since smugglr#462 moved `top_level_items` out of this module.

        /// A name that matches more than one item resolves to neither.
        #[test]
        fn a_name_matching_two_items_is_unresolved_rather_than_first_wins() {
            let sql = "CREATE TABLE t (\"foreign\" INTEGER, pid INTEGER, \
                       FOREIGN KEY (pid) REFERENCES p(id))";
            let wanted = ("foreign".to_string(), "VIRTUAL");
            assert!(
                verbatim_definitions_for(Some(sql), &[&wanted], "code").is_none(),
                "the column name collides with the FOREIGN KEY clause's leading token"
            );

            let missing = ("nope".to_string(), "STORED");
            assert!(
                verbatim_definitions_for(Some(sql), &[&missing], "code").is_none(),
                "a name that matches nothing is unresolved, not skipped"
            );
        }

        /// #396: an expression DEFAULT keeps its parentheses through a
        /// rebuild, so the CREATE TABLE is accepted.
        ///
        /// `PRAGMA table_info` reports `dflt_value` WITHOUT the parentheses
        /// SQLite requires around an expression default, so re-deriving the
        /// column emitted `DEFAULT 'a' || ')'` for a column declared
        /// `DEFAULT ('a' || ')')` -- a syntax error that failed the rebuild
        /// outright. Carrying the declaration verbatim has neither problem,
        /// because it is what the author wrote.
        ///
        /// Asserted behaviourally: insert a row without the column and read
        /// what landed. A DDL comparison would pass on a default that parses
        /// and evaluates to something else.
        #[test]
        fn drop_column_rebuild_keeps_an_expression_default_working() {
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE t (
                     id INTEGER PRIMARY KEY,
                     tricky TEXT DEFAULT ('a' || ')'),
                     computed INTEGER DEFAULT (2 + 3),
                     plain TEXT DEFAULT 'x',
                     code TEXT UNIQUE
                 );",
            )
            .unwrap();

            apply(
                &conn,
                Op::DropColumn {
                    table: "t".into(),
                    column: "code".into(),
                },
            )
            .expect("the rebuild must not be refused by its own DDL");

            conn.execute("INSERT INTO t (id) VALUES (1)", []).unwrap();
            let (tricky, computed, plain): (String, i64, String) = conn
                .query_row(
                    "SELECT tricky, computed, plain FROM t WHERE id = 1",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .unwrap();
            assert_eq!(
                tricky, "a)",
                "the parenthesised expression default still evaluates"
            );
            assert_eq!(computed, 5, "an arithmetic default is a number, not text");
            assert_eq!(plain, "x", "a literal default is unaffected");
        }

        /// A table-level foreign key sharing a column with an inline one is
        /// not swallowed by the inline-key skip.
        ///
        /// SQLite accepts both on one column and `foreign_key_list` reports
        /// both, but the verbatim declaration carries only the inline one. The
        /// first version of the skip matched on the column and dropped every
        /// row for it, losing the table-level key SILENTLY -- in a codepath
        /// whose whole argument is that a silent drop is not bounded.
        ///
        /// Found by an adversarial review. A column definition can hold at most
        /// one inline `REFERENCES`, so the skip consumes exactly one row.
        #[test]
        fn a_table_level_key_beside_an_inline_one_survives_the_rebuild() {
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE p (id INTEGER PRIMARY KEY);
                 CREATE TABLE q (id INTEGER PRIMARY KEY);
                 CREATE TABLE t (
                     id INTEGER PRIMARY KEY,
                     a INTEGER REFERENCES p(id),
                     code TEXT UNIQUE,
                     FOREIGN KEY (a) REFERENCES q(id)
                 );",
            )
            .unwrap();
            let before: i64 = conn
                .query_row(
                    "SELECT count(*) FROM pragma_foreign_key_list('t')",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(before, 2, "the table starts with both keys");

            apply(
                &conn,
                Op::DropColumn {
                    table: "t".into(),
                    column: "code".into(),
                },
            )
            .unwrap();

            let after: Vec<String> = {
                let mut stmt = conn
                    .prepare(
                        "SELECT \"table\" FROM pragma_foreign_key_list('t') ORDER BY \"table\"",
                    )
                    .unwrap();
                stmt.query_map([], |r| r.get::<_, String>(0))
                    .unwrap()
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .unwrap()
            };
            assert_eq!(
                after,
                vec!["p".to_string(), "q".to_string()],
                "both keys survive; the skip consumes the inline one only"
            );
        }

        /// A UNIQUE carried on an ordinary column is not reported as dropped.
        ///
        /// The filter used to consider only generated columns while its comment
        /// claimed it covered anything carried, so a constraint the rebuilt
        /// table still enforced was announced as lost. That is a false
        /// statement of loss rather than an over-report, and it is the failure
        /// the #342 warning cannot afford if operators are to keep reading it.
        #[test]
        fn a_unique_carried_on_an_ordinary_column_is_not_warned() {
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE t (
                     id INTEGER PRIMARY KEY,
                     u TEXT UNIQUE,
                     code TEXT UNIQUE
                 );
                 INSERT INTO t (id, u, code) VALUES (1, 'a', 'k');",
            )
            .unwrap();

            apply(
                &conn,
                Op::DropColumn {
                    table: "t".into(),
                    column: "code".into(),
                },
            )
            .unwrap();

            // Still enforced, so it was never lost.
            assert!(
                conn.execute("INSERT INTO t (id, u) VALUES (2, 'a')", [])
                    .is_err(),
                "the UNIQUE on the carried column still refuses a duplicate"
            );

            // ...and the loss list, filtered as the rebuild filters it, does
            // not name it.
            let sql = table_sql(&conn, "t").unwrap();
            let mut lost = lost_constructs(&conn, "t", "code", sql.as_deref(), &[]).unwrap();
            let carried = ["u"];
            let mut carried_unique: Vec<String> = Vec::new();
            for index in unique_constraint_indexes(&conn, "t").unwrap() {
                let cols = index_columns(&conn, &index).unwrap();
                if !cols.is_empty()
                    && cols
                        .iter()
                        .all(|c| carried.iter().any(|n| n.eq_ignore_ascii_case(c)))
                {
                    carried_unique.push(format!("UNIQUE({})", cols.join(", ")));
                }
            }
            lost.retain(|e| !carried_unique.iter().any(|u| u == e));
            assert!(
                !lost.iter().any(|l| l.contains("UNIQUE(u)")),
                "a UNIQUE carried through is not a loss: {lost:?}"
            );
        }

        /// #344: a rebuild leaves a typeless column typeless.
        ///
        /// `table_info` reports an empty declared type, and re-deriving the
        /// column turned that into `BLOB` -- moving it into the class
        /// `rowhash::is_blob_column` deliberately excludes, whose doc says
        /// base64-decoding a genuine text value there would corrupt it.
        ///
        /// The declaration carries no type, so carrying it verbatim keeps the
        /// column typeless without this path having to decide what an empty
        /// type means.
        #[test]
        fn drop_column_rebuild_leaves_a_typeless_column_typeless() {
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, untyped, code TEXT UNIQUE);
                 INSERT INTO t (id, untyped, code) VALUES (1, 'text', 'k1'), (2, 42, 'k2');",
            )
            .unwrap();

            apply(
                &conn,
                Op::DropColumn {
                    table: "t".into(),
                    column: "code".into(),
                },
            )
            .unwrap();

            let declared: String = conn
                .query_row(
                    "SELECT type FROM pragma_table_info('t') WHERE name = 'untyped'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(
                declared, "",
                "the column was declared with no type and a rebuild must not invent one"
            );

            // ...and it still stores each value as what it is, which is the
            // behaviour the declared type was standing in for.
            let kinds: Vec<String> = {
                let mut stmt = conn
                    .prepare("SELECT typeof(untyped) FROM t ORDER BY id")
                    .unwrap();
                stmt.query_map([], |r| r.get::<_, String>(0))
                    .unwrap()
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .unwrap()
            };
            assert_eq!(
                kinds,
                vec!["text".to_string(), "integer".to_string()],
                "a blank-affinity column stores each value as what it is"
            );
        }

        /// A column-level constraint the module doc listed as unrecoverable
        /// rides through on the verbatim declaration.
        ///
        /// Not a new capability so much as a consequence worth pinning: once a
        /// column is carried as written, its `CHECK`, `COLLATE` and `UNIQUE`
        /// come with it. The doc is corrected to match.
        #[test]
        fn drop_column_rebuild_carries_column_level_constraints() {
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE t (
                     id INTEGER PRIMARY KEY,
                     amount INTEGER CHECK (amount > 0),
                     tag TEXT COLLATE NOCASE,
                     code TEXT UNIQUE
                 );
                 INSERT INTO t (id, amount, tag, code) VALUES (1, 5, 'Abc', 'k1');",
            )
            .unwrap();

            apply(
                &conn,
                Op::DropColumn {
                    table: "t".into(),
                    column: "code".into(),
                },
            )
            .unwrap();

            assert!(
                conn.execute("INSERT INTO t (id, amount) VALUES (2, -1)", [])
                    .is_err(),
                "the CHECK survived and still refuses"
            );
            let matched: i64 = conn
                .query_row("SELECT count(*) FROM t WHERE tag = 'ABC'", [], |r| r.get(0))
                .unwrap();
            assert_eq!(matched, 1, "COLLATE NOCASE survived");
        }

        /// smugglr#462: `lost_constructs` used to warn about `CHECK`/`COLLATE`
        /// wherever either appeared *anywhere* in the table --
        /// `up.contains("CHECK")` on the whole original DDL -- even when the
        /// column carrying the construct was not the one being dropped and,
        /// per the previous test, rides through the rebuild verbatim (#387).
        /// That is a warning firing on work that succeeded. Pinned here as
        /// the before/after case the parser-unification diff must not carry
        /// silently: against the old whole-statement scan, this table would
        /// have produced both a `"CHECK...".contains` and a
        /// `"COLLATE...".contains` hit; the narrower, column-attributed scan
        /// must produce neither, since `amount` and `tag` both survive
        /// `code`'s drop untouched.
        #[test]
        fn check_and_collate_on_a_surviving_column_are_not_warned_losses() {
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE t (
                     id INTEGER PRIMARY KEY,
                     amount INTEGER CHECK (amount > 0),
                     tag TEXT COLLATE NOCASE,
                     code TEXT UNIQUE
                 );",
            )
            .unwrap();
            let sql = table_sql(&conn, "t").unwrap();
            let lost = lost_constructs(&conn, "t", "code", sql.as_deref(), &[]).unwrap();
            assert!(
                !lost.iter().any(|l| l.contains("CHECK")),
                "amount's CHECK is on a surviving column and must not be warned: {lost:?}"
            );
            assert!(
                !lost.iter().any(|l| l.contains("COLLATE")),
                "tag's COLLATE is on a surviving column and must not be warned: {lost:?}"
            );
        }

        /// The narrowing above must not silently swallow a genuine loss. A
        /// `CHECK` on the column actually being dropped is still named
        /// (nothing carries it forward -- the column is gone), and so is a
        /// table-level `CHECK`: this rebuild never re-emits a table-level
        /// constraint at all -- only `PRIMARY KEY` and `FOREIGN KEY` are
        /// specially reconstructed -- regardless of which column its
        /// expression happens to mention.
        #[test]
        fn check_on_the_dropped_column_or_table_level_is_still_a_warned_loss() {
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE t (
                     id INTEGER PRIMARY KEY,
                     code TEXT CHECK (code <> ''),
                     n INTEGER
                 );",
            )
            .unwrap();
            let sql = table_sql(&conn, "t").unwrap();
            let lost = lost_constructs(&conn, "t", "code", sql.as_deref(), &[]).unwrap();
            assert!(
                lost.iter().any(|l| l.contains("CHECK")),
                "the dropped column's own CHECK is a real loss: {lost:?}"
            );

            let conn2 = mem();
            conn2
                .execute_batch(
                    "CREATE TABLE u (
                     id INTEGER PRIMARY KEY,
                     a INTEGER,
                     code TEXT,
                     CHECK (a >= 0)
                 );",
                )
                .unwrap();
            let sql2 = table_sql(&conn2, "u").unwrap();
            let lost2 = lost_constructs(&conn2, "u", "code", sql2.as_deref(), &[]).unwrap();
            assert!(
                lost2.iter().any(|l| l.contains("CHECK")),
                "a table-level CHECK is never reconstructed, regardless of which column it \
                 names: {lost2:?}"
            );
        }

        /// A column named after a table-constraint keyword (`"check"`,
        /// `"unique"`, ...) is still just a column: its own CHECK/COLLATE, if
        /// any, rides through verbatim like any other survivor's, and it must
        /// not be mistaken for an actual table-level constraint merely
        /// because its *name* matches a keyword. The discriminator is
        /// whether the item's leading token is quoted -- a real `CHECK
        /// (...)` clause never is.
        #[test]
        fn a_column_named_after_a_constraint_keyword_is_not_mistaken_for_one() {
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE t (
                     id INTEGER PRIMARY KEY,
                     \"check\" TEXT COLLATE NOCASE,
                     code TEXT
                 );",
            )
            .unwrap();
            let sql = table_sql(&conn, "t").unwrap();
            let lost = lost_constructs(&conn, "t", "code", sql.as_deref(), &[]).unwrap();
            assert!(
                !lost.iter().any(|l| l.contains("COLLATE")),
                "\"check\" is a surviving column, not a table-level CHECK, and its COLLATE \
                 rides through verbatim: {lost:?}"
            );
        }

        /// smugglr#462: `WITHOUT ROWID` moved from a substring scan of the
        /// whole statement to reading the tail after the column list, so a
        /// comment mentioning the phrase inside the list no longer produces
        /// a false positive. The construct's own unconditional-loss shape is
        /// unchanged -- a real `WITHOUT ROWID` declaration still warns.
        #[test]
        fn without_rowid_mention_in_a_comment_is_not_a_false_positive() {
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE t (
                     id INTEGER PRIMARY KEY, -- not WITHOUT ROWID, just a note
                     code TEXT
                 );",
            )
            .unwrap();
            let sql = table_sql(&conn, "t").unwrap();
            let lost = lost_constructs(&conn, "t", "code", sql.as_deref(), &[]).unwrap();
            assert!(
                !lost.iter().any(|l| l.contains("WITHOUT ROWID")),
                "a comment mentioning the phrase must not be read as the table declaring it: \
                 {lost:?}"
            );

            let conn2 = mem();
            conn2
                .execute_batch("CREATE TABLE u (id TEXT PRIMARY KEY, code TEXT) WITHOUT ROWID;")
                .unwrap();
            let sql2 = table_sql(&conn2, "u").unwrap();
            let lost2 = lost_constructs(&conn2, "u", "code", sql2.as_deref(), &[]).unwrap();
            assert!(
                lost2.iter().any(|l| l.contains("WITHOUT ROWID")),
                "a real WITHOUT ROWID declaration is still a loss: {lost2:?}"
            );
        }

        /// MATCH cannot be reconstructed, so it is warned about instead.
        ///
        /// The pragma reports `NONE` even for a key declared `MATCH FULL`
        /// (measured, not assumed), which is why this is in the warned-loss list
        /// rather than in the reconstruction.
        #[test]
        fn a_match_clause_is_named_in_the_warned_losses() {
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE p (x TEXT PRIMARY KEY);
                 CREATE TABLE c (
                     id INTEGER PRIMARY KEY,
                     px TEXT,
                     code TEXT UNIQUE,
                     FOREIGN KEY(px) REFERENCES p(x) MATCH FULL
                 );",
            )
            .unwrap();
            let sql = table_sql(&conn, "c").unwrap();
            let lost = lost_constructs(&conn, "c", "code", sql.as_deref(), &[]).unwrap();
            assert!(
                lost.iter().any(|l| l.contains("MATCH")),
                "MATCH is unrecoverable and has to be named: {lost:?}"
            );

            // ...and an ordinary column whose name merely contains the keyword
            // does not trigger it. A substring scan would warn here on every
            // rebuild of a table nobody declared MATCH on.
            let plain = mem();
            plain
                .execute_batch(
                    "CREATE TABLE t (id INTEGER PRIMARY KEY, match_id TEXT, code TEXT UNIQUE);",
                )
                .unwrap();
            let plain_sql = table_sql(&plain, "t").unwrap();
            let plain_lost =
                lost_constructs(&plain, "t", "code", plain_sql.as_deref(), &[]).unwrap();
            assert!(
                !plain_lost.iter().any(|l| l.contains("MATCH")),
                "a column named match_id is not a MATCH clause: {plain_lost:?}"
            );
        }

        #[test]
        fn drop_column_preserves_fk_on_surviving_column() {
            // Breadth: a single-column FK on a SURVIVING column must be carried
            // through a rebuild. `code` is UNIQUE (forces rebuild); the FK is on
            // `parent_id`, which survives.
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE parent (id INTEGER PRIMARY KEY);
                 CREATE TABLE child (
                     id INTEGER PRIMARY KEY,
                     parent_id INTEGER REFERENCES parent(id),
                     code TEXT UNIQUE
                 );
                 INSERT INTO parent (id) VALUES (1), (2);
                 INSERT INTO child (id, parent_id, code) VALUES (10, 1, 'k1'), (20, 2, 'k2');",
            )
            .unwrap();
            apply(
                &conn,
                Op::DropColumn {
                    table: "child".into(),
                    column: "code".into(),
                },
            )
            .unwrap();
            assert_eq!(columns(&conn, "child"), vec!["id", "parent_id"]);

            // The FK survived...
            let fk_parent: i64 = conn
                .query_row(
                    "SELECT count(*) FROM pragma_foreign_key_list('child') WHERE \"table\" = 'parent'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(fk_parent, 1, "surviving-column FK must be preserved");
            // ...and is enforced: a dangling reference is now rejected.
            conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
            let bad = conn.execute("INSERT INTO child (id, parent_id) VALUES (30, 999)", []);
            assert!(bad.is_err(), "preserved FK must reject a dangling parent");
        }

        #[test]
        fn drop_column_recreates_surviving_indexes() {
            // Breadth: an explicit index on a SURVIVING column must be recreated
            // after the rebuild. Force the rebuild by dropping the UNIQUE `code`
            // column; the index under test is on `name` (a survivor).
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, code TEXT UNIQUE);
                 CREATE INDEX idx_name ON t(name);
                 INSERT INTO t VALUES (1, 'n1', 'c1'), (2, 'n2', 'c2');",
            )
            .unwrap();
            apply(
                &conn,
                Op::DropColumn {
                    table: "t".into(),
                    column: "code".into(),
                },
            )
            .unwrap();
            assert_eq!(columns(&conn, "t"), vec!["id", "name"]);
            // The survivor's index is back after the swap.
            let idx: i64 = conn
                .query_row(
                    "SELECT count(*) FROM sqlite_master WHERE type='index' AND name='idx_name'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(idx, 1, "index on the surviving column must be recreated");
        }

        /// Rows written by the `audit` side-effect table the trigger tests use.
        fn audit_rows(conn: &Connection) -> Vec<String> {
            let mut stmt = conn
                .prepare("SELECT note FROM audit ORDER BY rowid")
                .unwrap();
            stmt.query_map([], |r| r.get::<_, String>(0))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
        }

        #[test]
        fn drop_column_rebuild_keeps_the_trigger_firing() {
            // #336: the swap's DROP TABLE destroys every trigger on the table, and
            // the replay used to collect only indexes -- so an audit trigger went
            // silently missing. Presence in sqlite_master is not the property that
            // matters; FIRING is. Drop the UNIQUE `code` to force the rebuild.
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, note TEXT, code TEXT UNIQUE);
                 CREATE TABLE audit (note TEXT);
                 CREATE TRIGGER t_ai AFTER INSERT ON t
                   BEGIN INSERT INTO audit(note) VALUES (NEW.note); END;",
            )
            .unwrap();

            // Fire once BEFORE the rebuild, so a post-rebuild count of 1 cannot be
            // confused with "the trigger never fired at all".
            conn.execute(
                "INSERT INTO t (id, note, code) VALUES (1, 'before', 'c1')",
                [],
            )
            .unwrap();
            assert_eq!(
                audit_rows(&conn),
                vec!["before"],
                "trigger fires pre-rebuild"
            );

            apply(
                &conn,
                Op::DropColumn {
                    table: "t".into(),
                    column: "code".into(),
                },
            )
            .unwrap();
            assert_eq!(columns(&conn, "t"), vec!["id", "note"]);

            // The behavioural assertion: a NEW row still produces the side effect.
            conn.execute("INSERT INTO t (id, note) VALUES (2, 'after')", [])
                .unwrap();
            assert_eq!(
                audit_rows(&conn),
                vec!["before", "after"],
                "the trigger must still FIRE after the rebuild, not merely exist"
            );
        }

        #[test]
        fn drop_column_rebuild_abandons_a_trigger_that_references_the_column() {
            // #336 criterion 2: a trigger whose body names the dropped column
            // cannot be replayed -- SQLite would accept the CREATE and then fail
            // every later write. It is dropped, and named in the warned-loss list.
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, note TEXT, code TEXT UNIQUE);
                 CREATE TABLE audit (note TEXT);
                 CREATE TRIGGER t_code_ai AFTER INSERT ON t
                   BEGIN INSERT INTO audit(note) VALUES (NEW.code); END;",
            )
            .unwrap();

            // The loss is enumerated, so the shared warn! names the trigger.
            let lost = lost_constructs(
                &conn,
                "t",
                "code",
                table_sql(&conn, "t").unwrap().as_deref(),
                &aux_ddl_surviving_drop(&conn, "t", "code")
                    .unwrap()
                    .dropped_triggers,
            )
            .unwrap();
            assert!(
                lost.iter().any(|l| l.contains("t_code_ai")),
                "the abandoned trigger must be named in the warned-loss list, got {lost:?}"
            );

            apply(
                &conn,
                Op::DropColumn {
                    table: "t".into(),
                    column: "code".into(),
                },
            )
            .unwrap();

            let n: i64 = conn
                .query_row(
                    "SELECT count(*) FROM sqlite_master WHERE type='trigger' AND name='t_code_ai'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(
                n, 0,
                "a trigger referencing the dropped column is not replayed"
            );
            // The point of abandoning it: the table stays writable.
            conn.execute("INSERT INTO t (id, note) VALUES (2, 'after')", [])
                .unwrap();
            assert!(audit_rows(&conn).is_empty());
        }

        #[test]
        fn create_trigger_does_not_resolve_body_columns() {
            // The assumption the reference scan exists to cover, pinned against the
            // sqlite rusqlite actually bundles: a trigger body is resolved when the
            // triggering statement is PREPARED, not at CREATE TRIGGER. So sqlite
            // cannot be used as the oracle for "can this trigger be replayed" --
            // replaying a stale one succeeds and breaks every later write instead.
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, note TEXT);
                 CREATE TABLE audit (note TEXT);",
            )
            .unwrap();
            conn.execute_batch(
                "CREATE TRIGGER t_ai AFTER INSERT ON t
                   BEGIN INSERT INTO audit(note) VALUES (NEW.gone); END;",
            )
            .expect("CREATE TRIGGER accepts a body naming a column that does not exist");
            let err = conn
                .execute("INSERT INTO t (id, note) VALUES (1, 'x')", [])
                .expect_err("the write is what fails, once the body is resolved");
            assert!(
                err.to_string().contains("no such column"),
                "unexpected error: {err}"
            );
        }

        #[test]
        fn identifier_scan_sees_quoted_forms_and_ignores_literals() {
            // Bare, dotted, and every quoting form count as a reference...
            for body in [
                "CREATE TRIGGER x AFTER INSERT ON t BEGIN SELECT NEW.email; END",
                "CREATE TRIGGER x AFTER UPDATE OF email ON t BEGIN SELECT 1; END",
                "CREATE TRIGGER x AFTER INSERT ON t BEGIN SELECT NEW.\"email\"; END",
                "CREATE TRIGGER x AFTER INSERT ON t BEGIN SELECT NEW.`email`; END",
                "CREATE TRIGGER x AFTER INSERT ON t BEGIN SELECT NEW.[email]; END",
                "CREATE TRIGGER x AFTER INSERT ON t BEGIN SELECT NEW.EMAIL; END",
            ] {
                assert!(sql_mentions_identifier(body, "email"), "missed: {body}");
            }
            // ...while a string literal, a comment, and a merely-similar identifier
            // do not.
            for body in [
                "CREATE TRIGGER x AFTER INSERT ON t BEGIN SELECT 'email'; END",
                "CREATE TRIGGER x AFTER INSERT ON t BEGIN SELECT 1; END -- email",
                "CREATE TRIGGER x AFTER INSERT ON t BEGIN /* email */ SELECT 1; END",
                "CREATE TRIGGER x AFTER INSERT ON t BEGIN SELECT NEW.email_verified; END",
            ] {
                assert!(!sql_mentions_identifier(body, "email"), "false hit: {body}");
            }
        }

        #[test]
        fn rename_table_is_idempotent() {
            let conn = mem();
            conn.execute_batch("CREATE TABLE old_t (id TEXT PRIMARY KEY);")
                .unwrap();
            let rename = || Op::RenameTable {
                from: "old_t".into(),
                to: "new_t".into(),
            };
            apply(&conn, rename()).unwrap();
            apply(&conn, rename()).unwrap(); // already renamed: no-op
            assert!(table_exists(&conn, "new_t").unwrap());
            assert!(!table_exists(&conn, "old_t").unwrap());
        }

        #[test]
        fn rename_column_is_idempotent() {
            let conn = mem();
            conn.execute_batch("CREATE TABLE t (id TEXT PRIMARY KEY, old_c TEXT);")
                .unwrap();
            let rename = || Op::RenameColumn {
                table: "t".into(),
                from: "old_c".into(),
                to: "new_c".into(),
            };
            apply(&conn, rename()).unwrap();
            apply(&conn, rename()).unwrap(); // already renamed: no-op
            assert_eq!(columns(&conn, "t"), vec!["id", "new_c"]);
        }

        #[test]
        fn rebuild_preserves_sqlite_sequence_high_water() {
            // spike B, driven through the PUBLIC apply_ops(DropColumn) path: an
            // AUTOINCREMENT table rebuilt by DROP COLUMN must re-emit
            // AUTOINCREMENT and carry the sqlite_sequence high-water, or a retired
            // id is reused. `code` is UNIQUE so the direct ALTER is refused and
            // the reconstruction (which must reconstruct AUTOINCREMENT from the
            // original DDL) runs -- dropping a plain column would ALTER-drop
            // directly and never exercise the fix.
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, code TEXT UNIQUE);
                 INSERT INTO t (name, code) VALUES ('a','c1'),('b','c2'),('c','c3');
                 DELETE FROM t WHERE id = 3;",
            )
            .unwrap();
            // High-water is 3 even though max(id) is now 2.
            let before: i64 = conn
                .query_row("SELECT seq FROM sqlite_sequence WHERE name='t'", [], |r| {
                    r.get(0)
                })
                .unwrap();
            assert_eq!(before, 3);

            apply(
                &conn,
                Op::DropColumn {
                    table: "t".into(),
                    column: "code".into(),
                },
            )
            .unwrap();
            assert_eq!(columns(&conn, "t"), vec!["id", "name"]);

            let after: i64 = conn
                .query_row("SELECT seq FROM sqlite_sequence WHERE name='t'", [], |r| {
                    r.get(0)
                })
                .unwrap();
            assert_eq!(after, 3, "high-water preserved across rebuild");

            // The decisive check: the next insert must NOT reuse retired id 3.
            conn.execute("INSERT INTO t (name) VALUES ('d')", [])
                .unwrap();
            let new_id: i64 = conn
                .query_row("SELECT id FROM t WHERE name = 'd'", [], |r| r.get(0))
                .unwrap();
            assert_eq!(
                new_id, 4,
                "AUTOINCREMENT high-water honored; id 3 not reused"
            );
        }

        #[test]
        fn sql_has_autoincrement_ignores_literals_comments_and_identifiers() {
            // Real AUTOINCREMENT declarations are detected.
            assert!(sql_has_autoincrement(
                "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT)"
            ));
            assert!(sql_has_autoincrement(
                "create table t(\n id integer primary key autoincrement\n)"
            ));
            // The false positives the old blind substring match got wrong: the
            // keyword inside a DEFAULT literal, a block/line comment, a quoted
            // identifier, or as a substring of an unquoted identifier.
            assert!(!sql_has_autoincrement(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, note TEXT DEFAULT 'autoincrement')"
            ));
            assert!(!sql_has_autoincrement(
                "CREATE TABLE t (id INTEGER PRIMARY KEY /* autoincrement */)"
            ));
            assert!(!sql_has_autoincrement(
                "CREATE TABLE t (id INTEGER PRIMARY KEY -- autoincrement\n)"
            ));
            assert!(!sql_has_autoincrement(
                "CREATE TABLE t (\"autoincrement\" TEXT, id INTEGER PRIMARY KEY)"
            ));
            assert!(!sql_has_autoincrement(
                "CREATE TABLE t (autoincrement_flag TEXT, id INTEGER PRIMARY KEY)"
            ));
            assert!(!sql_has_autoincrement(
                "CREATE TABLE t (id INTEGER PRIMARY KEY)"
            ));
        }

        #[test]
        fn drop_column_rebuild_does_not_spuriously_add_autoincrement() {
            // Regression (found by re-verify): a DROP COLUMN rebuild whose single
            // INTEGER-PK survivor is the alias must NOT gain AUTOINCREMENT just
            // because the word appears in a comment or a DEFAULT literal. `code`
            // is UNIQUE so dropping it forces the DDL-reconstructing rebuild.
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, /* not autoincrement */ \
                 note TEXT DEFAULT 'autoincrement', code TEXT UNIQUE);
                 INSERT INTO t (note, code) VALUES ('x','c1');",
            )
            .unwrap();
            apply(
                &conn,
                Op::DropColumn {
                    table: "t".into(),
                    column: "code".into(),
                },
            )
            .unwrap();
            assert_eq!(columns(&conn, "t"), vec!["id", "note"]);
            // Independent of the detector: a real AUTOINCREMENT table materializes
            // a `sqlite_sequence` row on its first insert. The rebuilt table keeps
            // `note TEXT DEFAULT 'autoincrement'` (so its raw DDL still contains
            // the word), but it must NOT behave as autoincrement.
            conn.execute("INSERT INTO t (note) VALUES ('y')", [])
                .unwrap();
            let seq_tables: i64 = conn
                .query_row(
                    "SELECT count(*) FROM sqlite_master \
                     WHERE type='table' AND name='sqlite_sequence'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(
                seq_tables, 0,
                "no AUTOINCREMENT wrongly synthesized on the drop-column rebuild"
            );
        }

        #[test]
        fn drop_column_rebuild_preserves_a_descending_primary_key() {
            // #413: `code` is UNIQUE so dropping it forces the DDL-
            // reconstructing rebuild, and the surviving single INTEGER PK
            // column was declared DESC -- not the rowid alias. A rebuild that
            // re-derives it as a bare ascending `PRIMARY KEY` would silently
            // turn it into the alias it never was.
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE t (id INTEGER PRIMARY KEY DESC, note TEXT, code TEXT UNIQUE);
                 INSERT INTO t (id, note, code) VALUES (100, 'seeded', 'c1');",
            )
            .unwrap();
            apply(
                &conn,
                Op::DropColumn {
                    table: "t".into(),
                    column: "code".into(),
                },
            )
            .unwrap();
            assert_eq!(columns(&conn, "t"), vec!["id", "note"]);

            // Direct proof: the seeded key (100, nowhere near any rowid
            // SQLite would hand out) must not equal its own rowid.
            let aliased: i64 = conn
                .query_row("SELECT count(*) FROM t WHERE id = rowid", [], |r| r.get(0))
                .unwrap();
            assert_eq!(aliased, 0, "the DESC key must not equal its own rowid");

            // The consequence of not being an alias: an INSERT that omits the
            // key stores NULL rather than drawing a value from the rowid
            // sequence.
            conn.execute("INSERT INTO t (note) VALUES ('no key given')", [])
                .unwrap();
            let stored_null: i64 = conn
                .query_row(
                    "SELECT count(*) FROM t WHERE note = 'no key given' AND id IS NULL",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(
                stored_null, 1,
                "an omitted DESC key must stay NULL, not draw a value from the rowid sequence"
            );
        }

        #[test]
        fn drop_column_rebuild_does_not_invent_desc_on_a_without_rowid_key() {
            // Scope boundary (#413): a WITHOUT ROWID table's key has no rowid
            // to be an alias of, so ASC/DESC there is ordinary index
            // direction, not the alias question this fix answers. The
            // rebuild (which already drops WITHOUT ROWID itself, warned, and
            // unrelated to this fix) must not additionally invent a DESC the
            // original never declared.
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT UNIQUE, note TEXT) \
                 WITHOUT ROWID;
                 INSERT INTO t (id, v, note) VALUES (1, 'c1', 'seeded');",
            )
            .unwrap();
            apply(
                &conn,
                Op::DropColumn {
                    table: "t".into(),
                    column: "v".into(),
                },
            )
            .unwrap();
            assert_eq!(columns(&conn, "t"), vec!["id", "note"]);
            let sql: String = conn
                .query_row(
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name='t'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert!(
                !sql.to_ascii_uppercase().contains("DESC"),
                "a WITHOUT ROWID key's direction must not be changed by the rebuild: {sql}"
            );
        }

        #[test]
        fn rebuild_of_referenced_table_preserves_children() {
            // spike K: rebuilding a REFERENCED (parent) table with foreign_keys
            // ON would cascade-delete children when the old table is dropped.
            // foreign_keys=OFF during the rebuild protects them.
            let conn = mem();
            conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
            conn.execute_batch(
                "CREATE TABLE parent (id INTEGER PRIMARY KEY, label TEXT, junk TEXT);
                 CREATE TABLE child (
                     id INTEGER PRIMARY KEY,
                     parent_id INTEGER REFERENCES parent(id) ON DELETE CASCADE
                 );
                 INSERT INTO parent (id, label, junk) VALUES (1,'p1','x'),(2,'p2','y');
                 INSERT INTO child (id, parent_id) VALUES (10, 1), (20, 2);",
            )
            .unwrap();

            // Rebuild parent dropping `junk`.
            let spec = RebuildSpec {
                table: "parent".into(),
                target: RebuildTarget::Fragments {
                    body: vec!["\"id\" INTEGER PRIMARY KEY".into(), "\"label\" TEXT".into()],
                    without_rowid: false,
                },
                projection: vec![
                    ("id".into(), "\"id\"".into()),
                    ("label".into(), "\"label\"".into()),
                ],
                post_ddl: vec![],
            };
            rebuild_table(&conn, &spec).unwrap();

            // Children survived (not cascade-deleted).
            let children: i64 = conn
                .query_row("SELECT count(*) FROM child", [], |r| r.get(0))
                .unwrap();
            assert_eq!(children, 2);
            // Parent rebuilt correctly and enforcement is back on.
            assert_eq!(columns(&conn, "parent"), vec!["id", "label"]);
            let fk: i64 = conn
                .query_row("PRAGMA foreign_keys", [], |r| r.get(0))
                .unwrap();
            assert_eq!(fk, 1);
            // The FK relationship is still intact (no dangling refs).
            let violations = foreign_key_violations(&conn, "child").unwrap();
            assert!(violations.is_empty());
        }

        #[test]
        fn rebuild_reports_constraint_violators_and_rolls_back() {
            // spike L: constraint-ADD is data-dependent. A rebuild whose new
            // schema adds an FK the live data violates must report the offending
            // rows and leave the table untouched.
            let conn = mem();
            conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
            conn.execute_batch(
                "CREATE TABLE users (id INTEGER PRIMARY KEY);
                 CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER);
                 INSERT INTO users (id) VALUES (1);
                 INSERT INTO orders (id, user_id) VALUES (1, 1), (2, 999);",
            )
            .unwrap();

            // Rebuild orders ADDING an FK orders.user_id -> users.id; row 2
            // (user_id=999) violates it.
            let spec = RebuildSpec {
                table: "orders".into(),
                target: RebuildTarget::Fragments {
                    body: vec![
                        "\"id\" INTEGER PRIMARY KEY".into(),
                        "\"user_id\" INTEGER".into(),
                        "FOREIGN KEY (\"user_id\") REFERENCES \"users\"(\"id\")".into(),
                    ],
                    without_rowid: false,
                },
                projection: vec![
                    ("id".into(), "\"id\"".into()),
                    ("user_id".into(), "\"user_id\"".into()),
                ],
                post_ddl: vec![],
            };
            let err = rebuild_table(&conn, &spec).unwrap_err();
            match err {
                MigrateError::Apply(msg) => {
                    assert!(msg.contains("foreign keys"), "message: {msg}");
                    // MED#2: the offending row must be named, not just "foreign
                    // keys" -- row 2 (user_id=999) is the violator.
                    assert!(msg.contains("row 2"), "violator rowid must be named: {msg}");
                }
                other => panic!("expected Apply error, got {other:?}"),
            }

            // Rolled back: orders is unchanged (no FK, both rows present).
            let n: i64 = conn
                .query_row("SELECT count(*) FROM orders", [], |r| r.get(0))
                .unwrap();
            assert_eq!(n, 2);
            let fks: i64 = conn
                .query_row(
                    "SELECT count(*) FROM pragma_foreign_key_list('orders')",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(fks, 0, "the violating FK was not added");
            // Enforcement restored despite the failure.
            let fk: i64 = conn
                .query_row("PRAGMA foreign_keys", [], |r| r.get(0))
                .unwrap();
            assert_eq!(fk, 1);
        }

        #[test]
        fn type_narrowing_does_not_enforce_without_strict_or_check() {
            // spike Q: SQLite's flexible typing keeps 'hello' in an INTEGER
            // column. Type-change migrations need STRICT tables or CHECK(typeof).
            let conn = mem();

            // Plain INTEGER column: a text value is accepted (no enforcement).
            let plain = statement_for(&Op::CreateTable {
                table: "plain".into(),
                columns: vec![col("n", ColumnKind::Int)],
                without_rowid: false,
            });
            conn.execute_batch(&plain).unwrap();
            conn.execute("INSERT INTO plain (n) VALUES ('hello')", [])
                .unwrap();
            let stored: String = conn
                .query_row("SELECT typeof(n) FROM plain", [], |r| r.get(0))
                .unwrap();
            assert_eq!(stored, "text", "non-STRICT INTEGER stores text as-is");

            // A CHECK(typeof(...)) column, which render_column_def can emit,
            // rejects the same value.
            let guarded = Column {
                name: "n".into(),
                kind: ColumnKind::Int,
                constraints: vec![Constraint::Check("typeof(n) = 'integer'".into())],
                tags: vec![],
            };
            let def = render_column_def(&guarded);
            assert!(def.contains("CHECK (typeof(n) = 'integer')"), "def: {def}");
            conn.execute_batch(&format!("CREATE TABLE guarded ({def})"))
                .unwrap();
            assert!(
                conn.execute("INSERT INTO guarded (n) VALUES ('hello')", [])
                    .is_err(),
                "CHECK(typeof) rejects a text value in an integer column"
            );
            conn.execute("INSERT INTO guarded (n) VALUES (5)", [])
                .unwrap();
        }

        #[test]
        fn add_column_on_pkless_and_absent_table_is_tolerant() {
            // The precheck must not error the way local::table_info_inner would
            // on a PK-less table.
            let conn = mem();
            conn.execute_batch("CREATE TABLE pkless (a TEXT, b TEXT);")
                .unwrap();
            apply(
                &conn,
                Op::AddColumn {
                    table: "pkless".into(),
                    column: col("c", ColumnKind::Text),
                },
            )
            .unwrap();
            assert_eq!(columns(&conn, "pkless"), vec!["a", "b", "c"]);
            // Absent table: the precheck returns empty, the ALTER surfaces the
            // real "no such table" error rather than a spurious TableNotFound.
            let err = apply(
                &conn,
                Op::AddColumn {
                    table: "ghost".into(),
                    column: col("c", ColumnKind::Text),
                },
            )
            .unwrap_err();
            assert!(matches!(err, MigrateError::Apply(_)));
        }
    }
}
