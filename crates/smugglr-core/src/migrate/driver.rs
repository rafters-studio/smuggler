//! The composing apply-driver (#296): the one sanctioned forward-apply path.
//!
//! Every other migrate module is deliberately a single-purpose primitive --
//! [`ledger`](crate::migrate::ledger) records, [`apply`](crate::migrate::apply)
//! mutates, [`lint`](crate::migrate::lint) judges, [`reverse`](crate::migrate::reverse)
//! captures and restores -- and none of them composes. `apply.rs` in particular
//! is **ledger-free by invariant** (#273): it imports no ledger and resolves no
//! target. This module is where they are composed, so `apply.rs` never has to
//! learn about the ledger and the composition never has to be duplicated by a
//! CLI command or an embedder.
//!
//! # The apply lifecycle (`docs/plans/migration.md`, "Apply lifecycle")
//!
//! ```text
//! verify checksum
//!   -> ensure ledger schema
//!   -> [already applied? -> AlreadyApplied, no further checks]
//!   -> refuse a rowid-alias / AUTOINCREMENT key (#427)
//!   -> version = current_version + 1        (the DRIVER assigns it)
//!   -> [optional reconcile preflight -- #290]
//!   -> elect_apply_settle(version, checksum, run = ||
//!        lint_manifest + enforce_preimage
//!        -> apply_ops(up, pre_op = capture_before)
//!      )
//! ```
//!
//! Everything from `try_elect` through the settling `mark_success` /
//! `mark_failed` (below) is [`elect_apply_settle`](crate::migrate::driver::elect_apply_settle)'s
//! skeleton, not repeated here inline. It has exactly two callers:
//! [`apply_migration`](crate::migrate::driver::apply_migration), and
//! [`apply_compensating`](crate::migrate::reverse::apply_compensating) in
//! `reverse.rs`, which supplies its own `run` (`down_ops` / pre-image
//! restore) and none of the guards above -- see that function's own doc for
//! why each guard is not carried over (#463). That is what makes "there must
//! never be a second forward-apply loop" (below) true of the code and not
//! only the doc: the guards that only apply to a full authored manifest
//! (checksum verification, ensure-schema, the already-applied check, and the
//! #427 refusal) live once, before `apply_migration` calls
//! `elect_apply_settle`; the manifest-level lint gates run once, inside the
//! `run` closure `apply_migration` passes it; and the claim-run-settle
//! skeleton itself lives once, in `elect_apply_settle`, with `reverse.rs`
//! composing it rather than reimplementing it.
//!
//! ## The ledger write is two-phase, and election runs BEFORE apply
//!
//! There is no terminal "record the migration" step. Election claims the version
//! *before* the first byte of the database is mutated, and `mark_success` /
//! `mark_failed` only settle a row that already exists. That ordering is the
//! whole point: a ledger written *after* a successful apply would leave a crash
//! window in which the database has been mutated and the ledger is silent about
//! it -- the exact unrecoverable state recovery (#289) and reconcile (#290)
//! exist to avoid. With election first, every crash lands somewhere the ledger
//! already describes:
//!
//! | Crash point | Database | Ledger row | Next run |
//! |---|---|---|---|
//! | after `try_elect`, before op 1 | untouched | `pending`, leased | reclaimed once the lease expires, re-driven from op 1 |
//! | mid-loop | partially mutated | `pending`, leased | reclaimed, re-driven; per-op idempotency skips the applied ops |
//! | error mid-loop | partially mutated | `failed`, lease cleared | immediately reclaimable, re-driven |
//! | after the last op, before `mark_success` | fully mutated | `pending`, leased | reclaimed, re-driven as a no-op, then settles `success` |
//!
//! In every row of that table [`Ledger::current_version`] is unchanged, because
//! it keys on `status = 'success'`. A partially-applied version therefore never
//! advertises itself as applied.
//!
//! ## Local only
//!
//! 0.5.0 drives a local SQLite target and nothing else. Remote apply is not
//! runnable: #273 ships D1 / Turso / rqlite as pure statement *generators*, and
//! the host->target DDL transport is deferred to #291, which will build the
//! programmatic embedder API **on** [`apply_migration`] rather than beside it.
//! There must never be a second forward-apply loop -- and as of #463 that is
//! enforced by extraction, not merely stated: the one claim-run-settle
//! skeleton is [`elect_apply_settle`](crate::migrate::driver::elect_apply_settle),
//! [`apply_migration`](crate::migrate::driver::apply_migration) is its first
//! caller, and [`apply_compensating`](crate::migrate::reverse::apply_compensating)
//! is its second. A future third caller (#291's embedder, or a CLI wired
//! onto reverse) composes the same skeleton rather than hand-rolling another
//! copy of it.

#![cfg(feature = "native")]

use crate::error::Result;
use crate::migrate::apply::{apply_ops, rowid_alias_findings};
use crate::migrate::ledger::{Election, Ledger, DEFAULT_LEASE_SECS};
use crate::migrate::lint::{self, Classification};
use crate::migrate::reverse::{PreimageCapturer, PreimagePayload};
use crate::migrate::{ChecksummedManifest, ClassifiedOp, MigrateError};
use crate::pk_check::{self, PkCheckPolicy};
use rusqlite::{Connection, OpenFlags};
use std::path::Path;
use tracing::warn;

/// Knobs for one forward apply.
///
/// Two fields are **declared seams, not implemented behaviour** in 0.5.0. They
/// are here rather than in the later issues so the driver's success path -- a
/// serialize lane shared with #289 and #290 -- already has the shape those
/// issues fill in, instead of being reopened for a signature change. Setting
/// either one logs a warning rather than silently pretending, because a caller
/// who asked for a snapshot and did not get one is worse off than one who was
/// told no.
#[derive(Debug, Clone)]
pub struct ApplyOptions {
    /// How long the elected pending row's lease lasts, in seconds. A crash
    /// inside this window is reclaimable only after it expires.
    pub lease_secs: i64,

    /// Run the schema-drift preflight before electing.
    ///
    /// **Seam for #290.** The projection compare belongs before `try_elect`:
    /// refusing on drift must not leave a claimed pending row behind.
    pub reconcile_preflight: bool,

    /// Snapshot the database before mutating it (the coarse recovery parachute).
    ///
    /// **Seam for #289.** The `VACUUM INTO` snapshot belongs after the election
    /// is won and before the lint/apply block, so it captures exactly the state
    /// a failed apply must be restorable to.
    pub paranoid: bool,
}

impl Default for ApplyOptions {
    fn default() -> Self {
        Self {
            lease_secs: DEFAULT_LEASE_SECS,
            reconcile_preflight: false,
            paranoid: false,
        }
    }
}

/// What one [`apply_migration`] call did.
///
/// `election` is the honest outcome, not an error: losing an election is a
/// normal masterless result. When it is anything other than [`Election::Won`]
/// nothing was linted, applied, or captured, so `classifications` is empty and
/// `preimage` is `None`.
#[derive(Debug)]
pub struct ApplyOutcome {
    /// The version the driver assigned and elected.
    ///
    /// One exception, and it is the useful answer rather than a quirk: on
    /// [`Election::AlreadyApplied`] from a re-run this is the version the
    /// manifest ALREADY SUCCEEDED AT, not the version this call would have
    /// claimed. A caller asking "where does this migration live in the ledger"
    /// gets the real answer, and there is no version this call claimed to
    /// report (#325).
    pub version: u64,
    /// The **manifest's** checksum, copied from `sealed.checksum`. This is not
    /// re-read from the ledger row. Since #328 the two agree on the reclaim path --
    /// a reclaim now writes the reclaiming caller's checksum -- so this is the
    /// value the ledger holds rather than merely the value that was applied.
    /// Read the row via [`Ledger::entry`] if you need the recorded one as the
    /// ledger's own answer rather than as this call's.
    pub checksum: String,
    /// The election result. Only [`Election::Won`] means this call applied ops.
    pub election: Election,
    /// The effective per-op classification of every applied `up` op, in order
    /// (the lint's *surfacing* verdict, which honours over-declaration).
    pub classifications: Vec<Classification>,
    /// The delta-scoped pre-image captured while destructive ops ran, or `None`
    /// when nothing destructive applied.
    pub preimage: Option<PreimagePayload>,
}

/// Apply a migration to the local SQLite file at `db_path`.
///
/// Opens the database read-write **without** `CREATE`, matching
/// [`LocalDb::open`](crate::local::LocalDb::open): migrating a database that
/// does not exist is a mistake, not a request to conjure an empty one. Every
/// other concern is [`apply_migration`]'s; this only owns the connection so a
/// caller without a `rusqlite` dependency (the CLI) can still drive an apply.
pub fn apply_migration_to_file(
    db_path: &Path,
    sealed: &ChecksummedManifest,
    opts: &ApplyOptions,
) -> Result<ApplyOutcome> {
    let conn = Connection::open_with_flags(db_path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    apply_migration(&conn, sealed, opts)
}

/// The shared claim-apply-settle skeleton behind both call sites of
/// [`Ledger::try_elect`] in this crate: [`apply_migration`] below, and
/// [`apply_compensating`](crate::migrate::reverse::apply_compensating).
///
/// Owns exactly the three-step skeleton the module doc's crash table depends
/// on -- elect, run the caller's closure, settle (`mark_success` on `Ok`,
/// best-effort `mark_failed` on `Err`, with the original error still
/// returned either way) -- and nothing else. Election claims the version
/// *before* `run` executes, for the same reason the module doc gives: every
/// crash lands somewhere the ledger already describes.
///
/// This function is deliberately **not** where the guards that only make
/// sense for a full authored manifest live. Two different timings, both
/// outside this function's body:
///
/// - Checksum verification ([`ChecksummedManifest::verify`]),
///   [`Ledger::ensure_schema`], the `applied_version_of` already-applied
///   short-circuit, and the #427 rowid-alias refusal all run *before*
///   [`apply_migration`] ever calls this function -- claiming a version is
///   pointless if any of them is going to refuse.
/// - [`lint::lint_manifest`] / [`lint::enforce_preimage`] run *inside* the
///   `run` closure [`apply_migration`] passes here, after election succeeds,
///   exactly where they ran before this extraction -- they are manifest-level
///   gates, not pre-election ones, and moving them earlier would lint a
///   manifest before knowing whether this call even wins the election.
///
/// [`apply_compensating`](crate::migrate::reverse::apply_compensating) does
/// not run any of the above at all, and that omission predates this
/// extraction and is deliberate, not an oversight this function should paper
/// over: `down_ops` are the structural inverse of `up` ops that already
/// passed lint when they applied forward, or a captured pre-image restore --
/// neither is fresh user-authored DDL, so re-running manifest-level gates on
/// them is a separate design question (see that function's own doc for the
/// reasoning, and #463's PR body for why each guard was or was not carried
/// over).
///
/// Returns `(election, None)` when the election was not [`Election::Won`] --
/// `run` never executes and nothing was mutated. Returns
/// `(Election::Won, Some(value))` when `run` returned `Ok(value)`.
pub(crate) fn elect_apply_settle<T>(
    conn: &Connection,
    version: u64,
    checksum: &str,
    lease_secs: i64,
    run: impl FnOnce(&Connection) -> Result<T>,
) -> Result<(Election, Option<T>)> {
    let election = Ledger::try_elect(conn, version, checksum, lease_secs)?;
    if election != Election::Won {
        return Ok((election, None));
    }

    let attempt = run(conn);

    // `mark_success` is folded INTO the funnel rather than run after it: see
    // the module doc, "The ledger write is two-phase, and election runs
    // BEFORE apply". A bare `?` here would leave the row `pending` with a
    // live lease over a possibly-mutated database -- the abandoned-pending
    // state this funnel exists to prevent.
    let settled = attempt.and_then(|value| {
        Ledger::mark_success(conn, version)?;
        Ok(value)
    });

    match settled {
        Ok(value) => Ok((election, Some(value))),
        Err(e) => {
            // Best-effort settle: leave the row `failed` (and so immediately
            // reclaimable) rather than pending for the rest of the lease.
            // The original error is what the caller sees -- if this settle
            // also fails, the lease expiry is the backstop.
            let _ = Ledger::mark_failed(conn, version);
            Err(e)
        }
    }
}

/// Compose a full forward apply of `sealed` against a local connection.
///
/// The composition, and why each step sits where it does:
///
/// 1. **Verify the checksum.** [`ChecksummedManifest::verify`] establishes
///    exactly one thing: the body about to be applied matches the checksum
///    *travelling with it*, so the manifest was not altered after sealing. It
///    establishes nothing about the checksum on the ledger row, and the two can
///    legitimately diverge -- see the note below.
/// 2. **Ensure the ledger schema**, so a first-ever apply on a fresh database
///    does not fail reading a table that has never been created.
/// 3. **Assign the version.** `current_version + 1`, or 1 on an empty ledger.
///    The version is the *driver's* to assign, not the manifest's: the
///    generator (#270) hardcodes `version: 1` on every manifest it scaffolds,
///    so honouring `manifest.version` would make every migration claim v1.
/// 4. **Elect, run, settle.** [`elect_apply_settle`] owns this step: it wraps
///    [`Ledger::try_elect`] (transaction-free, resolves the race on
///    `UNIQUE(version)`, so this whole path is portable to a target with no
///    interactive transactions), runs the lint + apply closure only if the
///    election is [`Election::Won`], and settles the row -- `mark_success` on
///    `Ok`, `mark_failed` on `Err` -- so *any* failure past the election, a
///    lint refusal, a failed op, or a failed `mark_success`, settles the
///    claimed row as `failed` rather than abandoning it pending for a whole
///    lease. Only process death escapes the funnel, and the lease expiry
///    covers that.
///
/// The lint runs once over the manifest (both of its gates are manifest-level),
/// while pre-image capture rides the per-op `pre_op` write-ahead hook
/// [`apply_ops`] exposes, firing before each op's own transaction so it snapshots
/// committed, pre-mutation state.
///
/// # A closed gap, and the one it left behind
///
/// [`Ledger::try_elect`] used to write `sealed.checksum` **only** on the
/// fresh-`INSERT` path, so the ordinary fix-and-retry loop -- apply, fail, edit
/// the manifest, re-apply -- reclaimed row `vN` and settled it `success` while
/// the row still held the *previous* manifest's checksum.
/// [`Ledger::verify_chain`] could not catch it, because the chain is recomputed
/// from the stored checksum: the row was internally consistent and merely
/// factually wrong, so the tamper-evidence certified the divergence rather than
/// flagging it. Closed by #328 -- both reclaim arms now write the reclaiming
/// caller's checksum and recompute the row's chain-hash with it.
///
/// What that traded into, recorded because it is the more interesting half:
/// rewriting a row's chain-hash is safe only while a reclaimable row is the
/// tail of the chain, and that is upheld by the `pending` fence on
/// [`Ledger::mark_success`] and [`Ledger::mark_failed`] rather than being
/// inherent. Without the fence a node stalled past its lease can settle a row
/// another applier already finished, leaving a `failed` row behind a successor
/// -- after which the next honest reclaim orphans that successor's `prev_hash`
/// and `verify_chain` reports tampering permanently. The ledger's own docs
/// carry the sequence.
pub fn apply_migration(
    conn: &Connection,
    sealed: &ChecksummedManifest,
    opts: &ApplyOptions,
) -> Result<ApplyOutcome> {
    sealed.verify()?;
    let manifest = &sealed.manifest;

    Ledger::ensure_schema(conn)?;

    // The same migration re-run is not a new migration (#325).
    //
    // Two correct decisions composed into a defect. #270's generator hardcodes
    // `version: 1` on everything it scaffolds, so the manifest's self-reported
    // version says nothing; #296's driver therefore assigns the version itself,
    // as `current_version + 1`, which is right because honouring the manifest
    // would make every migration in a project claim v1 forever.
    //
    // Neither step asked whether THIS EXACT MIGRATION had already been applied.
    // So the increment fired unconditionally, a fresh version never collided
    // with anything, and the election always won. `Election::AlreadyApplied`
    // existed, was rendered by the CLI, and could not fire from this path.
    //
    // Asked on the CHECKSUM, which is the migration's identity. A manifest with
    // a different checksum is a different migration and still takes the next
    // version -- this branch cannot swallow it, because it never matches.
    //
    // KNOWN BROKEN, #419: A REVERSED MIGRATION CAN NEVER BE RE-APPLIED. A
    // reversal is a NEW ledgered step and leaves the reversed row byte-unchanged
    // on purpose -- editing it would trip `verify_chain`, which is what
    // `reverse.rs`'s module doc says and why. So the original manifest's success
    // row is still here with its original checksum, and this branch finds it and
    // refuses forever.
    //
    // Measured, both directions: with this branch, apply -> reverse -> re-apply
    // reports AlreadyApplied at the original version; without it, the same
    // sequence re-drives and the ops run.
    //
    // Not fixed here because there is no small fix: nothing in the ledger links
    // a reversal to what it reversed, and adding a field to the reversed row is
    // exactly what the chain hash forbids. `apply_compensating` has no
    // production caller today, so this is a library-level regression rather than
    // an operator-facing one -- #274 (closed) built `apply_compensating` and the
    // rest of `reverse.rs`, but wired no CLI caller onto it, and no CLI caller is
    // filed as a numbered issue yet (#463). Whoever files and builds one must
    // route it through `elect_apply_settle` below, the shared skeleton both
    // `apply_migration` and `apply_compensating` already call, rather than
    // hand-roll a third claim-apply-settle loop -- which is why #419 exists
    // rather than a comment saying "future work".
    if let Some(applied) = Ledger::applied_version_of(conn, &sealed.checksum)? {
        return Ok(ApplyOutcome {
            version: applied,
            checksum: sealed.checksum.clone(),
            election: Election::AlreadyApplied,
            classifications: Vec::new(),
            preimage: None,
        });
    }

    // Refuse a rowid-alias or AUTOINCREMENT primary key on any `create_table`
    // op before a version is claimed (#427). Deliberately *after* the
    // already-applied check above, not before it: a migration that minted
    // this shape and genuinely succeeded before this fix shipped must stay a
    // harmless no-op on re-run, not start failing an idempotent re-apply.
    // This is new DDL smugglr is about to mint itself, so unlike an existing
    // database (#280) there is an in-tool remedy: write `id:pk` (TEXT).
    // `generator::generate` runs the same check at scaffold time; this is the
    // second site, catching a hand-authored manifest that never passed
    // through the generator.
    let refusals = rowid_alias_findings(&manifest.up);
    pk_check::enforce(&refusals, PkCheckPolicy::Refuse)?;

    let version = Ledger::current_version(conn)?.map_or(1, |v| v + 1);

    if opts.reconcile_preflight {
        // Seam for #290: the schema-drift compare lands here, before the
        // election, so a refusal leaves no claimed row behind.
        warn!(
            version,
            "reconcile preflight requested but not implemented until #290; applying without a \
             drift check"
        );
    }

    // Apply is idempotent per-op, so a reclaimer re-driving these ops after a
    // crash re-runs them as no-ops and settles `success` -- the one state that
    // must never survive is a live lease nobody is holding, which is exactly
    // what `elect_apply_settle`'s funnel guarantees.
    let (election, applied) =
        elect_apply_settle(conn, version, &sealed.checksum, opts.lease_secs, |conn| {
            if opts.paranoid {
                // Seam for #289: the `VACUUM INTO` snapshot lands here -- after
                // the win, before the first mutation. Folded into the closure
                // (rather than checked before calling `elect_apply_settle`)
                // because it must fire only once the election is actually won.
                warn!(
                    version,
                    "--paranoid requested but the pre-migration snapshot is not implemented \
                     until #289; applying without a parachute"
                );
            }

            let classifications =
                lint::lint_manifest(manifest).map_err(|e| MigrateError::Lint(e.to_string()))?;
            lint::enforce_preimage(manifest).map_err(|e| MigrateError::Lint(e.to_string()))?;

            let mut capturer = PreimageCapturer::new();
            {
                let mut pre_op = |op: &ClassifiedOp| -> std::result::Result<(), MigrateError> {
                    capturer.capture_before(conn, op)
                };
                apply_ops(conn, &manifest.up, &mut pre_op)?;
            }
            Ok((classifications, capturer.into_payload()))
        })?;

    Ok(match applied {
        Some((classifications, payload)) => ApplyOutcome {
            version,
            checksum: sealed.checksum.clone(),
            election,
            classifications,
            preimage: (!payload.is_empty()).then_some(payload),
        },
        None => ApplyOutcome {
            version,
            checksum: sealed.checksum.clone(),
            election,
            classifications: Vec::new(),
            preimage: None,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrate::ledger::MigrationStatus;
    use crate::migrate::reverse::{CapturedValue, TablePreimage};
    use crate::migrate::{Column, ColumnKind, Constraint, Flags, Manifest, Op, OpClass, Preimage};
    use rusqlite::OptionalExtension;
    use std::cell::RefCell;

    fn col(name: &str, kind: ColumnKind) -> Column {
        Column {
            name: name.to_string(),
            kind,
            constraints: Vec::new(),
            tags: Vec::new(),
        }
    }

    /// A manifest whose `version` is deliberately the generator's hardcoded 1 --
    /// the driver is what assigns the real applied version.
    fn manifest_with(up: Vec<ClassifiedOp>, preimage: Option<Preimage>) -> ChecksummedManifest {
        ChecksummedManifest::seal(Manifest {
            version: 1,
            target_schema: "opaque".into(),
            up,
            down: Vec::new(),
            preimage,
            flags: Flags::default(),
            author: None,
        })
        .expect("seal manifest")
    }

    /// `users` carries a real primary key: the delta-scoped pre-image capture
    /// keys its surgical restore on the PK and refuses a PK-less table.
    ///
    /// TEXT, not INTEGER (#427): every test below inserts an explicit `id`
    /// value and never an omitted-key insert or `last_insert_rowid()`, so
    /// nothing here exercises rowid-alias semantics -- this helper minting a
    /// bare `INTEGER PRIMARY KEY` was itself the exact shape #427 forbids,
    /// which is part of why the driver applied it uncaught before that fix.
    fn create_users() -> ClassifiedOp {
        let mut id = col("id", ColumnKind::Text);
        id.constraints.push(Constraint::Pk);
        id.constraints.push(Constraint::NotNull);
        ClassifiedOp::new(Op::CreateTable {
            table: "users".into(),
            columns: vec![id, col("email", ColumnKind::Text)],
            without_rowid: false,
        })
    }

    fn conn() -> Connection {
        Connection::open_in_memory().expect("open in-memory db")
    }

    /// Assert against the live schema rather than the driver's own report, so a
    /// test cannot pass on a driver that returns the right outcome without
    /// having mutated anything.
    fn table_exists(conn: &Connection, table: &str) -> bool {
        conn.query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
            [table],
            |_| Ok(()),
        )
        .optional()
        .expect("query sqlite_master")
        .is_some()
    }

    fn column_exists(conn: &Connection, table: &str, column: &str) -> bool {
        let mut stmt = conn
            .prepare(&format!("PRAGMA table_info(\"{table}\")"))
            .expect("prepare table_info");
        let mut rows = stmt.query([]).expect("query table_info");
        while let Some(row) = rows.next().expect("read table_info row") {
            let name: String = row.get(1).expect("column name");
            if name == column {
                return true;
            }
        }
        false
    }

    #[test]
    fn apply_writes_the_ledger_on_a_real_apply() {
        // The property that makes reconcile (#290) non-hollow: after a real
        // apply the ledger has a baseline to compare against, so drift is
        // reportable instead of only "no baseline".
        let conn = conn();
        let sealed = manifest_with(vec![create_users()], None);

        let outcome = apply_migration(&conn, &sealed, &ApplyOptions::default()).unwrap();

        assert_eq!(outcome.election, Election::Won);
        assert_eq!(outcome.version, 1);
        assert_eq!(outcome.checksum, sealed.checksum);
        assert_eq!(Ledger::current_version(&conn).unwrap(), Some(1));

        let entry = Ledger::entry(&conn, 1).unwrap().expect("ledger row");
        assert_eq!(entry.status, MigrationStatus::Success);
        assert_eq!(entry.checksum, sealed.checksum);
        assert_eq!(entry.lease_expires_at, None);
        // And the ops actually ran.
        assert!(table_exists(&conn, "users"));
    }

    #[test]
    fn driver_assigns_the_version_not_the_manifest() {
        // Both manifests carry the generator's hardcoded `version: 1`; the
        // second must still land on v2.
        let conn = conn();
        let first = manifest_with(vec![create_users()], None);
        let second = manifest_with(
            vec![ClassifiedOp::new(Op::AddColumn {
                table: "users".into(),
                column: col("nickname", ColumnKind::Text),
            })],
            None,
        );
        assert_eq!(first.manifest.version, 1);
        assert_eq!(second.manifest.version, 1);

        apply_migration(&conn, &first, &ApplyOptions::default()).unwrap();
        let outcome = apply_migration(&conn, &second, &ApplyOptions::default()).unwrap();

        assert_eq!(outcome.version, 2);
        assert_eq!(Ledger::current_version(&conn).unwrap(), Some(2));
    }

    #[test]
    fn re_running_the_same_manifest_reports_already_applied_and_writes_no_second_row() {
        // #325. The driver assigns `current_version + 1` unconditionally, so a
        // re-run used to get a FRESH version, never collide with anything, and
        // win the election -- making `Election::AlreadyApplied` unreachable from
        // this path while the variant existed and the CLI rendered it.
        //
        // Two manifests are not enough to see this and that is why it survived:
        // `driver_assigns_the_version_not_the_manifest` uses two DIFFERENT ones
        // and is correct for what it tests. This drives the same one twice.
        let conn = conn();
        let sealed = manifest_with(vec![create_users()], None);

        let first = apply_migration(&conn, &sealed, &ApplyOptions::default()).unwrap();
        assert_eq!(first.election, Election::Won);
        assert_eq!(first.version, 1);

        let second = apply_migration(&conn, &sealed, &ApplyOptions::default()).unwrap();
        assert_eq!(
            second.election,
            Election::AlreadyApplied,
            "the same migration re-run is not a new migration"
        );
        assert_eq!(
            second.version, 1,
            "the reported version is where it actually landed, not the version this call would \
             have claimed"
        );

        // The ledger's meaning is "which migrations have been applied", not
        // "how many times someone ran apply". One row, still at v1.
        assert_eq!(Ledger::current_version(&conn).unwrap(), Some(1));
        assert_eq!(
            Ledger::entries(&conn).unwrap().len(),
            1,
            "a re-run must not append a second row for the same migration"
        );
    }

    #[test]
    fn the_same_manifest_after_a_failure_re_drives_rather_than_reporting_already_applied() {
        // The half that keeps #325's fix from swallowing the reclaim path. The
        // checksum lookup is `status = 'success'` for this reason: a failed row
        // carrying this same checksum has to fall through and be re-driven.
        //
        // Live in a way it was not when #325 was filed. Before #328 a reclaimed
        // row did not carry the checksum of the manifest that actually ran, so
        // a lookup missing that filter would now match rows it could not have
        // matched then.
        let conn = conn();
        let sealed = manifest_with(
            vec![
                create_users(),
                ClassifiedOp::new(Op::AddColumn {
                    table: "ghosts".into(),
                    column: col("boo", ColumnKind::Text),
                }),
            ],
            None,
        );

        let err = apply_migration(&conn, &sealed, &ApplyOptions::default()).unwrap_err();
        assert_eq!(err.exit_code(), 4);
        assert_eq!(
            Ledger::entry(&conn, 1).unwrap().unwrap().status,
            MigrationStatus::Failed
        );

        // Same checksum, failed row. It must NOT be read as already applied.
        let again = apply_migration(&conn, &sealed, &ApplyOptions::default()).unwrap_err();
        assert_eq!(
            again.exit_code(),
            4,
            "a failed migration re-run has to be re-driven and fail again, not be skipped as \
             already applied"
        );
        assert_eq!(Ledger::current_version(&conn).unwrap(), None);
    }

    #[test]
    fn a_held_election_applies_nothing() {
        // Another node holds a live lease on the version this driver would
        // claim: the driver reports the outcome and touches neither the schema
        // nor the lint.
        let conn = conn();
        Ledger::ensure_schema(&conn).unwrap();
        assert_eq!(
            Ledger::try_elect(&conn, 1, "someone-elses-checksum", 300).unwrap(),
            Election::Won
        );

        let sealed = manifest_with(vec![create_users()], None);
        let outcome = apply_migration(&conn, &sealed, &ApplyOptions::default()).unwrap();

        assert_eq!(outcome.election, Election::HeldByOther);
        assert!(outcome.classifications.is_empty());
        assert!(outcome.preimage.is_none());
        assert_eq!(Ledger::current_version(&conn).unwrap(), None);
        assert!(!table_exists(&conn, "users"));
    }

    /// Pins #273's `apply_ops` hook **contract**, not this driver's use of it:
    /// it drives `apply_ops` directly with its own closure, so it would still
    /// pass if the driver's capture wiring were deleted. The driver's own
    /// interleave is covered by
    /// [`the_driver_interleaves_capture_per_op_before_each_mutates`].
    #[test]
    fn the_apply_ops_primitive_fires_the_hook_once_per_op_before_it_mutates() {
        // The hook must see every op, in order, and must observe pre-mutation
        // state. `users` is created by op 1, so a hook that fires before op 1
        // cannot see it and a hook firing before op 2 must.
        let conn = conn();
        let sealed = manifest_with(
            vec![
                create_users(),
                ClassifiedOp::new(Op::AddColumn {
                    table: "users".into(),
                    column: col("nickname", ColumnKind::Text),
                }),
            ],
            None,
        );

        let seen = RefCell::new(Vec::new());
        let mut capturer = PreimageCapturer::new();
        {
            let mut pre_op = |op: &ClassifiedOp| -> std::result::Result<(), MigrateError> {
                seen.borrow_mut().push(table_exists(&conn, "users"));
                capturer.capture_before(&conn, op)
            };
            apply_ops(&conn, &sealed.manifest.up, &mut pre_op).unwrap();
        }

        // Two ops, two firings, and the second saw the first op's committed effect.
        assert_eq!(seen.into_inner(), vec![false, true]);
    }

    #[test]
    fn the_driver_interleaves_capture_per_op_before_each_mutates() {
        // The composition's own interleave, asserted through `apply_migration`
        // rather than through `apply_ops`: delete the wiring inside the driver
        // and this fails. The captured VALUES are the proof of ordering -- a
        // hook firing after its op would find the column already gone and
        // capture nothing (or error), so recovering the pre-drop cells can only
        // happen if the hook ran first. Two destructive ops give one capture
        // each, in apply order, which is the per-op part.
        let conn = conn();
        // TEXT, not INTEGER (#427): the insert below supplies an explicit id
        // and never reads it back, so nothing here depends on rowid-alias
        // semantics -- see create_users' doc for the same reasoning.
        let mut id = col("id", ColumnKind::Text);
        id.constraints.push(Constraint::Pk);
        id.constraints.push(Constraint::NotNull);
        apply_migration(
            &conn,
            &manifest_with(
                vec![ClassifiedOp::new(Op::CreateTable {
                    table: "users".into(),
                    columns: vec![
                        id,
                        col("email", ColumnKind::Text),
                        col("phone", ColumnKind::Text),
                    ],
                    without_rowid: false,
                })],
                None,
            ),
            &ApplyOptions::default(),
        )
        .unwrap();
        conn.execute_batch(
            "INSERT INTO users (id, email, phone) VALUES ('1', 'a@example.com', '555-0100');",
        )
        .unwrap();

        let sealed = manifest_with(
            vec![
                ClassifiedOp::new(Op::DropColumn {
                    table: "users".into(),
                    column: "email".into(),
                }),
                ClassifiedOp::new(Op::DropColumn {
                    table: "users".into(),
                    column: "phone".into(),
                }),
            ],
            // See the note in `a_destructive_apply_captures_its_pre_image`: an
            // absent pre-image is the normal state, and the driver captures.
            None,
        );
        let outcome = apply_migration(&conn, &sealed, &ApplyOptions::default()).unwrap();

        let payload = outcome.preimage.expect("the driver captured a pre-image");
        assert_eq!(payload.tables.len(), 2, "one capture per destructive op");

        // In apply order, each holding the value its own op was about to lose.
        let dropped_values: Vec<(String, CapturedValue)> = payload
            .tables
            .iter()
            .map(|t| match t {
                TablePreimage::Column { dropped, rows, .. } => {
                    (dropped.clone(), rows[0][1].clone())
                }
                other => panic!("expected a dropped-column capture, got {other:?}"),
            })
            .collect();
        assert_eq!(
            dropped_values,
            vec![
                (
                    "email".to_string(),
                    CapturedValue::Text("a@example.com".into())
                ),
                ("phone".to_string(), CapturedValue::Text("555-0100".into())),
            ]
        );
        assert!(!column_exists(&conn, "users", "email"));
        assert!(!column_exists(&conn, "users", "phone"));
    }

    #[test]
    fn a_failed_apply_marks_the_row_failed_and_does_not_advance_the_version() {
        // Op 1 succeeds, op 2 renames a table that does not exist. The database
        // is left partially mutated -- and the ledger says so, which is exactly
        // the state ledger-after-apply would have hidden.
        let conn = conn();
        let sealed = manifest_with(
            vec![
                create_users(),
                ClassifiedOp::new(Op::AddColumn {
                    table: "ghosts".into(),
                    column: col("boo", ColumnKind::Text),
                }),
            ],
            None,
        );

        let err = apply_migration(&conn, &sealed, &ApplyOptions::default()).unwrap_err();
        assert_eq!(err.exit_code(), 4);

        let entry = Ledger::entry(&conn, 1).unwrap().expect("ledger row");
        assert_eq!(entry.status, MigrationStatus::Failed);
        assert_eq!(entry.lease_expires_at, None);
        assert_eq!(Ledger::current_version(&conn).unwrap(), None);
        // Partially applied: op 1 landed, op 2 did not.
        assert!(table_exists(&conn, "users"));
    }

    #[test]
    fn a_lint_refusal_settles_the_claimed_row_rather_than_abandoning_it() {
        // The property: a lint refusal lands *after* the election is won, so the
        // claimed row must be settled `failed` rather than abandoned pending for a
        // whole lease. Provoked here by an under-declared op -- a `DropColumn`
        // declared `additive` -- which is a genuine `lint_manifest` refusal.
        //
        // This test previously provoked the refusal with a destructive op carrying
        // no pre-image. That path was the #326 defect: the gate refused the normal
        // pre-apply state of every honestly authored destructive manifest, so the
        // test was resting on the bug it now must not depend on.
        let conn = conn();
        apply_migration(
            &conn,
            &manifest_with(vec![create_users()], None),
            &ApplyOptions::default(),
        )
        .unwrap();

        let under_declared = manifest_with(
            vec![ClassifiedOp::declared(
                Op::DropColumn {
                    table: "users".into(),
                    column: "email".into(),
                },
                OpClass::Additive,
            )],
            None,
        );
        let err = apply_migration(&conn, &under_declared, &ApplyOptions::default()).unwrap_err();
        assert!(
            err.to_string().contains("under-states"),
            "expected an under-declaration refusal, got: {err}"
        );

        let entry = Ledger::entry(&conn, 2).unwrap().expect("ledger row");
        assert_eq!(entry.status, MigrationStatus::Failed);
        assert_eq!(Ledger::current_version(&conn).unwrap(), Some(1));
        // The refused op never ran.
        assert!(column_exists(&conn, "users", "email"));
    }

    #[test]
    fn an_under_declared_op_is_refused_before_anything_applies() {
        // `users` must exist first, or "nothing applied" would hold trivially --
        // a drop of an absent table leaves the schema unchanged either way, so
        // the assertion would prove nothing about the lint.
        let conn = conn();
        apply_migration(
            &conn,
            &manifest_with(vec![create_users()], None),
            &ApplyOptions::default(),
        )
        .unwrap();

        let sealed = manifest_with(
            vec![ClassifiedOp::declared(
                Op::DropTable {
                    table: "users".into(),
                },
                OpClass::Additive,
            )],
            None,
        );

        let err = apply_migration(&conn, &sealed, &ApplyOptions::default()).unwrap_err();
        assert!(err.to_string().contains("under-states"));

        // Refused *before anything applied*: the table is still there. Checking
        // only `current_version` would not say that -- it stays 1 under a
        // ledger-after-apply ordering too, which is the bug this whole design
        // exists to rule out.
        assert!(table_exists(&conn, "users"));
        let entry = Ledger::entry(&conn, 2).unwrap().expect("ledger row");
        assert_eq!(entry.status, MigrationStatus::Failed);
        assert_eq!(entry.lease_expires_at, None);
        assert_eq!(Ledger::current_version(&conn).unwrap(), Some(1));
    }

    #[test]
    fn a_destructive_apply_captures_its_pre_image() {
        // The manifest must already carry a pre-image to clear `enforce_preimage`
        // (0.5.0's gate is manifest-level); the capturer is what makes the
        // reverse honest by snapshotting the rows the drop is about to lose.
        let conn = conn();
        apply_migration(
            &conn,
            &manifest_with(vec![create_users()], None),
            &ApplyOptions::default(),
        )
        .unwrap();
        conn.execute_batch("INSERT INTO users (id, email) VALUES ('1', 'a@example.com');")
            .unwrap();

        let sealed = manifest_with(
            vec![ClassifiedOp::new(Op::DropColumn {
                table: "users".into(),
                column: "email".into(),
            })],
            // `None` is the honest pre-apply state of a destructive manifest: the
            // capture happens during `apply_ops`, so the body cannot carry one yet.
            None,
        );
        let outcome = apply_migration(&conn, &sealed, &ApplyOptions::default()).unwrap();

        assert_eq!(outcome.election, Election::Won);
        assert_eq!(outcome.version, 2);
        assert!(outcome.classifications[0].destructive);
        let payload = outcome
            .preimage
            .expect("destructive apply captures a pre-image");
        assert_eq!(payload.tables.len(), 1);
        assert!(!column_exists(&conn, "users", "email"));
    }

    #[test]
    fn an_additive_apply_captures_nothing() {
        let conn = conn();
        let outcome = apply_migration(
            &conn,
            &manifest_with(vec![create_users()], None),
            &ApplyOptions::default(),
        )
        .unwrap();

        assert!(outcome.preimage.is_none());
        assert_eq!(outcome.classifications.len(), 1);
        assert!(outcome.classifications[0].is_additive());
    }

    #[test]
    fn a_tampered_manifest_never_reaches_the_ledger() {
        let conn = conn();
        let mut sealed = manifest_with(vec![create_users()], None);
        sealed.manifest.target_schema = "swapped-after-sealing".into();

        let err = apply_migration(&conn, &sealed, &ApplyOptions::default()).unwrap_err();
        assert!(err.to_string().contains("checksum mismatch"));
        // The ledger schema exists only if `ensure_schema` ran; verification is
        // ahead of it, so nothing was created and nothing was claimed.
        assert!(Ledger::current_version(&conn).is_err());
    }

    // -- #427: refuse a rowid-alias primary key before claiming a version ---

    #[test]
    fn apply_migration_refuses_a_rowid_alias_manifest_before_claiming_a_version() {
        // The driver -- not just the CLI or the generator -- must refuse a
        // hand-authored manifest carrying the rowid alias, and must do it
        // before `Ledger::try_elect` ever claims a version.
        let conn = conn();
        let mut id = col("id", ColumnKind::Int);
        id.constraints.push(Constraint::Pk);
        let sealed = manifest_with(
            vec![ClassifiedOp::new(Op::CreateTable {
                table: "things".into(),
                columns: vec![id, col("name", ColumnKind::Text)],
                without_rowid: false,
            })],
            None,
        );

        let err = apply_migration(&conn, &sealed, &ApplyOptions::default()).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("INTEGER PRIMARY KEY") || msg.contains("rowid"),
            "must name the rowid-alias shape: {msg}"
        );
        assert!(
            !table_exists(&conn, "things"),
            "the refused table must not exist"
        );
        // No version was ever claimed: `ensure_schema` ran ahead of this
        // check (so the ledger table may exist), but nothing is recorded in
        // it.
        assert_eq!(Ledger::current_version(&conn).unwrap(), None);
    }

    #[test]
    fn a_migration_already_applied_before_427_stays_a_harmless_reapply() {
        // A migration that minted the forbidden shape and genuinely
        // succeeded before this fix shipped must not start failing an
        // idempotent re-apply of the identical manifest -- only a NEW claim
        // is refused. Simulates that pre-#427 history directly through the
        // ledger (bypassing `apply_migration`, which would now refuse the
        // manifest outright) -- exactly the row a node upgraded from before
        // this fix would already be carrying.
        let conn = conn();
        let mut id = col("id", ColumnKind::Int);
        id.constraints.push(Constraint::Pk);
        let sealed = manifest_with(
            vec![ClassifiedOp::new(Op::CreateTable {
                table: "legacy_things".into(),
                columns: vec![id, col("name", ColumnKind::Text)],
                without_rowid: false,
            })],
            None,
        );

        Ledger::ensure_schema(&conn).unwrap();
        assert_eq!(
            Ledger::try_elect(&conn, 1, &sealed.checksum, DEFAULT_LEASE_SECS).unwrap(),
            Election::Won
        );
        Ledger::mark_success(&conn, 1).unwrap();

        let outcome = apply_migration(&conn, &sealed, &ApplyOptions::default()).unwrap();
        assert_eq!(outcome.election, Election::AlreadyApplied);
        assert_eq!(outcome.version, 1);
    }

    // -- #463: the shared elect_apply_settle funnel, pinned directly --------
    //
    // [`elect_apply_settle`] is the one place both [`apply_migration`] (above)
    // and [`apply_compensating`](crate::migrate::reverse::apply_compensating)
    // (`reverse.rs`) get their claim-run-settle behavior from. Pinning it here,
    // on the shared function itself rather than only through each caller's own
    // tests, is the point: a future edit to this one copy that broke the
    // funnel for, say, `apply_compensating` alone would still pass every
    // `apply_migration` test above, because `apply_migration` never exercises
    // `apply_compensating`'s call site. A direct test on the shared function
    // is what actually protects both callers from drifting apart again -- the
    // failure mode #463 exists to close.

    /// The success half of the funnel: `run`'s value travels back out
    /// untouched, and the row settles `success`.
    #[test]
    fn elect_apply_settle_returns_runs_value_and_settles_success_on_ok() {
        let conn = conn();
        Ledger::ensure_schema(&conn).unwrap();

        let (election, applied) =
            elect_apply_settle(&conn, 1, "c1", DEFAULT_LEASE_SECS, |_| Ok(42u32)).unwrap();

        assert_eq!(election, Election::Won);
        assert_eq!(applied, Some(42));
        let entry = Ledger::entry(&conn, 1).unwrap().expect("ledger row");
        assert_eq!(entry.status, MigrationStatus::Success);
    }

    /// A lost election never runs `run` at all, and reports `None`.
    #[test]
    fn elect_apply_settle_does_not_run_the_closure_when_the_election_is_lost() {
        let conn = conn();
        Ledger::ensure_schema(&conn).unwrap();
        Ledger::try_elect(&conn, 1, "c1", DEFAULT_LEASE_SECS).unwrap();
        Ledger::mark_success(&conn, 1).unwrap();

        let ran = std::cell::Cell::new(false);
        let (election, applied) = elect_apply_settle(&conn, 1, "c1", DEFAULT_LEASE_SECS, |_| {
            ran.set(true);
            Ok(())
        })
        .unwrap();

        assert_eq!(election, Election::AlreadyApplied);
        assert_eq!(applied, None);
        assert!(
            !ran.get(),
            "run must not execute when the election is not Won"
        );
    }

    /// The failure half of the same funnel: `run` erroring settles the row
    /// `failed` (immediately reclaimable), not `pending`, and the original
    /// error is what the caller sees.
    #[test]
    fn elect_apply_settle_settles_failed_and_reelectable_on_a_run_error() {
        let conn = conn();
        Ledger::ensure_schema(&conn).unwrap();

        let err = elect_apply_settle(&conn, 1, "c1", DEFAULT_LEASE_SECS, |_| {
            Err::<(), _>(MigrateError::Apply("boom".into()).into())
        })
        .unwrap_err();
        assert!(err.to_string().contains("boom"));

        let entry = Ledger::entry(&conn, 1).unwrap().expect("ledger row");
        assert_eq!(entry.status, MigrationStatus::Failed);
        assert_eq!(entry.lease_expires_at, None);
        assert_eq!(
            Ledger::try_elect(&conn, 1, "c1", DEFAULT_LEASE_SECS).unwrap(),
            Election::Won,
            "a failed row must be immediately reclaimable"
        );
    }
}
