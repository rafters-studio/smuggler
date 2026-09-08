# The ledger belongs in smugglr's own store, not in every target

**Status:** design. Shape ruled; the mechanics are a relocation, not a rewrite.
**Operator ruling, 2026-09-08 (Sean):** the ledger and the operation log live in
smugglr's own local database. Migrations apply locally, their state is recorded
locally, and they release outward to targets as they are verified. Nothing
smugglr owns is written into a target database.

## The reason, which is scale

smugglr syncs one authoritative database to many targets, and the count has no
fixed ceiling -- `browser-wasm-d1-multitenant` is a D1 per tenant. A ledger table
in the target means creating, versioning, chaining and leasing
`_smugglr_migrations` in every one of them.

One ledger records the state instead, and **where it is stored is the operator's
choice -- a connection string, nothing smugglr needs an opinion about** (operator
ruling, 2026-09-08). The natural shape is a `[ledger]` section resolved by the
same target vocabulary `[target]` already uses: a local SQLite file, a D1, or
anything behind the http-sql plugin. That also answers a Worker authority with no
home directory: it points the ledger wherever it likes.

**Not designed for here:** the extreme topology, where the targets are hundreds
of thousands of browser wasm databases. Operator call, 2026-09-08 -- an outlier to
solve if it ever shows up. Its shape is noted under Open.

A remote ledger store is *not* in that category. D1 is arguably the most widely
used SQLite, so pointing `[ledger]` at one is a mainstream configuration, and the
work it needs is the first item under Open.

## What this is not

An earlier draft of this document proposed attaching a sidecar *per target
database* and staging table copies through it. That was wrong twice: it would
create one sidecar per target -- the exact multiplication this ruling exists to
remove -- and it invented a copy-and-release mechanism the codebase does not
need, because migrations already apply locally.

Recorded so the wrong shape is not rediscovered as a good idea.

## How this got built the other way

1. The migrate deep-research synthesis (reflection `019f684a`, 2026-07-15) listed
   two smugglr-specific decisions to resolve before building. The first was
   ledger location versus the no-contamination rule, ending: *"This is a genuine
   call; flag it for the design."*
2. `docs/plans/migration.md` was written from that research. It carries nine open
   questions. Ledger location is not among them -- dropped between the research
   and the design.
3. #272 then built it the way every standard tool does: a `schema_migrations`-shaped
   table in the target.
4. The sidecar intent survived in exactly one place: `migrate/log.rs`'s stub for
   #289, "a write-ahead, per-step, chain-hashed log in its own sidecar DB." The
   design knew smugglr's state belongs in smugglr's storage. The ledger never got
   the question asked.
5. The smugglr.dev audit (reflection `01a04a5c`) recorded that "the 'no state in
   user database' constraint is broken by the 0.5.0 `_smugglr_migrations`
   ledger." Written down, never escalated.

Do not cite reflection `019f6cd1` as approval of the location. It records a
ruling keeping the name "ledger" over "history", citing the `@smugglr/ledger`
namespace and the existing table prefix. That decided a word.

**The tell:** smugglr's default `[sync].exclude_tables` has to list
`_smugglr_migrations` (`config.rs`). A sync engine hiding its own table from
itself is the symptom.

## What actually moves

The machinery exists and keeps working. What changes is which connection the
ledger writes to.

- `ledger.rs` opens smugglr's own database rather than the target connection. Its
  five invariants -- `UNIQUE(version)` election, success-gated skip, leased
  reclaimable pending, per-version chain hash, observable current version -- are
  unchanged in mechanism. They already operate per-node, not fabric-wide:
  `_smugglr_migrations` is in the default `exclude_tables`, so it never
  replicated. Relocating it loses nothing that was ever cross-node.
- `driver.rs` keeps its apply lifecycle. The #427 refusal placement, the election,
  the lease and the settle are unchanged; only the ledger handle differs.
- **The ledger describes migrations, not targets.** One chain: version,
  checksum, ops, and the schema fingerprint that version produces. No row is
  keyed to a target, because nothing may scale with the number of targets.
- `config.rs` **keeps** the `_smugglr_migrations` entry in the default
  `exclude_tables`. An earlier draft of this document said to drop it, on the
  reasoning that nothing writes the table any more. That is wrong and the
  correction came from #456's author: every already-migrated database still
  *carries* the table after relocation ships. Dropping the exclusion would make
  sync start replicating that stale leftover to every target -- pushing smugglr's
  old bookkeeping into the databases this whole change exists to keep it out of.
  The entry stops being a tell and becomes a tombstone.
- `docs/examples/cli-migrate/README.md` documents the ledger as living inside the
  migrated database. It stops being true.

## The schema fingerprint

Operator ruling, 2026-09-08: there needs to be a fingerprint for the schema, to
compare against.

Why it becomes necessary now: while the ledger sat inside the database it
described, the two could not diverge -- a row saying "v5" was in the same file as
the schema v5 produced. Move the ledger out and they can. A target restored from
backup, replaced, or migrated by another hand looks identical to a current one,
and a local ledger has no way to notice.

**The mechanism is already provisioned.** The ledger carries a nullable
`schema_projection` column, added up front by #272 because the ledger "tracks
migrations and cannot be cleanly `ALTER`ed later", sitting outside the chain-hash
input. `migrate/schema_projection.rs` is its stub, and it already names the trap:

> a stable projection over `table_info` + `foreign_key_list` +
> `index_list`/`index_info`, **NOT a hash of `sqlite_master` text (which a
> 12-step rebuild rewrites)**.

That caveat is load-bearing. A text hash changes whenever smugglr's own rebuild
reformats equivalent DDL, so every target would read as drifted after the exact
operation smugglr performs most.

**#290 moves from a feature to a prerequisite.** Reconcile was specced as optional
drift detection; with the ledger relocated it is the only thing tying a ledger row
to the target it claims to describe.

## The rebuild scratch table is the same defect

The ledger is not the only smugglr state written into a target. The 12-step
rebuild builds `_smugglr_rebuild_tmp` too -- what #338 fixed a collision on, after
a real user table of that name was destroyed, and what #424 is still open about
(a VIEW at the scratch name yields a raw SQLite error rather than the crafted
one).

The rebuild runs against the local database, so its scratch table is local, and
this is a smaller problem than the ledger. But it is the same rule broken the same
way, and #424 should be judged against the rule rather than fixed on its own
terms.

## Open

- **The ledger is written against rusqlite, not against an abstraction. This is
  the barrier to a remote connection string, and it is bigger than an error
  code.** *(Now filed as #457; #456 is the relocation.)* Fourteen production functions in `ledger.rs` take `&rusqlite::Connection`
  (`try_elect`, `mark_success`, `current_version`, `verify_chain`, the rest), and
  the module is `#![cfg(feature = "native")]`. D1 is arguably the most widely used
  SQLite, so "the ledger lives in a D1" is a mainstream configuration, not an
  exotic one -- and today it cannot be expressed at all, because there is no type
  the ledger accepts that a plugin target can satisfy.

  Making it real means the ledger operates over an executor abstraction that both
  rusqlite and the http-sql plugin implement, rather than a concrete connection.
  `DataSource` is the existing candidate; a narrower ledger-specific trait is the
  alternative. Either way it is a rewrite of the module's surface, not a
  parameter change.

  Two things fall out of that rewrite rather than needing separate solving. The
  election race is detected today by catching `rusqlite::Error::SqliteFailure`
  with extended code `2067` at `ledger.rs:645`; over http-sql the same collision
  arrives as an error envelope, and #444's plugin error class (merged `af527a6`)
  is the channel for recognising it by class. And every ledger operation becomes a
  network round-trip, which the apply lifecycle currently treats as free.

- **Fingerprint scope.** Whole-database, or per-table? Per-table is what a
  migration touches and lets one drifted table refuse without blocking the rest;
  whole-database is one comparison and one column.
- **What a mismatch does.** Refuse the apply naming the drift, or record and
  proceed. Refusing is consistent with #427's reasoning; a false positive from an
  imprecise projection blocks every migration, which is why the
  semantic-not-textual requirement is the crux.
- **The extreme topology, if it ever arrives.** Authority in a Worker, targets as
  hundreds of thousands of browser wasm databases. Nothing may scale with target
  count there, so no per-target row works -- a target would locate itself by
  computing its own fingerprint, finding it in the chain, and applying the tail,
  needing no per-client state. It also needs a client-side migrate that does not
  exist: `ledger.rs` is `#![cfg(feature = "native")]` and `smugglr-wasm` carries no
  migrate code at all. Recorded, not scoped.
- **Migrating existing deployments.** Databases already carrying
  `_smugglr_migrations` need that history read out of the target into the local
  store once, or their migrations re-run. Neither is free.
- **Remote apply (#291).** Unbuilt, and it inherits this cleanly: with the ledger
  local, remote apply records state locally like every other target rather than
  needing a table on the far side.

Affected open issues whose premise this changes: **#419** (expressing "vN was
compensated" -- shaped by where the ledger is), **#327** (`verify_chain` has no
production call site), **#330** (`HeldByOther` names a node the schema cannot
identify), **#331** (`Manifest::flags` unvalidated), **#424** (above), and the
unbuilt **#289**, **#290**, **#291**, **#276**-**#279**.

The smugglr.dev pages stating the no-state constraint live in `shingle`. Once this
lands their claim is true again and the audit finding in `01a04a5c` closes -- as a
hand-off, not a task here.
