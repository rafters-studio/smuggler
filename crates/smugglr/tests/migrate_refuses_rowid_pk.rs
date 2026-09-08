//! Regression test for #427: `smugglr migrate new` and `migrate apply` must
//! refuse -- not warn -- the SQLite rowid-alias `INTEGER PRIMARY KEY`, the
//! primary-key shape smugglr's own first-run check (`pk_check.rs`) exists to
//! flag. Before the fix, both verbs minted it silently:
//!
//! ```text
//! $ smugglr migrate new create_things id:int:pk name > migrations/create_things.json
//! $ smugglr migrate apply migrations/create_things.json --db ./app.db
//! Applied migration v2 (1 op) -- checksum 025cc6ab...
//! $ sqlite3 app.db '.schema things'
//! CREATE TABLE IF NOT EXISTS "things" ("id" INTEGER PRIMARY KEY, "name" TEXT);
//! ```
//!
//! Two refusal sites, exercised here through the real CLI verbs (never an
//! internal helper): `migrate new` at scaffold time, and `migrate apply`
//! running the same check over a *hand-authored* manifest that never passed
//! through the generator -- the gap the issue calls out explicitly, since a
//! fix only at scaffold time is partial. A third case proves the fix is not
//! vacuous: `id:pk` (TEXT) must still scaffold and apply cleanly.

use std::fs;
use std::path::Path;
use std::process::Command;

use smugglr_core::migrate::{
    ChecksummedManifest, ClassifiedOp, Column, ColumnKind, Constraint, Flags, Manifest, Op,
};

fn smugglr() -> Command {
    Command::new(env!("CARGO_BIN_EXE_smugglr"))
}

fn create_empty_db(path: &Path) {
    rusqlite::Connection::open(path).expect("create empty sqlite db");
}

/// `migrate new` must refuse an explicit `int:pk` at scaffold time -- the
/// first of the two refusal sites #427 requires.
#[test]
fn migrate_new_refuses_a_bare_integer_primary_key() {
    let output = smugglr()
        .args(["migrate", "new", "create_things", "id:int:pk", "name"])
        .output()
        .expect("run smugglr migrate new");

    // Exit 2, not merely non-zero: a refusal is a CONFIGURATION error in the
    // documented 0-5 contract (`SyncError::exit_code`, and main.rs's
    // after_help), which tells a scripted caller to fix the input rather than
    // retry. Asserting only `!success()` would stay green if a future change
    // collapsed this into the generic exit 1 -- review of #427 flagged that.
    assert_eq!(
        output.status.code(),
        Some(2),
        "id:int:pk must be refused as a config error (exit 2); status: {:?}, stderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8(output.stderr).expect("utf8 stderr");
    assert!(
        stderr.contains("INTEGER PRIMARY KEY") || stderr.contains("rowid"),
        "refusal must name the rowid-alias shape, got: {stderr}"
    );
    assert!(
        stderr.contains("UUIDv7"),
        "refusal must carry the UUIDv7 remedy, got: {stderr}"
    );
    // Nothing was scaffolded: stdout carries no manifest JSON.
    let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
    assert!(
        stdout.trim().is_empty(),
        "a refused scaffold must print no manifest, got: {stdout}"
    );
}

/// `migrate apply` must refuse the same shape when it arrives in a
/// *hand-authored* manifest -- one that never passed through `migrate new`.
/// This is the second refusal site: catching only the generator would be a
/// partial fix, since nothing stops an author from writing the manifest JSON
/// directly.
#[test]
fn migrate_apply_refuses_a_hand_authored_rowid_alias_manifest() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("app.db");
    create_empty_db(&db);

    // Hand-authored: built directly from the structured op types, never
    // through `generator::generate`.
    let manifest = Manifest {
        version: 1,
        target_schema: String::new(),
        up: vec![ClassifiedOp::new(Op::CreateTable {
            table: "things".into(),
            columns: vec![
                Column {
                    name: "id".into(),
                    kind: ColumnKind::Int,
                    constraints: vec![Constraint::Pk],
                    tags: vec![],
                },
                Column {
                    name: "name".into(),
                    kind: ColumnKind::Text,
                    constraints: vec![],
                    tags: vec![],
                },
            ],
            without_rowid: false,
        })],
        down: vec![],
        preimage: None,
        flags: Flags::default(),
        author: None,
    };
    let sealed = ChecksummedManifest::seal(manifest).expect("seal manifest");
    let manifest_path = dir.path().join("create_things.json");
    fs::write(
        &manifest_path,
        serde_json::to_vec(&sealed).expect("serialize manifest"),
    )
    .expect("write manifest");

    let output = smugglr()
        .args([
            "migrate",
            "apply",
            manifest_path.to_str().expect("utf8 path"),
            "--db",
        ])
        .arg(&db)
        .output()
        .expect("run smugglr migrate apply");

    // Exit 2 for the same reason as the scaffold refusal above: this is a
    // configuration error, and the exit code is the scripting contract.
    assert_eq!(
        output.status.code(),
        Some(2),
        "a hand-authored rowid-alias manifest must be refused as a config error (exit 2); status: {:?}, stderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8(output.stderr).expect("utf8 stderr");
    assert!(
        stderr.contains("INTEGER PRIMARY KEY") || stderr.contains("rowid"),
        "refusal must name the rowid-alias shape, got: {stderr}"
    );

    // The refused table was never created.
    let conn = rusqlite::Connection::open(&db).expect("reopen db");
    let table_count: i64 = conn
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'things'",
            [],
            |r| r.get(0),
        )
        .expect("query sqlite_master");
    assert_eq!(table_count, 0, "the refused table must not exist");

    // No version was claimed. The ledger table may exist -- `ensure_schema`
    // and the already-applied lookup both run ahead of this refusal, so it
    // has a chance to create the table -- but it must hold no row for this
    // manifest's checksum.
    let ledger_table_exists: i64 = conn
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = '_smugglr_migrations'",
            [],
            |r| r.get(0),
        )
        .expect("query sqlite_master");
    if ledger_table_exists > 0 {
        let ledger_rows: i64 = conn
            .query_row("SELECT count(*) FROM _smugglr_migrations", [], |r| r.get(0))
            .expect("query ledger");
        assert_eq!(
            ledger_rows, 0,
            "refusing before a version is claimed must leave no ledger row"
        );
    }
}

/// Non-vacuity: `id:pk` (TEXT) -- the accepted shape -- must still scaffold
/// and apply cleanly through the same two real verbs.
#[test]
fn text_primary_key_still_scaffolds_and_applies() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("app.db");
    create_empty_db(&db);
    let manifest_path = dir.path().join("create_things.json");

    let new_output = smugglr()
        .args(["migrate", "new", "create_things", "id:pk", "name"])
        .output()
        .expect("run smugglr migrate new");
    assert!(
        new_output.status.success(),
        "id:pk (TEXT) must scaffold cleanly, got status {:?} stderr {}",
        new_output.status,
        String::from_utf8_lossy(&new_output.stderr)
    );
    fs::write(&manifest_path, &new_output.stdout).expect("write manifest");

    let apply_output = smugglr()
        .args([
            "migrate",
            "apply",
            manifest_path.to_str().expect("utf8 path"),
            "--db",
        ])
        .arg(&db)
        .output()
        .expect("run smugglr migrate apply");
    assert!(
        apply_output.status.success(),
        "id:pk (TEXT) must apply cleanly, got status {:?} stderr {}",
        apply_output.status,
        String::from_utf8_lossy(&apply_output.stderr)
    );

    let conn = rusqlite::Connection::open(&db).expect("reopen db");
    let schema: String = conn
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'things'",
            [],
            |r| r.get(0),
        )
        .expect("things table exists");
    assert!(
        schema.contains("\"id\" TEXT") && schema.contains("PRIMARY KEY"),
        "expected a TEXT primary key, got: {schema}"
    );
    assert!(
        !schema.contains("INTEGER PRIMARY KEY"),
        "must not be a rowid alias: {schema}"
    );
}
