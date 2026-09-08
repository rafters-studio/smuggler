//! First-run primary-key compatibility check.
//!
//! smugglr's identity **is** the primary key: the masterless last-received-wins
//! fabric matches rows across nodes by PK. A locally-sequential key
//! (`AUTOINCREMENT`, or a bare `INTEGER PRIMARY KEY` rowid alias) means two
//! machines both mint `id = 5` for different rows and the fabric silently
//! overwrites one with the other -- guaranteed cross-node data loss. See
//! `docs/plans/migration.md` (Precondition).
//!
//! **Which policy applies depends on whose DDL it is, and the two answers
//! differ today.**
//!
//! For an EXISTING database -- the first-run check -- 0.5.0 WARNS and does not
//! refuse (Sean, 2026-07-18): the sanctioned remedy, the in-tool
//! `int -> UUIDv7` conversion (#280), is deferred to 0.5.x, so hard-refusing an
//! onboarding user with no in-tool fix is worse than warning. Every warning
//! therefore carries the manual UUIDv7 remigration recipe. That default flips to
//! [`PkCheckPolicy::Refuse`] once #280 lands.
//!
//! For NEW DDL that smugglr is about to mint itself, `migrate new` and
//! `migrate apply` already pass [`PkCheckPolicy::Refuse`] directly, in 0.5.0,
//! with #280 unlanded (#427) -- because there the remedy exists and is one
//! keystroke: write `id:pk`. So `Refuse` is NOT unreachable this release, and a
//! maintainer reading only the paragraph above would conclude otherwise.
//!
//! Classification is **DDL-based and read-only**. `PRAGMA table_info` reports a
//! rowid-alias `INTEGER PRIMARY KEY` and a non-alias `INTEGER PRIMARY KEY DESC`
//! identically (the DESC trap, spike-verified), so the classifier parses the
//! declared `CREATE TABLE` DDL from `sqlite_master` rather than trusting
//! `table_info.type`. It never inserts a probe row and never requires a SQL-side
//! `DEFAULT` (UUIDv7 keys are typically app-minted and invisible to DDL).
//!
//! Three layers:
//! - **L1 shape** -- flag the rowid-alias `INTEGER PRIMARY KEY`, `AUTOINCREMENT`,
//!   and no-declared-PK. Accept a globally-unique TEXT/BLOB PK, a globally-unique
//!   BIGINT (snowflake), a composite text PK, and `INTEGER PRIMARY KEY DESC`.
//! - **L2 realness** -- the declared PK column(s) must be `NOT NULL`.
//! - **L3 stability** -- guidance when the PK name looks derived-and-volatile
//!   (kessel's `game_id = sha256(fqn:guid)` shifts every patch and reads as 100%
//!   churn); DDL cannot prove derivation, so this is a name heuristic.

use tracing::warn;

/// The manual `int -> UUIDv7` remigration recipe carried in every L1/L2 warning.
///
/// 0.5.0 defers the in-tool conversion (#280), so the warning ships the recipe as
/// prose. It intentionally pulls in no UUID dependency -- generation is the app's
/// job until #280 lands.
const UUIDV7_RECIPE: &str = "remedy: migrate this table to an app-minted, globally-unique \
     TEXT UUIDv7 primary key -- (1) add a new TEXT column, (2) mint a UUIDv7 per row in your \
     application, (3) rewrite every child foreign key to the new value, (4) swap the PRIMARY \
     KEY to the new column, (5) drop the old integer column. The in-tool conversion (#280) \
     will automate this in a later release.";

/// Which layer of the check produced a finding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PkLayer {
    /// L1 -- the declared PK shape mints per-node sequential ids.
    Shape,
    /// L2 -- the declared PK column is not a real (non-nullable) key.
    Realness,
    /// L3 -- the declared PK looks derived-and-volatile (guidance only).
    Stability,
}

/// The specific incompatibility a finding reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PkIssue {
    /// A bare `INTEGER PRIMARY KEY` -- the rowid alias, per-node sequential.
    IntegerPrimaryKey,
    /// `AUTOINCREMENT` -- explicitly per-node sequential.
    Autoincrement,
    /// No primary key declared -- identity falls back to the implicit rowid.
    NoPrimaryKey,
    /// A declared PK column that permits `NULL`.
    NullablePrimaryKey,
    /// A PK whose name suggests a content-derived, volatile value.
    DerivedVolatile,
}

impl PkIssue {
    fn layer(self) -> PkLayer {
        match self {
            PkIssue::IntegerPrimaryKey | PkIssue::Autoincrement | PkIssue::NoPrimaryKey => {
                PkLayer::Shape
            }
            PkIssue::NullablePrimaryKey => PkLayer::Realness,
            PkIssue::DerivedVolatile => PkLayer::Stability,
        }
    }
}

/// A single primary-key incompatibility found on a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PkFinding {
    /// The table the finding applies to.
    pub table: String,
    /// The classification layer that produced it.
    pub layer: PkLayer,
    /// The specific issue.
    pub issue: PkIssue,
    /// A human-facing message. L1/L2 messages carry the UUIDv7 recipe; the L3
    /// message names the entropy floor and the stable-identity requirement.
    pub message: String,
}

impl PkFinding {
    fn new(table: &str, issue: PkIssue, message: String) -> Self {
        Self {
            table: table.to_string(),
            layer: issue.layer(),
            issue,
            message,
        }
    }
}

/// Whether an incompatible schema warns (0.5.0) or refuses (0.5.x).
///
/// 0.5.0 defaults to [`PkCheckPolicy::Warn`]. The flip to
/// [`PkCheckPolicy::Refuse`] is gated on the `int -> UUIDv7` conversion (#280),
/// which gives a refused user an in-tool remedy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PkCheckPolicy {
    /// 0.5.0 behaviour: log every finding loudly, then proceed.
    #[default]
    Warn,
    /// 0.5.x behaviour (once #280 lands): reject an incompatible schema.
    Refuse,
}

/// Apply [`PkCheckPolicy`] to a set of findings.
///
/// Under [`PkCheckPolicy::Warn`] every finding is logged via `tracing::warn!`
/// (so it is unmissable on the console) and the function returns `Ok(())`. Under
/// [`PkCheckPolicy::Refuse`] any finding produces an `Err`. Per the design's
/// build note the refusal reuses [`crate::error::SyncError::Config`] rather than
/// adding a new variant.
/// The refusal message both migrate refusal sites emit (#427).
///
/// Shared rather than duplicated. `migrate new` cannot route through
/// [`enforce`]'s rendered `SyncError` -- `run_new` wraps its error again and the
/// "Configuration error:" prefix would double -- so it renders the same findings
/// itself. Before this function existed both sites carried their own copy of
/// this format string and agreed only by coincidence, which review of #427
/// flagged: a future edit to one would have silently diverged the scaffold-time
/// message from the apply-time one, uncaught because the two are asserted with
/// `contains`, never against each other.
pub(crate) fn render_refusals(findings: &[PkFinding]) -> String {
    let joined = findings
        .iter()
        .map(|f| format!("[{}] {}", f.table, f.message))
        .collect::<Vec<_>>()
        .join("; ");
    format!("incompatible primary key(s): {joined}")
}

pub fn enforce(findings: &[PkFinding], policy: PkCheckPolicy) -> crate::error::Result<()> {
    if findings.is_empty() {
        return Ok(());
    }
    match policy {
        PkCheckPolicy::Warn => {
            for finding in findings {
                warn!(
                    table = %finding.table,
                    "incompatible primary key: {}",
                    finding.message
                );
            }
            Ok(())
        }
        PkCheckPolicy::Refuse => Err(crate::error::SyncError::Config(render_refusals(findings))),
    }
}

/// Findings that new DDL must hard-refuse, rather than warn on: the
/// rowid-alias `INTEGER PRIMARY KEY` and `AUTOINCREMENT`, and nothing else.
///
/// This is narrower than "every L1 shape finding". [`PkIssue::NoPrimaryKey`]
/// is L1 too, but a table with no declared key at all is a different, softer
/// problem than one that actively mints per-node sequential ids -- #427 scopes
/// the hard-refusal to the shape its title names, the rowid alias (and its
/// explicit `AUTOINCREMENT` cousin). L2 ([`PkIssue::NullablePrimaryKey`]) and
/// L3 ([`PkIssue::DerivedVolatile`]) are excluded for the same reason: L3 is
/// explicitly guidance, never a gate, and L2 flags a different failure than
/// "mints a rowid alias".
///
/// Used by `migrate new` (scaffold time, #427) and `migrate apply` (before
/// the driver claims a ledger version, #427) to hard-refuse **new** DDL
/// smugglr is about to mint itself -- unlike an existing database (#280, a
/// different issue), this has an in-tool remedy (write `id:pk`), so 0.5.0's
/// "warn, no remedy" carve-out in [`enforce`] does not apply here.
pub fn rowid_refusals(findings: &[PkFinding]) -> Vec<PkFinding> {
    findings
        .iter()
        .filter(|f| matches!(f.issue, PkIssue::IntegerPrimaryKey | PkIssue::Autoincrement))
        .cloned()
        .collect()
}

/// Classify a single table from its declared `CREATE TABLE` DDL.
///
/// Returns every finding (empty means the schema is compatible). This is the
/// pure, read-only core of the check: it takes the DDL text exactly as stored in
/// `sqlite_master` and never touches a live connection.
pub fn classify_table_ddl(table: &str, ddl: &str) -> Vec<PkFinding> {
    let mut findings = Vec::new();

    let parsed = match ParsedTable::parse(ddl) {
        Some(p) => p,
        // A DDL we cannot parse (a view, a virtual table, or malformed text) is
        // not classified -- the check is best-effort and never fabricates a
        // finding it cannot ground in the parsed shape.
        None => return findings,
    };

    let pk_cols = parsed.primary_key_columns();

    // --- L1 shape -----------------------------------------------------------
    if let Some(col) = parsed.autoincrement_column() {
        findings.push(PkFinding::new(
            table,
            PkIssue::Autoincrement,
            format!(
                "column '{col}' is AUTOINCREMENT, which mints per-node sequential ids -- two \
                 nodes both mint id=5 for different rows and the fabric silently overwrites \
                 one. {UUIDV7_RECIPE}"
            ),
        ));
    } else if let Some(col) = parsed.rowid_alias_column() {
        findings.push(PkFinding::new(
            table,
            PkIssue::IntegerPrimaryKey,
            format!(
                "column '{col}' is a bare INTEGER PRIMARY KEY (a rowid alias), which mints \
                 per-node sequential ids -- two nodes both mint id=5 for different rows and \
                 the fabric silently overwrites one. {UUIDV7_RECIPE}"
            ),
        ));
    } else if pk_cols.is_empty() {
        findings.push(PkFinding::new(
            table,
            PkIssue::NoPrimaryKey,
            format!(
                "no primary key is declared, so identity falls back to the implicit rowid, \
                 which is per-node sequential -- two nodes both mint rowid=5 for different \
                 rows and the fabric silently overwrites one. {UUIDV7_RECIPE}"
            ),
        ));
    }

    // --- L2 realness --------------------------------------------------------
    // Only meaningful once a PK is declared and it is not the rowid alias
    // (which is implicitly NOT NULL and already flagged by L1).
    for col in &pk_cols {
        if !parsed.pk_column_is_not_null(col) {
            findings.push(PkFinding::new(
                table,
                PkIssue::NullablePrimaryKey,
                format!(
                    "primary key column '{col}' permits NULL -- a nullable PK is not a real \
                     identity (SQLite allows NULL in a non-INTEGER PRIMARY KEY unless it is \
                     declared NOT NULL). Declare it NOT NULL. {UUIDV7_RECIPE}"
                ),
            ));
        }
    }

    // --- L3 stability (guidance) -------------------------------------------
    for col in &pk_cols {
        if looks_derived_volatile(col) {
            findings.push(PkFinding::new(
                table,
                PkIssue::DerivedVolatile,
                format!(
                    "primary key column '{col}' looks content-derived (a hash/digest name). A \
                     re-derived key that shifts across versions (e.g. game_id = \
                     sha256(fqn:guid)) reads as 100% churn -- every row a delete + re-insert -- \
                     because content-hash sync assumes a logical row keeps its PK for its \
                     lifetime. Convergence must key on the stable identity, not a value that \
                     moves. Note the entropy floor: a 64-bit truncated hash is a birthday bet, \
                     not a uniqueness proof."
                ),
            ));
        }
    }

    findings
}

/// Whether `column` carries a column-level `PRIMARY KEY DESC` in `ddl`'s
/// `CREATE TABLE` -- SQLite's DESC trap: `x INTEGER PRIMARY KEY DESC` is
/// declared but is NOT the rowid alias, and `PRAGMA table_info` cannot see
/// the direction, only the position. `None` when `ddl` cannot be parsed as a
/// `CREATE TABLE` at all (a view, or malformed text): the caller must treat
/// that as "cannot confirm either way", never as "not DESC".
///
/// Built for the `DROP COLUMN` rebuild (#413, `migrate::apply`), which
/// re-derives a table's primary key from the *surviving* `PRAGMA table_info`
/// rows and, for a lone surviving `INTEGER` key column, re-emits it as a bare
/// `PRIMARY KEY` -- which IS the rowid alias. Asking this question rather
/// than "was this column already the rowid alias"
/// ([`ParsedTable::rowid_alias_column`]) is deliberate and narrower: that
/// question answers "no" for two shapes this fix does not own -- a
/// composite key's member (never had an ASC/DESC spelling of its own to
/// preserve) and a `WITHOUT ROWID` table's key (no rowid to be an alias of,
/// so ASC/DESC there is ordinary index direction, not the alias question at
/// all) -- and re-declaring DESC on either would be a direction change this
/// issue never asked for, in a case #413 has no test either side of. Reading
/// `pk_desc` straight off the parsed column leaves both exactly as declared.
///
/// Gated to match its only caller, `migrate::apply::rebuild_dropping_column`,
/// which is `native`-only. Without the gate this is dead code under `wasm32`
/// and `cargo clippy -p smugglr-wasm --target wasm32-unknown-unknown` fails on
/// `-D warnings` -- which the host clippy run cannot see, because
/// `smugglr-wasm` is `#![cfg(target_arch = "wasm32")]` and compiles to nothing
/// on the host. That is the shape of #422 and of #155 before it.
#[cfg(feature = "native")]
pub(crate) fn column_level_pk_is_desc(ddl: &str, column: &str) -> Option<bool> {
    let parsed = ParsedTable::parse(ddl)?;
    Some(
        parsed
            .columns
            .iter()
            .any(|c| c.column_pk && c.pk_desc && c.name.eq_ignore_ascii_case(column)),
    )
}

/// A PK column name looks derived-and-volatile.
///
/// DDL cannot prove a value is re-derived (it is app-computed and stored in a
/// plain column), so L3 is a name heuristic on the declared PK column: names that
/// read as a content hash / digest. False positives are acceptable because L3 is
/// guidance, not a gate.
fn looks_derived_volatile(col: &str) -> bool {
    let lower = col.to_ascii_lowercase();
    const HASHY: [&str; 6] = ["hash", "sha", "digest", "checksum", "fingerprint", "md5"];
    HASHY.iter().any(|needle| lower.contains(needle))
}

/// Inspect every user table on a live connection, read-only.
///
/// Reads the declared DDL from `sqlite_master` (a SELECT, never an insert-probe)
/// and classifies each user table. Internal `sqlite_%` tables are skipped.
#[cfg(feature = "native")]
pub fn check_schema(conn: &rusqlite::Connection) -> crate::error::Result<Vec<PkFinding>> {
    let mut stmt = conn.prepare(
        "SELECT name, sql FROM sqlite_master \
         WHERE type = 'table' AND name NOT LIKE 'sqlite_%' AND sql IS NOT NULL",
    )?;
    let rows = stmt.query_map([], |row| {
        let name: String = row.get(0)?;
        let sql: String = row.get(1)?;
        Ok((name, sql))
    })?;

    let mut findings = Vec::new();
    for row in rows {
        let (name, sql) = row?;
        findings.extend(classify_table_ddl(&name, &sql));
    }
    Ok(findings)
}

// ---------------------------------------------------------------------------
// DDL parsing
// ---------------------------------------------------------------------------

/// A parsed column definition from a `CREATE TABLE` body.
#[derive(Debug)]
struct ParsedColumn {
    name: String,
    /// The declared type token(s) as written, joined by single spaces. The
    /// rowid-alias rule keys on this being exactly `INTEGER` (case-insensitive).
    type_name: String,
    not_null: bool,
    /// This column carries a column-level `PRIMARY KEY` constraint.
    column_pk: bool,
    /// The column-level PK is declared `DESC` (disqualifies the rowid alias).
    pk_desc: bool,
    autoincrement: bool,
}

/// The parsed shape of a `CREATE TABLE` statement, reduced to what the PK check
/// needs.
#[derive(Debug)]
struct ParsedTable {
    columns: Vec<ParsedColumn>,
    /// Column names from a table-level `PRIMARY KEY (...)` constraint, in order.
    table_pk: Vec<String>,
    without_rowid: bool,
}

impl ParsedTable {
    /// Parse a `CREATE TABLE` statement. Returns `None` when the DDL is not a
    /// parseable `CREATE TABLE` (e.g. a view or malformed text).
    fn parse(ddl: &str) -> Option<Self> {
        let body = extract_body(ddl)?;
        let clauses = split_top_level(&body);

        let mut columns = Vec::new();
        let mut table_pk = Vec::new();

        for clause in &clauses {
            let clause = clause.trim();
            if clause.is_empty() {
                continue;
            }
            if let Some(cols) = parse_table_pk_constraint(clause) {
                table_pk = cols;
            } else if is_table_constraint(clause) {
                // UNIQUE / CHECK / FOREIGN KEY / CONSTRAINT -- not relevant here.
                continue;
            } else if let Some(col) = parse_column_def(clause) {
                columns.push(col);
            }
        }

        let without_rowid = tail_has_without_rowid(ddl);

        Some(Self {
            columns,
            table_pk,
            without_rowid,
        })
    }

    /// The declared PK column names, from either the table-level constraint or a
    /// column-level `PRIMARY KEY`.
    fn primary_key_columns(&self) -> Vec<String> {
        if !self.table_pk.is_empty() {
            return self.table_pk.clone();
        }
        self.columns
            .iter()
            .filter(|c| c.column_pk)
            .map(|c| c.name.clone())
            .collect()
    }

    /// The rowid-alias column, if any: a single-column `INTEGER PRIMARY KEY` in
    /// a rowid table, type token exactly `INTEGER` (case-insensitive).
    ///
    /// SQLite has a genuine asymmetry between the two ways to declare a
    /// single-INTEGER PK:
    /// - Column-level `x INTEGER PRIMARY KEY` is a rowid alias **only when not
    ///   `DESC`** -- `x INTEGER PRIMARY KEY DESC` is a real index, not an alias.
    /// - Table-level `PRIMARY KEY(x)` naming a single `INTEGER` column is a rowid
    ///   alias **regardless of ASC/DESC** -- the DESC trap does not apply to the
    ///   table-level form.
    ///
    /// A composite table-level `PRIMARY KEY(a, b)` is never a rowid alias.
    fn rowid_alias_column(&self) -> Option<String> {
        if self.without_rowid {
            return None;
        }
        // Table-level `PRIMARY KEY(col)`: a single INTEGER column is a rowid
        // alias regardless of ASC/DESC. Resolve the referenced column name
        // case-insensitively (SQLite identifiers are case-insensitive).
        if !self.table_pk.is_empty() {
            if self.table_pk.len() != 1 {
                return None;
            }
            let name = &self.table_pk[0];
            let col = self
                .columns
                .iter()
                .find(|c| c.name.eq_ignore_ascii_case(name))?;
            return if col.type_name.eq_ignore_ascii_case("INTEGER") {
                Some(col.name.clone())
            } else {
                None
            };
        }
        // Column-level `INTEGER PRIMARY KEY`: exactly `INTEGER`, not `DESC`,
        // single PK column.
        let pk_cols: Vec<&ParsedColumn> = self.columns.iter().filter(|c| c.column_pk).collect();
        if pk_cols.len() != 1 {
            return None;
        }
        let col = pk_cols[0];
        if col.type_name.eq_ignore_ascii_case("INTEGER") && !col.pk_desc {
            Some(col.name.clone())
        } else {
            None
        }
    }

    /// The `AUTOINCREMENT` column, if any. `AUTOINCREMENT` is only valid on an
    /// `INTEGER PRIMARY KEY`.
    fn autoincrement_column(&self) -> Option<String> {
        self.columns
            .iter()
            .find(|c| c.autoincrement)
            .map(|c| c.name.clone())
    }

    /// Whether a declared PK column is guaranteed `NOT NULL`.
    ///
    /// True when the column is explicitly `NOT NULL`, when the table is
    /// `WITHOUT ROWID` (PK columns are implicitly `NOT NULL` there), or when the
    /// column is the integer rowid alias (implicitly `NOT NULL`, whether the
    /// alias is declared column-level or table-level). The column name is
    /// resolved case-insensitively, since SQLite identifiers are.
    fn pk_column_is_not_null(&self, name: &str) -> bool {
        if self.without_rowid {
            return true;
        }
        // The integer rowid alias is implicitly NOT NULL; suppress L2 so it does
        // not double-fire on a column L1 already flagged as IntegerPrimaryKey.
        if let Some(alias) = self.rowid_alias_column() {
            if alias.eq_ignore_ascii_case(name) {
                return true;
            }
        }
        match self
            .columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name))
        {
            Some(col) => col.not_null,
            // A table-level PK naming a column we did not parse: treat as not
            // guaranteed (surface the L2 finding rather than assume safety).
            None => false,
        }
    }
}

/// Extract the parenthesised column-definition body of a `CREATE TABLE`.
fn extract_body(ddl: &str) -> Option<String> {
    let trimmed = ddl.trim_start();
    // Must begin with CREATE ... TABLE. `CREATE TEMP TABLE`, `CREATE TABLE IF
    // NOT EXISTS`, etc. all reach the first `(` the same way.
    let lower = trimmed.to_ascii_lowercase();
    if !lower.starts_with("create") || !lower.contains("table") {
        return None;
    }
    let open = ddl.find('(')?;
    let close = matching_paren(ddl, open)?;
    Some(ddl[open + 1..close].to_string())
}

/// Find the index of the `)` matching the `(` at `open`, respecting quotes.
fn matching_paren(s: &str, open: usize) -> Option<usize> {
    let bytes = s.as_bytes();
    let mut depth = 0usize;
    let mut i = open;
    let mut quote: Option<u8> = None;
    while i < bytes.len() {
        let c = bytes[i];
        match quote {
            Some(q) => {
                if c == q {
                    quote = None;
                }
            }
            None => match c {
                b'\'' | b'"' | b'`' => quote = Some(c),
                b'[' => quote = Some(b']'),
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(i);
                    }
                }
                _ => {}
            },
        }
        i += 1;
    }
    None
}

/// Split a column-definition body on top-level commas, respecting nested parens
/// and quoted identifiers/strings.
fn split_top_level(body: &str) -> Vec<String> {
    let bytes = body.as_bytes();
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut depth = 0usize;
    let mut quote: Option<u8> = None;
    let mut i = 0usize;
    while i < bytes.len() {
        let c = bytes[i];
        match quote {
            Some(q) => {
                if c == q {
                    quote = None;
                }
            }
            None => match c {
                b'\'' | b'"' | b'`' => quote = Some(c),
                b'[' => quote = Some(b']'),
                b'(' => depth += 1,
                b')' => depth = depth.saturating_sub(1),
                b',' if depth == 0 => {
                    out.push(body[start..i].to_string());
                    start = i + 1;
                }
                _ => {}
            },
        }
        i += 1;
    }
    if start < body.len() {
        out.push(body[start..].to_string());
    }
    out
}

/// Tokenize a clause into whitespace-separated words, stripping identifier
/// quoting from the first token and treating a `(...)` group as one token.
fn tokenize(clause: &str) -> Vec<String> {
    let bytes = clause.as_bytes();
    let mut tokens = Vec::new();
    let mut cur = String::new();
    let mut quote: Option<u8> = None;
    let mut depth = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        let c = bytes[i];
        match quote {
            Some(q) => {
                if c == q {
                    quote = None;
                } else {
                    cur.push(c as char);
                }
            }
            None => match c {
                b'\'' | b'"' | b'`' => quote = Some(c),
                b'[' => quote = Some(b']'),
                b'(' => {
                    depth += 1;
                    cur.push('(');
                }
                b')' => {
                    depth = depth.saturating_sub(1);
                    cur.push(')');
                }
                c if c.is_ascii_whitespace() && depth == 0 => {
                    if !cur.is_empty() {
                        tokens.push(std::mem::take(&mut cur));
                    }
                }
                c => cur.push(c as char),
            },
        }
        i += 1;
    }
    if !cur.is_empty() {
        tokens.push(cur);
    }
    tokens
}

/// The keywords that begin a table-level constraint clause.
fn is_table_constraint(clause: &str) -> bool {
    let lower = clause.trim_start().to_ascii_lowercase();
    lower.starts_with("primary key")
        || lower.starts_with("unique")
        || lower.starts_with("check")
        || lower.starts_with("foreign key")
        || lower.starts_with("constraint")
}

/// Parse a table-level `PRIMARY KEY (col, ...)` clause into its column names.
/// Returns `None` when the clause is not a table-level PK.
fn parse_table_pk_constraint(clause: &str) -> Option<Vec<String>> {
    let trimmed = clause.trim_start();
    let lower = trimmed.to_ascii_lowercase();
    // A named constraint: `CONSTRAINT pk PRIMARY KEY (...)`.
    let rest = if lower.starts_with("constraint") {
        let after = trimmed.get("constraint".len()..)?.trim_start();
        let after_lower = after.to_ascii_lowercase();
        let idx = after_lower.find("primary key")?;
        &after[idx..]
    } else if lower.starts_with("primary key") {
        trimmed
    } else {
        return None;
    };

    let open = rest.find('(')?;
    let close = matching_paren(rest, open)?;
    let inner = &rest[open + 1..close];
    let cols = inner
        .split(',')
        .map(|c| strip_identifier(c.trim()))
        // Drop a trailing ASC/DESC/COLLATE on a PK column reference.
        .map(|c| c.split_whitespace().next().unwrap_or("").to_string())
        .filter(|c| !c.is_empty())
        .collect::<Vec<_>>();
    if cols.is_empty() {
        None
    } else {
        Some(cols)
    }
}

/// Parse a single column definition clause.
fn parse_column_def(clause: &str) -> Option<ParsedColumn> {
    let tokens = tokenize(clause);
    if tokens.is_empty() {
        return None;
    }
    let name = tokens[0].clone();

    // The type is the run of tokens after the name, up to the first constraint
    // keyword. `id INTEGER PRIMARY KEY` -> type "INTEGER"; `n UNSIGNED BIG INT
    // NOT NULL` -> type "UNSIGNED BIG INT".
    let upper: Vec<String> = tokens.iter().map(|t| t.to_ascii_uppercase()).collect();
    let constraint_start = (1..tokens.len()).find(|&i| is_constraint_keyword(&upper, i));
    let type_end = constraint_start.unwrap_or(tokens.len());
    let type_name = tokens[1..type_end].join(" ");

    let mut not_null = false;
    let mut column_pk = false;
    let mut pk_desc = false;
    let mut autoincrement = false;

    let mut i = type_end;
    while i < tokens.len() {
        if upper[i] == "NOT" && upper.get(i + 1).map(|s| s.as_str()) == Some("NULL") {
            not_null = true;
            i += 2;
            continue;
        }
        if upper[i] == "PRIMARY" && upper.get(i + 1).map(|s| s.as_str()) == Some("KEY") {
            column_pk = true;
            let mut j = i + 2;
            // `PRIMARY KEY [ASC|DESC] [AUTOINCREMENT]`
            if upper.get(j).map(|s| s.as_str()) == Some("DESC") {
                pk_desc = true;
                j += 1;
            } else if upper.get(j).map(|s| s.as_str()) == Some("ASC") {
                j += 1;
            }
            if upper.get(j).map(|s| s.as_str()) == Some("AUTOINCREMENT") {
                autoincrement = true;
                j += 1;
            }
            i = j;
            continue;
        }
        i += 1;
    }

    Some(ParsedColumn {
        name,
        type_name,
        not_null,
        column_pk,
        pk_desc,
        autoincrement,
    })
}

/// Whether the token at `i` begins a column constraint (so the type run ends).
fn is_constraint_keyword(upper: &[String], i: usize) -> bool {
    matches!(
        upper[i].as_str(),
        "PRIMARY"
            | "NOT"
            | "NULL"
            | "UNIQUE"
            | "CHECK"
            | "DEFAULT"
            | "COLLATE"
            | "REFERENCES"
            | "GENERATED"
            | "AS"
            | "CONSTRAINT"
    )
}

/// Whether the DDL declares `WITHOUT ROWID` after the closing paren.
fn tail_has_without_rowid(ddl: &str) -> bool {
    if let Some(open) = ddl.find('(') {
        if let Some(close) = matching_paren(ddl, open) {
            let tail = ddl[close + 1..].to_ascii_lowercase();
            let tail = tail.replace(['\n', '\t'], " ");
            return tail.contains("without rowid");
        }
    }
    false
}

/// Strip surrounding identifier quoting (`"x"`, `` `x` ``, `[x]`) from a token.
fn strip_identifier(tok: &str) -> String {
    let tok = tok.trim();
    let bytes = tok.as_bytes();
    if bytes.len() >= 2 {
        let (first, last) = (bytes[0], bytes[bytes.len() - 1]);
        let matched = matches!((first, last), (b'"', b'"') | (b'`', b'`') | (b'[', b']'));
        if matched {
            return tok[1..tok.len() - 1].to_string();
        }
    }
    tok.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn issues(findings: &[PkFinding]) -> Vec<PkIssue> {
        findings.iter().map(|f| f.issue).collect()
    }

    // --- L1 shape: the six canonical shapes ---------------------------------

    #[test]
    fn integer_primary_key_is_flagged() {
        let findings = classify_table_ddl("t", "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
        assert_eq!(issues(&findings), vec![PkIssue::IntegerPrimaryKey]);
        // The warning must carry the manual UUIDv7 recipe (unmissable remedy).
        let msg = &findings[0].message;
        assert!(msg.contains("UUIDv7"), "missing UUIDv7 recipe: {msg}");
        assert!(
            msg.contains("foreign key"),
            "recipe must mention FK rewrite"
        );
        assert!(msg.contains("id=5"), "must explain the collision: {msg}");
    }

    #[test]
    fn autoincrement_is_flagged() {
        let findings = classify_table_ddl(
            "t",
            "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)",
        );
        assert_eq!(issues(&findings), vec![PkIssue::Autoincrement]);
        assert!(findings[0].message.contains("UUIDv7"));
        assert!(findings[0].message.contains("AUTOINCREMENT"));
    }

    #[test]
    fn no_primary_key_is_flagged() {
        let findings = classify_table_ddl("t", "CREATE TABLE t (a TEXT, b TEXT)");
        assert_eq!(issues(&findings), vec![PkIssue::NoPrimaryKey]);
        assert!(findings[0].message.contains("UUIDv7"));
        assert!(findings[0].message.contains("rowid"));
    }

    #[test]
    fn text_primary_key_passes() {
        let findings =
            classify_table_ddl("t", "CREATE TABLE t (id TEXT NOT NULL PRIMARY KEY, v TEXT)");
        assert!(findings.is_empty(), "text PK should pass: {findings:?}");
    }

    #[test]
    fn blob_primary_key_passes() {
        let findings =
            classify_table_ddl("t", "CREATE TABLE t (id BLOB NOT NULL PRIMARY KEY, v TEXT)");
        assert!(findings.is_empty(), "blob PK should pass: {findings:?}");
    }

    #[test]
    fn unique_bigint_primary_key_passes() {
        // BIGINT is not the literal `INTEGER` type token, so it is not a rowid
        // alias -- a globally-unique snowflake id passes.
        let findings = classify_table_ddl(
            "t",
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v TEXT)",
        );
        assert!(findings.is_empty(), "bigint PK should pass: {findings:?}");
    }

    #[test]
    fn composite_text_primary_key_passes() {
        let findings = classify_table_ddl(
            "t",
            "CREATE TABLE t (a TEXT NOT NULL, b TEXT NOT NULL, PRIMARY KEY (a, b))",
        );
        assert!(
            findings.is_empty(),
            "composite text PK should pass: {findings:?}"
        );
    }

    // --- The DESC trap ------------------------------------------------------

    #[test]
    fn integer_primary_key_desc_passes() {
        // `INTEGER PRIMARY KEY DESC` is NOT a rowid alias (SQLite treats it
        // differently from `INTEGER PRIMARY KEY`). PRAGMA table_info would
        // report both as type INTEGER identically -- the DESC is only visible in
        // the DDL, which is why we parse it.
        let findings = classify_table_ddl(
            "t",
            "CREATE TABLE t (id INTEGER PRIMARY KEY DESC NOT NULL, v TEXT)",
        );
        assert!(
            findings.is_empty(),
            "INTEGER PRIMARY KEY DESC should pass (DESC trap): {findings:?}"
        );
    }

    // --- table-level single-INTEGER PK is a rowid alias ---------------------

    #[test]
    fn table_level_integer_pk_is_flagged() {
        // `PRIMARY KEY(x)` on an INTEGER column (no NOT NULL) IS a rowid alias
        // (mints per-node 1,2,3, implicitly NOT NULL) -- must flag
        // IntegerPrimaryKey only, not pass and not double-fire NullablePrimaryKey.
        let findings = classify_table_ddl("t", "CREATE TABLE t (x INTEGER, PRIMARY KEY(x))");
        assert_eq!(issues(&findings), vec![PkIssue::IntegerPrimaryKey]);
    }

    #[test]
    fn table_level_integer_pk_asc_is_flagged() {
        // ASC does not change rowid-alias status for the table-level form.
        let findings = classify_table_ddl("t", "CREATE TABLE t (x INTEGER, PRIMARY KEY(x ASC))");
        assert_eq!(issues(&findings), vec![PkIssue::IntegerPrimaryKey]);
    }

    #[test]
    fn table_level_integer_pk_desc_is_flagged() {
        // The DESC-disqualifies quirk applies ONLY to the column-level
        // `INTEGER PRIMARY KEY DESC` form. The table-level `PRIMARY KEY(x DESC)`
        // is still a rowid alias -- a genuine SQLite asymmetry.
        let findings = classify_table_ddl("t", "CREATE TABLE t (x INTEGER, PRIMARY KEY(x DESC))");
        assert_eq!(issues(&findings), vec![PkIssue::IntegerPrimaryKey]);
    }

    #[test]
    fn table_level_integer_pk_case_insensitive_is_flagged() {
        // `PRIMARY KEY(ID)` references column `id` -- SQLite identifiers are
        // case-insensitive. This is a dangerous per-node-sequential PK that must
        // flag IntegerPrimaryKey, and must NOT double-fire NullablePrimaryKey
        // (the integer alias is implicitly NOT NULL).
        let findings = classify_table_ddl(
            "t",
            "CREATE TABLE t (id INTEGER NOT NULL, v, PRIMARY KEY(ID))",
        );
        assert_eq!(issues(&findings), vec![PkIssue::IntegerPrimaryKey]);
    }

    // --- L2 realness --------------------------------------------------------

    #[test]
    fn nullable_text_primary_key_is_flagged() {
        // A single-column TEXT PRIMARY KEY without NOT NULL permits NULL in a
        // rowid table -- not a real identity.
        let findings = classify_table_ddl("t", "CREATE TABLE t (id TEXT PRIMARY KEY, v TEXT)");
        assert_eq!(issues(&findings), vec![PkIssue::NullablePrimaryKey]);
        assert_eq!(findings[0].layer, PkLayer::Realness);
        assert!(findings[0].message.contains("NULL"));
        assert!(findings[0].message.contains("UUIDv7"));
    }

    #[test]
    fn without_rowid_text_pk_passes_l2() {
        // WITHOUT ROWID makes PK columns implicitly NOT NULL.
        let findings = classify_table_ddl(
            "t",
            "CREATE TABLE t (id TEXT PRIMARY KEY, v TEXT) WITHOUT ROWID",
        );
        assert!(
            findings.is_empty(),
            "WITHOUT ROWID text PK is implicitly NOT NULL: {findings:?}"
        );
    }

    // --- L3 stability (guidance) -------------------------------------------

    #[test]
    fn derived_volatile_pk_is_flagged_with_guidance() {
        let findings = classify_table_ddl(
            "games",
            "CREATE TABLE games (game_hash TEXT NOT NULL PRIMARY KEY, v TEXT)",
        );
        assert_eq!(issues(&findings), vec![PkIssue::DerivedVolatile]);
        assert_eq!(findings[0].layer, PkLayer::Stability);
        let msg = &findings[0].message;
        // L3 must name the entropy floor and the stable-identity requirement.
        assert!(
            msg.contains("birthday bet"),
            "L3 must name the entropy floor: {msg}"
        );
        assert!(
            msg.contains("stable identity"),
            "L3 must require keying on stable identity: {msg}"
        );
        assert!(
            msg.contains("100% churn"),
            "L3 must explain the churn: {msg}"
        );
    }

    // --- policy enforcement -------------------------------------------------

    #[test]
    fn warn_policy_proceeds() {
        let findings = classify_table_ddl("t", "CREATE TABLE t (id INTEGER PRIMARY KEY)");
        assert!(!findings.is_empty());
        // 0.5.0: warns but does not refuse.
        assert!(enforce(&findings, PkCheckPolicy::Warn).is_ok());
    }

    #[test]
    fn default_policy_is_warn() {
        assert_eq!(PkCheckPolicy::default(), PkCheckPolicy::Warn);
    }

    #[test]
    fn refuse_policy_rejects() {
        let findings = classify_table_ddl("t", "CREATE TABLE t (id INTEGER PRIMARY KEY)");
        // 0.5.x: the flag flips to hard-refuse (once #280 lands).
        let err = enforce(&findings, PkCheckPolicy::Refuse).unwrap_err();
        // Reuses SyncError::Config per the design build note (exit code 2).
        assert_eq!(err.exit_code(), 2);
    }

    #[test]
    fn refuse_policy_accepts_clean_schema() {
        let findings = classify_table_ddl("t", "CREATE TABLE t (id TEXT NOT NULL PRIMARY KEY)");
        assert!(findings.is_empty());
        assert!(enforce(&findings, PkCheckPolicy::Refuse).is_ok());
    }

    // --- parser robustness --------------------------------------------------

    #[test]
    fn quoted_identifiers_are_handled() {
        let findings = classify_table_ddl(
            "t",
            "CREATE TABLE \"t\" (\"id\" INTEGER PRIMARY KEY, \"references\" TEXT)",
        );
        assert_eq!(issues(&findings), vec![PkIssue::IntegerPrimaryKey]);
    }

    #[test]
    fn if_not_exists_is_parsed() {
        let findings =
            classify_table_ddl("t", "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY)");
        assert_eq!(issues(&findings), vec![PkIssue::IntegerPrimaryKey]);
    }

    #[test]
    fn int_type_is_not_a_rowid_alias() {
        // Only the exact `INTEGER` token aliases rowid; `INT` does not, but a
        // bare `INT PRIMARY KEY` without NOT NULL is still a nullable PK (L2).
        let findings = classify_table_ddl("t", "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)");
        assert!(
            findings.is_empty(),
            "INT (not INTEGER) NOT NULL PK should pass: {findings:?}"
        );
    }

    #[test]
    fn unparseable_ddl_yields_no_findings() {
        assert!(classify_table_ddl("v", "CREATE VIEW v AS SELECT 1").is_empty());
        assert!(classify_table_ddl("t", "not sql at all").is_empty());
    }

    #[test]
    fn autoincrement_takes_precedence_over_integer_pk() {
        // A single finding, the more specific AUTOINCREMENT one.
        let findings =
            classify_table_ddl("t", "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT)");
        assert_eq!(issues(&findings), vec![PkIssue::Autoincrement]);
    }

    // --- native: read-only inspection of a live sqlite_master ---------------

    #[test]
    #[cfg(feature = "native")]
    fn check_schema_flags_integer_pk_from_live_db() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute("CREATE TABLE bad (id INTEGER PRIMARY KEY, v TEXT)", [])
            .unwrap();
        let findings = check_schema(&conn).unwrap();
        assert_eq!(issues(&findings), vec![PkIssue::IntegerPrimaryKey]);
        assert_eq!(findings[0].table, "bad");
    }

    #[test]
    #[cfg(feature = "native")]
    fn check_schema_honors_desc_trap_at_sqlite_master_level() {
        // The build note's core claim: PRAGMA table_info reports INTEGER PRIMARY
        // KEY and INTEGER PRIMARY KEY DESC identically, but sqlite_master.sql
        // preserves the DESC -- so classifying the stored DDL passes the
        // non-alias DESC form. This proves it against a real SQLite, not a
        // hand-written DDL string.
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY DESC NOT NULL, v TEXT)",
            [],
        )
        .unwrap();
        let findings = check_schema(&conn).unwrap();
        assert!(
            findings.is_empty(),
            "INTEGER PRIMARY KEY DESC must pass at the live sqlite_master level: {findings:?}"
        );
    }

    // --- rowid_refusals: the #427 hard-refuse scope --------------------------

    #[test]
    fn rowid_refusals_keeps_integer_primary_key_and_autoincrement() {
        let findings = classify_table_ddl("t", "CREATE TABLE t (id INTEGER PRIMARY KEY)");
        assert_eq!(
            issues(&rowid_refusals(&findings)),
            vec![PkIssue::IntegerPrimaryKey]
        );

        let findings =
            classify_table_ddl("t", "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT)");
        assert_eq!(
            issues(&rowid_refusals(&findings)),
            vec![PkIssue::Autoincrement]
        );
    }

    #[test]
    fn rowid_refusals_drops_no_primary_key_and_l2_l3() {
        // NoPrimaryKey: L1, but not the shape #427 hard-refuses.
        let findings = classify_table_ddl("t", "CREATE TABLE t (a TEXT, b TEXT)");
        assert!(rowid_refusals(&findings).is_empty());

        // Nullable PK: L2, a different failure than "mints a rowid alias".
        let findings = classify_table_ddl("t", "CREATE TABLE t (id TEXT PRIMARY KEY, v TEXT)");
        assert!(rowid_refusals(&findings).is_empty());

        // Derived-volatile: L3, guidance only, never a gate.
        let findings = classify_table_ddl(
            "games",
            "CREATE TABLE games (game_hash TEXT NOT NULL PRIMARY KEY, v TEXT)",
        );
        assert!(rowid_refusals(&findings).is_empty());
    }

    #[test]
    fn rowid_refusals_passes_composite_and_desc_and_clean_schemas() {
        // #413's territory (DESC) and the composite-key acceptance case must
        // both survive the filter untouched -- empty in, empty out.
        let findings = classify_table_ddl(
            "t",
            "CREATE TABLE t (id INTEGER PRIMARY KEY DESC NOT NULL, v TEXT)",
        );
        assert!(rowid_refusals(&findings).is_empty());

        let findings = classify_table_ddl(
            "t",
            "CREATE TABLE t (a TEXT NOT NULL, b TEXT NOT NULL, PRIMARY KEY (a, b))",
        );
        assert!(rowid_refusals(&findings).is_empty());
    }

    // --- column_level_pk_is_desc: the #413 rebuild's own question ------------

    #[test]
    fn column_level_pk_is_desc_true_only_for_the_desc_spelling() {
        // The DESC trap itself: this is #413's exact question.
        assert_eq!(
            column_level_pk_is_desc("CREATE TABLE t (id INTEGER PRIMARY KEY DESC, v TEXT)", "id"),
            Some(true)
        );
        // DESC is column-level, not the alias question: a non-INTEGER column
        // can carry it too, and this function does not care about type --
        // its caller does, gating on `ty.eq_ignore_ascii_case("INTEGER")`
        // before ever asking. A TEXT column's DESC is ordinary index
        // direction, never the rowid-alias hazard #413 fixes, so the
        // rebuild leaves it exactly as `PRAGMA table_info` already did.
        assert_eq!(
            column_level_pk_is_desc("CREATE TABLE t (id TEXT PRIMARY KEY DESC, v TEXT)", "id"),
            Some(true)
        );
    }

    #[test]
    fn column_level_pk_is_desc_false_for_ascending_and_unrelated_columns() {
        assert_eq!(
            column_level_pk_is_desc("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", "id"),
            Some(false)
        );
        assert_eq!(
            column_level_pk_is_desc("CREATE TABLE t (id INTEGER PRIMARY KEY ASC, v TEXT)", "id"),
            Some(false)
        );
        // A different column entirely.
        assert_eq!(
            column_level_pk_is_desc("CREATE TABLE t (id INTEGER PRIMARY KEY DESC, v TEXT)", "v"),
            Some(false)
        );
    }

    #[test]
    fn column_level_pk_is_desc_false_for_table_level_and_composite_forms() {
        // Table-level `PRIMARY KEY(x)` -- even spelled `DESC` there -- is not
        // this function's question: the DESC trap is a column-level-only
        // quirk (see `rowid_alias_column`'s own doc), so a rebuild asking this
        // function for a table-level key gets "not column-level DESC" and
        // keeps re-deriving it exactly as it already did -- ascending, still
        // the alias it already was.
        assert_eq!(
            column_level_pk_is_desc("CREATE TABLE t (x INTEGER, PRIMARY KEY(x))", "x"),
            Some(false)
        );
        assert_eq!(
            column_level_pk_is_desc("CREATE TABLE t (x INTEGER, PRIMARY KEY(x DESC))", "x"),
            Some(false)
        );
        // A composite key's member was never declared with a column-level
        // PRIMARY KEY at all, DESC or otherwise -- collapsing it to a lone
        // survivor is out of this fix's scope (untested either direction), so
        // this must not manufacture a DESC that was never declared.
        assert_eq!(
            column_level_pk_is_desc(
                "CREATE TABLE t (a INTEGER NOT NULL, b TEXT NOT NULL, PRIMARY KEY (a, b))",
                "a"
            ),
            Some(false)
        );
    }

    #[test]
    fn column_level_pk_is_desc_ignores_without_rowid() {
        // A `WITHOUT ROWID` table's key has no rowid to be an alias of, so
        // ASC/DESC there is ordinary index direction, not the alias question
        // -- this function still reports exactly what was declared, in either
        // direction, rather than treating "no rowid to alias" as "must be
        // DESC".
        assert_eq!(
            column_level_pk_is_desc(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) WITHOUT ROWID",
                "id"
            ),
            Some(false)
        );
        assert_eq!(
            column_level_pk_is_desc(
                "CREATE TABLE t (id INTEGER PRIMARY KEY DESC, v TEXT) WITHOUT ROWID",
                "id"
            ),
            Some(true)
        );
    }

    #[test]
    fn column_level_pk_is_desc_none_when_the_ddl_does_not_parse() {
        assert_eq!(
            column_level_pk_is_desc("CREATE VIEW v AS SELECT 1", "id"),
            None
        );
    }

    #[test]
    #[cfg(feature = "native")]
    fn check_schema_is_read_only_and_skips_internal_tables() {
        // AUTOINCREMENT creates the internal sqlite_sequence table; check_schema
        // must skip sqlite_% tables and must not modify anything (read-only).
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)",
            [],
        )
        .unwrap();
        conn.execute("INSERT INTO t (v) VALUES ('a')", []).unwrap();
        let before: i64 = conn
            .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
            .unwrap();
        let findings = check_schema(&conn).unwrap();
        // Only the user table is flagged; the internal sqlite_sequence is skipped.
        assert_eq!(issues(&findings), vec![PkIssue::Autoincrement]);
        let after: i64 = conn
            .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(before, after, "check_schema must not write to the database");
    }
}
