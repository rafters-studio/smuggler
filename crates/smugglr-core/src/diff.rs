//! Change detection between local and remote databases

use crate::config::ConflictResolution;
use crate::datasource::{DataSource, RowMeta};
use crate::error::Result;
use serde::Serialize;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use tracing::{debug, info, warn};

/// Order two `updated_at` timestamps for conflict resolution.
///
/// Timestamps arrive as strings in three shapes: integer Unix time ("999",
/// "1000"), float Unix time ("2.5", "10.5" -- a REAL column rendered through
/// `f64::to_string` by `extract_updated_at`), and fixed-width ISO-8601 text.
/// Comparing any numeric form lexically is wrong across a digit-count boundary
/// ("1000" sorts before "999"; "10.5" before "2.5"), so parse numerically
/// first: an exact `i64` compare when BOTH sides are integers (preserving
/// precision for large millisecond/nanosecond timestamps that `f64` would
/// round), else an `f64` compare when BOTH sides parse as floats (covering
/// float/float and integer/float pairs). ISO-8601 text is fixed-width, so
/// lexical order == chronological -- fall back to string comparison when
/// NEITHER side is numeric. A numeric-vs-text pair (integer or float on one
/// side, ISO on the other) has no meaningful ordering -- return `None` so the
/// caller routes it to `content_differs` rather than inventing a confident
/// winner from a lexical compare of unlike representations.
/// `pub(crate)` so the multicast apply path can be cross-checked against it
/// (`local.rs::sql_guard_agrees_with_compare_ts_on_same_class_pairs`). The LAN
/// path cannot *call* this -- its comparison rides inside a SQL statement so it
/// is atomic against a concurrent local write -- but the two must agree on every
/// pair the documented precondition admits, and a test is the only thing that
/// keeps them from drifting.
pub(crate) fn compare_ts(a: &str, b: &str) -> Option<Ordering> {
    match (a.parse::<i64>(), b.parse::<i64>()) {
        (Ok(x), Ok(y)) => Some(x.cmp(&y)),
        _ => match (a.parse::<f64>(), b.parse::<f64>()) {
            (Ok(x), Ok(y)) => x.partial_cmp(&y),
            (Err(_), Err(_)) => Some(a.cmp(b)),
            _ => None,
        },
    }
}

/// Per-table diff statistics (counts only, no PK values).
#[derive(Debug, Clone, Serialize)]
pub struct DiffStats {
    pub local_only: usize,
    pub remote_only: usize,
    pub local_newer: usize,
    pub remote_newer: usize,
    pub content_differs: usize,
    pub identical: usize,
}

/// Represents the differences between local and remote for a table
#[derive(Debug, Default)]
pub struct TableDiff {
    /// Table this diff is for (used in conflict warnings).
    pub table: String,
    /// Rows that exist only in local
    pub local_only: Vec<String>,
    /// Rows that exist only in remote
    pub remote_only: Vec<String>,
    /// Rows where local is newer (by timestamp)
    pub local_newer: Vec<String>,
    /// Rows where remote is newer (by timestamp)
    pub remote_newer: Vec<String>,
    /// Rows that differ but have no timestamp (use content hash)
    pub content_differs: Vec<String>,
    /// Rows that are identical
    pub identical: Vec<String>,
}

impl TableDiff {
    pub fn new(table: &str) -> Self {
        Self {
            table: table.to_string(),
            ..Default::default()
        }
    }

    /// Check if there are any differences
    pub fn has_changes(&self) -> bool {
        !self.local_only.is_empty()
            || !self.remote_only.is_empty()
            || !self.local_newer.is_empty()
            || !self.remote_newer.is_empty()
            || !self.content_differs.is_empty()
    }

    /// Get rows that should be pushed (local -> remote)
    pub fn rows_to_push(&self, conflict_resolution: ConflictResolution) -> Vec<String> {
        let mut rows = Vec::new();
        rows.extend(self.local_only.clone());
        rows.extend(self.local_newer.clone());

        // local_wins pushes the local side of a content conflict. The other
        // policies pull it (remote_wins) or skip it (newer/uuid). The "skipped"
        // warning is emitted once per diff via `warn_unresolved_conflicts`, not
        // here -- this accessor stays pure (no logging side effect).
        if matches!(conflict_resolution, ConflictResolution::LocalWins) {
            rows.extend(self.content_differs.clone());
        }

        rows
    }

    /// Get rows that should be pulled (remote -> local)
    pub fn rows_to_pull(&self, conflict_resolution: ConflictResolution) -> Vec<String> {
        let mut rows = Vec::new();
        rows.extend(self.remote_only.clone());
        rows.extend(self.remote_newer.clone());

        // remote_wins pulls the remote side of a content conflict; the others
        // push it (local_wins) or skip it (newer/uuid). Pure accessor; see
        // `warn_unresolved_conflicts` for the skipped-rows warning.
        if matches!(conflict_resolution, ConflictResolution::RemoteWins) {
            rows.extend(self.content_differs.clone());
        }

        rows
    }

    /// Content-differing rows this policy leaves UNRESOLVED -- skipped in both
    /// directions because there's no usable tiebreaker. `newer_wins` skips
    /// same-timestamp conflicts; `local_wins` / `remote_wins` always resolve
    /// them, so none are unresolved.
    pub fn unresolved_conflicts(&self, conflict_resolution: ConflictResolution) -> &[String] {
        match conflict_resolution {
            ConflictResolution::NewerWins => &self.content_differs,
            ConflictResolution::LocalWins | ConflictResolution::RemoteWins => &[],
        }
    }

    /// Warn once about content-differing rows skipped under the conflict policy.
    ///
    /// Call this at each diff-creation site (not inside the row accessors) so the
    /// warning fires exactly once per table in every direction -- including a
    /// pull-only run, which previously got no warning at all.
    pub fn warn_unresolved_conflicts(&self, conflict_resolution: ConflictResolution) {
        let count = self.unresolved_conflicts(conflict_resolution).len();
        if count == 0 {
            return;
        }
        // count > 0 implies a skipping policy (unresolved_conflicts is empty for
        // local_wins/remote_wins); pick the reason, sharing one warning template.
        let reason = match conflict_resolution {
            ConflictResolution::NewerWins => {
                "missing or incomparable timestamps (skipped under newer_wins)"
            }
            ConflictResolution::LocalWins | ConflictResolution::RemoteWins => return,
        };
        warn!(
            "{} row(s) in '{}' have different content but {}. \
             Use local_wins or remote_wins to resolve.",
            count, self.table, reason
        );
    }

    /// Compute aggregate diff statistics (counts only).
    pub fn stats(&self) -> DiffStats {
        DiffStats {
            local_only: self.local_only.len(),
            remote_only: self.remote_only.len(),
            local_newer: self.local_newer.len(),
            remote_newer: self.remote_newer.len(),
            content_differs: self.content_differs.len(),
            identical: self.identical.len(),
        }
    }

    /// Summary string for display
    pub fn summary(&self) -> String {
        let mut parts = Vec::new();

        if !self.local_only.is_empty() {
            parts.push(format!("+{} local only", self.local_only.len()));
        }
        if !self.remote_only.is_empty() {
            parts.push(format!("+{} remote only", self.remote_only.len()));
        }
        if !self.local_newer.is_empty() {
            parts.push(format!("{} local newer", self.local_newer.len()));
        }
        if !self.remote_newer.is_empty() {
            parts.push(format!("{} remote newer", self.remote_newer.len()));
        }
        if !self.content_differs.is_empty() {
            parts.push(format!("{} content differs", self.content_differs.len()));
        }
        if !self.identical.is_empty() {
            parts.push(format!("{} identical", self.identical.len()));
        }

        if parts.is_empty() {
            "no data".to_string()
        } else {
            parts.join(", ")
        }
    }
}

/// Classify row-level differences between two pre-fetched metadata maps.
///
/// Pure function with no I/O: partitions primary keys into local-only,
/// remote-only, newer-on-each-side, content-differs, and identical buckets.
/// Used by [`diff_table`] after fetching metadata, and by the WASM package's
/// cached diff path which bypasses per-call full scans.
///
/// `hash_covers_synced_columns` says whether the content hash was computed over
/// every column that actually SYNCS -- deliberately not "every column in the
/// row". A table with only [`crate::config::SyncConfig::exclude_columns`] set
/// passes `true`, because those columns are stripped before transfer and so are
/// not synced at all: the hash covers everything that crosses the wire, which is
/// the property the skip condition needs. Pass `false` when the table has
/// [`crate::config::SyncConfig::converge_columns`] configured: those columns are
/// omitted from the hash but still transferred, so a hash match no longer proves
/// the rows are equal and cannot be used as the skip condition (#293). Pass
/// `true` when the hash covers everything that crosses the wire, which is the
/// historical behavior and stays the fast path.
pub fn classify_diff(
    local_meta: &HashMap<String, RowMeta>,
    remote_meta: &HashMap<String, RowMeta>,
    table: &str,
    hash_covers_synced_columns: bool,
) -> TableDiff {
    let local_keys: HashSet<&String> = local_meta.keys().collect();
    let remote_keys: HashSet<&String> = remote_meta.keys().collect();

    let mut diff = TableDiff::new(table);

    for pk in local_keys.difference(&remote_keys) {
        diff.local_only.push((*pk).clone());
    }

    for pk in remote_keys.difference(&local_keys) {
        diff.remote_only.push((*pk).clone());
    }

    for pk in local_keys.intersection(&remote_keys) {
        let local_row = &local_meta[*pk];
        let remote_row = &remote_meta[*pk];

        let hashes_match = local_row.content_hash == remote_row.content_hash;

        if hashes_match && hash_covers_synced_columns {
            diff.identical.push((*pk).clone());
            continue;
        }

        if hashes_match {
            // Hash match, but the hash does not cover every synced column, so it
            // is not proof of equality -- an edit confined to a converge column
            // lands here. Order by timestamp instead of skipping (#293).
            //
            // "Cannot compare" and "compared equal" are DIFFERENT answers here,
            // and the sibling branch below collapses them only because both of
            // its outcomes are the same bucket. This branch must keep them apart.
            //
            // A genuine tie (`Ordering::Equal`) resolves to `identical`. Equal
            // hashes mean the hashed columns agree, equal timestamps mean neither
            // side is newer, and the overwhelmingly common case reaching here is
            // a row that is simply unchanged. Routing that to `content_differs`
            // would flag EVERY unchanged row on a converge-column table as a
            // conflict, on every sync, forever -- unusable.
            //
            // `None` is not a tie. It means `compare_ts` refused: the two sides
            // carry unlike representations (one integer Unix time, one ISO-8601
            // text) and there is no meaningful ordering between them. Treating
            // that as `identical` would silently drop a real converge-column edit
            // whenever a deployment has mixed timestamp formats -- which is
            // exactly the failure class #293 exists to kill. It goes to
            // `content_differs`, which `warn_unresolved_conflicts` surfaces and a
            // conflict policy can resolve, matching the sibling branch's
            // treatment of the same `None`.
            match (&local_row.updated_at, &remote_row.updated_at) {
                (Some(local_ts), Some(remote_ts)) => match compare_ts(local_ts, remote_ts) {
                    Some(Ordering::Greater) => diff.local_newer.push((*pk).clone()),
                    Some(Ordering::Less) => diff.remote_newer.push((*pk).clone()),
                    Some(Ordering::Equal) => diff.identical.push((*pk).clone()),
                    // Unlike representations -- unorderable, not equal.
                    None => diff.content_differs.push((*pk).clone()),
                },
                // Exactly one side carries a timestamp. Also unorderable, and
                // anomalous rather than routine, so it is surfaced rather than
                // dropped.
                (Some(_), None) | (None, Some(_)) => diff.content_differs.push((*pk).clone()),
                // Neither side has a timestamp: this table has no ordering signal
                // at all, so converge columns cannot be reconciled on it by any
                // means. Every row would otherwise land in `content_differs` on
                // every sync, so they stay `identical` -- a documented limitation
                // of configuring converge_columns on a table with no usable
                // timestamp_column, not a judgement that the rows agree.
                //
                // Reviewed and kept deliberately: unlike the two silent-loss bugs
                // this branch was fixed for, this one cannot fire on a correctly
                // configured table. It requires converge_columns on a table where
                // `timestamp_column` resolves for no row at all -- i.e. where the
                // reconciliation mechanism the feature depends on is absent. And
                // `content_differs` would not rescue that table either: under
                // local_wins/remote_wins it churn-retransfers every row forever,
                // under newer_wins it moves nothing, which is the
                // same practical outcome as today with a warning attached.
                //
                // The genuine gap is that the state is SILENT -- nothing tells an
                // operator that converge_columns is configured but inert here.
                // Making it observable needs a once-per-table signal, and
                // `classify_diff` is documented as pure with no I/O, so it does
                // not belong at this line. Tracked rather than bolted on.
                (None, None) => diff.identical.push((*pk).clone()),
            }
            continue;
        }

        match (&local_row.updated_at, &remote_row.updated_at) {
            (Some(local_ts), Some(remote_ts)) => match compare_ts(local_ts, remote_ts) {
                Some(Ordering::Greater) => diff.local_newer.push((*pk).clone()),
                Some(Ordering::Less) => diff.remote_newer.push((*pk).clone()),
                // Equal timestamps or unlike representations (one numeric, one
                // text): no safe tiebreaker, so treat as a content conflict.
                Some(Ordering::Equal) | None => diff.content_differs.push((*pk).clone()),
            },
            _ => {
                diff.content_differs.push((*pk).clone());
            }
        }
    }

    diff
}

/// Compare two data sources for a table.
///
/// `converge_columns` are omitted from the content hash but still synced, so
/// their presence turns off the hash-match skip -- see [`classify_diff`] and
/// [`crate::config::SyncConfig::converge_columns`] (#293).
pub async fn diff_table<A: DataSource, B: DataSource>(
    local: &A,
    remote: &B,
    table: &str,
    timestamp_column: &str,
    exclude_columns: &[String],
    converge_columns: &[String],
) -> Result<TableDiff> {
    info!("Computing diff for table: {}", table);

    // Both sides must hash the SAME column set or identical rows produce
    // different hashes and never converge, so the union is computed once here
    // and used for both fetches.
    let hash_excluded = crate::config::hash_excluded_columns(exclude_columns, converge_columns);

    let local_meta = local
        .get_row_metadata(table, timestamp_column, &hash_excluded)
        .await?;
    let remote_meta = remote
        .get_row_metadata(table, timestamp_column, &hash_excluded)
        .await?;

    let diff = classify_diff(
        &local_meta,
        &remote_meta,
        table,
        converge_columns.is_empty(),
    );

    debug!(
        "Diff for {}: local_only={}, remote_only={}, local_newer={}, remote_newer={}, content_differs={}, identical={}",
        table,
        diff.local_only.len(),
        diff.remote_only.len(),
        diff.local_newer.len(),
        diff.remote_newer.len(),
        diff.content_differs.len(),
        diff.identical.len()
    );

    Ok(diff)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_newer_wins_push_includes_local_only_and_newer() {
        let diff = TableDiff {
            table: "items".to_string(),
            local_only: vec!["pk1".to_string()],
            remote_only: vec!["pk2".to_string()],
            local_newer: vec!["pk3".to_string()],
            remote_newer: vec!["pk4".to_string()],
            content_differs: vec![],
            identical: vec![],
        };

        let push = diff.rows_to_push(ConflictResolution::NewerWins);
        assert_eq!(push.len(), 2);
        assert!(push.contains(&"pk1".to_string()));
        assert!(push.contains(&"pk3".to_string()));

        let pull = diff.rows_to_pull(ConflictResolution::NewerWins);
        assert_eq!(pull.len(), 2);
        assert!(pull.contains(&"pk2".to_string()));
        assert!(pull.contains(&"pk4".to_string()));
    }

    #[test]
    fn test_newer_wins_content_differs_skipped() {
        let diff = TableDiff {
            table: "items".to_string(),
            local_only: vec![],
            remote_only: vec![],
            local_newer: vec![],
            remote_newer: vec![],
            content_differs: vec!["pk1".to_string()],
            identical: vec![],
        };

        assert!(diff.rows_to_push(ConflictResolution::NewerWins).is_empty());
        assert!(diff.rows_to_pull(ConflictResolution::NewerWins).is_empty());
    }

    #[test]
    fn unresolved_conflicts_tracks_only_newer() {
        let diff = TableDiff {
            table: "t".to_string(),
            local_only: vec![],
            remote_only: vec![],
            local_newer: vec![],
            remote_newer: vec![],
            content_differs: vec!["a".to_string(), "b".to_string()],
            identical: vec![],
        };
        // newer_wins leaves same-content conflicts unresolved (skipped both ways)
        assert_eq!(
            diff.unresolved_conflicts(ConflictResolution::NewerWins)
                .len(),
            2
        );
        // local/remote always resolve them -> none unresolved
        assert!(diff
            .unresolved_conflicts(ConflictResolution::LocalWins)
            .is_empty());
        assert!(diff
            .unresolved_conflicts(ConflictResolution::RemoteWins)
            .is_empty());
        // Regression for #144: the skip is symmetric -- a pull-only run under
        // newer_wins also skips the content-differs rows (the warning is now
        // emitted at the diff-creation site, not only on the push path).
        assert!(diff.rows_to_pull(ConflictResolution::NewerWins).is_empty());
        assert!(diff.rows_to_push(ConflictResolution::NewerWins).is_empty());
        // local_wins pushes them; remote_wins pulls them.
        assert_eq!(diff.rows_to_push(ConflictResolution::LocalWins).len(), 2);
        assert_eq!(diff.rows_to_pull(ConflictResolution::RemoteWins).len(), 2);
    }

    // --- Integer-timestamp conflict resolution (#176 / #177) ---

    fn one(hash: &str, ts: Option<&str>) -> HashMap<String, RowMeta> {
        let mut m = HashMap::new();
        m.insert(
            "r".to_string(),
            RowMeta {
                pk_value: "r".to_string(),
                updated_at: ts.map(String::from),
                content_hash: hash.to_string(),
            },
        );
        m
    }

    // --- converge_columns: a hash match is not proof of equality (#293) ---

    // The literal scenario from #293: two rows, same PK, both name=Alice (in the
    // hash), differing only in an excluded `email` and the timestamp. Because
    // `email` is omitted from the content hash, BOTH SIDES HASH IDENTICALLY --
    // that is the whole bug. The old skip-on-hash-match dropped the newer local
    // row silently: no error, no conflict, no transfer, and `identical` is not a
    // bucket anyone inspects.
    #[test]
    fn converge_column_edit_is_not_dropped_on_hash_match() {
        // Same hash on both sides: `email` never entered it.
        let local = one("same-hash", Some("200"));
        let remote = one("same-hash", Some("100"));

        let diff = classify_diff(&local, &remote, "t", false);

        assert_eq!(
            diff.local_newer,
            vec!["r".to_string()],
            "a newer row differing only in a converge column must win, not be skipped"
        );
        assert!(
            diff.identical.is_empty(),
            "the row must not be classified identical -- that is the data loss"
        );
        // And it actually moves: classification alone is not the fix.
        assert!(diff
            .rows_to_push(ConflictResolution::NewerWins)
            .contains(&"r".to_string()));
    }

    // The same inputs under the historical flag must still skip. This is what
    // keeps the fix scoped: a table with no converge_columns configured pays
    // nothing and behaves exactly as before.
    #[test]
    fn hash_match_still_skips_when_hash_covers_the_row() {
        let local = one("same-hash", Some("200"));
        let remote = one("same-hash", Some("100"));

        let diff = classify_diff(&local, &remote, "t", true);

        assert_eq!(diff.identical, vec!["r".to_string()]);
        assert!(diff.local_newer.is_empty());
    }

    // A tie resolves to `identical`, NOT `content_differs`. Equal hashes mean the
    // hashed columns agree, so only unhashed columns could differ, and equal
    // timestamps give no basis to prefer either side. Calling it a conflict would
    // park the row in `content_differs` on every sync forever -- permanent
    // warning noise for two rows we have no evidence differ at all.
    #[test]
    fn converge_column_tie_is_identical_not_a_conflict() {
        let local = one("same-hash", Some("100"));
        let remote = one("same-hash", Some("100"));

        let diff = classify_diff(&local, &remote, "t", false);

        assert_eq!(diff.identical, vec!["r".to_string()]);
        assert!(
            diff.content_differs.is_empty(),
            "an unorderable tie on equal hashes is not a conflict"
        );
    }

    // Neither side has a timestamp: the table has no ordering signal at all, so
    // converge columns cannot be reconciled on it by any means. Must not
    // manufacture a conflict for every row.
    #[test]
    fn converge_column_without_timestamps_is_identical() {
        let local = one("same-hash", None);
        let remote = one("same-hash", None);

        let diff = classify_diff(&local, &remote, "t", false);

        assert_eq!(diff.identical, vec!["r".to_string()]);
        assert!(diff.content_differs.is_empty());
    }

    // Review finding (#321): `compare_ts` returning None is "unorderable", NOT
    // "equal". Mixed representations -- integer Unix time on one side, ISO-8601
    // text on the other -- previously fell into the same arm as a genuine tie and
    // were classified `identical`, silently dropping a real converge-column edit.
    // That is the #293 failure class re-entering through the fix for it.
    #[test]
    fn converge_column_unorderable_timestamps_are_a_conflict_not_identical() {
        let local = one("same-hash", Some("1700000000"));
        let remote = one("same-hash", Some("2023-11-14T22:13:20Z"));

        let diff = classify_diff(&local, &remote, "t", false);

        assert_eq!(
            diff.content_differs,
            vec!["r".to_string()],
            "unlike timestamp representations are unorderable and must surface, not vanish"
        );
        assert!(
            diff.identical.is_empty(),
            "classifying an unorderable pair as identical is silent data loss"
        );
        // And it is actually visible: content_differs is the bucket
        // warn_unresolved_conflicts reports under a skipping policy.
        assert_eq!(
            diff.unresolved_conflicts(ConflictResolution::NewerWins),
            &["r".to_string()]
        );
    }

    // Asymmetric presence is also unorderable, and anomalous rather than routine.
    #[test]
    fn converge_column_one_sided_timestamp_is_a_conflict() {
        for (local_ts, remote_ts) in [(Some("200"), None), (None, Some("200"))] {
            let local = one("same-hash", local_ts);
            let remote = one("same-hash", remote_ts);

            let diff = classify_diff(&local, &remote, "t", false);

            assert_eq!(
                diff.content_differs,
                vec!["r".to_string()],
                "one-sided timestamp ({local_ts:?} vs {remote_ts:?}) must surface"
            );
            assert!(diff.identical.is_empty());
        }
    }

    // Differing hashes must keep their existing classification regardless of the
    // flag -- the new branch is reachable only on a hash MATCH.
    #[test]
    fn differing_hashes_are_unaffected_by_the_converge_flag() {
        let local = one("A", Some("200"));
        let remote = one("B", Some("100"));

        for hash_covers_synced_columns in [true, false] {
            let diff = classify_diff(&local, &remote, "t", hash_covers_synced_columns);
            assert_eq!(
                diff.local_newer,
                vec!["r".to_string()],
                "hash_covers_synced_columns={hash_covers_synced_columns} must not change a differing-hash row"
            );
        }
    }

    // #177: an integer-timestamp row whose content changed must be classified by
    // direction and actually sync -- NOT dropped into content_differs (which is
    // skipped in both directions under newer_wins). This is the literal symptom
    // the bug produced once both sides finally carry the integer timestamp.
    #[test]
    fn integer_timestamp_changed_row_syncs_not_skipped() {
        // remote newer (larger unix ts), different content
        let local = one("A", Some("1700000000"));
        let remote = one("B", Some("1700000100"));
        let diff = classify_diff(&local, &remote, "t", true);

        assert_eq!(diff.remote_newer, vec!["r".to_string()]);
        assert!(diff.content_differs.is_empty());
        // ...and it is actually pulled under newer_wins, not silently skipped.
        assert!(diff
            .rows_to_pull(ConflictResolution::NewerWins)
            .contains(&"r".to_string()));
        assert!(diff.rows_to_push(ConflictResolution::NewerWins).is_empty());
    }

    // #176: integer timestamps straddling a digit-count boundary. Lexically
    // "1000" < "999" (wrong); numerically 1000 > 999. The newer row (local) must
    // win and be pushed -- the pre-fix code picked the OLDER remote row.
    #[test]
    fn integer_timestamp_lexicographic_boundary_orders_numerically() {
        let local = one("A", Some("1000")); // newer
        let remote = one("B", Some("999")); // older
        let diff = classify_diff(&local, &remote, "t", true);

        assert_eq!(diff.local_newer, vec!["r".to_string()]);
        assert!(diff.remote_newer.is_empty());
        assert!(diff
            .rows_to_push(ConflictResolution::NewerWins)
            .contains(&"r".to_string()));
    }

    // ISO-8601 text timestamps are fixed-width -> lexical order is chronological.
    // Must keep working unchanged (they never enter the numeric branch).
    #[test]
    fn iso8601_timestamps_still_compare_lexically() {
        let local = one("A", Some("2023-06-01T00:00:01Z")); // newer
        let remote = one("B", Some("2023-06-01T00:00:00Z"));
        let diff = classify_diff(&local, &remote, "t", true);
        assert_eq!(diff.local_newer, vec!["r".to_string()]);
    }

    // Mixed representation (one integer, one ISO text for the same column --
    // reachable via schema skew between local and remote). There is no honest
    // ordering, so route to content_differs rather than invent a confident
    // lexical winner ("2023-..." vs "1700..." would always pick local).
    #[test]
    fn mixed_timestamp_representation_is_content_differs() {
        let local = one("A", Some("1700000000")); // integer
        let remote = one("B", Some("2023-06-01T00:00:00Z")); // ISO text
        let diff = classify_diff(&local, &remote, "t", true);
        assert_eq!(diff.content_differs, vec!["r".to_string()]);
        assert!(diff.local_newer.is_empty());
        assert!(diff.remote_newer.is_empty());
    }

    // Equal integer timestamps with differing content: no tiebreaker -> conflict.
    #[test]
    fn equal_integer_timestamps_are_content_differs() {
        let local = one("A", Some("1700000000"));
        let remote = one("B", Some("1700000000"));
        let diff = classify_diff(&local, &remote, "t", true);
        assert_eq!(diff.content_differs, vec!["r".to_string()]);
    }

    // --- Float-serialized timestamps (#241) ---

    // A REAL/float timestamp column renders through f64::to_string ("2.5",
    // "10.5"). Pre-fix compare_ts fell straight to lexical for any non-i64 pair,
    // so "10.5" sorted BEFORE "2.5" (byte '1' < '2') -- the reverse of the true
    // chronological order. Pin the numeric ordering directly.
    #[test]
    fn compare_ts_orders_float_timestamps_numerically() {
        assert_eq!(compare_ts("2.5", "10.5"), Some(Ordering::Less));
        assert_eq!(compare_ts("10.5", "2.5"), Some(Ordering::Greater));
        assert_eq!(compare_ts("2.5", "2.5"), Some(Ordering::Equal));
    }

    // Schema skew: one row's column reads as INTEGER, the other as REAL for the
    // same logical timestamp. Both are numeric, so they compare numerically
    // (this pair returned None pre-fix -- now an honest ordering, not a conflict).
    #[test]
    fn compare_ts_orders_integer_against_float_numerically() {
        assert_eq!(compare_ts("1000", "1000.5"), Some(Ordering::Less));
        assert_eq!(compare_ts("1001", "1000.5"), Some(Ordering::Greater));
    }

    // classify_diff over float timestamps: the larger float is newer and must be
    // classified by direction (and actually sync), not dropped to content_differs.
    // Pre-fix the lexical compare put the newer "10.5" row in remote_newer.
    #[test]
    fn float_timestamp_newer_row_syncs_not_skipped() {
        let local = one("A", Some("10.5")); // newer
        let remote = one("B", Some("2.5")); // older
        let diff = classify_diff(&local, &remote, "t", true);
        assert_eq!(diff.local_newer, vec!["r".to_string()]);
        assert!(diff.remote_newer.is_empty());
        assert!(diff.content_differs.is_empty());
        assert!(diff
            .rows_to_push(ConflictResolution::NewerWins)
            .contains(&"r".to_string()));
    }
}
