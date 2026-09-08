//! Shared `CREATE TABLE` DDL text scanning.
//!
//! Two callers used to carry their own hand-written scanner for the same
//! question -- "split a `CREATE TABLE`'s parenthesised column-definition list
//! into its top-level items" -- and one was weaker: `pk_check.rs`'s
//! `matching_paren`/`split_top_level` tracked quotes by exiting on the first
//! matching byte, with no doubled-quote lookahead, and had no branch for a
//! `--` or `/* */` comment at all. `migrate::apply`'s `top_level_items`
//! already handled all three (comments, doubled-quote escapes, nested
//! parens). This module is that scanner, generalized to a second caller
//! (smugglr#462).
//!
//! Placed crate-root rather than under `migrate/`, since `pk_check.rs` sits
//! outside the `migrate` module and both need this.
//!
//! Deliberately **ungated** (no `#[cfg(feature = "native")]`): every
//! function reachable from [`top_level_items`] or [`declares_without_rowid`]
//! is a pure `&str` scan with no I/O, and `pk_check::classify_table_ddl` --
//! one of the two callers -- must keep compiling on `wasm32`. A handful of
//! byte-oriented helpers (`skip_ws`, `match_kw`, `ident_end`, `is_ident_byte`)
//! moved here too for the same DRY reason but stay `#[cfg(feature =
//! "native")]`: their only caller, `migrate::apply::splice_create_table_name`,
//! is itself native-only (it rebuilds a live `Connection`'s schema), so
//! ungating them would leave them unreachable -- and therefore `dead_code`
//! under `-D warnings` -- on a `wasm32` build, which cannot see the native
//! code that would otherwise justify them.

/// A byte that can appear inside a bare SQL identifier.
#[cfg(feature = "native")]
pub(crate) fn is_ident_byte(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c == b'$'
}

/// Advance past ASCII whitespace from `i`.
#[cfg(feature = "native")]
pub(crate) fn skip_ws(b: &[u8], mut i: usize) -> usize {
    while i < b.len() && b[i].is_ascii_whitespace() {
        i += 1;
    }
    i
}

/// Match `word` case-insensitively at `i`, requiring a trailing word boundary.
/// Returns the index just past the keyword, or `None` if it does not match.
#[cfg(feature = "native")]
pub(crate) fn match_kw(b: &[u8], i: usize, word: &str) -> Option<usize> {
    let w = word.as_bytes();
    let end = i.checked_add(w.len())?;
    if end > b.len() || !b[i..end].eq_ignore_ascii_case(w) {
        return None;
    }
    if end < b.len() && is_ident_byte(b[end]) {
        return None;
    }
    Some(end)
}

/// The end index (exclusive) of one SQL identifier starting at `i`, honouring
/// `"..."`, `` `...` ``, `[...]`, and bare forms. `None` if `i` is not an
/// identifier start.
#[cfg(feature = "native")]
pub(crate) fn ident_end(b: &[u8], i: usize) -> Option<usize> {
    if i >= b.len() {
        return None;
    }
    match b[i] {
        q @ (b'"' | b'`') => {
            let mut j = i + 1;
            while j < b.len() {
                if b[j] == q {
                    // A doubled quote is an escaped literal, not the terminator.
                    if j + 1 < b.len() && b[j + 1] == q {
                        j += 2;
                    } else {
                        return Some(j + 1);
                    }
                } else {
                    j += 1;
                }
            }
            None
        }
        b'[' => {
            let mut j = i + 1;
            while j < b.len() {
                if b[j] == b']' {
                    return Some(j + 1);
                }
                j += 1;
            }
            None
        }
        c if is_ident_byte(c) && !c.is_ascii_digit() => {
            let mut j = i;
            while j < b.len() && is_ident_byte(b[j]) {
                j += 1;
            }
            Some(j)
        }
        _ => None,
    }
}

/// Strip surrounding identifier quoting (`"x"`, `` `x` ``, `[x]`) from a
/// token. Returns the input unchanged when it is not quoted.
pub(crate) fn strip_identifier(tok: &str) -> &str {
    let tok = tok.trim();
    let bytes = tok.as_bytes();
    if bytes.len() >= 2 {
        let (first, last) = (bytes[0], bytes[bytes.len() - 1]);
        let matched = matches!((first, last), (b'"', b'"') | (b'`', b'`') | (b'[', b']'));
        if matched {
            return &tok[1..tok.len() - 1];
        }
    }
    tok
}

/// Scan the first top-level parenthesised group in `sql`, honouring string
/// literals, quoted identifiers (with doubled-quote escapes), `[...]`
/// quoting inside the group, and `--`/`/* */` comments. Returns the open and
/// close byte indices plus every top-level (depth-1) comma position between
/// them. `None` on unbalanced parens, or when no group is found.
///
/// The single scanning implementation behind both [`top_level_items`] (which
/// also needs the comma positions, to split the group into items) and
/// [`top_level_span`] (which only needs the span, e.g. to find a table-level
/// clause's own nested parenthesised list, or the DDL tail after a `CREATE
/// TABLE`'s column list).
fn scan_top_level_group(sql: &str) -> Option<(usize, usize, Vec<usize>)> {
    let mut depth = 0usize;
    let mut open: Option<usize> = None;
    let mut close: Option<usize> = None;
    let mut splits: Vec<usize> = Vec::new();

    let mut chars = sql.char_indices().peekable();
    while let Some((at, c)) = chars.next() {
        match c {
            // Literals and quoted identifiers are skipped whole: a comma or a
            // parenthesis inside one is text, not structure. A doubled quote
            // escapes, as it does everywhere else in this module.
            '\'' | '"' | '`' => {
                while let Some((_, n)) = chars.next() {
                    if n == c {
                        if chars.peek().map(|(_, p)| *p) == Some(c) {
                            chars.next();
                            continue;
                        }
                        break;
                    }
                }
            }
            '[' if depth > 0 => {
                for (_, n) in chars.by_ref() {
                    if n == ']' {
                        break;
                    }
                }
            }
            '-' if chars.peek().map(|(_, p)| *p) == Some('-') => {
                for (_, n) in chars.by_ref() {
                    if n == '\n' {
                        break;
                    }
                }
            }
            '/' if chars.peek().map(|(_, p)| *p) == Some('*') => {
                chars.next();
                let mut prev = '\0';
                for (_, n) in chars.by_ref() {
                    if prev == '*' && n == '/' {
                        break;
                    }
                    prev = n;
                }
            }
            '(' => {
                depth += 1;
                if depth == 1 {
                    open = Some(at);
                }
            }
            ')' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 && close.is_none() {
                    close = Some(at);
                }
            }
            ',' if depth == 1 => splits.push(at),
            _ => {}
        }
    }

    // An unbalanced list, or none at all, is not something to guess about.
    if depth != 0 {
        return None;
    }
    let (open, close) = (open?, close?);
    Some((open, close, splits))
}

/// The `(open, close)` byte-index span of the first top-level parenthesised
/// group in `sql` -- e.g. a `CREATE TABLE`'s column-definition list, or a
/// single clause's own nested list such as `PRIMARY KEY (a, b)`. `None` when
/// none is found or the parens are unbalanced.
pub(crate) fn top_level_span(sql: &str) -> Option<(usize, usize)> {
    let (open, close, _) = scan_top_level_group(sql)?;
    Some((open, close))
}

/// Whether a `CREATE TABLE` statement declares `WITHOUT ROWID` after the
/// closing paren of its top-level column list -- checked from the tail
/// rather than as a substring scan of the whole statement, which would also
/// match the phrase inside a comment or a quoted default. `false` when `sql`
/// cannot be parsed as a `CREATE TABLE` at all.
pub(crate) fn declares_without_rowid(sql: &str) -> bool {
    let Some((_, close)) = top_level_span(sql) else {
        return false;
    };
    let Some(tail) = sql.get(close + 1..) else {
        return false;
    };
    let tail = tail.to_ascii_lowercase().replace(['\n', '\t'], " ");
    tail.contains("without rowid")
}

/// The verbatim text of each top-level item in a `CREATE TABLE`'s parenthesised
/// list, paired with the identifier it starts with.
///
/// This is how #387 preserves a generated column: `PRAGMA table_xinfo` names one
/// and gives its storage class, but the *generation expression* is in no pragma
/// at all -- `sqlite_master.sql` is the only place `(n * 2)` exists. Taking the
/// column's whole definition verbatim recovers the expression and everything
/// else declared alongside it, without this function needing to understand any
/// of it.
///
/// Returns `None` rather than a guess whenever the text cannot be split
/// confidently: no parenthesised list, an unbalanced one, or an item with no
/// leading identifier. **A wrong answer here produces a table that is broken
/// rather than merely diminished**, so every uncertain case falls back to the
/// caller's existing warn-and-drop behaviour.
///
/// Items include table-level constraints (`PRIMARY KEY (...)`, `FOREIGN KEY
/// (...)`), which start with a keyword rather than a column name. They are
/// returned too and the caller ignores them by looking up only names the pragma
/// called generated -- with a duplicate-name check as the safety valve, since a
/// column named `foreign` would otherwise collide with a `FOREIGN KEY` clause.
pub(crate) fn top_level_items(create_sql: &str) -> Option<Vec<(String, String)>> {
    let (open, close, splits) = scan_top_level_group(create_sql)?;

    let mut items = Vec::new();
    let mut from = open + 1;
    for cut in splits.iter().copied().chain(std::iter::once(close)) {
        let text = create_sql.get(from..cut)?.trim();
        from = cut + 1;
        if text.is_empty() {
            continue;
        }
        items.push((leading_identifier(text)?, text.to_string()));
    }
    Some(items)
}

/// The first identifier-position token of a column definition or constraint.
fn leading_identifier(item: &str) -> Option<String> {
    let mut first = None;
    any_sql_identifier(item, |_quoted, token| {
        first = Some(token.to_string());
        true
    });
    first
}

/// Scan `sql` for identifier-position tokens, calling `f(quoted, token)` on each
/// bare word and each quoted identifier (`"x"`, `` `x` ``, `[x]`). String
/// literals (`'...'`) and comments (`-- ...`, `/* ... */`) are skipped, never
/// reported. Char-based (UTF-8 safe); a doubled quote escapes.
///
/// Returns as soon as `f` returns `true`, reporting whether it ever did. The
/// `quoted` flag is the whole difference between `migrate::apply`'s two keyword
/// scans: `sql_has_autoincrement` wants bare tokens only (a table named
/// `"autoincrement"` must not count), while `sql_mentions_identifier` wants
/// both (a trigger body may write `NEW."email"`).
pub(crate) fn any_sql_identifier(sql: &str, mut f: impl FnMut(bool, &str) -> bool) -> bool {
    let mut chars = sql.chars().peekable();
    let mut bare = String::new();
    let mut quoted = String::new();
    while let Some(c) = chars.next() {
        // A bare identifier runs over ASCII word bytes plus any non-ASCII char
        // (SQLite admits those unquoted).
        if c.is_ascii_alphanumeric() || c == '_' || c == '$' || !c.is_ascii() {
            bare.push(c);
            continue;
        }
        if !bare.is_empty() {
            if f(false, &bare) {
                return true;
            }
            bare.clear();
        }
        match c {
            '\'' => {
                while let Some(n) = chars.next() {
                    if n == '\'' {
                        if chars.peek() == Some(&'\'') {
                            chars.next(); // doubled quote escapes
                            continue;
                        }
                        break;
                    }
                }
            }
            '"' | '`' => {
                quoted.clear();
                while let Some(n) = chars.next() {
                    if n == c {
                        if chars.peek() == Some(&c) {
                            chars.next();
                            quoted.push(c);
                            continue;
                        }
                        break;
                    }
                    quoted.push(n);
                }
                if f(true, &quoted) {
                    return true;
                }
            }
            '[' => {
                quoted.clear();
                for n in chars.by_ref() {
                    if n == ']' {
                        break;
                    }
                    quoted.push(n);
                }
                if f(true, &quoted) {
                    return true;
                }
            }
            '-' if chars.peek() == Some(&'-') => {
                chars.next();
                for n in chars.by_ref() {
                    if n == '\n' {
                        break;
                    }
                }
            }
            '/' if chars.peek() == Some(&'*') => {
                chars.next();
                let mut prev = '\0';
                for n in chars.by_ref() {
                    if prev == '*' && n == '/' {
                        break;
                    }
                    prev = n;
                }
            }
            _ => {}
        }
    }
    !bare.is_empty() && f(false, &bare)
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- top_level_items -----------------------------------------------------

    /// The definition split survives the shapes that break a naive parse.
    ///
    /// Each of these is a case where splitting on commas, or matching
    /// parentheses without tracking quoting, gets a different answer.
    #[test]
    fn top_level_items_survives_commas_and_parens_that_are_not_structure() {
        let sql = "CREATE TABLE t (\n  \
             a INTEGER,\n  \
             b TEXT DEFAULT 'x, (y)',\n  \
             c INTEGER GENERATED ALWAYS AS ((n + 1) * (m - 2)) STORED,\n  \
             d TEXT, -- a trailing comment, with a comma\n  \
             \"e,f\" INTEGER,\n  \
             PRIMARY KEY (a, b)\n\
             )";
        let items = top_level_items(sql).expect("this splits");
        let names: Vec<&str> = items.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, vec!["a", "b", "c", "d", "e,f", "PRIMARY"]);

        let (_, c) = items.iter().find(|(n, _)| n == "c").unwrap();
        assert!(
            c.contains("((n + 1) * (m - 2))"),
            "the nested expression is kept whole: {c}"
        );
    }

    /// An unbalanced or absent list is refused rather than guessed at.
    #[test]
    fn top_level_items_refuses_what_it_cannot_split() {
        assert!(top_level_items("CREATE TABLE t (a INTEGER").is_none());
        assert!(top_level_items("CREATE TABLE t").is_none());
    }

    /// A `(` hidden inside a `--` comment does not throw off the depth count
    /// -- the concrete case smugglr#462 fixes. `pk_check`'s old
    /// `matching_paren` had no comment branch at all, so an unbalanced paren
    /// inside a comment (common in an issue reference like `#123 (see
    /// thread)`) could make the whole statement look unparseable.
    #[test]
    fn top_level_items_ignores_parens_inside_comments() {
        let sql = "CREATE TABLE t (\n  id INTEGER PRIMARY KEY, -- needs (fixing\n  v TEXT\n)";
        let items = top_level_items(sql).expect("the comment must not break the split");
        let names: Vec<&str> = items.iter().map(|(n, _)| n.as_str()).collect();
        // The leading-identifier scan skips the comment on its own (it is
        // `any_sql_identifier`-based), so it lands on "v" -- the item's
        // *verbatim* text still carries the comment, which is what matters
        // for the split itself not derailing.
        assert_eq!(names, vec!["id", "v"]);
        let (_, second) = &items[1];
        assert!(second.contains("-- needs (fixing"));
    }

    /// A doubled double-quote inside a quoted identifier is one escaped
    /// literal quote, not a premature close -- both for splitting (a comma
    /// immediately after the escape is still inside the identifier) and for
    /// the identifier text `leading_identifier` recovers.
    #[test]
    fn top_level_items_recovers_a_doubled_quote_identifier_verbatim() {
        let sql = "CREATE TABLE t (\"na\"\"me\" INTEGER PRIMARY KEY, v TEXT)";
        let items = top_level_items(sql).expect("this splits");
        let names: Vec<&str> = items.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, vec!["na\"me", "v"]);
    }

    // --- any_sql_identifier ---------------------------------------------------

    #[test]
    fn any_sql_identifier_skips_literals_and_comments() {
        assert!(!any_sql_identifier(
            "SELECT 'email', 1 -- email\n /* email */",
            |_quoted, tok| tok.eq_ignore_ascii_case("email")
        ));
        assert!(any_sql_identifier(
            "SELECT NEW.\"email\"",
            |quoted, tok| quoted && tok == "email"
        ));
    }

    // --- strip_identifier -------------------------------------------------

    #[test]
    fn strip_identifier_removes_matching_quoting_only() {
        assert_eq!(strip_identifier("\"a\""), "a");
        assert_eq!(strip_identifier("`a`"), "a");
        assert_eq!(strip_identifier("[a]"), "a");
        assert_eq!(strip_identifier("a"), "a");
        assert_eq!(strip_identifier("\"a"), "\"a");
    }

    // --- declares_without_rowid -----------------------------------------------

    #[test]
    fn declares_without_rowid_reads_the_tail_not_the_whole_statement() {
        assert!(declares_without_rowid(
            "CREATE TABLE t (id TEXT PRIMARY KEY) WITHOUT ROWID"
        ));
        assert!(!declares_without_rowid(
            "CREATE TABLE t (id TEXT PRIMARY KEY)"
        ));
        // A comment mentioning the phrase, inside the column list, must not
        // count -- only the tail after the list does.
        assert!(!declares_without_rowid(
            "CREATE TABLE t (id TEXT PRIMARY KEY -- not WITHOUT ROWID\n)"
        ));
    }

    #[cfg(feature = "native")]
    mod native {
        use super::*;

        #[test]
        fn ident_end_honours_doubled_quote_escapes() {
            let b = b"\"a\"\"b\" rest";
            assert_eq!(ident_end(b, 0), Some(6));
        }

        #[test]
        fn match_kw_requires_a_word_boundary() {
            let b = b"CREATETABLE";
            assert!(match_kw(b, 0, "CREATE").is_none());
            let b = b"CREATE TABLE";
            assert_eq!(match_kw(b, 0, "CREATE"), Some(6));
        }
    }
}
