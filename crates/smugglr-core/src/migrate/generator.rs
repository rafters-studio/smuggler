//! Rails-style migration generator: parse a terse column-spec grammar into a
//! structured [`Manifest`](crate::migrate::Manifest).
//!
//! This is the developer front door -- the drizzle-kit replacement. It takes a
//! Rails-shaped invocation (`create_contacts id:pk address_id:fk email:text:pii`)
//! and emits a **structured op manifest** (#271), never raw SQL: the ops are the
//! contract, and dialect SQL is generated from them later at apply time (#273).
//!
//! Grammar, per column token: `name[:type][:modifier...]`.
//! - Default type is `text`; explicit types are `text` / `int` / `real` / `blob`.
//! - Modifiers: `pk`, `fk` (target inferred by naming convention --
//!   `address_id -> address(id)`), `unique`, `notnull`, `index`, `default=<v>`,
//!   `pii` (carried verbatim into [`Column::tags`](crate::migrate::Column) --
//!   smugglr never *infers* a classification, it only records the author's
//!   declaration), and `range` (lowered to a `CHECK` constraint).
//!
//! Each generated op self-declares its op-class via
//! [`ClassifiedOp::new`](crate::migrate::ClassifiedOp::new) (the honest,
//! machine-derived class), and the reverse is auto-derived for the additive
//! create-ops the generator emits (`create_table -> drop_table`,
//! `create_index -> drop_index`).
//!
//! 0.5.0 supports only the create-table form (`create_<table>`); the Rails-style
//! `AddXToY` alter form is deferred (see the issue's open question).

use crate::migrate::{ClassifiedOp, Column, ColumnKind, Constraint, Flags, Manifest, Op, OpClass};
use thiserror::Error;

/// A failure parsing the column-spec grammar.
///
/// Kept as its own error type (rather than a new [`MigrateError`](crate::migrate::MigrateError)
/// variant) so the generator is self-contained; the CLI maps it to a
/// configuration-class error (exit code 2 -- fix the input, do not retry).
#[derive(Error, Debug, PartialEq, Eq)]
pub enum GeneratorError {
    /// The migration name was empty.
    #[error("empty migration name")]
    EmptyName,
    /// The migration name is not a supported form. 0.5.0 supports only
    /// `create_<table>`; the alter form (`add_x_to_y`) is deferred.
    #[error(
        "unsupported migration name '{0}': 0.5.0 supports only the create-table \
         form 'create_<table>' (the alter form is deferred)"
    )]
    UnsupportedName(String),
    /// No column specs were given.
    #[error("migration '{0}' has no columns")]
    NoColumns(String),
    /// A column spec was the empty string.
    #[error("empty column spec")]
    EmptyColumnSpec,
    /// A column spec had no name before its first `:`.
    #[error("column spec '{0}' has no column name")]
    EmptyColumnName(String),
    /// A modifier token is not part of the grammar.
    #[error("unknown modifier '{modifier}' on column '{column}'")]
    UnknownModifier { column: String, modifier: String },
    /// A no-argument modifier (`pk`/`fk`/`unique`/`notnull`/`index`/`pii`) was
    /// given an `=value` suffix, which it does not accept.
    #[error("modifier '{modifier}' on column '{column}' does not take a value")]
    UnexpectedModifierValue { column: String, modifier: String },
    /// A column name is not a safe SQL identifier and so cannot be interpolated
    /// into a generated `CHECK` expression.
    #[error(
        "column name '{0}' is not a safe SQL identifier (expected \
         [A-Za-z_][A-Za-z0-9_]*)"
    )]
    UnsafeColumnName(String),
    /// An `fk` modifier could not infer its target table from the column name.
    #[error(
        "cannot infer foreign-key target for column '{0}': the name must end in \
         '_id' (e.g. address_id -> address(id))"
    )]
    FkInferenceFailed(String),
    /// A `default` modifier was given without a value.
    #[error("'default' modifier on column '{0}' requires a value (default=<value>)")]
    DefaultMissingValue(String),
    /// A `range` modifier was applied to a non-numeric column.
    #[error("'range' modifier on column '{column}' is only valid on int/real columns, not {kind}")]
    RangeOnNonNumeric { column: String, kind: &'static str },
    /// A `range=LOW..HIGH` argument was malformed.
    #[error("invalid range bounds '{bounds}' on column '{column}': expected LOW..HIGH with numeric bounds")]
    InvalidRangeBounds { column: String, bounds: String },
    /// The declared primary key mints the SQLite rowid alias (or explicit
    /// `AUTOINCREMENT`) -- forbidden by smugglr's own primary-key requirement
    /// (see `crate::pk_check`): a per-node sequential key collides across
    /// nodes on a masterless fabric. Unlike an existing database (#280, a
    /// different issue), this is new DDL `migrate new` is about to mint, so
    /// there is an in-tool remedy -- declare the column `id:pk` (TEXT)
    /// instead -- and this refuses rather than warns (#427).
    #[error("{0}")]
    RowidPrimaryKey(String),
}

/// Parse a Rails-style migration invocation into a [`Manifest`].
///
/// `name` is the migration name (e.g. `create_contacts`, from which the table
/// name is derived); `specs` are the column tokens (`id:pk`, `email:text:pii`,
/// ...). The returned manifest carries the forward ops (each with its
/// self-declared op-class), the auto-derived reverse, and honest reversibility
/// flags.
pub fn generate(name: &str, specs: &[String]) -> Result<Manifest, GeneratorError> {
    let table = table_from_name(name)?;
    if specs.is_empty() {
        return Err(GeneratorError::NoColumns(name.to_string()));
    }

    let mut columns = Vec::with_capacity(specs.len());
    let mut indexed = Vec::new();
    for spec in specs {
        let parsed = parse_column(spec)?;
        if parsed.index {
            indexed.push(parsed.column.name.clone());
        }
        columns.push(parsed.column);
    }

    // Forward ops: the table, then one index per column that asked for one.
    let mut up = Vec::with_capacity(1 + indexed.len());
    up.push(ClassifiedOp::new(Op::CreateTable {
        table: table.clone(),
        columns,
        without_rowid: false,
    }));

    // Reverse ops, built inline (never via reverse.rs -- #274 owns the
    // apply-time rollback engine; the generator only needs the structural
    // inverse of the additive create-ops it emits). Applied in reverse order:
    // drop the indexes, then the table.
    let mut down_forward_order = vec![Op::DropTable {
        table: table.clone(),
    }];
    for col in &indexed {
        let index = index_name(&table, col);
        up.push(ClassifiedOp::new(Op::CreateIndex {
            name: index.clone(),
            table: table.clone(),
            columns: vec![col.clone()],
            unique: false,
        }));
        down_forward_order.push(Op::DropIndex { name: index });
    }

    // Refuse at scaffold time (#427): a rowid-alias or AUTOINCREMENT primary
    // key must never reach a manifest in the first place. Reuses apply.rs's
    // exact DDL rendering and pk_check.rs's classifier so `migrate new` and
    // `migrate apply` (which runs the same check over a hand-authored
    // manifest) agree byte-for-byte on what counts as the forbidden shape.
    // The message is built here, once, from the raw findings -- routing
    // through `pk_check::enforce`'s own rendered error would double the
    // "Configuration error:" prefix once `run_new` wraps this error too.
    let refusals = crate::migrate::apply::rowid_alias_findings(&up);
    if !refusals.is_empty() {
        let joined = refusals
            .iter()
            .map(|f| format!("[{}] {}", f.table, f.message))
            .collect::<Vec<_>>()
            .join("; ");
        return Err(GeneratorError::RowidPrimaryKey(format!(
            "incompatible primary key(s): {joined}"
        )));
    }

    let down: Vec<ClassifiedOp> = down_forward_order
        .into_iter()
        .rev()
        .map(ClassifiedOp::new)
        .collect();

    // Flags are honest and keyed on the *forward* ops only. The reverse always
    // contains a destructive DropTable, but that must not mark a create
    // migration destructive (which would wrongly arm pre-image capture).
    let flags = Flags {
        destructive: up.iter().any(|c| c.op_class == OpClass::Destructive),
        hash_rewriting: up.iter().any(|c| c.op_class == OpClass::HashRewriting),
    };

    Ok(Manifest {
        version: 1,
        target_schema: String::new(),
        up,
        down,
        preimage: None,
        flags,
        author: None,
    })
}

/// A parsed column plus whether it asked for its own index.
struct ParsedColumn {
    column: Column,
    index: bool,
}

/// Derive the target table name from the migration name.
///
/// 0.5.0 supports only `create_<table>`; anything else is rejected rather than
/// silently coerced into a table name.
fn table_from_name(name: &str) -> Result<String, GeneratorError> {
    if name.is_empty() {
        return Err(GeneratorError::EmptyName);
    }
    match name.strip_prefix("create_") {
        Some(table) if !table.is_empty() => Ok(table.to_string()),
        _ => Err(GeneratorError::UnsupportedName(name.to_string())),
    }
}

/// Parse one `name[:type][:modifier...]` token.
fn parse_column(spec: &str) -> Result<ParsedColumn, GeneratorError> {
    if spec.is_empty() {
        return Err(GeneratorError::EmptyColumnSpec);
    }
    let mut parts = spec.split(':');
    let name = parts.next().unwrap_or("");
    if name.is_empty() {
        return Err(GeneratorError::EmptyColumnName(spec.to_string()));
    }
    let rest: Vec<&str> = parts.collect();

    // The type, if present, is only the token immediately after the name; a
    // non-type token there means the type defaults to text.
    let mut kind = ColumnKind::Text;
    let mut modifiers = &rest[..];
    if let Some(first) = rest.first() {
        if let Some(parsed) = parse_type(first) {
            kind = parsed;
            modifiers = &rest[1..];
        }
    }

    let mut constraints = Vec::new();
    let mut tags = Vec::new();
    let mut index = false;
    for (i, token) in modifiers.iter().enumerate() {
        let (key, arg) = match token.split_once('=') {
            Some((k, v)) => (k, Some(v)),
            None => (*token, None),
        };
        match key {
            "pk" => {
                reject_value(name, key, arg)?;
                constraints.push(Constraint::Pk);
            }
            "fk" => {
                reject_value(name, key, arg)?;
                constraints.push(Constraint::Fk {
                    table: infer_fk_table(name)?,
                    col: "id".to_string(),
                });
            }
            "unique" => {
                reject_value(name, key, arg)?;
                constraints.push(Constraint::Unique);
            }
            "notnull" => {
                reject_value(name, key, arg)?;
                constraints.push(Constraint::NotNull);
            }
            "index" => {
                reject_value(name, key, arg)?;
                index = true;
            }
            "pii" => {
                reject_value(name, key, arg)?;
                tags.push("pii".to_string());
            }
            "default" => {
                // The default value is opaque: it may itself contain ':' (a URL,
                // a timestamp), which the column-level `split(':')` would
                // otherwise treat as a modifier separator. Rejoin this token's
                // value with every following token so `default=a:b` keeps its
                // colon. As a consequence the value is greedy -- `default=` must
                // be the final modifier in a spec.
                let head =
                    arg.ok_or_else(|| GeneratorError::DefaultMissingValue(name.to_string()))?;
                let value: String = std::iter::once(head)
                    .chain(modifiers[i + 1..].iter().copied())
                    .collect::<Vec<&str>>()
                    .join(":");
                // Both `default` (no `=`) and `default=` (empty value) are
                // rejected -- an empty default would emit malformed SQL later.
                if value.is_empty() {
                    return Err(GeneratorError::DefaultMissingValue(name.to_string()));
                }
                constraints.push(Constraint::Default(value));
                break;
            }
            "range" => constraints.push(range_check(name, kind, arg)?),
            _ => {
                return Err(GeneratorError::UnknownModifier {
                    column: name.to_string(),
                    modifier: (*token).to_string(),
                })
            }
        }
    }

    Ok(ParsedColumn {
        column: Column {
            name: name.to_string(),
            kind,
            constraints,
            tags,
        },
        index,
    })
}

/// Reject an `=value` suffix on a no-argument modifier.
///
/// The no-arg modifiers (`pk`/`fk`/`unique`/`notnull`/`index`/`pii`) take no
/// value; a suffix like `pk=foo` is an author mistake and must fail loudly
/// rather than be silently swallowed.
fn reject_value(column: &str, modifier: &str, arg: Option<&str>) -> Result<(), GeneratorError> {
    match arg {
        None => Ok(()),
        Some(_) => Err(GeneratorError::UnexpectedModifierValue {
            column: column.to_string(),
            modifier: modifier.to_string(),
        }),
    }
}

/// Whether `name` is a safe SQL identifier: `[A-Za-z_][A-Za-z0-9_]*`.
///
/// Author-controlled column names flow into generated `CHECK` expressions, so
/// they are gated to this conservative shape before interpolation.
fn is_safe_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Map a type token to its [`ColumnKind`], or `None` if it is not a type.
fn parse_type(token: &str) -> Option<ColumnKind> {
    match token {
        "text" => Some(ColumnKind::Text),
        "int" => Some(ColumnKind::Int),
        "real" => Some(ColumnKind::Real),
        "blob" => Some(ColumnKind::Blob),
        _ => None,
    }
}

/// Infer an `fk` target table from a `<table>_id` column name.
fn infer_fk_table(column: &str) -> Result<String, GeneratorError> {
    match column.strip_suffix("_id") {
        Some(prefix) if !prefix.is_empty() => Ok(prefix.to_string()),
        _ => Err(GeneratorError::FkInferenceFailed(column.to_string())),
    }
}

/// Lower a `range` modifier to a `CHECK` constraint.
///
/// Bare `range` emits a non-negativity guard (`col >= 0`); `range=LOW..HIGH`
/// emits a bounded `col >= LOW AND col <= HIGH`. Bounds are validated as numeric
/// so the emitted expression can never carry arbitrary CLI text into the SQL.
fn range_check(
    column: &str,
    kind: ColumnKind,
    arg: Option<&str>,
) -> Result<Constraint, GeneratorError> {
    if !matches!(kind, ColumnKind::Int | ColumnKind::Real) {
        return Err(GeneratorError::RangeOnNonNumeric {
            column: column.to_string(),
            kind: kind_name(kind),
        });
    }
    // The column name is interpolated verbatim into the emitted CHECK
    // expression, so it must be a safe SQL identifier -- reject anything else
    // rather than carry arbitrary text into the generated SQL.
    if !is_safe_identifier(column) {
        return Err(GeneratorError::UnsafeColumnName(column.to_string()));
    }
    let expr = match arg {
        None => format!("{column} >= 0"),
        Some(bounds) => {
            let invalid = || GeneratorError::InvalidRangeBounds {
                column: column.to_string(),
                bounds: bounds.to_string(),
            };
            let (low, high) = bounds.split_once("..").ok_or_else(invalid)?;
            let (low, high) = (low.trim(), high.trim());
            if low.parse::<f64>().is_err() || high.parse::<f64>().is_err() {
                return Err(invalid());
            }
            format!("{column} >= {low} AND {column} <= {high}")
        }
    };
    Ok(Constraint::Check(expr))
}

/// The grammar keyword for a [`ColumnKind`] (used in error messages).
fn kind_name(kind: ColumnKind) -> &'static str {
    match kind {
        ColumnKind::Text => "text",
        ColumnKind::Int => "int",
        ColumnKind::Real => "real",
        ColumnKind::Blob => "blob",
    }
}

/// The conventional index name for a single-column index.
fn index_name(table: &str, column: &str) -> String {
    format!("idx_{table}_{column}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrate::ChecksummedManifest;

    /// Pull the single `CreateTable` op out of a manifest's forward list.
    fn create_table(manifest: &Manifest) -> (&str, &[Column], bool) {
        match &manifest.up[0].op {
            Op::CreateTable {
                table,
                columns,
                without_rowid,
            } => (table.as_str(), columns.as_slice(), *without_rowid),
            other => panic!("expected create_table, got {other:?}"),
        }
    }

    #[test]
    fn table_name_derived_from_create_prefix() {
        let m = generate("create_contacts", &["id:pk".into()]).unwrap();
        let (table, _, without_rowid) = create_table(&m);
        assert_eq!(table, "contacts");
        assert!(!without_rowid);
    }

    #[test]
    fn bare_column_defaults_to_text_with_no_constraints() {
        let m = generate("create_people", &["firstname".into()]).unwrap();
        let (_, cols, _) = create_table(&m);
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "firstname");
        assert_eq!(cols[0].kind, ColumnKind::Text);
        assert!(cols[0].constraints.is_empty());
        assert!(cols[0].tags.is_empty());
    }

    #[test]
    fn pk_modifier_defaults_type_to_text() {
        let m = generate("create_contacts", &["id:pk".into()]).unwrap();
        let (_, cols, _) = create_table(&m);
        assert_eq!(cols[0].kind, ColumnKind::Text);
        assert_eq!(cols[0].constraints, vec![Constraint::Pk]);
    }

    #[test]
    fn explicit_type_is_parsed() {
        let m = generate(
            "create_t",
            &["n:int".into(), "r:real".into(), "b:blob".into()],
        )
        .unwrap();
        let (_, cols, _) = create_table(&m);
        assert_eq!(cols[0].kind, ColumnKind::Int);
        assert_eq!(cols[1].kind, ColumnKind::Real);
        assert_eq!(cols[2].kind, ColumnKind::Blob);
    }

    #[test]
    fn fk_target_inferred_from_id_suffix() {
        let m = generate("create_contacts", &["address_id:fk".into()]).unwrap();
        let (_, cols, _) = create_table(&m);
        assert_eq!(
            cols[0].constraints,
            vec![Constraint::Fk {
                table: "address".to_string(),
                col: "id".to_string(),
            }]
        );
    }

    #[test]
    fn fk_without_id_suffix_is_rejected() {
        let err = generate("create_contacts", &["address:fk".into()]).unwrap_err();
        assert_eq!(
            err,
            GeneratorError::FkInferenceFailed("address".to_string())
        );
    }

    #[test]
    fn pii_rides_in_tags_verbatim() {
        let m = generate("create_contacts", &["email:text:pii".into()]).unwrap();
        let (_, cols, _) = create_table(&m);
        assert_eq!(cols[0].kind, ColumnKind::Text);
        assert_eq!(cols[0].tags, vec!["pii".to_string()]);
        // pii is a tag, never a constraint -- smugglr only records it.
        assert!(cols[0].constraints.is_empty());
    }

    #[test]
    fn range_lowers_to_check_constraint() {
        let m = generate("create_shifts", &["hours:int:range".into()]).unwrap();
        let (_, cols, _) = create_table(&m);
        assert_eq!(
            cols[0].constraints,
            vec![Constraint::Check("hours >= 0".to_string())]
        );
    }

    #[test]
    fn range_with_bounds_emits_bounded_check() {
        let m = generate("create_shifts", &["hours:int:range=0..24".into()]).unwrap();
        let (_, cols, _) = create_table(&m);
        assert_eq!(
            cols[0].constraints,
            vec![Constraint::Check("hours >= 0 AND hours <= 24".to_string())]
        );
    }

    #[test]
    fn range_on_text_is_rejected() {
        let err = generate("create_t", &["name:text:range".into()]).unwrap_err();
        assert_eq!(
            err,
            GeneratorError::RangeOnNonNumeric {
                column: "name".to_string(),
                kind: "text",
            }
        );
    }

    #[test]
    fn range_with_non_numeric_bounds_is_rejected() {
        let err = generate("create_t", &["h:int:range=a..z".into()]).unwrap_err();
        assert_eq!(
            err,
            GeneratorError::InvalidRangeBounds {
                column: "h".to_string(),
                bounds: "a..z".to_string(),
            }
        );
    }

    #[test]
    fn default_carries_its_value() {
        let m = generate("create_t", &["status:text:default=active".into()]).unwrap();
        let (_, cols, _) = create_table(&m);
        assert_eq!(
            cols[0].constraints,
            vec![Constraint::Default("active".to_string())]
        );
    }

    #[test]
    fn default_value_may_contain_colons() {
        // Finding 1: a colon in the default value must survive parsing -- the
        // top-level `split(':')` would otherwise drop or misparse `:b`.
        let m = generate("create_t", &["col:text:default=a:b".into()]).unwrap();
        let (_, cols, _) = create_table(&m);
        assert_eq!(
            cols[0].constraints,
            vec![Constraint::Default("a:b".to_string())]
        );
    }

    #[test]
    fn no_arg_modifier_with_value_is_rejected() {
        // Finding 2: a no-arg modifier must reject an `=value` suffix rather
        // than silently swallow it.
        let err = generate("create_t", &["id:pk=oops".into()]).unwrap_err();
        assert_eq!(
            err,
            GeneratorError::UnexpectedModifierValue {
                column: "id".to_string(),
                modifier: "pk".to_string(),
            }
        );
        // The same guard covers fk, whose target is inferred, not supplied.
        let err = generate("create_t", &["address_id:fk=oops".into()]).unwrap_err();
        assert_eq!(
            err,
            GeneratorError::UnexpectedModifierValue {
                column: "address_id".to_string(),
                modifier: "fk".to_string(),
            }
        );
    }

    #[test]
    fn unsafe_column_name_in_check_is_rejected() {
        // Finding 3: an unsafe column name must not be interpolated into the
        // generated CHECK expression. A leading digit is not a valid SQL
        // identifier.
        let err = generate("create_t", &["1col:int:range".into()]).unwrap_err();
        assert_eq!(err, GeneratorError::UnsafeColumnName("1col".to_string()));
    }

    #[test]
    fn default_without_value_is_rejected() {
        let err = generate("create_t", &["status:text:default".into()]).unwrap_err();
        assert_eq!(
            err,
            GeneratorError::DefaultMissingValue("status".to_string())
        );
    }

    #[test]
    fn default_with_empty_value_is_rejected() {
        // `default=` (empty value) is as malformed as bare `default`.
        let err = generate("create_t", &["status:text:default=".into()]).unwrap_err();
        assert_eq!(
            err,
            GeneratorError::DefaultMissingValue("status".to_string())
        );
    }

    #[test]
    fn unique_and_notnull_preserve_declared_order() {
        let m = generate("create_t", &["email:text:unique:notnull".into()]).unwrap();
        let (_, cols, _) = create_table(&m);
        assert_eq!(
            cols[0].constraints,
            vec![Constraint::Unique, Constraint::NotNull]
        );
    }

    #[test]
    fn unknown_modifier_is_rejected() {
        let err = generate("create_t", &["x:text:bogus".into()]).unwrap_err();
        assert_eq!(
            err,
            GeneratorError::UnknownModifier {
                column: "x".to_string(),
                modifier: "bogus".to_string(),
            }
        );
    }

    #[test]
    fn non_create_name_is_rejected() {
        let err = generate("add_email_to_users", &["email".into()]).unwrap_err();
        assert_eq!(
            err,
            GeneratorError::UnsupportedName("add_email_to_users".to_string())
        );
    }

    #[test]
    fn int_pk_is_refused_at_scaffold_time() {
        // #427: the generator must not mint the rowid alias `id INTEGER
        // PRIMARY KEY` that smugglr's own primary-key requirement forbids.
        let err = generate("create_things", &["id:int:pk".into(), "name".into()]).unwrap_err();
        match err {
            GeneratorError::RowidPrimaryKey(msg) => {
                assert!(msg.contains("things"), "must name the table: {msg}");
                assert!(msg.contains("UUIDv7"), "must carry the remedy: {msg}");
            }
            other => panic!("expected RowidPrimaryKey, got {other:?}"),
        }
    }

    #[test]
    fn int_pk_is_refused_even_when_not_the_first_column() {
        // The refusal must inspect every column, not just the first -- a
        // check that only looked at columns[0] would miss this.
        let err = generate("create_widgets", &["name".into(), "id:int:pk".into()]).unwrap_err();
        assert!(matches!(err, GeneratorError::RowidPrimaryKey(_)));
    }

    #[test]
    fn text_pk_still_scaffolds() {
        // The accepted case: `id:pk` defaults to TEXT (per
        // `pk_modifier_defaults_type_to_text` above), which is not a rowid
        // alias and must not be refused.
        let m = generate("create_contacts", &["id:pk".into(), "name".into()]).unwrap();
        let (_, cols, _) = create_table(&m);
        assert_eq!(cols[0].kind, ColumnKind::Text);
        assert_eq!(cols[0].constraints, vec![Constraint::Pk]);
    }

    #[test]
    fn no_columns_is_rejected() {
        let err = generate("create_contacts", &[]).unwrap_err();
        assert_eq!(
            err,
            GeneratorError::NoColumns("create_contacts".to_string())
        );
    }

    #[test]
    fn index_modifier_emits_a_create_index_op() {
        let m = generate("create_contacts", &["email:text:index".into()]).unwrap();
        assert_eq!(m.up.len(), 2);
        match &m.up[1].op {
            Op::CreateIndex {
                name,
                table,
                columns,
                unique,
            } => {
                assert_eq!(name, "idx_contacts_email");
                assert_eq!(table, "contacts");
                assert_eq!(columns, &vec!["email".to_string()]);
                assert!(!unique);
            }
            other => panic!("expected create_index, got {other:?}"),
        }
        // The index column is still a real column on the table.
        let (_, cols, _) = create_table(&m);
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "email");
    }

    #[test]
    fn every_op_self_declares_an_honest_class() {
        let m = generate(
            "create_contacts",
            &["id:pk".into(), "email:text:index".into()],
        )
        .unwrap();
        for classified in &m.up {
            assert_eq!(classified.op_class, classified.op.class());
            assert_eq!(classified.op_class, OpClass::Additive);
        }
    }

    #[test]
    fn reverse_is_auto_derived_and_ordered() {
        let m = generate(
            "create_contacts",
            &["id:pk".into(), "email:text:index".into()],
        )
        .unwrap();
        // up = [create_table, create_index]; down = [drop_index, drop_table].
        assert_eq!(m.down.len(), 2);
        match &m.down[0].op {
            Op::DropIndex { name } => assert_eq!(name, "idx_contacts_email"),
            other => panic!("expected drop_index first, got {other:?}"),
        }
        match &m.down[1].op {
            Op::DropTable { table } => assert_eq!(table, "contacts"),
            other => panic!("expected drop_table last, got {other:?}"),
        }
    }

    #[test]
    fn create_migration_is_not_destructive() {
        let m = generate("create_contacts", &["id:pk".into()]).unwrap();
        assert!(!m.flags.destructive);
        assert!(!m.flags.hash_rewriting);
    }

    #[test]
    fn full_example_parses_to_expected_manifest() {
        // The issue's canonical example.
        let specs: Vec<String> = [
            "id:pk",
            "address_id:fk",
            "firstname",
            "lastname",
            "phone",
            "email:text:pii",
            "hours:int:range",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let m = generate("create_contacts", &specs).unwrap();

        let (table, cols, _) = create_table(&m);
        assert_eq!(table, "contacts");
        assert_eq!(cols.len(), 7);

        assert_eq!(cols[0].name, "id");
        assert_eq!(cols[0].constraints, vec![Constraint::Pk]);

        assert_eq!(cols[1].name, "address_id");
        assert_eq!(
            cols[1].constraints,
            vec![Constraint::Fk {
                table: "address".to_string(),
                col: "id".to_string(),
            }]
        );

        for col in &cols[2..=4] {
            assert_eq!(col.kind, ColumnKind::Text);
            assert!(col.constraints.is_empty());
            assert!(col.tags.is_empty());
        }

        assert_eq!(cols[5].name, "email");
        assert_eq!(cols[5].tags, vec!["pii".to_string()]);

        assert_eq!(cols[6].name, "hours");
        assert_eq!(cols[6].kind, ColumnKind::Int);
        assert_eq!(
            cols[6].constraints,
            vec![Constraint::Check("hours >= 0".to_string())]
        );

        // Reverse auto-derived, op-classes honest, not destructive.
        assert_eq!(m.down.len(), 1);
        assert!(matches!(m.down[0].op, Op::DropTable { .. }));
        assert!(!m.flags.destructive);

        // The manifest seals cleanly (checksum round-trips).
        let sealed = ChecksummedManifest::seal(m).unwrap();
        assert!(sealed.verify().is_ok());
    }
}
