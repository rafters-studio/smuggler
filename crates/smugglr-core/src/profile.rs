//! Target profiles for different HTTP SQL endpoints.
//!
//! Each profile describes how to format requests and parse responses
//! for a specific HTTP SQL platform. This module lives in smugglr-core
//! so both the http-sql plugin (reqwest) and the WASM adapter (fetch)
//! can share profile definitions without duplication.

use crate::error::SyncError;
use serde_json::Value;

/// How to talk to a specific HTTP SQL endpoint.
#[derive(Debug, Clone)]
pub struct Profile {
    /// How to format the Authorization header
    pub auth_format: AuthFormat,
    /// How to build the request body from a SQL statement + params
    pub request_format: RequestFormat,
    /// JSON path to extract rows from the response
    pub rows_path: Vec<String>,
    /// Where this backend's column names come from
    pub columns: ColumnSource,
    /// Maximum bind parameters per query (0 = no limit)
    pub max_bind_params: usize,
}

#[derive(Debug, Clone)]
pub enum AuthFormat {
    Bearer,
    Basic,
    None,
}

/// Where a backend's column names come from (#436).
///
/// This used to be a bare `columns_path`, and every profile had to point it
/// somewhere. D1 has no column list in its response, so `Profile::d1` aimed it
/// at the ROWS -- and the extractor's "an object with a `name` key is a column
/// descriptor" rule then read `[{"name":"users"},{"name":"posts"}]`, the rows of
/// `SELECT name FROM sqlite_master`, as the column list `["users","posts"]`.
/// Table discovery against D1 has been broken on every path since 0.4.0 because
/// a "no column list" backend had no way to say so.
///
/// Naming the source makes that inexpressible: a profile either points at real
/// descriptors or declares it has none.
#[derive(Debug, Clone)]
pub enum ColumnSource {
    /// A JSON path to a list of column names or `{"name": ...}` descriptors.
    /// rqlite, Turso and Datasette all send one.
    Path(Vec<String>),
    /// The backend sends no column list; take the keys of the first row object,
    /// in the order the response carries them. D1 and any other row-objects
    /// backend.
    FirstRowKeys,
}

#[derive(Debug, Clone)]
pub enum RequestFormat {
    /// Turso/libSQL pipeline: `{"requests": [{"type": "execute", "stmt": {"sql": "<sql>", "args": [...]}}]}`
    Turso,
    /// rqlite: `[["<sql>", ...params]]`
    Rqlite,
    /// Datasette: `{"sql": "<sql>", "_shape": "array"}`. No positional bind API,
    /// so `build_request` errors rather than drop bound params (see #201).
    Datasette,
    /// Flat JSON: `{"sql": "<sql>", "params": [...]}`. Shared by Cloudflare D1,
    /// the http-sql v0.1 spec, and generic endpoints -- they emit this exact
    /// request body. Per-platform differences (response paths, bind limits) live
    /// in the `Profile` constructors, not in this enum.
    Generic,
}

impl Profile {
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "turso" | "libsql" => Some(Self::turso()),
            "rqlite" => Some(Self::rqlite()),
            "d1" | "cloudflare-d1" => Some(Self::d1()),
            "datasette" => Some(Self::datasette()),
            "sqlite-cloud" | "sqlitecloud" => Some(Self::sqlite_cloud()),
            "starbasedb" | "starbase" => Some(Self::starbasedb()),
            "generic" => Some(Self::generic()),
            "http-sql" | "httpsql" => Some(Self::http_sql()),
            _ => None,
        }
    }

    pub fn turso() -> Self {
        Self {
            auth_format: AuthFormat::Bearer,
            request_format: RequestFormat::Turso,
            rows_path: vec![
                "results".into(),
                "0".into(),
                "response".into(),
                "result".into(),
                "rows".into(),
            ],
            columns: ColumnSource::Path(vec![
                "results".into(),
                "0".into(),
                "response".into(),
                "result".into(),
                "cols".into(),
            ]),
            max_bind_params: 0,
        }
    }

    pub fn rqlite() -> Self {
        Self {
            auth_format: AuthFormat::Basic,
            request_format: RequestFormat::Rqlite,
            rows_path: vec!["results".into(), "0".into(), "values".into()],
            columns: ColumnSource::Path(vec!["results".into(), "0".into(), "columns".into()]),
            max_bind_params: 0,
        }
    }

    pub fn d1() -> Self {
        Self {
            auth_format: AuthFormat::Bearer,
            request_format: RequestFormat::Generic,
            rows_path: vec!["result".into(), "0".into(), "results".into()],
            // D1 sends no column list -- `result[0].results` is an array of row
            // OBJECTS and there is nothing else to point at. Aiming
            // `columns_path` here was the #436 defect: the rows of `SELECT name
            // FROM sqlite_master` look exactly like column descriptors.
            columns: ColumnSource::FirstRowKeys,
            max_bind_params: 100,
        }
    }

    /// The Cloudflare D1 query endpoint for an account and database (#429).
    ///
    /// Lives here rather than in config resolution because it is D1 platform
    /// knowledge, the same as [`Profile::d1`]'s response paths and bind limit.
    /// A caller with its own endpoint -- the Durable Objects bridge in
    /// `templates/do-bridge/` is the documented case -- supplies a URL instead
    /// and never calls this.
    pub fn d1_query_url(account_id: &str, database_id: &str) -> String {
        format!(
            "https://api.cloudflare.com/client/v4/accounts/{}/d1/database/{}/query",
            account_id, database_id
        )
    }

    pub fn datasette() -> Self {
        Self {
            auth_format: AuthFormat::Bearer,
            request_format: RequestFormat::Datasette,
            rows_path: vec!["rows".into()],
            columns: ColumnSource::Path(vec!["columns".into()]),
            max_bind_params: 0,
        }
    }

    pub fn sqlite_cloud() -> Self {
        Self {
            auth_format: AuthFormat::Bearer,
            request_format: RequestFormat::Generic,
            rows_path: vec!["data".into()],
            columns: ColumnSource::Path(vec!["columns".into()]),
            max_bind_params: 0,
        }
    }

    pub fn starbasedb() -> Self {
        Self {
            auth_format: AuthFormat::Bearer,
            request_format: RequestFormat::Generic,
            rows_path: vec!["result".into()],
            columns: ColumnSource::Path(vec!["columns".into()]),
            max_bind_params: 0,
        }
    }

    pub fn generic() -> Self {
        Self {
            auth_format: AuthFormat::Bearer,
            request_format: RequestFormat::Generic,
            rows_path: vec!["rows".into()],
            columns: ColumnSource::Path(vec!["columns".into()]),
            max_bind_params: 0,
        }
    }

    /// Profile for any server conforming to the http-sql v0.1 spec.
    /// See <https://github.com/rafters-studio/http-sql>.
    pub fn http_sql() -> Self {
        Self {
            auth_format: AuthFormat::Bearer,
            request_format: RequestFormat::Generic,
            rows_path: vec!["rows".into()],
            columns: ColumnSource::Path(vec!["columns".into()]),
            max_bind_params: 0,
        }
    }

    /// The column names for a response, by this profile's own rule (#436).
    ///
    /// Lives here rather than in each adapter because the http-sql plugin and
    /// the wasm fetch adapter carried byte-identical copies of this logic, which
    /// is why #436 broke both paths at once and why fixing it in one would have
    /// left the other wrong.
    ///
    /// The "an object with a `name` key is a column descriptor" rule applies
    /// ONLY to [`ColumnSource::Path`], where the profile has asserted the path
    /// really holds descriptors. Under [`ColumnSource::FirstRowKeys`] the same
    /// bytes are row data and are read as such.
    pub fn extract_columns(&self, response: &Value) -> Option<Vec<String>> {
        match &self.columns {
            ColumnSource::Path(path) => {
                let arr = Self::extract_path(response, path)?.as_array()?;
                let names: Vec<String> = arr
                    .iter()
                    .filter_map(|v| {
                        if let Some(s) = v.as_str() {
                            Some(s.to_string())
                        } else {
                            v.as_object()?
                                .get("name")
                                .and_then(|n| n.as_str())
                                .map(String::from)
                        }
                    })
                    .collect();
                if !names.is_empty() {
                    return Some(names);
                }
                // A declared descriptor path that held neither names nor
                // descriptors. Fall through to the rows rather than returning an
                // empty column list, which would silently produce zero-width
                // rows.
                self.first_row_keys(response)
            }
            ColumnSource::FirstRowKeys => self.first_row_keys(response),
        }
    }

    /// The keys of the first row object at `rows_path`, in response order.
    ///
    /// `serde_json` preserves object key order only with its `preserve_order`
    /// feature; without it the order is the map's, which is why every consumer
    /// reads values BY NAME (`extract_rows` looks each column up in the object)
    /// rather than by position. The order here decides display, not binding.
    fn first_row_keys(&self, response: &Value) -> Option<Vec<String>> {
        let arr = Self::extract_path(response, &self.rows_path)?.as_array()?;
        let first = arr.first()?.as_object()?;
        Some(first.keys().cloned().collect())
    }

    /// The rows for a response, as values in `columns` order.
    ///
    /// Shared for the same reason as [`Profile::extract_columns`]: two identical
    /// copies is how one path gets fixed and the other does not.
    pub fn extract_rows(&self, response: &Value, columns: &[String]) -> Option<Vec<Vec<Value>>> {
        let rows_val = Self::extract_path(response, &self.rows_path)?;
        let Some(arr) = rows_val.as_array() else {
            return Some(vec![]);
        };
        Some(
            arr.iter()
                .map(|row| {
                    if let Some(arr) = row.as_array() {
                        arr.clone()
                    } else if let Some(obj) = row.as_object() {
                        columns
                            .iter()
                            .map(|c| obj.get(c).cloned().unwrap_or(Value::Null))
                            .collect()
                    } else {
                        vec![row.clone()]
                    }
                })
                .collect(),
        )
    }

    /// Build the request body for a SQL query.
    ///
    /// Errors for the datasette profile when `params` is non-empty: Datasette's
    /// HTTP API has no positional (`?`) bind mechanism, so the previous behavior
    /// silently dropped the bound values and sent SQL with unbound `?`
    /// placeholders (endpoint error or, worse, a mis-bind). Surfacing the
    /// incompatibility as an error is the fix for #201.
    pub fn build_request(&self, sql: &str, params: &[Value]) -> Result<Value, SyncError> {
        let body = match self.request_format {
            RequestFormat::Turso => {
                if params.is_empty() {
                    serde_json::json!({
                        "requests": [
                            {"type": "execute", "stmt": {"sql": sql}}
                        ]
                    })
                } else {
                    let args: Vec<Value> = params
                        .iter()
                        .map(|p| {
                            if let Some(s) = p.as_str() {
                                serde_json::json!({"type": "text", "value": s})
                            } else if let Some(n) = p.as_i64() {
                                serde_json::json!({"type": "integer", "value": n.to_string()})
                            } else if let Some(n) = p.as_f64() {
                                serde_json::json!({"type": "float", "value": n})
                            } else if p.is_null() {
                                serde_json::json!({"type": "null"})
                            } else {
                                serde_json::json!({"type": "text", "value": p.to_string()})
                            }
                        })
                        .collect();
                    serde_json::json!({
                        "requests": [
                            {"type": "execute", "stmt": {"sql": sql, "args": args}}
                        ]
                    })
                }
            }
            RequestFormat::Rqlite => {
                if params.is_empty() {
                    serde_json::json!([[sql]])
                } else {
                    let mut stmt = vec![Value::String(sql.to_string())];
                    stmt.extend(params.iter().cloned());
                    serde_json::json!([stmt])
                }
            }
            RequestFormat::Datasette => {
                if !params.is_empty() {
                    return Err(SyncError::Config(format!(
                        "the datasette profile does not support parameterized queries: {} bind \
                         parameter(s) would be silently dropped. Datasette has no positional bind \
                         API; use a profile with parameter support (turso, rqlite, generic) for \
                         this endpoint.",
                        params.len()
                    )));
                }
                serde_json::json!({"sql": sql, "_shape": "array"})
            }
            RequestFormat::Generic => {
                if params.is_empty() {
                    serde_json::json!({"sql": sql})
                } else {
                    serde_json::json!({"sql": sql, "params": params})
                }
            }
        };
        Ok(body)
    }

    /// Navigate a JSON path to extract a nested value.
    pub fn extract_path<'a>(root: &'a Value, path: &[String]) -> Option<&'a Value> {
        let mut current = root;
        for segment in path {
            if let Ok(idx) = segment.parse::<usize>() {
                current = current.get(idx)?;
            } else {
                current = current.get(segment.as_str())?;
            }
        }
        Some(current)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_turso_request_no_params() {
        let p = Profile::turso();
        let body = p.build_request("SELECT 1", &[]).unwrap();
        assert!(body["requests"][0]["stmt"]["sql"]
            .as_str()
            .unwrap()
            .contains("SELECT 1"));
    }

    #[test]
    fn test_turso_request_with_params() {
        let p = Profile::turso();
        let body = p
            .build_request(
                "SELECT * FROM users WHERE id = ?",
                &[Value::String("42".into())],
            )
            .unwrap();
        let args = &body["requests"][0]["stmt"]["args"];
        assert_eq!(args[0]["type"], "text");
        assert_eq!(args[0]["value"], "42");
    }

    #[test]
    fn test_rqlite_request() {
        let p = Profile::rqlite();
        let body = p.build_request("SELECT 1", &[]).unwrap();
        assert_eq!(body[0][0], "SELECT 1");
    }

    #[test]
    fn test_generic_request() {
        let p = Profile::generic();
        let body = p.build_request("SELECT 1", &[]).unwrap();
        assert_eq!(body["sql"], "SELECT 1");
    }

    #[test]
    fn test_extract_path_simple() {
        let json = serde_json::json!({"rows": [{"id": 1}]});
        let result = Profile::extract_path(&json, &["rows".into()]);
        assert!(result.unwrap().is_array());
    }

    #[test]
    fn test_extract_path_nested() {
        let json = serde_json::json!({"results": [{"response": {"result": {"rows": [1,2,3]}}}]});
        let path = vec![
            "results".into(),
            "0".into(),
            "response".into(),
            "result".into(),
            "rows".into(),
        ];
        let result = Profile::extract_path(&json, &path).unwrap();
        assert_eq!(result.as_array().unwrap().len(), 3);
    }

    #[test]
    fn test_d1_request() {
        let p = Profile::d1();
        let body = p.build_request("SELECT 1", &[]).unwrap();
        assert_eq!(body["sql"], "SELECT 1");
    }

    #[test]
    fn test_datasette_request() {
        let p = Profile::datasette();
        let body = p.build_request("SELECT 1", &[]).unwrap();
        assert_eq!(body["sql"], "SELECT 1");
        assert_eq!(body["_shape"], "array");
    }

    #[test]
    fn datasette_rejects_parameterized_query() {
        // Regression for #201: Datasette has no positional bind API, so the
        // pre-fix build_request silently DROPPED params and emitted
        // {"sql": ..., "_shape": "array"} with unbound `?` placeholders (endpoint
        // error or mis-bind). It must now return an error instead of a
        // silently-misbinding body. Paramless datasette still works
        // (test_datasette_request above).
        let p = Profile::datasette();
        let result = p.build_request(
            "SELECT * FROM t WHERE id IN (?)",
            &[Value::String("k1".into())],
        );
        assert!(
            result.is_err(),
            "datasette must reject bound params, not silently drop them"
        );
    }

    #[test]
    fn test_profile_from_name() {
        assert!(Profile::from_name("turso").is_some());
        assert!(Profile::from_name("libsql").is_some());
        assert!(Profile::from_name("rqlite").is_some());
        assert!(Profile::from_name("d1").is_some());
        assert!(Profile::from_name("cloudflare-d1").is_some());
        assert!(Profile::from_name("datasette").is_some());
        assert!(Profile::from_name("sqlite-cloud").is_some());
        assert!(Profile::from_name("sqlitecloud").is_some());
        assert!(Profile::from_name("starbasedb").is_some());
        assert!(Profile::from_name("starbase").is_some());
        assert!(Profile::from_name("generic").is_some());
        assert!(Profile::from_name("http-sql").is_some());
        assert!(Profile::from_name("httpsql").is_some());
        assert!(Profile::from_name("unknown").is_none());
    }

    #[test]
    fn test_http_sql_request_no_params() {
        let p = Profile::http_sql();
        let body = p.build_request("SELECT 1", &[]).unwrap();
        assert_eq!(body["sql"], "SELECT 1");
        assert!(body.get("params").is_none());
    }

    #[test]
    fn test_http_sql_request_with_params() {
        let p = Profile::http_sql();
        let body = p
            .build_request(
                "SELECT * FROM notes WHERE id = ?",
                &[Value::String("42".into())],
            )
            .unwrap();
        assert_eq!(body["sql"], "SELECT * FROM notes WHERE id = ?");
        assert_eq!(body["params"][0], "42");
    }

    #[test]
    fn test_http_sql_response_paths() {
        // The v0.1 success envelope is {"columns": [...], "rows": [...], ...}
        // at the top level. Verify the profile points there.
        let p = Profile::http_sql();
        assert_eq!(p.rows_path, vec!["rows".to_string()]);
        let ColumnSource::Path(cols) = &p.columns else {
            panic!("http-sql sends a real column list; it must be a Path source");
        };
        assert_eq!(cols, &vec!["columns".to_string()]);
    }

    /// The #436 defect, as the response D1 actually sends.
    ///
    /// `SELECT name FROM sqlite_master` returns rows that are indistinguishable
    /// from column descriptors: objects with a single `name` key. Under the old
    /// `columns_path == rows_path` aliasing the extractor read them as the
    /// column list, so `list_tables` produced the table names as COLUMNS and
    /// then read the rows against that list. Table discovery could not start.
    mod d1_reads_its_own_responses {
        use super::*;

        /// A response recorded from a real D1 database.
        ///
        /// The four files under `tests/fixtures/d1/` were captured with
        /// wrangler's local D1 -- the same engine Cloudflare runs, reached
        /// through Miniflare rather than the REST endpoint, so no account or
        /// token is needed. That directory's README records the exact commands
        /// and states what these do and do not prove. Reading them from disk
        /// rather than inlining them is deliberate: a fixture that can be
        /// hand-edited to match the code is not a recording.
        fn recorded(name: &str) -> Value {
            let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/d1/");
            let text = std::fs::read_to_string(format!("{dir}{name}.json"))
                .unwrap_or_else(|e| panic!("recorded fixture {name}: {e}"));
            serde_json::from_str(&text)
                .unwrap_or_else(|e| panic!("fixture {name} is not JSON: {e}"))
        }

        /// A response in the recorded envelope, for shapes that probe reader
        /// behavior D1 does not currently produce (an empty result, a row
        /// missing a key). Those are about how the READER must behave, so they
        /// are constructed rather than recorded, and say so.
        fn constructed(rows: Value) -> Value {
            serde_json::json!({
                "result": [{ "results": rows, "success": true, "meta": {"duration": 0} }],
                "errors": [],
                "messages": [],
                "success": true
            })
        }

        #[test]
        fn a_table_listing_yields_table_names_as_rows_not_as_columns() {
            let response = recorded("sqlite_master_listing");
            let p = Profile::d1();

            let columns = p.extract_columns(&response).expect("columns");
            assert_eq!(
                columns,
                vec!["name".to_string()],
                "the only column is `name`; `customers` and `orders` are VALUES"
            );

            let rows = p.extract_rows(&response, &columns).expect("rows");
            assert_eq!(
                rows,
                vec![vec![Value::from("customers")], vec![Value::from("orders")],],
                "each table name must arrive as a row, which is what list_tables reads"
            );
        }

        #[test]
        fn pragma_table_info_keeps_its_primary_key_flag() {
            // table_info reads `name`, `type`, `notnull` and `pk` by position in
            // the column list. The old aliasing read the ROWS as descriptors, so
            // the column list became the column NAMES and every lookup missed --
            // yielding a table with no primary key, which sync refuses.
            let response = recorded("pragma_table_info");
            let p = Profile::d1();

            let columns = p.extract_columns(&response).expect("columns");
            assert!(columns.contains(&"pk".to_string()), "got {columns:?}");
            assert!(columns.contains(&"name".to_string()), "got {columns:?}");

            let rows = p.extract_rows(&response, &columns).expect("rows");
            let pk_idx = columns.iter().position(|c| c == "pk").expect("pk column");
            let name_idx = columns
                .iter()
                .position(|c| c == "name")
                .expect("name column");
            assert_eq!(rows[0][pk_idx], Value::from(1));
            assert_eq!(rows[0][name_idx], Value::from("id"));
            assert_eq!(rows[1][pk_idx], Value::from(0));
        }

        #[test]
        fn an_empty_result_set_is_not_an_error() {
            // No first row means no keys to read. This must not be mistaken for
            // a parse failure -- an empty table is ordinary.
            let response = constructed(serde_json::json!([]));
            let p = Profile::d1();
            assert_eq!(p.extract_columns(&response), None);
            assert_eq!(p.extract_rows(&response, &[]), Some(vec![]));
        }

        #[test]
        fn the_metadata_select_keeps_its_pk_and_timestamp() {
            // The third shape the adapter sends: `SELECT <pk> AS __pk,
            // updated_at, ... FROM t`. Read against the wrong column list this
            // yields no `__pk` and every row is skipped as unkeyed.
            let response = recorded("metadata_select");
            let p = Profile::d1();
            let columns = p.extract_columns(&response).expect("columns");
            assert!(columns.contains(&"__pk".to_string()), "got {columns:?}");

            let rows = p.extract_rows(&response, &columns).expect("rows");
            let pk = columns.iter().position(|c| c == "__pk").expect("__pk");
            assert_eq!(rows[0][pk], Value::from("a-uuid"));
            assert_eq!(rows[1][pk], Value::from("b-uuid"));
        }

        #[test]
        fn a_row_fetch_carries_every_column_including_nulls() {
            // The fourth shape: the full row fetch. A NULL must arrive as NULL
            // rather than as a missing column, because the row is rebuilt by
            // NAME from the column list -- a dropped key would shift nothing but
            // would silently write a NULL over a real value on upsert.
            let response = recorded("row_fetch");
            let p = Profile::d1();
            let columns = p.extract_columns(&response).expect("columns");
            assert_eq!(columns.len(), 3, "got {columns:?}");

            let rows = p.extract_rows(&response, &columns).expect("rows");
            let name = columns.iter().position(|c| c == "name").expect("name");
            assert_eq!(rows[0][name], Value::from("Alice"));
            assert_eq!(rows[1][name], Value::Null, "a NULL must arrive as NULL");
            assert_eq!(rows[1].len(), 3);
        }

        #[test]
        fn a_later_row_missing_a_key_reads_null_not_a_shift() {
            // Columns come from the FIRST row. If a later row omits a key --
            // which D1 does not do today, but the reader must not corrupt if it
            // did -- the value must be NULL in that column, never a left-shift
            // that puts a body where a timestamp belongs.
            let response = constructed(serde_json::json!([
                {"id": "a", "body": "first", "note": "n"},
                {"id": "b", "body": "second"}
            ]));
            let p = Profile::d1();
            let columns = p.extract_columns(&response).expect("columns");
            let rows = p.extract_rows(&response, &columns).expect("rows");
            let body = columns.iter().position(|c| c == "body").expect("body");
            let note = columns.iter().position(|c| c == "note").expect("note");
            assert_eq!(rows[1][body], Value::from("second"));
            assert_eq!(rows[1][note], Value::Null);
        }

        #[test]
        fn a_descriptor_profile_still_reads_descriptors() {
            // The confinement half of the fix: the "object with a `name` key is
            // a descriptor" rule must keep working where a profile actually
            // points at descriptors, or fixing D1 would break rqlite.
            let response = serde_json::json!({
                "results": [{
                    "columns": ["id", "body"],
                    "values": [["a", "first"], ["b", "second"]]
                }]
            });
            let p = Profile::rqlite();
            let columns = p.extract_columns(&response).expect("columns");
            assert_eq!(columns, vec!["id".to_string(), "body".to_string()]);
            let rows = p.extract_rows(&response, &columns).expect("rows");
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0][0], Value::from("a"));
        }
    }
}
