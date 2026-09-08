# Recorded Datasette responses

Three responses recorded from a real Datasette instance, for
[#436](https://github.com/rafters-studio/smugglr/issues/436)'s per-vendor
criterion. Datasette needs no account: it runs locally over a SQLite file.

## How they were recorded

Recorded 2026-09-08 against `datasette` installed on demand with `uvx`:

```sh
sqlite3 dsfix.db "CREATE TABLE customers (id TEXT PRIMARY KEY NOT NULL, name TEXT, updated_at INTEGER NOT NULL);
                  INSERT INTO customers VALUES ('a-uuid','Alice',1700000000),('b-uuid',NULL,1700000001);"
uvx --from datasette datasette serve dsfix.db --port 8767

curl -s "http://127.0.0.1:8767/dsfix.json?sql=<url-encoded statement>"
```

The files are the response bodies verbatim, including `query_ms`, which varies
per run and is not asserted on.

## What these pin

Datasette's rows are ARRAYS, not objects, and it sends a real `columns` list --
the opposite shape to D1, which is why `Profile::datasette` declares
`ColumnSource::Path(["columns"])` and D1 declares `FirstRowKeys`. These
recordings are what makes that a checked fact rather than a reading of the docs.

`PRAGMA table_info` is absent on purpose: Datasette's SQL endpoint is read-only
and does not answer a PRAGMA, so smugglr cannot use it as a sync target the way
it uses D1 or rqlite. Recording a fourth shape that the service refuses would be
recording a fiction.

## What these do not prove

Nothing about authentication (this instance had none), rate limits, or a hosted
Datasette's behavior. And nothing about the three vendors still unrecorded --
turso, starbasedb and sqlite-cloud are hosted-only and need an account; see
#436.
