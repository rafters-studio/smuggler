# Recorded D1 responses

Four responses, one per statement shape the http-sql adapter sends at a `d1`
target, recorded from a real D1 database rather than typed from the API docs.
They are the fixtures for [#436](https://github.com/rafters-studio/smugglr/issues/436),
where `Profile::d1` read its rows as its column list.

## How they were recorded

Wrangler's local D1 is the same engine Cloudflare runs, driven through
Miniflare instead of the REST endpoint, so a response can be captured without
an account or a token. Recorded 2026-09-08 with wrangler 4.129.0:

```sh
pnpm dlx wrangler d1 execute capture --local --command "
  CREATE TABLE customers (id TEXT PRIMARY KEY NOT NULL, name TEXT, updated_at INTEGER NOT NULL);
  CREATE TABLE orders (id TEXT PRIMARY KEY NOT NULL, note TEXT, updated_at INTEGER NOT NULL);
  INSERT INTO customers VALUES ('a-uuid','Alice',1700000000),('b-uuid',NULL,1700000001);"

pnpm dlx wrangler d1 execute capture --local --json --command "<the statement>"
```

`wrangler --json` prints the `result` array itself: `[{results, success, meta}]`.
Each file here wraps that array in the envelope the HTTP query API returns,
which is what the adapter parses:

```json
{ "result": <the wrangler output>, "errors": [], "messages": [], "success": true }
```

The `results` arrays are byte-for-byte what wrangler printed. The envelope
around them is the documented API wrapper, and `meta.duration` is left as
recorded.

## What is and is not proven

These pin the **row and column shape** D1 returns, which is the whole of #436:
`SELECT name FROM sqlite_master` yields `[{"name": "customers"}, ...]`, rows
that are indistinguishable from column descriptors and were read as such.

They do not prove anything about the transport -- authentication, rate limits,
error envelopes, or how the hosted service differs from the local engine. No
run against a live Cloudflare account backs these; see #436 for the remaining
per-vendor capture work, which needs credentials this repository does not have.

| File | Statement |
| --- | --- |
| `sqlite_master_listing.json` | `SELECT name FROM sqlite_master WHERE type='table' ...` |
| `pragma_table_info.json` | `PRAGMA table_info('customers')` |
| `metadata_select.json` | `SELECT id AS __pk, updated_at FROM customers` |
| `row_fetch.json` | `SELECT * FROM customers WHERE id IN (...)` |
