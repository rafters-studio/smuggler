# cli-d1-sync

Sync a local SQLite database with a Cloudflare D1 database from the command line.

## Read this first: no output blocks, because nobody has run it against real D1

The two defects that used to stop this example before the first request are both fixed. The release now ships `smugglr-http-sql` alongside `smugglr` (#430), and the `[target] type = "d1"` shape below resolves to Cloudflare's own D1 query endpoint and sends the token as a bearer credential (#429), both covered by tests that drive the documented config into a capture endpoint and assert the URL and the `Authorization` header.

What is still missing is a run. Every other example here carries output pasted from a real invocation; this one carries none, because reaching D1 needs a Cloudflare account, a database, and a token, and no one has run it end to end since the fix. Treat the commands as correct and the absence of output as what it is -- untested against the live service, not verified and omitted.

If you want D1 with captured output today, [node-server-to-d1](../node-server-to-d1/) builds the URL itself and works with the npm package.

## Prerequisites

`smugglr` 0.5.0 on your PATH (`cargo install smugglr`, or the release binary), `smugglr-http-sql` on your PATH (`cargo install smugglr-http-sql`), a Cloudflare account with D1, a D1 database (`pnpm dlx wrangler d1 create my-app`, or the dashboard), an API token with `D1:Edit` scope from <https://dash.cloudflare.com/profile/api-tokens>, and a local SQLite file whose tables also exist on D1.

## Setup

Copy the config and put the token in the environment so the file can be committed.

```sh
cp config.example.toml config.toml
export SMUGGLR_D1_TOKEN="your-cloudflare-api-token"
```

`config.example.toml`:

```toml
local_db = "./app.sqlite"

[target]
type = "d1"
account_id = "your-32-char-account-id"
database_id = "your-d1-database-uuid"
api_token = "${SMUGGLR_D1_TOKEN}"

[sync]
tables = []
timestamp_column = "updated_at"
conflict_resolution = "local_wins"
```

`${SMUGGLR_D1_TOKEN}` is expanded at load; an unset variable exits 2. The default `exclude_tables` already covers `sqlite_sequence`, `_cf_KV`, `__drizzle_migrations`, and `_smugglr_migrations`; add `d1_migrations` and `_cf_METADATA` if your D1 has them. Matching is exact, not glob.

The schema must exist on both sides. smugglr moves rows, not DDL: create the same tables locally and on D1, for example `wrangler d1 execute my-app --file=schema.sql`.

## Run

Check the connection and the row counts on both sides:

```sh
smugglr status
```

See what a push would write, without writing:

```sh
smugglr push --dry-run
```

Push, pull, or both:

```sh
smugglr push
smugglr pull
smugglr sync
```

Every command takes `--output json` as a global flag, before the command, and exits with the code the summary describes: 0 success, 2 configuration, 3 connection, 4 conflict, 5 target not found. What the summary block looks like, with real output, is in [cli-sqlite-to-sqlite](../cli-sqlite-to-sqlite/); it is the same block against D1.

## What this demonstrates

The minimum smugglr setup for a hosted backend: `local_db`, one `[target]`, one `[sync]`. Content-hashed delta, so a second push after no edits moves nothing. And the plugin boundary: D1 is one profile of `smugglr-http-sql`, and Turso, rqlite, Datasette, StarbaseDB, and SQLite Cloud are the others, chosen by `profile` under a `type = "plugin"` target.
