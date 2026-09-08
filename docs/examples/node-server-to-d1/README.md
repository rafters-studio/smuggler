# node-server-to-d1

A Node script reads a local SQLite database and pushes its rows to an HTTP-SQL endpoint with the `smugglr` npm package. No CLI is involved; the sync engine runs in-process as WebAssembly. The production destination is Cloudflare D1. The output below was captured against `local-endpoint.mjs`, a 50-line stand-in for D1 that ships with this example, because a D1 run needs an account and an API token this repository does not have. The data is the shared [westwind](../westwind/) sample: eight tables, 1,196 rows.

## Prerequisites

Node 24 (the capture ran on 24.12.0; the scripts use `--env-file`, and the local endpoint uses `node:sqlite`), pnpm, the `sqlite3` shell, and a `smugglr` binary for `../westwind/make.sh` (on your PATH, or named in `SMUGGLR`). For the D1 variant, a D1 database and an API token with `D1:Edit` scope from <https://dash.cloudflare.com/profile/api-tokens>.

## Setup

Install the dependencies. pnpm 10 and later refuse to run a dependency's build script until you approve it, and `better-sqlite3` needs its script to fetch a prebuilt binary.

```sh
pnpm install
pnpm approve-builds better-sqlite3
cp .env.example .env
```

Build the two databases. smugglr moves rows, not DDL, so the schema has to exist on both sides first; `--empty` applies it and skips the seed.

```sh
SMUGGLR=/path/to/smugglr ../westwind/make.sh ./app.sqlite
SMUGGLR=/path/to/smugglr ../westwind/make.sh --empty ./remote.sqlite
```

```
Applied migration v1 (1 op) -- checksum f40fed16bdb497a97d42a748a217ff175950ddcd4f5d34d007e0464fc5323f5f
Applied migration v2 (1 op) -- checksum 846afb23867541c0aa5b1e51caf0422b3b6139fdd74be483053ee963ec0149a2
Applied migration v3 (2 ops) -- checksum d45e3062fb7d3ebfc9d2c1ddaf5b1bfa1f01832d8f7d5c68fa6608b8c3ed81c2
Applied migration v4 (1 op) -- checksum c6447ae278a2e42d6b1cbf73e7e262ceac42b352b9ab77e483597f229b3f5942
Applied migration v5 (1 op) -- checksum c0bef91533708c8d4e3fc5c40f80fb9c5ea5db05f67becd58a994ff814fcbc25
Applied migration v6 (1 op) -- checksum 78ef6eae8eac922753460c0261d06006446d1b951298b0c11b1b6d2171e2828e
Applied migration v7 (3 ops) -- checksum 4b5cce42abd1e797066f5bfbebf4a96a53f3e8ff62d3ab6d0ad91cc703cbd701
Applied migration v8 (2 ops) -- checksum 282301d633321a5785ba012e5a176443fdc21981cb5c690dce9f24e18809bb6f
customers|40
orders|320
order_details|788
Applied migration v1 (1 op) -- checksum f40fed16bdb497a97d42a748a217ff175950ddcd4f5d34d007e0464fc5323f5f
Applied migration v2 (1 op) -- checksum 846afb23867541c0aa5b1e51caf0422b3b6139fdd74be483053ee963ec0149a2
Applied migration v3 (2 ops) -- checksum d45e3062fb7d3ebfc9d2c1ddaf5b1bfa1f01832d8f7d5c68fa6608b8c3ed81c2
Applied migration v4 (1 op) -- checksum c6447ae278a2e42d6b1cbf73e7e262ceac42b352b9ab77e483597f229b3f5942
Applied migration v5 (1 op) -- checksum c0bef91533708c8d4e3fc5c40f80fb9c5ea5db05f67becd58a994ff814fcbc25
Applied migration v6 (1 op) -- checksum 78ef6eae8eac922753460c0261d06006446d1b951298b0c11b1b6d2171e2828e
Applied migration v7 (3 ops) -- checksum 4b5cce42abd1e797066f5bfbebf4a96a53f3e8ff62d3ab6d0ad91cc703cbd701
Applied migration v8 (2 ops) -- checksum 282301d633321a5785ba012e5a176443fdc21981cb5c690dce9f24e18809bb6f
customers|0
orders|0
order_details|0
```

Start the endpoint in a second terminal. It answers `POST {sql, params}` with `{columns, rows}`, which is the request and response shape of smugglr's `generic` profile, and keeps its rows in `remote.sqlite`.

```sh
node local-endpoint.mjs
```

```
(node:31480) ExperimentalWarning: SQLite is an experimental feature and might change at any time
(Use `node --trace-warnings ...` to show where the warning was created)
http-sql endpoint on http://127.0.0.1:8765 (db: ./remote.sqlite)
```

## Run

```sh
node --env-file=.env push.mjs
```

```
push complete: {"command":"push","status":"ok","tables":[{"name":"categories","rowsPushed":8},{"name":"customers","rowsPushed":40},{"name":"employees","rowsPushed":9},{"name":"order_details","rowsPushed":788},{"name":"orders","rowsPushed":320},{"name":"products","rowsPushed":20},{"name":"shippers","rowsPushed":3},{"name":"suppliers","rowsPushed":8}]}
```

A second run pushes nothing, because every row's content hash already matches. A table with no rows moved omits its `rowsPushed` key.

```sh
node --env-file=.env push.mjs
```

```
push complete: {"command":"push","status":"ok","tables":[{"name":"categories"},{"name":"customers"},{"name":"employees"},{"name":"order_details"},{"name":"orders"},{"name":"products"},{"name":"shippers"},{"name":"suppliers"}]}
```

## Pushing to D1

Uncomment the three D1 lines in `.env` and fill in the account id, database id, and token; the script is unchanged. The URL is the D1 query endpoint, `https://api.cloudflare.com/client/v4/accounts/<account-id>/d1/database/<database-id>/query`, and `DEST_PROFILE=d1` selects D1's request and response shape. D1 needs the same eight tables first. The CLI cannot apply the Westwind manifests to a remote target at all -- `migrate apply` takes a local database file, and remote apply is unbuilt (#291) -- so export the schema from the local file and load it with wrangler: `sqlite3 app.sqlite .schema > schema.sql`, then `pnpm dlx wrangler d1 execute <name> --remote --file=schema.sql`. This path was not run here; it needs a D1 token.

## What this demonstrates

The `smugglr` package runs in Node with the same WebAssembly binary the browser gets. The binary is 316,800 bytes on disk in 0.5.0 and 126,497 bytes gzipped. Node's `fetch` has no `file:` scheme, so the wasm-bindgen loader's normal fetch-relative-to-the-glue-module path cannot work there; `Smugglr.init()` detects the Node runtime and reads the bundled `.wasm` bytes with `node:fs` instead, so the script above never touches WASM loading at all -- `push.mjs` is a plain `Smugglr.init(config)` call.

A `LocalEndpointConfig` with a custom `SqlExecutor` plugs any SQLite runtime into the local side. The executor here wraps `better-sqlite3` in ten lines: bind `params` positionally, and answer with `{columns, rows}` where each row is an array in column order. The same shape works for sql.js, the official sqlite-wasm package, or your own.

The destination is only an HTTP endpoint that speaks SQL. `local-endpoint.mjs` and D1 are interchangeable from the script's point of view; only the URL, the token, and the profile name change.

Table selection is the intersection of both sides' table lists, minus `excludeTables`, minus any table without a primary key, in alphabetical order. That order is not configurable, which is why Westwind declares no foreign keys (see its README). The `_smugglr_migrations` ledger that `make.sh` leaves behind has no primary key and is skipped on that ground alone; the script lists it in `excludeTables` anyway, because setting that option replaces the package's default list rather than adding to it.

## Files

| File | Purpose |
| ---- | ------- |
| `push.mjs` | The example. Wraps `better-sqlite3`, calls `push()` once. |
| `local-endpoint.mjs` | The `generic`-profile HTTP-SQL endpoint the capture ran against. `POST /fail/<n>` makes the next `n` queries answer 503; [node-auto-sync](../node-auto-sync/) uses that. |
| `.env.example` | Local endpoint values, with the D1 variant commented out. |
