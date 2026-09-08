<p align="center">
  <img src="smugglr.webp" alt="smugglr" width="400">
</p>

# smugglr

sqlite sync engine. one rust binary. mit.

If you're moving SQLite, smuggl it. Two SQLite files on one machine. Two machines on a LAN over encrypted UDP multicast. Any number of machines through an S3 relay. A hosted backend, D1, Turso, rqlite, Datasette, StarbaseDB, or SQLite Cloud, through one HTTP plugin with a profile per vendor. A browser or a Node process through the same engine compiled to WebAssembly. Content-hashed delta sync on every path, and since 0.5.0, schema migrations with a ledger that refuses to apply the same thing twice.

smugglr moves rows. It is not a database, and it does not reshape rows in transit.

## The one requirement: globally unique primary keys

smugglr's identity is the primary key. If a table uses `AUTOINCREMENT` or a bare `INTEGER PRIMARY KEY` rowid, two machines both mint `id = 5` for different rows and last-received-wins silently eats one of them: guaranteed data loss, not an edge case. Use UUIDv7, or any globally unique, k-sortable key. In 0.5.0 the first-run check warns on an incompatible schema and prints a manual recipe; the hard refusal and the in-tool `int -> UUIDv7` conversion are #280 and are not in this release. `smugglr migrate new` defaults every key to `TEXT`, which is the shape this wants.

## Install

Two binaries. `smugglr` is the CLI. `smugglr-http-sql` is the plugin every hosted backend is reached through; SQLite-to-SQLite, the relay, LAN broadcast, and `migrate` do not need it.

```sh
curl -fsSL https://raw.githubusercontent.com/rafters-studio/smugglr/main/install.sh | bash
```

The installer detects the platform, verifies the SHA256, and installs to `~/.local/bin` on Linux x64 and macOS x64 and arm64. Release archives from 0.5.1 carry both binaries; the 0.5.0 archive carries only the CLI. With cargo:

```sh
cargo install smugglr
cargo install smugglr-http-sql
```

Windows x64 has a release zip and the cargo path. Linux arm64 has no build yet.

## Show it running

Everything below is copied from [`docs/examples/`](docs/examples/), where it was captured from the 0.5.0 binary against the [Westwind](docs/examples/westwind/) sample: eight tables, forty customers, three hundred and twenty orders. This is [`cli-sqlite-to-sqlite`](docs/examples/cli-sqlite-to-sqlite/), two files on one machine. `config.toml` is the whole configuration:

```toml
local_db = "./local.db"

[target]
type = "sqlite"
database = "./backup.db"

[sync]
tables = []
timestamp_column = "updated_at"
conflict_resolution = "local_wins"
```

A dry-run reads both sides, hashes every row, and reports what a push would write. Nothing is written.

```
$ smugglr push --dry-run
--- Push Summary ---
  order_details: 788 rows
  suppliers: 8 rows
  employees: 9 rows
  shippers: 3 rows
  categories: 8 rows
  products: 20 rows
  customers: 40 rows
  orders: 320 rows

  (dry run - no actual changes made)
```

The push writes exactly what the dry-run described. Same counts; the lines come out in a different order because the summary iterates a map, so compare counts, not line positions.

```
$ smugglr push
--- Push Summary ---
  suppliers: 8 rows
  products: 20 rows
  employees: 9 rows
  customers: 40 rows
  order_details: 788 rows
  categories: 8 rows
  orders: 320 rows
  shippers: 3 rows
```

A second push has nothing to move, because every content hash now matches.

```
$ smugglr push

--- Push Summary ---
  No changes to push
```

The text mode also writes timestamped `INFO` lines to stderr for every step, with or without `-v`; the blocks above are stdout.

## Output for agents

`--output json` is a global flag, so it goes before the command. It silences the tracing and prints one JSON object on stdout. Captured after touching six orders locally:

```
$ sqlite3 local.db "UPDATE orders SET freight = freight + 5, updated_at = updated_at + 60 WHERE ship_city = 'Dragonstone'"
$ smugglr --output json push --dry-run
{"command":"push","status":"dry_run","tables":[{"name":"suppliers","local_only":0,"remote_only":0,"local_newer":0,"remote_newer":0,"content_differs":0,"identical":8,"rows_to_push":0,"rows_to_pull":0},{"name":"categories","local_only":0,"remote_only":0,"local_newer":0,"remote_newer":0,"content_differs":0,"identical":8,"rows_to_push":0,"rows_to_pull":0},{"name":"orders","local_only":0,"remote_only":0,"local_newer":6,"remote_newer":0,"content_differs":0,"identical":314,"rows_to_push":6,"rows_to_pull":0},{"name":"shippers","local_only":0,"remote_only":0,"local_newer":0,"remote_newer":0,"content_differs":0,"identical":3,"rows_to_push":0,"rows_to_pull":0},{"name":"order_details","local_only":0,"remote_only":0,"local_newer":0,"remote_newer":0,"content_differs":0,"identical":788,"rows_to_push":0,"rows_to_pull":0},{"name":"customers","local_only":0,"remote_only":0,"local_newer":0,"remote_newer":0,"content_differs":0,"identical":40,"rows_to_push":0,"rows_to_pull":0},{"name":"employees","local_only":0,"remote_only":0,"local_newer":0,"remote_newer":0,"content_differs":0,"identical":9,"rows_to_push":0,"rows_to_pull":0},{"name":"products","local_only":0,"remote_only":0,"local_newer":0,"remote_newer":0,"content_differs":0,"identical":20,"rows_to_push":0,"rows_to_pull":0}],"total_rows_to_push":6,"total_rows_to_pull":0,"exit_code":0}

$ smugglr --output json push
{"command":"push","status":"ok","tables":[{"name":"orders","rows_pushed":6}]}

$ smugglr --output json push
{"command":"push","status":"ok","tables":[]}
```

The dry-run object carries every table and every bucket; the push object carries only the tables that moved, so an empty `tables` is a no-op. The exit code is the scripting contract.

| Code | Meaning |
| --- | --- |
| 0 | success |
| 1 | general error |
| 2 | configuration error: fix the config, do not retry |
| 3 | connection error: transient, safe to retry |
| 4 | conflict: needs a human decision |
| 5 | target not found |

## How it works

For each table smugglr reads every primary key from both sides and hashes each row's content, excluding the configured `timestamp_column`, `exclude_columns`, and `converge_columns`. Each value is hashed with a type tag, so NULL and the empty string differ, numbers are canonicalized, and declared BLOB columns fold to one encoding whichever backend rendered them. When two hashes differ the timestamps decide which side is newer. Every row lands in one bucket.

| Bucket | Meaning | push | pull |
| --- | --- | --- | --- |
| `local_only` | added locally | insert | |
| `remote_only` | added remotely | | insert |
| `local_newer` | local timestamp wins | update | |
| `remote_newer` | remote timestamp wins | | update |
| `content_differs` | same timestamp, different content | by policy | by policy |
| `identical` | equal | skip | skip |

`conflict_resolution` settles `content_differs`: `local_wins` (the default), `remote_wins`, or `newer_wins`, which skips a row with no usable timestamp and says so once per table. Writes are upserts; nothing deletes. On the native path tables are written in whichever order the set iterates, which changes from run to run; the relay and the wasm client sort them. Timestamps lie, clocks drift, and bulk imports stamp everything "now", which is why the hash decides whether a row changed and the timestamp only decides which way it moves.

## The shapes

### Hosted backends through one plugin

Every remote target goes through `smugglr-http-sql`, selected by `type = "plugin"` with the vendor named in `profile`. The plugin binary is `name = "http-sql"`; the profile is the backend. From [`cli-http-sql-turso`](docs/examples/cli-http-sql-turso/):

```toml
local_db = "./local.db"

[target]
type = "plugin"
name = "http-sql"

[target.config]
profile = "turso"
url = "https://my-db.turso.io"
auth_token = "${TURSO_TOKEN}"
```

Profiles: `d1`, `turso`, `rqlite`, `datasette`, `sqlite-cloud`, `starbasedb`, `generic`, `http-sql`. Eight. A backend whose requests fit one of those shapes is a config change; a backend that speaks a new shape is a profile in the plugin. `${VAR}` and `${VAR:-default}` expand at load; an unset variable exits 2. The `d1` path is not working in 0.5.0; see Known limits.

### A relay, for machines that cannot reach each other

`stash` and `retrieve` sync through a SQLite file in an object store. From [`cli-stash-file-relay`](docs/examples/cli-stash-file-relay/), which runs the whole workflow through a `file://` URL:

```toml
local_db = "./machine-a.db"

[stash]
url = "file:///tmp/smugglr-relay/relay.sqlite"

[sync]
timestamp_column = "updated_at"
conflict_resolution = "newer_wins"
```

```
$ smugglr -c config-a.toml stash

--- Stash Summary ---
  categories: 8 rows
  customers: 40 rows
  employees: 9 rows
  order_details: 788 rows
  orders: 320 rows
  products: 20 rows
  shippers: 3 rows
  suppliers: 8 rows
```

```
$ smugglr -c config-b.toml retrieve

--- Retrieve Summary ---
  categories: 8 rows
  customers: 40 rows
  employees: 9 rows
  order_details: 788 rows
  orders: 320 rows
  products: 20 rows
  shippers: 3 rows
  suppliers: 8 rows
```

```
$ smugglr -c config-b.toml retrieve

--- Retrieve Summary ---
  No changes to retrieve
```

The relay is a SQLite file the diff engine syncs row by row in both commands. On S3 the upload is conditional on the ETag read at download, so two machines stashing at once cannot overwrite each other: the loser exits 4 and runs again. For S3 or R2, `url = "s3://bucket/path/relay.sqlite"` plus `access_key_id`, `secret_access_key`, `region`, and `endpoint`.

### A LAN, with no coordinator

`broadcast` keeps every machine on a subnet converged over UDP multicast. Each node multicasts a `primary_key -> content_hash` digest of its tables every interval, asks for the rows it lacks, and applies whatever arrives idempotently. No primary, no server, no internet. From [`cli-lan-broadcast`](docs/examples/cli-lan-broadcast/):

```toml
local_db = "./node.db"

[sync]
tables = []
timestamp_column = "updated_at"

[broadcast]
secret = "REPLACE_WITH_OUTPUT_OF_openssl_rand_-hex_32"
port = 31337
interval_secs = 5
conflict_resolution = "newer_wins"
ordering_columns = ["updated_at"]
```

```
$ smugglr broadcast
 INFO Acquired PID lock: .smugglr-broadcast.pid (PID 30382)
 INFO Starting masterless multicast sync (group 239.255.43.21, port 31999, interval 2s, instance peer-d, dry_run false)
 INFO Opening local database: d.db
 INFO Heartbeat #1: multicast 1 digest datagram(s)
 INFO Heartbeat #2: multicast 1 digest datagram(s)
```

With `secret` set, every datagram is XChaCha20-Poly1305 with a fresh nonce, and a node with a different key sees only ciphertext it cannot open. Leave `secret` out and the node runs in the clear, accepting any well-formed datagram on the subnet, and warns once at start. `remote_wins`, the default, is last-received-wins and can leave two nodes holding different rows for one key; `newer_wins` orders on `ordering_columns` and is the only policy that converges under concurrent edits, and both nodes must set it. Two peers on one machine need distinct `instance_id` values and separate working directories, and a firewall that drops inbound multicast makes both of them look deaf. This is for networks you trust; on anything else, run it inside a tunnel.

### A daemon

`smugglr watch` runs a full sync every interval in the foreground, holds a `.smugglr.pid` lock, prints nothing to stdout in text mode, and emits one JSON line per tick under `--output json`. It stops on SIGINT after the tick in flight completes.

### Snapshots

`snapshot` copies the local database file into the stash store under a timestamped key; `snapshots` lists them; `restore <timestamp>` replaces the local file with the latest snapshot at or before that time after a `quick_check`. Both act on the local file only; neither touches a hosted target.

## Schema migrations

`smugglr migrate` moves schema the way the rest of the tool moves rows: an inspectable manifest instead of a raw SQL string, a checksum, and a ledger inside the database. `migrate new` scaffolds from a Rails-style column spec and prints to stdout; `migrate apply` takes the manifest and an explicit `--db`, before any config is loaded, so a migration lands where you point it. From [`cli-migrate`](docs/examples/cli-migrate/), adding one table to Westwind, whose own eight tables were built the same way:

```
$ smugglr migrate new create_bribes id:pk:notnull harbormaster:text amount:int paid_at:int > migrations/create_bribes.json
```

```
$ smugglr migrate apply migrations/create_bribes.json --db ./westwind.db
Applied migration v9 (1 op) -- checksum 62f1d23e0c3b34222075d7f99ed090c4971710642a5201af5ad51bdb99d90148
```

```
$ smugglr migrate apply migrations/create_bribes.json --db ./westwind.db
Migration v9 is already applied -- nothing to do
```

The ledger, `_smugglr_migrations`, assigns the version as `current + 1`, claims it before the first op runs, and settles it after; each op is idempotent, so an apply interrupted midway converges on re-run. What 0.5.0 does not do: no reverse from the CLI, no recovery snapshot (`--paranoid` warns, #289), local SQLite only (#291), no `int -> UUIDv7` conversion (#280), and `migrate new` does not yet refuse an explicit `int:pk` (#427).

## Browser and Node

The engine compiles to WebAssembly and ships as the `smugglr` package on npm, with `@smugglr/zustand` and `@smugglr/nanostores` on top of it. `Smugglr.init({source, dest, sync})`, then `.push()`, `.pull()`, `.sync()`, `.diff()`, all with `dryRun`; `on("table-changed")` after a pull writes rows; `updateAuth`, `updateDest`, `eraseLocal`, `dispose`. A local SQLite in the browser is a `SqlExecutor`, `createWaSqliteExecutor` for wa-sqlite on OPFS or IndexedDB, or your own for any runtime. `autoSync` hydrates on init, re-syncs on `online`, and serializes across tabs with a Web Lock; it is browser-only. From [`node-server-to-d1`](docs/examples/node-server-to-d1/), pushing Westwind through `better-sqlite3` to a local HTTP-SQL endpoint:

```
push complete: {"command":"push","status":"ok","tables":[{"name":"categories","rowsPushed":8},{"name":"customers","rowsPushed":40},{"name":"employees","rowsPushed":9},{"name":"order_details","rowsPushed":788},{"name":"orders","rowsPushed":320},{"name":"products","rowsPushed":20},{"name":"shippers","rowsPushed":3},{"name":"suppliers","rowsPushed":8}]}
```

That's a bare `Smugglr.init(config)`, no WASM setup of its own. The wasm is 316,800 bytes on disk and 126,497 gzipped in the published 0.5.0 package. Node's `fetch` has no `file:` scheme, so the wasm-bindgen loader's default fetch-relative-to-the-glue-module path cannot work there; `init()` detects the Node runtime and reads the bundled `.wasm` bytes with `node:fs` itself. `setWasm` stays for callers who want to control where the binary comes from (a CDN, a bundler-resolved import) and no longer trips wasm-bindgen's deprecation warning while doing it.

## Embedding the engine

`smugglr-core` is the crate the CLI wraps. `sync_all` takes two `DataSource` values, and implementing the trait against anything row-shaped is six methods. From [`rust-custom-datasource`](docs/examples/rust-custom-datasource/), an in-memory store on each side, divergent, then one call:

```
before: a=2, b=2
  a: w1 alpha @ 2026-04-25T00:00:00Z
  a: w3 gamma @ 2026-04-25T00:00:00Z
  b: w2 beta @ 2026-04-25T00:00:01Z
  b: w3 gamma-edited @ 2026-04-25T00:00:05Z
sync:   widgets pushed a->b=1, pulled b->a=2
after:  a=3, b=3
  a: w1 alpha @ 2026-04-25T00:00:00Z
  a: w2 beta @ 2026-04-25T00:00:01Z
  a: w3 gamma-edited @ 2026-04-25T00:00:05Z
  b: w1 alpha @ 2026-04-25T00:00:00Z
  b: w2 beta @ 2026-04-25T00:00:01Z
  b: w3 gamma-edited @ 2026-04-25T00:00:05Z
```

[`rust-tokio-service`](docs/examples/rust-tokio-service/) does the same inside a tokio loop against the plugin, with a shutdown that finishes the sync in flight.

## Known limits in 0.5.0

Each of these is a reader's first ten minutes, stated here so it is met in the docs and not in production.

**D1 does not work from the CLI.** Three independent defects: the CLI hands the plugin the wrong keys and no URL (#429), the 0.5.0 archive and `cargo install smugglr` ship no plugin at all (#430, fixed for 0.5.1), and the `d1` profile reads its rows as its column list, so table discovery collapses on the plugin and wasm paths alike (#436). The npm example builds the D1 URL itself and works against a generic endpoint; against real D1 it hits #436. Until all three land, do not point this at D1 and expect rows.

**`uuid_v7_wins` is `newer_wins`.** No code reads the key's timestamp; the variant orders on `timestamp_column` like `newer_wins` and prints a different warning (#431).

**Retries never fire for hosted backends.** The engine's backoff is real, but the plugin reports every 429 and 5xx as permanent, so a rate limit ends a push on the first response with exit 1 (#432).

**A snapshot can miss committed rows.** `snapshot` reads the database file with `std::fs::read`; on a WAL-mode database, rows still in the WAL are counted in the metadata and absent from the file (#433). Checkpoint first.

**Table order ignores foreign keys.** The native path writes tables in set-iteration order and the relay and wasm paths alphabetically; none consults `foreign_key_list`, so on a target that enforces foreign keys a child table can land before its parent and be rejected (#435). Westwind declares no foreign keys for this reason.

**Pointing at the wrong target looks like success.** An empty table intersection reports `status: ok` with an empty `tables` and exit 0 (#438).

**`exclude_columns` leaves the hash on every path and leaves the wire on directional push and pull only.** On `stash`, `retrieve`, and a multicast peer's request, the column still travels (#322).

**Deletes do not replicate on any path** (#311). Model a delete as a `deleted_at` column, which rides the upsert path and converges.

## Configuration

`config.toml` in the working directory, or `-c <file>`. The sections are `local_db`, `[target]`, `[sync]`, `[stash]`, and `[broadcast]`; the annotated full file is [`config.example.toml`](config.example.toml), and every example directory carries the file it ran with. Defaults worth knowing: `timestamp_column = "updated_at"`, `conflict_resolution = "local_wins"`, `exclude_tables` covers `sqlite_sequence`, `_cf_KV`, `__drizzle_migrations`, and `_smugglr_migrations` and matches names exactly, `batch_size = 100`, `max_statement_bytes = 92160`, and `[broadcast]` at port 31337 every 30 seconds with `instance_id` defaulting to the hostname.

## Examples

| Surface | Example | What it shows |
| --- | --- | --- |
| Sample | [westwind](docs/examples/westwind) | The database every example uses, built by eight migrate manifests. |
| CLI | [cli-sqlite-to-sqlite](docs/examples/cli-sqlite-to-sqlite) | Two local files, dry-run, push, diff, JSON. No credentials. |
| CLI | [cli-stash-file-relay](docs/examples/cli-stash-file-relay) | stash and retrieve through a `file://` relay. |
| CLI | [cli-migrate](docs/examples/cli-migrate) | A manifest scaffolded, applied, and refused the second time. |
| CLI | [cli-lan-broadcast](docs/examples/cli-lan-broadcast) | Two machines converging over multicast. |
| CLI | [cli-http-sql-turso](docs/examples/cli-http-sql-turso) | The plugin config shape every hosted backend uses. |
| CLI | [cli-d1-sync](docs/examples/cli-d1-sync) | The D1 shape, and why 0.5.0 cannot complete it. |
| Node | [node-server-to-d1](docs/examples/node-server-to-d1) | One push through the npm package to an HTTP-SQL endpoint. |
| Node | [node-auto-sync](docs/examples/node-auto-sync) | A sync loop with backoff and a clean SIGTERM. |
| Rust | [rust-tokio-service](docs/examples/rust-tokio-service) | The engine inside a tokio service against the plugin. |
| Rust | [rust-custom-datasource](docs/examples/rust-custom-datasource) | `DataSource` against an in-memory store. |
| Browser | [browser-opfs-turso](docs/examples/browser-opfs-turso), [browser-idb-turso](docs/examples/browser-idb-turso), [browser-wasm-d1-multitenant](docs/examples/browser-wasm-d1-multitenant) | wa-sqlite on OPFS or IndexedDB syncing to Turso; many browsers into one D1 behind a tenant guard. |

Every block in this README is a substring of a file under `docs/examples/`, and CI checks that.

## Development

```sh
cargo test --workspace
cargo clippy --workspace -- -D warnings
cargo fmt -- --check
python3 scripts/check-examples.py --smugglr target/debug/smugglr
```

Running in production at [huttspawn.com](https://huttspawn.com) since early 2026. CI on Linux, macOS, and Windows. MIT.
