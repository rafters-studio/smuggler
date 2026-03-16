<p align="center">
  <img src="smuggler.webp" alt="Smuggler" width="400">
</p>

# Smuggler

> "Look, I ain't in this for your revolution, and I'm not in it for you, Princess. I expect to be well paid. I'm in it for the money." - Han Solo

Smuggle data between SQLite and Cloudflare D1. Fast. Stateless. Questionable life choices.

## Status: Beta (Kessel Run Certified)

Running in production at [huttspawn.com](https://huttspawn.com) since early 2026. Pluggable data source architecture. CI across Linux, macOS, and Windows. Checksummed releases with a one-line installer.

Not 1.0 yet -- the API surface may still shift. But the core sync engine is solid and battle-tested, with S3-compatible relay sync for cross-machine workflows.

There are [open issues](https://github.com/ezmode-games/smuggler/issues). We're shaving parsecs, not days.

## What It Does

D1 is SQLite at the edge, but Cloudflare doesn't give you a way to sync your local dev database with production. Smuggler fills that gap:

- **Content hashing** - SHA256 comparison of actual row data, not just timestamps
- **Delta sync** - Only moves rows that changed
- **Bidirectional** - Push to D1, pull from D1, or both
- **No state files** - Fresh comparison every run, no stale state to haunt you
- **Pluggable backends** - `DataSource` trait abstracts any database backend
- **Automatic retries** - Exponential backoff with configurable limits for transient D1 failures
- **Batched writes** - Respects D1's 100-parameter bind limit, splits large upserts automatically
- **Table validation** - `--table` input validated against live schema before any SQL runs

## Installation

### Quick install (recommended)

```bash
curl -fsSL https://ezmode.games/oss/smuggler/install.sh | bash
```

Detects your platform, downloads the right binary, verifies the SHA256 checksum, and installs to `~/.local/bin/`. Supports Linux x64, macOS x64, and macOS ARM64. Detects Rosetta 2 and installs the native arm64 binary.

Install a specific version:

```bash
curl -fsSL https://ezmode.games/oss/smuggler/install.sh | bash -s v0.1.2
```

### Manual download

| Platform | Download |
|----------|----------|
| Linux x64 | [smuggler-linux-x64.tar.gz](https://github.com/ezmode-games/smuggler/releases/latest/download/smuggler-linux-x64.tar.gz) |
| macOS x64 | [smuggler-macos-x64.tar.gz](https://github.com/ezmode-games/smuggler/releases/latest/download/smuggler-macos-x64.tar.gz) |
| macOS ARM64 | [smuggler-macos-arm64.tar.gz](https://github.com/ezmode-games/smuggler/releases/latest/download/smuggler-macos-arm64.tar.gz) |
| Windows x64 | [smuggler-windows-x64.zip](https://github.com/ezmode-games/smuggler/releases/latest/download/smuggler-windows-x64.zip) |

### From source

```bash
cargo install --git https://github.com/ezmode-games/smuggler
```

Requires Rust 1.75+.

## Quick Start

1. Copy the example config:

```bash
cp config.example.toml config.toml
```

2. Add your credentials (don't commit this file, genius):

```toml
cloudflare_account_id = "your-account-id"
cloudflare_api_token = "your-api-token"
database_id = "your-d1-database-id"
local_db = ".wrangler/state/v3/d1/miniflare-D1DatabaseObject/xxx.sqlite"
```

3. Check if you can reach D1:

```bash
smuggler status
```

4. See what's different:

```bash
smuggler diff
```

5. Push your local changes (point of no return):

```bash
smuggler push
```

## Commands

```
smuggler status      # Can we phone home?
smuggler diff        # What's different?
smuggler push        # Local -> D1 (YOLO)
smuggler pull        # D1 -> Local (safer YOLO)
smuggler stash       # Local -> S3 relay (cross-machine sync)
smuggler retrieve    # S3 relay -> Local (cross-machine sync)
```

### Options

```
-c, --config <FILE>   Config file [default: config.toml]
-v, --verbose         See what's happening under the hood
--dry-run             Coward mode (just kidding, it's smart)
--table <NAME>        Sync one table only (validated against schema)
```

## How It Works

For each table, Smuggler:

1. Grabs all primary keys from both databases
2. SHA256 hashes each row's content (excluding timestamp columns)
3. When content differs, compares timestamps to determine which side is newer
4. Sorts rows into buckets:

| Bucket | What it means | Push | Pull |
|--------|--------------|------|------|
| `local_only` | You added it locally | Insert to D1 | - |
| `remote_only` | Someone else added it | - | Insert locally |
| `local_newer` | Your timestamp wins | Update D1 | - |
| `remote_newer` | Their timestamp wins | - | Update local |
| `content_differs` | Same timestamp, different data | Configurable | Configurable |
| `identical` | Exactly the same | Skip | Skip |

5. Batches writes to stay within D1's bind parameter limits (100 params per statement)
6. Retries transient failures with exponential backoff

### Why content hashing?

Timestamps lie. Clocks drift. Bulk imports set everything to "now". Content hashing catches actual changes regardless of what the timestamps say.

### Tables without timestamp columns

Smuggler gracefully handles tables missing the configured timestamp column. Rows with different content land in the `content_differs` bucket and are resolved by your `conflict_resolution` setting. Use `local_wins` or `remote_wins` for deterministic behavior on these tables -- `newer_wins` will skip them since there's no timestamp to compare.

## Architecture

Smuggler's sync engine is built on the `DataSource` trait, which abstracts any database backend:

```
DataSource (trait)
  |-- LocalDb    (rusqlite, synchronous)
  |-- D1Client   (reqwest HTTP, async with retries)
```

The diff engine (`diff_table`) and table resolution (`get_tables_to_sync`) are generic over any two `DataSource` implementations. This means the same comparison logic works whether you're syncing local-to-D1, local-to-local, or any future backend.

## Cross-Machine Sync (Stash/Retrieve)

Smuggler can sync your local SQLite database through an S3-compatible object store, enabling multiple dev machines to share state without going through D1.

```
Machine A                    S3/R2/GCS                  Machine B
  local.sqlite  --stash-->  relay.sqlite  --retrieve-->  local.sqlite
                <--retrieve--             <--stash--
```

### How it works

Both sides are SQLite. Smuggler downloads the relay file from S3, opens it as a second `LocalDb`, and runs the same diff engine used for D1 sync. Only changed rows are transferred.

- **`smuggler stash`** -- diffs local against the relay, applies changes to the relay, uploads it back to S3
- **`smuggler retrieve`** -- downloads the relay from S3, diffs it against local, applies changes to local

On first stash, Smuggler creates the relay from scratch and initializes its schema from the local database. ETag conditional writes prevent concurrent overwrites when multiple machines stash at the same time.

### Stash configuration

Add a `[stash]` section to your config (independent from D1 settings):

```toml
[stash]
# S3-compatible URL to the relay file
url = "s3://my-bucket/smuggler/relay.sqlite"

# AWS credentials (optional if using instance roles or env vars)
access_key_id = "AKIA..."
secret_access_key = "..."
region = "us-east-1"

# Custom endpoint for Cloudflare R2, MinIO, etc.
endpoint = "https://account-id.r2.cloudflarestorage.com"
```

Supported URL schemes:
- `s3://bucket/path` -- Amazon S3 or any S3-compatible store (R2, MinIO)
- `file:///absolute/path` -- Local filesystem (useful for testing)

### Usage with automation

Stash/retrieve pairs well with session hooks. For example, with [Legion](https://github.com/ezmode-games/legion):

```bash
# SessionStart hook
smuggler retrieve && legion reindex

# SessionStop hook
smuggler stash
```

## Configuration

```toml
cloudflare_account_id = "abc123"
cloudflare_api_token = "your-token-with-d1-permissions"
database_id = "your-d1-uuid"
local_db = "/path/to/local.sqlite"

[sync]
# Empty = sync all tables except excluded
tables = []

# Things you definitely don't want to sync
exclude_tables = [
    "sqlite_sequence",
    "_cf_KV",
    "__drizzle_migrations",
]

# Column for timestamp ordering when content differs
timestamp_column = "updated_at"

# When both sides changed: "local_wins", "remote_wins", "newer_wins"
conflict_resolution = "local_wins"

# Retry settings for transient D1 failures
max_retries = 5
initial_retry_delay_ms = 100
max_retry_delay_ms = 30000
backoff_multiplier = 2.0

# Batch settings for large tables
batch_size = 100
max_statement_bytes = 92160
```

### Finding Your Local D1 Database

Wrangler hides it at:

```
.wrangler/state/v3/d1/miniflare-D1DatabaseObject/<hash>.sqlite
```

The hash is derived from your binding name. If you have multiple databases, may the Force be with you.

### API Token

Get one from [Cloudflare Dashboard](https://dash.cloudflare.com/profile/api-tokens):

- D1:Read - for `diff`, `pull`, `status`
- D1:Write - for `push`

Pro tip: Create one token with both permissions. Fewer tokens to lose.

## Limitations

Things we don't do (yet):

- **Schema sync** - Run your migrations separately, we're data movers not DDL runners
- **Full-sync transactions** - Each batch is atomic, but the whole sync isn't. Re-run if interrupted.
- **BLOB wizardry** - Binary data compared as hex strings. It works but it's not pretty.
- **Tables without primary keys** - We need something to compare. Add a PK.
- **Linux ARM64** - Not available yet. See [issues](https://github.com/ezmode-games/smuggler/issues) for updates.

## Troubleshooting

### "401 Unauthorized"
Your token is expired or wrong. Make a new one.

### "429 Too Many Requests"
Smuggler retries automatically with exponential backoff. If you're still hitting rate limits, increase `initial_retry_delay_ms` or reduce `batch_size` in your config.

### "Invalid table name"
Smuggler validates `--table` input against your database schema before touching any SQL. If the table doesn't exist, you'll get a list of available tables. Run your migrations on both databases first. We don't create tables.

### All rows show as "content_differs"
Check that column order and types match. NULL vs empty string will cause hash mismatches.

## Development

```bash
cargo test                       # Run the tests (55 and counting)
cargo fmt                        # Format code
cargo clippy --all-targets       # Lint (including tests)
RUST_LOG=debug cargo run -- diff # Debug output
```

## Related Projects

Part of the [ezmode-games](https://github.com/ezmode-games) toolchain, built for [huttspawn.com](https://huttspawn.com):

- More tools coming when we get around to it

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). TL;DR:

1. Fork it
2. Branch it
3. Fix it / Build it
4. Test it
5. PR it

## License

MIT. Do whatever you want. Not our fault if it deletes your production database.

---

*"Never tell me the odds."*
