<p align="center">
  <img src="smuggler.webp" alt="Smuggler" width="400">
</p>

# Smuggler

> "Look, I ain't in this for your revolution, and I'm not in it for you, Princess. I expect to be well paid. I'm in it for the money." - Han Solo

Smuggle data between SQLite and Cloudflare D1. Fast. Stateless. Questionable life choices.

## Status: Alpha (Han Solo Approved)

We use this in production at [huttspawn.com](https://huttspawn.com). It works. Mostly.

Should *you* use it? That depends. Do you:
- Read the manual before assembling furniture? **Maybe wait for 1.0**
- Shoot first? **Welcome aboard**

There are [known issues](https://github.com/ezmode-games/smuggler/issues). We're shaving parsecs, not days.

## What It Does

D1 is SQLite at the edge, but Cloudflare doesn't give you a way to sync your local dev database with production. Smuggler fills that gap:

- **Content hashing** - Compares actual row data, not just timestamps
- **Delta sync** - Only moves rows that changed
- **Bidirectional** - Push to D1, pull from D1, or both
- **No state files** - Fresh comparison every run, no stale state to haunt you

## Installation

### Quick install (recommended)

```bash
curl -fsSL https://ezmode.games/oss/smuggler/install.sh | bash
```

Detects your platform, downloads the right binary, verifies the checksum, and installs to `~/.local/bin/`. Supports Linux x64, macOS x64, and macOS ARM64.

Install a specific version:

```bash
curl -fsSL https://ezmode.games/oss/smuggler/install.sh | bash -s v0.1.0
```

### Manual download

| Platform | Download |
|----------|----------|
| Linux x64 | [smuggler-linux-x64.tar.gz](https://github.com/ezmode-games/smuggler/releases/latest/download/smuggler-linux-x64.tar.gz) |
| macOS x64 | [smuggler-macos-x64.tar.gz](https://github.com/ezmode-games/smuggler/releases/latest/download/smuggler-macos-x64.tar.gz) |
| macOS ARM64 | [smuggler-macos-arm64.tar.gz](https://github.com/ezmode-games/smuggler/releases/latest/download/smuggler-macos-arm64.tar.gz) |

### From source

```bash
cargo install --git https://github.com/ezmode-games/smuggler
```

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
smuggler status    # Can we phone home?
smuggler diff      # What's different?
smuggler push      # Local -> D1 (YOLO)
smuggler pull      # D1 -> Local (safer YOLO)
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

### Why content hashing?

Timestamps lie. Clocks drift. Bulk imports set everything to "now". Content hashing catches actual changes regardless of what the timestamps say.

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

# Optional column for timestamp ordering when content differs
timestamp_column = "updated_at"

# When both sides changed: "local_wins", "remote_wins", "newer_wins"
conflict_resolution = "local_wins"
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

Things we don't do:

- **Schema sync** - Run your migrations separately, we're data movers not DDL runners
- **Transactions** - Each batch is atomic, but the whole sync isn't. Re-run if interrupted.
- **BLOB wizardry** - Binary data compared as hex strings. It works but it's not pretty.
- **Tables without primary keys** - We need something to compare. Add a PK.

## Troubleshooting

### "401 Unauthorized"
Your token is expired or wrong. Make a new one.

### "Invalid table name"
Smuggler validates `--table` input against your database schema before touching any SQL. If the table doesn't exist, you'll get a list of available tables. Run your migrations on both databases first. We don't create tables.

### All rows show as "content_differs"
Check that column order and types match. NULL vs empty string will cause hash mismatches.

## Development

```bash
cargo test                       # Run the tests
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
