# LogHaven

```cmd
██╗      ██████╗  ██████╗ ██╗  ██╗ █████╗ ██╗   ██╗███████╗███╗   ██╗
██║     ██╔═══██╗██╔════╝ ██║  ██║██╔══██╗██║   ██║██╔════╝████╗  ██║
██║     ██║   ██║██║  ███╗███████║███████║██║   ██║█████╗  ██╔██╗ ██║
██║     ██║   ██║██║   ██║██╔══██║██╔══██║╚██╗ ██╔╝██╔══╝  ██║╚██╗██║
███████╗╚██████╔╝╚██████╔╝██║  ██║██║  ██║ ╚████╔╝ ███████╗██║ ╚████║
╚══════╝ ╚═════╝  ╚═════╝ ╚═╝  ╚═╝╚═╝  ╚═╝  ╚═══╝  ╚══════╝╚═╝  ╚═══╝
```

A local-first, composable, chain-aware observability runtime for on-chain and off-chain systems.

> **Status: pre-1.0.** Phases 0 (foundation), 1 (usable end to end), 2 (chain awareness) and 3 (interfaces) are done: transaction tracing and Ethereum, Solana and Stellar adapters are in v0.3. Phase 3 (interfaces) is done too: the terminal UI and the web dashboard are in v0.4. The adapters have been run against public Ethereum, Solana and Stellar nodes over HTTP, and against local and fake nodes; `wss://` block notifications were run against a public Ethereum node. The dashboard was run in headless Chromium. See the [roadmap](#roadmap).

## What it is

LogHaven is a daemon that runs on your machine. Your applications send it log records over a local UDP socket; it stores them as compressed Parquet files on your disk (optionally copying them to S3, R2 or MinIO) and serves them back through a local HTTP API and the `loghaven` CLI. Nothing leaves your machine unless you configure cloud sync, and there is no per-ingestion fee.

Records can carry a `chain` and `tx_hash`, so on-chain activity and off-chain logs can be correlated (see `loghaven trace`).

**Today it gives you**

- Local-first storage in open formats (Parquet, zstd), one folder per app and day
- Authenticated, replay-protected ingest (RS256 JWT, then a per-session HMAC key)
- Query by app, level, source, time range and text, newest first
- Live tail (`logs --follow`)
- Optional upload to S3, R2 or MinIO with retry, retention and size limits
- A Rust client and a documented wire format for other languages

## Install

### From source (works today)

Requires Rust 1.85 or newer (edition 2024).

```bash
git clone https://github.com/The-True-Hooha/loghaven.git
cd loghaven
cargo build --release
./target/release/loghaven --version

# or install into ~/.cargo/bin
cargo install --path .
```

Or straight from Git: `cargo install --git https://github.com/The-True-Hooha/loghaven`.

### Prebuilt binaries

Release builds for Linux (x86_64), macOS (Intel and Apple Silicon) and Windows (x86_64) are attached to GitHub releases when a release is published.

```bash
# Linux / macOS (installs to /usr/local/bin)
curl -fsSL https://raw.githubusercontent.com/The-True-Hooha/loghaven/main/scripts/install.sh | sh
```

```powershell
# Windows (PowerShell)
irm https://raw.githubusercontent.com/The-True-Hooha/loghaven/main/scripts/install.ps1 | iex
```

Check it worked with `loghaven --version`.

## Quick start

Five minutes from nothing to a log line you can query.

```bash
# 1. Create the config file
loghaven init

# 2. Create the signing keys. Do this BEFORE the first `run`:
#    the daemon loads the public key once, at startup.
loghaven auth keygen

# 3. Optional but handy for trying things out: write to disk every second
#    instead of every 30 seconds, and keep everything local.
loghaven config storage.local.flush_interval_secs 1
loghaven config storage.backend local

# 4. Start the daemon (use --foreground to watch its output)
loghaven run --foreground
```

In another terminal:

```bash
# 5. Send a few records
loghaven send web "payment confirmed" --count 3 --chain ethereum --tx-hash 0xabc

# 6. Read them back (a session is minted automatically)
loghaven logs web

# 7. Tail live
loghaven logs web --follow

# 8. Status and shutdown
loghaven status
loghaven stop
```

Records are buffered and written to disk on a timer (`flush_interval_secs`, default 30), so with the default config a record shows up in queries up to 30 seconds after it was sent.

A longer hands-on script, including calling the HTTP API with `curl`, is in [docs/MANUAL_TESTING.md](docs/MANUAL_TESTING.md).

## Commands

Every command accepts `--profile <NAME>` to use `<NAME>.toml` instead of `config.toml` (see [profiles](#profiles)).

| Command | What it does |
|---------|--------------|
| `loghaven init [--force] [--storage TYPE] [--storage-path PATH]` | Write a default config file. Refuses to overwrite without `--force`. Renames a legacy `config_toml` to `config.toml`. |
| `loghaven config <KEY> [VALUE]` | Print or set a config value. Settable keys: `storage.backend`, `storage.local.path`, `storage.local.max_size_gb`, `storage.local.rotate_size_mb`, `storage.local.rotate_records`, `storage.local.flush_interval_secs`, `storage.local.retention_days`, `agent.log_level`, `agent.name`, `daemon.tcp_port`, `daemon.socket_path`. Everything else: edit the file. Restart the daemon after changes. |
| `loghaven run [--foreground]` | Start the daemon (in the background by default). |
| `loghaven status` | Show PID, version, uptime, storage backend and log level. |
| `loghaven stop [--force]` | Graceful shutdown (flushes buffers and finishes uploads). `--force` kills the process. |
| `loghaven auth keygen [--force]` | Create an RSA-2048 keypair. `--force` replaces it; the daemon must then be restarted. |
| `loghaven auth public-key` | Print the path of the public key. |
| `loghaven auth token <APP> [--ttl-secs N]` | Print a signed JWT for an app (default 3600 s, max 30 days). |
| `loghaven send <APP> <MESSAGE> [options]` | Send records. Options: `--level` (default `info`), `--source` (default `cli`), `--count`, `--chain`, `--tx-hash`, `--trace-id`. |
| `loghaven watch ethereum [--once]` | Poll Ethereum contract logs and record them as logs (settings in `[chains.ethereum]`, see [docs/CONFIGURATION.md](docs/CONFIGURATION.md#chainsethereum)). |
| `loghaven watch solana [--once]` | Poll finalized Solana transactions for configured addresses and record them as logs (settings in `[chains.solana]`, see [docs/CONFIGURATION.md](docs/CONFIGURATION.md#chainssolana)). |
| `loghaven watch stellar [--once]` | Poll Stellar account transactions from a Horizon server and record them as logs (settings in `[chains.stellar]`, see [docs/CONFIGURATION.md](docs/CONFIGURATION.md#chainsstellar)). |
| `loghaven dashboard --app <APP> [--ttl-secs N] [--no-open]` | Open the web dashboard for one app in your browser (see [Web dashboard](#web-dashboard)). |
| `loghaven tui --app <APP> [--app <APP>...] [--token T]` | Live terminal view of one or more apps' logs (see [Terminal UI](#terminal-ui)). Needs an interactive terminal. |
| `loghaven trace <TX_HASH> --app <APP> [--app <APP>...]` | Show the records that carry one transaction hash, oldest first, merged across the apps you name. Options: `--from MS`, `--to MS`, `--limit` (default 1000: newest N per app and overall), `--token` (one app only). `0x`+hex hashes match ignoring case (they are stored lowercase in chunks written after v0.2.0); other chains match exactly. |
| `loghaven logs <APP> [options]` | Query records. Options: `--level`, `--source`, `--from MS`, `--to MS`, `--text`, `--limit` (default 100), `--token`, `--follow`. |

Short aliases: `i` (init), `r` (run), `s` (status), `st` (stop).

### Reading logs

```bash
loghaven logs web                                # 100 newest records, printed oldest first
loghaven logs web --level error --limit 20
loghaven logs web --text timeout                 # case-insensitive, in message, tags and metadata
loghaven logs web --from 1730000000000 --to 1730003600000   # unix milliseconds, inclusive
loghaven logs web --follow --level error         # keep printing new matches; Ctrl+C to stop
loghaven trace 0xabc...def --app web --app indexer   # one transaction across two apps
```

Each line looks like `[timestamp_ms] level source | message chain=... tx=... trace=...`.

- `--text` is a plain substring match, not a query language.
- `--follow` cannot be combined with `--to`; `--limit` applies only to the first fetch. New records can take up to one flush interval to appear; a record that is flushed later than twice the interval plus 5 s after it arrived is not shown by `--follow`.
- Without `--token`, `logs` reads the daemon's private key and mints a session for the app. Run it as the user that owns the key (see [Security](#security)).

### Sending logs from your application

For anything beyond testing, use the Rust client in `src/client` or follow the wire format in [docs/WIRE_FORMAT.md](docs/WIRE_FORMAT.md): get a session from `POST /auth`, then send signed UDP datagrams to `127.0.0.1:9100`. A record looks like:

```json
{"app": "web", "records": [{"level": "info", "source": "checkout", "message": "payment confirmed", "chain": "ethereum", "tx_hash": "0x..."}]}
```

UDP is fire-and-forget: there is no acknowledgement, and bad, replayed or cross-app datagrams are dropped silently (the daemon prints a rate-limited line).

## Where things live

| | Linux | macOS | Windows |
|---|---|---|---|
| Config | `~/.config/loghaven/config.toml` | `~/Library/Application Support/loghaven/config.toml` | `%APPDATA%\loghaven\config.toml` |
| Keys | `<config dir>/keys/` | same | same |
| Data | `~/.local/share/loghaven/data/` | `~/Library/Application Support/loghaven/data/` | `%LOCALAPPDATA%\loghaven\data\` |
| Socket / PID | `~/.local/share/loghaven/` | `~/Library/Application Support/loghaven/` | PID in `%LOCALAPPDATA%\loghaven\`; control channel is TCP `127.0.0.1:9090` |

Data layout:

```
<data>/<app>/<YYYY-MM-DD>/<ms>-<nonce>-<seq>.parquet         # a finished chunk
<data>/<app>/<YYYY-MM-DD>/<ms>-<nonce>-<seq>.parquet.synced  # uploaded to the cloud
```

Chunks are written whole and renamed into place, so a crash never leaves a half-written file. Days are UTC dates of the record timestamps. You can read the files with any Parquet tool, for example DuckDB: `select * from 'path/to/web/2025-01-01/*.parquet'`.

Columns: `id`, `app`, `timestamp_ms`, `level`, `source`, `message`, `trace_id`, `span_id`, `chain`, `tx_hash`, `tags`, `metadata`.

## Ports

All listeners bind to `127.0.0.1` only.

| Port | Protocol | Purpose | Setting |
|------|----------|---------|---------|
| 9100 | UDP | Log ingest | `daemon.ingest_port` |
| 9200 | HTTP | `POST /auth`, `GET /logs` | `auth.query_port` |
| 9090 | TCP | CLI control (Windows only; Unix uses a socket file) | `daemon.tcp_port` |

## Configuration

`loghaven init` writes a complete config with defaults. The full reference, with storage backend examples (S3, R2, MinIO), is in [docs/CONFIGURATION.md](docs/CONFIGURATION.md). The settings you will touch most:

```toml
[storage]
backend = "auto"            # auto | local | s3 | r2 | minio
delete_after_upload = false # true removes local chunks after upload (they stop being queryable)

[storage.local]
flush_interval_secs = 30    # how soon new records become queryable
retention_days = 30         # 0 = keep forever
max_size_gb = 10            # 0 = no limit; oldest days are removed first

[auth]
require_auth = true         # false accepts unsigned datagrams and skips tokens (development only)
session_ttl_secs = 3600
```

### Profiles

`--profile dev` uses `dev.toml` next to `config.toml`, and a separate PID file. Profiles do not change ports or the data directory by themselves: to run two daemons at once, give each profile its own `ingest_port`, `query_port`, `tcp_port`, `socket_path` and `storage.local.path`.

### Environment variables

Three overrides are applied on top of the file: `LOGHAVEN_STORAGE_BACKEND`, `LOGHAVEN_LOG_LEVEL`, `LOGHAVEN_DAEMON_PORT` (sets `daemon.tcp_port`).

## Security

- Everything listens on loopback only. Do not forward these ports to a network.
- With `require_auth = true` (default) every datagram is HMAC-signed with a per-session key and carries a counter, so captured datagrams cannot be replayed. A session belongs to one app and cannot write or read another app's data.
- `keys/private.pem` is created with mode `0600` in a `0700` directory. Whoever can read it can mint a token for any app, and `send`, `auth token` and `logs` (without `--token`) read it. Treat it like a password.
- Sessions are saved to `sessions.json` in `keys_dir` (mode 0600, the file holds the sessions' HMAC keys), so a restart keeps them: open dashboards, `tui` and `logs --follow` carry on, and clients keep sending. Changes are written once a second and on a clean shutdown. After a crash or `kill -9`, a datagram sent in the last second before it can be accepted a second time. A damaged `sessions.json` stops the daemon with a message; delete it to start without sessions.
- After `auth keygen --force`, restart the daemon so it loads the new public key.

## Troubleshooting

| Symptom | Likely cause |
|---------|--------------|
| `No configuration found` | Run `loghaven init`. |
| `cannot read private key ... run 'loghaven auth keygen'` | Run `loghaven auth keygen` as the same user. |
| `auth failed with 503 (daemon has no public key ...)` | The daemon started before the keys existed. Restart it. |
| `auth failed with 401` | Key mismatch (keys regenerated without a restart) or expired JWT. |
| `cannot reach query server` | Daemon not running (`loghaven status`) or `auth.query_port` differs. |
| `logs` returns 0 records right after `send` | Not flushed yet. Wait one `flush_interval_secs`, or lower it. |
| `Daemon already running` | `loghaven stop`; a stale PID file is cleaned up automatically. |
| Records sent but never appear | Datagram dropped: wrong app name, replayed counter, expired session, or `require_auth` mismatch. Run with `--foreground` and read the `Ingest:` lines. |
| `403` on `/logs` | The token belongs to a different app than the `app` parameter. |
| Uploaded data missing from queries | `delete_after_upload = true` removes local chunks; queries only read local files. |

## Terminal UI

`loghaven tui --app web [--app api ...]` opens a live view of those apps' logs, merged by time. It loads
the newest 200 records per app, then follows new ones the same way `logs --follow` does (new records can
take up to the flush interval to appear), including asking again with a bigger page when a poll comes back full. If more than 10000 records arrive in one poll, the footer says some may be missing. Each app is polled independently, so a slow app delays only itself. Each app gets its own session, reused for traces, so scoping is unchanged; `--token` is for a single app. It keeps the newest 5000 records in memory.

| Key | Action |
|-----|--------|
| `j` / `k`, arrows, PageUp/PageDown | Move the highlight. Moving to the newest row resumes following; moving away pauses it. |
| `g` / `G` | Oldest loaded record / newest record (and follow). |
| `p` | Pause or resume following (not in a trace). |
| `/` | Type a text filter (case-insensitive over message, source, app, tx hash, trace id, chain, tags). Enter applies, Esc cancels, Ctrl+U clears the line. |
| `l` | Cycle the level filter: all, error, warn and up, info and up. |
| `t` | Trace the highlighted record's `tx_hash`: fetch the records with it from all the apps you opened (the newest 1000 per app; the footer says when that cuts the list), oldest first. A trace has its own highlight; Esc returns to the live view exactly as you left it. |
| `Enter` | Show every field of the highlighted record. |
| `Esc` | Leave a trace, or clear the text filter. |
| `q`, Ctrl+C | Quit. |

Filters apply to what is on screen: the loaded records, or the trace's records. A trace asks the daemon, so it also finds older records than the 5000 loaded. If the highlighted record is dropped or hidden by a filter, the highlight moves to the next record in time. Records with an unrecognised level count as `info`.
Times are UTC. Log content is untrusted, so control, line-separator, zero-width and bidirectional-override characters are shown as `·` and long runs of combining marks are cut.
The terminal is restored on quit, Ctrl+C, SIGTERM, SIGHUP and a panic. `kill -9` cannot be caught: run `reset` afterwards.

## Web dashboard

`loghaven dashboard --app web` opens `http://127.0.0.1:9200/dashboard` in your browser. The daemon serves the page itself, so there is nothing else to run; it needs the daemon up like `logs` does.

When `require_auth` is on, the command opens a session for that app (`--ttl-secs`, default 8 hours) and puts the token in the URL fragment (`#token=...`). A browser never sends the fragment to a server, so the token does not reach the daemon's logs or a proxy, and the page removes it from the address bar as soon as it loads; it then keeps it in memory only. The session belongs to one app, like every session. When it expires the page says so and you run the command again. The token is also an argument of the browser-opening command for a moment, so other local users can see it in the process list; use a short `--ttl-secs` on a shared machine. `--no-open` prints the URL instead of opening it; the URL contains the token, so treat it like one.

The page shows the newest 200 records, then follows new ones with the same lookback as `logs --follow`. It has a level filter (all, error, warn and up, info and up), a text filter over message, source and tx hash, pause/resume, and a detail panel per record. **Trace tx hash** lists the records of that app with the same hash. Log text is shown as text, never as HTML. The page and its script come with a strict Content-Security-Policy, and the query server refuses any `Host` other than `127.0.0.1`, `localhost` or `[::1]` on every route, so a web page cannot reach the logs by DNS rebinding. The dashboard does not trace across apps (one session is one app); use `loghaven trace` for that.

## Roadmap

LogHaven is pre-1.0. Work ships in phases; each phase is a release batch.

| Phase | Theme | Status |
|-------|-------|--------|
| 0 - Foundation | Daemon, local storage, ingest, query API, auth, cloud sync | **Done** (v0.1) |
| 1 - Usable end to end | Token CLI, client SDK, `send`, `logs --follow`, wire spec | **Done** (v0.2, see [docs/MANUAL_TESTING.md](docs/MANUAL_TESTING.md)) |
| 2 - Chain awareness | tx-hash correlation (`trace`), Ethereum, Solana and Stellar adapters | **Done** (v0.3): `trace` and the Ethereum adapter shipped in v0.2; the Solana and Stellar adapters and Ethereum WebSocket block notifications in v0.3. Run against public networks since v0.4 |
| 3 - Interfaces | TUI, web dashboard | **Done** (v0.4): `loghaven tui` and `loghaven dashboard` |
| 4 - Production hardening | Client-side encryption of uploads, multi-backend, persistent sessions, license | **Done** |

### Phase 0 - Foundation (done)

- Daemon with CLI control over a Unix socket (TCP on Windows)
- UDP ingest on `127.0.0.1:9100`: signed datagrams with replay protection, sessions scoped to one app
- Storage as immutable Parquet chunks (`<app>/<YYYY-MM-DD>/*.parquet`), written atomically
- Query API on `127.0.0.1:9200` (`GET /logs`): newest first, filters for level, source, time range and text, bounded in size, concurrency and time
- S3, R2 and MinIO sync with retry; local copies are kept unless `delete_after_upload = true`
- Pruner by retention days and size; never removes data that is not yet uploaded
- Input validation (app names), CI-clean build, 100+ tests including an end-to-end ingest-to-query test

### Phase 1 - Usable end to end (done)

- `loghaven auth token <app>` prints a signed JWT
- `loghaven send <app> <message>` sends records (signs and sequences datagrams for you)
- `loghaven logs <app> --follow` tails live; `logs` mints its own session when no `--token` is given
- Rust client in `src/client` (session exchange, datagram signing, batching); wire format in [docs/WIRE_FORMAT.md](docs/WIRE_FORMAT.md)
- A language SDK crate and other-language SDKs are still to come

### Phase 2 - Chain awareness

- Done: `GET /logs?tx_hash=` and `loghaven trace <tx> --app ...` correlate records by transaction hash across apps. Chunks written after v0.2.0 carry a bloom filter on `tx_hash`, so a trace skips chunks that cannot contain the hash. Older chunks are still scanned in full, and `--from`/`--to` narrow the scan for both.
- Done: Ethereum adapter (`loghaven watch ethereum`): HTTP polling of `eth_getLogs` for configured contracts, confirmation depth, persisted cursor, normalized records. An optional `ws_url` starts a poll as soon as a new block is announced.
- Done: Solana adapter (`loghaven watch solana`): HTTP polling of finalized transactions for configured addresses, program logs, persisted per-address cursor. It shares the RPC client and watch loop with the Ethereum adapter. Run against Solana mainnet's public RPC; `backfill` records the newest transactions on the first run.
- Done: Stellar adapter (`loghaven watch stellar`): pages account transactions from Horizon with a persisted per-account cursor. Run against the public Horizon server; `backfill` records the newest transactions on the first run, and `operations = true` adds each transaction's operation types.

### Phase 3 - Interfaces

- Done: terminal UI (`loghaven tui`)
- Done: local web dashboard (`loghaven dashboard`)

### Phase 4 - Production hardening

- Done: client-side encryption of cloud uploads (`encrypt_uploads`, AES-256-GCM, bound to the chunk's name, key id and `rotate-key`). Local chunks and the UDP wire format are not encrypted.
- Done: multiple simultaneous storage backends (`secondary` store, uploaded alongside the main one)
- Done: persistent sessions across restarts
- Column projection in queries, dedicated writer threads
- Done: dual MIT OR Apache-2.0 license

Details of the completed hardening work are in [docs/HARDENING_PLAN.md](docs/HARDENING_PLAN.md).

## Development

```bash
cargo build
cargo test                       # 120+ tests, no network needed
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

`make help` lists shortcuts. CI runs test, fmt and clippy on Linux and builds on Linux, macOS and Windows.

### Project structure

```
src/
  main.rs, error.rs
  cli/        command-line interface (clap)
  client/     Rust client: JWT minting, session exchange, signing, batching, query, follow
  config/     config structs, defaults, validation, env overrides
  daemon/     lifecycle, IPC server, PID file
  ipc/        CLI-to-daemon protocol
  ingest/     UDP ingest server and payload parsing
  auth/       RSA keys, JWT verification, HMAC sessions, replay window
  query/      HTTP query API (axum)
  storage/    Parquet chunks, queries, cloud sync, pruner, date math
docs/
  CONFIGURATION.md   full config reference
  WIRE_FORMAT.md     ingest and query protocol
  MANUAL_TESTING.md  hands-on test script
  HARDENING_PLAN.md  record of the Phase 0 hardening work
scripts/     install.sh, install.ps1
```

## Architecture

```
 your app --UDP datagram--> [ ingest ] --> [ per-app buffer ] --flush--> Parquet chunks on disk --> (optional) S3 / R2 / MinIO
 CLI ------Unix socket----> [ daemon control ]
 CLI / curl --HTTP--------> [ query API ] --reads--> Parquet chunks
```

One daemon process owns all state. The CLI is a thin client: it talks to the daemon over a local socket for `status` and `stop`, and over the HTTP API for queries. Storage is plain files, so backups, `rsync` and external tools work without the daemon.

## Philosophy

**We believe observability should be:**

- **User-Owned** - Your data belongs to you, not a SaaS vendor
- **Cost-Predictable** - No surprise bills from ingestion spikes
- **Privacy-Respecting** - Sensitive logs stay on your infrastructure
- **Chain-Agnostic** - Web3 development spans multiple blockchains
- **Composable** - Build custom workflows, not rigid dashboards

**LogHaven is NOT:**

- A managed SaaS platform (we don't host your data)
- A replacement for APM tools (we complement them)
- Blockchain-specific (we support any chain via adapters)
- A closed ecosystem (bring your own storage, tools, workflows)

## Contributing

LogHaven is in active development. Issues and pull requests are welcome. Please run `cargo fmt`, `cargo clippy --all-targets -- -D warnings` and `cargo test` before opening a PR.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in LogHaven by you, as defined in the Apache-2.0 license, is dual licensed as above, without any additional terms or conditions.

## Links

- **Website:** <https://loghaven.dev>
- **Issues:** <https://github.com/The-True-Hooha/loghaven/issues>
- **Discussions:** <https://github.com/The-True-Hooha/loghaven/discussions>
