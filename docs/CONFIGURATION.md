# Configuration reference

The config file is TOML. `loghaven init` writes one with every default filled
in; any key you delete falls back to its default. Restart the daemon
(`loghaven stop && loghaven run`) after editing. `loghaven config <key>` prints
a value and `loghaven config <key> <value>` sets one (only for the keys listed
in the README; use the file for the rest).

File locations are in the README ("Where things live"). Use `--profile NAME`
on any command to read `NAME.toml` instead of `config.toml`.

## `[meta]`

| Key | Default | Notes |
|-----|---------|-------|
| `version` | `1` | Config schema version. |

## `[agent]`

| Key | Default | Notes |
|-----|---------|-------|
| `name` | `"loghaven-agent"` | Free-form label. |
| `log_level` | `"info"` | One of `trace`, `debug`, `info`, `warn`, `error`. Validated and shown by `status`; the daemon does not filter its own output by it yet. |

## `[daemon]`

| Key | Default | Notes |
|-----|---------|-------|
| `socket_path` | `<data dir>/loghaven.sock` | Unix control socket used by `status` and `stop`. |
| `tcp_port` | `9090` | Control channel on Windows (`127.0.0.1`). Override with `LOGHAVEN_DAEMON_PORT`. |
| `ingest_port` | `9100` | UDP port for log ingest (`127.0.0.1`). |

## `[auth]`

| Key | Default | Notes |
|-----|---------|-------|
| `keys_dir` | `<config dir>/keys` | Holds `private.pem` (0600) and `public.pem`. |
| `session_ttl_secs` | `3600` | Maximum session lifetime. A session never outlives the JWT that created it. |
| `query_port` | `9200` | HTTP port for `POST /auth` and `GET /logs` (`127.0.0.1`). |
| `require_auth` | `true` | `false` makes ingest accept unsigned datagrams and the query API skip tokens. Development only: any local process can then read and write any app. |

## `[storage]`

| Key | Default | Notes |
|-----|---------|-------|
| `backend` | `"auto"` | `local`: never upload. `s3` / `r2` / `minio`: upload to that store (its section and credentials are required, or startup fails). `auto`: upload to the first of S3, R2, MinIO that has credentials configured, otherwise stay local. Override with `LOGHAVEN_STORAGE_BACKEND`. |
| `delete_after_upload` | `false` | `true` removes a chunk from local disk once it is uploaded. Queries only read local chunks, so deleted chunks are no longer searchable. Switching this on later does not delete chunks that were already uploaded. |
| `primary`, `secondary` | `"local"`, unset | Reserved for multiple simultaneous backends (Phase 4). Currently unused. |

Cloud sync works per finished chunk: after each flush, every chunk without a
`.synced` marker is uploaded; failures are retried on the next flush and on
the next daemon start, and a clean shutdown waits (up to 30 s) for uploads.
Object keys are the chunk's path relative to the data directory, prefixed with
the configured `prefix`. Each upload times out after 120 s.

### `[storage.local]`

| Key | Default | Notes |
|-----|---------|-------|
| `path` | `<data dir>/data` | Root of all app data. |
| `flush_interval_secs` | `30` | Buffered records are written at least this often. This is also how long a new record can take to become queryable. |
| `rotate_records` | `500000` | Maximum records per chunk file. |
| `rotate_size_mb` | `128` | Ignored. Kept so old config files still load. |
| `retention_days` | `30` | Day folders older than this are deleted. `0` keeps everything. |
| `max_size_gb` | `10` | When the data folder is larger, the oldest day folders are deleted first. `0` means no limit. Today's data is never deleted this way. |

The pruner runs at startup and then hourly. It only removes `YYYY-MM-DD`
folders; it skips a day that was modified in the last hour and, when cloud
sync is configured, any day that still has chunks that were not uploaded yet.

### `[storage.s3]`

```toml
[storage]
backend = "s3"

[storage.s3]
bucket = "my-logs"
region = "us-east-1"
prefix = "loghaven"        # optional
access_key = "AKIA..."
secret_key = "..."
```

### `[storage.r2]` (Cloudflare R2)

```toml
[storage]
backend = "r2"

[storage.r2]
account_id = "your-account-id"
bucket = "my-logs"
prefix = "loghaven"        # optional
access_key = "..."
secret_key = "..."
```

### `[storage.minio]` (or any S3-compatible server)

```toml
[storage]
backend = "minio"

[storage.minio]
endpoint = "http://localhost:9000"
bucket = "loghaven"
access_key = "minioadmin"
secret_key = "minioadmin"
```

Credentials are stored in plain text in the config file. Keep the file
readable only by you (`chmod 600`).

## `[chains]`

```toml
[chains]
enabled = []               # "ethereum" | "solana" | "stellar"
```

Chain adapters arrive in Phase 2. The section is validated (an enabled chain
needs its own settings block) but nothing is monitored yet. Records can already
carry `chain` and `tx_hash` fields from your own applications.

## Environment variables

| Variable | Overrides |
|----------|-----------|
| `LOGHAVEN_STORAGE_BACKEND` | `storage.backend` |
| `LOGHAVEN_LOG_LEVEL` | `agent.log_level` |
| `LOGHAVEN_DAEMON_PORT` | `daemon.tcp_port` |

## Limits that are not configurable

| Limit | Value |
|-------|-------|
| App name | `[a-z0-9._-]`, 1 to 64 characters, no leading or trailing dot, no Windows device names |
| Query `limit` | 1 to 10000 (HTTP default 1000, CLI default 100) |
| Query result size | about 64 MiB, 4 concurrent queries, 30 s timeout |
| Sessions per app | 32 (oldest evicted) |
| Datagram size | one UDP packet (65507 bytes including the auth header) |
| Records held in memory when the disk is failing | 100000 records or 256 MiB, oldest dropped first |
