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
| `session_ttl_secs` | `3600` | Maximum session lifetime. A session never outlives the JWT that created it. Sessions are kept in `<keys_dir>/sessions.json` and survive a daemon restart. |
| `query_port` | `9200` | HTTP port for `POST /auth` and `GET /logs` (`127.0.0.1`). |
| `require_auth` | `true` | `false` makes ingest accept unsigned datagrams and the query API skip tokens. Development only: any local process can then read and write any app. |

## `[storage]`

| Key | Default | Notes |
|-----|---------|-------|
| `backend` | `"auto"` | `local`: never upload, even with `[[storage.targets]]` (this is also what `LOGHAVEN_STORAGE_BACKEND=local` does). `s3` / `r2` / `minio`: upload to that store (its section and credentials are required, or startup fails). `auto`: upload to the first of S3, R2, MinIO that has credentials configured, otherwise stay local. Override with `LOGHAVEN_STORAGE_BACKEND`. |
| `delete_after_upload` | `false` | `true` removes a chunk from local disk once it is uploaded (with a `secondary`, once it is in both stores). Queries only read local chunks, so deleted chunks are no longer searchable. Switching this on later does not delete chunks that were already uploaded. |
| `encrypt_uploads` | `false` | `true` encrypts every chunk with AES-256-GCM before it is uploaded, to every store; the object is named `<chunk>.parquet.enc`. Local chunks stay plain, so queries work as before. The key is 32 random bytes (64 hex characters): `LOGHAVEN_STORAGE_KEY` if set, otherwise `<keys_dir>/storage.key`, which is created with mode 0600 on first use (the daemon prints a notice) and is not overwritten by a start. If `storage.key` is missing but archived keys are beside it, the daemon refuses to start instead of making a new one. Each object records which key made it and is bound to its name `<app>/<date>/<chunk>.parquet`, so a chunk cannot be passed off under another name. Chunks uploaded before this was switched on stay unencrypted, and so does a chunk that was already uploaded to one of two stores before it was switched on. **Back the key up: without it uploaded chunks cannot be read.** See [Encrypted uploads](#encrypted-uploads). |
| `secondary` | unset | A second store (`s3`, `r2` or `minio`) that every chunk is uploaded to as well as to the one `backend` selects, each under its own prefix. It must be a different store from the first, fully configured, and needs a cloud `backend` (not `local`); otherwise startup fails. Local disk stays the copy that queries read. Not allowed together with `[[storage.targets]]`. |
| `primary` | `"local"` | Unused and ignored; local disk is always the primary copy. |

With a `secondary`, a chunk counts as uploaded once both stores have it. If one store is down, the other still gets the chunk, the chunk stays local and unsynced (and is never pruned or deleted), and each retry uploads only to the store that is missing it; `<chunk>.parquet.synced-<store>` marks the stores that are done. A `secondary` applies to chunks written after it was enabled: chunks already marked as synced to the first store are not copied to it.

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

The three sections below are the older way to configure uploads. The newer one is `[[storage.targets]]`, a list that also covers Azure and Google Cloud Storage; see [Storage targets](#storage-targets). `s3`, `r2` and `minio` are the same S3-API store with different settings. Anything that speaks the S3 API works through `[storage.s3]` with an
`endpoint`, so there is no separate section per provider.

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
# endpoint = "https://s3.us-west-004.backblazeb2.com"   # another S3-compatible service; unset is AWS
# path_style = false       # true puts the bucket in the URL path (host/bucket/key); self-hosted servers often need it
```

Examples for `endpoint` (set `region` to the one the provider names; it is used for request signing):

| Service | `endpoint` | `path_style` |
|---------|------------|--------------|
| Backblaze B2 | `https://s3.<region>.backblazeb2.com` | `false` |
| Wasabi | `https://s3.<region>.wasabisys.com` | `false` |
| DigitalOcean Spaces | `https://<region>.digitaloceanspaces.com` | `false` |
| Google Cloud Storage (interoperability, HMAC keys) | `https://storage.googleapis.com` | `false` |
| Ceph RGW, SeaweedFS, Garage | your server's URL | usually `true` |

None of these named services was tried; the adapter was run against AWS-style, path-style and prefixed settings with a local S3 server. Report any service that needs another setting.

Stores that do not use the S3 API (Azure Blob Storage, Google Cloud Storage's own API) are configured as `[[storage.targets]]` entries, described next; so is everything above.

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
# region = "us-east-1"     # optional, this is the default
# prefix = "loghaven"      # optional
# path_style = true        # optional, true is the default
```

Credentials are stored in plain text in the config file. Keep the file
readable only by you (`chmod 600`).

## `[chains]`

```toml
[chains]
enabled = []               # "ethereum" | "solana" | "stellar"
```

### `[chains.ethereum]`

```toml
[chains]
enabled = ["ethereum"]

[chains.ethereum]
rpc_url = "https://eth-mainnet.example/KEY"   # http(s) JSON-RPC endpoint
app = "ethereum"                # app the events are recorded under (default)
addresses = ["0x..."]           # contracts to watch, 0x + 40 hex; at least one
topics = []                     # optional event signature hashes (topic0), any matches
poll_interval_secs = 12         # wait between polls once caught up
confirmations = 6               # only blocks this far behind the head are read
max_block_range = 1000          # largest eth_getLogs span (1 to 10000)
# start_block = 21000000        # first run only; default is the newest confirmed block
# ws_url = "wss://eth-mainnet.example/KEY"   # optional: poll as soon as a block is announced
```

Run it with `loghaven watch ethereum` (the daemon must be running). It is a
separate process: stop it with Ctrl+C. `--once` reads what is available and
exits.

How it behaves:

- Each log becomes a record with `chain = "ethereum"`, `source = "ethereum"` and
  the transaction hash set, so `loghaven trace <tx> --app ethereum --app web`
  lines it up with your own logs. Address, topics, data, block number, block
  time and log index are in `metadata`.
- The record timestamp is the time it was ingested, not the block time. The
  block time is `metadata.block_timestamp` (unix seconds).
- The next block to read is kept in `ethereum.cursor` next to `config.toml`.
  Delete it to start again from `start_block`. A cursor file that is not a
  block number, or one far ahead of the chain head (wrong node or chain), stops
  the watcher with an error instead of guessing.
- Delivery is best effort. With auth on, when the daemon cannot be reached the
  watcher reports the error, keeps its cursor and retries (`--once` exits with the
  error). Once datagrams are sent
  over UDP, a record the daemon drops (for example because it crashed right
  then) is not re-sent. A crash or error between sending and saving the cursor
  can send the same events again, so a trace may show an event twice.
- `confirmations` is the only reorg protection. A reorg deeper than that is not
  corrected, and `confirmations = 0` has no protection at all. Logs the node
  marks as removed are skipped.
- Event `data` longer than 4096 hex characters is cut in `metadata.data`;
  `metadata.data_len` has the full length and `metadata.data_truncated` is set.
- If the node refuses a range because it matches too many logs, the watcher
  retries on half the span, down to one block. A single block the node still
  refuses stops the watcher with an error naming that block.
- The cursor remembers the chain id and the address and topic lists it was saved
  for. If you change them, or point `rpc_url` at another chain, the watcher stops
  with an error instead of continuing from a block number that no longer fits.
  Delete `ethereum.cursor` and set `start_block` to re-read history with the new
  settings. A cursor file from 0.2.0 has no such record: it is accepted without a check, a warning is printed, and the current settings are recorded from then on. A settings or cursor-file error stops the watcher; network errors are retried. The node must support `eth_chainId`.
- `ws_url` (optional, `ws://` or `wss://`) turns on new-block notifications: the
  watcher subscribes to `newHeads` and polls as soon as a block is announced,
  instead of waiting out `poll_interval_secs`. It never polls more than once a
  second. Reading, the cursor and `confirmations` are unchanged and still use
  `rpc_url`, so a socket that drops, stays silent or is refused only costs latency: the watcher polls
  on the interval and reconnects on the next wait. A node that answers the
  subscription with "method not found" is not asked again until the watcher
  restarts; any other refusal (a rate limit) is retried. The socket is read only
  while waiting, so a node that drops quiet clients can cause a reconnect each
  cycle. Messages over 1 MiB are refused. The URL is never printed in errors. `wss://` uses the built-in root certificates and has been run against a public
  node (`wss://ethereum-rpc.publicnode.com`): with a 60 second `poll_interval_secs`, polls
  followed the 12 second block time.
- If your node rejects a range because it holds too many logs, lower
  `max_block_range`.
- The RPC URL is never printed in errors, because it often contains an API key.
  It is stored in plain text in `config.toml`.

### `[chains.solana]`

```toml
[chains]
enabled = ["solana"]

[chains.solana]
rpc_url = "https://solana-mainnet.example/KEY"   # http(s) JSON-RPC endpoint
app = "solana"                  # app the transactions are recorded under (default)
addresses = ["..."]             # account or program addresses (base58); at least one
poll_interval_secs = 5          # wait between polls once caught up
max_signatures = 100            # most transactions per address in one poll (1 to 1000)
# backfill = 20                 # first run only: also record each address's newest 20 transactions (0 to 100)
```

Run it with `loghaven watch solana` (the daemon must be running). It is a
separate process: stop it with Ctrl+C. `--once` reads what is available and
exits.

How it behaves:

- Every finalized transaction that touches a watched address becomes one record
  with `chain = "solana"`, `source = "solana"` and the signature as `tx_hash`,
  so `loghaven trace <signature> --app solana --app web` lines it up with your
  own logs. The level is `error` for a failed transaction and `info` otherwise.
  Slot, block time, fee, compute units and the program logs are in `metadata`.
  Signatures are case sensitive and are matched exactly.
- Only finalized transactions are read, so there is no reorg window and no
  `confirmations` setting.
- The first run starts at the current tip and records nothing from before.
  With `backfill = N` it starts N transactions before the tip, so each address's
  newest N transactions are recorded first (at most `max_signatures` per poll). `backfill`
  is read only when there is no cursor; it is not part of what the cursor is bound to.
- The newest recorded signature per address is kept in `solana.cursor` next to
  `config.toml`: the first line is a fingerprint of the cluster and the address
  list, then one `<address> <signature> <slot>` line per address. An address with
  no line (it had no transactions at the first run) is read from its newest
  10000 transactions. A file that does not parse stops the watcher with an
  error instead of guessing.
- If the node lists history older than the saved slot, it does not know the saved
  signature yet (a lagging or load-balanced node). The poll fails and is retried
  instead of recording old transactions again.
- If you change the addresses, or point `rpc_url` at another cluster, the watcher
  stops with an error instead of continuing from a signature that no longer
  fits. Delete `solana.cursor` to start again from the current tip.
- A transaction that touches two watched addresses is recorded once. Each poll
  reads the finalized slot first and takes nothing above it, so a transaction that
  lands while the addresses are being read is taken by all of them in the next
  poll. With a backlog, a poll also stops at the slot where the most backlogged
  address stops. If one slot holds more than `max_signatures` transactions for an
  address, that address takes its page of it and the other addresses wait until it
  has finished the slot. The adapter makes one extra `getSlot` call per poll.
- Per poll and address, at most `max_signatures` transactions are recorded,
  oldest first, and the next poll continues. If more than 10000 transactions
  are waiting (a long outage), the oldest ones are skipped and one record with
  level `warn` and no `tx_hash` says so.
- Each program log line is cut at 512 characters and the logs of one
  transaction at about 4 KB; `metadata.log_count` has the real count and
  `metadata.logs_truncated` is set.
- If `getTransaction` fails, or returns nothing for a transaction under ten
  minutes old (a node that is behind), the poll fails and is retried on the
  next interval. After 5 tries the transaction is recorded with
  `metadata.logs_available = false` and `metadata.logs_error` (the error, or
  empty for a missing one), and the watcher moves on; those logs are not
  fetched later. A transaction older than ten minutes that the node no longer
  has (pruned history) is recorded without logs at once. The tries are counted
  in memory, so a restart starts counting again.
- During a catch-up the backlog is listed once and then worked through from
  memory, so new transactions on an address show up after the catch-up ends.
  A restart lists it again.
- Delivery is best effort, with the same limits as the Ethereum adapter: when the
  daemon cannot be reached the watcher reports the error, keeps its cursor and
  retries (`--once` exits with it), a record the daemon drops after the datagram is sent is not re-sent,
  and a crash between sending and saving the cursor can send events again. A
  settings or cursor-file error stops the watcher; network errors are retried.
- The node must support `getSignaturesForAddress`, `getTransaction` and
  `getGenesisHash`. Public nodes limit how much history they keep and how fast
  you may call them: one `getTransaction` call is made per recorded transaction.
  A reply of HTTP 429 is not counted as a failed attempt: what was already read in
  that poll is recorded, the cursor moves only that far, and the rest waits for the
  next poll after `poll_interval_secs`. Transactions of every version the node
  serves are read (mainnet already serves version 1). If a busy address produces more
  transactions than the node lets you read, lower `max_signatures` or use a paid node.
- `ws_url` is accepted but unused; the adapter polls over HTTP.
- The RPC URL is never printed in errors, because it often contains an API key.
  It is stored in plain text in `config.toml`.

### `[chains.stellar]`

```toml
[chains]
enabled = ["stellar"]

[chains.stellar]
horizon_url = "https://horizon.stellar.org"   # http(s) Horizon server
app = "stellar"                 # app the transactions are recorded under (default)
accounts = ["G..."]             # account ids to watch (G + 55 characters); at least one
poll_interval_secs = 5          # wait between polls once caught up
max_records = 100               # most transactions per account in one poll (1 to 1000)
# backfill = 20                 # first run only: also record each account's newest 20 transactions (0 to 100)
# operations = true             # also record each transaction's operation types (one more request each)
```

Run it with `loghaven watch stellar` (the daemon must be running). It is a
separate process: stop it with Ctrl+C. `--once` reads what is available and
exits.

How it behaves:

- Every transaction that involves a watched account (as source, or through any of
  its operations) becomes one record with `chain = "stellar"`, `source = "stellar"`
  and the transaction hash as `tx_hash` (stored lowercase), so
  `loghaven trace <hash> --app stellar --app web` lines it up with your own logs.
  The level is `error` for a failed transaction and `info` otherwise. Ledger, close
  time, fee charged, operation count, source account and memo are in `metadata`;
  the memo is cut at 128 characters. The operations themselves are not fetched.
- Horizon only serves closed ledgers, which are final, so there is no reorg window
  and no `confirmations` setting.
- The first run starts at each account's newest transaction and records nothing
  from before. With `backfill = N` it starts N transactions earlier, so each account's
  newest N transactions are recorded first (at most `max_records` per poll). `backfill`
  is read only when there is no cursor.
- With `operations = true`, each record's `metadata.operations` lists the operation types
  in order (for example `["payment", "manage_data"]`); it is `null` when the setting is off.
  This costs one more request per recorded transaction. A reply of HTTP 429 is handled as
  for Solana: what was read is recorded, the cursor moves only that far, the rest waits for
  the next poll.
- The Horizon `paging_token` of the newest recorded transaction per account is
  kept in `stellar.cursor` next to `config.toml`: the first line is a fingerprint of
  the network passphrase and the account list, then one `<account> <token>` line
  per account. An account with no line (it had no transactions at the first run, or
  Horizon does not know it yet) is read from its oldest transaction. A file that
  does not parse stops the watcher with an error instead of guessing.
- If you change the accounts, or point `horizon_url` at another network, the watcher
  stops with an error instead of continuing from a token that no longer fits.
  Delete `stellar.cursor` to start again from the current tip.
- Per poll and account, at most `max_records` transactions are recorded, oldest
  first, and the next poll continues. Only that many are read. With several
  accounts, a poll takes nothing above the latest closed ledger it read from `/`
  (`history_latest_ledger`) first, so a transaction that lands while the accounts are
  being read is taken by all of them in the next poll. With a backlog it also stops
  at the ledger where the most backlogged account stops. If one ledger holds more
  than `max_records` transactions for an account, that account takes its page of it
  and the other accounts wait until it has finished the ledger. A transaction on two
  accounts is therefore recorded once.
- A node that returns transactions out of order is an error and nothing is
  recorded.
- A mistyped account id that is still a valid id looks like an account with no
  transactions: Horizon answers 404 and the watcher records nothing for it.
- Horizon keeps limited history on some servers; a cursor older than that reads from
  the oldest transaction the server still has, without a warning.
- Delivery is best effort, with the same limits as the other adapters: when the
  daemon cannot be reached the watcher reports the error, keeps its cursor and
  retries (`--once` exits with it), a record the daemon drops after the datagram is sent is not re-sent, and a crash
  between sending and saving the cursor can send events again. A settings or
  cursor-file error stops the watcher; network errors are retried.
- The server must serve `/` (for the network passphrase) and
  `/accounts/<id>/transactions`. Public Horizon servers rate-limit requests.
- The Horizon URL is never printed in errors, because it may contain an API key. It
  is stored in plain text in `config.toml`.

### Node replies

All three adapters refuse a reply body over 16 MiB, and show a node's own error text
without control characters and cut at 200 characters.

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
| Datagram size | one UDP packet (65507 bytes including the auth header; 9000 for the Rust client on macOS) |
| Records held in memory when the disk is failing | 100000 records or 256 MiB, oldest dropped first |

## Encrypted uploads

Decrypt a downloaded object with `loghaven storage decrypt FILE -o OUT.parquet`. The chunk's name is taken from the last three parts of the path (`<app>/<date>/<chunk>.parquet.enc`), which is what `aws s3 sync` and similar keep; for a file saved elsewhere pass `--name <app>/<date>/<chunk>.parquet`. The output is created with mode 0600 and never replaces an existing file. The key is looked up in `LOGHAVEN_STORAGE_KEY`, `storage.key` and the archived `storage.key.<id>` files in `keys_dir`, and the one named in the object is used.

`loghaven storage rotate-key` archives the current key as `storage.key.<id>` and makes a new one, so old uploads stay readable. The running daemon keeps the key it started with: restart it for new uploads to use the new key. With `LOGHAVEN_STORAGE_KEY` set, rotate by changing the variable and keep the old key for reading (put it in `storage.key.<id>` where `<id>` is the 16 hex characters shown in the error when a key is missing). Only files named exactly `storage.key` or `storage.key.<16 hex characters>` count as keys; anything else in `keys_dir` is ignored.

While an upload is in progress a `<chunk>.parquet.enc.tmp` file (ciphertext, mode 0600) sits next to the chunk. It is removed when the upload ends, however it ends, and any left by a crash is removed at the next start. The chunk, the key and the data directory are as private as the rest of `keys_dir` and the data path: this feature protects what is in the store, not the machine.

## Storage targets

`[[storage.targets]]` lists where chunks are uploaded. Every chunk goes to every target; it is synced (and deleted, with `delete_after_upload`) once all of them have it. A target that is down does not stop the others, and retries go only to the target that is missing the chunk. `encrypt_uploads` encrypts the chunk once for all of them.

```toml
[[storage.targets]]
name = "archive"            # a-z 0-9 - _, at most 32, unique; names the .synced-<name> markers
type = "s3"                 # AWS S3 or any S3-compatible service
bucket = "my-logs"
region = "eu-west-1"
prefix = "loghaven"         # optional
# endpoint = "https://s3.us-west-004.backblazeb2.com"   # another service; unset is AWS
# path_style = false        # true for an IP address, localhost or a bare host name
access_key_env = "LOGHAVEN_S3_KEY"      # or access_key = "..."
secret_key_env = "LOGHAVEN_S3_SECRET"   # or secret_key = "..."

[[storage.targets]]
name = "backup"
type = "azure"              # Azure Blob Storage
account = "mystorageaccount"
container = "logs"
# endpoint = "http://127.0.0.1:10000/devstoreaccount1"  # a sovereign cloud or the Azurite emulator
access_key_env = "LOGHAVEN_AZURE_KEY"   # or sas_token / sas_token_env, one of them

[[storage.targets]]
name = "gcs"
type = "gcs"                # Google Cloud Storage, its own API
bucket = "my-logs"
credentials_file = "/etc/loghaven/service-account.json"   # optional
```

**Credentials.** Each secret is written in the file (`x`) or named as an environment variable (`x_env`), not both; the variable must be set and not blank when the daemon starts or startup fails naming it (never its value), and surrounding whitespace such as a trailing newline is removed. A blank value written in the file is an error. An S3 target takes `access_key` and `secret_key` together, both in the file or both as `_env`, or neither. An Azure target takes an access key or a SAS token, not both. A SAS token may be pasted as the portal shows it, percent-encoding and leading `?` included.

When a target gives its own credentials, nothing in the environment can change it: stray `AWS_ENDPOINT_URL_S3`, `AWS_SESSION_TOKEN`, `AZURE_STORAGE_*` or `GOOGLE_*` variables are ignored for it. A secret left out entirely falls back to the provider's usual ambient sources (`AWS_*` variables and instance roles, `AZURE_STORAGE_*` variables and managed identity, `GOOGLE_APPLICATION_CREDENTIALS` and the instance's service account), and then those variables are read as the provider's tools read them, so an `AWS_ENDPOINT_URL_S3` set in the environment applies to such a target.

**Typos.** A key that does not belong to the target's `type`, or is misspelt (`acess_key_env`, `path-style`), is an error naming the target and the key, not a silent default.

**Rules.** Names must be unique and are kept in the markers of chunks that are partly uploaded, so do not rename a target while chunks are waiting. A `prefix` is plain path segments: no empty, `.` or `..` segment, no backslash. A virtual-hosted S3 endpoint (the default) must be a dotted host name, because the bucket is put in front of it; an IP address, `localhost` or a bare name needs `path_style = true`, which the config check and the daemon both require. An upload that fails is reported with the provider's message, query strings removed (a SAS token or signature rides there), cut to 300 characters.

**Large chunks.** A chunk up to 8 MiB is one request. A larger one is sent in 8 MiB parts that are read from the file as they go, so memory stays at a few parts. There is no per-request timeout; the whole upload is allowed two minutes plus the time the chunk needs at 256 KiB/s, and a timeout is retried at the next flush.

**Replaces the older keys.** `backend`, `secondary` and the `[storage.s3]`, `[storage.r2]` and `[storage.minio]` sections still work and are read as targets named `s3`, `r2` and `minio`; they cannot be combined with `[[storage.targets]]`, and the daemon refuses to start with both. Two small changes came with the move: `r2` now addresses its bucket in the URL path (it works the same), and an `[storage.s3]` with an IP address or bare host name as `endpoint` is addressed by path automatically, as before. To move to the list, write one target per store you use and keep the same names, so chunks waiting for upload keep their markers. With `backend = "local"` no target is used.

**Tested with:** requests to local fake servers, which check the URL, headers and body each store type sends and that a big chunk goes up in parts (these run with the tests); by hand, S3-style uploads, including a 20 MiB chunk in parts, to a local S3 server (moto) and Azure Blob Storage, including a 20 MiB chunk in parts, to the Azurite emulator, with credentials from environment variables, with encryption, and with one store down and back. Google Cloud Storage was only checked against the local fake server (the PUT it sends to the `gcs_base_url` of a service-account file) and a locally run `fake-gcs-server`, which accepts the connection but not the XML-API upload the crate uses, so a real upload to Google Cloud Storage is untested. Real AWS, Azure and GCS accounts were not used.

**Not covered:** stores with neither the S3 API nor Azure or Google's, such as SFTP or WebDAV. The upload code calls a single small `put` function, so one is an extra implementation of it and an extra `type`; ask if you need one. Queries only ever read the local data directory, so with `delete_after_upload` uploaded data is not searchable until it is restored (below).

### Restoring chunks

```
loghaven storage restore web                       # every chunk of app "web"
loghaven storage restore web --from 2024-05-01 --to 2024-05-07
loghaven storage restore web --target s3           # needed when several targets are configured
```

Copies an app's chunks from one target back into the data directory as plain chunks, so queries, `logs`, the TUI and the dashboard see them again. It can run while the daemon runs. Chunks that were uploaded encrypted are decrypted with the keys in `keys_dir` and `LOGHAVEN_STORAGE_KEY` (each object records which key made it); without the right key those chunks are reported and the others are still restored. `--from` and `--to` are inclusive `YYYY-MM-DD` days.

- **Checks:** an object is restored only if it is named `<app>/<date>/<chunk>.parquet` and, once decrypted, begins and ends with the Parquet magic number. Anything else is counted as skipped or reported as failed; nothing is left behind. When a chunk is in the store both encrypted and plain, the plain object is skipped. A chunk bigger than 1 GiB is refused.
- **Local files:** the chunk is marked synced before it appears, so the uploader never sees it unmarked, and it is put in place only if nothing is there, so an existing chunk is never replaced. A day or app directory that is a symbolic link is refused. The command exits with an error when any chunk failed or when the store holds nothing for the app.
- **Memory:** a chunk is downloaded into memory whole and held about two to three times over while it is decrypted, so the largest chunk (`rotate_size_mb`, 128 by default) costs a few hundred MB at most.
- **Pruning:** the pruner treats restored chunks like any other, so with `retention_days` or `max_size_gb` set it can remove them again at its next pass; raise those first if the restored days are old.

Only the S3-style, Azure and Google stores can be read; a store added later must implement `list` and `get` for this to work with it.
