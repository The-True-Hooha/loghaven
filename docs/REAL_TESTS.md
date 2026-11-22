# Real-service tests

Nothing here is mocked: each suite runs real server or node software and the real `loghaven` daemon, and checks what ended up in the store or the log. The suites live in `scripts/realtests` and run on a Linux runner through `.github/workflows/real-services.yml` (by hand, and on any change to the suites). The same scripts run on a developer machine with podman.

## Results

Run on 2026-11-21 and 2026-11-22. "Linux" is a GitHub-hosted `ubuntu-latest` runner; "macOS" is an Apple-silicon laptop with a podman machine.

| Area | Software (version) | Where it passed | What the suite checks |
|---|---|---|---|
| S3 API | MinIO `2025-05-24` | Linux, macOS | store suite (below) |
| S3 API | RustFS `1.0.6` | Linux, macOS | store suite |
| Azure Blob | Azurite `3.38.0` (account key, and an account SAS token) | Linux, macOS | store suite, once with each credential |
| SFTP | OpenSSH `8.4p1` (atmoz/sftp) | Linux, macOS | store suite, plus key login, a wrong password and a wrong host key |
| WebDAV | Apache `2.4.37` mod_dav (bytemark/webdav) | Linux, macOS | store suite |
| Ethereum | Anvil `1.9.0` | Linux, macOS | an event contract, `ws://` new-block push, restart, 60 s soak |
| Stellar | stellar/quickstart (core and Horizon, a private network with a friendbot) | Linux, macOS | first run at the tip, operations, restart, 60 s soak |
| Solana | Agave test validator `2.3.13` | Linux only | recording, restart, 60 s soak |

| Load | the daemon, UDP ingest from a Python sender (`load.sh`) | Linux, macOS | 500 to 20 000 records/s for 10 s each, loss and duplicates counted; `kill -9` under load |
| Disk full | the daemon on a 16 MiB volume (`diskfull.sh`) | macOS only (the script needs `sudo mount` on Linux, not run) | writes fail, the daemon stays up, stored records stay queryable, writes resume |
| Store cut | MinIO stopped and started under the daemon (`storecut.sh`) | Linux, macOS | store down between uploads; store cut during a 100 MiB upload |

**The store suite** (`store_suite` in `scripts/realtests/lib.sh`): encrypted upload with `delete_after_upload` (the stored object starts with the `LHENC2` header and holds none of the record text); restore; read-through queries (a query with `--from` reads the days that are only in the store, one without it does not fetch); key rotation (old and new uploads both restore); and a 100 MiB chunk, which goes up in parts where the store has parts, comes back with the same SHA-256 and is restored byte for byte.

**The chain suites** compare the watcher with what the node holds, not with what the test sent: Solana's faucet drops some airdrops, so the check is that every transaction that landed is recorded exactly once, across a restart and a soak.

**Load results.** On Linux the sender reached 500, 2 000, 5 000 and 10 000 records/s with every record stored once (0 lost, 0 duplicated). At a 20 000 records/s target the Python sender managed 12 600/s on the runner (19 700/s on the laptop) and again lost none. No overload point was found: the limit here was the sender, not the daemon. Records are one per datagram; larger batches were not run.

**`kill -9`.** The daemon was killed 4 s into a 5 000 records/s run with a 1 s flush interval, and restarted. Records sent after the kill are lost because the sender had stopped. About a quarter of the sent records were not stored (24.9% on Linux, 24.3% on macOS): those are the records after the kill that the sender did not send plus those still buffered in memory when the process died. The run does not separate the two, so it shows no duplicates and a working restart, not how much buffered data a crash costs. A `.parquet.tmp` file cut off by the kill stays until the app writes again, then is removed.

## What is not covered

- **Solana on macOS.** The validator runs only under x86 emulation there, and its transaction port never receives. It passed on Linux.
- **Google Cloud Storage.** No server speaks the XML API the crate uses (the fake-gcs-server image rejects it) and there is no GCP account to run against. A real upload to GCS is untested.
- **Real AWS, Azure and GCS accounts.** MinIO, RustFS and Azurite speak the same APIs, but a billed service was not used.
- **`wss://` against a node of our own.** Anvil has no TLS and the watcher trusts only the built-in public roots. `wss://` was run against a public node earlier (see `docs/CONFIGURATION.md`).
- **Disk full on Linux**, load above what the Python sender can produce, and load with several apps at once.
- **Public chains for longer than a spot run, the dashboard in Firefox and WebKit, the TUI in a pseudo-terminal, and Windows and macOS runners** are planned in `docs/PLAN.md` and have no results yet.

## Bugs these runs found

- **Stellar, HTTP 429 while listing accounts** (fixed): a poll threw away every account it had read when Horizon rate-limited one listing, and failed at the same place again. It now keeps what was read.
- **Failed upload never retried** (fixed): after the store came back, a chunk whose upload had failed was uploaded only when the app wrote another chunk or the daemon restarted. An app that went quiet kept the chunk forever. The writer now retries once a minute.
- Everything else that failed in the first runs was a mistake in the test (a first run starts at the tip by design; podman cannot mount `/tmp` on macOS; the Solana watcher reads finalized transactions, so the test has to wait for finality; a faucet that drops airdrops).

## Running them

```
cargo build
LH_BIG_CHUNK=/tmp/big.parquet LH_BIG_CHUNK_MB=100 cargo test make_big_chunk -- --ignored
scripts/realtests/s3.sh minio target/debug/loghaven /tmp/big.parquet     # or rustfs
scripts/realtests/azure.sh target/debug/loghaven /tmp/big.parquet
scripts/realtests/sftp.sh target/debug/loghaven /tmp/big.parquet
scripts/realtests/webdav.sh target/debug/loghaven /tmp/big.parquet
scripts/realtests/ethereum.sh target/debug/loghaven
scripts/realtests/stellar.sh target/debug/loghaven
podman build --platform linux/amd64 -t lh-solana-test:2.3.13 -f scripts/realtests/solana.Containerfile .
scripts/realtests/solana.sh target/debug/loghaven
scripts/realtests/load.sh target/debug/loghaven
scripts/realtests/diskfull.sh target/debug/loghaven
scripts/realtests/storecut.sh target/debug/loghaven /tmp/big.parquet
```

Each script starts its own containers and an isolated daemon (ports 9200 and 9100 must be free), prints `PASS` and `FAIL` lines and tears everything down. `SOAK_SECONDS` sets the soak length of the chain suites.
