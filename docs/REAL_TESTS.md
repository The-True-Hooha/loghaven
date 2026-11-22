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

**The store suite** (`store_suite` in `scripts/realtests/lib.sh`): encrypted upload with `delete_after_upload` (the stored object starts with the `LHENC2` header and holds none of the record text); restore; read-through queries (a query with `--from` reads the days that are only in the store, one without it does not fetch); key rotation (old and new uploads both restore); and a 100 MiB chunk, which goes up in parts where the store has parts, comes back with the same SHA-256 and is restored byte for byte.

**The chain suites** compare the watcher with what the node holds, not with what the test sent: Solana's faucet drops some airdrops, so the check is that every transaction that landed is recorded exactly once, across a restart and a soak.

## What is not covered

- **Solana on macOS.** The validator runs only under x86 emulation there, and its transaction port never receives. It passed on Linux.
- **Google Cloud Storage.** No server speaks the XML API the crate uses (the fake-gcs-server image rejects it) and there is no GCP account to run against. A real upload to GCS is untested.
- **Real AWS, Azure and GCS accounts.** MinIO, RustFS and Azurite speak the same APIs, but a billed service was not used.
- **`wss://` against a node of our own.** Anvil has no TLS and the watcher trusts only the built-in public roots. `wss://` was run against a public node earlier (see `docs/CONFIGURATION.md`).
- **Public chains for longer than a spot run, the dashboard in Firefox and WebKit, the TUI in a pseudo-terminal, Windows and macOS runners, and load and failure tests** are planned in `docs/PLAN.md` and have no results yet.

## Bugs these runs found

- **Stellar, HTTP 429 while listing accounts** (fixed): a poll threw away every account it had read when Horizon rate-limited one listing, and failed at the same place again. It now keeps what was read.
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
```

Each script starts its own containers and an isolated daemon (ports 9200 and 9100 must be free), prints `PASS` and `FAIL` lines and tears everything down. `SOAK_SECONDS` sets the soak length of the chain suites.
