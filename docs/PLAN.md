# Plan: finish the open items, then test against real services

Everything below the line "Real-service tests" runs only after the build phases are merged.

## Build phases

### Phase 0: housekeeping
- Replace the README roadmap line "dedicated writer threads" with the real state: one writer task per app, disk work on blocking threads.
- Mark the `spawn_blocking` item in `docs/HARDENING_PLAN.md` done.
- Check the docs for other claims that no longer match the code.

### Phase 1: SFTP and WebDAV targets
- `type = "webdav"`: `url`, `username`, `password` or `password_env`, optional `prefix`. A small client over `reqwest` (PUT, GET, PROPFIND, MKCOL for parent collections).
- `type = "sftp"`: `host`, `port`, `username`, `key_file` or `password(_env)`, and a required pinned host key. Library: `russh` with `russh-sftp`.
- Both implement `put`, `list` and `get`, so encryption, markers, retries and `storage restore` work unchanged.
- Verified by fake-server tests, a fresh-agent review, mutation checks and live runs against containers (see Phase 5).

### Phase 2: download hardening (done)
- Stream downloads to the temporary file instead of holding the chunk in memory; replace the 1 GiB cap.
- Paginate listings and scale the timeout with the object count.

### Phase 3: remote data in queries (both options) (done)
- **A, kept:** `storage restore` copies chunks back explicitly.
- **B, added:** read-through queries. A query for a time range that has no local chunks fetches the missing chunks from one configured target into a cache directory, decrypts them and queries them as usual. The cache has a size cap and evicts the least recently used chunks. Egress per query is capped. Off unless `query.remote_target` is set.

### Phase 4: GCS correctness
- Fix anything Phase 5 finds in the Google Cloud Storage path.

## Real-service tests (store, Ethereum, Stellar and Solana suites done; see docs/REAL_TESTS.md)

Nothing mocked: real server software, real network, real browsers. Containers run under podman. Every run uses its own bucket or prefix and tears it down.

| Area | What runs |
|---|---|
| S3 API | MinIO and RustFS containers: plain and encrypted upload, a 100 MiB multipart chunk, `delete_after_upload`, restore, key rotation, read-through queries |
| Azure | Azurite container: account key and SAS token, multipart, restore |
| GCS | Decision pending: no trusted local server speaks the XML API the crate uses. Needs a real project, or a vetted emulator image |
| SFTP | OpenSSH server container: key and password login, wrong host key rejected, restore |
| WebDAV | WebDAV server container: upload, nested collections, restore |
| Chains, local | Real node software in containers: a private Stellar network (`stellar/quickstart`), Anvil (Ethereum), `solana-test-validator`. Includes `wss` against Anvil |
| Chains, public | Ethereum, Solana and Stellar public endpoints: a 30 minute run, then a 2 hour run. Checks cursor persistence across restarts, rate-limit handling, reconnects |
| Dashboard | Playwright on Chromium, Firefox and WebKit against a live daemon |
| TUI | A real pseudo-terminal session: resize, reconnect |
| Platforms | Build and run `scripts/e2e.sh` on Linux, macOS and Windows runners |
| Load and failure | UDP ingest at rising rates with loss counted, `kill -9` under load, disk full, store cut and restored mid-upload |

Results go into `docs/` with dates and versions. Each bug found is fixed in its own PR, and docs are corrected to state only what was run.

## Release
- v0.5.0 with release notes, cut after the real-service tests are green.
