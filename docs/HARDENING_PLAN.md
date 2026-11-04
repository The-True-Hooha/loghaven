# LogHaven hardening plan

Result of an end-to-end read of the codebase. Each step is implemented, tested,
checked with `cargo fmt --check && cargo clippy -- -D warnings && cargo test`,
reviewed by an independent adversarial pass (dod-design Phase 2), fixed once,
and committed on its own.

Tests live next to the code (`#[cfg(test)]`) and use temp dirs. No test touches
the network or the real config directory.

## Step 0 - Green baseline
Problem: clippy `-D warnings` failed (17 errors) and `cargo fmt --check` failed, so CI was red.
Change: `cargo fmt`, mechanical clippy fixes, ignore `.DS_Store`.
Done when: fmt, clippy, build pass.

## Step 1 - App name validation (path traversal)
Problem: `app` flows from the datagram payload and the query string into `data_path.join(app)`. `../../x` escapes the data directory (read and write).
Change: one `validate_app_name` (non-empty, <= 64 bytes, `[A-Za-z0-9._-]`, not `.`/`..`, no leading dot) enforced at ingest, `StorageRouter`, query handler and `scan_parquet`.
Tests: traversal strings, separators, NUL, empty, too long, unicode, valid names.

## Step 2 - Authorization scoping and replay protection
Problems:
- Ingest ignores the authenticated app and trusts `payload.app` (app A can write as app B).
- `/logs` lets any valid token read any app.
- Datagrams carry no freshness, so a captured datagram can be replayed until the session expires.
- `expires_in_secs` reports the JWT remaining time while the store enforces `session_ttl_secs`.
Change:
- Datagram header becomes `token_id:counter:hmac_hex\n`; HMAC covers `counter || payload`; the session keeps the highest accepted counter and rejects anything not strictly greater. Verification runs inline in the recv loop so ordering is preserved.
- Ingest requires `payload.app == session.app`.
- `/logs` requires `params.app == session.app`.
- `SessionData.expires_in_secs` reports the real session expiry.
Tests: replay, lower counter, wrong app, expired session, bad hmac, tampered counter, query cross-app.

## Step 3 - Storage durability and visibility
Problems:
- The open Parquet writer has no footer until rotation, so the current chunk is unreadable, a crash corrupts it, and one bad file fails a whole query.
- `expect("failed to create chunk writer")` kills the writer task.
- Tantivy commits only on rotation/shutdown and index errors are ignored.
Change:
- Every flush writes a complete chunk to `NNNNN.parquet.tmp` and atomically renames it. Chunks never exceed `rotate_records` records. Data is visible within `flush_interval_secs` and a crash loses at most the unflushed buffer.
- Writer errors are logged and the buffer is retained for retry up to a cap; the task never panics.
- Index commit on every flush; index errors are logged.
- `rotate_size_mb` is documented as advisory (flush threshold) or removed.
Tests: flush produces readable file, crash leftovers (`.tmp`) ignored and cleaned, rotate by record count, date rollover, unwritable directory does not kill the task, shutdown flushes.

## Step 4 - Query correctness
Problems: `text` filter ignored (index never passed), results oldest-first, one unreadable file fails the whole query, from-only/to-only filters do not prune, `unsafe` raw pointers in the reader.
Change:
- Query state holds the `StorageRouter`; text queries use the tantivy index when present, otherwise a case-insensitive substring scan of message/tags/metadata.
- Newest-first ordering with early stop.
- Unreadable chunks are skipped with a warning.
- Date pruning for open-ended ranges.
- Safe column access in the Parquet reader.
- `limit` capped server-side.
Tests: text match and miss, ordering, limit, filters, corrupt file skipped, open-ended ranges, empty app dir.

## Step 5 - Cloud sync safety
Problems: `delete_after_upload` hardcoded true makes uploaded data unqueryable; uploads are fire-and-forget and lost on shutdown; failed upload is never retried.
Change: `storage.delete_after_upload` config (default false), uploads tracked and awaited on shutdown, failed uploads retried on next flush/startup (scan for chunks without a `.synced` marker).
Tests: key construction, marker logic, delete flag (upload itself behind a trait so tests need no network).

## Step 6 - Pruner safety
Problems: pruner treats `<app>/index` as a date directory and can delete the live index under size pressure; non-date directories are compared as strings.
Change: only `YYYY-MM-DD` directories are ever pruned; the index is excluded from the size limit deletions; pruner also runs once at startup.
Tests: cutoff boundary, non-date dirs untouched, size limit deletes oldest first, index survives.

## Step 7 - Daemon, CLI, config hygiene
Problems: config file named `config_toml`; PID file not removed on signal shutdown; `Reload` claims success but does nothing; `loghaven logs` does not URL-encode; unused dependencies; session store lost on restart (documented, not changed).
Change: `config.toml` (read the old name as fallback), PID cleanup on shutdown, `Reload` returns an explicit error, `reqwest` `.query()` for params, remove unused deps, README corrections.
Tests: config path, URL building, IPC protocol round trip.

## Step 8 - Integration test and final review
End-to-end test: start ingest + query on ephemeral ports against a temp dir, send signed datagrams, query them back. Full-repo adversarial pass. README and this document updated.

## Out of scope
Chain adapters, TUI/dashboard, multi-backend `primary`/`secondary`, client-side encryption, persistent sessions.
