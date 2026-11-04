# Manual testing

A full local round trip. Build once with `cargo build`, then use
`target/debug/loghaven` (below shortened to `loghaven`). To keep your real
config untouched, prefix commands with an empty home: `export HOME=/tmp/lh-home`.

```bash
loghaven init
loghaven auth keygen                              # before the first run: the daemon loads the public key at startup
loghaven config storage.local.flush_interval_secs 1   # default is 30; 1 makes logs appear quickly
loghaven config storage.backend local
loghaven run --foreground                         # terminal 1
```

In a second terminal:

```bash
loghaven send web "payment confirmed" --count 3 --chain ethereum --tx-hash 0xabc
loghaven logs web                                 # a session is minted automatically
loghaven logs web --level error --text timeout    # filters
loghaven logs web --follow                        # live tail; Ctrl+C to stop
loghaven status
loghaven stop
```

`logs --follow` re-queries a short window before the newest record it has seen, because records only become queryable after a flush. A record that is flushed later than twice the flush interval plus 5 s after it was received will not be shown. `--follow` cannot be combined with `--to`, and `--limit` only applies to the first fetch.

Auth by hand:

```bash
JWT=$(loghaven auth token web --ttl-secs 600)
curl -s -X POST localhost:9200/auth -H 'content-type: application/json' -d "{\"jwt\":\"$JWT\"}"
curl -s "localhost:9200/logs?app=web&limit=5" -H "Authorization: Bearer <token_id>"
```

Things worth trying:

- `loghaven send other "x"` then `loghaven logs web`: apps are isolated.
- A token for `web` against `?app=other` returns 403.
- `loghaven config auth.require_auth false` is not a CLI key; edit the config file to test plain ingest.
- Data lands in `<data dir>/<app>/<YYYY-MM-DD>/*.parquet` (inspect with DuckDB: `select * from 'path/*.parquet'`).
- Kill the daemon with `kill -9`, start it again: records flushed before the kill are still there.
