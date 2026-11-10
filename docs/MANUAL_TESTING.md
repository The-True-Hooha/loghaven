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

## Phase 2: `trace` and `watch ethereum`

Use a throwaway `HOME` with a short path (the daemon's Unix socket path has a
length limit, about 100 bytes on macOS): `export HOME=/tmp/lh-m`. Set
`flush_interval_secs = 2` in `config.toml` to see records quickly.

### `trace`

```bash
TX=0x$(printf 'ab%.0s' $(seq 32))
loghaven send web "submit"  --chain ethereum --tx-hash $TX
loghaven send api "indexed" --chain ethereum --tx-hash $TX
loghaven trace $TX --app web --app api       # 2 rows, oldest first
loghaven trace 0xAB...AB --app web           # upper-case hex still matches
loghaven trace 0xdead --app web              # 0 records
loghaven trace $TX --app web --app web       # no duplicate rows
loghaven trace $TX --app web --app api --token x   # error: --token is for one app
```

### `watch ethereum` against the fake node

```bash
mkdir /tmp/lh-state && echo 1000 > /tmp/lh-state/head
python3 scripts/mock_eth_node.py /tmp/lh-state &
```

Add to `config.toml` (and set `enabled = ["ethereum"]` in the existing `[chains]`):

```toml
[chains.ethereum]
rpc_url = "http://127.0.0.1:18545/SECRETKEY"
addresses = ["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]
start_block = 980
max_block_range = 100
```

| Try | Expect |
|-----|--------|
| `loghaven watch ethereum --once` | `Recorded 2 event(s)`; `ethereum.cursor` is 995 |
| the same again | nothing new; cursor unchanged |
| `echo 1100 > /tmp/lh-state/head`, run again | 10 events, no repeats |
| `echo 30 > /tmp/lh-state/maxspan` | still progresses, in smaller steps |
| `echo 1 > /tmp/lh-state/reject` | error `block N cannot be read even alone`; cursor unchanged |
| `echo 1 > /tmp/lh-state/nulltime` | error `block has no timestamp`; cursor unchanged |
| `echo 1 > /tmp/lh-state/removed` | logs marked removed are not recorded |
| `echo 1 > /tmp/lh-state/big` | event recorded with `data_truncated: true` in `metadata` (see the HTTP API) |
| stop the mock node | error without the RPC URL (`SECRETKEY` never appears) |
| `echo junk > ethereum.cursor` | error: not a block number |
| `echo 99999999 > ethereum.cursor` | error: far ahead of the chain head |
| change `addresses`, then run again | error: cursor was saved for another chain or other addresses/topics |
| `echo 137 > /tmp/lh-state/chainid`, run again | the same error |
| stop the daemon | error reaching the query server; cursor unchanged |
| `cat /tmp/lh-state/lastfilter.json` | the address list and, with `topics` set, `"topics": [[...]]` |
| `loghaven trace 0x…03de --app ethereum` | the event for block 990 (`%064x` of 990) |
