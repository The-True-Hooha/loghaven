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
| stop the daemon | error reaching the query server, cursor unchanged (`--once` exits; without it the watcher retries) |
| `cat /tmp/lh-state/lastfilter.json` | the address list and, with `topics` set, `"topics": [[...]]` |
| `loghaven trace 0x…03de --app ethereum` | the event for block 990 (`%064x` of 990) |

### `watch solana` against the fake node

```bash
mkdir /tmp/lh-sol-state && echo 100 > /tmp/lh-sol-state/tip
python3 scripts/mock_sol_node.py /tmp/lh-sol-state &
```

Add to `config.toml` (and set `enabled = ["solana"]` in the existing `[chains]`):

```toml
[chains.solana]
rpc_url = "http://127.0.0.1:18899/SECRETKEY"
addresses = ["So11111111111111111111111111111111111111112"]
max_signatures = 30
```

Transactions `1..tip` touch the address; every 7th failed. A signature is 64 `S`
followed by the number, digits written as `a`..`j` (transaction 105 is `S…Sbaf`).

| Try | Expect |
|-----|--------|
| `loghaven watch solana --once` | nothing recorded; `solana.cursor` holds the tip (`…baa`) |
| `echo 110 > /tmp/lh-sol-state/tip`, run again | `Recorded 10 event(s)` |
| the same again | nothing new; cursor unchanged |
| `echo 200 > /tmp/lh-sol-state/tip`, run again | three batches of 30, then caught up |
| `loghaven trace S…Sbaf --app solana` | one `error` record, `failed` in the message |
| the same with the signature in lower case | 0 records (case sensitive) |
| `echo 1 > /tmp/lh-sol-state/fail` | `--once` errors with `rpc getTransaction error`, cursor unchanged; `watch solana` without `--once` retries, and after 5 tries records the transaction with `logs_error` and moves on |
| `echo 1 > /tmp/lh-sol-state/nulltx` | recorded with `logs_available: false` (the mock's block times are old, so it counts as pruned) |
| `echo 1 > /tmp/lh-sol-state/big` | recorded, `logs_truncated: true` |
| `echo AAAA… > /tmp/lh-sol-state/genesis` | error: cursor was saved for another cluster; exit code 1 |
| stop the mock node | error without the RPC URL |
| stop the daemon | error reaching the query server, cursor unchanged (`--once` exits; without it the watcher retries) |

### `ws_url` against anvil

Anvil serves HTTP and WebSocket on one port. Start it with `anvil --block-time 2`, deploy a
contract that emits an event, and set in `[chains.ethereum]`:

```toml
rpc_url = "http://127.0.0.1:8545"
ws_url = "ws://127.0.0.1:8545"
poll_interval_secs = 60
confirmations = 0
```

| Try | Expect |
|-----|--------|
| `loghaven watch ethereum`, then send a transaction to the contract | the banner says `new blocks over WebSocket`; `Recorded 1 event(s)` within a few seconds, not after 60 |
| remove `ws_url`, restart, send a transaction | nothing for up to 60 seconds |
| set `ws_url = "ws://127.0.0.1:8545/nope"` | a warning `websocket connect failed: HTTP error: 404`; events still arrive on the interval |
| stop anvil while watching | warnings once per kind, no crash; the watcher recovers when anvil is back |

### `watch stellar` against the fake Horizon

```bash
mkdir /tmp/lh-xlm-state && echo 100 > /tmp/lh-xlm-state/tip
python3 scripts/mock_horizon.py /tmp/lh-xlm-state &
```

Add to `config.toml` (and set `enabled = ["stellar"]` in the existing `[chains]`):

```toml
[chains.stellar]
horizon_url = "http://127.0.0.1:18000"
accounts = ["GAAZI4TCR3TY5OJHCTJC2A4QSY6CJWJH5IAJTGKIN2ER7LBNVKOCCWN7"]
max_records = 30
```

Transaction `n` has hash `%064x` of `n`; every 7th failed.

| Try | Expect |
|-----|--------|
| `loghaven watch stellar --once` | nothing recorded; `stellar.cursor` holds the newest token (transaction 100 is `409600`) |
| `echo 110 > /tmp/lh-xlm-state/tip`, run again | `Recorded 10 event(s)` |
| the same again | nothing new |
| `echo 200 > /tmp/lh-xlm-state/tip`, run three times | 30 events each time, then caught up |
| `loghaven trace $(printf '%064x' 105) --app stellar` | one `error` record, `failed` in the message |
| `echo 1 > /tmp/lh-xlm-state/bigmemo` | recorded, memo cut at 128 characters |
| `echo 503 > /tmp/lh-xlm-state/status` | error `HTTP 503 Service Unavailable`; cursor unchanged |
| `echo Public > /tmp/lh-xlm-state/passphrase` | error: cursor was saved for another network; exit code 1 |
| stop the daemon | error reaching the query server, cursor unchanged (`--once` exits; without it the watcher retries) |

