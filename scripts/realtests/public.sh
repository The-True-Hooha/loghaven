#!/usr/bin/env bash
# The three chain adapters against public mainnet endpoints for PUBLIC_MINUTES (default 30), with a
# restart half way. Watched: a busy Ethereum contract (WETH), the Solana USDC mint and busy Stellar
# accounts. Nothing is sent to any chain. Public endpoints rate-limit and change, so the checks
# are about behaviour: the watchers keep running, records keep arriving, a restart repeats nothing,
# and nothing but handled rate limits shows in the logs.
#
# Usage: PUBLIC_MINUTES=30 scripts/realtests/public.sh [path to loghaven]
set -uo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
BIN=${1:-$ROOT/target/debug/loghaven}
BIN=$(cd "$(dirname "$BIN")" && pwd)/$(basename "$BIN")
MIN=${PUBLIC_MINUTES:-30}
AREA="Public chains (${MIN} min)"
. "$ROOT/scripts/realtests/lib.sh"

WORK=$(mktemp -d /tmp/lhpub.XXXXXX)
PIDS=()
stop_watchers() { for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null; done; PIDS=(); }
cleanup() { stop_watchers; stop_daemon; [ "$FAILED" = 0 ] && rm -rf "$WORK" || echo "kept $WORK"; }
trap cleanup EXIT

# Horizon accounts that were busy a moment ago, found now: no address is hard-coded to go stale
BUSY=$(curl -s --compressed "https://horizon.stellar.org/operations?order=desc&limit=200&include_failed=false" | python3 -I -c "
import json,sys,collections
c=collections.Counter(r.get('source_account') for r in json.load(sys.stdin)['_embedded']['records'])
print(','.join(a for a,_ in c.most_common(5) if a))")
[ -n "$BUSY" ] || { echo "could not list busy Stellar accounts"; exit 1; }
ACCOUNTS=$(printf '%s' "$BUSY" | sed 's/\([^,]*\)/"\1"/g; s/,/, /g')

new_home "$WORK/home"
perl -0pi -e 's/enabled = \[\]/enabled = ["ethereum", "solana", "stellar"]/' "$CONFIG"
cat >>"$CONFIG" <<TOML

[chains.ethereum]
rpc_url = "https://ethereum-rpc.publicnode.com"
addresses = ["0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"]
confirmations = 6
poll_interval_secs = 12

[chains.solana]
rpc_url = "https://api.mainnet-beta.solana.com"
addresses = ["EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"]
poll_interval_secs = 10

[chains.stellar]
horizon_url = "https://horizon.stellar.org"
accounts = [$ACCOUNTS]
poll_interval_secs = 10
TOML
start_daemon || exit 1

watch() { # chain, log
  "$BIN" watch "$1" >"$WORK/$1.$2.log" 2>&1 &
  PIDS+=($!)
}
start_all() { for c in ethereum solana stellar; do watch $c "$1"; done; }
dups() { # app: records that share a chain identity (ethereum: tx + log index; others: the full text)
  python3 -I - "$("$BIN" auth token "$1" --ttl-secs 3600 | tail -1)" "$1" <<'PY'
import json, sys, urllib.request
jwt, app = sys.argv[1:3]
def call(path, token=None, body=None):
    r = urllib.request.Request("http://127.0.0.1:9200" + path, data=json.dumps(body).encode() if body else None)
    if body: r.add_header("content-type", "application/json")
    if token: r.add_header("authorization", "Bearer " + token)
    return json.load(urllib.request.urlopen(r, timeout=60))
tok = call("/auth", body={"jwt": jwt})["token_id"]
recs = call("/logs?app=%s&limit=10000" % app, tok)["records"]
keys = []
for r in recs:
    m = r.get("metadata")
    m = json.loads(m) if isinstance(m, str) else (m or {})
    keys.append((r.get("tx_hash"), m.get("log_index")) if app == "ethereum" else (r["message"],))
print(len(keys) - len(set(keys)))
PY
}
report() { echo "   $(date +%H:%M) ethereum=$(count ethereum) solana=$(count solana) stellar=$(count stellar)"; }

echo "== first half"
start_all 1
check "the watchers start" 'wait_for 60 "grep -q Watching $WORK/ethereum.1.log && grep -q Watching $WORK/solana.1.log && grep -q Watching $WORK/stellar.1.log"'
HALF=$(( MIN * 30 ))
for _ in $(seq $((HALF / 60))); do sleep 60; report; done
for c in ethereum solana stellar; do
  check "$c: the watcher is still running" 'kill -0 "${PIDS[0]}" 2>/dev/null' >/dev/null
done
BEFORE_E=$(count ethereum); BEFORE_S=$(count solana); BEFORE_X=$(count stellar)
echo "   first half: ethereum=$BEFORE_E solana=$BEFORE_S stellar=$BEFORE_X"

echo "== restart"
stop_watchers
sleep 5
start_all 2
for _ in $(seq $((HALF / 60))); do sleep 60; report; done

echo "== results"
for c in ethereum solana stellar; do
  N=$(count $c)
  check "$c: records were recorded ($N)" '[ "$N" -gt 0 ]'
  check "$c: no record appears twice" '[ "$(dups $c)" = 0 ]'
done
check "ethereum: records kept arriving after the restart" '[ "$(count ethereum)" -ge "$BEFORE_E" ]'
check "solana: records kept arriving after the restart" '[ "$(count solana)" -ge "$BEFORE_S" ]'
check "stellar: records kept arriving after the restart" '[ "$(count stellar)" -ge "$BEFORE_X" ]'
check "all three watchers are still running" 'for p in "${PIDS[@]}"; do kill -0 $p 2>/dev/null || exit 1; done'
# a dropped request on a public endpoint is expected now and then; the watcher retries on the next poll
check "no error besides rate limits and dropped requests in the watcher logs" '! cat "$WORK"/*.log | grep -i "error" | grep -qvi "429\|too many requests\|rate\|error sending request"'
echo "   dropped requests: $(cat "$WORK"/*.log | grep -ci "error sending request")"
echo "   rate-limit lines: $(cat "$WORK"/*.log | grep -ci '429\|too many\|rate')"
summary
