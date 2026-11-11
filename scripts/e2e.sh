#!/usr/bin/env bash
# End-to-end checks against a real daemon in an isolated HOME: sessions across a restart,
# the chain adapters against the fake nodes in scripts/, the dashboard in headless
# Chromium and the terminal UI in a pseudo-terminal.
#
# Usage: scripts/e2e.sh [path to the loghaven binary]
# Needs python3, curl, node with `playwright` installed (npm i playwright && npx playwright install chromium).
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
BIN=${1:-$ROOT/target/debug/loghaven}
BIN=$(cd "$(dirname "$BIN")" && pwd)/$(basename "$BIN")
WORK=$(mktemp -d /tmp/lhe2e.XXXXXX)
REAL_HOME=$HOME
export HOME=$WORK/h
mkdir -p "$HOME" "$WORK/eth" "$WORK/sol" "$WORK/xlm"
FAILED=0

pass() { echo "PASS $1"; }
fail() { echo "FAIL $1"; FAILED=1; }
check() { if eval "$2"; then pass "$1"; else fail "$1"; fi; }

cleanup() {
  "$BIN" stop >/dev/null 2>&1 || true
  pkill -P $$ 2>/dev/null || true
  [ "$FAILED" = 0 ] && rm -rf "$WORK" || echo "kept $WORK (daemon log: $WORK/daemon.log)"
}
trap cleanup EXIT

CONFIG=$("$BIN" init 2>&1 | sed -n 's/.*Edit config: //p')
[ -f "$CONFIG" ] || { echo "no config at '$CONFIG'"; exit 1; }
"$BIN" auth keygen >/dev/null

python3 - "$CONFIG" <<'PY'
import re, sys
p = sys.argv[1]
s = open(p).read()
s = s.replace("flush_interval_secs = 30", "flush_interval_secs = 1")
s = s.replace("enabled = []", 'enabled = ["ethereum", "solana", "stellar"]', 1)
s += """
[chains.ethereum]
rpc_url = "http://127.0.0.1:18545"
addresses = ["0x%s"]
confirmations = 6
start_block = 980

[chains.solana]
rpc_url = "http://127.0.0.1:18899"
addresses = ["So11111111111111111111111111111111111111112"]
backfill = 3

[chains.stellar]
horizon_url = "http://127.0.0.1:18000"
accounts = ["GAAZI4TCR3TY5OJHCTJC2A4QSY6CJWJH5IAJTGKIN2ER7LBNVKOCCWN7"]
backfill = 3
""" % ("aa" * 20)
open(p, "w").write(s)
PY

start_daemon() {
  (nohup "$BIN" run >>"$WORK/daemon.log" 2>&1 &)
  for _ in $(seq 50); do curl -s -o /dev/null http://127.0.0.1:9200/dashboard && return 0; sleep 0.2; done
  echo "daemon did not start"; cat "$WORK/daemon.log"; exit 1
}
stop_daemon() { "$BIN" stop >/dev/null; sleep 1; }
count() { "$BIN" logs "$1" --limit 1000 2>&1 | sed -n 's/^✓ \([0-9]*\) records.*/\1/p'; }
wait_count() { # app, wanted
  for _ in $(seq 40); do [ "$(count "$1")" = "$2" ] && return 0; sleep 0.5; done
  return 1
}

start_daemon

echo "== records and sessions across a restart"
TX=0x$(printf 'cd%.0s' $(seq 32))
"$BIN" send web 'plain hello' >/dev/null
"$BIN" send web '<img src=x onerror="window.PWNED=1"><script>window.PWNED=2</script>' --level error --tx-hash "$TX" >/dev/null
"$BIN" send web 'payment confirmed' --level warn --chain ethereum --tx-hash "$TX" >/dev/null
"$BIN" send tuiapp $'evil \x1b[2J\x1b]0;pwned\x07 line' >/dev/null
check "records become queryable" 'wait_count web 3'
URL=$("$BIN" dashboard --app web --no-open)
TOKEN=${URL##*token=}
status() { curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $1" 'http://127.0.0.1:9200/logs?app=web'; }
check "session works" '[ "$(status "$TOKEN")" = 200 ]'
check "wrong token is refused" '[ "$(status "${TOKEN:0:60}0000")" = 401 ]'
sleep 2
stop_daemon
start_daemon
check "session works after a restart" '[ "$(status "$TOKEN")" = 200 ]'
kill -9 "$(pgrep -f "$BIN run" | head -1)"; sleep 1
start_daemon
check "session works after kill -9" '[ "$(status "$TOKEN")" = 200 ]'
check "a foreign Host is refused" '[ "$(curl -s -o /dev/null -w "%{http_code}" -H "Host: evil.example" http://127.0.0.1:9200/logs?app=web)" = 403 ]'

echo "== dashboard in headless Chromium"
if command -v node >/dev/null && (cd "$ROOT/scripts" && node -e "require('playwright')" 2>/dev/null); then
  check "dashboard script" 'HOME=$REAL_HOME node "$ROOT/scripts/dashboard_e2e.js" "$("$BIN" dashboard --app web --no-open)" "$WORK" > "$WORK/dashboard.out" 2>&1 || { cat "$WORK/dashboard.out"; false; }'
else
  fail "dashboard script (playwright is not installed)"
fi

echo "== terminal UI in a pseudo-terminal"
check "tui" 'python3 "$ROOT/scripts/tui_e2e.py" "$BIN" tuiapp "evil" > "$WORK/tui.out" 2>&1 || { cat "$WORK/tui.out"; false; }'

echo "== chain adapters against the fake nodes"
echo 1000 > "$WORK/eth/head"
echo 100 > "$WORK/sol/tip"
echo 100 > "$WORK/xlm/tip"
python3 "$ROOT/scripts/mock_eth_node.py" "$WORK/eth" >/dev/null 2>&1 &
python3 "$ROOT/scripts/mock_sol_node.py" "$WORK/sol" >/dev/null 2>&1 &
python3 "$ROOT/scripts/mock_horizon.py" "$WORK/xlm" >/dev/null 2>&1 &
sleep 1
"$BIN" watch ethereum --once >/dev/null 2>&1
check "ethereum records the logs of blocks 980 and 990" 'wait_count ethereum 2'
for chain in solana stellar; do
  "$BIN" watch "$chain" --once >/dev/null 2>&1
  "$BIN" watch "$chain" --once >/dev/null 2>&1
  check "$chain backfill records the newest 3 transactions" "wait_count $chain 3"
done
echo 103 > "$WORK/sol/tip"
echo 103 > "$WORK/xlm/tip"
for chain in solana stellar; do
  "$BIN" watch "$chain" --once >/dev/null 2>&1
  check "$chain follows new transactions" "wait_count $chain 6"
done
# the mock writes transaction 103 as 64 'S' and the digits 1,0,3 as b,a,d
SIG=$(printf 'S%.0s' $(seq 64))bad
check "trace finds a Solana transaction by signature" '"$BIN" trace "$SIG" --app solana 2>&1 | grep -q "slot"'

[ "$FAILED" = 0 ] && echo "ALL PASSED" || { echo "SOME FAILED"; exit 1; }
