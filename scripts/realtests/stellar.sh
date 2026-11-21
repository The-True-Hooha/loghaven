#!/usr/bin/env bash
# The Stellar adapter against a real private network (stellar/quickstart in a container: core and
# Horizon with a friendbot). Each new account is funded by the friendbot, which is one real
# transaction per account. Covers backfill, operations, a restart and a SOAK_SECONDS soak.
#
# Usage: scripts/realtests/stellar.sh [path to loghaven]
set -uo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
BIN=${1:-$ROOT/target/debug/loghaven}
BIN=$(cd "$(dirname "$BIN")" && pwd)/$(basename "$BIN")
SOAK=${SOAK_SECONDS:-60}
AREA="Stellar (quickstart)"
. "$ROOT/scripts/realtests/lib.sh"

container lh-stellar -p 18000:8000 docker.io/stellar/quickstart:latest --local --enable core,horizon
WORK=$(mktemp -d /tmp/lhxlm.XXXXXX)
WATCH_PID=
stop_watch() { [ -n "$WATCH_PID" ] && kill "$WATCH_PID" 2>/dev/null; WATCH_PID=; }
cleanup() { stop_watch; stop_daemon; drop_containers; [ "$FAILED" = 0 ] && rm -rf "$WORK" || echo "kept $WORK"; }
trap cleanup EXIT

H=http://127.0.0.1:18000
ledger() { curl -s "$H/" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("history_latest_ledger",0))' 2>/dev/null; }
wait_for 600 '[ "$(ledger)" -gt 2 ] 2>/dev/null && curl -s -o /dev/null -w "%{http_code}" "$H/friendbot" | grep -qv 000' \
  || { echo "the network did not start"; podman_ logs lh-stellar 2>&1 | tail -20; exit 1; }
check "the private network is closing ledgers" '[ "$(ledger)" -gt 2 ]'

N=$((SOAK / 3 + 12)); [ "$N" -gt 20 ] && N=20  # Horizon here allows 20 requests a second; a poll makes N+1
ACCOUNTS=($(python3 "$ROOT/scripts/realtests/stellar_keys.py" "$N"))
fund() { curl -s -o /dev/null -w '%{http_code}' "$H/friendbot?addr=$1"; }
LIST=$(printf '"%s", ' "${ACCOUNTS[@]}"); LIST=${LIST%, }

new_home "$WORK/home"
perl -0pi -e 's/enabled = \[\]/enabled = ["stellar"]/' "$CONFIG"
cat >>"$CONFIG" <<TOML

[chains.stellar]
horizon_url = "$H"
accounts = [$LIST]
poll_interval_secs = 3
operations = true
TOML
start_daemon || exit 1

echo "== a real network"
"$BIN" watch stellar >"$WORK/watch.log" 2>&1 &
WATCH_PID=$!
check "the first run starts at the tip and records nothing" 'wait_for 30 "grep -q Watching $WORK/watch.log"'
sleep 8
[ "$(count stellar)" = 0 ] && pass "nothing is recorded before any account has a transaction" || fail "nothing is recorded before any account has a transaction"
for a in "${ACCOUNTS[@]:0:3}"; do fund "$a" >/dev/null; done
check "the first three funded accounts are recorded" 'wait_for 60 "[ \"\$(count stellar)\" = 3 ]"'
T0=$(date +%s)
fund "${ACCOUNTS[3]}" >/dev/null
check "a new transaction is recorded within a few ledgers" 'wait_for 30 "[ \"\$(count stellar)\" = 4 ]"'
echo "   latency: $(( $(date +%s) - T0 )) s"
check "records carry the operation types (text search reaches metadata)" '[ "$(count stellar --text create_account)" -ge 1 ]'
check "a record can be traced by hash" 'H1=$("$BIN" logs stellar --limit 1 2>&1 | grep -o "[0-9a-f]\{64\}" | head -1); [ -n "$H1" ] && "$BIN" trace "$H1" --app stellar 2>&1 | grep -qi "$H1\|ledger"'

echo "== restart keeps the cursor"
stop_watch
fund "${ACCOUNTS[4]}" >/dev/null; fund "${ACCOUNTS[5]}" >/dev/null
"$BIN" watch stellar >"$WORK/watch2.log" 2>&1 &
WATCH_PID=$!
check "two transactions made while the watcher was down are recorded once" 'wait_for 60 "[ \"\$(count stellar)\" = 6 ]"'
sleep 5
check "nothing is repeated after the restart" '[ "$(count stellar)" = 6 ]'

echo "== soak ($SOAK s, one transaction every 3 s)"
SENT=6
END=$(( $(date +%s) + SOAK ))
while [ "$(date +%s)" -lt "$END" ] && [ "$SENT" -lt "$N" ]; do fund "${ACCOUNTS[$SENT]}" >/dev/null; SENT=$((SENT+1)); sleep 3; done
check "every transaction of the soak is recorded exactly once" 'wait_for 60 "[ \"\$(count stellar)\" = $SENT ]"'
check "the watcher is still running" 'kill -0 "$WATCH_PID" 2>/dev/null'
check "the watcher log has no error" '! grep -qi "error" "$WORK/watch.log" "$WORK/watch2.log"'

summary
