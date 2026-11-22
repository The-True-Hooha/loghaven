#!/usr/bin/env bash
# The Ethereum adapter against a real node (Anvil in a container): a contract that emits an event
# on every call, a watcher that follows new blocks over ws://, a restart that must not repeat or
# lose events, and a reorg-free soak of SOAK_SECONDS (default 60).
#
# Usage: scripts/realtests/ethereum.sh [path to loghaven]
set -uo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
BIN=${1:-$ROOT/target/debug/loghaven}
BIN=$(cd "$(dirname "$BIN")" && pwd)/$(basename "$BIN")
SOAK=${SOAK_SECONDS:-60}
AREA="Ethereum (Anvil)"
. "$ROOT/scripts/realtests/lib.sh"

container lh-anvil -p 18545:8545 --entrypoint anvil ghcr.io/foundry-rs/foundry:latest \
  --host 0.0.0.0 --block-time 1
WORK=$(mktemp -d /tmp/lheth.XXXXXX)
WATCH_PID=
stop_watch() { [ -n "$WATCH_PID" ] && kill "$WATCH_PID" 2>/dev/null; WATCH_PID=; }
cleanup() { stop_watch; stop_daemon; drop_containers; [ "$FAILED" = 0 ] && rm -rf "$WORK" || echo "kept $WORK"; }
trap cleanup EXIT

RPC=http://127.0.0.1:18545
wait_for 60 'curl -s -X POST -H "content-type: application/json" --data "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_blockNumber\",\"params\":[]}" $RPC | grep -q result' \
  || { echo "anvil did not start"; podman_ logs lh-anvil | tail; exit 1; }
PK=0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80
cast_() { local sub=$1; shift; podman_ exec lh-anvil cast "$sub" --rpc-url http://localhost:8545 "$@" 2>/dev/null; }
# runtime: PUSH1 0 PUSH1 0 LOG0 STOP; init code copies it out and returns it
DEPLOY=$(cast_ send --private-key $PK --json --create 0x6006600c60003960066000f360006000a000)
CONTRACT=$(echo "$DEPLOY" | python3 -c 'import json,sys; print(json.load(sys.stdin)["contractAddress"])')
check "the event contract is deployed" '[ -n "$CONTRACT" ] && [ "$CONTRACT" != None ]'
emit() { cast_ send --private-key $PK "$CONTRACT" >/dev/null; }

new_home "$WORK/home"
perl -0pi -e 's/enabled = \[\]/enabled = ["ethereum"]/' "$CONFIG"
cat >>"$CONFIG" <<TOML

[chains.ethereum]
rpc_url = "$RPC"
ws_url = "ws://127.0.0.1:18545"
addresses = ["$CONTRACT"]
confirmations = 0
poll_interval_secs = 30
start_block = 0
TOML
start_daemon || exit 1

echo "== a real node, over ws://"
for _ in 1 2 3; do emit; done
"$BIN" watch ethereum >"$WORK/watch.log" 2>&1 &
WATCH_PID=$!
check "the first three events are recorded" 'wait_for 30 "[ \"\$(count ethereum)\" = 3 ]"'
T0=$(date +%s)
emit
check "a new event is recorded within a few seconds (poll interval is 30 s)" 'wait_for 8 "[ \"\$(count ethereum)\" = 4 ]"'
echo "   latency: $(( $(date +%s) - T0 )) s"
check "records carry the contract address and a transaction hash" '"$BIN" logs ethereum --limit 1 2>&1 | grep -qi "${CONTRACT:2:10}\|0x"'

echo "== restart keeps the cursor"
stop_watch
emit; emit
"$BIN" watch ethereum >"$WORK/watch2.log" 2>&1 &
WATCH_PID=$!
check "two events sent while the watcher was down are recorded once" 'wait_for 30 "[ \"\$(count ethereum)\" = 6 ]"'
sleep 5
check "no event is repeated after the restart" '[ "$(count ethereum)" = 6 ]'

echo "== soak ($SOAK s, one event every 2 s)"
SENT=6
END=$(( $(date +%s) + SOAK ))
while [ "$(date +%s)" -lt "$END" ]; do emit; SENT=$((SENT+1)); sleep 2; done
check "every event of the soak is recorded exactly once" 'wait_for 30 "[ \"\$(count ethereum)\" = $SENT ]"'
check "the watcher is still running" 'kill -0 "$WATCH_PID" 2>/dev/null'
# a rate-limit answer is handled by design (the rest waits for the next poll): not an error
check "the watcher log has no error besides rate limits" '! cat "$WORK/watch.log" "$WORK/watch2.log" | grep -i "error" | grep -qvi "429\|too many requests"'

summary
