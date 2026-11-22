#!/usr/bin/env bash
# The Solana adapter against a real node: solana-test-validator (Agave release binaries) in a
# container built from scripts/realtests/solana.Containerfile; the validator and the solana CLI run
# only inside it. Each transfer to the watched address is a real transaction. Covers recording, a
# restart that neither repeats nor loses any, and a SOAK_SECONDS soak.
#
# Usage: scripts/realtests/solana.sh [path to loghaven]   (build the image once, see the Containerfile)
set -uo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
BIN=${1:-$ROOT/target/debug/loghaven}
BIN=$(cd "$(dirname "$BIN")" && pwd)/$(basename "$BIN")
SOAK=${SOAK_SECONDS:-60}
AREA="Solana (test validator)"
. "$ROOT/scripts/realtests/lib.sh"

# The validator advertises one address for its transaction port, so it must be reached on that
# address: the host's own network on Linux, where podman shares it. (On macOS the VM publishes a
# port instead, and a validator under x86 emulation does not take transactions at all.)
VALIDATOR='solana-keygen new --no-bip39-passphrase --silent -o /root/.config/solana/id.json && exec solana-test-validator --reset --quiet --ledger /ledger --rpc-port 8899'
if [ "$(uname)" = Linux ]; then
  RPC=http://127.0.0.1:8899
  container lh-solana --platform linux/amd64 --network host --entrypoint sh lh-solana-test:2.3.13 -c "$VALIDATOR"
else
  RPC=http://127.0.0.1:18899
  container lh-solana --platform linux/amd64 -p 18899:8899 --entrypoint sh lh-solana-test:2.3.13 -c "$VALIDATOR --bind-address \$(hostname -i | awk '{print \$1}')"
fi
WORK=$(mktemp -d /tmp/lhsolana.XXXXXX)
WATCH_PID=
stop_watch() { [ -n "$WATCH_PID" ] && kill "$WATCH_PID" 2>/dev/null; WATCH_PID=; }
cleanup() { stop_watch; stop_daemon; drop_containers; [ "$FAILED" = 0 ] && rm -rf "$WORK" || echo "kept $WORK"; }
trap cleanup EXIT

sol() { podman_ exec lh-solana solana -u http://127.0.0.1:8899 "$@" 2>/dev/null; }
keys() { podman_ exec lh-solana solana-keygen "$@" 2>/dev/null; }
wait_for 180 'sol cluster-version >/dev/null' || { echo "the validator did not start"; podman_ logs lh-solana 2>&1 | tail; exit 1; }
keys new --no-bip39-passphrase --silent -o /watched.json >/dev/null
WATCHED=$(keys pubkey /watched.json)
# An airdrop is a real transaction that names the watched address. Sending from the solana CLI
# does not work where the validator runs under x86 emulation (its TPU never receives); on a
# native x86 Linux host both work.
send() { curl -s -m 30 -X POST -H 'content-type: application/json' \
  --data "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"requestAirdrop\",\"params\":[\"$WATCHED\",1000000]}" $RPC | grep -q '"result"'; }
# The faucet drops some airdrops, so the checks below are about the watcher matching what
# landed, not about how many were sent.
# what the chain holds for the address at the level the watcher reads (finalized). The first
# airdrop happens before the watcher starts and is not recorded: BASE is how many the chain
# held then.
chain_count() { curl -s -m 30 -X POST -H 'content-type: application/json' \
  --data "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getSignaturesForAddress\",\"params\":[\"$WATCHED\",{\"limit\":1000,\"commitment\":\"finalized\"}]}" $RPC \
  | python3 -c 'import json,sys; print(len(json.load(sys.stdin)["result"]))'; }
matches_chain() { # at least $1 transactions, and the watcher has exactly the chain's
  local want; want=$(( $(chain_count) - BASE )); [ "$want" -ge "$1" ] && [ "$(count solana)" = "$want" ]
}
check "an airdrop to the watched address lands" 'send && wait_for 60 "[ \"\$(sol balance \"$WATCHED\" | cut -d. -f1 | tr -dc 0-9)\" != \"\" ] && [ \"\$(sol balance \"$WATCHED\")\" != \"0 SOL\" ]"'

new_home "$WORK/home"
perl -0pi -e 's/enabled = \[\]/enabled = ["solana"]/' "$CONFIG"
cat >>"$CONFIG" <<TOML

[chains.solana]
rpc_url = "$RPC"
addresses = ["$WATCHED"]
poll_interval_secs = 2
TOML
start_daemon || exit 1
check "the first airdrop is finalized, so the first run starts after it" 'wait_for 60 "[ \"\$(chain_count)\" -ge 1 ]"'
BASE=$(chain_count)

echo "== a real validator"
"$BIN" watch solana >"$WORK/watch.log" 2>&1 &
WATCH_PID=$!
check "the watcher starts" 'wait_for 30 "grep -q Watching $WORK/watch.log"'
sleep 6
for _ in 1 2 3; do send; done
check "the transfers that landed are recorded (at least one landed)" 'wait_for 90 "matches_chain 1" || { echo "   chain=$(( $(chain_count) - BASE )) watcher=$(count solana)"; false; }'
T0=$(date +%s)
send
check "a new transfer is recorded within a few polls" 'wait_for 60 "matches_chain 2" || { echo "   chain=$(( $(chain_count) - BASE )) watcher=$(count solana)"; false; }'
echo "   latency: $(( $(date +%s) - T0 )) s"
SIG=$("$BIN" logs solana --limit 1 2>&1 | grep -o '[1-9A-HJ-NP-Za-km-z]\{80,90\}' | head -1)
check "a record can be traced by signature" '[ -n "$SIG" ] && "$BIN" trace "$SIG" --app solana 2>&1 | grep -q "slot"'

echo "== restart keeps the cursor"
stop_watch
send; send
"$BIN" watch solana >"$WORK/watch2.log" 2>&1 &
WATCH_PID=$!
check "transfers made while the watcher was down are recorded once" 'wait_for 90 "matches_chain 3" || { echo "   chain=$(( $(chain_count) - BASE )) watcher=$(count solana)"; false; }'
sleep 6
check "nothing is repeated after the restart" 'matches_chain 3 || { echo "   chain=$(( $(chain_count) - BASE )) watcher=$(count solana)"; false; }'

echo "== soak ($SOAK s, one transfer every 2 s)"
SENT=6
END=$(( $(date +%s) + SOAK ))
while [ "$(date +%s)" -lt "$END" ]; do send && SENT=$((SENT+1)); sleep 2; done
check "some soak transfers landed and each is recorded exactly once" 'wait_for 90 "matches_chain $((SENT / 3))" || { echo "   chain=$(( $(chain_count) - BASE )) watcher=$(count solana)"; false; }'
check "the watcher is still running" 'kill -0 "$WATCH_PID" 2>/dev/null'
# a rate-limit answer is handled by design (the rest waits for the next poll): not an error
check "the watcher log has no error besides rate limits" '! cat "$WORK/watch.log" "$WORK/watch2.log" | grep -i "error" | grep -qvi "429\|too many requests"'

[ "$FAILED" = 0 ] || { echo "-- recorded: $(count solana) of $SENT sent"; "$BIN" logs solana --limit 20 2>&1 | cut -c1-200; echo "-- watcher"; head -20 "$WORK/watch.log" | cut -c1-200; echo "-- validator log"; podman_ exec lh-solana sh -c 'tail -n 30 /ledger/validator.log' 2>&1 | cut -c1-200; }
summary
