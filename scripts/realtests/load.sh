#!/usr/bin/env bash
# Load tests on a real daemon: UDP ingest at rising rates with the loss counted, and kill -9 under
# load. Each rate runs for LOAD_SECONDS (default 10); the check is what the daemon stored against
# what was sent. Loss under overload is expected and is reported, not failed, except where noted.
#
# Usage: scripts/realtests/load.sh [path to loghaven]
set -uo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
BIN=${1:-$ROOT/target/debug/loghaven}
BIN=$(cd "$(dirname "$BIN")" && pwd)/$(basename "$BIN")
SECS=${LOAD_SECONDS:-10}
AREA="Load"
. "$ROOT/scripts/realtests/lib.sh"

WORK=$(mktemp -d /tmp/lhload.XXXXXX)
cleanup() { stop_daemon; [ "$FAILED" = 0 ] && rm -rf "$WORK" || echo "kept $WORK"; }
trap cleanup EXIT

new_home "$WORK/home"
start_daemon || exit 1
LOAD() { python3 -I "$ROOT/scripts/realtests/udp_load.py" "$("$BIN" auth token "$1" --ttl-secs 3600 | tail -1)" "$@"; }

echo "== rising rates (records/s, one record per datagram), $SECS s each"
for RATE in 500 2000 5000 10000 20000; do
  OUT=$(LOAD "r$RATE" "$RATE" "$SECS" 1)
  echo "   $RATE/s: $OUT"
  case $RATE in
    500|2000) check "no loss at $RATE records/s" 'echo "$OUT" | grep -q "lost=0 duplicated=0"' ;;
    *)        check "no duplicates at $RATE records/s" 'echo "$OUT" | grep -q "duplicated=0"' ;;
  esac
done
echo "== kill -9 under load (5000 records/s, killed at 4 s of 8; flush interval 1 s)"
OUT=$(LH_BIN=$BIN LH_KILL_AT=4 LOAD kill 5000 8 1)
echo "   $OUT"
check "after kill -9 the daemon restarts and the data answers" '"$BIN" logs kill --limit 5 2>&1 | grep -q "records"'
check "nothing is duplicated after kill -9" 'echo "$OUT" | grep -q "duplicated=0"'
"$BIN" send kill "after the crash" >/dev/null
check "a partial chunk left by the kill is removed once the app writes again" 'wait_for 10 "[ -z \"\$(find \"$DATA\" -name \"*.parquet.tmp\")\" ]"'

check "the daemon is still answering after the load" '"$BIN" status >/dev/null 2>&1'

summary
