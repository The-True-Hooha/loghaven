#!/usr/bin/env bash
# A full disk under a real daemon. The data directory sits on a small volume (a RAM disk on macOS,
# a tmpfs through sudo on Linux) that is filled until writes fail. Checks that the daemon stays up,
# keeps answering queries, loses no record that was already stored, and writes again once space is
# freed.
#
# Usage: scripts/realtests/diskfull.sh [path to loghaven]
set -uo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
BIN=${1:-$ROOT/target/debug/loghaven}
BIN=$(cd "$(dirname "$BIN")" && pwd)/$(basename "$BIN")
AREA="Disk full"
. "$ROOT/scripts/realtests/lib.sh"

WORK=$(mktemp -d /tmp/lhfull.XXXXXX)
VOL=$WORK/vol
mkdir -p "$VOL"
if [ "$(uname)" = Darwin ]; then
  hdiutil create -size 16m -fs HFS+ -volname lhfull -o "$WORK/vol.dmg" >/dev/null &&
    hdiutil attach "$WORK/vol.dmg" -mountpoint "$VOL" -nobrowse >/dev/null
else
  sudo mount -t tmpfs -o size=16m tmpfs "$VOL"
fi || { echo "cannot make a small volume"; exit 1; }
cleanup() {
  stop_daemon
  if [ "$(uname)" = Darwin ]; then for _ in 1 2 3 4 5; do hdiutil detach "$VOL" -force >/dev/null 2>&1 && break; sleep 2; done; else sudo umount "$VOL"; fi
  { [ "$FAILED" = 0 ] && rm -rf "$WORK" 2>/dev/null; } || echo "kept $WORK"
}
trap cleanup EXIT

new_home "$WORK/home"
mkdir -p "$VOL/data"
sed -i.bak "s|^path = .*|path = \"$VOL/data\"|" "$CONFIG"
DATA=$VOL/data
start_daemon || exit 1

for i in $(seq 50); do "$BIN" send before "stored before the disk fills $i" >/dev/null; done
check "records are stored while there is room" 'wait_for 20 "[ \"\$(count before)\" = 50 ]"'

echo "== fill the volume"
dd if=/dev/zero of="$VOL/ballast" bs=1k 2>/dev/null
check "the volume is full" '! dd if=/dev/zero of="$VOL/probe" bs=1k count=64 2>/dev/null'
rm -f "$VOL/probe"
for i in $(seq 200); do "$BIN" send full "written to a full disk $i" >/dev/null; done
sleep 5
check "the daemon is still running" '"$BIN" status 2>&1 | grep -qi "running"'
check "records stored before the fill can still be queried" '[ "$(count before)" = 50 ]'
check "the daemon says writes failed" 'grep -qi "no space\|write error\|failed" "$HOME/daemon.log"'
check "no partial chunk is left behind" '[ -z "$(find "$DATA" -name "*.parquet.tmp")" ]'

echo "== free the space"
rm -f "$VOL/ballast"
for i in $(seq 20); do "$BIN" send after "written after space came back $i" >/dev/null; done
check "writes resume" 'wait_for 30 "[ \"\$(count after)\" = 20 ]"'
check "earlier records are still there" '[ "$(count before)" = 50 ]'

echo "-- records of the full period that were kept: $(count full) of 200"
summary
