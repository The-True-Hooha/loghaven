#!/usr/bin/env bash
# The terminal UI in a pseudo-terminal against a real daemon: resize, daemon killed, daemon back.
#
# Usage: scripts/realtests/tui.sh [path to loghaven]
set -uo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
BIN=${1:-$ROOT/target/debug/loghaven}
BIN=$(cd "$(dirname "$BIN")" && pwd)/$(basename "$BIN")
AREA="TUI (pty)"
. "$ROOT/scripts/realtests/lib.sh"

WORK=$(mktemp -d /tmp/lhtui.XXXXXX)
cleanup() { stop_daemon; [ "$FAILED" = 0 ] && rm -rf "$WORK" || echo "kept $WORK"; }
trap cleanup EXIT

new_home "$WORK/home"
start_daemon || exit 1
python3 -I "$ROOT/scripts/realtests/tui_pty.py" "$BIN" tuiapp | tee "$WORK/out"
while read -r verdict rest; do
  case $verdict in PASS) pass "$rest" ;; FAIL) fail "$rest" ;; esac
done <"$WORK/out" >/dev/null
summary
