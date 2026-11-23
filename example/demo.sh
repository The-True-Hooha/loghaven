#!/usr/bin/env bash
# Runs the demo shop against a LogHaven daemon that lives entirely in ./.demo-home: nothing is
# installed or deployed, nothing outside this folder is touched, and nothing leaves your machine.
#
# Usage: example/demo.sh [path to loghaven]     (default: loghaven on PATH, else target/debug/loghaven)
# Needs python3 and free local ports 9100 (UDP) and 9200 (TCP).
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
BIN=${1:-$(command -v loghaven || echo "$HERE/../target/debug/loghaven")}
[ -x "$BIN" ] || { echo "no loghaven binary; build one with 'cargo build' or pass its path"; exit 1; }

export HOME=$HERE/.demo-home
unset XDG_CONFIG_HOME XDG_DATA_HOME
rm -rf "$HOME"; mkdir -p "$HOME"
trap '"$BIN" stop >/dev/null 2>&1 || true' EXIT

CONFIG=$("$BIN" init 2>&1 | sed -n 's/.*Edit config: //p')
"$BIN" auth keygen >/dev/null
# a unix socket path may be at most ~100 bytes; a deep folder would exceed that
"$BIN" config daemon.socket_path "${TMPDIR:-/tmp}/loghaven-demo.sock" >/dev/null
sed -i.bak 's/flush_interval_secs = 30/flush_interval_secs = 1/' "$CONFIG"   # records show up in a second
"$BIN" run >/dev/null 2>&1
for _ in $(seq 50); do curl -s -o /dev/null http://127.0.0.1:9200/dashboard && break; sleep 0.2; done

echo "== the shop logs 20 orders"
python3 "$HERE/shop.py" "$("$BIN" auth token shop --ttl-secs 600 | tail -1)" 20
sleep 2

echo; echo "== errors and warnings"
"$BIN" logs shop --level error --limit 5
WARN=$("$BIN" logs shop --level warn --limit 5 | tee /dev/stderr)

echo; echo "== one payment, by its transaction hash"
TX=$(printf '%s\n' "$WARN" | grep -o '0x[0-9a-f]\{64\}' | head -1)
"$BIN" trace "$TX" --app shop

echo; echo "== open the dashboard (Ctrl+C to finish; the daemon stops with it)"
echo "   or try:  HOME=$HOME $(cd "$(dirname "$BIN")" && pwd)/$(basename "$BIN") tui --app shop"
"$BIN" dashboard --app shop --no-open
read -r -p "Press Enter to stop the daemon... " _ || true
