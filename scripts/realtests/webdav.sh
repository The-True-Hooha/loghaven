#!/usr/bin/env bash
# Real WebDAV server (Apache mod_dav in a container) through the shared store suite in lib.sh.
#
# Usage: scripts/realtests/webdav.sh [path to loghaven] [path to a big .parquet chunk]
set -uo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
BIN=${1:-$ROOT/target/debug/loghaven}
BIN=$(cd "$(dirname "$BIN")" && pwd)/$(basename "$BIN")
BIG=${2:-}
AREA="WebDAV (Apache mod_dav)"
. "$ROOT/scripts/realtests/lib.sh"

PORT=19080
container lh-dav -p $PORT:80 -e AUTH_TYPE=Basic -e USERNAME=lhuser -e PASSWORD=lhpass123 \
  docker.io/bytemark/webdav:latest
WORK=$(mktemp -d /tmp/lhdav.XXXXXX)
cleanup() { stop_daemon; drop_containers; [ "$FAILED" = 0 ] && rm -rf "$WORK" || echo "kept $WORK"; }
trap cleanup EXIT

URL=http://127.0.0.1:$PORT
dav() { curl -s -u lhuser:lhpass123 "$@"; }
wait_for 60 '[ "$(dav -o /dev/null -w "%{http_code}" -X PROPFIND -H "Depth: 0" "$URL/")" = 207 ]' \
  || { echo "webdav did not start"; podman_ logs lh-dav | tail; exit 1; }
list_keys() { podman_ exec lh-dav sh -c "cd /var/lib/dav/data/data && find $1 -type f 2>/dev/null"; }
get_object() { dav -o "$2" "$URL/data/$1"; }

TARGET_TOML="[[storage.targets]]
name = \"real\"
type = \"webdav\"
url = \"$URL/data\"
username = \"lhuser\"
password = \"lhpass123\"
prefix = \"run\""
check "the data collection exists" '[ "$(dav -o /dev/null -w "%{http_code}" -X MKCOL "$URL/data/")" != 000 ]'

store_suite "$WORK" "$BIG"
summary
