#!/usr/bin/env bash
# The store goes away while the daemon uploads: a MinIO container is stopped, then started again.
# Checks that the daemon stays up, keeps the chunk locally while the store is down, uploads it once
# the store is back (once, not twice), and that a big chunk cut off in the middle of its upload is
# uploaded whole afterwards.
#
# Usage: scripts/realtests/storecut.sh [path to loghaven] [path to a big .parquet chunk]
set -uo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
BIN=${1:-$ROOT/target/debug/loghaven}
BIN=$(cd "$(dirname "$BIN")" && pwd)/$(basename "$BIN")
BIG=${2:-}
AREA="Store cut (MinIO)"
. "$ROOT/scripts/realtests/lib.sh"

KEY=lhkey; SECRET=lhsecret123; BUCKET=lh-real; PORT=19000
container lh-minio -p $PORT:9000 -e MINIO_ROOT_USER=$KEY -e MINIO_ROOT_PASSWORD=$SECRET \
  docker.io/bitnamilegacy/minio:latest
WORK=$(mktemp -d /tmp/lhcut.XXXXXX)
cleanup() { stop_daemon; drop_containers; [ "$FAILED" = 0 ] && rm -rf "$WORK" || echo "kept $WORK"; }
trap cleanup EXIT

URL=http://127.0.0.1:$PORT
s3() { curl -s -m 20 --aws-sigv4 "aws:amz:us-east-1:s3" --user "$KEY:$SECRET" "$@"; }
list_keys() { s3 "$URL/$BUCKET?list-type=2&prefix=$1" | grep -o '<Key>[^<]*</Key>' | sed 's/<[^>]*>//g'; }
stored() { list_keys "$1" | grep -c '\.parquet$'; }
local_chunks() { find "$DATA/$1" -name '*.parquet' 2>/dev/null; }
wait_for 60 's3 -o /dev/null "$URL/"' || { echo "minio did not start"; exit 1; }
s3 -o /dev/null -X PUT "$URL/$BUCKET"

new_home "$WORK/home"
cat >>"$CONFIG" <<TOML

[[storage.targets]]
name = "real"
type = "s3"
bucket = "$BUCKET"
region = "us-east-1"
endpoint = "$URL"
path_style = true
prefix = "run"
access_key = "$KEY"
secret_key = "$SECRET"
TOML
set_key delete_after_upload true
set_key retention_days 0
start_daemon || exit 1

echo "== store down between uploads"
podman_ stop -t 0 lh-minio >/dev/null
for i in $(seq 20); do "$BIN" send web "while the store is down $i" >/dev/null; done
sleep 8
check "the chunk stays on disk while the store is down" '[ -n "$(local_chunks web)" ]'
check "the daemon is still running" '"$BIN" status 2>&1 | grep -qi running'
check "the records can still be queried" '[ "$(count web)" = 20 ]'
podman_ start lh-minio >/dev/null
wait_for 60 's3 -o /dev/null "$URL/"'
check "the chunk is uploaded once the store is back" 'wait_for 90 "[ \"\$(stored run/web/)\" -ge 1 ]"'
check "the chunk is uploaded exactly once" '[ "$(stored run/web/)" = 1 ]'
check "the local copy is deleted after the upload" 'wait_for 30 "[ -z \"\$(local_chunks web)\" ]"'

if [ -n "$BIG" ] && [ -f "$BIG" ]; then
  echo "== store cut in the middle of a big upload"
  stop_daemon
  DAY=2024-01-01
  mkdir -p "$DATA/big/$DAY"
  cp "$BIG" "$DATA/big/$DAY/1-1.parquet"
  SUM=$(shasum -a 256 "$BIG" | cut -d' ' -f1)
  start_daemon || exit 1
  sleep "${CUT_AFTER:-0.3}"   # the upload of the chunk is under way
  podman_ stop -t 0 lh-minio >/dev/null
  sleep 5
  if [ -n "$(local_chunks big)" ]; then
    pass "the cut came before the upload finished, and the chunk is still on disk"
  else
    echo "   note: the upload had finished before the cut (CUT_AFTER=${CUT_AFTER:-0.3}); no cut mid-upload was tested"
  fi
  check "the daemon is still running" '"$BIN" status 2>&1 | grep -qi running'
  podman_ start lh-minio >/dev/null
  wait_for 60 's3 -o /dev/null "$URL/"'
  check "the big chunk is uploaded whole afterwards" 'wait_for 240 "list_keys run/big/ | grep -q 1-1.parquet$"'
  s3 -o "$WORK/big.back" "$URL/$BUCKET/run/big/$DAY/1-1.parquet"
  check "the stored bytes match" '[ "$(shasum -a 256 "$WORK/big.back" | cut -d" " -f1)" = "$SUM" ]'
fi
summary
