#!/usr/bin/env bash
# Real S3-API server in a container (MinIO or RustFS) through the shared store suite in lib.sh.
#
# Usage: scripts/realtests/s3.sh <minio|rustfs> [path to loghaven] [path to a big .parquet chunk]
# Needs podman (a running machine), curl >= 7.75 and python3.
set -uo pipefail

KIND=${1:?minio or rustfs}
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
BIN=${2:-$ROOT/target/debug/loghaven}
BIN=$(cd "$(dirname "$BIN")" && pwd)/$(basename "$BIN")
BIG=${3:-}
AREA="S3 API ($KIND)"
. "$ROOT/scripts/realtests/lib.sh"

KEY=lhkey
SECRET=lhsecret123
BUCKET=lh-real
case "$KIND" in
  minio)
    PORT=19000
    container lh-minio -p $PORT:9000 -e MINIO_ROOT_USER=$KEY -e MINIO_ROOT_PASSWORD=$SECRET \
      docker.io/bitnamilegacy/minio:latest ;;
  rustfs)
    PORT=19001
    container lh-rustfs -p $PORT:9000 -e RUSTFS_ACCESS_KEY=$KEY -e RUSTFS_SECRET_KEY=$SECRET \
      docker.io/rustfs/rustfs:latest ;;
  *) echo "unknown $KIND"; exit 2 ;;
esac
WORK=$(mktemp -d /tmp/lhs3.XXXXXX)
cleanup() { stop_daemon; drop_containers; [ "$FAILED" = 0 ] && rm -rf "$WORK" || echo "kept $WORK"; }
trap cleanup EXIT

URL=http://127.0.0.1:$PORT
s3() { curl -s --aws-sigv4 "aws:amz:us-east-1:s3" --user "$KEY:$SECRET" "$@"; }
list_keys() { s3 "$URL/$BUCKET?list-type=2&prefix=$1" | grep -o '<Key>[^<]*</Key>' | sed 's/<[^>]*>//g'; }
get_object() { s3 -o "$2" "$URL/$BUCKET/$1"; }
big_object_checks() { # key, size
  SIZE_=$2
  s3 -I "$URL/$BUCKET/$1" >"$WORK/head"
  check "the object has the chunk's size" 'grep -qi "content-length: $SIZE_" "$WORK/head"'
  check "the object was uploaded in parts (multipart ETag)" 'grep -i "^etag" "$WORK/head" | grep -q -- "-[0-9]*\""'
}

wait_for 60 's3 -o /dev/null "$URL/"' || { echo "$KIND did not start"; podman_ logs "lh-$KIND" | tail; exit 1; }
check "bucket is created" '[ "$(s3 -o /dev/null -w "%{http_code}" -X PUT "$URL/$BUCKET")" = 200 ]'

TARGET_TOML="[[storage.targets]]
name = \"real\"
type = \"s3\"
bucket = \"$BUCKET\"
region = \"us-east-1\"
endpoint = \"$URL\"
path_style = true
prefix = \"run\"
access_key = \"$KEY\"
secret_key = \"$SECRET\""

store_suite "$WORK" "$BIG"
summary
