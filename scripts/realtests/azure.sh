#!/usr/bin/env bash
# Real Azure Blob API (the Azurite emulator in a container) through the shared store suite in
# lib.sh, with the account key, and again with an account SAS token.
#
# Usage: scripts/realtests/azure.sh [path to loghaven] [path to a big .parquet chunk]
set -uo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
BIN=${1:-$ROOT/target/debug/loghaven}
BIN=$(cd "$(dirname "$BIN")" && pwd)/$(basename "$BIN")
BIG=${2:-}
AREA="Azure (Azurite)"
. "$ROOT/scripts/realtests/lib.sh"

ACCOUNT=devstoreaccount1
AKEY='Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=='
PORT=19100
ENDPOINT=http://127.0.0.1:$PORT/$ACCOUNT
container lh-azurite -p $PORT:10000 mcr.microsoft.com/azure-storage/azurite:latest \
  azurite-blob --blobHost 0.0.0.0 --skipApiVersionCheck
WORK=$(mktemp -d /tmp/lhaz.XXXXXX)
cleanup() { stop_daemon; drop_containers; [ "$FAILED" = 0 ] && rm -rf "$WORK" || echo "kept $WORK"; }
trap cleanup EXIT

az() { python3 "$ROOT/scripts/realtests/azure_rest.py" "$ENDPOINT" "$ACCOUNT" "$AKEY" "$@"; }
wait_for 60 'curl -s -o /dev/null "http://127.0.0.1:$PORT/"' || { echo "azurite did not start"; podman_ logs lh-azurite | tail; exit 1; }
CONTAINER=lhlogs
check "container is created" 'az create $CONTAINER >/dev/null'
list_keys() { az list $CONTAINER "$1"; }
get_object() { az get $CONTAINER "$1" "$2"; }

echo "== account key"
TARGET_TOML="[[storage.targets]]
name = \"real\"
type = \"azure\"
account = \"$ACCOUNT\"
container = \"$CONTAINER\"
endpoint = \"$ENDPOINT\"
access_key = \"$AKEY\"
prefix = \"run\""
store_suite "$WORK/key" "$BIG"

echo "== SAS token"
SAS=$(az sas $CONTAINER)
CONTAINER=lhsas
az create $CONTAINER >/dev/null
TARGET_TOML="[[storage.targets]]
name = \"real\"
type = \"azure\"
account = \"$ACCOUNT\"
container = \"$CONTAINER\"
endpoint = \"$ENDPOINT\"
sas_token = \"$SAS\"
prefix = \"run\""
stop_daemon
mkdir -p "$WORK/sas"
store_suite "$WORK/sas" ""
summary
