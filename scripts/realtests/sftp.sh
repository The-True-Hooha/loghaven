#!/usr/bin/env bash
# Real OpenSSH server in a container through the shared store suite in lib.sh, plus key login,
# a wrong password and a wrong host key.
#
# Usage: scripts/realtests/sftp.sh [path to loghaven] [path to a big .parquet chunk]
set -uo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
BIN=${1:-$ROOT/target/debug/loghaven}
BIN=$(cd "$(dirname "$BIN")" && pwd)/$(basename "$BIN")
BIG=${2:-}
AREA="SFTP (OpenSSH)"
. "$ROOT/scripts/realtests/lib.sh"

PORT=19022
# podman on macOS shares /private/tmp with its machine, not /tmp
MOUNT_BASE=/tmp; [ -d /private/tmp ] && MOUNT_BASE=/private/tmp
WORK=$(mktemp -d "$MOUNT_BASE/lhsftp.XXXXXX")
ssh-keygen -q -t ed25519 -N "" -f "$WORK/id" >/dev/null
mkdir -p "$WORK/pub"; cp "$WORK/id.pub" "$WORK/pub/id.pub"
container lh-sftp -p $PORT:22 -v "$WORK/pub:/home/lh/.ssh/keys:ro" \
  docker.io/atmoz/sftp:latest lh:lhpass123:::upload
cleanup() { stop_daemon; drop_containers; [ "$FAILED" = 0 ] && rm -rf "$WORK" || echo "kept $WORK"; }
trap cleanup EXIT

wait_for 60 'podman_ exec lh-sftp test -f /etc/ssh/ssh_host_ed25519_key.pub' || { echo "sshd did not start"; exit 1; }
wait_for 30 'nc -z 127.0.0.1 $PORT'
FP=$(podman_ exec lh-sftp ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub | awk '{print $2}')
list_keys() { podman_ exec lh-sftp sh -c "cd /home/lh/upload && find $1 -type f 2>/dev/null"; }
get_object() { podman_ cp "lh-sftp:/home/lh/upload/$1" "$2" >/dev/null 2>&1; }

block() { # extra lines
  printf '[[storage.targets]]\nname = "real"\ntype = "sftp"\nhost = "127.0.0.1"\nport = %s\nusername = "lh"\nhost_key = "%s"\npath = "upload"\nprefix = "run"\n%s' "$PORT" "${HOSTKEY:-$FP}" "$1"
}
TARGET_TOML=$(block 'password = "lhpass123"')
store_suite "$WORK" "$BIG"

echo "== logins and host key"
swap_target() { perl -0pi -e 's/\n\[\[storage\.targets\]\].*//s' "$CONFIG"; printf '\n%s\n' "$1" >>"$CONFIG"; }
rm -rf "$DATA/web" "$DATA/rot"
stop_daemon
swap_target "$(block "key_file = \"$WORK/id\"")"
start_daemon
check "key login works (restore)" '"$BIN" storage restore web >/dev/null 2>&1'
stop_daemon
swap_target "$(block 'password = "wrong-password"')"
start_daemon
check "a wrong password is refused" '! "$BIN" storage restore rot >"$WORK/wrongpw" 2>&1'
check "the error does not print the password" '! grep -q "wrong-password" "$WORK/wrongpw"'
stop_daemon
HOSTKEY="SHA256:$(printf 'A%.0s' $(seq 43))"
swap_target "$(block 'password = "lhpass123"')"
start_daemon
check "a wrong host key is refused" '! "$BIN" storage restore rot >"$WORK/wronghk" 2>&1 && grep -qi "host key" "$WORK/wronghk"'
summary
