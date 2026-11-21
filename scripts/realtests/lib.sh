#!/usr/bin/env bash
# Shared helpers for the real-service tests: an isolated daemon, a podman wrapper and PASS/FAIL
# bookkeeping. Source this file; it expects ROOT (the repo) and BIN (the loghaven binary).
# Results are appended to $RESULTS (default /tmp/lh-real-results.txt).

REAL_HOME=${REAL_HOME:-$HOME}
RESULTS=${RESULTS:-/tmp/lh-real-results.txt}
FAILED=0
CONTAINERS=()

podman_() { HOME=$REAL_HOME podman "$@"; }
pass() { echo "PASS $1"; echo "PASS $AREA: $1" >>"$RESULTS"; }
fail() { echo "FAIL $1"; echo "FAIL $AREA: $1" >>"$RESULTS"; FAILED=1; }
check() { if eval "$2"; then pass "$1"; else fail "$1"; fi; }

container() { # name, podman run args...
  local name=$1; shift
  podman_ rm -f "$name" >/dev/null 2>&1 || true
  podman_ run -d --name "$name" "$@" >/dev/null
  CONTAINERS+=("$name")
}

drop_containers() {
  for c in "${CONTAINERS[@]:-}"; do [ -n "$c" ] && podman_ rm -f "$c" >/dev/null 2>&1 || true; done
}

wait_http() { # url, seconds
  for _ in $(seq $(($2 * 2))); do curl -s -o /dev/null "$1" && return 0; sleep 0.5; done
  return 1
}

# An isolated HOME with a fresh config, keys and a short flush interval.
new_home() { # dir
  export HOME=$1
  mkdir -p "$HOME"
  unset XDG_CONFIG_HOME XDG_DATA_HOME
  local init; init=$("$BIN" init 2>&1)
  CONFIG=$(printf '%s\n' "$init" | sed -n 's/.*Edit config: //p')
  [ -f "$CONFIG" ] || { echo "no config at '$CONFIG'; init said:"; printf '%s\n' "$init"; env | grep -E "^(HOME|XDG)" ; exit 1; }
  "$BIN" auth keygen >/dev/null
  sed -i.bak 's/flush_interval_secs = 30/flush_interval_secs = 1/' "$CONFIG"
  DATA=$(sed -n 's/^path = "\(.*\)"/\1/p' "$CONFIG" | head -1)
  [ -n "$DATA" ] || DATA=$("$BIN" config storage.local.path | tail -1)
}

start_daemon() {
  (nohup "$BIN" run >>"$HOME/daemon.log" 2>&1 &)
  for _ in $(seq 60); do curl -s -o /dev/null http://127.0.0.1:9200/dashboard && return 0; sleep 0.2; done
  echo "daemon did not start"; cat "$HOME/daemon.log"; return 1
}
stop_daemon() { "$BIN" stop >/dev/null 2>&1; sleep 1; }

count() { "$BIN" logs "$1" --limit 10000 "${@:2}" 2>&1 | sed -n 's/^✓ \([0-9]*\) records.*/\1/p'; }
wait_for() { # seconds, command
  for _ in $(seq $(($1 * 2))); do eval "$2" && return 0; sleep 0.5; done
  return 1
}

summary() {
  echo "== $AREA: $([ "$FAILED" = 0 ] && echo all passed || echo FAILED)"
  [ "$FAILED" = 0 ]
}

set_key() { sed -i.bak "s/^$1 = .*/$1 = $2/" "$CONFIG"; }

# The suite every store type runs. The caller sets TARGET_TOML (one [[storage.targets]] block named
# "real" with prefix "run") and defines list_keys <prefix> and get_object <key> <file>; a
# function big_object_checks <key> <size> may add checks for the big chunk.
store_suite() { # work dir, optional big chunk
  local WORK=$1 BIG=${2:-}
  new_home "$WORK/home"
  printf '\n%s\n' "$TARGET_TOML" >>"$CONFIG"
  set_key delete_after_upload true
  set_key encrypt_uploads true
  set_key retention_days 0
  start_daemon || return 1
  CACHE="$DATA-remote-cache"

  echo "== encrypted upload with delete_after_upload"
  for i in $(seq 20); do "$BIN" send web "record $i" >/dev/null; done
  check "the chunk is uploaded encrypted" 'wait_for 40 "list_keys run/web/ | grep -q \\.parquet\\.enc$"'
  check "the local chunk is deleted after upload" 'wait_for 40 "[ -z \"\$(find $DATA/web -name \"*.parquet\" 2>/dev/null)\" ]"'
  local OBJ; OBJ=$(list_keys run/web/ | grep '\.parquet\.enc$' | head -1)
  get_object "$OBJ" "$WORK/obj"
  check "the stored object starts with the encryption header" '[ "$(head -c 6 "$WORK/obj")" = "LHENC2" ]'
  check "no record text is in the stored object" '! grep -q "record 1" "$WORK/obj"'
  check "uploaded data is not queryable without read-through" '[ "$(count web)" = 0 ]'

  echo "== restore"
  check "storage restore brings the chunk back" '"$BIN" storage restore web >"$WORK/restore.out" 2>&1'
  check "restored records are queryable" 'wait_for 10 "[ \"\$(count web)\" = 20 ]"'
  check "a second restore changes nothing" '"$BIN" storage restore web 2>&1 | grep -qi "already\|present\|0 restored\|nothing"'

  echo "== read-through queries"
  stop_daemon
  rm -rf "$DATA/web"
  perl -0pi -e 's/\[query\]\n/[query]\nremote_target = "real"\n/' "$CONFIG"
  start_daemon || return 1
  local FROM=$(( ($(date +%s) - 86400) * 1000 ))
  check "a query with --from reads through to the target" 'wait_for 20 "[ \"\$(count web --from $FROM)\" = 20 ]"'
  check "a query without --from reads only local data and the cache" '[ "$(count web)" = 20 ]'
  check "the fetched chunk is in the cache" 'find "$CACHE/web" -name "*.parquet" | grep -q .'

  echo "== key rotation"
  "$BIN" storage rotate-key >/dev/null 2>&1
  for i in $(seq 5); do "$BIN" send rot "after rotation $i" >/dev/null; done
  check "uploads continue under the new key" 'wait_for 40 "list_keys run/rot/ | grep -q \\.parquet\\.enc$"'
  stop_daemon
  rm -rf "$DATA/web" "$DATA/rot" "$CACHE"
  start_daemon || return 1
  check "old and new uploads both restore" '"$BIN" storage restore web >/dev/null 2>&1 && "$BIN" storage restore rot >/dev/null 2>&1 && wait_for 10 "[ \"\$(count web)\" = 20 ] && [ \"\$(count rot)\" = 5 ]"'

  if [ -n "$BIG" ] && [ -f "$BIG" ]; then
    echo "== big chunk"
    local SUM SIZE DAY=2024-01-01
    SUM=$(shasum -a 256 "$BIG" | cut -d' ' -f1)
    SIZE=$(stat -f %z "$BIG" 2>/dev/null || stat -c %s "$BIG")
    stop_daemon
    set_key encrypt_uploads false
    mkdir -p "$DATA/big/$DAY"
    cp "$BIG" "$DATA/big/$DAY/1-1.parquet"
    start_daemon || return 1
    check "the big chunk is uploaded" 'wait_for 150 "list_keys run/big/ | grep -q 1-1.parquet$"'
    get_object "run/big/$DAY/1-1.parquet" "$WORK/big.back"
    check "the downloaded bytes match" '[ "$(shasum -a 256 "$WORK/big.back" | cut -d" " -f1)" = "$SUM" ]'
    declare -F big_object_checks >/dev/null && big_object_checks "run/big/$DAY/1-1.parquet" "$SIZE"
    check "the local copy is deleted after upload" 'wait_for 30 "[ ! -e \"$DATA/big/$DAY/1-1.parquet\" ]"'
    check "restore streams the big chunk back byte for byte" '"$BIN" storage restore big >/dev/null 2>&1 && [ "$(shasum -a 256 "$DATA/big/$DAY/1-1.parquet" | cut -d" " -f1)" = "$SUM" ]'
  fi
}
