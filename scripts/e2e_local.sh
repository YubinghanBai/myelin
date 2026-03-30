#!/usr/bin/env bash
# Local E2E: Docker Postgres (logical) + optional NATS (docker compose).
#
#   chmod +x scripts/e2e_local.sh
#   # Phase 1 — log-only, slot resume (default)
#   ./scripts/e2e_local.sh
#   # Phase 2 — JetStream (compose NATS on 4222, monitoring 8222)
#   USE_NATS=1 ./scripts/e2e_local.sh
#   # Phase 3 — USE_NATS=1 only: oversized stall (exit ≠0) + drain replay; dead_letter + follow-up row (E2E_PHASE3=0 skips)
#   # Phase 6 — bulk INSERT → kill myelin → restart → every correlation_id appears (at-least-once; duplicates OK)
#   #   E2E_PHASE6=0 skips. E2E_BULK_ROWS defaults to 2000 (set 5000+ for stress). JetStream needs MYELIN_LOG_ENVELOPE=1 (set in script).
#   # SIGTERM — graceful exit (exit code 0): E2E_SIGTERM=1 (default 0). Works with USE_NATS=0 or 1.
#   # NATS fault — optional: E2E_NATS_FAULT=1 requires USE_NATS=1; stops NATS mid-run, expects myelin to exit non-zero after publish retries.
#   Phase 1 (dry-run) + Phase 2 (JetStream) each run UPDATE + DELETE on public.events (replica identity = PK).
#
# Cursor / sandboxes may set CARGO_TARGET_DIR; BIN path follows real cargo output.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target}"

export PGHOST="${PGHOST:-127.0.0.1}"
export PGPORT="${PGPORT:-5432}"
export PGUSER="${PGUSER:-postgres}"
export PGPASSWORD="${PGPASSWORD:-postgres}"
export PGDATABASE="${PGDATABASE:-postgres}"
export PGADMIN_URL="${PGADMIN_URL:-host=$PGHOST port=$PGPORT user=$PGUSER password=$PGPASSWORD dbname=$PGDATABASE}"
export RUST_LOG="${RUST_LOG:-info,myelin::envelope=info}"
export NATS_STREAM="${NATS_STREAM:-MYELIN}"

PG_CONTAINER="${PG_CONTAINER:-myelin-postgres}"
NATS_MONITOR_URL="${NATS_MONITOR_URL:-http://127.0.0.1:8222}"

if [[ "${USE_NATS:-0}" != "1" ]]; then
  unset NATS_URL || true
else
  export NATS_URL="${NATS_URL:-nats://127.0.0.1:4222}"
fi

docker compose up -d

echo "[e2e] PGHOST=$PGHOST PGPORT=$PGPORT BIN target=$TARGET_DIR/release/myelin"
echo "[e2e] Waiting for Postgres ($PG_CONTAINER)..."
for _ in $(seq 1 45); do
  if docker exec "$PG_CONTAINER" pg_isready -U postgres -d postgres &>/dev/null; then
    break
  fi
  sleep 1
done
docker exec "$PG_CONTAINER" pg_isready -U postgres -d postgres

# Free the logical slot if a prior myelin is still connected.
docker exec "$PG_CONTAINER" psql -U postgres -d postgres -Atq -c \
  "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = 'myelin_slot' AND active_pid IS NOT NULL;" \
  &>/dev/null || true

cargo build --release -q
BIN="$TARGET_DIR/release/myelin"
if [[ ! -x "$BIN" ]]; then
  echo "missing $BIN — cargo build --release failed?"
  exit 1
fi

E2E_TAG="$(date +%s)-$RANDOM"
LOG="$(mktemp)"
LOG_P6=""
LOG_SIGTERM=""
trap '[[ -n "${MYELIN_PID:-}" ]] && kill "$MYELIN_PID" 2>/dev/null || true; rm -f "$LOG"; [[ -n "${LOG_P6:-}" ]] && rm -f "$LOG_P6"; [[ -n "${LOG_SIGTERM:-}" ]] && rm -f "$LOG_SIGTERM"' EXIT

insert_row() {
  local cid="$1"
  docker exec "$PG_CONTAINER" psql -U postgres -d postgres -v ON_ERROR_STOP=1 -c "
    INSERT INTO public.events (correlation_id, kind, claim_uri, claim_meta)
    VALUES ('$cid', 'smoke', 's3://e2e/key', '{\"from\":\"e2e_local.sh\",\"tag\":\"$E2E_TAG\"}'::jsonb);
  "
}

# Requires myelin running; asserts on $LOG lines with cdc_envelope (dry-run always; JetStream needs MYELIN_LOG_ENVELOPE=1).
e2e_update_delete_smoke() {
  echo "[e2e] UPDATE + DELETE: insert → patch kind → insert → delete (pgoutput U / D)..."
  insert_row "e2e-${E2E_TAG}-ud-upd"
  local ud_up_id ud_del_id
  ud_up_id="$(docker exec "$PG_CONTAINER" psql -U postgres -d postgres -Atq -v ON_ERROR_STOP=1 -c \
    "SELECT id::text FROM public.events WHERE correlation_id = 'e2e-${E2E_TAG}-ud-upd' ORDER BY created_at DESC LIMIT 1;")"
  docker exec "$PG_CONTAINER" psql -U postgres -d postgres -v ON_ERROR_STOP=1 -c \
    "UPDATE public.events SET kind = 'patched' WHERE id = '${ud_up_id}'::uuid;" >/dev/null
  insert_row "e2e-${E2E_TAG}-ud-del"
  ud_del_id="$(docker exec "$PG_CONTAINER" psql -U postgres -d postgres -Atq -v ON_ERROR_STOP=1 -c \
    "SELECT id::text FROM public.events WHERE correlation_id = 'e2e-${E2E_TAG}-ud-del' ORDER BY created_at DESC LIMIT 1;")"
  docker exec "$PG_CONTAINER" psql -U postgres -d postgres -v ON_ERROR_STOP=1 -c \
    "DELETE FROM public.events WHERE id = '${ud_del_id}'::uuid;" >/dev/null
  export E2E_UD_DEL_ID="$ud_del_id"
  sleep 6
}

e2e_assert_update_delete_in_log() {
  if ! grep "e2e-${E2E_TAG}-ud-upd" "$LOG" | grep -q '"op":"update"'; then
    echo "=== FAIL: no update envelope for e2e-${E2E_TAG}-ud-upd (replica identity / publication?) ==="
    exit 1
  fi
  # Replica identity DEFAULT: delete tuple is often **PK columns only** — correlation_id may be null.
  if ! grep '"op":"delete"' "$LOG" | grep -q "$E2E_UD_DEL_ID"; then
    echo "=== FAIL: no delete envelope containing PK id $E2E_UD_DEL_ID (ud-del row) ==="
    exit 1
  fi
  if ! grep "e2e-${E2E_TAG}-ud-upd" "$LOG" | grep -q 'patched'; then
    echo "=== FAIL: update envelope should show kind patched ==="
    exit 1
  fi
  unset E2E_UD_DEL_ID || true
}

# Append myelin stdout/stderr to the given file (background).
start_myelin_to() {
  local logfile="$1"
  if [[ "${USE_NATS:-0}" == "1" ]]; then
    echo "[e2e] Starting myelin with NATS_URL=$NATS_URL (log -> ${logfile})"
  else
    echo "[e2e] Starting myelin (dry-run, NATS_URL unset) (log -> ${logfile})"
  fi
  "$BIN" >>"$logfile" 2>&1 &
  MYELIN_PID=$!
}

start_myelin() {
  start_myelin_to "$LOG"
}

stop_myelin() {
  if [[ -n "${MYELIN_PID:-}" ]]; then
    kill "$MYELIN_PID" 2>/dev/null || true
    wait "$MYELIN_PID" 2>/dev/null || true
    MYELIN_PID=
  fi
}

# Oversized row: jsonb pad forces serialized envelope over typical E2E caps.
insert_oversized_row() {
  local cid="$1"
  local reps="${2:-12000}"
  docker exec "$PG_CONTAINER" psql -U postgres -d postgres -v ON_ERROR_STOP=1 -c "
    INSERT INTO public.events (correlation_id, kind, claim_uri, claim_meta)
    VALUES (
      '$cid',
      'smoke',
      's3://e2e/key',
      jsonb_build_object('from','e2e_oversized','pad', repeat('x', ${reps}::int))
    );
  "
}

# Phase 3 (JetStream): MYELIN_OVERSIZED_POLICY stall → process exits non-zero; slot does not skip the bad change.
# Then drain with a high MYELIN_MAX_PAYLOAD_BYTES so the same WAL is replayed and published (unblocks slot).
# Phase 3b: dead_letter → DLQ notice + follow-up small row still on business path (no silent gap).
phase3_oversized_stall_and_dlq() {
  local LOG_P3A LOG_P3B LOG_DRAIN ec=0
  LOG_P3A="$(mktemp)"
  LOG_P3B="$(mktemp)"
  LOG_DRAIN="$(mktemp)"
  trap 'rm -f "$LOG_P3A" "$LOG_P3B" "$LOG_DRAIN"' RETURN

  echo ""
  echo "========== Phase 3: oversized stall → exit; drain; dead_letter + follow-up row (NATS only) =========="

  docker exec "$PG_CONTAINER" psql -U postgres -d postgres -Atq -c \
    "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = 'myelin_slot' AND active_pid IS NOT NULL;" \
    &>/dev/null || true
  stop_myelin
  sleep 1

  echo "[e2e] Phase 3a: MYELIN_MAX_PAYLOAD_BYTES low + stall — expect exit on oversized insert..."
  export MYELIN_MAX_PAYLOAD_BYTES=2500
  export MYELIN_OVERSIZED_POLICY=stall
  unset MYELIN_LOG_ENVELOPE || true

  "$BIN" >>"$LOG_P3A" 2>&1 &
  MYELIN_PID=$!
  sleep 4
  insert_oversized_row "e2e-${E2E_TAG}-p3a-huge" 12000

  local waited=0
  while kill -0 "$MYELIN_PID" 2>/dev/null && (( waited < 90 )); do
    sleep 1
    waited=$((waited + 1))
  done
  if kill -0 "$MYELIN_PID" 2>/dev/null; then
    echo "=== FAIL Phase 3a: myelin should exit after PayloadTooLarge (stall) ==="
    kill "$MYELIN_PID" 2>/dev/null || true
    wait "$MYELIN_PID" 2>/dev/null || true
    MYELIN_PID=
    tail -40 "$LOG_P3A"
    exit 1
  fi
  wait "$MYELIN_PID" || ec=$?
  MYELIN_PID=
  if [[ "${ec:-0}" -eq 0 ]]; then
    echo "=== FAIL Phase 3a: expected non-zero exit when stall hits oversized payload ==="
    tail -40 "$LOG_P3A"
    exit 1
  fi
  if ! grep -qiE 'PayloadTooLarge|payload .* exceeds max|exceeds max.*bytes' "$LOG_P3A"; then
    echo "=== FAIL Phase 3a: log should mention payload too large / exceeds max ==="
    tail -60 "$LOG_P3A"
    exit 1
  fi
  echo "=== PASS Phase 3a: stall produced non-zero exit and explicit oversize error in log ==="

  echo "[e2e] Phase 3a-drain: raise MYELIN_MAX_PAYLOAD_BYTES — replay same WAL, publish huge row, advance slot..."
  export MYELIN_MAX_PAYLOAD_BYTES=$((768 * 1024))
  export MYELIN_OVERSIZED_POLICY=stall
  export MYELIN_LOG_ENVELOPE=1
  start_myelin_to "$LOG_DRAIN"
  sleep 3
  local drain_wait=0
  while (( drain_wait < 120 )); do
    if grep -q "e2e-${E2E_TAG}-p3a-huge" "$LOG_DRAIN"; then
      break
    fi
    sleep 1
    drain_wait=$((drain_wait + 1))
  done
  stop_myelin
  unset MYELIN_LOG_ENVELOPE || true
  if ! grep -q "e2e-${E2E_TAG}-p3a-huge" "$LOG_DRAIN"; then
    echo "=== FAIL Phase 3a-drain: huge row should appear in log after replay with high max ==="
    tail -40 "$LOG_DRAIN"
    exit 1
  fi
  echo "=== PASS Phase 3a-drain: stuck oversized change replayed without silent skip ==="

  echo "[e2e] Phase 3b: dead_letter policy — huge row → DLQ notice; next small row still in log..."
  export MYELIN_MAX_PAYLOAD_BYTES=4096
  export MYELIN_OVERSIZED_POLICY=dead_letter
  export MYELIN_DLQ_SUBJECT="${NATS_SUBJECT_PREFIX:-myelin}.dlq"
  export MYELIN_LOG_ENVELOPE=1

  start_myelin_to "$LOG_P3B"
  sleep 4
  insert_row "e2e-${E2E_TAG}-p3b-small"
  sleep 3
  insert_oversized_row "e2e-${E2E_TAG}-p3b-huge" 12000
  sleep 2
  insert_row "e2e-${E2E_TAG}-p3b-after"
  sleep 12
  stop_myelin

  if ! grep -qiE 'MYELIN_OVERSIZED_PAYLOAD|myelin::dlq|dead-letter notice' "$LOG_P3B"; then
    echo "=== FAIL Phase 3b: expected DLQ / oversize notice in log ==="
    tail -80 "$LOG_P3B"
    exit 1
  fi
  for cid in "e2e-${E2E_TAG}-p3b-small" "e2e-${E2E_TAG}-p3b-after"; do
    if ! grep -q "$cid" "$LOG_P3B"; then
      echo "=== FAIL Phase 3b: missing envelope for $cid (silent gap after DLQ?) ==="
      tail -80 "$LOG_P3B"
      exit 1
    fi
  done

  unset MYELIN_LOG_ENVELOPE || true
  unset MYELIN_DLQ_SUBJECT || true
  export MYELIN_MAX_PAYLOAD_BYTES=$((768 * 1024))
  export MYELIN_OVERSIZED_POLICY=stall

  echo ""
  echo "=== PASS Phase 3: stall exits with error; drain replays; dead_letter allows pipeline after huge row ==="
  echo "Note: at-least-once — duplicate cdc_envelope lines for the same correlation_id are acceptable."
}

jsz_stream_messages() {
  local json
  json=$(curl -sf "${NATS_MONITOR_URL}/jsz?streams=1") || {
    echo "[e2e] FAIL: cannot GET ${NATS_MONITOR_URL}/jsz (is NATS -m 8222 up?)" >&2
    return 1
  }
  if command -v jq &>/dev/null; then
    echo "$json" | jq -r --arg s "$NATS_STREAM" '
      ([.account_details[]?.stream_detail[]? | select(.name == $s) | .state.messages][0]?) // 0
    '
    return
  fi
  if command -v python3 &>/dev/null; then
    echo "$json" | python3 -c "
import json,sys
stream='$NATS_STREAM'
data=json.load(sys.stdin)
for acc in data.get('account_details') or []:
  for s in acc.get('stream_detail') or []:
    if s.get('name')==stream:
      print(s.get('state',{}).get('messages',0))
      sys.exit(0)
print(0)
"
    return
  fi
  echo "[e2e] FAIL: need jq or python3 to parse JetStream /jsz" >&2
  return 1
}

# Bulk INSERT (single statement), then kill myelin mid-catch-up, restart, assert each id ≥1 log line.
phase6_bulk_kill_resume() {
  local n="${E2E_BULK_ROWS:-2000}"
  local bulk_tag="p6${RANDOM}$(date +%s)"
  # shellcheck disable=SC2001
  bulk_tag="$(echo "$bulk_tag" | sed 's/[^a-zA-Z0-9_]/_/g')"
  LOG_P6="$(mktemp)"
  echo ""
  echo "========== Phase 6: bulk ($n rows) + kill + resume (at-least-once; dup envelopes OK) =========="

  docker exec "$PG_CONTAINER" psql -U postgres -d postgres -Atq -c \
    "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = 'myelin_slot' AND active_pid IS NOT NULL;" \
    &>/dev/null || true
  stop_myelin
  sleep 1

  if [[ "${USE_NATS:-0}" == "1" ]]; then
    export MYELIN_LOG_ENVELOPE=1
  fi

  start_myelin_to "$LOG_P6"
  sleep 3

  echo "[e2e] Phase 6a: INSERT $n rows (correlation_id e2e-bulk-${bulk_tag}-<i>)..."
  # Shell-expand sanitized bulk_tag + integer n (avoid psql :var — brittle under docker exec).
  docker exec "$PG_CONTAINER" psql -U postgres -d postgres -v ON_ERROR_STOP=1 -c "
INSERT INTO public.events (correlation_id, kind, claim_uri, claim_meta)
SELECT format('e2e-bulk-${bulk_tag}-%s', g.i), 'smoke', 's3://e2e/key',
       jsonb_build_object('from','e2e_phase6','tag','${bulk_tag}','i', g.i)
FROM generate_series(1, ${n}) AS g(i);
"

  sleep 2
  echo "[e2e] Phase 6b: kill myelin (may interrupt catch-up)..."
  stop_myelin
  sleep 2

  echo "[e2e] Phase 6c: restart; expect replay + all ids present in log (duplicates allowed)..."
  start_myelin_to "$LOG_P6"
  # Scale wait with batch size (decode + optional JetStream).
  local wait_max=$((150 + (n + 499) / 500 * 60))
  (( wait_max > 420 )) && wait_max=420
  local waited=0
  local uniq_ids=0
  while (( waited < wait_max )); do
    uniq_ids="$(grep -o "e2e-bulk-${bulk_tag}-[0-9][0-9]*" "$LOG_P6" 2>/dev/null | sed "s/^e2e-bulk-${bulk_tag}-//" | sort -nu | wc -l | tr -d ' ')"
    [[ -z "$uniq_ids" ]] && uniq_ids=0
    if [[ "$uniq_ids" == "$n" ]]; then
      break
    fi
    sleep 1
    waited=$((waited + 1))
  done

  stop_myelin

  local db_count
  db_count="$(docker exec "$PG_CONTAINER" psql -U postgres -d postgres -Atq -c \
    "SELECT count(*)::text FROM public.events WHERE correlation_id LIKE 'e2e-bulk-${bulk_tag}-%'")"
  if [[ "$db_count" != "$n" ]]; then
    echo "=== FAIL Phase 6: PG row count for bulk batch = $db_count (expected $n) ==="
    tail -30 "$LOG_P6"
    exit 1
  fi

  if [[ "$uniq_ids" != "$n" ]]; then
    echo "=== FAIL Phase 6: after ${waited}s only $uniq_ids distinct envelope ids in log (expected $n) ==="
    tail -40 "$LOG_P6"
    exit 1
  fi

  local fail=0
  for i in $(seq 1 "$n"); do
    if ! grep -q "e2e-bulk-${bulk_tag}-${i}" "$LOG_P6"; then
      echo "=== FAIL Phase 6: correlation_id e2e-bulk-${bulk_tag}-$i never appeared in log ==="
      fail=1
    fi
  done
  if (( fail )); then
    tail -40 "$LOG_P6"
    exit 1
  fi

  echo "--- myelin tail (Phase 6) ---"
  tail -20 "$LOG_P6"
  echo ""
  echo "=== PASS Phase 6: all $n bulk ids present after kill+restart (repeats ok; dedupe downstream) ==="
  if [[ "${USE_NATS:-0}" == "1" ]]; then
    unset MYELIN_LOG_ENVELOPE || true
  fi
}

# --- Phase 1 (log envelope): burst, restart, resume row must appear ---
if [[ "${USE_NATS:-0}" != "1" ]]; then
  echo ""
  echo "========== Phase 1: dry-run + slot resume =========="
  start_myelin
  sleep 6
  echo "[e2e] Phase 1a: INSERT 3 rows (correlation_id e2e-${E2E_TAG}-bN)..."
  insert_row "e2e-${E2E_TAG}-b1"
  insert_row "e2e-${E2E_TAG}-b2"
  insert_row "e2e-${E2E_TAG}-b3"
  sleep 4
  echo "[e2e] Phase 1b: stop myelin, restart (same slot — expect no historical replay requirement)..."
  stop_myelin
  sleep 2
  start_myelin
  sleep 6
  echo "[e2e] Phase 1c: INSERT after restart (must be decoded)..."
  insert_row "e2e-${E2E_TAG}-resume"
  sleep 5
  echo "[e2e] Phase 1d: UPDATE + DELETE envelopes..."
  e2e_update_delete_smoke
  stop_myelin

  echo "--- myelin tail (Phase 1) ---"
  tail -60 "$LOG"

  if ! grep -q 'cdc_envelope' "$LOG"; then
    echo "=== FAIL Phase 1: no cdc_envelope in log ==="
    exit 1
  fi
  if ! grep -q "e2e-${E2E_TAG}-resume" "$LOG"; then
    echo "=== FAIL Phase 1: post-restart row e2e-${E2E_TAG}-resume not in log (resume broken?) ==="
    exit 1
  fi
  for n in 1 2 3; do
    if ! grep -q "e2e-${E2E_TAG}-b$n" "$LOG"; then
      echo "=== FAIL Phase 1: batch row e2e-${E2E_TAG}-b$n missing ==="
      exit 1
    fi
  done
  e2e_assert_update_delete_in_log
  echo ""
  echo "=== PASS Phase 1: burst + restart + resume + UPDATE/DELETE envelopes ==="
  echo "Note: at-least-once / duplicate envelopes on crash between decode and LSN update are possible;"
  echo "      downstream should dedupe (e.g. correlation_id + lsn_hex). See README.md / TESTING.md."

# --- Phase 2: JetStream, monitoring /jsz, reconnect smoke ---
else
  echo ""
  echo "========== Phase 2: JetStream (compose NATS) =========="
  echo "[e2e] Waiting for NATS monitoring $NATS_MONITOR_URL ..."
  for _ in $(seq 1 30); do
    if curl -sf "${NATS_MONITOR_URL}/varz" &>/dev/null; then
      break
    fi
    sleep 1
  done
  curl -sf "${NATS_MONITOR_URL}/varz" &>/dev/null || {
    echo "=== FAIL: NATS monitoring not reachable at $NATS_MONITOR_URL ==="
    exit 1
  }

  MSG0="$(jsz_stream_messages || true)"
  MSG0="${MSG0//[^0-9]/}"
  [[ -n "$MSG0" ]] || MSG0=0
  echo "[e2e] JetStream stream $NATS_STREAM messages (before): $MSG0"

  start_myelin
  sleep 8
  echo "[e2e] INSERT 2 rows (JetStream)..."
  insert_row "e2e-${E2E_TAG}-j1"
  insert_row "e2e-${E2E_TAG}-j2"
  sleep 6
  MSG1="$(jsz_stream_messages || true)"
  MSG1="${MSG1//[^0-9]/}"
  [[ -n "$MSG1" ]] || MSG1=0
  echo "[e2e] JetStream messages (after burst): $MSG1"
  if ! [[ "$MSG1" =~ ^[0-9]+$ ]] || (( MSG1 < MSG0 + 2 )); then
    echo "=== FAIL Phase 2: expected at least 2 new stream messages (before=$MSG0 after=$MSG1) ==="
    stop_myelin
    exit 1
  fi

  echo "[e2e] Phase 2b: kill myelin, reconnect, INSERT 1 row (at-least-once / resume smoke)..."
  stop_myelin
  sleep 2
  MSG_MID="$(jsz_stream_messages || true)"
  MSG_MID="${MSG_MID//[^0-9]/}"
  [[ -n "$MSG_MID" ]] || MSG_MID=0
  export MYELIN_LOG_ENVELOPE=1
  start_myelin
  sleep 8
  insert_row "e2e-${E2E_TAG}-j-after-reconnect"
  sleep 6
  echo "[e2e] Phase 2c: UPDATE + DELETE envelopes (log via MYELIN_LOG_ENVELOPE=1)..."
  e2e_update_delete_smoke
  MSG2="$(jsz_stream_messages || true)"
  MSG2="${MSG2//[^0-9]/}"
  [[ -n "$MSG2" ]] || MSG2=0
  stop_myelin
  echo "[e2e] JetStream messages (after reconnect + insert + UD): $MSG2 (mid=$MSG_MID)"

  if ! [[ "$MSG2" =~ ^[0-9]+$ ]] || (( MSG2 < MSG1 + 1 )); then
    echo "=== FAIL Phase 2: expected ≥1 more message after reconnect (msg1=$MSG1 msg2=$MSG2) ==="
    exit 1
  fi

  e2e_assert_update_delete_in_log
  unset MYELIN_LOG_ENVELOPE || true

  echo "--- myelin tail (Phase 2) ---"
  tail -40 "$LOG"

  docker exec "$PG_CONTAINER" psql -U postgres -d postgres -c \
    "SELECT slot_name, confirmed_flush_lsn, restart_lsn FROM pg_replication_slots WHERE slot_name = 'myelin_slot';"

  echo ""
  echo "=== PASS Phase 2: JetStream stream grew; reconnect + insert + UPDATE/DELETE OK ==="
  echo "Pull consumer (manual):"
  echo "  docker run --rm -it --network host natsio/nats-box:latest \\"
  echo "    nats -s $NATS_URL consumer add MYELIN e2e_pull --pull --filter 'myelin.>' --defaults"
  echo "  docker run --rm -it --network host natsio/nats-box:latest \\"
  echo "    nats -s $NATS_URL consumer next MYELIN e2e_pull 1"
  echo "On Docker Desktop (Mac), use host.docker.internal instead of 127.0.0.1 for -s if needed."

  if [[ "${E2E_PHASE3:-1}" == "1" ]]; then
    phase3_oversized_stall_and_dlq
  fi
fi

if [[ "${E2E_PHASE6:-1}" == "1" ]]; then
  phase6_bulk_kill_resume
fi

# --- SIGTERM: expect exit 0 and "graceful shutdown" in log (does not use MYELIN_PID / stop_myelin helpers). ---
e2e_sigterm_graceful_exit() {
  echo ""
  echo "========== SIGTERM graceful exit (expect status 0) =========="
  LOG_SIGTERM="$(mktemp)"
  docker exec "$PG_CONTAINER" psql -U postgres -d postgres -Atq -c \
    "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = 'myelin_slot' AND active_pid IS NOT NULL;" \
    &>/dev/null || true
  stop_myelin
  sleep 1

  if [[ "${USE_NATS:-0}" == "1" ]]; then
    export MYELIN_LOG_ENVELOPE="${MYELIN_LOG_ENVELOPE:-1}"
  fi

  # RUST_LOG must include myelin for shutdown line (default script export is enough).
  "$BIN" >>"$LOG_SIGTERM" 2>&1 &
  local st_pid=$!
  sleep 5
  insert_row "e2e-${E2E_TAG}-sigterm-probe"
  sleep 3
  kill -TERM "$st_pid"

  local ec=0
  wait "$st_pid" || ec=$?
  if [[ "$ec" -ne 0 ]]; then
    echo "=== FAIL SIGTERM: expected exit status 0, got $ec ==="
    tail -60 "$LOG_SIGTERM"
    exit 1
  fi
  if ! grep -qiE 'graceful shutdown|shutdown signal' "$LOG_SIGTERM"; then
    echo "=== FAIL SIGTERM: log should mention graceful shutdown (myelin shutdown path) ==="
    tail -60 "$LOG_SIGTERM"
    exit 1
  fi
  echo "--- myelin tail (SIGTERM) ---"
  tail -15 "$LOG_SIGTERM"
  echo "=== PASS SIGTERM: process exited 0 after SIGTERM ==="
  if [[ "${USE_NATS:-0}" == "1" ]] && [[ "${MYELIN_LOG_ENVELOPE:-}" == "1" ]]; then
    unset MYELIN_LOG_ENVELOPE || true
  fi
}

# --- NATS stopped: publish path should fail after retries (USE_NATS=1 only). ---
e2e_nats_fault_nonzero_exit() {
  echo ""
  echo "========== NATS fault: stop broker, expect myelin exit != 0 =========="
  [[ "${USE_NATS:-0}" == "1" ]] || {
    echo "=== SKIP E2E_NATS_FAULT: set USE_NATS=1 ==="
    return 0
  }
  local LOG_NF CONTAINER_NATS
  LOG_NF="$(mktemp)"
  CONTAINER_NATS="${NATS_CONTAINER:-myelin-nats}"

  docker exec "$PG_CONTAINER" psql -U postgres -d postgres -Atq -c \
    "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = 'myelin_slot' AND active_pid IS NOT NULL;" \
    &>/dev/null || true
  stop_myelin
  sleep 1

  export MYELIN_LOG_ENVELOPE=1
  # Speed up failure (still multiple retries).
  export MYELIN_PUBLISH_MAX_ATTEMPTS="${MYELIN_PUBLISH_MAX_ATTEMPTS:-4}"
  export MYELIN_PUBLISH_RETRY_INITIAL_MS="${MYELIN_PUBLISH_RETRY_INITIAL_MS:-80}"
  export MYELIN_PUBLISH_RETRY_MAX_MS="${MYELIN_PUBLISH_RETRY_MAX_MS:-800}"

  "$BIN" >>"$LOG_NF" 2>&1 &
  local nf_pid=$!
  sleep 5
  insert_row "e2e-${E2E_TAG}-nf-before"
  sleep 4

  echo "[e2e] Stopping $CONTAINER_NATS (NATS fault)..."
  docker stop "$CONTAINER_NATS" &>/dev/null || {
    echo "=== FAIL NATS fault: docker stop $CONTAINER_NATS ==="
    kill "$nf_pid" 2>/dev/null || true
    rm -f "$LOG_NF"
    exit 1
  }

  insert_row "e2e-${E2E_TAG}-nf-after-nats-down"
  local w=0 ec=0
  while kill -0 "$nf_pid" 2>/dev/null && (( w < 150 )); do
    sleep 1
    w=$((w + 1))
  done
  if kill -0 "$nf_pid" 2>/dev/null; then
    echo "=== FAIL NATS fault: myelin still running after ${w}s (expected publish exhaustion) ==="
    kill -TERM "$nf_pid" 2>/dev/null || true
    wait "$nf_pid" 2>/dev/null || true
    docker start "$CONTAINER_NATS" &>/dev/null || true
    tail -40 "$LOG_NF"
    rm -f "$LOG_NF"
    exit 1
  fi
  wait "$nf_pid" || ec=$?
  docker start "$CONTAINER_NATS" &>/dev/null || true
  sleep 3
  curl -sf "${NATS_MONITOR_URL}/varz" &>/dev/null || sleep 5

  if [[ "$ec" -eq 0 ]]; then
    echo "=== FAIL NATS fault: expected non-zero exit when NATS is down and rows are published ==="
    tail -60 "$LOG_NF"
    rm -f "$LOG_NF"
    exit 1
  fi
  if ! grep -qiE 'nats|JetStream|publish|PubAck|error|Error|retrying' "$LOG_NF"; then
    echo "=== WARN NATS fault: no obvious NATS/publish error in log (exit was $ec); check manually ==="
  fi
  echo "--- myelin tail (NATS fault) ---"
  tail -25 "$LOG_NF"
  echo "=== PASS NATS fault: myelin exited non-zero ($ec) after broker stop ==="
  rm -f "$LOG_NF"
  unset MYELIN_LOG_ENVELOPE || true
  unset MYELIN_PUBLISH_MAX_ATTEMPTS || true
  unset MYELIN_PUBLISH_RETRY_INITIAL_MS || true
  unset MYELIN_PUBLISH_RETRY_MAX_MS || true
}

if [[ "${E2E_SIGTERM:-0}" == "1" ]]; then
  e2e_sigterm_graceful_exit
fi

if [[ "${E2E_NATS_FAULT:-0}" == "1" ]]; then
  e2e_nats_fault_nonzero_exit
fi

echo ""
echo "Done."
