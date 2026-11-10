#!/usr/bin/env bash
# Phase 1 completion check: one request streamed  curl -N -> gateway -> one replica.
#
#   scripts/phase1_stream_check.sh real      # llama.cpp container + real model   -> report/evidence/
#   scripts/phase1_stream_check.sh standin   # stand-in replica, NO model (SIMULATED) -> report/evidence/simulated/
#
# The real replica uses the container settings from gateway/config.yaml (same flags
# as check_feasibility.sh). Phase 2 replaces this manual start with ReplicaManager.
# Env: PYTHON (default python3, needs requirements.txt installed), GW_PORT (default 8000).
set -u
cd "$(dirname "$0")/.."
MODE="${1:-}"
PYTHON="${PYTHON:-python3}"
GW_PORT="${GW_PORT:-8000}"
CONTAINER=llmgw-phase1-replica

case "$MODE" in
  real)    REPLICA_PORT=18080; LABEL="REAL MODEL RESULT (llama.cpp container, real model)"
           OUT=report/evidence/phase1_gateway_stream_real.txt ;;
  standin) REPLICA_PORT=18081; LABEL="SIMULATED RESULT (stand-in replica, no model; timings are NOT model performance)"
           OUT=report/evidence/simulated/phase1_gateway_stream_standin_SIMULATED.txt ;;
  *) echo "usage: $0 real|standin"; exit 2 ;;
esac
mkdir -p "$(dirname "$OUT")"
WORK=$(mktemp -d); GWLOG="$WORK/gateway.log"; STREAM="$WORK/stream.txt"
GW_PID=""; STANDIN_PID=""
cleanup() {
  [ -n "$GW_PID" ] && kill "$GW_PID" 2>/dev/null
  [ -n "$STANDIN_PID" ] && kill "$STANDIN_PID" 2>/dev/null
  [ "$MODE" = real ] && docker rm -f "$CONTAINER" >/dev/null 2>&1
  rm -rf "$WORK"
}
trap cleanup EXIT

echo "== starting replica ($MODE)"
if [ "$MODE" = real ]; then
  eval "$("$PYTHON" -c '
from gateway.interfaces import Config
c = Config.load("gateway/config.yaml")
print(f"IMAGE={c.replica_image} CPUS={c.replica_cpus} MEM={c.replica_memory} SLOTS={c.slots_per_replica} "
      f"CTX={c.slots_per_replica * c.ctx_per_slot} MODEL={c.model_file} MODELS_DIR={c.models_dir}")')"
  [ -f "$MODELS_DIR/$MODEL" ] || { echo "FAIL: model $MODELS_DIR/$MODEL missing (run check_feasibility.sh first)"; exit 1; }
  docker rm -f "$CONTAINER" >/dev/null 2>&1
  docker run -d --name "$CONTAINER" --cpus "$CPUS" --memory "$MEM" -p "127.0.0.1:$REPLICA_PORT:8080" \
    -v "$(realpath "$MODELS_DIR"):/models:ro" "$IMAGE" \
    -m "/models/$MODEL" -c "$CTX" -np "$SLOTS" --cont-batching --host 0.0.0.0 --port 8080 >/dev/null \
    || { echo "FAIL: docker run"; exit 1; }
  LIMITS=$(docker inspect -f 'NanoCpus={{.HostConfig.NanoCpus}} Memory={{.HostConfig.Memory}}' "$CONTAINER")
else
  "$PYTHON" tests/standin_replica.py --port "$REPLICA_PORT" --startup-delay 3 --token-rate 10 --slots 2 &
  STANDIN_PID=$!
  LIMITS="n/a (stand-in process: startup-delay 3 s, token-rate 10 tok/s, slots 2)"
fi

echo "== starting gateway on :$GW_PORT"
LLMGW_STATIC_REPLICA="http://127.0.0.1:$REPLICA_PORT" LLMGW_LOG_FILE="$GWLOG" \
  "$PYTHON" -m uvicorn gateway.main:app --port "$GW_PORT" --log-level warning >"$WORK/uvicorn.out" 2>&1 &
GW_PID=$!
for _ in $(seq 1 240); do
  grep -q '\[HEALTH\] replica ready' "$GWLOG" 2>/dev/null && break
  kill -0 "$GW_PID" 2>/dev/null || { echo "FAIL: gateway exited"; cat "$WORK/uvicorn.out"; exit 1; }
  sleep 0.5
done
grep -q '\[HEALTH\] replica ready' "$GWLOG" || { echo "FAIL: replica not ready after 120 s"; exit 1; }

echo "== curl -N through the gateway"
REQ='{"messages":[{"role":"user","content":"Say hello in five words."}],"max_tokens":32,"stream":true}'
T0=$(date +%s.%N)
curl -sN "http://127.0.0.1:$GW_PORT/v1/chat/completions" -H 'Content-Type: application/json' -d "$REQ" |
  while IFS= read -r line; do
    [ -n "$line" ] && printf '+%7.3fs  %s\n' "$(echo "$(date +%s.%N) - $T0" | bc)" "$line"
  done > "$STREAM"
sleep 0.3   # let the gateway write its 'complete' log line

TEXT=$("$PYTHON" -c '
import json, sys
out = ""
for line in open(sys.argv[1]):
    data = line.split("data: ", 1)[-1].strip()
    if data and data != "[DONE]":
        for c in json.loads(data).get("choices", []):
            out += (c.get("delta") or {}).get("content") or ""
print(out)' "$STREAM")

check() { if eval "$2"; then echo "PASS: $1"; else echo "FAIL: $1"; FAILED=1; fi; }
FAILED=0
{
  echo "# $LABEL"
  echo "# Phase 1 check: gateway streams one request through to one replica with curl -N"
  echo "# Run: $(date -Is) | host: $(uname -sr) | dir: $PWD"
  echo "# Replica limits: $LIMITS"
  echo "# Command: curl -sN http://127.0.0.1:$GW_PORT/v1/chat/completions -d '$REQ'"
  echo "# Left column: time since curl started, measured by the client"
  echo "== streamed response =="
  cat "$STREAM"
  echo "== assembled text =="
  echo "$TEXT"
  echo "== gateway decision log =="
  grep -E '\[(SCHED|ROUTE|HEALTH)\]' "$GWLOG"
  echo "== checks =="
  check "stream ended with [DONE]"                 'tail -1 "$STREAM" | grep -q "data: \[DONE\]"'
  check "more than one chunk arrived over time"    '[ "$(grep -c "data: {" "$STREAM")" -gt 2 ]'
  check "no error event"                           '! grep -q "\"error\"" "$STREAM"'
  check "gateway logged dispatch to r1"            'grep -q "\[ROUTE\] dispatch req=[0-9]* replica=r1" "$GWLOG"'
  check "gateway logged complete"                  'grep -q "\[ROUTE\] complete" "$GWLOG"'
} | tee "$OUT"
grep -q '^FAIL' "$OUT" && FAILED=1
echo "== saved: $OUT"
exit $FAILED
