#!/usr/bin/env bash
# Phase 1 completion check: one request streamed  curl -N -> gateway -> one replica.
#
#   scripts/phase1_stream_check.sh real      # llama.cpp container + real model   -> report/evidence/
#   scripts/phase1_stream_check.sh standin   # stand-in replica, NO model (SIMULATED) -> report/evidence/simulated/
#
# Since Phase 2 the gateway starts the replica itself through the ReplicaManager
# (min_replicas: 1 from gateway/config.yaml); standin mode only swaps replica_image.
# Existing evidence is never overwritten unless OVERWRITE=1.
# Env: PYTHON (default python3, needs requirements.txt installed), GW_PORT (default 8000).
set -u
cd "$(dirname "$0")/.."
MODE="${1:-}"
PYTHON="${PYTHON:-python3}"
GW_PORT="${GW_PORT:-8000}"

case "$MODE" in
  real)    LABEL="REAL MODEL RESULT (llama.cpp container, real model)"
           OUT=report/evidence/phase1_gateway_stream_real.txt ;;
  standin) LABEL="SIMULATED RESULT (stand-in replica, no model; timings are NOT model performance)"
           OUT=report/evidence/simulated/phase1_gateway_stream_standin_SIMULATED.txt ;;
  *) echo "usage: $0 real|standin"; exit 2 ;;
esac
if [ -e "$OUT" ] && [ "${OVERWRITE:-0}" != 1 ]; then echo "ABORT: $OUT exists (set OVERWRITE=1 to replace)"; exit 1; fi
mkdir -p "$(dirname "$OUT")"
WORK=$(mktemp -d); GWLOG="$WORK/gateway.log"; STREAM="$WORK/stream.txt"; CFG="$WORK/config.yaml"
GW_PID=""
cleanup() { [ -n "$GW_PID" ] && kill "$GW_PID" 2>/dev/null; wait 2>/dev/null; rm -rf "$WORK"; }
trap cleanup EXIT

if [ "$MODE" = standin ]; then
  docker build -q -t llmgw-standin:test -f tests/standin.Dockerfile tests >/dev/null || { echo "FAIL: build stand-in image"; exit 1; }
fi
"$PYTHON" - "$CFG" "$MODE" <<'EOF'
import sys, yaml
cfg = yaml.safe_load(open("gateway/config.yaml"))
cfg["min_replicas"] = 1
if sys.argv[2] == "standin":
    cfg["replica_image"] = "llmgw-standin:test"
yaml.safe_dump(cfg, open(sys.argv[1], "w"))
EOF
PREFIX=$("$PYTHON" -c 'import yaml,sys; print(yaml.safe_load(open(sys.argv[1]))["container_prefix"])' "$CFG")

echo "== starting gateway on :$GW_PORT (it starts replica r1 via the Docker API)"
LLMGW_CONFIG="$CFG" LLMGW_LOG_FILE="$GWLOG" \
  "$PYTHON" -m uvicorn gateway.main:app --port "$GW_PORT" --log-level warning >"$WORK/uvicorn.out" 2>&1 &
GW_PID=$!
for _ in $(seq 1 240); do
  grep -q '\[HEALTH\] replica ready' "$GWLOG" 2>/dev/null && break
  kill -0 "$GW_PID" 2>/dev/null || { echo "FAIL: gateway exited"; cat "$WORK/uvicorn.out"; exit 1; }
  sleep 0.5
done
grep -q '\[HEALTH\] replica ready' "$GWLOG" || { echo "FAIL: replica not ready after 120 s"; exit 1; }
LIMITS=$(docker inspect -f 'NanoCpus={{.HostConfig.NanoCpus}} Memory={{.HostConfig.Memory}} Image={{.Config.Image}}' "${PREFIX}r1")

echo "== curl -N through the gateway"
REQ='{"messages":[{"role":"user","content":"Say hello in five words."}],"max_tokens":32,"stream":true}'
# Elapsed time from /proc/uptime (monotonic, 10 ms resolution), not `date`: WSL2 can step the wall clock.
read -r T0 _ < /proc/uptime
curl -sN "http://127.0.0.1:$GW_PORT/v1/chat/completions" -H 'Content-Type: application/json' -d "$REQ" |
  while IFS= read -r line; do
    read -r NOW _ < /proc/uptime
    [ -n "$line" ] && printf '+%7.3fs  %s\n' "$(echo "$NOW - $T0" | bc)" "$line"
  done > "$STREAM"
sleep 0.3   # let the gateway write its 'complete' log line
kill -TERM "$GW_PID"; wait "$GW_PID" 2>/dev/null; GW_PID=""

TEXT=$("$PYTHON" -c '
import json, sys
out = ""
for line in open(sys.argv[1]):
    data = line.split("data: ", 1)[-1].strip()
    if data and data != "[DONE]":
        for c in json.loads(data).get("choices", []):
            out += (c.get("delta") or {}).get("content") or ""
print(out)' "$STREAM")

check() { if eval "$2"; then echo "PASS: $1"; else echo "FAIL: $1"; fi; }
{
  echo "# $LABEL"
  echo "# Phase 1 check: gateway streams one request through to one replica with curl -N"
  echo "# Run: $(date -Is) | host: $(uname -sr) | dir: $PWD"
  echo "# Replica r1 (started by the gateway): $LIMITS"
  echo "# Command: curl -sN http://127.0.0.1:$GW_PORT/v1/chat/completions -d '$REQ'"
  echo "# Left column: time since curl started, measured by the client (monotonic /proc/uptime)"
  echo "== streamed response =="
  cat "$STREAM"
  echo "== assembled text =="
  echo "$TEXT"
  echo "== gateway decision log =="
  grep -E '\[(SCHED|ROUTE|HEALTH|SCALE)\]' "$GWLOG"
  echo "== checks =="
  check "stream ended with [DONE]"                 'tail -1 "$STREAM" | grep -q "data: \[DONE\]"'
  check "more than one chunk arrived over time"    '[ "$(grep -c "data: {" "$STREAM")" -gt 2 ]'
  check "no error event"                           '! grep -q "\"error\"" "$STREAM"'
  check "gateway logged dispatch to r1"            'grep -q "\[ROUTE\] dispatch req=[0-9]* replica=r1" "$GWLOG"'
  check "gateway logged complete"                  'grep -q "\[ROUTE\] complete" "$GWLOG"'
  check "replica container removed at exit"        '! docker ps -a --format "{{.Names}}" | grep -q "^${PREFIX}r1$"'
} | tee "$OUT"
echo "== saved: $OUT"
! grep -q '^FAIL' "$OUT"
