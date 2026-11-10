#!/usr/bin/env bash
# Phase 2 completion check: N replicas started/stopped by the gateway via the Docker
# Engine API (limits confirmed with docker inspect), and round_robin vs least_busy
# visible in [ROUTE] logs for 10 requests each.
#
#   scripts/phase2_routing_check.sh real      # llama.cpp + real model        -> report/evidence/
#   scripts/phase2_routing_check.sh standin   # stand-in, NO model (SIMULATED) -> report/evidence/simulated/
#
# Only config values change between runs (min_replicas: 3, router: <policy>); no code change.
# Env: PYTHON (default python3, needs requirements.txt), GW_PORT (default 8000), OVERWRITE=1 to replace evidence.
set -u
cd "$(dirname "$0")/.."
MODE="${1:-}"
PYTHON="${PYTHON:-python3}"
GW_PORT="${GW_PORT:-8000}"
N=3
case "$MODE" in
  real)    LABEL="REAL MODEL RESULT (llama.cpp containers, real model)"
           OUT=report/evidence/phase2_routing_real.txt; LONG=24; SHORT=4 ;;
  standin) LABEL="SIMULATED RESULT (stand-in replica containers, no model; timings are NOT model performance)"
           OUT=report/evidence/simulated/phase2_routing_standin_SIMULATED.txt; LONG=40; SHORT=4 ;;
  *) echo "usage: $0 real|standin"; exit 2 ;;
esac
if [ -e "$OUT" ] && [ "${OVERWRITE:-0}" != 1 ]; then echo "ABORT: $OUT exists (set OVERWRITE=1 to replace)"; exit 1; fi
mkdir -p "$(dirname "$OUT")"
WORK=$(mktemp -d); GW_PID=""
cleanup() { [ -n "$GW_PID" ] && kill "$GW_PID" 2>/dev/null; wait 2>/dev/null; rm -rf "$WORK"; }
trap cleanup EXIT

if [ "$MODE" = standin ]; then
  docker build -q -t llmgw-standin:test -f tests/standin.Dockerfile tests >/dev/null || { echo "FAIL: build stand-in image"; exit 1; }
fi
PREFIX=$("$PYTHON" -c 'import yaml; print(yaml.safe_load(open("gateway/config.yaml"))["container_prefix"])')

{
echo "# $LABEL"
echo "# Phase 2 check: $N replicas via Docker Engine API + round_robin / least_busy routing, 10 requests each"
echo "# Run: $(date -Is) | host: $(uname -sr) | cores: $(nproc) | dir: $PWD"
echo "# Requests: 10 per policy, one every 0.4 s, alternating max_tokens=$LONG (even) / $SHORT (odd)"
} > "$WORK/report.txt"

FAILED=0
check() { if eval "$2"; then echo "PASS: $1"; else echo "FAIL: $1"; FAILED=1; fi; }

for ROUTER in round_robin least_busy; do
  CFG="$WORK/config_$ROUTER.yaml"; GWLOG="$WORK/gw_$ROUTER.log"
  "$PYTHON" - "$CFG" "$ROUTER" "$MODE" "$N" <<'EOF'
import sys, yaml
cfg = yaml.safe_load(open("gateway/config.yaml"))
cfg.update(min_replicas=int(sys.argv[4]), router=sys.argv[2])
if sys.argv[3] == "standin":
    cfg.update(replica_image="llmgw-standin:test")
yaml.safe_dump(cfg, open(sys.argv[1], "w"))
EOF
  read -r WANT_CPU WANT_MEM IMAGE < <("$PYTHON" - "$CFG" <<'EOF'
import sys, yaml
c = yaml.safe_load(open(sys.argv[1]))
m = str(c["replica_memory"]).lower(); mult = {"k": 1 << 10, "m": 1 << 20, "g": 1 << 30}
mem = int(float(m[:-1]) * mult[m[-1]]) if m[-1] in mult else int(m)
print(int(float(c["replica_cpus"]) * 1e9), mem, c["replica_image"])
EOF
)
  echo "== [$ROUTER] starting gateway (min_replicas=$N, router=$ROUTER, image=$IMAGE)"
  LLMGW_CONFIG="$CFG" LLMGW_LOG_FILE="$GWLOG" \
    "$PYTHON" -m uvicorn gateway.main:app --port "$GW_PORT" --log-level warning >"$WORK/uvicorn_$ROUTER.out" 2>&1 &
  GW_PID=$!
  for _ in $(seq 1 360); do
    READY=$(grep -c '\[HEALTH\] replica ready' "$GWLOG" 2>/dev/null)
    [ "${READY:-0}" -ge "$N" ] && break
    kill -0 "$GW_PID" 2>/dev/null || break
    sleep 0.5
  done

  NAMES=$(docker ps --format '{{.Names}}' | grep "^$PREFIX" | sort)
  INSPECT=$(for n in $NAMES; do docker inspect -f '{{.Name}} NanoCpus={{.HostConfig.NanoCpus}} Memory={{.HostConfig.Memory}} Network={{.HostConfig.NetworkMode}} Image={{.Config.Image}}' "$n"; done)

  echo "== [$ROUTER] sending 10 requests"
  for i in $(seq 0 9); do
    MT=$([ $((i % 2)) -eq 0 ] && echo "$LONG" || echo "$SHORT")
    curl -sN "http://127.0.0.1:$GW_PORT/v1/chat/completions" -H 'Content-Type: application/json' \
      -d "{\"messages\":[{\"role\":\"user\",\"content\":\"Request $i: count slowly from one upwards.\"}],\"max_tokens\":$MT,\"stream\":true}" \
      > "$WORK/resp_${ROUTER}_$i.txt" &
    sleep 0.4
  done
  wait $(jobs -p | grep -v "^$GW_PID$") 2>/dev/null
  sleep 0.5

  echo "== [$ROUTER] stopping gateway (it removes its containers through the Docker API)"
  kill -TERM "$GW_PID"; wait "$GW_PID" 2>/dev/null; GW_PID=""
  AFTER=$(docker ps -a --format '{{.Names}}' | grep -c "^$PREFIX")

  OKRESP=0; for i in $(seq 0 9); do
    tail -c 200 "$WORK/resp_${ROUTER}_$i.txt" | grep -q 'data: \[DONE\]' && ! grep -q '"error"' "$WORK/resp_${ROUTER}_$i.txt" && OKRESP=$((OKRESP + 1))
  done
  {
    echo
    echo "################ router: $ROUTER ################"
    echo "== containers while running (docker inspect) =="
    echo "$INSPECT"
    echo "== gateway decision log =="
    grep -E '\[(SCHED|ROUTE|HEALTH|SCALE)\]' "$GWLOG" | grep -v '\[SCHED\]'
    echo "== replica chosen per request (from [ROUTE] dispatch) =="
    grep '\[ROUTE\] dispatch' "$GWLOG" | sed -E 's/.*req=([0-9]+) replica=(r[0-9]+).*loads_before=([^ ]*).*/req=\1 -> \2   (loads before: \3)/'
    echo "== checks =="
    check "$N replicas reached READY"                    "[ \"\$(grep -c '\[HEALTH\] replica ready' '$GWLOG')\" -eq $N ]"
    check "$N containers running during the test"        "[ \"\$(echo '$NAMES' | grep -c .)\" -eq $N ]"
    check "every container has NanoCpus=$WANT_CPU"       "[ \"\$(echo '$INSPECT' | grep -c 'NanoCpus=$WANT_CPU ')\" -eq $N ]"
    check "every container has Memory=$WANT_MEM"         "[ \"\$(echo '$INSPECT' | grep -c 'Memory=$WANT_MEM ')\" -eq $N ]"
    check "10 dispatch decisions logged with policy=$ROUTER" "[ \"\$(grep -c '\[ROUTE\] dispatch.*policy=$ROUTER' '$GWLOG')\" -eq 10 ]"
    check "10 requests completed ([ROUTE] complete)"     "[ \"\$(grep -c '\[ROUTE\] complete' '$GWLOG')\" -eq 10 ]"
    check "all 10 clients got [DONE] and no error"       "[ $OKRESP -eq 10 ]"
    check "all $N containers removed after gateway exit" "[ $AFTER -eq 0 ] && [ \"\$(grep -c '\[SCALE\] replica removed' '$GWLOG')\" -eq $N ]"
  } >> "$WORK/report.txt"
done

cp "$WORK/report.txt" "$OUT"
cat "$OUT"
grep -q '^FAIL' "$OUT" && FAILED=1
echo "== saved: $OUT"
exit $FAILED
