#!/usr/bin/env bash
# run_all_demos.sh - run every phase demo/check, in phase order, with the existing scripts.
#
#   scripts/run_all_demos.sh real    [RESULTS_DIR]   # llama.cpp + real model  (~25 min)
#   scripts/run_all_demos.sh standin [RESULTS_DIR]   # stand-in image, SIMULATED (~20 min)
#
# The mode is required: nothing runs the real model unless "real" is given.
#
# Order (phase):  1 phase1_stream_check   2 phase2_routing_check   3 demo1_scheduling
#                 4 demo2_affinity        5 demo4_fault             6 phase6_autoscale_check
#                 7 demo3_prewarm         8 demo5_config
#
# Approved evidence is never touched: each demo writes to report/evidence/... of the tree it runs
# in, so the runner copies the project (without report/evidence/) into a fresh temporary
# directory and runs the unchanged demo scripts there. The copy is deleted at the end; give
# RESULTS_DIR to keep each demo's output file and console log there instead.
# A failing demo does not stop the run (so every failure is reported), but the runner exits 1.
# After each demo, leftover gateway containers are reported as a failure and removed.
#
# Prerequisites (Linux host with Docker, e.g. WSL2 Ubuntu; run from anywhere):
#   - Docker reachable by the current user; port GW_PORT (default 8000) free
#   - Python with requirements.txt installed: activate the venv, or set PYTHON=/path/to/python
#   - real mode: the model file in ./models and the llama.cpp image pulled
#     (scripts/check_feasibility.sh downloads/pulls both)
#   - standin mode: python:3.12-slim pullable (the demos build llmgw-standin:test themselves)
set -u
MODE="${1:-}"
RESULTS="${2:-}"
case "$MODE" in real|standin) ;; *) echo "usage: $0 real|standin [RESULTS_DIR]"; exit 2 ;; esac
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON="${PYTHON:-python3}"
export PYTHON GW_PORT="${GW_PORT:-8000}"
DEMOS="phase1_stream_check phase2_routing_check demo1_scheduling demo2_affinity demo4_fault phase6_autoscale_check demo3_prewarm demo5_config"

# ---- prerequisites (fail fast, before anything starts) -------------------------------------
fail() { echo "PREREQUISITE FAILED: $*"; exit 2; }
docker info >/dev/null 2>&1 || fail "Docker is not reachable"
"$PYTHON" -c "import fastapi, uvicorn, httpx, yaml, docker" 2>/dev/null || fail "$PYTHON lacks requirements.txt packages"
PREFIX=$("$PYTHON" -c 'import yaml,sys; print(yaml.safe_load(open(sys.argv[1]))["container_prefix"])' "$ROOT/gateway/config.yaml")
if [ "$MODE" = real ]; then
  read -r MODELS MODEL IMAGE < <("$PYTHON" -c 'import yaml,sys; c=yaml.safe_load(open(sys.argv[1])); print(c["models_dir"], c["model_file"], c["replica_image"])' "$ROOT/gateway/config.yaml")
  [ -f "$ROOT/$MODELS/$MODEL" ] || fail "model $ROOT/$MODELS/$MODEL missing (run scripts/check_feasibility.sh)"
  docker image inspect "$IMAGE" >/dev/null 2>&1 || fail "image $IMAGE not pulled (run scripts/check_feasibility.sh)"
fi
[ -z "$(docker ps -aq --filter "name=^${PREFIX}")" ] || fail "containers named ${PREFIX}* already exist; stop them first"
! (exec 3<>"/dev/tcp/127.0.0.1/$GW_PORT") 2>/dev/null || fail "port $GW_PORT is in use"

# ---- isolated copy of the project ------------------------------------------------------------
WORK=$(mktemp -d /tmp/llmgw-demos.XXXXXX)
cleanup() {
  ids=$(docker ps -aq --filter "name=^${PREFIX}"); [ -n "$ids" ] && docker rm -f $ids >/dev/null 2>&1
  rm -rf "$WORK"
}
trap cleanup EXIT
trap 'echo "interrupted"; exit 130' INT TERM
mkdir -p "$WORK/project"
tar -C "$ROOT" --exclude=./.git --exclude=./report/evidence --exclude=./report.zip --exclude=./models \
    --exclude='*/__pycache__' --exclude=./.pytest_cache -cf - . | tar -C "$WORK/project" -xf -
if [ -d "$ROOT/models" ]; then ln -s "$ROOT/models" "$WORK/project/models"; else mkdir "$WORK/project/models"; fi
[ -n "$RESULTS" ] && mkdir -p "$RESULTS"

echo "=================================================================="
echo " run_all_demos.sh  mode=$MODE  $( [ "$MODE" = real ] && echo 'REAL MODEL' || echo 'SIMULATED stand-in (no model)')"
echo " started $(date -Is) | host $(uname -sr) | python $("$PYTHON" -V 2>&1) | docker $(docker version --format '{{.Server.Version}}')"
echo " source $ROOT  ->  isolated copy $WORK/project"
echo "=================================================================="
declare -a SUMMARY
FAILED=0
cd "$WORK/project"
for demo in $DEMOS; do
  echo; echo "------------------------------------------------------------------"
  echo " [$demo] start $(date -Is)"
  echo "------------------------------------------------------------------"
  log="$WORK/$demo.log"
  t0=$(cut -d' ' -f1 /proc/uptime)
  timeout --signal=TERM --kill-after=30 "${DEMO_TIMEOUT_S:-1800}" bash "scripts/$demo.sh" "$MODE" > "$log" 2>&1
  rc=$?                                   # 124 = timed out
  t1=$(cut -d' ' -f1 /proc/uptime)
  npass=$(grep -c '^PASS' "$log"); nfail=$(grep -c '^FAIL' "$log")
  stray=""
  if (exec 3<>"/dev/tcp/127.0.0.1/$GW_PORT") 2>/dev/null; then     # a gateway still listening
    stray=yes; pkill -f "uvicorn gateway.main:app --port $GW_PORT"; sleep 3
    echo " [$demo] a gateway process was left running: stopped"
  fi
  left=$(docker ps -aq --filter "name=^${PREFIX}")
  if [ -n "$left" ]; then docker rm -f $left >/dev/null 2>&1; echo " [$demo] gateway containers were left behind: removed"; fi
  status=PASS
  { [ "$rc" -ne 0 ] || [ "$nfail" -ne 0 ] || [ "$npass" -eq 0 ] || [ -n "$left" ] || [ -n "$stray" ]; } && { status=FAIL; FAILED=1; }
  grep -E '^(PASS|FAIL)' "$log" | sed 's/^/   /'
  [ "$status" = FAIL ] && { echo "   --- last 15 lines of output ---"; tail -15 "$log" | sed 's/^/   | /'; }
  line=$(printf "%-24s %-4s exit=%-3s checks: %3s PASS %3s FAIL   %6.0f s" "$demo" "$status" "$rc" "$npass" "$nfail" "$(echo "$t1 - $t0" | bc)")
  echo " [$demo] $line"
  SUMMARY+=("$line")
  [ -n "$RESULTS" ] && cp "$log" "$RESULTS/"
done
# the copy started without report/evidence, so everything in it was written by this run
[ -n "$RESULTS" ] && [ -d report/evidence ] && cp -r report/evidence "$RESULTS/outputs"

echo; echo "=================================================================="
echo " SUMMARY  mode=$MODE  finished $(date -Is)"
for s in "${SUMMARY[@]}"; do echo "   $s"; done
left=$(docker ps -aq --filter "name=^${PREFIX}")
echo "   gateway containers left after the run: $( [ -z "$left" ] && echo none || echo "$left")"
[ "$FAILED" -eq 0 ] && echo " RESULT: ALL DEMOS PASSED" || echo " RESULT: AT LEAST ONE DEMO FAILED"
echo "=================================================================="
exit "$FAILED"
