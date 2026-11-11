#!/usr/bin/env bash
# Baseline comparison: fixed pool of 1, fixed pool of 3, autoscaling - same workload, same metrics.
#
#   scripts/compare_pools.sh real      # llama.cpp + real model        -> report/evidence/
#   scripts/compare_pools.sh standin   # stand-in, NO model (SIMULATED) -> report/evidence/simulated/
#
# Three runs, config-only differences (everything else = gateway/config.yaml):
#   fixed1     min_replicas=1 max_replicas=1 prewarm=none       (autoscaler has no room to act)
#   fixed3     min_replicas=3 max_replicas=3 prewarm=none       (autoscaler has no room to act)
#   autoscale  gateway/config.yaml as is (min 1, max 3, prewarm warm_pool, thresholds unchanged)
# Workload = demo 3 / Phase 6 burst: fresh gateway, wait until min_replicas replicas are READY,
# idle IDLE_S seconds (autoscale: the warm spare loads during this time), then NCLIENTS concurrent
# streamed clients with the same prompts and max_tokens; stop the gateway when all have finished.
#
# Metrics (same definitions as demo 3, from the gateway log, monotonic mono= field):
#   first token   arrival ([SCHED] queued) -> dispatch + that request's ttft_s ([ROUTE] complete)
#   complete      arrival -> [ROUTE] complete
#   makespan      first arrival -> last completion
# Cost (derived from [SCALE] replica started / stopped / removed events): peak replica containers,
# and container-seconds during the burst window (first arrival -> last completion); each container
# reserves replica_cpus CPUs, so this is a proxy for CPU reserved, not CPU used.
# Env: PYTHON (default python3), GW_PORT (default 8000), OVERWRITE=1 to replace evidence.
set -u
cd "$(dirname "$0")/.."
MODE="${1:-}"
PYTHON="${PYTHON:-python3}"
GW_PORT="${GW_PORT:-8000}"
IDLE_S=15
case "$MODE" in
  real)    LABEL="REAL MODEL RESULT (llama.cpp containers, real model)"
           OUT=report/evidence/pool_comparison_real.txt; NCLIENTS=14; MAXT=40 ;;
  standin) LABEL="SIMULATED RESULT (stand-in replica containers, no model; timings are NOT model performance)"
           OUT=report/evidence/simulated/pool_comparison_standin_SIMULATED.txt; NCLIENTS=14; MAXT=300 ;;
  *) echo "usage: $0 real|standin"; exit 2 ;;
esac
if [ -e "$OUT" ] && [ "${OVERWRITE:-0}" != 1 ]; then echo "ABORT: $OUT exists (set OVERWRITE=1 to replace)"; exit 1; fi
mkdir -p "$(dirname "$OUT")"
WORK=$(mktemp -d); GW_PID=""
cleanup() { [ -n "$GW_PID" ] && kill "$GW_PID" 2>/dev/null; wait 2>/dev/null; rm -rf "$WORK"; }
trap cleanup EXIT
[ "$MODE" = standin ] && { docker build -q -t llmgw-standin:test -f tests/standin.Dockerfile tests >/dev/null || exit 1; }
PREFIX=$("$PYTHON" -c 'import yaml; print(yaml.safe_load(open("gateway/config.yaml"))["container_prefix"])')
POOLS="fixed1 fixed3 autoscale"

for POOL in $POOLS; do
  MINR=$("$PYTHON" - "$WORK/cfg_$POOL.yaml" "$MODE" "$POOL" <<'EOF'
import sys, yaml
cfg = yaml.safe_load(open("gateway/config.yaml"))
pool = sys.argv[3]
if pool == "fixed1":
    cfg.update(min_replicas=1, max_replicas=1, prewarm="none")
elif pool == "fixed3":
    cfg.update(min_replicas=3, max_replicas=3, prewarm="none")
if sys.argv[2] == "standin":
    cfg["replica_image"] = "llmgw-standin:test"
yaml.safe_dump(cfg, open(sys.argv[1], "w"))
print(cfg["min_replicas"])
EOF
)
  echo "== [$POOL] starting gateway, waiting for $MINR READY replica(s)"
  LLMGW_CONFIG="$WORK/cfg_$POOL.yaml" LLMGW_LOG_FILE="$WORK/gw_$POOL.log" "$PYTHON" -m uvicorn gateway.main:app \
    --port "$GW_PORT" --log-level warning >"$WORK/uv_$POOL.out" 2>&1 &
  GW_PID=$!
  for _ in $(seq 1 240); do
    R=$(grep -c '\[HEALTH\] replica ready replica=r[0-9]* state=ready' "$WORK/gw_$POOL.log" 2>/dev/null)
    [ "${R:-0}" -ge "$MINR" ] && break; sleep 0.5
  done
  echo "== [$POOL] idle ${IDLE_S}s, then burst of $NCLIENTS clients (max_tokens=$MAXT)"
  sleep "$IDLE_S"
  "$PYTHON" -c 'import time; print(time.monotonic())' > "$WORK/burst_mono_$POOL.txt"
  PIDS=()
  for i in $(seq 1 "$NCLIENTS"); do
    curl -sN "http://127.0.0.1:$GW_PORT/v1/chat/completions" -H 'Content-Type: application/json' \
      -d "{\"messages\":[{\"role\":\"user\",\"content\":\"Request $i: write a few sentences about rivers.\"}],\"max_tokens\":$MAXT,\"stream\":true}" \
      > "$WORK/resp_${POOL}_$i.txt" &
    PIDS+=($!)
  done
  wait "${PIDS[@]}"
  sleep 0.5
  kill -TERM "$GW_PID"; wait "$GW_PID" 2>/dev/null; GW_PID=""
  docker ps -a --format '{{.Names}}' | grep -c "^$PREFIX" > "$WORK/left_$POOL.txt"
  OK=0; for i in $(seq 1 "$NCLIENTS"); do
    tail -c 100 "$WORK/resp_${POOL}_$i.txt" | grep -q 'data: \[DONE\]' && ! grep -q '"error"' "$WORK/resp_${POOL}_$i.txt" && OK=$((OK + 1))
  done
  echo "$OK" > "$WORK/ok_$POOL.txt"
done

{
  echo "# $LABEL"
  echo "# Baseline comparison: fixed pool of 1 vs fixed pool of 3 vs autoscaling, same burst, same metrics"
  echo "# Run: $(date -Is) | host: $(uname -sr) | dir: $PWD"
  echo "# Each run: fresh gateway, min_replicas READY, ${IDLE_S}s idle, burst of $NCLIENTS concurrent clients (same prompts), max_tokens=$MAXT. One run per pool."
  "$PYTHON" - "$WORK" "$NCLIENTS" "$POOLS" <<'EOF'
import re, statistics, sys, yaml
work, n, pools = sys.argv[1], int(sys.argv[2]), sys.argv[3].split()
cfgs = {p: yaml.safe_load(open(f"{work}/cfg_{p}.yaml")) for p in pools}
keys = ["min_replicas", "max_replicas", "prewarm", "warm_pool_size", "slots_per_replica", "scale_up_threshold",
        "scale_up_after_s", "scale_down_threshold", "scale_down_cooldown_s", "replica_image", "replica_cpus",
        "replica_memory", "scheduler", "router"]
for p in pools:
    print(f"# config {p}: " + " ".join(f"{k}={cfgs[p][k]}" for k in keys))
base = cfgs["autoscale"]
mono = lambda l: float(re.search(r"mono=([\d.]+)", l).group(1))
res = {}
for p in pools:
    log = open(f"{work}/gw_{p}.log").read().splitlines()
    burst = float(open(f"{work}/burst_mono_{p}.txt").read())
    ok, left = int(open(f"{work}/ok_{p}.txt").read()), int(open(f"{work}/left_{p}.txt").read())
    print(f"\n################ run {p} ################")
    queued = {m.group(1): mono(l) for l in log if (m := re.search(r"\[SCHED\] queued req=(\d+)", l))}
    disp = {}
    for l in log:
        if (m := re.search(r"\[ROUTE\] dispatch req=(\d+) replica=(r\d+)", l)):
            disp.setdefault(m.group(1), (mono(l), m.group(2)))
    comp = {m.group(1): (mono(l), float(m.group(2))) for l in log
            if (m := re.search(r"\[ROUTE\] complete req=(\d+) .*ttft_s=([\d.]+)", l))}
    ttfts = [disp[q][0] - queued[q] + comp[q][1] for q in comp if q in queued and q in disp]
    totals = [comp[q][0] - queued[q] for q in comp if q in queued]
    t_first, t_last = min(queued.values()), max(c for c, _ in comp.values())
    started = {m.group(1): mono(l) for l in log if (m := re.search(r"\[SCALE\] replica started replica=(r\d+)", l))}
    ended = {m.group(1): mono(l) for l in log if (m := re.search(r"\[SCALE\] replica (?:stopped|removed) replica=(r\d+)", l))}
    csec = sum(max(0.0, min(ended.get(r, float("inf")), t_last) - max(s, t_first)) for r, s in started.items())
    sig = [{k: int(v) for k, v in re.findall(r" (ready|total)=(\d+)", l)} for l in log
           if "[SCALE] signal " in l and t_first <= mono(l) <= t_last]
    idle = [{k: int(v) for k, v in re.findall(r" (ready|total)=(\d+)", l)} for l in log
            if "[SCALE] signal " in l and mono(l) < burst]
    idle_total = idle[-1]["total"] if idle else None
    peak_ready = max((s["ready"] for s in sig), default=None)
    peak_total = max((s["total"] for s in sig), default=None)
    ups = [l for l in log if re.search(r"\[SCALE\] scale up how=", l)]
    per_rep = {}
    for q, (_, r) in disp.items():
        per_rep[r] = per_rep.get(r, 0) + 1
    print("== replica events (s after burst start) ==")
    for l in log:
        if re.search(r"\[SCALE\] (replica started|scale up |spare promoted|prewarm spare)|\[HEALTH\] replica ready", l):
            print(f"  {mono(l) - burst:+7.2f}  " + l.split(" INFO gateway ")[1].split(" ip=")[0])
    print(f"== requests per replica: {dict(sorted(per_rep.items()))}")
    print("== request latency of the burst clients (arrival -> first token / -> complete, s) ==")
    print(f"  first token: mean {statistics.mean(ttfts):.2f}  median {statistics.median(ttfts):.2f}  max {max(ttfts):.2f}")
    print(f"  complete:    mean {statistics.mean(totals):.2f}  median {statistics.median(totals):.2f}  max {max(totals):.2f}")
    print(f"  burst makespan (first arrival -> last completion): {t_last - t_first:.2f}")
    print(f"== cost: replica containers while idle before the burst {idle_total}, "
          f"peak READY replicas {peak_ready}, peak replica containers {peak_total}, "
          f"container-seconds during the burst window {csec:.1f} (x replica_cpus={cfgs[p]['replica_cpus']} CPU each)")
    res[p] = dict(ft=statistics.mean(ttfts), ftmax=max(ttfts), cm=statistics.mean(totals), mk=t_last - t_first,
                  pr=peak_ready, pt=peak_total, cs=csec, it=idle_total)
    print("== checks ==")
    def check(desc, cond):
        print(("PASS: " if cond else "FAIL: ") + f"[{p}] " + desc)
    check(f"all {n} clients got [DONE] and no error ({ok}/{n})", ok == n)
    check(f"all {n} requests completed in the log ({len(comp)}/{n})", len(comp) == n)
    if p == "autoscale":
        check("the burst triggered a scale-up", bool(ups))
    else:
        check(f"fixed pool: no scale-up and exactly {cfgs[p]['min_replicas']} replica(s) started",
              not ups and len(started) == cfgs[p]["min_replicas"])
    check("all replica containers removed after gateway exit", left == 0)
print("\n################ comparison (one run per pool) ################")
print(f"  {'pool':>10} {'first token mean':>17} {'first token max':>16} {'complete mean':>14} {'makespan':>9} "
      f"{'idle containers':>16} {'peak READY':>11} {'peak containers':>16} {'container-s in burst':>21}")
for p in pools:
    r = res[p]
    print(f"  {p:>10} {r['ft']:>17.2f} {r['ftmax']:>16.2f} {r['cm']:>14.2f} {r['mk']:>9.2f} {r['it']!s:>16} {r['pr']!s:>11} {r['pt']!s:>16} {r['cs']:>21.1f}")
print("  Latency in seconds. One run per pool; no statistical claim is made.")
diff = {p: sorted(k for k in base if base[k] != cfgs[p].get(k)) for p in pools if p != "autoscale"}
want = {"fixed1": ["max_replicas", "prewarm"], "fixed3": ["min_replicas", "prewarm"]}   # config.yaml max is 3
print(f"\n# config keys that differ from autoscale: {diff}")
print(("PASS" if diff == want else "FAIL") + f": [all] only the pool keys differ between runs (expected {want})")
EOF
} > "$WORK/report.txt"

cp "$WORK/report.txt" "$OUT"
cat "$OUT"
echo "== saved: $OUT"
! grep -q '^FAIL' "$OUT"
