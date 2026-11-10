#!/usr/bin/env bash
# Phase 6 check: autoscaler. "Burst triggers SCALE up, idle triggers drain + SCALE down;
# no flapping over a 2-minute run."
#
#   scripts/phase6_autoscale_check.sh real      # llama.cpp + real model        -> report/evidence/
#   scripts/phase6_autoscale_check.sh standin   # stand-in, NO model (SIMULATED) -> report/evidence/simulated/
#
# Uses gateway/config.yaml as it is (scaling thresholds, timers, max_replicas, prewarm: warm_pool);
# standin mode only swaps replica_image. Timeline: start (min_replicas), a burst of concurrent
# clients, then idle until RUN_S seconds have passed since the gateway started.
# Everything is read from the gateway log; times are the monotonic mono= field.
# Env: PYTHON (default python3), GW_PORT (default 8000), OVERWRITE=1 to replace evidence.
set -u
cd "$(dirname "$0")/.."
MODE="${1:-}"
PYTHON="${PYTHON:-python3}"
GW_PORT="${GW_PORT:-8000}"
RUN_S=180
case "$MODE" in
  real)    LABEL="REAL MODEL RESULT (llama.cpp containers, real model)"
           OUT=report/evidence/phase6_autoscale_real.txt; NCLIENTS=14; MAXT=40 ;;
  standin) LABEL="SIMULATED RESULT (stand-in replica containers, no model; timings are NOT model performance)"
           OUT=report/evidence/simulated/phase6_autoscale_standin_SIMULATED.txt; NCLIENTS=14; MAXT=300 ;;
  *) echo "usage: $0 real|standin"; exit 2 ;;
esac
if [ -e "$OUT" ] && [ "${OVERWRITE:-0}" != 1 ]; then echo "ABORT: $OUT exists (set OVERWRITE=1 to replace)"; exit 1; fi
mkdir -p "$(dirname "$OUT")"
WORK=$(mktemp -d); GW_PID=""
cleanup() { [ -n "$GW_PID" ] && kill "$GW_PID" 2>/dev/null; wait 2>/dev/null; rm -rf "$WORK"; }
trap cleanup EXIT
[ "$MODE" = standin ] && { docker build -q -t llmgw-standin:test -f tests/standin.Dockerfile tests >/dev/null || exit 1; }
PREFIX=$("$PYTHON" -c 'import yaml; print(yaml.safe_load(open("gateway/config.yaml"))["container_prefix"])')

"$PYTHON" - "$WORK/cfg.yaml" "$MODE" > "$WORK/cfgline.txt" <<'EOF'
import sys, yaml
cfg = yaml.safe_load(open("gateway/config.yaml"))
if sys.argv[2] == "standin":
    cfg["replica_image"] = "llmgw-standin:test"
yaml.safe_dump(cfg, open(sys.argv[1], "w"))
keys = ["min_replicas", "max_replicas", "slots_per_replica", "scale_up_threshold", "scale_down_threshold",
        "scale_up_after_s", "scale_down_cooldown_s", "prewarm", "warm_pool_size", "prewarm_trigger_ratio",
        "replica_cpus", "replica_memory"]
print("# config (gateway/config.yaml): " + " ".join(f"{k}={cfg[k]}" for k in keys))
EOF

echo "== starting gateway (config.yaml as is)"
LLMGW_CONFIG="$WORK/cfg.yaml" LLMGW_LOG_FILE="$WORK/gw.log" "$PYTHON" -m uvicorn gateway.main:app \
  --port "$GW_PORT" --log-level warning >"$WORK/uv.out" 2>&1 &
GW_PID=$!
T_START=$(date +%s)
for _ in $(seq 1 240); do grep -q '\[HEALTH\] replica ready' "$WORK/gw.log" 2>/dev/null && break; sleep 0.5; done
echo "== waiting for the idle warm spare (if prewarm: warm_pool)"
for _ in $(seq 1 60); do grep -q 'state=warm' "$WORK/gw.log" 2>/dev/null && break; sleep 0.5; done
sleep 5

echo "== burst: $NCLIENTS concurrent clients, max_tokens=$MAXT"
"$PYTHON" -c 'import time; print(time.monotonic())' > "$WORK/burst_mono.txt"
PIDS=()
for i in $(seq 1 "$NCLIENTS"); do
  curl -sN "http://127.0.0.1:$GW_PORT/v1/chat/completions" -H 'Content-Type: application/json' \
    -d "{\"messages\":[{\"role\":\"user\",\"content\":\"Request $i: write a few sentences about rivers.\"}],\"max_tokens\":$MAXT,\"stream\":true}" \
    > "$WORK/resp_$i.txt" &
  PIDS+=($!)
done
wait "${PIDS[@]}"
"$PYTHON" -c 'import time; print(time.monotonic())' > "$WORK/burst_end_mono.txt"
echo "== burst done after $(( $(date +%s) - T_START ))s; idle until ${RUN_S}s"
while [ $(( $(date +%s) - T_START )) -lt "$RUN_S" ]; do sleep 1; done
kill -TERM "$GW_PID"; wait "$GW_PID" 2>/dev/null; GW_PID=""
LEFT=$(docker ps -a --format '{{.Names}}' | grep -c "^$PREFIX")
OK=0; for i in $(seq 1 "$NCLIENTS"); do
  tail -c 100 "$WORK/resp_$i.txt" | grep -q 'data: \[DONE\]' && ! grep -q '"error"' "$WORK/resp_$i.txt" && OK=$((OK + 1))
done

{
  echo "# $LABEL"
  echo "# Phase 6 check: autoscaler (burst -> scale up, idle -> drain + scale down, no flapping), ${RUN_S}s run"
  echo "# Run: $(date -Is) | host: $(uname -sr) | dir: $PWD"
  echo "# Burst: $NCLIENTS concurrent clients, max_tokens=$MAXT"
  cat "$WORK/cfgline.txt"
  "$PYTHON" - "$WORK" "$OK" "$NCLIENTS" "$LEFT" "$RUN_S" <<'EOF'
import re, sys, yaml
work, ok, n, left, run_s = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5])
cfg = yaml.safe_load(open(f"{work}/cfg.yaml"))
log = open(f"{work}/gw.log").read().splitlines()
mono = lambda l: float(re.search(r"mono=([\d.]+)", l).group(1))
t0 = mono(log[0])
burst0 = float(open(f"{work}/burst_mono.txt").read()) - t0
burst1 = float(open(f"{work}/burst_end_mono.txt").read()) - t0
sig = [l for l in log if "[SCALE] signal" in l]
events = [l for l in log if re.search(r"\[SCALE\] (scale up|scale down|prewarm spare|replica stopped|replica started|scale up wanted)"
                                      r"|\[HEALTH\] replica ready|\[SCALE\] spare promoted", l)]
print(f"\n== timeline (s since gateway start; burst {burst0:.1f} -> {burst1:.1f}) ==")
for l in events:
    print(f"  {mono(l) - t0:7.1f}  " + l.split(" INFO gateway ")[1])
print("\n== signal samples every 5 s (from [SCALE] signal) ==")
print(f"  {'t':>6} {'signal':>7} {'queued':>6} {'in_flight':>9} {'ready':>5} {'spares':>6} {'starting':>8} {'total':>5}")
last = -99
for l in sig:
    t = mono(l) - t0
    if t - last >= 5:
        f = dict(re.findall(r"(\w+)=(\S+)", l.split("[SCALE] signal")[1]))
        print(f"  {t:6.1f} {f['value']:>7} {f['queued']:>6} {f['in_flight']:>9} {f['ready']:>5} {f['spares']:>6} {f['starting']:>8} {f['total']:>5}")
        last = t
# --- checks
ups = [(mono(l) - t0, l) for l in log if "[SCALE] scale up how=" in l]
downs = [(mono(l) - t0, l) for l in log if "[SCALE] scale down replica=" in l]
stopped = [l for l in log if "[SCALE] replica stopped" in l]
seq = sorted([(t, "up") for t, _ in ups] + [(t, "down") for t, _ in downs])
reversals = sum(1 for a, b in zip(seq, seq[1:]) if a[1] != b[1])
# routable since: READY after load, or promotion of a spare
routable_at = {}
for l in log:
    m = re.search(r"\[HEALTH\] replica ready replica=(r\d+) state=ready", l) or re.search(r"\[SCALE\] spare promoted replica=(r\d+)", l)
    if m:
        routable_at.setdefault(m.group(1), mono(l))
early = [l for l in log if (m := re.search(r"\[ROUTE\] dispatch req=\d+ replica=(r\d+)", l))
         and (m.group(1) not in routable_at or mono(l) < routable_at[m.group(1)])]
final = dict(re.findall(r"(\w+)=(\S+)", sig[-1].split("[SCALE] signal")[1]))
span = mono(sig[-1]) - t0
print("\n== checks ==")
def check(desc, cond):
    print(("PASS: " if cond else "FAIL: ") + desc)
check(f"run lasted at least 120 s ({span:.0f} s of autoscaler ticks)", span >= 120)
check(f"all {n} burst clients got [DONE] and no error ({ok}/{n})", ok == n)
check(f"burst triggered SCALE up ({len(ups)}x, first at {ups[0][0]:.1f} s)" if ups else "burst triggered SCALE up", bool(ups) and burst0 <= ups[0][0] <= burst1)
check(f"idle triggered drain + SCALE down ({len(downs)}x, all after the burst)",
      bool(downs) and all(t > burst1 for t, _ in downs) and all("drained=True" in l for l in stopped) and len(stopped) >= len(downs))
check(f"no flapping: all scale-ups come before all scale-downs ({reversals} direction change)", reversals <= 1)
check(f"back to min_replicas={cfg['min_replicas']} READY at the end (ready={final['ready']})", int(final["ready"]) == cfg["min_replicas"])
check(f"never more than max_replicas={cfg['max_replicas']} containers", max(int(dict(re.findall(r'(\w+)=(\S+)', l))['total']) for l in sig) <= cfg["max_replicas"])
check("no request was dispatched to a replica before it was READY or promoted", not early)
check("all replica containers removed after gateway exit", left == 0)
EOF
} > "$WORK/report.txt"

cp "$WORK/report.txt" "$OUT"
cat "$OUT"
echo "== saved: $OUT"
! grep -q '^FAIL' "$OUT"
