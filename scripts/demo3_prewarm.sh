#!/usr/bin/env bash
# Demo 3 / Phase 7: pre-warming, measured. "Same burst run with prewarm: none and warm_pool; time
# from trigger to first request served on the new replica is recorded for both."
#
#   scripts/demo3_prewarm.sh real      # llama.cpp + real model        -> report/evidence/
#   scripts/demo3_prewarm.sh standin   # stand-in, NO model (SIMULATED) -> report/evidence/simulated/
#
# Two runs, one per mode. Config = gateway/config.yaml with ONLY `prewarm` set (none | warm_pool);
# standin mode also swaps replica_image (same for both runs). Each run is the full lifecycle:
# fresh gateway, wait until r1 is READY, idle IDLE_S seconds (in warm_pool the spare loads during
# this time), the same burst of NCLIENTS concurrent clients with the same prompts (scale-up), then
# idle until the autoscaler has drained and stopped the extra replicas and the pool is back at
# min_replicas READY (warm_pool: plus warm_pool_size re-armed spares), at most LIFE_TIMEOUT_S
# seconds after the burst; then stop the gateway.
#
# Measured from the gateway log (monotonic mono= field) for the FIRST scale-up of each run:
#   trigger           the "[SCALE] scale up" decision
#   routable          new replica READY (none) or spare promoted (warm_pool)
#   first dispatch    first request routed to the new replica
#   first token       first token from the new replica (dispatch + that request's ttft_s)
# Replica start-up (model load) is reported separately from request latency:
#   load_s of the new replica, and whether it was on the burst's critical path.
# Request latency per client: arrival (queued) -> first token, and arrival -> complete.
# Lifecycle (both modes): READY replicas before / peak / end, every scale-down (drained or not),
# time back at min_replicas, final pool, and that nothing scaled up again after the first scale-down.
# Env: PYTHON (default python3), GW_PORT (default 8000), OVERWRITE=1 to replace evidence.
set -u
cd "$(dirname "$0")/.."
MODE="${1:-}"
PYTHON="${PYTHON:-python3}"
GW_PORT="${GW_PORT:-8000}"
IDLE_S=15
LIFE_TIMEOUT_S=300
case "$MODE" in
  real)    LABEL="REAL MODEL RESULT (llama.cpp containers, real model)"
           OUT=report/evidence/phase7_prewarm_lifecycle_real.txt; NCLIENTS=14; MAXT=40 ;;
  standin) LABEL="SIMULATED RESULT (stand-in replica containers, no model; timings are NOT model performance)"
           OUT=report/evidence/simulated/phase7_prewarm_lifecycle_standin_SIMULATED.txt; NCLIENTS=14; MAXT=300 ;;
  *) echo "usage: $0 real|standin"; exit 2 ;;
esac
if [ -e "$OUT" ] && [ "${OVERWRITE:-0}" != 1 ]; then echo "ABORT: $OUT exists (set OVERWRITE=1 to replace)"; exit 1; fi
mkdir -p "$(dirname "$OUT")"
WORK=$(mktemp -d); GW_PID=""
cleanup() { [ -n "$GW_PID" ] && kill "$GW_PID" 2>/dev/null; wait 2>/dev/null; rm -rf "$WORK"; }
trap cleanup EXIT
[ "$MODE" = standin ] && { docker build -q -t llmgw-standin:test -f tests/standin.Dockerfile tests >/dev/null || exit 1; }
PREFIX=$("$PYTHON" -c 'import yaml; print(yaml.safe_load(open("gateway/config.yaml"))["container_prefix"])')

for PREWARM in none warm_pool; do
  "$PYTHON" - "$WORK/cfg_$PREWARM.yaml" "$MODE" "$PREWARM" <<'EOF'
import sys, yaml
cfg = yaml.safe_load(open("gateway/config.yaml"))
cfg["prewarm"] = sys.argv[3]
if sys.argv[2] == "standin":
    cfg["replica_image"] = "llmgw-standin:test"
yaml.safe_dump(cfg, open(sys.argv[1], "w"))
EOF
  echo "== [$PREWARM] starting gateway"
  LLMGW_CONFIG="$WORK/cfg_$PREWARM.yaml" LLMGW_LOG_FILE="$WORK/gw_$PREWARM.log" "$PYTHON" -m uvicorn gateway.main:app \
    --port "$GW_PORT" --log-level warning >"$WORK/uv_$PREWARM.out" 2>&1 &
  GW_PID=$!
  for _ in $(seq 1 240); do grep -q '\[HEALTH\] replica ready' "$WORK/gw_$PREWARM.log" 2>/dev/null && break; sleep 0.5; done
  echo "== [$PREWARM] idle ${IDLE_S}s, then burst of $NCLIENTS clients (max_tokens=$MAXT)"
  sleep "$IDLE_S"
  "$PYTHON" -c 'import time; print(time.monotonic())' > "$WORK/burst_mono_$PREWARM.txt"
  PIDS=()
  for i in $(seq 1 "$NCLIENTS"); do
    curl -sN "http://127.0.0.1:$GW_PORT/v1/chat/completions" -H 'Content-Type: application/json' \
      -d "{\"messages\":[{\"role\":\"user\",\"content\":\"Request $i: write a few sentences about rivers.\"}],\"max_tokens\":$MAXT,\"stream\":true}" \
      > "$WORK/resp_${PREWARM}_$i.txt" &
    PIDS+=($!)
  done
  wait "${PIDS[@]}"
  read -r MINR WPS < <("$PYTHON" -c 'import sys, yaml; c = yaml.safe_load(open(sys.argv[1])); print(c["min_replicas"], c["warm_pool_size"] if c["prewarm"] == "warm_pool" else 0)' "$WORK/cfg_$PREWARM.yaml")
  echo "== [$PREWARM] burst done; idle until back at $MINR READY replica(s) + $WPS spare(s) (max ${LIFE_TIMEOUT_S}s)"
  LOG="$WORK/gw_$PREWARM.log"
  for _ in $(seq 1 "$LIFE_TIMEOUT_S"); do
    ND=$(grep -c '\[SCALE\] scale down ' "$LOG"); NS=$(grep -c '\[SCALE\] replica stopped ' "$LOG")
    LAST=$(grep '\[SCALE\] signal ' "$LOG" | tail -1)
    if [ "$ND" -ge 1 ] && [ "$ND" = "$NS" ] && echo "$LAST" | grep -q " ready=$MINR spares=$WPS starting=0 "; then break; fi
    sleep 1
  done
  sleep 0.5
  kill -TERM "$GW_PID"; wait "$GW_PID" 2>/dev/null; GW_PID=""
  docker ps -a --format '{{.Names}}' | grep -c "^$PREFIX" > "$WORK/left_$PREWARM.txt"
  OK=0; for i in $(seq 1 "$NCLIENTS"); do
    tail -c 100 "$WORK/resp_${PREWARM}_$i.txt" | grep -q 'data: \[DONE\]' && ! grep -q '"error"' "$WORK/resp_${PREWARM}_$i.txt" && OK=$((OK + 1))
  done
  echo "$OK" > "$WORK/ok_$PREWARM.txt"
done

{
  echo "# $LABEL"
  echo "# Phase 7 / demo 3: prewarm none vs warm_pool, same burst; time from scale-up trigger to first request served on the new replica;"
  echo "# full lifecycle in both modes: scale-up, burst, idle, scale-down back to min_replicas"
  echo "# Run: $(date -Is) | host: $(uname -sr) | dir: $PWD"
  echo "# Each run: fresh gateway, r1 READY, ${IDLE_S}s idle, burst of $NCLIENTS concurrent clients (same prompts), max_tokens=$MAXT,"
  echo "# then idle until the pool is back at min_replicas READY (warm_pool: + re-armed spares), max ${LIFE_TIMEOUT_S}s. One run per mode."
  "$PYTHON" - "$WORK" "$NCLIENTS" <<'EOF'
import re, statistics, sys, yaml
work, n = sys.argv[1], int(sys.argv[2])
cfgs = {p: yaml.safe_load(open(f"{work}/cfg_{p}.yaml")) for p in ("none", "warm_pool")}
diff = sorted(k for k in cfgs["none"] if cfgs["none"][k] != cfgs["warm_pool"].get(k))
keys = ["min_replicas", "max_replicas", "slots_per_replica", "scale_up_threshold", "scale_up_after_s",
        "scale_down_threshold", "scale_down_cooldown_s", "warm_pool_size", "prewarm_trigger_ratio",
        "replica_image", "replica_cpus", "replica_memory", "scheduler", "router"]
print("# shared config: " + " ".join(f"{k}={cfgs['none'][k]}" for k in keys))
print(f"# config keys that differ between the two runs: {diff}")
mono = lambda l: float(re.search(r"mono=([\d.]+)", l).group(1))

def first(lines, pattern):
    for l in lines:
        m = re.search(pattern, l)
        if m:
            return l, m
    return None, None

res, checks = {}, []
for p in ("none", "warm_pool"):
    log = open(f"{work}/gw_{p}.log").read().splitlines()
    burst = float(open(f"{work}/burst_mono_{p}.txt").read())
    ok, left = int(open(f"{work}/ok_{p}.txt").read()), int(open(f"{work}/left_{p}.txt").read())
    print(f"\n################ run prewarm: {p} ################")
    queued = {m.group(1): mono(l) for l in log if (m := re.search(r"\[SCHED\] queued req=(\d+)", l))}
    disp = {}
    for l in log:
        m = re.search(r"\[ROUTE\] dispatch req=(\d+) replica=(r\d+)", l)
        if m:
            disp.setdefault(m.group(1), (mono(l), m.group(2)))
    comp = {m.group(1): (mono(l), float(m.group(2))) for l in log
            if (m := re.search(r"\[ROUTE\] complete req=(\d+) .*ttft_s=([\d.]+)", l))}
    ready_at = {m.group(1): (mono(l), float(m.group(2))) for l in log
                if (m := re.search(r"\[HEALTH\] replica ready replica=(r\d+) state=\w+ load_s=([\d.]+)", l))}
    promoted_at = {m.group(1): mono(l) for l in log if (m := re.search(r"\[SCALE\] spare promoted replica=(r\d+)", l))}
    routable_at = {}
    for l in log:
        m = re.search(r"\[HEALTH\] replica ready replica=(r\d+) state=ready", l) or re.search(r"\[SCALE\] spare promoted replica=(r\d+)", l)
        if m:
            routable_at.setdefault(m.group(1), mono(l))
    print("== scale events (s after burst start) ==")
    for l in log:
        if re.search(r"\[SCALE\] (scale up|prewarm spare|spare promoted|scale up wanted)|\[HEALTH\] replica ready", l):
            print(f"  {mono(l) - burst:+7.2f}  " + l.split(" INFO gateway ")[1])
    ups = [(l, m) for l in log if (m := re.search(r"\[SCALE\] scale up how=(\w+) replica=(r\d+)", l))]
    print("== each scale-up: trigger -> routable -> first dispatch -> first token on the new replica (s) ==")
    rows = []
    for k, (l, m) in enumerate(ups, 1):
        how, rid = m.group(1), m.group(2)
        t_trig = mono(l)
        t_rout = routable_at.get(rid)
        firsts = sorted((t, q) for q, (t, r) in disp.items() if r == rid)
        t_disp, q = firsts[0] if firsts else (None, None)
        t_tok = t_disp + comp[q][1] if q in comp else None
        load = ready_at.get(rid, (None, None))
        on_path = "yes" if how == "start_new" else f"no (loaded {load[0] - burst:+.2f} s vs burst start)" if load[0] else "?"
        rows.append((k, how, rid, t_trig, t_rout, t_disp, t_tok, load[1]))
        f = lambda x: f"{x - t_trig:6.2f}" if x is not None else "     -"
        print(f"  scale-up {k}: how={how} replica={rid} trigger at {t_trig - burst:+.2f} s | routable +{f(t_rout)} | "
              f"first dispatch +{f(t_disp)} | first token +{f(t_tok)} | replica load_s={load[1]} | load on critical path: {on_path}")
    ttfts = [disp[q][0] - queued[q] + comp[q][1] for q in comp if q in queued and q in disp]
    totals = [comp[q][0] - queued[q] for q in comp if q in queued]
    print("== request latency of the burst clients (arrival -> first token / -> complete, s) ==")
    if ttfts:
        print(f"  first token: mean {statistics.mean(ttfts):.2f}  median {statistics.median(ttfts):.2f}  max {max(ttfts):.2f}")
        print(f"  complete:    mean {statistics.mean(totals):.2f}  median {statistics.median(totals):.2f}  max {max(totals):.2f}")
        print(f"  burst makespan (first arrival -> last completion): {max(c for c, _ in comp.values()) - min(queued.values()):.2f}")
    early = [l for l in log if (m := re.search(r"\[ROUTE\] dispatch req=\d+ replica=(r\d+)", l))
             and (m.group(1) not in routable_at or mono(l) < routable_at[m.group(1)])]
    res[p] = rows[0] if rows else None
    sig = [(mono(l), {k: int(v) for k, v in re.findall(r" (ready|spares|starting|total)=(\d+)", l)})
           for l in log if "[SCALE] signal " in l]
    downs = [(mono(l), m.group(1)) for l in log if (m := re.search(r"\[SCALE\] scale down replica=(r\d+)", l))]
    stops = [(mono(l), m.group(1), m.group(2)) for l in log
             if (m := re.search(r"\[SCALE\] replica stopped replica=(r\d+) drained=(\w+)", l))]
    before = [s for t, s in sig if t < burst]
    after = [s for t, s in sig if t >= burst]
    final = sig[-1][1] if sig else {}
    min_r, wps = cfgs[p]["min_replicas"], (cfgs[p]["warm_pool_size"] if p == "warm_pool" else 0)
    last_done = max((c for c, _ in comp.values()), default=None)
    print("== lifecycle: scale-up -> burst -> idle -> scale-down (s after burst start) ==")
    print(f"  READY replicas: {before[-1]['ready'] if before else '?'} before the burst -> peak "
          f"{max((s['ready'] for s in after), default='?')} -> {final.get('ready', '?')} at the end")
    print(f"  scale-ups: {len(ups)}" + (f" (first at {mono(ups[0][0]) - burst:+.2f}, last at {mono(ups[-1][0]) - burst:+.2f})" if ups else ""))
    if last_done is not None:
        print(f"  last burst request complete at {last_done - burst:+.2f}")
    for t, rid in downs:
        st = next(((ts, d) for ts, r, d in stops if r == rid), None)
        print(f"  scale down {rid} at {t - burst:+.2f}" + (f" -> stopped at {st[0] - burst:+.2f}, drained={st[1]}" if st else " -> not stopped"))
    for l in log:
        if (m := re.search(r"\[SCALE\] prewarm spare replica=(r\d+) trigger=(\w+)", l)) and mono(l) >= burst:
            print(f"  prewarm spare {m.group(1)} (trigger={m.group(2)}) at {mono(l) - burst:+.2f}")
    if stops:
        print(f"  back at min_replicas={min_r} READY at {stops[-1][0] - burst:+.2f} (last replica stopped)")
    print(f"  final pool: " + " ".join(f"{k}={final.get(k, '?')}" for k in ("ready", "spares", "starting", "total")))
    print("== checks ==")
    def check(desc, cond):
        print(("PASS: " if cond else "FAIL: ") + f"[{p}] " + desc)
    check(f"all {n} clients got [DONE] and no error ({ok}/{n})", ok == n)
    check("the burst triggered a scale-up", bool(ups))
    want = "start_new" if p == "none" else "promote_spare"
    check(f"first scale-up was {want}", bool(ups) and ups[0][1].group(1) == want)
    if p == "warm_pool":
        sp = [r for r, (t, _) in ready_at.items() if t < burst and r in promoted_at]
        check("the promoted spare was loaded before the burst started", bool(sp))
    else:
        check("no spare was ever started", not any("[SCALE] prewarm spare" in l for l in log))
    check("no request dispatched to a replica before it was READY or promoted", not early)
    check(f"idle after the burst scaled down ({len(downs)} scale-down(s)) and every stopped replica drained",
          bool(downs) and len(stops) == len(downs) and all(d == "True" for _, _, d in stops))
    check(f"back at min_replicas={min_r} READY at the end, nothing starting "
          f"(ready={final.get('ready')} starting={final.get('starting')})",
          final.get("ready") == min_r and final.get("starting") == 0)
    check("no scale-up after the first scale-down (no flapping)",
          bool(downs) and all(mono(l) < downs[0][0] for l, _ in ups))
    if p == "warm_pool":
        check(f"warm pool re-armed at the end (spares={final.get('spares')}, warm_pool_size={wps})", final.get("spares") == wps)
    else:
        check(f"pool back to exactly min_replicas={min_r} containers (total={final.get('total')})", final.get("total") == min_r)
    check("all replica containers removed after gateway exit", left == 0)

print("\n################ comparison: first scale-up of each run ################")
print(f"  {'':>12} {'how':>14} {'trigger->routable':>18} {'trigger->1st dispatch':>22} {'trigger->1st token':>19} {'new replica load_s':>19}")
for p in ("none", "warm_pool"):
    r = res[p]
    if r:
        k, how, rid, tt, tr, td, tk, ld = r
        g = lambda x: f"{x - tt:.2f}" if x is not None else "-"
        print(f"  {p:>12} {how:>14} {g(tr):>18} {g(td):>22} {g(tk):>19} {str(ld):>19}")
print("  One run per mode (as the Phase 7 criterion specifies); no statistical claim is made.")
print(f"\nPASS: [both] config keys differing between runs are exactly ['prewarm']" if diff == ["prewarm"]
      else f"\nFAIL: [both] config keys differing between runs: {diff}")
EOF
} > "$WORK/report.txt"

cp "$WORK/report.txt" "$OUT"
cat "$OUT"
echo "== saved: $OUT"
! grep -q '^FAIL' "$OUT"
