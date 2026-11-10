#!/usr/bin/env bash
# Demo 1 / Phase 3 check: request scheduling through the gateway.
#
#   scripts/demo1_scheduling.sh real      # llama.cpp + real model        -> report/evidence/
#   scripts/demo1_scheduling.sh standin   # stand-in, NO model (SIMULATED) -> report/evidence/simulated/
#
# One replica with one slot (config only: min_replicas 1, slots_per_replica 1), so a queue forms
# in the gateway and the scheduler alone decides the service order. Four runs, config-only changes:
#   A  scheduler: fcfs                       blocker, L (long prompt), S1..S4 (short), 0.3 s apart
#   B  scheduler: spf_aging                  same workload           -> different order than A
#   C  scheduler: spf_aging (aging_alpha 5)  blocker, L, then a short request every 1 s (overload)
#   D  scheduler: spf_aging, aging_alpha 0   same overload, no aging -> contrast: L is served last
# Order and waits come from the gateway's [SCHED] log; times use its monotonic mono= field.
# Env: PYTHON (default python3), GW_PORT (default 8000), OVERWRITE=1 to replace evidence.
set -u
cd "$(dirname "$0")/.."
MODE="${1:-}"
PYTHON="${PYTHON:-python3}"
GW_PORT="${GW_PORT:-8000}"
case "$MODE" in
  real)    LABEL="REAL MODEL RESULT (llama.cpp container, real model)"
           OUT=report/evidence/phase3_scheduling_real.txt; BLOCK_T=16; GEN_T=4 ;;
  standin) LABEL="SIMULATED RESULT (stand-in replica container, no model; timings are NOT model performance)"
           OUT=report/evidence/simulated/phase3_scheduling_standin_SIMULATED.txt; BLOCK_T=80; GEN_T=40 ;;
  *) echo "usage: $0 real|standin"; exit 2 ;;
esac
if [ -e "$OUT" ] && [ "${OVERWRITE:-0}" != 1 ]; then echo "ABORT: $OUT exists (set OVERWRITE=1 to replace)"; exit 1; fi
mkdir -p "$(dirname "$OUT")"
WORK=$(mktemp -d); GW_PID=""
cleanup() { [ -n "$GW_PID" ] && kill "$GW_PID" 2>/dev/null; wait 2>/dev/null; rm -rf "$WORK"; }
trap cleanup EXIT
[ "$MODE" = standin ] && { docker build -q -t llmgw-standin:test -f tests/standin.Dockerfile tests >/dev/null || exit 1; }
PREFIX=$("$PYTHON" -c 'import yaml; print(yaml.safe_load(open("gateway/config.yaml"))["container_prefix"])')

LONG_PROMPT="Read this paragraph and then answer with one word only. The gateway keeps one central queue and sends a request to a replica only when that replica has a free slot, so the scheduler alone decides who goes next."   # ~210 chars
short_prompt() { echo "Short question $1: say hi."; }

{
  echo "# $LABEL"
  echo "# Phase 3 / demo 1: scheduling (fcfs vs spf_aging, aging vs no aging) through the gateway"
  echo "# Run: $(date -Is) | host: $(uname -sr) | dir: $PWD"
  echo "# 1 replica x 1 slot. Blocker max_tokens=$BLOCK_T, others max_tokens=$GEN_T. Times below are monotonic (log mono=)."
} > "$WORK/report.txt"

# run_scenario NAME SCHEDULER ALPHA N_SHORT INTERVAL
run_scenario() {
  local name=$1 sched=$2 alpha=$3 nshort=$4 interval=$5
  local cfg="$WORK/cfg_$name.yaml" log="$WORK/gw_$name.log"
  "$PYTHON" - "$cfg" "$MODE" "$sched" "$alpha" <<'EOF'
import sys, yaml
cfg = yaml.safe_load(open("gateway/config.yaml"))
cfg.update(min_replicas=1, slots_per_replica=1, router="least_busy", scheduler=sys.argv[3], aging_alpha=float(sys.argv[4]))
cfg.update(max_replicas=1, prewarm="none")   # fixed pool: the autoscaler (Phase 6) must not add a replica here
if sys.argv[2] == "standin":
    cfg["replica_image"] = "llmgw-standin:test"
yaml.safe_dump(cfg, open(sys.argv[1], "w"))
EOF
  echo "== [$name] scheduler=$sched aging_alpha=$alpha: starting gateway"
  LLMGW_CONFIG="$cfg" LLMGW_LOG_FILE="$log" "$PYTHON" -m uvicorn gateway.main:app --port "$GW_PORT" \
    --log-level warning >"$WORK/uv_$name.out" 2>&1 &
  GW_PID=$!
  for _ in $(seq 1 240); do grep -q '\[HEALTH\] replica ready' "$log" 2>/dev/null && break; sleep 0.5; done
  send() {  # send LABEL PROMPT MAX_TOKENS
    curl -sN "http://127.0.0.1:$GW_PORT/v1/chat/completions" -H 'Content-Type: application/json' \
      -d "{\"messages\":[{\"role\":\"user\",\"content\":\"$2\"}],\"max_tokens\":$3,\"stream\":true}" > "$WORK/resp_${name}_$1.txt" &
  }
  local labels="blocker L"
  send blocker "Count from one to fifty." "$BLOCK_T"; sleep 0.3
  send L "$LONG_PROMPT" "$GEN_T"; sleep "$interval"
  for i in $(seq 1 "$nshort"); do send "S$i" "$(short_prompt "$i")" "$GEN_T"; labels="$labels S$i"; sleep "$interval"; done
  echo "== [$name] waiting for all requests"
  wait $(jobs -p | grep -v "^$GW_PID$") 2>/dev/null
  sleep 0.3
  kill -TERM "$GW_PID"; wait "$GW_PID" 2>/dev/null; GW_PID=""
  local ok=0 n=0
  for l in $labels; do n=$((n + 1)); tail -c 100 "$WORK/resp_${name}_$l.txt" | grep -q 'data: \[DONE\]' \
    && ! grep -q '"error"' "$WORK/resp_${name}_$l.txt" && ok=$((ok + 1)); done
  local left; left=$(docker ps -a --format '{{.Names}}' | grep -c "^$PREFIX")
  "$PYTHON" "$WORK/analyze.py" "$log" "$name" "$alpha" "$labels" "$ok" "$n" "$left" >> "$WORK/report.txt"
}

cat > "$WORK/analyze.py" <<'EOF'
import re, sys
log, name, alpha, labels, ok, n, left = sys.argv[1], sys.argv[2], float(sys.argv[3]), sys.argv[4].split(), int(sys.argv[5]), int(sys.argv[6]), int(sys.argv[7])
text = open(log).read()
queued = re.findall(r"mono=([\d.]+) .*\[SCHED\] queued req=(\d+) prompt_tokens=(\d+)", text)
sel = re.findall(r"mono=([\d.]+) .*\[SCHED\] selected req=(\d+) policy=(\w+) prompt_tokens=\d+ wait_s=([\d.]+)(?: priority=(-?[\d.]+))?", text)
lab = {rid: labels[i] for i, (_, rid, _) in enumerate(queued)}
arr = {rid: float(m) for m, rid, _ in queued}
ptk = {rid: int(p) for _, rid, p in queued}
t0 = min(arr.values())
order = [lab[rid] for _, rid, _, _, _ in sel]
print(f"\n################ run {name} ################")
print(f"policy={sel[0][2]} aging_alpha={alpha}")
print("== service order (from [SCHED] selected) ==")
print("  " + " -> ".join(order))
print("== per request ==")
print(f"  {'req':>4} {'label':>7} {'prompt_tok':>10} {'arrived_s':>9} {'selected_s':>10} {'wait_s':>7} {'priority':>9}")
for m, rid, _, wait, prio in sel:
    print(f"  {rid:>4} {lab[rid]:>7} {ptk[rid]:>10} {arr[rid]-t0:>9.2f} {float(m)-t0:>10.2f} {float(wait):>7.2f} {prio or '-':>9}")
print("== [SCHED] / [ROUTE] log ==")
for line in text.splitlines():
    if "[SCHED]" in line or "[ROUTE]" in line:
        print("  " + line)
q = [l for l in labels if l not in ("blocker",)]
served = [o for o in order if o != "blocker"]
L = [rid for rid in lab if lab[rid] == "L"][0]
shorts = [rid for rid in lab if lab[rid].startswith("S")]
print("== checks ==")
def check(desc, cond):
    print(("PASS: " if cond else "FAIL: ") + desc)
check(f"all {n} clients got [DONE] and no error ({ok}/{n})", ok == n)
check("every request was selected exactly once", sorted(order) == sorted(labels))
check("replica container removed after gateway exit", left == 0)
if name == "A_fcfs":
    check("fcfs: service order equals arrival order", served == q)
if name == "B_spf":
    check("spf_aging: all short requests served before the long one", served[-1] == "L")
if name in ("C_aging", "D_no_aging"):
    sel_t = {rid: float(m) for m, rid, *_ in sel}
    bound = (ptk[L] - min(ptk[s] for s in shorts)) / alpha if alpha > 0 else float("inf")
    before = [s for s in shorts if sel_t[s] < sel_t[L]]
    after = [s for s in shorts if sel_t[s] > sel_t[L]]
    waitL = sel_t[L] - arr[L]
    print(f"   L: prompt_tokens={ptk[L]}, wait={waitL:.2f} s; crossover bound (p_L - p_S)/alpha = {bound:.2f} s")
    print(f"   shorts served before L: {len(before)}, after L: {len(after)}")
    if name == "C_aging":
        check("aging: L was served while short requests were still waiting", len(after) > 0)
        check("aging: every short served before L arrived within the bound after L",
              all(arr[s] - arr[L] <= bound for s in before))
    else:
        check("no aging: L served only after every short request (starved while shorts kept coming)", len(after) == 0)
EOF

run_scenario A_fcfs     fcfs      5.0 4  0.3
run_scenario B_spf      spf_aging 5.0 4  0.3
run_scenario C_aging    spf_aging 5.0 16 1.0
run_scenario D_no_aging spf_aging 0.0 16 1.0

cp "$WORK/report.txt" "$OUT"
cat "$OUT"
echo "== saved: $OUT"
! grep -q '^FAIL' "$OUT"
