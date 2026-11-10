#!/usr/bin/env bash
# Demo 4 / Phase 5 check: fault tolerance.
#
#   scripts/demo4_fault.sh real      # llama.cpp + real model        -> report/evidence/
#   scripts/demo4_fault.sh standin   # stand-in, NO model (SIMULATED) -> report/evidence/simulated/
#
# 2 replicas x 2 slots, least_busy, fcfs. 4 clients stream at once (2 per replica). While they are
# mid-stream, replica r1 fails:
#   run "kill"   docker kill  (container dies: stream connection error)
#   run "pause"  docker pause (container frozen: streams hang, only the health probes notice)
# Checked: HEALTH detection, every client still gets a complete answer, the replacement reaches READY,
# and assumption #6 (greedy decoding regenerates the same tokens): the gateway compares the
# regenerated prefix with what the client already has ("retry resumed ... prefix_matched=yes" or
# "retry output differs"), and each client's final text is compared with a fault-free reference
# run of the same 4 prompts (same concurrency).
# Times come from the gateway log's monotonic mono= field.
# Env: PYTHON (default python3), GW_PORT (default 8000), OVERWRITE=1 to replace evidence.
set -u
cd "$(dirname "$0")/.."
MODE="${1:-}"
PYTHON="${PYTHON:-python3}"
GW_PORT="${GW_PORT:-8000}"
case "$MODE" in
  real)    LABEL="REAL MODEL RESULT (llama.cpp containers, real model)"
           OUT=report/evidence/phase5_fault_real.txt; MAXT=48; FAULT_AT=15 ;;   # late enough that tokens were sent
  standin) LABEL="SIMULATED RESULT (stand-in replica containers, no model; timings are NOT model performance)"
           OUT=report/evidence/simulated/phase5_fault_standin_SIMULATED.txt; MAXT=80; FAULT_AT=1.5 ;;
  *) echo "usage: $0 real|standin"; exit 2 ;;
esac
if [ -e "$OUT" ] && [ "${OVERWRITE:-0}" != 1 ]; then echo "ABORT: $OUT exists (set OVERWRITE=1 to replace)"; exit 1; fi
mkdir -p "$(dirname "$OUT")"
WORK=$(mktemp -d); GW_PID=""
cleanup() { [ -n "$GW_PID" ] && kill "$GW_PID" 2>/dev/null; wait 2>/dev/null; rm -rf "$WORK"; }
trap cleanup EXIT
[ "$MODE" = standin ] && { docker build -q -t llmgw-standin:test -f tests/standin.Dockerfile tests >/dev/null || exit 1; }
PREFIX=$("$PYTHON" -c 'import yaml; print(yaml.safe_load(open("gateway/config.yaml"))["container_prefix"])')
PROMPTS=("Describe a sunrise over the sea in a few sentences." "Explain why the sky is blue to a child."
         "Write a short note about keeping plants healthy." "List some tips for learning to cook.")

cat > "$WORK/analyze.py" <<'EOF'
import json, re, sys
work, fault, left, maxt, mode = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), sys.argv[5]
log = open(f"{work}/gw_{fault}.log").read()
fault_mono = float(open(f"{work}/fault_mono_{fault}.txt").read())

def parse(path):
    raw = open(path).read()
    objs = [json.loads(l[6:]) for l in raw.splitlines() if l.startswith("data: ") and l[6:] != "[DONE]"]
    final, retry_events = "", 0
    for o in objs:
        if "llmgw_retry" in o:
            retry_events += 1
            final = ""                                   # client discards what it had
        for c in o.get("choices") or []:
            final += (c.get("delta") or {}).get("content") or ""
    ok = raw.rstrip().endswith("data: [DONE]") and not any("error" in o for o in objs)
    return final, retry_events, ok

def mono(pattern):
    m = re.search(r"mono=([\d.]+) .*" + pattern, log)
    return float(m.group(1)) if m else None

queued = re.findall(r"\[SCHED\] queued req=(\d+)", log)
client_of = {rid: i for i, rid in enumerate(queued)}
disp = {}
for rid, rep in re.findall(r"\[ROUTE\] dispatch req=(\d+) replica=(r\d+)", log):
    disp.setdefault(rid, []).append(rep)
resumed = dict(re.findall(r"\[ROUTE\] retry resumed req=(\d+) replica=r\d+ skipped_tokens=(\d+)", log))
differs = re.findall(r"\[ROUTE\] retry output differs, client told to restart req=(\d+)", log)
retried = re.findall(r"\[ROUTE\] stream failed, retrying req=(\d+)", log)
dead = re.search(r"\[HEALTH\] replica DEAD replica=r1 reason=(\w+)", log)
expect = "stream_error" if fault == "kill" else "health_checks"
t_dead, t_ready = mono(r"\[HEALTH\] replica DEAD replica=r1"), mono(r"\[HEALTH\] replica ready replica=r3")
ready_line = re.search(r"\[HEALTH\] replica ready replica=r3 .*load_s=([\d.]+)", log)

print(f"\n################ run {fault} (docker {fault} r1 while busy) ################")
print("== timeline (monotonic, seconds after the fault) ==")
print(f"  r1 declared DEAD:       {t_dead - fault_mono:+.2f} s  (reason={dead.group(1) if dead else '-'})" if t_dead else "  r1 declared DEAD: never")
print(f"  replacement r3 READY:   {t_ready - fault_mono:+.2f} s  (its own load time {ready_line.group(1)} s)" if t_ready else "  replacement r3 READY: never")
print("== per client ==")
print(f"  {'client':>6} {'req':>4} {'replicas':>10} {'resumed_after':>13} {'restarted':>9} {'tokens':>6} {'same_as_reference':>17}")
rows = []
for i in range(4):
    rid = queued[i]
    final, nretry, ok = parse(f"{work}/resp_{fault}_{i}.txt")
    ref, _, _ = parse(f"{work}/resp_reference_{i}.txt")
    rows.append((i, rid, final, nretry, ok, final == ref))
    print(f"  {i:>6} {rid:>4} {'>'.join(disp.get(rid, [])):>10} {resumed.get(rid, '-'):>13} {('yes' if nretry else 'no'):>9} "
          f"{len(final.split()):>6} {('yes' if final == ref else 'NO'):>17}")
print("== gateway log: HEALTH / SCALE / ROUTE (retry, fail, complete) ==")
for line in log.splitlines():
    if re.search(r"\[(HEALTH|SCALE)\]|\[ROUTE\] (retry|stream failed|complete|dispatch.*attempt)", line):
        print("  " + line)
print("== checks ==")
def check(desc, cond):
    print(("PASS: " if cond else "FAIL: ") + desc)
check(f"HEALTH declared r1 DEAD (reason={expect})", bool(dead) and dead.group(1) == expect)
check("every client got [DONE], no error, non-empty output", all(ok and final for _, _, final, _, ok, _ in rows))
check(f"the requests that were on r1 were retried on another replica ({len(set(retried))} retried)",
      len(set(retried)) >= 1 and all(len(disp[r]) >= 2 and disp[r][-1] != "r1" for r in set(retried)))
check("replacement r3 was started and reached READY", t_ready is not None)
check("all replica containers removed after gateway exit", left == 0)
if mode == "standin":
    sim = "".join(f"sim{i} " for i in range(maxt))
    check("stand-in: every client got exactly sim0..sim{n-1} once (seamless resume)",
          all(final == sim for _, _, final, _, _, _ in rows))
print("== assumption #6 (greedy decoding regenerates the same tokens) ==")
print(f"  gateway prefix check: matched for {len(resumed)} retried request(s), differed for {len(differs)}")
print(f"  final text equals the fault-free reference for {sum(r[5] for r in rows)}/4 clients")
EOF

{
  echo "# $LABEL"
  echo "# Phase 5 / demo 4: replica failure while busy (docker kill, docker pause), retry and replacement"
  echo "# Run: $(date -Is) | host: $(uname -sr) | dir: $PWD"
  echo "# 2 replicas x 2 slots, 4 concurrent clients, max_tokens=$MAXT, fault injected ${FAULT_AT}s after the clients start."
} > "$WORK/report.txt"

start_gw() {  # start_gw NAME   (sets GW_PID; runs in the current shell)
  "$PYTHON" - "$WORK/cfg.yaml" "$MODE" >> "$WORK/report.txt" <<'EOF'
import sys, yaml
cfg = yaml.safe_load(open("gateway/config.yaml"))
cfg.update(min_replicas=2, router="least_busy", scheduler="fcfs")
if sys.argv[2] == "standin":
    cfg["replica_image"] = "llmgw-standin:test"
yaml.safe_dump(cfg, open(sys.argv[1], "w"))
EOF
  LLMGW_CONFIG="$WORK/cfg.yaml" LLMGW_LOG_FILE="$WORK/gw_$1.log" "$PYTHON" -m uvicorn gateway.main:app \
    --port "$GW_PORT" --log-level warning >"$WORK/uv_$1.out" 2>&1 &
  GW_PID=$!
  for _ in $(seq 1 240); do
    R=$(grep -c '\[HEALTH\] replica ready' "$WORK/gw_$1.log" 2>/dev/null); [ "${R:-0}" -ge 2 ] && break; sleep 0.5
  done
}
stop_gw() { kill -TERM "$GW_PID"; wait "$GW_PID" 2>/dev/null; GW_PID=""; }
CLIENT_PIDS=()
clients() {  # clients NAME   (4 concurrent streams in the background; PIDs in CLIENT_PIDS)
  CLIENT_PIDS=()
  for i in 0 1 2 3; do
    curl -sN "http://127.0.0.1:$GW_PORT/v1/chat/completions" -H 'Content-Type: application/json' \
      -d "{\"messages\":[{\"role\":\"user\",\"content\":\"${PROMPTS[$i]}\"}],\"max_tokens\":$MAXT,\"stream\":true}" \
      > "$WORK/resp_$1_$i.txt" &
    CLIENT_PIDS+=($!); sleep 0.1
  done
}

"$PYTHON" -c "import yaml; c = yaml.safe_load(open('gateway/config.yaml')); print('# config: health_interval_s=%s health_fail_limit=%s max_retries=%s' % (c['health_interval_s'], c['health_fail_limit'], c['max_retries']))" >> "$WORK/report.txt"

echo "== [reference] no fault, same 4 prompts"
start_gw reference
clients reference; wait "${CLIENT_PIDS[@]}"
stop_gw

for FAULT in kill pause; do
  echo "== [$FAULT] 4 clients, then docker $FAULT ${PREFIX}r1 after ${FAULT_AT}s"
  start_gw "$FAULT"
  clients "$FAULT"
  sleep "$FAULT_AT"
  "$PYTHON" -c 'import time; print(time.monotonic())' > "$WORK/fault_mono_$FAULT.txt"
  docker "$FAULT" "${PREFIX}r1" >/dev/null
  wait "${CLIENT_PIDS[@]}"
  echo "== [$FAULT] clients done; waiting for the replacement to be READY"
  for _ in $(seq 1 240); do grep -q '\[HEALTH\] replica ready replica=r3' "$WORK/gw_$FAULT.log" && break; sleep 0.5; done
  stop_gw
  LEFT=$(docker ps -a --format '{{.Names}}' | grep -c "^$PREFIX")
  "$PYTHON" "$WORK/analyze.py" "$WORK" "$FAULT" "$LEFT" "$MAXT" "$MODE" >> "$WORK/report.txt"
done

cp "$WORK/report.txt" "$OUT"
cat "$OUT"
echo "== saved: $OUT"
! grep -q '^FAIL' "$OUT"
