#!/usr/bin/env bash
# Demo 2 / Phase 4 check: session affinity.
#
#   scripts/demo2_affinity.sh real      # llama.cpp + real model        -> report/evidence/
#   scripts/demo2_affinity.sh standin   # stand-in, NO model (SIMULATED) -> report/evidence/simulated/
#
# 2 replicas. Two runs that differ only in config:
#   affinity  router: session_affinity
#   baseline  router: round_robin   (consecutive turns go to different replicas)
# Each run: 2 concurrent warm-up requests (one per replica), then 3 conversations of 5 turns,
# sent one after another with a "session_id"; the client sends only the new message each turn.
# Per turn: replica and affinity decision ([ROUTE] dispatch), TTFT measured by the gateway with
# the monotonic clock ([ROUTE] complete ttft_s), and, for the real model, llama-server's own
# timings from the last chunk (cache_n = prompt tokens reused from the cache, prompt_n = new ones).
# Env: PYTHON (default python3), GW_PORT (default 8000), OVERWRITE=1 to replace evidence.
set -u
cd "$(dirname "$0")/.."
MODE="${1:-}"
PYTHON="${PYTHON:-python3}"
GW_PORT="${GW_PORT:-8000}"
case "$MODE" in
  real)    LABEL="REAL MODEL RESULT (llama.cpp containers, real model)"
           OUT=report/evidence/phase4_affinity_real.txt ;;
  standin) LABEL="SIMULATED RESULT (stand-in replica containers, no model; timings are NOT model performance)"
           OUT=report/evidence/simulated/phase4_affinity_standin_SIMULATED.txt ;;
  *) echo "usage: $0 real|standin"; exit 2 ;;
esac
if [ -e "$OUT" ] && [ "${OVERWRITE:-0}" != 1 ]; then echo "ABORT: $OUT exists (set OVERWRITE=1 to replace)"; exit 1; fi
mkdir -p "$(dirname "$OUT")"
WORK=$(mktemp -d); GW_PID=""
cleanup() { [ -n "$GW_PID" ] && kill "$GW_PID" 2>/dev/null; wait 2>/dev/null; rm -rf "$WORK"; }
trap cleanup EXIT
[ "$MODE" = standin ] && { docker build -q -t llmgw-standin:test -f tests/standin.Dockerfile tests >/dev/null || exit 1; }
PREFIX=$("$PYTHON" -c 'import yaml; print(yaml.safe_load(open("gateway/config.yaml"))["container_prefix"])')
MAXT=12
COLOURS="teal amber violet"
turn_text() {  # turn_text COLOUR TURN
  case $2 in
    1) echo "My favourite colour is $1. Remember it and reply only with OK." ;;
    2) echo "What is my favourite colour? Answer in one word." ;;
    3) echo "Name one fruit that has that colour. One word." ;;
    4) echo "Name one animal that has that colour. One word." ;;
    5) echo "Repeat my favourite colour once more. One word." ;;
  esac
}
post() {  # post OUTFILE MAX_TOKENS TEXT [SESSION]
  local sess=""; [ -n "${4:-}" ] && sess=",\"session_id\":\"$4\""
  curl -sN "http://127.0.0.1:$GW_PORT/v1/chat/completions" -H 'Content-Type: application/json' \
    -d "{\"messages\":[{\"role\":\"user\",\"content\":\"$3\"}],\"max_tokens\":$2,\"stream\":true$sess}" > "$1"
}

{
  echo "# $LABEL"
  echo "# Phase 4 / demo 2: session affinity vs round robin, 2 replicas, 3 conversations x 5 turns each"
  echo "# Run: $(date -Is) | host: $(uname -sr) | dir: $PWD"
  echo "# max_tokens per turn: $MAXT. TTFT = gateway-measured time to first token (monotonic)."
} > "$WORK/report.txt"

run() {  # run NAME ROUTER
  local name=$1 router=$2 cfg="$WORK/cfg_$1.yaml" log="$WORK/gw_$1.log"
  "$PYTHON" - "$cfg" "$MODE" "$router" <<'EOF'
import sys, yaml
cfg = yaml.safe_load(open("gateway/config.yaml"))
cfg.update(min_replicas=2, router=sys.argv[3])
if sys.argv[2] == "standin":
    cfg["replica_image"] = "llmgw-standin:test"
yaml.safe_dump(cfg, open(sys.argv[1], "w"))
EOF
  echo "== [$name] router=$router: starting gateway with 2 replicas"
  LLMGW_CONFIG="$cfg" LLMGW_LOG_FILE="$log" "$PYTHON" -m uvicorn gateway.main:app --port "$GW_PORT" \
    --log-level warning >"$WORK/uv_$name.out" 2>&1 &
  GW_PID=$!
  for _ in $(seq 1 240); do
    R=$(grep -c '\[HEALTH\] replica ready' "$log" 2>/dev/null); [ "${R:-0}" -ge 2 ] && break; sleep 0.5
  done
  echo "== [$name] warm-up (one request per replica)"
  post "$WORK/warm_${name}_1.txt" 4 "Say hi." & local w1=$!
  post "$WORK/warm_${name}_2.txt" 4 "Say hi." & local w2=$!
  wait "$w1" "$w2"
  for c in 1 2 3; do
    colour=$(echo $COLOURS | cut -d' ' -f$c)
    echo "== [$name] conversation c$c ($colour)"
    for t in 1 2 3 4 5; do post "$WORK/resp_${name}_c${c}_t${t}.txt" $MAXT "$(turn_text "$colour" $t)" "$name-c$c"; done
  done
  sleep 0.3
  kill -TERM "$GW_PID"; wait "$GW_PID" 2>/dev/null; GW_PID=""
  local left; left=$(docker ps -a --format '{{.Names}}' | grep -c "^$PREFIX")
  "$PYTHON" "$WORK/analyze.py" "$log" "$WORK" "$name" "$router" "$left" >> "$WORK/report.txt"
}

cat > "$WORK/analyze.py" <<'EOF'
import json, re, sys
log, work, name, router, left = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], int(sys.argv[5])
text = open(log).read()
disp = {m.group(1): m for m in re.finditer(r"\[ROUTE\] dispatch req=(\d+) replica=(r\d+) .*?session=(\S+)(?: affinity=(\S+))?", text)}
comp = {m.group(1): float(m.group(2)) for m in re.finditer(r"\[ROUTE\] complete req=(\d+) .*?ttft_s=([\d.]+)", text)}
by_session = {}
for rid, m in disp.items():
    by_session.setdefault(m.group(3), []).append((int(rid), m.group(2), m.group(4) or "-"))
def resp(c, t):
    raw = open(f"{work}/resp_{name}_c{c}_t{t}.txt").read()
    events = [l[6:] for l in raw.splitlines() if l.startswith("data: ") and l[6:] != "[DONE]"]
    objs = [json.loads(e) for e in events]
    txt = "".join((o.get("choices") or [{}])[0].get("delta", {}).get("content") or "" for o in objs if o.get("choices"))
    tim = next((o["timings"] for o in reversed(objs) if "timings" in o), None)
    nmsg = next((o["standin_prompt_messages"] for o in objs if "standin_prompt_messages" in o), None)
    ok = raw.rstrip().endswith("data: [DONE]") and '"error"' not in raw
    return txt, tim, nmsg, ok
print(f"\n################ run {name} (router: {router}) ################")
rows, all_ok, summary = [], True, {"turn2_ttft": [], "turn2_cache_n": []}
checks = []
for c in (1, 2, 3):
    sess = f"{name}-c{c}"
    turns = sorted(by_session.get(sess, []))
    print(f"== conversation {sess} ==")
    print(f"  {'turn':>4} {'req':>4} {'replica':>7} {'affinity':>14} {'ttft_s':>7} {'cache_n':>7} {'prompt_n':>8} {'msgs':>4}  reply")
    totals, msgs, reps = [], [], []
    for t, (rid, rep, dec) in enumerate(turns, 1):
        txt, tim, nmsg, ok = resp(c, t)
        all_ok &= ok
        cn = tim.get("cache_n") if tim else None
        pn = tim.get("prompt_n") if tim else None
        if tim: totals.append(cn + pn)
        if nmsg is not None: msgs.append(nmsg)
        reps.append(rep)
        ttft = comp.get(str(rid))
        if t == 2:
            summary["turn2_ttft"].append(ttft)
            summary["turn2_cache_n"].append(cn)
        print(f"  {t:>4} {rid:>4} {rep:>7} {dec:>14} {ttft:>7.3f} {str(cn if cn is not None else '-'):>7} "
              f"{str(pn if pn is not None else '-'):>8} {str(nmsg if nmsg is not None else '-'):>4}  {txt.strip()[:40]!r}")
    checks.append((f"{sess}: 5 turns dispatched", len(turns) == 5))
    if router == "session_affinity":
        checks.append((f"{sess}: all 5 turns on the same replica ({reps[0] if reps else '?'})", len(set(reps)) == 1 and len(reps) == 5))
        checks.append((f"{sess}: affinity decisions new, hit x4",
                       [d for _, _, d in turns] == ["new", "hit", "hit", "hit", "hit"]))
    else:
        checks.append((f"{sess}: turn 2 went to a different replica than turn 1", len(reps) >= 2 and reps[0] != reps[1]))
    if msgs:
        checks.append((f"{sess}: history replayed (stand-in got 1,3,5,7,9 messages)", msgs == [1, 3, 5, 7, 9]))
    if totals:
        checks.append((f"{sess}: history replayed (total prompt tokens grow every turn: {totals})",
                       all(b > a for a, b in zip(totals, totals[1:]))))
print("== checks ==")
checks.append(("all requests got [DONE] and no error", all_ok))
checks.append(("replica containers removed after gateway exit", left == 0))
for desc, ok in checks:
    print(("PASS: " if ok else "FAIL: ") + desc)
json.dump(summary, open(f"{work}/summary_{name}.json", "w"))
EOF

run affinity session_affinity
run baseline round_robin

"$PYTHON" - "$WORK" >> "$WORK/report.txt" <<'EOF'
import json, sys
w = sys.argv[1]
a, b = (json.load(open(f"{w}/summary_{n}.json")) for n in ("affinity", "baseline"))
mean = lambda xs: sum(xs) / len(xs)
print("\n################ turn-2 TTFT: with vs without affinity ################")
print(f"  {'':>22} {'c1':>7} {'c2':>7} {'c3':>7} {'mean':>7}   cache_n (turn 2)")
for label, s in (("session_affinity", a), ("round_robin (none)", b)):
    t = s["turn2_ttft"]
    print(f"  {label:>22} " + " ".join(f"{x:>7.3f}" for x in t) + f" {mean(t):>7.3f}   {s['turn2_cache_n']}")
print(f"  difference of means (round_robin - affinity): {mean(b['turn2_ttft']) - mean(a['turn2_ttft']):+.3f} s")
print("  (measured, not assumed: no PASS/FAIL on which is faster)")
EOF

cp "$WORK/report.txt" "$OUT"
cat "$OUT"
echo "== saved: $OUT"
! grep -q '^FAIL' "$OUT"
