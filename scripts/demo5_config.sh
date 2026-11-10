#!/usr/bin/env bash
# Demo 5 / Phase 8: configuration switching. "Scheduler/router/prewarm changed via config + reload,
# no restart or code edit, behaviour visibly changes."
#
#   scripts/demo5_config.sh real      # llama.cpp + real model        -> report/evidence/
#   scripts/demo5_config.sh standin   # stand-in, NO model (SIMULATED) -> report/evidence/simulated/
#
# ONE gateway process for the whole demo. Its config file (a copy of gateway/config.yaml with
# 2 replicas x 1 slot so queueing and placement are easy to see) is edited between stages and
# applied with POST /admin/reload:
#   stage A   scheduler: fcfs       router: round_robin       prewarm: none
#   reload 1  scheduler: spf_aging  router: session_affinity  prewarm: warm_pool
#   stage B   same workload as A
#   reload 2  invalid file (router: fastest)  -> must be rejected, old values stay
#   reload 3  prewarm: none                    -> the spare is stopped
# Workload per stage (loadgen/burst.py): "B,B,L,S,S,S,S" burst (2 busy requests take both slots,
# then a long and 4 short ones queue), a 4-turn session, then an idle wait for the warm pool.
# Env: PYTHON (default python3), GW_PORT (default 8000), OVERWRITE=1 to replace evidence.
set -u
cd "$(dirname "$0")/.."
MODE="${1:-}"
PYTHON="${PYTHON:-python3}"
GW_PORT="${GW_PORT:-8000}"
case "$MODE" in
  real)    LABEL="REAL MODEL RESULT (llama.cpp containers, real model)"
           OUT=report/evidence/phase8_config_real.txt; BUSY_T=24; GEN_T=4; TURN_T=8 ;;
  standin) LABEL="SIMULATED RESULT (stand-in replica containers, no model; timings are NOT model performance)"
           OUT=report/evidence/simulated/phase8_config_standin_SIMULATED.txt; BUSY_T=60; GEN_T=4; TURN_T=4 ;;
  *) echo "usage: $0 real|standin"; exit 2 ;;
esac
if [ -e "$OUT" ] && [ "${OVERWRITE:-0}" != 1 ]; then echo "ABORT: $OUT exists (set OVERWRITE=1 to replace)"; exit 1; fi
mkdir -p "$(dirname "$OUT")"
WORK=$(mktemp -d); GW_PID=""
cleanup() { [ -n "$GW_PID" ] && kill "$GW_PID" 2>/dev/null; wait 2>/dev/null; rm -rf "$WORK"; }
trap cleanup EXIT
[ "$MODE" = standin ] && { docker build -q -t llmgw-standin:test -f tests/standin.Dockerfile tests >/dev/null || exit 1; }
CFG="$WORK/config.yaml"; LOG="$WORK/gw.log"; URL="http://127.0.0.1:$GW_PORT"
PREFIX=$("$PYTHON" -c 'import yaml; print(yaml.safe_load(open("gateway/config.yaml"))["container_prefix"])')

set_config() {  # set_config key=value ...   (edits the file the gateway reads; values are YAML)
  "$PYTHON" - "$CFG" "$@" <<'EOF'
import sys, yaml
path, pairs = sys.argv[1], sys.argv[2:]
cfg = yaml.safe_load(open(path))
for p in pairs:
    k, v = p.split("=", 1)
    cfg[k] = yaml.safe_load(v)
yaml.safe_dump(cfg, open(path, "w"))
EOF
}
reload() {  # reload NAME : POST /admin/reload, keep status + body
  curl -s -o "$WORK/reload_$1.json" -w '%{http_code}' -X POST "$URL/admin/reload" > "$WORK/reload_$1.code"
}
stage() {  # stage NAME : the same workload every time
  "$PYTHON" loadgen/burst.py --url "$URL" --pattern "B,B,L,S,S,S,S" --interval 0.3 --max-tokens "$GEN_T" \
    --busy-max-tokens "$BUSY_T" --out "$WORK/burst_$1.jsonl" >/dev/null
  "$PYTHON" loadgen/burst.py --url "$URL" --session "sess-$1" --pattern "S,S,S,S" --max-tokens "$TURN_T" \
    --out "$WORK/session_$1.jsonl" >/dev/null
}

cp gateway/config.yaml "$CFG"
[ "$MODE" = standin ] && set_config replica_image=llmgw-standin:test
set_config min_replicas=2 max_replicas=3 slots_per_replica=1 scheduler=fcfs router=round_robin prewarm=none

echo "== starting ONE gateway"
LLMGW_CONFIG="$CFG" LLMGW_LOG_FILE="$LOG" "$PYTHON" -m uvicorn gateway.main:app --port "$GW_PORT" \
  --log-level warning >"$WORK/uv.out" 2>&1 &
GW_PID=$!
for _ in $(seq 1 240); do R=$(grep -c '\[HEALTH\] replica ready' "$LOG" 2>/dev/null); [ "${R:-0}" -ge 2 ] && break; sleep 0.5; done
echo "$GW_PID" > "$WORK/pid_start"

echo "== stage A (fcfs, round_robin, prewarm none)"
stage A
sleep 8                                   # idle: would a spare start?
grep -c '\[SCALE\] prewarm spare' "$LOG" > "$WORK/spares_A"

echo "== reload 1: spf_aging, session_affinity, warm_pool"
set_config scheduler=spf_aging router=session_affinity prewarm=warm_pool
reload 1
echo "== stage B"
stage B
for _ in $(seq 1 60); do grep -q 'state=warm' "$LOG" && break; sleep 0.5; done
sleep 1

echo "== reload 2: invalid router (must be rejected)"
set_config router=fastest
reload 2
"$PYTHON" loadgen/burst.py --url "$URL" --session "sess-B" --pattern "S" --max-tokens "$TURN_T" \
  --out "$WORK/session_after_invalid.jsonl" >/dev/null

echo "== reload 3: prewarm none (router fixed back)"
set_config router=session_affinity prewarm=none
reload 3
for _ in $(seq 1 30); do grep -q '\[SCALE\] replica stopped' "$LOG" && break; sleep 0.5; done
sleep 1
ps -p "$GW_PID" -o pid= > "$WORK/pid_end" 2>/dev/null
kill -TERM "$GW_PID"; wait "$GW_PID" 2>/dev/null; GW_PID=""
docker ps -a --format '{{.Names}}' | grep -c "^$PREFIX" > "$WORK/left"

{
  echo "# $LABEL"
  echo "# Phase 8 / demo 5: scheduler, router and prewarm switched by editing the config file + POST /admin/reload"
  echo "# Run: $(date -Is) | host: $(uname -sr) | dir: $PWD"
  echo "# One gateway process; 2 replicas x 1 slot, max_replicas 3. Burst B,B,L,S,S,S,S (B max_tokens=$BUSY_T, others $GEN_T), 4-turn session (max_tokens=$TURN_T)."
  "$PYTHON" - "$WORK" <<'EOF'
import json, re, sys
w = sys.argv[1]
log = open(f"{w}/gw.log").read().splitlines()
mono = lambda l: float(re.search(r"mono=([\d.]+)", l).group(1))
t0 = mono(log[0])
def jl(name):
    return [json.loads(l) for l in open(f"{w}/{name}").read().splitlines() if '"summary"' not in l]
def body(n):
    return open(f"{w}/reload_{n}.json").read().strip(), open(f"{w}/reload_{n}.code").read().strip()
selected = [m.group(1) for l in log if (m := re.search(r"\[SCHED\] selected req=(\d+)", l))]
dispatch = {m.group(1): (m.group(2), m.group(3)) for l in log
            if (m := re.search(r"\[ROUTE\] dispatch req=(\d+) replica=(r\d+) policy=(\w+)", l))}
def order(stage):
    recs = jl(f"burst_{stage}.jsonl")
    kind = {r["gateway_req"]: (r["kind"] + (str(sum(1 for x in recs[:i] if x["kind"] == r["kind"]) + 1)))
            for i, r in enumerate(recs)}
    return [kind[s] for s in selected if s in kind], recs
def placement(name):
    recs = jl(name)
    return [dispatch[r["gateway_req"]][0] for r in recs], [dispatch[r["gateway_req"]][1] for r in recs], recs

print("\n== gateway log: CONFIG lines and warm-pool events (s after gateway start) ==")
for l in log:
    if re.search(r"\[CONFIG\]|prewarm spare|spare stopped|replica stopped|state=warm|\[SCALE\] replica started", l):
        print(f"  {mono(l) - t0:7.1f}  " + l.split(" INFO gateway ")[1])
res = {}
for st in ("A", "B"):
    o, brecs = order(st)
    reps, pols, srecs = placement(f"session_{st}.jsonl")
    res[st] = (o, reps, pols, brecs, srecs)
    print(f"\n################ stage {st} ################")
    print(f"  burst service order (after the two B requests): {' -> '.join(x for x in o if not x.startswith('B'))}")
    pol = sorted({re.search(r"policy=(\w+)", l).group(1) for l in log if (m := re.search(r"\[SCHED\] selected req=(\d+)", l)) and m.group(1) in {r['gateway_req'] for r in brecs}})
    print(f"  scheduler policy in [SCHED] lines: {pol}")
    print(f"  session turns -> replicas: {reps}  (router policy in [ROUTE] lines: {sorted(set(pols))})")
    print(f"  burst: {sum(r['ok'] for r in brecs)}/{len(brecs)} ok; session: {sum(r['ok'] for r in srecs)}/{len(srecs)} ok")
for n in (1, 2, 3):
    b, c = body(n)
    print(f"\n== reload {n}: HTTP {c}\n  {b}")
after = jl("session_after_invalid.jsonl")
a_rep, a_pol = dispatch[after[0]["gateway_req"]]
pid0, pid1 = open(f"{w}/pid_start").read().strip(), open(f"{w}/pid_end").read().strip()
spares_A = int(open(f"{w}/spares_A").read())
left = int(open(f"{w}/left").read())
started = [l for l in log if "[SCALE] replica started" in l and "spare=False" in l]
print("\n== checks ==")
def check(desc, cond):
    print(("PASS: " if cond else "FAIL: ") + desc)
oA, oB = res["A"][0], res["B"][0]
qA, qB = [x for x in oA if not x.startswith("B")], [x for x in oB if not x.startswith("B")]
check("same gateway process for the whole demo (pid %s)" % pid0, pid0 and pid0 == pid1)
check("no restart: the 2 initial replicas were started once and never again", len(started) == 2)
check("all requests in both stages completed", all(r["ok"] for st in "AB" for r in res[st][3] + res[st][4]))
check("reload 1 accepted (HTTP 200) and changed scheduler, router and prewarm",
      body(1)[1] == "200" and all(k in json.loads(body(1)[0])["changed"] for k in ("scheduler", "router", "prewarm")))
check(f"scheduler: stage A fcfs served L first ({qA[0]}), stage B spf_aging served L last ({qB[-1]})",
      qA[:1] == ["L1"] and qB[-1:] == ["L1"])
check(f"router: stage A round_robin alternated {res['A'][1]}, stage B session_affinity kept one replica {res['B'][1]}",
      len(set(res["A"][1])) == 2 and len(set(res["B"][1])) == 1)
check(f"prewarm: no spare under none (stage A: {spares_A}), a spare loaded after switching to warm_pool",
      spares_A == 0 and any("state=warm" in l for l in log))
check("reload 2 (invalid router) rejected with HTTP 400 and logged",
      body(2)[1] == "400" and any("[CONFIG] reload rejected" in l for l in log))
check(f"after the rejected reload the old values stayed (policy={a_pol}, replica {a_rep} = stage B's)",
      a_pol == "session_affinity" and a_rep == res["B"][1][0])
check("reload 3 (prewarm none) accepted and the spare was stopped",
      body(3)[1] == "200" and any("spare stopped" in l for l in log) and any("[SCALE] replica stopped" in l for l in log))
check("all replica containers removed after gateway exit", left == 0)
EOF
} > "$WORK/report.txt"

cp "$WORK/report.txt" "$OUT"
cat "$OUT"
echo "== saved: $OUT"
! grep -q '^FAIL' "$OUT"
