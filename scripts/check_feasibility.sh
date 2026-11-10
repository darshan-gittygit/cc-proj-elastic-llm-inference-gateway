#!/usr/bin/env bash
# Phase 0 feasibility check. Run this ON THE MACHINE THAT WILL RUN THE DEMO.
# It measures the things the autoscaler thresholds depend on. Paste the full
# output back so the numbers in the report are real, not guessed.
#
# Usage: ./scripts/check_feasibility.sh
set -u
cd "$(dirname "$0")/.."
IMAGE="${IMAGE:-ghcr.io/ggml-org/llama.cpp:server}"
MODEL_FILE="${MODEL_FILE:-qwen2.5-0.5b-instruct-q4_k_m.gguf}"
MODEL_URL="https://huggingface.co/Qwen/Qwen2.5-0.5B-Instruct-GGUF/resolve/main/${MODEL_FILE}"
CPUS="${CPUS:-1.0}"; MEM="${MEM:-1g}"; SLOTS="${SLOTS:-2}"; CTX_PER_SLOT="${CTX_PER_SLOT:-2048}"
NAME=llmgw-feasibility
pass(){ echo "PASS: $*"; }; fail(){ echo "FAIL: $*"; }

echo "== host =="; echo "cores: $(nproc)"; free -h | sed -n 2p; uname -sr

echo "== docker =="
docker info >/dev/null 2>&1 && pass "docker daemon reachable" || { fail "docker daemon not reachable"; exit 1; }
docker compose version >/dev/null 2>&1 && pass "docker compose plugin present" || echo "NOTE: 'docker compose' plugin missing"
python3 -c "import docker" 2>/dev/null && pass "python docker SDK" || echo "NOTE: pip install docker"

echo "== model =="
mkdir -p models
if [ ! -f "models/${MODEL_FILE}" ]; then
  echo "downloading ${MODEL_FILE} (~400MB)"; curl -L --fail -o "models/${MODEL_FILE}" "$MODEL_URL" || { fail "model download"; exit 1; }
fi
ls -lh "models/${MODEL_FILE}"

echo "== image =="
docker pull "$IMAGE" >/dev/null 2>&1 && pass "pulled $IMAGE" || { fail "could not pull $IMAGE"; exit 1; }

echo "== start replica: --cpus $CPUS --memory $MEM, -np $SLOTS =="
docker rm -f $NAME >/dev/null 2>&1
T0=$(date +%s.%N)
docker run -d --name $NAME --cpus "$CPUS" --memory "$MEM" -p 18080:8080 \
  -v "$PWD/models:/models:ro" "$IMAGE" \
  -m "/models/${MODEL_FILE}" -c $((SLOTS*CTX_PER_SLOT)) -np "$SLOTS" --cont-batching \
  --host 0.0.0.0 --port 8080 >/dev/null || { fail "docker run"; exit 1; }
until curl -sf localhost:18080/health >/dev/null 2>&1; do
  sleep 0.5; [ "$(docker inspect -f '{{.State.Running}}' $NAME)" = "true" ] || { fail "container exited"; docker logs --tail 30 $NAME; exit 1; }
done
T1=$(date +%s.%N)
echo "MODEL LOAD TIME (container start -> /health ok): $(echo "$T1 - $T0" | bc) s"
docker inspect -f 'limits: NanoCpus={{.HostConfig.NanoCpus}} Memory={{.HostConfig.Memory}}' $NAME

echo "== single streamed request =="
curl -sN localhost:18080/v1/chat/completions -H 'Content-Type: application/json' \
  -d '{"messages":[{"role":"user","content":"Say hello in five words."}],"max_tokens":32,"stream":true,"temperature":0}' | head -c 600; echo

echo "== throughput: 1 request, 128 tokens =="
python3 - <<'EOF'
import json, time, urllib.request
def run(n=1, max_tokens=128):
    import threading
    res=[]
    def one():
        body=json.dumps({"messages":[{"role":"user","content":"Write a short story about a robot."}],
                         "max_tokens":max_tokens,"temperature":0,"stream":True}).encode()
        req=urllib.request.Request("http://localhost:18080/v1/chat/completions",body,{"Content-Type":"application/json"})
        t=time.time(); first=None; toks=0
        with urllib.request.urlopen(req) as r:
            for line in r:
                if line.startswith(b"data: {"):
                    toks+=1
                    if first is None: first=time.time()-t
        res.append((first,toks,time.time()-t))
    ths=[threading.Thread(target=one) for _ in range(n)]
    [t.start() for t in ths]; [t.join() for t in ths]
    return res
for n in (1,2,3):
    r=run(n); tot=sum(x[1] for x in r); wall=max(x[2] for x in r)
    print(f"concurrent={n}: ttft={[round(x[0],2) for x in r]}s  per-req tok/s={[round(x[1]/x[2],1) for x in r]}  aggregate tok/s={tot/wall:.1f}")
print("(concurrent=3 with 2 slots: the 3rd request should queue inside llama-server)")
EOF

echo "== slots endpoint (needed for session affinity checks) =="
curl -s localhost:18080/slots | head -c 400; echo
echo "== OOM / limits sanity =="
docker stats --no-stream --format 'cpu={{.CPUPerc}} mem={{.MemUsage}}' $NAME
docker rm -f $NAME >/dev/null
echo "== done: paste everything above =="
