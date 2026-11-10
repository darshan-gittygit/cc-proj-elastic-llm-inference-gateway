# Elastic LLM Inference Gateway - Architecture (frozen baseline)

Status: Phase 0. Architecture and interfaces are fixed here; later phases follow them.
Changes need a written reason in the "Change log" at the bottom.

## 1. Feasibility results (Phase 0)

Environment used for these checks: the Claude sandbox (2 cores, 7.8 GB RAM, Docker 29.9.2, cgroup v1).
That is a development environment only. **Final demo numbers must come from your own machine.**

| Check | Result | Evidence |
|---|---|---|
| Docker daemon can run and be driven by the Engine API (Python SDK) | VERIFIED | SDK started + stopped a limited container in 0.33 s |
| `--cpus` is enforced | VERIFIED | `--cpus 0.5` with 2 busy threads measured 49.6% and 50.2% CPU |
| `--memory` is enforced | VERIFIED | 400 MB allocation under `--memory 128m` -> exit 137, OOMKilled=true |
| `-np` parallel slots, `--cont-batching`, `/health`, `/slots`, streaming | NOT VERIFIED | needs llama.cpp image + model |
| Qwen2.5-0.5B-Instruct Q4_K_M GGUF loads under 1 CPU / 1 GB | NOT VERIFIED | model download blocked |
| Model load time, tokens/sec, effect of N replicas on one CPU pool | NOT VERIFIED | same |
| Docker Compose plugin | NOT PRESENT in sandbox | only needed for the static part of the setup |

Why not verified: in the sandbox, `huggingface.co`, `registry-1.docker.io` and `ghcr.io` are refused by the
network egress policy (HTTP 403 on CONNECT). GitHub and PyPI work. I did not try mirrors or other
workarounds. The sandbox also has no local base images, so the replica image cannot be built here.

`scripts/check_feasibility.sh` runs all the NOT VERIFIED items on your machine and prints model load
time, per-request and aggregate tokens/sec at 1, 2 and 3 concurrent requests, and the `/slots` output.
Those numbers set the autoscaler thresholds, so run it before Phase 1.

## 2. Final architecture

One Python process (FastAPI + asyncio) plus N replica containers. No database, no broker, no extra services.

```
curl --(SSE stream)--> FastAPI /v1/chat/completions
                         |  creates Request, pushes to Scheduler, awaits Request.out
                         v
   Scheduler (FCFS | SPF+aging)   <-- single central queue
                         |
   Dispatch loop (asyncio task): while queue non-empty AND some replica is routable:
        req = scheduler.pop();  replica = router.choose(req, pool);  start stream task
                         |
   Router (round_robin | least_busy | session_affinity)  -- pure decision, no I/O
                         |
   ReplicaPool (shared registry) <---- ReplicaManager (Docker Engine API)
        ^                                   ^
   HealthMonitor (probe loop)          Autoscaler (control loop, warm pool)
```

Key decisions:

1. **Central queue, replicas never queue.** A request is dispatched only when a replica has a free slot
   (capacity per replica = `slots_per_replica`, which equals llama-server `-np`). This is what makes SPF
   real; otherwise llama-server would reorder nothing and just FCFS internally.
2. **Streaming path:** the gateway posts to the replica's `/v1/chat/completions` with `stream=true`, and
   pushes each chunk into `Request.out`; the HTTP handler yields from that queue. `None` marks the end.
3. **Prompt length** is estimated at ingress as `len(text)/4` (documented approximation). SPF only needs
   relative order, so a tokenizer call is not justified.
4. **SPF with aging:** priority = `prompt_tokens - aging_alpha * wait_seconds`, lowest first. A long prompt's
   priority falls linearly with waiting, so it is guaranteed to reach the front. Bound:
   `wait <= (long_len - short_len) / alpha`. The demo will log each request's wait to show this.
5. **Session affinity:** gateway keeps `session_id -> replica_id` and the full conversation history per session
   (clients send only the new message). If the mapped replica is dead/draining/full, fall back to least busy
   and re-map; history is replayed, so correctness does not depend on the old replica's cache. Speed benefit
   comes from llama-server prompt-cache reuse on the same replica (to be measured, not assumed).
6. **Retry policy (simple, documented):** if a stream fails mid-way, the request goes back to the *front* of
   the queue and is restarted on another replica. Because `default_temperature = 0` (greedy), the restarted
   generation should reproduce the same tokens, so the gateway skips the first `tokens_emitted` tokens and
   continues the client's stream. This assumption (same model + greedy = same output) **must be checked
   in Phase 5**; if it fails, the fallback is to send the client a `retry` event and restart its output.
   Bounded by `max_retries`.
7. **Autoscaling:** signal = `(queued + in_flight) / ready_replicas`, smoothed. Up when above
   `scale_up_threshold` for `scale_up_after_s`; down when below `scale_down_threshold` for
   `scale_down_cooldown_s`. Scale-down = mark DRAINING, wait for `in_flight == 0`, then stop.
   Up/down thresholds differ (hysteresis) to prevent flapping.
8. **Pre-warming (two modes, switchable by config):**
   - `none`: start a container only when the scale-up condition triggers; users wait for the model to load.
   - `warm_pool`: keep `warm_pool_size` replicas loaded but not routable (`WARM_SPARE`). Scale-up promotes
     a spare instantly; a new spare is then started in the background. A spare is started when the
     signal reaches `prewarm_trigger_ratio * scale_up_threshold`, or at idle if capacity allows.
   - **Default: `warm_pool` with 1 spare**, because model load is the dominant scale-up delay (to be
     quantified in Phase 7) and one spare is the cheapest way to hide it on a CPU-limited single host.
     Final justification in the report uses the measured numbers.
9. **Fault tolerance:** HealthMonitor probes `/health` every `health_interval_s`; `health_fail_limit`
   consecutive failures (or a stream connection error) => DEAD. The dispatch loop's `on_dead` callback requeues
   that replica's in-flight requests; the manager removes the container and starts a replacement; the
   autoscaler counts it toward `min_replicas`.
10. **Config switching:** `POST /admin/reload` re-reads `config.yaml`; scheduler and router objects are
    rebuilt from the registries in `interfaces.py` (queued requests are carried over). No code change.
11. **Logging:** every decision goes through `EventLog` with tags `SCHED`, `ROUTE`, `SCALE`, `HEALTH`.
    Demo scripts grep these tags; plots are produced from these logs.
12. **Resource sizing rule:** `max_replicas * replica_cpus <= host cores`, otherwise scaling shows no benefit
    and the measurements are noise. On a 2-core host that means at most 2 replicas at 1 CPU; use
    `replica_cpus: 0.5` to demonstrate 3-4. Set this after `check_feasibility.sh` on your machine.

## 3. Repository layout

```
gateway/  interfaces.py  config.yaml  main.py  scheduler.py  router.py
          replica_manager.py  autoscaler.py  health.py  logging_conf.py
loadgen/  burst.py  (plain asyncio/httpx script; Locust only if you want it)
scripts/  check_feasibility.sh  demo1_scheduling.sh ... demo5_config.sh  run_all_demos.sh
report/   report.md  plots/
models/   (GGUF, not committed)
```

Files are added phase by phase; `interfaces.py` and `config.yaml` are already in place.

## 4. Phases and completion criteria

A phase is complete only when its test/demo script runs and its output is saved under `report/evidence/`.

| # | Phase | Done when |
|---|---|---|
| 1 | Env + gateway skeleton | `check_feasibility.sh` output recorded on the demo machine; gateway streams one request through to one replica with `curl -N` |
| 2 | Replica manager + routing | N replicas started/stopped via Docker API with limits confirmed by `docker inspect`; round robin and least-busy visible in `ROUTE` logs for 10 requests |
| 3 | Scheduling (demo 1) | `demo1_scheduling.sh` shows different service order under FCFS vs SPF, and a long prompt is still served (its wait logged) |
| 4 | Session affinity (demo 2) | 5-turn conversation: all 5 `ROUTE` lines name the same replica; turn-2 TTFT compared with and without affinity |
| 5 | Fault tolerance (demo 4) | `docker kill` on a busy replica: `HEALTH` detection log, all client requests finish, replacement reaches READY; retry-output assumption (#6) checked |
| 6 | Autoscaler | Burst triggers `SCALE up`, idle triggers drain + `SCALE down`; no flapping over a 2-minute run |
| 7 | Pre-warming (demo 3) | Same burst run with `prewarm: none` and `warm_pool`; time from trigger to first request served on the new replica is recorded for both |
| 8 | Config switching (demo 5) + load script | Scheduler/router/prewarm changed via config + reload, no restart or code edit, behaviour visibly changes |
| 9 | Measurements | Real numbers from the demo machine only; plots generated from logs |
| 10 | Report + `run_all_demos.sh` + two clean dry runs | Report states defaults and justification with measured data |

Time-wise this fits the 12-day plan; phases 1 and 2 share Day 1-3, and the rest follow the Day 4-12 order.

## 5. Open risk and testing strategy

The main risk is that the real model and llama.cpp image cannot be used in the sandbox, so I cannot
verify llama.cpp-specific behaviour (slots, prompt cache, streaming format, real load time) here.
Gateway logic (queue ordering, routing, autoscaler, retry, health, Docker lifecycle) does not depend on the
real model, so it can be tested here against a small stand-in replica that speaks the same HTTP endpoints
(`/health`, streamed `/v1/chat/completions`) with a configurable startup delay and token rate.
Anything measured against the stand-in is labelled as such and is never reported as performance of the real model.
Whether to build the stand-in is the one open decision (see the summary message).

## Change log
- Phase 0: removed "predictive" pre-warming (optional, not required); added container settings to `Config`.
