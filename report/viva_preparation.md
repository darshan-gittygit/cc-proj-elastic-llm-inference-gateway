# Viva preparation - Elastic LLM Inference Gateway

Internal preparation notes. Every statement points to code (`file:line` as of 2026-11-11) or to
evidence; where the project does not establish an answer, that is said explicitly. Numbers are
from `report/report.md` (repeated runs: `report/evidence/repeats/SUMMARY.txt`).

---

## A. Project overview

**Problem.** A CPU-hosted LLM replica is slow (a few tokens/s) and has a fixed number of parallel
slots (`-np`). Clients talking to replicas directly get no control over who waits, where a
conversation runs, what happens when a replica dies, or how many replicas exist.

**What the gateway solves.** One FastAPI process in front of a pool of llama.cpp containers makes
four decisions explicitly and logs each one: *scheduling* (which waiting request goes next),
*routing* (which replica serves it), *replica management* (start, health-check, drain, replace,
scale, keep a warm spare), and *runtime reconfiguration* (`POST /admin/reload`).

**Main contribution.** A working, single-host elastic serving system with:
- a central queue where only the gateway queues, so scheduling policies actually take effect;
- three routing policies including session affinity with gateway-held history;
- failure handling with requeue-at-front and prefix-verified stream resumption;
- load-based autoscaling with hysteresis and an optional warm pool;
- an honest evaluation: repeated real-model runs, a fixed-capacity baseline, and stated negative
  results (the warm pool's end-to-end benefit was not shown; decision 8's premise did not hold).

**Cloud computing concepts it demonstrates.**

| Concept | Where |
|---|---|
| Elastic horizontal scaling (scale out / in on a load signal, hysteresis) | `gateway/autoscaler.py` |
| Resource isolation and quotas (cgroups: `--cpus` -> `nano_cpus`, `--memory` -> `mem_limit`) | `replica_manager.py:144-153` |
| Programmatic infrastructure (Docker Engine API; containers as cattle, labelled and prefixed) | `replica_manager.py` |
| Health checking, self-healing, replacement | `gateway/health.py` |
| Cold-start mitigation with warm pools (the serverless trade-off: readiness vs idle cost) | `autoscaler.py:_maintain_warm_pool` |
| Load balancing and stateful placement (least-busy, session affinity) | `gateway/router.py` |
| Graceful draining before scale-in | `replica_manager.py:stop_replica` |
| Static vs dynamic infrastructure (Compose for the gateway, Engine API for replicas) | `docker-compose.yml` |
| Observability (one structured log line per decision, monotonic timestamps) | `logging_conf.py`, `EventLog` |

**Architecture walkthrough.**

```
client --SSE--> POST /v1/chat/completions --> central queue (Scheduler) --> dispatch loop
                                                                              | only when a slot is free
                         Router (round_robin | least_busy | session_affinity) v
          ReplicaPool <-- ReplicaManager (Docker Engine API) <-- Autoscaler (+ warm pool)
               ^-- HealthMonitor (probes, DEAD, replacement)
```

**End-to-end request lifecycle** (`gateway/main.py`).
1. `chat_completions` (`main.py:474`) validates the body (`messages` non-empty list; `stream:false`
   rejected; optional `session_id` string) and builds a `Request`. With a `session_id` the stored
   history is prepended (`new_request`, `main.py:198`); prompt length = characters / 4.
2. `submit` pushes it into the scheduler (the only queue) and wakes the dispatch loop.
3. `_dispatch_loop` (`main.py:272`) runs while the queue is non-empty **and** some replica is
   routable (READY with a free slot): `scheduler.pop()` -> `router.choose()` -> `_dispatch`.
4. `_dispatch` increments the replica's `in_flight`, logs `[ROUTE] dispatch`, starts `_stream`.
5. `_stream` (`main.py:312`) posts to the replica with `stream=true` and forwards each SSE chunk into
   `Request.out`; the HTTP handler yields from it and finally sends `data: [DONE]`.
6. On completion: the turn is appended to the session history (only completed turns), `[ROUTE]
   complete ... ttft_s total_s` is logged, the slot is freed and the dispatch loop is woken.
7. Client disconnects: the handler's `finally` calls `cancel` (`main.py:209`): a running stream is
   cancelled (slot freed); a queued request is marked FAILED and dropped by the dispatch loop.

---

## B. Design decisions

**Why a central queue?** If replicas queued (llama-server has its own slot queue), they would serve
FCFS internally and no gateway scheduling policy could have any effect. Dispatching only to a free
slot (decision 1) makes the scheduler the single point that decides order, and the router the single
point that decides placement. It also gives one place to measure wait and to drop abandoned requests.

**Why FCFS and shortest-prompt-first with aging?** FCFS is the fair, predictable baseline. SPF
favours short prompts, which reduces their waiting (measured: short requests 8.65 s vs 13.11 s),
at the cost of long ones (14.28 s vs 6.31 s). Plain SPF can starve long requests (scenario D: L served
last every time, 39.83 s); aging (`priority = prompt_tokens - alpha * wait_s`) lets L overtake short
requests that arrive more than `(p_L - p_S)/alpha` seconds after it (scenario C: 9 before, 7 after,
identical in all runs). Implemented as a heap on the static key `prompt_tokens + alpha * arrival_ts`
(same order, O(log n), `scheduler.py:69-74`).

**Why three routing strategies?** `round_robin` is the simplest baseline; `least_busy` adapts to
unequal request lengths (in-flight / slots); `session_affinity` exploits llama-server's prompt cache
for multi-turn conversations. They are interchangeable at run time through the registry and reload.

**Why gateway-side conversation history?** Clients send only the new message, and every replica
receives the full conversation, so correctness never depends on which replica served the previous
turn (failover and re-mapping are safe). Affinity then becomes a pure performance optimisation: the
same replica can reuse the cached prompt prefix (measured cache reuse on turn 2: 44 vs 24-28 tokens).

**Why autoscaling?** Load is bursty; fixed capacity either wastes containers at idle or queues
under bursts. The signal `(queued + in_flight) / READY` with separate up/down thresholds and two
timers is simple hysteresis. Measured: 1 -> 3 -> 1 in every lifecycle run, no flapping.

**Why a warm pool?** A scale-up that starts a new container waits for the model to load before the
replica is routable; a loaded spare removes that wait. The frozen design assumed load dominates
scale-up delay (decision 8). Measured on this host with a cached model: load ~1.3-2.1 s, and the
burst-level latency did not change measurably. It is kept as the default as a readiness-vs-cost
choice (one idle container), not as a measured performance win (section D, Q1).

**Why Docker-managed replicas through the Engine API (not Compose services)?** The number of
replicas changes at run time; Compose describes a static set. The Engine API gives per-container
limits (cgroups), labels for safe cleanup, networks, start/stop/kill, all from the gateway's own
control loop. Compose is used for the static part (the gateway service).

**Why llama.cpp and Qwen2.5-0.5B-Instruct?** CPU-only laptop. llama.cpp's server is a single
container with an OpenAI-compatible streaming API, `/health`, parallel slots and a prompt cache. A
0.5B Q4_K_M model fits 1 GiB per replica, so 3 replicas plus the gateway fit the 7.6 GiB WSL VM,
and it streams a few tokens/s on 1 CPU (Phase 1 feasibility). The gateway logic is model-agnostic.

---

## C. Experimental methodology

**Machine.** Windows 11 laptop, WSL2 Ubuntu 24.04 (16 logical cores, 7.6 GiB), Docker 29.1.3;
replicas `--cpus 1 --memory 1g`, 2 slots, 2048-token context per slot.

**Workloads and conditions** (report section 4).
- Pre-warming lifecycle (`demo3_prewarm.sh`): 1 replica READY, 15 s idle, 14 concurrent streamed
  clients x 40 tokens, then idle until back at `min_replicas`; `prewarm: none` vs `warm_pool`.
- Pool baseline (`compare_pools.sh`): the same burst; fixed1 (min = max = 1), fixed3 (min = max = 3),
  both without warm pool; autoscale = `config.yaml` (min 1, max 3, warm pool).
- Scheduling (`demo1_scheduling.sh`): 1 replica x 1 slot; A/B: blocker, long L, 4 short (fcfs vs
  spf_aging); C/D: blocker, L, a short request every 1 s, 16 in all (alpha 5 vs 0).
- Affinity (`demo2_affinity.sh`): 2 replicas, 3 conversations x 5 turns, strictly sequential.

**Metrics** (all from the gateway log's monotonic `mono=` field).
- *First token* (burst experiments): arrival in the gateway queue -> first token; includes queue wait.
- *Turn-2 TTFT* (affinity): gateway `ttft_s`, dispatch -> first token; excludes queue wait. Not
  compared with the first metric.
- *Makespan*: first arrival -> last completion of the burst.
- *Wait* (scheduling): arrival -> selection by the scheduler (`wait_s`); excludes service time.
- *Service interval*: mean time between consecutive selections with a non-empty 1-slot queue
  (= time per request); used only to explain host-speed differences.
- *load_s*: just before `docker run` -> `/health` 200, polled every 0.5 s (~0.5 s resolution).
- *Container-seconds*: containers alive x time in the burst window = CPU reserved, not CPU used.

**Repeated measurements.** Each of the four experiments run 3 times (12 runs, 39 min), sequentially,
in rounds that interleave the experiments; each in a fresh project copy; clean-state checks before
each script; 12/12 completed, 0 failed checks, none discarded. `report/aggregate_repeats.py` prints
per-run values, **mean** (arithmetic mean of the 3 per-run values) and **range** (min-max of the 3).
Descriptive only; **no statistical significance is claimed**. Overlapping ranges = "no measurable
difference in these runs", not "equal".

**Key results (mean [range], n = 3).**

| Comparison | Result |
|---|---|
| fixed1 / fixed3 / autoscale: first-token mean | 58.09 / 25.91 [20.74-34.83] / 25.40 [24.22-26.85] s |
| fixed1 / fixed3 / autoscale: makespan | 124.38 / 72.25 [58.50-98.18] / 59.84 [56.79-63.35] s |
| containers idle / peak | 1/1, 3/3, 2/3 |
| none vs warm_pool: trigger -> routable | 1.29 [1.03-1.69] vs 0.00 s |
| none vs warm_pool: burst first-token mean, makespan | 24.00 vs 23.37 s; 54.95 vs 57.51 s (overlapping) |
| fcfs vs spf_aging: short wait, long wait | 13.11 vs 8.65 s; 6.31 vs 14.28 s |
| aging vs no aging (overload): wait of L | 23.50 vs 39.83 s; order identical in all runs |
| affinity vs round robin: turn-2 TTFT | 1.90 [1.26-2.22] vs 3.34 [3.03-3.56] s; 9 vs 9 values do not overlap |

**Limitations and threats to validity.** n = 3; uncontrolled laptop host (absolute per-request time
varied up to ~1.6x between sessions); fixed experiment order; unexplained fixed3 run 1 outlier (raw
logs not kept); one workload shape per question; affinity only sequential; load_s most likely with
a cached model (cold load not measured); llama.cpp runs 8 threads under a 1-CPU quota (effect not
measured); single-run Phases 1, 5, 6, 8.

---

## D. Difficult questions

**1. Why is warm pool the default despite no demonstrated end-to-end improvement?**
It does what it is designed to do: the new capacity is routable at the scale-up trigger (0.00 s vs
1.29 s) and the new replica's first token came earlier in all 3 paired runs (5.05 vs 7.72 s). It did
not change burst-level latency here because a cached load (~1.3-2.1 s) is small next to the 5 s
persistence window and queueing. Its benefit is expected to grow when loads are slow (bigger model,
cold disk), which we did not measure. Cost: one loaded container held while idle. The report states
plainly that `prewarm: none` is equally defensible on this host; the default is a readiness-vs-cost
choice, switchable at run time with a reload.

**2. Is autoscaling actually better than fixed3?**
Not in latency. First-token means are comparable (25.40 vs 25.91 s), and fixed3 was faster to first
token in 2 of 3 runs (20.74, 22.17 vs 24-27 s) because its capacity exists before the burst.
Autoscale's lower mean makespan (59.84 vs 72.25 s) is driven by the fixed3 outlier; in runs 2-3
fixed3's makespan was within autoscale's range. Autoscaling's advantage is cost: 2 containers idle
instead of 3 (1 instead of 3 without the warm pool) for comparable latency on this burst.

**3. What caused the fixed3 outlier?**
Unknown. Run 1 was ~1.6x slower in every latency metric, but its routing (6/4/4 requests) and replica
load times (1.46-1.95 s) were normal. The demo scripts don't keep raw gateway logs, so it cannot be
diagnosed after the fact. Host interference (Windows background activity, CPU throttling) is
plausible but not shown. It is kept in every mean and range and labelled in Fig. 2.

**4. Why only three runs?**
Time budget on a single laptop: 12 runs took 39 minutes, and the runs had to be sequential. Three runs
are enough to show that deterministic behaviour repeats (scheduling order identical every time) and
to see large effects (fixed1 vs three replicas ~2x; affinity ranges do not overlap). They are not
enough to resolve small effects such as the warm pool's burst-level difference, which is why no
significance is claimed and overlapping ranges are reported as "no measurable difference".

**5. Why were experiments run in the same order?**
Simplicity of the runner. Rounds interleave the experiments, so slow drift affects all of them, but
within a round the order was fixed (demo3 -> pools -> scheduling -> affinity; fixed1 -> fixed3 ->
autoscale). An order effect (e.g., thermal state after the previous run) cannot be excluded;
randomising the order would be the fix. Acknowledged in report section 7.

**6. What happens under overload when the queue has no size limit?**
Requests accumulate in memory, each with an open SSE connection; there is no admission control (no
429/503). The autoscaler scales to `max_replicas` (3); beyond that latency grows without bound. Under
`spf_aging` the long request's position is still bounded relative to later arrivals (crossover);
under `fcfs` everyone waits in order. A client that gives up is dropped from the queue (`cancel` marks
it FAILED, `main.py:216-217`). Scenario C/D is a mild overload (arrivals every 1 s, ~2 s service) and the
scheduler behaved as designed; sustained overload was not tested. Fix: a queue bound with 503.

**7. What happens when conversation history exceeds the context window?**
Not handled and not tested. The gateway prepends the whole history (`main.py:201`) with no
truncation; each slot has 2048 tokens of context (llama-server log: `n_ctx_slot = 2048`). If
llama-server rejects the oversized prompt with a non-200 response, the gateway raises
`ReplicaHTTPError`, retries up to `max_retries` (all fail the same way) and then sends an error event
(`main.py:357-359`, `414-429`). Because only completed turns enter the history, the session would stay
over-long and every later turn would fail the same way. Also, as history grows, `spf_aging` sees the
turn as a long prompt and deprioritises it. Fix: bounded history (truncate or summarise old turns).

**8. Does session affinity help with concurrent sessions?**
Not established. The measurement used one conversation at a time. With concurrency, two effects
compete: cache reuse still helps, but pinning can send a session to a busy replica. The
implementation limits this: if the home replica is not READY with a free slot, the request falls back
to least_busy and the session is re-mapped (`router.py:69-88`), losing the cache benefit for that
turn. The net effect under concurrency was not measured.

**9. What happens when a replica fails during inference?**
Two detection paths (Phase 5, 2 runs): a killed container breaks the stream -> `httpx.TransportError`
-> replica marked DEAD (detected in 0.46-0.66 s); a frozen (paused) container keeps the stream open
(no read timeout), so only the health probes notice (3 failures at 1.5 s intervals; detected in
7.55-7.77 s). Then `_on_replica_dead` cancels its streams, the requests go back to the **front** of
the queue, the container is removed and a replacement started (READY 2.25-2.44 s after detection).
On retry, the regenerated tokens are compared with what the client already has and skipped if equal;
otherwise the client gets an `llmgw_retry` event and the output restarts. All 8 retried requests
completed on their first retry; the final text of one prompt differed from a fault-free run in 3 of
4 runs (greedy decoding is not exactly reproducible under batching).

**10. What happens when a warm spare fails while loading?**
`_load_failed` (`replica_manager.py:201-220`) removes its container and pool entry and counts a load
failure, but the manager does not replace a spare. The autoscaler's warm-pool maintenance starts a new
spare on a later tick when its conditions hold (busy or idle signal, nothing STARTING, below
`max_replicas`). There is no failure cap on that path, so with a permanently broken image spares
would be retried one at a time, each removed after failing (no container leak). A spare that dies
**after** loading is probed by the health monitor and replaced by a spare. Non-spare load failures
are replaced only below `min_replicas`, at most 3 in a row (test `test_failure_paths.py`).

**11. Why is least busy the routing default?**
It needs nothing from the client, adapts to unequal request lengths in principle, and it is also the
fallback inside `session_affinity`. In the measured runs (balanced load) it made the same choices as
round robin, so no performance advantage is claimed. Clients that send `session_id` benefit from
switching to `session_affinity` (reload, no restart).

**12. How does the 30-second drain period work?**
Scale-down picks the least-loaded READY replica (`autoscaler.py:120`), only when more than
`min_replicas` are READY. `stop_replica` (`replica_manager.py:116-126`) marks it DRAINING (no new
requests), waits until `in_flight == 0` (checked every 0.2 s) for at most 30 s, then stops (5 s grace)
and removes the container. If requests are still running at 30 s, their streams break and they take
the normal retry path (requeue at front, another replica). That last path is from the code; in all 15
recorded real scale-downs the replica had drained (`drained=True`). The 30 s bound is a documented
deviation from decision 7.

**13. Shortest-prompt-first vs ordinary shortest-job-first?**
SJF orders by total job (service) time, which is unknown for an LLM because the output length is
unknown until generation ends. SPF orders by the prompt length (estimated as characters / 4), which
is known at arrival and drives the prefill cost; it ignores output length (`max_tokens`). It is
non-preemptive (a dispatched request runs to completion), and aging is added to avoid SJF's
starvation of long jobs.

**14. Why alpha = 5?**
It is a design value, not tuned: no sweep was done. Units: estimated tokens per second of waiting.
It sets the crossover `(p_long - p_short)/alpha`: with the demo prompts (52 vs 6 tokens) a long
request overtakes short requests that arrive more than 9.2 s after it, a scale comparable to a few
request service times on this replica (~2 s each). alpha = 0 is pure SPF (starvation, scenario D);
a very large alpha approaches FCFS. Changeable at run time with a reload.

**15. Could CPU contention affect the results?**
Yes, in two ways. (a) Host: 16 logical cores, at most 3 replicas x 1 CPU quota plus gateway and load
generator, so the host is not oversubscribed by our own processes, but Windows background activity
was not controlled (absolute times varied up to ~1.6x between sessions). (b) Inside a replica:
llama.cpp starts 8 threads under a 1-CPU CFS quota (`report/evidence/replica_threads_real.txt`), so its
threads share 1 CPU of time; the effect on throughput was not measured. All conditions had the same
setting, so comparisons are like for like; absolute numbers may differ from a 1-thread setting.

**16. Are the measured model-loading times genuine cold-start times?**
No. They were most likely measured with the model file in the Linux page cache. A cold-load
measurement was planned but skipped (needs `sudo` to drop caches; in WSL2 that clears only the VM's
cache, not Windows'), and the unrun script was removed. `load_s` must not be read as a cold-start
time; a cold load would be slower and would strengthen the warm pool's case.

**17. Why a small model instead of a larger LLM?**
Hardware: CPU-only laptop, 7.6 GiB for the VM. The project's subject is the serving layer
(scheduling, routing, scaling, failure handling), which is model-agnostic. A 0.5B Q4 model allows 3
isolated 1 GiB replicas, so scaling and routing are observable. A larger model would load slower
(favouring the warm pool) and generate slower (more queueing); the mechanisms would not change.

**18. What happens when all retries are exhausted?**
`max_retries: 2` -> at most 3 attempts. After the last failure (`main.py:423-429`) the request is
FAILED, `[ROUTE] stream failed, retries exhausted` is logged, and the client receives
`data: {"error": {"message": "...", "type": "replica_error"}}` followed by `data: [DONE]`. The HTTP
status is already 200 (streaming), so the error is in-band.

**19. What happens when two turns of the same session arrive concurrently?**
Not serialised and not tested. Both are built from the same stored history (neither contains the
other); they may run in parallel (same or different replica). When each completes it overwrites the
session's history with its own messages plus answer (`main.py:383-385`), so the turn that finishes
last wins and the other turn disappears from the history. A per-session lock would fix it.

**20. Biggest limitations and next steps?**
Limitations: small n on an uncontrolled host; no cold-load data; affinity only sequential; no
admission control; unbounded session history; thread count not matched to the CPU quota; raw logs
not kept. Next: (1) keep raw logs and randomise run order with more runs; (2) set `-t 1` and measure
its effect; (3) measure cold load on native Linux; (4) admission control (queue bound, 503) and a read
timeout; (5) bounded history with a per-session lock; (6) affinity under concurrent sessions.

---

## E. Demo walkthrough

Run from the project root in WSL (venv active). Real mode needs the model in `./models` and the
llama.cpp image (`bash scripts/check_feasibility.sh`). Rehearsed end to end on 2026-11-11 with the
stand-in image; Compose and a real streamed request also checked the same day.

**1. Start the gateway** (it starts `min_replicas` replicas and, by default, a warm spare). Use a
copy of the config so the live reload in step 4 never edits a tracked file:
```bash
cp gateway/config.yaml /tmp/demo.yaml
LLMGW_CONFIG=/tmp/demo.yaml LLMGW_LOG_FILE=/tmp/gw.log uvicorn gateway.main:app --port 8000
```
Second terminal, watch the decisions:
```bash
tail -f /tmp/gw.log | grep --line-buffered -E '\[(SCHED|ROUTE|SCALE|HEALTH|CONFIG)\]'
docker ps --format '{{.Names}} {{.Status}}'
```
Expect `[SCALE] replica started r1`, `[HEALTH] replica ready`, then `prewarm spare r2 trigger=idle`.
Alternative start: `docker compose up -d --build` and `docker compose logs -f gateway`.

**2. Send a request and show streaming:**
```bash
curl -N http://127.0.0.1:8000/v1/chat/completions -H 'Content-Type: application/json' \
  -d '{"messages":[{"role":"user","content":"Say hello in five words."}],"max_tokens":32,"stream":true}'
```
Tokens arrive as `data: {...}` chunks, then `data: [DONE]`; the log shows `queued`, `selected`,
`dispatch`, `complete ... ttft_s`.

**3. Scale-up and warm pool:**
```bash
python loadgen/burst.py --n 14 --long-every 0 --max-tokens 40 --interval 0
```
Expect after ~5 s `scale up how=promote_spare replica=r2` (no load wait), `prewarm spare r3
trigger=load`, then a second promotion; `docker ps` shows 3 replicas.

**4. Session affinity** (switch the router live; no restart):
```bash
sed -i 's/^router: least_busy/router: session_affinity/' /tmp/demo.yaml
curl -s -X POST http://127.0.0.1:8000/admin/reload
python loadgen/burst.py --session conv1 --pattern "S,S,S,S" --max-tokens 8
```
Expect `[CONFIG] reload applied changed=router:least_busy->session_affinity` and four
`[ROUTE] dispatch ... session=conv1` lines on one replica: `affinity=new`, then `hit` x3.

**5. Scale-down:** stop sending; after ~30 s below the threshold: `scale down replica=r3` ->
`replica stopped ... drained=True`, a second one ~30 s later, and with the warm pool
`prewarm spare ... trigger=idle` re-arms the spare.

**6. Scheduling, affinity measurement, faults, config switching (scripted, with PASS/FAIL checks).**
The demo scripts write to `report/evidence/` and refuse to overwrite committed evidence, so run them
in a throw-away copy (stop your own gateway first; they use port 8000):
```bash
demo() { D=$(mktemp -d); tar -C . --exclude=./.git --exclude=./report/evidence --exclude=./models -cf - . | tar -C "$D" -xf -; ln -s "$PWD/models" "$D/models"; (cd "$D" && bash "scripts/$1.sh" "${2:-real}"); rm -rf "$D"; }
demo demo1_scheduling real    # fcfs vs spf_aging, aging crossover (~2 min)
demo demo2_affinity real      # affinity vs round robin, turn-2 TTFT (~1.5 min)
demo demo4_fault real         # docker kill / docker pause mid-stream (~2.5 min)
demo demo5_config real        # live reload changes order, placement, warm pool (~1 min)
demo demo3_prewarm real       # full 1 -> 3 -> 1 lifecycle, none vs warm_pool (~4 min)
```
Or everything at once: `bash scripts/run_all_demos.sh real` (8 demos, ~14 min before demo 3 was
extended; now a few minutes longer).

**7. Shut down:** Ctrl+C on uvicorn (or `docker compose down`). Expect `[SCALE] replica removed
... reason=gateway_shutdown` for every replica; then
```bash
docker ps -a --filter name=llmgw-replica- --format '{{.Names}}'    # prints nothing
```
