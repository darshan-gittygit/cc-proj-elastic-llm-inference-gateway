# Repeated real-model runs (Day 2 of finalization)

Written after the runs. Every `*_real_run<k>.txt` file is the unedited output of a demo script; the
numbers below come from `SUMMARY.txt`, which `report/aggregate_repeats.py` computes from those files.
All runs are **REAL MODEL** (llama.cpp server image, Qwen2.5-0.5B-Instruct Q4_K_M, 1 CPU / 1 GiB per
replica) on the demo machine (Windows 11 laptop, WSL2 Ubuntu 24.04, 16 cores visible, Docker 29.1.3).
No stand-in result is used. The committed single-run evidence of Phases 3, 4 and 7 is unchanged.

## How the runs were made

- Date: 2026-11-10, 19:59-20:38 UTC (`run_log.txt`: start/end, exit code, PASS/FAIL count, leftover
  containers, port check and load average before every demo).
- 3 rounds; each round ran, one after another and never overlapping:
  `demo3_prewarm.sh real`, `compare_pools.sh real`, `demo1_scheduling.sh real`, `demo2_affinity.sh real`.
  Rounds interleave the experiments, so slow drift of the host affects all of them alike.
- Each round ran in a fresh copy of the project in /tmp without `report/evidence/` (the method of
  `scripts/run_all_demos.sh`), so no committed evidence could be overwritten; each output was copied
  here as `<name>_run<k>.txt`. Before every demo: no `llmgw-replica-*` container and port 8000 free
  (checked and logged; never violated).
- Code: the runs used the gateway code as submitted (after the replica-load and crash-logging
  fixes); later changes to `gateway/` are comments and docstrings only.
- Scripts and configuration: the demo scripts as they are in the working tree (demo 3 with the Day 1
  lifecycle extension; `compare_pools.sh` new). Configuration = `gateway/config.yaml` with only the
  per-experiment keys each script sets (listed in every output file's header). No policy, threshold
  or architecture change.
- Result: 12 of 12 runs completed, 0 FAIL checks (demo 3: 21/21, pools: 13/13, demo 1: 17/17,
  demo 2: 25/25 per run). No run failed or was repeated or discarded.

## Results (n = 3 runs; mean, range min-max; descriptive only, no significance claimed)

### 1. Demo 3 lifecycle: prewarm `none` vs `warm_pool` (14 clients x max_tokens 40)

| Metric | none | warm_pool |
|---|---|---|
| First scale-up: trigger -> new capacity routable | 1.29 s (1.03-1.69) | 0.00 s (all runs) |
| First scale-up: trigger -> first token on new replica | 7.72 s (6.33-8.78) | 5.05 s (3.58-7.04) |
| load_s of that replica | 1.64 s (1.33-2.09) | 2.02 s (1.98-2.04), loaded before it was needed |
| Burst: first-token mean | 24.00 s (19.61-26.93) | 23.37 s (19.16-25.94) |
| Burst: complete mean | 38.43 s (29.89-44.57) | 38.55 s (31.04-42.33) |
| Burst: makespan | 54.95 s (40.26-64.71) | 57.51 s (47.20-64.34) |
| Idle: last completion -> back at min_replicas READY | 56.40 s (53.97-61.16) | 56.54 s (52.87-59.74) |
| Replica containers while idle | 1 | 2 (r1 + re-armed spare) |

Full lifecycle in every run, both modes: 1 READY -> peak 3 -> 2 drained scale-downs -> 1 READY; no
scale-up after the first scale-down; `warm_pool` re-armed its spare at the end.

**Trade-off as measured.** The warm pool removes model load from the scale-up path (new capacity is
routable at the trigger instead of ~1.3 s later) and the new replica's first token came earlier in
each of the 3 pairs. It did **not** change what the burst's clients saw: first-token mean, completion
mean and makespan overlap almost completely between the modes, because a replica loads in ~1.3-2.1 s
while the 5 s persistence window (`scale_up_after_s`) and queueing behind 2 slots per replica dominate
(the Phase 7 finding, now seen in 3 more runs). The price is one extra container (1 CPU / 1 GiB
reserved) the whole time the system is idle. The committed Phase 7 single run had the opposite sign
for trigger -> first token (none 6.87 s, warm_pool 8.17 s), so over all 4 runs warm_pool was lower in
3 of 4; this is not a robust difference.

### 2. Baseline: fixed pool of 1 vs fixed pool of 3 vs autoscaling (same burst as demo 3)

| Metric | fixed1 | fixed3 | autoscale (config.yaml) |
|---|---|---|---|
| First-token mean | 58.09 s (57.39-59.24) | 25.91 s (20.74-34.83) | 25.40 s (24.22-26.85) |
| First-token max | 110.21 s (108.56-111.97) | 54.03 s (44.35-72.45) | 44.89 s (42.98-47.23) |
| Complete mean | 72.84 s (71.74-74.27) | 48.55 s (38.25-66.03) | 41.82 s (38.69-44.38) |
| Makespan | 124.38 s (122.17-125.88) | 72.25 s (58.50-98.18) | 59.84 s (56.79-63.35) |
| Containers while idle / peak | 1 / 1 | 3 / 3 | 2 / 3 |
| Container-seconds in the burst window | 124.4 (122.2-125.9) | 216.7 (175.5-294.5) | 173.5 (164.4-183.9) |

- One replica (2 slots) gave about 2-2.8x the makespan and first-token latency of autoscaling in
  every run and of fixed3 in runs 2 and 3; against the anomalous fixed3 run 1 the ratios were 1.3x
  (makespan) and 1.7x (first token).
- **fixed3, run 1 is an outlier**: every latency in that run is ~1.6x its runs 2-3 values (makespan
  98.18 s vs 58.50 / 60.06 s) although the routing (6/4/4 requests per replica) and the replica
  load times were normal. The demo scripts do not keep raw gateway logs, so the cause could not be
  identified; host interference is the likely explanation but is not proven. It is kept in the
  mean and range. Without it, fixed3 and autoscale are close: makespan 58.50 / 60.06 s vs
  56.79-63.35 s, and fixed3 had a lower first-token mean (20.74 / 22.17 s vs 24.22-26.85 s).
- Cost: autoscaling held 2 containers while idle (r1 + warm spare) against 3 for fixed3, and reached
  3 only during the burst. Container-seconds is reserved capacity (replica_cpus x time), not
  measured CPU use; for fixed3 it scales with the makespan, so run 1 inflates it.
- Interpretation: for this one burst shape, autoscaling gave latency close to a permanently
  provisioned pool of 3 with fewer containers held idle; a fixed pool of 3 can still be faster for
  the first requests, since its capacity exists before the burst. A different burst (longer, or
  arriving faster than the 5 s scale-up window) could change this.

### 3. Scheduling (demo 1, 1 replica x 1 slot): wait of the long request L

| Run | Policy | Wait of L | Service order |
|---|---|---|---|
| A | fcfs | 6.31 s (6.20-6.43) | identical in all 3 runs (arrival order) |
| B | spf_aging, alpha 5 | 14.28 s (13.51-14.90) | identical in all 3 runs (L last) |
| C | spf_aging, alpha 5, short every 1 s | 23.50 s (22.43-25.12) | identical; 9 shorts before L, 7 after, all 3 runs |
| D | spf_aging, alpha 0, same load | 39.83 s (38.07-40.95) | identical; L last (16 shorts before), all 3 runs |

The scheduling behaviour (order, aging crossover, starvation without aging) reproduced exactly in
all 3 runs. In A, B and D, L waited longer than in the committed Phase 3 run (6.31 / 14.28 / 39.83 s
vs 5.25 / 10.22 / 27.28 s) because the replica served each request more slowly: mean time between
consecutive selections 2.61 / 1.96 / 2.06 s vs 1.70 / 1.55 / 1.31 s (`SUMMARY.txt`). In C both were
similar (2.16 vs 2.11 s per request; L waited 23.50 vs 24.50 s). The gateway's decisions were the
same. D grows most because L waits for all 16 short requests. Absolute times therefore depend on host conditions; the
order and the A < B < C < D ranking of L's wait do not.

### 4. Session affinity (demo 2, 2 replicas, 3 conversations x 5 turns): turn-2 TTFT

| | per-run mean of 3 conversations |
|---|---|
| session_affinity | 1.90 s (1.26-2.22) |
| round_robin | 3.34 s (3.03-3.56) |
| difference (rr - affinity) | 1.44 s (0.81-2.18) |

Across all 9 turn-2 requests per router, the ranges do not overlap (affinity 1.19-2.39 s, round
robin 2.93-3.82 s); prompt-cache reuse on turn 2 (cache_n) is listed per run in the run files.
The committed Phase 4 run (1.55 vs 3.25 s) lies within these ranges. Demo 2 uses config.yaml
defaults except min_replicas=2 and the router, as in the committed run.

## Fairness and limits

- Same workload, same metric code, same image/model/limits; only the keys under comparison differ
  (each output file checks and prints its config differences).
- Runs were sequential and isolated (clean-container and port checks logged before each demo), but
  the host is a laptop running Windows: background activity and power state were not controlled.
  Load average at demo start ranged 0.02-5.15 (1-minute value, logged in `run_log.txt`; the highest
  values were taken right after the previous demo had finished).
- Within a round the order was always demo3 -> pools -> demo1 -> demo2, and within the pool
  comparison fixed1 -> fixed3 -> autoscale; an order effect cannot be excluded.
- n = 3 per configuration: means and ranges only, no significance tests; overlapping ranges are
  reported as "no measurable difference in these runs", not as equality.
- Raw gateway logs are not kept by the demo scripts, so outliers can be described but not diagnosed
  after the fact.
- Only one burst shape (14 concurrent clients, 40 tokens) was used for the scaling comparisons.

## Not done

- Cold model-load measurement: not done (decided to skip; it needs `sudo` to drop the page cache,
  and in WSL2 it would clear only the Linux VM's cache, not Windows'). Every `load_s` in this
  project is therefore a start with the model file most likely in the page cache.
