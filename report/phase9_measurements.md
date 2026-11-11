# Phase 9 - Measurements and plots (real model only)

> This file records the Phase 9 single-run measurements. The later repeated runs (3 per condition)
> and the fixed-pool baseline are in `report/evidence/repeats/` and section 6.1 of `report.md`.

Frozen criterion (ARCHITECTURE.md section 4): *"Real numbers from the demo machine only; plots generated
from logs."* Every number below comes from the committed real-model evidence of Phases 1-7, recorded
on the demo machine (Windows 11 laptop, WSL2 Ubuntu 24.04, 16 cores visible, Docker 29.1.3,
llama.cpp server image, Qwen2.5-0.5B-Instruct Q4_K_M, 1 CPU / 1 GiB per replica). No experiment was
re-run for Phase 9 and nothing was estimated or interpolated.

## Data used

| Source (report/evidence/) | What it contains | Used for |
|---|---|---|
| `phase1_feasibility_wsl.txt` | output of the frozen `check_feasibility.sh` (wall-clock timing) | Fig. 1 |
| `phase3_scheduling_real.txt` | raw `[SCHED]` gateway log lines, monotonic `mono=` times | Fig. 2 |
| `phase4_affinity_real.txt` | per-turn table the demo computed from the gateway log + llama-server timings | Fig. 3 |
| `phase5_fault_real_run1_early_fault.txt`, `phase5_fault_real.txt` | timelines computed from raw log lines (monotonic) | Fig. 4 |
| `phase6_autoscale_real.txt` | `[SCALE] signal` samples every ~5 s + event timeline | Fig. 5 |
| `phase7_prewarm_real.txt` | per-scale-up table + client-latency aggregates | Fig. 6 |

**Real versus simulated:** only files whose first line is `# REAL MODEL RESULT` (plus the Phase 1
feasibility run) are read; `report/measure.py` refuses anything under `report/evidence/simulated/`.
No stand-in result appears in any figure or number here. **Provenance:** the demo scripts did not keep
their raw gateway logs; only Phase 3 (and Phase 5) evidence contains raw log lines. Phases 4, 6 and 7
are plotted from the tables the demo scripts computed from those logs at run time. Each value in
`report/plots/measurements.json` is stored with its source file and that file's SHA-256.
Phase 2 (routing behaviour) and Phase 8 (config switching behaviour) evidence has no performance
measurement that needs a plot and is not used.

## How to regenerate

From the project root, with the project's Python environment (WSL: `source ~/llmgw-venv/bin/activate`;
`requirements.txt` now pins `matplotlib==3.11.2`):

```
python report/measure.py                  # -> report/plots/*.png + report/plots/measurements.json
python -m pytest tests/test_phase9.py -v  # parsers match the evidence; outputs regenerate byte-identically
```

## Results

### Fig. 1 - Single-replica feasibility (`plots/phase1_feasibility.png`)
One replica (1 CPU, 2 slots), one run per concurrency level. Model load (container start to
`/health` ok): **2.97 s**. Aggregate streamed chunks/s: **3.4, 4.2, 4.5** at 1, 2, 3 concurrent
requests; per request 3.4 / 2.1 / 1.5-2.2. At 3 concurrent requests the third waited **58.11 s** for
its first chunk (only 2 slots). Chunks/s includes the wait for the first chunk, and the script used
the wall clock (frozen script), so these are approximate.

### Fig. 2 - Scheduling (`plots/phase3_scheduling.png`)
1 replica x 1 slot; wait = time in the gateway queue, from the scheduler's own `wait_s`.

| Run | Policy | Wait of the long request L |
|---|---|---|
| A | fcfs | 5.25 s |
| B | spf_aging, alpha 5 | 10.22 s |
| C | spf_aging, alpha 5, a short request every 1 s | 24.50 s |
| D | spf_aging, alpha 0, same load as C | 27.28 s |

In C the short requests that arrived within 9.2 s of L (the crossover (52 - 6) / 5) were served before
it and the later ones after it; in D every short request was served first. n = 1 long request per run.

### Fig. 3 - Session affinity (`plots/phase4_affinity.png`)
2 replicas, 3 five-turn conversations per router. Turn-2 TTFT (gateway, monotonic):
session_affinity **1.279 / 1.697 / 1.682 s (mean 1.553 s)**, round_robin **3.372 / 3.401 / 2.967 s
(mean 3.247 s)**. Prompt tokens reused from llama-server's cache at turn 2: 44 / 44 / 44 vs 24 / 28 / 28.
Turn 2 is the clean comparison (under round robin, turns 3-5 can reuse a cache from two turns earlier).

### Fig. 4 - Fault tolerance (`plots/phase5_fault.png`)
2 replicas, 4 concurrent clients, r1 failed while busy; two runs (run 1 fault at 6 s, run 2 at 15 s).

| | kill run1 | pause run1 | kill run2 | pause run2 |
|---|---|---|---|---|
| fault -> r1 DEAD (s) | 0.46 | 7.77 | 0.66 | 7.55 |
| fault -> replacement READY (s) | 2.71 | 10.21 | 2.97 | 9.84 |
| tokens already sent by retried requests | 0, 1 | 0, 1 | 21, 22 | 26, 27 |
| regenerated prefix matched / differed | 1 / 0 | 1 / 0 | 2 / 0 | 2 / 0 |
| final text = fault-free reference | 3/4 | 4/4 | 3/4 | 3/4 |

All 4 clients completed in every run. "kill" is detected through the broken stream, "pause" through
3 failed health probes (health_interval_s 1.5, plus 1 s probe timeouts).

### Fig. 5 - Autoscaler (`plots/phase6_autoscale.png`)
`config.yaml` defaults, one 180 s run, burst of 14 clients from 9.6 s to 59.2 s. Scale up at
**15.4 s** and **20.7 s** (both promotions of a warm spare; further scale-up blocked at max_replicas 3),
scale down at **82.9 s** and **113.4 s** (both drained with 0 requests left). No reversal of direction;
never more than 3 containers. Samples are every ~5 s, so step changes in the plot lag the events by up to 5 s.

### Fig. 6 - Pre-warming (`plots/phase7_prewarm.png`)
Same burst, only `prewarm` differs; one run per mode.

| | none: scale-up 1 / 2 | warm_pool: scale-up 1 / 2 |
|---|---|---|
| trigger -> first request on the new replica | **1.59 / 2.18 s** | **0.00 / 0.00 s** |
| trigger -> first token from the new replica | 6.87 / 10.36 s | 8.17 / 6.92 s |
| new replica's model load | 1.96 / 2.50 s (during the burst) | 1.87 / 2.05 s (before the trigger) |
| client first token mean / median / max | 30.19 / 30.83 / 58.88 s | 26.89 / 27.17 / 51.51 s |
| burst makespan | 81.09 s | 66.23 s |

Only aggregates of the client latencies were recorded, not per-request values, so no distribution is shown.

## Conclusions the data supports

- Shortest-prompt-first changes the service order, and aging lets a long request overtake later short
  ones; without aging it is served last (Fig. 2). Its total wait is longer than the 9.2 s crossover.
- Keeping a session on one replica lets llama-server reuse the previous turn from its prompt cache;
  in all 3 conversations turn-2 TTFT was about half of round robin's (Fig. 3).
- A killed replica is detected in under 1 s, a frozen one in about 7.5-7.8 s; a replacement is ready
  ~2-3 s after detection, and all clients finished; regenerated prefixes matched in every retry (Fig. 4).
- The autoscaler followed the burst up and the idle period down without flapping (Fig. 5).
- The warm pool removes the model-load wait (~1.6-2.2 s) from a scale-up (Fig. 6).

## Conclusions the data does NOT support

- Any statistical claim: each condition was run once (Phase 4 has 3 conversations per router in one
  run). Requests within one run are not independent repetitions.
- That the warm pool speeds up first tokens or overall latency: first token from the new replica was
  slower with warm_pool at scale-up 1 and faster at scale-up 2; the 15 s makespan difference is larger
  than the ~3.8 s of load time it hid, so it cannot be attributed to pre-warming from one run per mode.
- That model load is the dominant scale-up delay (decision 8's assumption): measured load was ~2 s,
  against the 5 s `scale_up_after_s` window and 6.9-10.4 s to the new replica's first token. A cold
  (non-cached) model load was never measured.
- That greedy decoding reproduces outputs exactly: the same one of the four prompts ended differently
  from the fault-free reference in 3 of 4 fault runs (Fig. 4), although every checked prefix matched.
- Anything about other hardware, models or loads.

## Limitations

- Raw gateway logs were not retained; Phases 4, 6, 7 are plotted from log-derived tables, Phase 1 from
  the feasibility script's output (wall clock). Phase 6 signal values are ~5 s samples.
- Throughput is in streamed chunks (about one token each), not model tokens.
- WSL2's wall clock jumped during some runs; all durations used here are monotonic except Phase 1.
- The Phase 7 runs used a model file already in the page cache.
